use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::foundation::io::persistence as persistence;
use crate::foundation::io::persistence::codec::types::{
    RDB_TYPE_HASH_ZIPMAP, RDB_TYPE_STREAM_LISTPACKS, RDB_TYPE_STREAM_LISTPACKS_2,
    RDB_TYPE_STREAM_LISTPACKS_3, RDB_TYPE_ZSET_LISTPACK, RDB_TYPE_ZSET_ZIPLIST,
};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

/// RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME s] [FREQ f]
#[command("RESTORE", runtime)]
pub fn restore(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    // args: [0]=key, [1]=ttl, [2]=serialized, [3..]=options
    if args.args.len() < 3 {
        return CommandOutcome::reply(CommandError::wrong_arity("restore"));
    }
    let key = &args.args[0];
    let mut replace = false;
    let mut absttl = false;
    let mut lru_idle: i64 = -1;
    let mut lfu_freq: i64 = -1;

    let mut j = 3;
    while j < args.args.len() {
        let opt = String::from_utf8_lossy(&args.args[j]).to_ascii_lowercase();
        let additional = args.args.len() - j - 1;
        match opt.as_str() {
            "replace" => replace = true,
            "absttl" => absttl = true,
            "idletime" if additional >= 1 && lfu_freq == -1 => {
                let v = match parse_i64(&args.args[j + 1]) {
                    Some(v) => v,
                    None => {
                        return CommandOutcome::reply(Value::error(
                            "ERR value is not an integer or out of range",
                        ))
                    }
                };
                if v < 0 {
                    return CommandOutcome::reply(Value::error(
                        "ERR Invalid IDLETIME value, must be >= 0",
                    ));
                }
                lru_idle = v;
                j += 1;
            }
            "freq" if additional >= 1 && lru_idle == -1 => {
                let v = match parse_i64(&args.args[j + 1]) {
                    Some(v) => v,
                    None => {
                        return CommandOutcome::reply(Value::error(
                            "ERR value is not an integer or out of range",
                        ))
                    }
                };
                if !(0..=255).contains(&v) {
                    return CommandOutcome::reply(Value::error(
                        "ERR Invalid FREQ value, must be >= 0 and <= 255",
                    ));
                }
                lfu_freq = v;
                j += 1;
            }
            _ => return CommandOutcome::reply(CommandError::syntax_error()),
        }
        j += 1;
    }

    let skip_checksum = runtime.skip_checksum_validation();
    let rdb_relaxed = runtime.rdb_version_check_relaxed();
    let lazyfree_server_del = runtime.should_use_lazyfree_server_del();
    let now_ms = runtime.now_ms();
    let db_idx = runtime.selected_db();

    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };

    if !replace && db.exists(key) {
        return CommandOutcome::reply(Value::error("BUSYKEY Target key name already exists."));
    }

    let ttl = match parse_i64(&args.args[1]) {
        Some(v) => v,
        None => {
            return CommandOutcome::reply(Value::error(
                "ERR value is not an integer or out of range",
            ))
        }
    };
    if ttl < 0 {
        return CommandOutcome::reply(Value::error("ERR Invalid TTL value, must be >= 0"));
    }

    let payload = &args.args[2];
    if verify_payload(payload, skip_checksum, rdb_relaxed).is_err() {
        return CommandOutcome::reply(Value::error(
            "ERR DUMP payload version or checksum are wrong",
        ));
    }

    let body = &payload[..payload.len() - 10];
    let mut pos = 0usize;
    let type_byte = body.first().copied();
    let (obj, hash_field_expires) =
        match persistence::load_typed_object_with_hash_field_expires_detailed(body, &mut pos, None)
        {
            Ok(loaded) if pos == body.len() => loaded,
            Err(persistence::ObjectLoadError::DuplicateStreamConsumer) => {
                log::warn!("Duplicate stream consumer detected");
                return CommandOutcome::reply(Value::error("ERR Bad data format"));
            }
            Err(persistence::ObjectLoadError::DuplicateStreamConsumerPel) => {
                log::warn!("Duplicated consumer PEL entry");
                return CommandOutcome::reply(Value::error("ERR Bad data format"));
            }
            Ok(_) | Err(persistence::ObjectLoadError::Invalid) => {
                log_restore_integrity_failure(type_byte);
                return CommandOutcome::reply(Value::error("ERR Bad data format"));
            }
        };
    if obj.is_empty_rdb_key() {
        log_restore_integrity_failure(type_byte);
        return CommandOutcome::reply(Value::error("ERR Bad data format"));
    }

    if replace {
        db.del(key);
    }

    let abs_expire = if ttl == 0 {
        None
    } else if absttl {
        Some(ttl as u64)
    } else {
        Some(now_ms + ttl as u64)
    };

    if let Some(exp) = abs_expire {
        if exp <= now_ms {
            let mut effects = CommandEffects {
                skip_propagate: true,
                ..CommandEffects::default()
            };
            if replace {
                effects.changed_keys.push((db_idx, key.clone()));
                effects.events.push(CoreEvent {
                    kind: "del",
                    key: key.clone(),
                });
                effects.custom_propagations.push(vec![
                    Bytes::copy_from_slice(if lazyfree_server_del {
                        b"UNLINK"
                    } else {
                        b"DEL"
                    }),
                    key.clone(),
                ]);
            }
            return CommandOutcome {
                reply: Value::ok(),
                effects,
            };
        }
    }

    db.set(key.to_vec(), obj);
    db.set_hash_field_expires_map(key.to_vec(), hash_field_expires);
    if let Some(exp) = abs_expire {
        db.set_expire(key, exp);
    }

    if lru_idle >= 0 {
        db.set_lru_idle(key, lru_idle);
    }
    if lfu_freq >= 0 {
        db.set_lfu_freq(key, lfu_freq as u8);
    }

    let mut custom_propagations = Vec::new();
    if !absttl && ttl > 0 {
        let abs_ms = abs_expire.unwrap();
        let mut parts = Vec::with_capacity(6);
        parts.push(Bytes::from_static(b"RESTORE"));
        parts.push(key.clone());
        parts.push(abs_ms.to_string().into());
        parts.push(args.args[2].clone());
        if replace {
            parts.push(Bytes::from_static(b"REPLACE"));
        }
        parts.push(Bytes::from_static(b"ABSTTL"));
        custom_propagations.push(parts);
    }
    CommandOutcome {
        reply: Value::ok(),
        effects: CommandEffects {
            changed_keys: vec![(db_idx, key.clone())],
            events: vec![CoreEvent {
                kind: "restore",
                key: key.clone(),
            }],
            custom_propagations,
            ..CommandEffects::default()
        },
    }
}
fn verify_payload(p: &[u8], skip_checksum: bool, rdb_relaxed: bool) -> Result<(), ()> {
    if p.len() < 10 {
        return Err(());
    }
    if skip_checksum {
        return Ok(());
    }
    let footer = &p[p.len() - 10..];
    let rdbver = u16::from_le_bytes([footer[0], footer[1]]);
    // DUMP payloads from older Redis/Valkey versions are valid input for
    // RESTORE; only future versions are rejected before object decoding.
    if !rdb_relaxed && rdbver > persistence::RDB_VERSION {
        return Err(());
    }
    let crc = persistence::crc64(0, &p[..p.len() - 8]);
    if crc.to_le_bytes() != footer[2..10] {
        return Err(());
    }
    Ok(())
}

fn parse_i64(b: &[u8]) -> Option<i64> {
    std::str::from_utf8(b).ok()?.trim().parse().ok()
}

fn log_restore_integrity_failure(type_byte: Option<u8>) {
    match type_byte {
        Some(RDB_TYPE_HASH_ZIPMAP) => log::warn!("Zipmap integrity check failed"),
        Some(RDB_TYPE_ZSET_LISTPACK) => {
            log::warn!("NAN score detected while loading zset")
        }
        Some(RDB_TYPE_ZSET_ZIPLIST) => {
            log::warn!("Zset ziplist integrity check failed");
            log::warn!("NAN score detected while loading zset");
        }
        Some(RDB_TYPE_STREAM_LISTPACKS | RDB_TYPE_STREAM_LISTPACKS_2 | RDB_TYPE_STREAM_LISTPACKS_3) => {
            log::warn!("Stream listpack integrity check failed")
        }
        _ => log::warn!("DUMP payload integrity check failed"),
    }
}
