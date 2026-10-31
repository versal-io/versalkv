use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::foundation::io::persistence;
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
    let should_preserve_deadline = runtime.preserves_expired_deadline();
    let should_suppress_expire = runtime.suppress_lazy_expire();
    let should_read_expired = runtime.read_expired_values();
    let now_ms = runtime.now_ms();
    let db_idx = runtime.selected_db();
    let (max_entries, max_value_size) = runtime.zset_encoding_limits();
    let (hash_max_entries, hash_max_value_size) = runtime.hash_encoding_limits();

    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };

    db.set_no_lazy_expire(should_suppress_expire);
    let has_existing = if should_read_expired {
        db.get_allow_expired(key).is_some()
    } else {
        db.exists_at(key, now_ms)
    };
    db.set_no_lazy_expire(false);
    if !replace && has_existing {
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
    let abs_expire = if ttl == 0 {
        None
    } else if absttl {
        Some(ttl as u64)
    } else {
        let Some(deadline) = i64::try_from(now_ms)
            .ok()
            .and_then(|base| base.checked_add(ttl))
        else {
            return CommandOutcome::reply(Value::error(
                "ERR invalid expire time in 'restore' command",
            ));
        };
        Some(deadline as u64)
    };

    let payload = &args.args[2];
    if verify_payload(payload, skip_checksum, rdb_relaxed).is_err() {
        return CommandOutcome::reply(Value::error(
            "ERR DUMP payload version or checksum are wrong",
        ));
    }

    let body = &payload[..payload.len() - 10];
    let mut pos = 0usize;
    let type_byte = body.first().copied();
    let (mut obj, hash_field_expires) =
        match persistence::load_typed_object_with_hash_limits_detailed(
            body, &mut pos, None, hash_max_entries, hash_max_value_size,
        )
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
    obj.zset_convert_with_limits(max_entries, max_value_size);

    let is_deleted = replace && db.del(key);

    if let Some(exp) = abs_expire {
        if exp <= now_ms && !should_preserve_deadline {
            let mut effects = CommandEffects {
                skip_propagate: true,
                ..CommandEffects::default()
            };
            if is_deleted {
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
    crate::foundation::io::store::structure::string::encoding::int_encodable(b)
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
        Some(
            RDB_TYPE_STREAM_LISTPACKS | RDB_TYPE_STREAM_LISTPACKS_2 | RDB_TYPE_STREAM_LISTPACKS_3,
        ) => {
            log::warn!("Stream listpack integrity check failed")
        }
        _ => log::warn!("DUMP payload integrity check failed"),
    }
}

#[cfg(test)]
mod tests {
    use super::restore;
    use crate::engine::core::Value;
    use crate::foundation::io::store::Robj;
    use crate::framework::commands::command_runtime::{CommandRuntime, TestCommandRuntime};
    use crate::framework::commands::generic::dump::serialize_object_with_hash_field_expires_with_compression;
    use crate::framework::CommandArgs;
    use bytes::Bytes;

    #[test]
    fn past_replace_signals_only_actual_deletion() {
        for has_existing in [false, true] {
            let mut runtime = TestCommandRuntime::new(1, 1_000);
            if has_existing {
                runtime
                    .db_mut()
                    .unwrap()
                    .set(b"key".to_vec(), Robj::string("original"));
            }
            let payload = serialize_object_with_hash_field_expires_with_compression(
                &Robj::string("value"),
                None,
                false,
            )
            .unwrap();
            let args = CommandArgs::from_parts(vec![
                Bytes::from_static(b"RESTORE"),
                Bytes::from_static(b"key"),
                Bytes::from_static(b"1"),
                Bytes::from(payload),
                Bytes::from_static(b"ABSTTL"),
                Bytes::from_static(b"REPLACE"),
            ]);
            let outcome = restore(&mut runtime, &args);
            assert_eq!(outcome.reply, Value::ok());
            assert_eq!(
                outcome.effects.changed_keys.len(),
                usize::from(has_existing)
            );
            assert_eq!(outcome.effects.events.len(), usize::from(has_existing));
            assert_eq!(
                outcome.effects.custom_propagations.len(),
                usize::from(has_existing)
            );
            if has_existing {
                assert_eq!(outcome.effects.events[0].kind, "del");
                assert_eq!(
                    outcome.effects.custom_propagations[0],
                    vec![Bytes::from_static(b"DEL"), Bytes::from_static(b"key")]
                );
            }
            assert!(runtime
                .db_mut()
                .unwrap()
                .get_allow_expired(b"key")
                .is_none());
            assert!(runtime.db_mut().unwrap().expired_deletions.is_empty());
        }
    }

    #[test]
    fn preserved_deadline_restores_the_raw_value() {
        let mut runtime = TestCommandRuntime::new(1, 1_000);
        runtime.read_expired_values = true;
        let payload = serialize_object_with_hash_field_expires_with_compression(
            &Robj::string("value"),
            None,
            false,
        )
        .unwrap();
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"RESTORE"),
            Bytes::from_static(b"key"),
            Bytes::from_static(b"1"),
            Bytes::from(payload),
            Bytes::from_static(b"ABSTTL"),
        ]);
        let outcome = restore(&mut runtime, &args);
        assert_eq!(outcome.reply, Value::ok());
        assert_eq!(
            runtime
                .db_mut()
                .unwrap()
                .get_allow_expired(b"key")
                .unwrap()
                .as_bytes(),
            b"value"
        );
        assert_eq!(runtime.db_mut().unwrap().get_expire(b"key"), Some(1));
        assert_eq!(outcome.effects.events[0].kind, "restore");
        assert!(runtime.db_mut().unwrap().expired_deletions.is_empty());
    }

    #[test]
    fn relative_deadline_overflow_precedes_payload_validation_and_replacement() {
        let payload = serialize_object_with_hash_field_expires_with_compression(
            &Robj::string("replacement"),
            None,
            false,
        )
        .unwrap();
        for payload in [payload, b"invalid".to_vec()] {
            let mut runtime = TestCommandRuntime::new(1, 1_000);
            let db = runtime.db_mut().unwrap();
            db.set(b"key".to_vec(), Robj::string("retained"));
            db.set_expire(b"key", 20_000);
            let args = CommandArgs::from_parts(vec![
                Bytes::from_static(b"RESTORE"),
                Bytes::from_static(b"key"),
                Bytes::from(i64::MAX.to_string()),
                Bytes::from(payload),
                Bytes::from_static(b"REPLACE"),
            ]);

            let outcome = restore(&mut runtime, &args);

            assert_eq!(
                outcome.reply,
                Value::error("ERR invalid expire time in 'restore' command")
            );
            let db = runtime.db_mut().unwrap();
            assert_eq!(
                db.get_allow_expired(b"key").unwrap().as_bytes(),
                b"retained"
            );
            assert_eq!(db.get_expire(b"key"), Some(20_000));
            assert!(outcome.effects.changed_keys.is_empty());
        }
    }

    #[test]
    fn relative_and_absolute_deadlines_accept_the_signed_upper_boundary() {
        for (ttl, options, expected_deadline) in [
            (0, vec![], None),
            (i64::MAX - 1_000, vec![], Some(i64::MAX as u64)),
            (i64::MAX, vec!["ABSTTL"], Some(i64::MAX as u64)),
        ] {
            let mut runtime = TestCommandRuntime::new(1, 1_000);
            let payload = serialize_object_with_hash_field_expires_with_compression(
                &Robj::string("value"),
                None,
                false,
            )
            .unwrap();
            let mut parts = vec![
                Bytes::from_static(b"RESTORE"),
                Bytes::from_static(b"key"),
                Bytes::from(ttl.to_string()),
                Bytes::from(payload),
            ];
            parts.extend(options.into_iter().map(Bytes::from));

            let outcome = restore(&mut runtime, &CommandArgs::from_parts(parts));

            assert_eq!(outcome.reply, Value::ok());
            let db = runtime.db_mut().unwrap();
            assert_eq!(db.get(b"key").unwrap().as_bytes(), b"value");
            assert_eq!(db.get_expire(b"key"), expected_deadline);
        }
    }
}
