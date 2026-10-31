
use super::runtime_support::{
    hash_field_deletion_propagation, hash_field_expiry_propagation,
    hash_field_persist_propagation, NOT_INT,
};
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

fn compute_expire_ms(unit: &str, val: i64, now_ms: u64) -> Result<u64, &'static str> {
    if val < 0 {
        return Err("ERR invalid expire time in 'hgetex' command");
    }
    let now = now_ms as i64;
    let abs_ms = match unit.to_lowercase().as_str() {
        "ex" => val.checked_mul(1000).and_then(|d| d.checked_add(now)),
        "px" => val.checked_add(now),
        "exat" => val.checked_mul(1000),
        "pxat" => Some(val),
        _ => return Err("ERR invalid expire type"),
    };
    abs_ms
        .map(|v| v as u64)
        .ok_or("ERR invalid expire time in 'hgetex' command")
}

#[command("HGETEX", runtime)]
pub fn hgetex(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.args.len() < 3 {
        return CommandOutcome::reply(CommandError::wrong_arity("hgetex"));
    }
    let key = &args.args[0];
    let db_idx = runtime.selected_db();
    let now_ms = runtime.now_ms();

    let mut expire_type: Option<&str> = None;
    let mut expire_val: i64 = 0;
    let mut is_persist = false;
    let mut expire_set = false;
    let mut i = 1;
    while i < args.args.len() {
        let opt = match std::str::from_utf8(&args.args[i]) {
            Ok(s) => s,
            Err(_) => return CommandOutcome::reply(CommandError::syntax_error()),
        };
        if opt.eq_ignore_ascii_case("fields") {
            break;
        }
        if opt.eq_ignore_ascii_case("ex")
            || opt.eq_ignore_ascii_case("px")
            || opt.eq_ignore_ascii_case("exat")
            || opt.eq_ignore_ascii_case("pxat")
        {
            if expire_set {
                return CommandOutcome::reply(Value::error(
                    "ERR only one expire option (EX, PX, EXAT, PXAT, or PERSIST) is allowed",
                ));
            }
            expire_set = true;
            expire_type = Some(opt);
            if i + 1 >= args.args.len() {
                return CommandOutcome::reply(CommandError::syntax_error());
            }
            expire_val = match std::str::from_utf8(&args.args[i + 1])
                .ok()
                .and_then(|s| s.trim().parse::<i64>().ok())
            {
                Some(v) => v,
                None => return CommandOutcome::reply(Value::error(NOT_INT)),
            };
            i += 2;
        } else if opt.eq_ignore_ascii_case("persist") {
            if !is_persist {
                if expire_set {
                    return CommandOutcome::reply(Value::error(
                        "ERR only one expire option (EX, PX, EXAT, PXAT, or PERSIST) is allowed",
                    ));
                }
                expire_set = true;
                is_persist = true;
            }
            i += 1;
        } else {
            return CommandOutcome::reply(CommandError::syntax_error());
        }
    }

    if i >= args.args.len() {
        return CommandOutcome::reply(CommandError::syntax_error());
    }

    let fields_idx = i;
    if fields_idx + 1 >= args.args.len() {
        return CommandOutcome::reply(Value::error(
            "ERR numfields should be greater than 0 and match the provided number of fields",
        ));
    }
    let num_fields =
        match std::str::from_utf8(&args.args[fields_idx + 1])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
        {
            Some(n) if n > 0 => n,
            _ => return CommandOutcome::reply(Value::error(
                "ERR numfields should be greater than 0 and match the provided number of fields",
            )),
        };
    if fields_idx + 2 + num_fields != args.args.len() {
        return CommandOutcome::reply(Value::error(
            "ERR numfields should be greater than 0 and match the provided number of fields",
        ));
    }
    let fields_start = fields_idx + 2;

    let expire_ms = if is_persist {
        None
    } else if let Some(etype) = expire_type {
        match compute_expire_ms(etype, expire_val, now_ms) {
            Ok(ms) => Some(ms),
            Err(e) => return CommandOutcome::reply(Value::error(e)),
        }
    } else {
        None
    };
    let ignore_expiry = runtime.read_expired_values();
    let should_preserve_deadline = runtime.preserves_expired_deadline();

    let (result, any_expire, any_expired, hash_empty, expired_fields_count, changed_fields) = {
        let db = match runtime.db_mut() {
            Some(d) => d,
            None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
        };

        let mut any_expire = false;
        let mut any_expired = false;
        let mut hash_empty = false;
        let mut expired_fields_count = 0;
        let mut changed_fields = Vec::new();

        let mut expired_on_read = Vec::with_capacity(num_fields);
        for j in 0..num_fields {
            let field = &args.args[fields_start + j];
            let expired = !ignore_expiry && db.hash_field_expired_at(key, field, now_ms);
            expired_on_read.push(expired);
        }

        let result = match db.get_hash_object(key) {
            Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
            Ok(None) => {
                let mut out = Vec::with_capacity(num_fields);
                for _ in 0..num_fields {
                    out.push(Value::null());
                }
                return CommandOutcome::reply(Value::Array(out));
            }
            Ok(Some(hash)) => {
                let mut out = Vec::with_capacity(num_fields);
                for (j, expired) in expired_on_read.iter().copied().enumerate() {
                    let field = &args.args[fields_start + j];
                    match (!expired).then(|| hash.hash_get(field)).flatten() {
                        Some(value) => out.push(Value::bulk(value)),
                        None => out.push(Value::null()),
                    }
                }
                out
            }
        };

        for (j, expired_on_read) in expired_on_read.iter().copied().enumerate() {
            let field = &args.args[fields_start + j];
            let field_exists = !expired_on_read
                && match db.get_hash_object(key) {
                    Ok(Some(hash)) => hash.hash_contains(field),
                    _ => false,
                };

            if let Some(ms) = expire_ms {
                if !field_exists {
                    continue;
                }
                if ms > now_ms || should_preserve_deadline {
                    db.set_hash_field_expire(key, field, ms);
                    any_expire = true;
                    changed_fields.push(field.clone());
                } else {
                    if let Ok(Some(hash)) = db.get_hash_object_mut(key) {
                        if hash.hash_contains(field) {
                            hash.hash_del(field);
                            db.remove_hash_field_expire(key, field);
                            expired_fields_count += 1;
                            any_expired = true;
                            changed_fields.push(field.clone());
                        }
                    }
                }
            } else if is_persist {
                if !field_exists {
                    continue;
                }
                if db.remove_hash_field_expire(key, field) {
                    any_expire = true;
                    changed_fields.push(field.clone());
                }
            }
        }
        if any_expired {
            if let Ok(Some(hash)) = db.get_hash_object(key) {
                if hash.hash_is_empty() {
                    hash_empty = true;
                }
            }
        }

        (
            result,
            any_expire,
            any_expired,
            hash_empty,
            expired_fields_count,
            changed_fields,
        )
    };
    let propagation = if changed_fields.is_empty() {
        None
    } else if any_expired {
        Some(hash_field_deletion_propagation(key, &changed_fields))
    } else if is_persist {
        Some(hash_field_persist_propagation(key, &changed_fields))
    } else {
        expire_ms.map(|when_ms| hash_field_expiry_propagation(key, when_ms, &changed_fields))
    };
    let mut events = Vec::new();
    if any_expire {
        events.push(CoreEvent {
            kind: if is_persist { "hpersist" } else { "hexpire" },
            key: key.clone(),
        });
    }
    if any_expired {
        events.push(CoreEvent {
            kind: "hexpired",
            key: key.clone(),
        });
        if hash_empty {
            if let Some(d) = runtime.db_mut() {
                d.del(key);
            }
            events.push(CoreEvent {
                kind: "del",
                key: key.clone(),
            });
        }
    }
    CommandOutcome {
        reply: Value::Array(result),
        effects: CommandEffects {
            changed_keys: (!changed_fields.is_empty())
                .then(|| (db_idx, key.clone()))
                .into_iter()
                .collect(),
            events,
            skip_propagate: true,
            custom_propagations: propagation.into_iter().collect(),
            expired_hash_fields: expired_fields_count,
            ..CommandEffects::default()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::hgetex;
    use crate::engine::command::CommandArgs;
    use crate::framework::commands::command_runtime::{CommandRuntime, TestCommandRuntime};
    use bytes::Bytes;

    fn runtime_with_hash() -> TestCommandRuntime {
        let mut runtime = TestCommandRuntime::new(1, 1_000);
        runtime
            .db_mut()
            .unwrap()
            .get_or_create_hash(b"key")
            .unwrap()
            .insert(Bytes::from_static(b"f1"), Bytes::from_static(b"v1"));
        runtime
    }

    fn args(parts: &[&[u8]]) -> CommandArgs {
        CommandArgs::from_parts(
            parts
                .iter()
                .map(|part| Bytes::copy_from_slice(part))
                .collect(),
        )
    }

    #[test]
    fn future_expiry_propagates_canonical_hpexpireat() {
        let mut runtime = runtime_with_hash();
        let command = args(&[
            b"HGETEX",
            b"key",
            b"PXAT",
            b"4000000000000",
            b"FIELDS",
            b"2",
            b"f1",
            b"missing",
        ]);

        let outcome = hgetex(&mut runtime, &command);

        assert_eq!(
            outcome.effects.custom_propagations,
            vec![vec![
                Bytes::from_static(b"HPEXPIREAT"),
                Bytes::from_static(b"key"),
                Bytes::from_static(b"4000000000000"),
                Bytes::from_static(b"FIELDS"),
                Bytes::from_static(b"1"),
                Bytes::from_static(b"f1"),
            ]]
        );
    }

    #[test]
    fn immediate_expiry_propagates_hdel() {
        let mut runtime = runtime_with_hash();
        let command = args(&[b"HGETEX", b"key", b"EXAT", b"0", b"FIELDS", b"1", b"f1"]);

        let outcome = hgetex(&mut runtime, &command);

        assert_eq!(
            outcome.effects.custom_propagations,
            vec![vec![
                Bytes::from_static(b"HDEL"),
                Bytes::from_static(b"key"),
                Bytes::from_static(b"f1"),
            ]]
        );
    }

    #[test]
    fn read_only_form_retains_an_expired_last_field() {
        let mut runtime = runtime_with_hash();
        runtime.db_mut().unwrap().set_hash_field_expire(
            b"key",
            b"f1",
            999,
        );
        let command = args(&[b"HGETEX", b"key", b"FIELDS", b"1", b"f1"]);

        assert_eq!(
            hgetex(&mut runtime, &command).reply,
            crate::foundation::io::protocol::Value::Array(vec![
                crate::foundation::io::protocol::Value::Null,
            ])
        );
        assert!(runtime.dbs.get(0).unwrap().get_allow_expired(b"key").is_some());
        assert_eq!(runtime.dbs.get(0).unwrap().get_hash_field_expire(b"key", b"f1"), Some(999));
    }

    #[test]
    fn read_only_form_hides_but_retains_expired_field_when_active_expiry_is_disabled() {
        let mut runtime = runtime_with_hash();
        runtime.active_expire_enabled = false;
        runtime.db_mut().unwrap().set_hash_field_expire(
            b"key",
            b"f1",
            999,
        );
        let command = args(&[b"HGETEX", b"key", b"FIELDS", b"1", b"f1"]);

        assert_eq!(
            hgetex(&mut runtime, &command).reply,
            crate::foundation::io::protocol::Value::Array(vec![
                crate::foundation::io::protocol::Value::Null,
            ])
        );
        assert!(runtime.dbs.get(0).unwrap().get_allow_expired(b"key").is_some());
        assert!(runtime
            .dbs
            .get(0)
            .unwrap()
            .get_hash_field_expire(b"key", b"f1")
            .is_some());
    }
}
