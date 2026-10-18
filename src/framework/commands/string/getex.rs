use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[command("GETEX", runtime)]
pub fn getex(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let key = &args.args[0];
    let db_idx = runtime.selected_db();
    let now_ms = runtime.now_ms();
    let mut effects = CommandEffects {
        skip_propagate: true,
        ..CommandEffects::default()
    };
    let mut has_write = false;
    let mut notify_expire = false;
    let mut i = 1;
    while i < args.args.len() {
        let opt = String::from_utf8_lossy(&args.args[i]).to_ascii_uppercase();
        match opt.as_str() {
            "EX" | "PX" | "EXAT" | "PXAT" => {
                if i + 1 >= args.args.len() {
                    return CommandOutcome::reply(CommandError::syntax_error());
                }
                let raw = &args.args[i + 1];
                let n = match std::str::from_utf8(raw)
                    .ok()
                    .and_then(|s| s.trim().parse::<i64>().ok())
                {
                    Some(v) => v,
                    None => {
                        return CommandOutcome::reply(Value::error(
                            "ERR value is not an integer or out of range",
                        ))
                    }
                };
                let abs_ms = match opt.as_str() {
                    "EX" => {
                        if n <= 0 {
                            return CommandOutcome::reply(Value::error(
                                "ERR invalid expire time in 'getex' command",
                            ));
                        }
                        n.checked_mul(1000)
                            .and_then(|d| d.checked_add(now_ms as i64))
                    }
                    "PX" => {
                        if n <= 0 {
                            return CommandOutcome::reply(Value::error(
                                "ERR invalid expire time in 'getex' command",
                            ));
                        }
                        n.checked_add(now_ms as i64)
                    }
                    "EXAT" => {
                        if n <= 0 {
                            return CommandOutcome::reply(Value::error(
                                "ERR invalid expire time in 'getex' command",
                            ));
                        }
                        n.checked_mul(1000)
                    }
                    _ => {
                        if n <= 0 {
                            return CommandOutcome::reply(Value::error(
                                "ERR invalid expire time in 'getex' command",
                            ));
                        }
                        Some(n)
                    }
                };
                let abs_ms = match abs_ms {
                    Some(v) => v,
                    None => {
                        return CommandOutcome::reply(Value::error(
                            "ERR invalid expire time in 'getex' command",
                        ))
                    }
                };
                {
                    let db = match runtime.db_mut() {
                        Some(d) => d,
                        None => {
                            return CommandOutcome::reply(CommandError::db_index_out_of_range())
                        }
                    };
                    if abs_ms <= now_ms as i64 {
                        db.del(key);
                        db.expired_deletions.push(key.to_vec());
                        effects.custom_propagations.clear();
                    } else {
                        db.set_expire(key, abs_ms as u64);
                        effects.custom_propagations.push(vec![
                            Bytes::from_static(b"PEXPIREAT"),
                            key.clone(),
                            abs_ms.to_string().into(),
                        ]);
                    }
                }
                notify_expire = true;
                has_write = true;
                i += 2;
            }
            "PERSIST" => {
                {
                    let db = match runtime.db_mut() {
                        Some(d) => d,
                        None => {
                            return CommandOutcome::reply(CommandError::db_index_out_of_range())
                        }
                    };
                    db.persist(key);
                }
                effects
                    .custom_propagations
                    .push(vec![Bytes::from_static(b"PERSIST"), key.clone()]);
                has_write = true;
                i += 1;
            }
            _ => return CommandOutcome::reply(CommandError::syntax_error()),
        }
    }

    let obj = {
        let db = match runtime.db_mut() {
            Some(d) => d,
            None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
        };
        match db.get(key) {
            Some(obj) => {
                if !obj.is_string() {
                    return CommandOutcome::reply(CommandError::wrong_type());
                }
                Some(obj.clone())
            }
            None => None,
        }
    };

    if obj.is_none() {
        effects.changed_keys.clear();
        effects.events.clear();
        effects.custom_propagations.clear();
        effects.skip_propagate = true;
        return CommandOutcome {
            reply: Value::null(),
            effects,
        };
    }

    if notify_expire {
        effects.events.push(CoreEvent {
            kind: "expire",
            key: key.clone(),
        });
    }

    if has_write {
        effects.skip_propagate = false;
        effects.changed_keys.push((db_idx, key.clone()));
    }

    CommandOutcome {
        reply: Value::bulk(
            obj.expect("GETEX value existence was checked above")
                .as_bytes()
                .to_vec(),
        ),
        effects,
    }
}

#[cfg(test)]
mod tests {
    use super::getex;
    use crate::engine::command::CommandArgs;
    use crate::engine::core::Value;
    use crate::foundation::io::store::Robj;
    use crate::framework::commands::command_runtime::{CommandRuntime, TestCommandRuntime};
    use bytes::Bytes;

    #[test]
    fn immediate_expiry_uses_only_the_expired_deletion_pipeline() {
        let mut runtime = TestCommandRuntime::new(1, 1_000);
        runtime
            .db_mut()
            .unwrap()
            .set(b"key".to_vec(), Robj::string("value"));
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"GETEX"),
            Bytes::from_static(b"key"),
            Bytes::from_static(b"PXAT"),
            Bytes::from_static(b"999"),
        ]);

        let outcome = getex(&mut runtime, &args);

        assert_eq!(outcome.reply, Value::null());
        assert!(outcome.effects.skip_propagate);
        assert!(outcome.effects.changed_keys.is_empty());
        assert!(outcome.effects.custom_propagations.is_empty());
        assert_eq!(
            runtime.db_mut().unwrap().expired_deletions,
            vec![b"key".to_vec()]
        );
    }
}
