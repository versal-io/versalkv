use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use command_macros::command;

#[command("EXPIRE", runtime)]
pub fn expire(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    expire_impl(runtime, args, 1_000, false, "expire")
}

pub(crate) fn expire_impl(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
    unit_ms: i64,
    absolute: bool,
    name: &'static str,
) -> CommandOutcome {
    let Some((key, rest)) = args.args.split_first() else {
        return CommandOutcome::reply(CommandError::wrong_arity(name));
    };
    let Some((timeout, flags)) = rest.split_first() else {
        return CommandOutcome::reply(CommandError::wrong_arity(name));
    };
    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;
    for flag in flags {
        if flag.eq_ignore_ascii_case(b"nx") {
            nx = true;
        } else if flag.eq_ignore_ascii_case(b"xx") {
            xx = true;
        } else if flag.eq_ignore_ascii_case(b"gt") {
            gt = true;
        } else if flag.eq_ignore_ascii_case(b"lt") {
            lt = true;
        } else {
            return CommandOutcome::reply(Value::error(format!(
                "ERR Unsupported option {}",
                String::from_utf8_lossy(flag)
            )));
        }
    }
    if nx && (xx || gt || lt) {
        return CommandOutcome::reply(Value::error(
            "ERR NX and XX, GT or LT options at the same time are not compatible",
        ));
    }
    if gt && lt {
        return CommandOutcome::reply(Value::error(
            "ERR GT and LT options at the same time are not compatible",
        ));
    }
    let Some(timeout) =
        crate::foundation::io::store::structure::string::encoding::int_encodable(timeout)
    else {
        return CommandOutcome::reply(Value::error("ERR value is not an integer or out of range"));
    };
    let now_ms = runtime.now_ms();
    let Some(scaled_timeout) = timeout.checked_mul(unit_ms) else {
        return CommandOutcome::reply(Value::error(format!(
            "ERR invalid expire time in '{}' command",
            name
        )));
    };
    let Some(deadline) = (if absolute {
        Some(scaled_timeout)
    } else {
        (now_ms as i64).checked_add(scaled_timeout)
    }) else {
        return CommandOutcome::reply(Value::error(format!(
            "ERR invalid expire time in '{}' command",
            name
        )));
    };
    let deadline = deadline.max(0);
    let db_index = runtime.selected_db();
    let should_suppress_expire = runtime.suppress_lazy_expire();
    let should_preserve_deadline = runtime.preserves_expired_deadline();
    let should_read_expired = runtime.read_expired_values();
    let mark_local_expire = runtime.should_mark_replica_local_expire();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    db.set_no_lazy_expire(should_suppress_expire);
    let has_existing = if should_read_expired {
        db.get_allow_expired(key).is_some()
    } else {
        db.exists_at(key, now_ms)
    };
    if !has_existing {
        db.set_no_lazy_expire(false);
        return CommandOutcome::reply(Value::integer(0));
    }
    let current = db.get_expire(key);
    let allowed = if nx {
        current.is_none()
    } else if xx && current.is_none() {
        false
    } else if gt {
        current.is_some_and(|current| deadline > current as i64)
    } else if lt {
        current.is_none_or(|current| deadline < current as i64)
    } else {
        true
    };
    if !allowed {
        db.set_no_lazy_expire(false);
        return CommandOutcome::reply(Value::integer(0));
    }
    db.set_no_lazy_expire(false);
    if deadline <= now_ms as i64 && !should_preserve_deadline {
        if db.del(key) {
            db.expired_deletions.push(key.to_vec());
        }
        return CommandOutcome {
            reply: Value::integer(1),
            effects: CommandEffects {
                skip_propagate: true,
                ..CommandEffects::default()
            },
        };
    }
    db.set_expire(key, deadline as u64);
    if mark_local_expire {
        db.mark_replica_local_expire(key);
    }
    let mut effects = CommandEffects {
        changed_keys: vec![(db_index, key.clone())],
        events: vec![CoreEvent {
            kind: "expire",
            key: key.clone(),
        }],
        ..CommandEffects::default()
    };
    let mut propagation = vec![
        bytes::Bytes::from_static(b"PEXPIREAT"),
        key.clone(),
        bytes::Bytes::from(deadline.to_string()),
    ];
    propagation.extend_from_slice(flags);
    effects.custom_propagations.push(propagation);
    CommandOutcome {
        reply: Value::integer(1),
        effects,
    }
}

#[cfg(test)]
mod tests {
    use super::expire_impl;
    use crate::engine::core::Value;
    use crate::foundation::io::store::Robj;
    use crate::framework::commands::command_runtime::{CommandRuntime, TestCommandRuntime};
    use crate::framework::CommandArgs;
    use bytes::Bytes;

    #[test]
    fn option_errors_precede_integer_validation() {
        for (parts, expected) in [
            (
                vec!["PEXPIRE", "key", "+1", "invalid"],
                "ERR Unsupported option invalid",
            ),
            (
                vec!["PEXPIRE", "key", "bogus", "NX", "XX"],
                "ERR NX and XX, GT or LT options at the same time are not compatible",
            ),
            (
                vec!["PEXPIRE", "key", "+1"],
                "ERR value is not an integer or out of range",
            ),
        ] {
            let mut runtime = TestCommandRuntime::new(1, 1_000);
            let args = CommandArgs::from_parts(parts.into_iter().map(Bytes::from).collect());
            let outcome = expire_impl(&mut runtime, &args, 1, false, "pexpire");
            assert_eq!(outcome.reply, Value::error(expected));
            assert!(outcome.effects.changed_keys.is_empty());
        }
    }

    #[test]
    fn preserved_negative_deadline_clamps_before_comparison() {
        let mut runtime = TestCommandRuntime::new(1, 1_000);
        runtime.read_expired_values = true;
        runtime
            .db_mut()
            .unwrap()
            .set(b"key".to_vec(), Robj::string("value"));
        runtime.db_mut().unwrap().set_expire(b"key", 0);
        for (options, expected) in [(vec![], 1), (vec!["LT"], 0), (vec!["GT"], 0)] {
            let args = CommandArgs::from_parts(
                ["PEXPIRE", "key", "-2000"]
                    .into_iter()
                    .chain(options)
                    .map(Bytes::from)
                    .collect(),
            );
            let outcome = expire_impl(&mut runtime, &args, 1, false, "pexpire");
            assert_eq!(outcome.reply, Value::integer(expected));
            assert_eq!(runtime.db_mut().unwrap().get_expire(b"key"), Some(0));
            assert!(runtime.db_mut().unwrap().expired_deletions.is_empty());
            if expected == 1 {
                assert_eq!(outcome.effects.events[0].kind, "expire");
                assert_eq!(
                    outcome.effects.custom_propagations,
                    vec![vec![
                        Bytes::from_static(b"PEXPIREAT"),
                        Bytes::from_static(b"key"),
                        Bytes::from_static(b"0"),
                    ]]
                );
            } else {
                assert!(outcome.effects.events.is_empty());
            }
        }
    }

    #[test]
    fn primary_past_deadline_uses_only_expired_deletions() {
        let mut runtime = TestCommandRuntime::new(1, 1_000);
        runtime
            .db_mut()
            .unwrap()
            .set(b"key".to_vec(), Robj::string("value"));
        let args = CommandArgs::from_parts(
            ["PEXPIRE", "key", "-1"]
                .into_iter()
                .map(Bytes::from)
                .collect(),
        );
        let outcome = expire_impl(&mut runtime, &args, 1, false, "pexpire");
        assert_eq!(outcome.reply, Value::integer(1));
        assert!(outcome.effects.skip_propagate);
        assert!(outcome.effects.changed_keys.is_empty());
        assert!(outcome.effects.events.is_empty());
        assert!(outcome.effects.custom_propagations.is_empty());
        assert_eq!(
            runtime.db_mut().unwrap().expired_deletions,
            vec![b"key".to_vec()]
        );
    }
}
