use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::foundation::io::store::Robj;
use crate::framework::commands::command_runtime::CommandRuntime;
use bytes::Bytes;
use command_macros::command;

#[command(
    "SET",
    runtime,
    get_keys = crate::framework::commands::key_extractors::set_get_keys
)]
pub fn set(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let [key, value, options @ ..] = args.args.as_slice() else {
        return CommandOutcome::reply(CommandError::wrong_arity("set"));
    };
    let now_ms = runtime.now_ms();
    let db_index = runtime.selected_db();
    let should_suppress_expire = runtime.suppress_lazy_expire();
    let should_preserve_deadline = runtime.preserves_expired_deadline();
    let read_expired_values = runtime.read_expired_values();
    let should_mark_replica_local_expire = runtime.should_mark_replica_local_expire();
    let opts = match parse_set_options(options, now_ms) {
        Ok(options) => options,
        Err(reply) => return CommandOutcome::reply(reply),
    };
    let key = key.clone();
    let value = Robj::string(value.clone());
    let old_value = {
        let Some(db) = runtime.db_mut() else {
            return CommandOutcome::reply(CommandError::db_index_out_of_range());
        };
        db.set_no_lazy_expire(should_suppress_expire);
        let old = if read_expired_values {
            db.get_allow_expired(&key).cloned()
        } else {
            db.get_at(&key, now_ms).cloned()
        };
        db.set_no_lazy_expire(false);
        old
    };
    if opts.get && old_value.as_ref().is_some_and(|object| !object.is_string()) {
        return CommandOutcome::reply(CommandError::wrong_type());
    }
    let should_set = match opts.condition {
        ConditionOption::None => true,
        ConditionOption::Nx => old_value.is_none(),
        ConditionOption::Xx => old_value.is_some(),
        ConditionOption::IfEq | ConditionOption::IfNe => match &old_value {
            Some(object) if object.is_string() => {
                let is_equal =
                    object.as_bytes() == opts.comparison_value.as_deref().unwrap_or_default();
                is_equal == matches!(opts.condition, ConditionOption::IfEq)
            }
            Some(_) => return CommandOutcome::reply(CommandError::wrong_type()),
            None => matches!(opts.condition, ConditionOption::IfNe),
        },
    };
    if !should_set {
        return CommandOutcome {
            reply: conditional_set_reply(opts.get, old_value),
            effects: CommandEffects {
                skip_propagate: true,
                ..CommandEffects::default()
            },
        };
    }
    if let ExpireOption::At(deadline_ms) = opts.expire {
        if deadline_ms <= now_ms as i64 && !should_preserve_deadline {
            let Some(db) = runtime.db_mut() else {
                return CommandOutcome::reply(CommandError::db_index_out_of_range());
            };
            if db.del(&key) {
                db.expired_deletions.push(key.to_vec());
            }
            return CommandOutcome {
                reply: get_set_reply(opts.get, old_value),
                effects: CommandEffects {
                    skip_propagate: true,
                    ..CommandEffects::default()
                },
            };
        }
    }
    let mut effects = CommandEffects {
        changed_keys: vec![(db_index, key.clone())],
        events: vec![CoreEvent {
            kind: "set",
            key: key.clone(),
        }],
        ..CommandEffects::default()
    };
    if old_value.is_none() {
        effects.events.push(CoreEvent {
            kind: "new",
            key: key.clone(),
        });
    }
    if let ExpireOption::At(deadline_ms) = opts.expire {
        effects.events.push(CoreEvent {
            kind: "expire",
            key: key.clone(),
        });
        if opts.expire_needs_propagation_conversion {
            effects.custom_propagations.push(vec![
                Bytes::from_static(b"SET"),
                key.clone(),
                Bytes::copy_from_slice(value.as_bytes()),
                Bytes::from_static(b"PXAT"),
                deadline_ms.to_string().into(),
            ]);
        }
    }
    {
        let Some(db) = runtime.db_mut() else {
            return CommandOutcome::reply(CommandError::db_index_out_of_range());
        };
        match opts.expire {
            ExpireOption::None => db.set(key.clone().to_vec(), value),
            ExpireOption::KeepTtl => db.set_keep_ttl(key.clone().to_vec(), value),
            ExpireOption::At(deadline_ms) => {
                db.set(key.clone().to_vec(), value);
                db.set_expire(&key, deadline_ms as u64);
                if should_mark_replica_local_expire {
                    db.mark_replica_local_expire(&key);
                }
            }
        }
    }
    CommandOutcome {
        reply: get_set_reply(opts.get, old_value),
        effects,
    }
}

enum ExpireOption {
    None,
    KeepTtl,
    At(i64),
}
enum ConditionOption {
    None,
    Nx,
    Xx,
    IfEq,
    IfNe,
}
struct SetOptions {
    expire: ExpireOption,
    expire_needs_propagation_conversion: bool,
    condition: ConditionOption,
    get: bool,
    comparison_value: Option<Vec<u8>>,
}

fn parse_set_options(options: &[Bytes], now_ms: u64) -> Result<SetOptions, Value> {
    let mut parsed = SetOptions {
        expire: ExpireOption::None,
        expire_needs_propagation_conversion: false,
        condition: ConditionOption::None,
        get: false,
        comparison_value: None,
    };
    let mut expiry_mode: Option<&[u8]> = None;
    let mut timeout = None;
    let mut index = 0;
    while index < options.len() {
        let option = String::from_utf8_lossy(&options[index]).to_ascii_uppercase();
        match option.as_str() {
            "KEEPTTL" => {
                if expiry_mode.is_some_and(|mode| !mode.eq_ignore_ascii_case(b"KEEPTTL")) {
                    return Err(CommandError::syntax_error());
                }
                expiry_mode = Some(&options[index]);
                parsed.expire = ExpireOption::KeepTtl;
                index += 1;
            }
            "EX" | "PX" | "EXAT" | "PXAT" => {
                if expiry_mode.is_some_and(|mode| !mode.eq_ignore_ascii_case(&options[index]))
                    || index + 1 >= options.len()
                {
                    return Err(CommandError::syntax_error());
                }
                expiry_mode = Some(&options[index]);
                timeout = Some(&options[index + 1]);
                parsed.expire_needs_propagation_conversion = option != "PXAT";
                index += 2;
            }
            "NX" | "XX" => {
                if !matches!(parsed.condition, ConditionOption::None)
                    && !matches!(
                        (&parsed.condition, option.as_str()),
                        (ConditionOption::Nx, "NX") | (ConditionOption::Xx, "XX")
                    )
                {
                    return Err(CommandError::syntax_error());
                }
                parsed.condition = if option == "NX" {
                    ConditionOption::Nx
                } else {
                    ConditionOption::Xx
                };
                index += 1;
            }
            "GET" => {
                parsed.get = true;
                index += 1;
            }
            "IFEQ" | "IFNE" => {
                if !matches!(parsed.condition, ConditionOption::None) || index + 1 >= options.len()
                {
                    return Err(CommandError::syntax_error());
                }
                parsed.condition = if option == "IFEQ" {
                    ConditionOption::IfEq
                } else {
                    ConditionOption::IfNe
                };
                parsed.comparison_value = Some(options[index + 1].to_vec());
                index += 2;
            }
            _ => return Err(CommandError::syntax_error()),
        }
    }
    if let (Some(mode), Some(timeout)) = (expiry_mode, timeout) {
        let value =
            crate::foundation::io::store::structure::string::encoding::int_encodable(timeout)
                .ok_or_else(|| Value::error("ERR value is not an integer or out of range"))?;
        if value <= 0 {
            return Err(invalid_expire());
        }
        let value_ms = if mode.eq_ignore_ascii_case(b"EX") || mode.eq_ignore_ascii_case(b"EXAT") {
            value.checked_mul(1_000).ok_or_else(invalid_expire)?
        } else {
            value
        };
        let deadline_ms = if mode.eq_ignore_ascii_case(b"EX") || mode.eq_ignore_ascii_case(b"PX") {
            i64::try_from(now_ms)
                .ok()
                .and_then(|base| base.checked_add(value_ms))
                .ok_or_else(invalid_expire)?
        } else {
            value_ms
        };
        parsed.expire = ExpireOption::At(deadline_ms);
    }
    Ok(parsed)
}
fn get_set_reply(get: bool, old_value: Option<Robj>) -> Value {
    if !get {
        return Value::ok();
    }
    match old_value {
        Some(value) => Value::bulk(value.as_bytes().to_vec()),
        None => Value::null(),
    }
}
fn conditional_set_reply(get: bool, old_value: Option<Robj>) -> Value {
    if get {
        get_set_reply(true, old_value)
    } else {
        Value::null()
    }
}
fn invalid_expire() -> Value {
    Value::error("ERR invalid expire time in 'set' command")
}

#[cfg(test)]
mod tests {
    use super::set;
    use crate::engine::core::Value;
    use crate::foundation::io::store::Robj;
    use crate::framework::commands::command_runtime::{CommandRuntime, TestCommandRuntime};
    use crate::framework::CommandArgs;
    use bytes::Bytes;

    fn request(options: &[&str]) -> CommandArgs {
        CommandArgs::from_parts(
            ["SET", "key", "replacement"]
                .into_iter()
                .chain(options.iter().copied())
                .map(|part| Bytes::copy_from_slice(part.as_bytes()))
                .collect(),
        )
    }

    #[test]
    fn conditions_precede_past_deadline_deletion() {
        for options in [
            vec!["IFEQ", "mismatch", "PXAT", "1"],
            vec!["IFNE", "original", "PXAT", "1"],
            vec!["NX", "PXAT", "1"],
        ] {
            let mut runtime = TestCommandRuntime::new(1, 1_000);
            runtime
                .db_mut()
                .unwrap()
                .set(b"key".to_vec(), Robj::string("original"));
            let outcome = set(&mut runtime, &request(&options));
            assert_eq!(outcome.reply, Value::null(), "{options:?}");
            assert_eq!(
                runtime
                    .db_mut()
                    .unwrap()
                    .get_allow_expired(b"key")
                    .unwrap()
                    .as_bytes(),
                b"original"
            );
            assert_eq!(runtime.db_mut().unwrap().get_expire(b"key"), None);
            assert!(runtime.db_mut().unwrap().expired_deletions.is_empty());
            assert!(outcome.effects.changed_keys.is_empty());
            assert!(outcome.effects.events.is_empty());
            assert!(outcome.effects.custom_propagations.is_empty());
        }
        for (condition, expected) in [("NX", Value::ok()), ("XX", Value::null())] {
            let mut runtime = TestCommandRuntime::new(1, 1_000);
            let outcome = set(&mut runtime, &request(&[condition, "PXAT", "1"]));
            assert_eq!(outcome.reply, expected);
            assert!(runtime
                .db_mut()
                .unwrap()
                .get_allow_expired(b"key")
                .is_none());
            assert!(outcome.effects.custom_propagations.is_empty());
        }
    }

    #[test]
    fn successful_past_deadline_uses_only_expired_deletions() {
        let mut runtime = TestCommandRuntime::new(1, 1_000);
        runtime
            .db_mut()
            .unwrap()
            .set(b"key".to_vec(), Robj::string("original"));
        let outcome = set(&mut runtime, &request(&["GET", "PXAT", "1"]));
        assert_eq!(outcome.reply, Value::bulk("original"));
        assert!(outcome.effects.skip_propagate);
        assert!(outcome.effects.changed_keys.is_empty());
        assert!(outcome.effects.events.is_empty());
        assert!(outcome.effects.custom_propagations.is_empty());
        assert_eq!(
            runtime.db_mut().unwrap().expired_deletions,
            vec![b"key".to_vec()]
        );
    }

    #[test]
    fn repeated_deadlines_validate_only_the_final_value_after_syntax() {
        for (options, expected, deadline) in [
            (
                vec!["PX", "bogus", "PX", "60000"],
                Value::ok(),
                Some(61_000),
            ),
            (
                vec!["PXAT", "0", "PXAT", "4000000000000"],
                Value::ok(),
                Some(4_000_000_000_000),
            ),
            (
                vec!["PX", "+1", "invalid"],
                Value::error("ERR syntax error"),
                None,
            ),
            (
                vec!["PX", "bogus", "EX", "1"],
                Value::error("ERR syntax error"),
                None,
            ),
            (
                vec!["PX", "+1"],
                Value::error("ERR value is not an integer or out of range"),
                None,
            ),
            (vec!["KEEPTTL", "KEEPTTL"], Value::ok(), None),
            (vec!["NX", "NX"], Value::ok(), None),
        ] {
            let mut runtime = TestCommandRuntime::new(1, 1_000);
            let outcome = set(&mut runtime, &request(&options));
            assert_eq!(outcome.reply, expected, "{options:?}");
            assert_eq!(runtime.db_mut().unwrap().get_expire(b"key"), deadline);
            if matches!(expected, Value::Error(_)) {
                assert!(runtime
                    .db_mut()
                    .unwrap()
                    .get_allow_expired(b"key")
                    .is_none());
            }
        }
    }

    #[test]
    fn retained_past_deadline_is_written_and_signalled() {
        let mut runtime = TestCommandRuntime::new(1, 1_000);
        runtime.read_expired_values = true;
        let outcome = set(&mut runtime, &request(&["PXAT", "1"]));
        assert_eq!(outcome.reply, Value::ok());
        assert_eq!(
            runtime
                .db_mut()
                .unwrap()
                .get_allow_expired(b"key")
                .unwrap()
                .as_bytes(),
            b"replacement"
        );
        assert_eq!(runtime.db_mut().unwrap().get_expire(b"key"), Some(1));
        assert!(runtime.db_mut().unwrap().expired_deletions.is_empty());
        assert_eq!(
            outcome
                .effects
                .events
                .iter()
                .map(|event| event.kind)
                .collect::<Vec<_>>(),
            vec!["set", "new", "expire"]
        );
    }
}
