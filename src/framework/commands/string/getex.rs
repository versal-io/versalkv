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
    let option = match parse_getex_options(&args.args[1..], now_ms) {
        Ok(option) => option,
        Err(error) => return CommandOutcome::reply(error),
    };
    let should_preserve_deadline = runtime.preserves_expired_deadline();
    let should_read_expired = runtime.read_expired_values();
    let should_suppress_expire = runtime.suppress_lazy_expire();
    let should_mark_local_expire = runtime.should_mark_replica_local_expire();
    let db = match runtime.db_mut() {
        Some(db) => db,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    db.set_no_lazy_expire(should_suppress_expire);
    let object = if should_read_expired {
        db.get_allow_expired(key)
    } else {
        db.get_at(key, now_ms)
    };
    let reply = match object {
        Some(object) if object.is_string() => Value::bulk(object.as_bytes().to_vec()),
        Some(_) => CommandError::wrong_type(),
        None => Value::null(),
    };
    db.set_no_lazy_expire(false);
    if matches!(reply, Value::Error(_) | Value::Null) {
        return CommandOutcome::reply(reply);
    }
    let mut effects = CommandEffects {
        skip_propagate: true,
        ..CommandEffects::default()
    };
    let event = match option {
        GetExOption::None => None,
        GetExOption::At(deadline_ms) if deadline_ms <= now_ms && !should_preserve_deadline => {
            db.del(key);
            db.expired_deletions.push(key.to_vec());
            None
        }
        GetExOption::At(deadline_ms) => {
            db.set_expire(key, deadline_ms);
            if should_mark_local_expire {
                db.mark_replica_local_expire(key);
            }
            effects.custom_propagations.push(vec![
                Bytes::from_static(b"PEXPIREAT"),
                key.clone(),
                Bytes::from(deadline_ms.to_string()),
            ]);
            Some("expire")
        }
        GetExOption::Persist if db.persist(key) => {
            effects
                .custom_propagations
                .push(vec![Bytes::from_static(b"PERSIST"), key.clone()]);
            Some("persist")
        }
        GetExOption::Persist => None,
    };
    if let Some(kind) = event {
        effects.changed_keys.push((db_idx, key.clone()));
        effects.events.push(CoreEvent {
            kind,
            key: key.clone(),
        });
    }
    CommandOutcome { reply, effects }
}

enum GetExOption {
    None,
    Persist,
    At(u64),
}

fn parse_getex_options(options: &[Bytes], now_ms: u64) -> Result<GetExOption, Value> {
    let mut mode: Option<&[u8]> = None;
    let mut timeout = None;
    let mut index = 0;
    while index < options.len() {
        let option = options[index].as_ref();
        let is_persist = option.eq_ignore_ascii_case(b"PERSIST");
        let is_expiry = [b"EX".as_slice(), b"PX", b"EXAT", b"PXAT"]
            .iter()
            .any(|candidate| option.eq_ignore_ascii_case(candidate));
        if !is_persist && !is_expiry {
            return Err(CommandError::syntax_error());
        }
        if mode.is_some_and(|previous| !previous.eq_ignore_ascii_case(option)) {
            return Err(CommandError::syntax_error());
        }
        mode = Some(option);
        index += 1;
        if is_expiry {
            timeout = Some(options.get(index).ok_or_else(CommandError::syntax_error)?);
            index += 1;
        }
    }
    let Some(mode) = mode else {
        return Ok(GetExOption::None);
    };
    let Some(timeout) = timeout else {
        return Ok(GetExOption::Persist);
    };
    let value = crate::foundation::io::store::structure::string::encoding::int_encodable(timeout)
        .ok_or_else(|| Value::error("ERR value is not an integer or out of range"))?;
    let invalid_expire = || Value::error("ERR invalid expire time in 'getex' command");
    if value <= 0 {
        return Err(invalid_expire());
    }
    let is_seconds = mode.eq_ignore_ascii_case(b"EX") || mode.eq_ignore_ascii_case(b"EXAT");
    let value_ms = if is_seconds {
        value.checked_mul(1_000).ok_or_else(invalid_expire)?
    } else {
        value
    };
    let deadline = if mode.eq_ignore_ascii_case(b"EX") || mode.eq_ignore_ascii_case(b"PX") {
        i64::try_from(now_ms)
            .ok()
            .and_then(|base| base.checked_add(value_ms))
            .ok_or_else(invalid_expire)?
    } else {
        value_ms
    };
    Ok(GetExOption::At(deadline as u64))
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

        assert_eq!(outcome.reply, Value::bulk("value"));
        assert!(outcome.effects.skip_propagate);
        assert!(outcome.effects.changed_keys.is_empty());
        assert!(outcome.effects.custom_propagations.is_empty());
        assert_eq!(
            runtime.db_mut().unwrap().expired_deletions,
            vec![b"key".to_vec()]
        );
    }

    #[test]
    fn invalid_options_preserve_the_value_and_deadline() {
        for options in [
            vec!["PX", "50000", "invalid"],
            vec!["PX", "50000", "EX", "5"],
            vec!["PXAT", "999", "PERSIST"],
        ] {
            let now_ms = crate::foundation::io::store::kv_engine::now_ms();
            let mut runtime = TestCommandRuntime::new(1, now_ms);
            let db = runtime.db_mut().unwrap();
            db.set(b"key".to_vec(), Robj::string("value"));
            db.set_expire(b"key", now_ms + 20_000);
            let args = CommandArgs::from_parts(
                ["GETEX", "key"]
                    .into_iter()
                    .chain(options)
                    .map(Bytes::from)
                    .collect(),
            );

            let outcome = getex(&mut runtime, &args);

            assert_eq!(outcome.reply, Value::error("ERR syntax error"));
            let db = runtime.db_mut().unwrap();
            assert_eq!(db.get(b"key").unwrap().as_bytes(), b"value");
            assert_eq!(db.get_expire(b"key"), Some(now_ms + 20_000));
            assert!(db.expired_deletions.is_empty());
            assert!(outcome.effects.changed_keys.is_empty());
        }
    }

    #[test]
    fn valid_options_return_the_value_and_update_only_the_deadline() {
        let now_ms = crate::foundation::io::store::kv_engine::now_ms();
        for (options, expected_deadline) in [
            (vec![], Some(now_ms + 20_000)),
            (vec!["PX", "50000", "PX", "60000"], Some(now_ms + 60_000)),
            (vec!["PERSIST", "PERSIST"], None),
        ] {
            let mut runtime = TestCommandRuntime::new(1, now_ms);
            let db = runtime.db_mut().unwrap();
            db.set(b"key".to_vec(), Robj::string("value"));
            db.set_expire(b"key", now_ms + 20_000);
            let args = CommandArgs::from_parts(
                ["GETEX", "key"]
                    .into_iter()
                    .chain(options)
                    .map(Bytes::from)
                    .collect(),
            );

            let outcome = getex(&mut runtime, &args);

            assert_eq!(outcome.reply, Value::bulk("value"));
            assert_eq!(
                runtime.db_mut().unwrap().get_expire(b"key"),
                expected_deadline
            );
        }
    }
}
