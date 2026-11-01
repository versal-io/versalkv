use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[command("DELIFEQ", runtime)]
pub fn delifeq(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    delete_if_value(runtime, &args.args[0], &args.args[1], true)
}

pub(super) fn delete_if_value(
    runtime: &mut dyn CommandRuntime,
    key: &Bytes,
    value: &Bytes,
    should_equal: bool,
) -> CommandOutcome {
    let db_idx = runtime.selected_db();
    let should_read_expired = runtime.read_expired_values();
    let should_suppress_expire = runtime.suppress_lazy_expire();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    db.set_no_lazy_expire(should_suppress_expire);
    let should_delete = if should_read_expired {
        db.get_string_allow_expired(key)
    } else {
        db.get_string(key)
    }
    .map(|current| current.is_some_and(|bytes| (bytes == value.as_ref()) == should_equal));
    db.set_no_lazy_expire(false);
    match should_delete {
        Err(_) => CommandOutcome::reply(CommandError::wrong_type()),
        Ok(true) => {
            db.del(key);
            CommandOutcome {
                reply: Value::integer(1),
                effects: CommandEffects {
                    changed_keys: vec![(db_idx, key.clone())],
                    events: vec![CoreEvent {
                        kind: "del",
                        key: key.clone(),
                    }],
                    custom_propagations: vec![vec![Bytes::from_static(b"DEL"), key.clone()]],
                    ..CommandEffects::default()
                },
            }
        }
        Ok(false) => CommandOutcome {
            reply: Value::integer(0),
            effects: CommandEffects {
                skip_propagate: true,
                ..CommandEffects::default()
            },
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::io::store::Robj;
    use crate::framework::commands::command_runtime::TestCommandRuntime;

    #[test]
    fn import_equality_uses_expired_integer_and_binary_strings() {
        for value in [
            Bytes::from_static(b"42"),
            Bytes::from_static(b"original\0value"),
            Bytes::from(vec![b'x'; 100]),
        ] {
            for should_match in [false, true] {
                let mut runtime = TestCommandRuntime::new(1, 1_000);
                runtime.read_expired_values = true;
                let db = runtime.db_mut().unwrap();
                db.set(b"expired".to_vec(), Robj::string(value.clone()));
                db.set_expire(b"expired", 1);
                let comparison = if should_match {
                    value.clone()
                } else {
                    Bytes::from_static(b"different")
                };
                let args = CommandArgs::from_parts(vec![
                    Bytes::from_static(b"DELIFEQ"),
                    Bytes::from_static(b"expired"),
                    comparison,
                ]);

                let outcome = delifeq(&mut runtime, &args);

                assert_eq!(outcome.reply, Value::integer(i64::from(should_match)));
                assert_eq!(outcome.effects.skip_propagate, !should_match);
                assert_eq!(
                    outcome.effects.changed_keys.len(),
                    usize::from(should_match)
                );
                assert_eq!(outcome.effects.events.len(), usize::from(should_match));
                assert_eq!(
                    outcome.effects.custom_propagations.len(),
                    usize::from(should_match)
                );
                let db = runtime.db_mut().unwrap();
                assert_eq!(
                    db.get_string_allow_expired(b"expired"),
                    Ok((!should_match).then_some(value.as_ref()))
                );
                assert_eq!(db.get_expire(b"expired"), (!should_match).then_some(1));
                assert!(db.expired_deletions.is_empty());
            }
        }
    }
}
