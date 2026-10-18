use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandEffects, CommandOutcome, Value};
use crate::foundation::io::store::Robj;
use crate::framework::commands::command_runtime::CommandRuntime;
use bytes::Bytes;
use command_macros::command;

#[command("INCR", runtime)]
pub fn incr(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    increment(runtime, args, 1, "incr")
}

pub(crate) fn increment(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
    delta: i64,
    name: &str,
) -> CommandOutcome {
    let [key, ..] = args.args.as_slice() else {
        return CommandOutcome::reply(CommandError::wrong_arity(name));
    };
    if args.len()
        != if matches!(name, "incr" | "decr") {
            1
        } else {
            2
        }
    {
        return CommandOutcome::reply(CommandError::wrong_arity(name));
    }
    let db_index = runtime.selected_db();
    let preserve_ttl = runtime.read_expired_values();
    let suppress_lazy_expire = runtime.suppress_lazy_expire();
    let now_ms = runtime.now_ms();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    db.set_no_lazy_expire(suppress_lazy_expire);
    let value = if preserve_ttl {
        db.get_string_allow_expired(key)
    } else {
        match db.get_at(key, now_ms) {
            Some(value) if value.is_string() => Ok(Some(value.as_bytes())),
            Some(_) => Err(()),
            None => Ok(None),
        }
    };
    let current = match value {
        Ok(None) => 0,
        Ok(Some(value)) => match parse_integer(value) {
            Some(value) => value,
            None => {
                db.set_no_lazy_expire(false);
                return CommandOutcome::reply(Value::error(
                    "ERR value is not an integer or out of range",
                ));
            }
        },
        Err(_) => {
            db.set_no_lazy_expire(false);
            return CommandOutcome::reply(CommandError::wrong_type());
        }
    };
    let Some(next) = current.checked_add(delta) else {
        db.set_no_lazy_expire(false);
        return CommandOutcome::reply(Value::error("ERR increment or decrement would overflow"));
    };
    let object = Robj::string(Bytes::from(next.to_string()));
    if preserve_ttl {
        db.set_keep_ttl(key.to_vec(), object);
    } else {
        db.set(key.to_vec(), object);
    }
    db.set_no_lazy_expire(false);
    CommandOutcome {
        reply: Value::integer(next),
        effects: CommandEffects {
            changed_keys: vec![(db_index, key.clone())],
            ..CommandEffects::default()
        },
    }
}

pub(crate) fn parse_integer(value: &[u8]) -> Option<i64> {
    std::str::from_utf8(value).ok()?.parse().ok()
}
