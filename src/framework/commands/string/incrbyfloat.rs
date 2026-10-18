use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandEffects, CommandOutcome, Value};
use crate::foundation::io::store::Robj;
use crate::framework::commands::command_runtime::CommandRuntime;
use bytes::Bytes;
use command_macros::command;

const NOT_FLOAT: &str = "ERR value is not a valid float";

#[command("INCRBYFLOAT", runtime)]
pub fn incrbyfloat(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let [key, increment] = args.args.as_slice() else {
        return CommandOutcome::reply(CommandError::wrong_arity("incrbyfloat"));
    };
    let db_index = runtime.selected_db();
    let preserve_expired = runtime.read_expired_values();
    let suppress_lazy_expire = runtime.suppress_lazy_expire();
    let now_ms = runtime.now_ms();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    db.set_no_lazy_expire(suppress_lazy_expire);
    let current = if preserve_expired {
        db.get_string_allow_expired(key)
    } else {
        match db.get_at(key, now_ms) {
            Some(value) if value.is_string() => Ok(Some(value.as_bytes())),
            Some(_) => Err(()),
            None => Ok(None),
        }
    };
    let current = match current {
        Err(_) => {
            db.set_no_lazy_expire(false);
            return CommandOutcome::reply(CommandError::wrong_type());
        }
        Ok(None) => 0.0,
        Ok(Some(value)) => match parse_float(value) {
            Some(value) => value,
            None => {
                db.set_no_lazy_expire(false);
                return CommandOutcome::reply(Value::error(NOT_FLOAT));
            }
        },
    };
    let increment = match parse_float(increment) {
        Some(value) => value,
        None => {
            db.set_no_lazy_expire(false);
            return CommandOutcome::reply(Value::error(NOT_FLOAT));
        }
    };
    let next = current + increment;
    if !next.is_finite() {
        db.set_no_lazy_expire(false);
        return CommandOutcome::reply(Value::error("ERR increment would produce NaN or Infinity"));
    }
    let formatted = Bytes::from(format_float(next));
    db.set_keep_ttl(key.to_vec(), Robj::string_raw(formatted.clone()));
    db.set_no_lazy_expire(false);
    CommandOutcome {
        reply: Value::bulk(formatted.clone()),
        effects: CommandEffects {
            changed_keys: vec![(db_index, key.clone())],
            custom_propagations: vec![vec![
                Bytes::from_static(b"SET"),
                key.clone(),
                formatted,
                Bytes::from_static(b"KEEPTTL"),
            ]],
            ..CommandEffects::default()
        },
    }
}

fn parse_float(value: &[u8]) -> Option<f64> {
    let value = std::str::from_utf8(value).ok()?;
    if value.is_empty() || value.trim() != value {
        return None;
    }
    value.parse::<f64>().ok()
}

fn format_float(value: f64) -> String {
    if value == 0.0 {
        "0".to_owned()
    } else {
        value.to_string()
    }
}
