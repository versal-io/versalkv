use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("HMGET", runtime)]
pub fn hmget(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    CommandOutcome::reply(hmget_reply(runtime, args))
}

fn hmget_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandError::db_index_out_of_range(),
    };
    let key = &args.args[0];
    let values = match db.get_hash_object(key) {
        Err(_) => return CommandError::wrong_type(),
        Ok(None) => {
            let mut out = Vec::with_capacity(args.args.len() - 1);
            for _ in 1..args.args.len() {
                out.push(Value::null());
            }
            return Value::Array(out);
        }
        Ok(Some(hash)) => args.args[1..]
            .iter()
            .map(|field| hash.hash_get(field))
            .collect::<Vec<_>>(),
    };
    let mut out = Vec::with_capacity(values.len());
    for (field, value) in args.args[1..].iter().zip(values) {
        match value {
            Some(v) if !db.hash_field_expired(key, field) => out.push(Value::bulk(v)),
            Some(_) | None => out.push(Value::null()),
        }
    }
    Value::Array(out)
}
