use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("HGETALL", runtime)]
pub fn hgetall(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    CommandOutcome::reply(hgetall_reply(runtime, args))
}

fn hgetall_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    let resp3 = runtime.resp3();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandError::db_index_out_of_range(),
    };
    let key = &args.args[0];
    let pairs = match db.get_hash_object(key) {
        Err(_) => return CommandError::wrong_type(),
        Ok(None) => {
            return if resp3 {
                Value::Map(Vec::new())
            } else {
                Value::Array(Vec::new())
            };
        }
        Ok(Some(hash)) => hash.hash_entries(),
    };
    if resp3 {
        let mut out: Vec<(Value, Value)> = Vec::with_capacity(pairs.len());
        for (f, v) in pairs {
            if db.hash_field_expired(key, &f) {
                continue;
            }
            out.push((Value::bulk(f), Value::bulk(v)));
        }
        Value::Map(out)
    } else {
        let mut out: Vec<Value> = Vec::with_capacity(pairs.len() * 2);
        for (f, v) in pairs {
            if db.hash_field_expired(key, &f) {
                continue;
            }
            out.push(Value::bulk(f));
            out.push(Value::bulk(v));
        }
        Value::Array(out)
    }
}
