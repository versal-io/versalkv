use super::basic::score_value;
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("ZMSCORE", runtime)]
pub fn zmscore(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    CommandOutcome::reply(zmscore_reply(runtime, args))
}

fn zmscore_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    let resp3 = runtime.resp3();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandError::db_index_out_of_range(),
    };
    let zset = match db.get_zset_object(&args.args[0]) {
        Ok(None) => {
            let mut out = Vec::new();
            for _ in 1..args.args.len() {
                out.push(Value::null());
            }
            return Value::Array(out);
        }
        Ok(Some(zset)) => zset,
        Err(_) => return CommandError::wrong_type(),
    };
    let mut out = Vec::new();
    for i in 1..args.args.len() {
        match zset.zset_score(&args.args[i]) {
            Some(s) => out.push(score_value(s, resp3)),
            None => out.push(Value::null()),
        }
    }
    Value::Array(out)
}
