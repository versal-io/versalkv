use super::basic::parse_score_range;
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("ZCOUNT", runtime)]
pub fn zcount(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    CommandOutcome::reply(zcount_reply(runtime, args))
}

fn zcount_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    let min = match parse_score_range(&args.args[1]) {
        Some(v) => v,
        None => return Value::error("ERR min or max is not a float"),
    };
    let max = match parse_score_range(&args.args[2]) {
        Some(v) => v,
        None => return Value::error("ERR min or max is not a float"),
    };
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandError::db_index_out_of_range(),
    };
    let zset = match db.get_zset_object(&args.args[0]) {
        Ok(None) => return Value::integer(0),
        Ok(Some(m)) => m,
        Err(_) => return CommandError::wrong_type(),
    };
    Value::integer(zset.zset_count_by_score(min, max) as i64)
}
