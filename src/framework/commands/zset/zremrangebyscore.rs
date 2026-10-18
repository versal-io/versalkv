use super::basic::parse_score_range;
use super::zrem::changed;
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("ZREMRANGEBYSCORE", runtime)]
pub fn zremrangebyscore(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
) -> CommandOutcome {
    let min = match parse_score_range(&args.args[1]) {
        Some(v) => v,
        None => return CommandOutcome::reply(Value::error("ERR min or max is not a float")),
    };
    let max = match parse_score_range(&args.args[2]) {
        Some(v) => v,
        None => return CommandOutcome::reply(Value::error("ERR min or max is not a float")),
    };
    let db_idx = runtime.selected_db();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let key = &args.args[0];
    let zset = match db.get_zset_object_mut(key) {
        Ok(Some(m)) => m,
        Ok(None) => return CommandOutcome::reply(Value::integer(0)),
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
    };
    let removed = zset.zset_remove_range_by_score(min, max) as i64;
    let is_empty = zset.zset_is_empty();
    if is_empty {
        db.del(key);
    }
    if removed == 0 {
        CommandOutcome::reply(Value::integer(0))
    } else {
        changed(Value::integer(removed), db_idx, key, "zremrangebyscore")
    }
}
