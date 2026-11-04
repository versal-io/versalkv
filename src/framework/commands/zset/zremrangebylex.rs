use super::basic::parse_lex_bound;
use super::zrem::changed;
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("ZREMRANGEBYLEX", runtime)]
pub fn zremrangebylex(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
) -> CommandOutcome {
    if args.args.len() < 3 {
        return CommandOutcome::reply(CommandError::syntax_error());
    }
    let min_raw = &args.args[1];
    let max_raw = &args.args[2];
    let min = match parse_lex_bound(min_raw) {
        Some(v) => v,
        None => {
            return CommandOutcome::reply(Value::error(
                "ERR min or max not valid string range item",
            ))
        }
    };
    let max = match parse_lex_bound(max_raw) {
        Some(v) => v,
        None => {
            return CommandOutcome::reply(Value::error(
                "ERR min or max not valid string range item",
            ))
        }
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
    let removed = zset.zset_remove_range_by_lex_bounds(min, max) as i64;
    let is_empty = zset.zset_is_empty();
    if is_empty {
        db.del(key);
    }
    if removed == 0 {
        CommandOutcome::reply(Value::integer(0))
    } else {
        changed(Value::integer(removed), db_idx, key, "zremrangebylex")
    }
}
