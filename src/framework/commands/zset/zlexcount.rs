use super::basic::parse_lex_bound;
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("ZLEXCOUNT", runtime)]
pub fn zlexcount(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    CommandOutcome::reply(zlexcount_reply(runtime, args))
}

fn zlexcount_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    if args.args.len() < 3 {
        return CommandError::syntax_error();
    }
    let min_raw = &args.args[1];
    let max_raw = &args.args[2];
    let (min_val, min_inc) = match parse_lex_bound(min_raw) {
        Some(v) => v,
        None => return Value::error("ERR min or max not valid string range item"),
    };
    let (max_val, max_inc) = match parse_lex_bound(max_raw) {
        Some(v) => v,
        None => return Value::error("ERR min or max not valid string range item"),
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
    Value::integer(
        zset.zset_range_by_lex(&min_val, min_inc, &max_val, max_inc, false)
            .len() as i64,
    )
}
