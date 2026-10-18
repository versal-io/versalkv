use super::zrem::changed;
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("ZREMRANGEBYRANK", runtime)]
pub fn zremrangebyrank(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
) -> CommandOutcome {
    let parse = |b: &bytes::Bytes| {
        std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
    };
    let start = match parse(&args.args[1]) {
        Some(n) => n,
        None => {
            return CommandOutcome::reply(Value::error(
                "ERR value is not an integer or out of range",
            ))
        }
    };
    let stop = match parse(&args.args[2]) {
        Some(n) => n,
        None => {
            return CommandOutcome::reply(Value::error(
                "ERR value is not an integer or out of range",
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
    let zlen = zset.zset_len() as i64;
    let mut s = if start < 0 { zlen + start } else { start };
    let mut e = if stop < 0 { zlen + stop } else { stop };
    if s < 0 {
        s = 0;
    }
    if e >= zlen {
        e = zlen - 1;
    }
    if s > e || zlen == 0 {
        return CommandOutcome::reply(Value::integer(0));
    }
    let removed = zset.zset_remove_range_by_rank(s, e) as i64;
    let is_empty = zset.zset_is_empty();
    if is_empty {
        db.del(key);
    }
    if removed == 0 {
        CommandOutcome::reply(Value::integer(0))
    } else {
        changed(Value::integer(removed), db_idx, key, "zremrangebyrank")
    }
}
