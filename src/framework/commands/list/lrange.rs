use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use command_macros::command;

#[command("LRANGE", runtime)]
pub fn lrange(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let [key, start, stop] = args.args.as_slice() else {
        return CommandOutcome::reply(CommandError::wrong_arity("lrange"));
    };
    let parse = |value: &[u8]| {
        std::str::from_utf8(value)
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
    };
    let (Some(start), Some(stop)) = (parse(start), parse(stop)) else {
        return CommandOutcome::reply(Value::error("ERR value is not an integer or out of range"));
    };
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    let list = match db.get_list(key) {
        Ok(Some(list)) => list,
        Ok(None) => return CommandOutcome::reply(Value::Array(Vec::new())),
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
    };
    let length = list.len() as i64;
    let mut first = if start < 0 { length + start } else { start };
    let mut last = if stop < 0 { length + stop } else { stop };
    if first < 0 {
        first = 0;
    }
    if last >= length {
        last = length - 1;
    }
    if first > last || length == 0 {
        return CommandOutcome::reply(Value::Array(Vec::new()));
    }
    CommandOutcome::reply(Value::Array(
        (first..=last)
            .map(|index| Value::bulk(list[index as usize].to_vec()))
            .collect(),
    ))
}
