use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[command("LINDEX", runtime)]
pub fn lindex(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    CommandOutcome::reply(lindex_reply(runtime, args))
}

fn lindex_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandError::db_index_out_of_range(),
    };
    let parse = |b: &Bytes| {
        std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
    };
    let index = match parse(&args.args[1]) {
        Some(n) => n,
        None => return Value::error("ERR value is not an integer or out of range"),
    };
    let list = match db.get_list(&args.args[0]) {
        Err(_) => return CommandError::wrong_type(),
        Ok(Some(l)) => l,
        Ok(None) => return Value::null(),
    };
    let len = list.len() as i64;
    let idx = if index < 0 { len + index } else { index };
    if idx < 0 || idx >= len {
        return Value::null();
    }
    Value::bulk(list[idx as usize].to_vec())
}
