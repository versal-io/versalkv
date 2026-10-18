use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use command_macros::command;

#[command("ZCARD", runtime)]
pub fn zcard(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let [key] = args.args.as_slice() else {
        return CommandOutcome::reply(CommandError::wrong_arity("zcard"));
    };
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    match db.get_zset_object(key) {
        Ok(None) => CommandOutcome::reply(Value::integer(0)),
        Ok(Some(zset)) => CommandOutcome::reply(Value::integer(zset.zset_len() as i64)),
        Err(_) => CommandOutcome::reply(CommandError::wrong_type()),
    }
}
