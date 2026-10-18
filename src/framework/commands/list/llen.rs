use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use command_macros::command;

#[command("LLEN", runtime)]
pub fn llen(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let [key] = args.args.as_slice() else {
        return CommandOutcome::reply(CommandError::wrong_arity("llen"));
    };
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    match db.get_list(key) {
        Ok(Some(list)) => CommandOutcome::reply(Value::integer(list.len() as i64)),
        Ok(None) => CommandOutcome::reply(Value::integer(0)),
        Err(_) => CommandOutcome::reply(CommandError::wrong_type()),
    }
}
