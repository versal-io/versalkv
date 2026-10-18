use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use command_macros::command;

#[command("SCARD", runtime)]
pub fn scard(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let [key] = args.args.as_slice() else {
        return CommandOutcome::reply(CommandError::wrong_arity("scard"));
    };
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    match db.get_set(key) {
        Ok(Some(set)) => CommandOutcome::reply(Value::integer(set.data.len() as i64)),
        Ok(None) => CommandOutcome::reply(Value::integer(0)),
        Err(_) => CommandOutcome::reply(CommandError::wrong_type()),
    }
}
