use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use command_macros::command;

#[command("DBSIZE", runtime)]
pub fn dbsize(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if !args.is_empty() {
        return CommandOutcome::reply(CommandError::wrong_arity("dbsize"));
    }
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    CommandOutcome::reply(Value::integer(db.dbsize() as i64))
}
