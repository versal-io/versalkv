use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("XLEN", runtime)]
pub fn xlen(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    CommandOutcome::reply(match db.get_stream(&args.args[0]) {
        Ok(None) => Value::integer(0),
        Ok(Some(s)) => Value::integer(s.len() as i64),
        Err(_) => CommandError::wrong_type(),
    })
}
