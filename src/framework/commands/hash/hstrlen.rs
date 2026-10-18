use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("HSTRLEN", runtime)]
pub fn hstrlen(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    CommandOutcome::reply(match db.get_hash_object(&args.args[0]) {
        Err(_) => CommandError::wrong_type(),
        Ok(None) => Value::integer(0),
        Ok(Some(hash)) => match hash.hash_get(&args.args[1]) {
            Some(value) => Value::integer(value.len() as i64),
            None => Value::integer(0),
        },
    })
}
