use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use command_macros::command;

#[command("HLEN", runtime)]
pub fn hlen(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let [key] = args.args.as_slice() else {
        return CommandOutcome::reply(CommandError::wrong_arity("hlen"));
    };
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    match db.get_hash_object(key) {
        Err(_) => CommandOutcome::reply(CommandError::wrong_type()),
        Ok(None) => CommandOutcome::reply(Value::integer(0)),
        Ok(Some(hash)) => CommandOutcome::reply(Value::integer(hash.hash_len() as i64)),
    }
}
