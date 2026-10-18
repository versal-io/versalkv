use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("SMISMEMBER", runtime)]
pub fn smismember(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
) -> CommandOutcome {
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    CommandOutcome::reply(match db.get_set(&args.args[0]) {
        Ok(Some(s)) => Value::Array(
            args.args[1..]
                .iter()
                .map(|m| Value::integer(if s.data.contains_key(m) { 1 } else { 0 }))
                .collect(),
        ),
        Ok(None) => Value::Array(args.args[1..].iter().map(|_| Value::integer(0)).collect()),
        Err(_) => CommandError::wrong_type(),
    })
}
