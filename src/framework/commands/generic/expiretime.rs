
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("EXPIRETIME", runtime)]
pub fn expiretime(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
) -> CommandOutcome {
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let ms = db.expiretime_ms(&args.args[0]);
    CommandOutcome::reply(if ms < 0 {
        Value::integer(ms)
    } else {
        Value::integer(ms / 1000)
    })
}
