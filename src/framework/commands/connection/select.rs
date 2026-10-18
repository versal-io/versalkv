use crate::engine::core::CommandOutcome;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("SELECT", runtime)]
pub fn select(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let idx = match std::str::from_utf8(&args.args[0])
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
    {
        Some(i) => i,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    runtime.select_db(idx)
}
