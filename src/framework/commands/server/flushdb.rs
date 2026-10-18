use crate::engine::command::CommandArgs;
use crate::engine::core::CommandOutcome;
use crate::framework::commands::command_runtime::{CommandRuntime, FlushMode};
use command_macros::command;

#[command("FLUSHDB", runtime)]
pub fn flushdb(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let mode = match args.args.as_slice() {
        [] => FlushMode::Default,
        [mode] if mode.eq_ignore_ascii_case(b"sync") => FlushMode::Sync,
        [mode] if mode.eq_ignore_ascii_case(b"async") => FlushMode::Async,
        _ => return CommandOutcome::reply(crate::engine::command::CommandError::syntax_error()),
    };
    runtime.flushdb(mode)
}
