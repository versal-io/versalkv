use crate::engine::core::CommandOutcome;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::introspection::command_reply;
use crate::framework::CommandArgs;
use command_macros::command;

#[command("COMMAND", runtime)]
pub fn command(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    CommandOutcome::reply(command_reply(runtime.command_catalog(), args))
}
