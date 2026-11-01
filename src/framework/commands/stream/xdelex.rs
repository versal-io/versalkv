use super::runtime_support::delete_references;
use crate::engine::core::CommandOutcome;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::CommandArgs;
use command_macros::command;

#[command("XDELEX", runtime)]
pub fn xdelex(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    delete_references(runtime, args, false)
}
