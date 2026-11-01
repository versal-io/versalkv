use super::runtime_support::delete_references;
use crate::engine::core::CommandOutcome;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::CommandArgs;
use command_macros::command;

#[command("XACKDEL", runtime)]
pub fn xackdel(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    delete_references(runtime, args, true)
}
