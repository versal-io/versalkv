use crate::engine::command::CommandArgs;
use crate::engine::core::CommandOutcome;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::generic::expire::expire_impl;
use command_macros::command;

#[command("PEXPIRE", runtime)]
pub fn pexpire(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    expire_impl(runtime, args, 1, false, "pexpire")
}
