
use super::expire::expire_impl;
use crate::engine::core::CommandOutcome;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::CommandArgs;
use command_macros::command;

#[command("EXPIREAT", runtime)]
pub fn expireat(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    expire_impl(runtime, args, 1_000, true, "expireat")
}
