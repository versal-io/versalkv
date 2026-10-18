use crate::engine::command::CommandArgs;
use crate::engine::core::CommandOutcome;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::generic::ttl::ttl_impl;
use command_macros::command;

#[command("PTTL", runtime)]
pub fn pttl(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    ttl_impl(runtime, args, true)
}
