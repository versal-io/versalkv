use crate::engine::core::CommandOutcome;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::CommandArgs;
use command_macros::command;

use super::bitfield::bitfield_impl;

#[command("BITFIELD_RO", runtime)]
pub fn bitfield_ro(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
) -> CommandOutcome {
    bitfield_impl(runtime, args, false)
}
