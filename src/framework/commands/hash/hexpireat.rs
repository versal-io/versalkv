use super::hexpire::{hash_expire, HashExpireMode};
use crate::engine::core::CommandOutcome;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::CommandArgs;
use command_macros::command;

#[command("HEXPIREAT", runtime)]
pub fn hexpireat(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    hash_expire(runtime, args, HashExpireMode::UnixSeconds)
}
