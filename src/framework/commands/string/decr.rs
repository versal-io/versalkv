use crate::engine::command::CommandArgs;
use crate::engine::core::CommandOutcome;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::string::incr::increment;
use command_macros::command;

#[command("DECR", runtime)]
pub fn decr(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    increment(runtime, args, -1, "decr")
}
