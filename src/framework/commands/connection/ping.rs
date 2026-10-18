use crate::engine::core::CommandOutcome;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::CommandArgs;
use command_macros::command;

#[command("PING", runtime)]
pub fn ping(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.args.len() > 1 {
        return CommandOutcome::reply(crate::framework::CommandError::wrong_arity("ping"));
    }
    CommandOutcome::reply(runtime.ping_response(args.args.first()))
}
