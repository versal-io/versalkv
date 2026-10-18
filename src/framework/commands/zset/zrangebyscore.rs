use crate::engine::command::CommandArgs;
use crate::engine::core::CommandOutcome;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::zset::zrange::{execute_zrange_request, RangeKind, ZRangeRequest};
use command_macros::command;

#[command("ZRANGEBYSCORE", runtime)]
pub fn zrangebyscore(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let request = match ZRangeRequest::parse_legacy(args, RangeKind::Score, false, true) {
        Ok(request) => request,
        Err(error) => return CommandOutcome::reply(error),
    };
    execute_zrange_request(runtime, &request)
}
