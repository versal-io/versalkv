use crate::engine::command::CommandArgs;
use crate::engine::core::CommandOutcome;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::zset::zrange::{execute_zrange_request, RangeKind, ZRangeRequest};
use command_macros::command;

#[command("ZREVRANGEBYLEX", runtime)]
pub fn zrevrangebylex(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let request = match ZRangeRequest::parse_legacy(args, RangeKind::Lex, true, false) {
        Ok(request) => request,
        Err(error) => return CommandOutcome::reply(error),
    };
    execute_zrange_request(runtime, &request)
}
