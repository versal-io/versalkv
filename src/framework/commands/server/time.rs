use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("TIME", runtime)]
pub fn time_cmd(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
) -> CommandOutcome {
    if !args.args.is_empty() {
        return CommandOutcome::reply(CommandError::wrong_arity("time"));
    }
    let now = runtime.now_us();
    let secs = now / 1_000_000;
    let micros = now % 1_000_000;
    CommandOutcome::reply(Value::Array(vec![
        Value::bulk(secs.to_string().into_bytes()),
        Value::bulk(micros.to_string().into_bytes()),
    ]))
}
