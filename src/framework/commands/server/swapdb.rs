use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("SWAPDB", runtime)]
pub fn swapdb(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.args.len() < 2 {
        return CommandOutcome::reply(CommandError::wrong_arity("swapdb"));
    }
    let db1 = match std::str::from_utf8(&args.args[0])
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
    {
        Some(n) => n,
        None => return CommandOutcome::reply(Value::error("ERR invalid first DB index")),
    };
    let db2 = match std::str::from_utf8(&args.args[1])
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
    {
        Some(n) => n,
        None => return CommandOutcome::reply(Value::error("ERR invalid second DB index")),
    };
    runtime.swapdb(db1, db2)
}
