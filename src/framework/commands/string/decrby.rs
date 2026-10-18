use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::string::incr::{increment, parse_integer};
use command_macros::command;

#[command("DECRBY", runtime)]
pub fn decrby(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let [_, amount] = args.args.as_slice() else {
        return CommandOutcome::reply(CommandError::wrong_arity("decrby"));
    };
    let Some(amount) = parse_integer(amount) else {
        return CommandOutcome::reply(Value::error("ERR value is not an integer or out of range"));
    };
    let Some(amount) = amount.checked_neg() else {
        return CommandOutcome::reply(Value::error("ERR decrement would overflow"));
    };
    increment(runtime, args, amount, "decrby")
}
