use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::CommandArgs;
use command_macros::command;

#[command("ECHO", runtime)]
pub fn echo(_runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    CommandOutcome::reply(match args.args.as_slice() {
        [message] => Value::bulk(message.clone()),
        _ => crate::framework::CommandError::wrong_arity("echo"),
    })
}
