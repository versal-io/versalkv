use command_macros::command;

use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;

use super::runtime_support::read_path_hash;

#[command("PHCARD", runtime)]
pub fn phcard(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let [key] = args.args.as_slice() else {
        return CommandOutcome::reply(CommandError::wrong_arity("phcard"));
    };
    let tree = match read_path_hash(runtime, key) {
        Ok(tree) => tree,
        Err(error) => return CommandOutcome::reply(error),
    };
    CommandOutcome::reply(Value::integer(tree.map_or(0, |tree| tree.len() as i64)))
}
