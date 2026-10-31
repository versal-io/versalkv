use command_macros::command;

use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;

use super::runtime_support::read_path_hash;

#[command("PHEXISTS", runtime)]
pub fn phexists(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let [key, path] = args.args.as_slice() else {
        return CommandOutcome::reply(CommandError::wrong_arity("phexists"));
    };
    let tree = match read_path_hash(runtime, key) {
        Ok(tree) => tree,
        Err(error) => return CommandOutcome::reply(error),
    };
    CommandOutcome::reply(Value::integer(i64::from(
        tree.is_some_and(|tree| tree.contains_path(path)),
    )))
}
