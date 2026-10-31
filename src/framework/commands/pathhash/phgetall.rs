use command_macros::command;

use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::CommandOutcome;
use crate::framework::commands::command_runtime::CommandRuntime;

use super::runtime_support::{read_path_hash, reply_payload};

#[command("PHGETALL", runtime)]
pub fn phgetall(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let [key, path] = args.args.as_slice() else {
        return CommandOutcome::reply(CommandError::wrong_arity("phgetall"));
    };
    let tree = match read_path_hash(runtime, key) {
        Ok(tree) => tree,
        Err(error) => return CommandOutcome::reply(error),
    };
    CommandOutcome::reply(reply_payload(tree.and_then(|tree| tree.get(path))))
}
