use command_macros::command;

use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::CommandOutcome;
use crate::framework::commands::command_runtime::CommandRuntime;

use super::runtime_support::{read_path_hash, reply_fields};

#[command("PHGET", runtime)]
pub fn phget(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.args.len() < 3 {
        return CommandOutcome::reply(CommandError::wrong_arity("phget"));
    }
    let tree = match read_path_hash(runtime, &args.args[0]) {
        Ok(tree) => tree,
        Err(error) => return CommandOutcome::reply(error),
    };
    let payload = tree.and_then(|tree| tree.get(&args.args[1]));
    CommandOutcome::reply(reply_fields(payload, &args.args[2..]))
}
