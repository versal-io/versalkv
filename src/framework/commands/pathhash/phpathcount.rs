use command_macros::command;

use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;

use super::runtime_support::{is_option, read_path_hash};

#[command("PHPATHCOUNT", runtime)]
pub fn phpathcount(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let (key, prefix) = match args.args.as_slice() {
        [key] => (key, b"".as_slice()),
        [key, option, prefix] if is_option(option, b"PREFIX") => (key, prefix.as_ref()),
        [] => return CommandOutcome::reply(CommandError::wrong_arity("phpathcount")),
        _ => return CommandOutcome::reply(CommandError::syntax_error()),
    };
    let tree = match read_path_hash(runtime, key) {
        Ok(tree) => tree,
        Err(error) => return CommandOutcome::reply(error),
    };
    CommandOutcome::reply(Value::integer(tree.map_or(0, |tree| {
        if prefix.is_empty() {
            tree.len() as i64
        } else {
            tree.paths
                .range::<[u8], _>((
                    std::ops::Bound::Included(prefix),
                    std::ops::Bound::Unbounded,
                ))
                .take_while(|(path, _)| path.starts_with(prefix))
                .count() as i64
        }
    })))
}
