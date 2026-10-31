use command_macros::command;

use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;

use super::runtime_support::{parse_field_groups, read_path_hash, reply_field};

#[command("PHMGET", runtime)]
pub fn phmget(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.args.len() < 5 {
        return CommandOutcome::reply(CommandError::wrong_arity("phmget"));
    }
    let groups = match parse_field_groups(&args.args, false) {
        Ok(groups) => groups,
        Err(error) => return CommandOutcome::reply(error),
    };
    let tree = match read_path_hash(runtime, &args.args[0]) {
        Ok(tree) => tree,
        Err(error) => return CommandOutcome::reply(error),
    };
    let mut values = Vec::new();
    for group in groups {
        let payload = tree.and_then(|tree| tree.get(group.path));
        values.extend(group.fields.iter().map(|field| reply_field(payload, field)));
    }
    CommandOutcome::reply(Value::Array(values))
}
