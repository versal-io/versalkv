use command_macros::command;

use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;

use super::runtime_support::{parse_match_options, read_path_hash, reply_match, PrefixMatches};

#[command("PHLONGEST", runtime)]
pub fn phlongest(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.args.len() < 2 {
        return CommandOutcome::reply(CommandError::wrong_arity("phlongest"));
    }
    let options = match parse_match_options(&args.args[2..], false, false) {
        Ok(options) => options,
        Err(error) => return CommandOutcome::reply(error),
    };
    let tree = match read_path_hash(runtime, &args.args[0]) {
        Ok(Some(tree)) => tree,
        Ok(None) => return CommandOutcome::reply(Value::null()),
        Err(error) => return CommandOutcome::reply(error),
    };
    let reply = PrefixMatches::new(tree, &args.args[1], None)
        .next()
        .map_or_else(Value::null, |(path, payload)| {
            reply_match(path, payload, &options)
        });
    CommandOutcome::reply(reply)
}
