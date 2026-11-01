use command_macros::command;

use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;

use super::runtime_support::{parse_match_options, read_path_hash, reply_match, PrefixMatches};

#[command("PHPREFIXES", runtime)]
pub fn phprefixes(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.args.len() < 2 {
        return CommandOutcome::reply(CommandError::wrong_arity("phprefixes"));
    }
    let options = match parse_match_options(&args.args[2..], true, true) {
        Ok(options) => options,
        Err(error) => return CommandOutcome::reply(error),
    };
    let tree = match read_path_hash(runtime, &args.args[0]) {
        Ok(Some(tree)) => tree,
        Ok(None) => return CommandOutcome::reply(Value::Array(Vec::new())),
        Err(error) => return CommandOutcome::reply(error),
    };
    let mut replies: Vec<Value> = PrefixMatches::new(tree, &args.args[1], options.max_path_len)
        .take(options.count.unwrap_or(usize::MAX))
        .map(|(path, payload)| reply_match(path, payload, &options))
        .collect();
    replies.reverse();
    CommandOutcome::reply(Value::Array(replies))
}
