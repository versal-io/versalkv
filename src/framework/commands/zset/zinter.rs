use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::zset::basic;
use crate::framework::commands::zset::zinterstore;
use bytes::Bytes;
use command_macros::command;

#[command("ZINTER", runtime, get_keys = crate::framework::commands::key_extractors::numkeys_ro_get_keys)]
pub fn zinter(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.args.len() < 2 {
        return CommandOutcome::reply(CommandError::syntax_error());
    }
    let _numkeys = match std::str::from_utf8(&args.args[0])
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
    {
        Some(n) if n > 0 => n,
        _ => {
            return CommandOutcome::reply(Value::error(
                "ERR at least 1 input key is needed for 'zinter' command",
            ))
        }
    };
    let mut has_scores = false;
    let mut store_args: Vec<Bytes> = Vec::new();
    let mut i = 0;
    while i < args.args.len() {
        if args.args[i].eq_ignore_ascii_case(b"withscores") {
            has_scores = true;
            i += 1;
        } else {
            store_args.push(args.args[i].clone());
            i += 1;
        }
    }
    let (key_weights, aggregate) = match basic::parse_set_operation_args(&store_args, 0, "zinter") {
        Ok(parsed) => parsed,
        Err(error) => return CommandOutcome::reply(error),
    };
    let is_resp3 = runtime.resp3();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    let result = match zinterstore::collect_intersection(db, &key_weights, aggregate) {
        Ok(result) => result,
        Err(error) => return CommandOutcome::reply(error),
    };
    CommandOutcome::reply(basic::set_operation_reply(result, has_scores, is_resp3))
}
