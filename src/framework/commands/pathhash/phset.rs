use bytes::Bytes;
use command_macros::command;

use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;

use super::runtime_support::{is_option, parse_long_range, read_path_hash, write_outcome};

#[command("PHSET", runtime)]
pub fn phset(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.args.len() < 6 {
        return CommandOutcome::reply(CommandError::wrong_arity("phset"));
    }
    let key = &args.args[0];
    let path = &args.args[1];
    let mut index = 2;
    let is_fnx = is_option(&args.args[index], b"FNX");
    let is_fxx = is_option(&args.args[index], b"FXX");
    if is_fnx || is_fxx {
        index += 1;
    }
    if index + 1 >= args.args.len() || !is_option(&args.args[index], b"FIELDS") {
        return CommandOutcome::reply(CommandError::syntax_error());
    }
    let count = match parse_long_range(&args.args[index + 1], 1) {
        Ok(count) => count,
        Err(error) => return CommandOutcome::reply(error),
    };
    let fields_index = index + 2;
    let remaining = args.args.len() - fields_index;
    if !remaining.is_multiple_of(2) || count != remaining / 2 {
        return CommandOutcome::reply(CommandError::syntax_error());
    }
    let db_index = runtime.selected_db();
    let (max_entries, max_value_size) = runtime.hash_encoding_limits();
    let should_read_expired_values = runtime.read_expired_values();
    let tree = match read_path_hash(runtime, key) {
        Ok(tree) => tree,
        Err(error) => return CommandOutcome::reply(error),
    };
    let is_new_key = tree.is_none();
    if is_fnx || is_fxx {
        let payload = tree.and_then(|tree| tree.get(path));
        for pair in args.args[fields_index..].as_chunks::<2>().0 {
            let has_field = payload.is_some_and(|payload| payload.contains(&pair[0]));
            if (is_fnx && has_field) || (is_fxx && !has_field) {
                return write_outcome(db_index, key, "phset", 0, Value::null());
            }
        }
    }
    let pairs: Vec<(Bytes, Bytes)> = args.args[fields_index..]
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| (pair[0].clone(), pair[1].clone()))
        .collect();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    if db
        .mutate_path_hash(key, true, should_read_expired_values, |tree| {
            tree.set_pairs(path, &pairs, max_entries, max_value_size);
        })
        .is_err()
    {
        return CommandOutcome::reply(CommandError::wrong_type());
    }
    let mut outcome = write_outcome(db_index, key, "phset", count, Value::ok());
    if is_new_key {
        outcome.effects.events.insert(
            0,
            CoreEvent {
                kind: "new",
                key: key.clone(),
            },
        );
    }
    outcome
}
