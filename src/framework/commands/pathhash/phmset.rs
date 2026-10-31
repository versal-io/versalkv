use bytes::Bytes;
use command_macros::command;

use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;

use super::runtime_support::{parse_field_groups, read_path_hash, write_outcome};

#[command("PHMSET", runtime)]
pub fn phmset(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.args.len() < 6 {
        return CommandOutcome::reply(CommandError::wrong_arity("phmset"));
    }
    let groups = match parse_field_groups(&args.args, true) {
        Ok(groups) => groups,
        Err(error) => return CommandOutcome::reply(error),
    };
    let assignments = groups.iter().map(|group| group.count).sum();
    let key = &args.args[0];
    let db_index = runtime.selected_db();
    let (max_entries, max_value_size) = runtime.hash_encoding_limits();
    let should_read_expired_values = runtime.read_expired_values();
    let is_new_key = match read_path_hash(runtime, key) {
        Ok(tree) => tree.is_none(),
        Err(error) => return CommandOutcome::reply(error),
    };
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    if db
        .mutate_path_hash(key, true, should_read_expired_values, |tree| {
            for group in &groups {
                let pairs: Vec<(Bytes, Bytes)> = group
                    .fields
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|pair| (pair[0].clone(), pair[1].clone()))
                    .collect();
                tree.set_pairs(group.path, &pairs, max_entries, max_value_size);
            }
        })
        .is_err()
    {
        return CommandOutcome::reply(CommandError::wrong_type());
    }
    let mut outcome = write_outcome(db_index, key, "phmset", assignments, Value::ok());
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
