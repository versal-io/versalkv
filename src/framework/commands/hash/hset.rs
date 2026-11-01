use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use bytes::Bytes;
use command_macros::command;

#[command("HSET", runtime)]
pub fn hset(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.args.len().is_multiple_of(2) {
        return CommandOutcome::reply(CommandError::wrong_arity("hset"));
    }
    let db_index = runtime.selected_db();
    let (max_entries, max_value_size) = runtime.hash_encoding_limits();
    let read_expired_values = runtime.read_expired_values();
    let key = args.args[0].clone();
    let pairs = args.args[1..]
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| (pair[0].clone(), pair[1].clone()))
        .collect::<Vec<_>>();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    if db.hash_prepare_pairs_with_limits(&key, &pairs, max_entries, max_value_size).is_err() {
        return CommandOutcome::reply(CommandError::wrong_type());
    }
    let mut added = 0i64;
    let mut expired_overwritten = 0u64;
    for (field, value) in &pairs {
        let expired = !read_expired_values && db.clear_expired_hash_field_ttl(&key, field);
        let was_new = match db.hash_set_with_limits(&key, field, value, max_entries, max_value_size)
        {
            Ok(added) => added,
            Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
        };
        if expired || was_new {
            added += 1;
        }
        expired_overwritten += u64::from(expired);
    }
    for (field, _) in &pairs {
        db.remove_hash_field_expire(&key, field);
    }
    let mut effects = CommandEffects::default();
    effects.changed_keys.push((db_index, key.clone()));
    effects.events.push(CoreEvent {
        kind: "hset",
        key: key.clone(),
    });
    effects.expired_hash_fields = expired_overwritten;
    if expired_overwritten > 0 {
        effects.skip_propagate = true;
        let mut propagation = Vec::with_capacity(2 + pairs.len() * 2);
        propagation.push(Bytes::from_static(b"HSET"));
        propagation.push(key.clone());
        for (field, value) in &pairs {
            propagation.push(field.clone());
            propagation.push(value.clone());
        }
        effects.custom_propagations.push(propagation);
        effects.events.insert(
            0,
            CoreEvent {
                kind: "hexpired",
                key,
            },
        );
    }
    CommandOutcome {
        reply: Value::integer(added),
        effects,
    }
}
