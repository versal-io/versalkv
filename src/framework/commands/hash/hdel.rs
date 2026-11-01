use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use command_macros::command;

#[command("HDEL", runtime)]
pub fn hdel(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let Some((key, fields)) = args.args.split_first() else {
        return CommandOutcome::reply(CommandError::wrong_arity("hdel"));
    };
    let db_index = runtime.selected_db();
    let read_expired_values = runtime.read_expired_values();
    let now_ms = runtime.now_ms();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    let has_volatile_fields = db.hash_key_has_volatile_fields(key);
    let expired_fields: std::collections::HashSet<_> = fields
        .iter()
        .filter(|field| !read_expired_values && db.hash_field_expired_at(key, field, now_ms))
        .map(|field| field.to_vec())
        .collect();
    let mut has_skipped = false;
    let mut removed_fields = Vec::new();
    let (removed, is_empty) = {
        let hash = match db.get_hash_object_mut(key) {
            Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
            Ok(None) => return CommandOutcome::reply(Value::integer(0)),
            Ok(Some(hash)) => hash,
        };
        let mut removed = 0i64;
        for field in fields {
            if !expired_fields.contains(field.as_ref()) && hash.hash_del(field) {
                removed += 1;
                removed_fields.push(field.clone());
            } else if has_volatile_fields {
                has_skipped = true;
            }
        }
        (removed, hash.hash_is_empty())
    };
    for field in &removed_fields {
        db.remove_hash_field_expire(key, field);
    }
    if is_empty {
        db.del(key);
    }
    let mut effects = CommandEffects {
        skip_propagate: removed == 0 || has_skipped,
        ..CommandEffects::default()
    };
    if removed > 0 {
        if has_skipped {
            effects.custom_propagations.push(
                super::runtime_support::hash_field_deletion_propagation(key, &removed_fields),
            );
        }
        effects.changed_keys.push((db_index, key.clone()));
        effects.events.push(CoreEvent {
            kind: "hdel",
            key: key.clone(),
        });
        if is_empty {
            effects.events.push(CoreEvent {
                kind: "del",
                key: key.clone(),
            });
        }
    }
    CommandOutcome {
        reply: Value::integer(removed),
        effects,
    }
}
