use super::runtime_support::{hash_field_deletion_propagation, parse_fields};
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("HGETDEL", runtime)]
pub fn hgetdel(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let Some(key) = args.args.first() else {
        return CommandOutcome::reply(CommandError::wrong_arity("hgetdel"));
    };
    let (_, fields) = match parse_fields(&args.args, 1) {
        Ok(fields) => fields,
        Err(error) => return CommandOutcome::reply(error),
    };
    let db_idx = runtime.selected_db();
    let read_expired_values = runtime.read_expired_values();
    let now_ms = runtime.now_ms();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    let has_volatile_fields = db.hash_key_has_volatile_fields(key);
    let mut has_skipped = false;
    let mut removed_fields = Vec::new();
    let mut reply = Vec::with_capacity(fields.len());
    for field in &fields {
        let value = match db.get_hash_object(key) {
            Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
            Ok(None) => None,
            Ok(Some(hash)) => hash.hash_get(field),
        };
        let value = value.filter(|_| read_expired_values || !db.hash_field_expired_at(key, field, now_ms));
        if let Some(value) = value {
            if let Ok(Some(hash)) = db.get_hash_object_mut(key) {
                hash.hash_del(field);
            }
            db.remove_hash_field_expire(key, field);
            removed_fields.push(field.clone());
            reply.push(Value::bulk(value));
        } else {
            has_skipped |= has_volatile_fields;
            reply.push(Value::null());
        }
    }
    if removed_fields.is_empty() {
        return CommandOutcome {
            reply: Value::Array(reply),
            effects: CommandEffects { skip_propagate: true, ..CommandEffects::default() },
        };
    }
    let mut events = vec![CoreEvent { kind: "hdel", key: key.clone() }];
    if matches!(db.get_hash_object(key), Ok(Some(hash)) if hash.hash_is_empty()) {
        db.del(key);
        events.push(CoreEvent { kind: "del", key: key.clone() });
    }
    CommandOutcome {
        reply: Value::Array(reply),
        effects: CommandEffects {
            changed_keys: vec![(db_idx, key.clone())],
            events,
            skip_propagate: has_skipped,
            custom_propagations: if has_skipped {
                vec![hash_field_deletion_propagation(key, &removed_fields)]
            } else {
                Vec::new()
            },
            ..CommandEffects::default()
        },
    }
}
