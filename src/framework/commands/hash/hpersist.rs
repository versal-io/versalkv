
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

use super::runtime_support::{hash_field_persist_propagation, parse_fields};

#[command("HPERSIST", runtime)]
pub fn hpersist(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.args.len() < 3 {
        return CommandOutcome::reply(CommandError::wrong_arity("hpersist"));
    }
    let db_idx = runtime.selected_db();
    let read_expired_values = runtime.read_expired_values();
    let now_ms = runtime.now_ms();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let key = &args.args[0];

    let (_, fields) = match parse_fields(&args.args, 1) {
        Ok(v) => v,
        Err(e) => return CommandOutcome::reply(e),
    };

    let mut results = Vec::with_capacity(fields.len());
    let mut any_removed = false;
    let has_volatile_fields = db.hash_key_has_volatile_fields(key);
    let mut has_skipped = false;
    let mut propagated_fields = Vec::new();

    for field in &fields {
        let field_exists = {
            let hash = match db.get_hash_object(key) {
                Ok(Some(hash)) => hash,
                Ok(None) => {
                    results.push(Value::integer(-2));
                    has_skipped |= has_volatile_fields;
                    continue;
                }
                Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
            };
            hash.hash_contains(field)
        };
        if !field_exists || (!read_expired_values && db.hash_field_expired_at(key, field, now_ms)) {
            results.push(Value::integer(-2));
            has_skipped |= has_volatile_fields;
            continue;
        }

        if db.remove_hash_field_expire(key, field) {
            any_removed = true;
            results.push(Value::integer(1));
            propagated_fields.push(field.clone());
        } else {
            results.push(Value::integer(-1));
            if !has_skipped {
                propagated_fields.push(field.clone());
            }
        }
    }

    if !any_removed {
        return CommandOutcome {
            reply: Value::Array(results),
            effects: CommandEffects { skip_propagate: true, ..CommandEffects::default() },
        };
    }
    CommandOutcome {
        reply: Value::Array(results),
        effects: CommandEffects {
            changed_keys: vec![(db_idx, key.clone())],
            events: vec![CoreEvent {
                kind: "hpersist",
                key: key.clone(),
            }],
            skip_propagate: true,
            custom_propagations: vec![hash_field_persist_propagation(key, &propagated_fields)],
            ..CommandEffects::default()
        },
    }
}
