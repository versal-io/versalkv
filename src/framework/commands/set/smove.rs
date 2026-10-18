use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[command("SMOVE", runtime)]
pub fn smove(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let (max_intset, max_lp_entries, max_lp_value) = runtime.set_encoding_limits();
    let db_idx = runtime.selected_db();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let src_key = &args.args[0];
    let dst_key = &args.args[1];
    let member = &args.args[2];

    if src_key == dst_key {
        let reply = match db.get_set(src_key) {
            Ok(Some(s)) => Value::integer(if s.data.contains_key(member) { 1 } else { 0 }),
            Ok(None) => Value::integer(0),
            Err(_) => CommandError::wrong_type(),
        };
        return skipped(reply);
    }

    match db.get_set(src_key) {
        Ok(None) => {
            return skipped(Value::integer(0));
        }
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
        Ok(Some(_)) => {}
    }

    if db.get_set(dst_key).is_err() {
        return CommandOutcome::reply(CommandError::wrong_type());
    }

    let dst_has_member = match db.get_set(dst_key) {
        Ok(Some(s)) => s.data.contains_key(member),
        _ => false,
    };

    let src_empty = {
        let src_set = match db.get_set_mut(src_key) {
            Ok(Some(s)) => s,
            _ => {
                return skipped(Value::integer(0));
            }
        };
        if src_set.data.remove(member).is_none() {
            return skipped(Value::integer(0));
        }
        src_set.data.is_empty()
    };

    if src_empty {
        db.del(src_key);
    }

    let mut effects = CommandEffects {
        changed_keys: vec![(db_idx, src_key.clone())],
        events: vec![CoreEvent {
            kind: "srem",
            key: src_key.clone(),
        }],
        ..CommandEffects::default()
    };
    if dst_has_member {
        effects.custom_propagations.push(vec![
            Bytes::from_static(b"SREM"),
            src_key.clone(),
            member.clone(),
        ]);
    } else {
        let set = match db.get_or_create_set(dst_key) {
            Ok(s) => s,
            Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
        };
        set.insert_with_encoding(member.clone(), max_intset, max_lp_entries, max_lp_value);
        effects.changed_keys.push((db_idx, dst_key.clone()));
        effects.events.push(CoreEvent {
            kind: "sadd",
            key: dst_key.clone(),
        });
    }

    CommandOutcome {
        reply: Value::integer(1),
        effects,
    }
}

fn skipped(reply: Value) -> CommandOutcome {
    CommandOutcome {
        reply,
        effects: CommandEffects {
            skip_propagate: true,
            ..CommandEffects::default()
        },
    }
}
