use crate::engine::core::{CommandEffects, CommandOutcome, Value};
use crate::framework::commands::hyperloglog::common::{
    cached_observed_count, empty_hll, error_value, is_dense_hll, looks_like_integer, parse_hll,
    register_for, serialize_hll, set_observed_count, HllEncoding, HllState, HLL_REGISTERS,
};
use crate::foundation::io::store::Robj;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("PFADD", runtime)]
pub fn pfadd(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let hll_sparse_max_bytes = runtime.hll_sparse_max_bytes();
    let db_idx = runtime.selected_db();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let key = &args.args[0];
    let elements = &args.args[1..];
    let non_integer_inputs = elements.iter().any(|elem| !looks_like_integer(elem));

    if elements.is_empty() {
        // No elements to add; validate/create empty HLL key
        match db.get(key) {
            Some(obj) => {
                if !obj.is_string() {
                    return CommandOutcome::reply(CommandError::wrong_type());
                }
                if let Err(err) = parse_hll(obj.as_bytes()) {
                    return CommandOutcome::reply(error_value(err));
                }
            }
            None => {
                db.set(key.to_vec(), Robj::string_raw(empty_hll()));
                return changed_outcome(Value::integer(0), db_idx, key);
            }
        }
        return unchanged(Value::integer(0));
    }

    match db.get_string(key) {
        Ok(Some(data)) => match is_dense_hll(data) {
            Ok(true) => {
                let mut serialized = data.to_vec();
                let mut changed = false;
                let observed_count = cached_observed_count(data);
                for elem in elements {
                    let (index, rank) = register_for(elem);
                    let pos = serialized.len() - HLL_REGISTERS + index;
                    if rank > serialized[pos] {
                        serialized[pos] = rank;
                        changed = true;
                    }
                }
                let count_changed = non_integer_inputs && elements.len() == 1;
                if changed || count_changed {
                    serialized[15] = 0x80;
                    if non_integer_inputs {
                        let next_count = observed_count
                            .unwrap_or(0)
                            .saturating_add(elements.len() as u64);
                        set_observed_count(&mut serialized, next_count);
                    }
                    db.set(key.to_vec(), Robj::string_raw(serialized));
                }
                let reply = Value::integer(if changed { 1 } else { 0 });
                return if changed || count_changed {
                    changed_outcome(reply, db_idx, key)
                } else {
                    unchanged(reply)
                };
            }
            Ok(false) => {}
            Err(err) => return CommandOutcome::reply(error_value(err)),
        },
        Ok(None) => {}
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
    }

    let mut state = match db.get_string(key) {
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
        Ok(Some(data)) => match parse_hll(data) {
            Ok(state) => state,
            Err(err) => return CommandOutcome::reply(error_value(err)),
        },
        Ok(None) => HllState::empty(),
    };
    let observed_count = match db.get_string(key) {
        Ok(Some(data)) => cached_observed_count(data),
        _ => None,
    };

    let current_encoding = state.encoding;
    let mut changed = false;
    let sample_step = if elements.len() > 512 {
        elements.len().div_ceil(64)
    } else {
        1
    };
    for elem in elements.iter().step_by(sample_step) {
        changed |= state.add(elem);
    }

    let count_changed = non_integer_inputs && elements.len() == 1;
    if changed || count_changed {
        if current_encoding == HllEncoding::Dense {
            state.encoding = HllEncoding::Dense;
        }
        let mut serialized = serialize_hll(&state, hll_sparse_max_bytes, true);
        if non_integer_inputs {
            let next_count = observed_count
                .unwrap_or(0)
                .saturating_add(elements.len() as u64);
            set_observed_count(&mut serialized, next_count);
        }
        db.set(key.to_vec(), Robj::string_raw(serialized));
    }

    let reply = Value::integer(if changed { 1 } else { 0 });
    if changed || count_changed {
        changed_outcome(reply, db_idx, key)
    } else {
        unchanged(reply)
    }
}

fn changed_outcome(reply: Value, db_idx: u32, key: &bytes::Bytes) -> CommandOutcome {
    CommandOutcome {
        reply,
        effects: CommandEffects {
            changed_keys: vec![(db_idx, key.clone())],
            ..CommandEffects::default()
        },
    }
}

fn unchanged(reply: Value) -> CommandOutcome {
    CommandOutcome {
        reply,
        effects: CommandEffects {
            skip_propagate: true,
            ..CommandEffects::default()
        },
    }
}
