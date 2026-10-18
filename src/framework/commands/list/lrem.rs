use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[command("LREM", runtime)]
pub fn lrem(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let max_lp_size = runtime.list_max_listpack_size();
    let db_idx = runtime.selected_db();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let parse = |b: &Bytes| {
        std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
    };
    let count = match parse(&args.args[1]) {
        Some(n) => n,
        None => {
            return CommandOutcome::reply(Value::error(
                "ERR value is not an integer or out of range",
            ))
        }
    };
    let key = &args.args[0];
    let element = &args.args[2];
    let mut delete_if_empty = false;
    let removed;
    {
        let list = match db.get_list_mut(key) {
            Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
            Ok(Some(l)) => l,
            Ok(None) => return skipped(Value::integer(0)),
        };
        let len = list.len();
        list.note_encoding_before_shrink(max_lp_size);
        let mut count_removed = 0;
        if count == 0 {
            list.retain(|x| {
                if x == element {
                    count_removed += 1;
                    false
                } else {
                    true
                }
            });
        } else if count > 0 {
            let mut remaining = count as usize;
            list.retain(|x| {
                if remaining > 0 && x == element {
                    remaining -= 1;
                    count_removed += 1;
                    false
                } else {
                    true
                }
            });
        } else {
            let mut remaining = (-count) as usize;
            let mut indices_to_remove = Vec::new();
            for i in (0..len).rev() {
                if remaining == 0 {
                    break;
                }
                if list[i] == element {
                    indices_to_remove.push(i);
                    remaining -= 1;
                    count_removed += 1;
                }
            }
            for &idx in &indices_to_remove {
                list.remove(idx);
            }
        }
        removed = count_removed;
        if list.is_empty() {
            delete_if_empty = true;
        } else if removed > 0 {
            list.update_encoding_after_shrink(max_lp_size);
        }
    }
    if delete_if_empty {
        db.del(key);
    }
    if removed == 0 {
        return skipped(Value::integer(0));
    }
    CommandOutcome {
        reply: Value::integer(removed as i64),
        effects: CommandEffects {
            changed_keys: vec![(db_idx, key.clone())],
            events: vec![CoreEvent {
                kind: "lrem",
                key: key.clone(),
            }],
            ..CommandEffects::default()
        },
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
