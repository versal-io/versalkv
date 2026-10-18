use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

fn build_srem_propagation(key: &[u8], members: &[Bytes]) -> Vec<Vec<Bytes>> {
    members
        .chunks(1024)
        .map(|batch| {
            let mut parts = Vec::with_capacity(batch.len() + 2);
            parts.push(Bytes::from_static(b"SREM"));
            parts.push(Bytes::copy_from_slice(key));
            parts.extend(batch.iter().cloned());
            parts
        })
        .collect()
}

#[command("SPOP", runtime)]
pub fn spop(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let db_idx = runtime.selected_db();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let key = &args.args[0];
    let count = if args.args.len() > 1 {
        match String::from_utf8_lossy(&args.args[1]).parse::<i64>() {
            Ok(n) if n >= 0 => n,
            _ => {
                return CommandOutcome::reply(Value::error(
                    "ERR value is not an integer or out of range",
                ))
            }
        }
    } else {
        -1
    };

    let is_empty;
    let result;
    let mut removed = Vec::new();
    let original_size;

    {
        let set = match db.get_set_mut(key) {
            Ok(None) => {
                return skipped(if count < 0 {
                    Value::Null
                } else {
                    Value::Array(Vec::new())
                });
            }
            Ok(Some(s)) => s,
            Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
        };
        original_size = set.data.len();

        if count < 0 {
            let member = set.data.keys().next().cloned();
            match member {
                Some(m) => {
                    set.data.remove(&m);
                    is_empty = set.data.is_empty();
                    removed.push(m.clone());
                    result = Value::bulk(m);
                }
                None => {
                    is_empty = true;
                    result = Value::Null;
                }
            }
        } else {
            let mut res = Vec::new();
            let count = count as usize;
            while !set.data.is_empty() && res.len() < count {
                let next_m = set.data.keys().next().cloned();
                if let Some(m) = next_m {
                    set.data.remove(&m);
                    removed.push(m.clone());
                    res.push(Value::bulk(m));
                } else {
                    break;
                }
            }
            is_empty = set.data.is_empty();
            result = Value::Array(res);
        }
    }

    let custom_propagations = if count >= 0 && original_size > 0 && count as usize >= original_size {
        let cmd = if removed.len() > 64 {
            b"UNLINK" as &[u8]
        } else {
            b"DEL" as &[u8]
        };
        if is_empty {
            db.del(key);
        }
        vec![vec![
            Bytes::copy_from_slice(cmd),
            key.clone(),
        ]]
    } else {
        if is_empty {
            db.del(key);
        }
        build_srem_propagation(key, &removed)
    };
    CommandOutcome {
        reply: result,
        effects: CommandEffects {
            changed_keys: vec![(db_idx, key.clone())],
            events: vec![CoreEvent {
                kind: "srem",
                key: key.clone(),
            }],
            custom_propagations,
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

#[cfg(test)]
mod tests {
    use super::build_srem_propagation;
    use bytes::Bytes;

    #[test]
    fn srem_propagation_batches_members_at_1024() {
        let members: Vec<Bytes> = (0..3072).map(|n| Bytes::from(n.to_string())).collect();
        let frames = build_srem_propagation(b"myset", &members);

        assert_eq!(frames.len(), 3);
        assert_eq!(frames[0][0], Bytes::from_static(b"SREM"));
        assert_eq!(frames[0].len(), 1026);
    }
}
