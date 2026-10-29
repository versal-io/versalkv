use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::foundation::io::store::structure::hashtable::HashTable;
use crate::foundation::io::store::Robj;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command(
    "ZDIFFSTORE",
    runtime,
    get_keys = crate::framework::commands::key_extractors::zsetstore_get_keys
)]
pub fn zdiffstore(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
) -> CommandOutcome {
    if args.args.len() < 3 {
        return CommandOutcome::reply(CommandError::syntax_error());
    }
    let numkeys = match std::str::from_utf8(&args.args[1])
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
    {
        Some(n) if n > 0 => n,
        _ => {
            return CommandOutcome::reply(Value::error(
                "ERR at least 1 input key is needed for 'zdiffstore' command",
            ))
        }
    };
    if 2 + numkeys > args.args.len() {
        return CommandOutcome::reply(CommandError::syntax_error());
    }
    if 2 + numkeys < args.args.len() {
        return CommandOutcome::reply(CommandError::syntax_error());
    }
    let (max_entries, max_value_size) = runtime.zset_encoding_limits();
    let db_idx = runtime.selected_db();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let dest = &args.args[0];
    let first_key = &args.args[2];
    let first_map = match db.get_zset_object(first_key) {
        Ok(Some(m)) => {
            let mut map = HashTable::new();
            for (member, score) in m.zset_entries() {
                map.insert(member, score);
            }
            map
        }
        Ok(None) => {
            db.del(dest);
            return changed(Value::integer(0), db_idx, dest);
        }
        Err(()) => {
            // Check if it's a Set
            match db.get(first_key) {
                Some(Robj::SetHashtable(s)) => {
                    let mut map: HashTable<f64> = HashTable::new();
                    for member in s.data.keys() {
                        map.insert(member.clone(), 1.0);
                    }
                    map
                }
                _ => return CommandOutcome::reply(CommandError::wrong_type()),
            }
        }
    };
    let mut result = first_map;
    for k in 3..numkeys + 2 {
        match db.get_zset_object(&args.args[k]) {
            Ok(Some(m)) => {
                for (member, _) in m.zset_entries() {
                    result.remove(&member);
                }
            }
            Ok(None) => continue,
            Err(()) => {
                // Check if it's a Set
                match db.get(&args.args[k]) {
                    Some(Robj::SetHashtable(s)) => {
                        for member in s.data.keys() {
                            result.remove(member);
                        }
                    }
                    _ => return CommandOutcome::reply(CommandError::wrong_type()),
                }
            }
        }
    }
    let count = result.len() as i64;
    if result.is_empty() {
        db.del(dest);
    } else {
        db.set(
            dest.to_vec(),
            Robj::zset_from_dict_with_limits(result, max_entries, max_value_size),
        );
    }
    changed(Value::integer(count), db_idx, dest)
}

fn changed(reply: Value, db_idx: u32, key: &bytes::Bytes) -> CommandOutcome {
    CommandOutcome {
        reply,
        effects: CommandEffects {
            changed_keys: vec![(db_idx, key.clone())],
            events: vec![CoreEvent {
                kind: "zdiffstore",
                key: key.clone(),
            }],
            ..CommandEffects::default()
        },
    }
}
