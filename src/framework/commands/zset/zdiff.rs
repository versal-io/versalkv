use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::CommandOutcome;
use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::structure::hashtable::HashTable;
use crate::foundation::io::store::Robj;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::zset::basic::{score_value, sorted_entries};
use command_macros::command;

#[command("ZDIFF", runtime, get_keys = crate::framework::commands::key_extractors::numkeys_ro_get_keys)]
pub fn zdiff(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.args.len() < 2 {
        return CommandOutcome::reply(CommandError::syntax_error());
    }
    let numkeys = match std::str::from_utf8(&args.args[0])
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
    {
        Some(n) if n > 0 => n,
        _ => {
            return CommandOutcome::reply(Value::error(
                "ERR at least 1 input key is needed for 'zdiff' command",
            ))
        }
    };
    if 1 + numkeys > args.args.len() {
        return CommandOutcome::reply(CommandError::syntax_error());
    }
    let mut withscores = false;
    let mut i = 1 + numkeys;
    while i < args.args.len() {
        if args.args[i].eq_ignore_ascii_case(b"withscores") {
            withscores = true;
            i += 1;
        } else {
            return CommandOutcome::reply(CommandError::syntax_error());
        }
    }
    let resp3 = runtime.resp3();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    let first_key = &args.args[1];
    let first_map = match db.get_zset_object(first_key) {
        Ok(Some(m)) => {
            let mut map = HashTable::new();
            for (member, score) in m.zset_entries() {
                map.insert(member, score);
            }
            map
        }
        Ok(None) => return CommandOutcome::reply(Value::Array(Vec::new())),
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
    for k in 2..numkeys + 1 {
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
    let entries = sorted_entries(&result);
    let mut out = Vec::new();
    for entry in &entries {
        if withscores && resp3 {
            out.push(Value::Array(vec![
                Value::bulk(entry.0.clone()),
                score_value(entry.1, resp3),
            ]));
        } else {
            out.push(Value::bulk(entry.0.clone()));
            if withscores {
                out.push(score_value(entry.1, resp3));
            }
        }
    }
    CommandOutcome::reply(Value::Array(out))
}
