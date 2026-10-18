use crate::engine::core::{CommandOutcome, Value};
use crate::foundation::io::store::Robj;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;
use std::collections::HashSet;

#[command(
    "ZINTERCARD",
    runtime,
    get_keys = crate::framework::commands::key_extractors::numkeys_ro_get_keys
)]
pub fn zintercard(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
) -> CommandOutcome {
    CommandOutcome::reply(zintercard_reply(runtime, args))
}

fn zintercard_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    if args.args.len() < 2 {
        return CommandError::syntax_error();
    }
    let numkeys = match std::str::from_utf8(&args.args[0])
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
    {
        Some(n) if n > 0 => n,
        _ => return Value::error("ERR at least 1 input key is needed for 'zintercard' command"),
    };
    if 1 + numkeys > args.args.len() {
        return CommandError::syntax_error();
    }
    let mut limit = 0usize;
    let mut i = 1 + numkeys;
    while i < args.args.len() {
        if args.args[i].eq_ignore_ascii_case(b"limit") {
            if i + 1 >= args.args.len() {
                return CommandError::syntax_error();
            }
            let limit_val = match std::str::from_utf8(&args.args[i + 1])
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
            {
                Some(n) => n,
                None => return Value::error("ERR LIMIT value is not an integer or out of range"),
            };
            if limit_val < 0 {
                return Value::error("ERR LIMIT can't be negative");
            }
            limit = limit_val as usize;
            i += 2;
        } else {
            return CommandError::syntax_error();
        }
    }
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandError::db_index_out_of_range(),
    };
    let mut smallest: Option<HashSet<Bytes>> = None;
    for k in 0..numkeys {
        match db.get_zset_object(&args.args[1 + k]) {
            Ok(Some(m)) => {
                let set: HashSet<Bytes> = m
                    .zset_entries()
                    .into_iter()
                    .map(|(member, _)| member)
                    .collect();
                if smallest.is_none() || set.len() < smallest.as_ref().unwrap().len() {
                    smallest = Some(set);
                }
            }
            Ok(None) => return Value::integer(0),
            Err(()) => {
                // Check if it's a Set
                match db.get(&args.args[1 + k]) {
                    Some(Robj::SetHashtable(s)) => {
                        let set: HashSet<Bytes> = s.data.keys().cloned().collect();
                        if smallest.is_none() || set.len() < smallest.as_ref().unwrap().len() {
                            smallest = Some(set);
                        }
                    }
                    _ => return CommandError::wrong_type(),
                }
            }
        }
    }
    let mut smallest: HashSet<Bytes> = match smallest {
        Some(s) => s,
        None => return Value::integer(0),
    };
    for k in 0..numkeys {
        match db.get_zset_object(&args.args[1 + k]) {
            Ok(Some(m)) => {
                let set: HashSet<Bytes> = m
                    .zset_entries()
                    .into_iter()
                    .map(|(member, _)| member)
                    .collect();
                smallest = smallest.intersection(&set).cloned().collect();
                if limit > 0 && smallest.len() >= limit {
                    return Value::integer(limit as i64);
                }
            }
            Ok(None) => {}
            Err(()) => {
                // Check if it's a Set
                if let Some(Robj::SetHashtable(s)) = db.get(&args.args[1 + k]) {
                    let set: HashSet<Bytes> = s.data.keys().cloned().collect();
                    smallest = smallest.intersection(&set).cloned().collect();
                    if limit > 0 && smallest.len() >= limit {
                        return Value::integer(limit as i64);
                    }
                }
            }
        }
    }
    Value::integer(smallest.len() as i64)
}
