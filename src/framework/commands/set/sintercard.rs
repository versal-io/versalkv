use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;
use std::collections::HashSet;

#[command(
    "SINTERCARD",
    runtime,
    get_keys = crate::framework::commands::key_extractors::numkeys_ro_get_keys
)]
pub fn sintercard(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
) -> CommandOutcome {
    CommandOutcome::reply(sintercard_reply(runtime, args))
}

fn sintercard_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("sintercard");
    }

    let numkeys = match std::str::from_utf8(&args.args[0])
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
    {
        Some(n) if n > 0 => n,
        Some(_) => return Value::error("ERR numkeys should be greater than 0"),
        None => return Value::error("ERR numkeys is not an integer or out of range"),
    };

    if args.args.len() < 1 + numkeys {
        return Value::error(
            "ERR Number of keys in request don't match the number of keys actually provided",
        );
    }

    let keys = &args.args[1..1 + numkeys];
    let rest = &args.args[1 + numkeys..];

    let mut limit: usize = 0;
    let mut i = 0;
    while i < rest.len() {
        if rest[i].eq_ignore_ascii_case(b"LIMIT") {
            if i + 1 >= rest.len() {
                return CommandError::syntax_error();
            }
            limit = match std::str::from_utf8(&rest[i + 1])
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
            {
                Some(n) if n >= 0 => n as usize,
                Some(_) => return Value::error("ERR LIMIT is negative"),
                None => return Value::error("ERR LIMIT is not an integer or out of range"),
            };
            i += 2;
        } else {
            return CommandError::syntax_error();
        }
    }

    let Some(db) = runtime.db_mut() else {
        return CommandError::db_index_out_of_range();
    };
        for key in keys {
            if db.get_set(key).is_err() {
                return CommandError::wrong_type();
            }
        }

        let mut sets: Vec<HashSet<Bytes>> = Vec::new();
        for key in keys {
            match db.get_set(key) {
                Ok(None) => return Value::integer(0),
                Ok(Some(s)) => sets.push(s.data.keys().cloned().collect()),
                Err(_) => return CommandError::wrong_type(),
            }
        }

        sets.sort_by_key(|s| s.len());

        let smallest = &sets[0];
        let mut count = 0;

        for m in smallest.iter() {
            let mut all_have = true;
            for s in &sets[1..] {
                if !s.contains(m) {
                    all_have = false;
                    break;
                }
            }
            if all_have {
                count += 1;
                if limit > 0 && count >= limit {
                    break;
                }
            }
        }

        Value::integer(count as i64)
}
