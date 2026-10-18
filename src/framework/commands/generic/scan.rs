use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use command_macros::command;

#[command("SCAN", runtime)]
pub fn scan(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let Some((cursor, options)) = args.args.split_first() else {
        return CommandOutcome::reply(CommandError::wrong_arity("scan"));
    };
    let Some(cursor) = std::str::from_utf8(cursor)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
    else {
        return CommandOutcome::reply(Value::error("ERR invalid cursor"));
    };
    let mut count = 10usize;
    let mut pattern = None;
    let mut type_filter = None;
    let mut index = 0;
    while index < options.len() {
        if options[index].eq_ignore_ascii_case(b"count") && index + 1 < options.len() {
            count = match std::str::from_utf8(&options[index + 1])
                .ok()
                .and_then(|value| value.parse().ok())
            {
                Some(value) if value > 0 => value,
                _ => return CommandOutcome::reply(Value::error("ERR invalid count")),
            };
            index += 2;
        } else if options[index].eq_ignore_ascii_case(b"match") && index + 1 < options.len() {
            pattern = Some(options[index + 1].as_ref());
            index += 2;
        } else if options[index].eq_ignore_ascii_case(b"type") && index + 1 < options.len() {
            let value = options[index + 1].to_ascii_lowercase();
            if !matches!(
                value.as_slice(),
                b"string" | b"list" | b"hash" | b"set" | b"zset" | b"stream"
            ) {
                return CommandOutcome::reply(Value::error(format!(
                    "unknown type name '{}'",
                    String::from_utf8_lossy(&value)
                )));
            }
            type_filter = Some(value);
            index += 2;
        } else {
            return CommandOutcome::reply(CommandError::syntax_error());
        }
    }
    runtime.reset_replication_selected_db();
    let suppress_lazy_expire = runtime.suppress_lazy_expire();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    db.set_no_lazy_expire(suppress_lazy_expire);
    let (next, mut keys) = db.scan_include_expired(cursor, count);
    if let Some(type_filter) = type_filter {
        keys.retain(|key| {
            db.get_allow_expired(key).is_some_and(|value| {
                value
                    .type_name()
                    .as_bytes()
                    .eq_ignore_ascii_case(&type_filter)
            })
        });
    }
    if let Some(pattern) = pattern {
        keys.retain(|key| scan_match(pattern, key));
    }
    if !suppress_lazy_expire {
        let mut live_keys = Vec::with_capacity(keys.len());
        for key in keys {
            db.set_no_lazy_expire(true);
            let expired = db.is_expired(&key);
            db.set_no_lazy_expire(false);
            if expired {
                db.del(&key);
                db.expired_deletions.push(key);
            } else {
                live_keys.push(key);
            }
        }
        keys = live_keys;
    }
    db.set_no_lazy_expire(false);
    keys.sort();
    CommandOutcome::reply(Value::Array(vec![
        Value::bulk(next.to_string()),
        Value::Array(keys.into_iter().map(Value::bulk).collect()),
    ]))
}

fn scan_match(pattern: &[u8], value: &[u8]) -> bool {
    let (mut p, mut v, mut star, mut retry) = (0, 0, None, 0);
    while v < value.len() {
        if pattern.get(p) == Some(&b'*') {
            star = Some(p);
            p += 1;
            retry = v;
        } else if pattern.get(p) == value.get(v) || pattern.get(p) == Some(&b'?') {
            p += 1;
            v += 1;
        } else if let Some(position) = star {
            p = position + 1;
            retry += 1;
            v = retry;
        } else {
            return false;
        }
    }
    pattern[p..].iter().all(|byte| *byte == b'*')
}
