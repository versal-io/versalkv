use crate::engine::core::{CommandOutcome, Value};
use crate::foundation::io::store::structure::dict::Dict;
use crate::foundation::memory::value::Encoding;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::pattern::string_match;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[command("HSCAN", runtime)]
pub fn hscan(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    CommandOutcome::reply(hscan_reply(runtime, args))
}

fn hscan_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    // HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]
    if args.args.len() < 2 {
        return CommandError::wrong_arity("hscan");
    }

    let key = &args.args[0];

    let cursor_str = match std::str::from_utf8(&args.args[1]) {
        Ok(s) => s,
        Err(_) => return Value::error("ERR invalid cursor"),
    };
    let cursor: u64 = match cursor_str.parse() {
        Ok(c) => c,
        Err(_) => return Value::error("ERR invalid cursor"),
    };

    let mut count: usize = 10;
    let mut pattern: Option<&[u8]> = None;
    let mut novalues: bool = false;

    let mut i = 2;
    while i < args.args.len() {
        let opt = match std::str::from_utf8(&args.args[i]) {
            Ok(s) => s,
            Err(_) => return CommandError::syntax_error(),
        };
        if opt.eq_ignore_ascii_case("count") && i + 1 < args.args.len() {
            let count_str = match std::str::from_utf8(&args.args[i + 1]) {
                Ok(s) => s,
                Err(_) => return CommandError::syntax_error(),
            };
            count = match count_str.parse() {
                Ok(c) if c > 0 => c,
                _ => return Value::error("ERR invalid count"),
            };
            i += 2;
        } else if opt.eq_ignore_ascii_case("match") && i + 1 < args.args.len() {
            pattern = Some(&args.args[i + 1]);
            i += 2;
        } else if opt.eq_ignore_ascii_case("novalues") {
            novalues = true;
            i += 1;
        } else {
            return CommandError::syntax_error();
        }
    }

    let read_expired_values = runtime.read_expired_values();
    let now_ms = runtime.now_ms();
    let Some(db) = runtime.db_mut() else {
        return CommandError::db_index_out_of_range();
    };

        let (encoding, hash_len, mut pairs): (Encoding, usize, Vec<(Bytes, Bytes)>) = {
            let hash = match db.get_hash_object(key) {
                Err(_) => {
                    return CommandError::wrong_type();
                }
                Ok(None) => {
                    return Value::Array(vec![
                        Value::bulk(b"0".to_vec()),
                        Value::Array(Vec::new()),
                    ]);
                }
                Ok(Some(h)) => h,
            };
            let encoding = hash.encoding();
            let len = hash.hash_len();
            let collected = hash.hash_entries();
            (encoding, len, collected)
        };
        pairs.retain(|(f, _)| read_expired_values || !db.hash_field_expired_at(key, f, now_ms));

        let match_all = match pattern {
            Some(p) => p == b"*",
            None => true,
        };

        let budget_threshold = count * 10;
        let target = if novalues { count } else { count * 2 };

        if encoding == Encoding::Hashtable && hash_len > budget_threshold {
            let mut scratch: Dict<Bytes, Bytes> = Dict::new();
            for (f, v) in pairs {
                scratch.insert(f, v);
            }
            let mut out: Vec<Value> = Vec::new();
            let mut cur = cursor;
            let max_iterations = budget_threshold;
            let mut iterations = 0;
            let mut sampled = 0usize;
            loop {
                cur = scratch.scan(cur, |f, v| {
                    sampled += 1;
                    if pattern
                        .map(|p| match_all || string_match(p, f, false))
                        .unwrap_or(true)
                    {
                        out.push(Value::bulk(f.clone()));
                        if !novalues {
                            out.push(Value::bulk(v.clone()));
                        }
                    }
                });
                iterations += 1;
                if cur == 0 || sampled >= count || iterations >= max_iterations {
                    break;
                }
                if out.len() >= target {
                    break;
                }
            }
            let next_cursor_str = cur.to_string().into_bytes();
            return Value::Array(vec![Value::bulk(next_cursor_str), Value::Array(out)]);
        }

        let mut out: Vec<Value> = Vec::new();
        for (f, v) in pairs {
            if pattern
                .map(|p| match_all || string_match(p, &f, false))
                .unwrap_or(true)
            {
                out.push(Value::bulk(f));
                if !novalues {
                    out.push(Value::bulk(v));
                }
            }
        }
        Value::Array(vec![Value::bulk(b"0".to_vec()), Value::Array(out)])
}
