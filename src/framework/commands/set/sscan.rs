use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::pattern::string_match;
use crate::framework::{CommandArgs, CommandError};
use crate::foundation::memory::value::SetOps;
use command_macros::command;

#[command("SSCAN", runtime)]
pub fn sscan(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    CommandOutcome::reply(sscan_reply(runtime, args))
}

fn sscan_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    // SSCAN key cursor [MATCH pattern] [COUNT count]
    if args.args.len() < 2 {
        return CommandError::wrong_arity("sscan");
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
        } else {
            return CommandError::syntax_error();
        }
    }

    let Some(db) = runtime.db_mut() else {
        return CommandError::db_index_out_of_range();
    };
        db.set_no_lazy_expire(true);
        let enc_len_members = db
            .get_set(key)
            .map(|s| s.map(|s| (s.encoding(), s.len(), s.members())))
            .map_err(|_| ());
        let (encoding, set_len, members) = match enc_len_members {
            Err(_) => {
                db.set_no_lazy_expire(false);
                return CommandError::wrong_type();
            }
            Ok(None) => {
                db.set_no_lazy_expire(false);
                return Value::Array(vec![
                    Value::bulk(b"0".to_vec()),
                    Value::Array(Vec::new()),
                ]);
            }
            Ok(Some(triple)) => triple,
        };

        let match_all = match pattern {
            Some(p) => p == b"*",
            None => true,
        };

        use crate::foundation::memory::value::Encoding;

        let budget_threshold = count * 10;

        // Hashtable encoding + larger than budget threshold: call .scan()
        // directly on the underlying Dict (via ValkeySet.scan) to avoid O(N)
        // scratch Dict materialization.
        if encoding == Encoding::Hashtable && set_len > budget_threshold {
            let s = match db.get_set(key) {
                Ok(Some(s)) => s,
                _ => {
                    db.set_no_lazy_expire(false);
                    return Value::Array(vec![
                        Value::bulk(b"0".to_vec()),
                        Value::Array(Vec::new()),
                    ]);
                }
            };
            let mut out: Vec<Value> = Vec::new();
            let mut cur = cursor;
            let max_iterations = budget_threshold;
            let mut iterations = 0;
            let mut sampled = 0usize;
            // Align C db.c scanGenericCommand + hashtableScanCallback:
            // `sampled` is incremented per entry visited by the underlying
            // bucket-chain walk, not per scan() call. This matches the
            // C semantics `data->sampled++` inside hashtableScanCallback.
            loop {
                cur = s.scan(cur, |k, _v| {
                    sampled += 1;
                    if pattern
                        .map(|p| match_all || string_match(p, k, false))
                        .unwrap_or(true)
                    {
                        out.push(Value::bulk(k.clone()));
                    }
                });
                iterations += 1;
                if cur == 0 || sampled >= count || iterations >= max_iterations {
                    break;
                }
            }
            db.set_no_lazy_expire(false);
            let next_cursor_str = cur.to_string().into_bytes();
            return Value::Array(vec![
                Value::bulk(next_cursor_str),
                Value::Array(out),
            ]);
        }

        // Small encodings (intset / listpack) OR small hashtables that fit
        // inside the scan budget: return every matching element in one shot
        // with cursor=0, matching C's iterator path for compact encodings.
        let mut out: Vec<Value> = Vec::with_capacity(members.len());
        for elem in members {
            if pattern
                .map(|p| match_all || string_match(p, &elem, false))
                .unwrap_or(true)
            {
                out.push(Value::bulk(elem));
            }
        }
        db.set_no_lazy_expire(false);
        Value::Array(vec![Value::bulk(b"0".to_vec()), Value::Array(out)])
}
