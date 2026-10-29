use super::basic::fmt_score;
use crate::engine::core::{CommandOutcome, Value};
use crate::foundation::io::store::Robj;
use crate::foundation::memory::value::Encoding;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::pattern::string_match;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[command("ZSCAN", runtime)]
pub fn zscan(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    CommandOutcome::reply(zscan_reply(runtime, args))
}

fn zscan_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    // ZSCAN key cursor [MATCH pattern] [COUNT count] [NOSCORES]
    if args.args.len() < 2 {
        return CommandError::wrong_arity("zscan");
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
    let mut noscores: bool = false;

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
        } else if opt.eq_ignore_ascii_case("noscores") {
            noscores = true;
            i += 1;
        } else {
            return CommandError::syntax_error();
        }
    }

    let Some(db) = runtime.db_mut() else {
        return CommandError::db_index_out_of_range();
    };
        db.set_no_lazy_expire(true);

        let enc_len_zref: Result<Option<(Encoding, usize, _)>, ()> = db
            .get_zset_object(key)
            .map(|z| z.map(|z| (z.encoding(), z.zset_len(), z)))
            .map_err(|_| ());
        let (encoding, zset_len, zref) = match enc_len_zref {
            Err(_) => {
                db.set_no_lazy_expire(false);
                return CommandError::wrong_type();
            }
            Ok(None) => {
                db.set_no_lazy_expire(false);
                return Value::Array(vec![Value::bulk(b"0".to_vec()), Value::Array(Vec::new())]);
            }
            Ok(Some(triple)) => triple,
        };

        let match_all = match pattern {
            Some(p) => p == b"*",
            None => true,
        };

        let budget_threshold = count * 10;
        let target = if noscores { count } else { count * 2 };

        if encoding == Encoding::Btree
            && zset_len > budget_threshold
            && matches!(zref, Robj::ZSetSkiplist(_))
        {
            let Robj::ZSetSkiplist(zref) = zref else {
                unreachable!("btree encoding must have an ordered zset payload");
            };
            let mut out: Vec<Value> = Vec::new();
            let mut cur = cursor;
            let max_iterations = budget_threshold;
            let mut iterations = 0;
            let mut sampled = 0usize;
            // Align C db.c scanGenericCommand + hashtableScanCallback:
            // `sampled` is incremented per entry visited by the underlying
            // bucket-chain walk, not per scan() call.
            loop {
                cur = zref.dict().scan(cur, |m, s| {
                    sampled += 1;
                    if pattern
                        .map(|p| match_all || string_match(p, m, false))
                        .unwrap_or(true)
                    {
                        out.push(Value::bulk(m.clone()));
                        if !noscores {
                            out.push(Value::bulk(fmt_score(*s)));
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
            db.set_no_lazy_expire(false);
            let next_cursor_str = cur.to_string().into_bytes();
            return Value::Array(vec![Value::bulk(next_cursor_str), Value::Array(out)]);
        }

        // Small encoding (listpack) OR small skiplists fitting the scan
        // budget: return every matching pair once with cursor=0.
        let small_iter: Vec<(Bytes, f64)> = zref.zset_entries();
        let mut out: Vec<Value> = Vec::new();
        for (m, s) in small_iter {
            if pattern
                .map(|p| match_all || string_match(p, &m, false))
                .unwrap_or(true)
            {
                out.push(Value::bulk(m));
                if !noscores {
                    out.push(Value::bulk(fmt_score(s)));
                }
            }
        }
        db.set_no_lazy_expire(false);
        Value::Array(vec![Value::bulk(b"0".to_vec()), Value::Array(out)])
}
