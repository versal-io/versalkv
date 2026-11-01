use super::runtime_support::{parse_i64_range, ParseIntResult, NOT_INT};
use crate::engine::core::{CommandEffects, CommandOutcome, Value};
use crate::foundation::io::store::kv_engine::Db;
use crate::foundation::io::sys::rand;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[command("HRANDFIELD", runtime)]
pub fn hrandfield(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
) -> CommandOutcome {
    let hard_output_limit = runtime.normal_output_buffer_hard_limit();
    let output_buffer_memory_usage = runtime.output_buffer_memory_usage();
    let resp3 = runtime.resp3();
    let now_ms = runtime.now_ms();
    let ignore_expiry = runtime.read_expired_values();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let (reply, close_connection) = hrandfield_reply(
        db,
        args,
        resp3,
        hard_output_limit,
        output_buffer_memory_usage,
        now_ms,
        ignore_expiry,
    );
    CommandOutcome {
        reply,
        effects: CommandEffects {
            suppress_reply: close_connection,
            close_connection,
            ..CommandEffects::default()
        },
    }
}

fn hrandfield_reply(
    db: &mut Db,
    args: &CommandArgs,
    resp3: bool,
    hard_output_limit: usize,
    output_buffer_memory_usage: usize,
    now_ms: u64,
    ignore_expiry: bool,
) -> (Value, bool) {
    let key = &args.args[0];

    if args.args.len() == 1 {
        let is_hashtable = matches!(db.get_no_expire(key), Some(crate::foundation::io::store::Robj::HashHashtable(_)));
        let fields: Vec<Bytes> = match db.get_hash_object(key) {
            Err(_) => return (CommandError::wrong_type(), false),
            Ok(None) => return (Value::null(), false),
            Ok(Some(hash)) => hash
                .hash_entries()
                .into_iter()
                .map(|(field, _)| field)
                .collect(),
        };
        // C first probes physical hashtable entries for a live field, then
        // falls back to a policy-aware reservoir when all probes miss.
        if is_hashtable && !fields.is_empty() {
            for _ in 0..100 {
                let field = &fields[rand::random_usize(fields.len())];
                if !db.hash_field_expired_at(key, field, now_ms) {
                    return (Value::bulk(field.clone()), false);
                }
            }
        }
        let valid: Vec<&Bytes> = fields
            .iter()
            .filter(|f| ignore_expiry || !db.hash_field_expired_at(key, f, now_ms))
            .collect();
        if valid.is_empty() {
            return (Value::null(), false);
        }
        let idx = rand::random_usize(valid.len());
        return (Value::bulk(valid[idx].clone()), false);
    }

    let count_str = &args.args[1];
    let count = match parse_i64_range(count_str, -i64::MAX, i64::MAX) {
        ParseIntResult::Ok(n) => n,
        ParseIntResult::NotInteger => return (Value::error(NOT_INT), false),
        ParseIntResult::OutOfRange => return (Value::error("ERR value is out of range"), false),
    };

    let withvalues = if args.args.len() > 2 {
        let wv = std::str::from_utf8(&args.args[2]).unwrap_or("");
        if !wv.eq_ignore_ascii_case("withvalues") {
            return (Value::error("ERR syntax error"), false);
        }
        if args.args.len() > 3 {
            return (Value::error("ERR syntax error"), false);
        }
        if !(-i64::MAX / 2..=i64::MAX / 2).contains(&count) {
            return (Value::error("ERR value is out of range"), false);
        }
        true
    } else {
        false
    };

    let all_pairs: Vec<(Bytes, Bytes)> = match db.get_hash_object(key) {
        Err(_) => return (CommandError::wrong_type(), false),
        Ok(None) => return (Value::Array(Vec::new()), false),
        Ok(Some(hash)) => hash.hash_entries(),
    };

    let entries: Vec<(Bytes, Bytes)> = all_pairs
        .into_iter()
        .filter(|(f, _)| ignore_expiry || !db.hash_field_expired_at(key, f, now_ms))
        .collect();

    let mut out: Vec<Value> = Vec::new();
    if entries.is_empty() {
        return (Value::Array(out), false);
    }
    let abs_count = count.unsigned_abs() as usize;
    if count < 0 && hard_output_limit > 0 {
        let minimum_item_bytes = entries
            .iter()
            .map(|(field, value)| {
                let field_bytes = resp_bulk_encoded_len(field.len());
                if withvalues {
                    let pair_bytes = field_bytes.saturating_add(resp_bulk_encoded_len(value.len()));
                    if resp3 {
                        pair_bytes.saturating_add(4)
                    } else {
                        pair_bytes
                    }
                } else {
                    field_bytes
                }
            })
            .min()
            .unwrap_or(0);
        let projected = resp_array_header_len(abs_count.saturating_mul(if withvalues && !resp3 {
            2
        } else {
            1
        }))
        .saturating_add(abs_count.saturating_mul(minimum_item_bytes))
        .saturating_add(output_buffer_memory_usage);
        if projected >= hard_output_limit {
            return (Value::Null, true);
        }
    }
    if count >= 0 {
        let n = abs_count.min(entries.len());
        let mut indices: Vec<usize> = (0..entries.len()).collect();
        rand::shuffle(&mut indices);
        for i in 0..n {
            let (f, v) = &entries[indices[i]];
            if withvalues && resp3 {
                out.push(Value::Array(vec![
                    Value::bulk(f.clone()),
                    Value::bulk(v.clone()),
                ]));
            } else {
                out.push(Value::bulk(f.clone()));
                if withvalues {
                    out.push(Value::bulk(v.clone()));
                }
            }
        }
    } else {
        for _ in 0..abs_count {
            let idx = rand::random_usize(entries.len());
            let (f, v) = &entries[idx];
            if withvalues && resp3 {
                out.push(Value::Array(vec![
                    Value::bulk(f.clone()),
                    Value::bulk(v.clone()),
                ]));
            } else {
                out.push(Value::bulk(f.clone()));
                if withvalues {
                    out.push(Value::bulk(v.clone()));
                }
            }
        }
    }
    (Value::Array(out), false)
}

fn decimal_digits(value: usize) -> usize {
    if value == 0 {
        1
    } else {
        value.ilog10() as usize + 1
    }
}

fn resp_bulk_encoded_len(len: usize) -> usize {
    1usize
        .saturating_add(decimal_digits(len))
        .saturating_add(2)
        .saturating_add(len)
        .saturating_add(2)
}

fn resp_array_header_len(len: usize) -> usize {
    1usize.saturating_add(decimal_digits(len)).saturating_add(2)
}
