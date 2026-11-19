use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandEffects, CommandOutcome, Value};
use crate::foundation::io::store::Robj;
use crate::foundation::memory::metrics::{memory_stats, MemoryCategory};
use crate::framework::commands::command_runtime::CommandRuntime;
use command_macros::command;

#[command("INCR", runtime)]
pub fn incr(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    increment(runtime, args, 1, "incr")
}

pub(crate) fn increment(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
    delta: i64,
    name: &str,
) -> CommandOutcome {
    let [key, ..] = args.args.as_slice() else {
        return CommandOutcome::reply(CommandError::wrong_arity(name));
    };
    if args.len()
        != if matches!(name, "incr" | "decr") {
            1
        } else {
            2
        }
    {
        return CommandOutcome::reply(CommandError::wrong_arity(name));
    }
    let db_index = runtime.selected_db();
    let preserve_ttl = runtime.read_expired_values();
    let suppress_lazy_expire = runtime.suppress_lazy_expire();
    let now_ms = runtime.now_ms();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    db.set_no_lazy_expire(suppress_lazy_expire);

    // Fast path: existing integer-encoded value is read and updated in place,
    // skipping the dict remove+insert round-trip and the string round-trip
    // (as_bytes -> parse -> to_string -> int_encodable). This mirrors C's
    // in-place `*(long*)o->ptr += incr` for OBJ_ENCODING_INT values.
    if !preserve_ttl {
        let next = {
            let obj = db.get_mut_at(key, now_ms);
            if let Some(Robj::StrInt(strint)) = obj {
                let current = strint.value;
                let Some(next) = current.checked_add(delta) else {
                    db.set_no_lazy_expire(false);
                    return CommandOutcome::reply(Value::error(
                        "ERR increment or decrement would overflow",
                    ));
                };
                let old_len = strint.len();
                strint.set_value(next);
                let new_len = strint.len();
                if old_len != new_len {
                    let key_len = key.len();
                    // StrInt estimated_size is len + 16; adjust only the delta.
                    memory_stats::track_sub(MemoryCategory::Dataset, key_len + old_len + 16);
                    memory_stats::track_add(MemoryCategory::Dataset, key_len + new_len + 16);
                }
                Some(next)
            } else {
                None
            }
        };
        if let Some(next) = next {
            db.remove_expire(key);
            db.set_no_lazy_expire(false);
            return CommandOutcome {
                reply: Value::integer(next),
                effects: CommandEffects {
                    changed_keys: vec![(db_index, key.clone())],
                    ..CommandEffects::default()
                },
            };
        }
    }

    let current: i64 = if preserve_ttl {
        match db.get_string_allow_expired(key) {
            Ok(None) => 0,
            Ok(Some(bytes)) => match parse_integer(bytes) {
                Some(v) => v,
                None => {
                    db.set_no_lazy_expire(false);
                    return CommandOutcome::reply(Value::error(
                        "ERR value is not an integer or out of range",
                    ));
                }
            },
            Err(_) => {
                db.set_no_lazy_expire(false);
                return CommandOutcome::reply(CommandError::wrong_type());
            }
        }
    } else {
        match db.get_at(key, now_ms) {
            // Integer-encoded value: read the stored i64 directly, skipping
            // the as_bytes -> parse round-trip.
            Some(Robj::StrInt(strint)) => strint.value,
            Some(value) if value.is_string() => match parse_integer(value.as_bytes()) {
                Some(v) => v,
                None => {
                    db.set_no_lazy_expire(false);
                    return CommandOutcome::reply(Value::error(
                        "ERR value is not an integer or out of range",
                    ));
                }
            },
            Some(_) => {
                db.set_no_lazy_expire(false);
                return CommandOutcome::reply(CommandError::wrong_type());
            }
            None => 0,
        }
    };
    let Some(next) = current.checked_add(delta) else {
        db.set_no_lazy_expire(false);
        return CommandOutcome::reply(Value::error("ERR increment or decrement would overflow"));
    };
    let object = Robj::from_i64(next);
    if preserve_ttl {
        db.set_keep_ttl_slice(key, object);
    } else {
        db.set_slice(key, object);
    }
    db.set_no_lazy_expire(false);
    CommandOutcome {
        reply: Value::integer(next),
        effects: CommandEffects {
            changed_keys: vec![(db_index, key.clone())],
            ..CommandEffects::default()
        },
    }
}

/// Parse a decimal integer from ASCII bytes in a single pass.
///
/// Mirrors the accepted set of `str::parse::<i64>()` (optional sign,
/// leading zeros, overflow rejection) but avoids the extra UTF-8
/// validation pass that `from_utf8` performs over every byte.
pub(crate) fn parse_integer(value: &[u8]) -> Option<i64> {
    let mut bytes = value;
    let negative = match bytes.first()? {
        b'-' => {
            bytes = &bytes[1..];
            true
        }
        b'+' => {
            bytes = &bytes[1..];
            false
        }
        _ => false,
    };
    if bytes.is_empty() {
        return None;
    }
    let mut result: i64 = 0;
    for &b in bytes {
        if !b.is_ascii_digit() {
            return None;
        }
        let digit = (b - b'0') as i64;
        result = result
            .checked_mul(10)?
            .checked_add(if negative { -digit } else { digit })?;
    }
    Some(result)
}
