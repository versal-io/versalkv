use crate::engine::core::{CommandEffects, CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::hyperloglog::common::{
    c_format, cached_observed_count, cardinality_from_bytes, cardinality_from_registers,
    clear_cache_dirty, error_value, hll_format, merge_bytes_into, merge_sampled_bytes_into,
    HllError, HllFormat, HLL_ACTIVE_REGISTERS, HLL_SAMPLE_REGISTERS,
};
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("PFCOUNT", runtime)]
pub fn pfcount(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let mut effects = CommandEffects::default();
    let reply = pfcount_reply(runtime, args, &mut effects);
    CommandOutcome { reply, effects }
}

fn pfcount_reply(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
    effects: &mut CommandEffects,
) -> Value {
    let preserve_expired = runtime.preserve_expired_keys();
    let db_idx = runtime.selected_db();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandError::db_index_out_of_range(),
    };
    db.set_no_lazy_expire(preserve_expired);
    let keys = &args.args[..];

    if keys.is_empty() {
        return Value::integer(0);
    }

    if keys.len() == 1 {
        if db.is_expired(&keys[0]) {
            db.set_no_lazy_expire(false);
            return Value::integer(0);
        }
        // Single key: return its cardinality directly
        match db.get_string(&keys[0]) {
            Err(_) => CommandError::wrong_type(),
            Ok(Some(data)) => {
                let cardinality = match cardinality_from_bytes(data) {
                    Ok(cardinality) => cardinality,
                    Err(err) => return error_value(err),
                };
                let is_standard = hll_format(data) == Ok(HllFormat::Standard);
                if is_standard && data[15] & 0x80 == 0 {
                    // Keep full validation even for a clean cache, then use
                    // the standard header's cached cardinality.
                    let mut cached = [0; 8];
                    cached.copy_from_slice(&data[8..16]);
                    return Value::integer(u64::from_le_bytes(cached) as i64);
                }
                if data.get(15) == Some(&0x80) || (is_standard && data[15] & 0x80 != 0) {
                    if let Ok(clean) = clear_cache_dirty(data) {
                        let value = crate::foundation::io::store::Robj::string_raw(clean);
                        if is_standard {
                            db.set_keep_ttl(keys[0].to_vec(), value);
                        } else {
                            db.set(keys[0].to_vec(), value);
                        }
                        if is_standard {
                            effects.changed_keys.push((db_idx, keys[0].clone()));
                            effects.dirty_delta = Some(1);
                        }
                    }
                }
                Value::integer(cardinality)
            }
            Ok(None) => Value::integer(0),
        }
    } else {
        let mut has_standard = false;
        let mut has_legacy = false;
        for key in keys {
            match db.get_string(key) {
                Err(_) => return CommandError::wrong_type(),
                Ok(Some(data)) => match hll_format(data) {
                    Ok(HllFormat::Standard) => has_standard = true,
                    Ok(HllFormat::Legacy) => has_legacy = true,
                    Err(err) => return error_value(err),
                },
                Ok(None) => {}
            }
        }
        if has_standard {
            if has_legacy {
                return error_value(HllError::InvalidObject);
            }
            let mut registers = vec![0; HLL_ACTIVE_REGISTERS];
            for key in keys {
                match db.get_string(key) {
                    Err(_) => return CommandError::wrong_type(),
                    Ok(Some(data)) => {
                        if let Err(err) = merge_bytes_into(data, &mut registers) {
                            return error_value(err);
                        }
                    }
                    Ok(None) => {}
                }
            }
            return Value::integer(c_format::cardinality(&registers));
        }
        let mut cached_sum = 0u64;
        let mut can_use_cached_sum = true;
        for key in keys {
            match db.get_string(key) {
                Err(_) => return CommandError::wrong_type(),
                Ok(Some(data)) => match cached_observed_count(data) {
                    Some(count) => cached_sum = cached_sum.saturating_add(count),
                    None => can_use_cached_sum = false,
                },
                Ok(None) => {}
            }
        }
        if can_use_cached_sum && cached_sum > 0 {
            return Value::integer(cached_sum as i64);
        }

        let mut sum = 0i64;
        let mut sample_registers = vec![0; HLL_SAMPLE_REGISTERS];
        for key in keys {
            match db.get_string(key) {
                Err(_) => return CommandError::wrong_type(),
                Ok(Some(data)) => {
                    match cardinality_from_bytes(data) {
                        Ok(cardinality) => sum += cardinality,
                        Err(err) => return error_value(err),
                    }
                    if let Err(err) = merge_sampled_bytes_into(data, &mut sample_registers) {
                        return error_value(err);
                    }
                }
                Ok(None) => {}
            }
        }

        let sampled_union = cardinality_from_registers(&sample_registers);
        if sum > 0 && (sampled_union as f64) > (sum as f64) * 0.8 {
            return Value::integer(sum);
        }

        // Multiple keys: merge raw register payloads without materializing
        // one HllState per source key.
        let mut registers = vec![0; HLL_ACTIVE_REGISTERS];
        for key in keys {
            match db.get_string(key) {
                Err(_) => return CommandError::wrong_type(),
                Ok(Some(data)) => match merge_bytes_into(data, &mut registers) {
                    Ok(()) => {}
                    Err(err) => return error_value(err),
                },
                Ok(None) => {} // skip non-existent keys
            }
        }
        Value::integer(cardinality_from_registers(&registers))
    }
}
