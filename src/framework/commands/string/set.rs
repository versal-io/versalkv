use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::foundation::io::store::Robj;
use crate::framework::commands::command_runtime::CommandRuntime;
use bytes::Bytes;
use command_macros::command;

#[command(
    "SET",
    runtime,
    get_keys = crate::framework::commands::key_extractors::set_get_keys
)]
pub fn set(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let [key, value, options @ ..] = args.args.as_slice() else {
        return CommandOutcome::reply(CommandError::wrong_arity("set"));
    };
    let now_ms = runtime.now_ms();
    let db_index = runtime.selected_db();
    let import_mode = runtime.suppress_lazy_expire();
    let read_expired_values = runtime.read_expired_values();
    let should_mark_replica_local_expire = runtime.should_mark_replica_local_expire();
    let opts = match parse_set_options(options, now_ms) {
        Ok(options) => options,
        Err(reply) => return CommandOutcome::reply(reply),
    };
    let key = key.clone();
    let value = Robj::string(value.clone());
    let old_value = {
        let Some(db) = runtime.db_mut() else {
            return CommandOutcome::reply(CommandError::db_index_out_of_range());
        };
        db.set_no_lazy_expire(import_mode);
        let old = if read_expired_values {
            db.get_allow_expired(&key).cloned()
        } else {
            db.get_at(&key, now_ms).cloned()
        };
        db.set_no_lazy_expire(false);
        old
    };
    if let ExpireOption::At(deadline_ms) = opts.expire {
        if deadline_ms <= now_ms as i64 && !import_mode {
            if matches!(opts.condition, ConditionOption::Nx) {
                return CommandOutcome {
                    reply: Value::null(),
                    effects: CommandEffects {
                        skip_propagate: true,
                        ..CommandEffects::default()
                    },
                };
            }
            let mut effects = CommandEffects::default();
            if old_value.is_some() {
                let Some(db) = runtime.db_mut() else {
                    return CommandOutcome::reply(CommandError::db_index_out_of_range());
                };
                db.del(&key);
                effects.changed_keys.push((db_index, key.clone()));
                effects
                    .custom_propagations
                    .push(vec![Bytes::from_static(b"UNLINK"), key.clone()]);
            } else {
                effects.skip_propagate = true;
            }
            return CommandOutcome {
                reply: get_set_reply(opts.get, old_value),
                effects,
            };
        }
    }
    let should_set = match opts.condition {
        ConditionOption::None => true,
        ConditionOption::Nx => old_value.is_none(),
        ConditionOption::Xx => old_value.is_some(),
        ConditionOption::IfEq => match &old_value {
            Some(object) if object.is_string() => {
                object.as_bytes() == opts.ifeq_value.as_deref().unwrap_or_default()
            }
            Some(_) => return CommandOutcome::reply(CommandError::wrong_type()),
            None => false,
        },
    };
    if opts.get && old_value.as_ref().is_some_and(|object| !object.is_string()) {
        return CommandOutcome::reply(CommandError::wrong_type());
    }
    if !should_set {
        return CommandOutcome {
            reply: conditional_set_reply(opts.get, old_value),
            effects: CommandEffects {
                skip_propagate: true,
                ..CommandEffects::default()
            },
        };
    }
    let mut effects = CommandEffects {
        changed_keys: vec![(db_index, key.clone())],
        events: vec![CoreEvent {
            kind: "set",
            key: key.clone(),
        }],
        ..CommandEffects::default()
    };
    if old_value.is_none() {
        effects.events.push(CoreEvent {
            kind: "new",
            key: key.clone(),
        });
    }
    if let ExpireOption::At(deadline_ms) = opts.expire {
        effects.events.push(CoreEvent {
            kind: "expire",
            key: key.clone(),
        });
        if opts.expire_needs_propagation_conversion {
            effects.custom_propagations.push(vec![
                Bytes::from_static(b"SET"),
                key.clone(),
                Bytes::copy_from_slice(value.as_bytes()),
                Bytes::from_static(b"PXAT"),
                deadline_ms.to_string().into(),
            ]);
        }
    }
    {
        let Some(db) = runtime.db_mut() else {
            return CommandOutcome::reply(CommandError::db_index_out_of_range());
        };
        match opts.expire {
            ExpireOption::None => db.set(key.clone().to_vec(), value),
            ExpireOption::KeepTtl => db.set_keep_ttl(key.clone().to_vec(), value),
            ExpireOption::At(deadline_ms) => {
                db.set(key.clone().to_vec(), value);
                db.set_expire(&key, deadline_ms as u64);
                if should_mark_replica_local_expire {
                    db.mark_replica_local_expire(&key);
                }
            }
        }
    }
    CommandOutcome {
        reply: get_set_reply(opts.get, old_value),
        effects,
    }
}

enum ExpireOption {
    None,
    KeepTtl,
    At(i64),
}
enum ConditionOption {
    None,
    Nx,
    Xx,
    IfEq,
}
struct SetOptions {
    expire: ExpireOption,
    expire_needs_propagation_conversion: bool,
    condition: ConditionOption,
    get: bool,
    ifeq_value: Option<Vec<u8>>,
}

fn parse_set_options(options: &[Bytes], now_ms: u64) -> Result<SetOptions, Value> {
    let mut parsed = SetOptions {
        expire: ExpireOption::None,
        expire_needs_propagation_conversion: false,
        condition: ConditionOption::None,
        get: false,
        ifeq_value: None,
    };
    let mut index = 0;
    while index < options.len() {
        let option = String::from_utf8_lossy(&options[index]).to_ascii_uppercase();
        match option.as_str() {
            "KEEPTTL" => {
                if !matches!(parsed.expire, ExpireOption::None) {
                    return Err(CommandError::syntax_error());
                }
                parsed.expire = ExpireOption::KeepTtl;
                index += 1;
            }
            "EX" | "PX" | "EXAT" | "PXAT" => {
                if !matches!(parsed.expire, ExpireOption::None) || index + 1 >= options.len() {
                    return Err(CommandError::syntax_error());
                }
                let Some(value) = std::str::from_utf8(&options[index + 1])
                    .ok()
                    .and_then(|value| value.trim().parse::<i64>().ok())
                else {
                    return Err(Value::error("ERR value is not an integer or out of range"));
                };
                let deadline_ms = match option.as_str() {
                    "EX" if value > 0 => value
                        .checked_mul(1_000)
                        .and_then(|ttl| ttl.checked_add(now_ms as i64)),
                    "PX" if value > 0 => value.checked_add(now_ms as i64),
                    "EXAT" if value > 0 => value.checked_mul(1_000),
                    "PXAT" if value > 0 => Some(value),
                    _ => return Err(invalid_expire()),
                };
                let Some(deadline_ms) = deadline_ms else {
                    return Err(invalid_expire());
                };
                parsed.expire = ExpireOption::At(deadline_ms);
                parsed.expire_needs_propagation_conversion = option != "PXAT";
                index += 2;
            }
            "NX" | "XX" => {
                if !matches!(parsed.condition, ConditionOption::None) {
                    return Err(CommandError::syntax_error());
                }
                parsed.condition = if option == "NX" {
                    ConditionOption::Nx
                } else {
                    ConditionOption::Xx
                };
                index += 1;
            }
            "GET" => {
                parsed.get = true;
                index += 1;
            }
            "IFEQ" => {
                if !matches!(parsed.condition, ConditionOption::None) || index + 1 >= options.len()
                {
                    return Err(CommandError::syntax_error());
                }
                parsed.condition = ConditionOption::IfEq;
                parsed.ifeq_value = Some(options[index + 1].to_vec());
                index += 2;
            }
            _ => return Err(CommandError::syntax_error()),
        }
    }
    Ok(parsed)
}
fn get_set_reply(get: bool, old_value: Option<Robj>) -> Value {
    if !get {
        return Value::ok();
    }
    match old_value {
        Some(value) => Value::bulk(value.as_bytes().to_vec()),
        None => Value::null(),
    }
}
fn conditional_set_reply(get: bool, old_value: Option<Robj>) -> Value {
    if get {
        get_set_reply(true, old_value)
    } else {
        Value::null()
    }
}
fn invalid_expire() -> Value {
    Value::error("ERR invalid expire time in 'set' command")
}
