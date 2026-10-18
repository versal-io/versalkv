use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::foundation::io::store::Robj;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[command("APPEND", runtime)]
pub fn append(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let key = &args.args[0];
    let suffix = &args.args[1];
    let db_idx = runtime.selected_db();
    let len;

    {
        let db = match runtime.db_mut() {
            Some(d) => d,
            None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
        };
        let mut buf = match db.get_string(key) {
            Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
            Ok(None) => Vec::new(),
            Ok(Some(b)) => b.to_vec(),
        };
        buf.extend_from_slice(suffix);
        len = buf.len() as i64;
        db.set(key.to_vec(), Robj::string_raw(Bytes::from(buf)));
    }

    changed_key_outcome(Value::integer(len), db_idx, key, "append")
}

#[command("SETRANGE", runtime)]
pub fn setrange(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let key = &args.args[0];
    let db_idx = runtime.selected_db();
    let offset = match std::str::from_utf8(&args.args[1])
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
    {
        Some(n) if n >= 0 => n as usize,
        Some(_) => return CommandOutcome::reply(Value::error("ERR offset is out of range")),
        None => {
            return CommandOutcome::reply(Value::error(
                "ERR value is not an integer or out of range",
            ))
        }
    };
    let patch = &args.args[2];

    let len;
    {
        let db = match runtime.db_mut() {
            Some(d) => d,
            None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
        };
        let mut buf = match db.get_string(key) {
            Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
            Ok(None) => Vec::new(),
            Ok(Some(b)) => b.to_vec(),
        };
        if patch.is_empty() {
            return CommandOutcome::reply(Value::integer(buf.len() as i64));
        }
        let end = offset.saturating_add(patch.len());
        if end > MAX_STRING_SIZE {
            return CommandOutcome::reply(Value::error(
                "ERR string exceeds maximum allowed size",
            ));
        }
        if buf.len() < end {
            buf.resize(end, 0);
        }
        buf[offset..end].copy_from_slice(patch);
        len = buf.len() as i64;
        db.set(key.to_vec(), Robj::string_raw(Bytes::from(buf)));
    }

    changed_key_outcome(Value::integer(len), db_idx, key, "setrange")
}

#[command("STRLEN", runtime)]
pub fn strlen(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    CommandOutcome::reply(match db.get_string(&args.args[0]) {
        Err(_) => CommandError::wrong_type(),
        Ok(None) => Value::integer(0),
        Ok(Some(b)) => Value::integer(b.len() as i64),
    })
}

const MAX_BIT_OFFSET: u64 = (1u64 << 32) - 1;
const MAX_STRING_SIZE: usize = 512 * 1024 * 1024;

#[command("SETBIT", runtime)]
pub fn setbit(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let db_idx = runtime.selected_db();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let key = &args.args[0];

    let offset = match std::str::from_utf8(&args.args[1])
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
    {
        Some(n) if n >= 0 && n as u64 <= MAX_BIT_OFFSET => n as u64,
        Some(_) => {
            return CommandOutcome::reply(Value::error(
                "ERR bit offset is not an integer or out of range",
            ));
        }
        None => {
            return CommandOutcome::reply(Value::error(
                "ERR bit offset is not an integer or out of range",
            ));
        }
    };

    let bit_value = match std::str::from_utf8(&args.args[2])
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
    {
        Some(0) => 0u8,
        Some(1) => 1u8,
        Some(_) | None => {
            return CommandOutcome::reply(Value::error(
                "ERR bit is not an integer or out of range",
            ));
        }
    };

    let byte_offset = (offset / 8) as usize;
    let bit_in_byte = 7 - (offset % 8) as u8;

    let (mut buf, key_new) = match db.get_string(key) {
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
        Ok(None) => (Vec::new(), true),
        Ok(Some(b)) => (b.to_vec(), false),
    };

    if byte_offset >= MAX_STRING_SIZE {
        return CommandOutcome::reply(Value::error(
            "ERR string exceeds maximum allowed size",
        ));
    }

    let extended = buf.len() <= byte_offset;
    if extended {
        buf.resize(byte_offset + 1, 0);
    }

    let old_bit = (buf[byte_offset] >> bit_in_byte) & 1;

    let dirty = key_new || extended || (old_bit != bit_value);
    if !dirty {
        return CommandOutcome {
            reply: Value::integer(old_bit as i64),
            effects: CommandEffects {
                skip_propagate: true,
                ..CommandEffects::default()
            },
        };
    }

    if bit_value == 1 {
        buf[byte_offset] |= 1 << bit_in_byte;
    } else {
        buf[byte_offset] &= !(1 << bit_in_byte);
    }

    db.set(key.to_vec(), Robj::string_raw(Bytes::from(buf)));
    changed_key_outcome(Value::integer(old_bit as i64), db_idx, key, "setbit")
}

#[command("GETBIT", runtime)]
pub fn getbit(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let key = &args.args[0];

    let offset = match std::str::from_utf8(&args.args[1])
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
    {
        Some(n) if n >= 0 && n as u64 <= MAX_BIT_OFFSET => n as u64,
        Some(_) => {
            return CommandOutcome::reply(Value::error(
                "ERR bit offset is not an integer or out of range",
            ));
        }
        None => {
            return CommandOutcome::reply(Value::error(
                "ERR bit offset is not an integer or out of range",
            ));
        }
    };

    let byte_offset = (offset / 8) as usize;
    let bit_in_byte = 7 - (offset % 8) as u8;

    CommandOutcome::reply(match db.get_string(key) {
        Err(_) => CommandError::wrong_type(),
        Ok(None) => Value::integer(0),
        Ok(Some(b)) => {
            let bytes = b;
            if byte_offset >= bytes.len() {
                return CommandOutcome::reply(Value::integer(0));
            }
            let bit = (bytes[byte_offset] >> bit_in_byte) & 1;
            Value::integer(bit as i64)
        }
    })
}

#[command("SUBSTR", runtime)]
#[command("GETRANGE", runtime)]
pub fn getrange(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let key = &args.args[0];

    let start = match std::str::from_utf8(&args.args[1])
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
    {
        Some(n) => n,
        None => {
            return CommandOutcome::reply(Value::error(
                "ERR value is not an integer or out of range",
            ))
        }
    };

    let end = match std::str::from_utf8(&args.args[2])
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
    {
        Some(n) => n,
        None => {
            return CommandOutcome::reply(Value::error(
                "ERR value is not an integer or out of range",
            ))
        }
    };

    CommandOutcome::reply(match db.get_string(key) {
        Err(_) => CommandError::wrong_type(),
        Ok(None) => Value::bulk(Bytes::new()),
        Ok(Some(b)) => {
            let bytes = b;
            let len = bytes.len() as i64;

            let mut s = if start < 0 { len + start } else { start };
            let mut e = if end < 0 { len + end } else { end };

            if s < 0 {
                s = 0;
            }
            if e >= len {
                e = len - 1;
            }

            if s > e || len == 0 {
                return CommandOutcome::reply(Value::bulk(Bytes::new()));
            }

            let s = s as usize;
            let e = e as usize;
            Value::bulk(Bytes::copy_from_slice(&bytes[s..=e]))
        }
    })
}

fn changed_key_outcome(
    reply: Value,
    db_idx: u32,
    key: &Bytes,
    event: &'static str,
) -> CommandOutcome {
    CommandOutcome {
        reply,
        effects: CommandEffects {
            changed_keys: vec![(db_idx, key.clone())],
            events: vec![CoreEvent {
                kind: event,
                key: key.clone(),
            }],
            ..CommandEffects::default()
        },
    }
}
