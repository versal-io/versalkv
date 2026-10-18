use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::foundation::io::store::Robj;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[command("BITOP", runtime)]
pub fn bitop(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.args.len() < 3 {
        return CommandOutcome::reply(CommandError::wrong_arity("bitop"));
    }

    let operation = &args.args[0];
    let destkey = &args.args[1];
    let src_keys = &args.args[2..];

    let is_not = operation.eq_ignore_ascii_case(b"NOT");
    let is_and = operation.eq_ignore_ascii_case(b"AND");
    let is_or = operation.eq_ignore_ascii_case(b"OR");
    let is_xor = operation.eq_ignore_ascii_case(b"XOR");

    if !is_not && !is_and && !is_or && !is_xor {
        return CommandOutcome::reply(Value::error("ERR syntax error"));
    }

    if is_not && src_keys.len() != 1 {
        return CommandOutcome::reply(Value::error(
            "ERR BITOP NOT must be called with a single source key",
        ));
    }

    let db_idx = runtime.selected_db();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };

    let mut src_strings: Vec<Vec<u8>> = Vec::with_capacity(src_keys.len());
    let mut max_len: usize = 0;

    for key in src_keys {
        match db.get_string(key) {
            Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
            Ok(None) => {
                src_strings.push(Vec::new());
            }
            Ok(Some(b)) => {
                let data = b.to_vec();
                let len = data.len();
                if len > max_len {
                    max_len = len;
                }
                src_strings.push(data);
            }
        }
    }

    if src_keys.is_empty() || max_len == 0 {
        if is_not {
            db.set(destkey.to_vec(), Robj::string_raw(Bytes::from(Vec::new())));
        } else {
            let need_set = true;
            if need_set {
                db.set(destkey.to_vec(), Robj::string_raw(Bytes::from(Vec::new())));
            }
        }
        return bitop_outcome(Value::integer(0), db_idx, destkey, args);
    }

    let mut result = vec![0u8; max_len];

    if is_not {
        let data = &src_strings[0];
        for i in 0..max_len {
            if i < data.len() {
                result[i] = !data[i];
            } else {
                result[i] = 0xFF;
            }
        }
    } else if is_and {
        for i in 0..max_len {
            let mut byte_val = 0xFFu8;
            for data in &src_strings {
                if i < data.len() {
                    byte_val &= data[i];
                } else {
                    byte_val = 0;
                    break;
                }
            }
            result[i] = byte_val;
        }
    } else if is_or {
        for i in 0..max_len {
            let mut byte_val = 0u8;
            for data in &src_strings {
                if i < data.len() {
                    byte_val |= data[i];
                }
            }
            result[i] = byte_val;
        }
    } else if is_xor {
        for i in 0..max_len {
            let mut byte_val = 0u8;
            for data in &src_strings {
                if i < data.len() {
                    byte_val ^= data[i];
                }
            }
            result[i] = byte_val;
        }
    }

    let result_len = result.len() as i64;

    db.set(destkey.to_vec(), Robj::string_raw(Bytes::from(result)));

    bitop_outcome(Value::integer(result_len), db_idx, destkey, args)
}

fn bitop_outcome(
    reply: Value,
    db_idx: u32,
    destination: &Bytes,
    args: &CommandArgs,
) -> CommandOutcome {
    let mut propagation = Vec::with_capacity(args.args.len() + 1);
    propagation.push(Bytes::from_static(b"BITOP"));
    propagation.extend(args.args.iter().cloned());
    CommandOutcome {
        reply,
        effects: CommandEffects {
            changed_keys: vec![(db_idx, destination.clone())],
            events: vec![CoreEvent {
                kind: "set",
                key: destination.clone(),
            }],
            custom_propagations: vec![propagation],
            ..CommandEffects::default()
        },
    }
}
