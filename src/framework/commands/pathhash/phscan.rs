use std::ops::Bound::{Excluded, Included, Unbounded};

use bytes::Bytes;
use command_macros::command;

use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;

use super::runtime_support::{is_option, parse_long_range, read_path_hash, reply_payload};

fn decode_hex_digit(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn decode_cursor(cursor: &[u8]) -> Result<Option<Bytes>, Value> {
    if cursor == b"0" {
        return Ok(None);
    }
    let invalid = || Value::error("ERR invalid cursor");
    let Some(hex) = cursor.strip_prefix(b"1:") else {
        return Err(invalid());
    };
    if !hex.len().is_multiple_of(2) {
        return Err(invalid());
    }
    let mut decoded = Vec::new();
    for pair in hex.as_chunks::<2>().0 {
        let high = decode_hex_digit(pair[0]).ok_or_else(invalid)?;
        let low = decode_hex_digit(pair[1]).ok_or_else(invalid)?;
        decoded.push((high << 4) | low);
    }
    Ok(Some(Bytes::from(decoded)))
}

fn encode_cursor(path: &[u8]) -> Bytes {
    let hex = b"0123456789abcdef";
    let mut cursor = Vec::from(b"1:");
    for byte in path {
        cursor.push(hex[usize::from(byte >> 4)]);
        cursor.push(hex[usize::from(byte & 0xf)]);
    }
    Bytes::from(cursor)
}

fn reply_page(cursor: Bytes, entries: Vec<Value>) -> Value {
    Value::Array(vec![Value::bulk(cursor), Value::Array(entries)])
}

#[command("PHSCAN", runtime)]
pub fn phscan(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.args.len() < 2 {
        return CommandOutcome::reply(CommandError::wrong_arity("phscan"));
    }
    let mut prefix: Option<&[u8]> = None;
    let mut count = 10;
    let mut has_count = false;
    let mut has_values = false;
    let mut index = 2;
    while index < args.args.len() {
        let option = &args.args[index];
        if is_option(option, b"PREFIX") && prefix.is_none() && index + 1 < args.args.len()
        {
            prefix = Some(&args.args[index + 1]);
            index += 2;
        } else if is_option(option, b"COUNT") && !has_count && index + 1 < args.args.len()
        {
            count = match parse_long_range(&args.args[index + 1], 1) {
                Ok(count) => count,
                Err(error) => return CommandOutcome::reply(error),
            };
            has_count = true;
            index += 2;
        } else if is_option(option, b"WITHVALUES") && !has_values {
            has_values = true;
            index += 1;
        } else {
            return CommandOutcome::reply(CommandError::syntax_error());
        }
    }
    let previous = match decode_cursor(&args.args[1]) {
        Ok(previous) => previous,
        Err(error) => return CommandOutcome::reply(error),
    };
    if let (Some(previous), Some(prefix)) = (&previous, prefix) {
        if !previous.starts_with(prefix) {
            return CommandOutcome::reply(Value::error(
                "ERR cursor does not belong to the requested prefix",
            ));
        }
    }
    let tree = match read_path_hash(runtime, &args.args[0]) {
        Ok(Some(tree)) => tree,
        Ok(None) => {
            return CommandOutcome::reply(reply_page(Bytes::from_static(b"0"), Vec::new()));
        }
        Err(error) => return CommandOutcome::reply(error),
    };
    let lower = match &previous {
        Some(previous) => Excluded(previous.as_ref()),
        None => prefix.map_or(Unbounded, Included),
    };
    let mut paths = tree.paths.range::<[u8], _>((lower, Unbounded));
    let mut entries = Vec::new();
    let mut last_path = None;
    while entries.len() < count {
        let Some((path, payload)) = paths.next() else {
            break;
        };
        if prefix.is_some_and(|prefix| !path.starts_with(prefix)) {
            break;
        }
        let reply = Value::bulk(path.clone());
        entries.push(if has_values {
            Value::Array(vec![reply, reply_payload(Some(payload))])
        } else {
            reply
        });
        last_path = Some(path);
    }
    // An exact COUNT page is final unless one more matching entry exists.
    let has_more = entries.len() == count
        && paths
            .next()
            .is_some_and(|(path, _)| prefix.is_none_or(|prefix| path.starts_with(prefix)));
    let cursor = if has_more {
        last_path.map_or_else(|| Bytes::from_static(b"0"), |path| encode_cursor(path))
    } else {
        Bytes::from_static(b"0")
    };
    CommandOutcome::reply(reply_page(cursor, entries))
}
