use crate::framework::{CommandArgs, CommandError};
use crate::framework::context::ServerContext;
use crate::foundation::io::store::structure::stream::StreamID;
pub(crate) use super::runtime_support::parse_stream_id;
use crate::foundation::io::protocol::Value;
use crate::engine::command::context::{BlockedCmd, CommandConn};
use command_macros::command;

pub(crate) fn entry_to_value(id: &StreamID, fields: &[(bytes::Bytes, bytes::Bytes)]) -> Value {
    let mut entry = Vec::with_capacity(2);
    entry.push(Value::bulk(bytes::Bytes::from(id.fmt_id())));
    let mut field_arr = Vec::with_capacity(fields.len() * 2);
    for (k, v) in fields {
        field_arr.push(Value::bulk(k.clone()));
        field_arr.push(Value::bulk(v.clone()));
    }
    entry.push(Value::Array(field_arr));
    Value::Array(entry)
}

pub(crate) fn parse_read_id(
    s: &[u8],
    stream: Option<&crate::foundation::io::store::structure::stream::ValkeyStream>,
) -> Result<ReadIdMode, Value> {
    if s == b"+" {
        return Ok(ReadIdMode::Last);
    }
    if s == b"$" {
        return match stream {
            Some(sm) => Ok(ReadIdMode::After(sm.recorded_last_id())),
            None => Ok(ReadIdMode::After(StreamID::zero())),
        };
    }
    if s == b"0" || s == b"0-0" {
        return Ok(ReadIdMode::After(StreamID::zero()));
    }
    let id = parse_stream_id(s, false)?;
    Ok(ReadIdMode::After(id))
}

pub(crate) enum ReadIdMode {
    Last,
    After(StreamID),
}

fn freeze_blocking_ids(
    args: &CommandArgs,
    first_id_index: usize,
    streams: &[Option<StreamID>],
) -> CommandArgs {
    let mut frozen = args.clone();
    for (idx, last_id) in streams.iter().enumerate() {
        let arg_idx = first_id_index + idx;
        if frozen.args[arg_idx].as_ref() == b"$" {
            let id = last_id.unwrap_or_else(StreamID::zero);
            frozen.args[arg_idx] = bytes::Bytes::from(id.fmt_id());
        }
    }
    frozen
}

#[command("XREAD", get_keys = crate::framework::commands::server::command::getkeys_helpers::xread_get_keys)]
pub fn xread(conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    let mut i = 0;
    let mut count: Option<usize> = None;
    let mut block_ms: Option<u64> = None;

    while i < args.args.len() {
        let arg_upper = args.args[i].to_ascii_uppercase();
        match arg_upper.as_slice() {
            b"COUNT" => {
                if i + 1 >= args.args.len() {
                    return CommandError::syntax_error();
                }
                i += 1;
                let c: i64 = match args.parse(i) {
                    Ok(c) => c,
                    Err(_) => return Value::error("ERR value is not an integer or out of range"),
                };
                if c <= 0 {
                    return Value::error("ERR value is not an integer or out of range");
                }
                count = Some(c as usize);
                i += 1;
            }
            b"BLOCK" => {
                if i + 1 >= args.args.len() {
                    return CommandError::syntax_error();
                }
                i += 1;
                let ms: i64 = match args.parse(i) {
                    Ok(m) => m,
                    Err(_) => return Value::error("ERR timeout is not a float or out of range"),
                };
                if ms < 0 {
                    return Value::error("ERR timeout is not a float or out of range");
                }
                block_ms = Some(ms as u64);
                i += 1;
            }
            b"STREAMS" => {
                i += 1;
                break;
            }
            _ => {
                return CommandError::syntax_error();
            }
        }
    }

    if i >= args.args.len() {
        return CommandError::syntax_error();
    }

    let remaining = args.args.len() - i;
    if remaining == 0 || !remaining.is_multiple_of(2) {
        return Value::error(
            "ERR Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified.",
        );
    }

    let num_keys = remaining / 2;
    let keys = &args.args[i..i + num_keys];
    let id_args = &args.args[i + num_keys..];

    let db = match ctx.db_mut(conn.selected_db()) {
        Some(d) => d,
        None => return CommandError::db_index_out_of_range(),
    };

    let mut streams_result = Vec::with_capacity(num_keys);
    let mut last_ids = Vec::with_capacity(num_keys);

    for idx in 0..num_keys {
        let key = &keys[idx];
        let stream_opt = match db.get_stream(key) {
            Ok(opt) => opt,
            Err(_) => return CommandError::wrong_type(),
        };
        last_ids.push(stream_opt.map(|stream| stream.recorded_last_id()));

        let mode = match parse_read_id(&id_args[idx], stream_opt) {
            Ok(m) => m,
            Err(e) => return e,
        };

        let entries = match (mode, stream_opt) {
            (ReadIdMode::Last, Some(stream)) => stream.last_entry().into_iter().collect(),
            (ReadIdMode::Last, None) => Vec::new(),
            (ReadIdMode::After(start_id), Some(stream)) => {
                stream.range_from_start(&start_id, count.unwrap_or(usize::MAX))
            }
            (ReadIdMode::After(_), None) => Vec::new(),
        };

        if entries.is_empty() {
            continue;
        }

        let mut entry_arr = Vec::with_capacity(entries.len());
        for e in &entries {
            entry_arr.push(entry_to_value(&e.id, &e.fields));
        }
        let key_entry = vec![Value::bulk(key.clone()), Value::Array(entry_arr)];
        streams_result.push(Value::Array(key_entry));
    }

    if streams_result.is_empty() {
        if let Some(timeout_ms) = block_ms {
            if !ctx.has_running_script() {
                let frozen_args = freeze_blocking_ids(args, i + num_keys, &last_ids);
                return conn.enter_blocking(
                    BlockedCmd::Xread,
                    keys.to_vec(),
                    timeout_ms,
                    &frozen_args,
                    conn.req_bytes_curr_cmd(),
                );
            }
        }
        return Value::null_array();
    }

    Value::Array(streams_result)
}
