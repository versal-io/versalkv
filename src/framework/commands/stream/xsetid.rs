use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use crate::foundation::io::store::structure::stream::StreamID;
use command_macros::command;

fn parse_stream_id_raw(s: &[u8], is_end: bool) -> Result<StreamID, Value> {
    if s == b"-" {
        return Ok(StreamID::zero());
    }
    if s == b"+" {
        return Ok(StreamID::max());
    }
    let s_str = std::str::from_utf8(s)
        .map_err(|_| Value::error("ERR Invalid stream ID specified as stream command argument"))?;
    if s_str.contains('-') {
        StreamID::parse(s).ok_or_else(|| {
            Value::error("ERR Invalid stream ID specified as stream command argument")
        })
    } else {
        let ms: u64 = s_str.parse().map_err(|_| {
            Value::error("ERR Invalid stream ID specified as stream command argument")
        })?;
        if is_end {
            Ok(StreamID::new(ms, u64::MAX))
        } else {
            Ok(StreamID::new(ms, 0))
        }
    }
}

fn parse_stream_id_ext(s: &[u8], is_end: bool) -> Result<(StreamID, bool), Value> {
    let mut exclusive = false;
    let id_str = if s.starts_with(b"(") {
        exclusive = true;
        &s[1..]
    } else {
        s
    };
    if id_str == b"-" {
        if exclusive {
            return Err(Value::error("ERR The - ID can't be exclusive"));
        }
        return Ok((StreamID::zero(), false));
    }
    if id_str == b"+" {
        if exclusive {
            return Err(Value::error("ERR The + ID can't be exclusive"));
        }
        return Ok((StreamID::max(), false));
    }
    let id = parse_stream_id_raw(id_str, is_end)?;
    if exclusive && id == StreamID::zero() && is_end {
        return Err(Value::error(
            "ERR The 0-0 ID can't be exclusive as end interval",
        ));
    }
    if exclusive && id == StreamID::max() && !is_end {
        return Err(Value::error(
            "ERR The greatest ID can't be exclusive as start interval",
        ));
    }
    Ok((id, exclusive))
}

fn parse_stream_id(s: &[u8], is_end: bool) -> Result<StreamID, Value> {
    parse_stream_id_ext(s, is_end).map(|(id, _)| id)
}

fn parse_stream_id_arg(s: &[u8]) -> Result<StreamID, Value> {
    parse_stream_id(s, false)
}

#[command("XSETID", runtime)]
pub fn xsetid(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let db_idx = runtime.selected_db();
    let key = args.args.first().cloned().unwrap_or_default();
    let reply = xsetid_reply(runtime, args);
    if matches!(reply, Value::Error(_)) {
        return CommandOutcome::reply(reply);
    }
    CommandOutcome {
        reply,
        effects: CommandEffects {
            changed_keys: vec![(db_idx, key.clone())],
            events: vec![CoreEvent {
                kind: "xsetid",
                key,
            }],
            ..CommandEffects::default()
        },
    }
}

fn xsetid_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("xsetid");
    }
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandError::db_index_out_of_range(),
    };
    let key = args.args[0].clone();
    let id = match parse_stream_id_arg(&args.args[1]) {
        Ok(id) => id,
        Err(e) => return e,
    };
    let mut entries_added: Option<u64> = None;
    let mut max_deleted_id: Option<StreamID> = None;
    if args.args.len() != 2 && args.args.len() != 6 {
        return CommandError::syntax_error();
    }
    if args.args.len() == 6 {
        let mut i = 2;
        while i < args.args.len() {
            if args.args[i].eq_ignore_ascii_case(b"ENTRIESADDED") {
                let parsed: i64 = match args.parse(i + 1) {
                    Ok(v) => v,
                    Err(_) => return Value::error("ERR entries_added must be positive"),
                };
                if parsed < 0 {
                    return Value::error("ERR entries_added must be positive");
                }
                entries_added = Some(parsed as u64);
            } else if args.args[i].eq_ignore_ascii_case(b"MAXDELETEDID") {
                max_deleted_id = match parse_stream_id_arg(&args.args[i + 1]) {
                    Ok(id) => Some(id),
                    Err(e) => return e,
                };
            } else {
                return CommandError::syntax_error();
            }
            i += 2;
        }
        if entries_added.is_none() || max_deleted_id.is_none() {
            return CommandError::syntax_error();
        }
    }

    let stream = match db.get_stream_mut(&key) {
        Ok(Some(s)) => s,
        Ok(None) => {
            return Value::error("ERR no such key")
        }
        Err(_) => return CommandError::wrong_type(),
    };

    if !stream.is_empty()
        && (id.ms < stream.last_id.ms
            || (id.ms == stream.last_id.ms && id.seq < stream.last_id.seq))
    {
        return Value::error(
            "ERR The ID specified in XSETID is smaller than the target stream top item",
        );
    }

    if !stream.is_empty()
        && (id.ms < stream.max_deleted_entry_id.ms
            || (id.ms == stream.max_deleted_entry_id.ms
                && id.seq < stream.max_deleted_entry_id.seq))
    {
        return Value::error(
            "ERR The ID specified in XSETID is smaller than the target stream max deleted entry ID",
        );
    }
    if let Some(max_deleted_id) = max_deleted_id {
        if max_deleted_id.ms > id.ms || (max_deleted_id.ms == id.ms && max_deleted_id.seq > id.seq)
        {
            return Value::error(
                "ERR The ID specified in XSETID is smaller than the provided max deleted ID",
            );
        }
        stream.max_deleted_entry_id = max_deleted_id;
    } else if stream.is_empty()
        && (stream.max_deleted_entry_id.ms > id.ms
            || (stream.max_deleted_entry_id.ms == id.ms
                && stream.max_deleted_entry_id.seq > id.seq))
    {
        stream.max_deleted_entry_id = id;
    }
    if let Some(entries_added) = entries_added {
        if entries_added < stream.len() as u64 {
            return Value::error(
                "ERR The entries_added specified in XSETID is smaller than the target stream length",
            );
        }
        stream.entries_added = entries_added;
    }

    stream.last_id = id;
    Value::ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use crate::framework::commands::command_runtime::TestCommandRuntime;

    #[test]
    fn set_id_records_effects_on_success_but_not_rejected_ids() {
        let mut runtime = TestCommandRuntime::new(1, 0);
        let stream = runtime.db_mut().unwrap().get_or_create_stream(b"s").unwrap();
        stream.append(StreamID::new(1, 0), vec![], true, false, 0).unwrap();
        for (id, should_dirty) in [(b"2-0".as_slice(), true), (b"0-0", false)] {
            let args = CommandArgs::from_parts([b"XSETID".as_slice(), b"s", id]
                .into_iter().map(Bytes::copy_from_slice).collect());
            let outcome = xsetid(&mut runtime, &args);
            assert_eq!(outcome.effects.changed_keys.is_empty(), !should_dirty);
            assert_eq!(matches!(outcome.reply, Value::Error(_)), !should_dirty);
        }
    }
}
