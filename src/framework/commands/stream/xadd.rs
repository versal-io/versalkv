use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use crate::foundation::io::store::structure::stream::StreamID;
use bytes::Bytes;
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

fn is_arg(arg: &Bytes, target: &[u8]) -> bool {
    arg.as_ref() == target
}

fn eq_ignore_case(arg: &Bytes, target: &[u8]) -> bool {
    arg.eq_ignore_ascii_case(target)
}

enum TrimStrategy {
    MaxLen {
        count: u64,
        approx: bool,
        limit: Option<usize>,
    },
    MinId {
        minid: StreamID,
        approx: bool,
        limit: Option<usize>,
    },
}

fn parse_xadd_id(id_arg: &Bytes) -> Result<(bool, bool, StreamID), Value> {
    if is_arg(id_arg, b"*") {
        return Ok((false, false, StreamID::zero()));
    }
    let id_str = std::str::from_utf8(id_arg)
        .map_err(|_| Value::error("ERR Invalid stream ID specified as stream command argument"))?;
    if let Some(ms_end) = id_str.find('-') {
        let (ms_part, seq_part) = id_str.split_at(ms_end);
        let seq_part = &seq_part[1..];
        let ms: u64 = ms_part.parse().map_err(|_| {
            Value::error("ERR Invalid stream ID specified as stream command argument")
        })?;
        if seq_part == "*" {
            Ok((true, true, StreamID::new(ms, 0)))
        } else {
            let seq: u64 = seq_part.parse().map_err(|_| {
                Value::error("ERR Invalid stream ID specified as stream command argument")
            })?;
            Ok((true, false, StreamID::new(ms, seq)))
        }
    } else {
        let ms: u64 = id_str.parse().map_err(|_| {
            Value::error("ERR Invalid stream ID specified as stream command argument")
        })?;
        Ok((true, false, StreamID::new(ms, 0)))
    }
}

#[command("XADD", runtime)]
pub fn xadd(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let mut i = 0;
    let key = args.args[0].clone();
    i += 1;

    let mut nomkstream = false;
    let mut trim_strategy: Option<TrimStrategy> = None;

    while i < args.args.len() {
        let arg = &args.args[i];
        if eq_ignore_case(arg, b"NOMKSTREAM") {
            nomkstream = true;
            i += 1;
        } else if eq_ignore_case(arg, b"MAXLEN") || eq_ignore_case(arg, b"MINID") {
            let is_maxlen = eq_ignore_case(arg, b"MAXLEN");
            i += 1;
            let mut approx = false;
            if i < args.args.len() && (is_arg(&args.args[i], b"=") || is_arg(&args.args[i], b"~")) {
                approx = is_arg(&args.args[i], b"~");
                i += 1;
            }
            if i >= args.args.len() {
                return CommandOutcome::reply(CommandError::syntax_error());
            }
            if is_maxlen {
                let count: i64 = match args.parse(i) {
                    Ok(c) => c,
                    Err(_) => {
                        return CommandOutcome::reply(Value::error(
                            "ERR value is not an integer or out of range",
                        ))
                    }
                };
                trim_strategy = Some(TrimStrategy::MaxLen {
                    count: count.max(0) as u64,
                    approx,
                    limit: None,
                });
            } else {
                let id = match parse_stream_id(&args.args[i], false) {
                    Ok(id) => id,
                    Err(e) => return CommandOutcome::reply(e),
                };
                trim_strategy = Some(TrimStrategy::MinId {
                    minid: id,
                    approx,
                    limit: None,
                });
            }
            i += 1;
            if i < args.args.len() && eq_ignore_case(&args.args[i], b"LIMIT") {
                if !approx {
                    return CommandOutcome::reply(CommandError::syntax_error());
                }
                i += 1;
                if i >= args.args.len() {
                    return CommandOutcome::reply(CommandError::syntax_error());
                }
                let limit: i64 = match args.parse(i) {
                    Ok(v) => v,
                    Err(_) => {
                        return CommandOutcome::reply(Value::error(
                            "ERR value is not an integer or out of range",
                        ))
                    }
                };
                if limit < 0 {
                    return CommandOutcome::reply(Value::error(
                        "ERR value is not an integer or out of range",
                    ));
                }
                match &mut trim_strategy {
                    Some(TrimStrategy::MaxLen { limit: l, .. })
                    | Some(TrimStrategy::MinId { limit: l, .. }) => {
                        *l = Some(limit as usize);
                    }
                    None => {}
                }
                i += 1;
            }
        } else {
            break;
        }
    }

    if i >= args.args.len() {
        return CommandOutcome::reply(CommandError::wrong_arity("xadd"));
    }

    let id_arg = args.args[i].clone();
    i += 1;

    let remaining = args.args.len() - i;
    if remaining == 0 || !remaining.is_multiple_of(2) {
        return CommandOutcome::reply(CommandError::wrong_arity("xadd"));
    }

    let mut fields = Vec::with_capacity(remaining / 2);
    let mut j = i;
    while j + 1 < args.args.len() {
        fields.push((args.args[j].clone(), args.args[j + 1].clone()));
        j += 2;
    }
    let fields_for_propagation = fields.clone();

    let (use_id, seq_auto, requested_id) = match parse_xadd_id(&id_arg) {
        Ok(t) => t,
        Err(e) => return CommandOutcome::reply(e),
    };

    let (node_max_entries, node_max_bytes) = runtime.stream_node_limits();
    let now = runtime.now_ms();
    let db_idx = runtime.selected_db();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };

    let exists = db.exists(&key);
    if nomkstream && !exists {
        return CommandOutcome {
            reply: Value::null(),
            effects: CommandEffects {
                skip_propagate: true,
                ..CommandEffects::default()
            },
        };
    }

    if use_id && !seq_auto && !exists && requested_id.ms == 0 && requested_id.seq == 0 {
        return CommandOutcome::reply(Value::error(
            "ERR The ID specified in XADD must be greater than 0-0",
        ));
    }

    let stream = match db.get_or_create_stream(&key) {
        Ok(s) => s,
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
    };
    let had_trim_strategy = trim_strategy.is_some();

    let new_id = match stream.append(requested_id, fields, use_id, seq_auto, now) {
        Ok(id) => id,
        Err(e) => {
            let should_cleanup = !exists && stream.entries_added == 0;
            if should_cleanup {
                db.del(&key);
            }
            return CommandOutcome::reply(Value::error(e));
        }
    };

    let trimmed = match trim_strategy {
        Some(TrimStrategy::MaxLen {
            count,
            approx,
            limit,
        }) => {
            stream.trim_by_length_limit_with_node_limits(
                count as usize,
                approx,
                limit,
                node_max_entries,
                node_max_bytes,
            )
        }
        Some(TrimStrategy::MinId {
            minid,
            approx,
            limit,
        }) => {
            stream.trim_by_id_limit_with_node_limits(
                &minid,
                approx,
                limit,
                node_max_entries,
                node_max_bytes,
            )
        }
        None => 0,
    };
    let mut custom_propagations = Vec::new();
    if had_trim_strategy || !use_id || seq_auto {
        let new_id_str = new_id.fmt_id();
        let mut xadd_parts = Vec::with_capacity(args.args.len() + 1);
        xadd_parts.push(Bytes::from_static(b"XADD"));
        xadd_parts.push(key.clone());
        xadd_parts.push(Bytes::from(new_id_str));
        for (field, value) in &fields_for_propagation {
            xadd_parts.push(field.clone());
            xadd_parts.push(value.clone());
        }
        custom_propagations.push(xadd_parts);
        if trimmed > 0 {
            let current_len = stream.len().to_string();
            custom_propagations.push(vec![
                Bytes::from_static(b"XTRIM"),
                key.clone(),
                Bytes::from_static(b"MAXLEN"),
                Bytes::from_static(b"="),
                current_len.into(),
            ]);
        }
    }
    CommandOutcome {
        reply: Value::bulk(Bytes::from(new_id.fmt_id())),
        effects: CommandEffects {
            changed_keys: vec![(db_idx, key.clone())],
            events: vec![CoreEvent {
                kind: "xadd",
                key,
            }],
            custom_propagations,
            ..CommandEffects::default()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::xadd;
    use crate::foundation::io::store::structure::stream::StreamID;
    use crate::framework::commands::command_runtime::{CommandRuntime, TestCommandRuntime};
    use crate::framework::CommandArgs;
    use bytes::Bytes;

    #[test]
    fn xadd_uses_stream_node_max_bytes_for_approximate_trim() {
        let mut runtime = TestCommandRuntime::new(1, 10);
        runtime.stream_node_limits = (100, 1);
        let stream = runtime.db_mut().unwrap().get_or_create_stream(b"stream").unwrap();
        for ms in 1..=3 {
            stream
                .append(
                    StreamID::new(ms, 0),
                    vec![(Bytes::from_static(b"field"), Bytes::from_static(b"value"))],
                    true,
                    false,
                    0,
                )
                .unwrap();
        }
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"XADD"),
            Bytes::from_static(b"stream"),
            Bytes::from_static(b"MAXLEN"),
            Bytes::from_static(b"~"),
            Bytes::from_static(b"1"),
            Bytes::from_static(b"*"),
            Bytes::from_static(b"field"),
            Bytes::from_static(b"value"),
        ]);

        assert!(matches!(
            xadd(&mut runtime, &args).reply,
            crate::foundation::io::protocol::Value::Bulk(_)
        ));
        let Some(crate::foundation::io::store::Robj::StreamListpack(stream)) =
            runtime.dbs.get(0).unwrap().get_no_expire(b"stream")
        else {
            panic!("stream value should remain after XADD");
        };
        assert_eq!(stream.len(), 1);
    }
}
