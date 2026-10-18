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
    MaxLen(u64, bool),
    MinId(StreamID, bool),
}

#[command("XTRIM", runtime)]
pub fn xtrim(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.args.len() < 2 {
        return CommandOutcome::reply(CommandError::wrong_arity("xtrim"));
    }
    let (node_max_entries, node_max_bytes) = runtime.stream_node_limits();
    let db_idx = runtime.selected_db();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let key = args.args[0].clone();
    let strategy_arg = args.args[1].clone();
    let strategy_upper = strategy_arg.to_ascii_uppercase();

    let mut i = 2;
    let mut approx = false;
    let mut limit: Option<usize> = None;

    if i < args.args.len() && (is_arg(&args.args[i], b"=") || is_arg(&args.args[i], b"~")) {
        approx = is_arg(&args.args[i], b"~");
        i += 1;
    }

    if i >= args.args.len() {
        return CommandOutcome::reply(CommandError::syntax_error());
    }

    let is_maxlen = strategy_upper.as_slice() == b"MAXLEN";

    let strategy = if is_maxlen {
        let limit: i64 = match args.parse(i) {
            Ok(v) => v,
            Err(_) => {
                return CommandOutcome::reply(Value::error(
                    "ERR value is not an integer or out of range",
                ))
            }
        };
        i += 1;
        TrimStrategy::MaxLen(limit.max(0) as u64, approx)
    } else if strategy_upper.as_slice() == b"MINID" {
        let minid = match parse_stream_id(&args.args[i], false) {
            Ok(id) => id,
            Err(e) => return CommandOutcome::reply(e),
        };
        i += 1;
        TrimStrategy::MinId(minid, approx)
    } else {
        return CommandOutcome::reply(CommandError::syntax_error());
    };

    while i < args.args.len() {
        if eq_ignore_case(&args.args[i], b"LIMIT") {
            i += 1;
            if !approx || i >= args.args.len() {
                return CommandOutcome::reply(CommandError::syntax_error());
            }
            let parsed_limit: i64 = match args.parse(i) {
                Ok(v) => v,
                Err(_) => {
                    return CommandOutcome::reply(Value::error(
                        "ERR value is not an integer or out of range",
                    ))
                }
            };
            if parsed_limit < 0 {
                return CommandOutcome::reply(Value::error(
                    "ERR value is not an integer or out of range",
                ));
            }
            limit = Some(parsed_limit as usize);
            i += 1;
        } else {
            return CommandOutcome::reply(CommandError::syntax_error());
        }
    }

    let stream = match db.get_stream_mut(&key) {
        Ok(Some(s)) => s,
        Ok(None) => {
            let _ = db.get_or_create_stream(&key);
            return CommandOutcome::reply(Value::integer(0));
        }
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
    };

    let removed = match strategy {
        TrimStrategy::MaxLen(count, approx) => {
            stream.trim_by_length_limit_with_node_limits(
                count as usize,
                approx,
                limit,
                node_max_entries,
                node_max_bytes,
            )
        }
        TrimStrategy::MinId(minid, approx) => {
            stream.trim_by_id_limit_with_node_limits(
                &minid,
                approx,
                limit,
                node_max_entries,
                node_max_bytes,
            )
        }
    };

    if removed == 0 {
        return CommandOutcome::reply(Value::integer(0));
    }
    let current_len = stream.len().to_string();
    CommandOutcome {
        reply: Value::integer(removed as i64),
        effects: CommandEffects {
            changed_keys: vec![(db_idx, key.clone())],
            events: vec![CoreEvent {
                kind: "xtrim",
                key: key.clone(),
            }],
            custom_propagations: vec![vec![
                Bytes::from_static(b"XTRIM"),
                key,
                Bytes::from_static(b"MAXLEN"),
                Bytes::from_static(b"="),
                current_len.into(),
            ]],
            ..CommandEffects::default()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::xtrim;
    use crate::foundation::io::protocol::Value;
    use crate::foundation::io::store::structure::stream::StreamID;
    use crate::framework::commands::command_runtime::{CommandRuntime, TestCommandRuntime};
    use crate::framework::CommandArgs;
    use bytes::Bytes;

    #[test]
    fn xtrim_uses_stream_node_max_bytes_for_approximate_boundaries() {
        let mut runtime = TestCommandRuntime::new(1, 0);
        runtime.stream_node_limits = (100, 1);
        let stream = runtime.db_mut().unwrap().get_or_create_stream(b"stream").unwrap();
        for ms in 1..=4 {
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
            Bytes::from_static(b"XTRIM"),
            Bytes::from_static(b"stream"),
            Bytes::from_static(b"MAXLEN"),
            Bytes::from_static(b"~"),
            Bytes::from_static(b"1"),
        ]);

        assert_eq!(
            xtrim(&mut runtime, &args).reply,
            Value::integer(3)
        );
    }

    #[test]
    fn xtrim_maxlen_matches_exact_and_approximate_tcl_sequence() {
        let mut runtime = TestCommandRuntime::new(1, 0);
        let stream = runtime.db_mut().unwrap().get_or_create_stream(b"mystream").unwrap();
        for ms in 1..=1000 {
            stream
                .append(
                    StreamID::new(ms, 0),
                    vec![(Bytes::from_static(b"xitem"), Bytes::from_static(b"v"))],
                    true,
                    false,
                    0,
                )
                .unwrap();
        }
        for (marker, count, expected) in [(None, b"666".as_slice(), 666), (Some(b"=".as_slice()), b"555".as_slice(), 555), (Some(b"~".as_slice()), b"444".as_slice(), 500)] {
            let mut parts = vec![Bytes::from_static(b"XTRIM"), Bytes::from_static(b"mystream"), Bytes::from_static(b"MAXLEN")];
            if let Some(marker) = marker { parts.push(Bytes::copy_from_slice(marker)); }
            parts.push(Bytes::copy_from_slice(count));
            let args = CommandArgs::from_parts(parts);
            assert!(matches!(
                xtrim(&mut runtime, &args).reply,
                Value::Integer(_)
            ));
            assert_eq!(runtime.db_mut().unwrap().get_stream(b"mystream").unwrap().unwrap().len(), expected);
        }
    }
}
