use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::stream::runtime_support::parse_stream_id;
use crate::framework::{CommandArgs, CommandError};
use crate::foundation::io::store::structure::stream::groups::PendingRange;
use crate::foundation::io::store::structure::stream::StreamID;
use bytes::Bytes;
use command_macros::command;

#[command("XPENDING", runtime)]
pub fn xpending(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
) -> CommandOutcome {
    CommandOutcome::reply(xpending_reply(runtime, args))
}

fn xpending_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    if args.args.len() != 2 && args.args.len() < 5 {
        return CommandError::wrong_arity("xpending");
    }
    let now_ms = runtime.now_ms() as i64;
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandError::db_index_out_of_range(),
    };
    let stream = match db.get_stream(&args.args[0]) {
        Ok(Some(stream)) => stream,
        Ok(None) => return Value::error("NOGROUP No such key or consumer group"),
        Err(_) => return CommandError::wrong_type(),
    };

    if args.args.len() == 2 {
        let summary = match stream.pending_summary(&args.args[1]) {
            Ok(summary) => summary,
            Err(_) => return Value::error("NOGROUP No such key or consumer group"),
        };
        let mut consumers = Vec::new();
        for (name, count) in summary.consumers {
            consumers.push(Value::Array(vec![
                Value::bulk(name),
                Value::integer(count as i64),
            ]));
        }
        return Value::Array(vec![
            Value::integer(summary.total as i64),
            summary
                .first_id
                .map(|id| Value::bulk(Bytes::from(id.fmt_id())))
                .unwrap_or_else(Value::null),
            summary
                .last_id
                .map(|id| Value::bulk(Bytes::from(id.fmt_id())))
                .unwrap_or_else(Value::null),
            Value::Array(consumers),
        ]);
    }

    let mut i = 2;
    let mut min_idle_ms: Option<i64> = None;
    if args.args[i].eq_ignore_ascii_case(b"IDLE") {
        if i + 1 >= args.args.len() {
            return CommandError::syntax_error();
        }
        let parsed: i64 = match args.parse(i + 1) {
            Ok(v) => v,
            Err(_) => return Value::error("ERR value is not an integer or out of range"),
        };
        if parsed < 0 {
            return Value::error("ERR value is not an integer or out of range");
        }
        min_idle_ms = Some(parsed);
        i += 2;
    }
    if i + 2 >= args.args.len() {
        return CommandError::syntax_error();
    }
    let (start, start_exclusive) = match parse_pending_bound(&args.args[i], false) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let (end, end_exclusive) = match parse_pending_bound(&args.args[i + 1], true) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let count: i64 = match args.parse(i + 2) {
        Ok(v) => v,
        Err(_) => return Value::error("ERR value is not an integer or out of range"),
    };
    if count < 0 {
        return Value::error("ERR value is not an integer or out of range");
    }
    let consumer = if i + 3 < args.args.len() {
        if i + 4 != args.args.len() {
            return CommandError::syntax_error();
        }
        Some(args.args[i + 3].as_ref())
    } else {
        None
    };
    let pending = match stream.pending_range(
        &args.args[1],
        PendingRange {
            start,
            end,
            start_exclusive,
            end_exclusive,
        },
        count as usize,
        consumer,
        min_idle_ms,
        now_ms,
    ) {
        Ok(pending) => pending,
        Err(_) => return Value::error("NOGROUP No such key or consumer group"),
    };
    Value::Array(
        pending
            .into_iter()
            .map(|item| {
                Value::Array(vec![
                    Value::bulk(Bytes::from(item.id.fmt_id())),
                    Value::bulk(item.consumer),
                    Value::integer(item.idle_ms),
                    Value::integer(item.delivery_count as i64),
                ])
            })
            .collect(),
    )
}

fn parse_pending_bound(raw: &[u8], is_end: bool) -> Result<(StreamID, bool), Value> {
    let exclusive = raw.starts_with(b"(");
    let raw = if exclusive { &raw[1..] } else { raw };
    let id = if raw == b"-" {
        StreamID::zero()
    } else if raw == b"+" {
        StreamID::max()
    } else {
        parse_stream_id(raw, is_end)?
    };
    Ok((id, exclusive))
}
