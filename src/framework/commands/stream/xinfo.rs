use bytes::Bytes;

use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

fn bs(s: &str) -> Value {
    Value::bulk(Bytes::from(s.to_string()))
}

fn entry_to_full_value(id: &crate::foundation::io::store::structure::stream::StreamID, fields: &[(Bytes, Bytes)]) -> Value {
    let mut arr = Vec::with_capacity(2);
    arr.push(Value::bulk(Bytes::from(id.fmt_id())));
    let mut fv = Vec::with_capacity(fields.len() * 2);
    for (k, v) in fields {
        fv.push(Value::bulk(k.clone()));
        fv.push(Value::bulk(v.clone()));
    }
    arr.push(Value::Array(fv));
    Value::Array(arr)
}

#[command("XINFO", runtime)]
pub fn xinfo(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    CommandOutcome::reply(xinfo_reply(runtime, args))
}

fn xinfo_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    if args.args.is_empty() {
        return CommandError::wrong_arity("xinfo");
    }

    let subcmd = args.args[0].to_ascii_uppercase();

    match subcmd.as_slice() {
        b"STREAM" => xinfo_stream(runtime, args),
        b"GROUPS" => {
            if args.args.len() != 2 {
                return CommandError::wrong_arity("xinfo");
            }
            let db = match runtime.db_mut() {
                Some(d) => d,
                None => return CommandError::db_index_out_of_range(),
            };
            let key = &args.args[1];
            match db.get_stream(key) {
                Ok(Some(stream)) => xinfo_groups_reply(stream),
                Ok(None) => Value::error("ERR no such key"),
                Err(_) => CommandError::wrong_type(),
            }
        }
        b"CONSUMERS" => {
            if args.args.len() != 3 {
                return CommandError::wrong_arity("xinfo");
            }
            let now_ms = runtime.now_ms() as i64;
            let db = match runtime.db_mut() {
                Some(d) => d,
                None => return CommandError::db_index_out_of_range(),
            };
            match db.get_stream(&args.args[1]) {
                Ok(Some(stream)) => xinfo_consumers_reply(stream, &args.args[2], now_ms),
                Ok(None) => Value::error("ERR no such key"),
                Err(_) => CommandError::wrong_type(),
            }
        }
        b"HELP" => {
            if args.args.len() != 1 {
                return CommandError::wrong_arity("xinfo|help");
            }
            bs("XINFO <subcommand> [<arg> [value] [opt] ...]. Subcommands are: CONSUMERS <key> <group>, GROUPS <key>, STREAM <key> [FULL [COUNT <count>]], HELP.")
        }
        _ => CommandError::syntax_error(),
    }
}

fn xinfo_stream(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("xinfo");
    }

    let key = &args.args[1];
    let mut full = false;
    let mut full_count: Option<usize> = None;

    let mut i = 2;
    while i < args.args.len() {
        let arg = args.args[i].to_ascii_uppercase();
        match arg.as_slice() {
            b"FULL" => {
                full = true;
                i += 1;
            }
            b"COUNT" => {
                i += 1;
                if i >= args.args.len() {
                    return CommandError::syntax_error();
                }
                let c: i64 = match args.parse(i) {
                    Ok(c) => c,
                    Err(_) => return Value::error("ERR value is not an integer or out of range"),
                };
                if c < 0 {
                    return Value::error("ERR value is not an integer or out of range");
                }
                full_count = Some(c as usize);
                i += 1;
            }
            _ => return CommandError::syntax_error(),
        }
    }

    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandError::db_index_out_of_range(),
    };

    let stream = match db.get_stream(key) {
        Ok(Some(s)) => s,
        Ok(None) => return Value::error("ERR no such key"),
        Err(_) => return CommandError::wrong_type(),
    };

    if full {
        xinfo_stream_full_reply(stream, full_count)
    } else {
        xinfo_stream_basic_reply(stream)
    }
}

fn entry_to_simple_value(e: &crate::foundation::io::store::structure::stream::StreamEntry) -> Value {
    let mut arr = Vec::with_capacity(2);
    arr.push(Value::bulk(Bytes::from(e.id.fmt_id())));
    let mut fields = Vec::with_capacity(e.fields.len() * 2);
    for (k, v) in &e.fields {
        fields.push(Value::bulk(k.clone()));
        fields.push(Value::bulk(v.clone()));
    }
    arr.push(Value::Array(fields));
    Value::Array(arr)
}

fn xinfo_stream_basic_reply(stream: &crate::foundation::io::store::structure::stream::ValkeyStream) -> Value {
    let mut reply = vec![bs("length")];
    reply.push(Value::integer(stream.len() as i64));

    reply.push(bs("radix-tree-keys"));
    reply.push(Value::integer(stream.len() as i64));

    reply.push(bs("radix-tree-nodes"));
    reply.push(Value::integer(std::cmp::max(1, stream.len()) as i64));

    reply.push(bs("groups"));
    reply.push(Value::integer(stream.groups().count() as i64));

    reply.push(bs("last-generated-id"));
    reply.push(Value::bulk(Bytes::from(stream.last_id.fmt_id())));

    reply.push(bs("first-entry"));
    match stream.first_entry() {
        Some(ref e) => reply.push(entry_to_simple_value(e)),
        None => reply.push(Value::null()),
    }

    reply.push(bs("last-entry"));
    match stream.last_entry() {
        Some(ref e) => reply.push(entry_to_simple_value(e)),
        None => reply.push(Value::null()),
    }

    reply.push(bs("max-deleted-entry-id"));
    reply.push(Value::bulk(Bytes::from(
        stream.max_deleted_entry_id.fmt_id(),
    )));

    reply.push(bs("entries-added"));
    reply.push(Value::integer(stream.entries_added as i64));

    reply.push(bs("recorded-first-entry-id"));
    let rfe = stream.recorded_first_entry_id();
    reply.push(Value::bulk(Bytes::from(rfe.fmt_id())));

    Value::Array(reply)
}

fn xinfo_stream_full_reply(
    stream: &crate::foundation::io::store::structure::stream::ValkeyStream,
    count: Option<usize>,
) -> Value {
    let mut reply = vec![bs("length")];
    reply.push(Value::integer(stream.len() as i64));

    reply.push(bs("radix-tree-keys"));
    reply.push(Value::integer(stream.len() as i64));

    reply.push(bs("radix-tree-nodes"));
    reply.push(Value::integer(std::cmp::max(1, stream.len()) as i64));

    reply.push(bs("groups"));
    reply.push(xinfo_groups_full_reply(stream));

    reply.push(bs("last-generated-id"));
    reply.push(Value::bulk(Bytes::from(stream.last_id.fmt_id())));

    reply.push(bs("max-deleted-entry-id"));
    reply.push(Value::bulk(Bytes::from(
        stream.max_deleted_entry_id.fmt_id(),
    )));

    reply.push(bs("entries-added"));
    reply.push(Value::integer(stream.entries_added as i64));

    reply.push(bs("recorded-first-entry-id"));
    let rfe = stream.recorded_first_entry_id();
    reply.push(Value::bulk(Bytes::from(rfe.fmt_id())));

    reply.push(bs("entries"));
    let mut entries_arr = Vec::new();
    let limit = count.unwrap_or(usize::MAX);
    for (id, fields) in stream.iter().take(limit) {
        entries_arr.push(entry_to_full_value(id, fields));
    }
    reply.push(Value::Array(entries_arr));

    Value::Array(reply)
}

fn xinfo_groups_reply(stream: &crate::foundation::io::store::structure::stream::ValkeyStream) -> Value {
    Value::Array(
        stream
            .groups()
            .map(|group| group_info_value(stream, group))
            .collect(),
    )
}

fn xinfo_groups_full_reply(stream: &crate::foundation::io::store::structure::stream::ValkeyStream) -> Value {
    Value::Array(
        stream
            .groups()
            .map(|group| group_full_value(stream, group))
            .collect(),
    )
}

fn xinfo_consumers_reply(
    stream: &crate::foundation::io::store::structure::stream::ValkeyStream,
    group_name: &[u8],
    now_ms: i64,
) -> Value {
    let Some(group) = stream.group(group_name) else {
        return Value::error("NOGROUP No such key or consumer group");
    };
    Value::Array(
        group
            .consumers
            .values()
            .map(|consumer| consumer_info_value(consumer, now_ms, false))
            .collect(),
    )
}

fn group_info_value(
    stream: &crate::foundation::io::store::structure::stream::ValkeyStream,
    group: &crate::foundation::io::store::structure::stream::groups::StreamGroup,
) -> Value {
    Value::Array(vec![
        bs("name"),
        Value::bulk(group.name.clone()),
        bs("consumers"),
        Value::integer(group.consumers.len() as i64),
        bs("pending"),
        Value::integer(group.pending.len() as i64),
        bs("last-delivered-id"),
        Value::bulk(Bytes::from(group.last_id.fmt_id())),
        bs("entries-read"),
        opt_u64(group.entries_read),
        bs("lag"),
        group_lag(stream, group),
    ])
}

fn group_full_value(
    stream: &crate::foundation::io::store::structure::stream::ValkeyStream,
    group: &crate::foundation::io::store::structure::stream::groups::StreamGroup,
) -> Value {
    let pending = group
        .pending
        .keys()
        .map(|id| Value::Array(vec![Value::bulk(Bytes::from(id.fmt_id()))]))
        .collect();
    Value::Array(vec![
        bs("name"),
        Value::bulk(group.name.clone()),
        bs("last-delivered-id"),
        Value::bulk(Bytes::from(group.last_id.fmt_id())),
        bs("entries-read"),
        opt_u64(group.entries_read),
        bs("lag"),
        group_lag(stream, group),
        bs("pending"),
        Value::Array(pending),
        bs("consumers"),
        Value::Array(
            group
                .consumers
                .values()
                .map(consumer_full_value)
                .collect(),
        ),
    ])
}

fn consumer_full_value(consumer: &crate::foundation::io::store::structure::stream::groups::StreamConsumer) -> Value {
    Value::Array(vec![
        bs("name"),
        Value::bulk(consumer.name.clone()),
        bs("pending"),
        Value::Array(
            consumer
                .pending
                .iter()
                .map(|id| Value::Array(vec![Value::bulk(Bytes::from(id.fmt_id()))]))
                .collect(),
        ),
        bs("seen-time"),
        Value::integer(consumer.seen_time_ms),
        bs("active-time"),
        match consumer.active_time_ms {
            Some(active) => Value::integer(active),
            None => Value::integer(-1),
        },
    ])
}

fn consumer_info_value(
    consumer: &crate::foundation::io::store::structure::stream::groups::StreamConsumer,
    now: i64,
    include_pending_ids: bool,
) -> Value {
    let mut arr = vec![
        bs("name"),
        Value::bulk(consumer.name.clone()),
        bs("pending"),
    ];
    if include_pending_ids {
        arr.push(Value::Array(
            consumer
                .pending
                .iter()
                .map(|id| Value::Array(vec![Value::bulk(Bytes::from(id.fmt_id()))]))
                .collect(),
        ));
    } else {
        arr.push(Value::integer(consumer.pending.len() as i64));
    }
    arr.push(bs("idle"));
    arr.push(Value::integer(
        now.saturating_sub(consumer.seen_time_ms).max(0),
    ));
    arr.push(bs("inactive"));
    arr.push(match consumer.active_time_ms {
        Some(active) => Value::integer(now.saturating_sub(active).max(0)),
        None => Value::integer(-1),
    });
    arr.push(bs("seen-time"));
    arr.push(Value::integer(consumer.seen_time_ms));
    arr.push(bs("active-time"));
    arr.push(match consumer.active_time_ms {
        Some(active) => Value::integer(active),
        None => Value::integer(-1),
    });
    Value::Array(arr)
}

fn group_lag(
    stream: &crate::foundation::io::store::structure::stream::ValkeyStream,
    group: &crate::foundation::io::store::structure::stream::groups::StreamGroup,
) -> Value {
    if group.entries_read.is_some() {
        if stream.max_deleted_entry_id > group.last_id
            && stream
                .first_id()
                .is_some_and(|first| stream.max_deleted_entry_id >= first)
            && stream.max_deleted_entry_id <= stream.last_entry_id()
        {
            return Value::null();
        }
        return group_lag_for_counts(group.entries_read, stream.entries_added, 0);
    }
    if stream.entries_added == 0 || stream.is_empty() {
        return Value::integer(0);
    }
    if stream
        .first_id()
        .is_some_and(|first| stream.max_deleted_entry_id >= first)
    {
        return Value::null();
    }
    Value::integer(stream.len() as i64)
}

fn group_lag_for_counts(entries_read: Option<u64>, entries_added: u64, default: i64) -> Value {
    match entries_read {
        Some(read) => Value::integer(entries_added.saturating_sub(read) as i64),
        None => Value::integer(default),
    }
}

fn opt_u64(value: Option<u64>) -> Value {
    value
        .map(|v| Value::integer(v as i64))
        .unwrap_or_else(Value::null)
}
