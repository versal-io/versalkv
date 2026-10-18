use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::stream::runtime_support::parse_stream_id;
use crate::framework::{CommandArgs, CommandError};
use crate::foundation::io::store::structure::stream::groups::ClaimedEntry;
use crate::foundation::io::store::structure::stream::StreamID;
use bytes::Bytes;
use command_macros::command;

#[command("XAUTOCLAIM", runtime)]
pub fn xautoclaim(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
) -> CommandOutcome {
    let db_idx = runtime.selected_db();
    let key = args.args.first().cloned().unwrap_or_default();
    let mut consumer_created = false;
    let reply = xautoclaim_reply(runtime, args, &mut consumer_created);
    CommandOutcome {
        reply,
        effects: CommandEffects {
            events: consumer_created
                .then(|| CoreEvent {
                    kind: "xgroup-createconsumer",
                    key,
                })
                .into_iter()
                .collect(),
            changed_keys: consumer_created.then(|| (db_idx, args.args[0].clone())).into_iter().collect(),
            ..CommandEffects::default()
        },
    }
}

fn xautoclaim_reply(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
    consumer_created: &mut bool,
) -> Value {
    if args.args.len() < 5 {
        return CommandError::wrong_arity("xautoclaim");
    }
    let min_idle_ms: i64 = match args.parse(3) {
        Ok(v) => v,
        Err(_) => return Value::error("ERR value is not an integer or out of range"),
    };
    if min_idle_ms < 0 {
        return Value::error("ERR value is not an integer or out of range");
    }
    let start = match parse_start_id(&args.args[4]) {
        Ok(id) => id,
        Err(e) => return e,
    };
    let mut count = 100usize;
    let mut just_id = false;
    let mut i = 5;
    while i < args.args.len() {
        if args.args[i].eq_ignore_ascii_case(b"COUNT") {
            if i + 1 >= args.args.len() {
                return CommandError::syntax_error();
            }
            let parsed: i64 = match args.parse(i + 1) {
                Ok(v) => v,
                Err(_) => return Value::error("ERR COUNT must be > 0"),
            };
            if parsed <= 0 || parsed > i32::MAX as i64 {
                return Value::error("ERR COUNT must be > 0");
            }
            count = match usize::try_from(parsed) {
                Ok(v) => v,
                Err(_) => return Value::error("ERR COUNT must be > 0"),
            };
            i += 2;
        } else if args.args[i].eq_ignore_ascii_case(b"JUSTID") {
            just_id = true;
            i += 1;
        } else {
            return CommandError::syntax_error();
        }
    }

    let now_ms = runtime.now_ms() as i64;
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandError::db_index_out_of_range(),
    };
    let stream = match db.get_stream_mut(&args.args[0]) {
        Ok(Some(stream)) => stream,
        Ok(None) => return Value::error("NOGROUP No such key or consumer group"),
        Err(_) => return CommandError::wrong_type(),
    };
    let (next, claimed, deleted, created) = match stream.autoclaim_group_pending(
        &args.args[1],
        &args.args[2],
        min_idle_ms,
        start,
        count,
        just_id,
        now_ms,
    ) {
        Ok(result) => result,
        Err(_) => return Value::error("NOGROUP No such key or consumer group"),
    };
    *consumer_created = created;
    let entries = claimed
        .into_iter()
        .filter_map(|entry| match entry {
            ClaimedEntry::Entry(entry) => {
                if just_id {
                    Some(Value::bulk(Bytes::from(entry.id.fmt_id())))
                } else {
                    Some(entry_reply(entry.id, &entry.fields))
                }
            }
            ClaimedEntry::Deleted(id) => {
                if just_id {
                    Some(Value::bulk(Bytes::from(id.fmt_id())))
                } else {
                    None
                }
            }
        })
        .collect();
    Value::Array(vec![
        Value::bulk(Bytes::from(next.fmt_id())),
        Value::Array(entries),
        Value::Array(
            deleted
                .into_iter()
                .map(|id| Value::bulk(Bytes::from(id.fmt_id())))
                .collect(),
        ),
    ])
}

fn parse_start_id(raw: &[u8]) -> Result<StreamID, Value> {
    if raw == b"-" {
        Ok(StreamID::zero())
    } else {
        parse_stream_id(raw, false)
    }
}

fn entry_reply(id: StreamID, fields: &[(Bytes, Bytes)]) -> Value {
    let mut arr = Vec::with_capacity(2);
    arr.push(Value::bulk(Bytes::from(id.fmt_id())));
    let mut fv = Vec::with_capacity(fields.len() * 2);
    for (field, value) in fields {
        fv.push(Value::bulk(field.clone()));
        fv.push(Value::bulk(value.clone()));
    }
    arr.push(Value::Array(fv));
    Value::Array(arr)
}
