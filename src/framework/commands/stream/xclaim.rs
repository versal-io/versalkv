use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::stream::runtime_support::parse_stream_id;
use crate::framework::{CommandArgs, CommandError};
use crate::foundation::io::store::structure::stream::groups::ClaimedEntry;
use crate::foundation::io::store::structure::stream::StreamID;
use bytes::Bytes;
use command_macros::command;

#[command("XCLAIM", runtime)]
pub fn xclaim(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let db_idx = runtime.selected_db();
    let key = args.args.first().cloned().unwrap_or_default();
    let mut consumer_created = false;
    let reply = xclaim_reply(runtime, args, &mut consumer_created);
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

fn xclaim_reply(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
    consumer_created: &mut bool,
) -> Value {
    // XCLAIM key group consumer min-idle-time id [id ...] [FORCE] [JUSTID] [LASTID id] [RETRYCOUNT retry_count]
    if args.args.len() < 4 {
        return CommandError::wrong_arity("xclaim");
    }
    let key = &args.args[0];
    let group = &args.args[1];
    let consumer = &args.args[2];
    let min_idle_ms: i64 = match args.parse(3) {
        Ok(v) => v,
        Err(_) => return Value::error("ERR value is not an integer or out of range"),
    };
    if min_idle_ms < 0 {
        return Value::error("ERR value is not an integer or out of range");
    }

    let mut ids = Vec::new();
    let mut just_id = false;
    let mut force = false;
    let mut delivery_time_ms: Option<i64> = None;
    let mut retry_count: Option<u64> = None;
    let mut last_id: Option<StreamID> = None;
    let mut i = 4;
    while i < args.args.len() {
        if args.args[i].eq_ignore_ascii_case(b"JUSTID") {
            just_id = true;
            i += 1;
        } else if args.args[i].eq_ignore_ascii_case(b"FORCE") {
            force = true;
            i += 1;
        } else if args.args[i].eq_ignore_ascii_case(b"LASTID") {
            if i + 1 >= args.args.len() {
                return CommandError::syntax_error();
            }
            match parse_stream_id(&args.args[i + 1], false) {
                Ok(id) => last_id = Some(id),
                Err(e) => return e,
            }
            i += 2;
        } else if args.args[i].eq_ignore_ascii_case(b"RETRYCOUNT") {
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
            retry_count = Some(parsed as u64);
            i += 2;
        } else if args.args[i].eq_ignore_ascii_case(b"TIME") {
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
            delivery_time_ms = Some(parsed);
            i += 2;
        } else {
            match parse_stream_id(&args.args[i], false) {
                Ok(id) => ids.push(id),
                Err(e) => return e,
            }
            i += 1;
        }
    }

    let now_ms = runtime.now_ms() as i64;
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandError::db_index_out_of_range(),
    };
    let stream = match db.get_stream_mut(key) {
        Ok(Some(stream)) => stream,
        Ok(None) => return Value::error("NOGROUP No such key or consumer group"),
        Err(_) => return CommandError::wrong_type(),
    };
    let result = match stream.claim_group_pending(
        group,
        consumer,
        min_idle_ms,
        &ids,
        just_id,
        force,
        delivery_time_ms,
        retry_count,
        last_id,
        now_ms,
    ) {
        Ok(result) => result,
        Err(_) => return Value::error("NOGROUP No such key or consumer group"),
    };
    *consumer_created = result.consumer_created;
    Value::Array(
        result
            .claimed
            .into_iter()
            .filter_map(|claimed| match claimed {
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
            .collect(),
    )
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
