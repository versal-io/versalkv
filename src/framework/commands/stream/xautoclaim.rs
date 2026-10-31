use crate::engine::core::{CommandEffects, CommandOutcome, Value};
use crate::foundation::io::store::structure::stream::groups::ClaimedEntry;
use crate::foundation::io::store::structure::stream::StreamID;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::stream::runtime_support::{claim_effects, parse_stream_id};
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[command("XAUTOCLAIM", runtime)]
pub fn xautoclaim(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let mut effects = CommandEffects {
        skip_propagate: true,
        dirty_delta: Some(0),
        ..CommandEffects::default()
    };
    let reply = xautoclaim_reply(runtime, args, &mut effects);
    CommandOutcome { reply, effects }
}

fn xautoclaim_reply(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
    effects: &mut CommandEffects,
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
    let db_idx = runtime.selected_db();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandError::db_index_out_of_range(),
    };
    let stream = match db.get_stream_mut(&args.args[0]) {
        Ok(Some(stream)) => stream,
        Ok(None) => return Value::error("NOGROUP No such key or consumer group"),
        Err(_) => return CommandError::wrong_type(),
    };
    let (next, result) = match stream.autoclaim_group_pending(
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
    if let Some(group_state) = stream.group(&args.args[1]) {
        *effects = claim_effects(
            db_idx,
            &args.args[0],
            &args.args[1],
            group_state.last_id,
            group_state.entries_read,
            &result,
        );
    }
    let entries = result
        .claimed
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
            result
                .deleted
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framework::commands::command_runtime::TestCommandRuntime;

    fn args(parts: &[&[u8]]) -> CommandArgs {
        CommandArgs::from_parts(
            parts
                .iter()
                .map(|part| Bytes::copy_from_slice(part))
                .collect(),
        )
    }

    #[test]
    fn autoclaim_effects_include_delivery_and_deleted_pending_cleanup() {
        for is_deleted in [false, true] {
            for just_id in [false, true] {
                let mut runtime = TestCommandRuntime::new(2, 10);
                runtime.selected_db = 1;
                let stream = runtime
                    .db_mut()
                    .unwrap()
                    .get_or_create_stream(b"s")
                    .unwrap();
                stream
                    .create_group(Bytes::from_static(b"g"), StreamID::zero(), None)
                    .unwrap();
                let id = StreamID::new(1, 0);
                stream.append(id, vec![], true, false, 0).unwrap();
                stream.read_group_new(b"g", b"c", 1, false, 0).unwrap();
                if is_deleted {
                    stream.delete(&id);
                }
                let mut parts = vec![b"XAUTOCLAIM".as_slice(), b"s", b"g", b"c", b"0", b"0-0"];
                if just_id {
                    parts.push(b"JUSTID");
                }
                let outcome = xautoclaim(&mut runtime, &args(&parts));
                assert_eq!(
                    outcome.effects.changed_keys,
                    vec![(1, Bytes::from_static(b"s"))]
                );
                assert!(outcome.effects.events.is_empty());
                let Value::Array(reply) = outcome.reply else {
                    panic!("expected autoclaim reply")
                };
                let Value::Array(entries) = &reply[if is_deleted { 2 } else { 1 }] else {
                    panic!("expected autoclaim entries");
                };
                assert_eq!(entries.len(), 1);
            }
        }
    }

    #[test]
    fn autoclaim_does_not_dirty_existing_consumer_without_eligible_pending_entries() {
        for min_idle_ms in [b"0".as_slice(), b"100"] {
            let mut runtime = TestCommandRuntime::new(1, 10);
            let stream = runtime
                .db_mut()
                .unwrap()
                .get_or_create_stream(b"s")
                .unwrap();
            stream
                .create_group(Bytes::from_static(b"g"), StreamID::zero(), None)
                .unwrap();
            stream.create_consumer(b"g", b"c", 0).unwrap();
            if min_idle_ms == b"100" {
                stream
                    .append(StreamID::new(1, 0), vec![], true, false, 0)
                    .unwrap();
                stream.read_group_new(b"g", b"c", 1, false, 0).unwrap();
            }
            let outcome = xautoclaim(
                &mut runtime,
                &args(&[b"XAUTOCLAIM", b"s", b"g", b"c", min_idle_ms, b"0-0"]),
            );
            assert!(outcome.effects.changed_keys.is_empty());
            assert!(outcome.effects.events.is_empty());
            assert!(outcome.effects.skip_propagate);
        }
    }
}
