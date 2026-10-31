use crate::engine::core::{CommandEffects, CommandOutcome, Value};
use crate::foundation::io::store::structure::stream::groups::ClaimedEntry;
use crate::foundation::io::store::structure::stream::StreamID;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::stream::runtime_support::{
    claim_effects, parse_signed_integer, parse_stream_id,
};
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[command("XCLAIM", runtime)]
pub fn xclaim(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let mut effects = CommandEffects {
        skip_propagate: true,
        dirty_delta: Some(0),
        ..CommandEffects::default()
    };
    let reply = xclaim_reply(runtime, args, &mut effects);
    CommandOutcome { reply, effects }
}

fn xclaim_reply(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
    effects: &mut CommandEffects,
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
            let parsed = match parse_signed_integer(&args.args[i + 1]) {
                Some(v) => v,
                None => return Value::error("ERR Invalid RETRYCOUNT option argument for XCLAIM"),
            };
            // C treats every negative RETRYCOUNT as unspecified, including signed propagated counts.
            retry_count = u64::try_from(parsed).ok();
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
    let db_idx = runtime.selected_db();
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
    if let Some(group_state) = stream.group(group) {
        *effects = claim_effects(
            db_idx,
            key,
            group,
            group_state.last_id,
            group_state.entries_read,
            &result,
        );
    }
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
    fn claims_dirty_existing_consumers_only_when_pending_or_last_id_changes() {
        let cases: &[(&[&[u8]], bool)] = &[
            (&[b"1-0"], true),
            (&[b"1-0", b"JUSTID"], true),
            (&[b"2-0"], false),
            (&[b"2-0", b"LASTID", b"2-0"], true),
            (&[b"2-0", b"LASTID", b"0-0"], false),
            (&[b"2-0", b"FORCE"], true),
        ];
        for &(options, should_dirty) in cases {
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
            for ms in 1..=2 {
                stream
                    .append(StreamID::new(ms, 0), vec![], true, false, 0)
                    .unwrap();
            }
            stream.read_group_new(b"g", b"c", 1, false, 0).unwrap();
            let mut parts = vec![b"XCLAIM".as_slice(), b"s", b"g", b"c", b"0"];
            parts.extend_from_slice(options);
            let outcome = xclaim(&mut runtime, &args(&parts));
            assert!(matches!(outcome.reply, Value::Array(_)));
            assert_eq!(
                outcome.effects.changed_keys,
                if should_dirty {
                    vec![(1, Bytes::from_static(b"s"))]
                } else {
                    vec![]
                },
                "options: {options:?}"
            );
            assert!(outcome.effects.events.is_empty());
            assert!(outcome.effects.skip_propagate);
            assert_eq!(
                outcome.effects.custom_propagations.len(),
                usize::from(should_dirty)
            );

            let outcome = xclaim(
                &mut runtime,
                &args(&[b"XCLAIM", b"s", b"g", b"c", b"100", b"1-0"]),
            );
            assert_eq!(outcome.reply, Value::Array(vec![]));
            assert!(outcome.effects.changed_keys.is_empty());
            assert!(outcome.effects.skip_propagate);
        }
    }

    #[test]
    fn claims_record_consumer_creation_but_not_invalid_arguments() {
        let mut runtime = TestCommandRuntime::new(1, 0);
        let stream = runtime
            .db_mut()
            .unwrap()
            .get_or_create_stream(b"s")
            .unwrap();
        stream
            .create_group(Bytes::from_static(b"g"), StreamID::zero(), None)
            .unwrap();
        let outcome = xclaim(
            &mut runtime,
            &args(&[b"XCLAIM", b"s", b"g", b"c", b"0", b"1-0"]),
        );
        assert_eq!(outcome.reply, Value::Array(vec![]));
        assert_eq!(
            outcome.effects.changed_keys,
            vec![(0, Bytes::from_static(b"s"))]
        );
        assert_eq!(outcome.effects.events[0].kind, "xgroup-createconsumer");
        let outcome = xclaim(
            &mut runtime,
            &args(&[b"XCLAIM", b"s", b"g", b"other", b"0", b"invalid"]),
        );
        assert!(matches!(outcome.reply, Value::Error(_)));
        assert!(outcome.effects.changed_keys.is_empty());
        assert!(!runtime
            .db_mut()
            .unwrap()
            .get_stream(b"s")
            .unwrap()
            .unwrap()
            .group(b"g")
            .unwrap()
            .consumers
            .contains_key(b"other".as_slice()));
    }
}
