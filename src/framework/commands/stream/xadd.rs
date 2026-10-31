use super::runtime_support::parse_add_trim_args;
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::foundation::io::store::structure::stream::StreamID;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

#[command("XADD", runtime)]
pub fn xadd(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.args.len() < 4 {
        return CommandOutcome::reply(CommandError::wrong_arity("xadd"));
    }
    let parsed = match parse_add_trim_args(&args.args, true, runtime.must_obey_command()) {
        Ok(parsed) => parsed,
        Err(error) => return CommandOutcome::reply(error),
    };
    let Some((use_id, seq_auto, requested_id)) = parsed.id else {
        return CommandOutcome::reply(CommandError::wrong_arity("xadd"));
    };
    let fields_start = parsed.id_index + 1;
    let remaining = args.args.len() - fields_start;
    if remaining == 0 || !remaining.is_multiple_of(2) {
        return CommandOutcome::reply(CommandError::wrong_arity("xadd"));
    }
    if use_id && !seq_auto && requested_id == StreamID::zero() {
        return CommandOutcome::reply(Value::error(
            "ERR The ID specified in XADD must be greater than 0-0",
        ));
    }
    let fields = args.args[fields_start..]
        .as_chunks::<2>()
        .0
        .iter()
        .map(|[field, value]| (field.clone(), value.clone()))
        .collect();
    let (node_max_entries, node_max_bytes) = runtime.stream_node_limits();
    let now_ms = runtime.now_ms();
    let db_idx = runtime.selected_db();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    let key = args.args[0].clone();
    let exists = db.exists(&key);
    if parsed.no_mkstream && !exists {
        return CommandOutcome {
            reply: Value::null(),
            effects: CommandEffects {
                skip_propagate: true,
                dirty_delta: Some(0),
                ..CommandEffects::default()
            },
        };
    }
    let stream = match db.get_or_create_stream(&key) {
        Ok(stream) => stream,
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
    };
    if stream.last_id == StreamID::max() {
        return CommandOutcome::reply(Value::error(
            "ERR The stream has exhausted the last possible ID, unable to add more items",
        ));
    }
    stream.set_append_node_limits(node_max_entries, node_max_bytes);
    let new_id = match stream.append(requested_id, fields, use_id, seq_auto, now_ms) {
        Ok(id) => id,
        Err(error) => {
            if !exists && stream.entries_added == 0 {
                db.del(&key);
            }
            return CommandOutcome::reply(Value::error(error));
        }
    };
    let trimmed = parsed.trim(stream, node_max_entries, node_max_bytes);
    let propagation = parsed.propagation(b"XADD", &args.args, stream, Some(new_id));
    let mut events = vec![CoreEvent {
        kind: "xadd",
        key: key.clone(),
    }];
    if trimmed != 0 {
        events.push(CoreEvent {
            kind: "xtrim",
            key: key.clone(),
        });
    }
    CommandOutcome {
        reply: Value::bulk(Bytes::from(new_id.fmt_id())),
        effects: CommandEffects {
            changed_keys: vec![(db_idx, key)],
            events,
            custom_propagations: vec![propagation],
            dirty_delta: Some(1),
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
        let stream = runtime
            .db_mut()
            .unwrap()
            .get_or_create_stream(b"stream")
            .unwrap();
        stream.set_append_node_limits(100, 1);
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
