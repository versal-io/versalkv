use super::runtime_support::parse_add_trim_args;
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("XTRIM", runtime)]
pub fn xtrim(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.args.len() < 2 {
        return CommandOutcome::reply(CommandError::wrong_arity("xtrim"));
    }
    let parsed = match parse_add_trim_args(&args.args, false, runtime.must_obey_command()) {
        Ok(parsed) => parsed,
        Err(error) => return CommandOutcome::reply(error),
    };
    let (node_max_entries, node_max_bytes) = runtime.stream_node_limits();
    let db_idx = runtime.selected_db();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    let key = args.args[0].clone();
    let stream = match db.get_stream_mut(&key) {
        Ok(Some(stream)) => stream,
        Ok(None) => return no_change(),
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
    };
    let removed = parsed.trim(stream, node_max_entries, node_max_bytes);
    if removed == 0 {
        return no_change();
    }
    let propagation = parsed.propagation(b"XTRIM", &args.args, stream, None);
    CommandOutcome {
        reply: Value::integer(removed as i64),
        effects: CommandEffects {
            changed_keys: vec![(db_idx, key.clone())],
            events: vec![CoreEvent { kind: "xtrim", key }],
            custom_propagations: vec![propagation],
            dirty_delta: Some(removed as i64),
            ..CommandEffects::default()
        },
    }
}

fn no_change() -> CommandOutcome {
    CommandOutcome {
        reply: Value::integer(0),
        effects: CommandEffects {
            skip_propagate: true,
            dirty_delta: Some(0),
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
    fn xtrim_rejects_negative_length_and_does_not_create_missing_key() {
        let mut runtime = TestCommandRuntime::new(1, 0);
        let args = CommandArgs::from_parts(
            [b"XTRIM".as_slice(), b"s", b"MAXLEN", b"-1"]
                .into_iter()
                .map(Bytes::copy_from_slice)
                .collect(),
        );
        assert_eq!(
            xtrim(&mut runtime, &args).reply,
            Value::error("ERR The MAXLEN argument must be >= 0.")
        );
        assert!(!runtime.db_mut().unwrap().exists(b"s"));
        let args = CommandArgs::from_parts(
            [b"XTRIM".as_slice(), b"s", b"MAXLEN", b"0"]
                .into_iter()
                .map(Bytes::copy_from_slice)
                .collect(),
        );
        assert_eq!(xtrim(&mut runtime, &args).reply, Value::integer(0));
        assert!(!runtime.db_mut().unwrap().exists(b"s"));
    }

    #[test]
    fn xtrim_uses_stream_node_max_bytes_for_approximate_boundaries() {
        let mut runtime = TestCommandRuntime::new(1, 0);
        runtime.stream_node_limits = (100, 1);
        let stream = runtime
            .db_mut()
            .unwrap()
            .get_or_create_stream(b"stream")
            .unwrap();
        stream.set_append_node_limits(100, 1);
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
        assert_eq!(xtrim(&mut runtime, &args).reply, Value::integer(3));
    }

    #[test]
    fn xtrim_maxlen_matches_exact_and_approximate_tcl_sequence() {
        let mut runtime = TestCommandRuntime::new(1, 0);
        let stream = runtime
            .db_mut()
            .unwrap()
            .get_or_create_stream(b"mystream")
            .unwrap();
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
        for (marker, count, expected) in [
            (None, b"666".as_slice(), 666),
            (Some(b"=".as_slice()), b"555".as_slice(), 555),
            (Some(b"~".as_slice()), b"444".as_slice(), 500),
        ] {
            let mut parts = vec![
                Bytes::from_static(b"XTRIM"),
                Bytes::from_static(b"mystream"),
                Bytes::from_static(b"MAXLEN"),
            ];
            if let Some(marker) = marker {
                parts.push(Bytes::copy_from_slice(marker));
            }
            parts.push(Bytes::copy_from_slice(count));
            let args = CommandArgs::from_parts(parts);
            assert!(matches!(
                xtrim(&mut runtime, &args).reply,
                Value::Integer(_)
            ));
            assert_eq!(
                runtime
                    .db_mut()
                    .unwrap()
                    .get_stream(b"mystream")
                    .unwrap()
                    .unwrap()
                    .len(),
                expected
            );
        }
    }
}
