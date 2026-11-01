use super::{parse_signed_integer, parse_stream_id_strict};
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::foundation::io::store::structure::stream::StreamReferenceMode;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;

pub(crate) fn delete_references(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
    has_target: bool,
) -> CommandOutcome {
    let minimum = if has_target { 5 } else { 4 };
    if args.args.len() < minimum {
        return CommandOutcome::reply(CommandError::wrong_arity(if has_target {
            "xackdel"
        } else {
            "xdelex"
        }));
    }
    let db_idx = runtime.selected_db();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    let key = args.args[0].clone();
    let stream = match db.get_stream_mut(&key) {
        Ok(stream) => stream,
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
    };
    let target = has_target.then(|| args.args[1].as_ref());
    let mut index = if has_target { 2 } else { 1 };
    let mode = if args.args[index].eq_ignore_ascii_case(b"KEEPREF") {
        index += 1;
        StreamReferenceMode::KeepRef
    } else if args.args[index].eq_ignore_ascii_case(b"DELREF") {
        index += 1;
        StreamReferenceMode::DelRef
    } else if args.args[index].eq_ignore_ascii_case(b"ACKED") {
        index += 1;
        StreamReferenceMode::Acked
    } else {
        StreamReferenceMode::KeepRef
    };
    if !args
        .args
        .get(index)
        .is_some_and(|raw| raw.eq_ignore_ascii_case(b"IDS"))
    {
        return CommandOutcome::reply(CommandError::syntax_error());
    }
    index += 1;
    let Some(count) = args
        .args
        .get(index)
        .and_then(|raw| parse_signed_integer(raw))
        .filter(|count| *count > 0)
    else {
        return CommandOutcome::reply(Value::error("ERR Number of IDs must be a positive integer"));
    };
    index += 1;
    if count as u64 != (args.args.len() - index) as u64 {
        return CommandOutcome::reply(CommandError::syntax_error());
    }
    let mut effects = CommandEffects {
        skip_propagate: true,
        dirty_delta: Some(0),
        ..CommandEffects::default()
    };
    let Some(stream) =
        stream.filter(|stream| target.is_none_or(|name| stream.group(name).is_some()))
    else {
        return CommandOutcome {
            reply: Value::Array(vec![Value::integer(-1); count as usize]),
            effects,
        };
    };
    let ids = match args.args[index..]
        .iter()
        .map(|raw| parse_stream_id_strict(raw))
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(ids) => ids,
        Err(error) => return CommandOutcome::reply(error),
    };
    let result = match stream.delete_with_references(target, &ids, mode) {
        Ok(result) => result,
        Err(_) => return CommandOutcome::reply(CommandError::syntax_error()),
    };
    effects.dirty_delta = Some(result.changed_count() as i64);
    if result.changed_count() != 0 {
        effects.changed_keys.push((db_idx, key.clone()));
    }
    for (group, acked) in result.acknowledgements {
        let mut parts = vec![Bytes::from_static(b"XACK"), key.clone(), group];
        parts.extend(acked.iter().map(|id| Bytes::from(id.fmt_id())));
        effects.custom_propagations.push(parts);
    }
    if !result.deleted_ids.is_empty() {
        effects.events.push(CoreEvent {
            kind: "xdel",
            key: key.clone(),
        });
        let mut parts = vec![Bytes::from_static(b"XDEL"), key];
        parts.extend(result.deleted_ids.iter().map(|id| Bytes::from(id.fmt_id())));
        effects.custom_propagations.push(parts);
    }
    CommandOutcome {
        reply: Value::Array(result.statuses.into_iter().map(Value::integer).collect()),
        effects,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::io::store::structure::stream::StreamID;
    use crate::framework::commands::command_runtime::TestCommandRuntime;

    fn args(parts: &[&[u8]]) -> CommandArgs {
        CommandArgs::from_parts(
            parts
                .iter()
                .map(|part| Bytes::copy_from_slice(part))
                .collect(),
        )
    }

    fn seed(runtime: &mut TestCommandRuntime) {
        let stream = runtime
            .db_mut()
            .unwrap()
            .get_or_create_stream(b"s")
            .unwrap();
        for ms in 1..=2 {
            stream
                .append(StreamID::new(ms, 0), vec![], true, false, 0)
                .unwrap();
        }
        for group in [b"a".as_slice(), b"z"] {
            stream
                .create_group(Bytes::copy_from_slice(group), StreamID::zero(), None)
                .unwrap();
            stream.read_group_new(group, b"c", 2, false, 7).unwrap();
        }
    }

    #[test]
    fn combined_delete_effects_use_ordered_primitives_and_actual_change_count() {
        let mut runtime = TestCommandRuntime::new(1, 0);
        seed(&mut runtime);
        let outcome = delete_references(
            &mut runtime,
            &args(&[
                b"XACKDEL", b"s", b"a", b"DELREF", b"IDS", b"2", b"1-0", b"1-0",
            ]),
            true,
        );
        assert_eq!(
            outcome.reply,
            Value::Array(vec![Value::integer(1), Value::integer(-1)])
        );
        assert_eq!(outcome.effects.dirty_delta, Some(3));
        assert!(outcome.effects.skip_propagate);
        assert_eq!(
            outcome.effects.changed_keys,
            vec![(0, Bytes::from_static(b"s"))]
        );
        assert_eq!(
            outcome.effects.events,
            vec![CoreEvent {
                kind: "xdel",
                key: Bytes::from_static(b"s")
            }]
        );
        assert_eq!(
            outcome.effects.custom_propagations,
            [
                vec![b"XACK".as_slice(), b"s", b"z", b"1-0"],
                vec![b"XACK".as_slice(), b"s", b"a", b"1-0"],
                vec![b"XDEL".as_slice(), b"s", b"1-0"],
            ]
            .into_iter()
            .map(|parts| parts.into_iter().map(Bytes::copy_from_slice).collect())
            .collect::<Vec<Vec<_>>>()
        );
        let stream = runtime
            .db_mut()
            .unwrap()
            .get_stream_mut(b"s")
            .unwrap()
            .unwrap();
        stream.delete(&StreamID::new(2, 0));
        let outcome = delete_references(
            &mut runtime,
            &args(&[b"XDELEX", b"s", b"DELREF", b"IDS", b"1", b"2-0"]),
            false,
        );
        assert_eq!(outcome.reply, Value::Array(vec![Value::integer(-1)]));
        assert_eq!(outcome.effects.dirty_delta, Some(2));
        assert_eq!(outcome.effects.custom_propagations.len(), 2);
        assert!(outcome.effects.events.is_empty());
        assert_eq!(
            outcome.effects.changed_keys,
            vec![(0, Bytes::from_static(b"s"))]
        );
        let outcome = delete_references(
            &mut runtime,
            &args(&[b"XDELEX", b"s", b"ACKED", b"IDS", b"1", b"2-0"]),
            false,
        );
        assert_eq!(outcome.effects.dirty_delta, Some(0));
        assert!(outcome.effects.custom_propagations.is_empty());
        assert!(outcome.effects.changed_keys.is_empty());
    }

    #[test]
    fn combined_delete_validates_syntax_before_missing_group_and_all_ids_before_mutation() {
        let mut runtime = TestCommandRuntime::new(1, 0);
        for (parts, expected) in [
            (
                vec![b"XACKDEL".as_slice(), b"s", b"a", b"IDS", b"1", b"bad"],
                Value::Array(vec![Value::integer(-1)]),
            ),
            (
                vec![b"XACKDEL".as_slice(), b"s", b"a", b"IDS", b"01", b"bad"],
                Value::error("ERR Number of IDs must be a positive integer"),
            ),
            (
                vec![b"XDELEX".as_slice(), b"s", b"IDS", b"2", b"bad"],
                CommandError::syntax_error(),
            ),
        ] {
            assert_eq!(
                delete_references(&mut runtime, &args(&parts), parts[0] == b"XACKDEL").reply,
                expected
            );
        }
        seed(&mut runtime);
        let outcome = delete_references(
            &mut runtime,
            &args(&[
                b"XACKDEL", b"s", b"a", b"DELREF", b"IDS", b"2", b"1-0", b"bad",
            ]),
            true,
        );
        assert_eq!(
            outcome.reply,
            Value::error("ERR Invalid stream ID specified as stream command argument")
        );
        let stream = runtime.db_mut().unwrap().get_stream(b"s").unwrap().unwrap();
        assert_eq!(stream.len(), 2);
        for group in stream.groups() {
            assert_eq!(group.pending.len(), 2);
            assert_eq!(group.consumers[b"c".as_slice()].pending.len(), 2);
        }
    }
}
