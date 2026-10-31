use bytes::Bytes;

use crate::engine::core::{CommandOutcome, Value};
use crate::foundation::io::store::structure::stream::StreamID;
use crate::framework::commands::command_runtime::{CommandRuntime, TestCommandRuntime};
use crate::framework::commands::stream::{xautoclaim::xautoclaim, xclaim::xclaim, xgroup::xgroup};
use crate::framework::CommandArgs;

fn args(parts: &[&[u8]]) -> CommandArgs {
    CommandArgs::from_parts(
        parts
            .iter()
            .map(|part| Bytes::copy_from_slice(part))
            .collect(),
    )
}

fn pending_runtime(count: u64) -> TestCommandRuntime {
    let mut runtime = TestCommandRuntime::new(2, 100);
    runtime.selected_db = 1;
    let stream = runtime
        .db_mut()
        .unwrap()
        .get_or_create_stream(b"s")
        .unwrap();
    stream
        .create_group(Bytes::from_static(b"g"), StreamID::zero(), Some(7))
        .unwrap();
    for ms in 1..=count {
        stream
            .append(StreamID::new(ms, 0), vec![], true, false, 0)
            .unwrap();
    }
    stream
        .read_group_new(b"g", b"old", count as usize, false, 10)
        .unwrap();
    runtime
}

fn canonical(consumer: &[u8], id: &[u8], time: &[u8], retries: &[u8], last: &[u8]) -> Vec<Bytes> {
    [
        b"XCLAIM".as_slice(),
        b"s",
        b"g",
        consumer,
        b"0",
        id,
        b"TIME",
        time,
        b"RETRYCOUNT",
        retries,
        b"FORCE",
        b"JUSTID",
        b"LASTID",
        last,
    ]
    .into_iter()
    .map(Bytes::copy_from_slice)
    .collect()
}

fn claim(runtime: &mut TestCommandRuntime, is_auto: bool, consumer: &[u8]) -> CommandOutcome {
    if is_auto {
        xautoclaim(
            runtime,
            &args(&[b"XAUTOCLAIM", b"s", b"g", consumer, b"0", b"0-0"]),
        )
    } else {
        xclaim(
            runtime,
            &args(&[b"XCLAIM", b"s", b"g", consumer, b"0", b"1-0", b"2-0"]),
        )
    }
}

#[test]
fn stream_final_claim_propagation_uses_final_history_delivery_state() {
    for is_auto in [false, true] {
        let mut runtime = pending_runtime(1);
        runtime
            .db_mut()
            .unwrap()
            .get_stream_mut(b"s")
            .unwrap()
            .unwrap()
            .read_group_history(b"g", b"old", StreamID::zero(), 1, 30)
            .unwrap();
        let outcome = claim(&mut runtime, is_auto, b"new");
        assert_eq!(
            outcome.effects.custom_propagations,
            vec![canonical(b"new", b"1-0", b"100", b"3", b"1-0")]
        );
        assert!(outcome.effects.skip_propagate);
    }
}

#[test]
fn stream_final_claim_dirty_counts_each_id_and_consumer_creation() {
    for is_auto in [false, true] {
        for consumer in [b"old".as_slice(), b"new"] {
            let mut runtime = pending_runtime(2);
            let outcome = claim(&mut runtime, is_auto, consumer);
            assert_eq!(
                outcome.effects.dirty_delta,
                Some(2 + i64::from(consumer == b"new"))
            );
            assert_eq!(
                outcome.effects.changed_keys,
                vec![(1, Bytes::from_static(b"s"))]
            );
        }
    }
}

#[test]
fn stream_final_claim_pure_consumer_creation_never_propagates_original_request() {
    for is_auto in [false, true] {
        let mut runtime = pending_runtime(0);
        let outcome = claim(&mut runtime, is_auto, b"new");
        assert!(outcome.effects.skip_propagate);
        assert!(outcome.effects.custom_propagations.is_empty());
        assert_eq!(outcome.effects.dirty_delta, Some(1));
        assert_eq!(outcome.effects.events[0].kind, "xgroup-createconsumer");
        let outcome = claim(&mut runtime, is_auto, b"new");
        assert_eq!(outcome.effects.dirty_delta, Some(0));
        assert!(outcome.effects.changed_keys.is_empty());
    }
}

#[test]
fn stream_final_claim_deleted_pending_cleanup_ignores_min_idle() {
    for is_auto in [false, true] {
        for just_id in [false, true] {
            let mut runtime = pending_runtime(1);
            runtime
                .db_mut()
                .unwrap()
                .get_stream_mut(b"s")
                .unwrap()
                .unwrap()
                .delete(&StreamID::new(1, 0));
            let mut parts = if is_auto {
                vec![
                    b"XAUTOCLAIM".as_slice(),
                    b"s",
                    b"g",
                    b"new",
                    b"60000",
                    b"0-0",
                ]
            } else {
                vec![b"XCLAIM".as_slice(), b"s", b"g", b"new", b"60000", b"1-0"]
            };
            if just_id {
                parts.push(b"JUSTID");
            }
            let outcome = if is_auto {
                xautoclaim(&mut runtime, &args(&parts))
            } else {
                xclaim(&mut runtime, &args(&parts))
            };
            let group = runtime
                .db_mut()
                .unwrap()
                .get_stream(b"s")
                .unwrap()
                .unwrap()
                .group(b"g")
                .unwrap();
            assert!(group.pending.is_empty());
            assert!(group.consumers[b"old".as_slice()].pending.is_empty());
            assert!(group.consumers[b"new".as_slice()].pending.is_empty());
            assert_eq!(
                outcome.effects.custom_propagations,
                vec![canonical(b"old", b"1-0", b"10", b"1", b"1-0")]
            );
            assert_eq!(outcome.effects.dirty_delta, Some(2));
            assert_eq!(
                outcome.reply,
                if is_auto {
                    Value::Array(vec![
                        Value::bulk(Bytes::from_static(b"0-0")),
                        Value::Array(vec![]),
                        Value::Array(vec![Value::bulk(Bytes::from_static(b"1-0"))]),
                    ])
                } else {
                    Value::Array(vec![])
                }
            );
        }
    }
}

#[test]
fn stream_final_claim_last_id_is_counted_only_without_id_effects() {
    for has_delivery in [false, true] {
        let mut runtime = pending_runtime(1);
        let id = if has_delivery {
            b"1-0".as_slice()
        } else {
            b"9-0"
        };
        let outcome = xclaim(
            &mut runtime,
            &args(&[b"XCLAIM", b"s", b"g", b"old", b"0", id, b"LASTID", b"8-0"]),
        );
        assert_eq!(outcome.effects.dirty_delta, Some(1));
        assert_eq!(
            outcome.effects.custom_propagations,
            if has_delivery {
                vec![canonical(b"old", b"1-0", b"100", b"2", b"8-0")]
            } else {
                vec![[
                    b"XGROUP".as_slice(),
                    b"SETID",
                    b"s",
                    b"g",
                    b"8-0",
                    b"ENTRIESREAD",
                    b"1",
                ]
                .into_iter()
                .map(Bytes::copy_from_slice)
                .collect()]
            }
        );
    }
}

#[test]
fn stream_final_claim_justid_and_explicit_retry_count_are_canonical() {
    for options in [
        vec![b"JUSTID".as_slice()],
        vec![b"TIME".as_slice(), b"50", b"RETRYCOUNT", b"9"],
    ] {
        let mut runtime = pending_runtime(1);
        let mut parts = vec![b"XCLAIM".as_slice(), b"s", b"g", b"old", b"0", b"1-0"];
        parts.extend(options.iter().copied());
        let outcome = xclaim(&mut runtime, &args(&parts));
        let (time, retries) = if options[0] == b"JUSTID" {
            (b"100".as_slice(), b"1".as_slice())
        } else {
            (b"50".as_slice(), b"9".as_slice())
        };
        assert_eq!(
            outcome.effects.custom_propagations,
            vec![canonical(b"old", b"1-0", time, retries, b"1-0")]
        );
    }
}

#[test]
fn stream_final_mkstream_invalid_id_never_creates_a_key() {
    let mut runtime = TestCommandRuntime::new(1, 100);
    let outcome = xgroup(
        &mut runtime,
        &args(&[b"XGROUP", b"CREATE", b"s", b"g", b"invalid", b"MKSTREAM"]),
    );
    assert!(matches!(outcome.reply, Value::Error(_)));
    assert!(runtime
        .db_mut()
        .unwrap()
        .get_stream(b"s")
        .unwrap()
        .is_none());
    assert_eq!(outcome.effects.dirty_delta, Some(0));
    assert!(outcome.effects.changed_keys.is_empty());
}

#[test]
fn stream_final_canonical_claim_replay_matches_unpropagated_history() {
    for is_auto in [false, true] {
        let mut primary = pending_runtime(1);
        let mut replica = pending_runtime(1);
        primary
            .db_mut()
            .unwrap()
            .get_stream_mut(b"s")
            .unwrap()
            .unwrap()
            .read_group_history(b"g", b"old", StreamID::zero(), 1, 30)
            .unwrap();
        let outcome = claim(&mut primary, is_auto, b"new");
        for frame in outcome.effects.custom_propagations {
            let replay = xclaim(&mut replica, &CommandArgs::from_parts(frame));
            assert!(!matches!(replay.reply, Value::Error(_)));
        }
        let primary_group = primary
            .db_mut()
            .unwrap()
            .get_stream(b"s")
            .unwrap()
            .unwrap()
            .group(b"g")
            .unwrap();
        let replica_group = replica
            .db_mut()
            .unwrap()
            .get_stream(b"s")
            .unwrap()
            .unwrap()
            .group(b"g")
            .unwrap();
        assert_eq!(replica_group.pending, primary_group.pending);
        assert_eq!(replica_group.last_id, primary_group.last_id);
    }
}

#[test]
fn stream_final_claim_duplicate_ids_keep_ordered_delivery_snapshots() {
    let mut runtime = pending_runtime(2);
    let outcome = xclaim(
        &mut runtime,
        &args(&[b"XCLAIM", b"s", b"g", b"old", b"0", b"1-0", b"2-0", b"1-0"]),
    );
    assert_eq!(
        outcome.effects.custom_propagations,
        vec![
            canonical(b"old", b"1-0", b"100", b"2", b"2-0"),
            canonical(b"old", b"2-0", b"100", b"2", b"2-0"),
            canonical(b"old", b"1-0", b"100", b"3", b"2-0"),
        ]
    );
    assert_eq!(outcome.effects.dirty_delta, Some(3));
}

#[test]
fn stream_final_claim_force_ignores_idle_only_for_live_new_pending() {
    for just_id in [false, true] {
        let mut runtime = pending_runtime(0);
        runtime
            .db_mut()
            .unwrap()
            .get_stream_mut(b"s")
            .unwrap()
            .unwrap()
            .append(StreamID::new(1, 0), vec![], true, false, 0)
            .unwrap();
        let mut parts = vec![
            b"XCLAIM".as_slice(),
            b"s",
            b"g",
            b"new",
            b"60000",
            b"1-0",
            b"FORCE",
        ];
        if just_id {
            parts.push(b"JUSTID");
        }
        let outcome = xclaim(&mut runtime, &args(&parts));
        assert_eq!(
            outcome.effects.custom_propagations,
            vec![canonical(
                b"new",
                b"1-0",
                b"100",
                if just_id { b"1" } else { b"2" },
                b"0-0"
            )]
        );
        assert_eq!(outcome.effects.dirty_delta, Some(2));
        let outcome = xclaim(&mut runtime, &args(&parts));
        assert!(outcome.effects.custom_propagations.is_empty());
        assert_eq!(outcome.effects.dirty_delta, Some(0));
        let outcome = xclaim(
            &mut runtime,
            &args(&[b"XCLAIM", b"s", b"g", b"new", b"60000", b"9-0", b"FORCE"]),
        );
        assert!(outcome.effects.custom_propagations.is_empty());
        assert_eq!(outcome.effects.dirty_delta, Some(0));
        assert!(!runtime
            .db_mut()
            .unwrap()
            .get_stream(b"s")
            .unwrap()
            .unwrap()
            .group(b"g")
            .unwrap()
            .pending
            .contains_key(&StreamID::new(9, 0)));
    }
}

#[test]
fn stream_spec_repairs_high_delivery_count_frames_preserve_portable_state() {
    for is_auto in [false, true] {
        let mut primary = pending_runtime(1);
        let initial = xclaim(
            &mut primary,
            &args(&[
                b"XCLAIM",
                b"s",
                b"g",
                b"old",
                b"0",
                b"1-0",
                b"RETRYCOUNT",
                b"9223372036854775807",
            ]),
        );
        assert!(matches!(initial.reply, Value::Array(_)));
        let incremented = claim(&mut primary, is_auto, b"new");
        let retained = xclaim(
            &mut primary,
            &args(&[b"XCLAIM", b"s", b"g", b"new", b"0", b"1-0", b"JUSTID"]),
        );
        for outcome in [incremented, retained] {
            assert_eq!(
                outcome.effects.custom_propagations,
                vec![canonical(
                    b"new",
                    b"1-0",
                    b"100",
                    b"-9223372036854775808",
                    b"1-0",
                )]
            );
        }
        let group = primary
            .db_mut()
            .unwrap()
            .get_stream(b"s")
            .unwrap()
            .unwrap()
            .group(b"g")
            .unwrap();
        let pending = &group.pending[&StreamID::new(1, 0)];
        assert_eq!(pending.delivery_count, (i64::MAX as u64) + 1);
        assert_eq!(pending.consumer, Bytes::from_static(b"new"));
        assert_eq!(pending.delivery_time_ms, 100);
        assert!(group.consumers[b"old".as_slice()].pending.is_empty());
        assert!(group.consumers[b"new".as_slice()]
            .pending
            .contains(&StreamID::new(1, 0)));
    }
}

#[cfg(not(feature = "wasm"))]
#[test]
fn stream_spec_repairs_high_delivery_count_frames_follow_signed_c_replay() {
    use crate::foundation::io::persistence::AofCommandPlayer;
    use crate::foundation::io::store::Robj;
    use crate::framework::aof_replay::AofReplayPlayer;
    use crate::framework::context::ServerContext;
    use crate::framework::CommandRegistry;

    for is_auto in [false, true] {
        let mut primary = pending_runtime(1);
        let mut ctx = ServerContext::new(2);
        let stream = primary.db_mut().unwrap().get_stream(b"s").unwrap().unwrap();
        ctx.db_mut(1).unwrap().set(
            b"s".to_vec(),
            Robj::StreamListpack(Box::new(stream.clone())),
        );
        let mut registry = CommandRegistry::new();
        crate::framework::commands::register_all(&mut registry);
        let mut player = AofReplayPlayer::new(&registry, &mut ctx);
        player.select_db(1);

        let initial = xclaim(
            &mut primary,
            &args(&[
                b"XCLAIM",
                b"s",
                b"g",
                b"old",
                b"0",
                b"1-0",
                b"RETRYCOUNT",
                b"9223372036854775807",
            ]),
        );
        assert!(matches!(initial.reply, Value::Array(_)));
        for frame in &initial.effects.custom_propagations {
            player
                .execute(&frame.iter().map(Bytes::as_ref).collect::<Vec<_>>())
                .unwrap();
        }

        let incremented = claim(&mut primary, is_auto, b"new");
        let retained = xclaim(
            &mut primary,
            &args(&[b"XCLAIM", b"s", b"g", b"new", b"0", b"1-0", b"JUSTID"]),
        );
        for outcome in [incremented, retained] {
            assert_eq!(outcome.effects.custom_propagations.len(), 1);
            for frame in &outcome.effects.custom_propagations {
                let replay = player.execute(&frame.iter().map(Bytes::as_ref).collect::<Vec<_>>());
                assert_eq!(replay, Ok(()), "self-generated frame: {frame:?}");
                assert_eq!(frame[9], Bytes::from_static(b"-9223372036854775808"));
            }
            let group = player
                .ctx
                .db_mut(1)
                .unwrap()
                .get_stream(b"s")
                .unwrap()
                .unwrap()
                .group(b"g")
                .unwrap();
            let pending = &group.pending[&StreamID::new(1, 0)];
            // Negative RETRYCOUNT is unspecified in C, and JUSTID preserves the replica count.
            assert_eq!(pending.delivery_count, i64::MAX as u64);
            assert_eq!(pending.consumer, Bytes::from_static(b"new"));
            assert_eq!(pending.delivery_time_ms, 100);
            assert_eq!(group.last_id, StreamID::new(1, 0));
            assert!(group.consumers[b"old".as_slice()].pending.is_empty());
            assert!(group.consumers[b"new".as_slice()]
                .pending
                .contains(&StreamID::new(1, 0)));
        }
        let pending = &primary
            .db_mut()
            .unwrap()
            .get_stream(b"s")
            .unwrap()
            .unwrap()
            .group(b"g")
            .unwrap()
            .pending[&StreamID::new(1, 0)];
        assert_eq!(pending.delivery_count, (i64::MAX as u64) + 1);
    }
}

#[test]
fn stream_spec_repairs_negative_retrycount_is_unspecified_not_unsigned() {
    for retry in [b"-1".as_slice(), b"-2", b"-9223372036854775808"] {
        for just_id in [false, true] {
            let mut runtime = pending_runtime(1);
            let mut parts = vec![
                b"XCLAIM".as_slice(),
                b"s",
                b"g",
                b"old",
                b"0",
                b"1-0",
                b"RETRYCOUNT",
                b"9",
                b"RETRYCOUNT",
                retry,
            ];
            if just_id {
                parts.push(b"JUSTID");
            }
            let outcome = xclaim(&mut runtime, &args(&parts));
            assert!(
                matches!(outcome.reply, Value::Array(_)),
                "{retry:?}: {:?}",
                outcome.reply
            );
            let pending = &runtime
                .db_mut()
                .unwrap()
                .get_stream(b"s")
                .unwrap()
                .unwrap()
                .group(b"g")
                .unwrap()
                .pending[&StreamID::new(1, 0)];
            assert_eq!(pending.delivery_count, if just_id { 1 } else { 2 });
            assert_eq!(outcome.effects.dirty_delta, Some(1));
        }
    }
}

#[test]
fn stream_spec_repairs_retrycount_rejects_c_invalid_integer_grammar() {
    for retry in [
        b"+1".as_slice(),
        b"01",
        b"-0",
        b" 1",
        b"1 ",
        b"1\0ignored",
        b"9223372036854775808",
        b"-9223372036854775809",
    ] {
        let mut runtime = pending_runtime(1);
        let before = runtime
            .db_mut()
            .unwrap()
            .get_stream(b"s")
            .unwrap()
            .unwrap()
            .group(b"g")
            .unwrap()
            .pending
            .clone();
        let outcome = xclaim(
            &mut runtime,
            &args(&[
                b"XCLAIM",
                b"s",
                b"g",
                b"new",
                b"0",
                b"1-0",
                b"RETRYCOUNT",
                retry,
            ]),
        );
        assert_eq!(
            outcome.reply,
            Value::error("ERR Invalid RETRYCOUNT option argument for XCLAIM"),
            "{retry:?}"
        );
        assert_eq!(outcome.effects.dirty_delta, Some(0));
        assert!(outcome.effects.changed_keys.is_empty());
        assert!(outcome.effects.custom_propagations.is_empty());
        let group = runtime
            .db_mut()
            .unwrap()
            .get_stream(b"s")
            .unwrap()
            .unwrap()
            .group(b"g")
            .unwrap();
        assert_eq!(group.pending, before);
        assert!(!group.consumers.contains_key(b"new".as_slice()));
    }
}

#[test]
fn stream_spec_repairs_mkstream_strict_ids_have_no_creation_effects() {
    for id in [b"+".as_slice(), b"-", b"(1-0"] {
        let mut runtime = TestCommandRuntime::new(1, 100);
        let outcome = xgroup(
            &mut runtime,
            &args(&[b"XGROUP", b"CREATE", b"s", b"g", id, b"MKSTREAM"]),
        );
        assert_eq!(
            outcome.reply,
            Value::error("ERR Invalid stream ID specified as stream command argument"),
            "{id:?}"
        );
        assert!(!runtime.db_mut().unwrap().exists_no_expire(b"s"));
        assert_eq!(outcome.effects.dirty_delta, Some(0));
        assert!(outcome.effects.skip_propagate);
        assert!(outcome.effects.changed_keys.is_empty());
        assert!(outcome.effects.events.is_empty());
        assert!(outcome.effects.custom_propagations.is_empty());
    }
}

#[test]
fn stream_spec_repairs_create_preserves_dollar_numeric_and_range_contracts() {
    use super::parse_stream_id;

    for (id, expected) in [
        (b"$".as_slice(), StreamID::zero()),
        (b"1", StreamID::new(1, 0)),
        (b"1-2", StreamID::new(1, 2)),
        (b"+01- 2", StreamID::new(1, 2)),
    ] {
        let mut runtime = TestCommandRuntime::new(1, 100);
        let outcome = xgroup(
            &mut runtime,
            &args(&[b"XGROUP", b"CREATE", b"s", b"g", id, b"MKSTREAM"]),
        );
        assert_eq!(outcome.reply, Value::ok());
        assert_eq!(
            runtime
                .db_mut()
                .unwrap()
                .get_stream(b"s")
                .unwrap()
                .unwrap()
                .group(b"g")
                .unwrap()
                .last_id,
            expected
        );
        assert_eq!(
            outcome.effects.changed_keys,
            vec![(0, Bytes::from_static(b"s"))]
        );
        assert_eq!(outcome.effects.custom_propagations.len(), 1);
        for sentinel in [b"+".as_slice(), b"-", b"(1-0"] {
            let expected = parse_stream_id(sentinel, false).unwrap();
            let outcome = xgroup(
                &mut runtime,
                &args(&[b"XGROUP", b"SETID", b"s", b"g", sentinel]),
            );
            assert_eq!(outcome.reply, Value::ok());
            assert_eq!(
                runtime
                    .db_mut()
                    .unwrap()
                    .get_stream(b"s")
                    .unwrap()
                    .unwrap()
                    .group(b"g")
                    .unwrap()
                    .last_id,
                expected
            );
        }
    }
}
