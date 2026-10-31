use super::*;

fn field(name: &'static [u8], value: &'static [u8]) -> (Bytes, Bytes) {
    (Bytes::from_static(name), Bytes::from_static(value))
}

fn append_id(stream: &mut ValkeyStream, ms: u64, seq: u64) -> StreamID {
    stream
        .append(
            StreamID::new(ms, seq),
            vec![field(b"f", b"v")],
            true,
            false,
            0,
        )
        .unwrap()
}

fn ids(entries: &[StreamEntry]) -> Vec<StreamID> {
    entries.iter().map(|entry| entry.id).collect()
}

#[test]
fn stream_id_parse_format_and_increment_edges() {
    let mut id = StreamID::parse(b"12-34").unwrap();
    assert_eq!(id, StreamID::new(12, 34));
    assert_eq!(id.fmt_id(), "12-34");
    assert_eq!(id.to_string(), "12-34");
    assert_eq!(
        StreamID::parse(b"12").unwrap_or(StreamID::zero()),
        StreamID::zero()
    );
    assert!(StreamID::parse(b"a-1").is_none());
    assert!(StreamID::parse(&[0xff, b'-', b'1']).is_none());

    assert!(id.incr());
    assert_eq!(id, StreamID::new(12, 35));
    let mut wrap = StreamID::new(7, u64::MAX);
    assert!(wrap.incr());
    assert_eq!(wrap, StreamID::new(8, 0));
    let mut exhausted = StreamID::max();
    assert!(!exhausted.incr());
    assert_eq!(
        StreamID::new(1, u64::MAX).incr_checked(),
        Some(StreamID::new(2, 0))
    );
    assert_eq!(StreamID::max().incr_checked(), None);
    assert_eq!(StreamID::new(2, 3).next(), Some(StreamID::new(2, 4)));
    assert_eq!(StreamID::max().next(), None);
}

#[test]
fn append_assigns_auto_ids_and_enforces_monotonic_order() {
    let mut empty = ValkeyStream::new();
    assert!(empty
        .append(StreamID::zero(), vec![field(b"x", b"0")], true, false, 0)
        .unwrap_err()
        .contains("greater than 0-0"));

    let mut stream = ValkeyStream::new();

    let first = stream
        .append(StreamID::zero(), vec![field(b"a", b"1")], false, false, 0)
        .unwrap();
    assert_eq!(first, StreamID::new(0, 1));
    assert_eq!(stream.recorded_first_entry_id(), first);

    let second = stream
        .append(StreamID::zero(), vec![field(b"b", b"2")], false, false, 0)
        .unwrap();
    assert_eq!(second, StreamID::new(0, 2));

    let third = stream
        .append(StreamID::new(4, 0), vec![field(b"c", b"3")], true, true, 0)
        .unwrap();
    assert_eq!(third, StreamID::new(4, 0));

    let fourth = stream
        .append(StreamID::new(4, 0), vec![field(b"d", b"4")], true, true, 0)
        .unwrap();
    assert_eq!(fourth, StreamID::new(4, 1));

    assert_eq!(stream.len(), 4);
    assert_eq!(stream.entries_added, 4);
    assert_eq!(stream.recorded_last_id(), fourth);
    assert_eq!(stream.last_entry_id(), fourth);

    assert!(stream
        .append(StreamID::zero(), vec![field(b"x", b"0")], true, false, 0)
        .unwrap_err()
        .contains("equal or smaller"));
    assert!(stream
        .append(StreamID::new(4, 1), vec![field(b"x", b"1")], true, false, 0)
        .unwrap_err()
        .contains("equal or smaller"));

    stream.last_id = StreamID::max();
    assert!(stream
        .append(StreamID::zero(), vec![field(b"x", b"2")], false, false, 0)
        .unwrap_err()
        .contains("exhausted"));
    assert!(stream
        .append(
            StreamID::new(u64::MAX, 0),
            vec![field(b"x", b"3")],
            true,
            true,
            0,
        )
        .unwrap_err()
        .contains("equal or smaller"));
}

#[test]
fn accessors_return_entries_in_stream_order() {
    let mut stream = ValkeyStream::new();
    assert!(stream.is_empty());
    assert_eq!(stream.first_id(), None);
    assert_eq!(stream.first_entry(), None);
    assert_eq!(stream.last_entry(), None);

    let a = append_id(&mut stream, 1, 0);
    let b = append_id(&mut stream, 2, 0);
    let c = append_id(&mut stream, 3, 0);

    assert!(!stream.is_empty());
    assert_eq!(stream.first_id(), Some(a));
    assert_eq!(stream.first_entry().unwrap().id, a);
    assert_eq!(stream.last_entry().unwrap().id, c);
    assert_eq!(ids(&stream.last_n_entries(2)), vec![b, c]);
    assert_eq!(stream.get_entry(&b).unwrap(), &vec![field(b"f", b"v")]);
    assert_eq!(
        stream.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        vec![a, b, c]
    );
    assert!(stream.memory_usage() >= std::mem::size_of_val(&stream));
}

#[test]
fn delete_and_delete_multiple_update_tombstone_and_recorded_first_id() {
    let mut stream = ValkeyStream::new();
    let a = append_id(&mut stream, 1, 0);
    let b = append_id(&mut stream, 2, 0);
    let c = append_id(&mut stream, 3, 0);

    assert!(!stream.delete(&StreamID::new(9, 9)));
    assert!(stream.delete(&a));
    assert_eq!(stream.max_deleted_entry_id, a);
    assert_eq!(stream.recorded_first_entry_id(), b);

    assert!(stream.delete(&c));
    assert_eq!(stream.max_deleted_entry_id, c);
    assert_eq!(stream.recorded_first_entry_id(), b);

    assert_eq!(stream.delete_multiple(&[a, b, c]), 1);
    assert_eq!(stream.recorded_first_entry_id(), StreamID::zero());
    assert_eq!(stream.len(), 0);
}

#[test]
fn range_honors_bounds_count_direction_and_exclusive_start() {
    let mut stream = ValkeyStream::new();
    for ms in 1..=5 {
        append_id(&mut stream, ms, 0);
    }

    let lower = StreamID::new(2, 0);
    let upper = StreamID::new(5, 0);
    assert_eq!(
        ids(&stream.range(&lower, &upper, false, false, Some(3), false)),
        vec![
            StreamID::new(2, 0),
            StreamID::new(3, 0),
            StreamID::new(4, 0)
        ]
    );
    assert_eq!(
        ids(&stream.range(&lower, &upper, true, true, None, false)),
        vec![StreamID::new(3, 0), StreamID::new(4, 0)]
    );
    assert_eq!(
        ids(&stream.range(&lower, &upper, false, false, Some(2), true)),
        vec![StreamID::new(5, 0), StreamID::new(4, 0)]
    );
    assert_eq!(
        ids(&stream.range(&lower, &upper, true, true, None, true)),
        vec![StreamID::new(4, 0), StreamID::new(3, 0)]
    );
    assert_eq!(
        ids(&stream.range_from_start(&StreamID::new(3, 0), 2)),
        vec![StreamID::new(4, 0), StreamID::new(5, 0)]
    );
}

#[test]
fn trim_by_length_and_id_support_exact_and_approximate_modes() {
    let mut stream = ValkeyStream::new();
    assert_eq!(stream.trim_by_id(&StreamID::new(10, 0), false), 0);

    for ms in 1..=105 {
        append_id(&mut stream, ms, 0);
    }

    assert_eq!(stream.trim_by_length(104, true), 0);
    assert_eq!(stream.len(), 105);
    assert_eq!(stream.trim_by_length(5, true), 100);
    assert_eq!(stream.first_id(), Some(StreamID::new(101, 0)));
    assert_eq!(stream.max_deleted_entry_id, StreamID::zero());
    assert_eq!(stream.trim_by_length(5, false), 0);

    let mut stream = ValkeyStream::new();
    for ms in 1..=105 {
        append_id(&mut stream, ms, 0);
    }
    assert_eq!(stream.trim_by_id(&StreamID::new(50, 0), true), 0);
    assert_eq!(stream.first_id(), Some(StreamID::new(1, 0)));
    assert_eq!(stream.trim_by_id(&StreamID::new(102, 0), true), 100);
    assert_eq!(stream.first_id(), Some(StreamID::new(101, 0)));

    assert_eq!(stream.trim_by_id(&StreamID::new(104, 0), false), 3);
    assert_eq!(stream.first_id(), Some(StreamID::new(104, 0)));
}

#[test]
fn trim_preserves_explicit_delete_metadata_and_zero_limit_is_unbounded() {
    let mut stream = ValkeyStream::new();
    for ms in 1..=105 {
        append_id(&mut stream, ms, 0);
    }
    assert!(stream.delete(&StreamID::new(3, 0)));
    assert_eq!(stream.trim_by_id(&StreamID::new(10, 0), false), 8);
    assert_eq!(stream.max_deleted_entry_id, StreamID::new(3, 0));
    assert_eq!(stream.trim_by_length_limit(0, true, Some(0), 100), 96);
    assert_eq!(stream.max_deleted_entry_id, StreamID::new(3, 0));
    assert_eq!(stream.recorded_first_entry_id(), StreamID::zero());
    assert_eq!(stream.entries_added, 105);
    assert_eq!(stream.last_id, StreamID::new(105, 0));
}

#[test]
fn approximate_trim_honors_stream_node_byte_boundaries() {
    let mut byte_bounded = ValkeyStream::new();
    byte_bounded.set_append_node_limits(100, 1);
    let mut unbounded = ValkeyStream::new();
    for ms in 1..=4 {
        let fields = vec![(Bytes::from_static(b"field"), Bytes::from_static(b"value"))];
        byte_bounded
            .append(StreamID::new(ms, 0), fields.clone(), true, false, 0)
            .unwrap();
        unbounded
            .append(StreamID::new(ms, 0), fields, true, false, 0)
            .unwrap();
    }

    assert_eq!(
        byte_bounded.trim_by_length_limit_with_node_limits(1, true, None, 100, 1),
        3
    );
    assert_eq!(
        unbounded.trim_by_length_limit_with_node_limits(1, true, None, 100, 0),
        0
    );
}

#[test]
fn approximate_minid_trim_stops_at_node_boundary() {
    let mut stream = ValkeyStream::new();
    for ms in 1..=101 {
        append_id(&mut stream, ms, 0);
    }

    assert_eq!(
        stream.trim_by_id_limit(&StreamID::new(100, 0), true, None, 100),
        0
    );
    assert_eq!(stream.len(), 101);
}

#[test]
fn consumer_group_tracks_new_reads_history_and_ack() {
    let mut stream = ValkeyStream::new();
    append_id(&mut stream, 1, 0);
    append_id(&mut stream, 2, 0);
    append_id(&mut stream, 3, 0);
    stream
        .create_group(Bytes::from_static(b"g"), StreamID::zero(), None)
        .unwrap();

    let first = stream.read_group_new(b"g", b"alice", 2, false, 10).unwrap();
    assert_eq!(
        ids(&first.entries),
        vec![StreamID::new(1, 0), StreamID::new(2, 0)]
    );
    assert_eq!(stream.pending_summary(b"g").unwrap().total, 2);

    let history = stream
        .read_group_history(b"g", b"alice", StreamID::zero(), 10, 20)
        .unwrap();
    assert_eq!(
        history
            .entries
            .iter()
            .map(|(id, _)| *id)
            .collect::<Vec<_>>(),
        vec![StreamID::new(1, 0), StreamID::new(2, 0)]
    );

    assert_eq!(
        stream
            .ack_group_pending(b"g", &[StreamID::new(1, 0), StreamID::new(9, 0)])
            .unwrap(),
        1
    );
    assert_eq!(stream.pending_summary(b"g").unwrap().total, 1);

    let second = stream.read_group_new(b"g", b"bob", 10, true, 30).unwrap();
    assert_eq!(ids(&second.entries), vec![StreamID::new(3, 0)]);
    assert_eq!(stream.pending_summary(b"g").unwrap().total, 1);
}

#[test]
fn consumer_group_entries_read_tracks_counted_reads() {
    let mut stream = ValkeyStream::new();
    for ms in 1..=5 {
        append_id(&mut stream, ms, 0);
    }
    stream
        .create_group(Bytes::from_static(b"g"), StreamID::zero(), None)
        .unwrap();

    stream.read_group_new(b"g", b"c", 1, false, 10).unwrap();
    assert_eq!(stream.group(b"g").unwrap().entries_read, Some(1));

    stream.read_group_new(b"g", b"c", 10, false, 20).unwrap();
    assert_eq!(stream.group(b"g").unwrap().entries_read, Some(5));
}
