//! Historical counters and both XINFO representations follow the pinned stream contract.

use super::{opt_u64, xinfo_groups_reply, xinfo_stream_full_reply};
use crate::engine::core::Value;
use crate::foundation::io::store::structure::stream::{StreamID, ValkeyStream};
use bytes::Bytes;

fn populated_stream(count: u64, last_id: StreamID, entries_read: Option<u64>) -> ValkeyStream {
    let mut stream = ValkeyStream::new();
    for ms in 1..=count {
        stream
            .append(StreamID::new(ms, 0), vec![], true, false, 0)
            .unwrap();
    }
    stream
        .create_group(Bytes::from_static(b"g"), last_id, entries_read)
        .unwrap();
    stream
}

fn info_field<'a>(reply: &'a Value, name: &[u8]) -> &'a Value {
    let Value::Array(fields) = reply else {
        panic!("Expected an information array");
    };
    let key = Value::bulk(Bytes::copy_from_slice(name));
    fields
        .as_chunks::<2>()
        .0
        .iter()
        .find(|pair| pair[0] == key)
        .map(|pair| &pair[1])
        .expect("Expected an information field")
}

fn assert_group_info(stream: &ValkeyStream, id: u64, read: Option<u64>, lag: Option<i64>) {
    let before = stream.clone();
    let ordinary = xinfo_groups_reply(stream);
    let full = xinfo_stream_full_reply(stream, None);
    for reply in [&ordinary, info_field(&full, b"groups")] {
        let Value::Array(groups) = reply else {
            panic!("Expected a groups array");
        };
        assert_eq!(groups.len(), 1);
        assert_eq!(
            info_field(&groups[0], b"last-delivered-id"),
            &Value::bulk(Bytes::from(StreamID::new(id, 0).fmt_id()))
        );
        assert_eq!(info_field(&groups[0], b"entries-read"), &opt_u64(read));
        assert_eq!(
            info_field(&groups[0], b"lag"),
            &lag.map(Value::integer).unwrap_or_else(Value::null)
        );
    }
    assert_eq!(stream, &before);
}

fn read_entries(
    stream: &mut ValkeyStream,
    count: usize,
    noack: bool,
    ids: &[u64],
    read: Option<u64>,
) {
    let result = stream.read_group_new(b"g", b"c", count, noack, 7).unwrap();
    assert_eq!(
        result
            .entries
            .iter()
            .map(|entry| entry.id)
            .collect::<Vec<_>>(),
        ids.iter()
            .map(|ms| StreamID::new(*ms, 0))
            .collect::<Vec<_>>()
    );
    assert_eq!(result.entries_read, read);
}

#[test]
fn trimmed_unread_entries_have_live_length_lag_and_preserve_counters() {
    let mut stream = populated_stream(6, StreamID::zero(), None);
    read_entries(&mut stream, 1, false, &[1], Some(1));
    assert_eq!(stream.trim_by_length(2, false), 4);
    assert_group_info(&stream, 1, Some(1), Some(2));
    read_entries(&mut stream, 2, false, &[5, 6], Some(6));
    assert_group_info(&stream, 6, Some(6), Some(0));
    stream
        .set_group_id(b"g", StreamID::new(1, 0), Some(1))
        .unwrap();
    assert_eq!(stream.trim_by_length(0, false), 2);
    assert_eq!(stream.entries_added, 6);
    assert_group_info(&stream, 1, Some(1), Some(0));
}

#[test]
fn partial_read_after_trim_restores_historical_distance() {
    for noack in [false, true] {
        let mut stream = populated_stream(6, StreamID::zero(), None);
        read_entries(&mut stream, 1, noack, &[1], Some(1));
        assert_eq!(stream.trim_by_length(2, false), 4);
        read_entries(&mut stream, 1, noack, &[5], Some(5));
        assert_group_info(&stream, 5, Some(5), Some(1));
    }
}

#[test]
fn batch_restores_at_first_live_entry_then_increments() {
    for noack in [false, true] {
        let mut stream = populated_stream(6, StreamID::zero(), None);
        read_entries(&mut stream, 1, noack, &[1], Some(1));
        assert_eq!(stream.trim_by_length(3, false), 3);
        read_entries(&mut stream, 2, noack, &[4, 5], Some(5));
        assert_group_info(&stream, 5, Some(5), Some(1));
    }
}

#[test]
fn old_deletion_does_not_invalidate_next_read() {
    for noack in [false, true] {
        let mut stream = populated_stream(6, StreamID::zero(), None);
        read_entries(&mut stream, 4, noack, &[1, 2, 3, 4], Some(4));
        assert!(stream.delete(&StreamID::new(2, 0)));
        read_entries(&mut stream, 1, noack, &[5], Some(5));
        assert_group_info(&stream, 5, Some(5), Some(1));
    }
}

#[test]
fn nonzero_cursor_before_trimmed_tombstone_has_unknown_lag() {
    let mut stream = populated_stream(6, StreamID::zero(), None);
    read_entries(&mut stream, 1, false, &[1], Some(1));
    assert!(stream.delete(&StreamID::new(3, 0)));
    assert_eq!(stream.trim_by_id(&StreamID::new(5, 0), false), 3);
    assert_group_info(&stream, 1, Some(1), None);
    read_entries(&mut stream, 1, false, &[5], Some(5));
    assert_group_info(&stream, 5, Some(5), Some(1));
}

#[test]
fn unknown_interior_cursor_is_not_assumed_to_have_full_length_lag() {
    let mut stream = populated_stream(6, StreamID::new(3, 0), None);
    assert_group_info(&stream, 3, None, None);
    read_entries(&mut stream, 1, false, &[4], None);
    read_entries(&mut stream, 2, false, &[5, 6], Some(6));
    assert_group_info(&stream, 6, Some(6), Some(0));
}

#[test]
fn tombstone_at_cursor_invalidates_lag_and_interior_read() {
    for noack in [false, true] {
        let mut stream = populated_stream(6, StreamID::zero(), None);
        read_entries(&mut stream, 3, noack, &[1, 2, 3], Some(3));
        assert!(stream.delete(&StreamID::new(3, 0)));
        assert!(stream.range_has_tombstones(Some(StreamID::new(3, 0)), Some(StreamID::new(3, 0))));
        assert!(!stream.range_has_tombstones(Some(StreamID::new(3, 1)), None));
        assert!(!stream.range_has_tombstones(None, Some(StreamID::new(2, u64::MAX))));
        assert_group_info(&stream, 3, Some(3), None);
        read_entries(&mut stream, 1, noack, &[4], None);
        read_entries(&mut stream, 2, noack, &[5, 6], Some(6));
        assert_group_info(&stream, 6, Some(6), Some(0));
    }
}

#[test]
fn zero_cursor_empty_history_and_future_cursor_follow_estimation_order() {
    let mut stream = populated_stream(6, StreamID::zero(), None);
    assert!(stream.delete(&StreamID::new(3, 0)));
    assert_eq!(stream.trim_by_id(&StreamID::new(5, 0), false), 3);
    assert_group_info(&stream, 0, None, Some(2));
    assert_eq!(
        stream.estimate_distance_from_first_ever_entry(StreamID::new(1, 0)),
        None
    );
    assert_eq!(
        stream.estimate_distance_from_first_ever_entry(StreamID::new(3, 0)),
        Some(4)
    );
    assert!(stream.delete(&StreamID::new(6, 0)));
    assert_eq!(stream.last_entry().unwrap().id, StreamID::new(5, 0));
    assert_eq!(stream.last_entry_id(), StreamID::new(6, 0));
    assert_eq!(
        stream.estimate_distance_from_first_ever_entry(StreamID::new(5, 0)),
        None
    );
    assert_eq!(stream.trim_by_length(0, false), 1);
    assert_group_info(&stream, 0, None, Some(0));
    stream
        .set_group_id(b"g", StreamID::new(7, 0), None)
        .unwrap();
    assert_group_info(&stream, 7, None, None);
    stream
        .set_group_id(b"g", StreamID::new(7, 0), Some(1))
        .unwrap();
    assert_group_info(&stream, 7, Some(1), Some(0));
}

#[test]
fn explicit_counters_increment_and_signed_lag_is_preserved() {
    let stream = populated_stream(0, StreamID::new(7, 0), Some(10));
    assert_group_info(&stream, 7, Some(10), Some(0));
    let mut stream = populated_stream(6, StreamID::new(1, 0), Some(10));
    assert_group_info(&stream, 1, Some(10), Some(-4));
    read_entries(&mut stream, 2, false, &[2, 3], Some(12));
    assert_group_info(&stream, 3, Some(12), Some(-6));
    let before = stream.group(b"g").unwrap().clone();
    stream
        .read_group_history(b"g", b"c", StreamID::zero(), 1, 9)
        .unwrap();
    assert_eq!(stream.group(b"g").unwrap().last_id, before.last_id);
    assert_eq!(
        stream.group(b"g").unwrap().entries_read,
        before.entries_read
    );
}

#[test]
fn raw_unknown_sentinel_is_normalized_for_both_info_replies() {
    let mut stream = populated_stream(6, StreamID::new(3, 0), Some(u64::MAX));
    assert_group_info(&stream, 3, None, None);
    read_entries(&mut stream, 1, false, &[4], None);
    read_entries(&mut stream, 2, false, &[5, 6], Some(6));
    assert_group_info(&stream, 6, Some(6), Some(0));
    stream
        .set_group_id(b"g", StreamID::new(3, 0), Some(u64::MAX))
        .unwrap();
    assert_group_info(&stream, 3, None, None);
}

#[test]
fn inconsistent_loaded_history_does_not_panic_or_make_sentinel_valid() {
    let mut stream = populated_stream(3, StreamID::zero(), None);
    stream.entries_added = 1;
    assert_group_info(&stream, 0, None, Some(3));
    read_entries(&mut stream, 1, false, &[1], None);
    assert_group_info(&stream, 1, None, None);
    read_entries(&mut stream, 2, false, &[2, 3], Some(1));
    assert_group_info(&stream, 3, Some(1), Some(0));
}
