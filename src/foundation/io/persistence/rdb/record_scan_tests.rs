use super::*;
use crate::foundation::io::persistence::codec::types::save_len;

fn record(type_byte: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![type_byte, 1, b'k'];
    out.extend_from_slice(body);
    out
}

#[test]
fn pending_large_string_keeps_progress_and_known_end() {
    let mut body = vec![2, 1, b'a'];
    save_len(&mut body, 1024 * 1024);
    let prefix = record(RDB_TYPE_SET, &body);
    let mut scan = RecordScan::new(RDB_TYPE_SET).unwrap();
    assert_eq!(scan.scan(&prefix), ScanOutcome::NeedInput);
    assert_eq!(scan.remaining, 1);
    let offset = scan.offset;
    let end = scan.pending_end.unwrap();
    assert_eq!(end, prefix.len() + 1024 * 1024);
    for _ in 0..4 {
        assert_eq!(scan.scan(&prefix), ScanOutcome::NeedInput);
        assert_eq!(scan.offset, offset);
        assert_eq!(scan.pending_end, Some(end));
        assert_eq!(scan.remaining, 1);
    }
    let mut complete = prefix;
    complete.resize(end, b'x');
    assert_eq!(scan.scan(&complete), ScanOutcome::Complete);
    assert_eq!(scan.offset, end);
    assert_eq!(scan.remaining, 0);
    assert_eq!(scan.pending_end, None);
}

#[test]
fn invalid_lengths_and_counts_fall_back_without_allocating_members() {
    let mut huge_count = Vec::new();
    save_len(&mut huge_count, u64::MAX);
    let mut huge_string = vec![1];
    save_len(&mut huge_string, u64::MAX);
    let cases = [
        (RDB_TYPE_SET, vec![0xc0]),
        (RDB_TYPE_SET, vec![0x82]),
        (RDB_TYPE_HASH, huge_count.clone()),
        (RDB_TYPE_ZSET_2, huge_count),
        (RDB_TYPE_SET, huge_string),
        (RDB_TYPE_SET, vec![1, 0xc4]),
        (RDB_TYPE_SET, vec![1, 0xc3, 0xc0]),
        (RDB_TYPE_SET, vec![1, 0xc3, 1, 0xc0]),
        (RDB_TYPE_LIST_QUICKLIST_2, vec![1, 0xc0]),
        (RDB_TYPE_LIST_QUICKLIST_2, vec![1, 3]),
        (RDB_TYPE_LIST_QUICKLIST_2, vec![1, 1, 0]),
        (RDB_TYPE_LIST_QUICKLIST, vec![1, 0xc0]),
    ];
    for (type_byte, body) in cases {
        let mut scan = RecordScan::new(type_byte).unwrap();
        assert_eq!(scan.scan(&record(type_byte, &body)), ScanOutcome::Fallback);
    }
}

#[test]
fn truncated_length_headers_need_input_without_progress_loss() {
    for body in [vec![0x40], vec![0x80, 0, 0], vec![0x81, 0, 0, 0]] {
        let mut scan = RecordScan::new(RDB_TYPE_SET).unwrap();
        let bytes = record(RDB_TYPE_SET, &body);
        assert_eq!(scan.scan(&bytes), ScanOutcome::NeedInput);
        assert_eq!(scan.phase, Phase::Count);
        assert_eq!(scan.offset, 3);
    }
    let mut scan = RecordScan::new(RDB_TYPE_SET).unwrap();
    let prefix = record(RDB_TYPE_SET, &[1, 0xc3, 5]);
    assert_eq!(scan.scan(&prefix), ScanOutcome::NeedInput);
    assert_eq!(scan.phase, Phase::Member);
    let offset = scan.offset;
    assert_eq!(scan.scan(&prefix), ScanOutcome::NeedInput);
    assert_eq!(scan.offset, offset);
    let mut full = prefix;
    full.extend_from_slice(&[4, 3, b'a', b'b', b'c', b'd']);
    assert_eq!(scan.scan(&full), ScanOutcome::Complete);
    assert_eq!(scan.offset, full.len());
}

#[test]
fn scanner_does_not_validate_contents_or_guess_other_layouts() {
    let bytes = record(RDB_TYPE_ZSET, &[1, 1, b'a', 4, b'b', b'a', b'd', b'!']);
    let mut scan = RecordScan::new(RDB_TYPE_ZSET).unwrap();
    assert_eq!(scan.scan(&bytes), ScanOutcome::Complete);
    assert_eq!(scan.offset, bytes.len());
    for type_byte in [0, 9, 10, 11, 12, 13, 15, 16, 17, 19, 20, 21, 22, 23, 99] {
        assert!(RecordScan::new(type_byte).is_none());
    }
}
