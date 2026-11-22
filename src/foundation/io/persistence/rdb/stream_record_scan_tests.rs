use super::*;
use crate::foundation::io::persistence::codec::types::{
    save_len, RDB_OPCODE_EOF, RDB_TYPE_HASH, RDB_TYPE_LIST, RDB_TYPE_SET, RDB_TYPE_ZSET,
    RDB_TYPE_ZSET_2,
};

fn append_raw(out: &mut Vec<u8>, value: &[u8]) {
    save_len(out, value.len() as u64);
    out.extend_from_slice(value);
}

fn collection_record(type_byte: u8, key: &[u8], count: usize) -> Vec<u8> {
    let mut out = vec![type_byte];
    append_raw(&mut out, key);
    save_len(&mut out, count as u64);
    for index in 0..count {
        append_raw(
            &mut out,
            format!("member-{index:04}-{}", "x".repeat(80)).as_bytes(),
        );
        match type_byte {
            RDB_TYPE_HASH => append_raw(&mut out, b"value"),
            RDB_TYPE_ZSET => out.extend_from_slice(b"\x041.25"),
            RDB_TYPE_ZSET_2 => out.extend_from_slice(&1.25f64.to_le_bytes()),
            _ => {}
        }
    }
    out
}

fn snapshot(records: &[u8]) -> Vec<u8> {
    let mut out = b"VALKEY081".to_vec();
    out.extend_from_slice(records);
    out.push(RDB_OPCODE_EOF);
    let checksum = crc64(0, &out);
    out.extend_from_slice(&checksum.to_le_bytes());
    out
}

#[test]
fn fragmented_collections_do_not_invoke_loader_before_record_completion() {
    for type_byte in [
        RDB_TYPE_SET,
        RDB_TYPE_LIST,
        RDB_TYPE_HASH,
        RDB_TYPE_ZSET,
        RDB_TYPE_ZSET_2,
    ] {
        let record = collection_record(type_byte, b"collection", 24);
        for chunk_size in [1, 128] {
            let mut decoder = RdbDecoder::new(true);
            let mut destination = DbSet::new(1);
            decoder.feed(b"VALKEY081");
            assert_eq!(decoder.step(&mut destination).unwrap(), RdbStep::NeedInput);
            let header_crc = decoder.crc;
            let partial = &record[..record.len() - 1];
            for chunk in partial.chunks(chunk_size) {
                decoder.feed(chunk);
                assert_eq!(decoder.step(&mut destination).unwrap(), RdbStep::NeedInput);
                assert_eq!(decoder.loaded(), 0);
                assert_eq!(destination.get(0).unwrap().dbsize(), 0);
                assert!(destination
                    .get(0)
                    .unwrap()
                    .get_allow_expired(b"collection")
                    .is_none());
                assert_eq!(decoder.consumed_bytes(), 9);
                assert_eq!(decoder.crc, header_crc);
                assert_eq!(
                    decoder.object_loader_invocations, 0,
                    "incomplete collection type {type_byte} must not invoke the object loader"
                );
            }
            decoder.feed(&record[record.len() - 1..]);
            assert_eq!(
                decoder.step(&mut destination).unwrap(),
                RdbStep::RecordApplied
            );
            assert_eq!(decoder.object_loader_invocations, 1);
            assert_eq!(decoder.loaded(), 1);
            assert_eq!(destination.get(0).unwrap().dbsize(), 1);
            assert_eq!(decoder.consumed_bytes(), 9 + record.len());
            assert_eq!(decoder.crc, crc64(header_crc, &record));
            let full = snapshot(&record);
            decoder.feed(&full[full.len() - 9..]);
            assert!(matches!(
                decoder.step(&mut destination).unwrap(),
                RdbStep::Finished(_)
            ));
        }
    }
}

fn assert_fragmented_record(record: &[u8], key: &[u8], expected_len: usize) {
    for chunk_size in [1, 37, 128 * 1024] {
        let mut decoder = RdbDecoder::new(true);
        let mut destination = DbSet::new(1);
        decoder.feed(b"VALKEY081");
        assert_eq!(decoder.step(&mut destination).unwrap(), RdbStep::NeedInput);
        for chunk in record[..record.len() - 1].chunks(chunk_size) {
            decoder.feed(chunk);
            assert_eq!(decoder.step(&mut destination).unwrap(), RdbStep::NeedInput);
            assert_eq!(decoder.object_loader_invocations, 0);
            assert_eq!(decoder.consumed_bytes(), 9);
            assert_eq!(destination.get(0).unwrap().dbsize(), 0);
        }
        decoder.feed(&record[record.len() - 1..]);
        assert_eq!(
            decoder.step(&mut destination).unwrap(),
            RdbStep::RecordApplied
        );
        assert_eq!(decoder.object_loader_invocations, 1);
        assert!(decoder.record_scan.is_none());
        let value = destination.get(0).unwrap().get_allow_expired(key).unwrap();
        let actual_len = match record[0] {
            RDB_TYPE_SET => value.set_len(),
            RDB_TYPE_HASH => value.hash_len(),
            RDB_TYPE_ZSET | RDB_TYPE_ZSET_2 => value.zset_len(),
            _ => value.list_len(),
        };
        assert_eq!(actual_len, expected_len);
    }
}

#[test]
fn encoded_keys_and_members_wait_for_complete_integer_and_lzf_spans() {
    let key_encodings = [
        (vec![0xc0, 0xff], b"-1".to_vec()),
        (vec![0xc1, 0x2e, 0xfb], b"-1234".to_vec()),
        (vec![0xc2, 0x40, 0xe2, 1, 0], b"123456".to_vec()),
        (vec![0xc3, 4, 3, 2, b'k', b'e', b'y'], b"key".to_vec()),
    ];
    for (key_encoding, key) in key_encodings {
        for type_byte in [
            RDB_TYPE_LIST,
            RDB_TYPE_SET,
            RDB_TYPE_HASH,
            RDB_TYPE_ZSET,
            RDB_TYPE_ZSET_2,
        ] {
            let mut record = vec![type_byte];
            record.extend_from_slice(&key_encoding);
            record.push(4);
            for member in [
                vec![0xc0, 0xff],
                vec![0xc1, 0x2e, 0xfb],
                vec![0xc2, 0x40, 0xe2, 1, 0],
                vec![0xc3, 5, 4, 3, b'a', b'b', b'c', b'd'],
            ] {
                record.extend_from_slice(&member);
                match type_byte {
                    RDB_TYPE_HASH => record.extend_from_slice(&[0xc0, 7]),
                    RDB_TYPE_ZSET => record.extend_from_slice(b"\x031.5"),
                    RDB_TYPE_ZSET_2 => record.extend_from_slice(&1.5f64.to_le_bytes()),
                    _ => {}
                }
            }
            assert_fragmented_record(&record, &key, 4);
        }
    }
}

fn quicklist_record(v2: bool, container: u8) -> Vec<u8> {
    use crate::foundation::io::persistence::codec::decoder_compact::listpack_encode;
    use crate::foundation::io::persistence::codec::types::{
        RDB_TYPE_LIST_QUICKLIST, RDB_TYPE_LIST_QUICKLIST_2,
    };
    let mut out = vec![if v2 {
        RDB_TYPE_LIST_QUICKLIST_2
    } else {
        RDB_TYPE_LIST_QUICKLIST
    }];
    append_raw(&mut out, b"quicklist");
    out.push(2);
    for node in 0..2 {
        if v2 {
            out.push(container);
        }
        let bytes = if v2 && container == 1 {
            format!("plain-node-{node}").into_bytes()
        } else if v2 {
            listpack_encode(&[b"x".to_vec(), b"y".to_vec()])
        } else {
            // Canonical ziplist containing "x" and "y": header, entries, EOF.
            vec![17, 0, 0, 0, 13, 0, 0, 0, 2, 0, 0, 1, b'x', 3, 1, b'y', 255]
        };
        append_raw(&mut out, &bytes);
    }
    out
}

#[test]
fn quicklist_versions_wait_for_nodes_without_decoding_containers() {
    for (v2, container, len) in [(false, 2, 4), (true, 2, 4), (true, 1, 2)] {
        assert_fragmented_record(&quicklist_record(v2, container), b"quicklist", len);
    }
}

#[test]
fn complete_spans_rejected_by_loader_leave_crc_cursor_and_database_unchanged() {
    let mut cases = Vec::new();
    for type_byte in [RDB_TYPE_SET, RDB_TYPE_HASH, RDB_TYPE_ZSET, RDB_TYPE_ZSET_2] {
        let mut record = vec![type_byte, 1, b'k', 2];
        for _ in 0..2 {
            append_raw(&mut record, b"duplicate");
            match type_byte {
                RDB_TYPE_HASH => append_raw(&mut record, b"value"),
                RDB_TYPE_ZSET => record.extend_from_slice(b"\x031.5"),
                RDB_TYPE_ZSET_2 => record.extend_from_slice(&1.5f64.to_le_bytes()),
                _ => {}
            }
        }
        cases.push(record);
    }
    cases.push(vec![RDB_TYPE_LIST, 1, b'k', 1, 0xc3, 1, 5, 0xff]);
    cases.push(vec![
        RDB_TYPE_ZSET,
        1,
        b'k',
        1,
        1,
        b'a',
        4,
        b'b',
        b'a',
        b'd',
        b'!',
    ]);
    cases.push(vec![RDB_TYPE_ZSET, 1, b'k', 1, 1, b'a', 253]);
    let mut nan = vec![RDB_TYPE_ZSET_2, 1, b'k', 1, 1, b'a'];
    nan.extend_from_slice(&f64::NAN.to_le_bytes());
    cases.push(nan);
    for record in cases {
        let mut decoder = RdbDecoder::new(true);
        let mut destination = DbSet::new(1);
        decoder.feed(b"VALKEY081");
        assert_eq!(decoder.step(&mut destination).unwrap(), RdbStep::NeedInput);
        let crc = decoder.crc;
        decoder.feed(&record);
        for invocation in 1..=2 {
            assert_eq!(decoder.step(&mut destination).unwrap(), RdbStep::NeedInput);
            assert_eq!(decoder.object_loader_invocations, invocation);
            assert!(decoder.record_scan.is_none());
            assert_eq!(decoder.crc, crc);
            assert_eq!(decoder.consumed_bytes(), 9);
            assert_eq!(decoder.loaded(), 0);
            assert_eq!(destination.get(0).unwrap().dbsize(), 0);
        }
    }
}

#[test]
fn layout_fallback_clears_progress_and_retains_loader_behavior() {
    use crate::foundation::io::persistence::codec::types::{
        RDB_TYPE_LIST_QUICKLIST, RDB_TYPE_LIST_QUICKLIST_2,
    };
    for record in [
        vec![RDB_TYPE_SET, 1, b'k', 0xc0],
        vec![RDB_TYPE_HASH, 1, b'k', 0x82],
        vec![RDB_TYPE_SET, 1, b'k', 1, 0xc4],
        vec![RDB_TYPE_LIST_QUICKLIST, 1, b'k', 1, 0xc0],
        vec![RDB_TYPE_LIST_QUICKLIST_2, 1, b'k', 1, 1, 0],
        vec![RDB_TYPE_LIST_QUICKLIST_2, 1, b'k', 1, 0xc0],
    ] {
        let mut decoder = RdbDecoder::new(true);
        let mut destination = DbSet::new(1);
        decoder.feed(&snapshot(&record));
        assert_eq!(decoder.step(&mut destination).unwrap(), RdbStep::NeedInput);
        assert_eq!(decoder.object_loader_invocations, 1);
        assert!(decoder.record_scan.is_none());
        assert_eq!(decoder.consumed_bytes(), 9);
        assert_eq!(decoder.loaded(), 0);
        assert_eq!(destination.get(0).unwrap().dbsize(), 0);
    }
    // The original codec treats an unknown container as packed. Do not guess
    // that layout or invent an error; fall back and let the codec own it.
    let mut records = quicklist_record(true, 3);
    records.extend_from_slice(&collection_record(RDB_TYPE_SET, b"next", 2));
    let mut decoder = RdbDecoder::new(true);
    let mut destination = DbSet::new(1);
    decoder.feed(&snapshot(&records));
    assert_eq!(
        decoder.drive(&mut destination, 0).unwrap(),
        StreamStep::Finished
    );
    assert!(decoder.record_scan.is_none());
    assert_eq!(decoder.object_loader_invocations, 2);
    assert_eq!(
        destination
            .get(0)
            .unwrap()
            .get_allow_expired(b"quicklist")
            .unwrap()
            .list_len(),
        4
    );
    assert_eq!(
        destination
            .get(0)
            .unwrap()
            .get_allow_expired(b"next")
            .unwrap()
            .set_len(),
        2
    );
}

#[test]
fn records_metadata_inline_commands_and_compaction_do_not_share_scan_progress() {
    use crate::foundation::io::persistence::codec::types::{
        RDB_OPCODE_AUX, RDB_OPCODE_EXPIRETIME_MS, RDB_OPCODE_FREQ, RDB_OPCODE_FUNCTION2,
        RDB_OPCODE_IDLE, RDB_OPCODE_RESIZEDB, RDB_OPCODE_SELECTDB, RDB_OPCODE_UPDATE,
        RDB_TYPE_STRING,
    };
    let first = collection_record(RDB_TYPE_SET, b"first", 4);
    let second = collection_record(RDB_TYPE_HASH, b"second", 4);
    let mut records = first.clone();
    records.push(RDB_OPCODE_SELECTDB);
    records.push(1);
    records.push(RDB_OPCODE_AUX);
    append_raw(&mut records, b"custom");
    append_raw(&mut records, b"metadata");
    records.extend_from_slice(&[
        RDB_OPCODE_RESIZEDB,
        2,
        0,
        RDB_OPCODE_IDLE,
        0,
        RDB_OPCODE_FREQ,
        5,
    ]);
    records.push(RDB_OPCODE_FUNCTION2);
    append_raw(&mut records, b"library-code");
    records.push(RDB_OPCODE_EXPIRETIME_MS);
    records.extend_from_slice(&(-1i64).to_le_bytes());
    let second_start = 9 + records.len();
    records.extend_from_slice(&second);
    records.push(RDB_OPCODE_UPDATE);
    records.extend_from_slice(b"*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n");
    records.push(RDB_TYPE_STRING);
    append_raw(&mut records, b"plain");
    append_raw(&mut records, b"value");
    records.extend_from_slice(&collection_record(RDB_TYPE_LIST, b"last", 3));
    let bytes = snapshot(&records);
    let split = second_start + second.len() / 2;
    let mut decoder = RdbDecoder::new(true);
    let mut destination = DbSet::new(2);
    decoder.feed(&bytes[..split]);
    assert_eq!(
        decoder.drive(&mut destination, 0).unwrap(),
        StreamStep::NeedMore
    );
    assert_eq!(decoder.loaded(), 1);
    assert_eq!(decoder.object_loader_invocations, 1);
    assert_eq!(decoder.consumed_bytes(), second_start);
    assert!(decoder.record_scan.is_some());
    assert!(decoder.consumed_prefix_len() > 0);
    decoder.feed(&bytes[split..]);
    assert_eq!(decoder.consumed_prefix_len(), 0);
    assert_eq!(
        decoder.drive(&mut destination, 0).unwrap(),
        StreamStep::InlineCommand {
            db: 1,
            args: vec![Bytes::from_static(b"PING"), Bytes::from_static(b"hello")]
        }
    );
    assert!(decoder.record_scan.is_none());
    assert_eq!(decoder.loaded(), 2);
    assert_eq!(
        decoder.drive(&mut destination, 0).unwrap(),
        StreamStep::Finished
    );
    assert!(decoder.record_scan.is_none());
    assert_eq!(decoder.loaded(), 4);
    assert_eq!(decoder.object_loader_invocations, 4);
    assert_eq!(decoder.consumed_bytes(), bytes.len());
    assert_eq!(decoder.function_codes(), &[b"library-code".to_vec()]);
    assert_eq!(
        decoder.take_aux_fields(),
        vec![(b"custom".to_vec(), b"metadata".to_vec())]
    );
    assert_eq!(destination.get(0).unwrap().dbsize(), 1);
    assert_eq!(destination.get(1).unwrap().dbsize(), 3);
    assert_eq!(
        decoder.drive(&mut destination, 0).unwrap(),
        StreamStep::Finished
    );
    decoder.feed(&[]);
    assert_eq!(
        decoder.drive(&mut destination, 0).unwrap(),
        StreamStep::Finished
    );
    assert_eq!(decoder.object_loader_invocations, 4);
}

#[test]
fn collection_expiry_duplicate_keys_and_checksum_keep_existing_publication_order() {
    use crate::foundation::io::persistence::codec::types::RDB_OPCODE_EXPIRETIME_MS;
    for policy in [
        RdbDecodePolicy::persisted(true, 0),
        RdbDecodePolicy::replication(true, 0),
    ] {
        let record = collection_record(RDB_TYPE_LIST, b"expired", 2);
        let mut records = vec![RDB_OPCODE_EXPIRETIME_MS];
        records.extend_from_slice(&1i64.to_le_bytes());
        records.extend_from_slice(&record);
        let bytes = snapshot(&records);
        let split = bytes.len() - 10;
        let mut decoder = RdbDecoder::with_policy(policy);
        let mut destination = DbSet::new(1);
        decoder.feed(&bytes[..split]);
        assert_eq!(
            decoder.drive(&mut destination, 0).unwrap(),
            StreamStep::NeedMore
        );
        assert_eq!(decoder.object_loader_invocations, 0);
        assert_eq!(decoder.expire_ms, Some(1));
        assert!(decoder.expiry_events.is_empty());
        decoder.feed(&bytes[split..]);
        assert_eq!(
            decoder.drive(&mut destination, 0).unwrap(),
            StreamStep::Finished
        );
        assert!(decoder.record_scan.is_none());
        assert_eq!(decoder.object_loader_invocations, 1);
        if policy.expired_keys == ExpiredKeyPolicy::Skip {
            assert_eq!(destination.get(0).unwrap().dbsize(), 0);
            assert_eq!(decoder.loaded(), 0);
            assert_eq!(decoder.expired, 1);
            assert_eq!(
                decoder.expiry_events,
                vec![RdbExpiryEvent::Key {
                    db: 0,
                    key: b"expired".to_vec()
                }]
            );
        } else {
            assert_eq!(destination.get(0).unwrap().dbsize(), 1);
            assert_eq!(decoder.loaded(), 1);
            assert_eq!(decoder.expired, 0);
            assert!(decoder.expiry_events.is_empty());
        }
    }
    let first = collection_record(RDB_TYPE_SET, b"duplicate-key", 2);
    let mut records = first.clone();
    records.extend_from_slice(&collection_record(RDB_TYPE_SET, b"duplicate-key", 3));
    for allow_duplicates in [false, true] {
        let mut policy = RdbDecodePolicy::replication(true, 0);
        policy.allow_duplicate_keys = allow_duplicates;
        let mut decoder = RdbDecoder::with_policy(policy);
        let mut destination = DbSet::new(1);
        decoder.feed(&snapshot(&records));
        let result = decoder.drive(&mut destination, 0);
        assert!(decoder.record_scan.is_none());
        if allow_duplicates {
            assert_eq!(result.unwrap(), StreamStep::Finished);
            assert_eq!(decoder.loaded(), 2);
        } else {
            assert!(matches!(result, Err(LoadError::Corrupt)));
            assert_eq!(decoder.consumed_bytes(), 9 + first.len());
            assert_eq!(decoder.loaded(), 1);
        }
        assert_eq!(
            destination
                .get(0)
                .unwrap()
                .get_allow_expired(b"duplicate-key")
                .unwrap()
                .set_len(),
            if allow_duplicates { 3 } else { 2 }
        );
    }
    let mut bytes = snapshot(&first);
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    let mut decoder = RdbDecoder::new(true);
    let mut destination = DbSet::new(1);
    decoder.feed(&bytes);
    assert!(matches!(
        decoder.drive(&mut destination, 0),
        Err(LoadError::CrcError)
    ));
    assert_eq!(decoder.loaded(), 1);
    assert_eq!(decoder.consumed_bytes(), 9 + first.len());
    assert!(decoder.record_scan.is_none());
}

#[test]
fn empty_collections_and_legacy_infinities_remain_loader_owned() {
    for type_byte in [
        RDB_TYPE_LIST,
        RDB_TYPE_SET,
        RDB_TYPE_HASH,
        RDB_TYPE_ZSET,
        RDB_TYPE_ZSET_2,
    ] {
        let mut decoder = RdbDecoder::new(true);
        let mut destination = DbSet::new(1);
        decoder.feed(&snapshot(&[type_byte, 1, b'k', 0]));
        assert_eq!(
            decoder.drive(&mut destination, 0).unwrap(),
            StreamStep::Finished
        );
        assert_eq!(decoder.empty_keys, 1);
        assert_eq!(decoder.object_loader_invocations, 1);
        assert_eq!(destination.get(0).unwrap().dbsize(), 0);
    }
    let record = [RDB_TYPE_ZSET, 1, b'k', 2, 1, b'a', 254, 1, b'b', 255];
    assert_fragmented_record(&record, b"k", 2);
}
