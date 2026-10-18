use crate::foundation::io::persistence::*;
use crate::foundation::io::store::kv_engine::DbSet;
use crate::foundation::io::store::structure::hash::HashHt;
use crate::foundation::io::store::structure::list::ListValue;
use crate::foundation::io::store::structure::set::ValkeySet;
use crate::foundation::io::store::structure::zset::ZSetSl;
use crate::foundation::io::store::Robj;
use bytes::Bytes;
use indexmap::IndexMap;
use std::collections::{HashSet, VecDeque};

#[test]
fn crc64_matches_c_golden() {
    let v = crc64(0, b"123456789");
    assert_eq!(v, 0xe9c6d914c4b8d9ca);
}

#[test]
fn checksum_disabled_rdb_uses_a_zero_trailer_and_remains_loadable() {
    let mut source = DbSet::new(1);
    source
        .get_mut(0)
        .expect("DB 0 exists")
        .set(b"key".to_vec(), Robj::string("value"));

    let bytes = serialize_all_with_functions_and_replication_and_checksum(
        &source,
        0,
        &[],
        true,
        None,
        &[],
        false,
    );

    assert_eq!(&bytes[bytes.len() - 8..], &[0; 8]);
    let mut restored = DbSet::new(1);
    load_rdb_bytes_checked(&bytes, &mut restored, true)
        .expect("an RDB with a disabled checksum must remain loadable");
    assert_eq!(
        restored
            .get(0)
            .expect("DB 0 exists")
            .get_no_expire(b"key")
            .expect("key restored")
            .as_bytes(),
        b"value"
    );
}

#[test]
fn rdb_compression_option_controls_lzf_encoding_without_changing_loadability() {
    let value = "compressible-value-".repeat(64);
    let mut source = DbSet::new(1);
    source
        .get_mut(0)
        .expect("DB 0 exists")
        .set(b"key".to_vec(), Robj::string(value.clone()));
    let uncompressed = serialize_all_with_functions_and_replication_with_options(
        &source,
        0,
        &[],
        true,
        None,
        &[],
        RdbSerializeOptions {
            compression: false,
            checksum: true,
        },
    );
    let compressed = serialize_all_with_functions_and_replication_with_options(
        &source,
        0,
        &[],
        true,
        None,
        &[],
        RdbSerializeOptions {
            compression: true,
            checksum: true,
        },
    );

    assert!(compressed.len() < uncompressed.len());
    assert!(uncompressed
        .windows(value.len())
        .any(|window| window == value.as_bytes()));
    assert!(!compressed
        .windows(value.len())
        .any(|window| window == value.as_bytes()));
    for bytes in [&uncompressed, &compressed] {
        let mut restored = DbSet::new(1);
        load_rdb_bytes_checked(bytes, &mut restored, true).expect("load serialized RDB");
        assert_eq!(
            restored
                .get(0)
                .expect("DB 0 exists")
                .get_no_expire(b"key")
                .expect("key restored")
                .as_bytes(),
            value.as_bytes()
        );
    }
}

#[test]
fn streaming_rdb_batches_small_records_before_emitting() {
    let mut dbs = DbSet::new(1);
    let db = dbs.get_mut(0).expect("DB 0 exists");
    for index in 0..512 {
        db.set(
            format!("key-{index:04}").into_bytes(),
            Robj::string("value-that-keeps-the-stream-record-small"),
        );
    }

    let mut emitted = Vec::new();
    assert!(serialize_all_with_functions_streaming(
        &dbs,
        0,
        &[],
        true,
        |chunk| {
            emitted.push(chunk.to_vec());
            true
        }
    ));
    assert!(
        emitted.len() < 16,
        "small RDB records must be batched instead of emitted per key; got {} frames",
        emitted.len()
    );

    let bytes: Vec<u8> = emitted.into_iter().flatten().collect();
    let mut restored = DbSet::new(1);
    load_rdb_bytes_checked(&bytes, &mut restored, true)
        .expect("batched stream must remain valid RDB");
    assert_eq!(
        restored.get(0).expect("DB 0 exists").dbsize(),
        512,
        "batching must preserve every RDB record"
    );
}

#[test]
fn replication_rdb_round_trips_extension_aux_fields() {
    let dbs = DbSet::new(1);
    let replication = RdbReplicationInfo {
        repl_stream_db: 0,
        repl_id: "0123456789abcdef0123456789abcdef01234567".to_string(),
        repl_offset: 42,
        aux_fields: vec![(
            b"cluster-slot-states".to_vec(),
            b"7<aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,13>bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb,".to_vec(),
        )],
    };
    let bytes =
        serialize_all_with_functions_and_replication(&dbs, 0, &[], true, Some(&replication), &[]);
    let mut decoder = RdbStreamDecoder::new(false);
    let mut restored = DbSet::new(1);
    decoder.feed(&bytes);

    assert_eq!(
        decoder.drive(&mut restored, 0).unwrap(),
        StreamStep::Finished
    );
    assert_eq!(decoder.take_aux_fields(), replication.aux_fields);
}

#[test]
fn streaming_replication_rdb_round_trips_extension_aux_fields() {
    let dbs = DbSet::new(1);
    let replication = RdbReplicationInfo {
        repl_stream_db: 0,
        repl_id: "0123456789abcdef0123456789abcdef01234567".to_string(),
        repl_offset: 42,
        aux_fields: vec![(
            b"cluster-slot-states".to_vec(),
            b"7>bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb,".to_vec(),
        )],
    };
    let mut bytes = Vec::new();
    assert!(serialize_all_with_functions_and_replication_streaming(
        &dbs,
        0,
        &[],
        true,
        Some(&replication),
        &[],
        |chunk| {
            bytes.extend_from_slice(chunk);
            true
        },
    ));
    let mut decoder = RdbStreamDecoder::new(false);
    let mut restored = DbSet::new(1);
    decoder.feed(&bytes);

    assert_eq!(
        decoder.drive(&mut restored, 0).unwrap(),
        StreamStep::Finished
    );
    assert_eq!(decoder.take_aux_fields(), replication.aux_fields);
}

#[test]
fn dump_tcl_golden_bar() {
    let prefix = b"\x00\x03\x62\x61\x72\xde\x00";
    let crc = crc64(0, prefix);
    let expected = [0x0f, b'Y', b'U', b'z', b'a', 0xd3, 0xec, 0xe0];
    assert_eq!(crc.to_le_bytes(), expected);
}

#[test]
fn len_roundtrip() {
    for len in [
        0u64,
        63,
        64,
        16383,
        16384,
        70000,
        u32::MAX as u64,
        (u32::MAX as u64) + 1,
    ] {
        let mut out = Vec::new();
        save_len(&mut out, len);
        let mut pos = 0;
        let (back, enc) = load_len(&out, &mut pos).unwrap();
        assert!(!enc);
        assert_eq!(back, len);
    }
}

#[test]
fn listpack_decode_uint() {
    let lp: [u8; 9] = [0x09, 0x00, 0x00, 0x00, 0x01, 0x00, 0x2a, 0x01, 0xff];
    let items = decoder_compact::listpack_decode(&lp).unwrap();
    assert_eq!(items, vec![b"42".to_vec()]);
}

#[test]
fn listpack_decode_str() {
    let lp: [u8; 11] = [
        0x0b, 0x00, 0x00, 0x00, 0x01, 0x00, 0x82, b'h', b'i', 0x03, 0xff,
    ];
    let items = decoder_compact::listpack_decode(&lp).unwrap();
    assert_eq!(items, vec![b"hi".to_vec()]);
}

#[test]
fn ziplist_decode_immediate() {
    let zl: [u8; 13] = [
        0x0d, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0xf5, 0xff,
    ];
    let items = decoder_compact::ziplist_decode(&zl).unwrap();
    assert_eq!(items, vec![b"4".to_vec()]);
}

#[test]
fn intset_decode_i16() {
    let is: [u8; 12] = [
        0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x02, 0x00,
    ];
    let items = decoder_compact::intset_decode(&is).unwrap();
    assert_eq!(items, vec![b"1".to_vec(), b"2".to_vec()]);
}

#[test]
fn zipmap_decode_pair() {
    let zm: [u8; 7] = [0x01, 0x01, b'k', 0x01, 0x00, b'v', 0xff];
    let pairs = decoder_compact::zipmap_decode(&zm).unwrap();
    assert_eq!(pairs, vec![(b"k".to_vec(), b"v".to_vec())]);
}

#[test]
fn lzf_decompress_literal() {
    let input: [u8; 4] = [0x02, b'a', b'b', b'c'];
    let out = decoder_compact::lzf_decompress(&input, 3).unwrap();
    assert_eq!(out, b"abc");
}

#[test]
fn string_roundtrip_raw() {
    let mut out = Vec::new();
    save_string(&mut out, b"bar");
    assert_eq!(out, b"\x03\x62\x61\x72");
    let mut pos = 0;
    let back = load_string(&out, &mut pos).unwrap();
    assert_eq!(back, b"bar");
    assert_eq!(pos, out.len());
}

#[test]
fn string_roundtrip_integers() {
    for &(val, _) in &[
        ("0", 0i32),
        ("127", 0),
        ("-128", 0),
        ("32767", 0),
        ("-32768", 0),
        ("2147483647", 0),
        ("-2147483648", 0),
    ] {
        let mut out = Vec::new();
        save_string(&mut out, val.as_bytes());
        let mut pos = 0;
        let back = load_string(&out, &mut pos).unwrap();
        assert_eq!(back, val.as_bytes());
    }
}

#[test]
fn binary_double_roundtrip() {
    for v in [0.0f64, 1.0, -1.5, 12345.6789, 1e17, -2.5e-10, f64::MAX] {
        let mut out = Vec::new();
        save_binary_double(&mut out, v);
        assert_eq!(out.len(), 8);
        let mut pos = 0;
        let back = load_binary_double(&out, &mut pos).unwrap();
        assert_eq!(back, v);
        assert_eq!(pos, 8);
    }
}

#[test]
fn load_rdb_bytes_basic() {
    use crate::foundation::io::store::kv_engine::now_ms;

    let mut buf = Vec::new();
    buf.extend_from_slice(format!("VALKEY{:03}", RDB_VERSION).as_bytes());
    buf.push(RDB_OPCODE_AUX);
    save_string(&mut buf, b"redis-ver");
    save_string(&mut buf, b"7.0.0");
    buf.push(RDB_OPCODE_SELECTDB);
    save_len(&mut buf, 0);
    let future = now_ms() + 100_000;
    buf.push(RDB_OPCODE_EXPIRETIME_MS);
    save_millisecond_time(&mut buf, future as i64);
    buf.push(type_byte(&Robj::string("bar")));
    save_string(&mut buf, b"foo");
    save_robj(&Robj::string("bar"), &mut buf);
    buf.push(type_byte(&Robj::string("123")));
    save_string(&mut buf, b"counter");
    save_robj(&Robj::string("123"), &mut buf);
    buf.push(RDB_OPCODE_EOF);
    let crc = crc64(0, &buf);
    buf.extend_from_slice(&crc.to_le_bytes());

    let mut dbs = DbSet::new(16);
    let (loaded, _consumed) = load_rdb_bytes(&buf, &mut dbs).unwrap();
    assert_eq!(loaded, 2);
    let db = dbs.get(0).unwrap();
    assert_eq!(db.get_no_expire(b"foo").unwrap().as_bytes(), b"bar");
    assert_eq!(db.get_no_expire(b"counter").unwrap().as_bytes(), b"123");
    assert_eq!(db.get_expire(b"foo"), Some(future));
    assert_eq!(db.get_expire(b"counter"), None);
}

#[test]
fn validate_rdb_header_checks_future_versions_before_loading_payload() {
    let future = b"VALKEY987";

    assert!(matches!(
        validate_rdb_header(future, true),
        Err(LoadError::IncompatibleVersion(987))
    ));
    assert!(validate_rdb_header(future, false).is_ok());
}

#[test]
fn load_rdb_bytes_skips_expired() {
    let mut buf = Vec::new();
    buf.extend_from_slice(format!("VALKEY{:03}", RDB_VERSION).as_bytes());
    buf.push(RDB_OPCODE_SELECTDB);
    save_len(&mut buf, 0);
    buf.push(RDB_OPCODE_EXPIRETIME_MS);
    save_millisecond_time(&mut buf, 1000);
    buf.push(type_byte(&Robj::string("v")));
    save_string(&mut buf, b"stale");
    save_robj(&Robj::string("v"), &mut buf);
    buf.push(RDB_OPCODE_EOF);
    let crc = crc64(0, &buf);
    buf.extend_from_slice(&crc.to_le_bytes());

    let mut dbs = DbSet::new(16);
    let (loaded, _consumed) = load_rdb_bytes(&buf, &mut dbs).unwrap();
    assert_eq!(loaded, 0);
    assert!(!dbs.get(0).unwrap().exists_no_expire(b"stale"));
}

#[test]
fn load_rdb_bytes_skips_empty_collections() {
    let mut buf = Vec::new();
    buf.extend_from_slice(format!("VALKEY{:03}", RDB_VERSION).as_bytes());
    buf.push(RDB_OPCODE_SELECTDB);
    save_len(&mut buf, 0);
    let empty_list = Robj::ListQuicklist(ListValue::new());
    buf.push(type_byte(&empty_list));
    save_string(&mut buf, b"empty-list");
    save_robj(&empty_list, &mut buf);
    buf.push(RDB_OPCODE_EOF);
    let crc = crc64(0, &buf);
    buf.extend_from_slice(&crc.to_le_bytes());

    let mut dbs = DbSet::new(16);
    let (loaded, _consumed) = load_rdb_bytes(&buf, &mut dbs).unwrap();
    assert_eq!(loaded, 0);
    assert_eq!(dbs.get(0).unwrap().dbsize(), 0);
}

#[test]
fn load_rdb_bytes_bad_crc() {
    let mut buf = Vec::new();
    buf.extend_from_slice(format!("VALKEY{:03}", RDB_VERSION).as_bytes());
    buf.push(RDB_OPCODE_EOF);
    buf.extend_from_slice(&0xdead_beef_u64.to_le_bytes());

    let mut dbs = DbSet::new(16);
    assert!(load_rdb_bytes(&buf, &mut dbs).is_err());
}

// P1 (G2r): the RDB load worker honors `key-load-delay` by sleeping per key,
// mirroring C's `debugDelay(server.key_load_delay)` in rdb.c. Default 0 must be
// a strict no-op; a large delay must slow the load proportionally to key count.
// This is the load-side pacing the slow/fast-replica sections depend on.
#[test]
fn load_rdb_honors_per_key_delay_and_is_noop_at_zero() {
    // Build a 3-key RDB (DB 0).
    let mut buf = Vec::new();
    buf.extend_from_slice(format!("VALKEY{:03}", RDB_VERSION).as_bytes());
    buf.push(RDB_OPCODE_SELECTDB);
    save_len(&mut buf, 0);
    for i in 0..3u32 {
        let key = format!("k{i}");
        buf.push(type_byte(&Robj::string("v")));
        save_string(&mut buf, key.as_bytes());
        save_robj(&Robj::string("v"), &mut buf);
    }
    buf.push(RDB_OPCODE_EOF);
    let crc = crc64(0, &buf);
    buf.extend_from_slice(&crc.to_le_bytes());

    // delay = 0 → strict no-op: all keys still load, and it returns near-instantly.
    let mut dbs0 = DbSet::new(16);
    let start = std::time::Instant::now();
    let (loaded0, _) = load_rdb_bytes_checked_with_delay(&buf, &mut dbs0, false, 0).unwrap();
    let zero_elapsed = start.elapsed();
    assert_eq!(loaded0, 3);
    assert!(
        zero_elapsed < std::time::Duration::from_millis(50),
        "delay=0 must be a no-op, took {zero_elapsed:?}"
    );

    // delay = 20ms/key → 3 keys must take clearly longer than 40ms while still
    // loading every key. Compared against the no-op run to keep it robust.
    let mut dbs1 = DbSet::new(16);
    let start = std::time::Instant::now();
    let (loaded1, _) = load_rdb_bytes_checked_with_delay(&buf, &mut dbs1, false, 20_000).unwrap();
    let delayed_elapsed = start.elapsed();
    assert_eq!(loaded1, 3);
    assert!(
        delayed_elapsed >= std::time::Duration::from_millis(40),
        "delay=20ms/key over 3 keys must exceed 40ms, took {delayed_elapsed:?}"
    );
}

#[test]
fn load_rdb_rebuilds_all_types() {
    let mut src = DbSet::new(16);
    {
        let db = src.get_mut(0).unwrap();
        db.set(b"s".to_vec(), Robj::string("hello"));
        let mut l = VecDeque::new();
        l.push_back(Bytes::from_static(b"a"));
        l.push_back(Bytes::from_static(b"b"));
        db.set(
            b"l".to_vec(),
            Robj::ListQuicklist(ListValue::from_vecdeque(l)),
        );
        let mut set = HashSet::new();
        set.insert(Bytes::from_static(b"x"));
        db.set(
            b"st".to_vec(),
            Robj::SetHashtable(ValkeySet::from_hashset(set)),
        );
        let mut h = IndexMap::new();
        h.insert(Bytes::from_static(b"f"), Bytes::from_static(b"v"));
        db.set(
            b"h".to_vec(),
            Robj::HashHashtable(HashHt { data: Box::new(h) }),
        );
        let mut z = crate::foundation::io::store::structure::hashtable::HashTable::new();
        z.insert(Bytes::from_static(b"m"), 1.5f64);
        db.set(b"z".to_vec(), Robj::ZSetSkiplist(ZSetSl::from_dict(z)));
    }
    let bytes = crate::foundation::io::persistence::serialize_all(&src, 0);

    let mut dst = DbSet::new(16);
    load_rdb(&bytes, &mut dst).expect("load_rdb ok");

    let db = dst.get(0).unwrap();
    assert_eq!(db.dbsize(), 5);
    assert_eq!(db.get_no_expire(b"s").unwrap().as_bytes(), b"hello");
    assert!(matches!(
        db.get_no_expire(b"l").unwrap(),
        Robj::ListQuicklist(_)
    ));
    assert!(matches!(
        db.get_no_expire(b"st").unwrap(),
        Robj::SetHashtable(_)
    ));
    assert!(matches!(
        db.get_no_expire(b"h").unwrap(),
        Robj::HashHashtable(_)
    ));
    assert!(matches!(
        db.get_no_expire(b"z").unwrap(),
        Robj::ZSetSkiplist(_)
    ));
}

#[test]
fn load_rdb_rejects_bad_magic() {
    let mut dst = DbSet::new(16);
    assert!(load_rdb(b"NOTRDB0080\xff", &mut dst).is_err());
}

#[test]
fn initial_preamble_roundtrip() {
    let mut src = DbSet::new(1);
    {
        let db = src.get_mut(0).unwrap();
        db.set(b"key1".to_vec(), Robj::string("value1"));
    }
    let bytes = crate::foundation::io::persistence::serialize_all(&src, 0);
    let mut dst = DbSet::new(1);
    crate::foundation::io::persistence::load_rdb_bytes(&bytes, &mut dst)
        .expect("preamble must round-trip through decoder");
    let db_dst = dst.get_mut(0).unwrap();
    assert!(db_dst.get(b"key1").is_some());
}
