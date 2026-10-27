use crate::common::{Client, TestServer};

fn bulk_payload(reply: &[u8]) -> &[u8] {
    assert_eq!(reply[0], b'$', "expected a bulk reply: {reply:?}");
    let header_end = reply.windows(2).position(|part| part == b"\r\n").unwrap();
    let len: usize = std::str::from_utf8(&reply[1..header_end])
        .unwrap()
        .parse()
        .unwrap();
    &reply[header_end + 2..header_end + 2 + len]
}

fn set_entries_limit(client: &mut Client, limit: &[u8]) {
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"zset-max-listpack-entries", limit]),
        b"+OK\r\n"
    );
}

fn assert_encoding(client: &mut Client, key: &[u8], encoding: &[u8], rdb_type: u8) {
    let reply = client.command(&[b"OBJECT", b"ENCODING", key]);
    assert_eq!(bulk_payload(&reply), encoding, "key: {key:?}");
    let dump = client.command(&[b"DUMP", key]);
    assert_eq!(bulk_payload(&dump)[0], rdb_type, "key: {key:?}");
}

#[test]
fn upgrade_581_zset_tree_encoding_survives_config_relaxation() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    set_entries_limit(&mut client, b"0");
    assert_eq!(client.command(&[b"ZADD", b"key", b"1", b"a"]), b":1\r\n");
    let dump = client.command(&[b"DUMP", b"key"]);
    assert_eq!(bulk_payload(&dump)[0], 5);
    set_entries_limit(&mut client, b"128");
    assert_encoding(&mut client, b"key", b"btree", 5);
    assert_eq!(client.command(&[b"ZADD", b"key", b"2", b"b"]), b":1\r\n");
    assert_encoding(&mut client, b"key", b"btree", 5);
}

#[test]
fn upgrade_581_zset_tree_encoding_survives_long_member_removal() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    let long_member = vec![b'x'; 65];
    assert_eq!(
        client.command(&[b"ZADD", b"key", b"1", b"a", b"2", &long_member]),
        b":2\r\n"
    );
    assert_eq!(client.command(&[b"ZREM", b"key", &long_member]), b":1\r\n");
    assert_encoding(&mut client, b"key", b"btree", 5);
}

#[test]
fn upgrade_581_zset_compact_reads_preserve_physical_encoding() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    assert_eq!(
        client.command(&[b"ZADD", b"key", b"0", b"a", b"0", b"b", b"0", b"c"]),
        b":3\r\n"
    );
    set_entries_limit(&mut client, b"0");
    assert_eq!(client.command(&[b"ZSCORE", b"key", b"b"]), b"$1\r\n0\r\n");
    assert_eq!(
        client.command(&[b"ZCOUNT", b"key", b"-inf", b"+inf"]),
        b":3\r\n"
    );
    assert_eq!(
        client.command(&[b"ZLEXCOUNT", b"key", b"(a", b"[c"]),
        b":2\r\n"
    );
    assert_eq!(
        client.command(&[b"ZRANGE", b"key", b"0", b"-1"]),
        b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
    );
    assert_encoding(&mut client, b"key", b"listpack", 17);
}

#[test]
fn upgrade_581_zset_store_results_use_real_compact_objects() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    assert_eq!(
        client.command(&[b"ZADD", b"src", b"1", b"a", b"2", b"b"]),
        b":2\r\n"
    );
    for command in [
        &[b"ZUNIONSTORE".as_slice(), b"dst", b"1", b"src"][..],
        &[b"ZINTERSTORE".as_slice(), b"dst", b"1", b"src"][..],
        &[b"ZDIFFSTORE".as_slice(), b"dst", b"1", b"src"][..],
        &[b"ZRANGESTORE".as_slice(), b"dst", b"src", b"0", b"-1"][..],
    ] {
        set_entries_limit(&mut client, b"128");
        assert_eq!(client.command(command), b":2\r\n", "{command:?}");
        assert_encoding(&mut client, b"dst", b"listpack", 17);
        set_entries_limit(&mut client, b"0");
        assert_encoding(&mut client, b"dst", b"listpack", 17);
    }
    assert_encoding(&mut client, b"src", b"listpack", 17);
}

#[test]
fn upgrade_581_zset_store_results_honor_entry_and_member_limits() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    assert_eq!(
        client.command(&[b"ZADD", b"src", b"1", b"a", b"2", b"b"]),
        b":2\r\n"
    );
    for command in [
        &[b"ZUNIONSTORE".as_slice(), b"dst", b"1", b"src"][..],
        &[b"ZINTERSTORE".as_slice(), b"dst", b"1", b"src"][..],
        &[b"ZDIFFSTORE".as_slice(), b"dst", b"1", b"src"][..],
        &[b"ZRANGESTORE".as_slice(), b"dst", b"src", b"0", b"-1"][..],
    ] {
        set_entries_limit(&mut client, b"1");
        assert_eq!(client.command(command), b":2\r\n");
        assert_encoding(&mut client, b"dst", b"btree", 5);
        set_entries_limit(&mut client, b"128");
        assert_encoding(&mut client, b"dst", b"btree", 5);
        assert_eq!(
            client.command(&[b"CONFIG", b"SET", b"zset-max-listpack-value", b"0"]),
            b"+OK\r\n"
        );
        assert_eq!(client.command(command), b":2\r\n");
        assert_encoding(&mut client, b"dst", b"btree", 5);
        assert_eq!(
            client.command(&[b"CONFIG", b"SET", b"zset-max-listpack-value", b"64"]),
            b"+OK\r\n"
        );
    }
}

#[test]
fn upgrade_581_sort_explicitly_upgrades_compact_zsets() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    for command in [b"SORT".as_slice(), b"SORT_RO".as_slice()] {
        assert_eq!(client.command(&[b"DEL", b"key"]), b":0\r\n");
        assert_eq!(
            client.command(&[b"ZADD", b"key", b"3", b"a", b"1", b"b", b"2", b"c"]),
            b":3\r\n"
        );
        assert_encoding(&mut client, b"key", b"listpack", 17);
        assert_eq!(
            client.command(&[command, b"key", b"BY", b"nosort", b"DESC", b"LIMIT", b"1", b"2"]),
            b"*2\r\n$1\r\nc\r\n$1\r\nb\r\n"
        );
        assert_encoding(&mut client, b"key", b"btree", 5);
        assert_eq!(client.command(&[b"DEL", b"key"]), b":1\r\n");
    }
}

#[test]
fn upgrade_581_sort_option_error_does_not_upgrade_compact_zsets() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    assert_eq!(client.command(&[b"ZADD", b"key", b"1", b"a"]), b":1\r\n");
    assert_eq!(
        client.command(&[b"SORT", b"key", b"invalid"]),
        b"-ERR syntax error\r\n"
    );
    assert_eq!(
        client.command(&[b"SORT_RO", b"key", b"STORE", b"dst"]),
        b"-ERR syntax error\r\n"
    );
    assert_encoding(&mut client, b"key", b"listpack", 17);
}

fn legacy_ziplist_dump() -> Vec<u8> {
    let ziplist = [
        23, 0, 0, 0, 19, 0, 0, 0, 4, 0, 0, 1, b'a', 3, 1, b'1', 3, 1, b'b', 3, 1, b'2', 255,
    ];
    let mut payload = vec![12, ziplist.len() as u8];
    payload.extend_from_slice(&ziplist);
    payload.extend_from_slice(&10u16.to_le_bytes());
    let mut crc = 0u64;
    for byte in &payload {
        crc ^= u64::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ if crc & 1 != 0 { 0x95ac9329ac4bc9b5 } else { 0 };
        }
    }
    payload.extend_from_slice(&crc.to_le_bytes());
    payload
}

#[test]
fn upgrade_581_restore_legacy_zset_ziplist_creates_real_listpack() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    let payload = legacy_ziplist_dump();
    assert_eq!(
        client.command(&[b"RESTORE", b"key", b"0", &payload]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"ZRANGE", b"key", b"0", b"-1", b"WITHSCORES"]),
        b"*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n"
    );
    assert_encoding(&mut client, b"key", b"listpack", 17);
    set_entries_limit(&mut client, b"0");
    assert_encoding(&mut client, b"key", b"listpack", 17);
}

#[test]
fn upgrade_581_restore_zset_listpack_preserves_real_listpack() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    assert_eq!(
        client.command(&[b"ZADD", b"src", b"1.5", b"a", b"2", b"b"]),
        b":2\r\n"
    );
    let dump = client.command(&[b"DUMP", b"src"]);
    assert_eq!(
        client.command(&[b"RESTORE", b"dst", b"0", bulk_payload(&dump)]),
        b"+OK\r\n"
    );
    assert_encoding(&mut client, b"dst", b"listpack", 17);
}

#[test]
fn upgrade_581_restore_regular_zset_uses_host_compact_limits() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    set_entries_limit(&mut client, b"0");
    assert_eq!(
        client.command(&[b"ZADD", b"src", b"1", b"a", b"2", b"b"]),
        b":2\r\n"
    );
    let dump = client.command(&[b"DUMP", b"src"]);
    assert_eq!(bulk_payload(&dump)[0], 5);
    set_entries_limit(&mut client, b"128");
    assert_eq!(
        client.command(&[b"RESTORE", b"dst", b"0", bulk_payload(&dump)]),
        b"+OK\r\n"
    );
    assert_encoding(&mut client, b"src", b"btree", 5);
    assert_encoding(&mut client, b"dst", b"listpack", 17);
}

#[test]
fn upgrade_581_restore_compact_zsets_upgrade_using_host_limits() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    assert_eq!(
        client.command(&[b"ZADD", b"src", b"1", b"a", b"2", b"b"]),
        b":2\r\n"
    );
    let dump = client.command(&[b"DUMP", b"src"]);
    for payload in [bulk_payload(&dump), legacy_ziplist_dump().as_slice()] {
        set_entries_limit(&mut client, b"1");
        assert_eq!(
            client.command(&[b"RESTORE", b"dst", b"0", payload, b"REPLACE"]),
            b"+OK\r\n"
        );
        assert_encoding(&mut client, b"dst", b"btree", 5);
        set_entries_limit(&mut client, b"128");
        assert_eq!(
            client.command(&[b"CONFIG", b"SET", b"zset-max-listpack-value", b"0"]),
            b"+OK\r\n"
        );
        assert_eq!(
            client.command(&[b"RESTORE", b"dst", b"0", payload, b"REPLACE"]),
            b"+OK\r\n"
        );
        assert_encoding(&mut client, b"dst", b"btree", 5);
        assert_eq!(
            client.command(&[b"CONFIG", b"SET", b"zset-max-listpack-value", b"64"]),
            b"+OK\r\n"
        );
    }
}

#[test]
fn upgrade_581_debug_reload_applies_host_zset_limits_in_both_directions() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    set_entries_limit(&mut client, b"0");
    assert_eq!(
        client.command(&[b"ZADD", b"key", b"1", b"a", b"2", b"b"]),
        b":2\r\n"
    );
    assert_eq!(client.command(&[b"SAVE"]), b"+OK\r\n");
    set_entries_limit(&mut client, b"128");
    assert_eq!(
        client.command(&[b"DEBUG", b"RELOAD", b"NOSAVE"]),
        b"+OK\r\n"
    );
    assert_encoding(&mut client, b"key", b"listpack", 17);

    assert_eq!(client.command(&[b"SAVE"]), b"+OK\r\n");
    set_entries_limit(&mut client, b"1");
    assert_eq!(
        client.command(&[b"DEBUG", b"RELOAD", b"NOSAVE"]),
        b"+OK\r\n"
    );
    assert_encoding(&mut client, b"key", b"btree", 5);
    assert_eq!(client.command(&[b"ZSCORE", b"key", b"b"]), b"$1\r\n2\r\n");
}

#[test]
fn upgrade_581_compact_zset_keeps_sub_epsilon_score_changes() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    assert_eq!(
        client.command(&[b"ZADD", b"key", b"0", b"member"]),
        b":1\r\n"
    );
    assert_eq!(
        client.command(&[b"ZADD", b"key", b"CH", b"1e-17", b"member"]),
        b":1\r\n"
    );
    let reply = client.command(&[b"ZSCORE", b"key", b"member"]);
    let score = std::str::from_utf8(bulk_payload(&reply))
        .unwrap()
        .parse::<f64>()
        .unwrap();
    assert_eq!(score.to_bits(), 1e-17_f64.to_bits());
    assert_encoding(&mut client, b"key", b"listpack", 17);
}

fn large_legacy_ziplist_dump(count: usize) -> Vec<u8> {
    let mut ziplist = vec![0; 10];
    let mut previous_len = 0u8;
    let mut tail = 10usize;
    for index in 0..count {
        let member = format!("{index:06}");
        ziplist.extend_from_slice(&[previous_len, 6]);
        ziplist.extend_from_slice(member.as_bytes());
        tail = ziplist.len();
        ziplist.extend_from_slice(&[8, 1, b'1']);
        previous_len = 3;
    }
    ziplist.push(255);
    let length = ziplist.len() as u32;
    ziplist[..4].copy_from_slice(&length.to_le_bytes());
    ziplist[4..8].copy_from_slice(&(tail as u32).to_le_bytes());
    let cached_count = u16::try_from(count * 2).unwrap_or(u16::MAX);
    ziplist[8..10].copy_from_slice(&cached_count.to_le_bytes());
    let mut payload = vec![12, 0x80];
    payload.extend_from_slice(&length.to_be_bytes());
    payload.extend_from_slice(&ziplist);
    payload.extend_from_slice(&10u16.to_le_bytes());
    let crc = versal::foundation::io::persistence::crc64(0, &payload);
    payload.extend_from_slice(&crc.to_le_bytes());
    payload
}

#[test]
fn upgrade_581_large_legacy_restore_and_geo_append_preserve_count() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    let payload = large_legacy_ziplist_dump(32_768);
    for (limit, encoding) in [
        (b"128".as_slice(), b"btree".as_slice()),
        (b"0".as_slice(), b"btree".as_slice()),
        (b"32768".as_slice(), b"listpack".as_slice()),
    ] {
        set_entries_limit(&mut client, limit);
        assert_eq!(
            client.command(&[b"RESTORE", b"key", b"0", &payload, b"REPLACE"]),
            b"+OK\r\n"
        );
        let reply = client.command(&[b"OBJECT", b"ENCODING", b"key"]);
        assert_eq!(bulk_payload(&reply), encoding);
        assert_eq!(client.command(&[b"ZCARD", b"key"]), b":32768\r\n");
        assert_eq!(
            client.command(&[b"ZSCORE", b"key", b"032767"]),
            b"$1\r\n1\r\n"
        );
    }
    let payload = large_legacy_ziplist_dump(32_767);
    assert_eq!(
        client.command(&[b"RESTORE", b"key", b"0", &payload, b"REPLACE"]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"GEOADD", b"key", b"0", b"0", b"last"]),
        b":1\r\n"
    );
    assert_eq!(client.command(&[b"ZCARD", b"key"]), b":32768\r\n");
    let reply = client.command(&[b"OBJECT", b"ENCODING", b"key"]);
    assert_eq!(bulk_payload(&reply), b"listpack");
}

#[test]
fn upgrade_581_full_sync_applies_replica_zset_limits() {
    use std::time::{Duration, Instant};
    for (source_limit, replica_limit, encoding, rdb_type) in [
        ("0", "128", b"listpack".as_slice(), 17),
        ("128", "1", b"btree".as_slice(), 5),
    ] {
        let primary = TestServer::start(&["--zset-max-listpack-entries", source_limit]);
        let mut source = primary.connect();
        assert_eq!(
            source.command(&[b"ZADD", b"key", b"1", b"a", b"2", b"b"]),
            b":2\r\n"
        );
        let replica = TestServer::start(&[
            "--zset-max-listpack-entries",
            replica_limit,
            "--replicaof",
            "127.0.0.1",
            &primary.port.to_string(),
        ]);
        let mut client = replica.connect();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let info = client.command_str(&[b"INFO", b"replication"]);
            if info.contains("master_link_status:up\r\n") {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "full sync did not finish: {info}"
            );
            std::thread::yield_now();
        }
        assert_encoding(&mut client, b"key", encoding, rdb_type);
        assert_eq!(client.command(&[b"ZCARD", b"key"]), b":2\r\n");
    }
}

#[test]
fn upgrade_581_zset_counts_respect_score_and_lex_bounds_in_both_encodings() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    for limit in [b"128".as_slice(), b"0".as_slice()] {
        set_entries_limit(&mut client, limit);
        client.command(&[b"DEL", b"scores", b"lex"]);
        assert_eq!(
            client.command(&[
                b"ZADD", b"scores", b"-inf", b"a", b"-0", b"b", b"0", b"c", b"1", b"d", b"+inf",
                b"e",
            ]),
            b":5\r\n"
        );
        for (min, max, count) in [
            (b"-inf".as_slice(), b"+inf".as_slice(), b":5\r\n".as_slice()),
            (b"-0".as_slice(), b"0".as_slice(), b":2\r\n".as_slice()),
            (b"(0".as_slice(), b"+inf".as_slice(), b":2\r\n".as_slice()),
            (
                b"(-inf".as_slice(),
                b"(+inf".as_slice(),
                b":3\r\n".as_slice(),
            ),
            (b"1".as_slice(), b"(1".as_slice(), b":0\r\n".as_slice()),
            (b"2".as_slice(), b"1".as_slice(), b":0\r\n".as_slice()),
        ] {
            assert_eq!(client.command(&[b"ZCOUNT", b"scores", min, max]), count);
        }
        assert_eq!(
            client.command(&[b"ZADD", b"lex", b"0", b"a", b"0", b"b", b"0", b"c"]),
            b":3\r\n"
        );
        for (min, max, count) in [
            (b"-".as_slice(), b"+".as_slice(), b":3\r\n".as_slice()),
            (b"[a".as_slice(), b"[c".as_slice(), b":3\r\n".as_slice()),
            (b"(a".as_slice(), b"(c".as_slice(), b":1\r\n".as_slice()),
            (b"[c".as_slice(), b"[a".as_slice(), b":0\r\n".as_slice()),
            (b"+".as_slice(), b"+".as_slice(), b":0\r\n".as_slice()),
            (b"-".as_slice(), b"-".as_slice(), b":0\r\n".as_slice()),
        ] {
            assert_eq!(client.command(&[b"ZLEXCOUNT", b"lex", min, max]), count);
        }
    }
}
