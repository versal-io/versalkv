use crate::common::{Client, TestServer};
use versal::foundation::io::persistence;
use versal::foundation::io::store::structure::set::SetLp;
use versal::foundation::io::store::Robj;
use versal::foundation::memory::value::SetOps;

#[test]
fn oversized_numkeys_returns_error_and_keeps_connection_usable() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    let out_of_range = (usize::MAX as u128 + 1).to_string();
    for (count, expected) in [
        (usize::MAX.to_string(), b"-ERR syntax error\r\n".as_slice()),
        ((usize::MAX - 1).to_string(), b"-ERR syntax error\r\n"),
        (
            out_of_range,
            b"-ERR at least 1 input key is needed for 'zintercard' command\r\n",
        ),
    ] {
        assert_eq!(
            client.command(&[b"ZINTERCARD", count.as_bytes(), b"key"]),
            expected,
            "count: {count}"
        );
        assert_eq!(client.command(&[b"PING"]), b"+PONG\r\n");
    }
}

fn restore_raw_score_source(client: &mut Client, key: &[u8], pairs: &[(&[u8], &[u8])]) {
    let mut listpack = vec![0; 6];
    listpack[4..6].copy_from_slice(&u16::try_from(pairs.len() * 2).unwrap().to_le_bytes());
    // Short string entries retain score bytes that normal serialization canonicalizes.
    for (member, score) in pairs {
        for entry in [member, score] {
            assert!(entry.len() <= 63);
            listpack.push(0x80 | entry.len() as u8);
            listpack.extend_from_slice(entry);
            listpack.push((entry.len() + 1) as u8);
        }
    }
    listpack.push(0xff);
    let size = u32::try_from(listpack.len()).unwrap();
    listpack[0..4].copy_from_slice(&size.to_le_bytes());
    let mut payload = vec![persistence::codec::types::RDB_TYPE_ZSET_LISTPACK];
    persistence::codec::types::save_len(&mut payload, listpack.len() as u64);
    payload.extend_from_slice(&listpack);
    payload.extend_from_slice(&persistence::RDB_VERSION.to_le_bytes());
    let crc = persistence::crc64(0, &payload);
    payload.extend_from_slice(&crc.to_le_bytes());
    assert_eq!(
        client.command(&[b"RESTORE", key, b"0", &payload]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"OBJECT", b"ENCODING", key]),
        b"$8\r\nlistpack\r\n"
    );
}

#[test]
fn restored_score_single_candidate_membership() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    for member in [b"x".as_slice(), b"", b"\0\xff", b"1"] {
        client.command(&[b"DEL", b"small", b"compact"]);
        restore_raw_score_source(&mut client, b"compact", &[(member, b" 1"), (b"y", b"2")]);
        add_source(&mut client, b"small", 4, &[member]);
        for keys in [[b"small".as_slice(), b"compact"], [b"compact", b"small"]] {
            for limit in [None, Some(b"0".as_slice()), Some(b"1".as_slice())] {
                let mut parts = vec![b"ZINTERCARD".as_slice(), b"2", keys[0], keys[1]];
                if let Some(limit) = limit {
                    parts.extend([b"LIMIT".as_slice(), limit]);
                }
                assert_eq!(
                    client.command(&parts),
                    b":1\r\n",
                    "member: {member:?}, keys: {keys:?}, limit: {limit:?}"
                );
            }
        }
    }
}

#[test]
fn restored_score_membership_survives_index_transition() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    restore_raw_score_source(
        &mut client,
        b"compact",
        &[
            (b"x", b" 1"),
            (b"y", b" 2"),
            (b"z", b" 3"),
            (b"spare", b"4"),
        ],
    );
    add_source(&mut client, b"small", 4, &[b"x", b"y", b"z"]);
    for keys in [[b"small".as_slice(), b"compact"], [b"compact", b"small"]] {
        for (limit, expected) in [
            (b"0".as_slice(), b":3\r\n".as_slice()),
            (b"1", b":1\r\n"),
            (b"10", b":3\r\n"),
        ] {
            assert_eq!(
                client.command(&[b"ZINTERCARD", b"2", keys[0], keys[1], b"LIMIT", limit]),
                expected,
                "keys: {keys:?}, limit: {limit:?}"
            );
        }
    }
}

#[test]
fn restored_score_compact_smallest_preserves_membership() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    restore_raw_score_source(&mut client, b"compact", &[(b"x", b" 1"), (b"y", b"2")]);
    add_source(&mut client, b"large", 4, &[b"x", b"y", b"z"]);
    for keys in [[b"compact".as_slice(), b"large"], [b"large", b"compact"]] {
        for (limit, expected) in [(b"0".as_slice(), b":2\r\n".as_slice()), (b"1", b":1\r\n")] {
            assert_eq!(
                client.command(&[b"ZINTERCARD", b"2", keys[0], keys[1], b"LIMIT", limit]),
                expected
            );
        }
    }
}

fn add_source(client: &mut Client, key: &[u8], kind: usize, members: &[&[u8]]) {
    let (config, limit, encoding) = match kind {
        0 => (
            b"zset-max-listpack-entries".as_slice(),
            b"128".as_slice(),
            b"listpack".as_slice(),
        ),
        1 => (
            b"zset-max-listpack-entries".as_slice(),
            b"0".as_slice(),
            b"btree".as_slice(),
        ),
        2 => (
            b"set-max-intset-entries".as_slice(),
            b"512".as_slice(),
            b"intset".as_slice(),
        ),
        3 => (
            b"set-max-listpack-entries".as_slice(),
            b"128".as_slice(),
            b"listpack".as_slice(),
        ),
        4 => (
            b"set-max-listpack-entries".as_slice(),
            b"0".as_slice(),
            b"hashtable".as_slice(),
        ),
        _ => unreachable!(),
    };
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", config, limit]),
        b"+OK\r\n"
    );
    if kind >= 3 {
        assert_eq!(
            client.command(&[b"CONFIG", b"SET", b"set-max-intset-entries", b"0"]),
            b"+OK\r\n"
        );
    }
    if kind == 3 {
        let mut set = SetLp::new();
        for member in members {
            assert!(SetOps::add(&mut set, member).unwrap());
        }
        let object = Robj::SetListpack(set);
        let mut payload = vec![persistence::type_byte(&object)];
        persistence::save_robj(&object, &mut payload);
        payload.extend_from_slice(&persistence::RDB_VERSION.to_le_bytes());
        let crc = persistence::crc64(0, &payload);
        payload.extend_from_slice(&crc.to_le_bytes());
        assert_eq!(
            client.command(&[b"RESTORE", key, b"0", &payload]),
            b"+OK\r\n"
        );
    } else {
        let mut command = vec![
            if kind < 2 {
                b"ZADD".as_slice()
            } else {
                b"SADD".as_slice()
            },
            key,
        ];
        for member in members {
            if kind < 2 {
                command.push(b"0");
            }
            command.push(member);
        }
        assert_eq!(
            client.command(&command),
            format!(":{}\r\n", members.len()).as_bytes()
        );
    }
    assert_eq!(
        client.command(&[b"OBJECT", b"ENCODING", key]),
        format!(
            "${}\r\n{}\r\n",
            encoding.len(),
            std::str::from_utf8(encoding).unwrap()
        )
        .as_bytes()
    );
}

#[test]
fn limit_counts_only_members_shared_by_all_sources() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    for kind in 0..2 {
        client.command(&[b"DEL", b"a", b"b", b"c"]);
        add_source(&mut client, b"a", kind, &[b"a", b"b", b"c"]);
        add_source(&mut client, b"b", kind, &[b"a", b"b", b"d"]);
        add_source(&mut client, b"c", kind, &[b"a", b"c", b"d"]);
        for keys in [
            [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()],
            [b"c".as_slice(), b"b".as_slice(), b"a".as_slice()],
            [b"b".as_slice(), b"a".as_slice(), b"c".as_slice()],
        ] {
            assert_eq!(
                client.command(&[
                    b"ZINTERCARD",
                    b"3",
                    keys[0],
                    keys[1],
                    keys[2],
                    b"LIMIT",
                    b"2"
                ]),
                b":1\r\n",
                "kind: {kind}, keys: {keys:?}"
            );
        }
    }
}

#[test]
fn intersection_supports_all_source_encoding_pairs() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    for first_kind in 0..5 {
        for second_kind in 0..5 {
            client.command(&[b"DEL", b"first", b"second"]);
            add_source(&mut client, b"first", first_kind, &[b"1", b"2", b"3"]);
            add_source(
                &mut client,
                b"second",
                second_kind,
                &[b"2", b"3", b"4", b"5"],
            );
            for (limit, count) in [
                (b"0".as_slice(), b":2\r\n".as_slice()),
                (b"1", b":1\r\n"),
                (b"2", b":2\r\n"),
                (b"10", b":2\r\n"),
            ] {
                assert_eq!(
                    client.command(&[b"ZINTERCARD", b"2", b"first", b"second", b"LIMIT", limit]),
                    count,
                    "kinds: {first_kind}, {second_kind}, limit: {limit:?}"
                );
            }
            assert_eq!(
                client.command(&[b"ZINTERCARD", b"2", b"first", b"second"]),
                b":2\r\n"
            );
            assert_eq!(
                client.command(&[b"ZINTERCARD", b"2", b"second", b"first"]),
                b":2\r\n"
            );
            assert_eq!(
                client.command(&[b"ZINTERCARD", b"1", b"first", b"LIMIT", b"2"]),
                b":2\r\n"
            );
            assert_eq!(
                client.command(&[b"ZINTERCARD", b"2", b"first", b"first"]),
                b":3\r\n"
            );
        }
    }
}

#[test]
fn integer_members_use_exact_byte_identity() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    add_source(
        &mut client,
        b"integers",
        2,
        &[b"0", b"1", b"-9223372036854775808", b"9223372036854775807"],
    );
    for kind in [0, 1, 3, 4] {
        client.command(&[b"DEL", b"aliases"]);
        add_source(
            &mut client,
            b"aliases",
            kind,
            &[b"01", b"+1", b"-0", b"9223372036854775808"],
        );
        assert_eq!(
            client.command(&[b"ZINTERCARD", b"2", b"aliases", b"integers"]),
            b":0\r\n"
        );
    }
}

#[test]
fn binary_members_and_disjoint_sources_preserve_identity() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    add_source(&mut client, b"compact", 0, &[b"", b"\0\xff", b"a"]);
    add_source(&mut client, b"tree", 1, &[b"", b"\0\xff", b"b"]);
    add_source(&mut client, b"set", 3, &[b"", b"\0\xff", b"c"]);
    assert_eq!(
        client.command(&[
            b"ZINTERCARD",
            b"3",
            b"compact",
            b"tree",
            b"set",
            b"LIMIT",
            b"10"
        ]),
        b":2\r\n"
    );
    add_source(&mut client, b"disjoint", 4, &[b"d", b"e", b"f"]);
    assert_eq!(
        client.command(&[
            b"ZINTERCARD",
            b"4",
            b"compact",
            b"tree",
            b"set",
            b"disjoint",
            b"LIMIT",
            b"1"
        ]),
        b":0\r\n"
    );
}

#[test]
fn input_validation_preserves_missing_and_wrongtype_order() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    add_source(&mut client, b"first", 0, &[b"a", b"b"]);
    add_source(&mut client, b"second", 1, &[b"a", b"b"]);
    assert_eq!(client.command(&[b"SET", b"wrong", b"value"]), b"+OK\r\n");
    let wrong_type = b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    assert_eq!(
        client.command(&[b"ZINTERCARD", b"2", b"missing", b"wrong"]),
        b":0\r\n"
    );
    assert_eq!(
        client.command(&[b"ZINTERCARD", b"2", b"wrong", b"missing"]),
        wrong_type
    );
    assert_eq!(
        client.command(&[
            b"ZINTERCARD",
            b"3",
            b"first",
            b"second",
            b"wrong",
            b"LIMIT",
            b"1"
        ]),
        wrong_type
    );
    assert_eq!(
        client.command(&[
            b"ZINTERCARD",
            b"3",
            b"first",
            b"second",
            b"missing",
            b"LIMIT",
            b"1"
        ]),
        b":0\r\n"
    );
    assert_eq!(client.command(&[b"PEXPIREAT", b"first", b"1"]), b":1\r\n");
    assert_eq!(
        client.command(&[b"ZINTERCARD", b"2", b"first", b"wrong"]),
        b":0\r\n"
    );
    assert_eq!(client.command(&[b"EXISTS", b"first"]), b":0\r\n");
}

#[test]
fn syntax_and_limit_validation_precede_database_lookup() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    let syntax = b"-ERR syntax error\r\n".as_slice();
    for (args, expected) in [
        (
            vec![b"ZINTERCARD".as_slice(), b"0", b"missing"],
            b"-ERR at least 1 input key is needed for 'zintercard' command\r\n".as_slice(),
        ),
        (vec![b"ZINTERCARD".as_slice(), b"2", b"missing"], syntax),
        (
            vec![b"ZINTERCARD".as_slice(), b"1", b"missing", b"LIMIT"],
            syntax,
        ),
        (
            vec![b"ZINTERCARD".as_slice(), b"1", b"missing", b"WEIGHTS", b"1"],
            syntax,
        ),
        (
            vec![b"ZINTERCARD".as_slice(), b"1", b"missing", b"LIMIT", b"-1"],
            b"-ERR LIMIT can't be negative\r\n".as_slice(),
        ),
        (
            vec![b"ZINTERCARD".as_slice(), b"1", b"missing", b"LIMIT", b"x"],
            b"-ERR LIMIT value is not an integer or out of range\r\n".as_slice(),
        ),
        (
            vec![
                b"ZINTERCARD".as_slice(),
                b"1",
                b"missing",
                b"LIMIT",
                b"9223372036854775808",
            ],
            b"-ERR LIMIT value is not an integer or out of range\r\n".as_slice(),
        ),
    ] {
        assert_eq!(client.command(&args), expected, "args: {args:?}");
    }
    add_source(&mut client, b"source", 0, &[b"a", b"b", b"c"]);
    assert_eq!(
        client.command(&[
            b"ZINTERCARD",
            b"1",
            b"source",
            b"lImIt",
            b"1",
            b"LIMIT",
            b"0"
        ]),
        b":3\r\n"
    );
}
