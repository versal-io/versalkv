use crate::common::TestServer;

fn extract_bulk(reply: &[u8]) -> Vec<u8> {
    assert_eq!(reply[0], b'$', "expected bulk reply, got: {:?}", reply);
    let crlf = reply
        .windows(2)
        .position(|w| w == b"\r\n")
        .expect("no CRLF in bulk header");
    let len: usize = std::str::from_utf8(&reply[1..crlf])
        .unwrap()
        .parse()
        .unwrap();
    let start = crlf + 2;
    reply[start..start + len].to_vec()
}

#[test]
fn dump_restore_string() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SET", b"src", b"hello world"]);
    let payload = extract_bulk(&c.command(&[b"DUMP", b"src"]));
    assert_eq!(c.command(&[b"RESTORE", b"dst", b"0", &payload]), b"+OK\r\n");
    assert_eq!(c.command(&[b"GET", b"dst"]), b"$11\r\nhello world\r\n");
    assert_eq!(c.command(&[b"TYPE", b"dst"]), b"+string\r\n");
}

#[test]
fn dump_restore_string_integer_encoding() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SET", b"src", b"12345"]);
    let payload = extract_bulk(&c.command(&[b"DUMP", b"src"]));
    assert_eq!(c.command(&[b"RESTORE", b"dst", b"0", &payload]), b"+OK\r\n");
    assert_eq!(c.command(&[b"GET", b"dst"]), b"$5\r\n12345\r\n");
}

#[test]
fn dump_restore_list_preserves_order() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"RPUSH", b"src", b"a", b"b", b"c"]);
    let payload = extract_bulk(&c.command(&[b"DUMP", b"src"]));
    assert_eq!(c.command(&[b"RESTORE", b"dst", b"0", &payload]), b"+OK\r\n");
    assert_eq!(c.command(&[b"TYPE", b"dst"]), b"+list\r\n");
    assert_eq!(
        c.command(&[b"LRANGE", b"dst", b"0", b"-1"]),
        b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
    );
}

#[test]
fn dump_restore_set() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SADD", b"src", b"x", b"y", b"z"]);
    let payload = extract_bulk(&c.command(&[b"DUMP", b"src"]));
    assert_eq!(c.command(&[b"RESTORE", b"dst", b"0", &payload]), b"+OK\r\n");
    assert_eq!(c.command(&[b"TYPE", b"dst"]), b"+set\r\n");
    assert_eq!(c.command(&[b"SCARD", b"dst"]), b":3\r\n");
    for m in [&b"x"[..], b"y", b"z"] {
        assert_eq!(c.command(&[b"SISMEMBER", b"dst", m]), b":1\r\n");
    }
}

#[test]
fn dump_restore_hash() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"HSET", b"src", b"f1", b"v1", b"f2", b"v2"]);
    let payload = extract_bulk(&c.command(&[b"DUMP", b"src"]));
    assert_eq!(c.command(&[b"RESTORE", b"dst", b"0", &payload]), b"+OK\r\n");
    assert_eq!(c.command(&[b"TYPE", b"dst"]), b"+hash\r\n");
    assert_eq!(c.command(&[b"HLEN", b"dst"]), b":2\r\n");
    assert_eq!(c.command(&[b"HGET", b"dst", b"f1"]), b"$2\r\nv1\r\n");
    assert_eq!(c.command(&[b"HGET", b"dst", b"f2"]), b"$2\r\nv2\r\n");
}

#[test]
fn dump_restore_hash_preserves_field_ttl_memory_usage() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(
        c.command(&[b"HSET", b"src{t}", b"f1", b"v1", b"f2", b"v2"]),
        b":2\r\n"
    );
    assert_eq!(
        c.command(&[b"HEXPIRE", b"src{t}", b"200", b"FIELDS", b"1", b"f1"]),
        b"*1\r\n:1\r\n"
    );
    let memory_before = c.command(&[b"MEMORY", b"USAGE", b"src{t}"]);
    let payload = extract_bulk(&c.command(&[b"DUMP", b"src{t}"]));

    assert_eq!(
        c.command(&[b"RESTORE", b"dst{t}", b"0", &payload]),
        b"+OK\r\n"
    );
    assert_eq!(c.command(&[b"MEMORY", b"USAGE", b"dst{t}"]), memory_before);
    assert_ne!(
        c.command(&[b"HPTTL", b"dst{t}", b"FIELDS", b"1", b"f1"]),
        b"*1\r\n:-1\r\n"
    );
}

#[test]
fn copy_hash_preserves_field_ttl_memory_usage() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(
        c.command(&[b"HSET", b"src{t}", b"f1", b"v1", b"f2", b"v2", b"f3", b"v3",]),
        b":3\r\n"
    );
    assert_eq!(
        c.command(&[b"HEXPIRE", b"src{t}", b"200", b"FIELDS", b"1", b"f1"]),
        b"*1\r\n:1\r\n"
    );
    let memory_before = c.command(&[b"MEMORY", b"USAGE", b"src{t}"]);

    assert_eq!(c.command(&[b"COPY", b"src{t}", b"dst{t}"]), b":1\r\n");
    assert_eq!(c.command(&[b"MEMORY", b"USAGE", b"dst{t}"]), memory_before);
}

#[test]
fn dump_restore_zset_preserves_scores_and_order() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"ZADD", b"src", b"1.5", b"a", b"2", b"b", b"1.5", b"c"]);
    let payload = extract_bulk(&c.command(&[b"DUMP", b"src"]));
    assert_eq!(c.command(&[b"RESTORE", b"dst", b"0", &payload]), b"+OK\r\n");
    assert_eq!(c.command(&[b"TYPE", b"dst"]), b"+zset\r\n");
    assert_eq!(
        c.command(&[b"ZRANGE", b"dst", b"0", b"-1", b"WITHSCORES"]),
        b"*6\r\n$1\r\na\r\n$3\r\n1.5\r\n$1\r\nc\r\n$3\r\n1.5\r\n$1\r\nb\r\n$1\r\n2\r\n"
    );
}

#[test]
fn restore_rejects_existing_key_without_replace() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SET", b"src", b"val"]);
    let payload = extract_bulk(&c.command(&[b"DUMP", b"src"]));
    c.command(&[b"SET", b"dst", b"other"]);
    assert!(c
        .command_str(&[b"RESTORE", b"dst", b"0", &payload])
        .contains("BUSYKEY"));
    assert_eq!(
        c.command(&[b"RESTORE", b"dst", b"0", &payload, b"REPLACE"]),
        b"+OK\r\n"
    );
    assert_eq!(c.command(&[b"GET", b"dst"]), b"$3\r\nval\r\n");
}

#[test]
fn restore_rejects_corrupt_payload() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SET", b"src", b"val"]);
    let mut payload = extract_bulk(&c.command(&[b"DUMP", b"src"]));
    let last = payload.len() - 1;
    payload[last] ^= 0xff;
    assert!(c
        .command_str(&[b"RESTORE", b"dst", b"0", &payload])
        .contains("checksum"));
}
