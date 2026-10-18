use crate::common::TestServer;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// ---------- List ----------

#[test]
fn list_encoding_recomputed_after_rdb_reload() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();

    assert_eq!(
        c.command(&[b"CONFIG", b"SET", b"list-max-listpack-size", b"3"]),
        b"+OK\r\n"
    );
    assert_eq!(c.command(&[b"RPUSH", b"lst", b"a", b"b", b"c"]), b":3\r\n");
    assert_eq!(c.command(&[b"DEBUG", b"RELOAD"]), b"+OK\r\n");
    assert_eq!(
        c.command(&[b"OBJECT", b"ENCODING", b"lst"]),
        b"$8\r\nlistpack\r\n"
    );

    assert_eq!(c.command(&[b"RPUSH", b"lst", b"d"]), b":4\r\n");
    assert_eq!(c.command(&[b"DEBUG", b"RELOAD"]), b"+OK\r\n");
    assert_eq!(
        c.command(&[b"OBJECT", b"ENCODING", b"lst"]),
        b"$9\r\nquicklist\r\n"
    );

    assert_eq!(c.command(&[b"RPOP", b"lst"]), b"$1\r\nd\r\n");
    assert_eq!(
        c.command(&[b"OBJECT", b"ENCODING", b"lst"]),
        b"$9\r\nquicklist\r\n"
    );
    assert_eq!(c.command(&[b"DEBUG", b"RELOAD"]), b"+OK\r\n");
    assert_eq!(
        c.command(&[b"OBJECT", b"ENCODING", b"lst"]),
        b"$8\r\nlistpack\r\n"
    );
}

// ---------- Hash ----------

#[test]
fn hset_hget_basic() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(
        c.command(&[b"HSET", b"h", b"f1", b"v1", b"f2", b"v2"]),
        b":2\r\n"
    );
    assert_eq!(c.command(&[b"HGET", b"h", b"f1"]), b"$2\r\nv1\r\n");
    assert_eq!(c.command(&[b"HGET", b"h", b"f2"]), b"$2\r\nv2\r\n");
    assert_eq!(c.command(&[b"HSET", b"h", b"f1", b"v1b"]), b":0\r\n");
    assert_eq!(c.command(&[b"HGET", b"h", b"f1"]), b"$3\r\nv1b\r\n");
    assert_eq!(c.command(&[b"HLEN", b"h"]), b":2\r\n");
}

#[test]
fn hash_listpack_hgetall_handles_32bit_string_encoding() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(
        c.command(&[b"CONFIG", b"SET", b"hash-max-listpack-entries", b"100"]),
        b"+OK\r\n"
    );
    assert_eq!(
        c.command(&[b"CONFIG", b"SET", b"hash-max-listpack-value", b"1000000",]),
        b"+OK\r\n"
    );
    let medium = vec![b'x'; 8_191];
    let large = vec![b'x'; 65_535];
    for (field, value) in [
        (b"ZIP_INT_8B" as &[u8], b"127" as &[u8]),
        (b"ZIP_INT_16B", b"32767"),
        (b"ZIP_INT_32B", b"2147483647"),
        (b"ZIP_INT_64B", b"9223372036854775808"),
        (b"ZIP_INT_IMM_MIN", b"0"),
        (b"ZIP_INT_IMM_MAX", b"12"),
        (b"ZIP_STR_06B", b"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"),
        (b"ZIP_STR_14B", medium.as_slice()),
        (b"ZIP_STR_32B", large.as_slice()),
    ] {
        assert_eq!(c.command(&[b"HSET", b"hash", field, value]), b":1\r\n");
    }

    let reply = c.command(&[b"HGETALL", b"hash"]);
    assert!(reply.starts_with(b"*18\r\n"));
    assert!(reply
        .windows(b"ZIP_STR_32B\r\n$65535\r\n".len())
        .any(|window| window == b"ZIP_STR_32B\r\n$65535\r\n"));
    assert!(reply.ends_with(b"\r\n"));
}

#[test]
fn hget_missing_returns_null() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(c.command(&[b"HGET", b"nope", b"f"]), b"$-1\r\n");
    c.command(&[b"HSET", b"h", b"f", b"v"]);
    assert_eq!(c.command(&[b"HGET", b"h", b"missing"]), b"$-1\r\n");
}

#[test]
fn hdel_removes_and_empties_key() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"HSET", b"h", b"a", b"1", b"b", b"2"]);
    assert_eq!(c.command(&[b"HDEL", b"h", b"a", b"nope"]), b":1\r\n");
    assert_eq!(c.command(&[b"HLEN", b"h"]), b":1\r\n");
    assert_eq!(c.command(&[b"HDEL", b"h", b"b"]), b":1\r\n");
    assert_eq!(c.command(&[b"TYPE", b"h"]), b"+none\r\n");
}

#[test]
fn hgetall_returns_all_pairs() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"HSET", b"h", b"a", b"1", b"b", b"2"]);
    let reply = c.command(&[b"HGETALL", b"h"]);
    assert!(reply.starts_with(b"*4\r\n"), "got: {:?}", reply);
    for token in [&b"a"[..], b"1", b"b", b"2"] {
        let needle = [format!("${}\r\n", token.len()).as_bytes(), token, b"\r\n"].concat();
        assert!(
            reply.windows(needle.len()).any(|w| w == needle.as_slice()),
            "missing {:?} in {:?}",
            token,
            reply
        );
    }
}

#[test]
fn hash_wrongtype_against_string() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SET", b"s", b"x"]);
    assert!(c
        .command_str(&[b"HSET", b"s", b"f", b"v"])
        .contains("WRONGTYPE"));
    assert!(c.command_str(&[b"HGET", b"s", b"f"]).contains("WRONGTYPE"));
}

#[test]
fn hscan_skips_expired_hash_fields() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();

    assert_eq!(
        c.command(&[
            b"HSET",
            b"h",
            b"expired",
            b"gone",
            b"persistent1",
            b"one",
            b"persistent2",
            b"two",
        ]),
        b":3\r\n"
    );
    assert_eq!(
        c.command(&[b"HPEXPIRE", b"h", b"1", b"FIELDS", b"1", b"expired"]),
        b"*1\r\n:1\r\n"
    );
    std::thread::sleep(Duration::from_millis(20));

    let reply = c.command_str(&[b"HSCAN", b"h", b"0"]);
    assert!(reply.contains("persistent1"), "got {reply:?}");
    assert!(reply.contains("persistent2"), "got {reply:?}");
    assert!(!reply.contains("expired"), "got {reply:?}");
}

#[test]
fn hrandfield_skips_expired_hash_fields() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();

    assert_eq!(
        c.command(&[b"HSET", b"h", b"a", b"1", b"b", b"2"]),
        b":2\r\n"
    );
    assert_eq!(
        c.command(&[b"HPEXPIRE", b"h", b"1", b"FIELDS", b"1", b"a"]),
        b"*1\r\n:1\r\n"
    );
    std::thread::sleep(Duration::from_millis(20));

    let reply = c.command_str(&[b"HRANDFIELD", b"h", b"2"]);
    assert!(reply.contains("$1\r\nb\r\n"), "got {reply:?}");
    assert!(!reply.contains("$1\r\na\r\n"), "got {reply:?}");
}

#[test]
fn hash_writes_treat_expired_fields_as_absent() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();

    assert_eq!(
        c.command(&[b"HSET", b"h", b"i", b"5", b"f", b"1.5", b"d", b"old"]),
        b":3\r\n"
    );
    assert_eq!(
        c.command(&[b"HPEXPIRE", b"h", b"1", b"FIELDS", b"3", b"i", b"f", b"d"]),
        b"*3\r\n:1\r\n:1\r\n:1\r\n"
    );
    std::thread::sleep(Duration::from_millis(20));

    assert_eq!(c.command(&[b"HINCRBY", b"h", b"i", b"1"]), b":1\r\n");
    assert_eq!(
        c.command(&[b"HINCRBYFLOAT", b"h", b"f", b"1"]),
        b"$1\r\n1\r\n"
    );
    assert_eq!(c.command(&[b"HDEL", b"h", b"d"]), b":0\r\n");
    assert_eq!(
        c.command(&[b"HPTTL", b"h", b"FIELDS", b"3", b"i", b"f", b"d"]),
        b"*3\r\n:-1\r\n:-1\r\n:-2\r\n"
    );
}

#[test]
fn rename_and_move_preserve_hash_field_ttls() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();

    assert_eq!(c.command(&[b"HSET", b"src", b"f", b"v"]), b":1\r\n");
    assert_eq!(
        c.command(&[b"HPEXPIRE", b"src", b"50000", b"FIELDS", b"1", b"f"]),
        b"*1\r\n:1\r\n"
    );
    assert_eq!(c.command(&[b"RENAME", b"src", b"renamed"]), b"+OK\r\n");
    let renamed_ttl = c.command_str(&[b"HPTTL", b"renamed", b"FIELDS", b"1", b"f"]);
    assert!(
        renamed_ttl.starts_with("*1\r\n:")
            && !renamed_ttl.contains(":-1")
            && !renamed_ttl.contains(":-2"),
        "got {renamed_ttl:?}"
    );

    assert_eq!(c.command(&[b"MOVE", b"renamed", b"1"]), b":1\r\n");
    assert_eq!(c.command(&[b"SELECT", b"1"]), b"+OK\r\n");
    let moved_ttl = c.command_str(&[b"HPTTL", b"renamed", b"FIELDS", b"1", b"f"]);
    assert!(
        moved_ttl.starts_with("*1\r\n:")
            && !moved_ttl.contains(":-1")
            && !moved_ttl.contains(":-2"),
        "got {moved_ttl:?}"
    );
}

#[test]
fn dump_restore_preserves_hash_field_ttls() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();

    assert_eq!(c.command(&[b"HSET", b"h", b"f", b"v"]), b":1\r\n");
    assert_eq!(
        c.command(&[b"HPEXPIRE", b"h", b"50000", b"FIELDS", b"1", b"f"]),
        b"*1\r\n:1\r\n"
    );
    let dump = c.command(&[b"DUMP", b"h"]);
    assert!(dump.starts_with(b"$"), "got {dump:?}");
    assert_eq!(
        c.command(&[b"RESTORE", b"copy", b"0", dump_payload(&dump)]),
        b"+OK\r\n"
    );

    let ttl = c.command_str(&[b"HPTTL", b"copy", b"FIELDS", b"1", b"f"]);
    assert!(
        ttl.starts_with("*1\r\n:") && !ttl.contains(":-1") && !ttl.contains(":-2"),
        "got {ttl:?}"
    );
}

#[test]
fn rdb_load_skips_hash_when_all_fields_expired() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();

    assert_eq!(c.command(&[b"HSET", b"h", b"f", b"v"]), b":1\r\n");
    let expire_at_ms = now_ms() + 1_000;
    let expire_at = expire_at_ms.to_string();
    assert_eq!(
        c.command(&[
            b"HPEXPIREAT",
            b"h",
            expire_at.as_bytes(),
            b"FIELDS",
            b"1",
            b"f",
        ]),
        b"*1\r\n:1\r\n"
    );
    assert_eq!(c.command(&[b"SAVE"]), b"+OK\r\n");
    sleep_until_after_ms(expire_at_ms);
    assert_eq!(c.command(&[b"DEBUG", b"RELOAD", b"NOSAVE"]), b"+OK\r\n");

    assert_eq!(c.command(&[b"EXISTS", b"h"]), b":0\r\n");
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before Unix epoch")
        .as_millis() as u64
}

fn sleep_until_after_ms(deadline_ms: u64) {
    let remaining_ms = deadline_ms.saturating_sub(now_ms());
    std::thread::sleep(Duration::from_millis(remaining_ms + 50));
}

#[test]
fn xadd_auto_id_is_deterministic_in_aof_replay() {
    let srv = TestServer::start(&["--appendonly"]);
    let mut c = srv.connect();

    for _ in 0..100 {
        assert!(c
            .command(&[b"XADD", b"mystream", b"*", b"field", b"value"])
            .starts_with(b"$"));
    }

    assert!(c
        .command(&[
            b"XADD",
            b"mystream",
            b"MAXLEN",
            b"200",
            b"*",
            b"field",
            b"value",
        ])
        .starts_with(b"$"));
    assert_eq!(c.command(&[b"XLEN", b"mystream"]), b":101\r\n");
    assert_eq!(c.command(&[b"DEBUG", b"LOADAOF"]), b"+OK\r\n");
    assert_eq!(c.command(&[b"XLEN", b"mystream"]), b":101\r\n");
}

#[test]
fn xsetid_allows_last_id_equal_to_max_deleted_id() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();

    assert_eq!(
        c.command(&[b"XADD", b"mystream", b"1-1", b"a", b"b"]),
        b"$3\r\n1-1\r\n"
    );
    assert_eq!(
        c.command(&[b"XADD", b"mystream", b"2-2", b"a", b"b"]),
        b"$3\r\n2-2\r\n"
    );
    assert_eq!(c.command(&[b"XDEL", b"mystream", b"2-2"]), b":1\r\n");
    assert_eq!(
        c.command(&[
            b"XSETID",
            b"mystream",
            b"2-2",
            b"ENTRIESADDED",
            b"2",
            b"MAXDELETEDID",
            b"2-2",
        ]),
        b"+OK\r\n"
    );

    let info = c.command_str(&[b"XINFO", b"STREAM", b"mystream"]);
    assert!(
        info.contains("$17\r\nlast-generated-id\r\n$3\r\n2-2\r\n"),
        "{info}"
    );
    assert!(
        info.contains("$20\r\nmax-deleted-entry-id\r\n$3\r\n2-2\r\n"),
        "{info}"
    );
}

#[test]
fn bgrewriteaof_preserves_stream_last_id_after_last_entry_deleted() {
    let srv = TestServer::start(&["--appendonly"]);
    let mut c = srv.connect();

    assert_eq!(
        c.command(&[b"CONFIG", b"SET", b"aof-use-rdb-preamble", b"no"]),
        b"+OK\r\n"
    );
    assert!(c
        .command(&[b"XADD", b"mystream", b"MAXLEN", b"0", b"*", b"a", b"b"])
        .starts_with(b"$"));
    assert_eq!(c.command(&[b"XSETID", b"mystream", b"0-0"]), b"+OK\r\n");
    assert_eq!(
        c.command(&[b"XADD", b"mystream", b"1-1", b"a", b"b"]),
        b"$3\r\n1-1\r\n"
    );
    assert_eq!(
        c.command(&[b"XADD", b"mystream", b"2-2", b"a", b"b"]),
        b"$3\r\n2-2\r\n"
    );
    assert_eq!(c.command(&[b"XDEL", b"mystream", b"2-2"]), b":1\r\n");
    assert!(c.command(&[b"BGREWRITEAOF"]).starts_with(b"+Background"));
    wait_for_bgrewriteaof(&mut c);
    assert_eq!(c.command(&[b"DEBUG", b"LOADAOF"]), b"+OK\r\n");

    let info = c.command_str(&[b"XINFO", b"STREAM", b"mystream"]);
    assert!(info.contains("$6\r\nlength\r\n:1\r\n"), "{info}");
    assert!(
        info.contains("$17\r\nlast-generated-id\r\n$3\r\n2-2\r\n"),
        "{info}"
    );
}

#[test]
fn bgrewriteaof_preserves_empty_stream_consumer_group() {
    let srv = TestServer::start(&["--appendonly"]);
    let mut c = srv.connect();

    assert_eq!(
        c.command(&[b"CONFIG", b"SET", b"aof-use-rdb-preamble", b"no"]),
        b"+OK\r\n"
    );
    assert_eq!(
        c.command(&[
            b"XGROUP",
            b"CREATE",
            b"mystream",
            b"group-name",
            b"$",
            b"MKSTREAM",
        ]),
        b"+OK\r\n"
    );
    let groups_before = c.command(&[b"XINFO", b"GROUPS", b"mystream"]);
    assert!(c.command(&[b"BGREWRITEAOF"]).starts_with(b"+Background"));
    wait_for_bgrewriteaof(&mut c);
    assert_eq!(c.command(&[b"DEBUG", b"LOADAOF"]), b"+OK\r\n");

    assert_eq!(c.command(&[b"XLEN", b"mystream"]), b":0\r\n");
    assert_eq!(
        c.command(&[b"XINFO", b"GROUPS", b"mystream"]),
        groups_before
    );
}

#[test]
fn bgrewriteaof_preserves_stream_consumers_without_pel() {
    let srv = TestServer::start(&["--appendonly"]);
    let mut c = srv.connect();

    assert_eq!(
        c.command(&[b"CONFIG", b"SET", b"aof-use-rdb-preamble", b"no"]),
        b"+OK\r\n"
    );
    assert_eq!(
        c.command(&[
            b"XGROUP",
            b"CREATE",
            b"mystream",
            b"mygroup",
            b"$",
            b"MKSTREAM",
        ]),
        b"+OK\r\n"
    );
    assert!(c
        .command(&[b"XADD", b"mystream", b"*", b"f", b"v"])
        .starts_with(b"$"));
    assert!(c
        .command(&[
            b"XREADGROUP",
            b"GROUP",
            b"mygroup",
            b"Alice",
            b"NOACK",
            b"STREAMS",
            b"mystream",
            b">",
        ])
        .starts_with(b"*"));
    assert_eq!(
        c.command(&[
            b"XGROUP",
            b"CREATECONSUMER",
            b"mystream",
            b"mygroup",
            b"Charlie",
        ]),
        b":1\r\n"
    );
    let groups_before = c.command(&[b"XINFO", b"GROUPS", b"mystream"]);
    assert!(groups_before
        .windows(b"consumers".len())
        .any(|w| w == b"consumers"));
    assert!(c.command(&[b"BGREWRITEAOF"]).starts_with(b"+Background"));
    wait_for_bgrewriteaof(&mut c);
    assert_eq!(c.command(&[b"DEBUG", b"LOADAOF"]), b"+OK\r\n");

    assert_eq!(
        c.command(&[b"XINFO", b"GROUPS", b"mystream"]),
        groups_before
    );
    let consumers = c.command_str(&[b"XINFO", b"CONSUMERS", b"mystream", b"mygroup"]);
    assert!(consumers.contains("Alice"), "{consumers}");
    assert!(consumers.contains("Charlie"), "{consumers}");
}

fn wait_for_bgrewriteaof(c: &mut crate::common::Client) {
    // AOF rewrite competes for CPU/IO under full-suite parallelism with
    // several other persistence tests; 5s is flaky on loaded machines. Use a
    // generous-but-bounded 30s total window with 20ms ticks so true hangs
    // still surface quickly.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while std::time::Instant::now() < deadline {
        let info = c.command_str(&[b"INFO", b"PERSISTENCE"]);
        if info.contains("aof_rewrite_in_progress:0") {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("BGREWRITEAOF did not finish");
}

// ---------- Set ----------

fn dump_payload(reply: &[u8]) -> &[u8] {
    let crlf = reply
        .windows(2)
        .position(|w| w == b"\r\n")
        .expect("bulk length line");
    let len: usize = std::str::from_utf8(&reply[1..crlf])
        .expect("bulk length utf8")
        .parse()
        .expect("bulk length number");
    let start = crlf + 2;
    let end = start + len;
    assert_eq!(&reply[end..end + 2], b"\r\n");
    &reply[start..end]
}

#[test]
fn sadd_scard_sismember() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(c.command(&[b"SADD", b"s", b"a", b"b", b"c"]), b":3\r\n");
    assert_eq!(c.command(&[b"SADD", b"s", b"a", b"d"]), b":1\r\n");
    assert_eq!(c.command(&[b"SCARD", b"s"]), b":4\r\n");
    assert_eq!(c.command(&[b"SISMEMBER", b"s", b"a"]), b":1\r\n");
    assert_eq!(c.command(&[b"SISMEMBER", b"s", b"z"]), b":0\r\n");
    assert_eq!(c.command(&[b"TYPE", b"s"]), b"+set\r\n");
}

#[test]
fn srem_empties_key() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SADD", b"s", b"a", b"b"]);
    assert_eq!(c.command(&[b"SREM", b"s", b"a", b"nope"]), b":1\r\n");
    assert_eq!(c.command(&[b"SREM", b"s", b"b"]), b":1\r\n");
    assert_eq!(c.command(&[b"TYPE", b"s"]), b"+none\r\n");
    assert_eq!(c.command(&[b"SCARD", b"s"]), b":0\r\n");
}

#[test]
fn smembers_returns_all() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SADD", b"s", b"x", b"y"]);
    let reply = c.command(&[b"SMEMBERS", b"s"]);
    assert!(reply.starts_with(b"*2\r\n"), "got: {:?}", reply);
    for token in [&b"x"[..], b"y"] {
        let needle = [format!("${}\r\n", token.len()).as_bytes(), token, b"\r\n"].concat();
        assert!(
            reply.windows(needle.len()).any(|w| w == needle.as_slice()),
            "missing {:?} in {:?}",
            token,
            reply
        );
    }
}

#[test]
fn set_wrongtype_against_string() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SET", b"k", b"x"]);
    assert!(c.command_str(&[b"SADD", b"k", b"a"]).contains("WRONGTYPE"));
    assert!(c.command_str(&[b"SMEMBERS", b"k"]).contains("WRONGTYPE"));
}

#[test]
fn sadd_512_elements_at_once() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();

    let mut parts: Vec<Vec<u8>> = Vec::new();
    parts.push(b"SADD".to_vec());
    parts.push(b"myset".to_vec());
    for i in 0..512 {
        parts.push(i.to_string().as_bytes().to_vec());
    }

    let parts_refs: Vec<&[u8]> = parts.iter().map(|p| p.as_slice()).collect();

    let reply = c.command(&parts_refs);
    assert!(
        reply.starts_with(b":512\r\n"),
        "expected :512, got {:?}",
        String::from_utf8_lossy(&reply)
    );

    assert_eq!(c.command(&[b"SCARD", b"myset"]), b":512\r\n");
}

#[test]
fn sadd_512_elements_one_by_one() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();

    for i in 0..512 {
        let istr = i.to_string();
        let reply = c.command(&[b"SADD", b"myset", istr.as_bytes()]);
        assert!(
            reply.starts_with(b":1\r\n"),
            "i={}, reply={:?}",
            i,
            String::from_utf8_lossy(&reply)
        );
    }

    assert_eq!(c.command(&[b"SCARD", b"myset"]), b":512\r\n");
}

#[test]
fn set_intset_encoding_for_integer_members() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();

    assert_eq!(
        c.command(&[b"CONFIG", b"SET", b"set-max-intset-entries", b"32"]),
        b"+OK\r\n"
    );
    assert_eq!(
        c.command(&[b"SADD", b"s", b"1", b"2", b"3", b"100", b"-5"]),
        b":5\r\n"
    );
    assert_eq!(
        c.command(&[b"OBJECT", b"ENCODING", b"s"]),
        b"$6\r\nintset\r\n"
    );
    assert_eq!(c.command(&[b"SCARD", b"s"]), b":5\r\n");
    assert_eq!(c.command(&[b"SISMEMBER", b"s", b"100"]), b":1\r\n");
    assert_eq!(c.command(&[b"SISMEMBER", b"s", b"999"]), b":0\r\n");
}

#[test]
fn set_intset_survives_debug_reload_as_mutable_set() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();

    assert_eq!(c.command(&[b"SADD", b"s", b"1", b"2", b"3"]), b":3\r\n");
    assert_eq!(
        c.command(&[b"OBJECT", b"ENCODING", b"s"]),
        b"$6\r\nintset\r\n"
    );
    assert_eq!(c.command(&[b"DEBUG", b"RELOAD"]), b"+OK\r\n");

    assert_eq!(c.command(&[b"SADD", b"s", b"4"]), b":1\r\n");
    assert_eq!(c.command(&[b"SCARD", b"s"]), b":4\r\n");
    assert_eq!(c.command(&[b"SISMEMBER", b"s", b"1"]), b":1\r\n");
    assert_eq!(c.command(&[b"SISMEMBER", b"s", b"4"]), b":1\r\n");
    assert_eq!(
        c.command(&[b"OBJECT", b"ENCODING", b"s"]),
        b"$6\r\nintset\r\n"
    );
}

#[test]
fn set_upgrades_to_listpack_on_non_integer_small_member() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();

    assert_eq!(
        c.command(&[b"CONFIG", b"SET", b"set-max-intset-entries", b"32"]),
        b"+OK\r\n"
    );
    assert_eq!(
        c.command(&[b"CONFIG", b"SET", b"set-max-listpack-entries", b"32"]),
        b"+OK\r\n"
    );
    assert_eq!(c.command(&[b"SADD", b"s", b"1", b"2", b"3"]), b":3\r\n");
    assert_eq!(
        c.command(&[b"OBJECT", b"ENCODING", b"s"]),
        b"$6\r\nintset\r\n"
    );

    // Adding a small non-integer member upgrades intset → listpack.
    assert_eq!(c.command(&[b"SADD", b"s", b"hi"]), b":1\r\n");
    assert_eq!(
        c.command(&[b"OBJECT", b"ENCODING", b"s"]),
        b"$8\r\nlistpack\r\n"
    );
    assert_eq!(c.command(&[b"SCARD", b"s"]), b":4\r\n");
    assert_eq!(c.command(&[b"SISMEMBER", b"s", b"1"]), b":1\r\n");
    assert_eq!(c.command(&[b"SISMEMBER", b"s", b"hi"]), b":1\r\n");
}

#[test]
fn set_upgrades_to_hashtable_when_exceeding_max_intset_entries() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();

    assert_eq!(
        c.command(&[b"CONFIG", b"SET", b"set-max-intset-entries", b"5"]),
        b"+OK\r\n"
    );
    // 5 integer members — should still be intset.
    for i in 1..=5 {
        let istr = i.to_string();
        assert_eq!(c.command(&[b"SADD", b"s", istr.as_bytes()]), b":1\r\n");
    }
    assert_eq!(
        c.command(&[b"OBJECT", b"ENCODING", b"s"]),
        b"$6\r\nintset\r\n"
    );
    assert_eq!(c.command(&[b"SCARD", b"s"]), b":5\r\n");

    // 6th integer member exceeds intset threshold → upgrades to hashtable.
    assert_eq!(c.command(&[b"SADD", b"s", b"6"]), b":1\r\n");
    assert_eq!(
        c.command(&[b"OBJECT", b"ENCODING", b"s"]),
        b"$9\r\nhashtable\r\n"
    );
    assert_eq!(c.command(&[b"SCARD", b"s"]), b":6\r\n");
    for i in 1..=6 {
        let istr = i.to_string();
        assert_eq!(c.command(&[b"SISMEMBER", b"s", istr.as_bytes()]), b":1\r\n");
    }
}

#[test]
fn set_upgrades_listpack_to_hashtable_on_large_member() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();

    assert_eq!(
        c.command(&[b"CONFIG", b"SET", b"set-max-intset-entries", b"0"]),
        b"+OK\r\n"
    );
    assert_eq!(
        c.command(&[b"CONFIG", b"SET", b"set-max-listpack-entries", b"32"]),
        b"+OK\r\n"
    );
    assert_eq!(
        c.command(&[b"CONFIG", b"SET", b"set-max-listpack-value", b"8"]),
        b"+OK\r\n"
    );

    // Small non-integer members → listpack encoding.
    assert_eq!(
        c.command(&[b"SADD", b"s", b"abc", b"def", b"ghi"]),
        b":3\r\n"
    );
    assert_eq!(
        c.command(&[b"OBJECT", b"ENCODING", b"s"]),
        b"$8\r\nlistpack\r\n"
    );

    // Adding a member larger than max-listpack-value upgrades to hashtable.
    let big = b"this_is_a_long_value";
    assert_eq!(c.command(&[b"SADD", b"s", big]), b":1\r\n");
    assert_eq!(
        c.command(&[b"OBJECT", b"ENCODING", b"s"]),
        b"$9\r\nhashtable\r\n"
    );
    assert_eq!(c.command(&[b"SCARD", b"s"]), b":4\r\n");
    assert_eq!(c.command(&[b"SISMEMBER", b"s", b"abc"]), b":1\r\n");
    assert_eq!(c.command(&[b"SISMEMBER", b"s", big]), b":1\r\n");
}

// ---------- ZSet ----------

#[test]
fn zadd_zscore_zcard() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(
        c.command(&[b"ZADD", b"z", b"1", b"a", b"2", b"b"]),
        b":2\r\n"
    );
    assert_eq!(c.command(&[b"ZSCORE", b"z", b"a"]), b"$1\r\n1\r\n");
    assert_eq!(c.command(&[b"ZSCORE", b"z", b"b"]), b"$1\r\n2\r\n");
    assert_eq!(c.command(&[b"ZSCORE", b"z", b"missing"]), b"$-1\r\n");
    assert_eq!(c.command(&[b"ZCARD", b"z"]), b":2\r\n");
    assert_eq!(c.command(&[b"ZADD", b"z", b"5", b"a"]), b":0\r\n");
    assert_eq!(c.command(&[b"ZSCORE", b"z", b"a"]), b"$1\r\n5\r\n");
    assert_eq!(c.command(&[b"TYPE", b"z"]), b"+zset\r\n");
}

#[test]
fn zrange_is_sorted_by_score_then_member() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"ZADD", b"z", b"2", b"b", b"1", b"c", b"1", b"a"]);
    assert_eq!(
        c.command(&[b"ZRANGE", b"z", b"0", b"-1"]),
        b"*3\r\n$1\r\na\r\n$1\r\nc\r\n$1\r\nb\r\n"
    );
    // WITHSCORES
    assert_eq!(
        c.command(&[b"ZRANGE", b"z", b"0", b"-1", b"WITHSCORES"]),
        b"*6\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nc\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n"
    );
    assert_eq!(
        c.command(&[b"ZRANGE", b"z", b"-1", b"-1"]),
        b"*1\r\n$1\r\nb\r\n"
    );
}

#[test]
fn zrem_empties_key() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"ZADD", b"z", b"1", b"a", b"2", b"b"]);
    assert_eq!(c.command(&[b"ZREM", b"z", b"a", b"nope"]), b":1\r\n");
    assert_eq!(c.command(&[b"ZREM", b"z", b"b"]), b":1\r\n");
    assert_eq!(c.command(&[b"TYPE", b"z"]), b"+none\r\n");
}

#[test]
fn zadd_rejects_invalid_score() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert!(c
        .command_str(&[b"ZADD", b"z", b"notanumber", b"a"])
        .contains("not a valid float"));
    assert_eq!(c.command(&[b"ZCARD", b"z"]), b":0\r\n");
}

#[test]
fn zset_wrongtype_against_string() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SET", b"k", b"x"]);
    assert!(c
        .command_str(&[b"ZADD", b"k", b"1", b"a"])
        .contains("WRONGTYPE"));
    assert!(c
        .command_str(&[b"ZRANGE", b"k", b"0", b"-1"])
        .contains("WRONGTYPE"));
}

#[test]
fn zincrby_basic() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"ZADD", b"z", b"1", b"a"]);
    assert_eq!(c.command(&[b"ZINCRBY", b"z", b"2", b"a"]), b"$1\r\n3\r\n");
    assert_eq!(c.command(&[b"ZINCRBY", b"z", b"-1", b"a"]), b"$1\r\n2\r\n");
    assert_eq!(c.command(&[b"ZINCRBY", b"z", b"5", b"new"]), b"$1\r\n5\r\n");
    assert_eq!(c.command(&[b"ZCARD", b"z"]), b":2\r\n");
}

#[test]
fn zadd_nx_option() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(
        c.command(&[b"ZADD", b"z", b"NX", b"1", b"a", b"2", b"b"]),
        b":2\r\n"
    );
    assert_eq!(c.command(&[b"ZSCORE", b"z", b"a"]), b"$1\r\n1\r\n");
    assert_eq!(
        c.command(&[b"ZADD", b"z", b"NX", b"10", b"a", b"3", b"c"]),
        b":1\r\n"
    );
    assert_eq!(c.command(&[b"ZSCORE", b"z", b"a"]), b"$1\r\n1\r\n");
    assert_eq!(c.command(&[b"ZSCORE", b"z", b"c"]), b"$1\r\n3\r\n");
    assert_eq!(c.command(&[b"ZCARD", b"z"]), b":3\r\n");
}

#[test]
fn zadd_xx_option() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"ZADD", b"z", b"1", b"a", b"2", b"b"]);
    assert_eq!(
        c.command(&[b"ZADD", b"z", b"XX", b"10", b"a", b"3", b"c"]),
        b":0\r\n"
    );
    assert_eq!(c.command(&[b"ZSCORE", b"z", b"a"]), b"$2\r\n10\r\n");
    assert_eq!(c.command(&[b"ZSCORE", b"z", b"c"]), b"$-1\r\n");
    assert_eq!(c.command(&[b"ZCARD", b"z"]), b":2\r\n");
}

#[test]
fn zadd_ch_option() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(
        c.command(&[b"ZADD", b"z", b"CH", b"1", b"a", b"2", b"b"]),
        b":2\r\n"
    );
    assert_eq!(
        c.command(&[b"ZADD", b"z", b"CH", b"10", b"a", b"2", b"b"]),
        b":1\r\n"
    );
    assert_eq!(c.command(&[b"ZSCORE", b"z", b"a"]), b"$2\r\n10\r\n");
}

#[test]
fn zadd_xx_ch_option() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"ZADD", b"z", b"1", b"a", b"2", b"b"]);
    assert_eq!(
        c.command(&[b"ZADD", b"z", b"XX", b"CH", b"10", b"a", b"3", b"c"]),
        b":1\r\n"
    );
    assert_eq!(c.command(&[b"ZSCORE", b"z", b"a"]), b"$2\r\n10\r\n");
    assert_eq!(c.command(&[b"ZSCORE", b"z", b"c"]), b"$-1\r\n");
}

#[test]
fn zadd_gt_option() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"ZADD", b"z", b"1", b"a", b"2", b"b"]);
    assert_eq!(
        c.command(&[b"ZADD", b"z", b"GT", b"CH", b"0", b"a", b"3", b"b", b"4", b"c"]),
        b":2\r\n"
    );
    assert_eq!(c.command(&[b"ZSCORE", b"z", b"a"]), b"$1\r\n1\r\n");
    assert_eq!(c.command(&[b"ZSCORE", b"z", b"b"]), b"$1\r\n3\r\n");
    assert_eq!(c.command(&[b"ZSCORE", b"z", b"c"]), b"$1\r\n4\r\n");
}

#[test]
fn zadd_lt_option() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"ZADD", b"z", b"5", b"a", b"10", b"b"]);
    assert_eq!(
        c.command(&[b"ZADD", b"z", b"LT", b"CH", b"3", b"a", b"20", b"b", b"1", b"c"]),
        b":2\r\n"
    );
    assert_eq!(c.command(&[b"ZSCORE", b"z", b"a"]), b"$1\r\n3\r\n");
    assert_eq!(c.command(&[b"ZSCORE", b"z", b"b"]), b"$2\r\n10\r\n");
    assert_eq!(c.command(&[b"ZSCORE", b"z", b"c"]), b"$1\r\n1\r\n");
}

#[test]
fn zadd_incr_option() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(
        c.command(&[b"ZADD", b"z", b"INCR", b"5", b"a"]),
        b"$1\r\n5\r\n"
    );
    assert_eq!(
        c.command(&[b"ZADD", b"z", b"INCR", b"3", b"a"]),
        b"$1\r\n8\r\n"
    );
    assert_eq!(c.command(&[b"ZCARD", b"z"]), b":1\r\n");
}

#[test]
fn zadd_incr_xx_option() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(
        c.command(&[b"ZADD", b"z", b"INCR", b"XX", b"5", b"a"]),
        b"$-1\r\n"
    );
    c.command(&[b"ZADD", b"z", b"1", b"a"]);
    assert_eq!(
        c.command(&[b"ZADD", b"z", b"INCR", b"XX", b"5", b"a"]),
        b"$1\r\n6\r\n"
    );
}

#[test]
fn zadd_incr_nx_option() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(
        c.command(&[b"ZADD", b"z", b"INCR", b"NX", b"5", b"a"]),
        b"$1\r\n5\r\n"
    );
    assert_eq!(
        c.command(&[b"ZADD", b"z", b"INCR", b"NX", b"10", b"a"]),
        b"$-1\r\n"
    );
    assert_eq!(c.command(&[b"ZSCORE", b"z", b"a"]), b"$1\r\n5\r\n");
}

#[test]
fn zadd_incr_gt_option() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"ZADD", b"z", b"10", b"a"]);
    assert_eq!(
        c.command(&[b"ZADD", b"z", b"INCR", b"GT", b"-5", b"a"]),
        b"$-1\r\n"
    );
    assert_eq!(
        c.command(&[b"ZADD", b"z", b"INCR", b"GT", b"5", b"a"]),
        b"$2\r\n15\r\n"
    );
}

#[test]
fn zadd_incr_lt_option() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"ZADD", b"z", b"10", b"a"]);
    assert_eq!(
        c.command(&[b"ZADD", b"z", b"INCR", b"LT", b"5", b"a"]),
        b"$-1\r\n"
    );
    assert_eq!(
        c.command(&[b"ZADD", b"z", b"INCR", b"LT", b"-5", b"a"]),
        b"$1\r\n5\r\n"
    );
}

#[test]
fn zadd_mutex_options_error() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert!(c
        .command_str(&[b"ZADD", b"z", b"NX", b"XX", b"1", b"a"])
        .contains("not compatible"));
    assert!(c
        .command_str(&[b"ZADD", b"z", b"NX", b"GT", b"1", b"a"])
        .contains("GT and LT"));
    assert!(c
        .command_str(&[b"ZADD", b"z", b"NX", b"LT", b"1", b"a"])
        .contains("GT and LT"));
    assert!(c
        .command_str(&[b"ZADD", b"z", b"GT", b"LT", b"1", b"a"])
        .contains("not compatible"));
}

#[test]
fn zadd_incr_multiple_pairs_error() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert!(c
        .command_str(&[b"ZADD", b"z", b"INCR", b"1", b"a", b"2", b"b"])
        .contains("ERR"));
}

#[test]
fn zrevrange_and_rank() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"]);
    assert_eq!(
        c.command(&[b"ZREVRANGE", b"z", b"0", b"-1"]),
        b"*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n"
    );
    assert_eq!(c.command(&[b"ZRANK", b"z", b"a"]), b":0\r\n");
    assert_eq!(c.command(&[b"ZRANK", b"z", b"c"]), b":2\r\n");
    assert_eq!(c.command(&[b"ZREVRANK", b"z", b"a"]), b":2\r\n");
    assert_eq!(c.command(&[b"ZREVRANK", b"z", b"c"]), b":0\r\n");
}

#[test]
fn zpopmin_zpopmax() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"]);
    assert_eq!(
        c.command(&[b"ZPOPMIN", b"z"]),
        b"*2\r\n$1\r\na\r\n$1\r\n1\r\n"
    );
    assert_eq!(
        c.command(&[b"ZPOPMAX", b"z"]),
        b"*2\r\n$1\r\nc\r\n$1\r\n3\r\n"
    );
    assert_eq!(c.command(&[b"ZCARD", b"z"]), b":1\r\n");
}

#[test]
fn zrangebyscore_basic() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"]);
    assert_eq!(
        c.command(&[b"ZRANGEBYSCORE", b"z", b"1", b"2"]),
        b"*2\r\n$1\r\na\r\n$1\r\nb\r\n"
    );
    assert_eq!(
        c.command(&[b"ZRANGEBYSCORE", b"z", b"-inf", b"+inf"]),
        b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
    );
}

#[test]
fn zremrangebyscore_basic() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"]);
    assert_eq!(
        c.command(&[b"ZREMRANGEBYSCORE", b"z", b"1", b"2"]),
        b":2\r\n"
    );
    assert_eq!(c.command(&[b"ZCARD", b"z"]), b":1\r\n");
}

#[test]
fn zmscore_basic() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"ZADD", b"z", b"1", b"a", b"2", b"b"]);
    assert_eq!(
        c.command(&[b"ZMSCORE", b"z", b"a", b"b", b"missing"]),
        b"*3\r\n$1\r\n1\r\n$1\r\n2\r\n$-1\r\n"
    );
}
