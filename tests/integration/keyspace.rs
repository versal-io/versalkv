use crate::common::TestServer;

#[test]
fn set_then_get() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(c.command(&[b"SET", b"k", b"v"]), b"+OK\r\n");
    assert_eq!(c.command(&[b"GET", b"k"]), b"$1\r\nv\r\n");
}

#[test]
fn get_missing_returns_null() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(c.command(&[b"GET", b"missing"]), b"$-1\r\n");
}

#[test]
fn del_returns_count() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SET", b"a", b"1"]);
    c.command(&[b"SET", b"b", b"2"]);
    assert_eq!(c.command(&[b"DEL", b"a", b"b", b"c"]), b":2\r\n");
    assert_eq!(c.command(&[b"GET", b"a"]), b"$-1\r\n");
}

#[test]
fn incr_from_missing_and_existing() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(c.command(&[b"INCR", b"n"]), b":1\r\n");
    assert_eq!(c.command(&[b"INCR", b"n"]), b":2\r\n");
    assert_eq!(c.command(&[b"GET", b"n"]), b"$1\r\n2\r\n");
}

#[test]
fn incr_on_non_integer_errors() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SET", b"s", b"abc"]);
    let reply = c.command_str(&[b"INCR", b"s"]);
    assert!(
        reply.contains("not an integer or out of range"),
        "got: {}",
        reply
    );
}

#[test]
fn mget_mixed_hits_and_misses() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SET", b"x", b"1"]);
    assert_eq!(
        c.command(&[b"MGET", b"x", b"y"]),
        b"*2\r\n$1\r\n1\r\n$-1\r\n"
    );
}

#[test]
fn select_isolates_keyspace() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SET", b"k", b"db0"]);
    assert_eq!(c.command(&[b"SELECT", b"1"]), b"+OK\r\n");
    assert_eq!(c.command(&[b"GET", b"k"]), b"$-1\r\n");
    assert_eq!(c.command(&[b"SELECT", b"0"]), b"+OK\r\n");
    assert_eq!(c.command(&[b"GET", b"k"]), b"$3\r\ndb0\r\n");
}

#[test]
fn set_nx_xx_conflict_errors() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let r = c.command_str(&[b"SET", b"k", b"v", b"NX", b"XX"]);
    assert!(r.contains("ERR syntax error"), "got: {}", r);
    assert_eq!(
        c.command(&[b"SET", b"k1", b"v", b"NX", b"EX", b"10"]),
        b"+OK\r\n"
    );
    assert_eq!(c.command(&[b"SET", b"k1", b"v2", b"NX"]), b"$-1\r\n");
    assert_eq!(c.command(&[b"SET", b"missing", b"v", b"XX"]), b"$-1\r\n");
    assert_eq!(c.command(&[b"SET", b"k1", b"v3", b"XX"]), b"+OK\r\n");
}

#[test]
fn incr_overflow_does_not_wrap() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    // i64::MAX = 9223372036854775807
    c.command(&[b"SET", b"big", b"9223372036854775807"]);
    let r = c.command_str(&[b"INCR", b"big"]);
    assert!(r.contains("overflow"), "expect overflow, got: {}", r);
    assert_eq!(
        c.command(&[b"GET", b"big"]),
        b"$19\r\n9223372036854775807\r\n"
    );
}

#[test]
fn msetnx_is_all_or_nothing() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SET", b"exists", b"old"]);
    assert_eq!(
        c.command(&[b"MSETNX", b"new", b"val", b"exists", b"override"]),
        b":0\r\n"
    );
    assert_eq!(c.command(&[b"GET", b"new"]), b"$-1\r\n");
    assert_eq!(c.command(&[b"GET", b"exists"]), b"$3\r\nold\r\n");
    assert_eq!(
        c.command(&[b"MSETNX", b"k1", b"v1", b"k2", b"v2"]),
        b":1\r\n"
    );
    assert_eq!(c.command(&[b"GET", b"k1"]), b"$2\r\nv1\r\n");
    assert_eq!(c.command(&[b"GET", b"k2"]), b"$2\r\nv2\r\n");
}

#[test]
fn setex_psetex_invalid_ttl_errors() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    for ttl in [b"0".as_ref(), b"-1"].iter() {
        let r = c.command_str(&[b"SETEX", b"k", ttl, b"v"]);
        assert!(
            r.contains("invalid expire time"),
            "ttl={:?} got: {}",
            ttl,
            r
        );
    }
    for ttl in [b"0".as_ref(), b"-5"].iter() {
        let r = c.command_str(&[b"PSETEX", b"k", ttl, b"v"]);
        assert!(
            r.contains("invalid expire time"),
            "ttl={:?} got: {}",
            ttl,
            r
        );
    }
    let r = c.command_str(&[b"SET", b"k", b"v", b"EX", b"0"]);
    assert!(r.contains("invalid expire time"), "got: {}", r);
    let r = c.command_str(&[b"SET", b"k", b"v", b"PX", b"0"]);
    assert!(r.contains("invalid expire time"), "got: {}", r);
}

#[test]
fn getset_wrong_type_errors() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"LPUSH", b"list", b"a"]);
    let r = c.command_str(&[b"GETSET", b"list", b"v"]);
    assert!(r.contains("WRONGTYPE"), "got: {}", r);
    c.command(&[b"SET", b"s", b"old"]);
    assert_eq!(c.command(&[b"GETSET", b"s", b"new"]), b"$3\r\nold\r\n");
    assert_eq!(c.command(&[b"GET", b"s"]), b"$3\r\nnew\r\n");
}

#[test]
fn set_ex_non_integer_errors() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let r = c.command_str(&[b"SET", b"k", b"v", b"EX", b"abc"]);
    assert!(r.contains("not an integer or out of range"), "got: {}", r);
    let r = c.command_str(&[b"SET", b"k", b"v", b"PX", b"notanum"]);
    assert!(r.contains("not an integer or out of range"), "got: {}", r);
}

#[test]
fn decrby_min_int_neg_overflows() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SET", b"n", b"0"]);
    let r = c.command_str(&[b"DECRBY", b"n", b"-9223372036854775808"]);
    assert!(
        r.contains("overflow"),
        "expect overflow on DECRBY MIN, got: {}",
        r
    );
    c.command(&[b"SET", b"big", b"9223372036854775807"]);
    let r = c.command_str(&[b"INCRBY", b"big", b"1"]);
    assert!(r.contains("overflow"), "expect INCRBY overflow, got: {}", r);
    c.command(&[b"SET", b"small", b"-9223372036854775808"]);
    let r = c.command_str(&[b"DECRBY", b"small", b"1"]);
    assert!(r.contains("overflow"), "expect DECRBY past MIN, got: {}", r);
}

#[test]
fn set_get_option_wrong_type_errors() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SADD", b"myset", b"a"]);
    let r = c.command_str(&[b"SET", b"myset", b"v", b"GET"]);
    assert!(r.contains("WRONGTYPE"), "got: {}", r);
    c.command(&[b"SET", b"s", b"old"]);
    assert_eq!(c.command(&[b"SET", b"s", b"new", b"GET"]), b"$3\r\nold\r\n");
    assert_eq!(c.command(&[b"GET", b"s"]), b"$3\r\nnew\r\n");
    assert_eq!(c.command(&[b"SET", b"nonexist", b"v", b"GET"]), b"$-1\r\n");
}

#[test]
fn set_dup_expire_opts_conflict() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let pairs: &[&[&[u8]]] = &[
        &[b"SET", b"k", b"v", b"EX", b"1", b"PX", b"1"],
        &[b"SET", b"k", b"v", b"EX", b"1", b"KEEPTTL"],
        &[b"SET", b"k", b"v", b"EXAT", b"1", b"PXAT", b"1"],
        &[b"SET", b"k", b"v", b"PX", b"1", b"KEEPTTL"],
    ];
    for cmd in pairs {
        let r = c.command_str(cmd);
        assert!(r.contains("ERR syntax error"), "cmd={:?} got: {}", cmd, r);
    }
}

#[test]
fn setnx_semantics() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(c.command(&[b"SETNX", b"k", b"first"]), b":1\r\n");
    assert_eq!(c.command(&[b"GET", b"k"]), b"$5\r\nfirst\r\n");
    assert_eq!(c.command(&[b"SETNX", b"k", b"second"]), b":0\r\n");
    assert_eq!(c.command(&[b"GET", b"k"]), b"$5\r\nfirst\r\n");
}

#[test]
fn set_ifeq_wrong_type_errors() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"HSET", b"myhash", b"f", b"v"]);
    let r = c.command_str(&[b"SET", b"myhash", b"new", b"IFEQ", b"f"]);
    assert!(r.contains("WRONGTYPE"), "got: {}", r);
    c.command(&[b"SET", b"s", b"old"]);
    assert_eq!(
        c.command(&[b"SET", b"s", b"new", b"IFEQ", b"old"]),
        b"+OK\r\n"
    );
    assert_eq!(c.command(&[b"GET", b"s"]), b"$3\r\nnew\r\n");
    let r = c.command(&[b"SET", b"s", b"x", b"IFEQ", b"notequal"]);
    assert_eq!(r, b"$-1\r\n");
    assert_eq!(c.command(&[b"GET", b"s"]), b"$3\r\nnew\r\n");
    let r = c.command_str(&[b"SET", b"s", b"v", b"NX", b"IFEQ", b"v"]);
    assert!(r.contains("ERR syntax error"), "got: {}", r);
}

#[test]
fn mset_odd_args_wrong_arity() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let r = c.command_str(&[b"MSET", b"a", b"1", b"b"]);
    assert!(
        r.contains("wrong number of arguments") || r.contains("ERR"),
        "got: {}",
        r
    );
    let r = c.command_str(&[b"MSETNX", b"a", b"1", b"b"]);
    assert!(
        r.contains("wrong number of arguments") || r.contains("ERR"),
        "got: {}",
        r
    );
    assert_eq!(c.command(&[b"MSET", b"a", b"1", b"b", b"2"]), b"+OK\r\n");
    assert_eq!(c.command(&[b"GET", b"a"]), b"$1\r\n1\r\n");
    assert_eq!(c.command(&[b"GET", b"b"]), b"$1\r\n2\r\n");
}

#[test]
fn lcs_same_key_uses_linear_fast_path() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let len = 8 * 1024usize;

    assert_eq!(
        c.command(&[
            b"SETRANGE",
            b"large",
            &(len - 1).to_string().into_bytes(),
            b"x"
        ]),
        format!(":{}\r\n", len).as_bytes()
    );
    assert_eq!(
        c.command(&[b"LCS", b"large", b"large", b"LEN"]),
        format!(":{}\r\n", len).as_bytes()
    );

    let reply = c.command(&[b"LCS", b"large", b"large"]);
    let header = format!("${}\r\n", len);
    assert_eq!(reply.len(), header.len() + len + 2);
    assert!(reply.starts_with(header.as_bytes()));
    assert_eq!(&reply[reply.len() - 3..], b"x\r\n");
}
