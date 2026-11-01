use crate::common::TestServer;
use std::thread::sleep;
use std::time::Duration;

#[test]
fn expire_sets_ttl_and_ttl_reads_it() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SET", b"k", b"v"]);
    assert_eq!(c.command(&[b"EXPIRE", b"k", b"100"]), b":1\r\n");
    let ttl = c.command_str(&[b"TTL", b"k"]);
    let n: i64 = ttl.trim_start_matches(':').trim().parse().unwrap();
    assert!(n > 90 && n <= 100, "ttl={}", n);
}

#[test]
fn expire_on_missing_key_returns_zero() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(c.command(&[b"EXPIRE", b"nope", b"100"]), b":0\r\n");
}

#[test]
fn ttl_states_missing_and_no_expire() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(c.command(&[b"TTL", b"missing"]), b":-2\r\n");
    c.command(&[b"SET", b"k", b"v"]);
    assert_eq!(c.command(&[b"TTL", b"k"]), b":-1\r\n");
    assert_eq!(c.command(&[b"PTTL", b"k"]), b":-1\r\n");
}

#[test]
fn pexpire_and_pttl_millisecond_precision() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SET", b"k", b"v"]);
    assert_eq!(c.command(&[b"PEXPIRE", b"k", b"100000"]), b":1\r\n");
    let pttl = c.command_str(&[b"PTTL", b"k"]);
    let n: i64 = pttl.trim_start_matches(':').trim().parse().unwrap();
    assert!(n > 90000 && n <= 100000, "pttl={}", n);
}

#[test]
fn expireat_in_the_past_deletes_key() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SET", b"k", b"v"]);
    assert_eq!(c.command(&[b"EXPIREAT", b"k", b"1"]), b":1\r\n");
    assert_eq!(c.command(&[b"GET", b"k"]), b"$-1\r\n");
    assert_eq!(c.command(&[b"EXISTS", b"k"]), b":0\r\n");
}

#[test]
fn pexpireat_absolute_future() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SET", b"k", b"v"]);
    assert_eq!(
        c.command(&[b"PEXPIREAT", b"k", b"99999999999999"]),
        b":1\r\n"
    );
    assert_eq!(c.command(&[b"PEXPIRETIME", b"k"]), b":99999999999999\r\n");
    assert_eq!(c.command(&[b"EXPIRETIME", b"k"]), b":100000000000\r\n");
}

#[test]
fn persist_removes_ttl() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SET", b"k", b"v"]);
    c.command(&[b"EXPIRE", b"k", b"100"]);
    assert_eq!(c.command(&[b"PERSIST", b"k"]), b":1\r\n");
    assert_eq!(c.command(&[b"TTL", b"k"]), b":-1\r\n");
    assert_eq!(c.command(&[b"PERSIST", b"k"]), b":0\r\n");
    assert_eq!(c.command(&[b"PERSIST", b"missing"]), b":0\r\n");
}

#[test]
fn expire_nx_only_when_no_ttl() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SET", b"k", b"v"]);
    assert_eq!(c.command(&[b"EXPIRE", b"k", b"100", b"NX"]), b":1\r\n");
    assert_eq!(c.command(&[b"EXPIRE", b"k", b"200", b"NX"]), b":0\r\n");
}

#[test]
fn expire_xx_only_when_ttl_exists() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SET", b"k", b"v"]);
    assert_eq!(c.command(&[b"EXPIRE", b"k", b"100", b"XX"]), b":0\r\n");
    c.command(&[b"EXPIRE", b"k", b"100"]);
    assert_eq!(c.command(&[b"EXPIRE", b"k", b"200", b"XX"]), b":1\r\n");
}

#[test]
fn expire_gt_lt_semantics() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SET", b"k", b"v"]);
    c.command(&[b"EXPIRE", b"k", b"100"]);
    assert_eq!(c.command(&[b"EXPIRE", b"k", b"50", b"GT"]), b":0\r\n");
    assert_eq!(c.command(&[b"EXPIRE", b"k", b"200", b"GT"]), b":1\r\n");
    assert_eq!(c.command(&[b"EXPIRE", b"k", b"300", b"LT"]), b":0\r\n");
    assert_eq!(c.command(&[b"EXPIRE", b"k", b"100", b"LT"]), b":1\r\n");
}

#[test]
fn expire_incompatible_flags_error() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SET", b"k", b"v"]);
    let r = c.command_str(&[b"EXPIRE", b"k", b"100", b"NX", b"XX"]);
    assert!(
        r.starts_with("-ERR") && r.contains("not compatible"),
        "got: {}",
        r
    );
    let r2 = c.command_str(&[b"EXPIRE", b"k", b"100", b"GT", b"LT"]);
    assert!(
        r2.starts_with("-ERR") && r2.contains("not compatible"),
        "got: {}",
        r2
    );
}

#[test]
fn expire_non_integer_errors() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SET", b"k", b"v"]);
    let r = c.command_str(&[b"EXPIRE", b"k", b"abc"]);
    assert!(r.contains("not an integer"), "got: {}", r);
}

#[test]
fn set_ex_option_sets_ttl() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(c.command(&[b"SET", b"k", b"v", b"EX", b"100"]), b"+OK\r\n");
    let ttl = c.command_str(&[b"TTL", b"k"]);
    let n: i64 = ttl.trim_start_matches(':').trim().parse().unwrap();
    assert!(n > 90 && n <= 100, "ttl={}", n);
}

#[test]
fn set_px_option_sets_ttl() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(
        c.command(&[b"SET", b"k", b"v", b"PX", b"100000"]),
        b"+OK\r\n"
    );
    let pttl = c.command_str(&[b"PTTL", b"k"]);
    let n: i64 = pttl.trim_start_matches(':').trim().parse().unwrap();
    assert!(n > 90000 && n <= 100000, "pttl={}", n);
}

#[test]
fn set_keepttl_preserves_ttl() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SET", b"k", b"v", b"EX", b"100"]);
    c.command(&[b"SET", b"k", b"v2"]);
    assert_eq!(c.command(&[b"TTL", b"k"]), b":-1\r\n");
    c.command(&[b"SET", b"k", b"v3", b"EX", b"100"]);
    c.command(&[b"SET", b"k", b"v4", b"KEEPTTL"]);
    let ttl = c.command_str(&[b"TTL", b"k"]);
    let n: i64 = ttl.trim_start_matches(':').trim().parse().unwrap();
    assert!(n > 90 && n <= 100, "ttl={}", n);
}

#[test]
fn set_invalid_expire_errors() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let r = c.command_str(&[b"SET", b"k", b"v", b"EX", b"0"]);
    assert!(r.contains("invalid expire time"), "got: {}", r);
    let r2 = c.command_str(&[b"SET", b"k", b"v", b"EX", b"-5"]);
    assert!(r2.contains("invalid expire time"), "got: {}", r2);
}

#[test]
fn lazy_expiration_after_deadline() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SET", b"k", b"v", b"PX", b"100"]);
    assert_eq!(c.command(&[b"GET", b"k"]), b"$1\r\nv\r\n");
    sleep(Duration::from_millis(200));
    assert_eq!(c.command(&[b"GET", b"k"]), b"$-1\r\n");
    assert_eq!(c.command(&[b"EXISTS", b"k"]), b":0\r\n");
    assert_eq!(c.command(&[b"TTL", b"k"]), b":-2\r\n");
}
