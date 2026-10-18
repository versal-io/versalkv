use crate::common::TestServer;
use std::path::PathBuf;
use std::time::{Duration, Instant};

fn migrate(
    c: &mut crate::common::Client,
    server: &TestServer,
    key: &[u8],
    options: &[&[u8]],
) -> Vec<u8> {
    let port = server.port.to_string();
    let mut args: Vec<&[u8]> = vec![
        b"MIGRATE",
        b"127.0.0.1",
        port.as_bytes(),
        key,
        b"0",
        b"5000",
    ];
    args.extend_from_slice(options);
    c.command(&args)
}

fn wait_until<F: FnMut() -> bool>(mut f: F) -> bool {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if f() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> TempDir {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "valkey-rust-migrate-{}-{}-{}",
            tag,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        TempDir(path)
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn migrate_single_key_and_nokey() {
    let source = TestServer::start_default();
    let target = TestServer::start_default();
    let mut c = source.connect();
    assert_eq!(c.command(&[b"SET", b"one", b"value"]), b"+OK\r\n");
    assert_eq!(migrate(&mut c, &target, b"one", &[]), b"+OK\r\n");
    assert_eq!(c.command(&[b"GET", b"one"]), b"$-1\r\n");
    assert_eq!(
        target.connect().command(&[b"GET", b"one"]),
        b"$5\r\nvalue\r\n"
    );
    assert_eq!(
        migrate(&mut c, &target, b"", &[b"KEYS", b"missing"]),
        b"+NOKEY\r\n"
    );
}

#[test]
fn migrate_copy_and_replace() {
    let source = TestServer::start_default();
    let target = TestServer::start_default();
    let mut c = source.connect();
    let mut tc = target.connect();
    c.command(&[b"SET", b"key", b"source"]);
    assert_eq!(migrate(&mut c, &target, b"key", &[b"COPY"]), b"+OK\r\n");
    assert_eq!(c.command(&[b"GET", b"key"]), b"$6\r\nsource\r\n");
    assert_eq!(tc.command(&[b"GET", b"key"]), b"$6\r\nsource\r\n");

    c.command(&[b"SET", b"key", b"replacement"]);
    let reply = migrate(&mut c, &target, b"key", &[]);
    assert!(reply.starts_with(b"-") && String::from_utf8_lossy(&reply).contains("BUSYKEY"));
    assert_eq!(migrate(&mut c, &target, b"key", &[b"REPLACE"]), b"+OK\r\n");
    assert_eq!(tc.command(&[b"GET", b"key"]), b"$11\r\nreplacement\r\n");
}

#[test]
fn migrate_preserves_ttl() {
    let source = TestServer::start_default();
    let target = TestServer::start_default();
    let mut c = source.connect();
    c.command(&[b"SET", b"ttl-key", b"value"]);
    c.command(&[b"PEXPIRE", b"ttl-key", b"10000"]);
    assert_eq!(migrate(&mut c, &target, b"ttl-key", &[]), b"+OK\r\n");
    let ttl = target.connect().command_str(&[b"PTTL", b"ttl-key"]);
    let ttl = ttl.trim_start_matches(':').trim().parse::<i64>().unwrap();
    assert!(ttl > 7000 && ttl <= 10000, "unexpected target TTL: {}", ttl);
}

#[test]
fn migrate_keys_filters_missing_and_partially_succeeds_on_busy_key() {
    let source = TestServer::start_default();
    let target = TestServer::start_default();
    let mut c = source.connect();
    let mut tc = target.connect();
    c.command(&[b"SET", b"first", b"one"]);
    c.command(&[b"SET", b"busy", b"source"]);
    c.command(&[b"SET", b"last", b"three"]);
    tc.command(&[b"SET", b"busy", b"target"]);
    let reply = migrate(
        &mut c,
        &target,
        b"",
        &[b"KEYS", b"first", b"busy", b"last", b"missing"],
    );
    assert!(reply.starts_with(b"-") && String::from_utf8_lossy(&reply).contains("BUSYKEY"));
    assert_eq!(c.command(&[b"GET", b"first"]), b"$-1\r\n");
    assert_eq!(c.command(&[b"GET", b"busy"]), b"$6\r\nsource\r\n");
    assert_eq!(c.command(&[b"GET", b"last"]), b"$-1\r\n");
    assert_eq!(tc.command(&[b"GET", b"first"]), b"$3\r\none\r\n");
    assert_eq!(tc.command(&[b"GET", b"busy"]), b"$6\r\ntarget\r\n");
    assert_eq!(tc.command(&[b"GET", b"last"]), b"$5\r\nthree\r\n");
}

#[test]
fn migrate_auth_and_auth2() {
    let source = TestServer::start_default();
    let target = TestServer::start_default();
    let mut c = source.connect();
    let mut tc = target.connect();
    assert_eq!(
        tc.command(&[b"CONFIG", b"SET", b"requirepass", b"secret"]),
        b"+OK\r\n"
    );

    c.command(&[b"SET", b"auth-key", b"value"]);
    let reply = migrate(&mut c, &target, b"auth-key", &[b"AUTH", b"wrong"]);
    assert!(
        reply.starts_with(b"-"),
        "wrong AUTH should fail: {:?}",
        reply
    );
    assert_eq!(c.command(&[b"GET", b"auth-key"]), b"$5\r\nvalue\r\n");
    assert_eq!(
        migrate(&mut c, &target, b"auth-key", &[b"AUTH", b"secret"]),
        b"+OK\r\n"
    );
    assert_eq!(c.command(&[b"GET", b"auth-key"]), b"$-1\r\n");
    assert_eq!(tc.command(&[b"GET", b"auth-key"]), b"$5\r\nvalue\r\n");

    assert_eq!(
        tc.command(&[
            b"ACL",
            b"SETUSER",
            b"migrate-user",
            b"on",
            b">secret",
            b"allcommands",
            b"allkeys",
        ]),
        b"+OK\r\n"
    );
    c.command(&[b"SET", b"auth2-key", b"value"]);
    let reply = migrate(
        &mut c,
        &target,
        b"auth2-key",
        &[b"AUTH2", b"migrate-user", b"incorrect"],
    );
    assert!(
        reply.starts_with(b"-"),
        "wrong AUTH2 should fail: {:?}",
        reply
    );
    assert_eq!(c.command(&[b"GET", b"auth2-key"]), b"$5\r\nvalue\r\n");
    assert_eq!(
        migrate(
            &mut c,
            &target,
            b"auth2-key",
            &[b"AUTH2", b"migrate-user", b"secret"],
        ),
        b"+OK\r\n"
    );
    assert_eq!(c.command(&[b"GET", b"auth2-key"]), b"$-1\r\n");
    assert_eq!(tc.command(&[b"GET", b"auth2-key"]), b"$5\r\nvalue\r\n");
}

#[test]
fn migrate_del_propagates_to_replica() {
    let source = TestServer::start_default();
    let replica = TestServer::start_default();
    let target = TestServer::start_default();
    let mut rc = replica.connect();
    let source_port = source.port.to_string();
    assert_eq!(
        rc.command(&[b"REPLICAOF", b"127.0.0.1", source_port.as_bytes()]),
        b"+OK\r\n"
    );
    assert!(wait_until(|| {
        replica
            .connect()
            .command_str(&[b"INFO", b"replication"])
            .contains("master_link_status:up")
    }));
    let mut c = source.connect();
    c.command(&[b"SET", b"replica-key", b"value"]);
    assert!(wait_until(|| {
        replica.connect().command(&[b"GET", b"replica-key"]) == b"$5\r\nvalue\r\n"
    }));
    assert_eq!(migrate(&mut c, &target, b"replica-key", &[]), b"+OK\r\n");
    assert!(wait_until(|| {
        replica.connect().command(&[b"GET", b"replica-key"]) == b"$-1\r\n"
    }));
}

#[test]
fn migrate_aof_replay_keeps_source_key_deleted() {
    let dir = TempDir::new("aof");
    let dir_str = dir.path().to_str().unwrap().to_string();
    let target = TestServer::start_default();
    {
        let source = TestServer::start(&[
            "--dir",
            &dir_str,
            "--appendonly",
            "yes",
            "--appendfsync",
            "always",
        ]);
        let mut c = source.connect();
        c.command(&[b"SET", b"aof-key", b"value"]);
        assert_eq!(migrate(&mut c, &target, b"aof-key", &[]), b"+OK\r\n");
        assert_eq!(
            target.connect().command(&[b"GET", b"aof-key"]),
            b"$5\r\nvalue\r\n"
        );
        assert_eq!(c.command(&[b"GET", b"aof-key"]), b"$-1\r\n");
    }
    {
        let restarted = TestServer::start(&[
            "--dir",
            &dir_str,
            "--appendonly",
            "yes",
            "--appendfsync",
            "always",
        ]);
        assert_eq!(
            restarted.connect().command(&[b"GET", b"aof-key"]),
            b"$-1\r\n"
        );
    }
}
