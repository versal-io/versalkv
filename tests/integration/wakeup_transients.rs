use crate::common::TestServer;
use std::time::{Duration, Instant};

fn _wait_for_blocked_client(admin: &mut crate::common::Client) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if admin
            .command_str(&[b"INFO", b"clients"])
            .contains("blocked_clients:1")
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("BLPOP client did not become blocked");
}

#[test]
fn rejected_blocked_wakeup_does_not_suppress_next_write_propagation() {
    let primary = TestServer::start(&["--appendonly", "--appendfsync", "always"]);
    let replica = TestServer::start_default();
    let mut replica_admin = replica.connect();
    let primary_port = primary.port.to_string();
    assert_eq!(
        replica_admin.command(&[b"REPLICAOF", b"127.0.0.1", primary_port.as_bytes()]),
        b"+OK\r\n"
    );
    let replica_ready_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let info = replica_admin.command_str(&[b"INFO", b"replication"]);
        if info.contains("master_link_status:up") {
            break;
        }
        assert!(
            Instant::now() < replica_ready_deadline,
            "replica did not become ready: {info}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    let mut admin = primary.connect();
    let mut blocked = primary.connect();
    assert_eq!(
        admin.command(&[
            b"ACL",
            b"SETUSER",
            b"blocked-propagation",
            b"on",
            b"nopass",
            b"+@all",
            b"allkeys",
        ]),
        b"+OK\r\n"
    );
    assert_eq!(
        blocked.command(&[b"AUTH", b"blocked-propagation", b"ignored"]),
        b"+OK\r\n"
    );
    blocked.send(&[b"BLPOP", b"blocked-list", b"0"]);
    _wait_for_blocked_client(&mut admin);

    assert_eq!(
        admin.command(&[b"ACL", b"SETUSER", b"blocked-propagation", b"resetkeys"]),
        b"+OK\r\n"
    );
    assert_eq!(
        admin.command(&[b"LPUSH", b"blocked-list", b"value"]),
        b":1\r\n"
    );
    assert!(blocked
        .read_reply()
        .starts_with(b"-NOPERM No permissions to access a key"));
    assert_eq!(
        admin.command(&[b"ACL", b"SETUSER", b"blocked-propagation", b"allkeys"]),
        b"+OK\r\n"
    );
    assert_eq!(
        blocked.command(&[b"HSET", b"after-rejected-wakeup", b"field", b"persisted"]),
        b":1\r\n"
    );

    let propagation_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if replica
            .connect()
            .command(&[b"HGET", b"after-rejected-wakeup", b"field"])
            == b"$9\r\npersisted\r\n"
        {
            break;
        }
        assert!(
            Instant::now() < propagation_deadline,
            "write after rejected wakeup was not propagated to replica"
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    let aof_dir = primary.data_dir().join("appendonlydir");
    let manifest = std::fs::read_to_string(aof_dir.join("appendonly.aof.manifest"))
        .expect("read AOF manifest");
    let increment = manifest
        .lines()
        .find(|line| line.ends_with(" type i"))
        .and_then(|line| line.split_whitespace().nth(1))
        .expect("active incremental AOF");
    let aof = std::fs::read(aof_dir.join(increment)).expect("read incremental AOF");
    assert_eq!(
        aof.windows(b"after-rejected-wakeup".len())
            .filter(|window| *window == b"after-rejected-wakeup")
            .count(),
        1,
        "the next write must reach AOF exactly once without stale custom/script effects"
    );
}
