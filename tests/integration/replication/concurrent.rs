use crate::common::TestServer;
use crate::replication::common::wait_until;
use crate::replication::psync2::extract_field;
use std::time::Duration;

#[test]
fn three_replicas_attach_concurrently_and_converge() {
    let master = TestServer::start(&["--save", ""]);
    let mut mc = master.connect();
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-diskless-sync", b"yes"]),
        b"+OK\r\n"
    );
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-diskless-sync-delay", b"1"]),
        b"+OK\r\n"
    );
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-diskless-sync-max-replicas", b"3"]),
        b"+OK\r\n"
    );
    for i in 0..200u32 {
        let k = format!("k{i}");
        assert_eq!(mc.command(&[b"SET", k.as_bytes(), b"v"]), b"+OK\r\n");
    }
    let r0 = TestServer::start(&["--save", ""]);
    let r1 = TestServer::start(&["--save", ""]);
    let r2 = TestServer::start(&["--save", ""]);
    let port = master.port.to_string();
    for r in [&r0, &r1, &r2] {
        assert_eq!(
            r.connect()
                .command(&[b"REPLICAOF", b"127.0.0.1", port.as_bytes()]),
            b"+OK\r\n"
        );
    }

    assert!(
        wait_until(
            || {
                let info = master.connect().command_str(&[b"INFO", b"replication"]);
                info.contains("slave0:")
                    && info.contains("slave1:")
                    && info.contains("slave2:")
                    && info.matches("state=online").count() >= 3
            },
            Duration::from_secs(20)
        ),
        "not all replicas online"
    );

    let info = master.connect().command_str(&[b"INFO", b"replication"]);
    assert_eq!(extract_field(&info, "replicas_waiting_psync"), "0");

    let md = master.connect().command_str(&[b"DEBUG", b"DIGEST"]);
    for r in [&r0, &r1, &r2] {
        assert!(
            wait_until(
                || r.connect().command_str(&[b"DEBUG", b"DIGEST"]) == md,
                Duration::from_secs(20)
            ),
            "digest mismatch"
        );
    }
    assert!(!md.contains("0000000000000000000000000000000000000000"));
}
