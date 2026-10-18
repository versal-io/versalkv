use crate::common::TestServer;
use crate::replication::common::wait_until;
use crate::replication::psync2::extract_field;
use std::time::Duration;

#[test]
fn partial_resync_resumes_on_cached_selected_db() {
    let master = TestServer::start_default();
    let master_port = master.port.to_string();
    let mut mc = master.connect();

    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-backlog-size", b"1048576"]),
        b"+OK\r\n"
    );

    let replica = TestServer::start_default();
    let mut rc = replica.connect();
    assert_eq!(
        rc.command(&[b"REPLICAOF", b"127.0.0.1", master_port.as_bytes()]),
        b"+OK\r\n"
    );
    assert!(
        wait_until(
            || {
                replica
                    .connect()
                    .command_str(&[b"INFO", b"replication"])
                    .contains("master_link_status:up")
            },
            Duration::from_secs(15),
        ),
        "initial full resync did not complete"
    );

    assert_eq!(mc.command(&[b"SELECT", b"9"]), b"+OK\r\n");
    assert_eq!(mc.command(&[b"SET", b"warm", b"w0"]), b"+OK\r\n");
    let mut rc9 = replica.connect();
    assert_eq!(rc9.command(&[b"SELECT", b"9"]), b"+OK\r\n");
    assert!(
        wait_until_get(&mut rc9, b"warm", b"w0"),
        "warm-up write on DB 9 did not reach replica online (offset must pass the SELECT)"
    );

    let baseline_partial_ok: u32 =
        extract_field(&mc.command_str(&[b"INFO", b"stats"]), "sync_partial_ok")
            .parse()
            .unwrap_or(0);

    // resume state）
    assert_eq!(
        replica.connect().command(&[b"REPLICAOF", b"NO", b"ONE"]),
        b"+OK\r\n"
    );
    assert!(
        wait_until(
            || {
                replica
                    .connect()
                    .command_str(&[b"INFO", b"replication"])
                    .contains("role:master")
            },
            Duration::from_secs(5),
        ),
        "replica did not switch to master role"
    );

    for i in 0..5 {
        let k = format!("gap{i}");
        let v = format!("gv{i}");
        assert_eq!(
            mc.command(&[b"SET", k.as_bytes(), v.as_bytes()]),
            b"+OK\r\n"
        );
    }

    assert_eq!(
        replica
            .connect()
            .command(&[b"REPLICAOF", b"127.0.0.1", master_port.as_bytes()]),
        b"+OK\r\n"
    );
    assert!(
        wait_until(
            || {
                replica
                    .connect()
                    .command_str(&[b"INFO", b"replication"])
                    .contains("master_link_status:up")
            },
            Duration::from_secs(15),
        ),
        "replica did not re-establish link"
    );

    assert!(
        wait_until(
            || {
                let partial_ok: u32 = extract_field(
                    &master.connect().command_str(&[b"INFO", b"stats"]),
                    "sync_partial_ok",
                )
                .parse()
                .unwrap_or(0);
                partial_ok > baseline_partial_ok
            },
            Duration::from_secs(10),
        ),
        "reconnect did not perform a partial resync (+CONTINUE); test cannot exercise §5.6"
    );

    let mut r9 = replica.connect();
    assert_eq!(r9.command(&[b"SELECT", b"9"]), b"+OK\r\n");
    for i in 0..5 {
        let k = format!("gap{i}");
        let v = format!("gv{i}");
        assert!(
            wait_until_get(&mut r9, k.as_bytes(), v.as_bytes()),
            "gap key {k} did not resume on DB 9 after partial resync"
        );
    }

    let mut r0 = replica.connect();
    assert_eq!(r0.command(&[b"SELECT", b"0"]), b"+OK\r\n");
    for i in 0..5 {
        let k = format!("gap{i}");
        assert_eq!(
            r0.command(&[b"GET", k.as_bytes()]),
            b"$-1\r\n",
            "gap key {k} leaked into DB 0: replica resumed on the wrong DB"
        );
    }
}

fn wait_until_get(client: &mut crate::common::Client, key: &[u8], expected: &[u8]) -> bool {
    let want = {
        let mut v = format!("${}\r\n", expected.len()).into_bytes();
        v.extend_from_slice(expected);
        v.extend_from_slice(b"\r\n");
        v
    };
    wait_until(
        || client.command(&[b"GET", key]) == want,
        Duration::from_secs(10),
    )
}
