use crate::common::TestServer;
use crate::replication::common::wait_until;
use std::time::Duration;

pub(crate) fn extract_field(info: &str, field: &str) -> String {
    let prefix = format!("{field}:");
    for line in info.lines() {
        if let Some(rest) = line.strip_prefix(&prefix) {
            return rest.trim_end_matches('\r').to_string();
        }
    }
    String::new()
}

#[test]
#[ignore = "MR-7 follow-up: replica_resume cache on streaming teardown + target-switch backoff
still show CI flakiness at high reconnect counts under CPU-starved runners;
partial_ok count occasionally falls below target by 1-2 reconnects.
Re-enable after the PSYNC2 teardown-path resilience MR lands."]
fn psync2_twenty_reconnects_zero_data_loss() {
    let master = TestServer::start_default();
    let master_port = master.port.to_string();
    let mut mc = master.connect();

    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-backlog-size", b"1048576"]),
        b"+OK\r\n"
    );

    let replica = TestServer::start(&["--appendonly"]);
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
    drop(rc);

    let baseline_stats = mc.command_str(&[b"INFO", b"stats"]);
    let baseline_full: u32 = extract_field(&baseline_stats, "sync_full")
        .parse()
        .unwrap_or(0);
    let baseline_partial_ok: u32 = extract_field(&baseline_stats, "sync_partial_ok")
        .parse()
        .unwrap_or(0);

    let mut key_counter: u64 = 0;

    for cycle in 0..20 {
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
            "replica did not switch to master at cycle {cycle}"
        );

        let gap_start = key_counter;
        for _ in 0..10 {
            let k = format!("k{key_counter}");
            let v = format!("v{key_counter}");
            assert_eq!(
                mc.command(&[b"SET", k.as_bytes(), v.as_bytes()]),
                b"+OK\r\n"
            );
            key_counter += 1;
        }

        assert_eq!(
            replica
                .connect()
                .command(&[b"REPLICAOF", b"127.0.0.1", master_port.as_bytes()]),
            b"+OK\r\n"
        );
        let reconnected = wait_until(
            || {
                replica
                    .connect()
                    .command_str(&[b"INFO", b"replication"])
                    .contains("master_link_status:up")
            },
            Duration::from_secs(15),
        );
        if !reconnected {
            let info = replica.connect().command_str(&[b"INFO", b"replication"]);
            let stats = master.connect().command_str(&[b"INFO", b"stats"]);
            panic!(
                "replica did not re-establish link at cycle {cycle}\n--- replica info ---\n{info}\n--- master stats ---\n{stats}"
            );
        }

        let all_gap_keys_present = wait_until(
            || {
                let mut c = replica.connect();
                (gap_start..key_counter).all(|i| {
                    let k = format!("k{i}");
                    let v = format!("v{i}");
                    let expected = format!("${}\r\n{v}\r\n", v.len());
                    c.command(&[b"GET", k.as_bytes()]) == expected.as_bytes().to_vec()
                })
            },
            Duration::from_secs(10),
        );
        assert!(
            all_gap_keys_present,
            "data loss at cycle {cycle}: gap keys not replicated"
        );
    }

    let final_stats = mc.command_str(&[b"INFO", b"stats"]);
    let final_full: u32 = extract_field(&final_stats, "sync_full")
        .parse()
        .unwrap_or(0);
    let final_partial_ok: u32 = extract_field(&final_stats, "sync_partial_ok")
        .parse()
        .unwrap_or(0);
    let final_partial_err: u32 = extract_field(&final_stats, "sync_partial_err")
        .parse()
        .unwrap_or(0);

    let full_resyncs = final_full.saturating_sub(baseline_full);
    let partial_ok = final_partial_ok.saturating_sub(baseline_partial_ok);
    let reconnects = full_resyncs + partial_ok;
    assert!(
        reconnects >= 20,
        "expected at least 20 reconnects, got {reconnects} (full_resyncs={full_resyncs}, partial_ok={partial_ok}, partial_err={final_partial_err})"
    );
    let continue_rate = partial_ok as f64 / reconnects as f64;
    assert!(
        continue_rate >= 0.9,
        "PSYNC CONTINUE rate {continue_rate:.2} below 90% (full_resyncs={full_resyncs}, partial_ok={partial_ok}, partial_err={final_partial_err})"
    );

    let mut c = replica.connect();
    for i in 0..key_counter {
        let k = format!("k{i}");
        let v = format!("v{i}");
        let expected = format!("${}\r\n{v}\r\n", v.len());
        assert_eq!(
            c.command(&[b"GET", k.as_bytes()]),
            expected.as_bytes().to_vec(),
            "key k{i} missing after stress test"
        );
    }
}

#[test]
#[ignore = "MR-7 follow-up: replica_resume cache on streaming teardown + target-switch backoff
still show CI flakiness at high reconnect counts under CPU-starved runners;
partial_ok count occasionally falls below target by 1-2 reconnects.
Re-enable after the PSYNC2 teardown-path resilience MR lands."]
fn psync2_thirty_reconnects_high_partial_resync() {
    //
    let master = TestServer::start_default();
    let master_port = master.port.to_string();
    let mut mc = master.connect();

    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-backlog-size", b"8388608"]),
        b"+OK\r\n"
    );

    let replica = TestServer::start(&["--appendonly"]);
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
    drop(rc);

    let baseline_stats = mc.command_str(&[b"INFO", b"stats"]);
    let baseline_full: u32 = extract_field(&baseline_stats, "sync_full")
        .parse()
        .unwrap_or(0);
    let baseline_partial_ok: u32 = extract_field(&baseline_stats, "sync_partial_ok")
        .parse()
        .unwrap_or(0);

    let mut key_counter: u64 = 0;
    const CYCLES: usize = 30;
    const KEYS_PER_GAP: u64 = 100;

    for cycle in 0..CYCLES {
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
            "replica did not switch to master at cycle {cycle}"
        );

        let gap_start = key_counter;
        for _ in 0..KEYS_PER_GAP {
            let k = format!("rk{key_counter}");
            let v = format!("rv{key_counter}");
            assert_eq!(
                mc.command(&[b"SET", k.as_bytes(), v.as_bytes()]),
                b"+OK\r\n"
            );
            key_counter += 1;
        }

        assert_eq!(
            replica
                .connect()
                .command(&[b"REPLICAOF", b"127.0.0.1", master_port.as_bytes()]),
            b"+OK\r\n"
        );
        let reconnected = wait_until(
            || {
                replica
                    .connect()
                    .command_str(&[b"INFO", b"replication"])
                    .contains("master_link_status:up")
            },
            Duration::from_secs(15),
        );
        if !reconnected {
            let info = replica.connect().command_str(&[b"INFO", b"replication"]);
            let stats = master.connect().command_str(&[b"INFO", b"stats"]);
            panic!(
                "replica did not re-establish link at cycle {cycle}\n--- replica info ---\n{info}\n--- master stats ---\n{stats}"
            );
        }

        let all_gap_keys_present = wait_until(
            || {
                let mut c = replica.connect();
                (gap_start..key_counter).all(|i| {
                    let k = format!("rk{i}");
                    let v = format!("rv{i}");
                    let expected = format!("${}\r\n{v}\r\n", v.len());
                    c.command(&[b"GET", k.as_bytes()]) == expected.as_bytes().to_vec()
                })
            },
            Duration::from_secs(10),
        );
        assert!(
            all_gap_keys_present,
            "data loss at cycle {cycle}: gap keys not replicated"
        );
    }

    let final_stats = mc.command_str(&[b"INFO", b"stats"]);
    let final_full: u32 = extract_field(&final_stats, "sync_full")
        .parse()
        .unwrap_or(0);
    let final_partial_ok: u32 = extract_field(&final_stats, "sync_partial_ok")
        .parse()
        .unwrap_or(0);
    let final_partial_err: u32 = extract_field(&final_stats, "sync_partial_err")
        .parse()
        .unwrap_or(0);

    let full_resyncs = final_full.saturating_sub(baseline_full);
    let partial_ok = final_partial_ok.saturating_sub(baseline_partial_ok);
    let reconnects = full_resyncs + partial_ok;
    assert!(
        reconnects >= CYCLES as u32,
        "expected at least {CYCLES} reconnects, got {reconnects} (full_resyncs={full_resyncs}, partial_ok={partial_ok}, partial_err={final_partial_err})"
    );
    let continue_rate = partial_ok as f64 / reconnects as f64;
    assert!(
        continue_rate >= 0.95,
        "PSYNC CONTINUE rate {continue_rate:.2} below 95% (full_resyncs={full_resyncs}, partial_ok={partial_ok}, partial_err={final_partial_err})"
    );

    let mut c = replica.connect();
    for i in 0..key_counter {
        let k = format!("rk{i}");
        let v = format!("rv{i}");
        let expected = format!("${}\r\n{v}\r\n", v.len());
        assert_eq!(
            c.command(&[b"GET", k.as_bytes()]),
            expected.as_bytes().to_vec(),
            "key rk{i} missing after 30-reconnect stress test"
        );
    }
}
