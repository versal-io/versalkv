use crate::common::TestServer;
use crate::replication::common::wait_until;
use crate::replication::psync2::extract_field;
use std::time::Duration;

/// When a diskless full-sync fork child is relaying its RDB and the last (only)
/// replica in the batch drops mid-transfer, the primary must log the C-verbatim
/// notice line, SIGKILL the fork child, and clear the relay
/// (mirrors networking.c:2022).
#[test]
fn diskless_last_replica_drop_kills_fork_child_and_logs() {
    let master = TestServer::start(&["--save", "", "--logfile", "server.log"]);
    let mut mc = master.connect();
    // Force the deferred fork/relay path (not the immediate delay-0 PSYNC), same
    // as the sibling test: diskless on, a positive delay, and max-replicas 1 so a
    // single replica short-circuits into `fire_deferred_full_resyncs` and forks.
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-diskless-sync", b"yes"]),
        b"+OK\r\n"
    );
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-diskless-sync-delay", b"5"]),
        b"+OK\r\n"
    );
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-diskless-sync-max-replicas", b"1"]),
        b"+OK\r\n"
    );
    // Throttle the fork child's per-key serialization so its RDB stream is still
    // draining through the pipe when the replica drops, landing the disconnect
    // mid-transfer. This is exactly how the upstream Tcl probe forces the window
    // (replication.tcl uses `config set rdb-key-save-delay`), and it mirrors C's
    // diskless save calling debugDelay(server.rdb_key_save_delay) per key.
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"rdb-key-save-delay", b"2000"]),
        b"+OK\r\n"
    );

    // Seed enough keys that, at the configured per-key delay, the child spends
    // well over a cron tick streaming — guaranteeing the relay is still live when
    // the replica drops.
    let value = vec![b'x'; 64];
    for i in 0..2000u32 {
        let key = format!("g2:seed:{i}");
        assert_eq!(mc.command(&[b"SET", key.as_bytes(), &value]), b"+OK\r\n");
    }

    let replica = TestServer::start(&["--save", ""]);
    let mut rc = replica.connect();
    let port = master.port.to_string();
    assert_eq!(
        rc.command(&[b"REPLICAOF", b"127.0.0.1", port.as_bytes()]),
        b"+OK\r\n"
    );

    // A real fork() must occur for the diskless sync before we drop the replica.
    assert!(
        wait_until(
            || {
                let stats = master.connect().command_str(&[b"INFO", b"stats"]);
                extract_field(&stats, "total_forks")
                    .parse::<u64>()
                    .unwrap_or(0)
                    >= 1
            },
            Duration::from_secs(10),
        ),
        "expected a diskless fork before dropping the replica"
    );

    // Drop the only replica: the harness Drop shuts it down, disconnecting the
    // last relay target while the throttled child is still streaming.
    drop(rc);
    drop(replica);

    let log_path = master.data_dir().join("server.log");
    assert!(
        wait_until(
            || {
                std::fs::read_to_string(&log_path).is_ok_and(|log| {
                    log.contains("Diskless rdb transfer, last replica dropped, killing fork child")
                })
            },
            Duration::from_secs(10),
        ),
        "primary did not log the kill-fork-child notice after the last replica dropped"
    );
}

#[test]
fn diskless_fullsync_forks_child_and_syncs_replica() {
    let master = TestServer::start(&["--save", ""]);
    let mut mc = master.connect();
    // Diskless full-sync (fork path) requires repl-diskless-sync on and the eof
    // capability, which the Rust replica advertises. The fork/relay path lives
    // in `fire_deferred_full_resyncs`, which the immediate (delay 0) PSYNC path
    // never reaches — only the deferred/short-circuit path does. Set a positive
    // delay plus max-replicas 1 so a single replica defers then immediately
    // short-circuits (transport.rs) into `fire_deferred_full_resyncs`, exercising
    // the fork without waiting out the delay.
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-diskless-sync", b"yes"]),
        b"+OK\r\n"
    );
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-diskless-sync-delay", b"5"]),
        b"+OK\r\n"
    );
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-diskless-sync-max-replicas", b"1"]),
        b"+OK\r\n"
    );
    assert_eq!(mc.command(&[b"SET", b"g2:diskless", b"1"]), b"+OK\r\n");

    let replica = TestServer::start(&["--save", ""]);
    let mut rc = replica.connect();
    let port = master.port.to_string();
    assert_eq!(
        rc.command(&[b"REPLICAOF", b"127.0.0.1", port.as_bytes()]),
        b"+OK\r\n"
    );

    // Replica converges on the master's data via a diskless full-sync, and the
    // live command stream keeps flowing (the link must reach Streaming).
    assert!(
        wait_until(
            || replica.connect().command_str(&[b"GET", b"g2:diskless"]) == "$1\r\n1\r\n",
            Duration::from_secs(10),
        ),
        "replica did not converge via diskless full-sync"
    );

    // A real fork() must have happened on the master for the diskless sync.
    let stats = master.connect().command_str(&[b"INFO", b"stats"]);
    let forks: u64 = extract_field(&stats, "total_forks").parse().unwrap_or(0);
    assert!(forks >= 1, "expected a diskless fork, total_forks={forks}");

    // The link must reach Streaming after the piped RDB, not merely receive the
    // snapshot: a post-sync write on the master must propagate to the replica.
    // This guards the relay's finish_full_resync + backlog-tail transition — the
    // single most important correctness requirement of the fork/relay path.
    assert_eq!(mc.command(&[b"SET", b"g2:after", b"live"]), b"+OK\r\n");
    assert!(
        wait_until(
            || replica.connect().command_str(&[b"GET", b"g2:after"]) == "$4\r\nlive\r\n",
            Duration::from_secs(10),
        ),
        "post-sync write did not propagate: diskless link never reached Streaming"
    );
}

/// Disk-based full-sync (the non-EOF path a legacy `SYNC` / a replica without the
/// `eof` capability takes) must also fork a child that serializes the RDB to a
/// pipe — the whole point of G2r D1 — so `get_child_pid` / `pgrep -P` probes find
/// a real child and INFO reports `rdb_bgsave_in_progress:1` during the transfer,
/// matching C valkey. A raw `SYNC` never advertises `eof`, so with diskless off
/// (the default) it lands squarely on `fork_disk_rdb_child`.
#[test]
fn disk_based_sync_forks_child_and_streams_bulk() {
    let master = TestServer::start(&["--save", ""]);
    let mut mc = master.connect();
    // Diskless must stay off so PSYNC/SYNC take the disk-based (`$<len>` bulk)
    // path; it is off by default, but pin it so the test does not depend on that.
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-diskless-sync", b"no"]),
        b"+OK\r\n"
    );
    // Throttle the fork child's per-key serialization so the child is still alive
    // (serializing before it writes the `$<len>` header) when we probe INFO.
    // Mirrors the upstream Tcl probe's `config set rdb-key-save-delay`. Kept at
    // 1ms/key so 2000 keys stay well under the client's socket read timeout.
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"rdb-key-save-delay", b"1000"]),
        b"+OK\r\n"
    );
    let value = vec![b'x'; 64];
    for i in 0..2000u32 {
        let key = format!("g2:disk:{i}");
        assert_eq!(mc.command(&[b"SET", key.as_bytes(), &value]), b"+OK\r\n");
    }

    let baseline: u64 = {
        let stats = master.connect().command_str(&[b"INFO", b"stats"]);
        extract_field(&stats, "total_forks").parse().unwrap_or(0)
    };

    // A bare `SYNC` (no REPLCONF capa eof) forces the non-EOF disk-based path.
    let mut sync_client = master.connect();
    sync_client.send_raw(b"SYNC\r\n");

    // While the throttled child streams its `$<len>` bulk, the master must show a
    // real fork and an in-progress bgsave — proving a child process exists (the
    // get_child_pid probe requirement), not an in-process serialization.
    assert!(
        wait_until(
            || {
                let mut probe = master.connect();
                let stats = probe.command_str(&[b"INFO", b"stats"]);
                let forks: u64 = extract_field(&stats, "total_forks").parse().unwrap_or(0);
                let persist = probe.command_str(&[b"INFO", b"persistence"]);
                forks > baseline && persist.contains("rdb_bgsave_in_progress:1")
            },
            Duration::from_secs(10),
        ),
        "disk-based SYNC did not fork a child / report rdb_bgsave_in_progress:1"
    );

    // Drain the self-framed `$<len>\r\n` + body bulk to confirm the relay delivers
    // the complete snapshot the child produced.
    sync_client.read_sync_rdb();

    // Once the child has finished and been reaped by the cron fallback, the master
    // must report no in-progress bgsave again — the single-child slot is released.
    assert!(
        wait_until(
            || {
                master
                    .connect()
                    .command_str(&[b"INFO", b"persistence"])
                    .contains("rdb_bgsave_in_progress:0")
            },
            Duration::from_secs(10),
        ),
        "disk-based SYNC child was never reaped (rdb_bgsave_in_progress stuck at 1)"
    );
}

/// A normal Rust replica advertises `capa eof`, but EOF capability chooses the
/// frame format, not whether disk-based replication may serialize inline. With
/// diskless sync disabled, C starts a disk-target child for this PSYNC path too;
/// otherwise a large RDB blocks the primary event loop and starves issue #141's
/// concurrent writers and replicas.
#[test]
fn disk_based_eof_capable_psync_forks_child() {
    let master = TestServer::start(&["--save", ""]);
    let mut primary = master.connect();
    assert_eq!(
        primary.command(&[b"CONFIG", b"SET", b"repl-diskless-sync", b"no"]),
        b"+OK\x0d\x0a"
    );
    assert_eq!(
        primary.command(&[b"CONFIG", b"SET", b"rdb-key-save-delay", b"1000"]),
        b"+OK\x0d\x0a"
    );
    for i in 0..2_000u32 {
        let key = format!("g2:eof-disk:{i}");
        assert_eq!(
            primary.command(&[b"SET", key.as_bytes(), b"value"]),
            b"+OK\x0d\x0a"
        );
    }
    let baseline: u64 = extract_field(
        &master.connect().command_str(&[b"INFO", b"stats"]),
        "total_forks",
    )
    .parse()
    .unwrap_or(0);

    let replica = TestServer::start(&["--save", ""]);
    assert_eq!(
        replica.connect().command(&[
            b"REPLICAOF",
            b"127.0.0.1",
            master.port.to_string().as_bytes(),
        ]),
        b"+OK\x0d\x0a"
    );

    assert!(
        wait_until(
            || {
                let mut probe = master.connect();
                let forks: u64 =
                    extract_field(&probe.command_str(&[b"INFO", b"stats"]), "total_forks")
                        .parse()
                        .unwrap_or(0);
                let persistence = probe.command_str(&[b"INFO", b"persistence"]);
                forks > baseline && persistence.contains("rdb_bgsave_in_progress:1")
            },
            Duration::from_secs(10),
        ),
        "EOF-capable disk-based PSYNC must fork a child instead of serializing on the event loop"
    );
}

#[test]
fn disk_based_multiple_replicas_join_single_batch_and_converge() {
    let master = TestServer::start(&["--save", ""]);
    let mut primary = master.connect();
    assert_eq!(
        primary.command(&[b"CONFIG", b"SET", b"repl-diskless-sync", b"no"]),
        b"+OK\r\n"
    );
    assert_eq!(
        primary.command(&[b"CONFIG", b"SET", b"rdb-key-save-delay", b"1000"]),
        b"+OK\r\n"
    );
    for i in 0..1000u32 {
        let key = format!("g2:batch:{i}");
        assert_eq!(
            primary.command(&[b"SET", key.as_bytes(), b"batch-value"]),
            b"+OK\r\n"
        );
    }

    let baseline_forks: u64 = extract_field(
        &master.connect().command_str(&[b"INFO", b"stats"]),
        "total_forks",
    )
    .parse()
    .unwrap_or(0);

    let replicas = [
        TestServer::start(&["--save", ""]),
        TestServer::start(&["--save", ""]),
        TestServer::start(&["--save", ""]),
    ];
    let port = master.port.to_string();
    for replica in &replicas {
        assert_eq!(
            replica
                .connect()
                .command(&[b"REPLICAOF", b"127.0.0.1", port.as_bytes()]),
            b"+OK\r\n"
        );
    }

    assert!(
        wait_until(
            || {
                master
                    .connect()
                    .command_str(&[b"INFO", b"replication"])
                    .matches("state=online")
                    .count()
                    == 3
            },
            Duration::from_secs(60),
        ),
        "all three disk-based replicas must reach online"
    );

    let final_forks: u64 = extract_field(
        &master.connect().command_str(&[b"INFO", b"stats"]),
        "total_forks",
    )
    .parse()
    .unwrap_or(0);
    assert_eq!(
        final_forks - baseline_forks,
        1,
        "three concurrent disk-based replicas must share ONE fork (batch), not three"
    );

    let digest = master.connect().command_str(&[b"DEBUG", b"DIGEST"]);
    for replica in &replicas {
        assert!(
            wait_until(
                || replica.connect().command_str(&[b"DEBUG", b"DIGEST"]) == digest,
                Duration::from_secs(15),
            ),
            "replica digest did not converge"
        );
    }

    assert_eq!(
        primary.command(&[b"SET", b"g2:after-batch", b"live"]),
        b"+OK\r\n"
    );
    for replica in &replicas {
        assert!(
            wait_until(
                || replica.connect().command_str(&[b"GET", b"g2:after-batch"]) == "$4\r\nlive\r\n",
                Duration::from_secs(10),
            ),
            "post-batch write did not propagate to replica"
        );
    }
}

/// A deferred diskless snapshot captures the dataset at fork time, but the
/// replica starts its following command stream in DB 0.  The primary must
/// therefore invalidate its stream DB at the fork boundary so the first
/// post-snapshot write emits SELECT even when it targets the same DB as the
/// final pre-fork write.
fn assert_deferred_diskless_fullsync_reselects_db_after_snapshot(dual_channel: bool) {
    let master = TestServer::start(&["--save", ""]);
    let mut primary = master.connect();
    assert_eq!(
        primary.command(&[b"CONFIG", b"SET", b"repl-diskless-sync", b"yes"]),
        b"+OK\r\n"
    );
    let dual_channel_value: &[u8] = if dual_channel { b"yes" } else { b"no" };
    assert_eq!(
        primary.command(&[
            b"CONFIG",
            b"SET",
            b"dual-channel-replication-enabled",
            dual_channel_value,
        ]),
        b"+OK\r\n"
    );
    assert_eq!(
        primary.command(&[b"CONFIG", b"SET", b"repl-diskless-sync-delay", b"2"]),
        b"+OK\r\n"
    );
    assert_eq!(
        primary.command(&[b"CONFIG", b"SET", b"rdb-key-save-delay", b"1000"]),
        b"+OK\r\n"
    );
    for i in 0..1_000u32 {
        let key = format!("diskless-db:seed:{i}");
        assert_eq!(
            primary.command(&[b"SET", key.as_bytes(), b"value"]),
            b"+OK\r\n"
        );
    }
    assert_eq!(primary.command(&[b"SELECT", b"12"]), b"+OK\r\n");

    let baseline_forks: u64 = extract_field(
        &master.connect().command_str(&[b"INFO", b"stats"]),
        "total_forks",
    )
    .parse()
    .unwrap_or(0);
    let replica = TestServer::start(&["--save", ""]);
    assert_eq!(
        replica.connect().command(&[
            b"CONFIG",
            b"SET",
            b"dual-channel-replication-enabled",
            dual_channel_value,
        ]),
        b"+OK\r\n"
    );
    assert_eq!(
        replica.connect().command(&[
            b"REPLICAOF",
            b"127.0.0.1",
            master.port.to_string().as_bytes(),
        ]),
        b"+OK\r\n"
    );

    assert!(
        wait_until(
            || {
                master
                    .connect()
                    .command_str(&[b"INFO", b"replication"])
                    .contains("state=wait_bgsave")
            },
            Duration::from_secs(10),
        ),
        "replica never entered the deferred diskless wait"
    );
    assert_eq!(
        primary.command(&[b"SET", b"diskless-db:snapshot", b"captured"]),
        b"+OK\r\n"
    );
    assert_eq!(
        primary.command(&[b"LPUSH", b"diskless-db:list", b"snapshot"]),
        b":1\r\n"
    );
    assert!(
        wait_until(
            || {
                extract_field(
                    &master.connect().command_str(&[b"INFO", b"stats"]),
                    "total_forks",
                )
                .parse::<u64>()
                .unwrap_or(0)
                    > baseline_forks
            },
            Duration::from_secs(10),
        ),
        "deferred diskless full sync never forked"
    );

    assert_eq!(
        primary.command(&[b"SET", b"diskless-db:post-fork", b"live"]),
        b"+OK\r\n"
    );
    assert_eq!(
        primary.command(&[b"LPUSH", b"diskless-db:list", b"post-fork"]),
        b":2\r\n"
    );
    assert!(
        wait_until(
            || {
                let mut probe = replica.connect();
                probe.command(&[b"SELECT", b"12"]) == b"+OK\r\n"
                    && probe.command(&[b"GET", b"diskless-db:post-fork"]) == b"$4\r\nlive\r\n"
            },
            Duration::from_secs(15),
        ),
        "post-fork DB 12 write was not replicated into DB 12"
    );
    let mut list_probe = replica.connect();
    assert_eq!(list_probe.command(&[b"SELECT", b"12"]), b"+OK\r\n");
    assert!(
        wait_until(
            || {
                list_probe.command(&[b"LRANGE", b"diskless-db:list", b"0", b"-1"])
                    == b"*2\r\n$9\r\npost-fork\r\n$8\r\nsnapshot\r\n"
            },
            Duration::from_secs(10),
        ),
        "snapshot-boundary list write was dropped or replayed"
    );
    assert_eq!(
        replica
            .connect()
            .command(&[b"GET", b"diskless-db:post-fork"]),
        b"$-1\r\n",
        "post-fork write leaked into DB 0 because the stream omitted SELECT 12"
    );
}

#[test]
fn deferred_diskless_fullsync_reselects_db_after_snapshot() {
    assert_deferred_diskless_fullsync_reselects_db_after_snapshot(false);
}

#[test]
fn deferred_dual_channel_fullsync_reselects_db_after_snapshot() {
    assert_deferred_diskless_fullsync_reselects_db_after_snapshot(true);
}

#[test]
fn disk_based_fullsync_reselects_db_after_snapshot() {
    let master = TestServer::start(&["--save", ""]);
    let mut primary = master.connect();
    assert_eq!(
        primary.command(&[b"CONFIG", b"SET", b"repl-diskless-sync", b"no"]),
        b"+OK\r\n"
    );
    assert_eq!(
        primary.command(&[b"CONFIG", b"SET", b"rdb-key-save-delay", b"1000"]),
        b"+OK\r\n"
    );
    for i in 0..1_000u32 {
        let key = format!("disk-db:seed:{i}");
        assert_eq!(
            primary.command(&[b"SET", key.as_bytes(), b"value"]),
            b"+OK\r\n"
        );
    }
    assert_eq!(primary.command(&[b"SELECT", b"12"]), b"+OK\r\n");
    assert_eq!(
        primary.command(&[b"LPUSH", b"disk-db:list", b"snapshot"]),
        b":1\r\n"
    );

    let baseline_forks: u64 = extract_field(
        &master.connect().command_str(&[b"INFO", b"stats"]),
        "total_forks",
    )
    .parse()
    .unwrap_or(0);
    let replica = TestServer::start(&["--save", ""]);
    assert_eq!(
        replica.connect().command(&[
            b"REPLICAOF",
            b"127.0.0.1",
            master.port.to_string().as_bytes(),
        ]),
        b"+OK\r\n"
    );

    assert!(
        wait_until(
            || {
                extract_field(
                    &master.connect().command_str(&[b"INFO", b"stats"]),
                    "total_forks",
                )
                .parse::<u64>()
                .unwrap_or(0)
                    > baseline_forks
            },
            Duration::from_secs(10),
        ),
        "disk-based full sync never forked"
    );
    assert_eq!(
        primary.command(&[b"LPUSH", b"disk-db:list", b"post-fork"]),
        b":2\r\n"
    );

    let mut db12_probe = replica.connect();
    assert_eq!(db12_probe.command(&[b"SELECT", b"12"]), b"+OK\r\n");
    assert!(
        wait_until(
            || {
                db12_probe.command(&[b"LRANGE", b"disk-db:list", b"0", b"-1"])
                    == b"*2\r\n$9\r\npost-fork\r\n$8\r\nsnapshot\r\n"
            },
            Duration::from_secs(15),
        ),
        "disk snapshot-boundary list write was dropped or replayed in the wrong DB"
    );
    assert_eq!(
        replica
            .connect()
            .command(&[b"LRANGE", b"disk-db:list", b"0", b"-1"]),
        b"*0\r\n",
        "post-fork write leaked into DB 0 because the stream omitted SELECT 12"
    );
}
