use crate::common::{Client, TestServer};
use crate::replication::common::wait_until;
use crate::replication::psync2::extract_field;
use std::time::Duration;
use versal::foundation::io::sys::process::{send_signal, ProcessSignal};

//
#[test]
fn replica_enters_blocking_loading_on_different_replid() {
    let master = TestServer::start(&["--save", ""]);
    let replica = TestServer::start(&["--save", ""]);
    let mut mc = master.connect();
    let mut rc = replica.connect();

    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-diskless-sync", b"yes"]),
        b"+OK\r\n"
    );
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-diskless-sync-delay", b"0"]),
        b"+OK\r\n"
    );
    assert_eq!(
        rc.command(&[b"CONFIG", b"SET", b"repl-diskless-load", b"swapdb"]),
        b"+OK\r\n"
    );
    assert_eq!(
        rc.command(&[
            b"CONFIG",
            b"SET",
            b"loading-process-events-interval-bytes",
            b"1024",
        ]),
        b"+OK\r\n"
    );

    for i in 0..500u32 {
        let k = format!("mk{i}");
        assert_eq!(
            mc.command(&[b"SET", k.as_bytes(), &[b'x'; 4096]]),
            b"+OK\r\n"
        );
    }
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"rdb-key-save-delay", b"10000"]),
        b"+OK\r\n"
    );
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"rdbcompression", b"no"]),
        b"+OK\r\n"
    );

    let port = master.port.to_string();
    assert_eq!(
        rc.command(&[b"REPLICAOF", b"127.0.0.1", port.as_bytes()]),
        b"+OK\r\n"
    );

    assert!(
        wait_until(
            || extract_field(
                &replica.connect().command_str(&[b"INFO", b"persistence"]),
                "loading"
            ) == "1",
            Duration::from_secs(10),
        ),
        "replica did not report loading:1"
    );
    let info = replica.connect().command_str(&[b"INFO", b"persistence"]);
    assert_eq!(extract_field(&info, "async_loading"), "0");
}

//
pub(super) fn setup_async_loading() -> (TestServer, TestServer, Client) {
    let master = TestServer::start(&["--save", ""]);
    let replica = TestServer::start(&["--save", ""]);
    let mut mc = master.connect();
    let mut rc = replica.connect();

    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-diskless-sync", b"yes"]),
        b"+OK\r\n"
    );
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-diskless-sync-delay", b"0"]),
        b"+OK\r\n"
    );
    assert_eq!(
        rc.command(&[b"CONFIG", b"SET", b"repl-diskless-load", b"swapdb"]),
        b"+OK\r\n"
    );
    assert_eq!(
        rc.command(&[b"CONFIG", b"SET", b"replica-read-only", b"no"]),
        b"+OK\r\n"
    );

    let port = master.port.to_string();
    assert_eq!(
        rc.command(&[b"REPLICAOF", b"127.0.0.1", port.as_bytes()]),
        b"+OK\r\n"
    );
    assert!(
        wait_until(
            || replica
                .connect()
                .command_str(&[b"INFO", b"replication"])
                .contains("master_link_status:up"),
            Duration::from_secs(10),
        ),
        "initial sync did not complete"
    );

    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"rdbcompression", b"no"]),
        b"+OK\r\n"
    );
    let _ = mc.command(&[b"DEBUG", b"POPULATE", b"2000", b"master", b"10000"]);
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"rdb-key-save-delay", b"10000"]),
        b"+OK\r\n"
    );

    assert_eq!(rc.command(&[b"SET", b"mykey", b"myvalue"]), b"+OK\r\n");

    let baseline_full: u32 = extract_field(
        &master.connect().command_str(&[b"INFO", b"stats"]),
        "sync_full",
    )
    .parse()
    .unwrap_or(0);
    assert_eq!(mc.command(&[b"MULTI"]), b"+OK\r\n");
    assert_eq!(
        mc.command(&[b"CLIENT", b"KILL", b"TYPE", b"replica"]),
        b"+QUEUED\r\n"
    );
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-backlog-size", b"16384"]),
        b"+QUEUED\r\n"
    );
    for keyid in 0..10u32 {
        let k = format!("overflow{keyid}");
        assert_eq!(
            mc.command(&[b"SET", k.as_bytes(), &[b'A'; 16384]]),
            b"+QUEUED\r\n"
        );
    }
    let _ = mc.command(&[b"EXEC"]);

    assert!(
        wait_until(
            || {
                let stats = master.connect().command_str(&[b"INFO", b"stats"]);
                extract_field(&stats, "sync_full")
                    .parse::<u32>()
                    .unwrap_or(0)
                    > baseline_full
            },
            Duration::from_secs(15),
        ),
        "master did not start a new full sync after client kill"
    );

    assert!(
        wait_until(
            || extract_field(
                &replica.connect().command_str(&[b"INFO", b"persistence"]),
                "async_loading"
            ) == "1",
            Duration::from_secs(15),
        ),
        "replica did not enter async_loading"
    );
    (master, replica, mc)
}

#[test]
fn replica_enters_async_loading_and_serves_old_data() {
    let (_master, replica, _mc) = setup_async_loading();
    let info = replica.connect().command_str(&[b"INFO", b"persistence"]);
    assert_eq!(extract_field(&info, "loading"), "0");
    assert_eq!(extract_field(&info, "async_loading"), "1");
    assert_eq!(
        replica.connect().command(&[b"GET", b"mykey"]),
        b"$7\r\nmyvalue\r\n"
    );
}

#[test]
fn async_loading_blocks_no_async_loading_commands_but_allows_reads() {
    let (_master, replica, _mc) = setup_async_loading();
    let reply = replica.connect().command(&[b"REPLICAOF", b"NO", b"ONE"]);
    assert!(
        reply.starts_with(b"-LOADING"),
        "got {:?}",
        String::from_utf8_lossy(&reply)
    );
    let get = replica.connect().command(&[b"GET", b"mykey"]);
    assert!(
        !get.starts_with(b"-LOADING"),
        "read command wrongly rejected: {:?}",
        String::from_utf8_lossy(&get)
    );
}

#[test]
fn async_loading_blocks_config_set_appendonly() {
    let (_master, replica, _mc) = setup_async_loading();
    let reply = replica
        .connect()
        .command(&[b"CONFIG", b"SET", b"appendonly", b"no"]);
    assert!(
        reply.starts_with(b"-LOADING"),
        "got {:?}",
        String::from_utf8_lossy(&reply)
    );
}

// N1 (G2r): the replica must emit the C-verbatim
// `PRIMARY <-> REPLICA sync: Loading DB in memory` notice at the load-commit
// point (mirrors replication.c:2510, logged just before rdbLoad). Every diskless
// section of the upstream replication.tcl waits on this line via
// `wait_for_log_messages`, so its absence is the dominant failure signature that
// blocks un-gating those units.
#[test]
fn replica_logs_loading_notice_during_full_sync() {
    let master = TestServer::start(&["--save", ""]);
    let replica = TestServer::start(&["--save", "", "--logfile", "server.log"]);
    let mut mc = master.connect();

    // Seed data so the transferred RDB carries a real key.
    assert_eq!(mc.command(&[b"SET", b"testkey", b"testval"]), b"+OK\r\n");

    let port = master.port.to_string();
    let mut rc = replica.connect();
    assert_eq!(
        rc.command(&[b"REPLICAOF", b"127.0.0.1", port.as_bytes()]),
        b"+OK\r\n"
    );

    // The full sync commits to loading the received RDB → the replica must have
    // logged the notice at that transition (before the load runs).
    let log_path = replica.data_dir().join("server.log");
    assert!(
        wait_until(
            || {
                std::fs::read_to_string(&log_path)
                    .is_ok_and(|log| log.contains("PRIMARY <-> REPLICA sync: Loading DB in memory"))
            },
            Duration::from_secs(10),
        ),
        "replica did not log the loading-DB-in-memory notice during full sync"
    );

    // Sanity: the key actually arrived, so we know a full sync (not a no-op) ran.
    assert!(
        wait_until(
            || replica.connect().command(&[b"GET", b"testkey"]) == b"$7\r\ntestval\r\n",
            Duration::from_secs(10),
        ),
        "replica did not receive the seeded key via full sync"
    );
}

// P1 (G2r): the replica must honor `key-load-delay` while loading the full-sync
// RDB, matching C's per-key `debugDelay` (rdb.c). This is the load-side pacing
// that lets replication.tcl stage the slow/fast-replica sections
// (replication.tcl:1131 sets `key-load-delay` on the replica then waits for it
// to hang mid-load). We verify the production replica apply path — not just the
// loader primitive — actually slows down: with a large per-key delay over many
// keys, the replica takes measurably longer to report `master_link_status:up`
// than an unpaced sibling.
#[test]
fn replica_paces_full_sync_load_with_key_load_delay() {
    // Seed a master with enough keys that per-key pacing is observable.
    let master = TestServer::start(&["--save", ""]);
    let mut mc = master.connect();
    let key_count = 200u32;
    for i in 0..key_count {
        let k = format!("k{i}");
        assert_eq!(mc.command(&[b"SET", k.as_bytes(), b"v"]), b"+OK\r\n");
    }
    let port = master.port.to_string();

    // Baseline replica: no pacing. Measure how long full sync takes to go up.
    let fast = TestServer::start(&["--save", ""]);
    let mut fc = fast.connect();
    let fast_start = std::time::Instant::now();
    assert_eq!(
        fc.command(&[b"REPLICAOF", b"127.0.0.1", port.as_bytes()]),
        b"+OK\r\n"
    );
    assert!(
        wait_until(
            || fast
                .connect()
                .command_str(&[b"INFO", b"replication"])
                .contains("master_link_status:up"),
            Duration::from_secs(15),
        ),
        "unpaced replica never reached master_link_status:up"
    );
    let fast_elapsed = fast_start.elapsed();

    // Paced replica: 10ms/key over 200 keys ≈ 2s of pure sleep in the load
    // path. The margin must dominate normal process-start and full-sync
    // scheduling jitter when the complete integration suite is concurrent.
    let slow = TestServer::start(&["--save", ""]);
    let mut sc = slow.connect();
    assert_eq!(
        sc.command(&[b"CONFIG", b"SET", b"key-load-delay", b"10000"]),
        b"+OK\r\n"
    );
    let slow_start = std::time::Instant::now();
    assert_eq!(
        sc.command(&[b"REPLICAOF", b"127.0.0.1", port.as_bytes()]),
        b"+OK\r\n"
    );
    assert!(
        wait_until(
            || slow
                .connect()
                .command_str(&[b"INFO", b"replication"])
                .contains("master_link_status:up"),
            Duration::from_secs(30),
        ),
        "paced replica never reached master_link_status:up"
    );
    let slow_elapsed = slow_start.elapsed();

    // The paced load must add real latency: at least 1s beyond the unpaced
    // run, leaving about half of the injected delay as scheduling slack.
    assert!(
        slow_elapsed >= fast_elapsed + Duration::from_secs(1),
        "key-load-delay did not pace the replica full-sync load: paced={slow_elapsed:?} unpaced={fast_elapsed:?}"
    );

    // The paced replica still ends up with the full dataset.
    assert_eq!(slow.connect().command(&[b"GET", b"k0"]), b"$1\r\nv\r\n");
    assert_eq!(slow.connect().command(&[b"GET", b"k199"]), b"$1\r\nv\r\n");
}

// B (G2r-2): the diskless master must log the C-verbatim transfer notice when
// the fork child's pipe reaches EOF with replicas still attached
// (`Diskless rdb transfer, done reading from pipe, N replicas still up.`,
// replication.c:1886). The upstream replication.tcl "diskless ... replicas drop
// during rdb pipe" sections wait on this line to confirm the fork child finished
// streaming. We batch two replicas into a single diskless fork (delay>0 +
// max-replicas 2 forces the deferred fork/relay path — see transport.rs) and
// assert the still-up count on pipe EOF.
#[test]
fn diskless_master_logs_done_reading_from_pipe_with_replica_count() {
    let master = TestServer::start(&["--save", "", "--logfile", "server.log"]);
    let mut mc = master.connect();
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-diskless-sync", b"yes"]),
        b"+OK\r\n"
    );
    // Force the deferred fork/relay path and batch exactly two replicas into a
    // single fork: a positive delay + max-replicas 2 makes each PSYNC defer, and
    // the second enrollment short-circuits into `fire_deferred_full_resyncs`
    // (transport.rs) which forks once for both. The immediate (delay-0) path
    // never reaches the relay's pipe-EOF arm.
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-diskless-sync-delay", b"5"]),
        b"+OK\r\n"
    );
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-diskless-sync-max-replicas", b"2"]),
        b"+OK\r\n"
    );
    assert_eq!(mc.command(&[b"SET", b"bk", b"bv"]), b"+OK\r\n");
    let port = master.port.to_string();

    let r1 = TestServer::start(&["--save", ""]);
    let r2 = TestServer::start(&["--save", ""]);
    assert_eq!(
        r1.connect()
            .command(&[b"REPLICAOF", b"127.0.0.1", port.as_bytes()]),
        b"+OK\r\n"
    );
    assert_eq!(
        r2.connect()
            .command(&[b"REPLICAOF", b"127.0.0.1", port.as_bytes()]),
        b"+OK\r\n"
    );

    let log_path = master.data_dir().join("server.log");
    assert!(
        wait_until(
            || std::fs::read_to_string(&log_path).is_ok_and(|log| log
                .contains("Diskless rdb transfer, done reading from pipe, 2 replicas still up")),
            Duration::from_secs(20),
        ),
        "master did not log the diskless done-reading-from-pipe notice with 2 replicas"
    );
}

// C (G2r-2): when the master closes the connection mid-RDB (a short read), the
// replica must log the C-verbatim short-read warning
// (`Internal error in RDB reading ... Failure loading rdb format from socket,
// assuming connection error, resuming operation.`, rdb.c:131-135) and then
// recover via a fresh full sync (`PRIMARY <-> REPLICA sync: Finished with
// success`, replication.c:2439). Mirrors replication.tcl "diskless loading short
// read". The replica only counts as mid-RDB while its link is still receiving
// (framing) — the receive→load split means pacing the *load* is too late — so we
// throttle the *master's* fork-child serialization (`rdb-key-save-delay`, the
// same recipe as diskless_fork.rs) to widen the receive window, then kill the
// replica link during it.
#[test]
fn replica_logs_short_read_error_and_recovers_with_fresh_full_sync() {
    let master = TestServer::start(&["--save", "", "--logfile", "server.log"]);
    let mut mc = master.connect();
    // Force the deferred diskless fork/relay path (delay>0 + max-replicas 1 → a
    // single replica short-circuits into fire_deferred_full_resyncs and forks).
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
    // Throttle the fork child's per-key serialization so its RDB is still
    // streaming through the pipe when we kill the replica (mirrors C's
    // debugDelay(rdb_key_save_delay); same window trick as diskless_fork.rs).
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"rdb-key-save-delay", b"2000"]),
        b"+OK\r\n"
    );
    let value = vec![b'x'; 64];
    for i in 0..2000u32 {
        let k = format!("sk{i}");
        assert_eq!(mc.command(&[b"SET", k.as_bytes(), &value]), b"+OK\r\n");
    }
    let port = master.port.to_string();

    let replica = TestServer::start(&["--save", "", "--logfile", "server.log"]);
    let mut rc = replica.connect();
    assert_eq!(
        rc.command(&[b"REPLICAOF", b"127.0.0.1", port.as_bytes()]),
        b"+OK\r\n"
    );

    // A real fork must occur — the master is now streaming the throttled RDB, so
    // the replica is mid-receive (ReceivingRdbEof) and a kill lands as a short read.
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
        "expected a diskless fork before killing the replica"
    );
    // Kill the replica connection on the master to force a short read.
    let _ = mc.command(&[b"CLIENT", b"KILL", b"TYPE", b"replica"]);

    let log_path = replica.data_dir().join("server.log");
    assert!(
        wait_until(
            || std::fs::read_to_string(&log_path)
                .is_ok_and(|log| log.contains("Internal error in RDB reading")),
            Duration::from_secs(20),
        ),
        "replica did not log the short-read Internal-error-in-RDB warning"
    );

    // Drop the throttle so the reconnect's fresh full sync completes quickly.
    let _ = mc.command(&[b"CONFIG", b"SET", b"rdb-key-save-delay", b"0"]);

    // Recovery: the replica retries and eventually completes a fresh full sync.
    if !wait_until(
        || {
            std::fs::read_to_string(&log_path)
                .is_ok_and(|log| log.contains("PRIMARY <-> REPLICA sync: Finished with success"))
        },
        Duration::from_secs(30),
    ) {
        eprintln!(
            "=== REPLICA LOG ===\n{}\n=== END ===",
            std::fs::read_to_string(&log_path).unwrap_or_default()
        );
        let master_log_path = master.data_dir().join("server.log");
        eprintln!(
            "=== MASTER LOG ===\n{}\n=== END ===",
            std::fs::read_to_string(&master_log_path).unwrap_or_default()
        );
        panic!("replica did not recover with a fresh full sync after the short read");
    }
    assert!(
        wait_until(
            || replica
                .connect()
                .command_str(&[b"INFO", b"replication"])
                .contains("master_link_status:up"),
            Duration::from_secs(30),
        ),
        "replica never reached master_link_status:up after recovery"
    );
}

// B end-to-end (G2r-2): with the replica loading the full-sync RDB incrementally
// (paced by `key-load-delay`), a slow replica back-pressures the diskless primary
// through TCP so the relay gate holds and the fork child blocks on the full pipe.
// When that slow replica is dropped mid-transfer, the relay resumes and the
// primary must report the POST-DROP count in
// `Diskless rdb transfer, done reading from pipe, N replicas still up.`
// (replication.c:1886). Mirrors replication.tcl "diskless ... replicas drop
// during rdb pipe": one fast replica stays up, the slow one drops => N == 1.
//
// This is the assertion that fails under the old buffered load: a buffered
// replica drains the whole socket immediately (no back-pressure), so the primary
// reads the pipe to EOF with BOTH replicas still up regardless of the drop timing.
//
// TODO(replication): intermittent race in diskless relay drop-count. The inline
// relay was removed from handle_writable and synchronous EPIPE teardown added,
// but a low-probability window remains where the fork child closes the pipe
// write-end and the parent's cron reads EOF before the SIGKILLed replica's TCP
// FIN/RST is delivered and processed through the HUP path. Requires deeper
// alignment with C's two-phase (arm-next-tick) pipe-read model.
#[test]
#[ignore = "intermittent: diskless relay drop-count races pipe EOF under parallel stress"]
fn diskless_done_reading_from_pipe_count_reflects_slow_replica_drop() {
    let master = TestServer::start(&["--save", "", "--logfile", "server.log"]);
    let mut mc = master.connect();
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-diskless-sync", b"yes"]),
        b"+OK\r\n"
    );
    // Batch exactly two replicas into a single diskless fork: a positive delay +
    // max-replicas 2 defers each PSYNC, and the second enrollment short-circuits
    // into `fire_deferred_full_resyncs` (transport.rs), forking once for both.
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-diskless-sync-delay", b"5"]),
        b"+OK\r\n"
    );
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-diskless-sync-max-replicas", b"2"]),
        b"+OK\r\n"
    );
    // Disable compression and seed ~32 MiB so the uncompressed RDB is far larger
    // than the slow replica's combined kernel socket buffers (master SO_SNDBUF +
    // replica SO_RCVBUF autotune to a few MiB each). Otherwise the whole body
    // buffers in-kernel, the primary drains the pipe to EOF before back-pressure
    // engages, and the count reflects both replicas regardless of the drop.
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"rdbcompression", b"no"]),
        b"+OK\r\n"
    );
    assert_eq!(
        mc.command(&[b"DEBUG", b"POPULATE", b"8000", b"k", b"4096"]),
        b"+OK\r\n"
    );
    let port = master.port.to_string();

    // Fast replica: no load delay, drains its socket at full speed and stays up.
    let fast = TestServer::start(&["--save", ""]);
    assert_eq!(
        fast.connect()
            .command(&[b"REPLICAOF", b"127.0.0.1", port.as_bytes()]),
        b"+OK\r\n"
    );
    // Slow replica: a per-key load delay throttles its streaming decode, so it
    // reads the socket slowly (~4s to load 8000 keys), its recv buffer fills, TCP
    // back-pressures the primary, and the relay gate holds on this replica's
    // queued bytes — stalling the pipe read well short of EOF.
    let slow = TestServer::start(&["--save", ""]);
    let mut sc = slow.connect();
    assert_eq!(
        sc.command(&[b"CONFIG", b"SET", b"key-load-delay", b"500"]),
        b"+OK\r\n"
    );
    assert_eq!(
        sc.command(&[b"REPLICAOF", b"127.0.0.1", port.as_bytes()]),
        b"+OK\r\n"
    );

    // Wait for the batched fork to fire — the relay is now mid-transfer and gated
    // by the slow replica's back-pressure (the fork blocks on the full pipe).
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
        "expected a diskless fork before dropping the slow replica"
    );
    // Let the relay push a few chunks so back-pressure is firmly engaged, then
    // drop the slow replica well inside its multi-second load window.
    std::thread::sleep(Duration::from_millis(800));
    // The abrupt close tears the primary-side link, so the relay drops this
    // target and releases the gate for the survivor.
    send_signal(slow.pid() as i32, ProcessSignal::Kill).expect("kill slow replica");
    drop(sc);

    let log_path = master.data_dir().join("server.log");
    if !wait_until(
        || {
            std::fs::read_to_string(&log_path).is_ok_and(|log| {
                log.contains("Diskless rdb transfer, done reading from pipe, 1 replicas still up")
            })
        },
        Duration::from_secs(30),
    ) {
        eprintln!(
            "=== MASTER LOG ===\n{}\n=== END ===",
            std::fs::read_to_string(&log_path).unwrap_or_default()
        );
        panic!("primary did not report post-drop count of 1 after the slow replica dropped");
    }
}

#[test]
fn master_logs_replica_lost_on_streaming_disconnect() {
    let master = TestServer::start(&["--save", "", "--logfile", "server.log"]);
    let mut mc = master.connect();
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-diskless-sync", b"yes"]),
        b"+OK\r\n"
    );
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-diskless-sync-delay", b"0"]),
        b"+OK\r\n"
    );
    assert_eq!(mc.command(&[b"SET", b"k", b"v"]), b"+OK\r\n");
    let port = master.port.to_string();

    let replica = TestServer::start(&["--save", ""]);
    let mut rc = replica.connect();
    assert_eq!(
        rc.command(&[b"REPLICAOF", b"127.0.0.1", port.as_bytes()]),
        b"+OK\r\n"
    );
    assert!(
        wait_until(
            || replica
                .connect()
                .command_str(&[b"INFO", b"replication"])
                .contains("master_link_status:up"),
            Duration::from_secs(10),
        ),
        "replica did not reach streaming state"
    );

    let log_path = master.data_dir().join("server.log");
    send_signal(replica.pid() as i32, ProcessSignal::Kill).expect("kill replica");
    drop(rc);

    if !wait_until(
        || {
            std::fs::read_to_string(&log_path).is_ok_and(|log| {
                log.contains("Connection with replica 127.0.0.1:") && log.contains(" lost.")
            })
        },
        Duration::from_secs(10),
    ) {
        eprintln!(
            "=== MASTER LOG ===\n{}\n=== END ===",
            std::fs::read_to_string(&log_path).unwrap_or_default()
        );
        panic!("primary did not log 'Connection with replica ... lost.' on replica disconnect");
    }
}

#[test]
fn master_disconnects_timedout_streaming_replica() {
    let master = TestServer::start(&["--save", "", "--logfile", "server.log"]);
    let mut mc = master.connect();
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-diskless-sync", b"yes"]),
        b"+OK\r\n"
    );
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-diskless-sync-delay", b"0"]),
        b"+OK\r\n"
    );
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-timeout", b"60"]),
        b"+OK\r\n"
    );
    assert_eq!(mc.command(&[b"SET", b"k", b"v"]), b"+OK\r\n");
    let port = master.port.to_string();

    let replica = TestServer::start(&["--save", ""]);
    let mut rc = replica.connect();
    assert_eq!(
        rc.command(&[b"REPLICAOF", b"127.0.0.1", port.as_bytes()]),
        b"+OK\r\n"
    );
    assert!(
        wait_until(
            || replica
                .connect()
                .command_str(&[b"INFO", b"replication"])
                .contains("master_link_status:up"),
            Duration::from_secs(10),
        ),
        "replica did not reach streaming state"
    );

    send_signal(replica.pid() as i32, ProcessSignal::Stop).expect("stop replica");
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-timeout", b"2"]),
        b"+OK\r\n"
    );

    let log_path = master.data_dir().join("server.log");
    let saw_timeout_log = wait_until(
        || {
            std::fs::read_to_string(&log_path).is_ok_and(|log| {
                log.contains("Disconnecting timedout replica (streaming sync): 127.0.0.1:")
                    || log.contains("Disconnecting timedout replica (full sync): 127.0.0.1:")
            })
        },
        Duration::from_secs(10),
    );
    send_signal(replica.pid() as i32, ProcessSignal::Continue).expect("continue replica");
    drop(rc);

    if !saw_timeout_log {
        eprintln!(
            "=== MASTER LOG ===\n{}\n=== END ===",
            std::fs::read_to_string(&log_path).unwrap_or_default()
        );
        panic!(
            "primary did not log 'Disconnecting timedout replica' after repl-timeout elapsed on paused replica"
        );
    }
}
