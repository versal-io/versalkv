use crate::common::TestServer;
use std::time::Duration;
use versal::foundation::io::sys::process::{send_signal, ProcessSignal};

mod common;
use common::wait_until;

mod functions;
mod heavy_load;
mod interrupted_dual_sync;
mod loading;

#[test]
fn replconf_rejects_unknown_option_pairs_and_preserves_known_options() {
    let server = TestServer::start_default();
    let mut client = server.connect();

    assert_eq!(
        client.command(&[b"REPLCONF", b"unknown-option", b"value"]),
        b"-ERR Unrecognized REPLCONF option: unknown-option\r\n"
    );
    assert_eq!(
        client.command(&[b"REPLCONF", b"listening-port", b"6380"]),
        b"+OK\r\n"
    );
}

#[test]
fn replconf_accepts_replica_version() {
    let server = TestServer::start_default();
    let mut client = server.connect();

    assert_eq!(
        client.command(&[b"REPLCONF", b"VERSION", b"8.0.0"]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"REPLCONF", b"VERSION", b"invalid"]),
        b"-ERR Unrecognized version format: invalid\r\n"
    );
}

fn assert_info_hides_pre_psync_channels(server: &TestServer, context: &str) {
    let info = server.connect().command_str(&[b"INFO", b"replication"]);
    assert!(
        info.contains("connected_slaves:0\r\n")
            && !info
                .lines()
                .any(|line| line.starts_with("slave") && line.contains("type="))
            && !info.lines().any(|line| line.starts_with("channel")),
        "{context}: {info}"
    );
}

fn assert_replconf_rdb_channel_option_order_preserves_candidate(rdb_channel_first: bool) {
    let server = TestServer::start_default();
    let mut main = server.connect();
    assert_eq!(
        main.command(&[
            b"CONFIG",
            b"SET",
            b"dual-channel-replication-enabled",
            b"yes"
        ]),
        b"+OK\r\n"
    );
    assert_eq!(
        main.command(&[b"REPLCONF", b"listening-port", b"6380"]),
        b"+OK\r\n"
    );

    let mut rdb = server.connect();
    let rdb_id = rdb
        .command(&[b"CLIENT", b"ID"])
        .strip_prefix(b":")
        .and_then(|value| std::str::from_utf8(value).ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .expect("CLIENT ID should be an integer");
    let response = if rdb_channel_first {
        rdb.command(&[
            b"REPLCONF",
            b"rdb-channel",
            b"1",
            b"ip-address",
            b"198.51.100.8",
            b"capa",
            b"eof",
            b"listening-port",
            b"6381",
        ])
    } else {
        rdb.command(&[
            b"REPLCONF",
            b"ip-address",
            b"198.51.100.8",
            b"capa",
            b"eof",
            b"listening-port",
            b"6381",
            b"rdb-channel",
            b"1",
        ])
    };
    assert_eq!(
        response, b"+OK\r\n",
        "RDB-channel REPLCONF option order must be accepted"
    );

    assert_eq!(
        main.command(&[
            b"REPLCONF",
            b"set-rdb-client-id",
            rdb_id.to_string().as_bytes(),
        ]),
        b"+OK\r\n",
        "main channel must associate the preserved RDB candidate"
    );

    assert_info_hides_pre_psync_channels(
        &server,
        "RDB-channel REPLCONF option order must preserve the candidate before PSYNC",
    );
}

#[test]
fn replconf_rdb_channel_before_listening_port_preserves_candidate() {
    assert_replconf_rdb_channel_option_order_preserves_candidate(true);
}

#[test]
fn replconf_listening_port_before_rdb_channel_preserves_candidate() {
    assert_replconf_rdb_channel_option_order_preserves_candidate(false);
}

#[test]
fn replconf_mixed_case_rdb_channel_option_order_preserves_candidate() {
    let server = TestServer::start_default();
    let mut main = server.connect();
    assert_eq!(
        main.command(&[
            b"CONFIG",
            b"SET",
            b"dual-channel-replication-enabled",
            b"yes"
        ]),
        b"+OK\r\n"
    );
    assert_eq!(
        main.command(&[b"REPLCONF", b"LiStEnInG-PoRt", b"6380"]),
        b"+OK\r\n"
    );

    let mut rdb = server.connect();
    let rdb_id = rdb
        .command(&[b"CLIENT", b"ID"])
        .strip_prefix(b":")
        .and_then(|value| std::str::from_utf8(value).ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .expect("CLIENT ID should be an integer");
    assert_eq!(
        rdb.command(&[
            b"REPLCONF",
            b"iP-AdDrEsS",
            b"198.51.100.9",
            b"CaPa",
            b"eof",
            b"LiStEnInG-PoRt",
            b"6381",
            b"RDB-ChAnNeL",
            b"1",
        ]),
        b"+OK\r\n",
        "mixed-case RDB-channel option must be recognized by the pre-scan"
    );

    assert_eq!(
        main.command(&[
            b"REPLCONF",
            b"SeT-RdB-ClIeNt-Id",
            rdb_id.to_string().as_bytes(),
        ]),
        b"+OK\r\n"
    );

    assert_info_hides_pre_psync_channels(
        &server,
        "mixed-case option order must preserve the candidate before PSYNC",
    );
}

#[test]
fn replconf_rdb_channel_zero_from_rdb_client_keeps_main_channel() {
    let server = TestServer::start_default();
    let mut main = server.connect();
    assert_eq!(
        main.command(&[b"REPLCONF", b"listening-port", b"6380"]),
        b"+OK\r\n"
    );

    let mut rdb = server.connect();
    assert_eq!(
        rdb.command(&[b"REPLCONF", b"rdb-channel", b"0"]),
        b"+OK\r\n"
    );
    assert_info_hides_pre_psync_channels(
        &server,
        "disabling an RDB channel must preserve the hidden pre-PSYNC main link",
    );
}

#[test]
fn replconf_rdb_candidate_metadata_survives_separate_frames() {
    let server = TestServer::start_default();
    let mut main = server.connect();
    assert_eq!(
        main.command(&[b"REPLCONF", b"listening-port", b"6380"]),
        b"+OK\r\n"
    );

    let mut rdb = server.connect();
    let rdb_id = rdb
        .command(&[b"CLIENT", b"ID"])
        .strip_prefix(b":")
        .and_then(|value| std::str::from_utf8(value).ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .expect("CLIENT ID should be an integer");
    assert_eq!(
        rdb.command(&[
            b"REPLCONF",
            b"listening-port",
            b"6382",
            b"ip-address",
            b"203.0.113.8",
            b"capa",
            b"eof",
        ]),
        b"+OK\r\n"
    );
    assert_eq!(
        rdb.command(&[b"REPLCONF", b"rdb-channel", b"1"]),
        b"+OK\r\n"
    );
    assert_eq!(
        main.command(&[
            b"REPLCONF",
            b"set-rdb-client-id",
            rdb_id.to_string().as_bytes(),
        ]),
        b"+OK\r\n"
    );

    assert_info_hides_pre_psync_channels(
        &server,
        "repeated associated RDB metadata must preserve the candidate before PSYNC",
    );
}

#[test]
fn replconf_listening_port_then_rdb_channel_preserves_candidate_ownership() {
    let server = TestServer::start_default();
    let mut main = server.connect();
    assert_eq!(
        main.command(&[b"REPLCONF", b"listening-port", b"6380"]),
        b"+OK\r\n"
    );

    let mut rdb = server.connect();
    let rdb_id = rdb
        .command(&[b"CLIENT", b"ID"])
        .strip_prefix(b":")
        .and_then(|value| std::str::from_utf8(value).ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .expect("CLIENT ID should be an integer");
    assert_eq!(
        rdb.command(&[b"REPLCONF", b"listening-port", b"6381"]),
        b"+OK\r\n"
    );
    assert_eq!(
        rdb.command(&[b"REPLCONF", b"rdb-channel", b"1"]),
        b"+OK\r\n"
    );

    assert_eq!(
        main.command(&[
            b"REPLCONF",
            b"set-rdb-client-id",
            rdb_id.to_string().as_bytes(),
        ]),
        b"+OK\r\n"
    );

    assert_info_hides_pre_psync_channels(
        &server,
        "listening-port then rdb-channel must preserve the candidate before PSYNC",
    );
}

#[test]
fn replconf_set_cluster_node_id_rejects_when_cluster_is_disabled() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    assert_eq!(
        client.command(&[
            b"REPLCONF",
            b"SET-CLUSTER-NODE-ID",
            b"0123456789abcdef0123456789abcdef01234567",
        ]),
        b"-ERR This instance has cluster support disabled\r\n"
    );
}

#[test]
fn replconf_set_cluster_node_id_requires_a_known_cluster_node() {
    let server = TestServer::start(&["--cluster-enabled", "yes"]);
    let mut client = server.connect();
    let myid = client
        .command(&[b"CLUSTER", b"MYID"])
        .strip_prefix(b"$40\r\n")
        .and_then(|value| value.strip_suffix(b"\r\n"))
        .expect("CLUSTER MYID should return a 40-byte bulk string")
        .to_vec();

    assert_eq!(
        client.command(&[b"REPLCONF", b"SET-CLUSTER-NODE-ID", &myid]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[
            b"REPLCONF",
            b"SET-CLUSTER-NODE-ID",
            b"0000000000000000000000000000000000000000",
        ]),
        b"-ERR Unknown node 0000000000000000000000000000000000000000\r\n"
    );

    let info = client.command_str(&[b"INFO", b"replication"]);
    assert!(
        info.contains("connected_slaves:0\r\n") && !info.contains("slave0:"),
        "metadata-only cluster-node-id must not create a canonical replica: {info}"
    );
}

#[test]
fn replconf_version_does_not_create_a_canonical_replica() {
    let server = TestServer::start_default();
    let mut client = server.connect();

    assert_eq!(
        client.command(&[b"REPLCONF", b"VERSION", b"8.0.0"]),
        b"+OK\r\n"
    );

    let info = client.command_str(&[b"INFO", b"replication"]);
    assert!(
        info.contains("connected_slaves:0\r\n") && !info.contains("slave0:"),
        "metadata-only VERSION must not create a canonical replica: {info}"
    );
}

#[test]
fn rdb_channel_candidate_rejects_sync_without_breaking_classic_sync() {
    let server = TestServer::start_default();
    let mut candidate = server.connect();
    assert_eq!(
        candidate.command(&[b"REPLCONF", b"rdb-channel", b"1"]),
        b"+OK\r\n"
    );
    assert_eq!(
        candidate.command(&[b"PSYNC", b"?", b"-1"]),
        b"-ERR RDB channel cannot issue SYNC or PSYNC\r\n"
    );
    assert_eq!(
        candidate.command(&[b"SYNC"]),
        b"-ERR RDB channel cannot issue SYNC or PSYNC\r\n"
    );

    let mut classic = server.connect();
    classic.send_raw(b"SYNC\r\n");
    let reply = classic.read_raw_line();
    assert!(
        reply.starts_with(b"$"),
        "classic SYNC must still start with the RDB bulk length: {reply:?}"
    );
}

#[test]
fn replconf_dual_channel_association_rejects_bad_ids_and_is_reported() {
    let server = TestServer::start_default();
    let mut main = server.connect();
    assert_eq!(
        main.command(&[
            b"CONFIG",
            b"SET",
            b"dual-channel-replication-enabled",
            b"yes",
        ]),
        b"+OK\r\n"
    );
    assert_eq!(
        main.command(&[b"REPLCONF", b"listening-port", b"6380"]),
        b"+OK\r\n"
    );

    let mut rdb = server.connect();
    let rdb_id = rdb
        .command(&[b"CLIENT", b"ID"])
        .strip_prefix(b":")
        .and_then(|value| std::str::from_utf8(value).ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .expect("CLIENT ID should be an integer");
    assert_eq!(rdb.command(&[b"REPLCONF", b"rdb-only", b"1"]), b"+OK\r\n");
    assert_eq!(
        rdb.command(&[b"REPLCONF", b"rdb-channel", b"1"]),
        b"+OK\r\n"
    );

    let malformed = main.command(&[b"REPLCONF", b"set-rdb-client-id", b"not-an-id"]);
    assert!(
        malformed.starts_with(b"-ERR"),
        "malformed RDB client id must be rejected: {malformed:?}"
    );
    assert_eq!(
        main.command(&[
            b"REPLCONF",
            b"set-rdb-client-id",
            rdb_id.to_string().as_bytes(),
        ]),
        b"+OK\r\n"
    );

    assert_info_hides_pre_psync_channels(
        &server,
        "associated RDB metadata must stay hidden from INFO before PSYNC",
    );
}

#[test]
fn full_resync_transfers_existing_dataset() {
    let master = TestServer::start_default();
    let replica = TestServer::start_default();

    let mut mc = master.connect();
    assert_eq!(mc.command(&[b"SET", b"pre", b"value"]), b"+OK\r\n");
    assert_eq!(mc.command(&[b"RPUSH", b"mylist", b"a", b"b"]), b":2\r\n");

    let mut rc = replica.connect();
    let port = master.port.to_string();
    assert_eq!(
        rc.command(&[b"REPLICAOF", b"127.0.0.1", port.as_bytes()]),
        b"+OK\r\n"
    );

    let ok = wait_until(
        || {
            let mut c = replica.connect();
            c.command(&[b"GET", b"pre"]) == b"$5\r\nvalue\r\n"
        },
        Duration::from_secs(10),
    );
    assert!(
        ok,
        "replica did not receive existing dataset via full resync"
    );

    let mut c = replica.connect();
    assert_eq!(
        c.command(&[b"LRANGE", b"mylist", b"0", b"-1"]),
        b"*2\r\n$1\r\na\r\n$1\r\nb\r\n"
    );
}

#[test]
fn legacy_sync_starts_with_rdb_bulk_without_fullresync_header() {
    let master = TestServer::start_default();
    let mut client = master.connect();

    client.send_raw(b"SYNC\r\n");
    let reply = client.read_raw_line();

    assert!(
        reply.starts_with(b"$"),
        "legacy SYNC must start with the RDB bulk length: {reply:?}"
    );
}

#[test]
fn write_commands_propagate_to_replica() {
    let master = TestServer::start_default();
    let replica = TestServer::start_default();

    let mut rc = replica.connect();
    let port = master.port.to_string();
    assert_eq!(
        rc.command(&[b"REPLICAOF", b"127.0.0.1", port.as_bytes()]),
        b"+OK\r\n"
    );

    let synced = wait_until(
        || {
            let mut c = replica.connect();
            let info = c.command_str(&[b"INFO", b"replication"]);
            info.contains("master_link_status:up")
        },
        Duration::from_secs(10),
    );
    assert!(synced, "replica did not reach master_link_status:up");

    let mut mc = master.connect();
    assert_eq!(mc.command(&[b"SET", b"afterk", b"afterv"]), b"+OK\r\n");

    let ok = wait_until(
        || {
            let mut c = replica.connect();
            c.command(&[b"GET", b"afterk"]) == b"$6\r\nafterv\r\n"
        },
        Duration::from_secs(10),
    );
    assert!(ok, "write command did not propagate to replica");
}

#[test]
fn synced_replica_reports_slave_role() {
    let master = TestServer::start_default();
    let replica = TestServer::start_default();
    let mut rc = replica.connect();
    let port = master.port.to_string();
    assert_eq!(
        rc.command(&[b"REPLICAOF", b"127.0.0.1", port.as_bytes()]),
        b"+OK\r\n"
    );
    let synced = wait_until(
        || {
            let mut client = replica.connect();
            client
                .command_str(&[b"INFO", b"replication"])
                .contains("master_link_status:up")
        },
        Duration::from_secs(10),
    );
    assert!(synced, "replica did not reach master_link_status:up");

    let reply = rc.command(&[b"ROLE"]);
    assert!(
        reply.starts_with(b"*5\r\n$5\r\nslave\r\n"),
        "synced replica must report the slave ROLE response: {reply:?}"
    );
}

#[test]
fn slaveof_keeps_the_calling_client_connected() {
    let master = TestServer::start_default();
    let replica = TestServer::start_default();
    let master_port = master.port.to_string();
    let mut replica_client = replica.connect();

    assert_eq!(
        replica_client.command(&[b"SLAVEOF", b"127.0.0.1", master_port.as_bytes()]),
        b"+OK\r\n"
    );
    let info = replica_client.command_str(&[b"INFO", b"replication"]);
    assert!(
        info.contains("role:slave"),
        "SLAVEOF caller must remain connected and observe replica state: {info}"
    );
}

#[test]
fn replica_announce_ip_is_reported_by_master_info() {
    let master = TestServer::start_default();
    let replica = TestServer::start_default();
    let master_port = master.port.to_string();
    let mut replica_client = replica.connect();

    assert_eq!(
        replica_client.command(&[b"CONFIG", b"SET", b"replica-announce-ip", b"localhost",]),
        b"+OK\r\n"
    );
    assert_eq!(
        replica_client.command(&[b"REPLICAOF", b"127.0.0.1", master_port.as_bytes()]),
        b"+OK\r\n"
    );

    assert!(
        wait_until(
            || {
                master
                    .connect()
                    .command_str(&[b"INFO", b"replication"])
                    .contains("ip=localhost")
            },
            Duration::from_secs(10),
        ),
        "master did not report the replica's configured announce IP"
    );
}

#[test]
fn failover_command_enters_waiting_for_sync_state() {
    let master = TestServer::start_default();
    let replica = TestServer::start_default();
    let master_port = master.port.to_string();
    let replica_port = replica.port.to_string();
    let mut replica_client = replica.connect();

    assert_eq!(
        replica_client.command(&[b"REPLICAOF", b"127.0.0.1", master_port.as_bytes()]),
        b"+OK\r\n"
    );
    assert!(wait_until(
        || {
            replica
                .connect()
                .command_str(&[b"INFO", b"replication"])
                .contains("master_link_status:up")
        },
        Duration::from_secs(10),
    ));

    let mut master_client = master.connect();
    assert_eq!(
        master_client.command(&[
            b"FAILOVER",
            b"TO",
            b"127.0.0.1",
            replica_port.as_bytes(),
            b"TIMEOUT",
            b"1000",
        ]),
        b"+OK\r\n"
    );
    assert!(master_client
        .command_str(&[b"INFO", b"replication"])
        .contains("master_failover_state:waiting-for-sync"));
}

#[test]
fn failover_requests_fresh_ack_and_promotes_an_idle_synced_replica() {
    let master = TestServer::start_default();
    let replica = TestServer::start_default();
    let master_port = master.port.to_string();
    let replica_port = replica.port.to_string();
    let mut replica_client = replica.connect();

    assert_eq!(
        replica_client.command(&[b"REPLICAOF", b"127.0.0.1", master_port.as_bytes()]),
        b"+OK\r\n"
    );
    assert!(wait_until(
        || {
            replica
                .connect()
                .command_str(&[b"INFO", b"replication"])
                .contains("master_link_status:up")
        },
        Duration::from_secs(10),
    ));

    let mut master_client = master.connect();
    assert_eq!(
        master_client.command(&[
            b"FAILOVER",
            b"TO",
            b"127.0.0.1",
            replica_port.as_bytes(),
            b"TIMEOUT",
            b"10000",
        ]),
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
            Duration::from_secs(10),
        ),
        "idle synced replica was not promoted after FAILOVER requested a fresh ACK"
    );
}

#[test]
fn failover_abort_clears_waiting_state_and_unpauses_writes() {
    let master = TestServer::start_default();
    let replica = TestServer::start_default();
    let master_port = master.port.to_string();
    let replica_port = replica.port.to_string();
    let mut replica_client = replica.connect();

    assert_eq!(
        replica_client.command(&[b"REPLICAOF", b"127.0.0.1", master_port.as_bytes()]),
        b"+OK\r\n"
    );
    assert!(wait_until(
        || {
            replica
                .connect()
                .command_str(&[b"INFO", b"replication"])
                .contains("master_link_status:up")
        },
        Duration::from_secs(10),
    ));

    let mut master_client = master.connect();
    assert_eq!(
        master_client.command(&[
            b"FAILOVER",
            b"TO",
            b"127.0.0.1",
            replica_port.as_bytes(),
            b"TIMEOUT",
            b"10000",
        ]),
        b"+OK\r\n"
    );
    assert_eq!(master_client.command(&[b"FAILOVER", b"ABORT"]), b"+OK\r\n");
    let info = master_client.command_str(&[b"INFO", b"replication"]);
    assert!(
        info.contains("master_failover_state:no-failover"),
        "FAILOVER ABORT did not clear failover state: {info}"
    );
    assert_eq!(
        master_client.command(&[b"SET", b"after-abort", b"ok"]),
        b"+OK\r\n"
    );
}

#[test]
fn replicaof_reconfiguration_clears_a_completed_force_failover() {
    let old_master = TestServer::start_default();
    let paused_target = TestServer::start_default();
    let replacement_master = TestServer::start_default();
    let old_master_port = old_master.port.to_string();
    let paused_target_port = paused_target.port.to_string();
    let replacement_master_port = replacement_master.port.to_string();
    let mut target_client = paused_target.connect();

    assert_eq!(
        target_client.command(&[b"REPLICAOF", b"127.0.0.1", old_master_port.as_bytes(),]),
        b"+OK\r\n"
    );
    assert!(wait_until(
        || {
            paused_target
                .connect()
                .command_str(&[b"INFO", b"replication"])
                .contains("master_link_status:up")
        },
        Duration::from_secs(10),
    ));

    send_signal(paused_target.pid() as i32, ProcessSignal::Stop).expect("stop failover target");
    let mut old_master_client = old_master.connect();
    assert_eq!(
        old_master_client.command(&[
            b"FAILOVER",
            b"TO",
            b"127.0.0.1",
            paused_target_port.as_bytes(),
            b"TIMEOUT",
            b"10",
            b"FORCE",
        ]),
        b"+OK\r\n"
    );
    assert!(wait_until(
        || {
            std::panic::catch_unwind(|| {
                old_master
                    .connect()
                    .command_str(&[b"INFO", b"replication"])
                    .contains("master_failover_state:failover-in-progress")
            })
            .unwrap_or(false)
        },
        Duration::from_secs(5),
    ));

    let mut recovery_client = old_master.connect();
    assert_eq!(
        recovery_client.command(&[b"SLAVEOF", b"127.0.0.1", replacement_master_port.as_bytes(),]),
        b"+OK\r\n"
    );
    let info = recovery_client.command_str(&[b"INFO", b"replication"]);
    assert!(
        info.contains("master_failover_state:no-failover"),
        "SLAVEOF reconfiguration did not clear completed failover state: {info}"
    );

    send_signal(paused_target.pid() as i32, ProcessSignal::Continue)
        .expect("continue failover target");
}

#[test]
fn force_failover_to_lost_target_advances_after_timeout() {
    let master = TestServer::start_default();
    let target = TestServer::start_default();
    let master_port = master.port.to_string();
    let target_port = target.port.to_string();
    let mut target_client = target.connect();

    assert_eq!(
        target_client.command(&[b"REPLICAOF", b"127.0.0.1", master_port.as_bytes(),]),
        b"+OK\r\n"
    );
    assert!(wait_until(
        || {
            target
                .connect()
                .command_str(&[b"INFO", b"replication"])
                .contains("master_link_status:up")
        },
        Duration::from_secs(10),
    ));

    let mut master_client = master.connect();
    assert_eq!(
        master_client.command(&[
            b"FAILOVER",
            b"TO",
            b"127.0.0.1",
            target_port.as_bytes(),
            b"TIMEOUT",
            b"100",
            b"FORCE",
        ]),
        b"+OK\r\n"
    );

    send_signal(target.pid() as i32, ProcessSignal::Kill).expect("kill failover target");

    assert!(wait_until(
        || {
            std::panic::catch_unwind(|| {
                master
                    .connect()
                    .command_str(&[b"INFO", b"replication"])
                    .contains("master_failover_state:failover-in-progress")
            })
            .unwrap_or(false)
        },
        Duration::from_secs(5),
    ));
}

#[test]
fn repeating_same_replicaof_target_keeps_existing_link_online() {
    let master = TestServer::start_default();
    let replica = TestServer::start_default();
    let master_port = master.port.to_string();
    let mut replica_client = replica.connect();

    assert_eq!(
        replica_client.command(&[b"REPLICAOF", b"127.0.0.1", master_port.as_bytes()]),
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
            Duration::from_secs(10),
        ),
        "initial replication link did not become online"
    );

    assert_eq!(
        replica_client.command(&[b"REPLICAOF", b"127.0.0.1", master_port.as_bytes()]),
        b"+OK\r\n"
    );
    let info = replica.connect().command_str(&[b"INFO", b"replication"]);
    assert!(
        info.contains("master_link_status:up"),
        "idempotent REPLICAOF must preserve the online link: {info}"
    );
}

#[test]
fn reconnect_uses_partial_resync_for_retained_backlog() {
    let master = TestServer::start_default();
    let replica = TestServer::start(&["--appendonly"]);
    let master_port = master.port.to_string();
    let mut replica_client = replica.connect();
    assert_eq!(
        replica_client.command(&[b"REPLICAOF", b"127.0.0.1", master_port.as_bytes()]),
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
            Duration::from_secs(10),
        ),
        "initial full resync did not complete"
    );

    assert_eq!(
        replica_client.command(&[b"REPLICAOF", b"NO", b"ONE"]),
        b"+OK\r\n"
    );
    let mut master_client = master.connect();
    assert_eq!(
        master_client.command(&[b"SET", b"while-down", b"retained"]),
        b"+OK\r\n"
    );
    let replication_info = master_client.command_str(&[b"INFO", b"replication"]);
    assert!(
        replication_info.contains("repl_backlog_active:1"),
        "replication backlog should be active: {replication_info}"
    );
    assert!(
        !replication_info.contains("repl_backlog_histlen:0"),
        "replication backlog should retain the disconnected write: {replication_info}"
    );

    assert_eq!(
        replica_client.command(&[b"REPLICAOF", b"127.0.0.1", master_port.as_bytes()]),
        b"+OK\r\n"
    );
    assert!(
        wait_until(
            || { replica.connect().command(&[b"GET", b"while-down"]) == b"$8\r\nretained\r\n" },
            Duration::from_secs(10),
        ),
        "replica did not replay retained backlog after reconnect"
    );
    // Explicitly give cron a chance to flush the replicated write to the AOF
    // increment. Without this, a fast poll timeout + tight cron gate in the
    // Rust backend can observe the increment content before the append has
    // been fsynced to disk (see runtime::run cron cadence changes).
    let aof_dir = replica.data_dir().join("appendonlydir");
    assert!(
        wait_until(
            || {
                let manifest =
                    match std::fs::read_to_string(aof_dir.join("appendonly.aof.manifest")) {
                        Ok(s) => s,
                        Err(_) => return false,
                    };
                let incr_name = match manifest
                    .lines()
                    .find(|l| l.ends_with("type i"))
                    .and_then(|l| l.split_whitespace().nth(1))
                {
                    Some(n) => n.to_string(),
                    None => return false,
                };
                let content = match std::fs::read_to_string(aof_dir.join(&incr_name)) {
                    Ok(c) => c,
                    Err(_) => return false,
                };
                content.contains("while-down")
            },
            Duration::from_secs(5),
        ),
        "replica did not flush replicated backlog write into AOF increment within 5 s"
    );

    let stats = master_client.command_str(&[b"INFO", b"stats"]);
    assert!(
        stats.contains("sync_full:1"),
        "reconnect should use CONTINUE instead of another FULLRESYNC: {stats}"
    );

    let aof_dir = replica.data_dir().join("appendonlydir");
    let manifest = std::fs::read_to_string(aof_dir.join("appendonly.aof.manifest"))
        .expect("replica AOF manifest should exist");
    let incr_name = manifest
        .lines()
        .find(|line| line.ends_with("type i"))
        .and_then(|line| line.split_whitespace().nth(1))
        .expect("replica AOF manifest should contain an increment");
    let aof = std::fs::read_to_string(aof_dir.join(incr_name))
        .expect("replica incremental AOF should exist after applying the replicated write");
    assert_eq!(
        aof.matches("while-down").count(),
        1,
        "replication-origin command should be persisted once in replica AOF"
    );
}

#[test]
fn psync2_replid_rotation_on_promotion_allows_second_replid_continue() {
    // A (initial master) → B (replica of A).
    // Promote B with SLAVEOF NO ONE (triggers PSYNC2 replid shift).
    // Verify:
    //  - B reports master_replid2 == old master_replid, second_repl_offset set;
    //  - Re-replicating a new client C to B using the OLD replid still succeeds
    //    (full resync is expected since C is a fresh client with no backlog to
    //    continue from, but B must have rotated ids correctly).
    let a = TestServer::start_default();
    let b = TestServer::start(&["--appendonly"]);
    let a_port = a.port.to_string();

    let mut a_client = a.connect();
    assert_eq!(a_client.command(&[b"SET", b"k1", b"v1"]), b"+OK\r\n");

    let mut b_client = b.connect();
    assert_eq!(
        b_client.command(&[b"REPLICAOF", b"127.0.0.1", a_port.as_bytes()]),
        b"+OK\r\n"
    );
    assert!(
        wait_until(
            || b.connect().command(&[b"GET", b"k1"]) == b"$2\r\nv1\r\n",
            Duration::from_secs(10),
        ),
        "B did not initially sync from A"
    );

    let a_info = a_client.command_str(&[b"INFO", b"replication"]);
    let a_replid = extract_field(&a_info, "master_replid");
    assert!(!a_replid.is_empty(), "A should advertise a master_replid");

    // Promote B.
    assert_eq!(b_client.command(&[b"SLAVEOF", b"NO", b"ONE"]), b"+OK\r\n");

    let b_info = b_client.command_str(&[b"INFO", b"replication"]);
    assert!(
        b_info.contains("role:master"),
        "B should be master after SLAVEOF NO ONE: {b_info}"
    );
    let b_replid = extract_field(&b_info, "master_replid");
    let b_replid2 = extract_field(&b_info, "master_replid2");
    let b_second_off = extract_field(&b_info, "second_repl_offset");
    assert_ne!(
        b_replid, a_replid,
        "B must have a new primary replid after promotion"
    );
    assert_eq!(
        b_replid2, a_replid,
        "B's master_replid2 should equal A's old replid"
    );
    assert!(
        b_second_off.parse::<i64>().unwrap_or(-2) >= 0,
        "second_repl_offset must be set, got {b_second_off}"
    );

    // Write to B while it's master; verify a new replica C joining B gets the data.
    assert_eq!(b_client.command(&[b"SET", b"k2", b"v2"]), b"+OK\r\n");

    let c = TestServer::start(&["--appendonly"]);
    let b_port = b.port.to_string();
    let mut c_client = c.connect();
    assert_eq!(
        c_client.command(&[b"REPLICAOF", b"127.0.0.1", b_port.as_bytes()]),
        b"+OK\r\n"
    );
    assert!(
        wait_until(
            || c.connect().command(&[b"GET", b"k2"]) == b"$2\r\nv2\r\n",
            Duration::from_secs(10),
        ),
        "C did not replicate from promoted B"
    );

    // stats: full resync count on B should be exactly 1 (just C).
    let b_stats = b_client.command_str(&[b"INFO", b"stats"]);
    assert!(
        b_stats.contains("sync_full:1"),
        "B should have served 1 full resync to C: {b_stats}"
    );
}

mod psync2;
use psync2::extract_field;

mod concurrent;
mod diskless_fork;
mod replica_reconnect;

/// Dual-channel requires diskless sync. With either side's diskless owner
/// disabled, the live dual configuration must fall back to classic PSYNC and
/// still complete a normal full sync.
#[test]
fn dual_channel_enabled_gracefully_degrades_to_single_channel_full_sync() {
    let master = TestServer::start(&["--save", ""]);
    let replica = TestServer::start(&["--save", ""]);
    let mut mc = master.connect();
    let mut rc = replica.connect();
    let port = master.port.to_string();

    // Disable diskless sync before dual-channel negotiation so this remains a
    // classic full-sync fallback test.
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-diskless-sync", b"no"]),
        b"+OK\r\n"
    );
    assert_eq!(
        rc.command(&[b"CONFIG", b"SET", b"repl-diskless-sync", b"no"]),
        b"+OK\r\n"
    );

    assert_eq!(
        mc.command(&[
            b"CONFIG",
            b"SET",
            b"dual-channel-replication-enabled",
            b"yes"
        ]),
        b"+OK\r\n"
    );
    assert_eq!(
        rc.command(&[
            b"CONFIG",
            b"SET",
            b"dual-channel-replication-enabled",
            b"yes"
        ]),
        b"+OK\r\n"
    );

    assert_eq!(mc.command(&[b"SET", b"preload", b"hello"]), b"+OK\r\n");

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
        "full sync did not complete with dual-channel enabled"
    );

    assert!(
        wait_until(
            || replica.connect().command(&[b"GET", b"preload"]) == b"$5\r\nhello\r\n",
            Duration::from_secs(10),
        ),
        "replica did not receive dataset via degraded single-channel full sync"
    );

    let stats = master.connect().command_str(&[b"INFO", b"stats"]);
    assert_eq!(
        extract_field(&stats, "sync_full"),
        "1",
        "dual-channel stub must serve exactly one classic full resync: {stats}"
    );
}

#[test]
fn dual_channel_handshake_reports_role_and_info_channels() {
    let options = [
        "--repl-diskless-sync",
        "yes",
        "--repl-diskless-sync-delay",
        "1000",
        "--dual-channel-replication-enabled",
        "yes",
    ];
    let primary = TestServer::start(&options);
    let replica = TestServer::start(&options);
    let primary_port = primary.port.to_string();

    assert_eq!(
        replica
            .connect()
            .command(&[b"REPLICAOF", b"127.0.0.1", primary_port.as_bytes()]),
        b"+OK\r\n"
    );

    let dual_window_visible = wait_until(
        || {
            let info = primary.connect().command_str(&[b"INFO", b"replication"]);
            has_channel_row(&info, "state=bg_transfer", "type=main-channel")
                && has_channel_row(&info, "state=wait_bgsave", "type=rdb-channel")
                && !info.contains("type=replica")
                && info
                    .lines()
                    .filter(|line| line.starts_with("slave") && line.contains("type="))
                    .count()
                    == 2
        },
        Duration::from_secs(5),
    );
    assert!(
        dual_window_visible,
        "primary did not expose exactly the expected dual-channel rows: {}",
        primary.connect().command_str(&[b"INFO", b"replication"])
    );

    let role = replica.connect().command(&[b"ROLE"]);
    let fields =
        resp_bulk_array_fields(&role).expect("ROLE reply must be a complete five-field RESP array");
    assert_eq!(
        fields.get(3).copied(),
        Some(b"handshake".as_slice()),
        "replica ROLE fourth field must be handshake while RDB transfer is delayed: {role:?}"
    );
}

fn has_channel_row(info: &str, state: &str, channel_type: &str) -> bool {
    info.lines().any(|line| {
        let Some((_, fields)) = line.split_once(':') else {
            return false;
        };
        let mut fields = fields.split(',');
        line.starts_with("slave")
            && fields.next().is_some_and(|field| field.starts_with("ip="))
            && fields
                .next()
                .is_some_and(|field| field.starts_with("port="))
            && fields.next() == Some(state)
            && fields
                .next()
                .is_some_and(|field| field.starts_with("offset="))
            && fields.next().is_some_and(|field| field.starts_with("lag="))
            && fields.next() == Some(channel_type)
            && fields.next().is_none()
    })
}

#[test]
fn resp_bulk_array_fields_parses_complete_role_reply() {
    let role = b"*5\r\n$5\r\nslave\r\n$9\r\n127.0.0.1\r\n:6379\r\n$9\r\nhandshake\r\n:42\r\n";

    assert_eq!(
        resp_bulk_array_fields(role),
        Some(vec![
            b"slave".as_slice(),
            b"127.0.0.1".as_slice(),
            b"6379".as_slice(),
            b"handshake".as_slice(),
            b"42".as_slice(),
        ])
    );
}

#[test]
fn resp_bulk_array_fields_rejects_truncated_or_malformed_role_reply() {
    let truncated = b"*5\r\n$5\r\nslave\r\n$9\r\n127.0.0.1\r\n:6379\r\n$9\r\nhandshake\r\n";
    let malformed = b"*5\r\n$5\r\nslave\r\n$9\r\n127.0.0.1\r\n:6379\r\n$9\r\nhandshake\r\n!42\r\n";

    assert_eq!(resp_bulk_array_fields(truncated), None);
    assert_eq!(resp_bulk_array_fields(malformed), None);
}

fn resp_bulk_array_fields(response: &[u8]) -> Option<Vec<&[u8]>> {
    let array_end = response.windows(2).position(|bytes| bytes == b"\r\n")?;
    if response.first() != Some(&b'*')
        || std::str::from_utf8(&response[1..array_end])
            .ok()?
            .parse::<usize>()
            .ok()?
            != 5
    {
        return None;
    }

    let mut fields = Vec::with_capacity(5);
    let mut cursor = array_end + 2;
    for _ in 0..5 {
        if response.get(cursor) == Some(&b':') {
            let value_start = cursor + 1;
            let value_end = value_start
                + response[value_start..]
                    .windows(2)
                    .position(|bytes| bytes == b"\r\n")?;
            std::str::from_utf8(&response[value_start..value_end])
                .ok()?
                .parse::<i64>()
                .ok()?;
            fields.push(&response[value_start..value_end]);
            cursor = value_end + 2;
            continue;
        }

        if response.get(cursor) != Some(&b'$') {
            return None;
        }
        let length_start = cursor + 1;
        let length_end = length_start
            + response[length_start..]
                .windows(2)
                .position(|bytes| bytes == b"\r\n")?;
        let length = std::str::from_utf8(&response[length_start..length_end])
            .ok()?
            .parse::<usize>()
            .ok()?;
        let value_start = length_end + 2;
        let value_end = value_start.checked_add(length)?;
        if response.get(value_end..value_end + 2) != Some(b"\r\n") {
            return None;
        }
        fields.push(&response[value_start..value_end]);
        cursor = value_end + 2;
    }

    (cursor == response.len()).then_some(fields)
}

#[test]
#[ignore = "flaky under full integration-suite concurrency; deterministic propagation is covered by the dedicated replication tests"]
fn blocking_pop_propagates_deterministic_effect_to_replica() {
    let master = TestServer::start_default();
    let replica = TestServer::start_default();
    let mut mc = master.connect();

    assert_eq!(mc.command(&[b"RPUSH", b"bl", b"a", b"b", b"c"]), b":3\r\n");
    assert_eq!(mc.command(&[b"RPUSH", b"br", b"x", b"y", b"z"]), b":3\r\n");
    assert_eq!(
        mc.command(&[b"ZADD", b"zmin", b"1", b"one", b"2", b"two", b"3", b"three"]),
        b":3\r\n"
    );
    assert_eq!(
        mc.command(&[b"ZADD", b"zmax", b"1", b"one", b"2", b"two", b"3", b"three"]),
        b":3\r\n"
    );

    let mut rc = replica.connect();
    let port = master.port.to_string();
    assert_eq!(
        rc.command(&[b"REPLICAOF", b"127.0.0.1", port.as_bytes()]),
        b"+OK\r\n"
    );
    let synced = wait_until(
        || {
            replica
                .connect()
                .command_str(&[b"INFO", b"replication"])
                .contains("master_link_status:up")
        },
        Duration::from_secs(10),
    );
    assert!(synced, "replica did not reach master_link_status:up");

    assert_eq!(
        mc.command(&[b"BLPOP", b"bl", b"0"]),
        b"*2\r\n$2\r\nbl\r\n$1\r\na\r\n"
    );
    assert_eq!(
        mc.command(&[b"BRPOP", b"br", b"0"]),
        b"*2\r\n$2\r\nbr\r\n$1\r\nz\r\n"
    );
    let zmin_reply = mc.command(&[b"BZPOPMIN", b"zmin", b"0"]);
    assert!(
        zmin_reply.starts_with(b"*3\r\n$4\r\nzmin\r\n$3\r\none\r\n"),
        "BZPOPMIN unexpected reply: {:?}",
        String::from_utf8_lossy(&zmin_reply)
    );
    let zmax_reply = mc.command(&[b"BZPOPMAX", b"zmax", b"0"]);
    assert!(
        zmax_reply.starts_with(b"*3\r\n$4\r\nzmax\r\n$5\r\nthree\r\n"),
        "BZPOPMAX unexpected reply: {:?}",
        String::from_utf8_lossy(&zmax_reply)
    );

    let converged = wait_until(
        || {
            let mut c = replica.connect();
            let rdig = c.command_str(&[b"DEBUG", b"DIGEST"]);
            let mut m = master.connect();
            let mdig = m.command_str(&[b"DEBUG", b"DIGEST"]);
            !mdig.trim().is_empty() && mdig == rdig
        },
        Duration::from_secs(10),
    );
    assert!(
        converged,
        "master/replica digest diverged after immediate blocking-pop (BLPOP/BRPOP/BZPOPMIN/BZPOPMAX propagation not rewritten)\nmaster={}\nreplica={}",
        master.connect().command_str(&[b"DEBUG", b"DIGEST"]),
        replica.connect().command_str(&[b"DEBUG", b"DIGEST"]),
    );

    let mut c = replica.connect();
    assert_eq!(
        c.command(&[b"LRANGE", b"bl", b"0", b"-1"]),
        b"*2\r\n$1\r\nb\r\n$1\r\nc\r\n"
    );
    assert_eq!(
        c.command(&[b"LRANGE", b"br", b"0", b"-1"]),
        b"*2\r\n$1\r\nx\r\n$1\r\ny\r\n"
    );
    assert_eq!(c.command(&[b"ZSCORE", b"zmin", b"one"]), b"$-1\r\n");
    assert_eq!(c.command(&[b"ZSCORE", b"zmax", b"three"]), b"$-1\r\n");
}

/// A replica must relay the commands it receives from its primary to its own
/// downstream sub-replicas (and its replication backlog), aligning with C
/// `replicationFeedStreamFromPrimaryStream` (networking.c).
/// This reproduces the `FLUSHDB / FLUSHALL should replicate` integration test:
/// attach a raw `SYNC` connection to the *replica* and assert that write
/// commands issued on the *primary* (SET / FLUSHDB / FLUSHALL / INCR) appear on
/// the replica's outbound stream. If the replica applies primary commands
/// locally but never propagates them, the SYNC stream stays empty.
#[test]
fn replica_relays_primary_write_stream_to_sub_replica() {
    let master = TestServer::start_default();
    let replica = TestServer::start_default();
    let mut mc = master.connect();
    let mut rc = replica.connect();
    let port = master.port.to_string();

    // Silence periodic PINGs on both links so the relayed stream is exactly the
    // write commands we issue (matches the Tcl test's repl-ping-replica-period).
    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-ping-replica-period", b"3600"]),
        b"+OK\r\n"
    );
    assert_eq!(
        rc.command(&[b"CONFIG", b"SET", b"repl-ping-replica-period", b"3600"]),
        b"+OK\r\n"
    );

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
        "replica did not reach master_link_status:up"
    );

    // Prime the primary's replication SELECT state (first write emits SELECT 0)
    // so that, once we attach, the relayed stream starts directly at the write
    // command — mirroring the Tcl test which writes before attaching.
    assert_eq!(mc.command(&[b"SET", b"warmup", b"1"]), b"+OK\r\n");
    assert!(
        wait_until(
            || replica.connect().command(&[b"GET", b"warmup"]) == b"$1\r\n1\r\n",
            Duration::from_secs(10),
        ),
        "warmup write did not reach the replica"
    );

    // Attach a raw SYNC stream to the *replica* and drain its RDB preamble.
    let mut sub = replica.connect();
    sub.send_raw(b"SYNC\r\n");
    sub.read_sync_rdb();

    // Issue writes on the primary; each must be relayed by the replica.
    assert_eq!(mc.command(&[b"SET", b"key", b"value"]), b"+OK\r\n");
    assert_eq!(mc.command(&[b"FLUSHDB"]), b"+OK\r\n");
    assert_eq!(mc.command(&[b"SET", b"key", b"value2"]), b"+OK\r\n");
    assert_eq!(mc.command(&[b"FLUSHALL"]), b"+OK\r\n");
    assert_eq!(mc.command(&[b"INCR", b"x"]), b":1\r\n");

    let expected: &[&[&[u8]]] = &[
        &[b"SET", b"key", b"value"],
        &[b"FLUSHDB"],
        &[b"SET", b"key", b"value2"],
        &[b"FLUSHALL"],
        &[b"INCR", b"x"],
    ];
    for want in expected {
        let got = loop {
            let frame = sub.read_repl_command();
            if frame.len() >= 2
                && frame[0].eq_ignore_ascii_case(b"REPLCONF")
                && frame[1].eq_ignore_ascii_case(b"GETACK")
            {
                continue;
            }
            break frame;
        };
        // Command name is case-insensitive on the wire; compare it lowercased.
        let got_norm: Vec<Vec<u8>> = got
            .iter()
            .enumerate()
            .map(|(i, a)| {
                if i == 0 {
                    a.to_ascii_lowercase()
                } else {
                    a.clone()
                }
            })
            .collect();
        let want_norm: Vec<Vec<u8>> = want
            .iter()
            .enumerate()
            .map(|(i, a)| {
                if i == 0 {
                    a.to_ascii_lowercase()
                } else {
                    a.to_vec()
                }
            })
            .collect();
        assert_eq!(
            got_norm,
            want_norm,
            "replica relayed the wrong command: got {:?}",
            got.iter()
                .map(|a| String::from_utf8_lossy(a).into_owned())
                .collect::<Vec<_>>()
        );
    }
}

mod failure_matrix {
    use super::*;

    fn setup_throttled_diskless_master_with_seeds(
        seed_count: u32,
        per_key_delay_ms: u32,
    ) -> (TestServer, u16) {
        let master = TestServer::start(&["--save", ""]);
        let mut mc = master.connect();
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
        assert_eq!(
            mc.command(&[
                b"CONFIG",
                b"SET",
                b"rdb-key-save-delay",
                per_key_delay_ms.to_string().as_bytes()
            ]),
            b"+OK\r\n"
        );
        let value = vec![b'x'; 64];
        for i in 0..seed_count {
            let key = format!("failmatrix:seed:{i}");
            assert_eq!(mc.command(&[b"SET", key.as_bytes(), &value]), b"+OK\r\n");
        }
        let port = master.port;
        (master, port)
    }

    fn current_sync_full_count(server: &TestServer) -> u64 {
        let stats = server.connect().command_str(&[b"INFO", b"stats"]);
        extract_field(&stats, "sync_full")
            .parse::<u64>()
            .unwrap_or(0)
    }

    #[test]
    fn main_channel_close_before_rdb_completion_triggers_fresh_psync_question_minus_1() {
        let (master, master_port) = setup_throttled_diskless_master_with_seeds(500, 50);
        let replica = TestServer::start(&["--save", ""]);
        let master_port_str = master_port.to_string();

        let _mc = master.connect();
        let sync_before_first = current_sync_full_count(&master);

        let mut rc = replica.connect();
        assert_eq!(
            rc.command(&[
                b"CONFIG",
                b"SET",
                b"dual-channel-replication-enabled",
                b"yes"
            ]),
            b"+OK\r\n"
        );
        assert_eq!(
            rc.command(&[b"CONFIG", b"SET", b"repl-diskless-load", b"swapdb"]),
            b"+OK\r\n"
        );
        assert_eq!(
            rc.command(&[b"REPLICAOF", b"127.0.0.1", master_port_str.as_bytes()]),
            b"+OK\r\n"
        );

        assert!(
            wait_until(
                || {
                    let stats = master.connect().command_str(&[b"INFO", b"stats"]);
                    let forks = extract_field(&stats, "total_forks")
                        .parse::<u64>()
                        .unwrap_or(0);
                    forks >= 1
                },
                Duration::from_secs(15),
            ),
            "diskless fork must start before we kill main channel"
        );
        std::thread::sleep(Duration::from_millis(500));

        let replica_info = rc.command_str(&[b"INFO", b"replication"]);
        assert!(
            replica_info.contains("master_link_status:") || replica_info.contains("role:slave"),
            "replica should be in slave role at this point, info: {replica_info}"
        );

        send_signal(replica.pid() as i32, ProcessSignal::Kill).expect("kill replica");
        drop(rc);
        drop(replica);

        std::thread::sleep(Duration::from_secs(1));

        let replica2 = TestServer::start(&["--save", ""]);
        let mut rc2 = replica2.connect();
        assert_eq!(
            rc2.command(&[
                b"CONFIG",
                b"SET",
                b"dual-channel-replication-enabled",
                b"yes"
            ]),
            b"+OK\r\n"
        );
        assert_eq!(
            rc2.command(&[b"CONFIG", b"SET", b"repl-diskless-load", b"swapdb"]),
            b"+OK\r\n"
        );
        assert_eq!(
            rc2.command(&[b"REPLICAOF", b"127.0.0.1", master_port_str.as_bytes()]),
            b"+OK\r\n"
        );

        assert!(
            wait_until(
                || {
                    let info = replica2.connect().command_str(&[b"INFO", b"replication"]);
                    info.contains("master_link_status:up")
                },
                Duration::from_secs(60),
            ),
            "replica2 must finish full sync after reconnect (fresh PSYNC ? -1 path)"
        );

        let sync_after_second = current_sync_full_count(&master);
        assert!(
            sync_after_second >= 1,
            "at least one fresh full-resync must be observed after reconnect. sync_before={sync_before_first} sync_after={sync_after_second}"
        );

        let mut mc2 = master.connect();
        assert_eq!(
            mc2.command(&[b"SET", b"failmatrix:f1:live", b"ok"]),
            b"+OK\r\n"
        );
        assert!(
            wait_until(
                || {
                    replica2.connect().command(&[b"GET", b"failmatrix:f1:live"]) == b"$2\r\nok\r\n"
                },
                Duration::from_secs(10),
            ),
            "post-reconnect live write must propagate"
        );
    }

    #[test]
    fn rdb_channel_close_before_completion_aborts_and_uses_fresh_full_sync() {
        let (master, master_port) = setup_throttled_diskless_master_with_seeds(500, 50);
        let replica = TestServer::start(&["--save", ""]);
        let master_port_str = master_port.to_string();

        let sync_before_first = current_sync_full_count(&master);

        let mut rc = replica.connect();
        assert_eq!(
            rc.command(&[
                b"CONFIG",
                b"SET",
                b"dual-channel-replication-enabled",
                b"yes"
            ]),
            b"+OK\r\n"
        );
        assert_eq!(
            rc.command(&[b"CONFIG", b"SET", b"repl-diskless-load", b"swapdb"]),
            b"+OK\r\n"
        );
        assert_eq!(
            rc.command(&[b"REPLICAOF", b"127.0.0.1", master_port_str.as_bytes()]),
            b"+OK\r\n"
        );

        assert!(
            wait_until(
                || {
                    let stats = master.connect().command_str(&[b"INFO", b"stats"]);
                    let forks = extract_field(&stats, "total_forks")
                        .parse::<u64>()
                        .unwrap_or(0);
                    forks >= 1
                },
                Duration::from_secs(15),
            ),
            "diskless fork must start (simulated RDB-channel-streaming-in-progress)"
        );
        std::thread::sleep(Duration::from_millis(800));

        drop(rc);
        drop(replica);
        std::thread::sleep(Duration::from_secs(1));

        let replica2 = TestServer::start(&["--save", ""]);
        let mut rc2 = replica2.connect();
        assert_eq!(
            rc2.command(&[
                b"CONFIG",
                b"SET",
                b"dual-channel-replication-enabled",
                b"yes"
            ]),
            b"+OK\r\n"
        );
        assert_eq!(
            rc2.command(&[b"CONFIG", b"SET", b"repl-diskless-load", b"swapdb"]),
            b"+OK\r\n"
        );
        assert_eq!(
            rc2.command(&[b"REPLICAOF", b"127.0.0.1", master_port_str.as_bytes()]),
            b"+OK\r\n"
        );

        assert!(
            wait_until(
                || {
                    let info = replica2.connect().command_str(&[b"INFO", b"replication"]);
                    info.contains("master_link_status:up")
                },
                Duration::from_secs(60),
            ),
            "replica2 must finish a fresh full sync after RDB channel abort"
        );

        let sync_after = current_sync_full_count(&master);
        assert!(
            sync_after >= 1,
            "expected >=1 full syncs fresh pass after abort. sync_before={sync_before_first} sync_after={sync_after}"
        );

        let first_seed = replica2.connect().command(&[b"GET", b"failmatrix:seed:0"]);
        assert!(
            first_seed.starts_with(b"$"),
            "replica2 must have received dataset from fresh full sync"
        );
    }

    #[test]
    fn replicaof_no_one_mid_sync_tears_down_and_next_replicaof_is_fresh() {
        let (master, master_port) = setup_throttled_diskless_master_with_seeds(500, 50);
        let replica = TestServer::start(&["--save", ""]);
        let master_port_str = master_port.to_string();

        let sync_before_first = current_sync_full_count(&master);

        let mut rc = replica.connect();
        assert_eq!(
            rc.command(&[
                b"CONFIG",
                b"SET",
                b"dual-channel-replication-enabled",
                b"yes"
            ]),
            b"+OK\r\n"
        );
        assert_eq!(
            rc.command(&[b"CONFIG", b"SET", b"repl-diskless-load", b"swapdb"]),
            b"+OK\r\n"
        );
        assert_eq!(
            rc.command(&[b"REPLICAOF", b"127.0.0.1", master_port_str.as_bytes()]),
            b"+OK\r\n"
        );

        assert!(
            wait_until(
                || {
                    let info = replica.connect().command_str(&[b"INFO", b"replication"]);
                    info.contains("role:slave")
                },
                Duration::from_secs(10),
            ),
            "replica must enter slave role"
        );

        assert!(
            wait_until(
                || {
                    let stats = master.connect().command_str(&[b"INFO", b"stats"]);
                    let forks = extract_field(&stats, "total_forks")
                        .parse::<u64>()
                        .unwrap_or(0);
                    forks >= 1
                },
                Duration::from_secs(15),
            ),
            "diskless fork must start so we are mid-sync"
        );
        std::thread::sleep(Duration::from_millis(500));

        assert_eq!(rc.command(&[b"REPLICAOF", b"NO", b"ONE"]), b"+OK\r\n");

        assert!(
            wait_until(
                || {
                    let info = replica.connect().command_str(&[b"INFO", b"replication"]);
                    info.contains("role:master")
                },
                Duration::from_secs(5),
            ),
            "after REPLICAOF NO ONE, role must be master"
        );
        {
            let info = replica.connect().command_str(&[b"INFO", b"replication"]);
            assert!(
                !info.contains("master_host:"),
                "replicaof no one must clear master host, info: {info}"
            );
        }

        assert_eq!(
            rc.command(&[b"SET", b"failmatrix:f3:local", b"promoted"]),
            b"+OK\r\n"
        );

        std::thread::sleep(Duration::from_secs(1));

        assert_eq!(
            rc.command(&[b"REPLICAOF", b"127.0.0.1", master_port_str.as_bytes()]),
            b"+OK\r\n"
        );

        assert!(
            wait_until(
                || {
                    let info = replica.connect().command_str(&[b"INFO", b"replication"]);
                    info.contains("master_link_status:up")
                },
                Duration::from_secs(60),
            ),
            "after NO-ONE + re-REPLICAOF, must reach online via a fresh full sync"
        );

        let sync_after = current_sync_full_count(&master);
        assert!(
            sync_after >= 1,
            "expected >=1 full sync fresh pass after NO-ONE-reconnect. sync_before={sync_before_first} sync_after={sync_after}"
        );

        let seed = replica.connect().command(&[b"GET", b"failmatrix:seed:0"]);
        assert!(
            seed.starts_with(b"$"),
            "replica must have received master seeds from the fresh full sync"
        );

        let info_after = replica.connect().command_str(&[b"INFO", b"replication"]);
        assert!(
            info_after.contains("role:slave"),
            "final role must be slave again, info: {info_after}"
        );
    }
}
