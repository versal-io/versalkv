//! Cluster engine baseline (C4-1) integration tests.
//! Verifies the single-node cluster bootstrap contract:
//! - `--cluster-enabled yes` starts without crashing
//! - CLUSTER MYID returns a 40-char hex id
//! - CLUSTER INFO reports cluster_state:ok / cluster_my_epoch:0 / cluster_size:1
//! - CLUSTER NODES returns exactly one line (myself, connected, myself flag)

use crate::common::TestServer;
use std::net::TcpListener;
use std::process::Command;
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn cluster_enabled_boots_and_reports_self() {
    let srv = TestServer::start(&["--cluster-enabled", "yes"]);
    let mut c = srv.connect();

    // CLUSTER MYID: returns a 40-char hex bulk string. Extract the payload line
    // (between $40\r\n and the trailing \r\n).
    let myid_raw = c.command_str(&[b"CLUSTER", b"MYID"]);
    let myid: String = myid_raw
        .lines()
        .find(|l| l.len() == 40 && l.chars().all(|ch| ch.is_ascii_hexdigit()))
        .unwrap_or_else(|| panic!("CLUSTER MYID must contain a 40-hex line, got {myid_raw:?}"))
        .to_string();
    assert_eq!(myid.len(), 40, "CLUSTER MYID must be 40 hex chars");

    // CLUSTER INFO: baseline fields.
    // An empty single-node cluster (no slots assigned yet) reports cluster_state:fail
    // because full 16384-slot coverage is not reached — matches C Valkey semantics.
    let info = c.command_str(&[b"CLUSTER", b"INFO"]);
    assert!(
        info.contains("cluster_state:fail"),
        "empty cluster should report cluster_state:fail: {info}"
    );
    assert!(
        info.contains("cluster_my_epoch:0"),
        "cluster_my_epoch:0 missing from CLUSTER INFO: {info}"
    );
    assert!(
        info.contains("cluster_size:0"),
        "cluster_size:0 for zero slots: {info}"
    );
    assert!(
        info.contains("cluster_known_nodes:1"),
        "cluster_known_nodes:1 (self) missing: {info}"
    );

    // After assigning all 16384 slots to myself, state flips to ok and size becomes 1.
    // Use ADDSLOTSRANGE to cover the whole keyspace in one command.
    assert_eq!(
        c.command(&[b"CLUSTER", b"ADDSLOTSRANGE", b"0", b"16383"]),
        b"+OK\r\n"
    );
    let info = c.command_str(&[b"CLUSTER", b"INFO"]);
    assert!(
        info.contains("cluster_state:ok"),
        "cluster_state:ok after full slot coverage: {info}"
    );
    assert!(
        info.contains("cluster_size:1"),
        "cluster_size:1 after self owns all slots: {info}"
    );

    // CLUSTER NODES: exactly one non-empty node line referring to myself with 'myself' flag
    // and connected state. Skip the RESP bulk length prefix ($NNN) and any blank lines.
    let nodes = c.command_str(&[b"CLUSTER", b"NODES"]);
    let nonempty: Vec<&str> = nodes
        .lines()
        .filter(|l| {
            let t = l.trim();
            !t.is_empty() && !t.starts_with('$') && !t.starts_with('+')
        })
        .collect();
    assert_eq!(
        nonempty.len(),
        1,
        "CLUSTER NODES should list exactly one node for single-node cluster, got {nodes:?}"
    );
    let line = nonempty[0];
    assert!(
        line.contains("myself"),
        "CLUSTER NODES line must contain 'myself': {line}"
    );
    assert!(
        line.contains("connected"),
        "CLUSTER NODES line must contain 'connected': {line}"
    );
    let fields: Vec<&str> = line.split_whitespace().collect();
    assert!(
        fields.len() >= 3,
        "CLUSTER NODES line must have at least name/addr/flags fields: {line}"
    );
    assert_eq!(
        fields[0], myid,
        "CLUSTER NODES self id must equal CLUSTER MYID"
    );
}

#[test]
fn cluster_replicate_registers_an_online_replica_on_its_primary() {
    let primary = TestServer::start(&["--cluster-enabled", "yes"]);
    let replica = TestServer::start(&["--cluster-enabled", "yes"]);
    let mut primary_client = primary.connect();
    let mut replica_client = replica.connect();

    let primary_id = primary_client.command_str(&[b"CLUSTER", b"MYID"]);
    let primary_id = primary_id
        .lines()
        .find(|line| line.len() == 40)
        .expect("CLUSTER MYID must return a node id")
        .as_bytes()
        .to_vec();
    assert_eq!(
        primary_client.command(&[b"CLUSTER", b"ADDSLOTSRANGE", b"0", b"16383"]),
        b"+OK\r\n"
    );
    assert_eq!(
        replica_client.command(&[
            b"CLUSTER",
            b"MEET",
            b"127.0.0.1",
            primary.port.to_string().as_bytes(),
        ]),
        b"+OK\r\n"
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if replica_client
            .command_str(&[b"CLUSTER", b"NODES"])
            .contains(std::str::from_utf8(&primary_id).expect("node id is utf-8"))
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(
        replica_client.command(&[b"CLUSTER", b"REPLICATE", &primary_id]),
        b"+OK\r\n"
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if primary_client
            .command_str(&[b"INFO", b"replication"])
            .contains("slave0:")
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    panic!(
        "cluster replica did not become online; primary info: {}\nreplica info: {}",
        primary_client.command_str(&[b"INFO", b"replication"]),
        replica_client.command_str(&[b"INFO", b"replication"]),
    );
}
#[test]
fn cluster_disabled_rejects_cluster_commands() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let reply = c.command_str(&[b"CLUSTER", b"INFO"]);
    assert!(
        reply.contains("cluster support disabled"),
        "without --cluster-enabled, CLUSTER INFO must error with disabled message: {reply}"
    );
}

#[test]
fn cluster_mode_rejects_swapdb() {
    let srv = TestServer::start(&["--cluster-enabled", "yes", "--databases", "2"]);
    let mut client = srv.connect();

    assert_eq!(
        client.command(&[b"SWAPDB", b"0", b"1"]),
        b"-ERR SWAPDB is not allowed in cluster mode\r\n"
    );
}

#[test]
fn cluster_reset_rejects_primary_with_keys_in_nonzero_database() {
    let srv = TestServer::start(&["--cluster-enabled", "yes", "--databases", "2"]);
    let mut client = srv.connect();
    assert_eq!(
        client.command(&[b"CLUSTER", b"ADDSLOTSRANGE", b"0", b"16383"]),
        b"+OK\r\n"
    );
    assert_eq!(client.command(&[b"SELECT", b"1"]), b"+OK\r\n");
    assert_eq!(client.command(&[b"SET", b"key", b"value"]), b"+OK\r\n");

    assert_eq!(
        client.command(&[b"CLUSTER", b"RESET"]),
        b"-ERR CLUSTER RESET can't be called with master nodes containing keys\r\n"
    );
}

#[test]
fn cluster_flushslots_requires_all_databases_empty() {
    let srv = TestServer::start(&["--cluster-enabled", "yes", "--databases", "2"]);
    let mut client = srv.connect();
    assert_eq!(
        client.command(&[b"CLUSTER", b"ADDSLOTSRANGE", b"0", b"16383"]),
        b"+OK\r\n"
    );
    assert_eq!(client.command(&[b"SELECT", b"1"]), b"+OK\r\n");
    assert_eq!(client.command(&[b"SET", b"key", b"value"]), b"+OK\r\n");
    assert_eq!(
        client.command(&[b"CLUSTER", b"FLUSHSLOTS"]),
        b"-ERR DB must be empty to perform CLUSTER FLUSHSLOTS.\r\n"
    );

    assert_eq!(client.command(&[b"FLUSHDB"]), b"+OK\r\n");
    assert_eq!(client.command(&[b"CLUSTER", b"FLUSHSLOTS"]), b"+OK\r\n");
}

#[test]
fn cluster_flushslot_deletes_only_the_requested_slot_across_databases() {
    let srv = TestServer::start(&["--cluster-enabled", "yes", "--databases", "16"]);
    let mut client = srv.connect();
    assert_eq!(
        client.command(&[b"CLUSTER", b"ADDSLOTSRANGE", b"0", b"16383"]),
        b"+OK\r\n"
    );
    let slot = client.command_str(&[b"CLUSTER", b"KEYSLOT", b"{flush}one"]);
    let slot = slot
        .trim()
        .strip_prefix(':')
        .expect("KEYSLOT integer reply")
        .to_string();

    assert_eq!(client.command(&[b"SET", b"{flush}one", b"v0"]), b"+OK\r\n");
    assert_eq!(client.command(&[b"SET", b"other", b"keep"]), b"+OK\r\n");
    assert_eq!(client.command(&[b"SELECT", b"15"]), b"+OK\r\n");
    assert_eq!(client.command(&[b"SET", b"{flush}two", b"v15"]), b"+OK\r\n");

    assert_eq!(
        client.command(&[b"CLUSTER", b"FLUSHSLOT", slot.as_bytes(), b"bogus"]),
        b"-ERR syntax error\r\n"
    );
    assert_eq!(client.command(&[b"EXISTS", b"{flush}two"]), b":1\r\n");
    assert_eq!(
        client.command(&[b"CLUSTER", b"FLUSHSLOT", slot.as_bytes(), b"ASYNC"]),
        b"+OK\r\n"
    );
    assert_eq!(client.command(&[b"EXISTS", b"{flush}two"]), b":0\r\n");
    assert_eq!(client.command(&[b"SELECT", b"0"]), b"+OK\r\n");
    assert_eq!(client.command(&[b"EXISTS", b"{flush}one"]), b":0\r\n");
    assert_eq!(client.command(&[b"EXISTS", b"other"]), b":1\r\n");

    assert_eq!(
        client.command(&[b"SET", b"{flush}three", b"v0"]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"CLUSTER", b"FLUSHSLOT", slot.as_bytes(), b"SYNC"]),
        b"+OK\r\n"
    );
    assert_eq!(client.command(&[b"EXISTS", b"{flush}three"]), b":0\r\n");
}

#[test]
fn cluster_bus_bind_failure_is_not_reported_as_initialized() {
    let occupied_bus = TcpListener::bind("127.0.0.1:0").expect("reserve cluster bus port");
    let bus_port = occupied_bus.local_addr().unwrap().port().to_string();
    let client_port = TcpListener::bind("127.0.0.1:0")
        .expect("reserve client port")
        .local_addr()
        .unwrap()
        .port()
        .to_string();
    let data_dir = std::env::temp_dir().join(format!(
        "valkey-rust-cluster-bind-test-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before Unix epoch")
            .as_nanos()
    ));
    std::fs::create_dir(&data_dir).expect("create cluster bind test directory");
    let config_path = data_dir.join("valkey.conf");
    std::fs::write(
        &config_path,
        format!(
            "bind 127.0.0.1\nport {client_port}\ndir {}\ncluster-enabled yes\ncluster-port {bus_port}\n",
            data_dir.display()
        ),
    )
    .expect("write cluster bind test config");
    let output = Command::new(env!("CARGO_BIN_EXE_versal-server"))
        .arg(&config_path)
        .output()
        .expect("run versal-server with occupied cluster bus port");
    std::fs::remove_dir_all(&data_dir).expect("remove cluster bind test directory");

    assert!(!output.status.success(), "cluster bus bind must fail");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stdout.contains("Failed listening on port"),
        "harness must recognize a busy listener: stdout={stdout:?}, stderr={stderr:?}"
    );
    assert!(
        !stdout.contains("Server initialized"),
        "startup marker must follow all listener binds: {stdout}"
    );
}

#[test]
fn config_rewrite_preserves_cluster_and_diskless_load_across_restart() {
    let data_dir = std::env::temp_dir().join(format!(
        "valkey-rust-cluster-config-rewrite-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before Unix epoch")
            .as_nanos()
    ));
    std::fs::create_dir(&data_dir).expect("create test directory");
    let config_path = data_dir.join("valkey.conf");
    std::fs::write(
        &config_path,
        "cluster-enabled yes\ncluster-config-file nodes.conf\n",
    )
    .expect("write test config");
    let config_path = config_path.to_string_lossy().into_owned();

    let mut server = TestServer::start_in_dir(data_dir, &[&config_path]);
    let mut client = server.connect();
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"repl-diskless-load", b"swapdb",]),
        b"+OK\r\n"
    );
    assert_eq!(client.command(&[b"CONFIG", b"REWRITE"]), b"+OK\r\n");

    let rewritten = std::fs::read_to_string(&config_path).expect("read rewritten config");
    assert!(
        rewritten.contains("cluster-enabled yes\n"),
        "cluster mode missing after rewrite: {rewritten}"
    );
    assert!(
        rewritten.contains("repl-diskless-load swapdb\n"),
        "diskless load mode missing after rewrite: {rewritten}"
    );

    client.send(&[b"SHUTDOWN", b"NOSAVE"]);
    assert_eq!(
        server.wait_exit(Duration::from_secs(5)),
        Some(0),
        "server must exit cleanly"
    );
    let rewritten = rewritten.replacen(&format!("port {}\n", server.port), "port invalid\n", 1);
    std::fs::write(&config_path, rewritten).expect("normalize test config port");
    let data_dir = server.take_data_dir();
    let restarted = TestServer::start_in_dir(data_dir, &[&config_path]);
    let mut client = restarted.connect();
    assert!(
        client
            .command_str(&[b"INFO", b"cluster"])
            .contains("cluster_enabled:1"),
        "cluster mode must survive CONFIG REWRITE and restart"
    );
    assert!(
        client
            .command_str(&[b"CONFIG", b"GET", b"repl-diskless-load"])
            .contains("swapdb"),
        "diskless load mode must survive CONFIG REWRITE and restart"
    );
}

#[test]
fn cluster_replica_config_restores_replication_role_on_startup() {
    let data_dir = std::env::temp_dir().join(format!(
        "valkey-rust-cluster-replica-restart-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before Unix epoch")
            .as_nanos()
    ));
    std::fs::create_dir(&data_dir).expect("create test directory");
    let config_path = data_dir.join("valkey.conf");
    std::fs::write(
        &config_path,
        "cluster-enabled yes\ncluster-config-file nodes.conf\ncluster-node-timeout 60000\n",
    )
    .expect("write test config");
    std::fs::write(
        data_dir.join("nodes.conf"),
        concat!(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 127.0.0.1:7000@17000 ",
            "myself,slave bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 0 0 2 connected\n",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 127.0.0.1:29999@39999 ",
            "master - 0 0 1 connected 0-16383\n",
            "vars currentEpoch 2 lastVoteEpoch 0\n",
        ),
    )
    .expect("write cluster config");
    let config_path = config_path.to_string_lossy().into_owned();

    let server = TestServer::start_in_dir(data_dir, &[&config_path]);
    let mut client = server.connect();
    let info = client.command_str(&[b"INFO", b"replication"]);

    assert!(info.contains("role:slave"), "{info}");
    assert!(info.contains("master_host:127.0.0.1"), "{info}");
    assert!(info.contains("master_port:29999"), "{info}");
}

fn cluster_config_test_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "valkey-rust-cluster-config-{name}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before Unix epoch")
            .as_nanos()
    ));
    std::fs::create_dir(&dir).expect("create cluster config test directory");
    dir
}

fn cluster_config_server(name: &str, save_behavior: &str) -> TestServer {
    let data_dir = cluster_config_test_dir(name);
    let config_path = data_dir.join("valkey.conf");
    std::fs::write(
        &config_path,
        format!(
            concat!(
                "cluster-enabled yes\n",
                "cluster-config-file nodes.conf\n",
                "cluster-config-save-behavior {}\n",
                "logfile cluster.log\n",
            ),
            save_behavior
        ),
    )
    .expect("write cluster config test config");
    let config_path = config_path.to_string_lossy().into_owned();
    TestServer::start_in_dir(data_dir, &[&config_path])
}

fn replace_nodes_config_with_directory(server: &TestServer) {
    let path = server.data_dir().join("nodes.conf");
    if path.is_file() {
        std::fs::remove_file(&path).expect("remove nodes.conf file");
    }
    std::fs::create_dir_all(path).expect("replace nodes.conf with directory");
}

#[test]
fn cluster_config_saveconfig_persists_metadata_across_restart() {
    let mut server = cluster_config_server("round-trip", "sync");
    let mut client = server.connect();
    let shard_id = client.command_str(&[b"CLUSTER", b"MYSHARDID"]);
    let shard_id = shard_id
        .lines()
        .find(|line| line.len() == 40)
        .expect("MYSHARDID payload")
        .to_string();
    assert_eq!(
        client.command(&[
            b"CONFIG",
            b"SET",
            b"cluster-announce-ip",
            b"198.51.100.10",
            b"cluster-announce-hostname",
            b"cache.example.com",
            b"cluster-announce-human-nodename",
            b"cache-east-1",
            b"cluster-announce-client-ipv4",
            b"192.0.2.10",
            b"cluster-announce-client-ipv6",
            b"2001:db8::10",
            b"cluster-announce-tls-port",
            b"7443",
            b"cluster-announce-client-port",
            b"7001",
            b"cluster-announce-client-tls-port",
            b"7444",
            b"availability-zone",
            b"zone-a",
        ]),
        b"+OK\r\n"
    );
    assert_eq!(client.command(&[b"CLUSTER", b"SAVECONFIG"]), b"+OK\r\n");
    let nodes_path = server.data_dir().join("nodes.conf");
    let saved = std::fs::read_to_string(&nodes_path).expect("read saved nodes.conf");
    for metadata in [
        "198.51.100.10",
        "cache.example.com",
        "nodename=cache-east-1",
        "tls-port=7443",
        "client-ipv4=192.0.2.10",
        "client-ipv6=2001:db8::10",
        "client-tcp-port=7001",
        "client-tls-port=7444",
        "availability-zone=zone-a",
    ] {
        assert!(saved.contains(metadata), "missing {metadata}: {saved}");
    }

    client.send(&[b"SHUTDOWN", b"NOSAVE"]);
    assert_eq!(server.wait_exit(Duration::from_secs(5)), Some(0));
    let data_dir = server.take_data_dir();
    let config_path = data_dir.join("valkey.conf");
    let config_path = config_path.to_string_lossy().into_owned();
    let restarted = TestServer::start_in_dir(data_dir, &[&config_path]);
    let mut client = restarted.connect();
    assert!(
        client
            .command_str(&[b"CLUSTER", b"MYSHARDID"])
            .contains(&shard_id),
        "shard ID must survive nodes.conf reload"
    );
    assert_eq!(client.command(&[b"CLUSTER", b"SAVECONFIG"]), b"+OK\r\n");
    let saved = std::fs::read_to_string(restarted.data_dir().join("nodes.conf"))
        .expect("read nodes.conf after restart");
    for metadata in [
        "198.51.100.10",
        "cache.example.com",
        "nodename=cache-east-1",
        "tls-port=7443",
        "client-ipv4=192.0.2.10",
        "client-ipv6=2001:db8::10",
        "client-tcp-port=7001",
        "client-tls-port=7444",
        "availability-zone=zone-a",
    ] {
        assert!(saved.contains(metadata), "missing {metadata}: {saved}");
    }
}

#[test]
fn cluster_config_saveconfig_returns_io_error_without_exiting() {
    let server = cluster_config_server("explicit-error", "sync");
    replace_nodes_config_with_directory(&server);
    let mut client = server.connect();

    let reply = client.command_str(&[b"CLUSTER", b"SAVECONFIG"]);

    assert!(
        reply.starts_with("-ERR error saving the cluster node config:"),
        "{reply}"
    );
    assert_eq!(client.command(&[b"PING"]), b"+PONG\r\n");
}

#[test]
fn cluster_config_saveconfig_inside_exec_returns_the_persist_result() {
    let server = cluster_config_server("exec-save", "sync");
    let mut client = server.connect();

    assert_eq!(client.command(&[b"MULTI"]), b"+OK\r\n");
    assert_eq!(client.command(&[b"CLUSTER", b"SAVECONFIG"]), b"+QUEUED\r\n");
    assert_eq!(client.command(&[b"EXEC"]), b"*1\r\n+OK\r\n");
    assert!(server.data_dir().join("nodes.conf").is_file());
}

#[test]
fn cluster_config_explicit_startup_metadata_overrides_nodes_config() {
    let data_dir = cluster_config_test_dir("startup-override");
    let config_path = data_dir.join("valkey.conf");
    std::fs::write(
        &config_path,
        concat!(
            "cluster-enabled yes\n",
            "cluster-config-file nodes.conf\n",
            "cluster-announce-human-nodename startup-name\n",
            "availability-zone startup-zone\n",
        ),
    )
    .expect("write cluster startup config");
    std::fs::write(
        data_dir.join("nodes.conf"),
        concat!(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa ",
            "127.0.0.1:7000@17000,,shard-id=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,",
            "nodename=persisted-name,availability-zone=persisted-zone ",
            "myself,master - 0 0 0 connected\n",
            "vars currentEpoch 0 lastVoteEpoch 0\n",
        ),
    )
    .expect("write persisted cluster metadata");
    let config_path = config_path.to_string_lossy().into_owned();
    let server = TestServer::start_in_dir(data_dir, &[&config_path]);
    let mut client = server.connect();

    assert_eq!(client.command(&[b"CLUSTER", b"SAVECONFIG"]), b"+OK\r\n");
    let saved = std::fs::read_to_string(server.data_dir().join("nodes.conf")).unwrap();
    assert!(saved.contains("nodename=startup-name"), "{saved}");
    assert!(saved.contains("availability-zone=startup-zone"), "{saved}");
    assert!(!saved.contains("persisted-name"), "{saved}");
    assert!(!saved.contains("persisted-zone"), "{saved}");
}

#[test]
fn cluster_config_explicit_empty_startup_metadata_clears_nodes_config() {
    let data_dir = cluster_config_test_dir("startup-clear");
    let config_path = data_dir.join("valkey.conf");
    std::fs::write(
        &config_path,
        concat!(
            "cluster-enabled yes\n",
            "cluster-config-file nodes.conf\n",
            "cluster-announce-human-nodename \"\"\n",
            "availability-zone \"\"\n",
        ),
    )
    .expect("write cluster startup config");
    std::fs::write(
        data_dir.join("nodes.conf"),
        concat!(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa ",
            "127.0.0.1:7000@17000,,shard-id=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,",
            "nodename=persisted-name,availability-zone=persisted-zone ",
            "myself,master - 0 0 0 connected\n",
            "vars currentEpoch 0 lastVoteEpoch 0\n",
        ),
    )
    .expect("write persisted cluster metadata");
    let config_path = config_path.to_string_lossy().into_owned();
    let server = TestServer::start_in_dir(data_dir, &[&config_path]);
    let mut client = server.connect();

    assert_eq!(client.command(&[b"CLUSTER", b"SAVECONFIG"]), b"+OK\r\n");
    let saved = std::fs::read_to_string(server.data_dir().join("nodes.conf")).unwrap();
    assert!(!saved.contains("nodename="), "{saved}");
    assert!(!saved.contains("availability-zone="), "{saved}");
}

#[test]
fn cluster_config_sync_auto_save_failure_exits_nonzero() {
    let mut server = cluster_config_server("sync-error", "sync");
    replace_nodes_config_with_directory(&server);
    let mut client = server.connect();

    let _ = client.command(&[
        b"CONFIG",
        b"SET",
        b"cluster-announce-human-nodename",
        b"fatal-save",
    ]);

    assert_eq!(server.wait_exit(Duration::from_secs(5)), Some(1));
    let log =
        std::fs::read_to_string(server.data_dir().join("cluster.log")).expect("read server log");
    assert!(
        log.contains("Fatal: can't update cluster config file."),
        "{log}"
    );
}

#[test]
fn cluster_config_best_effort_auto_save_failure_stays_alive() {
    let data_dir = cluster_config_test_dir("best-effort-error");
    let state_dir = data_dir.join("cluster-state");
    std::fs::create_dir(&state_dir).expect("create cluster config parent directory");
    let config_path = data_dir.join("valkey.conf");
    std::fs::write(
        &config_path,
        concat!(
            "cluster-enabled yes\n",
            "cluster-config-file cluster-state/nodes.conf\n",
            "cluster-config-save-behavior best-effort\n",
            "logfile cluster.log\n",
        ),
    )
    .expect("write cluster config test config");
    let config_path = config_path.to_string_lossy().into_owned();
    let server = TestServer::start_in_dir(data_dir, &[&config_path]);
    std::fs::rename(&state_dir, server.data_dir().join("cluster-state.saved"))
        .expect("move cluster config parent directory");
    // The save worker never creates parents, so it cannot replace this stable obstacle.
    std::fs::write(&state_dir, b"not a directory").expect("block cluster config parent path");
    let mut client = server.connect();

    assert_eq!(
        client.command(&[
            b"CONFIG",
            b"SET",
            b"cluster-announce-human-nodename",
            b"retry-save",
        ]),
        b"+OK\r\n"
    );
    // SAVECONFIG drains prior automatic saves and records their completion before replying.
    let reply = client.command_str(&[b"CLUSTER", b"SAVECONFIG"]);
    assert!(
        reply.starts_with("-ERR error saving the cluster node config:"),
        "{reply}"
    );
    let log =
        std::fs::read_to_string(server.data_dir().join("cluster.log")).expect("read server log");
    assert!(
        log.contains(
            "Cluster config updated even though writing the cluster config file to disk failed."
        ),
        "save failure was not logged: {log}"
    );
    assert_eq!(client.command(&[b"PING"]), b"+PONG\r\n");
}
