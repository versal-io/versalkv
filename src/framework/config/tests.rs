use super::*;
use crate::engine::extension::config::server::ProtectedAction;
use crate::foundation::io::persistence::AofFsync;
use crate::framework::context::RuntimeConfig;
use clap::Parser;
use std::time::{SystemTime, UNIX_EPOCH};

fn default_config() -> Config {
    Config::parse_from(["versal-server"])
}

fn load_temp_config(label: &str, contents: &str) -> std::io::Result<Config> {
    let path = std::env::temp_dir().join(format!(
        "versal-{label}-{}-{}.conf",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos()
    ));
    std::fs::write(&path, contents).expect("write temporary config");
    let mut config = default_config();
    let result = config.load_from_file(path.to_str().expect("UTF-8 config path"));
    let _ = std::fs::remove_file(path);
    result.map(|()| config)
}

#[test]
fn replicaof_config_file_preserves_quoted_empty_and_escaped_hosts() {
    let cases = [
        ("empty", "replicaof \"\" 6380\n", "", 6380),
        (
            "whitespace",
            "replicaof \"host name\" 6381\n",
            "host name",
            6381,
        ),
        (
            "escaped",
            r#"replicaof "host \"quoted\" \\ path" 6382
"#,
            "host \"quoted\" \\ path",
            6382,
        ),
        ("zero-port", "replicaof zero-port 0\n", "zero-port", 0),
        ("max-port", "replicaof max-port 65535\n", "max-port", 65535),
    ];

    for (label, contents, host, port) in cases {
        let config = load_temp_config(label, contents).expect("valid replicaof config");
        assert_eq!(config.replicaof_host.as_deref(), Some(host));
        assert_eq!(config.replicaof_port, port);

        let mut runtime = RuntimeConfig::new();
        config.apply_to(&mut runtime);
        assert_eq!(runtime.replication.master_host.as_deref(), Some(host));
        assert_eq!(runtime.replication.master_port, port);
    }
}

#[test]
fn replicaof_config_accepts_c_port_prefix_grammar() {
    let cases = [
        ("empty", "replicaof host \"\"\n", 0),
        ("negative-zero", "replicaof host -0\n", 0),
        ("positive-zero", "replicaof host +0\n", 0),
        ("leading-space", "replicaof host \" 6380\"\n", 6380),
        ("leading-tab", "replicaof host \"\\t6381\"\n", 6381),
        ("leading-plus", "replicaof host +6382\n", 6382),
    ];

    for (label, contents, port) in cases {
        let config = load_temp_config(label, contents).expect("C-compatible port must parse");
        assert_eq!(config.replicaof_host.as_deref(), Some("host"));
        assert_eq!(config.replicaof_port, port);
    }
}

#[test]
fn replicaof_no_one_clears_an_earlier_config_file_target() {
    let config = load_temp_config(
        "replicaof-clear",
        "replicaof 127.0.0.1 6380\nreplicaof no one\n",
    )
    .expect("replicaof no one should clear the target");

    assert_eq!(config.replicaof_host, None);
    assert_eq!(config.replicaof_port, 0);

    let mut runtime = RuntimeConfig::new();
    runtime.replication.master_host = Some("stale-primary".to_string());
    runtime.replication.master_port = 9999;
    config.apply_to(&mut runtime);
    assert_eq!(runtime.replication.master_host, None);
    assert_eq!(runtime.replication.master_port, 0);
}

#[test]
fn malformed_replicaof_config_files_are_invalid_data() {
    let cases = [
        ("missing", "replicaof 127.0.0.1\n"),
        ("extra", "replicaof 127.0.0.1 6380 extra\n"),
        ("invalid-port", "replicaof 127.0.0.1 abc\n"),
        ("whitespace-only", "replicaof 127.0.0.1 \"   \"\n"),
        ("plus-only", "replicaof 127.0.0.1 +\n"),
        ("minus-only", "replicaof 127.0.0.1 -\n"),
        ("negative-port", "replicaof 127.0.0.1 -1\n"),
        ("large-port", "replicaof 127.0.0.1 65536\n"),
        ("trailing-space", "replicaof 127.0.0.1 \"6380 \"\n"),
        ("trailing-junk", "replicaof 127.0.0.1 6380x\n"),
        ("double-unbalanced", "replicaof \"host 6380\n"),
        ("single-unbalanced", "replicaof 'host 6380\n"),
        ("unfinished-escape", "replicaof \"host\\\n"),
    ];

    for (label, contents) in cases {
        let error = load_temp_config(label, contents)
            .expect_err("malformed replicaof must reject the config file");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("replicaof"));
    }
}

#[test]
fn defaults_map_to_runtime_config() {
    let cfg = default_config();
    let mut runtime = RuntimeConfig::new();

    cfg.apply_to(&mut runtime);

    assert_eq!(runtime.server.listening_port, 6379);
    assert_eq!(runtime.persistence.dir, "./");
    assert_eq!(runtime.persistence.dbfilename, "dump.rdb");
    assert_eq!(runtime.persistence.aof_filename, "appendonly.aof");
    assert_eq!(runtime.persistence.aof_appenddirname, "appendonlydir");
    assert_eq!(runtime.persistence.aof_fsync, AofFsync::EverySec);
    assert!(runtime.persistence.aof_use_rdb_preamble);
    assert_eq!(runtime.replication.repl_timeout, 60);
    assert_eq!(runtime.server.maxclients, 10000);
    assert!(runtime.server.lazyfree_lazy_eviction);
    assert!(runtime.server.lazyfree_lazy_expire);
    assert!(runtime.server.lazyfree_lazy_server_del);
    assert!(runtime.server.enable_module_command == ProtectedAction::Yes);
    assert!(!runtime.server.cluster_enabled);
}

#[test]
fn config_file_transfers_diskless_sync_delay_to_runtime() {
    let mut cfg = default_config();
    cfg.parse_config_string("repl-diskless-sync-delay 0\n");
    let mut runtime = RuntimeConfig::new();

    cfg.apply_to(&mut runtime);

    assert_eq!(runtime.replication.repl_diskless_sync_delay, 0);
}

#[test]
fn config_file_transfers_signed_debug_delays_and_loading_interval() {
    let mut cfg = default_config();
    cfg.parse_config_string(
        "rdb-key-save-delay -17\nkey-load-delay -19\nloading-process-events-interval-bytes 1024\n",
    );
    let mut runtime = RuntimeConfig::new();

    cfg.apply_to(&mut runtime);

    assert_eq!(runtime.persistence.rdb_key_save_delay, -17);
    assert_eq!(runtime.persistence.key_load_delay, -19);
    assert_eq!(
        runtime.persistence.loading_process_events_interval_bytes,
        1024
    );
}

#[test]
fn config_file_does_not_apply_out_of_range_debug_delay_or_loading_interval() {
    let mut cfg = default_config();
    cfg.parse_config_string(
        "rdb-key-save-delay 2147483648\nkey-load-delay -2147483649\nloading-process-events-interval-bytes 1023\n",
    );
    let mut runtime = RuntimeConfig::new();

    cfg.apply_to(&mut runtime);

    assert_eq!(runtime.persistence.rdb_key_save_delay, 0);
    assert_eq!(runtime.persistence.key_load_delay, 0);
    assert_eq!(
        runtime.persistence.loading_process_events_interval_bytes,
        2 * 1024 * 1024
    );
}

#[test]
fn config_file_parses_memory_commandlog_thresholds_and_rejects_invalid_values() {
    let mut cfg = default_config();
    cfg.parse_config_string(
        "commandlog-request-larger-than 2mb\ncommandlog-reply-larger-than 3mb\ncommandlog-large-request-max-len 7\ncommandlog-large-reply-max-len 9\n",
    );
    let mut runtime = RuntimeConfig::new();
    cfg.apply_to(&mut runtime);
    assert_eq!(runtime.server.large_request_threshold, 2 * 1024 * 1024);
    assert_eq!(runtime.server.large_reply_threshold, 3 * 1024 * 1024);
    assert_eq!(runtime.server.large_request_max_len, 7);
    assert_eq!(runtime.server.large_reply_max_len, 9);

    let mut invalid = default_config();
    invalid.parse_config_string(
        "commandlog-request-larger-than -2\ncommandlog-reply-larger-than -2\ncommandlog-large-request-max-len -1\ncommandlog-large-reply-max-len -1\n",
    );
    let mut invalid_runtime = RuntimeConfig::new();
    invalid.apply_to(&mut invalid_runtime);
    assert_eq!(invalid_runtime.server.large_request_threshold, 1024 * 1024);
    assert_eq!(invalid_runtime.server.large_reply_threshold, 1024 * 1024);
    assert_eq!(invalid_runtime.server.large_request_max_len, 128);
    assert_eq!(invalid_runtime.server.large_reply_max_len, 128);
}

#[test]
fn load_from_file_rejects_invalid_commandlog_configuration() {
    let path = std::env::temp_dir().join(format!(
        "versal-invalid-commandlog-{}-{}.conf",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time")
            .as_nanos()
    ));
    std::fs::write(&path, "commandlog-request-larger-than -2\n").expect("write invalid config");
    let mut cfg = default_config();

    let error = cfg
        .load_from_file(path.to_str().expect("UTF-8 config path"))
        .expect_err("invalid commandlog threshold must reject the config file");

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("commandlog-request-larger-than"));
    let _ = std::fs::remove_file(path);
}

#[test]
fn config_file_transfers_prefetch_batch_size_to_runtime() {
    let mut cfg = default_config();
    cfg.parse_config_string("prefetch-batch-max-size 2\n");
    let mut runtime = RuntimeConfig::new();

    cfg.apply_to(&mut runtime);

    assert_eq!(runtime.server.prefetch_batch_max_size, 2);
}

#[test]
fn config_file_captures_registry_only_io_thread_settings_for_startup_replay() {
    let cfg = load_temp_config(
        "io-threads-runtime",
        "io-threads 5\nio-threads-always-active yes\n",
    )
    .expect("valid io thread config file");
    let mut runtime = RuntimeConfig::new();

    cfg.apply_to(&mut runtime);
    for (key, value) in &cfg.extra_cli_configs {
        crate::framework::commands::server::config::config_registry()
            .try_set_startup(&mut runtime, key, value)
            .expect("registry-only config should be known")
            .expect("registry-only config should be valid");
    }

    assert_eq!(runtime.server.io_threads, 5);
    assert!(runtime.server.io_threads_always_active);
}

#[test]
fn config_file_transfers_tls_listener_material_to_runtime() {
    let mut cfg = default_config();
    cfg.parse_config_string(
        "tls-port 6380\n\
         tls-cert-file /etc/versal/server.crt\n\
         tls-key-file /etc/versal/server.key\n\
         tls-ca-cert-file /etc/versal/ca.crt\n\
         tls-ca-cert-dir /etc/versal/ca\n\
         tls-auth-clients optional\n\
         tls-protocols TLSv1.2 TLSv1.3\n\
         tls-ciphers ECDHE-RSA-AES128-GCM-SHA256\n\
         tls-ciphersuites TLS_AES_128_GCM_SHA256\n\
         tls-prefer-server-ciphers yes\n\
         tls-session-caching no\n\
         tls-session-cache-size 123\n\
         tls-replication yes\n\
         tls-cluster yes\n\
         tls-client-cert-file /etc/versal/client.crt\n\
         tls-client-key-file /etc/versal/client.key\n",
    );
    let mut runtime = RuntimeConfig::new();

    cfg.apply_to(&mut runtime);

    assert_eq!(runtime.server.tls_port, 6380);
    assert_eq!(runtime.server.tls_cert_file, "/etc/versal/server.crt");
    assert_eq!(runtime.server.tls_key_file, "/etc/versal/server.key");
    assert_eq!(runtime.server.tls_ca_cert_file, "/etc/versal/ca.crt");
    assert_eq!(runtime.server.tls_ca_cert_dir, "/etc/versal/ca");
    assert_eq!(runtime.server.tls_auth_clients, "optional");
    assert_eq!(runtime.server.tls_protocols, "TLSv1.2 TLSv1.3");
    assert_eq!(runtime.server.tls_ciphers, "ECDHE-RSA-AES128-GCM-SHA256");
    assert_eq!(runtime.server.tls_ciphersuites, "TLS_AES_128_GCM_SHA256");
    assert!(runtime.server.tls_prefer_server_ciphers);
    assert!(!runtime.server.tls_session_caching);
    assert_eq!(runtime.server.tls_session_cache_size, 123);
    assert!(runtime.server.tls_replication);
    assert!(runtime.server.tls_cluster);
    assert_eq!(
        runtime.server.tls_client_cert_file,
        "/etc/versal/client.crt"
    );
    assert_eq!(runtime.server.tls_client_key_file, "/etc/versal/client.key");
}

#[test]
fn config_file_transfers_sharded_pubsub_cluster_down_policy() {
    let mut cfg = default_config();
    cfg.parse_config_string("cluster-enabled yes\ncluster-allow-pubsubshard-when-down no\n");
    let mut runtime = RuntimeConfig::new();

    cfg.apply_to(&mut runtime);

    assert!(runtime.server.cluster_enabled);
    assert!(!runtime.server.cluster_allow_pubsubshard_when_down);
}

#[test]
fn config_file_transfers_lazyfree_user_controls() {
    let mut cfg = default_config();
    cfg.parse_config_string("lazyfree-lazy-user-del no\nlazyfree-lazy-user-flush no\n");
    let mut runtime = RuntimeConfig::new();

    cfg.apply_to(&mut runtime);

    assert!(!runtime.server.lazyfree_lazy_user_del);
    assert!(!runtime.server.lazyfree_lazy_user_flush);
}

#[test]
fn config_file_transfers_stream_node_max_bytes() {
    let mut cfg = default_config();
    cfg.parse_config_string("stream-node-max-bytes 128\n");
    let mut runtime = RuntimeConfig::new();

    cfg.apply_to(&mut runtime);

    assert_eq!(runtime.encoding.stream_node_max_bytes, 128);
}

#[test]
fn config_file_transfers_cluster_blacklist_ttl() {
    let mut cfg = default_config();
    cfg.parse_config_string("cluster-blacklist-ttl 7\n");
    let mut runtime = RuntimeConfig::new();

    cfg.apply_to(&mut runtime);

    assert_eq!(runtime.server.cluster_blacklist_ttl, 7);
}

#[test]
fn config_file_transfers_rdb_incremental_fsync() {
    let mut cfg = default_config();
    cfg.parse_config_string("rdb-save-incremental-fsync no\n");
    let mut runtime = RuntimeConfig::new();

    cfg.apply_to(&mut runtime);

    assert!(!runtime.persistence.rdb_save_incremental_fsync);
}

#[test]
fn config_file_transfers_rdb_del_sync_files() {
    let mut cfg = default_config();
    cfg.parse_config_string("rdb-del-sync-files yes\n");
    let mut runtime = RuntimeConfig::new();

    cfg.apply_to(&mut runtime);

    assert!(runtime.persistence.rdb_del_sync_files);
}

#[test]
fn config_file_transfers_replica_disk_write_error_policy() {
    let mut cfg = default_config();
    cfg.parse_config_string("replica-ignore-disk-write-errors yes\n");
    let mut runtime = RuntimeConfig::new();

    cfg.apply_to(&mut runtime);

    assert!(runtime.replication.replica_ignore_disk_write_errors);
}

#[test]
fn config_file_transfers_replica_lazy_flush_alias() {
    let mut cfg = default_config();
    cfg.parse_config_string("slave-lazy-flush no\n");
    let mut runtime = RuntimeConfig::new();

    cfg.apply_to(&mut runtime);

    assert!(!runtime.replication.replica_lazy_flush);
}

#[test]
fn config_file_transfers_aof_rewrite_incremental_fsync() {
    let mut cfg = default_config();
    cfg.parse_config_string("aof-rewrite-incremental-fsync no\n");
    let mut runtime = RuntimeConfig::new();

    cfg.apply_to(&mut runtime);

    assert!(!runtime.persistence.aof_rewrite_incremental_fsync);
}

#[test]
fn config_file_transfers_repl_mptcp() {
    let mut cfg = default_config();
    cfg.parse_config_string("repl-mptcp yes\n");
    let mut runtime = RuntimeConfig::new();

    cfg.apply_to(&mut runtime);

    assert!(runtime.replication.repl_mptcp);
}

#[test]
fn config_file_maps_slowlog_aliases_to_commandlog_slow_category() {
    let mut cfg = default_config();
    cfg.parse_config_string("slowlog-log-slower-than 42\nslowlog-max-len 17\n");
    let mut runtime = RuntimeConfig::new();

    cfg.apply_to(&mut runtime);

    assert_eq!(runtime.server.commandlog_slow_threshold, 42);
    assert_eq!(runtime.server.slowlog_log_slower_than, 42);
    assert_eq!(runtime.server.commandlog_slow_max_len, 17);
    assert_eq!(runtime.server.slowlog_max_len, 17);
}

#[test]
fn cli_arguments_override_defaults_and_bind_ipv6_is_bracketed() {
    let cfg = Config::parse_from([
        "versal-server",
        "--port",
        "6380",
        "--bind",
        "127.0.0.1 ::1",
        "--loglevel",
        "debug",
        "--databases",
        "4",
        "--appendonly",
        "--appendfsync",
        "always",
        "--maxclients",
        "42",
        "--enable-module-command",
        "no",
        "--cluster-enabled",
    ]);
    let mut runtime = RuntimeConfig::new();

    cfg.apply_to(&mut runtime);

    assert_eq!(
        cfg.bind_targets()
            .iter()
            .map(|t| t.addr.clone())
            .collect::<Vec<_>>(),
        vec!["127.0.0.1:6380", "[::1]:6380"]
    );
    assert_eq!(
        cfg.log_level(),
        crate::foundation::io::sys::log::LogLevel::Debug
    );
    assert_eq!(runtime.server.listening_port, 6380);
    assert_eq!(runtime.persistence.aof_fsync, AofFsync::Always);
    assert_eq!(runtime.server.maxclients, 42);
    assert!(runtime.server.enable_module_command == ProtectedAction::No);
    assert!(runtime.server.cluster_enabled);
    assert_eq!(runtime.server.cluster_port, 16380);
}

#[test]
fn log_level_accepts_verbose_warning_warn_and_defaults_to_notice() {
    let mut cfg = default_config();

    cfg.loglevel = "verbose".to_string();
    assert_eq!(
        cfg.log_level(),
        crate::foundation::io::sys::log::LogLevel::Verbose
    );
    cfg.loglevel = "warning".to_string();
    assert_eq!(
        cfg.log_level(),
        crate::foundation::io::sys::log::LogLevel::Warning
    );
    cfg.loglevel = "warn".to_string();
    assert_eq!(
        cfg.log_level(),
        crate::foundation::io::sys::log::LogLevel::Warning
    );
    cfg.loglevel = "anything".to_string();
    assert_eq!(
        cfg.log_level(),
        crate::foundation::io::sys::log::LogLevel::Notice
    );
}

#[test]
fn parse_config_string_handles_quotes_empty_values_aliases_and_invalid_numbers() {
    let mut cfg = default_config();
    cfg.parse_config_string(
        r#"
        # comments and blanks are skipped
        port 7000
        bind "127.0.0.1 ::1"
        loglevel 'warning'
        logfile server.log
        databases 32
        daemonize yes
        requirepass 'secret'
        dir /tmp/valkey
        dbfilename custom.rdb
        save 60 1000
        save 300 10
        shutdown-on-sigterm now
        shutdown-on-sigint no
        appendonly yes
        appendfilename append.aof
        appenddirname aofdir
        appendfsync no
        repl-timeout 7
        slave-announce-port 17000
        slave-ignore-maxmemory no
        repl-disable-tcp-nodelay yes
        no-appendfsync-on-rewrite yes
        lazyfree-lazy-eviction no
        lazyfree-lazy-expire no
        lazyfree-lazy-server-del no
        aof-use-rdb-preamble no
        rdb-version-check relaxed
        tcp-keepalive 17
        active-expire-effort 7
        timeout 19
        tcp-backlog 321
        list-max-ziplist-size -4
        set-max-intset-entries 16
        set-max-listpack-entries 8
        set-max-listpack-value 32
        stream-node-max-entries 10
        mptcp true
        maxclients 0
        maxclients 99
        max-new-connections-per-cycle 25
        max-new-tls-connections-per-cycle 3
        enable-module-command false
        commandlog-execution-slower-than 10
        commandlog-slow-execution-max-len 11
        commandlog-request-larger-than 12
        commandlog-large-request-max-len 13
        commandlog-reply-larger-than 14
        commandlog-large-reply-max-len 15
        sentinel monitor mymaster 127.0.0.1 6379 2
        cluster-enabled yes
        cluster-port 17000
        cluster-ping-interval 100
        cluster-node-timeout 1234
        cluster-migration-barrier 999
        cluster-config-file cluster.conf
        cluster-announce-ip 10.0.0.1
        cluster-announce-client-ipv4 192.0.2.10
        cluster-announce-client-ipv6 2001:db8::10
        cluster-announce-hostname node.local
        cluster-announce-human-nodename node-a
        cluster-announce-port 7001
        cluster-announce-tls-port 7443
        cluster-announce-client-port 7002
        cluster-announce-client-tls-port 7444
        cluster-announce-bus-port 17001
        cluster-config-save-behavior best-effort
        cluster-message-gossip-perc 80
        availability-zone zone-a
        cluster-slot-stats-enabled yes
        cluster-databases 3
        unknown ignored
        "#,
    );

    assert_eq!(cfg.port, 7000);
    assert_eq!(cfg.bind, "127.0.0.1 ::1");
    assert_eq!(cfg.loglevel, "warning");
    assert_eq!(cfg.logfile, "server.log");
    assert_eq!(cfg.databases, 32);
    assert!(cfg.daemonize);
    assert_eq!(cfg.requirepass.as_deref(), Some(b"secret".as_slice()));
    assert_eq!(cfg.dir, "/tmp/valkey");
    assert_eq!(cfg.dbfilename, "custom.rdb");
    assert_eq!(
        cfg.save.as_ref().unwrap(),
        &vec![
            "300".to_string(),
            "10".to_string(),
            "60".to_string(),
            "1000".to_string()
        ]
    );
    assert_eq!(cfg.shutdown_on_sigterm, vec!["now".to_string()]);
    assert_eq!(cfg.shutdown_on_sigint, vec!["no".to_string()]);
    assert!(cfg.appendonly);
    assert_eq!(cfg.appendfilename, "append.aof");
    assert_eq!(cfg.appenddirname, "aofdir");
    assert_eq!(cfg.appendfsync, "no");
    assert_eq!(cfg.repl_timeout, 7);
    assert_eq!(cfg.replica_announce_port, 17000);
    assert!(!cfg.replica_ignore_maxmemory);
    assert!(cfg.repl_disable_tcp_nodelay);
    assert!(cfg.no_appendfsync_on_rewrite);
    assert!(!cfg.lazyfree_lazy_eviction);
    assert!(!cfg.lazyfree_lazy_expire);
    assert!(!cfg.lazyfree_lazy_server_del);
    assert!(!cfg.aof_use_rdb_preamble);
    assert_eq!(cfg.rdb_version_check, "relaxed");
    assert_eq!(cfg.tcp_keepalive, 17);
    assert_eq!(cfg.active_expire_effort, 7);
    assert_eq!(cfg.client_timeout, 19);
    assert_eq!(cfg.tcp_backlog, 321);
    assert_eq!(cfg.list_max_listpack_size, -4);
    assert_eq!(cfg.set_max_intset_entries, 16);
    assert_eq!(cfg.set_max_listpack_entries, 8);
    assert_eq!(cfg.set_max_listpack_value, 32);
    assert_eq!(cfg.stream_node_max_entries, 10);
    assert!(cfg.mptcp);
    assert_eq!(cfg.maxclients, 99);
    assert_eq!(cfg.max_new_connections_per_cycle, 25);
    assert_eq!(cfg.max_new_tls_connections_per_cycle, 3);
    assert_eq!(cfg.enable_module_command, "false");
    assert_eq!(cfg.commandlog_slow_threshold, 10);
    assert_eq!(cfg.commandlog_slow_max_len, 11);
    assert_eq!(cfg.commandlog_large_request_threshold, 12);
    assert_eq!(cfg.commandlog_large_request_max_len, 13);
    assert_eq!(cfg.commandlog_large_reply_threshold, 14);
    assert_eq!(cfg.commandlog_large_reply_max_len, 15);
    assert_eq!(
        cfg.sentinel_directives,
        vec!["monitor mymaster 127.0.0.1 6379 2".to_string()]
    );
    assert!(cfg.cluster_enabled);
    assert_eq!(cfg.cluster_port, 17000);
    assert_eq!(cfg.cluster_ping_interval, 100);
    assert_eq!(cfg.cluster_node_timeout, 1234);
    assert_eq!(cfg.cluster_migration_barrier, 999);
    assert_eq!(cfg.cluster_config_file, "cluster.conf");
    assert_eq!(cfg.cluster_announce_ip, "10.0.0.1");
    assert_eq!(cfg.cluster_announce_client_ipv4, "192.0.2.10");
    assert_eq!(cfg.cluster_announce_client_ipv6, "2001:db8::10");
    assert_eq!(cfg.cluster_announce_hostname, "node.local");
    assert_eq!(cfg.cluster_announce_human_nodename, "node-a");
    assert_eq!(cfg.cluster_announce_port, 7001);
    assert_eq!(cfg.cluster_announce_tls_port, 7443);
    assert_eq!(cfg.cluster_announce_client_port, 7002);
    assert_eq!(cfg.cluster_announce_client_tls_port, 7444);
    assert_eq!(cfg.cluster_announce_bus_port, 17001);
    assert_eq!(cfg.cluster_config_save_behavior, "best-effort");
    assert_eq!(cfg.cluster_message_gossip_perc, 80);
    assert_eq!(cfg.availability_zone, "zone-a");
    assert!(cfg.cluster_slot_stats_enabled);
    assert_eq!(cfg.cluster_databases, 3);
}

#[test]
fn empty_requirepass_and_save_clear_previous_values() {
    let mut cfg = default_config();
    cfg.requirepass = Some(b"secret".to_vec());
    cfg.save = Some(vec!["60".to_string(), "1".to_string()]);

    cfg.parse_config_string(
        r#"
        requirepass ""
        save
        "#,
    );

    assert!(cfg.requirepass.is_none());
    assert_eq!(cfg.save, Some(Vec::new()));
}

#[test]
fn table_driven_setters_cover_str_bool_and_parsed_classes() {
    let mut cfg = default_config();
    cfg.parse_config_string(
        "dir /x\nport notanumber\nport 1234\ndaemonize TRUE\nappendfsync ALWAYS\nunknown-key val\n",
    );
    assert_eq!(cfg.dir, "/x"); // Str
    assert_eq!(cfg.port, 1234); // ParsedU16: bad line ignored, good line applied
    assert!(cfg.daemonize); // Bool: "TRUE" (case-insensitive) → true
    assert_eq!(cfg.appendfsync, "always"); // Lower
}

#[test]
fn apply_to_transfers_cluster_and_commandlog_options() {
    let mut cfg = default_config();
    cfg.config_file = "valkey.conf".to_string();
    cfg.requirepass = Some(b"pw".to_vec());
    cfg.port = 7000;
    cfg.sentinel = true;
    cfg.mptcp = true;
    cfg.cluster_enabled = true;
    cfg.cluster_port = 17000;
    cfg.cluster_ping_interval = 100;
    cfg.cluster_node_timeout = 2222;
    cfg.cluster_migration_barrier = 7;
    cfg.cluster_config_file = "nodes-test.conf".to_string();
    cfg.cluster_announce_ip = "10.0.0.2".to_string();
    cfg.cluster_announce_client_ipv4 = "192.0.2.20".to_string();
    cfg.cluster_announce_client_ipv6 = "2001:db8::20".to_string();
    cfg.cluster_announce_hostname = "host".to_string();
    cfg.cluster_announce_human_nodename = "human".to_string();
    cfg.cluster_announce_port = 7001;
    cfg.cluster_announce_tls_port = 7443;
    cfg.cluster_announce_client_port = 7002;
    cfg.cluster_announce_client_tls_port = 7444;
    cfg.cluster_announce_bus_port = 17001;
    cfg.cluster_config_save_behavior = "best-effort".to_string();
    cfg.cluster_message_gossip_perc = 80;
    cfg.availability_zone = "zone-a".to_string();
    cfg.cluster_slot_stats_enabled = true;
    cfg.cluster_databases = 2;
    cfg.commandlog_slow_threshold = 1;
    cfg.commandlog_slow_max_len = 2;
    cfg.commandlog_large_request_threshold = 3;
    cfg.commandlog_large_request_max_len = 4;
    cfg.commandlog_large_reply_threshold = 5;
    cfg.commandlog_large_reply_max_len = 6;
    cfg.stream_node_max_entries = 10;
    cfg.lazyfree_lazy_expire = false;
    cfg.tcp_keepalive = 23;
    cfg.active_expire_effort = 8;
    cfg.max_new_connections_per_cycle = 31;
    cfg.max_new_tls_connections_per_cycle = 5;
    cfg.client_timeout = 29;
    cfg.replica_announce_port = 16379;
    cfg.tcp_backlog = 127;
    cfg.replica_ignore_maxmemory = false;
    cfg.repl_disable_tcp_nodelay = true;
    let mut runtime = RuntimeConfig::new();

    cfg.apply_to(&mut runtime);

    assert_eq!(runtime.security.requirepass, Some(b"pw".to_vec()));
    assert_eq!(runtime.server.config_file.as_deref(), Some("valkey.conf"));
    assert_eq!(runtime.server.listening_port, 7000);
    assert_eq!(runtime.encoding.stream_node_max_entries, 10);
    assert!(!runtime.server.lazyfree_lazy_expire);
    assert_eq!(runtime.server.tcp_keepalive, 23);
    assert_eq!(runtime.server.active_expire_effort, 8);
    assert_eq!(runtime.server.max_new_connections_per_cycle, 31);
    assert_eq!(runtime.server.max_new_tls_connections_per_cycle, 5);
    assert_eq!(runtime.server.client_timeout, 29);
    assert_eq!(runtime.replication.replica_announce_port, 16379);
    assert_eq!(runtime.server.tcp_backlog, 127);
    assert!(!runtime.replication.replica_ignore_maxmemory);
    assert!(runtime.replication.repl_disable_tcp_nodelay);
    assert!(runtime.server.sentinel_mode);
    assert!(runtime.server.mptcp);
    assert!(runtime.server.cluster_enabled);
    assert_eq!(runtime.server.cluster_port, 17000);
    assert_eq!(runtime.server.cluster_ping_interval, 100);
    assert_eq!(runtime.server.cluster_node_timeout, 2222);
    assert_eq!(runtime.server.cluster_migration_barrier, 7);
    assert_eq!(runtime.server.cluster_config_file, "nodes-test.conf");
    assert_eq!(runtime.server.cluster_announce_ip, "10.0.0.2");
    assert_eq!(runtime.server.cluster_announce_client_ipv4, "192.0.2.20");
    assert_eq!(runtime.server.cluster_announce_client_ipv6, "2001:db8::20");
    assert_eq!(runtime.server.cluster_announce_hostname, "host");
    assert_eq!(runtime.server.cluster_announce_human_nodename, "human");
    assert_eq!(runtime.server.cluster_announce_port, 7001);
    assert_eq!(runtime.server.cluster_announce_tls_port, 7443);
    assert_eq!(runtime.server.cluster_announce_client_port, 7002);
    assert_eq!(runtime.server.cluster_announce_client_tls_port, 7444);
    assert_eq!(runtime.server.cluster_announce_bus_port, 17001);
    assert_eq!(runtime.server.cluster_config_save_behavior, "best-effort");
    assert_eq!(runtime.server.cluster_message_gossip_perc, 80);
    assert_eq!(runtime.server.availability_zone, "zone-a");
    assert!(runtime.server.cluster_slot_stats_enabled);
    assert_eq!(runtime.server.cluster_databases, 2);
    assert_eq!(runtime.server.commandlog_slow_threshold, 1);
    assert_eq!(runtime.server.commandlog_slow_max_len, 2);
    assert_eq!(runtime.server.large_request_threshold, 3);
    assert_eq!(runtime.server.large_request_max_len, 4);
    assert_eq!(runtime.server.large_reply_threshold, 5);
    assert_eq!(runtime.server.large_reply_max_len, 6);
}

#[test]
fn load_from_file_ignores_empty_and_missing_paths_and_reads_existing_file() {
    let mut cfg = default_config();
    cfg.load_from_file("").unwrap();
    cfg.load_from_file("/tmp/valkey-rust-config-missing-do-not-create")
        .unwrap();

    let path = std::env::temp_dir().join(format!(
        "valkey-rust-config-{}-{}.conf",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::write(&path, "port 7111\nappendonly yes\n").unwrap();
    cfg.load_from_file(path.to_str().unwrap()).unwrap();
    let _ = std::fs::remove_file(&path);

    assert_eq!(cfg.port, 7111);
    assert!(cfg.appendonly);
}

#[test]
fn cli_save_flag_does_not_consume_trailing_loglevel() {
    // Covers introspection.tcl {save with empty input} variants:
    // --save must greedily eat 0 or 1 empty-string value, but stop at a
    // foreign flag like --loglevel so that the following positional
    // argument stays attached to its owner flag.
    let cfg = Config::parse_from(["versal-server", "--save", "--loglevel", "verbose"]);
    assert_eq!(cfg.save, Some(vec!["".to_string()]));
    assert_eq!(cfg.loglevel, "verbose");

    let cfg = Config::parse_from(["versal-server", "--loglevel", "verbose", "--save"]);
    assert_eq!(cfg.save, Some(vec!["".to_string()]));
    assert_eq!(cfg.loglevel, "verbose");

    let cfg = Config::parse_from(["versal-server", "--save", "", "--loglevel", "verbose"]);
    assert_eq!(cfg.save, Some(vec!["".to_string()]));
    assert_eq!(cfg.loglevel, "verbose");

    let cfg = Config::parse_from([
        "versal-server",
        "--save",
        "",
        "--save",
        "60 1000",
        "--loglevel",
        "verbose",
    ]);
    assert_eq!(cfg.save, Some(vec!["".to_string(), "60 1000".to_string()]));
    assert_eq!(cfg.loglevel, "verbose");
}
