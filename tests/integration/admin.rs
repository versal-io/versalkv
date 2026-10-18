use crate::common::TestServer;
use std::time::{Duration, Instant};

#[test]
fn info_server_section_has_version() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let reply = c.command_str(&[b"INFO", b"server"]);
    assert!(reply.contains("redis_version:"), "got: {}", reply);
    assert!(reply.contains("server_mode:standalone"), "got: {}", reply);
}

#[test]
fn info_keyspace_reflects_writes() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SET", b"k1", b"v"]);
    c.command(&[b"SET", b"k2", b"v"]);
    let reply = c.command_str(&[b"INFO", b"keyspace"]);
    assert!(reply.contains("db0:keys=2"), "got: {}", reply);
}

#[test]
fn info_default_sections_contain_expected_fields() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let reply = c.command_str(&[b"INFO"]);
    assert!(reply.contains("redis_version:"), "missing redis_version");
    assert!(reply.contains("used_cpu_user:"), "missing used_cpu_user");
    assert!(reply.contains("used_memory:"), "missing used_memory");
    assert!(
        !reply.contains("sentinel_tilt"),
        "should not have sentinel_tilt"
    );
    assert!(
        !reply.contains("rejected_calls"),
        "default should not have commandstats rejected_calls"
    );
}

#[test]
fn info_all_includes_commandstats() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let reply = c.command_str(&[b"INFO", b"all"]);
    assert!(
        reply.contains("rejected_calls"),
        "all should have commandstats rejected_calls"
    );
    assert!(
        reply.contains("redis_version:"),
        "all should still have server section"
    );
}

fn extract_info_bulk(raw: &str) -> &str {
    let idx = raw.find("\r\n").unwrap();
    &raw[idx + 2..raw.len() - 2]
}

fn info_sections(raw: &str) -> Vec<&str> {
    let content = extract_info_bulk(raw);
    content.split("\r\n# ").collect()
}

#[test]
fn info_everything_same_as_all() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let all = c.command_str(&[b"INFO", b"all"]);
    let everything = c.command_str(&[b"INFO", b"everything"]);
    let all_sections = info_sections(&all);
    let every_sections = info_sections(&everything);
    assert_eq!(all_sections.len(), every_sections.len());
    for (a, e) in all_sections.iter().zip(every_sections.iter()) {
        let a_header = a.lines().next().unwrap_or("");
        let e_header = e.lines().next().unwrap_or("");
        assert_eq!(a_header, e_header, "section header mismatch");
    }
}

#[test]
fn info_default_same_as_plain() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let plain = c.command_str(&[b"INFO"]);
    let default = c.command_str(&[b"INFO", b"default"]);
    let plain_sections = info_sections(&plain);
    let default_sections = info_sections(&default);
    assert_eq!(plain_sections.len(), default_sections.len());
    for (p, d) in plain_sections.iter().zip(default_sections.iter()) {
        let p_header = p.lines().next().unwrap_or("");
        let d_header = d.lines().next().unwrap_or("");
        assert_eq!(p_header, d_header, "section header mismatch");
    }
}

#[test]
fn info_single_section_cpu() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let reply = c.command_str(&[b"INFO", b"cpu"]);
    assert!(reply.contains("used_cpu_user:"), "missing used_cpu_user");
    assert!(
        !reply.contains("used_memory:"),
        "should not have memory section"
    );
    assert!(!reply.contains("sentinel_tilt"), "should not have sentinel");
}

#[test]
fn info_sentinel_empty_in_non_sentinel_mode() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let reply = c.command_str(&[b"INFO", b"sentinel"]);
    assert!(
        !reply.contains("sentinel_tilt"),
        "should not have sentinel_tilt"
    );
    assert!(!reply.contains("used_memory:"), "should not have memory");
}

#[test]
fn info_section_names_case_insensitive() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let lower = c.command_str(&[b"INFO", b"server"]);
    let upper = c.command_str(&[b"INFO", b"SERVER"]);
    let lower_content = extract_info_bulk(&lower);
    let upper_content = extract_info_bulk(&upper);
    let lower_fields: Vec<&str> = lower_content
        .lines()
        .filter(|l| !l.starts_with("server_time_usec:"))
        .collect();
    let upper_fields: Vec<&str> = upper_content
        .lines()
        .filter(|l| !l.starts_with("server_time_usec:"))
        .collect();
    assert_eq!(lower_fields, upper_fields);
}

#[test]
fn info_multiple_sections() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let reply = c.command_str(&[b"INFO", b"cpu", b"memory"]);
    assert!(reply.contains("used_cpu_user:"), "missing cpu");
    assert!(reply.contains("used_memory:"), "missing memory");
    assert!(
        !reply.contains("redis_version:"),
        "should not have server section"
    );
}

#[test]
fn info_cpu_all_no_duplicates() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let reply = c.command_str(&[b"INFO", b"cpu", b"all"]);
    assert!(reply.contains("used_cpu_user_children:"));
    let count = reply.matches("used_cpu_user_children:").count();
    assert_eq!(count, 1, "duplicate cpu section found: {}", reply);
    assert!(reply.contains("used_memory:"), "all should include memory");
    assert!(
        reply.contains("master_repl_offset:"),
        "all should include replication"
    );
    assert!(
        reply.contains("rejected_calls"),
        "all should include commandstats"
    );
}

#[test]
fn info_cpu_default_no_commandstats() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let reply = c.command_str(&[b"INFO", b"cpu", b"default"]);
    assert!(reply.contains("used_cpu_user_children:"));
    let count = reply.matches("used_cpu_user_children:").count();
    assert_eq!(count, 1, "duplicate cpu section found");
    assert!(
        reply.contains("used_memory:"),
        "default should include memory"
    );
    assert!(
        reply.contains("master_repl_offset:"),
        "default should include replication"
    );
    assert!(
        !reply.contains("rejected_calls"),
        "default should not include commandstats"
    );
}

#[test]
fn info_stats_has_keyspace_hits_and_misses() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SET", b"k1", b"v1"]);
    c.command(&[b"GET", b"k1"]);
    c.command(&[b"GET", b"nonexistent"]);
    let reply = c.command_str(&[b"INFO", b"stats"]);
    assert!(
        reply.contains("keyspace_hits:"),
        "missing keyspace_hits: {}",
        reply
    );
    assert!(
        reply.contains("keyspace_misses:"),
        "missing keyspace_misses: {}",
        reply
    );
    assert!(
        reply.contains("total_commands_processed:"),
        "missing total_commands_processed"
    );
    assert!(
        reply.contains("pubsub_channels:"),
        "missing pubsub_channels"
    );
    assert!(
        reply.contains("pubsub_patterns:"),
        "missing pubsub_patterns"
    );
}

#[test]
fn info_server_has_run_id_and_valkey_version() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let reply = c.command_str(&[b"INFO", b"server"]);
    assert!(reply.contains("run_id:"), "missing run_id");
    assert!(reply.contains("valkey_version:"), "missing valkey_version");
    assert!(reply.contains("server_name:valkey"), "missing server_name");
}

#[test]
fn info_cluster_section_shows_disabled() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let reply = c.command_str(&[b"INFO", b"cluster"]);
    assert!(
        reply.contains("cluster_enabled:0"),
        "expected cluster_enabled:0, got: {}",
        reply
    );
}

#[test]
fn debug_unknown_subcommand_reports_syntax_error() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let reply = c.command_str(&[b"DEBUG", b"NOSUCH"]);
    assert!(
        reply.starts_with("-ERR syntax error"),
        "unexpected reply: {}",
        reply
    );
}

#[test]
fn debug_help_lists_c_compatible_subcommands() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let reply = c.command_str(&[b"DEBUG", b"HELP"]);
    assert!(reply.contains("LOADAOF"), "missing LOADAOF: {}", reply);
    assert!(reply.contains("DIGEST"), "missing DIGEST: {}", reply);
    assert!(reply.contains("POPULATE"), "missing POPULATE: {}", reply);
    assert!(
        reply.contains("CLUSTERLINK KILL"),
        "missing cluster help: {}",
        reply
    );
}

#[test]
fn debug_protocol_uses_fixed_c_test_values() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(
        c.command(&[b"DEBUG", b"PROTOCOL", b"string"]),
        b"$11\r\nHello World\r\n"
    );
    assert_eq!(
        c.command(&[b"DEBUG", b"PROTOCOL", b"integer"]),
        b":12345\r\n"
    );
    assert_eq!(c.command(&[b"DEBUG", b"PROTOCOL", b"true"]), b":1\r\n");
    assert_eq!(c.command(&[b"DEBUG", b"PROTOCOL", b"false"]), b":0\r\n");
    let bad = c.command_str(&[b"DEBUG", b"PROTOCOL", b"unknown"]);
    assert!(
        bad.starts_with("-ERR Wrong protocol type name."),
        "unexpected reply: {}",
        bad
    );
}

#[test]
fn debug_error_returns_requested_error_reply() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(
        c.command(&[b"DEBUG", b"ERROR", b"ERR simulated\r\nfailure"]),
        b"-ERR simulated  failure\r\n"
    );
}

#[test]
fn debug_populate_creates_c_compatible_string_keys() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(
        c.command(&[b"DEBUG", b"POPULATE", b"3", b"seed", b"12"]),
        b"+OK\r\n"
    );
    assert_eq!(
        c.command(&[b"GET", b"seed:0"]),
        b"$12\r\nvalue:0\0\0\0\0\0\r\n"
    );
    assert_eq!(
        c.command(&[b"GET", b"seed:2"]),
        b"$12\r\nvalue:2\0\0\0\0\0\r\n"
    );
    assert_eq!(c.command(&[b"DBSIZE"]), b":3\r\n");
}

#[test]
fn debug_digest_and_digest_value_are_stable_for_same_dataset() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SET", b"a", b"1"]);
    c.command(&[b"SET", b"b", b"2"]);
    let digest1 = c.command(&[b"DEBUG", b"DIGEST"]);
    let digest2 = c.command(&[b"DEBUG", b"DIGEST"]);
    assert_eq!(digest1, digest2);
    assert_eq!(digest1.len(), 43, "status + 40 hex bytes + CRLF");

    let values = c.command_str(&[b"DEBUG", b"DIGEST-VALUE", b"a", b"missing"]);
    assert!(
        values.starts_with("*2\r\n+"),
        "unexpected digest-value reply: {}",
        values
    );
    assert!(
        values.contains("+0000000000000000000000000000000000000000\r\n"),
        "missing key should use zero digest: {}",
        values
    );
}

#[test]
fn debug_digest_value_ignores_key_name_for_copied_values() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();

    c.command(&[b"SET", b"src-string", b"value"]);
    assert_eq!(
        c.command(&[b"COPY", b"src-string", b"dst-string"]),
        b":1\r\n"
    );
    let string_digests = c.command_str(&[b"DEBUG", b"DIGEST-VALUE", b"src-string", b"dst-string"]);
    assert!(
        string_digests.starts_with("*2\r\n+"),
        "unexpected digest reply: {}",
        string_digests
    );
    let string_values: Vec<&str> = string_digests
        .lines()
        .filter_map(|line| line.strip_prefix('+'))
        .collect();
    assert_eq!(string_values, vec![string_values[0], string_values[0]]);

    c.command(&[b"RPUSH", b"src-list", b"a", b"b", b"c"]);
    assert_eq!(c.command(&[b"COPY", b"src-list", b"dst-list"]), b":1\r\n");
    let list_digests = c.command_str(&[b"DEBUG", b"DIGEST-VALUE", b"src-list", b"dst-list"]);
    let list_values: Vec<&str> = list_digests
        .lines()
        .filter_map(|line| line.strip_prefix('+'))
        .collect();
    assert_eq!(list_values, vec![list_values[0], list_values[0]]);
}

#[test]
fn debug_reload_supports_nosave_option() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SET", b"persisted", b"old"]);
    assert_eq!(c.command(&[b"SAVE"]), b"+OK\r\n");
    c.command(&[b"SET", b"persisted", b"new"]);
    assert_eq!(c.command(&[b"DEBUG", b"RELOAD", b"NOSAVE"]), b"+OK\r\n");
    assert_eq!(c.command(&[b"GET", b"persisted"]), b"$3\r\nold\r\n");
}

#[test]
fn debug_reload_services_other_clients_while_loading() {
    let server = TestServer::start_default();
    let mut seed = server.connect();
    for index in 0..200 {
        let key = format!("reload-{index}");
        assert_eq!(
            seed.command(&[b"SET", key.as_bytes(), b"value"]),
            b"+OK\r\n"
        );
    }
    assert_eq!(seed.command(&[b"SAVE"]), b"+OK\r\n");
    assert_eq!(
        seed.command(&[b"CONFIG", b"SET", b"key-load-delay", b"10000"]),
        b"+OK\r\n"
    );
    assert_eq!(
        seed.command(&[
            b"CONFIG",
            b"SET",
            b"loading-process-events-interval-bytes",
            b"1024",
        ]),
        b"+OK\r\n"
    );
    let port = server.port;
    let reload = std::thread::spawn(move || {
        let mut client = crate::common::Client::connect_to(port);
        client.command(&[b"DEBUG", b"RELOAD", b"NOSAVE"])
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    let mut observer = server.connect();
    loop {
        let info = observer.command_str(&[b"INFO", b"persistence"]);
        if info.contains("loading:1") {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "DEBUG RELOAD never entered loading"
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert!(observer
        .command_str(&[b"SET", b"during", b"reload"])
        .contains("LOADING"));
    assert_eq!(reload.join().expect("join reload"), b"+OK\r\n");
}

#[test]
fn debug_loadaof_replays_through_the_host_dispatcher() {
    let dir = std::env::temp_dir().join(format!(
        "pkv-debug-loadaof-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    let append_dir = dir.join("appendonlydir");
    std::fs::create_dir_all(&append_dir).expect("create AOF directory");
    std::fs::write(
        append_dir.join("appendonly.aof.manifest"),
        b"file appendonly.aof.1.incr.aof seq 1 type i\n",
    )
    .expect("write AOF manifest");
    std::fs::write(
        append_dir.join("appendonly.aof.1.incr.aof"),
        b"*3\r\n$3\r\nSET\r\n$8\r\nfrom-aof\r\n$2\r\nok\r\n",
    )
    .expect("write AOF increment");
    let server = TestServer::start_in_dir(dir, &[]);
    let mut client = server.connect();

    assert_eq!(client.command_str(&[b"DEBUG", b"LOADAOF"]), "+OK\r\n");
    assert_eq!(client.command_str(&[b"GET", b"from-aof"]), "$2\r\nok\r\n");
}

#[test]
fn debug_log_leak_and_test_toggles_return_ok() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(c.command(&[b"DEBUG", b"LOG", b"hello"]), b"+OK\r\n");
    assert_eq!(c.command(&[b"DEBUG", b"LEAK", b"hello"]), b"+OK\r\n");
    assert_eq!(
        c.command(&[b"DEBUG", b"STRINGMATCH-TEST"]),
        b"+Apparently the server did not crash: test passed\r\n"
    );
    assert_eq!(
        c.command(&[b"DEBUG", b"AOF-FLUSH-SLEEP", b"10"]),
        b"+OK\r\n"
    );
    assert_eq!(
        c.command(&[b"DEBUG", b"SET-DISABLE-DENY-SCRIPTS", b"1"]),
        b"+OK\r\n"
    );
    assert_eq!(
        c.command(&[b"DEBUG", b"HASHTABLE-CAN-ABORT-SHRINK", b"0"]),
        b"+OK\r\n"
    );
}

#[test]
fn debug_replybuffer_peak_reset_time_accepts_test_values() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(
        c.command(&[b"DEBUG", b"REPLYBUFFER", b"PEAK-RESET-TIME", b"100"]),
        b"+OK\r\n"
    );
    assert_eq!(
        c.command(&[b"DEBUG", b"REPLYBUFFER", b"PEAK-RESET-TIME", b"never"]),
        b"+OK\r\n"
    );
    assert_eq!(
        c.command(&[b"DEBUG", b"REPLYBUFFER", b"PEAK-RESET-TIME", b"reset"]),
        b"+OK\r\n"
    );
}

#[test]
fn debug_structsize_returns_core_size_fields() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let reply = c.command_str(&[b"DEBUG", b"STRUCTSIZE"]);
    assert!(reply.starts_with("$"), "expected bulk reply: {}", reply);
    for field in [
        "bits:",
        "robj:",
        "dictentry:",
        "sdshdr5:",
        "sdshdr8:",
        "sdshdr16:",
        "sdshdr32:",
        "sdshdr64:",
    ] {
        assert!(reply.contains(field), "missing {} in {}", field, reply);
    }
}

#[test]
fn debug_sdslen_reports_string_layout_and_type_errors() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SET", b"alpha", b"bravo"]);
    let reply = c.command_str(&[b"DEBUG", b"SDSLEN", b"alpha"]);
    for field in [
        "key_sds_len:5",
        "key_sds_avail:",
        "obj_alloc:",
        "val_sds_len:5",
        "val_sds_avail:",
        "val_alloc:",
    ] {
        assert!(reply.contains(field), "missing {} in {}", field, reply);
    }

    c.command(&[b"LPUSH", b"alist", b"x"]);
    let wrong_type = c.command_str(&[b"DEBUG", b"SDSLEN", b"alist"]);
    assert!(
        wrong_type.starts_with("-ERR Not an sds encoded string."),
        "unexpected reply: {}",
        wrong_type
    );
}

#[test]
fn debug_listpack_and_quicklist_report_c_compatible_shapes() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"HSET", b"h", b"f", b"v"]);
    assert_eq!(
        c.command(&[b"DEBUG", b"LISTPACK", b"h"]),
        b"+Listpack structure printed on stdout\r\n"
    );
    c.command(&[b"SET", b"s", b"v"]);
    let not_listpack = c.command_str(&[b"DEBUG", b"LISTPACK", b"s"]);
    assert!(
        not_listpack.starts_with("-ERR Not a listpack encoded object."),
        "unexpected reply: {}",
        not_listpack
    );

    c.command(&[b"LPUSH", b"l", b"x"]);
    assert_eq!(
        c.command(&[b"DEBUG", b"QUICKLIST", b"l", b"1"]),
        b"+Quicklist structure printed on stdout\r\n"
    );
    let not_quicklist = c.command_str(&[b"DEBUG", b"QUICKLIST", b"s"]);
    assert!(
        not_quicklist.starts_with("-ERR Not a quicklist encoded object."),
        "unexpected reply: {}",
        not_quicklist
    );
}

#[test]
fn debug_htstats_key_reports_hashtable_backed_values() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(
        c.command(&[b"CONFIG", b"SET", b"hash-max-listpack-entries", b"1"]),
        b"+OK\r\n"
    );
    c.command(&[b"HSET", b"h", b"f", b"v", b"f2", b"v2"]);
    let reply = c.command_str(&[b"DEBUG", b"HTSTATS-KEY", b"h"]);
    assert!(
        reply.contains("Hash table 0 stats"),
        "unexpected reply: {}",
        reply
    );

    c.command(&[b"SET", b"s", b"v"]);
    let wrong_type = c.command_str(&[b"DEBUG", b"HTSTATS-KEY", b"s"]);
    assert!(
        wrong_type.starts_with(
            "-ERR The value stored at the specified key is not represented using an hash table"
        ),
        "unexpected reply: {}",
        wrong_type
    );
}

#[test]
fn info_cpu_has_active_time_after_activity() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    for _ in 0..100 {
        c.command(&[b"PING"]);
    }
    let reply = c.command_str(&[b"INFO", b"cpu"]);
    assert!(
        reply.contains("used_active_time_main_thread:"),
        "missing used_active_time_main_thread"
    );
}

#[test]
fn command_count_is_positive() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let reply = c.command_str(&[b"COMMAND", b"COUNT"]);
    let n: i64 = reply.trim_start_matches(':').trim().parse().unwrap();
    assert!(n > 0, "expected positive command count, got {}", n);
}

#[test]
fn config_get_set_requirepass_roundtrip() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(
        c.command(&[b"CONFIG", b"SET", b"requirepass", b"pw"]),
        b"+OK\r\n"
    );
    assert_eq!(c.command(&[b"AUTH", b"pw"]), b"+OK\r\n");
    let reply = c.command_str(&[b"CONFIG", b"GET", b"requirepass"]);
    assert!(reply.contains("pw"), "got: {}", reply);
}

#[test]
fn config_get_hides_key_load_delay_from_patterns_but_allows_exact_lookup() {
    let srv = TestServer::start_default();
    let mut client = srv.connect();

    let wildcard = client.command_str(&[b"CONFIG", b"GET", b"*"]);
    assert!(
        !wildcard.contains("$14\r\nkey-load-delay\r\n"),
        "hidden config leaked through wildcard lookup: {wildcard}"
    );

    let exact = client.command_str(&[b"CONFIG", b"GET", b"key-load-delay"]);
    assert!(
        exact.contains("$14\r\nkey-load-delay\r\n"),
        "exact hidden config lookup did not return the key: {exact}"
    );
}

#[test]
fn replicaof_switches_role_to_slave() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let reply = c.command_str(&[b"INFO", b"replication"]);
    assert!(reply.contains("role:master"), "got: {}", reply);

    assert_eq!(
        c.command(&[b"REPLICAOF", b"127.0.0.1", b"6380"]),
        b"+OK\r\n"
    );
    let reply = c.command_str(&[b"INFO", b"replication"]);
    assert!(reply.contains("role:slave"), "got: {}", reply);
    assert!(reply.contains("master_port:6380"), "got: {}", reply);

    assert_eq!(c.command(&[b"REPLICAOF", b"NO", b"ONE"]), b"+OK\r\n");
    let reply = c.command_str(&[b"INFO", b"replication"]);
    assert!(reply.contains("role:master"), "got: {}", reply);
}

#[test]
fn client_kill_by_filter_works_inside_multi_exec() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();

    assert_eq!(c.command(&[b"MULTI"]), b"+OK\r\n");
    assert_eq!(
        c.command(&[b"CLIENT", b"KILL", b"TYPE", b"replica"]),
        b"+QUEUED\r\n"
    );
    let reply = c.command(&[b"EXEC"]);
    assert_eq!(
        reply,
        b"*1\r\n:0\r\n",
        "CLIENT KILL TYPE inside MULTI/EXEC must run, got: {}",
        String::from_utf8_lossy(&reply)
    );
}

#[test]
fn client_kill_type_primary_accepts_valkey_alias() {
    let primary = TestServer::start_default();
    let replica = TestServer::start_default();
    let mut replica_client = replica.connect();
    let primary_port = primary.port.to_string();

    assert_eq!(
        replica_client.command(&[b"REPLICAOF", b"127.0.0.1", primary_port.as_bytes()]),
        b"+OK\r\n"
    );

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let info = replica_client.command_str(&[b"INFO", b"replication"]);
        if info.contains("master_link_status:up") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "replica did not reach master_link_status:up: {info}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    assert_eq!(
        replica_client.command(&[b"CLIENT", b"KILL", b"TYPE", b"primary"]),
        b":1\r\n"
    );
}

#[test]
fn client_kill_not_type_primary_preserves_upstream_link() {
    let primary = TestServer::start_default();
    let replica = TestServer::start_default();
    let mut admin = replica.connect();
    let primary_port = primary.port.to_string();

    assert_eq!(
        admin.command(&[b"REPLICAOF", b"127.0.0.1", primary_port.as_bytes()]),
        b"+OK\r\n"
    );

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let info = admin.command_str(&[b"INFO", b"replication"]);
        if info.contains("master_link_status:up") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "replica did not reach master_link_status:up: {info}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    let mut victim = replica.connect();
    assert_eq!(
        victim.command(&[b"CLIENT", b"SETNAME", b"not-type-primary-victim"]),
        b"+OK\r\n"
    );
    let normal_clients = admin.command_str(&[b"CLIENT", b"LIST", b"TYPE", b"normal"]);
    assert!(
        normal_clients.contains("name=not-type-primary-victim"),
        "normal victim was not visible before CLIENT KILL: {normal_clients}"
    );

    assert_eq!(
        admin.command(&[b"CLIENT", b"KILL", b"NOT-TYPE", b"primary"]),
        b":1\r\n"
    );
    assert!(victim.is_closed(), "normal victim was not killed");

    let info = admin.command_str(&[b"INFO", b"replication"]);
    assert!(
        info.contains("master_link_status:up"),
        "NOT-TYPE primary killed the upstream primary link: {info}"
    );
}

#[test]
fn swapdb_stale_replacement_does_not_dirty_missing_watch() {
    let srv = TestServer::start_default();
    let mut client = srv.connect();

    assert_eq!(
        client.command(&[b"DEBUG", b"SET-ACTIVE-EXPIRE", b"0"]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"SET", b"x", b"value", b"PX", b"1"]),
        b"+OK\r\n"
    );
    std::thread::sleep(Duration::from_millis(10));
    assert_eq!(client.command(&[b"SELECT", b"1"]), b"+OK\r\n");
    assert_eq!(client.command(&[b"WATCH", b"x"]), b"+OK\r\n");
    assert_eq!(client.command(&[b"SWAPDB", b"0", b"1"]), b"+OK\r\n");
    assert_eq!(client.command(&[b"MULTI"]), b"+OK\r\n");
    assert_eq!(client.command(&[b"PING"]), b"+QUEUED\r\n");
    assert_eq!(client.command(&[b"EXEC"]), b"*1\r\n+PONG\r\n");
}

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
fn transaction_swapdb_dirties_other_client_watch() {
    let srv = TestServer::start_default();
    let mut watcher = srv.connect();
    let mut transaction = srv.connect();

    assert_eq!(transaction.command(&[b"SELECT", b"1"]), b"+OK\r\n");
    assert_eq!(transaction.command(&[b"SET", b"x", b"value"]), b"+OK\r\n");
    assert_eq!(transaction.command(&[b"SELECT", b"0"]), b"+OK\r\n");
    assert_eq!(watcher.command(&[b"WATCH", b"x"]), b"+OK\r\n");
    assert_eq!(transaction.command(&[b"MULTI"]), b"+OK\r\n");
    assert_eq!(
        transaction.command(&[b"SWAPDB", b"0", b"1"]),
        b"+QUEUED\r\n"
    );
    assert_eq!(transaction.command(&[b"EXEC"]), b"*1\r\n+OK\r\n");
    assert_eq!(watcher.command(&[b"MULTI"]), b"+OK\r\n");
    assert_eq!(watcher.command(&[b"PING"]), b"+QUEUED\r\n");
    assert_eq!(watcher.command(&[b"EXEC"]), b"*-1\r\n");
}

#[test]
fn swapdb_blocked_key_stale_replacement_does_not_dirty_missing_watch() {
    let srv = TestServer::start_default();
    let mut admin = srv.connect();
    let mut watcher = srv.connect();
    let mut blocked = srv.connect();

    assert_eq!(
        admin.command(&[b"DEBUG", b"SET-ACTIVE-EXPIRE", b"0"]),
        b"+OK\r\n"
    );
    assert_eq!(
        admin.command(&[b"SET", b"x", b"value", b"PX", b"1"]),
        b"+OK\r\n"
    );
    std::thread::sleep(Duration::from_millis(10));
    assert_eq!(watcher.command(&[b"SELECT", b"1"]), b"+OK\r\n");
    assert_eq!(watcher.command(&[b"WATCH", b"x"]), b"+OK\r\n");
    assert_eq!(blocked.command(&[b"SELECT", b"1"]), b"+OK\r\n");
    blocked.send(&[b"BLPOP", b"x", b"0"]);
    _wait_for_blocked_client(&mut admin);

    assert_eq!(admin.command(&[b"SWAPDB", b"0", b"1"]), b"+OK\r\n");
    assert_eq!(watcher.command(&[b"MULTI"]), b"+OK\r\n");
    assert_eq!(watcher.command(&[b"PING"]), b"+QUEUED\r\n");
    assert_eq!(watcher.command(&[b"EXEC"]), b"*1\r\n+PONG\r\n");
}
