use crate::common::{command_status_with_timeout, TestServer};
use std::path::PathBuf;
use std::time::{Duration, Instant};

#[test]
fn valkey_update_resetstat_clears_acl_counters_and_preserves_log() {
    let server = TestServer::start_default();
    let mut admin = server.connect();
    let mut restricted = server.connect();
    assert_eq!(
        admin.command(&[
            b"ACL",
            b"SETUSER",
            b"resetstat-user",
            b"on",
            b">secret",
            b"-@all",
            b"+get",
            b"+publish",
            b"+select",
            b"~allowed",
            b"&allowed",
            b"db=0",
        ]),
        b"+OK\r\n"
    );
    assert!(restricted
        .command(&[b"AUTH", b"resetstat-user", b"wrong"])
        .starts_with(b"-WRONGPASS"));
    assert_eq!(
        restricted.command(&[b"AUTH", b"resetstat-user", b"secret"]),
        b"+OK\r\n"
    );
    for command in [
        &[b"PING".as_slice()][..],
        &[b"GET".as_slice(), b"denied"][..],
        &[b"PUBLISH".as_slice(), b"denied", b"message"][..],
        &[b"SELECT".as_slice(), b"1"][..],
    ] {
        assert!(restricted.command(command).starts_with(b"-NOPERM"));
    }
    let counters = [
        "acl_access_denied_auth",
        "acl_access_denied_cmd",
        "acl_access_denied_key",
        "acl_access_denied_channel",
        "acl_access_denied_db",
    ];
    let stats = admin.command_str(&[b"INFO", b"stats"]);
    for counter in counters {
        assert!(stats.contains(&format!("{counter}:1\r\n")), "{stats}");
    }
    let log = acl_log_without_ages(admin.command(&[b"ACL", b"LOG"]));
    assert_eq!(admin.command(&[b"CONFIG", b"RESETSTAT"]), b"+OK\r\n");
    let stats = admin.command_str(&[b"INFO", b"stats"]);
    for counter in counters {
        assert!(stats.contains(&format!("{counter}:0\r\n")), "{stats}");
    }
    assert_eq!(acl_log_without_ages(admin.command(&[b"ACL", b"LOG"])), log);
    assert!(restricted.command(&[b"PING"]).starts_with(b"-NOPERM"));
    let stats = admin.command_str(&[b"INFO", b"stats"]);
    assert!(stats.contains("acl_access_denied_cmd:1\r\n"), "{stats}");
}

fn acl_log_without_ages(reply: Vec<u8>) -> String {
    let reply = String::from_utf8(reply).expect("UTF-8 ACL log");
    let marker = "$11\r\nage-seconds\r\n";
    let mut entries = reply.split(marker);
    let mut normalized = entries.next().expect("ACL reply").to_string();
    for entry in entries {
        normalized.push_str(marker);
        normalized.push_str("$1\r\n0\r\n");
        normalized.push_str(entry.splitn(3, "\r\n").nth(2).expect("bulk age value"));
    }
    normalized
}

#[test]
fn acl_setuser_state_modifiers_are_case_insensitive() {
    let server = TestServer::start_default();
    let mut admin = server.connect();

    assert_eq!(
        admin.command(&[b"ACL", b"SETUSER", b"case-user", b"ON"]),
        b"+OK\r\n"
    );
    assert_eq!(
        admin.command(&[b"ACL", b"SETUSER", b"case-user", b"OFF"]),
        b"+OK\r\n"
    );
}

#[test]
fn client_kill_user_default_closes_other_default_clients() {
    let server = TestServer::start_default();
    let mut admin = server.connect();
    let mut target = server.connect();

    assert_eq!(
        admin.command(&[b"CLIENT", b"KILL", b"USER", b"default", b"SKIPME", b"no",]),
        b":2\r\n"
    );
    assert!(target.is_closed());
}

#[test]
fn disabled_default_user_can_kill_existing_default_clients_before_reauth() {
    let server = TestServer::start_default();
    let mut admin = server.connect();
    let mut target = server.connect();

    assert_eq!(
        admin.command(&[
            b"ACL",
            b"SETUSER",
            b"testuser",
            b"on",
            b">secret",
            b"allchannels",
            b"+@all",
        ]),
        b"+OK\r\n"
    );
    assert_eq!(
        admin.command(&[b"ACL", b"SETUSER", b"default", b"off"]),
        b"+OK\r\n"
    );
    assert_eq!(
        admin.command(&[b"CLIENT", b"KILL", b"USER", b"default", b"SKIPME", b"no",]),
        b":2\r\n"
    );
    assert!(target.is_closed());
}

#[test]
fn client_kill_self_preserves_replies_across_write_turns() {
    let server = TestServer::start_default();
    let mut admin = server.connect();
    assert_eq!(
        admin.command(&[b"ACL", b"SETUSER", b"default", b"off"]),
        b"+OK\r\n"
    );
    let mut pipeline = b"*1\r\n$4\r\nPING\r\n".to_vec();
    let ping = pipeline.clone();
    // More replies than one vectored write can consume.
    for _ in 1..512 {
        pipeline.extend_from_slice(&ping);
    }
    pipeline.extend_from_slice(
        b"*6\r\n$6\r\nCLIENT\r\n$4\r\nKILL\r\n$4\r\nUSER\r\n$7\r\ndefault\r\n$6\r\nSKIPME\r\n$2\r\nno\r\n",
    );
    admin.send_raw(&pipeline);
    for _ in 0..512 {
        assert_eq!(admin.read_reply(), b"+PONG\r\n");
    }
    assert_eq!(admin.read_reply(), b":1\r\n");
    assert!(admin.is_closed());
}

#[test]
fn disabled_default_user_keeps_new_connections_unauthenticated() {
    let server = TestServer::start_default();
    let mut admin = server.connect();

    assert_eq!(
        admin.command(&[b"ACL", b"SETUSER", b"default", b"off"]),
        b"+OK\r\n"
    );

    let mut client = server.connect();
    assert!(client.command(&[b"PING"]).starts_with(b"-NOAUTH"));
    assert!(client.command(&[b"PING"]).starts_with(b"-NOAUTH"));
}

#[test]
fn acl_empty_selector_creates_no_permission_selector() {
    let server = TestServer::start_default();
    let mut admin = server.connect();

    assert_eq!(
        admin.command(&[b"ACL", b"SETUSER", b"selector-default", b"reset", b"()"]),
        b"+OK\r\n"
    );
    let user = admin.command_str(&[b"ACL", b"GETUSER", b"selector-default"]);
    // One additional selector with no key/channel access and -@all commands.
    assert!(
        user.contains("-@all"),
        "empty selector must render as -@all: {user:?}"
    );
    assert!(
        user.contains("selectors"),
        "GETUSER must expose a selectors field: {user:?}"
    );
}

#[test]
fn acl_pubsub_default_applies_to_new_nested_selectors() {
    let server = TestServer::start_default();
    let mut admin = server.connect();
    assert_eq!(
        admin.command(&[b"CONFIG", b"SET", b"acl-pubsub-default", b"ALLCHANNELS"]),
        b"+OK\r\n"
    );
    assert_eq!(
        admin.command(&[
            b"ACL",
            b"SETUSER",
            b"selector-pubsub-default",
            b"on",
            b"nopass",
            b"resetchannels",
            b"(+@all ~*)",
        ]),
        b"+OK\r\n"
    );
    let user = admin.command_str(&[b"ACL", b"GETUSER", b"selector-pubsub-default"]);
    assert!(
        user.contains("&*"),
        "nested selector must inherit allchannels: {user:?}"
    );
}

#[test]
fn invalid_startup_acl_pubsub_default_aborts_server() {
    let config = std::env::temp_dir().join(format!(
        "versal-invalid-acl-pubsub-{}-{}.conf",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    std::fs::write(&config, "acl-pubsub-default invalid\n").expect("write config");
    let status = command_status_with_timeout(
        std::process::Command::new(env!("CARGO_BIN_EXE_versal-server"))
            .arg(&config)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null()),
        Duration::from_secs(5),
    )
    .expect("start server with invalid config")
    .expect("invalid ACL Pub/Sub default was accepted and kept running");
    assert!(
        !status.success(),
        "invalid ACL Pub/Sub default must abort startup"
    );
    let _ = std::fs::remove_file(config);
}

#[test]
fn acl_invalid_read_write_key_rules_error_without_modifying_user() {
    let server = TestServer::start_default();
    let mut admin = server.connect();

    // Regression tests for CVE-2024-51741: %~ and % are syntax errors.
    assert_eq!(
        admin.command_str(&[b"ACL", b"SETUSER", b"invalid", b"%~"]),
        "-ERR Error in ACL SETUSER modifier '%~': Syntax error\r\n"
    );
    assert_eq!(
        admin.command_str(&[b"ACL", b"SETUSER", b"invalid", b"%"]),
        "-ERR Error in ACL SETUSER modifier '%': Syntax error\r\n"
    );
}

#[test]
fn acl_invalid_selector_is_atomic_and_reports_context() {
    let server = TestServer::start_default();
    let mut admin = server.connect();

    // Invalid inner rule reports the selector context and must not create the user.
    assert!(admin
        .command_str(&[
            b"ACL",
            b"SETUSER",
            b"selector-syntax",
            b"on",
            b"(this-is-invalid)"
        ])
        .starts_with("-ERR Error in ACL SETUSER modifier '("));
    // Unmatched parenthesis is reported distinctly.
    assert!(admin
        .command_str(&[
            b"ACL",
            b"SETUSER",
            b"selector-syntax",
            b"on",
            b"(+PING (+SELECT (+DEL",
        ])
        .starts_with("-ERR Unmatched parenthesis in acl selector"));
    // The user must not have been created by any failed attempt.
    assert_eq!(
        admin.command_str(&[b"ACL", b"GETUSER", b"selector-syntax"]),
        "$-1\r\n"
    );
}

#[test]
fn acl_selectors_do_not_combine_command_and_key_permissions() {
    let server = TestServer::start_default();
    let mut admin = server.connect();
    let mut client = server.connect();

    assert_eq!(
        admin.command(&[
            b"ACL",
            b"SETUSER",
            b"selector-user",
            b"on",
            b"nopass",
            b"(+get ~read::*)",
            b"(+set ~write::*)",
        ]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"AUTH", b"selector-user", b"ignored"]),
        b"+OK\r\n"
    );

    assert!(client
        .command(&[b"GET", b"write::key"])
        .starts_with(b"-NOPERM"));
    assert!(client
        .command(&[b"SET", b"read::key", b"value"])
        .starts_with(b"-NOPERM"));

    assert_eq!(
        admin.command(&[
            b"ACL",
            b"SETUSER",
            b"selector-user",
            b"reset",
            b"on",
            b"nopass",
            b"(+mget ~first::*)",
            b"(+mget ~second::*)",
        ]),
        b"+OK\r\n"
    );
    assert!(client
        .command(&[b"MGET", b"first::key", b"second::key"])
        .starts_with(b"-NOPERM"));
}

#[test]
fn acl_transactions_do_not_combine_selector_key_permissions() {
    let server = TestServer::start_default();
    let mut admin = server.connect();
    let mut client = server.connect();

    assert_eq!(
        admin.command(&[
            b"ACL",
            b"SETUSER",
            b"transaction-selector-user",
            b"on",
            b"nopass",
            b"(+multi +exec)",
            b"(+mget ~first::*)",
            b"(+mget ~second::*)",
        ]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"AUTH", b"transaction-selector-user", b"ignored"]),
        b"+OK\r\n"
    );
    assert_eq!(client.command(&[b"MULTI"]), b"+OK\r\n");

    assert!(client
        .command(&[b"MGET", b"first::key", b"second::key"])
        .starts_with(b"-NOPERM"));
    assert!(client.command(&[b"EXEC"]).starts_with(b"-EXECABORT"));
}

#[test]
fn acl_scripts_do_not_combine_selector_key_permissions() {
    let server = TestServer::start_default();
    let mut admin = server.connect();
    let mut client = server.connect();

    assert_eq!(
        admin.command(&[
            b"ACL",
            b"SETUSER",
            b"script-selector-user",
            b"on",
            b"nopass",
            b"(+eval)",
            b"(+mget ~first::*)",
            b"(+mget ~second::*)",
        ]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"AUTH", b"script-selector-user", b"ignored"]),
        b"+OK\r\n"
    );

    let reply = client.command(&[
        b"EVAL",
        b"return redis.call('MGET', 'first::key', 'second::key')",
        b"0",
    ]);
    assert!(reply.starts_with(b"-ERR ACL failure in script"));
    assert!(reply
        .windows(b"No permissions to access a key".len())
        .any(|w| w == b"No permissions to access a key"));
}

#[test]
fn acl_transaction_uses_each_queued_command_database() {
    let server = TestServer::start_default();
    let mut admin = server.connect();
    let mut client = server.connect();

    // Writes allowed only in DB 0/1, reads allowed only in DB 2/3.
    assert_eq!(
        admin.command(&[
            b"ACL",
            b"SETUSER",
            b"db-tx-user",
            b"on",
            b"nopass",
            b"(db=0,1 +@all -@read ~*)",
            b"(db=2,3 +@all -@write ~*)",
        ]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"AUTH", b"db-tx-user", b"ignored"]),
        b"+OK\r\n"
    );

    assert_eq!(client.command(&[b"SELECT", b"0"]), b"+OK\r\n");
    assert_eq!(client.command(&[b"MULTI"]), b"+OK\r\n");
    // Each queued command must be authorized against the DB selected by the
    // preceding queued SELECT, not the connection DB at MULTI start.
    assert_eq!(
        client.command(&[b"SET", b"key1", b"value1"]),
        b"+QUEUED\r\n"
    );
    assert_eq!(client.command(&[b"SELECT", b"2"]), b"+QUEUED\r\n");
    assert_eq!(client.command(&[b"GET", b"key2"]), b"+QUEUED\r\n");
    assert_eq!(client.command(&[b"SELECT", b"0"]), b"+QUEUED\r\n");
    assert_eq!(
        client.command(&[b"SET", b"key3", b"value3"]),
        b"+QUEUED\r\n"
    );

    let reply = client.command_str(&[b"EXEC"]);
    assert!(
        reply.starts_with("*5\r\n"),
        "EXEC must run all 5 queued commands: {reply:?}"
    );
    assert!(
        !reply.contains("NOPERM"),
        "no command should be denied when queued against the correct DB: {reply:?}"
    );
}

#[test]
fn acl_rejection_when_woken_from_blpop_is_counted_once() {
    let server = TestServer::start_default();
    let mut admin = server.connect();
    let mut blocked = server.connect();

    assert_eq!(
        admin.command(&[b"ACL", b"SETUSER", b"blocked", b"on", b"nopass", b"+@all", b"allkeys"]),
        b"+OK\r\n"
    );
    assert_eq!(admin.command(&[b"CONFIG", b"RESETSTAT"]), b"+OK\r\n");
    assert_eq!(
        blocked.command(&[b"AUTH", b"blocked", b"ignored"]),
        b"+OK\r\n"
    );

    blocked.send(&[b"BLPOP", b"blocked-list", b"0"]);
    wait_for_blocked_client(&mut admin);

    assert_eq!(
        admin.command(&[b"ACL", b"SETUSER", b"blocked", b"resetkeys"]),
        b"+OK\r\n"
    );
    assert_eq!(
        admin.command(&[b"LPUSH", b"blocked-list", b"value"]),
        b":1\r\n"
    );
    assert!(String::from_utf8_lossy(&blocked.read_reply())
        .starts_with("-NOPERM No permissions to access a key"),);

    let stats = admin.command_str(&[b"INFO", b"commandstats"]);
    assert_eq!(
        command_stat(&stats, "blpop", "rejected_calls"),
        Some(1),
        "BLPOP must be counted once when its wakeup ACL recheck rejects it: {stats}"
    );
}

#[test]
fn acl_database_denials_are_logged_with_database_reason() {
    let server = TestServer::start_default();
    let mut admin = server.connect();
    let mut dbuser = server.connect();

    assert_eq!(admin.command(&[b"ACL", b"LOG", b"RESET"]), b"+OK\r\n");
    assert_eq!(
        admin.command(&[
            b"ACL", b"SETUSER", b"dbuser", b"on", b"nopass", b"db=0", b"+@all", b"allkeys",
        ]),
        b"+OK\r\n"
    );
    assert_eq!(
        dbuser.command(&[b"AUTH", b"dbuser", b"ignored"]),
        b"+OK\r\n"
    );

    assert!(dbuser.command(&[b"SELECT", b"1"]).starts_with(b"-NOPERM"));
    assert!(dbuser
        .command(&[b"SWAPDB", b"0", b"2"])
        .starts_with(b"-NOPERM"));
    assert!(dbuser.command(&[b"FLUSHALL"]).starts_with(b"-NOPERM"));

    let log = admin.command_str(&[b"ACL", b"LOG"]);
    let expected_entries = [
        ("flushall", "database"),
        ("2", "database"),
        ("1", "database"),
    ];
    let mut entries = log.split("*20\r\n").skip(1);
    for (object, reason) in expected_entries {
        let entry = entries
            .next()
            .expect("ACL LOG must contain each denied command");
        assert!(
            entry.contains(&format!(
                "${}\r\nreason\r\n${}\r\n{reason}\r\n",
                6,
                reason.len()
            )),
            "expected database denial reason for {object}: {entry:?}"
        );
        assert!(
            entry.contains(&format!(
                "${}\r\nobject\r\n${}\r\n{object}\r\n",
                6,
                object.len()
            )),
            "expected denied database object {object}: {entry:?}"
        );
    }
}

#[test]
fn acllog_max_len_config_truncates_future_acl_denials() {
    let server = TestServer::start_default();
    let mut admin = server.connect();
    let mut restricted = server.connect();
    assert_eq!(admin.command(&[b"ACL", b"LOG", b"RESET"]), b"+OK\r\n");
    assert_eq!(
        admin.command(&[b"CONFIG", b"SET", b"acllog-max-len", b"2"]),
        b"+OK\r\n"
    );
    assert_eq!(
        admin.command(&[
            b"ACL",
            b"SETUSER",
            b"acllog-cap",
            b"on",
            b"nopass",
            b"-@all",
        ]),
        b"+OK\r\n"
    );
    assert_eq!(
        restricted.command(&[b"AUTH", b"acllog-cap", b"unused"]),
        b"+OK\r\n"
    );
    assert!(restricted.command(&[b"PING"]).starts_with(b"-NOPERM"));
    assert!(restricted
        .command(&[b"GET", b"first"])
        .starts_with(b"-NOPERM"));
    assert!(restricted
        .command(&[b"SET", b"second", b"value"])
        .starts_with(b"-NOPERM"));

    let log = admin.command_str(&[b"ACL", b"LOG"]);
    assert!(
        log.starts_with("*2\r\n"),
        "acllog-max-len must retain two entries: {log:?}"
    );
    assert!(
        log.contains("$3\r\nset\r\n"),
        "latest SET denial missing: {log:?}"
    );
    assert!(
        log.contains("$3\r\nget\r\n"),
        "second GET denial missing: {log:?}"
    );

    assert_eq!(
        admin.command(&[b"CONFIG", b"SET", b"acllog-max-len", b"0"]),
        b"+OK\r\n"
    );
    assert!(restricted
        .command(&[b"GET", b"first"])
        .starts_with(b"-NOPERM"));
    assert_eq!(admin.command(&[b"ACL", b"LOG"]), b"*0\r\n");
}

#[test]
fn startup_acllog_max_len_config_applies_to_acl_log_owner() {
    let config = std::env::temp_dir().join(format!(
        "versal-acllog-cap-{}-{}.conf",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    std::fs::write(&config, "acllog-max-len 0\n").expect("write config");
    let server = TestServer::start(&[config.to_str().expect("UTF-8 config path")]);
    let mut admin = server.connect();
    let mut restricted = server.connect();
    assert_eq!(
        admin.command(&[
            b"ACL",
            b"SETUSER",
            b"startup-acllog",
            b"on",
            b"nopass",
            b"-@all"
        ]),
        b"+OK\r\n"
    );
    assert_eq!(
        restricted.command(&[b"AUTH", b"startup-acllog", b"unused"]),
        b"+OK\r\n"
    );
    assert!(restricted.command(&[b"PING"]).starts_with(b"-NOPERM"));
    assert_eq!(admin.command(&[b"ACL", b"LOG"]), b"*0\r\n");
    drop(server);
    let _ = std::fs::remove_file(config);
}

#[test]
fn invalid_startup_acllog_max_len_aborts_server() {
    let config = std::env::temp_dir().join(format!(
        "versal-invalid-acllog-cap-{}-{}.conf",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    std::fs::write(&config, "acllog-max-len -1\n").expect("write config");
    let status = command_status_with_timeout(
        std::process::Command::new(env!("CARGO_BIN_EXE_versal-server"))
            .arg(&config)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null()),
        Duration::from_secs(5),
    )
    .expect("start server with invalid config")
    .expect("invalid acllog cap was accepted and kept running");
    assert!(!status.success(), "invalid acllog cap must abort startup");
    let _ = std::fs::remove_file(config);
}

#[test]
fn config_rewrite_persists_acl_user_rules_across_restart() {
    let data_dir = unique_test_dir("acl-config-rewrite");
    let config_path = data_dir.join("valkey.conf");
    std::fs::write(&config_path, "user default on nopass ~* +@all -flushdb\n")
        .expect("write test config");
    let config_path = config_path.to_string_lossy().into_owned();

    let mut server = TestServer::start_in_dir(data_dir, &[&config_path]);
    let mut client = server.connect();
    assert!(client.command(&[b"FLUSHDB"]).starts_with(b"-NOPERM"));
    assert_eq!(
        client.command(&[b"CONFIG", b"REWRITE"]),
        b"+OK\r\n",
        "CONFIG REWRITE must succeed"
    );
    let rewritten = std::fs::read_to_string(&config_path).expect("read rewritten config");
    let user_rules = rewritten
        .lines()
        .find(|line| line.starts_with("user default "))
        .expect("CONFIG REWRITE must persist the default user");
    assert_eq!(
        user_rules.split_whitespace().collect::<Vec<_>>(),
        [
            "user",
            "default",
            "on",
            "nopass",
            "~*",
            "resetchannels",
            "+@all",
            "-flushdb",
        ],
        "CONFIG REWRITE must persist the complete ACL user rule: {rewritten}"
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
        client.command(&[b"FLUSHDB"]).starts_with(b"-NOPERM"),
        "ACL command denial must survive CONFIG REWRITE and restart"
    );
}

#[test]
fn acl_getuser_keys_preserve_order_and_merge_rw() {
    let server = TestServer::start_default();
    let mut admin = server.connect();

    // Key patterns must render in insertion order with %R~/%W~/~ prefixes.
    assert_eq!(
        admin.command(&[
            b"ACL",
            b"SETUSER",
            b"selector-info",
            b"-@all",
            b"+get",
            b"resetchannels",
            b"&channel1",
            b"%R~foo1",
            b"%W~bar1",
            b"~baz1",
        ]),
        b"+OK\r\n"
    );
    let user = admin.command_str(&[b"ACL", b"GETUSER", b"selector-info"]);
    assert!(
        user.contains("%R~foo1 %W~bar1 ~baz1"),
        "keys must preserve insertion order: {user:?}"
    );

    // %R~foo + %W~foo collapses to a single ~foo (read+write == all).
    assert_eq!(
        admin.command(&[
            b"ACL",
            b"SETUSER",
            b"selector-rw-info",
            b"%R~foo",
            b"%W~foo",
            b"%RW~bar",
        ]),
        b"+OK\r\n"
    );
    let user = admin.command_str(&[b"ACL", b"GETUSER", b"selector-rw-info"]);
    assert!(
        user.contains("~foo ~bar") && !user.contains("%R~foo") && !user.contains("%W~foo"),
        "%R~foo + %W~foo must normalize to ~foo: {user:?}"
    );
}

#[test]
fn acl_getuser_databases_render_alldbs_resetdbs_and_db_list() {
    let server = TestServer::start_default();
    let mut admin = server.connect();

    admin.command(&[
        b"ACL",
        b"SETUSER",
        b"db-list",
        b"-@all",
        b"+get",
        b"~*",
        b"db=0,1,2",
    ]);
    let user = admin.command_str(&[b"ACL", b"GETUSER", b"db-list"]);
    assert!(user.contains("db=0,1,2"), "explicit db list: {user:?}");

    admin.command(&[
        b"ACL", b"SETUSER", b"db-all", b"-@all", b"+get", b"~*", b"alldbs",
    ]);
    let user = admin.command_str(&[b"ACL", b"GETUSER", b"db-all"]);
    assert!(user.contains("alldbs"), "alldbs render: {user:?}");

    admin.command(&[
        b"ACL",
        b"SETUSER",
        b"db-reset",
        b"-@all",
        b"+get",
        b"~*",
        b"resetdbs",
    ]);
    let user = admin.command_str(&[b"ACL", b"GETUSER", b"db-reset"]);
    // resetdbs renders as an empty databases field.
    assert!(
        !user.contains("alldbs") && !user.contains("db="),
        "resetdbs must render empty databases: {user:?}"
    );
}

#[test]
fn acl_list_serializes_selectors_and_dbs() {
    let server = TestServer::start_default();
    let mut admin = server.connect();

    assert_eq!(
        admin.command(&[
            b"ACL",
            b"SETUSER",
            b"user-idempotency",
            b"off",
            b"-@all",
            b"+get",
            b"resetchannels",
            b"&channel1",
            b"%R~foo1",
            b"%W~bar1",
            b"~baz1",
            b"(-@all +set resetchannels &channel2 %R~foo2 %W~bar2 ~baz2)",
        ]),
        b"+OK\r\n"
    );
    let list = admin.command_str(&[b"ACL", b"LIST"]);
    let line = list
        .lines()
        .find(|l| l.contains("user-idempotency"))
        .unwrap_or("")
        .to_string();
    assert!(line.contains("-@all"), "root -@all missing: {line:?}");
    assert!(line.contains("&channel1"), "root channel missing: {line:?}");
    assert!(
        line.contains("%R~foo1") && line.contains("%W~bar1") && line.contains("~baz1"),
        "root keys missing/misordered: {line:?}"
    );
    assert!(
        line.contains('('),
        "additional selector must be serialized: {line:?}"
    );
    assert!(
        line.contains("&channel2") && line.contains("%R~foo2"),
        "additional selector contents missing: {line:?}"
    );

    // db list appears in ACL LIST; implicit alldbs is omitted.
    admin.command(&[
        b"ACL", b"SETUSER", b"list-db", b"on", b"nopass", b"-@all", b"+get", b"~*", b"&*",
        b"alldbs",
    ]);
    let list = admin.command_str(&[b"ACL", b"LIST"]);
    let line = list
        .lines()
        .find(|l| l.contains("list-db"))
        .unwrap_or("")
        .to_string();
    assert!(
        !line.contains("alldbs"),
        "implicit alldbs must be omitted: {line:?}"
    );

    admin.command(&[b"ACL", b"SETUSER", b"list-db", b"db=0,1"]);
    let list = admin.command_str(&[b"ACL", b"LIST"]);
    let line = list
        .lines()
        .find(|l| l.contains("list-db"))
        .unwrap_or("")
        .to_string();
    assert!(
        line.contains("db=0,1"),
        "explicit db list must appear: {line:?}"
    );
}

#[test]
fn acl_separate_read_write_key_permissions_are_enforced() {
    let server = TestServer::start_default();
    let mut admin = server.connect();
    let mut client = server.connect();

    // %R~read* grants read only; writes to matching keys and any access to
    // non-matching keys must be denied.
    assert_eq!(
        admin.command(&[
            b"ACL",
            b"SETUSER",
            b"key-permission-R",
            b"on",
            b"nopass",
            b"%R~read*",
            b"+@all"
        ]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"AUTH", b"key-permission-R", b"x"]),
        b"+OK\r\n"
    );
    admin.command(&[b"SET", b"readstr", b"bar"]);
    assert_eq!(client.command(&[b"GET", b"readstr"]), b"$3\r\nbar\r\n");
    assert!(
        client
            .command(&[b"SET", b"readstr", b"bar"])
            .starts_with(b"-NOPERM"),
        "write to read-only key must be denied"
    );
    assert!(
        client
            .command(&[b"GET", b"notread"])
            .starts_with(b"-NOPERM"),
        "access to non-matching key must be denied"
    );
}

#[test]
fn acl_file_loads_parenthesized_selectors_at_startup() {
    let data_dir = unique_test_dir("acl-file-selectors");
    std::fs::write(
        data_dir.join("users.acl"),
        "user alice on (+get ~rw*)\nuser bob on (+set %W~w*) (+get %R~r*)\n",
    )
    .expect("write acl file");
    let config_path = data_dir.join("valkey.conf");
    std::fs::write(&config_path, "aclfile users.acl\n").expect("write config");
    let config_path = config_path.to_string_lossy().into_owned();

    let server = TestServer::start_in_dir(data_dir, &[&config_path]);
    let mut admin = server.connect();

    let alice = admin.command_str(&[b"ACL", b"GETUSER", b"alice"]);
    assert!(
        alice.contains("~rw*") && alice.contains("+get"),
        "alice's parenthesized selector must load: {alice:?}"
    );
    let bob = admin.command_str(&[b"ACL", b"GETUSER", b"bob"]);
    assert!(
        bob.contains("%W~w*") && bob.contains("%R~r*"),
        "bob's two selectors must load with read/write patterns: {bob:?}"
    );
}

#[test]
fn acl_dryrun_rejects_unknown_command_and_bad_arity() {
    let server = TestServer::start_default();
    let mut admin = server.connect();

    assert_eq!(
        admin.command(&[b"ACL", b"SETUSER", b"dry", b"on", b"nopass", b"+@all", b"~v*"]),
        b"+OK\r\n"
    );

    // Unknown command must be reported as a top-level error, not "OK".
    let unknown = admin.command_str(&[b"ACL", b"DRYRUN", b"dry", b"not-a-command", b"read"]);
    assert!(
        unknown.starts_with("-ERR") && unknown.contains("not-a-command"),
        "unknown command must error: {unknown:?}"
    );

    // Unknown user still errors.
    let no_user = admin.command_str(&[b"ACL", b"DRYRUN", b"not-a-user", b"GET", b"v"]);
    assert!(
        no_user.starts_with("-ERR") && no_user.contains("not-a-user"),
        "unknown user must error: {no_user:?}"
    );

    // Wrong arity for the target command must error with the command name.
    let bad_arity = admin.command_str(&[b"ACL", b"DRYRUN", b"dry", b"SET", b"v"]);
    assert!(
        bad_arity.starts_with("-ERR")
            && bad_arity.contains("wrong number of arguments")
            && bad_arity.contains("'set'"),
        "wrong arity must error: {bad_arity:?}"
    );

    // Valid arity is authorized.
    assert_eq!(
        admin.command_str(&[b"ACL", b"DRYRUN", b"dry", b"SET", b"v", b"v"]),
        "$2\r\nOK\r\n"
    );
}

#[test]
fn acl_dryrun_honors_dynamic_keys() {
    let server = TestServer::start_default();
    let mut admin = server.connect();

    assert_eq!(
        admin.command(&[
            b"ACL",
            b"SETUSER",
            b"cmd",
            b"+@all",
            b"%R~read*",
            b"%W~write*",
            b"%RW~rw*",
        ]),
        b"+OK\r\n"
    );

    let dry = |admin: &mut crate::common::Client, extra: &[&[u8]]| -> String {
        let mut parts: Vec<&[u8]> = vec![b"ACL", b"DRYRUN", b"cmd"];
        parts.extend_from_slice(extra);
        admin.command_str(&parts)
    };

    // MIGRATE static key (RW required): only rw* keys pass.
    assert_eq!(
        dry(&mut admin, &[b"MIGRATE", b"h", b"p", b"rw", b"0", b"500"]),
        "$2\r\nOK\r\n"
    );
    assert!(
        dry(&mut admin, &[b"MIGRATE", b"h", b"p", b"read", b"0", b"500"]).contains("'read' key")
    );

    // MIGRATE ... KEYS dynamic keys.
    assert_eq!(
        dry(
            &mut admin,
            &[b"MIGRATE", b"h", b"p", b"", b"0", b"5000", b"KEYS", b"rw"]
        ),
        "$2\r\nOK\r\n"
    );
    assert!(dry(
        &mut admin,
        &[b"MIGRATE", b"h", b"p", b"", b"0", b"5000", b"KEYS", b"read"]
    )
    .contains("'read' key"));

    // SORT ... STORE: source is read, STORE target is write.
    assert_eq!(
        dry(&mut admin, &[b"SORT", b"read", b"STORE", b"write"]),
        "$2\r\nOK\r\n"
    );
    assert!(dry(&mut admin, &[b"SORT", b"read", b"STORE", b"read"]).contains("'read' key"));
    assert!(dry(&mut admin, &[b"SORT", b"write", b"STORE", b"write"]).contains("'write' key"));

    // SINTERCARD numkeys dynamic keys (read access).
    assert_eq!(
        dry(&mut admin, &[b"SINTERCARD", b"2", b"read", b"read"]),
        "$2\r\nOK\r\n"
    );
    assert!(dry(&mut admin, &[b"SINTERCARD", b"2", b"write", b"read"]).contains("'write' key"));

    // MEMORY USAGE dynamic key (read access).
    assert_eq!(
        dry(&mut admin, &[b"MEMORY", b"USAGE", b"read"]),
        "$2\r\nOK\r\n"
    );
    assert!(dry(&mut admin, &[b"MEMORY", b"USAGE", b"nothing"]).contains("'nothing' key"));

    // EVAL keys are RW ("assume the worst"): a read-only pattern is denied,
    // while EVAL_RO keys only require read access.
    assert_eq!(
        dry(&mut admin, &[b"EVAL", b"", b"1", b"rw1"]),
        "$2\r\nOK\r\n"
    );
    assert!(dry(&mut admin, &[b"EVAL", b"", b"1", b"read"]).contains("'read' key"));
    assert_eq!(
        dry(&mut admin, &[b"EVAL_RO", b"", b"1", b"rw1"]),
        "$2\r\nOK\r\n"
    );
    assert_eq!(
        dry(&mut admin, &[b"EVAL_RO", b"", b"1", b"read"]),
        "$2\r\nOK\r\n"
    );

    // GEORADIUS: source is read; STORE target requires write.
    assert_eq!(
        dry(
            &mut admin,
            &[
                b"GEORADIUS",
                b"read",
                b"0",
                b"0",
                b"1",
                b"m",
                b"STORE",
                b"write"
            ]
        ),
        "$2\r\nOK\r\n"
    );
    assert!(dry(
        &mut admin,
        &[
            b"GEORADIUS",
            b"read",
            b"0",
            b"0",
            b"1",
            b"m",
            b"STORE",
            b"read"
        ]
    )
    .contains("'read' key"));

    // XREAD STREAMS keys (read access), parsed past leading options.
    assert_eq!(
        dry(
            &mut admin,
            &[b"XREAD", b"COUNT", b"1", b"STREAMS", b"read", b"read2", b"0", b"0"]
        ),
        "$2\r\nOK\r\n"
    );
    assert!(dry(&mut admin, &[b"XREAD", b"STREAMS", b"write", b"0"]).contains("'write' key"));

    // LMPOP numkeys keys require RW (access|delete): read-only pattern denied.
    assert_eq!(
        dry(&mut admin, &[b"LMPOP", b"1", b"rw1", b"LEFT"]),
        "$2\r\nOK\r\n"
    );
    assert!(dry(&mut admin, &[b"LMPOP", b"1", b"read", b"LEFT"]).contains("'read' key"));

    // ZUNION numkeys keys (read access).
    assert_eq!(
        dry(&mut admin, &[b"ZUNION", b"2", b"read", b"read2"]),
        "$2\r\nOK\r\n"
    );
    assert!(dry(&mut admin, &[b"ZUNION", b"2", b"write", b"read"]).contains("'write' key"));
}

#[test]
fn acl_sort_by_get_require_full_keyspace_read() {
    let server = TestServer::start_default();
    let mut admin = server.connect();
    let mut client = server.connect();

    admin.command(&[b"SET", b"v1", b"1"]);
    admin.command(&[b"LPUSH", b"mylist", b"1"]);

    // (+sort ~mylist): SORT allowed, but BY/GET need full keyspace read access.
    assert_eq!(
        admin.command(&[
            b"ACL",
            b"SETUSER",
            b"sort-acl",
            b"on",
            b"nopass",
            b"(+sort ~mylist)"
        ]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"AUTH", b"sort-acl", b"nopass"]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command_str(&[b"SORT", b"mylist", b"BY", b"v*"]),
        "-ERR BY option of SORT denied due to insufficient ACL permissions.\r\n"
    );
    assert_eq!(
        client.command_str(&[b"SORT", b"mylist", b"GET", b"v*"]),
        "-ERR GET option of SORT denied due to insufficient ACL permissions.\r\n"
    );

    // A narrower matching pattern (~v*) is still not full keyspace access.
    assert_eq!(
        admin.command(&[b"ACL", b"SETUSER", b"sort-acl", b"(+sort ~mylist ~v*)"]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command_str(&[b"SORT", b"mylist", b"BY", b"v*"]),
        "-ERR BY option of SORT denied due to insufficient ACL permissions.\r\n"
    );

    // %W~* grants full keyspace WRITE, but BY/GET need READ.
    assert_eq!(
        admin.command(&[b"ACL", b"SETUSER", b"sort-acl", b"(+sort ~mylist %W~*)"]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command_str(&[b"SORT", b"mylist", b"GET", b"v*"]),
        "-ERR GET option of SORT denied due to insufficient ACL permissions.\r\n"
    );

    // %R~* grants full keyspace READ: BY is now allowed.
    assert_eq!(
        admin.command(&[b"ACL", b"SETUSER", b"sort-acl", b"(+sort ~mylist %R~*)"]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"SORT", b"mylist", b"BY", b"v*"]),
        b"*1\r\n$1\r\n1\r\n"
    );

    // A constant BY (no '*') needs no key access and is always allowed.
    assert_eq!(
        admin.command(&[
            b"ACL",
            b"SETUSER",
            b"sort-acl",
            b"reset",
            b"on",
            b"nopass",
            b"(+sort ~mylist)"
        ]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"SORT", b"mylist", b"BY", b"nosort"]),
        b"*1\r\n$1\r\n1\r\n"
    );
}

#[test]
fn acl_copy_enforces_per_key_read_and_write_permissions() {
    // COPY has two key specs: source is read-only (RO|ACCESS), destination is
    // write-only (OW|UPDATE). A user with %R~read* + %W~write* may copy read->write
    // but not write->read, mirroring acl-v2.tcl "Test separate read and write
    // permissions".
    let server = TestServer::start_default();
    let mut admin = server.connect();
    let mut client = server.connect();

    assert_eq!(
        admin.command(&[
            b"ACL",
            b"SETUSER",
            b"copy-rw",
            b"on",
            b"nopass",
            b"%R~read*",
            b"%W~write*",
            b"+@all",
        ]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"AUTH", b"copy-rw", b"nopass"]),
        b"+OK\r\n"
    );
    admin.command(&[b"SET", b"read", b"bar"]);

    // read (source, needs READ) -> write (dest, needs WRITE): allowed.
    assert_eq!(client.command(&[b"COPY", b"read", b"write"]), b":1\r\n");

    // write (source, needs READ but only has WRITE) -> read (dest, needs WRITE
    // but only has READ): denied on a key.
    assert!(client
        .command(&[b"COPY", b"write", b"read"])
        .starts_with(b"-NOPERM"));
}

#[test]
fn acl_existence_and_cardinality_commands_accept_read_or_write() {
    // Commands whose keyspec is `['RO']` without ACCESS (STRLEN, HLEN, ...,
    // HEXISTS, SISMEMBER, TOUCH, EXISTS, TYPE, MEMORY USAGE) are executable
    // when *either* READ or WRITE is granted on the matching key, but denied
    // for a non-matching key. Mirrors acl-v2.tcl "Existence test commands are
    // not marked as access" / "Cardinality commands require some type of
    // permission" / "general keyspace commands require some type".
    let server = TestServer::start_default();
    let mut admin = server.connect();

    assert_eq!(
        admin.command(&[
            b"ACL",
            b"SETUSER",
            b"cmd",
            b"+@all",
            b"%R~read*",
            b"%W~write*",
            b"%RW~rw*",
        ]),
        b"+OK\r\n"
    );

    let dry = |admin: &mut crate::common::Client, extra: &[&[u8]]| -> String {
        let mut parts: Vec<&[u8]> = vec![b"ACL", b"DRYRUN", b"cmd"];
        parts.extend_from_slice(extra);
        admin.command_str(&parts)
    };

    // Existence tests: one key, either R or W pattern accepted.
    for cmd in [b"HEXISTS".as_slice(), b"HSTRLEN", b"SISMEMBER"] {
        assert_eq!(dry(&mut admin, &[cmd, b"read", b"foo"]), "$2\r\nOK\r\n");
        assert_eq!(dry(&mut admin, &[cmd, b"write", b"foo"]), "$2\r\nOK\r\n");
        assert!(dry(&mut admin, &[cmd, b"nothing", b"foo"]).contains("'nothing' key"));
    }

    // General keyspace commands with a single key.
    {
        let cmd = b"TYPE".as_slice();
        assert_eq!(dry(&mut admin, &[cmd, b"read"]), "$2\r\nOK\r\n");
        assert_eq!(dry(&mut admin, &[cmd, b"write"]), "$2\r\nOK\r\n");
        assert_eq!(dry(&mut admin, &[cmd, b"rw"]), "$2\r\nOK\r\n");
        assert!(dry(&mut admin, &[cmd, b"nothing"]).contains("'nothing' key"));
    }

    // MEMORY USAGE (container command, key after subcommand).
    assert_eq!(
        dry(&mut admin, &[b"MEMORY", b"USAGE", b"read"]),
        "$2\r\nOK\r\n"
    );
    assert_eq!(
        dry(&mut admin, &[b"MEMORY", b"USAGE", b"write"]),
        "$2\r\nOK\r\n"
    );
    assert!(dry(&mut admin, &[b"MEMORY", b"USAGE", b"nothing"]).contains("'nothing' key"));

    // Variadic keyspace commands (TOUCH, EXISTS): each key accepts R or W.
    for cmd in [b"TOUCH".as_slice(), b"EXISTS"] {
        assert_eq!(dry(&mut admin, &[cmd, b"read"]), "$2\r\nOK\r\n");
        assert_eq!(dry(&mut admin, &[cmd, b"write"]), "$2\r\nOK\r\n");
        assert_eq!(dry(&mut admin, &[cmd, b"rw"]), "$2\r\nOK\r\n");
        assert!(dry(&mut admin, &[cmd, b"nothing"]).contains("'nothing' key"));
    }

    // Cardinality commands with a single key.
    for cmd in [
        b"STRLEN".as_slice(),
        b"HLEN",
        b"LLEN",
        b"SCARD",
        b"ZCARD",
        b"XLEN",
    ] {
        assert_eq!(dry(&mut admin, &[cmd, b"read"]), "$2\r\nOK\r\n");
        assert_eq!(dry(&mut admin, &[cmd, b"write"]), "$2\r\nOK\r\n");
        assert_eq!(dry(&mut admin, &[cmd, b"rw"]), "$2\r\nOK\r\n");
        assert!(dry(&mut admin, &[cmd, b"nothing"]).contains("'nothing' key"));
    }
}

#[test]
fn acl_set_and_bitfield_use_dynamic_key_permissions() {
    // SET's key is write-only unless the GET option is present (then RW);
    // BITFIELD's key is read-only when only GET subcommands are used, else RW.
    // Mirrors acl-v2.tcl "Test SET/BITFIELD with separate read/write permissions".
    let server = TestServer::start_default();
    let mut admin = server.connect();
    let mut client = server.connect();

    // Read-only user (%R~read*): may read but not write.
    assert_eq!(
        admin.command(&[
            b"ACL",
            b"SETUSER",
            b"kperm-r",
            b"on",
            b"nopass",
            b"%R~read*",
            b"+@all"
        ]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"AUTH", b"kperm-r", b"nopass"]),
        b"+OK\r\n"
    );
    // Plain SET (write-only) denied.
    assert!(client
        .command(&[b"SET", b"readstr", b"bar"])
        .starts_with(b"-NOPERM"));
    // SET ... GET needs RW; only READ granted -> denied.
    assert!(client
        .command(&[b"SET", b"readstr", b"bar", b"GET"])
        .starts_with(b"-NOPERM"));
    // BITFIELD GET (read-only) allowed.
    assert_eq!(
        client.command(&[b"BITFIELD", b"readstr", b"GET", b"u4", b"0"]),
        b"*1\r\n:0\r\n"
    );
    // BITFIELD SET (write) denied.
    assert!(client
        .command(&[b"BITFIELD", b"readstr", b"SET", b"u4", b"0", b"1"])
        .starts_with(b"-NOPERM"));

    // Write-only user (%W~write*): may write-only SET but not SET ... GET.
    let mut w = server.connect();
    assert_eq!(
        admin.command(&[
            b"ACL",
            b"SETUSER",
            b"kperm-w",
            b"on",
            b"nopass",
            b"%W~write*",
            b"+@all"
        ]),
        b"+OK\r\n"
    );
    assert_eq!(w.command(&[b"AUTH", b"kperm-w", b"nopass"]), b"+OK\r\n");
    assert_eq!(w.command(&[b"SET", b"writestr", b"bar"]), b"+OK\r\n");
    // SET ... GET needs READ too -> denied.
    assert!(w
        .command(&[b"SET", b"writestr", b"bar", b"GET"])
        .starts_with(b"-NOPERM"));
    // BITFIELD SET needs RW (read+write); write-only user is denied, as is GET.
    assert!(w
        .command(&[b"BITFIELD", b"writestr", b"SET", b"u4", b"0", b"1"])
        .starts_with(b"-NOPERM"));
    assert!(w
        .command(&[b"BITFIELD", b"writestr", b"GET", b"u4", b"0"])
        .starts_with(b"-NOPERM"));
}

#[test]
fn acl_move_checks_destination_database_permission() {
    let server = TestServer::start_default();
    let mut admin = server.connect();
    let mut client = server.connect();

    assert_eq!(
        admin.command(&[
            b"ACL",
            b"SETUSER",
            b"move-user",
            b"on",
            b"nopass",
            b"+move",
            b"+set",
            b"~*",
            b"db=0,1",
        ]),
        b"+OK\r\n"
    );
    assert_eq!(client.command(&[b"AUTH", b"move-user", b"x"]), b"+OK\r\n");
    assert_eq!(client.command(&[b"SET", b"move-key", b"v"]), b"+OK\r\n");
    assert_eq!(client.command(&[b"MOVE", b"move-key", b"1"]), b":1\r\n");

    let denied = client.command_str(&[b"MOVE", b"move-key", b"2"]);
    assert!(
        denied.starts_with("-NOPERM") && denied.contains("database"),
        "MOVE to a denied DB must be rejected: {denied:?}"
    );
}

#[test]
fn acl_copy_checks_every_db_clause_permission() {
    let server = TestServer::start_default();
    let mut admin = server.connect();
    let mut client = server.connect();

    assert_eq!(
        admin.command(&[
            b"ACL",
            b"SETUSER",
            b"copy-user",
            b"on",
            b"nopass",
            b"+copy",
            b"+set",
            b"+get",
            b"+del",
            b"+select",
            b"~*",
            b"db=0,1",
        ]),
        b"+OK\r\n"
    );
    assert_eq!(client.command(&[b"AUTH", b"copy-user", b"x"]), b"+OK\r\n");
    assert_eq!(client.command(&[b"SELECT", b"0"]), b"+OK\r\n");
    assert_eq!(client.command(&[b"SET", b"copy-key", b"v"]), b"+OK\r\n");
    assert_eq!(
        client.command(&[b"COPY", b"copy-key", b"copy-dest", b"DB", b"1"]),
        b":1\r\n"
    );

    // Any DB clause naming a denied database must be rejected, even when
    // another clause names an allowed one (COPY keeps the last DB, but the
    // ACL must validate every occurrence).
    for extra in [
        &[b"DB".as_ref(), b"2".as_ref()][..],
        &[b"DB".as_ref(), b"2".as_ref(), b"REPLACE".as_ref()][..],
        &[b"DB".as_ref(), b"1".as_ref(), b"DB".as_ref(), b"2".as_ref()][..],
        &[b"DB".as_ref(), b"2".as_ref(), b"DB".as_ref(), b"1".as_ref()][..],
    ] {
        let mut cmd: Vec<&[u8]> = vec![b"COPY", b"copy-key", b"copy-dest2"];
        cmd.extend_from_slice(extra);
        let denied = client.command_str(&cmd);
        assert!(
            denied.starts_with("-NOPERM") && denied.contains("database"),
            "COPY with denied DB clause {extra:?} must be rejected: {denied:?}"
        );
    }
}

#[test]
fn acl_duplicate_db_ids_are_deduplicated_and_sorted() {
    let server = TestServer::start_default();
    let mut admin = server.connect();

    assert_eq!(
        admin.command(&[
            b"ACL",
            b"SETUSER",
            b"db-dup",
            b"on",
            b"nopass",
            b"+@all",
            b"~*",
            b"db=0,0,1,1,0",
        ]),
        b"+OK\r\n"
    );
    let user = admin.command_str(&[b"ACL", b"GETUSER", b"db-dup"]);
    assert!(
        user.contains("db=0,1") && !user.contains("db=0,0"),
        "duplicate db ids must render as db=0,1: {user:?}"
    );
    let list = admin.command_str(&[b"ACL", b"LIST"]);
    assert!(
        list.contains("db=0,1"),
        "ACL LIST must show deduplicated db list: {list:?}"
    );
}

#[test]
fn acl_dryrun_reports_denied_database() {
    let server = TestServer::start_default();
    let mut admin = server.connect();

    assert_eq!(
        admin.command(&[
            b"ACL",
            b"SETUSER",
            b"db-dryrun",
            b"on",
            b"nopass",
            b"+@all",
            b"~*",
            b"db=0,1",
        ]),
        b"+OK\r\n"
    );
    assert_eq!(
        admin.command(&[b"ACL", b"DRYRUN", b"db-dryrun", b"SELECT", b"0"]),
        b"$2\r\nOK\r\n"
    );
    for cmd in [
        &[b"SELECT".as_ref(), b"2".as_ref()][..],
        &[b"MOVE".as_ref(), b"key".as_ref(), b"2".as_ref()][..],
        &[b"SWAPDB".as_ref(), b"0".as_ref(), b"2".as_ref()][..],
        &[
            b"COPY".as_ref(),
            b"k1".as_ref(),
            b"k2".as_ref(),
            b"DB".as_ref(),
            b"2".as_ref(),
        ][..],
    ] {
        let mut full: Vec<&[u8]> = vec![b"ACL", b"DRYRUN", b"db-dryrun"];
        full.extend_from_slice(cmd);
        let reply = admin.command_str(&full);
        assert!(
            reply.contains("has no permissions to access database 2"),
            "DRYRUN {cmd:?} must report denied database 2: {reply:?}"
        );
    }
    // Allowed COPY DB clause must pass.
    assert_eq!(
        admin.command(&[
            b"ACL",
            b"DRYRUN",
            b"db-dryrun",
            b"COPY",
            b"k1",
            b"k2",
            b"DB",
            b"1"
        ]),
        b"$2\r\nOK\r\n"
    );
}

#[test]
fn acl_db_max_id_is_out_of_range() {
    // Default 16 databases: valid ids are 0..15. db=15 is the maximum valid id,
    // and SELECT to db 16 must fail with an out-of-range error (not a
    // permission error), mirroring C SELECT range validation.
    let server = TestServer::start_default();
    let mut admin = server.connect();
    let mut client = server.connect();

    assert_eq!(
        admin
            .command(&[b"ACL", b"SETUSER", b"db-max", b"on", b"nopass", b"+@all", b"~*", b"db=15"]),
        b"+OK\r\n"
    );
    assert_eq!(client.command(&[b"AUTH", b"db-max", b"x"]), b"+OK\r\n");
    assert_eq!(client.command(&[b"SELECT", b"15"]), b"+OK\r\n");

    let reply = client.command_str(&[b"SELECT", b"16"]);
    assert!(
        reply.to_lowercase().contains("out of range"),
        "SELECT beyond the max db must be out of range, not a permission error: {reply:?}"
    );
}

#[test]
fn acl_keyless_commands_bypass_database_restriction() {
    let server = TestServer::start_default();
    let mut admin = server.connect();
    let mut client = server.connect();

    assert_eq!(
        admin.command(&[
            b"ACL",
            b"SETUSER",
            b"db-keyless",
            b"on",
            b"nopass",
            b"db=15",
            b"+@all",
            b"~*",
        ]),
        b"+OK\r\n"
    );
    assert_eq!(client.command(&[b"AUTH", b"db-keyless", b"x"]), b"+OK\r\n");

    // Commands that don't touch keys/metadata must work regardless of db=.
    assert_eq!(client.command(&[b"PING"]), b"+PONG\r\n");
    assert!(client.command_str(&[b"INFO", b"server"]).starts_with("$"));

    // Commands that touch keys/metadata in the current db must be denied.
    let denied = client.command_str(&[b"GET", b"key"]);
    assert!(
        denied.starts_with("-NOPERM") && denied.contains("database"),
        "GET in a denied db must fail with a database error: {denied:?}"
    );
    assert!(client.command_str(&[b"SELECT", b"2"]).contains("database"));
}

fn wait_for_blocked_client(admin: &mut crate::common::Client) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let clients = admin.command_str(&[b"INFO", b"clients"]);
        if clients.contains("blocked_clients:1") {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("BLPOP client did not become blocked");
}

fn unique_test_dir(prefix: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "valkey-rust-{prefix}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before Unix epoch")
            .as_nanos()
    ));
    std::fs::create_dir(&dir).expect("create test directory");
    dir
}

fn command_stat(stats: &str, command: &str, field: &str) -> Option<u64> {
    let prefix = format!("cmdstat_{command}:");
    stats.lines().find_map(|line| {
        line.strip_prefix(&prefix).and_then(|fields| {
            fields.split(',').find_map(|entry| {
                entry
                    .strip_prefix(&format!("{field}="))
                    .and_then(|value| value.parse().ok())
            })
        })
    })
}
