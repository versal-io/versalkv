//! Black-box CONFIG compatibility coverage for C-aligned runtime settings.

#[cfg(unix)]
use crate::common::make_socket_test_dir;
use crate::common::{command_status_with_timeout, TestServer};
#[cfg(feature = "tls")]
use openssl::asn1::Asn1Time;
#[cfg(feature = "tls")]
use openssl::hash::MessageDigest;
#[cfg(feature = "tls")]
use openssl::pkey::PKey;
#[cfg(feature = "tls")]
use openssl::rsa::Rsa;
#[cfg(feature = "tls")]
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
#[cfg(feature = "tls")]
use openssl::x509::extension::{BasicConstraints, SubjectAlternativeName};
#[cfg(feature = "tls")]
use openssl::x509::{X509NameBuilder, X509};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use std::os::unix::net::{UnixDatagram, UnixStream};
#[cfg(feature = "tls")]
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use versal::foundation::io::sys::process::{send_signal, ProcessSignal};

fn assert_config_value(client: &mut crate::common::Client, name: &[u8], value: &str) {
    let reply = client.command_str(&[b"CONFIG", b"GET", name]);
    assert!(
        reply.contains(value),
        "CONFIG GET {} should contain {value:?}, got {reply:?}",
        String::from_utf8_lossy(name)
    );
}

fn free_tcp_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[cfg(feature = "tls")]
fn write_tls_credentials(stem: &str) -> (PathBuf, PathBuf) {
    let rsa = Rsa::generate(2048).unwrap();
    let private_key = PKey::from_rsa(rsa).unwrap();
    let mut subject = X509NameBuilder::new().unwrap();
    subject.append_entry_by_text("CN", "localhost").unwrap();
    let subject = subject.build();
    let mut certificate = X509::builder().unwrap();
    certificate.set_version(2).unwrap();
    certificate.set_subject_name(&subject).unwrap();
    certificate.set_issuer_name(&subject).unwrap();
    certificate.set_pubkey(&private_key).unwrap();
    certificate
        .append_extension(BasicConstraints::new().critical().ca().build().unwrap())
        .unwrap();
    let context = certificate.x509v3_context(None, None);
    certificate
        .append_extension(
            SubjectAlternativeName::new()
                .dns("localhost")
                .ip("127.0.0.1")
                .build(&context)
                .unwrap(),
        )
        .unwrap();
    certificate
        .set_not_before(&Asn1Time::days_from_now(0).unwrap())
        .unwrap();
    certificate
        .set_not_after(&Asn1Time::days_from_now(1).unwrap())
        .unwrap();
    certificate
        .sign(&private_key, MessageDigest::sha256())
        .unwrap();
    let certificate = certificate.build();
    let cert_path = std::env::temp_dir().join(format!("{stem}.crt"));
    let key_path = std::env::temp_dir().join(format!("{stem}.key"));
    std::fs::write(&cert_path, certificate.to_pem().unwrap()).unwrap();
    std::fs::write(&key_path, private_key.private_key_to_pem_pkcs8().unwrap()).unwrap();
    (cert_path, key_path)
}

#[test]
fn config_runtime_settings_round_trip_and_startup_only_backlog_is_rejected() {
    let server = TestServer::start_default();
    let mut client = server.connect();

    for (name, value) in [
        (b"tcp-keepalive" as &[u8], b"17" as &[u8]),
        (b"prefetch-batch-max-size", b"2"),
        (b"ignore-warnings", b"ARM64-COW-BUG"),
        (b"active-expire-effort", b"7"),
        (b"activedefrag", b"yes"),
        (b"active-defrag-cycle-min", b"2"),
        (b"active-defrag-cycle-max", b"50"),
        (b"active-defrag-threshold-lower", b"1"),
        (b"active-defrag-threshold-upper", b"200"),
        (b"active-defrag-cycle-us", b"1000"),
        (b"active-defrag-ignore-bytes", b"2"),
        (b"active-defrag-max-scan-fields", b"2"),
        (b"max-new-connections-per-cycle", b"25"),
        (b"timeout", b"19"),
        (b"replica-announce-port", b"16379"),
        (b"slave-ignore-maxmemory", b"no"),
        (b"repl-disable-tcp-nodelay", b"yes"),
        (b"cluster-allow-pubsubshard-when-down", b"no"),
        (b"lazyfree-lazy-user-del", b"no"),
        (b"lazyfree-lazy-user-flush", b"no"),
        (b"stream-node-max-bytes", b"128"),
        (b"slot-migration-max-failover-repl-bytes", b"-1"),
        (b"cluster-blacklist-ttl", b"7"),
        (b"rdb-save-incremental-fsync", b"no"),
        (b"rdb-del-sync-files", b"yes"),
        (b"replica-ignore-disk-write-errors", b"yes"),
        (b"slave-lazy-flush", b"no"),
        (b"aof-rewrite-incremental-fsync", b"no"),
    ] {
        assert_eq!(
            client.command(&[b"CONFIG", b"SET", name, value]),
            b"+OK\r\n"
        );
        assert_config_value(&mut client, name, std::str::from_utf8(value).unwrap());
    }

    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"client-query-buffer-limit", b"10m"]),
        b"+OK\r\n"
    );
    assert_config_value(&mut client, b"client-query-buffer-limit", "10485760");
    assert!(client
        .command_str(&[b"CONFIG", b"SET", b"client-query-buffer-limit", b"1023kb",])
        .contains("out of range"));

    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"tcp-backlog", b"127"]),
        b"-ERR CONFIG SET failed (possibly related to argument 'tcp-backlog') - can't set immutable config\r\n"
    );
    assert!(client
        .command_str(&[b"CONFIG", b"SET", b"tls-port", b"6380"])
        .contains("Unable to update TLS configuration"));
    let tls_cluster_reply = client.command_str(&[b"CONFIG", b"SET", b"tls-cluster", b"yes"]);
    #[cfg(feature = "tls")]
    assert!(tls_cluster_reply.contains("Unable to update TLS configuration"));
    #[cfg(not(feature = "tls"))]
    assert!(tls_cluster_reply.contains("TLS support is not compiled in"));
    assert_config_value(&mut client, b"tls-cluster", "no");
}

#[test]
fn hash_listpack_limits_apply_to_runtime_hset_encoding() {
    let server = TestServer::start_default();
    let mut client = server.connect();

    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"hash-max-listpack-entries", b"1",]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"hash-max-listpack-value", b"64",]),
        b"+OK\r\n"
    );

    assert_eq!(
        client.command(&[b"HSET", b"hash", b"first", b"value"]),
        b":1\r\n"
    );
    assert!(client
        .command_str(&[b"OBJECT", b"ENCODING", b"hash"])
        .contains("listpack"));

    assert_eq!(
        client.command(&[b"HSET", b"hash", b"second", b"value"]),
        b":1\r\n"
    );
    assert!(client
        .command_str(&[b"OBJECT", b"ENCODING", b"hash"])
        .contains("hashtable"));
}

#[test]
fn zset_listpack_limits_apply_to_runtime_zadd_encoding() {
    let server = TestServer::start_default();
    let mut client = server.connect();

    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"zset-max-listpack-entries", b"1",]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"zset-max-listpack-value", b"64",]),
        b"+OK\r\n"
    );

    assert_eq!(
        client.command(&[b"ZADD", b"zset", b"1", b"first"]),
        b":1\r\n"
    );
    assert!(client
        .command_str(&[b"OBJECT", b"ENCODING", b"zset"])
        .contains("listpack"));

    assert_eq!(
        client.command(&[b"ZADD", b"zset", b"2", b"second"]),
        b":1\r\n"
    );
    assert!(client
        .command_str(&[b"OBJECT", b"ENCODING", b"zset"])
        .contains("btree"));
}

#[test]
fn zset_pop_commands_keep_listpack_encoding() {
    let server = TestServer::start_default();
    let mut client = server.connect();

    assert_eq!(
        client.command(&[b"ZADD", b"min", b"1", b"a", b"2", b"b"]),
        b":2\r\n"
    );
    assert!(client
        .command_str(&[b"OBJECT", b"ENCODING", b"min"])
        .contains("listpack"));
    assert_eq!(
        client.command(&[b"ZPOPMIN", b"min", b"1"]),
        b"*2\r\n$1\r\na\r\n$1\r\n1\r\n"
    );
    assert!(client
        .command_str(&[b"OBJECT", b"ENCODING", b"min"])
        .contains("listpack"));

    assert_eq!(
        client.command(&[b"ZADD", b"max", b"1", b"a", b"2", b"b"]),
        b":2\r\n"
    );
    assert!(client
        .command_str(&[b"OBJECT", b"ENCODING", b"max"])
        .contains("listpack"));
    assert_eq!(
        client.command(&[b"ZPOPMAX", b"max", b"1"]),
        b"*2\r\n$1\r\nb\r\n$1\r\n2\r\n"
    );
    assert!(client
        .command_str(&[b"OBJECT", b"ENCODING", b"max"])
        .contains("listpack"));
}

#[test]
fn zset_count_commands_keep_listpack_encoding() {
    let server = TestServer::start_default();
    let mut client = server.connect();

    assert_eq!(
        client.command(&[b"ZADD", b"zset", b"1", b"a", b"2", b"b", b"3", b"c"]),
        b":3\r\n"
    );
    assert_eq!(client.command(&[b"ZCOUNT", b"zset", b"1", b"2"]), b":2\r\n");
    assert_eq!(
        client.command(&[b"ZLEXCOUNT", b"zset", b"-", b"+"]),
        b":3\r\n"
    );
    assert!(client
        .command_str(&[b"OBJECT", b"ENCODING", b"zset"])
        .contains("listpack"));
}

#[test]
fn zset_random_and_scan_reads_keep_listpack_encoding() {
    let server = TestServer::start_default();
    let mut client = server.connect();

    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"zset-max-listpack-entries", b"128"]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"ZADD", b"zset", b"1", b"a", b"2", b"b"]),
        b":2\r\n"
    );
    assert!(client
        .command_str(&[b"ZRANDMEMBER", b"zset", b"1", b"WITHSCORES"])
        .contains("*2"));
    let scan = client.command_str(&[b"ZSCAN", b"zset", b"0"]);
    assert!(scan.contains("a") && scan.contains("b"), "{scan}");
    assert!(client
        .command_str(&[b"OBJECT", b"ENCODING", b"zset"])
        .contains("listpack"));
}

#[test]
fn hide_user_data_from_log_defaults_to_redaction_and_applies_live() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    assert_config_value(&mut client, b"hide-user-data-from-log", "yes");

    let mut malformed =
        TcpStream::connect(("127.0.0.1", server.port)).expect("connect malformed protocol client");
    malformed
        .write_all(b"*invalid\r\ndefault-secret\r\n")
        .expect("write malformed request");
    let mut reply = [0_u8; 256];
    let _ = malformed.read(&mut reply);
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while std::time::Instant::now() < deadline
        && !server.server_log().contains("Query buffer: redacted")
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    let default_log = server.server_log();
    assert!(
        default_log.contains("Query buffer: redacted"),
        "{default_log}"
    );
    assert!(!default_log.contains("default-secret"), "{default_log}");

    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"hide-user-data-from-log", b"no"]),
        b"+OK\r\n"
    );
    let mut visible = TcpStream::connect(("127.0.0.1", server.port))
        .expect("connect second malformed protocol client");
    visible
        .write_all(b"*invalid\r\nvisible-secret\r\n")
        .expect("write second malformed request");
    let _ = visible.read(&mut reply);
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while std::time::Instant::now() < deadline && !server.server_log().contains("visible-secret") {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(server.server_log().contains("visible-secret"));
}

#[cfg(target_os = "linux")]
#[test]
fn oom_score_config_updates_and_restores_the_server_process_value() {
    let server = TestServer::start_default();
    let pid = server.pid();
    let oom_path = format!("/proc/{pid}/oom_score_adj");
    let original: i32 = std::fs::read_to_string(&oom_path)
        .expect("read server oom score")
        .trim()
        .parse()
        .expect("parse server oom score");
    let mut client = server.connect();

    assert_eq!(
        client.command(&[
            b"CONFIG",
            b"SET",
            b"oom-score-adj-values",
            b"100",
            b"200",
            b"300",
            b"latency-monitor-threshold",
            b"1",
        ]),
        b"+OK\r\n"
    );
    assert_config_value(&mut client, b"latency-monitor-threshold", "1");
    let absolute_reply = client.command(&[b"CONFIG", b"SET", b"oom-score-adj", b"absolute"]);
    if absolute_reply != b"+OK\r\n" {
        assert_eq!(
            absolute_reply,
            b"-ERR Failed to set current oom_score_adj. Check server logs.\r\n"
        );
        assert_config_value(&mut client, b"oom-score-adj", "no");
        assert_eq!(
            std::fs::read_to_string(&oom_path)
                .expect("read unchanged server oom score")
                .trim(),
            original.to_string()
        );
        assert_eq!(
            client.command(&[b"CONFIG", b"SET", b"oom-score-adj-values", b"11 22 33",]),
            b"+OK\r\n"
        );
        assert_config_value(&mut client, b"oom-score-adj-values", "11 22 33");
        return;
    }
    assert_config_value(&mut client, b"oom-score-adj", "absolute");
    assert_eq!(
        std::fs::read_to_string(&oom_path)
            .expect("read updated server oom score")
            .trim(),
        "100"
    );

    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"oom-score-adj", b"relative"]),
        b"+OK\r\n"
    );
    assert_config_value(&mut client, b"oom-score-adj", "yes");
    assert_eq!(
        std::fs::read_to_string(&oom_path)
            .expect("read relative server oom score")
            .trim(),
        &(original.saturating_add(100).clamp(-1000, 1000)).to_string()
    );

    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"oom-score-adj", b"no"]),
        b"+OK\r\n"
    );
    assert_eq!(
        std::fs::read_to_string(&oom_path)
            .expect("read restored server oom score")
            .trim(),
        original.to_string()
    );

    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"oom-score-adj-values", b"11 22 33",]),
        b"+OK\r\n"
    );
    assert_config_value(&mut client, b"oom-score-adj-values", "11 22 33");
}

#[test]
fn hz_config_clamps_to_c_bounds_and_info_reports_the_live_value() {
    let server = TestServer::start_default();
    let mut client = server.connect();

    for (requested, expected) in [(b"0" as &[u8], "1"), (b"501", "500")] {
        assert_eq!(
            client.command(&[b"CONFIG", b"SET", b"hz", requested]),
            b"+OK\r\n"
        );
        assert_config_value(&mut client, b"hz", expected);
        let info = client.command_str(&[b"INFO", b"server"]);
        assert!(info.contains(&format!("hz:{expected}\r\n")), "{info}");
        assert!(
            info.contains(&format!("configured_hz:{expected}\r\n")),
            "{info}"
        );
    }
}

#[test]
fn watchdog_period_clamps_to_two_live_cron_ticks() {
    let server = TestServer::start_default();
    let mut client = server.connect();

    // C's updateWatchdogPeriod keeps a watchdog interval at or above two cron
    // ticks. Set hz explicitly so this does not depend on a startup default.
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"hz", b"10"]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"watchdog-period", b"1"]),
        b"+OK\r\n"
    );
    assert_config_value(&mut client, b"watchdog-period", "200");

    // The configured timer must remain armed while the command blocks, emit
    // its SIGALRM diagnostic, and leave the process usable afterwards.
    assert_eq!(client.command(&[b"DEBUG", b"SLEEP", b"0.25"]), b"+OK\r\n");
    assert!(
        server.server_log().contains("WATCHDOG TIMER EXPIRED"),
        "watchdog diagnostic missing: {}",
        server.server_log()
    );
    assert_eq!(client.command(&[b"PING"]), b"+PONG\r\n");
}

#[test]
fn rdb_version_check_relaxed_loads_a_valid_future_version_snapshot() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "versal-rdb-version-check-{}-{unique}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create RDB test directory");
    let dir_arg = dir.to_str().expect("UTF-8 RDB test directory").to_string();
    {
        let server = TestServer::start(&["--dir", &dir_arg]);
        let mut client = server.connect();
        assert_eq!(client.command(&[b"SET", b"key", b"value"]), b"+OK\r\n");
        assert_eq!(client.command(&[b"SAVE"]), b"+OK\r\n");
    }

    let rdb_path = dir.join("dump.rdb");
    let mut rdb = std::fs::read(&rdb_path).expect("read saved RDB");
    rdb[6..9].copy_from_slice(b"999");
    let crc_offset = rdb.len() - 8;
    let crc = versal::foundation::io::persistence::crc64(0, &rdb[..crc_offset]);
    rdb[crc_offset..].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(&rdb_path, rdb).expect("write future-version RDB");
    let config = dir.join("valkey.conf");
    std::fs::write(
        &config,
        format!("dir {}\nrdb-version-check relaxed\n", dir.display()),
    )
    .expect("write relaxed RDB config");
    let config_arg = config.to_str().expect("UTF-8 config path").to_string();

    {
        let server = TestServer::start(&[&config_arg]);
        let mut client = server.connect();
        assert_config_value(&mut client, b"rdb-version-check", "relaxed");
        assert_eq!(client.command(&[b"GET", b"key"]), b"$5\r\nvalue\r\n");
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn dir_and_dbfilename_config_set_redirect_the_next_rdb_save() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "versal-rdb-config-path-{}-{unique}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create configured RDB directory");
    let dir_arg = dir.to_str().expect("UTF-8 RDB directory").to_string();
    let server = TestServer::start_default();
    let mut client = server.connect();
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"dir", dir_arg.as_bytes()]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"dbfilename", b"configured.rdb"]),
        b"+OK\r\n"
    );
    assert_eq!(client.command(&[b"SET", b"key", b"value"]), b"+OK\r\n");
    assert_eq!(client.command(&[b"SAVE"]), b"+OK\r\n");
    assert!(dir.join("configured.rdb").is_file());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn databases_is_startup_only_and_sizes_the_runtime_database_set() {
    let server = TestServer::start(&["--databases", "2"]);
    let mut client = server.connect();
    assert_eq!(client.command(&[b"SELECT", b"1"]), b"+OK\r\n");
    assert!(client
        .command_str(&[b"SELECT", b"2"])
        .contains("out of range"));
    assert!(client
        .command_str(&[b"CONFIG", b"SET", b"databases", b"3"])
        .contains("immutable config"));
}

#[test]
fn c_immutable_startup_configs_reject_runtime_mutation() {
    let server = TestServer::start_default();
    let mut client = server.connect();

    for (name, value) in [
        (b"aclfile" as &[u8], b"other.acl" as &[u8]),
        (b"unixsocket", b"/tmp/versal-runtime.sock"),
        (b"unixsocketgroup", b"staff"),
        (b"unixsocketperm", b"700"),
        (b"hash-seed", b"fixed-seed"),
    ] {
        assert!(
            client
                .command_str(&[b"CONFIG", b"SET", name, value])
                .contains("immutable config"),
            "{name:?} must reject CONFIG SET"
        );
    }
}

#[cfg(unix)]
#[test]
fn unix_socket_startup_serves_resp_with_configured_permissions() {
    let dir = make_socket_test_dir();
    let socket = dir.join("server.sock");
    let config = dir.join("versal.conf");
    std::fs::write(
        &config,
        format!("unixsocket {}\nunixsocketperm 700\n", socket.display()),
    )
    .expect("write unix socket config");
    let config_arg = config.to_string_lossy().into_owned();

    let server = TestServer::start(&[&config_arg]);
    let mut stream = UnixStream::connect(&socket).expect("connect configured Unix socket");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set Unix socket timeout");
    stream
        .write_all(b"*1\r\n$4\r\nPING\r\n")
        .expect("send PING");
    let mut reply = [0_u8; 7];
    stream.read_exact(&mut reply).expect("read PONG");
    assert_eq!(&reply, b"+PONG\r\n");
    assert_eq!(
        std::fs::metadata(&socket)
            .expect("stat Unix socket")
            .permissions()
            .mode()
            & 0o777,
        0o700
    );

    drop(server);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn import_mode_preserves_expired_keys_for_import_sources() {
    let server = TestServer::start_default();
    let mut source = server.connect();
    assert!(source
        .command_str(&[b"CLIENT", b"IMPORT-SOURCE", b"ON"])
        .contains("Server is not in import mode"));

    assert_eq!(
        source.command(&[b"CONFIG", b"SET", b"import-mode", b"yes"]),
        b"+OK\r\n"
    );
    assert_eq!(
        source.command(&[b"CLIENT", b"IMPORT-SOURCE", b"ON"]),
        b"+OK\r\n"
    );
    assert_eq!(
        source.command(&[b"SET", b"expired-import", b"value", b"PX", b"1"]),
        b"+OK\r\n"
    );
    std::thread::sleep(Duration::from_millis(20));

    let mut ordinary = server.connect();
    assert_eq!(ordinary.command(&[b"GET", b"expired-import"]), b"$-1\r\n");
    assert_eq!(
        source.command(&[b"GET", b"expired-import"]),
        b"$5\r\nvalue\r\n"
    );
    assert_eq!(ordinary.command(&[b"DBSIZE"]), b":1\r\n");

    assert_eq!(
        source.command(&[b"CLIENT", b"IMPORT-SOURCE", b"OFF"]),
        b"+OK\r\n"
    );
    assert_eq!(
        source.command(&[b"CONFIG", b"SET", b"import-mode", b"no"]),
        b"+OK\r\n"
    );
    assert_eq!(ordinary.command(&[b"GET", b"expired-import"]), b"$-1\r\n");
    assert_eq!(ordinary.command(&[b"DBSIZE"]), b":0\r\n");
}

#[test]
fn notify_keyspace_events_controls_live_keyevent_delivery() {
    let server = TestServer::start_default();
    let mut control = server.connect();
    let mut subscriber =
        TcpStream::connect(("127.0.0.1", server.port)).expect("connect subscriber");
    subscriber
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set subscriber timeout");
    subscriber
        .write_all(b"*2\r\n$10\r\nPSUBSCRIBE\r\n$16\r\n__keyevent@0__:*\r\n")
        .expect("subscribe keyevents");
    let mut response = [0_u8; 512];
    let count = subscriber
        .read(&mut response)
        .expect("read subscribe acknowledgement");
    assert!(
        String::from_utf8_lossy(&response[..count]).contains("psubscribe"),
        "unexpected subscribe response: {:?}",
        String::from_utf8_lossy(&response[..count])
    );

    assert_eq!(
        control.command(&[b"CONFIG", b"SET", b"notify-keyspace-events", b"E$"]),
        b"+OK\r\n"
    );
    assert_eq!(
        control.command(&[b"SET", b"notification-key", b"value"]),
        b"+OK\r\n"
    );
    let count = subscriber
        .read(&mut response)
        .expect("read keyevent notification");
    let notification = String::from_utf8_lossy(&response[..count]);
    assert!(
        notification.contains("__keyevent@0__:set"),
        "{notification:?}"
    );
    assert!(
        notification.contains("notification-key"),
        "{notification:?}"
    );

    assert_eq!(
        control.command(&[b"CONFIG", b"SET", b"notify-keyspace-events", b""]),
        b"+OK\r\n"
    );
    assert_eq!(
        control.command(&[b"SET", b"suppressed-key", b"value"]),
        b"+OK\r\n"
    );
    subscriber
        .set_read_timeout(Some(Duration::from_millis(150)))
        .expect("short subscriber timeout");
    match subscriber.read(&mut response) {
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
            ) => {}
        Ok(count) => panic!(
            "received notification after disabling config: {:?}",
            String::from_utf8_lossy(&response[..count])
        ),
        Err(error) => panic!("read disabled notification: {error}"),
    }
}

#[test]
fn notify_keyspace_events_emits_keymiss_for_a_missing_get() {
    let server = TestServer::start_default();
    let mut control = server.connect();
    let mut subscriber =
        TcpStream::connect(("127.0.0.1", server.port)).expect("connect subscriber");
    subscriber
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set subscriber timeout");
    subscriber
        .write_all(b"*2\r\n$10\r\nPSUBSCRIBE\r\n$16\r\n__keyevent@0__:*\r\n")
        .expect("subscribe keyevents");
    let mut response = [0_u8; 512];
    let count = subscriber
        .read(&mut response)
        .expect("read subscribe acknowledgement");
    assert!(String::from_utf8_lossy(&response[..count]).contains("psubscribe"));

    assert_eq!(
        control.command(&[b"CONFIG", b"SET", b"notify-keyspace-events", b"Em"]),
        b"+OK\r\n"
    );
    assert_eq!(control.command(&[b"GET", b"missing-key"]), b"$-1\r\n");

    let count = subscriber
        .read(&mut response)
        .expect("read keymiss notification");
    let notification = String::from_utf8_lossy(&response[..count]);
    assert!(
        notification.contains("__keyevent@0__:keymiss"),
        "{notification:?}"
    );
    assert!(notification.contains("missing-key"), "{notification:?}");
}

#[test]
fn stream_node_max_entries_controls_approximate_xadd_trimming() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"stream-node-max-entries", b"1"]),
        b"+OK\r\n"
    );

    for value in [b"one" as &[u8], b"two", b"three"] {
        assert!(client
            .command_str(&[b"XADD", b"stream", b"MAXLEN", b"~", b"2", b"*", b"field", value,])
            .starts_with('$'));
    }
    assert_eq!(client.command(&[b"XLEN", b"stream"]), b":2\r\n");
}

#[test]
fn port_config_set_rebinds_the_live_tcp_listener() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    let port = free_tcp_port();
    let port_arg = port.to_string();
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"port", port_arg.as_bytes()]),
        b"+OK\r\n"
    );
    assert_config_value(&mut client, b"port", &port_arg);
    let mut reconnected = crate::common::Client::connect_to(port);
    assert_eq!(reconnected.command(&[b"PING"]), b"+PONG\r\n");
}

#[test]
fn maxmemory_clients_config_evicts_an_oversized_reply_client() {
    let server = TestServer::start_default();
    let mut control = server.connect();
    let large_value = vec![b'x'; 256 * 1024];
    assert_eq!(
        control.command(&[b"SET", b"large", &large_value]),
        b"+OK\r\n"
    );
    assert_eq!(
        control.command(&[b"CONFIG", b"SET", b"maxmemory-clients", b"128kb"]),
        b"+OK\r\n"
    );
    let mut data = TcpStream::connect(("127.0.0.1", server.port)).expect("connect data client");
    data.write_all(b"*2\r\n$3\r\nGET\r\n$5\r\nlarge\r\n")
        .expect("request large reply");

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let stats = control.command_str(&[b"INFO", b"stats"]);
        let evicted = stats
            .lines()
            .find_map(|line| line.strip_prefix("evicted_clients:")?.parse::<u64>().ok())
            .unwrap_or(0);
        if evicted >= 1 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "maxmemory-clients did not evict the oversized reply client: {stats}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn min_replicas_configs_gate_writes_without_an_eligible_replica() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"min-replicas-max-lag", b"1"]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"min-replicas-to-write", b"1"]),
        b"+OK\r\n"
    );
    assert!(client
        .command_str(&[b"SET", b"key", b"value"])
        .starts_with("-NOREPLICAS"));
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"min-replicas-to-write", b"0"]),
        b"+OK\r\n"
    );
    assert_eq!(client.command(&[b"SET", b"key", b"value"]), b"+OK\r\n");
}

#[test]
fn bind_source_addr_applies_to_replica_upstream_connections() {
    let primary = TestServer::start_default();
    let replica = TestServer::start_default();
    let mut primary_client = primary.connect();
    let mut replica_client = replica.connect();
    // Linux assigns the entire 127/8 loopback range; Darwin only assigns
    // 127.0.0.1 by default, so binding 127.0.0.2 there fails with EADDRNOTAVAIL.
    let source_addr = if cfg!(target_os = "macos") {
        "127.0.0.1"
    } else {
        "127.0.0.2"
    };
    assert_eq!(
        replica_client.command(&[
            b"CONFIG",
            b"SET",
            b"bind-source-addr",
            source_addr.as_bytes(),
        ]),
        b"+OK\r\n"
    );
    assert_config_value(&mut replica_client, b"bind-source-addr", source_addr);
    let primary_port = primary.port.to_string();
    assert_eq!(
        replica_client.command(&[b"REPLICAOF", b"127.0.0.1", primary_port.as_bytes()]),
        b"+OK\r\n"
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let clients = primary_client.command_str(&[b"CLIENT", b"LIST"]);
        if clients.lines().any(|line| {
            line.split_whitespace()
                .any(|field| field.starts_with(&format!("addr={source_addr}:")))
                && line.split_whitespace().any(|field| {
                    field
                        .strip_prefix("flags=")
                        .is_some_and(|flags| flags.contains('S'))
                })
        }) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "primary did not observe the configured replica source address: {clients}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn extended_redis_compatibility_changes_live_identity_replies() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"extended-redis-compatibility", b"yes"]),
        b"+OK\r\n"
    );
    assert!(client.command_str(&[b"HELLO", b"3"]).contains("redis"));
    assert_eq!(client.command(&[b"LOLWUT"]), b"$16\r\nRedis ver. 7.2.0\r\n");
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"extended-redis-compatibility", b"no"]),
        b"+OK\r\n"
    );
    assert!(client.command_str(&[b"HELLO", b"3"]).contains("valkey"));
}

#[test]
fn requirepass_config_set_synchronizes_default_acl_authentication() {
    let server = TestServer::start_default();
    let mut control = server.connect();
    assert_eq!(
        control.command(&[b"CONFIG", b"SET", b"requirepass", b"secret"]),
        b"+OK\r\n"
    );
    let mut client = crate::common::Client::connect_to(server.port);
    assert!(client.command(&[b"PING"]).starts_with(b"-NOAUTH"));
    assert!(client
        .command(&[b"AUTH", b"default", b"wrong"])
        .starts_with(b"-WRONGPASS"));
    assert_eq!(
        client.command(&[b"AUTH", b"default", b"secret"]),
        b"+OK\r\n"
    );
    assert_eq!(
        control.command(&[b"CONFIG", b"SET", b"requirepass", b""]),
        b"+OK\r\n"
    );
}

#[test]
fn commandlog_large_categories_default_to_one_mebibyte_and_record_live_traffic() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    assert_config_value(&mut client, b"commandlog-request-larger-than", "1048576");
    assert_config_value(&mut client, b"commandlog-reply-larger-than", "1048576");
    for name in [
        b"commandlog-request-larger-than" as &[u8],
        b"commandlog-reply-larger-than",
    ] {
        let info = client.command_str(&[b"CONFIG", b"INFO", name]);
        assert!(
            info.contains("-1") && info.contains("9223372036854775807"),
            "CONFIG INFO must expose C's signed memory threshold range for {}: {info:?}",
            String::from_utf8_lossy(name)
        );
    }

    let payload = vec![b'x'; 1024 * 1024];
    assert_eq!(
        client.command(&[b"SET", b"commandlog-large-payload", &payload]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"COMMANDLOG", b"LEN", b"large-request"]),
        b":1\r\n"
    );
    assert_eq!(
        client.command(&[b"GET", b"commandlog-large-payload"]),
        [
            format!("${}\r\n", payload.len()).into_bytes(),
            payload.clone(),
            b"\r\n".to_vec(),
        ]
        .concat()
    );
    assert_eq!(
        client.command(&[b"COMMANDLOG", b"LEN", b"large-reply"]),
        b":1\r\n"
    );
}

#[test]
fn invalid_commandlog_config_file_aborts_server_startup() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    let config = std::env::temp_dir().join(format!(
        "versal-invalid-commandlog-{}-{unique}.conf",
        std::process::id()
    ));
    std::fs::write(&config, "commandlog-request-larger-than -2\n").expect("write invalid config");
    let status = command_status_with_timeout(
        Command::new(env!("CARGO_BIN_EXE_versal-server"))
            .arg(&config)
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
        Duration::from_secs(5),
    )
    .expect("start server with invalid commandlog config")
    .expect("invalid commandlog config was accepted and kept running");

    assert!(
        !status.success(),
        "invalid commandlog config must abort startup"
    );
    let _ = std::fs::remove_file(config);
}

#[test]
fn startup_rejects_aof_file_or_directory_paths() {
    for (setting, value) in [
        ("appendfilename", "../outside.aof"),
        ("appenddirname", "aof/subdir"),
    ] {
        let config = std::env::temp_dir().join(format!(
            "versal-invalid-{setting}-{}-{}.conf",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        ));
        std::fs::write(&config, format!("{setting} {value}\n")).expect("write config");
        let status = command_status_with_timeout(
            Command::new(env!("CARGO_BIN_EXE_versal-server"))
                .arg(&config)
                .stdout(Stdio::null())
                .stderr(Stdio::null()),
            Duration::from_secs(5),
        )
        .expect("start server with invalid AOF path")
        .expect("invalid AOF path was accepted and kept running");
        assert!(!status.success(), "{setting}={value:?} must abort startup");
        let _ = std::fs::remove_file(config);
    }
}

#[test]
fn latency_monitor_threshold_accepts_only_nonnegative_integer_milliseconds() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    assert!(client
        .command_str(&[b"CONFIG", b"SET", b"latency-monitor-threshold", b"-1"])
        .contains("out of range"));
    assert!(client
        .command_str(&[b"CONFIG", b"SET", b"latency-monitor-threshold", b"1k"])
        .contains("must be numeric"));
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"latency-monitor-threshold", b"7"]),
        b"+OK\r\n"
    );
    assert_config_value(&mut client, b"latency-monitor-threshold", "7");
}

#[test]
fn latency_tracking_percentiles_accept_an_explicit_empty_list() {
    let server = TestServer::start_default();
    let mut client = server.connect();

    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"latency-tracking-info-percentiles", b"",]),
        b"+OK\r\n"
    );
    assert_config_value(&mut client, b"latency-tracking-info-percentiles", "");
}

#[test]
fn latency_tracking_defaults_to_real_command_duration_percentiles() {
    let server = TestServer::start_default();
    let mut client = server.connect();

    assert_config_value(&mut client, b"latency-tracking", "yes");
    assert_eq!(client.command(&[b"PING"]), b"+PONG\r\n");
    assert_eq!(client.command(&[b"DEBUG", b"SLEEP", b"0.001"]), b"+OK\r\n");
    // Keep the samples distinct even when scheduling adds a few milliseconds.
    assert_eq!(client.command(&[b"DEBUG", b"SLEEP", b"0.2"]), b"+OK\r\n");
    assert_eq!(
        client.command(&[
            b"CONFIG",
            b"SET",
            b"latency-tracking-info-percentiles",
            b"50",
            b"99",
            b"latency-monitor-threshold",
            b"1",
        ]),
        b"+OK\r\n"
    );
    assert_config_value(&mut client, b"latency-monitor-threshold", "1");

    let stats = client.command_str(&[b"INFO", b"latencystats"]);
    let debug = stats
        .lines()
        .find(|line| line.starts_with("latency_percentiles_usec_debug:"))
        .expect("DEBUG command latency percentiles");
    let p50 = debug
        .split("p50=")
        .nth(1)
        .and_then(|value| value.split(',').next())
        .and_then(|value| value.parse::<f64>().ok())
        .expect("parse DEBUG p50");
    let p99 = debug
        .split("p99=")
        .nth(1)
        .and_then(|value| value.parse::<f64>().ok())
        .expect("parse DEBUG p99");
    assert!(p99 > p50 * 10.0, "{debug}");
}

#[test]
fn disabling_latency_tracking_skips_later_command_samples() {
    let server = TestServer::start_default();
    let mut client = server.connect();

    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"latency-tracking", b"no"]),
        b"+OK\r\n"
    );
    assert_eq!(client.command(&[b"DEBUG", b"SLEEP", b"0.01"]), b"+OK\r\n");
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"latency-tracking", b"yes"]),
        b"+OK\r\n"
    );

    let stats = client.command_str(&[b"INFO", b"latencystats"]);
    assert!(
        !stats.contains("latency_percentiles_usec_debug:"),
        "disabled command must not leave a later histogram sample: {stats}"
    );
}

#[test]
fn debug_delay_configs_keep_c_signed_values_and_loading_interval_floor() {
    let server = TestServer::start_default();
    let mut client = server.connect();

    // C accepts signed debug delays: a negative value means that a one-microsecond
    // sleep happens statistically once per absolute-value invocations.  It must
    // not silently become an enormous unsigned delay or be discarded.
    for name in [b"rdb-key-save-delay" as &[u8], b"key-load-delay"] {
        assert_eq!(
            client.command(&[b"CONFIG", b"SET", name, b"-10"]),
            b"+OK\r\n"
        );
        assert_config_value(&mut client, name, "-10");
    }

    for name in [
        b"rdb-key-save-delay" as &[u8],
        b"key-load-delay",
        b"loading-process-events-interval-bytes",
    ] {
        assert!(
            client
                .command_str(&[b"CONFIG", b"SET", name, b"1k"])
                .contains("must be numeric"),
            "{} must be an INTEGER_CONFIG rather than accepting a memory suffix",
            String::from_utf8_lossy(name)
        );
    }

    // Valkey's createOffTConfig lower bound is 1024 bytes.  Accepting a smaller
    // value changes when a blocked loader yields to the event loop.
    assert!(client
        .command_str(&[
            b"CONFIG",
            b"SET",
            b"loading-process-events-interval-bytes",
            b"1023",
        ])
        .contains("out of range"));
    assert_eq!(
        client.command(&[
            b"CONFIG",
            b"SET",
            b"loading-process-events-interval-bytes",
            b"1024",
        ]),
        b"+OK\r\n"
    );
    assert_config_value(
        &mut client,
        b"loading-process-events-interval-bytes",
        "1024",
    );

    assert!(client
        .command_str(&[b"CONFIG", b"SET", b"rdb-key-save-delay", b"2147483648",])
        .contains("out of range"));
    let info = client.command_str(&[b"CONFIG", b"INFO", b"rdb-key-save-delay"]);
    assert!(
        info.contains("numeric")
            && info.contains("-2147483648")
            && info.contains("2147483647")
            && info.contains("hidden"),
        "CONFIG INFO must derive the C numeric hidden metadata, got {info:?}"
    );
}

#[test]
fn config_rewrite_preserves_signed_debug_delays() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_nanos();
    let config = std::env::temp_dir().join(format!(
        "versal-debug-delay-rewrite-{}-{unique}.conf",
        std::process::id()
    ));
    std::fs::write(&config, "").expect("write empty config");
    let server = TestServer::start(&[config.to_str().expect("UTF-8 config path")]);
    let mut client = server.connect();

    for (name, value) in [
        (b"rdb-key-save-delay" as &[u8], b"-17" as &[u8]),
        (b"key-load-delay", b"-19"),
        (b"loading-process-events-interval-bytes", b"1024"),
    ] {
        assert_eq!(
            client.command(&[b"CONFIG", b"SET", name, value]),
            b"+OK\r\n"
        );
    }
    assert_eq!(client.command(&[b"CONFIG", b"REWRITE"]), b"+OK\r\n");
    let rewritten = std::fs::read_to_string(&config).expect("read rewritten config");
    for line in [
        "rdb-key-save-delay -17",
        "key-load-delay -19",
        "loading-process-events-interval-bytes 1024",
    ] {
        assert!(
            rewritten.contains(line),
            "CONFIG REWRITE must preserve {line:?}, got {rewritten:?}"
        );
    }
    drop(server);
    let _ = std::fs::remove_file(config);
}

#[test]
fn bind_config_set_rebinds_the_live_tcp_listener() {
    // Start wildcard-bound so changing to loopback requires the runtime's
    // transactional listener replacement rather than merely updating config.
    let server = TestServer::start(&["--bind", "0.0.0.0"]);
    let mut client = server.connect();
    assert_config_value(&mut client, b"bind", "0.0.0.0");

    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"bind", b"127.0.0.1"]),
        b"+OK\r\n"
    );
    assert_config_value(&mut client, b"bind", "127.0.0.1");
    let mut reconnected = crate::common::Client::connect_to(server.port);
    assert_eq!(reconnected.command(&[b"PING"]), b"+PONG\r\n");
}

#[test]
fn invalid_bind_config_set_keeps_the_existing_listener_available() {
    let server = TestServer::start_default();
    let mut client = server.connect();

    assert!(client
        .command_str(&[b"CONFIG", b"SET", b"bind", b"999.999.999.999"])
        .contains("Failed to bind to specified addresses"));

    assert_eq!(server.connect().command(&[b"PING"]), b"+PONG\r\n");
}

#[test]
fn maxclients_rejects_new_connections_and_updates_info_stats() {
    let server = TestServer::start_default();
    let mut control = server.connect();
    assert_eq!(
        control.command(&[b"CONFIG", b"SET", b"maxclients", b"1"]),
        b"+OK\r\n"
    );

    // Keep the control client connected so the next accepted peer is over the
    // configured cap. The listener writes C's rejection reply before its L1
    // transport registration is released.
    let mut rejected = TcpStream::connect(("127.0.0.1", server.port)).unwrap();
    rejected
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut reply = [0_u8; 128];
    let received = rejected
        .read(&mut reply)
        .expect("read maxclients rejection");
    assert!(
        std::str::from_utf8(&reply[..received])
            .unwrap()
            .contains("ERR max number of clients reached"),
        "unexpected maxclients response: {:?}",
        &reply[..received]
    );
    assert!(
        control
            .command_str(&[b"INFO", b"STATS"])
            .contains("rejected_connections:1"),
        "INFO STATS did not record the rejected connection"
    );
}

#[cfg(feature = "tls")]
#[test]
fn tls_listener_serves_resp_over_the_configured_tls_port() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_nanos();
    let tls_port = free_tcp_port();
    let stem = format!("versal-tls-integration-{}-{unique}", std::process::id());
    let (cert_path, key_path) = write_tls_credentials(&stem);
    let config = std::env::temp_dir().join(format!("{stem}.conf"));
    std::fs::write(
        &config,
        format!(
            "tls-port {tls_port}\ntls-cert-file {}\ntls-key-file {}\ntls-auth-clients no\n",
            cert_path.display(),
            key_path.display()
        ),
    )
    .expect("write TLS config");
    let server = TestServer::start(&[config.to_str().expect("UTF-8 config path")]);
    let mut connector = SslConnector::builder(SslMethod::tls()).unwrap();
    connector.set_verify(SslVerifyMode::NONE);
    let stream = TcpStream::connect(("127.0.0.1", tls_port)).expect("connect TLS port");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut stream = connector
        .build()
        .connect("localhost", stream)
        .expect("complete TLS handshake");
    stream.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
    let mut reply = [0u8; 7];
    stream.read_exact(&mut reply).unwrap();
    assert_eq!(&reply, b"+PONG\r\n");
    drop(server);
    let _ = std::fs::remove_file(config);
    let _ = std::fs::remove_file(cert_path);
    let _ = std::fs::remove_file(key_path);
}

#[cfg(feature = "tls")]
#[test]
fn config_set_tls_material_rebinds_the_tls_listener() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_nanos();
    let tls_port = free_tcp_port();
    let stem = format!("versal-tls-reconfigure-{}-{unique}", std::process::id());
    let (cert_path, key_path) = write_tls_credentials(&stem);
    let server = TestServer::start_default();
    let mut client = server.connect();
    assert_eq!(
        client.command(&[
            b"CONFIG",
            b"SET",
            b"tls-cert-file",
            cert_path
                .to_str()
                .expect("UTF-8 certificate path")
                .as_bytes(),
            b"tls-key-file",
            key_path.to_str().expect("UTF-8 key path").as_bytes(),
            b"tls-auth-clients",
            b"no",
            b"tls-port",
            tls_port.to_string().as_bytes(),
        ]),
        b"+OK\r\n"
    );
    assert_config_value(&mut client, b"tls-port", &tls_port.to_string());
    let mut connector = SslConnector::builder(SslMethod::tls()).unwrap();
    connector.set_verify(SslVerifyMode::NONE);
    let stream = TcpStream::connect(("127.0.0.1", tls_port)).expect("connect reconfigured TLS");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut stream = connector
        .build()
        .connect("localhost", stream)
        .expect("complete reconfigured TLS handshake");
    stream.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
    let mut reply = [0u8; 7];
    stream.read_exact(&mut reply).unwrap();
    assert_eq!(&reply, b"+PONG\r\n");
    assert_eq!(
        client.command(&[
            b"CONFIG",
            b"SET",
            b"tls-protocols",
            b"TLSv1.2",
            b"tls-ciphers",
            b"ECDHE-RSA-AES128-GCM-SHA256",
            b"tls-ciphersuites",
            b"TLS_AES_128_GCM_SHA256",
            b"tls-prefer-server-ciphers",
            b"yes",
            b"tls-session-caching",
            b"no",
            b"tls-session-cache-size",
            b"123",
        ]),
        b"+OK\r\n"
    );
    assert_config_value(&mut client, b"tls-protocols", "TLSv1.2");
    assert_config_value(&mut client, b"tls-ciphers", "ECDHE-RSA-AES128-GCM-SHA256");
    assert_config_value(&mut client, b"tls-session-caching", "no");
    assert_config_value(&mut client, b"tls-session-cache-size", "123");
    assert!(client
        .command_str(&[b"CONFIG", b"SET", b"tls-protocols", b"TLSv9"])
        .contains("Unable to update TLS configuration"));
    assert_config_value(&mut client, b"tls-protocols", "TLSv1.2");
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"tls-port", b"0"]),
        b"+OK\r\n"
    );
    drop(server);
    let _ = std::fs::remove_file(cert_path);
    let _ = std::fs::remove_file(key_path);
}

#[cfg(feature = "tls")]
#[test]
fn replica_syncs_over_the_configured_tls_upstream() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_nanos();
    let tls_port = free_tcp_port();
    let stem = format!("versal-tls-replication-{}-{unique}", std::process::id());
    let (cert_path, key_path) = write_tls_credentials(&stem);
    let primary_config = std::env::temp_dir().join(format!("{stem}-primary.conf"));
    std::fs::write(
        &primary_config,
        format!(
            "tls-port {tls_port}\ntls-cert-file {}\ntls-key-file {}\ntls-ca-cert-file {}\ntls-auth-clients yes\n",
            cert_path.display(),
            key_path.display(),
            cert_path.display()
        ),
    )
    .expect("write TLS primary config");
    let primary = TestServer::start(&[primary_config.to_str().expect("UTF-8 primary config")]);
    let mut primary_client = primary.connect();
    assert_eq!(
        primary_client.command(&[b"SET", b"tls-replica-seed", b"before"]),
        b"+OK\r\n"
    );

    let replica_config = std::env::temp_dir().join(format!("{stem}-replica.conf"));
    std::fs::write(
        &replica_config,
        format!(
            "replicaof localhost {tls_port}\ntls-replication yes\ntls-ca-cert-file {}\ntls-cert-file {}\ntls-key-file {}\ntls-client-cert-file {}\ntls-client-key-file {}\n",
            cert_path.display(),
            cert_path.display(),
            key_path.display(),
            cert_path.display(),
            key_path.display()
        ),
    )
    .expect("write TLS replica config");
    let replica = TestServer::start(&[replica_config.to_str().expect("UTF-8 replica config")]);
    let mut startup_client = replica.connect();
    assert_config_value(&mut startup_client, b"tls-replication", "yes");
    assert_config_value(
        &mut startup_client,
        b"tls-ca-cert-file",
        cert_path.to_str().expect("UTF-8 certificate path"),
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut synced = false;
    while std::time::Instant::now() < deadline {
        let mut replica_client = replica.connect();
        if replica_client
            .command_str(&[b"INFO", b"REPLICATION"])
            .contains("master_link_status:up")
        {
            synced = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let mut diagnostic = replica.connect();
    let replica_info = diagnostic.command_str(&[b"INFO", b"REPLICATION"]);
    assert!(
        synced,
        "TLS replica did not reach master_link_status:up; INFO={replica_info:?}; primary clients={:?}; logs:\n{}",
        primary_client.command_str(&[b"CLIENT", b"LIST"]),
        replica.server_log(),
    );
    let mut replica_client = replica.connect();
    assert_eq!(
        replica_client.command(&[b"GET", b"tls-replica-seed"]),
        b"$6\r\nbefore\r\n"
    );
    assert_eq!(
        primary_client.command(&[b"SET", b"tls-replica-late", b"after"]),
        b"+OK\r\n"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut replicated = false;
    while std::time::Instant::now() < deadline {
        if replica_client.command(&[b"GET", b"tls-replica-late"]) == b"$5\r\nafter\r\n" {
            replicated = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(replicated, "TLS replica did not apply a streaming write");
    drop(replica);
    drop(primary);
    let _ = std::fs::remove_file(primary_config);
    let _ = std::fs::remove_file(replica_config);
    let _ = std::fs::remove_file(cert_path);
    let _ = std::fs::remove_file(key_path);
}

#[cfg(feature = "tls")]
#[test]
fn cluster_meet_uses_the_configured_tls_bus_transport() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_nanos();
    let stem = format!("versal-tls-cluster-{}-{unique}", std::process::id());
    let (cert_path, key_path) = write_tls_credentials(&stem);
    let primary_config = std::env::temp_dir().join(format!("{stem}-primary.conf"));
    let replica_config = std::env::temp_dir().join(format!("{stem}-replica.conf"));
    let tls_config = format!(
        "cluster-enabled yes\ntls-cluster yes\ntls-cert-file {}\ntls-key-file {}\ntls-ca-cert-file {}\ntls-auth-clients no\n",
        cert_path.display(),
        key_path.display(),
        cert_path.display()
    );
    std::fs::write(&primary_config, &tls_config).expect("write TLS Cluster primary config");
    std::fs::write(&replica_config, &tls_config).expect("write TLS Cluster replica config");
    let primary = TestServer::start(&[primary_config.to_str().expect("UTF-8 primary config")]);
    let replica = TestServer::start(&[replica_config.to_str().expect("UTF-8 replica config")]);
    let mut primary_client = primary.connect();
    let primary_id = primary_client.command_str(&[b"CLUSTER", b"MYID"]);
    let primary_id = primary_id
        .lines()
        .find(|line| line.len() == 40)
        .expect("primary CLUSTER MYID")
        .to_string();
    let mut replica_client = replica.connect();
    assert_eq!(
        replica_client.command(&[
            b"CLUSTER",
            b"MEET",
            b"127.0.0.1",
            primary.port.to_string().as_bytes(),
        ]),
        b"+OK\r\n"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut met = false;
    while std::time::Instant::now() < deadline {
        if replica_client
            .command_str(&[b"CLUSTER", b"NODES"])
            .contains(&primary_id)
        {
            met = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        met,
        "TLS Cluster MEET did not converge; primary logs:\n{}\nreplica logs:\n{}",
        primary.server_log(),
        replica.server_log()
    );
    drop(replica);
    drop(primary);
    let _ = std::fs::remove_file(primary_config);
    let _ = std::fs::remove_file(replica_config);
    let _ = std::fs::remove_file(cert_path);
    let _ = std::fs::remove_file(key_path);
}

#[cfg(feature = "tls")]
#[test]
fn tls_cluster_config_set_rebinds_the_cluster_bus() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_nanos();
    let stem = format!("versal-tls-cluster-rebind-{}-{unique}", std::process::id());
    let (cert_path, key_path) = write_tls_credentials(&stem);
    let primary_config = std::env::temp_dir().join(format!("{stem}-primary.conf"));
    std::fs::write(
        &primary_config,
        format!(
            "cluster-enabled yes\ntls-cluster yes\ntls-cert-file {}\ntls-key-file {}\ntls-ca-cert-file {}\ntls-auth-clients no\n",
            cert_path.display(),
            key_path.display(),
            cert_path.display()
        ),
    )
    .expect("write TLS Cluster primary config");
    let primary = TestServer::start(&[primary_config.to_str().expect("UTF-8 primary config")]);
    let replica = TestServer::start(&["--cluster-enabled", "yes"]);
    let mut primary_client = primary.connect();
    let primary_id = primary_client.command_str(&[b"CLUSTER", b"MYID"]);
    let primary_id = primary_id
        .lines()
        .find(|line| line.len() == 40)
        .expect("primary CLUSTER MYID")
        .to_string();
    let mut replica_client = replica.connect();
    assert_eq!(
        replica_client.command(&[
            b"CLUSTER",
            b"MEET",
            b"127.0.0.1",
            primary.port.to_string().as_bytes(),
        ]),
        b"+OK\r\n"
    );
    std::thread::sleep(Duration::from_millis(250));
    assert!(
        !replica_client
            .command_str(&[b"CLUSTER", b"NODES"])
            .contains(&primary_id),
        "TCP-only peer unexpectedly joined the TLS Cluster Bus"
    );
    assert_eq!(
        primary_client.command(&[b"CONFIG", b"SET", b"tls-cluster", b"no"]),
        b"+OK\r\n"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut met = false;
    while std::time::Instant::now() < deadline {
        if replica_client
            .command_str(&[b"CLUSTER", b"NODES"])
            .contains(&primary_id)
        {
            met = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        met,
        "Cluster Bus did not reconnect after tls-cluster rebind; primary logs:\n{}\nreplica logs:\n{}",
        primary.server_log(),
        replica.server_log()
    );
    drop(replica);
    drop(primary);
    let _ = std::fs::remove_file(primary_config);
    let _ = std::fs::remove_file(cert_path);
    let _ = std::fs::remove_file(key_path);
}

#[test]
fn configured_pidfile_records_the_server_pid_and_is_removed_after_shutdown() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_nanos();
    let pidfile = std::env::temp_dir().join(format!(
        "versal-pidfile-integration-{}-{unique}.pid",
        std::process::id()
    ));
    let config = std::env::temp_dir().join(format!(
        "versal-pidfile-integration-{}-{unique}.conf",
        std::process::id()
    ));
    std::fs::write(&config, format!("pidfile {}\n", pidfile.display()))
        .expect("write pidfile test config");

    let mut server = TestServer::start(&[config.to_str().expect("UTF-8 config path")]);
    assert_eq!(
        std::fs::read_to_string(&pidfile).expect("pidfile created after startup"),
        format!("{}\n", server.pid())
    );

    let mut client = server.connect();
    assert_config_value(
        &mut client,
        b"pidfile",
        pidfile.to_str().expect("UTF-8 pidfile path"),
    );
    let pidfile_info = client.command_str(&[b"CONFIG", b"INFO", b"pidfile"]);
    assert!(
        pidfile_info.contains("pidfile")
            && pidfile_info.contains("string")
            && pidfile_info.contains("immutable"),
        "CONFIG INFO must describe pidfile as an immutable string, got {pidfile_info:?}"
    );
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"pidfile", b"other.pid"]),
        b"-ERR CONFIG SET failed (possibly related to argument 'pidfile') - can't set immutable config\r\n"
    );
    assert_eq!(client.command(&[b"CONFIG", b"REWRITE"]), b"+OK\r\n");
    assert!(
        std::fs::read_to_string(&config)
            .expect("read rewritten pidfile config")
            .contains(&format!("pidfile {}", pidfile.display())),
        "CONFIG REWRITE must preserve the configured pidfile"
    );
    client.send(&[b"SHUTDOWN", b"NOSAVE"]);
    assert_eq!(
        server.wait_exit(Duration::from_secs(5)),
        Some(0),
        "configured pidfile shutdown should exit cleanly"
    );
    assert!(
        !pidfile.exists(),
        "normal shutdown must remove the pidfile created by this server"
    );
    let _ = std::fs::remove_file(config);
}

#[cfg(target_os = "linux")]
#[test]
fn disabled_set_proc_title_preserves_the_original_command_line() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_nanos();
    let config = std::env::temp_dir().join(format!(
        "versal-proctitle-integration-{}-{unique}.conf",
        std::process::id()
    ));
    std::fs::write(&config, "set-proc-title no\n").expect("write process-title test config");

    let mut server = TestServer::start(&[config.to_str().expect("UTF-8 config path")]);
    let command_line =
        std::fs::read(format!("/proc/{}/cmdline", server.pid())).expect("read server command line");
    assert!(
        command_line
            .windows(b"--port".len())
            .any(|window| window == b"--port"),
        "set-proc-title no must keep the original command line, got {:?}",
        String::from_utf8_lossy(&command_line)
    );

    let mut client = server.connect();
    assert_config_value(&mut client, b"set-proc-title", "no");
    let info = client.command_str(&[b"CONFIG", b"INFO", b"set-proc-title"]);
    assert!(
        info.contains("set-proc-title") && info.contains("immutable"),
        "CONFIG INFO must mark set-proc-title immutable, got {info:?}"
    );
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"set-proc-title", b"yes"]),
        b"-ERR CONFIG SET failed (possibly related to argument 'set-proc-title') - can't set immutable config\r\n"
    );
    assert_eq!(client.command(&[b"CONFIG", b"REWRITE"]), b"+OK\r\n");
    assert!(
        std::fs::read_to_string(&config)
            .expect("read rewritten process-title config")
            .contains("set-proc-title no"),
        "CONFIG REWRITE must preserve set-proc-title"
    );

    client.send(&[b"SHUTDOWN", b"NOSAVE"]);
    assert_eq!(
        server.wait_exit(Duration::from_secs(5)),
        Some(0),
        "process-title test server should exit cleanly"
    );
    let _ = std::fs::remove_file(config);
}

#[test]
fn always_show_logo_forces_the_startup_banner_without_a_tty() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_nanos();
    let config = std::env::temp_dir().join(format!(
        "versal-startup-logo-integration-{}-{unique}.conf",
        std::process::id()
    ));
    std::fs::write(&config, "always-show-logo yes\n").expect("write startup-logo test config");

    let mut server = TestServer::start(&[config.to_str().expect("UTF-8 config path")]);
    assert!(
        server.server_log().contains("VersalKV startup banner"),
        "always-show-logo must force the startup banner even with captured stdout:\n{}",
        server.server_log()
    );

    let mut client = server.connect();
    assert_config_value(&mut client, b"always-show-logo", "yes");
    let info = client.command_str(&[b"CONFIG", b"INFO", b"always-show-logo"]);
    assert!(
        info.contains("always-show-logo") && info.contains("immutable"),
        "CONFIG INFO must mark always-show-logo immutable, got {info:?}"
    );
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"always-show-logo", b"no"]),
        b"-ERR CONFIG SET failed (possibly related to argument 'always-show-logo') - can't set immutable config\r\n"
    );
    assert_eq!(client.command(&[b"CONFIG", b"REWRITE"]), b"+OK\r\n");
    assert!(
        std::fs::read_to_string(&config)
            .expect("read rewritten startup-logo config")
            .contains("always-show-logo yes"),
        "CONFIG REWRITE must preserve always-show-logo"
    );

    client.send(&[b"SHUTDOWN", b"NOSAVE"]);
    assert_eq!(
        server.wait_exit(Duration::from_secs(5)),
        Some(0),
        "startup-logo test server should exit cleanly"
    );
    let _ = std::fs::remove_file(config);
}

#[test]
fn non_tty_startup_logs_mode_instead_of_the_banner_by_default() {
    let server = TestServer::start_default();
    let log = server.server_log();
    assert!(
        log.contains("Running mode=standalone"),
        "non-TTY startup must use the compact C-compatible mode log:\n{log}"
    );
    assert!(
        !log.contains("VersalKV startup banner"),
        "default non-TTY startup must not force the banner:\n{log}"
    );
}

#[cfg(unix)]
#[test]
fn syslog_startup_configuration_is_visible_and_immutable() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_nanos();
    let config = std::env::temp_dir().join(format!(
        "versal-syslog-integration-{}-{unique}.conf",
        std::process::id()
    ));
    std::fs::write(
        &config,
        "syslog-enabled yes\nsyslog-ident versal-config-test\nsyslog-facility local3\n",
    )
    .expect("write syslog test config");

    let mut server = TestServer::start(&[config.to_str().expect("UTF-8 config path")]);
    let mut client = server.connect();
    for (name, value) in [
        (b"syslog-enabled" as &[u8], "yes"),
        (b"syslog-ident", "versal-config-test"),
        (b"syslog-facility", "local3"),
    ] {
        assert_config_value(&mut client, name, value);
        let info = client.command_str(&[b"CONFIG", b"INFO", name]);
        assert!(
            info.contains("immutable"),
            "CONFIG INFO must mark {} immutable, got {info:?}",
            String::from_utf8_lossy(name)
        );
        assert_eq!(
            client.command(&[b"CONFIG", b"SET", name, b"local0"]),
            format!(
                "-ERR CONFIG SET failed (possibly related to argument '{}') - can't set immutable config\r\n",
                String::from_utf8_lossy(name)
            )
            .as_bytes(),
        );
    }

    assert_eq!(client.command(&[b"CONFIG", b"REWRITE"]), b"+OK\r\n");
    let rewritten = std::fs::read_to_string(&config).expect("read rewritten syslog config");
    for line in [
        "syslog-enabled yes",
        "syslog-ident versal-config-test",
        "syslog-facility local3",
    ] {
        assert!(
            rewritten.contains(line),
            "CONFIG REWRITE must preserve {line:?}, got {rewritten:?}"
        );
    }

    client.send(&[b"SHUTDOWN", b"NOSAVE"]);
    assert_eq!(
        server.wait_exit(Duration::from_secs(5)),
        Some(0),
        "syslog-configured server should exit cleanly"
    );
    let _ = std::fs::remove_file(config);
}

#[cfg(unix)]
#[test]
fn invalid_syslog_facility_rejects_server_startup() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_nanos();
    let config = std::env::temp_dir().join(format!(
        "versal-invalid-syslog-integration-{}-{unique}.conf",
        std::process::id()
    ));
    std::fs::write(
        &config,
        "port 0\nbind 127.0.0.1\nsyslog-enabled yes\nsyslog-facility invalid\n",
    )
    .expect("write invalid syslog test config");

    let mut child = Command::new(env!("CARGO_BIN_EXE_versal-server"))
        .arg(&config)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn server with invalid syslog facility");
    std::thread::sleep(Duration::from_millis(200));
    let status = child.try_wait().expect("poll invalid syslog server");
    if status.is_none() {
        let _ = child.kill();
        let _ = child.wait();
        let _ = std::fs::remove_file(config);
        panic!("invalid syslog-facility must reject startup instead of running");
    }
    let output = child
        .wait_with_output()
        .expect("collect invalid syslog server output");
    assert!(
        !output.status.success(),
        "invalid syslog-facility unexpectedly exited successfully"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("invalid syslog-facility"),
        "startup failure must identify the invalid syslog facility: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let _ = std::fs::remove_file(config);
}

#[cfg(target_os = "linux")]
fn linux_first_allowed_cpu() -> String {
    let status = std::fs::read_to_string("/proc/self/status").expect("read current process status");
    let allowed = status
        .lines()
        .find_map(|line| line.strip_prefix("Cpus_allowed_list:\t"))
        .expect("current process CPU allowance");
    allowed
        .split(',')
        .next()
        .expect("non-empty CPU allowance")
        .split('-')
        .next()
        .expect("CPU range start")
        .to_string()
}

#[cfg(target_os = "linux")]
fn linux_thread_affinity(pid: u32, thread_name: &str) -> Option<String> {
    let tasks = std::fs::read_dir(format!("/proc/{pid}/task")).ok()?;
    for task in tasks.flatten() {
        let task_path = task.path();
        let Ok(name) = std::fs::read_to_string(task_path.join("comm")) else {
            continue;
        };
        if !thread_name.starts_with(name.trim()) {
            continue;
        }
        let Ok(status) = std::fs::read_to_string(task_path.join("status")) else {
            continue;
        };
        return status.lines().find_map(|line| {
            line.strip_prefix("Cpus_allowed_list:\t")
                .map(str::to_string)
        });
    }
    None
}

#[cfg(target_os = "linux")]
#[test]
fn bio_cpulist_applies_to_each_background_lazyfree_worker() {
    let cpu = linux_first_allowed_cpu();
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_nanos();
    let config = std::env::temp_dir().join(format!(
        "versal-bio-cpulist-integration-{}-{unique}.conf",
        std::process::id()
    ));
    std::fs::write(&config, format!("bio-cpulist {cpu}\n")).expect("write bio-cpulist test config");

    let server = TestServer::start(&[config.to_str().expect("UTF-8 config path")]);
    for thread_name in ["pkv-lazyfree", "pkv-repl-lazyfree"] {
        assert_eq!(
            linux_thread_affinity(server.pid(), thread_name),
            Some(cpu.clone()),
            "{thread_name} must inherit the configured bio-cpulist"
        );
    }
    drop(server);
    let _ = std::fs::remove_file(config);
}

#[test]
fn log_timestamp_format_applies_at_startup_and_runtime() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_nanos();
    let config = std::env::temp_dir().join(format!(
        "versal-log-timestamp-integration-{}-{unique}.conf",
        std::process::id()
    ));
    std::fs::write(&config, "log-timestamp-format milliseconds\n")
        .expect("write timestamp-format test config");

    let mut server = TestServer::start(&[config.to_str().expect("UTF-8 config path")]);
    let startup_prefix = format!("{}:M ", server.pid());
    let startup_log = server.server_log();
    let startup_timestamp = startup_log
        .lines()
        .find_map(|line| line.strip_prefix(&startup_prefix))
        .and_then(|line| line.split_once(" *").map(|(timestamp, _)| timestamp))
        .expect("startup log timestamp");
    assert!(
        startup_timestamp.bytes().all(|byte| byte.is_ascii_digit()),
        "milliseconds format must emit epoch milliseconds, got {startup_timestamp:?}"
    );

    let mut client = server.connect();
    assert_config_value(&mut client, b"log-timestamp-format", "milliseconds");
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"log-timestamp-format", b"iso8601"]),
        b"+OK\r\n"
    );
    assert_eq!(client.command(&[b"CONFIG", b"REWRITE"]), b"+OK\r\n");
    assert!(
        std::fs::read_to_string(&config)
            .expect("read rewritten timestamp config")
            .contains("log-timestamp-format iso8601"),
        "CONFIG REWRITE must preserve the current timestamp format"
    );
    client.send(&[b"SHUTDOWN", b"NOSAVE"]);
    assert_eq!(
        server.wait_exit(Duration::from_secs(5)),
        Some(0),
        "timestamp-format test server should exit cleanly"
    );
    assert!(
        server
            .server_log()
            .lines()
            .any(|line| line.starts_with(&startup_prefix) && line.contains('T')),
        "runtime CONFIG SET iso8601 must affect subsequent logs:\n{}",
        server.server_log()
    );
    let _ = std::fs::remove_file(config);
}

#[test]
fn log_format_switches_live_between_logfmt_and_json() {
    let server = TestServer::start_default();
    let mut client = server.connect();

    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"log-format", b"logfmt"]),
        b"+OK\r\n"
    );
    assert_config_value(&mut client, b"log-format", "logfmt");
    assert_eq!(
        client.command(&[b"DEBUG", b"LOG", b"logfmt-probe"]),
        b"+OK\r\n"
    );
    let logs = server.server_log();
    assert!(
        logs.lines().any(|line| {
            line.starts_with("pid=")
                && line.contains(" role=primary ")
                && line.contains(" level=warning ")
                && line.contains("message=\"DEBUG LOG: logfmt-probe\"")
        }),
        "missing C-shaped logfmt record: {logs}"
    );

    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"log-format", b"json"]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"DEBUG", b"LOG", b"json-probe"]),
        b"+OK\r\n"
    );
    assert!(
        server
            .server_log()
            .lines()
            .any(|line| line.starts_with('{')
                && line.contains("\"message\":\"DEBUG LOG: json-probe\"")),
        "missing JSON record: {}",
        server.server_log()
    );
}

struct DaemonGuard(Option<i32>);

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        if let Some(pid) = self.0.take() {
            let _ = send_signal(pid, ProcessSignal::Kill);
        }
    }
}

#[test]
fn daemonize_detaches_launcher_and_keeps_a_reachable_pidfile_server() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "versal-daemonize-integration-{}-{unique}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create daemon test directory");
    let pidfile = dir.join("versal.pid");
    let logfile = dir.join("versal.log");
    let config = dir.join("versal.conf");
    let port = TcpListener::bind("127.0.0.1:0")
        .expect("reserve daemon port")
        .local_addr()
        .expect("read daemon port")
        .port();
    std::fs::write(
        &config,
        format!(
            "bind 127.0.0.1\nport {port}\ndaemonize yes\npidfile {}\nlogfile {}\ndir {}\n",
            pidfile.display(),
            logfile.display(),
            dir.display()
        ),
    )
    .expect("write daemon config");

    let mut launcher = Command::new(env!("CARGO_BIN_EXE_versal-server"))
        .arg(&config)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn daemon launcher");
    let launcher_deadline = std::time::Instant::now() + Duration::from_secs(3);
    let launcher_status = loop {
        if let Some(status) = launcher.try_wait().expect("wait daemon launcher") {
            break status;
        }
        if std::time::Instant::now() >= launcher_deadline {
            let _ = launcher.kill();
            let _ = launcher.wait();
            panic!("daemonize launcher did not exit promptly");
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    assert!(
        launcher_status.success(),
        "daemon launcher status: {launcher_status}"
    );

    let mut daemon = DaemonGuard(None);
    let ready_deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(pid) = std::fs::read_to_string(&pidfile)
            .ok()
            .and_then(|value| value.trim().parse::<i32>().ok())
            .ok_or(())
        {
            daemon.0 = Some(pid);
            if let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)) {
                stream
                    .write_all(b"*1\r\n$4\r\nPING\r\n")
                    .expect("send daemon PING");
                let mut reply = [0; 16];
                if let Ok(count) = stream.read(&mut reply) {
                    if &reply[..count] == b"+PONG\r\n" {
                        break;
                    }
                }
            }
        }
        assert!(
            std::time::Instant::now() < ready_deadline,
            "daemon did not become reachable"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect daemon shutdown");
    stream
        .write_all(b"*2\r\n$8\r\nSHUTDOWN\r\n$6\r\nNOSAVE\r\n")
        .expect("send daemon shutdown");
    let shutdown_deadline = std::time::Instant::now() + Duration::from_secs(5);
    while pidfile.exists() && std::time::Instant::now() < shutdown_deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(!pidfile.exists(), "daemon shutdown must remove pidfile");
    daemon.0 = None;
    let _ = std::fs::remove_dir_all(dir);
}

#[cfg(unix)]
#[test]
fn systemd_supervision_reports_loading_and_ready_without_daemonizing() {
    let dir = make_socket_test_dir();
    let notify_socket = dir.join("notify.sock");
    let socket = UnixDatagram::bind(&notify_socket).expect("bind systemd notify socket");
    socket
        .set_nonblocking(true)
        .expect("nonblocking notify socket");
    let port = TcpListener::bind("127.0.0.1:0")
        .expect("reserve supervised port")
        .local_addr()
        .expect("read supervised port")
        .port();
    let config = dir.join("versal.conf");
    std::fs::write(
        &config,
        format!(
            "bind 127.0.0.1\nport {port}\nsupervised systemd\ndir {}\nlogfile {}\n",
            dir.display(),
            dir.join("versal.log").display()
        ),
    )
    .expect("write systemd config");

    let mut child = Command::new(env!("CARGO_BIN_EXE_versal-server"))
        .arg(&config)
        .env("NOTIFY_SOCKET", &notify_socket)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn supervised server");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut notifications = Vec::new();
    loop {
        let mut buffer = [0_u8; 512];
        match socket.recv(&mut buffer) {
            Ok(count) => notifications.extend_from_slice(&buffer[..count]),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("receive systemd notification: {error}"),
        }
        if notifications
            .windows(b"READY=1".len())
            .any(|w| w == b"READY=1")
            && notifications
                .windows(b"STATUS=Valkey is loading".len())
                .any(|w| w == b"STATUS=Valkey is loading")
        {
            break;
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "missing systemd loading/ready notifications: {:?}",
                String::from_utf8_lossy(&notifications)
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect supervised server");
    stream
        .write_all(b"*1\r\n$4\r\nPING\r\n")
        .expect("send supervised PING");
    let mut pong = [0_u8; 16];
    let count = stream.read(&mut pong).expect("read supervised PONG");
    assert_eq!(&pong[..count], b"+PONG\r\n");
    stream
        .write_all(b"*2\r\n$8\r\nSHUTDOWN\r\n$6\r\nNOSAVE\r\n")
        .expect("shutdown supervised server");
    let status = child.wait().expect("wait supervised server");
    assert!(status.success(), "supervised shutdown status: {status}");
    let _ = std::fs::remove_dir_all(dir);
}

#[cfg(unix)]
#[test]
fn upstart_supervision_stops_until_the_supervisor_continues_it() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "versal-upstart-integration-{}-{unique}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create upstart test directory");
    let port = TcpListener::bind("127.0.0.1:0")
        .expect("reserve upstart port")
        .local_addr()
        .expect("read upstart port")
        .port();
    let config = dir.join("versal.conf");
    std::fs::write(
        &config,
        format!(
            "bind 127.0.0.1\nport {port}\nsupervised upstart\ndir {}\nlogfile {}\n",
            dir.display(),
            dir.join("versal.log").display()
        ),
    )
    .expect("write upstart config");
    let mut child = Command::new(env!("CARGO_BIN_EXE_versal-server"))
        .arg(&config)
        .env("UPSTART_JOB", "versal-test")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn upstart server");
    let stopped_deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        // ps exposes the stopped state on both Linux and Darwin without
        // requiring /proc or consuming the Child's wait status.
        let output = Command::new("ps")
            .args(["-o", "stat=", "-p", &child.id().to_string()])
            .output()
            .expect("observe upstart child process state");
        let status = String::from_utf8_lossy(&output.stdout);
        if output.status.success() && status.trim().starts_with('T') {
            break;
        }
        if std::time::Instant::now() >= stopped_deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("upstart-supervised child never entered SIGSTOP state");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    send_signal(child.id() as i32, ProcessSignal::Continue).expect("continue upstart child");
    let ready_deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)) {
            stream
                .write_all(b"*1\r\n$4\r\nPING\r\n")
                .expect("send upstart PING");
            let mut pong = [0_u8; 16];
            if let Ok(count) = stream.read(&mut pong) {
                if &pong[..count] == b"+PONG\r\n" {
                    break;
                }
            }
        }
        if std::time::Instant::now() >= ready_deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("continued upstart child did not become ready");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let mut shutdown = TcpStream::connect(("127.0.0.1", port)).expect("connect upstart shutdown");
    shutdown
        .write_all(b"*2\r\n$8\r\nSHUTDOWN\r\n$6\r\nNOSAVE\r\n")
        .expect("shutdown upstart server");
    assert!(child.wait().expect("wait upstart server").success());
    let _ = std::fs::remove_dir_all(dir);
}

#[cfg(target_os = "linux")]
#[test]
fn proc_title_template_config_set_rewrites_the_live_process_title() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    let title = format!("versal-live-title-{}", server.pid());
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"proc-title-template", title.as_bytes(),]),
        b"+OK\r\n"
    );
    assert_config_value(&mut client, b"proc-title-template", &title);

    let command_line =
        std::fs::read(format!("/proc/{}/cmdline", server.pid())).expect("read live command line");
    assert!(
        command_line
            .windows(title.len())
            .any(|window| window == title.as_bytes()),
        "CONFIG SET did not refresh process title: {:?}",
        String::from_utf8_lossy(&command_line)
    );
}

#[cfg(target_os = "linux")]
#[test]
fn server_cpulist_applies_the_configured_linux_affinity() {
    let allowed = std::fs::read_to_string("/proc/self/status")
        .expect("read current CPU affinity")
        .lines()
        .find_map(|line| line.strip_prefix("Cpus_allowed_list:\t"))
        .expect("current CPU list")
        .split(',')
        .next()
        .expect("first current CPU range")
        .split('-')
        .next()
        .expect("first current CPU")
        .to_string();
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_nanos();
    let config = std::env::temp_dir().join(format!(
        "versal-cpulist-integration-{}-{unique}.conf",
        std::process::id()
    ));
    std::fs::write(&config, format!("server-cpulist {allowed}\n")).expect("write cpulist config");
    let server = TestServer::start(&[config.to_str().expect("UTF-8 config path")]);
    let child_status = std::fs::read_to_string(format!("/proc/{}/status", server.pid()))
        .expect("read server CPU affinity");
    let child_allowed = child_status
        .lines()
        .find_map(|line| line.strip_prefix("Cpus_allowed_list:\t"))
        .expect("server CPU list");
    assert_eq!(child_allowed, allowed, "server affinity must equal cpulist");
    let mut client = server.connect();
    assert_config_value(&mut client, b"server-cpulist", &allowed);
    assert_config_value(&mut client, b"server_cpulist", &allowed);
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"server-cpulist", b"0"]),
        b"-ERR CONFIG SET failed (possibly related to argument 'server-cpulist') - can't set immutable config\r\n"
    );
    let _ = std::fs::remove_file(config);
}
