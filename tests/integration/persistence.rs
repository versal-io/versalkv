use crate::common::TestServer;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn make_temp_dir(tag: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    let uniq = format!(
        "valkey-rust-rdb-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    dir.push(uniq);
    std::fs::create_dir_all(&dir).expect("create temp dir failed");
    dir
}

#[test]
fn rdb_save_and_recover() {
    let dir = make_temp_dir("recover");
    let dir_str = dir.to_str().unwrap().to_string();

    {
        let server = TestServer::start(&["--dir", &dir_str, "--dbfilename", "dump.rdb"]);
        let mut c = server.connect();
        assert!(c.command_str(&[b"SET", b"foo", b"bar"]).contains("OK"));
        assert!(c
            .command_str(&[b"SET", b"counter", b"12345"])
            .contains("OK"));
        assert!(c
            .command_str(&[b"RPUSH", b"mylist", b"a", b"b", b"c"])
            .contains(":3"));
        assert!(c.command_str(&[b"SAVE"]).contains("OK"));
    } // server drop

    assert!(dir.join("dump.rdb").exists(), "dump.rdb ");

    {
        let server = TestServer::start(&["--dir", &dir_str, "--dbfilename", "dump.rdb"]);
        let mut c = server.connect();
        assert!(c.command_str(&[b"GET", b"foo"]).contains("bar"));
        assert!(c.command_str(&[b"GET", b"counter"]).contains("12345"));
        assert_eq!(c.command_str(&[b"LLEN", b"mylist"]), ":3\r\n");
        assert!(c
            .command_str(&[b"LRANGE", b"mylist", b"0", b"-1"])
            .contains("a"));
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn startup_rdbchecksum_no_writes_a_zero_crc_trailer() {
    let dir = make_temp_dir("checksum-disabled");
    let config = dir.join("valkey.conf");
    std::fs::write(
        &config,
        format!(
            "dir {}\ndbfilename dump.rdb\nrdbchecksum no\n",
            dir.display()
        ),
    )
    .expect("write checksum-disabled config");
    let config_arg = config.to_str().expect("UTF-8 config path").to_string();

    {
        let server = TestServer::start_in_dir(dir.clone(), &[&config_arg]);
        let mut client = server.connect();
        assert!(client
            .command_str(&[b"CONFIG", b"GET", b"rdbchecksum"])
            .contains("no"));
        assert_eq!(client.command(&[b"SET", b"key", b"value"]), b"+OK\r\n");
        assert!(client
            .command(&[b"BGSAVE"])
            .starts_with(b"+Background saving"));
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let info = client.command_str(&[b"INFO", b"persistence"]);
            if info.contains("rdb_bgsave_in_progress:0")
                && info.contains("rdb_last_bgsave_status:ok")
            {
                break;
            }
            assert!(Instant::now() < deadline, "BGSAVE success was not observed");
            std::thread::sleep(Duration::from_millis(20));
        }
        let bytes = std::fs::read(dir.join("dump.rdb")).expect("read saved RDB");
        assert_eq!(&bytes[bytes.len() - 8..], &[0; 8]);
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn rdbcompression_config_changes_bgsave_lzf_encoding() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    let value = "compressible-value-".repeat(256);
    assert_eq!(
        client.command(&[b"SET", b"key", value.as_bytes()]),
        b"+OK\r\n"
    );

    for (compression, expect_lzf) in [(b"no" as &[u8], false), (b"yes", true)] {
        assert_eq!(
            client.command(&[b"CONFIG", b"SET", b"rdbcompression", compression]),
            b"+OK\r\n"
        );
        assert!(client
            .command(&[b"BGSAVE"])
            .starts_with(b"+Background saving"));
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let info = client.command_str(&[b"INFO", b"persistence"]);
            if info.contains("rdb_bgsave_in_progress:0")
                && info.contains("rdb_last_bgsave_status:ok")
            {
                break;
            }
            assert!(Instant::now() < deadline, "BGSAVE success was not observed");
            std::thread::sleep(Duration::from_millis(20));
        }
        let bytes = std::fs::read(server.data_dir().join("dump.rdb")).expect("read saved RDB");
        assert_eq!(
            bytes[..bytes.len() - 8].contains(&0xC3),
            expect_lzf,
            "rdbcompression {compression:?} produced an unexpected RDB encoding"
        );
        if expect_lzf {
            assert!(bytes.len() < value.len());
        }
    }
}

#[test]
fn rdbcompression_config_changes_dump_encoding() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    let value = "compressible-value-".repeat(256);
    assert_eq!(
        client.command(&[b"SET", b"key", value.as_bytes()]),
        b"+OK\r\n"
    );

    for (compression, expect_lzf) in [(b"no" as &[u8], false), (b"yes", true)] {
        assert_eq!(
            client.command(&[b"CONFIG", b"SET", b"rdbcompression", compression]),
            b"+OK\r\n"
        );
        let dump = client.command(&[b"DUMP", b"key"]);
        assert_eq!(
            dump.contains(&0xC3),
            expect_lzf,
            "rdbcompression {compression:?} produced an unexpected DUMP encoding"
        );
    }
}

#[test]
fn rdb_recover_preserves_ttl() {
    let dir = make_temp_dir("ttl");
    let dir_str = dir.to_str().unwrap().to_string();

    {
        let server = TestServer::start(&["--dir", &dir_str, "--dbfilename", "dump.rdb"]);
        let mut c = server.connect();
        assert!(c.command_str(&[b"SET", b"withttl", b"v"]).contains("OK"));
        assert!(c
            .command_str(&[b"EXPIRE", b"withttl", b"10000"])
            .contains(":1"));
        assert!(c.command_str(&[b"SAVE"]).contains("OK"));
    }

    {
        let server = TestServer::start(&["--dir", &dir_str, "--dbfilename", "dump.rdb"]);
        let mut c = server.connect();
        assert!(c.command_str(&[b"GET", b"withttl"]).contains("v"));
        let ttl = c.command_str(&[b"TTL", b"withttl"]);
        assert!(ttl.starts_with(":"), "TTL : {}", ttl);
        let n: i64 = ttl.trim_start_matches(':').trim().parse().unwrap();
        assert!(n > 0 && n <= 10000, "TTL : {}", n);
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn rdb_no_file_starts_empty() {
    let dir = make_temp_dir("empty");
    let dir_str = dir.to_str().unwrap().to_string();

    let server = TestServer::start(&["--dir", &dir_str, "--dbfilename", "dump.rdb"]);
    let mut c = server.connect();
    assert_eq!(c.command_str(&[b"DBSIZE"]), ":0\r\n");
    assert!(c.command_str(&[b"GET", b"foo"]).contains("$-1"));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn corrupt_aof_reports_pid_before_fatal_startup_exit() {
    let dir = make_temp_dir("corrupt-aof-pid");
    let aof_dir = dir.join("appendonlydir");
    std::fs::create_dir_all(&aof_dir).unwrap();
    std::fs::write(
        aof_dir.join("appendonly.aof.manifest"),
        b"file appendonly.aof.1.incr.aof seq 1 type i\n",
    )
    .unwrap();
    std::fs::write(aof_dir.join("appendonly.aof.1.incr.aof"), b"!!!").unwrap();
    let config = dir.join("valkey.conf");
    std::fs::write(
        &config,
        format!(
            "dir {}\nport 0\nappendonly yes\naof-load-truncated no\n",
            dir.display()
        ),
    )
    .unwrap();

    let child = Command::new(env!("CARGO_BIN_EXE_versal-server"))
        .arg(config)
        .output()
        .expect("failed to run versal-server");

    assert!(!child.status.success());
    let stdout = String::from_utf8_lossy(&child.stdout);
    assert!(
        stdout.contains("PID:") && stdout.contains("Server initialized"),
        "missing startup PID marker before fatal AOF load:\n{stdout}"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn startup_rdb_services_loading_commands_and_rejects_role_change_before_ready() {
    use versal::foundation::io::persistence::serialize_all;
    use versal::foundation::io::store::kv_engine::DbSet;
    use versal::foundation::io::store::Robj;

    let dir = make_temp_dir("startup-host-rdb");
    let mut dbs = DbSet::new(1);
    // C permits no interval below 1024 bytes. Use records larger than that
    // floor so startup loading has repeated real service checkpoints instead
    // of relying on an invalid, artificially tiny interval.
    let value = "value".repeat(512);
    for index in 0..200 {
        dbs.get_mut(0).expect("DB 0").set(
            format!("key-{index}").into_bytes(),
            Robj::string(value.clone()),
        );
    }
    std::fs::write(dir.join("dump.rdb"), serialize_all(&dbs, 0)).expect("write RDB");
    let reservation = TcpListener::bind("127.0.0.1:0").expect("reserve port");
    let port = reservation.local_addr().expect("port").port();
    drop(reservation);
    let stdout_path = dir.join("startup.stdout");
    let stderr_path = dir.join("startup.stderr");
    let stdout = std::fs::File::create(&stdout_path).expect("stdout file");
    let stderr = std::fs::File::create(&stderr_path).expect("stderr file");
    let port_arg = port.to_string();
    let mut child = Command::new(env!("CARGO_BIN_EXE_versal-server"))
        .args([
            "--port",
            &port_arg,
            "--bind",
            "127.0.0.1",
            "--dir",
            dir.to_str().expect("UTF-8 path"),
            "--key-load-delay",
            "10000",
            "--loading-process-events-interval-bytes",
            "1024",
        ])
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .expect("spawn server");
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut stream = loop {
        if let Ok(stream) = TcpStream::connect(("127.0.0.1", port)) {
            break stream;
        }
        assert!(
            Instant::now() < deadline,
            "listener was not published during loading"
        );
        std::thread::sleep(Duration::from_millis(5));
    };
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .expect("set read timeout");
    stream
        .write_all(b"*2\r\n$4\r\nINFO\r\n$11\r\npersistence\r\n")
        .expect("send INFO");
    let mut info = [0_u8; 16 * 1024];
    let info_len = stream.read(&mut info).expect("read INFO while loading");
    assert!(info[..info_len].starts_with(b"$"));
    let startup_log = std::fs::read_to_string(&stdout_path).unwrap_or_default();
    assert!(!startup_log.contains("Ready to accept connections"));
    let mut denied = TcpStream::connect(("127.0.0.1", port)).expect("connect SET client");
    denied
        .set_read_timeout(Some(Duration::from_secs(3)))
        .expect("set denied-client timeout");
    denied
        .write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$1\r\ny\r\n")
        .expect("send SET");
    let mut loading = [0_u8; 128];
    let count = denied.read(&mut loading).expect("read LOADING reply");
    assert!(String::from_utf8_lossy(&loading[..count]).contains("LOADING"));

    denied
        .write_all(b"*3\r\n$9\r\nREPLICAOF\r\n$9\r\n127.0.0.1\r\n$1\r\n1\r\n")
        .expect("send REPLICAOF while loading");
    let mut replicaof_reply = [0_u8; 128];
    let count = denied
        .read(&mut replicaof_reply)
        .expect("read REPLICAOF reply");
    assert!(
        String::from_utf8_lossy(&replicaof_reply[..count]).contains("LOADING"),
        "REPLICAOF must remain blocked during startup RDB loading: {:?}",
        String::from_utf8_lossy(&replicaof_reply[..count])
    );

    denied
        .write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$1\r\ny\r\n")
        .expect("send SET after REPLICAOF");
    let mut loading = [0_u8; 128];
    let count = denied
        .read(&mut loading)
        .expect("read LOADING reply after REPLICAOF");
    assert!(
        String::from_utf8_lossy(&loading[..count]).contains("LOADING"),
        "REPLICAOF cleared the outer startup loading state: {:?}",
        String::from_utf8_lossy(&loading[..count])
    );

    assert!(
        child
            .try_wait()
            .expect("check server is still loading")
            .is_none(),
        "startup role-change rejection must keep the server alive"
    );
    let startup_log = std::fs::read_to_string(&stdout_path).unwrap_or_default();
    assert!(
        !startup_log.contains("replication role changed during startup RDB load"),
        "blocked role change must not trip the startup invariant:\n{startup_log}"
    );
    assert!(!startup_log.contains("Ready to accept connections"));
    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn incr_is_appended_to_multipart_aof() {
    let dir = make_temp_dir("incr-aof");
    let dir_str = dir.to_str().unwrap().to_string();
    {
        let server = TestServer::start(&["--dir", &dir_str, "--appendonly"]);
        let mut client = server.connect();
        assert_eq!(client.command_str(&[b"INCR", b"foo"]), ":1\r\n");
    }

    let bytes = std::fs::read(dir.join("appendonlydir").join("appendonly.aof.1.incr.aof")).unwrap();
    assert!(
        bytes.windows(4).any(|window| window == b"INCR"),
        "INCR was not propagated to AOF: {:?}",
        String::from_utf8_lossy(&bytes)
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn getex_without_options_does_not_append_to_aof() {
    let server = TestServer::start(&["--appendonly"]);
    let mut client = server.connect();
    let aof = server
        .data_dir()
        .join("appendonlydir")
        .join("appendonly.aof.1.incr.aof");

    assert_eq!(client.command(&[b"SET", b"foo", b"bar"]), b"+OK\r\n");
    let before = std::fs::metadata(&aof)
        .expect("SET reply must follow the AOF write")
        .len();

    assert_eq!(client.command(&[b"GETEX", b"foo"]), b"$3\r\nbar\r\n");
    let after = std::fs::metadata(&aof)
        .expect("read AOF metadata after GETEX")
        .len();

    assert_eq!(
        before, after,
        "GETEX without options must not enter the AOF"
    );
}

#[test]
fn aof_timestamp_enabled_annotates_runtime_writes() {
    let dir = make_temp_dir("timestamp-aof");
    let dir_str = dir.to_str().expect("UTF-8 temp dir").to_string();
    {
        let server = TestServer::start(&["--dir", &dir_str, "--appendonly"]);
        let mut client = server.connect();
        assert_eq!(
            client.command(&[b"CONFIG", b"SET", b"aof-timestamp-enabled", b"yes"]),
            b"+OK\r\n"
        );
        assert_eq!(client.command(&[b"INCR", b"timestamped"]), b":1\r\n");
    }

    let bytes = std::fs::read(dir.join("appendonlydir").join("appendonly.aof.1.incr.aof"))
        .expect("read timestamped AOF increment");
    assert!(
        bytes.windows(4).any(|window| window == b"#TS:"),
        "runtime timestamp setting must annotate AOF writes: {:?}",
        String::from_utf8_lossy(&bytes)
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn loading_debug_configs_can_be_set_and_read() {
    let server = TestServer::start_default();
    let mut client = server.connect();

    assert_eq!(
        client.command_str(&[b"CONFIG", b"SET", b"key-load-delay", b"1500"]),
        "+OK\r\n"
    );
    assert_eq!(
        client.command_str(&[
            b"CONFIG",
            b"SET",
            b"loading-process-events-interval-bytes",
            b"1024",
        ]),
        "+OK\r\n"
    );
    assert!(client
        .command_str(&[b"CONFIG", b"GET", b"key-load-delay"])
        .contains("1500"));
    assert!(client
        .command_str(&[b"CONFIG", b"GET", b"loading-process-events-interval-bytes",])
        .contains("1024"));
}

#[test]
fn failed_bgsave_blocks_writes_with_misconf() {
    let server = TestServer::start_default();
    let mut client = server.connect();

    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"dir", b"/"]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"dbfilename", b"tmp"]),
        b"+OK\r\n"
    );
    assert!(client
        .command(&[b"BGSAVE"])
        .starts_with(b"+Background saving"));

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let info = client.command_str(&[b"INFO", b"persistence"]);
        if info.contains("rdb_bgsave_in_progress:0") && info.contains("rdb_last_bgsave_status:err")
        {
            break;
        }
        assert!(Instant::now() < deadline, "BGSAVE failure was not observed");
        std::thread::sleep(Duration::from_millis(20));
    }

    assert!(client
        .command(&[b"SET", b"key", b"value"])
        .starts_with(b"-MISCONF"));
    assert!(client.command(&[b"PING"]).starts_with(b"-MISCONF"));

    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"stop-writes-on-bgsave-error", b"no"]),
        b"+OK\r\n"
    );
    assert_eq!(client.command(&[b"SET", b"key", b"recovered"]), b"+OK\r\n");
}
