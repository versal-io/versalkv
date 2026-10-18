use crate::common::TestServer;
use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::time::Duration;
use versal::foundation::io::sys::process::{send_signal, ProcessSignal};

#[test]
fn successful_shutdown_nosave_exits_without_reply() {
    let mut server = TestServer::start_default();
    let mut stream = TcpStream::connect(("127.0.0.1", server.port)).expect("connect shutdown");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set shutdown read timeout");
    stream
        .write_all(b"*2\r\n$8\r\nSHUTDOWN\r\n$6\r\nNOSAVE\r\n")
        .expect("send shutdown");

    let mut reply = [0_u8; 64];
    match stream.read(&mut reply) {
        Ok(0) => {}
        Ok(count) => panic!(
            "successful shutdown must close without replying, got {:?}",
            String::from_utf8_lossy(&reply[..count])
        ),
        Err(error) if error.kind() == ErrorKind::ConnectionReset => {}
        Err(error) => panic!("shutdown connection did not close cleanly: {error}"),
    }
    assert_eq!(
        server.wait_exit(Duration::from_secs(5)),
        Some(0),
        "successful shutdown must terminate with status 0"
    );
}

#[test]
fn shutdown_abort_no_op_returns_error() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let reply = c.command_str(&[b"SHUTDOWN", b"ABORT"]);
    assert!(
        reply.starts_with("-ERR No shutdown in progress"),
        "got: {}",
        reply
    );
}

#[test]
fn shutdown_syntax_error_on_unknown_flag() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let reply = c.command_str(&[b"SHUTDOWN", b"BOGUS"]);
    assert!(reply.starts_with("-ERR syntax error"), "got: {}", reply);
}

#[test]
fn sigterm_default_saves_and_exits() {
    let mut srv = TestServer::start(&["--save", "360000", "100000000"]);
    {
        let mut c = srv.connect();
        assert!(c.command_str(&[b"SET", b"k", b"v"]).starts_with("+OK"));
    }

    let pid = srv.pid();
    send_signal(pid as i32, ProcessSignal::Terminate).expect("terminate server");
    let status = srv.wait_exit(Duration::from_secs(10));
    assert_eq!(status, Some(0), "expected clean exit after SIGTERM");

    let dir: PathBuf = srv.take_data_dir();
    let srv2 = TestServer::start_in_dir(dir, &["--save", "360000", "100000000"]);
    let mut c = srv2.connect();
    let reply = c.command_str(&[b"GET", b"k"]);
    assert!(reply.contains("\r\nv\r\n"), "GET k = {:?}", reply);
}

fn configured_signal_nosave_skips_rdb_persistence(signal: ProcessSignal, setting: &[u8]) {
    // Keep automatic BGSAVE out of the observation: the only possible RDB
    // producer for this key must be the signal-triggered shutdown path.
    let mut server = TestServer::start(&["--save", "360000", "100000000"]);
    {
        let mut client = server.connect();
        assert_eq!(
            client.command(&[b"SET", b"signal-key", b"value"]),
            b"+OK\r\n"
        );
        assert_eq!(
            client.command(&[b"CONFIG", b"SET", setting, b"nosave"]),
            b"+OK\r\n"
        );
    }

    send_signal(server.pid() as i32, signal).expect("send configured shutdown signal");
    assert_eq!(
        server.wait_exit(Duration::from_secs(10)),
        Some(0),
        "configured NOSAVE signal should shut down cleanly"
    );

    // Restarting from exactly the exited server's data directory proves that
    // the live signal-specific flags reached prepare_for_shutdown: a default
    // signal shutdown would have persisted this key instead.
    let data_dir = server.take_data_dir();
    let restarted = TestServer::start_in_dir(data_dir, &["--save", "360000", "100000000"]);
    let mut client = restarted.connect();
    assert_eq!(
        client.command(&[b"GET", b"signal-key"]),
        b"$-1\r\n",
        "configured NOSAVE must suppress the signal-triggered RDB save"
    );
}

#[test]
fn sigterm_config_nosave_skips_rdb_persistence() {
    configured_signal_nosave_skips_rdb_persistence(
        ProcessSignal::Terminate,
        b"shutdown-on-sigterm",
    );
}

#[test]
fn sigint_config_nosave_skips_rdb_persistence() {
    configured_signal_nosave_skips_rdb_persistence(ProcessSignal::Interrupt, b"shutdown-on-sigint");
}

#[test]
fn sigterm_persists_runtime_acl_users_for_restart() {
    let data_dir = std::env::temp_dir().join(format!(
        "valkey-rust-sigterm-acl-rewrite-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before Unix epoch")
            .as_nanos()
    ));
    std::fs::create_dir(&data_dir).expect("create test directory");
    let config_path = data_dir.join("valkey.conf");
    std::fs::write(&config_path, "").expect("write config");
    let config_arg = config_path.to_string_lossy().into_owned();
    let mut server = TestServer::start_in_dir(data_dir, &[&config_arg]);
    {
        let mut client = server.connect();
        assert!(client
            .command_str(&[
                b"ACL",
                b"SETUSER",
                b"sentinel-user",
                b"on",
                b">sentinel-password",
                b"+@all",
                b"~*",
            ])
            .starts_with("+OK"));
        let rewrite = client.command_str(&[b"CONFIG", b"REWRITE"]);
        assert!(
            rewrite.starts_with("+OK"),
            "CONFIG REWRITE failed: {rewrite}"
        );
    }

    let pid = server.pid();
    send_signal(pid as i32, ProcessSignal::Terminate).expect("terminate server");
    assert_eq!(
        server.wait_exit(Duration::from_secs(10)),
        Some(0),
        "expected clean exit after SIGTERM"
    );

    let rewritten = std::fs::read_to_string(&config_path).expect("read rewritten config");
    let rewritten = rewritten.replacen(&format!("port {}\n", server.port), "port invalid\n", 1);
    std::fs::write(&config_path, rewritten).expect("normalize test config port");
    let data_dir = server.take_data_dir();
    let restarted = TestServer::start_in_dir(data_dir, &[&config_arg]);
    let mut client = restarted.connect();
    assert!(client
        .command_str(&[b"AUTH", b"sentinel-user", b"sentinel-password",])
        .starts_with("+OK"));
}

#[test]
fn double_sigint_forces_exit_and_cleans_temp() {
    let mut srv = TestServer::start(&["--save", ""]);
    let workdir: PathBuf = srv.data_dir().to_path_buf();
    {
        let mut c = srv.connect();
        assert!(c
            .command_str(&[b"CONFIG", b"SET", b"rdb-key-save-delay", b"5000000"])
            .starts_with("+OK"));
        for i in 0..20 {
            c.command(&[b"SET", format!("k{}", i).as_bytes(), b"v"]);
        }
        let reply = c.command_str(&[b"BGSAVE"]);
        assert!(
            reply.contains("Background saving started"),
            "got: {}",
            reply
        );
    }
    std::thread::sleep(Duration::from_millis(200));

    let pid = srv.pid();
    send_signal(pid as i32, ProcessSignal::Interrupt).expect("send first interrupt");
    std::thread::sleep(Duration::from_millis(5));
    send_signal(pid as i32, ProcessSignal::Interrupt).expect("send second interrupt");
    let status = srv.wait_exit(Duration::from_secs(10));
    assert_eq!(status, Some(1), "expected exit(1) after 2 SIGINTs");

    let mut leftover: Option<String> = None;
    if let Ok(entries) = std::fs::read_dir(&workdir) {
        for e in entries.flatten() {
            if let Some(name) = e.file_name().to_str() {
                if name.starts_with("temp-") && name.ends_with(".rdb") {
                    leftover = Some(name.to_string());
                    break;
                }
            }
        }
    }
    assert!(
        leftover.is_none(),
        "temp-*.rdb was not cleaned up: {:?}",
        leftover
    );
}

#[test]
fn single_sigint_with_failed_save_keeps_server_alive() {
    let mut srv = TestServer::start(&["--save", "900", "1"]);
    {
        let mut c = srv.connect();
        for i in 0..20 {
            assert!(c
                .command_str(&[b"SET", format!("k{}", i).as_bytes(), b"v"])
                .starts_with("+OK"));
        }
    }

    let dump_rdb = srv.data_dir().join("dump.rdb");
    let _ = std::fs::remove_file(&dump_rdb);
    std::fs::create_dir_all(&dump_rdb).expect("create dump.rdb dir to force save failure");

    let pid = srv.pid();
    send_signal(pid as i32, ProcessSignal::Interrupt).expect("interrupt server");

    std::thread::sleep(Duration::from_millis(300));
    assert!(
        srv.wait_exit(Duration::from_millis(200)).is_none(),
        "server exited after a single SIGINT with failed save; expected it to stay alive"
    );

    let mut c = srv.connect();
    let reply = c.command_str(&[b"SHUTDOWN"]);
    assert!(
        reply.contains("Errors trying to SHUTDOWN"),
        "expected error reply, got: {:?}",
        reply
    );
    assert!(
        c.command_str(&[b"PING"]).starts_with("+PONG"),
        "server should still respond to PING after failed SHUTDOWN"
    );

    let _ = std::fs::remove_dir_all(&dump_rdb);
}

#[test]
fn busy_shutdown_nosave_pipeline_cannot_be_aborted() {
    let mut server = TestServer::start(&["--appendonly"]);
    let mut admin = server.connect();
    assert_eq!(
        admin.command_str(&[b"CONFIG", b"SET", b"lua-time-limit", b"10"]),
        "+OK\r\n"
    );

    let mut eval = server.connect();
    eval.send(&[
        b"EVAL",
        b"redis.call('SET', KEYS[1], 'v'); while true do end",
        b"1",
        b"pipeline-key",
    ]);
    std::thread::sleep(Duration::from_millis(200));
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let reply = admin.command_str(&[b"PING"]);
        if reply.starts_with("-BUSY") {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "script never entered BUSY state: {reply:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    let mut shutdown = server.connect();
    shutdown.send_raw(
        b"*2\r\n$8\r\nSHUTDOWN\r\n$6\r\nNOSAVE\r\n*2\r\n$8\r\nSHUTDOWN\r\n$5\r\nABORT\r\n",
    );
    // C blocks the SHUTDOWN client and exits without replying after the busy
    // script unwinds. Requiring +OK here would reopen the listener-close race
    // that lets the pipelined ABORT or a new connection reach the server.
    assert_eq!(
        server.wait_exit(Duration::from_secs(5)),
        Some(0),
        "pipelined ABORT canceled an accepted BUSY shutdown"
    );
}
