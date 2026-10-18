//! DEBUG command tests that need isolated process handling.

use crate::common::TestServer;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

#[test]
fn debug_restart_replaces_the_process_and_accepts_new_connections() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.send(&[b"DEBUG", b"RESTART", b"0"]);
    drop(c);

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if let Ok(mut stream) = TcpStream::connect(("127.0.0.1", srv.port)) {
            let _ = stream.set_read_timeout(Some(Duration::from_millis(100)));
            if stream.write_all(b"*1\r\n$4\r\nPING\r\n").is_ok() {
                let mut reply = [0u8; 7];
                if stream.read_exact(&mut reply).is_ok() && &reply == b"+PONG\r\n" {
                    return;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("server did not accept connections after DEBUG RESTART");
}

#[test]
fn debug_restart_rejects_non_integer_delay() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let reply = c.command_str(&[b"DEBUG", b"CRASH-AND-RECOVER", b"not-an-int"]);
    assert!(
        reply.starts_with("-ERR value is not an integer or out of range"),
        "unexpected reply: {}",
        reply
    );
}

#[test]
fn debug_panic_terminates_server_process() {
    let mut srv = TestServer::start_default();
    let mut c = srv.connect();
    c.send(&[b"DEBUG", b"PANIC"]);
    let status = srv.wait_status(Duration::from_secs(5));
    drop(c);
    assert!(status.is_some(), "server did not exit after DEBUG PANIC");
    let log = srv.server_log();
    assert!(log.contains(""), "panic report missing: {log}");
    assert!(
        log.contains("command=debug"),
        "command context missing: {log}"
    );
    assert!(log.contains("client_id="), "client id missing: {log}");
    assert!(
        log.contains("command_seq="),
        "command sequence missing: {log}"
    );
}

#[test]
fn debug_panic_defaults_to_abort_signal() {
    let mut srv = TestServer::start_default();
    let mut client = srv.connect();
    client.send(&[b"DEBUG", b"PANIC"]);

    let status = srv
        .wait_status(Duration::from_secs(5))
        .expect("server did not exit after DEBUG PANIC");
    assert_eq!(
        status.code(),
        None,
        "the default C-compatible panic policy must terminate by signal, not an ordinary exit"
    );
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(libc::SIGABRT));
    }
    let stdout = std::fs::read_to_string(srv.data_dir().join("versal-server.stdout")).unwrap();
    assert_completed_crash_trace(&stdout, "debug_panic");
}

#[test]
fn use_exit_on_panic_changes_debug_panic_to_exit_code_one() {
    let mut srv = TestServer::start_default();
    let mut client = srv.connect();
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"use-exit-on-panic", b"yes"]),
        b"+OK\r\n"
    );
    client.send(&[b"DEBUG", b"PANIC"]);

    assert_eq!(
        srv.wait_exit(Duration::from_secs(5)),
        Some(1),
        "use-exit-on-panic must use a normal exit status instead of aborting"
    );
}

#[test]
fn use_exit_on_panic_also_applies_to_debug_assert() {
    let mut srv = TestServer::start_default();
    let mut client = srv.connect();
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"use-exit-on-panic", b"yes"]),
        b"+OK\r\n"
    );
    client.send(&[b"DEBUG", b"ASSERT"]);

    assert_eq!(
        srv.wait_exit(Duration::from_secs(5)),
        Some(1),
        "the configured panic policy must cover DEBUG ASSERT as well"
    );
}

fn assert_completed_crash_trace(log: &str, frame: &str) {
    let start = log
        .find("------ STACK TRACE ------")
        .unwrap_or_else(|| panic!("stack trace start missing: {log}"));
    let done = log
        .find("------ STACK TRACE DONE ------")
        .unwrap_or_else(|| panic!("stack trace completion missing: {log}"));
    assert!(start < done, "stack trace markers out of order: {log}");
    assert!(
        log[start..done].contains(frame),
        "actual crashing command frame missing: {log}"
    );
    assert_eq!(log.matches("------ STACK TRACE ------").count(), 1, "{log}");
    assert_eq!(
        log.matches("------ STACK TRACE DONE ------").count(),
        1,
        "{log}"
    );
}

#[test]
fn debug_assert_logs_completed_stack_trace_before_exit_without_rust_backtrace() {
    let mut srv = TestServer::start_with_env(
        &[
            "--use-exit-on-panic",
            "yes",
            "--crash-memcheck-enabled",
            "no",
        ],
        &[("RUST_BACKTRACE", "0"), ("RUST_LIB_BACKTRACE", "0")],
    );
    srv.connect().send(&[b"DEBUG", b"ASSERT"]);
    assert_eq!(srv.wait_exit(Duration::from_secs(5)), Some(1));
    let stdout = std::fs::read_to_string(srv.data_dir().join("versal-server.stdout")).unwrap();
    assert!(stdout.contains("ASSERTION FAILED"), "{stdout}");
    assert!(stdout.contains("command=debug"), "{stdout}");
    assert_completed_crash_trace(&stdout, "debug_assert");
}

#[test]
fn debug_assert_stack_trace_uses_configured_logfile() {
    let mut srv = TestServer::start_with_env(
        &[
            "--use-exit-on-panic",
            "yes",
            "--logfile",
            "assert-crash.log",
        ],
        &[("RUST_BACKTRACE", "0"), ("RUST_LIB_BACKTRACE", "0")],
    );
    srv.connect().send(&[b"DEBUG", b"ASSERT"]);
    assert_eq!(srv.wait_exit(Duration::from_secs(5)), Some(1));
    let logfile = std::fs::read_to_string(srv.data_dir().join("assert-crash.log")).unwrap();
    assert!(logfile.contains("ASSERTION FAILED"), "{logfile}");
    assert_completed_crash_trace(&logfile, "debug_assert");
    let stdout = std::fs::read_to_string(srv.data_dir().join("versal-server.stdout")).unwrap();
    assert!(!stdout.contains("STACK TRACE"), "{stdout}");
}

#[test]
fn disabled_crashlog_suppresses_panic_stack_trace_for_both_exit_policies() {
    for policy in ["yes", "no"] {
        let mut srv =
            TestServer::start(&["--use-exit-on-panic", policy, "--crash-log-enabled", "no"]);
        srv.connect().send(&[b"DEBUG", b"PANIC"]);
        let status = srv
            .wait_status(Duration::from_secs(5))
            .expect("panic must terminate");
        assert_eq!(status.code(), if policy == "yes" { Some(1) } else { None });
        let stdout = std::fs::read_to_string(srv.data_dir().join("versal-server.stdout")).unwrap();
        assert!(!stdout.contains("panic="), "{stdout}");
        assert!(!stdout.contains("STACK TRACE"), "{stdout}");
    }
}

#[test]
fn debug_oom_terminates_server_process() {
    let mut srv = TestServer::start_default();
    let mut c = srv.connect();
    c.send(&[b"DEBUG", b"OOM"]);
    let status = srv.wait_status(Duration::from_secs(5));
    drop(c);
    assert!(status.is_some(), "server did not exit after DEBUG OOM");
}
