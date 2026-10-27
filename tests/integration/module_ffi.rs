//! Module FFI integration skeleton.
//! Owner: AGENT D. Future tests should load module smoke fixtures through the shared harness.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use crate::common::TestServer;

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn server_exports_direct_module_abi_symbols() {
    let output = Command::new("nm")
        .arg(if cfg!(target_os = "macos") {
            "-g"
        } else {
            "-D"
        })
        .arg(env!("CARGO_BIN_EXE_versal-server"))
        .output()
        .expect("inspect versal-server dynamic symbols");
    assert!(
        output.status.success(),
        "nm failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let symbols = String::from_utf8_lossy(&output.stdout);
    for symbol in ["VM_CallArgv", "VM_ReplyRaw"] {
        let exported_name = if cfg!(target_os = "macos") {
            format!("_{symbol}")
        } else {
            symbol.to_string()
        };
        assert!(
            symbols.lines().any(|line| {
                let mut fields = line.split_whitespace().rev();
                fields.next() == Some(exported_name.as_str()) && fields.next() == Some("T")
            }),
            "{symbol} must be a defined export for direct module ABI calls"
        );
    }
}

#[test]
fn module_ffi_top10_smoke_fixture_loads_and_runs_commands() {
    let server = TestServer::start_default();
    let module_path = compile_top10_smoke_module(server.data_dir());
    let module_path = module_path.to_string_lossy().into_owned();
    let mut client = server.connect();

    assert_eq!(
        client.command(&[b"MODULE", b"LOAD", module_path.as_bytes()]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"TOP10.ECHO", b"module-api"]),
        b"$10\r\nmodule-api\r\n"
    );
    assert_eq!(client.command(&[b"TOP10.INTEGER"]), b":42\r\n");

    assert_eq!(
        client.command(&[b"SET", b"module-key", b"abcdef"]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"TOP10.KEYINFO", b"module-key"]),
        b"*2\r\n:1\r\n:6\r\n"
    );
    assert_eq!(
        client.command(&[b"TOP10.KEYINFO", b"missing-key"]),
        b"*2\r\n:0\r\n:0\r\n"
    );
    assert_eq!(client.command(&[b"LPUSH", b"module-list", b"a"]), b":1\r\n");
    assert_eq!(
        client.command(&[b"HSET", b"module-hash", b"f", b"v"]),
        b":1\r\n"
    );
    assert_eq!(client.command(&[b"SADD", b"module-set", b"m"]), b":1\r\n");
    assert_eq!(
        client.command(&[b"ZADD", b"module-zset", b"1", b"m"]),
        b":1\r\n"
    );
    assert_eq!(
        client.command(&[b"TOP10.KEYTYPE", b"module-list"]),
        b":2\r\n"
    );
    assert_eq!(
        client.command(&[b"TOP10.KEYTYPE", b"module-hash"]),
        b":3\r\n"
    );
    assert_eq!(
        client.command(&[b"TOP10.KEYTYPE", b"module-set"]),
        b":4\r\n"
    );
    assert_eq!(
        client.command(&[b"TOP10.KEYTYPE", b"module-zset"]),
        b":5\r\n"
    );

    assert_eq!(
        client.command(&[b"DEL", b"__module_ffi_counter__"]),
        b":0\r\n"
    );
    assert_eq!(
        client.command(&[b"TOP10.CALL"]),
        b"*3\r\n:2\r\n:1\r\n:1\r\n"
    );

    assert_eq!(
        client.command(&[b"TOP20.DB"]),
        b"*4\r\n:0\r\n:0\r\n:1\r\n:0\r\n"
    );
    assert_eq!(
        client.command(&[b"SET", b"module-delete", b"value"]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"TOP20.EXISTS_DELETE", b"module-delete"]),
        b"*3\r\n:1\r\n:0\r\n:0\r\n"
    );
    assert_eq!(
        client.command(&[b"TOP20.EXISTS_DELETE", b"missing-delete"]),
        b"*3\r\n:0\r\n:1\r\n:0\r\n"
    );
    assert_eq!(client.command(&[b"TOP20.DATATYPE"]), b"+OK\r\n");

    assert_eq!(
        client.command(&[b"TOP30.CONTAINER"]),
        b"-ERR wrong number of arguments for 'top30.container' command\r\n"
    );
    assert_eq!(
        client.command(&[b"TOP30.CONTAINER", b"CHILD"]),
        b"+subcommand\r\n"
    );
}

#[test]
fn module_call_monitor_events_precede_the_outer_module_command() {
    let server = TestServer::start_default();
    let module_path = compile_top10_smoke_module(server.data_dir());
    let module_path = module_path.to_string_lossy().into_owned();
    let mut client = server.connect();
    let mut monitor = server.connect();

    assert_eq!(
        client.command(&[b"MODULE", b"LOAD", module_path.as_bytes()]),
        b"+OK\r\n"
    );
    assert_eq!(monitor.command(&[b"MONITOR"]), b"+OK\r\n");
    assert_eq!(
        client.command(&[b"DEL", b"__module_ffi_counter__"]),
        b":0\r\n"
    );
    let _ = monitor.read_reply();

    assert_eq!(
        client.command(&[b"TOP10.CALL"]),
        b"*3\r\n:2\r\n:1\r\n:1\r\n"
    );
    let incr = String::from_utf8_lossy(&monitor.read_reply()).into_owned();
    let get = String::from_utf8_lossy(&monitor.read_reply()).into_owned();
    let outer = String::from_utf8_lossy(&monitor.read_reply()).into_owned();

    assert!(
        incr.to_ascii_lowercase()
            .contains("\"incr\" \"__module_ffi_counter__\""),
        "{incr}"
    );
    assert!(
        get.to_ascii_lowercase()
            .contains("\"get\" \"__module_ffi_counter__\""),
        "{get}"
    );
    assert!(
        outer.to_ascii_lowercase().contains("\"top10.call\""),
        "{outer}"
    );
}

#[test]
fn official_header_module_loads_without_symbol_preemption() {
    let server = TestServer::start_default();
    let module_path = compile_official_header_module(server.data_dir());
    let module_path = module_path.to_string_lossy().into_owned();
    let mut client = server.connect();

    assert_eq!(
        client.command(&[b"MODULE", b"LOAD", module_path.as_bytes()]),
        b"+OK\r\n"
    );
    assert_eq!(client.command(&[b"OFFICIAL.PING"]), b"+PONG\r\n");
}

#[test]
fn module_auto_memory_fixture_preserves_retained_values_and_isolates_callbacks() {
    let server = TestServer::start_default();
    let module_path = compile_header_module(server.data_dir(), "auto_memory_lifecycle");
    let path = module_path.to_string_lossy();
    let mut client = server.connect();
    assert!(client
        .command(&[b"MODULE", b"LOAD", path.as_bytes(), b"fail", b"now"])
        .starts_with(b"-ERR"));
    assert_eq!(
        client.command(&[b"MODULE", b"LOAD", path.as_bytes(), b"load-argument"]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"LIFECYCLE.ARGV"]),
        b"$13\r\nload-argument\r\n"
    );
    assert_eq!(client.command(&[b"LIFECYCLE.EXERCISE"]), b"+OK\r\n");
    assert_eq!(client.command(&[b"LIFECYCLE.NOTIFY"]), b"+OK\r\n");
    assert_eq!(
        client.command(&[b"LIFECYCLE.TIMER", b"timer-argument"]),
        b"+OK\r\n"
    );
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let value = client.command(&[b"GET", b"lifecycle:timer"]);
        if value == b"$14\r\ntimer-argument\r\n" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timer did not preserve its retained argument"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn official_header_direct_argv_raw_roundtrip() {
    let server = TestServer::start_default();
    let module_path = compile_official_header_module(server.data_dir());
    let module_path = module_path.to_string_lossy().into_owned();
    let mut client = server.connect();

    assert_eq!(
        client.command(&[b"MODULE", b"LOAD", module_path.as_bytes()]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"OFFICIAL.CALLARGV", b"ECHO", b"a\0b\r\nc"]),
        b"$6\r\na\0b\r\nc\r\n"
    );
    assert_eq!(
        client.command(&[b"OFFICIAL.CALLARGV", b"SET", b"argv-raw-key", b"\0value"]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"OFFICIAL.CALLARGV", b"GET", b"argv-raw-key"]),
        b"$6\r\n\0value\r\n"
    );
}

#[test]
fn module_event_loop_hooks_bracket_each_poll_iteration() {
    let server = TestServer::start_default();
    let module_path = compile_module_fixture(server.data_dir(), "event_loop_hooks");
    let module_path = module_path.to_string_lossy().into_owned();
    let mut client = server.connect();

    assert_eq!(
        client.command(&[b"MODULE", b"LOAD", module_path.as_bytes()]),
        b"+OK\r\n"
    );
    let first = parse_event_loop_counts(&client.command(&[b"EVENTLOOP.COUNTS"]));
    let second = parse_event_loop_counts(&client.command(&[b"EVENTLOOP.COUNTS"]));

    assert_eq!(first.0, first.1);
    assert!(first.0 > 0);
    assert_eq!(second.0, second.1);
    assert!(second.0 > first.0);
}

#[test]
fn module_ffi_replicate_propagates_custom_frame_to_replica() {
    let master = TestServer::start_default();
    let replica = TestServer::start_default();

    let module_path = compile_top10_smoke_module(master.data_dir());
    let module_path = module_path.to_string_lossy().into_owned();
    let mut master_client = master.connect();
    assert_eq!(
        master_client.command(&[b"MODULE", b"LOAD", module_path.as_bytes()]),
        b"+OK\r\n"
    );

    let mut replica_client = replica.connect();
    let master_port = master.port.to_string();
    assert_eq!(
        replica_client.command(&[b"REPLICAOF", b"127.0.0.1", master_port.as_bytes()]),
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

    assert_eq!(
        master_client.command(&[b"TOP30.REPLICATE", b"module-repl", b"copied"]),
        b"*2\r\n:0\r\n:0\r\n"
    );
    let propagated = wait_until(
        || {
            let mut client = replica.connect();
            client.command(&[b"GET", b"module-repl"]) == b"$6\r\ncopied\r\n"
        },
        Duration::from_secs(10),
    );
    assert!(propagated, "module Replicate frame did not reach replica");
}

#[test]
fn redisjson_smoke_module_runs_ten_roundtrips() {
    let server = TestServer::start_default();
    let module_path = compile_module_fixture(server.data_dir(), "redisjson_smoke");
    let module_path = module_path.to_string_lossy().into_owned();
    let mut client = server.connect();

    assert_eq!(
        client.command(&[b"MODULE", b"LOAD", module_path.as_bytes()]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"JSON.SET", b"doc", b".", br#"{"a":1}"#]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"JSON.GET", b"doc"]),
        b"$7\r\n{\"a\":1}\r\n"
    );
    assert_eq!(client.command(&[b"JSON.TYPE", b"doc"]), b"+object\r\n");
    assert_eq!(client.command(&[b"JSON.OBJLEN", b"doc"]), b":1\r\n");
    assert_eq!(
        client.command(&[b"JSON.SET", b"arr", b".", b"[1,2]"]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"JSON.ARRAPPEND", b"arr", b".", b"3"]),
        b":3\r\n"
    );
    assert_eq!(
        client.command(&[b"JSON.SET", b"num", b".", b"41"]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"JSON.NUMINCRBY", b"num", b".", b"1"]),
        b"$2\r\n42\r\n"
    );
    assert_eq!(
        client.command(&[b"JSON.MGET", b"doc", b"num", b"."]),
        b"*2\r\n$7\r\n{\"a\":1}\r\n$2\r\n42\r\n"
    );
    assert_eq!(
        client.command(&[b"JSON.RESP", b"num"]),
        b"*2\r\n$6\r\nnumber\r\n$2\r\n42\r\n"
    );
    assert_eq!(client.command(&[b"JSON.CLEAR", b"doc"]), b":1\r\n");
    assert_eq!(client.command(&[b"JSON.DEL", b"doc"]), b":1\r\n");
}

#[test]
fn redisearch_mini_module_runs_ten_roundtrips() {
    let server = TestServer::start_default();
    let module_path = compile_module_fixture(server.data_dir(), "redisearch_mini");
    let module_path = module_path.to_string_lossy().into_owned();
    let mut client = server.connect();

    assert_eq!(
        client.command(&[b"MODULE", b"LOAD", module_path.as_bytes()]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[
            b"FT.CREATE",
            b"idx",
            b"ON",
            b"HASH",
            b"SCHEMA",
            b"title",
            b"TEXT"
        ]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[
            b"FT.ADD",
            b"idx",
            b"doc:1",
            b"1.0",
            b"FIELDS",
            b"title",
            b"hello world"
        ]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[
            b"FT.ADD",
            b"idx",
            b"doc:2",
            b"1.0",
            b"FIELDS",
            b"title",
            b"rust world"
        ]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"FT.SEARCH", b"idx", b"hello"]),
        b"*3\r\n:1\r\n$5\r\ndoc:1\r\n*2\r\n$5\r\ntitle\r\n$11\r\nhello world\r\n"
    );
    assert_eq!(
        client.command(&[b"FT.SEARCH", b"idx", b"world"]),
        b"*5\r\n:2\r\n$5\r\ndoc:1\r\n*2\r\n$5\r\ntitle\r\n$11\r\nhello world\r\n$5\r\ndoc:2\r\n*2\r\n$5\r\ntitle\r\n$10\r\nrust world\r\n"
    );
    assert_eq!(
        client.command(&[b"FT.INFO", b"idx"]),
        b"*4\r\n$7\r\nindexes\r\n:1\r\n$9\r\ndocuments\r\n:2\r\n"
    );
    assert_eq!(client.command(&[b"FT._LIST"]), b"*1\r\n$3\r\nidx\r\n");
    assert_eq!(client.command(&[b"FT.DROPINDEX", b"idx"]), b"+OK\r\n");
    assert_eq!(client.command(&[b"FT._LIST"]), b"*0\r\n");
    assert_eq!(
        client.command(&[b"FT.SEARCH", b"idx", b"world"]),
        b"-ERR no such index\r\n"
    );
}

fn compile_top10_smoke_module(out_dir: &Path) -> PathBuf {
    compile_module_fixture(out_dir, "top10_smoke")
}

fn compile_official_header_module(out_dir: &Path) -> PathBuf {
    compile_header_module(out_dir, "official_header_smoke")
}

fn compile_header_module(out_dir: &Path, name: &str) -> PathBuf {
    let source = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("module_ffi")
        .join(format!("{name}.c"));
    let include_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("thirdparty")
        .join("valkey")
        .join("src");
    let output = out_dir.join(format!("{name}.so"));
    let mut compiler = Command::new(std::env::var_os("CC").unwrap_or_else(|| "clang".into()));
    compiler
        .args(["-shared", "-fPIC", "-Wall", "-Wextra", "-Werror"])
        .arg("-I")
        .arg(include_dir)
        .arg("-o")
        .arg(&output)
        .arg(&source);
    #[cfg(target_os = "linux")]
    compiler.arg("-fuse-ld=lld");
    #[cfg(target_os = "macos")]
    compiler.arg("-Wl,-undefined,dynamic_lookup");
    let status = compiler
        .status()
        .expect("failed to invoke cc for official-header module fixture");
    assert!(status.success(), "failed to compile {source:?}");
    output
}

fn compile_module_fixture(out_dir: &Path, name: &str) -> PathBuf {
    let source = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("module_ffi")
        .join(format!("{name}.c"));
    let output = out_dir.join(format!("{name}.so"));
    let mut compiler = Command::new(std::env::var_os("CC").unwrap_or_else(|| "clang".into()));
    compiler
        .args(["-shared", "-fPIC", "-Wall", "-Wextra", "-Werror"])
        .arg("-o")
        .arg(&output)
        .arg(&source);
    #[cfg(target_os = "linux")]
    compiler.arg("-fuse-ld=lld");
    let status = compiler
        .status()
        .expect("failed to invoke cc for module FFI fixture");
    assert!(status.success(), "failed to compile {source:?}");
    output
}

fn parse_event_loop_counts(reply: &[u8]) -> (i64, i64) {
    let mut lines = std::str::from_utf8(reply)
        .expect("event-loop reply is UTF-8")
        .split("\r\n");
    assert_eq!(lines.next(), Some("*2"));
    let before = lines
        .next()
        .and_then(|line| line.strip_prefix(':'))
        .expect("before-sleep integer")
        .parse()
        .expect("valid before-sleep count");
    let after = lines
        .next()
        .and_then(|line| line.strip_prefix(':'))
        .expect("after-sleep integer")
        .parse()
        .expect("valid after-sleep count");
    (before, after)
}

fn wait_until(mut predicate: impl FnMut() -> bool, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if predicate() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    false
}
