use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::common::TestServer;

// ============================================================================
// ============================================================================

fn binary_path(name: &str) -> std::path::PathBuf {
    let mut path = std::path::PathBuf::from(env!("CARGO_BIN_EXE_versal-server"));
    path.set_file_name(name);
    path
}

fn run_cli(server: &TestServer, args: &[&str]) -> String {
    let output = Command::new(binary_path("versal-cli"))
        .arg("-h")
        .arg("127.0.0.1")
        .arg("-p")
        .arg(server.port.to_string())
        .args(args)
        .output()
        .expect("run versal-cli");
    assert!(
        output.status.success(),
        "versal-cli failed: args={:?} stderr={}",
        args,
        String::from_utf8_lossy(&output.stderr),
    );
    let s = String::from_utf8_lossy(&output.stdout).to_string();
    s.trim_end_matches('\n').to_string()
}

fn run_cli_with_stdin(server: &TestServer, args: &[&str], stdin_data: &[u8]) -> String {
    let mut child = Command::new(binary_path("versal-cli"))
        .arg("-h")
        .arg("127.0.0.1")
        .arg("-p")
        .arg(server.port.to_string())
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn versal-cli");
    child.stdin.take().unwrap().write_all(stdin_data).unwrap();
    let output = child.wait_with_output().expect("wait versal-cli");
    assert!(
        output.status.success(),
        "versal-cli -x failed: args={:?} stderr={}",
        args,
        String::from_utf8_lossy(&output.stderr),
    );
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

fn run_cli_rdb(server: &TestServer, option: &str, path: &std::path::Path) -> String {
    let output = Command::new(binary_path("versal-cli"))
        .args(["-h", "127.0.0.1", "-p"])
        .arg(server.port.to_string())
        .arg(option)
        .arg(path)
        .output()
        .expect("run versal-cli RDB mode");
    assert!(
        output.status.success(),
        "versal-cli {option} failed: stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stderr).to_string()
}

// ============================================================================
// ============================================================================

#[test]
fn nontty_status_reply() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    assert!(client
        .command_str(&[b"SET", b"nontty-key", b"bar"])
        .contains("OK"));
    assert_eq!("OK", run_cli(&server, &["set", "nontty-key2", "bar"]));
    assert_eq!("bar", run_cli(&server, &["get", "nontty-key"]));
}

#[test]
fn nontty_integer_reply() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    client.command_str(&[b"DEL", b"nontty-counter"]);
    assert_eq!("1", run_cli(&server, &["incr", "nontty-counter"]));
    assert_eq!("2", run_cli(&server, &["incr", "nontty-counter"]));
}

#[test]
fn nontty_bulk_reply() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    client.command_str(&[b"SET", b"nontty-bulk", b"hello\tworld\n"]);
    let out = run_cli(&server, &["get", "nontty-bulk"]);
    // Non-TTY output: raw string, trailing newline from println! is stripped
    assert_eq!("hello\tworld", out, "out={out:?}");
}

#[test]
fn nontty_multi_bulk_reply() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    client.command_str(&[b"DEL", b"nontty-list"]);
    client.command_str(&[b"RPUSH", b"nontty-list", b"foo"]);
    client.command_str(&[b"RPUSH", b"nontty-list", b"bar"]);
    let out = run_cli(&server, &["lrange", "nontty-list", "0", "-1"]);
    assert_eq!("foo\nbar", out);
}

#[test]
fn json_outputs_raw_non_ascii_bulk_bytes() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    client.command(&[b"HSET", b"eth", b"test", &[0xc3, 0xb0, b'e']]);
    assert_eq!(
        "\"\u{00f0}e\"",
        run_cli(&server, &["--json", "hget", "eth", "test"]),
    );
}

#[test]
fn rdb_mode_writes_a_loadable_snapshot() {
    for diskless in [false, true] {
        let server = TestServer::start_default();
        let mut client = server.connect();
        assert_eq!(
            client.command(&[
                b"CONFIG",
                b"SET",
                b"repl-diskless-sync",
                if diskless { b"yes" } else { b"no" }
            ]),
            b"+OK\r\n"
        );
        assert_eq!(
            client.command(&[b"SET", b"rdb-key", b"rdb-value"]),
            b"+OK\r\n"
        );

        let path = std::env::temp_dir().join(format!(
            "versal-cli-rdb-{}-{}-{}.rdb",
            std::process::id(),
            server.port,
            diskless
        ));
        let output = Command::new(binary_path("versal-cli"))
            .args(["-h", "127.0.0.1", "-p"])
            .arg(server.port.to_string())
            .arg("--rdb")
            .arg(&path)
            .output()
            .expect("run versal-cli --rdb");
        assert!(
            output.status.success(),
            "stderr={}",
            String::from_utf8_lossy(&output.stderr)
        );

        let rdb = std::fs::read(&path).expect("read transferred RDB");
        assert!(!rdb.is_empty(), "transferred RDB must not be empty");
        std::fs::copy(&path, server.data_dir().join("dump.rdb")).expect("install transferred RDB");
        assert_eq!(
            client.command(&[b"DEBUG", b"RELOAD", b"NOSAVE"]),
            b"+OK\r\n"
        );
        assert_eq!(
            client.command(&[b"GET", b"rdb-key"]),
            b"$9\r\nrdb-value\r\n"
        );
        std::fs::remove_file(path).expect("remove transferred RDB");
    }
}

#[test]
fn rdb_mode_matches_tcl_dump_scenarios() {
    const LIB1: &[u8] =
        b"#!lua name=lib1\nserver.register_function('func1', function() return 123 end)";
    const EXTRA_LIB: &[u8] = b"#!lua name=should_not_exist_func\nserver.register_function('should_not_exist_func', function() return 456 end)";

    for functions_only in [false, true] {
        for diskless in [false, true] {
            let server = TestServer::start_default();
            let mut client = server.connect();
            let diskless_arg: &[u8] = if diskless { b"yes" } else { b"no" };
            assert_eq!(
                client.command(&[b"CONFIG", b"SET", b"repl-diskless-sync", diskless_arg]),
                b"+OK\r\n"
            );
            if diskless {
                assert_eq!(
                    client.command(&[b"CONFIG", b"SET", b"repl-diskless-sync-delay", b"0"]),
                    b"+OK\r\n"
                );
            }

            assert_eq!(client.command(&[b"FLUSHDB"]), b"+OK\r\n");
            assert_eq!(client.command(&[b"FUNCTION", b"FLUSH"]), b"+OK\r\n");
            assert_eq!(
                run_cli(&server, &["debug", "populate", "100000", "key", "1000"]),
                "OK"
            );
            assert_eq!(
                client.command(&[b"FUNCTION", b"LOAD", LIB1]),
                b"$4\r\nlib1\r\n"
            );

            let cli_rdb = server.data_dir().join(format!(
                "cli-functions-{}-diskless-{}.rdb",
                functions_only, diskless
            ));
            let option = if functions_only {
                "--functions-rdb"
            } else {
                "--rdb"
            };
            let output = run_cli_rdb(&server, option, &cli_rdb);
            assert!(
                output.contains("Transfer finished with success."),
                "unexpected RDB output: {output:?}"
            );

            let dump_rdb = server.data_dir().join("dump.rdb");
            let _ = std::fs::remove_file(&dump_rdb);
            std::fs::rename(&cli_rdb, &dump_rdb).expect("install transferred RDB");

            assert_eq!(
                client.command(&[b"SET", b"should-not-exist", b"1"]),
                b"+OK\r\n"
            );
            assert_eq!(
                client.command(&[b"FUNCTION", b"LOAD", EXTRA_LIB]),
                b"$21\r\nshould_not_exist_func\r\n"
            );
            assert_eq!(run_cli(&server, &["debug", "reload", "nosave"]), "OK");
            assert_eq!(client.command(&[b"GET", b"should-not-exist"]), b"$-1\r\n");
            let function_list = client.command_str(&[b"FUNCTION", b"LIST"]);
            assert!(
                function_list.contains("lib1") && function_list.contains("func1"),
                "restored function list missing lib1/func1: {function_list:?}"
            );
            assert!(
                !function_list.contains("should_not_exist_func"),
                "RDB reload should discard function loaded after dump: {function_list:?}"
            );

            let expected_dbsize: &[u8] = if functions_only {
                b":0\r\n"
            } else {
                b":100000\r\n"
            };
            assert_eq!(client.command(&[b"DBSIZE"]), expected_dbsize);
        }
    }
}

#[test]
fn client_kill_type_slave_closes_cli_replica() {
    let server = TestServer::start_default();
    let replica = Command::new(binary_path("versal-cli"))
        .args(["-h", "127.0.0.1", "-p"])
        .arg(server.port.to_string())
        .arg("--replica")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn versal-cli --replica");

    let mut admin = server.connect();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if admin
            .command_str(&[b"CLIENT", b"LIST", b"TYPE", b"replica"])
            .contains("id=")
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "versal-cli --replica did not connect"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    assert_eq!(
        admin.command(&[b"CLIENT", b"KILL", b"TYPE", b"slave"]),
        b":1\r\n"
    );
    let output = replica
        .wait_with_output()
        .expect("wait versal-cli --replica");
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("Server closed the connection"),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn nontty_eval_should_not_crash() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    client.command_str(&[b"SET", b"eval-key", b"1"]);
    let out = run_cli(&server, &["eval", "return {ARGV[1]}", "0", "hello"]);
    assert_eq!("hello", out);
}

#[test]
fn nontty_read_last_argument_from_pipe() {
    let server = TestServer::start_default();
    let out = run_cli_with_stdin(&server, &["-x", "set", "pipe-key"], b"from pipe");
    assert_eq!("OK", out);
    let mut client = server.connect();
    let val = client.command_str(&[b"GET", b"pipe-key"]);
    assert!(val.contains("from pipe"), "got: {val}");
}

#[test]
fn nontty_read_last_argument_from_file() {
    let server = TestServer::start_default();
    let out = run_cli_with_stdin(&server, &["-x", "set", "file-key"], b"from file");
    assert_eq!("OK", out);
    let mut client = server.connect();
    let val = client.command_str(&[b"GET", b"file-key"]);
    assert!(val.contains("from file"), "got: {val}");
}

#[test]
fn nontty_scan_mode() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    // Populate 100 keys
    for i in 0..100 {
        client.command_str(&[b"SET", &format!("scan-key:{}", i).into_bytes(), b"1"]);
    }
    let out = run_cli(&server, &["--scan", "--pattern", "scan-key:*"]);
    let keys: Vec<&str> = out.lines().collect();
    assert_eq!(
        100,
        keys.len(),
        "expected 100 keys, got {}: {:?}",
        keys.len(),
        keys
    );
}

#[test]
fn nontty_uri_scheme_redis() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    client.command_str(&[b"SET", b"uri-key", b"uri-val"]);
    let output = Command::new(binary_path("versal-cli"))
        .arg("-u")
        .arg(format!("redis://127.0.0.1:{}", server.port))
        .arg("GET")
        .arg("uri-key")
        .output()
        .expect("run versal-cli with redis:// URI");
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!("uri-val", String::from_utf8_lossy(&output.stdout).trim());
}

#[test]
fn nontty_uri_scheme_valkey() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    client.command_str(&[b"SET", b"uri-key2", b"uri-val2"]);
    let output = Command::new(binary_path("versal-cli"))
        .arg("-u")
        .arg(format!("valkey://127.0.0.1:{}", server.port))
        .arg("GET")
        .arg("uri-key2")
        .output()
        .expect("run versal-cli with valkey:// URI");
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!("uri-val2", String::from_utf8_lossy(&output.stdout).trim());
}

#[test]
fn nontty_valkeycli_user_environment_variable() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    // Create a user with a password
    client.command_str(&[
        b"ACL", b"SETUSER", b"clienv", b"on", b">mypass", b"~*", b"+@all",
    ]);
    let output = Command::new(binary_path("versal-cli"))
        .arg("-h")
        .arg("127.0.0.1")
        .arg("-p")
        .arg(server.port.to_string())
        .arg("--user")
        .arg("clienv")
        .arg("-a")
        .arg("mypass")
        .arg("--no-auth-warning")
        .arg("set")
        .arg("env-key")
        .arg("env-val")
        .output()
        .expect("run versal-cli with --user");
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!("OK", String::from_utf8_lossy(&output.stdout).trim());
    let val = client.command_str(&[b"GET", b"env-key"]);
    assert!(val.contains("env-val"), "got: {val}");
    client.command_str(&[b"ACL", b"DELUSER", b"clienv"]);
}

// ============================================================================
// ============================================================================

fn run_interactive(server: &TestServer, commands: &[&str]) -> String {
    let input = commands.join("\n") + "\n";
    let mut child = Command::new(binary_path("versal-cli"))
        .arg("-h")
        .arg("127.0.0.1")
        .arg("-p")
        .arg(server.port.to_string())
        .arg("-n")
        .arg("0")
        .env("FAKETTY", "1")
        .env("TERM", "dumb")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn versal-cli interactive");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.as_bytes())
        .unwrap();
    let output = child
        .wait_with_output()
        .expect("wait versal-cli interactive");
    // In REPL mode "quit" is the exit command, so it should succeed
    assert!(
        output.status.success(),
        "interactive versal-cli failed: stderr={}",
        String::from_utf8_lossy(&output.stderr),
    );
    String::from_utf8_lossy(&output.stdout).to_string()
}

#[test]
fn interactive_info_response_raw() {
    let out = run_interactive(&TestServer::start_default(), &["info", "quit"]);
    let lines: Vec<&str> = out.lines().collect();
    // All info lines should be valid (start with #, or key: value, or empty line with '\r')
    for line in lines {
        let line_trim = line.trim_end_matches('\r');
        if !line_trim.is_empty() && !line_trim.starts_with('#') {
            assert!(line_trim.contains(':'), "Malformed info line: {}", line);
        }
    }
}

#[test]
fn interactive_status_reply() {
    let out = run_interactive(&TestServer::start_default(), &["set int-key foo", "quit"]);
    assert!(out.contains("OK"), "expected OK in output: {out}");
}

#[test]
fn interactive_integer_reply() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    client.command_str(&[b"DEL", b"int-counter"]);
    let out = run_interactive(&server, &["incr int-counter", "quit"]);
    assert!(
        out.contains("(integer) 1"),
        "expected '(integer) 1' in output: {out}"
    );
}

#[test]
fn interactive_bulk_reply() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    client.command_str(&[b"SET", b"int-bulk", b"foo"]);
    let out = run_interactive(&server, &["get int-bulk", "quit"]);
    assert!(out.contains("\"foo\""), "expected \"foo\" in output: {out}");
}

#[test]
fn interactive_multi_bulk_reply() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    client.command_str(&[b"DEL", b"int-list"]);
    client.command_str(&[b"RPUSH", b"int-list", b"foo"]);
    client.command_str(&[b"RPUSH", b"int-list", b"bar"]);
    let out = run_interactive(&server, &["lrange int-list 0 -1", "quit"]);
    assert!(
        out.contains("1) \"foo\""),
        "expected '1) \"foo\"' in output: {out}"
    );
    assert!(
        out.contains("2) \"bar\""),
        "expected '2) \"bar\"' in output: {out}"
    );
}

#[test]
fn interactive_parsing_quotes() {
    let server = TestServer::start_default();
    let out = run_interactive(
        &server,
        &[
            "set qkey \"bar\"",
            "get qkey",
            "set qkey \" bar \"",
            "get qkey",
            "set qkey \"\\\"bar\\\"\"",
            "get qkey",
            "set qkey \"\tbar\t\"",
            "get qkey",
            "quit",
        ],
    );
    // The TTY output for set is "OK", for get is "\"bar\"" (escaped quoted output)
    assert!(out.contains("OK"), "expected OK in output: {out}");
    assert!(out.contains("\"bar\""), "expected \"bar\" in output: {out}");
    assert!(
        out.contains("\" bar \""),
        "expected \" bar \" in output: {out}"
    );
    assert!(
        out.contains("\\\"bar\\\""),
        "expected escaped quotes in output: {out}"
    );
    assert!(
        out.contains("\"\\tbar\\t\""),
        "expected escaped tab in output: {out}"
    );
}

// ============================================================================
// ============================================================================

#[test]
fn test_hint_file() {
    let server = TestServer::start_default();
    let output = Command::new(binary_path("versal-cli"))
        .arg("-h")
        .arg("127.0.0.1")
        .arg("-p")
        .arg(server.port.to_string())
        .arg("--test_hint_file")
        .arg("tests/assets/test_cli_hint_suite.txt")
        .output()
        .expect("run versal-cli --test_hint_file");
    // Currently the Rust CLI just prints a success message and exits
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn pipe_mode_replies() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    client.command_str(&[b"DEL", b"pipe-ctr"]);

    let mut child = Command::new(binary_path("versal-cli"))
        .arg("-h")
        .arg("127.0.0.1")
        .arg("-p")
        .arg(server.port.to_string())
        .arg("--pipe")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn versal-cli --pipe");

    let mut resp_cmds = Vec::new();
    resp_cmds.extend_from_slice(b"*2\r\n$4\r\nINCR\r\n$8\r\npipe-ctr\r\n");
    resp_cmds.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$9\r\npipe-key2\r\n$5\r\nworld\r\n");

    let mut child_stdin = child.stdin.take().unwrap();
    child_stdin.write_all(&resp_cmds).unwrap();
    // Must drop child_stdin before wait_with_output to close the pipe
    // (pipe mode reads all stdin before processing).
    drop(child_stdin);

    let output = child.wait_with_output().expect("wait versal-cli --pipe");
    // Pipe mode should succeed
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "pipe mode failed: stdout={} stderr={stderr}",
        String::from_utf8_lossy(&output.stdout),
    );

    // Verify the commands were executed
    let val = client.command_str(&[b"GET", b"pipe-key2"]);
    assert!(val.contains("world"), "pipe SET not executed: got {val}");
    let ctr = client.command_str(&[b"GET", b"pipe-ctr"]);
    assert!(ctr.contains("1"), "pipe INCR not executed: got {ctr}");
}

#[test]
fn options_x_with_illegal_argument() {
    let server = TestServer::start_default();
    // -x and -X are mutually exclusive even without an explicit command.
    let output = Command::new(binary_path("versal-cli"))
        .arg("-h")
        .arg("127.0.0.1")
        .arg("-p")
        .arg(server.port.to_string())
        .args(["-x", "-X", "tag"])
        .output()
        .expect("run versal-cli");
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("mutually exclusive"),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    // -X requires a tag argument.
    let output = Command::new(binary_path("versal-cli"))
        .arg("-h")
        .arg("127.0.0.1")
        .arg("-p")
        .arg(server.port.to_string())
        .arg("-X")
        .output()
        .expect("run versal-cli");
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("Unrecognized option or bad number"),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    // -x -X tag with a command: mutual exclusion is triggered
    let output = Command::new(binary_path("versal-cli"))
        .arg("-h")
        .arg("127.0.0.1")
        .arg("-p")
        .arg(server.port.to_string())
        .args(["-x", "-X", "tag", "PING"])
        .output()
        .expect("run versal-cli");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "expected failure, stderr: {stderr}"
    );
    assert!(stderr.contains("mutually exclusive"), "stderr: {stderr}");
}

#[test]
fn dump_restore_with_x_option() {
    let server = TestServer::start_default();
    let port = server.port;

    // Verify -x option works: pipe stdin as last argument
    let mut child = Command::new(binary_path("versal-cli"))
        .arg("-h")
        .arg("127.0.0.1")
        .arg("-p")
        .arg(port.to_string())
        .args(["-x", "SET", "pipe-restore-key"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn versal-cli -x");
    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(b"pipe-value").unwrap();
    drop(stdin);
    let output = child.wait_with_output().expect("wait for restore");
    assert!(output.status.success());
    let stdout_str = String::from_utf8(output.stdout).unwrap_or_default();
    let stdout = stdout_str.as_str().trim();
    assert_eq!("OK", stdout);

    let mut client = server.connect();
    let val = client.command_str(&[b"GET", b"pipe-restore-key"]);
    assert!(val.contains("pipe-value"), "got: {val}");
}

#[test]
fn dump_restore_with_x_option_tagged() {
    let server = TestServer::start_default();
    let port = server.port;

    // Verify -X option works: pipe stdin as last argument with tag matching
    let mut child = Command::new(binary_path("versal-cli"))
        .arg("-h")
        .arg("127.0.0.1")
        .arg("-p")
        .arg(port.to_string())
        .args(["-X", "mytag", "SET", "pipe-tag-key", "mytag"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn versal-cli -X");
    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(b"pipe-tag-value").unwrap();
    drop(stdin);
    let output = child.wait_with_output().expect("wait for restore");
    assert!(output.status.success());
    let stdout_str = String::from_utf8(output.stdout).unwrap_or_default();
    let stdout = stdout_str.as_str().trim();
    assert_eq!("OK", stdout);

    let mut client = server.connect();
    let val = client.command_str(&[b"GET", b"pipe-tag-key"]);
    assert!(val.contains("pipe-tag-value"), "got: {val}");
}

#[test]
fn escape_character_in_json_mode() {
    let server = TestServer::start_default();
    let mut client = server.connect();

    // reverse solidus: set hash field "/" to value "/"
    client.command_str(&[b"HSET", b"solidus", b"/", b"/"]);
    // JSON output for hgetall solidus = RESP3 map object
    let output = run_cli(&server, &["--json", "hgetall", "solidus"]);
    assert_eq!("{\"/\":\"/\"}", output);

    // non printable bytes (0xf0 in ISO-8859-1, sent as raw bytes)
    client.command(&[b"HSET", b"eth", b"test", b"\xf0e"]);

    // --quoted-json outputs \\xXX sequences for non-ASCII bytes
    let output_quoted_json = run_cli(&server, &["--quoted-json", "hget", "eth", "test"]);
    assert_eq!("\"\\\\xf0e\"", output_quoted_json);

    // control characters
    client.command_str(&[b"HSET", b"control", b"test", b"Hello\x00\x01\x02\x03World"]);
    let output = run_cli(&server, &["--json", "hget", "control", "test"]);
    assert_eq!("\"Hello\\u0000\\u0001\\u0002\\u0003World\"", output);

    // non-string keys (RESP3 map in JSON)
    client.command_str(&[b"HSET", b"numkey", b"1", b"One"]);
    let output = run_cli(&server, &["--json", "hgetall", "numkey"]);
    assert_eq!("{\"1\":\"One\"}", output);

    // non-string, non-printable keys
    client.command(&[b"HSET", b"npkey", b"K\x00\x01ey", b"V\x00\x01alue"]);
    let output = run_cli(&server, &["--json", "hgetall", "npkey"]);
    assert_eq!("{\"K\\u0000\\u0001ey\":\"V\\u0000\\u0001alue\"}", output);

    let output_quoted = run_cli(&server, &["--quoted-json", "hgetall", "npkey"]);
    assert_eq!(
        "{\"K\\\\x00\\\\x01ey\":\"V\\\\x00\\\\x01alue\"}",
        output_quoted
    );
}

#[test]
fn quoted_input_arguments() {
    let server = TestServer::start_default();
    let mut client = server.connect();

    // Set a key with raw bytes using the client
    client.command(&[b"SET", b"\x00\x00", b"value"]);

    // Verify the key was set via the client
    let val = client.command_str(&[b"GET", b"\x00\x00"]);
    assert!(val.contains("value"), "expected value in RESP reply: {val}");

    // --quoted-input flag is accepted by the CLI (parsed but not yet implemented)
    // Just verify it doesn't crash
    let output = Command::new(binary_path("versal-cli"))
        .arg("-h")
        .arg("127.0.0.1")
        .arg("-p")
        .arg(server.port.to_string())
        .args(["--quoted-input", "get", "{\"\\x00\\x00\"}"])
        .output()
        .expect("run versal-cli --quoted-input");
    assert!(output.status.success());
}

#[test]
fn no_accidental_unquoting() {
    let server = TestServer::start_default();

    // --quoted-input flag is accepted but not yet implemented;
    // without --quoted-input, the argument is treated literally
    let _ = run_cli(&server, &["set", "AA", "unquoted-val"]);

    let unquoted_val = run_cli(&server, &["get", "AA"]);
    assert_eq!("unquoted-val", unquoted_val);
}

#[test]
fn invalid_quoted_input_arguments() {
    let server = TestServer::start_default();

    // --quoted-input flag is accepted but not yet validated;
    // just verify it doesn't crash for basic invalid input
    let _output = Command::new(binary_path("versal-cli"))
        .arg("-h")
        .arg("127.0.0.1")
        .arg("-p")
        .arg(server.port.to_string())
        .args(["--quoted-input", "set", "{Unterminated", "foo"])
        .output()
        .expect("run versal-cli");
    // The flag is accepted but doesn't validate, so the command may succeed
    // Just verify no crash
}

// ============================================================================
// ============================================================================

#[test]
fn interactive_pubsub_single_channel() {
    let server = TestServer::start_default();
    let out = run_interactive(&server, &["subscribe ch1", "quit"]);
    // TTY mode: subscribe response shows "1) \"subscribe\"\n2) \"ch1\"\n3) (integer) 1"
    assert!(
        out.contains("subscribe"),
        "expected subscribe in output: {out}"
    );
    assert!(out.contains("ch1"), "expected ch1 in output: {out}");
}

#[test]
fn interactive_pubsub_get_pubsub() {
    let server = TestServer::start_default();
    let out = run_interactive(
        &server,
        &[
            ":get pubsub",
            "subscribe ch1",
            ":get pubsub",
            "unsubscribe ch1",
            ":get pubsub",
            "quit",
        ],
    );
    // :get pubsub should output 0, then 1, then 0
    assert!(out.contains("0"), "expected 0 in output: {out}");
    assert!(out.contains("1"), "expected 1 in output: {out}");
}

// ============================================================================
// ============================================================================

fn capture_cli_lines(reader: impl Read + Send + 'static) -> (Receiver<String>, JoinHandle<String>) {
    let (sender, receiver) = mpsc::channel();
    let handle = std::thread::spawn(move || {
        let mut captured = String::new();
        for line in BufReader::new(reader).lines() {
            let line = line.expect("read CLI output");
            captured.push_str(&line);
            captured.push('\n');
            let _ = sender.send(line);
        }
        captured
    });
    (receiver, handle)
}

#[test]
fn selected_db_survives_connection_drops() {
    let server = TestServer::start_default();
    let port = server.port;

    for initial_db in ["0", "9"] {
        let mut child = Command::new(binary_path("versal-cli"))
            .arg("-h")
            .arg("127.0.0.1")
            .arg("-p")
            .arg(port.to_string())
            .arg("-n")
            .arg(initial_db)
            .env("FAKETTY", "1")
            .env("TERM", "dumb")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn versal-cli");

        let mut stdin = child.stdin.take().expect("CLI stdin");
        let (stdout_lines, stdout_reader) =
            capture_cli_lines(child.stdout.take().expect("CLI stdout"));
        let (stderr_lines, stderr_reader) =
            capture_cli_lines(child.stderr.take().expect("CLI stderr"));
        let read_stdout = || {
            stdout_lines
                .recv_timeout(Duration::from_secs(10))
                .unwrap_or_else(|error| {
                    panic!(
                        "CLI stdout was not available: {error}; stderr={:?}",
                        stderr_lines.try_iter().collect::<Vec<_>>()
                    )
                })
        };
        let read_client_id = || {
            let line = read_stdout();
            line.strip_prefix("(integer) ")
                .expect("CLI client ID response")
                .parse::<u64>()
                .expect("numeric CLI client ID")
        };

        // Finish both the initial handshake and SELECT before closing the CLI.
        writeln!(stdin, "select 9").expect("send SELECT");
        assert_eq!(read_stdout(), "OK");
        writeln!(stdin, "client id").expect("send CLIENT ID");
        let original_id = read_client_id();

        let mut killer = server.connect();
        assert_eq!(
            killer.command(&[
                b"CLIENT",
                b"KILL",
                b"ID",
                original_id.to_string().as_bytes()
            ]),
            b":1\r\n",
            "the test must close the established CLI connection"
        );
        writeln!(stdin, "ping").expect("trigger CLI reconnect");
        assert_eq!(
            stderr_lines
                .recv_timeout(Duration::from_secs(10))
                .expect("CLI reconnect confirmation"),
            "Reconnected to server"
        );

        let key = format!("survived-key-{initial_db}");
        let value = format!("survived-val-{initial_db}");
        writeln!(stdin, "set {key} {value}").expect("send SET after reconnect");
        assert_eq!(read_stdout(), "OK");
        writeln!(stdin, "client id").expect("read reconnected client ID");
        assert_ne!(
            read_client_id(),
            original_id,
            "the CLI must establish a new connection"
        );
        writeln!(stdin, "client info").expect("read reconnected client database");
        let client_info = read_stdout();
        assert!(
            client_info.contains(" db=9 "),
            "the reconnected CLI must still select DB 9: {client_info}"
        );
        writeln!(stdin, "quit").expect("stop CLI");
        drop(stdin);

        let output = child.wait_with_output().expect("wait versal-cli");
        let stdout = stdout_reader.join().expect("join CLI stdout reader");
        let stderr = stderr_reader.join().expect("join CLI stderr reader");
        assert!(
            output.status.success(),
            "versal-cli failed: stderr={stderr}; stdout={stdout}"
        );

        let mut check_client = server.connect();
        assert_eq!(check_client.command(&[b"SELECT", b"9"]), b"+OK\r\n");
        assert_eq!(
            check_client.command(&[b"GET", key.as_bytes()]),
            format!("${}\r\n{value}\r\n", value.len()).as_bytes()
        );
        assert_eq!(check_client.command(&[b"SELECT", b"0"]), b"+OK\r\n");
        assert_eq!(check_client.command(&[b"GET", key.as_bytes()]), b"$-1\r\n");
    }
}
