use std::process::Command;

use crate::common::TestServer;

#[test]
fn benchmark_ratio_one_to_one_runs_set_and_get() {
    let server = TestServer::start_default();
    let output = Command::new(binary_path("versal-benchmark"))
        .arg("-h")
        .arg("127.0.0.1")
        .arg("-p")
        .arg(server.port.to_string())
        .arg("--ratio")
        .arg("1:1")
        .arg("--requests")
        .arg("20")
        .arg("-c")
        .arg("1")
        .arg("--key-maximum")
        .arg("10")
        .output()
        .expect("run versal-benchmark");
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_command_calls(&server, "set", 10);
    assert_command_calls(&server, "get", 10);
}

#[test]
fn benchmark_ratio_one_to_zero_runs_only_set() {
    let server = TestServer::start_default();
    let output = Command::new(binary_path("versal-benchmark"))
        .arg("-h")
        .arg("127.0.0.1")
        .arg("-p")
        .arg(server.port.to_string())
        .arg("--ratio")
        .arg("1:0")
        .arg("--requests")
        .arg("20")
        .arg("-c")
        .arg("1")
        .arg("--key-maximum")
        .arg("10")
        .output()
        .expect("run versal-benchmark");
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_command_calls(&server, "set", 20);
    assert_command_calls(&server, "get", 0);
}

#[test]
fn benchmark_ratio_zero_to_one_runs_only_get() {
    let server = TestServer::start_default();
    let mut client = server.connect();
    let reply = client.command_str(&[b"SET", b"memtier-0", b"x"]);
    assert!(reply.starts_with("+OK"), "unexpected SET reply: {reply}");

    let output = Command::new(binary_path("versal-benchmark"))
        .arg("-h")
        .arg("127.0.0.1")
        .arg("-p")
        .arg(server.port.to_string())
        .arg("--ratio")
        .arg("0:1")
        .arg("--requests")
        .arg("20")
        .arg("-c")
        .arg("1")
        .arg("--key-minimum")
        .arg("0")
        .arg("--key-maximum")
        .arg("0")
        .output()
        .expect("run versal-benchmark");
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_command_calls(&server, "set", 1);
    assert_command_calls(&server, "get", 20);
}

#[test]
fn benchmark_command_mode_replaces_key_and_data() {
    let server = TestServer::start_default();
    let output = Command::new(binary_path("versal-benchmark"))
        .arg("-h")
        .arg("127.0.0.1")
        .arg("-p")
        .arg(server.port.to_string())
        .arg("--requests")
        .arg("5")
        .arg("--command")
        .arg("SET __key__ __data__")
        .arg("--data-size")
        .arg("7")
        .arg("--key-pattern")
        .arg("S:S")
        .output()
        .expect("run versal-benchmark");
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_command_calls(&server, "set", 5);
}

#[test]
fn benchmark_command_error_returns_nonzero_exit() {
    let server = TestServer::start_default();
    let output = Command::new(binary_path("versal-benchmark"))
        .arg("-h")
        .arg("127.0.0.1")
        .arg("-p")
        .arg(server.port.to_string())
        .arg("--requests=1")
        .arg("--command=GET key extra")
        .output()
        .expect("run versal-benchmark");
    assert!(
        !output.status.success(),
        "benchmark should fail on Redis command errors; stdout={}; stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Redis command error"),
        "expected Redis command error in stderr; stderr={stderr}"
    );
}

#[test]
fn benchmark_json_output_contains_totals() {
    let server = TestServer::start_default();
    let json_path = temp_file("benchmark-summary.json");
    let output = Command::new(binary_path("versal-benchmark"))
        .arg("-h")
        .arg("127.0.0.1")
        .arg("-p")
        .arg(server.port.to_string())
        .arg("--ratio")
        .arg("1:0")
        .arg("--requests")
        .arg("3")
        .arg("--json-out-file")
        .arg(&json_path)
        .output()
        .expect("run versal-benchmark");
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let json = std::fs::read_to_string(json_path).unwrap();
    assert!(json.contains("\"total_requests\":3"));
    assert!(json.contains("\"name\":\"SET\""));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Memtier Redis Benchmark"),
        "expected normal console output with JSON file; stdout={stdout}"
    );
}

fn binary_path(name: &str) -> std::path::PathBuf {
    let mut path = std::path::PathBuf::from(env!("CARGO_BIN_EXE_versal-server"));
    path.set_file_name(name);
    path
}

#[test]
fn benchmark_rps_caps_request_rate() {
    let server = TestServer::start_default();
    let start = std::time::Instant::now();
    let output = Command::new(binary_path("versal-benchmark"))
        .arg("-h")
        .arg("127.0.0.1")
        .arg("-p")
        .arg(server.port.to_string())
        .arg("-c")
        .arg("1")
        .arg("-n")
        .arg("40")
        .arg("-t")
        .arg("set")
        .arg("--rps")
        .arg("40")
        .output()
        .expect("run versal-benchmark");
    let elapsed = start.elapsed();
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_command_calls(&server, "set", 40);
    assert!(
        elapsed >= std::time::Duration::from_millis(700),
        "expected --rps to pace requests; elapsed={elapsed:?}"
    );
}

#[test]
fn benchmark_mget_default_test_runs() {
    let server = TestServer::start_default();
    let output = Command::new(binary_path("versal-benchmark"))
        .arg("-h")
        .arg("127.0.0.1")
        .arg("-p")
        .arg(server.port.to_string())
        .arg("-c")
        .arg("1")
        .arg("-n")
        .arg("5")
        .arg("-t")
        .arg("mget")
        .output()
        .expect("run versal-benchmark");
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_command_calls(&server, "mget", 5);
}

#[test]
fn benchmark_xadd_default_test_runs() {
    let server = TestServer::start_default();
    let output = Command::new(binary_path("versal-benchmark"))
        .arg("-h")
        .arg("127.0.0.1")
        .arg("-p")
        .arg(server.port.to_string())
        .arg("-c")
        .arg("1")
        .arg("-n")
        .arg("3")
        .arg("-t")
        .arg("xadd")
        .output()
        .expect("run versal-benchmark");
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_command_calls(&server, "xadd", 3);
}

#[test]
fn benchmark_function_load_registers_library() {
    let server = TestServer::start_default();
    let output = Command::new(binary_path("versal-benchmark"))
        .arg("-h")
        .arg("127.0.0.1")
        .arg("-p")
        .arg(server.port.to_string())
        .arg("-c")
        .arg("1")
        .arg("-n")
        .arg("1")
        .arg("-t")
        .arg("function_load")
        .output()
        .expect("run versal-benchmark");
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut client = server.connect();
    let reply = client.command_str(&[b"FUNCTION", b"LIST"]);
    assert!(
        reply.contains("benchlib"),
        "expected benchlib in FUNCTION LIST reply: {reply}"
    );
}

#[test]
fn benchmark_fcall_invokes_registered_function() {
    let server = TestServer::start_default();
    let output = Command::new(binary_path("versal-benchmark"))
        .arg("-h")
        .arg("127.0.0.1")
        .arg("-p")
        .arg(server.port.to_string())
        .arg("-c")
        .arg("1")
        .arg("-n")
        .arg("5")
        .arg("-t")
        .arg("fcall")
        .output()
        .expect("run versal-benchmark");
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_command_calls(&server, "fcall", 5);
}

fn temp_file(name: &str) -> std::path::PathBuf {
    let unique = format!(
        "{}-{}-{name}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    std::env::temp_dir().join(unique)
}

fn assert_command_calls(server: &TestServer, command: &str, expected: u64) {
    let mut client = server.connect();
    let info = client.command_str(&[b"INFO", b"commandstats"]);
    let actual = command_calls(&info, command);
    assert_eq!(
        actual, expected,
        "expected {expected} {command} calls in INFO commandstats, got {actual}; info={info}"
    );
}

fn command_calls(info: &str, command: &str) -> u64 {
    let prefix = format!("cmdstat_{command}:calls=");
    for line in info.lines() {
        if let Some(rest) = line.strip_prefix(&prefix) {
            let calls = rest.split(',').next().unwrap_or("0");
            return calls.parse().unwrap_or(0);
        }
    }
    0
}
