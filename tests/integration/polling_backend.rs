//! Real-server coverage for the production Polling level-triggered backend.
//!
//! TLS listener, replication, Cluster Bus, and Sentinel LT coverage stays in
//! the existing large scenarios instead of being duplicated here:
//! `config_compat::tls_listener_serves_resp_over_the_configured_tls_port`,
//! `config_compat::replica_syncs_over_the_configured_tls_upstream`,
//! `config_compat::cluster_meet_uses_the_configured_tls_bus_transport`, and the
//! `sentinel` integration module.

use crate::common::TestServer;
#[cfg(target_os = "linux")]
use socket2::SockRef;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

const PING_REQUEST: &[u8] = b"*1\r\n$4\r\nPING\r\n";

fn info_counter(reply: &str, field: &str) -> u64 {
    reply
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            (name == field).then(|| value.trim_end_matches('\r').parse::<u64>().ok())?
        })
        .unwrap_or_else(|| panic!("missing or invalid {field} in INFO reply: {reply}"))
}

fn append_command(output: &mut Vec<u8>, parts: &[&[u8]]) {
    output.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
    for part in parts {
        output.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        output.extend_from_slice(part);
        output.extend_from_slice(b"\r\n");
    }
}

fn connect_with_timeout(port: u16, timeout: Duration) -> TcpStream {
    let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect to test server");
    stream
        .set_read_timeout(Some(timeout))
        .expect("set read timeout");
    stream
        .set_write_timeout(Some(timeout))
        .expect("set write timeout");
    stream
}

fn assert_prompt_ping(stream: &mut TcpStream, limit: Duration) -> Duration {
    let started = Instant::now();
    stream.write_all(PING_REQUEST).expect("write control PING");
    let mut reply = [0_u8; 7];
    stream.read_exact(&mut reply).expect("read control PING");
    let elapsed = started.elapsed();
    assert_eq!(&reply, b"+PONG\r\n");
    assert!(
        elapsed < limit,
        "control PING took {elapsed:?}, exceeding the {limit:?} fairness limit"
    );
    elapsed
}

fn wait_for_poll_counter_growth(server: &TestServer, initial: u64) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mut client = server.connect();
        let info = client.command_str(&[b"INFO", b"stats"]);
        let current = info_counter(&info, "io_threaded_poll_processed");
        if current > initial {
            return current;
        }
        assert!(
            Instant::now() < deadline,
            "worker-owned poll counter did not grow from {initial}; INFO={info:?}; logs:\n{}",
            server.server_log()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn large_pipeline_and_replies_preserve_second_client_fairness() {
    let server = TestServer::start_default();
    let mut setup = server.connect();
    let value = vec![b'x'; 16 * 1024];
    assert_eq!(
        setup.command(&[b"SET", b"polling-fair-value", &value]),
        b"+OK\r\n"
    );

    let mut pipeline = Vec::new();
    for _ in 0..2_048 {
        append_command(&mut pipeline, &[b"GET", b"polling-fair-value"]);
    }
    assert!(
        pipeline.len() > 64 * 1024,
        "pipeline must cross the production per-read budget"
    );

    let mut busy = server.connect();
    busy.send_raw(&pipeline);
    let mut control = connect_with_timeout(server.port, Duration::from_secs(1));
    let elapsed = assert_prompt_ping(&mut control, Duration::from_secs(1));
    eprintln!(
        "polling fairness diagnostic: pipeline_bytes={}, reply_bytes={}, control_ping={elapsed:?}",
        pipeline.len(),
        value.len() * 2_048
    );
}

#[cfg(target_os = "linux")]
fn linux_clock_ticks_per_second() -> u64 {
    let output = std::process::Command::new("getconf")
        .arg("CLK_TCK")
        .output()
        .expect("run getconf CLK_TCK");
    assert!(
        output.status.success(),
        "getconf CLK_TCK failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("CLK_TCK output must be UTF-8")
        .trim()
        .parse()
        .expect("CLK_TCK output must be an integer")
}

#[cfg(target_os = "linux")]
fn linux_process_cpu_ticks(pid: u32) -> u64 {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .unwrap_or_else(|error| panic!("read /proc/{pid}/stat: {error}"));
    let comm_end = stat
        .rfind(')')
        .unwrap_or_else(|| panic!("invalid /proc/{pid}/stat: {stat}"));
    let fields = stat[comm_end + 1..].split_whitespace().collect::<Vec<_>>();
    let user_ticks = fields
        .get(11)
        .unwrap_or_else(|| panic!("missing utime in /proc/{pid}/stat"))
        .parse::<u64>()
        .expect("utime must be an integer");
    let system_ticks = fields
        .get(12)
        .unwrap_or_else(|| panic!("missing stime in /proc/{pid}/stat"))
        .parse::<u64>()
        .expect("stime must be an integer");
    user_ticks + system_ticks
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
struct BackpressureStall {
    settle_elapsed: Duration,
    stable_samples: usize,
    output_bytes: u64,
    pending_reply_bytes: u64,
}

#[cfg(target_os = "linux")]
fn client_list_counter(line: &str, field: &str) -> Option<u64> {
    let prefix = format!("{field}=");
    line.split_whitespace()
        .find_map(|item| item.strip_prefix(&prefix))?
        .parse()
        .ok()
}

#[cfg(target_os = "linux")]
fn linux_tcp_bytes_sent(server_port: u16, client_port: u16) -> Option<u64> {
    let filter = format!("sport = :{server_port} and dport = :{client_port}");
    let output = std::process::Command::new("ss")
        .args(["-tinH", &filter])
        .output()
        .expect("run ss for TCP_INFO bytes_sent");
    assert!(
        output.status.success(),
        "ss TCP_INFO query failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .find_map(|item| item.strip_prefix("bytes_sent:"))?
        .parse()
        .ok()
}

#[cfg(target_os = "linux")]
fn wait_for_non_reader_stall(
    server: &TestServer,
    client_name: &str,
    client_port: u16,
    complete_reply_bytes: u64,
) -> BackpressureStall {
    const REQUIRED_STABLE_SAMPLES: usize = 5;
    const SAMPLE_INTERVAL: Duration = Duration::from_millis(25);
    const DEADLINE: Duration = Duration::from_secs(5);

    let started = Instant::now();
    let deadline = started + DEADLINE;
    let name_field = format!("name={client_name}");
    let mut previous_output = None;
    let mut stable_samples = 0;
    loop {
        let mut admin = server.connect();
        let last_list = admin.command_str(&[b"CLIENT", b"LIST"]);
        let target = last_list.lines().find(|line| {
            line.split_whitespace()
                .any(|item| item == name_field.as_str())
        });
        if let Some(line) = target {
            let pending_reply_bytes = client_list_counter(line, "omem")
                .unwrap_or_else(|| panic!("missing omem in CLIENT LIST row: {line}"));
            let total_output_bytes = client_list_counter(line, "tot-net-out")
                .unwrap_or_else(|| panic!("missing tot-net-out in CLIENT LIST row: {line}"));
            if pending_reply_bytes == 0 && total_output_bytes >= complete_reply_bytes {
                return BackpressureStall {
                    settle_elapsed: started.elapsed(),
                    stable_samples,
                    output_bytes: total_output_bytes,
                    pending_reply_bytes,
                };
            }
            if let Some(output_bytes) = linux_tcp_bytes_sent(server.port, client_port) {
                if pending_reply_bytes > 0 && previous_output == Some(output_bytes) {
                    stable_samples += 1;
                } else {
                    stable_samples = 0;
                }
                previous_output = Some(output_bytes);
                if stable_samples >= REQUIRED_STABLE_SAMPLES {
                    return BackpressureStall {
                        settle_elapsed: started.elapsed(),
                        stable_samples,
                        output_bytes,
                        pending_reply_bytes,
                    };
                }
            } else {
                stable_samples = 0;
                previous_output = None;
            }
        } else {
            previous_output = None;
            stable_samples = 0;
        }

        assert!(
            Instant::now() < deadline,
            "non-reading client {client_name} did not reach steady backpressure within {DEADLINE:?}; stable_samples={stable_samples}; CLIENT LIST={last_list:?}; logs:\n{}",
            server.server_log()
        );
        std::thread::sleep(SAMPLE_INTERVAL);
    }
}

#[cfg(target_os = "linux")]
#[test]
fn non_reading_large_reply_does_not_spin_at_full_core() {
    let server = TestServer::start_with_env(
        &[],
        &[
            ("VALKEY_TEST_TCP_SNDBUF_BYTES", "65536"),
            ("VALKEY_TEST_TCP_RCVBUF_BYTES", "1048576"),
        ],
    );
    let mut setup = server.connect();
    let value = vec![b'y'; 32 * 1024 * 1024];
    assert_eq!(
        setup.command(&[b"SET", b"polling-backpressure-value", &value]),
        b"+OK\r\n"
    );

    const NON_READER_NAME: &[u8] = b"polling-backpressure-non-reader";
    let mut non_reader = connect_with_timeout(server.port, Duration::from_secs(5));
    SockRef::from(&non_reader)
        .set_recv_buffer_size(4096)
        .expect("shrink non-reading client receive buffer");
    let mut setname_request = Vec::new();
    append_command(
        &mut setname_request,
        &[b"CLIENT", b"SETNAME", NON_READER_NAME],
    );
    non_reader
        .write_all(&setname_request)
        .expect("name non-reading client");
    let mut setname_reply = [0_u8; 5];
    non_reader
        .read_exact(&mut setname_reply)
        .expect("read CLIENT SETNAME reply");
    assert_eq!(&setname_reply, b"+OK\r\n");
    non_reader
        .write_all(b"*2\r\n$3\r\nGET\r\n$26\r\npolling-backpressure-value\r\n")
        .expect("request large reply");
    let stall = wait_for_non_reader_stall(
        &server,
        std::str::from_utf8(NON_READER_NAME).expect("client name must be UTF-8"),
        non_reader
            .local_addr()
            .expect("read non-reading client local address")
            .port(),
        value.len() as u64,
    );

    let ticks_per_second = linux_clock_ticks_per_second();
    let ticks_before = linux_process_cpu_ticks(server.pid());
    let sample_started = Instant::now();
    std::thread::sleep(Duration::from_secs(1));
    let sample_elapsed = sample_started.elapsed();
    let ticks_after = linux_process_cpu_ticks(server.pid());
    let consumed_ticks = ticks_after.saturating_sub(ticks_before);
    let full_core_ticks = (ticks_per_second as f64 * sample_elapsed.as_secs_f64()).ceil() as u64;
    let allowed_ticks = (full_core_ticks * 4 / 5).saturating_add(2);
    let mut control = connect_with_timeout(server.port, Duration::from_millis(250));
    let post_sample_ping = assert_prompt_ping(&mut control, Duration::from_millis(250));
    eprintln!(
        "polling backpressure CPU diagnostic: pid={}, settle={:?}, stable_samples={}, output_bytes={}, pending_reply_bytes={}, clk_tck={}, elapsed={sample_elapsed:?}, cpu_ticks={}, full_core_ticks={}, limit={}, post_sample_ping={post_sample_ping:?}",
        server.pid(),
        stall.settle_elapsed,
        stall.stable_samples,
        stall.output_bytes,
        stall.pending_reply_bytes,
        ticks_per_second,
        consumed_ticks,
        full_core_ticks,
        allowed_ticks,
    );
    assert!(
        consumed_ticks <= allowed_ticks,
        "non-reading peer consumed {consumed_ticks} CPU ticks in {sample_elapsed:?}; full-core estimate={full_core_ticks}, allowed={allowed_ticks}"
    );
}

#[test]
fn always_active_workers_process_real_poll_jobs() {
    let server = TestServer::start(&["--io-threads", "3", "--io-threads-always-active", "yes"]);
    let mut admin = server.connect();
    let initial = info_counter(
        &admin.command_str(&[b"INFO", b"stats"]),
        "io_threaded_poll_processed",
    );

    let payload = vec![b'p'; 2 * 1024 * 1024];
    let mut traffic = server.connect();
    traffic.send(&[b"ECHO", &payload]);
    let current = wait_for_poll_counter_growth(&server, initial);
    assert!(current > initial, "real worker poll counter must increase");
}

#[test]
fn shutdown_wakes_an_idle_worker_owned_poll_and_exits_cleanly() {
    let mut server = TestServer::start(&["--io-threads", "3", "--io-threads-always-active", "yes"]);
    let mut setup = server.connect();
    let value = vec![b's'; 16 * 1024 * 1024];
    assert_eq!(
        setup.command(&[b"SET", b"polling-shutdown-value", &value]),
        b"+OK\r\n"
    );
    let initial = info_counter(
        &setup.command_str(&[b"INFO", b"stats"]),
        "io_threaded_poll_processed",
    );

    let mut non_reader = connect_with_timeout(server.port, Duration::from_secs(5));
    non_reader
        .write_all(b"*2\r\n$3\r\nGET\r\n$22\r\npolling-shutdown-value\r\n")
        .expect("request backpressured reply");
    wait_for_poll_counter_growth(&server, initial);
    std::thread::sleep(Duration::from_millis(50));

    let mut shutdown = connect_with_timeout(server.port, Duration::from_secs(1));
    shutdown
        .write_all(b"*2\r\n$8\r\nSHUTDOWN\r\n$6\r\nNOSAVE\r\n")
        .expect("send shutdown while poll is worker-owned");
    assert_eq!(
        server.wait_exit(Duration::from_secs(5)),
        Some(0),
        "SHUTDOWN NOSAVE must wake the offloaded poll and exit cleanly; logs:\n{}",
        server.server_log()
    );
}
