use crate::common::{Client, TestServer};
use std::time::{Duration, Instant};

fn info_value(reply: &str, field: &str) -> Option<String> {
    reply.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        (name == field).then(|| value.trim_end_matches('\r').to_string())
    })
}

fn wait_for_worker_times(port: u16, workers: usize) -> Vec<f64> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mut client = Client::connect_to(port);
        let info = client.command_str(&[b"INFO", b"cpu"]);
        let times = (1..workers)
            .map(|id| {
                info_value(&info, &format!("used_active_time_io_thread_{id}"))
                    .and_then(|value| value.parse::<f64>().ok())
                    .unwrap_or(0.0)
            })
            .collect::<Vec<_>>();
        if times.iter().all(|time| *time > 0.0) {
            return times;
        }
        assert!(
            Instant::now() < deadline,
            "workers never reported activity: {info}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(target_os = "linux")]
fn info_counter(reply: &str, field: &str) -> u64 {
    info_value(reply, field)
        .unwrap_or_else(|| panic!("missing {field} in INFO reply: {reply}"))
        .parse()
        .unwrap_or_else(|error| panic!("invalid {field} in INFO reply: {error}; {reply}"))
}

#[cfg(target_os = "linux")]
fn run_increment_pipeline(client: &mut Client, key: &[u8], count: u64) {
    let mut pipeline = Vec::new();
    for _ in 0..count {
        pipeline.extend_from_slice(b"*2\r\n$4\r\nINCR\r\n$");
        pipeline.extend_from_slice(key.len().to_string().as_bytes());
        pipeline.extend_from_slice(b"\r\n");
        pipeline.extend_from_slice(key);
        pipeline.extend_from_slice(b"\r\n");
    }
    client.send_raw(&pipeline);
    for expected in 1..=count {
        assert_eq!(
            client.read_reply(),
            format!(":{expected}\r\n").into_bytes(),
            "pipeline replies must preserve FIFO command order"
        );
    }
}

#[cfg(target_os = "linux")]
fn linux_io_worker_names(pid: u32) -> Vec<String> {
    let mut names = std::fs::read_dir(format!("/proc/{pid}/task"))
        .unwrap_or_else(|error| panic!("read /proc/{pid}/task: {error}"))
        .filter_map(Result::ok)
        .filter_map(|task| std::fs::read_to_string(task.path().join("comm")).ok())
        .map(|name| name.trim().to_string())
        .filter(|name| name.starts_with("io_thd_"))
        .collect::<Vec<_>>();
    names.sort();
    names
}

#[cfg(target_os = "linux")]
fn wait_for_linux_io_worker_count(pid: u32, expected: usize) -> Vec<String> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let names = linux_io_worker_names(pid);
        if names.len() == expected {
            return names;
        }
        assert!(
            Instant::now() < deadline,
            "expected {expected} IO workers for pid {pid}, observed {names:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(target_os = "linux")]
#[test]
fn one_io_thread_is_an_exact_process_level_synchronous_fallback() {
    let server = TestServer::start(&["--io-threads", "1"]);
    assert!(
        linux_io_worker_names(server.pid()).is_empty(),
        "io-threads=1 must not create io_thd_ workers"
    );

    let mut client = server.connect();
    run_increment_pipeline(&mut client, b"single-thread-pipeline", 128);
    let info = client.command_str(&[b"INFO", b"stats"]);
    for field in [
        "io_threaded_reads_processed",
        "io_threaded_writes_processed",
        "io_threaded_poll_processed",
    ] {
        assert_eq!(
            info_counter(&info, field),
            0,
            "{field} must stay zero in the exact synchronous fallback; INFO={info:?}"
        );
    }
}

#[cfg(target_os = "linux")]
#[test]
fn one_many_one_resize_removes_workers_and_resumes_synchronous_io() {
    let server = TestServer::start(&["--io-threads", "1"]);
    let mut admin = server.connect();
    assert!(linux_io_worker_names(server.pid()).is_empty());
    assert_eq!(
        admin.command(&[b"CONFIG", b"SET", b"io-threads-always-active", b"yes"]),
        b"+OK\r\n"
    );
    assert_eq!(
        admin.command(&[b"CONFIG", b"SET", b"io-threads", b"4"]),
        b"+OK\r\n"
    );
    assert_eq!(
        wait_for_linux_io_worker_count(server.pid(), 3),
        ["io_thd_1", "io_thd_2", "io_thd_3"]
    );
    run_increment_pipeline(&mut admin, b"worker-pipeline", 256);

    assert_eq!(
        admin.command(&[b"CONFIG", b"SET", b"io-threads", b"1"]),
        b"+OK\r\n"
    );
    assert!(
        wait_for_linux_io_worker_count(server.pid(), 0).is_empty(),
        "1 -> N -> 1 must join every io_thd_ worker"
    );
    let before = admin.command_str(&[b"INFO", b"stats"]);
    let counters_before = [
        info_counter(&before, "io_threaded_reads_processed"),
        info_counter(&before, "io_threaded_writes_processed"),
        info_counter(&before, "io_threaded_poll_processed"),
    ];
    run_increment_pipeline(&mut admin, b"resumed-sync-pipeline", 128);
    assert_eq!(admin.command(&[b"PING"]), b"+PONG\r\n");
    let after = admin.command_str(&[b"INFO", b"stats"]);
    let counters_after = [
        info_counter(&after, "io_threaded_reads_processed"),
        info_counter(&after, "io_threaded_writes_processed"),
        info_counter(&after, "io_threaded_poll_processed"),
    ];
    assert_eq!(
        counters_after, counters_before,
        "synchronous traffic after downsize must not increment worker counters"
    );
}

#[test]
fn real_workers_survive_dynamic_five_one_five_resize() {
    let server = TestServer::start(&["--io-threads", "5"]);
    let mut admin = server.connect();

    assert_eq!(
        admin.command(&[b"CONFIG", b"SET", b"io-threads-always-active", b"yes"]),
        b"+OK\r\n"
    );

    let threads = (0..16)
        .map(|_| {
            let port = server.port;
            std::thread::spawn(move || {
                let mut client = Client::connect_to(port);
                for _ in 0..32 {
                    let reply = client.command(&[b"INCR", b"io-thread-counter"]);
                    assert_eq!(reply.first(), Some(&b':'));
                }
            })
        })
        .collect::<Vec<_>>();
    for thread in threads {
        thread.join().unwrap();
    }

    let before = wait_for_worker_times(server.port, 5);
    assert_eq!(
        admin.command(&[b"CONFIG", b"SET", b"io-threads", b"1"]),
        b"+OK\r\n"
    );
    assert_eq!(admin.command(&[b"PING"]), b"+PONG\r\n");
    let cpu = admin.command_str(&[b"INFO", b"cpu"]);
    assert!(!cpu.contains("used_active_time_io_thread_1:"), "{cpu}");

    assert_eq!(
        admin.command(&[b"CONFIG", b"SET", b"io-threads", b"5"]),
        b"+OK\r\n"
    );
    let after = wait_for_worker_times(server.port, 5);
    for (before, after) in before.into_iter().zip(after) {
        assert!(after >= before);
    }
}
