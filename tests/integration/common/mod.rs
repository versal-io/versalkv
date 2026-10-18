#![allow(dead_code)]

use std::ffi::OsString;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Run a command that is expected to terminate, killing it at the deadline.
/// Startup-rejection tests must fail promptly when the server unexpectedly
/// accepts an invalid config instead of hanging the entire integration job.
pub fn command_status_with_timeout(
    command: &mut Command,
    timeout: Duration,
) -> std::io::Result<Option<ExitStatus>> {
    let mut child = command.spawn()?;
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait()? {
            return Ok(Some(status));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let _ = child.kill();
    let _ = child.wait();
    Ok(None)
}

pub struct TestServer {
    child: Child,
    pub port: u16,
    data_dir: PathBuf,
}

impl TestServer {
    pub fn start(extra_args: &[&str]) -> TestServer {
        TestServer::spawn_with_retry(extra_args, &[], make_test_dir, true)
    }

    pub fn start_default() -> TestServer {
        TestServer::start(&[])
    }

    pub fn start_with_env(extra_args: &[&str], env: &[(&str, &str)]) -> TestServer {
        let env = env
            .iter()
            .map(|(key, value)| (OsString::from(key), OsString::from(value)))
            .collect::<Vec<_>>();
        TestServer::spawn_with_retry(extra_args, &env, make_test_dir, true)
    }

    pub fn start_in_dir(data_dir: PathBuf, extra_args: &[&str]) -> TestServer {
        TestServer::spawn_with_retry(extra_args, &[], move || data_dir.clone(), false)
    }

    fn spawn_with_retry(
        extra_args: &[&str],
        env: &[(OsString, OsString)],
        mut next_dir: impl FnMut() -> PathBuf,
        owns_dir: bool,
    ) -> TestServer {
        const MAX_ATTEMPTS: u32 = 5;
        let bin = env!("CARGO_BIN_EXE_versal-server");
        let mut last_err = String::new();
        let debug_server_url = std::env::var("DEBUG_SERVER_URL").ok();
        for attempt in 0..MAX_ATTEMPTS {
            let guard = SERVER_START_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            let port = free_port();
            let data_dir = next_dir();
            let port_str = port.to_string();
            let mut args: Vec<&str> = vec![
                "--port",
                &port_str,
                "--bind",
                "127.0.0.1",
                "--loglevel",
                "notice",
                // Integration tests exercise DEBUG and protected CONFIG
                // paths explicitly; keep production defaults restrictive.
                "--enable-debug-command",
                "yes",
                "--enable-protected-configs",
                "yes",
            ];
            args.extend_from_slice(extra_args);

            let stderr_path = data_dir.join("versal-server.stderr");
            let stdout_path = data_dir.join("versal-server.stdout");
            let stderr_file = std::fs::File::create(&stderr_path)
                .expect("failed to create child stderr capture file");
            let stdout_file = std::fs::File::create(&stdout_path)
                .expect("failed to create child stdout capture file");
            // #region debug-point A:first-or-restart
            let rdb_meta = std::fs::metadata(data_dir.join("dump.rdb"))
                .ok()
                .map(|meta| meta.len());
            if let Some(debug_server_url) = debug_server_url.as_deref() {
                let _ = Command::new("curl").args(["-sS", "-o", "/dev/null", "--noproxy", "*", "-H", "Content-Type: application/json", "-X", "POST", debug_server_url, "-d", &format!("{{\"sessionId\":\"sigterm-startup-segv\",\"runId\":\"pre-fix\",\"hypothesisId\":\"A\",\"location\":\"tests/integration/common/mod.rs:spawn-before\",\"msg\":\"[DEBUG] spawning test server\",\"data\":{{\"attempt\":{},\"ownsDir\":{},\"port\":{},\"rdbBytes\":{}}}}}", attempt + 1, owns_dir, port, rdb_meta.unwrap_or(0))]).status();
            }
            // #endregion
            let mut child = Command::new(bin)
                .args(&args)
                .envs(env.iter().map(|(key, value)| (key, value)))
                .current_dir(&data_dir)
                .stdout(Stdio::from(stdout_file))
                .stderr(Stdio::from(stderr_file))
                .spawn()
                .expect("failed to spawn versal-server binary");
            // #region debug-point D:child-identity
            if let Some(debug_server_url) = debug_server_url.as_deref() {
                let _ = Command::new("curl").args(["-sS", "-o", "/dev/null", "--noproxy", "*", "-H", "Content-Type: application/json", "-X", "POST", debug_server_url, "-d", &format!("{{\"sessionId\":\"sigterm-startup-segv\",\"runId\":\"pre-fix\",\"hypothesisId\":\"D\",\"location\":\"tests/integration/common/mod.rs:spawn-after\",\"msg\":\"[DEBUG] spawned test server\",\"data\":{{\"attempt\":{},\"pid\":{},\"port\":{}}}}}", attempt + 1, child.id(), port)]).status();
            }
            // #endregion

            match wait_child_ready(&mut child, port) {
                Ok(()) => {
                    // #region debug-point C:ready
                    if let Some(debug_server_url) = debug_server_url.as_deref() {
                        let _ = Command::new("curl").args(["-sS", "-o", "/dev/null", "--noproxy", "*", "-H", "Content-Type: application/json", "-X", "POST", debug_server_url, "-d", &format!("{{\"sessionId\":\"sigterm-startup-segv\",\"runId\":\"pre-fix\",\"hypothesisId\":\"C\",\"location\":\"tests/integration/common/mod.rs:ready\",\"msg\":\"[DEBUG] test server ready\",\"data\":{{\"attempt\":{},\"pid\":{},\"port\":{},\"ownsDir\":{}}}}}", attempt + 1, child.id(), port, owns_dir)]).status();
                    }
                    // #endregion
                    drop(guard);
                    return TestServer {
                        child,
                        port,
                        data_dir,
                    };
                }
                Err(err) => {
                    // #region debug-point B:startup-failed
                    let stderr = std::fs::read_to_string(&stderr_path).unwrap_or_default();
                    if let Some(debug_server_url) = debug_server_url.as_deref() {
                        let _ = Command::new("curl").args(["-sS", "-o", "/dev/null", "--noproxy", "*", "-H", "Content-Type: application/json", "-X", "POST", debug_server_url, "-d", &format!("{{\"sessionId\":\"sigterm-startup-segv\",\"runId\":\"pre-fix\",\"hypothesisId\":\"B\",\"location\":\"tests/integration/common/mod.rs:ready-error\",\"msg\":\"[DEBUG] test server startup failed\",\"data\":{{\"attempt\":{},\"pid\":{},\"port\":{},\"ownsDir\":{},\"error\":{:?},\"stderr\":{:?}}}}}", attempt + 1, child.id(), port, owns_dir, err, stderr)]).status();
                    }
                    // #endregion
                    drop(guard);
                    let _ = child.kill();
                    let _ = child.wait();
                    if owns_dir {
                        let _ = std::fs::remove_dir_all(&data_dir);
                    }
                    last_err = format!(
                        "attempt {} on port {port}: {err}; child stderr:\n{}",
                        attempt + 1,
                        stderr.trim()
                    );
                }
            }
        }
        panic!("server did not become ready after {MAX_ATTEMPTS} attempts: {last_err}");
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    pub fn server_log(&self) -> String {
        ["versal-server.stdout", "versal-server.stderr"]
            .into_iter()
            .filter_map(|name| std::fs::read_to_string(self.data_dir.join(name)).ok())
            .collect::<Vec<_>>()
            .join("\n")
    }

    pub fn wait_exit(&mut self, timeout: Duration) -> Option<i32> {
        self.wait_status(timeout).and_then(|status| status.code())
    }

    pub fn wait_status(&mut self, timeout: Duration) -> Option<ExitStatus> {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if let Some(status) = self.child.try_wait().unwrap_or(None) {
                return Some(status);
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        None
    }

    pub fn take_data_dir(mut self) -> PathBuf {
        let dir = std::mem::replace(&mut self.data_dir, PathBuf::new());
        std::mem::forget(self);
        dir
    }

    pub fn connect(&self) -> Client {
        Client::connect_to(self.port)
    }
}

impl Client {
    pub fn connect_to(port: u16) -> Client {
        let stream =
            TcpStream::connect(("127.0.0.1", port)).expect("failed to connect to test server");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        Client { stream }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let failed = std::thread::panicking();
        self.shutdown();
        if failed {
            eprintln!(
                "\n===== versal-server logs ({}) =====\n{}\n===== end versal-server logs =====",
                self.data_dir.display(),
                self.server_log()
            );
            eprintln!(
                "preserved failed integration test directory: {}",
                self.data_dir.display()
            );
        } else {
            let _ = std::fs::remove_dir_all(&self.data_dir);
        }
    }
}

impl TestServer {
    fn shutdown(&mut self) {
        if self.child.try_wait().unwrap_or(None).is_some() {
            return;
        }

        if let Ok(mut stream) = TcpStream::connect(("127.0.0.1", self.port)) {
            let _ = stream.write_all(b"*2\r\n$8\r\nSHUTDOWN\r\n$6\r\nNOSAVE\r\n");
        }

        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline {
            if self.child.try_wait().unwrap_or(None).is_some() {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

static NEXT_TEST_DIR: AtomicU64 = AtomicU64::new(0);
static SERVER_START_LOCK: Mutex<()> = Mutex::new(());

fn make_test_dir() -> PathBuf {
    let id = NEXT_TEST_DIR.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "valkey-rust-integration-{}-{}-{}",
        std::process::id(),
        id,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time before Unix epoch")
            .as_nanos()
    ));
    std::fs::create_dir(&dir).expect("create isolated test directory");
    dir
}

#[cfg(unix)]
pub fn make_socket_test_dir() -> PathBuf {
    use std::os::unix::fs::DirBuilderExt;

    // Darwin's TMPDIR is long enough to exceed sockaddr_un.sun_path when
    // fixture names are appended. Keep sockets in a private, short directory.
    let id = NEXT_TEST_DIR.fetch_add(1, Ordering::Relaxed);
    let dir = PathBuf::from("/tmp").join(format!(
        "versal-sock-{}-{id}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time before Unix epoch")
            .as_nanos()
    ));
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&dir)
        .expect("create private socket test directory");
    dir
}

fn free_port() -> u16 {
    // Config files can enable Cluster too, so every generated fixture port
    // must leave room for its default bus port, with both listeners available.
    for _ in 0..128 {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind :0 failed");
        let allocated_port = listener.local_addr().unwrap().port();
        let (port, other_port) = if allocated_port > u16::MAX - 10_000 {
            (allocated_port - 10_000, allocated_port - 10_000)
        } else {
            (allocated_port, allocated_port + 10_000)
        };
        if TcpListener::bind(("127.0.0.1", other_port)).is_ok() {
            return port;
        }
    }
    panic!("could not allocate an available client and default Cluster Bus port pair");
}

#[cfg(unix)]
#[test]
fn socket_fixture_directory_is_short_private_and_isolated() {
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::UnixDatagram;

    let first = make_socket_test_dir();
    let second = make_socket_test_dir();
    assert_ne!(first, second);
    for dir in [&first, &second] {
        assert_eq!(
            std::fs::metadata(dir).unwrap().permissions().mode() & 0o777,
            0o700
        );
        let _socket = UnixDatagram::bind(dir.join("notify.sock"))
            .expect("short fixture socket path must fit sockaddr_un");
    }
    std::fs::remove_dir_all(first).unwrap();
    std::fs::remove_dir_all(second).unwrap();
}

#[test]
fn allocated_ports_allow_an_available_default_cluster_bus() {
    let _guard = SERVER_START_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    for _ in 0..32 {
        let port = free_port();
        let bus_port = port
            .checked_add(10_000)
            .expect("fixture port must leave room for the default Cluster Bus");
        let _client = TcpListener::bind(("127.0.0.1", port)).expect("bind allocated client port");
        let _bus = TcpListener::bind(("127.0.0.1", bus_port)).expect("bind allocated bus port");
    }
}

fn wait_child_ready(child: &mut Child, port: u16) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().unwrap_or(None) {
            return Err(format!("process exited early: {status}"));
        }

        if let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)) {
            let _ = stream.set_read_timeout(Some(Duration::from_secs(1)));
            let _ = stream.set_write_timeout(Some(Duration::from_secs(1)));
            if stream.write_all(b"*1\r\n$4\r\nPING\r\n").is_ok() {
                let mut buf = [0u8; 16];
                if let Ok(n) = stream.read(&mut buf) {
                    if n >= 5 && buf.starts_with(b"+PONG") {
                        return Ok(());
                    }
                }
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    Err("timed out waiting for +PONG".to_string())
}

pub struct Client {
    stream: TcpStream,
}

impl Client {
    pub fn send(&mut self, parts: &[&[u8]]) {
        let mut req = Vec::new();
        req.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
        for p in parts {
            req.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            req.extend_from_slice(p);
            req.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&req).expect("write failed");
    }

    pub fn send_raw(&mut self, data: &[u8]) {
        self.stream.write_all(data).expect("write raw failed");
    }

    pub fn read_raw_line(&mut self) -> Vec<u8> {
        self.read_until_crlf()
    }

    pub fn read_sync_rdb(&mut self) {
        let type_byte = self.read_byte();
        assert_eq!(
            type_byte, b'$',
            "SYNC must start with the RDB bulk length header"
        );
        let line = self.read_until_crlf();
        let count = parse_i64(&line);
        assert!(count >= 0, "SYNC RDB bulk length must be non-negative");
        let mut body = vec![0u8; count as usize];
        self.read_exact(&mut body);
    }

    pub fn read_repl_command(&mut self) -> Vec<Vec<u8>> {
        // Skip lone `\n` keepalive pings the primary may interleave.
        let type_byte = loop {
            let b = self.read_byte();
            if b == b'\n' || b == b'\r' {
                continue;
            }
            break b;
        };
        assert_eq!(
            type_byte, b'*',
            "replication stream command must be a RESP array"
        );
        let n = parse_i64(&self.read_until_crlf());
        let mut args = Vec::with_capacity(n.max(0) as usize);
        for _ in 0..n.max(0) {
            let dollar = self.read_byte();
            assert_eq!(dollar, b'$', "replication array element must be a bulk");
            let len = parse_i64(&self.read_until_crlf());
            let mut arg = vec![0u8; len.max(0) as usize];
            self.read_exact(&mut arg);
            // consume trailing CRLF
            let mut crlf = [0u8; 2];
            self.read_exact(&mut crlf);
            args.push(arg);
        }
        args
    }

    pub fn command(&mut self, parts: &[&[u8]]) -> Vec<u8> {
        self.send(parts);
        self.read_reply()
    }

    pub fn read_reply(&mut self) -> Vec<u8> {
        let mut out = Vec::new();
        self.read_one(&mut out);
        out
    }

    pub fn command_str(&mut self, parts: &[&[u8]]) -> String {
        String::from_utf8_lossy(&self.command(parts)).into_owned()
    }

    fn read_one(&mut self, out: &mut Vec<u8>) {
        let type_byte = self.read_byte();
        out.push(type_byte);
        let line = self.read_until_crlf();
        out.extend_from_slice(&line);
        out.extend_from_slice(b"\r\n");
        match type_byte {
            b'+' | b'-' | b':' => {}
            b'$' => {
                let n = parse_i64(&line);
                if n >= 0 {
                    let mut body = vec![0u8; n as usize + 2];
                    self.read_exact(&mut body);
                    out.extend_from_slice(&body);
                }
            }
            b'*' | b'>' | b'~' => {
                let n = parse_i64(&line);
                for _ in 0..n.max(0) {
                    self.read_one(out);
                }
            }
            b'%' | b'|' => {
                let n = parse_i64(&line);
                for _ in 0..n.max(0) * 2 {
                    self.read_one(out);
                }
            }
            b'_' => {}
            other => panic!("unknown RESP type byte: {}", other as char),
        }
    }

    fn read_byte(&mut self) -> u8 {
        let mut b = [0u8; 1];
        self.read_exact(&mut b);
        b[0]
    }

    fn read_until_crlf(&mut self) -> Vec<u8> {
        let mut line = Vec::new();
        loop {
            let b = self.read_byte();
            if b == b'\r' {
                let n = self.read_byte();
                assert_eq!(n, b'\n', "expected \\n after \\r");
                return line;
            }
            line.push(b);
        }
    }

    fn read_exact(&mut self, buf: &mut [u8]) {
        use std::io::{ErrorKind, Read};
        // Linux `SO_RCVTIMEO` timeouts surface as `WouldBlock` rather than
        // `TimedOut`; SIGIO/network-stack transients surface as `Interrupted`.
        // We retry up to the caller's configured `set_read_timeout` window so
        // a single transient EAGAIN/EINTR doesn't mask the real failure mode
        // (server exit) as an implausible "Resource temporarily unavailable".
        const RETRY_LIMIT: u32 = 3;
        let mut attempt = 0;
        loop {
            match Read::read_exact(&mut self.stream, buf) {
                Ok(()) => return,
                Err(e)
                    if attempt < RETRY_LIMIT
                        && matches!(
                            e.kind(),
                            ErrorKind::WouldBlock | ErrorKind::TimedOut | ErrorKind::Interrupted
                        ) =>
                {
                    attempt += 1;
                    std::thread::sleep(std::time::Duration::from_millis(1));
                    continue;
                }
                Err(e) => panic!(
                    "read from server failed (connection closed?): {} (attempt {})",
                    e, attempt
                ),
            }
        }
    }

    pub fn is_closed(&mut self) -> bool {
        let mut b = [0u8; 1];
        matches!(self.stream.read(&mut b), Ok(0))
    }
}

fn parse_i64(bytes: &[u8]) -> i64 {
    std::str::from_utf8(bytes)
        .unwrap()
        .trim()
        .parse::<i64>()
        .unwrap()
}
