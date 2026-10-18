//! Signal-driven shutdown while startup persistence loading is in progress.

use std::fs::File;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use versal::foundation::io::persistence::serialize_all;
use versal::foundation::io::store::kv_engine::DbSet;
use versal::foundation::io::store::Robj;
use versal::foundation::io::sys::process::{send_signal, ProcessSignal};

struct TestDir(PathBuf);

impl TestDir {
    fn new(tag: &str) -> Self {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "versal-startup-loading-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock before Unix epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).expect("create startup-loading test directory");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct ChildGuard(Child);

impl ChildGuard {
    fn spawn(args: &[&str], stdout_path: &Path, stderr_path: &Path) -> Self {
        let stdout = File::create(stdout_path).expect("create startup stdout");
        let stderr = File::create(stderr_path).expect("create startup stderr");
        Self(
            Command::new(env!("CARGO_BIN_EXE_versal-server"))
                .args(args)
                .stdout(Stdio::from(stdout))
                .stderr(Stdio::from(stderr))
                .spawn()
                .expect("spawn versal-server"),
        )
    }

    fn pid(&self) -> u32 {
        self.0.id()
    }

    fn wait_for_exit(&mut self, timeout: Duration) -> Option<ExitStatus> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.0.try_wait().expect("wait for versal-server") {
                return Some(status);
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

fn reserve_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve startup-loading port");
    listener.local_addr().expect("reserved port").port()
}

fn wait_for_loading(child: &mut ChildGuard, port: u16, stdout_path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(12);
    loop {
        if let Some(status) = child.0.try_wait().expect("inspect startup child") {
            let stdout = std::fs::read_to_string(stdout_path).unwrap_or_default();
            panic!("server exited before loading probe: {status}\n{stdout}");
        }
        if let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)) {
            stream
                .set_read_timeout(Some(Duration::from_millis(500)))
                .expect("set loading probe timeout");
            if stream
                .write_all(b"*2\r\n$4\r\nINFO\r\n$11\r\npersistence\r\n")
                .is_ok()
            {
                let mut reply = [0_u8; 16 * 1024];
                if let Ok(size) = stream.read(&mut reply) {
                    if String::from_utf8_lossy(&reply[..size]).contains("loading:1") {
                        let stdout = std::fs::read_to_string(stdout_path).unwrap_or_default();
                        assert!(
                            !stdout.contains("Ready to accept connections"),
                            "server became ready before loading probe:\n{stdout}"
                        );
                        return;
                    }
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "server did not answer INFO while startup loading"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn assert_clean_loading_shutdown(
    child: &mut ChildGuard,
    stdout_path: &Path,
    port: u16,
    timeout: Duration,
) {
    send_signal(child.pid() as i32, ProcessSignal::Terminate)
        .expect("send SIGTERM during startup loading");
    let status = child
        .wait_for_exit(timeout)
        .expect("server did not stop promptly during startup loading");
    assert_eq!(status.code(), Some(0), "loading shutdown status: {status}");

    let stdout = std::fs::read_to_string(stdout_path).unwrap_or_default();
    assert!(
        !stdout.contains("Ready to accept connections"),
        "startup shutdown emitted ready banner:\n{stdout}"
    );
    assert!(
        !stdout.contains("Fatal error loading the DB"),
        "startup shutdown was reported as load failure:\n{stdout}"
    );
    TcpListener::bind(("127.0.0.1", port))
        .expect("startup shutdown did not release the listener port");
}

fn append_resp_command(output: &mut Vec<u8>, parts: &[&[u8]]) {
    output.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
    for part in parts {
        output.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        output.extend_from_slice(part);
        output.extend_from_slice(b"\r\n");
    }
}

#[test]
fn sigterm_during_startup_rdb_loading_exits_cleanly() {
    let dir = TestDir::new("rdb-sigterm");
    let mut dbs = DbSet::new(1);
    for index in 0..1_000 {
        dbs.get_mut(0)
            .expect("DB 0")
            .set(format!("key-{index}").into_bytes(), Robj::string("value"));
    }
    let rdb = serialize_all(&dbs, 0);
    std::fs::write(dir.path().join("dump.rdb"), &rdb).expect("write startup RDB");

    let port = reserve_port();
    let stdout_path = dir.path().join("stdout.log");
    let stderr_path = dir.path().join("stderr.log");
    let port_arg = port.to_string();
    let mut child = ChildGuard::spawn(
        &[
            "--port",
            &port_arg,
            "--bind",
            "127.0.0.1",
            "--dir",
            dir.path().to_str().expect("UTF-8 test path"),
            "--dbfilename",
            "dump.rdb",
            "--key-load-delay",
            "10000",
            "--loading-process-events-interval-bytes",
            "1024",
        ],
        &stdout_path,
        &stderr_path,
    );

    wait_for_loading(&mut child, port, &stdout_path);
    assert_clean_loading_shutdown(&mut child, &stdout_path, port, Duration::from_secs(2));
    assert_eq!(
        std::fs::read(dir.path().join("dump.rdb")).expect("read preserved RDB"),
        rdb,
        "loading shutdown rewrote the source RDB"
    );
}

#[test]
fn sigterm_during_startup_aof_loading_exits_cleanly() {
    let dir = TestDir::new("aof-sigterm");
    let aof_dir = dir.path().join("appendonlydir");
    std::fs::create_dir_all(&aof_dir).expect("create AOF directory");
    std::fs::write(
        aof_dir.join("appendonly.aof.manifest"),
        b"file appendonly.aof.1.incr.aof seq 1 type i\n",
    )
    .expect("write AOF manifest");
    let mut aof = Vec::new();
    for _ in 0..2_000_000 {
        append_resp_command(&mut aof, &[b"INCR", b"counter"]);
    }
    std::fs::write(aof_dir.join("appendonly.aof.1.incr.aof"), aof).expect("write incremental AOF");

    let port = reserve_port();
    let config_path = dir.path().join("valkey.conf");
    std::fs::write(
        &config_path,
        format!(
            "bind 127.0.0.1\nport {port}\ndir {}\nsave \"\"\nappendonly yes\nappenddirname appendonlydir\nappendfilename appendonly.aof\naof-load-truncated no\n",
            dir.path().display()
        ),
    )
    .expect("write AOF startup config");
    let stdout_path = dir.path().join("stdout.log");
    let stderr_path = dir.path().join("stderr.log");
    let mut child = ChildGuard::spawn(
        &[config_path.to_str().expect("UTF-8 config path")],
        &stdout_path,
        &stderr_path,
    );

    wait_for_loading(&mut child, port, &stdout_path);
    assert_clean_loading_shutdown(&mut child, &stdout_path, port, Duration::from_secs(1));
}

#[test]
fn startup_rejects_unfinished_transaction_before_last_aof_file() {
    let dir = TestDir::new("aof-nonfinal-transaction");
    let aof_dir = dir.path().join("appendonlydir");
    std::fs::create_dir_all(&aof_dir).expect("create AOF directory");
    std::fs::write(
        aof_dir.join("appendonly.aof.manifest"),
        b"file appendonly.aof.1.base.aof seq 1 type b\n\
file appendonly.aof.1.incr.aof seq 1 type i\n",
    )
    .expect("write AOF manifest");

    let mut base = Vec::new();
    append_resp_command(&mut base, &[b"SET", b"stable", b"yes"]);
    append_resp_command(&mut base, &[b"MULTI"]);
    append_resp_command(&mut base, &[b"SET", b"partial", b"no"]);
    std::fs::write(aof_dir.join("appendonly.aof.1.base.aof"), base).expect("write base AOF");
    let mut increment = Vec::new();
    append_resp_command(&mut increment, &[b"SET", b"later", b"yes"]);
    std::fs::write(aof_dir.join("appendonly.aof.1.incr.aof"), increment)
        .expect("write incremental AOF");

    let port = reserve_port();
    let config_path = dir.path().join("valkey.conf");
    std::fs::write(
        &config_path,
        format!(
            "bind 127.0.0.1\nport {port}\ndir {}\nsave \"\"\nappendonly yes\nappenddirname appendonlydir\nappendfilename appendonly.aof\naof-load-truncated yes\n",
            dir.path().display()
        ),
    )
    .expect("write AOF startup config");
    let stdout_path = dir.path().join("stdout.log");
    let stderr_path = dir.path().join("stderr.log");
    let mut child = ChildGuard::spawn(
        &[config_path.to_str().expect("UTF-8 config path")],
        &stdout_path,
        &stderr_path,
    );

    let status = child
        .wait_for_exit(Duration::from_secs(2))
        .expect("server accepted a truncated non-final AOF file");
    assert!(!status.success(), "non-final AOF truncation exited cleanly");
    let stdout = std::fs::read_to_string(stdout_path).unwrap_or_default();
    assert!(
        stdout.contains("truncated file is not the last file"),
        "missing non-final truncation error:\n{stdout}"
    );
    assert!(!stdout.contains("Ready to accept connections"));
}
