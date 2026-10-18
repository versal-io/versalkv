use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should follow the Unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "versal-checker-cli-{}-{}",
            std::process::id(),
            nonce
        ));
        fs::create_dir(&path).expect("checker test directory should be creatable");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn check_aof_process_reports_valid_file_and_success_exit() {
    let directory = TempDir::new();
    let path = directory.path().join("appendonly.aof");
    fs::write(&path, b"*1\r\n$4\r\nPING\r\n").unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_versal-check-aof"))
        .arg(&path)
        .output()
        .expect("versal-check-aof should run");

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.starts_with("Start checking Old-Style AOF\n"));
    assert!(stdout.contains("AOF analyzed:"));
    assert!(stdout.contains(&format!("AOF {} is valid", path.display())));
    assert!(output.stderr.is_empty());
}

#[test]
fn check_rdb_process_reports_disabled_checksum_and_success_exit() {
    let directory = TempDir::new();
    let path = directory.path().join("dump.rdb");
    let mut bytes = b"REDIS0005\xff".to_vec();
    bytes.extend_from_slice(&0u64.to_le_bytes());
    fs::write(&path, bytes).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_versal-check-rdb"))
        .arg(&path)
        .output()
        .expect("versal-check-rdb should run");

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("RDB file was saved with checksum disabled"));
    assert!(stdout.contains("\\o/ RDB looks OK! \\o/"));
    assert!(output.stderr.is_empty());
}

#[test]
fn check_aof_declined_repair_keeps_abort_message_on_stdout() {
    let directory = TempDir::new();
    let path = directory.path().join("broken.aof");
    fs::write(&path, b"*1\r\n$4\r\nPING\r\nbroken").unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_versal-check-aof"))
        .args(["--fix", path.to_str().unwrap()])
        .output()
        .expect("versal-check-aof should run");

    assert!(!output.status.success());
    assert!(String::from_utf8(output.stdout)
        .unwrap()
        .contains("Aborting..."));
    assert!(!String::from_utf8(output.stderr)
        .unwrap()
        .contains("Aborting..."));
}

#[test]
fn check_aof_timestamp_zero_checks_without_truncating() {
    let directory = TempDir::new();
    let path = directory.path().join("timestamp-zero.aof");
    let contents = b"#TS:10\r\n*1\r\n$4\r\nPING\r\n#TS:20\r\n";
    fs::write(&path, contents).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_versal-check-aof"))
        .args(["--truncate-to-timestamp", "0", path.to_str().unwrap()])
        .output()
        .expect("versal-check-aof should run");

    assert!(output.status.success());
    assert_eq!(fs::read(&path).unwrap(), contents);
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains(&format!("AOF {} is valid", path.display())));
    assert!(!stdout.contains("Successfully truncated"));
    assert!(!stdout.contains("Truncate nothing"));
    assert!(output.stderr.is_empty());
}
