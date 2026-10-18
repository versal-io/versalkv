use super::cli::{parse_args, AofCheckMode, AofCliAction, AofCliError};
use super::repair::{apply_repair, plan_repair, AofRepairEligibility, AofRepairPlan};
use super::scan::{
    scan_aof_bytes, AofPreambleInfo, AofScanOptions, AofScanOutcome, AofScanSnapshot,
};
use std::fs;
use std::mem::{needs_drop, size_of};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

fn command(name: &[u8], args: &[&[u8]]) -> Vec<u8> {
    let mut encoded = format!("*{}\r\n", args.len() + 1).into_bytes();
    for value in std::iter::once(name).chain(args.iter().copied()) {
        encoded.extend_from_slice(format!("${}\r\n", value.len()).as_bytes());
        encoded.extend_from_slice(value);
        encoded.extend_from_slice(b"\r\n");
    }
    encoded
}

fn options(timestamp: Option<u64>) -> AofScanOptions {
    AofScanOptions {
        filename: "appendonly.aof".to_string(),
        has_rdb_preamble: false,
        timestamp,
    }
}

struct TempFile(PathBuf);

impl TempFile {
    fn new(name: &str, contents: &[u8]) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should follow the Unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "versal-check-aof-{}-{}-{}",
            std::process::id(),
            nonce,
            name
        ));
        fs::write(&path, contents).expect("temporary AOF should be writable");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn temp_file(name: &str, contents: &[u8]) -> TempFile {
    TempFile::new(name, contents)
}

#[test]
fn scan_snapshot_and_repair_plan_are_constant_sized_values() {
    assert_eq!(size_of::<AofScanSnapshot>(), 40);
    assert!(!needs_drop::<AofScanSnapshot>());
    assert!(!needs_drop::<AofRepairPlan>());
}

#[test]
fn scans_valid_command_and_reports_complete_offset() {
    let data = command(b"SET", &[b"key", b"value"]);
    let report = scan_aof_bytes(&data, &options(None));

    assert_eq!(report.outcome, AofScanOutcome::Valid);
    assert_eq!(report.size, data.len() as u64);
    assert_eq!(report.ok_up_to, data.len() as u64);
    assert_eq!(report.line, 8);
    assert_eq!(report.preamble, AofPreambleInfo::None);
    assert_eq!(report.error, None);
}

#[test]
fn reports_truncated_command_without_mutating_source_file() {
    let mut data = command(b"SET", &[b"key", b"value"]);
    data.pop();
    let path = temp_file("truncated", &data);
    let before = fs::metadata(path.path()).expect("temporary AOF should exist");

    let report = scan_aof_bytes(&fs::read(path.path()).unwrap(), &options(None));

    assert_eq!(report.outcome, AofScanOutcome::Corrupt);
    assert_eq!(report.ok_up_to, 0);
    assert!(report
        .error
        .as_deref()
        .unwrap()
        .contains("Truncated bulk string"));
    assert_eq!(fs::read(path.path()).unwrap(), data);
    assert_eq!(fs::metadata(path.path()).unwrap().len(), before.len());
}

#[test]
fn unfinished_multi_rolls_repair_offset_back_before_transaction() {
    let mut data = command(b"SET", &[b"before", b"one"]);
    let transaction_offset = data.len() as u64;
    let multi = command(b"MULTI", &[]);
    let final_entry_offset = transaction_offset + multi.len() as u64;
    data.extend(multi);
    data.extend(command(b"SET", &[b"inside", b"two"]));

    let report = scan_aof_bytes(&data, &options(None));

    assert_eq!(report.outcome, AofScanOutcome::Corrupt);
    assert_eq!(report.ok_up_to, transaction_offset);
    assert!(report
        .error
        .as_deref()
        .unwrap()
        .starts_with(&format!("0x{:016x}", final_entry_offset)));
    assert!(report
        .error
        .as_deref()
        .unwrap()
        .contains("before reading EXEC"));
}

#[test]
fn trailing_bytes_after_last_complete_command_remain_repairable_corruption() {
    let mut data = command(b"PING", &[]);
    let valid_offset = data.len() as u64;
    data.extend_from_slice(b"\r\n");

    let report = scan_aof_bytes(&data, &options(None));

    assert_eq!(report.outcome, AofScanOutcome::Corrupt);
    assert_eq!(report.ok_up_to, valid_offset);
    assert_eq!(report.error, None);
}

#[test]
fn annotations_produce_a_timestamp_boundary_without_truncating() {
    let mut data = b"#TS:10\r\n".to_vec();
    data.extend(command(b"SET", &[b"before", b"one"]));
    let boundary = data.len() as u64;
    data.extend_from_slice(b"#TS:20\r\n");
    data.extend(command(b"SET", &[b"after", b"two"]));
    let path = temp_file("timestamp", &data);

    let report = scan_aof_bytes(&fs::read(path.path()).unwrap(), &options(Some(15)));

    assert_eq!(
        report.outcome,
        AofScanOutcome::TimestampBoundary {
            offset: boundary,
            timestamp: 20,
        }
    );
    assert_eq!(fs::read(path.path()).unwrap(), data);
}

#[test]
fn repair_plan_refuses_non_last_file_and_mutation_preserves_it() {
    let mut data = command(b"SET", &[b"key", b"value"]);
    data.extend_from_slice(b"broken");
    let report = scan_aof_bytes(&data, &options(None));
    let plan = plan_repair(&report, AofCheckMode::Fix, false);
    let path = temp_file("non-last", &data);

    assert!(matches!(
        plan,
        AofRepairPlan::Truncate {
            eligibility: AofRepairEligibility::NotLastFile,
            ..
        }
    ));
    assert!(apply_repair(path.path(), &plan).is_err());
    assert_eq!(fs::read(path.path()).unwrap(), data);
}

#[test]
fn eligible_repair_writes_exact_valid_prefix_only_when_invoked() {
    let prefix = command(b"SET", &[b"key", b"value"]);
    let mut data = prefix.clone();
    data.extend_from_slice(b"broken");
    let report = scan_aof_bytes(&data, &options(None));
    let plan = plan_repair(&report, AofCheckMode::Fix, true);
    let path = temp_file("eligible", &data);

    assert_eq!(fs::read(path.path()).unwrap(), data);
    apply_repair(path.path(), &plan).expect("eligible repair should succeed");
    assert_eq!(fs::read(path.path()).unwrap(), prefix);
}

#[test]
fn eligible_repair_handles_a_large_aof_without_retained_prefix_state() {
    let payload = vec![b'x'; 2 * 1024 * 1024];
    let mut data = command(b"SET", &[b"key", payload.as_slice()]);
    let valid_size = data.len();
    data.extend_from_slice(b"broken");
    let report = scan_aof_bytes(&data, &options(None));
    let plan = plan_repair(&report, AofCheckMode::Fix, true);
    let path = temp_file("large", &data);

    apply_repair(path.path(), &plan).expect("large eligible repair should succeed");

    let repaired = fs::read(path.path()).unwrap();
    assert_eq!(repaired.len(), valid_size);
    assert_eq!(repaired, data[..valid_size]);
}

#[test]
fn repair_rejects_same_size_content_change_without_mutating_replacement() {
    let mut data = command(b"SET", &[b"key", b"value"]);
    data.extend_from_slice(b"broken");
    let report = scan_aof_bytes(&data, &options(None));
    let plan = plan_repair(&report, AofCheckMode::Fix, true);
    let path = temp_file("same-size-change", &data);
    let mut replacement = data.clone();
    *replacement.last_mut().unwrap() = b'!';
    fs::write(path.path(), &replacement).unwrap();

    assert!(apply_repair(path.path(), &plan).is_err());
    assert_eq!(fs::read(path.path()).unwrap(), replacement);
}

#[test]
fn repair_rejects_different_size_content_change_without_mutating_replacement() {
    let mut data = command(b"SET", &[b"key", b"value"]);
    data.extend_from_slice(b"broken");
    let report = scan_aof_bytes(&data, &options(None));
    let plan = plan_repair(&report, AofCheckMode::Fix, true);
    let path = temp_file("different-size-change", &data);
    let mut replacement = data.clone();
    replacement.extend_from_slice(b"more");
    fs::write(path.path(), &replacement).unwrap();

    assert!(apply_repair(path.path(), &plan).is_err());
    assert_eq!(fs::read(path.path()).unwrap(), replacement);
}

#[test]
fn repair_rejects_shorter_file_without_extending_it() {
    let mut data = command(b"SET", &[b"key", b"value"]);
    data.extend_from_slice(b"broken");
    let report = scan_aof_bytes(&data, &options(None));
    let plan = plan_repair(&report, AofCheckMode::Fix, true);
    let path = temp_file("shorter-change", &data);
    let shortened = b"short";
    fs::write(path.path(), shortened).unwrap();

    assert!(apply_repair(path.path(), &plan).is_err());
    assert_eq!(fs::read(path.path()).unwrap(), shortened);
}

#[test]
fn parses_version_fix_and_timestamp_actions() {
    assert_eq!(parse_args(&["--version".into()]), Ok(AofCliAction::Version));
    assert_eq!(
        parse_args(&["--fix".into(), "appendonly.aof".into()]),
        Ok(AofCliAction::Check(super::cli::AofCheckOptions {
            file: PathBuf::from("appendonly.aof"),
            mode: AofCheckMode::Fix,
        }))
    );
    assert_eq!(
        parse_args(&[
            "--truncate-to-timestamp".into(),
            "42".into(),
            "appendonly.aof".into(),
        ]),
        Ok(AofCliAction::Check(super::cli::AofCheckOptions {
            file: PathBuf::from("appendonly.aof"),
            mode: AofCheckMode::Timestamp(42),
        }))
    );
    assert_eq!(
        parse_args(&[
            "--truncate-to-timestamp".into(),
            "0".into(),
            "appendonly.aof".into(),
        ]),
        Ok(AofCliAction::Check(super::cli::AofCheckOptions {
            file: PathBuf::from("appendonly.aof"),
            mode: AofCheckMode::Check,
        }))
    );
}

#[test]
fn rejects_invalid_aof_cli_forms_with_typed_errors() {
    assert_eq!(parse_args(&[]), Err(AofCliError::Usage));
    assert_eq!(
        parse_args(&[
            "--truncate-to-timestamp".into(),
            "later".into(),
            "appendonly.aof".into(),
        ]),
        Err(AofCliError::InvalidTimestamp)
    );
    assert_eq!(
        parse_args(&["--unknown".into(), "appendonly.aof".into()]),
        Err(AofCliError::Usage)
    );
}
