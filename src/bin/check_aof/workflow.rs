use crate::cli::{AofCheckMode, AofCheckOptions};
use crate::repair::{
    apply_repair, plan_repair, AofRepairEligibility, AofRepairPlan, AofRepairReason,
};
use crate::scan::{scan_aof_bytes, AofPreambleInfo, AofScanOptions, AofScanOutcome, AofScanReport};
use std::fmt;
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use versal::foundation::io::persistence::aof::parse_manifest_bytes;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InputFileType {
    Resp,
    RdbPreamble,
    MultiPart,
}

struct AofInfo {
    file_name: String,
}

struct AofManifest {
    base: Option<AofInfo>,
    increments: Vec<AofInfo>,
}

struct AofValidation {
    name: String,
    path: PathBuf,
    report: AofScanReport,
    plan: AofRepairPlan,
    mode: AofCheckMode,
}

#[derive(Debug)]
pub struct AofRunError {
    message: String,
    stream: ErrorStream,
}

#[derive(Debug)]
enum ErrorStream {
    Stdout,
    Stderr,
}

impl AofRunError {
    fn stderr(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            stream: ErrorStream::Stderr,
        }
    }

    fn stdout(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            stream: ErrorStream::Stdout,
        }
    }

    pub fn report(&self) {
        match self.stream {
            ErrorStream::Stdout => println!("{}", self.message),
            ErrorStream::Stderr => eprintln!("{}", self.message),
        }
    }
}

impl fmt::Display for AofRunError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

pub fn check_aof(options: &AofCheckOptions) -> Result<(), AofRunError> {
    let input_type = classify_path(&options.file);
    let directory = options.file.parent().unwrap_or_else(|| Path::new("."));
    match input_type {
        InputFileType::MultiPart => check_manifest(directory, options),
        InputFileType::Resp => check_old_style(options, false),
        InputFileType::RdbPreamble => check_old_style(options, true),
    }
}

fn check_old_style(options: &AofCheckOptions, preamble: bool) -> Result<(), AofRunError> {
    println!("Start checking Old-Style AOF");
    let validation = validate_file(&options.file, &options.file, true, preamble, options.mode)?;
    let status = execute_validation(&validation)?;
    print_aof_style(status, &validation.name, "AOF");
    Ok(())
}

fn check_manifest(directory: &Path, options: &AofCheckOptions) -> Result<(), AofRunError> {
    println!("Start checking Multi Part AOF");
    let manifest = parse_manifest(&options.file)?;
    let total = manifest.base.is_some() as usize + manifest.increments.len();
    let mut index = 0;
    if let Some(base) = manifest.base {
        index += 1;
        check_manifest_file(directory, &base, index == total, true, options.mode)?;
    }
    for increment in manifest.increments {
        index += 1;
        check_manifest_file(directory, &increment, index == total, false, options.mode)?;
    }
    println!("All AOF files and manifest are valid");
    Ok(())
}

fn check_manifest_file(
    directory: &Path,
    info: &AofInfo,
    last_file: bool,
    is_base: bool,
    mode: AofCheckMode,
) -> Result<(), AofRunError> {
    let path = directory.join(&info.file_name);
    let preamble = is_base && file_is_rdb(&path);
    print_manifest_start(is_base, preamble);
    let validation = validate_file(&info.file_name, &path, last_file, preamble, mode)?;
    let status = execute_validation(&validation)?;
    print_aof_style(
        status,
        &info.file_name,
        if is_base { "BASE AOF" } else { "INCR AOF" },
    );
    Ok(())
}

fn print_manifest_start(is_base: bool, preamble: bool) {
    if is_base {
        println!(
            "Start to check BASE AOF ({} format).",
            if preamble { "RDB" } else { "RESP" }
        );
    } else {
        println!("Start to check INCR files.");
    }
}

fn validate_file(
    name: impl AsRef<Path>,
    path: &Path,
    last_file: bool,
    preamble: bool,
    mode: AofCheckMode,
) -> Result<AofValidation, AofRunError> {
    let data = fs::read(path).map_err(|error| {
        AofRunError::stderr(format!(
            "Cannot open file {}: {}, aborting...",
            path.display(),
            error
        ))
    })?;
    let name = name.as_ref().to_string_lossy().into_owned();
    let report = scan_aof_bytes(
        &data,
        &AofScanOptions {
            filename: name.clone(),
            has_rdb_preamble: preamble,
            timestamp: timestamp(mode),
        },
    );
    let plan = plan_repair(&report, mode, last_file);
    Ok(AofValidation {
        name,
        path: path.to_path_buf(),
        report,
        plan,
        mode,
    })
}

fn execute_validation(validation: &AofValidation) -> Result<ValidationStatus, AofRunError> {
    render_preamble(&validation.report)?;
    if let Some(error) = &validation.report.error {
        println!("{}", error);
    }
    match validation.report.outcome {
        AofScanOutcome::Empty => Ok(ValidationStatus::Empty),
        AofScanOutcome::Valid => finish_valid(validation),
        AofScanOutcome::Corrupt => finish_corrupt(validation),
        AofScanOutcome::TimestampBoundary { .. } => finish_timestamp(validation),
    }
}

fn finish_valid(validation: &AofValidation) -> Result<ValidationStatus, AofRunError> {
    if let AofCheckMode::Timestamp(target) = validation.mode {
        println!(
            "Truncate nothing in AOF {} to timestamp {}",
            validation.name, target
        );
        return Ok(ValidationStatus::Valid);
    }
    render_analysis(validation);
    Ok(ValidationStatus::Valid)
}

fn finish_corrupt(validation: &AofValidation) -> Result<ValidationStatus, AofRunError> {
    if validation.report.preamble == AofPreambleInfo::Invalid {
        return Err(AofRunError::stderr(
            "RDB preamble of AOF file is not sane, aborting.".to_string(),
        ));
    }
    render_analysis(validation);
    match &validation.plan {
        AofRepairPlan::None => Err(AofRunError::stderr(format!(
            "AOF {} is not valid. Use the --fix option to try fixing it.",
            validation.name
        ))),
        plan @ AofRepairPlan::Truncate {
            offset,
            eligibility,
            snapshot,
            ..
        } => execute_corruption_repair(
            validation,
            plan,
            *offset,
            snapshot.original_size,
            *eligibility,
        ),
    }
}

fn execute_corruption_repair(
    validation: &AofValidation,
    plan: &AofRepairPlan,
    offset: u64,
    original_size: u64,
    eligibility: AofRepairEligibility,
) -> Result<ValidationStatus, AofRunError> {
    if eligibility == AofRepairEligibility::NotLastFile {
        return Err(AofRunError::stderr(format!(
            "Failed to truncate AOF {} because it is not the last file",
            validation.name
        )));
    }
    println!(
        "This will shrink the AOF {} from {} bytes, with {} bytes, to {} bytes",
        validation.name,
        original_size,
        original_size - offset,
        offset
    );
    confirm_repair()?;
    apply(validation, plan, "Failed to truncate AOF")?;
    println!("Successfully truncated AOF {}", validation.name);
    Ok(ValidationStatus::Truncated)
}

fn finish_timestamp(validation: &AofValidation) -> Result<ValidationStatus, AofRunError> {
    let AofRepairPlan::Truncate {
        offset,
        reason: AofRepairReason::Timestamp { target, .. },
        eligibility,
        ..
    } = &validation.plan
    else {
        return Err(AofRunError::stderr("Missing timestamp repair request"));
    };
    match eligibility {
        AofRepairEligibility::NothingBeforeTimestamp => Err(AofRunError::stderr(format!(
            "AOF {} has nothing before timestamp {}, aborting...",
            validation.path.display(), target
        ))),
        AofRepairEligibility::NotLastFile => Err(AofRunError::stderr(format!(
            "Failed to truncate AOF {} to timestamp {} at offset {} because it is not the last file.",
            validation.path.display(), target, offset
        ))),
        AofRepairEligibility::Eligible => {
            apply(validation, &validation.plan, "Cannot truncate")?;
            println!(
                "Successfully truncated AOF {} to timestamp {}",
                validation.path.display(), target
            );
            Ok(ValidationStatus::TimestampTruncated)
        }
    }
}

fn apply(
    validation: &AofValidation,
    plan: &AofRepairPlan,
    prefix: &str,
) -> Result<(), AofRunError> {
    apply_repair(&validation.path, plan).map_err(|error| {
        AofRunError::stderr(format!("{} {}: {}", prefix, validation.name, error))
    })?;
    Ok(())
}

fn confirm_repair() -> Result<(), AofRunError> {
    print!("Continue? [y/N]: ");
    io::stdout().flush().ok();
    let mut input = String::new();
    let accepted =
        io::stdin().read_line(&mut input).is_ok() && input.trim().eq_ignore_ascii_case("y");
    if accepted {
        Ok(())
    } else {
        Err(AofRunError::stdout("Aborting..."))
    }
}

fn render_preamble(report: &AofScanReport) -> Result<(), AofRunError> {
    match report.preamble {
        AofPreambleInfo::None => Ok(()),
        AofPreambleInfo::Only { .. } => {
            println!("RDB preamble is OK, no AOF tail found.");
            Ok(())
        }
        AofPreambleInfo::Tail { .. } => {
            println!("RDB preamble is OK, proceeding with AOF tail...");
            Ok(())
        }
        AofPreambleInfo::Invalid => Err(AofRunError::stderr(
            "RDB preamble of AOF file is not sane, aborting.".to_string(),
        )),
    }
}

fn render_analysis(validation: &AofValidation) {
    let diff = validation.report.size - validation.report.ok_up_to;
    println!(
        "AOF analyzed: filename={}, size={}, ok_up_to={}, ok_up_to_line={}, diff={}",
        validation.name,
        validation.report.size,
        validation.report.ok_up_to,
        validation.report.line,
        diff
    );
}

#[derive(Clone, Copy)]
enum ValidationStatus {
    Valid,
    Empty,
    Truncated,
    TimestampTruncated,
}

fn print_aof_style(status: ValidationStatus, name: &str, kind: &str) {
    match status {
        ValidationStatus::Valid => println!("{} {} is valid", kind, name),
        ValidationStatus::Empty => println!("{} {} is empty", kind, name),
        ValidationStatus::Truncated | ValidationStatus::TimestampTruncated => {}
    }
}

fn timestamp(mode: AofCheckMode) -> Option<u64> {
    match mode {
        AofCheckMode::Timestamp(value) => Some(value),
        AofCheckMode::Check | AofCheckMode::Fix => None,
    }
}

fn classify_path(path: &Path) -> InputFileType {
    if file_is_manifest(path) {
        InputFileType::MultiPart
    } else if file_is_rdb(path) {
        InputFileType::RdbPreamble
    } else {
        InputFileType::Resp
    }
}

fn file_is_manifest(path: &Path) -> bool {
    let Ok(file) = File::open(path) else {
        return false;
    };
    let mut reader = io::BufReader::new(file);
    let mut line = Vec::new();
    loop {
        line.clear();
        if io::BufRead::read_until(&mut reader, b'\n', &mut line).ok() == Some(0) {
            return false;
        }
        if line.first() != Some(&b'#') {
            return line.starts_with(b"file");
        }
    }
}

fn file_is_rdb(path: &Path) -> bool {
    let Ok(mut file) = File::open(path) else {
        return false;
    };
    let mut signature = [0; 9];
    file.read(&mut signature)
        .is_ok_and(|length| versal::foundation::io::persistence::is_rdb_magic(&signature[..length]))
}

fn parse_manifest(path: &Path) -> Result<AofManifest, AofRunError> {
    let bytes = fs::read(path).map_err(|error| {
        AofRunError::stderr(format!(
            "Failed to parse manifest: Cannot read manifest: {}",
            error
        ))
    })?;
    let entries = parse_manifest_bytes(&bytes)
        .map_err(|error| AofRunError::stderr(format!("Failed to parse manifest: {}", error)))?;
    let mut manifest = AofManifest {
        base: None,
        increments: Vec::new(),
    };
    for entry in entries {
        let info = AofInfo {
            file_name: entry.filename,
        };
        match entry.kind {
            'b' => manifest.base = Some(info),
            'i' => manifest.increments.push(info),
            'h' => {}
            _ => {
                return Err(AofRunError::stderr(
                    "Failed to parse manifest: Invalid AOF manifest file format".to_string(),
                ));
            }
        }
    }
    Ok(manifest)
}

#[cfg(test)]
mod codec_fix_tests {
    use super::*;

    #[test]
    fn short_vcs_is_an_unrepairable_preamble_in_old_and_manifest_aof() {
        let directory = std::env::temp_dir().join(format!(
            "versal-short-vcs-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&directory).unwrap();
        let mut prefix = b"VCS\x01\x01\x00\x01\x04".to_vec();
        for length in 3..=8 {
            let bytes = &prefix[..length];
            let base = directory.join("appendonly.aof.1.base.rdb");
            fs::write(&base, bytes).unwrap();
            assert!(file_is_rdb(&base), "VCS prefix length {length}");
            assert_eq!(classify_path(&base), InputFileType::RdbPreamble);
            let options = AofCheckOptions {
                file: base.clone(),
                mode: AofCheckMode::Fix,
            };
            assert!(check_aof(&options).is_err());
            assert_eq!(fs::read(&base).unwrap(), bytes);
            let manifest = directory.join("appendonly.aof.manifest");
            fs::write(&manifest, b"file appendonly.aof.1.base.rdb seq 1 type b\n").unwrap();
            assert!(check_aof(&AofCheckOptions {
                file: manifest,
                mode: AofCheckMode::Fix,
            })
            .is_err());
            assert_eq!(fs::read(&base).unwrap(), bytes);
        }
        prefix[..3].copy_from_slice(b"VCX");
        let other = directory.join("other.aof");
        for bytes in [&b""[..], &b"V"[..], &b"VC"[..], &prefix[..]] {
            fs::write(&other, bytes).unwrap();
            assert!(!file_is_rdb(&other));
        }
        fs::remove_dir_all(directory).unwrap();
    }
}
