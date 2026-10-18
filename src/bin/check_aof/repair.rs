use crate::cli::AofCheckMode;
use crate::scan::{AofScanOutcome, AofScanReport, AofScanSnapshot};
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

const VERIFY_BUFFER_SIZE: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AofRepairReason {
    Corruption,
    Timestamp { target: u64, found: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AofRepairEligibility {
    Eligible,
    NotLastFile,
    NothingBeforeTimestamp,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AofRepairPlan {
    None,
    Truncate {
        offset: u64,
        reason: AofRepairReason,
        eligibility: AofRepairEligibility,
        snapshot: AofScanSnapshot,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AofRepairResult {
    NotNeeded,
    Truncated { offset: u64 },
}

pub fn plan_repair(report: &AofScanReport, mode: AofCheckMode, last_file: bool) -> AofRepairPlan {
    let requested = match (report.outcome, mode) {
        (AofScanOutcome::Corrupt, AofCheckMode::Fix) => {
            Some((report.ok_up_to, AofRepairReason::Corruption))
        }
        (
            AofScanOutcome::TimestampBoundary { offset, timestamp },
            AofCheckMode::Timestamp(target),
        ) => Some((
            offset,
            AofRepairReason::Timestamp {
                target,
                found: timestamp,
            },
        )),
        _ => None,
    };
    let Some((offset, reason)) = requested else {
        return AofRepairPlan::None;
    };
    truncate_plan(report, offset, reason, last_file)
}

fn truncate_plan(
    report: &AofScanReport,
    offset: u64,
    reason: AofRepairReason,
    last_file: bool,
) -> AofRepairPlan {
    let eligibility = if matches!(reason, AofRepairReason::Timestamp { .. }) && offset == 0 {
        AofRepairEligibility::NothingBeforeTimestamp
    } else if !last_file {
        AofRepairEligibility::NotLastFile
    } else {
        AofRepairEligibility::Eligible
    };
    AofRepairPlan::Truncate {
        offset,
        reason,
        eligibility,
        snapshot: report.snapshot,
    }
}

pub fn apply_repair(path: &Path, plan: &AofRepairPlan) -> io::Result<AofRepairResult> {
    let AofRepairPlan::Truncate {
        offset,
        eligibility,
        snapshot,
        ..
    } = plan
    else {
        return Ok(AofRepairResult::NotNeeded);
    };
    if *eligibility != AofRepairEligibility::Eligible {
        return Err(repair_error("AOF repair plan is not eligible for mutation"));
    }
    validate_plan(*offset, snapshot)?;
    mutate_verified_file(path, *offset, snapshot)?;
    Ok(AofRepairResult::Truncated { offset: *offset })
}

fn validate_plan(offset: u64, snapshot: &AofScanSnapshot) -> io::Result<()> {
    if offset > snapshot.original_size {
        return Err(repair_error("AOF repair plan has an invalid offset"));
    }
    Ok(())
}

fn mutate_verified_file(path: &Path, offset: u64, snapshot: &AofScanSnapshot) -> io::Result<()> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    let current_size = file.metadata()?.len();
    if current_size != snapshot.original_size || offset > current_size {
        return Err(repair_error("AOF changed after it was scanned"));
    }
    let (read_size, digest) = stream_digest(&mut file)?;
    if read_size != current_size || digest != snapshot.digest {
        return Err(repair_error("AOF changed after it was scanned"));
    }
    file.set_len(offset)
}

fn stream_digest(file: &mut File) -> io::Result<(u64, [u8; 32])> {
    file.seek(SeekFrom::Start(0))?;
    let mut digest = Sha256::new();
    let mut total = 0u64;
    let mut buffer = [0u8; VERIFY_BUFFER_SIZE];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        total = total
            .checked_add(count as u64)
            .ok_or_else(|| repair_error("Current AOF is too large to verify"))?;
        digest.update(&buffer[..count]);
    }
    Ok((total, digest.finalize().into()))
}

fn repair_error(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
