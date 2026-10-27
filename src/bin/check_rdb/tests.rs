use crate::check::{scan_rdb, scan_rdb_at, RdbCheckPhase};
use crate::cli::{parse_args, RdbCheckOptions, RdbCliAction, RdbCliError, RdbOutputFormat};
use crate::state::STATS_TYPE_NAMES;
use std::path::PathBuf;
use versal::foundation::io::persistence::codec::types::crc64;

fn minimal_rdb(version: u64, checksum: Option<u64>) -> Vec<u8> {
    let mut data = format!("REDIS{:04}", version).into_bytes();
    data.push(255);
    if let Some(checksum) = checksum {
        data.extend_from_slice(&checksum.to_le_bytes());
    }
    data
}

#[test]
fn scans_minimal_rdb_with_disabled_checksum() {
    let data = minimal_rdb(5, Some(0));
    let report = scan_rdb(&data).expect("minimal RDB should be valid");

    assert_eq!(report.state.rdbver, 5);
    assert_eq!(report.state.keys, 0);
    assert_eq!(report.state.final_offset, data.len() as u64);
    assert!(report
        .notices
        .iter()
        .any(|notice| notice.message.contains("checksum disabled")));
}

#[test]
fn rejects_bad_header_with_header_phase() {
    let failure = scan_rdb(b"NOTRDB000").expect_err("signature must be rejected");

    assert_eq!(failure.phase, RdbCheckPhase::Header);
    assert_eq!(failure.offset, 9);
    assert!(failure.message.contains("Wrong signature"));
}

#[test]
fn reports_truncated_opcode_and_object_phases() {
    let opcode_failure = scan_rdb(b"REDIS0005").expect_err("missing opcode must fail");
    assert_eq!(opcode_failure.phase, RdbCheckPhase::ReadType);
    assert_eq!(opcode_failure.offset, 9);

    let object_failure = scan_rdb(b"REDIS0005\x00\x01k").expect_err("missing object must fail");
    assert_eq!(object_failure.phase, RdbCheckPhase::ReadObject);
    assert_eq!(object_failure.state.keys, 0);
}

#[test]
fn rejects_checksum_mismatch_at_final_offset() {
    let mut data = minimal_rdb(5, None);
    let correct = crc64(0, &data);
    data.extend_from_slice(&correct.wrapping_add(1).to_le_bytes());

    let failure = scan_rdb(&data).expect_err("bad checksum must fail");

    assert_eq!(failure.phase, RdbCheckPhase::Checksum);
    assert_eq!(failure.offset, data.len() as u64);
    assert_eq!(failure.message, "RDB CRC error");
}

#[test]
fn returns_typed_stats_and_resets_expiry_after_object() {
    let mut data = b"REDIS0005".to_vec();
    data.extend_from_slice(&[252]);
    data.extend_from_slice(&0i64.to_le_bytes());
    data.extend_from_slice(&[0, 1, b'k', 1, b'v', 255]);
    data.extend_from_slice(&0u64.to_le_bytes());

    let report = scan_rdb_at(&data, 1).expect("one-string RDB should be valid");

    assert_eq!(report.state.keys, 1);
    assert_eq!(report.state.expires, 1);
    assert_eq!(report.state.already_expired, 1);
    assert_eq!(report.state.type_counts[0][0], 1);
    assert_eq!(STATS_TYPE_NAMES[0], "string");
    assert_eq!(report.state.final_offset, data.len() as u64);
}

#[test]
fn expiry_evaluation_uses_the_explicit_scan_time() {
    let mut data = b"REDIS0005".to_vec();
    data.extend_from_slice(&[252]);
    data.extend_from_slice(&1000i64.to_le_bytes());
    data.extend_from_slice(&[0, 1, b'k', 1, b'v', 255]);
    data.extend_from_slice(&0u64.to_le_bytes());

    let current = scan_rdb_at(&data, 1000).expect("RDB should be valid");
    let expired = scan_rdb_at(&data, 1001).expect("RDB should be valid");

    assert_eq!(current.state.already_expired, 0);
    assert_eq!(expired.state.already_expired, 1);
}

#[test]
fn parses_typed_rdb_output_formats() {
    let expected = |format| {
        Ok(RdbCliAction::Check(RdbCheckOptions {
            file: PathBuf::from("dump.rdb"),
            show_stats: true,
            format,
            output_file: None,
        }))
    };
    for (name, format) in [
        ("table", RdbOutputFormat::Table),
        ("info", RdbOutputFormat::Info),
        ("csv", RdbOutputFormat::Csv),
    ] {
        assert_eq!(
            parse_args(&[
                "dump.rdb".into(),
                "--stats".into(),
                "--format".into(),
                name.into(),
            ]),
            expected(format)
        );
    }
}

#[test]
fn rejects_missing_rdb_option_values() {
    assert_eq!(
        parse_args(&["dump.rdb".into(), "--format".into()]),
        Err(RdbCliError::Usage)
    );
    assert_eq!(
        parse_args(&["dump.rdb".into(), "--output".into()]),
        Err(RdbCliError::Usage)
    );
}

fn slot_import_rdb(count: u64, ranges: &[(u64, u64)]) -> Vec<u8> {
    let mut data = b"VALKEY080".to_vec();
    data.push(243);
    versal::foundation::io::persistence::save_string(&mut data, &[b'a'; 40]);
    for value in std::iter::once(count).chain(ranges.iter().flat_map(|&(start, end)| [start, end]))
    {
        data.push(0x81);
        data.extend_from_slice(&value.to_be_bytes());
    }
    data.push(255);
    let checksum = crc64(0, &data);
    data.extend_from_slice(&checksum.to_le_bytes());
    data
}

#[test]
fn slot_import_accepts_counts_and_ranges_without_extra_range_validation() {
    for ranges in [vec![], vec![(1, 2)], vec![(2, 1), (16384, u64::MAX - 1)]] {
        let data = slot_import_rdb(ranges.len() as u64, &ranges);
        let report = scan_rdb(&data).expect("SLOT IMPORT lengths should be valid");
        assert_eq!(report.state.final_offset, data.len() as u64);
        assert!(report
            .notices
            .iter()
            .any(|notice| notice.message.contains("Checksum OK")));
    }
}

#[test]
fn slot_import_rejects_length_error_in_each_numeric_field() {
    for (count, ranges, field_offset) in [
        (u64::MAX, vec![], 51),
        (1, vec![(u64::MAX, 2)], 60),
        (1, vec![(2, u64::MAX)], 69),
    ] {
        let data = slot_import_rdb(count, &ranges);
        let failure = scan_rdb(&data).expect_err("RDB_LENERR must fail even with a valid CRC");
        assert_eq!(failure.phase, RdbCheckPhase::ReadLength);
        assert_eq!(failure.offset, field_offset + 9);
        assert_eq!(failure.message, "Unexpected EOF reading SLOT IMPORT");
    }
}
