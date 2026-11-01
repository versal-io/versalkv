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

fn final_edges_rdb(payload_len: usize) -> Vec<u8> {
    let mut data = b"VALKEY081".to_vec();
    data.push(0);
    versal::foundation::io::persistence::save_string(&mut data, b"key");
    let payload: Vec<_> = (0..payload_len).map(|index| (index % 251) as u8).collect();
    versal::foundation::io::persistence::save_string(&mut data, &payload);
    data.push(255);
    data.extend_from_slice(&crc64(0, &data).to_le_bytes());
    data
}

#[test]
fn final_edges_compressed_rdb_requires_exact_logical_endpoint() {
    use versal::foundation::io::compression::{StreamEncoder, StreamKind};
    use versal::foundation::io::persistence::rdb::compression::encode_rdb_stream;

    for checksum in [false, true] {
        let logical = final_edges_rdb(20);
        let mut extra = logical.clone();
        extra.extend_from_slice(b"X");
        let single_block = encode_rdb_stream(&extra, checksum).unwrap();
        let mut encoder = StreamEncoder::new(StreamKind::Rdb, checksum, checksum);
        let mut separate_block = encoder.flush_batch(&logical).unwrap();
        separate_block.extend(encoder.flush_batch(b"extra logical data").unwrap());
        separate_block.extend(encoder.finish().unwrap());
        for bytes in [single_block, separate_block] {
            let failure = scan_rdb(&bytes).expect_err("frame-internal data must be rejected");
            assert_eq!(failure.message, "Compressed RDB stream did not end cleanly");
            assert_eq!(failure.phase, RdbCheckPhase::Checksum);
            assert_eq!(failure.offset, logical.len() as u64);
            assert_eq!(failure.state.keys, 1);
            assert_eq!(failure.state.physical_offset, Some(bytes.len() as u64));
        }
    }
}

#[test]
fn final_edges_checker_preserves_frame_external_and_plain_legacy_tails() {
    use versal::foundation::io::persistence::rdb::compression::encode_rdb_stream;

    for checksum in [false, true] {
        let logical = final_edges_rdb(20);
        let mut compressed = encode_rdb_stream(&logical, checksum).unwrap();
        let physical_end = compressed.len();
        compressed.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
        let report = scan_rdb(&compressed).unwrap();
        assert_eq!(report.state.final_offset, logical.len() as u64);
        assert_eq!(report.state.physical_offset, Some(physical_end as u64));
        assert_eq!(report.state.keys, 1);

        let mut plain = logical.clone();
        plain.extend_from_slice(b"legacy tail");
        let report = scan_rdb(&plain).unwrap();
        assert_eq!(report.state.final_offset, logical.len() as u64);
        assert_eq!(report.state.physical_offset, None);
        plain[logical.len() - 1] ^= 1;
        assert_eq!(scan_rdb(&plain).unwrap_err().message, "RDB CRC error");
    }
}

#[test]
fn final_edges_checker_distinguishes_frame_finish_failures_by_logical_progress() {
    use versal::foundation::io::persistence::rdb::compression::encode_rdb_stream;

    for payload_len in [20, 150_000] {
        let logical = final_edges_rdb(payload_len);
        let encoded = encode_rdb_stream(&logical, true).unwrap();
        for missing in 1..=8 {
            let bytes = &encoded[..encoded.len() - missing];
            let failure = scan_rdb(bytes).expect_err("incomplete frame trailer must fail");
            assert_eq!(
                failure.message, "Compressed RDB stream did not end cleanly",
                "payload={payload_len}, missing={missing}"
            );
            assert_eq!(failure.phase, RdbCheckPhase::Checksum);
            assert_eq!(failure.offset, logical.len() as u64);
            assert_eq!(failure.state.keys, 1);
            assert!(failure.state.physical_offset.is_some());
        }
        let mut bad_content_checksum = encoded;
        *bad_content_checksum.last_mut().unwrap() ^= 1;
        let failure = scan_rdb(&bad_content_checksum).unwrap_err();
        assert_eq!(failure.message, "Compressed RDB stream did not end cleanly");
        assert_eq!(failure.offset, logical.len() as u64);
        assert_eq!(failure.state.keys, 1);
    }
}

#[test]
fn final_edges_checker_reports_corruption_before_complete_logical_rdb() {
    use versal::foundation::io::persistence::rdb::compression::encode_rdb_stream;

    let logical = final_edges_rdb(30_000);
    let encoded = encode_rdb_stream(&logical, true).unwrap();
    let mut corrupt_block = encoded.clone();
    let middle = corrupt_block.len() / 2;
    corrupt_block[middle] ^= 1;
    let incomplete_logical = &logical[..logical.len() - 1];
    let mut incomplete_checksum = encode_rdb_stream(incomplete_logical, true).unwrap();
    incomplete_checksum.truncate(incomplete_checksum.len() - 1);
    for bytes in [
        &encoded[..encoded.len() * 6 / 10],
        corrupt_block.as_slice(),
        incomplete_checksum.as_slice(),
    ] {
        let failure = scan_rdb(bytes).expect_err("corrupt compressed input must fail");
        assert_eq!(failure.message, "Corrupt compressed RDB stream");
        assert!(failure.state.physical_offset.is_some());
        assert!(failure.offset < logical.len() as u64);
    }
    for length in 3..7 {
        let failure = scan_rdb(&encoded[..length]).unwrap_err();
        assert_eq!(
            failure.message,
            "Invalid or unsupported RDB stream envelope"
        );
    }
}

fn assert_truncated_magic_envelope(data: &[u8]) {
    let failure = scan_rdb(data).expect_err("truncated VCS magic must fail");
    assert_eq!(
        failure.message,
        "Invalid or unsupported RDB stream envelope"
    );
    assert_eq!(failure.phase, RdbCheckPhase::Header);
    assert_eq!(failure.offset, 0);
    assert_eq!(failure.state.final_offset, 0);
    assert_eq!(failure.state.physical_offset, Some(data.len() as u64));
    assert_eq!(failure.state.keys, 0);
    assert!(failure.notices.is_empty());
}

#[test]
fn narrow_spec_checker_rejects_single_byte_magic_as_incompatible() {
    assert_truncated_magic_envelope(b"V");
}

#[test]
fn narrow_spec_checker_rejects_two_byte_magic_as_incompatible() {
    assert_truncated_magic_envelope(b"VC");
}

fn narrow_spec_raw_frame(logical: &[u8]) -> Vec<u8> {
    use xxhash_rust::xxh32::xxh32;

    let mut frame = b"VCS\x01\x01\x00\x01\x04\x22\x4d\x18\x54\x40".to_vec();
    frame.push((xxh32(&[0x54, 0x40], 0) >> 8) as u8);
    frame.extend_from_slice(&(0x8000_0000 | logical.len() as u32).to_le_bytes());
    frame.extend_from_slice(logical);
    frame.extend_from_slice(&xxh32(logical, 0).to_le_bytes());
    frame.extend_from_slice(&0u32.to_le_bytes());
    frame.extend_from_slice(&xxh32(logical, 0).to_le_bytes());
    frame
}

#[test]
fn narrow_spec_checker_preserves_raw_block_progress_without_checksum_tail() {
    use versal::foundation::io::persistence::rdb::compression::{
        decode_rdb_stream, CompressionErrorKind,
    };

    let logical = b"VALKEY081\xff\x00\x00\x00\x00\x00\x00\x00\x00";
    let frame = narrow_spec_raw_frame(logical);
    assert_eq!(logical.len(), 18);
    assert_eq!(frame.len(), 48);
    assert_eq!(&frame[..7], b"VCS\x01\x01\x00\x01");
    assert_eq!(&frame[7..11], b"\x04\x22\x4d\x18");
    assert_eq!(&frame[11..13], &[0x54, 0x40]);
    assert_eq!(&frame[14..18], &0x8000_0012u32.to_le_bytes());
    assert_eq!(&frame[18..36], logical);
    let report = scan_rdb(&frame).expect("complete raw frame should be valid");
    assert_eq!(report.state.final_offset, 18);
    assert_eq!(report.state.physical_offset, Some(48));
    assert_eq!(report.state.keys, 0);
    assert_eq!(scan_rdb(logical).unwrap().state.final_offset, 18);

    for physical_end in 36..40 {
        let input = &frame[..physical_end];
        let error = decode_rdb_stream(input, false).err().unwrap();
        assert_eq!(error.kind, CompressionErrorKind::Truncated);
        assert_eq!(error.physical_offset, 36);
        let failure = scan_rdb(input).expect_err("missing raw block checksum must fail");
        assert_eq!(
            failure.message, "Compressed RDB stream did not end cleanly",
            "physical_end={physical_end}"
        );
        assert_eq!(failure.phase, RdbCheckPhase::Checksum);
        assert_eq!(failure.offset, 18);
        assert_eq!(failure.state.final_offset, 18);
        assert_eq!(failure.state.rdbver, 81);
        assert_eq!(failure.state.keys, 0);
        assert_eq!(failure.state.physical_offset, Some(36));
        assert_eq!(failure.notices.last().unwrap().offset, 18);
        assert_eq!(failure.notices.last().unwrap().physical_offset, Some(36));
    }
}

#[test]
fn narrow_spec_checker_keeps_plain_short_headers_and_raw_corruption_distinct() {
    for plain in [b"".as_slice(), b"X", b"VX", b"Va", b"VS"] {
        let failure = scan_rdb(plain).unwrap_err();
        assert_eq!(failure.message, "File too short for RDB header");
        assert_eq!(failure.phase, RdbCheckPhase::Header);
        assert_eq!(failure.offset, plain.len() as u64);
        assert_eq!(failure.state.physical_offset, None);
    }

    let logical = b"VALKEY081\xff\x00\x00\x00\x00\x00\x00\x00\x00";
    let frame = narrow_spec_raw_frame(logical);
    let mut bad_block = frame.clone();
    bad_block[20] ^= 1;
    let mut bad_header = frame.clone();
    bad_header[13] ^= 1;
    let mut oversized_block = frame.clone();
    oversized_block[14..18].copy_from_slice(&0x8001_0001u32.to_le_bytes());
    for input in [bad_block, bad_header, oversized_block] {
        let failure = scan_rdb(&input).expect_err("corruption must not recover raw progress");
        assert_eq!(failure.message, "Corrupt compressed RDB stream");
        assert_eq!(failure.phase, RdbCheckPhase::Header);
        assert_eq!(failure.offset, 0);
        assert!(failure.state.physical_offset.is_some());
    }

    let incomplete_crc = narrow_spec_raw_frame(&logical[..17]);
    let failure = scan_rdb(&incomplete_crc[..incomplete_crc.len() - 1]).unwrap_err();
    assert_eq!(failure.message, "Corrupt compressed RDB stream");
    assert_eq!(failure.phase, RdbCheckPhase::Checksum);
    assert_eq!(failure.offset, 17);

    let failure = scan_rdb(&incomplete_crc[..35]).unwrap_err();
    assert_eq!(failure.message, "Corrupt compressed RDB stream");
    assert_eq!(failure.phase, RdbCheckPhase::Checksum);
    assert_eq!(failure.offset, 17);
    assert_eq!(failure.state.physical_offset, Some(35));
}

#[test]
fn narrow_spec_checker_recovers_raw_progress_after_a_verified_block() {
    let logical = b"VALKEY081\xff\x00\x00\x00\x00\x00\x00\x00\x00";
    let mut frame = narrow_spec_raw_frame(&logical[..9]);
    frame.truncate(31);
    frame.extend_from_slice(&0x8000_0009u32.to_le_bytes());
    frame.extend_from_slice(&logical[9..]);
    assert_eq!(frame.len(), 44);

    let failure = scan_rdb(&frame).expect_err("missing second block checksum must fail");
    assert_eq!(failure.message, "Compressed RDB stream did not end cleanly");
    assert_eq!(failure.phase, RdbCheckPhase::Checksum);
    assert_eq!(failure.offset, 18);
    assert_eq!(failure.state.final_offset, 18);
    assert_eq!(failure.state.physical_offset, Some(44));
}
