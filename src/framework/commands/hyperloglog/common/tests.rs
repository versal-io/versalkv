use super::super::common::*;

#[test]
fn hll_round_trip_sparse() {
    let mut state = HllState::empty();
    state.add(b"a");
    state.add(b"b");

    let encoded = serialize_hll_as(&state, HllEncoding::Sparse, true);
    let decoded = parse_hll(&encoded).unwrap();

    assert_eq!(decoded.encoding, HllEncoding::Sparse);
    assert_eq!(decoded.registers(), state.registers());
    assert_eq!(encoded[15], 0x80);
}

#[test]
fn hll_rejects_trailing_sparse_bytes() {
    let mut encoded = empty_hll();
    encoded.extend_from_slice(b"tail");

    assert!(matches!(parse_hll(&encoded), Err(HllError::InvalidObject)));
}

#[test]
fn hll_rejects_invalid_header_as_wrong_type() {
    let mut encoded = empty_hll();
    encoded[0] = b'X';
    assert!(matches!(parse_hll(&encoded), Err(HllError::WrongType)));

    let mut encoded = empty_hll();
    encoded[4] = b'x';
    assert!(matches!(parse_hll(&encoded), Err(HllError::WrongType)));
}

#[test]
fn hll_cardinality_rejects_impossible_register_count() {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(HLL_MAGIC);
    encoded.push(HLL_SPARSE);
    encoded.extend_from_slice(&[0; 10]);
    encoded.push(0x80);
    encoded.extend_from_slice(&((HLL_REGISTERS as u32) + 1).to_le_bytes());

    assert!(matches!(
        cardinality_from_bytes(&encoded),
        Err(HllError::InvalidObject)
    ));
}

#[test]
fn hll_dense_has_fixed_length() {
    let mut state = HllState::empty();
    state.add(b"a");

    let encoded = serialize_hll_as(&state, HllEncoding::Dense, true);

    assert!(encoded.starts_with(b"HYLL\0"));
    assert_eq!(encoded.len(), HLL_HDR_SIZE + HLL_REGISTERS);
    assert!(parse_hll(&encoded).is_ok());
}
