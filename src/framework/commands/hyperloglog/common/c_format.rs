use super::{HllError, HLL_HDR_SIZE, HLL_REGISTERS};

pub const DENSE_SIZE: usize = HLL_HDR_SIZE + HLL_REGISTERS * 6 / 8;

#[derive(Clone, Copy)]
struct SparseOp {
    span: usize,
    rank: u8,
    width: usize,
}

fn encoding(data: &[u8]) -> Result<u8, HllError> {
    if data.len() < HLL_HDR_SIZE || &data[..4] != b"HYLL" || data[4] > 1 {
        return Err(HllError::WrongType);
    }
    if data[4] == 0 && data.len() != DENSE_SIZE {
        return Err(HllError::WrongType);
    }
    Ok(data[4])
}

fn sparse_op(data: &[u8], pos: usize) -> Result<SparseOp, HllError> {
    let byte = *data.get(pos).ok_or(HllError::InvalidObject)?;
    Ok(match byte {
        0..=0x3f => SparseOp {
            span: usize::from(byte) + 1,
            rank: 0,
            width: 1,
        },
        0x40..=0x7f => SparseOp {
            span: ((usize::from(byte & 0x3f) << 8)
                | usize::from(*data.get(pos + 1).ok_or(HllError::InvalidObject)?))
                + 1,
            rank: 0,
            width: 2,
        },
        _ => SparseOp {
            span: usize::from(byte & 3) + 1,
            rank: ((byte >> 2) & 0x1f) + 1,
            width: 1,
        },
    })
}

pub fn decode(data: &[u8]) -> Result<Vec<u8>, HllError> {
    let encoding = encoding(data)?;
    let mut registers = vec![0; HLL_REGISTERS];
    if encoding == 0 {
        for (index, rank) in registers.iter_mut().enumerate() {
            *rank = dense_get(data, index);
        }
        return Ok(registers);
    }
    let mut pos = HLL_HDR_SIZE;
    let mut first = 0;
    while pos < data.len() {
        let op = sparse_op(data, pos)?;
        let end = first + op.span;
        if end > HLL_REGISTERS {
            return Err(HllError::InvalidObject);
        }
        registers[first..end].fill(op.rank);
        first = end;
        pos += op.width;
    }
    if first != HLL_REGISTERS {
        return Err(HllError::InvalidObject);
    }
    Ok(registers)
}

fn dense_get(data: &[u8], index: usize) -> u8 {
    let bit = index * 6;
    let pos = HLL_HDR_SIZE + bit / 8;
    let word = u16::from(data[pos]) | (u16::from(data.get(pos + 1).copied().unwrap_or(0)) << 8);
    ((word >> (bit % 8)) & 63) as u8
}

fn dense_set(data: &mut [u8], index: usize, rank: u8) -> bool {
    if dense_get(data, index) >= rank {
        return false;
    }
    let bit = index * 6;
    let pos = HLL_HDR_SIZE + bit / 8;
    let shift = bit % 8;
    data[pos] = (data[pos] & !(63u8 << shift)) | (rank << shift);
    if shift > 2 {
        data[pos + 1] = (data[pos + 1] & !(63u8 >> (8 - shift))) | (rank >> (8 - shift));
    }
    true
}

pub fn empty() -> Vec<u8> {
    let mut data = vec![0; HLL_HDR_SIZE];
    data[..4].copy_from_slice(b"HYLL");
    data[4] = 1;
    data.extend_from_slice(&[0x7f, 0xff]);
    data
}

pub fn to_dense(data: &[u8]) -> Result<Vec<u8>, HllError> {
    let registers = decode(data)?;
    Ok(encode_dense(&data[..HLL_HDR_SIZE], &registers))
}

fn encode_dense(header: &[u8], registers: &[u8]) -> Vec<u8> {
    let mut data = vec![0; DENSE_SIZE];
    data[..HLL_HDR_SIZE].copy_from_slice(header);
    data[4] = 0;
    for (index, rank) in registers.iter().copied().enumerate() {
        dense_set(&mut data, index, rank);
    }
    data
}

fn push_zero(out: &mut Vec<u8>, span: usize) {
    if span == 0 {
        return;
    }
    let value = span - 1;
    if span <= 64 {
        out.push(value as u8);
    } else {
        out.push(0x40 | ((value >> 8) as u8));
        out.push(value as u8);
    }
}

fn push_value(out: &mut Vec<u8>, rank: u8, span: usize) {
    if span > 0 {
        out.push(0x80 | ((rank - 1) << 2) | (span - 1) as u8);
    }
}

pub fn encode(registers: &[u8], use_dense: bool, sparse_max_bytes: i64) -> Vec<u8> {
    let mut data = empty();
    data.truncate(HLL_HDR_SIZE);
    let mut first = 0;
    while first < registers.len() {
        let rank = registers[first];
        if rank > 32 || use_dense {
            return encode_dense(&data[..HLL_HDR_SIZE], registers);
        }
        let mut end = first + 1;
        let max_span = if rank == 0 { HLL_REGISTERS } else { 4 };
        while end < registers.len() && end - first < max_span && registers[end] == rank {
            end += 1;
        }
        if rank == 0 {
            push_zero(&mut data, end - first);
        } else {
            push_value(&mut data, rank, end - first);
        }
        first = end;
    }
    if data.len() as i64 > sparse_max_bytes {
        encode_dense(&data[..HLL_HDR_SIZE], registers)
    } else {
        data
    }
}

pub fn add(data: &mut Vec<u8>, value: &[u8], sparse_max_bytes: i64) -> Result<bool, HllError> {
    let (index, rank) = register_for(value);
    set_register(data, index, rank, sparse_max_bytes)
}

pub fn set_register(
    data: &mut Vec<u8>,
    index: usize,
    rank: u8,
    sparse_max_bytes: i64,
) -> Result<bool, HllError> {
    let encoding = encoding(data)?;
    if index >= HLL_REGISTERS || rank > 63 {
        return Err(HllError::InvalidObject);
    }
    if encoding == 0 {
        return Ok(dense_set(data, index, rank));
    }
    if rank > 32 {
        let mut dense = to_dense(data)?;
        let changed = dense_set(&mut dense, index, rank);
        *data = dense;
        return Ok(changed);
    }

    // PFADD validates only the visited prefix, so an underfull tail does not
    // undo earlier successful elements. Full readers and promotion use decode.
    let mut pos = HLL_HDR_SIZE;
    let mut first = 0;
    let mut previous = HLL_HDR_SIZE;
    while pos < data.len() {
        let op = sparse_op(data, pos)?;
        let end = first + op.span;
        if end > HLL_REGISTERS {
            return Err(HllError::InvalidObject);
        }
        if index >= end {
            previous = pos;
            first = end;
            pos += op.width;
            continue;
        }
        if op.rank >= rank {
            return Ok(false);
        }
        let left = index - first;
        let right = end - index - 1;
        let mut replacement = Vec::with_capacity(5);
        if op.rank == 0 {
            push_zero(&mut replacement, left);
            push_value(&mut replacement, rank, 1);
            push_zero(&mut replacement, right);
        } else {
            push_value(&mut replacement, op.rank, left);
            push_value(&mut replacement, rank, 1);
            push_value(&mut replacement, op.rank, right);
        }
        // A zero-size delta must not promote even when the configured limit
        // is already below the current string length.
        if replacement.len() > op.width
            && (data.len() + replacement.len() - op.width) as i64 > sparse_max_bytes
        {
            let mut dense = to_dense(data)?;
            let changed = dense_set(&mut dense, index, rank);
            *data = dense;
            return Ok(changed);
        }
        data.splice(pos..pos + op.width, replacement);
        coalesce_values(data, previous);
        data[15] |= 0x80;
        return Ok(true);
    }
    Err(HllError::InvalidObject)
}

fn coalesce_values(data: &mut Vec<u8>, mut pos: usize) {
    for _ in 0..5 {
        if pos >= data.len() {
            break;
        }
        if data[pos] < 0x80 {
            pos += if data[pos] >= 0x40 { 2 } else { 1 };
            continue;
        }
        if pos + 1 < data.len() && data[pos + 1] >= 0x80 {
            let rank = ((data[pos] >> 2) & 31) + 1;
            let next_rank = ((data[pos + 1] >> 2) & 31) + 1;
            let span = usize::from(data[pos] & 3) + usize::from(data[pos + 1] & 3) + 2;
            if rank == next_rank && span <= 4 {
                data[pos] = 0x80 | ((rank - 1) << 2) | (span - 1) as u8;
                data.remove(pos + 1);
                continue;
            }
        }
        pos += 1;
    }
}

pub fn register_for(value: &[u8]) -> (usize, u8) {
    const MULTIPLIER: u64 = 0xc6a4_a793_5bd1_e995;
    let mut hash = 0xadc8_3b19 ^ (value.len() as u64).wrapping_mul(MULTIPLIER);
    let (chunks, tail) = value.as_chunks::<8>();
    for chunk in chunks {
        let mut word = u64::from_le_bytes(*chunk);
        word = word.wrapping_mul(MULTIPLIER);
        word ^= word >> 47;
        word = word.wrapping_mul(MULTIPLIER);
        hash ^= word;
        hash = hash.wrapping_mul(MULTIPLIER);
    }
    for (shift, byte) in tail.iter().copied().enumerate() {
        hash ^= u64::from(byte) << (shift * 8);
    }
    if !tail.is_empty() {
        hash = hash.wrapping_mul(MULTIPLIER);
    }
    hash ^= hash >> 47;
    hash = hash.wrapping_mul(MULTIPLIER);
    hash ^= hash >> 47;
    let index = hash as usize & (HLL_REGISTERS - 1);
    let rank = ((hash >> 14) | (1u64 << 50)).trailing_zeros() as u8 + 1;
    (index, rank)
}

pub fn cardinality(registers: &[u8]) -> i64 {
    let mut histogram = [0usize; 64];
    for rank in registers {
        histogram[usize::from(*rank)] += 1;
    }
    let m = HLL_REGISTERS as f64;
    let mut z = m * tau((m - histogram[51] as f64) / m);
    for count in histogram[1..=50].iter().rev() {
        z = (z + *count as f64) * 0.5;
    }
    z += m * sigma(histogram[0] as f64 / m);
    (0.721_347_520_444_481_7 * m * m / z).round() as i64
}

fn sigma(mut x: f64) -> f64 {
    if x == 1.0 {
        return f64::INFINITY;
    }
    let mut y = 1.0;
    let mut z = x;
    loop {
        x *= x;
        let previous = z;
        z += x * y;
        y *= 2.0;
        if previous == z {
            return z;
        }
    }
}

fn tau(mut x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
        return 0.0;
    }
    let mut y = 1.0;
    let mut z = 1.0 - x;
    loop {
        x = x.sqrt();
        let previous = z;
        y *= 0.5;
        z -= (1.0 - x).powi(2) * y;
        if previous == z {
            return z / 3.0;
        }
    }
}

pub fn decode_text(data: &[u8]) -> Result<String, HllError> {
    if encoding(data)? != 1 {
        return Err(HllError::WrongType);
    }
    decode(data)?;
    let mut text = String::new();
    let mut pos = HLL_HDR_SIZE;
    while pos < data.len() {
        let op = sparse_op(data, pos)?;
        if op.rank == 0 {
            text.push_str(&format!(
                "{}:{} ",
                if op.width == 1 { "z" } else { "Z" },
                op.span
            ));
        } else {
            text.push_str(&format!("v:{},{} ", op.rank, op.span));
        }
        pos += op.width;
    }
    text.pop();
    Ok(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sparse(payload: &[u8]) -> Vec<u8> {
        let mut data = empty();
        data.truncate(HLL_HDR_SIZE);
        data.extend_from_slice(payload);
        data
    }

    #[test]
    fn short_sparse_zero_delta_preserves_encoding_below_limit() {
        let mut data = sparse(&[0x40, 0x40, 0x7f, 0xbe]);
        assert!(set_register(&mut data, 0, 1, 0).unwrap());
        assert_eq!(&data[HLL_HDR_SIZE..], &[0x80, 0x3f, 0x7f, 0xbe]);
        assert_eq!(cardinality(&decode(&data).unwrap()), 1);
    }

    #[test]
    fn truncated_and_out_of_range_runs_fail_full_readers() {
        for payload in [
            &[0x3f, 0x3f, 0x3f, 0x3e, 0x7f][..],
            &[0x7f][..],
            &[0x5f, 0xff][..],
            &[0x7f, 0xff, 0][..],
            &[0x7f, 0xff, 0x80][..],
        ] {
            let data = sparse(payload);
            assert_eq!(decode(&data), Err(HllError::InvalidObject));
            assert_eq!(to_dense(&data), Err(HllError::InvalidObject));
            assert_eq!(decode_text(&data), Err(HllError::InvalidObject));
        }
        let mut data = sparse(&[0x3f, 0x3f, 0x3f, 0x3e, 0x7f]);
        let before = data.clone();
        assert_eq!(add(&mut data, b"a", 3000), Err(HllError::InvalidObject));
        assert_eq!(data, before);
    }

    #[test]
    fn partial_sparse_add_keeps_applied_prefix() {
        let mut data = sparse(&[0x5f, 0xff]);
        let (index, rank) = register_for(b"d");
        assert_eq!((index, rank), (7292, 1));
        assert!(register_for(b"a").0 >= 8192);
        assert!(add(&mut data, b"d", 3000).unwrap());
        assert_eq!(&data[HLL_HDR_SIZE..], &[0x5c, 0x7b, 0x80, 0x43, 0x82]);
        let after_first = data.clone();
        assert_eq!(add(&mut data, b"a", 3000), Err(HllError::InvalidObject));
        assert_eq!(data, after_first);
        assert_eq!(decode(&data), Err(HllError::InvalidObject));
        assert_eq!(data[15] & 0x80, 0x80);
    }

    #[test]
    fn promotion_validates_full_object_before_mutation() {
        let mut data = sparse(&[0x5f, 0xff]);
        let before = data.clone();
        assert_eq!(add(&mut data, b"d", 0), Err(HllError::InvalidObject));
        assert_eq!(data, before);
        assert_eq!(
            set_register(&mut data, 0, 33, 3000),
            Err(HllError::InvalidObject)
        );
        assert_eq!(data, before);
    }

    #[test]
    fn packed_dense_roundtrip_preserves_neighbors_and_last_register() {
        let registers: Vec<u8> = (0..HLL_REGISTERS).map(|index| (index % 64) as u8).collect();
        let data = encode(&registers, true, 3000);
        assert_eq!(data.len(), DENSE_SIZE);
        assert_eq!(decode(&data).unwrap(), registers);
    }

    #[test]
    fn standard_sparse_and_dense_estimates_agree_without_sampling() {
        let mut data = empty();
        for index in 0u32..2048 {
            add(&mut data, &index.to_le_bytes(), 100_000).unwrap();
        }
        let registers = decode(&data).unwrap();
        let dense = to_dense(&data).unwrap();
        assert_eq!(decode(&dense).unwrap(), registers);
        assert_eq!(
            cardinality(&registers),
            cardinality(&decode(&dense).unwrap())
        );
        assert!((cardinality(&registers) - 2048).abs() < 100);
    }

    #[test]
    fn invalid_headers_and_register_indices_return_errors() {
        for data in [
            &b""[..],
            &b"HYLL"[..],
            &b"HYLL\x02\0\0\0\0\0\0\0\0\0\0\0"[..],
        ] {
            assert_eq!(decode(data), Err(HllError::WrongType));
            assert_eq!(to_dense(data), Err(HllError::WrongType));
        }
        let mut data = empty();
        assert_eq!(
            set_register(&mut data, HLL_REGISTERS, 1, 3000),
            Err(HllError::InvalidObject)
        );
        assert_eq!(
            set_register(&mut data, 0, 64, 3000),
            Err(HllError::InvalidObject)
        );
    }
}
