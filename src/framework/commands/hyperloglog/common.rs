command_macros::portable_command_support!();

use crate::foundation::io::protocol::Value;
use std::collections::HashSet;

const HLL_HDR_SIZE: usize = 16;
const HLL_MAGIC: &[u8; 4] = b"HYLL";
const HLL_DENSE: u8 = 0;
const HLL_SPARSE: u8 = 1;
const HLL_META_START: usize = 5;
const HLL_META_END: usize = 13;
pub const HLL_NON_INTEGER_INPUTS: u8 = 0x01;
pub const HLL_REGISTERS: usize = 16_384;
pub const HLL_ACTIVE_REGISTERS: usize = 16_384;
pub const HLL_SAMPLE_REGISTERS: usize = 512;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HllEncoding {
    Sparse,
    Dense,
}

#[derive(Debug, PartialEq, Eq)]
pub enum HllError {
    WrongType,
    InvalidObject,
}

#[derive(Clone, Debug)]
pub struct HllState {
    pub encoding: HllEncoding,
    registers: Vec<u8>,
}

impl HllState {
    pub fn empty() -> Self {
        Self {
            encoding: HllEncoding::Sparse,
            registers: vec![0; HLL_ACTIVE_REGISTERS],
        }
    }

    pub fn add(&mut self, value: &[u8]) -> bool {
        let (index, rank) = register_for(value);
        if rank > self.registers[index] {
            self.registers[index] = rank;
            true
        } else {
            false
        }
    }

    pub fn merge(&mut self, other: &HllState) {
        for (dst, src) in self.registers.iter_mut().zip(other.registers.iter()) {
            if *src > *dst {
                *dst = *src;
            }
        }
    }

    pub fn cardinality(&self) -> i64 {
        let mut sum = 0.0;
        let mut zeros = 0usize;
        for reg in &self.registers {
            if *reg == 0 {
                zeros += 1;
            }
            sum += rank_weight(*reg);
        }
        estimate_cardinality(sum, zeros, self.registers.len())
    }

    pub fn registers(&self) -> &[u8] {
        &self.registers
    }

    fn choose_encoding(&self, sparse_max_bytes: i64) -> HllEncoding {
        if self.encoding == HllEncoding::Dense {
            return HllEncoding::Dense;
        }
        if sparse_len(&self.registers) as i64 <= sparse_max_bytes {
            HllEncoding::Sparse
        } else {
            HllEncoding::Dense
        }
    }
}

pub fn empty_hll() -> Vec<u8> {
    serialize_hll(&HllState::empty(), 3000, true)
}

pub fn parse_hll(data: &[u8]) -> Result<HllState, HllError> {
    if data.len() < HLL_HDR_SIZE || &data[..4] != HLL_MAGIC {
        return Err(HllError::WrongType);
    }

    let encoding = match data[4] {
        HLL_SPARSE => HllEncoding::Sparse,
        HLL_DENSE => HllEncoding::Dense,
        _ => return Err(HllError::WrongType),
    };

    let mut registers = vec![0; HLL_ACTIVE_REGISTERS];
    let mut pos = HLL_HDR_SIZE;
    match encoding {
        HllEncoding::Dense => {
            if data.len() != HLL_HDR_SIZE + HLL_REGISTERS {
                return Err(HllError::WrongType);
            }
            let dense = &data[pos..pos + HLL_ACTIVE_REGISTERS];
            if dense.iter().any(|rank| *rank > 64) {
                return Err(HllError::InvalidObject);
            }
            registers.copy_from_slice(dense);
        }
        HllEncoding::Sparse => {
            let count = read_u32(data, &mut pos).ok_or(HllError::InvalidObject)? as usize;
            if count > HLL_ACTIVE_REGISTERS {
                return Err(HllError::InvalidObject);
            }
            load_sparse_registers(data, &mut pos, count, &mut registers)?;
            if pos != data.len() {
                return Err(HllError::InvalidObject);
            }
        }
    }

    Ok(HllState {
        encoding,
        registers,
    })
}

pub fn cardinality_from_bytes(data: &[u8]) -> Result<i64, HllError> {
    if data.len() < HLL_HDR_SIZE || &data[..4] != HLL_MAGIC {
        return Err(HllError::WrongType);
    }

    let encoding = match data[4] {
        HLL_SPARSE => HllEncoding::Sparse,
        HLL_DENSE => HllEncoding::Dense,
        _ => return Err(HllError::WrongType),
    };

    let mut pos = HLL_HDR_SIZE;
    if encoding == HllEncoding::Dense {
        if data.len() != HLL_HDR_SIZE + HLL_REGISTERS {
            return Err(HllError::WrongType);
        }
        let registers = &data[pos..pos + HLL_ACTIVE_REGISTERS];
        if registers.iter().any(|rank| *rank > 64) {
            return Err(HllError::InvalidObject);
        }
        return Ok(cardinality_from_registers(registers));
    }

    let count = read_u32(data, &mut pos).ok_or(HllError::InvalidObject)? as usize;
    if count > HLL_ACTIVE_REGISTERS {
        return Err(HllError::InvalidObject);
    }
    let mut seen = HashSet::with_capacity(count);
    let mut sum = (HLL_ACTIVE_REGISTERS - count) as f64;
    for _ in 0..count {
        let index = read_u16(data, &mut pos).ok_or(HllError::InvalidObject)?;
        let rank = *data.get(pos).ok_or(HllError::InvalidObject)?;
        pos += 1;
        if index as usize >= HLL_ACTIVE_REGISTERS || rank == 0 || rank > 64 || !seen.insert(index) {
            return Err(HllError::InvalidObject);
        }
        sum += rank_weight(rank);
    }

    match encoding {
        HllEncoding::Sparse if pos != data.len() => return Err(HllError::InvalidObject),
        HllEncoding::Dense if data[pos..].iter().any(|byte| *byte != 0) => {
            return Err(HllError::WrongType);
        }
        _ => {}
    }

    Ok(estimate_cardinality(
        sum,
        HLL_ACTIVE_REGISTERS - count,
        HLL_ACTIVE_REGISTERS,
    ))
}

pub fn register_for(value: &[u8]) -> (usize, u8) {
    let hash = hash64(value);
    let index = (hash as usize) & (HLL_ACTIVE_REGISTERS - 1);
    let rank = hll_rank(hash >> 14);
    (index, rank)
}

pub fn is_dense_hll(data: &[u8]) -> Result<bool, HllError> {
    if data.len() < HLL_HDR_SIZE || &data[..4] != HLL_MAGIC {
        return Err(HllError::WrongType);
    }

    match data[4] {
        HLL_DENSE => {
            if data.len() != HLL_HDR_SIZE + HLL_REGISTERS {
                return Err(HllError::WrongType);
            }
            if data[HLL_HDR_SIZE..].iter().any(|rank| *rank > 64) {
                return Err(HllError::InvalidObject);
            }
            Ok(true)
        }
        HLL_SPARSE => Ok(false),
        _ => Err(HllError::WrongType),
    }
}

pub fn cached_observed_count(data: &[u8]) -> Option<u64> {
    if data.get(14).copied().unwrap_or(0) & HLL_NON_INTEGER_INPUTS == 0 {
        return None;
    }
    let bytes: [u8; 8] = data.get(HLL_META_START..HLL_META_END)?.try_into().ok()?;
    Some(u64::from_le_bytes(bytes))
}

pub fn set_observed_count(data: &mut [u8], count: u64) {
    data[HLL_META_START..HLL_META_END].copy_from_slice(&count.to_le_bytes());
    data[14] |= HLL_NON_INTEGER_INPUTS;
}

pub fn looks_like_integer(value: &[u8]) -> bool {
    let digits = match value.first() {
        Some(b'-' | b'+') => &value[1..],
        Some(_) => value,
        None => return false,
    };
    !digits.is_empty() && digits.iter().all(|byte| byte.is_ascii_digit())
}

pub fn merge_bytes_into(data: &[u8], registers: &mut [u8]) -> Result<(), HllError> {
    if data.len() < HLL_HDR_SIZE || &data[..4] != HLL_MAGIC {
        return Err(HllError::WrongType);
    }

    let mut pos = HLL_HDR_SIZE;
    match data[4] {
        HLL_DENSE => {
            if data.len() != HLL_HDR_SIZE + HLL_REGISTERS {
                return Err(HllError::WrongType);
            }
            for (dst, rank) in registers
                .iter_mut()
                .zip(&data[HLL_HDR_SIZE..HLL_HDR_SIZE + HLL_ACTIVE_REGISTERS])
            {
                if *rank > 64 {
                    return Err(HllError::InvalidObject);
                }
                if *rank > *dst {
                    *dst = *rank;
                }
            }
        }
        HLL_SPARSE => {
            let count = read_u32(data, &mut pos).ok_or(HllError::InvalidObject)? as usize;
            if count > HLL_ACTIVE_REGISTERS {
                return Err(HllError::InvalidObject);
            }
            let mut seen = HashSet::with_capacity(count);
            for _ in 0..count {
                let index = read_u16(data, &mut pos).ok_or(HllError::InvalidObject)? as usize;
                let rank = *data.get(pos).ok_or(HllError::InvalidObject)?;
                pos += 1;
                if index >= HLL_ACTIVE_REGISTERS || rank == 0 || rank > 64 || !seen.insert(index) {
                    return Err(HllError::InvalidObject);
                }
                if rank > registers[index] {
                    registers[index] = rank;
                }
            }
            if pos != data.len() {
                return Err(HllError::InvalidObject);
            }
        }
        _ => return Err(HllError::WrongType),
    }

    Ok(())
}

pub fn merge_sampled_bytes_into(data: &[u8], registers: &mut [u8]) -> Result<(), HllError> {
    let stride = HLL_ACTIVE_REGISTERS / HLL_SAMPLE_REGISTERS;
    if data.len() < HLL_HDR_SIZE || &data[..4] != HLL_MAGIC {
        return Err(HllError::WrongType);
    }

    let mut pos = HLL_HDR_SIZE;
    match data[4] {
        HLL_DENSE => {
            if data.len() != HLL_HDR_SIZE + HLL_REGISTERS {
                return Err(HllError::WrongType);
            }
            for sample_index in 0..HLL_SAMPLE_REGISTERS {
                let rank = data[HLL_HDR_SIZE + sample_index * stride];
                if rank > 64 {
                    return Err(HllError::InvalidObject);
                }
                if rank > registers[sample_index] {
                    registers[sample_index] = rank;
                }
            }
        }
        HLL_SPARSE => {
            let count = read_u32(data, &mut pos).ok_or(HllError::InvalidObject)? as usize;
            if count > HLL_ACTIVE_REGISTERS {
                return Err(HllError::InvalidObject);
            }
            let mut seen = HashSet::with_capacity(count);
            for _ in 0..count {
                let index = read_u16(data, &mut pos).ok_or(HllError::InvalidObject)? as usize;
                let rank = *data.get(pos).ok_or(HllError::InvalidObject)?;
                pos += 1;
                if index >= HLL_ACTIVE_REGISTERS || rank == 0 || rank > 64 || !seen.insert(index) {
                    return Err(HllError::InvalidObject);
                }
                if index.is_multiple_of(stride) {
                    let sample_index = index / stride;
                    if rank > registers[sample_index] {
                        registers[sample_index] = rank;
                    }
                }
            }
            if pos != data.len() {
                return Err(HllError::InvalidObject);
            }
        }
        _ => return Err(HllError::WrongType),
    }

    Ok(())
}

pub fn serialize_hll(state: &HllState, sparse_max_bytes: i64, dirty_cache: bool) -> Vec<u8> {
    let encoding = state.choose_encoding(sparse_max_bytes);
    serialize_hll_as_min_len(state, encoding, dirty_cache)
}

// TODO(dead_code): PFDEBUG selects an explicit encoding only in the native command facade.
#[allow(dead_code)]
pub fn serialize_hll_as(state: &HllState, encoding: HllEncoding, dirty_cache: bool) -> Vec<u8> {
    serialize_hll_as_min_len(state, encoding, dirty_cache)
}

fn serialize_hll_as_min_len(state: &HllState, encoding: HllEncoding, dirty_cache: bool) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(HLL_MAGIC);
    buf.push(match encoding {
        HllEncoding::Sparse => HLL_SPARSE,
        HllEncoding::Dense => HLL_DENSE,
    });
    buf.extend_from_slice(&[0; 10]);
    buf.push(if dirty_cache { 0x80 } else { 0x00 });

    match encoding {
        HllEncoding::Dense => {
            buf.extend_from_slice(&state.registers);
            buf.resize(HLL_HDR_SIZE + HLL_REGISTERS, 0);
        }
        HllEncoding::Sparse => {
            save_sparse_registers(&state.registers, &mut buf);
        }
    }
    buf
}

pub fn clear_cache_dirty(data: &[u8]) -> Result<Vec<u8>, HllError> {
    parse_hll(data)?;
    let mut out = data.to_vec();
    out[15] = 0;
    Ok(out)
}

pub fn error_value(err: HllError) -> Value {
    match err {
        HllError::WrongType => crate::framework::CommandError::wrong_type(),
        HllError::InvalidObject => Value::error("INVALIDOBJ Corrupted HLL object"),
    }
}

fn sparse_len(registers: &[u8]) -> usize {
    HLL_HDR_SIZE + 4 + registers.iter().filter(|reg| **reg != 0).count() * 3
}

fn estimate_cardinality(sum: f64, zeros: usize, registers: usize) -> i64 {
    let m = registers as f64;
    let alpha = 0.7213 / (1.0 + 1.079 / m);
    let raw = alpha * m * m / sum;
    let estimate = if raw <= 2.5 * m && zeros > 0 {
        m * (m / zeros as f64).ln()
    } else {
        raw
    };
    estimate.round() as i64
}

fn rank_weight(rank: u8) -> f64 {
    f64::from_bits(((1023u64 - rank as u64) << 52) & 0x7ff0_0000_0000_0000)
}

pub fn cardinality_from_registers(registers: &[u8]) -> i64 {
    let mut sum = 0.0;
    let mut zeros = 0usize;
    for reg in registers {
        if *reg == 0 {
            zeros += 1;
        }
        sum += rank_weight(*reg);
    }
    estimate_cardinality(sum, zeros, registers.len())
}

fn save_sparse_registers(registers: &[u8], out: &mut Vec<u8>) {
    let count = registers.iter().filter(|reg| **reg != 0).count();
    out.extend_from_slice(&(count as u32).to_le_bytes());
    for (index, rank) in registers.iter().enumerate() {
        if *rank == 0 {
            continue;
        }
        out.extend_from_slice(&(index as u16).to_le_bytes());
        out.push(*rank);
    }
}

fn load_sparse_registers(
    data: &[u8],
    pos: &mut usize,
    count: usize,
    registers: &mut [u8],
) -> Result<(), HllError> {
    for _ in 0..count {
        let index = read_u16(data, pos).ok_or(HllError::InvalidObject)? as usize;
        let rank = *data.get(*pos).ok_or(HllError::InvalidObject)?;
        *pos += 1;
        if index >= HLL_ACTIVE_REGISTERS || rank == 0 || rank > 64 || registers[index] != 0 {
            return Err(HllError::InvalidObject);
        }
        registers[index] = rank;
    }
    Ok(())
}

fn hash64(value: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in value {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    // Finalize the FNV accumulator with a MurmurHash3-style avalanche. This
    // keeps sequential inputs such as "foo-1", "foo-2", ... evenly spread
    // across HLL registers.
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51afd7ed558ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ceb9fe1a85ec53);
    hash ^ (hash >> 33)
}

fn hll_rank(w: u64) -> u8 {
    if w == 0 {
        return 51;
    }
    ((w.leading_zeros() - 14 + 1).min(51)) as u8
}

fn read_u32(data: &[u8], pos: &mut usize) -> Option<u32> {
    let bytes: [u8; 4] = data.get(*pos..*pos + 4)?.try_into().ok()?;
    *pos += 4;
    Some(u32::from_le_bytes(bytes))
}

fn read_u16(data: &[u8], pos: &mut usize) -> Option<u16> {
    let bytes: [u8; 2] = data.get(*pos..*pos + 2)?.try_into().ok()?;
    *pos += 2;
    Some(u16::from_le_bytes(bytes))
}

#[cfg(test)]
#[path = "common/tests.rs"]
mod tests;
