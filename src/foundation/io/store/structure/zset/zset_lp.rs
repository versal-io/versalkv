// SPDX-License-Identifier: BSD-3-Clause

use crate::foundation::memory::value::{Encoding, EncodingChange, ZSetOps};

#[derive(Debug, Clone, PartialEq)]
pub struct ZSetLp {
    lp: Vec<u8>,
}

const LP_HDR_SIZE: usize = 6; // 4 bytes total_size + 2 bytes num_items
const LP_EOF: u8 = 0xFF;

impl ZSetLp {
    pub fn new() -> Self {
        let mut lp = vec![0u8; LP_HDR_SIZE + 1];
        lp[0..4].copy_from_slice(&((LP_HDR_SIZE as u32 + 1).to_le_bytes()));
        lp[4..6].copy_from_slice(&0u16.to_le_bytes());
        lp[LP_HDR_SIZE] = LP_EOF;
        ZSetLp { lp }
    }

    pub fn from_raw_bytes(lp: Vec<u8>) -> Option<Self> {
        if lp.len() < LP_HDR_SIZE + 1 {
            return None;
        }
        let total = u32::from_le_bytes(lp[0..4].try_into().ok()?) as usize;
        let num = u16::from_le_bytes(lp[4..6].try_into().ok()?) as usize;
        if total != lp.len() || *lp.last()? != LP_EOF {
            return None;
        }
        let mut count = 0;
        let mut p = LP_HDR_SIZE;
        while p < lp.len() - 1 {
            let entry_size = lp_entry_total_size(&lp, p)?;
            p += entry_size;
            count += 1;
        }
        if num != usize::from(u16::MAX) && count != num {
            return None;
        }
        if !count.is_multiple_of(2) {
            return None;
        }
        Some(ZSetLp { lp })
    }

    pub fn raw_bytes(&self) -> &[u8] {
        &self.lp
    }

    pub fn add_with_threshold(
        &mut self,
        member: &[u8],
        score: f64,
        max_entries: usize,
        max_value_size: usize,
    ) -> Result<bool, EncodingChange> {
        if member.len() > max_value_size {
            return Err(EncodingChange::Upgrade);
        }

        if let Some((pair_idx, _offset)) = lp_find_member(&self.lp, member) {
            let member_off = lp_nth_pair_member_offset(&self.lp, pair_idx);
            let score_offset = match lp_next_entry_offset(&self.lp, member_off) {
                Some(o) => o,
                None => return Ok(false),
            };
            let old_score = match lp_entry_data(&self.lp, score_offset) {
                Some(v) => v,
                None => return Ok(false),
            };
            let old_val = match parse_score(&old_score) {
                Some(v) => v,
                None => return Ok(false),
            };
            if old_val == score {
                return Ok(false);
            }
            lp_remove_pair(&mut self.lp, pair_idx);
            let (new_idx, new_off, _) = lp_find_insert_pos(&self.lp, member, score);
            lp_insert_pair_at(&mut self.lp, new_off, new_idx, member, score);
            return Ok(false);
        }

        let num_pairs = lp_num_pairs(&self.lp);
        if num_pairs >= max_entries {
            return Err(EncodingChange::Upgrade);
        }

        let (pair_idx, insert_off, _) = lp_find_insert_pos(&self.lp, member, score);
        lp_insert_pair_at(&mut self.lp, insert_off, pair_idx, member, score);
        Ok(true)
    }

    pub fn all_pairs(&self) -> Vec<(Vec<u8>, f64)> {
        let mut result = Vec::with_capacity(self.len());
        let mut p = LP_HDR_SIZE;
        while p < self.lp.len() - 1 {
            let member = lp_entry_data(&self.lp, p).unwrap_or_default().to_vec();
            p = lp_next_entry_offset(&self.lp, p).unwrap_or(p);
            let score_bytes = lp_entry_data(&self.lp, p).unwrap_or_default();
            let score = parse_score(&score_bytes).unwrap_or(0.0);
            p = lp_next_entry_offset(&self.lp, p).unwrap_or(p);
            result.push((member, score));
        }
        result
    }

    pub fn memory_usage(&self) -> usize {
        self.lp.capacity()
    }
}

impl Default for ZSetLp {
    fn default() -> Self {
        Self::new()
    }
}

// ===== ZSetOps impl =====

impl ZSetOps for ZSetLp {
    fn add(&mut self, member: &[u8], score: f64) -> Result<bool, EncodingChange> {
        self.add_with_threshold(member, score, 128, 64)
    }

    fn remove(&mut self, member: &[u8]) -> Result<bool, EncodingChange> {
        if let Some((pair_idx, _)) = lp_find_member(&self.lp, member) {
            lp_remove_pair(&mut self.lp, pair_idx);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn score(&self, member: &[u8]) -> Option<f64> {
        let (_idx, offset) = lp_find_member(&self.lp, member)?;
        let score_off = lp_next_entry_offset(&self.lp, offset)?;
        let data = lp_entry_data(&self.lp, score_off)?;
        parse_score(&data)
    }

    fn len(&self) -> usize {
        lp_num_pairs(&self.lp)
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn encoding(&self) -> Encoding {
        Encoding::Listpack
    }
}

fn lp_num_pairs(lp: &[u8]) -> usize {
    let cached = u16::from_le_bytes(lp[4..6].try_into().unwrap_or([0, 0]));
    if cached != u16::MAX {
        return usize::from(cached) / 2;
    }
    let mut count = 0usize;
    let mut offset = LP_HDR_SIZE;
    while offset < lp.len() - 1 {
        let Some(entry_size) = lp_entry_total_size(lp, offset) else {
            break;
        };
        offset += entry_size;
        count += 1;
    }
    count / 2
}

fn lp_set_header(buf: &mut [u8], total: u32, num_items: u16) {
    buf[0..4].copy_from_slice(&total.to_le_bytes());
    buf[4..6].copy_from_slice(&num_items.to_le_bytes());
}

fn lp_backlen_size(entry_len: usize) -> usize {
    if entry_len <= 127 {
        1
    } else if entry_len < 16384 {
        2
    } else if entry_len < 2097152 {
        3
    } else if entry_len < 268435456 {
        4
    } else {
        5
    }
}

fn lp_write_backlen(buf: &mut Vec<u8>, entry_len: usize) {
    let mut v = entry_len as u64;
    loop {
        buf.push((v & 0x7F) as u8);
        v >>= 7;
        if v == 0 {
            break;
        }
    }
}

fn lp_append_string_entry(buf: &mut Vec<u8>, data: &[u8]) {
    let slen = data.len();
    let body_start = buf.len();
    if slen <= 63 {
        buf.push(0x80 | slen as u8);
    } else if slen < 4096 {
        buf.push(0xE0 | ((slen >> 8) & 0x0F) as u8);
        buf.push((slen & 0xFF) as u8);
    } else {
        buf.push(0xF0);
        buf.extend_from_slice(&(slen as u32).to_le_bytes());
    }
    buf.extend_from_slice(data);
    let body_len = buf.len() - body_start;
    lp_write_backlen(buf, body_len);
}

fn lp_entry_total_size(lp: &[u8], p: usize) -> Option<usize> {
    let b0 = *lp.get(p)?;
    if b0 == LP_EOF {
        return None;
    }

    let (data_len, hdr_len) = if b0 & 0x80 == 0 {
        // uint7: 1 byte total
        (0usize, 1usize)
    } else if b0 & 0xC0 == 0x80 {
        // string, 6-bit len
        ((b0 & 0x3F) as usize, 1)
    } else if b0 & 0xE0 == 0xC0 {
        (1usize, 1usize)
    } else if b0 & 0xF0 == 0xE0 {
        // string, 14-bit len
        let b1 = *lp.get(p + 1)?;
        let len = (((b0 & 0x0F) as usize) << 8) | b1 as usize;
        (len, 2)
    } else if b0 == 0xF1 {
        // int16: 1 type byte + 2 data bytes = 3 bytes body
        (2usize, 1usize)
    } else if b0 == 0xF2 {
        // int24: 1 + 3 = 4 bytes
        (3usize, 1usize)
    } else if b0 == 0xF3 {
        // int32: 1 + 4 = 5 bytes
        (4usize, 1usize)
    } else if b0 == 0xF4 {
        // int64: 1 + 8 = 9 bytes
        (8usize, 1usize)
    } else if b0 == 0xF0 {
        // string, 32-bit len
        let len_bytes: [u8; 4] = lp.get(p + 1..p + 5)?.try_into().ok()?;
        (u32::from_le_bytes(len_bytes) as usize, 5)
    } else {
        return None;
    };

    let body_size = hdr_len + data_len;
    Some(body_size + lp_backlen_size(body_size))
}

fn lp_entry_data(lp: &[u8], p: usize) -> Option<Vec<u8>> {
    let b0 = *lp.get(p)?;
    if b0 == LP_EOF {
        return None;
    }

    if b0 & 0x80 == 0 {
        // uint7
        return Some((b0 as u64).to_string().into_bytes());
    }
    if b0 & 0xC0 == 0x80 {
        let slen = (b0 & 0x3F) as usize;
        return Some(lp.get(p + 1..p + 1 + slen)?.to_vec());
    }
    if b0 & 0xE0 == 0xC0 {
        // int13: signed 13-bit
        let b1 = *lp.get(p + 1)?;
        let raw = (((b0 & 0x1F) as i16) << 8) | b1 as i16;
        let val = if raw & 0x1000 != 0 {
            raw | 0xE000u16 as i16 // sign-extend to 16 bits
        } else {
            raw
        };
        return Some(val.to_string().into_bytes());
    }
    if b0 & 0xF0 == 0xE0 {
        let b1 = *lp.get(p + 1)?;
        let slen = (((b0 & 0x0F) as usize) << 8) | b1 as usize;
        return Some(lp.get(p + 2..p + 2 + slen)?.to_vec());
    }
    match b0 {
        0xF1 => {
            // int16
            let bytes: [u8; 2] = lp.get(p + 1..p + 3)?.try_into().ok()?;
            Some(i16::from_le_bytes(bytes).to_string().into_bytes())
        }
        0xF2 => {
            // int24
            let bytes = lp.get(p + 1..p + 4)?;
            let mut arr = [0u8; 4];
            arr[..3].copy_from_slice(bytes);
            let val = i32::from_le_bytes(arr);
            // sign-extend from 24 bits
            let val = if val & 0x0080_0000 != 0 {
                val | 0xFF00_0000u32 as i32
            } else {
                val
            };
            Some(val.to_string().into_bytes())
        }
        0xF3 => {
            // int32
            let bytes: [u8; 4] = lp.get(p + 1..p + 5)?.try_into().ok()?;
            Some(i32::from_le_bytes(bytes).to_string().into_bytes())
        }
        0xF4 => {
            // int64
            let bytes: [u8; 8] = lp.get(p + 1..p + 9)?.try_into().ok()?;
            Some(i64::from_le_bytes(bytes).to_string().into_bytes())
        }
        0xF0 => {
            // string, 32-bit len
            let len_bytes: [u8; 4] = lp.get(p + 1..p + 5)?.try_into().ok()?;
            let slen = u32::from_le_bytes(len_bytes) as usize;
            Some(lp.get(p + 5..p + 5 + slen)?.to_vec())
        }
        _ => None,
    }
}

fn lp_next_entry_offset(lp: &[u8], p: usize) -> Option<usize> {
    Some(p + lp_entry_total_size(lp, p)?)
}

fn lp_find_member(lp: &[u8], member: &[u8]) -> Option<(usize, usize)> {
    let mut pair_idx = 0;
    let mut p = LP_HDR_SIZE;
    while p < lp.len() - 1 {
        let entry_data = lp_entry_data(lp, p)?;
        if entry_data.as_slice() == member {
            return Some((pair_idx, p));
        }
        // skip member + score
        p = lp_next_entry_offset(lp, p)?;
        p = lp_next_entry_offset(lp, p)?;
        pair_idx += 1;
    }
    None
}

fn lp_nth_pair_member_offset(lp: &[u8], n: usize) -> usize {
    let mut p = LP_HDR_SIZE;
    for _ in 0..n {
        p = lp_next_entry_offset(lp, p).unwrap_or(p);
        p = lp_next_entry_offset(lp, p).unwrap_or(p);
    }
    p
}

fn lp_find_insert_pos(lp: &[u8], member: &[u8], score: f64) -> (usize, usize, bool) {
    let mut pair_idx = 0;
    let mut p = LP_HDR_SIZE;
    while p < lp.len() - 1 {
        let m = match lp_entry_data(lp, p) {
            Some(v) => v,
            None => break,
        };
        let score_off = match lp_next_entry_offset(lp, p) {
            Some(o) => o,
            None => break,
        };
        let s = match lp_entry_data(lp, score_off).and_then(|d| parse_score(&d)) {
            Some(v) => v,
            None => break,
        };

        match score.partial_cmp(&s).unwrap_or(std::cmp::Ordering::Equal) {
            std::cmp::Ordering::Less => return (pair_idx, p, false),
            std::cmp::Ordering::Equal => match member.cmp(&m) {
                std::cmp::Ordering::Less => return (pair_idx, p, false),
                std::cmp::Ordering::Equal => return (pair_idx, p, true),
                std::cmp::Ordering::Greater => {}
            },
            std::cmp::Ordering::Greater => {}
        }

        p = lp_next_entry_offset(lp, score_off).unwrap_or(p);
        pair_idx += 1;
    }
    (pair_idx, p, false)
}

fn lp_insert_pair_at(
    lp: &mut Vec<u8>,
    byte_offset: usize,
    _pair_idx: usize,
    member: &[u8],
    score: f64,
) {
    let score_str = format_score(score);

    let mut pair_buf = Vec::new();
    lp_append_string_entry(&mut pair_buf, member);
    lp_append_string_entry(&mut pair_buf, &score_str);

    let pair_len = pair_buf.len();
    let eof_pos = lp.len() - 1;

    lp.resize(lp.len() + pair_len, 0);

    lp.copy_within(byte_offset..=eof_pos, byte_offset + pair_len);

    lp[byte_offset..byte_offset + pair_len].copy_from_slice(&pair_buf);

    let new_total = lp.len() as u32;
    let old_num = u16::from_le_bytes(lp[4..6].try_into().unwrap_or([0, 0]));
    let new_num = old_num.saturating_add(2);
    lp_set_header(lp, new_total, new_num);
}

fn lp_remove_pair(lp: &mut Vec<u8>, pair_idx: usize) {
    let member_off = lp_nth_pair_member_offset(lp, pair_idx);
    let score_off = match lp_next_entry_offset(lp, member_off) {
        Some(o) => o,
        None => return,
    };
    let after_pair = match lp_next_entry_offset(lp, score_off) {
        Some(o) => o,
        None => return,
    };
    let pair_len = after_pair - member_off;

    let eof_pos = lp.len() - 1;
    lp.copy_within(after_pair..=eof_pos, member_off);
    lp.truncate(lp.len() - pair_len);

    let new_total = lp.len() as u32;
    let old_num = u16::from_le_bytes(lp[4..6].try_into().unwrap_or([0, 0]));
    let new_num = if old_num == u16::MAX {
        u16::MAX
    } else {
        old_num - 2
    };
    lp_set_header(lp, new_total, new_num);
}

fn parse_score(bytes: &[u8]) -> Option<f64> {
    let s = std::str::from_utf8(bytes).ok()?;
    match s {
        "inf" | "+inf" => Some(f64::INFINITY),
        "-inf" => Some(f64::NEG_INFINITY),
        _ => s.parse::<f64>().ok().filter(|v| !v.is_nan()),
    }
}

fn format_score(score: f64) -> Vec<u8> {
    if score.is_infinite() {
        if score > 0.0 {
            b"inf".to_vec()
        } else {
            b"-inf".to_vec()
        }
    } else if score.fract() == 0.0 && score.abs() < 1e15 {
        (score as i64).to_string().into_bytes()
    } else {
        score.to_string().into_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_is_empty() {
        let z = ZSetLp::new();
        assert!(z.is_empty());
        assert_eq!(z.len(), 0);
    }

    #[test]
    fn add_single_member() {
        let mut z = ZSetLp::new();
        assert_eq!(z.add_with_threshold(b"a", 1.0, 128, 64), Ok(true));
        assert_eq!(z.len(), 1);
        assert_eq!(z.score(b"a"), Some(1.0));
    }

    #[test]
    fn string_length_4096_uses_32bit_encoding() {
        let mut zset = ZSetLp::new();
        zset.add_with_threshold(&vec![b'x'; 4096], 1.0, 2, 4096)
            .unwrap();

        assert_eq!(
            &zset.raw_bytes()[LP_HDR_SIZE..LP_HDR_SIZE + 5],
            &[0xF0, 0, 16, 0, 0]
        );
    }

    #[test]
    fn add_updates_existing_member_score() {
        let mut z = ZSetLp::new();
        assert_eq!(z.add_with_threshold(b"a", 1.0, 128, 64), Ok(true));
        assert_eq!(z.add_with_threshold(b"a", 2.0, 128, 64), Ok(false));
        assert_eq!(z.len(), 1);
        assert_eq!(z.score(b"a"), Some(2.0));
    }

    #[test]
    fn add_multiple_maintains_sort_order() {
        let mut z = ZSetLp::new();
        z.add_with_threshold(b"c", 3.0, 128, 64).unwrap();
        z.add_with_threshold(b"a", 1.0, 128, 64).unwrap();
        z.add_with_threshold(b"b", 2.0, 128, 64).unwrap();

        let pairs = z.all_pairs();
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0].0, b"a");
        assert_eq!(pairs[0].1, 1.0);
        assert_eq!(pairs[1].0, b"b");
        assert_eq!(pairs[1].1, 2.0);
        assert_eq!(pairs[2].0, b"c");
        assert_eq!(pairs[2].1, 3.0);
    }

    #[test]
    fn same_score_sorted_lexicographically() {
        let mut z = ZSetLp::new();
        z.add_with_threshold(b"b", 1.0, 128, 64).unwrap();
        z.add_with_threshold(b"a", 1.0, 128, 64).unwrap();
        z.add_with_threshold(b"c", 1.0, 128, 64).unwrap();

        let pairs = z.all_pairs();
        assert_eq!(pairs[0].0, b"a");
        assert_eq!(pairs[1].0, b"b");
        assert_eq!(pairs[2].0, b"c");
        for p in &pairs {
            assert_eq!(p.1, 1.0);
        }
    }

    #[test]
    fn negative_scores() {
        let mut z = ZSetLp::new();
        z.add_with_threshold(b"a", -5.0, 128, 64).unwrap();
        z.add_with_threshold(b"b", 0.0, 128, 64).unwrap();
        z.add_with_threshold(b"c", -2.5, 128, 64).unwrap();

        let pairs = z.all_pairs();
        assert_eq!(pairs[0].0, b"a");
        assert_eq!(pairs[0].1, -5.0);
        assert_eq!(pairs[1].0, b"c");
        assert_eq!(pairs[1].1, -2.5);
        assert_eq!(pairs[2].0, b"b");
        assert_eq!(pairs[2].1, 0.0);
    }

    #[test]
    fn remove_existing() {
        let mut z = ZSetLp::new();
        z.add_with_threshold(b"a", 1.0, 128, 64).unwrap();
        z.add_with_threshold(b"b", 2.0, 128, 64).unwrap();
        assert_eq!(z.remove(b"a"), Ok(true));
        assert_eq!(z.len(), 1);
        assert_eq!(z.score(b"a"), None);
        assert_eq!(z.score(b"b"), Some(2.0));
    }

    #[test]
    fn remove_nonexistent() {
        let mut z = ZSetLp::new();
        z.add_with_threshold(b"a", 1.0, 128, 64).unwrap();
        assert_eq!(z.remove(b"b"), Ok(false));
        assert_eq!(z.len(), 1);
    }

    #[test]
    fn score_nonexistent() {
        let z = ZSetLp::new();
        assert_eq!(z.score(b"a"), None);
    }

    #[test]
    fn upgrade_on_max_entries() {
        let mut z = ZSetLp::new();
        for i in 0..5 {
            let member = format!("m{}", i);
            assert_eq!(
                z.add_with_threshold(member.as_bytes(), i as f64, 5, 64),
                Ok(true)
            );
        }
        assert_eq!(z.len(), 5);
        assert_eq!(
            z.add_with_threshold(b"m5", 5.0, 5, 64),
            Err(EncodingChange::Upgrade)
        );
        assert_eq!(z.len(), 5);
    }

    #[test]
    fn update_existing_does_not_upgrade() {
        let mut z = ZSetLp::new();
        for i in 0..5 {
            let member = format!("m{}", i);
            z.add_with_threshold(member.as_bytes(), i as f64, 5, 64)
                .unwrap();
        }
        assert_eq!(z.add_with_threshold(b"m0", 100.0, 5, 64), Ok(false));
        assert_eq!(z.len(), 5);
        assert_eq!(z.score(b"m0"), Some(100.0));
    }

    #[test]
    fn upgrade_on_large_member() {
        let mut z = ZSetLp::new();
        let big = vec![b'x'; 100];
        assert_eq!(
            z.add_with_threshold(&big, 1.0, 128, 64),
            Err(EncodingChange::Upgrade)
        );
        assert!(z.is_empty());
    }

    #[test]
    fn from_raw_bytes_round_trip() {
        let mut z = ZSetLp::new();
        z.add_with_threshold(b"a", 1.0, 128, 64).unwrap();
        z.add_with_threshold(b"b", 2.0, 128, 64).unwrap();
        z.add_with_threshold(b"c", 3.0, 128, 64).unwrap();

        let raw = z.raw_bytes().to_vec();
        let z2 = ZSetLp::from_raw_bytes(raw).unwrap();
        assert_eq!(z, z2);
        assert_eq!(z2.len(), 3);
    }

    #[test]
    fn from_raw_bytes_rejects_malformed() {
        assert!(ZSetLp::from_raw_bytes(vec![]).is_none());
        assert!(ZSetLp::from_raw_bytes(vec![0; 5]).is_none());
        let mut bad = vec![0u8; 7];
        bad[0..4].copy_from_slice(&100u32.to_le_bytes());
        bad[4..6].copy_from_slice(&0u16.to_le_bytes());
        bad[6] = LP_EOF;
        assert!(ZSetLp::from_raw_bytes(bad).is_none());
    }

    #[test]
    fn inf_scores() {
        let mut z = ZSetLp::new();
        z.add_with_threshold(b"a", f64::NEG_INFINITY, 128, 64)
            .unwrap();
        z.add_with_threshold(b"b", f64::INFINITY, 128, 64).unwrap();
        z.add_with_threshold(b"c", 0.0, 128, 64).unwrap();

        let pairs = z.all_pairs();
        assert_eq!(pairs[0].0, b"a");
        assert!(pairs[0].1.is_infinite() && pairs[0].1 < 0.0);
        assert_eq!(pairs[1].0, b"c");
        assert_eq!(pairs[1].1, 0.0);
        assert_eq!(pairs[2].0, b"b");
        assert!(pairs[2].1.is_infinite() && pairs[2].1 > 0.0);
    }

    #[test]
    fn integer_scores_stored_as_string() {
        let mut z = ZSetLp::new();
        z.add_with_threshold(b"a", 42.0, 128, 64).unwrap();
        assert_eq!(z.score(b"a"), Some(42.0));

        let pairs = z.all_pairs();
        assert_eq!(pairs[0].1, 42.0);
    }

    #[test]
    fn all_pairs_sorted() {
        let mut z = ZSetLp::new();
        z.add_with_threshold(b"m3", 3.0, 128, 64).unwrap();
        z.add_with_threshold(b"m1", 1.0, 128, 64).unwrap();
        z.add_with_threshold(b"m2", 2.0, 128, 64).unwrap();
        z.add_with_threshold(b"m0", 0.0, 128, 64).unwrap();

        let pairs = z.all_pairs();
        for (index, (member, score)) in pairs.iter().enumerate().take(4) {
            assert_eq!(member, format!("m{index}").as_bytes());
            assert_eq!(*score, index as f64);
        }
    }

    #[test]
    fn zsetops_impl_encoding() {
        let z = ZSetLp::new();
        assert_eq!(z.encoding(), Encoding::Listpack);
    }

    #[test]
    fn remove_all_returns_to_empty() {
        let mut z = ZSetLp::new();
        z.add_with_threshold(b"a", 1.0, 128, 64).unwrap();
        z.add_with_threshold(b"b", 2.0, 128, 64).unwrap();
        assert_eq!(z.remove(b"a"), Ok(true));
        assert_eq!(z.remove(b"b"), Ok(true));
        assert!(z.is_empty());
        assert_eq!(z.len(), 0);
    }
}
