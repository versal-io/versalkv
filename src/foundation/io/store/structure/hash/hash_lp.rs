// SPDX-License-Identifier: BSD-3-Clause

use crate::foundation::memory::value::{Encoding, EncodingChange, HashOps};

#[derive(Debug, Clone, PartialEq)]
pub struct HashLp {
    lp: Vec<u8>,
}

const LP_HDR_SIZE: usize = 6; // 4 bytes total_size + 2 bytes num_items
const LP_EOF: u8 = 0xFF;

impl HashLp {
    pub fn new() -> Self {
        let mut lp = vec![0u8; LP_HDR_SIZE + 1];
        lp[0..4].copy_from_slice(&((LP_HDR_SIZE as u32 + 1).to_le_bytes()));
        lp[4..6].copy_from_slice(&0u16.to_le_bytes());
        lp[LP_HDR_SIZE] = LP_EOF;
        HashLp { lp }
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
        if count != num {
            return None;
        }
        if !num.is_multiple_of(2) {
            return None;
        }
        Some(HashLp { lp })
    }

    pub fn raw_bytes(&self) -> &[u8] {
        &self.lp
    }

    pub fn set_with_threshold(
        &mut self,
        field: &[u8],
        value: &[u8],
        max_entries: usize,
        max_value_size: usize,
    ) -> Result<bool, EncodingChange> {
        if field.len() > max_value_size || value.len() > max_value_size {
            return Err(EncodingChange::Upgrade);
        }

        if let Some(pair_idx) = lp_find_field(&self.lp, field) {
            lp_remove_pair(&mut self.lp, pair_idx);
            let insert_off = lp_nth_pair_key_offset(&self.lp, pair_idx);
            lp_insert_pair_at(&mut self.lp, insert_off, pair_idx, field, value);
            return Ok(false);
        }

        let num_pairs = lp_num_pairs(&self.lp);
        if num_pairs >= max_entries {
            return Err(EncodingChange::Upgrade);
        }

        let eof_pos = self.lp.len() - 1;
        lp_insert_pair_at(&mut self.lp, eof_pos, num_pairs, field, value);
        Ok(true)
    }

    pub fn all_pairs(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut result = Vec::with_capacity(self.len());
        let mut p = LP_HDR_SIZE;
        while p < self.lp.len() - 1 {
            let key = lp_entry_data(&self.lp, p).unwrap_or_default();
            p = lp_next_entry_offset(&self.lp, p).unwrap_or(p);
            let val = lp_entry_data(&self.lp, p).unwrap_or_default();
            p = lp_next_entry_offset(&self.lp, p).unwrap_or(p);
            result.push((key, val));
        }
        result
    }

    pub fn memory_usage(&self) -> usize {
        self.lp.capacity()
    }
}

impl Default for HashLp {
    fn default() -> Self {
        Self::new()
    }
}

// ===== HashOps impl =====

impl HashOps for HashLp {
    fn set(&mut self, field: &[u8], value: &[u8]) -> Result<bool, EncodingChange> {
        self.set_with_threshold(field, value, 512, 64)
    }

    fn get(&self, field: &[u8]) -> Option<Vec<u8>> {
        let pair_idx = lp_find_field(&self.lp, field)?;
        let key_off = lp_nth_pair_key_offset(&self.lp, pair_idx);
        let val_off = lp_next_entry_offset(&self.lp, key_off)?;
        lp_entry_data(&self.lp, val_off)
    }

    fn del(&mut self, field: &[u8]) -> Result<bool, EncodingChange> {
        if let Some(pair_idx) = lp_find_field(&self.lp, field) {
            lp_remove_pair(&mut self.lp, pair_idx);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn len(&self) -> usize {
        lp_num_pairs(&self.lp)
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn contains(&self, field: &[u8]) -> bool {
        lp_find_field(&self.lp, field).is_some()
    }

    fn encoding(&self) -> Encoding {
        Encoding::Listpack
    }
}

fn lp_num_pairs(lp: &[u8]) -> usize {
    (u16::from_le_bytes(lp[4..6].try_into().unwrap_or([0, 0])) as usize) / 2
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
        (0usize, 1usize)
    } else if b0 & 0xC0 == 0x80 {
        ((b0 & 0x3F) as usize, 1)
    } else if b0 & 0xE0 == 0xC0 {
        (1usize, 1usize)
    } else if b0 & 0xF0 == 0xE0 {
        let b1 = *lp.get(p + 1)?;
        let len = (((b0 & 0x0F) as usize) << 8) | b1 as usize;
        (len, 2)
    } else if b0 == 0xF1 {
        (2usize, 1usize)
    } else if b0 == 0xF2 {
        (3usize, 1usize)
    } else if b0 == 0xF3 {
        (4usize, 1usize)
    } else if b0 == 0xF4 {
        (8usize, 1usize)
    } else if b0 == 0xF0 {
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
        return Some((b0 as u64).to_string().into_bytes());
    }
    if b0 & 0xC0 == 0x80 {
        let slen = (b0 & 0x3F) as usize;
        return Some(lp.get(p + 1..p + 1 + slen)?.to_vec());
    }
    if b0 & 0xE0 == 0xC0 {
        let b1 = *lp.get(p + 1)?;
        let raw = (((b0 & 0x1F) as i16) << 8) | b1 as i16;
        let val = if raw & 0x1000 != 0 {
            raw | 0xE000u16 as i16
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
            let bytes: [u8; 2] = lp.get(p + 1..p + 3)?.try_into().ok()?;
            Some(i16::from_le_bytes(bytes).to_string().into_bytes())
        }
        0xF2 => {
            let bytes = lp.get(p + 1..p + 4)?;
            let mut arr = [0u8; 4];
            arr[..3].copy_from_slice(bytes);
            let val = i32::from_le_bytes(arr);
            let val = if val & 0x0080_0000 != 0 {
                val | 0xFF00_0000u32 as i32
            } else {
                val
            };
            Some(val.to_string().into_bytes())
        }
        0xF3 => {
            let bytes: [u8; 4] = lp.get(p + 1..p + 5)?.try_into().ok()?;
            Some(i32::from_le_bytes(bytes).to_string().into_bytes())
        }
        0xF4 => {
            let bytes: [u8; 8] = lp.get(p + 1..p + 9)?.try_into().ok()?;
            Some(i64::from_le_bytes(bytes).to_string().into_bytes())
        }
        0xF0 => {
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

fn lp_find_field(lp: &[u8], field: &[u8]) -> Option<usize> {
    let mut pair_idx = 0;
    let mut p = LP_HDR_SIZE;
    while p < lp.len() - 1 {
        let entry_data = lp_entry_data(lp, p)?;
        if entry_data.as_slice() == field {
            return Some(pair_idx);
        }
        p = lp_next_entry_offset(lp, p)?;
        p = lp_next_entry_offset(lp, p)?;
        pair_idx += 1;
    }
    None
}

fn lp_nth_pair_key_offset(lp: &[u8], n: usize) -> usize {
    let mut p = LP_HDR_SIZE;
    for _ in 0..n {
        p = lp_next_entry_offset(lp, p).unwrap_or(p);
        p = lp_next_entry_offset(lp, p).unwrap_or(p);
    }
    p
}

fn lp_insert_pair_at(
    lp: &mut Vec<u8>,
    byte_offset: usize,
    _pair_idx: usize,
    key: &[u8],
    value: &[u8],
) {
    let mut pair_buf = Vec::new();
    lp_append_string_entry(&mut pair_buf, key);
    lp_append_string_entry(&mut pair_buf, value);

    let pair_len = pair_buf.len();
    let eof_pos = lp.len() - 1;

    lp.resize(lp.len() + pair_len, 0);
    lp.copy_within(byte_offset..=eof_pos, byte_offset + pair_len);
    lp[byte_offset..byte_offset + pair_len].copy_from_slice(&pair_buf);

    let new_total = lp.len() as u32;
    let old_num = u16::from_le_bytes(lp[4..6].try_into().unwrap_or([0, 0]));
    let new_num = old_num + 2;
    lp_set_header(lp, new_total, new_num);
}

fn lp_remove_pair(lp: &mut Vec<u8>, pair_idx: usize) {
    let key_off = lp_nth_pair_key_offset(lp, pair_idx);
    let val_off = match lp_next_entry_offset(lp, key_off) {
        Some(o) => o,
        None => return,
    };
    let after_pair = match lp_next_entry_offset(lp, val_off) {
        Some(o) => o,
        None => return,
    };
    let pair_len = after_pair - key_off;

    let eof_pos = lp.len() - 1;
    lp.copy_within(after_pair..=eof_pos, key_off);
    lp.truncate(lp.len() - pair_len);

    let new_total = lp.len() as u32;
    let old_num = u16::from_le_bytes(lp[4..6].try_into().unwrap_or([0, 0]));
    let new_num = old_num - 2;
    lp_set_header(lp, new_total, new_num);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_is_empty() {
        let h = HashLp::new();
        assert!(h.is_empty());
        assert_eq!(h.len(), 0);
    }

    #[test]
    fn set_new_field_returns_true() {
        let mut h = HashLp::new();
        assert_eq!(h.set_with_threshold(b"k1", b"v1", 512, 64), Ok(true));
        assert_eq!(h.len(), 1);
        assert_eq!(h.get(b"k1"), Some(b"v1".to_vec()));
    }

    #[test]
    fn set_existing_field_returns_false() {
        let mut h = HashLp::new();
        assert_eq!(h.set_with_threshold(b"k1", b"v1", 512, 64), Ok(true));
        assert_eq!(h.set_with_threshold(b"k1", b"v2", 512, 64), Ok(false));
        assert_eq!(h.len(), 1);
        assert_eq!(h.get(b"k1"), Some(b"v2".to_vec()));
    }

    #[test]
    fn del_existing_returns_true() {
        let mut h = HashLp::new();
        h.set_with_threshold(b"k1", b"v1", 512, 64).unwrap();
        assert_eq!(h.del(b"k1"), Ok(true));
        assert_eq!(h.len(), 0);
        assert_eq!(h.get(b"k1"), None);
    }

    #[test]
    fn del_nonexistent_returns_false() {
        let mut h = HashLp::new();
        h.set_with_threshold(b"k1", b"v1", 512, 64).unwrap();
        assert_eq!(h.del(b"k2"), Ok(false));
        assert_eq!(h.len(), 1);
    }

    #[test]
    fn contains_works() {
        let mut h = HashLp::new();
        h.set_with_threshold(b"k1", b"v1", 512, 64).unwrap();
        assert!(h.contains(b"k1"));
        assert!(!h.contains(b"k2"));
    }

    #[test]
    fn multiple_fields_preserve_insertion_order() {
        let mut h = HashLp::new();
        h.set_with_threshold(b"a", b"1", 512, 64).unwrap();
        h.set_with_threshold(b"b", b"2", 512, 64).unwrap();
        h.set_with_threshold(b"c", b"3", 512, 64).unwrap();

        let pairs = h.all_pairs();
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0].0, b"a");
        assert_eq!(pairs[1].0, b"b");
        assert_eq!(pairs[2].0, b"c");
    }

    #[test]
    fn upgrade_on_max_entries() {
        let mut h = HashLp::new();
        for i in 0..5 {
            let key = format!("k{}", i);
            let val = format!("v{}", i);
            assert_eq!(
                h.set_with_threshold(key.as_bytes(), val.as_bytes(), 5, 64),
                Ok(true)
            );
        }
        assert_eq!(h.len(), 5);
        assert_eq!(
            h.set_with_threshold(b"k5", b"v5", 5, 64),
            Err(EncodingChange::Upgrade)
        );
        assert_eq!(h.len(), 5);
    }

    #[test]
    fn update_existing_does_not_upgrade() {
        let mut h = HashLp::new();
        for i in 0..5 {
            let key = format!("k{}", i);
            let val = format!("v{}", i);
            h.set_with_threshold(key.as_bytes(), val.as_bytes(), 5, 64)
                .unwrap();
        }
        assert_eq!(h.set_with_threshold(b"k0", b"newval", 5, 64), Ok(false));
        assert_eq!(h.len(), 5);
        assert_eq!(h.get(b"k0"), Some(b"newval".to_vec()));
    }

    #[test]
    fn upgrade_on_large_key() {
        let mut h = HashLp::new();
        let big = vec![b'x'; 100];
        assert_eq!(
            h.set_with_threshold(&big, b"v", 512, 64),
            Err(EncodingChange::Upgrade)
        );
        assert!(h.is_empty());
    }

    #[test]
    fn upgrade_on_large_value() {
        let mut h = HashLp::new();
        let big = vec![b'x'; 100];
        assert_eq!(
            h.set_with_threshold(b"k", &big, 512, 64),
            Err(EncodingChange::Upgrade)
        );
        assert!(h.is_empty());
    }

    #[test]
    fn from_raw_bytes_round_trip() {
        let mut h = HashLp::new();
        h.set_with_threshold(b"a", b"1", 512, 64).unwrap();
        h.set_with_threshold(b"b", b"2", 512, 64).unwrap();
        h.set_with_threshold(b"c", b"3", 512, 64).unwrap();

        let raw = h.raw_bytes().to_vec();
        let h2 = HashLp::from_raw_bytes(raw).unwrap();
        assert_eq!(h, h2);
        assert_eq!(h2.len(), 3);
    }

    #[test]
    fn from_raw_bytes_rejects_malformed() {
        assert!(HashLp::from_raw_bytes(vec![]).is_none());
        assert!(HashLp::from_raw_bytes(vec![0; 5]).is_none());
        let mut bad = vec![0u8; 7];
        bad[0..4].copy_from_slice(&100u32.to_le_bytes());
        bad[4..6].copy_from_slice(&0u16.to_le_bytes());
        bad[6] = LP_EOF;
        assert!(HashLp::from_raw_bytes(bad).is_none());
    }

    #[test]
    fn hashops_impl_encoding() {
        let h = HashLp::new();
        assert_eq!(h.encoding(), Encoding::Listpack);
    }

    #[test]
    fn remove_all_returns_to_empty() {
        let mut h = HashLp::new();
        h.set_with_threshold(b"a", b"1", 512, 64).unwrap();
        h.set_with_threshold(b"b", b"2", 512, 64).unwrap();
        assert_eq!(h.del(b"a"), Ok(true));
        assert_eq!(h.del(b"b"), Ok(true));
        assert!(h.is_empty());
        assert_eq!(h.len(), 0);
    }
}
