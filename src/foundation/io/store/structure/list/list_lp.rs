// SPDX-License-Identifier: BSD-3-Clause

use crate::foundation::memory::value::{Encoding, EncodingChange, ListOps};
use bytes::Bytes;

#[derive(Debug, Clone, PartialEq)]
pub struct ListLp {
    lp: Vec<u8>,
}

const LP_HDR_SIZE: usize = 6; // 4 bytes total_size + 2 bytes num_items
const LP_EOF: u8 = 0xFF;

impl ListLp {
    pub fn new() -> Self {
        let mut lp = vec![0u8; LP_HDR_SIZE + 1];
        lp[0..4].copy_from_slice(&((LP_HDR_SIZE as u32 + 1).to_le_bytes()));
        lp[4..6].copy_from_slice(&0u16.to_le_bytes());
        lp[LP_HDR_SIZE] = LP_EOF;
        ListLp { lp }
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
        Some(ListLp { lp })
    }

    pub fn raw_bytes(&self) -> &[u8] {
        &self.lp
    }

    pub fn all_elements(&self) -> Vec<Vec<u8>> {
        let mut result = Vec::with_capacity(self.len());
        let mut p = LP_HDR_SIZE;
        while p < self.lp.len() - 1 {
            let elem = lp_entry_data(&self.lp, p).unwrap_or_default();
            p = lp_next_entry_offset(&self.lp, p).unwrap_or(p);
            result.push(elem);
        }
        result
    }

    pub fn push_back_with_threshold(
        &mut self,
        value: &[u8],
        max_entries: usize,
    ) -> Result<usize, EncodingChange> {
        if self.len() >= max_entries {
            return Err(EncodingChange::Upgrade);
        }
        let eof_pos = self.lp.len() - 1;
        lp_insert_entry_at(&mut self.lp, eof_pos, value);
        Ok(self.len())
    }

    pub fn push_front_with_threshold(
        &mut self,
        value: &[u8],
        max_entries: usize,
    ) -> Result<usize, EncodingChange> {
        if self.len() >= max_entries {
            return Err(EncodingChange::Upgrade);
        }
        lp_insert_entry_at(&mut self.lp, LP_HDR_SIZE, value);
        Ok(self.len())
    }

    pub fn memory_usage(&self) -> usize {
        self.lp.capacity()
    }
}

impl Default for ListLp {
    fn default() -> Self {
        Self::new()
    }
}

// ===== ListOps impl =====

impl ListOps for ListLp {
    fn push_back(&mut self, value: &[u8]) -> Result<usize, EncodingChange> {
        self.push_back_with_threshold(value, 512)
    }

    fn push_front(&mut self, value: &[u8]) -> Result<usize, EncodingChange> {
        self.push_front_with_threshold(value, 512)
    }

    fn pop_back(&mut self) -> Result<Option<Bytes>, EncodingChange> {
        if self.is_empty() {
            return Ok(None);
        }
        let last_idx = self.len() - 1;
        let offset = lp_nth_entry_offset(&self.lp, last_idx);
        let data = lp_entry_data(&self.lp, offset).unwrap_or_default();
        lp_remove_entry_at(&mut self.lp, last_idx);
        Ok(Some(Bytes::from(data)))
    }

    fn pop_front(&mut self) -> Result<Option<Bytes>, EncodingChange> {
        if self.is_empty() {
            return Ok(None);
        }
        let data = lp_entry_data(&self.lp, LP_HDR_SIZE).unwrap_or_default();
        lp_remove_entry_at(&mut self.lp, 0);
        Ok(Some(Bytes::from(data)))
    }

    fn len(&self) -> usize {
        u16::from_le_bytes(self.lp[4..6].try_into().unwrap_or([0, 0])) as usize
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn get(&self, index: isize) -> Option<Vec<u8>> {
        let idx = if index < 0 {
            let len = self.len() as isize;
            let adjusted = len + index;
            if adjusted < 0 {
                return None;
            }
            adjusted as usize
        } else {
            index as usize
        };
        if idx >= self.len() {
            return None;
        }
        let offset = lp_nth_entry_offset(&self.lp, idx);
        lp_entry_data(&self.lp, offset)
    }

    fn encoding(&self) -> Encoding {
        Encoding::Listpack
    }
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

fn lp_nth_entry_offset(lp: &[u8], n: usize) -> usize {
    let mut p = LP_HDR_SIZE;
    for _ in 0..n {
        p = lp_next_entry_offset(lp, p).unwrap_or(p);
    }
    p
}

fn lp_insert_entry_at(lp: &mut Vec<u8>, byte_offset: usize, data: &[u8]) {
    let mut entry_buf = Vec::new();
    lp_append_string_entry(&mut entry_buf, data);

    let entry_len = entry_buf.len();
    let eof_pos = lp.len() - 1;

    lp.resize(lp.len() + entry_len, 0);
    lp.copy_within(byte_offset..=eof_pos, byte_offset + entry_len);
    lp[byte_offset..byte_offset + entry_len].copy_from_slice(&entry_buf);

    let new_total = lp.len() as u32;
    let old_num = u16::from_le_bytes(lp[4..6].try_into().unwrap_or([0, 0]));
    let new_num = old_num + 1;
    lp_set_header(lp, new_total, new_num);
}

fn lp_remove_entry_at(lp: &mut Vec<u8>, idx: usize) {
    let entry_off = lp_nth_entry_offset(lp, idx);
    let after_entry = match lp_next_entry_offset(lp, entry_off) {
        Some(o) => o,
        None => return,
    };
    let entry_len = after_entry - entry_off;

    let eof_pos = lp.len() - 1;
    lp.copy_within(after_entry..=eof_pos, entry_off);
    lp.truncate(lp.len() - entry_len);

    let new_total = lp.len() as u32;
    let old_num = u16::from_le_bytes(lp[4..6].try_into().unwrap_or([0, 0]));
    let new_num = old_num - 1;
    lp_set_header(lp, new_total, new_num);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_is_empty() {
        let l = ListLp::new();
        assert!(l.is_empty());
        assert_eq!(l.len(), 0);
    }

    #[test]
    fn push_back_increases_len() {
        let mut l = ListLp::new();
        assert_eq!(l.push_back(b"a"), Ok(1));
        assert_eq!(l.push_back(b"b"), Ok(2));
        assert_eq!(l.len(), 2);
    }

    #[test]
    fn string_length_4096_uses_32bit_encoding() {
        let mut list = ListLp::new();
        list.push_back_with_threshold(&vec![b'x'; 4096], 2).unwrap();

        assert_eq!(
            &list.raw_bytes()[LP_HDR_SIZE..LP_HDR_SIZE + 5],
            &[0xF0, 0, 16, 0, 0]
        );
    }

    #[test]
    fn push_front_increases_len() {
        let mut l = ListLp::new();
        assert_eq!(l.push_front(b"a"), Ok(1));
        assert_eq!(l.push_front(b"b"), Ok(2));
        assert_eq!(l.len(), 2);
    }

    #[test]
    fn push_front_prepends_elements() {
        let mut l = ListLp::new();
        l.push_back(b"c").unwrap();
        l.push_front(b"b").unwrap();
        l.push_front(b"a").unwrap();
        let elems = l.all_elements();
        assert_eq!(elems, vec![b"a", b"b", b"c"]);
    }

    #[test]
    fn pop_back_returns_last_element() {
        let mut l = ListLp::new();
        l.push_back(b"a").unwrap();
        l.push_back(b"b").unwrap();
        l.push_back(b"c").unwrap();
        assert_eq!(l.pop_back().unwrap(), Some(Bytes::from("c")));
        assert_eq!(l.len(), 2);
        assert_eq!(l.pop_back().unwrap(), Some(Bytes::from("b")));
        assert_eq!(l.pop_back().unwrap(), Some(Bytes::from("a")));
        assert_eq!(l.pop_back().unwrap(), None);
        assert!(l.is_empty());
    }

    #[test]
    fn pop_front_returns_first_element() {
        let mut l = ListLp::new();
        l.push_back(b"a").unwrap();
        l.push_back(b"b").unwrap();
        l.push_back(b"c").unwrap();
        assert_eq!(l.pop_front().unwrap(), Some(Bytes::from("a")));
        assert_eq!(l.len(), 2);
        assert_eq!(l.pop_front().unwrap(), Some(Bytes::from("b")));
        assert_eq!(l.pop_front().unwrap(), Some(Bytes::from("c")));
        assert_eq!(l.pop_front().unwrap(), None);
        assert!(l.is_empty());
    }

    #[test]
    fn get_by_index() {
        let mut l = ListLp::new();
        l.push_back(b"a").unwrap();
        l.push_back(b"b").unwrap();
        l.push_back(b"c").unwrap();
        assert_eq!(l.get(0), Some(b"a".to_vec()));
        assert_eq!(l.get(1), Some(b"b".to_vec()));
        assert_eq!(l.get(2), Some(b"c".to_vec()));
        assert_eq!(l.get(3), None);
        assert_eq!(l.get(-1), Some(b"c".to_vec()));
        assert_eq!(l.get(-2), Some(b"b".to_vec()));
        assert_eq!(l.get(-3), Some(b"a".to_vec()));
        assert_eq!(l.get(-4), None);
    }

    #[test]
    fn upgrade_on_max_entries() {
        let mut l = ListLp::new();
        for i in 0..5 {
            let val = format!("v{}", i);
            assert_eq!(l.push_back_with_threshold(val.as_bytes(), 5), Ok(i + 1));
        }
        assert_eq!(l.len(), 5);
        assert_eq!(
            l.push_back_with_threshold(b"v5", 5),
            Err(EncodingChange::Upgrade)
        );
        assert_eq!(l.len(), 5);
    }

    #[test]
    fn from_raw_bytes_round_trip() {
        let mut l = ListLp::new();
        l.push_back(b"a").unwrap();
        l.push_back(b"b").unwrap();
        l.push_back(b"c").unwrap();

        let raw = l.raw_bytes().to_vec();
        let l2 = ListLp::from_raw_bytes(raw).unwrap();
        assert_eq!(l, l2);
        assert_eq!(l2.len(), 3);
    }

    #[test]
    fn from_raw_bytes_rejects_malformed() {
        assert!(ListLp::from_raw_bytes(vec![]).is_none());
        assert!(ListLp::from_raw_bytes(vec![0; 5]).is_none());
        let mut bad = vec![0u8; 7];
        bad[0..4].copy_from_slice(&100u32.to_le_bytes());
        bad[4..6].copy_from_slice(&0u16.to_le_bytes());
        bad[6] = LP_EOF;
        assert!(ListLp::from_raw_bytes(bad).is_none());
    }

    #[test]
    fn listops_impl_encoding() {
        let l = ListLp::new();
        assert_eq!(l.encoding(), Encoding::Listpack);
    }

    #[test]
    fn all_elements_empty() {
        let l = ListLp::new();
        assert!(l.all_elements().is_empty());
    }

    #[test]
    fn push_pop_many() {
        let mut l = ListLp::new();
        for i in 0..100 {
            let val = format!("v{}", i);
            l.push_back(val.as_bytes()).unwrap();
        }
        assert_eq!(l.len(), 100);
        for i in 0..100 {
            let expected = format!("v{}", i);
            assert_eq!(l.get(i as isize), Some(expected.into_bytes()));
        }
    }
}
