// SPDX-License-Identifier: BSD-3-Clause

use crate::foundation::memory::value::{Encoding, EncodingChange, SetOps};

#[derive(Debug, Clone, PartialEq)]
pub struct SetLp {
    lp: Vec<u8>,
}

const LP_HDR_SIZE: usize = 6; // 4 bytes total_size + 2 bytes num_items
const LP_EOF: u8 = 0xFF;

impl SetLp {
    pub fn new() -> Self {
        let mut lp = vec![0u8; LP_HDR_SIZE + 1];
        lp[0..4].copy_from_slice(&((LP_HDR_SIZE as u32 + 1).to_le_bytes()));
        lp[4..6].copy_from_slice(&0u16.to_le_bytes());
        lp[LP_HDR_SIZE] = LP_EOF;
        SetLp { lp }
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
        Some(SetLp { lp })
    }

    pub fn raw_bytes(&self) -> &[u8] {
        &self.lp
    }

    pub fn all_members(&self) -> Vec<Vec<u8>> {
        let mut result = Vec::with_capacity(self.len());
        let mut p = LP_HDR_SIZE;
        while p < self.lp.len() - 1 {
            let elem = lp_entry_data(&self.lp, p).unwrap_or_default();
            p = lp_next_entry_offset(&self.lp, p).unwrap_or(p);
            result.push(elem);
        }
        result
    }

    pub fn add_with_threshold(
        &mut self,
        member: &[u8],
        max_entries: usize,
        max_value_len: usize,
    ) -> Result<bool, EncodingChange> {
        if self.contains(member) {
            return Ok(false);
        }
        if self.len() >= max_entries || member.len() > max_value_len {
            return Err(EncodingChange::Upgrade);
        }
        let eof_pos = self.lp.len() - 1;
        lp_insert_entry_at(&mut self.lp, eof_pos, member);
        Ok(true)
    }

    fn index_of(&self, member: &[u8]) -> Option<usize> {
        let mut p = LP_HDR_SIZE;
        let mut idx = 0;
        while p < self.lp.len() - 1 {
            if let Some(data) = lp_entry_data(&self.lp, p) {
                if data.as_slice() == member {
                    return Some(idx);
                }
            }
            p = lp_next_entry_offset(&self.lp, p).unwrap_or(p);
            idx += 1;
        }
        None
    }

    pub fn memory_usage(&self) -> usize {
        self.lp.capacity()
    }
}

impl Default for SetLp {
    fn default() -> Self {
        Self::new()
    }
}

// ===== SetOps impl =====

impl SetOps for SetLp {
    fn add(&mut self, member: &[u8]) -> Result<bool, EncodingChange> {
        self.add_with_threshold(member, 128, 64)
    }

    fn remove(&mut self, member: &[u8]) -> Result<bool, EncodingChange> {
        if let Some(idx) = self.index_of(member) {
            lp_remove_entry_at(&mut self.lp, idx);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn contains(&self, member: &[u8]) -> bool {
        self.index_of(member).is_some()
    }

    fn len(&self) -> usize {
        u16::from_le_bytes(self.lp[4..6].try_into().unwrap_or([0, 0])) as usize
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn members(&self) -> Vec<Vec<u8>> {
        self.all_members()
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
        let s = SetLp::new();
        assert!(s.is_empty());
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn add_increases_len() {
        let mut s = SetLp::new();
        assert_eq!(s.add(b"a"), Ok(true));
        assert_eq!(s.add(b"b"), Ok(true));
        assert_eq!(s.len(), 2);
    }

    #[test]
    fn string_length_4096_uses_32bit_encoding() {
        let mut set = SetLp::new();
        set.add_with_threshold(&vec![b'x'; 4096], 2, 4096).unwrap();

        assert_eq!(
            &set.raw_bytes()[LP_HDR_SIZE..LP_HDR_SIZE + 5],
            &[0xF0, 0, 16, 0, 0]
        );
    }

    #[test]
    fn add_duplicate_returns_false() {
        let mut s = SetLp::new();
        assert_eq!(s.add(b"a"), Ok(true));
        assert_eq!(s.add(b"a"), Ok(false));
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn contains_works() {
        let mut s = SetLp::new();
        s.add(b"a").unwrap();
        s.add(b"b").unwrap();
        s.add(b"c").unwrap();
        assert!(s.contains(b"a"));
        assert!(s.contains(b"b"));
        assert!(s.contains(b"c"));
        assert!(!s.contains(b"d"));
    }

    #[test]
    fn remove_works() {
        let mut s = SetLp::new();
        s.add(b"a").unwrap();
        s.add(b"b").unwrap();
        assert_eq!(s.remove(b"a"), Ok(true));
        assert_eq!(s.len(), 1);
        assert!(!s.contains(b"a"));
        assert!(s.contains(b"b"));
        assert_eq!(s.remove(b"x"), Ok(false));
    }

    #[test]
    fn members_returns_all() {
        let mut s = SetLp::new();
        s.add(b"a").unwrap();
        s.add(b"b").unwrap();
        s.add(b"c").unwrap();
        let members = s.members();
        assert_eq!(members.len(), 3);
        assert!(members.contains(&b"a".to_vec()));
        assert!(members.contains(&b"b".to_vec()));
        assert!(members.contains(&b"c".to_vec()));
    }

    #[test]
    fn upgrade_on_max_entries() {
        let mut s = SetLp::new();
        for i in 0..5 {
            let val = format!("v{}", i);
            assert_eq!(s.add_with_threshold(val.as_bytes(), 5, 64), Ok(true));
        }
        assert_eq!(s.len(), 5);
        assert_eq!(
            s.add_with_threshold(b"v5", 5, 64),
            Err(EncodingChange::Upgrade)
        );
        assert_eq!(s.len(), 5);
    }

    #[test]
    fn upgrade_on_max_value_len() {
        let mut s = SetLp::new();
        let big = vec![b'x'; 100];
        assert_eq!(
            s.add_with_threshold(&big, 128, 64),
            Err(EncodingChange::Upgrade)
        );
        assert!(s.is_empty());
    }

    #[test]
    fn from_raw_bytes_round_trip() {
        let mut s = SetLp::new();
        s.add(b"a").unwrap();
        s.add(b"b").unwrap();
        s.add(b"c").unwrap();

        let raw = s.raw_bytes().to_vec();
        let s2 = SetLp::from_raw_bytes(raw).unwrap();
        assert_eq!(s, s2);
        assert_eq!(s2.len(), 3);
    }

    #[test]
    fn from_raw_bytes_rejects_malformed() {
        assert!(SetLp::from_raw_bytes(vec![]).is_none());
        assert!(SetLp::from_raw_bytes(vec![0; 5]).is_none());
        let mut bad = vec![0u8; 7];
        bad[0..4].copy_from_slice(&100u32.to_le_bytes());
        bad[4..6].copy_from_slice(&0u16.to_le_bytes());
        bad[6] = LP_EOF;
        assert!(SetLp::from_raw_bytes(bad).is_none());
    }

    #[test]
    fn setops_impl_encoding() {
        let s = SetLp::new();
        assert_eq!(s.encoding(), Encoding::Listpack);
    }

    #[test]
    fn from_raw_bytes_accepts_int13_entry() {
        fn hex(s: &str) -> Vec<u8> {
            (0..s.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
                .collect()
        }
        let raw = hex(
            "400000000200c14902b432323130333430303030303332343232343233343433333\
             03431303131333234303134333332313334333431303031333134343135ff",
        );
        let s = SetLp::from_raw_bytes(raw).expect("int13 listpack must parse");
        assert_eq!(s.len(), 2);
        let members = s.all_members();
        assert!(members.contains(&b"329".to_vec()));
        assert!(members.contains(&b"2210340000032422423443304101132401433213434100131441".to_vec()));
    }

    #[test]
    fn add_remove_many() {
        let mut s = SetLp::new();
        for i in 0..100 {
            let val = format!("v{}", i);
            s.add_with_threshold(val.as_bytes(), 1000, 64).unwrap();
        }
        assert_eq!(s.len(), 100);
        for i in 0..100 {
            let val = format!("v{}", i);
            assert!(s.contains(val.as_bytes()));
        }
        for i in 0..50 {
            let val = format!("v{}", i);
            assert_eq!(s.remove(val.as_bytes()), Ok(true));
        }
        assert_eq!(s.len(), 50);
    }
}
