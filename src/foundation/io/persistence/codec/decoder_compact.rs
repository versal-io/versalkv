pub(crate) fn lzf_decompress(input: &[u8], expected_len: usize) -> Option<Vec<u8>> {
    let mut out: Vec<u8> = Vec::with_capacity(expected_len);
    let mut ip = 0usize;
    while ip < input.len() {
        let ctrl = input[ip] as usize;
        ip += 1;
        if ctrl < (1 << 5) {
            let run = ctrl + 1;
            let end = ip.checked_add(run)?;
            let lit = input.get(ip..end)?;
            out.extend_from_slice(lit);
            ip = end;
        } else {
            let mut len = ctrl >> 5;
            if len == 7 {
                len += *input.get(ip)? as usize;
                ip += 1;
            }
            let next = *input.get(ip)? as usize;
            ip += 1;
            let back = ((ctrl & 0x1f) << 8) + 1 + next;
            if back > out.len() {
                return None;
            }
            let copy_len = len + 2;
            if back >= copy_len {
                let src = out.len() - back;
                out.extend_from_within(src..src + copy_len);
            } else {
                let src = out.len() - back;
                out.reserve(copy_len);
                for i in 0..copy_len {
                    let b = out[src + (i % back)];
                    out.push(b);
                }
            }
        }
    }
    if out.len() != expected_len {
        return None;
    }
    Some(out)
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

fn lp_backlen_matches(backlen: &[u8], entry_len: usize) -> bool {
    if backlen.is_empty() || backlen.len() > 5 || backlen[0] & 0x80 != 0 {
        return false;
    }
    let mut len = backlen[0] as usize;
    for &byte in &backlen[1..] {
        if byte & 0x80 == 0 {
            return false;
        }
        len = (len << 7) | ((byte & 0x7f) as usize);
    }
    len == entry_len
}

fn lp_write_backlen(buf: &mut Vec<u8>, entry_len: usize) {
    if entry_len <= 127 {
        buf.push(entry_len as u8);
    } else if entry_len < 16384 {
        buf.push((entry_len >> 7) as u8);
        buf.push(((entry_len & 127) as u8) | 128);
    } else if entry_len < 2097152 {
        buf.push((entry_len >> 14) as u8);
        buf.push((((entry_len >> 7) & 127) as u8) | 128);
        buf.push(((entry_len & 127) as u8) | 128);
    } else if entry_len < 268435456 {
        buf.push((entry_len >> 21) as u8);
        buf.push((((entry_len >> 14) & 127) as u8) | 128);
        buf.push((((entry_len >> 7) & 127) as u8) | 128);
        buf.push(((entry_len & 127) as u8) | 128);
    } else {
        buf.push((entry_len >> 28) as u8);
        buf.push((((entry_len >> 21) & 127) as u8) | 128);
        buf.push((((entry_len >> 14) & 127) as u8) | 128);
        buf.push((((entry_len >> 7) & 127) as u8) | 128);
        buf.push(((entry_len & 127) as u8) | 128);
    }
}

pub(crate) fn listpack_decode(lp: &[u8]) -> Option<Vec<Vec<u8>>> {
    if lp.len() < 7 {
        return None;
    }
    let total = u32::from_le_bytes(lp[0..4].try_into().ok()?) as usize;
    if total != lp.len() {
        return None;
    }
    let expected_count = u16::from_le_bytes(lp[4..6].try_into().ok()?);
    let mut items: Vec<Vec<u8>> = Vec::new();
    let mut p = 6usize;
    loop {
        let b0 = *lp.get(p)?;
        if b0 == 0xFF {
            break;
        }
        let (value, encoding_data_size): (Vec<u8>, usize) = if b0 & 0x80 == 0 {
            (uint_to_string(b0 as u64), 1)
        } else if b0 & 0xC0 == 0x80 {
            let slen = (b0 & 0x3F) as usize;
            let start = p + 1;
            let end = start.checked_add(slen)?;
            (lp.get(start..end)?.to_vec(), 1 + slen)
        } else if b0 & 0xE0 == 0xC0 {
            let p1 = *lp.get(p + 1)?;
            let uval = (((b0 & 0x1f) as u64) << 8) | p1 as u64;
            (int13_to_string(uval), 2)
        } else if b0 == 0xF1 {
            let raw = lp.get(p + 1..p + 3)?;
            let uval = u16::from_le_bytes(raw.try_into().ok()?) as u64;
            (signed_to_string(uval, 1 << 15, u16::MAX as u64), 3)
        } else if b0 == 0xF2 {
            let r = lp.get(p + 1..p + 4)?;
            let uval = (r[0] as u64) | (r[1] as u64) << 8 | (r[2] as u64) << 16;
            (signed_to_string(uval, 1 << 23, (u32::MAX >> 8) as u64), 4)
        } else if b0 == 0xF3 {
            let raw = lp.get(p + 1..p + 5)?;
            let uval = u32::from_le_bytes(raw.try_into().ok()?) as u64;
            (signed_to_string(uval, 1 << 31, u32::MAX as u64), 5)
        } else if b0 == 0xF4 {
            let raw = lp.get(p + 1..p + 9)?;
            let uval = u64::from_le_bytes(raw.try_into().ok()?);
            (signed_to_string(uval, 1u64 << 63, u64::MAX), 9)
        } else if b0 & 0xF0 == 0xE0 {
            let p1 = *lp.get(p + 1)?;
            let slen = (((b0 & 0x0F) as usize) << 8) | p1 as usize;
            let start = p + 2;
            let end = start.checked_add(slen)?;
            (lp.get(start..end)?.to_vec(), 2 + slen)
        } else if b0 == 0xF0 {
            let raw = lp.get(p + 1..p + 5)?;
            let slen = u32::from_le_bytes(raw.try_into().ok()?) as usize;
            let start = p + 5;
            let end = start.checked_add(slen)?;
            (lp.get(start..end)?.to_vec(), 5 + slen)
        } else {
            return None;
        };
        items.push(value);
        let backlen_size = lp_backlen_size(encoding_data_size);
        let backlen_start = p.checked_add(encoding_data_size)?;
        let backlen_end = backlen_start.checked_add(backlen_size)?;
        if !lp_backlen_matches(lp.get(backlen_start..backlen_end)?, encoding_data_size) {
            return None;
        }
        p = backlen_end;
    }
    if p != lp.len() - 1 {
        return None;
    }
    if expected_count != u16::MAX && expected_count as usize != items.len() {
        return None;
    }
    Some(items)
}

fn uint_to_string(v: u64) -> Vec<u8> {
    v.to_string().into_bytes()
}

fn int13_to_string(uval: u64) -> Vec<u8> {
    signed_to_string(uval, 1 << 12, 8191)
}

fn signed_to_string(uval: u64, negstart: u64, negmax: u64) -> Vec<u8> {
    let val: i64 = if uval >= negstart {
        let u = negmax - uval;
        -(u as i64) - 1
    } else {
        uval as i64
    };
    val.to_string().into_bytes()
}

pub(crate) fn ziplist_decode(zl: &[u8]) -> Option<Vec<Vec<u8>>> {
    if zl.len() < 11 {
        return None;
    }
    let total = u32::from_le_bytes(zl[0..4].try_into().ok()?) as usize;
    if total != zl.len() {
        return None;
    }
    let tail_offset = u32::from_le_bytes(zl[4..8].try_into().ok()?) as usize;
    let expected_count = u16::from_le_bytes(zl[8..10].try_into().ok()?);
    let mut items: Vec<Vec<u8>> = Vec::new();
    let mut p = 10usize;
    let mut prev_entry_len = 0usize;
    let mut last_entry_start = 10usize;
    loop {
        let b = *zl.get(p)?;
        if b == 0xFF {
            break;
        }
        let entry_start = p;
        let (prevlensize, prevlen) = if b < 254 {
            (1usize, b as usize)
        } else {
            let raw: [u8; 4] = zl.get(p + 1..p + 5)?.try_into().ok()?;
            (5usize, u32::from_le_bytes(raw) as usize)
        };
        if prevlen != prev_entry_len {
            return None;
        }
        p += prevlensize;
        let enc = *zl.get(p)?;
        if enc & 0xC0 < 0xC0 {
            let (lensize, slen) = match enc & 0xC0 {
                0x00 => (1usize, (enc & 0x3F) as usize),
                0x40 => {
                    let p1 = *zl.get(p + 1)?;
                    (2usize, (((enc & 0x3F) as usize) << 8) | p1 as usize)
                }
                0x80 => {
                    let raw = zl.get(p + 1..p + 5)?;
                    (5usize, u32::from_be_bytes(raw.try_into().ok()?) as usize)
                }
                _ => return None,
            };
            let start = p + lensize;
            let end = start.checked_add(slen)?;
            items.push(zl.get(start..end)?.to_vec());
            p = end;
        } else {
            let (isize, val): (usize, i64) = match enc {
                0xC0 => {
                    let raw = zl.get(p + 1..p + 3)?;
                    (2, i16::from_le_bytes(raw.try_into().ok()?) as i64)
                }
                0xD0 => {
                    let raw = zl.get(p + 1..p + 5)?;
                    (4, i32::from_le_bytes(raw.try_into().ok()?) as i64)
                }
                0xE0 => {
                    let raw = zl.get(p + 1..p + 9)?;
                    (8, i64::from_le_bytes(raw.try_into().ok()?))
                }
                0xF0 => {
                    let r = zl.get(p + 1..p + 4)?;
                    let u = (r[0] as u32) | (r[1] as u32) << 8 | (r[2] as u32) << 16;
                    let v = ((u << 8) as i32) >> 8;
                    (3, v as i64)
                }
                0xFE => {
                    let v = *zl.get(p + 1)? as i8;
                    (1, v as i64)
                }
                _ => {
                    if (0xF1..=0xFD).contains(&enc) {
                        (0, ((enc & 0x0F) as i64) - 1)
                    } else {
                        return None;
                    }
                }
            };
            items.push(val.to_string().into_bytes());
            p += 1 + isize;
        }
        prev_entry_len = p.checked_sub(entry_start)?;
        last_entry_start = entry_start;
    }
    if p != zl.len() - 1 {
        return None;
    }
    let item_count = items.len();
    if item_count == 0 {
        if tail_offset != 10 {
            return None;
        }
    } else if tail_offset != last_entry_start {
        return None;
    }
    if expected_count != u16::MAX && expected_count as usize != item_count {
        return None;
    }
    Some(items)
}

pub(crate) fn intset_decode(is: &[u8]) -> Option<Vec<Vec<u8>>> {
    Some(
        intset_decode_ints(is)?
            .into_iter()
            .map(|v| v.to_string().into_bytes())
            .collect(),
    )
}

pub(crate) fn intset_decode_ints(is: &[u8]) -> Option<Vec<i64>> {
    if is.len() < 8 {
        return None;
    }
    let enc = u32::from_le_bytes(is[0..4].try_into().ok()?) as usize;
    let len = u32::from_le_bytes(is[4..8].try_into().ok()?) as usize;
    if enc != 2 && enc != 4 && enc != 8 {
        return None;
    }
    let mut items: Vec<i64> = Vec::with_capacity(len);
    let mut p = 8usize;
    for _ in 0..len {
        let end = p.checked_add(enc)?;
        let raw = is.get(p..end)?;
        let v: i64 = match enc {
            2 => i16::from_le_bytes(raw.try_into().ok()?) as i64,
            4 => i32::from_le_bytes(raw.try_into().ok()?) as i64,
            _ => i64::from_le_bytes(raw.try_into().ok()?),
        };
        items.push(v);
        p = end;
    }
    Some(items)
}

pub(crate) fn intset_encode(values: &[i64]) -> Vec<u8> {
    let mut enc: u32 = 2;
    for &v in values {
        if v < i16::MIN as i64 || v > i16::MAX as i64 {
            enc = 4;
        }
        if v < i32::MIN as i64 || v > i32::MAX as i64 {
            enc = 8;
            break;
        }
    }
    let mut buf = Vec::with_capacity(8 + values.len() * enc as usize);
    buf.extend_from_slice(&enc.to_le_bytes());
    buf.extend_from_slice(&(values.len() as u32).to_le_bytes());
    match enc {
        2 => {
            for &v in values {
                buf.extend_from_slice(&(v as i16).to_le_bytes());
            }
        }
        4 => {
            for &v in values {
                buf.extend_from_slice(&(v as i32).to_le_bytes());
            }
        }
        _ => {
            for &v in values {
                buf.extend_from_slice(&v.to_le_bytes());
            }
        }
    }
    buf
}

fn lp_encode_entry(item: &[u8]) -> Vec<u8> {
    use crate::foundation::io::store::structure::string::encoding::int_encodable;
    if let Some(v) = int_encodable(item) {
        return lp_encode_integer(v);
    }
    let size = item.len();
    let mut entry = Vec::new();
    if size < 64 {
        entry.push(0x80 | size as u8);
    } else if size < 4096 {
        entry.push(0xE0 | ((size >> 8) as u8));
        entry.push((size & 0xff) as u8);
    } else {
        entry.push(0xF0);
        entry.extend_from_slice(&(size as u32).to_le_bytes());
    }
    entry.extend_from_slice(item);
    entry
}

fn lp_encode_integer(v: i64) -> Vec<u8> {
    let mut e = Vec::new();
    if (0..=127).contains(&v) {
        e.push(v as u8);
    } else if (-4096..=4095).contains(&v) {
        let uv = if v < 0 { (1i64 << 13) + v } else { v };
        e.push(((uv >> 8) as u8) | 0xC0);
        e.push((uv & 0xff) as u8);
    } else if (-32768..=32767).contains(&v) {
        let uv = if v < 0 { (1i64 << 16) + v } else { v };
        e.push(0xF1);
        e.push((uv & 0xff) as u8);
        e.push((uv >> 8) as u8);
    } else if (-8388608..=8388607).contains(&v) {
        let uv = if v < 0 { (1i64 << 24) + v } else { v };
        e.push(0xF2);
        e.push((uv & 0xff) as u8);
        e.push((uv >> 8) as u8);
        e.push((uv >> 16) as u8);
    } else if (-(1i64 << 31)..=((1i64 << 31) - 1)).contains(&v) {
        let uv = if v < 0 { (1i64 << 32) + v } else { v };
        e.push(0xF3);
        e.push((uv & 0xff) as u8);
        e.push((uv >> 8) as u8);
        e.push((uv >> 16) as u8);
        e.push((uv >> 24) as u8);
    } else {
        e.push(0xF4);
        e.extend_from_slice(&(v as u64).to_le_bytes());
    }
    e
}

pub(crate) fn listpack_encode(items: &[Vec<u8>]) -> Vec<u8> {
    let mut buf = Vec::new();
    let total_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]);
    let cached_count = u16::try_from(items.len()).unwrap_or(u16::MAX);
    buf.extend_from_slice(&cached_count.to_le_bytes());

    for item in items {
        let entry_start = buf.len();
        buf.extend_from_slice(&lp_encode_entry(item));
        let encoding_data_size = buf.len() - entry_start;
        lp_write_backlen(&mut buf, encoding_data_size);
    }
    buf.push(0xFF);

    let total_bytes = buf.len() as u32;
    buf[total_pos..total_pos + 4].copy_from_slice(&total_bytes.to_le_bytes());
    buf
}

pub(crate) fn zipmap_decode(zm: &[u8]) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
    if zm.is_empty() {
        return None;
    }
    let mut p = 1usize;
    let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    loop {
        let b = *zm.get(p)?;
        if b == 0xFF {
            break;
        }
        let (klen, ksize) = zipmap_decode_len(zm, p)?;
        p += ksize;
        let kend = p.checked_add(klen)?;
        let key = zm.get(p..kend)?.to_vec();
        p = kend;
        let (vlen, vsize) = zipmap_decode_len(zm, p)?;
        p += vsize;
        let free = *zm.get(p)? as usize;
        p += 1;
        let vend = p.checked_add(vlen)?;
        let value = zm.get(p..vend)?.to_vec();
        p = vend + free;
        pairs.push((key, value));
    }
    Some(pairs)
}

fn zipmap_decode_len(zm: &[u8], p: usize) -> Option<(usize, usize)> {
    let b = *zm.get(p)?;
    if b < 254 {
        Some((b as usize, 1))
    } else {
        let raw = zm.get(p + 1..p + 5)?;
        let len = u32::from_le_bytes(raw.try_into().ok()?) as usize;
        if len < 254 {
            return None;
        }
        Some((len, 5))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn two_item_ziplist() -> Vec<u8> {
        vec![
            17, 0, 0, 0, // zlbytes
            13, 0, 0, 0, // zltail
            2, 0, // zllen
            0, 1, b'a', // first entry
            3, 1, b'b', // second entry
            0xff, // zlend
        ]
    }

    fn zipmap_len(len: usize) -> Vec<u8> {
        if len < 254 {
            vec![len as u8]
        } else {
            let mut out = vec![254];
            out.extend_from_slice(&(len as u32).to_le_bytes());
            out
        }
    }

    #[test]
    fn listpack_encode_decode_string_length_boundaries() {
        let items = vec![
            Vec::new(),
            vec![b'a'; 63],
            vec![b'b'; 64],
            vec![b'c'; 16_384],
        ];

        let encoded = listpack_encode(&items);

        assert_eq!(listpack_decode(&encoded).unwrap(), items);
    }

    #[test]
    fn listpack_decode_validates_metadata_and_backlen() {
        let encoded = listpack_encode(&[b"a".to_vec()]);
        assert_eq!(listpack_decode(&encoded).unwrap(), vec![b"a".to_vec()]);

        let mut bad_backlen = encoded.clone();
        bad_backlen[8] = 1;
        assert!(listpack_decode(&bad_backlen).is_none());

        let mut bad_count = encoded.clone();
        bad_count[4..6].copy_from_slice(&2u16.to_le_bytes());
        assert!(listpack_decode(&bad_count).is_none());

        let mut trailing_eof = encoded;
        let total = trailing_eof.len() + 1;
        trailing_eof[0..4].copy_from_slice(&(total as u32).to_le_bytes());
        trailing_eof.push(0xff);
        assert!(listpack_decode(&trailing_eof).is_none());
    }

    #[test]
    fn listpack_32bit_negative_ints_round_trip() {
        let vals = [
            -2147483648_i64,
            -1018986169,
            -249629816,
            -375994007,
            -8388609, //  24bit
            -1,
            8388608, //  24bit
            2147483647,
        ];
        let items: Vec<Vec<u8>> = vals.iter().map(|v| v.to_string().into_bytes()).collect();
        let encoded = listpack_encode(&items);
        assert_eq!(listpack_decode(&encoded).unwrap(), items);
    }

    #[test]
    fn listpack_decode_13bit_negative_int_then_string() {
        let lp_bytes = vec![
            0x0f, 0x00, 0x00, 0x00, // total bytes
            0x02, 0x00, // 2 elements
            0xdf, 0x52, 0x02, // -174 + backlen 2
            0x83, b'h', b'e', b'a', 0x04, // "hea" + backlen 4
            0xff, // EOF
        ];
        assert_eq!(
            listpack_decode(&lp_bytes).unwrap(),
            vec![b"-174".to_vec(), b"hea".to_vec()]
        );

        let roundtrip = listpack_encode(&[b"-4096".to_vec(), b"4095".to_vec(), b"x".to_vec()]);
        assert_eq!(
            listpack_decode(&roundtrip).unwrap(),
            vec![b"-4096".to_vec(), b"4095".to_vec(), b"x".to_vec()]
        );
    }

    #[test]
    fn listpack_decode_caught_corrupt_payload() {
        const HEX: &str = "400000000200c14902b43232313033343030303030333234323234323334343333303431303131333234303134333332313334333431303031333134343135ff";
        let lp: Vec<u8> = HEX
            .as_bytes()
            .chunks(2)
            .map(|c| u8::from_str_radix(std::str::from_utf8(c).unwrap(), 16).unwrap())
            .collect();
        let items = listpack_decode(&lp).expect("payload from running server must decode");
        assert_eq!(items[0], b"329");
        assert_eq!(items[1].len(), 52);
    }

    #[test]
    fn ziplist_decode_validates_metadata() {
        let valid = two_item_ziplist();
        assert_eq!(
            ziplist_decode(&valid).unwrap(),
            vec![b"a".to_vec(), b"b".to_vec()]
        );

        let mut bad_prevlen = valid.clone();
        bad_prevlen[13] = 2;
        assert!(ziplist_decode(&bad_prevlen).is_none());

        let mut bad_tail = valid.clone();
        bad_tail[4..8].copy_from_slice(&10u32.to_le_bytes());
        assert!(ziplist_decode(&bad_tail).is_none());

        let mut bad_count = valid;
        bad_count[8..10].copy_from_slice(&3u16.to_le_bytes());
        assert!(ziplist_decode(&bad_count).is_none());
    }

    #[test]
    fn intset_encode_decode_selects_i16_i32_and_i64_widths() {
        let i16_blob = intset_encode(&[-1, 0, i16::MAX as i64]);
        assert_eq!(u32::from_le_bytes(i16_blob[0..4].try_into().unwrap()), 2);
        assert_eq!(
            intset_decode(&i16_blob).unwrap(),
            vec![b"-1".to_vec(), b"0".to_vec(), b"32767".to_vec()]
        );

        let i32_blob = intset_encode(&[i16::MAX as i64 + 1, -1]);
        assert_eq!(u32::from_le_bytes(i32_blob[0..4].try_into().unwrap()), 4);
        assert_eq!(
            intset_decode(&i32_blob).unwrap(),
            vec![b"32768".to_vec(), b"-1".to_vec()]
        );

        let i64_blob = intset_encode(&[i32::MAX as i64 + 1]);
        assert_eq!(u32::from_le_bytes(i64_blob[0..4].try_into().unwrap()), 8);
        assert_eq!(
            intset_decode(&i64_blob).unwrap(),
            vec![b"2147483648".to_vec()]
        );
        assert!(intset_decode(&[3, 0, 0, 0, 1, 0, 0, 0]).is_none());
        assert!(intset_decode(&[2, 0, 0, 0, 2, 0, 0, 0, 1, 0]).is_none());
    }

    #[test]
    fn lzf_decompress_backrefs_extended_len_and_rejects_bad_streams() {
        assert_eq!(
            lzf_decompress(&[0x02, b'a', b'b', b'c', 0x20, 0x02], 6).unwrap(),
            b"abcabc"
        );
        assert_eq!(
            lzf_decompress(&[0x00, b'a', 0xe0, 0x01, 0x00], 11).unwrap(),
            vec![b'a'; 11]
        );
        assert!(lzf_decompress(&[0x20, 0x00], 3).is_none());
        assert!(lzf_decompress(&[0x02, b'a', b'b', b'c'], 2).is_none());
        assert!(lzf_decompress(&[0x02, b'a'], 3).is_none());
    }

    #[test]
    fn zipmap_decode_long_lengths_free_bytes_and_overlong_rejection() {
        let key = vec![b'k'; 254];
        let mut zipmap = vec![1];
        zipmap.extend_from_slice(&zipmap_len(key.len()));
        zipmap.extend_from_slice(&key);
        zipmap.extend_from_slice(&zipmap_len(1));
        zipmap.push(2);
        zipmap.push(b'v');
        zipmap.extend_from_slice(&[0, 0]);
        zipmap.push(0xff);

        let pairs = zipmap_decode(&zipmap).unwrap();

        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].0, key);
        assert_eq!(pairs[0].1, b"v");
        assert!(zipmap_decode(&[1, 254, 1, 0, 0, 0]).is_none());
        assert!(zipmap_decode(&[]).is_none());
    }
}
