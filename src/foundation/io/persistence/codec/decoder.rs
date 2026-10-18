use crate::foundation::io::persistence::codec::decoder_compact::lzf_decompress;
use crate::foundation::io::persistence::codec::types::{
    load_len, parse_double, RDB_ENC_INT16, RDB_ENC_INT32, RDB_ENC_INT8, RDB_ENC_LZF,
};

pub fn load_binary_double(buf: &[u8], pos: &mut usize) -> Option<f64> {
    let bytes: [u8; 8] = buf.get(*pos..*pos + 8)?.try_into().ok()?;
    *pos += 8;
    Some(f64::from_le_bytes(bytes))
}

pub fn load_double(buf: &[u8], pos: &mut usize) -> Option<f64> {
    let len = *buf.get(*pos)?;
    *pos += 1;
    match len {
        255 => Some(f64::NEG_INFINITY),
        254 => Some(f64::INFINITY),
        253 => Some(f64::NAN),
        n => {
            let end = pos.checked_add(n as usize)?;
            let raw = buf.get(*pos..end)?;
            *pos = end;
            parse_double(raw)
        }
    }
}

pub fn load_string(buf: &[u8], pos: &mut usize) -> Option<Vec<u8>> {
    let (len, encoded) = load_len(buf, pos)?;
    if encoded {
        match len as u8 {
            RDB_ENC_INT8 => {
                let v = *buf.get(*pos)? as i8;
                *pos += 1;
                Some(v.to_string().into_bytes())
            }
            RDB_ENC_INT16 => {
                let bytes: [u8; 2] = buf.get(*pos..*pos + 2)?.try_into().ok()?;
                *pos += 2;
                Some(i16::from_le_bytes(bytes).to_string().into_bytes())
            }
            RDB_ENC_INT32 => {
                let bytes: [u8; 4] = buf.get(*pos..*pos + 4)?.try_into().ok()?;
                *pos += 4;
                Some(i32::from_le_bytes(bytes).to_string().into_bytes())
            }
            RDB_ENC_LZF => {
                let (clen, enc1) = load_len(buf, pos)?;
                if enc1 {
                    return None;
                }
                let (ulen, enc2) = load_len(buf, pos)?;
                if enc2 {
                    return None;
                }
                let end = pos.checked_add(clen as usize)?;
                let compressed = buf.get(*pos..end)?;
                let out = lzf_decompress(compressed, ulen as usize)?;
                *pos = end;
                Some(out)
            }
            _ => None,
        }
    } else {
        let end = pos.checked_add(len as usize)?;
        let data = buf.get(*pos..end)?.to_vec();
        *pos = end;
        Some(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encoded_string(body: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        crate::foundation::io::persistence::codec::types::save_len(&mut out, body.len() as u64);
        out.extend_from_slice(body);
        out
    }

    #[test]
    fn load_double_handles_legacy_specials_and_ascii_payloads() {
        let mut pos = 0;
        assert_eq!(load_double(&[255], &mut pos), Some(f64::NEG_INFINITY));
        assert_eq!(pos, 1);

        let mut pos = 0;
        assert_eq!(load_double(&[254], &mut pos), Some(f64::INFINITY));
        let mut pos = 0;
        assert!(load_double(&[253], &mut pos).unwrap().is_nan());

        let mut pos = 0;
        assert_eq!(load_double(b"\x041.25", &mut pos), Some(1.25));
        assert_eq!(pos, 5);

        let mut pos = 0;
        assert_eq!(load_double(b"\x04bad!", &mut pos), None);
        let mut pos = 0;
        assert_eq!(load_double(b"\x041.", &mut pos), None);
    }

    #[test]
    fn load_binary_double_requires_eight_bytes_and_advances_position() {
        let mut pos = 0;
        let bytes = 1.5f64.to_le_bytes();
        assert_eq!(load_binary_double(&bytes, &mut pos), Some(1.5));
        assert_eq!(pos, 8);

        let mut pos = 0;
        assert_eq!(load_binary_double(&bytes[..7], &mut pos), None);
        assert_eq!(pos, 0);
    }

    #[test]
    fn load_string_decodes_raw_integer_and_lzf_payloads() {
        let mut pos = 0;
        assert_eq!(
            load_string(&encoded_string(b"raw"), &mut pos),
            Some(b"raw".to_vec())
        );

        let mut int8 = vec![
            (crate::foundation::io::persistence::codec::types::RDB_ENCVAL << 6) | RDB_ENC_INT8,
            0xff,
        ];
        let mut pos = 0;
        assert_eq!(load_string(&int8, &mut pos), Some(b"-1".to_vec()));

        let mut int16 = vec![
            (crate::foundation::io::persistence::codec::types::RDB_ENCVAL << 6) | RDB_ENC_INT16,
        ];
        int16.extend_from_slice(&(-1234i16).to_le_bytes());
        let mut pos = 0;
        assert_eq!(load_string(&int16, &mut pos), Some(b"-1234".to_vec()));

        let mut int32 = vec![
            (crate::foundation::io::persistence::codec::types::RDB_ENCVAL << 6) | RDB_ENC_INT32,
        ];
        int32.extend_from_slice(&(123456i32).to_le_bytes());
        let mut pos = 0;
        assert_eq!(load_string(&int32, &mut pos), Some(b"123456".to_vec()));

        let mut lzf =
            vec![(crate::foundation::io::persistence::codec::types::RDB_ENCVAL << 6) | RDB_ENC_LZF];
        crate::foundation::io::persistence::codec::types::save_len(&mut lzf, 5);
        crate::foundation::io::persistence::codec::types::save_len(&mut lzf, 4);
        lzf.extend_from_slice(&[0x03, b'a', b'b', b'c', b'd']);
        let mut pos = 0;
        assert_eq!(load_string(&lzf, &mut pos), Some(b"abcd".to_vec()));

        int8[0] = (crate::foundation::io::persistence::codec::types::RDB_ENCVAL << 6) | 63;
        let mut pos = 0;
        assert_eq!(load_string(&int8, &mut pos), None);
    }
}
