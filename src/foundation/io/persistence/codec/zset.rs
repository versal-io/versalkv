use super::CodecLoad;
use crate::foundation::io::persistence::codec::decoder;
use crate::foundation::io::persistence::codec::decoder_compact;
use crate::foundation::io::persistence::codec::types::{
    load_len, RDB_TYPE_ZSET, RDB_TYPE_ZSET_2, RDB_TYPE_ZSET_LISTPACK, RDB_TYPE_ZSET_ZIPLIST,
};
use crate::foundation::io::persistence::codec::RdbWriter;
use crate::foundation::io::store::structure::zset::{ZSetLp, ZSetSl};
use crate::foundation::io::store::Robj;
use crate::foundation::memory::value::ZSetOps;

pub struct ZSetCodec;

impl CodecLoad for ZSetCodec {
    fn load_dispatch(buf: &[u8], pos: &mut usize, type_byte: u8) -> Option<Robj> {
        if type_byte == RDB_TYPE_ZSET_LISTPACK {
            let data = decoder::load_string(buf, pos)?;
            validate_zset_listpack(&data)?;
            return Some(Robj::ZSetListpack(ZSetLp::from_raw_bytes(data)?));
        }
        let zset = ZSetCodec::load(buf, pos, type_byte)?;
        Some(Robj::ZSetSkiplist(zset))
    }
}

fn validate_zset_listpack(data: &[u8]) -> Option<()> {
    let items = decoder_compact::listpack_decode(data)?;
    load_zset_compact_items(items).map(|_| ())
}

fn load_zset_compact_items(items: Vec<Vec<u8>>) -> Option<ZSetSl> {
    if !items.len().is_multiple_of(2) {
        return None;
    }
    let mut zset = ZSetSl::new();
    let mut it = items.into_iter();
    while let (Some(member), Some(score_raw)) = (it.next(), it.next()) {
        let score = crate::foundation::io::persistence::codec::types::parse_double(&score_raw)?;
        if score.is_nan() {
            return None;
        }
        if !ZSetOps::add(&mut zset, &member, score).ok()? {
            return None;
        }
    }
    Some(zset)
}

impl ZSetCodec {
    pub fn save<W: RdbWriter>(val: &ZSetSl, w: &mut W) {
        w.write_len(val.len() as u64);
        for (member, &score) in val.dict() {
            w.write_string(member);
            w.write_binary_double(score);
        }
    }

    pub fn load(buf: &[u8], pos: &mut usize, type_byte: u8) -> Option<ZSetSl> {
        match type_byte {
            RDB_TYPE_ZSET | RDB_TYPE_ZSET_2 => {
                let (len, encoded) = load_len(buf, pos)?;
                if encoded {
                    return None;
                }
                let mut zset = ZSetSl::new();
                for _ in 0..len {
                    let member = decoder::load_string(buf, pos)?;
                    let score = if type_byte == RDB_TYPE_ZSET_2 {
                        decoder::load_binary_double(buf, pos)?
                    } else {
                        decoder::load_double(buf, pos)?
                    };
                    if score.is_nan() {
                        return None;
                    }
                    if !ZSetOps::add(&mut zset, &member, score).ok()? {
                        return None;
                    }
                }
                Some(zset)
            }
            RDB_TYPE_ZSET_ZIPLIST | RDB_TYPE_ZSET_LISTPACK => {
                let data = decoder::load_string(buf, pos)?;
                let items = if type_byte == RDB_TYPE_ZSET_LISTPACK {
                    decoder_compact::listpack_decode(&data)?
                } else {
                    decoder_compact::ziplist_decode(&data)?
                };
                load_zset_compact_items(items)
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::io::persistence::codec::types::save_len;

    fn enc_string(s: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        save_len(&mut out, s.len() as u64);
        out.extend_from_slice(s);
        out
    }

    #[test]
    fn load_zset_rejects_encoded_length() {
        let buf = vec![0xC0u8];
        let mut pos = 0;
        assert!(ZSetCodec::load(&buf, &mut pos, RDB_TYPE_ZSET_2).is_none());
    }

    #[test]
    fn load_zset_ziplist_and_listpack_reject_odd_item_count() {
        let items = vec![b"m".to_vec()];
        let lp = decoder_compact::listpack_encode(&items);
        let mut buf = enc_string(&lp);
        let mut pos = 0;
        assert!(ZSetCodec::load(&buf, &mut pos, RDB_TYPE_ZSET_LISTPACK).is_none());

        let zl_items = vec![b"m".to_vec()];
        let zl = decoder_compact::listpack_encode(&zl_items);
        buf = enc_string(&zl);
        pos = 0;
        assert!(ZSetCodec::load(&buf, &mut pos, RDB_TYPE_ZSET_ZIPLIST).is_none());
    }

    #[test]
    fn load_zset_rejects_duplicate_members() {
        let mut buf = Vec::new();
        save_len(&mut buf, 2);
        buf.extend_from_slice(&enc_string(b"a"));
        buf.extend_from_slice(&1.0f64.to_le_bytes());
        buf.extend_from_slice(&enc_string(b"a"));
        buf.extend_from_slice(&2.0f64.to_le_bytes());
        let mut pos = 0;
        assert!(ZSetCodec::load(&buf, &mut pos, RDB_TYPE_ZSET_2).is_none());
    }

    #[test]
    fn load_zset_compact_rejects_duplicate_members() {
        let items = vec![b"a".to_vec(), b"1".to_vec(), b"a".to_vec(), b"2".to_vec()];
        let lp = decoder_compact::listpack_encode(&items);
        let mut buf = enc_string(&lp);
        let mut pos = 0;
        assert!(ZSetCodec::load(&buf, &mut pos, RDB_TYPE_ZSET_LISTPACK).is_none());

        buf = enc_string(&lp);
        pos = 0;
        assert!(ZSetCodec::load(&buf, &mut pos, RDB_TYPE_ZSET_ZIPLIST).is_none());
    }

    #[test]
    fn load_zset_listpack_dispatch_rejects_nan_score() {
        let payload = b"\x11\x19\x19\x00\x00\x00\x04\x00\x82\x6D\x31\x03\x83\x6E\x61\x6E\x04\x82\x6D\x32\x03\x83\x32\x2E\x35\x04\xFF\x50\x00\xC5\x5C\xC6\x0C\x7D\xFF\xB5\x52";
        let mut pos = 1;
        assert!(ZSetCodec::load_dispatch(payload, &mut pos, RDB_TYPE_ZSET_LISTPACK).is_none());
    }

    #[test]
    fn load_zset_unknown_type_byte_returns_none() {
        let mut pos = 0;
        assert!(ZSetCodec::load(&[], &mut pos, 99).is_none());
    }
}
