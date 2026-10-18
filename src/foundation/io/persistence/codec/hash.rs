use super::CodecLoad;
use crate::foundation::io::persistence::codec::decoder;
use crate::foundation::io::persistence::codec::decoder_compact;
use crate::foundation::io::persistence::codec::types::{
    load_len, RDB_TYPE_HASH, RDB_TYPE_HASH_LISTPACK, RDB_TYPE_HASH_ZIPLIST, RDB_TYPE_HASH_ZIPMAP,
};
use crate::foundation::io::persistence::codec::RdbWriter;
use crate::foundation::io::store::structure::hash::{HashHt, HashLp};
use crate::foundation::io::store::Robj;
use bytes::Bytes;
use indexmap::IndexMap;
use std::collections::HashMap;

pub struct HashCodec;

impl CodecLoad for HashCodec {
    fn load_dispatch(buf: &[u8], pos: &mut usize, type_byte: u8) -> Option<Robj> {
        if type_byte == RDB_TYPE_HASH_LISTPACK {
            let data = decoder::load_string(buf, pos)?;
            validate_hash_listpack(&data)?;
            return Some(Robj::HashListpack(HashLp::from_raw_bytes(data)?));
        }
        let hash = HashCodec::load(buf, pos, type_byte)?;
        Some(Robj::HashHashtable(hash))
    }
}

fn validate_hash_listpack(data: &[u8]) -> Option<()> {
    let items = decoder_compact::listpack_decode(data)?;
    if !items.len().is_multiple_of(2) {
        return None;
    }
    let mut fields = IndexMap::with_capacity(items.len() / 2);
    let mut it = items.into_iter();
    while let (Some(field), Some(_value)) = (it.next(), it.next()) {
        if fields.insert(field, ()).is_some() {
            return None;
        }
    }
    Some(())
}

impl HashCodec {
    pub fn save<W: RdbWriter>(val: &HashHt, w: &mut W) {
        w.write_len(val.len() as u64);
        for (field, value) in val.data.iter() {
            w.write_string(field);
            w.write_string(value);
        }
    }

    pub fn save_with_field_expires<W: RdbWriter>(
        val: &HashHt,
        field_expires: &HashMap<Vec<u8>, u64>,
        w: &mut W,
    ) {
        w.write_len(val.len() as u64);
        for (field, value) in val.data.iter() {
            w.write_string(field);
            w.write_string(value);
            let expiry = field_expires
                .get(field.as_ref())
                .map(|&t| t as i64)
                .unwrap_or(-1);
            w.write_millisecond_time(expiry);
        }
    }

    pub fn load(buf: &[u8], pos: &mut usize, type_byte: u8) -> Option<HashHt> {
        match type_byte {
            RDB_TYPE_HASH => {
                let (len, encoded) = load_len(buf, pos)?;
                if encoded {
                    return None;
                }
                let mut map = IndexMap::with_capacity(len as usize);
                for _ in 0..len {
                    let field = Bytes::from(decoder::load_string(buf, pos)?);
                    let value = Bytes::from(decoder::load_string(buf, pos)?);
                    if map.insert(field, value).is_some() {
                        return None;
                    }
                }
                Some(HashHt {
                    data: Box::new(map),
                })
            }
            tb @ (RDB_TYPE_HASH_ZIPLIST | RDB_TYPE_HASH_LISTPACK) => {
                let data = decoder::load_string(buf, pos)?;
                let items = if tb == RDB_TYPE_HASH_LISTPACK {
                    decoder_compact::listpack_decode(&data)?
                } else {
                    decoder_compact::ziplist_decode(&data)?
                };
                if !items.len().is_multiple_of(2) {
                    return None;
                }
                let mut map = IndexMap::with_capacity(items.len() / 2);
                let mut it = items.into_iter();
                while let (Some(field), Some(value)) = (it.next(), it.next()) {
                    if map.insert(Bytes::from(field), Bytes::from(value)).is_some() {
                        return None;
                    }
                }
                Some(HashHt {
                    data: Box::new(map),
                })
            }
            RDB_TYPE_HASH_ZIPMAP => {
                let data = decoder::load_string(buf, pos)?;
                let pairs = decoder_compact::zipmap_decode(&data)?;
                Some(HashHt {
                    data: Box::new(
                        pairs
                            .into_iter()
                            .map(|(k, v)| (Bytes::from(k), Bytes::from(v)))
                            .collect(),
                    ),
                })
            }
            _ => None,
        }
    }

    pub(super) fn load_with_field_expires_detailed(
        buf: &[u8],
        pos: &mut usize,
        now_ms: Option<u64>,
    ) -> Option<(HashHt, HashMap<Vec<u8>, u64>, Vec<Bytes>)> {
        let (len, encoded) = load_len(buf, pos)?;
        if encoded {
            return None;
        }
        let mut map = IndexMap::with_capacity(len as usize);
        let mut expires = HashMap::new();
        let mut skipped_expired_fields = Vec::new();
        for _ in 0..len {
            let field = decoder::load_string(buf, pos)?;
            let value = decoder::load_string(buf, pos)?;
            let raw: [u8; 8] = buf.get(*pos..*pos + 8)?.try_into().ok()?;
            *pos += 8;
            let expiry = i64::from_le_bytes(raw);
            if expiry < -1 {
                return None;
            }
            if matches!(now_ms, Some(now) if expiry != -1 && (expiry as u64) < now) {
                skipped_expired_fields.push(Bytes::from(field));
                continue;
            }
            if expiry != -1 {
                expires.insert(field.clone(), expiry as u64);
            }
            if map.insert(Bytes::from(field), Bytes::from(value)).is_some() {
                return None;
            }
        }
        Some((
            HashHt {
                data: Box::new(map),
            },
            expires,
            skipped_expired_fields,
        ))
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
    fn load_hash_rejects_encoded_length_and_accepts_plain() {
        // encoded len (0xC0 = RDB_ENCVAL prefix) → rejected
        let buf = vec![0xC0u8];
        let mut pos = 0;
        assert!(HashCodec::load(&buf, &mut pos, RDB_TYPE_HASH).is_none());

        // plain len=0 → empty map
        let buf = vec![0x00u8];
        pos = 0;
        let map = HashCodec::load(&buf, &mut pos, RDB_TYPE_HASH).unwrap();
        assert!(map.is_empty());
    }

    #[test]
    fn hash_2_round_trips_field_expiry_metadata() {
        let mut hash = HashHt::new();
        hash.insert(Bytes::from_static(b"a"), Bytes::from_static(b"1"));
        hash.insert(Bytes::from_static(b"b"), Bytes::from_static(b"2"));
        let mut expires = HashMap::new();
        expires.insert(b"a".to_vec(), 12345);

        let mut buf = Vec::new();
        HashCodec::save_with_field_expires(&hash, &expires, &mut buf);
        let mut pos = 0;
        let (loaded, loaded_expires, skipped) =
            HashCodec::load_with_field_expires_detailed(&buf, &mut pos, None).unwrap();

        assert_eq!(
            loaded.get(&Bytes::from_static(b"a")).unwrap().as_ref(),
            b"1"
        );
        assert_eq!(
            loaded.get(&Bytes::from_static(b"b")).unwrap().as_ref(),
            b"2"
        );
        assert_eq!(loaded_expires.get(b"a" as &[u8]), Some(&12345));
        assert_eq!(loaded_expires.get(b"b" as &[u8]), None);
        assert!(skipped.is_empty());
        assert_eq!(pos, buf.len());
    }

    #[test]
    fn hash_2_loader_skips_expired_fields() {
        let mut buf = Vec::new();
        save_len(&mut buf, 2);
        buf.extend_from_slice(&enc_string(b"expired"));
        buf.extend_from_slice(&enc_string(b"old"));
        buf.extend_from_slice(&10i64.to_le_bytes());
        buf.extend_from_slice(&enc_string(b"live"));
        buf.extend_from_slice(&enc_string(b"new"));
        buf.extend_from_slice(&100i64.to_le_bytes());

        let mut pos = 0;
        let (loaded, expires, _) =
            HashCodec::load_with_field_expires_detailed(&buf, &mut pos, Some(50)).unwrap();

        assert!(!loaded.contains_key(&Bytes::from_static(b"expired")));
        assert_eq!(
            loaded.get(&Bytes::from_static(b"live")).unwrap().as_ref(),
            b"new"
        );
        assert_eq!(expires.get(b"live" as &[u8]), Some(&100));
    }

    #[test]
    fn hash_2_loader_reports_skipped_expired_fields() {
        let mut buf = Vec::new();
        save_len(&mut buf, 2);
        buf.extend_from_slice(&enc_string(b"expired"));
        buf.extend_from_slice(&enc_string(b"old"));
        buf.extend_from_slice(&10i64.to_le_bytes());
        buf.extend_from_slice(&enc_string(b"live"));
        buf.extend_from_slice(&enc_string(b"new"));
        buf.extend_from_slice(&100i64.to_le_bytes());

        let mut pos = 0;
        let (loaded, expires, skipped) =
            HashCodec::load_with_field_expires_detailed(&buf, &mut pos, Some(50)).unwrap();

        assert_eq!(skipped, vec![Bytes::from_static(b"expired")]);
        assert!(!loaded.contains_key(&Bytes::from_static(b"expired")));
        assert_eq!(
            loaded.get(&Bytes::from_static(b"live")).unwrap().as_ref(),
            b"new"
        );
        assert_eq!(expires.get(b"live" as &[u8]), Some(&100));
        assert_eq!(pos, buf.len());
    }

    #[test]
    fn hash_2_loader_retains_field_expiring_at_cutoff() {
        let mut buf = Vec::new();
        save_len(&mut buf, 1);
        buf.extend_from_slice(&enc_string(b"boundary"));
        buf.extend_from_slice(&enc_string(b"value"));
        buf.extend_from_slice(&50i64.to_le_bytes());

        let mut pos = 0;
        let (loaded, expires, skipped) =
            HashCodec::load_with_field_expires_detailed(&buf, &mut pos, Some(50)).unwrap();

        assert!(skipped.is_empty());
        assert_eq!(
            loaded
                .get(&Bytes::from_static(b"boundary"))
                .unwrap()
                .as_ref(),
            b"value"
        );
        assert_eq!(expires.get(b"boundary" as &[u8]), Some(&50));
        assert_eq!(pos, buf.len());
    }

    #[test]
    fn load_hash_ziplist_and_listpack_reject_odd_item_count() {
        let items = vec![b"k".to_vec()];
        let lp =
            crate::foundation::io::persistence::codec::decoder_compact::listpack_encode(&items);
        let mut buf = enc_string(&lp);
        let mut pos = 0;
        assert!(HashCodec::load(&buf, &mut pos, RDB_TYPE_HASH_LISTPACK).is_none());

        let zl_items = vec![b"k".to_vec()];
        let zl =
            crate::foundation::io::persistence::codec::decoder_compact::listpack_encode(&zl_items);
        buf = enc_string(&zl);
        pos = 0;
        assert!(HashCodec::load(&buf, &mut pos, RDB_TYPE_HASH_ZIPLIST).is_none());
    }

    #[test]
    fn load_hash_rejects_duplicate_fields() {
        let mut buf = Vec::new();
        save_len(&mut buf, 2);
        buf.extend_from_slice(&enc_string(b"a"));
        buf.extend_from_slice(&enc_string(b"b"));
        buf.extend_from_slice(&enc_string(b"a"));
        buf.extend_from_slice(&enc_string(b"d"));
        let mut pos = 0;
        assert!(HashCodec::load(&buf, &mut pos, RDB_TYPE_HASH).is_none());
    }

    #[test]
    fn load_hash_compact_rejects_duplicate_fields() {
        let items = vec![b"a".to_vec(), b"b".to_vec(), b"a".to_vec(), b"d".to_vec()];
        let lp =
            crate::foundation::io::persistence::codec::decoder_compact::listpack_encode(&items);
        let mut buf = enc_string(&lp);
        let mut pos = 0;
        assert!(HashCodec::load(&buf, &mut pos, RDB_TYPE_HASH_LISTPACK).is_none());

        buf = enc_string(&lp);
        pos = 0;
        assert!(HashCodec::load(&buf, &mut pos, RDB_TYPE_HASH_ZIPLIST).is_none());
    }

    #[test]
    fn load_hash_listpack_dispatch_rejects_duplicate_fields() {
        let mut payload = b"\x10\x17\x17\x00\x00\x00\x04\x00\x82\x61\x00\x03\x82\x62\x00\x03\x82\x61\x00\x03\x82\x64\x00\x03\xff\x0a\x00\xc0\xcf\xa6\x87\xe5\xa7\xc5\xbe".to_vec();
        let mut pos = 1;
        assert!(HashCodec::load_dispatch(&payload, &mut pos, RDB_TYPE_HASH_LISTPACK).is_none());

        payload[17] = b'c';
        pos = 1;
        assert!(HashCodec::load_dispatch(&payload, &mut pos, RDB_TYPE_HASH_LISTPACK).is_some());
    }

    #[test]
    fn load_hash_zipmap_returns_pairs() {
        let zm = vec![0x01u8, 0x01, b'k', 0x01, 0x00, b'v', 0xFF];
        let buf = enc_string(&zm);
        let mut pos = 0;
        let map = HashCodec::load(&buf, &mut pos, RDB_TYPE_HASH_ZIPMAP).unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(
            map.get(&Bytes::from_static(b"k")).unwrap(),
            &Bytes::from_static(b"v")
        );
    }

    #[test]
    fn load_hash_unknown_type_byte_returns_none() {
        let mut pos = 0;
        assert!(HashCodec::load(&[], &mut pos, 99).is_none());
    }
}
