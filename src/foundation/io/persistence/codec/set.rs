use super::CodecLoad;
use crate::foundation::io::persistence::codec::decoder;
use crate::foundation::io::persistence::codec::decoder_compact;
use crate::foundation::io::persistence::codec::types::{
    load_len, RDB_TYPE_SET, RDB_TYPE_SET_INTSET, RDB_TYPE_SET_LISTPACK,
};
use crate::foundation::io::persistence::codec::RdbWriter;
use crate::foundation::io::store::structure::dict::Dict;
use crate::foundation::io::store::structure::set::{IntSet, SetEncoding, SetLp, ValkeySet};
use crate::foundation::io::store::structure::string::encoding::int_encodable;
use crate::foundation::io::store::Robj;
use bytes::Bytes;

pub struct SetCodec;

impl CodecLoad for SetCodec {
    fn load_dispatch(buf: &[u8], pos: &mut usize, type_byte: u8) -> Option<Robj> {
        if type_byte == RDB_TYPE_SET_LISTPACK {
            let data = decoder::load_string(buf, pos)?;
            validate_set_listpack(&data)?;
            return Some(Robj::SetListpack(SetLp::from_raw_bytes(data)?));
        }
        match type_byte {
            RDB_TYPE_SET_INTSET => {
                let set = SetCodec::load(buf, pos, type_byte)?;
                Some(Robj::SetHashtable(ValkeySet {
                    data: Box::new(set),
                    encoding: SetEncoding::Intset,
                }))
            }
            _ => {
                let set = SetCodec::load(buf, pos, type_byte)?;
                let encoding = match type_byte {
                    RDB_TYPE_SET_LISTPACK => SetEncoding::Listpack,
                    _ => {
                        if set.len() <= 512 && set.keys().all(|v| int_encodable(v).is_some()) {
                            SetEncoding::Intset
                        } else if set.len() <= 128 && set.keys().all(|v| v.len() <= 64) {
                            SetEncoding::Listpack
                        } else {
                            SetEncoding::Hashtable
                        }
                    }
                };
                Some(Robj::SetHashtable(ValkeySet {
                    data: Box::new(set),
                    encoding,
                }))
            }
        }
    }
}

fn validate_set_listpack(data: &[u8]) -> Option<()> {
    let items = decoder_compact::listpack_decode(data)?;
    let mut set = Dict::new();
    for member in items {
        if set.insert(Bytes::from(member), ()).is_some() {
            return None;
        }
    }
    Some(())
}

impl SetCodec {
    pub fn save<W: RdbWriter>(val: &ValkeySet, w: &mut W) {
        match val.encoding {
            SetEncoding::Intset => {
                let mut ints: Vec<i64> = Vec::with_capacity(val.data.len());
                for member in val.data.keys() {
                    if let Some(n) = int_encodable(member) {
                        ints.push(n);
                    }
                }
                let blob = decoder_compact::intset_encode(&ints);
                w.write_string(&blob);
            }
            SetEncoding::Listpack => {
                let items: Vec<Vec<u8>> = val.data.keys().map(|b| b.to_vec()).collect();
                let blob = decoder_compact::listpack_encode(&items);
                w.write_string(&blob);
            }
            SetEncoding::Hashtable => {
                w.write_len(val.data.len() as u64);
                for member in val.data.keys() {
                    w.write_string(member);
                }
            }
        }
    }

    pub fn save_intset<W: RdbWriter>(val: &IntSet, w: &mut W) {
        let ints: Vec<i64> = val.iter().collect();
        let blob = decoder_compact::intset_encode(&ints);
        w.write_string(&blob);
    }

    pub fn load(buf: &[u8], pos: &mut usize, type_byte: u8) -> Option<Dict<Bytes, ()>> {
        match type_byte {
            RDB_TYPE_SET => {
                let (len, encoded) = load_len(buf, pos)?;
                if encoded {
                    return None;
                }
                let mut set = Dict::new();
                for _ in 0..len {
                    if set
                        .insert(Bytes::from(decoder::load_string(buf, pos)?), ())
                        .is_some()
                    {
                        return None;
                    }
                }
                Some(set)
            }
            RDB_TYPE_SET_INTSET => {
                let data = decoder::load_string(buf, pos)?;
                let items = decoder_compact::intset_decode(&data)?;
                let mut set = Dict::new();
                for v in items {
                    if set.insert(Bytes::from(v), ()).is_some() {
                        return None;
                    }
                }
                Some(set)
            }
            RDB_TYPE_SET_LISTPACK => {
                let data = decoder::load_string(buf, pos)?;
                let items = decoder_compact::listpack_decode(&data)?;
                let mut set = Dict::new();
                for v in items {
                    if set.insert(Bytes::from(v), ()).is_some() {
                        return None;
                    }
                }
                Some(set)
            }
            _ => None,
        }
    }

    #[cfg(test)]
    pub fn load_intset(buf: &[u8], pos: &mut usize) -> Option<IntSet> {
        let data = decoder::load_string(buf, pos)?;
        let ints = decoder_compact::intset_decode_ints(&data)?;
        Some(IntSet::from_slice(&ints))
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
    fn load_set_rejects_encoded_length() {
        let buf = vec![0xC0u8];
        let mut pos = 0;
        assert!(SetCodec::load(&buf, &mut pos, RDB_TYPE_SET).is_none());
    }

    #[test]
    fn load_set_intset_decodes_items() {
        let blob = decoder_compact::intset_encode(&[1, 2, 3]);
        let buf = enc_string(&blob);
        let mut pos = 0;
        let set = SetCodec::load(&buf, &mut pos, RDB_TYPE_SET_INTSET).unwrap();
        assert_eq!(set.len(), 3);
        assert!(set.contains_key(&Bytes::from_static(b"1")));
    }

    #[test]
    fn load_set_listpack_decodes_items() {
        let items: Vec<Vec<u8>> = vec![b"a".to_vec(), b"b".to_vec()];
        let lp = decoder_compact::listpack_encode(&items);
        let buf = enc_string(&lp);
        let mut pos = 0;
        let set = SetCodec::load(&buf, &mut pos, RDB_TYPE_SET_LISTPACK).unwrap();
        assert_eq!(set.len(), 2);
        assert!(set.contains_key(&Bytes::from_static(b"a")));
    }

    #[test]
    fn load_set_rejects_duplicate_members() {
        let mut buf = Vec::new();
        save_len(&mut buf, 2);
        buf.extend_from_slice(&enc_string(b"a"));
        buf.extend_from_slice(&enc_string(b"a"));
        let mut pos = 0;
        assert!(SetCodec::load(&buf, &mut pos, RDB_TYPE_SET).is_none());
    }

    #[test]
    fn load_set_listpack_rejects_duplicate_members() {
        let items: Vec<Vec<u8>> = vec![b"a".to_vec(), b"a".to_vec()];
        let lp = decoder_compact::listpack_encode(&items);
        let buf = enc_string(&lp);
        let mut pos = 0;
        assert!(SetCodec::load(&buf, &mut pos, RDB_TYPE_SET_LISTPACK).is_none());
    }

    #[test]
    fn load_dispatch_set_listpack_rejects_duplicate_members() {
        let items: Vec<Vec<u8>> = vec![b"a".to_vec(), b"a".to_vec()];
        let lp = decoder_compact::listpack_encode(&items);
        let buf = enc_string(&lp);
        let mut pos = 0;
        assert!(SetCodec::load_dispatch(&buf, &mut pos, RDB_TYPE_SET_LISTPACK).is_none());
    }

    #[test]
    fn load_set_unknown_type_byte_returns_none() {
        let mut pos = 0;
        assert!(SetCodec::load(&[], &mut pos, 99).is_none());
    }

    #[test]
    fn intset_save_load_round_trip() {
        let mut is = IntSet::new();
        let values = [1i64, 5, -3, 100, 0];
        for v in &values {
            is.add(*v, 1000).unwrap();
        }

        // Save to bytes.
        let mut buf = Vec::new();
        SetCodec::save_intset(&is, &mut buf);

        // Load back as IntSet.
        let mut pos = 0;
        let loaded = SetCodec::load_intset(&buf, &mut pos).unwrap();
        assert_eq!(loaded.len(), values.len());
        for v in &values {
            assert!(loaded.contains(*v));
        }
        assert_eq!(pos, buf.len());
    }

    #[test]
    fn load_dispatch_intset_returns_mutable_valkey_set() {
        let blob = decoder_compact::intset_encode(&[1, 2, 3]);
        let buf = enc_string(&blob);
        let mut pos = 0;
        let obj = SetCodec::load_dispatch(&buf, &mut pos, RDB_TYPE_SET_INTSET).unwrap();
        assert!(matches!(
            obj,
            Robj::SetHashtable(ValkeySet {
                encoding: SetEncoding::Intset,
                ..
            })
        ));
        assert_eq!(obj.set_len(), 3);
        assert!(obj.set_contains(b"1"));
        assert!(obj.set_contains(b"3"));
    }
}
