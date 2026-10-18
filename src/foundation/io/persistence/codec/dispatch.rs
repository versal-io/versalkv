use super::decoder_compact;
use super::{hash, list, registry::build_registry, set, stream, string, zset, LoadFn, RdbWriter};
use crate::foundation::io::persistence::codec::types::{
    RDB_TYPE_HASH, RDB_TYPE_HASH_2, RDB_TYPE_LIST, RDB_TYPE_SET, RDB_TYPE_SET_INTSET,
    RDB_TYPE_SET_LISTPACK, RDB_TYPE_STREAM_LISTPACKS_3, RDB_TYPE_STRING, RDB_TYPE_ZSET_2,
};
use crate::foundation::io::store::structure::hash::HashHt;
use crate::foundation::io::store::structure::set::SetEncoding;
use crate::foundation::io::store::Robj;
use crate::foundation::memory::value::ListOps;
#[cfg(not(feature = "wasm"))]
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::OnceLock;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectLoadError {
    DuplicateStreamConsumer,
    DuplicateStreamConsumerPel,
    Invalid,
}

pub(crate) struct LoadedObject {
    pub(crate) object: Robj,
    pub(crate) hash_field_expires: HashMap<Vec<u8>, u64>,
    #[cfg(not(feature = "wasm"))]
    pub(crate) skipped_expired_hash_fields: Vec<Bytes>,
}

pub fn save_robj<W: RdbWriter>(obj: &Robj, w: &mut W) {
    match obj {
        Robj::StrInt(s) => string::StringCodec::save(s.as_bytes(), w),
        Robj::StrEmbstr(s) => string::StringCodec::save(s.as_bytes(), w),
        Robj::StrRaw(s) => string::StringCodec::save(&s.0, w),
        Robj::ListQuicklist(l) => list::ListCodec::save(l, w),
        Robj::ListListpack(l) => {
            w.write_len(l.len() as u64);
            for item in l.all_elements() {
                w.write_string(&item);
            }
        }
        Robj::SetHashtable(s) => set::SetCodec::save(s, w),
        Robj::SetIntset(s) => set::SetCodec::save_intset(s, w),
        Robj::SetListpack(s) => {
            w.write_string(&decoder_compact::listpack_encode(&s.all_members()));
        }
        Robj::HashHashtable(h) => hash::HashCodec::save(h, w),
        Robj::HashListpack(h) => {
            let items: Vec<Vec<u8>> = h
                .all_pairs()
                .into_iter()
                .flat_map(|(field, value)| [field, value])
                .collect();
            w.write_string(&decoder_compact::listpack_encode(&items));
        }
        Robj::ZSetSkiplist(z) => zset::ZSetCodec::save(z, w),
        Robj::ZSetListpack(z) => {
            let items: Vec<Vec<u8>> = z
                .all_pairs()
                .into_iter()
                .flat_map(|(member, score)| [member, score.to_string().into_bytes()])
                .collect();
            w.write_string(&decoder_compact::listpack_encode(&items));
        }
        Robj::StreamListpack(s) => stream::StreamCodec::save(s, w),
        Robj::StreamRadix(_) => unreachable!("radix stream encoding not implemented"),
    }
}

pub fn save_robj_with_hash_field_expires<W: RdbWriter>(
    obj: &Robj,
    hash_field_expires: Option<&HashMap<Vec<u8>, u64>>,
    w: &mut W,
) {
    match (obj, hash_field_expires) {
        (Robj::HashHashtable(h), Some(fields)) if !fields.is_empty() => {
            hash::HashCodec::save_with_field_expires(h, fields, w);
        }
        (Robj::HashListpack(_), Some(fields)) if !fields.is_empty() => {
            let mut hash = HashHt::new();
            for (field, value) in obj.hash_entries() {
                hash.insert(field, value);
            }
            hash::HashCodec::save_with_field_expires(&hash, fields, w);
        }
        _ => save_robj(obj, w),
    }
}

pub fn type_byte(obj: &Robj) -> u8 {
    match obj {
        Robj::StrInt(_) | Robj::StrEmbstr(_) | Robj::StrRaw(_) => RDB_TYPE_STRING,
        Robj::ListQuicklist(_) | Robj::ListListpack(_) => RDB_TYPE_LIST,
        Robj::SetHashtable(s) => match s.encoding {
            SetEncoding::Intset => RDB_TYPE_SET_INTSET,
            SetEncoding::Listpack => RDB_TYPE_SET_LISTPACK,
            SetEncoding::Hashtable => RDB_TYPE_SET,
        },
        Robj::SetIntset(_) => RDB_TYPE_SET_INTSET,
        Robj::SetListpack(_) => RDB_TYPE_SET_LISTPACK,
        Robj::HashHashtable(_) => RDB_TYPE_HASH,
        Robj::HashListpack(_) => super::types::RDB_TYPE_HASH_LISTPACK,
        Robj::ZSetSkiplist(_) => RDB_TYPE_ZSET_2,
        Robj::ZSetListpack(_) => super::types::RDB_TYPE_ZSET_LISTPACK,
        Robj::StreamListpack(_) | Robj::StreamRadix(_) => RDB_TYPE_STREAM_LISTPACKS_3,
    }
}

pub fn type_byte_with_hash_field_expires(
    obj: &Robj,
    hash_field_expires: Option<&HashMap<Vec<u8>, u64>>,
) -> u8 {
    match (obj, hash_field_expires) {
        (Robj::HashHashtable(_) | Robj::HashListpack(_), Some(fields)) if !fields.is_empty() => {
            RDB_TYPE_HASH_2
        }
        _ => type_byte(obj),
    }
}

pub fn load_robj(buf: &[u8], pos: &mut usize, type_byte: u8) -> Option<Robj> {
    static REGISTRY: OnceLock<[Option<LoadFn>; 256]> = OnceLock::new();
    let registry = REGISTRY.get_or_init(build_registry);
    match registry[type_byte as usize] {
        Some(loader) => loader(buf, pos, type_byte),
        None => None,
    }
}

pub fn load_typed_object(buf: &[u8], pos: &mut usize) -> Option<Robj> {
    let type_byte = *buf.get(*pos)?;
    *pos += 1;
    load_robj(buf, pos, type_byte)
}

pub fn load_robj_with_hash_field_expires(
    buf: &[u8],
    pos: &mut usize,
    type_byte: u8,
    now_ms: Option<u64>,
) -> Option<(Robj, HashMap<Vec<u8>, u64>)> {
    load_robj_with_hash_field_expires_detailed(buf, pos, type_byte, now_ms)
        .map(|loaded| (loaded.object, loaded.hash_field_expires))
}

pub(crate) fn load_robj_with_hash_field_expires_detailed(
    buf: &[u8],
    pos: &mut usize,
    type_byte: u8,
    now_ms: Option<u64>,
) -> Option<LoadedObject> {
    if type_byte == RDB_TYPE_HASH_2 {
        let loaded = hash::HashCodec::load_with_field_expires_detailed(buf, pos, now_ms)?;
        return Some(LoadedObject {
            object: Robj::HashHashtable(loaded.0),
            hash_field_expires: loaded.1,
            #[cfg(not(feature = "wasm"))]
            skipped_expired_hash_fields: loaded.2,
        });
    }
    load_robj(buf, pos, type_byte).map(|object| LoadedObject {
        object,
        hash_field_expires: HashMap::new(),
        #[cfg(not(feature = "wasm"))]
        skipped_expired_hash_fields: Vec::new(),
    })
}

pub fn load_typed_object_with_hash_field_expires(
    buf: &[u8],
    pos: &mut usize,
    now_ms: Option<u64>,
) -> Option<(Robj, HashMap<Vec<u8>, u64>)> {
    let type_byte = *buf.get(*pos)?;
    *pos += 1;
    load_robj_with_hash_field_expires(buf, pos, type_byte, now_ms)
}

pub fn load_typed_object_with_hash_field_expires_detailed(
    buf: &[u8],
    pos: &mut usize,
    now_ms: Option<u64>,
) -> Result<(Robj, HashMap<Vec<u8>, u64>), ObjectLoadError> {
    let type_byte = *buf.get(*pos).ok_or(ObjectLoadError::Invalid)?;
    *pos += 1;
    if type_byte == super::types::RDB_TYPE_STREAM_LISTPACKS
        || type_byte == super::types::RDB_TYPE_STREAM_LISTPACKS_2
    {
        let stream = stream::StreamCodec::load_legacy_listpacks_detailed(buf, pos, type_byte)
            .map_err(|error| match error {
                stream::StreamLoadError::DuplicateConsumer => {
                    ObjectLoadError::DuplicateStreamConsumer
                }
                stream::StreamLoadError::DuplicateConsumerPel => {
                    ObjectLoadError::DuplicateStreamConsumerPel
                }
                stream::StreamLoadError::Invalid => ObjectLoadError::Invalid,
            })?;
        return Ok((Robj::StreamListpack(Box::new(stream)), HashMap::new()));
    }
    load_robj_with_hash_field_expires(buf, pos, type_byte, now_ms).ok_or(ObjectLoadError::Invalid)
}
