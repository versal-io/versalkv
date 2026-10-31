use bytes::{Bytes, BytesMut};
use std::fmt::Debug;

use crate::foundation::io::store::structure::hash::{HashHt, HashLp};
use crate::foundation::io::store::structure::hashtable::HashTable;
use crate::foundation::io::store::structure::list::{ListLp, ListValue};
use crate::foundation::io::store::structure::path_hash::PathHash;
use crate::foundation::io::store::structure::set::{IntSet, SetLp, ValkeySet};
use crate::foundation::io::store::structure::stream::{StreamLp, ValkeyStream};
use crate::foundation::io::store::structure::string::encoding::int_encodable;
use crate::foundation::io::store::structure::string::{StrEmbstr, StrInt, StrRaw};
use crate::foundation::io::store::structure::zset::{ZSetLp, ZSetSl};
use crate::foundation::memory::value::{
    Encoding, EncodingChange, HashOps, ListOps, SetOps, StreamOps, Type, ZSetOps,
};

pub trait Collection: Debug {
    fn type_name(&self) -> &'static str;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn memory_usage(&self) -> usize;
}

impl Collection for ListValue {
    fn type_name(&self) -> &'static str {
        "list"
    }
    fn len(&self) -> usize {
        self.inner.len()
    }
    fn memory_usage(&self) -> usize {
        self.inner
            .iter()
            .map(|b| b.len() + std::mem::size_of::<Bytes>())
            .sum::<usize>()
            + std::mem::size_of_val(self)
    }
}

impl Collection for ListLp {
    fn type_name(&self) -> &'static str {
        "list"
    }
    fn len(&self) -> usize {
        ListOps::len(self)
    }
    fn memory_usage(&self) -> usize {
        self.memory_usage()
    }
}

impl Collection for HashHt {
    fn type_name(&self) -> &'static str {
        "hash"
    }
    fn len(&self) -> usize {
        self.data.len()
    }
    fn memory_usage(&self) -> usize {
        self.data
            .iter()
            .map(|(k, v)| k.len() + v.len() + std::mem::size_of::<Bytes>() * 2)
            .sum::<usize>()
            + std::mem::size_of_val(self)
    }
}

impl Collection for HashLp {
    fn type_name(&self) -> &'static str {
        "hash"
    }
    fn len(&self) -> usize {
        HashOps::len(self)
    }
    fn memory_usage(&self) -> usize {
        self.memory_usage()
    }
}

impl Collection for ValkeySet {
    fn type_name(&self) -> &'static str {
        "set"
    }
    fn len(&self) -> usize {
        self.data.len()
    }
    fn memory_usage(&self) -> usize {
        self.data
            .keys()
            .map(|b| b.len() + std::mem::size_of::<Bytes>())
            .sum::<usize>()
            + std::mem::size_of_val(self)
    }
}

impl Collection for IntSet {
    fn type_name(&self) -> &'static str {
        "set"
    }
    fn len(&self) -> usize {
        IntSet::len(self)
    }
    fn memory_usage(&self) -> usize {
        IntSet::memory_usage(self)
    }
}

impl Collection for SetLp {
    fn type_name(&self) -> &'static str {
        "set"
    }
    fn len(&self) -> usize {
        SetOps::len(self)
    }
    fn memory_usage(&self) -> usize {
        self.memory_usage()
    }
}

impl Collection for ZSetSl {
    fn type_name(&self) -> &'static str {
        "zset"
    }
    fn len(&self) -> usize {
        self.dict().len()
    }
    fn memory_usage(&self) -> usize {
        ZSetSl::memory_usage(self)
    }
}

impl Collection for ZSetLp {
    fn type_name(&self) -> &'static str {
        "zset"
    }
    fn len(&self) -> usize {
        ZSetOps::len(self)
    }
    fn memory_usage(&self) -> usize {
        self.memory_usage()
    }
}

impl Collection for ValkeyStream {
    fn type_name(&self) -> &'static str {
        "stream"
    }
    fn len(&self) -> usize {
        self.len()
    }
    fn memory_usage(&self) -> usize {
        self.memory_usage()
    }
}

impl Collection for PathHash {
    fn type_name(&self) -> &'static str {
        "pathhash"
    }
    fn len(&self) -> usize {
        self.len()
    }
    fn memory_usage(&self) -> usize {
        self.memory_usage()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Robj {
    StrInt(StrInt),
    StrEmbstr(StrEmbstr),
    StrRaw(StrRaw),
    ListQuicklist(ListValue),
    ListListpack(ListLp),
    HashHashtable(HashHt),
    HashListpack(HashLp),
    SetHashtable(ValkeySet),
    SetIntset(IntSet),
    SetListpack(SetLp),
    ZSetSkiplist(ZSetSl),
    ZSetListpack(ZSetLp),
    StreamListpack(Box<ValkeyStream>),
    StreamRadix(StreamLp),
    PathHash(Box<PathHash>),
}

impl Robj {
    pub fn string(data: impl Into<Bytes>) -> Self {
        let data = data.into();
        if let Some(n) = int_encodable(&data) {
            Robj::StrInt(StrInt::new(n, data))
        } else if data.len()
            <= crate::foundation::io::store::structure::string::encoding::EMBSTR_SIZE_LIMIT
        {
            Robj::StrEmbstr(StrEmbstr::new(data))
        } else {
            Robj::StrRaw(StrRaw(data))
        }
    }

    pub fn string_raw(data: impl Into<Bytes>) -> Self {
        Robj::StrRaw(StrRaw(data.into()))
    }

    pub fn type_(&self) -> Type {
        match self {
            Robj::StrInt(_) | Robj::StrEmbstr(_) | Robj::StrRaw(_) => Type::String,
            Robj::ListQuicklist(_) | Robj::ListListpack(_) => Type::List,
            Robj::HashHashtable(_) | Robj::HashListpack(_) => Type::Hash,
            Robj::SetHashtable(_) | Robj::SetIntset(_) | Robj::SetListpack(_) => Type::Set,
            Robj::ZSetSkiplist(_) | Robj::ZSetListpack(_) => Type::ZSet,
            Robj::StreamListpack(_) | Robj::StreamRadix(_) => Type::Stream,
            Robj::PathHash(_) => Type::PathHash,
        }
    }

    pub fn encoding(&self) -> Encoding {
        match self {
            Robj::StrInt(s) => s.encoding(),
            Robj::StrEmbstr(s) => s.encoding(),
            Robj::StrRaw(s) => s.encoding(),
            Robj::SetHashtable(s) => s.encoding(),
            Robj::SetIntset(s) => s.encoding(),
            Robj::SetListpack(s) => s.encoding(),
            Robj::ListQuicklist(s) => s.encoding(),
            Robj::ListListpack(_) => Encoding::Listpack,
            Robj::HashHashtable(s) => s.encoding(),
            Robj::HashListpack(_) => Encoding::Listpack,
            Robj::ZSetSkiplist(s) => s.encoding(),
            Robj::ZSetListpack(_) => Encoding::Listpack,
            Robj::StreamListpack(_) => Encoding::StreamP,
            Robj::StreamRadix(_) => Encoding::StreamP,
            Robj::PathHash(_) => Encoding::PathHash,
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Robj::StrInt(s) => s.as_bytes(),
            Robj::StrEmbstr(s) => s.as_bytes(),
            Robj::StrRaw(s) => s.as_bytes(),
            _ => b"",
        }
    }

    pub fn is_string(&self) -> bool {
        matches!(self, Robj::StrInt(_) | Robj::StrEmbstr(_) | Robj::StrRaw(_))
    }

    /// Overwrite a string range, reusing uniquely owned raw storage.
    /// Returns None for non-strings or an overflowing range.
    pub(crate) fn set_string_range(&mut self, offset: usize, patch: &[u8]) -> Option<usize> {
        if !self.is_string() {
            return None;
        }
        if patch.is_empty() {
            return Some(self.as_bytes().len());
        }
        let end = offset.checked_add(patch.len())?;
        let mut buffer = match self {
            Robj::StrRaw(raw) => std::mem::take(&mut raw.0)
                .try_into_mut()
                .unwrap_or_else(|shared| BytesMut::from(shared.as_ref())),
            Robj::StrInt(value) => BytesMut::from(value.as_bytes()),
            Robj::StrEmbstr(value) => BytesMut::from(value.as_bytes()),
            _ => return None,
        };
        if buffer.len() < end {
            buffer.resize(end, 0);
        }
        buffer[offset..end].copy_from_slice(patch);
        let len = buffer.len();
        *self = Self::string_raw(buffer.freeze());
        Some(len)
    }

    /// Produce a semantically identical value whose string payload is newly
    /// allocated. Container clones preserve their existing ownership model;
    /// active defrag uses this only while the owning DB entry retains metadata.
    pub fn relocated_clone(&self) -> Self {
        match self {
            Robj::StrInt(value) => Robj::StrInt(value.clone()),
            Robj::StrEmbstr(value) => Robj::string(Bytes::copy_from_slice(value.as_bytes())),
            Robj::StrRaw(value) => Robj::string_raw(Bytes::copy_from_slice(value.as_bytes())),
            _ => self.clone(),
        }
    }

    pub fn type_name(&self) -> &'static str {
        self.type_().name()
    }

    pub fn encoding_name(&self) -> &'static str {
        self.encoding().name()
    }

    pub fn as_collection(&self) -> Option<&dyn Collection> {
        match self {
            Robj::ListQuicklist(v) => Some(v as &dyn Collection),
            Robj::ListListpack(v) => Some(v as &dyn Collection),
            Robj::HashHashtable(v) => Some(v as &dyn Collection),
            Robj::HashListpack(v) => Some(v as &dyn Collection),
            Robj::SetHashtable(v) => Some(v as &dyn Collection),
            Robj::SetIntset(v) => Some(v as &dyn Collection),
            Robj::SetListpack(v) => Some(v as &dyn Collection),
            Robj::ZSetSkiplist(v) => Some(v as &dyn Collection),
            Robj::ZSetListpack(v) => Some(v as &dyn Collection),
            Robj::StreamListpack(v) => Some(v.as_ref() as &dyn Collection),
            Robj::StreamRadix(_) => None,
            Robj::PathHash(v) => Some(v.as_ref() as &dyn Collection),
            Robj::StrInt(_) | Robj::StrEmbstr(_) | Robj::StrRaw(_) => None,
        }
    }

    pub fn as_collection_mut(&mut self) -> Option<&mut dyn Collection> {
        match self {
            Robj::ListQuicklist(v) => Some(v as &mut dyn Collection),
            Robj::ListListpack(v) => Some(v as &mut dyn Collection),
            Robj::HashHashtable(v) => Some(v as &mut dyn Collection),
            Robj::HashListpack(v) => Some(v as &mut dyn Collection),
            Robj::SetHashtable(v) => Some(v as &mut dyn Collection),
            Robj::SetIntset(v) => Some(v as &mut dyn Collection),
            Robj::SetListpack(v) => Some(v as &mut dyn Collection),
            Robj::ZSetSkiplist(v) => Some(v as &mut dyn Collection),
            Robj::ZSetListpack(v) => Some(v as &mut dyn Collection),
            Robj::StreamListpack(v) => Some(v.as_mut() as &mut dyn Collection),
            Robj::StreamRadix(_) => None,
            Robj::PathHash(v) => Some(v.as_mut() as &mut dyn Collection),
            Robj::StrInt(_) | Robj::StrEmbstr(_) | Robj::StrRaw(_) => None,
        }
    }

    /// Mirrors C's `RDB_LOAD_ERR_EMPTY_KEY` classification. A stream with no
    /// entries is still a valid object because its consumer groups and stream
    /// metadata are persisted separately from the entries.
    pub fn is_empty_rdb_key(&self) -> bool {
        match self {
            Robj::ListQuicklist(_) | Robj::ListListpack(_) => self.list_is_empty(),
            Robj::SetHashtable(_) | Robj::SetIntset(_) | Robj::SetListpack(_) => {
                self.set_is_empty()
            }
            Robj::HashHashtable(_) | Robj::HashListpack(_) => self.hash_is_empty(),
            Robj::ZSetSkiplist(_) | Robj::ZSetListpack(_) => self.zset_is_empty(),
            Robj::StreamListpack(_) | Robj::StreamRadix(_) => false,
            Robj::PathHash(_) => false,
            Robj::StrInt(_) | Robj::StrEmbstr(_) | Robj::StrRaw(_) => false,
        }
    }

    pub fn estimated_size(&self) -> usize {
        match self {
            Robj::StrInt(s) => s.len() + 16,
            Robj::StrEmbstr(s) => s.len() + 16,
            Robj::StrRaw(s) => s.0.len() + 16,
            Robj::ListQuicklist(v) => v.memory_usage(),
            Robj::ListListpack(v) => v.memory_usage(),
            Robj::HashHashtable(v) => v.memory_usage(),
            Robj::HashListpack(v) => v.memory_usage(),
            Robj::SetHashtable(v) => v.memory_usage(),
            Robj::SetIntset(v) => v.memory_usage(),
            Robj::SetListpack(v) => v.memory_usage(),
            Robj::ZSetSkiplist(v) => v.memory_usage(),
            Robj::ZSetListpack(v) => v.memory_usage(),
            Robj::StreamListpack(v) => v.memory_usage(),
            Robj::StreamRadix(_) => std::mem::size_of::<Robj>(),
            Robj::PathHash(v) => v.memory_usage(),
        }
    }

    // ---- Set typed methods ----

    pub fn set_add(&mut self, member: &[u8], max_intset_entries: usize) -> bool {
        match self {
            Robj::SetHashtable(s) => SetOps::add(s, member).expect("hashtable add never fails"),
            Robj::SetListpack(s) => match SetOps::add(s, member) {
                Ok(added) => added,
                Err(EncodingChange::Upgrade) => {
                    Self::upgrade_set_listpack_to_hashtable(self, member)
                }
                Err(EncodingChange::Downgrade) => unreachable!("listpack never downgrades"),
            },
            Robj::SetIntset(s) => {
                let value = match std::str::from_utf8(member)
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                {
                    Some(v) => v,
                    None => {
                        return Self::upgrade_set_intset_to_hashtable(self, member);
                    }
                };
                match IntSet::add(s, value, max_intset_entries) {
                    Ok(added) => added,
                    Err(EncodingChange::Upgrade) => {
                        Self::upgrade_set_intset_to_hashtable(self, member)
                    }
                    Err(EncodingChange::Downgrade) => {
                        unreachable!("intset never downgrades")
                    }
                }
            }
            _ => panic!("Robj::set_add called on non-set type"),
        }
    }

    fn upgrade_set_intset_to_hashtable(this: &mut Self, member: &[u8]) -> bool {
        let old = std::mem::replace(this, Robj::SetIntset(IntSet::new()));
        let intset = match old {
            Robj::SetIntset(s) => s,
            _ => unreachable!("upgrade_set_intset_to_hashtable called on non-intset"),
        };
        let mut ht = ValkeySet::from_hashset(intset.into_hashset());
        let added = SetOps::add(&mut ht, member).expect("hashtable add never fails");
        *this = Robj::SetHashtable(ht);
        added
    }

    fn upgrade_set_listpack_to_hashtable(this: &mut Self, member: &[u8]) -> bool {
        let old = std::mem::replace(this, Robj::SetListpack(SetLp::new()));
        let listpack = match old {
            Robj::SetListpack(s) => s,
            _ => unreachable!("upgrade_set_listpack_to_hashtable called on non-listpack"),
        };
        let mut ht = ValkeySet::from_hashset(
            listpack
                .all_members()
                .into_iter()
                .map(Bytes::from)
                .collect(),
        );
        let added = SetOps::add(&mut ht, member).expect("hashtable add never fails");
        *this = Robj::SetHashtable(ht);
        added
    }

    pub fn set_remove(&mut self, member: &[u8]) -> bool {
        match self {
            Robj::SetHashtable(s) => {
                SetOps::remove(s, member).expect("hashtable remove never fails")
            }
            Robj::SetIntset(s) => SetOps::remove(s, member).expect("intset remove never fails"),
            Robj::SetListpack(s) => SetOps::remove(s, member).expect("listpack remove never fails"),
            _ => panic!("Robj::set_remove called on non-set type"),
        }
    }

    pub fn set_contains(&self, member: &[u8]) -> bool {
        match self {
            Robj::SetHashtable(s) => SetOps::contains(s, member),
            Robj::SetIntset(s) => SetOps::contains(s, member),
            Robj::SetListpack(s) => SetOps::contains(s, member),
            _ => panic!("Robj::set_contains called on non-set type"),
        }
    }

    pub fn set_len(&self) -> usize {
        match self {
            Robj::SetHashtable(s) => SetOps::len(s),
            Robj::SetIntset(s) => SetOps::len(s),
            Robj::SetListpack(s) => SetOps::len(s),
            _ => panic!("Robj::set_len called on non-set type"),
        }
    }

    pub fn set_is_empty(&self) -> bool {
        match self {
            Robj::SetHashtable(s) => SetOps::is_empty(s),
            Robj::SetIntset(s) => SetOps::is_empty(s),
            Robj::SetListpack(s) => SetOps::is_empty(s),
            _ => panic!("Robj::set_is_empty called on non-set type"),
        }
    }

    pub fn set_members(&self) -> Vec<Vec<u8>> {
        match self {
            Robj::SetHashtable(s) => SetOps::members(s),
            Robj::SetIntset(s) => SetOps::members(s),
            Robj::SetListpack(s) => SetOps::members(s),
            _ => panic!("Robj::set_members called on non-set type"),
        }
    }

    // ---- List typed methods ----

    pub fn list_push_back(&mut self, value: &[u8]) -> usize {
        match self {
            Robj::ListQuicklist(l) => {
                ListOps::push_back(l, value).expect("quicklist push never fails")
            }
            Robj::ListListpack(l) => match ListOps::push_back(l, value) {
                Ok(len) => len,
                Err(EncodingChange::Upgrade) => {
                    let old = std::mem::replace(self, Robj::ListListpack(ListLp::new()));
                    let listpack = match old {
                        Robj::ListListpack(v) => v,
                        _ => unreachable!("listpack list replacement mismatch"),
                    };
                    let mut quicklist = ListValue::from_vecdeque(
                        listpack
                            .all_elements()
                            .into_iter()
                            .map(Bytes::from)
                            .collect(),
                    );
                    let len = ListOps::push_back(&mut quicklist, value)
                        .expect("quicklist push never fails");
                    *self = Robj::ListQuicklist(quicklist);
                    len
                }
                Err(EncodingChange::Downgrade) => unreachable!("listpack never downgrades"),
            },
            _ => panic!("Robj::list_push_back called on non-list type"),
        }
    }

    pub fn list_push_front(&mut self, value: &[u8]) -> usize {
        match self {
            Robj::ListQuicklist(l) => {
                ListOps::push_front(l, value).expect("quicklist push never fails")
            }
            Robj::ListListpack(l) => match ListOps::push_front(l, value) {
                Ok(len) => len,
                Err(EncodingChange::Upgrade) => {
                    let old = std::mem::replace(self, Robj::ListListpack(ListLp::new()));
                    let listpack = match old {
                        Robj::ListListpack(v) => v,
                        _ => unreachable!("listpack list replacement mismatch"),
                    };
                    let mut quicklist = ListValue::from_vecdeque(
                        listpack
                            .all_elements()
                            .into_iter()
                            .map(Bytes::from)
                            .collect(),
                    );
                    let len = ListOps::push_front(&mut quicklist, value)
                        .expect("quicklist push never fails");
                    *self = Robj::ListQuicklist(quicklist);
                    len
                }
                Err(EncodingChange::Downgrade) => unreachable!("listpack never downgrades"),
            },
            _ => panic!("Robj::list_push_front called on non-list type"),
        }
    }

    pub fn list_pop_back(&mut self) -> Option<Bytes> {
        match self {
            Robj::ListQuicklist(l) => ListOps::pop_back(l).expect("quicklist pop never fails"),
            Robj::ListListpack(l) => ListOps::pop_back(l).expect("listpack pop never fails"),
            _ => panic!("Robj::list_pop_back called on non-list type"),
        }
    }

    pub fn list_pop_front(&mut self) -> Option<Bytes> {
        match self {
            Robj::ListQuicklist(l) => ListOps::pop_front(l).expect("quicklist pop never fails"),
            Robj::ListListpack(l) => ListOps::pop_front(l).expect("listpack pop never fails"),
            _ => panic!("Robj::list_pop_front called on non-list type"),
        }
    }

    pub fn list_len(&self) -> usize {
        match self {
            Robj::ListQuicklist(l) => ListOps::len(l),
            Robj::ListListpack(l) => ListOps::len(l),
            _ => panic!("Robj::list_len called on non-list type"),
        }
    }

    pub fn list_is_empty(&self) -> bool {
        match self {
            Robj::ListQuicklist(l) => ListOps::is_empty(l),
            Robj::ListListpack(l) => ListOps::is_empty(l),
            _ => panic!("Robj::list_is_empty called on non-list type"),
        }
    }

    pub fn list_get(&self, index: isize) -> Option<Vec<u8>> {
        match self {
            Robj::ListQuicklist(l) => ListOps::get(l, index),
            Robj::ListListpack(l) => ListOps::get(l, index),
            _ => panic!("Robj::list_get called on non-list type"),
        }
    }

    // ---- Hash typed methods ----

    pub fn hash_set(&mut self, field: &[u8], value: &[u8]) -> bool {
        match self {
            Robj::HashHashtable(h) => {
                HashOps::set(h, field, value).expect("hashtable set never fails")
            }
            Robj::HashListpack(h) => match HashOps::set(h, field, value) {
                Ok(added) => added,
                Err(EncodingChange::Upgrade) => {
                    let old = std::mem::replace(self, Robj::HashListpack(HashLp::new()));
                    let listpack = match old {
                        Robj::HashListpack(v) => v,
                        _ => unreachable!("listpack hash replacement mismatch"),
                    };
                    let mut hashtable = HashHt::new();
                    for (old_field, old_value) in listpack.all_pairs() {
                        hashtable
                            .data
                            .insert(Bytes::from(old_field), Bytes::from(old_value));
                    }
                    let added = HashOps::set(&mut hashtable, field, value)
                        .expect("hashtable set never fails");
                    *self = Robj::HashHashtable(hashtable);
                    added
                }
                Err(EncodingChange::Downgrade) => unreachable!("listpack never downgrades"),
            },
            _ => panic!("Robj::hash_set called on non-hash type"),
        }
    }

    pub fn hash_get(&self, field: &[u8]) -> Option<Vec<u8>> {
        match self {
            Robj::HashHashtable(h) => HashOps::get(h, field),
            Robj::HashListpack(h) => HashOps::get(h, field),
            _ => panic!("Robj::hash_get called on non-hash type"),
        }
    }

    pub fn hash_del(&mut self, field: &[u8]) -> bool {
        match self {
            Robj::HashHashtable(h) => HashOps::del(h, field).expect("hashtable del never fails"),
            Robj::HashListpack(h) => HashOps::del(h, field).expect("listpack del never fails"),
            _ => panic!("Robj::hash_del called on non-hash type"),
        }
    }

    pub fn hash_len(&self) -> usize {
        match self {
            Robj::HashHashtable(h) => HashOps::len(h),
            Robj::HashListpack(h) => HashOps::len(h),
            _ => panic!("Robj::hash_len called on non-hash type"),
        }
    }

    pub fn hash_is_empty(&self) -> bool {
        match self {
            Robj::HashHashtable(h) => HashOps::is_empty(h),
            Robj::HashListpack(h) => HashOps::is_empty(h),
            _ => panic!("Robj::hash_is_empty called on non-hash type"),
        }
    }

    pub fn hash_contains(&self, field: &[u8]) -> bool {
        match self {
            Robj::HashHashtable(h) => HashOps::contains(h, field),
            Robj::HashListpack(h) => HashOps::contains(h, field),
            _ => panic!("Robj::hash_contains called on non-hash type"),
        }
    }

    /// Return field/value pairs in the encoding's stable iteration order.
    /// Listpack callers receive owned bytes so reads never need to materialize
    /// a hashtable just to borrow entries.
    pub fn hash_entries(&self) -> Vec<(Bytes, Bytes)> {
        match self {
            Robj::HashHashtable(h) => h
                .data
                .iter()
                .map(|(field, value)| (field.clone(), value.clone()))
                .collect(),
            Robj::HashListpack(h) => h
                .all_pairs()
                .into_iter()
                .map(|(field, value)| (Bytes::from(field), Bytes::from(value)))
                .collect(),
            _ => panic!("Robj::hash_entries called on non-hash type"),
        }
    }

    // ---- ZSet typed methods ----

    /// Choose physical storage for a newly created STORE result.
    pub fn zset_from_dict_with_limits(
        dict: HashTable<f64>,
        max_entries: usize,
        max_value_size: usize,
    ) -> Self {
        if max_entries == 0
            || dict.len() > max_entries
            || dict.keys().any(|member| member.len() > max_value_size)
        {
            return Robj::ZSetSkiplist(ZSetSl::from_dict(dict));
        }
        let mut listpack = ZSetLp::new();
        for (member, score) in &dict {
            listpack
                .add_with_threshold(member, *score, max_entries, max_value_size)
                .expect("new zset members were checked against listpack limits");
        }
        Robj::ZSetListpack(listpack)
    }

    /// Explicitly choose storage for load or STORE, never for ordinary writes.
    /// These creation paths may downgrade a tree to listpack.
    pub fn zset_convert_with_limits(&mut self, max_entries: usize, max_value_size: usize) {
        match self {
            Robj::ZSetSkiplist(zset) => {
                if max_entries > 0
                    && ZSetOps::len(zset) <= max_entries
                    && zset
                        .dict()
                        .keys()
                        .all(|member| member.len() <= max_value_size)
                {
                    let dict = std::mem::take(zset).into_dict();
                    *self = Self::zset_from_dict_with_limits(dict, max_entries, max_value_size);
                }
            }
            Robj::ZSetListpack(listpack)
                if max_entries == 0
                    || ZSetOps::len(listpack) > max_entries
                    || listpack
                        .all_pairs()
                        .iter()
                        .any(|(member, _)| member.len() > max_value_size) =>
            {
                self.zset_upgrade_to_tree();
            }
            _ => {}
        }
    }

    /// Destructively upgrade a compact object for commands such as SORT.
    /// The caller retains the DB entry and its expiration metadata.
    pub fn zset_upgrade_to_tree(&mut self) {
        let Robj::ZSetListpack(listpack) = self else {
            return;
        };
        let mut dict = HashTable::new();
        for (member, score) in listpack.all_pairs() {
            dict.insert(Bytes::from(member), score);
        }
        *self = Robj::ZSetSkiplist(ZSetSl::from_dict(dict));
    }

    pub fn zset_add(&mut self, member: &[u8], score: f64) -> bool {
        match self {
            Robj::ZSetSkiplist(z) => {
                ZSetOps::add(z, member, score).expect("skiplist add never fails")
            }
            Robj::ZSetListpack(z) => match ZSetOps::add(z, member, score) {
                Ok(added) => added,
                Err(EncodingChange::Upgrade) => {
                    let old = std::mem::replace(self, Robj::ZSetListpack(ZSetLp::new()));
                    let listpack = match old {
                        Robj::ZSetListpack(v) => v,
                        _ => unreachable!("listpack zset replacement mismatch"),
                    };
                    let mut skiplist = ZSetSl::new();
                    for (old_member, old_score) in listpack.all_pairs() {
                        ZSetOps::add(&mut skiplist, &old_member, old_score)
                            .expect("skiplist add never fails");
                    }
                    let added = ZSetOps::add(&mut skiplist, member, score)
                        .expect("skiplist add never fails");
                    *self = Robj::ZSetSkiplist(skiplist);
                    added
                }
                Err(EncodingChange::Downgrade) => unreachable!("listpack never downgrades"),
            },
            _ => panic!("Robj::zset_add called on non-zset type"),
        }
    }

    pub fn zset_remove(&mut self, member: &[u8]) -> bool {
        match self {
            Robj::ZSetSkiplist(z) => {
                ZSetOps::remove(z, member).expect("skiplist remove never fails")
            }
            Robj::ZSetListpack(z) => {
                ZSetOps::remove(z, member).expect("listpack remove never fails")
            }
            _ => panic!("Robj::zset_remove called on non-zset type"),
        }
    }

    pub fn zset_score(&self, member: &[u8]) -> Option<f64> {
        match self {
            Robj::ZSetSkiplist(z) => ZSetOps::score(z, member),
            Robj::ZSetListpack(z) => ZSetOps::score(z, member),
            _ => panic!("Robj::zset_score called on non-zset type"),
        }
    }

    pub fn zset_len(&self) -> usize {
        match self {
            Robj::ZSetSkiplist(z) => ZSetOps::len(z),
            Robj::ZSetListpack(z) => ZSetOps::len(z),
            _ => panic!("Robj::zset_len called on non-zset type"),
        }
    }

    pub fn zset_is_empty(&self) -> bool {
        match self {
            Robj::ZSetSkiplist(z) => ZSetOps::is_empty(z),
            Robj::ZSetListpack(z) => ZSetOps::is_empty(z),
            _ => panic!("Robj::zset_is_empty called on non-zset type"),
        }
    }

    /// Return ZSet members and scores in score/member order without exposing
    /// an encoding-specific Dict or SkipList borrow.
    pub fn zset_entries(&self) -> Vec<(Bytes, f64)> {
        match self {
            Robj::ZSetSkiplist(z) => z.sorted_entries(),
            Robj::ZSetListpack(z) => z
                .all_pairs()
                .into_iter()
                .map(|(member, score)| (Bytes::from(member), score))
                .collect(),
            _ => panic!("Robj::zset_entries called on non-zset type"),
        }
    }

    /// Return a member's zero-based rank in score/member order.
    pub fn zset_rank(&self, member: &[u8]) -> Option<usize> {
        match self {
            Robj::ZSetSkiplist(z) => z.rank(member),
            Robj::ZSetListpack(z) => z
                .all_pairs()
                .iter()
                .position(|(candidate, _)| candidate.as_slice() == member),
            _ => panic!("Robj::zset_rank called on non-zset type"),
        }
    }

    /// Return a rank interval using the same negative-index and reverse
    /// semantics as the Skiplist-backed implementation.
    pub fn zset_range_by_rank(&self, start: i64, end: i64, rev: bool) -> Vec<(Bytes, f64)> {
        match self {
            Robj::ZSetSkiplist(z) => z.range_by_rank(start, end, rev),
            Robj::ZSetListpack(z) => {
                let entries = z
                    .all_pairs()
                    .into_iter()
                    .map(|(member, score)| (Bytes::from(member), score))
                    .collect::<Vec<_>>();
                let len = entries.len() as i64;
                if len == 0 {
                    return Vec::new();
                }
                let mut range_start = if start < 0 { len + start } else { start };
                let mut range_end = if end < 0 { len + end } else { end };
                if range_start < 0 {
                    range_start = 0;
                }
                if range_end >= len {
                    range_end = len - 1;
                }
                if range_start > range_end {
                    return Vec::new();
                }
                if rev {
                    let first = (len - 1 - range_end) as usize;
                    let last = (len - 1 - range_start) as usize;
                    entries[first..=last].iter().cloned().rev().collect()
                } else {
                    entries[range_start as usize..=range_end as usize].to_vec()
                }
            }
            _ => panic!("Robj::zset_range_by_rank called on non-zset type"),
        }
    }

    /// Return a score interval with C-compatible inclusive/exclusive bounds,
    /// offset, limit and reverse ordering.
    pub fn zset_range_by_score(
        &self,
        min: (f64, bool),
        max: (f64, bool),
        offset: i64,
        limit: i64,
        rev: bool,
    ) -> Vec<(Bytes, f64)> {
        match self {
            Robj::ZSetSkiplist(z) => z.range_by_score(min, max, offset, limit, rev),
            Robj::ZSetListpack(_) => {
                let mut entries = self
                    .zset_entries()
                    .into_iter()
                    .filter(|(_, score)| {
                        (*score > min.0 || (min.1 && *score == min.0))
                            && (*score < max.0 || (max.1 && *score == max.0))
                    })
                    .collect::<Vec<_>>();
                if rev {
                    entries.reverse();
                }
                let skip = offset.max(0) as usize;
                let take = if limit < 0 {
                    usize::MAX
                } else {
                    limit as usize
                };
                entries.into_iter().skip(skip).take(take).collect()
            }
            _ => panic!("Robj::zset_range_by_score called on non-zset type"),
        }
    }

    pub fn zset_count_by_score(&self, min: (f64, bool), max: (f64, bool)) -> usize {
        match self {
            Robj::ZSetSkiplist(zset) => zset.count_by_score(min, max),
            Robj::ZSetListpack(zset) => zset
                .all_pairs()
                .into_iter()
                .filter(|(_, score)| {
                    (*score > min.0 || (min.1 && *score == min.0))
                        && (*score < max.0 || (max.1 && *score == max.0))
                })
                .count(),
            _ => panic!("Robj::zset_count_by_score called on non-zset type"),
        }
    }

    /// Return a lex interval using the same bound markers and reverse ordering
    /// as the Skiplist-backed implementation.
    pub fn zset_range_by_lex(
        &self,
        min: &[u8],
        min_inclusive: bool,
        max: &[u8],
        max_inclusive: bool,
        rev: bool,
    ) -> Vec<(Bytes, f64)> {
        match self {
            Robj::ZSetSkiplist(z) => z.range_by_lex(min, min_inclusive, max, max_inclusive, rev),
            Robj::ZSetListpack(_) => {
                let mut entries = self
                    .zset_entries()
                    .into_iter()
                    .filter(|(member, _)| {
                        let meets_min = min == b"-"
                            || (min != b"+"
                                && if min_inclusive {
                                    member.as_ref() >= min
                                } else {
                                    member.as_ref() > min
                                });
                        let meets_max = max == b"+"
                            || (max != b"-"
                                && if max_inclusive {
                                    member.as_ref() <= max
                                } else {
                                    member.as_ref() < max
                                });
                        meets_min && meets_max
                    })
                    .collect::<Vec<_>>();
                if rev {
                    entries.reverse();
                }
                entries
            }
            _ => panic!("Robj::zset_range_by_lex called on non-zset type"),
        }
    }

    pub fn zset_count_by_lex(
        &self,
        min: &[u8],
        min_inclusive: bool,
        max: &[u8],
        max_inclusive: bool,
    ) -> usize {
        match self {
            Robj::ZSetSkiplist(zset) => zset.count_by_lex(min, min_inclusive, max, max_inclusive),
            Robj::ZSetListpack(zset) => zset
                .all_pairs()
                .into_iter()
                .filter(|(member, _)| {
                    let meets_min = min == b"-"
                        || (min != b"+"
                            && if min_inclusive {
                                member.as_slice() >= min
                            } else {
                                member.as_slice() > min
                            });
                    let meets_max = max == b"+"
                        || (max != b"-"
                            && if max_inclusive {
                                member.as_slice() <= max
                            } else {
                                member.as_slice() < max
                            });
                    meets_min && meets_max
                })
                .count(),
            _ => panic!("Robj::zset_count_by_lex called on non-zset type"),
        }
    }

    /// Pop up to `count` members from the low-score end.
    pub fn zset_pop_min(&mut self, count: usize) -> Vec<(Bytes, f64)> {
        match self {
            Robj::ZSetSkiplist(z) => z.pop_min(count),
            Robj::ZSetListpack(_) => {
                let entries = self
                    .zset_entries()
                    .into_iter()
                    .take(count)
                    .collect::<Vec<_>>();
                for (member, _) in &entries {
                    self.zset_remove(member);
                }
                entries
            }
            _ => panic!("Robj::zset_pop_min called on non-zset type"),
        }
    }

    /// Pop up to `count` members from the high-score end.
    pub fn zset_pop_max(&mut self, count: usize) -> Vec<(Bytes, f64)> {
        match self {
            Robj::ZSetSkiplist(z) => z.pop_max(count),
            Robj::ZSetListpack(_) => {
                let mut entries = self.zset_entries();
                entries.reverse();
                entries.truncate(count);
                for (member, _) in &entries {
                    self.zset_remove(member);
                }
                entries
            }
            _ => panic!("Robj::zset_pop_max called on non-zset type"),
        }
    }

    pub fn zset_remove_range_by_rank(&mut self, start: i64, end: i64) -> usize {
        match self {
            Robj::ZSetSkiplist(zset) => {
                let len = ZSetOps::len(zset) as i64;
                if len == 0 {
                    return 0;
                }
                let start = if start < 0 { len + start } else { start }.max(0);
                let end = if end < 0 { len + end } else { end }.min(len - 1);
                if start > end {
                    0
                } else {
                    zset.remove_range_by_rank(start as usize, end as usize)
                }
            }
            Robj::ZSetListpack(_) => {
                let members = self.zset_range_by_rank(start, end, false);
                for (member, _) in &members {
                    self.zset_remove(member);
                }
                members.len()
            }
            _ => panic!("Robj::zset_remove_range_by_rank called on non-zset type"),
        }
    }

    pub fn zset_remove_range_by_score(&mut self, min: (f64, bool), max: (f64, bool)) -> usize {
        match self {
            Robj::ZSetSkiplist(zset) => zset.remove_range_by_score(min, max),
            Robj::ZSetListpack(_) => {
                let members = self.zset_range_by_score(min, max, 0, -1, false);
                for (member, _) in &members {
                    self.zset_remove(member);
                }
                members.len()
            }
            _ => panic!("Robj::zset_remove_range_by_score called on non-zset type"),
        }
    }

    pub fn zset_remove_range_by_lex(
        &mut self,
        min: &[u8],
        min_inclusive: bool,
        max: &[u8],
        max_inclusive: bool,
    ) -> usize {
        match self {
            Robj::ZSetSkiplist(zset) => {
                zset.remove_range_by_lex(min, min_inclusive, max, max_inclusive)
            }
            Robj::ZSetListpack(_) => {
                let members = self.zset_range_by_lex(min, min_inclusive, max, max_inclusive, false);
                for (member, _) in &members {
                    self.zset_remove(member);
                }
                members.len()
            }
            _ => panic!("Robj::zset_remove_range_by_lex called on non-zset type"),
        }
    }

    // ---- Stream typed methods ----

    pub fn stream_len(&self) -> usize {
        match self {
            Robj::StreamListpack(s) => StreamOps::len(s.as_ref()),
            _ => panic!("Robj::stream_len called on non-stream type"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::io::store::structure::set::SetEncoding;

    #[test]
    fn string_constructors_choose_expected_encoding() {
        assert_eq!(Robj::string("42").encoding_name(), "int");
        assert_eq!(Robj::string("hello").encoding_name(), "embstr");
        assert_eq!(Robj::string("x".repeat(116)).encoding_name(), "embstr");
        assert_eq!(Robj::string("x".repeat(117)).encoding_name(), "raw");
        assert_eq!(Robj::string_raw("42").encoding_name(), "raw");
    }

    #[test]
    fn string_accessors_are_empty_for_non_strings() {
        let string = Robj::string("value");
        let list = Robj::ListQuicklist(ListValue::new());

        assert!(string.is_string());
        assert_eq!(string.as_bytes(), b"value");
        assert!(!list.is_string());
        assert_eq!(list.as_bytes(), b"");
    }

    #[test]
    fn empty_rdb_key_keeps_empty_streams_as_valid_objects() {
        assert!(!Robj::StreamListpack(Box::default()).is_empty_rdb_key());
        assert!(Robj::ListQuicklist(ListValue::new()).is_empty_rdb_key());
    }

    #[test]
    fn object_type_and_encoding_names_cover_all_variants() {
        let mut set = ValkeySet::new();
        set.encoding = SetEncoding::Hashtable;
        let variants = [
            (Robj::string("v"), "string", "embstr"),
            (Robj::ListQuicklist(ListValue::new()), "list", "quicklist"),
            (Robj::HashHashtable(HashHt::new()), "hash", "hashtable"),
            (Robj::SetHashtable(set), "set", "hashtable"),
            (Robj::ZSetSkiplist(ZSetSl::new()), "zset", "btree"),
            (Robj::StreamListpack(Box::default()), "stream", "stream"),
        ];

        for (obj, type_name, encoding) in variants {
            assert_eq!(obj.type_name(), type_name);
            assert_eq!(obj.encoding_name(), encoding);
        }
    }

    #[test]
    fn type_and_encoding_methods_return_correct_enum_values() {
        assert_eq!(Robj::string("42").type_(), Type::String);
        assert_eq!(Robj::string("42").encoding(), Encoding::Int);
        assert_eq!(Robj::string("hello").encoding(), Encoding::Embstr);
        assert_eq!(Robj::string("x".repeat(117)).encoding(), Encoding::Raw);
        assert_eq!(Robj::ListQuicklist(ListValue::new()).type_(), Type::List);

        let set = ValkeySet::from_hashset(Default::default());
        assert_eq!(Robj::SetHashtable(set).type_(), Type::Set);
    }

    #[test]
    fn listpack_objects_are_operable_through_robj() {
        let mut list = Robj::ListListpack(ListLp::new());
        assert_eq!(list.list_push_back(b"first"), 1);
        assert_eq!(list.list_push_front(b"zero"), 2);
        assert_eq!(list.list_get(0), Some(b"zero".to_vec()));
        assert_eq!(list.list_pop_back(), Some(Bytes::from_static(b"first")));

        let mut hash = Robj::HashListpack(HashLp::new());
        assert!(hash.hash_set(b"field", b"value"));
        assert_eq!(hash.hash_get(b"field"), Some(b"value".to_vec()));
        assert!(hash.hash_del(b"field"));

        let mut set = Robj::SetListpack(SetLp::new());
        assert!(set.set_add(b"member", 512));
        assert!(set.set_contains(b"member"));
        assert_eq!(set.set_members(), vec![b"member".to_vec()]);

        let mut zset = Robj::ZSetListpack(ZSetLp::new());
        assert!(zset.zset_add(b"member", 1.5));
        assert_eq!(zset.zset_score(b"member"), Some(1.5));
        assert_eq!(
            zset.zset_entries(),
            vec![(Bytes::from_static(b"member"), 1.5)]
        );
        assert_eq!(zset.zset_range_by_rank(0, -1, false), zset.zset_entries());
    }

    #[test]
    fn zset_listpack_score_ranges_honor_bounds_offset_limit_and_reverse() {
        let mut zset = Robj::ZSetListpack(ZSetLp::new());
        assert!(zset.zset_add(b"a", 1.0));
        assert!(zset.zset_add(b"b", 2.0));
        assert!(zset.zset_add(b"c", 3.0));

        assert_eq!(
            zset.zset_range_by_score((1.0, true), (3.0, false), 1, 1, false),
            vec![(Bytes::from_static(b"b"), 2.0)]
        );
        assert_eq!(
            zset.zset_range_by_score((1.0, false), (3.0, true), 0, -1, true),
            vec![
                (Bytes::from_static(b"c"), 3.0),
                (Bytes::from_static(b"b"), 2.0)
            ]
        );
    }

    #[test]
    fn zset_listpack_lex_ranges_honor_bounds_and_reverse() {
        let mut zset = Robj::ZSetListpack(ZSetLp::new());
        assert!(zset.zset_add(b"a", 1.0));
        assert!(zset.zset_add(b"b", 1.0));
        assert!(zset.zset_add(b"c", 1.0));

        assert_eq!(
            zset.zset_range_by_lex(b"a", false, b"c", true, false),
            vec![
                (Bytes::from_static(b"b"), 1.0),
                (Bytes::from_static(b"c"), 1.0)
            ]
        );
        assert_eq!(
            zset.zset_range_by_lex(b"-", true, b"+", true, true),
            vec![
                (Bytes::from_static(b"c"), 1.0),
                (Bytes::from_static(b"b"), 1.0),
                (Bytes::from_static(b"a"), 1.0),
            ]
        );
    }

    #[test]
    fn zset_listpack_pop_preserves_encoding_and_score_order() {
        let mut zset = Robj::ZSetListpack(ZSetLp::new());
        assert!(zset.zset_add(b"b", 1.0));
        assert!(zset.zset_add(b"a", 1.0));
        assert!(zset.zset_add(b"c", 2.0));
        assert!(zset.zset_add(b"d", 3.0));

        assert_eq!(
            zset.zset_pop_min(2),
            vec![
                (Bytes::from_static(b"a"), 1.0),
                (Bytes::from_static(b"b"), 1.0),
            ]
        );
        assert!(matches!(zset, Robj::ZSetListpack(_)));
        assert_eq!(
            zset.zset_pop_max(2),
            vec![
                (Bytes::from_static(b"d"), 3.0),
                (Bytes::from_static(b"c"), 2.0),
            ]
        );
        assert!(matches!(zset, Robj::ZSetListpack(_)));
        assert!(zset.zset_is_empty());
    }

    #[test]
    fn zset_listpack_range_removals_preserve_encoding() {
        let mut zset = Robj::ZSetListpack(ZSetLp::new());
        assert!(zset.zset_add(b"a", 1.0));
        assert!(zset.zset_add(b"b", 2.0));
        assert!(zset.zset_add(b"c", 3.0));

        assert_eq!(zset.zset_remove_range_by_rank(0, 0), 1);
        assert_eq!(zset.zset_remove_range_by_score((2.0, true), (2.0, true)), 1);
        assert_eq!(zset.zset_remove_range_by_lex(b"c", true, b"c", true), 1);
        assert!(matches!(zset, Robj::ZSetListpack(_)));
        assert!(zset.zset_is_empty());
    }

    #[test]
    fn listpack_objects_upgrade_when_their_encoding_rejects_a_write() {
        let mut list = Robj::ListListpack(ListLp::new());
        for index in 0..512 {
            list.list_push_back(index.to_string().as_bytes());
        }
        assert!(matches!(list, Robj::ListListpack(_)));
        list.list_push_back(b"upgrade");
        assert!(matches!(list, Robj::ListQuicklist(_)));

        let mut hash = Robj::HashListpack(HashLp::new());
        for index in 0..512 {
            let field = format!("f{index}");
            hash.hash_set(field.as_bytes(), b"value");
        }
        hash.hash_set(b"upgrade", b"value");
        assert!(matches!(hash, Robj::HashHashtable(_)));

        let mut set = Robj::SetListpack(SetLp::new());
        for index in 0..128 {
            set.set_add(format!("m{index}").as_bytes(), 512);
        }
        set.set_add(b"upgrade", 512);
        assert!(matches!(set, Robj::SetHashtable(_)));

        let mut zset = Robj::ZSetListpack(ZSetLp::new());
        for index in 0..128 {
            zset.zset_add(format!("m{index}").as_bytes(), index as f64);
        }
        zset.zset_add(b"upgrade", 1000.0);
        assert!(matches!(zset, Robj::ZSetSkiplist(_)));
    }

    #[test]
    fn collection_views_expose_len_empty_and_memory_usage() {
        let mut list = ListValue::new();
        let _ = list.push_back(b"a");
        let obj = Robj::ListQuicklist(list);
        let collection = obj.as_collection().unwrap();

        assert_eq!(collection.type_name(), "list");
        assert_eq!(collection.len(), 1);
        assert!(!collection.is_empty());
        assert!(collection.memory_usage() >= 1);
        assert!(Robj::string("x").as_collection().is_none());
    }

    #[test]
    fn mutable_collection_view_allows_observing_container_after_mutation() {
        let mut obj = Robj::HashHashtable(HashHt::new());
        {
            let collection = obj.as_collection_mut().unwrap();
            assert_eq!(collection.type_name(), "hash");
            assert!(collection.is_empty());
        }
        if let Robj::HashHashtable(hash) = &mut obj {
            hash.insert(Bytes::from_static(b"field"), Bytes::from_static(b"value"));
        }
        assert_eq!(obj.as_collection().unwrap().len(), 1);
    }

    #[test]
    fn estimated_size_accounts_for_strings_and_containers() {
        let string_size = Robj::string("abc").estimated_size();
        let mut zset = ZSetSl::new();
        let _ = ZSetOps::add(&mut zset, b"member", 1.5);
        let zset_size = Robj::ZSetSkiplist(zset).estimated_size();

        assert!(string_size >= 3);
        assert!(zset_size >= "member".len());
    }

    #[test]
    fn estimated_size_does_not_charge_enum_storage_per_string() {
        let payload = vec![b'x'; 1024];
        let size = Robj::string_raw(payload.clone()).estimated_size();

        assert_eq!(size, payload.len() + 16);
    }

    #[test]
    fn string_values_do_not_inline_the_stream_payload_layout() {
        eprintln!(
            "Robj={} ListValue={} HashHt={} ValkeySet={} ZSetSl={} ValkeyStream={} StrInt={} StrEmbstr={} StrRaw={}",
            std::mem::size_of::<Robj>(),
            std::mem::size_of::<ListValue>(),
            std::mem::size_of::<HashHt>(),
            std::mem::size_of::<ValkeySet>(),
            std::mem::size_of::<ZSetSl>(),
            std::mem::size_of::<ValkeyStream>(),
            std::mem::size_of::<StrInt>(),
            std::mem::size_of::<StrEmbstr>(),
            std::mem::size_of::<StrRaw>(),
        );
        assert!(
            std::mem::size_of::<Robj>() <= 40,
            "Robj must stay close to a C object header so short strings do not pay for stream state"
        );
    }

    #[test]
    fn set_typed_methods_work_on_set_hashtable() {
        let mut set = ValkeySet::new();
        set.encoding = SetEncoding::Hashtable;
        let mut obj = Robj::SetHashtable(set);

        assert!(obj.set_add(b"hello", 512));
        assert!(!obj.set_add(b"hello", 512));
        assert!(obj.set_contains(b"hello"));
        assert_eq!(obj.set_len(), 1);
        assert!(!obj.set_is_empty());
        assert!(obj.set_remove(b"hello"));
        assert!(!obj.set_remove(b"hello"));
        assert!(obj.set_is_empty());
    }

    #[test]
    fn set_intset_adds_integers_and_upgrades_on_non_integer() {
        use crate::foundation::io::store::structure::set::IntSet;
        let mut obj = Robj::SetIntset(IntSet::new());

        assert!(obj.set_add(b"42", 5));
        assert!(obj.set_add(b"-7", 5));
        assert!(obj.set_add(b"0", 5));
        assert_eq!(obj.set_len(), 3);
        assert_eq!(obj.encoding_name(), "intset");

        // Adding a non-integer triggers upgrade to hashtable
        assert!(obj.set_add(b"hello", 5));
        assert_eq!(obj.set_len(), 4);
        assert_eq!(obj.encoding_name(), "hashtable");
        assert!(obj.set_contains(b"42"));
        assert!(obj.set_contains(b"hello"));
    }

    #[test]
    fn set_intset_upgrades_when_exceeding_max_entries() {
        use crate::foundation::io::store::structure::set::IntSet;
        let mut obj = Robj::SetIntset(IntSet::new());

        for i in 0..5 {
            assert!(obj.set_add(&i.to_string().into_bytes(), 5));
        }
        assert_eq!(obj.set_len(), 5);
        assert_eq!(obj.encoding_name(), "intset");

        // 6th integer exceeds max_entries=5 → upgrade
        assert!(obj.set_add(b"100", 5));
        assert_eq!(obj.set_len(), 6);
        assert_eq!(obj.encoding_name(), "hashtable");
    }
}
