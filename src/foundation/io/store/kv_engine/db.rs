use crate::foundation::io::store::structure::dict::ResizeBudget;
use crate::foundation::io::store::structure::hash::{HashHt, HashLp};
use crate::foundation::io::store::structure::kvstore::KVStore;
use crate::foundation::io::store::structure::list::ListValue;
use crate::foundation::io::store::structure::set::{SetEncoding, ValkeySet};
use crate::foundation::io::store::structure::stream::ValkeyStream;
use crate::foundation::io::store::structure::string::StoredKey;
use crate::foundation::io::store::structure::zset::{ZSetLp, ZSetSl};
use crate::foundation::io::store::Robj;
use crate::foundation::io::sys::time;
use crate::foundation::memory::metrics::memory_stats::{self, MemoryCategory};
use crate::foundation::memory::value::{EncodingChange, ZSetOps};
use bytes::Bytes;
use indexmap::{IndexMap, IndexSet};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::ffi::c_void;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use super::hash_field_expires::HashFieldExpiry;
use super::{now_ms, EvictionCandidate, EvictionPolicy};
type SlotRouter = fn(&[u8]) -> usize;
const RESIZE_SLOTS_PER_CRON: usize = 16;

fn standalone_slot(_key: &[u8]) -> usize {
    0
}

pub struct Db {
    pub(super) data: KVStore<StoredKey, DbValue>,
    pub(super) expires: KVStore<StoredKey, u64>,
    pub(super) expiry_cursor: u64,
    pub expired_deletions: Vec<Vec<u8>>,
    pub active_expired_deletions: Vec<Vec<u8>>,
    pub(super) hash_field_expires: HashMap<Bytes, HashMap<Vec<u8>, u64>>,
    pub(super) hash_field_expiry_index: BTreeSet<HashFieldExpiry>,
    pub(super) hash_field_expiry_cursor: Option<HashFieldExpiry>,
    hash_string_refs: HashMap<Vec<u8>, HashSet<Vec<u8>>>,
    pub(super) keys_with_volatile_items: usize,
    pub(super) module_values: HashMap<Vec<u8>, (u64, *mut c_void)>,
    pub lazy_expired_hash_fields: u64,
    pub pending_hexpired: IndexSet<(Vec<u8>, Vec<u8>)>,
    pub(super) no_lazy_expire: bool,
    pub(super) pause_skip_expire: bool,
    /// Keys whose TTL was assigned by a local writable-replica client.
    /// Primary-originated expiry remains controlled by the primary.
    pub(super) replica_local_expire_keys: HashSet<Vec<u8>>,
    pub(super) replica_local_expire_only: bool,
    use_lfu: bool,
    lfu_log_factor: u32,
    lfu_decay_time: u16,
    slot_router: SlotRouter,
    pub(super) db_id: u32,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct KeyMeta(u64);

impl KeyMeta {
    const LFU_FLAG: u64 = 1 << 63;
    const LRU_MASK: u64 = 0xffff_ffff;

    fn lru_now() -> Self {
        Self(time::unixtime().max(0) as u64 & Self::LRU_MASK)
    }

    fn lfu_now(freq: u8) -> Self {
        let minutes = (time::mstime().max(0) as u64 / 60_000) & 0xffff;
        Self(Self::LFU_FLAG | (minutes << 8) | u64::from(freq))
    }

    fn lru_raw(self) -> i64 {
        if self.0 & Self::LFU_FLAG != 0 {
            0
        } else {
            ((self.0 & Self::LRU_MASK) as u32) as i64
        }
    }

    fn lfu_freq(self) -> Option<u8> {
        (self.0 & Self::LFU_FLAG != 0).then_some(self.0 as u8)
    }

    fn lfu_access_minutes(self) -> Option<u32> {
        (self.0 & Self::LFU_FLAG != 0).then_some(((self.0 >> 8) & 0xffff) as u32)
    }

    fn decayed_lfu_freq(self, decay_time: u16) -> Option<u8> {
        let freq = self.lfu_freq()?;
        if decay_time == 0 {
            return Some(freq);
        }
        let now = ((time::mstime().max(0) as u64 / 60_000) & 0xffff) as u16;
        let then = ((self.0 >> 8) & 0xffff) as u16;
        let periods = now.wrapping_sub(then) / decay_time;
        Some(freq.saturating_sub(periods.min(u16::from(u8::MAX)) as u8))
    }

    fn is_lfu(self) -> bool {
        self.0 & Self::LFU_FLAG != 0
    }

    fn set_lru_idle(&mut self, seconds: i64) {
        self.0 = time::unixtime().saturating_sub(seconds).max(0) as u64 & Self::LRU_MASK;
    }

    fn set_lfu_freq(&mut self, freq: u8) {
        *self = Self::lfu_now(freq);
    }
}

#[derive(Debug)]
pub(crate) enum SnapshotValue {
    Owned(Robj),
    Shared(Arc<Robj>),
}

impl SnapshotValue {
    pub(crate) fn as_ref(&self) -> &Robj {
        match self {
            Self::Owned(value) => value,
            Self::Shared(value) => value,
        }
    }

    pub(crate) fn is_shared(&self) -> bool {
        matches!(self, Self::Shared(_))
    }

    #[cfg(test)]
    pub(crate) fn collection_storage_ptr(&self) -> *const () {
        match self {
            Self::Owned(value) => collection_storage_ptr(value),
            Self::Shared(value) => collection_storage_ptr(value),
        }
    }
}

#[cfg(test)]
fn collection_storage_ptr(value: &Robj) -> *const () {
    match value {
        Robj::ListQuicklist(list) => list.as_slices().0.as_ptr().cast(),
        _ => std::ptr::null(),
    }
}

#[derive(Clone, Debug)]
pub(super) enum ValueSlot {
    Owned(Robj),
    Shared(Arc<Robj>),
}

impl Deref for ValueSlot {
    type Target = Robj;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Owned(value) => value,
            Self::Shared(value) => value,
        }
    }
}

impl DerefMut for ValueSlot {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Owned(value) => value,
            Self::Shared(value) => Arc::make_mut(value),
        }
    }
}

impl ValueSlot {
    fn snapshot(&mut self, large_value_threshold: usize) -> SnapshotValue {
        if self.estimated_size() <= large_value_threshold {
            return SnapshotValue::Owned((**self).clone());
        }
        if let Self::Owned(value) = self {
            let owned = std::mem::replace(value, Robj::string_raw(Bytes::new()));
            *self = Self::Shared(Arc::new(owned));
        }
        let Self::Shared(value) = self else {
            unreachable!("large snapshot value must be shared");
        };
        SnapshotValue::Shared(Arc::clone(value))
    }

    fn into_owned(self) -> Robj {
        match self {
            Self::Owned(value) => value,
            Self::Shared(value) => Arc::unwrap_or_clone(value),
        }
    }

    fn into_deferred_flush_value(self) -> Option<Robj> {
        match self {
            Self::Owned(value) => Some(value),
            Self::Shared(value) => Arc::into_inner(value),
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct DbValue {
    pub(super) value: ValueSlot,
    pub(super) key_meta: KeyMeta,
}

impl DbValue {
    fn new(value: Robj, use_lfu: bool) -> Self {
        Self {
            value: ValueSlot::Owned(value),
            key_meta: if use_lfu {
                KeyMeta::lfu_now(5)
            } else {
                KeyMeta::lru_now()
            },
        }
    }
}

impl Default for Db {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(clippy::result_unit_err)]
impl Db {
    pub fn set_no_lazy_expire(&mut self, val: bool) {
        self.no_lazy_expire = val;
    }

    pub fn set_pause_skip_expire(&mut self, val: bool) {
        self.pause_skip_expire = val;
    }

    pub fn set_replica_local_expire_only(&mut self, val: bool) {
        self.replica_local_expire_only = val;
    }

    pub fn mark_replica_local_expire(&mut self, key: &[u8]) {
        self.replica_local_expire_keys.insert(key.to_vec());
    }

    pub(crate) fn should_expire_locally(&self, key: &[u8]) -> bool {
        !self.replica_local_expire_only || self.replica_local_expire_keys.contains(key)
    }

    pub fn new() -> Self {
        Self::new_with_slot_router(0, standalone_slot)
    }

    pub fn new_with_slot_router(slot_bits: u32, slot_router: SlotRouter) -> Self {
        Self::new_with_slot_router_and_resize_budget(
            slot_bits,
            slot_router,
            ResizeBudget::new(None),
        )
    }

    pub(super) fn new_with_slot_router_and_resize_budget(
        slot_bits: u32,
        slot_router: SlotRouter,
        resize_budget: ResizeBudget,
    ) -> Self {
        Self {
            data: KVStore::with_resize_budget(slot_bits, resize_budget.clone()),
            expires: KVStore::with_resize_budget(slot_bits, resize_budget),
            expiry_cursor: 0,
            expired_deletions: Vec::new(),
            active_expired_deletions: Vec::new(),
            hash_field_expires: HashMap::new(),
            hash_field_expiry_index: BTreeSet::new(),
            hash_field_expiry_cursor: None,
            hash_string_refs: HashMap::new(),
            keys_with_volatile_items: 0,
            pending_hexpired: IndexSet::new(),
            module_values: HashMap::new(),
            no_lazy_expire: false,
            pause_skip_expire: false,
            replica_local_expire_keys: HashSet::new(),
            replica_local_expire_only: false,
            use_lfu: false,
            lfu_log_factor: 10,
            lfu_decay_time: 1,
            lazy_expired_hash_fields: 0,
            slot_router,
            db_id: 0,
        }
    }

    #[inline]
    pub(super) fn key_slot(&self, key: &[u8]) -> usize {
        (self.slot_router)(key)
    }

    pub(super) fn slot_bits(&self) -> u32 {
        self.data.slot_bits()
    }

    pub(super) fn slot_router(&self) -> SlotRouter {
        self.slot_router
    }

    pub fn set_lfu_mode(&mut self, use_lfu: bool) {
        self.use_lfu = use_lfu;
    }

    pub fn set_eviction_metadata_config(
        &mut self,
        use_lfu: bool,
        lfu_log_factor: u32,
        lfu_decay_time: u16,
    ) {
        self.use_lfu = use_lfu;
        self.lfu_log_factor = lfu_log_factor;
        self.lfu_decay_time = lfu_decay_time;
    }

    pub fn is_expired(&self, key: &[u8]) -> bool {
        let slot = self.key_slot(key);
        match self.expires.get_in_slot(slot, key) {
            Some(&t) => now_ms() >= t,
            None => false,
        }
    }

    pub fn is_expired_at(&self, key: &[u8], now: u64) -> bool {
        let slot = self.key_slot(key);
        match self.expires.get_in_slot(slot, key) {
            Some(&t) => now >= t,
            None => false,
        }
    }

    pub fn get(&mut self, key: &[u8]) -> Option<&Robj> {
        self.get_at(key, now_ms())
    }

    pub fn get_at(&mut self, key: &[u8], now_ms: u64) -> Option<&Robj> {
        crate::foundation::memory::key_access::record_lookup(self.db_id, key);
        if self.is_expired_at(key, now_ms) {
            if !self.no_lazy_expire && !self.pause_skip_expire && self.should_expire_locally(key) {
                self.expired_deletions.push(key.to_vec());
                let _suppression =
                    crate::foundation::memory::key_access::KeyAccessSuppression::enter();
                self.del(key);
            }
            return None;
        }
        let slot = self.key_slot(key);
        self.data.get_in_slot(slot, key).map(|entry| &*entry.value)
    }

    pub fn get_no_expire(&self, key: &[u8]) -> Option<&Robj> {
        crate::foundation::memory::key_access::record_lookup(self.db_id, key);
        if self.is_expired(key) {
            return None;
        }
        let slot = self.key_slot(key);
        self.data.get_in_slot(slot, key).map(|entry| &*entry.value)
    }

    pub fn get_allow_expired(&self, key: &[u8]) -> Option<&Robj> {
        crate::foundation::memory::key_access::record_lookup(self.db_id, key);
        let slot = self.key_slot(key);
        self.data.get_in_slot(slot, key).map(|entry| &*entry.value)
    }

    /// Promote one large value at the handoff boundary. Ordinary values stay
    /// owned by the event-loop store and are copied only into the work item.
    pub(crate) fn snapshot_value_for_background(
        &mut self,
        key: &[u8],
        large_value_threshold: usize,
    ) -> Option<SnapshotValue> {
        let slot = self.key_slot(key);
        self.data
            .get_mut_in_slot(slot, key)
            .map(|entry| entry.value.snapshot(large_value_threshold))
    }

    pub(crate) fn release_background_snapshot(&mut self, key: &[u8]) -> bool {
        let slot = self.key_slot(key);
        let Some(entry) = self.data.get_mut_in_slot(slot, key) else {
            return true;
        };
        let ValueSlot::Shared(shared) = &entry.value else {
            return true;
        };
        if Arc::strong_count(shared) != 1 {
            return false;
        }
        let ValueSlot::Shared(shared) = std::mem::replace(
            &mut entry.value,
            ValueSlot::Owned(Robj::string_raw(Bytes::new())),
        ) else {
            unreachable!("snapshot value was checked as shared");
        };
        entry.value = ValueSlot::Owned(
            Arc::into_inner(shared).expect("sole snapshot owner must unwrap without cloning"),
        );
        true
    }

    pub fn expire_of(&self, key: &[u8]) -> Option<u64> {
        let slot = self.key_slot(key);
        self.expires.get_in_slot(slot, key).copied()
    }

    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut Robj> {
        self.get_mut_at(key, now_ms())
    }

    pub fn get_mut_at(&mut self, key: &[u8], now_ms: u64) -> Option<&mut Robj> {
        crate::foundation::memory::key_access::record_lookup(self.db_id, key);
        if self.is_expired_at(key, now_ms) {
            if !self.no_lazy_expire && !self.pause_skip_expire && self.should_expire_locally(key) {
                self.expired_deletions.push(key.to_vec());
                let _suppression =
                    crate::foundation::memory::key_access::KeyAccessSuppression::enter();
                self.del(key);
            }
            return None;
        }
        let slot = self.key_slot(key);
        self.data
            .get_mut_in_slot(slot, key)
            .map(|entry| &mut *entry.value)
    }

    fn upgrade_compact_encoding_for_command(&mut self, key: &[u8]) {
        let slot = self.key_slot(key);
        let Some(entry) = self.data.get_mut_in_slot(slot, key) else {
            return;
        };
        let obj = &mut *entry.value;
        let replacement = match obj {
            Robj::ListListpack(list) => Some(Robj::ListQuicklist(ListValue::from_vecdeque(
                list.all_elements().into_iter().map(Bytes::from).collect(),
            ))),
            Robj::HashListpack(hash) => {
                let mut converted = HashHt::new();
                for (field, value) in hash.all_pairs() {
                    converted
                        .data
                        .insert(Bytes::from(field), Bytes::from(value));
                }
                Some(Robj::HashHashtable(converted))
            }
            Robj::ZSetListpack(zset) => {
                let mut converted = ZSetSl::new();
                for (member, score) in zset.all_pairs() {
                    let _ = crate::foundation::memory::value::ZSetOps::add(
                        &mut converted,
                        &member,
                        score,
                    );
                }
                Some(Robj::ZSetSkiplist(converted))
            }
            Robj::SetListpack(set) => {
                let mut converted = ValkeySet::new();
                converted.encoding = SetEncoding::Listpack;
                for member in set.all_members() {
                    converted.data.insert(Bytes::from(member), ());
                }
                Some(Robj::SetHashtable(converted))
            }
            Robj::SetIntset(set) => {
                let mut converted = ValkeySet::from_hashset(
                    set.iter()
                        .map(|member| Bytes::from(member.to_string()))
                        .collect(),
                );
                converted.encoding = SetEncoding::Intset;
                Some(Robj::SetHashtable(converted))
            }
            _ => None,
        };
        if let Some(replacement) = replacement {
            *obj = replacement;
        }
    }

    pub fn get_list(&mut self, key: &[u8]) -> Result<Option<&ListValue>, ()> {
        self.upgrade_compact_encoding_for_command(key);
        match self.get(key) {
            Some(Robj::ListQuicklist(v)) => Ok(Some(v)),
            Some(Robj::ListListpack(_)) => Ok(None),
            Some(_) => Err(()),
            None => Ok(None),
        }
    }

    pub fn get_zset(&mut self, key: &[u8]) -> Result<Option<&ZSetSl>, ()> {
        self.upgrade_compact_encoding_for_command(key);
        match self.get(key) {
            Some(Robj::ZSetSkiplist(v)) => Ok(Some(v)),
            Some(Robj::ZSetListpack(_)) => Ok(None),
            Some(_) => Err(()),
            None => Ok(None),
        }
    }

    /// Return a ZSet object without converting listpack storage to Skiplist.
    pub fn get_zset_object(&mut self, key: &[u8]) -> Result<Option<&Robj>, ()> {
        match self.get(key) {
            Some(value @ (Robj::ZSetSkiplist(_) | Robj::ZSetListpack(_))) => Ok(Some(value)),
            Some(_) => Err(()),
            None => Ok(None),
        }
    }

    pub fn get_hash(&mut self, key: &[u8]) -> Result<Option<&IndexMap<Bytes, Bytes>>, ()> {
        self.upgrade_compact_encoding_for_command(key);
        match self.get(key) {
            Some(Robj::HashHashtable(v)) => Ok(Some(v)),
            Some(Robj::HashListpack(_)) => Ok(None),
            Some(_) => Err(()),
            None => Ok(None),
        }
    }

    /// Return a Hash object without forcing its compact encoding to upgrade.
    /// Command paths that support both encodings must use this boundary rather
    /// than the legacy `get_hash` concrete-Hashtable accessor.
    pub fn get_path_hash(
        &mut self,
        key: &[u8],
    ) -> Result<Option<&crate::foundation::io::store::structure::path_hash::PathHash>, ()> {
        match self.get(key) {
            Some(Robj::PathHash(value)) => Ok(Some(value)),
            Some(_) => Err(()),
            None => Ok(None),
        }
    }

    pub fn get_or_create_path_hash(
        &mut self,
        key: &[u8],
    ) -> Result<&mut crate::foundation::io::store::structure::path_hash::PathHash, ()> {
        if self.get_mut(key).is_none() {
            self.set(key.to_vec(), Robj::PathHash(Box::default()));
        }
        match self.get_mut(key) {
            Some(Robj::PathHash(value)) => Ok(value),
            _ => Err(()),
        }
    }

    /// Mutate a path hash in place while preserving TTL and object metadata.
    pub fn mutate_path_hash<T>(
        &mut self,
        key: &[u8],
        create: bool,
        should_read_expired_values: bool,
        operation: impl FnOnce(&mut crate::foundation::io::store::structure::path_hash::PathHash) -> T,
    ) -> Result<Option<T>, ()> {
        let is_missing = if should_read_expired_values {
            self.get_allow_expired(key).is_none()
        } else {
            self.get(key).is_none()
        };
        if is_missing {
            if !create {
                return Ok(None);
            }
            self.set(key.to_vec(), Robj::PathHash(Box::default()));
        }
        let value = if should_read_expired_values {
            let slot = self.key_slot(key);
            // Dereferencing ValueSlot preserves snapshot copy-on-write.
            self.data
                .get_mut_in_slot(slot, key)
                .map(|entry| &mut *entry.value)
        } else {
            self.get_mut(key)
        };
        let (result, old_size, new_size) = match value {
            Some(Robj::PathHash(value)) => {
                // Both reads use cached sizes; only the mutation touches entries.
                let old_size = value.memory_usage();
                let result = operation(value);
                (result, old_size, value.memory_usage())
            }
            _ => return Err(()),
        };
        if new_size >= old_size {
            memory_stats::track_add(MemoryCategory::Dataset, new_size - old_size);
        } else {
            memory_stats::track_sub(MemoryCategory::Dataset, old_size - new_size);
        }
        Ok(Some(result))
    }

    pub fn get_hash_object(&mut self, key: &[u8]) -> Result<Option<&Robj>, ()> {
        match self.get(key) {
            Some(value @ (Robj::HashHashtable(_) | Robj::HashListpack(_))) => Ok(Some(value)),
            Some(_) => Err(()),
            None => Ok(None),
        }
    }

    pub fn get_set(&mut self, key: &[u8]) -> Result<Option<&ValkeySet>, ()> {
        self.upgrade_compact_encoding_for_command(key);
        match self.get(key) {
            Some(Robj::SetHashtable(v)) => Ok(Some(v)),
            Some(Robj::SetListpack(_)) => Ok(None),
            Some(_) => Err(()),
            None => Ok(None),
        }
    }

    pub fn get_string(&mut self, key: &[u8]) -> Result<Option<&[u8]>, ()> {
        match self.get(key) {
            Some(Robj::StrInt(s)) => Ok(Some(s.as_bytes())),
            Some(Robj::StrEmbstr(s)) => Ok(Some(s.as_bytes())),
            Some(Robj::StrRaw(s)) => Ok(Some(s.as_bytes())),
            Some(_) => Err(()),
            None => Ok(None),
        }
    }

    /// Mutate a string without replacing its DB entry or expiration metadata.
    pub fn set_string_range(
        &mut self,
        key: &[u8],
        offset: usize,
        patch: &[u8],
    ) -> Result<usize, ()> {
        if let Some(value) = self.get_mut(key) {
            let old_size = value.estimated_size();
            let len = value.set_string_range(offset, patch).ok_or(())?;
            let new_size = value.estimated_size();
            if new_size > old_size {
                memory_stats::track_add(MemoryCategory::Dataset, new_size - old_size);
            }
            return Ok(len);
        }

        let mut value = Robj::string_raw(Bytes::new());
        let len = value.set_string_range(offset, patch).ok_or(())?;
        if !patch.is_empty() {
            self.set(key.to_vec(), value);
        }
        Ok(len)
    }

    pub fn get_string_allow_expired(&self, key: &[u8]) -> Result<Option<&[u8]>, ()> {
        let slot = self.key_slot(key);
        match self.data.get_in_slot(slot, key).map(|entry| &*entry.value) {
            Some(Robj::StrInt(s)) => Ok(Some(s.as_bytes())),
            Some(Robj::StrEmbstr(s)) => Ok(Some(s.as_bytes())),
            Some(Robj::StrRaw(s)) => Ok(Some(s.as_bytes())),
            Some(_) => Err(()),
            None => Ok(None),
        }
    }

    pub fn get_list_mut(&mut self, key: &[u8]) -> Result<Option<&mut ListValue>, ()> {
        self.upgrade_compact_encoding_for_command(key);
        match self.get_mut(key) {
            Some(Robj::ListQuicklist(v)) => Ok(Some(v)),
            Some(Robj::ListListpack(_)) => Ok(None),
            Some(_) => Err(()),
            None => Ok(None),
        }
    }

    pub fn get_zset_mut(&mut self, key: &[u8]) -> Result<Option<&mut ZSetSl>, ()> {
        self.upgrade_compact_encoding_for_command(key);
        match self.get_mut(key) {
            Some(Robj::ZSetSkiplist(v)) => Ok(Some(v)),
            Some(Robj::ZSetListpack(_)) => Ok(None),
            Some(_) => Err(()),
            None => Ok(None),
        }
    }

    /// Mutable ZSet object counterpart preserving either concrete encoding.
    pub fn get_zset_object_mut(&mut self, key: &[u8]) -> Result<Option<&mut Robj>, ()> {
        match self.get_mut(key) {
            Some(value @ (Robj::ZSetSkiplist(_) | Robj::ZSetListpack(_))) => Ok(Some(value)),
            Some(_) => Err(()),
            None => Ok(None),
        }
    }

    pub fn get_hash_mut(&mut self, key: &[u8]) -> Result<Option<&mut IndexMap<Bytes, Bytes>>, ()> {
        self.upgrade_compact_encoding_for_command(key);
        match self.get_mut(key) {
            Some(Robj::HashHashtable(v)) => Ok(Some(v)),
            Some(Robj::HashListpack(_)) => Ok(None),
            Some(_) => Err(()),
            None => Ok(None),
        }
    }

    /// Mutable Hash object counterpart to `get_hash_object`; it preserves
    /// listpack storage so mutations can apply the caller's explicit limits.
    pub fn get_hash_object_mut(&mut self, key: &[u8]) -> Result<Option<&mut Robj>, ()> {
        match self.get_mut(key) {
            Some(value @ (Robj::HashHashtable(_) | Robj::HashListpack(_))) => Ok(Some(value)),
            Some(_) => Err(()),
            None => Ok(None),
        }
    }

    pub fn get_set_mut(&mut self, key: &[u8]) -> Result<Option<&mut ValkeySet>, ()> {
        self.upgrade_compact_encoding_for_command(key);
        match self.get_mut(key) {
            Some(Robj::SetHashtable(v)) => Ok(Some(v)),
            Some(Robj::SetListpack(_)) => Ok(None),
            Some(_) => Err(()),
            None => Ok(None),
        }
    }

    pub fn get_stream(&mut self, key: &[u8]) -> Result<Option<&ValkeyStream>, ()> {
        match self.get(key) {
            Some(Robj::StreamListpack(v)) => Ok(Some(v)),
            Some(_) => Err(()),
            None => Ok(None),
        }
    }

    pub fn get_stream_mut(&mut self, key: &[u8]) -> Result<Option<&mut ValkeyStream>, ()> {
        match self.get_mut(key) {
            Some(Robj::StreamListpack(v)) => Ok(Some(v)),
            Some(_) => Err(()),
            None => Ok(None),
        }
    }

    pub fn get_or_create_list(&mut self, key: &[u8]) -> Result<&mut ListValue, ()> {
        let slot = self.key_slot(key);
        let use_lfu = self.use_lfu;
        let Db {
            ref mut data,
            ref mut expires,
            ..
        } = self;
        if !data.contains_key_in_slot(slot, key) {
            expires.remove_in_slot(slot, key);
            data.insert_in_slot(
                slot,
                StoredKey::from(key),
                DbValue::new(Robj::ListQuicklist(ListValue::new()), use_lfu),
            );
            return Ok(match &mut *data.get_mut_in_slot(slot, key).unwrap().value {
                Robj::ListQuicklist(v) => v,
                _ => unreachable!(),
            });
        }
        match &mut *data.get_mut_in_slot(slot, key).unwrap().value {
            Robj::ListQuicklist(v) => Ok(v),
            _ => Err(()),
        }
    }

    pub fn get_or_create_zset(&mut self, key: &[u8]) -> Result<&mut ZSetSl, ()> {
        let slot = self.key_slot(key);
        let use_lfu = self.use_lfu;
        let Db {
            ref mut data,
            ref mut expires,
            ..
        } = self;
        if !data.contains_key_in_slot(slot, key) {
            expires.remove_in_slot(slot, key);
            data.insert_in_slot(
                slot,
                StoredKey::from(key),
                DbValue::new(Robj::ZSetSkiplist(ZSetSl::new()), use_lfu),
            );
            return Ok(match &mut *data.get_mut_in_slot(slot, key).unwrap().value {
                Robj::ZSetSkiplist(v) => v,
                _ => unreachable!(),
            });
        }
        match &mut *data.get_mut_in_slot(slot, key).unwrap().value {
            Robj::ZSetSkiplist(v) => Ok(v),
            _ => Err(()),
        }
    }

    pub fn get_or_create_hash(&mut self, key: &[u8]) -> Result<&mut IndexMap<Bytes, Bytes>, ()> {
        let slot = self.key_slot(key);
        let use_lfu = self.use_lfu;
        let Db {
            ref mut data,
            ref mut expires,
            ..
        } = self;
        if !data.contains_key_in_slot(slot, key) {
            expires.remove_in_slot(slot, key);
            data.insert_in_slot(
                slot,
                StoredKey::from(key),
                DbValue::new(Robj::HashHashtable(HashHt::new()), use_lfu),
            );
            return Ok(match &mut *data.get_mut_in_slot(slot, key).unwrap().value {
                Robj::HashHashtable(v) => v,
                _ => unreachable!(),
            });
        }
        match &mut *data.get_mut_in_slot(slot, key).unwrap().value {
            Robj::HashHashtable(v) => Ok(v),
            _ => Err(()),
        }
    }

    /// Set a hash field while retaining the listpack representation until the
    /// caller-supplied C-compatible limits require a one-way upgrade.
    pub fn hash_set_with_limits(
        &mut self,
        key: &[u8],
        field: &[u8],
        value: &[u8],
        max_entries: usize,
        max_value_size: usize,
    ) -> Result<bool, ()> {
        // A write treats an expired key as absent before inspecting its raw
        // storage entry, matching the other get-or-create paths.
        let _ = self.get_mut(key);
        let slot = self.key_slot(key);
        if !self.data.contains_key_in_slot(slot, key) {
            self.expires.remove_in_slot(slot, key);
            self.data.insert_in_slot(
                slot,
                StoredKey::from(key),
                DbValue::new(Robj::HashListpack(HashLp::new()), self.use_lfu),
            );
        }

        let object = &mut self
            .data
            .get_mut_in_slot(slot, key)
            .expect("hash object was inserted before mutation")
            .value;
        let object = &mut **object;
        match object {
            Robj::HashHashtable(hash) => Ok(hash
                .data
                .insert(Bytes::copy_from_slice(field), Bytes::copy_from_slice(value))
                .is_none()),
            Robj::HashListpack(hash) => {
                match hash.set_with_threshold(field, value, max_entries, max_value_size) {
                    Ok(added) => Ok(added),
                    Err(EncodingChange::Upgrade) => {
                        let old = std::mem::replace(object, Robj::HashListpack(HashLp::new()));
                        let Robj::HashListpack(listpack) = old else {
                            unreachable!("hash listpack upgrade replaced the wrong object type");
                        };
                        let mut converted = HashHt::new();
                        for (old_field, old_value) in listpack.all_pairs() {
                            converted
                                .data
                                .insert(Bytes::from(old_field), Bytes::from(old_value));
                        }
                        let added = converted
                            .data
                            .insert(Bytes::copy_from_slice(field), Bytes::copy_from_slice(value))
                            .is_none();
                        *object = Robj::HashHashtable(converted);
                        Ok(added)
                    }
                    Err(EncodingChange::Downgrade) => {
                        unreachable!("hash listpack does not downgrade during mutation")
                    }
                }
            }
            _ => Err(()),
        }
    }

    /// Preconvert for a whole input batch, including duplicate fields.
    pub fn hash_prepare_pairs_with_limits(
        &mut self,
        key: &[u8],
        pairs: &[(Bytes, Bytes)],
        max_entries: usize,
        max_value_size: usize,
    ) -> Result<(), ()> {
        let should_convert = pairs.len() > max_entries
            || pairs
                .iter()
                .any(|(field, value)| field.len() > max_value_size || value.len() > max_value_size);
        let _ = self.get_mut(key);
        if should_convert {
            if self.get_hash_object(key)?.is_some() {
                self.get_hash_mut(key)?;
            } else {
                self.get_or_create_hash(key)?;
            }
        } else {
            self.get_hash_object(key)?;
        }
        Ok(())
    }

    /// Add a ZSet member while retaining listpack storage until the caller's
    /// C-compatible entry or member-size limit requires a one-way upgrade.
    pub fn zset_add_with_limits(
        &mut self,
        key: &[u8],
        member: &[u8],
        score: f64,
        max_entries: usize,
        max_value_size: usize,
    ) -> Result<bool, ()> {
        let _ = self.get_mut(key);
        let slot = self.key_slot(key);
        if !self.data.contains_key_in_slot(slot, key) {
            self.expires.remove_in_slot(slot, key);
            self.data.insert_in_slot(
                slot,
                StoredKey::from(key),
                DbValue::new(Robj::ZSetListpack(ZSetLp::new()), self.use_lfu),
            );
        }

        let object = &mut self
            .data
            .get_mut_in_slot(slot, key)
            .expect("zset object was inserted before mutation")
            .value;
        let object = &mut **object;
        match object {
            Robj::ZSetSkiplist(zset) => ZSetOps::add(zset, member, score).map_err(|_| ()),
            Robj::ZSetListpack(zset) => {
                match zset.add_with_threshold(member, score, max_entries, max_value_size) {
                    Ok(added) => Ok(added),
                    Err(EncodingChange::Upgrade) => {
                        let old = std::mem::replace(object, Robj::ZSetListpack(ZSetLp::new()));
                        let Robj::ZSetListpack(listpack) = old else {
                            unreachable!("zset listpack upgrade replaced the wrong object type");
                        };
                        let mut converted = ZSetSl::new();
                        for (old_member, old_score) in listpack.all_pairs() {
                            ZSetOps::add(&mut converted, &old_member, old_score)
                                .expect("skiplist add never fails");
                        }
                        let added = ZSetOps::add(&mut converted, member, score)
                            .expect("skiplist add never fails");
                        *object = Robj::ZSetSkiplist(converted);
                        Ok(added)
                    }
                    Err(EncodingChange::Downgrade) => {
                        unreachable!("zset listpack does not downgrade during mutation")
                    }
                }
            }
            _ => Err(()),
        }
    }

    pub fn get_or_create_set(&mut self, key: &[u8]) -> Result<&mut ValkeySet, ()> {
        // A write treats an expired key as absent. Run the normal lazy-expire
        // path before consulting the raw table so SADD recreates the set and
        // clears its old TTL; replicas still honor replica_local_expire_only.
        let _ = self.get_mut(key);
        self.upgrade_compact_encoding_for_command(key);
        let slot = self.key_slot(key);
        let use_lfu = self.use_lfu;
        let Db {
            ref mut data,
            ref mut expires,
            ..
        } = self;
        if !data.contains_key_in_slot(slot, key) {
            expires.remove_in_slot(slot, key);
            data.insert_in_slot(
                slot,
                StoredKey::from(key),
                DbValue::new(Robj::SetHashtable(ValkeySet::new()), use_lfu),
            );
            return Ok(match &mut *data.get_mut_in_slot(slot, key).unwrap().value {
                Robj::SetHashtable(v) => v,
                _ => unreachable!(),
            });
        }
        match &mut *data.get_mut_in_slot(slot, key).unwrap().value {
            Robj::SetHashtable(v) => Ok(v),
            _ => Err(()),
        }
    }

    pub fn get_or_create_stream(&mut self, key: &[u8]) -> Result<&mut ValkeyStream, ()> {
        let slot = self.key_slot(key);
        let use_lfu = self.use_lfu;
        let Db {
            ref mut data,
            ref mut expires,
            ..
        } = self;
        if !data.contains_key_in_slot(slot, key) {
            expires.remove_in_slot(slot, key);
            data.insert_in_slot(
                slot,
                StoredKey::from(key),
                DbValue::new(Robj::StreamListpack(Box::default()), use_lfu),
            );
            return Ok(match &mut *data.get_mut_in_slot(slot, key).unwrap().value {
                Robj::StreamListpack(v) => v,
                _ => unreachable!(),
            });
        }
        match &mut *data.get_mut_in_slot(slot, key).unwrap().value {
            Robj::StreamListpack(v) => Ok(v),
            _ => Err(()),
        }
    }

    fn downgrade_embstr_for_key(key_len: usize, val: Robj) -> Robj {
        if let Robj::StrEmbstr(e) = val {
            if !crate::foundation::io::store::structure::string::encoding::fits_embstr_with_key(
                key_len,
                e.len(),
            ) {
                let raw = crate::foundation::io::store::structure::string::StrRaw(e.into_bytes());
                return Robj::StrRaw(raw);
            }
            return Robj::StrEmbstr(e);
        }
        val
    }

    pub fn set(&mut self, key: Vec<u8>, val: Robj) {
        let slot = self.key_slot(&key);
        let key_len = key.len();
        let val = Self::downgrade_embstr_for_key(key_len, val);
        let val_size = val.estimated_size();

        if let Some(old) = self.data.remove_in_slot(slot, key.as_slice()) {
            let old_key_total = key_len + old.value.estimated_size();
            memory_stats::track_sub(MemoryCategory::Dataset, old_key_total);
        }
        self.expires.remove_in_slot(slot, key.as_slice());
        self.replica_local_expire_keys.remove(&key);
        self.clear_hash_field_expires(&key);
        self.data
            .insert_in_slot(slot, StoredKey::from(key), DbValue::new(val, self.use_lfu));
        memory_stats::track_add(MemoryCategory::Dataset, key_len + val_size);
    }

    pub fn set_keep_ttl(&mut self, key: Vec<u8>, val: Robj) {
        let slot = self.key_slot(&key);
        let key_len = key.len();
        let val = Self::downgrade_embstr_for_key(key_len, val);
        let val_size = val.estimated_size();

        if let Some(old) = self.data.remove_in_slot(slot, key.as_slice()) {
            let old_key_total = key_len + old.value.estimated_size();
            memory_stats::track_sub(MemoryCategory::Dataset, old_key_total);
        }
        self.clear_hash_field_expires(&key);

        self.data
            .insert_in_slot(slot, StoredKey::from(key), DbValue::new(val, self.use_lfu));
        memory_stats::track_add(MemoryCategory::Dataset, key_len + val_size);
    }

    pub fn load_entry(&mut self, key: Vec<u8>, val: Robj, expire: Option<u64>) {
        self.load_entry_with_hash_field_expires(key, val, expire, HashMap::new());
    }

    pub fn load_entry_with_hash_field_expires(
        &mut self,
        key: Vec<u8>,
        val: Robj,
        expire: Option<u64>,
        hash_field_expires: HashMap<Vec<u8>, u64>,
    ) {
        let slot = self.key_slot(&key);
        let key_len = key.len();
        self.expires.remove_in_slot(slot, key.as_slice());
        self.data.insert_in_slot(
            slot,
            StoredKey::from(key.clone()),
            DbValue::new(val, self.use_lfu),
        );
        self.set_hash_field_expires_map(key.clone(), hash_field_expires);
        let val_size = self
            .data
            .get_in_slot(slot, key.as_slice())
            .expect("loaded value was inserted before size accounting")
            .value
            .estimated_size();
        if let Some(t) = expire {
            self.expires.insert_in_slot(slot, StoredKey::from(key), t);
        }
        memory_stats::track_add(MemoryCategory::Dataset, key_len + val_size);
    }

    pub fn exists(&mut self, key: &[u8]) -> bool {
        self.exists_at(key, now_ms())
    }

    pub fn exists_at(&mut self, key: &[u8], now_ms: u64) -> bool {
        crate::foundation::memory::key_access::record_lookup(self.db_id, key);
        if self.is_expired_at(key, now_ms) {
            if !self.no_lazy_expire && !self.pause_skip_expire && self.should_expire_locally(key) {
                self.expired_deletions.push(key.to_vec());
                let _suppression =
                    crate::foundation::memory::key_access::KeyAccessSuppression::enter();
                self.del(key);
            }
            return false;
        }
        let slot = self.key_slot(key);
        self.data.contains_key_in_slot(slot, key)
    }

    pub fn exists_no_expire(&self, key: &[u8]) -> bool {
        crate::foundation::memory::key_access::record_lookup(self.db_id, key);
        if self.is_expired(key) {
            return false;
        }
        let slot = self.key_slot(key);
        self.data.contains_key_in_slot(slot, key)
    }

    pub fn keys(&self) -> Vec<Vec<u8>> {
        self.data
            .keys()
            .map(|key| key.as_bytes().to_vec())
            .collect()
    }

    /// Return one dictionary scan bucket. Callers can retain the cursor and
    /// process one bounded bucket per event-loop turn.
    pub(crate) fn snapshot_scan_bucket(&self, cursor: u64) -> (u64, Vec<Vec<u8>>) {
        let mut keys = Vec::new();
        let next = self.data.scan(cursor, |key, _| {
            keys.push(key.as_bytes().to_vec());
        });
        (next, keys)
    }

    pub(crate) fn hash_field_expires_for(&self, key: &[u8]) -> Option<&HashMap<Vec<u8>, u64>> {
        self.hash_field_expires.get(key)
    }

    pub fn random_eviction_key(&self) -> Option<Vec<u8>> {
        self.data.random_entry().map(|(k, _)| k.as_bytes().to_vec())
    }

    pub fn random_eviction_expire_key(&self) -> Option<Vec<u8>> {
        self.expires
            .random_entry()
            .map(|(k, _)| k.as_bytes().to_vec())
    }

    pub fn eviction_score(&self, key: &[u8], policy: EvictionPolicy) -> u64 {
        let slot = self.key_slot(key);
        match policy {
            EvictionPolicy::AllkeysLru | EvictionPolicy::VolatileLru => {
                self.get_lru_idle(key).max(0) as u64
            }
            EvictionPolicy::AllkeysLfu | EvictionPolicy::VolatileLfu => self
                .data
                .get_in_slot(slot, key)
                .and_then(|entry| entry.key_meta.decayed_lfu_freq(self.lfu_decay_time))
                .map_or(0, |freq| u64::from(u8::MAX - freq)),
            EvictionPolicy::VolatileTtl => self
                .expires
                .get_in_slot(slot, key)
                .map_or(0, |expire| u64::MAX.saturating_sub(*expire)),
            EvictionPolicy::AllkeysRandom | EvictionPolicy::VolatileRandom => 0,
            EvictionPolicy::NoEviction => 0,
        }
    }

    pub fn sample_eviction_candidate(
        &self,
        policy: EvictionPolicy,
        samples: usize,
    ) -> Option<EvictionCandidate> {
        if policy == EvictionPolicy::NoEviction {
            return None;
        }
        let mut best: Option<EvictionCandidate> = None;
        for _ in 0..samples.max(1) {
            let key = if policy.is_volatile() {
                self.random_eviction_expire_key()
            } else {
                self.random_eviction_key()
            }?;
            let score = self.eviction_score(&key, policy);
            if policy.is_random() {
                return Some(EvictionCandidate { key, score });
            }
            if best
                .as_ref()
                .is_none_or(|candidate| score > candidate.score)
            {
                best = Some(EvictionCandidate { key, score });
            }
        }
        best
    }

    pub fn keys_in_slot(&self, slot: usize) -> Vec<Vec<u8>> {
        let mut keys = Vec::new();
        self.data.for_each_in_slot(slot, |key, _| {
            if !self.is_expired(key) {
                keys.push(key.as_bytes().to_vec());
            }
        });
        keys.sort_unstable();
        keys
    }

    pub fn get_keys_in_slot(&self, slot: usize, count: usize) -> Vec<Vec<u8>> {
        let mut keys = Vec::new();
        self.data.for_each_in_slot(slot, |key, _| {
            if !self.is_expired(key) {
                keys.push(key.as_bytes().to_vec());
            }
        });
        keys.sort_unstable();
        keys.truncate(count);
        keys
    }

    pub fn take_slot(
        &mut self,
        slot: usize,
    ) -> Vec<(Vec<u8>, Robj, Option<u64>, HashMap<Vec<u8>, u64>)> {
        let mut keys = Vec::new();
        self.data
            .for_each_in_slot(slot, |key, _| keys.push(key.as_bytes().to_vec()));
        keys.into_iter()
            .filter_map(|key| {
                self.take(&key)
                    .map(|(value, expire, hfe)| (key, value, expire, hfe))
            })
            .collect()
    }

    /// Detach every value while resetting the database's event-loop-owned
    /// metadata. Callers that need C's async FLUSH semantics can hand the
    /// returned values to an L5 lazyfree worker after this borrow ends.
    pub fn drain_values_for_lazyfree(&mut self) -> Vec<Robj> {
        let values = self
            .data
            .drain()
            .into_iter()
            .filter_map(|(key, val)| {
                let key_total = key.len() + val.value.estimated_size();
                memory_stats::track_sub(MemoryCategory::Dataset, key_total);
                val.value.into_deferred_flush_value()
            })
            .collect();
        for _key in self.expires.keys() {}
        self.expires.clear();
        self.replica_local_expire_keys.clear();
        self.expired_deletions.clear();
        self.active_expired_deletions.clear();
        self.hash_field_expires.clear();
        self.hash_field_expiry_index.clear();
        self.hash_field_expiry_cursor = None;
        self.hash_string_refs.clear();
        self.keys_with_volatile_items = 0;
        self.module_values.clear();
        values
    }

    pub fn flush(&mut self) {
        drop(self.drain_values_for_lazyfree());
    }

    pub fn del(&mut self, key: &[u8]) -> bool {
        let slot = self.key_slot(key);
        self.replica_local_expire_keys.remove(key);
        self.expires.remove_in_slot(slot, key);
        self.clear_hash_field_expires(key);
        self.hash_string_refs.remove(key);
        self.module_values.remove(key);
        let removed = self.data.remove_in_slot(slot, key);
        if let Some(val) = removed {
            crate::foundation::memory::key_access::record_delete(self.db_id, key);
            let key_total = key.len() + val.value.estimated_size();
            memory_stats::track_sub(MemoryCategory::Dataset, key_total);
            true
        } else {
            false
        }
    }

    pub fn set_expire(&mut self, key: &[u8], abs_ms: u64) {
        let slot = self.key_slot(key);
        self.expires
            .insert_in_slot(slot, StoredKey::from(key), abs_ms);
    }

    pub fn get_expire(&self, key: &[u8]) -> Option<u64> {
        let slot = self.key_slot(key);
        self.expires.get_in_slot(slot, key).copied()
    }

    pub fn get_lru_idle(&self, key: &[u8]) -> i64 {
        self.data
            .get_in_slot(self.key_slot(key), key)
            .map(|entry| {
                if entry.key_meta.is_lfu() {
                    return 0;
                }
                let raw = entry.key_meta.lru_raw();
                (time::unixtime() - raw).max(0)
            })
            .unwrap_or(0)
    }

    pub fn get_lru_raw(&self, key: &[u8]) -> i64 {
        self.data
            .get_in_slot(self.key_slot(key), key)
            .map(|entry| entry.key_meta.lru_raw())
            .unwrap_or(0)
    }

    pub fn set_lru_idle(&mut self, key: &[u8], seconds: i64) {
        if let Some(entry) = self.data.get_mut_in_slot(self.key_slot(key), key) {
            entry.key_meta.set_lru_idle(seconds);
        }
    }

    pub fn get_lfu_freq(&self, key: &[u8]) -> u8 {
        self.data
            .get_in_slot(self.key_slot(key), key)
            .and_then(|entry| entry.key_meta.decayed_lfu_freq(self.lfu_decay_time))
            .unwrap_or(0)
    }

    pub fn set_lfu_freq(&mut self, key: &[u8], freq: u8) {
        if let Some(entry) = self.data.get_mut_in_slot(self.key_slot(key), key) {
            entry.key_meta.set_lfu_freq(freq);
        }
    }

    pub fn get_lfu_access_minutes(&self, key: &[u8]) -> Option<u32> {
        self.data
            .get_in_slot(self.key_slot(key), key)
            .and_then(|entry| entry.key_meta.lfu_access_minutes())
    }

    pub fn initialize_lfu_freq(&mut self, key: &[u8], initial_freq: u8) {
        let slot = self.key_slot(key);
        if !self.data.contains_key_in_slot(slot, key) && !self.module_values.contains_key(key) {
            return;
        }
        if let Some(entry) = self.data.get_mut_in_slot(slot, key) {
            if entry.key_meta.lfu_freq().is_none() {
                entry.key_meta.set_lfu_freq(initial_freq);
            }
        }
    }

    pub fn touch_access_time(&mut self, key: &[u8]) -> bool {
        let exists = {
            let _suppression = crate::foundation::memory::key_access::KeyAccessSuppression::enter();
            self.exists(key)
        };
        if !exists {
            return false;
        }
        if let Some(entry) = self.data.get_mut_in_slot(self.key_slot(key), key) {
            if self.use_lfu {
                let freq = entry
                    .key_meta
                    .decayed_lfu_freq(self.lfu_decay_time)
                    .unwrap_or(5);
                let base = u32::from(freq.saturating_sub(5));
                let denominator =
                    base.saturating_mul(self.lfu_log_factor).saturating_add(1) as usize;
                if crate::foundation::io::sys::rand::random_usize(denominator) == 0 {
                    entry.key_meta.set_lfu_freq(freq.saturating_add(1));
                } else {
                    entry.key_meta.set_lfu_freq(freq);
                }
            } else {
                entry.key_meta = KeyMeta::lru_now();
            }
        }
        true
    }

    pub fn persist(&mut self, key: &[u8]) -> bool {
        if !self.exists(key) {
            return false;
        }
        let slot = self.key_slot(key);
        self.expires.remove_in_slot(slot, key).is_some()
    }

    pub fn expiretime_ms(&mut self, key: &[u8]) -> i64 {
        if !self.exists(key) {
            return -2;
        }
        let slot = self.key_slot(key);
        match self.expires.get_in_slot(slot, key) {
            Some(&t) => t as i64,
            None => -1,
        }
    }

    pub fn ttl_ms(&mut self, key: &[u8]) -> i64 {
        self.ttl_ms_at(key, now_ms())
    }

    pub fn ttl_ms_at(&mut self, key: &[u8], now_ms: u64) -> i64 {
        if !self.exists_at(key, now_ms) {
            return -2;
        }
        let slot = self.key_slot(key);
        match self.expires.get_in_slot(slot, key) {
            Some(&t) => {
                if t > now_ms {
                    (t - now_ms) as i64
                } else {
                    0
                }
            }
            None => -1,
        }
    }

    pub fn set_hash_string_ref(&mut self, key: &[u8], field: &[u8], value: &[u8]) -> bool {
        if self.hash_field_expired(key, field)
            || !matches!(self.get_hash_object(key), Ok(Some(hash)) if hash.hash_contains(field))
        {
            return false;
        }
        let updated = match self.get_hash_mut(key) {
            Ok(Some(hash)) => {
                hash.insert(Bytes::copy_from_slice(field), Bytes::copy_from_slice(value));
                true
            }
            Ok(None) | Err(()) => false,
        };
        if updated {
            self.hash_string_refs
                .entry(key.to_vec())
                .or_default()
                .insert(field.to_vec());
        }
        updated
    }

    pub fn hash_has_string_ref(&self, key: &[u8], field: &[u8]) -> bool {
        !self.hash_field_expired(key, field)
            && matches!(self.get_no_expire(key), Some(hash @ (Robj::HashHashtable(_) | Robj::HashListpack(_))) if hash.hash_contains(field))
            && self
                .hash_string_refs
                .get(key)
                .is_some_and(|fields| fields.contains(field))
    }

    /// String references require physical hashtable storage, even without TTLs.
    pub fn hash_key_has_string_refs(&self, key: &[u8]) -> bool {
        self.hash_string_refs
            .get(key)
            .is_some_and(|fields| !fields.is_empty())
    }

    pub fn clear_hash_string_ref(&mut self, key: &[u8], field: &[u8]) {
        let should_remove_key = self.hash_string_refs.get_mut(key).is_some_and(|fields| {
            fields.remove(field);
            fields.is_empty()
        });
        if should_remove_key {
            self.hash_string_refs.remove(key);
        }
    }

    pub fn take(&mut self, key: &[u8]) -> Option<(Robj, Option<u64>, HashMap<Vec<u8>, u64>)> {
        let slot = self.key_slot(key);
        if self.is_expired(key) && !self.no_lazy_expire && !self.pause_skip_expire {
            self.expired_deletions.push(key.to_vec());
            let _suppression = crate::foundation::memory::key_access::KeyAccessSuppression::enter();
            self.del(key);
            return None;
        }
        let val = self.data.remove_in_slot(slot, key)?;
        crate::foundation::memory::key_access::record_delete(self.db_id, key);
        let expire = self.expires.remove_in_slot(slot, key);
        self.replica_local_expire_keys.remove(key);
        let hfe = self.take_hash_field_expires(key);
        self.hash_string_refs.remove(key);
        self.module_values.remove(key);
        let key_total = key.len() + val.value.estimated_size();
        memory_stats::track_sub(MemoryCategory::Dataset, key_total);
        Some((val.value.into_owned(), expire, hfe))
    }

    pub fn clone_entry(&self, key: &[u8]) -> Option<(Robj, Option<u64>, HashMap<Vec<u8>, u64>)> {
        if self.is_expired(key) {
            return None;
        }
        self.clone_entry_allow_expired(key)
    }

    pub fn clone_entry_allow_expired(
        &self,
        key: &[u8],
    ) -> Option<(Robj, Option<u64>, HashMap<Vec<u8>, u64>)> {
        let slot = self.key_slot(key);
        let val = (*self.data.get_in_slot(slot, key)?.value).clone();
        let expire = self.expires.get_in_slot(slot, key).copied();
        let hfe = self
            .hash_field_expires
            .get(key)
            .cloned()
            .unwrap_or_default();
        Some((val, expire, hfe))
    }

    /// Relocate an unexpired value while retaining all per-key metadata stored
    /// outside `DbValue` (TTL, hash-field expiry, replica-local expiry, and
    /// module side tables). Used only by the incremental active defrag pass.
    pub fn relocate_entry(&mut self, key: &[u8]) -> bool {
        if self.is_expired(key) {
            return false;
        }
        let slot = self.key_slot(key);
        let Some(entry) = self.data.get_mut_in_slot(slot, key) else {
            return false;
        };
        entry.value = ValueSlot::Owned(entry.value.relocated_clone());
        true
    }

    pub fn dbsize(&self) -> usize {
        self.data.len()
    }

    pub fn next_expire_at_ms(&self) -> Option<u64> {
        self.expires.iter().map(|(_, deadline)| *deadline).min()
    }

    pub fn set_slot_importing(&mut self, slot: usize, is_importing: bool) {
        if is_importing {
            self.data.set_slot_importing(slot);
            self.expires.set_slot_importing(slot);
        } else {
            self.data.unset_slot_importing(slot);
            self.expires.unset_slot_importing(slot);
        }
    }

    pub fn clear_importing_slots(&mut self) {
        self.data.clear_importing_slots();
        self.expires.clear_importing_slots();
    }

    pub fn data_bucket_count(&self) -> usize {
        self.data.bucket_count()
    }

    pub fn data_stats_msg(&self, full: bool) -> String {
        self.data.stats_msg(full)
    }

    pub fn expires_stats_msg(&self, full: bool) -> String {
        self.expires.stats_msg(full)
    }

    pub fn rehashing_count(&self) -> usize {
        self.data.rehashing_count() + self.expires.rehashing_count()
    }

    pub fn rehashing_bucket_count(&self) -> usize {
        self.data.rehashing_bucket_count() + self.expires.rehashing_bucket_count()
    }

    pub fn set_data_resize_policy(
        &mut self,
        policy: crate::foundation::io::store::structure::dict::ResizePolicy,
    ) {
        self.data.set_resize_policy(policy);
        self.expires.set_resize_policy(policy);
    }

    pub fn set_resize_budget(&mut self, budget: Option<usize>) {
        self.data.set_resize_budget(budget);
    }

    pub fn try_resize(&mut self, active_rehash: bool) {
        self.data
            .try_resize_with_active_rehash(RESIZE_SLOTS_PER_CRON, active_rehash);
        self.expires
            .try_resize_with_active_rehash(RESIZE_SLOTS_PER_CRON, active_rehash);
    }

    pub fn expires_count(&self) -> usize {
        self.expires.len()
    }

    pub fn keys_with_volatile_items_count(&self) -> usize {
        self.keys_with_volatile_items
    }

    pub fn expires_iter(&self) -> impl Iterator<Item = (&[u8], &u64)> {
        self.expires
            .iter()
            .map(|(key, value)| (key.as_bytes(), value))
    }

    /// Returns true if `key` belongs to a slot that is currently being imported.
    /// Used by eviction/expiration to avoid touching keys owned by a migration
    /// stream (mirrors C's `keyIsInImportingSlot`).
    pub fn is_importing_slot_key(&self, key: &[u8]) -> bool {
        self.data.is_importing_slot(self.key_slot(key))
    }

    pub fn scan_expiry<F>(&self, cursor: u64, mut callback: F) -> u64
    where
        F: FnMut(&[u8], &u64),
    {
        if !self.expires.is_sharded() {
            return self
                .expires
                .scan(cursor, |key, value| callback(key.as_bytes(), value));
        }

        let last_slot = self.expires.slot_count().saturating_sub(1);
        self.expires.scan_range(
            cursor,
            0,
            last_slot,
            |_slot, dict| {
                let buckets = dict.bucket_count();
                buckets > 0 && (dict.len() * 100 / buckets) < 1
            },
            |key, value| callback(key.as_bytes(), value),
        )
    }

    pub fn first_key(&self) -> Option<Vec<u8>> {
        self.data.iter().next().map(|(k, _)| k.as_bytes().to_vec())
    }

    pub fn for_each_entry<F: FnMut(&[u8], &Robj, Option<u64>)>(&self, mut f: F) {
        self.for_each_entry_with_hash_field_expires(|key, val, expire, _| f(key, val, expire));
    }

    pub fn for_each_entry_with_hash_field_expires<
        F: FnMut(&[u8], &Robj, Option<u64>, Option<&HashMap<Vec<u8>, u64>>),
    >(
        &self,
        f: F,
    ) {
        use super::hash_field_expires::hfe_for_each_entry;
        hfe_for_each_entry(
            &self.data,
            &self.expires,
            &self.hash_field_expires,
            |k| self.key_slot(k),
            self.no_lazy_expire,
            self.pause_skip_expire,
            f,
        );
    }

    /// Traverse every physically present entry without applying expiration.
    /// Atomic slot migration snapshots use the fork-time dictionary as their
    /// source of truth, matching C `rewriteSlotToAppendOnlyFileRio()`. The
    /// absolute TTL is emitted with the value so the source's later deletion
    /// remains authoritative for the hidden import dataset.
    pub(crate) fn for_each_entry_including_expired_with_hash_field_expires<
        F: FnMut(&[u8], &Robj, Option<u64>, Option<&HashMap<Vec<u8>, u64>>),
    >(
        &self,
        mut f: F,
    ) {
        for (key, value) in self.data.iter() {
            let key = key.as_bytes();
            let expire = self.expires.get_in_slot(self.key_slot(key), key).copied();
            f(key, &value.value, expire, self.hash_field_expires.get(key));
        }
    }

    pub fn for_each_entry_including_expired<F: FnMut(&[u8], &Robj, Option<u64>)>(&self, mut f: F) {
        self.for_each_entry_including_expired_with_hash_field_expires(|k, v, e, _| f(k, v, e));
    }
}
