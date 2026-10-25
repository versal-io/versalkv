//! P1b-0b

use std::borrow::Borrow;
use std::collections::{BTreeSet, HashSet};
use std::hash::Hash;

use super::super::dict::{Dict, ResizeBudget, ResizePolicy};

pub struct KVStore<K, V> {
    slots: Vec<Option<Box<Dict<K, V>>>>,
    slot_bits: u32,
    total_len: usize,
    importing_slots: HashSet<usize>,
    importing_len: usize,
    resize_policy: ResizePolicy,
    resize_budget: ResizeBudget,
    allocated_slots: BTreeSet<usize>,
    resize_cursor: usize,
}

impl<K: Eq + Hash, V> KVStore<K, V> {
    pub fn new(slot_bits: u32) -> Self {
        Self::with_resize_budget(slot_bits, ResizeBudget::new(None))
    }

    pub(crate) fn with_resize_budget(slot_bits: u32, resize_budget: ResizeBudget) -> Self {
        let slot_count = 1usize << slot_bits;
        let mut slots = Vec::with_capacity(slot_count);
        slots.resize_with(slot_count, || None);
        Self {
            slots,
            slot_bits,
            total_len: 0,
            importing_slots: HashSet::new(),
            importing_len: 0,
            resize_policy: ResizePolicy::Allow,
            resize_budget,
            allocated_slots: BTreeSet::new(),
            resize_cursor: 0,
        }
    }

    /// `key_hash_slot`
    #[inline]
    fn slot_of(&self) -> usize {
        0
    }

    fn slot_dict_mut(&mut self, slot: usize) -> &mut Dict<K, V> {
        if self.slots[slot].is_none() {
            let mut dict = Dict::with_resize_budget(self.resize_budget.clone());
            dict.set_resize_policy(self.resize_policy);
            self.slots[slot] = Some(Box::new(dict));
            self.allocated_slots.insert(slot);
        }
        self.slots[slot].as_mut().unwrap()
    }

    pub fn len(&self) -> usize {
        self.total_len.saturating_sub(self.importing_len)
    }

    pub fn importing_len(&self) -> usize {
        self.importing_len
    }

    pub fn is_importing_slot(&self, slot: usize) -> bool {
        self.importing_slots.contains(&slot)
    }

    /// Mark a slot as importing. Keys added to/removed from this slot are tracked
    /// in `importing_len` and excluded from `len()`/random/scan/iteration.
    pub fn set_slot_importing(&mut self, slot: usize) {
        if slot < self.slots.len() && self.importing_slots.insert(slot) {
            let slot_keys = self
                .slots
                .get(slot)
                .and_then(|s| s.as_ref())
                .map(|dict| dict.len())
                .unwrap_or(0);
            self.importing_len += slot_keys;
        }
    }

    pub fn unset_slot_importing(&mut self, slot: usize) {
        if self.importing_slots.remove(&slot) {
            let slot_keys = self
                .slots
                .get(slot)
                .and_then(|s| s.as_ref())
                .map(|dict| dict.len())
                .unwrap_or(0);
            self.importing_len = self.importing_len.saturating_sub(slot_keys);
        }
    }

    pub fn clear_importing_slots(&mut self) {
        self.importing_slots.clear();
        self.importing_len = 0;
    }

    pub fn slot_count(&self) -> usize {
        self.slots.len()
    }

    pub fn slot_bits(&self) -> u32 {
        self.slot_bits
    }

    pub fn is_sharded(&self) -> bool {
        self.slot_bits > 0
    }

    pub fn slot_len(&self, slot: usize) -> usize {
        self.slots
            .get(slot)
            .and_then(|s| s.as_ref())
            .map(|dict| dict.len())
            .unwrap_or(0)
    }

    /// Raw hashtable size for a slot, ignoring the importing flag. Mirrors C's
    /// `kvstoreHashtableSize` used by `countKeysInSlot` so importing keys are
    /// counted by COUNTKEYSINSLOT even though they are excluded from DBSIZE.
    pub fn hashtable_slot_len(&self, slot: usize) -> usize {
        self.slots
            .get(slot)
            .and_then(|s| s.as_ref())
            .map(|dict| dict.len())
            .unwrap_or(0)
    }

    /// Iterate a slot's raw hashtable regardless of the importing flag. Used by
    /// `Db::take_slot` so importing keys can be moved into the import dataset.
    pub fn for_each_in_slot<F>(&self, slot: usize, mut f: F)
    where
        F: FnMut(&K, &V),
    {
        if let Some(dict) = self.slots.get(slot).and_then(|s| s.as_ref()) {
            let mut cursor = 0;
            loop {
                cursor = dict.scan(cursor, &mut f);
                if cursor == 0 {
                    break;
                }
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.total_len == 0
    }

    pub fn bucket_count(&self) -> usize {
        self.slots
            .iter()
            .flatten()
            .map(|dict| dict.bucket_count())
            .sum()
    }

    pub fn stats_msg(&self, full: bool) -> String {
        let mut out = String::new();
        for dict in self.slots.iter().flatten() {
            out.push_str(&dict.stats_msg(full));
        }
        out
    }

    pub fn rehashing_count(&self) -> usize {
        self.slots
            .iter()
            .flatten()
            .filter(|dict| dict.is_rehashing())
            .count()
    }

    pub fn rehashing_bucket_count(&self) -> usize {
        self.slots
            .iter()
            .flatten()
            .map(|dict| dict.rehashing_bucket_count())
            .sum()
    }

    pub fn get<Q: Hash + Eq + ?Sized>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
    {
        let slot = self.slot_of();
        self.get_in_slot(slot, key)
    }

    pub fn get_in_slot<Q: Hash + Eq + ?Sized>(&self, slot: usize, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
    {
        self.slots
            .get(slot)
            .and_then(|s| s.as_ref())
            .and_then(|dict| dict.get(key))
    }

    pub fn get_mut<Q: Hash + Eq + ?Sized>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
    {
        let slot = self.slot_of();
        self.get_mut_in_slot(slot, key)
    }

    pub fn get_mut_in_slot<Q: Hash + Eq + ?Sized>(&mut self, slot: usize, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
    {
        self.slots
            .get_mut(slot)
            .and_then(|s| s.as_mut())
            .and_then(|dict| dict.get_mut(key))
    }

    pub fn contains_key<Q: Hash + Eq + ?Sized>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
    {
        self.get(key).is_some()
    }

    pub fn contains_key_in_slot<Q: Hash + Eq + ?Sized>(&self, slot: usize, key: &Q) -> bool
    where
        K: Borrow<Q>,
    {
        self.get_in_slot(slot, key).is_some()
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let slot = self.slot_of();
        self.insert_in_slot(slot, key, value)
    }

    pub fn insert_in_slot(&mut self, slot: usize, key: K, value: V) -> Option<V> {
        let prev = self.slot_dict_mut(slot).insert(key, value);
        if prev.is_none() {
            self.total_len += 1;
            if self.importing_slots.contains(&slot) {
                self.importing_len += 1;
            }
        }
        prev
    }

    pub fn remove<Q: Hash + Eq + ?Sized>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
    {
        let slot = self.slot_of();
        self.remove_in_slot(slot, key)
    }

    pub fn remove_in_slot<Q: Hash + Eq + ?Sized>(&mut self, slot: usize, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
    {
        let slot_entry = self.slots.get_mut(slot)?;
        let dict = slot_entry.as_mut()?;
        let removed = dict.remove(key);
        if removed.is_some() {
            self.total_len -= 1;
            if self.importing_slots.contains(&slot) {
                self.importing_len = self.importing_len.saturating_sub(1);
            }
            if self.slot_bits > 0 && dict.is_empty() {
                *slot_entry = None;
                self.allocated_slots.remove(&slot);
            }
        }
        removed
    }

    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.iter().map(|(k, _)| k)
    }

    /// Return a pseudo-random (key, value) pair, skipping importing slots.
    /// O(1) amortized; used by eviction / RANDOMKEY so the event loop is not
    /// blocked by `keys().collect()` on large datasets (mirrors C's
    /// `kvstoreGetFairRandomHashtableIndex` + `kvstoreHashtableRandomEntry`).
    pub fn random_entry(&self) -> Option<(&K, &V)> {
        if self.is_empty() {
            return None;
        }
        let n = self.slots.len();
        if n == 0 {
            return None;
        }
        // Try a bounded number of random slots first.
        let tries = n.min(16);
        for _ in 0..tries {
            let slot = fast_random_idx(n);
            if self.importing_slots.contains(&slot) {
                continue;
            }
            if let Some(dict) = self.slots.get(slot).and_then(|s| s.as_ref()) {
                if let Some(pair) = dict.random_entry() {
                    return Some(pair);
                }
            }
        }
        // Fallback: scan for the first non-empty non-importing slot.
        for slot in 0..n {
            if self.importing_slots.contains(&slot) {
                continue;
            }
            if let Some(dict) = self.slots.get(slot).and_then(|s| s.as_ref()) {
                if let Some(pair) = dict.random_entry() {
                    return Some(pair);
                }
            }
        }
        None
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.slots
            .iter()
            .enumerate()
            .filter(|(slot, s)| !self.importing_slots.contains(slot) && s.is_some())
            .flat_map(|(_, s)| s.as_ref().unwrap().iter())
    }

    pub fn clear(&mut self) {
        for slot in self.slots.iter_mut() {
            *slot = None;
        }
        self.total_len = 0;
        self.importing_len = 0;
        self.allocated_slots.clear();
        self.resize_cursor = 0;
    }

    pub fn drain(&mut self) -> Vec<(K, V)> {
        let mut out = Vec::with_capacity(self.total_len);
        for slot in self.slots.iter_mut() {
            if let Some(mut dict) = slot.take() {
                out.extend(dict.drain());
            }
        }
        self.total_len = 0;
        self.importing_len = 0;
        self.allocated_slots.clear();
        self.resize_cursor = 0;
        out
    }

    /// Returns whether the policy changed and therefore had to be propagated
    /// to already allocated slot dictionaries.
    pub fn set_resize_policy(&mut self, policy: ResizePolicy) -> bool {
        if self.resize_policy == policy {
            return false;
        }
        self.resize_policy = policy;
        for dict in self.slots.iter_mut().flatten() {
            dict.set_resize_policy(policy);
        }
        true
    }

    pub fn set_resize_budget(&mut self, budget: Option<usize>) {
        self.resize_budget.reset(budget);
    }

    /// Check at most `limit` allocated slot dictionaries with a persistent
    /// cursor. The index avoids scanning thousands of unallocated slots.
    pub fn try_resize(&mut self, limit: usize) {
        self.try_resize_with_active_rehash(limit, true);
    }

    pub fn try_resize_with_active_rehash(&mut self, limit: usize, active_rehash: bool) {
        let visits = limit.min(self.allocated_slots.len());
        for _ in 0..visits {
            let slot = self
                .allocated_slots
                .range(self.resize_cursor..)
                .next()
                .copied()
                .or_else(|| self.allocated_slots.first().copied())
                .expect("an allocated slot index must reference a dictionary");
            self.slots[slot]
                .as_mut()
                .expect("allocated slot index must remain synchronized")
                .try_resize_with_active_rehash(active_rehash);
            self.resize_cursor = (slot + 1) % self.slots.len();
        }
    }

    pub fn scan<F>(&self, cursor: u64, mut f: F) -> u64
    where
        F: FnMut(&K, &V),
    {
        let last_slot = self.slots.len().saturating_sub(1);
        self.scan_range(
            cursor,
            0,
            last_slot,
            |_, _| false,
            |key, val| {
                f(key, val);
            },
        )
    }

    pub fn scan_range<S, F>(
        &self,
        cursor: u64,
        first_slot: usize,
        last_slot: usize,
        mut should_skip: S,
        mut f: F,
    ) -> u64
    where
        S: FnMut(usize, &Dict<K, V>) -> bool,
        F: FnMut(&K, &V),
    {
        if self.slots.is_empty() || first_slot >= self.slots.len() {
            return 0;
        }
        let last_slot = last_slot.min(self.slots.len() - 1);
        if first_slot > last_slot {
            return 0;
        }

        let slot_mask: u64 = (1u64 << self.slot_bits) - 1;
        let decoded_slot = (cursor & slot_mask) as usize;
        let mut slot_idx = if cursor == 0 {
            first_slot
        } else {
            decoded_slot.max(first_slot)
        };
        let mut dict_cursor = cursor >> self.slot_bits;

        loop {
            if let Some(dict) = self.slots.get(slot_idx).and_then(|s| s.as_ref()) {
                if !self.importing_slots.contains(&slot_idx) && !should_skip(slot_idx, dict) {
                    dict_cursor = dict.scan(dict_cursor, &mut f);
                    if dict_cursor != 0 {
                        return (dict_cursor << self.slot_bits) | slot_idx as u64;
                    }
                    if slot_idx >= last_slot {
                        return 0;
                    }
                    let next = (slot_idx + 1..=last_slot).find(|&idx| {
                        self.slots
                            .get(idx)
                            .and_then(|s| s.as_ref())
                            .is_some_and(|d| !d.is_empty())
                    });
                    return match next {
                        Some(idx) => idx as u64,
                        None => 0,
                    };
                }
            }
            slot_idx += 1;
            dict_cursor = 0;
            if slot_idx > last_slot {
                return 0;
            }
        }
    }
}

fn fast_random_idx(bound: usize) -> usize {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let entropy = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let r = entropy
        .wrapping_add(COUNTER.fetch_add(1, Ordering::Relaxed))
        .wrapping_mul(0x9e3779b97f4a7c15)
        .rotate_left(17);
    (r % bound as u64) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    fn standalone() -> KVStore<Vec<u8>, u64> {
        KVStore::new(0)
    }

    #[test]
    fn empty_cluster_slot_directory_uses_at_most_one_pointer_per_slot() {
        let kv: KVStore<Vec<u8>, u64> = KVStore::new(14);
        let directory_bytes = std::mem::size_of_val(kv.slots.as_slice());
        let pointer_directory_bytes = kv.slot_count() * std::mem::size_of::<usize>();

        assert!(
            directory_bytes <= pointer_directory_bytes,
            "empty slot directory uses {directory_bytes} bytes, expected at most {pointer_directory_bytes}"
        );
        assert!(kv.allocated_slots.is_empty());
        assert_eq!(kv.bucket_count(), 0);
    }

    #[test]
    fn insert_get_remove_roundtrip() {
        let mut kv = standalone();
        assert_eq!(kv.len(), 0);
        assert!(kv.is_empty());

        assert_eq!(kv.insert(b"a".to_vec(), 1), None);
        assert_eq!(kv.insert(b"b".to_vec(), 2), None);
        assert_eq!(kv.insert(b"a".to_vec(), 3), Some(1)); //
        assert_eq!(kv.len(), 2);

        assert_eq!(kv.get(b"a".as_slice()), Some(&3));
        assert_eq!(kv.get(b"missing".as_slice()), None);
        assert!(kv.contains_key(b"b".as_slice()));

        assert_eq!(kv.remove(b"a".as_slice()), Some(3));
        assert_eq!(kv.remove(b"a".as_slice()), None);
        assert_eq!(kv.len(), 1);
    }

    #[test]
    fn get_mut_updates_in_place() {
        let mut kv = standalone();
        kv.insert(b"k".to_vec(), 10);
        *kv.get_mut(b"k".as_slice()).unwrap() += 5;
        assert_eq!(kv.get(b"k".as_slice()), Some(&15));
    }

    #[test]
    fn keys_and_iter_cover_all_entries() {
        let mut kv = standalone();
        for i in 0..8u64 {
            kv.insert(vec![i as u8], i);
        }
        let mut seen: Vec<u64> = kv.iter().map(|(_, v)| *v).collect();
        seen.sort_unstable();
        assert_eq!(seen, (0..8).collect::<Vec<_>>());
        assert_eq!(kv.keys().count(), 8);
    }

    #[test]
    fn drain_empties_store_and_returns_entries() {
        let mut kv = standalone();
        for i in 0..5u64 {
            kv.insert(vec![i as u8], i);
        }
        let drained = kv.drain();
        assert_eq!(drained.len(), 5);
        assert_eq!(kv.len(), 0);
        assert!(kv.is_empty());
        kv.insert(b"x".to_vec(), 99);
        assert_eq!(kv.get(b"x".as_slice()), Some(&99));
    }

    #[test]
    fn clear_resets_len() {
        let mut kv = standalone();
        kv.insert(b"a".to_vec(), 1);
        kv.clear();
        assert_eq!(kv.len(), 0);
        assert_eq!(kv.get(b"a".as_slice()), None);
    }

    #[test]
    fn scan_visits_every_key_and_terminates() {
        let mut kv = standalone();
        for i in 0..64u64 {
            kv.insert(vec![i as u8], i);
        }
        let mut collected: Vec<u64> = Vec::new();
        let mut cursor = 0u64;
        let mut guard = 0;
        loop {
            cursor = kv.scan(cursor, |_, v| collected.push(*v));
            guard += 1;
            assert!(guard < 10_000, "scan should terminate");
            if cursor == 0 {
                break;
            }
        }
        collected.sort_unstable();
        collected.dedup();
        assert_eq!(collected, (0..64).collect::<Vec<_>>());
    }

    #[test]
    fn scan_matches_flat_dict_cursor_semantics() {
        let mut kv = standalone();
        let mut dict: Dict<Vec<u8>, u64> = Dict::new();
        for i in 0..40u64 {
            kv.insert(vec![i as u8], i);
            dict.insert(vec![i as u8], i);
        }
        let mut c_kv = 0u64;
        let mut c_dict = 0u64;
        loop {
            c_kv = kv.scan(c_kv, |_, _| {});
            c_dict = dict.scan(c_dict, |_, _| {});
            assert_eq!(c_kv, c_dict, "cursor must match flat Dict");
            if c_kv == 0 {
                break;
            }
        }
    }

    #[test]
    fn resize_policy_survives_lazy_allocation() {
        let mut kv = standalone();
        kv.set_resize_policy(ResizePolicy::Avoid);
        for i in 0..100u64 {
            kv.insert(vec![i as u8], i);
        }
        assert_eq!(kv.len(), 100);
        assert_eq!(kv.get(vec![7u8].as_slice()), Some(&7));
        kv.set_resize_policy(ResizePolicy::Allow);
        assert_eq!(kv.get(vec![42u8].as_slice()), Some(&42));
    }

    #[test]
    fn unchanged_resize_policy_does_not_reapply_to_slot_dicts() {
        let mut kv = KVStore::new(2);
        kv.insert_in_slot(0, b"zero".to_vec(), 0);
        kv.insert_in_slot(3, b"three".to_vec(), 3);

        assert!(!kv.set_resize_policy(ResizePolicy::Allow));
        assert!(kv.set_resize_policy(ResizePolicy::Avoid));
        assert!(!kv.set_resize_policy(ResizePolicy::Avoid));
    }

    #[test]
    fn lazy_slot_dicts_consume_one_shared_resize_budget() {
        let expansion_bytes = 2 * std::mem::size_of::<usize>();
        let budget = ResizeBudget::new(Some(expansion_bytes));
        let mut kv = KVStore::with_resize_budget(1, budget);
        for index in 0..7u8 {
            kv.insert_in_slot(0, vec![index], index);
            kv.insert_in_slot(1, vec![index], index);
        }

        kv.insert_in_slot(0, vec![7], 7);
        kv.insert_in_slot(1, vec![7], 7);

        assert_eq!(kv.rehashing_count(), 1);
        assert_eq!(kv.bucket_count(), 3);
    }

    #[test]
    fn explicit_slot_routes_same_key_to_independent_dicts() {
        let mut kv = KVStore::new(2);

        assert_eq!(kv.insert_in_slot(1, b"k".to_vec(), 10), None);
        assert_eq!(kv.insert_in_slot(2, b"k".to_vec(), 20), None);

        assert_eq!(kv.len(), 2);
        assert_eq!(kv.slot_len(1), 1);
        assert_eq!(kv.slot_len(2), 1);
        assert_eq!(kv.get_in_slot(1, b"k".as_slice()), Some(&10));
        assert_eq!(kv.get_in_slot(2, b"k".as_slice()), Some(&20));

        assert_eq!(kv.remove_in_slot(1, b"k".as_slice()), Some(10));
        assert_eq!(kv.get_in_slot(1, b"k".as_slice()), None);
        assert_eq!(kv.get_in_slot(2, b"k".as_slice()), Some(&20));
        assert_eq!(kv.len(), 1);
    }

    #[test]
    fn scan_range_limits_slots_and_can_skip_a_slot_dict() {
        let mut kv = KVStore::new(2);
        kv.insert_in_slot(0, b"zero".to_vec(), 0);
        kv.insert_in_slot(1, b"one".to_vec(), 1);
        kv.insert_in_slot(2, b"two".to_vec(), 2);
        kv.insert_in_slot(3, b"three".to_vec(), 3);

        let mut values = Vec::new();
        let mut cursor = 0;
        let mut guard = 0;
        loop {
            cursor = kv.scan_range(
                cursor,
                1,
                2,
                |slot, _dict| slot == 1,
                |_, v| {
                    values.push(*v);
                },
            );
            guard += 1;
            assert!(guard < 10_000, "scan_range should terminate");
            if cursor == 0 {
                break;
            }
        }

        assert_eq!(cursor, 0);
        values.sort_unstable();
        values.dedup();
        assert_eq!(values, vec![2]);
    }

    #[test]
    fn rehashing_count_aggregates_slot_dicts() {
        let mut kv = KVStore::new(2);
        assert_eq!(kv.rehashing_count(), 0);

        for index in 0..8u64 {
            kv.insert_in_slot(1, format!("k{index}").into_bytes(), index);
        }

        assert_eq!(kv.rehashing_count(), 1);
        assert_eq!(kv.rehashing_bucket_count(), 1);
    }

    #[test]
    fn resize_cron_advances_a_bounded_slot_window() {
        let mut kv: KVStore<Vec<u8>, u64> = KVStore::new(4);
        kv.insert_in_slot(1, b"one".to_vec(), 1);
        kv.insert_in_slot(3, b"three".to_vec(), 3);

        kv.try_resize(1);
        assert_eq!(kv.resize_cursor, 2);

        kv.try_resize(1);
        assert_eq!(kv.resize_cursor, 4);
    }

    #[test]
    fn importing_slot_excluded_from_len_iter_scan() {
        let mut kv: KVStore<Vec<u8>, u64> = KVStore::new(2); // 4 slots
        for slot in 0..4 {
            for i in 0..5 {
                kv.insert_in_slot(slot, format!("s{slot}-{i}").into_bytes(), i);
            }
        }
        assert_eq!(kv.len(), 20);

        kv.set_slot_importing(1);
        assert_eq!(kv.len(), 15);
        assert_eq!(kv.importing_len(), 5);
        assert!(kv.is_importing_slot(1));

        // iter excludes importing slot
        let iter_keys = kv.keys().cloned().collect::<Vec<_>>();
        assert!(iter_keys.iter().all(|k| !k.starts_with(b"s1-")));
        assert_eq!(iter_keys.len(), 15);

        // scan excludes importing slot
        let mut scanned = Vec::new();
        let mut cursor = 0u64;
        loop {
            cursor = kv.scan(cursor, |k, _| scanned.push(k.clone()));
            if cursor == 0 {
                break;
            }
        }
        assert!(scanned.iter().all(|k| !k.starts_with(b"s1-")));
        assert_eq!(scanned.len(), 15);

        // add a key into the importing slot; importing_len grows but len stays flat
        kv.insert_in_slot(1, b"s1-extra".to_vec(), 99);
        assert_eq!(kv.importing_len(), 6);
        assert_eq!(kv.len(), 15);

        kv.unset_slot_importing(1);
        assert_eq!(kv.importing_len(), 0);
        assert_eq!(kv.len(), 21);
        assert_eq!(kv.keys().count(), 21);
    }
}
