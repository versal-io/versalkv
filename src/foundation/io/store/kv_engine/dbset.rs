use super::db::Db;
use crate::foundation::io::store::structure::dict::ResizeBudget;
use crate::foundation::io::store::Robj;
use std::collections::HashMap;

pub struct SlotEntry {
    pub db_id: u32,
    pub key: Vec<u8>,
    pub value: Robj,
    pub expire: Option<u64>,
    pub hash_field_expires: HashMap<Vec<u8>, u64>,
}

pub struct DbSet {
    dbs: Vec<Db>,
    resize_budget: ResizeBudget,
}

impl DbSet {
    pub fn new(num_dbs: u32) -> Self {
        Self::new_with_slot_router(num_dbs, 0, |_key| 0)
    }

    pub fn new_with_slot_router(
        num_dbs: u32,
        slot_bits: u32,
        slot_router: fn(&[u8]) -> usize,
    ) -> Self {
        let resize_budget = ResizeBudget::new(None);
        let mut dbs = Vec::with_capacity(num_dbs as usize);
        for _ in 0..num_dbs {
            dbs.push(Db::new_with_slot_router_and_resize_budget(
                slot_bits,
                slot_router,
                resize_budget.clone(),
            ));
        }
        Self { dbs, resize_budget }
    }

    pub fn num_dbs(&self) -> usize {
        self.dbs.len()
    }

    pub fn empty_like(&self) -> Self {
        match self.dbs.first() {
            Some(db) => {
                Self::new_with_slot_router(self.dbs.len() as u32, db.slot_bits(), db.slot_router())
            }
            None => Self {
                dbs: Vec::new(),
                resize_budget: ResizeBudget::new(None),
            },
        }
    }

    pub fn get(&self, idx: u32) -> Option<&Db> {
        self.dbs.get(idx as usize)
    }

    pub fn get_mut(&mut self, idx: u32) -> Option<&mut Db> {
        self.dbs.get_mut(idx as usize)
    }

    pub fn iter_dbs(&self) -> impl Iterator<Item = (usize, &Db)> {
        self.dbs.iter().enumerate()
    }

    pub fn set_lfu_mode(&mut self, use_lfu: bool) {
        for db in &mut self.dbs {
            db.set_lfu_mode(use_lfu);
        }
    }

    pub fn set_eviction_metadata_config(
        &mut self,
        use_lfu: bool,
        lfu_log_factor: u32,
        lfu_decay_time: u16,
    ) {
        for db in &mut self.dbs {
            db.set_eviction_metadata_config(use_lfu, lfu_log_factor, lfu_decay_time);
        }
    }

    pub fn db_size(&self, idx: u32) -> usize {
        self.dbs.get(idx as usize).map(|d| d.dbsize()).unwrap_or(0)
    }

    pub fn data_bucket_count(&self, idx: u32) -> usize {
        self.dbs
            .get(idx as usize)
            .map(|d| d.data_bucket_count())
            .unwrap_or(0)
    }

    pub fn expires_count(&self, idx: u32) -> usize {
        self.dbs
            .get(idx as usize)
            .map(|d| d.expires_count())
            .unwrap_or(0)
    }

    pub fn rehashing_count(&self, idx: u32) -> usize {
        self.dbs
            .get(idx as usize)
            .map(|d| d.rehashing_count())
            .unwrap_or(0)
    }

    pub fn rehashing_bucket_count(&self, idx: u32) -> usize {
        self.dbs
            .get(idx as usize)
            .map(|d| d.rehashing_bucket_count())
            .unwrap_or(0)
    }

    pub fn keys_with_volatile_items_count(&self, idx: u32) -> usize {
        self.dbs
            .get(idx as usize)
            .map(|d| d.keys_with_volatile_items_count())
            .unwrap_or(0)
    }

    pub fn set_data_resize_policy(
        &mut self,
        idx: u32,
        policy: crate::foundation::io::store::structure::dict::ResizePolicy,
    ) {
        if let Some(db) = self.dbs.get_mut(idx as usize) {
            db.set_data_resize_policy(policy);
        }
    }

    pub fn set_resize_budget(&mut self, budget: Option<usize>) {
        self.resize_budget.reset(budget);
    }

    pub fn try_resize_all(&mut self, active_rehash: bool) {
        for db in self.dbs.iter_mut() {
            db.try_resize(active_rehash);
        }
    }

    pub fn swap(&mut self, idx1: u32, idx2: u32) {
        if idx1 == idx2 {
            return;
        }
        let len = self.dbs.len();
        if idx1 as usize >= len || idx2 as usize >= len {
            return;
        }
        self.dbs.swap(idx1 as usize, idx2 as usize);
    }

    pub fn flush_all(&mut self) {
        for db in &mut self.dbs {
            db.flush();
        }
    }

    /// Detach all values while leaving each database's metadata reset on the
    /// event-loop thread. FLUSHALL hands the returned values to lazyfree when
    /// its selected mode is asynchronous.
    pub fn drain_all_values_for_lazyfree(&mut self) -> Vec<Robj> {
        self.dbs
            .iter_mut()
            .flat_map(Db::drain_values_for_lazyfree)
            .collect()
    }

    pub fn count_keys_in_slot(&self, slot: usize) -> usize {
        self.dbs.iter().map(|db| db.scan_slot_key_count(slot)).sum()
    }

    pub fn get_keys_in_slot(&self, slot: usize, count: usize) -> Vec<Vec<u8>> {
        let mut keys = self
            .dbs
            .iter()
            .flat_map(|db| db.keys_in_slot(slot))
            .collect::<Vec<_>>();
        keys.sort_unstable();
        keys.truncate(count);
        keys
    }

    pub fn take_slot(&mut self, slot: usize) -> Vec<SlotEntry> {
        let mut entries = Vec::new();
        for (db_id, db) in self.dbs.iter_mut().enumerate() {
            entries.extend(db.take_slot(slot).into_iter().map(
                |(key, value, expire, hash_field_expires)| SlotEntry {
                    db_id: db_id as u32,
                    key,
                    value,
                    expire,
                    hash_field_expires,
                },
            ));
        }
        entries
    }

    pub fn restore_slot_entries(&mut self, entries: Vec<SlotEntry>) {
        for entry in entries {
            if let Some(db) = self.get_mut(entry.db_id) {
                db.load_entry_with_hash_field_expires(
                    entry.key,
                    entry.value,
                    entry.expire,
                    entry.hash_field_expires,
                );
            }
        }
    }

    pub fn delete_slot(&mut self, slot: usize) -> usize {
        self.take_slot(slot).len()
    }

    pub fn set_pause_skip_expire_all(&mut self, value: bool) {
        for db in &mut self.dbs {
            db.set_pause_skip_expire(value);
        }
    }

    pub fn set_no_lazy_expire_all(&mut self, value: bool) {
        for db in &mut self.dbs {
            db.set_no_lazy_expire(value);
        }
    }

    pub fn set_replica_local_expire_only_all(&mut self, value: bool) {
        for db in &mut self.dbs {
            db.set_replica_local_expire_only(value);
        }
    }

    pub fn set_slot_importing(&mut self, slot: usize, is_importing: bool) {
        for db in &mut self.dbs {
            db.set_slot_importing(slot, is_importing);
        }
    }

    pub fn clear_importing_slots(&mut self) {
        for db in &mut self.dbs {
            db.clear_importing_slots();
        }
    }

    pub fn drain_expired_deletions(&mut self) -> Vec<(u32, Vec<u8>)> {
        let mut result = Vec::new();
        for (idx, db) in self.dbs.iter_mut().enumerate() {
            if db.expired_deletions.is_empty() {
                continue;
            }
            for key in db.expired_deletions.drain(..) {
                result.push((idx as u32, key));
            }
        }
        result
    }

    pub fn drain_active_expired_deletions(&mut self) -> Vec<(u32, Vec<u8>)> {
        let mut result = Vec::new();
        for (idx, db) in self.dbs.iter_mut().enumerate() {
            for key in db.active_expired_deletions.drain(..) {
                result.push((idx as u32, key));
            }
        }
        result
    }

    pub fn clear_expired_deletions(&mut self) {
        for db in self.dbs.iter_mut() {
            db.expired_deletions.clear();
            db.active_expired_deletions.clear();
        }
    }

    pub fn drain_lazy_expired_hash_fields(&mut self) -> u64 {
        let mut total = 0;
        for db in self.dbs.iter_mut() {
            total += db.lazy_expired_hash_fields;
            db.lazy_expired_hash_fields = 0;
        }
        total
    }

    pub fn drain_pending_hexpired(&mut self) -> Vec<(u32, Vec<u8>, Vec<u8>)> {
        let mut result = Vec::new();
        for (idx, db) in self.dbs.iter_mut().enumerate() {
            for (key, field) in db.pending_hexpired.drain(..) {
                result.push((idx as u32, key, field));
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::io::store::Robj;

    fn first_byte_slot(key: &[u8]) -> usize {
        key.first().copied().unwrap_or_default() as usize & 0b11
    }

    #[test]
    fn new_with_slot_router_applies_router_to_each_db() {
        let mut dbs = DbSet::new_with_slot_router(2, 2, first_byte_slot);

        dbs.get_mut(0)
            .unwrap()
            .set(vec![1, b'a'], Robj::string("v"));
        dbs.get_mut(1)
            .unwrap()
            .set(vec![2, b'a'], Robj::string("v"));

        assert_eq!(dbs.get(0).unwrap().data.slot_len(1), 1);
        assert_eq!(dbs.get(1).unwrap().data.slot_len(2), 1);
    }

    #[test]
    fn empty_like_preserves_slot_router_configuration() {
        let dbs = DbSet::new_with_slot_router(2, 2, first_byte_slot);
        let mut replacement = dbs.empty_like();

        replacement
            .get_mut(0)
            .unwrap()
            .set(vec![3, b'a'], Robj::string("v"));

        assert_eq!(replacement.num_dbs(), 2);
        assert_eq!(replacement.get(0).unwrap().data.slot_len(3), 1);
    }
}
