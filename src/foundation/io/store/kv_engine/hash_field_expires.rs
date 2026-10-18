use super::now_ms;
use crate::foundation::io::store::kv_engine::db::DbValue;
use crate::foundation::io::store::structure::hash::HashHt;
use crate::foundation::io::store::structure::kvstore::KVStore;
use crate::foundation::io::store::structure::string::StoredKey;
use crate::foundation::io::store::Robj;
use bytes::Bytes;
use std::collections::HashMap;

pub(super) fn hfe_for_each_entry<
    F: FnMut(&[u8], &Robj, Option<u64>, Option<&HashMap<Vec<u8>, u64>>),
>(
    store: &KVStore<StoredKey, DbValue>,
    expires: &KVStore<StoredKey, u64>,
    hash_field_expires: &HashMap<Vec<u8>, HashMap<Vec<u8>, u64>>,
    key_slot: impl Fn(&[u8]) -> usize,
    no_lazy_expire: bool,
    pause_skip_expire: bool,
    mut f: F,
) {
    let now = now_ms();
    for (key, val) in store.iter() {
        let fields = hash_field_expires.get(key.as_bytes());
        let slot = key_slot(key.as_bytes());
        match expires.get_in_slot(slot, key.as_bytes()) {
            Some(&t) if !no_lazy_expire && !pause_skip_expire && now >= t => continue,
            Some(&t) => f(key.as_bytes(), &val.value, Some(t), fields),
            None => f(key.as_bytes(), &val.value, None, fields),
        }
    }
}

impl super::Db {
    fn upgrade_hash_for_field_expiry(&mut self, key: &[u8]) {
        let Some(value) = self.get_mut(key) else {
            return;
        };
        let Robj::HashListpack(listpack) = value else {
            return;
        };
        let mut hash = HashHt::new();
        for (field, field_value) in listpack.all_pairs() {
            hash.insert(Bytes::from(field), Bytes::from(field_value));
        }
        *value = Robj::HashHashtable(hash);
    }

    pub(super) fn remove_hash_field_expire_internal(&mut self, key: &[u8], field: &[u8]) {
        if let Some(fields) = self.hash_field_expires.get_mut(key) {
            fields.remove(field);
            if fields.is_empty() {
                self.hash_field_expires.remove(key);
                self.keys_with_volatile_items = self.keys_with_volatile_items.saturating_sub(1);
            }
        }
    }

    pub(super) fn clear_hash_field_expires(&mut self, key: &[u8]) {
        if self.hash_field_expires.remove(key).is_some() {
            self.keys_with_volatile_items = self.keys_with_volatile_items.saturating_sub(1);
        }
    }

    pub fn check_hash_field_expired(&mut self, key: &[u8], field: &[u8]) -> bool {
        self.check_hash_field_expired_inner(key, field, true)
    }

    pub fn check_hash_field_expired_no_counter(&mut self, key: &[u8], field: &[u8]) -> bool {
        self.check_hash_field_expired_inner(key, field, false)
    }

    fn check_hash_field_expired_inner(
        &mut self,
        key: &[u8],
        field: &[u8],
        count_stat: bool,
    ) -> bool {
        if self.no_lazy_expire || self.pause_skip_expire {
            return false;
        }
        let now = now_ms();
        let expired = self
            .hash_field_expires
            .get(key)
            .and_then(|fields| fields.get(field))
            .map(|&t| now >= t)
            .unwrap_or(false);
        if expired {
            self.remove_hash_field_expire_internal(key, field);
            if let Ok(Some(hash)) = self.get_hash_object_mut(key) {
                hash.hash_del(field);
            }
            if count_stat {
                self.lazy_expired_hash_fields += 1;
                let event = (key.to_vec(), field.to_vec());
                if !self.pending_hexpired.contains(&event) {
                    self.pending_hexpired.push(event);
                }
            }
        }
        expired
    }

    /// Remove all expired fields from a hash key and return the number removed.
    pub fn cleanup_expired_hash_fields(&mut self, key: &[u8]) -> u64 {
        if self.no_lazy_expire || self.pause_skip_expire {
            return 0;
        }
        let now = now_ms();
        let expired: Vec<Vec<u8>> = match self.hash_field_expires.get(key) {
            Some(fields) => fields
                .iter()
                .filter(|&(_field, &ts)| now >= ts)
                .map(|(field, &_ts)| field.clone())
                .collect(),
            None => return 0,
        };
        if expired.is_empty() {
            return 0;
        }

        let mut removed = 0u64;
        for field in &expired {
            self.remove_hash_field_expire_internal(key, field);
            if let Ok(Some(hash)) = self.get_hash_object_mut(key) {
                if hash.hash_del(field) {
                    removed += 1;
                }
            }
        }
        if removed > 0 {
            self.lazy_expired_hash_fields += removed;
            for field in expired {
                let event = (key.to_vec(), field);
                if !self.pending_hexpired.contains(&event) {
                    self.pending_hexpired.push(event);
                }
            }
            let delete_empty =
                matches!(self.get_hash_object(key), Ok(Some(hash)) if hash.hash_is_empty());
            if delete_empty {
                self.del(key);
            }
        }
        removed
    }

    pub fn set_hash_field_expire(&mut self, key: &[u8], field: &[u8], abs_ms: u64) {
        self.upgrade_hash_for_field_expiry(key);
        let is_new = !self.hash_field_expires.contains_key(key);
        self.hash_field_expires
            .entry(key.to_vec())
            .or_default()
            .insert(field.to_vec(), abs_ms);
        if is_new {
            self.keys_with_volatile_items += 1;
        }
    }

    pub fn get_hash_field_expire(&self, key: &[u8], field: &[u8]) -> Option<u64> {
        self.hash_field_expires
            .get(key)
            .and_then(|fields| fields.get(field))
            .copied()
    }

    pub fn hash_key_has_volatile_fields(&self, key: &[u8]) -> bool {
        self.hash_field_expires.contains_key(key)
    }

    /// Return hash keys that currently have at least one field-level TTL.
    pub fn hash_keys_with_volatile_fields(&self) -> Vec<Vec<u8>> {
        self.hash_field_expires.keys().cloned().collect()
    }

    pub fn hash_field_expired(&self, key: &[u8], field: &[u8]) -> bool {
        let now = now_ms();
        self.hash_field_expires
            .get(key)
            .and_then(|fields| fields.get(field))
            .map(|&t| now >= t)
            .unwrap_or(false)
    }

    /// If a field is already expired, remove only its TTL metadata while
    /// leaving the value in place for an immediately following overwrite.
    /// This preserves hash iteration order and avoids synthesizing a separate
    /// expiry propagation when the write command will propagate its final value.
    pub fn clear_expired_hash_field_ttl(&mut self, key: &[u8], field: &[u8]) -> bool {
        if self.hash_field_expired(key, field) {
            self.remove_hash_field_expire_internal(key, field);
            true
        } else {
            false
        }
    }

    pub fn remove_hash_field_expire(&mut self, key: &[u8], field: &[u8]) -> bool {
        let existed = self
            .hash_field_expires
            .get(key)
            .and_then(|fields| fields.get(field))
            .is_some();
        if existed {
            self.remove_hash_field_expire_internal(key, field);
        }
        existed
    }

    /// Remove and return all field-level TTL metadata for a hash key.
    pub fn take_hash_field_expires(&mut self, key: &[u8]) -> HashMap<Vec<u8>, u64> {
        match self.hash_field_expires.remove(key) {
            Some(fields) => {
                self.keys_with_volatile_items = self.keys_with_volatile_items.saturating_sub(1);
                fields
            }
            None => HashMap::new(),
        }
    }

    /// Attach field-level TTL metadata to a hash key, replacing any existing metadata.
    pub fn set_hash_field_expires_map(&mut self, key: Vec<u8>, fields: HashMap<Vec<u8>, u64>) {
        self.clear_hash_field_expires(&key);
        if !fields.is_empty() {
            self.upgrade_hash_for_field_expiry(&key);
            self.hash_field_expires.insert(key, fields);
            self.keys_with_volatile_items += 1;
        }
    }

    pub fn hash_field_expiretime_ms(&mut self, key: &[u8], field: &[u8]) -> i64 {
        if self.hash_field_expired(key, field) {
            return -2;
        }
        match self.get_hash_field_expire(key, field) {
            Some(t) => t as i64,
            None => -1,
        }
    }

    pub fn hash_field_ttl_ms(&mut self, key: &[u8], field: &[u8]) -> i64 {
        if self.hash_field_expired(key, field) {
            return -2;
        }
        match self.get_hash_field_expire(key, field) {
            Some(t) => {
                let now = now_ms();
                if t > now {
                    (t - now) as i64
                } else {
                    -2
                }
            }
            None => -1,
        }
    }
}
