use super::db::Db;
use super::now_ms;
use crate::foundation::io::store::Robj;

impl Db {
    pub fn random_key(&mut self) -> Option<&[u8]> {
        let now = now_ms();
        let allvolatile = self.data.len() == self.expires.len();
        let mut maxtries = 100;

        loop {
            let candidate = self
                .data
                .random_entry()
                .map(|(key, _)| key.as_bytes().to_vec())?;
            if !self.is_expired_at(&candidate, now) {
                return self.stored_key_bytes(&candidate);
            }

            let restricted_expire = self.no_lazy_expire
                || self.pause_skip_expire
                || !self.should_expire_locally(&candidate);
            if restricted_expire {
                if allvolatile {
                    maxtries -= 1;
                    if maxtries == 0 {
                        return self.stored_key_bytes(&candidate);
                    }
                }
                continue;
            }

            self.del(&candidate);
            self.expired_deletions.push(candidate);
        }
    }

    fn stored_key_bytes(&self, key: &[u8]) -> Option<&[u8]> {
        let slot = self.key_slot(key);
        self.data
            .get_key_value_in_slot(slot, key)
            .map(|(stored_key, _)| stored_key.as_bytes())
    }

    pub fn delete_expired(&mut self) {
        self.delete_expired_at(now_ms());
    }

    pub fn delete_expired_with_grace(&mut self) {
        self.delete_expired_at(now_ms().saturating_add(100));
    }

    pub fn delete_expired_at(&mut self, now: u64) {
        let mut candidates: Vec<Vec<u8>> = self
            .data
            .keys()
            .map(|key| key.as_bytes().to_vec())
            .collect();
        for key in self.expires.keys() {
            if !candidates
                .iter()
                .any(|candidate| candidate.as_slice() == key.as_bytes())
            {
                candidates.push(key.as_bytes().to_vec());
            }
        }
        let expired: Vec<Vec<u8>> = candidates
            .into_iter()
            .filter(|key| {
                // Skip keys belonging to an importing slot: their TTL is owned by
                // the source node's migration stream and must not be deleted
                // locally during import (mirrors C's `keyIsInImportingSlot` guard
                // in active expiration / eviction paths).
                !self.data.is_importing_slot(self.key_slot(key))
                    && self
                        .get_expire(key)
                        .is_some_and(|expire_at| now >= expire_at)
            })
            .collect();
        for key in &expired {
            self.del(key);
            self.expired_deletions.push(key.clone());
        }
    }

    pub fn scan(&mut self, cursor: u64, count: usize) -> (u64, Vec<Vec<u8>>) {
        self.scan_at(cursor, count, now_ms())
    }

    pub fn scan_at(&mut self, cursor: u64, count: usize, now_ms: u64) -> (u64, Vec<Vec<u8>>) {
        if !self.no_lazy_expire && !self.pause_skip_expire {
            self.delete_expired_at(now_ms.saturating_add(100));
        }

        let budget = count.max(1);
        let mut out: Vec<Vec<u8>> = Vec::new();
        let max_iterations = budget;
        let mut cur = cursor;
        let mut iterations = 0;

        loop {
            let mut batch_keys: Vec<Vec<u8>> = Vec::new();
            cur = self.data.scan(cur, |key, _val| {
                if self.no_lazy_expire || self.pause_skip_expire || !self.is_expired_at(key, now_ms)
                {
                    batch_keys.push(key.as_bytes().to_vec());
                }
            });
            out.extend(batch_keys);
            iterations += 1;
            if cur == 0 || out.len() >= budget || iterations >= max_iterations {
                break;
            }
        }

        (cur, out)
    }

    pub fn scan_include_expired(&mut self, cursor: u64, count: usize) -> (u64, Vec<Vec<u8>>) {
        let budget = count.max(1);
        let mut out: Vec<Vec<u8>> = Vec::new();
        let max_iterations = budget;
        let mut cur = cursor;
        let mut iterations = 0;

        loop {
            cur = self.data.scan(cur, |key, _val| {
                out.push(key.as_bytes().to_vec());
            });
            iterations += 1;
            if cur == 0 || out.len() >= budget || iterations >= max_iterations {
                break;
            }
        }

        (cur, out)
    }

    pub fn scan_range_step_include_expired(
        &mut self,
        first_slot: usize,
        last_slot: usize,
        cursor: u64,
    ) -> (u64, Vec<Vec<u8>>) {
        let mut out: Vec<Vec<u8>> = Vec::new();
        let cursor = self.data.scan_range(
            cursor,
            first_slot,
            last_slot,
            |_, _| false,
            |key, _val| {
                out.push(key.as_bytes().to_vec());
            },
        );
        (cursor, out)
    }

    pub fn scan_slot_key_count(&self, slot: usize) -> usize {
        self.data.hashtable_slot_len(slot)
    }

    pub fn next_populated_slot(&self, first_slot: usize, last_slot: usize) -> Option<usize> {
        let last_slot = last_slot.min(self.data.slot_count().saturating_sub(1));
        (first_slot..=last_slot).find(|&slot| self.data.slot_len(slot) != 0)
    }

    pub fn all_keys_sorted(&self) -> Vec<Vec<u8>> {
        let mut keys: Vec<Vec<u8>> = self
            .data
            .keys()
            .map(|key| key.as_bytes().to_vec())
            .collect();
        keys.sort();
        keys
    }

    #[allow(clippy::result_unit_err)]
    pub fn module_scan_key(&mut self, key: &[u8]) -> Result<Vec<(Vec<u8>, Option<Vec<u8>>)>, ()> {
        let Some(value) = self.get_no_expire(key) else {
            return Ok(Vec::new());
        };
        match value {
            Robj::HashHashtable(_) | Robj::HashListpack(_) => Ok(value
                .hash_entries()
                .into_iter()
                .map(|(field, value)| (field.to_vec(), Some(value.to_vec())))
                .collect()),
            Robj::SetHashtable(_) | Robj::SetIntset(_) | Robj::SetListpack(_) => Ok(value
                .set_members()
                .into_iter()
                .map(|member| (member, None))
                .collect()),
            Robj::ZSetSkiplist(_) | Robj::ZSetListpack(_) => Ok(value
                .zset_entries()
                .into_iter()
                .map(|(member, score)| (member.to_vec(), Some(score.to_string().into_bytes())))
                .collect()),
            _ => Err(()),
        }
    }
}

#[cfg(test)]
mod module_scan_tests {
    use super::*;
    use crate::foundation::io::store::structure::hash::HashLp;
    use crate::foundation::io::store::structure::set::SetLp;
    use crate::foundation::io::store::structure::zset::ZSetLp;

    #[test]
    fn module_scan_key_reads_compact_collection_encodings() {
        let mut db = Db::new();

        let mut hash = Robj::HashListpack(HashLp::new());
        hash.hash_set(b"f1", b"v1");
        hash.hash_set(b"f2", b"v2");
        db.set(b"hash".to_vec(), hash);

        let mut set = Robj::SetListpack(SetLp::new());
        set.set_add(b"a", usize::MAX);
        set.set_add(b"b", usize::MAX);
        db.set(b"set".to_vec(), set);

        let mut zset = Robj::ZSetListpack(ZSetLp::new());
        zset.zset_add(b"one", 1.0);
        zset.zset_add(b"two", 2.0);
        db.set(b"zset".to_vec(), zset);

        assert_eq!(
            db.module_scan_key(b"hash"),
            Ok(vec![
                (b"f1".to_vec(), Some(b"v1".to_vec())),
                (b"f2".to_vec(), Some(b"v2".to_vec())),
            ])
        );
        assert_eq!(
            db.module_scan_key(b"set"),
            Ok(vec![(b"a".to_vec(), None), (b"b".to_vec(), None)])
        );
        assert_eq!(
            db.module_scan_key(b"zset"),
            Ok(vec![
                (b"one".to_vec(), Some(b"1".to_vec())),
                (b"two".to_vec(), Some(b"2".to_vec())),
            ])
        );
    }
}
