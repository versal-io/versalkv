use bytes::Bytes;

use super::super::hash::HashLp;
use super::super::hashtable::HashTable;
use crate::foundation::memory::value::HashOps;

#[derive(Debug, Clone, PartialEq)]
enum PayloadStorage {
    Listpack(HashLp),
    Hashtable {
        hash: HashTable<Bytes>,
        entry_bytes: usize,
    },
}

/// A hash payload owned by one exact binary path, with tracked field bytes.
#[derive(Debug, Clone, PartialEq)]
pub struct PathHashPayload {
    storage: PayloadStorage,
}

impl Default for PathHashPayload {
    fn default() -> Self {
        Self {
            storage: PayloadStorage::Listpack(HashLp::new()),
        }
    }
}

impl PathHashPayload {
    pub fn len(&self) -> usize {
        match &self.storage {
            PayloadStorage::Listpack(hash) => hash.len(),
            PayloadStorage::Hashtable { hash, .. } => hash.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, field: &[u8]) -> Option<Vec<u8>> {
        match &self.storage {
            PayloadStorage::Listpack(hash) => hash.get(field),
            PayloadStorage::Hashtable { hash, .. } => hash.get(field).map(|value| value.to_vec()),
        }
    }

    pub fn contains(&self, field: &[u8]) -> bool {
        match &self.storage {
            PayloadStorage::Listpack(hash) => hash.contains(field),
            PayloadStorage::Hashtable { hash, .. } => hash.contains_key(field),
        }
    }

    pub fn entries(&self) -> Vec<(Bytes, Bytes)> {
        match &self.storage {
            PayloadStorage::Listpack(hash) => hash
                .all_pairs()
                .into_iter()
                .map(|(field, value)| (Bytes::from(field), Bytes::from(value)))
                .collect(),
            PayloadStorage::Hashtable { hash, .. } => hash
                .iter()
                .map(|(field, value)| (field.clone(), value.clone()))
                .collect(),
        }
    }

    /// Compact payloads finish in one call. Promoted payloads scan their
    /// persistent field dictionary, sampling before command-level filtering.
    pub fn scan(&self, cursor: u64, count: usize) -> (u64, Vec<(Bytes, Bytes)>) {
        match &self.storage {
            PayloadStorage::Listpack(_) => (0, self.entries()),
            PayloadStorage::Hashtable { hash, .. } => {
                let mut entries = Vec::new();
                let mut next_cursor = cursor;
                for _ in 0..count.saturating_mul(10) {
                    next_cursor = scan_bucket(hash, next_cursor, |field, value| {
                        entries.push((field.clone(), value.clone()));
                    });
                    if next_cursor == 0 || entries.len() >= count {
                        break;
                    }
                }
                (next_cursor, entries)
            }
        }
    }

    fn upgrade(&mut self) {
        if let PayloadStorage::Listpack(hash) = &self.storage {
            let mut converted = HashTable::new();
            let mut entry_bytes = 0;
            for (field, value) in hash.all_pairs() {
                entry_bytes += field.len() + value.len();
                converted.insert(Bytes::from(field), Bytes::from(value));
            }
            self.storage = PayloadStorage::Hashtable {
                hash: converted,
                entry_bytes,
            };
        }
    }

    pub fn set_pairs(
        &mut self,
        pairs: &[(Bytes, Bytes)],
        max_entries: usize,
        max_value_size: usize,
    ) {
        if pairs
            .iter()
            .any(|(field, value)| field.len() > max_value_size || value.len() > max_value_size)
        {
            self.upgrade();
        }
        for (field, value) in pairs {
            if let PayloadStorage::Listpack(hash) = &mut self.storage {
                if hash
                    .set_with_threshold(field, value, max_entries, max_value_size)
                    .is_ok()
                {
                    continue;
                }
                self.upgrade();
            }
            if let PayloadStorage::Hashtable { hash, entry_bytes } = &mut self.storage {
                // Replace in place so the stored key's allocation remains accounted.
                if let Some(old_value) = hash.get_mut(field.as_ref()) {
                    *entry_bytes = *entry_bytes - old_value.len() + value.len();
                    *old_value = value.clone();
                } else {
                    *entry_bytes += field.len() + value.len();
                    hash.insert(field.clone(), value.clone());
                }
            }
        }
    }

    pub(super) fn delete(&mut self, field: &[u8]) -> bool {
        match &mut self.storage {
            PayloadStorage::Listpack(hash) => hash.del(field).unwrap_or(false),
            PayloadStorage::Hashtable { hash, entry_bytes } => {
                if let Some(value) = hash.remove(field) {
                    *entry_bytes -= field.len() + value.len();
                    if hash.is_empty() {
                        hash.clear();
                    }
                    true
                } else {
                    false
                }
            }
        }
    }

    pub(super) fn heap_usage(&self) -> usize {
        match &self.storage {
            PayloadStorage::Listpack(hash) => hash.memory_usage(),
            PayloadStorage::Hashtable { hash, entry_bytes } => {
                // Includes both retained rehash tables and collision buckets.
                // The dictionary itself is inline in Self.
                hash.allocated_bytes() + entry_bytes
            }
        }
    }

    pub fn memory_usage(&self) -> usize {
        std::mem::size_of::<Self>() + self.heap_usage()
    }

    #[cfg(test)]
    pub(super) fn assert_accounting(&self) {
        if let PayloadStorage::Hashtable { hash, entry_bytes } = &self.storage {
            assert_eq!(
                *entry_bytes,
                hash.iter()
                    .map(|(field, value)| field.len() + value.len())
                    .sum::<usize>()
            );
        }
    }
}

fn scan_bucket<F: FnMut(&Bytes, &Bytes)>(
    hash: &HashTable<Bytes>,
    cursor: u64,
    mut visit: F,
) -> u64 {
    if !hash.is_rehashing() {
        return hash.scan(cursor, visit);
    }
    let main_buckets = hash.stats(0, false).toplevel_buckets;
    let target_buckets = hash.stats(1, false).toplevel_buckets;
    if target_buckets >= main_buckets {
        return hash.scan(cursor, visit);
    }
    // During shrink, revisit the small-table bucket and all of its old-table
    // buckets. Repeats are allowed; skipping fields moved between pages is not.
    let small_mask = target_buckets as u64 - 1;
    let large_mask = main_buckets as u64 - 1;
    let mut cursor = cursor & small_mask;
    loop {
        cursor = hash.scan(cursor, &mut visit);
        if cursor & (small_mask ^ large_mask) == 0 {
            return cursor;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn scanned_pairs(payload: &PathHashPayload, count: usize) -> BTreeMap<Bytes, Bytes> {
        let mut cursor = 0;
        let mut found = BTreeMap::new();
        for _ in 0..payload.len().saturating_mul(10) + 1 {
            let (next, pairs) = payload.scan(cursor, count);
            found.extend(pairs);
            cursor = next;
            if cursor == 0 {
                return found;
            }
        }
        panic!("payload scan must terminate");
    }

    #[test]
    fn payload_scan_preserves_compact_promotion_mutations_and_clone_cursors() {
        let mut payload = PathHashPayload::default();
        let mut expected = BTreeMap::new();
        for index in 0..113 {
            let pair = (
                Bytes::from(format!("f{index:04}\0")),
                Bytes::from(format!("v{index}")),
            );
            payload.set_pairs(std::slice::from_ref(&pair), 2, 64);
            expected.insert(pair.0, pair.1);
            if index == 1 {
                assert_eq!(
                    payload.scan(u64::MAX, 1),
                    (0, payload.entries()),
                    "compact payloads ignore COUNT and cursor"
                );
            }
        }
        payload.assert_accounting();
        let PayloadStorage::Hashtable { hash, entry_bytes } = &payload.storage else {
            panic!("payload must have promoted");
        };
        assert!(hash.is_rehashing());
        assert!(hash.stats(0, false).toplevel_buckets > 0);
        assert!(hash.stats(1, false).toplevel_buckets > 0);
        assert_eq!(
            payload.memory_usage(),
            std::mem::size_of::<PathHashPayload>() + hash.allocated_bytes() + entry_bytes
        );
        let (cursor, first) = payload.scan(0, 1);
        assert_ne!(cursor, 0);
        assert!(first.len() < payload.len());
        let cloned = payload.clone();
        assert_eq!(cloned.memory_usage(), payload.memory_usage());
        let PayloadStorage::Hashtable { hash: copy, .. } = &cloned.storage else {
            panic!("clone must retain encoding");
        };
        assert_eq!(copy.rehash_index(), hash.rehash_index());
        assert_eq!(copy.allocated_bytes(), hash.allocated_bytes());
        for table in [0, 1] {
            assert_eq!(
                copy.stats(table, false).toplevel_buckets,
                hash.stats(table, false).toplevel_buckets
            );
        }
        let mut next_cursor = cursor;
        loop {
            let before = payload.memory_usage();
            let page = payload.scan(next_cursor, 1);
            assert_eq!(page, cloned.scan(next_cursor, 1));
            assert_eq!(payload.memory_usage(), before);
            next_cursor = page.0;
            if next_cursor == 0 {
                break;
            }
        }
        for count in [1, 7, 17] {
            assert_eq!(scanned_pairs(&payload, count), expected);
            assert_eq!(scanned_pairs(&cloned, count), expected);
        }
        let original = expected.clone();
        let replacement = (
            Bytes::from_static(b"f0000\0"),
            Bytes::from(vec![b'x'; 4096]),
        );
        payload.set_pairs(std::slice::from_ref(&replacement), 512, 64);
        expected.insert(replacement.0, replacement.1);
        for index in 1..110 {
            let field = Bytes::from(format!("f{index:04}\0"));
            assert!(payload.delete(&field));
            expected.remove(&field);
            payload.assert_accounting();
            assert_eq!(scanned_pairs(&payload, 1), expected);
        }
        for index in 113..2048 {
            let pair = (Bytes::from(format!("f{index:04}")), Bytes::from_static(b""));
            payload.set_pairs(std::slice::from_ref(&pair), 512, 64);
            expected.insert(pair.0, pair.1);
        }
        payload.assert_accounting();
        assert_eq!(scanned_pairs(&payload, 7), expected);
        assert_eq!(scanned_pairs(&cloned, 1), original);
        let size = payload.memory_usage();
        assert!(!payload.delete(b"missing"));
        assert_eq!(payload.memory_usage(), size);

        let (mut cursor, pairs) = payload.scan(0, 1);
        assert_ne!(cursor, 0);
        let mut seen: BTreeMap<_, _> = pairs.into_iter().collect();
        let mut has_shrink = false;
        // Start shrinking after the cursor has already passed some buckets.
        for index in 113..2048 {
            let field = Bytes::from(format!("f{index:04}"));
            assert!(payload.delete(&field));
            expected.remove(&field);
            payload.assert_accounting();
            let PayloadStorage::Hashtable { hash, .. } = &payload.storage else {
                panic!("deletions must retain promoted encoding");
            };
            if hash.is_rehashing()
                && hash.stats(1, false).toplevel_buckets < hash.stats(0, false).toplevel_buckets
            {
                has_shrink = true;
                break;
            }
        }
        assert!(has_shrink);
        for _ in 0..2048 {
            let (next, pairs) = payload.scan(cursor, 1);
            seen.extend(pairs);
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(cursor, 0);
        for (field, value) in expected {
            assert_eq!(seen.get(&field), Some(&value));
        }
    }

    #[test]
    fn payload_tracks_replacements_growth_deletion_and_clone() {
        for max_entries in [0, 2, 512] {
            let mut payload = PathHashPayload::default();
            for index in 0..128 {
                let field = Bytes::from(format!("f{index}"));
                payload.set_pairs(
                    &[(field.clone(), Bytes::from(vec![b'x'; index]))],
                    max_entries,
                    64,
                );
                payload.assert_accounting();
                let cloned = payload.clone();
                assert_eq!(cloned, payload);
                assert_eq!(cloned.memory_usage(), payload.memory_usage());
                payload.set_pairs(
                    &[
                        (field.clone(), Bytes::from_static(b"v")),
                        (field, Bytes::new()),
                    ],
                    max_entries,
                    64,
                );
                payload.assert_accounting();
                assert_eq!(payload.len(), index + 1);
            }
            let original_size = payload.memory_usage();
            for index in (0..128).rev() {
                assert!(payload.delete(format!("f{index}").as_bytes()));
                payload.assert_accounting();
            }
            assert!(payload.is_empty());
            assert!(payload.memory_usage() < original_size);
            let size = payload.memory_usage();
            assert!(!payload.delete(b"missing"));
            assert_eq!(payload.memory_usage(), size);
        }
    }
}
