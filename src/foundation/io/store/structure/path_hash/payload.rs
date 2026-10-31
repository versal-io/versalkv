use bytes::Bytes;

use super::super::hash::{HashHt, HashLp};
use crate::foundation::memory::value::HashOps;

#[derive(Debug, PartialEq)]
enum PayloadStorage {
    Listpack(HashLp),
    Hashtable { hash: HashHt, entry_bytes: usize },
}

/// A hash payload owned by one exact binary path, with tracked field bytes.
#[derive(Debug, PartialEq)]
pub struct PathHashPayload {
    storage: PayloadStorage,
}

impl Clone for PathHashPayload {
    fn clone(&self) -> Self {
        let storage = match &self.storage {
            PayloadStorage::Listpack(hash) => PayloadStorage::Listpack(hash.clone()),
            PayloadStorage::Hashtable { hash, entry_bytes } => {
                let mut cloned = HashHt::new();
                cloned.data.reserve(hash.data.capacity());
                cloned
                    .data
                    .extend(hash.data.iter().map(|(k, v)| (k.clone(), v.clone())));
                PayloadStorage::Hashtable {
                    hash: cloned,
                    entry_bytes: *entry_bytes,
                }
            }
        };
        Self { storage }
    }
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
            PayloadStorage::Hashtable { hash, .. } => hash.data.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, field: &[u8]) -> Option<Vec<u8>> {
        match &self.storage {
            PayloadStorage::Listpack(hash) => hash.get(field),
            PayloadStorage::Hashtable { hash, .. } => hash.get(field),
        }
    }

    pub fn contains(&self, field: &[u8]) -> bool {
        match &self.storage {
            PayloadStorage::Listpack(hash) => hash.contains(field),
            PayloadStorage::Hashtable { hash, .. } => hash.contains(field),
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
                .data
                .iter()
                .map(|(field, value)| (field.clone(), value.clone()))
                .collect(),
        }
    }

    fn upgrade(&mut self) {
        if let PayloadStorage::Listpack(hash) = &self.storage {
            let mut converted = HashHt::new();
            let mut entry_bytes = 0;
            for (field, value) in hash.all_pairs() {
                entry_bytes += field.len() + value.len();
                converted
                    .data
                    .insert(Bytes::from(field), Bytes::from(value));
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
                if let Some(old_value) = hash.data.get_mut(field.as_ref()) {
                    *entry_bytes = *entry_bytes - old_value.len() + value.len();
                    *old_value = value.clone();
                } else {
                    *entry_bytes += field.len() + value.len();
                    hash.data.insert(field.clone(), value.clone());
                }
            }
        }
    }

    pub(super) fn delete(&mut self, field: &[u8]) -> bool {
        match &mut self.storage {
            PayloadStorage::Listpack(hash) => hash.del(field).unwrap_or(false),
            PayloadStorage::Hashtable { hash, entry_bytes } => {
                if let Some((field, value)) = hash.data.shift_remove_entry(field) {
                    *entry_bytes -= field.len() + value.len();
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
                std::mem::size_of::<indexmap::IndexMap<Bytes, Bytes>>()
                    + hash.data.capacity() * std::mem::size_of::<(Bytes, Bytes)>()
                    + entry_bytes
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
                hash.data
                    .iter()
                    .map(|(field, value)| field.len() + value.len())
                    .sum::<usize>()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
