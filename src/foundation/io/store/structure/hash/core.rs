use bytes::Bytes;
use indexmap::IndexMap;
use std::ops::{Deref, DerefMut};

use crate::foundation::memory::value::{Encoding, EncodingChange, HashOps};

/// Hashtable-backed hash encoding.
#[derive(Debug, Clone, PartialEq)]
pub struct HashHt {
    pub data: Box<IndexMap<Bytes, Bytes>>,
}

impl HashHt {
    pub fn new() -> Self {
        Self {
            data: Box::new(IndexMap::new()),
        }
    }
}

impl Default for HashHt {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for HashHt {
    type Target = IndexMap<Bytes, Bytes>;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl DerefMut for HashHt {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl HashOps for HashHt {
    fn set(&mut self, field: &[u8], value: &[u8]) -> Result<bool, EncodingChange> {
        let key = Bytes::copy_from_slice(field);
        let val = Bytes::copy_from_slice(value);
        Ok(self.data.insert(key, val).is_none())
    }

    fn get(&self, field: &[u8]) -> Option<Vec<u8>> {
        self.data.get(field).map(|v| v.to_vec())
    }

    fn del(&mut self, field: &[u8]) -> Result<bool, EncodingChange> {
        Ok(self.data.shift_remove(field).is_some())
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    fn contains(&self, field: &[u8]) -> bool {
        self.data.contains_key(field)
    }

    fn encoding(&self) -> Encoding {
        Encoding::Hashtable
    }
}

pub fn infer_hash_encoding(
    data: &IndexMap<Bytes, Bytes>,
    max_entries: usize,
    max_value_size: usize,
) -> &'static str {
    if max_entries == 0 {
        return "hashtable";
    }
    if data.len() > max_entries {
        return "hashtable";
    }
    for (k, v) in data.iter() {
        if k.len() > max_value_size || v.len() > max_value_size {
            return "hashtable";
        }
    }
    "listpack"
}

pub fn check_hash_encoding_upgrade(
    data: &IndexMap<Bytes, Bytes>,
    new_key: &[u8],
    new_val: &[u8],
    max_entries: usize,
    max_value_size: usize,
) -> Option<&'static str> {
    if max_entries == 0 {
        return Some("hashtable");
    }
    let is_new_key = !data.keys().any(|key| key.as_ref() == new_key);
    if data.len() > max_entries || (is_new_key && data.len() >= max_entries) {
        return Some("hashtable");
    }
    if new_key.len() > max_value_size || new_val.len() > max_value_size {
        return Some("hashtable");
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(len: usize) -> Bytes {
        Bytes::from(vec![b'x'; len])
    }

    #[test]
    fn infer_hash_encoding_honors_entry_and_value_thresholds() {
        let mut data = IndexMap::new();
        data.insert(bytes(4), bytes(4));
        data.insert(bytes(8), bytes(8));

        assert_eq!(infer_hash_encoding(&data, 0, 64), "hashtable");
        assert_eq!(infer_hash_encoding(&data, 2, 8), "listpack");
        assert_eq!(infer_hash_encoding(&data, 1, 8), "hashtable");

        let mut long_key = IndexMap::new();
        long_key.insert(bytes(9), bytes(1));
        assert_eq!(infer_hash_encoding(&long_key, 2, 8), "hashtable");

        let mut long_value = IndexMap::new();
        long_value.insert(bytes(1), bytes(9));
        assert_eq!(infer_hash_encoding(&long_value, 2, 8), "hashtable");
    }

    #[test]
    fn check_hash_encoding_upgrade_reports_only_actual_threshold_crossings() {
        let mut data = IndexMap::new();
        data.insert(bytes(1), bytes(1));
        data.insert(bytes(2), bytes(2));

        assert_eq!(
            check_hash_encoding_upgrade(&data, b"k", b"v", 0, 8),
            Some("hashtable")
        );
        assert_eq!(
            check_hash_encoding_upgrade(&data, b"k", b"v", 1, 8),
            Some("hashtable")
        );
        assert_eq!(
            check_hash_encoding_upgrade(&data, &[b'x'; 9], b"v", 2, 8),
            Some("hashtable")
        );
        assert_eq!(
            check_hash_encoding_upgrade(&data, b"k", &[b'x'; 9], 2, 8),
            Some("hashtable")
        );
        assert_eq!(
            check_hash_encoding_upgrade(&data, b"k", b"v", 2, 8),
            Some("hashtable")
        );
        assert_eq!(
            check_hash_encoding_upgrade(&data, data.get_index(0).unwrap().0.as_ref(), b"v", 2, 8),
            None
        );
    }
}
