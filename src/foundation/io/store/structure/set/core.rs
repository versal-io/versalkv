use crate::foundation::io::store::structure::dict::Dict;
use crate::foundation::io::store::structure::string::encoding::int_encodable;
use crate::foundation::memory::value::{Encoding, EncodingChange, SetOps};
use bytes::Bytes;
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SetEncoding {
    Intset,
    Listpack,
    Hashtable,
}

impl SetEncoding {
    pub fn name(&self) -> &'static str {
        match self {
            SetEncoding::Intset => "intset",
            SetEncoding::Listpack => "listpack",
            SetEncoding::Hashtable => "hashtable",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ValkeySet {
    pub data: Box<Dict<Bytes, ()>>,
    pub encoding: SetEncoding,
}

impl PartialEq for ValkeySet {
    fn eq(&self, other: &Self) -> bool {
        if self.encoding != other.encoding || self.data.len() != other.data.len() {
            return false;
        }
        self.data.keys().all(|k| other.data.contains_key(k))
    }
}

impl ValkeySet {
    pub fn new() -> Self {
        ValkeySet {
            data: Box::new(Dict::new()),
            encoding: SetEncoding::Intset,
        }
    }

    pub fn from_hashset(data: HashSet<Bytes>) -> Self {
        let mut dict = Dict::new();
        for v in data {
            dict.insert(v, ());
        }
        ValkeySet {
            data: Box::new(dict),
            encoding: SetEncoding::Hashtable,
        }
    }

    pub fn from_hashset_with_thresholds(
        data: HashSet<Bytes>,
        max_intset_entries: usize,
        max_listpack_entries: usize,
        max_listpack_value: usize,
    ) -> Self {
        let encoding =
            if max_intset_entries > 0 && data.len() <= max_intset_entries && all_integers(&data) {
                SetEncoding::Intset
            } else if max_listpack_entries > 0
                && data.len() <= max_listpack_entries
                && data.iter().all(|v| v.len() <= max_listpack_value)
            {
                SetEncoding::Listpack
            } else {
                SetEncoding::Hashtable
            };
        let mut dict = Dict::new();
        for v in data {
            dict.insert(v, ());
        }
        ValkeySet {
            data: Box::new(dict),
            encoding,
        }
    }

    pub fn insert_with_encoding(
        &mut self,
        value: Bytes,
        max_intset_entries: usize,
        max_listpack_entries: usize,
        max_listpack_value: usize,
    ) -> bool {
        let new = self.data.insert(value.clone(), ()).is_none();
        if new {
            self.maybe_upgrade(
                &value,
                max_intset_entries,
                max_listpack_entries,
                max_listpack_value,
            );
        }
        new
    }

    pub fn remove(&mut self, value: &[u8]) -> bool {
        self.data.remove(value).is_some()
    }

    pub fn scan<F: FnMut(&Bytes, &())>(&self, cursor: u64, f: F) -> u64 {
        self.data.scan(cursor, f)
    }

    fn maybe_upgrade(
        &mut self,
        value: &[u8],
        max_intset_entries: usize,
        max_listpack_entries: usize,
        max_listpack_value: usize,
    ) {
        match self.encoding {
            SetEncoding::Hashtable => {}
            SetEncoding::Listpack => {
                if value.len() > max_listpack_value || self.data.len() > max_listpack_entries {
                    self.encoding = SetEncoding::Hashtable;
                }
            }
            SetEncoding::Intset => {
                if int_encodable(value).is_none() {
                    if self.data.len() <= max_listpack_entries && value.len() <= max_listpack_value
                    {
                        self.encoding = SetEncoding::Listpack;
                    } else {
                        self.encoding = SetEncoding::Hashtable;
                    }
                } else if self.data.len() > max_intset_entries {
                    self.encoding = SetEncoding::Hashtable;
                }
            }
        }
    }
}

impl SetOps for ValkeySet {
    /// Add a member. Always returns `Ok(...)` — hashtable never needs upgrade.
    fn add(&mut self, member: &[u8]) -> Result<bool, EncodingChange> {
        Ok(self
            .data
            .insert(Bytes::copy_from_slice(member), ())
            .is_none())
    }

    /// Remove a member. Always returns `Ok(...)` — hashtable never downgrades.
    fn remove(&mut self, member: &[u8]) -> Result<bool, EncodingChange> {
        Ok(self.data.remove(member).is_some())
    }

    fn contains(&self, member: &[u8]) -> bool {
        self.data.contains_key(member)
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    fn members(&self) -> Vec<Vec<u8>> {
        self.data.keys().map(|b| b.to_vec()).collect()
    }

    fn encoding(&self) -> Encoding {
        match self.encoding {
            SetEncoding::Intset => Encoding::Intset,
            SetEncoding::Listpack => Encoding::Listpack,
            SetEncoding::Hashtable => Encoding::Hashtable,
        }
    }
}

impl Default for ValkeySet {
    fn default() -> Self {
        Self::new()
    }
}

pub fn all_integers(data: &HashSet<Bytes>) -> bool {
    data.iter().all(|b| int_encodable(b).is_some())
}

pub fn infer_set_encoding(
    data: &HashSet<Bytes>,
    max_intset_entries: usize,
    max_listpack_entries: usize,
    max_listpack_value: usize,
) -> &'static str {
    for elem in data.iter() {
        if elem.len() > max_listpack_value {
            return "hashtable";
        }
    }
    if max_intset_entries > 0 && data.len() <= max_intset_entries && all_integers(data) {
        return "intset";
    }
    if max_listpack_entries > 0 && data.len() <= max_listpack_entries {
        return "listpack";
    }
    "hashtable"
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(values: &[&[u8]]) -> HashSet<Bytes> {
        values
            .iter()
            .map(|value| Bytes::copy_from_slice(value))
            .collect()
    }

    #[test]
    fn set_encoding_names_and_constructors_are_stable() {
        assert_eq!(SetEncoding::Intset.name(), "intset");
        assert_eq!(SetEncoding::Listpack.name(), "listpack");
        assert_eq!(SetEncoding::Hashtable.name(), "hashtable");

        let new_set = ValkeySet::new();
        assert!(new_set.data.is_empty());
        assert_eq!(new_set.encoding, SetEncoding::Intset);
        assert_eq!(ValkeySet::default(), new_set);

        let data = set(&[b"a", b"b"]);
        let from_hashset = ValkeySet::from_hashset(data.clone());
        assert_eq!(from_hashset.data.len(), data.len());
        for v in &data {
            assert!(from_hashset.data.contains_key(v));
        }
        assert_eq!(from_hashset.encoding, SetEncoding::Hashtable);
    }

    #[test]
    fn from_hashset_with_thresholds_selects_compact_or_hashtable_encoding() {
        let integers = set(&[b"1", b"-2"]);
        assert_eq!(
            ValkeySet::from_hashset_with_thresholds(integers.clone(), 2, 4, 8).encoding,
            SetEncoding::Intset
        );
        assert_eq!(
            ValkeySet::from_hashset_with_thresholds(integers, 1, 4, 8).encoding,
            SetEncoding::Listpack
        );

        let mixed = set(&[b"1", b"abc"]);
        assert_eq!(
            ValkeySet::from_hashset_with_thresholds(mixed.clone(), 4, 4, 8).encoding,
            SetEncoding::Listpack
        );
        assert_eq!(
            ValkeySet::from_hashset_with_thresholds(mixed.clone(), 0, 4, 8).encoding,
            SetEncoding::Listpack
        );
        assert_eq!(
            ValkeySet::from_hashset_with_thresholds(mixed, 4, 0, 8).encoding,
            SetEncoding::Hashtable
        );

        let large = set(&[b"small", b"0123456789"]);
        assert_eq!(
            ValkeySet::from_hashset_with_thresholds(large, 4, 4, 8).encoding,
            SetEncoding::Hashtable
        );
    }

    #[test]
    fn insert_with_encoding_only_upgrades_and_remove_does_not_downgrade() {
        let mut s = ValkeySet::new();
        assert!(s.insert_with_encoding(Bytes::from_static(b"1"), 2, 4, 8));
        assert_eq!(s.encoding, SetEncoding::Intset);
        assert!(!s.insert_with_encoding(Bytes::from_static(b"1"), 2, 4, 8));
        assert_eq!(s.encoding, SetEncoding::Intset);

        assert!(s.insert_with_encoding(Bytes::from_static(b"abc"), 2, 4, 8));
        assert_eq!(s.encoding, SetEncoding::Listpack);
        assert!(s.insert_with_encoding(Bytes::from_static(b"0123456789"), 2, 4, 8));
        assert_eq!(s.encoding, SetEncoding::Hashtable);
        assert!(s.remove(b"0123456789"));
        assert_eq!(s.encoding, SetEncoding::Hashtable);

        let mut by_count = ValkeySet::new();
        assert!(by_count.insert_with_encoding(Bytes::from_static(b"1"), 1, 4, 8));
        assert!(by_count.insert_with_encoding(Bytes::from_static(b"2"), 1, 4, 8));
        assert_eq!(by_count.encoding, SetEncoding::Hashtable);

        let mut listpack = ValkeySet {
            data: Box::new(Dict::new()),
            encoding: SetEncoding::Listpack,
        };
        assert!(listpack.insert_with_encoding(Bytes::from_static(b"a"), 4, 1, 8));
        assert!(listpack.insert_with_encoding(Bytes::from_static(b"b"), 4, 1, 8));
        assert_eq!(listpack.encoding, SetEncoding::Hashtable);
    }

    #[test]
    fn integer_detection_and_inferred_encoding_cover_boundaries() {
        assert!(all_integers(&HashSet::new()));
        assert!(all_integers(&set(&[
            b"0",
            b"-1",
            i64::MAX.to_string().as_bytes(),
            i64::MIN.to_string().as_bytes()
        ])));
        assert!(!all_integers(&set(&[b"01"])));
        assert!(!all_integers(&set(&[b"+1"])));
        assert!(!all_integers(&set(&[b"abc"])));

        assert_eq!(infer_set_encoding(&set(&[b"1", b"2"]), 2, 2, 8), "intset");
        assert_eq!(infer_set_encoding(&set(&[b"1", b"a"]), 2, 2, 8), "listpack");
        assert_eq!(
            infer_set_encoding(&set(&[b"1", b"a", b"b"]), 2, 2, 8),
            "hashtable"
        );
        assert_eq!(
            infer_set_encoding(&set(&[b"0123456789"]), 2, 2, 8),
            "hashtable"
        );
    }
}
