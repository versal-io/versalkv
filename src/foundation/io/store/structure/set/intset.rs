// SPDX-License-Identifier: BSD-3-Clause

use crate::foundation::memory::value::{Encoding, EncodingChange, SetOps};
use bytes::Bytes;

#[derive(Debug, Clone, PartialEq)]
pub struct IntSet {
    elements: Vec<i64>,
}

impl IntSet {
    pub fn new() -> Self {
        IntSet {
            elements: Vec::new(),
        }
    }

    pub fn from_slice(values: &[i64]) -> Self {
        let mut elements: Vec<i64> = values.to_vec();
        elements.sort_unstable();
        elements.dedup();
        IntSet { elements }
    }

    pub fn len(&self) -> usize {
        self.elements.len()
    }

    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    pub fn contains(&self, value: i64) -> bool {
        self.elements.binary_search(&value).is_ok()
    }

    pub fn get(&self, index: usize) -> Option<i64> {
        self.elements.get(index).copied()
    }

    pub fn iter(&self) -> impl Iterator<Item = i64> + '_ {
        self.elements.iter().copied()
    }

    pub fn random(&self) -> Option<i64> {
        if self.elements.is_empty() {
            return None;
        }
        use crate::foundation::io::sys::rand::random_usize;
        let idx = random_usize(self.elements.len());
        Some(self.elements[idx])
    }

    pub fn add(&mut self, value: i64, max_entries: usize) -> Result<bool, EncodingChange> {
        match self.elements.binary_search(&value) {
            Ok(_) => Ok(false),
            Err(pos) => {
                if self.elements.len() >= max_entries {
                    return Err(EncodingChange::Upgrade);
                }
                self.elements.insert(pos, value);
                Ok(true)
            }
        }
    }

    pub fn remove(&mut self, value: i64) -> bool {
        match self.elements.binary_search(&value) {
            Ok(pos) => {
                self.elements.remove(pos);
                true
            }
            Err(_) => false,
        }
    }

    pub fn into_hashset(self) -> std::collections::HashSet<Bytes> {
        self.elements
            .into_iter()
            .map(|v| Bytes::from(v.to_string()))
            .collect()
    }

    pub fn memory_usage(&self) -> usize {
        self.elements.len() * std::mem::size_of::<i64>()
            + std::mem::size_of::<Vec<i64>>()
            + std::mem::size_of::<Self>()
    }
}

impl Default for IntSet {
    fn default() -> Self {
        Self::new()
    }
}

impl SetOps for IntSet {
    fn add(&mut self, member: &[u8]) -> Result<bool, EncodingChange> {
        let value = parse_i64(member).ok_or(EncodingChange::Upgrade)?;
        IntSet::add(self, value, 512)
    }

    fn remove(&mut self, member: &[u8]) -> Result<bool, EncodingChange> {
        let value = match parse_i64(member) {
            Some(v) => v,
            None => return Ok(false),
        };
        Ok(IntSet::remove(self, value))
    }

    fn contains(&self, member: &[u8]) -> bool {
        parse_i64(member)
            .map(|v| IntSet::contains(self, v))
            .unwrap_or(false)
    }

    fn len(&self) -> usize {
        IntSet::len(self)
    }

    fn is_empty(&self) -> bool {
        IntSet::is_empty(self)
    }

    fn members(&self) -> Vec<Vec<u8>> {
        self.elements
            .iter()
            .map(|v| v.to_string().into_bytes())
            .collect()
    }

    fn encoding(&self) -> Encoding {
        Encoding::Intset
    }
}

fn parse_i64(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    let s = std::str::from_utf8(bytes).ok()?;
    s.parse::<i64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_is_empty() {
        let s = IntSet::new();
        assert!(s.is_empty());
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn add_and_contains() {
        let mut s = IntSet::new();
        assert!(s.add(5, 512).unwrap());
        assert!(s.add(3, 512).unwrap());
        assert!(s.add(8, 512).unwrap());
        assert!(!s.add(3, 512).unwrap()); // duplicate
        assert_eq!(s.len(), 3);
        assert!(s.contains(5));
        assert!(s.contains(3));
        assert!(s.contains(8));
        assert!(!s.contains(7));
    }

    #[test]
    fn elements_stay_sorted() {
        let mut s = IntSet::new();
        s.add(10, 512).unwrap();
        s.add(1, 512).unwrap();
        s.add(5, 512).unwrap();
        s.add(-3, 512).unwrap();
        let collected: Vec<i64> = s.iter().collect();
        assert_eq!(collected, vec![-3, 1, 5, 10]);
    }

    #[test]
    fn remove_works() {
        let mut s = IntSet::from_slice(&[1, 3, 5, 7]);
        assert!(s.remove(3));
        assert_eq!(s.len(), 3);
        assert!(!s.contains(3));
        assert!(!s.remove(9));
    }

    #[test]
    fn get_by_index() {
        let s = IntSet::from_slice(&[10, -5, 0, 3]);
        assert_eq!(s.get(0), Some(-5));
        assert_eq!(s.get(1), Some(0));
        assert_eq!(s.get(3), Some(10));
        assert_eq!(s.get(4), None);
    }

    #[test]
    fn max_entries_triggers_upgrade() {
        let mut s = IntSet::new();
        for i in 0..5 {
            s.add(i, 5).unwrap();
        }
        assert_eq!(s.len(), 5);
        // Adding a 6th element when max_entries=5 should trigger upgrade
        let result = s.add(100, 5);
        assert_eq!(result, Err(EncodingChange::Upgrade));
        assert_eq!(s.len(), 5); // not added
    }

    #[test]
    fn set_ops_trait_add_via_bytes() {
        let mut s = IntSet::new();
        assert!(SetOps::add(&mut s, b"42").unwrap());
        assert!(!SetOps::add(&mut s, b"42").unwrap());
        assert!(SetOps::add(&mut s, b"-7").unwrap());
        assert_eq!(s.len(), 2);
        assert!(SetOps::contains(&s, b"42"));
        assert!(!SetOps::contains(&s, b"abc"));
    }

    #[test]
    fn set_ops_non_integer_triggers_upgrade() {
        let mut s = IntSet::new();
        let result = SetOps::add(&mut s, b"hello");
        assert_eq!(result, Err(EncodingChange::Upgrade));
    }
}
