use bytes::Bytes;
use std::ops::Deref;

use super::ordered_index::OrderedIndex;
use crate::foundation::io::store::structure::hashtable::HashTable;
use crate::foundation::memory::value::{Encoding, EncodingChange, ZSetOps};

#[derive(Debug, Clone, PartialEq)]
struct Repr {
    dict: HashTable<f64>,
    zsl: Box<OrderedIndex>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ZSetSl(Box<Repr>);

impl Deref for ZSetSl {
    type Target = HashTable<f64>;
    fn deref(&self) -> &Self::Target {
        &self.0.dict
    }
}

impl<'a> IntoIterator for &'a ZSetSl {
    type Item = (&'a Bytes, &'a f64);
    type IntoIter = Box<dyn Iterator<Item = (&'a Bytes, &'a f64)> + 'a>;
    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.0.dict.iter())
    }
}

impl ZSetSl {
    pub fn new() -> Self {
        Self(Box::new(Repr {
            dict: HashTable::new(),
            zsl: Box::new(OrderedIndex::new()),
        }))
    }

    /// Build the dictionary and counted B+tree from unique member scores.
    pub fn from_dict(mut dict: HashTable<f64>) -> Self {
        let mut zsl = Box::new(OrderedIndex::new());
        let entries: Vec<_> = dict
            .iter()
            .map(|(member, score)| (member.clone(), *score))
            .collect();
        for (member, score) in entries {
            if score.is_nan() {
                dict.remove(&member);
                continue;
            }
            let score = if score == 0.0 { 0.0 } else { score };
            dict.insert(member.clone(), score);
            zsl.insert(score, member);
        }
        Self(Box::new(Repr { dict, zsl }))
    }

    pub fn sorted_entries(&self) -> Vec<(Bytes, f64)> {
        self.0.zsl.sorted_vec()
    }

    pub fn dict(&self) -> &HashTable<f64> {
        &self.0.dict
    }
    pub fn into_dict(self) -> HashTable<f64> {
        self.0.dict
    }

    pub fn ordered_index(&self) -> &OrderedIndex {
        &self.0.zsl
    }

    /// Retained dictionary/tree allocations plus member payloads counted once.
    /// Allocator metadata, size-class rounding, and Bytes owner metadata are
    /// excluded, matching the collection memory estimator's contract.
    pub fn memory_usage(&self) -> usize {
        std::mem::size_of::<Self>()
            + std::mem::size_of::<Repr>()
            + std::mem::size_of::<OrderedIndex>()
            + self.0.dict.allocated_bytes()
            + self.0.zsl.memory_usage()
            + self.0.dict.keys().map(|member| member.len()).sum::<usize>()
    }

    pub fn rank(&self, member: &[u8]) -> Option<usize> {
        let (dict, zsl) = (&self.0.dict, &self.0.zsl);
        dict.get(member)
            .copied()
            .and_then(|s| zsl.get_rank(s, member))
    }

    pub fn count_by_score(&self, min: (f64, bool), max: (f64, bool)) -> usize {
        self.0.zsl.count_in_score_range(min, max)
    }

    pub fn range_by_score(
        &self,
        min: (f64, bool),
        max: (f64, bool),
        off: i64,
        lim: i64,
        rev: bool,
    ) -> Vec<(Bytes, f64)> {
        self.0.zsl.range_by_score(min, max, off, lim, rev)
    }

    pub fn range_by_rank(&self, start: i64, end: i64, rev: bool) -> Vec<(Bytes, f64)> {
        self.0.zsl.range_by_rank(start, end, rev)
    }

    pub fn range_by_lex(
        &self,
        min: &[u8],
        min_inc: bool,
        max: &[u8],
        max_inc: bool,
        rev: bool,
    ) -> Vec<(Bytes, f64)> {
        self.0.zsl.range_by_lex(min, min_inc, max, max_inc, rev)
    }

    pub fn count_by_lex(&self, min: &[u8], min_inc: bool, max: &[u8], max_inc: bool) -> usize {
        self.0.zsl.count_in_lex_range(min, min_inc, max, max_inc)
    }

    pub fn pop_min(&mut self, count: usize) -> Vec<(Bytes, f64)> {
        let result = self.0.zsl.pop_min(count);
        for (member, _score) in &result {
            self.0.dict.remove(member);
        }
        result
    }

    pub fn pop_max(&mut self, count: usize) -> Vec<(Bytes, f64)> {
        let result = self.0.zsl.pop_max(count);
        for (member, _score) in &result {
            self.0.dict.remove(member);
        }
        result
    }

    pub fn remove_range_by_score(&mut self, min: (f64, bool), max: (f64, bool)) -> usize {
        let items = self.0.zsl.range_by_score(min, max, 0, -1, false);
        let c = items.len();
        self.0.dict.pause_auto_shrink();
        for (member, score) in items {
            self.0.zsl.delete(score, &member);
            self.0.dict.remove(&member);
        }
        self.0.dict.resume_auto_shrink();
        c
    }

    pub fn remove_range_by_rank(&mut self, start: usize, end: usize) -> usize {
        let items = self.0.zsl.range_by_rank(start as i64, end as i64, false);
        let c = items.len();
        self.0.dict.pause_auto_shrink();
        for (member, score) in items {
            self.0.zsl.delete(score, &member);
            self.0.dict.remove(&member);
        }
        self.0.dict.resume_auto_shrink();
        c
    }

    pub fn remove_range_by_lex(
        &mut self,
        min: &[u8],
        min_inc: bool,
        max: &[u8],
        max_inc: bool,
    ) -> usize {
        let items = self.0.zsl.range_by_lex(min, min_inc, max, max_inc, false);
        let c = items.len();
        self.0.dict.pause_auto_shrink();
        for (member, score) in items {
            self.0.zsl.delete(score, &member);
            self.0.dict.remove(&member);
        }
        self.0.dict.resume_auto_shrink();
        c
    }

    pub fn encoding(&self) -> Encoding {
        Encoding::Btree
    }
}

impl Default for ZSetSl {
    fn default() -> Self {
        Self::new()
    }
}

impl ZSetOps for ZSetSl {
    fn add(&mut self, member: &[u8], score: f64) -> Result<bool, EncodingChange> {
        if score.is_nan() {
            return Ok(false);
        }
        let score = if score == 0.0 { 0.0 } else { score };
        let repr = self.0.as_mut();
        let (dict, zsl) = (&mut repr.dict, &mut repr.zsl);
        match dict
            .get_key_value(member)
            .map(|(key, score)| (key.clone(), *score))
        {
            Some((member_bytes, old_score)) => {
                if old_score == score {
                    return Ok(false);
                }
                dict.insert(member_bytes.clone(), score);
                zsl.delete(old_score, member);
                zsl.insert(score, member_bytes);
                Ok(false)
            }
            None => {
                let member_bytes = Bytes::copy_from_slice(member);
                dict.insert(member_bytes.clone(), score);
                zsl.insert(score, member_bytes);
                Ok(true)
            }
        }
    }

    fn remove(&mut self, member: &[u8]) -> Result<bool, EncodingChange> {
        let repr = self.0.as_mut();
        let (dict, zsl) = (&mut repr.dict, &mut repr.zsl);
        match dict.remove(member) {
            Some(score) => {
                zsl.delete(score, member);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    fn score(&self, member: &[u8]) -> Option<f64> {
        self.0.dict.get(member).copied()
    }

    fn len(&self) -> usize {
        self.0.dict.len()
    }

    fn is_empty(&self) -> bool {
        self.0.dict.is_empty()
    }

    fn encoding(&self) -> Encoding {
        Encoding::Btree
    }
}

pub fn infer_zset_encoding(
    data: &HashTable<f64>,
    max_entries: usize,
    max_value_size: usize,
) -> &'static str {
    if max_entries == 0 {
        return "btree";
    }
    if data.peak_len() > max_entries {
        return "btree";
    }
    for k in data.keys() {
        if k.len() > max_value_size {
            return "btree";
        }
    }
    "listpack"
}

pub fn check_zset_encoding_upgrade(
    data: &HashTable<f64>,
    new_member: &[u8],
    max_entries: usize,
    max_value_size: usize,
) -> Option<&'static str> {
    if max_entries == 0 {
        return Some("btree");
    }
    let is_new_member = !data.keys().any(|member| member.as_ref() == new_member);
    if data.len() > max_entries || (is_new_member && data.len() >= max_entries) {
        return Some("btree");
    }
    if new_member.len() > max_value_size {
        return Some("btree");
    }
    None
}
