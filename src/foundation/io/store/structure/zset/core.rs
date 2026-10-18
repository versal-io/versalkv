use bytes::Bytes;
use std::ops::{Deref, DerefMut};

use super::skiplist::SkipList;
use crate::foundation::io::store::structure::hashtable::HashTable;
use crate::foundation::memory::value::{Encoding, EncodingChange, ZSetOps};

#[derive(Debug, Clone, PartialEq)]
struct Repr {
    dict: HashTable<f64>,
    zsl: Box<SkipList>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ZSetSl(Box<Repr>);

impl Deref for ZSetSl {
    type Target = HashTable<f64>;
    fn deref(&self) -> &Self::Target {
        &self.0.dict
    }
}

impl DerefMut for ZSetSl {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0.dict
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
            zsl: Box::new(SkipList::new()),
        }))
    }

    /// Build a dual-struct ZSetSl from an already-populated dict by replaying
    /// its (member, score) pairs into a fresh skiplist. Dict ownership is moved.
    pub fn from_dict(dict: HashTable<f64>) -> Self {
        let mut zsl = Box::new(SkipList::new());
        for (member, score) in &dict {
            zsl.insert(*score, member.clone());
        }
        Self(Box::new(Repr { dict, zsl }))
    }

    pub fn sorted_entries(&self) -> Vec<(Bytes, f64)> {
        self.0.zsl.sorted_vec()
    }

    pub fn dict(&self) -> &HashTable<f64> {
        &self.0.dict
    }
    pub fn dict_mut(&mut self) -> &mut HashTable<f64> {
        &mut self.0.dict
    }
    pub fn zsl(&self) -> &SkipList {
        &self.0.zsl
    }
    pub fn zsl_mut(&mut self) -> &mut SkipList {
        &mut self.0.zsl
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
        Encoding::Skiplist
    }
}

impl Default for ZSetSl {
    fn default() -> Self {
        Self::new()
    }
}

impl ZSetOps for ZSetSl {
    fn add(&mut self, member: &[u8], score: f64) -> Result<bool, EncodingChange> {
        let member_bytes = Bytes::copy_from_slice(member);
        let repr = self.0.as_mut();
        let (dict, zsl) = (&mut repr.dict, &mut repr.zsl);
        match dict.get(member).copied() {
            Some(old_score) => {
                if old_score == score {
                    return Ok(false);
                }
                dict.insert(member_bytes.clone(), score);
                zsl.delete(old_score, member);
                zsl.insert(score, member_bytes);
                Ok(false)
            }
            None => {
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
        Encoding::Skiplist
    }
}

pub fn infer_zset_encoding(
    data: &HashTable<f64>,
    max_entries: usize,
    max_value_size: usize,
) -> &'static str {
    if max_entries == 0 {
        return "skiplist";
    }
    if data.peak_len() > max_entries {
        return "skiplist";
    }
    for k in data.keys() {
        if k.len() > max_value_size {
            return "skiplist";
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
        return Some("skiplist");
    }
    let is_new_member = !data.keys().any(|member| member.as_ref() == new_member);
    if data.len() > max_entries || (is_new_member && data.len() >= max_entries) {
        return Some("skiplist");
    }
    if new_member.len() > max_value_size {
        return Some("skiplist");
    }
    None
}
