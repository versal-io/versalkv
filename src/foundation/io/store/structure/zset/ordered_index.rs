//! A safe, counted B+tree ordered by numeric score and binary member.
//!
//! Updates, rank lookups, and score counts take O(log n). Contiguous rank and
//! score ranges take O(log n + k), including offset jumps. Lexicographic
//! filtering takes O(n), since members need not be ordered across scores.
#![forbid(unsafe_code)]

use bytes::Bytes;
use std::fmt;
use std::mem::size_of;

#[path = "ordered_index/node.rs"]
mod node;
#[path = "ordered_index/query.rs"]
mod query;
#[path = "ordered_index/range.rs"]
mod range;

use node::{Entry, Node, NodeData};
pub use range::OrderedIndexIter;

#[cfg(test)]
#[path = "ordered_index/randomized_tests.rs"]
mod randomized_tests;
#[cfg(test)]
#[path = "ordered_index/structural_tests.rs"]
mod structural_tests;
#[cfg(test)]
#[path = "ordered_index/tests.rs"]
mod tests;

/// Retained structural storage, excluding all shared `Bytes` payloads.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OrderedIndexMemory {
    /// Inline size of the index, not included in `heap_bytes`.
    pub struct_overhead: usize,
    /// Exact sum of retained vector capacities multiplied by element sizes.
    /// Allocator metadata and size-class rounding are not included.
    pub heap_bytes: usize,
    pub entry_capacity: usize,
    pub child_capacity: usize,
    pub separator_capacity: usize,
    pub leaf_nodes: usize,
    pub branch_nodes: usize,
}

/// An ordered set of `(score, member)` keys.
///
/// Identical keys are idempotent; callers changing a member's score must
/// remove its old key first. NaN insertions are rejected without mutation.
/// Signed zeros compare identically and are stored as positive zero.
#[derive(Clone)]
pub struct OrderedIndex {
    root: Node,
}

impl OrderedIndex {
    pub fn new() -> Self {
        Self {
            root: Node::empty(),
        }
    }

    pub fn len(&self) -> usize {
        self.root.count
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn insert(&mut self, score: f64, member: Bytes) {
        if score.is_nan() {
            return;
        }
        let score = normalize_score(score);
        if let Some(right) = self.root.insert(Entry { score, member }) {
            let left = std::mem::replace(&mut self.root, Node::empty());
            self.root = Node::branch(vec![left, right]);
        }
    }

    pub fn delete(&mut self, score: f64, member: &[u8]) -> bool {
        if score.is_nan() || !self.root.delete(normalize_score(score), member) {
            return false;
        }
        loop {
            let replacement = match &mut self.root.data {
                NodeData::Branch { children, .. } if children.len() == 1 => {
                    Some(children.remove(0))
                }
                _ => None,
            };
            match replacement {
                Some(root) => self.root = root,
                None => break,
            }
        }
        if self.is_empty() {
            self.root = Node::empty();
        }
        true
    }

    pub fn get_rank(&self, score: f64, member: &[u8]) -> Option<usize> {
        if score.is_nan() {
            return None;
        }
        self.root.get_rank(normalize_score(score), member)
    }

    pub fn get_element_by_rank(&self, rank: usize) -> Option<(Bytes, f64)> {
        self.root.entry_by_rank(rank).map(Entry::to_pair)
    }

    pub fn sorted_vec(&self) -> Vec<(Bytes, f64)> {
        self.collect_ranks(0, self.len(), false)
    }

    pub fn pop_min(&mut self, count: usize) -> Vec<(Bytes, f64)> {
        self.pop_entries(count, false)
    }

    pub fn pop_max(&mut self, count: usize) -> Vec<(Bytes, f64)> {
        self.pop_entries(count, true)
    }

    fn pop_entries(&mut self, count: usize, is_reverse: bool) -> Vec<(Bytes, f64)> {
        let count = count.min(self.len());
        let start = if is_reverse { self.len() - count } else { 0 };
        let entries = self.collect_ranks(start, start + count, is_reverse);
        for (member, score) in &entries {
            self.delete(*score, member);
        }
        entries
    }

    /// Heap allocations owned by the tree; shared member storage is excluded.
    pub fn memory_usage(&self) -> usize {
        self.memory_layout().heap_bytes
    }

    pub fn memory_layout(&self) -> OrderedIndexMemory {
        let mut memory = OrderedIndexMemory {
            struct_overhead: size_of::<Self>(),
            ..OrderedIndexMemory::default()
        };
        self.root.measure_memory(&mut memory);
        memory
    }
}

fn normalize_score(score: f64) -> f64 {
    if score == 0.0 {
        0.0
    } else {
        score
    }
}

impl Default for OrderedIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for OrderedIndex {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OrderedIndex")
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

impl PartialEq for OrderedIndex {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().eq(other.iter())
    }
}

impl Eq for OrderedIndex {}
