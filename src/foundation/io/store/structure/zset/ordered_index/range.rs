use super::{node::Entry, Node, NodeData, OrderedIndex};
use bytes::Bytes;

impl OrderedIndex {
    fn score_ranks(&self, min: (f64, bool), max: (f64, bool)) -> (usize, usize) {
        if min.0.is_nan() || max.0.is_nan() || min.0 > max.0 {
            return (0, 0);
        }
        let start = self.root.rank_before_score(min.0, !min.1);
        let end = self.root.rank_before_score(max.0, max.1);
        if start >= end {
            (0, 0)
        } else {
            (start, end)
        }
    }

    pub fn count_in_score_range(&self, min: (f64, bool), max: (f64, bool)) -> usize {
        let (start, end) = self.score_ranks(min, max);
        end - start
    }

    pub fn range_by_score(
        &self,
        min: (f64, bool),
        max: (f64, bool),
        off: i64,
        lim: i64,
        rev: bool,
    ) -> Vec<(Bytes, f64)> {
        let (start, end) = self.score_ranks(min, max);
        let skip = if off < 0 {
            0
        } else {
            usize::try_from(off).unwrap_or(usize::MAX)
        };
        if skip >= end - start || lim == 0 {
            return Vec::new();
        }
        let available = end - start - skip;
        let take = if lim < 0 {
            available
        } else {
            available.min(usize::try_from(lim).unwrap_or(usize::MAX))
        };
        let first = if rev { end - skip - take } else { start + skip };
        self.collect_ranks(first, first + take, rev)
    }

    pub fn range_by_rank(&self, start: i64, end: i64, rev: bool) -> Vec<(Bytes, f64)> {
        let length = self.len() as i128;
        let start = i128::from(start);
        let end = i128::from(end);
        let start = if start < 0 { length + start } else { start }.max(0);
        let end = if end < 0 { length + end } else { end }.min(length - 1);
        if start > end || start >= length || end < 0 {
            return Vec::new();
        }
        let (start, end) = if rev {
            (length - 1 - end, length - start)
        } else {
            (start, end + 1)
        };
        self.collect_ranks(start as usize, end as usize, rev)
    }

    pub(super) fn collect_ranks(
        &self,
        start: usize,
        end: usize,
        is_reverse: bool,
    ) -> Vec<(Bytes, f64)> {
        let mut output = Vec::with_capacity(end - start);
        self.root.collect_ranks(start, end, is_reverse, &mut output);
        output
    }

    /// `-` and `+` are infinity sentinels; empty bounds are literal bytes.
    pub fn range_by_lex(
        &self,
        min: &[u8],
        min_inc: bool,
        max: &[u8],
        max_inc: bool,
        rev: bool,
    ) -> Vec<(Bytes, f64)> {
        let mut entries: Vec<_> = self
            .iter()
            .filter(|(member, _)| is_in_lex_range(member, min, min_inc, max, max_inc))
            .map(|(member, score)| (member.clone(), *score))
            .collect();
        if rev {
            entries.reverse();
        }
        entries
    }

    pub fn count_in_lex_range(
        &self,
        min: &[u8],
        min_inc: bool,
        max: &[u8],
        max_inc: bool,
    ) -> usize {
        self.iter()
            .filter(|(member, _)| is_in_lex_range(member, min, min_inc, max, max_inc))
            .count()
    }

    pub fn iter(&self) -> OrderedIndexIter<'_> {
        OrderedIndexIter {
            pending: vec![&self.root],
            leaf: [].iter(),
            remaining: self.len(),
        }
    }
}

fn is_in_lex_range(member: &[u8], min: &[u8], min_inc: bool, max: &[u8], max_inc: bool) -> bool {
    if min == b"+" || max == b"-" {
        return false;
    }
    (min == b"-" || member > min || (min_inc && member == min))
        && (max == b"+" || member < max || (max_inc && member == max))
}

pub struct OrderedIndexIter<'a> {
    pending: Vec<&'a Node>,
    leaf: std::slice::Iter<'a, Entry>,
    remaining: usize,
}

impl<'a> Iterator for OrderedIndexIter<'a> {
    type Item = (&'a Bytes, &'a f64);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(entry) = self.leaf.next() {
                self.remaining -= 1;
                return Some((&entry.member, &entry.score));
            }
            match &self.pending.pop()?.data {
                NodeData::Leaf(entries) => self.leaf = entries.iter(),
                NodeData::Branch { children, .. } => self.pending.extend(children.iter().rev()),
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for OrderedIndexIter<'_> {}
impl std::iter::FusedIterator for OrderedIndexIter<'_> {}
