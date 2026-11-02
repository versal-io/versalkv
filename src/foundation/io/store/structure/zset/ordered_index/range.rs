use super::super::{LexBound, LexRange};
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
        if off < 0 {
            return Vec::new();
        }
        let (start, end) = self.score_ranks(min, max);
        let skip = usize::try_from(off).unwrap_or(usize::MAX);
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
        self.range_by_lex_with_limit((min, min_inc), (max, max_inc), 0, -1, rev)
    }

    pub fn range_by_lex_with_limit(
        &self,
        min: (&[u8], bool),
        max: (&[u8], bool),
        offset: i64,
        limit: i64,
        rev: bool,
    ) -> Vec<(Bytes, f64)> {
        self.range_by_lex_bounds_with_limit(
            LexBound::from_parts(min.0, min.1),
            LexBound::from_parts(max.0, max.1),
            offset,
            limit,
            rev,
        )
    }

    pub(crate) fn range_by_lex_bounds_with_limit(
        &self,
        min: LexBound,
        max: LexBound,
        offset: i64,
        limit: i64,
        rev: bool,
    ) -> Vec<(Bytes, f64)> {
        let range = LexRange { min, max };
        if !self.can_search_lex_range(&range, offset, limit) {
            return Vec::new();
        }
        let skip = usize::try_from(offset).unwrap_or(usize::MAX);
        let take = if limit < 0 {
            usize::MAX
        } else {
            usize::try_from(limit).unwrap_or(usize::MAX)
        };
        if rev {
            collect_lex_entries(
                self.iter_reverse(),
                |member| range.meets_max(member),
                |member| range.meets_min(member),
                skip,
                take,
            )
        } else {
            collect_lex_entries(
                self.iter(),
                |member| range.meets_min(member),
                |member| range.meets_max(member),
                skip,
                take,
            )
        }
    }

    pub fn count_in_lex_range(
        &self,
        min: &[u8],
        min_inc: bool,
        max: &[u8],
        max_inc: bool,
    ) -> usize {
        self.count_in_lex_bounds(
            LexBound::from_parts(min, min_inc),
            LexBound::from_parts(max, max_inc),
        )
    }

    pub(crate) fn count_in_lex_bounds(&self, min: LexBound, max: LexBound) -> usize {
        let range = LexRange { min, max };
        if !self.can_search_lex_range(&range, 0, -1) {
            return 0;
        }
        count_lex_entries(
            self.iter(),
            |member| range.meets_min(member),
            |member| range.meets_max(member),
        )
    }

    pub fn iter(&self) -> OrderedIndexIter<'_> {
        OrderedIndexIter {
            pending: vec![&self.root],
            leaf: [].iter(),
            remaining: self.len(),
            is_reverse: false,
        }
    }

    fn iter_reverse(&self) -> OrderedIndexIter<'_> {
        OrderedIndexIter {
            pending: vec![&self.root],
            leaf: [].iter(),
            remaining: self.len(),
            is_reverse: true,
        }
    }

    fn can_search_lex_range(&self, range: &LexRange, offset: i64, limit: i64) -> bool {
        if self.is_empty() || offset < 0 || limit == 0 || !range.is_valid() {
            return false;
        }
        let first = self.iter().next().map(|(member, _)| member.as_ref());
        let last = self
            .iter_reverse()
            .next()
            .map(|(member, _)| member.as_ref());
        first.is_some_and(|member| range.meets_max(member))
            && last.is_some_and(|member| range.meets_min(member))
    }
}

pub struct OrderedIndexIter<'a> {
    pending: Vec<&'a Node>,
    leaf: std::slice::Iter<'a, Entry>,
    remaining: usize,
    is_reverse: bool,
}

impl<'a> Iterator for OrderedIndexIter<'a> {
    type Item = (&'a Bytes, &'a f64);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let entry = if self.is_reverse {
                self.leaf.next_back()
            } else {
                self.leaf.next()
            };
            if let Some(entry) = entry {
                self.remaining -= 1;
                return Some((&entry.member, &entry.score));
            }
            match &self.pending.pop()?.data {
                NodeData::Leaf(entries) => self.leaf = entries.iter(),
                NodeData::Branch { children, .. } if self.is_reverse => {
                    self.pending.extend(children.iter())
                }
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

fn collect_lex_entries<'a>(
    mut entries: impl Iterator<Item = (&'a Bytes, &'a f64)>,
    starts_range: impl Fn(&[u8]) -> bool,
    remains_in_range: impl Fn(&[u8]) -> bool,
    skip: usize,
    take: usize,
) -> Vec<(Bytes, f64)> {
    let Some(first) = entries.find(|(member, _)| starts_range(member)) else {
        return Vec::new();
    };
    if !remains_in_range(first.0) {
        return Vec::new();
    }
    std::iter::once(first)
        .chain(entries)
        .skip(skip)
        .take(take)
        .take_while(|(member, _)| remains_in_range(member))
        .map(|(member, score)| (member.clone(), *score))
        .collect()
}

fn count_lex_entries<'a>(
    mut entries: impl Iterator<Item = (&'a Bytes, &'a f64)>,
    starts_range: impl Fn(&[u8]) -> bool,
    remains_in_range: impl Fn(&[u8]) -> bool,
) -> usize {
    let Some(first) = entries.find(|(member, _)| starts_range(member)) else {
        return 0;
    };
    if !remains_in_range(first.0) {
        return 0;
    }
    std::iter::once(first)
        .chain(entries)
        .take_while(|(member, _)| remains_in_range(member))
        .count()
}
