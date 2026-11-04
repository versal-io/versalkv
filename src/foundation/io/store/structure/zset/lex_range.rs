use bytes::Bytes;
use std::ops::Range;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum LexBound {
    NegativeInfinity,
    PositiveInfinity,
    Value { value: Bytes, inclusive: bool },
}

impl LexBound {
    pub(crate) fn from_parts(value: &[u8], inclusive: bool) -> Self {
        match value {
            b"-" => Self::NegativeInfinity,
            b"+" => Self::PositiveInfinity,
            _ => Self::Value {
                value: Bytes::copy_from_slice(value),
                inclusive,
            },
        }
    }
}

/// Lex bounds applied to entries in score order, including mixed-score sets.
pub(crate) struct LexRange {
    pub min: LexBound,
    pub max: LexBound,
}

impl LexRange {
    pub(crate) fn meets_min(&self, member: &[u8]) -> bool {
        match &self.min {
            LexBound::NegativeInfinity => true,
            LexBound::PositiveInfinity => false,
            LexBound::Value { value, inclusive } => {
                member > value.as_ref() || (*inclusive && member == value.as_ref())
            }
        }
    }

    pub(crate) fn meets_max(&self, member: &[u8]) -> bool {
        match &self.max {
            LexBound::NegativeInfinity => false,
            LexBound::PositiveInfinity => true,
            LexBound::Value { value, inclusive } => {
                member < value.as_ref() || (*inclusive && member == value.as_ref())
            }
        }
    }

    pub(crate) fn is_valid(&self) -> bool {
        match (&self.min, &self.max) {
            (LexBound::PositiveInfinity, _) | (_, LexBound::NegativeInfinity) => false,
            (LexBound::NegativeInfinity, _) | (_, LexBound::PositiveInfinity) => true,
            (
                LexBound::Value {
                    value: min,
                    inclusive: min_inclusive,
                },
                LexBound::Value {
                    value: max,
                    inclusive: max_inclusive,
                },
            ) => min < max || (min == max && *min_inclusive && *max_inclusive),
        }
    }

    pub fn indices(
        &self,
        entries: &[(Bytes, f64)],
        is_reverse: bool,
        offset: i64,
        limit: i64,
    ) -> Range<usize> {
        if entries.is_empty()
            || offset < 0
            || limit == 0
            || !self.is_valid()
            || !self.meets_min(&entries[entries.len() - 1].0)
            || !self.meets_max(&entries[0].0)
        {
            return 0..0;
        }
        let skip = usize::try_from(offset).unwrap_or(usize::MAX);
        let take = if limit < 0 {
            usize::MAX
        } else {
            usize::try_from(limit).unwrap_or(usize::MAX)
        };
        if is_reverse {
            let Some(last) = entries
                .iter()
                .rposition(|(member, _)| self.meets_max(member))
            else {
                return 0..0;
            };
            if !self.meets_min(&entries[last].0) {
                return 0..0;
            }
            let Some(last) = last.checked_sub(skip) else {
                return 0..0;
            };
            let count = entries[..=last]
                .iter()
                .rev()
                .take(take)
                .take_while(|(member, _)| self.meets_min(member))
                .count();
            last + 1 - count..last + 1
        } else {
            let Some(first) = entries
                .iter()
                .position(|(member, _)| self.meets_min(member))
            else {
                return 0..0;
            };
            if !self.meets_max(&entries[first].0) {
                return 0..0;
            }
            let first = first.saturating_add(skip).min(entries.len());
            let count = entries[first..]
                .iter()
                .take(take)
                .take_while(|(member, _)| self.meets_max(member))
                .count();
            first..first + count
        }
    }

    pub fn collect(
        &self,
        entries: &[(Bytes, f64)],
        is_reverse: bool,
        offset: i64,
        limit: i64,
    ) -> Vec<(Bytes, f64)> {
        let indices = self.indices(entries, is_reverse, offset, limit);
        if is_reverse {
            entries[indices].iter().rev().cloned().collect()
        } else {
            entries[indices].to_vec()
        }
    }
}
