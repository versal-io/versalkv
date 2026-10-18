use std::fmt;

use crate::engine::distributed::cluster::state::CLUSTER_SLOTS;
use crate::engine::distributed::cluster::SlotRange;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SlotRanges(Vec<SlotRange>);

impl SlotRanges {
    #[cfg(test)]
    pub fn new(ranges: Vec<SlotRange>) -> Result<Self, SlotRangesError> {
        if ranges.is_empty() {
            return Err(SlotRangesError::Empty);
        }
        for (index, current) in ranges.iter().enumerate() {
            if let Some(previous) = ranges[..index]
                .iter()
                .find(|previous| ranges_overlap(**previous, *current))
            {
                return Err(SlotRangesError::Overlap {
                    current: *current,
                    previous: *previous,
                });
            }
        }
        Ok(Self(ranges))
    }

    pub fn parse_pairs<T: AsRef<[u8]>>(values: &[T]) -> Result<Self, SlotRangesError> {
        if values.is_empty() {
            return Err(SlotRangesError::Empty);
        }
        if !values.len().is_multiple_of(2) {
            return Err(SlotRangesError::MissingEnd);
        }

        let mut ranges = Vec::with_capacity(values.len() / 2);
        for pair in values.as_chunks::<2>().0 {
            let start = parse_slot(pair[0].as_ref())?;
            let end = parse_slot(pair[1].as_ref())?;
            if start > end {
                return Err(SlotRangesError::Reversed { start, end });
            }
            let current = SlotRange { start, end };
            if let Some(previous) = ranges
                .iter()
                .find(|previous| ranges_overlap(**previous, current))
            {
                return Err(SlotRangesError::Overlap {
                    current,
                    previous: *previous,
                });
            }
            ranges.push(current);
        }
        Ok(Self(ranges))
    }

    pub fn contains(&self, slot: u16) -> bool {
        self.0.iter().any(|range| range.contains(slot))
    }

    pub fn overlaps(&self, other: &Self) -> Option<u16> {
        self.0.iter().find_map(|left| {
            other
                .0
                .iter()
                .find(|right| ranges_overlap(*left, **right))
                .map(|right| left.start.max(right.start))
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = &SlotRange> {
        self.0.iter()
    }
}

impl fmt::Display for SlotRanges {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, range) in self.0.iter().enumerate() {
            if index != 0 {
                formatter.write_str(" ")?;
            }
            write!(formatter, "{}-{}", range.start, range.end)?;
        }
        Ok(())
    }
}

fn parse_slot(value: &[u8]) -> Result<u16, SlotRangesError> {
    let value = std::str::from_utf8(value).map_err(|_| SlotRangesError::InvalidSlot)?;
    let slot = value
        .parse::<u16>()
        .map_err(|_| SlotRangesError::InvalidSlot)?;
    if slot as usize >= CLUSTER_SLOTS {
        return Err(SlotRangesError::InvalidSlot);
    }
    Ok(slot)
}

fn ranges_overlap(left: SlotRange, right: SlotRange) -> bool {
    left.end >= right.start && left.start <= right.end
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlotRangesError {
    Empty,
    MissingEnd,
    InvalidSlot,
    Reversed {
        start: u16,
        end: u16,
    },
    Overlap {
        current: SlotRange,
        previous: SlotRange,
    },
}
