use std::fmt;

use super::CLUSTER_SLOTS;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SlotRange {
    pub start: u16,
    pub end: u16,
}

impl SlotRange {
    pub fn new(start: u16, end: u16) -> Result<Self, SlotRangeError> {
        if start > end {
            return Err(SlotRangeError::Reversed { start, end });
        }
        if end as usize >= CLUSTER_SLOTS {
            return Err(SlotRangeError::OutOfRange { slot: end });
        }
        Ok(Self { start, end })
    }

    pub fn contains(&self, slot: u16) -> bool {
        self.start <= slot && slot <= self.end
    }
}

impl fmt::Display for SlotRange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.start == self.end {
            write!(formatter, "{}", self.start)
        } else {
            write!(formatter, "{}-{}", self.start, self.end)
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlotRangeError {
    Reversed { start: u16, end: u16 },
    OutOfRange { slot: u16 },
}

impl fmt::Display for SlotRangeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Reversed { start, end } => {
                write!(formatter, "slot range start {start} exceeds end {end}")
            }
            Self::OutOfRange { slot } => write!(formatter, "slot {slot} is out of range"),
        }
    }
}

impl std::error::Error for SlotRangeError {}
