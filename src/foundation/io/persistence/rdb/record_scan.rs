//! Allocation-free, resumable boundaries for the ordinary collection records.
//! Content validation and construction remain exclusively with the codecs.

use crate::foundation::io::persistence::codec::types::{
    load_len, QUICKLIST_NODE_CONTAINER_PACKED, QUICKLIST_NODE_CONTAINER_PLAIN, RDB_ENC_INT16,
    RDB_ENC_INT32, RDB_ENC_INT8, RDB_ENC_LZF, RDB_TYPE_HASH, RDB_TYPE_LIST,
    RDB_TYPE_LIST_QUICKLIST, RDB_TYPE_LIST_QUICKLIST_2, RDB_TYPE_SET, RDB_TYPE_ZSET,
    RDB_TYPE_ZSET_2,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ScanOutcome {
    NeedInput,
    Complete,
    /// Unsupported or malformed layout: preserve the original loader behavior.
    Fallback,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Layout {
    Strings,
    HashPairs,
    LegacyZset,
    BinaryZset,
    Quicklist,
    Quicklist2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Key,
    Count,
    Member,
    LegacyScore,
    BinaryScore,
    Container,
    Node,
    Complete,
}

enum Field<T> {
    Ready(T),
    NeedInput,
    Fallback,
}

enum Length {
    Raw(usize),
    Encoded(u8),
}

pub(super) struct RecordScan {
    layout: Layout,
    phase: Phase,
    /// All positions are relative to the type byte, not the decoder buffer.
    offset: usize,
    remaining: usize,
    pending_end: Option<usize>,
}

impl RecordScan {
    pub(super) fn new(type_byte: u8) -> Option<Self> {
        let layout = match type_byte {
            RDB_TYPE_LIST | RDB_TYPE_SET => Layout::Strings,
            RDB_TYPE_HASH => Layout::HashPairs,
            RDB_TYPE_ZSET => Layout::LegacyZset,
            RDB_TYPE_ZSET_2 => Layout::BinaryZset,
            RDB_TYPE_LIST_QUICKLIST => Layout::Quicklist,
            RDB_TYPE_LIST_QUICKLIST_2 => Layout::Quicklist2,
            _ => return None,
        };
        Some(Self {
            layout,
            phase: Phase::Key,
            offset: 1,
            remaining: 0,
            pending_end: None,
        })
    }

    pub(super) fn scan(&mut self, input: &[u8]) -> ScanOutcome {
        loop {
            let field = match self.phase {
                Phase::Key | Phase::Member => self.scan_string(input),
                Phase::Count => self.scan_count(input),
                Phase::LegacyScore => self.scan_legacy_score(input),
                Phase::BinaryScore => self.scan_span(input, self.offset.checked_add(8)),
                Phase::Container => self.scan_container(input),
                Phase::Node => self.scan_node(input),
                Phase::Complete => return ScanOutcome::Complete,
            };
            match field {
                Field::NeedInput => return ScanOutcome::NeedInput,
                Field::Fallback => return ScanOutcome::Fallback,
                Field::Ready(()) => self.advance_phase(),
            }
        }
    }

    fn advance_phase(&mut self) {
        self.phase = match self.phase {
            Phase::Key => Phase::Count,
            Phase::Count if self.remaining == 0 => Phase::Complete,
            Phase::Count => self.first_member_phase(),
            Phase::Member if self.layout == Layout::LegacyZset => Phase::LegacyScore,
            Phase::Member if self.layout == Layout::BinaryZset => Phase::BinaryScore,
            Phase::Container => Phase::Node,
            Phase::Member | Phase::LegacyScore | Phase::BinaryScore | Phase::Node => {
                self.remaining -= 1;
                if self.remaining == 0 {
                    Phase::Complete
                } else {
                    self.first_member_phase()
                }
            }
            Phase::Complete => Phase::Complete,
        };
    }

    fn first_member_phase(&self) -> Phase {
        match self.layout {
            Layout::Quicklist => Phase::Node,
            Layout::Quicklist2 => Phase::Container,
            _ => Phase::Member,
        }
    }

    fn scan_count(&mut self, input: &[u8]) -> Field<()> {
        let mut pos = self.offset;
        let count = match read_length(input, &mut pos) {
            Field::Ready(Length::Raw(count)) => count,
            Field::Ready(Length::Encoded(_)) | Field::Fallback => return Field::Fallback,
            Field::NeedInput => return Field::NeedInput,
        };
        let fields_per_entry = if self.layout == Layout::HashPairs {
            2
        } else {
            1
        };
        let Some(remaining) = count.checked_mul(fields_per_entry) else {
            return Field::Fallback;
        };
        let minimum_entry_size = match self.layout {
            Layout::LegacyZset | Layout::Quicklist2 => 2,
            Layout::BinaryZset => 9,
            _ => 1,
        };
        // Reject only unrepresentable boundaries, never reserve from a count.
        if remaining
            .checked_mul(minimum_entry_size)
            .and_then(|size| pos.checked_add(size))
            .is_none()
        {
            return Field::Fallback;
        }
        self.offset = pos;
        self.remaining = remaining;
        Field::Ready(())
    }

    fn scan_string(&mut self, input: &[u8]) -> Field<()> {
        if let Some(end) = self.pending_end {
            return self.scan_span(input, Some(end));
        }
        let mut pos = self.offset;
        let length = match read_length(input, &mut pos) {
            Field::Ready(length) => length,
            Field::NeedInput => return Field::NeedInput,
            Field::Fallback => return Field::Fallback,
        };
        let size = match length {
            Length::Raw(size) => size,
            Length::Encoded(RDB_ENC_INT8) => 1,
            Length::Encoded(RDB_ENC_INT16) => 2,
            Length::Encoded(RDB_ENC_INT32) => 4,
            Length::Encoded(RDB_ENC_LZF) => {
                let compressed_size = match read_raw_length(input, &mut pos) {
                    Field::Ready(size) => size,
                    Field::NeedInput => return Field::NeedInput,
                    Field::Fallback => return Field::Fallback,
                };
                match read_raw_length(input, &mut pos) {
                    Field::Ready(_) => {}
                    Field::NeedInput => return Field::NeedInput,
                    Field::Fallback => return Field::Fallback,
                }
                compressed_size
            }
            Length::Encoded(_) => return Field::Fallback,
        };
        self.scan_span(input, pos.checked_add(size))
    }

    fn scan_node(&mut self, input: &[u8]) -> Field<()> {
        if let Some(end) = self.pending_end {
            return self.scan_span(input, Some(end));
        }
        let mut pos = self.offset;
        match read_raw_length(input, &mut pos) {
            Field::Ready(0) | Field::Fallback => Field::Fallback,
            Field::Ready(size) => self.scan_span(input, pos.checked_add(size)),
            Field::NeedInput => Field::NeedInput,
        }
    }

    fn scan_container(&mut self, input: &[u8]) -> Field<()> {
        let mut pos = self.offset;
        match read_raw_length(input, &mut pos) {
            Field::Ready(QUICKLIST_NODE_CONTAINER_PLAIN_USIZE)
            | Field::Ready(QUICKLIST_NODE_CONTAINER_PACKED_USIZE) => {
                self.offset = pos;
                Field::Ready(())
            }
            Field::Ready(_) | Field::Fallback => Field::Fallback,
            Field::NeedInput => Field::NeedInput,
        }
    }

    fn scan_legacy_score(&mut self, input: &[u8]) -> Field<()> {
        if let Some(end) = self.pending_end {
            return self.scan_span(input, Some(end));
        }
        let Some(&length) = input.get(self.offset) else {
            return Field::NeedInput;
        };
        let size = if length >= 253 {
            0
        } else {
            usize::from(length)
        };
        self.scan_span(
            input,
            self.offset
                .checked_add(1)
                .and_then(|pos| pos.checked_add(size)),
        )
    }

    fn scan_span(&mut self, input: &[u8], end: Option<usize>) -> Field<()> {
        let Some(end) = end else {
            return Field::Fallback;
        };
        if end > input.len() {
            self.pending_end = Some(end);
            return Field::NeedInput;
        }
        self.offset = end;
        self.pending_end = None;
        Field::Ready(())
    }
}

const QUICKLIST_NODE_CONTAINER_PLAIN_USIZE: usize = QUICKLIST_NODE_CONTAINER_PLAIN as usize;
const QUICKLIST_NODE_CONTAINER_PACKED_USIZE: usize = QUICKLIST_NODE_CONTAINER_PACKED as usize;

fn read_raw_length(input: &[u8], pos: &mut usize) -> Field<usize> {
    match read_length(input, pos) {
        Field::Ready(Length::Raw(value)) => Field::Ready(value),
        Field::Ready(Length::Encoded(_)) | Field::Fallback => Field::Fallback,
        Field::NeedInput => Field::NeedInput,
    }
}

fn read_length(input: &[u8], pos: &mut usize) -> Field<Length> {
    let Some(&prefix) = input.get(*pos) else {
        return Field::NeedInput;
    };
    let size = match prefix >> 6 {
        0 | 3 => 1,
        1 => 2,
        _ if prefix == 0x80 => 5,
        _ if prefix == 0x81 => 9,
        _ => return Field::Fallback,
    };
    let Some(end) = pos.checked_add(size) else {
        return Field::Fallback;
    };
    if end > input.len() {
        return Field::NeedInput;
    }
    match load_len(input, pos) {
        Some((value, true)) => Field::Ready(Length::Encoded(value as u8)),
        Some((value, false)) => match usize::try_from(value) {
            Ok(value) => Field::Ready(Length::Raw(value)),
            Err(_) => Field::Fallback,
        },
        None => Field::Fallback,
    }
}

#[cfg(test)]
#[path = "record_scan_tests.rs"]
mod tests;
