//! Connection output queues and their logical/allocator accounting.

use std::collections::VecDeque;

mod frame;
pub(crate) use frame::{OutputFrame, SharedOutputBlock};

#[cfg(test)]
mod shared_tests;

pub(super) const CLIENT_INLINE_OUTPUT_CAPACITY: usize = 16 * 1024;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum OutputFrameKind {
    Ordinary,
    Replication,
    PreClientHandshake,
    SlotSnapshot,
}

pub(super) struct OutputFrameAccounting {
    pub(super) remaining_bytes: usize,
    pub(super) kind: OutputFrameKind,
    // Logical capacity for client limits, not physical shared ownership.
    pub(super) allocation_bytes: usize,
    pub(super) counts_toward_client_output: bool,
}

pub(crate) struct OutputState {
    pub(crate) queue: VecDeque<OutputFrame>,
    pub(super) frame_accounting: VecDeque<OutputFrameAccounting>,
    pub(super) pending_frame_allocation_bytes: usize,
    pub(super) pending_frame_count: usize,
    pub(super) inline_bytes: usize,
    pub(super) allocation_bytes: usize,
    pub(crate) replication_bytes: usize,
    pub(crate) ordinary_bytes: usize,
    pub(super) pre_client_handshake_bytes: usize,
    pub(super) slot_snapshot_bytes: usize,
    pub(crate) soft_limit_reached_at_ms: Option<i64>,
}

impl OutputState {
    pub(super) fn with_capacity(capacity: usize) -> Self {
        let queue = VecDeque::with_capacity(capacity);
        let frame_accounting = VecDeque::with_capacity(capacity);
        let allocation_bytes = queue
            .capacity()
            .saturating_mul(std::mem::size_of::<OutputFrame>())
            .saturating_add(
                frame_accounting
                    .capacity()
                    .saturating_mul(std::mem::size_of::<OutputFrameAccounting>()),
            );
        Self {
            queue,
            frame_accounting,
            pending_frame_allocation_bytes: 0,
            pending_frame_count: 0,
            inline_bytes: 0,
            allocation_bytes,
            replication_bytes: 0,
            ordinary_bytes: 0,
            pre_client_handshake_bytes: 0,
            slot_snapshot_bytes: 0,
            soft_limit_reached_at_ms: None,
        }
    }
}

pub(crate) struct ThreadedWriteSnapshot {
    pub(super) frames: VecDeque<OutputFrame>,
    pub(super) logical_len: usize,
}

impl ThreadedWriteSnapshot {
    pub(crate) fn logical_len(&self) -> usize {
        self.logical_len
    }

    #[cfg(test)]
    pub(crate) fn frames(&self) -> &VecDeque<OutputFrame> {
        &self.frames
    }

    pub(crate) fn into_frames(self) -> VecDeque<OutputFrame> {
        self.frames
    }
}

impl super::Connection {
    /// RDB relay data is ordinary output, never command-stream replication.
    pub(crate) fn queue_shared_reply(&mut self, block: SharedOutputBlock) {
        self.output.ordinary_bytes = self.output.ordinary_bytes.saturating_add(block.bytes.len());
        self.tot_net_out += block.bytes.len() as u64;
        self.queue_output_frame(OutputFrame::Shared(block), OutputFrameKind::Ordinary);
    }
}
