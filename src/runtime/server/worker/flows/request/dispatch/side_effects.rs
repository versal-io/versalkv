//! Command-local effects and read-batch output.
//! A command builds `CommandEffects` in isolation. The dispatch loop commits
//! that value into `ReadBatchEffects` before parsing the next command, making
//! the command boundary explicit while retaining the existing batched socket
//! flush behavior.

use super::CmdLogEntry;
use bytes::Bytes;

use std::ops::{Deref, DerefMut};

#[derive(Clone, Copy)]
pub(crate) struct DeferredSlotMetric {
    pub(crate) slot: u16,
    pub(crate) cpu_usec: u64,
    pub(crate) network_bytes_in: u64,
}

#[derive(Default)]
pub(crate) struct EffectQueue {
    pub replies: Vec<Vec<u8>>,
    command_reply_bytes: usize,
    deferred_slot_metric: Option<DeferredSlotMetric>,
    pub cmd_logs: Vec<CmdLogEntry>,
    pub monitor: Vec<crate::framework::context::MonitorEvent>,
    pub watch_dirty: Vec<(u32, Bytes)>,
    /// Keys that should wake blocked clients without invalidating WATCH.
    pub wakeup_only: Vec<(u32, Bytes)>,
    pub replication_only: Vec<Vec<u8>>,
    pub aof_only: Vec<Vec<u8>>,
    pub publish: Vec<(Bytes, Bytes)>,
    pub publish_shard: Vec<(Bytes, Bytes)>,
    pub cluster_publish: Vec<(Bytes, Bytes)>,
    pub cluster_publish_shard: Vec<(Bytes, Bytes)>,
    pub unblock: Vec<(usize, bool)>,
    pub tracking: Vec<(usize, Vec<u8>)>,
    pub swapdb: Option<(u32, u32)>,
    pub mark_closing: bool,
}

impl EffectQueue {
    fn append(&mut self, mut other: Self) {
        self.replies.append(&mut other.replies);
        self.command_reply_bytes = self
            .command_reply_bytes
            .saturating_add(other.command_reply_bytes);
        self.deferred_slot_metric = other.deferred_slot_metric.or(self.deferred_slot_metric);
        self.cmd_logs.append(&mut other.cmd_logs);
        self.monitor.append(&mut other.monitor);
        self.watch_dirty.append(&mut other.watch_dirty);
        self.wakeup_only.append(&mut other.wakeup_only);
        self.replication_only.append(&mut other.replication_only);
        self.aof_only.append(&mut other.aof_only);
        self.publish.append(&mut other.publish);
        self.publish_shard.append(&mut other.publish_shard);
        self.cluster_publish.append(&mut other.cluster_publish);
        self.cluster_publish_shard
            .append(&mut other.cluster_publish_shard);
        self.unblock.append(&mut other.unblock);
        self.tracking.append(&mut other.tracking);
        self.swapdb = other.swapdb.or(self.swapdb);
        self.mark_closing |= other.mark_closing;
    }
}

#[derive(Default)]
pub(crate) struct CommandEffects(pub(super) EffectQueue);

impl CommandEffects {
    pub(super) fn commit(&mut self, command: Self) {
        self.0.append(command.0);
    }

    pub(crate) fn exec_sinks(&mut self) -> ExecEffectSinks<'_> {
        let EffectQueue {
            watch_dirty,
            wakeup_only,
            replication_only,
            aof_only,
            ..
        } = &mut self.0;
        ExecEffectSinks {
            watch_dirty,
            wakeup_only,
            replication_only,
            aof_only,
        }
    }

    pub(crate) fn push_command_reply(&mut self, frame: Vec<u8>) {
        self.command_reply_bytes = self.command_reply_bytes.saturating_add(frame.len());
        self.replies.push(frame);
    }

    pub(crate) fn attribute_replies(&mut self, start: usize, end: usize) {
        self.command_reply_bytes = self.command_reply_bytes.saturating_add(
            self.replies[start.min(self.replies.len())..end.min(self.replies.len())]
                .iter()
                .map(Vec::len)
                .sum::<usize>(),
        );
    }

    pub(crate) fn take_command_reply_bytes(&mut self) -> usize {
        std::mem::take(&mut self.command_reply_bytes)
    }

    pub(crate) fn defer_slot_metric(&mut self, metric: DeferredSlotMetric) {
        debug_assert!(self.deferred_slot_metric.is_none());
        self.deferred_slot_metric = Some(metric);
    }

    pub(crate) fn take_deferred_slot_metric(&mut self) -> Option<DeferredSlotMetric> {
        self.deferred_slot_metric.take()
    }

    pub(crate) fn clear_replies(&mut self) {
        self.replies.clear();
        self.command_reply_bytes = 0;
    }

    #[cfg(test)]
    pub(crate) fn replies(&self) -> &[Vec<u8>] {
        &self.replies
    }

    pub(crate) fn into_exec_effects(self) -> ExecCollectedEffects {
        ExecCollectedEffects {
            replies: self.0.replies,
            watch_dirty: self.0.watch_dirty,
            wakeup_only: self.0.wakeup_only,
            replication_only: self.0.replication_only,
            aof_only: self.0.aof_only,
            publish: self.0.publish,
            publish_shard: self.0.publish_shard,
        }
    }

    pub(crate) fn take_watch_dirty(&mut self) -> Vec<(u32, Bytes)> {
        std::mem::take(&mut self.watch_dirty)
    }

    pub(crate) fn take_wakeup_only(&mut self) -> Vec<(u32, Bytes)> {
        std::mem::take(&mut self.wakeup_only)
    }
}

pub(crate) struct ExecEffectSinks<'a> {
    pub(crate) watch_dirty: &'a mut Vec<(u32, Bytes)>,
    pub(crate) wakeup_only: &'a mut Vec<(u32, Bytes)>,
    pub(crate) replication_only: &'a mut Vec<Vec<u8>>,
    pub(crate) aof_only: &'a mut Vec<Vec<u8>>,
}

pub(crate) struct ExecCollectedEffects {
    pub(crate) replies: Vec<Vec<u8>>,
    pub(crate) watch_dirty: Vec<(u32, Bytes)>,
    pub(crate) wakeup_only: Vec<(u32, Bytes)>,
    pub(crate) replication_only: Vec<Vec<u8>>,
    pub(crate) aof_only: Vec<Vec<u8>>,
    pub(crate) publish: Vec<(Bytes, Bytes)>,
    pub(crate) publish_shard: Vec<(Bytes, Bytes)>,
}

impl Deref for CommandEffects {
    type Target = EffectQueue;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for CommandEffects {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Default)]
pub(crate) struct ReadBatchEffects(pub(super) EffectQueue);

impl ReadBatchEffects {
    pub(super) fn commit(&mut self, command: CommandEffects) {
        self.0.append(command.0);
    }
}

impl Deref for ReadBatchEffects {
    type Target = EffectQueue;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ReadBatchEffects {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(test)]
mod tests {
    use super::{CommandEffects, ReadBatchEffects};

    #[test]
    fn read_batch_commits_each_command_in_order() {
        let mut batch = ReadBatchEffects::default();
        let mut first = CommandEffects::default();
        first.replies.push(b"first".to_vec());
        first.replication_only.push(b"repl-first".to_vec());
        let mut second = CommandEffects::default();
        second.replies.push(b"second".to_vec());
        second.replication_only.push(b"repl-second".to_vec());
        second.mark_closing = true;

        batch.commit(first);
        batch.commit(second);

        assert_eq!(batch.replies, [b"first".to_vec(), b"second".to_vec()]);
        assert_eq!(
            batch.replication_only,
            [b"repl-first".to_vec(), b"repl-second".to_vec()]
        );
        assert!(batch.mark_closing);
    }
}
