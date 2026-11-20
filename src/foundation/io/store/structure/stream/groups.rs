use super::{StreamEntry, StreamID, ValkeyStream};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};

/// The native stream format reserves the all-ones counter for unknown reads.
pub(crate) fn valid_entries_read(read: u64) -> Option<u64> {
    (read != u64::MAX).then_some(read)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamConsumer {
    pub name: Bytes,
    pub seen_time_ms: i64,
    pub active_time_ms: Option<i64>,
    pub pending: BTreeSet<StreamID>,
}

impl StreamConsumer {
    fn new(name: Bytes, now_ms: i64) -> Self {
        Self {
            name,
            seen_time_ms: now_ms,
            active_time_ms: None,
            pending: BTreeSet::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamPendingEntry {
    pub consumer: Bytes,
    pub delivery_time_ms: i64,
    pub delivery_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamGroup {
    pub name: Bytes,
    pub last_id: StreamID,
    pub entries_read: Option<u64>,
    pub consumers: BTreeMap<Bytes, StreamConsumer>,
    pub pending: BTreeMap<StreamID, StreamPendingEntry>,
}

impl StreamGroup {
    pub(super) fn new(name: Bytes, last_id: StreamID, entries_read: Option<u64>) -> Self {
        Self {
            name,
            last_id,
            entries_read: entries_read.and_then(valid_entries_read),
            consumers: BTreeMap::new(),
            pending: BTreeMap::new(),
        }
    }

    pub(super) fn touch_consumer(&mut self, name: &[u8], now_ms: i64) -> bool {
        let key = Bytes::copy_from_slice(name);
        let is_new = !self.consumers.contains_key(&key);
        self.consumers
            .entry(key.clone())
            .or_insert_with(|| StreamConsumer::new(key, now_ms))
            .seen_time_ms = now_ms;
        is_new
    }

    fn mark_consumer_active(&mut self, name: &[u8], now_ms: i64) {
        if let Some(consumer) = self.consumers.get_mut(name) {
            consumer.active_time_ms = Some(now_ms);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamGroupCreateError {
    BusyGroup,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamGroupLookupError {
    NoGroup,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingInfo {
    pub id: StreamID,
    pub consumer: Bytes,
    pub idle_ms: i64,
    pub delivery_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingSummary {
    pub total: usize,
    pub first_id: Option<StreamID>,
    pub last_id: Option<StreamID>,
    pub consumers: Vec<(Bytes, usize)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingRange {
    pub start: StreamID,
    pub end: StreamID,
    pub start_exclusive: bool,
    pub end_exclusive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimedEntry {
    Entry(StreamEntry),
    Deleted(StreamID),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadGroupNewResult {
    pub entries: Vec<StreamEntry>,
    pub consumer_created: bool,
    pub last_id: StreamID,
    pub entries_read: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadGroupHistoryResult {
    pub entries: Vec<(StreamID, Option<Vec<(Bytes, Bytes)>>)>,
    pub consumer_created: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimGroupResult {
    pub claimed: Vec<ClaimedEntry>,
    pub deleted: Vec<StreamID>,
    pub pending_updates: Vec<(StreamID, StreamPendingEntry)>,
    pub consumer_created: bool,
    pub last_id_changed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoredStreamConsumer {
    pub name: Bytes,
    pub seen_time_ms: i64,
    pub active_time_ms: Option<i64>,
    pub pending: Vec<StreamID>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoredStreamPendingEntry {
    pub id: StreamID,
    pub consumer: Bytes,
    pub delivery_time_ms: i64,
    pub delivery_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoredStreamGroup {
    pub name: Bytes,
    pub last_id: StreamID,
    pub entries_read: Option<u64>,
    pub consumers: Vec<RestoredStreamConsumer>,
    pub pending: Vec<RestoredStreamPendingEntry>,
}

impl ValkeyStream {
    pub fn create_group(
        &mut self,
        name: Bytes,
        last_id: StreamID,
        entries_read: Option<u64>,
    ) -> Result<(), StreamGroupCreateError> {
        if self.groups.contains_key(&name) {
            return Err(StreamGroupCreateError::BusyGroup);
        }
        self.groups
            .insert(name.clone(), StreamGroup::new(name, last_id, entries_read));
        Ok(())
    }

    pub fn destroy_group(&mut self, name: &[u8]) -> bool {
        self.groups.remove(name).is_some()
    }

    pub fn set_group_id(
        &mut self,
        name: &[u8],
        last_id: StreamID,
        entries_read: Option<u64>,
    ) -> Result<(), StreamGroupLookupError> {
        let group = self
            .groups
            .get_mut(name)
            .ok_or(StreamGroupLookupError::NoGroup)?;
        group.last_id = last_id;
        group.entries_read = entries_read.and_then(valid_entries_read);
        Ok(())
    }

    pub fn group(&self, name: &[u8]) -> Option<&StreamGroup> {
        self.groups.get(name)
    }

    pub fn groups(&self) -> impl Iterator<Item = &StreamGroup> {
        self.groups.values()
    }

    pub fn create_consumer(
        &mut self,
        group_name: &[u8],
        consumer_name: &[u8],
        now_ms: i64,
    ) -> Result<bool, StreamGroupLookupError> {
        let group = self
            .groups
            .get_mut(group_name)
            .ok_or(StreamGroupLookupError::NoGroup)?;
        Ok(group.touch_consumer(consumer_name, now_ms))
    }

    pub fn delete_consumer(
        &mut self,
        group_name: &[u8],
        consumer_name: &[u8],
    ) -> Result<usize, StreamGroupLookupError> {
        let group = self
            .groups
            .get_mut(group_name)
            .ok_or(StreamGroupLookupError::NoGroup)?;
        let Some(consumer) = group.consumers.remove(consumer_name) else {
            return Ok(0);
        };
        let removed = consumer.pending.len();
        for id in consumer.pending {
            group.pending.remove(&id);
        }
        Ok(removed)
    }

    pub fn read_group_new(
        &mut self,
        group_name: &[u8],
        consumer_name: &[u8],
        count: usize,
        noack: bool,
        now_ms: i64,
    ) -> Result<ReadGroupNewResult, StreamGroupLookupError> {
        let group = self
            .groups
            .get(group_name)
            .ok_or(StreamGroupLookupError::NoGroup)?;
        let start_id = group.last_id;
        let mut last_id = start_id;
        let mut entries_read = group.entries_read.and_then(valid_entries_read);
        let entries = self.range_from_start(&start_id, count);
        let first_id = self.first_id();
        // Advance the logical counter using each delivered entry's previous cursor.
        for entry in &entries {
            if entry.id <= last_id {
                continue;
            }
            if entries_read.is_some()
                && first_id.is_some_and(|first| last_id >= first)
                && !self.range_has_tombstones(Some(last_id), None)
            {
                entries_read =
                    entries_read.and_then(|read| valid_entries_read(read.wrapping_add(1)));
            } else if self.entries_added != 0 {
                entries_read = self.estimate_distance_from_first_ever_entry(entry.id);
            }
            last_id = entry.id;
        }

        let group = self
            .groups
            .get_mut(group_name)
            .ok_or(StreamGroupLookupError::NoGroup)?;
        let consumer_created = group.touch_consumer(consumer_name, now_ms);
        if entries.is_empty() {
            return Ok(ReadGroupNewResult {
                entries,
                consumer_created,
                last_id: group.last_id,
                entries_read: group.entries_read,
            });
        }

        group.last_id = last_id;
        group.mark_consumer_active(consumer_name, now_ms);
        group.entries_read = entries_read;

        if !noack {
            for entry in &entries {
                add_pending_to_group(group, consumer_name, entry.id, now_ms, 1);
            }
        }
        Ok(ReadGroupNewResult {
            entries,
            consumer_created,
            last_id: group.last_id,
            entries_read: group.entries_read,
        })
    }

    pub fn read_group_history(
        &mut self,
        group_name: &[u8],
        consumer_name: &[u8],
        start_id: StreamID,
        count: usize,
        now_ms: i64,
    ) -> Result<ReadGroupHistoryResult, StreamGroupLookupError> {
        let group = self
            .groups
            .get_mut(group_name)
            .ok_or(StreamGroupLookupError::NoGroup)?;
        let consumer_created = group.touch_consumer(consumer_name, now_ms);
        let mut out = Vec::new();
        let ids: Vec<StreamID> = group
            .consumers
            .get(consumer_name)
            .map(|consumer| consumer.pending.iter().copied().collect())
            .unwrap_or_default();
        for id in ids {
            if id <= start_id {
                continue;
            }
            let fields = self.entries.get(&id).cloned();
            if fields.is_some() {
                if let Some(pending) = group.pending.get_mut(&id) {
                    pending.delivery_time_ms = now_ms;
                    pending.delivery_count = pending.delivery_count.saturating_add(1);
                }
            }
            out.push((id, fields));
            if out.len() >= count {
                break;
            }
        }
        Ok(ReadGroupHistoryResult {
            entries: out,
            consumer_created,
        })
    }

    pub fn pending_summary(
        &self,
        group_name: &[u8],
    ) -> Result<PendingSummary, StreamGroupLookupError> {
        let group = self
            .groups
            .get(group_name)
            .ok_or(StreamGroupLookupError::NoGroup)?;
        let mut consumers = Vec::new();
        for consumer in group.consumers.values() {
            if !consumer.pending.is_empty() {
                consumers.push((consumer.name.clone(), consumer.pending.len()));
            }
        }
        Ok(PendingSummary {
            total: group.pending.len(),
            first_id: group.pending.keys().next().copied(),
            last_id: group.pending.keys().next_back().copied(),
            consumers,
        })
    }

    pub fn pending_range(
        &self,
        group_name: &[u8],
        range: PendingRange,
        count: usize,
        consumer_name: Option<&[u8]>,
        min_idle_ms: Option<i64>,
        now_ms: i64,
    ) -> Result<Vec<PendingInfo>, StreamGroupLookupError> {
        let group = self
            .groups
            .get(group_name)
            .ok_or(StreamGroupLookupError::NoGroup)?;
        let mut out = Vec::new();
        for (id, pending) in group.pending.range(range.start..=range.end) {
            if range.start_exclusive && *id == range.start {
                continue;
            }
            if range.end_exclusive && *id == range.end {
                continue;
            }
            if consumer_name.is_some_and(|name| pending.consumer.as_ref() != name) {
                continue;
            }
            let idle_ms = now_ms.saturating_sub(pending.delivery_time_ms);
            if min_idle_ms.is_some_and(|min| idle_ms < min) {
                continue;
            }
            out.push(PendingInfo {
                id: *id,
                consumer: pending.consumer.clone(),
                idle_ms,
                delivery_count: pending.delivery_count,
            });
            if out.len() >= count {
                break;
            }
        }
        Ok(out)
    }

    pub fn ack_group_pending(
        &mut self,
        group_name: &[u8],
        ids: &[StreamID],
    ) -> Result<usize, StreamGroupLookupError> {
        let group = self
            .groups
            .get_mut(group_name)
            .ok_or(StreamGroupLookupError::NoGroup)?;
        let mut acked = 0;
        for id in ids {
            if super::references::remove_group_pending(group, id) {
                acked += 1;
            }
        }
        Ok(acked)
    }
}

fn add_pending_to_group(
    group: &mut StreamGroup,
    consumer_name: &[u8],
    id: StreamID,
    now_ms: i64,
    delivery_count: u64,
) {
    let consumer_key = Bytes::copy_from_slice(consumer_name);
    if let Some(consumer) = group.consumers.get_mut(&consumer_key) {
        consumer.pending.insert(id);
    }
    group.pending.insert(
        id,
        StreamPendingEntry {
            consumer: consumer_key,
            delivery_time_ms: now_ms,
            delivery_count,
        },
    );
}
