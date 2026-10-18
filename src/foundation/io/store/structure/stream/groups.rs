use super::{StreamEntry, StreamID, ValkeyStream};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};

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
    fn new(name: Bytes, last_id: StreamID, entries_read: Option<u64>) -> Self {
        Self {
            name,
            last_id,
            entries_read,
            consumers: BTreeMap::new(),
            pending: BTreeMap::new(),
        }
    }

    fn touch_consumer(&mut self, name: &[u8], now_ms: i64) -> bool {
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
    pub consumer_created: bool,
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
        group.entries_read = entries_read;
        Ok(())
    }

    pub fn group(&self, name: &[u8]) -> Option<&StreamGroup> {
        self.groups.get(name)
    }

    pub fn groups(&self) -> impl Iterator<Item = &StreamGroup> {
        self.groups.values()
    }

    pub(crate) fn restore_group(
        &mut self,
        restored: RestoredStreamGroup,
    ) -> Result<(), StreamGroupCreateError> {
        if self.groups.contains_key(&restored.name) {
            return Err(StreamGroupCreateError::BusyGroup);
        }
        let mut group = StreamGroup::new(
            restored.name.clone(),
            restored.last_id,
            restored.entries_read,
        );
        for consumer in restored.consumers {
            if group.consumers.contains_key(&consumer.name) {
                return Err(StreamGroupCreateError::BusyGroup);
            }
            let pending: BTreeSet<StreamID> = consumer.pending.into_iter().collect();
            group.consumers.insert(
                consumer.name.clone(),
                StreamConsumer {
                    name: consumer.name,
                    seen_time_ms: consumer.seen_time_ms,
                    active_time_ms: consumer.active_time_ms,
                    pending,
                },
            );
        }
        for pending in restored.pending {
            if !group.consumers.contains_key(&pending.consumer) {
                return Err(StreamGroupCreateError::BusyGroup);
            }
            if !group
                .consumers
                .get(&pending.consumer)
                .is_some_and(|consumer| consumer.pending.contains(&pending.id))
            {
                return Err(StreamGroupCreateError::BusyGroup);
            }
            if group
                .pending
                .insert(
                    pending.id,
                    StreamPendingEntry {
                        consumer: pending.consumer,
                        delivery_time_ms: pending.delivery_time_ms,
                        delivery_count: pending.delivery_count,
                    },
                )
                .is_some()
            {
                return Err(StreamGroupCreateError::BusyGroup);
            }
        }
        let pending_ids: BTreeSet<StreamID> = group.pending.keys().copied().collect();
        for consumer in group.consumers.values() {
            if !consumer.pending.is_subset(&pending_ids) {
                return Err(StreamGroupCreateError::BusyGroup);
            }
        }
        self.groups.insert(restored.name, group);
        Ok(())
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
        let start_id = self
            .groups
            .get(group_name)
            .ok_or(StreamGroupLookupError::NoGroup)?
            .last_id;
        let entries = self.range_from_start(&start_id, count);
        let entries_added = self.entries_added;
        let max_deleted = self.max_deleted_entry_id;
        let stream_last_id = self.last_id;
        let known_len = self.entries.len();

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

        let last_id = entries.last().map(|entry| entry.id).unwrap_or(start_id);
        group.last_id = last_id;
        group.mark_consumer_active(consumer_name, now_ms);
        group.entries_read = calculate_entries_read(
            entries_added,
            known_len,
            max_deleted,
            stream_last_id,
            last_id,
            self.entries.keys().filter(|id| **id <= last_id).count(),
        );

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
            let Some(pending) = group.pending.remove(id) else {
                continue;
            };
            if let Some(consumer) = group.consumers.get_mut(&pending.consumer) {
                consumer.pending.remove(id);
            }
            acked += 1;
        }
        Ok(acked)
    }

    pub fn claim_group_pending(
        &mut self,
        group_name: &[u8],
        consumer_name: &[u8],
        min_idle_ms: i64,
        ids: &[StreamID],
        just_id: bool,
        force: bool,
        delivery_time_ms: Option<i64>,
        retry_count: Option<u64>,
        last_id: Option<StreamID>,
        now_ms: i64,
    ) -> Result<ClaimGroupResult, StreamGroupLookupError> {
        let group = self
            .groups
            .get_mut(group_name)
            .ok_or(StreamGroupLookupError::NoGroup)?;
        let consumer_created = group.touch_consumer(consumer_name, now_ms);
        if let Some(last_id) = last_id {
            group.last_id = last_id;
        }
        let mut out = Vec::new();
        for id in ids {
            let existing_pending = match group.pending.get(id).cloned() {
                Some(pending) => pending,
                None if force => StreamPendingEntry {
                    consumer: Bytes::copy_from_slice(consumer_name),
                    delivery_time_ms: delivery_time_ms.unwrap_or(now_ms),
                    delivery_count: retry_count.unwrap_or(1),
                },
                None => continue,
            };
            let idle_ms = now_ms.saturating_sub(existing_pending.delivery_time_ms);
            if idle_ms < min_idle_ms {
                continue;
            }
            let fields = self.entries.get(id).cloned();
            if let Some(old_consumer) = group.consumers.get_mut(&existing_pending.consumer) {
                old_consumer.pending.remove(id);
            }
            if let Some(new_consumer) = group.consumers.get_mut(consumer_name) {
                new_consumer.pending.insert(*id);
                new_consumer.active_time_ms = Some(now_ms);
            }
            let delivery_count = if let Some(retry_count) = retry_count {
                retry_count
            } else if just_id {
                existing_pending.delivery_count
            } else {
                existing_pending.delivery_count.saturating_add(1)
            };
            group.pending.insert(
                *id,
                StreamPendingEntry {
                    consumer: Bytes::copy_from_slice(consumer_name),
                    delivery_time_ms: delivery_time_ms.unwrap_or(now_ms),
                    delivery_count,
                },
            );
            if let Some(fields) = fields {
                out.push(ClaimedEntry::Entry(StreamEntry { id: *id, fields }));
            }
        }
        Ok(ClaimGroupResult {
            claimed: out,
            consumer_created,
        })
    }

    pub fn autoclaim_group_pending(
        &mut self,
        group_name: &[u8],
        consumer_name: &[u8],
        min_idle_ms: i64,
        start: StreamID,
        count: usize,
        just_id: bool,
        now_ms: i64,
    ) -> Result<(StreamID, Vec<ClaimedEntry>, Vec<StreamID>, bool), StreamGroupLookupError> {
        let ids: Vec<StreamID> = self
            .groups
            .get(group_name)
            .ok_or(StreamGroupLookupError::NoGroup)?
            .pending
            .keys()
            .copied()
            .filter(|id| *id >= start)
            .take(count)
            .collect();
        let next = ids
            .last()
            .and_then(|last_seen| {
                self.groups
                    .get(group_name)
                    .and_then(|group| group.pending.keys().copied().find(|id| id > last_seen))
            })
            .unwrap_or_else(StreamID::zero);

        let group = self
            .groups
            .get_mut(group_name)
            .ok_or(StreamGroupLookupError::NoGroup)?;
        let consumer_created = group.touch_consumer(consumer_name, now_ms);

        let mut claimed = Vec::new();
        let mut deleted = Vec::new();
        for id in ids {
            let Some(existing_pending) = group.pending.get(&id).cloned() else {
                continue;
            };
            let idle_ms = now_ms.saturating_sub(existing_pending.delivery_time_ms);
            if idle_ms < min_idle_ms {
                continue;
            }
            let fields = self.entries.get(&id).cloned();
            if fields.is_none() {
                if let Some(old_consumer) = group.consumers.get_mut(&existing_pending.consumer) {
                    old_consumer.pending.remove(&id);
                }
                group.pending.remove(&id);
                deleted.push(id);
                continue;
            }
            if let Some(old_consumer) = group.consumers.get_mut(&existing_pending.consumer) {
                old_consumer.pending.remove(&id);
            }
            if let Some(new_consumer) = group.consumers.get_mut(consumer_name) {
                new_consumer.pending.insert(id);
                new_consumer.active_time_ms = Some(now_ms);
            }
            group.pending.insert(
                id,
                StreamPendingEntry {
                    consumer: Bytes::copy_from_slice(consumer_name),
                    delivery_time_ms: now_ms,
                    delivery_count: if just_id {
                        existing_pending.delivery_count
                    } else {
                        existing_pending.delivery_count.saturating_add(1)
                    },
                },
            );
            claimed.push(ClaimedEntry::Entry(StreamEntry {
                id,
                fields: fields.unwrap_or_default(),
            }));
        }
        Ok((next, claimed, deleted, consumer_created))
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

fn calculate_entries_read(
    entries_added: u64,
    known_len: usize,
    max_deleted: StreamID,
    stream_last_id: StreamID,
    group_last_id: StreamID,
    known_read: usize,
) -> Option<u64> {
    if group_last_id == stream_last_id {
        return Some(entries_added);
    }
    if max_deleted != StreamID::zero() {
        return None;
    }
    Some(known_read.min(known_len).min(entries_added as usize) as u64)
}
