use super::groups::{ClaimGroupResult, ClaimedEntry, StreamGroupLookupError, StreamPendingEntry};
use super::{StreamEntry, StreamID, ValkeyStream};
use bytes::Bytes;

impl ValkeyStream {
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
        let last_id = last_id.filter(|id| *id > group.last_id);
        let last_id_changed = last_id.is_some();
        if let Some(last_id) = last_id {
            group.last_id = last_id;
        }
        let delivery_time_ms = delivery_time_ms
            .filter(|time| *time >= 0 && *time <= now_ms)
            .unwrap_or(now_ms);
        let mut out = Vec::new();
        let mut deleted = Vec::new();
        let mut pending_updates = Vec::new();
        for id in ids {
            let Some(fields) = self.entries.get(id).cloned() else {
                if let Some(pending) = group.pending.remove(id) {
                    if let Some(old_consumer) = group.consumers.get_mut(&pending.consumer) {
                        old_consumer.pending.remove(id);
                    }
                    pending_updates.push((*id, pending));
                    deleted.push(*id);
                }
                continue;
            };
            let has_existing_pending = group.pending.contains_key(id);
            let existing_pending = match group.pending.get(id).cloned() {
                Some(pending) => pending,
                None if force => StreamPendingEntry {
                    consumer: Bytes::copy_from_slice(consumer_name),
                    delivery_time_ms: now_ms,
                    delivery_count: 1,
                },
                None => continue,
            };
            let idle_ms = now_ms.saturating_sub(existing_pending.delivery_time_ms);
            if has_existing_pending && idle_ms < min_idle_ms {
                continue;
            }
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
            let pending = StreamPendingEntry {
                consumer: Bytes::copy_from_slice(consumer_name),
                delivery_time_ms,
                delivery_count,
            };
            group.pending.insert(*id, pending.clone());
            pending_updates.push((*id, pending));
            out.push(ClaimedEntry::Entry(StreamEntry { id: *id, fields }));
        }
        Ok(ClaimGroupResult {
            claimed: out,
            deleted,
            pending_updates,
            consumer_created,
            last_id_changed,
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
    ) -> Result<(StreamID, ClaimGroupResult), StreamGroupLookupError> {
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
        let mut pending_updates = Vec::new();
        for id in ids {
            let Some(existing_pending) = group.pending.get(&id).cloned() else {
                continue;
            };
            let Some(fields) = self.entries.get(&id).cloned() else {
                if let Some(old_consumer) = group.consumers.get_mut(&existing_pending.consumer) {
                    old_consumer.pending.remove(&id);
                }
                group.pending.remove(&id);
                pending_updates.push((id, existing_pending));
                deleted.push(id);
                continue;
            };
            let idle_ms = now_ms.saturating_sub(existing_pending.delivery_time_ms);
            if idle_ms < min_idle_ms {
                continue;
            }
            if let Some(old_consumer) = group.consumers.get_mut(&existing_pending.consumer) {
                old_consumer.pending.remove(&id);
            }
            if let Some(new_consumer) = group.consumers.get_mut(consumer_name) {
                new_consumer.pending.insert(id);
                new_consumer.active_time_ms = Some(now_ms);
            }
            let pending = StreamPendingEntry {
                consumer: Bytes::copy_from_slice(consumer_name),
                delivery_time_ms: now_ms,
                delivery_count: if just_id {
                    existing_pending.delivery_count
                } else {
                    existing_pending.delivery_count.saturating_add(1)
                },
            };
            group.pending.insert(id, pending.clone());
            pending_updates.push((id, pending));
            claimed.push(ClaimedEntry::Entry(StreamEntry { id, fields }));
        }
        Ok((
            next,
            ClaimGroupResult {
                claimed,
                deleted,
                pending_updates,
                consumer_created,
                last_id_changed: false,
            },
        ))
    }
}
