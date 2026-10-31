use super::groups::{
    RestoredStreamGroup, StreamConsumer, StreamGroup, StreamGroupCreateError, StreamPendingEntry,
};
use super::{StreamID, ValkeyStream};
use std::collections::BTreeSet;

impl ValkeyStream {
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
}
