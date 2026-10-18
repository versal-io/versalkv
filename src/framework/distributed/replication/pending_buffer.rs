use std::collections::VecDeque;

use crate::engine::distributed::replication::ReplicationOffset;

use super::{ReplicaEvent, ReplicationApply};

const ENTRY_OVERHEAD: usize = std::mem::size_of::<PendingEntry>();

pub struct PendingEntry {
    pub event: ReplicaEvent,
    storage_bytes: usize,
}

impl PendingEntry {
    pub fn storage_bytes(&self) -> usize {
        self.storage_bytes
    }

    pub fn end_offset(&self) -> ReplicationOffset {
        let ReplicaEvent::Apply { end_offset, .. } = &self.event;
        *end_offset
    }
}

pub struct PendingReplicationBuffer {
    inner: VecDeque<PendingEntry>,
    max_bytes: usize,
    used_bytes: usize,
    snapshot_end_offset: ReplicationOffset,
    selected_db: u32,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PushError {
    BeforeSnapshot {
        offset: ReplicationOffset,
        snapshot: ReplicationOffset,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub enum PushOutcome {
    /// The entry was retained without crossing the configured byte limit.
    Buffered,
    /// The entry was retained after the configured byte limit was crossed.
    BufferedAndLimitReached { used: usize, limit: usize },
}

impl PushOutcome {
    pub fn limit_reached(&self) -> Option<(usize, usize)> {
        match self {
            Self::Buffered => None,
            Self::BufferedAndLimitReached { used, limit } => Some((*used, *limit)),
        }
    }
}

impl PendingReplicationBuffer {
    pub fn new(max_bytes: usize, snapshot_end_offset: ReplicationOffset, selected_db: u32) -> Self {
        Self {
            inner: VecDeque::new(),
            max_bytes,
            used_bytes: 0,
            snapshot_end_offset,
            selected_db,
        }
    }

    /// Retains every event before reporting whether the byte limit was reached.
    /// Once reached, the caller may finish the already-decoded batch but must
    /// stop reading another socket batch, bounding overshoot to admitted input.
    pub fn push(&mut self, event: ReplicaEvent) -> Result<PushOutcome, PushError> {
        let end_offset = match &event {
            ReplicaEvent::Apply { end_offset, .. } => *end_offset,
        };
        if end_offset <= self.snapshot_end_offset {
            return Err(PushError::BeforeSnapshot {
                offset: end_offset,
                snapshot: self.snapshot_end_offset,
            });
        }
        let storage_bytes = replica_event_storage_bytes(&event);
        let needed = self.used_bytes.saturating_add(storage_bytes);
        self.inner.push_back(PendingEntry {
            event,
            storage_bytes,
        });
        self.used_bytes = needed;
        if needed > self.max_bytes {
            Ok(PushOutcome::BufferedAndLimitReached {
                used: needed,
                limit: self.max_bytes,
            })
        } else {
            Ok(PushOutcome::Buffered)
        }
    }

    #[cfg(test)]
    pub fn is_full(&self) -> bool {
        self.used_bytes >= self.max_bytes
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn used_bytes(&self) -> usize {
        self.used_bytes
    }

    #[cfg(test)]
    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    #[cfg(test)]
    pub fn snapshot_end_offset(&self) -> ReplicationOffset {
        self.snapshot_end_offset
    }

    pub fn selected_db(&self) -> u32 {
        self.selected_db
    }

    pub fn pop_front(&mut self) -> Option<PendingEntry> {
        let entry = self.inner.pop_front()?;
        self.used_bytes -= entry.storage_bytes;
        Some(entry)
    }

    #[cfg(test)]
    pub fn clear(&mut self) {
        self.inner.clear();
        self.used_bytes = 0;
    }
}

fn replica_event_storage_bytes(event: &ReplicaEvent) -> usize {
    let ReplicaEvent::Apply { apply, .. } = event;
    match apply {
        ReplicationApply::Command(command) => {
            let payload_bytes = command.args.iter().fold(command.name.len(), |total, arg| {
                total.saturating_add(arg.len())
            });
            ENTRY_OVERHEAD.saturating_add(payload_bytes).saturating_add(
                command
                    .args
                    .capacity()
                    .saturating_mul(std::mem::size_of::<bytes::Bytes>()),
            )
        }
        ReplicationApply::Select(_) | ReplicationApply::Ping | ReplicationApply::GetAck => {
            ENTRY_OVERHEAD
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn offset(value: i64) -> ReplicationOffset {
        ReplicationOffset::try_from(value).expect("valid test offset")
    }

    fn make_apply_event(end_offset: i64) -> ReplicaEvent {
        ReplicaEvent::Apply {
            apply: crate::framework::distributed::replication::ReplicationApply::Ping,
            end_offset: ReplicationOffset::try_from(end_offset)
                .expect("test event offset must be non-negative"),
        }
    }

    #[test]
    fn crossing_entry_is_retained_in_fifo_and_reports_the_limit() {
        let mut buf = PendingReplicationBuffer::new(ENTRY_OVERHEAD * 3, offset(100), 0);
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.used_bytes(), 0);

        assert_eq!(buf.push(make_apply_event(101)), Ok(PushOutcome::Buffered));
        assert_eq!(buf.push(make_apply_event(102)), Ok(PushOutcome::Buffered));
        assert_eq!(buf.push(make_apply_event(103)), Ok(PushOutcome::Buffered));

        assert_eq!(buf.len(), 3);
        assert_eq!(buf.used_bytes(), 3 * ENTRY_OVERHEAD);

        assert!(buf.is_full());

        let result = buf.push(make_apply_event(104));
        assert_eq!(
            result,
            Ok(PushOutcome::BufferedAndLimitReached {
                used: 4 * ENTRY_OVERHEAD,
                limit: 3 * ENTRY_OVERHEAD,
            })
        );
        assert_eq!(buf.len(), 4);
        assert_eq!(buf.used_bytes(), 4 * ENTRY_OVERHEAD);

        let offsets: Vec<i64> = std::iter::from_fn(|| buf.pop_front())
            .map(|entry| entry.end_offset().get())
            .collect();
        assert_eq!(offsets, vec![101, 102, 103, 104]);
    }

    #[test]
    fn push_accounts_for_command_payload_bytes() {
        let payload = Bytes::from(vec![b'x'; 1_024]);
        let event = ReplicaEvent::Apply {
            apply: crate::framework::distributed::replication::ReplicationApply::Command(
                crate::engine::command::CommandArgs::from_parts(vec![
                    Bytes::from_static(b"SET"),
                    Bytes::from_static(b"k"),
                    payload,
                ]),
            ),
            end_offset: ReplicationOffset::try_from(101).unwrap(),
        };
        let mut buf = PendingReplicationBuffer::new(usize::MAX, offset(100), 0);

        buf.push(event).unwrap();

        assert!(
            buf.used_bytes() >= 1_024,
            "the pending buffer must account for retained command payloads"
        );
    }

    #[test]
    fn clear_resets_count_not_metadata() {
        let mut buf = PendingReplicationBuffer::new(ENTRY_OVERHEAD * 5, offset(200), 7);
        buf.push(make_apply_event(201)).unwrap();
        buf.push(make_apply_event(202)).unwrap();

        assert_eq!(buf.len(), 2);
        assert_eq!(buf.used_bytes(), 2 * ENTRY_OVERHEAD);

        buf.clear();

        assert_eq!(buf.len(), 0);
        assert_eq!(buf.used_bytes(), 0);
        assert!(buf.is_empty());
        assert!(!buf.is_full());

        assert_eq!(buf.snapshot_end_offset().get(), 200);
        assert_eq!(buf.selected_db(), 7);
    }

    #[test]
    fn before_snapshot_rejected() {
        let mut buf = PendingReplicationBuffer::new(ENTRY_OVERHEAD * 10, offset(100), 0);

        let result = buf.push(make_apply_event(100));
        assert_eq!(
            result,
            Err(PushError::BeforeSnapshot {
                offset: ReplicationOffset::try_from(100).unwrap(),
                snapshot: ReplicationOffset::try_from(100).unwrap(),
            })
        );

        let result = buf.push(make_apply_event(99));
        assert_eq!(
            result,
            Err(PushError::BeforeSnapshot {
                offset: ReplicationOffset::try_from(99).unwrap(),
                snapshot: ReplicationOffset::try_from(100).unwrap(),
            })
        );

        assert!(buf.push(make_apply_event(101)).is_ok());
    }

    #[test]
    fn pending_buffer_endpoints_use_replication_offsets() {
        let mut buf = PendingReplicationBuffer::new(ENTRY_OVERHEAD * 2, offset(100), 0);
        buf.push(make_apply_event(101)).unwrap();
        let entry = buf.pop_front().unwrap();
        let ReplicaEvent::Apply { end_offset, .. } = &entry.event;
        let expected =
            std::any::type_name::<crate::engine::distributed::replication::ReplicationOffset>();

        assert_eq!(
            std::any::type_name_of_val(&buf.snapshot_end_offset),
            expected
        );
        assert_eq!(std::any::type_name_of_val(end_offset), expected);
        assert_eq!(std::any::type_name_of_val(&entry.end_offset()), expected);
    }

    #[test]
    fn pending_buffer_pop_front_preserves_cost() {
        let mut buf = PendingReplicationBuffer::new(ENTRY_OVERHEAD * 5, offset(100), 3);
        buf.push(make_apply_event(101)).unwrap();
        buf.push(make_apply_event(102)).unwrap();

        let before = buf.used_bytes();
        let entry = buf.pop_front().unwrap();

        assert_eq!(entry.end_offset().get(), 101);
        assert_eq!(buf.used_bytes(), before - entry.storage_bytes());
        assert_eq!(buf.len(), 1);
        assert_eq!(buf.pop_front().unwrap().end_offset().get(), 102);
        assert!(buf.pop_front().is_none());
        assert_eq!(buf.used_bytes(), 0);
    }
    #[test]
    fn max_bytes_zero_retains_one_crossing_entry() {
        let mut buf = PendingReplicationBuffer::new(0, offset(0), 0);

        assert!(buf.is_full());
        assert!(buf.is_empty());

        let result = buf.push(make_apply_event(1));
        assert_eq!(
            result,
            Ok(PushOutcome::BufferedAndLimitReached {
                used: ENTRY_OVERHEAD,
                limit: 0,
            })
        );
        assert_eq!(buf.len(), 1);
        assert_eq!(buf.used_bytes(), ENTRY_OVERHEAD);
    }
}
