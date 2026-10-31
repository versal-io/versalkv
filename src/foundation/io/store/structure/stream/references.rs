use super::groups::{StreamGroup, StreamGroupLookupError};
use super::{StreamID, ValkeyStream};
use bytes::Bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamReferenceMode {
    KeepRef,
    DelRef,
    Acked,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending_stream() -> ValkeyStream {
        let mut stream = ValkeyStream::new();
        for ms in 1..=3 {
            stream
                .append(StreamID::new(ms, 0), vec![], true, false, 0)
                .unwrap();
        }
        for group in [b"a".as_slice(), b"z"] {
            stream
                .create_group(Bytes::copy_from_slice(group), StreamID::zero(), None)
                .unwrap();
            stream.read_group_new(group, b"c", 3, false, 7).unwrap();
        }
        stream
    }

    #[test]
    fn reference_deletion_preserves_position_gates_and_both_pending_indexes() {
        for mode in [
            StreamReferenceMode::KeepRef,
            StreamReferenceMode::DelRef,
            StreamReferenceMode::Acked,
        ] {
            let mut stream = pending_stream();
            let before = stream.group(b"a").unwrap().consumers[b"c".as_slice()].clone();
            let id = StreamID::new(1, 0);
            let result = stream
                .delete_with_references(Some(b"a"), &[id, id], mode)
                .unwrap();
            assert_eq!(
                result.statuses,
                if mode == StreamReferenceMode::Acked {
                    vec![2, -1]
                } else {
                    vec![1, -1]
                }
            );
            let group = stream.group(b"a").unwrap();
            assert!(!group.pending.contains_key(&id));
            assert!(!group.consumers[b"c".as_slice()].pending.contains(&id));
            assert_eq!(
                group.consumers[b"c".as_slice()].seen_time_ms,
                before.seen_time_ms
            );
            assert_eq!(
                group.consumers[b"c".as_slice()].active_time_ms,
                before.active_time_ms
            );
            assert_eq!(group.last_id, StreamID::new(3, 0));
            assert_eq!(group.entries_read, Some(3));
            let remaining = stream.group(b"z").unwrap();
            assert_eq!(
                remaining.pending.contains_key(&id),
                mode != StreamReferenceMode::DelRef
            );
            assert_eq!(
                remaining.consumers[b"c".as_slice()].pending.contains(&id),
                mode != StreamReferenceMode::DelRef
            );
            assert_eq!(
                result.changed_count(),
                match mode {
                    StreamReferenceMode::KeepRef => 2,
                    StreamReferenceMode::DelRef => 3,
                    StreamReferenceMode::Acked => 1,
                }
            );
            if mode == StreamReferenceMode::DelRef {
                assert_eq!(
                    result.acknowledgements,
                    vec![
                        (Bytes::from_static(b"z"), vec![id]),
                        (Bytes::from_static(b"a"), vec![id]),
                    ]
                );
            }
        }
    }

    #[test]
    fn acked_checks_dangling_pending_before_cursor_and_skips_target_cursor() {
        let id = StreamID::new(1, 0);
        for mode in [
            StreamReferenceMode::KeepRef,
            StreamReferenceMode::DelRef,
            StreamReferenceMode::Acked,
        ] {
            let mut stream = pending_stream();
            stream.delete(&id);
            stream.ack_group_pending(b"z", &[id]).unwrap();
            stream.set_group_id(b"z", StreamID::zero(), None).unwrap();
            let result = stream
                .delete_with_references(Some(b"a"), &[id], mode)
                .unwrap();
            assert_eq!(result.statuses, vec![1]);
            assert_eq!(result.changed_count(), 1);
        }
        let mut stream = pending_stream();
        stream.set_group_id(b"a", StreamID::zero(), None).unwrap();
        stream.ack_group_pending(b"z", &[id]).unwrap();
        assert_eq!(
            stream
                .delete_with_references(Some(b"a"), &[id], StreamReferenceMode::Acked)
                .unwrap()
                .statuses,
            vec![1]
        );
        let mut stream = pending_stream();
        stream.delete(&id);
        stream.set_group_id(b"a", StreamID::zero(), None).unwrap();
        assert_eq!(
            stream
                .delete_with_references(None, &[id, id], StreamReferenceMode::Acked)
                .unwrap()
                .statuses,
            vec![2, 2]
        );
        let result = stream
            .delete_with_references(None, &[id, id], StreamReferenceMode::DelRef)
            .unwrap();
        assert_eq!(result.statuses, vec![-1, -1]);
        assert_eq!(result.changed_count(), 2);
        assert_eq!(
            stream
                .delete_with_references(None, &[id], StreamReferenceMode::Acked)
                .unwrap()
                .statuses,
            vec![-1]
        );
        assert_eq!(
            stream.delete_with_references(Some(b"missing"), &[id], StreamReferenceMode::Acked),
            Err(StreamGroupLookupError::NoGroup)
        );
    }

    #[test]
    fn reference_deletion_keeps_empty_stream_groups_and_history() {
        let mut stream = pending_stream();
        let ids = [
            StreamID::new(1, 0),
            StreamID::new(2, 0),
            StreamID::new(3, 0),
        ];
        let result = stream
            .delete_with_references(None, &ids, StreamReferenceMode::DelRef)
            .unwrap();
        assert_eq!(result.statuses, vec![1, 1, 1]);
        assert_eq!(result.changed_count(), 9);
        assert_eq!(stream.len(), 0);
        assert_eq!(stream.groups().count(), 2);
        assert_eq!(stream.recorded_first_entry_id(), StreamID::zero());
        assert_eq!(stream.last_id, ids[2]);
        assert_eq!(stream.entries_added, 3);
        assert_eq!(stream.max_deleted_entry_id, ids[2]);
        for group in stream.groups() {
            assert!(group.pending.is_empty());
            assert!(group.consumers[b"c".as_slice()].pending.is_empty());
        }
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct StreamDeleteResult {
    pub statuses: Vec<i64>,
    pub deleted_ids: Vec<StreamID>,
    /// Other groups in name order, followed by the target group.
    pub acknowledgements: Vec<(Bytes, Vec<StreamID>)>,
}

impl StreamDeleteResult {
    pub fn changed_count(&self) -> usize {
        self.deleted_ids.len()
            + self
                .acknowledgements
                .iter()
                .map(|(_, ids)| ids.len())
                .sum::<usize>()
    }
}

pub(super) fn remove_group_pending(group: &mut StreamGroup, id: &StreamID) -> bool {
    let Some(pending) = group.pending.remove(id) else {
        return false;
    };
    if let Some(consumer) = group.consumers.get_mut(&pending.consumer) {
        consumer.pending.remove(id);
    }
    true
}

impl ValkeyStream {
    pub fn delete_with_references(
        &mut self,
        target_group: Option<&[u8]>,
        ids: &[StreamID],
        mode: StreamReferenceMode,
    ) -> Result<StreamDeleteResult, StreamGroupLookupError> {
        if target_group.is_some_and(|name| !self.groups.contains_key(name)) {
            return Err(StreamGroupLookupError::NoGroup);
        }
        let mut result = StreamDeleteResult {
            statuses: vec![1; ids.len()],
            ..StreamDeleteResult::default()
        };
        let mut target_acks = Vec::new();

        if mode == StreamReferenceMode::KeepRef {
            for (index, id) in ids.iter().enumerate() {
                if let Some(name) = target_group {
                    let group = self
                        .groups
                        .get_mut(name)
                        .ok_or(StreamGroupLookupError::NoGroup)?;
                    if !remove_group_pending(group, id) {
                        result.statuses[index] = -1;
                        continue;
                    }
                    target_acks.push(*id);
                }
                if self.delete(id) {
                    result.deleted_ids.push(*id);
                } else if target_group.is_none() {
                    result.statuses[index] = -1;
                }
            }
        } else {
            // Target eligibility is fixed before any other group is scanned.
            if let Some(name) = target_group {
                let group = self
                    .groups
                    .get_mut(name)
                    .ok_or(StreamGroupLookupError::NoGroup)?;
                for (index, id) in ids.iter().enumerate() {
                    if remove_group_pending(group, id) {
                        target_acks.push(*id);
                    } else {
                        result.statuses[index] = -1;
                    }
                }
            }
            let exists: Vec<bool> = ids.iter().map(|id| self.entries.contains_key(id)).collect();
            for (name, group) in &mut self.groups {
                if target_group == Some(name.as_ref()) {
                    continue;
                }
                let mut acked = Vec::new();
                for (index, id) in ids.iter().enumerate() {
                    if result.statuses[index] != 1 {
                        continue;
                    }
                    if mode == StreamReferenceMode::DelRef {
                        if remove_group_pending(group, id) {
                            acked.push(*id);
                        }
                    } else if group.pending.contains_key(id)
                        || (exists[index] && *id > group.last_id)
                    {
                        result.statuses[index] = 2;
                    }
                }
                if !acked.is_empty() {
                    result.acknowledgements.push((name.clone(), acked));
                }
            }
            for (index, id) in ids.iter().enumerate() {
                if result.statuses[index] != 1 {
                    continue;
                }
                if self.delete(id) {
                    result.deleted_ids.push(*id);
                } else if target_group.is_none() {
                    result.statuses[index] = -1;
                }
            }
        }
        if let Some(name) = target_group {
            if !target_acks.is_empty() {
                result
                    .acknowledgements
                    .push((Bytes::copy_from_slice(name), target_acks));
            }
        }
        Ok(result)
    }
}
