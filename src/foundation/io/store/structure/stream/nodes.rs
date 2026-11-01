use super::{StreamID, ValkeyStream};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq)]
pub(super) struct StreamNode {
    pub ids: BTreeSet<StreamID>,
    pub live: usize,
    deleted: usize,
    bytes: usize,
    fields: Vec<Bytes>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StreamNodeSnapshot {
    pub primary_id: StreamID,
    pub ids: BTreeSet<StreamID>,
    pub bytes: usize,
    pub fields: Vec<Bytes>,
}

fn integer_size(value: i64) -> usize {
    let encoded = if (0..=127).contains(&value) {
        1
    } else if (-4096..=4095).contains(&value) {
        2
    } else if i16::try_from(value).is_ok() {
        3
    } else if (-8_388_608..=8_388_607).contains(&value) {
        4
    } else if i32::try_from(value).is_ok() {
        5
    } else {
        9
    };
    encoded + 1
}

fn string_size(value: &[u8]) -> usize {
    if let Ok(text) = std::str::from_utf8(value) {
        if let Ok(number) = text.parse::<i64>() {
            if number.to_string() == text {
                return integer_size(number);
            }
        }
    }
    let header = if value.len() < 64 {
        1
    } else if value.len() < 4096 {
        2
    } else {
        5
    };
    let encoded = value.len().saturating_add(header);
    let backlen = if encoded <= 127 {
        1
    } else if encoded < 16_383 {
        2
    } else if encoded < 2_097_151 {
        3
    } else if encoded < 268_435_455 {
        4
    } else {
        5
    };
    encoded.saturating_add(backlen)
}

impl StreamNode {
    fn new(fields: &[(Bytes, Bytes)]) -> Self {
        Self {
            ids: BTreeSet::new(),
            live: 0,
            deleted: 0,
            bytes: 7
                + integer_size(0) * 3
                + integer_size(fields.len() as i64)
                + fields
                    .iter()
                    .map(|(field, _)| string_size(field))
                    .sum::<usize>(),
            fields: fields.iter().map(|(field, _)| field.clone()).collect(),
        }
    }

    fn append(&mut self, primary: StreamID, id: StreamID, fields: &[(Bytes, Bytes)]) {
        let same_fields = self.fields.len() == fields.len()
            && self
                .fields
                .iter()
                .zip(fields)
                .all(|(master, (field, _))| master == field);
        self.bytes = self.bytes - integer_size(self.live as i64)
            + integer_size((self.live + 1) as i64)
            + integer_size(0)
            + integer_size(id.ms.wrapping_sub(primary.ms) as i64)
            + integer_size(id.seq.wrapping_sub(primary.seq) as i64);
        if !same_fields {
            self.bytes += integer_size(fields.len() as i64);
        }
        for (field, value) in fields {
            if !same_fields {
                self.bytes += string_size(field);
            }
            self.bytes += string_size(value);
        }
        let count = fields.len() + 3 + if same_fields { 0 } else { fields.len() + 1 };
        self.bytes += integer_size(count as i64);
        self.live += 1;
        self.ids.insert(id);
    }

    fn snapshot(&self, primary_id: StreamID) -> StreamNodeSnapshot {
        StreamNodeSnapshot {
            primary_id,
            ids: self.ids.clone(),
            bytes: self.bytes,
            fields: self.fields.clone(),
        }
    }
}

impl ValkeyStream {
    pub fn set_append_node_limits(&mut self, entries: usize, bytes: usize) {
        self.append_node_limits = (entries, bytes);
    }

    pub fn physical_first_id(&self) -> StreamID {
        self.nodes
            .first_key_value()
            .and_then(|(_, node)| node.ids.first().copied())
            .unwrap_or(StreamID::max())
    }

    pub(crate) fn node_snapshots(&self) -> impl ExactSizeIterator<Item = StreamNodeSnapshot> + '_ {
        self.nodes
            .iter()
            .map(|(primary_id, node)| node.snapshot(*primary_id))
    }

    pub(crate) fn restore_persistence_state(
        &mut self,
        last_id: StreamID,
        entries_added: u64,
        max_deleted_entry_id: StreamID,
        recorded_first_entry_id: StreamID,
        snapshots: Vec<StreamNodeSnapshot>,
    ) -> Option<()> {
        if entries_added < self.entries.len() as u64
            || self
                .entries
                .keys()
                .next_back()
                .is_some_and(|entry_id| *entry_id > last_id)
            || max_deleted_entry_id > last_id
            || recorded_first_entry_id != self.first_id().unwrap_or(StreamID::zero())
        {
            return None;
        }

        let mut restored_nodes = BTreeMap::new();
        let mut restored_ids = BTreeSet::new();
        let mut previous_last_id = None;
        for snapshot in snapshots {
            if snapshot.ids.first().copied() != Some(snapshot.primary_id)
                || snapshot
                    .ids
                    .last()
                    .is_some_and(|node_last_id| *node_last_id > last_id)
                || previous_last_id.is_some_and(|previous| previous >= snapshot.primary_id)
            {
                return None;
            }
            if snapshot.ids.iter().any(|id| !restored_ids.insert(*id)) {
                return None;
            }

            let live = snapshot
                .ids
                .iter()
                .filter(|id| self.entries.contains_key(id))
                .count();
            let deleted = snapshot.ids.len().checked_sub(live)?;
            let encoded_live = i64::try_from(live).ok()?;
            let encoded_deleted = i64::try_from(deleted).ok()?;
            // A physical listpack carries its total length in a 32-bit header.
            // Larger claimed sizes cannot describe a valid node and would
            // overflow the header-width adjustments performed during deletion.
            if snapshot.bytes > (u32::MAX as usize).min(isize::MAX as usize)
                || snapshot.bytes < integer_size(encoded_live) + integer_size(encoded_deleted)
            {
                return None;
            }
            previous_last_id = snapshot.ids.last().copied();
            restored_nodes.insert(
                snapshot.primary_id,
                StreamNode {
                    ids: snapshot.ids,
                    live,
                    deleted,
                    bytes: snapshot.bytes,
                    fields: snapshot.fields,
                },
            );
        }
        if self
            .entries
            .keys()
            .any(|entry_id| !restored_ids.contains(entry_id))
        {
            return None;
        }

        self.last_id = last_id;
        self.entries_added = entries_added;
        self.max_deleted_entry_id = max_deleted_entry_id;
        self.recorded_first_entry_id = recorded_first_entry_id;
        self.nodes = restored_nodes;
        Some(())
    }

    pub(super) fn record_node_append(&mut self, id: StreamID, fields: &[(Bytes, Bytes)]) {
        let (entry_limit, byte_limit) = self.append_node_limits;
        let byte_limit = if byte_limit == 0 {
            1 << 30
        } else {
            byte_limit.min(1 << 30)
        };
        let payload = fields
            .iter()
            .map(|(field, value)| field.len() + value.len())
            .sum::<usize>();
        let has_room = self.nodes.last_key_value().is_some_and(|(_, node)| {
            (entry_limit == 0 || node.ids.len() < entry_limit)
                && node.bytes.saturating_add(payload) < byte_limit
        });
        if !has_room {
            self.nodes.insert(id, StreamNode::new(fields));
        }
        if let Some(primary) = self.nodes.last_key_value().map(|(primary, _)| *primary) {
            if let Some(node) = self.nodes.get_mut(&primary) {
                node.append(primary, id, fields);
            }
        }
    }

    pub(super) fn record_node_deletion(&mut self, id: &StreamID, remove_empty: bool) {
        let Some((&primary, _)) = self.nodes.range(..=id).next_back() else {
            return;
        };
        if let Some(node) = self.nodes.get_mut(&primary) {
            node.bytes = node.bytes - integer_size(node.live as i64)
                + integer_size((node.live - 1) as i64)
                - integer_size(node.deleted as i64)
                + integer_size((node.deleted + 1) as i64);
            node.live -= 1;
            node.deleted += 1;
            if remove_empty && node.live == 0 {
                self.nodes.remove(&primary);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn append(stream: &mut ValkeyStream, ms: u64) {
        stream
            .append(StreamID::new(ms, 0), vec![], true, false, 0)
            .unwrap();
    }

    #[test]
    fn approximate_trim_observes_append_time_nodes_and_hole_positions() {
        let mut stream = ValkeyStream::new();
        stream.set_append_node_limits(2, 0);
        for ms in 1..=4 {
            append(&mut stream, ms);
        }
        stream.delete(&StreamID::new(3, 0));
        assert_eq!(stream.trim_by_length_limit(1, true, Some(1), 2), 0);
        assert_eq!(stream.len(), 3);
        stream.delete(&StreamID::new(1, 0));
        assert_eq!(stream.physical_first_id(), StreamID::new(1, 0));
        assert_eq!(stream.trim_by_length_limit(1, true, Some(1), 2), 1);
        assert_eq!(stream.physical_first_id(), StreamID::new(3, 0));
        assert_eq!(stream.first_id(), Some(StreamID::new(4, 0)));
        stream.set_append_node_limits(1, 0);
        append(&mut stream, 5);
        assert_eq!(stream.trim_by_length_limit(1, true, Some(0), 1), 1);
        assert_eq!(stream.first_id(), Some(StreamID::new(5, 0)));
    }

    #[test]
    fn minid_rewrite_edge_keeps_partial_node_tombstones_and_empty_sentinel() {
        let mut stream = ValkeyStream::new();
        stream.set_append_node_limits(3, 0);
        for ms in 1..=3 {
            append(&mut stream, ms);
        }
        assert_eq!(stream.trim_by_id(&StreamID::new(2, 0), false), 1);
        assert_eq!(stream.first_id(), Some(StreamID::new(2, 0)));
        assert_eq!(stream.physical_first_id(), StreamID::new(1, 0));
        assert_eq!(stream.max_deleted_entry_id, StreamID::zero());
        assert_eq!(stream.trim_by_length(0, false), 2);
        assert_eq!(stream.physical_first_id(), StreamID::max());
    }
}
