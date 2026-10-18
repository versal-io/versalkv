use super::groups;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::fmt;

use crate::foundation::memory::value::{Encoding, StreamOps};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamID {
    pub ms: u64,
    pub seq: u64,
}

impl StreamID {
    pub fn new(ms: u64, seq: u64) -> Self {
        StreamID { ms, seq }
    }

    pub fn zero() -> Self {
        StreamID { ms: 0, seq: 0 }
    }

    pub fn max() -> Self {
        StreamID {
            ms: u64::MAX,
            seq: u64::MAX,
        }
    }

    pub fn parse(s: &[u8]) -> Option<Self> {
        let s = std::str::from_utf8(s).ok()?;
        let (ms_str, seq_str) = s.split_once('-')?;
        let ms: u64 = ms_str.parse().ok()?;
        let seq: u64 = seq_str.parse().ok()?;
        Some(StreamID { ms, seq })
    }

    pub fn fmt_id(&self) -> String {
        format!("{}-{}", self.ms, self.seq)
    }

    pub fn incr(&mut self) -> bool {
        if self.seq == u64::MAX {
            if self.ms == u64::MAX {
                return false;
            }
            self.ms += 1;
            self.seq = 0;
        } else {
            self.seq += 1;
        }
        true
    }

    pub fn incr_checked(&self) -> Option<Self> {
        if let Some(next_seq) = self.seq.checked_add(1) {
            Some(StreamID::new(self.ms, next_seq))
        } else {
            self.ms
                .checked_add(1)
                .map(|next_ms| StreamID::new(next_ms, 0))
        }
    }

    pub fn next(&self) -> Option<Self> {
        let mut next = *self;
        if next.incr() {
            Some(next)
        } else {
            None
        }
    }
}

impl fmt::Display for StreamID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamEntry {
    pub id: StreamID,
    pub fields: Vec<(Bytes, Bytes)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ValkeyStream {
    pub(super) entries: BTreeMap<StreamID, Vec<(Bytes, Bytes)>>,
    pub(crate) groups: BTreeMap<Bytes, groups::StreamGroup>,
    pub last_id: StreamID,
    pub entries_added: u64,
    pub max_deleted_entry_id: StreamID,
    recorded_first_entry_id: StreamID,
    approx_base_entries_added: u64,
}

// TODO(dead_code): StreamLp is a placeholder for the future listpack-encoded
// stream representation. Currently all streams use ValkeyStream (hashtable-like),
// and this stub exists so Robj can declare both encoding variants uniformly.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
pub struct StreamLp;

impl StreamOps for ValkeyStream {
    fn len(&self) -> usize {
        self.entries.len()
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn encoding(&self) -> Encoding {
        Encoding::StreamP
    }
}

impl ValkeyStream {
    pub fn new() -> Self {
        ValkeyStream {
            entries: BTreeMap::new(),
            groups: BTreeMap::new(),
            last_id: StreamID::zero(),
            entries_added: 0,
            max_deleted_entry_id: StreamID::zero(),
            recorded_first_entry_id: StreamID::zero(),
            approx_base_entries_added: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn first_id(&self) -> Option<StreamID> {
        self.entries.keys().next().copied()
    }

    pub fn first_entry(&self) -> Option<StreamEntry> {
        self.entries.iter().next().map(|(id, fields)| StreamEntry {
            id: *id,
            fields: fields.clone(),
        })
    }

    pub fn recorded_first_entry_id(&self) -> StreamID {
        self.recorded_first_entry_id
    }

    pub fn last_entry(&self) -> Option<StreamEntry> {
        self.entries
            .iter()
            .next_back()
            .map(|(id, fields)| StreamEntry {
                id: *id,
                fields: fields.clone(),
            })
    }

    pub fn last_n_entries(&self, n: usize) -> Vec<StreamEntry> {
        let mut result = Vec::with_capacity(n);
        for (id, fields) in self.entries.iter().rev().take(n) {
            result.push(StreamEntry {
                id: *id,
                fields: fields.clone(),
            });
        }
        result.reverse();
        result
    }

    pub fn recorded_last_id(&self) -> StreamID {
        self.last_id
    }

    pub fn last_entry_id(&self) -> StreamID {
        self.last_id
    }

    pub fn append(
        &mut self,
        mut id: StreamID,
        fields: Vec<(Bytes, Bytes)>,
        use_id: bool,
        seq_auto: bool,
        now_ms: u64,
    ) -> Result<StreamID, &'static str> {
        let has_history = self.entries_added > 0;

        if !has_history {
            if !use_id {
                id = StreamID::new(now_ms, 0);
                if id.ms == 0 {
                    id.seq = 1;
                }
            } else if seq_auto {
                if id.ms == 0 {
                    id.seq = 1;
                } else {
                    id.seq = 0;
                }
            } else {
                if id.ms == 0 && id.seq == 0 {
                    return Err("ERR The ID specified in XADD must be greater than 0-0");
                }
            }
        } else {
            let last = self.last_id;
            if !use_id {
                if now_ms > last.ms {
                    id = StreamID::new(now_ms, 0);
                } else {
                    match last.incr_checked() {
                        Some(next) => id = next,
                        None => return Err("ERR The stream has exhausted the IDs"),
                    }
                }
            } else if seq_auto {
                if id.ms > last.ms {
                    id.seq = 0;
                } else if id.ms == last.ms {
                    match last.seq.checked_add(1) {
                        Some(next_seq) => id.seq = next_seq,
                        None => return Err("ERR The ID specified in XADD is equal or smaller than the target stream top item"),
                    }
                } else {
                    return Err("ERR The ID specified in XADD is equal or smaller than the target stream top item");
                }
            } else {
                if id.ms < last.ms || (id.ms == last.ms && id.seq <= last.seq) {
                    return Err("ERR The ID specified in XADD is equal or smaller than the target stream top item");
                }
            }
        }

        if id.ms == 0 && id.seq == 0 {
            return Err("ERR The ID specified in XADD must be greater than 0-0");
        }

        if self.entries.contains_key(&id) {
            return Err("ERR The ID specified is already existing");
        }

        self.entries.insert(id, fields);
        self.last_id = id;
        self.entries_added += 1;
        if self.entries.len() == 1 {
            self.recorded_first_entry_id = id;
        }
        Ok(id)
    }

    pub fn delete(&mut self, id: &StreamID) -> bool {
        if self.entries.remove(id).is_some() {
            if id.ms > self.max_deleted_entry_id.ms
                || (id.ms == self.max_deleted_entry_id.ms && id.seq > self.max_deleted_entry_id.seq)
            {
                self.max_deleted_entry_id = *id;
            }
            if *id == self.recorded_first_entry_id {
                self.recorded_first_entry_id = self
                    .entries
                    .keys()
                    .next()
                    .copied()
                    .unwrap_or(StreamID::zero());
            }
            if self.entries.is_empty() {
                self.approx_base_entries_added = self.entries_added;
            }
            true
        } else {
            false
        }
    }

    pub fn delete_multiple(&mut self, ids: &[StreamID]) -> usize {
        let mut deleted = 0;
        for id in ids {
            if self.delete(id) {
                deleted += 1;
            }
        }
        deleted
    }

    pub fn get_entry(&self, id: &StreamID) -> Option<&Vec<(Bytes, Bytes)>> {
        self.entries.get(id)
    }

    pub fn range(
        &self,
        lower: &StreamID,
        upper: &StreamID,
        excl_lower: bool,
        excl_upper: bool,
        count: Option<usize>,
        rev: bool,
    ) -> Vec<StreamEntry> {
        let mut result = Vec::new();
        let limit = count.unwrap_or(usize::MAX);

        if rev {
            for (id, fields) in self.entries.range(lower..=upper).rev() {
                if excl_upper && id == upper {
                    continue;
                }
                if excl_lower && id == lower {
                    break;
                }
                result.push(StreamEntry {
                    id: *id,
                    fields: fields.clone(),
                });
                if result.len() >= limit {
                    break;
                }
            }
        } else {
            for (id, fields) in self.entries.range(lower..=upper) {
                if excl_lower && id == lower {
                    continue;
                }
                if excl_upper && id == upper {
                    break;
                }
                result.push(StreamEntry {
                    id: *id,
                    fields: fields.clone(),
                });
                if result.len() >= limit {
                    break;
                }
            }
        }
        result
    }

    pub fn range_from_start(&self, exclusive_start: &StreamID, count: usize) -> Vec<StreamEntry> {
        let mut result = Vec::new();
        for (id, fields) in self.entries.range(..) {
            if id <= exclusive_start {
                continue;
            }
            result.push(StreamEntry {
                id: *id,
                fields: fields.clone(),
            });
            if result.len() >= count {
                break;
            }
        }
        result
    }

    pub fn trim_by_length(&mut self, maxlen: usize, approx: bool) -> usize {
        self.trim_by_length_limit_with_node_limits(maxlen, approx, None, 100, 0)
    }

    pub fn trim_by_length_limit(
        &mut self,
        maxlen: usize,
        approx: bool,
        limit: Option<usize>,
        node_max_entries: usize,
    ) -> usize {
        self.trim_by_length_limit_with_node_limits(maxlen, approx, limit, node_max_entries, 0)
    }

    pub fn trim_by_length_limit_with_node_limits(
        &mut self,
        maxlen: usize,
        approx: bool,
        limit: Option<usize>,
        node_max_entries: usize,
        node_max_bytes: usize,
    ) -> usize {
        if self.entries.len() <= maxlen {
            return 0;
        }
        let mut to_remove = self.entries.len() - maxlen;
        if approx {
            to_remove =
                self.approximate_maxlen_trim_count(maxlen, limit, node_max_entries, node_max_bytes);
            if to_remove == 0 {
                return 0;
            }
        }
        let mut removed = 0;
        let keys: Vec<StreamID> = self.entries.keys().take(to_remove).copied().collect();
        for key in keys {
            self.delete(&key);
            removed += 1;
        }
        removed
    }

    pub fn trim_by_id(&mut self, minid: &StreamID, approx: bool) -> usize {
        self.trim_by_id_limit_with_node_limits(minid, approx, None, 100, 0)
    }

    pub fn trim_by_id_limit(
        &mut self,
        minid: &StreamID,
        approx: bool,
        limit: Option<usize>,
        node_max_entries: usize,
    ) -> usize {
        self.trim_by_id_limit_with_node_limits(minid, approx, limit, node_max_entries, 0)
    }

    pub fn trim_by_id_limit_with_node_limits(
        &mut self,
        minid: &StreamID,
        approx: bool,
        limit: Option<usize>,
        node_max_entries: usize,
        node_max_bytes: usize,
    ) -> usize {
        if self.is_empty() {
            return 0;
        }
        let mut keys: Vec<StreamID> = self.entries.range(..minid).map(|(k, _)| *k).collect();
        if approx {
            let count = self.approximate_node_trim_count(
                keys.len(),
                limit,
                node_max_entries,
                node_max_bytes,
            );
            if count == 0 {
                return 0;
            }
            keys.truncate(count);
        }
        let mut removed = 0;
        for key in &keys {
            self.delete(key);
            removed += 1;
        }
        removed
    }

    pub fn iter(&self) -> impl Iterator<Item = (&StreamID, &Vec<(Bytes, Bytes)>)> {
        self.entries.iter()
    }

    pub fn memory_usage(&self) -> usize {
        let fields_size: usize = self
            .entries
            .values()
            .map(|fs| fs.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>())
            .sum();
        std::mem::size_of_val(self) + fields_size
    }

    fn approximate_maxlen_trim_count(
        &self,
        maxlen: usize,
        limit: Option<usize>,
        node_max_entries: usize,
        node_max_bytes: usize,
    ) -> usize {
        if self.entry_cap_defines_logical_nodes(node_max_entries, node_max_bytes) {
            return self.approximate_maxlen_trim_count_by_entries(maxlen, limit, node_max_entries);
        }
        let len = self.entries.len();
        if len <= maxlen {
            return 0;
        }
        let mut remaining_len = len;
        let mut remaining_limit =
            limit.unwrap_or_else(|| Self::default_approx_trim_limit(node_max_entries));
        let mut removable = 0usize;
        for node_len in self.logical_node_lengths(node_max_entries, node_max_bytes) {
            if node_len > remaining_limit || remaining_len.saturating_sub(node_len) < maxlen {
                break;
            }
            removable += node_len;
            remaining_len -= node_len;
            remaining_limit -= node_len;
        }
        removable
    }

    fn approximate_maxlen_trim_count_by_entries(
        &self,
        maxlen: usize,
        limit: Option<usize>,
        node_max_entries: usize,
    ) -> usize {
        let len = self.entries.len();
        if len <= maxlen {
            return 0;
        }
        let node_size = node_max_entries.max(1);
        let default_limit = node_size.saturating_mul(100);
        let work_limit = limit.unwrap_or(default_limit);
        if work_limit == 0 {
            return 0;
        }

        let deleted = self.approx_deleted_count(len);
        let deleted_in_head_node = deleted % node_size;
        let mut next_node_len = if deleted_in_head_node == 0 {
            node_size
        } else {
            node_size - deleted_in_head_node
        };
        next_node_len = next_node_len.min(len);

        let mut removable = 0usize;
        let mut remaining_len = len;
        let mut remaining_limit = work_limit;
        loop {
            if next_node_len == 0
                || next_node_len > remaining_limit
                || remaining_len.saturating_sub(next_node_len) < maxlen
            {
                break;
            }
            removable += next_node_len;
            remaining_len -= next_node_len;
            remaining_limit -= next_node_len;
            next_node_len = node_size.min(remaining_len);
        }
        removable
    }

    fn approximate_node_trim_count(
        &self,
        candidate_count: usize,
        limit: Option<usize>,
        node_max_entries: usize,
        node_max_bytes: usize,
    ) -> usize {
        if self.entry_cap_defines_logical_nodes(node_max_entries, node_max_bytes) {
            return self.approximate_node_trim_count_by_entries(
                candidate_count,
                limit,
                node_max_entries,
            );
        }
        let mut remaining_candidates = candidate_count;
        let mut remaining_limit =
            limit.unwrap_or_else(|| Self::default_approx_trim_limit(node_max_entries));
        let mut removable = 0usize;
        for node_len in self.logical_node_lengths(node_max_entries, node_max_bytes) {
            if node_len > remaining_candidates || node_len > remaining_limit {
                break;
            }
            removable += node_len;
            remaining_candidates -= node_len;
            remaining_limit -= node_len;
        }
        removable
    }

    fn approximate_node_trim_count_by_entries(
        &self,
        candidate_count: usize,
        limit: Option<usize>,
        node_max_entries: usize,
    ) -> usize {
        let node_size = node_max_entries.max(1);
        let mut remaining_limit = limit.unwrap_or_else(|| node_size.saturating_mul(100));
        if remaining_limit == 0 {
            return 0;
        }

        let deleted = self.approx_deleted_count(self.entries.len());
        let deleted_in_head_node = deleted % node_size;
        let mut next_node_len = if deleted_in_head_node == 0 {
            node_size
        } else {
            node_size - deleted_in_head_node
        };
        next_node_len = next_node_len.min(self.entries.len());

        let mut removable = 0usize;
        let mut remaining_candidates = candidate_count;
        while next_node_len != 0
            && next_node_len <= remaining_candidates
            && next_node_len <= remaining_limit
        {
            removable += next_node_len;
            remaining_candidates -= next_node_len;
            remaining_limit -= next_node_len;
            next_node_len = node_size.min(self.entries.len().saturating_sub(removable));
        }
        removable
    }

    fn default_approx_trim_limit(node_max_entries: usize) -> usize {
        if node_max_entries == 0 {
            10_000
        } else {
            node_max_entries.saturating_mul(100).min(1_000_000)
        }
    }

    fn entry_cap_defines_logical_nodes(
        &self,
        node_max_entries: usize,
        node_max_bytes: usize,
    ) -> bool {
        if node_max_entries == 0 {
            return false;
        }
        if node_max_bytes == 0 {
            return true;
        }
        self.logical_node_lengths(node_max_entries, node_max_bytes)
            .first()
            .is_some_and(|length| *length == node_max_entries)
    }

    fn logical_node_lengths(&self, node_max_entries: usize, node_max_bytes: usize) -> Vec<usize> {
        let mut nodes = Vec::new();
        let mut entries_in_node = 0usize;
        let mut bytes_in_node = 0usize;
        for fields in self.entries.values() {
            let entry_bytes = Self::logical_entry_bytes(fields);
            let entry_limit_hit = node_max_entries != 0 && entries_in_node >= node_max_entries;
            let byte_limit_hit =
                node_max_bytes != 0 && bytes_in_node.saturating_add(entry_bytes) >= node_max_bytes;
            if entries_in_node != 0 && (entry_limit_hit || byte_limit_hit) {
                nodes.push(entries_in_node);
                entries_in_node = 0;
                bytes_in_node = 0;
            }
            entries_in_node += 1;
            bytes_in_node = bytes_in_node.saturating_add(entry_bytes);
        }
        if entries_in_node != 0 {
            nodes.push(entries_in_node);
        }
        nodes
    }

    fn logical_entry_bytes(fields: &[(Bytes, Bytes)]) -> usize {
        // The Rust stream keeps BTreeMap entries instead of C listpacks. This
        // fixed ID/listpack-header allowance makes byte-limited logical nodes
        // conservative while the field payload remains exact.
        fields.iter().fold(24usize, |bytes, (field, value)| {
            bytes
                .saturating_add(field.len())
                .saturating_add(value.len())
        })
    }

    fn approx_deleted_count(&self, len: usize) -> usize {
        self.entries_added
            .saturating_sub(self.approx_base_entries_added)
            .saturating_sub(len as u64) as usize
    }
}

impl Default for ValkeyStream {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[path = "tests_extra.rs"]
mod tests_extra;

#[cfg(test)]
mod tests {
    use super::*;

    fn field(name: &'static [u8], value: &'static [u8]) -> (Bytes, Bytes) {
        (Bytes::from_static(name), Bytes::from_static(value))
    }

    fn append_id(stream: &mut ValkeyStream, ms: u64, seq: u64) -> StreamID {
        stream
            .append(
                StreamID::new(ms, seq),
                vec![field(b"f", b"v")],
                true,
                false,
                0,
            )
            .unwrap()
    }

    fn ids(entries: &[StreamEntry]) -> Vec<StreamID> {
        entries.iter().map(|entry| entry.id).collect()
    }

    #[test]
    fn stream_id_parse_format_and_increment_edges() {
        let mut id = StreamID::parse(b"12-34").unwrap();
        assert_eq!(id, StreamID::new(12, 34));
        assert_eq!(id.fmt_id(), "12-34");
        assert_eq!(id.to_string(), "12-34");
        assert_eq!(
            StreamID::parse(b"12").unwrap_or(StreamID::zero()),
            StreamID::zero()
        );
        assert!(StreamID::parse(b"a-1").is_none());
        assert!(StreamID::parse(&[0xff, b'-', b'1']).is_none());

        assert!(id.incr());
        assert_eq!(id, StreamID::new(12, 35));
        let mut wrap = StreamID::new(7, u64::MAX);
        assert!(wrap.incr());
        assert_eq!(wrap, StreamID::new(8, 0));
        let mut exhausted = StreamID::max();
        assert!(!exhausted.incr());
        assert_eq!(
            StreamID::new(1, u64::MAX).incr_checked(),
            Some(StreamID::new(2, 0))
        );
        assert_eq!(StreamID::max().incr_checked(), None);
        assert_eq!(StreamID::new(2, 3).next(), Some(StreamID::new(2, 4)));
        assert_eq!(StreamID::max().next(), None);
    }

    #[test]
    fn append_assigns_auto_ids_and_enforces_monotonic_order() {
        let mut empty = ValkeyStream::new();
        assert!(empty
            .append(StreamID::zero(), vec![field(b"x", b"0")], true, false, 0)
            .unwrap_err()
            .contains("greater than 0-0"));

        let mut stream = ValkeyStream::new();

        let first = stream
            .append(StreamID::zero(), vec![field(b"a", b"1")], false, false, 0)
            .unwrap();
        assert_eq!(first, StreamID::new(0, 1));
        assert_eq!(stream.recorded_first_entry_id(), first);

        let second = stream
            .append(StreamID::zero(), vec![field(b"b", b"2")], false, false, 0)
            .unwrap();
        assert_eq!(second, StreamID::new(0, 2));

        let third = stream
            .append(StreamID::new(4, 0), vec![field(b"c", b"3")], true, true, 0)
            .unwrap();
        assert_eq!(third, StreamID::new(4, 0));

        let fourth = stream
            .append(StreamID::new(4, 0), vec![field(b"d", b"4")], true, true, 0)
            .unwrap();
        assert_eq!(fourth, StreamID::new(4, 1));

        assert_eq!(stream.len(), 4);
        assert_eq!(stream.entries_added, 4);
        assert_eq!(stream.recorded_last_id(), fourth);
        assert_eq!(stream.last_entry_id(), fourth);

        assert!(stream
            .append(StreamID::zero(), vec![field(b"x", b"0")], true, false, 0)
            .unwrap_err()
            .contains("equal or smaller"));
        assert!(stream
            .append(StreamID::new(4, 1), vec![field(b"x", b"1")], true, false, 0)
            .unwrap_err()
            .contains("equal or smaller"));

        stream.last_id = StreamID::max();
        assert!(stream
            .append(StreamID::zero(), vec![field(b"x", b"2")], false, false, 0)
            .unwrap_err()
            .contains("exhausted"));
        assert!(stream
            .append(
                StreamID::new(u64::MAX, 0),
                vec![field(b"x", b"3")],
                true,
                true,
                0,
            )
            .unwrap_err()
            .contains("equal or smaller"));
    }

    #[test]
    fn accessors_return_entries_in_stream_order() {
        let mut stream = ValkeyStream::new();
        assert!(stream.is_empty());
        assert_eq!(stream.first_id(), None);
        assert_eq!(stream.first_entry(), None);
        assert_eq!(stream.last_entry(), None);

        let a = append_id(&mut stream, 1, 0);
        let b = append_id(&mut stream, 2, 0);
        let c = append_id(&mut stream, 3, 0);

        assert!(!stream.is_empty());
        assert_eq!(stream.first_id(), Some(a));
        assert_eq!(stream.first_entry().unwrap().id, a);
        assert_eq!(stream.last_entry().unwrap().id, c);
        assert_eq!(ids(&stream.last_n_entries(2)), vec![b, c]);
        assert_eq!(stream.get_entry(&b).unwrap(), &vec![field(b"f", b"v")]);
        assert_eq!(
            stream.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![a, b, c]
        );
        assert!(stream.memory_usage() >= std::mem::size_of_val(&stream));
    }

    #[test]
    fn delete_and_delete_multiple_update_tombstone_and_recorded_first_id() {
        let mut stream = ValkeyStream::new();
        let a = append_id(&mut stream, 1, 0);
        let b = append_id(&mut stream, 2, 0);
        let c = append_id(&mut stream, 3, 0);

        assert!(!stream.delete(&StreamID::new(9, 9)));
        assert!(stream.delete(&a));
        assert_eq!(stream.max_deleted_entry_id, a);
        assert_eq!(stream.recorded_first_entry_id(), b);

        assert!(stream.delete(&c));
        assert_eq!(stream.max_deleted_entry_id, c);
        assert_eq!(stream.recorded_first_entry_id(), b);

        assert_eq!(stream.delete_multiple(&[a, b, c]), 1);
        assert_eq!(stream.recorded_first_entry_id(), StreamID::zero());
        assert_eq!(stream.len(), 0);
    }

    #[test]
    fn range_honors_bounds_count_direction_and_exclusive_start() {
        let mut stream = ValkeyStream::new();
        for ms in 1..=5 {
            append_id(&mut stream, ms, 0);
        }

        let lower = StreamID::new(2, 0);
        let upper = StreamID::new(5, 0);
        assert_eq!(
            ids(&stream.range(&lower, &upper, false, false, Some(3), false)),
            vec![
                StreamID::new(2, 0),
                StreamID::new(3, 0),
                StreamID::new(4, 0)
            ]
        );
        assert_eq!(
            ids(&stream.range(&lower, &upper, true, true, None, false)),
            vec![StreamID::new(3, 0), StreamID::new(4, 0)]
        );
        assert_eq!(
            ids(&stream.range(&lower, &upper, false, false, Some(2), true)),
            vec![StreamID::new(5, 0), StreamID::new(4, 0)]
        );
        assert_eq!(
            ids(&stream.range(&lower, &upper, true, true, None, true)),
            vec![StreamID::new(4, 0), StreamID::new(3, 0)]
        );
        assert_eq!(
            ids(&stream.range_from_start(&StreamID::new(3, 0), 2)),
            vec![StreamID::new(4, 0), StreamID::new(5, 0)]
        );
    }

    #[test]
    fn trim_by_length_and_id_support_exact_and_approximate_modes() {
        let mut stream = ValkeyStream::new();
        assert_eq!(stream.trim_by_id(&StreamID::new(10, 0), false), 0);

        for ms in 1..=105 {
            append_id(&mut stream, ms, 0);
        }

        assert_eq!(stream.trim_by_length(104, true), 0);
        assert_eq!(stream.len(), 105);
        assert_eq!(stream.trim_by_length(5, true), 100);
        assert_eq!(stream.first_id(), Some(StreamID::new(101, 0)));
        assert_eq!(stream.max_deleted_entry_id, StreamID::new(100, 0));
        assert_eq!(stream.trim_by_length(5, false), 0);

        let mut stream = ValkeyStream::new();
        for ms in 1..=105 {
            append_id(&mut stream, ms, 0);
        }
        assert_eq!(stream.trim_by_id(&StreamID::new(50, 0), true), 0);
        assert_eq!(stream.first_id(), Some(StreamID::new(1, 0)));
        assert_eq!(stream.trim_by_id(&StreamID::new(102, 0), true), 100);
        assert_eq!(stream.first_id(), Some(StreamID::new(101, 0)));

        assert_eq!(stream.trim_by_id(&StreamID::new(104, 0), false), 3);
        assert_eq!(stream.first_id(), Some(StreamID::new(104, 0)));
    }

    #[test]
    fn approximate_trim_honors_stream_node_byte_boundaries() {
        let mut byte_bounded = ValkeyStream::new();
        let mut unbounded = ValkeyStream::new();
        for ms in 1..=4 {
            let fields = vec![(Bytes::from_static(b"field"), Bytes::from_static(b"value"))];
            byte_bounded
                .append(StreamID::new(ms, 0), fields.clone(), true, false, 0)
                .unwrap();
            unbounded
                .append(StreamID::new(ms, 0), fields, true, false, 0)
                .unwrap();
        }

        assert_eq!(
            byte_bounded.trim_by_length_limit_with_node_limits(1, true, None, 100, 1),
            3
        );
        assert_eq!(
            unbounded.trim_by_length_limit_with_node_limits(1, true, None, 100, 0),
            0
        );
    }

    #[test]
    fn approximate_minid_trim_stops_at_node_boundary() {
        let mut stream = ValkeyStream::new();
        for ms in 1..=101 {
            append_id(&mut stream, ms, 0);
        }

        assert_eq!(
            stream.trim_by_id_limit(&StreamID::new(100, 0), true, None, 100),
            0
        );
        assert_eq!(stream.len(), 101);
    }

    #[test]
    fn consumer_group_tracks_new_reads_history_and_ack() {
        let mut stream = ValkeyStream::new();
        append_id(&mut stream, 1, 0);
        append_id(&mut stream, 2, 0);
        append_id(&mut stream, 3, 0);
        stream
            .create_group(Bytes::from_static(b"g"), StreamID::zero(), None)
            .unwrap();

        let first = stream.read_group_new(b"g", b"alice", 2, false, 10).unwrap();
        assert_eq!(
            ids(&first.entries),
            vec![StreamID::new(1, 0), StreamID::new(2, 0)]
        );
        assert_eq!(stream.pending_summary(b"g").unwrap().total, 2);

        let history = stream
            .read_group_history(b"g", b"alice", StreamID::zero(), 10, 20)
            .unwrap();
        assert_eq!(
            history
                .entries
                .iter()
                .map(|(id, _)| *id)
                .collect::<Vec<_>>(),
            vec![StreamID::new(1, 0), StreamID::new(2, 0)]
        );

        assert_eq!(
            stream
                .ack_group_pending(b"g", &[StreamID::new(1, 0), StreamID::new(9, 0)])
                .unwrap(),
            1
        );
        assert_eq!(stream.pending_summary(b"g").unwrap().total, 1);

        let second = stream.read_group_new(b"g", b"bob", 10, true, 30).unwrap();
        assert_eq!(ids(&second.entries), vec![StreamID::new(3, 0)]);
        assert_eq!(stream.pending_summary(b"g").unwrap().total, 1);
    }

    #[test]
    fn consumer_group_entries_read_tracks_counted_reads() {
        let mut stream = ValkeyStream::new();
        for ms in 1..=5 {
            append_id(&mut stream, ms, 0);
        }
        stream
            .create_group(Bytes::from_static(b"g"), StreamID::zero(), None)
            .unwrap();

        stream.read_group_new(b"g", b"c", 1, false, 10).unwrap();
        assert_eq!(stream.group(b"g").unwrap().entries_read, Some(1));

        stream.read_group_new(b"g", b"c", 10, false, 20).unwrap();
        assert_eq!(stream.group(b"g").unwrap().entries_read, Some(5));
    }
}
