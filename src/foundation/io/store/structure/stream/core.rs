use super::groups;
use super::nodes::StreamNode;
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
    pub(super) recorded_first_entry_id: StreamID,
    pub(super) nodes: BTreeMap<StreamID, StreamNode>,
    pub(super) append_node_limits: (usize, usize),
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
            nodes: BTreeMap::new(),
            append_node_limits: (100, 4096),
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

        self.record_node_append(id, &fields);
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
            self.record_node_deletion(id, true);
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
#[path = "tests.rs"]
mod tests;
