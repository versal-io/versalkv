use super::{StreamID, ValkeyStream};

#[derive(Clone, Copy)]
enum TrimThreshold {
    Length(usize),
    Id(StreamID),
}

impl ValkeyStream {
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
        _node_max_bytes: usize,
    ) -> usize {
        if self.len() <= maxlen {
            return 0;
        }
        if maxlen == 0 && !approx {
            let deleted = self.len();
            self.entries.clear();
            self.nodes.clear();
            self.recorded_first_entry_id = StreamID::zero();
            return deleted;
        }
        self.trim_nodes(
            TrimThreshold::Length(maxlen),
            approx,
            limit,
            node_max_entries,
        )
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
        _node_max_bytes: usize,
    ) -> usize {
        self.trim_nodes(TrimThreshold::Id(*minid), approx, limit, node_max_entries)
    }

    fn trim_nodes(
        &mut self,
        threshold: TrimThreshold,
        approx: bool,
        limit: Option<usize>,
        node_max_entries: usize,
    ) -> usize {
        let work_limit = if !approx || limit == Some(0) {
            usize::MAX
        } else {
            limit.unwrap_or_else(|| {
                if node_max_entries == 0 {
                    10_000
                } else {
                    node_max_entries.saturating_mul(100).min(1_000_000)
                }
            })
        };
        let mut deleted = 0;
        while let Some((&first, node)) = self.nodes.first_key_value() {
            if matches!(threshold, TrimThreshold::Length(maxlen) if self.len() <= maxlen) {
                break;
            }
            let live = node.live;
            if live > work_limit.saturating_sub(deleted) {
                break;
            }
            let remove_node = match threshold {
                TrimThreshold::Length(maxlen) => self.len().saturating_sub(live) >= maxlen,
                TrimThreshold::Id(minid) => node.ids.last().is_some_and(|last| *last < minid),
            };
            if remove_node {
                let node = self
                    .nodes
                    .remove(&first)
                    .expect("head node was just verified");
                for id in node.ids {
                    self.entries.remove(&id);
                }
                deleted += live;
                continue;
            }
            if approx {
                break;
            }
            let ids: Vec<_> = node.ids.iter().copied().collect();
            for id in ids {
                let should_stop = match threshold {
                    TrimThreshold::Length(maxlen) => self.len() <= maxlen,
                    TrimThreshold::Id(minid) => id >= minid,
                };
                if should_stop {
                    break;
                }
                if self.entries.remove(&id).is_some() {
                    self.record_node_deletion(&id, false);
                    deleted += 1;
                }
            }
            break;
        }
        if deleted != 0 || self.entries.is_empty() {
            self.recorded_first_entry_id = self.first_id().unwrap_or(StreamID::zero());
        }
        deleted
    }
}
