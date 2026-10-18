use crate::engine::distributed::cluster::state::CLUSTER_SLOTS;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SlotMetricCounters {
    pub cpu_usec: u64,
    pub network_bytes_in: u64,
    pub network_bytes_out: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlotMetric {
    KeyCount,
    CpuUsec,
    NetworkBytesIn,
    NetworkBytesOut,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlotOrder {
    Asc,
    Desc,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlotStatsRow {
    pub slot: usize,
    pub key_count: u64,
    pub cpu_usec: u64,
    pub network_bytes_in: u64,
    pub network_bytes_out: u64,
}

pub struct SlotMetrics {
    counters: Box<[SlotMetricCounters]>,
}

impl SlotMetrics {
    pub fn new() -> Self {
        Self::with_len(CLUSTER_SLOTS)
    }

    fn with_len(slot_count: usize) -> Self {
        Self {
            counters: vec![SlotMetricCounters::default(); slot_count].into_boxed_slice(),
        }
    }

    #[cfg(test)]
    fn with_slot_count(slot_count: usize) -> Self {
        Self::with_len(slot_count)
    }

    pub fn record(&mut self, slots: &[u16], cpu_usec: u64, in_bytes: u64, out_bytes: u64) {
        let mut slots = slots.to_vec();
        slots.sort_unstable();
        slots.dedup();
        for slot in slots {
            let Some(counters) = self.counters.get_mut(slot as usize) else {
                continue;
            };
            counters.cpu_usec = counters.cpu_usec.saturating_add(cpu_usec);
            counters.network_bytes_in = counters.network_bytes_in.saturating_add(in_bytes);
            counters.network_bytes_out = counters.network_bytes_out.saturating_add(out_bytes);
        }
    }

    pub fn reset_counters(&mut self) {
        self.counters.fill(SlotMetricCounters::default());
    }

    pub fn reset_slot(&mut self, slot: usize) {
        if let Some(counters) = self.counters.get_mut(slot) {
            *counters = SlotMetricCounters::default();
        }
    }

    pub fn counters(&self, slot: usize) -> Option<SlotMetricCounters> {
        self.counters.get(slot).copied()
    }

    pub fn query_range(
        &self,
        start: usize,
        end: usize,
        owned: &[bool],
        key_counts: &[u64],
    ) -> Vec<SlotStatsRow> {
        (start..=end)
            .filter(|slot| owned.get(*slot).copied().unwrap_or(false))
            .filter_map(|slot| self.row(slot, key_counts))
            .collect()
    }

    pub fn query_ordered(
        &self,
        metric: SlotMetric,
        order: SlotOrder,
        limit: usize,
        owned: &[bool],
        key_counts: &[u64],
    ) -> Vec<SlotStatsRow> {
        let mut rows: Vec<_> = (0..self.counters.len())
            .filter(|slot| owned.get(*slot).copied().unwrap_or(false))
            .filter_map(|slot| self.row(slot, key_counts))
            .collect();
        rows.sort_by(|left, right| {
            let ordering = metric.value(left).cmp(&metric.value(right));
            let ordering = match order {
                SlotOrder::Asc => ordering,
                SlotOrder::Desc => ordering.reverse(),
            };
            ordering.then_with(|| left.slot.cmp(&right.slot))
        });
        rows.truncate(limit);
        rows
    }

    fn row(&self, slot: usize, key_counts: &[u64]) -> Option<SlotStatsRow> {
        let counters = self.counters(slot)?;
        Some(SlotStatsRow {
            slot,
            key_count: key_counts.get(slot).copied().unwrap_or(0),
            cpu_usec: counters.cpu_usec,
            network_bytes_in: counters.network_bytes_in,
            network_bytes_out: counters.network_bytes_out,
        })
    }
}

impl Default for SlotMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl SlotMetric {
    fn value(self, row: &SlotStatsRow) -> u64 {
        match self {
            SlotMetric::KeyCount => row.key_count,
            SlotMetric::CpuUsec => row.cpu_usec,
            SlotMetric::NetworkBytesIn => row.network_bytes_in,
            SlotMetric::NetworkBytesOut => row.network_bytes_out,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{SlotMetric, SlotMetrics, SlotOrder};

    #[test]
    fn record_deduplicates_slots_and_saturates_each_counter() {
        let mut metrics = SlotMetrics::with_slot_count(8);
        metrics.record(&[2, 2, 5], u64::MAX - 1, 7, 11);
        metrics.record(&[2], 10, u64::MAX, u64::MAX);

        let slot_two = metrics.counters(2).unwrap();
        assert_eq!(slot_two.cpu_usec, u64::MAX);
        assert_eq!(slot_two.network_bytes_in, u64::MAX);
        assert_eq!(slot_two.network_bytes_out, u64::MAX);
        assert_eq!(metrics.counters(5).unwrap().cpu_usec, u64::MAX - 1);
        assert_eq!(metrics.counters(1).unwrap().cpu_usec, 0);
    }

    #[test]
    fn reset_counters_preserves_query_key_counts() {
        let mut metrics = SlotMetrics::with_slot_count(4);
        metrics.record(&[1], 9, 10, 11);
        metrics.reset_counters();
        let owned = [false, true, false, false];
        let key_counts = [0, 3, 0, 0];

        let rows = metrics.query_range(0, 3, &owned, &key_counts);

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].slot, 1);
        assert_eq!(rows[0].key_count, 3);
        assert_eq!(rows[0].cpu_usec, 0);
        assert_eq!(rows[0].network_bytes_in, 0);
        assert_eq!(rows[0].network_bytes_out, 0);
    }

    #[test]
    fn ordered_query_filters_ownership_limits_and_breaks_ties_by_slot() {
        let mut metrics = SlotMetrics::with_slot_count(6);
        metrics.record(&[1], 9, 1, 0);
        metrics.record(&[3], 9, 2, 0);
        metrics.record(&[4], 4, 3, 0);
        let owned = [false, true, false, true, true, false];
        let key_counts = [0, 2, 0, 1, 3, 0];

        let desc =
            metrics.query_ordered(SlotMetric::CpuUsec, SlotOrder::Desc, 2, &owned, &key_counts);
        assert_eq!(
            desc.iter().map(|row| row.slot).collect::<Vec<_>>(),
            vec![1, 3]
        );

        let asc =
            metrics.query_ordered(SlotMetric::KeyCount, SlotOrder::Asc, 6, &owned, &key_counts);
        assert_eq!(
            asc.iter().map(|row| row.slot).collect::<Vec<_>>(),
            vec![3, 1, 4]
        );
    }
}
