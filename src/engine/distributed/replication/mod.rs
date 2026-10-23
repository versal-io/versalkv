use crate::engine::distributed::replication::backlog::ReplicationBacklog;
use std::collections::HashMap;

pub mod backlog;
mod offset;
pub(crate) mod protocol;
pub(crate) use offset::PsyncRequestOffset;
pub use offset::{BacklogCursor, InvalidReplicationPosition, ReplicationOffset, SnapshotBoundary};

#[cfg(test)]
mod tests;

#[derive(Debug, Clone)]
pub struct ReplicaStatus {
    pub ip: String,
    pub port: u16,
    pub offset: Option<ReplicationOffset>,
    pub state: String,
    pub lag_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationChannelInfo {
    pub channel_type: &'static str,
    pub state: String,
    pub ip: String,
    pub port: u16,
}

#[derive(Default)]
pub struct ReplicationView {
    pub replica_ack_offsets: HashMap<usize, Option<ReplicationOffset>>,
    pub replica_aof_ack_offsets: HashMap<usize, Option<ReplicationOffset>>,
    pub replica_info: HashMap<usize, ReplicaStatus>,
    pub replica_order: Vec<usize>,
    /// INFO replication transport order. A paired dual replica contributes
    /// its main channel followed by its RDB channel.
    pub info_order: Vec<usize>,
    pub channel_info: HashMap<usize, ReplicationChannelInfo>,
    pub live_replica_count: usize,
    pub connected_replica_count: usize,
    pub replicas_waiting_psync: usize,
    pub cluster_replica_views: std::collections::BTreeMap<
        String,
        crate::engine::distributed::cluster::state::ClusterReplicationView,
    >,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationRole {
    Primary,
    Replica,
}

pub struct ReplicationState {
    pub role: ReplicationRole,
    pub repl_seldb: i64,
    pub pending_failover_target: Option<(String, u16)>,
    pub failover_sync_offset: Option<ReplicationOffset>,
    pub failover_state: String,
    pub failover_end_time_ms: i64,
    pub force_failover: bool,
    pub disconnect_clients_until_ms: i64,
    pub backlog: ReplicationBacklog,
    primary_repl_offset: ReplicationOffset,
    pub slave_repl_offset: ReplicationOffset,
    pub slave_read_repl_offset: ReplicationOffset,
    pub async_loading: bool,
    pub view: ReplicationView,
    pub pending_replicaof_request: bool,
    /// A primary switching to a new upstream caches its local history before
    /// the reconnect, matching C `replicationCachePrimaryUsingMyself`.
    pub cache_primary_on_next_reconnect: bool,
    /// Rebind an existing upstream cached-primary position to a new target
    /// after REPLICAOF/SLAVEOF changes while already a replica.
    pub rebind_replica_resume_on_next_reconnect: bool,
    pub force_full_sync_on_next_reconnect: bool,
    pub replica_output_buffer_bytes: usize,
    pub replica_output_buffer_allocation_bytes: usize,
    pub replica_output_buffer_peak: usize,
    pub backlog_idle_deadline_ms: i64,
    primary_down_since_ms: i64,
    primary_link_was_observed: bool,
}

impl ReplicationState {
    pub fn new(replid: String, backlog_capacity: usize) -> Self {
        let zero = ReplicationOffset::try_from(0).expect("zero is a valid replication offset");
        Self {
            role: ReplicationRole::Primary,
            repl_seldb: -1,
            pending_failover_target: None,
            failover_sync_offset: None,
            failover_state: "no-failover".to_string(),
            failover_end_time_ms: 0,
            force_failover: false,
            disconnect_clients_until_ms: 0,
            backlog: ReplicationBacklog::new(replid, backlog_capacity),
            primary_repl_offset: zero,
            slave_repl_offset: zero,
            slave_read_repl_offset: zero,
            async_loading: false,
            view: ReplicationView::default(),
            pending_replicaof_request: false,
            cache_primary_on_next_reconnect: false,
            rebind_replica_resume_on_next_reconnect: false,
            force_full_sync_on_next_reconnect: false,
            replica_output_buffer_bytes: 0,
            replica_output_buffer_allocation_bytes: 0,
            replica_output_buffer_peak: 0,
            backlog_idle_deadline_ms: 0,
            primary_down_since_ms: 0,
            primary_link_was_observed: false,
        }
    }

    pub fn repl_buffer_mem(&self) -> usize {
        if self.is_replica() {
            self.backlog.history_len() + self.replica_output_buffer_bytes
        } else {
            self.backlog
                .history_len()
                .max(self.replica_output_buffer_bytes)
        }
    }

    pub fn primary_repl_offset(&self) -> ReplicationOffset {
        self.primary_repl_offset.max(self.backlog.reported_offset())
    }

    pub fn record_local_propagation(
        &mut self,
        replication_bytes: usize,
        is_aof_enabled: bool,
    ) -> Result<Option<ReplicationOffset>, InvalidReplicationPosition> {
        if self.is_replica() || replication_bytes == 0 {
            return Ok(None);
        }
        let delta = if self.backlog.is_active() {
            replication_bytes
        } else if is_aof_enabled {
            1
        } else {
            return Ok(None);
        };
        let next = self
            .primary_repl_offset()
            .checked_advance(delta)
            .ok_or(InvalidReplicationPosition)?;
        self.primary_repl_offset = next;
        if !self.backlog.is_active() {
            self.backlog.align_inactive_offset(next)?;
        }
        Ok(Some(next))
    }

    pub fn is_waiting_for_failover_sync(&self) -> bool {
        self.failover_state == "waiting-for-sync"
    }

    pub fn is_failover_in_progress(&self) -> bool {
        self.failover_state == "failover-in-progress"
    }

    pub fn pending_failover_target(&self) -> Option<&(String, u16)> {
        self.pending_failover_target.as_ref()
    }

    pub fn failover_sync_offset(&self) -> Option<ReplicationOffset> {
        self.failover_sync_offset
    }

    pub fn failover_end_time_ms(&self) -> i64 {
        self.failover_end_time_ms
    }

    pub fn is_forced_failover(&self) -> bool {
        self.force_failover
    }

    pub fn set_failover_sync_offset(&mut self, offset: ReplicationOffset) {
        self.failover_sync_offset = Some(offset);
    }

    /// Complete the primary side of a coordinated handoff and enter the
    /// temporary replica-side acceptance window.
    pub fn enter_failover_handoff(&mut self) {
        self.role = ReplicationRole::Replica;
        self.pending_failover_target = None;
        self.failover_sync_offset = None;
        self.failover_state = "failover-in-progress".to_string();
        self.failover_end_time_ms = 0;
        self.force_failover = false;
        self.disconnect_clients_until_ms = 0;
    }

    pub fn reset_failover(&mut self) {
        self.pending_failover_target = None;
        self.failover_sync_offset = None;
        self.failover_state = "no-failover".to_string();
        self.failover_end_time_ms = 0;
        self.force_failover = false;
    }

    pub fn reset_failover_and_disconnect_window(&mut self) {
        self.reset_failover();
        self.disconnect_clients_until_ms = 0;
    }

    pub fn mark_failover_complete(&mut self) {
        self.failover_state = "no-failover".to_string();
    }

    pub fn is_replica(&self) -> bool {
        self.role == ReplicationRole::Replica
    }

    pub fn observe_primary_disconnected(&mut self, now_ms: i64) {
        self.primary_link_was_observed = true;
        if self.primary_down_since_ms == 0 {
            self.primary_down_since_ms = now_ms;
        }
    }

    pub fn observe_primary_connected(&mut self) {
        self.primary_link_was_observed = true;
        self.primary_down_since_ms = 0;
    }

    pub fn primary_data_age_ms(&self, now_ms: i64) -> u64 {
        if self.primary_down_since_ms == 0 {
            return 0;
        }
        now_ms.saturating_sub(self.primary_down_since_ms) as u64
    }

    /// Data age used by Cluster automatic-failover eligibility.
    /// C Valkey leaves `repl_down_since` at zero until a configured primary
    /// link has either connected or failed. Its disconnected-age calculation
    /// therefore treats only the pre-attempt window as stale; a failed first
    /// connection establishes the ordinary finite disconnection baseline.
    /// Keep INFO's display projection separate from this eligibility sentinel.
    pub fn primary_failover_data_age_ms(&self, now_ms: i64) -> u64 {
        if self.is_replica() && !self.primary_link_was_observed {
            u64::MAX
        } else {
            self.primary_data_age_ms(now_ms)
        }
    }

    pub fn switch_to_replica(&mut self) {
        if self.role == ReplicationRole::Primary {
            // C `replicationSetPrimary()` keeps `primary_repl_offset` visible
            // until a successful full sync adopts the upstream cursor. Mirror
            // that unified offset here instead of publishing a false zero.
            let reported_offset = self.primary_repl_offset();
            self.slave_repl_offset = reported_offset;
            self.slave_read_repl_offset = reported_offset;
            self.primary_down_since_ms = 0;
            self.primary_link_was_observed = false;
        }
        self.role = ReplicationRole::Replica;
        self.backlog_idle_deadline_ms = 0;
        crate::foundation::io::sys::log::set_log_role_replica();
        self.cache_primary_on_next_reconnect = true;
        self.pending_replicaof_request = true;
    }

    /// Commit a replica-to-primary transition once. The boolean result lets
    /// coordinators keep replication-ID rotation idempotent.
    pub fn switch_to_primary(&mut self) -> bool {
        if self.role == ReplicationRole::Primary {
            return false;
        }
        self.primary_repl_offset = self.slave_repl_offset;
        self.backlog.shift_replication_id();
        self.role = ReplicationRole::Primary;
        self.backlog_idle_deadline_ms = 0;
        crate::foundation::io::sys::log::set_log_role_primary();
        self.pending_replicaof_request = false;
        self.cache_primary_on_next_reconnect = false;
        self.rebind_replica_resume_on_next_reconnect = false;
        self.force_full_sync_on_next_reconnect = false;
        self.primary_down_since_ms = 0;
        self.primary_link_was_observed = false;
        true
    }

    pub fn detach_to_primary(&mut self) -> bool {
        let changed = self.switch_to_primary();
        let zero = ReplicationOffset::try_from(0).expect("zero is a valid replication offset");
        self.slave_repl_offset = zero;
        self.slave_read_repl_offset = zero;
        self.async_loading = false;
        self.view.cluster_replica_views.clear();
        changed
    }

    pub fn live_replica_count(&self) -> usize {
        self.view.live_replica_count
    }

    pub fn count_replicas_acked_to(&self, min_offset: ReplicationOffset) -> i64 {
        self.view
            .replica_info
            .values()
            .filter(|info| {
                info.state == "online" && info.offset.is_some_and(|offset| offset >= min_offset)
            })
            .count() as i64
    }

    pub fn count_replicas_aof_acked_to(&self, min_offset: ReplicationOffset) -> i64 {
        self.view
            .replica_info
            .iter()
            .filter(|(_, info)| info.state == "online")
            .filter(|(id, _)| {
                self.view
                    .replica_aof_ack_offsets
                    .get(id)
                    .copied()
                    .flatten()
                    .is_some_and(|offset| offset >= min_offset)
            })
            .count() as i64
    }
}
