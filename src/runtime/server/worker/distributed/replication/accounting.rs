//! Replication view, backlog-retention, and output-buffer accounting.
//! These methods project live link and full-sync state into the engine's
//! cached replication view without changing transport lifecycle.

use super::*;
use crate::framework::capabilities::io::Token;
use std::collections::{hash_map::Entry, HashMap};

#[derive(Clone, Copy)]
struct ReplicaOutputBufferLimits {
    hard: Option<usize>,
    soft: Option<usize>,
    soft_seconds: u64,
}

impl ReplicaOutputBufferLimits {
    fn new(
        configured_hard: usize,
        configured_soft: usize,
        soft_seconds: u64,
        repl_backlog_size: i64,
    ) -> Self {
        let backlog_size = repl_backlog_size.max(0) as usize;
        Self {
            hard: (configured_hard != 0).then_some(configured_hard.max(backlog_size)),
            soft: (configured_soft != 0).then_some(configured_soft.max(backlog_size)),
            soft_seconds,
        }
    }
}

#[derive(Clone, Copy)]
struct MainOutputBufferEntry {
    connection_id: ConnectionId,
    owner_connection_id: ConnectionId,
    bytes: usize,
    cursor_error: bool,
    limit_enforcement_active: bool,
}

#[derive(Clone, Copy)]
struct MainOutputBufferOwnerEntry {
    owner_connection_id: ConnectionId,
    diagnostic_connection_id: ConnectionId,
    bytes: usize,
    cursor_error: bool,
    limit_enforcement_active: bool,
}

#[derive(Clone, Copy)]
struct MainOutputBufferViolation {
    owner_connection_id: ConnectionId,
    diagnostic_connection_id: ConnectionId,
    bytes: usize,
    hard_exceeded: bool,
    cursor_error: bool,
}

#[derive(Clone, Copy)]
struct RetainedOutputBufferViolation {
    public_client_id: u64,
    bytes: usize,
    hard_exceeded: bool,
    soft_timed_out: bool,
    cursor_error: bool,
}

impl RetainedOutputBufferViolation {
    fn limit_type(self) -> &'static str {
        if self.cursor_error {
            "cursor"
        } else if self.hard_exceeded {
            "hard"
        } else {
            "soft"
        }
    }
}

impl ServerWorkerState {
    pub(crate) fn refresh_replication_view(&mut self) {
        let mut view = self.replication_runtime.rebuild_view();
        if let Some(state) = self.ctx.cluster_state().as_ref() {
            let previous = &self.ctx.replication().view.cluster_replica_views;
            for node in state
                .nodes
                .values()
                .filter(|node| node.is_replica() && node.replicaof == state.myself.name)
            {
                if view.cluster_replica_views.contains_key(&node.name) {
                    continue;
                }
                if let Some(previous) = previous.get(&node.name) {
                    let mut disconnected = previous.clone();
                    disconnected.health =
                        crate::engine::distributed::cluster::state::ClusterReplicaHealth::Disconnected;
                    view.cluster_replica_views
                        .insert(node.name.clone(), disconnected);
                }
            }
            let loading = self.ctx.is_loading() || self.ctx.replication().async_loading;
            if state.myself.is_replica() {
                let mut local = self
                    .replication_runtime
                    .cluster_view(&state.myself.name, loading);
                if local.health
                    == crate::engine::distributed::cluster::state::ClusterReplicaHealth::Disconnected
                {
                    local.received_offset = self.ctx.replication().slave_read_repl_offset;
                    local.applied_offset = self.ctx.replication().slave_repl_offset;
                    local.acknowledged_offset = local.applied_offset;
                }
                local.upstream = NodeId::parse(state.myself.replicaof.as_bytes()).ok();
                local.priority = self.ctx.config().replication.replica_priority;
                view.cluster_replica_views
                    .insert(state.myself.name.clone(), local);
            }

            for node in state.nodes.values().filter(|node| node.is_replica()) {
                if let Some(entry) = view.cluster_replica_views.get_mut(&node.name) {
                    entry.upstream = NodeId::parse(node.replicaof.as_bytes()).ok();
                    entry.priority = node.replica_priority;
                }
            }
        }
        self.ctx.replication_mut().view = view;
        self.refresh_replication_buffer_accounting();
    }

    /// Recompute the backlog retention floor and the INFO-visible userspace
    /// replication-buffer total after a replica consumes queued bytes.
    pub(crate) fn refresh_replication_buffer_accounting(&mut self) {
        self.replication_buffer_accounting_dirty = false;
        let protected = if !self.full_sync.pending_dual_rdb_snapshots.is_empty()
            && self.full_sync.diskless_relay.is_none()
        {
            self.replication_runtime
                .min_protected_backlog_cursor_without_psync_wait()
        } else {
            self.replication_runtime.min_protected_backlog_cursor()
        };
        let backlog_capacity = self.ctx.config().server.repl_backlog_size.max(0) as usize;
        let protected = min_protected_cursor(protected, self.active_replication_transfer_floor());
        self.ctx
            .replication_mut()
            .backlog
            .set_min_protected_cursor(protected);
        self.ctx
            .replication_mut()
            .backlog
            .set_capacity(backlog_capacity);
        self.refresh_replica_output_buffer_bytes();
    }

    pub(crate) fn refresh_replication_buffer_accounting_if_dirty(&mut self) {
        if self.replication_buffer_accounting_dirty {
            self.refresh_replication_buffer_accounting();
        }
    }

    pub(super) fn active_replication_transfer_floor(&self) -> Option<BacklogCursor> {
        let mut targets = Vec::new();
        if let Some(relay) = &self.full_sync.diskless_relay {
            targets.extend(relay.targets.iter().copied());
        }
        if let Some(batch) = &self.full_sync.disk_sync_batch {
            targets.extend(batch.target_ids());
        }
        targets
            .into_iter()
            .filter_map(|id: ReplicaId| self.replication_runtime.primary_link(id))
            .filter(|link| {
                matches!(
                    link.state(),
                    PrimaryLinkState::PreparingFullResync | PrimaryLinkState::SendingRdb
                ) || link.dual_state() == PrimaryDualChannelState::WaitingForMainPsync
            })
            .map(|link| link.next_send_cursor())
            .min()
    }

    /// Snapshot the bytes still queued in every replica link's output buffer
    /// into `ReplicationState` so INFO memory can report
    /// `mem_total_replication_buffers` without borrowing the connection slab.
    /// A paused replica's socket stops draining, so its `pump`ed backlog frames
    /// pile up here — this is exactly what the shutdown test's fill loop waits
    /// to exceed 2MB before triggering the graceful-shutdown path.
    pub(crate) fn refresh_replica_output_buffer_bytes(&mut self) {
        // On a primary, the shared backlog and each replica's user-space
        // output queue are the replication-buffer memory reported by INFO.
        // Bytes already accepted by the kernel are no longer retained by the
        // server's replication buffers; counting them makes callers believe a
        // following write is queued behind backpressure when it may still be
        // accepted by the socket.
        let live_total = if self.ctx.replication().is_replica() {
            self.replication_runtime
                .replica_pending_buffer()
                .map_or(0, |buffer| buffer.used_bytes())
        } else {
            let backlog_bytes = self.ctx.replication().backlog.history_len();
            let queued = self
                .replication_runtime
                .primary_links()
                .values()
                .filter_map(|link| self.connection_registry.get(Token(link.connection_id().0)))
                .map(|conn| conn.replication_output_bytes())
                .max()
                .unwrap_or(0);
            backlog_bytes.max(queued)
        };
        let allocation = if self.ctx.replication().is_replica() {
            self.replication_runtime
                .replica_pending_buffer()
                .map_or(0, |buffer| buffer.used_bytes())
        } else {
            self.replication_runtime
                .primary_links()
                .values()
                .filter_map(|link| self.connection_registry.get(Token(link.connection_id().0)))
                .map(|conn| conn.output_buffer_allocation_bytes())
                .max()
                .unwrap_or(0)
        };
        let codecs: usize = self
            .replication_runtime
            .primary_links()
            .values()
            .map(|link| link.compression_memory_bytes())
            .sum::<usize>()
            + self
                .replication_runtime
                .replica_link()
                .map_or(0, |link| link.compression_memory_bytes());
        let replication = self.ctx.replication_mut();
        replication.replica_output_buffer_bytes = live_total.saturating_add(codecs);
        replication.replica_output_buffer_allocation_bytes = allocation.saturating_add(codecs);
        replication.replica_output_buffer_peak =
            replication.replica_output_buffer_peak.max(live_total);
    }

    fn collect_main_output_buffer_entries(
        &self,
        backlog_tail: BacklogCursor,
    ) -> Vec<MainOutputBufferEntry> {
        let mut entries = Vec::new();
        for link in self.replication_runtime.primary_links().values() {
            let connection_id = link.connection_id();
            if !self.connection_registry.contains(Token(connection_id.0)) {
                continue;
            }
            // The flat VecDeque representation tracks logical payload bytes.
            // Its allocator overhead is intentionally excluded because C
            // replica clients share backlog blocks rather than owning an
            // equivalent per-frame allocation.
            let queued_bytes = self
                .connection_registry
                .get(Token(connection_id.0))
                .map_or(0, |conn| conn.output_buffer_bytes())
                .saturating_add(link.compression_memory_bytes());
            let snapshot_transfer_active = self.replica_snapshot_transfer_active(connection_id);
            let dual_channel_link = link
                .replica()
                .capability
                .iter()
                .any(|capability| capability.eq_ignore_ascii_case("dual-channel"));
            let full_resync_backlog_bytes = if matches!(
                link.state(),
                PrimaryLinkState::PreparingFullResync
                    | PrimaryLinkState::SendingRdb
                    | PrimaryLinkState::WaitingForReplicaAck
            ) || (link.is_compressed()
                && link.state() == PrimaryLinkState::Streaming)
            {
                checked_cursor_distance(backlog_tail, link.next_send_cursor()).map(Some)
            } else {
                Ok(None)
            };
            let shared_repl_buffer_bytes = self
                .replication_runtime
                .dual_main_rdb_reference_cursor(connection_id)
                .or_else(|| {
                    self.replication_runtime
                        .psync_wait_pin_for_main(connection_id)
                })
                .map_or(Ok(None), |pin| {
                    checked_cursor_distance(backlog_tail, pin).map(Some)
                });
            let cursor_error =
                full_resync_backlog_bytes.is_err() || shared_repl_buffer_bytes.is_err();
            let full_resync_backlog_bytes =
                full_resync_backlog_bytes.ok().flatten().unwrap_or_default();
            let shared_repl_buffer_bytes =
                shared_repl_buffer_bytes.ok().flatten().unwrap_or_default();
            let rdb_channel_cob_active = dual_channel_link
                && matches!(
                    link.dual_state(),
                    PrimaryDualChannelState::WaitingForMainPsync
                        | PrimaryDualChannelState::WaitingForReplicaAck
                        | PrimaryDualChannelState::Online
                );
            let shared_bytes = if rdb_channel_cob_active {
                shared_repl_buffer_bytes
            } else {
                shared_repl_buffer_bytes.max(full_resync_backlog_bytes)
            };
            let bytes = if snapshot_transfer_active {
                // The RDB socket carries the transient snapshot while the main
                // intentionally retains the shared tail until handoff.
                shared_bytes
            } else {
                queued_bytes.max(shared_bytes)
            };
            // Classic streaming replicas also own queued replication output.
            // Snapshot transfer bytes are excluded by the usage calculation above.
            let limit_enforcement_active = true;
            entries.push(MainOutputBufferEntry {
                connection_id,
                owner_connection_id: connection_id,
                bytes,
                cursor_error,
                limit_enforcement_active,
            });
            if rdb_channel_cob_active {
                self.push_rdb_output_buffer_entry(
                    &mut entries,
                    connection_id,
                    cursor_error,
                    limit_enforcement_active,
                );
            }
        }
        entries
    }

    fn aggregate_main_output_buffer_entries(
        entries: Vec<MainOutputBufferEntry>,
    ) -> Vec<MainOutputBufferOwnerEntry> {
        let mut owners = HashMap::with_capacity(entries.len());
        for entry in entries {
            match owners.entry(entry.owner_connection_id) {
                Entry::Vacant(owner) => {
                    owner.insert(MainOutputBufferOwnerEntry {
                        owner_connection_id: entry.owner_connection_id,
                        diagnostic_connection_id: entry.connection_id,
                        bytes: entry.bytes,
                        cursor_error: entry.cursor_error,
                        limit_enforcement_active: entry.limit_enforcement_active,
                    });
                }
                Entry::Occupied(mut owner) => {
                    let owner = owner.get_mut();
                    if (entry.bytes > owner.bytes || entry.cursor_error) && !owner.cursor_error {
                        owner.diagnostic_connection_id = entry.connection_id;
                    }
                    owner.bytes = owner.bytes.max(entry.bytes);
                    owner.cursor_error |= entry.cursor_error;
                    owner.limit_enforcement_active |= entry.limit_enforcement_active;
                }
            }
        }
        owners.into_values().collect()
    }

    fn push_rdb_output_buffer_entry(
        &self,
        entries: &mut Vec<MainOutputBufferEntry>,
        main_connection_id: ConnectionId,
        cursor_error: bool,
        limit_enforcement_active: bool,
    ) {
        let Some(rdb_connection_id) = self
            .replication_runtime
            .rdb_channel_for_main(main_connection_id)
        else {
            return;
        };
        let bytes = self
            .connection_registry
            .get(Token(rdb_connection_id.0))
            .map_or(0, |conn| conn.output_buffer_bytes());
        entries.push(MainOutputBufferEntry {
            connection_id: rdb_connection_id,
            owner_connection_id: main_connection_id,
            bytes,
            cursor_error,
            limit_enforcement_active,
        });
    }

    fn replica_snapshot_transfer_active(&self, connection_id: ConnectionId) -> bool {
        self.replication_runtime
            .replica_id_for_connection(connection_id)
            .is_some_and(|id| {
                self.full_sync
                    .diskless_relay
                    .as_ref()
                    .is_some_and(|relay| relay.targets.contains(&id))
                    || self
                        .full_sync
                        .disk_sync_batch
                        .as_ref()
                        .is_some_and(|batch| batch.contains_target(id))
            })
    }

    fn evaluate_main_output_buffer_entries(
        &mut self,
        entries: Vec<MainOutputBufferOwnerEntry>,
        limits: ReplicaOutputBufferLimits,
        now_ms: i64,
    ) -> Vec<MainOutputBufferViolation> {
        let mut violations = Vec::new();
        let config = &self.ctx.config().replication;
        let target = crate::engine::distributed::replication::throttle::ReplicationThrottle::target(
            config.replica_output_buffer_soft_limit,
            config.replica_output_buffer_hard_limit,
        );
        let may_protect = config.repl_throttling_enabled
            && !self.ctx.replication().is_replica()
            && self.ctx.replication().throttle.is_active()
            && !self.ctx.maxmemory_state().is_over_limit();
        for entry in entries {
            let hard_limit = limits.hard.filter(|_| entry.limit_enforcement_active);
            let soft_limit = limits.soft.filter(|_| entry.limit_enforcement_active);
            let hard_exceeded = hard_limit.is_some_and(|limit| entry.bytes >= limit);
            let soft_exceeded = soft_limit.is_some_and(|limit| entry.bytes >= limit);
            let soft_timed_out = self
                .replication_runtime
                .primary_for_connection_mut(entry.owner_connection_id)
                .is_some_and(|link| {
                    let timed_out = link.output_buffer_soft_limit_reached(
                        soft_exceeded,
                        now_ms,
                        limits.soft_seconds,
                    );
                    let protected = may_protect
                        && entry.bytes >= target
                        && link
                            .output_buffer_soft_limit_age_ms(now_ms)
                            .is_some_and(|age| age <= 120_000);
                    timed_out && !protected
                });
            if entry.cursor_error || hard_exceeded || soft_timed_out {
                violations.push(MainOutputBufferViolation {
                    owner_connection_id: entry.owner_connection_id,
                    diagnostic_connection_id: entry.diagnostic_connection_id,
                    bytes: entry.bytes,
                    hard_exceeded,
                    cursor_error: entry.cursor_error,
                });
            }
        }
        violations
    }

    fn cleanup_main_output_buffer_violations(
        &mut self,
        violations: Vec<MainOutputBufferViolation>,
    ) {
        for violation in violations {
            let failed_rdb = self.shared_output_limit_rdb(violation);
            if violation.cursor_error {
                crate::server_log!(
                    warning,
                    "Closing replica owner token={} endpoint token={} after replication cursor accounting invariant violation",
                    violation.owner_connection_id.0,
                    violation.diagnostic_connection_id.0
                );
            } else {
                crate::server_log!(
                    warning,
                    "Client token={} scheduled to be closed ASAP for overcoming of output buffer limits. endpoint_token={} queued_bytes={} limit_type={}",
                    violation.owner_connection_id.0,
                    failed_rdb.unwrap_or(violation.diagnostic_connection_id).0,
                    violation.bytes,
                    if violation.hard_exceeded { "hard" } else { "soft" }
                );
                self.ctx
                    .stats_mut()
                    .client_output_buffer_limit_disconnections += 1;
            }
            if let Some(rdb) = failed_rdb {
                // Before main PSYNC, the protected RDB client owns the shared
                // backlog. Release that failed snapshot while leaving the main
                // socket alive to receive the normal lack-of-backlog rejection.
                self.replication_runtime
                    .detach_rdb_channel_preserving_main(rdb);
                self.replication_runtime
                    .abort_psync_wait_for_main(violation.owner_connection_id);
                self.remove_connection(rdb.0);
                continue;
            }
            if !violation.cursor_error {
                // A configured hard or soft close is a definitive failure.
                // Disconnect grace exists only for a peer that may still
                // establish PSYNC, so retire every owner and pin first.
                self.replication_runtime
                    .abort_psync_wait_for_main(violation.owner_connection_id);
            }
            self.remove_connection(violation.owner_connection_id.0);
        }
    }

    fn shared_output_limit_rdb(
        &self,
        violation: MainOutputBufferViolation,
    ) -> Option<ConnectionId> {
        let main = violation.owner_connection_id;
        if violation.cursor_error
            || violation.diagnostic_connection_id != main
            || self
                .replication_runtime
                .psync_wait_pin_for_main(main)
                .is_none()
        {
            return None;
        }
        let link = self.replication_runtime.primary_for_connection(main)?;
        if link.dual_state() != PrimaryDualChannelState::WaitingForMainPsync {
            return None;
        }
        let rdb = self.replication_runtime.rdb_channel_for_main(main)?;
        let main_bytes = self
            .connection_registry
            .get(Token(main.0))?
            .output_buffer_bytes()
            .saturating_add(link.compression_memory_bytes());
        let rdb_bytes = self
            .connection_registry
            .get(Token(rdb.0))?
            .output_buffer_bytes();
        (main_bytes < violation.bytes && rdb_bytes < violation.bytes).then_some(rdb)
    }

    fn enforce_retained_output_buffer_limits(
        &mut self,
        backlog_tail: BacklogCursor,
        limits: ReplicaOutputBufferLimits,
        now_ms: i64,
    ) {
        let mut violations = Vec::new();
        let removed_owner = self
            .replication_runtime
            .remove_unassociated_retained_rdb_owners_if(|owner, reached_at| {
                let bytes = owner
                    .pin_cursor
                    .map_or(Ok(0), |pin| checked_cursor_distance(backlog_tail, pin));
                let cursor_error = bytes.is_err();
                let bytes = bytes.unwrap_or_default();
                let hard_exceeded = limits.hard.is_some_and(|limit| bytes >= limit);
                let soft_exceeded = limits.soft.is_some_and(|limit| bytes >= limit);
                let soft_timed_out = OutputBufferSoftLimitReachedAt::update(
                    reached_at,
                    soft_exceeded,
                    now_ms,
                    limits.soft_seconds,
                );
                if !cursor_error && !hard_exceeded && !soft_timed_out {
                    return false;
                }
                violations.push(RetainedOutputBufferViolation {
                    public_client_id: owner.public_client_id,
                    bytes,
                    hard_exceeded,
                    soft_timed_out,
                    cursor_error,
                });
                true
            });
        self.cleanup_retained_output_buffer_violations(violations);
        if removed_owner {
            self.refresh_replication_view();
        }
    }

    fn cleanup_retained_output_buffer_violations(
        &mut self,
        violations: Vec<RetainedOutputBufferViolation>,
    ) {
        for violation in violations {
            crate::server_log!(
                warning,
                "{}",
                retained_owner_cob_log_message(
                    violation.public_client_id,
                    violation.bytes,
                    violation.limit_type(),
                )
            );
            if violation.hard_exceeded || violation.soft_timed_out {
                self.ctx
                    .stats_mut()
                    .client_output_buffer_limit_disconnections += 1;
            }
        }
    }

    /// C checks replica output limits when a new replication-buffer block is
    /// appended. Rust queues backlog frames per connection, so the equivalent
    /// retained memory is the main link's unsent output queue.
    pub(crate) fn enforce_replica_output_buffer_limits(&mut self) {
        let config = &self.ctx.config().replication;
        let limits = ReplicaOutputBufferLimits::new(
            config.replica_output_buffer_hard_limit,
            config.replica_output_buffer_soft_limit,
            config.replica_output_buffer_soft_limit_seconds,
            self.ctx.config().server.repl_backlog_size,
        );
        let now_ms = crate::foundation::io::sys::time::mstime();
        let backlog_tail = self.ctx.replication().backlog.next_stream_cursor();
        let entries = self.collect_main_output_buffer_entries(backlog_tail);
        let entries = Self::aggregate_main_output_buffer_entries(entries);
        let violations = self.evaluate_main_output_buffer_entries(entries, limits, now_ms);
        self.cleanup_main_output_buffer_violations(violations);
        self.enforce_retained_output_buffer_limits(backlog_tail, limits, now_ms);
    }

    pub(crate) fn adjust_replication_throttle(&mut self) {
        let config = &self.ctx.config().replication;
        let target = crate::engine::distributed::replication::throttle::ReplicationThrottle::target(
            config.replica_output_buffer_soft_limit,
            config.replica_output_buffer_hard_limit,
        );
        let is_enabled_primary =
            config.repl_throttling_enabled && !self.ctx.replication().is_replica();
        let entries =
            Self::aggregate_main_output_buffer_entries(self.collect_main_output_buffer_entries(
                self.ctx.replication().backlog.next_stream_cursor(),
            ));
        let online_buffers: Vec<_> = entries
            .iter()
            .filter_map(|entry| {
                let link = self
                    .replication_runtime
                    .primary_for_connection(entry.owner_connection_id)?;
                (link.state() == PrimaryLinkState::Streaming)
                    .then_some((entry.owner_connection_id.0, entry.bytes))
            })
            .collect();
        let has_replicas = !self.replication_runtime.primary_links().is_empty();
        self.ctx.replication_mut().throttle.adjust(
            is_enabled_primary,
            target,
            has_replicas,
            &online_buffers,
        );
    }

    pub(crate) fn schedule_replication_throttle(&mut self) {
        use crate::runtime::server::worker::reactor::TimerEvent;
        let kind = TimerEvent::ReplicationThrottle;
        if self.ctx.throttled_clients().is_empty()
            || self
                .pending_due_time_events
                .values()
                .any(|event| event.kind == kind)
            || self.event_batch.contains_timer(kind)
        {
            return;
        }
        let delay = self.ctx.replication_mut().throttle.release_delay();
        self.time_events.schedule_unique(
            crate::foundation::io::sys::time::MonotonicInstant::now(),
            delay,
            kind,
        );
    }

    pub(crate) fn release_throttled_clients(&mut self) {
        let start = std::time::Instant::now();
        let mut dirty = Vec::new();
        while start.elapsed() < std::time::Duration::from_millis(10) {
            let Some(token) = self.ctx.throttled_clients().first().copied() else {
                break;
            };
            if !self.ctx.replication_mut().throttle.try_release() {
                break;
            }
            self.ctx.throttled_clients_mut().shift_remove(&token);
            dirty.extend(self.wakeup_blocked_client(token, None, None));
        }
        if !dirty.is_empty() {
            self.wakeup_blocked_on_keys(&dirty, None, None);
        }
    }
}

fn checked_cursor_distance(end: BacklogCursor, start: BacklogCursor) -> Result<usize, ()> {
    let distance = end.get().checked_sub(start.get()).ok_or(())?;
    usize::try_from(distance).map_err(|_| ())
}

fn retained_owner_cob_log_message(public_client_id: u64, bytes: usize, limit_type: &str) -> String {
    format!(
        "Client id={public_client_id} scheduled to be closed ASAP for overcoming of output buffer limits. output-buffer-memory={bytes} limit_type={limit_type}"
    )
}

#[cfg(test)]
mod tests {
    use super::retained_owner_cob_log_message;

    #[test]
    fn retained_owner_cob_log_uses_c_compatible_phrase_and_public_id() {
        assert_eq!(
            retained_owner_cob_log_message(90_202, 4, "hard"),
            "Client id=90202 scheduled to be closed ASAP for overcoming of output buffer limits. output-buffer-memory=4 limit_type=hard"
        );
    }
}
