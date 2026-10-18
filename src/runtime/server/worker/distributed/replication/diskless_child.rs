use super::super::super::ServerWorkerState;
use crate::foundation::io::transport::{Interest, OwnedPollSource, WriteOutcome};
use crate::framework::capabilities::io::Token;
use crate::framework::distributed::replication::{ConnectionId, ReplicaId, ReplicationConnection};
use crate::runtime::server::worker::reactor::sources::{RuntimeSourceHandle, TransferId};
use crate::runtime::server::worker::reactor::transport::PendingRemovalOwner;
use std::collections::HashSet;
use std::os::unix::io::AsRawFd;

/// In-flight diskless fork relay: bytes stream from the child's pipe to every
/// replica in this batch until EOF, then the trailing `$EOF` marker is sent.
/// Classic targets must acknowledge that boundary (or be removed) before the
/// child is released and reaped.
pub(crate) struct DisklessRelay {
    pub(crate) transfer_id: TransferId,
    pub(crate) source: Option<RuntimeSourceHandle>,
    /// Parent-owned release end matching C's `rdb_child_exit_pipe`. Dropping it
    /// after every EOF marker drains lets the child exit.
    pub(crate) child_release_write_end: Option<std::fs::File>,
    /// `ReplicaId` for every replica receiving this snapshot. The connection_token is
    /// NOT cached: a connection can close between ticks and its slab slot be
    /// reused by another client, so a stale connection_token would misdeliver the RDB
    /// stream (ABA). Instead each tick re-resolves the connection_token from the link's
    /// live `connection_id`, exactly as `pump_full_resync_transfers` and
    /// `propagate_to_replicas` do.
    pub(crate) targets: Vec<ReplicaId>,
    /// Trailing marker to append after the RDB body ($EOF framing).
    pub(crate) marker: Vec<u8>,
    pub(crate) complete_full_resync: bool,
    pub(crate) not_before_ms: i64,
    pub(crate) eof_reached: bool,
    /// Targets whose EOF marker has been queued but is not fully flushed yet.
    /// The marker must be queued once across Partial/WouldBlock retries.
    pub(crate) marker_queued_targets: HashSet<ReplicaId>,
    pub(crate) marker_sent_targets: HashSet<ReplicaId>,
    pub(crate) completion_announced: bool,
    pub(crate) completion_state: DisklessCompletionState,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct DisklessCompletionState {
    pipe_eof: bool,
    child_succeeded: bool,
    markers_flushed: bool,
    terminal_ack_satisfied: bool,
    failed: bool,
    published: bool,
}

#[derive(Clone, Debug)]
pub(crate) enum DisklessFact {
    PipeEof,
    ChildExited(crate::runtime::server::worker::reactor::sources::ChildExitOutcome),
    MarkersFlushed,
    TerminalAckSatisfied,
}

impl DisklessCompletionState {
    pub(crate) fn apply_fact(&mut self, fact: DisklessFact) -> bool {
        match fact {
            DisklessFact::PipeEof => self.pipe_eof = true,
            DisklessFact::ChildExited(
                crate::runtime::server::worker::reactor::sources::ChildExitOutcome::Exited(0),
            ) => self.child_succeeded = true,
            DisklessFact::ChildExited(_) => self.failed = true,
            DisklessFact::MarkersFlushed => self.markers_flushed = true,
            DisklessFact::TerminalAckSatisfied => self.terminal_ack_satisfied = true,
        }
        self.try_publish()
    }

    #[cfg(test)]
    pub(crate) fn published(&self) -> bool {
        self.published
    }

    fn try_publish(&mut self) -> bool {
        if self.published || self.failed {
            return false;
        }
        if self.pipe_eof
            && self.child_succeeded
            && self.markers_flushed
            && self.terminal_ack_satisfied
        {
            self.published = true;
            return true;
        }
        false
    }
}

impl DisklessRelay {
    fn has_pending_producer_work(&self) -> bool {
        !self.eof_reached
            || self
                .targets
                .iter()
                .any(|id| !self.marker_sent_targets.contains(id))
    }
}

pub(crate) struct DirectDualRdbChild {
    pub(crate) pid: i32,
    pub(crate) child: crate::runtime::server::worker::reactor::sources::ChildHandle,
    pub(crate) rdb_connections: Vec<ConnectionId>,
    pub(crate) targets: Vec<ReplicaId>,
    pub(crate) shared_replication_buffer_bytes: usize,
    pub(crate) reaped_successfully: bool,
    pub(crate) metric_read_end: std::fs::File,
    pub(crate) phase_read_end: std::fs::File,
    pub(crate) phase_source: Option<RuntimeSourceHandle>,
    pub(crate) phases: Vec<crate::framework::commands::server::save::DirectDualRdbChildPhase>,
    pub(crate) completion_state: DirectDualCompletionState,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct DirectDualCompletionState {
    child_succeeded: bool,
    marker_phase_seen: bool,
    metrics_drained: bool,
    cohort_failed: bool,
    leases_released: bool,
    published: bool,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum DirectDualFact {
    ChildSucceeded,
    MarkerPhase,
    MetricsDrained,
    TargetHup,
    LeaseRelease,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DirectDualRdbDropOutcome {
    NotOwned,
    CompletedPaired,
    CompletedUnpaired,
    Failed,
}

impl DirectDualCompletionState {
    pub(crate) fn apply_fact(&mut self, fact: DirectDualFact) -> bool {
        match fact {
            DirectDualFact::ChildSucceeded => self.child_succeeded = true,
            DirectDualFact::MarkerPhase => self.marker_phase_seen = true,
            DirectDualFact::MetricsDrained => self.metrics_drained = true,
            DirectDualFact::TargetHup => self.cohort_failed = true,
            DirectDualFact::LeaseRelease => self.leases_released = true,
        }
        self.can_publish()
    }

    fn can_publish(&self) -> bool {
        !self.cohort_failed
            && !self.published
            && self.child_succeeded
            && self.marker_phase_seen
            && self.metrics_drained
    }

    fn mark_published(&mut self) {
        self.published = true;
    }
}

impl DirectDualRdbChild {
    pub(crate) fn logical_replication_buffer_bytes(&mut self, live_bytes: usize) -> usize {
        self.shared_replication_buffer_bytes = live_bytes;
        live_bytes
    }
}

impl ServerWorkerState {
    pub(crate) fn on_diskless_pipe_readable(
        &mut self,
        transfer_id: TransferId,
    ) -> crate::runtime::server::worker::reactor::DispatchOutcome {
        if self
            .full_sync
            .diskless_relay
            .as_ref()
            .is_none_or(|relay| relay.transfer_id != transfer_id)
        {
            return crate::runtime::server::worker::reactor::DispatchOutcome::Stale;
        }
        if self
            .full_sync
            .diskless_relay
            .as_ref()
            .and_then(|relay| relay.source)
            .is_none()
        {
            return crate::runtime::server::worker::reactor::DispatchOutcome::Stale;
        }
        self.service_diskless_relay_source();
        crate::runtime::server::worker::reactor::DispatchOutcome::Handled
    }

    pub(crate) fn on_diskless_pipe_error(
        &mut self,
        transfer_id: TransferId,
    ) -> crate::runtime::server::worker::reactor::DispatchOutcome {
        if self
            .full_sync
            .diskless_relay
            .as_ref()
            .is_none_or(|relay| relay.transfer_id != transfer_id)
        {
            return crate::runtime::server::worker::reactor::DispatchOutcome::Stale;
        }
        crate::server_log!(warning, "diskless relay pipe readiness reported an error");
        self.fail_diskless_relay();
        self.reap_registered_diskless_child_if_any();
        crate::runtime::server::worker::reactor::DispatchOutcome::Handled
    }

    pub(crate) fn on_diskless_target_writable(
        &mut self,
        id: ReplicaId,
    ) -> crate::runtime::server::worker::reactor::DispatchOutcome {
        if self
            .full_sync
            .diskless_relay
            .as_ref()
            .is_none_or(|relay| !relay.targets.contains(&id))
        {
            return crate::runtime::server::worker::reactor::DispatchOutcome::Stale;
        }
        self.flush_diskless_relay_targets();
        self.finish_diskless_relay_if_ready();
        crate::runtime::server::worker::reactor::DispatchOutcome::Handled
    }

    pub(crate) fn on_diskless_target_closed(
        &mut self,
        id: ReplicaId,
    ) -> crate::runtime::server::worker::reactor::DispatchOutcome {
        if self.full_sync.diskless_relay.is_none() {
            return crate::runtime::server::worker::reactor::DispatchOutcome::Stale;
        }
        self.drop_diskless_relay_target(id);
        crate::runtime::server::worker::reactor::DispatchOutcome::Handled
    }

    pub(crate) fn on_diskless_terminal_ack(
        &mut self,
        id: ReplicaId,
    ) -> crate::runtime::server::worker::reactor::DispatchOutcome {
        if self
            .full_sync
            .diskless_relay
            .as_ref()
            .is_none_or(|relay| !relay.targets.contains(&id))
        {
            return crate::runtime::server::worker::reactor::DispatchOutcome::Stale;
        }
        self.finish_diskless_relay_if_ready();
        crate::runtime::server::worker::reactor::DispatchOutcome::Handled
    }

    pub(crate) fn on_diskless_child_completed(
        &mut self,
        child: crate::runtime::server::worker::reactor::sources::ChildHandle,
        outcome: crate::runtime::server::worker::reactor::sources::ChildExitOutcome,
    ) -> crate::runtime::server::worker::reactor::DispatchOutcome {
        let Some(relay_child) = self
            .ctx
            .persistence()
            .diskless_child_pid()
            .and_then(|pid| self.child_registry.child_for_pid(pid))
        else {
            return crate::runtime::server::worker::reactor::DispatchOutcome::Stale;
        };
        if relay_child != child {
            return crate::runtime::server::worker::reactor::DispatchOutcome::Stale;
        }
        if let Some(relay) = self.full_sync.diskless_relay.as_mut() {
            relay
                .completion_state
                .apply_fact(DisklessFact::ChildExited(outcome));
        }
        let _ = self.reap_diskless_relay_child_if_done();
        crate::runtime::server::worker::reactor::DispatchOutcome::Handled
    }

    pub(crate) fn direct_dual_logical_replication_buffer_bytes(
        &mut self,
        live_bytes: usize,
    ) -> usize {
        self.full_sync
            .direct_dual_rdb_child
            .as_mut()
            .map_or(live_bytes, |child| {
                child.logical_replication_buffer_bytes(live_bytes)
            })
    }

    pub(crate) fn direct_dual_rdb_child_marker_sent(&mut self) -> bool {
        let Some(child) = self.full_sync.direct_dual_rdb_child.as_mut() else {
            return false;
        };
        let phases = crate::framework::commands::server::save::drain_direct_dual_rdb_phase_pipe(
            &mut child.phase_read_end,
        );
        Self::record_direct_dual_phases(child, phases);
        child.phases.contains(
            &crate::framework::commands::server::save::DirectDualRdbChildPhase::MarkerSent,
        )
    }

    pub(crate) fn on_direct_dual_phase_readable(
        &mut self,
        child_handle: crate::runtime::server::worker::reactor::sources::ChildHandle,
    ) -> crate::runtime::server::worker::reactor::DispatchOutcome {
        let Some(source) = self
            .full_sync
            .direct_dual_rdb_child
            .as_ref()
            .filter(|child| child.child == child_handle)
            .and_then(|child| child.phase_source)
        else {
            return crate::runtime::server::worker::reactor::DispatchOutcome::Stale;
        };
        let mut buffer = [0u8; 64];
        loop {
            match self.transport.read_source(source.token, &mut buffer) {
                Ok(0) => {
                    let source = self
                        .full_sync
                        .direct_dual_rdb_child
                        .as_mut()
                        .and_then(|child| child.phase_source.take());
                    self.retire_direct_dual_phase_source(source);
                    break;
                }
                Ok(count) => {
                    if let Some(child) = self.full_sync.direct_dual_rdb_child.as_mut() {
                        let phases = buffer[..count]
                            .iter()
                            .filter_map(|&byte| {
                                crate::framework::commands::server::save::DirectDualRdbChildPhase::from_byte(byte)
                            })
                            .collect::<Vec<_>>();
                        Self::record_direct_dual_phases(child, phases);
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(_) => break,
            }
        }
        self.publish_direct_dual_child_if_ready();
        crate::runtime::server::worker::reactor::DispatchOutcome::Handled
    }

    pub(crate) fn on_direct_dual_child_completed(
        &mut self,
        child_handle: crate::runtime::server::worker::reactor::sources::ChildHandle,
        _outcome: crate::runtime::server::worker::reactor::sources::ChildExitOutcome,
    ) -> crate::runtime::server::worker::reactor::DispatchOutcome {
        if self
            .full_sync
            .direct_dual_rdb_child
            .as_ref()
            .is_none_or(|child| child.child != child_handle)
        {
            return crate::runtime::server::worker::reactor::DispatchOutcome::Stale;
        }
        self.reap_direct_dual_rdb_child();
        crate::runtime::server::worker::reactor::DispatchOutcome::Handled
    }

    /// Fork a diskless RDB child for this batch when diskless+EOF applies and no
    /// persistence child is active. Returns the pipe read-end (set non-blocking)
    /// and the shared `$EOF` marker, or `None` to signal the in-process fallback
    /// (fork/pipe failure or an already-active persistence child — single-child
    /// invariant). The single child serves every EOF-capable replica in the
    /// batch (fork-once + multi-fd broadcast, aligning with C's diskless sync).
    pub(crate) fn try_begin_direct_dual_fork(
        &mut self,
        libs: &[Vec<u8>],
        rdb_connections: &[ConnectionId],
    ) -> bool {
        if self.ctx.persistence().has_active_child()
            || self.full_sync.direct_dual_rdb_child.is_some()
        {
            return false;
        }
        let leased_tokens: Vec<_> = rdb_connections
            .iter()
            .filter_map(|connection_id| {
                let connection_token = self
                    .connection_registry
                    .contains(crate::framework::capabilities::io::Token(connection_id.0))
                    .then_some(connection_id.0)?;
                Some(
                    self.connection_registry
                        [crate::framework::capabilities::io::Token(connection_token)]
                    .token(),
                )
            })
            .collect();
        let fds: Vec<_> = leased_tokens
            .iter()
            .filter_map(|token| self.transport.raw_fd(*token).ok())
            .collect();
        if fds.len() != rdb_connections.len() || fds.is_empty() {
            return false;
        }
        if leased_tokens
            .iter()
            .any(|token| !self.socket_owned_by_main_thread(*token))
        {
            return false;
        }
        let marker: Vec<u8> =
            crate::engine::distributed::replication::protocol::generate_replid().into();
        let Some((pid, metric_read_end, phase_read_end)) =
            crate::framework::commands::server::save::fork_direct_diskless_rdb_child(
                &mut self.ctx,
                libs,
                &fds,
                &marker,
            )
        else {
            return false;
        };
        if self
            .ctx
            .persistence_mut()
            .begin_diskless_child(pid)
            .is_err()
        {
            let _ = crate::foundation::io::sys::process::send_signal(
                pid,
                crate::foundation::io::sys::process::ProcessSignal::Kill,
            );
            return false;
        }
        let child = self.register_child_process(
            crate::runtime::server::worker::reactor::sources::ChildOwner::DirectDualRdb,
            pid,
        );
        if self
            .acquire_child_socket_leases(child, &leased_tokens)
            .is_err()
        {
            let _ = crate::foundation::io::sys::process::send_signal(
                pid,
                crate::foundation::io::sys::process::ProcessSignal::Kill,
            );
            let _ = crate::foundation::io::sys::process::wait_blocking(pid);
            self.ctx.persistence_mut().complete_diskless_child();
            self.finalize_child_registry_entry(child);
            return false;
        }
        let phase_source = phase_read_end.try_clone().ok().and_then(|file| {
            self.install_runtime_source(
                OwnedPollSource::readable(file),
                crate::runtime::server::worker::reactor::sources::SourceOwner::InternalPipe(
                    crate::runtime::server::worker::reactor::sources::InternalPipeOwner::DirectDualPhase {
                        child,
                    },
                ),
            )
            .ok()
        });
        if phase_source.is_none() {
            self.release_child_socket_leases(child);
            let _ = crate::foundation::io::sys::process::send_signal(
                pid,
                crate::foundation::io::sys::process::ProcessSignal::Kill,
            );
            let _ = crate::foundation::io::sys::process::wait_blocking(pid);
            self.ctx.persistence_mut().complete_diskless_child();
            self.finalize_child_registry_entry(child);
            return false;
        }
        let targets: Vec<_> = rdb_connections
            .iter()
            .filter_map(|connection_id| {
                self.replication_runtime
                    .replica_id_for_connection(*connection_id)
            })
            .collect();
        self.full_sync.direct_dual_rdb_child = Some(DirectDualRdbChild {
            pid,
            child,
            rdb_connections: rdb_connections.to_vec(),
            targets: targets.clone(),
            shared_replication_buffer_bytes: 0,
            reaped_successfully: false,
            metric_read_end,
            phase_read_end,
            phase_source,
            phases: Vec::new(),
            completion_state: DirectDualCompletionState::default(),
        });
        // A full-sync RDB starts the replica's following command stream in DB
        // 0. Invalidate the primary's cached stream DB at the actual fork
        // boundary so the first post-snapshot write emits SELECT, matching
        // replicationSetupReplicaForFullResync() in C Valkey.
        self.ctx.reset_repl_seldb();
        self.replication_runtime
            .activate_waiting_psync_pins_for_rdb_connections(
                rdb_connections,
                &mut self.ctx.replication_mut().backlog,
            );
        crate::server_log!(
            debug,
            "direct dual RDB child {} started for replicas {:?}",
            pid,
            targets
        );
        // Match C dual-channel diskless sync: stop the parent after fork while
        // the child retains and writes the RDB socket directly.
        if crate::framework::commands::server::save::pause_parent_after_replication_fork(&self.ctx)
        {
            crate::server_log!(
                debug,
                "direct dual RDB fork pause: pid={} phase=after-sigcont targets={:?}",
                pid,
                rdb_connections
            );
        }
        true
    }

    pub(crate) fn associate_direct_dual_rdb_child_target(
        &mut self,
        rdb: ConnectionId,
        main: ConnectionId,
    ) {
        let Some(replica_id) = self.replication_runtime.primary_id(main) else {
            return;
        };
        let completed = {
            let Some(child) = self.full_sync.direct_dual_rdb_child.as_mut() else {
                return;
            };
            if !child.rdb_connections.contains(&rdb) {
                return;
            }
            if !child.targets.contains(&replica_id) {
                child.targets.push(replica_id);
            }
            child.reaped_successfully
        };
        if completed {
            if let Some(link) = self.replication_runtime.primary_link_mut(replica_id) {
                link.mark_dual_rdb_complete();
                let _ = link.promote_dual_replica_on_ack();
            }
            if let Some(child) = self.full_sync.direct_dual_rdb_child.as_mut() {
                child.rdb_connections.retain(|target| *target != rdb);
                child.targets.retain(|target| *target != replica_id);
                if child.rdb_connections.is_empty() {
                    self.full_sync.direct_dual_rdb_child = None;
                }
            }
        }
    }

    pub(crate) fn reap_direct_dual_rdb_child(&mut self) {
        let Some(mut child) = self.full_sync.direct_dual_rdb_child.take() else {
            return;
        };
        if child.reaped_successfully {
            child.rdb_connections.retain(|connection| {
                self.transport
                    .is_registered(crate::framework::capabilities::io::Token(connection.0))
            });
            if !child.rdb_connections.is_empty() {
                self.full_sync.direct_dual_rdb_child = Some(child);
            }
            return;
        }
        let observed_phases =
            crate::framework::commands::server::save::drain_direct_dual_rdb_phase_pipe(
                &mut child.phase_read_end,
            );
        if !observed_phases.is_empty() {
            Self::record_direct_dual_phases(&mut child, observed_phases);
        }
        let Some((handle, succeeded)) = self.take_child_exit_success(
            crate::runtime::server::worker::reactor::sources::ChildOwner::DirectDualRdb,
            child.pid,
        ) else {
            self.full_sync.direct_dual_rdb_child = Some(child);
            return;
        };
        let observed_phases =
            crate::framework::commands::server::save::drain_direct_dual_rdb_phase_pipe(
                &mut child.phase_read_end,
            );
        if !observed_phases.is_empty() {
            Self::record_direct_dual_phases(&mut child, observed_phases);
        }
        self.ctx.persistence_mut().complete_diskless_child();
        self.finalize_child_registry_entry(handle);
        let phase_source = child.phase_source.take();
        self.retire_direct_dual_phase_source(phase_source);
        if succeeded {
            use std::io::Read;
            let mut metric = [0u8; 8];
            let bytes = child
                .metric_read_end
                .read(&mut metric)
                .ok()
                .filter(|&n| n == metric.len())
                .map(|_| u64::from_ne_bytes(metric))
                .unwrap_or(0);
            crate::server_log!(
                debug,
                "direct dual RDB child reaped: pid={} succeeded={} metric_bytes={} phases={:?} targets={:?}",
                child.pid,
                succeeded,
                bytes,
                child.phases,
                child.targets
            );
            self.ctx.stats_mut().add_net_repl_output_bytes(bytes);
            child
                .completion_state
                .apply_fact(DirectDualFact::ChildSucceeded);
            child
                .completion_state
                .apply_fact(DirectDualFact::MetricsDrained);
            if child.phases.contains(
                &crate::framework::commands::server::save::DirectDualRdbChildPhase::MarkerSent,
            ) {
                child
                    .completion_state
                    .apply_fact(DirectDualFact::MarkerPhase);
            }
            self.full_sync.direct_dual_rdb_child = Some(child);
            self.publish_direct_dual_child_if_ready();
            if let Some(child) = self.full_sync.direct_dual_rdb_child.as_mut() {
                child.reaped_successfully = true;
            }
        } else {
            crate::server_log!(
                warning,
                "direct dual RDB child reap failed: pid={} succeeded=false phases={:?} targets={:?}",
                child.pid,
                child.phases,
                child.targets
            );
            crate::server_log!(warning, "Background RDB transfer error");
            self.close_failed_direct_child_cohort(&child);
        }
    }

    fn publish_direct_dual_child_if_ready(&mut self) {
        let ready = self
            .full_sync
            .direct_dual_rdb_child
            .as_ref()
            .is_some_and(|child| child.completion_state.can_publish());
        if !ready {
            return;
        }
        let Some(mut child) = self.full_sync.direct_dual_rdb_child.take() else {
            return;
        };
        if !child.completion_state.leases_released {
            self.release_child_socket_leases(child.child);
            child
                .completion_state
                .apply_fact(DirectDualFact::LeaseRelease);
        }
        child.completion_state.mark_published();
        for id in std::mem::take(&mut child.targets) {
            if let Some(link) = self.replication_runtime.primary_link_mut(id) {
                link.mark_dual_rdb_complete();
                if link.promote_dual_replica_on_ack() {
                    crate::server_log!(
                        debug,
                        "direct dual RDB child completion promoted replica {:?} after prior ACK",
                        id
                    );
                }
            }
        }
        child.rdb_connections.retain(|connection| {
            self.transport
                .is_registered(crate::framework::capabilities::io::Token(connection.0))
                && self
                    .replication_runtime
                    .replica_id_for_connection(*connection)
                    .is_none()
        });
        if !child.rdb_connections.is_empty() {
            child.reaped_successfully = true;
            self.full_sync.direct_dual_rdb_child = Some(child);
        }
    }

    fn record_direct_dual_phases(
        child: &mut DirectDualRdbChild,
        phases: impl IntoIterator<
            Item = crate::framework::commands::server::save::DirectDualRdbChildPhase,
        >,
    ) {
        child.phases.extend(phases);
        if child.phases.contains(
            &crate::framework::commands::server::save::DirectDualRdbChildPhase::MarkerSent,
        ) {
            child
                .completion_state
                .apply_fact(DirectDualFact::MarkerPhase);
        }
    }

    fn close_failed_direct_child_cohort(&mut self, child: &DirectDualRdbChild) {
        let mut roots = HashSet::new();
        for rdb in &child.rdb_connections {
            match self.replication_runtime.role(*rdb) {
                Some(ReplicationConnection::RdbChannel { main }) => {
                    roots.insert(main);
                }
                _ => {
                    roots.insert(*rdb);
                }
            }
        }
        for replica_id in &child.targets {
            if let Some(link) = self.replication_runtime.primary_link(*replica_id) {
                roots.insert(link.connection_id());
            }
        }
        for connection in roots {
            if self
                .connection_registry
                .contains(crate::framework::capabilities::io::Token(connection.0))
                || self.replication_runtime.role(connection).is_some()
            {
                self.remove_connection(connection.0);
            }
        }
    }

    pub(crate) fn drop_direct_dual_rdb_child_target(&mut self, id: ReplicaId) {
        let Some(child) = self.full_sync.direct_dual_rdb_child.as_mut() else {
            return;
        };
        if !child.targets.contains(&id) {
            return;
        }
        // The replica closes its RDB socket immediately after consuming the
        // marker. The phase pipe is drained by the parent asynchronously, so
        // a close can race observation of MarkerSent. Once the main channel is
        // waiting for the terminal ACK, retain the child target and let reap
        // publish the final RDB-complete state.
        let waiting_for_terminal_ack = self
            .replication_runtime
            .primary_link(id)
            .is_some_and(|link| {
                matches!(
                    link.dual_state(),
                    crate::framework::distributed::replication::PrimaryDualChannelState::WaitingForMainPsync
                        | crate::framework::distributed::replication::PrimaryDualChannelState::WaitingForReplicaAck
                )
            });
        let observed_phases =
            crate::framework::commands::server::save::drain_direct_dual_rdb_phase_pipe(
                &mut child.phase_read_end,
            );
        if !observed_phases.is_empty() {
            child.phases.extend(observed_phases);
        }
        if child.phases.contains(
            &crate::framework::commands::server::save::DirectDualRdbChildPhase::MarkerSent,
        ) && waiting_for_terminal_ack
        {
            // The replica closes its dedicated RDB socket immediately after
            // consuming the marker. The child has already delivered the full
            // snapshot; keep the target until waitpid reaps it so completion
            // can promote the surviving main channel and release the child slot.
            return;
        }
        child.targets.retain(|target| *target != id);
        if child.targets.is_empty() {
            child.completion_state.apply_fact(DirectDualFact::TargetHup);
            self.abort_failed_direct_dual_rdb_child();
        } else {
            self.abort_orphaned_direct_dual_rdb_child();
        }
    }

    pub(crate) fn drop_direct_dual_rdb_child_connection(
        &mut self,
        connection: ConnectionId,
    ) -> DirectDualRdbDropOutcome {
        let Some(role) = self.replication_runtime.role(connection) else {
            if let Some(child) = self.full_sync.direct_dual_rdb_child.as_mut() {
                child
                    .rdb_connections
                    .retain(|candidate| *candidate != connection);
            }
            self.prune_stale_direct_child_rdb_connections();
            self.abort_orphaned_direct_dual_rdb_child();
            return DirectDualRdbDropOutcome::NotOwned;
        };
        let replica_id = match role {
            ReplicationConnection::Primary(id) => Some(id),
            ReplicationConnection::RdbChannel { main } => self.replication_runtime.primary_id(main),
            ReplicationConnection::RdbChannelCandidate => None,
            _ => None,
        };
        let owned_by_child = self
            .full_sync
            .direct_dual_rdb_child
            .as_ref()
            .is_some_and(|child| match role {
                ReplicationConnection::Primary(id) => child.targets.contains(&id),
                ReplicationConnection::RdbChannel { .. }
                | ReplicationConnection::RdbChannelCandidate => {
                    child.rdb_connections.contains(&connection)
                }
                _ => false,
            });
        if !owned_by_child {
            return DirectDualRdbDropOutcome::NotOwned;
        }
        if let Some(id) = replica_id.filter(|_| matches!(role, ReplicationConnection::Primary(_))) {
            let is_last_target = self
                .full_sync
                .direct_dual_rdb_child
                .as_ref()
                .is_some_and(|child| child.targets.len() == 1 && child.targets.contains(&id));
            if is_last_target {
                if let Some(child) = self.full_sync.direct_dual_rdb_child.as_mut() {
                    child.completion_state.apply_fact(DirectDualFact::TargetHup);
                }
                self.abort_failed_direct_dual_rdb_child();
                return DirectDualRdbDropOutcome::Failed;
            }
            if let Some(child) = self.full_sync.direct_dual_rdb_child.as_mut() {
                child.targets.retain(|target| *target != id);
            }
            self.abort_orphaned_direct_dual_rdb_child();
            return DirectDualRdbDropOutcome::Failed;
        }
        let marker_sent = self
            .full_sync
            .direct_dual_rdb_child
            .as_mut()
            .is_some_and(|child| {
                let observed_phases =
                    crate::framework::commands::server::save::drain_direct_dual_rdb_phase_pipe(
                        &mut child.phase_read_end,
                    );
                Self::record_direct_dual_phases(child, observed_phases);
                child.phases.contains(
                    &crate::framework::commands::server::save::DirectDualRdbChildPhase::MarkerSent,
                )
            });
        if matches!(role, ReplicationConnection::RdbChannelCandidate) && marker_sent {
            let child_finished = self
                .full_sync
                .direct_dual_rdb_child
                .as_ref()
                .is_some_and(|child| child.reaped_successfully);
            if let Some(child) = self.full_sync.direct_dual_rdb_child.as_mut() {
                child
                    .rdb_connections
                    .retain(|candidate| *candidate != connection);
            }
            if child_finished {
                self.full_sync.direct_dual_rdb_child = None;
            }
            return DirectDualRdbDropOutcome::CompletedUnpaired;
        }
        let marker_allows_close = replica_id.is_some_and(|id| {
            let waiting_for_terminal_ack =
                self.replication_runtime
                    .primary_link(id)
                    .is_some_and(|link| {
                        matches!(
                            link.dual_state(),
                            crate::framework::distributed::replication::PrimaryDualChannelState::WaitingForMainPsync
                                | crate::framework::distributed::replication::PrimaryDualChannelState::WaitingForReplicaAck
                        )
                    });
            marker_sent && waiting_for_terminal_ack
        });
        if !marker_allows_close {
            if let Some(id) = replica_id {
                let is_last_target = self
                    .full_sync
                    .direct_dual_rdb_child
                    .as_ref()
                    .is_some_and(|child| child.targets.len() == 1 && child.targets.contains(&id));
                if !is_last_target {
                    if let Some(child) = self.full_sync.direct_dual_rdb_child.as_mut() {
                        child.targets.retain(|target| *target != id);
                        child
                            .rdb_connections
                            .retain(|candidate| *candidate != connection);
                    }
                    self.prune_stale_direct_child_rdb_connections();
                    self.abort_orphaned_direct_dual_rdb_child();
                    return DirectDualRdbDropOutcome::Failed;
                }
            }
            if let Some(child) = self.full_sync.direct_dual_rdb_child.as_mut() {
                child.completion_state.apply_fact(DirectDualFact::TargetHup);
            }
            self.abort_failed_direct_dual_rdb_child();
            return DirectDualRdbDropOutcome::Failed;
        }
        if let Some(child) = self.full_sync.direct_dual_rdb_child.as_mut() {
            child
                .rdb_connections
                .retain(|candidate| *candidate != connection);
        }
        self.prune_stale_direct_child_rdb_connections();
        self.abort_orphaned_direct_dual_rdb_child();
        DirectDualRdbDropOutcome::CompletedPaired
    }

    fn prune_stale_direct_child_rdb_connections(&mut self) {
        let Some(child) = self.full_sync.direct_dual_rdb_child.as_ref() else {
            return;
        };
        let live: HashSet<_> = child
            .rdb_connections
            .iter()
            .copied()
            .filter(|connection| {
                self.connection_registry
                    .contains(crate::framework::capabilities::io::Token(connection.0))
                    && matches!(
                        self.replication_runtime.role(*connection),
                        Some(
                            ReplicationConnection::RdbChannelCandidate
                                | ReplicationConnection::RdbChannel { .. }
                        )
                    )
            })
            .collect();
        if let Some(child) = self.full_sync.direct_dual_rdb_child.as_mut() {
            child
                .rdb_connections
                .retain(|connection| live.contains(connection));
        }
    }

    fn abort_orphaned_direct_dual_rdb_child(&mut self) {
        let is_orphaned = self
            .full_sync
            .direct_dual_rdb_child
            .as_ref()
            .is_some_and(|child| child.targets.is_empty() && child.rdb_connections.is_empty());
        if !is_orphaned {
            return;
        }
        let Some(child) = self.full_sync.direct_dual_rdb_child.take() else {
            return;
        };
        crate::server_log!(
            notice,
            "Direct dual RDB transfer, last replica dropped, killing fork child {}.",
            child.pid
        );
        self.release_child_socket_leases(child.child);
        self.retire_direct_dual_phase_source(child.phase_source);
        if !child.reaped_successfully && child.pid > 0 {
            let _ = crate::foundation::io::sys::process::send_signal(
                child.pid,
                crate::foundation::io::sys::process::ProcessSignal::Kill,
            );
            let _ = crate::foundation::io::sys::process::wait_blocking(child.pid);
        }
        crate::server_log!(warning, "Background RDB transfer error");
        self.ctx.persistence_mut().complete_diskless_child();
        self.register_active_persistence_child();
    }

    fn abort_failed_direct_dual_rdb_child(&mut self) {
        let Some(child) = self.full_sync.direct_dual_rdb_child.take() else {
            return;
        };
        self.release_child_socket_leases(child.child);
        self.retire_direct_dual_phase_source(child.phase_source);
        if !child.reaped_successfully && child.pid > 0 {
            let _ = crate::foundation::io::sys::process::send_signal(
                child.pid,
                crate::foundation::io::sys::process::ProcessSignal::Kill,
            );
            let _ = crate::foundation::io::sys::process::wait_blocking(child.pid);
        }
        crate::server_log!(warning, "Background RDB transfer error");
        self.close_failed_direct_child_cohort(&child);
        self.ctx.persistence_mut().complete_diskless_child();
        self.register_active_persistence_child();
    }

    pub(crate) fn abort_direct_dual_rdb_child_for_shutdown(&mut self) {
        let Some(child) = self.full_sync.direct_dual_rdb_child.take() else {
            return;
        };
        crate::server_log!(
            notice,
            "Shutdown aborting direct dual RDB child {}.",
            child.pid
        );
        self.release_child_socket_leases(child.child);
        self.retire_direct_dual_phase_source(child.phase_source);
        if !child.reaped_successfully {
            let _ = crate::foundation::io::sys::process::send_signal(
                child.pid,
                crate::foundation::io::sys::process::ProcessSignal::Kill,
            );
            let _ = crate::foundation::io::sys::process::wait_blocking(child.pid);
        }
        self.ctx.persistence_mut().complete_diskless_child();
    }

    fn retire_direct_dual_phase_source(&mut self, source: Option<RuntimeSourceHandle>) {
        if let Some(source) = source {
            self.retire_runtime_source(source.token);
            self.deregister_or_defer(source.token, PendingRemovalOwner::RuntimeSource);
        }
    }

    pub(crate) fn try_begin_diskless_fork(
        &mut self,
        libs: &[Vec<u8>],
    ) -> Option<(std::fs::File, Vec<u8>, std::fs::File)> {
        if !self.ctx.config().replication.repl_diskless_sync
            || self.ctx.persistence().has_active_child()
        {
            return None;
        }
        let child =
            crate::framework::commands::server::save::fork_diskless_rdb_child(&mut self.ctx, libs)?;
        self.ctx
            .persistence_mut()
            .begin_diskless_child(child.pid)
            .ok()?;
        self.register_child_process(
            crate::runtime::server::worker::reactor::sources::ChildOwner::DisklessReplication,
            child.pid,
        );
        self.ctx.reset_repl_seldb();
        // Align with C's startBgsaveForReplication: pause the parent right after
        // forking the replication RDB child when `debug pause-after-fork` is set.
        if crate::framework::commands::server::save::pause_parent_after_replication_fork(&self.ctx)
        {
            crate::server_log!(
                debug,
                "diskless replication fork pause: pid={} phase=after-sigcont",
                child.pid
            );
        }
        let _ = crate::foundation::io::sys::fd::set_nonblocking(
            std::os::fd::AsFd::as_fd(&child.data_read_end),
            true,
        );
        let marker = crate::engine::distributed::replication::protocol::generate_replid().into();
        Some((child.data_read_end, marker, child.child_release_write_end))
    }

    /// Install the batch's diskless relay after headers have been enqueued. If no
    /// EOF replica actually enrolled (all links dropped between fork and enqueue),
    /// reap the orphan child so the single-child slot is not leaked.
    pub(crate) fn store_diskless_relay(
        &mut self,
        read_end: std::fs::File,
        child_release_write_end: std::fs::File,
        marker: Vec<u8>,
        targets: Vec<ReplicaId>,
        complete_full_resync: bool,
        not_before_ms: i64,
    ) {
        if targets.is_empty() {
            self.reap_registered_diskless_child_if_any();
            return;
        }
        self.full_sync.next_transfer_id = self.full_sync.next_transfer_id.saturating_add(1).max(1);
        let transfer_id = TransferId(self.full_sync.next_transfer_id);
        let fd = read_end.as_raw_fd();
        let Some(source) = self
            .install_runtime_source(
                OwnedPollSource::readable(read_end),
                crate::runtime::server::worker::reactor::sources::SourceOwner::InternalPipe(
                    crate::runtime::server::worker::reactor::sources::InternalPipeOwner::DisklessRdb {
                        transfer_id,
                    },
                ),
            )
            .ok()
        else {
            crate::server_log!(
                warning,
                "Failed to register diskless RDB pipe source; aborting diskless relay"
            );
            self.close_diskless_relay_targets(&targets);
            self.reap_registered_diskless_child_if_any();
            return;
        };
        crate::server_log!(
            debug,
            "diskless relay installed: fd={} targets={:?} complete_full_resync={}",
            fd,
            targets,
            complete_full_resync
        );
        self.full_sync.diskless_relay = Some(DisklessRelay {
            transfer_id,
            source: Some(source),
            child_release_write_end: Some(child_release_write_end),
            targets,
            marker,
            complete_full_resync,
            not_before_ms,
            eof_reached: false,
            marker_queued_targets: HashSet::new(),
            marker_sent_targets: HashSet::new(),
            completion_announced: false,
            completion_state: DisklessCompletionState::default(),
        });
    }

    /// Drop a closing replica from the in-flight diskless relay. If it was the
    /// last remaining target, log the notice, terminate and reap the fork child
    /// before releasing its persistence slot. C's `killRDBChild()` marks the
    /// child for immediate async cleanup; Rust must not leave the slot occupied
    /// between adjacent full-sync attempts, or a deferred second sync can wait
    /// indefinitely behind a killed child. Call this from `remove_connection`
    /// with the `ReplicaId` resolved before the `PrimaryLink` is erased.
    pub(crate) fn drop_diskless_relay_target(&mut self, id: ReplicaId) {
        let has_survivors = {
            let Some(relay) = self.full_sync.diskless_relay.as_mut() else {
                return;
            };
            relay.targets.retain(|&t| t != id);
            !relay.targets.is_empty()
        };
        if has_survivors {
            self.finish_diskless_relay_if_ready();
            self.enable_diskless_source_if_unblocked();
            return;
        }
        if let Some(pid) = self.ctx.persistence().diskless_child_pid() {
            crate::server_log!(
                notice,
                "Diskless rdb transfer, last replica dropped, killing fork child."
            );
            let _ = crate::foundation::io::sys::process::send_signal(
                pid,
                crate::foundation::io::sys::process::ProcessSignal::Kill,
            );
        }
        self.clear_diskless_relay_source();
        self.full_sync.diskless_relay = None;
        if let Some(pid) = self.ctx.persistence().diskless_child_pid() {
            // The child was explicitly killed above, so waiting here cannot
            // block on a healthy transfer and makes the next full sync
            // immediately eligible to fork.
            let _ = crate::foundation::io::sys::process::wait_blocking(pid);
            self.ctx.persistence_mut().complete_diskless_child();
            self.register_active_persistence_child();
        }
    }

    pub(crate) fn finish_diskless_relay_if_ready(&mut self) {
        let ready = self.full_sync.diskless_relay.as_ref().is_some_and(|relay| {
            !relay.completion_announced
                && relay.eof_reached
                && relay
                    .targets
                    .iter()
                    .all(|id| relay.marker_sent_targets.contains(id))
                && (!relay.complete_full_resync
                    || relay.targets.iter().all(|id| {
                        self.replication_runtime
                            .primary_link(*id)
                            .is_some_and(|link| {
                                link.state()
                                    == crate::framework::distributed::replication::PrimaryLinkState::Streaming
                            })
                    }))
        });
        if !ready {
            return;
        }
        let relay = self
            .full_sync
            .diskless_relay
            .as_mut()
            .expect("ready diskless relay must remain installed");
        relay.completion_state.apply_fact(DisklessFact::PipeEof);
        relay
            .completion_state
            .apply_fact(DisklessFact::MarkersFlushed);
        relay
            .completion_state
            .apply_fact(DisklessFact::TerminalAckSatisfied);
        // Keep the upstream message text for compatibility, but emit it only
        // after Rust has reconciled terminal ACKs and removals rather than at
        // the physical pipe-EOF instant used by C Valkey.
        crate::server_log!(
            notice,
            "Diskless rdb transfer, done reading from pipe, {} replicas still up.",
            relay.targets.len()
        );
        relay.completion_announced = true;
        relay.child_release_write_end.take();
        let _ = self.reap_diskless_relay_child_if_done();
    }

    /// Resolve a live `connection_token` for a replica id, preferring the associated
    /// RDB channel when the primary link is in `WaitingForMainPsync` and the
    /// RDB channel token is still valid in the connection registry. Falls
    /// back to the main connection's token when the dual-state guard doesn't
    /// match, no RDB channel is associated, or the RDB channel token is stale
    /// — double defense so a stale or missing RDB channel never drops bytes.
    /// Re-resolving each tick avoids retaining registry-internal storage.
    fn diskless_target_token(&self, id: ReplicaId) -> Option<Token> {
        let link = self.replication_runtime.primary_link(id)?;
        let main_connection_id = link.connection_id();
        let target_connection_id = self
            .replication_runtime
            .resolve_diskless_rdb_target(main_connection_id);
        let target_token = Token(target_connection_id.0);
        let main_token = Token(main_connection_id.0);
        self.connection_registry
            .contains(target_token)
            .then_some(target_token)
            .or_else(|| {
                self.connection_registry
                    .contains(main_token)
                    .then_some(main_token)
            })
    }

    /// Resolve the main connection's connection_token for a replica id — always the
    /// primary link's own connection, never the RDB channel. Used for the
    /// backlog tail flush after EOF, which belongs to the main link's owner
    /// socket even in dual-channel mode.
    fn diskless_main_target_token(&self, id: ReplicaId) -> Option<Token> {
        let main_connection_id = self.replication_runtime.primary_link(id)?.connection_id();
        let token = Token(main_connection_id.0);
        self.connection_registry.contains(token).then_some(token)
    }

    /// Cron fallback that retires a lingering diskless child once its relay has
    /// finished. The EOF / error / orphan paths reap with a single
    /// `waitpid(WNOHANG)`, which races the child's `close(write_fd)` → `_exit`
    /// window: the parent can observe pipe EOF and reap *before* the child has
    /// actually exited, so `WNOHANG` returns 0 and the single-child slot would
    /// leak forever. Retrying each tick — but only when no relay is in flight —
    /// drains that zombie. Gating on `diskless_relay.is_none()` is essential: an
    /// active relay legitimately holds the child slot, and reaping it early would
    /// let a second fork clobber the still-draining relay's pipe.
    pub(crate) fn collect_idle_diskless_child_exit(&mut self) {
        if self.full_sync.diskless_relay.is_some() {
            let _ = self.reap_diskless_relay_child_if_done();
            return;
        }
        if self.full_sync.direct_dual_rdb_child.is_some() {
            return;
        }
        self.reap_registered_diskless_child_if_any();
    }

    fn reap_registered_diskless_child_if_any(&mut self) {
        let Some(pid) = self.ctx.persistence().diskless_child_pid() else {
            return;
        };
        let Some((handle, succeeded)) = self.take_child_exit_success(
            crate::runtime::server::worker::reactor::sources::ChildOwner::DisklessReplication,
            pid,
        ) else {
            return;
        };
        if !succeeded {
            crate::server_log!(warning, "Diskless RDB child {} exited abnormally", pid);
        }
        self.ctx.persistence_mut().complete_diskless_child();
        self.finalize_child_registry_entry(handle);
    }

    pub(crate) fn reap_registered_disk_sync_child_if_any(&mut self) {
        let Some(pid) = self.ctx.persistence().disk_sync_child_pid() else {
            return;
        };
        let Some((handle, _succeeded)) = self.take_child_exit_success(
            crate::runtime::server::worker::reactor::sources::ChildOwner::DiskSyncReplication,
            pid,
        ) else {
            return;
        };
        self.ctx.persistence_mut().complete_disk_sync_child();
        self.finalize_child_registry_entry(handle);
    }

    pub(crate) fn abort_diskless_relay_for_dataset_flush(&mut self) {
        let Some(pid) = self.ctx.persistence().diskless_child_pid() else {
            return;
        };
        if self.full_sync.diskless_relay.is_none() {
            return;
        }
        crate::server_log!(notice, "Aborting background saving child {}", pid);
        let _ = crate::foundation::io::sys::process::send_signal(
            pid,
            crate::foundation::io::sys::process::ProcessSignal::User1,
        );
        let _ = crate::foundation::io::sys::process::wait_blocking(pid);
        let targets = self
            .full_sync
            .diskless_relay
            .take()
            .map(|relay| {
                self.retire_diskless_source_handle(relay.source);
                relay.targets
            })
            .unwrap_or_default();
        self.ctx.persistence_mut().abort_child();
        self.register_active_persistence_child();
        let connections: Vec<Token> = targets
            .iter()
            .filter_map(|id| {
                self.replication_runtime
                    .primary_link(*id)
                    .map(|link| Token(link.connection_id().0))
            })
            .collect();
        for token in connections {
            self.remove_connection(token.0);
        }
        self.refresh_replication_view();
    }

    fn reap_diskless_relay_child_if_done(&mut self) -> bool {
        let Some(pid) = self.ctx.persistence().diskless_child_pid() else {
            return false;
        };
        let Some((handle, child_succeeded)) = self.take_child_exit_success(
            crate::runtime::server::worker::reactor::sources::ChildOwner::DisklessReplication,
            pid,
        ) else {
            return false;
        };
        if self.full_sync.diskless_relay.is_none() {
            self.ctx.persistence_mut().complete_diskless_child();
            self.finalize_child_registry_entry(handle);
            return true;
        }
        let child_fact = if child_succeeded {
            crate::runtime::server::worker::reactor::sources::ChildExitOutcome::Exited(0)
        } else {
            crate::runtime::server::worker::reactor::sources::ChildExitOutcome::Exited(1)
        };
        let ready_to_publish = self.full_sync.diskless_relay.as_mut().is_some_and(|relay| {
            relay
                .completion_state
                .apply_fact(DisklessFact::ChildExited(child_fact));
            relay.completion_state.published
        });
        if child_succeeded && !ready_to_publish {
            return false;
        }
        let relay = self.full_sync.diskless_relay.take();
        self.ctx.persistence_mut().complete_diskless_child();
        self.finalize_child_registry_entry(handle);
        if child_succeeded {
            if let Some(relay) = relay {
                self.publish_diskless_relay_targets(relay);
            }
        } else {
            crate::server_log!(warning, "Diskless RDB child {} exited abnormally", pid);
            crate::server_log!(warning, "Background RDB transfer error");
            let targets = relay
                .map(|relay| {
                    self.retire_diskless_source_handle(relay.source);
                    relay.targets
                })
                .unwrap_or_default();
            self.close_diskless_relay_targets(&targets);
        }
        true
    }

    fn close_diskless_relay_targets(&mut self, targets: &[ReplicaId]) {
        let connections: Vec<Token> = targets
            .iter()
            .filter_map(|id| {
                self.replication_runtime
                    .primary_link(*id)
                    .map(|link| Token(link.connection_id().0))
            })
            .collect();
        for token in connections {
            self.remove_connection(token.0);
        }
        self.refresh_replication_view();
    }

    fn fail_diskless_relay(&mut self) {
        let targets = self
            .full_sync
            .diskless_relay
            .take()
            .map(|relay| {
                self.retire_diskless_source_handle(relay.source);
                relay.targets
            })
            .unwrap_or_default();
        self.close_diskless_relay_targets(&targets);
    }

    fn clear_diskless_relay_source(&mut self) {
        let source = self
            .full_sync
            .diskless_relay
            .as_mut()
            .and_then(|relay| relay.source.take());
        self.retire_diskless_source_handle(source);
    }

    fn retire_diskless_source_handle(&mut self, source: Option<RuntimeSourceHandle>) {
        if let Some(source) = source {
            self.retire_runtime_source(source.token);
            self.deregister_or_defer(source.token, PendingRemovalOwner::RuntimeSource);
        }
    }

    fn publish_diskless_relay_targets(&mut self, relay: DisklessRelay) {
        for id in relay.targets {
            let rdb_token = self
                .diskless_target_token(id)
                .and_then(|connection_token| self.connection_registry.get(connection_token))
                .map(|connection| connection.token());
            if !relay.complete_full_resync {
                if let Some(link) = self.replication_runtime.primary_link_mut(id) {
                    link.mark_dual_rdb_complete();
                }
            }
            let Some(main_connection_token) = self.diskless_main_target_token(id) else {
                continue;
            };
            let main_token = main_connection_token;
            if self.connection_registry.contains(main_connection_token) {
                self.transport
                    .set_interest(main_token, Interest::ReadWritable)
                    .ok();
            }
            if let Some(rdb_token) = rdb_token.filter(|token| *token != main_token) {
                self.transport
                    .set_interest(rdb_token, Interest::ReadWritable)
                    .ok();
            }
        }
        self.refresh_replication_view();
    }

    /// True while any in-flight diskless relay target still holds undrained
    /// output queued from a previous pipe chunk. Paces pipe reads to the
    /// slowest replica's socket, mirroring C's `rdb_pipe_numconns_writing > 0`
    /// gate (replication.c:1928). Targets whose link/connection have gone
    /// (killed mid-transfer) resolve to no connection_token and are ignored: a dead
    /// replica must not stall the transfer for the survivors.
    fn diskless_relay_has_backpressure(&self) -> bool {
        let Some(relay) = self.full_sync.diskless_relay.as_ref() else {
            return false;
        };
        relay.targets.iter().any(|&id| {
            self.diskless_target_token(id)
                .and_then(|connection_token| self.connection_registry.get(connection_token))
                .is_some_and(|conn| !conn.output.queue.is_empty())
        })
    }

    fn disable_diskless_source_while_backpressured(&mut self) {
        if !self.diskless_relay_has_backpressure() {
            return;
        }
        if let Some(source) = self
            .full_sync
            .diskless_relay
            .as_ref()
            .and_then(|relay| relay.source)
        {
            let _ = self.transport.set_source_enabled(source.token, false);
        }
    }

    fn flush_diskless_relay_targets(&mut self) {
        let targets = self
            .full_sync
            .diskless_relay
            .as_ref()
            .map(|relay| relay.targets.clone())
            .unwrap_or_default();
        for id in targets {
            let Some(connection_token) = self.diskless_target_token(id) else {
                continue;
            };
            if !self
                .connection_registry
                .get(connection_token)
                .is_some_and(|conn| conn.has_pending_output())
            {
                self.mark_diskless_marker_sent_if_drained(id);
                continue;
            }
            let result = self.flush_connection_output(connection_token.0);
            if matches!(result, WriteOutcome::Failed(_)) {
                if let Some(conn) = self.connection_registry.get(connection_token) {
                    self.remove_connection(conn.token().0);
                }
                continue;
            }
            if matches!(result, WriteOutcome::Complete) {
                self.mark_diskless_marker_sent_if_drained(id);
            }
        }
        if !self.diskless_relay_has_backpressure() {
            self.enable_diskless_source_if_unblocked();
        }
    }

    fn enable_diskless_source_if_unblocked(&mut self) {
        if self.diskless_relay_has_backpressure() {
            return;
        }
        if let Some(source) = self
            .full_sync
            .diskless_relay
            .as_ref()
            .and_then(|relay| relay.source)
        {
            let _ = self.transport.set_source_enabled(source.token, true);
        }
    }

    fn mark_diskless_marker_sent_if_drained(&mut self, id: ReplicaId) {
        let should_mark = self.full_sync.diskless_relay.as_ref().is_some_and(|relay| {
            relay.eof_reached
                && relay.marker_queued_targets.contains(&id)
                && !relay.marker_sent_targets.contains(&id)
                && self
                    .diskless_target_token(id)
                    .and_then(|connection_token| self.connection_registry.get(connection_token))
                    .is_some_and(|conn| !conn.has_pending_output())
        });
        if !should_mark {
            return;
        }
        if let Some(relay) = self.full_sync.diskless_relay.as_mut() {
            relay.marker_sent_targets.insert(id);
            relay.marker_queued_targets.remove(&id);
        }
        let complete_full_resync = self
            .full_sync
            .diskless_relay
            .as_ref()
            .is_some_and(|relay| relay.complete_full_resync);
        if complete_full_resync {
            if let Some(link) = self.replication_runtime.primary_link_mut(id) {
                link.finish_full_resync();
            }
        }
    }

    /// Pump a diskless child's pipe into replica output queues. On EOF, append
    /// each `$EOF` marker and move classic targets into their terminal-ACK
    /// wait. Once every survivor ACKs or is removed, release the child; a later
    /// successful reap performs final relay publication and cleanup.
    fn service_diskless_relay_source(&mut self) {
        if self.full_sync.diskless_relay.is_none() {
            return;
        }
        // A relay can be installed while the PSYNC readable event is still
        // being processed. Flush its queued handshake/header first; otherwise
        // the backpressure gate may wait for a writable event that was already
        // coalesced with that readable event.
        self.flush_diskless_relay_targets();
        if crate::foundation::io::sys::time::mstime()
            < self
                .full_sync
                .diskless_relay
                .as_ref()
                .map(|r| r.not_before_ms)
                .unwrap_or(0)
        {
            return;
        }
        if self
            .full_sync
            .diskless_relay
            .as_ref()
            .is_some_and(|relay| relay.eof_reached && relay.child_release_write_end.is_none())
        {
            self.reap_diskless_relay_child_if_done();
            return;
        }
        if self
            .full_sync
            .diskless_relay
            .as_ref()
            .is_some_and(|relay| !relay.has_pending_producer_work())
        {
            return;
        }
        let mut buf = [0u8; 16 * 1024];
        // Backpressure gate: don't pull more from the pipe while any
        // replica still holds undrained bytes from the previous chunk.
        // Mirrors C's `rdb_pipe_numconns_writing` gate (replication.c:1928):
        // the pipe read handler is removed whenever a replica write is
        // partial, which blocks the fork child on write() and paces RDB
        // generation to the slowest replica. Without this the whole RDB is
        // buffered in a single tick, so pipe EOF is observed with every
        // replica still attached and the "N replicas still up" count no
        // longer reflects mid-transfer drops. Draining resumes on a later
        // tick once handle_writable has emptied the lagging replica's queue.
        if self.diskless_relay_has_backpressure() {
            self.disable_diskless_source_while_backpressured();
            return;
        }
        let source = self
            .full_sync
            .diskless_relay
            .as_ref()
            .and_then(|relay| relay.source);
        let Some(source) = source else {
            return;
        };
        let read_result = crate::foundation::io::sys::os::retry_on_interrupt(|| {
            self.transport.read_source(source.token, &mut buf)
        });
        match read_result {
            Ok(0) => {
                crate::server_log!(debug, "diskless relay reached pipe EOF");
                if crate::foundation::io::sys::time::mstime()
                    < self
                        .full_sync
                        .diskless_relay
                        .as_ref()
                        .map(|r| r.not_before_ms)
                        .unwrap_or(0)
                {
                    return;
                }
                // EOF: child finished writing the RDB body. Append the
                // trailing marker. Classic links enter the terminal-ACK wait
                // only after their own marker has fully left userspace.
                if let Some(relay) = self.full_sync.diskless_relay.as_mut() {
                    relay.eof_reached = true;
                    relay.completion_state.apply_fact(DisklessFact::PipeEof);
                }
                self.clear_diskless_relay_source();
                let (targets, marker) = self
                    .full_sync
                    .diskless_relay
                    .as_ref()
                    .map(|relay| (relay.targets.clone(), relay.marker.clone()))
                    .unwrap_or_default();
                let mut published_classic_online = false;
                for id in targets {
                    let already_sent = self
                        .full_sync
                        .diskless_relay
                        .as_ref()
                        .is_some_and(|relay| relay.marker_sent_targets.contains(&id));
                    if already_sent {
                        continue;
                    }
                    // Send the $EOF trailing marker over the RDB channel (or
                    // main channel when dual-channel is inactive). The final
                    // survivor notice is deferred until target outcomes are
                    // known.
                    let Some(rdb_connection_token) = self.diskless_target_token(id) else {
                        continue;
                    };
                    let rdb_token = rdb_connection_token;
                    let marker_was_queued = self
                        .full_sync
                        .diskless_relay
                        .as_ref()
                        .is_some_and(|relay| relay.marker_queued_targets.contains(&id));
                    if !marker_was_queued {
                        if let Some(conn) = self.connection_registry.get_mut(rdb_connection_token) {
                            conn.queue_reply(marker.clone());
                        }
                        if let Some(relay) = self.full_sync.diskless_relay.as_mut() {
                            relay.marker_queued_targets.insert(id);
                        }
                    }
                    match self.flush_connection_output(rdb_connection_token.0) {
                        WriteOutcome::Failed(_) => {
                            self.remove_connection(rdb_token.0);
                            continue;
                        }
                        WriteOutcome::Complete => {}
                        WriteOutcome::BudgetExhausted | WriteOutcome::Backpressured => {
                            // The marker is queued exactly once, so a
                            // short write must leave the target armed for
                            // the next writable event. Without this, the
                            // relay switches back to readable-only and a
                            // split marker can strand the replica forever.
                            self.transport
                                .set_interest(rdb_token, Interest::ReadWritable)
                                .ok();
                            continue;
                        }
                    }
                    if let Some(relay) = self.full_sync.diskless_relay.as_mut() {
                        relay.marker_sent_targets.insert(id);
                        relay.marker_queued_targets.remove(&id);
                    }
                    let complete_full_resync = self
                        .full_sync
                        .diskless_relay
                        .as_ref()
                        .is_some_and(|relay| relay.complete_full_resync);
                    if complete_full_resync {
                        if let Some(link) = self.replication_runtime.primary_link_mut(id) {
                            link.finish_full_resync();
                            published_classic_online = true;
                        }
                    }
                }
                if published_classic_online {
                    // C's replicaPutOnline() publishes ONLINE as soon as the
                    // diskless RDB is fully written. Command-stream pumping
                    // still waits for the terminal ACK, but INFO must not wait
                    // for that ACK to observe the online state.
                    self.refresh_replication_view();
                }
                self.finish_diskless_relay_if_ready();
                let finish_without_child = self.ctx.persistence().diskless_child_pid().is_none()
                    && self
                        .full_sync
                        .diskless_relay
                        .as_ref()
                        .is_some_and(|relay| relay.completion_announced);
                if finish_without_child {
                    if let Some(relay) = self.full_sync.diskless_relay.take() {
                        self.publish_diskless_relay_targets(relay);
                    }
                }
            }
            Ok(n) => {
                let chunk = buf[..n].to_vec();
                self.ctx.stats_mut().add_net_repl_output_bytes(n as u64);
                if let Some(relay) = self.full_sync.diskless_relay.as_ref() {
                    let ids: Vec<ReplicaId> = relay.targets.clone();
                    for id in ids {
                        // Re-resolve the socket each tick (ABA-safe); see the EOF
                        // branch and `diskless_target_token`.
                        let Some(connection_token) = self.diskless_target_token(id) else {
                            continue;
                        };
                        let token = connection_token;
                        {
                            let Some(conn) = self.connection_registry.get_mut(connection_token)
                            else {
                                continue;
                            };
                            conn.queue_reply(chunk.clone());
                        }
                        // Push the chunk to this replica's socket immediately,
                        // mirroring C's synchronous connWrite in rdbPipeReadHandler
                        // (replication.c:1902). A fast reader drains fully so the
                        // next loop iteration keeps reading the pipe; a slow reader
                        // leaves residue in output_queue, tripping the backpressure
                        // gate above until handle_writable drains it.
                        let result = self.flush_connection_output(connection_token.0);
                        if matches!(result, WriteOutcome::Failed(_)) {
                            // Hard write error (EPIPE/ECONNRESET/peer-closed):
                            // synchronously tear down the dead replica, mirroring
                            // C's `freeClient(replica)` + NULL of the
                            // `rdb_pipe_conns[i]` slot (replication.c:1902-1908).
                            // This immediately drops the replica from
                            // `relay.targets` so the backpressure gate, EOF count,
                            // and subsequent chunks reflect only living replicas.
                            // Without this synchronous removal, the dead replica
                            // would linger in targets until the next poll tick
                            // delivers its HUP/error event, letting the relay read
                            // pipe EOF with an inflated "still up" count.
                            if let Some(conn) = self.connection_registry.get(connection_token) {
                                let token = conn.token();
                                self.remove_connection(token.0);
                            }
                            continue;
                        }
                        if let Some(conn) = self.connection_registry.get(connection_token) {
                            // Keep writable interest only while bytes remain, so a
                            // drained replica does not spin on writable events
                            // (C removes the write handler once fully written).
                            let interest = if conn.output.queue.is_empty() {
                                Interest::Readable
                            } else {
                                Interest::ReadWritable
                            };
                            self.transport.set_interest(token, interest).ok();
                        }
                    }
                }
                // C removes the pipe read handler after every producer
                // chunk and only restores it after all targets have
                // completed that chunk. Yield even when every socket was
                // immediately writable so HUP and timeout work can run
                // before the next producer continuation.
                self.disable_diskless_source_while_backpressured();
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => {
                crate::server_log!(warning, "diskless relay pipe read failed: {}", error);
                // Pipe error: reap and drop the relay; replicas will retry.
                self.fail_diskless_relay();
                self.reap_registered_diskless_child_if_any();
            }
        }
    }

    /// Cron fallback that retires a lingering disk-sync child after its batch
    /// finished. The batch path normally observes runtime child-completion
    /// events, but this keeps the persistence slot recoverable if the batch was
    /// cancelled after the child had already exited.
    pub(crate) fn collect_idle_disk_sync_child_exit(&mut self) {
        if self.full_sync.disk_sync_batch.is_some() {
            return;
        }
        self.reap_registered_disk_sync_child_if_any();
    }
}
