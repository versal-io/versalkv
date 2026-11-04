use super::super::super::ServerWorkerState;
use crate::foundation::io::transport::{Interest, OwnedPollSource, WriteOutcome};
use crate::framework::capabilities::io::Token;
use crate::framework::distributed::replication::{ConnectionId, PrimaryLinkState, ReplicaId};
use crate::runtime::server::worker::reactor::sources::{RuntimeSourceHandle, TransferId};
use crate::runtime::server::worker::reactor::transport::PendingRemovalOwner;
use std::collections::HashSet;
use std::io::Write;
use std::os::unix::io::AsRawFd;

/// Classic transport follows a stable replica identity. Dual transport owns
/// the dedicated RDB session from the start of the snapshot, before pairing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum DisklessTarget {
    Replica(ReplicaId),
    Rdb {
        connection_id: ConnectionId,
        client_id: u64,
    },
}

impl From<ReplicaId> for DisklessTarget {
    fn from(id: ReplicaId) -> Self {
        Self::Replica(id)
    }
}

/// In-flight diskless fork relay: bytes stream from the child's pipe to every
/// replica in this batch until EOF, then the trailing `$EOF` marker is sent.
/// The producer is released once every marker drains. Classic targets must
/// acknowledge that boundary (or be removed) before final relay publication.
pub(crate) struct DisklessRelay {
    pub(crate) transfer_id: TransferId,
    pub(crate) source: Option<RuntimeSourceHandle>,
    /// Parent-owned release end matching C's `rdb_child_exit_pipe`. Writing a
    /// byte after every EOF marker drains explicitly releases the child; EOF
    /// remains a fallback when the parent exits.
    pub(crate) child_release_write_end: Option<std::fs::File>,
    /// Fixed snapshot cohort. Dedicated RDB tokens are validated against the
    /// public client identity on every access to reject reused transport slots.
    pub(crate) targets: Vec<DisklessTarget>,
    /// Trailing marker to append after the RDB body ($EOF framing).
    pub(crate) marker: Vec<u8>,
    pub(crate) complete_full_resync: bool,
    pub(crate) not_before_ms: i64,
    pub(crate) eof_reached: bool,
    /// Targets whose EOF marker has been queued but is not fully flushed yet.
    /// The marker must be queued once across Partial/WouldBlock retries.
    pub(crate) marker_queued_targets: HashSet<DisklessTarget>,
    pub(crate) marker_sent_targets: HashSet<DisklessTarget>,
    pub(crate) completion_announced: bool,
    pub(crate) completion_state: DisklessCompletionState,
}

impl DisklessRelay {
    pub(crate) fn release_child(&mut self) {
        if let Some(mut write_end) = self.child_release_write_end.take() {
            // An explicit byte releases the child even if a concurrent test
            // fork inherited another copy of this write descriptor.
            let _ = write_end.write_all(&[1]);
        }
    }
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

    #[cfg(test)]
    pub(crate) fn on_diskless_target_writable(
        &mut self,
        id: ReplicaId,
    ) -> crate::runtime::server::worker::reactor::DispatchOutcome {
        if self.full_sync.diskless_relay.as_ref().is_none_or(|relay| {
            !relay
                .targets
                .iter()
                .any(|target| self.diskless_target_replica_id(*target) == Some(id))
        }) {
            return crate::runtime::server::worker::reactor::DispatchOutcome::Stale;
        }
        self.flush_diskless_relay_targets();
        self.finish_diskless_relay_if_ready();
        crate::runtime::server::worker::reactor::DispatchOutcome::Handled
    }

    pub(crate) fn on_diskless_terminal_ack(
        &mut self,
        id: ReplicaId,
    ) -> crate::runtime::server::worker::reactor::DispatchOutcome {
        if self.full_sync.diskless_relay.as_ref().is_none_or(|relay| {
            !relay
                .targets
                .iter()
                .any(|target| self.diskless_target_replica_id(*target) == Some(id))
        }) {
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

    #[cfg(test)]
    pub(crate) fn try_begin_diskless_fork(
        &mut self,
        libs: &[Vec<u8>],
    ) -> Option<(std::fs::File, Vec<u8>, std::fs::File)> {
        let snapshot = crate::foundation::io::persistence::rdb::compression::RdbSnapshotOptions {
            compression: self.ctx.config().persistence.rdbcompression,
            checksum: self.ctx.config().persistence.rdbchecksum,
        };
        self.try_begin_diskless_fork_with_options(libs, snapshot, "normal sync", true)
    }

    pub(crate) fn try_begin_diskless_fork_with_options(
        &mut self,
        libs: &[Vec<u8>],
        snapshot: crate::foundation::io::persistence::rdb::compression::RdbSnapshotOptions,
        sync_target_label: &'static str,
        pause_after_fork: bool,
    ) -> Option<(std::fs::File, Vec<u8>, std::fs::File)> {
        if self.ctx.persistence().has_active_child() || self.full_sync.diskless_relay.is_some() {
            return None;
        }
        if !pause_after_fork && self.ctx.config().server.debug_pause_after_fork {
            if let Err(error) = self.quiesce_ordinary_threaded_writes() {
                crate::server_log!(
                    warning,
                    "Failed to reclaim ordinary writes before the dual-channel pause: {}",
                    error
                );
                return None;
            }
        }
        let child = crate::framework::commands::server::save::fork_diskless_rdb_child_with_options(
            &mut self.ctx,
            libs,
            snapshot,
        )?;
        crate::server_log!(
            notice,
            "Starting BGSAVE for SYNC with target: replicas sockets using: {}, compression: {}",
            sync_target_label,
            if snapshot.compression
                == crate::foundation::io::persistence::rdb::compression::RdbCompression::Lz4
            {
                "lz4"
            } else {
                "none"
            }
        );
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
        if pause_after_fork
            && crate::framework::commands::server::save::pause_parent_after_replication_fork(
                &self.ctx,
            )
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
        self.store_diskless_transport_relay(
            read_end,
            child_release_write_end,
            marker,
            targets.into_iter().map(DisklessTarget::Replica).collect(),
            complete_full_resync,
            not_before_ms,
        );
    }

    pub(crate) fn store_diskless_transport_relay(
        &mut self,
        read_end: std::fs::File,
        child_release_write_end: std::fs::File,
        marker: Vec<u8>,
        targets: Vec<DisklessTarget>,
        complete_full_resync: bool,
        not_before_ms: i64,
    ) {
        let has_targets = !targets.is_empty();
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
        if !has_targets {
            self.abort_empty_diskless_relay();
        }
    }

    pub(crate) fn diskless_relay_target_for_connection(
        &self,
        connection_id: ConnectionId,
    ) -> Option<DisklessTarget> {
        self.full_sync
            .diskless_relay
            .as_ref()?
            .targets
            .iter()
            .copied()
            .find(|target| {
                self.diskless_target_token(*target) == Some(Token(connection_id.0))
                    || self.diskless_target_replica_id(*target)
                        == self.replication_runtime.primary_id(connection_id)
                        && self.replication_runtime.primary_id(connection_id).is_some()
            })
    }

    pub(crate) fn on_diskless_connection_writable(&mut self, token: Token) {
        if self
            .diskless_relay_target_for_connection(ConnectionId(token.0))
            .is_some()
        {
            self.flush_diskless_relay_targets();
            self.finish_diskless_relay_if_ready();
        }
    }

    /// Drop a closing replica from the in-flight diskless relay. If it was the
    /// last remaining target, log the notice, terminate and reap the fork child
    /// before releasing its persistence slot. C's `killRDBChild()` marks the
    /// child for immediate async cleanup; Rust must not leave the slot occupied
    /// between adjacent full-sync attempts, or a deferred second sync can wait
    /// indefinitely behind a killed child. Call this from `remove_connection`
    /// with the `ReplicaId` resolved before the `PrimaryLink` is erased.
    #[cfg(test)]
    pub(crate) fn drop_diskless_relay_target(&mut self, id: ReplicaId) {
        if self
            .full_sync
            .diskless_relay
            .as_ref()
            .is_some_and(|relay| relay.targets.is_empty())
        {
            self.abort_empty_diskless_relay();
            return;
        }
        let target = self.full_sync.diskless_relay.as_ref().and_then(|relay| {
            relay
                .targets
                .iter()
                .copied()
                .find(|target| self.diskless_target_replica_id(*target) == Some(id))
        });
        if let Some(target) = target {
            self.drop_diskless_transport_target(target);
        }
    }

    pub(crate) fn drop_diskless_transport_target(&mut self, target: DisklessTarget) {
        let has_survivors = {
            let Some(relay) = self.full_sync.diskless_relay.as_mut() else {
                return;
            };
            if !relay.targets.contains(&target) {
                return;
            }
            relay.targets.retain(|&t| t != target);
            relay.marker_queued_targets.remove(&target);
            relay.marker_sent_targets.remove(&target);
            !relay.targets.is_empty()
        };
        if has_survivors {
            self.finish_diskless_relay_if_ready();
            self.enable_diskless_source_if_unblocked();
            return;
        }
        self.abort_empty_diskless_relay();
    }

    fn abort_empty_diskless_relay(&mut self) {
        if let Some(pid) = self.ctx.persistence().diskless_child_pid() {
            crate::server_log!(
                notice,
                "Diskless rdb transfer, last replica dropped, killing fork child."
            );
            crate::server_log!(warning, "Background RDB transfer error");
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
        // The relay no longer has transport work that could require a pause.
        // Surviving main channels may retain a PSYNC grace wait after their
        // RDB channel closes, so their association reply must not be stranded.
        self.release_paused_dual_main_replies();
    }

    pub(crate) fn finish_diskless_relay_if_ready(&mut self) {
        let is_drained = self.full_sync.diskless_relay.as_ref().is_some_and(|relay| {
            relay.eof_reached
                && relay
                    .targets
                    .iter()
                    .all(|id| relay.marker_sent_targets.contains(id))
        });
        if !is_drained {
            return;
        }
        let relay = self
            .full_sync
            .diskless_relay
            .as_mut()
            .expect("drained diskless relay must remain installed");
        relay.completion_state.apply_fact(DisklessFact::PipeEof);
        relay
            .completion_state
            .apply_fact(DisklessFact::MarkersFlushed);
        // C announces physical transport completion and releases the child
        // before the terminal ACK admits the replication command stream.
        relay.release_child();
        if !relay.completion_announced {
            crate::server_log!(
                notice,
                "Diskless rdb transfer, done reading from pipe, {} replicas still up.",
                relay.targets.len()
            );
            let should_pause_after_dual_relay =
                !relay.complete_full_resync && self.ctx.config().server.debug_pause_after_fork;
            relay.completion_announced = true;
            if should_pause_after_dual_relay
                && crate::framework::commands::server::save::pause_parent_after_replication_fork(
                    &self.ctx,
                )
            {
                crate::server_log!(
                    debug,
                    "dual-channel diskless relay pause: phase=after-sigcont"
                );
            }
            self.release_paused_dual_main_replies();
        }
        // Reply enqueueing may synchronously close the last target after an
        // output-buffer overrun and retire the relay along with its child.
        let Some(relay) = self.full_sync.diskless_relay.as_mut() else {
            return;
        };
        if !relay.complete_full_resync
            || relay.targets.iter().all(|id| {
                self.replication_runtime
                    .primary_link(match id {
                        DisklessTarget::Replica(id) => *id,
                        DisklessTarget::Rdb { .. } => return false,
                    })
                    .is_some_and(|link| link.state() == PrimaryLinkState::Streaming)
            })
        {
            relay
                .completion_state
                .apply_fact(DisklessFact::TerminalAckSatisfied);
        }
        let _ = self.reap_diskless_relay_child_if_done();
    }

    fn release_paused_dual_main_replies(&mut self) {
        // Remove all barriers before rescheduling or enqueueing output.
        let deferred_replies = std::mem::take(&mut self.full_sync.paused_dual_main_replies);
        let paused_clients = std::mem::take(&mut self.full_sync.paused_dual_clients);
        for token in paused_clients {
            let Some(connection) = self.connection_registry.get(token) else {
                continue;
            };
            let interest = if connection.has_pending_output() {
                Interest::ReadWritable
            } else {
                Interest::Readable
            };
            if self.transport.set_interest(token, interest).is_err() {
                self.remove_connection(token.0);
                continue;
            }
            self.queue_client_for_reprocessing(token);
            self.schedule_ordinary_write(token.0);
        }
        for (token, replies) in deferred_replies {
            for reply in replies {
                self.queue_client_reply(token, reply);
            }
        }
    }

    /// Pairing changes the command-stream owner, never the RDB transport cohort.
    pub(super) fn diskless_target_replica_id(&self, target: DisklessTarget) -> Option<ReplicaId> {
        match target {
            DisklessTarget::Replica(id) => Some(id),
            DisklessTarget::Rdb { connection_id, .. } => {
                self.diskless_target_token(target)?;
                self.replication_runtime
                    .replica_id_for_connection(connection_id)
            }
        }
    }

    fn diskless_target_token(&self, target: DisklessTarget) -> Option<Token> {
        let id = match target {
            DisklessTarget::Replica(id) => id,
            DisklessTarget::Rdb {
                connection_id,
                client_id,
            } => {
                let token = Token(connection_id.0);
                return self
                    .connection_registry
                    .get(token)
                    .filter(|connection| connection.client_id() == client_id)
                    .map(|_| token);
            }
        };
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
    fn diskless_main_target_token(&self, target: DisklessTarget) -> Option<Token> {
        let id = self.diskless_target_replica_id(target)?;
        let main_connection_id = self.replication_runtime.primary_link(id)?.connection_id();
        let token = Token(main_connection_id.0);
        self.connection_registry.contains(token).then_some(token)
    }

    /// Collect the producer's exit independently of terminal ACK. Its process
    /// slot and metrics end when it exits; the relay retains the completion
    /// fact until all replica outcomes allow publication. New diskless forks
    /// remain excluded while that relay still owns the transport cohort.
    pub(crate) fn collect_idle_diskless_child_exit(&mut self) {
        if self.full_sync.diskless_relay.is_some() {
            let _ = self.reap_diskless_relay_child_if_done();
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
        self.close_diskless_relay_targets(&targets);
        self.release_paused_dual_main_replies();
    }

    fn reap_diskless_relay_child_if_done(&mut self) -> bool {
        let Some(pid) = self.ctx.persistence().diskless_child_pid() else {
            if self
                .full_sync
                .diskless_relay
                .as_ref()
                .is_some_and(|relay| relay.completion_state.published)
            {
                let relay = self
                    .full_sync
                    .diskless_relay
                    .take()
                    .expect("published diskless relay was verified above");
                self.publish_diskless_relay_targets(relay);
                return true;
            }
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
        // Reaping ends ownership of the PID and its permanently readable
        // pidfd even when the relay is still waiting for a replica's ACK.
        self.ctx.persistence_mut().complete_diskless_child();
        self.finalize_child_registry_entry(handle);
        if child_succeeded && !ready_to_publish {
            return true;
        }
        let relay = self.full_sync.diskless_relay.take();
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
            self.release_paused_dual_main_replies();
        }
        true
    }

    fn close_diskless_relay_targets(&mut self, targets: &[DisklessTarget]) {
        let connections: Vec<Token> = targets
            .iter()
            .filter_map(|target| {
                self.diskless_main_target_token(*target)
                    .or_else(|| self.diskless_target_token(*target))
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
        self.release_paused_dual_main_replies();
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
                if let Some(link) = self
                    .diskless_target_replica_id(id)
                    .and_then(|replica_id| self.replication_runtime.primary_link_mut(replica_id))
                {
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
                self.drop_diskless_transport_target(id);
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

    fn mark_diskless_marker_sent_if_drained(&mut self, id: DisklessTarget) {
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
        self.complete_diskless_target(id, complete_full_resync);
    }

    fn complete_diskless_target(&mut self, target: DisklessTarget, complete_full_resync: bool) {
        if let DisklessTarget::Rdb { connection_id, .. } = target {
            if self.diskless_target_token(target).is_some() {
                self.replication_runtime
                    .mark_rdb_snapshot_complete(connection_id);
            }
        } else if let Some(link) = self
            .diskless_target_replica_id(target)
            .and_then(|id| self.replication_runtime.primary_link_mut(id))
        {
            if complete_full_resync {
                link.finish_full_resync();
            } else {
                link.mark_dual_rdb_complete();
            }
        }
    }

    /// Pump a diskless child's pipe into replica output queues. On EOF, append
    /// each `$EOF` marker and move classic targets into their terminal-ACK
    /// wait. Release the child after its markers drain; final publication also
    /// requires every survivor to ACK or be removed and a successful child exit.
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
                    // main channel when dual-channel is inactive). Transport
                    // completion is announced after every surviving marker
                    // drains, independently of later terminal ACKs.
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
                    self.complete_diskless_target(id, complete_full_resync);
                    published_classic_online |= complete_full_resync;
                }
                if published_classic_online {
                    // C's replicaPutOnline() publishes ONLINE as soon as the
                    // diskless RDB is fully written. Command-stream pumping
                    // still waits for the terminal ACK, but INFO must not wait
                    // for that ACK to observe the online state.
                    self.refresh_replication_view();
                }
                self.finish_diskless_relay_if_ready();
            }
            Ok(n) => {
                let chunk = buf[..n].to_vec();
                if let Some(relay) = self.full_sync.diskless_relay.as_ref() {
                    let ids = relay.targets.clone();
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
