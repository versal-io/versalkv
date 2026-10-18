use super::super::super::ServerWorkerState;
use crate::engine::distributed::cluster::migration::{MigrationEvent, MigrationJobId};
use crate::engine::distributed::replication::protocol::encode_command;
use crate::foundation::io::sys::fd::{pipe, set_nonblocking, PipeFlags};
use crate::foundation::io::sys::process::{
    exit_immediately, send_signal, wait_blocking, ProcessSignal,
};
use crate::foundation::io::transport::OwnedPollSource;
use crate::framework::capabilities::io::Token;
use crate::framework::capabilities::process::{fork_process, ForkBranch};
use crate::runtime::server::worker::reactor::sources::{ChildExitOutcome, RuntimeSourceHandle};
use crate::runtime::server::worker::reactor::transport::PendingRemovalOwner;
use std::fs::File;
use std::os::fd::AsFd;

pub(crate) struct SlotSnapshotRelay {
    pub(crate) job_id: MigrationJobId,
    pub(crate) pid: i32,
    pub(crate) source: Option<RuntimeSourceHandle>,
    pub(crate) completion_state: SlotSnapshotCompletionState,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SlotSnapshotCompletionState {
    pipe_eof: bool,
    child_outcome: Option<ChildExitOutcome>,
    read_failed: bool,
}

#[derive(Clone, Debug)]
pub(crate) enum SnapshotFact {
    PipeEof,
    ReadError,
    ChildExited(ChildExitOutcome),
}

impl SlotSnapshotCompletionState {
    pub(crate) fn apply_fact(&mut self, fact: SnapshotFact) {
        match fact {
            SnapshotFact::PipeEof => self.pipe_eof = true,
            SnapshotFact::ReadError => self.read_failed = true,
            SnapshotFact::ChildExited(outcome) => self.child_outcome = Some(outcome),
        }
    }

    pub(crate) fn can_finish(&self) -> bool {
        !self.read_failed
            && self.pipe_eof
            && matches!(self.child_outcome, Some(ChildExitOutcome::Exited(0)))
    }

    pub(crate) fn failed(&self) -> bool {
        self.read_failed
            || self
                .child_outcome
                .as_ref()
                .is_some_and(|outcome| !matches!(outcome, ChildExitOutcome::Exited(0)))
    }
}

impl ServerWorkerState {
    pub(super) fn try_start_slot_snapshot(&mut self, id: &MigrationJobId) {
        if self.slot_migration_snapshot.is_some() || self.ctx.persistence().has_active_child() {
            return;
        }
        let ranges = self
            .ctx
            .cluster_state()
            .as_deref()
            .and_then(|state| state.migrations.get(id))
            .map(|job| job.ranges.clone());
        let Some(ranges) = ranges else { return };
        let Some((pid, read_end)) = fork_slot_snapshot(&mut self.ctx, &ranges) else {
            self.fail_slot_migration(id, "Failed to start snapshot");
            return;
        };
        if self
            .ctx
            .persistence_mut()
            .begin_slot_migration_child(pid)
            .is_err()
        {
            let _ = send_signal(pid, ProcessSignal::User1);
            let _ = wait_blocking(pid);
            return;
        }
        if set_nonblocking(read_end.as_fd(), true).is_err() {
            let _ = send_signal(pid, ProcessSignal::User1);
            let _ = wait_blocking(pid);
            self.ctx.persistence_mut().complete_slot_migration_child();
            self.fail_slot_migration(id, "Failed to configure snapshot pipe");
            return;
        }
        let Some(source) = self
            .install_runtime_source(
                OwnedPollSource::readable(read_end),
                crate::runtime::server::worker::reactor::sources::SourceOwner::InternalPipe(
                    crate::runtime::server::worker::reactor::sources::InternalPipeOwner::SlotSnapshot {
                        job_id: id.clone(),
                    },
                ),
            )
            .ok()
        else {
            let _ = send_signal(pid, ProcessSignal::User1);
            let _ = wait_blocking(pid);
            self.ctx.persistence_mut().complete_slot_migration_child();
            self.register_active_persistence_child();
            self.fail_slot_migration(id, "Failed to register snapshot pipe");
            return;
        };
        self.register_child_process(
            crate::runtime::server::worker::reactor::sources::ChildOwner::SlotSnapshot(id.clone()),
            pid,
        );
        self.slot_migration_snapshot = Some(SlotSnapshotRelay {
            job_id: id.clone(),
            pid,
            source: Some(source),
            completion_state: SlotSnapshotCompletionState::default(),
        });
        self.transition_slot_migration(id, MigrationEvent::SnapshotStarted);
    }

    pub(crate) fn on_slot_snapshot_pipe_readable(
        &mut self,
        id: MigrationJobId,
    ) -> crate::runtime::server::worker::reactor::DispatchOutcome {
        if self
            .slot_migration_snapshot
            .as_ref()
            .is_none_or(|relay| relay.job_id != id)
        {
            return crate::runtime::server::worker::reactor::DispatchOutcome::Stale;
        }
        let Some(connection_id) = self.slot_migration_runtime.export_connection(&id) else {
            self.abort_slot_snapshot(&id);
            return crate::runtime::server::worker::reactor::DispatchOutcome::Handled;
        };
        let Some(connection_token) = self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(connection_id))
            .then_some(connection_id)
        else {
            self.abort_slot_snapshot(&id);
            return crate::runtime::server::worker::reactor::DispatchOutcome::Handled;
        };
        if self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .has_pending_output()
        {
            self.disable_slot_snapshot_source();
            return crate::runtime::server::worker::reactor::DispatchOutcome::Handled;
        }
        let mut bytes = [0u8; 16 * 1024];
        let source = self
            .slot_migration_snapshot
            .as_ref()
            .and_then(|relay| relay.source);
        let Some(source) = source else {
            return crate::runtime::server::worker::reactor::DispatchOutcome::Stale;
        };
        let read_result = crate::foundation::io::sys::os::retry_on_interrupt(|| {
            self.transport.read_source(source.token, &mut bytes)
        });
        match read_result {
            Ok(0) => {
                self.apply_slot_snapshot_fact(&id, SnapshotFact::PipeEof);
                self.clear_slot_snapshot_source();
            }
            Ok(count) => {
                self.queue_slot_snapshot_chunk(&id, connection_id, bytes[..count].to_vec());
                if self.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)]
                .has_pending_output()
                {
                    self.disable_slot_snapshot_source();
                }
                return crate::runtime::server::worker::reactor::DispatchOutcome::Handled;
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                return crate::runtime::server::worker::reactor::DispatchOutcome::Handled;
            }
            Err(_) => {
                self.apply_slot_snapshot_fact(&id, SnapshotFact::ReadError);
                self.fail_slot_snapshot_relay(&id, "Failed to read slot snapshot");
                return crate::runtime::server::worker::reactor::DispatchOutcome::Handled;
            }
        }
        self.finish_slot_snapshot_if_ready(&id);
        crate::runtime::server::worker::reactor::DispatchOutcome::Handled
    }

    pub(crate) fn on_slot_snapshot_pipe_error(
        &mut self,
        id: MigrationJobId,
    ) -> crate::runtime::server::worker::reactor::DispatchOutcome {
        self.apply_slot_snapshot_fact(&id, SnapshotFact::ReadError);
        self.fail_slot_snapshot_relay(&id, "Failed to read slot snapshot");
        crate::runtime::server::worker::reactor::DispatchOutcome::Handled
    }

    pub(crate) fn on_slot_snapshot_target_writable(&mut self, id: &MigrationJobId) {
        if self
            .slot_migration_snapshot
            .as_ref()
            .is_none_or(|relay| &relay.job_id != id)
        {
            return;
        }
        self.enable_slot_snapshot_source_if_unblocked();
        self.finish_slot_snapshot_if_ready(id);
    }

    pub(crate) fn on_slot_snapshot_target_closed(&mut self, id: &MigrationJobId) {
        if self
            .slot_migration_snapshot
            .as_ref()
            .is_some_and(|relay| &relay.job_id == id)
        {
            self.abort_slot_snapshot(id);
        }
    }

    pub(crate) fn on_slot_snapshot_child_completed(
        &mut self,
        id: MigrationJobId,
        outcome: ChildExitOutcome,
    ) -> crate::runtime::server::worker::reactor::DispatchOutcome {
        let Some(pid) = self
            .slot_migration_snapshot
            .as_ref()
            .filter(|relay| relay.job_id == id)
            .map(|relay| relay.pid)
        else {
            return crate::runtime::server::worker::reactor::DispatchOutcome::Stale;
        };
        let Some((handle, _succeeded)) = self.take_child_exit_success(
            crate::runtime::server::worker::reactor::sources::ChildOwner::SlotSnapshot(id.clone()),
            pid,
        ) else {
            return crate::runtime::server::worker::reactor::DispatchOutcome::Stale;
        };
        self.apply_slot_snapshot_fact(&id, SnapshotFact::ChildExited(outcome));
        if self
            .slot_migration_snapshot
            .as_ref()
            .is_some_and(|relay| relay.completion_state.failed())
        {
            self.ctx.persistence_mut().complete_slot_migration_child();
            self.finalize_child_registry_entry(handle);
            self.fail_slot_snapshot_relay(&id, "Failed to create slot snapshot");
            return crate::runtime::server::worker::reactor::DispatchOutcome::Handled;
        }
        if self
            .slot_migration_snapshot
            .as_ref()
            .is_some_and(|relay| relay.completion_state.can_finish())
        {
            self.finish_slot_snapshot_if_ready(&id);
        }
        crate::runtime::server::worker::reactor::DispatchOutcome::Handled
    }

    pub(super) fn abort_slot_snapshot(&mut self, id: &MigrationJobId) {
        let Some(relay) = self.slot_migration_snapshot.take() else {
            return;
        };
        if &relay.job_id != id {
            self.slot_migration_snapshot = Some(relay);
            return;
        }
        let _ = send_signal(relay.pid, ProcessSignal::User1);
        let _ = wait_blocking(relay.pid);
        self.retire_slot_snapshot_source(relay.source);
        self.ctx.persistence_mut().complete_slot_migration_child();
        self.register_active_persistence_child();
    }

    fn apply_slot_snapshot_fact(&mut self, id: &MigrationJobId, fact: SnapshotFact) {
        if let Some(relay) = self
            .slot_migration_snapshot
            .as_mut()
            .filter(|relay| &relay.job_id == id)
        {
            relay.completion_state.apply_fact(fact);
        }
    }

    fn finish_slot_snapshot_if_ready(&mut self, id: &MigrationJobId) {
        let ready = self
            .slot_migration_snapshot
            .as_ref()
            .is_some_and(|relay| &relay.job_id == id && relay.completion_state.can_finish());
        if !ready {
            return;
        }
        let Some(pid) = self.slot_migration_snapshot.as_ref().map(|relay| relay.pid) else {
            return;
        };
        let Some((handle, succeeded)) = self.take_child_exit_success(
            crate::runtime::server::worker::reactor::sources::ChildOwner::SlotSnapshot(id.clone()),
            pid,
        ) else {
            return;
        };
        if !succeeded {
            self.ctx.persistence_mut().complete_slot_migration_child();
            self.finalize_child_registry_entry(handle);
            self.fail_slot_snapshot_relay(id, "Failed to create slot snapshot");
            return;
        }
        let Some(relay) = self.slot_migration_snapshot.take() else {
            return;
        };
        self.retire_slot_snapshot_source(relay.source);
        self.ctx.persistence_mut().complete_slot_migration_child();
        self.finalize_child_registry_entry(handle);
        if let Some(connection_id) = self.slot_migration_runtime.export_connection(id) {
            for frame in self.take_slot_export_deferred_mutations(id) {
                if !self.queue_slot_migration_output(id, connection_id, frame) {
                    return;
                }
            }
        }
        self.transition_slot_migration(id, MigrationEvent::SnapshotFinished);
    }

    fn fail_slot_snapshot_relay(&mut self, id: &MigrationJobId, message: &str) {
        let Some(relay) = self.slot_migration_snapshot.take() else {
            return;
        };
        if &relay.job_id != id {
            self.slot_migration_snapshot = Some(relay);
            return;
        }
        self.retire_slot_snapshot_source(relay.source);
        if matches!(
            self.ctx.persistence().child(),
            Some(crate::engine::persistence::PersistenceChild::SlotMigration { pid })
                if pid == relay.pid
        ) {
            let _ = send_signal(relay.pid, ProcessSignal::User1);
            let _ = wait_blocking(relay.pid);
            self.ctx.persistence_mut().complete_slot_migration_child();
            self.register_active_persistence_child();
        }
        self.fail_slot_migration(id, message);
    }

    fn clear_slot_snapshot_source(&mut self) {
        let source = self
            .slot_migration_snapshot
            .as_mut()
            .and_then(|relay| relay.source.take());
        self.retire_slot_snapshot_source(source);
    }

    fn retire_slot_snapshot_source(&mut self, source: Option<RuntimeSourceHandle>) {
        if let Some(source) = source {
            self.retire_runtime_source(source.token);
            self.deregister_or_defer(source.token, PendingRemovalOwner::RuntimeSource);
        }
    }

    fn disable_slot_snapshot_source(&mut self) {
        let Some((id, source)) = self
            .slot_migration_snapshot
            .as_ref()
            .and_then(|relay| relay.source.map(|source| (relay.job_id.clone(), source)))
        else {
            return;
        };
        if let Err(error) = self.transport.set_source_enabled(source.token, false) {
            crate::server_log!(
                warning,
                "slot snapshot source lifecycle token={} operation=disable error={}",
                source.token.0,
                error
            );
            self.fail_slot_snapshot_source_lifecycle(&id, source.token, "disable");
        }
    }

    fn enable_slot_snapshot_source_if_unblocked(&mut self) {
        let Some(id) = self
            .slot_migration_snapshot
            .as_ref()
            .map(|relay| relay.job_id.clone())
        else {
            return;
        };
        let Some(connection_id) = self.slot_migration_runtime.export_connection(&id) else {
            return;
        };
        if self
            .connection_registry
            .get(crate::framework::capabilities::io::Token(connection_id))
            .is_some_and(|connection| connection.has_pending_output())
        {
            return;
        }
        let Some((id, source)) = self
            .slot_migration_snapshot
            .as_ref()
            .and_then(|relay| relay.source.map(|source| (relay.job_id.clone(), source)))
        else {
            return;
        };
        if let Err(error) = self.transport.set_source_enabled(source.token, true) {
            crate::server_log!(
                warning,
                "slot snapshot source lifecycle token={} operation=enable error={}",
                source.token.0,
                error
            );
            self.fail_slot_snapshot_source_lifecycle(&id, source.token, "enable");
        }
    }

    fn fail_slot_snapshot_source_lifecycle(
        &mut self,
        id: &MigrationJobId,
        token: Token,
        operation: &str,
    ) {
        self.retire_runtime_source(token);
        self.transport.quarantine_registered(token);
        let _ = self.transport.set_source_enabled(token, false);
        self.deregister_or_defer(token, PendingRemovalOwner::RuntimeSource);
        self.fail_slot_snapshot_relay(id, &format!("Failed to {operation} slot snapshot source"));
    }
}

fn fork_slot_snapshot(
    ctx: &mut crate::framework::context::ServerContext,
    ranges: &crate::engine::distributed::cluster::migration::SlotRanges,
) -> Option<(i32, File)> {
    let snapshot_pipe = pipe(PipeFlags::CLOEXEC).ok()?;
    let listener_fds = ctx.listener_fds().to_vec();
    let key_delay_us = ctx.config().persistence.rdb_key_save_delay;
    let bgsave_affinity =
        match crate::framework::commands::server::save::prepare_bgsave_cpu_affinity(
            &ctx.config().server.bgsave_cpulist,
        ) {
            Ok(affinity) => affinity,
            Err(error) => {
                crate::server_log!(warning, "invalid bgsave-cpulist: {}", error);
                return None;
            }
        };
    let started = std::time::Instant::now();
    let child = match fork_process().ok()? {
        ForkBranch::Parent(child) => child,
        ForkBranch::Child => {
            let (read_end, mut writer) = snapshot_pipe.into_parts();
            drop(read_end);
            crate::framework::commands::server::save::child_after_fork(ctx.config(), &listener_fds);
            if let Some(affinity) = bgsave_affinity.as_ref() {
                let _ = crate::foundation::io::sys::process::apply_prepared_cpu_affinity(affinity);
            }
            let result =
                crate::foundation::io::persistence::aof_rewrite::write_filtered_command_snapshot(
                    ctx.dbs(),
                    |_db, key| {
                        ranges.contains(crate::engine::distributed::cluster::node::key_hash_slot(
                            key,
                        ))
                    },
                    false,
                    key_delay_us,
                    &mut writer,
                )
                .and_then(|_| {
                    std::io::Write::write_all(
                        &mut writer,
                        &encode_command(&[b"CLUSTER", b"SYNCSLOTS", b"SNAPSHOT-EOF"]),
                    )
                });
            drop(writer);
            exit_immediately(if result.is_ok() { 0 } else { 1 });
        }
    };
    let pid = child.into_pid();
    let (read_end, write_end) = snapshot_pipe.into_parts();
    drop(write_end);
    ctx.stats_mut()
        .record_fork(started.elapsed().as_micros() as u64);
    Some((pid, read_end))
}
