use super::super::super::ServerWorkerState;
use crate::engine::distributed::replication::SnapshotBoundary;
use crate::foundation::io::transport::{Interest, WriteOutcome};
use crate::framework::capabilities::io::Token;
use crate::framework::distributed::replication::ReplicaId;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::time::Instant;

const DISK_SYNC_BUFFER_BYTES: usize = 128 * 1024;
const DISK_SYNC_BYTES_PER_TARGET_PER_TURN: usize = 256 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DiskSyncPhase {
    Producing,
    Serving,
}

#[derive(Debug)]
pub(crate) struct DiskSyncServingTarget {
    pub(crate) file: File,
    pub(crate) remaining_bytes: u64,
    pub(crate) header_pending: bool,
    pub(crate) bytes_sent: u64,
    pub(crate) would_block_count: u64,
    pending: Vec<u8>,
    pending_offset: usize,
    write_blocked: bool,
}

impl DiskSyncServingTarget {
    fn new(file: File, remaining_bytes: u64) -> Self {
        Self {
            file,
            remaining_bytes,
            header_pending: true,
            bytes_sent: 0,
            would_block_count: 0,
            pending: Vec::with_capacity(DISK_SYNC_BUFFER_BYTES),
            pending_offset: 0,
            write_blocked: false,
        }
    }

    fn record_flush_result(&mut self, outcome: &WriteOutcome) {
        self.write_blocked = matches!(outcome, WriteOutcome::Backpressured);
        if self.write_blocked {
            self.would_block_count = self.would_block_count.saturating_add(1);
        }
    }

    fn refill_pending(&mut self, file_len: u64) -> io::Result<()> {
        if self.pending_offset < self.pending.len() || self.remaining_bytes == 0 {
            return Ok(());
        }
        self.pending.clear();
        let to_read = (self.remaining_bytes as usize).min(DISK_SYNC_BUFFER_BYTES);
        self.pending.resize(to_read, 0);
        let n = crate::foundation::io::sys::os::retry_on_interrupt(|| {
            self.file.read(&mut self.pending[..to_read])
        })?;
        if n == 0 {
            self.pending.clear();
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "snapshot ended with {}/{} bytes still expected",
                    self.remaining_bytes, file_len
                ),
            ));
        }
        self.pending.truncate(n);
        self.pending_offset = 0;
        Ok(())
    }

    fn advance_written(&mut self, written: usize) {
        self.pending_offset = self.pending_offset.saturating_add(written);
        self.remaining_bytes = self.remaining_bytes.saturating_sub(written as u64);
        self.bytes_sent = self.bytes_sent.saturating_add(written as u64);
        self.write_blocked = false;
        if self.pending_offset == self.pending.len() {
            self.pending.clear();
            self.pending_offset = 0;
        }
    }

    fn write_pending(
        &mut self,
        writer: &mut impl Write,
        limit: usize,
    ) -> io::Result<Option<usize>> {
        let end = self.pending_offset.saturating_add(limit);
        let result = crate::foundation::io::sys::os::retry_on_interrupt(|| {
            writer.write(&self.pending[self.pending_offset..end])
        });
        match result {
            Ok(0) => Err(io::ErrorKind::WriteZero.into()),
            Ok(written) => {
                self.advance_written(written);
                Ok(Some(written))
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                self.record_flush_result(&WriteOutcome::Backpressured);
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DiskSyncServeProgress {
    written: usize,
    blocked: bool,
    complete: bool,
}

#[derive(Debug)]
enum DiskSyncServeError {
    Snapshot(io::Error),
    Transport(io::Error),
}

#[derive(Debug)]
pub(crate) struct DiskSyncBatch {
    pub(crate) child_pid: Option<i32>,
    pub(crate) snapshot_path: PathBuf,
    pub(crate) boundary: SnapshotBoundary,
    pub(crate) phase: DiskSyncPhase,
    pub(crate) file_len: u64,
    pub(crate) targets: HashMap<ReplicaId, Option<DiskSyncServingTarget>>,
    dirty_at_fork: i64,
    persist_when_targetless: bool,
    production_started_at: Instant,
    serving_started_at: Option<Instant>,
}

impl DiskSyncBatch {
    pub(crate) fn new_producing(
        child_pid: i32,
        snapshot_path: PathBuf,
        boundary: SnapshotBoundary,
        dirty_at_fork: i64,
    ) -> Self {
        Self {
            child_pid: Some(child_pid),
            snapshot_path,
            boundary,
            phase: DiskSyncPhase::Producing,
            file_len: 0,
            targets: HashMap::new(),
            dirty_at_fork,
            persist_when_targetless: false,
            production_started_at: Instant::now(),
            serving_started_at: None,
        }
    }

    pub(crate) fn add_waiting_target(&mut self, id: ReplicaId) {
        self.targets.entry(id).or_insert(None);
    }

    pub(crate) fn contains_target(&self, id: ReplicaId) -> bool {
        self.targets.contains_key(&id)
    }

    pub(crate) fn target_count(&self) -> usize {
        self.targets.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }

    pub(crate) fn remove_target(&mut self, id: ReplicaId) -> bool {
        self.targets.remove(&id).is_some()
    }

    pub(crate) fn target_ids(&self) -> Vec<ReplicaId> {
        self.targets.keys().copied().collect()
    }

    pub(crate) fn child_pid(&self) -> Option<i32> {
        self.child_pid
    }

    pub(crate) fn mark_child_reaped(&mut self) {
        self.child_pid = None;
    }

    fn cleanup(&mut self) {
        if let Some(pid) = self.child_pid.take() {
            if matches!(
                crate::foundation::io::sys::process::wait_nonblocking(pid),
                Ok(crate::foundation::io::sys::process::WaitOutcome::Running)
            ) {
                let _ = crate::foundation::io::sys::process::send_signal(
                    pid,
                    crate::foundation::io::sys::process::ProcessSignal::Kill,
                );
                let _ = crate::foundation::io::sys::process::wait_blocking(pid);
            }
        }
        let _ = std::fs::remove_file(&self.snapshot_path);
    }

    pub(crate) fn promote_to_serving(&mut self, file_len: u64) -> std::io::Result<()> {
        debug_assert!(self.phase == DiskSyncPhase::Producing);
        self.phase = DiskSyncPhase::Serving;
        self.file_len = file_len;
        self.serving_started_at = Some(Instant::now());
        for target_state in self.targets.values_mut() {
            let file = File::open(&self.snapshot_path)?;
            *target_state = Some(DiskSyncServingTarget::new(file, file_len));
        }
        Ok(())
    }

    pub(crate) fn serving_target_mut(
        &mut self,
        id: ReplicaId,
    ) -> Option<&mut DiskSyncServingTarget> {
        self.targets.get_mut(&id)?.as_mut()
    }
}

impl Drop for DiskSyncBatch {
    fn drop(&mut self) {
        self.cleanup();
    }
}

impl ServerWorkerState {
    fn disk_sync_batch_target_token(&self, id: ReplicaId) -> Option<Token> {
        let connection_id = self.replication_runtime.primary_link(id)?.connection_id();
        let token = Token(connection_id.0);
        (self.connection_registry.contains(token) && self.transport.is_registered(token))
            .then_some(token)
    }

    pub(crate) fn drop_disk_sync_batch_target(&mut self, id: ReplicaId) {
        let persistence_enabled = !self.ctx.config().persistence.saveparams.is_empty();
        let Some(batch) = self.full_sync.disk_sync_batch.as_mut() else {
            return;
        };
        if !batch.remove_target(id) {
            return;
        }
        let preserve_producing_child = batch.phase == DiskSyncPhase::Producing
            && (persistence_enabled || batch.persist_when_targetless);
        if batch.is_empty() && preserve_producing_child {
            batch.persist_when_targetless = true;
            return;
        }
        if batch.is_empty()
            && batch.phase == DiskSyncPhase::Serving
            && batch.persist_when_targetless
        {
            let snapshot_path = batch.snapshot_path.clone();
            let dirty_at_fork = batch.dirty_at_fork;
            self.finish_targetless_disk_sync_save(snapshot_path, dirty_at_fork);
            return;
        }
        if batch.is_empty() && !preserve_producing_child {
            let pid = batch.child_pid();
            let phase = batch.phase;
            self.full_sync.disk_sync_batch = None;
            if phase == DiskSyncPhase::Producing
                && self.ctx.persistence().disk_sync_child_pid() == pid
            {
                crate::server_log!(
                    notice,
                    "Disk-based rdb transfer, last replica dropped, killing fork child."
                );
            }
            let _ = self.reap_disk_sync_file_child_event();
        }
    }

    pub(crate) fn begin_disk_sync_batch(
        &mut self,
        first_target: ReplicaId,
        libs: &[Vec<u8>],
    ) -> Option<SnapshotBoundary> {
        if self.full_sync.disk_sync_batch.is_some() {
            return None;
        }
        if self.ctx.persistence().has_active_child() {
            return None;
        }
        let boundary = self.ctx.replication().backlog.snapshot_boundary();
        let dir = self.ctx.config().persistence.dir.clone();
        let unique = format!("temp-disk-sync-{}.rdb", std::process::id());
        let snapshot_path = std::path::PathBuf::from(dir).join(unique);
        let pid = crate::framework::commands::server::save::fork_disk_rdb_file_child(
            &mut self.ctx,
            libs,
            &snapshot_path,
        )?;
        self.register_child_process(
            crate::runtime::server::worker::reactor::sources::ChildOwner::DiskSyncReplication,
            pid,
        );
        // The RDB fixes the dataset at this fork. Replicas load it with their
        // command stream reset to DB 0, so force the first following write to
        // carry an explicit SELECT just as C Valkey does after replication
        // BGSAVE setup.
        self.ctx.reset_repl_seldb();
        // Align with C's startBgsaveForReplication: pause the parent right after
        // forking the replication RDB child when `debug pause-after-fork` is set,
        // so a paused primary is observable (`T` state) before the transfer.
        if crate::framework::commands::server::save::pause_parent_after_replication_fork(&self.ctx)
        {
            crate::server_log!(
                debug,
                "disk-sync batch replication fork pause: pid={} phase=after-sigcont",
                pid
            );
        }
        let dirty_at_fork = self.ctx.persistence().dirty();
        let mut batch = DiskSyncBatch::new_producing(pid, snapshot_path, boundary, dirty_at_fork);
        batch.add_waiting_target(first_target);
        self.full_sync.disk_sync_batch = Some(batch);
        let failed_connection = self
            .replication_runtime
            .primary_link_mut(first_target)
            .and_then(|link| {
                link.set_full_resync_boundary(boundary)
                    .is_err()
                    .then_some(link.connection_id())
            });
        if let Some(connection_id) = failed_connection {
            self.remove_connection(connection_id.0);
            return None;
        }
        Some(boundary)
    }

    pub(crate) fn join_disk_sync_batch(&mut self, id: ReplicaId) -> Option<SnapshotBoundary> {
        let batch = self.full_sync.disk_sync_batch.as_mut()?;
        let boundary = batch.boundary;
        let phase = batch.phase;
        let file_len = batch.file_len;
        let path = batch.snapshot_path.clone();
        let is_new = !batch.contains_target(id);
        batch.add_waiting_target(id);
        if phase == DiskSyncPhase::Serving && is_new {
            if let Some(target_state) = batch.targets.get_mut(&id) {
                match File::open(&path) {
                    Ok(f) => {
                        *target_state = Some(DiskSyncServingTarget::new(f, file_len));
                    }
                    Err(e) => {
                        crate::server_log!(
                            warning,
                            "Failed to open snapshot for late-joining replica: {}",
                            e
                        );
                    }
                }
            }
        }
        let failed_connection = self
            .replication_runtime
            .primary_link_mut(id)
            .and_then(|link| {
                link.set_full_resync_boundary(boundary)
                    .is_err()
                    .then_some(link.connection_id())
            });
        if let Some(connection_id) = failed_connection {
            self.remove_connection(connection_id.0);
            return None;
        }
        if phase == DiskSyncPhase::Serving {
            if let Some(connection_token) = self.disk_sync_batch_target_token(id) {
                if let Some(conn) = self.connection_registry.get_mut(connection_token) {
                    let token = conn.token();
                    self.transport
                        .set_interest(token, Interest::ReadWritable)
                        .ok();
                }
            }
        }
        Some(boundary)
    }

    fn reap_disk_sync_file_child_event(
        &mut self,
    ) -> crate::framework::commands::server::save::DiskSyncChildExit {
        let Some(pid) = self.ctx.persistence().disk_sync_child_pid() else {
            return crate::framework::commands::server::save::DiskSyncChildExit::NotRunning;
        };
        let Some((handle, succeeded)) = self.take_child_exit_success(
            crate::runtime::server::worker::reactor::sources::ChildOwner::DiskSyncReplication,
            pid,
        ) else {
            return crate::framework::commands::server::save::DiskSyncChildExit::Running;
        };
        self.ctx.persistence_mut().complete_disk_sync_child();
        self.finalize_child_registry_entry(handle);
        if succeeded {
            crate::framework::commands::server::save::DiskSyncChildExit::Succeeded
        } else {
            crate::framework::commands::server::save::DiskSyncChildExit::Failed
        }
    }

    fn complete_disk_sync_production(&mut self) -> Option<bool> {
        let batch = self.full_sync.disk_sync_batch.as_mut()?;
        batch.mark_child_reaped();
        if batch.is_empty() && batch.persist_when_targetless {
            let snapshot_path = batch.snapshot_path.clone();
            let dirty_at_fork = batch.dirty_at_fork;
            self.finish_targetless_disk_sync_save(snapshot_path, dirty_at_fork);
            return None;
        }
        let file_len = match std::fs::metadata(&batch.snapshot_path) {
            Ok(metadata) => metadata.len(),
            Err(error) => {
                self.fail_disk_sync_batch(&format!(
                    "snapshot child reported success but the RDB is unreadable: {error}"
                ));
                return None;
            }
        };
        let target_count = batch.target_count();
        let production_elapsed_ms = batch.production_started_at.elapsed().as_millis();
        if let Err(error) = batch.promote_to_serving(file_len) {
            self.fail_disk_sync_batch(&format!(
                "failed to open the RDB for serving targets: {error}"
            ));
            return None;
        }
        crate::server_log!(
            notice,
            "disk-sync phase=produced bytes={} targets={} elapsed_ms={}",
            file_len,
            target_count,
            production_elapsed_ms
        );
        Some(true)
    }

    pub(crate) fn on_disk_sync_child_completed(
        &mut self,
        child: crate::runtime::server::worker::reactor::sources::ChildHandle,
        outcome: crate::runtime::server::worker::reactor::sources::ChildExitOutcome,
    ) -> crate::runtime::server::worker::reactor::DispatchOutcome {
        let Some(expected_child) = self
            .full_sync
            .disk_sync_batch
            .as_ref()
            .and_then(|batch| batch.child_pid())
            .and_then(|pid| self.child_registry.child_for_pid(pid))
        else {
            return crate::runtime::server::worker::reactor::DispatchOutcome::Stale;
        };
        if expected_child != child {
            return crate::runtime::server::worker::reactor::DispatchOutcome::Stale;
        }
        self.ctx.persistence_mut().complete_disk_sync_child();
        self.finalize_child_registry_entry(child);
        if matches!(
            outcome,
            crate::runtime::server::worker::reactor::sources::ChildExitOutcome::Exited(0)
        ) {
            if let Some(phase_promoted) = self.complete_disk_sync_production() {
                self.arm_disk_sync_batch_targets_writable();
                self.service_disk_sync_batch_targets_once();
                self.cleanup_finished_disk_sync_batch(phase_promoted);
            }
        } else {
            self.fail_disk_sync_batch("snapshot child failed before producing a complete RDB");
        }
        crate::runtime::server::worker::reactor::DispatchOutcome::Handled
    }

    fn fail_disk_sync_batch(&mut self, reason: &str) {
        let Some(mut batch) = self.full_sync.disk_sync_batch.take() else {
            return;
        };
        batch.mark_child_reaped();
        let persistence_dirty = batch.persist_when_targetless.then_some(batch.dirty_at_fork);
        let failed_ids = batch.target_ids();
        crate::server_log!(
            warning,
            "Disk-based rdb batch failed ({}), aborting full resync for {} waiting replicas.",
            reason,
            failed_ids.len()
        );
        let _ = std::fs::remove_file(&batch.snapshot_path);
        drop(batch);
        if let Some(dirty_at_fork) = persistence_dirty {
            let now_sec = crate::foundation::io::store::kv_engine::now_ms() / 1_000;
            self.ctx.persistence_mut().record_promoted_rdb_save(
                dirty_at_fork,
                false,
                now_sec as i64,
            );
        }
        self.close_disk_sync_batch_targets(failed_ids);
        self.refresh_replication_view();
    }

    fn close_disk_sync_batch_targets(&mut self, ids: Vec<ReplicaId>) {
        for id in ids {
            if let Some(connection_token) = self.disk_sync_batch_target_token(id) {
                self.remove_connection(connection_token.0);
            }
        }
    }

    fn arm_disk_sync_batch_targets_writable(&mut self) {
        let target_ids = self
            .full_sync
            .disk_sync_batch
            .as_ref()
            .map(DiskSyncBatch::target_ids)
            .unwrap_or_default();
        for id in target_ids {
            if let Some(connection_token) = self.disk_sync_batch_target_token(id) {
                self.transport
                    .set_interest(connection_token, Interest::ReadWritable)
                    .ok();
            }
        }
    }

    fn service_disk_sync_batch_targets_once(&mut self) {
        let target_ids = self
            .full_sync
            .disk_sync_batch
            .as_ref()
            .map(DiskSyncBatch::target_ids)
            .unwrap_or_default();
        for id in target_ids {
            let Some(connection_token) = self.disk_sync_batch_target_token(id) else {
                continue;
            };
            self.handle_writable(connection_token);
        }
    }

    fn serve_disk_sync_target(&mut self, id: ReplicaId) -> bool {
        let Some(connection_token) = self.disk_sync_batch_target_token(id) else {
            return true;
        };
        let token = connection_token;
        if !self.connection_registry[connection_token]
            .output
            .queue
            .is_empty()
        {
            return false;
        }
        match self.flush_disk_sync_header(id, connection_token, token) {
            Ok(true) => {}
            Ok(false) => return false,
            Err(error) => return self.handle_disk_sync_serve_error(id, token, "header", error),
        }
        let progress = match self.write_disk_sync_target_turn(id, token) {
            Ok(progress) => progress,
            Err(error) => return self.handle_disk_sync_serve_error(id, token, "payload", error),
        };
        self.record_direct_disk_sync_write(connection_token, progress.written);
        let interest = if progress.complete {
            Interest::Readable
        } else {
            Interest::ReadWritable
        };
        self.transport.set_interest(token, interest).ok();
        progress.complete
    }

    pub(crate) fn on_disk_sync_batch_target_writable(
        &mut self,
        id: ReplicaId,
    ) -> crate::runtime::server::worker::reactor::DispatchOutcome {
        let is_serving_target = self
            .full_sync
            .disk_sync_batch
            .as_ref()
            .is_some_and(|batch| {
                batch.phase == DiskSyncPhase::Serving && batch.contains_target(id)
            });
        if !is_serving_target {
            return crate::runtime::server::worker::reactor::DispatchOutcome::Stale;
        }
        let completed = self.serve_disk_sync_target(id);
        if completed {
            self.finish_disk_sync_targets(vec![id]);
        }
        self.cleanup_finished_disk_sync_batch(false);
        crate::runtime::server::worker::reactor::DispatchOutcome::Handled
    }

    fn handle_disk_sync_serve_error(
        &mut self,
        id: ReplicaId,
        token: crate::framework::capabilities::io::Token,
        stage: &str,
        error: DiskSyncServeError,
    ) -> bool {
        match error {
            DiskSyncServeError::Snapshot(error) => {
                self.fail_disk_sync_batch(&format!("snapshot transfer read failed: {error}"));
            }
            DiskSyncServeError::Transport(error) => {
                crate::server_log!(
                    warning,
                    "disk-sync {} write failed for replica {}: {}",
                    stage,
                    id.0,
                    error
                );
                self.remove_connection(token.0);
            }
        }
        false
    }

    fn flush_disk_sync_header(
        &mut self,
        id: ReplicaId,
        connection_token: Token,
        token: Token,
    ) -> Result<bool, DiskSyncServeError> {
        let Some(header) = self.take_disk_sync_header(id)? else {
            return Ok(true);
        };
        self.queue_disk_sync_header(connection_token, header);
        let result = self.flush_connection_output(connection_token.0);
        if let Some(target) = self
            .full_sync
            .disk_sync_batch
            .as_mut()
            .and_then(|batch| batch.serving_target_mut(id))
        {
            target.record_flush_result(&result);
        }
        if let WriteOutcome::Failed(error) = result {
            return Err(DiskSyncServeError::Transport(error));
        }
        let drained = self.connection_registry[connection_token]
            .output
            .queue
            .is_empty();
        let interest = if drained {
            Interest::Readable
        } else {
            Interest::ReadWritable
        };
        self.transport.set_interest(token, interest).ok();
        Ok(drained)
    }

    fn take_disk_sync_header(
        &mut self,
        id: ReplicaId,
    ) -> Result<Option<Vec<u8>>, DiskSyncServeError> {
        let batch = self
            .full_sync
            .disk_sync_batch
            .as_mut()
            .ok_or_else(|| DiskSyncServeError::Snapshot(io::ErrorKind::NotFound.into()))?;
        let target = batch
            .serving_target_mut(id)
            .ok_or_else(|| DiskSyncServeError::Snapshot(io::ErrorKind::NotFound.into()))?;
        if !target.header_pending {
            return Ok(None);
        }
        target.header_pending = false;
        Ok(Some(
            format!("${}\r\n", target.remaining_bytes).into_bytes(),
        ))
    }

    fn queue_disk_sync_header(&mut self, connection_token: Token, header: Vec<u8>) {
        self.ctx
            .stats_mut()
            .add_net_repl_output_bytes(header.len() as u64);
        if let Some(conn) = self.connection_registry.get_mut(connection_token) {
            conn.queue_reply(header);
        }
    }

    fn write_disk_sync_target_turn(
        &mut self,
        id: ReplicaId,
        token: Token,
    ) -> Result<DiskSyncServeProgress, DiskSyncServeError> {
        let batch = self
            .full_sync
            .disk_sync_batch
            .as_mut()
            .ok_or_else(|| DiskSyncServeError::Snapshot(io::ErrorKind::NotFound.into()))?;
        let file_len = batch.file_len;
        let target = batch
            .serving_target_mut(id)
            .ok_or_else(|| DiskSyncServeError::Snapshot(io::ErrorKind::NotFound.into()))?;
        let mut stream = self
            .transport
            .stream(token)
            .ok_or_else(|| DiskSyncServeError::Transport(io::ErrorKind::NotFound.into()))?;
        Self::serve_disk_sync_target_slice(
            target,
            file_len,
            &mut stream,
            DISK_SYNC_BYTES_PER_TARGET_PER_TURN,
        )
    }

    fn record_direct_disk_sync_write(&mut self, connection_token: Token, written: usize) {
        if written == 0 {
            return;
        }
        self.ctx
            .stats_mut()
            .add_net_repl_output_bytes(written as u64);
        if let Some(connection) = self.connection_registry.get_mut(connection_token) {
            connection.record_direct_output_bytes(written);
        }
    }

    fn serve_disk_sync_target_slice(
        target: &mut DiskSyncServingTarget,
        file_len: u64,
        writer: &mut impl Write,
        byte_budget: usize,
    ) -> Result<DiskSyncServeProgress, DiskSyncServeError> {
        let mut written = 0usize;
        target.write_blocked = false;
        while written < byte_budget && target.remaining_bytes > 0 {
            target
                .refill_pending(file_len)
                .map_err(DiskSyncServeError::Snapshot)?;
            let available = target.pending.len().saturating_sub(target.pending_offset);
            let allowed = available.min(byte_budget.saturating_sub(written));
            match target
                .write_pending(writer, allowed)
                .map_err(DiskSyncServeError::Transport)?
            {
                Some(count) => written = written.saturating_add(count),
                None => {
                    return Ok(DiskSyncServeProgress {
                        written,
                        blocked: true,
                        complete: false,
                    });
                }
            }
        }
        Ok(DiskSyncServeProgress {
            written,
            blocked: false,
            complete: target.remaining_bytes == 0 && target.pending.is_empty(),
        })
    }

    fn finish_disk_sync_targets(&mut self, completed: Vec<ReplicaId>) {
        for id in completed {
            if self.full_sync.disk_sync_batch.is_none() {
                break;
            }
            let completion = self.full_sync.disk_sync_batch.as_ref().and_then(|batch| {
                let target = batch.targets.get(&id)?.as_ref()?;
                let elapsed_ms = batch.serving_started_at?.elapsed().as_millis();
                Some((target.bytes_sent, target.would_block_count, elapsed_ms))
            });
            if let Some((bytes_sent, would_block_count, elapsed_ms)) = completion {
                crate::server_log!(
                    notice,
                    "disk-sync phase=target-complete replica={} bytes={} would_block={} elapsed_ms={}",
                    id.0,
                    bytes_sent,
                    would_block_count,
                    elapsed_ms
                );
            }
            if let Some(batch) = self.full_sync.disk_sync_batch.as_mut() {
                batch.remove_target(id);
            }
            self.finish_disk_sync_target(id);
        }
    }

    fn finish_disk_sync_target(&mut self, id: ReplicaId) {
        let Some(connection_token) = self.disk_sync_batch_target_token(id) else {
            return;
        };
        let token = connection_token;
        let command_stream_started = {
            let Some(link) = self.replication_runtime.primary_link_mut(id) else {
                return;
            };
            link.finish_disk_based_full_resync();
            link.take_command_stream_start_pending()
        };
        if command_stream_started {
            // C's replicaStartCommandStream() seeds an empty history at the
            // RDB-to-command-stream boundary; propagation also queues the PING
            // to the promoted replica before its backlog tail is pumped.
            self.start_replica_command_stream();
        }
        let pumped = {
            let link = self
                .replication_runtime
                .primary_link_mut(id)
                .expect("primary link exists while completing disk sync");
            link.pump(&self.ctx.replication().backlog)
        };
        let tail = match pumped {
            Ok(tail) => tail,
            Err(_) => {
                self.remove_connection(connection_token.0);
                self.refresh_replication_view();
                return;
            }
        };
        if let Some(conn) = self.connection_registry.get_mut(connection_token) {
            if !tail.is_empty() {
                if let Some(link) = self.replication_runtime.primary_link_mut(id) {
                    link.mark_replication_queued(tail.len());
                }
                self.ctx
                    .stats_mut()
                    .add_net_repl_output_bytes(tail.len() as u64);
                conn.queue_replication_reply(tail);
            }
            self.transport
                .set_interest(token, Interest::ReadWritable)
                .ok();
        }
    }

    fn cleanup_finished_disk_sync_batch(&mut self, phase_promoted: bool) {
        let should_remove = self
            .full_sync
            .disk_sync_batch
            .as_ref()
            .is_some_and(DiskSyncBatch::is_empty);
        if should_remove {
            let persistence = self.full_sync.disk_sync_batch.as_ref().and_then(|batch| {
                batch
                    .persist_when_targetless
                    .then(|| (batch.snapshot_path.clone(), batch.dirty_at_fork))
            });
            if let Some((snapshot_path, dirty_at_fork)) = persistence {
                self.finish_targetless_disk_sync_save(snapshot_path, dirty_at_fork);
                return;
            }
            if let Some(batch) = self.full_sync.disk_sync_batch.take() {
                let _ = std::fs::remove_file(&batch.snapshot_path);
                crate::server_log!(notice, "Disk-based rdb batch complete, snapshot removed.");
            }
            self.refresh_replication_view();
        } else if phase_promoted {
            self.refresh_replication_view();
        }
    }

    fn finish_targetless_disk_sync_save(&mut self, snapshot_path: PathBuf, dirty_at_fork: i64) {
        let target = std::path::Path::new(&self.ctx.config().persistence.dir)
            .join(&self.ctx.config().persistence.dbfilename);
        let result = std::fs::rename(&snapshot_path, &target).and_then(|_| {
            File::open(target.parent().unwrap_or_else(|| std::path::Path::new(".")))?.sync_all()
        });
        let success = result.is_ok();
        let now_sec = crate::foundation::io::store::kv_engine::now_ms() / 1_000;
        self.ctx
            .persistence_mut()
            .record_promoted_rdb_save(dirty_at_fork, success, now_sec as i64);
        if let Err(error) = result {
            crate::server_log!(
                warning,
                "Failed to promote replication RDB as BGSAVE: {}",
                error
            );
        } else {
            crate::server_log!(notice, "Background saving terminated with success");
        }
        self.full_sync.disk_sync_batch = None;
        self.refresh_replication_view();
    }

    pub(crate) fn cancel_disk_sync_batch(&mut self) {
        let child_pid = self
            .full_sync
            .disk_sync_batch
            .as_ref()
            .and_then(DiskSyncBatch::child_pid);
        self.full_sync.disk_sync_batch = None;
        if child_pid.is_some() && self.ctx.persistence().disk_sync_child_pid() == child_pid {
            self.ctx.persistence_mut().complete_disk_sync_child();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DiskSyncBatch, DiskSyncServingTarget, ServerWorkerState, WriteOutcome,
        DISK_SYNC_BUFFER_BYTES, DISK_SYNC_BYTES_PER_TARGET_PER_TURN,
    };
    use std::collections::VecDeque;
    use std::io::{self, Write};

    enum WriteStep {
        Bytes(usize),
        WouldBlock,
    }

    struct ScriptedWriter {
        steps: VecDeque<WriteStep>,
        bytes: Vec<u8>,
    }

    impl ScriptedWriter {
        fn new(steps: impl IntoIterator<Item = WriteStep>) -> Self {
            Self {
                steps: steps.into_iter().collect(),
                bytes: Vec::new(),
            }
        }
    }

    impl Write for ScriptedWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            match self.steps.pop_front() {
                Some(WriteStep::Bytes(limit)) => {
                    let written = limit.min(buf.len());
                    self.bytes.extend_from_slice(&buf[..written]);
                    Ok(written)
                }
                Some(WriteStep::WouldBlock) => Err(io::ErrorKind::WouldBlock.into()),
                None => {
                    self.bytes.extend_from_slice(buf);
                    Ok(buf.len())
                }
            }
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn serving_target(snapshot_path: &std::path::Path, file_len: u64) -> DiskSyncServingTarget {
        DiskSyncServingTarget::new(std::fs::File::open(snapshot_path).unwrap(), file_len)
    }

    fn zero_boundary() -> crate::engine::distributed::replication::SnapshotBoundary {
        crate::engine::distributed::replication::backlog::ReplicationBacklog::new(
            "test".to_string(),
            1,
        )
        .snapshot_boundary()
    }

    #[test]
    fn disk_sync_target_serving_never_exceeds_per_target_turn_budget() {
        let snapshot_path = std::env::temp_dir().join(format!(
            "valkey-disk-sync-direct-budget-{}-{:?}.rdb",
            std::process::id(),
            std::thread::current().id()
        ));
        let snapshot = vec![0x5a; DISK_SYNC_BYTES_PER_TARGET_PER_TURN + 17];
        std::fs::write(&snapshot_path, &snapshot).unwrap();
        let mut target = serving_target(&snapshot_path, snapshot.len() as u64);
        let mut writer = Vec::new();

        let progress = ServerWorkerState::serve_disk_sync_target_slice(
            &mut target,
            snapshot.len() as u64,
            &mut writer,
            DISK_SYNC_BYTES_PER_TARGET_PER_TURN,
        )
        .unwrap();

        assert_eq!(progress.written, DISK_SYNC_BYTES_PER_TARGET_PER_TURN);
        assert!(!progress.blocked);
        assert!(!progress.complete);
        assert_eq!(writer, snapshot[..DISK_SYNC_BYTES_PER_TARGET_PER_TURN]);
        assert_eq!(target.remaining_bytes, 17);
        std::fs::remove_file(snapshot_path).unwrap();
    }

    #[test]
    fn disk_sync_target_serving_retains_exact_suffix_after_partial_would_block() {
        let snapshot_path = std::env::temp_dir().join(format!(
            "valkey-disk-sync-direct-partial-{}-{:?}.rdb",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::write(&snapshot_path, b"abcdef").unwrap();
        let mut target = serving_target(&snapshot_path, 6);
        let mut blocked = ScriptedWriter::new([WriteStep::Bytes(3), WriteStep::WouldBlock]);

        let first = ServerWorkerState::serve_disk_sync_target_slice(
            &mut target,
            6,
            &mut blocked,
            DISK_SYNC_BYTES_PER_TARGET_PER_TURN,
        )
        .unwrap();

        assert_eq!(first.written, 3);
        assert!(first.blocked);
        assert!(!first.complete);
        assert_eq!(blocked.bytes, b"abc");
        assert_eq!(&target.pending[target.pending_offset..], b"def");
        assert_eq!(target.remaining_bytes, 3);
        assert_eq!(target.bytes_sent, 3);
        assert_eq!(target.would_block_count, 1);
        assert!(target.write_blocked);

        let mut resumed = Vec::new();
        let second = ServerWorkerState::serve_disk_sync_target_slice(
            &mut target,
            6,
            &mut resumed,
            DISK_SYNC_BYTES_PER_TARGET_PER_TURN,
        )
        .unwrap();
        assert_eq!(second.written, 3);
        assert!(!second.blocked);
        assert!(second.complete);
        assert_eq!(resumed, b"def");
        assert_eq!(target.remaining_bytes, 0);
        assert_eq!(target.bytes_sent, 6);
        assert!(!target.write_blocked);
        assert!(target.pending.is_empty());
        std::fs::remove_file(snapshot_path).unwrap();
    }

    #[test]
    fn disk_sync_target_serving_keeps_replica_cursors_independent() {
        let snapshot_path = std::env::temp_dir().join(format!(
            "valkey-disk-sync-direct-cursors-{}-{:?}.rdb",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::write(&snapshot_path, b"abcdef").unwrap();
        let mut first = serving_target(&snapshot_path, 6);
        let mut second = serving_target(&snapshot_path, 6);
        let mut first_bytes = Vec::new();
        let mut second_bytes = Vec::new();

        ServerWorkerState::serve_disk_sync_target_slice(&mut first, 6, &mut first_bytes, 2)
            .unwrap();
        ServerWorkerState::serve_disk_sync_target_slice(&mut second, 6, &mut second_bytes, 4)
            .unwrap();

        assert_eq!(first_bytes, b"ab");
        assert_eq!(second_bytes, b"abcd");
        assert_eq!(first.remaining_bytes, 4);
        assert_eq!(second.remaining_bytes, 2);
        std::fs::remove_file(snapshot_path).unwrap();
    }

    #[test]
    fn disk_sync_target_serving_reuses_pending_buffer_capacity() {
        let snapshot_path = std::env::temp_dir().join(format!(
            "valkey-disk-sync-direct-reuse-{}-{:?}.rdb",
            std::process::id(),
            std::thread::current().id()
        ));
        let snapshot = vec![0x5a; 2 * super::DISK_SYNC_BUFFER_BYTES];
        std::fs::write(&snapshot_path, &snapshot).unwrap();
        let mut target = serving_target(&snapshot_path, snapshot.len() as u64);
        let mut writer = Vec::new();

        ServerWorkerState::serve_disk_sync_target_slice(
            &mut target,
            snapshot.len() as u64,
            &mut writer,
            super::DISK_SYNC_BUFFER_BYTES,
        )
        .unwrap();
        let capacity = target.pending.capacity();
        ServerWorkerState::serve_disk_sync_target_slice(
            &mut target,
            snapshot.len() as u64,
            &mut writer,
            super::DISK_SYNC_BUFFER_BYTES,
        )
        .unwrap();

        assert_eq!(target.pending.capacity(), capacity);
        assert_eq!(writer, snapshot);
        std::fs::remove_file(snapshot_path).unwrap();
    }

    #[test]
    fn single_target_batch_promotes_and_serves_with_the_same_cursor_path() {
        let snapshot_path = std::env::temp_dir().join(format!(
            "valkey-disk-sync-single-target-{}-{:?}.rdb",
            std::process::id(),
            std::thread::current().id()
        ));
        let snapshot = vec![0x37; DISK_SYNC_BYTES_PER_TARGET_PER_TURN + 11];
        std::fs::write(&snapshot_path, &snapshot).unwrap();
        let replica_id = crate::framework::distributed::replication::ReplicaId(11);
        let mut batch = DiskSyncBatch {
            child_pid: None,
            snapshot_path: snapshot_path.clone(),
            boundary: zero_boundary(),
            phase: super::DiskSyncPhase::Producing,
            file_len: 0,
            targets: std::collections::HashMap::new(),
            dirty_at_fork: 0,
            persist_when_targetless: false,
            production_started_at: std::time::Instant::now(),
            serving_started_at: None,
        };
        batch.add_waiting_target(replica_id);

        batch.promote_to_serving(snapshot.len() as u64).unwrap();

        assert_eq!(batch.phase, super::DiskSyncPhase::Serving);
        assert_eq!(batch.target_count(), 1);
        let target = batch.serving_target_mut(replica_id).unwrap();
        let mut writer = Vec::new();
        let first = ServerWorkerState::serve_disk_sync_target_slice(
            target,
            snapshot.len() as u64,
            &mut writer,
            DISK_SYNC_BYTES_PER_TARGET_PER_TURN,
        )
        .unwrap();
        assert_eq!(first.written, DISK_SYNC_BYTES_PER_TARGET_PER_TURN);
        assert!(!first.complete);
        assert!(target.remaining_bytes > 0);

        let second = ServerWorkerState::serve_disk_sync_target_slice(
            target,
            snapshot.len() as u64,
            &mut writer,
            DISK_SYNC_BYTES_PER_TARGET_PER_TURN,
        )
        .unwrap();
        assert_eq!(second.written, 11);
        assert!(second.complete);
        assert_eq!(writer, snapshot);
        drop(batch);
        assert!(!snapshot_path.exists());
    }

    #[test]
    fn disk_sync_phase_timing_and_target_counters_follow_lifecycle() {
        let snapshot_path = std::env::temp_dir().join(format!(
            "valkey-disk-sync-metrics-{}-{:?}.rdb",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::write(&snapshot_path, b"snapshot").unwrap();
        let replica_id = crate::framework::distributed::replication::ReplicaId(7);
        let mut batch = DiskSyncBatch {
            child_pid: None,
            snapshot_path: snapshot_path.clone(),
            boundary: zero_boundary(),
            phase: super::DiskSyncPhase::Producing,
            file_len: 0,
            targets: std::collections::HashMap::new(),
            dirty_at_fork: 0,
            persist_when_targetless: false,
            production_started_at: std::time::Instant::now(),
            serving_started_at: None,
        };
        batch.add_waiting_target(replica_id);

        batch.promote_to_serving(8).unwrap();

        assert!(batch.serving_started_at.is_some());
        let target = batch.serving_target_mut(replica_id).unwrap();
        assert_eq!(target.bytes_sent, 0);
        assert_eq!(target.would_block_count, 0);
        let mut writer = Vec::new();
        ServerWorkerState::serve_disk_sync_target_slice(target, 8, &mut writer, 8).unwrap();
        assert_eq!(writer, b"snapshot");
        assert_eq!(target.bytes_sent, 8);
        target.record_flush_result(&WriteOutcome::Complete);
        target.record_flush_result(&WriteOutcome::BudgetExhausted);
        target.record_flush_result(&WriteOutcome::Backpressured);
        target.record_flush_result(&WriteOutcome::Failed(io::ErrorKind::BrokenPipe.into()));
        assert_eq!(target.would_block_count, 1);

        drop(batch);
        assert!(!snapshot_path.exists());
    }

    #[test]
    fn disk_sync_read_rejects_unexpected_eof() {
        let snapshot_path = std::env::temp_dir().join(format!(
            "valkey-disk-sync-truncated-{}-{:?}.rdb",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::write(&snapshot_path, b"x").unwrap();
        let mut target =
            DiskSyncServingTarget::new(std::fs::File::open(&snapshot_path).unwrap(), 2);
        target.header_pending = false;
        let mut writer = Vec::new();

        assert_eq!(
            ServerWorkerState::serve_disk_sync_target_slice(&mut target, 2, &mut writer, 1)
                .unwrap()
                .written,
            1
        );
        assert_eq!(writer, b"x");
        let error = ServerWorkerState::serve_disk_sync_target_slice(&mut target, 2, &mut writer, 1)
            .unwrap_err();

        assert!(matches!(
            error,
            super::DiskSyncServeError::Snapshot(error)
                if error.kind() == std::io::ErrorKind::UnexpectedEof
        ));
        std::fs::remove_file(snapshot_path).unwrap();
    }

    #[test]
    fn serving_batch_aborts_on_truncated_snapshot() {
        use crate::framework::distributed::replication::PrimaryLinkState;
        use crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer;
        use std::net::TcpListener;

        let snapshot_path = std::env::temp_dir().join(format!(
            "valkey-disk-sync-truncated-batch-{}-{:?}.rdb",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::write(&snapshot_path, b"x").unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let mut runtime = ServerWorkerState::new(1).unwrap();
        let peer = runtime.transport.connect_tcp(address).unwrap();
        let (_sink, _) = listener.accept().unwrap();
        let token = peer.token;
        runtime.add_connection(RuntimeConnectedPeer {
            token,
            peer_addr: address,
        });
        let replica_id = runtime.replication_runtime.register_primary(
            crate::framework::distributed::replication::ConnectionId(token.0),
            crate::framework::distributed::replication::ReplicaConnectionInfo {
                ip: address.ip().to_string(),
                port: address.port(),
                capability: Vec::new(),
                rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
            },
        );
        runtime
            .replication_runtime
            .primary_link_mut(replica_id)
            .unwrap()
            .set_state(PrimaryLinkState::SendingRdb);
        let mut batch = DiskSyncBatch {
            child_pid: None,
            snapshot_path: snapshot_path.clone(),
            boundary: zero_boundary(),
            phase: super::DiskSyncPhase::Producing,
            file_len: 0,
            targets: std::collections::HashMap::new(),
            dirty_at_fork: 0,
            persist_when_targetless: false,
            production_started_at: std::time::Instant::now(),
            serving_started_at: None,
        };
        batch.add_waiting_target(replica_id);
        batch.promote_to_serving(2).unwrap();
        runtime.full_sync.disk_sync_batch = Some(batch);

        assert!(!runtime.serve_disk_sync_target(replica_id));

        assert!(runtime.full_sync.disk_sync_batch.is_none());
        assert!(!runtime
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token.0)));
        assert!(!snapshot_path.exists());
    }

    #[test]
    fn serving_batch_respects_the_per_target_byte_budget() {
        use crate::framework::distributed::replication::PrimaryLinkState;
        use crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer;
        use std::net::TcpListener;

        let snapshot_path = std::env::temp_dir().join(format!(
            "valkey-disk-sync-one-chunk-{}-{:?}.rdb",
            std::process::id(),
            std::thread::current().id()
        ));
        let snapshot = vec![0x5a; DISK_SYNC_BYTES_PER_TARGET_PER_TURN + DISK_SYNC_BUFFER_BYTES];
        std::fs::write(&snapshot_path, &snapshot).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let mut runtime = ServerWorkerState::new(1).unwrap();
        let peer = runtime.transport.connect_tcp(address).unwrap();
        let (_sink, _) = listener.accept().unwrap();
        let token = peer.token;
        runtime.add_connection(RuntimeConnectedPeer {
            token,
            peer_addr: address,
        });
        let replica_id = runtime.replication_runtime.register_primary(
            crate::framework::distributed::replication::ConnectionId(token.0),
            crate::framework::distributed::replication::ReplicaConnectionInfo {
                ip: address.ip().to_string(),
                port: address.port(),
                capability: Vec::new(),
                rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
            },
        );
        runtime
            .replication_runtime
            .primary_link_mut(replica_id)
            .unwrap()
            .set_state(PrimaryLinkState::SendingRdb);
        let mut batch = DiskSyncBatch {
            child_pid: None,
            snapshot_path: snapshot_path.clone(),
            boundary: zero_boundary(),
            phase: super::DiskSyncPhase::Producing,
            file_len: 0,
            targets: std::collections::HashMap::new(),
            dirty_at_fork: 0,
            persist_when_targetless: false,
            production_started_at: std::time::Instant::now(),
            serving_started_at: None,
        };
        batch.add_waiting_target(replica_id);
        batch.promote_to_serving(snapshot.len() as u64).unwrap();
        runtime.full_sync.disk_sync_batch = Some(batch);

        assert_eq!(
            runtime.next_poll_timeout(crate::foundation::io::sys::time::MonotonicInstant::now()),
            None,
            "a serving batch is advanced by writable readiness, not by fast-poll deadlines"
        );

        runtime.handle_writable(token);
        assert_eq!(
            runtime.full_sync.disk_sync_batch.as_ref().unwrap().targets[&replica_id]
                .as_ref()
                .unwrap()
                .remaining_bytes,
            (snapshot.len() - DISK_SYNC_BYTES_PER_TARGET_PER_TURN) as u64
        );
        assert_eq!(
            runtime.full_sync.disk_sync_batch.as_ref().unwrap().targets[&replica_id]
                .as_ref()
                .unwrap()
                .bytes_sent,
            DISK_SYNC_BYTES_PER_TARGET_PER_TURN as u64
        );
        assert!(
            runtime.connection_registry[crate::framework::capabilities::io::Token(token.0)]
                .output
                .queue
                .is_empty()
        );

        runtime.cancel_disk_sync_batch();
        assert!(!snapshot_path.exists());
    }

    #[test]
    fn disk_sync_child_completion_promotes_batch_without_before_poll_pump() {
        use crate::runtime::server::worker::reactor::sources::{ChildExitOutcome, ChildOwner};

        let snapshot_path = std::env::temp_dir().join(format!(
            "valkey-disk-sync-child-completion-{}-{:?}.rdb",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::write(&snapshot_path, b"snapshot").unwrap();
        let replica_id = crate::framework::distributed::replication::ReplicaId(31);
        let mut runtime = ServerWorkerState::new(1).unwrap();
        let pid = i32::MAX - 31;
        runtime
            .ctx
            .persistence_mut()
            .begin_disk_sync_child(pid)
            .unwrap();
        let child = runtime.install_test_child(ChildOwner::DiskSyncReplication, pid);
        let mut batch =
            DiskSyncBatch::new_producing(pid, snapshot_path.clone(), zero_boundary(), 0);
        batch.add_waiting_target(replica_id);
        runtime.full_sync.disk_sync_batch = Some(batch);

        let outcome = runtime.dispatch_child_completion_for_test(
            crate::runtime::server::worker::reactor::sources::ChildCompletionEvent {
                child,
                outcome: ChildExitOutcome::Exited(0),
            },
            crate::runtime::server::worker::reactor::DispatchContext::Normal,
        );

        assert_eq!(
            outcome,
            crate::runtime::server::worker::reactor::DispatchOutcome::Handled
        );
        let batch = runtime.full_sync.disk_sync_batch.as_ref().unwrap();
        assert_eq!(batch.phase, super::DiskSyncPhase::Serving);
        assert_eq!(batch.file_len, 8);
        assert!(batch.child_pid.is_none());
        assert!(batch.targets[&replica_id].is_some());
        assert!(runtime.ctx.persistence().disk_sync_child_pid().is_none());
        runtime.cancel_disk_sync_batch();
        assert!(!snapshot_path.exists());
    }

    #[test]
    fn writable_replica_event_serves_disk_sync_batch_target() {
        use crate::framework::distributed::replication::PrimaryLinkState;
        use crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer;
        use std::io::Read;
        use std::net::TcpListener;
        use std::time::Duration;

        let snapshot_path = std::env::temp_dir().join(format!(
            "valkey-disk-sync-writable-{}-{:?}.rdb",
            std::process::id(),
            std::thread::current().id()
        ));
        let snapshot = b"snapshot".to_vec();
        std::fs::write(&snapshot_path, &snapshot).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let mut runtime = ServerWorkerState::new(1).unwrap();
        let peer = runtime.transport.connect_tcp(address).unwrap();
        let (mut replica, _) = listener.accept().unwrap();
        replica
            .set_read_timeout(Some(Duration::from_millis(200)))
            .unwrap();
        let token = peer.token;
        runtime.add_connection(RuntimeConnectedPeer {
            token,
            peer_addr: address,
        });
        let replica_id = runtime.replication_runtime.register_primary(
            crate::framework::distributed::replication::ConnectionId(token.0),
            crate::framework::distributed::replication::ReplicaConnectionInfo {
                ip: address.ip().to_string(),
                port: address.port(),
                capability: Vec::new(),
                rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
            },
        );
        runtime
            .replication_runtime
            .primary_link_mut(replica_id)
            .unwrap()
            .set_state(PrimaryLinkState::SendingRdb);
        let mut batch = DiskSyncBatch {
            child_pid: None,
            snapshot_path: snapshot_path.clone(),
            boundary: zero_boundary(),
            phase: super::DiskSyncPhase::Producing,
            file_len: 0,
            targets: std::collections::HashMap::new(),
            dirty_at_fork: 0,
            persist_when_targetless: false,
            production_started_at: std::time::Instant::now(),
            serving_started_at: None,
        };
        batch.add_waiting_target(replica_id);
        batch.promote_to_serving(snapshot.len() as u64).unwrap();
        runtime.full_sync.disk_sync_batch = Some(batch);

        runtime.handle_writable(token);

        let expected = b"$8\r\nsnapshot";
        let mut received = vec![0; expected.len()];
        replica
            .read_exact(&mut received)
            .expect("writable readiness must serve the disk-sync target");
        assert_eq!(received, expected);
        assert!(runtime.full_sync.disk_sync_batch.is_none());
        assert!(!snapshot_path.exists());
    }

    fn add_replica(
        runtime: &mut ServerWorkerState,
        token_value: usize,
    ) -> crate::framework::distributed::replication::ReplicaId {
        use crate::framework::capabilities::connection::Connection;
        use crate::framework::distributed::replication::{ConnectionId, ReplicaConnectionInfo};
        let _key = runtime.connection_registry.insert(Connection::new(
            crate::framework::capabilities::io::Token(token_value),
            std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
            false,
        ));
        runtime.replication_runtime.register_primary(
            ConnectionId(token_value),
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6381,
                capability: Vec::new(),
                rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
            },
        )
    }

    #[test]
    fn late_join_installs_the_stored_snapshot_stream_cursor() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        let replica_id = add_replica(&mut runtime, 70);
        let mut backlog = crate::engine::distributed::replication::backlog::ReplicationBacklog::new(
            "local".to_string(),
            64,
        );
        backlog
            .restore_primary_from_rdb(
                "restored",
                crate::engine::distributed::replication::ReplicationOffset::try_from(41).unwrap(),
            )
            .unwrap();
        let boundary = backlog.snapshot_boundary();
        let snapshot_path = std::env::temp_dir().join(format!(
            "valkey-disk-sync-boundary-{}-{:?}.rdb",
            std::process::id(),
            std::thread::current().id()
        ));
        runtime.full_sync.disk_sync_batch = Some(DiskSyncBatch {
            child_pid: None,
            snapshot_path,
            boundary,
            phase: super::DiskSyncPhase::Producing,
            file_len: 0,
            targets: std::collections::HashMap::new(),
            dirty_at_fork: 0,
            persist_when_targetless: false,
            production_started_at: std::time::Instant::now(),
            serving_started_at: None,
        });

        assert_eq!(runtime.join_disk_sync_batch(replica_id), Some(boundary));
        let link = runtime
            .replication_runtime
            .primary_link(replica_id)
            .unwrap();
        assert_eq!(link.next_send_cursor().get(), 42);
        assert_eq!(link.sent_cursor().get(), 42);
    }

    #[test]
    fn disk_sync_completion_pump_error_removes_stale_replica() {
        use crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer;
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let mut runtime = ServerWorkerState::new(1).unwrap();
        let peer = runtime.transport.connect_tcp(address).unwrap();
        let (_sink, _) = listener.accept().unwrap();
        let token = peer.token;
        runtime.add_connection(RuntimeConnectedPeer {
            token,
            peer_addr: address,
        });
        let replica_id = runtime.replication_runtime.register_primary(
            crate::framework::distributed::replication::ConnectionId(token.0),
            crate::framework::distributed::replication::ReplicaConnectionInfo {
                ip: address.ip().to_string(),
                port: address.port(),
                capability: Vec::new(),
                rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
            },
        );
        {
            let backlog = &runtime.ctx.replication().backlog;
            runtime
                .replication_runtime
                .primary_link_mut(replica_id)
                .unwrap()
                .plan_full_resync(backlog);
        }
        runtime.ctx.replication_mut().backlog.set_capacity(0);
        runtime
            .ctx
            .replication_mut()
            .backlog
            .append(b"evicted-tail")
            .unwrap();

        runtime.finish_disk_sync_target(replica_id);

        assert!(!runtime.connection_registry.contains(token));
        assert!(runtime
            .replication_runtime
            .primary_link(replica_id)
            .is_none());
    }

    fn sleeping_child() -> i32 {
        crate::framework::capabilities::process::fork_owned_child(|| loop {
            let _ = crate::framework::capabilities::process::pause_current_process();
        })
        .expect("fork sleeping child")
        .into_pid()
    }

    fn successful_child() -> i32 {
        crate::framework::capabilities::process::fork_owned_child(|| 0)
            .expect("fork successful child")
            .into_pid()
    }

    fn failed_child() -> i32 {
        crate::framework::capabilities::process::fork_owned_child(|| 1)
            .expect("fork failed child")
            .into_pid()
    }

    fn dispatch_disk_sync_child_completion(runtime: &mut ServerWorkerState, child_pid: i32) {
        let _child = runtime.install_test_child(
            crate::runtime::server::worker::reactor::sources::ChildOwner::DiskSyncReplication,
            child_pid,
        );
        for _ in 0..100 {
            if let Some(completion) = runtime.take_child_completion_for_pid(child_pid) {
                assert_eq!(
                    runtime.dispatch_child_completion_for_test(
                        completion,
                        crate::runtime::server::worker::reactor::DispatchContext::Normal,
                    ),
                    crate::runtime::server::worker::reactor::DispatchOutcome::Handled
                );
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        panic!("disk-sync child did not complete");
    }

    #[test]
    fn dropping_producing_batch_reaps_child_and_removes_snapshot() {
        let snapshot_path = std::env::temp_dir().join(format!(
            "valkey-disk-sync-drop-{}-{:?}.rdb",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_file(&snapshot_path);
        std::fs::write(&snapshot_path, b"snapshot").unwrap();

        let child_pid = sleeping_child();

        drop(DiskSyncBatch::new_producing(
            child_pid,
            snapshot_path.clone(),
            zero_boundary(),
            0,
        ));

        let wait_result = crate::foundation::io::sys::process::wait_nonblocking(child_pid);
        let snapshot_exists = snapshot_path.exists();

        if matches!(
            wait_result,
            Ok(crate::foundation::io::sys::process::WaitOutcome::Running)
        ) {
            // Keep a failing assertion from leaving the test child alive.
            let _ = crate::foundation::io::sys::process::send_signal(
                child_pid,
                crate::foundation::io::sys::process::ProcessSignal::Kill,
            );
            let _ = crate::foundation::io::sys::process::wait_blocking(child_pid);
        }
        let _ = std::fs::remove_file(&snapshot_path);

        assert!(
            wait_result.is_err(),
            "dropping the batch must reap its child"
        );
        assert!(
            !snapshot_exists,
            "dropping the batch must remove its snapshot"
        );
    }

    #[test]
    fn shutdown_exit_cancels_batch_and_clears_persistence_child() {
        let snapshot_path = std::env::temp_dir().join(format!(
            "valkey-disk-sync-cancel-{}-{:?}.rdb",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_file(&snapshot_path);
        std::fs::write(&snapshot_path, b"snapshot").unwrap();
        let child_pid = sleeping_child();
        let mut runtime = ServerWorkerState::new(1).unwrap();
        runtime
            .ctx
            .persistence_mut()
            .begin_disk_sync_child(child_pid)
            .unwrap();
        runtime.full_sync.disk_sync_batch = Some(DiskSyncBatch::new_producing(
            child_pid,
            snapshot_path.clone(),
            zero_boundary(),
            0,
        ));

        runtime.ctx.set_shutdown_requested(true);
        assert!(runtime.exit_if_shutdown_requested());

        assert!(runtime.full_sync.disk_sync_batch.is_none());
        assert!(runtime.ctx.persistence().disk_sync_child_pid().is_none());
        assert!(!snapshot_path.exists());
        assert!(crate::foundation::io::sys::process::wait_nonblocking(child_pid).is_err());
    }

    #[test]
    fn dropping_last_producing_batch_target_clears_persistence_child() {
        use crate::framework::capabilities::connection::Connection;
        use crate::framework::distributed::replication::{ConnectionId, ReplicaConnectionInfo};
        use std::net::SocketAddr;

        let snapshot_path = std::env::temp_dir().join(format!(
            "valkey-disk-sync-last-target-{}-{:?}.rdb",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_file(&snapshot_path);
        std::fs::write(&snapshot_path, b"snapshot").unwrap();
        let child_pid = sleeping_child();
        let mut runtime = ServerWorkerState::new(1).unwrap();
        runtime.ctx.config_mut().persistence.saveparams.clear();
        runtime
            .ctx
            .persistence_mut()
            .begin_disk_sync_child(child_pid)
            .unwrap();
        let token_value = 41;
        let peer_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let _key = runtime.connection_registry.insert(Connection::new(
            crate::framework::capabilities::io::Token(token_value),
            peer_addr,
            false,
        ));
        let replica_id = runtime.replication_runtime.register_primary(
            ConnectionId(token_value),
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6381,
                capability: Vec::new(),
                rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
            },
        );
        let mut batch =
            DiskSyncBatch::new_producing(child_pid, snapshot_path.clone(), zero_boundary(), 0);
        batch.add_waiting_target(replica_id);
        runtime.full_sync.disk_sync_batch = Some(batch);

        runtime.drop_disk_sync_batch_target(replica_id);

        assert!(runtime.full_sync.disk_sync_batch.is_none());
        assert!(runtime.ctx.persistence().disk_sync_child_pid().is_none());
        assert!(!runtime.ctx.persistence().has_active_child());
        assert!(!snapshot_path.exists());
        assert!(crate::foundation::io::sys::process::wait_nonblocking(child_pid).is_err());
    }

    #[test]
    fn dropping_last_producing_batch_target_preserves_child_when_saveparams_exist() {
        use crate::engine::extension::config::persistence::SaveParam;
        use crate::framework::capabilities::connection::Connection;
        use crate::framework::distributed::replication::{ConnectionId, ReplicaConnectionInfo};
        use std::net::SocketAddr;

        let snapshot_path = std::env::temp_dir().join(format!(
            "valkey-disk-sync-preserve-{}-{:?}.rdb",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_file(&snapshot_path);
        std::fs::write(&snapshot_path, b"snapshot").unwrap();
        let child_pid = sleeping_child();
        let mut runtime = ServerWorkerState::new(1).unwrap();
        runtime.ctx.config_mut().persistence.saveparams = vec![SaveParam {
            seconds: 900,
            changes: 1,
        }];
        runtime
            .ctx
            .persistence_mut()
            .begin_disk_sync_child(child_pid)
            .unwrap();
        let token_value = 42;
        let peer_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let _key = runtime.connection_registry.insert(Connection::new(
            crate::framework::capabilities::io::Token(token_value),
            peer_addr,
            false,
        ));
        let replica_id = runtime.replication_runtime.register_primary(
            ConnectionId(token_value),
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6381,
                capability: Vec::new(),
                rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
            },
        );
        let mut batch =
            DiskSyncBatch::new_producing(child_pid, snapshot_path.clone(), zero_boundary(), 0);
        batch.add_waiting_target(replica_id);
        runtime.full_sync.disk_sync_batch = Some(batch);

        runtime.drop_disk_sync_batch_target(replica_id);

        assert!(runtime.full_sync.disk_sync_batch.is_some());
        assert_eq!(
            runtime.ctx.persistence().disk_sync_child_pid(),
            Some(child_pid)
        );
        assert!(runtime.ctx.persistence().has_active_child());
        assert!(snapshot_path.exists());

        runtime.cancel_disk_sync_batch();

        assert!(runtime.full_sync.disk_sync_batch.is_none());
        assert!(runtime.ctx.persistence().disk_sync_child_pid().is_none());
        assert!(!runtime.ctx.persistence().has_active_child());
        assert!(!snapshot_path.exists());
        assert!(crate::foundation::io::sys::process::wait_nonblocking(child_pid).is_err());
    }

    #[test]
    fn targetless_saveparams_batch_promotes_snapshot_and_accounts_bgsave() {
        use crate::engine::extension::config::persistence::SaveParam;
        use crate::engine::persistence::OperationStatus;
        use crate::framework::capabilities::connection::Connection;
        use crate::framework::distributed::replication::{ConnectionId, ReplicaConnectionInfo};
        use std::net::SocketAddr;

        let dir = std::env::temp_dir().join(format!(
            "valkey-disk-sync-persist-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let snapshot_path = dir.join("temp-replication.rdb");
        std::fs::write(&snapshot_path, b"snapshot").unwrap();
        let child_pid = successful_child();
        let mut runtime = ServerWorkerState::new(1).unwrap();
        runtime.ctx.config_mut().persistence.dir = dir.to_string_lossy().into_owned();
        runtime.ctx.config_mut().persistence.dbfilename = "dump.rdb".to_string();
        runtime.ctx.config_mut().persistence.saveparams = vec![SaveParam {
            seconds: 900,
            changes: 1,
        }];
        runtime.ctx.persistence_mut().add_dirty(5);
        runtime
            .ctx
            .persistence_mut()
            .begin_disk_sync_child(child_pid)
            .unwrap();
        let token_value = 56;
        let _key = runtime.connection_registry.insert(Connection::new(
            crate::framework::capabilities::io::Token(token_value),
            SocketAddr::from(([127, 0, 0, 1], 0)),
            false,
        ));
        let replica_id = runtime.replication_runtime.register_primary(
            ConnectionId(token_value),
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6381,
                capability: Vec::new(),
                rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
            },
        );
        let mut batch = DiskSyncBatch::new_producing(child_pid, snapshot_path, zero_boundary(), 5);
        batch.add_waiting_target(replica_id);
        runtime.full_sync.disk_sync_batch = Some(batch);
        runtime.drop_disk_sync_batch_target(replica_id);
        runtime.ctx.persistence_mut().add_dirty(3);

        dispatch_disk_sync_child_completion(&mut runtime, child_pid);

        assert_eq!(std::fs::read(dir.join("dump.rdb")).unwrap(), b"snapshot");
        assert_eq!(runtime.ctx.persistence().dirty(), 3);
        assert!(runtime.ctx.persistence().rdb().last_save_time_sec > 0);
        assert_eq!(
            runtime.ctx.persistence().rdb().last_bgsave_status,
            OperationStatus::Ok
        );
        assert!(runtime.full_sync.disk_sync_batch.is_none());
        assert!(!runtime.ctx.persistence().has_active_child());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn targetless_saveparams_promotion_failure_preserves_dirty_and_records_error() {
        use crate::engine::extension::config::persistence::SaveParam;
        use crate::engine::persistence::OperationStatus;
        use crate::framework::capabilities::connection::Connection;
        use crate::framework::distributed::replication::{ConnectionId, ReplicaConnectionInfo};
        use std::net::SocketAddr;

        let dir = std::env::temp_dir().join(format!(
            "valkey-disk-sync-persist-failure-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("dump.rdb")).unwrap();
        let snapshot_path = dir.join("temp-replication.rdb");
        std::fs::write(&snapshot_path, b"snapshot").unwrap();
        let child_pid = successful_child();
        let mut runtime = ServerWorkerState::new(1).unwrap();
        runtime.ctx.config_mut().persistence.dir = dir.to_string_lossy().into_owned();
        runtime.ctx.config_mut().persistence.dbfilename = "dump.rdb".to_string();
        runtime.ctx.config_mut().persistence.saveparams = vec![SaveParam {
            seconds: 900,
            changes: 1,
        }];
        runtime.ctx.persistence_mut().add_dirty(5);
        runtime
            .ctx
            .persistence_mut()
            .begin_disk_sync_child(child_pid)
            .unwrap();
        let token_value = 57;
        let _key = runtime.connection_registry.insert(Connection::new(
            crate::framework::capabilities::io::Token(token_value),
            SocketAddr::from(([127, 0, 0, 1], 0)),
            false,
        ));
        let replica_id = runtime.replication_runtime.register_primary(
            ConnectionId(token_value),
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6381,
                capability: Vec::new(),
                rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
            },
        );
        let mut batch =
            DiskSyncBatch::new_producing(child_pid, snapshot_path.clone(), zero_boundary(), 5);
        batch.add_waiting_target(replica_id);
        runtime.full_sync.disk_sync_batch = Some(batch);
        runtime.drop_disk_sync_batch_target(replica_id);

        dispatch_disk_sync_child_completion(&mut runtime, child_pid);

        assert_eq!(runtime.ctx.persistence().dirty(), 5);
        assert_eq!(runtime.ctx.persistence().rdb().last_save_time_sec, 0);
        assert_eq!(
            runtime.ctx.persistence().rdb().last_bgsave_status,
            OperationStatus::Err
        );
        assert!(runtime.full_sync.disk_sync_batch.is_none());
        assert!(!runtime.ctx.persistence().has_active_child());
        assert!(!snapshot_path.exists());
        assert!(dir.join("dump.rdb").is_dir());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn saveparams_intent_survives_late_join_until_serving_finishes() {
        use crate::engine::extension::config::persistence::SaveParam;
        use crate::engine::persistence::OperationStatus;

        let dir = std::env::temp_dir().join(format!(
            "valkey-disk-sync-late-join-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let snapshot_path = dir.join("temp-replication.rdb");
        std::fs::write(&snapshot_path, b"snapshot").unwrap();
        let child_pid = successful_child();
        let mut runtime = ServerWorkerState::new(1).unwrap();
        runtime.ctx.config_mut().persistence.dir = dir.to_string_lossy().into_owned();
        runtime.ctx.config_mut().persistence.dbfilename = "dump.rdb".to_string();
        runtime.ctx.config_mut().persistence.saveparams = vec![SaveParam {
            seconds: 900,
            changes: 1,
        }];
        runtime.ctx.persistence_mut().add_dirty(5);
        runtime
            .ctx
            .persistence_mut()
            .begin_disk_sync_child(child_pid)
            .unwrap();
        let first = add_replica(&mut runtime, 58);
        let late = add_replica(&mut runtime, 59);
        let mut batch =
            DiskSyncBatch::new_producing(child_pid, snapshot_path.clone(), zero_boundary(), 5);
        batch.add_waiting_target(first);
        runtime.full_sync.disk_sync_batch = Some(batch);

        runtime.drop_disk_sync_batch_target(first);
        assert_eq!(runtime.join_disk_sync_batch(late), Some(zero_boundary()));
        runtime.ctx.persistence_mut().add_dirty(3);
        dispatch_disk_sync_child_completion(&mut runtime, child_pid);
        let batch = runtime.full_sync.disk_sync_batch.as_mut().unwrap();
        assert_eq!(batch.phase, super::DiskSyncPhase::Serving);
        assert!(batch.persist_when_targetless);
        assert!(batch.remove_target(late));

        runtime.cleanup_finished_disk_sync_batch(false);

        assert_eq!(std::fs::read(dir.join("dump.rdb")).unwrap(), b"snapshot");
        assert_eq!(runtime.ctx.persistence().dirty(), 3);
        assert!(runtime.ctx.persistence().rdb().last_save_time_sec > 0);
        assert_eq!(
            runtime.ctx.persistence().rdb().last_bgsave_status,
            OperationStatus::Ok
        );
        assert!(runtime.full_sync.disk_sync_batch.is_none());
        assert!(!snapshot_path.exists());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn saveparams_failure_is_recorded_even_with_a_late_target() {
        use crate::engine::extension::config::persistence::SaveParam;
        use crate::engine::persistence::OperationStatus;

        let dir = std::env::temp_dir().join(format!(
            "valkey-disk-sync-late-join-failure-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let snapshot_path = dir.join("temp-replication.rdb");
        std::fs::write(&snapshot_path, b"snapshot").unwrap();
        let child_pid = failed_child();
        let mut runtime = ServerWorkerState::new(1).unwrap();
        runtime.ctx.config_mut().persistence.dir = dir.to_string_lossy().into_owned();
        runtime.ctx.config_mut().persistence.dbfilename = "dump.rdb".to_string();
        runtime.ctx.config_mut().persistence.saveparams = vec![SaveParam {
            seconds: 900,
            changes: 1,
        }];
        runtime.ctx.persistence_mut().add_dirty(5);
        runtime
            .ctx
            .persistence_mut()
            .begin_disk_sync_child(child_pid)
            .unwrap();
        let first = add_replica(&mut runtime, 60);
        let late = add_replica(&mut runtime, 61);
        let mut batch =
            DiskSyncBatch::new_producing(child_pid, snapshot_path.clone(), zero_boundary(), 5);
        batch.add_waiting_target(first);
        runtime.full_sync.disk_sync_batch = Some(batch);
        runtime.drop_disk_sync_batch_target(first);
        assert_eq!(runtime.join_disk_sync_batch(late), Some(zero_boundary()));
        runtime.ctx.persistence_mut().add_dirty(3);

        dispatch_disk_sync_child_completion(&mut runtime, child_pid);

        assert_eq!(runtime.ctx.persistence().dirty(), 8);
        assert_eq!(runtime.ctx.persistence().rdb().last_save_time_sec, 0);
        assert_eq!(
            runtime.ctx.persistence().rdb().last_bgsave_status,
            OperationStatus::Err
        );
        assert!(runtime.full_sync.disk_sync_batch.is_none());
        assert!(!snapshot_path.exists());
        assert!(!dir.join("dump.rdb").exists());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn late_join_disconnect_promotes_preserved_saveparams_snapshot() {
        use crate::engine::extension::config::persistence::SaveParam;
        use crate::engine::persistence::OperationStatus;

        let dir = std::env::temp_dir().join(format!(
            "valkey-disk-sync-late-disconnect-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let snapshot_path = dir.join("temp-replication.rdb");
        std::fs::write(&snapshot_path, b"snapshot").unwrap();
        let child_pid = successful_child();
        let mut runtime = ServerWorkerState::new(1).unwrap();
        runtime.ctx.config_mut().persistence.dir = dir.to_string_lossy().into_owned();
        runtime.ctx.config_mut().persistence.dbfilename = "dump.rdb".to_string();
        runtime.ctx.config_mut().persistence.saveparams = vec![SaveParam {
            seconds: 900,
            changes: 1,
        }];
        runtime.ctx.persistence_mut().add_dirty(5);
        runtime
            .ctx
            .persistence_mut()
            .begin_disk_sync_child(child_pid)
            .unwrap();
        let first = add_replica(&mut runtime, 62);
        let late = add_replica(&mut runtime, 63);
        let mut batch =
            DiskSyncBatch::new_producing(child_pid, snapshot_path.clone(), zero_boundary(), 5);
        batch.add_waiting_target(first);
        runtime.full_sync.disk_sync_batch = Some(batch);
        runtime.drop_disk_sync_batch_target(first);
        assert_eq!(runtime.join_disk_sync_batch(late), Some(zero_boundary()));
        dispatch_disk_sync_child_completion(&mut runtime, child_pid);
        assert_eq!(
            runtime.full_sync.disk_sync_batch.as_ref().unwrap().phase,
            super::DiskSyncPhase::Serving
        );

        runtime.drop_disk_sync_batch_target(late);

        assert_eq!(std::fs::read(dir.join("dump.rdb")).unwrap(), b"snapshot");
        assert_eq!(runtime.ctx.persistence().dirty(), 0);
        assert_eq!(
            runtime.ctx.persistence().rdb().last_bgsave_status,
            OperationStatus::Ok
        );
        assert!(runtime.full_sync.disk_sync_batch.is_none());
        assert!(!snapshot_path.exists());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
