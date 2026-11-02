use super::super::ServerWorkerState;
use crate::engine::distributed::replication::BacklogCursor;
use crate::foundation::io::transport::{Interest, WriteOutcome};
use crate::framework::capabilities::connection::{Connection, ConnectionState};
use crate::framework::capabilities::io::Token;
use crate::runtime::server::worker::reactor::io_workers::{
    WriteCompletion, WriteCompletionAction, WriteSubmission,
};
use std::collections::{HashSet, VecDeque};
use std::io::{IoSlice, Write};

#[cfg(test)]
mod regression_tests;
mod replica_disk_error;
mod state;

use replica_disk_error::{replica_disk_write_error_action, ReplicaDiskWriteErrorAction};
use state::{WritableContext, WritableFlush, WritableTarget};

const NET_MAX_WRITES_PER_EVENT: usize = 64 * 1024;
const MAX_IOV_PER_WRITE: usize = 128;

fn write_failure_log_message(token: Token, owner: &str, error: &std::io::Error) -> String {
    format!(
        "write failed: token={} owner={} operation=write error={}",
        token.0, owner, error
    )
}

fn writable_failure_log_message(context: &WritableContext, error: &std::io::Error) -> String {
    write_failure_log_message(context.token, context.target.as_str(), error)
}

fn replication_accounting_error() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "replication output accounting invariant violated",
    )
}

pub(super) fn is_slot_export_connection(connection: &Connection) -> bool {
    matches!(
        connection.slot_migration_role,
        Some(
            crate::engine::distributed::cluster::migration::SlotMigrationConnectionRole::Export(_)
        )
    )
}

/// Match C `writevToClient`: include complete reply frames until the first
/// frame crosses the per-event write threshold. A single large bulk reply may
/// exceed the threshold, but later replies wait for another writable turn.
fn c_client_write_budget(queue: &VecDeque<Vec<u8>>) -> usize {
    let mut total = 0usize;
    for frame in queue.iter().take(MAX_IOV_PER_WRITE) {
        total = total.saturating_add(frame.len());
        if total > NET_MAX_WRITES_PER_EVENT {
            break;
        }
    }
    total
}

fn flush_queue_vectored(
    stream: &mut impl Write,
    queue: &mut VecDeque<Vec<u8>>,
    byte_budget: Option<usize>,
) -> (WriteOutcome, usize) {
    let mut total_written = 0usize;
    let mut released_allocation_bytes = 0usize;
    loop {
        if queue.is_empty() {
            return (WriteOutcome::Complete, released_allocation_bytes);
        }
        let remaining_budget = byte_budget
            .map(|budget| budget.saturating_sub(total_written))
            .unwrap_or(usize::MAX);
        if remaining_budget == 0 {
            return (WriteOutcome::BudgetExhausted, released_allocation_bytes);
        }
        // Compute total and call write_vectored; iov_buf must be dropped before mutating queue
        let (total, nwritten) = {
            let mut iov_buf = [IoSlice::new(&[]); MAX_IOV_PER_WRITE];
            let mut total = 0usize;
            let mut nslices = 0usize;
            for chunk in queue.iter().take(MAX_IOV_PER_WRITE) {
                let available = remaining_budget.saturating_sub(total);
                if available == 0 {
                    break;
                }
                let included = chunk.len().min(available);
                iov_buf[nslices] = IoSlice::new(&chunk[..included]);
                total += included;
                nslices += 1;
            }
            match stream.write_vectored(&iov_buf[..nslices]) {
                Ok(0) => {
                    // Peer closed/EOF: leave the unsent data in the queue so
                    // backpressure gates stay closed (e.g. diskless relay does
                    // not keep draining the pipe into a dead socket). The
                    // owner observes Failed and performs role-aware teardown.
                    // Clearing the queue here would falsely signal "drained"
                    // and let in-flight relays finish before the connection
                    // death is observed (GH issue: slow-replica-drop count
                    // reported 2 replicas still up after one died).
                    return (
                        WriteOutcome::Failed(std::io::ErrorKind::WriteZero.into()),
                        released_allocation_bytes,
                    );
                }
                Ok(n) => (total, n),
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    return (WriteOutcome::Backpressured, released_allocation_bytes);
                }
                Err(error) => {
                    // Hard write error (EPIPE/ECONNRESET/etc.). Same reasoning
                    // as Ok(0): do not drop unsent bytes before owner teardown.
                    return (WriteOutcome::Failed(error), released_allocation_bytes);
                }
            }
        };
        total_written = total_written.saturating_add(nwritten);
        let mut n = nwritten;
        while n > 0 {
            let Some(front) = queue.front() else {
                break;
            };
            let flen = front.len();
            if n >= flen {
                let frame = queue.pop_front().expect("front exists");
                released_allocation_bytes = released_allocation_bytes.saturating_add(
                    crate::foundation::memory::util_alloc::vec_allocation_bytes(&frame),
                );
                n -= flen;
            } else {
                queue.front_mut().unwrap().drain(..n);
                break;
            }
        }
        if nwritten < total {
            return (WriteOutcome::Backpressured, released_allocation_bytes);
        }
        if byte_budget.is_some_and(|budget| total_written >= budget) && !queue.is_empty() {
            return (WriteOutcome::BudgetExhausted, released_allocation_bytes);
        }
    }
}

impl ServerWorkerState {
    fn try_submit_threaded_write(
        &mut self,
        connection_token: usize,
        byte_budget: Option<usize>,
    ) -> bool {
        let token = self.connection_registry[Token(connection_token)].token();
        if !self.connection_registry[Token(connection_token)].has_pending_output() {
            return false;
        }
        let snapshot =
            self.connection_registry[Token(connection_token)].take_threaded_write_snapshot();
        if snapshot.logical_len() == 0 {
            self.connection_registry[Token(connection_token)]
                .restore_threaded_write_snapshot(snapshot.into_frames());
            return false;
        }
        match self.io_threads.submit_write(
            token,
            snapshot.into_frames(),
            byte_budget.unwrap_or(usize::MAX),
        ) {
            WriteSubmission::Submitted => {
                if let Some(id) = self.io_threads.write_inflight_id(token) {
                    self.acquire_io_worker_write_socket_lease(id);
                }
                true
            }
            WriteSubmission::AlreadyPending(frames) => {
                self.connection_registry[Token(connection_token)]
                    .restore_threaded_write_snapshot(frames);
                true
            }
            WriteSubmission::Unavailable(frames) => {
                self.connection_registry[Token(connection_token)]
                    .restore_threaded_write_snapshot(frames);
                false
            }
        }
    }

    pub(crate) fn apply_threaded_write_completion(&mut self, completion: WriteCompletion) {
        let id = completion.id;
        match self.io_threads.complete_write(completion) {
            WriteCompletionAction::Apply {
                token,
                frames,
                written,
                released_allocation_bytes,
                outcome,
            } => {
                let Some(connection_token) = self
                    .connection_registry
                    .contains(Token(token.0))
                    .then_some(token.0)
                else {
                    self.release_io_worker_write_socket_lease(id);
                    return;
                };
                let queued_before = written.checked_add(
                    frames
                        .iter()
                        .try_fold(0usize, |total, frame| total.checked_add(frame.len()))
                        .unwrap_or(usize::MAX),
                );
                self.record_replication_wire_write(token.0, written);
                let replication_written = {
                    let connection = &mut self.connection_registry[Token(connection_token)];
                    connection.restore_threaded_write_snapshot(frames);
                    let replication_written = queued_before.ok_or(()).and_then(|queued_before| {
                        connection
                            .consume_written_bytes(written, queued_before)
                            .map_err(|_| ())
                    });
                    connection.release_output_frame_allocations(released_allocation_bytes);
                    replication_written
                };
                let Ok(replication_written) = replication_written else {
                    crate::server_log!(
                        warning,
                        "Closing connection token={} after output accounting invariant violation",
                        token.0
                    );
                    self.release_io_worker_write_socket_lease(id);
                    self.remove_connection(token.0);
                    return;
                };
                let backlog_end = self.ctx.replication().backlog.next_stream_cursor();
                if !self.apply_replication_written(token.0, replication_written, backlog_end) {
                    self.release_io_worker_write_socket_lease(id);
                    return;
                }
                if replication_written > 0 {
                    self.replication_buffer_accounting_dirty = true;
                }
                self.refresh_client_memory_usage(token, false);
                if let WriteOutcome::Failed(error) = &outcome {
                    crate::server_log!(
                        warning,
                        "{}",
                        write_failure_log_message(token, "ordinary-client", error)
                    );
                    self.release_io_worker_write_socket_lease(id);
                    self.remove_connection(token.0);
                    return;
                }
                if self.connection_registry[Token(connection_token)].has_pending_output() {
                    if matches!(outcome, WriteOutcome::BudgetExhausted) {
                        self.schedule_runtime_runnable_next_turn(
                            crate::runtime::server::worker::reactor::RunnableKey::ClientWrite(
                                token,
                            ),
                        );
                    }
                    let interest = if self.connection_registry[Token(connection_token)].state
                        == ConnectionState::Closing
                    {
                        Interest::Writable
                    } else {
                        Interest::ReadWritable
                    };
                    let _ = self.transport.set_interest(token, interest);
                } else {
                    if self.connection_registry[Token(connection_token)].state
                        == ConnectionState::Closing
                        && !self.io_threads.is_write_inflight(token)
                    {
                        self.remove_connection(token.0);
                    } else {
                        let _ = self.transport.set_interest(token, Interest::Readable);
                    }
                }
            }
            WriteCompletionAction::CloseTransport(token) => {
                self.deregister_or_defer(token, super::super::PendingRemovalOwner::Client);
            }
            WriteCompletionAction::Stale => {}
        }
        self.release_io_worker_write_socket_lease(id);
    }

    fn connection_write_budget(
        &mut self,
        connection_token: usize,
        token_value: usize,
        is_slot_export: bool,
    ) -> Option<usize> {
        if is_slot_export {
            return Some(
                self.writes
                    .slot_exports
                    .remaining_budget(Token(token_value), NET_MAX_WRITES_PER_EVENT),
            );
        }
        let connection_id = crate::framework::distributed::replication::ConnectionId(token_value);
        if self.replication_runtime.role(connection_id).is_some() {
            return None;
        }
        let over_maxmemory = self.ctx.maxmemory_state().is_over_limit();
        if over_maxmemory
            || self.connection_registry[Token(connection_token)]
                .output
                .queue
                .is_empty()
        {
            None
        } else {
            Some(c_client_write_budget(
                &self.connection_registry[Token(connection_token)]
                    .output
                    .queue,
            ))
        }
    }

    fn schedule_slot_export_write_continuation(
        &mut self,
        token_value: usize,
        outcome: &WriteOutcome,
    ) {
        if !matches!(outcome, WriteOutcome::BudgetExhausted)
            || !self
                .connection_registry
                .get(Token(token_value))
                .is_some_and(|connection| {
                    is_slot_export_connection(connection) && connection.has_pending_output()
                })
        {
            return;
        }
        self.schedule_runtime_runnable_next_turn(
            crate::runtime::server::worker::reactor::RunnableKey::SlotExportWrite(Token(
                token_value,
            )),
        );
    }

    pub(crate) fn drain_current_slot_export_writes(&mut self) {
        let _ = self.dispatch_before_poll_slot(
            crate::runtime::server::worker::reactor::BeforePollSlot::DistributedWrites,
            crate::runtime::server::worker::reactor::DispatchContext::Normal,
        );
    }

    pub(crate) fn drive_pending_slot_export_writes(&mut self) {
        self.drain_current_slot_export_writes();
    }

    /// Schedule C `handleClientsWithPendingWrites` work for downstream replicas.
    /// Replication propagation queues bytes while a client command is being
    /// processed. The `DistributedWrites` before-poll slot gives each ready
    /// replica one write attempt before the event loop sleeps; a
    /// partial/WouldBlock result keeps writable interest armed through
    /// `handle_writable`.
    pub(crate) fn schedule_pending_replica_writes_before_sleep(&mut self) {
        let streaming: HashSet<Token> = self
            .replication_runtime
            .primary_links()
            .values()
            .filter(|link| {
                link.state()
                    == crate::framework::distributed::replication::PrimaryLinkState::Streaming
            })
            .map(|link| Token(link.connection_id().0))
            .collect();
        self.writes.retain_streaming_replica_waiters(&streaming);
        let pending: Vec<Token> = streaming
            .into_iter()
            .filter(|token| !self.writes.replica_waiting_for_writable(*token))
            .filter(|token_value| {
                self.connection_registry
                    .get(*token_value)
                    .is_some_and(|connection| {
                        !connection.protected
                            && connection.state != ConnectionState::CloseAfterReply
                            && connection.has_pending_output()
                    })
            })
            .collect();

        for token in pending {
            self.schedule_runtime_runnable(
                crate::runtime::server::worker::reactor::RunnableKey::ClientWrite(token),
            );
        }
        self.refresh_replication_buffer_accounting_if_dirty();
    }

    /// Flush the output queue for a connection synchronously, without changing
    /// interest or handling close-after-write. Returns the flush outcome so
    /// callers (e.g. the diskless relay pump) can synchronously tear down a
    /// connection that hit a hard write error, mirroring C's
    /// `rdbPipeReadHandler` which calls `freeClient` + NULLs the
    /// `rdb_pipe_conns[]` slot immediately on connWrite failure
    /// (replication.c:1902-1908).
    pub(crate) fn flush_connection_output(&mut self, connection_token: usize) -> WriteOutcome {
        let token_value = self.connection_registry[Token(connection_token)].token().0;
        let is_master_link = self.is_master_link(token_value);
        let is_slot_export =
            is_slot_export_connection(&self.connection_registry[Token(connection_token)]);
        let write_budget =
            self.connection_write_budget(connection_token, token_value, is_slot_export);
        let (result, replication_written, written) = {
            let conn = &mut self.connection_registry[Token(connection_token)];
            let mut stream = match self.transport.stream(conn.token()) {
                Some(s) => s,
                None => {
                    return WriteOutcome::Failed(std::io::Error::new(
                        std::io::ErrorKind::NotConnected,
                        "client transport is not registered",
                    ));
                }
            };
            let mut queue = std::mem::take(&mut conn.output.queue);
            let queued_before: usize = queue.iter().map(Vec::len).sum();
            let (result, released_allocation_bytes) =
                flush_queue_vectored(&mut stream, &mut queue, write_budget);
            let written = queued_before.saturating_sub(queue.iter().map(Vec::len).sum::<usize>());
            let replication_written = conn.consume_written_bytes(written, queued_before);
            conn.release_output_frame_allocations(released_allocation_bytes);
            conn.output.queue = queue;
            (result, replication_written, written)
        };
        self.record_replication_wire_write(token_value, written);
        let replication_written = match replication_written {
            Ok(replication_written) => replication_written,
            Err(_) => return WriteOutcome::Failed(replication_accounting_error()),
        };
        self.refresh_client_memory_usage(Token(token_value), false);
        if is_slot_export {
            self.writes
                .slot_exports
                .consume_budget(Token(token_value), written);
            self.reconcile_slot_export_output_allocation(token_value);
        }
        let backlog_end = self.ctx.replication().backlog.next_stream_cursor();
        if !is_master_link
            && !self.apply_replication_written(token_value, replication_written, backlog_end)
        {
            return WriteOutcome::Failed(replication_accounting_error());
        }
        // If this connection is a replica (primary link) and the socket
        // failed to drain the output queue, stamp the first stall moment
        // for the "(full sync)" replica-timeout path (replication.c:5429-5437).
        // Cleared in `handle_writable` once the queue is fully drained.
        if !is_master_link
            && !self.connection_registry[Token(connection_token)]
                .output
                .queue
                .is_empty()
        {
            let cid = crate::framework::distributed::replication::ConnectionId(token_value);
            if let Some(link) = self.replication_runtime.primary_for_connection_mut(cid) {
                link.mark_partial_write(crate::foundation::io::sys::time::unixtime());
            }
        }
        if replication_written > 0 {
            self.replication_buffer_accounting_dirty = true;
        }
        self.schedule_slot_export_write_continuation(token_value, &result);
        result
    }

    pub(crate) fn handle_writable(&mut self, token: Token) {
        if !self.socket_write_owned_by_main_thread(token) {
            return;
        }
        let Some(context) = self.prepare_writable_context(token) else {
            return;
        };
        let replica_target = self.replication_runtime.replica_id_for_connection(
            crate::framework::distributed::replication::ConnectionId(context.token.0),
        );
        let write_budget = self.connection_write_budget(
            context.token.0,
            context.token.0,
            context.is_slot_export(),
        );
        if context.is_ordinary() && self.try_submit_threaded_write(context.token.0, write_budget) {
            return;
        }
        let flush = self.flush_writable_queue(&context, write_budget);
        if !self.record_writable_flush(&context, &flush) {
            return;
        }
        if let WriteOutcome::Failed(error) = &flush.result {
            self.handle_writable_error(&context, error);
            return;
        }
        self.schedule_slot_export_write_continuation(context.token.0, &flush.result);
        self.log_writable_flush(&context, &flush);
        if flush.flushed_dual_channel_sync {
            self.start_dual_rdb_wait(context.token);
        }
        if matches!(&flush.result, WriteOutcome::Complete) {
            self.finish_drained_writable(&context, flush.flushed_dual_rdb_client_id);
        } else {
            self.finish_partial_writable(&context, &flush.result);
        }
        if matches!(&flush.result, WriteOutcome::Complete) {
            if let Some(id) = replica_target {
                let _ = self.on_diskless_target_writable(id);
                let _ = self.on_disk_sync_batch_target_writable(id);
            }
            if context.is_slot_export() {
                if let Some(id) = self
                    .connection_registry
                    .get(context.token)
                    .and_then(|connection| connection.slot_migration_role.as_ref())
                    .map(|role| role.job_id().clone())
                {
                    self.on_slot_snapshot_target_writable(&id);
                }
            }
        }
    }

    fn prepare_writable_context(&mut self, token: Token) -> Option<WritableContext> {
        if !self.connection_registry.contains(token) {
            return None;
        }
        let role = self.replication_runtime.role(
            crate::framework::distributed::replication::ConnectionId(token.0),
        );
        let is_slot_export = is_slot_export_connection(&self.connection_registry[token]);
        let is_ordinary = role.is_none() && !is_slot_export;
        if is_ordinary && !self.writes.ordinary.claim_service(token) {
            return None;
        }
        if is_slot_export && !self.writes.slot_exports.claim_service(token) {
            return None;
        }
        let _ = self.handle_slot_export_writable(token.0);
        if !self.connection_registry.contains(token) {
            return None;
        }
        if self.is_master_link(token.0) && !self.finish_master_connect(token) {
            return None;
        }
        let target = if is_slot_export_connection(&self.connection_registry[token]) {
            WritableTarget::SlotExport
        } else if self.is_master_link(token.0) {
            WritableTarget::ReplicaMain
        } else if self
            .replication_runtime
            .replica_rdb_link()
            .is_some_and(|link| link.connection_id().0 == token.0)
        {
            WritableTarget::ReplicaRdb
        } else if role.is_some() {
            WritableTarget::DownstreamReplica
        } else {
            WritableTarget::OrdinaryClient
        };
        Some(WritableContext { token, target })
    }

    fn finish_master_connect(&mut self, token: Token) -> bool {
        if self.ctx.config().replication.master_link_status != "connect" {
            return true;
        }
        match self.transport.take_connect_error(token) {
            Ok(None) => {
                self.handle_master_link_connected(token.0);
                self.ctx.config_mut().replication.master_link_status = "handshake".to_string();
                true
            }
            Ok(Some(error)) | Err(error) => {
                crate::server_log!(
                    warning,
                    "runtime source lifecycle token={} owner=connection operation=connect error={}",
                    token.0,
                    error
                );
                self.remove_connection(token.0);
                self.ctx.config_mut().replication.master_link_status = "down".to_string();
                false
            }
        }
    }

    fn flush_writable_queue(
        &mut self,
        context: &WritableContext,
        write_budget: Option<usize>,
    ) -> WritableFlush {
        let conn = &mut self.connection_registry[context.token];
        let Some(mut stream) = self.transport.stream(conn.token()) else {
            return WritableFlush {
                result: WriteOutcome::Failed(std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    "client transport is not registered",
                )),
                flushed_dual_channel_sync: false,
                flushed_dual_rdb_client_id: false,
                replication_written: 0,
                written: 0,
            };
        };
        let mut queue = std::mem::take(&mut conn.output.queue);
        let queued_before: usize = queue.iter().map(Vec::len).sum();
        let flushing_dual_rdb_client_id = context.is_master_link()
            && queue.iter().any(|frame| {
                frame
                    .windows(b"set-rdb-client-id".len())
                    .any(|window| window.eq_ignore_ascii_case(b"set-rdb-client-id"))
            });
        let flushing_dual_channel_sync = !context.is_master_link()
            && queue.iter().any(|frame| {
                frame == crate::engine::distributed::replication::protocol::DUAL_CHANNEL_SYNC_REPLY
            });
        let (result, released) = flush_queue_vectored(&mut stream, &mut queue, write_budget);
        let written = queued_before.saturating_sub(queue.iter().map(Vec::len).sum::<usize>());
        let replication_written = conn.consume_written_bytes(written, queued_before);
        conn.release_output_frame_allocations(released);
        conn.output.queue = queue;
        let (result, replication_written) = match replication_written {
            Ok(replication_written) => (result, replication_written),
            Err(_) => (WriteOutcome::Failed(replication_accounting_error()), 0),
        };
        let flush_succeeded = matches!(
            &result,
            WriteOutcome::Complete | WriteOutcome::BudgetExhausted | WriteOutcome::Backpressured
        );
        let flushed_dual_channel_sync =
            matches!(&result, WriteOutcome::Complete) && flushing_dual_channel_sync;
        WritableFlush {
            result,
            flushed_dual_channel_sync,
            flushed_dual_rdb_client_id: flush_succeeded && flushing_dual_rdb_client_id,
            replication_written,
            written,
        }
    }

    fn record_writable_flush(&mut self, context: &WritableContext, flush: &WritableFlush) -> bool {
        self.record_replication_wire_write(context.token.0, flush.written);
        self.refresh_client_memory_usage(context.token, false);
        if context.is_slot_export() {
            self.writes
                .slot_exports
                .consume_budget(context.token, flush.written);
            self.reconcile_slot_export_output_allocation(context.token.0);
        }
        let backlog_end = self.ctx.replication().backlog.next_stream_cursor();
        if !context.is_master_link()
            && !self.apply_replication_written(
                context.token.0,
                flush.replication_written,
                backlog_end,
            )
        {
            return false;
        }
        if flush.replication_written > 0 {
            self.replication_buffer_accounting_dirty = true;
        }
        true
    }

    fn apply_replication_written(
        &mut self,
        token_value: usize,
        bytes: usize,
        backlog_end: BacklogCursor,
    ) -> bool {
        if bytes == 0 {
            return true;
        }
        let connection_id = crate::framework::distributed::replication::ConnectionId(token_value);
        let result = self
            .replication_runtime
            .primary_for_connection_mut(connection_id)
            .map(|link| link.mark_replication_written(bytes, backlog_end));
        if let Some(stats) = self
            .replication_runtime
            .primary_for_connection(connection_id)
            .and_then(|link| link.compression_stats())
        {
            self.ctx
                .replication_mut()
                .view
                .compression_stats
                .insert(token_value, stats);
        }
        if !matches!(result, Some(Err(_))) {
            return true;
        }
        crate::server_log!(
            warning,
            "Closing replica token={} after replication cursor accounting invariant violation",
            token_value
        );
        self.remove_connection(token_value);
        false
    }

    fn handle_writable_error(&mut self, context: &WritableContext, error: &std::io::Error) {
        crate::server_log!(warning, "{}", writable_failure_log_message(context, error));
        if context.is_replica_rdb() {
            crate::server_log!(
                warning,
                "dual-rdb open: rdb={} initial frame flush failed",
                context.token.0
            );
        }
        self.remove_connection(context.token.0);
    }

    fn log_writable_flush(&self, context: &WritableContext, flush: &WritableFlush) {
        if flush.flushed_dual_rdb_client_id && matches!(&flush.result, WriteOutcome::Complete) {
            crate::server_log!(
                debug,
                "dual-rdb main handshake: flushed REPLCONF set-rdb-client-id main_token={}",
                context.token.0
            );
        }
        if context.is_replica_rdb() {
            crate::server_log!(
                debug,
                "dual-rdb open: rdb={} writable flush={:?} pending_bytes={}",
                context.token.0,
                &flush.result,
                self.connection_registry[context.token]
                    .output
                    .queue
                    .iter()
                    .map(Vec::len)
                    .sum::<usize>()
            );
        }
    }

    fn start_dual_rdb_wait(&mut self, token: Token) {
        let delay_ms = self
            .ctx
            .config()
            .replication
            .repl_diskless_sync_delay
            .saturating_mul(1_000)
            .max(0);
        let deadline_ms = crate::foundation::io::sys::time::mstime()
            .saturating_add(delay_ms)
            .saturating_add(5_000);
        let connection_id = crate::framework::distributed::replication::ConnectionId(token.0);
        if let Some(link) = self
            .replication_runtime
            .primary_for_connection_mut(connection_id)
        {
            link.rdb_channel_wait_deadline_ms = Some(deadline_ms);
            crate::server_log!(
                debug,
                "dual-rdb main handshake: flushed +DUALCHANNELSYNC main_token={} wait_deadline_ms={}",
                token.0,
                deadline_ms
            );
        }
    }

    fn finish_drained_writable(
        &mut self,
        context: &WritableContext,
        flushed_dual_rdb_client_id: bool,
    ) {
        self.writes.clear_replica_writable_wait(context.token);
        let key = if context.is_slot_export() {
            crate::runtime::server::worker::reactor::RunnableKey::SlotExportWrite(context.token)
        } else {
            crate::runtime::server::worker::reactor::RunnableKey::ClientWrite(context.token)
        };
        self.cancel_runtime_runnable(key);
        if context.is_master_link() {
            if let Some(link) = self.replication_runtime.replica_link_mut() {
                link.mark_io_activity();
            }
        } else {
            // Mirrors C's rdbPipeWriteHandlerConnRemoved clearing the partial
            // write stamp once a replica catches up.
            let id = crate::framework::distributed::replication::ConnectionId(context.token.0);
            if let Some(link) = self.replication_runtime.primary_for_connection_mut(id) {
                link.clear_partial_write();
            }
        }
        let keep_master_paused =
            context.is_master_link() && self.replication_runtime.replica_main_read_paused();
        if !self.rearm_drained_writable(context.token, keep_master_paused) {
            return;
        }
        if self.connection_registry[context.token].state == ConnectionState::Closing {
            self.remove_connection(context.token.0);
        } else if flushed_dual_rdb_client_id && !keep_master_paused {
            self.read_dual_rdb_ack_after_flush(context.token);
        } else {
            self.queue_forked_debugger_for_reprocessing(context.token);
        }
    }

    fn rearm_drained_writable(&mut self, token: Token, keep_master_paused: bool) -> bool {
        let interest = if keep_master_paused {
            if self.ctx.persistence().forkless_capture_in_progress() {
                Interest::None
            } else {
                Interest::Writable
            }
        } else {
            Interest::Readable
        };
        if let Err(error) = self.transport.set_interest(token, interest) {
            crate::server_log!(
                warning,
                "failed to rearm connection interest: token={} interest={:?} error={}",
                token.0,
                interest,
                error
            );
            self.remove_connection(token.0);
            return false;
        }
        true
    }

    fn record_replication_wire_write(&mut self, token_value: usize, bytes: usize) {
        use crate::framework::distributed::replication::{ConnectionId, ReplicationConnection};
        if matches!(
            self.replication_runtime.role(ConnectionId(token_value)),
            Some(
                ReplicationConnection::Primary(_)
                    | ReplicationConnection::RdbChannelCandidate
                    | ReplicationConnection::RdbChannel { .. }
            )
        ) {
            self.ctx.stats_mut().add_net_repl_output_bytes(bytes as u64);
        }
    }

    fn read_dual_rdb_ack_after_flush(&mut self, token: Token) {
        crate::server_log!(
            debug,
            "dual-rdb main handshake: proactively reading +OK after set-rdb-client-id flush token={}",
            token.0
        );
        self.handle_master_link_readable(token.0, token.0);
        if !self.connection_registry.contains(token)
            || !self.connection_registry[token].has_pending_output()
        {
            return;
        }
        let result = self.flush_connection_output(token.0);
        match result {
            WriteOutcome::Failed(error) => {
                crate::server_log!(
                    warning,
                    "{}",
                    write_failure_log_message(token, "replica-main", &error)
                );
                self.remove_connection(token.0);
            }
            WriteOutcome::BudgetExhausted | WriteOutcome::Backpressured => {
                let interest = if self.replication_runtime.replica_main_read_paused() {
                    if self.ctx.persistence().forkless_capture_in_progress() {
                        Interest::None
                    } else {
                        Interest::Writable
                    }
                } else {
                    Interest::ReadWritable
                };
                self.transport.set_interest(token, interest).ok();
            }
            WriteOutcome::Complete => {}
        }
    }

    fn finish_partial_writable(&mut self, context: &WritableContext, outcome: &WriteOutcome) {
        if !context.is_master_link() {
            let cid = crate::framework::distributed::replication::ConnectionId(context.token.0);
            if let Some(link) = self.replication_runtime.primary_for_connection_mut(cid) {
                link.mark_partial_write(crate::foundation::io::sys::time::unixtime());
            }
            if !context.is_slot_export()
                && self
                    .transport
                    .set_interest(context.token, Interest::ReadWritable)
                    .is_err()
            {
                self.remove_connection(context.token.0);
                return;
            }
            if context.is_downstream_replica() {
                match outcome {
                    WriteOutcome::Backpressured => {
                        self.writes.mark_replica_waiting_for_writable(context.token)
                    }
                    WriteOutcome::BudgetExhausted => {
                        self.writes.clear_replica_writable_wait(context.token);
                        if self.connection_registry[context.token].has_pending_output() {
                            self.schedule_runtime_runnable_next_turn(
                                crate::runtime::server::worker::reactor::RunnableKey::ClientWrite(
                                    context.token,
                                ),
                            );
                        }
                    }
                    WriteOutcome::Complete | WriteOutcome::Failed(_) => {}
                }
            }
            if matches!(outcome, WriteOutcome::BudgetExhausted)
                && context.is_ordinary()
                && self.is_scheduleable_ordinary_write(context.token.0)
            {
                self.schedule_runtime_runnable_next_turn(
                    crate::runtime::server::worker::reactor::RunnableKey::ClientWrite(
                        context.token,
                    ),
                );
            }
        }
    }

    pub(crate) fn reap_bgsave_child(&mut self) {
        if let Some(outcome) = self
            .ctx
            .poll_forkless_save(crate::foundation::io::sys::time::mstime() / 1_000)
        {
            use crate::foundation::io::persistence::snapshot::background::RdbSaveOutcome;
            match outcome {
                RdbSaveOutcome::Saved => {
                    crate::server_log!(notice, "Forkless background saving completed")
                }
                RdbSaveOutcome::Cancelled => {
                    crate::server_log!(notice, "Forkless background saving cancelled")
                }
                RdbSaveOutcome::Failed(error) => {
                    crate::server_log!(warning, "Forkless background saving failed: {}", error)
                }
            }
            self.release_forkless_clients();
        }
        let Some(pid) = self.ctx.persistence().rdb_child_pid() else {
            return;
        };
        self.register_child_process(
            crate::runtime::server::worker::reactor::sources::ChildOwner::RdbSave,
            pid,
        );
        let Some(completion) = self.take_child_completion_for_pid(pid) else {
            return;
        };
        let exited_ok = match &completion.outcome {
            crate::runtime::server::worker::reactor::sources::ChildExitOutcome::Exited(0) => true,
            crate::runtime::server::worker::reactor::sources::ChildExitOutcome::WaitFailed(_) => {
                crate::server_log!(warning, "waitpid on bgsave child {} failed", pid);
                self.ctx.persistence_mut().abort_child();
                self.finalize_child_registry_entry(completion.child);
                return;
            }
            _ => false,
        };
        let _ = crate::framework::commands::server::save::apply_bgsave_child_completion(
            &mut self.ctx,
            pid,
            exited_ok,
            crate::foundation::io::sys::time::mstime() / 1_000,
        );
        self.finalize_child_registry_entry(completion.child);
        crate::server_log!(
            notice,
            "Background saving {} (child {})",
            if exited_ok {
                "terminated with success"
            } else {
                "error"
            },
            pid
        );
    }

    pub(crate) fn release_forkless_clients(&mut self) {
        let tokens: Vec<_> = self.ctx.forkless_clients_mut().drain(..).collect();
        let mut dirty = Vec::new();
        for token in tokens {
            self.ctx.paused_clients_mut().insert(token);
            dirty.extend(self.wakeup_blocked_client(token, None, None));
        }
        if !dirty.is_empty() {
            self.wakeup_blocked_on_keys(&dirty, None, None);
        }
    }

    pub(crate) fn check_auto_bgsave(&mut self) {
        crate::framework::commands::server::save::check_auto_bgsave(&mut self.ctx);
    }

    pub(crate) fn check_auto_aof_rewrite(&mut self) {
        crate::framework::commands::server::bgrewriteaof::check_auto_aof_rewrite(&mut self.ctx);
    }

    pub(crate) fn check_scheduled_aof_rewrite(&mut self) {
        crate::framework::commands::server::bgrewriteaof::check_scheduled_aof_rewrite(
            &mut self.ctx,
        );
    }

    pub(crate) fn reap_aof_child(&mut self) {
        let Some(pid) = self.ctx.persistence().aof_rewrite_child_pid() else {
            return;
        };
        self.register_child_process(
            crate::runtime::server::worker::reactor::sources::ChildOwner::AofRewrite,
            pid,
        );
        let Some(completion) = self.take_child_completion_for_pid(pid) else {
            return;
        };
        let exited_ok = matches!(
            completion.outcome,
            crate::runtime::server::worker::reactor::sources::ChildExitOutcome::Exited(0)
        );
        let _ =
            crate::framework::commands::server::bgrewriteaof::apply_aof_rewrite_child_completion(
                &mut self.ctx,
                pid,
                exited_ok,
            );
        self.finalize_child_registry_entry(completion.child);
    }

    pub(crate) fn feed_aof(&mut self, frame: &[u8]) {
        let timestamp_sec = (crate::foundation::io::store::kv_engine::now_ms() / 1_000) as i64;
        if self.ctx.config().persistence.aof_timestamp_enabled
            && self
                .ctx
                .persistence_mut()
                .should_feed_aof_timestamp(timestamp_sec)
        {
            let annotation = format!("#TS:{}\r\n", timestamp_sec);
            self.ctx.persistence_mut().feed_aof(annotation.as_bytes());
        }
        self.ctx.persistence_mut().feed_aof(frame);
    }

    pub(crate) fn flush_aof_if_needed(&mut self) {
        #[cfg(test)]
        if self.ctx.config().persistence.aof_enabled {
            self.trace_turn_for_test(
                crate::runtime::server::worker::reactor::TurnTracePoint::AofFlush,
            );
        }
        let aof_enabled = self.ctx.config_mut().persistence.aof_enabled;

        if !aof_enabled {
            if let Some(mut aof) = self.ctx.aof_mut().journal.take() {
                let _ = aof.appender.shutdown();
                crate::server_log!(notice, "AOF turned off");
            }
            self.ctx.persistence_mut().reset_aof_selected_db();
            self.reject_blocked_waitaof_local_if_aof_disabled();
            return;
        }

        if self.ctx.aof_mut().journal.is_none() {
            if self.ctx.persistence().aof_rewrite_child_pid().is_some()
                || self.ctx.persistence().aof_rewrite_scheduled()
                || self.ctx.is_loading()
            {
                return;
            }
            let options =
                crate::engine::persistence::AofOptions::from_config(&self.ctx.config().persistence);
            let mode = if self.ctx.aof_mut().started_this_session {
                crate::foundation::io::persistence::AofOpenMode::AppendExisting
            } else {
                crate::foundation::io::persistence::AofOpenMode::CreateNew
            };
            match crate::foundation::io::persistence::AofAppender::open(
                options.dir.to_string_lossy().as_ref(),
                &options.filename,
                &options.appenddirname,
                mode,
                options.fsync,
            ) {
                Ok(mut appender) => {
                    self.ctx
                        .persistence_mut()
                        .record_aof_size(appender.current_size());
                    let _ = appender.force_fsync();
                    self.ctx.aof_mut().last_fsync_sec =
                        (crate::foundation::io::store::kv_engine::now_ms() / 1000) as i64;
                    self.ctx.aof_mut().last_write_status =
                        crate::engine::persistence::OperationStatus::Ok;
                    self.ctx.aof_mut().started_this_session = true;
                    self.ctx.persistence_mut().reset_aof_selected_db();
                    crate::server_log!(notice, "AOF turned on (fsync={})", options.fsync.as_str());
                    self.ctx.aof_mut().journal = Some(
                        crate::foundation::io::persistence::journal::AofJournal::new(appender),
                    );
                }
                Err(e) => {
                    crate::server_log!(warning, "Can't open AOF for append: {}", e);
                    self.ctx.config_mut().persistence.aof_enabled = false;
                    self.ctx.persistence_mut().reset_aof_selected_db();
                    return;
                }
            }
        }

        let configured_fsync = self.ctx.config().persistence.aof_fsync;
        // C aof.c skips both synchronous and everysec background fsync while
        // any persistence child is doing disk I/O. Switching the appender to
        // `no` stops its everysec worker as well as suppressing `always`.
        let effective_fsync = if self.ctx.config().persistence.aof_no_fsync_on_rewrite
            && self.ctx.persistence().has_active_child()
        {
            crate::foundation::io::persistence::AofFsync::No
        } else {
            configured_fsync
        };
        let aof = match self.ctx.aof_mut().journal.as_mut() {
            Some(a) => a,
            None => return,
        };
        // Keep the appender on the live configured mode except while the C
        // no-appendfsync-on-rewrite gate is active.
        if aof.appender.fsync_mode() != effective_fsync {
            if let Err(e) = aof.appender.set_fsync_mode(effective_fsync) {
                crate::server_log!(warning, "AOF fsync mode switch failed: {}", e);
            }
        }
        let fsync_mode = aof.appender.fsync_mode();
        // The replication offset that the data about to be flushed represents.
        // Mirrors C passing `server.primary_repl_offset` into the fsync path.
        let repl_offset = self.ctx.replication().primary_repl_offset().get();
        let aof = self.ctx.aof_mut().journal.as_mut().expect("checked above");
        // Record the offset the next successful fsync should report as durable.
        // Skipped while `no` (or the no-appendfsync-on-rewrite gate) is active so
        // the reported offset stays frozen, matching C's `aof_fsync != NO` guard.
        if fsync_mode != crate::foundation::io::persistence::AofFsync::No {
            aof.appender.set_fsync_target(repl_offset);
        }
        let aof_flush_start = std::time::Instant::now();
        let flush_result = aof.appender.flush(false);
        let aof_flush_duration_usec = aof_flush_start.elapsed().as_micros() as u64;
        let current_size = aof.appender.current_size();
        let reported_fsync_offset = aof.appender.fsynced_reported_offset();
        let _ = aof;
        self.ctx.record_eventloop_aof(aof_flush_duration_usec);

        match flush_result {
            Ok(()) => {
                self.ctx.persistence_mut().record_aof_size(current_size);
                self.ctx.aof_mut().last_write_status =
                    crate::engine::persistence::OperationStatus::Ok;
                if fsync_mode == crate::foundation::io::persistence::AofFsync::EverySec
                    || fsync_mode == crate::foundation::io::persistence::AofFsync::Always
                {
                    self.ctx.aof_mut().last_fsync_sec =
                        (crate::foundation::io::store::kv_engine::now_ms() / 1000) as i64;
                }
                // Advance the two-phase fsynced offset. `always` fsyncs inline,
                // so everything up to the flushed replication offset is durable
                // immediately. `everysec` adopts whatever the background fsync
                // thread last confirmed. `no` never reports, so the confirmed
                // offset stays frozen. The confirmed value is then promoted from
                // pending unless an initial rewrite is still outstanding.
                match fsync_mode {
                    crate::foundation::io::persistence::AofFsync::Always => {
                        self.ctx.aof_mut().set_fsynced_pending(repl_offset);
                    }
                    crate::foundation::io::persistence::AofFsync::EverySec => {
                        if let Some(reported) = reported_fsync_offset {
                            self.ctx.aof_mut().set_fsynced_pending(reported);
                        }
                    }
                    crate::foundation::io::persistence::AofFsync::No => {}
                }
                self.ctx.aof_mut().advance_fsynced_from_pending();
                self.check_wait_replica_wakeups();
            }
            Err(e) => {
                self.ctx.aof_mut().last_write_status =
                    crate::engine::persistence::OperationStatus::Err;
                crate::server_log!(warning, "AOF flush error: {}", e);
                let replica_action = replica_disk_write_error_action(
                    self.ctx.replication().is_replica(),
                    self.ctx
                        .config()
                        .replication
                        .replica_ignore_disk_write_errors,
                );
                if replica_action == ReplicaDiskWriteErrorAction::Fatal {
                    panic!("Replica was unable to write command to disk.");
                }
                if fsync_mode == crate::foundation::io::persistence::AofFsync::Always
                    && !self.ctx.replication().is_replica()
                {
                    crate::server_log!(
                        warning,
                        "AOF fsync always failed, aborting (server can't guarantee durability)."
                    );
                    std::process::exit(1);
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "write_tests.rs"]
mod tests;
