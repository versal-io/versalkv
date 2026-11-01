//! Streaming full-sync RDB receive path (closes G2r-2 Gap B).
//! While the replica link is not yet in the pure command-`Streaming` phase (i.e.
//! during the handshake and the full-sync RDB body), this path reads bounded
//! `IO_BUF_SIZE` chunks, forwards each framed body chunk to the link's
//! `RdbStreamDecoder`, and drives a paced decode inline. Classic receives may
//! continue to `WouldBlock`; the auxiliary dual-RDB TCP channel yields after one
//! chunk and relies on level readiness for the next turn. The `key-load-delay`
//! sleeps inside `drive` gate the read cadence to the decode rate: the kernel
//! socket buffer fills between chunks, TCP back-pressures the primary, and the
//! already-landed primary diskless relay gate engages. This makes the `Diskless
//! rdb transfer, done reading from pipe, N replicas still up.` count reflect a
//! slow replica drop. See ADR 2026-08-19-replica-streaming-rdb-load.
//! The pre-RDB handshake lines (`+FULLRESYNC`, `$<len>`/`$EOF:` header) arrive in
//! the same socket buffer immediately before the body, so this path also owns the
//! handshake. Only the pure post-RDB command stream (`is_streaming()`) is handled
//! by `recv_replication_stream`, whose drain loop applies commands without any
//! paced decode.

use super::super::super::ServerWorkerState;
use crate::engine::distributed::replication::backlog::ReplicationBacklog;
use crate::foundation::io::persistence::{RdbStep, StreamStep};
use crate::framework::capabilities::io::Token;
use crate::framework::distributed::replication::{
    ConnectionId, LinkError, MasterReconnectCause, ReplicaAction, ReplicaEvent, ReplicaLink,
    ReplicaRdbChannelState, ReplicationConnection,
};
use crate::runtime::server::worker::reactor::blocked_events::{BlockedOperation, ResumeDecision};
use std::io::Read;

const RDB_CHANNEL_READ_CHUNKS_PER_DISPATCH: usize = 1;

#[derive(Clone, Copy, PartialEq, Eq)]
enum RdbChannelReadMode {
    Fair,
    DrainAfterHup,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReplicationStreamReadMode {
    Fair,
    DrainAfterHup,
}

enum BoundedRdbRead {
    Data(Vec<u8>),
    WouldBlock,
    Closed,
    MissingTransport,
}

struct ClassicChunkContext<'a> {
    token_value: usize,
    connection_token: usize,
    listening_port: u16,
    replica_announce_ip: &'a str,
}

struct ClassicChunkResult {
    link: ReplicaLink,
    events: Vec<ReplicaEvent>,
    actions: Vec<ReplicaAction>,
    rdb_was_active: bool,
}

struct DecodedReplicaChunk {
    events: Vec<ReplicaEvent>,
    actions: Vec<ReplicaAction>,
    payload_delta: usize,
}

struct ClassicDispatchResult {
    link: ReplicaLink,
    events: Vec<ReplicaEvent>,
    actions: Vec<ReplicaAction>,
    peer_closed: bool,
}

struct DualChunkContext {
    main: ConnectionId,
    rdb_token_value: usize,
    rdb_connection_token: usize,
}

fn disk_backed_transfer_is_pending(link: &ReplicaLink) -> bool {
    if link.rdb_load_mode()
        != Some(crate::framework::distributed::replication::RdbLoadMode::DiskBacked)
    {
        return false;
    }
    if link.is_dual_rdb_mode_active() {
        !link.rdb_framing_is_complete()
    } else {
        !link.is_streaming()
    }
}

fn rdb_phase_matches_operation(link: &ReplicaLink, operation: BlockedOperation) -> bool {
    match operation {
        BlockedOperation::ClassicRdb { .. } => {
            link.has_pending_rdb_receive() && !link.is_dual_rdb_mode_active()
        }
        BlockedOperation::DualRdb { .. } => {
            link.has_pending_rdb_receive()
                && link.rdb_channel_state() == ReplicaRdbChannelState::ReceivingRdb
        }
        BlockedOperation::PendingReplay { .. } => false,
    }
}

fn blocked_operation_main(operation: BlockedOperation) -> ConnectionId {
    match operation {
        BlockedOperation::ClassicRdb { main }
        | BlockedOperation::DualRdb { main, .. }
        | BlockedOperation::PendingReplay { main } => main,
    }
}

fn blocked_operation_rdb(operation: BlockedOperation) -> Option<ConnectionId> {
    match operation {
        BlockedOperation::DualRdb { rdb, .. } => Some(rdb),
        BlockedOperation::ClassicRdb { .. } | BlockedOperation::PendingReplay { .. } => None,
    }
}

impl ServerWorkerState {
    /// Read bounded chunks and drive the paced decode after each one until
    /// `WouldBlock` or a runtime fairness boundary. Buffered protocol input left
    /// by a busy script is resumed by the queue-free before-sleep pending phase;
    /// unread TCP bytes remain visible through level readiness. The inline
    /// `key-load-delay` sleep still paces consumption, so the socket fills, TCP
    /// back-pressures the primary, and the diskless relay gate engages (G2r-2 B).
    pub(crate) fn recv_streaming_rdb_chunk(&mut self, token_value: usize, connection_token: usize) {
        let listening_port = self
            .ctx
            .config()
            .replication
            .effective_replica_announce_port(self.ctx.config().server.listening_port);
        let replica_announce_ip = self.ctx.config().replication.replica_announce_ip.clone();
        let Some(mut link) = self.replication_runtime.take_replica_link() else {
            return;
        };
        self.classic_rdb_loading_progress
            .begin_session(link.session_id());
        let mut all_events = Vec::new();
        let mut all_actions = Vec::new();
        let mut peer_closed = false;
        let mut process_buffered_input = link.has_buffered_input();
        loop {
            let read_data = if process_buffered_input {
                process_buffered_input = false;
                Vec::new()
            } else {
                let token = self.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)]
                .token();
                match self.read_bounded_rdb_chunk(token) {
                    BoundedRdbRead::Data(data) => data,
                    BoundedRdbRead::Closed => {
                        peer_closed = true;
                        break;
                    }
                    BoundedRdbRead::WouldBlock => break,
                    BoundedRdbRead::MissingTransport => {
                        self.replication_runtime.set_replica_link(link);
                        return;
                    }
                }
            };
            let context = ClassicChunkContext {
                token_value,
                connection_token,
                listening_port,
                replica_announce_ip: &replica_announce_ip,
            };
            if link.is_dual_rdb_mode_active()
                && !all_events.is_empty()
                && self
                    .buffer_replica_events_into_pending_inner(
                        connection_token,
                        &mut link,
                        std::mem::take(&mut all_events),
                        false,
                    )
                    .is_err()
            {
                self.replication_runtime.set_replica_link(link);
                self.abort_replica_read(token_value);
                return;
            }
            let Some(chunk) = self.process_classic_rdb_chunk(link, &context, &read_data) else {
                return;
            };
            link = chunk.link;
            all_events.extend(chunk.events);
            all_actions.extend(chunk.actions);
            if self.ctx.is_script_busy() && link.is_receiving_rdb() {
                break;
            }
            if self.classic_rdb_read_should_yield(&link, chunk.rdb_was_active) {
                break;
            }
        }
        self.finish_classic_rdb_dispatch(
            token_value,
            connection_token,
            ClassicDispatchResult {
                link,
                events: all_events,
                actions: all_actions,
                peer_closed,
            },
        );
    }

    fn read_bounded_rdb_chunk(&mut self, token: Token) -> BoundedRdbRead {
        let interval = self
            .ctx
            .config()
            .persistence
            .loading_process_events_interval_bytes;
        let budget = self
            .classic_rdb_loading_progress
            .read_budget(self.io_buf.len(), interval);
        let Some(mut stream) = self.transport.stream(token) else {
            return BoundedRdbRead::MissingTransport;
        };
        match crate::foundation::io::sys::os::retry_on_interrupt(|| {
            stream.read(&mut self.io_buf[..budget])
        }) {
            Ok(0) => BoundedRdbRead::Closed,
            Ok(n) => BoundedRdbRead::Data(self.io_buf[..n].to_vec()),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                BoundedRdbRead::WouldBlock
            }
            Err(_) => BoundedRdbRead::Closed,
        }
    }

    fn decode_classic_rdb_chunk(
        &mut self,
        link: &mut ReplicaLink,
        context: &ClassicChunkContext<'_>,
        read_data: &[u8],
    ) -> Result<DecodedReplicaChunk, LinkError> {
        self.pin_replica_rdb_load_mode(link);
        let payload_before = link.rdb_payload_bytes();
        let decoded = if self.ctx.is_script_busy() {
            link.on_bytes_while_script_blocked(
                read_data,
                context.listening_port,
                context.replica_announce_ip,
            )
        } else {
            link.on_bytes_with_announce_ip(
                read_data,
                context.listening_port,
                context.replica_announce_ip,
            )
        };
        self.account_replica_wire_input(link);
        let (events, actions) = decoded?;
        self.mark_replica_transfer_started(link);
        let payload_delta = link
            .rdb_payload_bytes()
            .saturating_sub(payload_before)
            .min(usize::MAX as u64) as usize;
        self.apply_replica_handshake_transitions(link)?;
        Ok(DecodedReplicaChunk {
            events,
            actions,
            payload_delta,
        })
    }

    fn reject_classic_rdb_chunk(
        &mut self,
        token_value: usize,
        link: ReplicaLink,
        error: LinkError,
        read_data: &[u8],
    ) {
        crate::server_log!(
            warning,
            "dual-rdb main decode rejected: token={} rdb_state={:?} error={:?} bytes={:?}",
            token_value,
            link.rdb_channel_state(),
            error,
            String::from_utf8_lossy(read_data)
        );
        let main = link.connection_id();
        let rdb = self.replication_runtime.rdb_channel_for_main(main);
        self.replication_runtime.set_replica_link(link);
        if let Some(rdb) = rdb {
            let _ = self.replication_runtime.set_replica_rdb_link(rdb, main);
        }
        self.abort_master_link_on_error(token_value, error);
    }

    fn process_classic_rdb_chunk(
        &mut self,
        mut link: ReplicaLink,
        context: &ClassicChunkContext<'_>,
        read_data: &[u8],
    ) -> Option<ClassicChunkResult> {
        let mut decoded = match self.decode_classic_rdb_chunk(&mut link, context, read_data) {
            Ok(decoded) => decoded,
            Err(error) => {
                self.reject_classic_rdb_chunk(context.token_value, link, error, read_data);
                return None;
            }
        };
        // RDB commit may read newer main-channel frames during handoff.
        // Publish this older batch first without recursively entering handoff.
        if link.is_dual_rdb_mode_active()
            && !decoded.events.is_empty()
            && self
                .buffer_replica_events_into_pending_inner(
                    context.connection_token,
                    &mut link,
                    std::mem::take(&mut decoded.events),
                    false,
                )
                .is_err()
        {
            self.replication_runtime.set_replica_link(link);
            self.abort_replica_read(context.token_value);
            return None;
        }
        if link.has_received_rdb_payload() {
            self.apply_streaming_loading_gate(&link);
        }
        let rdb_was_active = link.has_pending_rdb_receive() && !link.is_dual_rdb_mode_active();
        let main = link.connection_id();
        link = match self.service_rdb_progress_boundary(
            link,
            BlockedOperation::ClassicRdb { main },
            decoded.payload_delta,
        ) {
            ResumeDecision::Resume(link) => link,
            ResumeDecision::Aborted => return None,
        };
        link = match self.drive_replica_rdb_decode(
            link,
            context.connection_token,
            BlockedOperation::ClassicRdb { main },
        ) {
            ResumeDecision::Resume(link) => link,
            ResumeDecision::Aborted => return None,
        };
        Some(ClassicChunkResult {
            link,
            events: decoded.events,
            actions: decoded.actions,
            rdb_was_active,
        })
    }

    fn service_rdb_progress_boundary(
        &mut self,
        link: ReplicaLink,
        operation: BlockedOperation,
        payload_delta: usize,
    ) -> ResumeDecision<ReplicaLink> {
        if !rdb_phase_matches_operation(&link, operation) {
            return ResumeDecision::Resume(link);
        }
        let interval = self
            .ctx
            .config()
            .persistence
            .loading_process_events_interval_bytes;
        if !self
            .classic_rdb_loading_progress
            .record_payload(payload_delta, interval)
        {
            return ResumeDecision::Resume(link);
        }
        let main = blocked_operation_main(operation);
        let rdb = blocked_operation_rdb(operation);
        let session_id = link.session_id();
        self.replication_runtime.set_replica_link(link);
        self.queue_replica_loading_newline(main);
        let progress = crate::engine::host::LoadProgress {
            loaded_bytes: self.classic_rdb_loading_progress.payload_bytes(),
            total_bytes: None,
            loaded_keys: 0,
        };
        let control = self.with_host_dispatcher(
            crate::runtime::server::worker::tasks::hosted::HostTaskScope::blocked(operation),
            |dispatcher| crate::engine::host::yield_rdb(dispatcher, progress, None),
        );
        if !matches!(control, Ok(crate::engine::host::HostControl::Continue)) {
            return ResumeDecision::Aborted;
        }
        if self.ctx.is_shutdown_requested() {
            return ResumeDecision::Aborted;
        }
        match self.retake_matching_rdb_session(main, rdb, session_id) {
            Some(link) => ResumeDecision::Resume(link),
            None => ResumeDecision::Aborted,
        }
    }

    fn classic_rdb_read_should_yield(&self, link: &ReplicaLink, rdb_was_active: bool) -> bool {
        matches!(link.rdb_channel_state(), ReplicaRdbChannelState::RdbLoaded)
            || (rdb_was_active
                && link.is_streaming()
                && !link.is_receiving_rdb()
                && !link.has_pending_rdb_receive())
    }

    fn apply_classic_rdb_events(
        &mut self,
        connection_token: usize,
        link: &mut ReplicaLink,
        events: Vec<ReplicaEvent>,
    ) -> Result<(), ()> {
        let paused = self
            .ctx
            .client_pause()
            .is_paused(crate::foundation::io::sys::time::mstime())
            != crate::framework::context::PauseAction::None;
        if paused && link.is_streaming() {
            if events.is_empty() && link.is_dual_rdb_mode_active() {
                self.buffer_replica_events_into_pending(connection_token, link, events)
            } else {
                self.defer_replica_events(connection_token, link, events)
            }
        } else if link.is_dual_rdb_mode_active() {
            self.buffer_replica_events_into_pending(connection_token, link, events)
        } else {
            self.apply_replica_events(connection_token, link, events)
        }
    }

    fn abort_replica_read(&mut self, token_value: usize) {
        self.ctx.replication_mut().async_loading = false;
        self.ctx.set_loading(false);
        self.ctx.config_mut().replication.master_link_status = "down".to_string();
        self.remove_connection(token_value);
    }

    fn finish_classic_rdb_dispatch(
        &mut self,
        token_value: usize,
        connection_token: usize,
        mut result: ClassicDispatchResult,
    ) {
        self.ctx.replication_mut().slave_read_repl_offset = result.link.received_position();
        let applied =
            self.apply_classic_rdb_events(connection_token, &mut result.link, result.events);
        let handoff_failed = result.link.is_streaming()
            && result.link.rdb_channel_state() == ReplicaRdbChannelState::RdbLoaded
            && self
                .commit_and_replay_dual_handoff_only(connection_token, &mut result.link)
                .is_err();
        if handoff_failed || applied.is_err() {
            self.abort_replica_read(token_value);
            return;
        }
        let applied_offset = result.link.applied_position();
        let is_streaming = result.link.is_streaming();
        let is_loading =
            result.link.is_dual_rdb_mode_active() || result.link.has_pending_rdb_receive();
        self.replication_runtime.set_replica_link(result.link);
        self.queue_replica_actions(connection_token, result.actions);
        self.ctx.replication_mut().slave_repl_offset = applied_offset;
        if is_streaming && !is_loading {
            self.ctx.config_mut().replication.master_link_status = "up".to_string();
            self.ctx.replication_mut().mark_failover_complete();
            self.replication_runtime.reset_master_reconnect_backoff();
        }
        if result.peer_closed {
            if is_loading {
                crate::server_log!(
                    warning,
                    "RDB stream ended before the announced transfer size"
                );
            }
            if !is_streaming {
                self.ctx.config_mut().replication.master_link_status = "down".to_string();
            }
            self.remove_connection(token_value);
        }
    }

    /// Disk-backed transfers keep serving the old dataset until all RDB framing
    /// is received, matching C's background file download before disk loading.
    /// Socket loading starts with the first payload; `swapdb` with the same
    /// primary replid keeps the old dataset readable through `async_loading`.
    fn apply_streaming_loading_gate(&mut self, link: &ReplicaLink) {
        if disk_backed_transfer_is_pending(link) {
            return;
        }
        let swapdb = link.rdb_load_mode()
            == Some(crate::framework::distributed::replication::RdbLoadMode::Swap);
        let same_replid = link
            .master_replid()
            .map(|m| m == self.ctx.replication().backlog.replid())
            .unwrap_or(false);
        if swapdb && same_replid {
            self.ctx.replication_mut().async_loading = true;
            self.ctx.set_loading(false);
        } else {
            self.ctx.set_loading(true);
            self.ctx.replication_mut().async_loading = false;
            if let Some(cluster) = self.ctx.cluster_state_mut().as_deref_mut() {
                // C has no live primary client while a replacement RDB is
                // loading, so replicationGetReplicaOffset() returns zero.
                // Publish that transition immediately: cluster traffic is
                // serviced inside the paced loader before the regular cron
                // has another chance to refresh this cached bus field.
                cluster.set_local_replication_offset(
                    crate::engine::distributed::replication::ReplicationOffset::try_from(0)
                        .expect("zero is a valid replication offset"),
                );
            }
        }
    }

    fn mark_replica_transfer_started(&mut self, link: &ReplicaLink) {
        if link.has_pending_rdb_receive()
            && self.ctx.config().replication.master_link_status != "up"
        {
            self.ctx.config_mut().replication.master_link_status = "sync".to_string();
        }
    }

    fn log_disk_backed_transfer_started(&self, link: &mut ReplicaLink) {
        if link.rdb_load_mode()
            == Some(crate::framework::distributed::replication::RdbLoadMode::DiskBacked)
            && link
                .rdb_receive_mut()
                .is_some_and(|receive| receive.mark_transfer_started())
        {
            crate::server_log!(
                notice,
                "PRIMARY <-> REPLICA sync: receiving streamed RDB from primary for persistence to disk (buffering in memory; delayed installation after transfer completes)"
            );
        }
    }

    /// File-style downloads are complete before decoding starts, so socket
    /// reads no longer provide event-service boundaries. Yield on consumed RDB
    /// bytes while retaining the same protected operation and session checks.
    fn drive_replica_rdb_decode(
        &mut self,
        mut link: ReplicaLink,
        connection_token: usize,
        operation: BlockedOperation,
    ) -> ResumeDecision<ReplicaLink> {
        let main = blocked_operation_main(operation);
        self.log_disk_backed_transfer_started(&mut link);
        let can_decode_file = link.rdb_load_mode()
            == Some(crate::framework::distributed::replication::RdbLoadMode::DiskBacked)
            && !disk_backed_transfer_is_pending(&link)
            && link.has_received_rdb_payload();
        if can_decode_file {
            let is_uninit = link.rdb_receive_mut().is_some_and(|recv| recv.is_uninit());
            if self.init_streaming_target(&mut link).is_err() {
                self.replication_runtime.set_replica_link(link);
                self.tear_down_master_link(main.0);
                return ResumeDecision::Aborted;
            }
            if is_uninit {
                // Decode progress must not inherit bytes counted while the
                // complete snapshot was being downloaded.
                self.classic_rdb_loading_progress.reset();
                self.classic_rdb_loading_progress
                    .begin_session(link.session_id());
            }
            if let Some(receive) = link.rdb_receive_mut() {
                receive.finish_input();
                receive.begin_decode_turn();
            }
            loop {
                let key_load_delay_us = self.ctx.config().persistence.key_load_delay;
                let Some(recv) = link.rdb_receive_mut() else {
                    break;
                };
                let before = recv.consumed_bytes();
                let step = recv.step(self.ctx.dbs_mut(), key_load_delay_us);
                let consumed_delta = recv.consumed_bytes().saturating_sub(before);
                match step {
                    Ok(RdbStep::NeedInput) => return ResumeDecision::Resume(link),
                    Ok(RdbStep::Finished(_)) => break,
                    Ok(RdbStep::RecordApplied) => {
                        link = match self.service_rdb_progress_boundary(
                            link,
                            operation,
                            consumed_delta,
                        ) {
                            ResumeDecision::Resume(link) => link,
                            ResumeDecision::Aborted => return ResumeDecision::Aborted,
                        };
                    }
                    Err(error) => {
                        super::replica_apply::log_rdb_decode_error(&error);
                        self.replication_runtime.set_replica_link(link);
                        self.tear_down_master_link(main.0);
                        return ResumeDecision::Aborted;
                    }
                }
            }
        }
        // Socket modes still decode each bounded read. A completed file load
        // reaches this driver only for its existing terminal commit.
        if self
            .drive_streaming_decode(main.0, connection_token, &mut link)
            .is_err()
        {
            self.replication_runtime.set_replica_link(link);
            self.tear_down_master_link(main.0);
            return ResumeDecision::Aborted;
        }
        ResumeDecision::Resume(link)
    }

    /// Drive the paced decode against the target `DbSet`. On the first drive it
    /// selects the target (swapdb temp vs. flushed serving DbSet) and logs the
    /// "Loading DB in memory" notice. On `Finished` it runs the terminal commit;
    /// on a decode error it logs and tears the link down. Returns `Err(())` when
    /// the link was consumed after teardown so the caller stops.
    #[cfg_attr(test, allow(dead_code))]
    pub(crate) fn drive_streaming_decode(
        &mut self,
        _token_value: usize,
        connection_token: usize,
        link: &mut ReplicaLink,
    ) -> Result<(), ()> {
        let is_dual_mode = matches!(
            link.rdb_channel_state(),
            ReplicaRdbChannelState::ReceivingRdb | ReplicaRdbChannelState::RdbLoaded
        );
        let framing_owned_by_rdb = link.rdb_framing_is_pending() || link.rdb_framing_is_complete();

        // No active RDB phase (handshake-only or partial resync): nothing to do.
        if link.rdb_receive_mut().is_none() || (is_dual_mode && !framing_owned_by_rdb) {
            return Ok(());
        }
        self.log_disk_backed_transfer_started(link);
        if !link.has_received_rdb_payload() || disk_backed_transfer_is_pending(link) {
            return Ok(());
        }
        // Socket loads start on the first payload. Disk-backed loads wait for
        // the complete transfer before decoding or applying key-load-delay.
        self.init_streaming_target(link).map_err(|_| ())?;
        let input_complete =
            !link.is_receiving_rdb() && (!is_dual_mode || link.rdb_framing_is_complete());
        let key_load_delay_us = self.ctx.config().persistence.key_load_delay;
        let step = {
            let Some(recv) = link.rdb_receive_mut() else {
                return Ok(());
            };
            if input_complete {
                recv.finish_input();
            }
            recv.drive(self.ctx.dbs_mut(), key_load_delay_us)
        };
        match step {
            Ok(StreamStep::NeedMore) => Ok(()),
            Ok(StreamStep::Finished) => {
                if link.is_receiving_rdb() || (is_dual_mode && !link.rdb_framing_is_complete()) {
                    return Ok(());
                }
                if let Some(receive) = link.rdb_receive_mut() {
                    receive.validate_complete().map_err(|error| {
                        super::replica_apply::log_rdb_decode_error(&error);
                    })?;
                }
                if is_dual_mode {
                    let loaded_keys = link
                        .rdb_receive_mut()
                        .map(|recv| recv.loaded())
                        .unwrap_or(0);
                    crate::server_log!(
                        debug,
                        "dual-rdb decoder reached RDB EOF: main={} loaded_keys={} framing_complete={}",
                        link.connection_id().0,
                        loaded_keys,
                        link.rdb_framing_is_complete()
                    );
                    let recv = link.take_rdb_receive();
                    let Some(recv) = recv else {
                        return Ok(());
                    };
                    let commit = recv.finish();
                    if self
                        .commit_streamed_rdb(connection_token, link, commit)
                        .is_err()
                    {
                        return Err(());
                    }
                    crate::server_log!(notice, "Done loading RDB");
                    return Ok(());
                }
                let Some(recv) = link.take_rdb_receive() else {
                    return Ok(());
                };
                let commit = recv.finish();
                if self
                    .commit_streamed_rdb(connection_token, link, commit)
                    .is_err()
                {
                    return Err(());
                }
                Ok(())
            }
            Err(e) => {
                super::replica_apply::log_rdb_decode_error(&e);
                Err(())
            }
        }
    }

    /// Select the decode target on the first driveable chunk, mirroring the old
    /// buffered path's swapdb-vs-flush split. `swapdb` decodes into a fresh DbSet
    /// swapped in atomically at commit; the default flushes the serving DbSet once
    /// and decodes straight into it. Emits the "Loading DB in memory" notice at
    /// this transition (replication.c:2510), before any record is applied.
    fn init_streaming_target(&mut self, link: &mut ReplicaLink) -> Result<(), LinkError> {
        match link.rdb_receive_mut() {
            Some(recv) if recv.is_uninit() => {}
            _ => return Ok(()),
        }
        if let Some(recv) = link.rdb_receive_mut() {
            recv.set_zset_limits(
                self.ctx.config().encoding.zset_max_ziplist_entries as usize,
                self.ctx.config().encoding.zset_max_ziplist_value as usize,
            );
        }
        let load_mode = link
            .rdb_load_mode()
            .unwrap_or(crate::framework::distributed::replication::RdbLoadMode::Direct);
        let swapdb = load_mode == crate::framework::distributed::replication::RdbLoadMode::Swap;
        let disk_backed =
            load_mode == crate::framework::distributed::replication::RdbLoadMode::DiskBacked;
        if !swapdb && !disk_backed {
            // C attaches a replica to the new primary before loading a
            // non-SWAPDB RDB: downstream replicas are disconnected and the
            // old replication backlog is discarded before the new dataset is
            // installed.
            if let Some(replid) = link.master_replid() {
                let snapshot_position = link.prevalidate_rdb_apply_position()?;
                let boundary = ReplicationBacklog::rdb_snapshot_boundary(snapshot_position)
                    .map_err(|_| LinkError::Protocol)?;
                self.ctx
                    .replication_mut()
                    .backlog
                    .adopt_primary_from_rdb_boundary(replid, boundary);
            } else {
                self.ctx.replication_mut().backlog.clear_history();
            }
            self.replication_runtime.discard_replica_resume();
            self.reset_downstream_after_full_sync(link.connection_id());
        }
        self.ctx.config_mut().replication.master_link_status = "sync".to_string();
        crate::server_log!(notice, "PRIMARY <-> REPLICA sync: Loading DB in memory");
        if swapdb || disk_backed {
            let temp = self.ctx.dbs().empty_like();
            if let Some(recv) = link.rdb_receive_mut() {
                recv.set_swap(temp);
            }
        } else {
            if self.ctx.config().replication.replica_lazy_flush {
                let values = self.ctx.drain_all_db_values_for_lazyfree();
                self.dispose_data_lazyfree_objects(values);
            } else {
                self.ctx.flush_all_dbs();
            }
            if let Some(recv) = link.rdb_receive_mut() {
                recv.set_direct();
            }
        }
        Ok(())
    }

    /// Shared teardown for a handshake/framing error on the master link, mirroring
    /// the error arm of the classic `recv_replication_stream`.
    pub(super) fn abort_master_link_on_error(&mut self, token_value: usize, error: LinkError) {
        if error == LinkError::FailoverRejected || self.ctx.replication().is_failover_in_progress()
        {
            self.abort_rejected_failover(token_value);
            return;
        }
        if error == LinkError::Authentication {
            crate::server_log!(warning, "Unable to AUTH to PRIMARY");
            self.replication_runtime
                .reject_authentication(self.current_authentication_attempt());
        }
        let retry_later = error == LinkError::TryLater;
        self.tear_down_master_link(token_value);
        if retry_later {
            self.schedule_master_reconnect(MasterReconnectCause::TryLater);
        }
    }

    fn abort_rejected_failover(&mut self, token_value: usize) {
        crate::server_log!(warning, "Failover target rejected psync request");
        let downstream: Vec<_> = self
            .replication_runtime
            .primary_links()
            .values()
            .map(|link| link.connection_id().0)
            .collect();
        self.tear_down_master_link(token_value);
        self.replication_runtime.clear_replica_resume();
        self.ctx.config_mut().replication.master_host = None;
        self.ctx.config_mut().replication.master_port = 0;
        self.ctx.config_mut().replication.master_link_status = "down".to_string();
        self.ctx.replication_mut().switch_to_primary();
        self.ctx.replication_mut().repl_seldb = -1;
        for token in downstream {
            self.remove_connection(token);
        }
        self.clear_pending_failover();
    }

    /// Clear the loading flags, mark the link down, and drop the connection. The
    /// reconnect/backoff scheduled by `remove_connection` drives a fresh full
    /// sync.
    pub(crate) fn tear_down_master_link(&mut self, token_value: usize) {
        crate::server_log!(
            debug,
            "dual-rdb master teardown: token={} role={:?} paired_rdb={:?}",
            token_value,
            self.replication_runtime.role(ConnectionId(token_value)),
            self.replication_runtime
                .channel_pair_for(ConnectionId(token_value))
                .map(|connection_id| connection_id.0)
        );
        let paired_rdb_token = self
            .replication_runtime
            .channel_pair_for(ConnectionId(token_value))
            .map(|connection_id| connection_id.0);
        let replica_rdb_token = self
            .replication_runtime
            .take_replica_rdb_link()
            .map(|link| link.connection_id().0);
        self.take_and_dispose_replica_pending_buffer();
        self.ctx.replication_mut().async_loading = false;
        self.ctx.set_loading(false);
        self.classic_rdb_loading_progress.reset();
        self.ctx.config_mut().replication.master_link_status = "down".to_string();
        self.remove_connection(token_value);
        if let Some(rdb_token) = paired_rdb_token.or(replica_rdb_token) {
            self.remove_connection(rdb_token);
        }
    }

    /// Classic drain-to-`WouldBlock` read of the pure command stream (post-RDB
    /// `Streaming` phase). Applies decoded commands, honoring client-pause defer.
    /// Only reached once the RDB phase has committed, so it never sees a
    /// half-loaded RDB; the bounded path above owns the handshake + RDB body.
    pub(crate) fn recv_replication_stream(&mut self, token_value: usize, connection_token: usize) {
        self.recv_replication_stream_with_mode(
            token_value,
            connection_token,
            ReplicationStreamReadMode::Fair,
        );
    }

    pub(crate) fn recv_replication_stream_after_hup(
        &mut self,
        token_value: usize,
        connection_token: usize,
    ) {
        self.recv_replication_stream_with_mode(
            token_value,
            connection_token,
            ReplicationStreamReadMode::DrainAfterHup,
        );
    }

    fn recv_replication_stream_with_mode(
        &mut self,
        token_value: usize,
        connection_token: usize,
        mode: ReplicationStreamReadMode,
    ) {
        let mut read_data = Vec::new();
        let close = {
            let Some(conn) = self
                .connection_registry
                .get(crate::framework::capabilities::io::Token(connection_token))
            else {
                return;
            };
            let mut stream = match self.transport.stream(conn.token()) {
                Some(s) => s,
                None => return,
            };
            match mode {
                ReplicationStreamReadMode::Fair => {
                    crate::foundation::io::sys::os::drain_nonblocking_bounded(
                        &mut self.io_buf,
                        &mut read_data,
                        crate::runtime::server::worker::NORMAL_CLIENT_READ_BUDGET,
                        |buf| stream.read(buf),
                    )
                }
                ReplicationStreamReadMode::DrainAfterHup => {
                    crate::foundation::io::sys::os::drain_nonblocking(
                        &mut self.io_buf,
                        &mut read_data,
                        |buf| stream.read(buf),
                    )
                }
            }
            .unwrap_or(true)
        };
        if close && read_data.is_empty() {
            self.remove_connection(token_value);
            self.ctx.config_mut().replication.master_link_status = "down".to_string();
            return;
        }
        if read_data.is_empty()
            && !self
                .replication_runtime
                .replica_link()
                .is_some_and(|link| link.has_pending_decompression())
        {
            return;
        }

        let listening_port = self
            .ctx
            .config()
            .replication
            .effective_replica_announce_port(self.ctx.config().server.listening_port);
        let replica_announce_ip = self.ctx.config().replication.replica_announce_ip.clone();
        let Some(mut link) = self.replication_runtime.take_replica_link() else {
            return;
        };
        let decoded =
            link.on_bytes_with_announce_ip(&read_data, listening_port, &replica_announce_ip);
        self.account_replica_wire_input(&mut link);
        let (events, actions) = match decoded {
            Ok(decoded) => decoded,
            Err(error) => {
                crate::server_log!(
                    warning,
                    "dual-rdb stream decode rejected: token={} rdb_state={:?} error={:?} bytes={:?}",
                    token_value,
                    link.rdb_channel_state(),
                    error,
                    String::from_utf8_lossy(&read_data)
                );
                let main_connection_id = link.connection_id();
                let rdb_connection_id = self
                    .replication_runtime
                    .rdb_channel_for_main(main_connection_id);
                self.replication_runtime.set_replica_link(link);
                if let Some(rdb_connection_id) = rdb_connection_id {
                    let _ = self
                        .replication_runtime
                        .set_replica_rdb_link(rdb_connection_id, main_connection_id);
                }
                self.abort_master_link_on_error(token_value, error);
                return;
            }
        };
        if let Err(error) = self.apply_replica_handshake_transitions(&mut link) {
            let main_connection_id = link.connection_id();
            let rdb_connection_id = self
                .replication_runtime
                .rdb_channel_for_main(main_connection_id);
            self.replication_runtime.set_replica_link(link);
            if let Some(rdb_connection_id) = rdb_connection_id {
                let _ = self
                    .replication_runtime
                    .set_replica_rdb_link(rdb_connection_id, main_connection_id);
            }
            self.abort_master_link_on_error(token_value, error);
            return;
        }
        self.ctx.replication_mut().slave_read_repl_offset = link.received_position();
        let paused = self
            .ctx
            .client_pause()
            .is_paused(crate::foundation::io::sys::time::mstime())
            != crate::framework::context::PauseAction::None;
        let apply_result =
            if (paused || self.ctx.module_registry().busy_reply.is_some()) && link.is_streaming() {
                if events.is_empty() && link.is_dual_rdb_mode_active() {
                    self.buffer_replica_events_into_pending(connection_token, &mut link, events)
                } else {
                    self.defer_replica_events(connection_token, &mut link, events)
                }
            } else if link.is_dual_rdb_mode_active() {
                self.buffer_replica_events_into_pending(connection_token, &mut link, events)
            } else {
                self.apply_replica_events(connection_token, &mut link, events)
            };
        if apply_result.is_err() {
            self.ctx.replication_mut().async_loading = false;
            self.ctx.set_loading(false);
            self.ctx.config_mut().replication.master_link_status = "down".to_string();
            self.remove_connection(token_value);
            return;
        }
        let applied_offset = link.applied_position();
        let is_streaming = link.is_streaming();
        let dual_join_pending = link.is_dual_rdb_mode_active();
        self.replication_runtime.set_replica_link(link);
        self.queue_replica_actions(connection_token, actions);
        self.ctx.replication_mut().slave_repl_offset = applied_offset;
        if is_streaming && !dual_join_pending {
            self.ctx.config_mut().replication.master_link_status = "up".to_string();
            self.ctx.replication_mut().mark_failover_complete();
            self.replication_runtime.reset_master_reconnect_backoff();
        }
        if close {
            self.remove_connection(token_value);
        }
    }

    fn reject_dual_rdb_chunk(
        &mut self,
        context: &DualChunkContext,
        link: ReplicaLink,
        error: LinkError,
        read_data: &[u8],
    ) {
        crate::server_log!(
            warning,
            "dual RDB channel framing error: {:?}, state={:?}, bytes={:?}",
            error,
            link.rdb_channel_state(),
            String::from_utf8_lossy(read_data)
        );
        self.replication_runtime.set_replica_link(link);
        self.tear_down_master_link(context.main.0);
    }

    fn queue_dual_rdb_actions(&mut self, main: ConnectionId, actions: Vec<ReplicaAction>) -> bool {
        if actions.is_empty() {
            return true;
        }
        // C sends set-rdb-client-id immediately after ENDOFF; delaying these
        // actions until WouldBlock can postpone main PSYNC behind a large RDB.
        let Some(main_connection_token) = self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(main.0))
            .then_some(main.0)
        else {
            return false;
        };
        self.queue_replica_actions(main_connection_token, actions);
        true
    }

    fn process_dual_rdb_chunk(
        &mut self,
        mut link: ReplicaLink,
        context: &DualChunkContext,
        read_data: &[u8],
    ) -> Option<ReplicaLink> {
        self.pin_replica_rdb_load_mode(&mut link);
        let payload_before = link.rdb_payload_bytes();
        let previous_state = link.rdb_channel_state();
        let decoded = link.on_rdb_bytes(read_data);
        self.account_replica_wire_input(&mut link);
        let actions = match decoded {
            Ok((_events, actions)) => actions,
            Err(error) => {
                self.reject_dual_rdb_chunk(context, link, error, read_data);
                return None;
            }
        };
        self.send_dual_rdb_sync_after_handshake(
            context.rdb_connection_token,
            previous_state,
            &link,
        );
        self.mark_replica_transfer_started(&link);
        let payload_delta = link.rdb_payload_bytes().saturating_sub(payload_before);
        let payload_delta = payload_delta.min(usize::MAX as u64) as usize;
        if !self.queue_dual_rdb_actions(context.main, actions) {
            self.replication_runtime.set_replica_link(link);
            return None;
        }
        if link.has_received_rdb_payload() {
            self.apply_streaming_loading_gate(&link);
        }
        let operation = BlockedOperation::DualRdb {
            main: context.main,
            rdb: ConnectionId(context.rdb_token_value),
        };
        link = match self.service_rdb_progress_boundary(link, operation, payload_delta) {
            ResumeDecision::Resume(link) => link,
            ResumeDecision::Aborted => return None,
        };
        match self.drive_replica_rdb_decode(link, context.rdb_connection_token, operation) {
            ResumeDecision::Resume(link) => Some(link),
            ResumeDecision::Aborted => None,
        }
    }

    pub(crate) fn recv_rdb_channel_streaming_chunk(
        &mut self,
        rdb_token_value: usize,
        rdb_connection_token: usize,
    ) {
        self.recv_rdb_channel_streaming(
            rdb_token_value,
            rdb_connection_token,
            RdbChannelReadMode::Fair,
        );
    }

    pub(crate) fn recv_rdb_channel_streaming_after_hup(
        &mut self,
        rdb_token_value: usize,
        rdb_connection_token: usize,
    ) {
        self.recv_rdb_channel_streaming(
            rdb_token_value,
            rdb_connection_token,
            RdbChannelReadMode::DrainAfterHup,
        );
    }

    fn recv_rdb_channel_streaming(
        &mut self,
        rdb_token_value: usize,
        rdb_connection_token: usize,
        mode: RdbChannelReadMode,
    ) {
        let Some(mut link) = self.replication_runtime.take_replica_link() else {
            return;
        };
        self.classic_rdb_loading_progress
            .begin_session(link.session_id());
        let context = DualChunkContext {
            main: link.connection_id(),
            rdb_token_value,
            rdb_connection_token,
        };
        let mut chunks_read = 0;
        loop {
            let Some(conn) =
                self.connection_registry
                    .get(crate::framework::capabilities::io::Token(
                        rdb_connection_token,
                    ))
            else {
                self.replication_runtime.set_replica_link(link);
                return;
            };
            let read_data = match self.read_bounded_rdb_chunk(conn.token()) {
                BoundedRdbRead::Data(data) => data,
                BoundedRdbRead::WouldBlock => break,
                BoundedRdbRead::Closed => {
                    self.replication_runtime.set_replica_link(link);
                    self.tear_down_master_link(context.main.0);
                    return;
                }
                BoundedRdbRead::MissingTransport => {
                    self.replication_runtime.set_replica_link(link);
                    return;
                }
            };
            let Some(processed) = self.process_dual_rdb_chunk(link, &context, &read_data) else {
                return;
            };
            link = processed;
            chunks_read += 1;
            if matches!(link.rdb_channel_state(), ReplicaRdbChannelState::RdbLoaded) {
                break;
            }
            if mode == RdbChannelReadMode::Fair
                && chunks_read == RDB_CHANNEL_READ_CHUNKS_PER_DISPATCH
            {
                break;
            }
        }
        self.replication_runtime.set_replica_link(link);
    }

    pub(crate) fn recv_dual_main_while_rdb_loading(
        &mut self,
        token_value: usize,
        connection_token: usize,
    ) -> Result<(), ()> {
        let (read_data, close) = self.read_dual_main_input(connection_token)?;
        if close && read_data.is_empty() {
            self.tear_down_master_link(token_value);
            return Err(());
        }
        if read_data.is_empty() {
            return Ok(());
        }
        let (link, events, actions) = self.decode_dual_main_input(token_value, &read_data)?;
        let link = self.buffer_dual_main_events(token_value, connection_token, link, events)?;
        self.finish_dual_main_read(token_value, connection_token, link, actions, close)
    }

    fn read_dual_main_input(&mut self, connection_token: usize) -> Result<(Vec<u8>, bool), ()> {
        let mut read_data = Vec::new();
        let Some(conn) = self
            .connection_registry
            .get(crate::framework::capabilities::io::Token(connection_token))
        else {
            return Err(());
        };
        let mut stream = self.transport.stream(conn.token()).ok_or(())?;
        let close = crate::foundation::io::sys::os::drain_nonblocking_bounded(
            &mut self.io_buf,
            &mut read_data,
            crate::runtime::server::worker::NORMAL_CLIENT_READ_BUDGET,
            |buf| stream.read(buf),
        )
        .unwrap_or(true);
        Ok((read_data, close))
    }

    fn decode_dual_main_input(
        &mut self,
        token_value: usize,
        read_data: &[u8],
    ) -> Result<(ReplicaLink, Vec<ReplicaEvent>, Vec<ReplicaAction>), ()> {
        let listening_port = self
            .ctx
            .config()
            .replication
            .effective_replica_announce_port(self.ctx.config().server.listening_port);
        let replica_announce_ip = self.ctx.config().replication.replica_announce_ip.clone();
        let Some(mut link) = self.replication_runtime.take_replica_link() else {
            return Err(());
        };
        if link.connection_id().0 != token_value || !link.is_dual_rdb_mode_active() {
            self.replication_runtime.set_replica_link(link);
            return Err(());
        }
        let decoded =
            link.on_bytes_with_announce_ip(read_data, listening_port, &replica_announce_ip);
        self.account_replica_wire_input(&mut link);
        match decoded {
            Ok((events, actions)) => Ok((link, events, actions)),
            Err(error) => {
                crate::server_log!(warning, "dual-rdb main decode rejected while RDB loading: token={} rdb_state={:?} error={:?} bytes={:?}", token_value, link.rdb_channel_state(), error, String::from_utf8_lossy(read_data));
                self.replication_runtime.set_replica_link(link);
                self.abort_master_link_on_error(token_value, error);
                Err(())
            }
        }
    }

    fn buffer_dual_main_events(
        &mut self,
        token_value: usize,
        connection_token: usize,
        mut link: ReplicaLink,
        events: Vec<ReplicaEvent>,
    ) -> Result<ReplicaLink, ()> {
        if let Err(error) = self.apply_replica_handshake_transitions(&mut link) {
            self.replication_runtime.set_replica_link(link);
            self.abort_master_link_on_error(token_value, error);
            return Err(());
        }
        self.ctx.replication_mut().slave_read_repl_offset = link.received_position();
        if self
            .buffer_replica_events_into_pending(connection_token, &mut link, events)
            .is_err()
        {
            self.replication_runtime.set_replica_link(link);
            self.tear_down_master_link(token_value);
            return Err(());
        }
        Ok(link)
    }

    fn finish_dual_main_read(
        &mut self,
        token_value: usize,
        connection_token: usize,
        link: ReplicaLink,
        actions: Vec<ReplicaAction>,
        close: bool,
    ) -> Result<(), ()> {
        let applied_offset = link.applied_position();
        self.replication_runtime.set_replica_link(link);
        self.queue_replica_actions(connection_token, actions);
        self.ctx.replication_mut().slave_repl_offset = applied_offset;
        if close {
            self.tear_down_master_link(token_value);
            return Err(());
        }
        Ok(())
    }

    pub(crate) fn take_matching_replica_session(
        &mut self,
        main: ConnectionId,
        rdb: Option<ConnectionId>,
    ) -> Option<ReplicaLink> {
        let link = self.replication_runtime.take_replica_link()?;
        let main_matches = link.connection_id() == main
            && matches!(
                self.replication_runtime.role(main),
                Some(ReplicationConnection::Replica)
            );
        let rdb_matches = match rdb {
            Some(rdb) => {
                self.replication_runtime.channel_pair_for(main) == Some(rdb)
                    && self
                        .replication_runtime
                        .replica_rdb_link()
                        .is_some_and(|link| link.connection_id() == rdb)
                    && matches!(
                        self.replication_runtime.role(rdb),
                        Some(ReplicationConnection::RdbChannel { main: paired })
                            if paired == main
                    )
            }
            None => true,
        };
        let phase_matches = match rdb {
            Some(_) => {
                link.is_dual_rdb_mode_active()
                    && matches!(
                        link.rdb_channel_state(),
                        ReplicaRdbChannelState::ReceivingRdb | ReplicaRdbChannelState::RdbLoaded
                    )
            }
            None => true,
        };

        if main_matches && rdb_matches && phase_matches {
            Some(link)
        } else {
            self.replication_runtime.set_replica_link(link);
            None
        }
    }

    fn retake_matching_rdb_session(
        &mut self,
        main: ConnectionId,
        rdb: Option<ConnectionId>,
        session_id: u64,
    ) -> Option<ReplicaLink> {
        let link = self.take_matching_replica_session(main, rdb)?;
        let phase_matches = match rdb {
            Some(_) => {
                link.has_pending_rdb_receive()
                    && link.rdb_channel_state() == ReplicaRdbChannelState::ReceivingRdb
            }
            None => link.has_pending_rdb_receive() && !link.is_dual_rdb_mode_active(),
        };
        if link.session_id() == session_id && phase_matches {
            Some(link)
        } else {
            self.replication_runtime.set_replica_link(link);
            None
        }
    }
}

#[cfg(test)]
mod replay_fifo_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::distributed::cluster::node::{CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA};
    use crate::engine::distributed::replication::ReplicationOffset;
    use crate::foundation::io::store::Robj;
    use crate::framework::distributed::replication::{ConnectionId, MasterInfo, RdbLoadMode};

    fn offset(value: i64) -> ReplicationOffset {
        ReplicationOffset::try_from(value).expect("valid test offset")
    }

    #[test]
    fn continue_activates_a_missing_local_backlog() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        assert!(!runtime.ctx.replication().backlog.is_active());
        let mut link = ReplicaLink::new(
            ConnectionId(40),
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        );
        link.set_dual_channel_enabled(false);
        link.set_resume_position(
            "0123456789abcdef0123456789abcdef01234567".to_string(),
            offset(42),
            0,
        );
        link.connected(None, None);
        link.on_bytes(b"+PONG\r\n+OK\r\n+OK\r\n+CONTINUE\r\n", 6380)
            .expect("accept partial resynchronization");

        runtime
            .apply_replica_handshake_transitions(&mut link)
            .expect("valid partial-resync transition");

        assert!(runtime.ctx.replication().backlog.is_active());
    }

    fn classic_receiving_link(main: ConnectionId) -> ReplicaLink {
        let mut link = ReplicaLink::new(
            main,
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        );
        link.connected(None, None);
        link.on_bytes(
            b"+PONG\r\n+OK\r\n+OK\r\n+FULLRESYNC 0123456789abcdef0123456789abcdef01234567 0\r\n$5\r\nab",
            6380,
        )
        .expect("enter classic RDB receive");
        link
    }

    fn assert_disk_backed_transfer_preserves_reads(is_diskless: bool) {
        use crate::framework::capabilities::connection::Connection;

        let directory = std::env::temp_dir().join(format!(
            "versal-rdb-transfer-{}-{:?}-{is_diskless}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let mut runtime = ServerWorkerState::new(1).unwrap();
        runtime.ctx.config_mut().persistence.dir = directory.to_string_lossy().into_owned();
        runtime.ctx.config_mut().replication.repl_diskless_load = "disabled".to_string();
        runtime
            .ctx
            .db_mut(0)
            .unwrap()
            .set(b"old".to_vec(), Robj::string("value"));
        let mut connection = Connection::new_fake();
        connection.set_token_for_test(Token(41));
        let connection_token = runtime.connection_registry.insert(connection);
        let mut link = ReplicaLink::new(
            ConnectionId(41),
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        );
        link.set_dual_channel_enabled(false);
        link.connected(None, None);
        link.on_bytes(b"+PONG\r\n+OK\r\n+OK\r\n", 6380).unwrap();

        let mut rdb = b"VALKEY080\x00".to_vec();
        crate::foundation::io::persistence::save_string(&mut rdb, b"new");
        crate::foundation::io::persistence::save_string(&mut rdb, b"value");
        rdb.push(0);
        crate::foundation::io::persistence::save_string(&mut rdb, b"tail");
        crate::foundation::io::persistence::save_string(&mut rdb, &[b'x'; 256]);
        rdb.push(0xff);
        rdb.extend_from_slice(&[0; 8]);
        let marker = "0123456789abcdef0123456789abcdef01234567";
        let mut frame = format!("+FULLRESYNC {marker} 0\r\n").into_bytes();
        if is_diskless {
            frame.extend_from_slice(format!("$EOF:{marker}\r\n").as_bytes());
        } else {
            frame.extend_from_slice(format!("${}\r\n", rdb.len()).as_bytes());
        }
        frame.extend_from_slice(&rdb[..rdb.len() - 9]);
        let context = ClassicChunkContext {
            token_value: 41,
            connection_token,
            listening_port: 6380,
            replica_announce_ip: "",
        };

        let mut received = runtime
            .process_classic_rdb_chunk(link, &context, &frame)
            .unwrap();

        assert!(
            !runtime.ctx.is_loading(),
            "disk-backed transfer must keep the old dataset readable until framing completes"
        );
        assert!(!runtime.ctx.replication().async_loading);
        assert!(runtime.ctx.db(0).unwrap().get_no_expire(b"old").is_some());
        assert!(runtime.ctx.db(0).unwrap().get_no_expire(b"new").is_none());
        assert_eq!(
            received.link.rdb_receive_mut().unwrap().loaded(),
            0,
            "C receives the complete RDB before decoding a disk-backed transfer"
        );

        let mut terminal = rdb[rdb.len() - 9..].to_vec();
        if is_diskless {
            terminal.extend_from_slice(marker.as_bytes());
        }
        let completed = runtime
            .process_classic_rdb_chunk(received.link, &context, &terminal)
            .unwrap();

        assert!(!runtime.ctx.is_loading());
        assert!(!completed.link.has_pending_rdb_receive());
        assert!(completed.link.can_ack_primary());
        assert!(runtime.ctx.db(0).unwrap().get_no_expire(b"old").is_none());
        assert_eq!(
            runtime
                .ctx
                .db(0)
                .unwrap()
                .get_no_expire(b"new")
                .unwrap()
                .as_bytes(),
            b"value"
        );
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn disk_backed_length_transfer_keeps_old_dataset_readable() {
        assert_disk_backed_transfer_preserves_reads(false);
    }

    #[test]
    fn disk_backed_eof_transfer_keeps_old_dataset_readable() {
        assert_disk_backed_transfer_preserves_reads(true);
    }

    #[test]
    fn disk_backed_dual_transfer_waits_for_its_own_framing() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        runtime
            .ctx
            .db_mut(0)
            .unwrap()
            .set(b"old".to_vec(), Robj::string("value"));
        let main = ConnectionId(41);
        let mut link = ReplicaLink::new(
            main,
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        );
        link.pin_rdb_load_mode(RdbLoadMode::DiskBacked);
        link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
            .unwrap();
        link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
            .unwrap();
        let mut frame = b"+OK\r\n".to_vec();
        frame.extend_from_slice(
            &crate::engine::distributed::replication::protocol::encode_end_offset(
                offset(50),
                "0123456789abcdef0123456789abcdef01234567",
                0,
                42,
            ),
        );
        frame.extend_from_slice(b"$20\r\nVALKEY080");
        link.on_rdb_bytes(&frame).unwrap();

        runtime.apply_streaming_loading_gate(&link);
        runtime.drive_streaming_decode(42, 0, &mut link).unwrap();

        assert!(!runtime.ctx.is_loading());
        assert!(link.rdb_framing_is_pending());
        assert!(runtime.ctx.db(0).unwrap().get_no_expire(b"old").is_some());
        assert_eq!(link.rdb_receive_mut().unwrap().loaded(), 0);

        link.on_rdb_bytes(&[0; 11]).unwrap();
        assert!(link.rdb_framing_is_complete());
        runtime.apply_streaming_loading_gate(&link);

        assert!(runtime.ctx.is_loading());
        assert!(!runtime.ctx.replication().async_loading);
    }

    #[test]
    fn classic_fullresync_dispatch_does_not_publish_uncommitted_snapshot_offset() {
        use crate::framework::capabilities::connection::Connection;

        for cached_offset in [0, 42] {
            let mut runtime = ServerWorkerState::new(1).unwrap();
            runtime.ctx.replication_mut().switch_to_replica();
            runtime.ctx.replication_mut().slave_repl_offset = offset(cached_offset);
            let mut connection = Connection::new_fake();
            connection.set_token_for_test(Token(41));
            let connection_token = runtime.connection_registry.insert(connection);
            let mut link = ReplicaLink::new(
                ConnectionId(41),
                MasterInfo {
                    host: "127.0.0.1".to_string(),
                    port: 6379,
                },
            );
            if cached_offset != 0 {
                link.set_resume_position("cached-replid".to_string(), offset(cached_offset), 0);
            }
            link.connected(None, None);
            let (events, actions) = link
                .on_bytes(
                    b"+PONG\r\n+OK\r\n+OK\r\n+FULLRESYNC 0123456789abcdef0123456789abcdef01234567 1563\r\n",
                    6380,
                )
                .unwrap();
            runtime.finish_classic_rdb_dispatch(
                41,
                connection_token,
                ClassicDispatchResult {
                    link,
                    events,
                    actions,
                    peer_closed: false,
                },
            );
            assert!(!runtime.ctx.is_loading());
            assert_eq!(
                runtime.ctx.replication().slave_repl_offset,
                offset(cached_offset),
                "the runtime must publish processed history until the RDB commit"
            );
        }
    }

    #[test]
    fn direct_full_sync_seeds_backlog_without_applying_snapshot_position() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        runtime.ctx.replication_mut().switch_to_replica();
        let mut link = ReplicaLink::new(
            ConnectionId(41),
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        );
        link.set_resume_position("cached-replid".to_string(), offset(42), 0);
        link.connected(None, None);
        link.on_bytes(
            b"+PONG\r\n+OK\r\n+OK\r\n+FULLRESYNC 0123456789abcdef0123456789abcdef01234567 1563\r\n$100\r\nab",
            6380,
        )
        .unwrap();
        assert_eq!(link.applied_position(), offset(42));

        runtime.init_streaming_target(&mut link).unwrap();

        assert_eq!(
            runtime.ctx.replication().backlog.reported_offset(),
            offset(1563)
        );
        assert_eq!(link.applied_position(), offset(42));
    }

    #[test]
    fn direct_full_sync_rejects_a_negative_peer_offset() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        runtime.ctx.replication_mut().switch_to_replica();
        let mut link = ReplicaLink::new(
            ConnectionId(41),
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        );
        link.connected(None, None);
        link.on_bytes(b"+PONG\r\n+OK\r\n+OK\r\n", 6380)
            .expect("advance to the PSYNC reply");
        let received = link.received_position();

        assert!(matches!(
            link.on_bytes(
                b"+FULLRESYNC 0123456789abcdef0123456789abcdef01234567 -1\r\n$5\r\nab",
                6380,
            ),
            Err(LinkError::Protocol)
        ));
        assert_eq!(link.received_position(), received);
        assert!(link.master_replid().is_none());
        assert!(!link.is_receiving_rdb());
        assert!(!runtime.ctx.replication().backlog.is_active());
    }

    #[test]
    fn direct_full_sync_lazy_flushes_old_values_before_decoding() {
        use crate::foundation::io::store::structure::list::ListValue;
        use bytes::Bytes;
        use std::collections::VecDeque;

        let mut runtime = ServerWorkerState::new(1).unwrap();
        runtime.ctx.replication_mut().switch_to_replica();
        runtime.ctx.db_mut(0).unwrap().set(
            b"old".to_vec(),
            Robj::ListQuicklist(ListValue::from_vecdeque(VecDeque::from(
                (0..65)
                    .map(|_| Bytes::from_static(b"value"))
                    .collect::<Vec<_>>(),
            ))),
        );
        let mut link = classic_receiving_link(ConnectionId(41));

        runtime
            .init_streaming_target(&mut link)
            .expect("valid direct full-sync boundary");

        assert!(runtime.ctx.db(0).unwrap().get_no_expire(b"old").is_none());
        assert_eq!(runtime.ctx.stats().lazyfree_pending_objects, 65);
    }

    #[test]
    fn busy_script_defers_classic_rdb_payload_until_script_finishes() {
        let (mut runtime, _primary, token) = setup_buffered_busy_classic_rdb();

        let link = runtime
            .replication_runtime
            .replica_link()
            .expect("replica link remains registered");
        assert!(link.has_pending_rdb_receive());
        assert!(!link.has_received_rdb_payload());
        assert!(!runtime.ctx.is_loading());

        runtime.ctx.clear_script_busy();
        assert_eq!(
            runtime.processable_classic_replica_input_token(),
            Some(token)
        );
        assert_ne!(
            runtime.next_poll_timeout(1_000),
            Some(std::time::Duration::ZERO),
            "before-poll work must not force a zero poll timeout"
        );
        assert_eq!(
            runtime
                .run_before_poll_phase()
                .expect("before-poll phase handles buffered classic RDB"),
            crate::runtime::server::worker::reactor::LoopControl::Continue
        );

        assert!(runtime
            .replication_runtime
            .replica_link()
            .is_some_and(|link| !link.has_pending_rdb_receive()));
        assert_eq!(runtime.ctx.config().replication.master_link_status, "up");
        assert!(runtime.transport.is_registered(token));
    }

    #[test]
    fn stale_buffered_replica_event_is_ignored_without_zero_spin() {
        let (mut runtime, _primary, token) = setup_buffered_busy_classic_rdb();
        runtime.ctx.clear_script_busy();
        let events = runtime.collect_before_sleep_pending_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].token, token);
        let role = runtime.replication_runtime.role(ConnectionId(token.0));

        runtime
            .transport
            .remove_registered(token)
            .expect("remove stale replica transport");
        for event in events {
            runtime.dispatch_transport_event(
                crate::runtime::server::worker::reactor::ReadyEvent {
                    token: event.token,
                    readable: event.readable,
                    writable: event.writable,
                    error: event.error,
                    read_hup: event.read_hup,
                },
                false,
            );
        }

        assert_eq!(
            runtime.replication_runtime.role(ConnectionId(token.0)),
            role
        );
        assert!(runtime.replication_runtime.replica_link().is_some());
        assert_ne!(
            runtime.next_poll_timeout(1_000),
            Some(std::time::Duration::ZERO)
        );
    }

    fn setup_buffered_busy_classic_rdb() -> (
        ServerWorkerState,
        std::net::TcpStream,
        crate::framework::capabilities::io::Token,
    ) {
        use crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer;
        use std::io::Write;

        let mut runtime = ServerWorkerState::new(1).expect("create runtime");
        runtime.ctx.config_mut().replication.repl_diskless_load = "flush-before-load".to_string();
        runtime.ctx.set_script_busy(true, false);

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind primary peer");
        let primary_addr = listener.local_addr().expect("primary peer address");
        let peer = runtime
            .transport
            .connect_tcp(primary_addr)
            .expect("connect replica transport");
        let (mut primary, _) = listener.accept().expect("accept replica transport");
        runtime.add_connection(RuntimeConnectedPeer {
            token: peer.token,
            peer_addr: primary_addr,
        });
        let connection_token = peer.token.0;
        let mut link = ReplicaLink::new(
            ConnectionId(peer.token.0),
            MasterInfo {
                host: primary_addr.ip().to_string(),
                port: primary_addr.port(),
            },
        );
        link.set_dual_channel_enabled(false);
        link.connected(None, None);
        link.on_bytes(b"+PONG\r\n+OK\r\n+OK\r\n", 6380)
            .expect("advance replica link to PSYNC reply");
        runtime.replication_runtime.set_replica_link(link);

        let rdb = crate::foundation::io::persistence::serialize_all(runtime.ctx.dbs(), 0);
        let mut frame = b"+FULLRESYNC 0123456789abcdef0123456789abcdef01234567 0\r\n".to_vec();
        frame.extend_from_slice(format!("${}\r\n", rdb.len()).as_bytes());
        frame.extend_from_slice(&rdb);
        primary.write_all(&frame).expect("write full sync frame");
        runtime.transport.wait_readable_for_test(peer.token);

        runtime.handle_master_link_readable(peer.token.0, connection_token);
        (runtime, primary, peer.token)
    }

    #[test]
    fn on_empty_db_load_mode_stays_direct_across_chunks() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        runtime.ctx.config_mut().replication.repl_diskless_load = "on-empty-db".to_string();
        let mut link = ReplicaLink::new(
            ConnectionId(40),
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        );

        runtime.pin_replica_rdb_load_mode(&mut link);
        assert_eq!(link.rdb_load_mode(), Some(RdbLoadMode::Direct));

        // Model a later chunk after the direct decoder has populated the
        // formerly empty serving dataset. Re-evaluating `on-empty-db` now would
        // incorrectly switch this same transfer to disk-backed installation.
        runtime
            .ctx
            .db_mut(0)
            .unwrap()
            .set(b"decoded".to_vec(), Robj::string("value"));
        runtime.pin_replica_rdb_load_mode(&mut link);

        assert_eq!(link.rdb_load_mode(), Some(RdbLoadMode::Direct));
    }

    #[test]
    fn config_mutation_does_not_change_pinned_load_mode() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        runtime.ctx.config_mut().replication.repl_diskless_load = "swapdb".to_string();
        let mut link = ReplicaLink::new(
            ConnectionId(40),
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        );

        runtime.pin_replica_rdb_load_mode(&mut link);
        runtime.ctx.config_mut().replication.repl_diskless_load = "disabled".to_string();
        runtime.pin_replica_rdb_load_mode(&mut link);

        assert_eq!(link.rdb_load_mode(), Some(RdbLoadMode::Swap));
    }

    #[test]
    fn retake_rdb_session_rejects_same_token_link_replacement() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        let main = ConnectionId(41);
        let original = classic_receiving_link(main);
        let original_session_id = original.session_id();
        runtime.replication_runtime.set_replica_link(original);

        let replacement = classic_receiving_link(main);
        let replacement_session_id = replacement.session_id();
        runtime.replication_runtime.set_replica_link(replacement);

        assert!(
            runtime
                .retake_matching_rdb_session(main, None, original_session_id)
                .is_none(),
            "nested dispatch must not substitute a new link on the same token"
        );
        assert_eq!(
            runtime
                .replication_runtime
                .replica_link()
                .expect("replacement remains runtime-owned")
                .session_id(),
            replacement_session_id
        );
    }

    #[test]
    fn retake_rdb_session_rejects_a_link_that_left_the_rdb_phase() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        let main = ConnectionId(42);
        let mut link = classic_receiving_link(main);
        let session_id = link.session_id();
        let _finished_receive = link
            .take_rdb_receive()
            .expect("test link starts with an active RDB receive");
        assert!(!link.has_pending_rdb_receive());
        runtime.replication_runtime.set_replica_link(link);

        assert!(
            runtime
                .retake_matching_rdb_session(main, None, session_id)
                .is_none(),
            "nested dispatch must abort the old loader after its RDB phase changes"
        );
        assert!(runtime.replication_runtime.replica_link().is_some());
    }

    #[test]
    fn terminal_shutdown_during_rdb_progress_aborts_decoder_resume() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let peer_addr = listener.local_addr().unwrap();
        let peer = runtime.transport.connect_tcp(peer_addr).unwrap();
        let (_primary, _) = listener.accept().unwrap();
        runtime.add_connection(
            crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer {
                token: peer.token,
                peer_addr,
            },
        );
        runtime
            .ctx
            .config_mut()
            .persistence
            .loading_process_events_interval_bytes = 1;
        runtime.ctx.set_shutdown_requested(true);
        let main = ConnectionId(peer.token.0);
        let link = classic_receiving_link(main);

        assert!(matches!(
            runtime.service_rdb_progress_boundary(link, BlockedOperation::ClassicRdb { main }, 1,),
            ResumeDecision::Aborted
        ));
        assert!(runtime.replication_runtime.replica_link().is_some());
    }

    #[test]
    fn blocking_full_sync_immediately_advertises_zero_cluster_offset() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        runtime.ctx.replication_mut().switch_to_replica();
        runtime.ctx.replication_mut().slave_repl_offset = offset(42);
        let mut cluster =
            crate::engine::distributed::cluster::ClusterState::new("127.0.0.1", 6380, 16_380);
        cluster.myself.flags &= !CLUSTER_NODE_PRIMARY;
        cluster.myself.flags |= CLUSTER_NODE_REPLICA;
        cluster.myself.repl_offset = 42;
        *runtime.ctx.cluster_state_mut() = Some(Box::new(cluster));
        let link = ReplicaLink::new(
            ConnectionId(41),
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        );

        runtime.apply_streaming_loading_gate(&link);

        assert!(runtime.ctx.is_loading());
        assert_eq!(
            runtime
                .ctx
                .cluster_state()
                .as_deref()
                .unwrap()
                .myself
                .repl_offset,
            0
        );
    }

    #[test]
    fn dual_rdb_header_only_preserves_cached_primary_until_payload_arrives() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        runtime.ctx.replication_mut().switch_to_replica();
        runtime.ctx.config_mut().replication.repl_diskless_load = "on".to_string();
        let cached_replid = runtime.ctx.replication().backlog.replid().to_string();
        let main = ConnectionId(41);
        let rdb = ConnectionId(42);
        let mut link = ReplicaLink::new(
            main,
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        );
        link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
            .unwrap();
        link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
            .unwrap();
        let mut framing = b"+OK\r\n".to_vec();
        framing.extend_from_slice(
            &crate::engine::distributed::replication::protocol::encode_end_offset(
                offset(50),
                "0123456789abcdef0123456789abcdef01234567",
                0,
                rdb.0 as u64,
            ),
        );
        framing.extend_from_slice(b"$EOF:fedcba9876543210fedcba9876543210fedcba98\r\n");
        link.on_rdb_bytes(&framing)
            .expect("header-only dual RDB framing must be accepted");
        assert!(!link.has_received_rdb_payload());
        runtime.mark_replica_transfer_started(&link);

        runtime
            .drive_streaming_decode(rdb.0, 0, &mut link)
            .expect("header-only RDB drive must remain pending");

        assert_eq!(runtime.ctx.replication().backlog.replid(), cached_replid);
        assert!(
            !runtime.ctx.is_loading(),
            "framing alone must not attach the replica to the new primary"
        );
        assert_eq!(
            runtime.ctx.config().replication.master_link_status,
            "sync",
            "RDB framing starts C's TRANSFER state before the first payload byte"
        );
    }

    #[test]
    fn direct_rdb_load_discards_cached_primary_before_replacing_dataset() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        let cached_master = MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6380,
        };
        runtime.replication_runtime.set_replica_resume(
            cached_master.clone(),
            "old-committed".to_string(),
            offset(7),
            0,
        );
        let mut link = ReplicaLink::new(
            ConnectionId(41),
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        );
        link.pin_rdb_load_mode(RdbLoadMode::Direct);
        let marker = "0123456789abcdef0123456789abcdef01234567";
        link.connected(None, None);
        link.on_bytes(
            format!("+PONG\r\n+OK\r\n+OK\r\n+FULLRESYNC {marker} 0\r\n$EOF:{marker}\r\n")
                .as_bytes(),
            6380,
        )
        .expect("full-sync header must initialize RDB receive state");

        runtime
            .init_streaming_target(&mut link)
            .expect("valid direct full-sync boundary");

        assert_eq!(
            runtime
                .replication_runtime
                .replica_resume_for(&cached_master),
            None,
            "C discards its cached primary before a direct load mutates the serving DB"
        );
        assert!(!runtime.replication_runtime.take_force_full_sync());
    }

    #[test]
    fn failed_non_swapdb_rdb_load_discards_the_partial_dataset() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        runtime.ctx.replication_mut().switch_to_replica();
        runtime.ctx.config_mut().replication.repl_diskless_load = "flush-before-load".to_string();
        runtime.ctx.set_loading(true);
        runtime.ctx.db_mut(0).unwrap().set(
            b"partially-loaded".to_vec(),
            crate::foundation::io::store::Robj::string("value"),
        );
        runtime
            .replication_runtime
            .set_replica_link(ReplicaLink::new(
                ConnectionId(41),
                MasterInfo {
                    host: "127.0.0.1".to_string(),
                    port: 6379,
                },
            ));

        runtime.remove_connection(41);

        assert_eq!(runtime.ctx.db_size(0), 0);
        assert!(!runtime.ctx.is_loading());
    }
}
