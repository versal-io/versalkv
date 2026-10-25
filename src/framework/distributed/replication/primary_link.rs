use crate::engine::distributed::replication::{
    backlog::{ContinueMatch, ReplicationBacklog},
    BacklogCursor, PsyncRequestOffset, ReplicationOffset, SnapshotBoundary,
};

use super::{
    ConnectionId, LinkError, PrimaryDualChannelState, PrimaryLinkState, PrimarySyncPlan,
    RdbFilterMode, ReplicaConnectionInfo,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OutputBufferSoftLimitReachedAt(i64);

impl OutputBufferSoftLimitReachedAt {
    pub(crate) const fn from_millis(value: i64) -> Self {
        Self(value)
    }

    fn elapsed_at(self, now_ms: i64, limit_seconds: u64) -> bool {
        now_ms.saturating_sub(self.0) > (limit_seconds as i64).saturating_mul(1_000)
    }

    pub(crate) fn update(
        reached_at: &mut Option<Self>,
        over_limit: bool,
        now_ms: i64,
        limit_seconds: u64,
    ) -> bool {
        if !over_limit {
            *reached_at = None;
            return false;
        }
        let Some(started_at) = *reached_at else {
            *reached_at = Some(Self::from_millis(now_ms));
            return false;
        };
        started_at.elapsed_at(now_ms, limit_seconds)
    }
}

#[derive(Debug)]
pub struct PrimaryLink {
    connection_id: ConnectionId,
    replica: ReplicaConnectionInfo,
    state: PrimaryLinkState,
    dual_state: PrimaryDualChannelState,
    /// Rust half-open backlog cursor of the next byte to queue.
    next_send_cursor: BacklogCursor,
    dual_endoff_boundary: Option<SnapshotBoundary>,
    dual_main_backlog_pin_cursor: Option<BacklogCursor>,
    /// Rust half-open backlog cursor of the next byte not yet written.
    sent_cursor: BacklogCursor,
    pending_replication_bytes: usize,
    dual_main_stream_started: bool,
    dual_main_resume_read_cursor: Option<BacklogCursor>,
    dual_rdb_complete: bool,
    dual_post_psync_ack_received: bool,
    full_resync: Option<FullResyncTransfer>,
    /// Mirrors C's per-replica command-stream start boundary
    /// (`replicaStartCommandStream`): set when a classic full resync hands the
    /// replica from the RDB stream to the incremental command stream. The L5
    /// runtime takes this flag once to seed an empty replication history with
    /// the bootstrap PING. Partial PSYNC and dual-channel resumes never set it.
    command_stream_start_pending: bool,
    /// Diskless classic sync keeps the command stream gated until the first
    /// post-RDB ACK. The replica is already externally online at this point,
    /// matching C Valkey's split between `replicaPutOnline` and
    /// `replicaStartCommandStream`.
    full_resync_ack_pending: bool,
    last_ack_offset: Option<ReplicationOffset>,
    last_aof_ack_offset: Option<ReplicationOffset>,
    last_ack_time_ms: i64,
    last_send_time_ms: i64,
    replica_version: u32,
    /// Last unix-time (seconds) at which this replica's socket failed to drain
    /// a full chunk during a diskless/disk-sync full resync. Zero while the
    /// socket is accepting writes without stalling, or once the replica reaches
    /// the streaming state. Mirrors C's `repl_last_partial_write` used by the
    /// "(full sync)" replica-timeout path (replication.c:5431).
    last_partial_write_sec: i64,
    /// When `Some(t)`, the link is waiting for the diskless-sync delay to
    /// expire before generating and streaming the RDB snapshot.  `t` is the
    /// absolute mstime at which we can proceed.
    // TODO(dead_code): reserved for future deferred-diskless-sync PSYNC path;
    // keep until transport.rs wires the delay handler back up.
    #[allow(dead_code)]
    diskless_ready_at_ms: Option<i64>,
    /// Whether +FULLRESYNC prefix needs to be sent once delay expires (used
    /// for PSYNC-style full resyncs; plain SYNC skips the prefix).
    // TODO(dead_code): reserved for future deferred-diskless-sync PSYNC path;
    // keep until transport.rs wires the delay handler back up.
    #[allow(dead_code)]
    deferred_send_fullresync: bool,
    pub(crate) rdb_channel_wait_deadline_ms: Option<i64>,
    pub(crate) dual_channel_already_failed: bool,
    output_buffer_soft_limit_reached_at: Option<OutputBufferSoftLimitReachedAt>,
    /// Position in C's `server.replicas` list, assigned once when PSYNC is
    /// accepted (replication.c:961 / replication.c:1230). Registration order
    /// at REPLCONF time can differ across concurrently connecting replicas.
    accept_seq: Option<u64>,
}

#[derive(Debug)]
struct FullResyncTransfer {
    bytes: Vec<u8>,
    cursor: usize,
    chunk_size: usize,
    delay_ms: i64,
    next_chunk_at_ms: i64,
    should_wait_for_ack: bool,
}

impl PrimaryLink {
    pub fn new(connection_id: ConnectionId, replica: ReplicaConnectionInfo) -> Self {
        let zero = BacklogCursor::try_from(0).expect("zero is a valid replication cursor");
        Self {
            connection_id,
            replica,
            state: PrimaryLinkState::AwaitingPsync,
            dual_state: PrimaryDualChannelState::Disabled,
            next_send_cursor: zero,
            dual_endoff_boundary: None,
            dual_main_backlog_pin_cursor: None,
            sent_cursor: zero,
            pending_replication_bytes: 0,
            dual_main_stream_started: false,
            dual_main_resume_read_cursor: None,
            dual_rdb_complete: false,
            dual_post_psync_ack_received: false,
            full_resync: None,
            command_stream_start_pending: false,
            full_resync_ack_pending: false,
            last_ack_offset: None,
            last_aof_ack_offset: None,
            last_ack_time_ms: 0,
            last_send_time_ms: crate::foundation::io::sys::time::mstime(),
            replica_version: 0,
            last_partial_write_sec: 0,
            diskless_ready_at_ms: None,
            deferred_send_fullresync: false,
            rdb_channel_wait_deadline_ms: None,
            dual_channel_already_failed: false,
            output_buffer_soft_limit_reached_at: None,
            accept_seq: None,
        }
    }

    pub(crate) fn handle_psync(
        &mut self,
        replid: &str,
        requested: PsyncRequestOffset,
        backlog: &ReplicationBacklog,
    ) -> PrimarySyncPlan {
        let plan = match backlog.resolve_continue(replid, requested) {
            Some(position) if position.history == ContinueMatch::Primary => {
                self.put_online();
                self.next_send_cursor = position.cursor;
                self.sent_cursor = position.cursor;
                PrimarySyncPlan::Continue { new_replid: None }
            }
            Some(position) if position.history == ContinueMatch::Second => {
                self.put_online();
                self.next_send_cursor = position.cursor;
                self.sent_cursor = position.cursor;
                PrimarySyncPlan::Continue {
                    new_replid: Some(backlog.replid().to_string()),
                }
            }
            Some(_) => unreachable!("continuation history has only primary and second variants"),
            None => self.plan_full_resync(backlog),
        };
        if Self::enforce_cursor_invariant(
            self.sent_cursor,
            self.next_send_cursor,
            backlog.next_stream_cursor(),
        )
        .is_err()
        {
            self.state = PrimaryLinkState::Closing;
        }
        plan
    }

    pub(crate) fn plan_full_resync(&mut self, backlog: &ReplicationBacklog) -> PrimarySyncPlan {
        let boundary = backlog.snapshot_boundary();
        self.state = PrimaryLinkState::PreparingFullResync;
        self.next_send_cursor = boundary.stream_cursor();
        self.sent_cursor = boundary.stream_cursor();
        if Self::enforce_cursor_invariant(
            self.sent_cursor,
            self.next_send_cursor,
            backlog.next_stream_cursor(),
        )
        .is_err()
        {
            self.state = PrimaryLinkState::Closing;
        }
        PrimarySyncPlan::FullResync { boundary }
    }

    /// Finish producing a diskless classic full-sync snapshot. The link is
    /// visible as online, but backlog pumping stays disabled until the first
    /// post-RDB ACK so command bytes cannot be glued to the EOF marker.
    pub fn finish_full_resync(&mut self) {
        self.full_resync = None;
        self.dual_main_backlog_pin_cursor = None;
        self.full_resync_ack_pending = true;
        self.state = PrimaryLinkState::WaitingForReplicaAck;
        self.last_ack_time_ms = crate::foundation::io::sys::time::mstime();
        self.last_partial_write_sec = 0;
    }

    /// Complete a disk-based classic full sync once the final RDB bytes have
    /// left the userspace output queue. C Valkey's `sendBulkToReplica` makes
    /// this path online immediately; only socket-based diskless transfer keeps
    /// command streaming behind the replica's terminal ACK.
    pub fn finish_disk_based_full_resync(&mut self) {
        self.full_resync = None;
        self.dual_main_backlog_pin_cursor = None;
        self.put_online();
        // C calls replicaStartCommandStream() immediately after the last RDB
        // byte of a disk/length-framed transfer; the runtime owes the
        // empty-history bootstrap PING once it observes this flag.
        self.command_stream_start_pending = true;
    }

    pub fn promote_full_resync_on_ack(&mut self) -> bool {
        if self.state != PrimaryLinkState::WaitingForReplicaAck
            || !self.full_resync_ack_pending
            || self.last_ack_offset.is_none()
        {
            return false;
        }
        self.put_online();
        // Equivalent C boundary: REPLCONF ACK runs replicaStartCommandStream()
        // for an EOF-framed diskless transfer once the replica confirms the
        // RDB load.
        self.command_stream_start_pending = true;
        true
    }

    /// Consume and return whether the classic full-resync RDB-to-command-stream
    /// boundary (`replicaStartCommandStream` in C) fired since the last take.
    pub fn take_command_stream_start_pending(&mut self) -> bool {
        std::mem::take(&mut self.command_stream_start_pending)
    }

    pub fn begin_full_resync(&mut self, bytes: Vec<u8>, chunk_count: usize, now_ms: i64) {
        self.begin_full_resync_transfer(bytes, chunk_count, now_ms, true);
    }

    /// Start a length-framed full sync. Unlike EOF-framed socket transfers,
    /// the replica knows the exact RDB boundary and can receive command bytes
    /// immediately after the final payload byte.
    pub fn begin_length_framed_full_resync(
        &mut self,
        bytes: Vec<u8>,
        chunk_count: usize,
        now_ms: i64,
    ) {
        self.begin_full_resync_transfer(bytes, chunk_count, now_ms, false);
    }

    fn begin_full_resync_transfer(
        &mut self,
        bytes: Vec<u8>,
        _chunk_count: usize,
        now_ms: i64,
        should_wait_for_ack: bool,
    ) {
        let chunk_size = bytes.len().max(1);
        self.full_resync = Some(FullResyncTransfer {
            bytes,
            cursor: 0,
            chunk_size,
            delay_ms: 0,
            next_chunk_at_ms: now_ms,
            should_wait_for_ack,
        });
        self.state = PrimaryLinkState::SendingRdb;
    }

    pub fn pump_full_resync(&mut self, now_ms: i64) -> (Vec<u8>, bool) {
        let Some(transfer) = self.full_resync.as_mut() else {
            return (Vec::new(), false);
        };
        if now_ms < transfer.next_chunk_at_ms {
            return (Vec::new(), false);
        }
        let end = (transfer.cursor + transfer.chunk_size).min(transfer.bytes.len());
        let bytes = transfer.bytes[transfer.cursor..end].to_vec();
        transfer.cursor = end;
        transfer.next_chunk_at_ms = transfer.next_chunk_at_ms.saturating_add(transfer.delay_ms);
        let is_done = transfer.cursor == transfer.bytes.len();
        let should_wait_for_ack = transfer.should_wait_for_ack;
        if is_done {
            if should_wait_for_ack {
                self.finish_full_resync();
            } else {
                self.finish_disk_based_full_resync();
            }
        }
        (bytes, is_done)
    }

    pub fn pump(&mut self, backlog: &ReplicationBacklog) -> Result<Vec<u8>, LinkError> {
        if self.state != PrimaryLinkState::Streaming {
            return Ok(Vec::new());
        }
        let backlog_end = backlog.next_stream_cursor();
        Self::enforce_cursor_invariant(self.sent_cursor, self.next_send_cursor, backlog_end)?;
        let read_cursor = self
            .dual_main_resume_read_cursor
            .unwrap_or(self.next_send_cursor);
        let bytes = match backlog.read_from(read_cursor) {
            Some(bytes) => bytes,
            None => {
                self.state = PrimaryLinkState::Closing;
                return Err(LinkError::BacklogEvicted);
            }
        };
        Self::enforce_cursor_invariant(self.sent_cursor, backlog_end, backlog_end)?;
        self.next_send_cursor = backlog_end;
        self.dual_main_resume_read_cursor = None;
        self.dual_main_backlog_pin_cursor = None;
        Self::enforce_cursor_invariant(self.sent_cursor, self.next_send_cursor, backlog_end)?;
        Ok(bytes)
    }

    pub fn mark_replication_queued(&mut self, bytes: usize) {
        self.pending_replication_bytes = self.pending_replication_bytes.saturating_add(bytes);
    }

    pub fn mark_replication_written(
        &mut self,
        bytes: usize,
        backlog_end: BacklogCursor,
    ) -> Result<(), LinkError> {
        if bytes > self.pending_replication_bytes {
            return Err(LinkError::Protocol);
        }
        Self::enforce_cursor_invariant(self.sent_cursor, self.next_send_cursor, backlog_end)?;
        let sent_cursor = self
            .sent_cursor
            .checked_advance(bytes)
            .ok_or(LinkError::Protocol)?;
        Self::enforce_cursor_invariant(sent_cursor, self.next_send_cursor, backlog_end)?;
        self.pending_replication_bytes -= bytes;
        self.sent_cursor = sent_cursor;
        Self::enforce_cursor_invariant(self.sent_cursor, self.next_send_cursor, backlog_end)
    }

    pub fn has_pending_replication(&self) -> bool {
        self.pending_replication_bytes != 0
    }

    /// During dual RDB loading, the main PSYNC has already established a live
    /// socket while the replica retains incremental frames locally. Those
    /// frames are written directly to the socket and must not keep the primary
    /// backlog pinned until the terminal ACK arrives.
    pub fn advance_terminal_dual_main_stream(
        &mut self,
        append_end: BacklogCursor,
        backlog_end: BacklogCursor,
    ) -> Result<bool, LinkError> {
        if self.state != PrimaryLinkState::Streaming
            || self.dual_state != PrimaryDualChannelState::WaitingForReplicaAck
        {
            Self::enforce_cursor_invariant(self.sent_cursor, self.next_send_cursor, backlog_end)?;
            return Ok(false);
        }
        if append_end < self.next_send_cursor || append_end > backlog_end {
            return Err(LinkError::Protocol);
        }
        Self::enforce_cursor_invariant(self.sent_cursor, append_end, backlog_end)?;
        self.next_send_cursor = append_end;
        self.dual_main_stream_started = true;
        self.dual_main_resume_read_cursor = None;
        Self::enforce_cursor_invariant(self.sent_cursor, self.next_send_cursor, backlog_end)?;
        Ok(true)
    }

    pub fn keepalive_due(&self, now_ms: i64, interval_ms: i64) -> bool {
        self.state == PrimaryLinkState::Streaming
            && now_ms.saturating_sub(self.last_send_time_ms) >= interval_ms
    }

    pub fn mark_send_activity(&mut self, now_ms: i64) {
        self.last_send_time_ms = now_ms;
    }

    pub fn acknowledge(&mut self, offset: ReplicationOffset, now_ms: i64) {
        if self.last_ack_offset.is_none_or(|current| offset > current) {
            self.last_ack_offset = Some(offset);
        }
        if self.dual_state == PrimaryDualChannelState::WaitingForReplicaAck {
            self.dual_post_psync_ack_received = true;
        }
        self.last_ack_time_ms = now_ms;
    }

    pub fn acknowledge_aof(&mut self, offset: ReplicationOffset) {
        if self
            .last_aof_ack_offset
            .is_none_or(|current| offset > current)
        {
            self.last_aof_ack_offset = Some(offset);
        }
    }

    /// Returns true if this link is waiting for the diskless sync delay to
    /// expire (PSYNC received, snapshot not yet started).
    // TODO(dead_code): reserved for future deferred-diskless-sync PSYNC path.
    #[allow(dead_code)]
    pub fn is_deferred_diskless(&self) -> bool {
        self.diskless_ready_at_ms.is_some()
    }

    /// Mark this link as waiting for the diskless sync delay.  `delay_ms` is
    /// relative; converted to absolute mstime using `now_ms`.  `send_fullresync`
    /// must be `true` for PSYNC (needs `+FULLRESYNC` header) and `false` for
    /// plain SYNC (starts directly with `$<len>` RDB bulk).
    // TODO(dead_code): reserved for future deferred-diskless-sync PSYNC path.
    #[allow(dead_code)]
    pub fn defer_diskless_sync(&mut self, delay_ms: i64, now_ms: i64, send_fullresync: bool) {
        debug_assert!(matches!(self.state, PrimaryLinkState::PreparingFullResync));
        self.diskless_ready_at_ms = Some(now_ms.saturating_add(delay_ms.max(0)));
        self.deferred_send_fullresync = send_fullresync;
    }

    /// Returns true if the diskless delay has elapsed (or we never deferred).
    // TODO(dead_code): reserved for future deferred-diskless-sync PSYNC path.
    #[allow(dead_code)]
    pub fn diskless_delay_ready(&self, now_ms: i64) -> bool {
        match self.diskless_ready_at_ms {
            None => true,
            Some(t) => now_ms >= t,
        }
    }

    /// Align the RDB snapshot boundary at fire time. For deferred diskless
    /// syncs the RDB is serialized when the delay expires, not at PSYNC time,
    /// so the advertised `+FULLRESYNC` offset and the backlog resume cursor
    /// must both track the state the freshly serialized snapshot reflects.
    /// Using the stale PSYNC-time boundary would
    /// re-stream writes already baked into the RDB (digest divergence) or hit
    /// an evicted offset and tear the link down under heavy write load.
    pub fn set_full_resync_boundary(
        &mut self,
        boundary: SnapshotBoundary,
    ) -> Result<(), LinkError> {
        let cursor = boundary.stream_cursor();
        self.next_send_cursor = cursor;
        self.sent_cursor = cursor;
        Self::enforce_cursor_invariant(self.sent_cursor, self.next_send_cursor, cursor)
    }

    /// Whether `+FULLRESYNC replid off` must be prepended to the RDB bulk
    /// after the delay fires.
    // TODO(dead_code): reserved for future deferred-diskless-sync PSYNC path.
    #[allow(dead_code)]
    pub fn deferred_send_fullresync(&self) -> bool {
        self.deferred_send_fullresync
    }

    /// Take and clear the deferred state once the delay has been handled.
    // TODO(dead_code): reserved for future deferred-diskless-sync PSYNC path.
    #[allow(dead_code)]
    pub fn take_deferred(&mut self) -> bool {
        let was = self.diskless_ready_at_ms.take().is_some();
        self.deferred_send_fullresync = false;
        was
    }

    fn put_online(&mut self) {
        self.state = PrimaryLinkState::Streaming;
        self.full_resync_ack_pending = false;
        self.last_ack_time_ms = crate::foundation::io::sys::time::mstime();
        self.last_partial_write_sec = 0;
    }

    pub fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }

    pub(crate) fn accept_seq(&self) -> Option<u64> {
        self.accept_seq
    }

    pub(crate) fn assign_accept_seq(&mut self, seq: u64) {
        // The ordering position is stamped once when PSYNC is accepted; a
        // re-processed PSYNC frame must not reorder an admitted replica.
        if self.accept_seq.is_none() {
            self.accept_seq = Some(seq);
        }
    }

    pub fn replica(&self) -> &ReplicaConnectionInfo {
        &self.replica
    }

    pub fn set_replica_ip(&mut self, ip: String) {
        self.replica.ip = ip;
    }

    pub fn set_replica_port(&mut self, port: u16) {
        self.replica.port = port;
    }

    pub fn add_replica_capability(&mut self, capability: String) {
        self.replica.capability.push(capability);
    }

    pub fn set_replica_rdb_filter_mode(&mut self, mode: RdbFilterMode) {
        self.replica.rdb_filter_mode = mode;
    }

    pub fn replica_version(&self) -> u32 {
        self.replica_version
    }

    pub fn set_replica_version(&mut self, version: u32) {
        self.replica_version = version;
    }

    pub fn state(&self) -> PrimaryLinkState {
        self.state
    }

    pub fn dual_state(&self) -> PrimaryDualChannelState {
        self.dual_state
    }

    pub fn transition_dual_state(
        &mut self,
        next: PrimaryDualChannelState,
    ) -> Result<(), LinkError> {
        self.dual_state = self.dual_state.transition(next)?;
        Ok(())
    }

    pub fn transition_to_waiting_for_rdb_channel(&mut self) -> Result<(), LinkError> {
        if !matches!(
            self.dual_state,
            PrimaryDualChannelState::Disabled | PrimaryDualChannelState::Negotiating
        ) {
            return Err(LinkError::Protocol);
        }
        self.dual_state = self
            .dual_state
            .transition(PrimaryDualChannelState::Negotiating)?
            .transition(PrimaryDualChannelState::WaitingForRdbChannel)?;
        self.dual_endoff_boundary = None;
        self.dual_rdb_complete = false;
        self.dual_post_psync_ack_received = false;
        Ok(())
    }

    pub fn transition_to_waiting_for_main_psync(&mut self) -> Result<(), LinkError> {
        if !matches!(
            self.dual_state,
            PrimaryDualChannelState::WaitingForRdbChannel
        ) {
            return Err(LinkError::Protocol);
        }
        self.dual_state = self
            .dual_state
            .transition(PrimaryDualChannelState::WaitingForMainPsync)?;
        Ok(())
    }

    pub fn record_dual_channel_endoff_boundary(&mut self, boundary: SnapshotBoundary) {
        self.dual_endoff_boundary = Some(boundary);
    }

    pub fn clear_dual_channel_endoff_boundary(&mut self) {
        self.dual_endoff_boundary = None;
    }

    pub fn expects_dual_channel_main_psync(&self) -> bool {
        self.dual_state == PrimaryDualChannelState::WaitingForMainPsync
            && self.dual_endoff_boundary.is_some()
    }

    /// Complete the main PSYNC that follows a paired dual-channel ENDOFF.
    /// ENDOFF uses C's inclusive snapshot coordinate, so the matching wire
    /// PSYNC requests one position past it. Internal send cursors remain in the
    /// Rust half-open backlog domain and resume from the snapshot cursor.
    pub fn complete_dual_channel_main_psync(
        &mut self,
        replid: &str,
        requested: PsyncRequestOffset,
        backlog: &ReplicationBacklog,
    ) -> bool {
        let Some(boundary) = self.dual_endoff_boundary else {
            return false;
        };
        let Some(expected_request) = boundary.reported_offset().checked_next_request() else {
            return false;
        };
        if self.dual_state != PrimaryDualChannelState::WaitingForMainPsync
            || replid != backlog.replid()
            || requested != expected_request
        {
            return false;
        }
        let cursor = boundary.stream_cursor();
        if Self::enforce_cursor_invariant(cursor, cursor, backlog.next_stream_cursor()).is_err() {
            return false;
        }
        self.state = PrimaryLinkState::Streaming;
        self.last_ack_time_ms = crate::foundation::io::sys::time::mstime();
        self.next_send_cursor = cursor;
        self.sent_cursor = cursor;
        self.dual_main_resume_read_cursor = Some(cursor);
        // Once main PSYNC succeeds, the main socket owns the queued incremental
        // bytes. C removes the RDB wait-set block reference at this boundary;
        // keeping a scalar backlog floor here would retain the entire flat
        // backlog from the snapshot offset and overstate shared-buffer memory.
        self.dual_main_backlog_pin_cursor = None;
        self.dual_main_stream_started = false;
        crate::server_log!(
            debug,
            "dual backlog pin: main={} set main_pin={:?} on PSYNC",
            self.connection_id.0,
            self.dual_main_backlog_pin_cursor
        );
        self.dual_endoff_boundary = None;
        self.rdb_channel_wait_deadline_ms = None;
        self.dual_post_psync_ack_received = false;
        self.dual_state = PrimaryDualChannelState::WaitingForReplicaAck;
        if Self::enforce_cursor_invariant(
            self.sent_cursor,
            self.next_send_cursor,
            backlog.next_stream_cursor(),
        )
        .is_err()
        {
            self.state = PrimaryLinkState::Closing;
            return false;
        }
        true
    }

    fn enforce_cursor_invariant(
        sent_cursor: BacklogCursor,
        next_send_cursor: BacklogCursor,
        backlog_end: BacklogCursor,
    ) -> Result<(), LinkError> {
        let valid = sent_cursor <= next_send_cursor && next_send_cursor <= backlog_end;
        if !valid {
            return Err(LinkError::Protocol);
        }
        debug_assert!(
            sent_cursor <= next_send_cursor && next_send_cursor <= backlog_end,
            "replication cursor invariant violated: sent={sent_cursor:?} next={next_send_cursor:?} backlog_end={backlog_end:?}"
        );
        Ok(())
    }

    pub fn promote_dual_replica_on_ack(&mut self) -> bool {
        if self.dual_state != PrimaryDualChannelState::WaitingForReplicaAck
            || !self.dual_rdb_complete
            || !self.dual_post_psync_ack_received
        {
            return false;
        }
        self.put_online();
        // Match C's replicaPutOnline: ACK changes state and ACK time only.
        // Socket write completion is the sole owner of the written cursor.
        self.dual_state = PrimaryDualChannelState::Online;
        true
    }

    pub fn mark_dual_rdb_complete(&mut self) {
        self.dual_rdb_complete = true;
    }

    pub fn dual_rdb_complete(&self) -> bool {
        self.dual_rdb_complete
    }

    #[cfg(test)]
    pub fn full_resync_is_none(&self) -> bool {
        self.full_resync.is_none()
    }

    #[cfg(test)]
    pub fn set_state(&mut self, state: PrimaryLinkState) {
        self.state = state;
    }

    pub const fn last_ack_position(&self) -> Option<ReplicationOffset> {
        self.last_ack_offset
    }

    pub const fn last_aof_ack_position(&self) -> Option<ReplicationOffset> {
        self.last_aof_ack_offset
    }

    /// Lowest backlog offset this link still needs to read. While streaming,
    /// that is `next_send_cursor` (the cursor `pump` reads from). During a full
    /// resync the link resumes at `next_send_cursor` once the RDB completes, so
    /// the same value pins the backlog. Used to compute the backlog eviction
    /// floor so a lagging replica is never torn down by aggressive trimming.
    pub const fn next_send_cursor(&self) -> BacklogCursor {
        self.next_send_cursor
    }

    /// Lowest offset whose replication bytes have not yet reached the socket.
    /// `next_send_cursor` advances when bytes are queued, while this cursor
    /// advances only after the connection write consumes them; the latter is
    /// the shared-buffer reference for a slow dual-channel main.
    pub const fn sent_cursor(&self) -> BacklogCursor {
        self.sent_cursor
    }

    pub fn backlog_protection_cursor(&self) -> BacklogCursor {
        if self.has_pending_replication() {
            self.sent_cursor
        } else {
            self.next_send_cursor
        }
    }

    #[cfg(test)]
    pub fn dual_endoff_boundary(&self) -> Option<SnapshotBoundary> {
        self.dual_endoff_boundary
    }

    pub const fn dual_main_backlog_pin_cursor(&self) -> Option<BacklogCursor> {
        self.dual_main_backlog_pin_cursor
    }

    pub(crate) fn set_dual_main_backlog_pin_cursor(&mut self, cursor: Option<BacklogCursor>) {
        self.dual_main_backlog_pin_cursor = cursor;
    }

    pub fn mark_first_dual_main_stream_sent(&mut self) -> bool {
        if self.dual_state == PrimaryDualChannelState::WaitingForReplicaAck
            && !self.dual_main_stream_started
        {
            self.dual_main_stream_started = true;
            self.dual_main_resume_read_cursor = None;
            return true;
        }
        false
    }

    pub fn last_ack_time_ms(&self) -> i64 {
        self.last_ack_time_ms
    }

    pub fn last_partial_write_sec(&self) -> i64 {
        self.last_partial_write_sec
    }

    /// Mark that the replica's socket failed to accept all enqueued bytes during
    /// a full-sync transfer (back-pressure / short write). Called once per cron
    /// tick where the output queue remains non-empty after writing.
    pub fn mark_partial_write(&mut self, now_sec: i64) {
        if self.last_partial_write_sec == 0 {
            self.last_partial_write_sec = now_sec;
        }
    }

    /// Clear the partial-write stamp once the socket fully drains (output queue
    /// empty after a writable event), mirroring C's rdbPipeWriteHandlerConnRemoved.
    pub fn clear_partial_write(&mut self) {
        self.last_partial_write_sec = 0;
    }

    pub fn output_buffer_soft_limit_reached(
        &mut self,
        over_limit: bool,
        now_ms: i64,
        limit_seconds: u64,
    ) -> bool {
        OutputBufferSoftLimitReachedAt::update(
            &mut self.output_buffer_soft_limit_reached_at,
            over_limit,
            now_ms,
            limit_seconds,
        )
    }

    pub(crate) fn merge_output_buffer_soft_limit_reached_at(
        &mut self,
        reached_at: Option<OutputBufferSoftLimitReachedAt>,
    ) {
        self.output_buffer_soft_limit_reached_at =
            match (self.output_buffer_soft_limit_reached_at, reached_at) {
                (Some(main), Some(retained)) => Some(OutputBufferSoftLimitReachedAt::from_millis(
                    main.0.min(retained.0),
                )),
                (Some(main), None) => Some(main),
                (None, retained) => retained,
            };
    }
}

#[cfg(test)]
mod tests {
    use super::{OutputBufferSoftLimitReachedAt, PrimaryLink};
    use crate::engine::distributed::replication::{
        backlog::ReplicationBacklog, BacklogCursor, PsyncRequestOffset, ReplicationOffset,
        SnapshotBoundary,
    };
    use crate::framework::distributed::replication::{
        ConnectionId, LinkError, PrimaryDualChannelState, PrimaryLinkState, PrimarySyncPlan,
        ReplicaConnectionInfo,
    };

    fn link() -> PrimaryLink {
        PrimaryLink::new(
            ConnectionId(11),
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: vec!["psync2".to_string()],
                rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
            },
        )
    }

    #[test]
    fn retained_soft_limit_merge_keeps_earlier_main_timestamp() {
        let mut link = link();
        assert!(!link.output_buffer_soft_limit_reached(true, 100, 10));

        link.merge_output_buffer_soft_limit_reached_at(Some(
            OutputBufferSoftLimitReachedAt::from_millis(200),
        ));

        assert_eq!(
            link.output_buffer_soft_limit_reached_at,
            Some(OutputBufferSoftLimitReachedAt::from_millis(100))
        );
    }

    #[test]
    fn retained_soft_limit_merge_adopts_earlier_retained_timestamp() {
        let mut link = link();
        assert!(!link.output_buffer_soft_limit_reached(true, 200, 10));

        link.merge_output_buffer_soft_limit_reached_at(Some(
            OutputBufferSoftLimitReachedAt::from_millis(100),
        ));

        assert_eq!(
            link.output_buffer_soft_limit_reached_at,
            Some(OutputBufferSoftLimitReachedAt::from_millis(100))
        );
    }

    #[test]
    fn retained_soft_limit_none_does_not_clear_active_main_timestamp() {
        let mut link = link();
        assert!(!link.output_buffer_soft_limit_reached(true, 100, 10));

        link.merge_output_buffer_soft_limit_reached_at(None);

        assert_eq!(
            link.output_buffer_soft_limit_reached_at,
            Some(OutputBufferSoftLimitReachedAt::from_millis(100))
        );
    }

    #[test]
    fn retained_soft_limit_merge_keeps_both_none() {
        let mut link = link();

        link.merge_output_buffer_soft_limit_reached_at(None);

        assert_eq!(link.output_buffer_soft_limit_reached_at, None);
    }

    fn request(value: i64) -> PsyncRequestOffset {
        PsyncRequestOffset::try_from_raw(value).expect("test request must be non-negative")
    }

    fn boundary(value: usize) -> SnapshotBoundary {
        let mut backlog = ReplicationBacklog::new("boundary".to_string(), value.max(1));
        backlog.append(&vec![b'x'; value]).unwrap();
        backlog.snapshot_boundary()
    }

    fn boundary_request(boundary: SnapshotBoundary) -> PsyncRequestOffset {
        boundary
            .reported_offset()
            .checked_next_request()
            .expect("test boundary must leave room for a PSYNC request")
    }

    fn cursor(value: i64) -> BacklogCursor {
        BacklogCursor::try_from(value).expect("test cursor must be non-negative")
    }

    fn offset(value: i64) -> ReplicationOffset {
        ReplicationOffset::try_from(value).expect("valid test offset")
    }

    #[test]
    fn new_primary_link_marks_ack_offsets_unreceived() {
        // Keep the internal sentinel distinct from a real ACK at offset zero.
        // C's zero-initialized shutdown comparison is modeled at the shutdown
        // boundary without making an unacknowledged replica look healthy.
        assert_eq!(link().last_ack_position(), None);
    }

    #[test]
    fn primary_dual_state_rejects_illegal_jumps() {
        assert_eq!(
            PrimaryDualChannelState::Disabled.transition(PrimaryDualChannelState::Negotiating),
            Ok(PrimaryDualChannelState::Negotiating)
        );
        assert!(PrimaryDualChannelState::Disabled
            .transition(PrimaryDualChannelState::SendingRdb)
            .is_err());
        assert!(PrimaryDualChannelState::WaitingForRdbChannel
            .transition(PrimaryDualChannelState::Online)
            .is_err());
        assert_eq!(
            PrimaryDualChannelState::SendingRdb
                .transition(PrimaryDualChannelState::WaitingForMainPsync),
            Ok(PrimaryDualChannelState::WaitingForMainPsync)
        );
        assert_eq!(
            PrimaryDualChannelState::WaitingForMainPsync
                .transition(PrimaryDualChannelState::WaitingForReplicaAck),
            Ok(PrimaryDualChannelState::WaitingForReplicaAck)
        );
        assert_eq!(
            PrimaryDualChannelState::WaitingForReplicaAck
                .transition(PrimaryDualChannelState::Online),
            Ok(PrimaryDualChannelState::Online)
        );
    }

    #[test]
    fn paired_psync_waits_for_replica_ack_before_online() {
        let mut backlog = ReplicationBacklog::new("current".to_string(), 32);
        backlog.append(b"pending").unwrap();
        let mut link = link();
        link.transition_to_waiting_for_rdb_channel().unwrap();
        link.record_dual_channel_endoff_boundary(boundary(0));
        link.transition_to_waiting_for_main_psync().unwrap();
        link.acknowledge(offset(0), 999);

        assert!(link.complete_dual_channel_main_psync("current", request(1), &backlog));
        assert_eq!(link.next_send_cursor().get(), 0);
        link.mark_dual_rdb_complete();
        assert_eq!(link.state(), PrimaryLinkState::Streaming);
        assert_eq!(
            link.dual_state(),
            PrimaryDualChannelState::WaitingForReplicaAck
        );
        assert_eq!(
            link.dual_main_backlog_pin_cursor()
                .map(|cursor| cursor.get()),
            None
        );

        link.acknowledge(offset(7), 1_000);
        assert!(link.promote_dual_replica_on_ack());
        assert_eq!(link.dual_state(), PrimaryDualChannelState::Online);
        assert_eq!(
            link.dual_main_backlog_pin_cursor()
                .map(|cursor| cursor.get()),
            None
        );
    }

    #[test]
    fn restored_rdb_endoff_installs_captured_stream_cursor() {
        let mut backlog = ReplicationBacklog::new("local".to_string(), 64);
        backlog
            .restore_primary_from_rdb("restored", offset(41))
            .unwrap();
        let boundary = backlog.snapshot_boundary();
        let replid = backlog.replid().to_string();
        let mut link = link();
        link.transition_to_waiting_for_rdb_channel().unwrap();
        link.record_dual_channel_endoff_boundary(boundary);
        link.transition_to_waiting_for_main_psync().unwrap();

        assert!(link.complete_dual_channel_main_psync(&replid, request(42), &backlog));
        assert_eq!(link.next_send_cursor().get(), 42);
        assert_eq!(link.sent_cursor().get(), 42);

        backlog.append(b"tail").unwrap();
        assert_eq!(link.pump(&backlog), Ok(b"tail".to_vec()));
    }

    #[test]
    fn terminal_advancement_rejects_regression_and_past_end_without_mutation() {
        let mut backlog = ReplicationBacklog::new("local".to_string(), 64);
        backlog
            .restore_primary_from_rdb("restored", offset(41))
            .unwrap();
        let boundary = backlog.snapshot_boundary();
        let replid = backlog.replid().to_string();
        let mut link = link();
        link.transition_to_waiting_for_rdb_channel().unwrap();
        link.record_dual_channel_endoff_boundary(boundary);
        link.transition_to_waiting_for_main_psync().unwrap();
        assert!(link.complete_dual_channel_main_psync(&replid, request(42), &backlog));
        let appended = backlog.append(b"abc").unwrap();

        assert_eq!(
            link.advance_terminal_dual_main_stream(appended.end, backlog.next_stream_cursor()),
            Ok(true)
        );
        link.mark_replication_queued(3);
        link.mark_replication_written(3, backlog.next_stream_cursor())
            .unwrap();
        let before = (
            link.sent_cursor(),
            link.next_send_cursor(),
            link.has_pending_replication(),
        );

        assert_eq!(
            link.advance_terminal_dual_main_stream(cursor(44), backlog.next_stream_cursor()),
            Err(LinkError::Protocol)
        );
        assert_eq!(
            (
                link.sent_cursor(),
                link.next_send_cursor(),
                link.has_pending_replication(),
            ),
            before
        );
        assert_eq!(
            link.advance_terminal_dual_main_stream(cursor(46), backlog.next_stream_cursor()),
            Err(LinkError::Protocol)
        );
        assert_eq!(
            (
                link.sent_cursor(),
                link.next_send_cursor(),
                link.has_pending_replication(),
            ),
            before
        );
    }

    #[test]
    fn replication_write_overconsumption_is_rejected_without_mutation() {
        let mut backlog = ReplicationBacklog::new("local".to_string(), 64);
        backlog
            .restore_primary_from_rdb("restored", offset(41))
            .unwrap();
        let boundary = backlog.snapshot_boundary();
        let mut link = link();
        link.set_full_resync_boundary(boundary).unwrap();
        link.set_state(PrimaryLinkState::Streaming);
        backlog.append(b"ab").unwrap();
        assert_eq!(link.pump(&backlog), Ok(b"ab".to_vec()));
        link.mark_replication_queued(2);
        let before = (
            link.sent_cursor(),
            link.next_send_cursor(),
            link.has_pending_replication(),
        );

        assert_eq!(
            link.mark_replication_written(3, backlog.next_stream_cursor()),
            Err(LinkError::Protocol)
        );
        assert_eq!(
            (
                link.sent_cursor(),
                link.next_send_cursor(),
                link.has_pending_replication(),
            ),
            before
        );
    }

    #[test]
    fn replication_write_rejects_next_cursor_past_live_backlog_without_mutation() {
        let mut backlog = ReplicationBacklog::new("local".to_string(), 64);
        backlog
            .restore_primary_from_rdb("restored", offset(41))
            .unwrap();
        let boundary = backlog.snapshot_boundary();
        let mut link = link();
        link.set_full_resync_boundary(boundary).unwrap();
        link.set_state(PrimaryLinkState::Streaming);
        backlog.append(b"ab").unwrap();
        assert_eq!(link.pump(&backlog), Ok(b"ab".to_vec()));
        link.mark_replication_queued(2);
        let before = (
            link.sent_cursor(),
            link.next_send_cursor(),
            link.pending_replication_bytes,
        );

        assert_eq!(
            link.mark_replication_written(1, cursor(43)),
            Err(LinkError::Protocol)
        );
        assert_eq!(
            (
                link.sent_cursor(),
                link.next_send_cursor(),
                link.pending_replication_bytes,
            ),
            before
        );
    }

    #[test]
    fn dual_terminal_ack_pumps_snapshot_tail_when_main_stream_never_started() {
        let mut backlog = ReplicationBacklog::new("current".to_string(), 64);
        backlog.append(b"pending").unwrap();
        let mut link = link();
        link.transition_to_waiting_for_rdb_channel().unwrap();
        link.record_dual_channel_endoff_boundary(boundary(0));
        link.transition_to_waiting_for_main_psync().unwrap();

        assert!(link.complete_dual_channel_main_psync("current", request(1), &backlog));
        link.mark_dual_rdb_complete();
        link.acknowledge(offset(0), 1_000);
        assert!(link.promote_dual_replica_on_ack());

        assert_eq!(link.pump(&backlog), Ok(b"pending".to_vec()));
        assert_eq!(
            link.dual_main_backlog_pin_cursor()
                .map(|cursor| cursor.get()),
            None
        );
    }

    #[test]
    fn dual_terminal_ack_does_not_repump_already_started_main_stream() {
        let mut backlog = ReplicationBacklog::new("current".to_string(), 64);
        backlog.append(b"first").unwrap();
        let mut link = link();
        link.transition_to_waiting_for_rdb_channel().unwrap();
        link.record_dual_channel_endoff_boundary(boundary(0));
        link.transition_to_waiting_for_main_psync().unwrap();

        assert!(link.complete_dual_channel_main_psync("current", request(1), &backlog));
        let first_end = backlog.next_stream_cursor();
        assert_eq!(
            link.advance_terminal_dual_main_stream(first_end, first_end),
            Ok(true)
        );
        assert!(!link.mark_first_dual_main_stream_sent());
        backlog.append(b"second").unwrap();
        let second_end = backlog.next_stream_cursor();
        assert_eq!(
            link.advance_terminal_dual_main_stream(second_end, second_end),
            Ok(true)
        );
        assert!(!link.mark_first_dual_main_stream_sent());

        link.mark_dual_rdb_complete();
        link.acknowledge(offset(5), 1_000);
        assert!(link.promote_dual_replica_on_ack());

        assert_eq!(link.pump(&backlog), Ok(Vec::new()));
        assert_eq!(
            link.dual_main_backlog_pin_cursor()
                .map(|cursor| cursor.get()),
            None
        );
    }

    #[test]
    fn dual_terminal_ack_delayed_drain_keeps_half_open_cursor_and_backlog_valid() {
        let mut backlog = ReplicationBacklog::new("current".to_string(), 64);
        backlog.append(b"before").unwrap();
        let boundary = backlog.snapshot_boundary();
        let pending = b"pending";
        backlog.append(pending).unwrap();
        let mut link = link();
        link.transition_to_waiting_for_rdb_channel().unwrap();
        link.record_dual_channel_endoff_boundary(boundary);
        link.transition_to_waiting_for_main_psync().unwrap();

        assert!(link.complete_dual_channel_main_psync(
            "current",
            boundary_request(boundary),
            &backlog,
        ));
        let tail = link.pump(&backlog).expect("snapshot tail must be retained");
        assert_eq!(tail, pending);
        link.mark_replication_queued(tail.len());
        assert_eq!(
            link.next_send_cursor().get(),
            backlog.next_stream_cursor().get()
        );
        assert!(link.next_send_cursor().get() > link.sent_cursor().get());
        assert!(link.has_pending_replication());
        link.mark_replication_written(2, backlog.next_stream_cursor())
            .unwrap();

        link.mark_dual_rdb_complete();
        link.acknowledge(offset(backlog.reported_offset().get()), 1_000);
        assert!(link.promote_dual_replica_on_ack());
        link.mark_replication_written(tail.len() - 2, backlog.next_stream_cursor())
            .unwrap();

        assert!(!link.has_pending_replication());
        assert_eq!(link.sent_cursor().get(), link.next_send_cursor().get());

        backlog.set_min_protected_cursor(Some(link.backlog_protection_cursor()));
        backlog.set_capacity(1);
        let next = b"next";
        backlog.append(next).unwrap();
        assert_eq!(link.pump(&backlog), Ok(next.to_vec()));
    }

    #[test]
    fn dual_terminal_ack_keeps_fully_written_cursor_at_stream_end() {
        let mut backlog = ReplicationBacklog::new("current".to_string(), 64);
        backlog.append(b"pending").unwrap();
        let mut link = link();
        link.transition_to_waiting_for_rdb_channel().unwrap();
        link.record_dual_channel_endoff_boundary(boundary(0));
        link.transition_to_waiting_for_main_psync().unwrap();

        assert!(link.complete_dual_channel_main_psync("current", request(1), &backlog));
        let tail = link.pump(&backlog).expect("snapshot tail must be retained");
        link.mark_replication_queued(tail.len());
        link.mark_replication_written(tail.len(), backlog.next_stream_cursor())
            .unwrap();
        assert!(!link.has_pending_replication());

        link.mark_dual_rdb_complete();
        link.acknowledge(offset(backlog.reported_offset().get()), 1_000);
        assert!(link.promote_dual_replica_on_ack());

        assert_eq!(link.sent_cursor().get(), link.next_send_cursor().get());
        assert_eq!(
            link.backlog_protection_cursor().get(),
            link.next_send_cursor().get()
        );
    }

    #[test]
    fn dual_rdb_completion_without_post_psync_ack_does_not_promote() {
        let mut backlog = ReplicationBacklog::new("current".to_string(), 32);
        backlog.append(b"pending").unwrap();
        let mut link = link();
        link.transition_to_waiting_for_rdb_channel().unwrap();
        link.record_dual_channel_endoff_boundary(boundary(0));
        link.transition_to_waiting_for_main_psync().unwrap();

        assert!(link.complete_dual_channel_main_psync("current", request(1), &backlog));
        link.mark_dual_rdb_complete();

        assert!(!link.promote_dual_replica_on_ack());
        assert_eq!(
            link.dual_state(),
            PrimaryDualChannelState::WaitingForReplicaAck
        );
    }

    #[test]
    fn classic_primary_link_starts_with_dual_state_disabled() {
        let mut backlog = ReplicationBacklog::new("current".to_string(), 32);
        backlog.append(b"abc").unwrap();
        let mut link = link();

        assert_eq!(link.dual_state(), PrimaryDualChannelState::Disabled);
        assert_eq!(
            link.plan_full_resync(&backlog),
            PrimarySyncPlan::FullResync {
                boundary: backlog.snapshot_boundary()
            }
        );
        assert_eq!(link.state(), PrimaryLinkState::PreparingFullResync);
        assert_eq!(link.dual_state(), PrimaryDualChannelState::Disabled);
    }

    #[test]
    fn unknown_history_requires_full_resync_at_snapshot_offset() {
        let mut backlog = ReplicationBacklog::new("current".to_string(), 32);
        backlog.append(b"abc").unwrap();
        let mut link = link();

        assert_eq!(
            link.handle_psync("old", request(1), &backlog),
            PrimarySyncPlan::FullResync {
                boundary: backlog.snapshot_boundary()
            }
        );
        assert_eq!(link.state(), PrimaryLinkState::PreparingFullResync);
        assert_eq!(link.next_send_cursor.get(), 3);
    }

    #[test]
    fn restored_full_resync_plan_preserves_both_snapshot_coordinates() {
        let mut backlog = ReplicationBacklog::new("local".to_string(), 32);
        backlog
            .restore_primary_from_rdb("restored", offset(41))
            .unwrap();
        let boundary = backlog.snapshot_boundary();
        let mut link = link();

        assert_eq!(
            link.handle_psync("missing", request(42), &backlog),
            PrimarySyncPlan::FullResync { boundary }
        );
        assert_eq!(link.next_send_cursor.get(), 42);
        assert_eq!(link.sent_cursor.get(), 42);
    }

    #[test]
    fn retained_history_continues_and_pumps_exact_suffix() {
        let mut backlog = ReplicationBacklog::new("current".to_string(), 32);
        backlog.append(b"abcdef").unwrap();
        let mut link = link();

        assert_eq!(
            link.handle_psync("current", request(3), &backlog),
            PrimarySyncPlan::Continue { new_replid: None }
        );
        assert_eq!(link.state(), PrimaryLinkState::Streaming);
        assert_eq!(link.pump(&backlog), Ok(b"cdef".to_vec()));
        assert_eq!(link.next_send_cursor.get(), 6);
        assert_eq!(link.pump(&backlog), Ok(Vec::new()));
    }

    #[test]
    fn unpaired_primary_link_keeps_generic_psync_behavior() {
        let mut backlog = ReplicationBacklog::new("current".to_string(), 32);
        backlog.append(b"abcdef").unwrap();
        let mut link = link();
        link.record_dual_channel_endoff_boundary(backlog.snapshot_boundary());

        assert_eq!(
            link.handle_psync("current", request(3), &backlog),
            PrimarySyncPlan::Continue { new_replid: None }
        );
        assert_eq!(link.pump(&backlog), Ok(b"cdef".to_vec()));
    }

    #[test]
    fn diskless_full_resync_holds_snapshot_tail_until_first_ack() {
        let mut backlog = ReplicationBacklog::new("current".to_string(), 32);
        backlog.append(b"before").unwrap();
        let mut link = link();
        assert_eq!(
            link.plan_full_resync(&backlog),
            PrimarySyncPlan::FullResync {
                boundary: backlog.snapshot_boundary()
            }
        );
        backlog.append(b"after").unwrap();

        link.finish_full_resync();

        assert_eq!(link.state(), PrimaryLinkState::WaitingForReplicaAck);
        assert_eq!(link.pump(&backlog), Ok(Vec::new()));
        link.acknowledge(offset(0), 1_000);
        assert!(link.promote_full_resync_on_ack());
        assert_eq!(link.state(), PrimaryLinkState::Streaming);
        assert_eq!(link.pump(&backlog), Ok(b"after".to_vec()));
    }

    #[test]
    fn full_resync_waits_for_replica_ack_before_online() {
        let mut backlog = ReplicationBacklog::new("current".to_string(), 32);
        backlog.append(b"before").unwrap();
        let mut link = link();
        link.plan_full_resync(&backlog);
        link.finish_full_resync();

        assert_eq!(link.state(), PrimaryLinkState::WaitingForReplicaAck);
        assert!(!link.promote_full_resync_on_ack());

        link.acknowledge(offset(0), 1_000);
        assert!(link.promote_full_resync_on_ack());
        assert_eq!(link.state(), PrimaryLinkState::Streaming);
    }

    #[test]
    fn disk_based_full_resync_goes_online_without_replica_ack() {
        let mut backlog = ReplicationBacklog::new("current".to_string(), 32);
        backlog.append(b"before").unwrap();
        let mut link = link();
        link.plan_full_resync(&backlog);
        backlog.append(b"after").unwrap();

        link.finish_disk_based_full_resync();

        assert_eq!(link.state(), PrimaryLinkState::Streaming);
        assert_eq!(link.pump(&backlog), Ok(b"after".to_vec()));
        assert!(!link.promote_full_resync_on_ack());
    }

    #[test]
    fn length_framed_full_resync_streams_without_replica_ack() {
        let mut backlog = ReplicationBacklog::new("current".to_string(), 32);
        backlog.append(b"before").unwrap();
        let mut link = link();
        link.plan_full_resync(&backlog);
        backlog.append(b"after").unwrap();

        link.begin_length_framed_full_resync(b"$3\r\nrdb".to_vec(), 1, 0);
        assert_eq!(link.pump_full_resync(1_000), (b"$3\r\nrdb".to_vec(), true));
        assert_eq!(link.state(), PrimaryLinkState::Streaming);
        assert_eq!(link.pump(&backlog), Ok(b"after".to_vec()));
        assert!(!link.promote_full_resync_on_ack());
    }

    #[test]
    fn partial_resync_does_not_request_command_stream_bootstrap() {
        // C only calls replicaStartCommandStream() at the full-sync RDB-to-
        // stream boundary. A `+CONTINUE` partial resync (including an empty
        // master restored at offset zero) must keep the history untouched.
        let mut backlog = ReplicationBacklog::new("current".to_string(), 32);
        backlog.append(b"abcdef").unwrap();
        let mut link = link();

        assert_eq!(
            link.handle_psync("current", request(2), &backlog),
            PrimarySyncPlan::Continue { new_replid: None }
        );
        assert_eq!(link.state(), PrimaryLinkState::Streaming);
        assert!(!link.take_command_stream_start_pending());
    }

    #[test]
    fn classic_full_resync_requests_single_command_stream_bootstrap() {
        let mut backlog = ReplicationBacklog::new("current".to_string(), 32);
        backlog.append(b"before").unwrap();
        let mut disk_link = link();
        disk_link.handle_psync("?", request(0), &backlog);
        disk_link.finish_disk_based_full_resync();
        assert!(disk_link.take_command_stream_start_pending());
        assert!(!disk_link.take_command_stream_start_pending());

        let mut framed_link = link();
        framed_link.handle_psync("?", request(0), &backlog);
        framed_link.begin_length_framed_full_resync(b"$3\r\nrdb".to_vec(), 1, 0);
        framed_link.pump_full_resync(1_000);
        assert!(framed_link.take_command_stream_start_pending());

        let mut eof_link = link();
        eof_link.handle_psync("?", request(0), &backlog);
        eof_link.finish_full_resync();
        assert!(!eof_link.take_command_stream_start_pending());
        assert!(!eof_link.promote_full_resync_on_ack());
        eof_link.acknowledge(offset(0), 1_000);
        assert!(eof_link.promote_full_resync_on_ack());
        assert!(eof_link.take_command_stream_start_pending());
    }

    #[test]
    fn dual_channel_resync_does_not_request_command_stream_bootstrap() {
        let mut backlog = ReplicationBacklog::new("current".to_string(), 32);
        backlog.append(b"pending").unwrap();
        let mut link = link();
        link.transition_to_waiting_for_rdb_channel().unwrap();
        link.record_dual_channel_endoff_boundary(boundary(0));
        link.transition_to_waiting_for_main_psync().unwrap();

        assert!(link.complete_dual_channel_main_psync("current", request(1), &backlog));
        assert!(!link.take_command_stream_start_pending());
        link.mark_dual_rdb_complete();
        link.acknowledge(offset(7), 1_000);
        assert!(link.promote_dual_replica_on_ack());
        assert!(!link.take_command_stream_start_pending());
    }

    #[test]
    fn entering_streaming_state_refreshes_ack_time() {
        let _guard = crate::foundation::io::sys::time::enter_command_time_snapshot(12_345);
        let mut backlog = ReplicationBacklog::new("current".to_string(), 32);
        backlog.append(b"abc").unwrap();

        let mut partial = link();
        partial.handle_psync("current", request(1), &backlog);
        assert_eq!(partial.last_ack_time_ms(), 12_345);

        let mut full = link();
        full.plan_full_resync(&backlog);
        full.finish_full_resync();
        full.acknowledge(offset(backlog.reported_offset().get()), 12_345);
        assert!(full.promote_full_resync_on_ack());
        assert_eq!(full.last_ack_time_ms(), 12_345);
    }

    #[test]
    fn keepalive_is_due_only_for_idle_streaming_links() {
        let mut link = link();
        let send_at = crate::foundation::io::sys::time::mstime();
        assert!(!link.keepalive_due(send_at, 1));
        link.handle_psync(
            "current",
            request(1),
            &ReplicationBacklog::new("current".to_string(), 32),
        );
        link.finish_full_resync();
        link.acknowledge(offset(0), send_at);
        assert!(link.promote_full_resync_on_ack());
        link.mark_send_activity(send_at);
        assert!(!link.keepalive_due(send_at + 999, 1_000));
        assert!(link.keepalive_due(send_at + 1_001, 1_000));
    }

    #[test]
    fn full_resync_does_not_add_transport_delay_after_snapshot_serialization() {
        let mut backlog = ReplicationBacklog::new("current".to_string(), 32);
        backlog.append(b"before").unwrap();
        let mut link = link();
        link.plan_full_resync(&backlog);
        link.begin_full_resync(b"abcdef".to_vec(), 3, 1_000);

        assert_eq!(link.state(), PrimaryLinkState::SendingRdb);
        backlog.append(b"after").unwrap();
        assert_eq!(link.pump(&backlog), Ok(Vec::new()));
        assert_eq!(link.pump_full_resync(1_000), (b"abcdef".to_vec(), true));
        assert_eq!(link.state(), PrimaryLinkState::WaitingForReplicaAck);
        link.acknowledge(offset(6), 1_000);
        assert!(link.promote_full_resync_on_ack());
        assert_eq!(link.state(), PrimaryLinkState::Streaming);
        assert_eq!(link.pump(&backlog), Ok(b"after".to_vec()));
    }

    #[test]
    fn acknowledgements_only_move_forward() {
        let mut link = link();

        link.acknowledge(offset(9), 1000);
        link.acknowledge(offset(4), 2000);
        link.acknowledge_aof(offset(8));
        link.acknowledge_aof(offset(3));

        assert_eq!(link.last_ack_position(), Some(offset(9)));
        assert_eq!(link.last_aof_ack_position(), Some(offset(8)));
        assert_eq!(link.last_ack_time_ms(), 2000);
    }

    #[test]
    fn stale_send_cursor_closes_link() {
        let mut backlog = ReplicationBacklog::new("current".to_string(), 4);
        backlog.append(b"abcd").unwrap();
        let mut link = link();
        assert_eq!(
            link.handle_psync("current", request(1), &backlog),
            PrimarySyncPlan::Continue { new_replid: None }
        );
        backlog.append(b"efgh").unwrap();
        backlog.append(b"ijkl").unwrap();

        assert_eq!(link.pump(&backlog), Err(LinkError::BacklogEvicted));
        assert_eq!(link.state(), PrimaryLinkState::Closing);
    }

    #[test]
    fn second_replid_continues_with_new_replid_advertisement() {
        let mut bl = ReplicationBacklog::new("old-id".to_string(), 64);
        bl.append(b"abcdef").unwrap(); // cursors 0..6
        bl.shift_replication_id(); // old-id -> second_replid, PSYNC boundary = 6
        let new_id = bl.replid().to_string();
        assert_ne!(new_id, "old-id");
        bl.append(b"gh").unwrap(); // cursors 6..8 under new id

        let mut link1 = link();
        assert_eq!(
            link1.handle_psync("old-id", request(4), &bl),
            PrimarySyncPlan::Continue {
                new_replid: Some(new_id.clone())
            }
        );
        assert_eq!(link1.state(), PrimaryLinkState::Streaming);
        assert_eq!(link1.pump(&bl), Ok(b"defgh".to_vec()));

        // The cached-primary cursor at second_replid_offset is still valid;
        // the first offset beyond it must force full resync.
        let mut link2 = link();
        assert_eq!(
            link2.handle_psync("old-id", request(7), &bl),
            PrimarySyncPlan::Continue {
                new_replid: Some(new_id.clone())
            }
        );
        let mut link3 = link();
        assert_eq!(
            link3.handle_psync("old-id", request(8), &bl),
            PrimarySyncPlan::FullResync {
                boundary: bl.snapshot_boundary()
            }
        );
    }

    #[test]
    fn transition_to_waiting_for_rdb_channel_rejects_illegal_state() {
        let mut l = link();
        l.dual_state = PrimaryDualChannelState::WaitingForRdbChannel;
        assert_eq!(
            l.transition_to_waiting_for_rdb_channel(),
            Err(LinkError::Protocol)
        );
    }
}
