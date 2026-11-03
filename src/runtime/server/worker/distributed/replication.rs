use super::super::ServerWorkerState;
use crate::engine::distributed::cluster::NodeId;
use crate::engine::distributed::replication::{
    protocol as replication, BacklogCursor, ReplicationOffset, SnapshotBoundary,
};
use crate::foundation::io::transport::{Interest, WriteOutcome};
use crate::framework::capabilities::connection::Connection;
use crate::framework::capabilities::io::Token;
use crate::framework::distributed::replication::{
    AuthenticationAttempt, CloseReason, ConnectionId, MasterInfo, MasterReconnectCause,
    OutputBufferSoftLimitReachedAt, PrimaryDualChannelState, PrimaryLinkState, RdbFilterMode,
    RdbLoadMode, ReplicaAction, ReplicaId, ReplicaLink, ReplicaRdbChannelState, ReplicationAction,
    ReplicationConnection, ReplicationTickFacts,
};
use crate::runtime::server::worker::distributed::replication::disk_sync_batch::DiskSyncBatch;
use crate::runtime::server::worker::distributed::replication::diskless_child::DisklessRelay;
use crate::runtime::server::worker::reactor::blocked_events::BlockedOperation;
use crate::runtime::server::worker::reactor::sources::SourceOwner;
use std::collections::HashMap;
use std::io::Read;
use std::net::{SocketAddr, ToSocketAddrs};

mod accounting;
pub(crate) mod disk_sync_batch;
pub(crate) mod diskless_child;
mod failover;
pub(crate) mod pending_lazyfree;
mod rdb_open;
mod rdb_recv_path;
mod replica_apply;

mod timeout;

const REPLICATION_LOG_MESSAGE_MAX_BYTES: usize = 4096;

struct BoundedReplicationLog {
    output: String,
}

impl BoundedReplicationLog {
    fn new() -> Self {
        Self {
            output: String::with_capacity(REPLICATION_LOG_MESSAGE_MAX_BYTES),
        }
    }

    fn push_ascii(&mut self, value: &str) {
        let remaining = REPLICATION_LOG_MESSAGE_MAX_BYTES.saturating_sub(self.output.len());
        self.output.push_str(&value[..value.len().min(remaining)]);
    }

    fn push_sanitized(&mut self, mut value: &[u8]) {
        while !value.is_empty() && self.output.len() < REPLICATION_LOG_MESSAGE_MAX_BYTES {
            let (character, consumed) = next_lossy_character(value);
            let character = if matches!(character, '\r' | '\n') {
                ' '
            } else {
                character
            };
            if self.output.len() + character.len_utf8() > REPLICATION_LOG_MESSAGE_MAX_BYTES {
                break;
            }
            self.output.push(character);
            value = &value[consumed..];
        }
    }

    fn push_arguments(&mut self, args: &crate::engine::command::CommandArgs) {
        if args.args.is_empty() {
            return;
        }
        self.push_ascii(", with args beginning with: ");
        for (index, argument) in args.args.iter().take(8).enumerate() {
            if self.output.len() == REPLICATION_LOG_MESSAGE_MAX_BYTES {
                break;
            }
            if index != 0 {
                self.push_ascii(" ");
            }
            self.push_ascii("'");
            self.push_sanitized(argument);
            self.push_ascii("'");
        }
    }

    fn finish(self) -> String {
        self.output
    }
}

fn next_lossy_character(value: &[u8]) -> (char, usize) {
    if value[0].is_ascii() {
        return (char::from(value[0]), 1);
    }
    let probe = &value[..value.len().min(4)];
    match std::str::from_utf8(probe) {
        Ok(valid) => valid
            .chars()
            .next()
            .map(|character| (character, character.len_utf8()))
            .unwrap_or(('\u{fffd}', 1)),
        Err(error) if error.valid_up_to() > 0 => {
            let valid = std::str::from_utf8(&probe[..error.valid_up_to()]).ok();
            valid
                .and_then(|valid| valid.chars().next())
                .map(|character| (character, character.len_utf8()))
                .unwrap_or(('\u{fffd}', 1))
        }
        Err(error) => ('\u{fffd}', error.error_len().unwrap_or(probe.len()).max(1)),
    }
}

pub(crate) fn render_replication_error_for_log(
    error: &[u8],
    args: &crate::engine::command::CommandArgs,
    hide_user_data: bool,
) -> String {
    if hide_user_data {
        return "*redacted*".to_string();
    }
    let mut renderer = BoundedReplicationLog::new();
    renderer.push_sanitized(error);
    renderer.push_arguments(args);
    renderer.finish()
}

pub(crate) fn render_unknown_replication_command_for_log(
    args: &crate::engine::command::CommandArgs,
    hide_user_data: bool,
) -> String {
    if hide_user_data {
        return "*redacted*".to_string();
    }
    let mut renderer = BoundedReplicationLog::new();
    renderer.push_ascii("ERR unknown command '");
    renderer.push_sanitized(&args.name);
    renderer.push_ascii("'");
    renderer.push_arguments(args);
    renderer.finish()
}

#[derive(Default)]
pub(crate) struct FullSyncCoordinator {
    pub(crate) next_transfer_id: u64,
    pub(crate) pending_dual_rdb_snapshots: HashMap<Token, (i64, Vec<u8>)>,
    /// One fork pipe streaming an RDB to a cohort of EOF-capable replicas.
    pub(crate) diskless_relay: Option<DisklessRelay>,
    /// A shared disk snapshot served to independent replica cursors.
    pub(crate) disk_sync_batch: Option<DiskSyncBatch>,
    pub(crate) disk_snapshot_options:
        Option<crate::foundation::io::persistence::rdb::compression::RdbSnapshotOptions>,
    pub(crate) disk_snapshot_version: u32,
}

fn resolve_master_addr(host: &str, port: u16) -> Option<SocketAddr> {
    let mut addrs = (host, port).to_socket_addrs().ok()?;
    let mut fallback = None;
    for addr in &mut addrs {
        if addr.is_ipv4() {
            return Some(addr);
        }
        fallback.get_or_insert(addr);
    }
    fallback
}

fn replica_aof_ack_offset(
    ctx: &crate::framework::context::ServerContext,
    _applied_offset: ReplicationOffset,
) -> Option<ReplicationOffset> {
    if !ctx.config().persistence.aof_enabled || ctx.aof().journal.is_none() {
        return None;
    }
    // FACK always reports the two-phase confirmed fsynced offset, never the
    // merely-applied offset. Mirrors C's `replicationSendAck` sending
    // `server.fsynced_reploff`. `None` (C's `-1`) suppresses the FACK field.
    ctx.aof()
        .fsynced_repl_offset
        .and_then(|offset| ReplicationOffset::try_from(offset).ok())
}

fn min_protected_cursor(
    left: Option<BacklogCursor>,
    right: Option<BacklogCursor>,
) -> Option<BacklogCursor> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(cursor), None) | (None, Some(cursor)) => Some(cursor),
        (None, None) => None,
    }
}

impl ServerWorkerState {
    pub(super) fn account_replica_wire_input(&mut self, link: &mut ReplicaLink) {
        self.ctx
            .stats_mut()
            .add_net_repl_input_bytes(link.take_wire_input_bytes());
    }

    pub(crate) fn replication_snapshot_options(
        &self,
        connection: ConnectionId,
    ) -> Option<crate::foundation::io::persistence::rdb::compression::RdbSnapshotOptions> {
        let config = self.ctx.config();
        self.replication_runtime.full_sync_snapshot_options(
            connection,
            &config.replication.repl_compression,
            config.persistence.rdbcompression,
            config.persistence.rdbchecksum,
        )
    }

    pub(super) fn replica_rdb_load_mode(&self) -> RdbLoadMode {
        match self.ctx.config().replication.repl_diskless_load.as_str() {
            "swapdb" => RdbLoadMode::Swap,
            "flush-before-load" | "on" => RdbLoadMode::Direct,
            "on-empty-db" => {
                if (0..self.ctx.num_dbs() as u32).any(|db_id| self.ctx.db_size(db_id) != 0) {
                    RdbLoadMode::DiskBacked
                } else {
                    RdbLoadMode::Direct
                }
            }
            _ => RdbLoadMode::DiskBacked,
        }
    }

    pub(super) fn pin_replica_rdb_load_mode(&self, link: &mut ReplicaLink) {
        link.prepare_rdb_snapshot_options(
            crate::foundation::io::persistence::rdb::compression::RdbSnapshotOptions {
                compression: self.ctx.config().persistence.rdbcompression,
                checksum: self.ctx.config().persistence.rdbchecksum,
            },
            !self.ctx.config().persistence.rdbchecksum
                || self.ctx.config().server.skip_checksum_validation,
        );
        if link.rdb_load_mode().is_none() {
            link.pin_rdb_load_mode(self.replica_rdb_load_mode());
        }
    }

    fn schedule_master_reconnect(&mut self, cause: MasterReconnectCause) {
        self.replication_runtime
            .schedule_master_reconnect(crate::foundation::io::sys::time::mstime(), cause);
    }

    fn should_abort_master_link_for_cluster_failure(&self) -> bool {
        let Some(link) = self.replication_runtime.replica_link() else {
            return false;
        };
        if !(link.is_receiving_rdb() || self.ctx.replication().async_loading) {
            return false;
        }
        let node_timeout_ms = self.ctx.config().server.cluster_node_timeout as i64;
        if node_timeout_ms <= 0 {
            return false;
        }
        let io_stalled = crate::foundation::io::sys::time::mstime()
            .saturating_sub(link.last_io_time_ms())
            >= node_timeout_ms;
        if !io_stalled {
            return false;
        }
        let Some(state) = self.ctx.cluster_state().as_ref() else {
            return false;
        };
        if !state.myself.is_replica() || state.myself.replicaof.is_empty() {
            return false;
        }
        let Some(primary) = state.nodes.get(&state.myself.replicaof) else {
            return false;
        };
        primary.ip == link.master().host
            && primary.port == link.master().port
            && primary.has_flag(
                crate::engine::distributed::cluster::node::CLUSTER_NODE_FAIL
                    | crate::engine::distributed::cluster::node::CLUSTER_NODE_PFAIL,
            )
    }

    fn master_link_timed_out(&self) -> bool {
        let Some(link) = self.replication_runtime.replica_link() else {
            return false;
        };
        // C switches the main socket to the dual-channel handshake while the
        // RDB socket receives REPLCONF/ENDOFF. During that interval the main
        // socket is intentionally quiet, so its normal I/O deadline must not
        // tear down the pair before the RDB channel can drive the session.
        if !matches!(link.rdb_channel_state(), ReplicaRdbChannelState::None) {
            return false;
        }
        let timeout_sec = self.ctx.config().replication.repl_timeout;
        if timeout_sec == 0 {
            return false;
        }
        let deadline = link
            .last_io_time_ms()
            .saturating_add((timeout_sec as i64).saturating_mul(1000));
        crate::foundation::io::sys::time::mstime() >= deadline
    }

    pub(crate) fn propagate_to_replicas(&mut self, frame: &[u8]) {
        self.propagate_to_replicas_with_ack(frame, false);
    }

    pub(crate) fn propagate_replication_frame(&mut self, frame: &[u8]) {
        self.propagate_to_replicas_with_ack(frame, false);
    }

    pub(crate) fn propagate_to_replicas_with_ack(&mut self, frame: &[u8], request_ack: bool) {
        // C's feedReplicationBuffer() is a no-op until a replication backlog
        // exists. A pre-PSYNC connection is not yet a replica and must not
        // start or advance replication history merely by registering a link.
        if !self.ctx.replication().backlog.is_active() {
            return;
        }
        let backlog_capacity = self.ctx.config().server.repl_backlog_size.max(0) as usize;
        let first_frame_cursor = self.ctx.replication().backlog.next_stream_cursor();
        for rdb in self
            .replication_runtime
            .backfill_waiting_psync_pins(first_frame_cursor)
        {
            crate::server_log!(
                debug,
                "Attach replica rdb client {} to repl buf block",
                rdb.0
            );
        }
        let should_request_ack = self
            .replication_runtime
            .primary_links()
            .values()
            .any(|link| link.replica().port != 0);
        // Pin the eviction floor to the lowest offset a connected replica still
        // needs *before* trimming to `repl-backlog-size`. Otherwise a tiny
        // `repl-backlog-size` (the shutdown test sets 1 byte) evicts bytes a
        // live replica has not yet drained, its `pump` sees `BacklogEvicted`,
        // and the link is torn down in a loop. Mirrors C Valkey holding
        // replication-buffer blocks referenced by lagging replicas.
        // C backfills RDB clients that entered the wait-PSYNC set with an
        // empty replication buffer as soon as the first block is created.
        let delayed_snapshot_without_child = !self.full_sync.pending_dual_rdb_snapshots.is_empty()
            && self.full_sync.diskless_relay.is_none();
        let mut protected = if delayed_snapshot_without_child {
            self.replication_runtime
                .min_protected_backlog_cursor_without_psync_wait()
        } else {
            self.replication_runtime.min_protected_backlog_cursor()
        };
        if delayed_snapshot_without_child {
            // While the diskless-sync delay is still pending, the eventual RDB
            // snapshot has not captured the dataset yet. Retain only the normal
            // backlog window; once the child starts, its ENDOFF pin protects the
            // pre-snapshot tail for the slow replica.
            let backlog_end = self.ctx.replication().backlog.next_stream_cursor();
            let trim_floor = backlog_end
                .checked_retreat(backlog_capacity)
                .unwrap_or(BacklogCursor::ZERO);
            protected = protected.map(|cursor| cursor.max(trim_floor));
        }
        protected = min_protected_cursor(protected, self.active_replication_transfer_floor());
        self.ctx
            .replication_mut()
            .backlog
            .set_min_protected_cursor(protected);
        self.ctx
            .replication_mut()
            .backlog
            .set_capacity(backlog_capacity);
        let protected_after_backfill = if delayed_snapshot_without_child {
            self.replication_runtime
                .min_protected_backlog_cursor_without_psync_wait()
        } else {
            self.replication_runtime.min_protected_backlog_cursor()
        };
        let protected_after_backfill = min_protected_cursor(
            protected_after_backfill,
            self.active_replication_transfer_floor(),
        );
        self.ctx
            .replication_mut()
            .backlog
            .set_min_protected_cursor(protected_after_backfill);
        let getack = (request_ack && should_request_ack)
            .then(|| replication::encode_command(&[b"REPLCONF" as &[u8], b"GETACK", b"*"]));
        let terminal_dual_frame = {
            let mut bytes = frame.to_vec();
            if let Some(getack) = &getack {
                bytes.extend_from_slice(getack);
            }
            bytes
        };
        let appended = match self
            .ctx
            .replication_mut()
            .backlog
            .append(&terminal_dual_frame)
        {
            Ok(appended) => appended,
            Err(_) => {
                crate::server_log!(
                    warning,
                    "Replication backlog cursor overflow; requesting server shutdown"
                );
                self.ctx.set_shutdown_requested(true);
                return;
            }
        };
        debug_assert_eq!(appended.start, first_frame_cursor);
        let backlog_end = self.ctx.replication().backlog.next_stream_cursor();
        let ids: Vec<_> = self
            .replication_runtime
            .primary_links()
            .keys()
            .copied()
            .collect();
        let mut stale_connections = Vec::new();
        for id in ids {
            let pumped = {
                let backlog = &self.ctx.replication().backlog;
                let Some(link) = self.replication_runtime.primary_link_mut(id) else {
                    continue;
                };
                let connection_id = link.connection_id();
                match link.advance_terminal_dual_main_stream(appended.end, backlog_end) {
                    Ok(true) => {
                        link.mark_replication_queued(terminal_dual_frame.len());
                        Ok((connection_id, terminal_dual_frame.clone()))
                    }
                    Ok(false) => link.pump(backlog).map(|bytes| {
                        link.mark_replication_queued(bytes.len());
                        (connection_id, bytes)
                    }),
                    Err(error) => Err(error),
                }
            };
            let (connection_id, bytes) = match pumped {
                Ok(pumped) => pumped,
                Err(_) => {
                    if let Some(link) = self.replication_runtime.primary_link(id) {
                        stale_connections.push(link.connection_id().0);
                    }
                    continue;
                }
            };
            let Some(connection_token) = self
                .connection_registry
                .contains(crate::framework::capabilities::io::Token(connection_id.0))
                .then_some(connection_id.0)
            else {
                continue;
            };
            let token = self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .token();
            if !bytes.is_empty() {
                self.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)]
                .queue_replication_reply(bytes);
                if let Some(link) = self.replication_runtime.primary_link_mut(id) {
                    link.mark_first_dual_main_stream_sent();
                    link.mark_send_activity(crate::foundation::io::sys::time::mstime());
                }
            }
            self.transport
                .set_interest(token, Interest::ReadWritable)
                .ok();
        }
        for token_value in stale_connections {
            self.remove_connection(token_value);
        }
        self.enforce_replica_output_buffer_limits();
        self.refresh_replica_output_buffer_bytes();
    }

    /// Run the command-stream start side effects that C performs in
    /// `replicaStartCommandStream()` (replication.c): a classic full resync
    /// has just handed a replica from the RDB stream to the incremental
    /// command stream, so an empty replication history is seeded with one
    /// PING. Cluster health reports the replica online immediately instead
    /// of waiting for the first keepalive.
    ///
    /// Partial PSYNC and dual-channel main-PSYNC resumes reuse the existing
    /// history through the `+CONTINUE` path, where C never calls
    /// `replicaStartCommandStream()`; their links must not invoke this. The
    /// zero-offset guard also makes the seed global (at most one PING per
    /// empty history) and preserves an offset restored as zero from RDB aux
    /// fields after an empty-master restart.
    pub(crate) fn start_replica_command_stream(&mut self) {
        if self.ctx.config().replication.master_host.is_some()
            || self.ctx.replication().is_replica()
            || !self.ctx.replication().backlog.is_active()
            || self.ctx.replication().backlog.reported_offset() != ReplicationOffset::ZERO
        {
            return;
        }
        let ping = replication::encode_command(&[b"PING"]);
        self.propagate_replication_frame(&ping);
    }

    pub(crate) fn send_replication_keepalives(&mut self) {
        // C's replicationFeedReplicas() returns immediately on a node with an
        // upstream primary. Chained replicas receive the upstream's identical
        // stream; only a top-level primary generates offset-bearing PINGs.
        if self.ctx.config().replication.master_host.is_some()
            || self.ctx.replication().is_replica()
        {
            return;
        }
        let timeout_sec = self.ctx.config().replication.repl_timeout;
        if timeout_sec == 0 {
            return;
        }
        let now = crate::foundation::io::sys::time::mstime();
        let interval_ms = (self
            .ctx
            .config()
            .replication
            .repl_ping_replica_period
            .saturating_mul(1_000) as i64)
            .max(1);
        let ping = replication::encode_command(&[b"PING"]);
        let due_ids: Vec<_> = self
            .replication_runtime
            .primary_links()
            .keys()
            .copied()
            .collect();
        let due_ids: Vec<_> = due_ids
            .into_iter()
            .filter(|id| {
                self.replication_runtime
                    .primary_link(*id)
                    .is_some_and(|link| link.keepalive_due(now, interval_ms))
            })
            .collect();
        if !due_ids.is_empty() {
            // Keepalives are replication-stream bytes, not out-of-band socket
            // writes. C feeds the PING into the shared backlog first, so every
            // replica (including chained replicas) advances the same cursor.
            self.propagate_replication_frame(&ping);
            for id in due_ids {
                if let Some(link) = self.replication_runtime.primary_link_mut(id) {
                    link.mark_send_activity(now);
                }
            }
        }
    }

    pub(crate) fn pump_full_resync_transfers(&mut self) {
        self.pump_compressed_replication_batches();
        let now = crate::foundation::io::sys::time::mstime();
        let ids: Vec<_> = self
            .replication_runtime
            .primary_links()
            .keys()
            .copied()
            .collect();
        for id in ids {
            let (connection_id, rdb_bytes, is_done) = {
                let Some(link) = self.replication_runtime.primary_link_mut(id) else {
                    continue;
                };
                let connection_id = link.connection_id();
                let mut bytes = Vec::new();
                let mut transfer_done = false;
                loop {
                    let (chunk, done) = link.pump_full_resync(now);
                    if chunk.is_empty() {
                        break;
                    }
                    bytes.extend_from_slice(&chunk);
                    if done {
                        transfer_done = true;
                        break;
                    }
                }
                (connection_id, bytes, transfer_done)
            };
            let tail = if is_done {
                let pumped = {
                    let backlog = &self.ctx.replication().backlog;
                    self.replication_runtime
                        .primary_link_mut(id)
                        .expect("primary link exists while completing full resync")
                        .pump(backlog)
                };
                let tail = match pumped {
                    Ok(tail) => tail,
                    Err(_) => {
                        self.remove_connection(connection_id.0);
                        self.refresh_replication_view();
                        continue;
                    }
                };
                if !tail.is_empty() {
                    if let Some(link) = self.replication_runtime.primary_link_mut(id) {
                        link.mark_replication_queued(tail.len());
                    }
                }
                tail
            } else {
                Vec::new()
            };
            if rdb_bytes.is_empty() && tail.is_empty() {
                continue;
            }
            let Some(connection_token) = self
                .connection_registry
                .contains(crate::framework::capabilities::io::Token(connection_id.0))
                .then_some(connection_id.0)
            else {
                continue;
            };
            let token = self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .token();
            let connection = &mut self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)];
            if !rdb_bytes.is_empty() {
                connection.queue_reply(rdb_bytes);
            }
            if !tail.is_empty() {
                connection.queue_replication_reply(tail);
            }
            self.transport
                .set_interest(token, Interest::ReadWritable)
                .ok();
        }
    }

    fn pump_compressed_replication_batches(&mut self) {
        let ids: Vec<_> = self
            .replication_runtime
            .primary_links()
            .keys()
            .copied()
            .collect();
        for id in ids {
            let result = {
                let Some(link) = self.replication_runtime.primary_link_mut(id) else {
                    continue;
                };
                if !link.is_compressed() || link.has_pending_replication() {
                    continue;
                }
                let connection_id = link.connection_id();
                link.pump(&self.ctx.replication().backlog).map(|bytes| {
                    link.mark_replication_queued(bytes.len());
                    (connection_id, bytes)
                })
            };
            match result {
                Ok((connection_id, bytes)) if !bytes.is_empty() => {
                    let token = crate::framework::capabilities::io::Token(connection_id.0);
                    if self.connection_registry.contains(token) {
                        self.connection_registry[token].queue_replication_reply(bytes);
                        let _ = self.transport.set_interest(token, Interest::ReadWritable);
                    }
                }
                Err(_) => {
                    if let Some(link) = self.replication_runtime.primary_link(id) {
                        self.remove_connection(link.connection_id().0);
                    }
                }
                _ => {}
            }
        }
    }

    pub(crate) fn handle_master_link_readable(
        &mut self,
        token_value: usize,
        connection_token: usize,
    ) {
        if matches!(
            self.blocked_events.current(),
            Some(BlockedOperation::DualRdb { main, .. }) if main.0 == token_value
        ) {
            let _ = self.recv_dual_main_while_rdb_loading(token_value, connection_token);
            self.clear_accepted_failover();
            return;
        }

        // Route the handshake and the full-sync RDB body through the bounded-read
        // streaming path so a paced (`key-load-delay`) decode throttles socket
        // reads and back-pressures the primary (G2r-2 B). The pre-RDB handshake
        // lines share the socket buffer with the RDB prefix, so draining there
        // would slurp the whole available RDB and defeat back-pressure — hence the
        // bounded path owns everything up to (and including) the RDB phase. Only
        // the pure post-RDB command stream uses the classic drain loop.
        let stream_only = self
            .replication_runtime
            .replica_link()
            .is_some_and(|l| l.is_streaming() && !l.has_pending_rdb_receive());
        if stream_only {
            self.recv_replication_stream(token_value, connection_token);
        } else {
            self.recv_streaming_rdb_chunk(token_value, connection_token);
        }
        self.clear_accepted_failover();
    }

    #[allow(dead_code)]
    pub(crate) fn handle_rdb_channel_readable(
        &mut self,
        _token_value: usize,
        connection_token: usize,
    ) {
        let mut read_data = Vec::new();
        let mut close = false;
        {
            let conn = &self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)];
            if let Some(mut stream) = self.transport.stream(conn.token()) {
                match crate::foundation::io::sys::os::retry_on_interrupt(|| {
                    stream.read(&mut self.io_buf)
                }) {
                    Ok(0) => close = true,
                    Ok(n) => read_data.extend_from_slice(&self.io_buf[..n]),
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(_) => close = true,
                }
            }
        }
        if let Some(rdb_link) = self.replication_runtime.replica_rdb_link_mut() {
            rdb_link.push_bytes(&read_data);
        }

        if close {
            if let Some(link) = self.replication_runtime.replica_link_mut() {
                let _ = link.transition_rdb_channel_state(ReplicaRdbChannelState::Aborting);
                let _ = link.transition_rdb_channel_state(ReplicaRdbChannelState::None);
            }
            self.replication_runtime.take_replica_rdb_link();
            crate::server_log!(
                warning,
                "RDB channel closed during $ENDOFF handshake, waiting for primary timeout fallback"
            );
            return;
        }

        let rdb_bytes = self
            .replication_runtime
            .replica_rdb_link_mut()
            .map(|rdb| std::mem::take(&mut rdb.input))
            .unwrap_or_default();

        if rdb_bytes.is_empty() {
            return;
        }

        let Some(mut link) = self.replication_runtime.take_replica_link() else {
            return;
        };
        let main_connection_id = link.connection_id();
        let Some(main_connection_token) = self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(
                main_connection_id.0,
            ))
            .then_some(main_connection_id.0)
        else {
            self.replication_runtime.set_replica_link(link);
            return;
        };

        let previous_state = link.rdb_channel_state();
        let decoded = link.on_rdb_bytes(&rdb_bytes);
        self.account_replica_wire_input(&mut link);
        match decoded {
            Ok((_events, actions)) => {
                self.send_dual_rdb_sync_after_handshake(connection_token, previous_state, &link);
                self.replication_runtime.set_replica_link(link);
                self.queue_replica_actions(main_connection_token, actions);
            }
            Err(error) => {
                let _ = link.transition_rdb_channel_state(ReplicaRdbChannelState::Aborting);
                let _ = link.transition_rdb_channel_state(ReplicaRdbChannelState::None);
                self.replication_runtime.set_replica_link(link);
                crate::server_log!(
                    warning,
                    "Malformed $ENDOFF frame on RDB channel (error={:?}), \
                     waiting for primary timeout fallback",
                    error
                );
            }
        }
    }

    pub(super) fn send_dual_rdb_sync_after_handshake(
        &mut self,
        connection_token: usize,
        previous_state: ReplicaRdbChannelState,
        link: &ReplicaLink,
    ) {
        if previous_state != ReplicaRdbChannelState::Handshake
            || link.rdb_channel_state() != ReplicaRdbChannelState::WaitingForEndOffset
        {
            return;
        }
        let token = Token(connection_token);
        self.connection_registry[token].queue_reply(
            crate::engine::distributed::replication::protocol::encode_command(&[b"SYNC"]),
        );
        self.transport
            .set_interest(token, Interest::ReadWritable)
            .ok();
    }

    /// Classic drain-to-`WouldBlock` read of the pure command stream (post-RDB
    /// `Streaming` phase) lives in `rdb_recv_path::recv_replication_stream`,
    /// alongside the bounded RDB path it dispatches to.
    pub(crate) fn apply_deferred_replica_events(&mut self) {
        let paused = self
            .ctx
            .client_pause()
            .is_paused(crate::foundation::io::sys::time::mstime())
            != crate::framework::context::PauseAction::None
            || self.ctx.persistence().forkless_capture_in_progress()
            || self.ctx.module_registry().busy_reply.is_some();
        if paused {
            return;
        }
        let Some(mut link) = self.replication_runtime.take_replica_link() else {
            return;
        };
        let events = link.take_deferred_events();
        let connection_id = link.connection_id();
        if events.is_empty() {
            let Some(connection_token) = self
                .connection_registry
                .contains(crate::framework::capabilities::io::Token(connection_id.0))
                .then_some(connection_id.0)
            else {
                self.replication_runtime.set_replica_link(link);
                return;
            };
            if matches!(link.rdb_channel_state(), ReplicaRdbChannelState::RdbLoaded)
                && self
                    .commit_and_replay_dual_handoff_only(connection_token, &mut link)
                    .is_err()
            {
                self.ctx.replication_mut().async_loading = false;
                self.ctx.set_loading(false);
                self.ctx.config_mut().replication.master_link_status = "down".to_string();
                self.remove_connection(connection_id.0);
                return;
            }
            self.replication_runtime.set_replica_link(link);
            return;
        }
        let Some(connection_token) = self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(connection_id.0))
            .then_some(connection_id.0)
        else {
            self.replication_runtime.set_replica_link(link);
            return;
        };
        if self
            .apply_replica_events(connection_token, &mut link, events)
            .is_err()
        {
            self.ctx.replication_mut().async_loading = false;
            self.ctx.set_loading(false);
            self.ctx.config_mut().replication.master_link_status = "down".to_string();
            self.remove_connection(connection_id.0);
            return;
        }
        if matches!(link.rdb_channel_state(), ReplicaRdbChannelState::RdbLoaded)
            && self
                .commit_and_replay_dual_handoff_only(connection_token, &mut link)
                .is_err()
        {
            self.ctx.replication_mut().async_loading = false;
            self.ctx.set_loading(false);
            self.ctx.config_mut().replication.master_link_status = "down".to_string();
            self.remove_connection(connection_id.0);
            return;
        }
        self.ctx.replication_mut().slave_repl_offset = link.applied_position();
        self.ctx.replication_mut().slave_read_repl_offset = link.received_position();
        if !link.is_dual_rdb_mode_active()
            && self.resume_replica_main_reads(connection_token).is_err()
        {
            self.ctx.config_mut().replication.master_link_status = "down".to_string();
            self.remove_connection(connection_id.0);
            return;
        }
        self.replication_runtime.set_replica_link(link);
    }

    /// Returns how many grace-held wait-PSYNC members were released. C derives
    /// `replicas_waiting_psync` from the live rax so it is always current; the
    /// Rust equivalent is cached, hence callers must refresh the view when
    /// this returns non-zero.
    pub(crate) fn expire_grace_psync_waits(&mut self) -> usize {
        let now = crate::foundation::io::sys::time::mstime();
        let expired_grace = self.replication_runtime.take_expired_grace_psync_waits(now);
        for main in &expired_grace {
            crate::server_log!(
                debug,
                "Replica main channel failed to establish PSYNC within the grace period. Freeing RDB client {}.",
                main.0
            );
        }
        let expired_retained = self
            .replication_runtime
            .take_expired_retained_rdb_psync_waits(now);
        for (public_client_id, main) in &expired_retained {
            if let Some(main) = main {
                crate::server_log!(
                    debug,
                    "Replica main channel failed to establish PSYNC within the grace period. Freeing RDB client {}.",
                    main.0
                );
                self.remove_connection(main.0);
            } else {
                crate::server_log!(
                    debug,
                    "Retained RDB client {} failed to establish PSYNC within the grace period.",
                    public_client_id
                );
            }
        }
        expired_grace.len() + expired_retained.len()
    }
}

struct ConnectUpstreamArgs {
    host: String,
    port: u16,
    addr: std::net::SocketAddr,
    bind_source_addr: Option<std::net::IpAddr>,
    use_mptcp: bool,
    use_tls: bool,
    dual_channel_enabled: bool,
    failover_in_progress: bool,
    cluster_node_id: Option<String>,
    force_full_sync: bool,
    force_full_sync_from_reconfigure: bool,
    resume: Option<(String, ReplicationOffset, u32)>,
}

impl ServerWorkerState {
    pub(crate) fn drive_replication(&mut self) {
        self.drive_replication_with_cluster_node_id(None);
    }

    pub(crate) fn drive_cluster_replication(&mut self, cluster_node_id: &str) {
        self.drive_replication_with_cluster_node_id(Some(cluster_node_id));
    }

    fn drive_replication_with_cluster_node_id(&mut self, cluster_node_id: Option<&str>) {
        let released_grace_waits = self.expire_grace_psync_waits();
        let mut facts = self.build_replication_tick_facts(released_grace_waits);
        if let Some(cluster_node_id) = cluster_node_id {
            facts.cluster_node_id = Some(cluster_node_id.to_string());
        }
        let actions = self.replication_runtime.plan_tick(&facts);
        for action in actions {
            self.execute_replication_action(action);
        }
    }

    fn build_replication_tick_facts(&self, released_grace_waits: usize) -> ReplicationTickFacts {
        let now_ms = crate::foundation::io::sys::time::mstime();
        let master_host = self.ctx.config().replication.master_host.clone();
        let master_port = self.ctx.config().replication.master_port;
        let authentication = self.current_authentication_attempt();
        let auth_rejected = self
            .replication_runtime
            .authentication_is_rejected(&authentication);
        let abort_for_cluster_failure = self.should_abort_master_link_for_cluster_failure();
        let link_timed_out = self.master_link_timed_out();

        let bind_source_addr = match self.ctx.config().server.bind_source_addr.as_str() {
            "" => None,
            configured => match configured.parse::<std::net::IpAddr>() {
                Ok(address) => Some(address),
                Err(error) => {
                    crate::server_log!(
                        warning,
                        "Invalid bind-source-addr {}: {}",
                        configured,
                        error
                    );
                    None
                }
            },
        };
        let bind_source_addr_invalid =
            !self.ctx.config().server.bind_source_addr.is_empty() && bind_source_addr.is_none();

        let resolved_master_addr = master_host
            .as_ref()
            .and_then(|h| resolve_master_addr(h, master_port));

        ReplicationTickFacts {
            now_ms,
            master_host,
            master_port,
            master_link_status: self.ctx.config().replication.master_link_status.clone(),
            auth_rejected,
            abort_for_cluster_failure,
            link_timed_out,
            force_full_sync_on_next_reconnect: self
                .ctx
                .replication()
                .force_full_sync_on_next_reconnect,
            dual_channel_replication: self.ctx.config().replication.dual_channel_replication,
            failover_in_progress: self.ctx.replication().is_failover_in_progress(),
            cluster_node_id: self
                .ctx
                .cluster_state()
                .as_ref()
                .map(|state| state.myself.name.clone()),
            bind_source_addr,
            bind_source_addr_invalid,
            repl_mptcp: self.ctx.config().replication.repl_mptcp,
            tls_replication: self.ctx.config().server.tls_replication,
            pending_replicaof: self.ctx.replication().pending_replicaof_request,
            resolved_master_addr,
            grace_waits_released: released_grace_waits > 0,
        }
    }

    fn execute_replication_action(&mut self, action: ReplicationAction) {
        match action {
            ReplicationAction::RefreshView => self.refresh_replication_view(),
            ReplicationAction::Close {
                connection_id,
                reason,
            } => self.execute_close(connection_id, reason),
            ReplicationAction::CloseReplicaLinks => {
                if let Some(link) = self.replication_runtime.replica_link() {
                    self.remove_connection(link.connection_id().0);
                }
                if let Some(link) = self.replication_runtime.take_replica_rdb_link() {
                    self.remove_connection(link.connection_id().0);
                }
            }
            ReplicationAction::ScheduleReconnect(cause) => {
                self.ctx.config_mut().replication.master_link_status = "down".to_string();
                self.schedule_master_reconnect(cause);
            }
            ReplicationAction::StartFullResync {
                connection_id,
                primary_id,
            } => self.execute_dual_channel_full_resync(connection_id, primary_id),
            ReplicationAction::ApplyPendingReplicaof => self.apply_pending_replicaof_request(),
            ReplicationAction::ConnectUpstream {
                host,
                port,
                addr,
                bind_source_addr,
                use_mptcp,
                use_tls,
                dual_channel_enabled,
                failover_in_progress,
                cluster_node_id,
                force_full_sync,
                force_full_sync_from_reconfigure,
                resume,
            } => self.execute_connect_upstream(ConnectUpstreamArgs {
                host,
                port,
                addr,
                bind_source_addr,
                use_mptcp,
                use_tls,
                dual_channel_enabled,
                failover_in_progress,
                cluster_node_id,
                force_full_sync,
                force_full_sync_from_reconfigure,
                resume,
            }),
        }
    }

    fn execute_close(&mut self, connection_id: ConnectionId, reason: CloseReason) {
        match reason {
            CloseReason::DualChannelPsyncGrace => {
                crate::server_log!(
                    notice,
                    "Replica main channel failed to establish PSYNC within the grace period. Freeing RDB client {}.",
                    connection_id.0
                );
            }
            CloseReason::ClusterFailure { host, port } => {
                crate::server_log!(
                    warning,
                    "MASTER {}:{} marked down by cluster while full sync is in progress",
                    host,
                    port
                );
            }
            CloseReason::Timeout {
                host,
                port,
                in_handshake,
            } => {
                if in_handshake {
                    crate::server_log!(
                        warning,
                        "Timeout connecting to the PRIMARY {}:{}, setting replication link down",
                        host,
                        port
                    );
                } else {
                    crate::server_log!(
                        warning,
                        "MASTER timeout while receiving replication stream from {}:{}",
                        host,
                        port
                    );
                }
            }
            CloseReason::StaleLink => {}
        }
        self.remove_connection(connection_id.0);
    }

    fn execute_dual_channel_full_resync(&mut self, conn_id: ConnectionId, primary_id: ReplicaId) {
        if !self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(conn_id.0))
        {
            return;
        }
        let connection_token = conn_id.0;
        let boundary = self.ctx.replication().backlog.snapshot_boundary();
        let filter_mode = {
            let Some(link) = self.replication_runtime.primary_link_mut(primary_id) else {
                return;
            };
            if link.dual_state() == PrimaryDualChannelState::WaitingForRdbChannel {
                if link
                    .transition_dual_state(PrimaryDualChannelState::Aborting)
                    .and_then(|_| link.transition_dual_state(PrimaryDualChannelState::Disabled))
                    .is_err()
                {
                    return;
                }
            } else if link.dual_state() != PrimaryDualChannelState::Disabled {
                return;
            }
            link.rdb_channel_wait_deadline_ms = None;
            link.dual_channel_already_failed = true;
            link.clear_dual_channel_endoff_boundary();
            if link.set_full_resync_boundary(boundary).is_err() {
                None
            } else {
                Some(link.replica().rdb_filter_mode)
            }
        };
        let Some(filter_mode) = filter_mode else {
            self.remove_connection(connection_token);
            return;
        };
        let now = crate::foundation::io::sys::time::mstime();
        let replid = self.ctx.replication().backlog.replid().to_string();
        self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .queue_reply(
                format!(
                    "+FULLRESYNC {} {}\r\n",
                    replid,
                    boundary.reported_offset().get()
                )
                .into_bytes(),
            );
        let function_codes: Vec<Vec<u8>> = if filter_mode == RdbFilterMode::Empty {
            Vec::new()
        } else {
            self.ctx
                .script()
                .function_registry
                .list_libraries()
                .into_iter()
                .map(|library| library.code.as_bytes().to_vec())
                .collect()
        };
        let replication = self.ctx.rdb_full_sync_replication_info();
        let slot_imports = self.ctx.slot_import_dbs();
        let rdb = crate::foundation::io::persistence::serialize_all_with_functions_and_replication_with_options(
            self.ctx.dbs(),
            self.ctx.config().persistence.rdb_key_save_delay,
            &function_codes,
            filter_mode == RdbFilterMode::All,
            Some(&replication),
            &slot_imports,
            crate::foundation::io::persistence::RdbSerializeOptions {
                compression: self.ctx.config().persistence.rdbcompression.uses_lzf(),
                checksum: true,
            },
        );
        let total_keys = (0..self.ctx.num_dbs() as u32)
            .map(|db| self.ctx.db_size(db))
            .sum::<usize>();
        let bulk = {
            let mut bulk = format!("${}\r\n", rdb.len()).into_bytes();
            bulk.extend_from_slice(&rdb);
            bulk
        };
        if let Some(link) = self.replication_runtime.primary_link_mut(primary_id) {
            link.begin_full_resync(bulk, total_keys.max(1), now);
            let (first_chunk, _) = link.pump_full_resync(now);
            if !first_chunk.is_empty() {
                self.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)]
                .queue_reply(first_chunk);
            }
        }
        if self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .has_pending_output()
        {
            let token = self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .token();
            self.transport
                .set_interest(token, Interest::ReadWritable)
                .ok();
        }
    }

    fn execute_connect_upstream(&mut self, args: ConnectUpstreamArgs) {
        let addr = args.addr;
        let addr_str = format!("{}:{}", args.host, args.port);
        let connect_result = if args.use_tls {
            if args.use_mptcp {
                Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "tls-replication cannot use MPTCP transport",
                ))
            } else {
                self.configure_tls_client_from_config().and_then(|_| {
                    self.transport.connect_tls_bound(
                        addr,
                        args.bind_source_addr,
                        args.host.as_str(),
                    )
                })
            }
        } else if args.use_mptcp {
            self.transport
                .connect_mptcp_bound(addr, args.bind_source_addr)
        } else {
            self.transport
                .connect_tcp_bound(addr, args.bind_source_addr)
        }
        .and_then(|handle| {
            self.route_runtime_source(handle.token, SourceOwner::Connection)?;
            Ok(handle)
        });
        match connect_result {
            Ok(handle) => {
                let conn = Connection::new(handle.token, addr, false);
                self.connection_registry
                    .register(conn)
                    .expect("a newly connected primary transport token must be unique");
                self.prioritize_connection(handle.token);
                let master = MasterInfo {
                    host: args.host.clone(),
                    port: args.port,
                };
                let mut link = ReplicaLink::new(ConnectionId(handle.token.0), master.clone());
                link.set_failover_request(args.failover_in_progress);
                link.set_cluster_node_id(args.cluster_node_id.clone());
                link.set_dual_channel_enabled(args.dual_channel_enabled);
                link.set_compression_mode(&self.ctx.config().replication.repl_compression);
                if args.force_full_sync_from_reconfigure {
                    self.ctx.replication_mut().force_full_sync_on_next_reconnect = false;
                    self.replication_runtime.clear_replica_resume();
                }
                if let Some((replid, offset, selected_db)) = args.resume.clone() {
                    crate::server_log!(
                        debug,
                        "dual-rdb reconnect using cached resume: replid={} offset={} db={}",
                        replid,
                        offset.get(),
                        selected_db
                    );
                    link.set_resume_position(replid, offset, selected_db);
                    self.connection_registry
                        [crate::framework::capabilities::io::Token(handle.token.0)]
                    .selected_db = selected_db;
                } else {
                    crate::server_log!(
                        debug,
                        "dual-rdb reconnect has no cached resume: force_full_sync={}",
                        args.force_full_sync
                    );
                }
                self.replication_runtime.set_replica_link(link);
                self.ctx.config_mut().replication.master_link_status = "connect".to_string();
                crate::server_log!(debug, "Connecting to PRIMARY {}", addr_str);
            }
            Err(e) => {
                crate::server_log!(warning, "Unable to connect to PRIMARY {}: {}", addr_str, e);
                self.ctx.config_mut().replication.master_link_status = "down".to_string();
                self.schedule_master_reconnect(MasterReconnectCause::ConnectFailed);
            }
        }
    }

    /// Apply a replica target change before its reply reaches the caller.
    pub(crate) fn apply_pending_replicaof_request(&mut self) {
        if !self.ctx.replication().pending_replicaof_request {
            return;
        }
        self.ctx.replication_mut().pending_replicaof_request = false;
        self.abort_replica_session_for_role_change();
        if !self.ctx.replication().is_replica() {
            self.replication_runtime.clear_replica_resume();
        }
        if self.ctx.replication().is_replica()
            && (self.ctx.replication().throttle.is_active()
                || !self.ctx.throttled_clients().is_empty())
        {
            self.ctx.replication_mut().force_full_sync_on_next_reconnect = true;
        }
        self.schedule_master_reconnect(MasterReconnectCause::ExplicitTargetChange);
        if self.ctx.replication().cache_primary_on_next_reconnect
            && !self.ctx.replication().force_full_sync_on_next_reconnect
        {
            let master = crate::framework::distributed::replication::MasterInfo {
                host: self
                    .ctx
                    .config()
                    .replication
                    .master_host
                    .clone()
                    .unwrap_or_default(),
                port: self.ctx.config().replication.master_port,
            };
            let replid = self.ctx.replication().backlog.replid().to_string();
            let second_replid = self.ctx.replication().backlog.second_replid();
            let current_reported_offset = self.ctx.replication().backlog.reported_offset();
            let cached_is_local_history = self
                .replication_runtime
                .replica_resume_for(&master)
                .is_some_and(|(cached_replid, cached_offset, _)| {
                    cached_replid == replid
                        || (!second_replid.is_empty()
                            && cached_replid == second_replid
                            && cached_offset == current_reported_offset)
                });
            let dual_channel = self.ctx.config().replication.dual_channel_replication;
            if dual_channel || !cached_is_local_history {
                // A stale cache from an unrelated earlier topology must be
                // replaced with this primary's current history, matching C's
                // discard + replicationCachePrimaryUsingMyself sequence. A
                // classic cached cursor matching replid2 is still the valid
                // former-upstream position for a direct target change.
                let selected_db = self.ctx.repl_seldb().max(0) as u32;
                self.replication_runtime.set_replica_resume(
                    master,
                    replid,
                    current_reported_offset,
                    selected_db,
                );
                self.ctx.replication_mut().force_full_sync_on_next_reconnect = false;
            }
            self.ctx.replication_mut().cache_primary_on_next_reconnect = false;
        }
        if self
            .ctx
            .replication()
            .rebind_replica_resume_on_next_reconnect
        {
            let master = crate::framework::distributed::replication::MasterInfo {
                host: self
                    .ctx
                    .config()
                    .replication
                    .master_host
                    .clone()
                    .unwrap_or_default(),
                port: self.ctx.config().replication.master_port,
            };
            self.replication_runtime.rebind_replica_resume(master);
            self.ctx
                .replication_mut()
                .rebind_replica_resume_on_next_reconnect = false;
        }
        if self.ctx.replication().force_full_sync_on_next_reconnect {
            self.replication_runtime.clear_replica_resume();
        }
        if self.ctx.replication().is_replica()
            && self.ctx.config().replication.master_host.is_some()
        {
            self.drive_replication();
        }
    }

    /// Mirror C `replicationUnsetPrimary` / `replicationSetPrimary`: discard
    /// both sides of a dual sync before the new role becomes externally
    /// visible. Keep the runtime links registered until `remove_connection`
    /// has isolated the whole pair, otherwise the main close is treated as an
    /// unrelated client close and can leave the RDB loading gate behind.
    pub(crate) fn abort_replica_session_for_role_change(&mut self) {
        let main = self
            .replication_runtime
            .replica_link()
            .map(|link| link.connection_id());
        let rdb = self
            .replication_runtime
            .replica_rdb_link()
            .map(|link| link.connection_id());

        self.take_and_dispose_replica_pending_buffer();
        self.replication_runtime.set_replica_main_read_paused(false);
        self.replication_runtime.clear_rejected_authentication();
        self.ctx.replication_mut().async_loading = false;
        self.ctx.set_loading(false);
        self.ctx.config_mut().replication.master_link_status = "down".to_string();

        for connection_id in [main, rdb].into_iter().flatten() {
            if self
                .connection_registry
                .contains(crate::framework::capabilities::io::Token(connection_id.0))
            {
                self.remove_connection(connection_id.0);
            }
        }
        let pending = self.replication_runtime.clear_replica_session();
        self.dispose_replica_pending_buffer(pending);
    }

    pub(crate) fn drive_pending_dual_rdb_snapshots(&mut self) {
        if self.ctx.persistence().has_active_child() || self.full_sync.diskless_relay.is_some() {
            return;
        }
        let now = crate::foundation::io::sys::time::mstime();
        // Select one compatible filter cohort before publishing any ENDOFF.
        // Capture its boundary in the same turn that starts the snapshot.
        let Some((filter_mode, earliest_deadline_ms, first_connection)) = self
            .full_sync
            .pending_dual_rdb_snapshots
            .iter()
            .filter(|(token, _)| {
                self.connection_registry.contains(**token)
                    && matches!(
                        self.replication_runtime.role(ConnectionId(token.0)),
                        Some(
                            ReplicationConnection::RdbChannelCandidate
                                | ReplicationConnection::RdbChannel { .. }
                        )
                    )
            })
            .min_by_key(|(token, (ready_at, _))| (*ready_at, token.0))
            .map(|(token, (ready_at, _))| {
                (
                    self.replication_runtime
                        .replica_sync_preferences(ConnectionId(token.0))
                        .0,
                    *ready_at,
                    ConnectionId(token.0),
                )
            })
        else {
            return;
        };
        let snapshot = self.replication_snapshot_options(first_connection);
        let snapshot_version = self
            .replication_runtime
            .replica_snapshot_version(first_connection);
        let ready: Vec<crate::framework::capabilities::io::Token> = self
            .full_sync
            .pending_dual_rdb_snapshots
            .keys()
            .filter(|token| {
                self.connection_registry.contains(**token)
                    && matches!(
                        self.replication_runtime.role(ConnectionId(token.0)),
                        Some(
                            ReplicationConnection::RdbChannelCandidate
                                | ReplicationConnection::RdbChannel { .. }
                        )
                    )
                    && self
                        .replication_runtime
                        .replica_sync_preferences(ConnectionId(token.0))
                        .0
                        == filter_mode
                    && self.replication_snapshot_options(ConnectionId(token.0)) == snapshot
                    && self
                        .replication_runtime
                        .replica_snapshot_version(ConnectionId(token.0))
                        == snapshot_version
            })
            .copied()
            .collect();
        let config = &self.ctx.config().replication;
        let max_replicas = config.repl_diskless_sync_max_replicas.max(0) as usize;
        let should_start = !config.repl_diskless_sync
            || config.repl_diskless_sync_delay == 0
            || (max_replicas > 0 && ready.len() >= max_replicas)
            || earliest_deadline_ms <= now;
        if !should_start {
            return;
        }
        let mut endoff_candidates = Vec::new();
        for token in ready {
            let token_value = token.0;
            let rdb_connection_id =
                crate::framework::distributed::replication::ConnectionId(token_value);
            let role = self.replication_runtime.role(rdb_connection_id);
            crate::server_log!(
                debug,
                "dual-rdb boundary: pending snapshot due rdb={} role={:?} paired_main={:?}",
                token_value,
                role,
                self.replication_runtime
                    .channel_pair_for(rdb_connection_id)
                    .map(|id| id.0)
            );
            match role {
                Some(ReplicationConnection::RdbChannelCandidate) => {
                    let Some((_, sent_endoff)) =
                        self.full_sync.pending_dual_rdb_snapshots.get(&token)
                    else {
                        continue;
                    };
                    if !sent_endoff.is_empty() || !self.connection_registry.contains(token) {
                        continue;
                    }
                    endoff_candidates.push(rdb_connection_id);
                }
                Some(ReplicationConnection::RdbChannel { .. }) => {}
                _ => continue,
            }
        }

        let endoff_frames = if endoff_candidates.is_empty() {
            Vec::new()
        } else {
            let snapshot_db = self.ctx.repl_seldb().max(0) as u32;
            let ServerWorkerState {
                ref mut replication_runtime,
                ref connection_registry,
                ref mut ctx,
                ..
            } = self;
            replication_runtime.take_endoff_for_rdb_cohort(
                &endoff_candidates,
                |connection_id| {
                    connection_registry
                        .get(crate::framework::capabilities::io::Token(connection_id.0))
                        .map(|connection| connection.client_id())
                },
                &mut ctx.replication_mut().backlog,
                snapshot_db,
            )
        };
        self.refresh_replication_view();
        for (rdb_connection_id, frame) in endoff_frames {
            let Some(connection_token) = self
                .connection_registry
                .contains(crate::framework::capabilities::io::Token(
                    rdb_connection_id.0,
                ))
                .then_some(rdb_connection_id.0)
            else {
                continue;
            };
            let client_id = self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .client_id();
            let frame = crate::runtime::server::worker::sessions::transport::replconf::rewrite_endoff_client_id(
                frame, client_id,
            );
            self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
                .queue_reply(frame);
            if matches!(
                self.flush_connection_output(connection_token),
                WriteOutcome::Complete
            ) {
                if let Some((_, sent_endoff)) = self
                    .full_sync
                    .pending_dual_rdb_snapshots
                    .get_mut(&crate::framework::capabilities::io::Token(connection_token))
                {
                    sent_endoff.push(1);
                }
            } else {
                self.remove_connection(rdb_connection_id.0);
            }
        }
        let has_candidate_waiting_for_endoff_flush = self
            .full_sync
            .pending_dual_rdb_snapshots
            .iter()
            .any(|(token, (_, sent_endoff))| {
                let connection_id =
                    crate::framework::distributed::replication::ConnectionId(token.0);
                self.replication_runtime
                    .replica_sync_preferences(connection_id)
                    .0
                    == filter_mode
                    && matches!(
                        self.replication_runtime.role(connection_id),
                        Some(ReplicationConnection::RdbChannelCandidate)
                    )
                    && sent_endoff.is_empty()
                    && self.replication_snapshot_options(connection_id) == snapshot
                    && self
                        .replication_runtime
                        .replica_snapshot_version(connection_id)
                        == snapshot_version
            });
        if has_candidate_waiting_for_endoff_flush {
            return;
        }
        let paired_ready: Vec<_> = self
            .full_sync
            .pending_dual_rdb_snapshots
            .iter()
            .filter_map(|(&token, (_, sent_endoff))| {
                let connection_id =
                    crate::framework::distributed::replication::ConnectionId(token.0);
                if self
                    .replication_runtime
                    .replica_sync_preferences(connection_id)
                    .0
                    != filter_mode
                    || self.replication_snapshot_options(connection_id) != snapshot
                    || self
                        .replication_runtime
                        .replica_snapshot_version(connection_id)
                        != snapshot_version
                {
                    return None;
                }
                let can_snapshot = matches!(
                    self.replication_runtime.role(connection_id),
                    Some(ReplicationConnection::RdbChannel { .. })
                ) || matches!(
                    self.replication_runtime.role(connection_id),
                    Some(ReplicationConnection::RdbChannelCandidate)
                ) && !sent_endoff.is_empty();
                can_snapshot.then_some((token, connection_id))
            })
            .collect();
        let mut fork_targets = Vec::with_capacity(paired_ready.len());
        for (token, rdb_connection_id) in paired_ready {
            if self
                .full_sync
                .pending_dual_rdb_snapshots
                .remove(&token)
                .is_some()
            {
                fork_targets.push(rdb_connection_id);
            }
        }
        if fork_targets.is_empty() {
            return;
        }

        let function_codes = if filter_mode == RdbFilterMode::Empty {
            Vec::new()
        } else {
            self.ctx
                .script()
                .function_registry
                .list_libraries()
                .into_iter()
                .map(|library| library.code.as_bytes().to_vec())
                .collect()
        };
        let Some(snapshot) = snapshot else {
            for rdb_connection_id in fork_targets {
                self.remove_connection(rdb_connection_id.0);
            }
            return;
        };
        let Some((read_end, marker, release)) = self.try_begin_diskless_fork_with_options(
            &function_codes,
            snapshot,
            "dual-channel",
            false,
        ) else {
            for rdb_connection_id in fork_targets {
                self.remove_connection(rdb_connection_id.0);
            }
            return;
        };
        let mut header = b"$EOF:".to_vec();
        header.extend_from_slice(&marker);
        header.extend_from_slice(b"\r\n");
        for rdb_connection_id in &fork_targets {
            if let Some(connection) = self.connection_registry.get_mut(Token(rdb_connection_id.0)) {
                connection.queue_reply(header.clone());
            }
        }
        for rdb_connection_id in &fork_targets {
            if !matches!(
                self.flush_connection_output(rdb_connection_id.0),
                WriteOutcome::Complete
            ) {
                self.remove_connection(rdb_connection_id.0);
            }
        }
        self.store_diskless_relay(read_end, release, marker, Vec::new(), false, 0);
    }

    pub(crate) fn is_replica_connection(&self, token_value: usize) -> bool {
        matches!(
            self.replication_runtime.role(ConnectionId(token_value)),
            Some(ReplicationConnection::Primary(_))
        )
    }

    pub(crate) fn is_master_link(&self, token_value: usize) -> bool {
        matches!(
            self.replication_runtime.role(ConnectionId(token_value)),
            Some(ReplicationConnection::Replica)
        )
    }

    pub(crate) fn handle_master_link_connected(&mut self, connection_token: usize) {
        let primary_user = self.ctx.config().security.primaryuser.clone();
        let primary_auth = self.ctx.config().security.primaryauth.clone();
        let actions = self
            .replication_runtime
            .replica_link_mut()
            .map(|link| link.connected(primary_user.as_deref(), primary_auth.as_deref()))
            .unwrap_or_default();
        self.queue_replica_actions(connection_token, actions);
    }

    fn queue_replica_actions(&mut self, connection_token: usize, actions: Vec<ReplicaAction>) {
        let token = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .token();
        let mut main_has_output = false;
        for action in actions {
            match action {
                ReplicaAction::Send(frame) => {
                    if frame.starts_with(b"*3\r\n$5\r\nPSYNC\r\n")
                        || frame.starts_with(b"*4\r\n$5\r\nPSYNC\r\n")
                    {
                        self.pause_before_replica_psync();
                    }
                    if frame
                        .windows(b"set-rdb-client-id".len())
                        .any(|window| window.eq_ignore_ascii_case(b"set-rdb-client-id"))
                    {
                        crate::server_log!(
                            debug,
                            "dual-rdb main handshake: queued REPLCONF set-rdb-client-id main_token={} bytes={}",
                            token.0,
                            frame.len()
                        );
                    }
                    self.connection_registry
                        [crate::framework::capabilities::io::Token(connection_token)]
                    .queue_reply(frame);
                    main_has_output = true;
                }
                ReplicaAction::SendDualPsync(frame) => {
                    self.pause_before_replica_psync();
                    if self.consume_debug_pause_after_fork() {
                        crate::server_log!(
                            debug,
                            "dual-rdb replica main PSYNC pause: token={} phase=before-sigstop bytes={}",
                            token.0,
                            frame.len()
                        );
                        let _ = crate::framework::capabilities::process::stop_current_process();
                        crate::server_log!(
                            debug,
                            "dual-rdb replica main PSYNC pause: token={} phase=after-sigcont",
                            token.0
                        );
                    }
                    crate::server_log!(
                        debug,
                        "dual-rdb replica main PSYNC queued: token={} bytes={}",
                        token.0,
                        frame.len()
                    );
                    self.connection_registry
                        [crate::framework::capabilities::io::Token(connection_token)]
                    .queue_reply(frame);
                    // Submit the resumed PSYNC before another ready RDB read
                    // can observe EOF and tear down the paired main socket.
                    self.handle_writable(token);
                    if !self.connection_registry.contains(token) {
                        return;
                    }
                    main_has_output = true;
                }
                ReplicaAction::OpenRdbChannel { initial_frames } => {
                    if let Err(error) = self.open_replica_rdb_channel(initial_frames) {
                        if let Some(link) = self.replication_runtime.replica_link_mut() {
                            let _ =
                                link.transition_rdb_channel_state(ReplicaRdbChannelState::Aborting);
                            let _ = link.transition_rdb_channel_state(ReplicaRdbChannelState::None);
                        }
                        crate::server_log!(
                            warning,
                            "Failed to open RDB channel for dual-channel sync at {}: {}, \
                             waiting for primary timeout fallback",
                            error.stage(),
                            error
                        );
                    }
                }
            }
        }
        if main_has_output
            && self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
                .has_pending_output()
        {
            self.transport
                .set_interest(token, Interest::ReadWritable)
                .ok();
        }
    }

    fn pause_before_replica_psync(&mut self) {
        if std::mem::take(&mut self.ctx.config_mut().server.debug_pause_before_psync) {
            let _ = crate::framework::capabilities::process::stop_current_process();
        }
    }

    fn current_authentication_attempt(&self) -> AuthenticationAttempt {
        AuthenticationAttempt {
            host: self
                .ctx
                .config()
                .replication
                .master_host
                .clone()
                .unwrap_or_default(),
            port: self.ctx.config().replication.master_port,
            user: self.ctx.config().security.primaryuser.clone(),
            password: self.ctx.config().security.primaryauth.clone(),
        }
    }

    /// Cron hook for deferred diskless full-resyncs.  Drains links whose
    /// diskless delay has elapsed and materializes their RDB snapshot.
    pub(crate) fn drive_deferred_full_resyncs(&mut self) {
        let now = crate::foundation::io::sys::time::mstime();
        let ids = self
            .replication_runtime
            .drain_ready_deferred_links(now, false);
        if ids.is_empty() {
            return;
        }
        self.fire_deferred_full_resyncs(ids);
    }

    /// Start one codec/version/filter cohort while leaving other deferred links
    /// queued for their own child round.
    pub(crate) fn fire_deferred_full_resyncs(
        &mut self,
        mut ids: Vec<crate::framework::distributed::replication::ReplicaId>,
    ) {
        if self.ctx.persistence().has_active_child() {
            // Apply to cron and max-replicas alike: only keyless cohorts can
            // progress while the All child is running or waiting for ACK.
            ids.retain(|id| {
                self.replication_runtime
                    .primary_link(*id)
                    .is_some_and(|link| link.replica().rdb_filter_mode != RdbFilterMode::All)
            });
        }
        let Some(first_connection) = ids.iter().find_map(|id| {
            self.replication_runtime
                .primary_link(*id)
                .map(|link| link.connection_id())
        }) else {
            return;
        };
        let Some(snapshot) = self.replication_snapshot_options(first_connection) else {
            self.remove_connection(first_connection.0);
            return;
        };
        let cohort_filter = self
            .replication_runtime
            .replica_sync_preferences(first_connection)
            .0;
        let cohort_version = self
            .replication_runtime
            .replica_snapshot_version(first_connection);
        let ids: Vec<_> = ids
            .into_iter()
            .filter(|id| {
                self.replication_runtime
                    .primary_link(*id)
                    .is_some_and(|link| {
                        let connection = link.connection_id();
                        self.replication_snapshot_options(connection) == Some(snapshot)
                            && self
                                .replication_runtime
                                .replica_sync_preferences(connection)
                                .0
                                == cohort_filter
                            && self
                                .replication_runtime
                                .replica_snapshot_version(connection)
                                == cohort_version
                    })
            })
            .collect();
        for &id in &ids {
            if let Some(connection) = self
                .replication_runtime
                .primary_link(id)
                .map(|link| link.connection_id())
            {
                self.replication_runtime
                    .freeze_full_sync_snapshot_options(connection, snapshot);
            }
        }
        self.fire_full_resync_cohort(ids, snapshot);
    }

    /// Serialize RDB snapshots for the given replica ids and enqueue their
    /// full-resync transfers.  Shared between the cron-driven delay path and
    /// the `repl-diskless-sync-max-replicas` short-circuit path.
    fn fire_full_resync_cohort(
        &mut self,
        ids: Vec<crate::framework::distributed::replication::ReplicaId>,
        snapshot: crate::foundation::io::persistence::rdb::compression::RdbSnapshotOptions,
    ) {
        if ids.is_empty() {
            return;
        }
        let key_delay_us = self.ctx.config().persistence.rdb_key_save_delay;
        let now = crate::foundation::io::sys::time::mstime();
        let libs: Vec<Vec<u8>> = self
            .ctx
            .script()
            .function_registry
            .list_libraries()
            .iter()
            .map(|l| l.code.as_bytes().to_vec())
            .collect();
        // The shared RDB reflects the backlog state at fire time, not at the
        // earlier PSYNC. The event loop admits no writes between this atomic
        // boundary capture and serialization below.
        let fire_boundary: SnapshotBoundary = self.ctx.replication().backlog.snapshot_boundary();
        let fire_offset = fire_boundary.reported_offset().get();
        let total_keys = (0..self.ctx.num_dbs() as u32)
            .map(|db| self.ctx.db_size(db))
            .sum::<usize>();
        let replication = self.ctx.rdb_full_sync_replication_info();

        // Per-batch fork decision (single child serves every EOF-capable replica
        // in this batch — fork-once + multi-fd broadcast, aligning with C's
        // diskless full-sync). A fork failure or an already-active persistence
        // child falls back to today's in-process serialization for the batch.
        let batch_is_diskless = self.ctx.config().replication.repl_diskless_sync
            || (snapshot.compression
                == crate::foundation::io::persistence::rdb::compression::RdbCompression::Lz4)
                != (self.ctx.config().persistence.rdbcompression
                    == crate::foundation::io::persistence::rdb::compression::RdbCompression::Lz4);
        if !batch_is_diskless
            && ids.iter().all(|id| {
                self.replication_runtime
                    .primary_link(*id)
                    .is_some_and(|link| link.replica().rdb_filter_mode == RdbFilterMode::All)
            })
        {
            if let Some(&first) = ids.first() {
                if let Some(boundary) = self.begin_disk_sync_batch(first, &libs) {
                    for id in ids {
                        if id != first && self.join_disk_sync_batch(id).is_none() {
                            continue;
                        }
                        let Some(link) = self.replication_runtime.primary_link_mut(id) else {
                            continue;
                        };
                        let connection = link.connection_id();
                        let send_header = link.deferred_send_fullresync();
                        link.take_deferred();
                        if send_header {
                            let header = format!(
                                "+FULLRESYNC {} {}\r\n",
                                self.ctx.replication().backlog.replid(),
                                boundary.reported_offset().get()
                            );
                            self.queue_client_reply(Token(connection.0), header.into_bytes());
                        }
                    }
                    self.refresh_replication_view();
                    return;
                }
            }
        }
        let has_all_target = ids.iter().any(|id| {
            self.replication_runtime
                .primary_link(*id)
                .is_some_and(|link| {
                    let (mode, eof) = self
                        .replication_runtime
                        .replica_sync_preferences(link.connection_id());
                    mode == RdbFilterMode::All && eof
                })
        });
        let fork = if batch_is_diskless && has_all_target {
            self.try_begin_diskless_fork_with_options(&libs, snapshot, "normal sync", true)
        } else {
            None
        };
        let (fork_read_end, fork_marker, fork_release) = match fork {
            Some((read_end, marker, release)) => (Some(read_end), Some(marker), Some(release)),
            None => (None, None, None),
        };
        let forked = fork_read_end.is_some();

        // Shared in-process RDB for the fallback / non-EOF / non-diskless paths.
        // Serialized lazily: skipped entirely when the whole batch forks.
        let mut in_process_rdb: Option<std::rc::Rc<Vec<u8>>> = None;
        let mut relay_targets: Vec<crate::framework::distributed::replication::ReplicaId> =
            Vec::new();

        for id in ids {
            let (send_fullresync, connection_id, boundary_installed) = {
                let Some(link) = self.replication_runtime.primary_link_mut(id) else {
                    continue;
                };
                let send_fullresync = link.deferred_send_fullresync();
                let connection_id = link.connection_id();
                link.take_deferred();
                let boundary_installed = link.set_full_resync_boundary(fire_boundary).is_ok();
                (send_fullresync, connection_id, boundary_installed)
            };
            if !boundary_installed {
                self.remove_connection(connection_id.0);
                continue;
            }
            let Some(connection_token) = self
                .connection_registry
                .contains(crate::framework::capabilities::io::Token(connection_id.0))
                .then_some(connection_id.0)
            else {
                continue;
            };
            let token = self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .token();
            let filter_mode = self
                .replication_runtime
                .replica_sync_preferences(connection_id)
                .0;
            let supports_eof = self
                .replication_runtime
                .primary_link(id)
                .and_then(|l| {
                    l.replica()
                        .capability
                        .iter()
                        .find(|c| c.eq_ignore_ascii_case("eof"))
                })
                .is_some();

            if forked && supports_eof && filter_mode == RdbFilterMode::All {
                // Fork/relay path: enqueue only the header now. The RDB body
                // streams from the child's pipe in `service_diskless_relay_source`, which
                // also sends the trailing marker and drives the completion state
                // transition (finish_full_resync + backlog tail). Do NOT call
                // begin_full_resync here — the link stays in PreparingFullResync
                // (so pump/keepalives stay quiet) until the relay completes it.
                let marker = fork_marker.as_ref().expect("fork marker set when forked");
                let mut header: Vec<u8> = Vec::new();
                if send_fullresync {
                    let replid = self.ctx.replication().backlog.replid().to_string();
                    header.extend_from_slice(
                        format!("+FULLRESYNC {} {}\r\n", replid, fire_offset).as_bytes(),
                    );
                }
                header.extend_from_slice(b"$EOF:");
                header.extend_from_slice(marker);
                header.extend_from_slice(b"\r\n");
                self.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)]
                .queue_reply(header);
                self.transport
                    .set_interest(token, Interest::ReadWritable)
                    .ok();
                relay_targets.push(id);
                continue;
            }

            // In-process path: classic `$<len>` bulk, or in-process `$EOF` when
            // the batch did not fork (fork failed / child active / not diskless).
            let filtered_rdb;
            let rdb = if filter_mode != RdbFilterMode::All {
                let slot_imports = self.ctx.slot_import_dbs();
                let functions = if filter_mode == RdbFilterMode::Empty {
                    &[][..]
                } else {
                    libs.as_slice()
                };
                filtered_rdb =
                    crate::foundation::io::persistence::rdb::serialize::serialize_replication_snapshot(
                        self.ctx.dbs(),
                        key_delay_us,
                        functions,
                        false,
                        Some(&replication),
                        &slot_imports,
                        snapshot,
                    );
                filtered_rdb.as_slice()
            } else {
                in_process_rdb.get_or_insert_with(|| {
                let slot_imports = self.ctx.slot_import_dbs();
                std::rc::Rc::new(
                    crate::foundation::io::persistence::rdb::serialize::serialize_replication_snapshot(
                        self.ctx.dbs(),
                        key_delay_us,
                        &libs,
                        true,
                        Some(&replication),
                        &slot_imports,
                        snapshot,
                    ),
                )
                }).as_slice()
            };
            let mut bytes: Vec<u8> = Vec::new();
            if send_fullresync {
                let replid = self.ctx.replication().backlog.replid().to_string();
                bytes.extend_from_slice(
                    format!("+FULLRESYNC {} {}\r\n", replid, fire_offset).as_bytes(),
                );
            }
            let uses_eof = supports_eof && (batch_is_diskless || filter_mode != RdbFilterMode::All);
            if uses_eof {
                let marker = crate::engine::distributed::replication::protocol::generate_replid();
                bytes.extend_from_slice(format!("$EOF:{}\r\n", marker).as_bytes());
                bytes.extend_from_slice(rdb);
                bytes.extend_from_slice(marker.as_bytes());
            } else {
                bytes.extend_from_slice(format!("${}\r\n", rdb.len()).as_bytes());
                bytes.extend_from_slice(rdb);
            }
            let link = self
                .replication_runtime
                .primary_link_mut(id)
                .expect("primary link exists while firing deferred full resync");
            if uses_eof {
                link.begin_full_resync(bytes, total_keys.max(1), now);
            } else {
                link.begin_length_framed_full_resync(bytes, total_keys.max(1), now);
            }
            let (first_chunk, _) = link.pump_full_resync(now);
            if !first_chunk.is_empty() {
                self.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)]
                .queue_reply(first_chunk);
                self.transport
                    .set_interest(token, Interest::ReadWritable)
                    .ok();
            }
        }

        if let (Some(read_end), Some(marker), Some(release)) =
            (fork_read_end, fork_marker, fork_release)
        {
            self.store_diskless_relay(read_end, release, marker, relay_targets, true, 0);
        }
        // The replica restarts the command stream in DB 0 after installing the
        // RDB.  Writes accepted during the diskless delay may have moved the
        // primary stream DB after PSYNC first reset it, so invalidate it again
        // at the actual snapshot boundary.  This mirrors C Valkey's
        // replicationSetupReplicaForFullResync(), which forces the first
        // post-BGSAVE write to emit SELECT.
        self.ctx.reset_repl_seldb();
        self.refresh_replication_view();
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod overflow_tests {
    use super::ServerWorkerState;
    use crate::engine::distributed::replication::ReplicationOffset;

    #[test]
    fn propagation_cursor_overflow_requests_shutdown_without_publishing_bytes() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        runtime
            .ctx
            .replication_mut()
            .backlog
            .restore_primary_from_rdb(
                "persisted",
                ReplicationOffset::try_from(i64::MAX - 1).unwrap(),
            )
            .expect("the final representable cursor is valid");
        let boundary = runtime.ctx.replication().backlog.snapshot_boundary();

        runtime.propagate_replication_frame(b"x");

        assert!(runtime.ctx.is_shutdown_requested());
        assert_eq!(
            runtime.ctx.replication().backlog.snapshot_boundary(),
            boundary
        );
        assert_eq!(runtime.ctx.replication().backlog.history_len(), 0);
    }
}
