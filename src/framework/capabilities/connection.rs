use crate::engine::command::CommandArgs;
use crate::engine::command::SessionId;
use crate::foundation::io::protocol::parser::Parser;
use crate::foundation::io::protocol::Value;
use crate::foundation::io::sys::time::mstime;
use crate::framework::capabilities::io::Token;
use bytes::Bytes;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

mod command_conn;
mod command_cycle;
mod notifications;
mod output;
mod reply;
mod tracking;
mod transaction;

pub(crate) use command_cycle::{CommandCycleState, CommandEffectDrain, ConnectionCommandEffects};
use output::{
    OutputFrameAccounting, OutputFrameKind, OutputState, ThreadedWriteSnapshot,
    CLIENT_INLINE_OUTPUT_CAPACITY,
};
use reply::ReplyPolicy;
use tracking::TrackingState;
pub(crate) use transaction::{TransactionEnd, TransactionState};

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);
const C_RAX_NODE_ACCOUNTING_BYTES: usize =
    std::mem::size_of::<u32>() + std::mem::size_of::<usize>();

fn tracking_prefix_rax_node_count(prefixes: &[Bytes]) -> usize {
    if prefixes.is_empty() {
        return 0;
    }

    fn count_node(keys: &[&[u8]], depth: usize) -> usize {
        let longer_start = keys.partition_point(|key| key.len() == depth);
        let longer = &keys[longer_start..];
        if longer.is_empty() {
            return 1;
        }

        let first = longer[0];
        let last = longer[longer.len() - 1];
        let mut common_len = depth;
        let max_common = first.len().min(last.len());
        while common_len < max_common && first[common_len] == last[common_len] {
            common_len += 1;
        }
        if common_len > depth {
            return 1usize.saturating_add(count_node(longer, common_len));
        }

        let mut nodes = 1usize;
        let mut group_start = 0;
        while group_start < longer.len() {
            let edge = longer[group_start][depth];
            let mut group_end = group_start + 1;
            while group_end < longer.len() && longer[group_end][depth] == edge {
                group_end += 1;
            }
            nodes = nodes.saturating_add(count_node(&longer[group_start..group_end], depth + 1));
            group_start = group_end;
        }
        nodes
    }

    let mut sorted: Vec<&[u8]> = prefixes.iter().map(Bytes::as_ref).collect();
    sorted.sort_unstable();
    sorted.dedup();
    count_node(&sorted, 0)
}

pub use crate::engine::command::context::{BlockedCmd, BlockedState, ConnectionState};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ClientMemoryUsage {
    pub(crate) total: usize,
    pub(crate) output: usize,
    pub(crate) argv: usize,
    pub(crate) transaction: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OutputAccountingError;

pub struct Connection {
    token: Token,
    /// Stable, externally visible ID, decoupled from the reusable transport token.
    client_id: u64,
    /// Monotonic sequence of the command currently being processed.
    pub command_seq: u64,
    /// Replication offset produced by this connection's latest propagated write.
    last_write_offset: crate::engine::distributed::replication::ReplicationOffset,
    pub peer_addr: SocketAddr,
    pub state: ConnectionState,
    pub parser: Parser,
    pub last_query_snapshot: Vec<u8>,
    pub(crate) output: OutputState,
    pub resp_version: u8,
    pub authenticated: bool,
    pub ever_authenticated: bool,
    pub username: String,
    pub selected_db: u32,
    pub last_interaction: Instant,
    pub create_time: Instant,
    pub sub_channel_count: usize,
    pub ssub_channel_count: usize,
    command_cycle: CommandCycleState,
    reply: ReplyPolicy,
    pub psub_channel_count: usize,
    pub(crate) transaction: TransactionState,
    pub blocked: Option<BlockedState>,
    pub name: Option<Bytes>,
    pub lib_name: String,
    pub lib_ver: String,
    pub capa_flags: Vec<String>,
    pub no_touch: bool,
    pub no_evict: bool,
    pub import_source: bool,
    pub slot_migration_role:
        Option<crate::engine::distributed::cluster::migration::SlotMigrationConnectionRole>,
    pub(crate) slot_migration_tcp_buffers_applied: bool,
    pub(crate) is_test_tcp_buffer_policy_applied: bool,
    tracking: TrackingState,
    pub monitoring: bool,
    pub readonly: bool,
    pub deny_blocking: bool,
    /// One-shot permission armed by ASKING for the next command.
    pub asking: bool,
    /// Cumulative network input bytes (tot-net-in)
    pub tot_net_in: u64,
    /// Cumulative network output bytes (tot-net-out)
    pub tot_net_out: u64,
    /// Cumulative command count (tot-cmds)
    pub cmd_count: u64,
    pub query_buffer_capacity: usize,
    accounted_normal_memory: usize,
    pub protected: bool,
    /// Retirement was requested while a protected command still owned this client.
    pub(crate) is_async_close_pending: bool,
    pub module_auth_callback: Option<(extern "C" fn(u64, *mut libc::c_void), *mut libc::c_void)>,
    pub module_auth_user: Option<String>,
}

impl Connection {
    pub(crate) fn token(&self) -> Token {
        self.token
    }

    pub(crate) fn client_id(&self) -> u64 {
        self.client_id
    }

    pub(crate) fn session_id(&self) -> SessionId {
        SessionId::from_u64(self.client_id)
    }

    #[cfg(test)]
    pub(crate) fn set_token_for_test(&mut self, token: Token) {
        self.token = token;
    }

    #[cfg(test)]
    pub(crate) fn set_client_id_for_test(&mut self, client_id: u64) {
        self.client_id = client_id;
    }

    pub fn new(token: Token, peer_addr: SocketAddr, requirepass: bool) -> Self {
        let client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
        Self::new_with_client_id(token, client_id, peer_addr, requirepass)
    }

    pub fn new_with_client_id(
        token: Token,
        client_id: u64,
        peer_addr: SocketAddr,
        requirepass: bool,
    ) -> Self {
        let now = Instant::now();
        let authed = !requirepass;
        Self {
            token,
            client_id,
            command_seq: 0,
            last_write_offset: crate::engine::distributed::replication::ReplicationOffset::ZERO,
            peer_addr,
            state: ConnectionState::Connected,
            parser: Parser::new(),
            last_query_snapshot: Vec::new(),
            output: OutputState::with_capacity(16),
            resp_version: 2,
            authenticated: authed,
            ever_authenticated: authed,
            username: "default".to_string(),
            selected_db: 0,
            last_interaction: now,
            create_time: now,
            sub_channel_count: 0,
            ssub_channel_count: 0,
            command_cycle: CommandCycleState::default(),
            reply: ReplyPolicy::default(),
            psub_channel_count: 0,
            transaction: TransactionState::default(),
            blocked: None,
            name: None,
            lib_name: String::new(),
            lib_ver: String::new(),
            capa_flags: Vec::new(),
            no_touch: false,
            no_evict: false,
            import_source: false,
            slot_migration_role: None,
            slot_migration_tcp_buffers_applied: false,
            is_test_tcp_buffer_policy_applied: false,
            tracking: TrackingState::default(),
            monitoring: false,
            readonly: false,
            deny_blocking: false,
            asking: false,
            tot_net_in: 0,
            tot_net_out: 0,
            cmd_count: 0,
            query_buffer_capacity: 0,
            accounted_normal_memory: 0,
            protected: false,
            is_async_close_pending: false,
            module_auth_callback: None,
            module_auth_user: None,
        }
    }

    /// Record every parsed command, including commands rejected before
    /// authentication, so diagnostic identity remains monotonic.
    pub(crate) fn record_received_command(&mut self) -> u64 {
        self.command_seq = self.command_seq.saturating_add(1);
        self.command_seq
    }

    /// Begin accounting for an authenticated and admitted command.
    pub(crate) fn begin_admitted_command(&mut self, command_name: &str) {
        let request_bytes = self.parser.last_cmd_len();
        self.command_cycle.begin(request_bytes, self.tot_net_out);
        if request_bytes > 32 * 1024 {
            self.refresh_query_buffer_capacity();
        }
        self.tot_net_in += request_bytes as u64;
        if self.transaction.is_active() && command_name != "exec" {
            self.transaction.record_input(request_bytes as u64);
        }
        self.cmd_count += 1;
        self.resize_query_buffer_after_command();
    }

    /// Finish request-owned state only after a blocking command becomes
    /// terminal; blocked argv/input accounting must survive until wakeup.
    pub(crate) fn complete_command(&mut self) {
        // C retains argv/net_input_bytes_curr_cmd while a command is blocked
        // and resets both only after the command has actually completed.
        if self.blocked.is_none() {
            self.command_cycle.discard_effects();
            self.command_cycle.finish_accounting();
        }
    }

    pub fn queue_reply(&mut self, data: Vec<u8>) {
        let n = data.len();
        self.output.ordinary_bytes = self.output.ordinary_bytes.saturating_add(n);
        self.tot_net_out += n as u64;
        self.queue_output_frame(data, OutputFrameKind::Ordinary);
    }

    pub(crate) fn queue_command_reply(&mut self, data: Vec<u8>) {
        if self.should_suppress_current_reply() {
            return;
        }
        self.command_cycle.add_reply_bytes(data.len());
        self.queue_reply(data);
    }

    pub(crate) fn queue_command_push_reply(&mut self, data: Vec<u8>) {
        self.command_cycle.add_reply_bytes(data.len());
        self.queue_reply(data);
    }

    pub fn queue_replication_reply(&mut self, data: Vec<u8>) {
        let n = data.len();
        self.output.replication_bytes = self.output.replication_bytes.saturating_add(n);
        self.tot_net_out += n as u64;
        self.queue_output_frame(data, OutputFrameKind::Replication);
    }

    pub(crate) fn queue_slot_snapshot_reply(&mut self, data: Vec<u8>) {
        let n = data.len();
        self.output.slot_snapshot_bytes = self.output.slot_snapshot_bytes.saturating_add(n);
        self.tot_net_out += n as u64;
        self.queue_output_frame(data, OutputFrameKind::SlotSnapshot);
    }

    pub(crate) fn queue_pre_client_handshake_reply(&mut self, data: Vec<u8>) {
        let n = data.len();
        self.output.pre_client_handshake_bytes =
            self.output.pre_client_handshake_bytes.saturating_add(n);
        self.tot_net_out += n as u64;
        self.queue_output_frame(data, OutputFrameKind::PreClientHandshake);
    }

    fn queue_output_frame(&mut self, data: Vec<u8>, kind: OutputFrameKind) {
        if data.is_empty() {
            return;
        }
        let frame_len = data.len();
        let is_slot_export = matches!(
            self.slot_migration_role,
            Some(
                crate::engine::distributed::cluster::migration::SlotMigrationConnectionRole::Export(
                    _
                )
            )
        );
        let is_inline_reply = kind == OutputFrameKind::Ordinary
            && !is_slot_export
            && self.output.pending_frame_count == 0
            && self.output.inline_bytes.saturating_add(frame_len) <= CLIENT_INLINE_OUTPUT_CAPACITY;
        let counts_toward_client_output = matches!(
            kind,
            OutputFrameKind::Ordinary | OutputFrameKind::Replication
        ) && !is_inline_reply;
        let output_capacity = self.output.queue.capacity();
        let accounting_capacity = self.output.frame_accounting.capacity();
        let frame_allocation = crate::foundation::memory::util_alloc::vec_allocation_bytes(&data);
        self.output.queue.push_back(data);
        self.output
            .frame_accounting
            .push_back(OutputFrameAccounting {
                remaining_bytes: self.output.queue.back().map_or(0, Vec::len),
                kind,
                allocation_bytes: frame_allocation,
                counts_toward_client_output,
            });
        let output_growth = self
            .output
            .queue
            .capacity()
            .saturating_sub(output_capacity)
            .saturating_mul(std::mem::size_of::<Vec<u8>>());
        let accounting_growth = self
            .output
            .frame_accounting
            .capacity()
            .saturating_sub(accounting_capacity)
            .saturating_mul(std::mem::size_of::<OutputFrameAccounting>());
        if is_inline_reply {
            self.output.inline_bytes = self.output.inline_bytes.saturating_add(frame_len);
        }
        if counts_toward_client_output {
            self.output.pending_frame_allocation_bytes = self
                .output
                .pending_frame_allocation_bytes
                .saturating_add(frame_allocation);
            self.output.pending_frame_count = self.output.pending_frame_count.saturating_add(1);
        }
        self.output.allocation_bytes = self
            .output
            .allocation_bytes
            .saturating_add(frame_allocation)
            .saturating_add(output_growth)
            .saturating_add(accounting_growth);
    }

    pub fn consume_written_bytes(
        &mut self,
        mut bytes: usize,
        queued_before: usize,
    ) -> Result<usize, OutputAccountingError> {
        let accounting_total = self
            .output
            .frame_accounting
            .iter()
            .try_fold(0usize, |total, frame| {
                total.checked_add(frame.remaining_bytes)
            })
            .ok_or(OutputAccountingError)?;
        if bytes > queued_before || queued_before > accounting_total {
            return Err(OutputAccountingError);
        }
        let mut remaining = bytes;
        let mut ordinary = 0usize;
        let mut replication = 0usize;
        let mut pre_client_handshake = 0usize;
        let mut slot_snapshot = 0usize;
        let mut inline = 0usize;
        let mut released_allocation = 0usize;
        let mut released_frames = 0usize;
        for frame in &self.output.frame_accounting {
            if remaining == 0 {
                break;
            }
            let consumed = remaining.min(frame.remaining_bytes);
            remaining -= consumed;
            match frame.kind {
                OutputFrameKind::Ordinary => {
                    ordinary = ordinary
                        .checked_add(consumed)
                        .ok_or(OutputAccountingError)?;
                    if !frame.counts_toward_client_output {
                        inline = inline.checked_add(consumed).ok_or(OutputAccountingError)?;
                    }
                }
                OutputFrameKind::Replication => {
                    replication = replication
                        .checked_add(consumed)
                        .ok_or(OutputAccountingError)?;
                }
                OutputFrameKind::PreClientHandshake => {
                    pre_client_handshake = pre_client_handshake
                        .checked_add(consumed)
                        .ok_or(OutputAccountingError)?;
                }
                OutputFrameKind::SlotSnapshot => {
                    slot_snapshot = slot_snapshot
                        .checked_add(consumed)
                        .ok_or(OutputAccountingError)?;
                }
            }
            if consumed == frame.remaining_bytes && frame.counts_toward_client_output {
                released_allocation = released_allocation
                    .checked_add(frame.allocation_bytes)
                    .ok_or(OutputAccountingError)?;
                released_frames = released_frames
                    .checked_add(1)
                    .ok_or(OutputAccountingError)?;
            }
        }
        if remaining != 0
            || ordinary > self.output.ordinary_bytes
            || replication > self.output.replication_bytes
            || pre_client_handshake > self.output.pre_client_handshake_bytes
            || slot_snapshot > self.output.slot_snapshot_bytes
            || inline > self.output.inline_bytes
            || released_allocation > self.output.pending_frame_allocation_bytes
            || released_frames > self.output.pending_frame_count
        {
            return Err(OutputAccountingError);
        }

        let replication_written = replication;
        while bytes > 0 {
            let accounting = self
                .output
                .frame_accounting
                .front_mut()
                .expect("validated output accounting must retain the consumed prefix");
            let consumed = bytes.min(accounting.remaining_bytes);
            accounting.remaining_bytes -= consumed;
            bytes -= consumed;
            match accounting.kind {
                OutputFrameKind::Ordinary => {
                    self.output.ordinary_bytes -= consumed;
                    if !accounting.counts_toward_client_output {
                        self.output.inline_bytes -= consumed;
                    }
                }
                OutputFrameKind::Replication => {
                    self.output.replication_bytes -= consumed;
                }
                OutputFrameKind::PreClientHandshake => {
                    self.output.pre_client_handshake_bytes -= consumed;
                }
                OutputFrameKind::SlotSnapshot => {
                    self.output.slot_snapshot_bytes -= consumed;
                }
            }
            if accounting.remaining_bytes == 0 {
                let allocation_bytes = accounting.allocation_bytes;
                let counts_toward_client_output = accounting.counts_toward_client_output;
                self.output.frame_accounting.pop_front();
                if counts_toward_client_output {
                    self.output.pending_frame_allocation_bytes -= allocation_bytes;
                    self.output.pending_frame_count -= 1;
                }
            }
        }
        Ok(replication_written)
    }

    pub fn clear_output_queue(&mut self) {
        self.output.queue.clear();
        self.output.frame_accounting.clear();
        self.output.pending_frame_allocation_bytes = 0;
        self.output.pending_frame_count = 0;
        self.output.inline_bytes = 0;
        self.output.allocation_bytes = self
            .output
            .queue
            .capacity()
            .saturating_mul(std::mem::size_of::<Vec<u8>>())
            .saturating_add(
                self.output
                    .frame_accounting
                    .capacity()
                    .saturating_mul(std::mem::size_of::<OutputFrameAccounting>()),
            );
        self.output.replication_bytes = 0;
        self.output.ordinary_bytes = 0;
        self.output.pre_client_handshake_bytes = 0;
        self.output.slot_snapshot_bytes = 0;
        self.output.soft_limit_reached_at_ms = None;
    }

    pub(crate) fn take_threaded_write_snapshot(&mut self) -> ThreadedWriteSnapshot {
        let frames = std::mem::take(&mut self.output.queue);
        let logical_len = frames.iter().map(Vec::len).sum();
        ThreadedWriteSnapshot {
            frames,
            logical_len,
        }
    }

    pub(crate) fn restore_threaded_write_snapshot(&mut self, mut frames: VecDeque<Vec<u8>>) {
        frames.append(&mut self.output.queue);
        self.output.queue = frames;
    }

    pub fn has_pending_output(&self) -> bool {
        !self.output.queue.is_empty()
    }

    pub fn output_buffer_bytes(&self) -> usize {
        self.output
            .ordinary_bytes
            .saturating_add(self.output.replication_bytes)
            .saturating_add(self.output.pre_client_handshake_bytes)
            .saturating_add(self.output.slot_snapshot_bytes)
    }

    pub(crate) fn replication_output_bytes(&self) -> usize {
        self.output.replication_bytes
    }

    #[cfg(test)]
    pub(crate) fn ordinary_output_bytes(&self) -> usize {
        self.output.ordinary_bytes
    }

    pub(crate) fn pending_reply_bytes(&self) -> usize {
        self.output
            .ordinary_bytes
            .saturating_add(self.output.replication_bytes)
    }

    pub(crate) fn output_buffer_allocation_bytes(&self) -> usize {
        self.output.allocation_bytes
    }

    pub(crate) fn pending_output_memory_usage(&self) -> usize {
        self.output
            .pending_frame_allocation_bytes
            .saturating_add(
                self.output
                    .pending_frame_count
                    .saturating_mul(std::mem::size_of::<Vec<u8>>()),
            )
            .saturating_add(
                self.output
                    .pending_frame_count
                    .saturating_mul(std::mem::size_of::<OutputFrameAccounting>()),
            )
    }

    pub(crate) fn pending_output_list_len(&self) -> usize {
        self.output.pending_frame_count
    }

    pub(crate) fn client_memory_usage(&self) -> ClientMemoryUsage {
        let output = self.pending_output_memory_usage();
        let argv = self
            .command_cycle
            .request_bytes()
            .saturating_add(std::mem::size_of::<Bytes>());
        let transaction = self.transaction.queue_memory_usage();
        let watched = self.transaction.watch_memory_usage();
        let pubsub_count = self
            .sub_channel_count
            .saturating_add(self.psub_channel_count)
            .saturating_add(self.ssub_channel_count);
        let pubsub = pubsub_count.saturating_mul(
            std::mem::size_of::<Bytes>().saturating_add(2 * std::mem::size_of::<usize>()),
        );
        // C deliberately underestimates tracking-prefix memory as
        // rax->numnodes * (sizeof(raxNode) + sizeof(raxNode *)). Keep this
        // client-facing projection independent of Rust's Vec/Bytes layout.
        let tracking = self
            .tracking
            .prefix_node_count
            .saturating_mul(C_RAX_NODE_ACCOUNTING_BYTES);
        let owned_strings = self
            .username
            .capacity()
            .saturating_add(self.lib_name.capacity())
            .saturating_add(self.lib_ver.capacity())
            .saturating_add(self.name.as_ref().map_or(0, Bytes::len));
        let parser_memory = self.parser.inflight_memory_usage();
        let query = if self.parser.has_inflight_request() {
            // During an incomplete bulk request the declared payload length is
            // already committed client memory in C. Partial argv payloads are
            // separate objects while the consumed query buffer is retained.
            // C reserves big arguments non-greedily, so Rust Vec growth slack
            // must not affect the client eviction decision.
            self.query_buffer_total()
                .max(parser_memory.query_buffer)
                .saturating_add(parser_memory.partial_argv)
        } else {
            // C counts sdsAllocSize(querybuf) even after the request is consumed.
            self.parser.buffer_allocation_bytes()
        };
        let total = std::mem::size_of::<Self>()
            // Match C getClientMemoryUsage(): the fixed 16 KiB reply buffer is
            // part of total client memory even while it remains outside omem.
            .saturating_add(CLIENT_INLINE_OUTPUT_CAPACITY)
            .saturating_add(query)
            .saturating_add(output)
            .saturating_add(argv)
            .saturating_add(transaction)
            .saturating_add(watched)
            .saturating_add(pubsub)
            .saturating_add(tracking)
            .saturating_add(owned_strings);
        ClientMemoryUsage {
            total,
            output,
            argv,
            transaction,
        }
    }

    pub(crate) fn client_memory_usage_with_pending_replies(
        &self,
        replies: &[Vec<u8>],
    ) -> ClientMemoryUsage {
        let mut usage = self.client_memory_usage();
        let is_slot_export = matches!(
            self.slot_migration_role,
            Some(
                crate::engine::distributed::cluster::migration::SlotMigrationConnectionRole::Export(
                    _
                )
            )
        );
        let mut listed_frame_count = self.output.pending_frame_count;
        let mut inline_bytes = self.output.inline_bytes;
        let mut additional_output = 0usize;
        for reply in replies.iter().filter(|reply| !reply.is_empty()) {
            let is_inline = !is_slot_export
                && listed_frame_count == 0
                && inline_bytes.saturating_add(reply.len()) <= CLIENT_INLINE_OUTPUT_CAPACITY;
            if is_inline {
                inline_bytes = inline_bytes.saturating_add(reply.len());
                continue;
            }
            listed_frame_count = listed_frame_count.saturating_add(1);
            additional_output = additional_output
                .saturating_add(crate::foundation::memory::util_alloc::vec_allocation_bytes(
                    reply,
                ))
                .saturating_add(std::mem::size_of::<Vec<u8>>())
                .saturating_add(std::mem::size_of::<OutputFrameAccounting>());
        }
        usage.output = usage.output.saturating_add(additional_output);
        usage.total = usage.total.saturating_add(additional_output);
        usage
    }

    pub(crate) fn accounted_normal_memory(&self) -> usize {
        self.accounted_normal_memory
    }

    pub(crate) fn replace_accounted_normal_memory(&mut self, bytes: usize) -> usize {
        std::mem::replace(&mut self.accounted_normal_memory, bytes)
    }

    pub(crate) fn take_accounted_normal_memory(&mut self) -> usize {
        std::mem::take(&mut self.accounted_normal_memory)
    }

    pub(crate) fn release_output_frame_allocations(&mut self, bytes: usize) {
        self.output.allocation_bytes = self.output.allocation_bytes.saturating_sub(bytes);
    }

    pub fn mark_closing(&mut self) {
        self.state = ConnectionState::Closing;
    }

    pub(crate) fn has_custom_reply(&self) -> bool {
        self.command_has_custom_reply()
    }

    pub(crate) fn set_custom_reply_enabled(&mut self, enabled: bool) {
        self.set_command_custom_reply(enabled);
    }

    pub(crate) fn should_suppress_current_reply(&self) -> bool {
        self.reply
            .should_suppress_current(self.command_reply_suppressed())
    }

    pub(crate) fn finish_reply_cycle(&mut self) {
        self.finish_command_reply_state();
        self.reply.finish_command();
    }

    #[cfg(test)]
    pub(crate) fn pending_notifications(&self) -> &[(Bytes, Bytes)] {
        self.notifications()
    }

    #[cfg(test)]
    pub(crate) fn pending_shard_notifications(&self) -> &[(Bytes, Bytes)] {
        self.shard_notifications()
    }

    pub(crate) fn discard_command_cycle(&mut self) {
        self.discard_command_effects();
        self.complete_command();
    }

    pub fn refresh_query_buffer_capacity(&mut self) {
        let hint = self.parser.query_buffer_capacity_hint();
        if hint > 0 {
            self.query_buffer_capacity = self.query_buffer_capacity.max(hint.max(16 * 1024));
        }
    }

    pub fn query_buffer_total(&self) -> usize {
        let idle_secs = Instant::now()
            .duration_since(self.last_interaction)
            .as_secs();
        if idle_secs >= 2 {
            if self.parser.has_inflight_request() {
                return self.parser.pending_len().max(16 * 1024);
            }
            return 0;
        }
        self.query_buffer_capacity
            .max(self.parser.query_buffer_capacity_hint())
    }

    pub fn remember_query_bytes(&mut self, bytes: &[u8]) {
        // C logs only a bounded query-buffer excerpt for protocol/crash
        // diagnostics. Retaining a megabyte copy for every normal client
        // read makes CLIENT REPLY OFF pipelines consume memory unrelated to
        // the keyspace.
        const MAX_SNAPSHOT: usize = 64;
        if let Some(debug_at) = (!self.last_query_snapshot.is_empty())
            .then(|| {
                bytes
                    .windows(5)
                    .position(|window| window.eq_ignore_ascii_case(b"DEBUG"))
            })
            .flatten()
        {
            let remaining = MAX_SNAPSHOT.saturating_sub(self.last_query_snapshot.len());
            self.last_query_snapshot
                .extend_from_slice(&bytes[..debug_at.min(remaining)]);
            return;
        }
        self.last_query_snapshot.clear();
        self.last_query_snapshot
            .extend_from_slice(&bytes[..bytes.len().min(MAX_SNAPSHOT)]);
    }

    pub fn query_buffer_used(&self) -> usize {
        self.parser.pending_len()
    }

    pub fn query_buffer_free(&self) -> usize {
        self.query_buffer_total()
            .saturating_sub(self.query_buffer_used())
    }

    pub fn resize_query_buffer_after_command(&mut self) {
        if !self.parser.has_inflight_request()
            && self.command_cycle.request_bytes() > 0
            && self.command_cycle.request_bytes() <= 32 * 1024
            && self.query_buffer_capacity > 32 * 1024
        {
            self.query_buffer_capacity = 16 * 1024;
        }
    }

    pub fn shrink_idle_query_buffer(&mut self) {
        let idle_ms = Instant::now()
            .duration_since(self.last_interaction)
            .as_millis() as u64;
        if idle_ms >= 2000 {
            self.parser.shrink_idle_buffer();
            self.query_buffer_capacity = 16 * 1024;
        }
    }

    pub fn enter_blocking(
        &mut self,
        cmd: BlockedCmd,
        keys: Vec<Bytes>,
        timeout_ms: u64,
        orig_args: &CommandArgs,
        req_bytes: usize,
    ) -> Value {
        if self.transaction.is_active() {
            return Value::NullArray;
        }
        self.install_blocked_state(cmd, keys, timeout_ms, orig_args, req_bytes);
        self.command_cycle.set_skip_propagation();
        Value::Null
    }

    /// Retire preflight scratch before installing a postponed blocked state.
    pub(crate) fn postpone_command(
        &mut self,
        cmd: BlockedCmd,
        keys: Vec<Bytes>,
        timeout_ms: u64,
        orig_args: &CommandArgs,
        req_bytes: usize,
    ) -> Value {
        if self.transaction.is_active() {
            return Value::NullArray;
        }
        self.retire_command_attempt_for_postpone();
        self.install_blocked_state(cmd, keys, timeout_ms, orig_args, req_bytes);
        Value::Null
    }

    pub(crate) fn postpone_transaction_command(&mut self, orig_args: &CommandArgs) {
        let req_bytes = self.request_bytes_current_command();
        self.retire_command_attempt_for_postpone();
        self.install_blocked_state(BlockedCmd::Paused, Vec::new(), 0, orig_args, req_bytes);
    }

    fn install_blocked_state(
        &mut self,
        cmd: BlockedCmd,
        keys: Vec<Bytes>,
        timeout_ms: u64,
        orig_args: &CommandArgs,
        req_bytes: usize,
    ) {
        let prev = self.blocked.take();
        let orig_wrapped = match (&cmd, prev.as_ref()) {
            // When the outer CLIENT PAUSE postpone wraps a real timeout-able
            // blocking command, preserve the previous state so CLIENT UNBLOCK
            // and UNPAUSE can still see the underlying BLPOP/BRPOP/etc block.
            // Alignment with C Valkey: BLOCKED_POSTPONE is a separate
            // postponement layer over the real BLOCKED_LIST/ZSET/STREAM btype.
            (BlockedCmd::Paused, Some(prev_b)) => {
                if !matches!(prev_b.cmd, BlockedCmd::Paused | BlockedCmd::Script) {
                    Some(Box::new(prev_b.clone()))
                } else {
                    None
                }
            }
            _ => None,
        };
        self.blocked = Some(BlockedState {
            cmd,
            keys,
            timeout_ms,
            start_time_ms: mstime(),
            orig_args: orig_args.clone(),
            req_bytes,
            orig_wrapped,
        });
    }

    pub fn new_fake() -> Self {
        let now = Instant::now();
        Self {
            token: Token(usize::MAX),
            client_id: usize::MAX as u64,
            command_seq: 0,
            last_write_offset: crate::engine::distributed::replication::ReplicationOffset::ZERO,
            peer_addr: ([127, 0, 0, 1], 0).into(),
            state: ConnectionState::Connected,
            parser: Parser::new(),
            last_query_snapshot: Vec::new(),
            output: OutputState::with_capacity(0),
            resp_version: 2,
            authenticated: true,
            ever_authenticated: true,
            username: "default".to_string(),
            selected_db: 0,
            last_interaction: now,
            create_time: now,
            sub_channel_count: 0,
            ssub_channel_count: 0,
            command_cycle: CommandCycleState::synthetic(),
            reply: ReplyPolicy::default(),
            psub_channel_count: 0,
            transaction: TransactionState::default(),
            blocked: None,
            name: None,
            lib_name: String::new(),
            lib_ver: String::new(),
            capa_flags: Vec::new(),
            no_touch: false,
            no_evict: false,
            import_source: false,
            slot_migration_role: None,
            slot_migration_tcp_buffers_applied: false,
            is_test_tcp_buffer_policy_applied: false,
            tracking: TrackingState::default(),
            monitoring: false,
            readonly: false,
            deny_blocking: false,
            asking: false,
            tot_net_in: 0,
            tot_net_out: 0,
            cmd_count: 0,
            query_buffer_capacity: 0,
            accounted_normal_memory: 0,
            protected: false,
            is_async_close_pending: false,
            module_auth_callback: None,
            module_auth_user: None,
        }
    }
}

#[cfg(test)]
mod tests;
