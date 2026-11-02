//! Ordinary client read, parser, HUP-drain, and continuation ownership.
//! This module owns transport-facing request admission. Parsed commands cross
//! into the worker request flow; command policy never reads sockets directly.

use super::super::ServerWorkerState;
use crate::engine::command::CommandArgs;
use crate::foundation::io::persistence::AofFsync;
use crate::foundation::io::protocol::parser::ParseError;
use crate::foundation::io::protocol::serializer::{serialize_to_vec_resp2, serialize_to_vec_resp3};
use crate::foundation::io::protocol::Value;
use crate::foundation::io::transport::{Interest, ReadOutcome};
use crate::framework::capabilities::connection::ConnectionState;
use crate::framework::capabilities::io::Token;
use crate::runtime::server::worker::flows::request::dispatch::{LoopAction, ReadBatchEffects};
use crate::runtime::server::worker::reactor::RunnableKey;
use std::io::Read;
use std::time::Instant;

pub(crate) fn command_bulk_limits(authenticated: bool, configured_max_bulk: i64) -> (usize, usize) {
    let configured = usize::try_from(configured_max_bulk).unwrap_or(usize::MAX);
    if authenticated {
        (0, configured)
    } else {
        (
            crate::runtime::server::worker::flows::request::UNAUTH_MBULK_MAX,
            crate::runtime::server::worker::flows::request::UNAUTH_BULK_MAX.min(configured),
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClientReadMode {
    Fair,
    DrainAfterHup,
}

enum ClientParseResult {
    Command(CommandArgs, String, bool),
    Pending,
    ProtocolError(String, bool),
}

fn read_stream_bounded(
    reader: &mut impl Read,
    scratch: &mut [u8],
    byte_budget: usize,
) -> (Vec<u8>, ReadOutcome) {
    let mut read_data = Vec::with_capacity(byte_budget.min(scratch.len()));
    if byte_budget == 0 {
        return (read_data, ReadOutcome::BudgetExhausted);
    }
    if scratch.is_empty() {
        return (
            read_data,
            ReadOutcome::Failed(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "client read scratch buffer is empty",
            )),
        );
    }
    while read_data.len() < byte_budget {
        let chunk_len = (byte_budget - read_data.len()).min(scratch.len());
        match crate::foundation::io::sys::os::retry_on_interrupt(|| {
            reader.read(&mut scratch[..chunk_len])
        }) {
            Ok(0) => return (read_data, ReadOutcome::PeerClosed),
            Ok(read) => read_data.extend_from_slice(&scratch[..read]),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                return (read_data, ReadOutcome::Quiescent);
            }
            Err(error) => return (read_data, ReadOutcome::Failed(error)),
        }
    }
    (read_data, ReadOutcome::BudgetExhausted)
}

impl ServerWorkerState {
    pub(crate) fn handle_readable(&mut self, token_value: usize, mode: ClientReadMode) {
        let connection_token = match self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token_value))
            .then_some(token_value)
        {
            Some(k) => k,
            None => return,
        };
        if !self.socket_read_owned_by_main_thread(
            self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
                .token(),
        ) {
            return;
        }
        // Match C networking.c canParseCommand(): once a command marks the
        // client close-after-reply, later TCP segments must not extend the
        // reply or execute more commands while the terminal reply waits for a
        // future pending-write turn.
        if self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .state
            != ConnectionState::Connected
        {
            return;
        }
        if self.is_master_link(token_value) {
            self.handle_master_link_readable(token_value, connection_token);
            return;
        }
        let is_replica_side_rdb = self
            .replication_runtime
            .replica_rdb_link()
            .is_some_and(|link| link.connection_id().0 == token_value);
        if is_replica_side_rdb {
            self.recv_rdb_channel_streaming_chunk(token_value, connection_token);
            return;
        }
        if self.ctx.replication().is_replica()
            && crate::foundation::io::sys::time::mstime()
                < self.ctx.replication().disconnect_clients_until_ms
        {
            let _ = self.transport.shutdown(
                self.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)]
                .token(),
            );
            self.remove_connection(token_value);
            return;
        }

        if self.try_submit_io_read(
            self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
                .token(),
            mode == ClientReadMode::DrainAfterHup,
        ) {
            return;
        }

        let (read_data, outcome) = match mode {
            ClientReadMode::Fair => self.read_from_connection_bounded_outcome(connection_token),
            ClientReadMode::DrainAfterHup => self.read_from_connection_after_hup(connection_token),
        };
        self.process_client_read_outcome(Token(token_value), mode, read_data, outcome);
    }

    pub(crate) fn process_client_read_outcome(
        &mut self,
        token: Token,
        mode: ClientReadMode,
        read_data: Vec<u8>,
        outcome: ReadOutcome,
    ) {
        self.authenticate_tls_peer(token);
        let (close, yielded, error) = match outcome {
            ReadOutcome::Quiescent => (false, false, None),
            ReadOutcome::BudgetExhausted => (false, true, None),
            ReadOutcome::PeerClosed => (true, false, None),
            ReadOutcome::Failed(error) => (true, false, Some(error)),
        };
        let should_finish_hup = mode == ClientReadMode::DrainAfterHup || close;
        let effective_mode = if should_finish_hup {
            ClientReadMode::DrainAfterHup
        } else {
            mode
        };
        if mode == ClientReadMode::Fair && close {
            self.client_hup_drains.insert(token);
        }
        if let Some(error) = error {
            crate::server_log!(
                warning,
                "runtime source lifecycle token={} owner=connection operation=read error={}",
                token.0,
                error
            );
        }
        self.process_client_read_result(token, effective_mode, read_data, close, yielded);
        if should_finish_hup {
            self.finish_client_hup_drain_turn(token.0);
        }
    }

    pub(crate) fn process_client_read_result(
        &mut self,
        token: Token,
        mode: ClientReadMode,
        read_data: Vec<u8>,
        close: bool,
        yielded: bool,
    ) {
        if !self.should_process_client_read(token, mode, &read_data, close) {
            return;
        }
        if !self.begin_client_read_batch(token, &read_data) {
            return;
        }
        if self.connection_registry[token].blocked.is_some() {
            self.schedule_client_read_continuation(token, yielded);
            return;
        }
        self.prefetch_pending_pipeline_keys(token.0);
        let mut fx = ReadBatchEffects::default();
        let pipeline_yielded = self.process_client_pipeline(token, mode, &mut fx);
        let yielded = yielded || pipeline_yielded;
        self.finalize_handle_readable(token.0, token.0, fx);
        self.schedule_client_read_continuation(token, yielded);
    }

    fn should_process_client_read(
        &mut self,
        token: Token,
        mode: ClientReadMode,
        read_data: &[u8],
        close: bool,
    ) -> bool {
        let Some(connection) = self.connection_registry.get(token) else {
            return false;
        };
        if connection.state != ConnectionState::Connected {
            return false;
        }
        if close && mode == ClientReadMode::Fair {
            self.remove_connection(token.0);
            return false;
        }
        !read_data.is_empty() || connection.parser.has_pending_data()
    }

    fn begin_client_read_batch(&mut self, token: Token, read_data: &[u8]) -> bool {
        // Replica writable readiness can drain userspace output earlier in
        // the same poll batch. Settle its deferred O(replicas) accounting
        // before a following command reads INFO or maxmemory state.
        self.refresh_replication_buffer_accounting_if_dirty();
        {
            let conn = &mut self.connection_registry[token];
            conn.last_interaction = Instant::now();
            if !read_data.is_empty() {
                conn.parser.feed(read_data);
                conn.remember_query_bytes(read_data);
            }
        }
        let query_buffer_limit = self.ctx.config().server.client_query_buffer_limit;
        if query_buffer_limit > 0
            && self.connection_registry[token].parser.pending_len() > query_buffer_limit
        {
            self.ctx
                .stats_mut()
                .client_query_buffer_limit_disconnections += 1;
            self.remove_connection(token.0);
            return false;
        }
        // C updates client memory after socket reads. Enforce the aggregate
        // client limit before another connection can observe this newly grown
        // query buffer in the same poll batch.
        self.refresh_client_memory_usage(token, false);
        self.evict_clients();
        if !self.connection_registry.contains(token) {
            return false;
        }
        true
    }

    fn process_client_pipeline(
        &mut self,
        token: Token,
        mode: ClientReadMode,
        fx: &mut ReadBatchEffects,
    ) -> bool {
        let mut parsed_commands = 0;
        loop {
            if !self
                .connection_registry
                .get(token)
                .is_some_and(|connection| connection.state == ConnectionState::Connected)
            {
                return false;
            }
            match self.parse_next_client_command(token) {
                ClientParseResult::Command(args, name, resp3) => {
                    parsed_commands += 1;
                    if self.process_parsed_command(token, args, name, resp3, fx)
                        == LoopAction::Break
                    {
                        return false;
                    }
                }
                ClientParseResult::Pending => {
                    self.refresh_pending_query_buffer(token);
                    return false;
                }
                ClientParseResult::ProtocolError(message, resp3) => {
                    self.record_client_protocol_error(token, &message, resp3, fx);
                    return false;
                }
            }
            self.process_dirty_keys_in_loop(token.0, fx);
            if self
                .ctx
                .script()
                .debug_modes
                .get(&self.connection_registry[token].session_id())
                == Some(&crate::engine::extension::script::ScriptDebugMode::Forked)
            {
                // Commit earlier pipeline replies before a later EVAL transfers
                // this socket to a debugger child.
                return self.connection_registry[token].parser.has_pending_data();
            }
            if mode == ClientReadMode::DrainAfterHup
                && parsed_commands >= crate::runtime::server::worker::HUP_CLIENT_COMMAND_BUDGET
            {
                return true;
            }
        }
    }

    fn parse_next_client_command(&mut self, token: Token) -> ClientParseResult {
        let conn = &mut self.connection_registry[token];
        let (max_mbulk, max_bulk) = command_bulk_limits(
            conn.authenticated,
            self.ctx.config().server.proto_max_bulk_len,
        );
        match conn.parser.try_parse_limited(max_mbulk, max_bulk) {
            Ok(Some(parts)) => {
                let args = CommandArgs::from_parts(parts);
                let name = String::from_utf8_lossy(&args.name).to_ascii_lowercase();
                ClientParseResult::Command(args, name, conn.resp_version >= 3)
            }
            Err(ParseError::Protocol(message)) => {
                ClientParseResult::ProtocolError(message, conn.resp_version >= 3)
            }
            Ok(None) | Err(_) => ClientParseResult::Pending,
        }
    }

    fn refresh_pending_query_buffer(&mut self, token: Token) {
        if self.connection_registry[token].parser.has_pending_data() {
            self.connection_registry[token].refresh_query_buffer_capacity();
        }
    }

    fn record_client_protocol_error(
        &mut self,
        token: Token,
        message: &str,
        resp3: bool,
        fx: &mut ReadBatchEffects,
    ) {
        crate::server_log!(warning, "Protocol error token {}: {}", token.0, message);
        let snapshot = &self.connection_registry[token].last_query_snapshot;
        if !snapshot.is_empty() {
            let line = if self.ctx.config().server.hide_user_data_from_log {
                "Query buffer: redacted\n".to_string()
            } else {
                format!(
                    "Query buffer: {}\n",
                    String::from_utf8_lossy(snapshot).replace(['\r', '\n'], " ")
                )
            };
            let mut stdout = std::io::stdout().lock();
            let _ = std::io::Write::write_all(&mut stdout, line.as_bytes());
            let _ = std::io::Write::flush(&mut stdout);
        }
        let reply = Value::error(format!("ERR {}", message));
        fx.replies.push(if resp3 {
            serialize_to_vec_resp3(&reply)
        } else {
            serialize_to_vec_resp2(&reply)
        });
        self.connection_registry[token].mark_closing();
        fx.mark_closing = true;
    }

    fn schedule_client_read_continuation(&mut self, token: Token, yielded: bool) {
        if yielded && self.connection_registry.contains(token) {
            let session = self.connection_registry[token].session_id();
            let is_forked_debugger = self.ctx.script().debug_modes.get(&session)
                == Some(&crate::engine::extension::script::ScriptDebugMode::Forked);
            if is_forked_debugger
                && (self.connection_registry[token].has_pending_output()
                    || self.io_threads.is_write_inflight(token)
                    || !self.socket_owned_by_main_thread(token))
            {
                return;
            }
            self.schedule_client_read_runnable(token, true);
        }
    }

    pub(crate) fn queue_forked_debugger_for_reprocessing(&mut self, token: Token) {
        let should_queue = self
            .connection_registry
            .get(token)
            .is_some_and(|connection| {
                connection.state == ConnectionState::Connected
                    && connection.blocked.is_none()
                    && connection.parser.has_pending_data()
                    && !connection.has_pending_output()
                    && self.ctx.script().debug_modes.get(&connection.session_id())
                        == Some(&crate::engine::extension::script::ScriptDebugMode::Forked)
            })
            && !self.io_threads.is_write_inflight(token)
            && self.socket_owned_by_main_thread(token);
        if should_queue {
            self.schedule_client_read_runnable(token, false);
        }
    }

    fn schedule_client_read_runnable(&mut self, token: Token, next_turn: bool) {
        let key = RunnableKey::ClientRead(token);
        if next_turn {
            self.schedule_runtime_runnable_next_turn(key);
        } else {
            self.schedule_runtime_runnable(key);
        }
    }

    /// Mirrors C `queueClientForReprocessing`: parser data accumulated while
    /// blocked becomes runnable only after the retained command is terminal.
    pub(crate) fn queue_client_for_reprocessing(&mut self, token: Token) {
        let should_queue = self
            .connection_registry
            .get(token)
            .is_some_and(|connection| {
                connection.state == ConnectionState::Connected
                    && connection.blocked.is_none()
                    && connection.parser.has_pending_data()
            });
        if should_queue {
            self.schedule_client_read_runnable(token, false);
        }
    }

    /// Warm the key-store lookup path for a bounded preview of an already
    /// buffered authenticated-client pipeline. The preview parser is isolated
    /// from the live parser, so command execution, AUTH state changes, and
    /// per-command request-byte accounting retain their normal ordering.
    fn prefetch_pending_pipeline_keys(&mut self, connection_token: usize) {
        let configured = self.ctx.config().server.prefetch_batch_max_size.min(128);
        let Some((dbid, commands)) = self.preview_prefetch_commands(connection_token, configured)
        else {
            return;
        };
        let (candidate_keys, prefetched_entries) =
            self.warm_prefetch_keys(dbid, &commands, configured);
        if candidate_keys <= 1 {
            return;
        }
        let stats = self.ctx.stats_mut();
        stats.io_threaded_total_prefetch_batches =
            stats.io_threaded_total_prefetch_batches.saturating_add(1);
        stats.io_threaded_total_prefetch_entries = stats
            .io_threaded_total_prefetch_entries
            .saturating_add(prefetched_entries);
    }

    fn preview_prefetch_commands(
        &self,
        connection_token: usize,
        configured: usize,
    ) -> Option<(u32, Vec<CommandArgs>)> {
        let conn =
            &self.connection_registry[crate::framework::capabilities::io::Token(connection_token)];
        if !conn.authenticated || configured < 2 || !conn.parser.has_pending_data() {
            return None;
        }
        let dbid = conn.selected_db;
        let mut preview = conn.parser.clone();
        let mut commands = Vec::with_capacity(configured);
        for _ in 0..configured {
            match preview.try_parse_limited(0, 0) {
                Ok(Some(parts)) => commands.push(CommandArgs::from_parts(parts)),
                Ok(None) | Err(_) => break,
            }
        }
        (!commands.is_empty()).then_some((dbid, commands))
    }

    fn warm_prefetch_keys(
        &self,
        dbid: u32,
        commands: &[CommandArgs],
        configured: usize,
    ) -> (usize, u64) {
        let Some(db) = self.ctx.db(dbid) else {
            return (0, 0);
        };
        let mut remaining = configured;
        let mut entries = 0u64;
        for command in commands {
            let Some(spec) = self.registry.lookup(&command.name) else {
                continue;
            };
            entries = entries.saturating_add(Self::warm_prefetch_command(
                db,
                spec,
                command,
                &mut remaining,
            ));
            if remaining == 0 {
                break;
            }
        }
        (configured.saturating_sub(remaining), entries)
    }

    fn warm_prefetch_command(
        db: &crate::foundation::io::store::kv_engine::Db,
        spec: &crate::framework::CommandSpec,
        command: &CommandArgs,
        remaining: &mut usize,
    ) -> u64 {
        let mut entries = 0u64;
        {
            let mut warm = |key: &[u8]| {
                if *remaining == 0 {
                    return false;
                }
                *remaining -= 1;
                if std::hint::black_box(db.get_no_expire(key)).is_some() {
                    entries = entries.saturating_add(1);
                }
                true
            };
            if let Some(get_keys) = spec.get_keys {
                for (key, _) in get_keys(&command.args) {
                    if !warm(key) {
                        break;
                    }
                }
            } else if let Some(indices) = spec.descriptor().key_indices(command) {
                for index in indices {
                    if !warm(&command.args[index]) {
                        break;
                    }
                }
            }
        }
        entries
    }

    #[cfg(test)]
    pub(crate) fn read_from_connection_bounded(
        &mut self,
        token_value: usize,
        connection_token: usize,
    ) -> (Vec<u8>, bool, bool) {
        let (read_data, outcome) = self.read_from_connection_bounded_outcome(connection_token);
        match outcome {
            ReadOutcome::Quiescent => (read_data, false, false),
            ReadOutcome::BudgetExhausted => (read_data, false, true),
            ReadOutcome::PeerClosed => (read_data, true, false),
            ReadOutcome::Failed(error) => {
                crate::server_log!(
                    warning,
                    "runtime source lifecycle token={} owner=connection operation=read error={}",
                    token_value,
                    error
                );
                (read_data, true, false)
            }
        }
    }

    pub(crate) fn read_from_connection_bounded_outcome(
        &mut self,
        connection_token: usize,
    ) -> (Vec<u8>, ReadOutcome) {
        let conn_token = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .token();
        let mut stream = match self.transport.stream(conn_token) {
            Some(stream) => stream,
            None => {
                return (
                    Vec::new(),
                    ReadOutcome::Failed(std::io::Error::new(
                        std::io::ErrorKind::NotConnected,
                        "client transport is not registered",
                    )),
                );
            }
        };
        read_stream_bounded(
            &mut stream,
            &mut self.io_buf,
            crate::runtime::server::worker::NORMAL_CLIENT_READ_BUDGET,
        )
    }

    fn read_from_connection_after_hup(
        &mut self,
        connection_token: usize,
    ) -> (Vec<u8>, ReadOutcome) {
        let conn_token = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .token();
        let mut stream = match self.transport.stream(conn_token) {
            Some(stream) => stream,
            None => {
                return (
                    Vec::new(),
                    ReadOutcome::Failed(std::io::Error::new(
                        std::io::ErrorKind::NotConnected,
                        "client transport is not registered",
                    )),
                );
            }
        };
        read_stream_bounded(
            &mut stream,
            &mut self.io_buf,
            crate::runtime::server::worker::HUP_CLIENT_READ_BUDGET,
        )
    }

    pub(crate) fn drive_pending_client_read(&mut self, token: Token) {
        let token_value = token.0;
        if let Some(connection_token) = self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token_value))
            .then_some(token_value)
        {
            let mode = if self.client_hup_drains.contains(&token) {
                ClientReadMode::DrainAfterHup
            } else {
                ClientReadMode::Fair
            };
            if self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
                .blocked
                .is_some()
                && mode == ClientReadMode::DrainAfterHup
            {
                self.buffer_blocked_client_after_hup(token_value, connection_token);
            } else {
                self.handle_readable(token_value, mode);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn drive_one_pending_client_read(&mut self) -> bool {
        self.event_batch.activate_next_turn_before_poll_slots(&[
            crate::runtime::server::worker::reactor::BeforePollSlot::PreAof,
        ]);
        let Some(event) = self
            .event_batch
            .pop_before_poll_slot(crate::runtime::server::worker::reactor::BeforePollSlot::PreAof)
        else {
            return false;
        };
        let crate::runtime::server::worker::reactor::EventPayload::Runnable(runnable) =
            event.payload
        else {
            panic!("expected a client-read runnable");
        };
        let RunnableKey::ClientRead(token) = runnable.key else {
            panic!("expected a client-read runnable");
        };
        self.drive_pending_client_read(token);
        true
    }

    #[cfg(test)]
    pub(crate) fn drive_pending_client_reads(&mut self) {
        while self.drive_one_pending_client_read() {
            if self.ctx.is_shutdown_requested() {
                break;
            }
        }
    }

    fn buffer_blocked_client_after_hup(&mut self, token_value: usize, connection_token: usize) {
        let (read_data, outcome) = self.read_from_connection_after_hup(connection_token);
        if !read_data.is_empty() && !self.begin_client_read_batch(Token(token_value), &read_data) {
            return;
        }
        let close = matches!(outcome, ReadOutcome::PeerClosed | ReadOutcome::Failed(_));
        if let ReadOutcome::Failed(error) = &outcome {
            crate::server_log!(
                warning,
                "runtime source lifecycle token={} owner=connection operation=read error={}",
                token_value,
                error
            );
        }
        if close
            && (self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .blocked
            .as_ref()
            .is_some_and(|blocked| {
                matches!(
                    blocked.cmd,
                    crate::framework::capabilities::connection::BlockedCmd::Throttled
                )
            }) || !self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .parser
            .has_pending_data())
        {
            self.remove_connection(token_value);
            return;
        }
        // Do not dispatch parser-buffered commands ahead of the blocked
        // command. Keeping one dormant continuation lets unblocking resume the
        // pipeline, while poll_deadline prevents it from spinning meanwhile.
        self.schedule_client_read_runnable(Token(token_value), true);
    }

    pub(crate) fn drain_client_readable_after_hup(&mut self, token_value: usize) {
        self.client_hup_drains.insert(Token(token_value));
        self.cancel_runtime_runnable(RunnableKey::ClientRead(Token(token_value)));
        self.handle_readable(token_value, ClientReadMode::DrainAfterHup);
    }

    fn finish_client_hup_drain_turn(&mut self, token_value: usize) {
        if !self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token_value))
            || self
                .event_batch
                .contains_runnable(RunnableKey::ClientRead(Token(token_value)))
        {
            return;
        }
        let connection_token = token_value;
        if self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .blocked
            .is_some()
        {
            self.schedule_client_read_runnable(Token(token_value), true);
            return;
        }
        self.client_hup_drains.remove(&Token(token_value));
        self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .mark_closing();
        // EOF needs no second write turn once all admitted replies are gone.
        // A prior flush may already have claimed this turn's writable service.
        if !self.connection_registry[Token(connection_token)].has_pending_output()
            && !self.io_threads.is_write_inflight(Token(token_value))
        {
            self.remove_connection(token_value);
            return;
        }
        let token = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .token();
        if self.io_threads.is_write_inflight(Token(token_value)) {
            let _ = self.transport.set_interest(token, Interest::Writable);
            return;
        }
        // Match networking.c installClientWriteHandler: half-close draining
        // must retain new replies until the appendfsync=always barrier runs.
        let should_defer_close_write = self.ctx.config().persistence.aof_enabled
            && self.ctx.config().persistence.aof_fsync == AofFsync::Always
            && self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .has_pending_output();
        if should_defer_close_write {
            self.schedule_ordinary_write(token_value);
            let _ = self.transport.set_interest(token, Interest::Writable);
            return;
        }
        self.handle_writable(Token(token_value));
        let Some(connection_token) = self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token_value))
            .then_some(token_value)
        else {
            return;
        };
        if self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .has_pending_output()
        {
            let _ = self.transport.set_interest(token, Interest::Writable);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{read_stream_bounded, ClientReadMode, ServerWorkerState};
    use crate::foundation::io::transport::ReadOutcome;
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::capabilities::io::Token;
    use crate::runtime::server::worker::reactor::transport::RuntimeConnectedPeer;
    use crate::runtime::server::worker::reactor::RunnableKey;
    use std::io::{Cursor, Error, ErrorKind, Read};
    use std::net::{Shutdown, TcpListener};

    struct ErrorReader(ErrorKind);

    impl Read for ErrorReader {
        fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
            Err(Error::from(self.0))
        }
    }

    fn runtime_with_fake_client() -> (ServerWorkerState, Token) {
        let mut runtime = ServerWorkerState::new(1).expect("create runtime");
        let mut connection = Connection::new_fake();
        connection.set_token_for_test(Token(0));
        let connection_token = runtime.connection_registry.insert(connection);
        (runtime, Token(connection_token))
    }

    #[test]
    fn bounded_read_reports_quiescent_exactly() {
        let mut reader = ErrorReader(ErrorKind::WouldBlock);
        let mut scratch = [0_u8; 8];

        let (bytes, outcome) = read_stream_bounded(&mut reader, &mut scratch, 8);

        assert!(bytes.is_empty());
        assert!(matches!(outcome, ReadOutcome::Quiescent));
    }

    #[test]
    fn bounded_read_reports_budget_exhausted_exactly() {
        let mut reader = Cursor::new(vec![b'x'; 9]);
        let mut scratch = [0_u8; 8];

        let (bytes, outcome) = read_stream_bounded(&mut reader, &mut scratch, 8);

        assert_eq!(bytes, vec![b'x'; 8]);
        assert!(matches!(outcome, ReadOutcome::BudgetExhausted));
    }

    #[test]
    fn bounded_read_reports_peer_closed_exactly() {
        let mut reader = Cursor::new(Vec::<u8>::new());
        let mut scratch = [0_u8; 8];

        let (bytes, outcome) = read_stream_bounded(&mut reader, &mut scratch, 8);

        assert!(bytes.is_empty());
        assert!(matches!(outcome, ReadOutcome::PeerClosed));
    }

    #[test]
    fn bounded_read_preserves_hard_error() {
        let mut reader = ErrorReader(ErrorKind::ConnectionReset);
        let mut scratch = [0_u8; 8];

        let (bytes, outcome) = read_stream_bounded(&mut reader, &mut scratch, 8);

        assert!(bytes.is_empty());
        assert!(matches!(
            outcome,
            ReadOutcome::Failed(error) if error.kind() == ErrorKind::ConnectionReset
        ));
    }

    #[test]
    fn hup_budget_exhausted_schedules_exactly_one_continuation_without_closing() {
        let (mut runtime, token) = runtime_with_fake_client();
        runtime.client_hup_drains.insert(token);

        runtime.process_client_read_outcome(
            token,
            ClientReadMode::DrainAfterHup,
            b"*1\r\n$4\r\nPI".to_vec(),
            ReadOutcome::BudgetExhausted,
        );
        runtime.process_client_read_outcome(
            token,
            ClientReadMode::DrainAfterHup,
            Vec::new(),
            ReadOutcome::BudgetExhausted,
        );

        assert!(runtime
            .event_batch
            .contains_runnable(RunnableKey::ClientRead(token)));
        assert!(runtime.client_hup_drains.contains(&token));
        assert_eq!(
            runtime.connection_registry[token].state,
            crate::framework::capabilities::connection::ConnectionState::Connected
        );
    }

    #[test]
    fn quiescent_read_schedules_no_client_continuation() {
        let (mut runtime, token) = runtime_with_fake_client();

        runtime.process_client_read_outcome(
            token,
            ClientReadMode::Fair,
            Vec::new(),
            ReadOutcome::Quiescent,
        );

        assert!(!runtime
            .event_batch
            .contains_runnable(RunnableKey::ClientRead(token)));
    }

    #[test]
    fn script_debug_is_rejected_after_an_earlier_pipeline_reply() {
        let (mut runtime, token) = runtime_with_fake_client();
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.connection_registry[token].authenticated = true;
        runtime.connection_registry[token].ever_authenticated = true;
        runtime.connection_registry[token].set_custom_reply_enabled(false);
        let session = runtime.connection_registry[token].session_id();
        runtime.process_client_read_outcome(
            token,
            ClientReadMode::Fair,
            b"*1\r\n$4\r\nPING\r\n*3\r\n$6\r\nSCRIPT\r\n$5\r\nDEBUG\r\n$3\r\nYES\r\n".to_vec(),
            ReadOutcome::Quiescent,
        );

        let output = runtime.connection_registry[token]
            .take_threaded_write_snapshot()
            .into_frames()
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert!(!runtime.ctx.script().debug_modes.contains_key(&session));
        assert!(
            String::from_utf8_lossy(&output)
                .contains("ERR SCRIPT DEBUG must be called outside a pipeline"),
            "unexpected pipeline output: {:?}",
            String::from_utf8_lossy(&output)
        );
    }

    #[test]
    fn script_debug_is_allowed_after_reply_skip_suppresses_pipeline_output() {
        let (mut runtime, token) = runtime_with_fake_client();
        crate::framework::commands::register_all(&mut runtime.registry);
        runtime.connection_registry[token].authenticated = true;
        runtime.connection_registry[token].ever_authenticated = true;
        runtime.connection_registry[token].set_custom_reply_enabled(false);
        let session = runtime.connection_registry[token].session_id();

        runtime.process_client_read_outcome(
            token,
            ClientReadMode::Fair,
            b"*3\r\n$6\r\nCLIENT\r\n$5\r\nREPLY\r\n$4\r\nSKIP\r\n*3\r\n$6\r\nSCRIPT\r\n$5\r\nDEBUG\r\n$3\r\nYES\r\n"
                .to_vec(),
            ReadOutcome::Quiescent,
        );

        assert_eq!(
            runtime.ctx.script().debug_modes.get(&session),
            Some(&crate::engine::extension::script::ScriptDebugMode::Forked)
        );
    }

    #[test]
    fn forked_debugger_continuation_waits_for_write_socket_lease_release() {
        let (mut runtime, token) = runtime_with_fake_client();
        let session = runtime.connection_registry[token].session_id();
        runtime.ctx.script_mut().debug_modes.insert(
            session,
            crate::engine::extension::script::ScriptDebugMode::Forked,
        );
        runtime.connection_registry[token]
            .parser
            .feed(b"*1\r\n$4\r\nEVAL\r\n");
        let operation = crate::runtime::server::worker::reactor::io_workers::IoOperationId {
            token,
            generation: 1,
            sequence: 1,
        };
        runtime.acquire_io_worker_write_socket_lease(operation);

        runtime.schedule_client_read_continuation(token, true);
        assert!(!runtime
            .event_batch
            .contains_runnable(RunnableKey::ClientRead(token)));

        runtime.release_io_worker_write_socket_lease(operation);
        assert!(runtime
            .event_batch
            .contains_runnable(RunnableKey::ClientRead(token)));
    }

    #[test]
    fn peer_close_drains_admitted_command_before_teardown() {
        let mut runtime = ServerWorkerState::new(1).expect("create runtime");
        crate::framework::commands::register_all(&mut runtime.registry);
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind peer listener");
        let address = listener.local_addr().expect("read peer address");
        let peer = runtime
            .transport
            .connect_tcp(address)
            .expect("connect runtime transport");
        let (mut target, _) = listener.accept().expect("accept runtime transport");
        runtime.add_connection(RuntimeConnectedPeer {
            token: peer.token,
            peer_addr: address,
        });
        target
            .shutdown(Shutdown::Write)
            .expect("half-close peer write side");

        runtime.process_client_read_outcome(
            peer.token,
            ClientReadMode::Fair,
            b"*1\r\n$4\r\nPING\r\n".to_vec(),
            ReadOutcome::PeerClosed,
        );

        let mut response = [0_u8; 7];
        target
            .read_exact(&mut response)
            .expect("read reply for command admitted before EOF");
        assert_eq!(&response, b"+PONG\r\n");
    }

    #[test]
    fn peer_close_after_reply_flush_releases_client_in_the_same_write_turn() {
        let mut runtime = ServerWorkerState::new(1).expect("create runtime");
        crate::framework::commands::register_all(&mut runtime.registry);
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind peer listener");
        let address = listener.local_addr().expect("read peer address");
        let peer = runtime.transport.connect_tcp(address).unwrap();
        let (mut target, _) = listener.accept().unwrap();
        runtime.add_connection(RuntimeConnectedPeer {
            token: peer.token,
            peer_addr: address,
        });
        runtime.ctx.set_host_task_loading(true);
        runtime.writes.ordinary.begin_turn();
        runtime.connection_registry[peer.token].queue_reply(b"+OK\r\n".to_vec());
        runtime.handle_writable(peer.token);
        let mut response = [0_u8; 5];
        target.read_exact(&mut response).unwrap();
        assert_eq!(&response, b"+OK\r\n");
        assert!(runtime.writes.ordinary.was_serviced(peer.token));

        runtime.process_client_read_outcome(
            peer.token,
            ClientReadMode::Fair,
            Vec::new(),
            ReadOutcome::PeerClosed,
        );

        assert!(!runtime.connection_registry.contains(peer.token));
        assert!(!runtime.transport.is_registered(peer.token));
    }

    #[test]
    fn failed_read_drains_admitted_command_before_teardown() {
        let mut runtime = ServerWorkerState::new(1).expect("create runtime");
        crate::framework::commands::register_all(&mut runtime.registry);
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind peer listener");
        let address = listener.local_addr().expect("read peer address");
        let peer = runtime
            .transport
            .connect_tcp(address)
            .expect("connect runtime transport");
        let (mut target, _) = listener.accept().expect("accept runtime transport");
        runtime.add_connection(RuntimeConnectedPeer {
            token: peer.token,
            peer_addr: address,
        });

        runtime.process_client_read_outcome(
            peer.token,
            ClientReadMode::Fair,
            b"*1\r\n$4\r\nPING\r\n".to_vec(),
            ReadOutcome::Failed(std::io::Error::new(
                ErrorKind::ConnectionReset,
                "read failed after command",
            )),
        );

        let mut response = [0_u8; 7];
        target
            .read_exact(&mut response)
            .expect("read reply for command admitted before failure");
        assert_eq!(&response, b"+PONG\r\n");
        assert!(!runtime.connection_registry.contains(peer.token));
        assert!(!runtime.transport.is_registered(peer.token));
    }
}
