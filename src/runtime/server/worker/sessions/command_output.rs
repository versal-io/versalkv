//! Connection-facing application of committed command output.
//! Command completion decides what should happen; this module owns reply
//! queueing, socket interest, bounded flushing, and terminal connection state.

use super::super::ServerWorkerState;
use crate::foundation::io::transport::{Interest, WriteOutcome};
use crate::framework::capabilities::io::Token;

impl ServerWorkerState {
    pub(crate) fn dual_relay_parent_pause_pending(&self) -> bool {
        self.ctx.config().server.debug_pause_after_fork
            && self
                .full_sync
                .diskless_relay
                .as_ref()
                .is_some_and(|relay| !relay.complete_full_resync && !relay.completion_announced)
    }

    pub(crate) fn pause_dual_relay_client_if_needed(&mut self, token: Token) -> bool {
        let has_reply_barrier = self.full_sync.paused_dual_main_replies.contains_key(&token);
        let is_ordinary = self
            .replication_runtime
            .role(crate::framework::distributed::replication::ConnectionId(
                token.0,
            ))
            .is_none();
        if !self.connection_registry.contains(token)
            || (!has_reply_barrier && !(is_ordinary && self.dual_relay_parent_pause_pending()))
        {
            return false;
        }
        // A stopped C parent cannot parse commands or deliver asynchronous
        // replies. Only the RDB transport must progress in our parent relay.
        self.full_sync.paused_dual_clients.insert(token);
        self.cancel_runtime_runnable(
            crate::runtime::server::worker::reactor::RunnableKey::ClientRead(token),
        );
        self.cancel_runtime_runnable(
            crate::runtime::server::worker::reactor::RunnableKey::ClientWrite(token),
        );
        if self.transport.set_interest(token, Interest::None).is_err() {
            self.remove_connection(token.0);
        }
        true
    }

    pub(crate) fn prepare_slot_import_output(&mut self, connection_token: usize) -> bool {
        let became_slot_import = self.connection_registry.get(crate::framework::capabilities::io::Token(connection_token)).is_some_and(|connection| {
            matches!(
                connection.slot_migration_role,
                Some(
                    crate::engine::distributed::cluster::migration::SlotMigrationConnectionRole::Import(
                        _
                    )
                )
            ) && !connection.slot_migration_tcp_buffers_applied
        });
        !became_slot_import || self.ensure_slot_migration_test_tcp_buffers(connection_token)
    }
    pub(crate) fn queue_origin_replies(
        &mut self,
        connection_token: usize,
        token_value: usize,
        replies: &mut Vec<Vec<u8>>,
    ) -> bool {
        let trace_rdb_ok = self.should_trace_rdb_ok(connection_token, token_value, replies);
        if self
            .full_sync
            .paused_dual_main_replies
            .contains_key(&Token(token_value))
        {
            // C's dual-channel child can transmit RDB while the parent is stopped.
            // The parent-owned pipe relay must finish that transport work first.
            // Its main-channel association reply stays pending until resume.
            self.full_sync
                .paused_dual_main_replies
                .entry(Token(token_value))
                .or_default()
                .append(replies);
            return false;
        }
        let mut needs_flush = false;
        if self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(connection_token))
        {
            let connection = &mut self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)];
            for reply in replies.drain(..) {
                connection.queue_reply(reply);
            }
            needs_flush = !connection.output.queue.is_empty();
            if trace_rdb_ok {
                crate::server_log!(
                    debug,
                    "dual-rdb primary set-rdb-client-id response writer: phase=queued token={} pending_bytes={}",
                    token_value,
                    connection.output.queue.iter().map(Vec::len).sum::<usize>()
                );
            }
        }
        self.refresh_client_memory_usage(Token(token_value), false);
        if self.enforce_client_output_buffer_limit_for(token_value) {
            return false;
        }
        self.evict_clients();
        if !self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(connection_token))
        {
            return false;
        }
        let is_slot_export = self.is_slot_export_connection(connection_token);
        let is_replication = self
            .replication_runtime
            .role(crate::framework::distributed::replication::ConnectionId(
                token_value,
            ))
            .is_some();
        self.reconcile_slot_export_output_allocation(token_value);
        if needs_flush {
            needs_flush = self.route_origin_output(token_value, is_slot_export, is_replication);
        }
        if needs_flush
            && self
                .connection_registry
                .contains(crate::framework::capabilities::io::Token(connection_token))
        {
            self.flush_origin_reply(connection_token, token_value, trace_rdb_ok);
        }
        is_slot_export
    }

    pub(crate) fn should_trace_rdb_ok(
        &self,
        connection_token: usize,
        token_value: usize,
        replies: &[Vec<u8>],
    ) -> bool {
        self.connection_registry.contains(crate::framework::capabilities::io::Token(connection_token))
            && replies.iter().any(|reply| reply.as_slice() == b"+OK\r\n")
            && self
                .replication_runtime
                .primary_for_connection(
                    crate::framework::distributed::replication::ConnectionId(token_value),
                )
                .is_some_and(|link| {
                    link.dual_state()
                        == crate::framework::distributed::replication::PrimaryDualChannelState::WaitingForMainPsync
                })
    }

    pub(crate) fn is_slot_export_connection(&self, connection_token: usize) -> bool {
        self.connection_registry.get(crate::framework::capabilities::io::Token(connection_token)).is_some_and(|connection| {
            matches!(
                connection.slot_migration_role,
                Some(
                    crate::engine::distributed::cluster::migration::SlotMigrationConnectionRole::Export(
                        _
                    )
                )
            )
        })
    }

    pub(crate) fn route_origin_output(
        &mut self,
        token_value: usize,
        is_slot_export: bool,
        is_replication: bool,
    ) -> bool {
        if is_slot_export {
            let _ = self.arm_slot_export_output(token_value);
            return false;
        }
        if is_replication {
            return !self.enforce_client_output_buffer_limit_for(token_value);
        }
        let _ = self.schedule_ordinary_write(token_value);
        false
    }

    pub(crate) fn flush_origin_reply(
        &mut self,
        connection_token: usize,
        token_value: usize,
        trace_rdb_ok: bool,
    ) {
        let result = self.flush_connection_output(connection_token);
        if let WriteOutcome::Failed(error) = &result {
            crate::server_log!(
                verbose,
                "reply flush failed on token {}: {}",
                token_value,
                error
            );
            self.remove_connection(token_value);
            return;
        }
        if !trace_rdb_ok {
            return;
        }
        let token = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .token();
        let socket = self.transport.tcp_socket_ports(token);
        crate::server_log!(
            debug,
            "dual-rdb primary set-rdb-client-id response writer: phase=flush token={} result={:?} pending_bytes={} socket={:?}",
            token_value,
            result,
            self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
                .output.queue
                .iter()
                .map(Vec::len)
                .sum::<usize>(),
            socket
        );
        if !matches!(&result, WriteOutcome::Complete) {
            self.transport
                .set_interest(token, Interest::ReadWritable)
                .ok();
            crate::server_log!(
                debug,
                "dual-rdb primary set-rdb-client-id response writer: phase=writable-interest token={}",
                token_value
            );
        }
    }

    pub(crate) fn finish_caller_connection(
        &mut self,
        connection_token: usize,
        token_value: usize,
        is_slot_export: bool,
        mark_closing: bool,
    ) {
        if !self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(connection_token))
        {
            return;
        }
        if self.pause_dual_relay_client_if_needed(Token(token_value)) {
            return;
        }
        use crate::engine::command::context::ConnectionState;
        let connection_state = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .state;
        let has_output = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .has_pending_output();
        if connection_state == ConnectionState::CloseAfterReply {
            // Preserve the terminal reply when this turn's write service was
            // already consumed or the socket cannot drain in one write.
            if !is_slot_export
                && has_output
                && self.writes.ordinary.claim_service(Token(token_value))
            {
                if let WriteOutcome::Failed(error) = self.flush_connection_output(connection_token)
                {
                    crate::server_log!(
                        verbose,
                        "close-after-reply flush failed on token {}: {}",
                        token_value,
                        error
                    );
                    self.remove_connection(token_value);
                    return;
                }
            }
            if self.connection_registry[Token(connection_token)].has_pending_output() {
                self.connection_registry[Token(connection_token)].mark_closing();
                self.transport
                    .set_interest(Token(token_value), Interest::Writable)
                    .ok();
                self.schedule_ordinary_write(token_value);
                return;
            }
            self.remove_connection(token_value);
            return;
        }
        if has_output {
            let token = self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .token();
            self.transport
                .set_interest(token, Interest::ReadWritable)
                .ok();
        } else if mark_closing {
            self.remove_connection(token_value);
            return;
        }
        self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .complete_command();
    }
}
