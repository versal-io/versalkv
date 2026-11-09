use super::super::super::ServerWorkerState;
use super::replica_aof_ack_offset;
use crate::framework::capabilities::io::Token;
use crate::framework::distributed::replication::{
    PrimaryDualChannelState, PrimaryLinkState, ReplicaAction,
};

fn format_primary_link_name(
    replica: &crate::framework::distributed::replication::ReplicaConnectionInfo,
) -> String {
    replica.display_name()
}

impl ServerWorkerState {
    /// If this node is a replica with a live streaming link to its primary,
    /// emit a REPLCONF ACK roughly once per second. Mirrors C's
    /// `replicationSendAck()` in serverCron (replication.c); without this,
    /// the primary's `last_ack_time` never advances on idle streams and the
    /// "(streaming sync)" replica-timeout path spuriously fires.
    pub(crate) fn send_replica_periodic_ack(&mut self) {
        // A classic full-sync replica can already parse the command stream
        // while its RDB decoder is still loading. C keeps the primary in
        // BG_RDB_LOAD until the post-load ACK; suppress this premature ACK.
        if self.ctx.is_loading()
            || self.ctx.replication().async_loading
            || self
                .replication_runtime
                .replica_link()
                .is_some_and(|link| link.has_pending_rdb_receive())
        {
            return;
        }
        let Some(connection_token) = self.replication_runtime.replica_link().and_then(|link| {
            self.connection_registry
                .contains(Token(link.connection_id().0))
                .then_some(link.connection_id().0)
        }) else {
            return;
        };
        let now = crate::foundation::io::sys::time::mstime();
        let aof_offset = self
            .replication_runtime
            .replica_link()
            .and_then(|link| replica_aof_ack_offset(&self.ctx, link.applied_position()));
        let action = match self.replication_runtime.replica_link_mut() {
            Some(link) => link.take_periodic_ack(now, aof_offset),
            None => None,
        };
        let Some(ReplicaAction::Send(frame)) = action else {
            return;
        };
        let token = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .token();
        self.queue_client_reply(token, frame);
    }

    /// Iterate connected replicas and disconnect those whose last observed
    /// activity exceeds `repl-timeout`. Two stall detectors, mirroring C's
    /// replicationCron (replication.c:5408-5440):
    ///   - classic and completed dual streaming replicas are timed out using
    ///     `last_ack_time_ms`, i.e. the last REPLCONF ACK received. Dual
    ///     replicas awaiting the RDB/terminal ACK remain in the sync lifecycle.
    ///   - full-sync replicas (diskless fork path) are timed out using
    ///     `last_partial_write_sec`, stamped when the socket first refuses
    ///     to drain an output chunk.
    ///
    /// In both paths the primary link is closed via `remove_connection`,
    /// which emits the "Connection with replica ... lost." notice and
    /// cleans up the diskless/disk-sync relay targets.
    pub(crate) fn disconnect_timedout_replicas(&mut self) {
        self.reconcile_replication_compression();
        let now_ms = crate::foundation::io::sys::time::mstime();
        let now_sec = crate::foundation::io::sys::time::unixtime();
        self.disconnect_timedout_replicas_at(now_ms, now_sec);
    }

    fn reconcile_replication_compression(&mut self) {
        if self.ctx.replication().failover_state != "no-failover" {
            return;
        }
        let mode = self.ctx.config().replication.repl_compression.clone();
        let downstream = self
            .replication_runtime
            .primary_links()
            .values()
            .find(|link| {
                link.state() == PrimaryLinkState::Streaming
                    && matches!(
                        link.dual_state(),
                        PrimaryDualChannelState::Disabled | PrimaryDualChannelState::Online
                    )
                    && crate::engine::distributed::replication::protocol::negotiate_lz4(
                        &mode,
                        &link.replica().capability,
                    ) != link.is_compressed()
            })
            .map(|link| link.connection_id().0);
        if let Some(token) = downstream {
            self.remove_connection(token);
        }
        let upstream = self
            .replication_runtime
            .replica_link()
            .filter(|link| {
                link.is_streaming()
                    && !link.has_pending_rdb_receive()
                    && !(mode == "no" && link.compression_is_plaintext())
                    && crate::engine::distributed::replication::protocol::compression_is_lz4(
                        link.advertised_compression_mode(),
                    ) != crate::engine::distributed::replication::protocol::compression_is_lz4(
                        &mode,
                    )
            })
            .map(|link| link.connection_id().0);
        if let Some(token) = upstream {
            self.tear_down_master_link(token);
        }
    }

    pub(crate) fn disconnect_timedout_replicas_at(&mut self, now_ms: i64, now_sec: i64) {
        let timeout_sec = self.ctx.config().replication.repl_timeout;
        if timeout_sec == 0 {
            return;
        }
        // A graceful shutdown owns the replica-wait deadline. Keep lagging
        // replicas connected during that window so a paused replica can
        // resume and acknowledge the final write; shutdown progress itself
        // decides whether the configured shutdown timeout has elapsed.
        if self.ctx.shutdown_state().initiated && self.ctx.shutdown_state().mstime_deadline != 0 {
            return;
        }
        let timeout_ms = (timeout_sec as i64).saturating_mul(1000);
        let mut to_remove: Vec<(usize, String, &'static str)> = Vec::new();
        for link in self.replication_runtime.primary_links().values() {
            let name = format_primary_link_name(link.replica());
            match link.state() {
                PrimaryLinkState::Streaming => {
                    if !matches!(
                        link.dual_state(),
                        PrimaryDualChannelState::Disabled | PrimaryDualChannelState::Online
                    ) {
                        continue;
                    }
                    if link.last_ack_time_ms() == 0 || link.has_pending_replication() {
                        continue;
                    }
                    if now_ms.saturating_sub(link.last_ack_time_ms()) > timeout_ms {
                        crate::server_log!(
                            warning,
                            "Disconnecting timedout replica (streaming sync): {}",
                            name
                        );
                        to_remove.push((link.connection_id().0, name, "streaming"));
                    }
                }
                PrimaryLinkState::WaitingForReplicaAck if link.last_ack_time_ms() != 0 => {
                    if now_ms.saturating_sub(link.last_ack_time_ms()) > timeout_ms {
                        crate::server_log!(
                            warning,
                            "Disconnecting timedout replica (streaming sync): {}",
                            name
                        );
                        to_remove.push((link.connection_id().0, name, "streaming"));
                    }
                }
                PrimaryLinkState::PreparingFullResync
                | PrimaryLinkState::SendingRdb
                | PrimaryLinkState::WaitingForReplicaAck => {
                    let pw = link.last_partial_write_sec();
                    if pw != 0 && now_sec.saturating_sub(pw) > timeout_sec as i64 {
                        crate::server_log!(
                            warning,
                            "Disconnecting timedout replica (full sync): {}",
                            name
                        );
                        to_remove.push((link.connection_id().0, name, "full"));
                    }
                }
                _ => {}
            }
        }
        for (token_value, _name, _kind) in to_remove {
            self.remove_connection(token_value);
        }
    }
}
