//! Standalone FAILOVER orchestration.
//! L5 selects the target and executes connection effects while grouped state
//! transitions remain owned by the L3 replication state.
use super::*;

impl ServerWorkerState {
    pub(crate) fn process_pending_failover(&mut self) {
        if !self.ctx.replication().is_waiting_for_failover_sync() {
            return;
        }
        let requested_target = self.ctx.replication().pending_failover_target().cloned();
        let now = crate::foundation::io::sys::time::mstime();
        let deadline = self.ctx.replication().failover_end_time_ms();
        let timed_out = deadline > 0 && now >= deadline;
        if self.ctx.client_pause().is_paused(now) == crate::framework::context::PauseAction::None {
            self.ctx.client_pause_mut().action = crate::framework::context::PauseAction::Write;
            self.ctx.client_pause_mut().end_time_ms = i64::MAX;
            self.ctx.client_pause_mut().reason = "failover_in_progress";
        }

        if self.ctx.replication().failover_sync_offset().is_none() {
            self.propagate_to_replicas_with_ack(&[], true);
            let offset = self.ctx.replication().primary_repl_offset();
            self.ctx.replication_mut().set_failover_sync_offset(offset);
            return;
        }
        // C compares each replica ACK against the live primary offset. The
        // pause stops client writes, but replication keepalives may still move
        // the offset while we wait, so a one-time snapshot can become stale.
        let sync_offset = self.ctx.replication().primary_repl_offset();
        self.ctx
            .replication_mut()
            .set_failover_sync_offset(sync_offset);
        let target = self
            .replication_runtime
            .primary_links()
            .values()
            .find(|link| {
                if let Some((host, port)) = &requested_target {
                    link.replica().ip.eq_ignore_ascii_case(host) && link.replica().port == *port
                } else {
                    link.state() == PrimaryLinkState::Streaming
                        && link
                            .last_ack_position()
                            .is_some_and(|offset| offset >= sync_offset)
                }
            });
        let Some(target) = target else {
            if timed_out {
                if self.ctx.replication().is_forced_failover() {
                    if let Some((host, port)) = requested_target {
                        crate::server_log!(
                            notice,
                            "FAILOVER to {}:{} time out exceeded, failing over.",
                            host,
                            port
                        );
                        self.complete_failover(host, port);
                    }
                } else {
                    self.clear_pending_failover();
                }
            }
            return;
        };
        let connection_id = target.connection_id();
        let target_offset = target.last_ack_position();
        let host = target.replica().ip.clone();
        let port = target.replica().port;
        if !self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(connection_id.0))
        {
            if timed_out && !self.ctx.replication().is_forced_failover() {
                self.clear_pending_failover();
            }
            return;
        }
        let target_offset = self
            .replication_runtime
            .primary_for_connection(connection_id)
            .map(|link| link.last_ack_position())
            .unwrap_or(target_offset);
        if target_offset.is_none_or(|offset| offset < sync_offset)
            && !(timed_out && self.ctx.replication().is_forced_failover())
        {
            if timed_out {
                self.clear_pending_failover();
            }
            return;
        }

        if timed_out && self.ctx.replication().is_forced_failover() {
            crate::server_log!(
                notice,
                "FAILOVER to {}:{} time out exceeded, failing over.",
                host,
                port
            );
        } else {
            crate::server_log!(
                notice,
                "Failover target {}:{} is synced, failing over.",
                host,
                port
            );
        }

        self.complete_failover(host, port);
    }

    fn complete_failover(&mut self, host: String, port: u16) {
        let master = crate::framework::distributed::replication::MasterInfo {
            host: host.clone(),
            port,
        };
        let replid = self.ctx.replication().backlog.replid().to_string();
        let offset = self.ctx.replication().primary_repl_offset();
        let selected_db = self.ctx.repl_seldb().max(0) as u32;
        // Match C replicationSetPrimary(): a primary synthesizes a cached
        // primary from its own current history before following the promoted
        // replica, allowing the reverse link to continue with PSYNC2.
        self.replication_runtime
            .set_replica_resume(master, replid, offset, selected_db);
        self.ctx.config_mut().replication.master_host = Some(host);
        self.ctx.config_mut().replication.master_port = port;
        self.ctx.config_mut().replication.master_link_status = "down".to_string();
        self.ctx.replication_mut().enter_failover_handoff();
    }

    pub(super) fn clear_pending_failover(&mut self) {
        self.ctx
            .replication_mut()
            .reset_failover_and_disconnect_window();
        if self.ctx.client_pause().reason == "failover_in_progress" {
            self.ctx.client_pause_mut().action = crate::framework::context::PauseAction::None;
            self.ctx.client_pause_mut().end_time_ms = 0;
            self.ctx.client_pause_mut().reason = "none";
            self.ctx.set_pending_unpause(true);
        }
    }

    pub(crate) fn clear_accepted_failover(&mut self) {
        let accepted = self
            .replication_runtime
            .replica_link_mut()
            .is_some_and(ReplicaLink::take_failover_accepted);
        if accepted {
            let blocked_tokens: Vec<_> = self
                .ctx
                .blocking_registry()
                .blocked_tokens()
                .into_iter()
                .collect();
            self.ctx
                .pending_blocked_to_reject_mut()
                .extend(blocked_tokens);
            // C disconnectOrRedirectAllBlockedClients() deliberately leaves
            // BLOCKED_POSTPONE clients alone. A failover-owned pause is lowered
            // here so those commands are reprocessed; an explicit CLIENT PAUSE
            // remains until its owner (including Sentinel) sends UNPAUSE.
            self.clear_pending_failover();
        }
    }
}
