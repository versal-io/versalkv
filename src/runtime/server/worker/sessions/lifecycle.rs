use super::super::reactor::transport::RuntimeConnectedPeer;
use super::super::{PendingRemovalOwner, ServerWorkerState};
use crate::framework::capabilities::connection::{Connection, ConnectionState};
use crate::framework::capabilities::io::Token;
use crate::framework::distributed::replication::{ConnectionId, MasterReconnectCause, ReplicaId};
use crate::runtime::server::worker::reactor::sources::SourceOwner;
use std::io::Write;

/// Frozen facts for a connection teardown, captured before any cleanup mutates
/// shared owner state. Execution then proceeds in a fixed order without
/// re-reading owners whose indexes have already changed.
struct ConnectionTeardownPlan {
    token_value: usize,
    connection_id: ConnectionId,
    public_client_id: Option<u64>,
    migration: Option<MigrationTeardown>,
    was_master_link: bool,
    master_link_was_established: bool,
    short_read_master: bool,
    preserve_serving_db_on_short_read: bool,
    lost_replica_name: Option<String>,
    relayed_replica: Option<ReplicaId>,
    direct_child_main_target: bool,
    direct_child_rdb_target: bool,
    preserve_main_after_rdb_close: bool,
    defer_main_close_pin: bool,
    peer: Option<ConnectionId>,
}

struct MigrationTeardown {
    job_id: crate::engine::distributed::cluster::migration::MigrationJobId,
    fail_message: Option<&'static str>,
}

fn dual_psync_grace_seconds(debug_delay_sec: i64, diskless_delay_sec: i64) -> i64 {
    if debug_delay_sec > 0 {
        debug_delay_sec.max(diskless_delay_sec)
    } else {
        diskless_delay_sec.max(5)
    }
}

fn protected_mode_rejects_peer(
    protected_mode: bool,
    default_user_nopass: bool,
    peer_addr: std::net::SocketAddr,
) -> bool {
    protected_mode && default_user_nopass && !peer_addr.ip().is_loopback()
}

impl ServerWorkerState {
    pub(crate) fn close_listeners_for_shutdown(&mut self) {
        let listeners = self.listeners.all.clone();
        for token in listeners {
            self.remove_listener(token);
        }
    }

    pub fn connected_clients(&self) -> usize {
        self.connection_registry.len()
    }

    pub(crate) fn add_connection(&mut self, client: RuntimeConnectedPeer) {
        if let Err(error) = self.route_runtime_source(client.token, SourceOwner::Connection) {
            crate::server_log!(
                warning,
                "Failed to route connection token {}: {}",
                client.token.0,
                error
            );
            return;
        }
        let default_user_requires_auth = !crate::engine::extension::acl::default_user_has_nopass();
        let mut conn = Connection::new(client.token, client.peer_addr, default_user_requires_auth);
        let client_id = conn.client_id();
        let io_handles = self
            .transport
            .duplicate_stream(client.token)
            .and_then(|read| {
                self.transport
                    .duplicate_stream(client.token)
                    .map(|write| (read, write))
            });
        {
            let users = crate::engine::extension::acl::acl_users().lock().unwrap();
            if let Some(default_user) = users.get("default") {
                if !default_user.is_on() {
                    conn.authenticated = false;
                }
            }
        }
        self.connection_registry
            .register(conn)
            .expect("a newly accepted transport token and client id must be unique");
        self.refresh_client_memory_usage(client.token, true);
        match io_handles {
            Ok((read, write)) => {
                self.io_threads
                    .register_connection(client.token, read, write);
            }
            Err(error) => {
                crate::server_log!(
                    warning,
                    "Threaded IO unavailable for client token {}: {}",
                    client.token.0,
                    error
                );
            }
        }
        self.ctx.incr_connection();
        let now = std::time::Instant::now();
        self.ctx.client_infos_mut().insert(
            client.token.0,
            crate::engine::state::db::ClientInfo {
                last_cmd: String::new(),
                flags: "N".to_string(),
                db: 0,
                name: String::new(),
                rbs: 16384,
                watched_keys: 0,
                lib_name: String::new(),
                lib_ver: String::new(),
                last_interaction: now,
            },
        );
        crate::server_log!(
            verbose,
            "Accepted from {} (client id {}, token {})",
            client.peer_addr,
            client_id,
            client.token.0
        );
    }

    pub(crate) fn remove_connection(&mut self, token_value: usize) {
        self.remove_connection_with_reconnect_cause(token_value, None);
    }

    pub(crate) fn remove_connection_with_reconnect_cause(
        &mut self,
        token_value: usize,
        reconnect_cause: Option<MasterReconnectCause>,
    ) {
        let token = Token(token_value);
        // Match C networking.c freeClient(): protected command owners survive
        // nested event processing and are retired after the outer command unwinds.
        if self
            .connection_registry
            .get(token)
            .is_some_and(|connection| connection.protected)
        {
            self.connection_registry[token].mark_closing();
            return;
        }
        // Already isolated from every business index; only its L1 registration
        // is still retained for retry, so re-entry is a no-op.
        if self
            .pending_transport_removals
            .contains_key(&Token(token_value))
        {
            return;
        }
        if self.io_threads.is_close_pending(Token(token_value)) {
            return;
        }
        self.retire_runtime_source(token);
        self.full_sync.pending_dual_rdb_snapshots.remove(&token);
        self.cancel_runtime_runnable(
            crate::runtime::server::worker::reactor::RunnableKey::ClientRead(token),
        );
        self.cancel_runtime_runnable(
            crate::runtime::server::worker::reactor::RunnableKey::ClientWrite(token),
        );
        self.cancel_runtime_runnable(
            crate::runtime::server::worker::reactor::RunnableKey::SlotExportWrite(token),
        );
        self.cancel_runtime_runnable(
            crate::runtime::server::worker::reactor::RunnableKey::DeferredReadiness(token),
        );
        self.blocked_events.remove_deferred(token);
        self.writes.remove(token);
        self.client_hup_drains.remove(&token);
        let peer = self
            .connection_registry
            .get(token)
            .map(|conn| conn.peer_addr);
        let replication_role = self.replication_runtime.role(
            crate::framework::distributed::replication::ConnectionId(token_value),
        );
        crate::server_log!(
            debug,
            "remove connection token={} peer={:?} role={:?} active_child={}",
            token_value,
            peer,
            replication_role,
            self.ctx.persistence().has_active_child()
        );
        if self.ctx.persistence().has_active_child() {
            if let Some(connection_token) = self
                .connection_registry
                .contains(crate::framework::capabilities::io::Token(token_value))
                .then_some(token_value)
            {
                if self
                    .connection_registry
                    .contains(crate::framework::capabilities::io::Token(connection_token))
                {
                    let _ = self.transport.shutdown(
                        self.connection_registry
                            [crate::framework::capabilities::io::Token(connection_token)]
                        .token(),
                    );
                }
            }
        }
        // Isolate from all L4/L5 business state first, then complete or defer the
        // L1 deregister. The connection never survives in a business index once
        // teardown starts.
        let defer_transport_removal = self.io_threads.begin_close(token);
        self.finalize_connection_removal(token_value, reconnect_cause);
        if !defer_transport_removal {
            self.deregister_or_defer(token, PendingRemovalOwner::Client);
        }
    }

    fn finalize_connection_removal(
        &mut self,
        token_value: usize,
        reconnect_cause: Option<MasterReconnectCause>,
    ) {
        let plan = self.gather_connection_teardown_plan(token_value);
        self.execute_connection_teardown(plan, reconnect_cause);
    }

    fn gather_connection_teardown_plan(&self, token_value: usize) -> ConnectionTeardownPlan {
        use crate::engine::distributed::cluster::migration::{
            ExportPhase, MigrationPhase, SlotMigrationConnectionRole,
        };
        use crate::framework::distributed::replication::ReplicationConnection;
        let token = Token(token_value);
        let connection_id = ConnectionId(token_value);
        let public_client_id = self
            .connection_registry
            .get(token)
            .map(|connection| connection.client_id());
        let migration = self
            .connection_registry
            .get(token)
            .and_then(|connection| connection.slot_migration_role.as_ref())
            .cloned()
            .map(|role| {
                let job_id = role.job_id().clone();
                let is_export = matches!(role, SlotMigrationConnectionRole::Export(_));
                let phase = self
                    .ctx
                    .cluster_state()
                    .as_deref()
                    .and_then(|state| state.migrations.get(&job_id))
                    .map(|job| job.phase);
                let export_was_connecting = is_export
                    && matches!(
                        phase,
                        Some(MigrationPhase::Export(ExportPhase::Connecting))
                    );
                let has_recorded_failure = export_was_connecting
                    && self
                        .ctx
                        .cluster_state()
                        .as_deref()
                        .and_then(|state| state.migrations.get(&job_id))
                        .is_some_and(|job| job.message.is_some());
                let preserve_granted_export = is_export
                    && matches!(
                        phase,
                        Some(MigrationPhase::Export(ExportPhase::FailoverGranted))
                    );
                let fail_message = if preserve_granted_export {
                    None
                } else if has_recorded_failure {
                    // A readiness failure may already have consumed SO_ERROR and
                    // recorded the concrete connect error before teardown runs.
                    // Do not replace that diagnostic with the generic close path.
                    None
                } else if export_was_connecting {
                    Some("Unable to connect to target node")
                } else {
                    Some(match role {
                        SlotMigrationConnectionRole::Export(_) => {
                            "Connection lost to target. Check CLUSTER GETSLOTMIGRATIONS on the target node for more information."
                        }
                        SlotMigrationConnectionRole::Import(_) => {
                            "Connection lost to source. Check CLUSTER GETSLOTMIGRATIONS on the source node for more information."
                        }
                    })
                };
                MigrationTeardown {
                    job_id,
                    fail_message,
                }
            });
        let was_master_link = self.is_master_link(token_value);
        let master_link_was_established = was_master_link
            && self
                .replication_runtime
                .replica_link()
                .is_some_and(|link| link.is_streaming());
        let replica_rdb_loading = self.ctx.is_loading() || self.ctx.replication().async_loading;
        let preserve_serving_db_on_short_read = self
            .replication_runtime
            .replica_link()
            .is_some_and(|link| link.rdb_receive_preserves_serving_db());
        let short_read_master = was_master_link
            && (replica_rdb_loading
                || self
                    .replication_runtime
                    .replica_link()
                    .is_some_and(|link| {
                        link.is_receiving_rdb()
                            || link.has_pending_rdb_receive()
                            || matches!(
                                link.rdb_channel_state(),
                                crate::framework::distributed::replication::ReplicaRdbChannelState::ReceivingRdb
                            )
                    }));
        let lost_replica_name = self
            .replication_runtime
            .primary_for_connection(connection_id)
            .map(|link| link.replica().display_name());
        let has_relay_target = self.full_sync.diskless_relay.is_some()
            || self.full_sync.direct_dual_rdb_child.is_some()
            || self.full_sync.disk_sync_batch.is_some();
        let relayed_replica = has_relay_target
            .then(|| {
                self.replication_runtime
                    .replica_id_for_connection(connection_id)
            })
            .flatten();
        let role = self.replication_runtime.role(connection_id);
        let direct_child_main_target = matches!(role, Some(ReplicationConnection::Primary(_)))
            && relayed_replica.is_some_and(|id| {
                self.full_sync
                    .direct_dual_rdb_child
                    .as_ref()
                    .is_some_and(|child| child.targets.contains(&id))
            });
        let direct_child_rdb_target =
            matches!(role, Some(ReplicationConnection::RdbChannel { .. }))
                && self
                    .full_sync
                    .direct_dual_rdb_child
                    .as_ref()
                    .is_some_and(|child| child.rdb_connections.contains(&connection_id));
        let preserve_main_after_rdb_close = matches!(
            role,
            Some(ReplicationConnection::RdbChannel { main }) if self
                .replication_runtime
                .primary_for_connection(main)
                .is_some_and(|link| {
                    use crate::framework::distributed::replication::PrimaryDualChannelState;
                    if link.dual_state() == PrimaryDualChannelState::WaitingForMainPsync {
                        return true;
                    }
                    link.dual_state() == PrimaryDualChannelState::Online
                        || link.dual_state() == PrimaryDualChannelState::WaitingForReplicaAck
                })
        );
        let defer_main_close_pin = matches!(role, Some(ReplicationConnection::Primary(_)))
            && self
                .replication_runtime
                .primary_for_connection(connection_id)
                .is_some_and(|link| {
                    matches!(
                        link.dual_state(),
                        crate::framework::distributed::replication::PrimaryDualChannelState::WaitingForMainPsync
                    )
                });
        let peer = self.replication_runtime.channel_pair_for(connection_id);
        ConnectionTeardownPlan {
            token_value,
            connection_id,
            public_client_id,
            migration,
            was_master_link,
            master_link_was_established,
            short_read_master,
            preserve_serving_db_on_short_read,
            lost_replica_name,
            relayed_replica,
            direct_child_main_target,
            direct_child_rdb_target,
            preserve_main_after_rdb_close,
            defer_main_close_pin,
            peer,
        }
    }

    fn execute_connection_teardown(
        &mut self,
        plan: ConnectionTeardownPlan,
        reconnect_cause: Option<MasterReconnectCause>,
    ) {
        let token = Token(plan.token_value);
        self.socket_owners.remove(&token);
        // 1. Detach migration.
        if let Some(migration) = &plan.migration {
            self.on_slot_snapshot_target_closed(&migration.job_id);
            self.release_slot_export_connection_output(plan.token_value);
            self.slot_migration_runtime
                .remove_connection(plan.token_value);
            if let Some(message) = migration.fail_message {
                self.fail_slot_migration(&migration.job_id, message);
            }
        }
        // 2. Cancel full-sync/child ownership.
        self.direct_dual_rdb_child_marker_sent();
        let direct_child_drop = self.drop_direct_dual_rdb_child_connection(plan.connection_id);
        if direct_child_drop
            == crate::runtime::server::worker::distributed::replication::diskless_child::DirectDualRdbDropOutcome::CompletedUnpaired
        {
            let grace_sec = dual_psync_grace_seconds(
                self.ctx.config().server.debug_delay_rdb_client_free_seconds,
                self.ctx.config().replication.repl_diskless_sync_delay,
            );
            let deadline = crate::foundation::io::sys::time::mstime()
                .saturating_add(grace_sec.saturating_mul(1_000));
            let retained = plan.public_client_id.is_some_and(|public_client_id| {
                self.replication_runtime
                    .retain_completed_unpaired_rdb_wait(
                        plan.connection_id,
                        public_client_id,
                        deadline,
                    )
            });
            if !retained {
                crate::server_log!(
                    warning,
                    "Unable to retain completed unpaired RDB wait for connection {}",
                    plan.connection_id.0
                );
            }
        }
        if plan.was_master_link {
            self.take_and_dispose_replica_pending_buffer();
        }
        // 3. Detach replication pair/session.
        if plan.defer_main_close_pin {
            let grace_sec = dual_psync_grace_seconds(
                self.ctx.config().server.debug_delay_rdb_client_free_seconds,
                self.ctx.config().replication.repl_diskless_sync_delay,
            );
            let grace_ms = grace_sec.saturating_mul(1_000);
            if grace_ms > 0 {
                let deadline = crate::foundation::io::sys::time::mstime().saturating_add(grace_ms);
                if self
                    .replication_runtime
                    .defer_psync_wait_for_main(plan.connection_id, deadline)
                {
                    crate::server_log!(
                        debug,
                        "dual-rdb grace: preserve pin after main={} close for {}ms",
                        plan.connection_id.0,
                        grace_ms
                    );
                } else {
                    self.replication_runtime
                        .set_grace_psync_wait_deadline(plan.connection_id, deadline);
                }
            }
        }
        if let Some(peer) = plan.peer {
            if plan.preserve_main_after_rdb_close {
                if let Some(main) = self
                    .replication_runtime
                    .detach_rdb_channel_preserving_main(plan.connection_id)
                {
                    let grace_sec = dual_psync_grace_seconds(
                        self.ctx.config().server.debug_delay_rdb_client_free_seconds,
                        self.ctx.config().replication.repl_diskless_sync_delay,
                    );
                    let grace_ms = grace_sec.saturating_mul(1_000);
                    if grace_ms > 0 {
                        if let Some(link) =
                            self.replication_runtime.primary_for_connection_mut(main)
                        {
                            if matches!(
                                link.dual_state(),
                                crate::framework::distributed::replication::PrimaryDualChannelState::WaitingForMainPsync
                            ) {
                                let deadline = crate::foundation::io::sys::time::mstime()
                                    .saturating_add(grace_ms);
                                link.rdb_channel_wait_deadline_ms = Some(deadline);
                                self.replication_runtime
                                    .set_grace_psync_wait_deadline(main, deadline);
                                crate::server_log!(
                                    debug,
                                    "dual-rdb grace: postpone main={} RDB client release for {}ms",
                                    main.0,
                                    grace_ms
                                );
                            }
                        }
                    }
                }
            } else {
                self.replication_runtime
                    .remove_channel_pair(plan.connection_id);
                self.remove_connection(peer.0);
            }
        }
        // 4. Isolate business indexes and release accounting.
        self.discard_blocked_client(token);
        self.ctx
            .replication_mut()
            .view
            .replica_ack_offsets
            .remove(&plan.token_value);
        self.ctx
            .replication_mut()
            .view
            .replica_info
            .remove(&plan.token_value);
        self.ctx
            .replication_mut()
            .view
            .channel_info
            .remove(&plan.token_value);
        self.replication_runtime
            .remove_connection(plan.connection_id);
        if let Some(ref name) = plan.lost_replica_name {
            crate::server_log!(notice, "Connection with replica {} lost.", name);
        }
        if let Some(id) = plan.relayed_replica {
            if plan.direct_child_main_target || plan.direct_child_rdb_target {
                self.drop_direct_dual_rdb_child_target(id);
            }
            let _ = self.on_diskless_target_closed(id);
            self.drop_disk_sync_batch_target(id);
            self.refresh_replication_view();
        }
        self.refresh_replication_view();
        if plan.was_master_link {
            let short_read_master = self.current_master_link_short_read(plan.short_read_master);
            self.ctx
                .replication_mut()
                .observe_primary_disconnected(crate::foundation::io::sys::time::mstime());
            self.ctx.config_mut().replication.master_link_status = "down".to_string();
            if short_read_master {
                if !plan.preserve_serving_db_on_short_read {
                    self.ctx.flush_all_dbs();
                }
                let zero = crate::engine::distributed::replication::ReplicationOffset::try_from(0)
                    .expect("zero is a valid replication offset");
                self.ctx.replication_mut().slave_repl_offset = zero;
                self.ctx.replication_mut().slave_read_repl_offset = zero;
                crate::server_log!(
                    warning,
                    "Failed trying to load the PRIMARY synchronization DB from socket, check server logs."
                );
                crate::server_log!(
                    warning,
                    "Internal error in RDB reading: Failure loading rdb format from socket, assuming connection error, resuming operation."
                );
            }
            self.ctx.replication_mut().async_loading = false;
            self.ctx.set_loading(false);
            let now_ms = crate::foundation::io::sys::time::mstime();
            let inferred_cause = if short_read_master {
                MasterReconnectCause::RdbTransferFailed
            } else if plan.master_link_was_established {
                MasterReconnectCause::EstablishedLinkLost
            } else {
                MasterReconnectCause::HandshakeFailed
            };
            let cause = reconnect_cause.unwrap_or(inferred_cause);
            self.replication_runtime
                .schedule_master_reconnect(now_ms, cause);
        }
        if self.connection_registry.contains(token) {
            self.remove_client_memory_contribution(plan.token_value);
            let conn = self
                .connection_registry
                .remove(token)
                .expect("connection presence was checked before removal");
            crate::server_log!(verbose, "Closing conn from {}", conn.peer_addr);
            self.ctx.add_net_input_bytes(conn.tot_net_in);
            self.ctx.add_net_output_bytes(conn.tot_net_out);
            if conn.monitoring {
                self.ctx.unregister_monitor_client(conn.client_id());
            }
            self.ctx.decr_connection();
        }
        self.ctx.client_infos_mut().remove(&plan.token_value);
        self.ctx.pubsub_mut().remove_client(plan.token_value);
        self.ctx.tracking_mut().clear_client(plan.token_value);
    }

    fn current_master_link_short_read(&self, planned_short_read: bool) -> bool {
        if !planned_short_read {
            return false;
        }
        if self.ctx.is_loading() || self.ctx.replication().async_loading {
            return true;
        }
        self.replication_runtime.replica_link().is_none_or(|link| {
                link.is_receiving_rdb()
                    || link.has_pending_rdb_receive()
                    || matches!(
                        link.rdb_channel_state(),
                        crate::framework::distributed::replication::ReplicaRdbChannelState::ReceivingRdb
                    )
            })
    }

    pub(crate) fn handle_listener_token(&mut self, listener: Token) {
        if !matches!(
            self.runtime_sources
                .resolve_token(listener)
                .map(|route| &route.owner),
            Some(SourceOwner::Listener)
        ) || !self.transport.is_registered(listener)
        {
            return;
        }
        let accept_budget = self.listener_accept_budget();
        if accept_budget == 0 {
            return;
        }
        let maxclients = self.ctx.config().server.maxclients;
        for _ in 0..accept_budget {
            match self.transport.accept(listener) {
                Ok(Some(c)) => {
                    let keepalive = self.ctx.config().server.tcp_keepalive;
                    if let Err(error) = self.transport.apply_tcp_keepalive(
                        c.token,
                        (keepalive != 0).then(|| std::time::Duration::from_secs(keepalive)),
                    ) {
                        crate::server_log!(
                            warning,
                            "failed to configure TCP keepalive for {}: {}",
                            c.peer_addr,
                            error
                        );
                    }
                    if protected_mode_rejects_peer(
                        self.ctx.config().server.protected_mode,
                        crate::engine::extension::acl::default_user_has_nopass(),
                        c.peer_addr,
                    ) {
                        self.reject_protected_mode_client(c);
                    } else if self.connected_clients() >= maxclients as usize {
                        self.reject_accepted_client(c, maxclients);
                    } else {
                        self.add_connection(c);
                    }
                }
                Ok(None) => {
                    break;
                }
                Err(error) => {
                    crate::server_log!(warning, "accept error: {}", error);
                    break;
                }
            }
        }
    }

    fn listener_accept_budget(&self) -> usize {
        self.ctx.config().server.max_new_connections_per_cycle as usize
    }

    fn reject_accepted_client(&mut self, client: RuntimeConnectedPeer, maxclients: u32) {
        if let Some(mut stream) = self.transport.stream(client.token) {
            let _ = stream.write_all(b"-ERR max number of clients reached\r\n");
        }
        self.ctx.incr_rejected_connections();
        // A rejected client was never inserted into any business index, so
        // teardown is just the L1 deregister (deferred to a retry on failure).
        self.deregister_or_defer(client.token, PendingRemovalOwner::RejectedClient);
        crate::server_log!(
            verbose,
            "Refused connection from {} (maxclients {})",
            client.peer_addr,
            maxclients
        );
    }

    fn reject_protected_mode_client(&mut self, client: RuntimeConnectedPeer) {
        if let Some(mut stream) = self.transport.stream(client.token) {
            let _ = stream.write_all(
                b"-DENIED Running in protected mode because protected mode is enabled and no password is set for the default user. In this mode connections are only accepted from the loopback interface.\r\n",
            );
        }
        self.ctx.incr_rejected_connections();
        self.deregister_or_defer(client.token, PendingRemovalOwner::RejectedClient);
        crate::server_log!(
            verbose,
            "Refused non-loopback connection from {} in protected mode",
            client.peer_addr
        );
    }

    pub(crate) fn is_auth_allowed(cmd_name: &[u8]) -> bool {
        let name = std::str::from_utf8(cmd_name)
            .map(|s| s.to_ascii_lowercase())
            .unwrap_or_default();
        matches!(
            name.as_str(),
            "auth" | "hello" | "quit" | "command" | "reset"
        )
    }

    pub(crate) fn cleanup_closed(&mut self) {
        let mut to_remove = Vec::new();
        for (_, conn) in self.connection_registry.iter() {
            if conn.state == ConnectionState::Closing && !conn.has_pending_output() {
                to_remove.push(conn.token().0);
            }
        }
        for t in to_remove {
            self.remove_connection(t);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        protected_mode_rejects_peer, PendingRemovalOwner, RuntimeConnectedPeer, ServerWorkerState,
    };
    use crate::engine::command::context::{BlockedCmd, BlockedState};
    use crate::engine::distributed::cluster::migration::{
        MigrationJob, MigrationJobId, MigrationOperation, SlotMigrationConnectionRole, SlotRanges,
    };
    use crate::engine::distributed::cluster::{ClusterState, NodeId, SlotRange};
    use crate::engine::distributed::replication::{PsyncRequestOffset, ReplicationOffset};
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::capabilities::io::Token;
    use crate::framework::distributed::replication::{
        ConnectionId, MasterInfo, PendingReplicationBuffer, PrimaryDualChannelState,
        PrimaryLinkState, ReplicaConnectionInfo, ReplicaEvent, ReplicaLink, ReplicaRdbChannelState,
        ReplicationApply,
    };
    use crate::framework::CommandArgs;
    use bytes::Bytes;
    use std::fs::File;
    use std::net::SocketAddr;

    fn offset(value: i64) -> ReplicationOffset {
        ReplicationOffset::try_from(value).expect("valid test offset")
    }

    fn blocked_completion_stats(
        backend: &ServerWorkerState,
        name: &str,
    ) -> (u64, u64, Option<(u64, u64, Option<u64>)>) {
        (
            backend.ctx.stats().total_commands_processed,
            backend.ctx.stats().active_time_usec,
            backend
                .ctx
                .stats()
                .cmd_stats
                .get(name)
                .map(|stat| (stat.calls, stat.usec, stat.percentile_usec(50.0))),
        )
    }

    fn assert_blocked_disconnect_is_not_completed(cmd: BlockedCmd, name: &'static [u8]) {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let token = Token(93);
        let args = CommandArgs::from_parts(vec![Bytes::from_static(name)]);
        let mut connection = Connection::new_fake();
        connection.set_token_for_test(token);
        connection.blocked = Some(BlockedState {
            cmd,
            keys: Vec::new(),
            timeout_ms: 0,
            start_time_ms: crate::foundation::io::sys::time::mstime().saturating_sub(10),
            orig_args: args,
            req_bytes: 24,
            orig_wrapped: None,
        });
        backend.connection_registry.insert(connection);
        let session = backend.connection_registry[token].session_id();
        backend
            .ctx
            .blocking_registry_mut()
            .register_keyless_client(session);
        let name = String::from_utf8_lossy(name).to_ascii_lowercase();
        let before = blocked_completion_stats(&backend, &name);

        backend.remove_connection(token.0);

        assert_eq!(blocked_completion_stats(&backend, &name), before);
        assert!(!backend.ctx.blocking_registry().is_client_blocked(&session));
    }

    #[test]
    fn disconnected_wait_and_waitaof_are_cancelled_without_completion_stats() {
        assert_blocked_disconnect_is_not_completed(
            BlockedCmd::Wait {
                required: 1,
                target_offset: offset(1),
            },
            b"WAIT",
        );
        assert_blocked_disconnect_is_not_completed(
            BlockedCmd::WaitAof {
                numlocal: 1,
                numreplicas: 1,
                target_offset: offset(1),
            },
            b"WAITAOF",
        );
    }

    #[test]
    fn repeated_disconnect_removes_deferred_readiness_payload_and_runnable() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let token = Token(94);
        let mut connection = Connection::new_fake();
        connection.set_token_for_test(token);
        backend.connection_registry.insert(connection);
        for _ in 0..32 {
            backend.blocked_events.defer(
                crate::runtime::server::worker::reactor::blocked_events::DeferredReadyEvent::new(
                    token, true, false, true, false,
                ),
            );
            backend.schedule_runtime_runnable_next_turn(
                crate::runtime::server::worker::reactor::RunnableKey::DeferredReadiness(token),
            );
        }
        assert!(backend.blocked_events.has_deferred_read(token));

        backend.remove_connection(token.0);
        backend.remove_connection(token.0);

        assert!(!backend.blocked_events.has_deferred_read(token));
        assert!(backend.blocked_events.take_deferred(token).is_none());
        assert!(!backend.event_batch.contains_runnable(
            crate::runtime::server::worker::reactor::RunnableKey::DeferredReadiness(token)
        ));
    }

    #[test]
    fn protected_mode_uses_the_default_acl_user_nopass_state() {
        let external = SocketAddr::from(([192, 0, 2, 1], 6379));
        assert!(protected_mode_rejects_peer(true, true, external));
        assert!(!protected_mode_rejects_peer(true, false, external));
        assert!(!protected_mode_rejects_peer(
            true,
            true,
            SocketAddr::from(([127, 0, 0, 1], 6379))
        ));
        assert!(!protected_mode_rejects_peer(false, true, external));
    }

    #[test]
    fn debug_grace_delay_overrides_default_minimum() {
        assert_eq!(super::dual_psync_grace_seconds(2, 0), 2);
        assert_eq!(super::dual_psync_grace_seconds(0, 2), 5);
        assert_eq!(super::dual_psync_grace_seconds(0, 8), 8);
        assert_eq!(super::dual_psync_grace_seconds(2, 8), 8);
    }

    #[test]
    fn listener_accept_budget_uses_configured_c_compatible_limit() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        assert_eq!(backend.listener_accept_budget(), 10);

        backend
            .ctx
            .config_mut()
            .server
            .max_new_connections_per_cycle = 25;
        assert_eq!(backend.listener_accept_budget(), 25);
    }

    #[test]
    fn failed_client_deregistration_isolates_owner_and_retries_l1_only() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let token = listener.token;
        backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
        backend
            .transport
            .deregister_for_test(token)
            .expect("deregister test transport");

        backend.remove_connection(token.0);

        // Centralized teardown: the owner is isolated from every business index
        // immediately; only the retained L1 registration + a minimal retry entry
        // survive the failed deregister.
        assert_eq!(backend.connected_clients(), 0);
        assert!(!backend
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token.0)));
        assert_eq!(
            backend
                .pending_transport_removals
                .get(&token)
                .map(|removal| removal.owner_kind),
            Some(PendingRemovalOwner::Client)
        );
        assert!(backend.transport.is_registered(token));

        backend
            .transport
            .reregister_for_test(token)
            .expect("reregister test transport");
        backend.run_background_tasks();

        assert!(!backend.pending_transport_removals.contains_key(&token));
        assert!(!backend.transport.is_registered(token));
    }

    #[test]
    fn protected_client_request_resolves_stable_client_id_to_transport_token() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer_addr = SocketAddr::from(([127, 0, 0, 1], 0));

        let target_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let target_token = target_listener.token;
        backend.add_connection(RuntimeConnectedPeer {
            token: target_token,
            peer_addr,
        });
        let target_slab = target_token.0;
        let target_client_id = backend.connection_registry
            [crate::framework::capabilities::io::Token(target_slab)]
        .client_id();

        backend.ctx.protect_client(target_client_id);
        backend.drain_protected_clients();

        assert!(
            backend.connection_registry[crate::framework::capabilities::io::Token(target_slab)]
                .protected
        );
        assert!(backend.ctx.protected_clients().is_empty());
    }

    #[test]
    fn removing_connect_failed_export_preserves_connect_error_message() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6380, 16_380);
        let id = MigrationJobId::parse(b"cccccccccccccccccccccccccccccccccccccccc").unwrap();
        let job = MigrationJob::new(
            id.clone(),
            NodeId::parse(state.myself.name.as_bytes()).unwrap(),
            NodeId::parse(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap(),
            SlotRanges::new(vec![SlotRange::new(1, 1).unwrap()]).unwrap(),
            MigrationOperation::Export,
            1,
        );
        state.migrations.start(job).unwrap();
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));

        let token = Token(91);
        let mut connection = Connection::new_fake();
        connection.set_token_for_test(token);
        connection.slot_migration_role = Some(SlotMigrationConnectionRole::Export(id.clone()));
        let _connection_token = backend.connection_registry.insert(connection);
        backend.slot_migration_runtime.register_export(
            crate::framework::distributed::migration::SlotExportLink::new(id.clone(), token.0),
        );

        backend.remove_connection(token.0);

        assert_eq!(
            backend
                .ctx
                .cluster_state()
                .as_deref()
                .unwrap()
                .migrations
                .get(&id)
                .unwrap()
                .message
                .as_deref(),
            Some("Unable to connect to target node")
        );
    }

    #[test]
    fn rejected_accepted_transport_retries_on_later_background_ticks() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let token = listener.token;
        backend
            .transport
            .deregister_for_test(token)
            .expect("deregister test transport");

        backend.reject_accepted_client(RuntimeConnectedPeer { token, peer_addr }, 0);

        assert_eq!(
            backend
                .pending_transport_removals
                .get(&token)
                .map(|removal| removal.owner_kind),
            Some(PendingRemovalOwner::RejectedClient)
        );
        assert!(backend.transport.is_registered(token));
        assert_eq!(backend.connected_clients(), 0);

        backend.run_background_tasks();
        assert_eq!(
            backend
                .pending_transport_removals
                .get(&token)
                .map(|removal| removal.owner_kind),
            Some(PendingRemovalOwner::RejectedClient)
        );
        assert!(backend.transport.is_registered(token));

        backend
            .transport
            .reregister_for_test(token)
            .expect("reregister test transport");
        backend.run_background_tasks();

        assert!(!backend.pending_transport_removals.contains_key(&token));
        assert!(!backend.transport.is_registered(token));
    }

    #[test]
    fn removing_streaming_replica_refreshes_cached_replication_view() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let replica_id = backend.replication_runtime.register_primary(
            ConnectionId(77),
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: Vec::new(),
                rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
            },
        );
        backend
            .replication_runtime
            .primary_link_mut(replica_id)
            .unwrap()
            .set_state(PrimaryLinkState::Streaming);
        backend.refresh_replication_view();
        assert_eq!(backend.ctx.replication().view.live_replica_count, 1);

        backend.remove_connection(77);

        assert_eq!(backend.ctx.replication().view.live_replica_count, 0);
    }

    #[test]
    fn removing_rdb_channel_cleans_the_associated_relay_target() {
        use crate::runtime::server::worker::distributed::replication::diskless_child::DisklessRelay;

        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let main_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let rdb_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        backend.add_connection(RuntimeConnectedPeer {
            token: main_listener.token,
            peer_addr,
        });
        backend.add_connection(RuntimeConnectedPeer {
            token: rdb_listener.token,
            peer_addr,
        });

        let main = ConnectionId(main_listener.token.0);
        let rdb = ConnectionId(rdb_listener.token.0);
        let replica_id = backend.replication_runtime.register_primary(
            main,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: Vec::new(),
                rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
            },
        );
        backend
            .replication_runtime
            .register_rdb_channel(rdb, main)
            .expect("RDB channel should associate with main channel");
        backend.full_sync.diskless_relay = Some(DisklessRelay {
            transfer_id: crate::runtime::server::worker::reactor::sources::TransferId(0),
            source: None,
            child_release_write_end: None,
            targets: vec![replica_id],
            marker: Vec::new(),
            complete_full_resync: true,
            not_before_ms: 0,
            eof_reached: false,
            marker_queued_targets: std::collections::HashSet::new(),
            marker_sent_targets: std::collections::HashSet::new(),
            completion_announced: false,
            completion_state: crate::runtime::server::worker::distributed::replication::diskless_child::DisklessCompletionState::default(),
        });

        backend.remove_connection(rdb.0);

        assert!(
            backend.full_sync.diskless_relay.is_none(),
            "RDB-side teardown must remove the associated main ReplicaId from the relay"
        );
        assert_eq!(backend.replication_runtime.role(main), None);
        assert_eq!(backend.replication_runtime.role(rdb), None);
    }

    #[test]
    fn removing_waiting_dual_rdb_channel_preserves_main_psync_until_grace_deadline() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend
            .ctx
            .config_mut()
            .replication
            .repl_diskless_sync_delay = 5;
        let peer_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let main_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let rdb_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        backend.add_connection(RuntimeConnectedPeer {
            token: main_listener.token,
            peer_addr,
        });
        backend.add_connection(RuntimeConnectedPeer {
            token: rdb_listener.token,
            peer_addr,
        });

        let main = ConnectionId(main_listener.token.0);
        let rdb = ConnectionId(rdb_listener.token.0);
        let replica_id = backend.replication_runtime.register_primary(
            main,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: Vec::new(),
                rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
            },
        );
        backend
            .replication_runtime
            .primary_link_mut(replica_id)
            .unwrap()
            .set_state(PrimaryLinkState::Streaming);
        backend
            .replication_runtime
            .primary_link_mut(replica_id)
            .unwrap()
            .transition_to_waiting_for_rdb_channel()
            .unwrap();
        backend
            .replication_runtime
            .register_rdb_candidate(rdb)
            .unwrap();

        let endoff = backend
            .replication_runtime
            .take_endoff_for_rdb_candidate_if_unambiguous(
                rdb,
                true,
                &mut backend.ctx.replication_mut().backlog,
            )
            .expect("paired RDB channel must receive ENDOFF");
        assert!(!endoff.is_empty());
        backend
            .replication_runtime
            .backfill_waiting_psync_pins(backend.ctx.replication().backlog.next_stream_cursor());
        backend
            .replication_runtime
            .associate_rdb_channel(main, rdb)
            .unwrap();
        backend
            .replication_runtime
            .primary_link_mut(replica_id)
            .unwrap()
            .transition_to_waiting_for_main_psync()
            .unwrap();

        backend.remove_connection(rdb.0);

        assert_eq!(backend.replication_runtime.role(rdb), None);
        assert_eq!(
            backend.replication_runtime.role(main),
            Some(
                crate::framework::distributed::replication::ReplicationConnection::Primary(
                    replica_id,
                )
            )
        );
        assert_eq!(backend.replication_runtime.channel_pair_for(main), None);
        assert_eq!(
            backend.replication_runtime.psync_wait_pin_for_main(main),
            Some(crate::engine::distributed::replication::BacklogCursor::try_from(0).unwrap(),),
            "RDB-first teardown must retain the protected wait pin during grace"
        );

        let replid = backend.ctx.replication().backlog.replid().to_string();
        let offset = backend.ctx.replication().backlog.reported_offset().get();
        let backlog = backend.ctx.replication().backlog.clone();
        let link = backend
            .replication_runtime
            .primary_link_mut(replica_id)
            .expect("main link must survive RDB teardown");
        assert_eq!(
            link.dual_state(),
            PrimaryDualChannelState::WaitingForMainPsync
        );
        let requested = PsyncRequestOffset::try_from_raw(offset.checked_add(1).unwrap()).unwrap();
        assert!(link.complete_dual_channel_main_psync(&replid, requested, &backlog));
    }

    #[test]
    fn closing_main_after_rdb_first_detach_sets_grace_deadline() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        backend
            .ctx
            .config_mut()
            .server
            .debug_delay_rdb_client_free_seconds = 3;
        let peer_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let main_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let rdb_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        backend.add_connection(RuntimeConnectedPeer {
            token: main_listener.token,
            peer_addr,
        });
        backend.add_connection(RuntimeConnectedPeer {
            token: rdb_listener.token,
            peer_addr,
        });
        let main = ConnectionId(main_listener.token.0);
        let rdb = ConnectionId(rdb_listener.token.0);
        let id = backend.replication_runtime.register_primary(
            main,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: vec!["dual-channel".to_string()],
                rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
            },
        );
        backend
            .replication_runtime
            .primary_link_mut(id)
            .unwrap()
            .transition_to_waiting_for_rdb_channel()
            .unwrap();
        backend
            .replication_runtime
            .register_rdb_channel(rdb, main)
            .unwrap();
        backend
            .replication_runtime
            .primary_link_mut(id)
            .unwrap()
            .transition_to_waiting_for_main_psync()
            .unwrap();
        backend.remove_connection(rdb.0);
        backend.remove_connection(main.0);
        assert_eq!(
            backend
                .replication_runtime
                .rebuild_view()
                .replicas_waiting_psync,
            1
        );
        assert!(backend
            .replication_runtime
            .take_expired_grace_psync_waits(0)
            .is_empty());
        assert_eq!(
            backend
                .replication_runtime
                .take_expired_grace_psync_waits(i64::MAX),
            vec![main]
        );
    }

    #[test]
    fn removing_online_dual_rdb_channel_removes_the_main_replica_too() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let main_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let rdb_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        backend.add_connection(RuntimeConnectedPeer {
            token: main_listener.token,
            peer_addr,
        });
        backend.add_connection(RuntimeConnectedPeer {
            token: rdb_listener.token,
            peer_addr,
        });

        let main = ConnectionId(main_listener.token.0);
        let rdb = ConnectionId(rdb_listener.token.0);
        let replica_id = backend.replication_runtime.register_primary(
            main,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: vec!["dual-channel".to_string()],
                rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
            },
        );
        backend
            .replication_runtime
            .primary_link_mut(replica_id)
            .expect("main link must exist")
            .transition_to_waiting_for_rdb_channel()
            .unwrap();
        backend
            .replication_runtime
            .register_rdb_channel(rdb, main)
            .unwrap();
        backend
            .replication_runtime
            .primary_link_mut(replica_id)
            .expect("main link must exist")
            .transition_to_waiting_for_main_psync()
            .unwrap();
        backend
            .replication_runtime
            .primary_link_mut(replica_id)
            .expect("main link must exist")
            .transition_dual_state(PrimaryDualChannelState::WaitingForReplicaAck)
            .unwrap();
        backend
            .replication_runtime
            .primary_link_mut(replica_id)
            .expect("main link must exist")
            .transition_dual_state(PrimaryDualChannelState::Online)
            .unwrap();

        backend.remove_connection(rdb.0);

        assert!(matches!(
            backend.replication_runtime.role(main),
            Some(crate::framework::distributed::replication::ReplicationConnection::Primary(_))
        ));
        assert_eq!(backend.ctx.replication().view.connected_replica_count, 1);
    }

    #[test]
    fn removing_one_dual_rdb_channel_before_marker_keeps_remaining_cohort() {
        use crate::runtime::server::worker::distributed::replication::diskless_child::DirectDualRdbChild;
        use std::fs::File;

        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let main_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let rdb_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        backend.add_connection(RuntimeConnectedPeer {
            token: main_listener.token,
            peer_addr,
        });
        backend.add_connection(RuntimeConnectedPeer {
            token: rdb_listener.token,
            peer_addr,
        });

        let main = ConnectionId(main_listener.token.0);
        let rdb = ConnectionId(rdb_listener.token.0);
        let replica_id = backend.replication_runtime.register_primary(
            main,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: Vec::new(),
                rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
            },
        );
        backend
            .replication_runtime
            .register_rdb_channel(rdb, main)
            .unwrap();
        backend
            .replication_runtime
            .primary_link_mut(replica_id)
            .unwrap()
            .transition_to_waiting_for_rdb_channel()
            .unwrap();
        backend
            .replication_runtime
            .primary_link_mut(replica_id)
            .unwrap()
            .transition_to_waiting_for_main_psync()
            .unwrap();
        let child = backend.install_test_child(
            crate::runtime::server::worker::reactor::sources::ChildOwner::DirectDualRdb,
            0,
        );
        backend.full_sync.direct_dual_rdb_child = Some(DirectDualRdbChild {
            pid: 0,
            child,
            rdb_connections: vec![rdb],
            targets: vec![
                replica_id,
                crate::framework::distributed::replication::ReplicaId(999),
            ],
            shared_replication_buffer_bytes: 0,
            reaped_successfully: false,
            metric_read_end: File::open("/dev/null").unwrap(),
            phase_read_end: File::open("/dev/null").unwrap(),
            phase_source: None,
            phases: Vec::new(),
            completion_state: crate::runtime::server::worker::distributed::replication::diskless_child::DirectDualCompletionState::default(),
        });

        backend.remove_connection(rdb.0);

        assert_eq!(
            backend
                .full_sync
                .direct_dual_rdb_child
                .as_ref()
                .map(|child| child.targets.clone()),
            Some(vec![crate::framework::distributed::replication::ReplicaId(
                999
            )])
        );
        assert_eq!(backend.replication_runtime.role(rdb), None);
        assert!(matches!(
            backend.replication_runtime.role(main),
            Some(crate::framework::distributed::replication::ReplicationConnection::Primary(_))
        ));
    }

    #[test]
    fn removing_unrelated_replica_does_not_abort_active_direct_dual_child() {
        use crate::runtime::server::worker::distributed::replication::diskless_child::DirectDualRdbChild;
        use std::fs::File;

        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let online_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let syncing_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let rdb_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        for token in [
            online_listener.token,
            syncing_listener.token,
            rdb_listener.token,
        ] {
            backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
        }

        let online = ConnectionId(online_listener.token.0);
        let syncing = ConnectionId(syncing_listener.token.0);
        let rdb = ConnectionId(rdb_listener.token.0);
        let online_id = backend.replication_runtime.register_primary(
            online,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: Vec::new(),
                rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
            },
        );
        backend
            .replication_runtime
            .primary_link_mut(online_id)
            .unwrap()
            .set_state(PrimaryLinkState::Streaming);
        let syncing_id = backend.replication_runtime.register_primary(
            syncing,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6381,
                capability: Vec::new(),
                rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
            },
        );
        backend
            .replication_runtime
            .register_rdb_channel(rdb, syncing)
            .unwrap();
        backend
            .replication_runtime
            .primary_link_mut(syncing_id)
            .unwrap()
            .transition_to_waiting_for_rdb_channel()
            .unwrap();
        backend
            .replication_runtime
            .primary_link_mut(syncing_id)
            .unwrap()
            .transition_to_waiting_for_main_psync()
            .unwrap();
        let child = backend.install_test_child(
            crate::runtime::server::worker::reactor::sources::ChildOwner::DirectDualRdb,
            0,
        );
        backend.full_sync.direct_dual_rdb_child = Some(DirectDualRdbChild {
            pid: 0,
            child,
            rdb_connections: vec![rdb],
            targets: vec![syncing_id],
            shared_replication_buffer_bytes: 0,
            reaped_successfully: false,
            metric_read_end: File::open("/dev/null").unwrap(),
            phase_read_end: File::open("/dev/null").unwrap(),
            phase_source: None,
            phases: Vec::new(),
            completion_state: crate::runtime::server::worker::distributed::replication::diskless_child::DirectDualCompletionState::default(),
        });

        backend.remove_connection(online.0);

        assert_eq!(backend.replication_runtime.role(online), None);
        assert!(matches!(
            backend.replication_runtime.role(syncing),
            Some(crate::framework::distributed::replication::ReplicationConnection::Primary(_))
        ));
        assert!(matches!(
            backend.replication_runtime.role(rdb),
            Some(
                crate::framework::distributed::replication::ReplicationConnection::RdbChannel { .. }
            )
        ));
        assert!(backend.full_sync.direct_dual_rdb_child.is_some());
    }

    #[test]
    fn removing_one_direct_dual_main_keeps_remaining_child_target() {
        use crate::runtime::server::worker::distributed::replication::diskless_child::DirectDualRdbChild;
        use std::fs::File;

        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let first_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let second_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        for token in [first_listener.token, second_listener.token] {
            backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
        }

        let first = ConnectionId(first_listener.token.0);
        let second = ConnectionId(second_listener.token.0);
        let first_id = backend.replication_runtime.register_primary(
            first,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: Vec::new(),
                rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
            },
        );
        let second_id = backend.replication_runtime.register_primary(
            second,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6381,
                capability: Vec::new(),
                rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
            },
        );
        let child = backend.install_test_child(
            crate::runtime::server::worker::reactor::sources::ChildOwner::DirectDualRdb,
            0,
        );
        backend.full_sync.direct_dual_rdb_child = Some(DirectDualRdbChild {
            pid: 0,
            child,
            rdb_connections: Vec::new(),
            targets: vec![first_id, second_id],
            shared_replication_buffer_bytes: 0,
            reaped_successfully: false,
            metric_read_end: File::open("/dev/null").unwrap(),
            phase_read_end: File::open("/dev/null").unwrap(),
            phase_source: None,
            phases: Vec::new(),
            completion_state: crate::runtime::server::worker::distributed::replication::diskless_child::DirectDualCompletionState::default(),
        });

        backend.remove_connection(first.0);

        assert_eq!(backend.replication_runtime.role(first), None);
        assert!(matches!(
            backend.replication_runtime.role(second),
            Some(crate::framework::distributed::replication::ReplicationConnection::Primary(_))
        ));
        assert_eq!(
            backend
                .full_sync
                .direct_dual_rdb_child
                .as_ref()
                .map(|child| child.targets.clone()),
            Some(vec![second_id])
        );
    }

    #[test]
    fn removing_dual_rdb_after_child_marker_keeps_target_for_reap() {
        use crate::framework::commands::server::save::DirectDualRdbChildPhase;
        use crate::runtime::server::worker::distributed::replication::diskless_child::DirectDualRdbChild;
        use std::io::Write;

        let phase_pipe = crate::foundation::io::sys::fd::pipe(
            crate::foundation::io::sys::fd::PipeFlags::NONBLOCK,
        )
        .expect("create phase pipe");
        let (phase_read_end, mut phase_write_end) = phase_pipe.into_parts();
        phase_write_end
            .write_all(&[DirectDualRdbChildPhase::MarkerSent.as_byte()])
            .unwrap();

        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let main_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let rdb_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        backend.add_connection(RuntimeConnectedPeer {
            token: main_listener.token,
            peer_addr,
        });
        backend.add_connection(RuntimeConnectedPeer {
            token: rdb_listener.token,
            peer_addr,
        });
        let main = ConnectionId(main_listener.token.0);
        let rdb = ConnectionId(rdb_listener.token.0);
        let replica_id = backend.replication_runtime.register_primary(
            main,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: Vec::new(),
                rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
            },
        );
        backend
            .replication_runtime
            .register_rdb_channel(rdb, main)
            .unwrap();
        backend
            .replication_runtime
            .primary_link_mut(replica_id)
            .unwrap()
            .transition_to_waiting_for_rdb_channel()
            .unwrap();
        backend
            .replication_runtime
            .primary_link_mut(replica_id)
            .unwrap()
            .transition_to_waiting_for_main_psync()
            .unwrap();
        let child = backend.install_test_child(
            crate::runtime::server::worker::reactor::sources::ChildOwner::DirectDualRdb,
            0,
        );
        backend.full_sync.direct_dual_rdb_child = Some(DirectDualRdbChild {
            pid: 0,
            child,
            rdb_connections: vec![rdb],
            targets: vec![replica_id],
            shared_replication_buffer_bytes: 0,
            reaped_successfully: false,
            metric_read_end: File::open("/dev/null").unwrap(),
            phase_read_end,
            phase_source: None,
            phases: Vec::new(),
            completion_state: crate::runtime::server::worker::distributed::replication::diskless_child::DirectDualCompletionState::default(),
        });

        backend.remove_connection(rdb.0);

        assert_eq!(
            backend
                .full_sync
                .direct_dual_rdb_child
                .as_ref()
                .map(|child| child.targets.clone()),
            Some(vec![replica_id]),
            "a child that already sent EOF must finish and promote the main link"
        );
    }

    #[test]
    fn completed_unpaired_direct_rdb_close_removes_transport_but_retains_wait_owner() {
        use crate::framework::commands::server::save::DirectDualRdbChildPhase;
        use crate::runtime::server::worker::distributed::replication::diskless_child::DirectDualRdbChild;
        use std::fs::File;

        let mut backend = ServerWorkerState::new(1).unwrap();
        backend
            .ctx
            .config_mut()
            .server
            .debug_delay_rdb_client_free_seconds = 5;
        let peer_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let main_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let rdb_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        for token in [main_listener.token, rdb_listener.token] {
            backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
        }
        let main = ConnectionId(main_listener.token.0);
        let rdb = ConnectionId(rdb_listener.token.0);
        let public_client_id = backend.connection_registry[rdb_listener.token].client_id();
        let replica_id = backend.replication_runtime.register_primary(
            main,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: vec!["dual-channel".to_string()],
                rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
            },
        );
        backend
            .replication_runtime
            .primary_link_mut(replica_id)
            .unwrap()
            .transition_to_waiting_for_rdb_channel()
            .unwrap();
        backend
            .replication_runtime
            .register_rdb_candidate(rdb)
            .unwrap();
        let wait_boundary = backend.ctx.replication().backlog.snapshot_boundary();
        assert!(backend
            .replication_runtime
            .take_endoff_for_rdb_candidate_if_unambiguous(
                rdb,
                true,
                &mut backend.ctx.replication_mut().backlog,
            )
            .is_some());
        backend
            .replication_runtime
            .backfill_waiting_psync_pins(backend.ctx.replication().backlog.next_stream_cursor());
        let child = backend.install_test_child(
            crate::runtime::server::worker::reactor::sources::ChildOwner::DirectDualRdb,
            0,
        );
        backend.full_sync.direct_dual_rdb_child = Some(DirectDualRdbChild {
            pid: 0,
            child,
            rdb_connections: vec![rdb],
            targets: Vec::new(),
            shared_replication_buffer_bytes: 0,
            reaped_successfully: false,
            metric_read_end: File::open("/dev/null").unwrap(),
            phase_read_end: File::open("/dev/null").unwrap(),
            phase_source: None,
            phases: vec![DirectDualRdbChildPhase::MarkerSent],
            completion_state: crate::runtime::server::worker::distributed::replication::diskless_child::DirectDualCompletionState::default(),
        });

        backend.remove_connection(rdb.0);

        assert!(!backend.connection_registry.contains(rdb_listener.token));
        assert_eq!(
            backend
                .connection_registry
                .token_for_client_id(public_client_id),
            None,
            "generic client-id lookup must not expose retained replication waits"
        );
        assert!(!backend.transport.is_registered(rdb_listener.token));
        assert_eq!(backend.replication_runtime.role(rdb), None);
        assert!(backend.ctx.client_info(rdb.0).is_none());
        assert_eq!(backend.replication_runtime.psync_wait_owner_count(), 1);
        assert_eq!(
            backend.replication_runtime.min_protected_backlog_cursor(),
            Some(wait_boundary.stream_cursor())
        );
        assert!(backend.full_sync.direct_dual_rdb_child.is_some());

        assert!(backend
            .replication_runtime
            .try_associate_retained_rdb_wait(
                main,
                public_client_id,
                crate::foundation::io::sys::time::mstime(),
            )
            .is_ok());
        let link = backend
            .replication_runtime
            .primary_for_connection(main)
            .unwrap();
        assert_eq!(
            link.dual_state(),
            crate::framework::distributed::replication::PrimaryDualChannelState::WaitingForMainPsync
        );
        assert_eq!(link.dual_endoff_boundary(), Some(wait_boundary));
        assert!(link.dual_rdb_complete());
    }

    #[test]
    fn pre_marker_unpaired_direct_rdb_close_does_not_retain_wait() {
        use crate::runtime::server::worker::distributed::replication::diskless_child::DirectDualRdbChild;
        use std::fs::File;

        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let main_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let rdb_listener = backend.transport.bind_tcp(peer_addr).unwrap();
        for token in [main_listener.token, rdb_listener.token] {
            backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
        }
        let main = ConnectionId(main_listener.token.0);
        let rdb = ConnectionId(rdb_listener.token.0);
        let public_client_id = backend.connection_registry[rdb_listener.token].client_id();
        let replica_id = backend.replication_runtime.register_primary(
            main,
            ReplicaConnectionInfo {
                ip: "127.0.0.1".to_string(),
                port: 6380,
                capability: vec!["dual-channel".to_string()],
                rdb_filter_mode: crate::framework::distributed::replication::RdbFilterMode::All,
            },
        );
        backend
            .replication_runtime
            .primary_link_mut(replica_id)
            .unwrap()
            .transition_to_waiting_for_rdb_channel()
            .unwrap();
        backend
            .replication_runtime
            .register_rdb_candidate(rdb)
            .unwrap();
        assert!(backend
            .replication_runtime
            .take_endoff_for_rdb_candidate_if_unambiguous(
                rdb,
                true,
                &mut backend.ctx.replication_mut().backlog,
            )
            .is_some());
        let child = backend.install_test_child(
            crate::runtime::server::worker::reactor::sources::ChildOwner::DirectDualRdb,
            0,
        );
        backend.full_sync.direct_dual_rdb_child = Some(DirectDualRdbChild {
            pid: 0,
            child,
            rdb_connections: vec![rdb],
            targets: Vec::new(),
            shared_replication_buffer_bytes: 0,
            reaped_successfully: false,
            metric_read_end: File::open("/dev/null").unwrap(),
            phase_read_end: File::open("/dev/null").unwrap(),
            phase_source: None,
            phases: Vec::new(),
            completion_state: crate::runtime::server::worker::distributed::replication::diskless_child::DirectDualCompletionState::default(),
        });

        backend.remove_connection(rdb.0);

        assert_eq!(backend.replication_runtime.psync_wait_owner_count(), 0);
        assert_eq!(
            backend.replication_runtime.min_protected_backlog_cursor(),
            None
        );
        assert!(backend
            .replication_runtime
            .try_associate_retained_rdb_wait(
                main,
                public_client_id,
                crate::foundation::io::sys::time::mstime(),
            )
            .is_err());
    }

    #[test]
    fn removing_master_during_rdb_load_lazyfrees_pending_replication_buffer() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let token = listener.token;
        backend.add_connection(RuntimeConnectedPeer { token, peer_addr });

        let mut link = ReplicaLink::new(
            ConnectionId(token.0),
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        );
        link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
            .unwrap();
        link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
            .unwrap();
        link.transition_rdb_channel_state(ReplicaRdbChannelState::WaitingForEndOffset)
            .unwrap();
        link.transition_rdb_channel_state(ReplicaRdbChannelState::ReceivingRdb)
            .unwrap();
        let session_id = link.session_id();
        backend.replication_runtime.set_replica_link(link);

        let mut pending = PendingReplicationBuffer::new(usize::MAX, offset(100), 0);
        pending
            .push(ReplicaEvent::Apply {
                apply: ReplicationApply::Command(CommandArgs::from_parts(vec![
                    Bytes::from_static(b"SET"),
                    Bytes::from_static(b"k"),
                    Bytes::from(vec![b'x'; 65 * 16 * 1024]),
                ])),
                end_offset: offset(101),
            })
            .unwrap();
        let pending_effort = pending.used_bytes().saturating_add(16 * 1024 - 1) / (16 * 1024);
        let _ = backend
            .replication_runtime
            .set_replica_pending_buffer(session_id, pending);

        backend.remove_connection(token.0);
        backend.remove_connection(token.0);

        assert!(
            backend
                .replication_runtime
                .replica_pending_buffer()
                .is_none(),
            "failed RDB loading must release the buffered replication commands"
        );
        assert_eq!(
            backend.ctx.stats().lazyfree_pending_objects,
            pending_effort as u64
        );
        assert_eq!(backend.ctx.stats().lazyfreed_objects, 0);

        backend.wait_for_replication_lazyfree_for_test();
        backend.drain_replication_lazyfree_completions_for_test();

        assert_eq!(backend.ctx.stats().lazyfree_pending_objects, 0);
        assert!(backend.ctx.stats().lazyfreed_objects > 0);
    }

    #[test]
    fn removing_master_during_handshake_keeps_reconnect_backoff() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let token = listener.token;
        backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
        backend
            .replication_runtime
            .set_replica_link(ReplicaLink::new(
                ConnectionId(token.0),
                MasterInfo {
                    host: "127.0.0.1".to_string(),
                    port: 6379,
                },
            ));
        let before = crate::foundation::io::sys::time::mstime();

        backend.remove_connection(token.0);

        assert!(
            !backend
                .replication_runtime
                .master_connect_allowed(before.saturating_add(999)),
            "a handshake failure must retain the reconnect backoff"
        );
        assert!(
            backend.replication_runtime.master_connect_allowed(
                crate::foundation::io::sys::time::mstime().saturating_add(1_000)
            ),
            "the handshake reconnect must become eligible after the backoff"
        );
    }

    #[test]
    fn removing_established_master_allows_immediate_reconnect() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let token = listener.token;
        backend.add_connection(RuntimeConnectedPeer { token, peer_addr });
        let mut link = ReplicaLink::new(
            ConnectionId(token.0),
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        );
        link.connected(None, None);
        link.on_bytes(b"+PONG\r\n+OK\r\n+OK\r\n", 6379)
            .expect("advance handshake to PSYNC");
        link.on_bytes(
            b"+CONTINUE 0123456789abcdef0123456789abcdef01234567\r\n",
            6379,
        )
        .expect("accept partial resynchronization");
        assert!(link.is_streaming());
        backend.replication_runtime.set_replica_link(link);

        backend.remove_connection(token.0);

        assert!(
            backend
                .replication_runtime
                .master_connect_allowed(crate::foundation::io::sys::time::mstime()),
            "an established master link must be eligible to reconnect immediately"
        );
    }

    #[test]
    fn stale_short_read_plan_is_cleared_after_rdb_commit() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let main = ConnectionId(95);
        let mut link = ReplicaLink::new(
            main,
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        );
        link.connected(None, None);
        link.on_bytes(b"+PONG\r\n+OK\r\n+OK\r\n", 6379)
            .expect("advance handshake to PSYNC");
        link.on_bytes(
            b"+CONTINUE 0123456789abcdef0123456789abcdef01234567\r\n",
            6379,
        )
        .expect("accept partial resynchronization");
        assert!(link.is_streaming());
        backend.replication_runtime.set_replica_link(link);
        backend.ctx.replication_mut().async_loading = false;
        backend.ctx.set_loading(false);

        assert!(!backend.current_master_link_short_read(true));

        let held_link = backend
            .replication_runtime
            .take_replica_link()
            .expect("replica link exists");
        assert!(backend.current_master_link_short_read(true));
        backend.replication_runtime.set_replica_link(held_link);

        backend.ctx.set_loading(true);

        assert!(backend.current_master_link_short_read(true));
    }

    #[test]
    fn removing_master_while_dual_rdb_loader_holds_link_resets_offsets() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let token = listener.token;
        backend.add_connection(RuntimeConnectedPeer { token, peer_addr });

        backend.ctx.replication_mut().slave_repl_offset = offset(123);
        backend.ctx.replication_mut().slave_read_repl_offset = offset(124);
        backend.ctx.replication_mut().async_loading = true;
        backend.ctx.set_loading(true);

        // The dual-channel RDB callback temporarily owns ReplicaLink while it
        // drives the decoder, so the runtime lookup below intentionally sees
        // no master link even though the full sync is still in progress.
        let link = ReplicaLink::new(
            ConnectionId(token.0),
            MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        );
        backend.replication_runtime.set_replica_link(link);
        let held_link = backend
            .replication_runtime
            .take_replica_link()
            .expect("dual RDB callback must own the main link");

        backend.remove_connection(token.0);

        assert_eq!(backend.ctx.replication().slave_repl_offset, offset(0));
        assert_eq!(backend.ctx.replication().slave_read_repl_offset, offset(0));
        assert!(!backend.ctx.is_loading());
        assert!(!backend.ctx.replication().async_loading);
        assert!(
            backend
                .replication_runtime
                .master_connect_allowed(crate::foundation::io::sys::time::mstime()),
            "an interrupted RDB transfer must be eligible to reconnect immediately"
        );
        drop(held_link);
    }
}
