use crate::engine::distributed::cluster::{
    action::{ClusterAction, ClusterActionBatch},
    event::ClusterEvent,
    reducer::reduce,
    ClusterLinkView, LinkBufferEvent, LinkIoEvent, NodeId,
};
use crate::foundation::io::sys::time::mstime;
use crate::foundation::io::transport::Interest;
use crate::framework::capabilities::io::Token;
use crate::runtime::server::worker::reactor::sources::SourceOwner;
use crate::runtime::server::worker::PendingRemovalOwner;
use crate::runtime::server::worker::ServerWorkerState;
use std::io::Write;
use std::net::SocketAddr;

#[derive(Default)]
pub(crate) struct ClusterConfigFile {
    pub path: Option<std::path::PathBuf>,
    pub last_best_effort_warning_ms: i64,
}

impl ServerWorkerState {
    /// Atomically replaces Cluster Bus listeners when `tls-cluster` changes.
    /// All new binds succeed before any old listener or peer link is retired,
    /// so CONFIG SET cannot leave a node with a partially switched bus.
    pub(crate) fn rebind_cluster_bus_transport(&mut self) -> std::io::Result<()> {
        let Some(state) = self.ctx.cluster_state().as_deref() else {
            return Ok(());
        };
        let config = self.ctx.config().server.clone();
        let cluster_port = if config.cluster_port == 0 {
            state.myself.endpoints.bus_port()
        } else {
            config.cluster_port
        };
        if config.tls_cluster {
            self.configure_tls_server_from_config()?;
            self.configure_tls_client_from_config()?;
        }
        let targets = crate::framework::config::bind_targets(&config.bind, cluster_port);
        let old_listeners = std::mem::take(&mut self.cluster_bus.listener_tokens);
        let old_fds: Vec<_> = old_listeners
            .iter()
            .filter_map(|token| self.listeners.inherited_fds.remove(&token.0))
            .collect();
        self.ctx.replace_listener_fds(&old_fds, &[]);
        for token in old_listeners {
            self.retire_runtime_source(token);
            self.deregister_or_defer(token, PendingRemovalOwner::ClusterBus);
        }

        let mut new_handles = Vec::with_capacity(targets.len());
        for target in targets {
            let address = target
                .addr
                .parse::<SocketAddr>()
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
            let bind_result = if config.tls_cluster {
                self.transport.bind_tls(address)
            } else {
                self.transport.bind_tcp(address)
            }
            .and_then(|handle| self.route_listener_handle(handle, SourceOwner::ClusterBus));
            match bind_result {
                Ok(handle) => new_handles.push(handle),
                Err(error)
                    if target.optional && crate::server::is_optional_bind_soft_error(&error) =>
                {
                    crate::server_log!(
                        warning,
                        "Could not create optional Cluster Bus listener {}: {}",
                        target.addr,
                        error
                    );
                }
                Err(error) => {
                    for handle in new_handles {
                        self.retire_runtime_source(handle.token);
                        self.deregister_or_defer(handle.token, PendingRemovalOwner::ClusterBus);
                    }
                    return Err(error);
                }
            }
        }

        self.cluster_bus.listener_tokens = new_handles.iter().map(|handle| handle.token).collect();
        let new_fds: Vec<_> = new_handles
            .iter()
            .map(|handle| handle.inherited_fd)
            .collect();
        self.ctx.replace_listener_fds(&[], &new_fds);
        for handle in &new_handles {
            self.listeners
                .inherited_fds
                .insert(handle.token.0, handle.inherited_fd);
        }
        let links: Vec<_> = self
            .cluster_bus
            .connections
            .keys()
            .copied()
            .map(Token)
            .collect();
        for token in links {
            self.remove_cluster_bus_connection(token);
        }
        Ok(())
    }

    pub(crate) fn pump_cluster_bus_reads(&mut self) {
        let tokens: Vec<Token> = self
            .cluster_bus
            .connections
            .keys()
            .copied()
            .map(Token)
            .collect();
        if tokens.is_empty() {
            return;
        }
        // All readable bus sockets belong to one beforeSleep-equivalent batch:
        // election results collected across connections settle only after every
        // packet-induced reparent has committed.
        self.begin_cluster_bus_receive_batch();
        for token in tokens {
            self.handle_cluster_bus_readable(token);
        }
        self.finish_cluster_bus_receive_batch();
    }

    /// Open an event-loop batch that drains several Cluster Bus readable
    /// sockets. C packet handlers only schedule
    /// `CLUSTER_TODO_HANDLE_FAILOVER`; the promotion decision is taken once,
    /// in beforeSleep. While a batch is active, receive-path election results
    /// remain queued until `finish_cluster_bus_receive_batch`.
    pub(crate) fn begin_cluster_bus_receive_batch(&mut self) {
        self.cluster_bus_receive_batch_depth =
            self.cluster_bus_receive_batch_depth.saturating_add(1);
    }

    pub(crate) fn finish_cluster_bus_receive_batch(&mut self) {
        if self.cluster_bus_receive_batch_depth == 0 {
            return;
        }
        self.cluster_bus_receive_batch_depth -= 1;
        if self.cluster_bus_receive_batch_depth == 0 {
            self.settle_cluster_bus_receive_batch();
        }
    }

    fn cluster_bus_receive_batch_active(&self) -> bool {
        self.cluster_bus_receive_batch_depth > 0
    }

    /// Settle receive-path failover work once for the whole event-loop batch.
    /// Ordering mirrors C: `clusterProcessPacket` performs a sub-replica
    /// reparent synchronously (and voids the old election via
    /// `clusterSetPrimary`), whereas beforeSleep evaluates queued election
    /// results only after every readable socket has been drained.
    fn settle_cluster_bus_receive_batch(&mut self) {
        let mut state_opt = self.ctx.cluster_state_mut().take();
        if let Some(state) = state_opt.as_deref_mut() {
            self.cluster_apply_pending_replication_switch(state);
            let now = mstime();
            let failover_actions = self.cluster_bus.drain_receive_failover_actions(state, now);
            self.execute_cluster_actions(state, failover_actions.actions, now);

            let node_timeout = self.ctx.config().server.cluster_node_timeout.max(1_000);
            let (local_failover_status, local_replication_offset) =
                self.local_replica_failover_inputs(now);
            state.observe_local_replication(
                local_replication_offset,
                local_failover_status.priority,
            );
            let failover_actions = self
                .cluster_bus
                .reevaluate_automatic_failover_after_receive(
                    state,
                    now,
                    node_timeout,
                    &local_failover_status,
                );
            self.execute_cluster_actions(state, failover_actions.actions, now);
            let actions = reduce(state, ClusterEvent::BroadcastTopology);
            self.execute_cluster_bus_actions(state, actions);
            self.refresh_cluster_link_views_for_state(state);
        }
        *self.ctx.cluster_state_mut() = state_opt;
        self.drain_cluster_bus_publishes();
        // Reevaluation may queue an auth request without a topology broadcast.
        // C installs a write handler whenever its Cluster send queue grows.
        self.flush_cluster_bus_writes();
    }

    pub(crate) fn flush_cluster_bus_writes(&mut self) {
        let tokens = self.cluster_bus.writable_tokens();
        for token_value in tokens {
            let token = Token(token_value);
            if self
                .transport
                .set_interest(token, Interest::ReadWritable)
                .is_ok()
            {
                if let Some(connection) = self.cluster_bus.connections.get_mut(&token_value) {
                    connection.events = crate::engine::distributed::cluster::LinkEvents::READ_WRITE;
                }
            }
            self.handle_cluster_bus_writable(token);
        }
    }

    pub(crate) fn flush_pending_topology_broadcast(&mut self) {
        let mut state = self.ctx.cluster_state_mut().take();
        if let Some(cluster_state) = state.as_deref_mut() {
            let actions = reduce(cluster_state, ClusterEvent::BroadcastTopology);
            self.execute_cluster_bus_actions(cluster_state, actions);
        }
        *self.ctx.cluster_state_mut() = state;
    }

    pub(in crate::runtime) fn execute_cluster_bus_actions(
        &mut self,
        state: &mut crate::engine::distributed::cluster::ClusterState,
        actions: ClusterActionBatch,
    ) {
        self.execute_cluster_bus_actions_at_offsets(
            state,
            actions,
            &std::collections::HashMap::new(),
        );
    }

    pub(in crate::runtime) fn execute_cluster_bus_actions_at_offsets(
        &mut self,
        state: &mut crate::engine::distributed::cluster::ClusterState,
        actions: ClusterActionBatch,
        offsets: &std::collections::HashMap<usize, usize>,
    ) {
        for action in actions.actions {
            if let ClusterAction::BroadcastTopology = action {
                let lengths: std::collections::HashMap<usize, usize> = self
                    .cluster_bus
                    .connections
                    .iter()
                    .map(|(token, connection)| (*token, connection.write_buf.len()))
                    .collect();
                self.cluster_bus.queue_topology_broadcast(state);
                for (token, offset) in offsets {
                    let Some(length) = lengths.get(token) else {
                        continue;
                    };
                    let Some(connection) = self.cluster_bus.connections.get_mut(token) else {
                        continue;
                    };
                    if connection.write_buf.len() <= *length || *offset > *length {
                        continue;
                    }
                    let broadcast = connection.write_buf.split_off(*length);
                    connection.write_buf.splice(*offset..*offset, broadcast);
                }
                for token_value in self.cluster_bus.writable_tokens() {
                    let token = Token(token_value);
                    if self
                        .transport
                        .set_interest(token, Interest::ReadWritable)
                        .is_ok()
                    {
                        if let Some(connection) = self.cluster_bus.connections.get_mut(&token_value)
                        {
                            connection.events =
                                crate::engine::distributed::cluster::LinkEvents::READ_WRITE;
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn remove_cluster_bus_connection(&mut self, token: Token) {
        self.teardown_cluster_bus_connection(token, true);
    }

    pub(crate) fn remove_cluster_bus_listener(&mut self, token: Token) {
        self.retire_runtime_source(token);
        self.cluster_bus
            .listener_tokens
            .retain(|candidate| *candidate != token);
        if let Some(fd) = self.listeners.inherited_fds.remove(&token.0) {
            self.ctx.replace_listener_fds(&[fd], &[]);
        }
        self.deregister_or_defer(token, PendingRemovalOwner::ClusterBus);
    }

    #[cfg(test)]
    pub(crate) fn remove_cluster_bus_connection_without_retry(&mut self, token: Token) {
        self.teardown_cluster_bus_connection(token, false);
    }

    /// Isolate a bus connection from every business index first, then complete
    /// or defer the L1 deregister. `schedule_reconnect` chooses whether an
    /// outgoing handshake link should be re-armed for reconnect on removal.
    fn teardown_cluster_bus_connection(&mut self, token: Token, schedule_reconnect: bool) {
        // Already isolated; only its retained L1 registration awaits retry.
        if self.pending_transport_removals.contains_key(&token) {
            return;
        }
        self.retire_runtime_source(token);
        let mut state = self.ctx.cluster_state_mut().take();
        self.isolate_cluster_bus_connection(state.as_deref_mut(), token, schedule_reconnect);
        *self.ctx.cluster_state_mut() = state;
        self.deregister_or_defer(token, PendingRemovalOwner::ClusterBus);
    }

    fn isolate_cluster_bus_connection(
        &mut self,
        state: Option<&mut crate::engine::distributed::cluster::ClusterState>,
        token: Token,
        schedule_reconnect: bool,
    ) {
        let removed_link = self
            .cluster_bus
            .connections
            .get(&token.0)
            .and_then(|connection| {
                let node_name = if connection.projected_node_name.is_empty() {
                    &connection.node_name
                } else {
                    &connection.projected_node_name
                };
                NodeId::parse(node_name.as_bytes())
                    .ok()
                    .map(|node_id| (node_id, connection.direction()))
            });
        match (schedule_reconnect, state) {
            (true, Some(cluster_state)) => {
                self.cluster_bus
                    .remove_and_schedule_retry(cluster_state, token.0);
                if let Some((node_id, direction)) = removed_link {
                    cluster_state.observe_link_close(node_id.as_str(), direction);
                }
                Self::update_cluster_link_memory(&self.cluster_bus, cluster_state);
            }
            (_, Some(cluster_state)) => {
                self.cluster_bus.remove(token.0);
                if let Some((node_id, direction)) = removed_link {
                    cluster_state.observe_link_close(node_id.as_str(), direction);
                }
                Self::update_cluster_link_memory(&self.cluster_bus, cluster_state);
            }
            (_, None) => self.cluster_bus.remove(token.0),
        }
    }

    pub(in crate::runtime) fn teardown_cluster_bus_connection_with_state(
        &mut self,
        state: &mut crate::engine::distributed::cluster::ClusterState,
        token: Token,
        schedule_reconnect: bool,
    ) {
        if self.pending_transport_removals.contains_key(&token) {
            return;
        }
        self.retire_runtime_source(token);
        self.isolate_cluster_bus_connection(Some(state), token, schedule_reconnect);
        self.deregister_or_defer(token, PendingRemovalOwner::ClusterBus);
    }

    pub(crate) fn refresh_cluster_link_views(&mut self) {
        let mut state = self.ctx.cluster_state_mut().take();
        if let Some(state) = state.as_deref_mut() {
            self.refresh_cluster_link_views_for_state(state);
        }
        *self.ctx.cluster_state_mut() = state;
    }

    pub(in crate::runtime) fn refresh_cluster_link_views_for_state(
        &mut self,
        state: &mut crate::engine::distributed::cluster::ClusterState,
    ) {
        let kernel_pending: std::collections::HashMap<usize, usize> = self
            .cluster_bus
            .connections
            .keys()
            .copied()
            .map(|token| {
                let pending = self
                    .transport
                    .raw_fd(Token(token))
                    .and_then(crate::foundation::io::sys::os::socket_pending_write_bytes_fd)
                    .unwrap_or(0);
                (token, pending)
            })
            .collect();
        let observations: Vec<_> = self
            .cluster_bus
            .connections
            .iter_mut()
            .filter_map(|(token, connection)| {
                let identity = if connection.node_name.is_empty() {
                    &connection.handshake_node_name
                } else {
                    &connection.node_name
                };
                let Ok(node_id) = NodeId::parse(identity.as_bytes()) else {
                    return None;
                };
                let direction = connection.direction();
                let previous_node_id = (!connection.projected_node_name.is_empty()).then(|| {
                    std::mem::replace(&mut connection.projected_node_name, node_id.to_string())
                });
                if previous_node_id.is_none() {
                    connection.projected_node_name = node_id.to_string();
                }
                Some((
                    previous_node_id,
                    ClusterLinkView::new(
                        node_id,
                        direction,
                        connection.created_at,
                        connection.events,
                    )
                    .with_endpoint(connection.addr.ip().to_string(), connection.addr.port())
                    .with_temporary_handshake(!connection.handshake_node_name.is_empty()),
                    LinkIoEvent {
                        read_at_ms: (connection.last_recv > 0).then_some(connection.last_recv),
                        wrote_at_ms: (connection.last_send > 0).then_some(connection.last_send),
                        total_bytes_received: connection.bytes_received,
                        total_bytes_sent: connection.bytes_sent,
                    },
                    LinkBufferEvent {
                        allocated: connection.write_buf.capacity(),
                        used: connection
                            .write_buf
                            .len()
                            .saturating_add(kernel_pending.get(token).copied().unwrap_or(0)),
                        events: connection.events,
                    },
                ))
            })
            .collect();
        for (previous_node_id, view, io, buffer) in observations {
            let node_id = view.node_id.to_string();
            let direction = view.direction;
            state.observe_link_identity(previous_node_id.as_deref(), view);
            state.observe_link_io(&node_id, direction, io);
            state.observe_link_buffer(&node_id, direction, buffer);
        }
        Self::update_cluster_link_memory(&self.cluster_bus, state);
    }

    fn update_cluster_link_memory(
        bus: &crate::engine::distributed::cluster::bus::ClusterBusManager,
        state: &mut crate::engine::distributed::cluster::ClusterState,
    ) {
        let memory = bus
            .connections
            .values()
            .map(|connection| {
                connection
                    .read_buf
                    .capacity()
                    .saturating_add(connection.write_buf.capacity())
            })
            .sum();
        state.set_cluster_link_memory(memory);
    }

    #[cfg(test)]
    pub(crate) fn enforce_cluster_bus_sendbuf_limit(&mut self, limit: u64) {
        let mut state = self.ctx.cluster_state_mut().take();
        if let Some(state) = state.as_deref_mut() {
            self.enforce_cluster_bus_sendbuf_limit_for_state(state, limit);
        }
        *self.ctx.cluster_state_mut() = state;
    }

    pub(in crate::runtime) fn enforce_cluster_bus_sendbuf_limit_for_state(
        &mut self,
        state: &mut crate::engine::distributed::cluster::ClusterState,
        limit: u64,
    ) {
        if limit == 0 {
            return;
        }
        let tokens: Vec<Token> = self
            .cluster_bus
            .connections
            .iter()
            .filter_map(|(token, connection)| {
                let kernel_pending = self
                    .transport
                    .raw_fd(Token(*token))
                    .and_then(crate::foundation::io::sys::os::socket_pending_write_bytes_fd)
                    .unwrap_or(0);
                let used = connection.write_buf.len().saturating_add(kernel_pending);
                let used = used.max(connection.bytes_sent_since_read);
                ((used as u64) > limit).then_some(Token(*token))
            })
            .collect();
        if tokens.is_empty() {
            return;
        }
        state.increment_link_buffer_limit_exceeded(tokens.len());
        for token in tokens {
            self.teardown_cluster_bus_connection_with_state(state, token, true);
        }
    }

    pub(crate) fn handle_cluster_bus_listener(&mut self, token: Token) {
        loop {
            match self.transport.accept(token) {
                Ok(Some(handle)) => {
                    if let Err(error) =
                        self.route_runtime_source(handle.token, SourceOwner::ClusterBus)
                    {
                        crate::server_log!(
                            warning,
                            "Failed to route accepted Cluster Bus connection token={}: {}",
                            handle.token.0,
                            error
                        );
                        continue;
                    }
                    self.cluster_bus
                        .register_incoming(handle.token, handle.peer_addr);
                    if let Err(error) = self
                        .transport
                        .set_interest(handle.token, Interest::ReadWritable)
                    {
                        crate::server_log!(
                            warning,
                            "Failed to arm accepted Cluster Bus connection token={}: {}",
                            handle.token.0,
                            error
                        );
                    }
                    if let Some(connection) = self.cluster_bus.connections.get_mut(&handle.token.0)
                    {
                        connection.events =
                            crate::engine::distributed::cluster::LinkEvents::READ_WRITE;
                    }
                    crate::server_log!(
                        notice,
                        "Cluster bus connection from {} (token={})",
                        handle.peer_addr,
                        handle.token.0
                    );
                }
                Ok(None) => break,
                Err(e) => {
                    crate::server_log!(warning, "Cluster bus accept error: {}", e);
                    break;
                }
            }
        }
    }

    fn local_replica_failover_inputs(
        &self,
        now: i64,
    ) -> (
        crate::engine::distributed::cluster::state::failover::LocalReplicaFailoverStatus,
        i64,
    ) {
        let status =
            crate::engine::distributed::cluster::state::failover::LocalReplicaFailoverStatus {
                loading: self.ctx.is_loading() || self.ctx.replication().async_loading,
                priority: self.ctx.config().replication.replica_priority,
                data_age_ms: self.ctx.replication().primary_failover_data_age_ms(now),
                repl_ping_period_ms: self
                    .ctx
                    .config()
                    .replication
                    .repl_ping_replica_period
                    .saturating_mul(1_000),
                replica_validity_factor: self
                    .ctx
                    .config()
                    .server
                    .cluster_replica_validity_factor
                    .max(0) as u64,
            };
        let replication_offset = if self.ctx.replication().is_replica() {
            if status.loading {
                0
            } else {
                self.ctx.replication().slave_repl_offset.get()
            }
        } else {
            self.ctx.replication().primary_repl_offset().get()
        };
        (status, replication_offset)
    }

    pub(crate) fn handle_cluster_bus_readable(&mut self, token: Token) {
        let tv = token.0;
        if !self.cluster_bus.is_bus_token(token) {
            return;
        }
        let now = mstime();
        let node_timeout = self.ctx.config().server.cluster_node_timeout.max(1_000);
        let manual_failover_timeout = self.ctx.config().server.cluster_manual_failover_timeout;
        let allow_replica_migration = self.ctx.config().server.cluster_allow_replica_migration;
        let mut state_opt = self.ctx.cluster_state_mut().take();
        let mut should_close = false;
        let mut became_replica = false;
        if let Some(ref mut state) = state_opt {
            state.set_failure_report_node_timeout_ms(node_timeout);
            state.set_manual_failover_timeout_ms(manual_failover_timeout);
            state.set_replica_migration_allowed(allow_replica_migration);
            let Some(mut stream) = self.transport.stream(token) else {
                self.teardown_cluster_bus_connection_with_state(state, token, true);
                *self.ctx.cluster_state_mut() = state_opt;
                return;
            };
            should_close = self.cluster_bus.readable(state, &mut stream, tv, now);
            // A reciprocal MEET retires its obsolete outgoing link inside
            // C packet processing. Complete receive-generated closures before
            // another socket can confirm or replace that peer's link.
            let actions = reduce(state, ClusterEvent::DrainLinkCloses);
            self.execute_cluster_link_actions(state, actions);
            self.cluster_process_pending_dirty_slots(state);
            let was_primary = state.myself.is_primary();
            // C performs a sub-replica reparent synchronously inside packet
            // processing (clusterSetPrimary), so commit the switch before any
            // election result queued by the packets just received.
            self.cluster_apply_pending_replication_switch(state);
            if !self.cluster_bus_receive_batch_active() {
                // Without an event-loop batch this readable turn is its own
                // beforeSleep boundary; direct lower-level callers (and their
                // unit tests) keep the immediate settlement semantics.
                let failover_actions = self.cluster_bus.drain_receive_failover_actions(state, now);
                self.execute_cluster_actions(state, failover_actions.actions, now);
                let reevaluation_now = mstime();
                let (local_failover_status, local_replication_offset) =
                    self.local_replica_failover_inputs(reevaluation_now);
                state.observe_local_replication(
                    local_replication_offset,
                    local_failover_status.priority,
                );
                let failover_actions = self
                    .cluster_bus
                    .reevaluate_automatic_failover_after_receive(
                        state,
                        reevaluation_now,
                        node_timeout,
                        &local_failover_status,
                    );
                self.execute_cluster_actions(state, failover_actions.actions, reevaluation_now);
            }
            became_replica = was_primary && state.myself.is_replica();
            let actions = reduce(state, ClusterEvent::BroadcastTopology);
            self.execute_cluster_bus_actions(state, actions);
            if let Some(connection) = self.cluster_bus.connections.get_mut(&tv) {
                connection.events = if connection.write_buf.is_empty() {
                    crate::engine::distributed::cluster::LinkEvents::READABLE
                } else {
                    crate::engine::distributed::cluster::LinkEvents::READ_WRITE
                };
            }
            for writable in self.cluster_bus.writable_tokens() {
                if let Ok(()) = self
                    .transport
                    .set_interest(Token(writable), Interest::ReadWritable)
                {
                    if let Some(connection) = self.cluster_bus.connections.get_mut(&writable) {
                        connection.events =
                            crate::engine::distributed::cluster::LinkEvents::READ_WRITE;
                    }
                }
            }
            self.refresh_cluster_link_views_for_state(state);
        }
        self.drain_cluster_bus_publishes();
        if should_close {
            if let Some(state) = state_opt.as_deref_mut() {
                self.teardown_cluster_bus_connection_with_state(state, token, true);
            } else {
                self.remove_cluster_bus_connection(token);
            }
        }
        // Align with C `replicationSetMaster`: redirect blocked writes after
        // the role transition is committed. READONLY
        // non-writing waits remain valid on the new replica.
        if became_replica {
            if let Some(state) = state_opt.as_deref() {
                self.check_blocked_cluster_redirects_after_demotion(state);
            }
        }
        *self.ctx.cluster_state_mut() = state_opt;
        if !should_close
            && self
                .cluster_bus
                .connections
                .get(&tv)
                .is_some_and(|connection| !connection.write_buf.is_empty())
        {
            // A PING/MEET receive path can queue a PONG after the poller has
            // already reported this socket writable. Flush once here so the
            // reply cannot wait indefinitely for another writable edge.
            self.handle_cluster_bus_writable(token);
        }
    }

    pub(crate) fn handle_cluster_bus_writable(&mut self, token: Token) {
        let tv = token.0;
        let mut state_opt = self.ctx.cluster_state_mut().take();
        let Some(mut stream) = self.transport.stream(token) else {
            if let Some(state) = state_opt.as_deref_mut() {
                self.teardown_cluster_bus_connection_with_state(state, token, true);
            } else {
                self.remove_cluster_bus_connection(token);
            }
            *self.ctx.cluster_state_mut() = state_opt;
            return;
        };
        let result = if let Some(ref mut state) = state_opt {
            let res = self.cluster_bus.writable(&mut stream, tv, state);
            self.refresh_cluster_link_views_for_state(state);
            res
        } else {
            let buf = self
                .cluster_bus
                .connections
                .get(&tv)
                .map(|c| c.write_buf.clone())
                .unwrap_or_default();
            stream.write(&buf).inspect(|&n| {
                if let Some(conn) = self.cluster_bus.connections.get_mut(&tv) {
                    conn.write_buf.drain(..n);
                }
            })
        };
        match result {
            Ok(_) => {
                let interest = self.cluster_bus.connections.get(&tv).map(|connection| {
                    if connection.write_buf.is_empty() {
                        Interest::Readable
                    } else {
                        Interest::ReadWritable
                    }
                });
                if let Some(interest) = interest {
                    if self.transport.set_interest(token, interest).is_ok() {
                        if let Some(connection) = self.cluster_bus.connections.get_mut(&tv) {
                            connection.events = match interest {
                                Interest::Readable => {
                                    crate::engine::distributed::cluster::LinkEvents::READABLE
                                }
                                Interest::Writable | Interest::ReadWritable => {
                                    crate::engine::distributed::cluster::LinkEvents::READ_WRITE
                                }
                            };
                        }
                    } else {
                        crate::server_log!(
                            warning,
                            "Failed to update Cluster Bus interest token={} interest={:?}",
                            token.0,
                            interest
                        );
                    }
                }
            }
            Err(e) => {
                crate::server_log!(warning, "Cluster bus write error: {}", e);
                if let Some(state) = state_opt.as_deref_mut() {
                    self.teardown_cluster_bus_connection_with_state(state, token, true);
                } else {
                    self.remove_cluster_bus_connection(token);
                }
            }
        }
        let sendbuf_limit = self.ctx.config().server.cluster_link_sendbuf_limit;
        if let Some(state) = state_opt.as_deref_mut() {
            self.refresh_cluster_link_views_for_state(state);
            self.enforce_cluster_bus_sendbuf_limit_for_state(state, sendbuf_limit);
        }
        *self.ctx.cluster_state_mut() = state_opt;
    }
}

#[cfg(test)]
mod tests {
    use super::ServerWorkerState;
    use crate::engine::distributed::cluster::event::ClusterEvent;
    use crate::engine::distributed::cluster::link::LinkEvents;
    use crate::engine::distributed::cluster::node::{
        ClusterNode, CLUSTER_NODE_MYSELF, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
    };
    use crate::engine::distributed::cluster::reducer::reduce;
    use crate::engine::distributed::cluster::{ClusterLinkDirection, ClusterState};
    use crate::engine::distributed::replication::ReplicationOffset;
    use crate::framework::capabilities::io::Token;
    use std::io::Write;
    use std::net::SocketAddr;

    fn offset(value: i64) -> ReplicationOffset {
        ReplicationOffset::try_from(value).expect("valid test offset")
    }

    #[test]
    fn receive_failover_inputs_retain_cached_offset_before_rdb_load() {
        use crate::framework::distributed::replication::{ConnectionId, MasterInfo, ReplicaLink};

        let mut backend = ServerWorkerState::new(1).unwrap();
        backend.ctx.replication_mut().switch_to_replica();
        backend.ctx.replication_mut().slave_repl_offset = offset(42);
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
            b"+PONG\r\n+OK\r\n+OK\r\n+FULLRESYNC 0123456789abcdef0123456789abcdef01234567 1563\r\n",
            6380,
        )
        .unwrap();
        backend.replication_runtime.set_replica_link(link);

        let (status, replication_offset) =
            backend.local_replica_failover_inputs(crate::foundation::io::sys::time::mstime());

        assert!(!status.loading);
        assert_eq!(replication_offset, 42);
    }

    #[test]
    fn failed_cluster_deregistration_retries_on_background_tick() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let listener = backend.transport.bind_tcp(peer_addr).unwrap();
        let token = listener.token;
        backend.cluster_bus.register_incoming(token, peer_addr);
        backend
            .transport
            .deregister_for_test(token)
            .expect("deregister test transport");

        backend.remove_cluster_bus_connection_without_retry(token);

        // Isolate-first teardown drops the connection from cluster_bus.connections
        // immediately; only the retry entry keeps the L1 registration reserved.
        assert!(!backend.cluster_bus.connections.contains_key(&token.0));
        assert!(backend.is_pending_transport_removal(token));
        assert!(backend.transport.is_registered(token));

        backend
            .transport
            .reregister_for_test(token)
            .expect("reregister test transport");
        backend.run_background_tasks();

        assert!(!backend.cluster_bus.connections.contains_key(&token.0));
        assert!(!backend.is_pending_transport_removal(token));
        assert!(!backend.transport.is_registered(token));
    }

    #[test]
    fn runtime_projects_two_directional_links_and_closes_only_one() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let mut node = ClusterNode::new(
            node_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16380,
            true,
        );
        node.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(node_id.to_string(), node);
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));
        for (token, is_outgoing) in [(Token(41), true), (Token(42), false)] {
            if is_outgoing {
                backend.cluster_bus.register_outgoing_for_node(
                    token,
                    "127.0.0.1:16380".parse().unwrap(),
                    false,
                    node_id.to_string(),
                );
            } else {
                backend
                    .cluster_bus
                    .register_incoming(token, "127.0.0.1:26380".parse().unwrap());
                backend
                    .cluster_bus
                    .connections
                    .get_mut(&token.0)
                    .unwrap()
                    .node_name = node_id.to_string();
            }
            let connection = backend.cluster_bus.connections.get_mut(&token.0).unwrap();
            connection.created_at = if is_outgoing { 100 } else { 200 };
            connection.last_recv = 300;
            connection.last_send = 400;
            connection.bytes_received = if is_outgoing { 11 } else { 13 };
            connection.bytes_sent = if is_outgoing { 17 } else { 19 };
            connection.events = LinkEvents::READ_WRITE;
            connection.write_buf.extend_from_slice(b"queued");
        }
        backend.refresh_cluster_link_views();

        let state = backend.ctx.cluster_state().as_ref().unwrap();
        let mut links: Vec<_> = state.link_views().collect();
        links.sort_unstable_by_key(|link| link.direction);
        assert_eq!(links.len(), 2);
        assert_eq!(links[0].direction, ClusterLinkDirection::To);
        assert_eq!(links[0].created_at_ms, 100);
        assert_eq!(links[0].bytes_received, 11);
        assert_eq!(links[0].bytes_sent, 17);
        assert_eq!(links[0].send_buffer_used, 6);
        assert!(links[0].send_buffer_allocated >= 6);
        assert_eq!(links[0].events.as_str(), "rw");
        assert_eq!(links[1].direction, ClusterLinkDirection::From);
        assert_eq!(links[1].created_at_ms, 200);

        backend.remove_cluster_bus_connection_without_retry(Token(41));

        let state = backend.ctx.cluster_state().as_ref().unwrap();
        let links: Vec<_> = state.link_views().collect();
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].direction, ClusterLinkDirection::From);
    }

    #[test]
    fn writable_cluster_bus_keeps_write_interest_when_a_partial_frame_remains() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let peer = backend.transport.connect_tcp(addr).unwrap();
        let (_server, _) = listener.accept().unwrap();
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let mut node = ClusterNode::new(
            node_id.to_string(),
            addr.ip().to_string(),
            addr.port(),
            addr.port(),
            true,
        );
        node.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(node_id.to_string(), node);
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));
        backend.cluster_bus.register_outgoing_for_node(
            peer.token,
            addr,
            false,
            node_id.to_string(),
        );
        let connection = backend
            .cluster_bus
            .connections
            .get_mut(&peer.token.0)
            .unwrap();
        connection.handshake_sent = true;
        connection
            .write_buf
            .extend_from_slice(b"queued cluster frame");

        backend.handle_cluster_bus_writable(peer.token);

        let connection = &backend.cluster_bus.connections[&peer.token.0];
        assert!(
            connection.write_buf.is_empty() || connection.events == LinkEvents::READ_WRITE,
            "a buffered cluster frame must remain registered for writable readiness"
        );
    }

    #[test]
    fn readable_cluster_bus_flushes_a_queued_reply_without_a_writable_event() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer = backend.transport.connect_tcp(addr).unwrap();
        let (mut server, _) = listener.accept().unwrap();
        server
            .set_read_timeout(Some(std::time::Duration::from_millis(100)))
            .unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut node = ClusterNode::new(
            node_id.to_string(),
            addr.ip().to_string(),
            addr.port(),
            addr.port(),
            true,
        );
        node.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(node_id.to_string(), node);
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));
        backend
            .route_runtime_source(
                peer.token,
                crate::runtime::server::worker::reactor::sources::SourceOwner::ClusterBus,
            )
            .unwrap();
        backend.cluster_bus.register_incoming(peer.token, addr);
        backend
            .cluster_bus
            .connections
            .get_mut(&peer.token.0)
            .unwrap()
            .write_buf
            .extend_from_slice(b"queued reply");

        backend.dispatch_transport_event(
            crate::runtime::server::worker::reactor::ReadyEvent {
                token: peer.token,
                readable: true,
                writable: false,
                error: false,
                read_hup: false,
            },
            false,
        );

        use std::io::Read;
        let mut received = [0u8; 32];
        let n = server.read(&mut received).unwrap();
        assert_eq!(&received[..n], b"queued reply");
        assert!(backend.cluster_bus.connections[&peer.token.0]
            .write_buf
            .is_empty());
    }

    #[test]
    fn cron_flushes_pending_cluster_bus_frames_without_waiting_for_a_poll_edge() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer = backend.transport.connect_tcp(addr).unwrap();
        let (mut server, _) = listener.accept().unwrap();
        server
            .set_read_timeout(Some(std::time::Duration::from_millis(100)))
            .unwrap();
        backend.cluster_bus.register_incoming(peer.token, addr);
        backend
            .cluster_bus
            .connections
            .get_mut(&peer.token.0)
            .unwrap()
            .write_buf
            .extend_from_slice(b"cron queued frame");

        backend.flush_cluster_bus_writes();

        use std::io::Read;
        let mut received = [0u8; 32];
        let n = server.read(&mut received).unwrap();
        assert_eq!(&received[..n], b"cron queued frame");
        assert!(backend.cluster_bus.connections[&peer.token.0]
            .write_buf
            .is_empty());
    }

    #[test]
    fn cron_read_pump_processes_a_peer_frame_without_a_readable_event() {
        use crate::engine::distributed::cluster::msg::{
            ClusterBusBody, ClusterBusMessage, ClusterMsgType,
        };

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer = backend.transport.connect_tcp(addr).unwrap();
        let (mut server, _) = listener.accept().unwrap();
        server
            .set_read_timeout(Some(std::time::Duration::from_millis(100)))
            .unwrap();
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let mut node = ClusterNode::new(
            node_id.to_string(),
            addr.ip().to_string(),
            addr.port(),
            addr.port(),
            true,
        );
        node.flags &= !CLUSTER_NODE_MYSELF;
        let packet = ClusterBusMessage::from_node(
            ClusterMsgType::Ping,
            &node,
            1,
            ClusterBusBody::Gossip(Vec::new()),
        )
        .encode()
        .unwrap();
        state.nodes.insert(node_id.to_string(), node);
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));
        backend.cluster_bus.register_incoming(peer.token, addr);
        use std::io::{Read, Write};
        server.write_all(&packet).unwrap();
        // Poll only establishes arrival; the cron pump still receives no dispatched event.
        backend.transport.wait_readable_for_test(peer.token);

        backend.pump_cluster_bus_reads();
        backend.flush_cluster_bus_writes();

        let mut response = vec![0; 4096];
        let n = server.read(&mut response).unwrap();
        let (response, _) = ClusterBusMessage::decode(&response[..n]).unwrap();
        assert_eq!(response.msg_type, ClusterMsgType::Pong);
    }

    #[test]
    fn reciprocal_meet_retires_the_old_outgoing_link_in_the_receive_turn() {
        use crate::engine::distributed::cluster::msg::{
            ClusterBusBody, ClusterBusMessage, ClusterMsgType,
        };

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut backend = ServerWorkerState::new(1).unwrap();
        let outgoing = backend.transport.connect_tcp(addr).unwrap();
        let (_outgoing_peer, _) = listener.accept().unwrap();
        let incoming = backend.transport.connect_tcp(addr).unwrap();
        let (mut incoming_peer, _) = listener.accept().unwrap();
        for token in [outgoing.token, incoming.token] {
            backend
                .route_runtime_source(
                    token,
                    crate::runtime::server::worker::reactor::sources::SourceOwner::ClusterBus,
                )
                .unwrap();
        }
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut node = ClusterNode::new(
            node_id.to_string(),
            addr.ip().to_string(),
            addr.port(),
            addr.port(),
            true,
        );
        node.flags &= !CLUSTER_NODE_MYSELF;
        node.created_at = crate::foundation::io::sys::time::mstime() - 5_000;
        node.asymmetric_meet_started_at = node.created_at;
        let packet = ClusterBusMessage::from_node(
            ClusterMsgType::Meet,
            &node,
            1,
            ClusterBusBody::Gossip(Vec::new()),
        )
        .encode()
        .unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        state.set_failure_report_node_timeout_ms(1_000);
        state.nodes.insert(node_id.to_string(), node);
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));
        backend.cluster_bus.register_outgoing_for_node(
            outgoing.token,
            addr,
            false,
            node_id.to_string(),
        );
        backend.cluster_bus.register_incoming(incoming.token, addr);
        incoming_peer.write_all(&packet).unwrap();
        backend.transport.wait_readable_for_test(incoming.token);

        backend.handle_cluster_bus_readable(incoming.token);

        assert!(
            !backend
                .cluster_bus
                .connections
                .contains_key(&outgoing.token.0),
            "reciprocal MEET must not leave its obsolete outbound link active until cron"
        );
        assert!(backend
            .cluster_bus
            .connections
            .contains_key(&incoming.token.0));
        assert!(backend
            .ctx
            .cluster_state()
            .as_ref()
            .unwrap()
            .pending_link_closes
            .is_empty());
    }

    #[test]
    fn repeated_meet_after_gossip_identification_keeps_the_inbound_reply_link() {
        use crate::engine::distributed::cluster::msg::{
            ClusterBusBody, ClusterBusMessage, ClusterMsgType,
        };
        use std::io::Read;

        fn read_reply(peer: &mut std::net::TcpStream) -> ClusterBusMessage {
            let mut bytes = Vec::new();
            let mut chunk = [0; 4096];
            loop {
                let count = peer.read(&mut chunk).unwrap();
                assert!(count > 0, "the MEET reply link must remain open");
                bytes.extend_from_slice(&chunk[..count]);
                if let Ok((message, _)) = ClusterBusMessage::decode(&bytes) {
                    return message;
                }
            }
        }

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut backend = ServerWorkerState::new(1).unwrap();
        let incoming = backend.transport.connect_tcp(addr).unwrap();
        let (mut peer, _) = listener.accept().unwrap();
        peer.set_read_timeout(Some(std::time::Duration::from_secs(1)))
            .unwrap();
        backend
            .route_runtime_source(
                incoming.token,
                crate::runtime::server::worker::reactor::sources::SourceOwner::ClusterBus,
            )
            .unwrap();
        backend.cluster_bus.register_incoming(incoming.token, addr);
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut node = ClusterNode::new(
            node_id.to_string(),
            addr.ip().to_string(),
            addr.port(),
            addr.port(),
            true,
        );
        node.flags &= !CLUSTER_NODE_MYSELF;
        let packet = ClusterBusMessage::from_node(
            ClusterMsgType::Meet,
            &node,
            1,
            ClusterBusBody::Gossip(Vec::new()),
        )
        .encode()
        .unwrap();
        *backend.ctx.cluster_state_mut() =
            Some(Box::new(ClusterState::new("127.0.0.1", 6379, 16_379)));
        peer.write_all(&packet).unwrap();
        backend.transport.wait_readable_for_test(incoming.token);
        backend.handle_cluster_bus_readable(incoming.token);
        assert_eq!(read_reply(&mut peer).msg_type, ClusterMsgType::Pong);
        let placeholder_id = backend.cluster_bus.connections[&incoming.token.0]
            .handshake_node_name
            .clone();
        assert!(!placeholder_id.is_empty());

        // Another peer's trusted gossip identifies the sender while this
        // original inbound link is still projected through its placeholder.
        backend
            .ctx
            .cluster_state_mut()
            .as_deref_mut()
            .unwrap()
            .nodes
            .insert(node_id.to_string(), node);
        peer.write_all(&packet).unwrap();
        backend.transport.wait_readable_for_test(incoming.token);

        backend.handle_cluster_bus_readable(incoming.token);

        assert!(
            backend
                .cluster_bus
                .connections
                .contains_key(&incoming.token.0),
            "resolving the placeholder must retain the current inbound reply link"
        );
        let connection = &backend.cluster_bus.connections[&incoming.token.0];
        assert_eq!(connection.node_name, node_id);
        assert!(connection.handshake_node_name.is_empty());
        assert!(!backend
            .ctx
            .cluster_state()
            .as_deref()
            .unwrap()
            .nodes
            .contains_key(&placeholder_id));
        assert_eq!(read_reply(&mut peer).msg_type, ClusterMsgType::Pong);
    }

    #[test]
    fn outbound_pong_retires_other_handshake_owned_links() {
        use crate::engine::distributed::cluster::msg::{
            ClusterBusBody, ClusterBusMessage, ClusterMsgType,
        };

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut backend = ServerWorkerState::new(1).unwrap();
        let outgoing = backend.transport.connect_tcp(addr).unwrap();
        let (mut outgoing_peer, _) = listener.accept().unwrap();
        let incoming = backend.transport.connect_tcp(addr).unwrap();
        let (_incoming_peer, _) = listener.accept().unwrap();
        for token in [outgoing.token, incoming.token] {
            backend
                .route_runtime_source(
                    token,
                    crate::runtime::server::worker::reactor::sources::SourceOwner::ClusterBus,
                )
                .unwrap();
        }
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut node = ClusterNode::new(
            node_id.to_string(),
            addr.ip().to_string(),
            addr.port(),
            addr.port(),
            true,
        );
        node.flags &= !CLUSTER_NODE_MYSELF;
        let packet = ClusterBusMessage::from_node(
            ClusterMsgType::Pong,
            &node,
            1,
            ClusterBusBody::Gossip(Vec::new()),
        )
        .encode()
        .unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let handshake_id = state.ensure_handshake_node(
            &addr.ip().to_string(),
            addr.port(),
            addr.port(),
            crate::foundation::io::sys::time::mstime(),
        );
        state.nodes.insert(node_id.to_string(), node);
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));
        backend.cluster_bus.register_outgoing_for_handshake(
            outgoing.token,
            addr,
            handshake_id.clone(),
        );
        backend.cluster_bus.register_incoming(incoming.token, addr);
        backend
            .cluster_bus
            .connections
            .get_mut(&incoming.token.0)
            .unwrap()
            .handshake_node_name = handshake_id.clone();
        backend.refresh_cluster_link_views();
        outgoing_peer.write_all(&packet).unwrap();
        backend.transport.wait_readable_for_test(outgoing.token);

        backend.handle_cluster_bus_readable(outgoing.token);

        assert!(
            !backend
                .cluster_bus
                .connections
                .contains_key(&incoming.token.0),
            "removing a handshake identity must retire its other socket owners"
        );
        let confirmed = &backend.cluster_bus.connections[&outgoing.token.0];
        assert_eq!(confirmed.node_name, node_id);
        assert!(confirmed.handshake_node_name.is_empty());
        let state = backend.ctx.cluster_state().as_deref().unwrap();
        assert!(!state.nodes.contains_key(&handshake_id));
        assert!(state.pending_link_closes.is_empty());
    }

    #[test]
    fn send_buffer_limit_closes_link_once_and_removes_its_projection() {
        let mut backend = ServerWorkerState::new(1).unwrap();
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let mut node = ClusterNode::new(
            node_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16380,
            true,
        );
        node.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(node_id.to_string(), node);
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));
        backend.cluster_bus.register_outgoing_for_node(
            Token(43),
            "127.0.0.1:16380".parse().unwrap(),
            false,
            node_id.to_string(),
        );
        backend
            .cluster_bus
            .connections
            .get_mut(&43)
            .unwrap()
            .write_buf = vec![0; 65];
        backend.refresh_cluster_link_views();

        backend.enforce_cluster_bus_sendbuf_limit(64);
        backend.enforce_cluster_bus_sendbuf_limit(64);

        assert!(!backend.cluster_bus.connections.contains_key(&43));
        let state = backend.ctx.cluster_state().as_ref().unwrap();
        assert_eq!(state.links_buffer_limit_exceeded(), 1);
        assert!(state.link_views().next().is_none());
    }

    #[test]
    fn command_completion_broadcasts_pending_topology_before_cron() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut backend = ServerWorkerState::new(1).unwrap();
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let mut peer = ClusterNode::new(
            node_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        peer.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(node_id.to_string(), peer);
        state.request_topology_broadcast();
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));
        let peer = backend.transport.connect_tcp(addr).unwrap();
        let _server = listener.accept().unwrap();
        backend.cluster_bus.register_outgoing_for_node(
            peer.token,
            addr,
            false,
            node_id.to_string(),
        );
        backend
            .cluster_bus
            .connections
            .get_mut(&peer.token.0)
            .unwrap()
            .handshake_sent = true;
        backend
            .cluster_bus
            .connections
            .get_mut(&peer.token.0)
            .unwrap()
            .events = LinkEvents::READABLE;

        backend.flush_pending_topology_broadcast();

        assert!(!backend.cluster_bus.connections[&peer.token.0]
            .write_buf
            .is_empty());
        assert_eq!(
            backend.cluster_bus.connections[&peer.token.0].events,
            LinkEvents::READ_WRITE
        );
        assert!(!backend
            .ctx
            .cluster_state_mut()
            .as_deref_mut()
            .unwrap()
            .take_topology_broadcast_request());
    }

    #[test]
    fn receive_batch_settlement_flushes_election_requests_without_topology_changes() {
        use crate::engine::distributed::cluster::msg::{
            ClusterBusBody, ClusterBusMessage, ClusterMsgType,
        };
        use std::io::Read;

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut backend = ServerWorkerState::new(1).unwrap();
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let mut voter = ClusterNode::new(
            node_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        voter.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(node_id.to_string(), voter);
        let request = ClusterBusMessage::from_node(
            ClusterMsgType::FailoverAuthRequest,
            &state.myself,
            10,
            ClusterBusBody::Empty,
        )
        .encode()
        .unwrap();
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));
        let peer = backend.transport.connect_tcp(addr).unwrap();
        let (mut server, _) = listener.accept().unwrap();
        server
            .set_read_timeout(Some(std::time::Duration::from_millis(100)))
            .unwrap();
        backend.cluster_bus.register_outgoing_for_node(
            peer.token,
            addr,
            false,
            node_id.to_string(),
        );
        let connection = backend
            .cluster_bus
            .connections
            .get_mut(&peer.token.0)
            .unwrap();
        connection.handshake_sent = true;
        connection.events = LinkEvents::READABLE;

        backend.begin_cluster_bus_receive_batch();
        // Receive-path reevaluation can enqueue requests after the last ready
        // event, without producing a topology mutation or writable readiness.
        backend
            .cluster_bus
            .connections
            .get_mut(&peer.token.0)
            .unwrap()
            .write_buf
            .extend_from_slice(&request);
        backend.finish_cluster_bus_receive_batch();

        let mut received = vec![0; request.len()];
        server.read_exact(&mut received).unwrap();
        assert_eq!(received, request);
    }

    #[test]
    fn cluster_action_batch_broadcasts_topology_through_bus_transport() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut backend = ServerWorkerState::new(1).unwrap();
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let mut peer = ClusterNode::new(
            node_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        peer.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(node_id.to_string(), peer);
        state.request_topology_broadcast();
        let peer = backend.transport.connect_tcp(addr).unwrap();
        let _server = listener.accept().unwrap();
        backend.cluster_bus.register_outgoing_for_node(
            peer.token,
            addr,
            false,
            node_id.to_string(),
        );
        let connection = backend
            .cluster_bus
            .connections
            .get_mut(&peer.token.0)
            .unwrap();
        connection.handshake_sent = true;
        connection.events = LinkEvents::READABLE;

        let actions = reduce(&mut state, ClusterEvent::BroadcastTopology);
        backend.execute_cluster_bus_actions(&mut state, actions);

        assert!(!backend.cluster_bus.connections[&peer.token.0]
            .write_buf
            .is_empty());
        assert_eq!(
            backend.cluster_bus.connections[&peer.token.0].events,
            LinkEvents::READ_WRITE
        );
    }

    #[test]
    fn cluster_action_batch_inserts_cron_broadcast_before_new_ping() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut backend = ServerWorkerState::new(1).unwrap();
        let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let mut peer = ClusterNode::new(
            node_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        peer.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(node_id.to_string(), peer);
        let peer = backend.transport.connect_tcp(addr).unwrap();
        let _server = listener.accept().unwrap();
        backend.cluster_bus.register_outgoing_for_node(
            peer.token,
            addr,
            false,
            node_id.to_string(),
        );
        let connection = backend
            .cluster_bus
            .connections
            .get_mut(&peer.token.0)
            .unwrap();
        connection.events = LinkEvents::READABLE;
        let offsets = std::collections::HashMap::from([(peer.token.0, 0)]);
        backend
            .cluster_bus
            .queue_initial_handshake(&mut state, peer.token.0);
        state.request_topology_broadcast();
        let actions = reduce(&mut state, ClusterEvent::BroadcastTopology);

        backend.execute_cluster_bus_actions_at_offsets(&mut state, actions, &offsets);

        let bytes = &backend.cluster_bus.connections[&peer.token.0].write_buf;
        let (first, consumed) =
            crate::engine::distributed::cluster::msg::ClusterBusMessage::decode(bytes).unwrap();
        let (second, _) =
            crate::engine::distributed::cluster::msg::ClusterBusMessage::decode(&bytes[consumed..])
                .unwrap();
        assert_eq!(
            first.msg_type,
            crate::engine::distributed::cluster::msg::ClusterMsgType::Pong
        );
        assert_eq!(
            second.msg_type,
            crate::engine::distributed::cluster::msg::ClusterMsgType::Ping
        );
    }

    #[test]
    fn quorum_ack_promotes_replica_in_the_same_readable_turn() {
        use crate::engine::distributed::cluster::msg::{
            ClusterBusBody, ClusterBusMessage, ClusterMsgType,
        };
        use crate::engine::distributed::cluster::node::CLUSTER_NODE_FAIL;
        use crate::engine::distributed::cluster::state::failover::{
            AutomaticFailoverFacts, FailedPrimaryFacts, ReplicaEligibility,
        };
        use crate::engine::distributed::cluster::NodeId;

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer = backend.transport.connect_tcp(addr).unwrap();
        let (mut writer, _) = listener.accept().unwrap();

        let failed_primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let voter_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let mut state = ClusterState::new("127.0.0.1", 6380, 16_380);
        state.current_epoch = 9;
        state.myself.flags &= !CLUSTER_NODE_PRIMARY;
        state.myself.flags |= CLUSTER_NODE_REPLICA;
        state.myself.replicaof = failed_primary_id.to_string();

        let mut failed_primary = ClusterNode::new(
            failed_primary_id.to_string(),
            "127.0.0.1".to_string(),
            6379,
            16_379,
            true,
        );
        failed_primary.flags &= !CLUSTER_NODE_MYSELF;
        failed_primary.flags |= CLUSTER_NODE_FAIL;
        failed_primary.slots[7] = true;
        failed_primary.numslots = 1;
        failed_primary.config_epoch = 7;
        let inherited_slots = failed_primary.slots.clone();
        state.insert_node(failed_primary);

        let mut voter = ClusterNode::new(
            voter_id.to_string(),
            "127.0.0.1".to_string(),
            6381,
            16_381,
            true,
        );
        voter.flags &= !CLUSTER_NODE_MYSELF;
        voter.slots[8] = true;
        voter.numslots = 1;
        voter.config_epoch = 8;
        state.insert_node(voter.clone());

        let local = ReplicaEligibility {
            node_id: NodeId::parse(state.myself.name.as_bytes()).unwrap(),
            no_failover: false,
            cluster_link_connected: true,
            cluster_view_stale: false,
            loading: false,
            priority: 100,
            replication_offset: 42,
            data_age_ms: 0,
            node_timeout_ms: 1_000,
            repl_ping_period_ms: 0,
            replica_validity_factor: 0,
        };
        let facts = AutomaticFailoverFacts {
            now_ms: 1_000,
            node_timeout_ms: 1_000,
            current_epoch: 9,
            voting_primaries: 1,
            failed_primary_rank: 0,
            all_replicas_primary_failed: true,
            local: Some(local.clone()),
            primary: Some(FailedPrimaryFacts {
                node_id: NodeId::parse(failed_primary_id.as_bytes()).unwrap(),
                shard_id: NodeId::parse(failed_primary_id.as_bytes()).unwrap(),
                slots: inherited_slots,
                is_failed: true,
            }),
            siblings: vec![local],
            manual: false,
        };
        assert!(state.automatic_failover_on_tick(&facts).is_empty());
        assert!(matches!(
            state.automatic_failover_on_tick(&facts).as_slice(),
            [crate::engine::distributed::cluster::action::ClusterAction::BroadcastFailoverAuthRequest {
                election_epoch: 10,
                ..
            }]
        ));

        let packet = ClusterBusMessage::from_node(
            ClusterMsgType::FailoverAuthAck,
            &voter,
            10,
            ClusterBusBody::Empty,
        )
        .encode()
        .unwrap();
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));
        backend.cluster_bus.register_incoming(peer.token, addr);
        writer.write_all(&packet).unwrap();
        backend.transport.wait_readable_for_test(peer.token);

        backend.handle_cluster_bus_readable(peer.token);

        let state = backend.ctx.cluster_state().as_ref().unwrap();
        assert!(
            state.myself.is_primary(),
            "quorum promotion must not wait for the next cluster cron"
        );
        assert_eq!(state.myself.config_epoch, 10);
        assert!(state.myself.slots[7]);
    }

    #[test]
    fn readable_bus_immediately_starts_a_newly_best_replica() {
        use crate::engine::distributed::cluster::msg::{
            ClusterBusBody, ClusterBusMessage, ClusterMsgType,
        };
        use crate::engine::distributed::cluster::node::{
            CLUSTER_NODE_FAIL, CLUSTER_NODE_MY_PRIMARY_FAIL,
        };
        use crate::engine::distributed::cluster::state::failover::{
            AutomaticFailoverFacts, AutomaticFailoverPhase, FailedPrimaryFacts, ReplicaEligibility,
        };
        use crate::engine::distributed::cluster::NodeId;
        use crate::foundation::io::sys::time::mstime;

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer = backend.transport.connect_tcp(addr).unwrap();
        let (mut writer, _) = listener.accept().unwrap();

        let failed_primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let reporting_sibling_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let other_sibling_id = "cccccccccccccccccccccccccccccccccccccccc";
        let voter_id = "dddddddddddddddddddddddddddddddddddddddd";
        let mut state = ClusterState::new("127.0.0.1", 6380, 16_380);
        state.current_epoch = 9;
        state.myself.flags &= !CLUSTER_NODE_PRIMARY;
        state.myself.flags |= CLUSTER_NODE_REPLICA | CLUSTER_NODE_MY_PRIMARY_FAIL;
        state.myself.replicaof = failed_primary_id.to_string();
        // Deliberately leave the Cluster projection stale. The readable path
        // must rank from the live replication offset just like cluster cron.
        state.myself.repl_offset = 0;

        let mut failed_primary = ClusterNode::new(
            failed_primary_id.to_string(),
            "127.0.0.1".to_string(),
            6379,
            16_379,
            true,
        );
        failed_primary.flags &= !CLUSTER_NODE_MYSELF;
        failed_primary.flags |= CLUSTER_NODE_FAIL;
        failed_primary.slots[7] = true;
        failed_primary.numslots = 1;
        let inherited_slots = failed_primary.slots.clone();
        state
            .nodes
            .insert(failed_primary_id.to_string(), failed_primary);

        let mut reporting_sibling = ClusterNode::new(
            reporting_sibling_id.to_string(),
            "127.0.0.1".to_string(),
            6381,
            16_381,
            false,
        );
        reporting_sibling.flags &= !(CLUSTER_NODE_MYSELF | CLUSTER_NODE_PRIMARY);
        reporting_sibling.flags |= CLUSTER_NODE_REPLICA;
        reporting_sibling.replicaof = failed_primary_id.to_string();
        reporting_sibling.repl_offset = 0;
        state
            .nodes
            .insert(reporting_sibling_id.to_string(), reporting_sibling.clone());

        let mut other_sibling = ClusterNode::new(
            other_sibling_id.to_string(),
            "127.0.0.1".to_string(),
            6382,
            16_382,
            false,
        );
        other_sibling.flags &= !(CLUSTER_NODE_MYSELF | CLUSTER_NODE_PRIMARY);
        other_sibling.flags |= CLUSTER_NODE_REPLICA | CLUSTER_NODE_MY_PRIMARY_FAIL;
        other_sibling.replicaof = failed_primary_id.to_string();
        other_sibling.repl_offset = 0;
        state
            .nodes
            .insert(other_sibling_id.to_string(), other_sibling);

        let mut voter = ClusterNode::new(
            voter_id.to_string(),
            "127.0.0.1".to_string(),
            6383,
            16_383,
            true,
        );
        voter.flags &= !CLUSTER_NODE_MYSELF;
        voter.slots[8] = true;
        voter.numslots = 1;
        state.nodes.insert(voter_id.to_string(), voter);

        let local = ReplicaEligibility {
            node_id: NodeId::parse(state.myself.name.as_bytes()).unwrap(),
            no_failover: false,
            cluster_link_connected: true,
            cluster_view_stale: false,
            loading: false,
            priority: 100,
            replication_offset: 11_679,
            data_age_ms: 0,
            node_timeout_ms: 15_000,
            repl_ping_period_ms: 0,
            replica_validity_factor: 0,
        };
        let started_at = mstime();
        let facts = AutomaticFailoverFacts {
            now_ms: started_at,
            node_timeout_ms: 15_000,
            current_epoch: 9,
            voting_primaries: 1,
            failed_primary_rank: 0,
            all_replicas_primary_failed: false,
            local: Some(local.clone()),
            primary: Some(FailedPrimaryFacts {
                node_id: NodeId::parse(failed_primary_id.as_bytes()).unwrap(),
                shard_id: NodeId::parse(failed_primary_id.as_bytes()).unwrap(),
                slots: inherited_slots,
                is_failed: true,
            }),
            siblings: vec![
                local,
                ReplicaEligibility {
                    node_id: NodeId::parse(reporting_sibling_id.as_bytes()).unwrap(),
                    no_failover: false,
                    cluster_link_connected: true,
                    cluster_view_stale: false,
                    loading: false,
                    priority: 100,
                    replication_offset: 0,
                    data_age_ms: 0,
                    node_timeout_ms: 15_000,
                    repl_ping_period_ms: 0,
                    replica_validity_factor: 0,
                },
            ],
            manual: false,
        };
        assert!(state.automatic_failover_on_tick(&facts).is_empty());
        assert_eq!(
            state.automatic_failover.phase(),
            AutomaticFailoverPhase::WaitingRankDelay
        );
        assert!(state.automatic_failover.deadline_ms() > mstime());

        reporting_sibling.flags |= CLUSTER_NODE_MY_PRIMARY_FAIL;
        let packet = ClusterBusMessage::from_node(
            ClusterMsgType::Pong,
            &reporting_sibling,
            9,
            ClusterBusBody::Gossip(Vec::new()),
        )
        .encode()
        .unwrap();
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));
        backend.ctx.replication_mut().switch_to_replica();
        backend.ctx.replication_mut().slave_repl_offset = offset(11_679);
        backend
            .ctx
            .config_mut()
            .server
            .cluster_replica_validity_factor = 0;
        backend.cluster_bus.register_incoming(peer.token, addr);
        backend.cluster_bus.register_outgoing_for_node(
            Token(99),
            "127.0.0.1:16383".parse().unwrap(),
            false,
            voter_id.to_string(),
        );
        backend
            .cluster_bus
            .connections
            .get_mut(&99)
            .unwrap()
            .handshake_sent = true;
        writer.write_all(&packet).unwrap();
        backend.transport.wait_readable_for_test(peer.token);

        backend.handle_cluster_bus_readable(peer.token);

        let state = backend.ctx.cluster_state().as_ref().unwrap();
        assert_eq!(
            state.automatic_failover.phase(),
            AutomaticFailoverPhase::RequestingVotes,
            "a newly best rank-0 replica must not wait for the next full cluster cron"
        );
        assert_eq!(state.automatic_failover.election_epoch(), 10);
        assert_eq!(state.myself.repl_offset, 11_679);
        let queued = &backend.cluster_bus.connections[&99].write_buf;
        let (request, consumed) = ClusterBusMessage::decode(queued).unwrap();
        assert_eq!(request.msg_type, ClusterMsgType::FailoverAuthRequest);
        assert_eq!(request.current_epoch, 10);
        assert_eq!(consumed, queued.len(), "the request must be queued once");
    }

    #[test]
    fn readable_reparents_before_reevaluating_the_old_primary_election() {
        use crate::engine::distributed::cluster::msg::{
            ClusterBusBody, ClusterBusMessage, ClusterMsgType,
        };
        use crate::engine::distributed::cluster::node::{
            CLUSTER_NODE_FAIL, CLUSTER_NODE_MY_PRIMARY_FAIL,
        };
        use crate::engine::distributed::cluster::state::failover::{
            AutomaticFailoverFacts, AutomaticFailoverPhase, FailedPrimaryFacts, ReplicaEligibility,
        };
        use crate::engine::distributed::cluster::NodeId;
        use crate::foundation::io::sys::time::mstime;

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut backend = ServerWorkerState::new(1).unwrap();
        let peer = backend.transport.connect_tcp(addr).unwrap();
        let (mut writer, _) = listener.accept().unwrap();

        let old_primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let promoted_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let voter_id = "cccccccccccccccccccccccccccccccccccccccc";
        let shard_id = "dddddddddddddddddddddddddddddddddddddddd";
        let mut state = ClusterState::new("127.0.0.1", 6380, 16_380);
        state.current_epoch = 9;
        state.myself.flags &= !CLUSTER_NODE_PRIMARY;
        state.myself.flags |= CLUSTER_NODE_REPLICA | CLUSTER_NODE_MY_PRIMARY_FAIL;
        state.myself.replicaof = old_primary_id.to_string();
        state.myself.shard_id = shard_id.to_string();
        state.myself.repl_offset = 42;

        let mut old_primary = ClusterNode::new(
            old_primary_id.to_string(),
            "127.0.0.1".to_string(),
            6379,
            16_379,
            true,
        );
        old_primary.flags &= !CLUSTER_NODE_MYSELF;
        old_primary.flags |= CLUSTER_NODE_FAIL;
        old_primary.shard_id = shard_id.to_string();
        old_primary.config_epoch = 4;
        old_primary.slots[7] = true;
        old_primary.numslots = 1;
        let inherited_slots = old_primary.slots.clone();
        state.insert_node(old_primary);

        let mut promoted = ClusterNode::new(
            promoted_id.to_string(),
            "127.0.0.1".to_string(),
            6381,
            16_381,
            false,
        );
        promoted.flags &= !(CLUSTER_NODE_MYSELF | CLUSTER_NODE_PRIMARY);
        promoted.flags |= CLUSTER_NODE_REPLICA | CLUSTER_NODE_MY_PRIMARY_FAIL;
        promoted.replicaof = old_primary_id.to_string();
        promoted.shard_id = shard_id.to_string();
        promoted.repl_offset = 41;
        state
            .nodes
            .insert(promoted_id.to_string(), promoted.clone());

        let mut voter = ClusterNode::new(
            voter_id.to_string(),
            "127.0.0.1".to_string(),
            6382,
            16_382,
            true,
        );
        voter.flags &= !CLUSTER_NODE_MYSELF;
        voter.slots[8] = true;
        voter.numslots = 1;
        state.insert_node(voter);

        let local = ReplicaEligibility {
            node_id: NodeId::parse(state.myself.name.as_bytes()).unwrap(),
            no_failover: false,
            cluster_link_connected: true,
            cluster_view_stale: false,
            loading: false,
            priority: 100,
            replication_offset: 42,
            data_age_ms: 0,
            node_timeout_ms: 1_000,
            repl_ping_period_ms: 0,
            replica_validity_factor: 0,
        };
        let facts = AutomaticFailoverFacts {
            now_ms: mstime().saturating_sub(100),
            node_timeout_ms: 1_000,
            current_epoch: 9,
            voting_primaries: 1,
            failed_primary_rank: 0,
            all_replicas_primary_failed: true,
            local: Some(local.clone()),
            primary: Some(FailedPrimaryFacts {
                node_id: NodeId::parse(old_primary_id.as_bytes()).unwrap(),
                shard_id: NodeId::parse(shard_id.as_bytes()).unwrap(),
                slots: inherited_slots,
                is_failed: true,
            }),
            siblings: vec![local],
            manual: false,
        };
        assert!(state.automatic_failover_on_tick(&facts).is_empty());

        promoted.flags &= !CLUSTER_NODE_REPLICA;
        promoted.flags |= CLUSTER_NODE_PRIMARY;
        promoted.replicaof.clear();
        promoted.config_epoch = 10;
        promoted.slots[7] = true;
        promoted.numslots = 1;
        let packet = ClusterBusMessage::from_node(
            ClusterMsgType::Pong,
            &promoted,
            10,
            ClusterBusBody::Gossip(Vec::new()),
        )
        .encode()
        .unwrap();
        *backend.ctx.cluster_state_mut() = Some(Box::new(state));
        backend.ctx.replication_mut().switch_to_replica();
        backend.ctx.replication_mut().slave_repl_offset = offset(42);
        backend
            .ctx
            .config_mut()
            .server
            .cluster_replica_validity_factor = 0;
        backend.cluster_bus.register_incoming(peer.token, addr);
        backend.cluster_bus.register_outgoing_for_node(
            Token(99),
            "127.0.0.1:16382".parse().unwrap(),
            false,
            voter_id.to_string(),
        );
        backend
            .cluster_bus
            .connections
            .get_mut(&99)
            .unwrap()
            .handshake_sent = true;
        writer.write_all(&packet).unwrap();
        backend.transport.wait_readable_for_test(peer.token);

        backend.handle_cluster_bus_readable(peer.token);

        let state = backend.ctx.cluster_state().as_ref().unwrap();
        assert_eq!(state.myself.replicaof, promoted_id);
        assert_eq!(
            state.automatic_failover.phase(),
            AutomaticFailoverPhase::Idle
        );
        let mut queued = backend.cluster_bus.connections[&99].write_buf.as_slice();
        while !queued.is_empty() {
            let (message, consumed) = ClusterBusMessage::decode(queued).unwrap();
            assert_ne!(message.msg_type, ClusterMsgType::FailoverAuthRequest);
            queued = &queued[consumed..];
        }
    }

    #[test]
    fn cross_connection_ack_before_demotion_pong_settles_at_batch_boundary() {
        use crate::engine::distributed::cluster::msg::{
            ClusterBusBody, ClusterBusMessage, ClusterMsgType,
        };
        use crate::engine::distributed::cluster::node::{
            CLUSTER_NODE_FAIL, CLUSTER_NODE_MY_PRIMARY_FAIL, CLUSTER_NODE_PRIMARY,
            CLUSTER_NODE_REPLICA,
        };
        use crate::engine::distributed::cluster::state::failover::{
            AutomaticFailoverFacts, AutomaticFailoverPhase, FailedPrimaryFacts, ReplicaEligibility,
        };
        use crate::engine::distributed::cluster::NodeId;

        // Two bus sockets: the deciding FAILOVER_AUTH_ACK arrives from a voting
        // primary while the PONG announcing the demotion of the local replica's
        // old primary arrives on a different connection. C processes all
        // readable sockets first and settles the election once in beforeSleep,
        // so the committed reparent must void the stale promotion.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let listener_addr = listener.local_addr().unwrap();
        let mut backend = ServerWorkerState::new(1).unwrap();
        let voter_peer = backend.transport.connect_tcp(listener_addr).unwrap();
        let (mut voter_writer, _) = listener.accept().unwrap();
        let demotion_peer = backend.transport.connect_tcp(listener_addr).unwrap();
        let (mut demotion_writer, _) = listener.accept().unwrap();

        let old_primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let new_primary_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let old_shard_id = "dddddddddddddddddddddddddddddddddddddddd";
        let new_shard_id = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
        let mut state = ClusterState::new("127.0.0.1", 6380, 16_380);
        state.current_epoch = 9;
        state.myself.config_epoch = 9;
        state.myself.flags &= !CLUSTER_NODE_PRIMARY;
        state.myself.flags |= CLUSTER_NODE_REPLICA | CLUSTER_NODE_MY_PRIMARY_FAIL;
        state.myself.replicaof = old_primary_id.to_string();
        state.myself.shard_id = old_shard_id.to_string();
        state.myself.repl_offset = 42;

        let mut old_primary = ClusterNode::new(
            old_primary_id.to_string(),
            "127.0.0.1".to_string(),
            6379,
            16_379,
            true,
        );
        old_primary.flags &= !CLUSTER_NODE_MYSELF;
        old_primary.flags |= CLUSTER_NODE_FAIL;
        old_primary.shard_id = old_shard_id.to_string();
        old_primary.config_epoch = 4;
        old_primary.slots[7] = true;
        old_primary.numslots = 1;
        let inherited_slots = old_primary.slots.clone();
        state.insert_node(old_primary);

        let mut new_primary = ClusterNode::new(
            new_primary_id.to_string(),
            "127.0.0.1".to_string(),
            6381,
            16_381,
            true,
        );
        new_primary.flags &= !CLUSTER_NODE_MYSELF;
        new_primary.shard_id = new_shard_id.to_string();
        new_primary.slots[8] = true;
        new_primary.numslots = 1;
        new_primary.config_epoch = 8;
        state.insert_node(new_primary.clone());

        let local = ReplicaEligibility {
            node_id: NodeId::parse(state.myself.name.as_bytes()).unwrap(),
            no_failover: false,
            cluster_link_connected: true,
            cluster_view_stale: false,
            loading: false,
            priority: 100,
            replication_offset: 42,
            data_age_ms: 0,
            node_timeout_ms: 1_000,
            repl_ping_period_ms: 0,
            replica_validity_factor: 0,
        };
        let facts = AutomaticFailoverFacts {
            now_ms: 1_000,
            node_timeout_ms: 1_000,
            current_epoch: 9,
            voting_primaries: 1,
            failed_primary_rank: 0,
            all_replicas_primary_failed: true,
            local: Some(local.clone()),
            primary: Some(FailedPrimaryFacts {
                node_id: NodeId::parse(old_primary_id.as_bytes()).unwrap(),
                shard_id: NodeId::parse(old_shard_id.as_bytes()).unwrap(),
                slots: inherited_slots,
                is_failed: true,
            }),
            siblings: vec![local],
            manual: false,
        };
        assert!(state.automatic_failover_on_tick(&facts).is_empty());
        assert!(matches!(
            state.automatic_failover_on_tick(&facts).as_slice(),
            [crate::engine::distributed::cluster::action::ClusterAction::BroadcastFailoverAuthRequest {
                election_epoch: 10,
                ..
            }]
        ));

        let ack_packet = ClusterBusMessage::from_node(
            ClusterMsgType::FailoverAuthAck,
            &new_primary,
            10,
            ClusterBusBody::Empty,
        )
        .encode()
        .unwrap();

        let mut demoted_old_primary = state.nodes[old_primary_id].clone();
        demoted_old_primary.flags &= !(CLUSTER_NODE_PRIMARY | CLUSTER_NODE_FAIL);
        demoted_old_primary.flags |= CLUSTER_NODE_REPLICA;
        demoted_old_primary.replicaof = new_primary_id.to_string();
        demoted_old_primary.shard_id = new_shard_id.to_string();
        demoted_old_primary.slots.fill(false);
        demoted_old_primary.numslots = 0;
        demoted_old_primary.config_epoch = 10;
        let demotion_packet = ClusterBusMessage::from_node(
            ClusterMsgType::Pong,
            &demoted_old_primary,
            10,
            ClusterBusBody::Gossip(Vec::new()),
        )
        .encode()
        .unwrap();

        *backend.ctx.cluster_state_mut() = Some(Box::new(state));
        backend.ctx.replication_mut().switch_to_replica();
        backend.ctx.replication_mut().slave_repl_offset = offset(42);
        backend
            .ctx
            .config_mut()
            .server
            .cluster_replica_validity_factor = 0;
        backend.cluster_bus.register_outgoing_for_node(
            voter_peer.token,
            listener_addr,
            false,
            new_primary_id.to_string(),
        );
        backend.cluster_bus.register_outgoing_for_node(
            demotion_peer.token,
            listener_addr,
            false,
            old_primary_id.to_string(),
        );
        voter_writer.write_all(&ack_packet).unwrap();
        demotion_writer.write_all(&demotion_packet).unwrap();
        backend.transport.wait_readable_for_test(voter_peer.token);
        backend
            .transport
            .wait_readable_for_test(demotion_peer.token);

        // Drive the voter ACK connection first and the demotion PONG
        // connection second within the same event-loop receive batch.
        backend.begin_cluster_bus_receive_batch();
        backend.handle_cluster_bus_readable(voter_peer.token);
        backend.handle_cluster_bus_readable(demotion_peer.token);
        backend.finish_cluster_bus_receive_batch();

        let state = backend.ctx.cluster_state().as_ref().unwrap();
        assert!(
            state.myself.is_replica(),
            "the sub-replica must not promote itself on an election voided by the old primary's demotion"
        );
        assert_eq!(state.myself.replicaof, new_primary_id);
        assert!(!state.myself.slots[7]);
        assert_eq!(state.myself.numslots, 0);
        assert_eq!(
            state.myself.config_epoch, 9,
            "the stale election must not burn a promotion config epoch"
        );
        assert_eq!(
            state.automatic_failover.phase(),
            AutomaticFailoverPhase::Idle
        );
        assert!(state.pending_replication_switch.is_none());
        assert!(state.nodes[old_primary_id].is_replica());
        assert_eq!(state.nodes[old_primary_id].replicaof, new_primary_id);
    }

    #[test]
    fn readable_bus_slot_claim_commits_pending_replication_switch_immediately() {
        use crate::engine::distributed::cluster::msg::{
            ClusterBusBody, ClusterBusMessage, ClusterMsgType,
        };

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut backend = ServerWorkerState::new_with_slot_router(1, 14, |_| 0).unwrap();
        crate::framework::commands::register_all(&mut backend.registry);
        backend.ctx.config_mut().server.cluster_enabled = true;
        let peer = backend.transport.connect_tcp(addr).unwrap();
        let (mut writer, _) = listener.accept().unwrap();
        let observer_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        state.myself.config_epoch = 1;
        state
            .add_slots_batch(
                &(0..crate::engine::distributed::cluster::state::CLUSTER_SLOTS).collect::<Vec<_>>(),
            )
            .unwrap();
        let old_primary_id = state.myself.name.clone();
        let shard_id = state.myself.shard_id.clone();
        let new_primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut known_replica = ClusterNode::new(
            new_primary_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            false,
        );
        known_replica.flags &= !(CLUSTER_NODE_MYSELF | CLUSTER_NODE_PRIMARY);
        known_replica.flags |= CLUSTER_NODE_REPLICA;
        known_replica.replicaof = old_primary_id;
        known_replica.shard_id = shard_id.clone();
        state
            .nodes
            .insert(new_primary_id.to_string(), known_replica);
        let mut observer = ClusterNode::new(
            observer_id.to_string(),
            "127.0.0.1".to_string(),
            6381,
            16_381,
            true,
        );
        observer.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(observer_id.to_string(), observer);

        let mut promoted = ClusterNode::new(
            new_primary_id.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        promoted.flags &= !CLUSTER_NODE_MYSELF;
        promoted.shard_id = shard_id;
        promoted.config_epoch = 5;
        promoted.slots.fill(true);
        promoted.numslots = crate::engine::distributed::cluster::state::CLUSTER_SLOTS;
        let packet = ClusterBusMessage::from_node(
            ClusterMsgType::Pong,
            &promoted,
            5,
            ClusterBusBody::Gossip(Vec::new()),
        )
        .encode()
        .unwrap();

        *backend.ctx.cluster_state_mut() = Some(Box::new(state));
        let blocked_token = Token(91);
        let mut blocked = crate::framework::capabilities::connection::Connection::new_fake();
        blocked.set_token_for_test(blocked_token);
        blocked.set_custom_reply_enabled(false);
        blocked.begin_nested_command_effects();
        let blocked_args = crate::engine::command::CommandArgs::from_parts(vec![
            bytes::Bytes::from_static(b"BLPOP"),
            bytes::Bytes::from_static(b"slot-zero-key"),
            bytes::Bytes::from_static(b"0"),
        ]);
        blocked.enter_blocking(
            crate::engine::command::context::BlockedCmd::Blpop,
            vec![bytes::Bytes::from_static(b"slot-zero-key")],
            0,
            &blocked_args,
            0,
        );
        let blocked_slab = backend.connection_registry.insert(blocked);
        let blocked_session = backend.connection_registry[blocked_token].session_id();
        backend.ctx.blocking_registry_mut().register_client(
            0,
            &[bytes::Bytes::from_static(b"slot-zero-key")],
            blocked_session,
        );
        let readonly_token = Token(92);
        let mut readonly_blocked =
            crate::framework::capabilities::connection::Connection::new_fake();
        readonly_blocked.set_token_for_test(readonly_token);
        readonly_blocked.readonly = true;
        readonly_blocked.set_custom_reply_enabled(false);
        readonly_blocked.begin_nested_command_effects();
        let readonly_args = crate::engine::command::CommandArgs::from_parts(vec![
            bytes::Bytes::from_static(b"XREAD"),
            bytes::Bytes::from_static(b"BLOCK"),
            bytes::Bytes::from_static(b"0"),
            bytes::Bytes::from_static(b"STREAMS"),
            bytes::Bytes::from_static(b"slot-zero-key"),
            bytes::Bytes::from_static(b"0-0"),
        ]);
        readonly_blocked.enter_blocking(
            crate::engine::command::context::BlockedCmd::Xread,
            vec![bytes::Bytes::from_static(b"slot-zero-key")],
            0,
            &readonly_args,
            0,
        );
        let readonly_slab = backend.connection_registry.insert(readonly_blocked);
        let readonly_session = backend.connection_registry[readonly_token].session_id();
        backend.ctx.blocking_registry_mut().register_client(
            0,
            &[bytes::Bytes::from_static(b"slot-zero-key")],
            readonly_session,
        );
        backend.ctx.db_mut(0).unwrap().set(
            b"slot-zero-key".to_vec(),
            crate::foundation::io::store::Robj::string(b"value".as_slice()),
        );
        backend.cluster_bus.register_incoming(peer.token, addr);
        backend.cluster_bus.register_outgoing_for_node(
            Token(42),
            "127.0.0.1:16381".parse().unwrap(),
            false,
            observer_id.to_string(),
        );
        backend
            .cluster_bus
            .connections
            .get_mut(&42)
            .unwrap()
            .handshake_sent = true;
        writer.write_all(&packet).unwrap();
        backend.transport.wait_readable_for_test(peer.token);
        backend.handle_cluster_bus_readable(peer.token);

        let state = backend.ctx.cluster_state().as_ref().unwrap();
        assert!(state.myself.is_replica());
        assert_eq!(state.myself.replicaof, new_primary_id);
        assert!(state.pending_replication_switch.is_none());
        assert_eq!(
            state.state,
            crate::engine::distributed::cluster::state::CLUSTER_OK
        );
        assert!(
            (0..crate::engine::distributed::cluster::state::CLUSTER_SLOTS).all(|slot| {
                state.slot_owner_ref(slot).map(|owner| owner.name.as_str()) == Some(new_primary_id)
            })
        );
        assert!(backend.connection_registry
            [crate::framework::capabilities::io::Token(blocked_slab)]
        .blocked
        .is_none());
        assert!(backend.connection_registry
            [crate::framework::capabilities::io::Token(readonly_slab)]
        .blocked
        .is_some());
        assert_eq!(backend.ctx.blocking_registry().count(), 1);
        assert!(backend.connection_registry
            [crate::framework::capabilities::io::Token(readonly_slab)]
        .output
        .queue
        .is_empty());
        let blocked_reply = std::str::from_utf8(
            backend.connection_registry[crate::framework::capabilities::io::Token(blocked_slab)]
                .output
                .queue
                .front()
                .expect("role change must immediately reject the stale blocked command"),
        )
        .unwrap();
        assert!(
            blocked_reply.contains("MOVED ") && blocked_reply.contains("127.0.0.1:6380"),
            "unexpected blocked-route reply: {blocked_reply:?}"
        );
        assert!(backend
            .ctx
            .db(0)
            .unwrap()
            .get_allow_expired(b"slot-zero-key")
            .is_some());
        let queued = &backend.cluster_bus.connections[&42].write_buf;
        let (broadcast, _) = ClusterBusMessage::decode(queued).unwrap();
        assert_eq!(broadcast.msg_type, ClusterMsgType::Pong);
        assert_eq!(broadcast.replicaof, new_primary_id);
    }
}
