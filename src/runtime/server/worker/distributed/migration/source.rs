use super::super::super::ServerWorkerState;
use crate::engine::distributed::cluster::migration::{
    ExportPhase, MigrationAction, MigrationEvent, MigrationJobId, MigrationPhase,
    MigrationProtocolMessage, SlotMigrationConnectionRole,
};
use crate::engine::distributed::replication::protocol::encode_command;
use crate::foundation::io::transport::{Interest, ReadOutcome};
use crate::framework::capabilities::connection::Connection;
use crate::framework::context::PauseAction;
use crate::framework::distributed::migration::{SlotExportLink, SlotExportReply};
use crate::runtime::server::worker::reactor::sources::SourceOwner;
use std::net::{SocketAddr, ToSocketAddrs};

impl ServerWorkerState {
    pub(crate) fn reconcile_slot_export_output_allocation(&mut self, token_value: usize) {
        let current = self
            .connection_registry
            .get(crate::framework::capabilities::io::Token(token_value))
            .and_then(|connection| {
                let SlotMigrationConnectionRole::Export(id) =
                    connection.slot_migration_role.as_ref()?
                else {
                    return None;
                };
                Some((
                    id.clone(),
                    connection.output_buffer_allocation_bytes().saturating_add(
                        self.slot_migration_runtime.deferred_output_memory_usage(id),
                    ),
                    connection
                        .pending_reply_bytes()
                        .saturating_add(self.slot_migration_runtime.deferred_reply_bytes(id)),
                ))
            });
        let Some((id, current_allocation, current_reply_bytes)) = current else {
            self.remove_slot_export_output_allocation(token_value);
            return;
        };
        let previous = self
            .writes
            .slot_export_allocations
            .insert(
                crate::framework::capabilities::io::Token(token_value),
                current_allocation,
            )
            .unwrap_or_default();
        debug_assert!(previous <= self.writes.slot_export_allocation_bytes);
        self.writes.slot_export_allocation_bytes = self
            .writes
            .slot_export_allocation_bytes
            .saturating_sub(previous)
            .saturating_add(current_allocation);
        self.ctx
            .set_slot_export_output_memory(self.writes.slot_export_allocation_bytes);
        if let Some(state) = self.ctx.cluster_state_mut().as_deref_mut() {
            // C's CLUSTER GETSLOTMIGRATIONS reads job->client->reply_bytes
            // directly. Keep the Rust job projection synchronized at every
            // enqueue/write boundary so command replies never expose a stale
            // cron sample after the socket queue has drained.
            state.set_migration_remaining_repl_size(&id, current_reply_bytes);
        }
    }

    pub(crate) fn track_slot_export_connection_output(&mut self, connection_token: usize) {
        let Some(token_value) = self
            .connection_registry
            .get(crate::framework::capabilities::io::Token(connection_token))
            .map(|connection| connection.token().0)
        else {
            return;
        };
        self.reconcile_slot_export_output_allocation(token_value);
    }

    pub(crate) fn release_slot_export_connection_output(&mut self, token_value: usize) {
        let export_id = self
            .connection_registry
            .get(crate::framework::capabilities::io::Token(token_value))
            .and_then(
                |connection| match connection.slot_migration_role.as_ref()? {
                    SlotMigrationConnectionRole::Export(id) => Some(id.clone()),
                    SlotMigrationConnectionRole::Import(_) => None,
                },
            );
        self.reconcile_slot_export_output_allocation(token_value);
        self.remove_slot_export_output_allocation(token_value);
        if let (Some(id), Some(state)) = (export_id, self.ctx.cluster_state_mut().as_deref_mut()) {
            state.set_migration_remaining_repl_size(&id, 0);
        }
    }

    fn remove_slot_export_output_allocation(&mut self, token_value: usize) {
        let Some(allocation) = self
            .writes
            .slot_export_allocations
            .remove(&crate::framework::capabilities::io::Token(token_value))
        else {
            return;
        };
        debug_assert!(allocation <= self.writes.slot_export_allocation_bytes);
        self.writes.slot_export_allocation_bytes = self
            .writes
            .slot_export_allocation_bytes
            .saturating_sub(allocation);
        self.ctx
            .set_slot_export_output_memory(self.writes.slot_export_allocation_bytes);
    }

    fn queue_slot_export_reply(
        &mut self,
        connection_token: usize,
        frame: Vec<u8>,
        is_mutation: bool,
    ) -> bool {
        let token_value = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .token()
        .0;
        if is_mutation {
            self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
                .queue_replication_reply(frame);
        } else {
            self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
                .queue_reply(frame);
        }
        self.reconcile_slot_export_output_allocation(token_value);
        self.arm_slot_export_output(token_value)
    }

    pub(crate) fn arm_slot_export_output(&mut self, token_value: usize) -> bool {
        if self.enforce_client_output_buffer_limit_for(token_value) {
            return false;
        }
        let Some(connection_token) = self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token_value))
            .then_some(token_value)
        else {
            return false;
        };
        let token = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .token();
        if let Err(error) = self.transport.set_interest(token, Interest::ReadWritable) {
            crate::server_log!(
                warning,
                "failed to arm slot-export output: token={} error={}",
                token_value,
                error
            );
            self.remove_connection(token_value);
            return false;
        }
        let token = crate::framework::capabilities::io::Token(token_value);
        let key = crate::runtime::server::worker::reactor::RunnableKey::SlotExportWrite(token);
        if self.writes.slot_exports.was_serviced(token) {
            self.schedule_runtime_runnable_next_turn(key);
        } else {
            self.schedule_runtime_runnable(key);
        }
        true
    }

    pub(crate) fn queue_slot_export_mutation(
        &mut self,
        id: &MigrationJobId,
        db_id: u32,
        frame: Vec<u8>,
        defer: bool,
    ) -> Vec<Vec<u8>> {
        let frames = self
            .slot_migration_runtime
            .queue_export_mutation(id, db_id, frame, defer);
        if let Some(connection_id) = self.slot_migration_runtime.export_connection(id) {
            self.reconcile_slot_export_output_allocation(connection_id);
        }
        frames
    }

    pub(crate) fn take_slot_export_deferred_mutations(
        &mut self,
        id: &MigrationJobId,
    ) -> Vec<Vec<u8>> {
        let frames = self.slot_migration_runtime.take_deferred_mutations(id);
        if let Some(connection_id) = self.slot_migration_runtime.export_connection(id) {
            self.reconcile_slot_export_output_allocation(connection_id);
        }
        frames
    }

    pub(crate) fn queue_slot_migration_output(
        &mut self,
        id: &MigrationJobId,
        connection_id: usize,
        frame: Vec<u8>,
    ) -> bool {
        let Some(connection_token) = self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(connection_id))
            .then_some(connection_id)
        else {
            return false;
        };
        let owns_job = self.slot_migration_runtime.export_connection(id) == Some(connection_id);
        if !owns_job {
            return false;
        }
        if !self.queue_slot_export_reply(connection_token, frame, true) {
            return false;
        }
        true
    }

    pub(crate) fn queue_slot_snapshot_chunk(
        &mut self,
        id: &MigrationJobId,
        connection_id: usize,
        chunk: Vec<u8>,
    ) -> bool {
        let Some(connection_token) = self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(connection_id))
            .then_some(connection_id)
        else {
            return false;
        };
        if self.slot_migration_runtime.export_connection(id) != Some(connection_id) {
            return false;
        }
        self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .queue_slot_snapshot_reply(chunk);
        self.reconcile_slot_export_output_allocation(connection_id);
        self.transport
            .set_interest(
                self.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)]
                .token(),
                Interest::ReadWritable,
            )
            .is_ok()
    }

    pub(crate) fn slot_export_reply_bytes(
        &self,
        id: &MigrationJobId,
        connection_id: usize,
    ) -> usize {
        let ready = self
            .connection_registry
            .get(crate::framework::capabilities::io::Token(connection_id))
            .map_or(0, Connection::pending_reply_bytes);
        ready.saturating_add(self.slot_migration_runtime.deferred_reply_bytes(id))
    }

    pub(crate) fn slot_export_output_memory_usage(
        &self,
        id: &MigrationJobId,
        connection_id: usize,
    ) -> usize {
        let ready = self
            .connection_registry
            .get(crate::framework::capabilities::io::Token(connection_id))
            .map_or(0, Connection::pending_output_memory_usage);
        ready.saturating_add(self.slot_migration_runtime.deferred_output_memory_usage(id))
    }

    fn refresh_slot_migration_output_bytes(&mut self) {
        let samples = self
            .ctx
            .cluster_state()
            .as_deref()
            .map(|state| state.migrations.active_export_views())
            .unwrap_or_default()
            .into_iter()
            .filter_map(|job| {
                let connection_id = self.slot_migration_runtime.export_connection(&job.id)?;
                Some((
                    job.id.clone(),
                    self.slot_export_reply_bytes(&job.id, connection_id),
                ))
            })
            .collect::<Vec<_>>();
        if let Some(state) = self.ctx.cluster_state_mut().as_deref_mut() {
            for (id, bytes) in samples {
                state.set_migration_remaining_repl_size(&id, bytes);
            }
        }
    }

    fn pause_for_slot_migration(&mut self, now_ms: i64) {
        let timeout_ms = self
            .ctx
            .config()
            .server
            .cluster_manual_failover_timeout
            .max(1)
            .saturating_mul(2);
        self.ctx.client_pause_mut().action = PauseAction::Write;
        self.ctx.client_pause_mut().end_time_ms = now_ms.saturating_add(timeout_ms);
        self.ctx.client_pause_mut().reason = "slot_migration_in_progress";
    }

    fn unpause_slot_migration_if_idle(&mut self) {
        let has_paused_export = self.ctx.cluster_state().as_deref().is_some_and(|state| {
            state.migrations.active_export_views().iter().any(|job| {
                matches!(
                    job.phase,
                    MigrationPhase::Export(
                        ExportPhase::FailoverPaused | ExportPhase::FailoverGranted
                    )
                )
            })
        });
        if !has_paused_export && self.ctx.client_pause().reason == "slot_migration_in_progress" {
            self.ctx.client_pause_mut().action = PauseAction::None;
            self.ctx.client_pause_mut().end_time_ms = 0;
            self.ctx.client_pause_mut().reason = "none";
            self.ctx.set_pending_unpause(true);
        }
    }

    pub(crate) fn drive_slot_migration_actions(&mut self) {
        loop {
            let actions = self
                .ctx
                .cluster_state_mut()
                .as_deref_mut()
                .map(|state| state.take_migration_actions())
                .unwrap_or_default();
            if actions.is_empty() {
                break;
            }
            for (index, action) in actions.iter().cloned().enumerate() {
                if let MigrationAction::StartSnapshot(_) = &action {
                    if self.slot_migration_snapshot.is_some()
                        || self.ctx.persistence().has_active_child()
                    {
                        if let Some(state) = self.ctx.cluster_state_mut().as_deref_mut() {
                            state.queue_migration_actions(actions[index..].iter().cloned());
                        }
                        return;
                    }
                }
                self.execute_slot_migration_action(action);
            }
        }
    }

    fn execute_slot_migration_action(&mut self, action: MigrationAction) {
        match action {
            MigrationAction::Connect(id) => self.connect_slot_export(id),
            MigrationAction::CreateImport(id) => {
                let ranges = self
                    .ctx
                    .cluster_state()
                    .as_deref()
                    .and_then(|state| state.migrations.get(&id))
                    .map(|job| job.ranges.clone());
                if let Some(ranges) = ranges {
                    self.ctx.create_slot_import(id, &ranges);
                }
            }
            MigrationAction::SendAuth(id) => self.queue_slot_export_auth(&id),
            MigrationAction::SendEstablish(id) => self.queue_slot_export_establish(&id),
            MigrationAction::Send(id, message) => self.queue_slot_migration_message(&id, message),
            MigrationAction::StartSnapshot(id) => self.try_start_slot_snapshot(&id),
            MigrationAction::Cleanup(id) => self.cleanup_slot_migration(&id),
        }
    }

    fn connect_slot_export(&mut self, id: MigrationJobId) {
        let endpoint = self
            .ctx
            .cluster_state()
            .as_deref()
            .and_then(|state| state.migrations.get(&id))
            .and_then(|job| {
                self.ctx
                    .cluster_state()
                    .as_deref()?
                    .nodes
                    .get(job.target.as_str())
            })
            .and_then(|node| resolve_target(node.ip(), node.port()));
        let Some(endpoint) = endpoint else {
            self.fail_slot_migration(&id, "Unable to connect to target node");
            return;
        };
        let peer = match self.transport.connect_tcp(endpoint) {
            Ok(peer) => peer,
            Err(error) => {
                self.fail_slot_migration(
                    &id,
                    &format!("Unable to connect to target node: {error}"),
                );
                return;
            }
        };
        if let Err(error) = self.route_runtime_source(peer.token, SourceOwner::Connection) {
            self.fail_slot_migration(&id, &format!("Unable to route target connection: {error}"));
            return;
        }
        let mut connection = Connection::new(peer.token, endpoint, false);
        connection.slot_migration_role = Some(SlotMigrationConnectionRole::Export(id.clone()));
        self.connection_registry
            .register(connection)
            .expect("a newly connected migration transport token must be unique");
        let connection_token = peer.token.0;
        self.ctx.incr_connection();
        self.slot_migration_runtime
            .register_export(SlotExportLink::new(id, peer.token.0));
        self.track_slot_export_connection_output(connection_token);
        self.transport
            .set_interest(peer.token, Interest::ReadWritable)
            .ok();
    }

    pub(crate) fn ensure_slot_migration_test_tcp_buffers(
        &mut self,
        connection_token: usize,
    ) -> bool {
        let Some(connection) = self
            .connection_registry
            .get(crate::framework::capabilities::io::Token(connection_token))
        else {
            return false;
        };
        if connection.slot_migration_tcp_buffers_applied || connection.slot_migration_role.is_none()
        {
            return true;
        }
        let token = connection.token();
        let id = connection
            .slot_migration_role
            .as_ref()
            .expect("slot migration role was checked")
            .job_id()
            .clone();
        if let Err(error) = self.transport.apply_test_tcp_buffer_limits(token) {
            crate::server_log!(
                warning,
                "failed to configure slot-migration TCP buffers: token={} error={}",
                token.0,
                error
            );
            self.fail_slot_migration(
                &id,
                &format!("Unable to configure slot migration connection: {error}"),
            );
            self.remove_connection(token.0);
            return false;
        }
        if let Some(connection) = self
            .connection_registry
            .get_mut(crate::framework::capabilities::io::Token(connection_token))
        {
            connection.slot_migration_tcp_buffers_applied = true;
        }
        true
    }

    pub(crate) fn handle_slot_export_writable(&mut self, token_value: usize) -> bool {
        if !self
            .slot_migration_runtime
            .is_export_connection(token_value)
        {
            return false;
        }
        let Some(connection_token) = self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token_value))
            .then_some(token_value)
        else {
            return true;
        };
        let id = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .slot_migration_role
        .as_ref()
        .map(|role| role.job_id().clone())
        .expect("export connection has migration role");
        let phase = self.migration_phase(&id);
        if phase == Some(MigrationPhase::Export(ExportPhase::Connecting)) {
            let token = self.connection_registry
                [crate::framework::capabilities::io::Token(connection_token)]
            .token();
            match self.transport.take_connect_error(token) {
                Ok(None) => {}
                Ok(Some(error)) | Err(error) => {
                    self.fail_slot_migration(
                        &id,
                        &format!("Unable to connect to target node: {error}"),
                    );
                    self.remove_connection(token_value);
                    return true;
                }
            }
            // Match C socket.c: resolve SO_ERROR before configuring the connected
            // socket. macOS returns EINVAL for buffer options after a refused
            // connect, which must not replace the pending ECONNREFUSED diagnostic.
            if !self.ensure_slot_migration_test_tcp_buffers(connection_token) {
                return true;
            }
            let auth_required = self.ctx.config().security.primaryauth.is_some();
            self.transition_slot_migration(&id, MigrationEvent::Connected { auth_required });
        }
        false
    }

    pub(crate) fn handle_slot_export_connect_failure(&mut self, token_value: usize) -> bool {
        if !self
            .slot_migration_runtime
            .is_export_connection(token_value)
        {
            return false;
        }
        let Some(connection_token) = self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token_value))
            .then_some(token_value)
        else {
            return false;
        };
        let Some(SlotMigrationConnectionRole::Export(id)) = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .slot_migration_role
        .clone() else {
            return false;
        };
        if self.migration_phase(&id) != Some(MigrationPhase::Export(ExportPhase::Connecting)) {
            return false;
        }
        let token = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .token();
        let message = match self.transport.take_connect_error(token) {
            Ok(Some(error)) => format!("Unable to connect to target node: {error}"),
            Ok(None) => "Unable to connect to target node".to_string(),
            Err(error) => format!("Unable to connect to target node: {error}"),
        };
        self.fail_slot_migration(&id, &message);
        true
    }

    pub(crate) fn handle_slot_export_readable(&mut self, token_value: usize) -> bool {
        if !self
            .slot_migration_runtime
            .is_export_connection(token_value)
        {
            return false;
        }
        let Some(connection_token) = self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(token_value))
            .then_some(token_value)
        else {
            return true;
        };
        let id = self.connection_registry
            [crate::framework::capabilities::io::Token(connection_token)]
        .slot_migration_role
        .as_ref()
        .map(|role| role.job_id().clone())
        .expect("export connection has migration role");
        let phase = self.migration_phase(&id);
        if phase == Some(MigrationPhase::Export(ExportPhase::Connecting)) {
            // A refused nonblocking connect can arrive as readable + HUP.
            // Resolve SO_ERROR before the generic read path treats it as a
            // post-handshake connection loss.
            return self.handle_slot_export_connect_failure(token_value);
        }
        let expects_status = matches!(
            phase,
            Some(MigrationPhase::Export(
                ExportPhase::ReadingAuthResponse | ExportPhase::ReadingEstablishResponse
            ))
        );
        if !expects_status {
            return false;
        }
        let (bytes, outcome) = self.read_from_connection_bounded_outcome(connection_token);
        let replies = self
            .slot_migration_runtime
            .export_mut(token_value)
            .map(|link| link.on_status_bytes(&bytes))
            .unwrap_or_default();
        for reply in replies {
            match reply {
                SlotExportReply::StatusOk => {
                    let event = if phase
                        == Some(MigrationPhase::Export(ExportPhase::ReadingAuthResponse))
                    {
                        let username = self
                            .ctx
                            .config()
                            .security
                            .primaryuser
                            .as_ref()
                            .map(|user| String::from_utf8_lossy(user).into_owned())
                            .unwrap_or_else(|| "default".to_string());
                        self.connection_registry
                            [crate::framework::capabilities::io::Token(connection_token)]
                        .authenticated = true;
                        self.connection_registry
                            [crate::framework::capabilities::io::Token(connection_token)]
                        .ever_authenticated = true;
                        self.connection_registry
                            [crate::framework::capabilities::io::Token(connection_token)]
                        .username = username;
                        MigrationEvent::AuthAccepted
                    } else {
                        MigrationEvent::Established
                    };
                    self.transition_slot_migration(&id, event);
                }
                SlotExportReply::Error(error) => {
                    let message = if phase
                        == Some(MigrationPhase::Export(ExportPhase::ReadingAuthResponse))
                    {
                        format!("Failed to AUTH to target node: {error}")
                    } else {
                        format!("Received error during handshake to target: {error}")
                    };
                    self.fail_slot_migration(&id, &message);
                    self.remove_connection(token_value);
                }
            }
        }
        match outcome {
            ReadOutcome::PeerClosed => {
                if self
                    .connection_registry
                    .contains(crate::framework::capabilities::io::Token(connection_token))
                {
                    self.remove_connection(token_value);
                }
            }
            ReadOutcome::Failed(error) => {
                crate::server_log!(
                    verbose,
                    "slot migration read failed on token {}: {}",
                    token_value,
                    error
                );
                if self
                    .connection_registry
                    .contains(crate::framework::capabilities::io::Token(connection_token))
                {
                    self.remove_connection(token_value);
                }
            }
            ReadOutcome::Quiescent | ReadOutcome::BudgetExhausted => {}
        }
        true
    }

    fn queue_slot_export_auth(&mut self, id: &MigrationJobId) {
        let Some(connection_id) = self.slot_migration_runtime.export_connection(id) else {
            return;
        };
        let Some(connection_token) = self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(connection_id))
            .then_some(connection_id)
        else {
            return;
        };
        let Some(password) = self.ctx.config().security.primaryauth.clone() else {
            return;
        };
        let frame = if let Some(user) = self.ctx.config().security.primaryuser.as_deref() {
            encode_command(&[b"AUTH", user, &password])
        } else {
            encode_command(&[b"AUTH", &password])
        };
        self.connection_registry[crate::framework::capabilities::io::Token(connection_token)]
            .queue_pre_client_handshake_reply(frame);
        self.reconcile_slot_export_output_allocation(connection_id);
        self.transition_slot_migration(id, MigrationEvent::AuthCommandSent);
        self.transport
            .set_interest(
                self.connection_registry
                    [crate::framework::capabilities::io::Token(connection_token)]
                .token(),
                Interest::ReadWritable,
            )
            .ok();
    }

    fn queue_slot_export_establish(&mut self, id: &MigrationJobId) {
        let Some(connection_id) = self.slot_migration_runtime.export_connection(id) else {
            return;
        };
        let Some(connection_token) = self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(connection_id))
            .then_some(connection_id)
        else {
            return;
        };
        let Some(job) = self
            .ctx
            .cluster_state()
            .as_deref()
            .and_then(|state| state.migrations.get(id))
        else {
            return;
        };
        let mut owned = vec![
            b"CLUSTER".to_vec(),
            b"SYNCSLOTS".to_vec(),
            b"ESTABLISH".to_vec(),
            b"SOURCE".to_vec(),
            job.source.as_str().as_bytes().to_vec(),
            b"NAME".to_vec(),
            id.as_str().as_bytes().to_vec(),
            b"SLOTSRANGE".to_vec(),
        ];
        for range in job.ranges.iter() {
            owned.push(range.start.to_string().into_bytes());
            owned.push(range.end.to_string().into_bytes());
        }
        let refs: Vec<&[u8]> = owned.iter().map(Vec::as_slice).collect();
        if !self.queue_slot_export_reply(connection_token, encode_command(&refs), false) {
            return;
        }
        self.transition_slot_migration(id, MigrationEvent::EstablishCommandSent);
    }

    fn queue_slot_migration_message(
        &mut self,
        id: &MigrationJobId,
        message: MigrationProtocolMessage,
    ) {
        let connection_id = self
            .slot_migration_runtime
            .export_connection(id)
            .or_else(|| {
                self.connection_registry.iter().find_map(|(_, conn)| {
                    conn.slot_migration_role
                        .as_ref()
                        .and_then(|role| (role.job_id() == id).then_some(conn.token().0))
                })
            });
        let Some(connection_id) = connection_id else {
            return;
        };
        let Some(connection_token) = self
            .connection_registry
            .contains(crate::framework::capabilities::io::Token(connection_id))
            .then_some(connection_id)
        else {
            return;
        };
        let subcommand = match message {
            MigrationProtocolMessage::Ack => b"ACK".as_slice(),
            MigrationProtocolMessage::RequestPause => b"REQUEST-PAUSE".as_slice(),
            MigrationProtocolMessage::Paused => b"PAUSED".as_slice(),
            MigrationProtocolMessage::RequestFailover => b"REQUEST-FAILOVER".as_slice(),
            MigrationProtocolMessage::FailoverGranted => b"FAILOVER-GRANTED".as_slice(),
        };
        let _ = self.queue_slot_export_reply(
            connection_token,
            encode_command(&[b"CLUSTER", b"SYNCSLOTS", subcommand]),
            false,
        );
    }

    fn commit_slot_import(&mut self, id: &MigrationJobId) {
        let ranges = self
            .ctx
            .cluster_state()
            .as_deref()
            .and_then(|state| state.migrations.get(id))
            .map(|job| job.ranges.clone());
        let Some(ranges) = ranges else { return };
        let snapshot = match self.ctx.slot_import_snapshot(id, &ranges) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                self.fail_slot_migration(
                    id,
                    &format!("Failed to serialize imported data: {error}"),
                );
                return;
            }
        };
        if let Err(error) = self.ctx.commit_slot_import(id, &ranges) {
            self.fail_slot_migration(id, &format!("Failed to commit slot import: {error:?}"));
            return;
        }
        let mut state = self.ctx.cluster_state_mut().take();
        let committed = state.as_deref_mut().is_some_and(|cluster| {
            if cluster.bump_config_epoch_without_consensus().is_err() {
                return false;
            }
            let local_id = cluster.myself.name.clone();
            for range in ranges.iter() {
                for slot in range.start..=range.end {
                    if cluster.set_slot_node(slot as usize, &local_id).is_err() {
                        return false;
                    }
                }
            }
            cluster.finalize_slot_changes();
            let actions = cluster
                .transition_migration(
                    id,
                    MigrationEvent::OwnershipCommitted,
                    crate::foundation::io::sys::time::unixtime(),
                )
                .unwrap_or_default();
            cluster.queue_migration_actions(actions);
            true
        });
        *self.ctx.cluster_state_mut() = state;
        if committed {
            if !snapshot.is_empty() {
                self.propagate_to_replicas(&snapshot);
                if self.ctx.config().persistence.aof_enabled {
                    self.feed_aof(&snapshot);
                    self.flush_aof_if_needed();
                }
                self.ctx.set_repl_seldb(-1);
                self.ctx.persistence_mut().reset_aof_selected_db();
            }
            self.drive_slot_migration_actions();
        } else {
            self.fail_slot_migration(id, "Failed to commit slot ownership");
        }
    }

    fn cleanup_slot_migration(&mut self, id: &MigrationJobId) {
        let terminal_import = self
            .ctx
            .cluster_state()
            .as_deref()
            .and_then(|state| {
                let job = state.migrations.get(id)?;
                match job.phase {
                    MigrationPhase::Terminal(terminal)
                        if job.operation
                            == crate::engine::distributed::cluster::migration::MigrationOperation::Import
                            && state.myself.is_primary() =>
                    {
                        Some((terminal, job.message.clone()))
                    }
                    _ => None,
                }
            });
        if let Some((terminal, message)) = terminal_import {
            let mut fields = vec![
                b"CLUSTER".to_vec(),
                b"SYNCSLOTS".to_vec(),
                b"FINISH".to_vec(),
                b"STATE".to_vec(),
                match terminal {
                    crate::engine::distributed::cluster::migration::TerminalPhase::Success => {
                        b"SUCCESS".to_vec()
                    }
                    _ => b"FAILED".to_vec(),
                },
                b"NAME".to_vec(),
                id.as_str().as_bytes().to_vec(),
            ];
            if let Some(message) = message {
                fields.push(b"MESSAGE".to_vec());
                fields.push(message.into_bytes());
            }
            let refs = fields.iter().map(Vec::as_slice).collect::<Vec<_>>();
            let frame = encode_command(&refs);
            self.propagate_to_replicas(&frame);
            if self.ctx.config().persistence.aof_enabled {
                self.feed_aof(&frame);
                self.flush_aof_if_needed();
            }
        }
        self.abort_slot_snapshot(id);
        let mut connections = self
            .connection_registry
            .iter()
            .filter_map(|(_, conn)| {
                conn.slot_migration_role
                    .as_ref()
                    .is_some_and(|role| role.job_id() == id)
                    .then_some(conn.token().0)
            })
            .collect::<Vec<_>>();
        if let Some(connection_id) = self.slot_migration_runtime.remove_job(id) {
            self.reconcile_slot_export_output_allocation(connection_id);
            if !connections.contains(&connection_id) {
                connections.push(connection_id);
            }
        }
        self.ctx.discard_slot_import(id);
        for connection_id in connections {
            let is_replication_source = self.is_master_link(connection_id);
            if let Some(connection_token) = self
                .connection_registry
                .contains(crate::framework::capabilities::io::Token(connection_id))
                .then_some(connection_id)
            {
                if self
                    .connection_registry
                    .contains(crate::framework::capabilities::io::Token(connection_token))
                {
                    if is_replication_source {
                        self.release_slot_export_connection_output(connection_id);
                        self.connection_registry
                            [crate::framework::capabilities::io::Token(connection_token)]
                        .slot_migration_role = None;
                    } else {
                        // Keep the role until teardown so any already-buffered
                        // migration protocol frame is suppressed instead of
                        // being dispatched as a normal client command.
                        self.connection_registry
                            [crate::framework::capabilities::io::Token(connection_token)]
                        .mark_closing();
                    }
                }
            }
        }
        self.wake_slot_migration_clients(id);
        self.unpause_slot_migration_if_idle();
    }

    fn wake_slot_migration_clients(&mut self, id: &MigrationJobId) {
        let tokens = self.connection_registry
            .iter()
            .filter_map(|(_, conn)| {
                conn.blocked
                    .as_ref()
                    .is_some_and(|blocked| matches!(&blocked.cmd, crate::engine::command::context::BlockedCmd::SlotMigration(job) if job == id))
                    .then_some(conn.token())
            })
            .collect::<Vec<_>>();
        for token in tokens {
            self.ctx.paused_clients_mut().shift_remove(&token);
            self.wakeup_blocked_client(token, None, None);
        }
    }

    pub(crate) fn drive_slot_migration_progress(&mut self) {
        self.expire_stalled_slot_migrations();
        self.trim_slot_migration_log();
        self.drive_slot_migration_actions();
        self.refresh_slot_migration_output_bytes();
        let now_ms = crate::foundation::io::sys::time::mstime();
        let migration_pause_active = self.ctx.client_pause().reason == "slot_migration_in_progress"
            && self.ctx.client_pause().is_paused(now_ms) != PauseAction::None;
        let (resume_pause, commit_import, complete_exports, timed_out_exports) = self
            .ctx
            .cluster_state()
            .as_deref()
            .map(|state| {
                let views = state.migrations.active_export_views();
                let max_remaining = self.ctx.config().server.slot_migration_max_failover_repl_bytes;
                let resume_pause = (!state.debug_prevent_slot_migration_pause())
                    .then(|| views.iter().find(|job| job.phase == MigrationPhase::Export(ExportPhase::WaitingToPause)).map(|job| job.id.clone()))
                    .flatten()
                    .filter(|id| {
                        max_remaining < 0
                            || state
                                .migrations
                                .get(id)
                                .is_some_and(|job| job.remaining_repl_size <= max_remaining as usize)
                    });
                let commit_import = (!state.debug_prevent_slot_migration_failover())
                    .then(|| state.migrations.views().into_iter().find(|job| job.phase == MigrationPhase::Import(crate::engine::distributed::cluster::migration::ImportPhase::FailoverGranted)).map(|job| job.id))
                    .flatten();
                let complete_exports = views.into_iter().filter(|job| {
                    job.phase == MigrationPhase::Export(ExportPhase::FailoverGranted)
                        && job.ranges.iter().all(|range| (range.start..=range.end).all(|slot| {
                            state.declared_node_by_slot(slot as usize).is_some_and(|owner| owner.name == job.target.as_str())
                        }))
                }).map(|job| job.id).collect::<Vec<_>>();
                let timed_out_exports = if !migration_pause_active {
                    state
                        .migrations
                        .active_export_views()
                        .into_iter()
                        .filter_map(|job| match job.phase {
                            MigrationPhase::Export(ExportPhase::FailoverPaused) => {
                                Some((job.id, "Timed out before streaming completed"))
                            }
                            MigrationPhase::Export(ExportPhase::FailoverGranted) => Some((
                                job.id,
                                "Unpaused before migration completed (migration may have succeeded with lost writes)",
                            )),
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                };
                (resume_pause, commit_import, complete_exports, timed_out_exports)
            })
            .unwrap_or_default();
        if !timed_out_exports.is_empty() {
            for (id, message) in timed_out_exports {
                self.fail_slot_migration(&id, message);
            }
            self.drive_slot_migration_actions();
            return;
        }
        if let Some(id) = resume_pause {
            let remaining = self
                .ctx
                .cluster_state()
                .as_deref()
                .and_then(|state| state.migrations.get(&id))
                .map_or(0, |job| job.remaining_repl_size);
            crate::server_log!(
                notice,
                "Pausing writes (remaining_repl_size is {}) to allow slot migration {} to finalize failover.",
                remaining,
                id
            );
            self.pause_for_slot_migration(now_ms);
            self.transition_slot_migration(&id, MigrationEvent::OutputDrained);
        }
        if let Some(id) = commit_import {
            self.commit_slot_import(&id);
        }
        for id in complete_exports {
            let ranges = self
                .ctx
                .cluster_state()
                .as_deref()
                .and_then(|state| state.migrations.get(&id))
                .map(|job| job.ranges.clone());
            if let Some(ranges) = ranges {
                for range in ranges.iter() {
                    for slot in range.start..=range.end {
                        let entries = self.ctx.dbs_mut().take_slot(slot as usize);
                        if !entries.is_empty() {
                            self.ctx.persistence_mut().add_dirty(entries.len() as i64);
                        }
                        for entry in entries {
                            self._propagate_background_unlink(entry.db_id, &entry.key);
                        }
                    }
                }
            }
            let actions = self
                .ctx
                .cluster_state_mut()
                .as_deref_mut()
                .and_then(|state| {
                    state
                        .transition_migration(
                            &id,
                            MigrationEvent::OwnershipCommitted,
                            crate::foundation::io::sys::time::unixtime(),
                        )
                        .ok()
                })
                .unwrap_or_default();
            if let Some(state) = self.ctx.cluster_state_mut().as_deref_mut() {
                state.queue_migration_actions(actions);
            }
            self.drive_slot_migration_actions();
        }
    }

    pub(crate) fn send_slot_migration_periodic_acks(&mut self) {
        let now = crate::foundation::io::sys::time::unixtime();
        let jobs = self
            .ctx
            .cluster_state()
            .as_deref()
            .map(|state| {
                state
                    .migrations
                    .views()
                    .into_iter()
                    .filter(|job| {
                        // Match C clusterSlotMigrationCron: after an exporter
                        // grants failover it no longer sends periodic ACKs. A
                        // late ACK would make remaining_repl_size nonzero even
                        // though the target has already completed the job.
                        // The 1s throttle is enforced separately by `ack_due`.
                        // Do NOT gate on `last_ack_time` (the last *received*
                        // ack time): doing so makes a node that keeps receiving
                        // ACKs stop sending its own, which deadlocks the mutual
                        // liveness exchange and stalls the peer's last_ack_time.
                        !job.phase.is_terminal()
                            && !matches!(
                                job.phase,
                                MigrationPhase::Import(crate::engine::distributed::cluster::migration::ImportPhase::WaitingForAck)
                                    | MigrationPhase::Import(crate::engine::distributed::cluster::migration::ImportPhase::OccurringOnPrimary)
                                    | MigrationPhase::Export(ExportPhase::Connecting
                                        | ExportPhase::SendingAuthCommand
                                        | ExportPhase::ReadingAuthResponse
                                        | ExportPhase::SendingEstablishCommand
                                        | ExportPhase::ReadingEstablishResponse
                                        | ExportPhase::Snapshotting
                                        | ExportPhase::FailoverGranted)
                            )
                    })
                    .map(|job| job.id)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for id in jobs {
            if self.slot_migration_runtime.ack_due(&id, now) {
                crate::server_log!(
                    debug,
                    "Slot migration {} sending periodic ACK at {}",
                    id,
                    now
                );
                self.queue_slot_migration_message(&id, MigrationProtocolMessage::Ack);
                // C updates `job->last_ack` only when receiving ACK. Queueing
                // this liveness message must not advance received-ACK state;
                // the shared before-sleep pending phase owns its socket write.
            }
        }
    }

    pub(super) fn transition_slot_migration(&mut self, id: &MigrationJobId, event: MigrationEvent) {
        let before = self.migration_phase(id);
        let actions = self
            .ctx
            .cluster_state_mut()
            .as_deref_mut()
            .and_then(|state| {
                state
                    .transition_migration(id, event, crate::foundation::io::sys::time::unixtime())
                    .ok()
            })
            .unwrap_or_default();
        let after = self.migration_phase(id);
        crate::server_log!(
            debug,
            "Slot migration {} transition {:?} -> {:?} on {:?}",
            id,
            before,
            after,
            event
        );
        if let Some(state) = self.ctx.cluster_state_mut().as_deref_mut() {
            state.queue_migration_actions(actions);
        }
        self.drive_slot_migration_actions();
    }

    fn migration_phase(&self, id: &MigrationJobId) -> Option<MigrationPhase> {
        self.ctx
            .cluster_state()
            .as_deref()?
            .migrations
            .get(id)
            .map(|job| job.phase)
    }

    pub(crate) fn fail_slot_migration(&mut self, id: &MigrationJobId, message: &str) {
        crate::server_log!(warning, "Slot migration {} failed: {}", id, message);
        let actions = self
            .ctx
            .cluster_state_mut()
            .as_deref_mut()
            .and_then(|state| {
                state
                    .migrations
                    .finish(
                        id,
                        crate::engine::distributed::cluster::migration::TerminalPhase::Failed,
                        Some(message.to_string()),
                        crate::foundation::io::sys::time::unixtime(),
                    )
                    .ok()
            })
            .unwrap_or_default();
        if let Some(state) = self.ctx.cluster_state_mut().as_deref_mut() {
            state.queue_migration_actions(actions);
        }
        self.drive_slot_migration_actions();
    }

    /// Trim terminal migration jobs to the configured history length. Mirrors C's
    /// `clusterCleanupSlotMigrationLog`, which runs on every cron tick so a failed
    /// or completed job cannot permanently block re-migration of the same slot.
    fn trim_slot_migration_log(&mut self) {
        let max_len = self.ctx.config().server.cluster_slot_migration_log_max_len as usize;
        if let Some(state) = self.ctx.cluster_state_mut().as_deref_mut() {
            state.trim_migration_history(max_len);
        }
    }
}

fn resolve_target(host: &str, port: u16) -> Option<SocketAddr> {
    let authority = if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    authority.to_socket_addrs().ok()?.next()
}
