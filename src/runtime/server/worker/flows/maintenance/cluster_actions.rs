use crate::engine::distributed::cluster::action::{
    ClusterAction, Promotion, ReplicationSwitchMode, ReplicationTarget,
};
use crate::engine::distributed::cluster::event::ClusterEvent;
use crate::engine::distributed::cluster::reducer::reduce;
use crate::engine::distributed::cluster::state::ClusterState;
use crate::framework::context::PauseAction;
use crate::framework::distributed::replication::MasterInfo;
use crate::runtime::server::worker::ServerWorkerState;
use std::collections::VecDeque;

impl ServerWorkerState {
    pub(crate) fn apply_pending_cluster_role_actions(&mut self) {
        let mut state = self.ctx.cluster_state_mut().take();
        if let Some(cluster_state) = state.as_deref_mut() {
            let actions = reduce(cluster_state, ClusterEvent::DrainLinkCloses);
            self.execute_cluster_link_actions(cluster_state, actions);
            let actions = cluster_state.take_manual_failover_actions();
            self.execute_cluster_actions(
                cluster_state,
                actions,
                crate::foundation::io::sys::time::mstime(),
            );
            self.cluster_apply_pending_replication_switch(cluster_state);
            // Best-effort role changes must attempt persistence at this command
            // boundary so the visible role and its retry diagnostic stay in
            // lockstep. Sync mode keeps the request queued for server cron: a
            // fatal save failure must occur only after this command's reply has
            // been flushed to the client.
            if self.ctx.config().server.cluster_config_save_behavior == "best-effort" {
                self.persist_cluster_config_if_changed(cluster_state);
            }
        }
        *self.ctx.cluster_state_mut() = state;
        self.drive_slot_migration_actions();
    }

    pub(in crate::runtime) fn execute_cluster_actions(
        &mut self,
        state: &mut ClusterState,
        actions: Vec<ClusterAction>,
        now_ms: i64,
    ) {
        let mut actions = VecDeque::from(actions);
        while let Some(action) = actions.pop_front() {
            match action {
                ClusterAction::PromoteSelf(promotion) => {
                    actions.extend(self.commit_cluster_promotion(state, &promotion, now_ms));
                }
                ClusterAction::DetachReplica => {
                    self.detach_cluster_replica(state);
                }
                ClusterAction::FollowPrimary(target) => {
                    if self.follow_cluster_primary(state, &target) {
                        actions.extend(state.commit_manual_role_change());
                    }
                }
                ClusterAction::PauseWrites {
                    requester,
                    until_ms,
                } => {
                    if self.ctx.has_running_script() {
                        state.queue_cluster_actions([ClusterAction::PauseWrites {
                            requester,
                            until_ms,
                        }]);
                        continue;
                    }
                    crate::server_log!(
                        notice,
                        "Manual failover requested by replica {}.",
                        requester
                    );
                    self.ctx.client_pause_mut().action = PauseAction::Write;
                    self.ctx.client_pause_mut().end_time_ms = until_ms;
                    self.ctx.client_pause_mut().reason = "failover_in_progress";
                    state
                        .set_local_replication_offset(self.ctx.replication().primary_repl_offset());
                    state.commit_manual_primary_pause();
                    self.cluster_bus.queue_paused_pong(state, &requester);
                }
                ClusterAction::ResumeWrites { timed_out } => {
                    if timed_out {
                        crate::server_log!(warning, "Manual failover timed out.");
                    }
                    self.ctx.client_pause_mut().action = PauseAction::None;
                    self.ctx.client_pause_mut().end_time_ms = 0;
                    self.ctx.client_pause_mut().reason = "none";
                    self.ctx.set_pending_unpause(true);
                }
                ClusterAction::SendManualFailoverStart { primary } => {
                    self.cluster_bus
                        .queue_manual_failover_start(state, &primary);
                }
                ClusterAction::BeginManualElection => {}
                ClusterAction::PersistClusterConfig => state.request_config_save(),
                ClusterAction::BroadcastTopology => state.request_topology_broadcast(),
                ClusterAction::BroadcastFailoverAuthRequest { .. }
                | ClusterAction::SendFailoverAuthAck { .. }
                | ClusterAction::RecordVote { .. }
                | ClusterAction::OpenMeet { .. }
                | ClusterAction::OpenConnect { .. }
                | ClusterAction::CloseLinks { .. }
                | ClusterAction::ForgetLinks { .. } => {}
            }
        }
    }

    fn commit_cluster_promotion(
        &mut self,
        state: &mut ClusterState,
        promotion: &Promotion,
        now_ms: i64,
    ) -> Vec<ClusterAction> {
        if !state.apply_promotion(promotion, now_ms) {
            return Vec::new();
        }
        let migration_actions = state.fail_import_migrations(
            "A failover occurred during slot import",
            crate::foundation::io::sys::time::unixtime(),
        );
        state.queue_migration_actions(migration_actions);
        self.abort_replica_session_for_role_change();
        self.ctx.config_mut().replication.master_host = None;
        self.ctx.config_mut().replication.master_port = 0;
        self.ctx.config_mut().replication.master_link_status = "down".to_string();
        self.ctx.replication_mut().switch_to_primary();
        self.clear_cluster_role_transition();
        let mut actions = state.automatic_failover_on_topology_change(true, true);
        actions.extend(state.commit_manual_role_change());
        state.request_topology_broadcast();
        actions
    }

    fn detach_cluster_replica(&mut self, state: &mut ClusterState) {
        let cluster_role_is_replica = state.myself.is_replica();
        if !cluster_role_is_replica && !self.ctx.replication().is_replica() {
            return;
        }
        let previous_primary = state.myself.replicaof.clone();
        self.abort_replica_session_for_role_change();
        self.replication_runtime.clear_replica_resume();
        self.ctx.flush_all_dbs();
        self.ctx.clear_slot_imports();
        self.ctx.config_mut().replication.master_host = None;
        self.ctx.config_mut().replication.master_port = 0;
        self.ctx.config_mut().replication.master_link_status = "down".to_string();
        self.ctx.replication_mut().detach_to_primary();
        if cluster_role_is_replica {
            state.reset_replica_as_empty_primary();
            state.clear_primary_migration_target_if_orphaned(&previous_primary);
        }
        self.clear_cluster_role_transition();
    }

    pub(super) fn follow_known_cluster_primary(
        &mut self,
        state: &mut ClusterState,
        node_id: &str,
        mode: ReplicationSwitchMode,
    ) -> bool {
        let Ok(node_id) = crate::engine::distributed::cluster::NodeId::parse(node_id.as_bytes())
        else {
            return false;
        };
        let Some(primary) = state.nodes.get(node_id.as_str()) else {
            return false;
        };
        let target = ReplicationTarget {
            node_id: node_id.clone(),
            host: primary.ip().to_string(),
            port: primary.port(),
            mode,
        };
        self.execute_cluster_actions(
            state,
            vec![ClusterAction::FollowPrimary(target)],
            crate::foundation::io::sys::time::mstime(),
        );
        state.myself.is_replica() && state.myself.replicaof == node_id.as_str()
    }

    pub(super) fn follow_cluster_primary(
        &mut self,
        state: &mut ClusterState,
        target: &ReplicationTarget,
    ) -> bool {
        if !self.cluster_target_matches(state, target) {
            return false;
        }
        let master = MasterInfo {
            host: target.host.clone(),
            port: target.port,
        };
        let already_following = state.myself.is_replica()
            && state.myself.replicaof == target.node_id.as_str()
            && self.ctx.replication().is_replica()
            && self.ctx.config().replication.master_host.as_deref() == Some(target.host.as_str())
            && self.ctx.config().replication.master_port == target.port
            && match target.mode {
                ReplicationSwitchMode::TryPartial => self
                    .replication_runtime
                    .replica_resume_for(&master)
                    .is_some(),
                ReplicationSwitchMode::ForceFull => {
                    self.replication_runtime.force_full_sync_pending()
                        || self.replication_runtime.replica_link().is_some_and(|link| {
                            link.master() == &master
                                && (link.is_streaming()
                                    || self
                                        .replication_runtime
                                        .replica_resume_for(&master)
                                        .is_none())
                        })
                }
            };
        if already_following {
            return true;
        }
        let was_primary = state.myself.is_primary();
        let cluster_role_matches = state.myself.is_replica()
            && state.myself.replicaof == target.node_id.as_str()
            && state.myself.shard_id == state.nodes[target.node_id.as_str()].shard_id
            && state.myself.numslots == 0;
        if !cluster_role_matches && state.replicate(target.node_id.as_str().as_bytes()).is_err() {
            return false;
        }
        if was_primary && state.myself.is_replica() {
            // C `replicationSetMaster` redirects blocked writers after the
            // primary-to-replica mutation. The background coordinator holds
            // ClusterState out of ServerContext here, so pass this exact
            // snapshot rather than relying on a later cron lookup.
            self.check_blocked_cluster_redirects_after_demotion(state);
        }
        let import_ids =
            state.track_imports_after_demotion(crate::foundation::io::sys::time::unixtime());
        for id in import_ids {
            let connections = self.connection_registry
                .iter()
                .filter_map(|(_, connection)| {
                    matches!(
                        connection.slot_migration_role.as_ref(),
                        Some(crate::engine::distributed::cluster::migration::SlotMigrationConnectionRole::Import(job_id))
                            if job_id == &id
                    )
                    .then_some(connection.token().0)
                })
                .collect::<Vec<_>>();
            for connection_id in connections {
                if let Some(connection_token) = self
                    .connection_registry
                    .contains(crate::framework::capabilities::io::Token(connection_id))
                    .then_some(connection_id)
                {
                    self.connection_registry
                        [crate::framework::capabilities::io::Token(connection_token)]
                    .slot_migration_role = None;
                    self.connection_registry
                        [crate::framework::capabilities::io::Token(connection_token)]
                    .mark_closing();
                }
            }
        }
        let resume = self.cluster_replication_resume(target.mode);
        // Reparenting replaces the entire old replica session.  Keep the
        // transport, async RDB loader, and externally visible loading state in
        // one teardown path; removing only the main socket can leave an old
        // loader rejecting client reads after the new primary is selected.
        self.abort_replica_session_for_role_change();
        self.install_cluster_replication_target(target, resume);
        state.request_topology_broadcast();
        // C replicationSetPrimary starts the nonblocking connection during the
        // role change. This state is temporarily outside ServerContext, so pass
        // the local node ID explicitly for the replication handshake.
        self.drive_cluster_replication(&state.myself.name);
        true
    }

    fn cluster_target_matches(&self, state: &ClusterState, target: &ReplicationTarget) -> bool {
        state
            .nodes
            .get(target.node_id.as_str())
            .is_some_and(|node| {
                node.is_primary() && node.ip() == target.host && node.port() == target.port
            })
    }

    fn cluster_replication_resume(
        &self,
        mode: ReplicationSwitchMode,
    ) -> Option<(
        String,
        crate::engine::distributed::replication::ReplicationOffset,
        u32,
    )> {
        if mode == ReplicationSwitchMode::ForceFull
            || self.replication_runtime.force_full_sync_pending()
        {
            return None;
        }
        match self.replication_runtime.replica_link() {
            Some(link) if link.can_resume_after_disconnect() => link
                .master_replid()
                .map(|id| (id.to_string(), link.applied_position(), link.selected_db())),
            Some(_) => None,
            None if self.ctx.replication().is_replica() => {
                let previous_master = MasterInfo {
                    host: self.ctx.config().replication.master_host.clone()?,
                    port: self.ctx.config().replication.master_port,
                };
                self.replication_runtime
                    .replica_resume_for(&previous_master)
                    .map(|(id, offset, db)| (id.to_string(), offset, db))
            }
            None => Some((
                self.ctx.replication().backlog.replid().to_string(),
                self.ctx.replication().primary_repl_offset(),
                0,
            )),
        }
    }

    fn install_cluster_replication_target(
        &mut self,
        target: &ReplicationTarget,
        resume: Option<(
            String,
            crate::engine::distributed::replication::ReplicationOffset,
            u32,
        )>,
    ) {
        let master = MasterInfo {
            host: target.host.clone(),
            port: target.port,
        };
        match (target.mode, resume) {
            (ReplicationSwitchMode::TryPartial, Some((replid, offset, selected_db))) => self
                .replication_runtime
                .set_replica_resume(master, replid, offset, selected_db),
            (ReplicationSwitchMode::TryPartial | ReplicationSwitchMode::ForceFull, None) => {
                self.replication_runtime.clear_replica_resume();
            }
            _ => return,
        }
        self.ctx.config_mut().replication.master_host = Some(target.host.clone());
        self.ctx.config_mut().replication.master_port = target.port;
        self.ctx.config_mut().replication.master_link_status = "down".to_string();
        self.ctx.replication_mut().switch_to_replica();
        if target.mode == ReplicationSwitchMode::ForceFull {
            // A cross-shard CLUSTER REPLICATE must start from offset 0 until
            // the replacement full sync commits, matching C replicationSetPrimary.
            let zero = crate::engine::distributed::replication::ReplicationOffset::try_from(0)
                .expect("zero is a valid replication offset");
            self.ctx.replication_mut().slave_repl_offset = zero;
            self.ctx.replication_mut().slave_read_repl_offset = zero;
        }
        self.ctx.replication_mut().cache_primary_on_next_reconnect = false;
        // This Cluster-owned transition already tore down the old role and
        // installed the resume target above. Do not let the generic REPLICAOF
        // post-command hook repeat teardown and erase the fresh PSYNC cursor.
        self.ctx.replication_mut().pending_replicaof_request = false;
        // A Cluster role change already selected a valid primary and owns the
        // reconnect lifecycle. Do not inherit the generic disconnect backoff:
        // doing so exposes a window where the promoted primary has no online
        // replica and can also turn an otherwise valid PSYNC handoff into a
        // full resync before the next command observes the settled topology.
        self.replication_runtime.reset_master_reconnect_backoff();
        self.clear_cluster_role_transition();
    }

    fn clear_cluster_role_transition(&mut self) {
        self.ctx.replication_mut().pending_failover_target = None;
        self.ctx.replication_mut().failover_sync_offset = None;
        self.ctx.replication_mut().failover_state = "no-failover".to_string();
        self.ctx.replication_mut().failover_end_time_ms = 0;
        self.ctx.replication_mut().force_failover = false;
        self.ctx.client_pause_mut().action = PauseAction::None;
        self.ctx.client_pause_mut().end_time_ms = 0;
        self.ctx.set_pending_unpause(true);
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::distributed::cluster::action::{
        ClusterAction, Promotion, ReplicationSwitchMode, ReplicationTarget,
    };
    use crate::engine::distributed::cluster::node::{
        ClusterNode, CLUSTER_NODE_MYSELF, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
    };
    use crate::engine::distributed::cluster::state::{ClusterState, CLUSTER_SLOTS};
    use crate::engine::distributed::cluster::NodeId;
    use crate::engine::distributed::replication::{ReplicationOffset, ReplicationRole};
    use crate::engine::extension::script::ScriptRunCtx;
    use crate::framework::context::PauseAction;
    use crate::runtime::server::worker::ServerWorkerState;

    const PRIMARY_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const REPLICA_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn offset(value: i64) -> ReplicationOffset {
        ReplicationOffset::try_from(value).expect("valid test offset")
    }

    fn replica_state() -> ClusterState {
        let mut state = ClusterState::new("127.0.0.1", 6380, 16_380);
        state.myself.name = REPLICA_ID.to_string();
        state.myself.flags &= !CLUSTER_NODE_PRIMARY;
        state.myself.flags |= CLUSTER_NODE_REPLICA;
        state.myself.replicaof = PRIMARY_ID.to_string();

        let mut primary = ClusterNode::new(
            PRIMARY_ID.to_string(),
            "127.0.0.1".to_string(),
            6379,
            16_379,
            true,
        );
        primary.flags &= !CLUSTER_NODE_MYSELF;
        primary.shard_id = PRIMARY_ID.to_string();
        primary.slots[42] = true;
        primary.numslots = 1;
        state.nodes.insert(primary.name.clone(), primary);
        state
    }

    fn promotion() -> Promotion {
        let mut slots = vec![false; CLUSTER_SLOTS];
        slots[42] = true;
        Promotion {
            old_primary: NodeId::parse(PRIMARY_ID.as_bytes()).unwrap(),
            shard_id: NodeId::parse(PRIMARY_ID.as_bytes()).unwrap(),
            slots,
            config_epoch: 7,
        }
    }

    #[test]
    fn pending_manual_failover_start_is_sent_to_primary() {
        use crate::engine::distributed::cluster::msg::{ClusterBusMessage, ClusterMsgType};
        use crate::framework::capabilities::io::Token;

        let mut runtime = ServerWorkerState::new(1).unwrap();
        let mut state = replica_state();
        let token = Token(99);
        runtime.cluster_bus.register_outgoing_for_node(
            token,
            "127.0.0.1:16379".parse().unwrap(),
            false,
            PRIMARY_ID.to_string(),
        );
        state.queue_cluster_actions([ClusterAction::SendManualFailoverStart {
            primary: NodeId::parse(PRIMARY_ID.as_bytes()).unwrap(),
        }]);
        *runtime.ctx.cluster_state_mut() = Some(Box::new(state));

        runtime.apply_pending_cluster_role_actions();

        let bytes = &runtime.cluster_bus.connections[&token.0].write_buf;
        let (message, consumed) = ClusterBusMessage::decode(bytes).unwrap();
        assert_eq!(message.msg_type, ClusterMsgType::MfStart);
        assert_eq!(consumed, bytes.len());
    }

    #[test]
    fn post_command_cluster_actions_close_all_requested_bus_links() {
        use crate::engine::distributed::cluster::ClusterLinkDirection;
        use crate::framework::capabilities::io::Token;

        const OTHER_ID: &str = "cccccccccccccccccccccccccccccccccccccccc";

        let mut runtime = ServerWorkerState::new(1).unwrap();
        let mut state = replica_state();
        state
            .pending_link_closes
            .push((PRIMARY_ID.to_string(), ClusterLinkDirection::All));
        *runtime.ctx.cluster_state_mut() = Some(Box::new(state));

        runtime.cluster_bus.register_outgoing_for_node(
            Token(97),
            "127.0.0.1:16379".parse().unwrap(),
            false,
            PRIMARY_ID.to_string(),
        );
        runtime
            .cluster_bus
            .register_incoming(Token(98), "127.0.0.1:26379".parse().unwrap());
        runtime
            .cluster_bus
            .connections
            .get_mut(&98)
            .unwrap()
            .node_name = PRIMARY_ID.to_string();
        runtime.cluster_bus.register_outgoing_for_node(
            Token(99),
            "127.0.0.1:16381".parse().unwrap(),
            false,
            OTHER_ID.to_string(),
        );

        runtime.apply_pending_cluster_role_actions();

        assert!(!runtime.cluster_bus.connections.contains_key(&97));
        assert!(!runtime.cluster_bus.connections.contains_key(&98));
        assert!(runtime.cluster_bus.connections.contains_key(&99));
        assert!(runtime
            .ctx
            .cluster_state()
            .as_ref()
            .unwrap()
            .pending_link_closes
            .is_empty());
    }

    #[test]
    fn post_command_link_close_to_disconnects_outbound_and_queues_one_reconnect() {
        use crate::engine::distributed::cluster::ClusterLinkDirection;
        use crate::framework::capabilities::io::Token;

        let mut runtime = ServerWorkerState::new(1).unwrap();
        let mut state = replica_state();
        state
            .pending_link_closes
            .push((PRIMARY_ID.to_string(), ClusterLinkDirection::To));
        state
            .pending_connects
            .push(("127.0.0.1".to_string(), 16_379, false));
        *runtime.ctx.cluster_state_mut() = Some(Box::new(state));

        runtime.cluster_bus.register_outgoing_for_node(
            Token(97),
            "127.0.0.1:16379".parse().unwrap(),
            false,
            PRIMARY_ID.to_string(),
        );
        runtime
            .cluster_bus
            .register_incoming(Token(98), "127.0.0.1:26379".parse().unwrap());
        runtime
            .cluster_bus
            .connections
            .get_mut(&98)
            .unwrap()
            .node_name = PRIMARY_ID.to_string();

        runtime.apply_pending_cluster_role_actions();

        assert!(!runtime.cluster_bus.connections.contains_key(&97));
        assert!(runtime.cluster_bus.connections.contains_key(&98));
        assert_eq!(
            runtime
                .ctx
                .cluster_state()
                .as_ref()
                .unwrap()
                .pending_connects,
            vec![("127.0.0.1".to_string(), 16_379, false)]
        );
    }

    #[test]
    fn promotion_commits_cluster_and_replication_roles_once() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        let mut state = replica_state();
        runtime.ctx.replication_mut().switch_to_replica();
        runtime.ctx.config_mut().replication.master_host = Some("127.0.0.1".to_string());
        runtime.ctx.config_mut().replication.master_port = 6379;
        let old_replid = runtime.ctx.replication().backlog.replid().to_string();

        runtime.execute_cluster_actions(
            &mut state,
            vec![ClusterAction::PromoteSelf(promotion())],
            1_000,
        );

        assert!(state.myself.is_primary());
        assert_eq!(state.myself.shard_id, PRIMARY_ID);
        assert!(state.myself.slots[42]);
        assert_eq!(state.myself.config_epoch, 7);
        assert_eq!(runtime.ctx.replication().role, ReplicationRole::Primary);
        assert_eq!(
            runtime.ctx.replication().backlog.second_replid(),
            old_replid
        );
        assert!(runtime.ctx.config().replication.master_host.is_none());
        assert!(state.take_config_save_request());
        assert!(state.take_topology_broadcast_request());

        let promoted_replid = runtime.ctx.replication().backlog.replid().to_string();
        runtime.execute_cluster_actions(
            &mut state,
            vec![ClusterAction::PromoteSelf(promotion())],
            1_001,
        );
        assert_eq!(runtime.ctx.replication().backlog.replid(), promoted_replid);
    }

    #[test]
    fn post_command_promotion_attempts_best_effort_config_save_before_returning() {
        let dir = std::env::temp_dir().join(format!(
            "valkey-cluster-role-save-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock before Unix epoch")
                .as_nanos()
        ));
        std::fs::create_dir(&dir).expect("create test directory");
        let path = dir.join("nodes.conf");
        std::fs::create_dir(&path).expect("make config destination a directory");

        let mut runtime = ServerWorkerState::new(1).unwrap();
        runtime.cluster_config.path = Some(path);
        runtime.ctx.config_mut().server.cluster_config_save_behavior = "best-effort".to_string();
        let mut state = replica_state();
        state.queue_cluster_actions([ClusterAction::PromoteSelf(promotion())]);
        *runtime.ctx.cluster_state_mut() = Some(Box::new(state));

        runtime.apply_pending_cluster_role_actions();

        assert!(runtime.cluster_config.last_best_effort_warning_ms > 0);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn promotion_fails_tracking_slot_imports() {
        use crate::engine::distributed::cluster::migration::{
            MigrationAction, MigrationJob, MigrationJobId, MigrationOperation, MigrationPhase,
            SlotRanges, TerminalPhase,
        };
        use crate::engine::distributed::cluster::SlotRange;

        let mut runtime = ServerWorkerState::new(1).unwrap();
        let mut state = replica_state();
        let id = MigrationJobId::parse(b"cccccccccccccccccccccccccccccccccccccccc").unwrap();
        let job = MigrationJob::new_tracking_import(
            id.clone(),
            NodeId::parse(PRIMARY_ID.as_bytes()).unwrap(),
            NodeId::parse(REPLICA_ID.as_bytes()).unwrap(),
            SlotRanges::new(vec![SlotRange::new(42, 42).unwrap()]).unwrap(),
            1,
        );
        assert_eq!(job.operation, MigrationOperation::Import);
        state.migrations.start(job).unwrap();

        runtime.execute_cluster_actions(
            &mut state,
            vec![ClusterAction::PromoteSelf(promotion())],
            1_000,
        );

        let job = state.migrations.get(&id).unwrap();
        assert_eq!(job.phase, MigrationPhase::Terminal(TerminalPhase::Failed));
        assert_eq!(
            job.message.as_deref(),
            Some("A failover occurred during slot import")
        );
        assert_eq!(
            state.take_migration_actions(),
            vec![MigrationAction::Cleanup(id)]
        );
    }

    #[test]
    fn detach_replica_commits_empty_primary_and_clears_replication_session() {
        use crate::engine::distributed::cluster::state::ClusterReplicaHealth;
        use crate::foundation::io::store::Robj;
        use crate::framework::distributed::replication::{ConnectionId, MasterInfo, ReplicaLink};
        let mut runtime = ServerWorkerState::new(1).unwrap();
        let mut state = replica_state();
        let old_shard = state.myself.shard_id.clone();
        runtime.ctx.replication_mut().switch_to_replica();
        runtime.ctx.config_mut().replication.master_host = Some("127.0.0.1".to_string());
        runtime.ctx.config_mut().replication.master_port = 6379;
        runtime.ctx.db_mut(0).unwrap().set(
            b"key".to_vec(),
            Robj::string(bytes::Bytes::from_static(b"value")),
        );
        runtime
            .replication_runtime
            .set_replica_link(ReplicaLink::streaming(
                ConnectionId(99),
                MasterInfo {
                    host: "127.0.0.1".to_string(),
                    port: 6379,
                },
                "0123456789012345678901234567890123456789".to_string(),
                offset(42),
            ));

        runtime.execute_cluster_actions(&mut state, vec![ClusterAction::DetachReplica], 1_000);

        assert!(state.myself.is_primary());
        assert!(state.myself.replicaof.is_empty());
        assert_ne!(state.myself.shard_id, old_shard);
        assert_eq!(state.myself.numslots, 0);
        assert_eq!(runtime.ctx.db_size(0), 0);
        assert_eq!(runtime.ctx.replication().role, ReplicationRole::Primary);
        assert!(runtime.ctx.config().replication.master_host.is_none());
        assert_eq!(runtime.ctx.config().replication.master_port, 0);
        assert!(runtime.replication_runtime.replica_link().is_none());
        assert!(runtime
            .ctx
            .replication()
            .view
            .cluster_replica_views
            .values()
            .all(|view| view.health != ClusterReplicaHealth::Online));
        assert!(state.take_config_save_request());
        assert!(state.take_topology_broadcast_request());
    }

    #[test]
    fn reset_replica_detach_clears_resume_and_forces_next_full_sync() {
        use crate::engine::distributed::cluster::migration::MigrationJobId;
        use crate::framework::distributed::replication::{ConnectionId, MasterInfo, ReplicaLink};

        let mut runtime = ServerWorkerState::new(1).unwrap();
        let mut state = replica_state();
        runtime.ctx.replication_mut().switch_to_replica();
        runtime.ctx.config_mut().replication.master_host = Some("127.0.0.1".to_string());
        runtime.ctx.config_mut().replication.master_port = 6379;
        runtime.ctx.create_slot_import(
            MigrationJobId::parse(b"dddddddddddddddddddddddddddddddddddddddd").unwrap(),
            &crate::engine::distributed::cluster::migration::SlotRanges::new(vec![
                crate::engine::distributed::cluster::SlotRange::new(0, 0).unwrap(),
            ])
            .unwrap(),
        );
        assert_eq!(runtime.ctx.slot_import_dbs().len(), 1);
        let master = MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        };
        runtime
            .replication_runtime
            .set_replica_link(ReplicaLink::streaming(
                ConnectionId(99),
                master.clone(),
                "0123456789012345678901234567890123456789".to_string(),
                offset(42),
            ));
        state.reset_soft();

        runtime.execute_cluster_actions(&mut state, vec![ClusterAction::DetachReplica], 1_000);

        assert!(state.myself.is_primary());
        assert_eq!(runtime.ctx.replication().role, ReplicationRole::Primary);
        assert!(runtime.replication_runtime.replica_link().is_none());
        assert!(runtime.ctx.slot_import_dbs().is_empty());
        assert_eq!(
            runtime.replication_runtime.replica_resume_for(&master),
            None
        );
        assert!(runtime.replication_runtime.force_full_sync_pending());
        assert_eq!(
            runtime.cluster_replication_resume(ReplicationSwitchMode::TryPartial),
            None
        );
    }

    #[test]
    fn cluster_role_switch_starts_upstream_connect_before_next_cron() {
        use crate::engine::distributed::replication::protocol;
        use crate::framework::distributed::replication::ReplicaAction;
        use std::net::TcpListener;

        for mode in [
            ReplicationSwitchMode::TryPartial,
            ReplicationSwitchMode::ForceFull,
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            let mut runtime = ServerWorkerState::new(1).unwrap();
            runtime.ctx.replication_mut().backlog.activate();
            runtime
                .ctx
                .replication_mut()
                .backlog
                .append(&[b'x'; 42])
                .unwrap();
            let replid = runtime.ctx.replication().backlog.replid().to_string();
            let mut state = ClusterState::new("127.0.0.1", 6380, 16_380);
            let local_id = state.myself.name.clone();
            let mut primary = ClusterNode::new(
                PRIMARY_ID.to_string(),
                "127.0.0.1".to_string(),
                port,
                16_379,
                true,
            );
            primary.flags &= !CLUSTER_NODE_MYSELF;
            state.nodes.insert(primary.name.clone(), primary);

            // Cluster packet processing holds this state outside ServerContext.
            assert!(runtime.ctx.cluster_state().is_none());
            runtime.execute_cluster_actions(
                &mut state,
                vec![ClusterAction::FollowPrimary(ReplicationTarget {
                    node_id: NodeId::parse(PRIMARY_ID.as_bytes()).unwrap(),
                    host: "127.0.0.1".to_string(),
                    port,
                    mode,
                })],
                1_000,
            );

            assert_eq!(
                runtime.ctx.config().replication.master_link_status,
                "connect"
            );
            assert_eq!(
                runtime
                    .ctx
                    .replication()
                    .primary_failover_data_age_ms(2_000),
                u64::MAX,
                "starting TCP must not make an unsynchronized replica eligible"
            );
            let link = runtime
                .replication_runtime
                .replica_link_mut()
                .expect("the role switch must start its nonblocking connection immediately");
            assert_eq!(link.master().port, port);
            link.connected(None, None);
            let (_, actions) = link.on_bytes(b"+PONG\r\n", 6380).unwrap();
            assert!(actions.contains(&ReplicaAction::Send(
                protocol::build_replconf_cluster_node_id(&local_id)
            )));
            let (_, actions) = link
                .on_bytes(b"+OK\r\n+OK\r\n+OK\r\n+OK\r\n", 6380)
                .unwrap();
            let expected = match mode {
                ReplicationSwitchMode::TryPartial => protocol::build_psync(&replid, 43),
                ReplicationSwitchMode::ForceFull => protocol::build_psync_full(),
            };
            assert_eq!(actions, vec![ReplicaAction::Send(expected)]);
        }
    }

    #[test]
    fn follow_primary_honors_partial_and_full_sync_modes() {
        for mode in [
            ReplicationSwitchMode::TryPartial,
            ReplicationSwitchMode::ForceFull,
        ] {
            let mut runtime = ServerWorkerState::new(1).unwrap();
            runtime.replication_runtime.schedule_master_reconnect(
                i64::MAX - 1,
                crate::framework::distributed::replication::MasterReconnectCause::ConnectFailed,
            );
            let mut state = ClusterState::new("127.0.0.1", 6380, 16_380);
            let mut primary = ClusterNode::new(
                PRIMARY_ID.to_string(),
                "127.0.0.1".to_string(),
                6379,
                16_379,
                true,
            );
            primary.flags &= !CLUSTER_NODE_MYSELF;
            state.nodes.insert(primary.name.clone(), primary);

            runtime.execute_cluster_actions(
                &mut state,
                vec![ClusterAction::FollowPrimary(ReplicationTarget {
                    node_id: NodeId::parse(PRIMARY_ID.as_bytes()).unwrap(),
                    host: "127.0.0.1".to_string(),
                    port: 6379,
                    mode,
                })],
                1_000,
            );

            assert!(state.myself.is_replica());
            assert_eq!(state.myself.replicaof, PRIMARY_ID);
            assert_eq!(runtime.ctx.replication().role, ReplicationRole::Replica);
            assert_eq!(
                runtime.ctx.config().replication.master_host.as_deref(),
                Some("127.0.0.1")
            );
            assert_eq!(runtime.ctx.config().replication.master_port, 6379);
            assert!(!runtime.ctx.replication().pending_replicaof_request);
            assert!(runtime.replication_runtime.master_connect_allowed(1_000));
            match mode {
                ReplicationSwitchMode::TryPartial => {
                    let target = crate::framework::distributed::replication::MasterInfo {
                        host: "127.0.0.1".to_string(),
                        port: 6379,
                    };
                    assert!(runtime
                        .replication_runtime
                        .replica_resume_for(&target)
                        .is_some());
                }
                ReplicationSwitchMode::ForceFull => {
                    let link = runtime.replication_runtime.replica_link().unwrap();
                    assert!(link.master_replid().is_none());
                    assert!(!runtime.replication_runtime.force_full_sync_pending());
                }
            }
        }
    }

    #[test]
    fn follow_primary_after_backlog_expiry_sends_only_the_new_identity() {
        use crate::engine::distributed::replication::protocol;
        use crate::framework::distributed::replication::{
            ConnectionId, MasterInfo, ReplicaAction, ReplicaLink,
        };

        let mut runtime = ServerWorkerState::new(1).unwrap();
        runtime
            .ctx
            .replication_mut()
            .backlog
            .append(b"expired-history")
            .unwrap();
        let expired_replid = runtime.ctx.replication().backlog.replid().to_string();
        runtime.ctx.replication_mut().backlog.expire_history();
        let current_replid = runtime.ctx.replication().backlog.replid().to_string();
        let mut state = ClusterState::new("127.0.0.1", 6380, 16_380);
        let mut primary = ClusterNode::new(
            PRIMARY_ID.to_string(),
            "127.0.0.1".to_string(),
            6379,
            16_379,
            true,
        );
        primary.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(primary.name.clone(), primary);

        runtime.execute_cluster_actions(
            &mut state,
            vec![ClusterAction::FollowPrimary(ReplicationTarget {
                node_id: NodeId::parse(PRIMARY_ID.as_bytes()).unwrap(),
                host: "127.0.0.1".to_string(),
                port: 6379,
                mode: ReplicationSwitchMode::TryPartial,
            })],
            1_000,
        );

        let master = MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        };
        let (cached_replid, cached_offset, cached_db) = runtime
            .replication_runtime
            .replica_resume_for(&master)
            .map(|(replid, offset, db)| (replid.to_string(), offset, db))
            .expect("partial follow must cache the post-expiry boundary");
        assert_eq!(cached_replid, current_replid);
        assert_ne!(cached_replid, expired_replid);

        let mut link = ReplicaLink::new(ConnectionId(99), master);
        link.set_resume_position(cached_replid.clone(), cached_offset, cached_db);
        link.connected(None, None);
        link.on_bytes(b"+PONG\r\n", 6380).unwrap();
        link.on_bytes(b"+OK\r\n", 6380).unwrap();
        let (_, actions) = link.on_bytes(b"+OK\r\n", 6380).unwrap();

        assert_eq!(
            actions,
            vec![ReplicaAction::Send(protocol::build_psync(
                &current_replid,
                cached_offset.get() + 1,
            ))]
        );
        assert!(actions.iter().all(|action| match action {
            ReplicaAction::Send(frame) => !frame
                .windows(expired_replid.len())
                .any(|window| window == expired_replid.as_bytes()),
            _ => true,
        }));
    }

    #[test]
    fn unfinished_full_sync_cannot_supply_reparent_resume_cursor() {
        use crate::framework::distributed::replication::{ConnectionId, MasterInfo, ReplicaLink};

        let mut runtime = ServerWorkerState::new(1).unwrap();
        let old_master = MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        };
        let mut link = ReplicaLink::new(ConnectionId(99), old_master);
        link.set_resume_position("replid".to_string(), offset(42), 0);
        assert!(!link.is_streaming());
        runtime.replication_runtime.set_replica_link(link);

        assert_eq!(
            runtime.cluster_replication_resume(ReplicationSwitchMode::TryPartial),
            None
        );
    }

    #[test]
    fn disconnected_unsynced_replica_cannot_fall_back_to_local_backlog() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        runtime.ctx.replication_mut().switch_to_replica();
        runtime.ctx.config_mut().replication.master_host = Some("127.0.0.1".to_string());
        runtime.ctx.config_mut().replication.master_port = 6379;

        assert_eq!(
            runtime.cluster_replication_resume(ReplicationSwitchMode::TryPartial),
            None
        );
    }

    #[test]
    fn duplicate_follow_primary_action_preserves_replica_progress() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6380, 16_380);
        let mut primary = ClusterNode::new(
            PRIMARY_ID.to_string(),
            "127.0.0.1".to_string(),
            6379,
            16_379,
            true,
        );
        primary.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(primary.name.clone(), primary);
        let action = ClusterAction::FollowPrimary(ReplicationTarget {
            node_id: NodeId::parse(PRIMARY_ID.as_bytes()).unwrap(),
            host: "127.0.0.1".to_string(),
            port: 6379,
            mode: ReplicationSwitchMode::TryPartial,
        });

        runtime.execute_cluster_actions(&mut state, vec![action.clone()], 1_000);
        runtime.ctx.replication_mut().slave_repl_offset = offset(42);
        runtime.ctx.replication_mut().slave_read_repl_offset = offset(41);
        runtime.execute_cluster_actions(&mut state, vec![action], 1_001);

        assert_eq!(runtime.ctx.replication().slave_repl_offset, offset(42));
        assert_eq!(runtime.ctx.replication().slave_read_repl_offset, offset(41));
    }

    #[test]
    fn force_full_upgrade_for_same_primary_replaces_partial_resume() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6380, 16_380);
        let mut primary = ClusterNode::new(
            PRIMARY_ID.to_string(),
            "127.0.0.1".to_string(),
            6379,
            16_379,
            true,
        );
        primary.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(primary.name.clone(), primary);
        let target = |mode| {
            ClusterAction::FollowPrimary(ReplicationTarget {
                node_id: NodeId::parse(PRIMARY_ID.as_bytes()).unwrap(),
                host: "127.0.0.1".to_string(),
                port: 6379,
                mode,
            })
        };

        runtime.execute_cluster_actions(
            &mut state,
            vec![target(ReplicationSwitchMode::TryPartial)],
            1_000,
        );
        runtime.execute_cluster_actions(
            &mut state,
            vec![target(ReplicationSwitchMode::ForceFull)],
            1_001,
        );

        let link = runtime.replication_runtime.replica_link().unwrap();
        assert!(link.master_replid().is_none());
        assert_eq!(
            runtime
                .replication_runtime
                .replica_resume_for(link.master()),
            None
        );
        assert!(!runtime.replication_runtime.force_full_sync_pending());
    }

    #[test]
    fn duplicate_force_full_action_does_not_restart_an_established_link() {
        use crate::framework::distributed::replication::{ConnectionId, MasterInfo, ReplicaLink};

        let mut runtime = ServerWorkerState::new(1).unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6380, 16_380);
        let mut primary = ClusterNode::new(
            PRIMARY_ID.to_string(),
            "127.0.0.1".to_string(),
            6379,
            16_379,
            true,
        );
        primary.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(primary.name.clone(), primary);
        let target = ReplicationTarget {
            node_id: NodeId::parse(PRIMARY_ID.as_bytes()).unwrap(),
            host: "127.0.0.1".to_string(),
            port: 6379,
            mode: ReplicationSwitchMode::ForceFull,
        };
        runtime.ctx.replication_mut().backlog.activate();
        runtime
            .ctx
            .replication_mut()
            .backlog
            .append(b"old-shard-history")
            .unwrap();
        runtime.execute_cluster_actions(
            &mut state,
            vec![ClusterAction::FollowPrimary(target.clone())],
            1_000,
        );
        assert!(!runtime.replication_runtime.force_full_sync_pending());
        assert!(runtime
            .replication_runtime
            .replica_link()
            .unwrap()
            .master_replid()
            .is_none());
        assert_eq!(runtime.ctx.replication().slave_repl_offset, offset(0));
        assert_eq!(runtime.ctx.replication().slave_read_repl_offset, offset(0));
        runtime
            .replication_runtime
            .set_replica_link(ReplicaLink::new(
                ConnectionId(99),
                MasterInfo {
                    host: target.host.clone(),
                    port: target.port,
                },
            ));
        runtime.ctx.replication_mut().slave_repl_offset = offset(42);
        runtime.ctx.replication_mut().slave_read_repl_offset = offset(42);

        runtime.execute_cluster_actions(
            &mut state,
            vec![ClusterAction::FollowPrimary(target)],
            1_001,
        );

        assert_eq!(runtime.ctx.replication().slave_repl_offset, offset(42));
        assert_eq!(
            runtime
                .replication_runtime
                .replica_link()
                .map(|link| link.connection_id()),
            Some(ConnectionId(99))
        );
    }

    #[test]
    fn pending_follow_is_retained_until_target_is_valid() {
        use crate::engine::distributed::cluster::state::PendingReplicationSwitch;

        let mut runtime = ServerWorkerState::new(1).unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6380, 16_380);
        state.pending_replication_switch = Some(PendingReplicationSwitch {
            node_id: PRIMARY_ID.to_string(),
            mode: ReplicationSwitchMode::TryPartial,
        });

        runtime.cluster_apply_pending_replication_switch(&mut state);
        assert!(state.pending_replication_switch.is_some());

        let mut primary = ClusterNode::new(
            PRIMARY_ID.to_string(),
            "127.0.0.1".to_string(),
            6379,
            16_379,
            true,
        );
        primary.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(primary.name.clone(), primary);
        runtime.cluster_apply_pending_replication_switch(&mut state);

        assert!(state.pending_replication_switch.is_none());
        assert_eq!(state.myself.replicaof, PRIMARY_ID);
        assert_eq!(runtime.ctx.replication().role, ReplicationRole::Replica);
    }

    #[test]
    fn following_promoted_replica_finishes_primary_side_manual_failover() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let promoted_id = NodeId::parse(REPLICA_ID.as_bytes()).unwrap();
        let mut promoted = ClusterNode::new(
            REPLICA_ID.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        promoted.flags &= !CLUSTER_NODE_MYSELF;
        state.nodes.insert(promoted.name.clone(), promoted);

        let pause_actions =
            state
                .manual_failover
                .accept_on_primary(promoted_id.clone(), 1_000, 5_000);
        runtime.execute_cluster_actions(&mut state, pause_actions, 1_000);
        assert_eq!(
            runtime.ctx.client_pause().is_paused(1_001),
            PauseAction::Write
        );

        runtime.execute_cluster_actions(
            &mut state,
            vec![ClusterAction::FollowPrimary(ReplicationTarget {
                node_id: promoted_id,
                host: "127.0.0.1".to_string(),
                port: 6380,
                mode: ReplicationSwitchMode::TryPartial,
            })],
            1_002,
        );

        assert!(!state.manual_failover.is_active());
        assert!(state.manual_failover.on_timeout(10_000).is_empty());
        assert_eq!(
            runtime.ctx.client_pause().is_paused(1_003),
            PauseAction::None
        );
    }

    #[test]
    fn primary_pause_waits_until_running_script_finishes() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let requester = NodeId::parse(REPLICA_ID.as_bytes()).unwrap();
        let actions = state
            .manual_failover
            .accept_on_primary(requester, 1_000, 5_000);
        let run = ScriptRunCtx::new_bare_for_test(0);
        runtime.ctx.enter_script(run.control.clone());

        runtime.execute_cluster_actions(&mut state, actions, 1_000);

        assert_eq!(
            runtime.ctx.client_pause().is_paused(1_001),
            PauseAction::None
        );
        assert!(!state.manual_failover.is_pausing_primary());
        let delayed = state.take_manual_failover_actions();
        assert!(matches!(
            delayed.as_slice(),
            [ClusterAction::PauseWrites { .. }]
        ));

        runtime.ctx.leave_script();
        runtime.execute_cluster_actions(&mut state, delayed, 1_002);
        assert_eq!(
            runtime.ctx.client_pause().is_paused(1_003),
            PauseAction::Write
        );
        assert!(state.manual_failover.is_pausing_primary());
    }
}
