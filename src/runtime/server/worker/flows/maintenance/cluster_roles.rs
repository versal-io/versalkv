use crate::engine::distributed::cluster::action::{ReplicationSwitchMode, ReplicationTarget};
use crate::engine::distributed::cluster::state::ClusterState;
use crate::engine::distributed::cluster::NodeId;
use crate::runtime::server::worker::ServerWorkerState;

impl ServerWorkerState {
    /// Orphaned primary -> replica of the higher-epoch owner.
    pub(super) fn cluster_maybe_orphan_to_replica(&mut self, state: &mut ClusterState) {
        let Some(new_primary_id) = state.orphan_replication_target() else {
            return;
        };
        let Some(primary) = state.nodes.get(&new_primary_id) else {
            return;
        };
        // Match C `clusterUpdateSlotsConfigWith`: cross-shard reparent
        // requires cluster-allow-replica-migration; same-shard reparent
        // (post-failover recovery) is always allowed.
        let same_shard = !primary.shard_id.is_empty() && primary.shard_id == state.myself.shard_id;
        if !same_shard && !state.is_replica_migration_allowed() {
            return;
        }
        let (host, port) = (primary.ip().to_string(), primary.port());
        crate::server_log!(
            notice,
            "Cluster self-heal: orphaned primary now replica of {} ({}:{})",
            new_primary_id,
            host,
            port
        );
        self.follow_known_cluster_primary(
            state,
            &new_primary_id,
            ReplicationSwitchMode::TryPartial,
        );
    }

    /// Apply an UPDATE-produced replication switch through the same role
    /// coordinator used by failover and replica migration.
    pub(crate) fn cluster_apply_pending_replication_switch(&mut self, state: &mut ClusterState) {
        let Some(pending) = state.pending_replication_switch.clone() else {
            return;
        };
        let Ok(node_id) = NodeId::parse(pending.node_id.as_bytes()) else {
            return;
        };
        let action = ReplicationTarget {
            node_id,
            host: state
                .nodes
                .get(&pending.node_id)
                .map(|node| node.ip().to_string())
                .unwrap_or_default(),
            port: state
                .nodes
                .get(&pending.node_id)
                .map(|node| node.port())
                .unwrap_or_default(),
            mode: pending.mode,
        };
        if self.follow_cluster_primary(state, &action) {
            state.clear_pending_replication_switch();
            // C `clusterSetPrimary` resets an in-flight manual failover after
            // completing the primary-to-replica transition. UPDATE-produced
            // switches must finish through the same coordinator path as an
            // explicit `ClusterAction::FollowPrimary`.
            let actions = state.commit_manual_role_change();
            self.execute_cluster_actions(
                state,
                actions,
                crate::foundation::io::sys::time::mstime(),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::distributed::cluster::action::ReplicationSwitchMode;
    use crate::engine::distributed::cluster::node::{
        ClusterNode, CLUSTER_NODE_MYSELF, CLUSTER_NODE_PRIMARY,
    };
    use crate::engine::distributed::cluster::state::{ClusterState, PendingReplicationSwitch};
    use crate::engine::distributed::cluster::NodeId;
    use crate::framework::context::PauseAction;
    use crate::runtime::server::worker::ServerWorkerState;

    const LOCAL_SHARD: &str = "0000000000000000000000000000000000000001";
    const REMOTE_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const REMOTE_SHARD: &str = "0000000000000000000000000000000000000002";

    fn primary_with_slot(slot: usize) -> ClusterState {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        state.myself.shard_id = LOCAL_SHARD.to_string();
        state.myself.config_epoch = 1;
        state.add_slot(slot).unwrap();
        state
    }

    fn insert_cross_shard_primary(state: &mut ClusterState, slot: usize, epoch: u64) {
        let mut node = ClusterNode::new(
            REMOTE_ID.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        node.flags &= !CLUSTER_NODE_MYSELF;
        node.flags |= CLUSTER_NODE_PRIMARY;
        node.shard_id = REMOTE_SHARD.to_string();
        node.config_epoch = epoch;
        node.slots[slot] = true;
        node.numslots = 1;
        state.nodes.insert(REMOTE_ID.to_string(), node);
    }

    #[test]
    fn cross_shard_orphan_demotion_requires_replica_migration_allowed() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        let mut state = primary_with_slot(0);
        insert_cross_shard_primary(&mut state, 0, 5);
        state.set_replica_migration_allowed(false);

        runtime.cluster_maybe_orphan_to_replica(&mut state);

        assert!(
            state.myself.is_primary(),
            "cross-shard rebalance must not demote a slot-owning primary when \
             cluster-allow-replica-migration is disabled"
        );
        assert!(state.myself.replicaof.is_empty());
        assert!(state.myself.slots[0]);
    }

    #[test]
    fn cross_shard_orphan_demotion_runs_when_replica_migration_allowed() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        let mut state = primary_with_slot(0);
        insert_cross_shard_primary(&mut state, 0, 5);
        state.set_replica_migration_allowed(true);

        runtime.cluster_maybe_orphan_to_replica(&mut state);

        assert!(state.myself.is_replica());
        assert_eq!(state.myself.replicaof, REMOTE_ID);
    }

    #[test]
    fn pending_follow_finishes_primary_side_manual_failover() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        let mut state = primary_with_slot(0);
        insert_cross_shard_primary(&mut state, 0, 5);
        let promoted = NodeId::parse(REMOTE_ID.as_bytes()).unwrap();
        let pause_actions = state
            .manual_failover
            .accept_on_primary(promoted, 1_000, 5_000);
        runtime.execute_cluster_actions(&mut state, pause_actions, 1_000);
        assert_eq!(
            runtime.ctx.client_pause().is_paused(1_001),
            PauseAction::Write
        );
        state.pending_replication_switch = Some(PendingReplicationSwitch {
            node_id: REMOTE_ID.to_string(),
            mode: ReplicationSwitchMode::TryPartial,
        });

        runtime.cluster_apply_pending_replication_switch(&mut state);

        assert!(state.pending_replication_switch.is_none());
        assert!(!state.manual_failover.is_active());
        assert!(state.manual_failover.on_timeout(10_000).is_empty());
        assert_eq!(
            runtime.ctx.client_pause().is_paused(1_002),
            PauseAction::None
        );
    }
}
