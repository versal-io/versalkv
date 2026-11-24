use super::*;
use crate::engine::distributed::cluster::action::Promotion;
use crate::engine::distributed::cluster::node::{
    ClusterNode, CLUSTER_NODE_MYSELF, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
};
use crate::engine::distributed::cluster::state::failover::{
    AutomaticFailoverPhase, ManualFailoverMode, ManualFailoverPhase,
};
use crate::engine::distributed::cluster::{ClusterState, NodeId};

fn offset(value: i64) -> ReplicationOffset {
    ReplicationOffset::try_from(value).unwrap()
}

fn awaiting_catchup() -> ServerWorkerState {
    let mut worker = ServerWorkerState::new(1).unwrap();
    worker.ctx.replication_mut().switch_to_replica();
    worker
        .ctx
        .config_mut()
        .server
        .cluster_replica_validity_factor = 0;
    let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
    let primary_id = NodeId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
    let mut primary = ClusterNode::new(
        primary_id.to_string(),
        "127.0.0.1".to_string(),
        6379,
        16379,
        true,
    );
    primary.flags &= !CLUSTER_NODE_MYSELF;
    primary.slots[7] = true;
    primary.numslots = 1;
    let promotion = Promotion {
        old_primary: primary_id.clone(),
        shard_id: NodeId::parse(primary.shard_id.as_bytes()).unwrap(),
        slots: primary.slots.clone(),
        config_epoch: 1,
    };
    state.nodes.insert(primary.name.clone(), primary);
    state.myself.flags &= !CLUSTER_NODE_PRIMARY;
    state.myself.flags |= CLUSTER_NODE_REPLICA;
    state.myself.replicaof = primary_id.to_string();
    state.manual_failover.start_replica(
        ManualFailoverMode::Coordinated,
        primary_id.clone(),
        promotion,
        mstime(),
        30_000,
    );
    state.observe_manual_primary_paused(&primary_id, offset(42));
    *worker.ctx.cluster_state_mut() = Some(Box::new(state));
    worker
}

#[test]
fn applied_offset_advances_manual_election_at_sleep_boundary_without_cron() {
    let mut worker = awaiting_catchup();
    worker.ctx.replication_mut().slave_repl_offset = offset(41);
    worker.advance_manual_failover_before_sleep();
    assert_eq!(
        worker
            .ctx
            .cluster_state()
            .as_ref()
            .unwrap()
            .manual_failover
            .phase(),
        ManualFailoverPhase::WaitingReplicaCatchup
    );
    worker.ctx.replication_mut().slave_repl_offset = offset(42);
    worker.advance_manual_failover_before_sleep();
    let state = worker.ctx.cluster_state().as_ref().unwrap();
    assert!(state.manual_failover.is_ready_for_election());
    assert!(state.automatic_failover.is_manual());
    assert_eq!(
        state.automatic_failover.phase(),
        AutomaticFailoverPhase::RequestingVotes
    );
    let epoch = state.current_epoch;
    worker.advance_manual_failover_before_sleep();
    assert_eq!(
        worker.ctx.cluster_state().as_ref().unwrap().current_epoch,
        epoch
    );
}

#[test]
fn loading_cannot_satisfy_a_manual_catchup_target() {
    let mut worker = awaiting_catchup();
    worker.ctx.replication_mut().slave_repl_offset = offset(42);
    worker.ctx.replication_mut().async_loading = true;
    worker.advance_manual_failover_before_sleep();
    assert_eq!(
        worker
            .ctx
            .cluster_state()
            .as_ref()
            .unwrap()
            .manual_failover
            .phase(),
        ManualFailoverPhase::WaitingReplicaCatchup
    );
    assert_eq!(
        worker
            .ctx
            .cluster_state()
            .as_ref()
            .unwrap()
            .automatic_failover
            .phase(),
        AutomaticFailoverPhase::Idle
    );
}
