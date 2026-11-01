use super::super::super::types::ClusterBusManager;
use crate::engine::command::context::Token;
use crate::engine::distributed::cluster::action::{ClusterAction, Promotion};
use crate::engine::distributed::cluster::msg::{
    ClusterBusMessage, ClusterMessageFlags, ClusterMsgType,
};
use crate::engine::distributed::cluster::node::{
    ClusterNode, CLUSTER_NODE_FAIL, CLUSTER_NODE_MYSELF, CLUSTER_NODE_NOFAILOVER,
    CLUSTER_NODE_PFAIL, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
};
use crate::engine::distributed::cluster::state::failover::{
    failover_election_delay_ms, AutomaticFailoverPhase, LocalReplicaFailoverStatus,
    ManualFailoverMode,
};
use crate::engine::distributed::cluster::{ClusterState, NodeId};

fn failed_primary(id: &str) -> ClusterNode {
    let mut primary = ClusterNode::new(id.to_string(), "127.0.0.1".into(), 6379, 16379, true);
    primary.flags &= !CLUSTER_NODE_MYSELF;
    primary.flags |= CLUSTER_NODE_FAIL;
    primary.slots[7] = true;
    primary.numslots = 1;
    primary
}

fn make_replica(state: &mut ClusterState, primary_id: &str) {
    state.myself.flags &= !CLUSTER_NODE_PRIMARY;
    state.myself.flags |= CLUSTER_NODE_REPLICA;
    state.myself.replicaof = primary_id.to_string();
    if state.nodes[primary_id].has_flag(CLUSTER_NODE_FAIL) {
        state.myself.flags |=
            crate::engine::distributed::cluster::node::CLUSTER_NODE_MY_PRIMARY_FAIL;
    }
}

#[test]
fn best_ranked_replica_with_data_starts_immediately() {
    assert_eq!(failover_election_delay_ms(1_000, 0, 10, false), 0);
}

#[test]
fn lower_ranked_replica_waits_twice_the_c_base_delay_per_rank() {
    let best_delay = failover_election_delay_ms(1_000, 0, 10, false);
    let lower_delay = failover_election_delay_ms(1_000, 1, 10, false);
    assert_eq!(lower_delay, best_delay + 66);
}

#[test]
fn newly_added_replica_waits_an_extra_half_second() {
    let synced_delay = failover_election_delay_ms(1_000, 1, 10, false);
    let new_replica_delay = failover_election_delay_ms(1_000, 1, 0, false);
    assert_eq!(new_replica_delay, synced_delay + 500);
}

#[test]
fn replica_does_not_start_election_for_pfail_only_primary() {
    let mut bus = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
    let primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mut primary = failed_primary(primary_id);
    primary.flags &= !CLUSTER_NODE_FAIL;
    primary.flags |= CLUSTER_NODE_PFAIL;
    primary.pong_received = 1;
    state.nodes.insert(primary.name.clone(), primary);
    make_replica(&mut state, primary_id);

    bus.cron(
        &mut state,
        2_000,
        1_000,
        &std::collections::HashMap::new(),
        0,
    );

    assert_eq!(
        state.automatic_failover.phase(),
        AutomaticFailoverPhase::Idle
    );
}

#[test]
fn nofailover_replica_does_not_start_automatic_election() {
    let mut bus = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
    let primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    state
        .nodes
        .insert(primary_id.to_string(), failed_primary(primary_id));
    make_replica(&mut state, primary_id);
    state.myself.flags |= CLUSTER_NODE_NOFAILOVER;

    bus.cron(
        &mut state,
        2_000,
        1_000,
        &std::collections::HashMap::new(),
        0,
    );

    assert_eq!(
        state.automatic_failover.phase(),
        AutomaticFailoverPhase::Idle
    );
}

#[test]
fn zero_offset_warning_relogs_each_second_and_resets_after_reason_changes() {
    let mut bus = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
    let primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    state
        .nodes
        .insert(primary_id.to_string(), failed_primary(primary_id));
    make_replica(&mut state, primary_id);
    let offsets = std::collections::HashMap::from([(state.myself.name.clone(), 0)]);
    let health = LocalReplicaFailoverStatus {
        no_failover_if_empty: true,
        ..LocalReplicaFailoverStatus::default()
    };
    for (now_ms, expected_last_ms) in [(1000, 1000), (1999, 1000), (2000, 2000)] {
        bus.tick_automatic_failover(&mut state, now_ms, 1000, &offsets, &health);
        assert_eq!(bus.last_zero_offset_warning_ms, Some(expected_last_ms));
        assert_eq!(
            state.automatic_failover.phase(),
            AutomaticFailoverPhase::Idle
        );
    }
    let healthy = LocalReplicaFailoverStatus::default();
    bus.tick_automatic_failover(&mut state, 2001, 1000, &offsets, &healthy);
    assert_eq!(bus.last_zero_offset_warning_ms, None);
    bus.tick_automatic_failover(&mut state, 2002, 1000, &offsets, &health);
    assert_eq!(bus.last_zero_offset_warning_ms, Some(2002));

    state.nodes.get_mut(primary_id).unwrap().flags &= !CLUSTER_NODE_FAIL;
    bus.tick_automatic_failover(&mut state, 2003, 1000, &offsets, &health);
    assert_eq!(bus.last_zero_offset_warning_ms, None);
    state.nodes.get_mut(primary_id).unwrap().flags |= CLUSTER_NODE_FAIL;
    bus.tick_automatic_failover(&mut state, 2004, 1000, &offsets, &health);
    assert_eq!(bus.last_zero_offset_warning_ms, Some(2004));
}

#[test]
fn cron_starts_election_through_automatic_state_machine() {
    let mut bus = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
    let primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let voter_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let mut primary = failed_primary(primary_id);
    primary.shard_id = "ssssssssssssssssssssssssssssssssssssssss".to_string();
    let mut voter = ClusterNode::new(voter_id.to_string(), "127.0.0.1".into(), 6381, 16381, true);
    voter.flags &= !CLUSTER_NODE_MYSELF;
    voter.slots[8] = true;
    voter.numslots = 1;
    voter.pong_received = 2_000;
    state.nodes.insert(primary_id.to_string(), primary);
    state.nodes.insert(voter_id.to_string(), voter);
    make_replica(&mut state, primary_id);
    bus.register_outgoing_for_node(
        Token(41),
        "127.0.0.1:16381".parse().unwrap(),
        false,
        voter_id.to_string(),
    );
    bus.connections.get_mut(&41).unwrap().handshake_sent = true;
    bus.last_random_ping_time = 2_000;

    let offsets = std::collections::HashMap::from([(state.myself.name.clone(), 42)]);
    bus.cron(&mut state, 2_000, 1_000, &offsets, 0);

    assert_eq!(
        state.automatic_failover.phase(),
        AutomaticFailoverPhase::RequestingVotes
    );
    assert_eq!(state.current_epoch, 1);
    let (request, _) = ClusterBusMessage::decode(&bus.connections[&41].write_buf).unwrap();
    assert_eq!(request.msg_type, ClusterMsgType::FailoverAuthRequest);
    assert_eq!(request.current_epoch, 1);
    assert!(request.has_message_flag(ClusterMessageFlags::FORCE_ACK));
}

#[test]
fn automatic_election_ignores_nonzero_runtime_priority_for_ranking() {
    let mut bus = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
    let primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let sibling_id = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";
    let voter_id = "cccccccccccccccccccccccccccccccccccccccc";
    state
        .nodes
        .insert(primary_id.to_string(), failed_primary(primary_id));
    let mut sibling = ClusterNode::new(
        sibling_id.to_string(),
        "127.0.0.1".into(),
        6381,
        16381,
        false,
    );
    sibling.flags &= !CLUSTER_NODE_MYSELF;
    sibling.replicaof = primary_id.to_string();
    sibling.replica_priority = 100;
    let mut voter = ClusterNode::new(voter_id.to_string(), "127.0.0.1".into(), 6382, 16382, true);
    voter.flags &= !CLUSTER_NODE_MYSELF;
    voter.slots[8] = true;
    voter.numslots = 1;
    state.nodes.insert(sibling_id.to_string(), sibling);
    state.nodes.insert(voter_id.to_string(), voter);
    make_replica(&mut state, primary_id);
    for (token, port, node_id) in [(43, 16382, voter_id), (44, 16381, sibling_id)] {
        bus.register_outgoing_for_node(
            Token(token),
            format!("127.0.0.1:{port}").parse().unwrap(),
            false,
            node_id.to_string(),
        );
    }
    let offsets = std::collections::HashMap::from([
        (state.myself.name.clone(), 42),
        (sibling_id.to_string(), 42),
    ]);
    let health = LocalReplicaFailoverStatus {
        priority: 200,
        ..LocalReplicaFailoverStatus::default()
    };

    bus.cron_with_replica_status(&mut state, 1_000, 1_000, &offsets, &health);

    assert_eq!(
        state.automatic_failover.phase(),
        AutomaticFailoverPhase::WaitingRankDelay
    );
    assert_eq!(state.automatic_failover.rank(), 0);
    assert!(state.automatic_failover.deadline_ms() > 1_000);
}

#[test]
fn scheduling_automatic_failover_broadcasts_status_to_sibling_replicas() {
    let mut bus = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
    let primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let sibling_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let voter_id = "cccccccccccccccccccccccccccccccccccccccc";
    state
        .nodes
        .insert(primary_id.to_string(), failed_primary(primary_id));
    let mut sibling = ClusterNode::new(
        sibling_id.to_string(),
        "127.0.0.1".into(),
        6381,
        16381,
        false,
    );
    sibling.flags &= !CLUSTER_NODE_MYSELF;
    sibling.replicaof = primary_id.to_string();
    let mut voter = ClusterNode::new(voter_id.to_string(), "127.0.0.1".into(), 6382, 16382, true);
    voter.flags &= !CLUSTER_NODE_MYSELF;
    voter.slots[8] = true;
    voter.numslots = 1;
    state.nodes.insert(sibling_id.to_string(), sibling);
    state.nodes.insert(voter_id.to_string(), voter);
    make_replica(&mut state, primary_id);
    state.myself.repl_offset = 42;
    for (token, port, node_id) in [(43, 16381, sibling_id), (44, 16382, voter_id)] {
        bus.register_outgoing_for_node(
            Token(token),
            format!("127.0.0.1:{port}").parse().unwrap(),
            false,
            node_id.to_string(),
        );
        bus.connections.get_mut(&token).unwrap().handshake_sent = true;
    }
    let offsets = std::collections::HashMap::from([
        (state.myself.name.clone(), 42),
        (sibling_id.to_string(), 41),
    ]);

    bus.tick_automatic_failover(
        &mut state,
        1_000,
        1_000,
        &offsets,
        &LocalReplicaFailoverStatus::default(),
    );

    let (message, _) = ClusterBusMessage::decode(&bus.connections[&43].write_buf).unwrap();
    assert_eq!(message.msg_type, ClusterMsgType::Pong);
    assert!(
        message.header.flags
            & crate::engine::distributed::cluster::node::CLUSTER_NODE_MY_PRIMARY_FAIL
            != 0
    );
    assert_eq!(message.header.repl_offset, state.myself.repl_offset);
    assert!(bus.connections[&44].write_buf.is_empty());
}

#[test]
fn queued_automatic_promotion_is_cancelled_when_primary_recovers() {
    let mut bus = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
    let primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mut primary = failed_primary(primary_id);
    primary.flags &= !CLUSTER_NODE_FAIL;
    state.nodes.insert(primary_id.to_string(), primary);
    make_replica(&mut state, primary_id);
    let mut slots = vec![false; 16_384];
    slots[7] = true;
    bus.pending_failover_actions
        .push(ClusterAction::PromoteSelf(Promotion {
            old_primary: NodeId::parse(primary_id.as_bytes()).unwrap(),
            shard_id: NodeId::parse(primary_id.as_bytes()).unwrap(),
            slots,
            config_epoch: 10,
        }));

    let hints = bus.cron(
        &mut state,
        2_000,
        1_000,
        &std::collections::HashMap::new(),
        0,
    );

    assert!(hints.actions.is_empty());
    assert!(state.myself.is_replica());
    assert_eq!(
        state.automatic_failover.phase(),
        AutomaticFailoverPhase::Idle
    );
}

#[test]
fn queued_automatic_promotion_is_cancelled_when_replica_becomes_ineligible() {
    let mut bus = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
    let primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let voter_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    state
        .nodes
        .insert(primary_id.to_string(), failed_primary(primary_id));
    let mut voter = ClusterNode::new(voter_id.to_string(), "127.0.0.1".into(), 6381, 16381, true);
    voter.flags &= !CLUSTER_NODE_MYSELF;
    voter.slots[8] = true;
    voter.numslots = 1;
    state.nodes.insert(voter_id.to_string(), voter);
    make_replica(&mut state, primary_id);
    state.myself.flags |= CLUSTER_NODE_NOFAILOVER;
    bus.register_outgoing_for_node(
        Token(45),
        "127.0.0.1:16381".parse().unwrap(),
        false,
        voter_id.to_string(),
    );
    let mut slots = vec![false; 16_384];
    slots[7] = true;
    bus.pending_failover_actions
        .push(ClusterAction::PromoteSelf(Promotion {
            old_primary: NodeId::parse(primary_id.as_bytes()).unwrap(),
            shard_id: NodeId::parse(primary_id.as_bytes()).unwrap(),
            slots,
            config_epoch: 10,
        }));

    let hints = bus.cron(
        &mut state,
        2_000,
        1_000,
        &std::collections::HashMap::new(),
        0,
    );

    assert!(hints.actions.is_empty());
    assert!(state.myself.is_replica());
    assert_eq!(
        state.automatic_failover.phase(),
        AutomaticFailoverPhase::Idle
    );
}

#[test]
fn quorum_approved_promotion_survives_later_voter_disconnect() {
    let mut bus = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
    let primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    state
        .nodes
        .insert(primary_id.to_string(), failed_primary(primary_id));
    make_replica(&mut state, primary_id);
    let mut slots = vec![false; 16_384];
    slots[7] = true;

    let hints = bus.execute_failover_actions(
        &mut state,
        vec![ClusterAction::PromoteSelf(Promotion {
            old_primary: NodeId::parse(primary_id.as_bytes()).unwrap(),
            shard_id: NodeId::parse(primary_id.as_bytes()).unwrap(),
            slots,
            config_epoch: 10,
        })],
        2_000,
        None,
    );

    assert!(matches!(
        hints.actions.as_slice(),
        [ClusterAction::PromoteSelf(_)]
    ));
}

#[test]
fn takeover_promotion_bypasses_election_validation_and_connected_voters() {
    let mut bus = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
    let primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mut primary = failed_primary(primary_id);
    primary.flags &= !CLUSTER_NODE_FAIL;
    state.nodes.insert(primary_id.to_string(), primary);
    make_replica(&mut state, primary_id);

    state
        .start_manual_failover(ManualFailoverMode::Takeover, 1_000, 5_000)
        .unwrap();
    let hints = bus.cron(
        &mut state,
        1_001,
        1_000,
        &std::collections::HashMap::new(),
        0,
    );

    assert!(matches!(
        hints.actions.as_slice(),
        [ClusterAction::PromoteSelf(_)]
    ));
}

#[test]
fn ready_manual_failover_bypasses_the_regular_ping_interval() {
    let mut bus = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
    let primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let voter_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let mut primary = failed_primary(primary_id);
    primary.flags &= !CLUSTER_NODE_FAIL;
    state.nodes.insert(primary_id.to_string(), primary);
    let mut voter = ClusterNode::new(voter_id.to_string(), "127.0.0.1".into(), 6381, 16381, true);
    voter.flags &= !CLUSTER_NODE_MYSELF;
    voter.slots[8] = true;
    voter.numslots = 1;
    state.nodes.insert(voter_id.to_string(), voter);
    make_replica(&mut state, primary_id);
    bus.register_outgoing_for_node(
        Token(47),
        "127.0.0.1:16381".parse().unwrap(),
        false,
        voter_id.to_string(),
    );
    bus.connections.get_mut(&47).unwrap().handshake_sent = true;
    bus.ping_interval = 300_000;
    bus.last_random_ping_time = 1_100;
    state
        .start_manual_failover(ManualFailoverMode::Force, 1_050, 5_000)
        .unwrap();
    let offsets = std::collections::HashMap::from([(state.myself.name.clone(), 14)]);

    bus.cron(&mut state, 1_100, 1_000, &offsets, 0);

    assert_eq!(
        state.automatic_failover.phase(),
        AutomaticFailoverPhase::RequestingVotes
    );
    let (request, _) = ClusterBusMessage::decode(&bus.connections[&47].write_buf).unwrap();
    assert_eq!(request.msg_type, ClusterMsgType::FailoverAuthRequest);
    assert!(request.has_message_flag(ClusterMessageFlags::FORCE_ACK));
}

#[test]
fn topology_broadcast_action_is_returned_for_runtime_execution() {
    let mut bus = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
    let peer_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mut peer = ClusterNode::new(
        peer_id.to_string(),
        "127.0.0.1".to_string(),
        6381,
        16381,
        true,
    );
    peer.flags &= !CLUSTER_NODE_MYSELF;
    state.nodes.insert(peer_id.to_string(), peer);
    bus.register_outgoing_for_node(
        Token(46),
        "127.0.0.1:16381".parse().unwrap(),
        false,
        peer_id.to_string(),
    );
    bus.connections.get_mut(&46).unwrap().handshake_sent = true;

    let actions = bus.execute_failover_actions(
        &mut state,
        vec![ClusterAction::BroadcastTopology],
        1_000,
        None,
    );

    assert_eq!(actions.actions, vec![ClusterAction::BroadcastTopology]);
    assert!(bus.connections[&46].write_buf.is_empty());
}

#[test]
fn requested_topology_broadcast_is_returned_when_random_ping_is_disabled() {
    let mut bus = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
    let peer_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mut peer = ClusterNode::new(
        peer_id.to_string(),
        "127.0.0.1".to_string(),
        6381,
        16381,
        true,
    );
    peer.flags &= !CLUSTER_NODE_MYSELF;
    state.nodes.insert(peer_id.to_string(), peer);
    bus.register_outgoing_for_node(
        Token(48),
        "127.0.0.1:16381".parse().unwrap(),
        false,
        peer_id.to_string(),
    );
    bus.connections.get_mut(&48).unwrap().handshake_sent = true;
    state.set_debug_disable_random_ping(true);
    state.request_topology_broadcast();

    let actions = bus.cron(
        &mut state,
        1_000,
        1_000,
        &std::collections::HashMap::new(),
        0,
    );

    assert_eq!(actions.actions, vec![ClusterAction::BroadcastTopology]);
    assert!(bus.connections[&48].write_buf.is_empty());
}

#[test]
fn runtime_replica_loading_and_stale_data_block_automatic_election() {
    let mut bus = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
    let primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    state
        .nodes
        .insert(primary_id.to_string(), failed_primary(primary_id));
    make_replica(&mut state, primary_id);
    let offsets = std::collections::HashMap::from([(state.myself.name.clone(), 42)]);
    let mut health = LocalReplicaFailoverStatus::default();
    bus.cron_with_replica_status(&mut state, 1_000, 1_000, &offsets, &health);
    assert_eq!(
        state.automatic_failover.phase(),
        AutomaticFailoverPhase::RequestingVotes,
        "a transient lack of voting-primary links must not cancel C-compatible election state"
    );

    let voter_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let mut voter = ClusterNode::new(voter_id.to_string(), "127.0.0.1".into(), 6381, 16381, true);
    voter.flags &= !CLUSTER_NODE_MYSELF;
    voter.slots[8] = true;
    voter.numslots = 1;
    state.nodes.insert(voter_id.to_string(), voter);
    bus.register_outgoing_for_node(
        Token(42),
        "127.0.0.1:16381".parse().unwrap(),
        false,
        voter_id.to_string(),
    );
    health.loading = true;
    bus.last_random_ping_time = 2_000;
    bus.cron_with_replica_status(&mut state, 2_000, 1_000, &offsets, &health);
    assert_eq!(
        state.automatic_failover.phase(),
        AutomaticFailoverPhase::Idle
    );

    health.loading = false;
    health.data_age_ms = 16_001;
    health.repl_ping_period_ms = 1_000;
    health.replica_validity_factor = 2;
    bus.last_random_ping_time = 3_000;
    bus.cron_with_replica_status(&mut state, 3_000, 5_000, &offsets, &health);
    assert_eq!(
        state.automatic_failover.phase(),
        AutomaticFailoverPhase::Idle
    );
}
