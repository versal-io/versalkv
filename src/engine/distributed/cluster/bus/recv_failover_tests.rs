use super::ClusterBusManager;
use crate::engine::command::context::Token;
use crate::engine::distributed::cluster::action::{ClusterAction, Promotion};
use crate::engine::distributed::cluster::msg::{
    self, ClusterBusBody, ClusterExtension, ClusterMessageFlags, ClusterMsgType,
};
use crate::engine::distributed::cluster::node::{
    ClusterNode, CLUSTER_NODE_FAIL, CLUSTER_NODE_MIGRATE_TO, CLUSTER_NODE_MYSELF,
    CLUSTER_NODE_MY_PRIMARY_FAIL, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
};
use crate::engine::distributed::cluster::state::failover::{
    AutomaticFailoverFacts, AutomaticFailoverPhase, FailedPrimaryFacts, ManualFailoverMode,
    ReplicaEligibility,
};
use crate::engine::distributed::cluster::{ClusterState, NodeId};
use std::io::{Error, ErrorKind, Read};

struct PacketReader {
    packet: Option<Vec<u8>>,
}

impl Read for PacketReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self.packet.take() {
            Some(packet) => {
                buf[..packet.len()].copy_from_slice(&packet);
                Ok(packet.len())
            }
            None => Err(Error::from(ErrorKind::WouldBlock)),
        }
    }
}

fn failover_packet(sender: &ClusterNode, message_type: ClusterMsgType, epoch: u64) -> Vec<u8> {
    msg::ClusterBusMessage::from_node(message_type, sender, epoch, ClusterBusBody::Empty)
        .encode()
        .unwrap()
}

fn requester(primary_id: &str, requester_id: &str) -> ClusterNode {
    let mut requester = ClusterNode::new(
        requester_id.to_string(),
        "127.0.0.1".to_string(),
        6381,
        16381,
        false,
    );
    requester.flags &= !(CLUSTER_NODE_MYSELF | CLUSTER_NODE_PRIMARY);
    requester.flags |= CLUSTER_NODE_REPLICA;
    requester.replicaof = primary_id.to_string();
    requester
}

#[test]
fn non_heartbeat_sender_updates_preserve_learned_cluster_priority() {
    for kind in [
        ClusterMsgType::FailoverAuthRequest,
        ClusterMsgType::Update,
        ClusterMsgType::MfStart,
    ] {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let peer = requester(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        );
        let mut learned = peer.clone();
        learned.cluster_replica_priority = 10;
        state.nodes.insert(peer.name.clone(), learned);
        let body = if kind == ClusterMsgType::Update {
            ClusterBusBody::Update(msg::SlotOwnershipUpdate {
                config_epoch: 1,
                node_id: NodeId::parse(peer.name.as_bytes()).unwrap(),
                slots: msg::slots_to_bytes(&peer.slots),
            })
        } else {
            ClusterBusBody::Empty
        };
        let packet = msg::ClusterBusMessage::from_node(kind, &peer, 1, body)
            .encode()
            .unwrap();
        let (message, _) = msg::ClusterBusMessage::decode(&packet).unwrap();

        ClusterBusManager::add_or_update_node(&mut state, &message, true);

        assert_eq!(
            state.nodes[&peer.name].cluster_replica_priority, 10,
            "{kind:?}"
        );
    }
}

#[test]
fn heartbeat_sender_updates_clear_missing_priority_and_apply_last_full_u32() {
    for kind in [
        ClusterMsgType::Ping,
        ClusterMsgType::Pong,
        ClusterMsgType::Meet,
    ] {
        for has_extensions in [false, true] {
            let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
            let mut peer = requester(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            );
            peer.cluster_replica_priority = 10;
            state.nodes.insert(peer.name.clone(), peer.clone());
            let mut message = msg::ClusterBusMessage::from_node(
                kind,
                &peer,
                1,
                ClusterBusBody::Gossip(Vec::new()),
            );
            message.extensions.clear();
            if has_extensions {
                message.set_message_flag(ClusterMessageFlags::EXT_DATA);
                message
                    .extensions
                    .push(ClusterExtension::HumanNodeName("peer".to_string()));
            } else {
                message.header.message_flags[0] &= !ClusterMessageFlags::EXT_DATA;
            }
            ClusterBusManager::add_or_update_node(&mut state, &message, true);
            assert_eq!(
                state.nodes[&peer.name].cluster_replica_priority, 0,
                "{kind:?}"
            );

            message.set_message_flag(ClusterMessageFlags::EXT_DATA);
            message.extensions.extend([
                ClusterExtension::ReplicaPriority(5),
                ClusterExtension::ReplicaPriority(u32::MAX),
            ]);
            ClusterBusManager::add_or_update_node(&mut state, &message, true);
            assert_eq!(
                state.nodes[&peer.name].cluster_replica_priority,
                u32::MAX,
                "{kind:?}"
            );
        }
    }
}

#[test]
fn direct_replica_headers_refresh_primary_failure_view() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let replica_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let mut primary = ClusterNode::new(
        primary_id.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    primary.flags &= !CLUSTER_NODE_MYSELF;
    state.nodes.insert(primary_id.to_string(), primary);
    let mut replica = requester(primary_id, replica_id);
    state.nodes.insert(replica_id.to_string(), replica.clone());

    replica.flags |= CLUSTER_NODE_MY_PRIMARY_FAIL;
    let failed_view = msg::ClusterBusMessage::from_node(
        ClusterMsgType::Pong,
        &replica,
        1,
        ClusterBusBody::Gossip(Vec::new()),
    );
    ClusterBusManager::add_or_update_node(&mut state, &failed_view, true);
    assert!(state.nodes[replica_id].has_flag(CLUSTER_NODE_MY_PRIMARY_FAIL));
    assert!(state.nodes[primary_id].has_flag(CLUSTER_NODE_MIGRATE_TO));

    replica.flags &= !CLUSTER_NODE_MY_PRIMARY_FAIL;
    let recovered_view = msg::ClusterBusMessage::from_node(
        ClusterMsgType::Pong,
        &replica,
        1,
        ClusterBusBody::Gossip(Vec::new()),
    );
    ClusterBusManager::add_or_update_node(&mut state, &recovered_view, true);
    assert!(!state.nodes[replica_id].has_flag(CLUSTER_NODE_MY_PRIMARY_FAIL));
}

#[test]
fn stable_replica_heartbeat_refreshes_primary_failure_view_without_slot_reconciliation() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let replica_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let mut primary = ClusterNode::new(
        primary_id.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    primary.flags &= !CLUSTER_NODE_MYSELF;
    state.nodes.insert(primary_id.to_string(), primary);

    let mut replica = requester(primary_id, replica_id);
    state.nodes.insert(replica_id.to_string(), replica.clone());
    replica.flags |= CLUSTER_NODE_MY_PRIMARY_FAIL;
    let heartbeat = msg::ClusterBusMessage::from_node(
        ClusterMsgType::Pong,
        &replica,
        1,
        ClusterBusBody::Gossip(Vec::new()),
    );

    ClusterBusManager::add_or_update_node_without_slot_bitmap(&mut state, &heartbeat, true);

    assert!(state.nodes[replica_id].has_flag(CLUSTER_NODE_MY_PRIMARY_FAIL));
}

#[test]
fn equal_primary_epoch_claim_resets_an_active_election() {
    let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
    let failed_primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let competing_primary_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    state.current_epoch = 9;
    state.myself.flags &= !CLUSTER_NODE_PRIMARY;
    state.myself.flags |= CLUSTER_NODE_REPLICA;
    state.myself.replicaof = failed_primary_id.to_string();

    let local_id = NodeId::parse(state.myself.name.as_bytes()).unwrap();
    let local = ReplicaEligibility {
        node_id: local_id,
        no_failover: false,
        cluster_link_connected: true,
        cluster_view_stale: false,
        loading: false,
        priority: 100,
        cluster_replica_priority: 0,
        replication_offset: 42,
        data_age_ms: 0,
        node_timeout_ms: 5_000,
        repl_ping_period_ms: 10_000,
        replica_validity_factor: 10,
    };
    let facts = AutomaticFailoverFacts {
        now_ms: 1_000,
        node_timeout_ms: 5_000,
        current_epoch: 9,
        voting_primaries: 3,
        failed_primary_rank: 0,
        all_replicas_primary_failed: true,
        local: Some(local.clone()),
        primary: Some(FailedPrimaryFacts {
            node_id: NodeId::parse(failed_primary_id.as_bytes()).unwrap(),
            shard_id: NodeId::parse(failed_primary_id.as_bytes()).unwrap(),
            slots: vec![false; 16_384],
            is_failed: true,
        }),
        siblings: vec![local],
        manual: false,
    };
    assert!(state.automatic_failover_on_tick(&facts).is_empty());
    assert!(matches!(
        state.automatic_failover_on_tick(&facts).as_slice(),
        [ClusterAction::BroadcastFailoverAuthRequest {
            election_epoch: 10,
            ..
        }]
    ));
    assert_eq!(
        state.automatic_failover.phase(),
        AutomaticFailoverPhase::RequestingVotes
    );

    let mut competing_primary = ClusterNode::new(
        competing_primary_id.to_string(),
        "127.0.0.1".to_string(),
        6381,
        16381,
        true,
    );
    competing_primary.flags &= !CLUSTER_NODE_MYSELF;
    competing_primary.config_epoch = 9;
    state
        .nodes
        .insert(competing_primary_id.to_string(), competing_primary.clone());
    competing_primary.config_epoch = 10;
    let claim = msg::ClusterBusMessage::from_node(
        ClusterMsgType::Pong,
        &competing_primary,
        10,
        ClusterBusBody::Gossip(Vec::new()),
    );

    ClusterBusManager::add_or_update_node(&mut state, &claim, true);

    assert_eq!(
        state.automatic_failover.phase(),
        AutomaticFailoverPhase::Idle
    );
    assert_eq!(state.automatic_failover.election_epoch(), 0);
}

#[test]
fn better_sibling_auth_request_defers_local_election_once() {
    assert_sibling_auth_request_deferral(1_384, 0, 0, true);
}

#[test]
fn equal_offset_lower_priority_sibling_auth_request_defers_local_election_once() {
    for (local_priority, requester_priority) in [(5, 0), (10, 5)] {
        assert_sibling_auth_request_deferral(750, local_priority, requester_priority, true);
    }
}

#[test]
fn equal_offset_higher_priority_sibling_auth_request_preserves_local_election_deadline() {
    assert_sibling_auth_request_deferral(750, 5, 10, false);
}

#[test]
fn higher_offset_sibling_auth_request_defers_despite_higher_priority() {
    assert_sibling_auth_request_deferral(1_384, 5, 10, true);
}

fn assert_sibling_auth_request_deferral(
    requester_offset: i64,
    local_priority: u32,
    requester_priority: u32,
    should_defer: bool,
) {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
    state.set_failure_report_node_timeout_ms(5_000);
    let primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let requester_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    state.current_epoch = 9;
    state.myself.flags &= !CLUSTER_NODE_PRIMARY;
    state.myself.flags |= CLUSTER_NODE_REPLICA;
    state.myself.replicaof = primary_id.to_string();
    state.myself.repl_offset = 750;
    state.myself.cluster_replica_priority = local_priority;

    let mut primary = ClusterNode::new(
        primary_id.to_string(),
        "127.0.0.1".to_string(),
        6379,
        16379,
        true,
    );
    primary.flags &= !CLUSTER_NODE_MYSELF;
    primary.flags |= CLUSTER_NODE_FAIL;
    primary.slots[7] = true;
    primary.numslots = 1;
    primary.config_epoch = 7;
    let primary_slots = primary.slots.clone();
    state.nodes.insert(primary_id.to_string(), primary);

    let mut requester = requester(primary_id, requester_id);
    requester.repl_offset = requester_offset;
    requester.replica_priority = 100;
    requester.cluster_replica_priority = requester_priority;
    let mut heartbeat = msg::ClusterBusMessage::from_node(
        ClusterMsgType::Pong,
        &requester,
        9,
        ClusterBusBody::Gossip(Vec::new()),
    );
    if requester_priority != 0 {
        heartbeat.set_message_flag(ClusterMessageFlags::EXT_DATA);
        heartbeat
            .extensions
            .push(ClusterExtension::ReplicaPriority(requester_priority));
    }
    let (heartbeat, _) = msg::ClusterBusMessage::decode(&heartbeat.encode().unwrap()).unwrap();
    ClusterBusManager::add_or_update_node(&mut state, &heartbeat, true);
    assert_eq!(
        state.nodes[requester_id].cluster_replica_priority,
        requester_priority
    );

    let local = ReplicaEligibility {
        node_id: NodeId::parse(state.myself.name.as_bytes()).unwrap(),
        no_failover: false,
        cluster_link_connected: true,
        cluster_view_stale: false,
        loading: false,
        priority: 100,
        cluster_replica_priority: local_priority,
        replication_offset: 750,
        data_age_ms: 0,
        node_timeout_ms: 5_000,
        repl_ping_period_ms: 10_000,
        replica_validity_factor: 10,
    };
    let facts = AutomaticFailoverFacts {
        now_ms: 1_000,
        node_timeout_ms: 5_000,
        current_epoch: 9,
        voting_primaries: 5,
        failed_primary_rank: 0,
        all_replicas_primary_failed: true,
        local: Some(local.clone()),
        primary: Some(FailedPrimaryFacts {
            node_id: NodeId::parse(primary_id.as_bytes()).unwrap(),
            shard_id: NodeId::parse(primary_id.as_bytes()).unwrap(),
            slots: primary_slots.clone(),
            is_failed: true,
        }),
        siblings: vec![
            local,
            ReplicaEligibility {
                node_id: NodeId::parse(requester_id.as_bytes()).unwrap(),
                no_failover: false,
                cluster_link_connected: true,
                cluster_view_stale: false,
                loading: false,
                priority: 100,
                cluster_replica_priority: requester_priority,
                replication_offset: requester_offset,
                data_age_ms: 0,
                node_timeout_ms: 5_000,
                repl_ping_period_ms: 10_000,
                replica_validity_factor: 10,
            },
        ],
        manual: false,
    };
    assert!(state.automatic_failover_on_tick(&facts).is_empty());
    let original_deadline = state.automatic_failover.deadline_ms();

    let token = Token(42);
    manager.register_incoming(token, "127.0.0.1:16381".parse().unwrap());
    let mut request = msg::ClusterBusMessage::from_node(
        ClusterMsgType::FailoverAuthRequest,
        &requester,
        10,
        ClusterBusBody::Empty,
    );
    request.header.config_epoch = 7;
    request.header.myslots = msg::slots_to_bytes(&primary_slots);
    let encoded = request.encode().unwrap();
    let mut first = PacketReader {
        packet: Some(encoded.clone()),
    };

    assert!(!manager.readable(&mut state, &mut first, token.0, 1_100));
    let deferred_deadline = state.automatic_failover.deadline_ms();
    assert_eq!(
        state.nodes[requester_id].cluster_replica_priority,
        requester_priority
    );
    if should_defer {
        assert!(
            deferred_deadline > original_deadline,
            "a better sibling election must restart the lower-ranked delay"
        );
        assert!(
            deferred_deadline < 2_000,
            "the normal rank delay must leave time to recover a split vote"
        );
    } else {
        assert_eq!(deferred_deadline, original_deadline);
    }

    let mut duplicate = PacketReader {
        packet: Some(encoded),
    };
    assert!(!manager.readable(&mut state, &mut duplicate, token.0, 1_101));
    assert_eq!(
        state.automatic_failover.deadline_ms(),
        deferred_deadline,
        "duplicate requests for one epoch must not extend the delay forever"
    );
}

#[test]
fn paused_ping_from_primary_advances_manual_failover_to_catchup() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
    let primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
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
    primary.repl_offset = 42;
    state.nodes.insert(primary_id.to_string(), primary.clone());
    state.myself.flags &= !CLUSTER_NODE_PRIMARY;
    state.myself.flags |= CLUSTER_NODE_REPLICA;
    state.myself.replicaof = primary_id.to_string();
    let promotion = Promotion {
        old_primary: NodeId::parse(primary_id.as_bytes()).unwrap(),
        shard_id: NodeId::parse(primary_id.as_bytes()).unwrap(),
        slots: primary.slots.clone(),
        config_epoch: 2,
    };
    state.manual_failover.start_replica(
        ManualFailoverMode::Coordinated,
        NodeId::parse(primary_id.as_bytes()).unwrap(),
        promotion,
        1_000,
        5_000,
    );

    let token = Token(41);
    manager.register_incoming(token, "127.0.0.1:16379".parse().unwrap());
    let mut message = msg::ClusterBusMessage::from_node(
        ClusterMsgType::Ping,
        &primary,
        1,
        ClusterBusBody::Gossip(Vec::new()),
    );
    message.set_message_flag(ClusterMessageFlags::PAUSED);
    let mut reader = PacketReader {
        packet: Some(message.encode().unwrap()),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 1_001));
    assert_eq!(
        state.manual_failover.on_replica_caught_up(
            crate::engine::distributed::replication::ReplicationOffset::try_from(42)
                .expect("valid test offset"),
        ),
        vec![ClusterAction::BeginManualElection]
    );
}

#[test]
fn failover_auth_request_for_live_primary_is_rejected_without_force_ack() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.add_slot(8).unwrap();
    state.state = crate::engine::distributed::cluster::state::CLUSTER_OK;
    let primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let requester_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let mut primary = ClusterNode::new(
        primary_id.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    primary.flags &= !CLUSTER_NODE_MYSELF;
    primary.slots[7] = true;
    primary.numslots = 1;
    primary.config_epoch = 7;
    let requester = requester(primary_id, requester_id);
    state.nodes.insert(primary_id.to_string(), primary);
    state
        .nodes
        .insert(requester_id.to_string(), requester.clone());
    let token = Token(34);
    manager.register_incoming(token, "127.0.0.1:16381".parse().unwrap());
    let mut message = msg::ClusterBusMessage::from_node(
        ClusterMsgType::FailoverAuthRequest,
        &requester,
        10,
        ClusterBusBody::Empty,
    );
    message.header.config_epoch = 7;
    message.header.myslots = msg::slots_to_bytes(&state.nodes[primary_id].slots);
    let mut reader = PacketReader {
        packet: Some(message.encode().unwrap()),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 100));
    assert_eq!(state.last_vote_epoch, 0);
    assert!(manager.connections[&token.0].write_buf.is_empty());
}

#[test]
fn valid_failover_auth_request_records_vote_and_replies_once() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.add_slot(8).unwrap();
    state.observe_topology_evaluation(0);
    let primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let requester_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let mut primary = ClusterNode::new(
        primary_id.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    primary.flags &= !CLUSTER_NODE_MYSELF;
    primary.flags |= CLUSTER_NODE_FAIL;
    primary.slots[7] = true;
    primary.numslots = 1;
    primary.config_epoch = 7;
    let requester = requester(primary_id, requester_id);
    state.nodes.insert(primary_id.to_string(), primary);
    state
        .nodes
        .insert(requester_id.to_string(), requester.clone());
    let token = Token(39);
    manager.register_incoming(token, "127.0.0.1:16381".parse().unwrap());
    let mut message = msg::ClusterBusMessage::from_node(
        ClusterMsgType::FailoverAuthRequest,
        &requester,
        10,
        ClusterBusBody::Empty,
    );
    message.header.config_epoch = 7;
    message.header.myslots = msg::slots_to_bytes(&state.nodes[primary_id].slots);
    let mut reader = PacketReader {
        packet: Some(message.encode().unwrap()),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 3_000));
    assert_eq!(state.last_vote_epoch, 10);
    assert!(state.take_config_save_request());
    let (response, _) =
        msg::ClusterBusMessage::decode(&manager.connections[&token.0].write_buf).unwrap();
    assert_eq!(response.msg_type, ClusterMsgType::FailoverAuthAck);
    assert_eq!(response.current_epoch, 10);
}

#[test]
fn failover_auth_reply_uses_both_identified_link_directions() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.add_slot(8).unwrap();
    state.observe_topology_evaluation(0);
    let primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let requester_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let mut primary = ClusterNode::new(
        primary_id.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    primary.flags &= !CLUSTER_NODE_MYSELF;
    primary.flags |= CLUSTER_NODE_FAIL;
    primary.slots[7] = true;
    primary.numslots = 1;
    primary.config_epoch = 7;
    let requester = requester(primary_id, requester_id);
    state.nodes.insert(primary_id.to_string(), primary);
    state
        .nodes
        .insert(requester_id.to_string(), requester.clone());
    let incoming = Token(39);
    let outgoing = Token(40);
    manager.register_incoming(incoming, "127.0.0.1:16381".parse().unwrap());
    manager.register_outgoing_for_node(
        outgoing,
        "127.0.0.1:16381".parse().unwrap(),
        false,
        requester_id.to_string(),
    );
    manager
        .connections
        .get_mut(&outgoing.0)
        .unwrap()
        .handshake_sent = true;
    let mut message = msg::ClusterBusMessage::from_node(
        ClusterMsgType::FailoverAuthRequest,
        &requester,
        10,
        ClusterBusBody::Empty,
    );
    message.header.config_epoch = 7;
    message.header.myslots = msg::slots_to_bytes(&state.nodes[primary_id].slots);
    let mut reader = PacketReader {
        packet: Some(message.encode().unwrap()),
    };

    assert!(!manager.readable(&mut state, &mut reader, incoming.0, 3_000));
    for token in [incoming, outgoing] {
        let (response, _) =
            msg::ClusterBusMessage::decode(&manager.connections[&token.0].write_buf).unwrap();
        assert_eq!(response.msg_type, ClusterMsgType::FailoverAuthAck);
        assert_eq!(response.current_epoch, 10);
    }
}

#[test]
fn forced_vote_accepts_replica_of_the_local_primary() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.add_slot(7).unwrap();
    state.observe_topology_evaluation(0);
    let primary_id = state.myself.name.clone();
    let requester_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let requester = requester(&primary_id, requester_id);
    state
        .nodes
        .insert(requester_id.to_string(), requester.clone());
    let token = Token(40);
    manager.register_incoming(token, "127.0.0.1:16381".parse().unwrap());
    let mut message = msg::ClusterBusMessage::from_node(
        ClusterMsgType::FailoverAuthRequest,
        &requester,
        10,
        ClusterBusBody::Empty,
    );
    message.header.config_epoch = state.myself.config_epoch;
    message.header.myslots = msg::slots_to_bytes(&state.myself.slots);
    message.set_message_flag(ClusterMessageFlags::FORCE_ACK);
    let mut reader = PacketReader {
        packet: Some(message.encode().unwrap()),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 3_000));
    assert_eq!(state.last_vote_epoch, 10);
    let (response, _) =
        msg::ClusterBusMessage::decode(&manager.connections[&token.0].write_buf).unwrap();
    assert_eq!(response.msg_type, ClusterMsgType::FailoverAuthAck);
}

#[test]
fn stale_failover_request_receives_update_for_newer_slot_owner() {
    use crate::engine::distributed::cluster::msg::bytes_to_slots;

    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.myself.config_epoch = 9;
    state.myself.slots[7] = true;
    state.myself.numslots = 1;
    state.state = crate::engine::distributed::cluster::state::CLUSTER_OK;
    let primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let requester_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let mut primary = ClusterNode::new(
        primary_id.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    primary.flags &= !CLUSTER_NODE_MYSELF;
    primary.flags |= CLUSTER_NODE_FAIL;
    primary.slots[7] = true;
    primary.numslots = 1;
    primary.config_epoch = 7;
    let requester = requester(primary_id, requester_id);
    state.nodes.insert(primary_id.to_string(), primary);
    state
        .nodes
        .insert(requester_id.to_string(), requester.clone());
    let token = Token(38);
    manager.register_incoming(token, "127.0.0.1:16381".parse().unwrap());
    let mut message = msg::ClusterBusMessage::from_node(
        ClusterMsgType::FailoverAuthRequest,
        &requester,
        10,
        ClusterBusBody::Empty,
    );
    message.header.config_epoch = 7;
    message.header.myslots = msg::slots_to_bytes(&state.myself.slots);
    let mut reader = PacketReader {
        packet: Some(message.encode().unwrap()),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 3_000));
    assert_eq!(state.last_vote_epoch, 0);
    let (response, _) =
        msg::ClusterBusMessage::decode(&manager.connections[&token.0].write_buf).unwrap();
    assert!(matches!(
        response.body,
        ClusterBusBody::Update(ref update)
            if update.node_id.as_str() == state.myself.name
                && update.config_epoch == 9
                && bytes_to_slots(&update.slots)[7]
    ));
}

#[test]
fn failover_auth_acks_are_deduplicated_by_voting_primary_identity() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6380, 16380);
    let primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let voter_a_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let voter_b_id = "cccccccccccccccccccccccccccccccccccccccc";
    let mut failed_primary = ClusterNode::new(
        primary_id.to_string(),
        "127.0.0.1".to_string(),
        6379,
        16379,
        true,
    );
    failed_primary.flags &= !CLUSTER_NODE_MYSELF;
    failed_primary.flags |= CLUSTER_NODE_FAIL;
    failed_primary.slots[7] = true;
    failed_primary.numslots = 1;
    for (id, port, slot) in [(voter_a_id, 6381, 8), (voter_b_id, 6382, 9)] {
        let mut voter = ClusterNode::new(
            id.to_string(),
            "127.0.0.1".to_string(),
            port,
            port + 10_000,
            true,
        );
        voter.flags &= !CLUSTER_NODE_MYSELF;
        voter.slots[slot] = true;
        voter.numslots = 1;
        state.nodes.insert(id.to_string(), voter);
    }
    state.nodes.insert(primary_id.to_string(), failed_primary);
    state.myself.flags &= !CLUSTER_NODE_PRIMARY;
    state.myself.flags |= CLUSTER_NODE_REPLICA;
    state.myself.replicaof = primary_id.to_string();
    let local = ReplicaEligibility {
        node_id: NodeId::parse(state.myself.name.as_bytes()).unwrap(),
        no_failover: false,
        cluster_link_connected: true,
        cluster_view_stale: false,
        loading: false,
        priority: 100,
        cluster_replica_priority: 0,
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
        voting_primaries: 3,
        failed_primary_rank: 0,
        all_replicas_primary_failed: true,
        local: Some(local.clone()),
        primary: Some(FailedPrimaryFacts {
            node_id: NodeId::parse(primary_id.as_bytes()).unwrap(),
            shard_id: NodeId::parse(primary_id.as_bytes()).unwrap(),
            slots: state.nodes[primary_id].slots.clone(),
            is_failed: true,
        }),
        siblings: vec![local],
        manual: false,
    };
    state.automatic_failover.on_tick(&facts);
    state.automatic_failover.on_tick(&facts);
    state.current_epoch = 10;

    for (index, (token_value, voter_id)) in [(35, voter_a_id), (36, voter_a_id), (37, voter_b_id)]
        .into_iter()
        .enumerate()
    {
        let token = Token(token_value);
        manager.register_incoming(token, "127.0.0.1:16390".parse().unwrap());
        manager.connections.get_mut(&token.0).unwrap().node_name = voter_id.to_string();
        let packet = failover_packet(
            state.nodes.get(voter_id).unwrap(),
            ClusterMsgType::FailoverAuthAck,
            11,
        );
        let mut reader = PacketReader {
            packet: Some(packet),
        };
        assert!(!manager.readable(&mut state, &mut reader, token.0, 1_001));
        if index == 2 {
            assert!(
                state.myself.is_replica(),
                "receive path must defer promotion to the cron action executor"
            );
        }
    }

    assert_eq!(state.automatic_failover.voter_count(), 2);
    assert_eq!(state.current_epoch, 11);
    let hints = manager.cron(
        &mut state,
        2_000,
        1_000,
        &std::collections::HashMap::new(),
        0,
    );
    assert!(matches!(
        hints.actions.as_slice(),
        [ClusterAction::PromoteSelf(_)]
    ));
    assert!(state.myself.is_replica());
}
