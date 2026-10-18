use super::ClusterBusManager;
use crate::engine::command::context::Token;
use crate::engine::distributed::cluster::msg::{self, ClusterMsgType, GossipEntry};
use crate::engine::distributed::cluster::node::{
    ClusterNode, CLUSTER_NODE_FAIL, CLUSTER_NODE_MYSELF, CLUSTER_NODE_NOADDR, CLUSTER_NODE_PRIMARY,
};
use crate::engine::distributed::cluster::ClusterState;
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

struct PacketThenEofReader {
    packet: Option<Vec<u8>>,
}

impl Read for PacketThenEofReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self.packet.take() {
            Some(packet) => {
                buf[..packet.len()].copy_from_slice(&packet);
                Ok(packet.len())
            }
            None => Ok(0),
        }
    }
}

fn sender(node_id: &str) -> ClusterNode {
    let mut node = ClusterNode::new(
        node_id.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    node.flags &= !CLUSTER_NODE_MYSELF;
    node
}

#[test]
fn third_party_update_keeps_new_primary_slots_after_old_primary_replicates() {
    use crate::engine::distributed::cluster::msg::{
        slots_to_bytes, ClusterBusBody, ClusterBusMessage, SlotOwnershipUpdate,
    };
    use crate::engine::distributed::cluster::node::CLUSTER_NODE_REPLICA;
    use crate::engine::distributed::cluster::NodeId;

    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    state.myself.config_epoch = 1;
    state.myself.slots.fill(true);
    state.myself.numslots = crate::engine::distributed::cluster::state::CLUSTER_SLOTS;
    let local_id = state.myself.name.clone();
    let shard_id = state.myself.shard_id.clone();
    let observer_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let new_primary_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    let observer = sender(observer_id);
    state
        .nodes
        .insert(observer_id.to_string(), observer.clone());
    let mut new_primary = sender(new_primary_id);
    new_primary.flags &= !CLUSTER_NODE_PRIMARY;
    new_primary.flags |= CLUSTER_NODE_REPLICA;
    new_primary.replicaof = local_id;
    new_primary.shard_id = shard_id;
    let stale_replica_pong = ClusterBusMessage::from_node(
        ClusterMsgType::Pong,
        &new_primary,
        8,
        ClusterBusBody::Gossip(Vec::new()),
    );
    state.nodes.insert(new_primary_id.to_string(), new_primary);

    let slots = vec![true; crate::engine::distributed::cluster::state::CLUSTER_SLOTS];
    let packet = ClusterBusMessage::from_node(
        ClusterMsgType::Update,
        &observer,
        9,
        ClusterBusBody::Update(SlotOwnershipUpdate {
            config_epoch: 9,
            node_id: NodeId::parse(new_primary_id.as_bytes()).unwrap(),
            slots: slots_to_bytes(&slots),
        }),
    )
    .encode()
    .unwrap();
    let token = Token(43);
    manager.register_incoming(token, "127.0.0.1:16381".parse().unwrap());
    let mut reader = PacketReader {
        packet: Some(packet),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 100));
    state.replicate(new_primary_id.as_bytes()).unwrap();
    ClusterBusManager::add_or_update_node(&mut state, &stale_replica_pong, true);

    assert!(state.nodes[new_primary_id].slots[7]);
    assert_eq!(
        state.slot_owner_ref(7).map(|owner| owner.name.as_str()),
        Some(new_primary_id)
    );
    assert_eq!(
        state.state,
        crate::engine::distributed::cluster::state::CLUSTER_OK
    );
}

#[test]
fn newer_cached_sender_epoch_does_not_cross_local_slot_delete_barrier() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.current_epoch = 10;
    state.myself.config_epoch = 10;
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mut known_sender = sender(sender_id);
    known_sender.config_epoch = 1;
    known_sender.slots[0] = true;
    known_sender.numslots = 1;
    state.insert_node(known_sender);

    state.del_slot(0).unwrap();
    assert_eq!(state.slot_delete_epoch(0), 11);

    let mut stale_sender = sender(sender_id);
    stale_sender.config_epoch = 2;
    stale_sender.slots[0] = true;
    stale_sender.numslots = 1;
    let message = msg::ClusterBusMessage::from_node(
        ClusterMsgType::Pong,
        &stale_sender,
        2,
        msg::ClusterBusBody::Gossip(Vec::new()),
    );

    ClusterBusManager::add_or_update_node(&mut state, &message, true);

    assert!(!state.nodes[sender_id].slots[0]);
    assert_eq!(state.nodes[sender_id].numslots, 0);
}

#[test]
fn epoch_zero_sender_claim_is_accepted_without_a_delete_barrier() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    state.nodes.insert(sender_id.to_string(), sender(sender_id));

    let mut epoch_zero_sender = sender(sender_id);
    epoch_zero_sender.slots[0] = true;
    epoch_zero_sender.numslots = 1;
    let message = msg::ClusterBusMessage::from_node(
        ClusterMsgType::Pong,
        &epoch_zero_sender,
        0,
        msg::ClusterBusBody::Gossip(Vec::new()),
    );
    let token = Token(34);
    manager.register_incoming(token, "127.0.0.1:16380".parse().unwrap());
    let mut reader = PacketReader {
        packet: Some(message.encode().unwrap()),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 100));

    assert!(state.nodes[sender_id].slots[0]);
    assert_eq!(state.nodes[sender_id].numslots, 1);
}

#[test]
fn pending_reparent_skips_the_primary_epoch_collision_after_slot_reconciliation() {
    use crate::engine::distributed::cluster::action::ReplicationSwitchMode;
    use crate::engine::distributed::cluster::state::PendingReplicationSwitch;

    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.current_epoch = 9;
    state.myself.config_epoch = 9;
    state.pending_replication_switch = Some(PendingReplicationSwitch {
        node_id: sender_id.to_string(),
        mode: ReplicationSwitchMode::TryPartial,
    });

    let mut primary = sender(sender_id);
    primary.config_epoch = 9;
    let message = msg::ClusterBusMessage::from_node(
        ClusterMsgType::Pong,
        &primary,
        9,
        msg::ClusterBusBody::Gossip(Vec::new()),
    );

    ClusterBusManager::resolve_config_epoch_collisions(&mut state, &message);

    assert_eq!(state.myself.config_epoch, 9);
}

#[test]
fn newer_partial_bitmap_does_not_unassign_missing_sender_slots() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mut cached_sender = sender(sender_id);
    cached_sender.config_epoch = 1;
    cached_sender.slots[0] = true;
    cached_sender.numslots = 1;
    state.nodes.insert(sender_id.to_string(), cached_sender);

    let mut newer_sender = sender(sender_id);
    newer_sender.config_epoch = 2;
    newer_sender.slots[1] = true;
    newer_sender.numslots = 1;
    let message = msg::ClusterBusMessage::from_node(
        ClusterMsgType::Pong,
        &newer_sender,
        2,
        msg::ClusterBusBody::Gossip(Vec::new()),
    );
    let token = Token(35);
    manager.register_incoming(token, "127.0.0.1:16380".parse().unwrap());
    let mut reader = PacketReader {
        packet: Some(message.encode().unwrap()),
    };

    let generation = state.topology_generation();
    assert!(!manager.readable(&mut state, &mut reader, token.0, 100));

    assert!(state.nodes[sender_id].slots[0]);
    assert!(state.nodes[sender_id].slots[1]);
    assert_eq!(state.nodes[sender_id].config_epoch, 2);
    assert!(state.topology_generation() > generation);
}

#[test]
fn stale_primary_heartbeat_does_not_restore_a_slot_reassigned_to_a_newer_owner() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let source_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let target_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let slot = 7;
    let mut source = sender(source_id);
    source.config_epoch = 4;
    source.slots[slot] = true;
    source.numslots = 1;
    let mut target = sender(target_id);
    target.config_epoch = 5;
    target.slots[slot] = true;
    target.numslots = 1;
    let mut cached_source = source.clone();
    cached_source.slots[slot] = false;
    cached_source.numslots = 0;
    state.insert_node(cached_source);
    state.insert_node(target);
    let token = Token(33);
    manager.register_incoming(token, "127.0.0.1:16380".parse().unwrap());
    let message = msg::ClusterBusMessage::from_node(
        ClusterMsgType::Pong,
        &source,
        4,
        msg::ClusterBusBody::Gossip(Vec::new()),
    );
    let mut reader = PacketReader {
        packet: Some(message.encode().unwrap()),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 100));
    assert!(state.nodes[target_id].slots[slot]);
    assert!(
        !state.nodes[source_id].slots[slot],
        "a stale heartbeat must not recreate the source claim"
    );
}

#[test]
fn known_sender_message_advances_cluster_current_epoch() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.current_epoch = 3;
    state.myself.config_epoch = 1;
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    state.nodes.insert(sender_id.to_string(), sender(sender_id));
    let token = Token(31);
    manager.register_incoming(token, "127.0.0.1:16380".parse().unwrap());
    let mut reader = PacketReader {
        packet: Some(msg::encode_msg(
            ClusterMsgType::Pong,
            &sender(sender_id),
            27,
            &[],
        )),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 100));
    assert_eq!(state.current_epoch, 27);
}

#[test]
fn malformed_packet_before_eof_is_counted_before_link_close() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let token = Token(30);
    manager.register_incoming(token, "127.0.0.1:16380".parse().unwrap());
    let mut packet = msg::encode_msg(
        ClusterMsgType::Meet,
        &sender("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        1,
        &[],
    );
    packet[14..16].copy_from_slice(&100u16.to_be_bytes());
    let mut reader = PacketThenEofReader {
        packet: Some(packet),
    };

    assert!(manager.readable(&mut state, &mut reader, token.0, 100));
    assert_eq!(state.total_bus_messages_received(), 1);
}

#[test]
fn mismatching_pong_marks_the_expected_node_noaddr_and_fail() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let token = Token(28);
    let expected_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let actual_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    state
        .nodes
        .insert(expected_id.to_string(), sender(expected_id));
    manager.register_outgoing_for_node(
        token,
        "127.0.0.1:16380".parse().unwrap(),
        false,
        expected_id.to_string(),
    );
    manager
        .connections
        .get_mut(&token.0)
        .unwrap()
        .handshake_sent = true;
    let witness_id = "cccccccccccccccccccccccccccccccccccccccc";
    state
        .nodes
        .insert(witness_id.to_string(), sender(witness_id));
    manager.register_outgoing_for_node(
        Token(32),
        "127.0.0.1:16381".parse().unwrap(),
        false,
        witness_id.to_string(),
    );
    manager.connections.get_mut(&32).unwrap().handshake_sent = true;
    let mut reader = PacketReader {
        packet: Some(msg::encode_msg(
            ClusterMsgType::Pong,
            &sender(actual_id),
            1,
            &[],
        )),
    };

    let generation = state.topology_generation();
    assert!(manager.readable(&mut state, &mut reader, token.0, 100));
    let expected = &state.nodes[expected_id];
    assert!(expected.has_flag(CLUSTER_NODE_NOADDR | CLUSTER_NODE_FAIL));
    assert_eq!(expected.ip(), "");
    assert_eq!(expected.port(), 0);
    assert_eq!(expected.cport(), 0);
    assert!(!state.nodes.contains_key(actual_id));
    assert!(state.pending_connects.is_empty());
    assert!(state.topology_generation() > generation);
    let (fail, _) = msg::ClusterBusMessage::decode(&manager.connections[&32].write_buf).unwrap();
    assert!(matches!(
        fail.body,
        msg::ClusterBusBody::Fail { ref node_id } if node_id.as_str() == expected_id
    ));
}

#[test]
fn unknown_ping_receives_pong_without_joining_the_sender() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let token = Token(29);
    manager.register_incoming(token, "127.0.0.1:16380".parse().unwrap());
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mut reader = PacketReader {
        packet: Some(msg::encode_msg(
            ClusterMsgType::Ping,
            &sender(sender_id),
            1,
            &[],
        )),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 100));
    assert!(!state.nodes.contains_key(sender_id));
    let (response, _) =
        msg::ClusterBusMessage::decode(&manager.connections[&token.0].write_buf).unwrap();
    assert_eq!(response.msg_type, ClusterMsgType::Pong);
    assert_eq!(response.sender_id, state.myself.name);
}

#[test]
fn inbound_ping_does_not_complete_the_outbound_probe() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let token = Token(33);
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mut known_sender = sender(sender_id);
    known_sender.ping_sent = 50;
    known_sender.pong_received = 25;
    state.nodes.insert(sender_id.to_string(), known_sender);
    manager.register_incoming(token, "127.0.0.1:16380".parse().unwrap());
    manager.connections.get_mut(&token.0).unwrap().node_name = sender_id.to_string();
    let mut reader = PacketReader {
        packet: Some(msg::encode_msg(
            ClusterMsgType::Ping,
            &sender(sender_id),
            1,
            &[],
        )),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 100));

    assert_eq!(state.nodes[sender_id].ping_sent, 50);
    assert_eq!(state.nodes[sender_id].pong_received, 25);
}

#[test]
fn inbound_pong_does_not_complete_the_outbound_probe() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let token = Token(34);
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mut known_sender = sender(sender_id);
    known_sender.ping_sent = 50;
    known_sender.pong_received = 25;
    state.nodes.insert(sender_id.to_string(), known_sender);
    manager.register_incoming(token, "127.0.0.1:16380".parse().unwrap());
    manager.connections.get_mut(&token.0).unwrap().node_name = sender_id.to_string();
    let mut reader = PacketReader {
        packet: Some(msg::encode_msg(
            ClusterMsgType::Pong,
            &sender(sender_id),
            1,
            &[],
        )),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 100));

    assert_eq!(state.nodes[sender_id].ping_sent, 50);
    assert_eq!(state.nodes[sender_id].pong_received, 25);
}

#[test]
fn outbound_pong_keeps_asymmetric_repair_armed_without_an_inbound_link() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let token = Token(35);
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mut known_sender = sender(sender_id);
    known_sender.asymmetric_meet_started_at = 50;
    state.nodes.insert(sender_id.to_string(), known_sender);
    manager.register_outgoing_for_node(
        token,
        "127.0.0.1:16380".parse().unwrap(),
        true,
        sender_id.to_string(),
    );
    manager
        .connections
        .get_mut(&token.0)
        .unwrap()
        .handshake_sent = true;
    let mut reader = PacketReader {
        packet: Some(msg::encode_msg(
            ClusterMsgType::Pong,
            &sender(sender_id),
            1,
            &[],
        )),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 100));

    assert_eq!(state.nodes[sender_id].asymmetric_meet_started_at, 50);
}

#[test]
fn known_meet_reconnects_outbound_even_after_repair_timestamp_was_refreshed() {
    use crate::engine::distributed::cluster::ClusterLinkDirection;

    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    state.set_failure_report_node_timeout_ms(1_000);
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mut known_sender = sender(sender_id);
    known_sender.created_at = 1_001;
    // The reciprocal MEET scheduler refreshes this field when it retries.
    // Receiving MEET on the inbound link still proves that the peer does not
    // know us and must replace the stale outbound link immediately.
    known_sender.asymmetric_meet_started_at = 1_001;
    state.nodes.insert(sender_id.to_string(), known_sender);
    manager.register_incoming(Token(36), "127.0.0.1:26380".parse().unwrap());
    manager.register_outgoing_for_node(
        Token(37),
        "127.0.0.1:16380".parse().unwrap(),
        false,
        sender_id.to_string(),
    );
    manager.connections.get_mut(&37).unwrap().created_at = 1_001;
    let mut reader = PacketReader {
        packet: Some(msg::encode_msg(
            ClusterMsgType::Meet,
            &sender(sender_id),
            1,
            &[],
        )),
    };

    assert!(!manager.readable(&mut state, &mut reader, 36, 1_002));

    assert!(state
        .pending_link_closes
        .contains(&(sender_id.to_string(), ClusterLinkDirection::To)));
}

#[test]
fn known_meet_from_an_old_node_reconnects_outbound_without_repair_marker() {
    use crate::engine::distributed::cluster::ClusterLinkDirection;

    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    state.set_failure_report_node_timeout_ms(1_000);
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mut known_sender = sender(sender_id);
    known_sender.created_at = 1;
    state.nodes.insert(sender_id.to_string(), known_sender);
    manager.register_incoming(Token(38), "127.0.0.1:26380".parse().unwrap());
    manager.register_outgoing_for_node(
        Token(39),
        "127.0.0.1:16380".parse().unwrap(),
        false,
        sender_id.to_string(),
    );
    let mut reader = PacketReader {
        packet: Some(msg::encode_msg(
            ClusterMsgType::Meet,
            &sender(sender_id),
            1,
            &[],
        )),
    };

    assert!(!manager.readable(&mut state, &mut reader, 38, 1_002));

    assert!(state
        .pending_link_closes
        .contains(&(sender_id.to_string(), ClusterLinkDirection::To)));
}

#[test]
fn known_meet_binds_its_inbound_link_to_the_sender_identity() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    state.set_failure_report_node_timeout_ms(1_000);
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mut known_sender = sender(sender_id);
    known_sender.created_at = 1;
    state.nodes.insert(sender_id.to_string(), known_sender);
    manager.register_incoming(Token(40), "127.0.0.1:26380".parse().unwrap());
    manager.register_outgoing_for_node(
        Token(41),
        "127.0.0.1:16380".parse().unwrap(),
        false,
        sender_id.to_string(),
    );
    let mut reader = PacketReader {
        packet: Some(msg::encode_msg(
            ClusterMsgType::Meet,
            &sender(sender_id),
            1,
            &[],
        )),
    };

    assert!(!manager.readable(&mut state, &mut reader, 40, 1_002));

    assert_eq!(
        manager.connections[&40].node_name, sender_id,
        "C getNodeFromLinkAndMsg binds a known sender to its inbound link
         before processing the MEET frame"
    );
}

#[test]
fn third_party_gossip_does_not_clear_noaddr() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let target_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let sender = sender(sender_id);
    state.nodes.insert(sender_id.to_string(), sender.clone());
    let mut target = ClusterNode::new(target_id.to_string(), String::new(), 0, 0, true);
    target.flags &= !CLUSTER_NODE_MYSELF;
    target.flags |= CLUSTER_NODE_NOADDR | CLUSTER_NODE_FAIL;
    state.nodes.insert(target_id.to_string(), target);
    let gossip = GossipEntry {
        node_id: target_id.to_string(),
        flags: CLUSTER_NODE_PRIMARY,
        ping_sent: 0,
        pong_received: 0,
        ip: "127.0.0.1".to_string(),
        port: 6381,
        tls_port: 0,
        bus_port: 16381,
        config_epoch: 1,
    };
    let message = msg::ClusterBusMessage::from_node(
        ClusterMsgType::Ping,
        &sender,
        1,
        msg::ClusterBusBody::Gossip(vec![gossip]),
    );

    ClusterBusManager::add_or_update_node(&mut state, &message, true);

    let target = &state.nodes[target_id];
    assert!(target.has_flag(CLUSTER_NODE_NOADDR | CLUSTER_NODE_FAIL));
    assert_eq!(target.ip(), "");
    assert_eq!(target.cport(), 0);
}

#[test]
fn inbound_pong_does_not_clear_noaddr_and_fail() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let token = Token(31);
    let node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mut node = ClusterNode::new(node_id.to_string(), String::new(), 0, 0, true);
    node.flags &= !CLUSTER_NODE_MYSELF;
    node.flags |= CLUSTER_NODE_NOADDR | CLUSTER_NODE_FAIL;
    state.nodes.insert(node_id.to_string(), node);
    manager.register_incoming(token, "127.0.0.1:16380".parse().unwrap());
    manager.connections.get_mut(&token.0).unwrap().node_name = node_id.to_string();
    let mut reader = PacketReader {
        packet: Some(msg::encode_msg(
            ClusterMsgType::Pong,
            &sender(node_id),
            1,
            &[],
        )),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 100));
    assert!(state.nodes[node_id].has_flag(CLUSTER_NODE_NOADDR | CLUSTER_NODE_FAIL));
}

#[test]
fn publish_from_known_sender_is_queued_once() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let token = Token(27);
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let sender = sender(sender_id);
    state.nodes.insert(sender_id.to_string(), sender.clone());
    manager.register_incoming(token, "127.0.0.1:16380".parse().unwrap());
    let packet = msg::ClusterBusMessage::from_node(
        ClusterMsgType::PublishShard,
        &sender,
        1,
        msg::ClusterBusBody::Publish(msg::ClusterPublish::new(
            bytes::Bytes::from_static(b"channel"),
            bytes::Bytes::from_static(b"payload"),
            true,
        )),
    )
    .encode()
    .unwrap();
    let mut reader = PacketReader {
        packet: Some(packet),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 100));
    assert_eq!(
        manager.pending_publishes,
        vec![super::types::ReceivedClusterPublish {
            channel: bytes::Bytes::from_static(b"channel"),
            payload: bytes::Bytes::from_static(b"payload"),
            shard: true,
        }]
    );
}
