use super::ClusterBusManager;
use crate::engine::command::context::Token;
use crate::engine::distributed::cluster::msg::{self, ClusterMsgType, GossipEntry};
use crate::engine::distributed::cluster::node::{
    ClusterNode, CLUSTER_NODE_FAIL, CLUSTER_NODE_HANDSHAKE, CLUSTER_NODE_MYSELF,
    CLUSTER_NODE_PFAIL, CLUSTER_NODE_PRIMARY,
};
use crate::engine::distributed::cluster::ClusterState;
use std::io::{Error, ErrorKind, Read};

use super::gossip::{ObservedRole, SenderObservation};

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

fn meet_packet() -> (String, Vec<u8>) {
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
    let sender = ClusterNode::new(
        sender_id.clone(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    (
        sender_id,
        msg::encode_msg(ClusterMsgType::Meet, &sender, 1, &[]),
    )
}

#[test]
fn sender_observation_normalizes_fallback_ip_and_replica_role() {
    use crate::engine::distributed::cluster::node::{CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA};

    let mut sender = ClusterNode::new(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        String::new(),
        6380,
        16_380,
        false,
    );
    sender.flags &= !(CLUSTER_NODE_MYSELF | CLUSTER_NODE_PRIMARY);
    sender.flags |= CLUSTER_NODE_REPLICA;
    sender.replicaof = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string();
    let message = msg::ClusterBusMessage::from_node(
        ClusterMsgType::Pong,
        &sender,
        9,
        msg::ClusterBusBody::Gossip(Vec::new()),
    );

    let observation = SenderObservation::normalize(&message).unwrap();

    assert_eq!(observation.sender_ip, "127.0.0.1");
    assert_eq!(observation.role, ObservedRole::Replica);
    assert_eq!(observation.replicaof, sender.replicaof);
}

#[test]
fn filtered_packet_is_not_applied_to_cluster_topology() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let token = Token(7);
    manager.register_incoming(token, "127.0.0.1:16380".parse().unwrap());
    state.set_debug_packet_filter(2);
    let (sender_id, packet) = meet_packet();
    let mut reader = PacketReader {
        packet: Some(packet),
    };

    let should_close = manager.readable(&mut state, &mut reader, token.0, 100);

    assert!(!should_close);
    assert!(!state.nodes.contains_key(&sender_id));
}

#[test]
fn malformed_packet_requests_close_without_mutating_topology() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let token = Token(22);
    manager.register_incoming(token, "127.0.0.1:16380".parse().unwrap());
    let (_, mut packet) = meet_packet();
    packet[40] = b'A';
    let before_node_count = state.nodes.len();
    let before_epoch = state.current_epoch;
    let mut reader = PacketReader {
        packet: Some(packet),
    };

    assert!(manager.readable(&mut state, &mut reader, token.0, 100));
    assert_eq!(state.nodes.len(), before_node_count);
    assert_eq!(state.current_epoch, before_epoch);
    assert_eq!(state.total_bus_messages_received(), 1);
}

#[test]
fn pong_extensions_update_sender_metadata_after_full_decode() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let token = Token(23);
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
    let mut sender = ClusterNode::new(
        sender_id.clone(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    sender.flags &= !CLUSTER_NODE_MYSELF;
    sender.endpoints.set_hostname(b"cache.example.com").unwrap();
    sender.endpoints.set_human_name(b"cache-east-1").unwrap();
    sender.endpoints.set_client_ipv4(b"192.0.2.8").unwrap();
    sender.endpoints.set_client_ipv6(b"2001:db8::8").unwrap();
    sender.endpoints.set_client_port(7000);
    sender.endpoints.set_client_tls_port(7443);
    sender.endpoints.set_availability_zone(b"zone-a").unwrap();
    state.nodes.insert(
        sender_id.clone(),
        ClusterNode::new(sender_id.clone(), "127.0.0.1".into(), 6380, 16380, true),
    );
    manager.register_incoming(token, "127.0.0.1:16380".parse().unwrap());
    let packet = msg::ClusterBusMessage::from_node(
        ClusterMsgType::Pong,
        &sender,
        1,
        msg::ClusterBusBody::Gossip(Vec::new()),
    )
    .encode()
    .unwrap();
    let mut reader = PacketReader {
        packet: Some(packet),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 100));
    let endpoints = &state.nodes[&sender_id].endpoints;
    assert_eq!(endpoints.hostname(), Some("cache.example.com"));
    assert_eq!(endpoints.human_name(), Some("cache-east-1"));
    assert_eq!(endpoints.client_ipv4(), Some("192.0.2.8"));
    assert_eq!(endpoints.client_ipv6(), Some("2001:db8::8"));
    assert_eq!(endpoints.client_port(), 7000);
    assert_eq!(endpoints.client_tls_port(), 7443);
    assert_eq!(endpoints.availability_zone(), Some("zone-a"));
}

#[test]
fn replica_pong_does_not_assign_primary_slots_to_the_replica() {
    use crate::engine::distributed::cluster::node::{CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA};

    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let token = Token(26);
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
    primary.config_epoch = 5;
    primary.slots[7] = true;
    primary.numslots = 1;
    state.nodes.insert(primary_id.to_string(), primary);
    let mut replica = ClusterNode::new(
        replica_id.to_string(),
        "127.0.0.1".to_string(),
        6381,
        16381,
        false,
    );
    replica.flags &= !(CLUSTER_NODE_MYSELF | CLUSTER_NODE_PRIMARY);
    replica.flags |= CLUSTER_NODE_REPLICA;
    replica.replicaof = primary_id.to_string();
    state.nodes.insert(replica_id.to_string(), replica.clone());
    let mut message = msg::ClusterBusMessage::from_node(
        ClusterMsgType::Pong,
        &replica,
        1,
        msg::ClusterBusBody::Gossip(Vec::new()),
    );
    message.header.config_epoch = 5;
    message.header.myslots = msg::slots_to_bytes(&state.nodes[primary_id].slots);
    manager.register_incoming(token, "127.0.0.1:16381".parse().unwrap());
    let mut reader = PacketReader {
        packet: Some(message.encode().unwrap()),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 100));
    assert!(!state.nodes[replica_id].slots[7]);
    assert_eq!(state.nodes[replica_id].numslots, 0);
    assert!(state.nodes[primary_id].slots[7]);
}

#[test]
fn replica_header_metadata_never_copies_primary_slots_to_replica() {
    use crate::engine::distributed::cluster::node::{CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA};

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
    primary.config_epoch = 5;
    primary.slots[7] = true;
    primary.numslots = 1;
    state.nodes.insert(primary_id.to_string(), primary);
    let mut replica = ClusterNode::new(
        replica_id.to_string(),
        "127.0.0.1".to_string(),
        6381,
        16381,
        false,
    );
    replica.flags &= !(CLUSTER_NODE_MYSELF | CLUSTER_NODE_PRIMARY);
    replica.flags |= CLUSTER_NODE_REPLICA;
    replica.replicaof = primary_id.to_string();
    state.nodes.insert(replica_id.to_string(), replica.clone());
    let mut message = msg::ClusterBusMessage::from_node(
        ClusterMsgType::Pong,
        &replica,
        1,
        msg::ClusterBusBody::Gossip(Vec::new()),
    );
    message.header.config_epoch = 5;
    message.header.myslots = msg::slots_to_bytes(&state.nodes[primary_id].slots);

    ClusterBusManager::add_or_update_node(&mut state, &message, true);

    assert!(!state.nodes[replica_id].slots[7]);
    assert_eq!(state.nodes[replica_id].numslots, 0);
    assert_eq!(state.nodes[replica_id].config_epoch, 0);
}

#[test]
fn slotless_primary_accepts_lower_epoch_replica_announcement_during_setup() {
    use crate::engine::distributed::cluster::node::{CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA};

    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let replica_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let mut primary = ClusterNode::new(
        primary_id.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16_380,
        true,
    );
    primary.flags &= !CLUSTER_NODE_MYSELF;
    state.nodes.insert(primary_id.to_string(), primary);

    let mut replica = ClusterNode::new(
        replica_id.to_string(),
        "127.0.0.1".to_string(),
        6381,
        16_381,
        true,
    );
    replica.flags &= !CLUSTER_NODE_MYSELF;
    replica.config_epoch = 5;
    state.nodes.insert(replica_id.to_string(), replica.clone());

    replica.flags &= !CLUSTER_NODE_PRIMARY;
    replica.flags |= CLUSTER_NODE_REPLICA;
    replica.replicaof = primary_id.to_string();
    let mut message = msg::ClusterBusMessage::from_node(
        ClusterMsgType::Pong,
        &replica,
        1,
        msg::ClusterBusBody::Gossip(Vec::new()),
    );
    message.header.config_epoch = 0;

    ClusterBusManager::add_or_update_node(&mut state, &message, true);

    assert!(state.nodes[replica_id].is_replica());
    assert_eq!(state.nodes[replica_id].replicaof, primary_id);
    assert_eq!(state.nodes[replica_id].numslots, 0);
}

#[test]
fn stable_primary_refresh_skips_unchanged_slot_bitmap_rebuild() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
    let mut sender = ClusterNode::new(
        sender_id.clone(),
        "127.0.0.1".to_string(),
        6380,
        16_380,
        true,
    );
    sender.flags &= !CLUSTER_NODE_MYSELF;
    sender.config_epoch = 5;
    sender.slots[7] = true;
    sender.numslots = 1;
    state.nodes.insert(sender_id.clone(), sender.clone());

    sender.slots[8] = true;
    sender.numslots = 2;
    let message = msg::ClusterBusMessage::from_node(
        ClusterMsgType::Pong,
        &sender,
        1,
        msg::ClusterBusBody::Gossip(Vec::new()),
    );

    ClusterBusManager::add_or_update_node_without_slot_bitmap(&mut state, &message, true);

    assert!(state.nodes[&sender_id].slots[7]);
    assert!(
        !state.nodes[&sender_id].slots[8],
        "the stable-message path must not rebuild a known primary's bitmap"
    );
}

#[test]
fn update_body_changes_the_reported_owner_not_the_sender() {
    use crate::engine::distributed::cluster::msg::{
        slots_to_bytes, ClusterBusBody, ClusterBusMessage, SlotOwnershipUpdate,
    };
    use crate::engine::distributed::cluster::node::{CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA};
    use crate::engine::distributed::cluster::NodeId;

    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.myself.config_epoch = 1;
    state.add_slot(7).unwrap();
    let token = Token(24);
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let owner_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let mut sender = ClusterNode::new(
        sender_id.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    sender.flags &= !CLUSTER_NODE_MYSELF;
    state.nodes.insert(sender_id.to_string(), sender.clone());
    let mut owner = ClusterNode::new(
        owner_id.to_string(),
        "127.0.0.1".to_string(),
        6381,
        16381,
        false,
    );
    owner.flags &= !CLUSTER_NODE_MYSELF;
    owner.flags &= !CLUSTER_NODE_PRIMARY;
    owner.flags |= CLUSTER_NODE_REPLICA;
    owner.replicaof = sender_id.to_string();
    state.nodes.insert(owner_id.to_string(), owner);
    let mut slots = vec![false; crate::engine::distributed::cluster::state::CLUSTER_SLOTS];
    slots[7] = true;
    let packet = ClusterBusMessage::from_node(
        ClusterMsgType::Update,
        &sender,
        9,
        ClusterBusBody::Update(SlotOwnershipUpdate {
            config_epoch: 9,
            node_id: NodeId::parse(owner_id.as_bytes()).unwrap(),
            slots: slots_to_bytes(&slots),
        }),
    )
    .encode()
    .unwrap();
    manager.register_incoming(token, "127.0.0.1:16380".parse().unwrap());
    let mut reader = PacketReader {
        packet: Some(packet),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 100));
    let owner = &state.nodes[owner_id];
    assert!(owner.is_primary());
    assert!(owner.replicaof.is_empty());
    assert_eq!(owner.config_epoch, 9);
    assert!(owner.slots[7]);
}

#[test]
fn update_from_promoted_sender_applies_body_after_header_refreshes_epoch() {
    use crate::engine::distributed::cluster::action::ReplicationSwitchMode;
    use crate::engine::distributed::cluster::msg::{
        slots_to_bytes, ClusterBusBody, ClusterBusMessage, SlotOwnershipUpdate,
    };
    use crate::engine::distributed::cluster::node::{
        CLUSTER_NODE_MYSELF, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
    };
    use crate::engine::distributed::cluster::state::PendingReplicationSwitch;
    use crate::engine::distributed::cluster::NodeId;

    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.myself.config_epoch = 1;
    state.add_slot(7).unwrap();
    let local_id = state.myself.name.clone();
    let shard_id = state.myself.shard_id.clone();
    let token = Token(40);
    let promoted_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mut known_replica = ClusterNode::new(
        promoted_id.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        false,
    );
    known_replica.flags &= !(CLUSTER_NODE_MYSELF | CLUSTER_NODE_PRIMARY);
    known_replica.flags |= CLUSTER_NODE_REPLICA;
    known_replica.replicaof = local_id;
    known_replica.shard_id = shard_id.clone();
    state.nodes.insert(promoted_id.to_string(), known_replica);

    let mut promoted = ClusterNode::new(
        promoted_id.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    promoted.flags &= !CLUSTER_NODE_MYSELF;
    promoted.shard_id = shard_id;
    promoted.config_epoch = 9;
    promoted.slots[7] = true;
    promoted.numslots = 1;
    let packet = ClusterBusMessage::from_node(
        ClusterMsgType::Update,
        &promoted,
        9,
        ClusterBusBody::Update(SlotOwnershipUpdate {
            config_epoch: 9,
            node_id: NodeId::parse(promoted_id.as_bytes()).unwrap(),
            slots: slots_to_bytes(&promoted.slots),
        }),
    )
    .encode()
    .unwrap();
    manager.register_incoming(token, "127.0.0.1:16380".parse().unwrap());
    let mut reader = PacketReader {
        packet: Some(packet),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 100));
    assert!(!state.myself.slots[7]);
    assert_eq!(
        state.pending_replication_switch,
        Some(PendingReplicationSwitch {
            node_id: promoted_id.to_string(),
            mode: ReplicationSwitchMode::TryPartial,
        })
    );
}

#[test]
fn update_from_unknown_sender_does_not_mutate_topology() {
    use crate::engine::distributed::cluster::msg::{
        slots_to_bytes, ClusterBusBody, ClusterBusMessage, SlotOwnershipUpdate,
    };
    use crate::engine::distributed::cluster::NodeId;

    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.myself.config_epoch = 1;
    state.myself.slots[7] = true;
    state.myself.numslots = 1;
    let token = Token(25);
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let owner_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let mut sender = ClusterNode::new(
        sender_id.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    sender.flags &= !CLUSTER_NODE_MYSELF;
    let mut owner = ClusterNode::new(
        owner_id.to_string(),
        "127.0.0.1".to_string(),
        6381,
        16381,
        true,
    );
    owner.flags &= !CLUSTER_NODE_MYSELF;
    owner.config_epoch = 2;
    state.nodes.insert(owner_id.to_string(), owner);
    let mut slots = vec![false; crate::engine::distributed::cluster::state::CLUSTER_SLOTS];
    slots[7] = true;
    let packet = ClusterBusMessage::from_node(
        ClusterMsgType::Update,
        &sender,
        9,
        ClusterBusBody::Update(SlotOwnershipUpdate {
            config_epoch: 9,
            node_id: NodeId::parse(owner_id.as_bytes()).unwrap(),
            slots: slots_to_bytes(&slots),
        }),
    )
    .encode()
    .unwrap();
    manager.register_incoming(token, "127.0.0.1:16380".parse().unwrap());
    let mut reader = PacketReader {
        packet: Some(packet),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 100));
    assert!(!state.nodes.contains_key(sender_id));
    assert_eq!(state.nodes[owner_id].config_epoch, 2);
    assert!(!state.nodes[owner_id].slots[7]);
    assert!(state.myself.slots[7]);
    assert!(manager.connections[&token.0].node_name.is_empty());
}

#[test]
fn filtered_packet_requests_link_close_when_debug_option_is_enabled() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let token = Token(8);
    manager.register_outgoing_for_node(
        token,
        "127.0.0.1:16380".parse().unwrap(),
        true,
        String::new(),
    );
    state.set_debug_packet_filter(-2);
    state.set_debug_close_link_on_packet_drop(true);
    let (_, packet) = meet_packet();
    let mut reader = PacketReader {
        packet: Some(packet),
    };

    assert!(manager.readable(&mut state, &mut reader, token.0, 100));
    assert_eq!(
        state.pending_connects,
        vec![("127.0.0.1".to_string(), 16380, true)]
    );
}

#[test]
fn closing_unfinished_outgoing_meet_schedules_retry() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let token = Token(9);
    manager.register_outgoing_for_node(
        token,
        "127.0.0.1:16380".parse().unwrap(),
        true,
        String::new(),
    );

    manager.remove_and_schedule_retry(&mut state, token.0);

    assert_eq!(
        state.pending_connects,
        vec![("127.0.0.1".to_string(), 16380, true)]
    );
}

#[test]
fn dropped_handshake_meet_marks_retry_reason_on_handshake_node() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let token = Token(18);
    let handshake_id = "handshake-node".to_string();
    let mut handshake = ClusterNode::new(
        handshake_id.clone(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    handshake.flags &= !crate::engine::distributed::cluster::node::CLUSTER_NODE_MYSELF;
    handshake.flags |= CLUSTER_NODE_HANDSHAKE;
    state.nodes.insert(handshake_id.clone(), handshake);
    manager.register_outgoing_for_handshake(
        token,
        "127.0.0.1:16380".parse().unwrap(),
        handshake_id.clone(),
    );

    state.set_debug_packet_filter(-2);
    state.set_debug_close_link_on_packet_drop(true);
    let (_, packet) = meet_packet();
    let mut reader = PacketReader {
        packet: Some(packet),
    };
    assert!(manager.readable(&mut state, &mut reader, token.0, 100));

    assert!(state.nodes[&handshake_id].handshake_retry_on_timeout);
}

#[test]
fn unknown_meet_creates_temporary_handshake_node() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let token = Token(10);
    manager.register_incoming(token, "127.0.0.1:16380".parse().unwrap());
    let (sender_id, packet) = meet_packet();
    let mut reader = PacketReader {
        packet: Some(packet),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 100));

    assert!(!state.nodes.contains_key(&sender_id));
    let handshake_id = manager.connections[&token.0].handshake_node_name.clone();
    assert_ne!(handshake_id, sender_id);
    assert!(state.nodes[&handshake_id].has_flag(CLUSTER_NODE_HANDSHAKE));
}

#[test]
fn unknown_meet_recreates_an_expired_handshake_for_the_same_endpoint() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let token = Token(16);
    manager.register_incoming(token, "127.0.0.1:16380".parse().unwrap());
    let expired_id = state.ensure_handshake_node("127.0.0.1", 6380, 16380, 100);
    assert_eq!(state.expire_handshake_nodes(1_101, 1_000), vec![expired_id]);

    let (sender_id, packet) = meet_packet();
    let mut reader = PacketReader {
        packet: Some(packet),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 1_102));

    let handshake_id = manager.connections[&token.0].handshake_node_name.clone();
    assert_ne!(handshake_id, sender_id);
    assert!(state.nodes[&handshake_id].has_flag(CLUSTER_NODE_HANDSHAKE));
}

#[test]
fn unknown_meet_registers_sender_slots_for_cluster_state() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let token = Token(17);
    manager.register_incoming(token, "127.0.0.1:16380".parse().unwrap());
    let (sender_id, _) = meet_packet();
    let sender = ClusterNode::new(
        sender_id.clone(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    let packet = msg::encode_msg(ClusterMsgType::Meet, &sender, 1, &[]);
    let mut reader = PacketReader {
        packet: Some(packet),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 100));

    assert!(!state.nodes.contains_key(&sender_id));
    let handshake_id = manager.connections[&token.0].handshake_node_name.clone();
    assert!(state.nodes[&handshake_id].has_flag(CLUSTER_NODE_HANDSHAKE));
}

#[test]
fn known_meet_reconciles_the_sender_slot_bitmap() {
    // A node can be learned through gossip before its reciprocal MEET arrives.
    // MEET carries the same topology bitmap as PING/PONG, so the direct
    // announcement must enter the epoch-aware claim path rather than merely
    // refreshing advertised_slots. Otherwise a custom bus-port cluster may
    // know all peers while one peer's assigned slot range remains absent.
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let token = Token(19);
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
    let mut cached_sender = ClusterNode::new(
        sender_id.clone(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    cached_sender.flags &= !CLUSTER_NODE_MYSELF;
    state.nodes.insert(sender_id.clone(), cached_sender);
    manager.register_incoming(token, "127.0.0.1:16380".parse().unwrap());

    let slot = 7;
    let mut sender = ClusterNode::new(
        sender_id.clone(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    sender.flags &= !CLUSTER_NODE_MYSELF;
    sender.config_epoch = 1;
    sender.slots[slot] = true;
    sender.numslots = 1;
    let mut reader = PacketReader {
        packet: Some(msg::encode_msg(ClusterMsgType::Meet, &sender, 1, &[])),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 100));
    assert!(state.nodes[&sender_id].slots[slot]);
    assert_eq!(state.nodes[&sender_id].numslots, 1);
}

#[test]
fn known_meet_removes_an_endpoint_matched_handshake_placeholder() {
    // Gossip can reveal the sender's real ID while a concurrent outbound
    // handshake still owns a random-ID placeholder for the same endpoint.
    // Its reciprocal MEET must converge those two records; otherwise the
    // placeholder leaks into CLUSTER NODES as a phantom member.
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let token = Token(29);
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
    let mut cached_sender = ClusterNode::new(
        sender_id.clone(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    cached_sender.flags &= !CLUSTER_NODE_MYSELF;
    state.nodes.insert(sender_id.clone(), cached_sender);

    let handshake_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string();
    let mut handshake = ClusterNode::new(
        handshake_id.clone(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    handshake.flags &= !CLUSTER_NODE_MYSELF;
    handshake.flags |= CLUSTER_NODE_HANDSHAKE;
    state.nodes.insert(handshake_id.clone(), handshake);
    manager.register_incoming(token, "127.0.0.1:16380".parse().unwrap());
    manager.register_outgoing_for_node(
        Token(30),
        "127.0.0.1:16380".parse().unwrap(),
        false,
        handshake_id.clone(),
    );

    let slot = 7;
    let mut sender = ClusterNode::new(
        sender_id.clone(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    sender.flags &= !CLUSTER_NODE_MYSELF;
    sender.config_epoch = 1;
    sender.slots[slot] = true;
    sender.numslots = 1;
    let mut reader = PacketReader {
        packet: Some(msg::encode_msg(ClusterMsgType::Meet, &sender, 1, &[])),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 100));
    assert!(state.nodes.contains_key(&sender_id));
    assert!(state.nodes[&sender_id].slots[slot]);
    assert!(!state.nodes.contains_key(&handshake_id));
    assert!(state.pending_link_closes.contains(&(
        handshake_id,
        crate::engine::distributed::cluster::ClusterLinkDirection::All,
    )));
}

#[test]
fn unknown_meet_gossip_arms_one_asymmetric_repair() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let token = Token(11);
    manager.register_incoming(token, "127.0.0.1:16380".parse().unwrap());
    let sender = ClusterNode::new(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    let gossip_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string();
    let gossip = GossipEntry {
        node_id: gossip_id.clone(),
        flags: CLUSTER_NODE_PRIMARY,
        ping_sent: 0,
        pong_received: 0,
        ip: "127.0.0.1".to_string(),
        port: 6381,
        tls_port: 0,
        bus_port: 16381,
        config_epoch: 0,
    };
    let mut reader = PacketReader {
        packet: Some(msg::encode_msg(ClusterMsgType::Meet, &sender, 1, &[gossip])),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 100));

    assert_eq!(state.nodes.get(&gossip_id).unwrap().pong_received, 0);
    assert_eq!(
        state
            .nodes
            .get(&gossip_id)
            .unwrap()
            .asymmetric_meet_started_at,
        100
    );
    assert_eq!(
        state.pending_connects,
        vec![("127.0.0.1".to_string(), 16381, false)]
    );
}

#[test]
fn gossip_learned_node_is_trusted_not_in_handshake() {
    // Align with C's clusterProcessGossipSection: a node learned from the
    // gossip section of a trusted sender is added with its real node id as a
    // normal (non-handshake) node, not a HANDSHAKE placeholder. Otherwise the
    // CLUSTER NODES output keeps a lingering `handshake` flag for that peer
    // (cluster-reliable-meet.tcl: "Node 0 never exited handshake state").
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let token = Token(21);
    manager.register_incoming(token, "127.0.0.1:16380".parse().unwrap());
    let sender = ClusterNode::new(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    let gossip_id = "cccccccccccccccccccccccccccccccccccccccc".to_string();
    let gossip = GossipEntry {
        node_id: gossip_id.clone(),
        flags: CLUSTER_NODE_PRIMARY,
        ping_sent: 0,
        pong_received: 0,
        ip: "127.0.0.1".to_string(),
        port: 6381,
        tls_port: 0,
        bus_port: 16381,
        config_epoch: 0,
    };
    let mut reader = PacketReader {
        packet: Some(msg::encode_msg(ClusterMsgType::Meet, &sender, 1, &[gossip])),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 100));

    let learned = state.nodes.get(&gossip_id).expect("gossip node learned");
    assert!(
        !learned.has_flag(CLUSTER_NODE_HANDSHAKE),
        "gossip-learned node must not carry the handshake flag"
    );
    // Still queued for a direct connection so the link is established via cron.
    assert!(state
        .pending_connects
        .contains(&("127.0.0.1".to_string(), 16381, false)));
}

#[test]
fn gossip_learned_node_keeps_reported_failure_flags() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let token = Token(31);
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
    let mut sender = ClusterNode::new(
        sender_id.clone(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    sender.flags &= !CLUSTER_NODE_MYSELF;
    state.nodes.insert(sender_id.clone(), sender.clone());
    manager.register_incoming(token, "127.0.0.1:16380".parse().unwrap());
    let failed_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string();
    let gossip = GossipEntry {
        node_id: failed_id.clone(),
        flags: CLUSTER_NODE_PRIMARY | CLUSTER_NODE_FAIL,
        ping_sent: 0,
        pong_received: 0,
        ip: "127.0.0.1".to_string(),
        port: 6381,
        tls_port: 0,
        bus_port: 16381,
        config_epoch: 0,
    };
    let mut reader = PacketReader {
        packet: Some(msg::encode_msg(ClusterMsgType::Pong, &sender, 1, &[gossip])),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 100));
    assert!(state.nodes[&failed_id].has_flag(CLUSTER_NODE_FAIL));
}

#[test]
fn gossip_via_ping_pong_learns_trusted_node_not_handshake() {
    // Align with C's clusterProcessGossipSection (cluster_legacy.c:2937): a
    // node learned from the gossip section of a trusted, already-known sender
    // is added with its real node id as a *normal* node — never a HANDSHAKE
    // placeholder. The PING/PONG/known-MEET path funnels gossip through
    // `add_or_update_node` (not `process_gossip_entries`), so it must apply the
    // same trusted semantics. Otherwise the learned peer keeps a lingering
    // `handshake` flag that `expire_handshake_nodes` then reaps, matching the
    // failure in cluster-reliable-meet.tcl ("Node 0 never exited handshake
    // state").
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let token = Token(20);
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
    let mut sender = ClusterNode::new(
        sender_id.clone(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    sender.flags &= !crate::engine::distributed::cluster::node::CLUSTER_NODE_MYSELF;
    // The sender is a well-known, trusted node already in our view.
    state.nodes.insert(sender_id.clone(), sender.clone());
    // Incoming (non-outgoing) link: the explicit-MEET clearing branch in
    // handle_pong_received does not fire, so the learned node keeps whatever
    // `add_or_update_node` set for it.
    manager.register_incoming(token, "127.0.0.1:16380".parse().unwrap());

    let gossip_id = "dddddddddddddddddddddddddddddddddddddddd".to_string();
    let gossip = GossipEntry {
        node_id: gossip_id.clone(),
        flags: CLUSTER_NODE_PRIMARY,
        ping_sent: 12_000,
        pong_received: 34_000,
        ip: "127.0.0.1".to_string(),
        port: 6381,
        tls_port: 0,
        bus_port: 16381,
        config_epoch: 0,
    };
    let mut reader = PacketReader {
        packet: Some(msg::encode_msg(ClusterMsgType::Pong, &sender, 1, &[gossip])),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 200));

    let learned = state.nodes.get(&gossip_id).expect("gossip node learned");
    assert!(
        !learned.has_flag(CLUSTER_NODE_HANDSHAKE),
        "gossip-learned node must be trusted, not in handshake state"
    );
    assert_eq!(
        (learned.ping_sent, learned.pong_received),
        (0, 0),
        "third-party gossip timestamps must not become this node's direct liveness state"
    );
    // Still queued for a direct connection so cron establishes the link.
    assert!(state
        .pending_connects
        .contains(&("127.0.0.1".to_string(), 16381, false)));
}

#[test]
fn ping_on_an_inbound_handshake_link_does_not_complete_temporary_identity() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let token = Token(12);
    manager.register_incoming(token, "127.0.0.1:16380".parse().unwrap());
    let (sender_id, meet) = meet_packet();
    let mut meet_reader = PacketReader { packet: Some(meet) };
    assert!(!manager.readable(&mut state, &mut meet_reader, token.0, 100));
    let temporary_id = manager
        .connections
        .get(&token.0)
        .unwrap()
        .handshake_node_name
        .clone();
    let sender = ClusterNode::new(
        sender_id.clone(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    let mut ping_reader = PacketReader {
        packet: Some(msg::encode_msg(ClusterMsgType::Ping, &sender, 1, &[])),
    };

    assert!(!manager.readable(&mut state, &mut ping_reader, token.0, 200));

    // C only accepts the real sender ID from a handshake PING on the
    // outbound side. An inbound PING merely earns a PONG: accepting it here
    // lets the peer survive a one-sided handshake timeout even though it
    // never received our PONG.
    assert!(!state.nodes.contains_key(&sender_id));
    assert_eq!(
        manager.connections[&token.0].handshake_node_name,
        temporary_id
    );
    assert!(state.nodes[&temporary_id].has_flag(CLUSTER_NODE_HANDSHAKE));
}

#[test]
fn ping_on_a_temporary_inbound_link_stays_anonymous_when_gossip_knows_the_sender() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let token = Token(18);
    manager.register_incoming(token, "127.0.0.1:16380".parse().unwrap());
    let (sender_id, meet) = meet_packet();
    let mut meet_reader = PacketReader { packet: Some(meet) };
    assert!(!manager.readable(&mut state, &mut meet_reader, token.0, 100));
    let temporary_id = manager.connections[&token.0].handshake_node_name.clone();

    let mut known_sender = ClusterNode::new(
        sender_id.clone(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    known_sender.flags &= !CLUSTER_NODE_MYSELF;
    state.nodes.insert(sender_id.clone(), known_sender.clone());
    let mut ping_reader = PacketReader {
        packet: Some(msg::encode_msg(ClusterMsgType::Ping, &known_sender, 1, &[])),
    };

    assert!(!manager.readable(&mut state, &mut ping_reader, token.0, 200));

    assert_eq!(
        manager.connections[&token.0].handshake_node_name,
        temporary_id
    );
    assert!(state.nodes[&temporary_id].has_flag(CLUSTER_NODE_HANDSHAKE));
}

#[test]
fn pong_on_a_temporary_inbound_link_stays_anonymous_when_gossip_knows_the_sender() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let token = Token(20);
    manager.register_incoming(token, "127.0.0.1:16380".parse().unwrap());
    let (sender_id, meet) = meet_packet();
    let mut meet_reader = PacketReader { packet: Some(meet) };
    assert!(!manager.readable(&mut state, &mut meet_reader, token.0, 100));
    let temporary_id = manager.connections[&token.0].handshake_node_name.clone();

    let mut known_sender = ClusterNode::new(
        sender_id.clone(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    known_sender.flags &= !CLUSTER_NODE_MYSELF;
    state.nodes.insert(sender_id.clone(), known_sender.clone());
    let mut pong_reader = PacketReader {
        packet: Some(msg::encode_msg(ClusterMsgType::Pong, &known_sender, 1, &[])),
    };

    assert!(!manager.readable(&mut state, &mut pong_reader, token.0, 200));

    assert_eq!(
        manager.connections[&token.0].handshake_node_name,
        temporary_id
    );
    assert!(state.nodes[&temporary_id].has_flag(CLUSTER_NODE_HANDSHAKE));
}

#[test]
fn ping_on_a_second_inbound_link_completes_endpoint_matched_handshake() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let meet_token = Token(12);
    let ping_token = Token(13);
    manager.register_incoming(meet_token, "127.0.0.1:16380".parse().unwrap());
    manager.register_incoming(ping_token, "127.0.0.1:16381".parse().unwrap());
    let (sender_id, meet) = meet_packet();
    let mut meet_reader = PacketReader { packet: Some(meet) };
    assert!(!manager.readable(&mut state, &mut meet_reader, meet_token.0, 100));
    let temporary_id = manager.connections[&meet_token.0]
        .handshake_node_name
        .clone();
    let sender = ClusterNode::new(
        sender_id.clone(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    let mut ping_reader = PacketReader {
        packet: Some(msg::encode_msg(ClusterMsgType::Ping, &sender, 1, &[])),
    };

    assert!(!manager.readable(&mut state, &mut ping_reader, ping_token.0, 200));

    assert!(state.nodes.contains_key(&sender_id));
    assert!(!state.nodes.contains_key(&temporary_id));
    assert_eq!(manager.connections[&ping_token.0].node_name, sender_id);
}

#[test]
fn meet_on_a_second_inbound_link_completes_endpoint_matched_handshake() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let first_token = Token(12);
    let second_token = Token(13);
    manager.register_incoming(first_token, "127.0.0.1:16380".parse().unwrap());
    manager.register_incoming(second_token, "127.0.0.1:16381".parse().unwrap());
    let (sender_id, first_meet) = meet_packet();
    let mut first_reader = PacketReader {
        packet: Some(first_meet),
    };
    assert!(!manager.readable(&mut state, &mut first_reader, first_token.0, 100));
    let temporary_id = manager.connections[&first_token.0]
        .handshake_node_name
        .clone();
    let sender = ClusterNode::new(
        sender_id.clone(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    let mut second_reader = PacketReader {
        packet: Some(msg::encode_msg(ClusterMsgType::Meet, &sender, 1, &[])),
    };

    assert!(!manager.readable(&mut state, &mut second_reader, second_token.0, 200));

    assert!(state.nodes.contains_key(&sender_id));
    assert!(!state.nodes.contains_key(&temporary_id));
    assert_eq!(manager.connections[&second_token.0].node_name, sender_id);
}

#[test]
fn ping_after_repeated_inbound_meet_does_not_complete_temporary_identity() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let token = Token(19);
    manager.register_incoming(token, "127.0.0.1:16380".parse().unwrap());
    let (sender_id, meet) = meet_packet();
    let mut first_meet = PacketReader { packet: Some(meet) };
    assert!(!manager.readable(&mut state, &mut first_meet, token.0, 100));

    let sender = ClusterNode::new(
        sender_id.clone(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    let repeated_meet = msg::encode_msg(ClusterMsgType::Meet, &sender, 1, &[]);
    let mut second_meet = PacketReader {
        packet: Some(repeated_meet),
    };
    assert!(!manager.readable(&mut state, &mut second_meet, token.0, 150));

    let mut ping = PacketReader {
        packet: Some(msg::encode_msg(ClusterMsgType::Ping, &sender, 1, &[])),
    };
    assert!(!manager.readable(&mut state, &mut ping, token.0, 200));

    assert!(!state.nodes.contains_key(&sender_id));
    assert!(state
        .nodes
        .contains_key(&manager.connections[&token.0].handshake_node_name));
    assert_eq!(
        state
            .nodes
            .values()
            .filter(|node| node.has_flag(CLUSTER_NODE_HANDSHAKE))
            .count(),
        1
    );
}

#[test]
fn pong_completing_regular_gossip_handshake_does_not_arm_repair() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let token = Token(13);
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
    let mut sender = ClusterNode::new(
        sender_id.clone(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    sender.flags &= !crate::engine::distributed::cluster::node::CLUSTER_NODE_MYSELF;
    let mut handshake = sender.clone();
    handshake.flags |= CLUSTER_NODE_HANDSHAKE;
    handshake.created_at = 100;
    state.nodes.insert(sender_id.clone(), handshake);
    manager.register_outgoing_for_node(
        token,
        "127.0.0.1:16380".parse().unwrap(),
        false,
        sender_id.clone(),
    );
    manager
        .connections
        .get_mut(&token.0)
        .unwrap()
        .handshake_sent = true;
    let mut reader = PacketReader {
        packet: Some(msg::encode_msg(ClusterMsgType::Pong, &sender, 1, &[])),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 200));

    assert_eq!(
        state
            .nodes
            .get(&sender_id)
            .unwrap()
            .asymmetric_meet_started_at,
        0
    );
}

#[test]
fn explicit_meet_pong_learns_gossiped_node_as_trusted() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let token = Token(14);
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
    let mut sender = ClusterNode::new(
        sender_id.clone(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    sender.flags &= !crate::engine::distributed::cluster::node::CLUSTER_NODE_MYSELF;
    state.nodes.insert(sender_id.clone(), sender.clone());
    manager.register_outgoing_for_node(token, "127.0.0.1:16380".parse().unwrap(), true, sender_id);
    manager
        .connections
        .get_mut(&token.0)
        .unwrap()
        .handshake_sent = true;
    let gossip_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string();
    let gossip = GossipEntry {
        node_id: gossip_id.clone(),
        flags: CLUSTER_NODE_PRIMARY,
        ping_sent: 0,
        pong_received: 0,
        ip: "127.0.0.1".to_string(),
        port: 6381,
        tls_port: 0,
        bus_port: 16381,
        config_epoch: 0,
    };
    let mut reader = PacketReader {
        packet: Some(msg::encode_msg(ClusterMsgType::Pong, &sender, 1, &[gossip])),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 200));

    // C clusterProcessGossipSection: the gossip-learned node is trusted from
    // the start (no handshake), and the asymmetric-meet repair is armed so a
    // reciprocal MEET establishes the bidirectional link.
    let gossip_node = state.nodes.get(&gossip_id).unwrap();
    assert!(!gossip_node.has_flag(CLUSTER_NODE_HANDSHAKE));
    assert!(gossip_node.asymmetric_meet_started_at > 0);
    assert!(gossip_node.meet_pending);
    assert!(
        !manager.connections[&token.0].send_meet,
        "a successful explicit MEET must not be retried after later teardown"
    );
}

#[test]
fn pong_on_gossip_discovered_outbound_ping_keeps_reciprocal_meet_pending() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let token = Token(24);
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
    let mut sender = ClusterNode::new(
        sender_id.clone(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    sender.flags &= !crate::engine::distributed::cluster::node::CLUSTER_NODE_MYSELF;
    sender.asymmetric_meet_started_at = 100;
    sender.meet_pending = true;
    state.nodes.insert(sender_id.clone(), sender.clone());
    // A gossip-discovered peer receives an ordinary outbound PING first. Its
    // PONG only proves liveness, not that it knows this node, so the pending
    // reciprocal MEET must survive until that MEET is sent.
    manager.register_outgoing_for_node(
        token,
        "127.0.0.1:16380".parse().unwrap(),
        false,
        sender_id.clone(),
    );
    manager
        .connections
        .get_mut(&token.0)
        .unwrap()
        .handshake_sent = true;
    let mut reader = PacketReader {
        packet: Some(msg::encode_msg(ClusterMsgType::Pong, &sender, 1, &[])),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 200));

    assert!(state.nodes[&sender_id].meet_pending);
}

#[test]
fn pong_after_node_owned_meet_clears_reciprocal_meet_pending() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let token = Token(27);
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
    let mut sender = ClusterNode::new(
        sender_id.clone(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    sender.flags &= !crate::engine::distributed::cluster::node::CLUSTER_NODE_MYSELF;
    sender.asymmetric_meet_started_at = 100;
    sender.meet_pending = true;
    state.nodes.insert(sender_id.clone(), sender.clone());
    manager.register_outgoing_for_node(
        token,
        "127.0.0.1:16380".parse().unwrap(),
        false,
        sender_id.clone(),
    );
    let connection = manager.connections.get_mut(&token.0).unwrap();
    connection.handshake_sent = true;
    connection.meet_sent_at = 150;
    let mut reader = PacketReader {
        packet: Some(msg::encode_msg(ClusterMsgType::Pong, &sender, 1, &[])),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 200));

    assert!(!state.nodes[&sender_id].meet_pending);
}

#[test]
fn outgoing_temporary_meet_pong_confirms_the_peer_identity() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let token = Token(25);
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
    let mut sender = ClusterNode::new(
        sender_id.clone(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    sender.flags &= !CLUSTER_NODE_MYSELF;
    let handshake_id = state.ensure_handshake_node("127.0.0.1", 6380, 16380, 100);
    let stale_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string();
    let mut stale = ClusterNode::new(stale_id.clone(), "127.0.0.1".to_string(), 6380, 16380, true);
    stale.flags &= !CLUSTER_NODE_MYSELF;
    stale.flags |= CLUSTER_NODE_HANDSHAKE;
    state.nodes.insert(stale_id, stale);
    manager.register_outgoing_for_handshake(
        token,
        "127.0.0.1:16380".parse().unwrap(),
        handshake_id,
    );
    manager
        .connections
        .get_mut(&token.0)
        .unwrap()
        .handshake_sent = true;
    let mut reader = PacketReader {
        packet: Some(msg::encode_msg(ClusterMsgType::Pong, &sender, 1, &[])),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 200));

    assert!(state.nodes.contains_key(&sender_id));
    assert_eq!(manager.connections[&token.0].node_name, sender_id);
    assert!(manager.connections[&token.0].handshake_node_name.is_empty());
    assert!(
        !state.nodes.values().any(|node| {
            node.has_flag(CLUSTER_NODE_HANDSHAKE)
                && node.ip == "127.0.0.1"
                && node.port == 6380
                && node.cport == 16380
        }),
        "a confirmed node identity must remove every stale endpoint handshake"
    );
}

#[test]
fn pong_from_known_sender_removes_bound_handshake_with_different_endpoint() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let token = Token(26);
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
    let mut sender = ClusterNode::new(
        sender_id.clone(),
        "127.0.0.1".to_string(),
        6381,
        16381,
        true,
    );
    sender.flags &= !CLUSTER_NODE_MYSELF;
    state.nodes.insert(sender_id.clone(), sender.clone());
    let handshake_id = state.ensure_handshake_node("127.0.0.1", 6380, 16380, 100);
    manager.register_outgoing_for_handshake(
        token,
        "127.0.0.1:16380".parse().unwrap(),
        handshake_id.clone(),
    );
    manager
        .connections
        .get_mut(&token.0)
        .unwrap()
        .handshake_sent = true;
    let mut reader = PacketReader {
        packet: Some(msg::encode_msg(ClusterMsgType::Pong, &sender, 1, &[])),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 200));

    assert!(state.nodes.contains_key(&sender_id));
    assert!(!state.nodes.contains_key(&handshake_id));
    assert!(state.info_str().contains("cluster_known_nodes:2\r\n"));
}

#[test]
fn replica_explicit_meet_learns_gossiped_node_as_trusted() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    state.myself.flags &= !crate::engine::distributed::cluster::node::CLUSTER_NODE_PRIMARY;
    state.myself.flags |= crate::engine::distributed::cluster::node::CLUSTER_NODE_REPLICA;
    let token = Token(15);
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
    let mut sender = ClusterNode::new(
        sender_id.clone(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    sender.flags &= !crate::engine::distributed::cluster::node::CLUSTER_NODE_MYSELF;
    state.nodes.insert(sender_id.clone(), sender.clone());
    manager.register_outgoing_for_node(token, "127.0.0.1:16380".parse().unwrap(), true, sender_id);
    manager
        .connections
        .get_mut(&token.0)
        .unwrap()
        .handshake_sent = true;
    let gossip_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string();
    let gossip = GossipEntry {
        node_id: gossip_id.clone(),
        flags: CLUSTER_NODE_PRIMARY,
        ping_sent: 0,
        pong_received: 0,
        ip: "127.0.0.1".to_string(),
        port: 6381,
        tls_port: 0,
        bus_port: 16381,
        config_epoch: 0,
    };
    let mut reader = PacketReader {
        packet: Some(msg::encode_msg(ClusterMsgType::Pong, &sender, 1, &[gossip])),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 200));

    // Gossip learning does not depend on the local node's role — a replica
    // trusts and learns gossip-advertised nodes exactly like a primary (C
    // clusterProcessGossipSection makes no primary/replica distinction).
    let gossip_node = state.nodes.get(&gossip_id).unwrap();
    assert!(!gossip_node.has_flag(CLUSTER_NODE_HANDSHAKE));
    assert!(gossip_node.asymmetric_meet_started_at > 0);
}

#[test]
fn pong_from_failed_primary_clears_local_fail_and_pfail_flags() {
    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let token = Token(16);
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
    let mut sender = ClusterNode::new(
        sender_id.clone(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    sender.flags &= !crate::engine::distributed::cluster::node::CLUSTER_NODE_MYSELF;
    sender.flags |= CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL;
    sender.ping_sent = 50;
    sender.pong_received = 25;
    sender.slots.fill(true);
    sender.numslots = crate::engine::distributed::cluster::state::CLUSTER_SLOTS;
    state.nodes.insert(sender_id.clone(), sender.clone());
    state.recompute_state();
    assert_ne!(
        state.state,
        crate::engine::distributed::cluster::state::CLUSTER_OK
    );
    manager.register_outgoing_for_node(
        token,
        "127.0.0.1:16380".parse().unwrap(),
        false,
        sender_id.clone(),
    );
    manager
        .connections
        .get_mut(&token.0)
        .unwrap()
        .handshake_sent = true;

    let mut pong_sender = sender.clone();
    pong_sender.flags &= !(CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL);
    let mut reader = PacketReader {
        packet: Some(msg::encode_msg(ClusterMsgType::Pong, &pong_sender, 1, &[])),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 200));

    let updated = state.nodes.get(&sender_id).unwrap();
    assert_eq!(updated.pong_received, 200);
    assert_eq!(updated.ping_sent, 0);
    assert!(!updated.has_flag(CLUSTER_NODE_FAIL));
    assert!(!updated.has_flag(CLUSTER_NODE_PFAIL));
    assert_eq!(
        state.state,
        crate::engine::distributed::cluster::state::CLUSTER_OK
    );
}

#[test]
fn gossip_fail_and_recovery_update_the_sender_failure_report() {
    let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let target_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let mut sender = ClusterNode::new(
        sender_id.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16380,
        true,
    );
    sender.flags &= !CLUSTER_NODE_MYSELF;
    sender.slots[1] = true;
    sender.numslots = 1;
    let mut target = ClusterNode::new(
        target_id.to_string(),
        "127.0.0.1".to_string(),
        6381,
        16381,
        true,
    );
    target.flags &= !CLUSTER_NODE_MYSELF;
    state.nodes.insert(sender_id.to_string(), sender.clone());
    state.nodes.insert(target_id.to_string(), target);

    let gossip = |flags| GossipEntry {
        node_id: target_id.to_string(),
        flags,
        ping_sent: 0,
        pong_received: 0,
        ip: "127.0.0.1".to_string(),
        port: 6381,
        tls_port: 0,
        bus_port: 16381,
        config_epoch: 0,
    };
    let failed = msg::ClusterBusMessage::from_node(
        ClusterMsgType::Pong,
        &sender,
        1,
        msg::ClusterBusBody::Gossip(vec![gossip(CLUSTER_NODE_PRIMARY | CLUSTER_NODE_FAIL)]),
    );

    ClusterBusManager::add_or_update_node(&mut state, &failed, true);
    assert!(state
        .fail_reports
        .get(target_id)
        .is_some_and(|reports| reports.contains_key(sender_id)));

    let recovered = msg::ClusterBusMessage::from_node(
        ClusterMsgType::Pong,
        &sender,
        1,
        msg::ClusterBusBody::Gossip(vec![gossip(CLUSTER_NODE_PRIMARY)]),
    );
    ClusterBusManager::add_or_update_node(&mut state, &recovered, true);

    assert!(!state
        .fail_reports
        .get(target_id)
        .is_some_and(|reports| reports.contains_key(sender_id)));
}

#[test]
fn direct_sender_nofailover_flag_tracks_latest_header() {
    use crate::engine::distributed::cluster::node::CLUSTER_NODE_NOFAILOVER;

    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mut sender = ClusterNode::new(
        sender_id.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16_380,
        false,
    );
    sender.flags &= !CLUSTER_NODE_MYSELF;
    sender.flags |= CLUSTER_NODE_NOFAILOVER;
    let with_flag = msg::ClusterBusMessage::from_node(
        ClusterMsgType::Pong,
        &sender,
        1,
        msg::ClusterBusBody::Gossip(Vec::new()),
    );

    ClusterBusManager::add_or_update_node(&mut state, &with_flag, true);
    assert!(state.nodes[sender_id].has_flag(CLUSTER_NODE_NOFAILOVER));

    sender.flags &= !CLUSTER_NODE_NOFAILOVER;
    let without_flag = msg::ClusterBusMessage::from_node(
        ClusterMsgType::Pong,
        &sender,
        1,
        msg::ClusterBusBody::Gossip(Vec::new()),
    );
    ClusterBusManager::add_or_update_node(&mut state, &without_flag, true);

    assert!(!state.nodes[sender_id].has_flag(CLUSTER_NODE_NOFAILOVER));
}

#[test]
fn stable_heartbeat_updates_direct_sender_nofailover_flag() {
    use crate::engine::distributed::cluster::node::CLUSTER_NODE_NOFAILOVER;

    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mut sender = ClusterNode::new(
        sender_id.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16_380,
        false,
    );
    sender.flags &= !CLUSTER_NODE_MYSELF;
    state.nodes.insert(sender_id.to_string(), sender.clone());

    sender.flags |= CLUSTER_NODE_NOFAILOVER;
    let with_flag = msg::ClusterBusMessage::from_node(
        ClusterMsgType::Pong,
        &sender,
        1,
        msg::ClusterBusBody::Gossip(Vec::new()),
    );

    ClusterBusManager::add_or_update_node_without_slot_bitmap(&mut state, &with_flag, true);

    assert!(state.nodes[sender_id].has_flag(CLUSTER_NODE_NOFAILOVER));
}

#[test]
fn stable_primary_heartbeat_updates_migration_target_flag() {
    use crate::engine::distributed::cluster::node::CLUSTER_NODE_MIGRATE_TO;

    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mut sender = ClusterNode::new(
        sender_id.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16_380,
        true,
    );
    sender.flags &= !CLUSTER_NODE_MYSELF;
    state.nodes.insert(sender_id.to_string(), sender.clone());

    sender.flags |= CLUSTER_NODE_MIGRATE_TO;
    let heartbeat = msg::ClusterBusMessage::from_node(
        ClusterMsgType::Pong,
        &sender,
        1,
        msg::ClusterBusBody::Gossip(Vec::new()),
    );

    ClusterBusManager::add_or_update_node_without_slot_bitmap(&mut state, &heartbeat, true);

    assert!(state.nodes[sender_id].has_flag(CLUSTER_NODE_MIGRATE_TO));
}

#[test]
fn primary_heartbeat_clears_a_known_node_stale_replicaof() {
    use crate::engine::distributed::cluster::node::CLUSTER_NODE_REPLICA;

    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let sender_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let former_primary_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let mut known_replica = ClusterNode::new(
        sender_id.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16_380,
        false,
    );
    known_replica.flags &= !CLUSTER_NODE_MYSELF;
    known_replica.flags |= CLUSTER_NODE_REPLICA;
    known_replica.replicaof = former_primary_id.to_string();
    known_replica.config_epoch = 7;
    state.nodes.insert(sender_id.to_string(), known_replica);

    let mut promoted = ClusterNode::new(
        sender_id.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16_380,
        true,
    );
    promoted.flags &= !CLUSTER_NODE_MYSELF;
    promoted.config_epoch = 7;
    promoted.slots[42] = true;
    promoted.numslots = 1;
    let pong = msg::ClusterBusMessage::from_node(
        ClusterMsgType::Pong,
        &promoted,
        7,
        msg::ClusterBusBody::Gossip(Vec::new()),
    );

    ClusterBusManager::add_or_update_node(&mut state, &pong, true);

    let updated = &state.nodes[sender_id];
    assert!(updated.is_primary());
    assert!(updated.replicaof.is_empty());
}

#[test]
fn old_primary_replica_announcement_atomically_reparents_sub_replica() {
    use crate::engine::distributed::cluster::action::ReplicationSwitchMode;
    use crate::engine::distributed::cluster::node::{CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA};
    use crate::engine::distributed::cluster::state::PendingReplicationSwitch;

    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let old_primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let new_primary_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    state.myself.flags &= !CLUSTER_NODE_PRIMARY;
    state.myself.flags |= CLUSTER_NODE_REPLICA;
    state.myself.replicaof = old_primary_id.to_string();
    state.myself.shard_id = old_primary_id.to_string();
    state.set_replica_migration_allowed(false);

    let mut old_primary = ClusterNode::new(
        old_primary_id.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16_380,
        true,
    );
    old_primary.flags &= !CLUSTER_NODE_MYSELF;
    old_primary.shard_id = old_primary_id.to_string();
    old_primary.config_epoch = 1;
    old_primary.slots[0] = true;
    old_primary.numslots = 1;
    state
        .nodes
        .insert(old_primary_id.to_string(), old_primary.clone());

    let mut new_primary = ClusterNode::new(
        new_primary_id.to_string(),
        "127.0.0.1".to_string(),
        6381,
        16_381,
        false,
    );
    new_primary.flags &= !CLUSTER_NODE_MYSELF;
    new_primary.replicaof = old_primary_id.to_string();
    new_primary.shard_id = old_primary_id.to_string();
    state.nodes.insert(new_primary_id.to_string(), new_primary);
    // A stale heartbeat from the future primary may arrive before the old
    // primary announces its demotion. The atomic failover transition must
    // replace this withdrawal marker together with the slot binding.
    state.set_slot_claim_withdrawn(0, Some(new_primary_id.to_string()));
    state.seed_migrating_slot(1, old_primary_id);
    state.seed_importing_slot(2, old_primary_id);

    old_primary.flags &= !CLUSTER_NODE_PRIMARY;
    old_primary.flags |= CLUSTER_NODE_REPLICA;
    old_primary.replicaof = new_primary_id.to_string();
    old_primary.slots.fill(false);
    old_primary.numslots = 0;
    let stale_new_primary_gossip = GossipEntry {
        node_id: new_primary_id.to_string(),
        flags: CLUSTER_NODE_REPLICA,
        ping_sent: 0,
        pong_received: 0,
        ip: "127.0.0.1".to_string(),
        port: 6381,
        tls_port: 0,
        bus_port: 16_381,
        config_epoch: 0,
    };
    let mut message = msg::ClusterBusMessage::from_node(
        ClusterMsgType::Pong,
        &old_primary,
        9,
        msg::ClusterBusBody::Gossip(vec![stale_new_primary_gossip]),
    );
    message.header.config_epoch = 9;
    let token = Token(41);
    manager.register_incoming(token, "127.0.0.1:16380".parse().unwrap());
    let mut reader = PacketReader {
        packet: Some(message.encode().unwrap()),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 100));

    assert!(state.nodes[new_primary_id].is_primary());
    assert!(state.nodes[new_primary_id].slots[0]);
    assert_eq!(state.nodes[old_primary_id].numslots, 0);
    assert!(state.slot_claim_withdrawn_by(0).is_none());
    assert_eq!(
        state.slot_owner_ref(0).map(|node| node.name.as_str()),
        Some(new_primary_id)
    );
    assert_eq!(
        state.topology_snapshot().slot_owners[0]
            .as_ref()
            .map(crate::engine::distributed::cluster::NodeId::as_str),
        Some(new_primary_id)
    );
    assert_eq!(state.migrating_slot_target(1), Some(new_primary_id));
    assert_eq!(state.importing_slot_source(2), Some(new_primary_id));
    assert_eq!(
        state.pending_replication_switch,
        Some(PendingReplicationSwitch {
            node_id: new_primary_id.to_string(),
            mode: ReplicationSwitchMode::TryPartial,
        })
    );

    state.pending_replication_switch = None;
    let repeated = msg::ClusterBusMessage::from_node(
        ClusterMsgType::Pong,
        &old_primary,
        9,
        msg::ClusterBusBody::Gossip(Vec::new()),
    );
    let mut repeated_reader = PacketReader {
        packet: Some(repeated.encode().unwrap()),
    };
    assert!(!manager.readable(&mut state, &mut repeated_reader, token.0, 100));
    assert!(
        state.pending_replication_switch.is_none(),
        "a repeated replica announcement must not reparent an already-following sub-replica"
    );
}

#[test]
fn empty_primary_replica_announcement_does_not_reparent_child() {
    use crate::engine::distributed::cluster::node::{CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA};

    let mut manager = ClusterBusManager::new();
    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let empty_primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let target_primary_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    state.myself.flags &= !CLUSTER_NODE_PRIMARY;
    state.myself.flags |= CLUSTER_NODE_REPLICA;
    state.myself.replicaof = empty_primary_id.to_string();
    state.myself.shard_id = "shared-shard".to_string();
    state.set_replica_migration_allowed(true);

    let mut empty_primary = ClusterNode::new(
        empty_primary_id.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16_380,
        true,
    );
    empty_primary.flags &= !CLUSTER_NODE_MYSELF;
    empty_primary.shard_id = "shared-shard".to_string();
    empty_primary.config_epoch = 8;
    state
        .nodes
        .insert(empty_primary_id.to_string(), empty_primary.clone());

    let mut target_primary = ClusterNode::new(
        target_primary_id.to_string(),
        "127.0.0.1".to_string(),
        6381,
        16_381,
        true,
    );
    target_primary.flags &= !CLUSTER_NODE_MYSELF;
    target_primary.shard_id = "shared-shard".to_string();
    target_primary.config_epoch = 8;
    target_primary.slots[0] = true;
    target_primary.numslots = 1;
    state
        .nodes
        .insert(target_primary_id.to_string(), target_primary);

    empty_primary.flags &= !CLUSTER_NODE_PRIMARY;
    empty_primary.flags |= CLUSTER_NODE_REPLICA;
    empty_primary.replicaof = target_primary_id.to_string();
    let message = msg::ClusterBusMessage::from_node(
        ClusterMsgType::Pong,
        &empty_primary,
        9,
        msg::ClusterBusBody::Gossip(Vec::new()),
    );
    let token = Token(42);
    manager.register_incoming(token, "127.0.0.1:16380".parse().unwrap());
    let mut reader = PacketReader {
        packet: Some(message.encode().unwrap()),
    };

    assert!(!manager.readable(&mut state, &mut reader, token.0, 100));
    assert!(state.pending_replication_switch.is_none());
    assert_eq!(state.myself.replicaof, empty_primary_id);
}

#[test]
fn cross_shard_primary_demotion_reparents_sub_replica_with_full_sync() {
    use crate::engine::distributed::cluster::action::ReplicationSwitchMode;
    use crate::engine::distributed::cluster::node::{CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA};
    use crate::engine::distributed::cluster::state::PendingReplicationSwitch;

    let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
    let old_primary_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let new_primary_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    state.myself.flags &= !CLUSTER_NODE_PRIMARY;
    state.myself.flags |= CLUSTER_NODE_REPLICA;
    state.myself.replicaof = old_primary_id.to_string();
    state.myself.shard_id = old_primary_id.to_string();
    state.set_replica_migration_allowed(false);

    let mut old_primary = ClusterNode::new(
        old_primary_id.to_string(),
        "127.0.0.1".to_string(),
        6380,
        16_380,
        true,
    );
    old_primary.flags &= !CLUSTER_NODE_MYSELF;
    old_primary.shard_id = old_primary_id.to_string();
    old_primary.config_epoch = 4;
    state
        .nodes
        .insert(old_primary_id.to_string(), old_primary.clone());

    let mut new_primary = ClusterNode::new(
        new_primary_id.to_string(),
        "127.0.0.1".to_string(),
        6381,
        16_381,
        true,
    );
    new_primary.flags &= !CLUSTER_NODE_MYSELF;
    new_primary.shard_id = new_primary_id.to_string();
    new_primary.config_epoch = 9;
    new_primary.slots[0] = true;
    new_primary.numslots = 1;
    state.nodes.insert(new_primary_id.to_string(), new_primary);

    old_primary.flags &= !CLUSTER_NODE_PRIMARY;
    old_primary.flags |= CLUSTER_NODE_REPLICA;
    old_primary.replicaof = new_primary_id.to_string();
    old_primary.slots.fill(false);
    old_primary.numslots = 0;
    let message = msg::ClusterBusMessage::from_node(
        ClusterMsgType::Pong,
        &old_primary,
        9,
        msg::ClusterBusBody::Gossip(Vec::new()),
    );

    ClusterBusManager::add_or_update_node(&mut state, &message, true);

    assert_eq!(
        state.pending_replication_switch,
        Some(PendingReplicationSwitch {
            node_id: new_primary_id.to_string(),
            mode: ReplicationSwitchMode::ForceFull,
        })
    );
}
