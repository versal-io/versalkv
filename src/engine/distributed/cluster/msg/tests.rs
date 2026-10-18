use super::header::{read_u16, read_u64};
use super::*;
use crate::engine::distributed::cluster::node::{
    ClusterNode, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
};
use crate::engine::distributed::cluster::NodeId;

fn node(name: &str, is_primary: bool) -> ClusterNode {
    let mut node = ClusterNode::new(
        name.to_string(),
        "127.0.0.1".to_string(),
        6379,
        16379,
        is_primary,
    );
    node.config_epoch = 7;
    node.slots[0] = true;
    node.slots[1] = true;
    node.slots[16383] = true;
    node
}

fn literal_packet(message_type: u16, body: &[u8]) -> Vec<u8> {
    let mut packet = vec![0u8; CLUSTER_BUS_HEADER_LEN];
    packet[0..4].copy_from_slice(b"RCmb");
    packet[8..10].copy_from_slice(&1u16.to_be_bytes());
    packet[10..12].copy_from_slice(&6379u16.to_be_bytes());
    packet[12..14].copy_from_slice(&message_type.to_be_bytes());
    packet[40..80].copy_from_slice(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    packet[2168..2177].copy_from_slice(b"127.0.0.1");
    packet[2248..2250].copy_from_slice(&16379u16.to_be_bytes());
    packet.extend_from_slice(body);
    let total_len = u32::try_from(packet.len()).unwrap();
    packet[4..8].copy_from_slice(&total_len.to_be_bytes());
    packet
}

fn empty_body_for(kind: u16) -> Vec<u8> {
    match kind {
        0..=2 | 5 | 6 | 8 => Vec::new(),
        3 => b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_vec(),
        4 | 10 => {
            let mut body = Vec::new();
            body.extend_from_slice(&0u32.to_be_bytes());
            body.extend_from_slice(&0u32.to_be_bytes());
            body
        }
        7 => {
            let mut body = 9u64.to_be_bytes().to_vec();
            body.extend_from_slice(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
            body.resize(8 + 40 + SLOT_BYTES, 0);
            body
        }
        9 => {
            let mut body = 42u64.to_be_bytes().to_vec();
            body.extend_from_slice(&0u32.to_be_bytes());
            body.push(7);
            body
        }
        _ => unreachable!(),
    }
}

#[test]
fn message_type_ids_match_valkey() {
    let expected = [
        ClusterMsgType::Ping,
        ClusterMsgType::Pong,
        ClusterMsgType::Meet,
        ClusterMsgType::Fail,
        ClusterMsgType::Publish,
        ClusterMsgType::FailoverAuthRequest,
        ClusterMsgType::FailoverAuthAck,
        ClusterMsgType::Update,
        ClusterMsgType::MfStart,
        ClusterMsgType::Module,
        ClusterMsgType::PublishShard,
    ];
    for (wire_id, kind) in expected.into_iter().enumerate() {
        assert_eq!(kind as usize, wire_id);
        let (decoded, _) = ClusterBusMessage::decode(&literal_packet(
            wire_id as u16,
            &empty_body_for(wire_id as u16),
        ))
        .unwrap();
        assert_eq!(decoded.msg_type, kind);
    }
}

#[test]
fn literal_c_bodies_decode_to_typed_messages() {
    let victim = b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let (failed, consumed) = ClusterBusMessage::decode(&literal_packet(3, victim)).unwrap();
    assert_eq!(consumed, CLUSTER_BUS_HEADER_LEN + 40);
    assert!(
        matches!(failed.body, ClusterBusBody::Fail { node_id } if node_id.as_str() == std::str::from_utf8(victim).unwrap())
    );

    let mut update = Vec::with_capacity(8 + 40 + SLOT_BYTES);
    update.extend_from_slice(&9u64.to_be_bytes());
    update.extend_from_slice(victim);
    update.extend_from_slice(&vec![0x5a; SLOT_BYTES]);
    let (update, consumed) = ClusterBusMessage::decode(&literal_packet(7, &update)).unwrap();
    assert_eq!(consumed, CLUSTER_BUS_HEADER_LEN + 8 + 40 + SLOT_BYTES);
    assert!(
        matches!(update.body, ClusterBusBody::Update(ref value) if value.config_epoch == 9 && value.node_id.as_str() == std::str::from_utf8(victim).unwrap() && value.slots == vec![0x5a; SLOT_BYTES])
    );

    for (message_type, shard) in [(4, false), (10, true)] {
        let mut publish = Vec::new();
        publish.extend_from_slice(&3u32.to_be_bytes());
        publish.extend_from_slice(&4u32.to_be_bytes());
        publish.extend_from_slice(&[0xff, 0x00, b'a']);
        publish.extend_from_slice(&[0x80, b'b', 0x00, b'c']);
        let (decoded, consumed) =
            ClusterBusMessage::decode(&literal_packet(message_type, &publish)).unwrap();
        assert_eq!(consumed, CLUSTER_BUS_HEADER_LEN + 15);
        assert!(
            matches!(decoded.body, ClusterBusBody::Publish(ref value) if value.shard == shard && value.channel.as_ref() == [0xff, 0x00, b'a'] && value.payload.as_ref() == [0x80, b'b', 0x00, b'c'])
        );
    }
}

#[test]
fn typed_bodies_encode_to_literal_c_layouts() {
    let sender = node("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true);
    let victim = NodeId::parse(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
    let failed = ClusterBusMessage::from_node(
        ClusterMsgType::Fail,
        &sender,
        3,
        ClusterBusBody::Fail {
            node_id: victim.clone(),
        },
    )
    .encode()
    .unwrap();
    assert_eq!(read_u16(&failed, 12).unwrap(), 3);
    assert_eq!(
        &failed[CLUSTER_BUS_HEADER_LEN..],
        victim.as_str().as_bytes()
    );

    let slots = vec![0xa5; SLOT_BYTES];
    let update = ClusterBusMessage::from_node(
        ClusterMsgType::Update,
        &sender,
        4,
        ClusterBusBody::Update(SlotOwnershipUpdate {
            config_epoch: 11,
            node_id: victim.clone(),
            slots: slots.clone(),
        }),
    )
    .encode()
    .unwrap();
    assert_eq!(read_u16(&update, 12).unwrap(), 7);
    assert_eq!(read_u64(&update, CLUSTER_BUS_HEADER_LEN).unwrap(), 11);
    assert_eq!(
        &update[CLUSTER_BUS_HEADER_LEN + 8..CLUSTER_BUS_HEADER_LEN + 48],
        victim.as_str().as_bytes()
    );
    assert_eq!(&update[CLUSTER_BUS_HEADER_LEN + 48..], slots);

    let publish = ClusterBusMessage::from_node(
        ClusterMsgType::PublishShard,
        &sender,
        5,
        ClusterBusBody::Publish(super::body::ClusterPublish {
            channel: bytes::Bytes::from_static(b"abc"),
            payload: bytes::Bytes::from_static(b"defg"),
            shard: true,
        }),
    )
    .encode()
    .unwrap();
    assert_eq!(read_u16(&publish, 12).unwrap(), 10);
    assert_eq!(
        &publish[CLUSTER_BUS_HEADER_LEN..],
        &[0, 0, 0, 3, 0, 0, 0, 4, b'a', b'b', b'c', b'd', b'e', b'f', b'g']
    );

    let module = ClusterBusMessage::from_node(
        ClusterMsgType::Module,
        &sender,
        6,
        ClusterBusBody::Module(super::body::ClusterModuleMessage {
            module_id: 42,
            subtype: 7,
            payload: bytes::Bytes::from_static(&[0xff, 0x00, b'm']),
        }),
    )
    .encode()
    .unwrap();
    assert_eq!(read_u16(&module, 12).unwrap(), 9);
    assert_eq!(
        &module[CLUSTER_BUS_HEADER_LEN..],
        &[0, 0, 0, 0, 0, 0, 0, 42, 0, 0, 0, 3, 7, 0xff, 0x00, b'm']
    );
}

#[test]
fn paused_manual_failover_ack_is_a_pong_flag() {
    let sender = node("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true);
    let mut message = ClusterBusMessage::from_node(
        ClusterMsgType::Pong,
        &sender,
        0,
        ClusterBusBody::Gossip(Vec::new()),
    );
    message.set_message_flag(ClusterMessageFlags::PAUSED);

    let encoded = message.encode().unwrap();
    assert_eq!(read_u16(&encoded, 12).unwrap(), 1);
    assert_ne!(encoded[2253] & ClusterMessageFlags::PAUSED, 0);
}

#[test]
fn aligned_extensions_decode_and_unknown_kinds_are_skipped() {
    let mut packet = literal_packet(0, &[]);
    packet[2214..2216].copy_from_slice(&3u16.to_be_bytes());
    packet[2253] = ClusterMessageFlags::EXT_DATA;
    for (kind, payload) in [
        (0u16, b"cache\0".as_slice()),
        (6u16, 7000u16.to_be_bytes().as_slice()),
        (99u16, b"unknown".as_slice()),
    ] {
        let aligned = payload.len().div_ceil(8) * 8;
        packet.extend_from_slice(&u32::try_from(8 + aligned).unwrap().to_be_bytes());
        packet.extend_from_slice(&kind.to_be_bytes());
        packet.extend_from_slice(&0u16.to_be_bytes());
        packet.extend_from_slice(payload);
        packet.resize(packet.len() + aligned - payload.len(), 0);
    }
    let total = u32::try_from(packet.len()).unwrap();
    packet[4..8].copy_from_slice(&total.to_be_bytes());

    let (decoded, consumed) = ClusterBusMessage::decode(&packet).unwrap();
    assert_eq!(consumed, packet.len());
    assert_eq!(
        decoded.extensions[0],
        ClusterExtension::Hostname("cache".into())
    );
    assert_eq!(decoded.extensions[1], ClusterExtension::ClientPort(7000));
    assert!(matches!(
        decoded.extensions[2],
        ClusterExtension::Unknown { kind: 99, .. }
    ));
}

#[test]
fn malformed_lengths_ids_and_string_extensions_are_rejected() {
    let mut bad_signature = literal_packet(0, &[]);
    bad_signature[0..4].copy_from_slice(b"NOPE");
    assert!(matches!(
        ClusterBusMessage::decode(&bad_signature),
        Err(ClusterProtocolError::InvalidSignature)
    ));

    let mut bad_version = literal_packet(0, &[]);
    bad_version[8..10].copy_from_slice(&2u16.to_be_bytes());
    assert!(matches!(
        ClusterBusMessage::decode(&bad_version),
        Err(ClusterProtocolError::UnsupportedVersion(2))
    ));

    let mut bad_sender = literal_packet(0, &[]);
    bad_sender[40] = b'A';
    assert!(matches!(
        ClusterBusMessage::decode(&bad_sender),
        Err(ClusterProtocolError::InvalidNodeId(_))
    ));

    let mut truncated_gossip = literal_packet(0, &[]);
    truncated_gossip[14..16].copy_from_slice(&1u16.to_be_bytes());
    assert!(matches!(
        ClusterBusMessage::decode(&truncated_gossip),
        Err(ClusterProtocolError::InvalidBody(_))
    ));

    let mut extra_fail = literal_packet(3, b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbx");
    assert!(matches!(
        ClusterBusMessage::decode(&extra_fail),
        Err(ClusterProtocolError::InvalidLength { .. })
    ));
    extra_fail.truncate(CLUSTER_BUS_HEADER_LEN + 20);
    assert!(matches!(
        ClusterBusMessage::decode(&extra_fail),
        Err(ClusterProtocolError::Incomplete { .. })
    ));

    let mut bad_extension = literal_packet(0, &[]);
    bad_extension[2214..2216].copy_from_slice(&1u16.to_be_bytes());
    bad_extension[2253] = ClusterMessageFlags::EXT_DATA;
    bad_extension.extend_from_slice(&16u32.to_be_bytes());
    bad_extension.extend_from_slice(&0u16.to_be_bytes());
    bad_extension.extend_from_slice(&0u16.to_be_bytes());
    bad_extension.extend_from_slice(b"12345678");
    let total = u32::try_from(bad_extension.len()).unwrap();
    bad_extension[4..8].copy_from_slice(&total.to_be_bytes());
    assert!(matches!(
        ClusterBusMessage::decode(&bad_extension),
        Err(ClusterProtocolError::InvalidExtension(_))
    ));
}

#[test]
fn decoder_consumes_exactly_one_packet_from_a_stream_buffer() {
    let first = literal_packet(5, &[]);
    let second = literal_packet(6, &[]);
    let mut stream = first.clone();
    stream.extend_from_slice(&second);

    let (decoded, consumed) = ClusterBusMessage::decode(&stream).unwrap();

    assert_eq!(decoded.msg_type, ClusterMsgType::FailoverAuthRequest);
    assert_eq!(consumed, first.len());
    assert_eq!(&stream[consumed..], second);
}

#[test]
fn encode_decode_round_trip_uses_c_offsets_and_extensions() {
    let mut sender = node("abcdef1234567890abcdef1234567890abcdef12", true);
    sender.repl_offset = 123;
    sender.endpoints.set_hostname(b"cache.example.com").unwrap();
    sender.endpoints.set_human_name(b"cache-east-1").unwrap();
    sender.endpoints.set_client_port(7000);
    let other = node("1234567890abcdef1234567890abcdef12345678", false);
    let gossip = build_gossip_entries(&[&sender, &other], &sender, 10);
    let encoded = encode_msg(ClusterMsgType::Ping, &sender, 42, &gossip);

    assert_eq!(&encoded[..4], b"RCmb");
    assert_eq!(read_u16(&encoded, 10).unwrap(), 6379);
    assert_eq!(read_u16(&encoded, 12).unwrap(), 0);
    assert_eq!(read_u16(&encoded, 14).unwrap(), 1);
    assert_eq!(read_u64(&encoded, 16).unwrap(), 42);
    assert_eq!(read_u64(&encoded, 24).unwrap(), 7);
    assert_eq!(read_u64(&encoded, 32).unwrap(), 123);
    assert_eq!(read_u16(&encoded, 2248).unwrap(), 16379);
    assert_ne!(encoded[2253] & ClusterMessageFlags::EXT_DATA, 0);

    let (decoded, consumed) = ClusterBusMessage::decode(&encoded).unwrap();
    assert_eq!(consumed, encoded.len());
    assert_eq!(decoded.sender_id, sender.name);
    assert_eq!(decoded.gossip()[0].node_id, other.name);
    assert!(decoded
        .extensions
        .contains(&ClusterExtension::Hostname("cache.example.com".into())));
    assert!(decoded
        .extensions
        .contains(&ClusterExtension::HumanNodeName("cache-east-1".into())));
    assert!(decoded
        .extensions
        .contains(&ClusterExtension::ClientPort(7000)));
}

#[test]
fn full_header_does_not_advertise_unsupported_light_messages() {
    use crate::engine::distributed::cluster::node::{
        CLUSTER_NODE_EXTENSIONS_SUPPORTED, CLUSTER_NODE_LIGHT_HDR_MODULE_SUPPORTED,
        CLUSTER_NODE_LIGHT_HDR_PUBLISH_SUPPORTED,
    };

    let sender = node("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true);
    let encoded = encode_msg(ClusterMsgType::Ping, &sender, 0, &[]);
    let wire_flags = read_u16(&encoded, 2250).unwrap();

    assert_ne!(wire_flags & CLUSTER_NODE_EXTENSIONS_SUPPORTED, 0);
    assert_eq!(wire_flags & CLUSTER_NODE_LIGHT_HDR_PUBLISH_SUPPORTED, 0);
    assert_eq!(wire_flags & CLUSTER_NODE_LIGHT_HDR_MODULE_SUPPORTED, 0);
}

#[test]
fn replica_header_round_trips_primary_id() {
    let mut replica = node("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", false);
    replica.replicaof = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string();
    replica.flags &= !CLUSTER_NODE_PRIMARY;
    replica.flags |= CLUSTER_NODE_REPLICA;
    let encoded = encode_msg(ClusterMsgType::Pong, &replica, 0, &[]);
    let (decoded, _) = ClusterBusMessage::decode(&encoded).unwrap();
    assert_eq!(decoded.replicaof, replica.replicaof);
}

#[test]
fn slot_bitmap_round_trip_preserves_edges() {
    let mut slots = vec![false; crate::engine::distributed::cluster::state::CLUSTER_SLOTS];
    slots[0] = true;
    slots[100] = true;
    slots[16383] = true;
    let decoded = bytes_to_slots(&slots_to_bytes(&slots));
    assert!(decoded[0] && decoded[100] && decoded[16383]);
    assert!(!decoded[1] && !decoded[5000]);
}

#[test]
fn gossip_timestamps_use_seconds_on_wire_and_milliseconds_in_memory() {
    let sender = node("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true);
    let gossip = GossipEntry {
        node_id: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
        flags: CLUSTER_NODE_PRIMARY,
        ping_sent: 12_345,
        pong_received: 67_890,
        ip: "127.0.0.1".to_string(),
        port: 6380,
        tls_port: 0,
        bus_port: 16380,
        config_epoch: 0,
    };
    let encoded = encode_msg(ClusterMsgType::Ping, &sender, 0, &[gossip]);
    assert_eq!(
        u32::from_be_bytes(
            encoded[CLUSTER_BUS_HEADER_LEN + 40..CLUSTER_BUS_HEADER_LEN + 44]
                .try_into()
                .unwrap()
        ),
        12
    );
    let (decoded, _) = ClusterBusMessage::decode(&encoded).unwrap();
    assert_eq!(decoded.gossip()[0].ping_sent, 12_000);
    assert_eq!(decoded.gossip()[0].pong_received, 67_000);
}
