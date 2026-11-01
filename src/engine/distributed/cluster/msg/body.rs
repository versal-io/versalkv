use bytes::Bytes;

use super::header::{read_u16, read_u32, read_u64, ClusterMsgType, ClusterProtocolError};
use super::{ClusterBusHeader, CLUSTER_BUS_HEADER_LEN, NET_IP_STR_LEN};
use crate::engine::distributed::cluster::endpoint::NodeId;
use crate::engine::distributed::cluster::node::ClusterNode;
use crate::engine::distributed::cluster::state::CLUSTER_SLOTS;

pub const SLOT_BYTES: usize = CLUSTER_SLOTS / 8;
pub const GOSSIP_ENTRY_WIRE_BYTES: usize = 104;
const FAIL_BODY_LEN: usize = 40;
const PUBLISH_PREFIX_LEN: usize = 8;
const UPDATE_BODY_LEN: usize = 8 + 40 + SLOT_BYTES;
const MODULE_PREFIX_LEN: usize = 13;
const FAILOVER_AUTH_NACK_BODY_LEN: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GossipEntry {
    pub node_id: String,
    pub flags: u16,
    pub ping_sent: u64,
    pub pong_received: u64,
    pub ip: String,
    pub port: u16,
    pub tls_port: u16,
    pub bus_port: u16,
    pub config_epoch: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterPublish {
    pub channel: Bytes,
    pub payload: Bytes,
    pub shard: bool,
}

impl ClusterPublish {
    pub fn new(channel: Bytes, payload: Bytes, shard: bool) -> Self {
        Self {
            channel,
            payload,
            shard,
        }
    }

    pub fn channel(&self) -> &Bytes {
        &self.channel
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    pub fn is_shard(&self) -> bool {
        self.shard
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotOwnershipUpdate {
    pub config_epoch: u64,
    pub node_id: NodeId,
    pub slots: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterModuleMessage {
    pub module_id: u64,
    pub subtype: u8,
    pub payload: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterBusBody {
    Gossip(Vec<GossipEntry>),
    Fail { node_id: NodeId },
    Publish(ClusterPublish),
    Update(SlotOwnershipUpdate),
    Module(ClusterModuleMessage),
    FailoverAuthNack { request_epoch: u64, reason: u8 },
    Empty,
}

pub(super) fn encode_body(
    message_type: ClusterMsgType,
    body: &ClusterBusBody,
    output: &mut Vec<u8>,
) -> Result<u16, ClusterProtocolError> {
    match (message_type, body) {
        (
            ClusterMsgType::Ping | ClusterMsgType::Pong | ClusterMsgType::Meet,
            ClusterBusBody::Gossip(entries),
        ) => {
            let count =
                u16::try_from(entries.len()).map_err(|_| ClusterProtocolError::LengthOverflow)?;
            for entry in entries {
                encode_gossip_entry(output, entry)?;
            }
            Ok(count)
        }
        (ClusterMsgType::Fail, ClusterBusBody::Fail { node_id }) => {
            output.extend_from_slice(node_id.as_str().as_bytes());
            Ok(0)
        }
        (
            ClusterMsgType::FailoverAuthNack,
            ClusterBusBody::FailoverAuthNack {
                request_epoch,
                reason,
            },
        ) => {
            output.extend_from_slice(&request_epoch.to_be_bytes());
            output.push(*reason);
            output.extend_from_slice(&[0; 7]);
            Ok(0)
        }
        (
            ClusterMsgType::Publish | ClusterMsgType::PublishShard,
            ClusterBusBody::Publish(publish),
        ) if publish.shard == (message_type == ClusterMsgType::PublishShard) => {
            write_u32(output, publish.channel.len())?;
            write_u32(output, publish.payload.len())?;
            output.extend_from_slice(&publish.channel);
            output.extend_from_slice(&publish.payload);
            Ok(0)
        }
        (ClusterMsgType::Update, ClusterBusBody::Update(update)) => {
            if update.slots.len() != SLOT_BYTES {
                return Err(ClusterProtocolError::InvalidBody(
                    "UPDATE slot bitmap must be 2048 bytes".to_string(),
                ));
            }
            output.extend_from_slice(&update.config_epoch.to_be_bytes());
            output.extend_from_slice(update.node_id.as_str().as_bytes());
            output.extend_from_slice(&update.slots);
            Ok(0)
        }
        (ClusterMsgType::Module, ClusterBusBody::Module(module)) => {
            output.extend_from_slice(&module.module_id.to_be_bytes());
            write_u32(output, module.payload.len())?;
            output.push(module.subtype);
            output.extend_from_slice(&module.payload);
            Ok(0)
        }
        (
            ClusterMsgType::FailoverAuthRequest
            | ClusterMsgType::FailoverAuthAck
            | ClusterMsgType::MfStart,
            ClusterBusBody::Empty,
        ) => Ok(0),
        _ => Err(ClusterProtocolError::BodyTypeMismatch),
    }
}

pub(super) fn decode_body(
    packet: &[u8],
    header: &ClusterBusHeader,
) -> Result<(ClusterBusBody, usize), ClusterProtocolError> {
    let start = CLUSTER_BUS_HEADER_LEN;
    if !matches!(
        header.msg_type,
        ClusterMsgType::Ping | ClusterMsgType::Pong | ClusterMsgType::Meet
    ) && header.gossip_count != 0
    {
        return Err(ClusterProtocolError::InvalidBody(
            "non-gossip message has a non-zero gossip count".to_string(),
        ));
    }
    match header.msg_type {
        ClusterMsgType::Ping | ClusterMsgType::Pong | ClusterMsgType::Meet => {
            let bytes = usize::from(header.gossip_count)
                .checked_mul(GOSSIP_ENTRY_WIRE_BYTES)
                .ok_or(ClusterProtocolError::LengthOverflow)?;
            let end = start
                .checked_add(bytes)
                .ok_or(ClusterProtocolError::LengthOverflow)?;
            require_len(packet, end, "gossip body")?;
            let mut entries = Vec::with_capacity(usize::from(header.gossip_count));
            for offset in (start..end).step_by(GOSSIP_ENTRY_WIRE_BYTES) {
                entries.push(decode_gossip_entry(packet, offset)?);
            }
            Ok((ClusterBusBody::Gossip(entries), end))
        }
        ClusterMsgType::Fail => {
            let end = start + FAIL_BODY_LEN;
            require_exact(packet, end, "FAIL body")?;
            let node_id = NodeId::parse(&packet[start..end])
                .map_err(|error| ClusterProtocolError::InvalidNodeId(error.to_string()))?;
            Ok((ClusterBusBody::Fail { node_id }, end))
        }
        ClusterMsgType::FailoverAuthNack => {
            let end = start + FAILOVER_AUTH_NACK_BODY_LEN;
            require_exact(packet, end, "FAILOVER_AUTH_NACK body")?;
            Ok((
                ClusterBusBody::FailoverAuthNack {
                    request_epoch: read_u64(packet, start)?,
                    reason: packet[start + 8],
                },
                end,
            ))
        }
        ClusterMsgType::Publish | ClusterMsgType::PublishShard => {
            require_len(packet, start + PUBLISH_PREFIX_LEN, "PUBLISH body")?;
            let channel_len = read_u32(packet, start)? as usize;
            let message_len = read_u32(packet, start + 4)? as usize;
            let end = start
                .checked_add(PUBLISH_PREFIX_LEN)
                .and_then(|value| value.checked_add(channel_len))
                .and_then(|value| value.checked_add(message_len))
                .ok_or(ClusterProtocolError::LengthOverflow)?;
            require_exact(packet, end, "PUBLISH body")?;
            let channel_start = start + PUBLISH_PREFIX_LEN;
            let payload_start = channel_start + channel_len;
            Ok((
                ClusterBusBody::Publish(ClusterPublish {
                    channel: Bytes::copy_from_slice(&packet[channel_start..payload_start]),
                    payload: Bytes::copy_from_slice(&packet[payload_start..end]),
                    shard: header.msg_type == ClusterMsgType::PublishShard,
                }),
                end,
            ))
        }
        ClusterMsgType::Update => {
            let end = start + UPDATE_BODY_LEN;
            require_exact(packet, end, "UPDATE body")?;
            let node_id = NodeId::parse(&packet[start + 8..start + 48])
                .map_err(|error| ClusterProtocolError::InvalidNodeId(error.to_string()))?;
            Ok((
                ClusterBusBody::Update(SlotOwnershipUpdate {
                    config_epoch: read_u64(packet, start)?,
                    node_id,
                    slots: packet[start + 48..end].to_vec(),
                }),
                end,
            ))
        }
        ClusterMsgType::Module => {
            require_len(packet, start + MODULE_PREFIX_LEN, "MODULE body")?;
            let payload_len = read_u32(packet, start + 8)? as usize;
            let end = start
                .checked_add(MODULE_PREFIX_LEN)
                .and_then(|value| value.checked_add(payload_len))
                .ok_or(ClusterProtocolError::LengthOverflow)?;
            require_exact(packet, end, "MODULE body")?;
            Ok((
                ClusterBusBody::Module(ClusterModuleMessage {
                    module_id: read_u64(packet, start)?,
                    subtype: packet[start + 12],
                    payload: Bytes::copy_from_slice(&packet[start + 13..end]),
                }),
                end,
            ))
        }
        ClusterMsgType::FailoverAuthRequest
        | ClusterMsgType::FailoverAuthAck
        | ClusterMsgType::MfStart => {
            require_exact(packet, start, "empty message body")?;
            Ok((ClusterBusBody::Empty, start))
        }
    }
}

fn encode_gossip_entry(
    output: &mut Vec<u8>,
    entry: &GossipEntry,
) -> Result<(), ClusterProtocolError> {
    let node_id = NodeId::parse(entry.node_id.as_bytes())
        .map_err(|error| ClusterProtocolError::InvalidNodeId(error.to_string()))?;
    output.extend_from_slice(node_id.as_str().as_bytes());
    output.extend_from_slice(
        &((entry.ping_sent / 1_000).min(u64::from(u32::MAX)) as u32).to_be_bytes(),
    );
    output.extend_from_slice(
        &((entry.pong_received / 1_000).min(u64::from(u32::MAX)) as u32).to_be_bytes(),
    );
    encode_fixed_string(output, &entry.ip, NET_IP_STR_LEN)?;
    output.extend_from_slice(&entry.port.to_be_bytes());
    output.extend_from_slice(&entry.bus_port.to_be_bytes());
    output.extend_from_slice(&entry.flags.to_be_bytes());
    output.extend_from_slice(&entry.tls_port.to_be_bytes());
    output.extend_from_slice(&0u16.to_be_bytes());
    Ok(())
}

fn decode_gossip_entry(packet: &[u8], offset: usize) -> Result<GossipEntry, ClusterProtocolError> {
    let end = offset + GOSSIP_ENTRY_WIRE_BYTES;
    require_len(packet, end, "gossip entry")?;
    let node_id = NodeId::parse(&packet[offset..offset + 40])
        .map_err(|error| ClusterProtocolError::InvalidNodeId(error.to_string()))?;
    Ok(GossipEntry {
        node_id: node_id.to_string(),
        ping_sent: u64::from(read_u32(packet, offset + 40)?).saturating_mul(1_000),
        pong_received: u64::from(read_u32(packet, offset + 44)?).saturating_mul(1_000),
        ip: decode_fixed_string(packet, offset + 48, NET_IP_STR_LEN)?,
        port: read_u16(packet, offset + 94)?,
        bus_port: read_u16(packet, offset + 96)?,
        flags: read_u16(packet, offset + 98)?,
        tls_port: read_u16(packet, offset + 100)?,
        config_epoch: 0,
    })
}

pub(super) fn encode_fixed_string(
    output: &mut Vec<u8>,
    value: &str,
    width: usize,
) -> Result<(), ClusterProtocolError> {
    if value.len() >= width {
        return Err(ClusterProtocolError::InvalidBody(format!(
            "fixed string is too long for {width}-byte field"
        )));
    }
    output.extend_from_slice(value.as_bytes());
    output.resize(output.len() + width - value.len(), 0);
    Ok(())
}

pub(super) fn decode_fixed_string(
    packet: &[u8],
    offset: usize,
    width: usize,
) -> Result<String, ClusterProtocolError> {
    require_len(packet, offset + width, "fixed string")?;
    let field = &packet[offset..offset + width];
    let end = field.iter().position(|byte| *byte == 0).unwrap_or(width);
    std::str::from_utf8(&field[..end])
        .map(str::to_string)
        .map_err(|_| ClusterProtocolError::InvalidUtf8)
}

fn require_len(packet: &[u8], needed: usize, field: &str) -> Result<(), ClusterProtocolError> {
    if packet.len() < needed {
        Err(ClusterProtocolError::InvalidBody(format!(
            "truncated {field}"
        )))
    } else {
        Ok(())
    }
}

fn require_exact(packet: &[u8], expected: usize, field: &str) -> Result<(), ClusterProtocolError> {
    if packet.len() != expected {
        Err(ClusterProtocolError::InvalidLength {
            declared: packet.len(),
            expected,
            field: field.to_string(),
        })
    } else {
        Ok(())
    }
}

fn write_u32(output: &mut Vec<u8>, value: usize) -> Result<(), ClusterProtocolError> {
    let value = u32::try_from(value).map_err(|_| ClusterProtocolError::LengthOverflow)?;
    output.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

pub fn slots_to_bytes(slots: &[bool]) -> Vec<u8> {
    let mut bytes = vec![0u8; SLOT_BYTES];
    for (slot, is_owned) in slots.iter().copied().enumerate().take(CLUSTER_SLOTS) {
        if is_owned {
            bytes[slot / 8] |= 1 << (7 - (slot % 8));
        }
    }
    bytes
}

pub fn bytes_to_slots(bytes: &[u8]) -> Vec<bool> {
    let mut slots = vec![false; CLUSTER_SLOTS];
    for (index, byte) in bytes.iter().copied().enumerate().take(SLOT_BYTES) {
        for bit in 0..8 {
            if byte & (1 << (7 - bit)) != 0 {
                slots[index * 8 + bit] = true;
            }
        }
    }
    slots
}

#[cfg(test)]
pub fn build_gossip_entries(
    nodes: &[&ClusterNode],
    myself: &ClusterNode,
    max_entries: usize,
) -> Vec<GossipEntry> {
    use crate::engine::distributed::cluster::node::{
        CLUSTER_NODE_HANDSHAKE, CLUSTER_NODE_NOADDR, CLUSTER_NODE_PFAIL,
    };

    let eligible = |node: &&&ClusterNode| {
        node.name != myself.name
            && !node.has_flag(CLUSTER_NODE_HANDSHAKE | CLUSTER_NODE_NOADDR)
            && NodeId::parse(node.name.as_bytes()).is_ok()
    };
    let mut entries: Vec<_> = nodes
        .iter()
        .filter(eligible)
        .filter(|node| !node.has_flag(CLUSTER_NODE_PFAIL))
        .take(max_entries)
        .map(|node| gossip_entry(node))
        .collect();
    entries.extend(
        nodes
            .iter()
            .filter(eligible)
            .filter(|node| node.has_flag(CLUSTER_NODE_PFAIL))
            .map(|node| gossip_entry(node)),
    );
    entries
}

pub fn build_rotating_gossip_entries(
    nodes: &[&ClusterNode],
    myself: &ClusterNode,
    max_entries: usize,
    cursor: usize,
) -> Vec<GossipEntry> {
    use crate::engine::distributed::cluster::node::{
        CLUSTER_NODE_HANDSHAKE, CLUSTER_NODE_NOADDR, CLUSTER_NODE_PFAIL,
    };

    let mut regular: Vec<_> = nodes
        .iter()
        .copied()
        .filter(|node| {
            node.name != myself.name
                && !node.has_flag(CLUSTER_NODE_HANDSHAKE | CLUSTER_NODE_NOADDR | CLUSTER_NODE_PFAIL)
                && NodeId::parse(node.name.as_bytes()).is_ok()
        })
        .collect();
    regular.sort_unstable_by(|left, right| left.name.cmp(&right.name));
    if !regular.is_empty() {
        let rotate = cursor % regular.len();
        regular.rotate_left(rotate);
    }
    let mut entries: Vec<_> = regular
        .into_iter()
        .take(max_entries)
        .map(gossip_entry)
        .collect();
    entries.extend(
        nodes
            .iter()
            .copied()
            .filter(|node| {
                node.name != myself.name
                    && node.has_flag(CLUSTER_NODE_PFAIL)
                    && !node.has_flag(CLUSTER_NODE_HANDSHAKE | CLUSTER_NODE_NOADDR)
                    && NodeId::parse(node.name.as_bytes()).is_ok()
            })
            .map(gossip_entry),
    );
    entries
}

fn gossip_entry(node: &ClusterNode) -> GossipEntry {
    GossipEntry {
        node_id: node.name.clone(),
        flags: node.flags,
        ping_sent: node.ping_sent.max(0) as u64,
        pong_received: node.pong_received.max(0) as u64,
        ip: node.ip().to_string(),
        port: node.port(),
        tls_port: node.endpoints.tls_port(),
        bus_port: node.cport(),
        config_epoch: 0,
    }
}
