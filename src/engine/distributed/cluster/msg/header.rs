use std::fmt;

use super::body::{decode_body, encode_body, encode_fixed_string, slots_to_bytes, ClusterBusBody};
use super::extension::{decode_extensions, extensions_for_node};
use super::{ClusterBusMessage, ClusterMessageFlags, SLOT_BYTES};
use crate::engine::distributed::cluster::endpoint::NodeId;
use crate::engine::distributed::cluster::node::ClusterNode;

pub const CLUSTER_BUS_HEADER_LEN: usize = 2256;
pub const NET_IP_STR_LEN: usize = 46;
const CLUSTER_PROTOCOL_VERSION: u16 = 1;
const SIGNATURE: &[u8; 4] = b"RCmb";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum ClusterMsgType {
    Ping = 0,
    Pong = 1,
    Meet = 2,
    Fail = 3,
    Publish = 4,
    FailoverAuthRequest = 5,
    FailoverAuthAck = 6,
    Update = 7,
    MfStart = 8,
    Module = 9,
    PublishShard = 10,
    FailoverAuthNack = 11,
}

impl ClusterMsgType {
    pub fn from_u16(value: u16) -> Option<Self> {
        Some(match value {
            0 => Self::Ping,
            1 => Self::Pong,
            2 => Self::Meet,
            3 => Self::Fail,
            4 => Self::Publish,
            5 => Self::FailoverAuthRequest,
            6 => Self::FailoverAuthAck,
            7 => Self::Update,
            8 => Self::MfStart,
            9 => Self::Module,
            10 => Self::PublishShard,
            11 => Self::FailoverAuthNack,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterBusHeader {
    pub msg_type: ClusterMsgType,
    pub sender_id: String,
    pub current_epoch: u64,
    pub config_epoch: u64,
    pub repl_offset: i64,
    pub port: u16,
    pub tls_port: u16,
    pub bus_port: u16,
    pub flags: u16,
    pub replicaof: String,
    pub sender_ip: String,
    pub myslots: Vec<u8>,
    pub gossip_count: u16,
    pub extension_count: u16,
    pub cluster_state: u8,
    pub message_flags: [u8; 3],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterProtocolError {
    Incomplete {
        available: usize,
        needed: usize,
    },
    InvalidSignature,
    UnsupportedVersion(u16),
    UnknownMessageType(u16),
    InvalidLength {
        declared: usize,
        expected: usize,
        field: String,
    },
    InvalidNodeId(String),
    InvalidUtf8,
    InvalidBody(String),
    InvalidExtension(String),
    BodyTypeMismatch,
    LengthOverflow,
}

impl ClusterProtocolError {
    pub fn is_incomplete(&self) -> bool {
        matches!(self, Self::Incomplete { .. })
    }
}

impl fmt::Display for ClusterProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Incomplete { available, needed } => {
                write!(
                    formatter,
                    "incomplete cluster packet: have {available}, need {needed}"
                )
            }
            Self::InvalidSignature => formatter.write_str("invalid cluster message signature"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported cluster protocol version {version}")
            }
            Self::UnknownMessageType(kind) => {
                write!(formatter, "unknown cluster message type {kind}")
            }
            Self::InvalidLength {
                declared,
                expected,
                field,
            } => write!(
                formatter,
                "invalid {field} length: declared {declared}, expected {expected}"
            ),
            Self::InvalidNodeId(message) => formatter.write_str(message),
            Self::InvalidUtf8 => formatter.write_str("cluster string is not valid UTF-8"),
            Self::InvalidBody(message) | Self::InvalidExtension(message) => {
                formatter.write_str(message)
            }
            Self::BodyTypeMismatch => {
                formatter.write_str("cluster message body does not match its type")
            }
            Self::LengthOverflow => formatter.write_str("cluster message length overflow"),
        }
    }
}

impl std::error::Error for ClusterProtocolError {}

impl ClusterBusMessage {
    pub fn from_node_with_slots(
        msg_type: ClusterMsgType,
        sender: &ClusterNode,
        current_epoch: u64,
        body: ClusterBusBody,
        myslots: Vec<u8>,
    ) -> Self {
        let extensions = if matches!(
            msg_type,
            ClusterMsgType::Ping | ClusterMsgType::Pong | ClusterMsgType::Meet
        ) {
            extensions_for_node(sender)
        } else {
            Vec::new()
        };
        Self {
            header: ClusterBusHeader {
                msg_type,
                sender_id: sender.name.clone(),
                current_epoch,
                config_epoch: sender.config_epoch,
                repl_offset: sender.repl_offset,
                port: sender.port(),
                tls_port: sender.endpoints.tls_port(),
                bus_port: sender.cport(),
                flags: sender.flags
                    | crate::engine::distributed::cluster::node::CLUSTER_NODE_EXTENSIONS_SUPPORTED
                    | crate::engine::distributed::cluster::node::CLUSTER_NODE_FAILOVER_AUTH_NACK_SUPPORTED,
                replicaof: sender.replicaof.clone(),
                sender_ip: sender.ip().to_string(),
                myslots,
                gossip_count: 0,
                extension_count: 0,
                cluster_state: 0,
                message_flags: [0; 3],
            },
            body,
            extensions,
        }
    }

    pub fn from_node(
        msg_type: ClusterMsgType,
        sender: &ClusterNode,
        current_epoch: u64,
        body: ClusterBusBody,
    ) -> Self {
        Self::from_node_with_slots(
            msg_type,
            sender,
            current_epoch,
            body,
            slots_to_bytes(&sender.slots),
        )
    }

    pub fn set_message_flag(&mut self, flag: u8) {
        self.header.message_flags[0] |= flag;
    }

    pub fn has_message_flag(&self, flag: u8) -> bool {
        self.header.message_flags[0] & flag != 0
    }

    pub fn encode(&self) -> Result<Vec<u8>, ClusterProtocolError> {
        validate_node_id(&self.header.sender_id, false)?;
        validate_node_id(&self.header.replicaof, true)?;
        if self.header.myslots.len() != SLOT_BYTES {
            return Err(ClusterProtocolError::InvalidBody(
                "header slot bitmap must be 2048 bytes".to_string(),
            ));
        }
        let mut body = Vec::new();
        let gossip_count = encode_body(self.header.msg_type, &self.body, &mut body)?;
        let allows_extensions = matches!(
            self.header.msg_type,
            ClusterMsgType::Ping | ClusterMsgType::Pong | ClusterMsgType::Meet
        );
        if !allows_extensions && !self.extensions.is_empty() {
            return Err(ClusterProtocolError::BodyTypeMismatch);
        }
        let mut extension_bytes = Vec::new();
        for extension in &self.extensions {
            extension.encode(&mut extension_bytes)?;
        }
        let extension_count = u16::try_from(self.extensions.len())
            .map_err(|_| ClusterProtocolError::LengthOverflow)?;
        let total_len = CLUSTER_BUS_HEADER_LEN
            .checked_add(body.len())
            .and_then(|value| value.checked_add(extension_bytes.len()))
            .ok_or(ClusterProtocolError::LengthOverflow)?;
        let total_len_u32 =
            u32::try_from(total_len).map_err(|_| ClusterProtocolError::LengthOverflow)?;

        let mut output = Vec::with_capacity(total_len);
        output.extend_from_slice(SIGNATURE);
        output.extend_from_slice(&total_len_u32.to_be_bytes());
        output.extend_from_slice(&CLUSTER_PROTOCOL_VERSION.to_be_bytes());
        output.extend_from_slice(&self.header.port.to_be_bytes());
        output.extend_from_slice(&(self.header.msg_type as u16).to_be_bytes());
        output.extend_from_slice(&gossip_count.to_be_bytes());
        output.extend_from_slice(&self.header.current_epoch.to_be_bytes());
        output.extend_from_slice(&self.header.config_epoch.to_be_bytes());
        output.extend_from_slice(&(self.header.repl_offset.max(0) as u64).to_be_bytes());
        output.extend_from_slice(self.header.sender_id.as_bytes());
        output.extend_from_slice(&self.header.myslots);
        encode_optional_node_id(&mut output, &self.header.replicaof)?;
        encode_fixed_string(&mut output, &self.header.sender_ip, NET_IP_STR_LEN)?;
        output.extend_from_slice(&extension_count.to_be_bytes());
        output.extend_from_slice(&[0; 30]);
        output.extend_from_slice(&self.header.tls_port.to_be_bytes());
        output.extend_from_slice(&self.header.bus_port.to_be_bytes());
        output.extend_from_slice(&self.header.flags.to_be_bytes());
        output.push(self.header.cluster_state);
        let mut flags = self.header.message_flags;
        if extension_count > 0 {
            flags[0] |= ClusterMessageFlags::EXT_DATA;
        } else {
            flags[0] &= !ClusterMessageFlags::EXT_DATA;
        }
        output.extend_from_slice(&flags);
        output.extend_from_slice(&body);
        output.extend_from_slice(&extension_bytes);
        Ok(output)
    }

    pub fn decode(data: &[u8]) -> Result<(Self, usize), ClusterProtocolError> {
        if data.len() < 8 {
            return Err(ClusterProtocolError::Incomplete {
                available: data.len(),
                needed: 8,
            });
        }
        if &data[..4] != SIGNATURE {
            return Err(ClusterProtocolError::InvalidSignature);
        }
        let total_len = read_u32(data, 4)? as usize;
        if total_len < CLUSTER_BUS_HEADER_LEN {
            return Err(ClusterProtocolError::InvalidLength {
                declared: total_len,
                expected: CLUSTER_BUS_HEADER_LEN,
                field: "cluster packet".to_string(),
            });
        }
        if data.len() < total_len {
            return Err(ClusterProtocolError::Incomplete {
                available: data.len(),
                needed: total_len,
            });
        }
        let packet = &data[..total_len];
        let header = decode_header(packet)?;
        let (body, body_end) = decode_body(packet, &header)?;
        let has_extensions = header.message_flags[0] & ClusterMessageFlags::EXT_DATA != 0;
        if !has_extensions && header.extension_count != 0 {
            return Err(ClusterProtocolError::InvalidExtension(
                "extension count present without EXT_DATA flag".to_string(),
            ));
        }
        let (extensions, end) = if has_extensions {
            decode_extensions(packet, body_end, header.extension_count)?
        } else {
            (Vec::new(), body_end)
        };
        if end != packet.len() {
            return Err(ClusterProtocolError::InvalidLength {
                declared: packet.len(),
                expected: end,
                field: "cluster packet body".to_string(),
            });
        }
        Ok((
            Self {
                header,
                body,
                extensions,
            },
            total_len,
        ))
    }
}

fn decode_header(packet: &[u8]) -> Result<ClusterBusHeader, ClusterProtocolError> {
    let version = read_u16(packet, 8)?;
    if version != CLUSTER_PROTOCOL_VERSION {
        return Err(ClusterProtocolError::UnsupportedVersion(version));
    }
    let message_type = read_u16(packet, 12)?;
    let msg_type = ClusterMsgType::from_u16(message_type)
        .ok_or(ClusterProtocolError::UnknownMessageType(message_type))?;
    let sender_id = decode_node_id(&packet[40..80], false)?;
    let replicaof = decode_node_id(&packet[2128..2168], true)?;
    Ok(ClusterBusHeader {
        msg_type,
        sender_id,
        current_epoch: read_u64(packet, 16)?,
        config_epoch: read_u64(packet, 24)?,
        repl_offset: read_u64(packet, 32)?.min(i64::MAX as u64) as i64,
        port: read_u16(packet, 10)?,
        tls_port: read_u16(packet, 2246)?,
        bus_port: read_u16(packet, 2248)?,
        flags: read_u16(packet, 2250)?,
        replicaof,
        sender_ip: super::body::decode_fixed_string(packet, 2168, NET_IP_STR_LEN)?,
        myslots: packet[80..2128].to_vec(),
        gossip_count: read_u16(packet, 14)?,
        extension_count: read_u16(packet, 2214)?,
        cluster_state: packet[2252],
        message_flags: [packet[2253], packet[2254], packet[2255]],
    })
}

fn validate_node_id(value: &str, allow_empty: bool) -> Result<(), ClusterProtocolError> {
    if allow_empty && value.is_empty() {
        return Ok(());
    }
    NodeId::parse(value.as_bytes())
        .map(|_| ())
        .map_err(|error| ClusterProtocolError::InvalidNodeId(error.to_string()))
}

fn decode_node_id(field: &[u8], allow_empty: bool) -> Result<String, ClusterProtocolError> {
    if allow_empty && field.iter().all(|byte| *byte == 0) {
        return Ok(String::new());
    }
    NodeId::parse(field)
        .map(|node_id| node_id.to_string())
        .map_err(|error| ClusterProtocolError::InvalidNodeId(error.to_string()))
}

fn encode_optional_node_id(
    output: &mut Vec<u8>,
    node_id: &str,
) -> Result<(), ClusterProtocolError> {
    if node_id.is_empty() {
        output.extend_from_slice(&[0; 40]);
        return Ok(());
    }
    validate_node_id(node_id, false)?;
    output.extend_from_slice(node_id.as_bytes());
    Ok(())
}

pub fn read_u16(data: &[u8], offset: usize) -> Result<u16, ClusterProtocolError> {
    let bytes = data
        .get(offset..offset + 2)
        .ok_or(ClusterProtocolError::Incomplete {
            available: data.len(),
            needed: offset + 2,
        })?;
    Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
}

pub fn read_u32(data: &[u8], offset: usize) -> Result<u32, ClusterProtocolError> {
    let bytes = data
        .get(offset..offset + 4)
        .ok_or(ClusterProtocolError::Incomplete {
            available: data.len(),
            needed: offset + 4,
        })?;
    Ok(u32::from_be_bytes(
        bytes.try_into().expect("four-byte slice"),
    ))
}

pub fn read_u64(data: &[u8], offset: usize) -> Result<u64, ClusterProtocolError> {
    let bytes = data
        .get(offset..offset + 8)
        .ok_or(ClusterProtocolError::Incomplete {
            available: data.len(),
            needed: offset + 8,
        })?;
    Ok(u64::from_be_bytes(
        bytes.try_into().expect("eight-byte slice"),
    ))
}
