use super::header::{read_u16, read_u32, read_u64, ClusterProtocolError};
use crate::engine::distributed::cluster::endpoint::NodeId;
use crate::engine::distributed::cluster::node::ClusterNode;
use crate::engine::distributed::cluster::ClusterEndpoints;

const EXT_HEADER_LEN: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterExtension {
    Hostname(String),
    HumanNodeName(String),
    ForgottenNode { node_id: NodeId, ttl_seconds: u64 },
    ShardId(NodeId),
    ClientIpv4(String),
    ClientIpv6(String),
    ClientPort(u16),
    ClientTlsPort(u16),
    AvailabilityZone(String),
    ReplicaPriority(u32),
    Unknown { kind: u16, payload: Vec<u8> },
}

impl ClusterExtension {
    pub(super) fn encode(&self, output: &mut Vec<u8>) -> Result<(), ClusterProtocolError> {
        let (kind, payload) = self.payload()?;
        let aligned_payload_len = align_eight(payload.len())?;
        let total_len = EXT_HEADER_LEN
            .checked_add(aligned_payload_len)
            .ok_or(ClusterProtocolError::LengthOverflow)?;
        output.extend_from_slice(
            &u32::try_from(total_len)
                .map_err(|_| ClusterProtocolError::LengthOverflow)?
                .to_be_bytes(),
        );
        output.extend_from_slice(&kind.to_be_bytes());
        output.extend_from_slice(&0u16.to_be_bytes());
        output.extend_from_slice(&payload);
        output.resize(output.len() + aligned_payload_len - payload.len(), 0);
        Ok(())
    }

    fn payload(&self) -> Result<(u16, Vec<u8>), ClusterProtocolError> {
        let value = match self {
            Self::Hostname(value) => (0, nul_terminated(value)),
            Self::HumanNodeName(value) => (1, nul_terminated(value)),
            Self::ForgottenNode {
                node_id,
                ttl_seconds,
            } => {
                let mut payload = node_id.as_str().as_bytes().to_vec();
                payload.extend_from_slice(&ttl_seconds.to_be_bytes());
                (2, payload)
            }
            Self::ShardId(node_id) => (3, node_id.as_str().as_bytes().to_vec()),
            Self::ClientIpv4(value) => (4, nul_terminated(value)),
            Self::ClientIpv6(value) => (5, nul_terminated(value)),
            Self::ClientPort(port) => (6, port.to_be_bytes().to_vec()),
            Self::ClientTlsPort(port) => (7, port.to_be_bytes().to_vec()),
            Self::AvailabilityZone(value) => (8, nul_terminated(value)),
            Self::ReplicaPriority(priority) => (9, priority.to_be_bytes().to_vec()),
            Self::Unknown { kind, payload } => (*kind, payload.clone()),
        };
        Ok(value)
    }
}

pub(super) fn decode_extensions(
    packet: &[u8],
    mut offset: usize,
    count: u16,
) -> Result<(Vec<ClusterExtension>, usize), ClusterProtocolError> {
    let mut extensions = Vec::with_capacity(usize::from(count));
    for _ in 0..count {
        if packet.len().saturating_sub(offset) < EXT_HEADER_LEN {
            return Err(ClusterProtocolError::InvalidExtension(
                "truncated extension header".to_string(),
            ));
        }
        let length = read_u32(packet, offset)? as usize;
        if length < EXT_HEADER_LEN || !length.is_multiple_of(8) {
            return Err(ClusterProtocolError::InvalidExtension(format!(
                "extension length {length} is not eight-byte aligned"
            )));
        }
        let end = offset
            .checked_add(length)
            .ok_or(ClusterProtocolError::LengthOverflow)?;
        if end > packet.len() {
            return Err(ClusterProtocolError::InvalidExtension(
                "extension exceeds packet length".to_string(),
            ));
        }
        let kind = read_u16(packet, offset + 4)?;
        let payload = &packet[offset + EXT_HEADER_LEN..end];
        extensions.push(decode_extension(kind, payload)?);
        offset = end;
    }
    Ok((extensions, offset))
}

fn decode_extension(kind: u16, payload: &[u8]) -> Result<ClusterExtension, ClusterProtocolError> {
    Ok(match kind {
        0 => {
            let value = decode_c_string(payload)?;
            ClusterEndpoints::validate_hostname(value.as_bytes())
                .map_err(|error| ClusterProtocolError::InvalidExtension(error.to_string()))?;
            ClusterExtension::Hostname(value)
        }
        1 => {
            let value = decode_c_string(payload)?;
            ClusterEndpoints::validate_aux_value(value.as_bytes())
                .map_err(|error| ClusterProtocolError::InvalidExtension(error.to_string()))?;
            ClusterExtension::HumanNodeName(value)
        }
        2 => {
            require_payload(payload, 48, "forgotten-node")?;
            let node_id = NodeId::parse(&payload[..40])
                .map_err(|error| ClusterProtocolError::InvalidNodeId(error.to_string()))?;
            ClusterExtension::ForgottenNode {
                node_id,
                ttl_seconds: read_u64(payload, 40)?,
            }
        }
        3 => {
            require_payload(payload, 40, "shard-id")?;
            ClusterExtension::ShardId(
                NodeId::parse(&payload[..40])
                    .map_err(|error| ClusterProtocolError::InvalidNodeId(error.to_string()))?,
            )
        }
        4 => {
            let value = decode_c_string(payload)?;
            ClusterEndpoints::validate_ipv4(value.as_bytes())
                .map_err(|error| ClusterProtocolError::InvalidExtension(error.to_string()))?;
            ClusterExtension::ClientIpv4(value)
        }
        5 => {
            let value = decode_c_string(payload)?;
            ClusterEndpoints::validate_ipv6(value.as_bytes())
                .map_err(|error| ClusterProtocolError::InvalidExtension(error.to_string()))?;
            ClusterExtension::ClientIpv6(value)
        }
        6 => {
            require_payload(payload, 8, "client-port")?;
            ClusterExtension::ClientPort(read_u16(payload, 0)?)
        }
        7 => {
            require_payload(payload, 8, "client-tls-port")?;
            ClusterExtension::ClientTlsPort(read_u16(payload, 0)?)
        }
        8 => ClusterExtension::AvailabilityZone(decode_c_string(payload)?),
        9 => {
            require_payload(payload, 8, "replica-priority")?;
            ClusterExtension::ReplicaPriority(read_u32(payload, 0)?)
        }
        _ => ClusterExtension::Unknown {
            kind,
            payload: payload.to_vec(),
        },
    })
}

fn require_payload(
    payload: &[u8],
    expected: usize,
    name: &str,
) -> Result<(), ClusterProtocolError> {
    if payload.len() != expected {
        return Err(ClusterProtocolError::InvalidExtension(format!(
            "{name} extension payload must be {expected} bytes"
        )));
    }
    Ok(())
}

fn decode_c_string(payload: &[u8]) -> Result<String, ClusterProtocolError> {
    let end = payload.iter().position(|byte| *byte == 0).ok_or_else(|| {
        ClusterProtocolError::InvalidExtension("string extension is not NUL terminated".into())
    })?;
    std::str::from_utf8(&payload[..end])
        .map(str::to_string)
        .map_err(|_| ClusterProtocolError::InvalidUtf8)
}

fn nul_terminated(value: &str) -> Vec<u8> {
    let mut payload = value.as_bytes().to_vec();
    payload.push(0);
    payload
}

fn align_eight(length: usize) -> Result<usize, ClusterProtocolError> {
    length
        .checked_add(7)
        .map(|value| value / 8 * 8)
        .ok_or(ClusterProtocolError::LengthOverflow)
}

pub fn extensions_for_node(node: &ClusterNode) -> Vec<ClusterExtension> {
    let mut extensions = Vec::new();
    if let Some(value) = node.endpoints.hostname() {
        extensions.push(ClusterExtension::Hostname(value.to_string()));
    }
    if let Some(value) = node.endpoints.human_name() {
        extensions.push(ClusterExtension::HumanNodeName(value.to_string()));
    }
    if let Some(value) = node.endpoints.client_ipv4() {
        extensions.push(ClusterExtension::ClientIpv4(value.to_string()));
    }
    if let Some(value) = node.endpoints.client_ipv6() {
        extensions.push(ClusterExtension::ClientIpv6(value.to_string()));
    }
    if node.endpoints.client_port() != 0 {
        extensions.push(ClusterExtension::ClientPort(node.endpoints.client_port()));
    }
    if node.endpoints.client_tls_port() != 0 {
        extensions.push(ClusterExtension::ClientTlsPort(
            node.endpoints.client_tls_port(),
        ));
    }
    if let Some(value) = node.endpoints.availability_zone() {
        extensions.push(ClusterExtension::AvailabilityZone(value.to_string()));
    }
    if let Ok(shard_id) = NodeId::parse(node.shard_id.as_bytes()) {
        extensions.push(ClusterExtension::ShardId(shard_id));
    }
    if node.cluster_replica_priority != 0 {
        extensions.push(ClusterExtension::ReplicaPriority(
            node.cluster_replica_priority,
        ));
    }
    extensions
}
