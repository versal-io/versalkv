//! Cluster node representation.
//! Each cluster node has a unique 40-character node ID, a shard ID,
//! flags, config epoch, IP/port, and slot assignment.

use super::endpoint::{ClusterAddress, ClusterEndpoints};
use crate::engine::distributed::cluster::msg::SLOT_BYTES;
use std::ops::Deref;

/// Node flags (matching C cluster.h CLUSTER_NODE_*).
pub const CLUSTER_NODE_PRIMARY: u16 = 1 << 0;
pub const CLUSTER_NODE_REPLICA: u16 = 1 << 1;
pub const CLUSTER_NODE_PFAIL: u16 = 1 << 2;
pub const CLUSTER_NODE_FAIL: u16 = 1 << 3;
pub const CLUSTER_NODE_MYSELF: u16 = 1 << 4;
pub const CLUSTER_NODE_HANDSHAKE: u16 = 1 << 5;
pub const CLUSTER_NODE_NOADDR: u16 = 1 << 6;
/// This primary is eligible to receive a migrated replica. The bit is carried
/// on the cluster bus but intentionally omitted from CLUSTER NODES/config.
pub const CLUSTER_NODE_MIGRATE_TO: u16 = 1 << 8;
pub const CLUSTER_NODE_NOFAILOVER: u16 = 1 << 9;
pub const CLUSTER_NODE_EXTENSIONS_SUPPORTED: u16 = 1 << 10;
#[cfg(test)]
pub const CLUSTER_NODE_LIGHT_HDR_PUBLISH_SUPPORTED: u16 = 1 << 11;
#[cfg(test)]
pub const CLUSTER_NODE_LIGHT_HDR_MODULE_SUPPORTED: u16 = 1 << 12;
/// This replica currently considers its own primary to be in FAIL state.
pub const CLUSTER_NODE_MY_PRIMARY_FAIL: u16 = 1 << 13;
/// This peer understands negative failover authorization replies.
pub const CLUSTER_NODE_FAILOVER_AUTH_NACK_SUPPORTED: u16 = 1 << 14;

/// A node in the cluster.
#[derive(Clone)]
pub struct ClusterNode {
    /// 40-char lowercase alphanumeric node ID.
    pub name: String,
    /// 40-char hex shard ID.
    pub shard_id: String,
    /// Node flags (bitmask of CLUSTER_NODE_*).
    pub flags: u16,
    /// Config epoch for this node.
    pub config_epoch: u64,
    /// Last advertised replication offset.
    pub repl_offset: i64,
    /// Ordinary replication priority, independent of cluster elections.
    pub replica_priority: i64,
    /// Cluster election tie-breaker. Lower values rank first, including zero.
    pub cluster_replica_priority: u32,
    /// Slot bitmap: 16384 bits (2048 bytes).
    pub slots: Vec<bool>,
    /// Last compact bitmap received directly from this node. Stable cluster
    /// heartbeats compare this C-compatible representation before expanding
    /// individual slots.
    pub advertised_slots: Vec<u8>,
    pub has_advertised_slots: bool,
    /// Number of slots handled.
    pub numslots: usize,
    /// Validated advertised networking metadata.
    pub endpoints: ClusterEndpoints,
    /// Primary node name (empty if this is a primary).
    pub replicaof: String,
    /// Ping sent timestamp (ms).
    pub ping_sent: i64,
    /// Pong received timestamp (ms).
    pub pong_received: i64,
    /// Time when the node entered FAIL state (ms).
    pub fail_time: i64,
    /// Last time any valid Cluster Bus frame was received from this node.
    pub data_received: i64,
    /// Node creation time, matching C clusterNode.ctime.
    pub created_at: i64,
    /// Whether an explicitly requested MEET should reconnect after timeout.
    pub handshake_retry_on_timeout: bool,
    /// Start time for a node learned from an unconfirmed MEET gossip section.
    pub asymmetric_meet_started_at: i64,
    /// Whether this normal node still needs a reciprocal MEET. This follows
    /// C's node-level CLUSTER_NODE_MEET state rather than the lifetime of any
    /// individual bus connection.
    pub meet_pending: bool,
    /// Last time a MEET was queued for this normal node (ms). This is the
    /// node-level retry cooldown, so reconnecting links retain it.
    pub meet_sent_at: i64,
    /// When the known node most recently lacked a confirmed inbound link.
    /// This carries C's `inbound_link_freed_time` across runtime sockets.
    pub inbound_link_missing_since: i64,
    /// Whether a confirmed inbound link has existed since this node became
    /// known. Only a later loss starts the reciprocal-MEET timer.
    pub has_seen_inbound_link: bool,
}

impl Deref for ClusterNode {
    type Target = ClusterAddress;

    fn deref(&self) -> &Self::Target {
        self.endpoints.address()
    }
}

impl ClusterNode {
    /// Create a new cluster node with a generated ID.
    pub fn new(name: String, ip: String, port: u16, cport: u16, is_primary: bool) -> Self {
        let mut flags = CLUSTER_NODE_MYSELF;
        if is_primary {
            flags |= CLUSTER_NODE_PRIMARY;
        } else {
            flags |= CLUSTER_NODE_REPLICA;
        }
        let shard_id = if is_primary {
            name.clone()
        } else {
            String::new()
        };
        Self {
            name,
            shard_id,
            flags,
            config_epoch: 0,
            repl_offset: 0,
            replica_priority: 100,
            cluster_replica_priority: 0,
            slots: vec![false; 16384],
            advertised_slots: vec![0; SLOT_BYTES],
            has_advertised_slots: false,
            numslots: 0,
            endpoints: ClusterEndpoints::new(ip, port, cport),
            replicaof: String::new(),
            ping_sent: 0,
            pong_received: 0,
            fail_time: 0,
            data_received: 0,
            created_at: crate::foundation::io::sys::time::mstime(),
            handshake_retry_on_timeout: false,
            asymmetric_meet_started_at: 0,
            meet_pending: false,
            meet_sent_at: 0,
            inbound_link_missing_since: 0,
            has_seen_inbound_link: false,
        }
    }

    /// Check if this node has a specific flag set.
    pub fn has_flag(&self, flag: u16) -> bool {
        self.flags & flag != 0
    }

    /// Check if this node is myself.
    pub fn is_myself(&self) -> bool {
        self.has_flag(CLUSTER_NODE_MYSELF)
    }

    /// Check if this node is a primary.
    pub fn is_primary(&self) -> bool {
        self.has_flag(CLUSTER_NODE_PRIMARY)
    }

    /// Check if this node is a replica.
    pub fn is_replica(&self) -> bool {
        self.has_flag(CLUSTER_NODE_REPLICA)
    }

    /// Count assigned slots.
    pub fn count_slots(&self) -> usize {
        self.slots.iter().filter(|&&s| s).count()
    }

    pub fn ip(&self) -> &str {
        self.endpoints.ip()
    }

    pub fn port(&self) -> u16 {
        self.endpoints.port()
    }

    pub fn cport(&self) -> u16 {
        self.endpoints.bus_port()
    }
}

/// Generate a 40-char hex node ID (matching C cluster.c's createClusterNode()).
pub fn generate_node_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut rng = ts as u64;
    let mut id = String::with_capacity(40);
    for _ in 0..40 {
        rng = rng
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let nibble = ((rng >> 32) & 0x0f) as u8;
        id.push(hex_char(nibble));
    }
    id
}

fn hex_char(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        _ => (b'a' + nibble - 10) as char,
    }
}

/// Compute CRC16 XMODEM for key hash slot.
pub fn key_hash_slot(key: &[u8]) -> u16 {
    let s = key.iter().position(|&b| b == b'{');
    let s = match s {
        Some(idx) => idx,
        None => return crc16(key) & 0x3FFF,
    };
    let e = key[s + 1..].iter().position(|&b| b == b'}');
    let e = match e {
        Some(idx) => s + 1 + idx,
        None => return crc16(key) & 0x3FFF,
    };
    if e == s + 1 {
        return crc16(key) & 0x3FFF;
    }
    crc16(&key[s + 1..e]) & 0x3FFF
}

/// If it can be inferred that a glob-style pattern only matches keys belonging
/// to a single slot, return `Some(slot)`. Otherwise return `None`.
pub fn pattern_hash_slot(pattern: &[u8]) -> Option<u16> {
    let mut s: Option<usize> = None;
    for (i, &c) in pattern.iter().enumerate() {
        if c == b'*' || c == b'?' || c == b'[' || c == b'\\' {
            return None;
        } else if s.is_none() && c == b'{' {
            s = Some(i);
        } else if let Some(start) = s {
            if c == b'}' && i == start + 1 {
                s = Some(usize::MAX);
            } else if c == b'}' && start != usize::MAX {
                return Some(crc16(&pattern[start + 1..i]) & 0x3FFF);
            }
        }
    }
    Some(crc16(pattern) & 0x3FFF)
}

fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        crc ^= (byte as u16) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::ClusterNode;

    #[test]
    fn primary_node_defaults_shard_id_to_node_id() {
        let node = ClusterNode::new(
            "primary-node".to_string(),
            "127.0.0.1".to_string(),
            6379,
            16379,
            true,
        );

        assert_eq!(node.shard_id, node.name);
    }

    #[test]
    fn replica_node_starts_without_shard_id() {
        let node = ClusterNode::new(
            "replica-node".to_string(),
            "127.0.0.1".to_string(),
            6380,
            16380,
            false,
        );

        assert!(node.shard_id.is_empty());
    }
}
