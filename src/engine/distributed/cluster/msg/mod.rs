//! C-compatible Valkey Cluster Bus codec.

mod body;
mod extension;
mod header;

#[cfg(test)]
mod tests;

use std::ops::Deref;

#[cfg(test)]
pub(crate) use body::build_gossip_entries;
pub use body::{
    build_rotating_gossip_entries, bytes_to_slots, slots_to_bytes, ClusterBusBody, ClusterPublish,
    GossipEntry, SlotOwnershipUpdate, SLOT_BYTES,
};
pub use extension::ClusterExtension;
pub use header::{
    ClusterBusHeader, ClusterMsgType, ClusterProtocolError, CLUSTER_BUS_HEADER_LEN, NET_IP_STR_LEN,
};

pub struct ClusterMessageFlags;

impl ClusterMessageFlags {
    pub const PAUSED: u8 = 1 << 0;
    pub const FORCE_ACK: u8 = 1 << 1;
    pub const EXT_DATA: u8 = 1 << 2;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterBusMessage {
    pub header: ClusterBusHeader,
    pub body: ClusterBusBody,
    pub extensions: Vec<ClusterExtension>,
}

impl Deref for ClusterBusMessage {
    type Target = ClusterBusHeader;

    fn deref(&self) -> &Self::Target {
        &self.header
    }
}

impl ClusterBusMessage {
    pub fn gossip(&self) -> &[GossipEntry] {
        match &self.body {
            ClusterBusBody::Gossip(entries) => entries,
            _ => &[],
        }
    }

    pub fn topology_node_id(&self) -> &str {
        match &self.body {
            ClusterBusBody::Update(update) => update.node_id.as_str(),
            _ => &self.header.sender_id,
        }
    }

    pub fn topology_config_epoch(&self) -> u64 {
        match &self.body {
            ClusterBusBody::Update(update) => update.config_epoch,
            _ => self.header.config_epoch,
        }
    }

    pub fn topology_slots(&self) -> &[u8] {
        match &self.body {
            ClusterBusBody::Update(update) => &update.slots,
            _ => &self.header.myslots,
        }
    }
}

pub type ClusterMsg = ClusterBusMessage;

pub fn parse_msg(data: &[u8]) -> Result<(ClusterBusMessage, usize), ClusterProtocolError> {
    ClusterBusMessage::decode(data)
}

#[cfg(test)]
pub(crate) fn encode_msg(
    msg_type: ClusterMsgType,
    sender: &crate::engine::distributed::cluster::node::ClusterNode,
    current_epoch: u64,
    gossip: &[GossipEntry],
) -> Vec<u8> {
    let body = if matches!(
        msg_type,
        ClusterMsgType::Ping | ClusterMsgType::Pong | ClusterMsgType::Meet
    ) {
        ClusterBusBody::Gossip(gossip.to_vec())
    } else {
        ClusterBusBody::Empty
    };
    ClusterBusMessage::from_node(msg_type, sender, current_epoch, body)
        .encode()
        .expect("cluster state must contain valid wire metadata")
}
