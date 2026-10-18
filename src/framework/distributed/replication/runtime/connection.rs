//! Per-connection replication metadata.
//! `ChannelRecord` is the single source of truth for state whose lifetime is
//! bound to one transport connection. Pairing and grace waits intentionally
//! live elsewhere because they describe relationships or outlive a connection.

use super::ReplicationRuntime;
use crate::framework::distributed::replication::{
    ConnectionId, RdbFilterMode, ReplicaConnectionInfo, ReplicationConnection,
};

#[derive(Clone, Default)]
pub(super) struct ChannelRecord {
    pub(super) role: Option<ReplicationConnection>,
    pub(super) endpoint: Option<ChannelEndpoint>,
    pub(super) pending_replconf: PendingReplconfMetadata,
    pub(super) state: Option<String>,
    pub(super) replica_node_id: Option<String>,
    pub(super) replica_version: u32,
    pub(super) rdb_only: bool,
}

#[derive(Clone, Default)]
pub(super) struct ChannelEndpoint {
    pub(super) ip: String,
    pub(super) port: u16,
    pub(super) capabilities: Vec<String>,
    pub(super) rdb_filter_mode: RdbFilterMode,
}

#[derive(Clone, Default)]
pub(super) struct PendingReplconfMetadata {
    pub(super) ip: Option<String>,
    pub(super) port: Option<u16>,
    pub(super) capabilities: Option<Vec<String>>,
    pub(super) rdb_filter_mode: Option<RdbFilterMode>,
}

impl ReplicationRuntime {
    pub fn set_channel_endpoint(&mut self, connection_id: ConnectionId, ip: String, port: u16) {
        let endpoint = self
            .channel_records
            .entry(connection_id)
            .or_default()
            .endpoint
            .get_or_insert_with(ChannelEndpoint::default);
        endpoint.ip = ip;
        endpoint.port = port;
    }

    pub fn add_channel_capability(&mut self, connection_id: ConnectionId, capability: String) {
        if let Some(endpoint) = self
            .channel_records
            .get_mut(&connection_id)
            .and_then(|record| record.endpoint.as_mut())
        {
            endpoint.capabilities.push(capability);
        }
    }

    pub fn channel_endpoint(&self, connection_id: ConnectionId) -> Option<(&str, u16)> {
        self.channel_records
            .get(&connection_id)
            .and_then(|record| record.endpoint.as_ref())
            .map(|endpoint| (endpoint.ip.as_str(), endpoint.port))
    }

    #[allow(dead_code)] // Consumed by the Task 3 channel state machine.
    pub fn channel_capabilities(&self, connection_id: ConnectionId) -> Option<&[String]> {
        self.channel_records
            .get(&connection_id)
            .and_then(|record| record.endpoint.as_ref())
            .map(|endpoint| endpoint.capabilities.as_slice())
    }

    pub fn stage_replconf_ip(&mut self, connection_id: ConnectionId, ip: String) {
        self.channel_records
            .entry(connection_id)
            .or_default()
            .pending_replconf
            .ip = Some(ip);
    }

    pub fn stage_replconf_port(&mut self, connection_id: ConnectionId, port: u16) {
        self.channel_records
            .entry(connection_id)
            .or_default()
            .pending_replconf
            .port = Some(port);
    }

    pub fn stage_replconf_rdb_filter_only(
        &mut self,
        connection_id: ConnectionId,
        mode: RdbFilterMode,
    ) {
        if matches!(
            self.role(connection_id),
            Some(
                ReplicationConnection::RdbChannelCandidate
                    | ReplicationConnection::RdbChannel { .. }
            )
        ) {
            self.channel_records
                .entry(connection_id)
                .or_default()
                .endpoint
                .get_or_insert_with(ChannelEndpoint::default)
                .rdb_filter_mode = mode;
            if let Some(ReplicationConnection::RdbChannel { main }) = self.role(connection_id) {
                if let Some(link) = self.primary_for_connection_mut(main) {
                    link.set_replica_rdb_filter_mode(mode);
                }
            }
        } else {
            self.channel_records
                .entry(connection_id)
                .or_default()
                .pending_replconf
                .rdb_filter_mode = Some(mode);
        }
    }

    pub fn stage_replconf_capability(&mut self, connection_id: ConnectionId, capability: String) {
        self.channel_records
            .entry(connection_id)
            .or_default()
            .pending_replconf
            .capabilities
            .get_or_insert_with(Vec::new)
            .push(capability);
    }

    #[allow(dead_code)] // Consumed by the Task 3 channel state machine.
    pub fn set_channel_state(&mut self, connection_id: ConnectionId, state: String) {
        self.channel_records.entry(connection_id).or_default().state = Some(state);
    }

    pub fn set_replica_node_id(&mut self, connection_id: ConnectionId, node_id: String) {
        self.channel_records
            .entry(connection_id)
            .or_default()
            .replica_node_id = Some(node_id);
    }

    pub fn set_replica_version(&mut self, connection_id: ConnectionId, version: u32) {
        self.channel_records
            .entry(connection_id)
            .or_default()
            .replica_version = version;
    }

    pub fn replica_node_id(&self, connection_id: ConnectionId) -> Option<&str> {
        self.channel_records
            .get(&connection_id)
            .and_then(|record| record.replica_node_id.as_deref())
    }
}

pub(super) fn apply_pending_metadata(
    mut replica: ReplicaConnectionInfo,
    pending: Option<PendingReplconfMetadata>,
) -> ReplicaConnectionInfo {
    let Some(pending) = pending else {
        return replica;
    };
    if let Some(ip) = pending.ip {
        replica.ip = ip;
    }
    if let Some(port) = pending.port {
        replica.port = port;
    }
    if let Some(capabilities) = pending.capabilities {
        replica.capability = capabilities;
    }
    if let Some(rdb_filter_mode) = pending.rdb_filter_mode {
        replica.rdb_filter_mode = rdb_filter_mode;
    }
    replica
}
