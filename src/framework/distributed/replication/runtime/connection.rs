//! Per-connection replication metadata.
//! `ChannelRecord` is the single source of truth for state whose lifetime is
//! bound to one transport connection. Pairing and grace waits intentionally
//! live elsewhere because they describe relationships or outlive a connection.

use super::ReplicationRuntime;
use crate::foundation::io::persistence::rdb::compression::{RdbCompression, RdbSnapshotOptions};
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
    pub(super) snapshot_options: Option<RdbSnapshotOptions>,
    /// Physical RDB transport completion may precede main-channel pairing.
    pub(super) rdb_snapshot_complete: bool,
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
    pub fn full_sync_snapshot_options(
        &self,
        connection_id: ConnectionId,
        mode: &str,
        disk: RdbCompression,
        checksum: bool,
    ) -> Option<RdbSnapshotOptions> {
        if let Some(options) = self
            .channel_records
            .get(&connection_id)
            .and_then(|record| record.snapshot_options)
        {
            return Some(options);
        }
        let capabilities = self
            .primary_for_connection(connection_id)
            .map(|link| link.replica().capability.as_slice())
            .or_else(|| self.channel_capabilities(connection_id))
            .or_else(|| {
                self.channel_records
                    .get(&connection_id)
                    .and_then(|record| record.pending_replconf.capabilities.as_deref())
            })
            .unwrap_or_default();
        let eof = capabilities
            .iter()
            .any(|value| value.eq_ignore_ascii_case("eof"));
        let accepts_lz4 = capabilities
            .iter()
            .any(|value| value.eq_ignore_ascii_case("lz4"));
        let compression = if eof {
            if crate::engine::distributed::replication::protocol::negotiate_lz4(mode, capabilities)
            {
                RdbCompression::Lz4
            } else {
                if disk.uses_lzf() {
                    disk
                } else {
                    RdbCompression::No
                }
            }
        } else if disk == RdbCompression::Lz4 {
            if !accepts_lz4 {
                return None;
            }
            disk
        } else {
            disk
        };
        Some(RdbSnapshotOptions {
            compression,
            checksum,
        })
    }

    pub fn freeze_full_sync_snapshot_options(
        &mut self,
        connection_id: ConnectionId,
        options: RdbSnapshotOptions,
    ) {
        self.channel_records
            .entry(connection_id)
            .or_default()
            .snapshot_options = Some(options);
    }

    pub fn replica_snapshot_version(&self, connection_id: ConnectionId) -> u32 {
        self.primary_for_connection(connection_id).map_or_else(
            || {
                self.channel_records
                    .get(&connection_id)
                    .map_or(0, |record| record.replica_version)
            },
            |link| link.replica_version(),
        )
    }

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

    pub fn replica_sync_preferences(&self, connection_id: ConnectionId) -> (RdbFilterMode, bool) {
        let (filter_mode, capabilities) =
            if let Some(link) = self.primary_for_connection(connection_id) {
                (
                    link.replica().rdb_filter_mode,
                    link.replica().capability.as_slice(),
                )
            } else if let Some(record) = self.channel_records.get(&connection_id) {
                if let Some(endpoint) = record.endpoint.as_ref() {
                    (endpoint.rdb_filter_mode, endpoint.capabilities.as_slice())
                } else if record.role.is_none() {
                    (
                        record.pending_replconf.rdb_filter_mode.unwrap_or_default(),
                        record
                            .pending_replconf
                            .capabilities
                            .as_deref()
                            .unwrap_or_default(),
                    )
                } else {
                    (RdbFilterMode::All, &[][..])
                }
            } else {
                (RdbFilterMode::All, &[][..])
            };
        (
            filter_mode,
            capabilities
                .iter()
                .any(|capability| capability.eq_ignore_ascii_case("eof")),
        )
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
