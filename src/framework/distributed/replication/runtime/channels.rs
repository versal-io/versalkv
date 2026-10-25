//! Primary/RDB channel registration, pairing, and teardown.
//! `ChannelRecord` owns connection-scoped facts. This module owns relationship
//! indexes and updates both sides atomically; PSYNC pins that can outlive a
//! transport remain in `psync_wait`.

use super::*;

impl ReplicationRuntime {
    pub fn register_primary(
        &mut self,
        connection_id: ConnectionId,
        replica: ReplicaConnectionInfo,
    ) -> ReplicaId {
        let previous = self
            .channel_records
            .get(&connection_id)
            .cloned()
            .unwrap_or_default();
        self.remove_connection(connection_id);
        let replica = apply_pending_metadata(replica, Some(previous.pending_replconf));
        let id = ReplicaId(self.next_replica_id);
        self.next_replica_id = self.next_replica_id.saturating_add(1);
        let mut link = PrimaryLink::new(connection_id, replica);
        link.set_replica_version(previous.replica_version);
        self.primary_links.insert(id, link);
        crate::server_log!(
            debug,
            "dual-rdb primary link registered: replica_id={:?} main={} active_links={:?}",
            id,
            connection_id.0,
            self.primary_links
                .iter()
                .map(|(id, link)| (*id, link.connection_id().0, link.state(), link.dual_state()))
                .collect::<Vec<_>>()
        );
        self.channel_records.insert(
            connection_id,
            ChannelRecord {
                role: Some(ReplicationConnection::Primary(id)),
                replica_node_id: previous.replica_node_id,
                replica_version: previous.replica_version,
                rdb_only: previous.rdb_only,
                ..ChannelRecord::default()
            },
        );
        id
    }
    /// Stamp the position C assigns with `listAddNodeTail(server.replicas, c)`
    /// when PSYNC is accepted (partial CONTINUE or full-resync start,
    /// including the dual-channel main PSYNC). REPLCONF-time registration order
    /// may differ from this acceptance order when several replicas connect
    /// concurrently. Idempotent for an already admitted link.
    pub(crate) fn stamp_primary_acceptance(&mut self, connection_id: ConnectionId) {
        let needs_stamp = self
            .primary_for_connection_mut(connection_id)
            .is_some_and(|link| link.accept_seq().is_none());
        if !needs_stamp {
            return;
        }
        let seq = self.next_accept_seq;
        self.next_accept_seq = self.next_accept_seq.saturating_add(1);
        if let Some(link) = self.primary_for_connection_mut(connection_id) {
            link.assign_accept_seq(seq);
        }
    }
    /// Register an RDB connection before its main channel is known. The
    /// connection id is the value later supplied by
    /// `REPLCONF set-rdb-client-id`.
    pub fn register_rdb_candidate(&mut self, connection_id: ConnectionId) -> Result<(), LinkError> {
        if self.role(connection_id).is_some() {
            return Err(LinkError::Protocol);
        }
        let record = self.channel_records.entry(connection_id).or_default();
        let pending = std::mem::take(&mut record.pending_replconf);
        record.role = Some(ReplicationConnection::RdbChannelCandidate);
        record.state = Some("wait_bgsave".to_string());
        record.endpoint = Some(ChannelEndpoint {
            ip: pending.ip.unwrap_or_default(),
            port: pending.port.unwrap_or_default(),
            capabilities: pending.capabilities.unwrap_or_default(),
            rdb_filter_mode: pending.rdb_filter_mode.unwrap_or_default(),
        });
        Ok(())
    }

    /// Convert a pre-PSYNC primary registration into an RDB-channel candidate.
    /// A replica may announce `listening-port` and `rdb-channel` in separate
    /// frames. The first frame temporarily creates a primary link, so retain
    /// its endpoint metadata when the second frame identifies the connection.
    pub fn convert_primary_to_rdb_candidate(
        &mut self,
        connection_id: ConnectionId,
    ) -> Result<(), LinkError> {
        let Some(ReplicationConnection::Primary(replica_id)) = self.role(connection_id) else {
            return Err(LinkError::Protocol);
        };
        let Some(link) = self.primary_links.get(&replica_id) else {
            return Err(LinkError::Protocol);
        };
        if !matches!(
            link.state(),
            PrimaryLinkState::AwaitingPsync | PrimaryLinkState::PreparingFullResync
        ) || self.channel_pairs.contains_key(&connection_id)
        {
            return Err(LinkError::Protocol);
        }
        let replica = link.replica().clone();
        self.primary_links.remove(&replica_id);
        let record = self.channel_records.entry(connection_id).or_default();
        record.role = Some(ReplicationConnection::RdbChannelCandidate);
        record.state = Some("wait_bgsave".to_string());
        record.endpoint = Some(ChannelEndpoint {
            ip: replica.ip,
            port: replica.port,
            capabilities: replica.capability,
            rdb_filter_mode: replica.rdb_filter_mode,
        });
        Ok(())
    }

    /// Register and associate an RDB channel when the owning main channel is
    /// already known to the caller.
    #[allow(dead_code)] // Used by the primary dual-channel setup in a later slice.
    pub fn register_rdb_channel(
        &mut self,
        connection_id: ConnectionId,
        main_connection_id: ConnectionId,
    ) -> Result<(), LinkError> {
        if connection_id == main_connection_id
            || !matches!(
                self.role(main_connection_id),
                Some(ReplicationConnection::Primary(_))
            )
            || self.channel_pairs.contains_key(&main_connection_id)
            || self.role(connection_id).is_some()
        {
            return Err(LinkError::Protocol);
        }
        self.register_rdb_candidate(connection_id)?;
        self.associate_rdb_channel(main_connection_id, connection_id)
    }

    /// Associate a previously registered RDB candidate with a primary link.
    pub fn associate_rdb_channel(
        &mut self,
        main_connection_id: ConnectionId,
        rdb_connection_id: ConnectionId,
    ) -> Result<(), LinkError> {
        if main_connection_id == rdb_connection_id
            || !matches!(
                self.role(main_connection_id),
                Some(ReplicationConnection::Primary(_))
            )
            || !matches!(
                self.role(rdb_connection_id),
                Some(ReplicationConnection::RdbChannelCandidate)
            )
            || self.channel_pairs.contains_key(&main_connection_id)
        {
            return Err(LinkError::Protocol);
        }
        let rdb_filter_mode = self
            .channel_records
            .get(&rdb_connection_id)
            .and_then(|record| record.endpoint.as_ref())
            .map(|endpoint| endpoint.rdb_filter_mode)
            .unwrap_or_default();
        self.channel_records
            .entry(rdb_connection_id)
            .or_default()
            .role = Some(ReplicationConnection::RdbChannel {
            main: main_connection_id,
        });
        self.channel_pairs
            .insert(main_connection_id, rdb_connection_id);
        self.channel_pairs
            .insert(rdb_connection_id, main_connection_id);
        let boundary = self
            .rdb_psync_waits
            .get_mut(&rdb_connection_id)
            .map(|wait| {
                wait.main = Some(main_connection_id);
                wait.boundary
            });
        if let Some(link) = self.primary_for_connection_mut(main_connection_id) {
            link.set_replica_rdb_filter_mode(rdb_filter_mode);
            if let Some(boundary) = boundary {
                link.record_dual_channel_endoff_boundary(boundary);
            }
        }
        Ok(())
    }

    /// After a successful pair, transition a waiting dual-channel main link
    /// to await its main-channel PSYNC. `$ENDOFF` was already sent to the RDB
    /// candidate before association.
    pub(crate) fn finalize_dual_channel_rdb_association(
        &mut self,
        main_connection_id: ConnectionId,
        rdb_connection_id: ConnectionId,
    ) -> Result<(), LinkError> {
        let Some(main_link) = self.primary_for_connection_mut(main_connection_id) else {
            return Ok(());
        };
        if !matches!(
            main_link.dual_state(),
            PrimaryDualChannelState::WaitingForRdbChannel
        ) {
            return Ok(());
        }
        if !matches!(
            self.role(rdb_connection_id),
            Some(ReplicationConnection::RdbChannel { main }) if main == main_connection_id
        ) {
            return Ok(());
        }
        if let Some(main_link) = self.primary_for_connection_mut(main_connection_id) {
            main_link.transition_to_waiting_for_main_psync()?;
        }
        Ok(())
    }

    #[allow(dead_code)] // Used by channel routing and INFO state in later slices.
    pub fn rdb_channel_for_main(&self, main: ConnectionId) -> Option<ConnectionId> {
        self.channel_pairs.get(&main).copied().filter(|rdb| {
            matches!(
                self.role(*rdb),
                Some(ReplicationConnection::RdbChannel { main: owner }) if owner == main
            )
        })
    }

    pub(crate) fn resolve_diskless_rdb_target(
        &self,
        main_connection_id: ConnectionId,
    ) -> ConnectionId {
        // A completed pair owns the snapshot on the dedicated RDB socket even
        // during the short main-link association transition. Sending `$EOF`
        // through the main socket corrupts its RESP command stream.
        if let Some(rdb_conn_id) = self.rdb_channel_for_main(main_connection_id) {
            return rdb_conn_id;
        }
        if let Some(link) = self.primary_for_connection(main_connection_id) {
            if matches!(
                link.dual_state(),
                crate::framework::distributed::replication::PrimaryDualChannelState::WaitingForMainPsync
                    | crate::framework::distributed::replication::PrimaryDualChannelState::WaitingForReplicaAck
            ) {
                if let Some(rdb_conn_id) = self.rdb_channel_for_main(main_connection_id) {
                    return rdb_conn_id;
                }
            }
        }
        main_connection_id
    }

    pub fn channel_pair_for(&self, connection_id: ConnectionId) -> Option<ConnectionId> {
        self.channel_pairs.get(&connection_id).copied()
    }

    /// Remove the association and return the other connection id. The caller
    /// is responsible for closing that transport; this method also removes
    /// both role entries so a later stale token cannot affect a replacement.
    pub fn remove_channel_pair(&mut self, connection_id: ConnectionId) -> Vec<ConnectionId> {
        let Some(other) = self.channel_pairs.remove(&connection_id) else {
            match self.role(connection_id) {
                Some(ReplicationConnection::Primary(replica_id)) => {
                    self.abort_psync_wait_for_main(connection_id);
                    self.primary_links.remove(&replica_id);
                }
                Some(ReplicationConnection::RdbChannelCandidate) => {
                    self.clear_endoff_snapshot_for_rdb_candidate(connection_id);
                }
                _ => {}
            }
            self.channel_records.remove(&connection_id);
            return Vec::new();
        };
        self.channel_pairs.remove(&other);
        for id in [connection_id, other] {
            if let Some(ReplicationConnection::Primary(replica_id)) = self.role(id) {
                self.primary_links.remove(&replica_id);
            }
        }
        self.channel_records.remove(&connection_id);
        self.channel_records.remove(&other);
        self.clear_endoff_snapshot_for_rdb_candidate(connection_id);
        self.clear_endoff_snapshot_for_rdb_candidate(other);
        vec![other]
    }

    pub fn detach_rdb_channel_preserving_main(
        &mut self,
        rdb_connection_id: ConnectionId,
    ) -> Option<ConnectionId> {
        let Some(ReplicationConnection::RdbChannel { main }) = self.role(rdb_connection_id) else {
            return None;
        };
        let wait = self.rdb_psync_waits.remove(&rdb_connection_id);
        self.channel_pairs.remove(&rdb_connection_id);
        self.channel_pairs.remove(&main);
        self.channel_records.remove(&rdb_connection_id);
        let pin = wait.and_then(|wait| wait.pin_cursor);
        crate::server_log!(
            debug,
            "dual-rdb RDB-first detach: rdb={} main={} pin={:?}",
            rdb_connection_id.0,
            main.0,
            pin
        );
        // C keeps the protected RDB client only until the paired main PSYNC
        // succeeds. After that point the RDB socket may close while the
        // terminal ACK is still pending, but it must not remain in
        // `replicas_waiting_psync`.
        if self.primary_for_connection(main).is_some_and(|link| {
            matches!(
                link.dual_state(),
                PrimaryDualChannelState::WaitingForMainPsync
            )
        }) {
            // The Rust socket is gone, so carry the C protected-client pin
            // under the paired main connection until grace expiry.
            self.grace_psync_pin_by_main.insert(
                main,
                GracePsyncWait {
                    deadline_ms: i64::MAX,
                    pin_cursor: pin,
                },
            );
        }
        Some(main)
    }

    pub fn set_rdb_only(&mut self, connection_id: ConnectionId, enabled: bool) {
        self.channel_records
            .entry(connection_id)
            .or_default()
            .rdb_only = enabled;
    }

    #[allow(dead_code)] // Exposed when the RDB channel state machine consumes the flag.
    pub fn rdb_only(&self, connection_id: ConnectionId) -> bool {
        self.channel_records
            .get(&connection_id)
            .is_some_and(|record| record.rdb_only)
    }

    pub fn clear_rdb_channel(&mut self, connection_id: ConnectionId) -> Vec<ConnectionId> {
        crate::server_log!(
            debug,
            "dual-rdb clear channel: connection={} role={:?} primary_links={:?}",
            connection_id.0,
            self.role(connection_id),
            self.primary_links
                .values()
                .map(|link| (link.connection_id().0, link.state(), link.dual_state()))
                .collect::<Vec<_>>()
        );
        match self.role(connection_id) {
            Some(ReplicationConnection::RdbChannelCandidate) => {
                self.clear_endoff_snapshot_for_rdb_candidate(connection_id);
                self.channel_records.remove(&connection_id);
                Vec::new()
            }
            Some(ReplicationConnection::RdbChannel { main }) => {
                if let Some(main_link) = self.primary_for_connection_mut(main) {
                    main_link.clear_dual_channel_endoff_boundary();
                }
                self.clear_endoff_snapshot_for_rdb_candidate(connection_id);
                self.channel_pairs.remove(&connection_id);
                self.channel_pairs.remove(&main);
                self.channel_records.remove(&connection_id);
                vec![connection_id]
            }
            Some(ReplicationConnection::Primary(id)) => {
                let Some(rdb) = self.rdb_channel_for_main(connection_id) else {
                    if let Some(record) = self.channel_records.get_mut(&connection_id) {
                        record.rdb_only = false;
                    }
                    return Vec::new();
                };
                self.clear_endoff_snapshot_for_rdb_candidate(rdb);
                self.channel_pairs.remove(&connection_id);
                self.channel_pairs.remove(&rdb);
                self.channel_records.remove(&rdb);
                self.channel_records.entry(connection_id).or_default().role =
                    Some(ReplicationConnection::Primary(id));
                vec![rdb]
            }
            _ => Vec::new(),
        }
    }
    pub fn role(&self, connection_id: ConnectionId) -> Option<ReplicationConnection> {
        self.channel_records
            .get(&connection_id)
            .and_then(|record| record.role)
    }

    pub fn primary_id(&self, connection_id: ConnectionId) -> Option<ReplicaId> {
        match self.role(connection_id) {
            Some(ReplicationConnection::Primary(id)) => Some(id),
            _ => None,
        }
    }

    pub fn replica_id_for_connection(&self, connection_id: ConnectionId) -> Option<ReplicaId> {
        match self.role(connection_id) {
            Some(ReplicationConnection::Primary(id)) => Some(id),
            Some(ReplicationConnection::RdbChannel { main }) => self.primary_id(main),
            _ => None,
        }
    }

    pub fn primary_for_connection(&self, connection_id: ConnectionId) -> Option<&PrimaryLink> {
        self.primary_link(self.primary_id(connection_id)?)
    }

    pub fn primary_for_connection_mut(
        &mut self,
        connection_id: ConnectionId,
    ) -> Option<&mut PrimaryLink> {
        let id = self.primary_id(connection_id)?;
        self.primary_link_mut(id)
    }

    pub fn primary_link(&self, id: ReplicaId) -> Option<&PrimaryLink> {
        self.primary_links.get(&id)
    }

    pub fn primary_link_mut(&mut self, id: ReplicaId) -> Option<&mut PrimaryLink> {
        self.primary_links.get_mut(&id)
    }

    pub fn primary_links(&self) -> &HashMap<ReplicaId, PrimaryLink> {
        &self.primary_links
    }
}
