//! Upstream replica-session lifecycle and reconnect state.
//! The active `ReplicaLink` is the authoritative session. Cached resume state
//! survives only the failure causes defined here; L5 remains responsible for
//! opening and closing its transport.

use super::*;
use crate::framework::distributed::replication::RdbFilterMode;

impl ReplicationRuntime {
    pub fn set_replica_link(&mut self, link: ReplicaLink) {
        let connection_id = link.connection_id();
        let replaced_connection = self
            .replica_link
            .as_ref()
            .map(ReplicaLink::connection_id)
            .filter(|old| *old != connection_id);
        if let Some(old) = replaced_connection {
            self.remove_connection(old);
        }
        if !matches!(
            self.role(connection_id),
            Some(ReplicationConnection::Replica)
        ) {
            self.remove_connection(connection_id);
        }
        self.channel_records.entry(connection_id).or_default().role =
            Some(ReplicationConnection::Replica);
        self.replica_link = Some(link);
    }
    pub fn remove_connection(&mut self, connection_id: ConnectionId) {
        let role = self.role(connection_id);
        if matches!(role, Some(ReplicationConnection::Primary(_)))
            || self
                .primary_for_connection(connection_id)
                .is_some_and(|link| !matches!(link.dual_state(), PrimaryDualChannelState::Disabled))
        {
            crate::server_log!(
                debug,
                "dual-rdb runtime remove: connection={} role={:?} primary_links={:?}",
                connection_id.0,
                role,
                self.primary_links
                    .values()
                    .map(|link| (link.connection_id().0, link.state(), link.dual_state()))
                    .collect::<Vec<_>>()
            );
        }
        if matches!(role, Some(ReplicationConnection::Primary(_))) {
            self.clear_psync_wait_for_main(connection_id);
        }
        self.clear_endoff_snapshot_for_rdb_candidate(connection_id);
        self.channel_records.remove(&connection_id);

        let is_replica_side_main = matches!(role, Some(ReplicationConnection::Replica))
            || self
                .replica_link
                .as_ref()
                .is_some_and(|link| link.connection_id() == connection_id);
        let is_active_replica_side_rdb = matches!(
            role,
            Some(ReplicationConnection::RdbChannel { main })
                if matches!(self.role(main), Some(ReplicationConnection::Replica))
        );
        let is_replica_side_rdb = is_active_replica_side_rdb
            || self
                .replica_rdb_link
                .as_ref()
                .is_some_and(|link| link.connection_id() == connection_id);

        if is_replica_side_main || is_replica_side_rdb {
            if is_replica_side_main || is_active_replica_side_rdb {
                self.replica_main_read_paused = false;
            }
            if is_replica_side_main {
                if let Some(link) = self.replica_link.take() {
                    if link.can_resume_after_disconnect()
                        || matches!(link.rdb_channel_state(), ReplicaRdbChannelState::RdbLoaded)
                    {
                        if let Some(replid) = link.master_replid() {
                            crate::server_log!(
                                debug,
                                "dual-rdb preserving resume after main close: main={} replid={} offset={} rdb_state={:?}",
                                connection_id.0,
                                replid,
                                link.applied_position().get(),
                                link.rdb_channel_state()
                            );
                            // A committed session replaces any prior forced-full-sync request.
                            self.set_replica_resume(
                                link.master().clone(),
                                replid.to_string(),
                                link.applied_position(),
                                link.selected_db(),
                            );
                        }
                    } else if link.dual_main_psync_was_paused() && self.replica_resume.is_none() {
                        if let Some((replid, offset, selected_db)) =
                            link.provisional_resume_position()
                        {
                            crate::server_log!(
                                debug,
                                "dual-rdb preserving handshake resume after main close: main={} replid={} offset={} rdb_state={:?}",
                                connection_id.0,
                                replid,
                                offset.get(),
                                link.rdb_channel_state()
                            );
                            self.set_replica_resume(
                                link.master().clone(),
                                replid.to_string(),
                                offset,
                                selected_db,
                            );
                        }
                    }
                }
                self.channel_pairs.remove(&connection_id);
            }
            if is_replica_side_rdb {
                if is_active_replica_side_rdb {
                    if let Some(link) = self.replica_link_mut() {
                        link.mark_rdb_transfer_failed();
                    }
                }
                self.replica_rdb_link = None;
                self.channel_pairs.remove(&connection_id);
            }
        } else {
            self.remove_channel_pair(connection_id);
            match role {
                Some(ReplicationConnection::Primary(id)) => {
                    self.primary_links.remove(&id);
                }
                Some(ReplicationConnection::Replica)
                    if self
                        .replica_link
                        .as_ref()
                        .is_some_and(|link| link.connection_id() == connection_id) =>
                {
                    if let Some(link) = self.replica_link.take() {
                        if link.can_resume_after_disconnect() {
                            if let Some(replid) = link.master_replid() {
                                self.set_replica_resume(
                                    link.master().clone(),
                                    replid.to_string(),
                                    link.applied_position(),
                                    link.selected_db(),
                                );
                            }
                        }
                    }
                }
                Some(ReplicationConnection::Replica) => {}
                Some(ReplicationConnection::RdbChannelCandidate)
                | Some(ReplicationConnection::RdbChannel { .. }) => {}
                None => {}
            }
            if let Some(ReplicationConnection::Primary(id)) = role {
                self.primary_links.remove(&id);
            }
        }
    }
    pub fn replica_dual_channel_failed(&self, ip: &str, port: u16) -> bool {
        self.dual_channel_failed_replicas
            .contains(&(ip.to_string(), port))
    }

    pub fn mark_replica_dual_channel_failed(&mut self, ip: String, port: u16) {
        self.dual_channel_failed_replicas.insert((ip, port));
    }

    pub fn replica_link(&self) -> Option<&ReplicaLink> {
        self.replica_link.as_ref()
    }

    pub fn replica_link_mut(&mut self) -> Option<&mut ReplicaLink> {
        self.replica_link.as_mut()
    }

    pub fn take_replica_link(&mut self) -> Option<ReplicaLink> {
        self.replica_link.take()
    }

    pub fn clear_replica_session(&mut self) -> Option<PendingReplicationBuffer> {
        if let Some(link) = self.replica_link.take() {
            self.channel_records.remove(&link.connection_id());
        }
        if let Some(link) = self.replica_rdb_link.take() {
            self.channel_records.remove(&link.connection_id());
        }
        let pending = self
            .replica_pending_buffer
            .take()
            .map(|pending| pending.buffer);
        self.replica_main_read_paused = false;
        // `replica_resume` and `force_full_sync` describe the next connection,
        // not the active session. The caller captures a streaming link's
        // replid/offset/DB before reaching this cleanup and separately decides
        // whether the next PSYNC must be full.
        pending
    }

    pub(crate) fn replica_pending_buffer(&self) -> Option<&PendingReplicationBuffer> {
        self.replica_pending_buffer
            .as_ref()
            .map(|pending| &pending.buffer)
    }

    pub(crate) fn replica_pending_buffer_for(
        &self,
        session_id: u64,
    ) -> Option<&PendingReplicationBuffer> {
        self.replica_pending_buffer
            .as_ref()
            .filter(|pending| pending.session_id == session_id)
            .map(|pending| &pending.buffer)
    }

    pub(crate) fn replica_pending_buffer_mut_for(
        &mut self,
        session_id: u64,
    ) -> Option<&mut PendingReplicationBuffer> {
        self.replica_pending_buffer
            .as_mut()
            .filter(|pending| pending.session_id == session_id)
            .map(|pending| &mut pending.buffer)
    }

    pub(crate) fn set_replica_pending_buffer(
        &mut self,
        session_id: u64,
        buffer: PendingReplicationBuffer,
    ) -> Option<PendingReplicationBuffer> {
        self.replica_pending_buffer
            .replace(ReplicaPendingBuffer { session_id, buffer })
            .map(|pending| pending.buffer)
    }

    pub(crate) fn take_replica_pending_buffer_for(
        &mut self,
        session_id: u64,
    ) -> Option<PendingReplicationBuffer> {
        if self
            .replica_pending_buffer
            .as_ref()
            .is_some_and(|pending| pending.session_id == session_id)
        {
            self.replica_pending_buffer
                .take()
                .map(|pending| pending.buffer)
        } else {
            None
        }
    }

    pub(crate) fn take_replica_pending_buffer(&mut self) -> Option<PendingReplicationBuffer> {
        self.replica_pending_buffer
            .take()
            .map(|pending| pending.buffer)
    }

    pub(crate) fn replica_main_read_paused(&self) -> bool {
        self.replica_main_read_paused
    }

    pub(crate) fn set_replica_main_read_paused(&mut self, paused: bool) {
        self.replica_main_read_paused = paused;
    }

    #[allow(dead_code)]
    pub(crate) fn set_replica_rdb_link(
        &mut self,
        rdb_connection_id: ConnectionId,
        main_connection_id: ConnectionId,
    ) -> Result<(), LinkError> {
        if !matches!(
            self.role(main_connection_id),
            Some(ReplicationConnection::Replica)
        ) {
            return Err(LinkError::Protocol);
        }
        if self.role(rdb_connection_id).is_some() {
            return Err(LinkError::Protocol);
        }
        if self.replica_link.is_none() {
            return Err(LinkError::Protocol);
        }
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
        self.replica_rdb_link = Some(ReplicaRdbLink::new(rdb_connection_id));
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn replica_rdb_link(&self) -> Option<&ReplicaRdbLink> {
        self.replica_rdb_link.as_ref()
    }

    #[allow(dead_code)]
    pub(crate) fn replica_rdb_link_mut(&mut self) -> Option<&mut ReplicaRdbLink> {
        self.replica_rdb_link.as_mut()
    }

    #[allow(dead_code)]
    pub(crate) fn take_replica_rdb_link(&mut self) -> Option<ReplicaRdbLink> {
        self.replica_rdb_link.take()
    }

    #[allow(dead_code)]
    pub(crate) fn replica_connection_class(&self, id: ConnectionId) -> ReplicaConnClass {
        if let Some(link) = &self.replica_link {
            if link.connection_id() == id {
                return ReplicaConnClass::Main;
            }
        }
        if let Some(link) = &self.replica_rdb_link {
            if link.connection_id() == id {
                return ReplicaConnClass::Rdb;
            }
        }
        ReplicaConnClass::Unknown
    }

    pub fn master_connect_allowed(&self, now_ms: i64) -> bool {
        now_ms >= self.next_master_connect_ms
    }

    pub fn schedule_master_reconnect(&mut self, now_ms: i64, cause: MasterReconnectCause) {
        let delay_ms = cause.delay_ms();
        self.next_master_connect_ms = now_ms.saturating_add(delay_ms);
        crate::server_log!(
            debug,
            "Scheduling PRIMARY reconnect: cause={:?} delay={}ms",
            cause,
            delay_ms
        );
    }

    pub fn reset_master_reconnect_backoff(&mut self) {
        self.next_master_connect_ms = 0;
    }

    /// Return the list of `ReplicaId`s whose deferred diskless full-resync
    /// delay has elapsed by `now_ms`, or all of them if `force_all` is true
    /// (used when `repl-diskless-sync-max-replicas` is satisfied).
    pub fn drain_ready_deferred_links(&mut self, now_ms: i64, force_all: bool) -> Vec<ReplicaId> {
        let mut ready = Vec::new();
        for (&id, link) in self.primary_links.iter() {
            if !link.is_deferred_diskless() {
                continue;
            }
            if force_all || link.diskless_delay_ready(now_ms) {
                ready.push(id);
            }
        }
        ready
    }

    /// Count how many primary links are currently in the deferred diskless
    /// window (PreparingFullResync and waiting for the delay).
    #[cfg(test)]
    pub fn count_deferred_diskless_links(&self) -> usize {
        self.primary_links
            .values()
            .filter(|l| l.is_deferred_diskless())
            .count()
    }

    /// Collect only deferred links that can share the selected RDB filter.
    pub fn collect_deferred_links_for_filter(&self, filter_mode: RdbFilterMode) -> Vec<ReplicaId> {
        self.primary_links
            .iter()
            .filter(|(_, link)| {
                link.is_deferred_diskless() && link.replica().rdb_filter_mode == filter_mode
            })
            .map(|(&id, _)| id)
            .collect()
    }

    pub fn set_replica_resume(
        &mut self,
        master: MasterInfo,
        replid: String,
        offset: ReplicationOffset,
        selected_db: u32,
    ) {
        self.replica_resume = Some((master, replid, offset, selected_db));
        self.force_full_sync = false;
    }

    pub fn clear_replica_resume(&mut self) {
        self.replica_resume = None;
        self.force_full_sync = true;
    }

    pub(crate) fn discard_replica_resume(&mut self) {
        self.replica_resume = None;
    }

    pub fn rebind_replica_resume(&mut self, master: MasterInfo) {
        if let Some((resume_master, _, _, _)) = self.replica_resume.as_mut() {
            *resume_master = master;
            self.force_full_sync = false;
        }
    }

    pub fn replica_resume_for(
        &self,
        master: &MasterInfo,
    ) -> Option<(&str, ReplicationOffset, u32)> {
        if self.force_full_sync {
            return None;
        }
        self.replica_resume
            .as_ref()
            .filter(|(resume_master, _, _, _)| resume_master == master)
            .map(|(_, replid, offset, selected_db)| (replid.as_str(), *offset, *selected_db))
    }

    pub fn take_force_full_sync(&mut self) -> bool {
        std::mem::take(&mut self.force_full_sync)
    }

    pub fn force_full_sync_pending(&self) -> bool {
        self.force_full_sync
    }

    pub fn reject_authentication(&mut self, attempt: AuthenticationAttempt) {
        self.rejected_auth = Some(attempt);
    }

    pub fn authentication_is_rejected(&self, attempt: &AuthenticationAttempt) -> bool {
        self.rejected_auth.as_ref() == Some(attempt)
    }

    pub fn clear_rejected_authentication(&mut self) {
        self.rejected_auth = None;
    }
}
