//! Dual-channel PSYNC wait ownership.
//! Each RDB candidate has one record for its ENDOFF boundary, optional main
//! owner and backlog pin. Grace waits remain keyed by the former main channel
//! because they deliberately outlive the disconnected transport.

use super::*;

#[derive(Clone, Copy, Debug)]
pub(super) struct RdbPsyncWait {
    pub(super) boundary: SnapshotBoundary,
    pub(super) main: Option<ConnectionId>,
    pub(super) pin_cursor: Option<BacklogCursor>,
    pub(super) pin_from_existing_tail: bool,
}

impl RdbPsyncWait {
    pub(super) fn new(
        boundary: SnapshotBoundary,
        pin_cursor: Option<BacklogCursor>,
        pin_from_existing_tail: bool,
    ) -> Self {
        Self {
            boundary,
            main: None,
            pin_cursor,
            pin_from_existing_tail,
        }
    }
}

impl ReplicationRuntime {
    /// Count C-style `replicas_waiting_psync` lifecycle owners. Ownership is
    /// independent from a backlog pin because an empty-history ENDOFF wait
    /// still prevents backlog expiry until PSYNC completes or grace ends.
    pub fn psync_wait_owner_count(&self) -> usize {
        self.rdb_psync_waits.len()
            + self.retained_rdb_psync_waits.len()
            + self.grace_psync_pin_by_main.len()
    }

    pub fn has_psync_wait_owners(&self) -> bool {
        self.psync_wait_owner_count() != 0
    }

    #[cfg(test)]
    pub(crate) fn unassociated_retained_rdb_owners(&self) -> Vec<UnassociatedRetainedRdbOwner> {
        self.retained_rdb_psync_waits
            .iter()
            .filter_map(|(&public_client_id, retained)| {
                retained
                    .wait
                    .main
                    .is_none()
                    .then_some(UnassociatedRetainedRdbOwner {
                        public_client_id,
                        pin_cursor: retained.wait.pin_cursor,
                    })
            })
            .collect()
    }

    pub(crate) fn remove_unassociated_retained_rdb_owners_if(
        &mut self,
        mut should_remove: impl FnMut(
            UnassociatedRetainedRdbOwner,
            &mut Option<OutputBufferSoftLimitReachedAt>,
        ) -> bool,
    ) -> bool {
        let original_len = self.retained_rdb_psync_waits.len();
        self.retained_rdb_psync_waits
            .retain(|&public_client_id, retained| {
                // Association can happen after an earlier accounting snapshot.
                // Revalidate ownership while holding the retained map entry so
                // cleanup never removes a wait that has acquired a main.
                if retained.wait.main.is_some() {
                    return true;
                }
                let owner = UnassociatedRetainedRdbOwner {
                    public_client_id,
                    pin_cursor: retained.wait.pin_cursor,
                };
                !should_remove(owner, &mut retained.soft_limit_reached_at)
            });
        self.retained_rdb_psync_waits.len() != original_len
    }

    /// Move a marker-complete, unpaired wait away from its closing transport.
    /// Validation precedes removal so duplicate public ids or already-paired
    /// waits cannot lose the live owner.
    #[cfg(test)]
    pub(crate) fn retain_completed_unpaired_rdb_wait(
        &mut self,
        rdb_connection_id: ConnectionId,
        public_client_id: u64,
        deadline_ms: i64,
    ) -> bool {
        if self
            .retained_rdb_psync_waits
            .contains_key(&public_client_id)
            || !matches!(
                self.role(rdb_connection_id),
                Some(ReplicationConnection::RdbChannelCandidate)
            )
            || self
                .rdb_psync_waits
                .get(&rdb_connection_id)
                .is_none_or(|wait| wait.main.is_some())
        {
            return false;
        }
        let Some(wait) = self.rdb_psync_waits.remove(&rdb_connection_id) else {
            return false;
        };
        self.retained_rdb_psync_waits.insert(
            public_client_id,
            RetainedRdbPsyncWait {
                wait,
                deadline_ms,
                soft_limit_reached_at: None,
            },
        );
        true
    }

    /// Associate a completed retained RDB wait with exactly one waiting main.
    /// The retained record remains the wait owner until main PSYNC, abort, main
    /// close, or deadline expiry; no dead RDB transport is recreated.
    pub fn try_associate_retained_rdb_wait(
        &mut self,
        main_connection_id: ConnectionId,
        public_client_id: u64,
        now_ms: i64,
    ) -> Result<(), LinkError> {
        let Some(retained) = self.retained_rdb_psync_waits.get(&public_client_id) else {
            return Err(LinkError::Protocol);
        };
        if now_ms >= retained.deadline_ms {
            self.retained_rdb_psync_waits.remove(&public_client_id);
            return Err(LinkError::Protocol);
        }
        if retained.wait.main.is_some()
            || self.channel_pairs.contains_key(&main_connection_id)
            || !self
                .primary_for_connection(main_connection_id)
                .is_some_and(|link| {
                    link.dual_state() == PrimaryDualChannelState::WaitingForRdbChannel
                })
        {
            return Err(LinkError::Protocol);
        }
        let boundary = retained.wait.boundary;
        let deadline_ms = retained.deadline_ms;
        let soft_limit_reached_at = retained.soft_limit_reached_at;
        let Some(main_link) = self.primary_for_connection_mut(main_connection_id) else {
            return Err(LinkError::Protocol);
        };
        main_link.record_dual_channel_endoff_boundary(boundary);
        main_link.transition_to_waiting_for_main_psync()?;
        main_link.mark_dual_rdb_complete();
        main_link.rdb_channel_wait_deadline_ms = Some(deadline_ms);
        main_link.merge_output_buffer_soft_limit_reached_at(soft_limit_reached_at);
        self.retained_rdb_psync_waits
            .get_mut(&public_client_id)
            .expect("validated retained RDB wait must remain present")
            .wait
            .main = Some(main_connection_id);
        Ok(())
    }

    pub fn take_expired_retained_rdb_psync_waits(
        &mut self,
        now_ms: i64,
    ) -> Vec<(u64, Option<ConnectionId>)> {
        let mut expired: Vec<_> = self
            .retained_rdb_psync_waits
            .iter()
            .filter_map(|(&public_id, retained)| {
                (now_ms >= retained.deadline_ms).then_some((public_id, retained.wait.main))
            })
            .collect();
        expired.sort_by_key(|(public_id, _)| *public_id);
        for (public_id, main) in &expired {
            self.retained_rdb_psync_waits.remove(public_id);
            if let Some(link) = main.and_then(|main| self.primary_for_connection_mut(main)) {
                link.clear_dual_channel_endoff_boundary();
                link.rdb_channel_wait_deadline_ms = None;
            }
        }
        expired
    }

    /// Build `$ENDOFF` for an unpaired RDB candidate after its REPLCONF reply
    /// has been accepted. The caller owns ordering it after that `+OK`.
    #[cfg(test)]
    pub(crate) fn take_endoff_for_rdb_candidate_if_unambiguous(
        &mut self,
        rdb_connection_id: ConnectionId,
        token_present: bool,
        backlog: &mut ReplicationBacklog,
    ) -> Option<Vec<u8>> {
        let boundary = backlog.snapshot_boundary();
        self.take_endoff_for_rdb_candidate_at_boundary(
            rdb_connection_id,
            token_present.then_some(rdb_connection_id.0 as u64),
            backlog,
            boundary,
            0,
        )
    }

    pub(crate) fn take_endoff_for_rdb_cohort(
        &mut self,
        rdb_connection_ids: &[ConnectionId],
        mut public_client_id: impl FnMut(ConnectionId) -> Option<u64>,
        backlog: &mut ReplicationBacklog,
        db_id: u32,
    ) -> Vec<(ConnectionId, Vec<u8>)> {
        let boundary = backlog.snapshot_boundary();
        rdb_connection_ids
            .iter()
            .filter_map(|&rdb_connection_id| {
                self.take_endoff_for_rdb_candidate_at_boundary(
                    rdb_connection_id,
                    public_client_id(rdb_connection_id),
                    backlog,
                    boundary,
                    db_id,
                )
                .map(|frame| (rdb_connection_id, frame))
            })
            .collect()
    }

    fn take_endoff_for_rdb_candidate_at_boundary(
        &mut self,
        rdb_connection_id: ConnectionId,
        public_client_id: Option<u64>,
        backlog: &mut ReplicationBacklog,
        boundary: SnapshotBoundary,
        db_id: u32,
    ) -> Option<Vec<u8>> {
        let token_present = public_client_id.is_some();
        let candidate_role = self.role(rdb_connection_id);
        let endoff_already_sent = self.rdb_psync_waits.contains_key(&rdb_connection_id);
        let waiting_mains = self.unpaired_waiting_mains();
        if !self.rdb_candidate_can_receive_endoff(
            candidate_role,
            endoff_already_sent,
            token_present,
            &waiting_mains,
        ) {
            log_endoff_skipped(
                rdb_connection_id,
                candidate_role,
                endoff_already_sent,
                &waiting_mains,
                token_present,
            );
            return None;
        }
        let public_client_id = public_client_id?;
        activate_backlog_history(backlog);
        let endoff_bytes = crate::engine::distributed::replication::protocol::encode_end_offset(
            boundary.reported_offset(),
            backlog.replid(),
            db_id,
            public_client_id,
        );
        self.record_rdb_psync_wait(rdb_connection_id, boundary, backlog);
        Some(endoff_bytes)
    }

    fn unpaired_waiting_mains(&self) -> Vec<ConnectionId> {
        self.primary_links
            .values()
            .filter(|link| {
                matches!(
                    link.dual_state(),
                    PrimaryDualChannelState::WaitingForRdbChannel
                        | PrimaryDualChannelState::WaitingForMainPsync
                )
            })
            .map(|link| link.connection_id())
            .filter(|main| !self.channel_pairs.contains_key(main))
            .filter(|main| {
                !self
                    .retained_rdb_psync_waits
                    .values()
                    .any(|retained| retained.wait.main == Some(*main))
            })
            .collect()
    }

    fn rdb_candidate_can_receive_endoff(
        &self,
        role: Option<ReplicationConnection>,
        already_sent: bool,
        token_present: bool,
        waiting_mains: &[ConnectionId],
    ) -> bool {
        let eligible_role = matches!(role, Some(ReplicationConnection::RdbChannelCandidate));
        let assigned = self
            .rdb_psync_waits
            .iter()
            .filter(|(candidate, wait)| {
                wait.main.is_none() && !self.channel_pairs.contains_key(candidate)
            })
            .count()
            + self
                .retained_rdb_psync_waits
                .values()
                .filter(|retained| retained.wait.main.is_none())
                .count();
        eligible_role && !already_sent && token_present && waiting_mains.len() > assigned
    }

    fn record_rdb_psync_wait(
        &mut self,
        rdb_connection_id: ConnectionId,
        boundary: SnapshotBoundary,
        backlog: &mut ReplicationBacklog,
    ) {
        // A non-empty Rust backlog is the flat-buffer equivalent of C's
        // non-empty repl_buffer_blocks list. A PSYNC-wait candidate can pin
        // that tail immediately; otherwise it waits for the first later frame
        // to create history and backfill the pin.
        let has_repl_buffer_tail = backlog.history_len() > 0;
        self.rdb_psync_waits.insert(
            rdb_connection_id,
            RdbPsyncWait::new(
                boundary,
                has_repl_buffer_tail.then_some(backlog.tail_block_start_cursor()),
                has_repl_buffer_tail,
            ),
        );
        // The C tail ref is effective immediately, not only after the next
        // replication frame. Apply the corresponding byte eviction floor now.
        backlog.set_min_protected_cursor(self.min_protected_backlog_cursor());
        crate::server_log!(
            verbose,
            "Add rdb replica {} to waiting psync, {}",
            rdb_connection_id.0,
            if has_repl_buffer_tail {
                "tracking repl-backlog tail"
            } else {
                "no repl-backlog to track"
            }
        );
    }

    pub fn clear_psync_wait_for_main(&mut self, main: ConnectionId) {
        if self.grace_psync_pin_by_main.contains_key(&main) {
            return;
        }
        let pins: Vec<_> = self
            .rdb_psync_waits
            .iter()
            .filter_map(|(rdb, wait)| (wait.main == Some(main)).then_some((rdb.0, wait.pin_cursor)))
            .collect();
        crate::server_log!(
            debug,
            "dual-rdb clearing wait pins: main={} dual_state={:?} candidates={:?} main_pin={:?}",
            main.0,
            self.primary_for_connection(main)
                .map(|link| link.dual_state()),
            pins,
            self.grace_psync_pin_by_main.get(&main)
        );
        self.grace_psync_pin_by_main.remove(&main);
        let rdb_candidates: Vec<_> = self
            .rdb_psync_waits
            .iter()
            .filter_map(|(rdb, wait)| (wait.main == Some(main)).then_some(*rdb))
            .collect();
        for rdb in rdb_candidates {
            self.rdb_psync_waits.remove(&rdb);
        }
        self.retained_rdb_psync_waits
            .retain(|_, retained| retained.wait.main != Some(main));
        if let Some(main_link) = self.primary_for_connection_mut(main) {
            crate::server_log!(
                debug,
                "dual backlog pin: clear wait pins main={} main_pin={:?}",
                main.0,
                main_link.dual_main_backlog_pin_cursor()
            );
            main_link.set_dual_main_backlog_pin_cursor(None);
            main_link.clear_dual_channel_endoff_boundary();
        }
    }

    pub fn complete_psync_wait_for_main(&mut self, main: ConnectionId) {
        self.grace_psync_pin_by_main.remove(&main);
        let transferred_pin = self
            .rdb_psync_waits
            .values()
            .filter(|wait| wait.main == Some(main))
            .filter_map(|wait| wait.pin_cursor)
            .min()
            .or_else(|| {
                self.retained_rdb_psync_waits
                    .values()
                    .filter(|retained| retained.wait.main == Some(main))
                    .filter_map(|retained| retained.wait.pin_cursor)
                    .min()
            })
            .or_else(|| {
                self.primary_for_connection(main)
                    .and_then(|link| link.dual_main_backlog_pin_cursor())
            });
        self.clear_psync_wait_for_main(main);
        if let Some(pin) = transferred_pin {
            if let Some(link) = self.primary_for_connection_mut(main) {
                link.set_dual_main_backlog_pin_cursor(Some(pin));
            }
        }
    }

    pub(crate) fn has_existing_tail_pin_for_main(&self, main: ConnectionId) -> bool {
        self.rdb_psync_waits
            .values()
            .any(|wait| wait.main == Some(main) && wait.pin_from_existing_tail)
            || self.retained_rdb_psync_waits.values().any(|retained| {
                retained.wait.main == Some(main) && retained.wait.pin_from_existing_tail
            })
    }

    pub(crate) fn set_dual_main_pin_from_existing_tail(&mut self, main: ConnectionId) {
        let pin = self
            .rdb_psync_waits
            .values()
            .find(|wait| wait.main == Some(main) && wait.pin_from_existing_tail)
            .and_then(|wait| wait.pin_cursor)
            .or_else(|| {
                self.retained_rdb_psync_waits
                    .values()
                    .find(|retained| {
                        retained.wait.main == Some(main) && retained.wait.pin_from_existing_tail
                    })
                    .and_then(|retained| retained.wait.pin_cursor)
            });
        if let Some(pin) = pin.filter(|pin| pin.get() > 0) {
            if let Some(link) = self.primary_for_connection_mut(main) {
                link.set_dual_main_backlog_pin_cursor(Some(pin));
            }
        }
    }

    /// Abort a failed main PSYNC without honoring the delayed-free grace
    /// protection used only for a peer that may still establish PSYNC.
    pub fn abort_psync_wait_for_main(&mut self, main: ConnectionId) {
        self.grace_psync_pin_by_main.remove(&main);
        let rdb_candidates: Vec<_> = self
            .rdb_psync_waits
            .iter()
            .filter_map(|(rdb, wait)| (wait.main == Some(main)).then_some(*rdb))
            .collect();
        for rdb in rdb_candidates {
            self.rdb_psync_waits.remove(&rdb);
        }
        self.retained_rdb_psync_waits
            .retain(|_, retained| retained.wait.main != Some(main));
        if let Some(link) = self.primary_for_connection_mut(main) {
            link.clear_dual_channel_endoff_boundary();
        }
    }

    /// Preserve a paired RDB wait pin after the main socket closes. Mirrors
    /// C's protected RDB client surviving async free until its grace deadline.
    pub fn defer_psync_wait_for_main(&mut self, main: ConnectionId, deadline_ms: i64) -> bool {
        let live_rdb = self
            .rdb_psync_waits
            .iter()
            .find_map(|(rdb, wait)| (wait.main == Some(main)).then_some(*rdb));
        let wait = if let Some(rdb) = live_rdb {
            self.rdb_psync_waits.remove(&rdb)
        } else {
            let retained_public_id =
                self.retained_rdb_psync_waits
                    .iter()
                    .find_map(|(public_id, retained)| {
                        (retained.wait.main == Some(main)).then_some(*public_id)
                    });
            retained_public_id
                .and_then(|public_id| self.retained_rdb_psync_waits.remove(&public_id))
                .map(|retained| retained.wait)
        };
        let Some(wait) = wait else { return false };
        if let Some(link) = self.primary_for_connection_mut(main) {
            link.clear_dual_channel_endoff_boundary();
        }
        self.grace_psync_pin_by_main.insert(
            main,
            GracePsyncWait {
                deadline_ms,
                pin_cursor: wait.pin_cursor,
            },
        );
        crate::server_log!(
            debug,
            "dual-rdb grace defer: main={} deadline={} map_len={}",
            main.0,
            deadline_ms,
            self.grace_psync_pin_by_main.len()
        );
        true
    }

    pub fn take_expired_grace_psync_waits(&mut self, now_ms: i64) -> Vec<ConnectionId> {
        let expired: Vec<_> = self
            .grace_psync_pin_by_main
            .iter()
            .filter_map(|(main, wait)| (now_ms >= wait.deadline_ms).then_some(*main))
            .collect();
        for main in &expired {
            let deadline = self
                .grace_psync_pin_by_main
                .remove(main)
                .map(|wait| wait.deadline_ms)
                .unwrap_or_default();
            crate::server_log!(
                debug,
                "dual-rdb grace remove: main={} deadline={} now={} map_len={}",
                main.0,
                deadline,
                now_ms,
                self.grace_psync_pin_by_main.len()
            );
        }
        expired
    }

    pub fn set_grace_psync_wait_deadline(&mut self, main: ConnectionId, deadline_ms: i64) {
        if let Some(wait) = self.grace_psync_pin_by_main.get_mut(&main) {
            wait.deadline_ms = deadline_ms;
        }
        for retained in self.retained_rdb_psync_waits.values_mut() {
            if retained.wait.main == Some(main) {
                retained.deadline_ms = deadline_ms;
            }
        }
    }

    pub fn start_grace_psync_wait_for_main(&mut self, main: ConnectionId, deadline_ms: i64) {
        let pin_cursor = self
            .primary_for_connection(main)
            .and_then(|link| link.dual_main_backlog_pin_cursor());
        self.grace_psync_pin_by_main.insert(
            main,
            GracePsyncWait {
                deadline_ms,
                pin_cursor,
            },
        );
        crate::server_log!(
            debug,
            "dual-rdb grace start: main={} deadline={} pin={:?} map_len={}",
            main.0,
            deadline_ms,
            pin_cursor,
            self.grace_psync_pin_by_main.len()
        );
    }

    pub fn clear_grace_psync_wait_for_main(&mut self, main: ConnectionId) {
        if self.grace_psync_pin_by_main.remove(&main).is_some() {
            crate::server_log!(
                debug,
                "dual-rdb grace clear: main={} map_len={}",
                main.0,
                self.grace_psync_pin_by_main.len()
            );
        }
    }

    pub fn has_grace_psync_wait_for_main(&self, main: ConnectionId) -> bool {
        self.grace_psync_pin_by_main.contains_key(&main)
    }

    #[cfg(test)]
    pub(crate) fn insert_grace_psync_wait_for_test(
        &mut self,
        main: ConnectionId,
        deadline_ms: i64,
    ) {
        self.grace_psync_pin_by_main.insert(
            main,
            GracePsyncWait {
                deadline_ms,
                pin_cursor: None,
            },
        );
    }

    #[cfg(test)]
    pub(crate) fn insert_unpaired_rdb_psync_wait_for_test(
        &mut self,
        rdb: ConnectionId,
        boundary: SnapshotBoundary,
    ) {
        self.rdb_psync_waits
            .insert(rdb, RdbPsyncWait::new(boundary, None, false));
    }

    /// C computes a protected RDB client's output-buffer usage from its shared
    /// repl-buffer reference through the current tail. This returns the Rust
    /// byte-offset equivalent for a paired main connection.
    pub fn psync_wait_pin_for_main(&self, main: ConnectionId) -> Option<BacklogCursor> {
        self.grace_psync_pin_by_main
            .get(&main)
            .and_then(|wait| wait.pin_cursor)
            .or_else(|| {
                self.rdb_psync_waits
                    .values()
                    .find(|wait| wait.main == Some(main))
                    .and_then(|wait| wait.pin_cursor)
                    .or_else(|| {
                        self.retained_rdb_psync_waits
                            .values()
                            .find(|retained| retained.wait.main == Some(main))
                            .and_then(|retained| retained.wait.pin_cursor)
                    })
                    .or_else(|| {
                        self.primary_for_connection(main)
                            .filter(|link| {
                                link.dual_state() == PrimaryDualChannelState::WaitingForReplicaAck
                            })
                            .and_then(|link| link.dual_main_backlog_pin_cursor())
                    })
            })
    }

    pub fn dual_main_rdb_reference_cursor(&self, main: ConnectionId) -> Option<BacklogCursor> {
        self.primary_for_connection(main)
            .filter(|link| {
                link.dual_state() == PrimaryDualChannelState::WaitingForReplicaAck
                    && !link.dual_rdb_complete()
            })
            .map(|link| link.sent_cursor())
    }

    /// Attach wait-PSYNC members to the first replication frame created after
    /// an empty ENDOFF boundary, matching C's
    /// `backfillRdbReplicasToPsyncWait`.
    pub fn backfill_waiting_psync_pins(
        &mut self,
        first_frame_cursor: BacklogCursor,
    ) -> Vec<ConnectionId> {
        let mut backfilled_rdb_connections = Vec::new();
        for (rdb, wait) in &mut self.rdb_psync_waits {
            if wait.pin_cursor.is_none() {
                wait.pin_cursor = Some(first_frame_cursor);
                backfilled_rdb_connections.push(*rdb);
            }
        }
        for wait in self.grace_psync_pin_by_main.values_mut() {
            if wait.pin_cursor.is_none() {
                wait.pin_cursor = Some(first_frame_cursor);
            }
        }
        for retained in self.retained_rdb_psync_waits.values_mut() {
            if retained.wait.pin_cursor.is_none() {
                retained.wait.pin_cursor = Some(first_frame_cursor);
            }
        }
        self.repl_buffer_tail_exists = true;
        backfilled_rdb_connections.sort_by_key(|connection| connection.0);
        backfilled_rdb_connections
    }

    pub(super) fn clear_endoff_snapshot_for_rdb_candidate(
        &mut self,
        rdb_connection_id: ConnectionId,
    ) {
        let Some(wait) = self.rdb_psync_waits.remove(&rdb_connection_id) else {
            return;
        };
        if let Some(main_link) = wait
            .main
            .and_then(|main| self.primary_for_connection_mut(main))
        {
            main_link.clear_dual_channel_endoff_boundary();
        }
    }
}

fn log_endoff_skipped(
    rdb: ConnectionId,
    role: Option<ReplicationConnection>,
    already_sent: bool,
    waiting_mains: &[ConnectionId],
    token_present: bool,
) {
    crate::server_log!(
        debug,
        "dual-rdb ENDOFF skipped: rdb={} role={:?} already_sent={} waiting_mains={:?} token_present={}",
        rdb.0,
        role,
        already_sent,
        waiting_mains,
        token_present
    );
}

fn activate_backlog_history(backlog: &mut ReplicationBacklog) {
    if backlog.activate_new_history() {
        crate::server_log!(
            notice,
            "Replication backlog created, my new replication IDs are '{}' and '{}'",
            backlog.replid(),
            backlog.second_replid()
        );
    }
}
