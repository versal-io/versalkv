//! Read-only replication projections for Cluster, INFO, and accounting.
//! Views are rebuilt from authoritative channel/session owners and must not
//! mutate pairing, reconnect, or PSYNC-wait lifecycle state.

use super::*;
use crate::engine::distributed::replication::{BacklogCursor, ReplicationOffset};

impl ReplicationRuntime {
    pub fn cluster_view(
        &self,
        node_id: &str,
        loading: bool,
    ) -> crate::engine::distributed::cluster::state::ClusterReplicationView {
        use crate::engine::distributed::cluster::state::{
            ClusterReplicaHealth, ClusterReplicationView,
        };

        if let Some(link) = self
            .replica_link()
            .filter(|link| link.cluster_node_id() == Some(node_id))
        {
            let health = if loading
                || !link.is_streaming()
                || link.has_pending_rdb_receive()
                || !matches!(
                    link.rdb_channel_state(),
                    ReplicaRdbChannelState::None | ReplicaRdbChannelState::RdbLoaded
                ) {
                ClusterReplicaHealth::Loading
            } else {
                ClusterReplicaHealth::Online
            };
            return ClusterReplicationView {
                upstream: None,
                received_offset: link.received_position(),
                applied_offset: link.applied_position(),
                acknowledged_offset: link.acknowledged_position(),
                priority: 0,
                health,
            };
        }

        let downstream = self
            .primary_links
            .values()
            .find(|link| self.replica_node_id(link.connection_id()) == Some(node_id));
        let Some(link) = downstream else {
            return ClusterReplicationView::disconnected();
        };
        let zero = ReplicationOffset::try_from(0).expect("zero is a valid replication offset");
        let acknowledged_offset = link.last_ack_position().unwrap_or(zero);
        let health = match link.state() {
            PrimaryLinkState::Closing => ClusterReplicaHealth::Stale,
            PrimaryLinkState::Streaming
                if link.dual_state() != PrimaryDualChannelState::WaitingForReplicaAck
                    && link.last_ack_position().is_some_and(|offset| offset > zero) =>
            {
                ClusterReplicaHealth::Online
            }
            _ => ClusterReplicaHealth::Loading,
        };
        ClusterReplicationView {
            upstream: None,
            received_offset: acknowledged_offset,
            applied_offset: acknowledged_offset,
            acknowledged_offset,
            priority: 0,
            health,
        }
    }

    pub fn rebuild_view(&self) -> crate::engine::distributed::replication::ReplicationView {
        let mut view = crate::engine::distributed::replication::ReplicationView::default();
        let now = mstime();
        view.live_replica_count = self.live_replica_count();
        view.connected_replica_count = self.connected_replica_count();
        view.cluster_replica_views = self
            .channel_records
            .values()
            .filter_map(|record| record.replica_node_id.as_ref())
            .map(|node_id| (node_id.clone(), self.cluster_view(node_id, false)))
            .collect();
        // C's `replicas_waiting_psync` includes protected RDB clients during
        // delayed free. Once an RDB-first close transfers ownership from its
        // RDB connection to the main connection, keep that wait-set member
        // visible until PSYNC succeeds or the grace deadline releases it.
        view.replicas_waiting_psync = self.psync_wait_owner_count();
        for link in self.primary_links.values() {
            let is_online = classic_link_is_online(link);
            let connection_id = link.connection_id().0;
            view.replica_ack_offsets
                .insert(connection_id, link.last_ack_position());
            view.replica_aof_ack_offsets
                .insert(connection_id, link.last_aof_ack_position());
            if is_pre_psync_main_link(link) {
                continue;
            }
            let lag = if is_online {
                match link.last_ack_time_ms() {
                    0 => i64::MAX,
                    acked_at => now.saturating_sub(acked_at),
                }
            } else {
                0
            };
            let rdb_connection_id = self.rdb_channel_for_main(link.connection_id()).or_else(|| {
                if link.dual_state() != PrimaryDualChannelState::WaitingForRdbChannel {
                    return None;
                }
                if let Some((rdb, _owner)) = self
                    .rdb_psync_waits
                    .iter()
                    .find(|(_, wait)| wait.main == Some(link.connection_id()))
                {
                    return Some(*rdb);
                }
                let candidates: Vec<ConnectionId> = self
                    .channel_records
                    .iter()
                    .filter_map(|(&connection_id, record)| {
                        matches!(
                            record.role,
                            Some(ReplicationConnection::RdbChannelCandidate)
                        )
                        .then_some(connection_id)
                    })
                    .collect();
                let unassigned_waiting_mains = self
                    .primary_links
                    .values()
                    .filter(|candidate| {
                        candidate.dual_state() == PrimaryDualChannelState::WaitingForRdbChannel
                    })
                    .map(|candidate| candidate.connection_id())
                    .filter(|main| {
                        !self
                            .rdb_psync_waits
                            .values()
                            .any(|wait| wait.main == Some(*main))
                    })
                    .count();
                if candidates.len() == 1 && unassigned_waiting_mains == 1 {
                    Some(candidates[0])
                } else {
                    None
                }
            });
            if let Some(rdb_connection_id) = rdb_connection_id {
                let rdb_id = rdb_connection_id.0;
                let (rdb_ip, rdb_port) = self
                    .channel_endpoint(rdb_connection_id)
                    .map(|(ip, port)| (ip.to_string(), port))
                    .unwrap_or_default();
                let state = self
                    .channel_records
                    .get(&rdb_connection_id)
                    .and_then(|record| record.state.clone())
                    .unwrap_or_else(|| "wait_bgsave".to_string());
                view.channel_info.insert(
                    connection_id,
                    crate::engine::distributed::replication::ReplicationChannelInfo {
                        channel_type: "main-channel",
                        state: main_channel_state_name(link).to_string(),
                        ip: link.replica().ip.clone(),
                        port: link.replica().port,
                    },
                );
                view.channel_info.insert(
                    rdb_id,
                    crate::engine::distributed::replication::ReplicationChannelInfo {
                        channel_type: "rdb-channel",
                        state,
                        ip: rdb_ip,
                        port: rdb_port,
                    },
                );
            } else if is_dual_main_channel(link) {
                view.channel_info.insert(
                    connection_id,
                    crate::engine::distributed::replication::ReplicationChannelInfo {
                        channel_type: "main-channel",
                        state: main_channel_state_name(link).to_string(),
                        ip: link.replica().ip.clone(),
                        port: link.replica().port,
                    },
                );
            } else {
                view.replica_info.insert(
                    connection_id,
                    crate::engine::distributed::replication::ReplicaStatus {
                        ip: link.replica().ip.clone(),
                        port: link.replica().port,
                        offset: link.last_ack_position(),
                        state: main_channel_state_name(link).to_string(),
                        lag_ms: lag,
                    },
                );
            }
            if let Some(stats) = link.compression_stats() {
                view.compression_stats.insert(connection_id, stats);
            }
        }
        for (&connection_id, record) in &self.channel_records {
            if !matches!(
                record.role,
                Some(ReplicationConnection::RdbChannelCandidate)
            ) {
                continue;
            }
            let connection_id = connection_id.0;
            let (ip, port) = self
                .channel_endpoint(ConnectionId(connection_id))
                .map(|(ip, port)| (ip.to_string(), port))
                .unwrap_or_default();
            view.channel_info.insert(
                connection_id,
                crate::engine::distributed::replication::ReplicationChannelInfo {
                    channel_type: "rdb-channel",
                    state: record
                        .state
                        .clone()
                        .unwrap_or_else(|| "wait_bgsave".to_string()),
                    ip,
                    port,
                },
            );
        }
        // C appends to `server.replicas` only when PSYNC is accepted
        // (replication.c:961 for partial CONTINUE and the dual-channel main
        // PSYNC, replication.c:1230 for full-resync start), not when REPLCONF
        // registers the connection. Order admitted links by the acceptance
        // sequence so concurrently connecting replicas keep stable slaveN
        // numbering; links not yet past PSYNC acceptance remain behind, in
        // registration order.
        let mut ordered_links: Vec<_> = self.primary_links.iter().collect();
        ordered_links.sort_by_key(|(replica_id, link)| match link.accept_seq() {
            Some(seq) => (0, seq),
            None => (1, replica_id.0),
        });
        view.replica_order = ordered_links
            .iter()
            .filter(|(_, link)| !is_pre_psync_main_link(link))
            .map(|(_, link)| link.connection_id().0)
            .collect();
        for (_, link) in &ordered_links {
            if is_pre_psync_main_link(link) {
                continue;
            }
            let main = link.connection_id();
            if let Some(rdb) = self.rdb_channel_for_main(main) {
                view.info_order.push(rdb.0);
            }
            view.info_order.push(main.0);
        }
        let mut unpaired_channels: Vec<_> = view
            .channel_info
            .iter()
            .filter(|(id, _)| !view.info_order.contains(id))
            .collect();
        unpaired_channels.sort_by(|(left_id, left), (right_id, right)| {
            (left.ip.as_str(), left.port, **left_id).cmp(&(
                right.ip.as_str(),
                right.port,
                **right_id,
            ))
        });
        view.info_order
            .extend(unpaired_channels.into_iter().map(|(id, _)| *id));
        view
    }

    /// Number of replicas in C's `REPLICA_STATE_ONLINE` equivalent. A dual
    /// replica remains off-line while it waits for the terminal ACK after main
    /// PSYNC, even though its main transport is already `Streaming`.
    pub fn live_replica_count(&self) -> usize {
        self.primary_links
            .values()
            .filter(|link| classic_link_is_online(link))
            .count()
    }

    pub fn connected_replica_count(&self) -> usize {
        self.primary_links
            .values()
            .filter(|link| {
                link.state() != PrimaryLinkState::Closing && !is_pre_psync_main_link(link)
            })
            .map(|link| 1 + usize::from(self.rdb_channel_for_main(link.connection_id()).is_some()))
            .sum()
    }

    /// Count replicas that C exposes as `REPLICA_STATE_ONLINE` and whose
    /// liveness timestamp is within the configured lag bound. A classic
    /// diskless replica enters this state when its EOF marker drains, while
    /// command-stream delivery remains gated on its first ACK.
    pub fn count_good_replicas(&self, max_lag_ms: i64, now_ms: i64) -> usize {
        self.primary_links
            .values()
            .filter(|link| classic_link_is_online(link))
            .filter(|link| {
                max_lag_ms == 0 || now_ms.saturating_sub(link.last_ack_time_ms()) <= max_lag_ms
            })
            .count()
    }

    /// Lowest backlog cursor any connected replica still needs. Includes
    /// normal links past PSYNC and RDB clients waiting for their associated
    /// main PSYNC. `None` lets eviction trim strictly to backlog capacity.
    pub fn min_protected_backlog_cursor(&self) -> Option<BacklogCursor> {
        self.min_protected_backlog_cursor_with_wait(true)
    }

    /// Return the floor held by streaming links while excluding RDB wait-set
    /// references. During diskless-sync delay C has not created the RDB child
    /// yet, so those references must not retain the replication buffer early.
    pub fn min_protected_backlog_cursor_without_psync_wait(&self) -> Option<BacklogCursor> {
        self.min_protected_backlog_cursor_with_wait(false)
    }

    fn min_protected_backlog_cursor_with_wait(
        &self,
        include_psync_wait: bool,
    ) -> Option<BacklogCursor> {
        let mut cursors: Vec<BacklogCursor> = self
            .primary_links
            .values()
            .filter(|link| {
                matches!(
                    link.dual_state(),
                    PrimaryDualChannelState::Disabled | PrimaryDualChannelState::Online
                ) || (link.dual_state() == PrimaryDualChannelState::WaitingForReplicaAck
                    && link.has_pending_replication())
            })
            .filter(|link| {
                !matches!(
                    link.state(),
                    PrimaryLinkState::Closing
                        | PrimaryLinkState::AwaitingPsync
                        | PrimaryLinkState::PreparingFullResync
                )
            })
            .map(|link| link.backlog_protection_cursor())
            .chain(
                self.primary_links
                    .values()
                    .filter_map(|link| link.dual_main_backlog_pin_cursor()),
            )
            .collect();
        if include_psync_wait {
            cursors.extend(
                self.rdb_psync_waits
                    .values()
                    .filter_map(|wait| wait.pin_cursor),
            );
            cursors.extend(
                self.retained_rdb_psync_waits
                    .values()
                    .filter_map(|retained| retained.wait.pin_cursor),
            );
            cursors.extend(
                self.grace_psync_pin_by_main
                    .values()
                    .filter_map(|wait| wait.pin_cursor),
            );
        }
        cursors.into_iter().min()
    }

    /// Count Streaming replicas whose last ACK offset is at least `min_offset`
    /// and whose last ACK was received within `max_lag_ms` milliseconds from `now_ms`.
    /// A `max_lag_ms` of 0 disables the lag check (counts by offset only), which
    /// matches WAIT's fast path when `numreplicas` is already met synchronously.
    pub fn count_acked_replicas(
        &self,
        min_offset: ReplicationOffset,
        max_lag_ms: i64,
        now_ms: i64,
    ) -> usize {
        self.primary_links
            .values()
            .filter(|l| l.state() == PrimaryLinkState::Streaming)
            .filter(|link| {
                link.last_ack_position()
                    .is_some_and(|offset| offset >= min_offset)
            })
            .filter(|l| {
                max_lag_ms == 0 || now_ms.saturating_sub(l.last_ack_time_ms()) <= max_lag_ms
            })
            .count()
    }
}

fn primary_link_state_name(state: PrimaryLinkState) -> &'static str {
    match state {
        PrimaryLinkState::Streaming | PrimaryLinkState::WaitingForReplicaAck => "online",
        PrimaryLinkState::Closing => "offline",
        _ => "wait_bgsave",
    }
}

fn classic_link_is_online(link: &PrimaryLink) -> bool {
    matches!(
        link.state(),
        PrimaryLinkState::Streaming | PrimaryLinkState::WaitingForReplicaAck
    ) && link.dual_state() != PrimaryDualChannelState::WaitingForReplicaAck
}

pub(super) fn main_channel_state_name(link: &PrimaryLink) -> &'static str {
    if link.dual_state() == PrimaryDualChannelState::WaitingForRdbChannel {
        return "bg_transfer";
    }
    if link.dual_state() == PrimaryDualChannelState::WaitingForReplicaAck {
        return "bg_transfer";
    }
    primary_link_state_name(link.state())
}

fn is_dual_main_channel(link: &PrimaryLink) -> bool {
    matches!(
        link.dual_state(),
        PrimaryDualChannelState::WaitingForRdbChannel
            | PrimaryDualChannelState::SendingRdb
            | PrimaryDualChannelState::WaitingForMainPsync
            | PrimaryDualChannelState::WaitingForReplicaAck
    )
}

fn is_pre_psync_main_link(link: &PrimaryLink) -> bool {
    link.state() == PrimaryLinkState::AwaitingPsync
        && matches!(
            link.dual_state(),
            PrimaryDualChannelState::Disabled | PrimaryDualChannelState::Negotiating
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::distributed::cluster::state::ClusterReplicaHealth;

    const LOCAL_REPLICA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const DOWNSTREAM_REPLICA: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn master() -> MasterInfo {
        MasterInfo {
            host: "127.0.0.1".to_string(),
            port: 6379,
        }
    }

    fn replica_info() -> ReplicaConnectionInfo {
        ReplicaConnectionInfo {
            ip: "127.0.0.1".to_string(),
            port: 6380,
            capability: Vec::new(),
            rdb_filter_mode: RdbFilterMode::All,
        }
    }

    fn cursor(value: i64) -> crate::engine::distributed::replication::BacklogCursor {
        crate::engine::distributed::replication::BacklogCursor::try_from(value)
            .expect("test cursor must be non-negative")
    }

    fn offset(value: i64) -> crate::engine::distributed::replication::ReplicationOffset {
        crate::engine::distributed::replication::ReplicationOffset::try_from(value)
            .expect("valid test offset")
    }

    fn boundary(value: usize) -> crate::engine::distributed::replication::SnapshotBoundary {
        let mut backlog = ReplicationBacklog::new("boundary".to_string(), value.max(1));
        backlog.append(&vec![b'x'; value]).unwrap();
        backlog.snapshot_boundary()
    }

    #[test]
    fn typed_backlog_protection_is_none_without_live_pins() {
        let runtime = ReplicationRuntime::new();

        assert_eq!(runtime.min_protected_backlog_cursor(), None);
        assert_eq!(
            runtime.min_protected_backlog_cursor_without_psync_wait(),
            None
        );
    }

    #[test]
    fn typed_backlog_protection_selects_the_lowest_wait_pin() {
        let mut runtime = ReplicationRuntime::new();
        runtime.rdb_psync_waits.insert(
            ConnectionId(30),
            RdbPsyncWait {
                boundary: boundary(42),
                main: None,
                pin_cursor: Some(cursor(42)),
                pin_from_existing_tail: true,
            },
        );
        runtime.rdb_psync_waits.insert(
            ConnectionId(31),
            RdbPsyncWait {
                boundary: boundary(17),
                main: None,
                pin_cursor: Some(cursor(17)),
                pin_from_existing_tail: true,
            },
        );

        assert_eq!(runtime.min_protected_backlog_cursor(), Some(cursor(17)));
        assert_eq!(
            runtime.min_protected_backlog_cursor_without_psync_wait(),
            None
        );
    }

    #[test]
    fn cluster_view_maps_disconnected_replica() {
        let runtime = ReplicationRuntime::new();

        let view = runtime.cluster_view(LOCAL_REPLICA, false);

        assert_eq!(view.health, ClusterReplicaHealth::Disconnected);
        assert_eq!(view.received_offset, offset(0));
        assert_eq!(view.applied_offset, offset(0));
        assert_eq!(view.acknowledged_offset, offset(0));
    }

    #[test]
    fn cluster_view_maps_handshake_and_rdb_receive_to_loading() {
        let mut runtime = ReplicationRuntime::new();
        let mut link = ReplicaLink::new(ConnectionId(7), master());
        link.set_cluster_node_id(Some(LOCAL_REPLICA.to_string()));
        link.connected(None, None);
        runtime.set_replica_link(link);

        assert_eq!(
            runtime.cluster_view(LOCAL_REPLICA, false).health,
            ClusterReplicaHealth::Loading
        );

        let link = runtime.replica_link_mut().unwrap();
        link.on_bytes(b"+PONG\r\n", 6380).unwrap();
        link.on_bytes(
            b"+OK\r\n+OK\r\n+OK\r\n+OK\r\n+FULLRESYNC 0123456789012345678901234567890123456789 42\r\n$5\r\n",
            6380,
        )
        .unwrap();

        let view = runtime.cluster_view(LOCAL_REPLICA, false);
        assert_eq!(view.health, ClusterReplicaHealth::Loading);
        assert_eq!(view.received_offset, offset(42));
        // A fresh full-sync snapshot is received before it is applied or ACKed.
        assert_eq!(view.applied_offset, offset(0));
        assert_eq!(view.acknowledged_offset, offset(0));
    }

    #[test]
    fn cluster_view_keeps_async_loading_out_of_online_state() {
        let mut runtime = ReplicationRuntime::new();
        let mut link = ReplicaLink::streaming(
            ConnectionId(8),
            master(),
            "0123456789012345678901234567890123456789".to_string(),
            offset(43),
        );
        link.set_cluster_node_id(Some(LOCAL_REPLICA.to_string()));
        runtime.set_replica_link(link);

        assert_eq!(
            runtime.cluster_view(LOCAL_REPLICA, true).health,
            ClusterReplicaHealth::Loading
        );
    }

    #[test]
    fn cluster_view_keeps_streaming_link_loading_until_rdb_commit() {
        let mut runtime = ReplicationRuntime::new();
        let mut link = ReplicaLink::streaming(
            ConnectionId(12),
            master(),
            "0123456789012345678901234567890123456789".to_string(),
            offset(43),
        );
        link.set_cluster_node_id(Some(LOCAL_REPLICA.to_string()));
        link.transition_rdb_channel_state(ReplicaRdbChannelState::Connecting)
            .unwrap();
        link.transition_rdb_channel_state(ReplicaRdbChannelState::Handshake)
            .unwrap();
        link.transition_rdb_channel_state(ReplicaRdbChannelState::WaitingForEndOffset)
            .unwrap();
        link.transition_rdb_channel_state(ReplicaRdbChannelState::ReceivingRdb)
            .unwrap();
        runtime.set_replica_link(link);

        assert_eq!(
            runtime.cluster_view(LOCAL_REPLICA, false).health,
            ClusterReplicaHealth::Loading
        );
    }

    #[test]
    fn cluster_view_reports_monotonic_streaming_offsets() {
        let mut runtime = ReplicationRuntime::new();
        let mut link = ReplicaLink::streaming(
            ConnectionId(9),
            master(),
            "0123456789012345678901234567890123456789".to_string(),
            offset(44),
        );
        link.set_cluster_node_id(Some(LOCAL_REPLICA.to_string()));
        runtime.set_replica_link(link);

        let view = runtime.cluster_view(LOCAL_REPLICA, false);

        assert_eq!(view.health, ClusterReplicaHealth::Online);
        assert_eq!(view.received_offset, offset(44));
        assert_eq!(view.applied_offset, offset(44));
        assert_eq!(view.acknowledged_offset, offset(44));
        assert!(view.acknowledged_offset <= view.applied_offset);
        assert!(view.applied_offset <= view.received_offset);
    }

    #[test]
    fn cluster_view_maps_downstream_ack_and_stale_link() {
        let mut runtime = ReplicationRuntime::new();
        let connection_id = ConnectionId(10);
        runtime.set_replica_node_id(connection_id, DOWNSTREAM_REPLICA.to_string());
        let replica_id = runtime.register_primary(connection_id, replica_info());
        let link = runtime.primary_link_mut(replica_id).unwrap();
        link.set_state(PrimaryLinkState::Streaming);
        link.acknowledge(offset(45), 1_000);

        let online = runtime.cluster_view(DOWNSTREAM_REPLICA, false);
        assert_eq!(online.health, ClusterReplicaHealth::Online);
        assert_eq!(online.received_offset, offset(45));
        assert_eq!(online.applied_offset, offset(45));
        assert_eq!(online.acknowledged_offset, offset(45));

        runtime
            .primary_link_mut(replica_id)
            .unwrap()
            .set_state(PrimaryLinkState::Closing);
        assert_eq!(
            runtime.cluster_view(DOWNSTREAM_REPLICA, false).health,
            ClusterReplicaHealth::Stale
        );
    }

    #[test]
    fn cluster_view_keeps_zero_ack_downstream_loading() {
        let mut runtime = ReplicationRuntime::new();
        let connection_id = ConnectionId(13);
        runtime.set_replica_node_id(connection_id, DOWNSTREAM_REPLICA.to_string());
        let replica_id = runtime.register_primary(connection_id, replica_info());
        let link = runtime.primary_link_mut(replica_id).unwrap();
        link.set_state(PrimaryLinkState::Streaming);
        link.acknowledge(offset(0), 1_000);

        assert_eq!(
            runtime.cluster_view(DOWNSTREAM_REPLICA, false).health,
            ClusterReplicaHealth::Loading
        );
    }

    #[test]
    fn classic_diskless_ack_gate_is_visible_as_online_without_counting_as_acked() {
        let mut runtime = ReplicationRuntime::new();
        let replica_id = runtime.register_primary(ConnectionId(14), replica_info());
        let link = runtime.primary_link_mut(replica_id).unwrap();
        link.finish_full_resync();
        let completion_time_ms = link.last_ack_time_ms();

        let view = runtime.rebuild_view();

        assert_eq!(view.replica_info[&14].state, "online");
        assert_eq!(runtime.live_replica_count(), 1);
        assert_eq!(
            runtime.count_good_replicas(3_000, completion_time_ms),
            1,
            "C replicaPutOnline refreshes good-replica liveness before the first ACK"
        );
        assert_eq!(runtime.count_acked_replicas(offset(0), 0, 1_000), 0);
    }

    #[test]
    fn cluster_view_rebuild_indexes_live_links_by_cluster_node_id() {
        let mut runtime = ReplicationRuntime::new();
        let connection_id = ConnectionId(11);
        runtime.set_replica_node_id(connection_id, DOWNSTREAM_REPLICA.to_string());
        let replica_id = runtime.register_primary(connection_id, replica_info());
        let link = runtime.primary_link_mut(replica_id).unwrap();
        link.set_state(PrimaryLinkState::Streaming);
        link.acknowledge(offset(46), 1_000);

        let view = runtime.rebuild_view();

        assert_eq!(
            view.cluster_replica_views[DOWNSTREAM_REPLICA].acknowledged_offset,
            offset(46)
        );
    }

    #[test]
    fn aof_ack_count_uses_fack_offset_not_normal_ack_offset() {
        let mut runtime = ReplicationRuntime::new();
        let id1 = runtime.register_primary(ConnectionId(21), replica_info());
        let id2 = runtime.register_primary(ConnectionId(22), replica_info());
        for id in [id1, id2] {
            runtime
                .primary_link_mut(id)
                .expect("primary link must exist")
                .set_state(PrimaryLinkState::Streaming);
        }
        let link1 = runtime.primary_link_mut(id1).unwrap();
        link1.acknowledge(offset(100), 1_000);
        link1.acknowledge_aof(offset(40));
        let link2 = runtime.primary_link_mut(id2).unwrap();
        link2.acknowledge(offset(100), 1_000);
        link2.acknowledge_aof(offset(100));

        let view = runtime.rebuild_view();
        let normal_acks = view
            .replica_info
            .values()
            .filter(|info| {
                info.state == "online" && info.offset.is_some_and(|value| value >= offset(100))
            })
            .count();
        let aof_acks = view
            .replica_info
            .iter()
            .filter(|(_, info)| info.state == "online")
            .filter(|(id, _)| {
                view.replica_aof_ack_offsets
                    .get(id)
                    .copied()
                    .flatten()
                    .is_some_and(|value| value >= offset(100))
            })
            .count();
        assert_eq!(normal_acks, 2);
        assert_eq!(aof_acks, 1);
        assert_eq!(view.replica_aof_ack_offsets[&21], Some(offset(40)));
        assert_eq!(view.replica_aof_ack_offsets[&22], Some(offset(100)));
    }
}
