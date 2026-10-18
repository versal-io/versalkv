//! Replication tick planning: pure decision logic that turns observed state
//! into ordered [`ReplicationAction`]s. L5 owns transport, RDB generation and
//! connection teardown; this module only reads/writes `ReplicationRuntime`
//! bookkeeping and surfaces the intended effects.

use std::net::{IpAddr, SocketAddr};

use super::{
    ConnectionId, MasterInfo, MasterReconnectCause, PrimaryDualChannelState, ReplicaId,
    ReplicaLink, ReplicationRuntime,
};

/// Snapshot of L5-owned state that the replication tick needs to decide on.
/// Gathered by the L5 `drive_replication` entry before calling
/// [`ReplicationRuntime::plan_tick`]. Anything that requires `ServerContext`
/// or the transport layer is pre-computed here so the planner stays
/// transport-independent.
pub struct ReplicationTickFacts {
    pub now_ms: i64,
    pub master_host: Option<String>,
    pub master_port: u16,
    pub master_link_status: String,
    pub auth_rejected: bool,
    pub abort_for_cluster_failure: bool,
    pub link_timed_out: bool,
    pub force_full_sync_on_next_reconnect: bool,
    pub dual_channel_replication: bool,
    pub failover_in_progress: bool,
    pub cluster_node_id: Option<String>,
    /// `None` when not configured; an invalid configured value is signalled via
    /// `bind_source_addr_invalid` so the planner can schedule a reconnect.
    pub bind_source_addr: Option<IpAddr>,
    pub bind_source_addr_invalid: bool,
    pub repl_mptcp: bool,
    pub tls_replication: bool,
    pub pending_replicaof: bool,
    pub resolved_master_addr: Option<SocketAddr>,
    /// Whether grace-held PSYNC waits were released at the start of this tick.
    /// Expiry itself mutates runtime state and is performed by L5 before
    /// planning so the `before_sleep` path can share the same expiry call.
    pub grace_waits_released: bool,
}

/// Why a replication connection is being closed. Drives the L5 log message
/// and distinguishes primary-side PSYNC grace expiry from replica-side
/// failures.
#[derive(Clone, Debug)]
pub enum CloseReason {
    DualChannelPsyncGrace,
    ClusterFailure {
        host: String,
        port: u16,
    },
    Timeout {
        host: String,
        port: u16,
        in_handshake: bool,
    },
    StaleLink,
}

/// Ordered effects emitted by [`ReplicationRuntime::plan_tick`]. L5 executes
/// them in order; each variant maps to a narrow execution step.
pub enum ReplicationAction {
    /// Refresh the cached INFO/Cluster replication view after grace-PSYNC
    /// members were released.
    RefreshView,
    /// Tear down a single connection (primary main channel that missed its
    /// PSYNC grace deadline, or the replica main link on failure).
    Close {
        connection_id: ConnectionId,
        reason: CloseReason,
    },
    /// Tear down both the replica main and RDB links because no upstream is
    /// configured.
    CloseReplicaLinks,
    ScheduleReconnect(MasterReconnectCause),
    /// A dual-channel replica's RDB wait timed out; send a classic full
    /// resync on the surviving main connection.
    StartFullResync {
        connection_id: ConnectionId,
        primary_id: ReplicaId,
    },
    ApplyPendingReplicaof,
    ConnectUpstream {
        host: String,
        port: u16,
        addr: SocketAddr,
        bind_source_addr: Option<IpAddr>,
        use_mptcp: bool,
        use_tls: bool,
        dual_channel_enabled: bool,
        failover_in_progress: bool,
        cluster_node_id: Option<String>,
        force_full_sync: bool,
        force_full_sync_from_reconfigure: bool,
        resume: Option<(
            String,
            crate::engine::distributed::replication::ReplicationOffset,
            u32,
        )>,
    },
}

impl ReplicationRuntime {
    /// Decide the replication tick's ordered effects from runtime state plus
    /// L5-supplied facts.
    /// Mutations here are restricted to `ReplicationRuntime` bookkeeping
    /// (grace-wait expiry, dual-channel state transitions, failed-replica
    /// marking). Transport, RDB serialization and connection teardown are
    /// deferred to L5 via the returned actions.
    pub fn plan_tick(&mut self, facts: &ReplicationTickFacts) -> Vec<ReplicationAction> {
        let mut actions = Vec::new();

        if facts.grace_waits_released {
            // Publish the release immediately: INFO replication's
            // replicas_waiting_psync is read from this cached view, and an
            // unrelated state change (e.g. a reconnecting replica) may be the
            // next refresh only seconds later.
            actions.push(ReplicationAction::RefreshView);
        }

        self.plan_dual_channel_timeouts(facts, &mut actions);
        self.plan_replica_side(facts, &mut actions);

        actions
    }

    fn plan_dual_channel_timeouts(
        &mut self,
        facts: &ReplicationTickFacts,
        actions: &mut Vec<ReplicationAction>,
    ) {
        let timed_out: Vec<(ReplicaId, ConnectionId, String, u16, bool)> = self
            .primary_links()
            .iter()
            .filter_map(|(&id, link)| {
                let state = link.dual_state();
                let waiting = matches!(
                    state,
                    PrimaryDualChannelState::WaitingForRdbChannel
                        | PrimaryDualChannelState::WaitingForMainPsync
                );
                if !waiting {
                    return None;
                }
                let deadline = link.rdb_channel_wait_deadline_ms?;
                (facts.now_ms >= deadline).then(|| {
                    (
                        id,
                        link.connection_id(),
                        link.replica().ip.clone(),
                        link.replica().port,
                        matches!(state, PrimaryDualChannelState::WaitingForMainPsync),
                    )
                })
            })
            .collect();

        for (id, conn_id, ip, port, waiting_for_main) in timed_out {
            if waiting_for_main {
                actions.push(ReplicationAction::Close {
                    connection_id: conn_id,
                    reason: CloseReason::DualChannelPsyncGrace,
                });
                continue;
            }
            // RDB-side wait timed out: fall back to a classic full resync on
            // the main channel. The dual-channel session is abandoned here.
            if let Some(link) = self.primary_link_mut(id) {
                let _ = link.transition_dual_state(PrimaryDualChannelState::Aborting);
                let _ = link.transition_dual_state(PrimaryDualChannelState::Disabled);
                link.rdb_channel_wait_deadline_ms = None;
                link.dual_channel_already_failed = true;
            }
            self.mark_replica_dual_channel_failed(ip, port);
            actions.push(ReplicationAction::StartFullResync {
                connection_id: conn_id,
                primary_id: id,
            });
        }
    }

    fn plan_replica_side(
        &mut self,
        facts: &ReplicationTickFacts,
        actions: &mut Vec<ReplicationAction>,
    ) {
        if self.plan_replica_early_exit(facts, actions) {
            return;
        }
        if self.plan_replica_link_failure(facts, actions) {
            return;
        }
        self.plan_replica_connect(facts, actions);
    }

    /// Pending target change, no configured master, or rejected authentication.
    /// Returns true when the tick should stop here.
    fn plan_replica_early_exit(
        &mut self,
        facts: &ReplicationTickFacts,
        actions: &mut Vec<ReplicationAction>,
    ) -> bool {
        if facts.pending_replicaof {
            actions.push(ReplicationAction::ApplyPendingReplicaof);
            // ApplyPendingReplicaof may change master_host via ctx; the next
            // tick re-evaluates the replica side.
            return true;
        }
        if facts.master_host.is_none() {
            actions.push(ReplicationAction::CloseReplicaLinks);
            return true;
        }
        facts.auth_rejected
    }

    /// Cluster-driven abort or replication timeout. Returns true when the tick
    /// should stop here.
    fn plan_replica_link_failure(
        &mut self,
        facts: &ReplicationTickFacts,
        actions: &mut Vec<ReplicationAction>,
    ) -> bool {
        if facts.abort_for_cluster_failure {
            self.push_close_replica_link_with_reason(actions, |link| CloseReason::ClusterFailure {
                host: link.master().host.clone(),
                port: link.master().port,
            });
            actions.push(ReplicationAction::ScheduleReconnect(
                MasterReconnectCause::ClusterPrimaryUnavailable,
            ));
            return true;
        }
        if facts.link_timed_out {
            self.push_close_replica_link_with_reason(actions, |link| CloseReason::Timeout {
                host: link.master().host.clone(),
                port: link.master().port,
                in_handshake: link.is_in_handshake(),
            });
            actions.push(ReplicationAction::ScheduleReconnect(
                MasterReconnectCause::ReplicationTimeout,
            ));
            return true;
        }
        false
    }

    fn push_close_replica_link_with_reason(
        &mut self,
        actions: &mut Vec<ReplicationAction>,
        reason: impl FnOnce(&ReplicaLink) -> CloseReason,
    ) {
        if let Some(link) = self.replica_link() {
            let r = reason(link);
            actions.push(ReplicationAction::Close {
                connection_id: link.connection_id(),
                reason: r,
            });
        }
    }

    fn plan_replica_connect(
        &mut self,
        facts: &ReplicationTickFacts,
        actions: &mut Vec<ReplicationAction>,
    ) {
        if !self.master_connect_allowed(facts.now_ms) {
            return;
        }
        let host = facts.master_host.clone().unwrap();
        let port = facts.master_port;
        let master = MasterInfo {
            host: host.clone(),
            port,
        };

        // Short-circuit when a ReplicaLink already exists for this master and
        // has not been torn down. When the poll timeout is aggressive (1 ms)
        // we loop `drive_replication` much more often than C Valkey's
        // serverCron. Without this short-circuit, repeated REPLICAOF <host>
        // <port> assertions in PSYNC2 integration tests can race the
        // scheduler: cron fires before the previous handshake has registered
        // `master_link_status != "down"` and produces a duplicate connect
        // attempt, causing one of the adjacent handshake cycles to never
        // increment `sync_partial_ok` on the primary.
        if self
            .replica_link()
            .is_some_and(|l| l.master().host == host && l.master().port == port)
        {
            return;
        }
        if let Some(link) = self.replica_link() {
            if link.master().host == host
                && link.master().port == port
                && facts.master_link_status != "down"
            {
                return;
            }
            actions.push(ReplicationAction::Close {
                connection_id: link.connection_id(),
                reason: CloseReason::StaleLink,
            });
        }

        if facts.bind_source_addr_invalid || facts.resolved_master_addr.is_none() {
            actions.push(ReplicationAction::ScheduleReconnect(
                MasterReconnectCause::ResolveFailed,
            ));
            return;
        }

        let addr = facts.resolved_master_addr.unwrap();
        let force_full_sync_from_reconfigure = facts.force_full_sync_on_next_reconnect;
        let force_full_sync = self.take_force_full_sync() || force_full_sync_from_reconfigure;

        let resume = if force_full_sync {
            None
        } else {
            self.replica_resume_for(&master)
                .map(|(replid, offset, db)| (replid.to_string(), offset, db))
        };

        actions.push(ReplicationAction::ConnectUpstream {
            host,
            port,
            addr,
            bind_source_addr: facts.bind_source_addr,
            use_mptcp: facts.repl_mptcp,
            use_tls: facts.tls_replication,
            dual_channel_enabled: facts.dual_channel_replication,
            failover_in_progress: facts.failover_in_progress,
            cluster_node_id: facts.cluster_node_id.clone(),
            force_full_sync,
            force_full_sync_from_reconfigure,
            resume,
        });
    }
}
