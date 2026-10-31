use std::collections::{HashMap, VecDeque};

use crate::engine::distributed::cluster::action::{ClusterAction, ClusterActionBatch};
use crate::engine::distributed::cluster::node::{
    CLUSTER_NODE_FAIL, CLUSTER_NODE_NOFAILOVER, CLUSTER_NODE_PFAIL,
};
use crate::engine::distributed::cluster::state::failover::{
    AutomaticFailoverFacts, AutomaticFailoverPhase, FailedPrimaryFacts, LocalReplicaFailoverStatus,
    ReplicaEligibility,
};
use crate::engine::distributed::cluster::state::ClusterState;
use crate::engine::distributed::cluster::NodeId;

use super::super::super::types::ClusterBusManager;

impl ClusterBusManager {
    pub(super) fn automatic_failover_facts(
        &self,
        state: &ClusterState,
        now_ms: i64,
        node_timeout_ms: u64,
        repl_offsets: &HashMap<String, i64>,
        local_status: &LocalReplicaFailoverStatus,
    ) -> AutomaticFailoverFacts {
        let primary = state
            .nodes
            .get(&state.myself.replicaof)
            .and_then(|primary| {
                Some(FailedPrimaryFacts {
                    node_id: NodeId::parse(primary.name.as_bytes()).ok()?,
                    shard_id: NodeId::parse(primary.shard_id.as_bytes()).ok()?,
                    slots: primary.slots.clone(),
                    is_failed: primary.has_flag(CLUSTER_NODE_FAIL),
                })
            });
        let candidate = |node: &crate::engine::distributed::cluster::node::ClusterNode| {
            let node_id = NodeId::parse(node.name.as_bytes()).ok()?;
            let cluster_link_connected = if node.is_myself() {
                self.connections.values().any(|connection| {
                    connection.connected
                        && state.nodes.get(&connection.node_name).is_some_and(|peer| {
                            ClusterState::is_voting_primary(peer)
                                && !peer.has_flag(CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL)
                        })
                })
            } else {
                self.connections
                    .values()
                    .any(|connection| connection.connected && connection.node_name == node.name)
            };
            Some(ReplicaEligibility {
                node_id,
                no_failover: node.has_flag(CLUSTER_NODE_NOFAILOVER),
                cluster_link_connected,
                cluster_view_stale: node.has_flag(CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL),
                loading: node.is_myself() && local_status.loading,
                priority: if node.is_myself() {
                    local_status.priority
                } else {
                    node.replica_priority
                },
                cluster_replica_priority: node.cluster_replica_priority,
                replication_offset: repl_offsets
                    .get(&node.name)
                    .copied()
                    .unwrap_or(node.repl_offset),
                data_age_ms: if node.is_myself() {
                    local_status.data_age_ms
                } else {
                    0
                },
                node_timeout_ms,
                repl_ping_period_ms: local_status.repl_ping_period_ms,
                replica_validity_factor: local_status.replica_validity_factor,
            })
        };
        let local = state
            .myself
            .is_replica()
            .then(|| candidate(&state.myself))
            .flatten();
        let siblings = local
            .clone()
            .into_iter()
            .chain(
                state
                    .nodes
                    .values()
                    .filter(|node| node.is_replica() && node.replicaof == state.myself.replicaof)
                    .filter_map(candidate),
            )
            .collect();
        let failed_primary_rank = primary
            .as_ref()
            .map(|local_primary| {
                state
                    .nodes
                    .values()
                    .filter(|node| {
                        ClusterState::is_voting_primary(node)
                            && node.has_flag(CLUSTER_NODE_FAIL)
                            && node.shard_id.as_str() < local_primary.shard_id.as_str()
                            && state.nodes.values().any(|replica| {
                                replica.is_replica() && replica.replicaof == node.name
                            })
                    })
                    .count()
            })
            .unwrap_or_default();
        let all_replicas_primary_failed = primary.as_ref().is_some_and(|primary| {
            primary.is_failed
                && state.myself.has_flag(
                    crate::engine::distributed::cluster::node::CLUSTER_NODE_MY_PRIMARY_FAIL,
                )
                && state
                    .nodes
                    .values()
                    .filter(|node| node.is_replica() && node.replicaof == primary.node_id.as_str())
                    .all(|node| {
                        node.has_flag(
                            crate::engine::distributed::cluster::node::CLUSTER_NODE_MY_PRIMARY_FAIL,
                        )
                    })
        });
        AutomaticFailoverFacts {
            now_ms,
            node_timeout_ms,
            current_epoch: state.current_epoch,
            voting_primaries: state.primary_count(),
            failed_primary_rank,
            all_replicas_primary_failed,
            local,
            primary,
            siblings,
            manual: state.manual_failover.is_ready_for_election(),
        }
    }

    pub(in crate::engine::distributed::cluster::bus) fn tick_automatic_failover(
        &mut self,
        state: &mut ClusterState,
        now_ms: i64,
        node_timeout_ms: u64,
        repl_offsets: &HashMap<String, i64>,
        local_status: &LocalReplicaFailoverStatus,
    ) -> ClusterActionBatch {
        let facts = self.automatic_failover_facts(
            state,
            now_ms,
            node_timeout_ms,
            repl_offsets,
            local_status,
        );
        let previous_phase = state.automatic_failover.phase();
        let previous_deadline_ms = state.automatic_failover.deadline_ms();
        let mut actions = state.automatic_failover_on_tick(&facts);
        if previous_phase == AutomaticFailoverPhase::RequestingVotes
            && state.automatic_failover.phase() == AutomaticFailoverPhase::WaitingRankDelay
        {
            crate::server_log!(notice, "Failover attempt expired.");
        }
        let should_queue_status = previous_phase == AutomaticFailoverPhase::Idle
            && state.automatic_failover.phase() == AutomaticFailoverPhase::WaitingRankDelay;
        if should_queue_status {
            let offset = facts
                .local
                .as_ref()
                .map(|local| local.replication_offset)
                .unwrap_or_default();
            if !facts.manual
                && state.automatic_failover.rank() == 0
                && state.automatic_failover.failed_primary_rank() == 0
                && offset != 0
                && facts.all_replicas_primary_failed
            {
                crate::server_log!(
                    notice,
                    "This is the best ranked replica and can initiate the election immediately."
                );
            }
            crate::server_log!(
                notice,
                "Start of election delayed for {} milliseconds (rank #{}, primary rank #{}, offset {}, replica priority {}).",
                state.automatic_failover.deadline_ms().saturating_sub(now_ms),
                state.automatic_failover.rank(),
                state.automatic_failover.failed_primary_rank(),
                offset,
                facts
                    .local
                    .as_ref()
                    .map(|local| local.cluster_replica_priority)
                    .unwrap_or_default(),
            );
        } else if previous_phase == AutomaticFailoverPhase::WaitingRankDelay
            && state.automatic_failover.deadline_ms() < previous_deadline_ms
            && !facts.manual
            && state.automatic_failover.rank() == 0
            && state.automatic_failover.failed_primary_rank() == 0
            && facts
                .local
                .as_ref()
                .is_some_and(|local| local.replication_offset != 0)
            && facts.all_replicas_primary_failed
        {
            crate::server_log!(
                notice,
                "Myself become the best ranked replica, initiate the election immediately."
            );
        }
        if state.automatic_failover.phase() == AutomaticFailoverPhase::WaitingRankDelay
            && state.automatic_failover.deadline_ms() <= now_ms
        {
            actions.extend(state.automatic_failover_on_tick(&facts));
            crate::server_log!(
                notice,
                "Start of election for epoch {} (rank #{})",
                state.automatic_failover.election_epoch(),
                state.automatic_failover.rank(),
            );
        }
        if should_queue_status {
            self.queue_local_replica_status(state);
        }
        self.execute_failover_actions(state, actions, now_ms, None)
    }

    pub(in crate::engine::distributed::cluster::bus) fn execute_failover_actions(
        &mut self,
        state: &mut ClusterState,
        actions: Vec<ClusterAction>,
        _now_ms: i64,
        preferred_recipient_token: Option<usize>,
    ) -> ClusterActionBatch {
        let mut actions = VecDeque::from(actions);
        let mut hints = ClusterActionBatch::default();
        while let Some(action) = actions.pop_front() {
            match action {
                ClusterAction::BroadcastFailoverAuthRequest {
                    election_epoch,
                    force_ack,
                } => {
                    state.current_epoch = election_epoch;
                    crate::server_log!(
                        notice,
                        "Starting a failover election for epoch {}, node config epoch is {}",
                        election_epoch,
                        state.myself.config_epoch,
                    );
                    state.request_config_save();
                    let request =
                        self.build_failover_auth_request(state, election_epoch, force_ack);
                    for connection in self.connections.values_mut() {
                        if connection.connected && !connection.node_name.is_empty() {
                            connection.write_buf.extend_from_slice(&request);
                            state.record_bus_message_sent(
                                crate::engine::distributed::cluster::msg::ClusterMsgType::FailoverAuthRequest,
                            );
                        }
                    }
                }
                ClusterAction::SendFailoverAuthAck {
                    recipient,
                    election_epoch,
                } => {
                    let ack = Self::build_failover_auth_ack(state, election_epoch);
                    // C has one canonical `node->link`. Rust retains both
                    // directions, and either one may be replaced while the
                    // request and ACK cross. Send the same vote over every
                    // identified direction; the requester deduplicates by
                    // voter node ID, so this preserves one-vote semantics.
                    let mut tokens = self
                        .connections
                        .iter()
                        .filter_map(|(token, connection)| {
                            (connection.connected && connection.node_name == recipient.as_str())
                                .then_some(*token)
                        })
                        .collect::<Vec<_>>();
                    if let Some(token) = preferred_recipient_token.filter(|token| {
                        self.connections.get(token).is_some_and(|connection| {
                            connection.connected
                                && (connection.node_name.is_empty()
                                    || connection.node_name == recipient.as_str())
                        })
                    }) {
                        tokens.push(token);
                    }
                    tokens.sort_unstable();
                    tokens.dedup();
                    for token in tokens {
                        self.connections
                            .get_mut(&token)
                            .expect("selected Cluster Bus connection must exist")
                            .write_buf
                            .extend_from_slice(&ack);
                        state.record_bus_message_sent(
                            crate::engine::distributed::cluster::msg::ClusterMsgType::FailoverAuthAck,
                        );
                    }
                }
                ClusterAction::RecordVote { epoch } => {
                    if state.record_vote_epoch(epoch) {
                        state.request_config_save();
                    }
                }
                ClusterAction::PromoteSelf(promotion) => {
                    let primary_failed =
                        state.is_master_objectively_down(promotion.old_primary.as_str());
                    let manual_failover = state.manual_failover.is_active();
                    let takeover = state.manual_failover.is_takeover();
                    // Promotion reaches this path only after the automatic
                    // state machine counted a quorum of FAILOVER_AUTH_ACKs.
                    // A later socket close must not revoke that completed
                    // authorization: under simultaneous shard failures the
                    // remaining voters can reconnect between ACK delivery and
                    // this cron action, otherwise stranding one replica on a
                    // FAIL primary forever.
                    if takeover || primary_failed || manual_failover {
                        hints.actions.push(ClusterAction::PromoteSelf(promotion));
                    } else if !primary_failed {
                        actions.extend(state.automatic_failover_on_topology_change(false, false));
                    }
                }
                ClusterAction::FollowPrimary(target) => {
                    hints.actions.push(ClusterAction::FollowPrimary(target));
                }
                ClusterAction::DetachReplica => hints.actions.push(ClusterAction::DetachReplica),
                ClusterAction::SendManualFailoverStart { primary } => {
                    self.queue_manual_failover_start(state, &primary);
                }
                ClusterAction::BeginManualElection
                | ClusterAction::PauseWrites { .. }
                | ClusterAction::ResumeWrites { .. } => hints.actions.push(action),
                ClusterAction::PersistClusterConfig => state.request_config_save(),
                ClusterAction::BroadcastTopology
                | ClusterAction::OpenMeet { .. }
                | ClusterAction::OpenConnect { .. }
                | ClusterAction::CloseLinks { .. }
                | ClusterAction::ForgetLinks { .. } => hints.actions.push(action),
            }
        }
        hints
    }
}
