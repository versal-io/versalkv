use std::collections::HashSet;

use crate::engine::distributed::cluster::action::{ClusterAction, Promotion};
use crate::engine::distributed::cluster::NodeId;

use super::{failover_election_delay_ms, rank_replicas, ReplicaEligibility};

#[derive(Clone, Debug)]
pub struct LocalReplicaFailoverStatus {
    pub loading: bool,
    pub priority: i64,
    pub data_age_ms: u64,
    pub repl_ping_period_ms: u64,
    pub replica_validity_factor: u64,
}

impl Default for LocalReplicaFailoverStatus {
    fn default() -> Self {
        Self {
            loading: false,
            priority: 100,
            data_age_ms: 0,
            repl_ping_period_ms: 10_000,
            replica_validity_factor: 10,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailedPrimaryFacts {
    pub node_id: NodeId,
    pub shard_id: NodeId,
    pub slots: Vec<bool>,
    pub is_failed: bool,
}

#[derive(Clone, Debug)]
pub struct AutomaticFailoverFacts {
    pub now_ms: i64,
    pub node_timeout_ms: u64,
    pub current_epoch: u64,
    pub voting_primaries: usize,
    pub failed_primary_rank: usize,
    pub all_replicas_primary_failed: bool,
    pub local: Option<ReplicaEligibility>,
    pub primary: Option<FailedPrimaryFacts>,
    pub siblings: Vec<ReplicaEligibility>,
    pub manual: bool,
}

#[derive(Clone, Debug)]
pub struct VoteRequestFacts {
    pub requester: NodeId,
    pub local_is_voting_primary: bool,
    pub safe_to_vote: bool,
    pub requester_is_replica: bool,
    pub requester_primary_known: bool,
    pub requester_primary_failed: bool,
    pub force_ack: bool,
    pub request_epoch: u64,
    pub current_epoch: u64,
    pub request_config_epoch: u64,
    pub maximum_claimed_slot_epoch: u64,
    pub persisted_last_vote_epoch: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AutomaticFailoverPhase {
    #[default]
    Idle,
    WaitingRankDelay,
    RequestingVotes,
    Promoting,
    Broadcasting,
}

#[derive(Clone, Debug, Default)]
pub struct AutomaticFailover {
    failed_primary: Option<FailedPrimaryFacts>,
    election_epoch: u64,
    rank: usize,
    deadline_ms: i64,
    started_at_ms: i64,
    quorum: usize,
    failed_primary_rank: usize,
    all_replicas_primary_failed: bool,
    voters: HashSet<NodeId>,
    last_vote_epoch: u64,
    deferred_for_competing_epoch: u64,
    phase: AutomaticFailoverPhase,
    manual: bool,
}

impl AutomaticFailover {
    fn election_delay_ms(
        node_timeout_ms: u64,
        rank: usize,
        failed_primary_rank: usize,
        replication_offset: i64,
        manual: bool,
        all_replicas_primary_failed: bool,
    ) -> u64 {
        if manual {
            return 0;
        }
        let base_delay = (node_timeout_ms / 30).min(500);
        let mut delay =
            failover_election_delay_ms(node_timeout_ms, rank, replication_offset, false)
                .saturating_add(base_delay.saturating_mul(failed_primary_rank as u64));
        let may_start_immediately = rank == 0
            && failed_primary_rank == 0
            && replication_offset != 0
            && all_replicas_primary_failed;
        if !may_start_immediately && delay == 0 {
            delay = base_delay;
        }
        delay
    }

    pub fn phase(&self) -> AutomaticFailoverPhase {
        self.phase
    }

    #[cfg(test)]
    pub fn failed_primary(&self) -> Option<&NodeId> {
        self.failed_primary.as_ref().map(|primary| &primary.node_id)
    }

    pub fn election_epoch(&self) -> u64 {
        self.election_epoch
    }

    pub fn is_manual(&self) -> bool {
        self.manual
    }

    pub fn rank(&self) -> usize {
        self.rank
    }

    pub fn failed_primary_rank(&self) -> usize {
        self.failed_primary_rank
    }

    pub fn voter_count(&self) -> usize {
        self.voters.len()
    }

    pub fn deadline_ms(&self) -> i64 {
        self.deadline_ms
    }

    pub fn on_tick(&mut self, facts: &AutomaticFailoverFacts) -> Vec<ClusterAction> {
        if self.phase == AutomaticFailoverPhase::Broadcasting {
            self.reset();
            return Vec::new();
        }
        let Some(local) = facts
            .local
            .as_ref()
            .filter(|local| facts.manual || local.is_eligible())
        else {
            self.reset();
            return Vec::new();
        };
        let Some(primary) = facts
            .primary
            .as_ref()
            .filter(|primary| facts.manual || primary.is_failed)
        else {
            if !self.manual {
                self.reset();
            }
            return Vec::new();
        };
        if self
            .failed_primary
            .as_ref()
            .is_some_and(|known| known.node_id != primary.node_id)
        {
            self.reset();
        }
        if self.phase == AutomaticFailoverPhase::Promoting {
            return Vec::new();
        }

        match self.phase {
            AutomaticFailoverPhase::Idle => {
                let ranked = rank_replicas(&facts.siblings);
                self.rank = if facts.manual {
                    0
                } else {
                    ranked
                        .iter()
                        .position(|node_id| node_id == &local.node_id)
                        .unwrap_or(ranked.len())
                };
                let delay = Self::election_delay_ms(
                    facts.node_timeout_ms,
                    self.rank,
                    facts.failed_primary_rank,
                    local.replication_offset,
                    facts.manual,
                    facts.all_replicas_primary_failed,
                );
                self.deadline_ms = facts
                    .now_ms
                    .saturating_add(delay.min(i64::MAX as u64) as i64);
                self.started_at_ms = facts.now_ms;
                self.quorum = facts.voting_primaries.max(1) / 2 + 1;
                self.failed_primary_rank = facts.failed_primary_rank;
                self.all_replicas_primary_failed = facts.all_replicas_primary_failed;
                self.failed_primary = Some(primary.clone());
                self.manual = facts.manual;
                self.phase = AutomaticFailoverPhase::WaitingRankDelay;
                Vec::new()
            }
            AutomaticFailoverPhase::WaitingRankDelay => {
                if !facts.manual {
                    let ranked = rank_replicas(&facts.siblings);
                    let new_rank = ranked
                        .iter()
                        .position(|node_id| node_id == &local.node_id)
                        .unwrap_or(ranked.len());
                    let rank_changed = new_rank != self.rank;
                    let primary_rank_changed =
                        facts.failed_primary_rank != self.failed_primary_rank;
                    let failure_confirmation_changed =
                        facts.all_replicas_primary_failed != self.all_replicas_primary_failed;
                    if rank_changed || primary_rank_changed || failure_confirmation_changed {
                        let old_delay = Self::election_delay_ms(
                            facts.node_timeout_ms,
                            self.rank,
                            self.failed_primary_rank,
                            local.replication_offset,
                            false,
                            self.all_replicas_primary_failed,
                        );
                        let new_delay = Self::election_delay_ms(
                            facts.node_timeout_ms,
                            new_rank,
                            facts.failed_primary_rank,
                            local.replication_offset,
                            false,
                            facts.all_replicas_primary_failed,
                        );
                        self.deadline_ms = self
                            .deadline_ms
                            .saturating_add(new_delay as i64 - old_delay as i64);
                        self.rank = new_rank;
                        self.failed_primary_rank = facts.failed_primary_rank;
                        self.all_replicas_primary_failed = facts.all_replicas_primary_failed;
                    }
                }
                if facts.now_ms < self.deadline_ms {
                    return Vec::new();
                }
                let next_epoch = facts.current_epoch.checked_add(1);
                let Some(next_epoch) = next_epoch.filter(|epoch| *epoch > facts.current_epoch)
                else {
                    self.reset();
                    return Vec::new();
                };
                self.election_epoch = next_epoch;
                self.started_at_ms = facts.now_ms;
                self.voters.clear();
                self.phase = AutomaticFailoverPhase::RequestingVotes;
                vec![ClusterAction::BroadcastFailoverAuthRequest {
                    election_epoch: self.election_epoch,
                    force_ack: self.manual
                        || (self.rank == 0
                            && self.failed_primary_rank == 0
                            && local.replication_offset != 0
                            && self.all_replicas_primary_failed),
                }]
            }
            AutomaticFailoverPhase::RequestingVotes => {
                let timeout_ms = facts.node_timeout_ms.saturating_mul(2).max(2_000);
                if facts.now_ms.saturating_sub(self.started_at_ms)
                    > timeout_ms.min(i64::MAX as u64) as i64
                {
                    // C waits for the full retry window and then schedules a
                    // fresh rank-delayed attempt. Reapply that delay here;
                    // otherwise a stale/zero-offset replica can start the next
                    // epoch before the winning replica's topology broadcast.
                    let ranked = rank_replicas(&facts.siblings);
                    self.rank = ranked
                        .iter()
                        .position(|node_id| node_id == &local.node_id)
                        .unwrap_or(ranked.len());
                    self.failed_primary_rank = facts.failed_primary_rank;
                    self.all_replicas_primary_failed = facts.all_replicas_primary_failed;
                    let retry_delay = Self::election_delay_ms(
                        facts.node_timeout_ms,
                        self.rank,
                        self.failed_primary_rank,
                        local.replication_offset,
                        facts.manual,
                        self.all_replicas_primary_failed,
                    );
                    self.election_epoch = 0;
                    self.voters.clear();
                    self.started_at_ms = facts.now_ms;
                    self.deadline_ms = facts
                        .now_ms
                        .saturating_add(timeout_ms.saturating_sub(1).min(i64::MAX as u64) as i64)
                        .saturating_add(retry_delay.min(i64::MAX as u64) as i64);
                    self.phase = AutomaticFailoverPhase::WaitingRankDelay;
                }
                Vec::new()
            }
            _ => Vec::new(),
        }
    }

    pub fn on_vote_ack(
        &mut self,
        voter: NodeId,
        election_epoch: u64,
        voter_is_voting_primary: bool,
    ) -> Vec<ClusterAction> {
        if self.phase != AutomaticFailoverPhase::RequestingVotes
            || election_epoch < self.election_epoch
            || !voter_is_voting_primary
            || !self.voters.insert(voter)
            || self.voters.len() < self.quorum
        {
            return Vec::new();
        }
        let Some(primary) = self.failed_primary.as_ref() else {
            return Vec::new();
        };
        self.phase = AutomaticFailoverPhase::Promoting;
        vec![ClusterAction::PromoteSelf(Promotion {
            old_primary: primary.node_id.clone(),
            shard_id: primary.shard_id.clone(),
            slots: primary.slots.clone(),
            config_epoch: self.election_epoch,
        })]
    }

    pub fn on_vote_request(&mut self, facts: &VoteRequestFacts) -> Vec<ClusterAction> {
        self.last_vote_epoch = self.last_vote_epoch.max(facts.persisted_last_vote_epoch);
        let may_vote = facts.local_is_voting_primary
            && facts.safe_to_vote
            && facts.requester_is_replica
            && facts.requester_primary_known
            && (facts.requester_primary_failed || facts.force_ack)
            && facts.request_epoch >= facts.current_epoch
            && facts.request_epoch > self.last_vote_epoch
            && facts.maximum_claimed_slot_epoch <= facts.request_config_epoch;
        if !may_vote {
            return Vec::new();
        }
        self.last_vote_epoch = facts.request_epoch;
        vec![
            ClusterAction::RecordVote {
                epoch: facts.request_epoch,
            },
            ClusterAction::SendFailoverAuthAck {
                recipient: facts.requester.clone(),
                election_epoch: facts.request_epoch,
            },
        ]
    }

    pub fn defer_for_better_replica_election(
        &mut self,
        request_epoch: u64,
        now_ms: i64,
        node_timeout_ms: u64,
    ) -> bool {
        if self.manual
            || self.phase != AutomaticFailoverPhase::WaitingRankDelay
            || request_epoch <= self.deferred_for_competing_epoch
        {
            return false;
        }
        // A known-better sibling gets the same bounded vote-collection window
        // as C clusterHandleReplicaFailover. Starting a newer local epoch
        // inside that window can supersede the winner before its topology
        // broadcast reaches this replica.
        let auth_timeout_ms = node_timeout_ms.saturating_mul(2).max(2_000);
        let holdoff_deadline = now_ms.saturating_add(auth_timeout_ms.min(i64::MAX as u64) as i64);
        self.started_at_ms = now_ms;
        self.deadline_ms = self.deadline_ms.max(holdoff_deadline);
        self.deferred_for_competing_epoch = request_epoch;
        true
    }

    /// Cancel an election whose epoch has already been claimed by a primary.
    /// This mirrors C Valkey's direct-sender config-epoch check in
    /// `clusterProcessPacket`: once the request was broadcast, an equal or
    /// newer primary claim proves this election can no longer win.
    pub fn reset_on_competing_primary_claim(&mut self, claimed_epoch: u64) -> bool {
        let election_was_sent = matches!(
            self.phase,
            AutomaticFailoverPhase::RequestingVotes | AutomaticFailoverPhase::Promoting
        );
        if !election_was_sent || self.election_epoch == 0 || claimed_epoch < self.election_epoch {
            return false;
        }
        self.reset();
        true
    }

    pub fn on_topology_change(
        &mut self,
        primary_failed: bool,
        promotion_committed: bool,
    ) -> Vec<ClusterAction> {
        if self.phase == AutomaticFailoverPhase::Promoting && promotion_committed {
            self.phase = AutomaticFailoverPhase::Broadcasting;
            return vec![
                ClusterAction::PersistClusterConfig,
                ClusterAction::BroadcastTopology,
            ];
        }
        if !primary_failed {
            self.reset();
        }
        Vec::new()
    }

    pub fn reset(&mut self) {
        *self = Self {
            last_vote_epoch: self.last_vote_epoch,
            ..Self::default()
        };
    }
}
