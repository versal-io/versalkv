use std::cmp::Ordering;
use std::collections::HashSet;

use crate::engine::distributed::cluster::NodeId;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailureReport {
    pub reporter: NodeId,
    pub reporter_is_primary: bool,
    pub reporter_slots: usize,
    pub reported_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailureFacts {
    pub target_is_pfail: bool,
    pub target_is_fail: bool,
    pub local_is_voting_primary: bool,
    pub voting_primaries: usize,
    pub now_ms: i64,
    pub node_timeout_ms: u64,
    pub reports: Vec<FailureReport>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureDecision {
    Reachable,
    AlreadyFailed,
    AwaitQuorum { votes: usize, quorum: usize },
    MarkFail { votes: usize, quorum: usize },
}

pub fn evaluate_failure(facts: &FailureFacts) -> FailureDecision {
    if facts.target_is_fail {
        return FailureDecision::AlreadyFailed;
    }
    if !facts.target_is_pfail {
        return FailureDecision::Reachable;
    }

    let cutoff = facts
        .now_ms
        .saturating_sub(facts.node_timeout_ms.saturating_mul(2).min(i64::MAX as u64) as i64);
    let mut reporters = HashSet::new();
    for report in &facts.reports {
        if report.reporter_is_primary && report.reporter_slots > 0 && report.reported_at_ms > cutoff
        {
            reporters.insert(report.reporter.clone());
        }
    }
    let votes = reporters.len() + usize::from(facts.local_is_voting_primary);
    let quorum = facts.voting_primaries.max(1) / 2 + 1;
    if votes >= quorum {
        FailureDecision::MarkFail { votes, quorum }
    } else {
        FailureDecision::AwaitQuorum { votes, quorum }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaEligibility {
    pub node_id: NodeId,
    pub no_failover: bool,
    pub cluster_link_connected: bool,
    pub cluster_view_stale: bool,
    pub loading: bool,
    pub priority: i64,
    pub cluster_replica_priority: u32,
    pub replication_offset: i64,
    /// Raw time since the last interaction with the primary. The normal
    /// node-timeout detection allowance is removed before applying the
    /// configured validity window, matching clusterHandleReplicaFailover.
    pub data_age_ms: u64,
    pub node_timeout_ms: u64,
    pub repl_ping_period_ms: u64,
    pub replica_validity_factor: u64,
}

impl ReplicaEligibility {
    pub fn is_eligible(&self) -> bool {
        if self.no_failover || self.cluster_view_stale || self.loading {
            return false;
        }
        if self.replica_validity_factor == 0 {
            return true;
        }
        let adjusted_age = self.data_age_ms.saturating_sub(self.node_timeout_ms);
        let maximum_age = self.repl_ping_period_ms.saturating_add(
            self.node_timeout_ms
                .saturating_mul(self.replica_validity_factor),
        );
        adjusted_age <= maximum_age
    }

    pub fn rank_key(&self) -> Option<ReplicaRankKey> {
        (!self.no_failover).then(|| ReplicaRankKey {
            replication_offset: self.replication_offset,
            cluster_replica_priority: self.cluster_replica_priority,
            node_id: self.node_id.clone(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaRankKey {
    pub replication_offset: i64,
    pub cluster_replica_priority: u32,
    pub node_id: NodeId,
}

impl Ord for ReplicaRankKey {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .replication_offset
            .cmp(&self.replication_offset)
            .then_with(|| {
                self.cluster_replica_priority
                    .cmp(&other.cluster_replica_priority)
            })
            .then_with(|| self.node_id.as_str().cmp(other.node_id.as_str()))
    }
}

impl PartialOrd for ReplicaRankKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub fn rank_replicas(candidates: &[ReplicaEligibility]) -> Vec<NodeId> {
    let mut ranked = candidates
        .iter()
        .filter_map(|candidate| {
            candidate
                .rank_key()
                .map(|key| (key, candidate.node_id.clone()))
        })
        .collect::<Vec<_>>();
    ranked.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    ranked.into_iter().map(|(_, node_id)| node_id).collect()
}

pub fn failover_election_delay_ms(
    node_timeout_ms: u64,
    rank: usize,
    replication_offset: i64,
    is_manual: bool,
) -> u64 {
    if is_manual {
        return 0;
    }
    let base_delay = (node_timeout_ms / 30).min(500);
    let rank_delay = base_delay.saturating_mul(2).saturating_mul(rank as u64);
    let new_replica_delay = if replication_offset == 0 { 500 } else { 0 };
    rank_delay.saturating_add(new_replica_delay)
}
