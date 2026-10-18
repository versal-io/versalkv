use std::collections::HashMap;

use super::{ClusterState, CLUSTER_SLOTS};
use crate::engine::distributed::cluster::node::ClusterNode;
use crate::engine::distributed::cluster::NodeId;

const STRONG_OVERLAP_SLOTS: usize = 1_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DemotionReason {
    ShardEpochSuperseder,
    BitmapFreeStaleClaimant {
        slotted_primaries: usize,
        distinct_shards: usize,
        duplicate_shard: bool,
        extra_claimant: bool,
        lowest_epoch: u64,
    },
    SlotOverlap,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StalePrimaryDecision {
    KeepPrimary,
    FollowPrimary {
        target: NodeId,
        reason: DemotionReason,
        overlap: usize,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct StalePrimaryFacts<'a> {
    migration_allowed: bool,
    local: StalePrimaryNodeFacts<'a>,
    peers: Vec<StalePrimaryNodeFacts<'a>>,
}

#[derive(Clone, Copy, Debug)]
struct StalePrimaryNodeFacts<'a> {
    node_id: &'a str,
    shard_id: &'a str,
    config_epoch: u64,
    slots: &'a [bool],
    numslots: usize,
    is_primary: bool,
    replicaof: &'a str,
}

struct BitmapFreeSignal {
    slotted_primaries: usize,
    distinct_shards: usize,
    duplicate_shard: bool,
    extra_claimant: bool,
    lowest_epoch: u64,
}

impl<'a> StalePrimaryNodeFacts<'a> {
    fn from_node(node_id: &'a str, node: &'a ClusterNode) -> Self {
        Self {
            node_id,
            shard_id: &node.shard_id,
            config_epoch: node.config_epoch,
            slots: &node.slots,
            numslots: node.numslots,
            is_primary: node.is_primary(),
            replicaof: &node.replicaof,
        }
    }

    fn has_slots(&self) -> bool {
        self.numslots > 0
    }
}

impl<'a> StalePrimaryFacts<'a> {
    fn from_state(state: &'a ClusterState) -> Self {
        let local = StalePrimaryNodeFacts::from_node(&state.myself.name, &state.myself);
        let peers = state
            .nodes
            .iter()
            .map(|(node_id, node)| StalePrimaryNodeFacts::from_node(node_id, node))
            .collect();
        Self {
            migration_allowed: state.is_replica_migration_allowed(),
            local,
            peers,
        }
    }
}

impl ClusterState {
    pub fn decide_stale_primary(&self) -> StalePrimaryDecision {
        decide_stale_primary(&StalePrimaryFacts::from_state(self))
    }
}

fn decide_stale_primary(facts: &StalePrimaryFacts<'_>) -> StalePrimaryDecision {
    if !facts.migration_allowed || !facts.local.is_primary || !facts.local.has_slots() {
        return StalePrimaryDecision::KeepPrimary;
    }
    if !is_genuine_stale_primary(facts) {
        return StalePrimaryDecision::KeepPrimary;
    }
    if let Some(target) = shard_epoch_superseder(facts) {
        return follow_decision(
            facts,
            target,
            DemotionReason::ShardEpochSuperseder,
            facts.local.numslots,
        );
    }
    if let Some((target, reason)) = bitmap_free_superseder(facts) {
        return follow_decision(facts, target, reason, facts.local.numslots);
    }
    if let Some((target, overlap)) = overlap_superseder(facts) {
        return follow_decision(facts, target, DemotionReason::SlotOverlap, overlap);
    }
    StalePrimaryDecision::KeepPrimary
}

fn is_genuine_stale_primary(facts: &StalePrimaryFacts<'_>) -> bool {
    let local = &facts.local;
    let shard_recognized = !local.shard_id.is_empty()
        && facts
            .peers
            .iter()
            .any(|peer| peer.shard_id == local.shard_id);
    let mut slotted_epochs = std::iter::once(local.config_epoch).chain(
        facts
            .peers
            .iter()
            .filter(|peer| peer.is_primary && peer.has_slots())
            .map(|peer| peer.config_epoch),
    );
    let min_epoch = slotted_epochs.by_ref().min().unwrap_or(0);
    let slotted_count = 1 + facts
        .peers
        .iter()
        .filter(|peer| peer.is_primary && peer.has_slots())
        .count();
    let prior_epoch_strict = slotted_count >= 2 && local.config_epoch < min_epoch;
    shard_recognized || prior_epoch_strict || has_strong_overlap(facts)
}

fn has_strong_overlap(facts: &StalePrimaryFacts<'_>) -> bool {
    facts.peers.iter().any(|peer| {
        peer.is_primary
            && peer.has_slots()
            && peer.config_epoch > facts.local.config_epoch
            && slot_overlap(&facts.local, peer) >= STRONG_OVERLAP_SLOTS
    })
}

fn shard_epoch_superseder<'facts, 'state>(
    facts: &'facts StalePrimaryFacts<'state>,
) -> Option<&'facts StalePrimaryNodeFacts<'state>> {
    if facts.local.shard_id.is_empty() {
        return None;
    }
    best_epoch_then_id(facts.peers.iter().filter(|peer| {
        peer.is_primary
            && peer.shard_id == facts.local.shard_id
            && peer.config_epoch > facts.local.config_epoch
    }))
}

fn bitmap_free_superseder<'facts, 'state>(
    facts: &'facts StalePrimaryFacts<'state>,
) -> Option<(&'facts StalePrimaryNodeFacts<'state>, DemotionReason)> {
    let signal = bitmap_free_signal(facts)?;
    if !local_is_lowest_epoch_loser(facts, signal.lowest_epoch) {
        return None;
    }
    let target = best_epoch_then_id(
        facts
            .peers
            .iter()
            .filter(|peer| peer.is_primary && peer.has_slots()),
    )?;
    let reason = DemotionReason::BitmapFreeStaleClaimant {
        slotted_primaries: signal.slotted_primaries,
        distinct_shards: signal.distinct_shards,
        duplicate_shard: signal.duplicate_shard,
        extra_claimant: signal.extra_claimant,
        lowest_epoch: signal.lowest_epoch,
    };
    Some((target, reason))
}

fn bitmap_free_signal(facts: &StalePrimaryFacts<'_>) -> Option<BitmapFreeSignal> {
    let mut shard_counts = HashMap::<&str, usize>::new();
    for node in slotted_primaries(facts) {
        if !node.shard_id.is_empty() {
            *shard_counts.entry(node.shard_id).or_default() += 1;
        }
    }
    let distinct_shards = shard_counts.len();
    let duplicate_shard = shard_counts.values().any(|count| *count > 1);
    let slotted_count = slotted_primaries(facts).count();
    let extra_claimant = distinct_shards > 0 && slotted_count > distinct_shards;
    let setup_window =
        distinct_shards == 0 && slotted_primaries(facts).all(|node| node.config_epoch <= 1);
    if (!duplicate_shard && !extra_claimant) || setup_window {
        return None;
    }
    Some(BitmapFreeSignal {
        slotted_primaries: slotted_count,
        distinct_shards,
        duplicate_shard,
        extra_claimant,
        lowest_epoch: slotted_primaries(facts)
            .map(|node| node.config_epoch)
            .min()?,
    })
}

fn local_is_lowest_epoch_loser(facts: &StalePrimaryFacts<'_>, lowest_epoch: u64) -> bool {
    slotted_primaries(facts)
        .filter(|node| node.config_epoch == lowest_epoch)
        .max_by(|left, right| left.node_id.cmp(right.node_id))
        .is_some_and(|loser| loser.node_id == facts.local.node_id)
}

fn slotted_primaries<'facts, 'state>(
    facts: &'facts StalePrimaryFacts<'state>,
) -> impl Iterator<Item = &'facts StalePrimaryNodeFacts<'state>> {
    std::iter::once(&facts.local).chain(
        facts
            .peers
            .iter()
            .filter(|peer| peer.is_primary && peer.has_slots()),
    )
}

fn overlap_superseder<'facts, 'state>(
    facts: &'facts StalePrimaryFacts<'state>,
) -> Option<(&'facts StalePrimaryNodeFacts<'state>, usize)> {
    let local = &facts.local;
    let mut best = None;
    for peer in facts.peers.iter().filter(|peer| {
        peer.is_primary && peer.has_slots() && peer.config_epoch > local.config_epoch
    }) {
        let both_sharded = !local.shard_id.is_empty() && !peer.shard_id.is_empty();
        if both_sharded && local.shard_id != peer.shard_id {
            continue;
        }
        let overlap = slot_overlap(local, peer);
        if overlap == 0 || !better_overlap_candidate(peer, overlap, best) {
            continue;
        }
        best = Some((peer, overlap));
    }
    best
}

fn best_epoch_then_id<'facts, 'state>(
    candidates: impl Iterator<Item = &'facts StalePrimaryNodeFacts<'state>>,
) -> Option<&'facts StalePrimaryNodeFacts<'state>> {
    let mut best: Option<&'facts StalePrimaryNodeFacts<'state>> = None;
    for candidate in candidates {
        if best.is_none_or(|current| {
            candidate.config_epoch > current.config_epoch
                || (candidate.config_epoch == current.config_epoch
                    && candidate.node_id < current.node_id)
        }) {
            best = Some(candidate);
        }
    }
    best
}

fn better_overlap_candidate(
    candidate: &StalePrimaryNodeFacts<'_>,
    overlap: usize,
    current: Option<(&StalePrimaryNodeFacts<'_>, usize)>,
) -> bool {
    current.is_none_or(|(best, best_overlap)| {
        overlap > best_overlap
            || (overlap == best_overlap && candidate.config_epoch > best.config_epoch)
            || (overlap == best_overlap
                && candidate.config_epoch == best.config_epoch
                && candidate.node_id < best.node_id)
    })
}

fn follow_decision(
    facts: &StalePrimaryFacts<'_>,
    target: &StalePrimaryNodeFacts<'_>,
    reason: DemotionReason,
    overlap: usize,
) -> StalePrimaryDecision {
    if target.replicaof == facts.local.node_id {
        StalePrimaryDecision::KeepPrimary
    } else {
        let Ok(target) = NodeId::parse(target.node_id.as_bytes()) else {
            return StalePrimaryDecision::KeepPrimary;
        };
        StalePrimaryDecision::FollowPrimary {
            target,
            reason,
            overlap,
        }
    }
}

fn slot_overlap(left: &StalePrimaryNodeFacts<'_>, right: &StalePrimaryNodeFacts<'_>) -> usize {
    (0..CLUSTER_SLOTS)
        .filter(|slot| left.slots[*slot] && right.slots[*slot])
        .count()
}
