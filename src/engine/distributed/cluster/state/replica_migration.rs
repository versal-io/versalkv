use std::collections::BTreeMap;

use crate::engine::distributed::cluster::action::ReplicationSwitchMode;
use crate::engine::distributed::cluster::state::ClusterReplicaHealth;
use crate::engine::distributed::cluster::NodeId;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationPrimaryFacts {
    pub node_id: NodeId,
    pub has_slots: bool,
    pub accepts_migration: bool,
    pub is_failed: bool,
    pub orphaned_duration_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationReplicaFacts {
    pub node_id: NodeId,
    pub upstream: Option<NodeId>,
    pub upstream_is_primary: bool,
    pub is_local: bool,
    pub has_data: bool,
    pub health: ClusterReplicaHealth,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationFacts {
    pub enabled: bool,
    pub cluster_ok: bool,
    pub barrier: usize,
    pub primaries: Vec<MigrationPrimaryFacts>,
    pub replicas: Vec<MigrationReplicaFacts>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaMigrationDecision {
    pub source_primary: NodeId,
    pub candidate_replica: NodeId,
    pub orphan_primary: NodeId,
    pub mode: ReplicationSwitchMode,
}

pub fn select_replica_migration(facts: &MigrationFacts) -> Option<ReplicaMigrationDecision> {
    if !facts.enabled || !facts.cluster_ok {
        return None;
    }

    let healthy = facts
        .replicas
        .iter()
        .filter(|replica| {
            replica.upstream_is_primary
                && replica.upstream.is_some()
                && replica.health == ClusterReplicaHealth::Online
        })
        .collect::<Vec<_>>();
    let mut counts = BTreeMap::<&str, usize>::new();
    for replica in &healthy {
        let upstream = replica
            .upstream
            .as_ref()
            .expect("healthy replicas have a primary");
        *counts.entry(upstream.as_str()).or_default() += 1;
    }
    let max_replicas = counts.values().copied().max()?;
    if max_replicas <= facts.barrier {
        return None;
    }

    let candidate = healthy
        .into_iter()
        .filter(|replica| {
            replica.upstream.as_ref().is_some_and(|upstream| {
                counts.get(upstream.as_str()).copied() == Some(max_replicas)
            })
        })
        .min_by(|left, right| left.node_id.as_str().cmp(right.node_id.as_str()))?;
    if !candidate.is_local {
        return None;
    }

    let orphan = facts
        .primaries
        .iter()
        .filter(|primary| {
            primary.has_slots
                && primary.accepts_migration
                && !primary.is_failed
                && primary.orphaned_duration_ms > 5_000
                && counts
                    .get(primary.node_id.as_str())
                    .copied()
                    .unwrap_or_default()
                    == 0
        })
        .min_by(|left, right| left.node_id.as_str().cmp(right.node_id.as_str()))?;

    Some(ReplicaMigrationDecision {
        source_primary: candidate.upstream.clone()?,
        candidate_replica: candidate.node_id.clone(),
        orphan_primary: orphan.node_id.clone(),
        mode: ReplicationSwitchMode::ForceFull,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        select_replica_migration, MigrationFacts, MigrationPrimaryFacts, MigrationReplicaFacts,
        ReplicaMigrationDecision,
    };
    use crate::engine::distributed::cluster::action::ReplicationSwitchMode;
    use crate::engine::distributed::cluster::state::ClusterReplicaHealth;
    use crate::engine::distributed::cluster::NodeId;

    fn node(byte: u8) -> NodeId {
        NodeId::parse(&[byte; 40]).unwrap()
    }

    fn primary(id: u8, orphaned_duration_ms: u64) -> MigrationPrimaryFacts {
        MigrationPrimaryFacts {
            node_id: node(id),
            has_slots: true,
            accepts_migration: true,
            is_failed: false,
            orphaned_duration_ms,
        }
    }

    fn replica(id: u8, upstream: u8, is_local: bool) -> MigrationReplicaFacts {
        MigrationReplicaFacts {
            node_id: node(id),
            upstream: Some(node(upstream)),
            upstream_is_primary: true,
            is_local,
            has_data: true,
            health: ClusterReplicaHealth::Online,
        }
    }

    fn eligible_facts() -> MigrationFacts {
        MigrationFacts {
            enabled: true,
            cluster_ok: true,
            barrier: 1,
            primaries: vec![primary(b's', 0), primary(b'o', 5_001)],
            replicas: vec![replica(b'a', b's', true), replica(b'b', b's', false)],
        }
    }

    #[test]
    fn selects_local_lowest_id_replica_from_most_replicated_source() {
        assert_eq!(
            select_replica_migration(&eligible_facts()),
            Some(ReplicaMigrationDecision {
                source_primary: node(b's'),
                candidate_replica: node(b'a'),
                orphan_primary: node(b'o'),
                mode: ReplicationSwitchMode::ForceFull,
            })
        );
    }

    #[test]
    fn rejects_disabled_unhealthy_young_or_barrier_blocked_migrations() {
        let mut cases = Vec::new();
        cases.push({
            let mut facts = eligible_facts();
            facts.enabled = false;
            facts
        });
        cases.push({
            let mut facts = eligible_facts();
            facts.cluster_ok = false;
            facts
        });
        cases.push({
            let mut facts = eligible_facts();
            facts.barrier = 2;
            facts
        });
        cases.push({
            let mut facts = eligible_facts();
            facts.primaries[1].orphaned_duration_ms = 5_000;
            facts
        });
        for health in [
            ClusterReplicaHealth::Loading,
            ClusterReplicaHealth::Disconnected,
            ClusterReplicaHealth::Stale,
        ] {
            let mut facts = eligible_facts();
            facts.replicas[0].health = health;
            cases.push(facts);
        }

        for facts in cases {
            assert_eq!(select_replica_migration(&facts), None, "{facts:?}");
        }
    }

    #[test]
    fn full_sync_allows_nonempty_candidate_but_rejects_sub_replica() {
        let mut nonempty = eligible_facts();
        nonempty.replicas[0].has_data = true;
        assert!(select_replica_migration(&nonempty).is_some());

        let mut sub_replica = eligible_facts();
        sub_replica.replicas[0].upstream_is_primary = false;
        assert_eq!(select_replica_migration(&sub_replica), None);
    }

    #[test]
    fn selection_is_deterministic_across_source_candidate_and_orphan_ties() {
        let mut facts = eligible_facts();
        facts.primaries.push(primary(b'n', 5_001));
        facts.primaries.push(primary(b't', 0));
        facts
            .replicas
            .extend([replica(b'c', b't', false), replica(b'd', b't', false)]);
        facts.primaries.swap(0, 2);
        facts.replicas.reverse();

        let decision = select_replica_migration(&facts).unwrap();

        assert_eq!(decision.source_primary, node(b's'));
        assert_eq!(decision.candidate_replica, node(b'a'));
        assert_eq!(decision.orphan_primary, node(b'n'));
    }

    #[test]
    fn remote_candidate_wins_tie_without_emitting_a_local_decision() {
        let mut facts = eligible_facts();
        facts.replicas[0].node_id = node(b'z');
        facts.replicas[1].node_id = node(b'a');

        assert_eq!(select_replica_migration(&facts), None);
    }
}
