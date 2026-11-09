use crate::engine::distributed::cluster::node::{
    ClusterNode, CLUSTER_NODE_FAIL, CLUSTER_NODE_MIGRATE_TO, CLUSTER_NODE_PFAIL,
};
use crate::engine::distributed::cluster::state::{
    select_replica_migration, ClusterReplicaHealth, ClusterState, MigrationFacts,
    MigrationPrimaryFacts, MigrationReplicaFacts, CLUSTER_OK,
};
use crate::engine::distributed::cluster::NodeId;
use crate::runtime::server::worker::ServerWorkerState;
use std::collections::{BTreeSet, HashMap};

impl ServerWorkerState {
    pub(super) fn cluster_migrate_replica_to_orphan(
        &mut self,
        state: &mut ClusterState,
        now_ms: i64,
    ) {
        let enabled = state.is_replica_migration_allowed();
        let cluster_ok = state.state == CLUSTER_OK;
        if !enabled || !cluster_ok || !state.myself.is_replica() {
            state.retain_orphaned_primary(None);
            return;
        }
        let barrier = self.ctx.config().server.cluster_migration_barrier.max(0) as usize;
        let views = &self.ctx.replication().view.cluster_replica_views;
        let replicas = replica_facts(state, views);
        let healthy_counts = healthy_replica_counts(&replicas);
        let orphan_ids = orphan_primary_ids(state, &healthy_counts);
        state.retain_orphaned_primaries(&orphan_ids);
        let primaries = primary_facts(state, &orphan_ids, now_ms);
        let facts = MigrationFacts {
            enabled,
            cluster_ok,
            barrier,
            primaries,
            replicas,
        };
        let Some(decision) = select_replica_migration(&facts) else {
            return;
        };
        let target_id = decision.orphan_primary.to_string();
        crate::server_log!(
            notice,
            "Migrating to orphaned primary {} in shard {}",
            target_id,
            state
                .nodes
                .get(&target_id)
                .map(|node| node.shard_id.as_str())
                .unwrap_or("")
        );
        if self.follow_known_cluster_primary(state, &target_id, decision.mode) {
            state.retain_orphaned_primary(None);
        }
    }
}

fn replica_facts(
    state: &ClusterState,
    _views: &std::collections::BTreeMap<
        String,
        crate::engine::distributed::cluster::state::ClusterReplicationView,
    >,
) -> Vec<MigrationReplicaFacts> {
    std::iter::once(&state.myself)
        .chain(state.nodes.values())
        .filter(|node| node.is_replica())
        .filter_map(|node| {
            let node_id = NodeId::parse(node.name.as_bytes()).ok()?;
            let upstream = NodeId::parse(node.replicaof.as_bytes()).ok();
            let upstream_is_primary = upstream.as_ref().is_some_and(|upstream| {
                state
                    .nodes
                    .get(upstream.as_str())
                    .is_some_and(ClusterNode::is_primary)
            });
            // C `clusterCountNonFailingReplicas` uses the cluster node FAIL
            // flags for every replica, including myself. Runtime link phases
            // are transient during a topology switch and must not disqualify
            // the deterministic lowest-id migration candidate.
            let health = if node.has_flag(CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL) {
                ClusterReplicaHealth::Stale
            } else {
                ClusterReplicaHealth::Online
            };
            Some(MigrationReplicaFacts {
                node_id,
                upstream,
                upstream_is_primary,
                is_local: node.is_myself(),
                has_data: node.repl_offset > 0,
                health,
            })
        })
        .collect()
}

fn healthy_replica_counts(replicas: &[MigrationReplicaFacts]) -> HashMap<&str, usize> {
    let mut counts = HashMap::new();
    for replica in replicas.iter().filter(|replica| {
        replica.upstream_is_primary && replica.health == ClusterReplicaHealth::Online
    }) {
        if let Some(upstream) = &replica.upstream {
            *counts.entry(upstream.as_str()).or_default() += 1;
        }
    }
    counts
}

fn orphan_primary_ids(
    state: &ClusterState,
    healthy_counts: &HashMap<&str, usize>,
) -> BTreeSet<String> {
    state
        .nodes
        .values()
        .filter(|node| {
            node.is_primary()
                && node.numslots > 0
                && node.has_flag(CLUSTER_NODE_MIGRATE_TO)
                && !node.has_flag(CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL)
                && healthy_counts
                    .get(node.name.as_str())
                    .copied()
                    .unwrap_or_default()
                    == 0
        })
        .map(|node| node.name.clone())
        .collect()
}

fn primary_facts(
    state: &mut ClusterState,
    orphan_ids: &BTreeSet<String>,
    now_ms: i64,
) -> Vec<MigrationPrimaryFacts> {
    let node_ids = state.nodes.keys().cloned().collect::<Vec<_>>();
    node_ids
        .into_iter()
        .filter_map(|node_id| {
            let facts = {
                let node = state.nodes.get(&node_id)?;
                (
                    NodeId::parse(node.name.as_bytes()).ok()?,
                    node.numslots > 0,
                    node.has_flag(CLUSTER_NODE_MIGRATE_TO),
                    node.has_flag(CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL),
                )
            };
            let orphaned_duration_ms = if orphan_ids.contains(&node_id) {
                state.orphaned_primary_duration_ms(&node_id, now_ms).max(0) as u64
            } else {
                0
            };
            Some(MigrationPrimaryFacts {
                node_id: facts.0,
                has_slots: facts.1,
                accepts_migration: facts.2,
                is_failed: facts.3,
                orphaned_duration_ms,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::replica_facts;
    use crate::engine::distributed::cluster::node::{
        ClusterNode, CLUSTER_NODE_MYSELF, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
    };
    use crate::engine::distributed::cluster::state::{
        ClusterReplicaHealth, ClusterReplicationView, ClusterState,
    };
    use crate::engine::distributed::cluster::NodeId;
    use crate::engine::distributed::replication::ReplicationOffset;
    use std::collections::BTreeMap;

    const PRIMARY: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const REMOTE_REPLICA: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn offset(value: i64) -> ReplicationOffset {
        ReplicationOffset::try_from(value).expect("valid test offset")
    }

    fn replica_view(health: ClusterReplicaHealth) -> ClusterReplicationView {
        ClusterReplicationView {
            upstream: Some(NodeId::parse(PRIMARY.as_bytes()).unwrap()),
            received_offset: offset(42),
            applied_offset: offset(42),
            acknowledged_offset: offset(42),
            priority: 100,
            health,
        }
    }

    fn state_with_remote_replica() -> ClusterState {
        let mut state = ClusterState::new("127.0.0.1", 6380, 16_380);
        state.myself.flags &= !CLUSTER_NODE_PRIMARY;
        state.myself.flags |= CLUSTER_NODE_REPLICA;
        state.myself.replicaof = PRIMARY.to_string();
        let mut primary = ClusterNode::new(
            PRIMARY.to_string(),
            "127.0.0.1".to_string(),
            6379,
            16_379,
            true,
        );
        primary.flags &= !CLUSTER_NODE_MYSELF;
        let mut remote = ClusterNode::new(
            REMOTE_REPLICA.to_string(),
            "127.0.0.1".to_string(),
            6381,
            16_381,
            false,
        );
        remote.flags &= !(CLUSTER_NODE_MYSELF | CLUSTER_NODE_PRIMARY);
        remote.flags |= CLUSTER_NODE_REPLICA;
        remote.replicaof = PRIMARY.to_string();
        remote.repl_offset = 42;
        state.nodes.insert(PRIMARY.to_string(), primary);
        state.nodes.insert(REMOTE_REPLICA.to_string(), remote);
        state
    }

    #[test]
    fn remote_replica_health_comes_from_cluster_node_flags() {
        let state = state_with_remote_replica();
        let views = BTreeMap::from([(
            REMOTE_REPLICA.to_string(),
            replica_view(ClusterReplicaHealth::Disconnected),
        )]);

        let facts = replica_facts(&state, &views);
        let remote = facts
            .iter()
            .find(|replica| replica.node_id.as_str() == REMOTE_REPLICA)
            .unwrap();

        assert_eq!(remote.health, ClusterReplicaHealth::Online);
    }

    #[test]
    fn local_replica_health_uses_cluster_failure_flags() {
        let state = state_with_remote_replica();
        let views = BTreeMap::from([(
            state.myself.name.clone(),
            replica_view(ClusterReplicaHealth::Disconnected),
        )]);

        let facts = replica_facts(&state, &views);
        let local = facts.iter().find(|replica| replica.is_local).unwrap();

        assert_eq!(local.health, ClusterReplicaHealth::Online);
    }
}
