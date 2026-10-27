use std::collections::BTreeMap;

use super::state::{
    ClusterDebugSnapshot, ClusterReplicationView, ClusterRouteDecision, ClusterTopologySnapshot,
    RouteRequest, CLUSTER_OK,
};
use super::ClusterState;

/// Borrowed read boundary for Cluster consumers. Expensive owned views are
/// built only by the specific accessor that needs them.
pub struct ClusterProjection<'a> {
    state: &'a ClusterState,
}

impl ClusterState {
    pub fn projection(&self) -> ClusterProjection<'_> {
        ClusterProjection { state: self }
    }
}

impl ClusterProjection<'_> {
    pub fn management_topology(&self) -> ClusterTopologySnapshot {
        self.management_topology_with_replication(&BTreeMap::new())
    }

    pub fn management_topology_with_replication(
        &self,
        replication: &BTreeMap<String, ClusterReplicationView>,
    ) -> ClusterTopologySnapshot {
        self.management_topology_with_local_replication_offset(
            replication,
            self.state.myself.repl_offset,
        )
    }

    pub fn management_topology_with_local_replication_offset(
        &self,
        replication: &BTreeMap<String, ClusterReplicationView>,
        local_replication_offset: i64,
    ) -> ClusterTopologySnapshot {
        self.state
            .build_topology_snapshot(replication, local_replication_offset)
    }

    pub fn decide_route(&self, request: RouteRequest<'_>) -> ClusterRouteDecision {
        self.state.decide_route(request)
    }

    pub fn is_slot_open(&self, slot: u16) -> bool {
        self.state.migrating_slot_target(slot as usize).is_some()
            || self.state.importing_slot_source(slot as usize).is_some()
    }

    pub fn has_declared_owner(&self, slot: usize) -> bool {
        self.state.declared_node_by_slot(slot).is_some()
    }

    pub fn local_owns_slot(&self, slot: usize) -> bool {
        self.state.myself.slots.get(slot).copied().unwrap_or(false)
    }

    pub fn health_is_ok(&self) -> bool {
        self.state.state == CLUSTER_OK
    }

    pub fn local_slot_range_end(&self, start_slot: usize) -> Option<usize> {
        self.state.local_slot_range_end(start_slot)
    }

    pub fn debug_snapshot(&self) -> ClusterDebugSnapshot {
        let topology = self.management_topology();
        self.state.build_debug_snapshot(&topology)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::engine::distributed::cluster::node::ClusterNode;
    use crate::engine::distributed::cluster::{
        ClusterLinkDirection, ClusterLinkView, ClusterState, LinkEvents, NodeId,
    };

    #[test]
    fn projection_combines_equivalent_topology_and_diagnostics() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let peer_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut peer = ClusterNode::new(
            peer_id.to_string(),
            "192.0.2.1".to_string(),
            6380,
            16_380,
            true,
        );
        peer.flags &= !crate::engine::distributed::cluster::node::CLUSTER_NODE_MYSELF;
        state.nodes.insert(peer_id.to_string(), peer);
        state.add_slot(7).unwrap();
        state.observe_link_open(ClusterLinkView::new(
            NodeId::parse(peer_id.as_bytes()).unwrap(),
            ClusterLinkDirection::To,
            123,
            LinkEvents::READABLE,
        ));
        state.queue_meet(("192.0.2.2".to_string(), 6381, 16_381));

        let previous = state.build_topology_snapshot(&BTreeMap::new(), state.myself.repl_offset);
        let projection = state.projection();
        let topology = projection.management_topology();
        let debug = projection.debug_snapshot();

        assert_eq!(topology, previous);
        assert_eq!(debug.topology_generation, topology.generation);
        assert_eq!(debug.health, topology.health);
        assert_eq!(debug.node_count, topology.nodes.len());
        assert_eq!(debug.assigned_slot_count, 1);
        assert_eq!(debug.links.len(), 1);
        assert_eq!(debug.links[0].node_id.as_str(), peer_id);
        assert_eq!(debug.pending.meets, 1);
    }

    #[test]
    fn repeated_projection_is_stable_without_state_changes() {
        let state = ClusterState::new("127.0.0.1", 6379, 16_379);
        assert_eq!(
            state.projection().debug_snapshot(),
            state.projection().debug_snapshot()
        );
    }

    #[test]
    fn manual_election_is_not_double_counted_as_automatic() {
        use crate::engine::distributed::cluster::state::failover::{
            AutomaticFailoverFacts, FailedPrimaryFacts, ReplicaEligibility,
        };

        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let local = NodeId::parse(state.myself.name.as_bytes()).unwrap();
        let primary = NodeId::parse(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
        state.automatic_failover.on_tick(&AutomaticFailoverFacts {
            now_ms: 1,
            node_timeout_ms: 1_000,
            current_epoch: 1,
            voting_primaries: 1,
            failed_primary_rank: 0,
            all_replicas_primary_failed: true,
            local: Some(ReplicaEligibility {
                node_id: local,
                no_failover: false,
                cluster_link_connected: true,
                cluster_view_stale: false,
                loading: false,
                priority: 100,
                cluster_replica_priority: 0,
                replication_offset: 1,
                data_age_ms: 0,
                node_timeout_ms: 1_000,
                repl_ping_period_ms: 10_000,
                replica_validity_factor: 10,
            }),
            primary: Some(FailedPrimaryFacts {
                node_id: primary.clone(),
                shard_id: primary,
                slots: vec![false; 16_384],
                is_failed: true,
            }),
            siblings: Vec::new(),
            manual: true,
        });

        let active = state.projection().debug_snapshot().active;
        assert!(!active.automatic_failover);
        assert!(active.manual_failover);
    }
}
