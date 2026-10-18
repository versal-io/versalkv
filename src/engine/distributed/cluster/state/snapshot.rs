use std::collections::BTreeMap;

use crate::engine::distributed::cluster::node::{
    ClusterNode, CLUSTER_NODE_FAIL, CLUSTER_NODE_HANDSHAKE, CLUSTER_NODE_NOADDR,
    CLUSTER_NODE_PFAIL, CLUSTER_NODE_REPLICA,
};
use crate::engine::distributed::cluster::{ClusterEndpoints, NodeId};

use super::{
    ClusterReplicaHealth, ClusterReplicationView, ClusterState, SlotRange, CLUSTER_OK,
    CLUSTER_SLOTS,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClusterHealth {
    Ok,
    Fail,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClusterNodeRole {
    Primary,
    Replica,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClusterNodeHealth {
    Online,
    Loading,
    PFail,
    Fail,
    NoAddr,
    Handshake,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeView {
    pub id: NodeId,
    pub shard_id: NodeId,
    pub endpoints: ClusterEndpoints,
    pub role: ClusterNodeRole,
    pub health: ClusterNodeHealth,
    pub flags: u16,
    pub replica_of: Option<NodeId>,
    pub config_epoch: u64,
    pub replication_offset: i64,
    pub ping_sent: i64,
    pub pong_received: i64,
    pub outbound_connected: bool,
    pub slots: Vec<bool>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardView {
    pub id: NodeId,
    pub slot_ranges: Vec<SlotRange>,
    pub nodes: Vec<NodeView>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterTopologySnapshot {
    pub generation: u64,
    pub health: ClusterHealth,
    pub slot_owners: Vec<Option<NodeId>>,
    pub nodes: Vec<NodeView>,
    pub shards: Vec<ShardView>,
    pub migrating_slots_to: Vec<(u16, NodeId)>,
    pub importing_slots_from: Vec<(u16, NodeId)>,
}

impl ClusterTopologySnapshot {
    /// Canonical management slots owned by any node in the local shard.
    pub fn local_shard_slots(&self) -> Vec<bool> {
        let local = self
            .nodes
            .iter()
            .find(|node| {
                node.flags & crate::engine::distributed::cluster::node::CLUSTER_NODE_MYSELF != 0
            })
            .expect("topology snapshot always contains myself");
        self.slot_owners
            .iter()
            .map(|owner| {
                owner
                    .as_ref()
                    .and_then(|owner| self.nodes.iter().find(|node| node.id == *owner))
                    .is_some_and(|owner| owner.shard_id == local.shard_id)
            })
            .collect()
    }
}

impl ClusterState {
    pub fn topology_snapshot(&self) -> ClusterTopologySnapshot {
        self.projection().management_topology()
    }

    pub fn topology_snapshot_with_replication(
        &self,
        replication: &BTreeMap<String, ClusterReplicationView>,
    ) -> ClusterTopologySnapshot {
        self.projection()
            .management_topology_with_replication(replication)
    }

    pub fn topology_snapshot_with_local_replication_offset(
        &self,
        replication: &BTreeMap<String, ClusterReplicationView>,
        local_replication_offset: i64,
    ) -> ClusterTopologySnapshot {
        self.projection()
            .management_topology_with_local_replication_offset(
                replication,
                local_replication_offset,
            )
    }

    pub(crate) fn build_topology_snapshot(
        &self,
        replication: &BTreeMap<String, ClusterReplicationView>,
        local_replication_offset: i64,
    ) -> ClusterTopologySnapshot {
        let mut slot_owners = self
            .slot_owners_ref()
            .into_iter()
            .map(|owner| owner.map(|node| validated_node_id(&node.name)))
            .collect::<Vec<_>>();
        for (slot, owner) in slot_owners.iter_mut().enumerate() {
            if !self.require_full_coverage && owner.is_none() {
                // Partial-coverage mode may still expose a historical claimant
                // that is not eligible for strict routing.
                *owner = self
                    .declared_node_by_slot(slot)
                    .map(|node| validated_node_id(&node.name));
            }
        }
        let mut nodes = std::iter::once(&self.myself)
            .chain(self.nodes.values().filter(|node| !node.is_myself()))
            .map(|node| {
                // Match C getNodeReplicationOffset(): only myself reads the
                // live replication runtime. Remote nodes are projected from
                // the last offset they advertised on the cluster bus.
                let local_replication = node
                    .is_myself()
                    .then(|| replication.get(&node.name))
                    .flatten();
                node_view(
                    node,
                    self,
                    local_replication,
                    node.is_myself().then_some(local_replication_offset),
                )
            })
            .collect::<Vec<_>>();
        nodes.sort_unstable_by(|left, right| left.id.as_str().cmp(right.id.as_str()));

        let shard_by_node = nodes
            .iter()
            .map(|node| (node.id.as_str(), node.shard_id.as_str()))
            .collect::<BTreeMap<_, _>>();
        let mut grouped = BTreeMap::<String, Vec<NodeView>>::new();
        for node in nodes.iter().cloned() {
            grouped
                .entry(node.shard_id.as_str().to_string())
                .or_default()
                .push(node);
        }
        let mut shards = grouped
            .into_iter()
            .map(|(shard_id, mut shard_nodes)| {
                shard_nodes.sort_unstable_by(|left, right| {
                    role_rank(left.role)
                        .cmp(&role_rank(right.role))
                        .then_with(|| left.id.as_str().cmp(right.id.as_str()))
                });
                let slot_ranges = slot_ranges_for_shard(&slot_owners, &shard_by_node, &shard_id);
                ShardView {
                    id: validated_node_id(&shard_id),
                    slot_ranges,
                    nodes: shard_nodes,
                }
            })
            .collect::<Vec<_>>();
        shards.sort_unstable_by(|left, right| {
            left.slot_ranges
                .first()
                .map(|range| range.start)
                .unwrap_or(u16::MAX)
                .cmp(
                    &right
                        .slot_ranges
                        .first()
                        .map(|range| range.start)
                        .unwrap_or(u16::MAX),
                )
                .then_with(|| left.id.as_str().cmp(right.id.as_str()))
        });

        ClusterTopologySnapshot {
            generation: self.topology_generation(),
            health: if self.state == CLUSTER_OK {
                ClusterHealth::Ok
            } else {
                ClusterHealth::Fail
            },
            slot_owners,
            nodes,
            shards,
            migrating_slots_to: topology_transitions(&self.slot_ownership.migrating_to),
            importing_slots_from: topology_transitions(&self.slot_ownership.importing_from),
        }
    }
}

fn node_view(
    node: &ClusterNode,
    state: &ClusterState,
    replication: Option<&ClusterReplicationView>,
    local_replication_offset: Option<i64>,
) -> NodeView {
    let shard_id = if !node.shard_id.is_empty() {
        node.shard_id.as_str()
    } else if node.is_replica() {
        state
            .nodes
            .get(&node.replicaof)
            .map(|primary| primary.shard_id.as_str())
            .filter(|id| !id.is_empty())
            .or_else(|| (!node.replicaof.is_empty()).then_some(node.replicaof.as_str()))
            .unwrap_or(node.name.as_str())
    } else {
        node.name.as_str()
    };
    NodeView {
        id: validated_node_id(&node.name),
        shard_id: validated_node_id(shard_id),
        endpoints: node.endpoints.clone(),
        role: if node.is_replica() {
            ClusterNodeRole::Replica
        } else {
            ClusterNodeRole::Primary
        },
        health: node_health(node, replication),
        flags: node.flags,
        replica_of: (!node.replicaof.is_empty()).then(|| validated_node_id(&node.replicaof)),
        config_epoch: node.config_epoch,
        replication_offset: local_replication_offset.unwrap_or_else(|| {
            replication
                .map(|view| view.applied_offset.get())
                .unwrap_or(node.repl_offset)
        }),
        ping_sent: node.ping_sent,
        pong_received: node.pong_received,
        outbound_connected: node.is_myself()
            || state.has_connected_link(
                &node.name,
                crate::engine::distributed::cluster::ClusterLinkDirection::To,
            ),
        slots: node.slots.clone(),
    }
}

fn node_health(
    node: &ClusterNode,
    replication: Option<&ClusterReplicationView>,
) -> ClusterNodeHealth {
    if node.has_flag(CLUSTER_NODE_HANDSHAKE) {
        ClusterNodeHealth::Handshake
    } else if node.has_flag(CLUSTER_NODE_NOADDR) {
        ClusterNodeHealth::NoAddr
    } else if node.has_flag(CLUSTER_NODE_FAIL) {
        ClusterNodeHealth::Fail
    } else if node.has_flag(CLUSTER_NODE_PFAIL) {
        ClusterNodeHealth::PFail
    } else if node.has_flag(CLUSTER_NODE_REPLICA) {
        match replication.map(|view| view.health) {
            Some(ClusterReplicaHealth::Loading) => ClusterNodeHealth::Loading,
            Some(ClusterReplicaHealth::Online | ClusterReplicaHealth::Stale) => {
                ClusterNodeHealth::Online
            }
            Some(ClusterReplicaHealth::Disconnected)
                if replication.is_some_and(|view| {
                    view.applied_offset
                        > crate::engine::distributed::replication::ReplicationOffset::try_from(0)
                            .expect("zero is a valid replication offset")
                }) =>
            {
                ClusterNodeHealth::Online
            }
            Some(ClusterReplicaHealth::Disconnected) => ClusterNodeHealth::Loading,
            None if node.repl_offset == 0 => ClusterNodeHealth::Loading,
            None => ClusterNodeHealth::Online,
        }
    } else {
        ClusterNodeHealth::Online
    }
}

fn role_rank(role: ClusterNodeRole) -> u8 {
    match role {
        ClusterNodeRole::Primary => 0,
        ClusterNodeRole::Replica => 1,
    }
}

fn slot_ranges_for_shard(
    owners: &[Option<NodeId>],
    shard_by_node: &BTreeMap<&str, &str>,
    shard_id: &str,
) -> Vec<SlotRange> {
    let belongs = |slot: usize| {
        owners[slot]
            .as_ref()
            .and_then(|owner| shard_by_node.get(owner.as_str()))
            .is_some_and(|owner_shard| *owner_shard == shard_id)
    };
    let mut ranges = Vec::new();
    let mut slot = 0usize;
    while slot < CLUSTER_SLOTS {
        if !belongs(slot) {
            slot += 1;
            continue;
        }
        let start = slot;
        while slot + 1 < CLUSTER_SLOTS && belongs(slot + 1) {
            slot += 1;
        }
        ranges.push(
            SlotRange::new(start as u16, slot as u16)
                .expect("snapshot slots are bounded by CLUSTER_SLOTS"),
        );
        slot += 1;
    }
    ranges
}

fn validated_node_id(value: &str) -> NodeId {
    NodeId::parse(value.as_bytes()).expect("cluster node IDs are validated at ingress")
}

fn topology_transitions(
    transitions: &std::collections::HashMap<usize, String>,
) -> Vec<(u16, NodeId)> {
    let mut transitions = transitions
        .iter()
        .filter_map(|(&slot, node_id)| {
            u16::try_from(slot)
                .ok()
                .map(|slot| (slot, validated_node_id(node_id)))
        })
        .collect::<Vec<_>>();
    transitions.sort_unstable_by_key(|(slot, _)| *slot);
    transitions
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::engine::distributed::cluster::node::{
        ClusterNode, CLUSTER_NODE_MYSELF, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
    };
    use crate::engine::distributed::cluster::state::{
        ClusterReplicaHealth, ClusterReplicationView,
    };

    const REPLICA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn offset(value: i64) -> crate::engine::distributed::replication::ReplicationOffset {
        crate::engine::distributed::replication::ReplicationOffset::try_from(value)
            .expect("valid test offset")
    }

    fn state_with_replica() -> ClusterState {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let mut replica = ClusterNode::new(
            REPLICA.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16380,
            false,
        );
        replica.flags &= !(CLUSTER_NODE_MYSELF | CLUSTER_NODE_PRIMARY);
        replica.flags |= CLUSTER_NODE_REPLICA;
        replica.replicaof = state.myself.name.clone();
        replica.shard_id = state.myself.shard_id.clone();
        state.nodes.insert(replica.name.clone(), replica);
        state
    }

    fn replica_view(health: ClusterReplicaHealth, offset: i64) -> ClusterReplicationView {
        ClusterReplicationView {
            upstream: None,
            received_offset: self::offset(offset),
            applied_offset: self::offset(offset),
            acknowledged_offset: self::offset(offset),
            priority: 100,
            health,
        }
    }

    #[test]
    fn remote_runtime_view_does_not_override_zero_bus_offset() {
        let state = state_with_replica();
        let views = BTreeMap::from([(
            REPLICA.to_string(),
            replica_view(ClusterReplicaHealth::Online, 42),
        )]);

        let snapshot = state.topology_snapshot_with_replication(&views);
        let replica = snapshot
            .nodes
            .iter()
            .find(|node| node.id.as_str() == REPLICA)
            .unwrap();

        assert_eq!(replica.health, ClusterNodeHealth::Loading);
        assert_eq!(replica.replication_offset, 0);
    }

    #[test]
    fn snapshot_retains_a_withdrawn_binding_until_a_new_owner_arrives() {
        const OWNER: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        let mut owner = ClusterNode::new(
            OWNER.to_string(),
            "127.0.0.1".to_string(),
            6380,
            16_380,
            true,
        );
        owner.flags &= !CLUSTER_NODE_MYSELF;
        owner.slots[7] = true;
        owner.numslots = 1;
        state.nodes.insert(owner.name.clone(), owner);
        state.slot_ownership.withdrawn_by[7] = Some(OWNER.to_string());

        let snapshot = state.topology_snapshot();

        assert_eq!(
            snapshot.slot_owners[7].as_ref().map(NodeId::as_str),
            Some(OWNER)
        );
        assert_eq!(
            state.slot_owner_ref(7).map(|node| node.name.as_str()),
            Some(OWNER)
        );
    }

    #[test]
    fn snapshot_keeps_an_explicitly_deleted_slot_unassigned() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16_379);
        state.add_slot(7).unwrap();
        state.del_slot(7).unwrap();
        state.slot_ownership.withdrawn_by[7] = Some(state.myself.name.clone());

        let snapshot = state.topology_snapshot();

        assert!(snapshot.slot_owners[7].is_none());
    }

    #[test]
    fn remote_loading_view_does_not_override_nonzero_bus_offset() {
        let mut state = state_with_replica();
        state.nodes.get_mut(REPLICA).unwrap().repl_offset = 42;
        let views = BTreeMap::from([(
            REPLICA.to_string(),
            replica_view(ClusterReplicaHealth::Loading, 0),
        )]);

        let snapshot = state.topology_snapshot_with_replication(&views);
        let replica = snapshot
            .nodes
            .iter()
            .find(|node| node.id.as_str() == REPLICA)
            .unwrap();

        assert_eq!(replica.health, ClusterNodeHealth::Online);
        assert_eq!(replica.replication_offset, 42);
    }

    #[test]
    fn topology_snapshot_preserves_cluster_fail_over_replication_health() {
        let mut state = state_with_replica();
        state.nodes.get_mut(REPLICA).unwrap().flags |= CLUSTER_NODE_FAIL;
        let views = BTreeMap::from([(
            REPLICA.to_string(),
            replica_view(ClusterReplicaHealth::Loading, 42),
        )]);

        let snapshot = state.topology_snapshot_with_replication(&views);
        let replica = snapshot
            .nodes
            .iter()
            .find(|node| node.id.as_str() == REPLICA)
            .unwrap();

        assert_eq!(replica.health, ClusterNodeHealth::Fail);
        assert_eq!(replica.replication_offset, 0);
    }

    #[test]
    fn disconnected_replica_retains_nonzero_bus_offset() {
        let mut state = state_with_replica();
        state.nodes.get_mut(REPLICA).unwrap().repl_offset = 42;
        let views = BTreeMap::from([(
            REPLICA.to_string(),
            replica_view(ClusterReplicaHealth::Disconnected, 0),
        )]);

        let snapshot = state.topology_snapshot_with_replication(&views);
        let replica = snapshot
            .nodes
            .iter()
            .find(|node| node.id.as_str() == REPLICA)
            .unwrap();

        assert_eq!(replica.health, ClusterNodeHealth::Online);
        assert_eq!(replica.replication_offset, 42);
    }
}
