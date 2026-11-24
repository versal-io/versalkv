//! Reusable management structure with fresh node status on every query.

use std::collections::BTreeMap;
use std::sync::Arc;

use super::snapshot::node_view;
use super::types::SlotOwner;
use super::{ClusterReplicationView, ClusterState, NodeView, SlotRange};
use crate::engine::distributed::cluster::node::{
    ClusterNode, CLUSTER_NODE_FAIL, CLUSTER_NODE_HANDSHAKE, CLUSTER_NODE_MYSELF,
    CLUSTER_NODE_NOADDR, CLUSTER_NODE_PRIMARY, CLUSTER_NODE_REPLICA,
};
use crate::engine::distributed::cluster::slot_storage::{SharedSlots, SlotBitmap};
use crate::engine::distributed::cluster::NodeId;

const STRUCTURAL_FLAGS: u16 = CLUSTER_NODE_FAIL
    | CLUSTER_NODE_HANDSHAKE
    | CLUSTER_NODE_MYSELF
    | CLUSTER_NODE_NOADDR
    | CLUSTER_NODE_PRIMARY
    | CLUSTER_NODE_REPLICA;

struct StructuralNode {
    name: String,
    shard_id: String,
    replica_of: String,
    flags: u16,
    config_epoch: u64,
    slots: SlotBitmap,
}

impl StructuralNode {
    fn matches(&self, node: &ClusterNode) -> bool {
        self.name == node.name
            && self.shard_id == node.shard_id
            && self.replica_of == node.replicaof
            && self.flags == node.flags & STRUCTURAL_FLAGS
            && self.config_epoch == node.config_epoch
            && self.slots == node.slots
    }
}

pub(super) struct ManagementCache {
    nodes: Vec<StructuralNode>,
    owners: SharedSlots<SlotOwner>,
    delete_epochs: SharedSlots<u64>,
    withdrawn_by: SharedSlots<Option<String>>,
    require_full_coverage: bool,
    structure: Arc<ManagementStructure>,
}

pub struct ManagementShard {
    pub id: NodeId,
    pub slot_ranges: Vec<SlotRange>,
    pub node_indices: Vec<usize>,
}

pub struct ManagementStructure {
    pub slot_ranges: Vec<(SlotRange, usize)>,
    pub replica_indices: Vec<Vec<usize>>,
    pub shards: Vec<ManagementShard>,
}

pub struct ManagementTopology {
    pub nodes: Vec<NodeView>,
    pub structure: Arc<ManagementStructure>,
}

impl ManagementCache {
    fn refresh_slot_inputs(&mut self, state: &ClusterState, nodes: &[&ClusterNode]) {
        for (cached, node) in self.nodes.iter_mut().zip(nodes) {
            cached.slots = node.slots.clone();
        }
        self.owners = state.slot_ownership.owners.clone();
        self.delete_epochs = state.slot_ownership.delete_epochs.clone();
        self.withdrawn_by = state.slot_ownership.withdrawn_by.clone();
    }

    fn matches(&self, state: &ClusterState, nodes: &[&ClusterNode]) -> bool {
        // Exact input comparisons also cover bottom-level mutations that do
        // not bump the topology generation. Live fields never enter this key.
        self.require_full_coverage == state.require_full_coverage
            && self.nodes.len() == nodes.len()
            && self
                .nodes
                .iter()
                .zip(nodes)
                .all(|(cached, node)| cached.matches(node))
            && self.owners == state.slot_ownership.owners
            && self.delete_epochs == state.slot_ownership.delete_epochs
            && self.withdrawn_by == state.slot_ownership.withdrawn_by
    }

    fn build(state: &ClusterState, nodes: &[&ClusterNode]) -> Self {
        let snapshot = state.build_topology_snapshot(&BTreeMap::new(), state.myself.repl_offset);
        let indices = snapshot
            .nodes
            .iter()
            .enumerate()
            .map(|(index, node)| (node.id.as_str(), index))
            .collect::<BTreeMap<_, _>>();
        let mut slot_ranges = Vec::new();
        let mut slot = 0;
        while slot < snapshot.slot_owners.len() {
            let Some(owner) = &snapshot.slot_owners[slot] else {
                slot += 1;
                continue;
            };
            let start = slot;
            while slot + 1 < snapshot.slot_owners.len()
                && snapshot.slot_owners[slot + 1].as_ref() == Some(owner)
            {
                slot += 1;
            }
            if let Some(&index) = indices.get(owner.as_str()) {
                slot_ranges.push((
                    SlotRange {
                        start: start as u16,
                        end: slot as u16,
                    },
                    index,
                ));
            }
            slot += 1;
        }
        let replica_indices = snapshot
            .nodes
            .iter()
            .map(|primary| {
                snapshot
                    .nodes
                    .iter()
                    .enumerate()
                    .filter_map(|(index, node)| {
                        (node.role == super::ClusterNodeRole::Replica
                            && node.replica_of.as_ref() == Some(&primary.id))
                        .then_some(index)
                    })
                    .collect()
            })
            .collect();
        let shards = snapshot
            .shards
            .iter()
            .map(|shard| ManagementShard {
                id: shard.id.clone(),
                slot_ranges: shard.slot_ranges.clone(),
                node_indices: shard
                    .nodes
                    .iter()
                    .filter_map(|node| indices.get(node.id.as_str()).copied())
                    .collect(),
            })
            .collect();
        Self {
            nodes: nodes
                .iter()
                .map(|node| StructuralNode {
                    name: node.name.clone(),
                    shard_id: node.shard_id.clone(),
                    replica_of: node.replicaof.clone(),
                    flags: node.flags & STRUCTURAL_FLAGS,
                    config_epoch: node.config_epoch,
                    slots: node.slots.clone(),
                })
                .collect(),
            owners: state.slot_ownership.owners.clone(),
            delete_epochs: state.slot_ownership.delete_epochs.clone(),
            withdrawn_by: state.slot_ownership.withdrawn_by.clone(),
            require_full_coverage: state.require_full_coverage,
            structure: Arc::new(ManagementStructure {
                slot_ranges,
                replica_indices,
                shards,
            }),
        }
    }
}

impl ClusterState {
    pub fn management_topology(
        &self,
        replication: &BTreeMap<String, ClusterReplicationView>,
        local_replication_offset: i64,
    ) -> ManagementTopology {
        let mut nodes = std::iter::once(&self.myself)
            .chain(self.nodes.values().filter(|node| !node.is_myself()))
            .collect::<Vec<_>>();
        nodes.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        let structure = {
            let mut cache = self.management_cache.borrow_mut();
            if cache
                .as_ref()
                .is_none_or(|cache| !cache.matches(self, &nodes))
            {
                *cache = Some(ManagementCache::build(self, &nodes));
            } else if let Some(cache) = cache.as_mut() {
                // A no-op write may detach storage without changing topology.
                // Keep the shared snapshots current after exact comparison.
                cache.refresh_slot_inputs(self, &nodes);
            }
            Arc::clone(
                &cache
                    .as_ref()
                    .expect("management cache was populated")
                    .structure,
            )
        };
        let nodes = nodes
            .into_iter()
            .map(|node| {
                node_view(
                    node,
                    self,
                    node.is_myself()
                        .then(|| replication.get(&node.name))
                        .flatten(),
                    node.is_myself().then_some(local_replication_offset),
                    false,
                )
            })
            .collect();
        ManagementTopology { nodes, structure }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn management_cache_shares_slot_inputs() {
        let state = ClusterState::new("127.0.0.1", 6379, 16379);
        let nodes = vec![&state.myself];
        let cache = ManagementCache::build(&state, &nodes);
        assert_eq!(cache.nodes[0].slots.as_ptr(), state.myself.slots.as_ptr());
        assert_eq!(cache.owners.as_ptr(), state.slot_ownership.owners.as_ptr());
        assert_eq!(
            cache.delete_epochs.as_ptr(),
            state.slot_ownership.delete_epochs.as_ptr()
        );
        assert_eq!(
            cache.withdrawn_by.as_ptr(),
            state.slot_ownership.withdrawn_by.as_ptr()
        );
    }

    #[test]
    fn no_op_writes_refresh_snapshots_without_rebuilding_structure() {
        let mut state = ClusterState::new("127.0.0.1", 6379, 16379);
        let original = state.management_topology(&BTreeMap::new(), 0);
        state.myself.slots[8] = false;
        state.slot_ownership.owners[8] = SlotOwner::Unassigned;
        let refreshed = state.management_topology(&BTreeMap::new(), 0);
        assert!(Arc::ptr_eq(&original.structure, &refreshed.structure));
        let cache = state.management_cache.borrow();
        let cache = cache.as_ref().unwrap();
        assert_eq!(cache.nodes[0].slots.as_ptr(), state.myself.slots.as_ptr());
        assert!(cache.owners.shares_storage(&state.slot_ownership.owners));
    }
}
