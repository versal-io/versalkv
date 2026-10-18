use crate::engine::distributed::cluster::ClusterLinkView;

use super::ClusterHealth;

/// Stable read-only summary of ClusterState for logs and diagnostics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterDebugSnapshot {
    pub topology_generation: u64,
    pub health: ClusterHealth,
    pub current_epoch: u64,
    pub node_count: usize,
    pub primary_count: usize,
    pub assigned_slot_count: usize,
    pub link_count: usize,
    pub links: Vec<ClusterLinkView>,
    pub pending: ClusterPendingWork,
    pub active: ClusterActiveOperations,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClusterActiveOperations {
    pub migrations: usize,
    pub automatic_failover: bool,
    pub manual_failover: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClusterPendingWork {
    pub meets: usize,
    pub forgets: usize,
    pub connects: usize,
    pub link_closes: usize,
    pub actions: usize,
    pub migration_actions: usize,
    pub topology_broadcast: bool,
    pub config_save: bool,
    pub config_fsync: bool,
    pub replication_switch: bool,
}
