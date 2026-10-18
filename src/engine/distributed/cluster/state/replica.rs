use crate::engine::distributed::cluster::NodeId;
use crate::engine::distributed::replication::ReplicationOffset;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClusterReplicaHealth {
    Loading,
    Online,
    Disconnected,
    Stale,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterReplicationView {
    pub upstream: Option<NodeId>,
    pub received_offset: ReplicationOffset,
    pub applied_offset: ReplicationOffset,
    pub acknowledged_offset: ReplicationOffset,
    pub priority: i64,
    pub health: ClusterReplicaHealth,
}

impl ClusterReplicationView {
    pub fn disconnected() -> Self {
        let zero = ReplicationOffset::try_from(0).expect("zero is a valid replication offset");
        Self {
            upstream: None,
            received_offset: zero,
            applied_offset: zero,
            acknowledged_offset: zero,
            priority: 0,
            health: ClusterReplicaHealth::Disconnected,
        }
    }
}
