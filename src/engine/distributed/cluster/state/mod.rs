//! Cluster state machine (split into submodules).
//! ClusterState holds the entire cluster view: the local node, all known nodes,
//! slot assignments, migration state, and failover state.
//! - `types`: struct definition, constants, construction/reset, shared helpers
//! - `slots`: slot assignment + migration mutation ops
//! - `routing`: slot ownership resolution, redirect, key extraction, slots array
//! - `topology`: state recomputation, CLUSTER NODES/INFO rendering, replicate
//! - `failover`: quorum, fail reports, promotion, orphan-to-replica

mod debug;
#[path = "failover.rs"]
pub mod failover;
mod handshake;
pub mod invariants;
mod replica;
mod replica_migration;
mod routing;
#[cfg(test)]
mod routing_tests;
mod shard_pubsub;
mod slot_range;
mod slots;
mod snapshot;
mod stale_primary;
#[cfg(test)]
mod stale_primary_tests;
mod topology;
mod types;

pub use crate::engine::distributed::cluster::action::ReplicationSwitchMode;
pub use debug::ClusterDebugSnapshot;
pub use replica::{ClusterReplicaHealth, ClusterReplicationView};
pub use replica_migration::{
    select_replica_migration, MigrationFacts, MigrationPrimaryFacts, MigrationReplicaFacts,
};
#[cfg(test)]
pub use routing::RedirectOptions;
pub use routing::{ClusterRouteDecision, OpenSlotPolicy, RouteRequest};
pub use slot_range::{SlotRange, SlotRangeError};
pub(crate) use slots::{OpenSlotUpdate, SlotClaimDecision, SlotOwnerUpdate};
pub use snapshot::{
    ClusterHealth, ClusterNodeHealth, ClusterNodeRole, ClusterTopologySnapshot, NodeView, ShardView,
};
pub use stale_primary::{DemotionReason, StalePrimaryDecision};
pub use types::{
    ClusterLinkDirection, ClusterState, PendingReplicationSwitch, CLUSTER_OK, CLUSTER_SLOTS,
};
