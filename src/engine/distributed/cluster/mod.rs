//! # Cluster Module (Engine Layer, L2)
//! This module provides the cluster state machine, node management, and
//! cluster bus protocol for the Valkey Rust server.
//! ## Layering
//! - This is part of the engine layer (L3).
//! - It depends on foundation value/protocol utilities and the standard library.
//! - It MUST NOT depend on framework, runtime, or server.
//! ## Current Status
//! Phase 0: Single-node cluster mode with basic command support.
//! - ClusterState with node ID, shard ID, slot bitmap
//! - ClusterNode with basic attributes
//! - ClusterLink for bus connections (stub)
//! - No gossip, no failover, no slot migration yet.

pub mod action;
pub mod bus;
pub mod config;
mod display;
pub mod endpoint;
pub mod event;
pub mod link;
pub mod migration;
pub mod msg;
pub mod node;
pub mod projection;
pub mod reducer;
pub mod state;

pub use endpoint::{
    AdvertisedEndpoint, ClientTransport, ClusterEndpoints, EndpointError, NodeId, NodeIdError,
    PreferredEndpoint,
};
pub use link::{ClusterLinkView, LinkBufferEvent, LinkEvents, LinkIoEvent};
pub use projection::ClusterProjection;
pub use state::{
    ClusterDebugSnapshot, ClusterHealth, ClusterLinkDirection, ClusterNodeHealth, ClusterNodeRole,
    ClusterRouteDecision, ClusterState, ClusterTopologySnapshot, DemotionReason, NodeView,
    OpenSlotPolicy, RouteRequest, ShardView, SlotRange, SlotRangeError, StalePrimaryDecision,
};
