//! Cluster bus manager (split into submodules).
//! - `types`: connection/manager structs + connection lifecycle methods
//! - `recv`: incoming read path (readable/parse/dispatch/handshake handlers)
//! - `gossip`: gossip application (slot-claim processing, node upsert)
//! - `send`: outgoing message builders + writable
//! - `cron`: periodic gossip/failure-detection/failover tick

#[path = "cron.rs"]
mod cron;
mod gossip;
mod recv;
#[cfg(test)]
mod recv_edge_tests;
#[cfg(test)]
mod recv_failover_tests;
#[cfg(test)]
mod recv_tests;
mod send;
mod slotclaim;
mod slotclaim_demotion;
#[cfg(test)]
mod slotclaim_demotion_tests;
#[cfg(test)]
mod slotclaim_tests;
mod types;

pub use types::ClusterBusManager;
#[cfg(test)]
pub(crate) use types::ReceivedClusterPublish;
