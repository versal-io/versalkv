//! Framework-layer (L4) distributed orchestration.
//!
//! This layer owns the *mechanism* side of distributed features: live links,
//! transports, timers, and per-tick runtime orchestration. The *state* side --
//! the durable contracts such as `ReplicationState` and `SentinelState` -- lives
//! one layer down in `engine::distributed`, which this layer drives.
//!
//! Division of responsibility against `engine::distributed`:
//!
//! - `replication`: engine holds `ReplicationState` (offsets, backlog, replica
//!   bookkeeping); this module holds the primary/replica links, RDB receive
//!   pipeline, pending buffer, and per-tick runtime that advance that state.
//! - `sentinel`: engine holds `SentinelState` (monitored instances, failover
//!   bookkeeping); this module holds the sentinel links, transports, and timers.
//! - `migration`: engine models slot migration under `cluster::migration` (slot
//!   ownership is a cluster concern). This module keeps the migration apply,
//!   dataset import, and transport here because those are link/IO-bound
//!   mechanisms rather than cluster-topology state. The naming is intentionally
//!   flattened to `migration` since L4 has no `cluster` submodule of its own --
//!   cluster command handlers live under `commands/cluster`.

pub mod migration;
pub mod replication;
pub mod sentinel;
