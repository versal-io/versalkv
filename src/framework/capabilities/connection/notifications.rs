//! Per-command Pub/Sub and keyspace notification queues.

use bytes::Bytes;

type Notification = (Bytes, Bytes);

#[derive(Default)]
pub(super) struct PendingNotifications {
    pub(super) module: Vec<crate::engine::extension::module::notifications::ModuleKeyspaceEvent>,
    pub(super) callbacks:
        Vec<crate::engine::extension::module::api::call::pending::ModuleCallEffects>,
    pub(super) local: Vec<Notification>,
    pub(super) shard: Vec<Notification>,
    pub(super) cluster: Vec<Notification>,
    pub(super) cluster_shard: Vec<Notification>,
}
