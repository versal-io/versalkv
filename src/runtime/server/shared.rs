use std::num::NonZeroUsize;
use std::sync::Arc;

use super::message::WorkerId;

#[derive(Clone)]
pub(super) struct ServerShared {
    topology: Arc<ServerTopology>,
}

struct ServerTopology {
    worker_count: NonZeroUsize,
}

impl ServerShared {
    pub(super) fn single_worker() -> Self {
        Self {
            topology: Arc::new(ServerTopology {
                worker_count: NonZeroUsize::MIN,
            }),
        }
    }

    pub(super) fn contains(&self, id: WorkerId) -> bool {
        id.index() < self.topology.worker_count.get()
    }

    pub(super) fn shares_topology_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.topology, &other.topology)
    }
}
