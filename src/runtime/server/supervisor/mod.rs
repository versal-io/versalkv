pub(crate) mod shutdown;

use std::io;

use super::message::WorkerId;
use super::shared::ServerShared;
use super::worker::{ServerWorkerRuntime, ServerWorkerState};

pub(super) struct ServerSupervisor {
    shared: ServerShared,
    worker: ServerWorkerRuntime,
}

impl ServerSupervisor {
    pub(super) fn from_state(state: ServerWorkerState) -> Self {
        let shared = ServerShared::single_worker();
        let worker = ServerWorkerRuntime::from_state(WorkerId::PRIMARY, shared.clone(), state);
        Self { shared, worker }
    }

    pub(super) fn run(&mut self) -> io::Result<()> {
        debug_assert_eq!(self.worker.id(), WorkerId::PRIMARY);
        debug_assert!(self.worker.shares_topology_with(&self.shared));
        self.worker.run()
    }

    pub(super) fn state(&self) -> &ServerWorkerState {
        self.worker.state()
    }

    pub(super) fn state_mut(&mut self) -> &mut ServerWorkerState {
        self.worker.state_mut()
    }
}
