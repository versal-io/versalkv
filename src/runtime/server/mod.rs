use std::io;
use std::ops::{Deref, DerefMut};

mod message;
mod shared;
pub(crate) mod supervisor;
pub(crate) mod worker;

use supervisor::ServerSupervisor;
use worker::ServerWorkerState;

pub(crate) struct ServerRuntime {
    supervisor: ServerSupervisor,
}

impl ServerRuntime {
    pub fn from_state(state: ServerWorkerState) -> Self {
        Self {
            supervisor: ServerSupervisor::from_state(state),
        }
    }

    pub fn new(num_dbs: u32) -> io::Result<Self> {
        ServerWorkerState::new(num_dbs).map(Self::from_state)
    }

    pub fn new_with_slot_router(
        num_dbs: u32,
        slot_bits: u32,
        slot_router: fn(&[u8]) -> usize,
    ) -> io::Result<Self> {
        ServerWorkerState::new_with_slot_router(num_dbs, slot_bits, slot_router)
            .map(Self::from_state)
    }

    pub fn run(&mut self) -> io::Result<()> {
        self.supervisor.run()
    }

    #[cfg(test)]
    pub(crate) fn run_owned_for_test(state: ServerWorkerState) -> io::Result<Self> {
        let mut server = Self::from_state(state);
        server.run()?;
        Ok(server)
    }
}

impl Deref for ServerRuntime {
    type Target = ServerWorkerState;

    fn deref(&self) -> &Self::Target {
        self.supervisor.state()
    }
}

impl DerefMut for ServerRuntime {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.supervisor.state_mut()
    }
}
