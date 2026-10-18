use super::ServerContext;
use crate::engine::persistence::{AofState, PersistenceState};

impl ServerContext {
    // ── Persistence ──
    pub fn persistence(&self) -> &PersistenceState {
        &self.persistence
    }
    pub(crate) fn persistence_mut(&mut self) -> &mut PersistenceState {
        &mut self.persistence
    }
    pub fn aof(&self) -> &AofState {
        self.persistence.aof()
    }
    pub(crate) fn aof_mut(&mut self) -> &mut AofState {
        self.persistence.aof_mut()
    }
}
