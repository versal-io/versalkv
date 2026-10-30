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
    pub(crate) fn drive_forkless_feed(&mut self) -> bool {
        let was_capturing = self.persistence.forkless_capture_in_progress();
        let mut additional_datasets = self
            .slot_imports
            .iter_mut()
            .map(|(id, dataset)| (id.as_str().as_bytes().to_vec(), dataset.dbs_mut()))
            .collect::<Vec<_>>();
        let released = self
            .persistence
            .drive_forkless_feed(&mut self.db.dbs, &mut additional_datasets);
        drop(additional_datasets);
        if was_capturing && !self.persistence.forkless_capture_in_progress() {
            self.set_forkless_capture_expiration_paused(false);
        }
        released
    }
    pub(crate) fn poll_forkless_save(
        &mut self,
        now_sec: i64,
    ) -> Option<crate::foundation::io::persistence::snapshot::background::RdbSaveOutcome> {
        let mut additional_datasets = self
            .slot_imports
            .iter_mut()
            .map(|(id, dataset)| (id.as_str().as_bytes().to_vec(), dataset.dbs_mut()))
            .collect::<Vec<_>>();
        let outcome = self.persistence.poll_forkless_save(
            now_sec,
            &mut self.db.dbs,
            &mut additional_datasets,
        );
        drop(additional_datasets);
        if outcome.is_some() {
            self.set_forkless_capture_expiration_paused(false);
        }
        outcome
    }
    pub(crate) fn abort_forkless_save(&mut self, now_sec: i64) {
        let mut additional_datasets = self
            .slot_imports
            .iter_mut()
            .map(|(id, dataset)| (id.as_str().as_bytes().to_vec(), dataset.dbs_mut()))
            .collect::<Vec<_>>();
        self.persistence
            .abort_forkless_save(now_sec, &mut self.db.dbs, &mut additional_datasets);
        drop(additional_datasets);
        self.set_forkless_capture_expiration_paused(false);
    }
    pub(crate) fn set_forkless_capture_expiration_paused(&mut self, is_paused: bool) {
        self.db.dbs.set_pause_skip_expire_all(is_paused);
        for dataset in self.slot_imports.values_mut() {
            dataset.dbs_mut().set_pause_skip_expire_all(is_paused);
        }
    }
    pub fn aof(&self) -> &AofState {
        self.persistence.aof()
    }
    pub(crate) fn aof_mut(&mut self) -> &mut AofState {
        self.persistence.aof_mut()
    }
}
