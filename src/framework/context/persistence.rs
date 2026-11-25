use super::ServerContext;
use crate::engine::persistence::{AofState, PersistenceState};

impl ServerContext {
    pub(crate) fn abort_upstream_transaction(&mut self, connection: usize) {
        if self
            .replication_mut()
            .end_upstream_transaction(connection)
            .is_some_and(|phase| phase.invalidates_baseline_on_abort())
        {
            self.aof_mut().invalidate_replication_baseline();
        }
    }

    pub(crate) fn abort_upstream_transactions(&mut self) -> Vec<usize> {
        let transactions = self.replication_mut().take_upstream_transactions();
        if transactions
            .values()
            .any(|phase| phase.invalidates_baseline_on_abort())
        {
            self.aof_mut().invalidate_replication_baseline();
        }
        transactions.into_keys().collect()
    }

    pub(crate) fn replication_aof_write_error(
        &self,
    ) -> Option<crate::foundation::io::protocol::Value> {
        if !self.config().replication.repl_after_aof_fsync {
            return None;
        }
        let pending_limit = self.config().replication.replica_output_buffer_hard_limit;
        if pending_limit != 0
            && self.replication().backlog.unreleased_bytes()
                >= pending_limit.max(self.config().server.repl_backlog_size.max(0) as usize)
        {
            return Some(crate::foundation::io::protocol::Value::error(
                "OOM local AOF replication backlog is waiting for synchronization",
            ));
        }
        if self.aof().last_write_status == crate::engine::persistence::OperationStatus::Err
            || self.aof().replication_baseline != crate::engine::persistence::AofBaseline::Ready
            || self
                .aof()
                .journal
                .as_ref()
                .is_none_or(|journal| !journal.appender.last_write_ok())
        {
            return Some(crate::foundation::io::protocol::Value::error(
                "MISCONF local AOF is not durable; data writes are disabled",
            ));
        }
        None
    }

    pub(crate) fn ensure_replication_snapshot_durable(&mut self) -> Result<(), String> {
        if self.config().replication.repl_after_aof_fsync {
            self.sync_replication_aof()?;
        }
        Ok(())
    }

    pub(crate) fn sync_replication_aof(&mut self) -> Result<(), String> {
        if self.aof().replication_baseline != crate::engine::persistence::AofBaseline::Ready {
            return Err("ERR local AOF baseline is not ready".to_string());
        }
        let journal = self
            .aof_mut()
            .journal
            .as_mut()
            .ok_or_else(|| "ERR local AOF baseline is not ready".to_string())?;
        journal
            .appender
            .flush(true)
            .map_err(|error| format!("MISCONF local AOF synchronization failed: {error}"))
    }

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
