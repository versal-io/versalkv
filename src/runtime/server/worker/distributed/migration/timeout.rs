use super::super::super::ServerWorkerState;
use crate::engine::distributed::cluster::migration::{
    ExportPhase, ImportPhase, MigrationJobId, MigrationOperation, MigrationPhase,
};
use std::time::{Duration, Instant};

impl ServerWorkerState {
    pub(super) fn expire_stalled_slot_migrations(&mut self) {
        let timeout_sec = self.ctx.config().replication.repl_timeout;
        if timeout_sec == 0 {
            return;
        }
        let timeout = Duration::from_secs(timeout_sec);
        let now = Instant::now();
        let now_unix = crate::foundation::io::sys::time::unixtime();
        let stalled = self
            .ctx
            .cluster_state()
            .as_deref()
            .map(|state| {
                state
                    .migrations
                    .views()
                    .into_iter()
                    .filter(|job| {
                        if !job.is_active_for_connection_timeout() {
                            return false;
                        }
                        match job.operation {
                            // Exports only ever write to the target (snapshot data,
                            // ACKs); the connection is never read after handshake, so
                            // `last_interaction` is never bumped. Mirror C: use the
                            // last ACK time the source sent to the target.
                            MigrationOperation::Export => {
                                job.last_ack_time > 0
                                    && now_unix.saturating_sub(job.last_ack_time)
                                        > timeout_sec as i64
                            }
                            // Imports read from the source; the connection's
                            // `last_interaction` is bumped on every incoming frame.
                            MigrationOperation::Import => self
                                .connection_registry
                                .iter()
                                .find_map(|(_, connection)| {
                                    connection
                                        .slot_migration_role
                                        .as_ref()
                                        .is_some_and(|role| role.job_id() == &job.id)
                                        .then_some(connection.last_interaction)
                                })
                                .is_some_and(|last| now.duration_since(last) > timeout),
                        }
                    })
                    .map(|job| job.id)
                    .collect::<Vec<MigrationJobId>>()
            })
            .unwrap_or_default();
        for id in stalled {
            self.fail_slot_migration(&id, "Timed out after too long with no interaction");
        }
    }
}

trait ConnectionTimeoutPhase {
    fn is_active_for_connection_timeout(&self) -> bool;
}

impl ConnectionTimeoutPhase for crate::engine::distributed::cluster::migration::MigrationJobView {
    fn is_active_for_connection_timeout(&self) -> bool {
        !self.phase.is_terminal()
            && !matches!(
                self.phase,
                MigrationPhase::Export(ExportPhase::FailoverGranted)
                    | MigrationPhase::Import(ImportPhase::OccurringOnPrimary)
            )
    }
}
