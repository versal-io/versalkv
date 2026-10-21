use super::{ServerContext, TrackingState};
use crate::engine::distributed::pubsub::PubSubState;
use crate::engine::distributed::replication::ReplicationState;
use crate::framework::distributed::migration::transport::MigrateSocketCache;

impl ServerContext {
    // ── Pub/Sub and tracking ──
    pub fn pubsub(&self) -> &PubSubState {
        &self.pubsub
    }
    pub(crate) fn pubsub_mut(&mut self) -> &mut PubSubState {
        &mut self.pubsub
    }
    pub fn tracking(&self) -> &TrackingState {
        &self.tracking
    }
    pub(crate) fn tracking_mut(&mut self) -> &mut TrackingState {
        &mut self.tracking
    }

    // ── Replication and migration ──
    pub fn replication(&self) -> &ReplicationState {
        &self.replication
    }
    pub(crate) fn replication_mut(&mut self) -> &mut ReplicationState {
        &mut self.replication
    }

    /// Replication identity to persist in an RDB. A replica must persist the
    /// upstream replid and applied offset, not its local primary backlog cursor,
    /// so restart can issue PSYNC2 against that upstream history.
    pub fn rdb_replication_info(&self) -> crate::foundation::io::persistence::RdbReplicationInfo {
        let offset = if self.replication.is_replica() {
            self.replication.slave_repl_offset.get()
        } else {
            self.replication.primary_repl_offset().get()
        };
        let mut info = crate::foundation::io::persistence::RdbReplicationInfo {
            repl_stream_db: self.repl_seldb().max(0),
            repl_id: self.replication.backlog.replid().to_string(),
            repl_offset: offset,
            aux_fields: Vec::new(),
        };
        if let Some(cluster) = self.cluster_state.as_deref() {
            info.aux_fields.extend(
                cluster
                    .migrations
                    .views()
                    .iter()
                    .filter_map(
                        crate::framework::distributed::migration::SlotImportMetadata::from_job,
                    )
                    .map(|metadata| {
                        (
                            crate::framework::distributed::migration::SLOT_IMPORT_AUX_KEY.to_vec(),
                            metadata.encode(),
                        )
                    }),
            );
        }
        info
    }

    /// Replication snapshot metadata, including state that must travel only on
    /// a full sync and must not be restored from an ordinary persisted RDB.
    pub fn rdb_full_sync_replication_info(
        &self,
    ) -> crate::foundation::io::persistence::RdbReplicationInfo {
        let mut info = self.rdb_replication_info();
        if let Some(slot_states) = self
            .cluster_state
            .as_deref()
            .and_then(|cluster| cluster.encode_open_slots_aux_field())
        {
            info.aux_fields
                .push((b"cluster-slot-states".to_vec(), slot_states));
        }
        info
    }
    /// Current replication SELECT DB index (-1 means unset / use connection's db).
    pub fn repl_seldb(&self) -> i64 {
        self.replication.repl_seldb
    }
    /// Set the replication SELECT DB index.
    pub fn set_repl_seldb(&mut self, val: i64) {
        self.replication.repl_seldb = val;
    }
    /// Reset the replication SELECT DB index to "unset" (-1).
    pub fn reset_repl_seldb(&mut self) {
        self.replication.repl_seldb = -1;
    }
    pub fn migrate_sockets(&self) -> &MigrateSocketCache {
        &self.migrate_sockets
    }
    pub(crate) fn migrate_sockets_mut(&mut self) -> &mut MigrateSocketCache {
        &mut self.migrate_sockets
    }
}
