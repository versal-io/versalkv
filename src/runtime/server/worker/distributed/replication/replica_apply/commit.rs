use super::super::super::super::ServerWorkerState;
use crate::engine::distributed::cluster::migration::{MigrationJob, MigrationRegistry};
use crate::framework::distributed::migration::{SlotImportMetadata, SLOT_IMPORT_AUX_KEY};
use std::collections::HashSet;

pub(super) fn replication_buffer_limit_message(limit: usize) -> String {
    format!(
        "Replication buffer limit reached ({limit} bytes), stopping buffering. Further accumulation will occur on primary side."
    )
}

pub(super) struct PreparedRdbCommit {
    pub(super) function_codes: Vec<Vec<u8>>,
    pub(super) aux_fields: Vec<(Vec<u8>, Vec<u8>)>,
    pub(super) repl_stream_db: u32,
    pub(super) disk_backed: bool,
    pub(super) raw: Option<Vec<u8>>,
    pub(super) swapped: Option<crate::foundation::io::store::kv_engine::DbSet>,
}

pub(super) struct PreparedAofBaseInstall {
    pub(super) manifest: crate::foundation::io::persistence::PreparedSyncedRdbAofBase,
    pub(super) journal: crate::foundation::io::persistence::journal::AofJournal,
}

pub(super) struct PreparedReceivedRdb {
    pub(super) temp: std::path::PathBuf,
    pub(super) target: std::path::PathBuf,
}

impl PreparedReceivedRdb {
    pub(super) fn commit(self) -> std::io::Result<()> {
        std::fs::rename(&self.temp, &self.target).map_err(|error| {
            let target_name = self
                .target
                .file_name()
                .unwrap_or(self.target.as_os_str())
                .to_string_lossy();
            crate::server_log!(
                warning,
                "Failed trying to rename the temp DB into {} in PRIMARY <-> REPLICA synchronization: {}",
                target_name,
                error
            );
            error
        })
    }
}

impl Drop for PreparedReceivedRdb {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.temp);
    }
}

impl ServerWorkerState {
    pub(super) fn validate_streamed_rdb_metadata(
        &self,
        aux_fields: &[(Vec<u8>, Vec<u8>)],
        function_codes: &[Vec<u8>],
    ) -> Result<(), ()> {
        self.validate_slot_import_aux(aux_fields)?;
        if self.ctx.cluster_state().is_some() {
            let slot_states = aux_fields
                .iter()
                .find_map(|(key, value)| {
                    key.eq_ignore_ascii_case(b"cluster-slot-states")
                        .then_some(value.as_slice())
                })
                .unwrap_or_default();
            crate::engine::distributed::cluster::ClusterState::validate_open_slots_aux_field(
                slot_states,
            )
            .map_err(|error| {
                crate::server_log!(warning, "Failed to restore cluster open slots: {}", error);
            })?;
        }
        self.validate_function_replacement(function_codes)
    }

    fn validate_slot_import_aux(&self, aux_fields: &[(Vec<u8>, Vec<u8>)]) -> Result<(), ()> {
        let metadata = aux_fields
            .iter()
            .filter(|(key, _)| key.eq_ignore_ascii_case(SLOT_IMPORT_AUX_KEY))
            .map(|(_, value)| SlotImportMetadata::decode(value))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| {
                crate::server_log!(warning, "Failed to restore cluster slot imports: {}", error);
            })?;
        if !metadata.is_empty() && self.ctx.cluster_state().is_none() {
            crate::server_log!(
                warning,
                "Failed to restore cluster slot imports: cluster mode is disabled"
            );
            return Err(());
        }
        let jobs = metadata
            .into_iter()
            .map(|import| {
                MigrationJob::new_tracking_import(
                    import.id,
                    import.source,
                    import.target,
                    import.ranges,
                    crate::foundation::io::sys::time::unixtime(),
                )
            })
            .collect();
        MigrationRegistry::default()
            .start_all(jobs)
            .map_err(|error| {
                crate::server_log!(warning, "Failed to restore cluster slot imports: {error:?}");
            })?;
        Ok(())
    }

    fn validate_function_replacement(&self, function_codes: &[Vec<u8>]) -> Result<(), ()> {
        let registry = &self.ctx.script().function_registry;
        let mut library_names = HashSet::new();
        let mut function_names = HashSet::new();
        for code in function_codes {
            let code = String::from_utf8_lossy(code);
            let library_name = crate::engine::extension::lua::shebang::extract_shebang_name(&code)
                .map_err(|_| ())?;
            if !library_names.insert(library_name) {
                return Err(());
            }
            let engine_name = crate::engine::extension::function::parse_engine_name(&code);
            let engine = registry.engine_mgr().get(&engine_name).ok_or(())?;
            let compiled = engine.compile(&code, 0).map_err(|_| ())?;
            if compiled
                .iter()
                .any(|function| !function_names.insert(function.name.to_ascii_lowercase()))
            {
                return Err(());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::replication_buffer_limit_message;

    #[test]
    fn replication_buffer_limit_log_matches_valkey_wording() {
        assert_eq!(
            replication_buffer_limit_message(16_383),
            "Replication buffer limit reached (16383 bytes), stopping buffering. Further accumulation will occur on primary side."
        );
    }
}
