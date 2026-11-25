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
        })?;
        std::fs::File::open(self.target.parent().unwrap_or(std::path::Path::new(".")))?.sync_all()
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
    use super::{PreparedRdbCommit, ServerWorkerState};

    fn assert_strict_baseline(mode: &str, preamble: bool, multipart: bool) {
        use crate::foundation::io::persistence::{AofAppender, AofFsync};
        use crate::foundation::io::store::{kv_engine::DbSet, Robj};
        let dir = std::env::temp_dir().join(format!(
            "versal-task5-baseline-{mode}-{preamble}-{multipart}-{}",
            std::process::id(),
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut worker = ServerWorkerState::new(1).unwrap();
        worker.ctx.config_mut().replication.repl_after_aof_fsync = true;
        let config = &mut worker.ctx.config_mut().persistence;
        config.dir = dir.to_string_lossy().into_owned();
        config.aof_enabled = true;
        config.aof_use_rdb_preamble = preamble;
        let appenddirname = if multipart { "appendonlydir" } else { "" };
        config.aof_appenddirname = appenddirname.to_string();
        config.aof_fsync = AofFsync::No;
        let mut old = AofAppender::open_append_path(&dir.join("old.aof"), AofFsync::No).unwrap();
        old.feed(b"*1\r\n$4\r\nPING\r\n");
        worker.ctx.aof_mut().journal =
            Some(crate::foundation::io::persistence::journal::AofJournal::new(old));
        worker
            .ctx
            .db_mut(0)
            .unwrap()
            .set(b"old".to_vec(), Robj::string("old"));
        let mut dataset = DbSet::new(1);
        dataset
            .get_mut(0)
            .unwrap()
            .set(b"new".to_vec(), Robj::string("value"));
        let raw = (mode == "disk")
            .then(|| crate::foundation::io::persistence::serialize_all(&dataset, 0));
        let swapped = if mode == "direct" {
            *worker.ctx.dbs_mut() = dataset;
            None
        } else {
            Some(dataset)
        };
        let code =
            b"#!lua name=baseline\nserver.register_function('baselinefn', function() return 1 end)";
        let prepared = PreparedRdbCommit {
            function_codes: vec![code.to_vec()],
            aux_fields: Vec::new(),
            repl_stream_db: 0,
            disk_backed: mode == "disk",
            raw,
            swapped,
        };

        let mut link = crate::framework::distributed::replication::ReplicaLink::new(
            crate::framework::distributed::replication::ConnectionId(84),
            crate::framework::distributed::replication::MasterInfo {
                host: "127.0.0.1".to_string(),
                port: 6379,
            },
        );
        link.set_resume_position(
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
            crate::engine::distributed::replication::ReplicationOffset::try_from(19).unwrap(),
            0,
        );
        let install = worker
            .prepare_synced_rdb_aof_base(&prepared, &link)
            .unwrap()
            .expect("strict full sync must prepare a durable AOF baseline in every load mode");
        let handle = install.journal.appender.sync_handle();
        assert!(
            handle.is_durable(install.journal.appender.appended_stamp()),
            "the replacement journal must already be synced before publication"
        );
        drop(install);
        let mut connection = crate::framework::capabilities::connection::Connection::new_fake();
        connection.set_token_for_test(crate::framework::capabilities::io::Token(84));
        let token = worker.connection_registry.insert(connection);
        worker.ctx.set_loading(true);
        worker
            .commit_streamed_rdb(
                token,
                &mut link,
                crate::framework::distributed::replication::RdbCommit {
                    function_codes: prepared.function_codes,
                    aux_fields: prepared.aux_fields,
                    repl_stream_db: prepared.repl_stream_db,
                    diskless: mode != "disk",
                    disk_backed: prepared.disk_backed,
                    raw: prepared.raw,
                    swapped: prepared.swapped,
                },
            )
            .unwrap();
        assert_eq!(
            worker.ctx.aof().replication_baseline,
            crate::engine::persistence::AofBaseline::Ready
        );
        assert_eq!(worker.ctx.aof().fsynced_repl_offset, Some(19));
        assert!(!worker.ctx.is_loading());
        let journal = worker.ctx.aof().journal.as_ref().unwrap();
        assert!(journal
            .appender
            .sync_handle()
            .is_durable(journal.appender.appended_stamp()));
        let paths = crate::foundation::io::persistence::resolve_aof_read_paths_checked(
            dir.to_str().unwrap(),
            "appendonly.aof",
            appenddirname,
        )
        .unwrap();
        let bytes = std::fs::read(&paths[0]).unwrap();
        let mut loaded = DbSet::new(1);
        let (_, _, _, functions) =
            crate::foundation::io::persistence::load_rdb_bytes_checked_with_functions(
                &bytes,
                &mut loaded,
                true,
            )
            .unwrap();
        assert!(loaded.get(0).unwrap().get_no_expire(b"new").is_some());
        assert!(loaded.get(0).unwrap().get_no_expire(b"old").is_none());
        assert_eq!(functions, vec![code.to_vec()]);
        assert_eq!(
            std::fs::read(dir.join("old.aof")).unwrap(),
            b"*1\r\n$4\r\nPING\r\n"
        );
        assert!(!worker.ctx.persistence().has_active_child());
        worker
            .ctx
            .aof_mut()
            .journal
            .take()
            .unwrap()
            .appender
            .shutdown()
            .unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn failed_received_rdb_install_invalidates_aof_even_when_manifest_rollback_fails() {
        use crate::foundation::io::persistence::{AofAppender, AofFsync};
        use crate::foundation::io::store::{kv_engine::DbSet, Robj};
        for should_fail_rollback in [false, true] {
            let dir = std::env::temp_dir()
                .join(format!("versal-sync-rollback-{}", rand::random::<u64>()));
            let app_dir = dir.join("appendonlydir");
            std::fs::create_dir_all(&app_dir).unwrap();
            let manifest_path = app_dir.join("appendonly.aof.manifest");
            let old_manifest = b"file appendonly.aof.1.base.rdb seq 1 type b\nfile appendonly.aof.1.incr.aof seq 1 type i\n";
            let mut old_dataset = DbSet::new(1);
            old_dataset
                .get_mut(0)
                .unwrap()
                .set(b"old".to_vec(), Robj::string("old"));
            std::fs::write(
                app_dir.join("appendonly.aof.1.base.rdb"),
                crate::foundation::io::persistence::serialize_all(&old_dataset, 0),
            )
            .unwrap();
            std::fs::write(&manifest_path, old_manifest).unwrap();
            let mut worker = ServerWorkerState::new(1).unwrap();
            *worker.ctx.dbs_mut() = old_dataset;
            worker.ctx.config_mut().replication.repl_after_aof_fsync = true;
            worker.ctx.config_mut().persistence.dir = dir.to_str().unwrap().to_string();
            worker.ctx.config_mut().persistence.aof_enabled = true;
            worker.ctx.config_mut().persistence.aof_fsync = AofFsync::No;
            let appender = AofAppender::open_append_path(
                &app_dir.join("appendonly.aof.1.incr.aof"),
                AofFsync::No,
            )
            .unwrap();
            worker.ctx.aof_mut().journal =
                Some(crate::foundation::io::persistence::journal::AofJournal::new(appender));
            worker.ctx.aof_mut().replication_baseline =
                crate::engine::persistence::AofBaseline::Ready;
            std::fs::create_dir(dir.join("dump.rdb")).unwrap();
            if should_fail_rollback {
                std::fs::create_dir(app_dir.join("appendonly.aof.manifest.tmp")).unwrap();
            }
            let mut replacement = DbSet::new(1);
            replacement
                .get_mut(0)
                .unwrap()
                .set(b"new".to_vec(), Robj::string("new"));
            let raw = crate::foundation::io::persistence::serialize_all(&replacement, 0);
            let mut link = crate::framework::distributed::replication::ReplicaLink::new(
                crate::framework::distributed::replication::ConnectionId(84),
                crate::framework::distributed::replication::MasterInfo {
                    host: "127.0.0.1".to_string(),
                    port: 6379,
                },
            );
            link.set_resume_position(
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
                crate::engine::distributed::replication::ReplicationOffset::try_from(19).unwrap(),
                0,
            );
            assert!(worker
                .commit_streamed_rdb(
                    84,
                    &mut link,
                    crate::framework::distributed::replication::RdbCommit {
                        function_codes: Vec::new(),
                        aux_fields: Vec::new(),
                        repl_stream_db: 0,
                        diskless: false,
                        disk_backed: true,
                        raw: Some(raw),
                        swapped: Some(replacement),
                    }
                )
                .is_err());
            assert!(worker.ctx.db(0).unwrap().get_no_expire(b"old").is_some());
            assert!(worker.ctx.db(0).unwrap().get_no_expire(b"new").is_none());
            assert_eq!(
                worker.ctx.aof().replication_baseline,
                crate::engine::persistence::AofBaseline::Unavailable
            );
            assert_eq!(
                worker.ctx.aof().last_write_status,
                crate::engine::persistence::OperationStatus::Err
            );
            worker.flush_aof_if_needed();
            assert_eq!(
                worker.ctx.aof().last_write_status,
                crate::engine::persistence::OperationStatus::Err
            );
            if should_fail_rollback {
                assert_ne!(std::fs::read(&manifest_path).unwrap(), old_manifest);
                assert!(app_dir.join("appendonly.aof.2.base.rdb").exists());
                assert!(app_dir.join("appendonly.aof.1.base.rdb").exists());
            } else {
                assert_eq!(std::fs::read(&manifest_path).unwrap(), old_manifest);
                assert!(!app_dir.join("appendonly.aof.2.base.rdb").exists());
            }
            drop(worker);
            std::fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn task5_strict_direct_preamble_baseline() {
        assert_strict_baseline("direct", true, true);
    }
    #[test]
    fn task5_strict_direct_text_policy_baseline() {
        assert_strict_baseline("direct", false, true);
    }
    #[test]
    fn task5_strict_swap_preamble_baseline() {
        assert_strict_baseline("swap", true, true);
    }
    #[test]
    fn task5_strict_swap_text_policy_baseline() {
        assert_strict_baseline("swap", false, true);
    }
    #[test]
    fn task5_strict_disk_preamble_baseline() {
        assert_strict_baseline("disk", true, true);
    }
    #[test]
    fn task5_strict_disk_text_policy_baseline() {
        assert_strict_baseline("disk", false, true);
    }

    #[test]
    fn task5_strict_single_file_baselines_cover_all_loading_and_preamble_modes() {
        for mode in ["direct", "swap", "disk"] {
            for preamble in [false, true] {
                assert_strict_baseline(mode, preamble, false);
            }
        }
    }

    #[test]
    fn replication_buffer_limit_log_matches_valkey_wording() {
        assert_eq!(
            replication_buffer_limit_message(16_383),
            "Replication buffer limit reached (16383 bytes), stopping buffering. Further accumulation will occur on primary side."
        );
    }
}
