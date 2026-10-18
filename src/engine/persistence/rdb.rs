use super::{OperationStatus, PersistenceState, RdbSaveOptions};
use crate::engine::extension::config::persistence::{SaveParam, BGSAVE_RETRY_DELAY_SEC};
use crate::foundation::io::store::kv_engine::DbSet;

pub fn save_with_replication(
    dbs: &DbSet,
    additional_datasets: &[&DbSet],
    options: &RdbSaveOptions,
    replication: Option<&crate::foundation::io::persistence::RdbReplicationInfo>,
) -> Result<(), ()> {
    crate::foundation::io::persistence::snapshot::save_rdb_atomically_with_functions_and_replication_and_incremental_fsync_with_options(
        dbs,
        &options.dir,
        &options.filename,
        options.key_delay_us,
        &[],
        replication,
        additional_datasets,
        options.incremental_fsync,
        crate::foundation::io::persistence::RdbSerializeOptions {
            compression: options.compression,
            checksum: options.checksum,
        },
    )
    .map_err(|_| ())
}

pub fn save_with_functions_and_replication(
    dbs: &DbSet,
    additional_datasets: &[&DbSet],
    options: &RdbSaveOptions,
    function_codes: &[Vec<u8>],
    replication: Option<&crate::foundation::io::persistence::RdbReplicationInfo>,
) -> Result<(), ()> {
    crate::foundation::io::persistence::snapshot::save_rdb_atomically_with_functions_and_replication_and_incremental_fsync_with_options(
        dbs,
        &options.dir,
        &options.filename,
        options.key_delay_us,
        function_codes,
        replication,
        additional_datasets,
        options.incremental_fsync,
        crate::foundation::io::persistence::RdbSerializeOptions {
            compression: options.compression,
            checksum: options.checksum,
        },
    )
    .map_err(|_| ())
}

pub fn should_auto_bgsave(
    state: &PersistenceState,
    saveparams: &[SaveParam],
    now_sec: i64,
) -> bool {
    if state.has_active_child() {
        return false;
    }
    saveparams.iter().any(|saveparam| {
        state.dirty() >= saveparam.changes
            && now_sec - state.rdb().last_save_time_sec > saveparam.seconds
            && (state.rdb().last_bgsave_status == OperationStatus::Ok
                || now_sec - state.rdb().last_bgsave_try_sec > BGSAVE_RETRY_DELAY_SEC)
    })
}

#[cfg(test)]
mod tests {
    use super::save_with_replication;
    use crate::engine::persistence::RdbSaveOptions;
    use crate::foundation::io::store::kv_engine::DbSet;
    use crate::foundation::io::store::Robj;

    #[test]
    fn snapshot_save_honors_disabled_checksum_option() {
        let mut source = DbSet::new(1);
        source
            .get_mut(0)
            .expect("DB 0 exists")
            .set(b"key".to_vec(), Robj::string("value"));
        let dir = std::env::temp_dir().join(format!(
            "versal-rdb-checksum-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos()
        ));
        std::fs::create_dir(&dir).expect("create temporary snapshot directory");
        let options = RdbSaveOptions {
            dir: dir.clone(),
            filename: "dump.rdb".to_string(),
            key_delay_us: 0,
            compression: true,
            checksum: false,
            incremental_fsync: false,
        };

        save_with_replication(&source, &[], &options, None).expect("save snapshot");

        let bytes = std::fs::read(dir.join(&options.filename)).expect("read snapshot");
        assert_eq!(&bytes[bytes.len() - 8..], &[0; 8]);
        std::fs::remove_dir_all(dir).expect("remove temporary snapshot directory");
    }
}
