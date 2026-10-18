use super::super::Snapshot;
use crate::foundation::io::store::kv_engine::DbSet;
use std::io::Write;
use std::path::Path;

pub struct RdbSnapshot;

/// C Valkey's RIO autosync threshold used by both RDB save and AOF rewrite.
pub const RDB_AUTOSYNC_BYTES: usize = 32 * 1024 * 1024;

fn write_with_incremental_sync<W, F>(
    writer: &mut W,
    bytes: &[u8],
    incremental_sync: bool,
    mut sync: F,
) -> std::io::Result<()>
where
    W: Write,
    F: FnMut(&mut W) -> std::io::Result<()>,
{
    if !incremental_sync {
        return writer.write_all(bytes);
    }
    for chunk in bytes.chunks(RDB_AUTOSYNC_BYTES) {
        writer.write_all(chunk)?;
        if chunk.len() == RDB_AUTOSYNC_BYTES {
            sync(writer)?;
        }
    }
    Ok(())
}

pub fn save_rdb_atomically(
    dbs: &DbSet,
    dir: &Path,
    filename: &str,
    key_delay_us: i64,
) -> std::io::Result<()> {
    save_rdb_atomically_with_functions(dbs, dir, filename, key_delay_us, &[])
}

pub fn save_rdb_atomically_with_functions(
    dbs: &DbSet,
    dir: &Path,
    filename: &str,
    key_delay_us: i64,
    function_codes: &[Vec<u8>],
) -> std::io::Result<()> {
    save_rdb_atomically_with_functions_and_replication(
        dbs,
        dir,
        filename,
        key_delay_us,
        function_codes,
        None,
        &[],
    )
}

pub fn save_rdb_atomically_with_functions_and_replication(
    dbs: &DbSet,
    dir: &Path,
    filename: &str,
    key_delay_us: i64,
    function_codes: &[Vec<u8>],
    replication: Option<&crate::foundation::io::persistence::RdbReplicationInfo>,
    additional_datasets: &[&DbSet],
) -> std::io::Result<()> {
    save_rdb_atomically_with_functions_and_replication_and_incremental_fsync(
        dbs,
        dir,
        filename,
        key_delay_us,
        function_codes,
        replication,
        additional_datasets,
        true,
    )
}

pub fn save_rdb_atomically_with_functions_and_replication_and_incremental_fsync(
    dbs: &DbSet,
    dir: &Path,
    filename: &str,
    key_delay_us: i64,
    function_codes: &[Vec<u8>],
    replication: Option<&crate::foundation::io::persistence::RdbReplicationInfo>,
    additional_datasets: &[&DbSet],
    incremental_fsync: bool,
) -> std::io::Result<()> {
    save_rdb_atomically_with_functions_and_replication_and_incremental_fsync_and_checksum(
        dbs,
        dir,
        filename,
        key_delay_us,
        function_codes,
        replication,
        additional_datasets,
        incremental_fsync,
        true,
    )
}

pub fn save_rdb_atomically_with_functions_and_replication_and_incremental_fsync_and_checksum(
    dbs: &DbSet,
    dir: &Path,
    filename: &str,
    key_delay_us: i64,
    function_codes: &[Vec<u8>],
    replication: Option<&crate::foundation::io::persistence::RdbReplicationInfo>,
    additional_datasets: &[&DbSet],
    incremental_fsync: bool,
    checksum: bool,
) -> std::io::Result<()> {
    save_rdb_atomically_with_functions_and_replication_and_incremental_fsync_with_options(
        dbs,
        dir,
        filename,
        key_delay_us,
        function_codes,
        replication,
        additional_datasets,
        incremental_fsync,
        crate::foundation::io::persistence::RdbSerializeOptions {
            compression: true,
            checksum,
        },
    )
}

pub fn save_rdb_atomically_with_functions_and_replication_and_incremental_fsync_with_options(
    dbs: &DbSet,
    dir: &Path,
    filename: &str,
    key_delay_us: i64,
    function_codes: &[Vec<u8>],
    replication: Option<&crate::foundation::io::persistence::RdbReplicationInfo>,
    additional_datasets: &[&DbSet],
    incremental_fsync: bool,
    serialization: crate::foundation::io::persistence::RdbSerializeOptions,
) -> std::io::Result<()> {
    let path = dir.join(filename);
    let tmp = dir.join(format!("temp-{}.rdb", std::process::id()));
    let result = (|| {
        let mut file = std::fs::File::create(&tmp)?;
        let bytes = crate::foundation::io::persistence::serialize_all_with_functions_and_replication_with_options(
            dbs,
            key_delay_us,
            function_codes,
            true,
            replication,
            additional_datasets,
            serialization,
        );
        write_with_incremental_sync(&mut file, &bytes, incremental_fsync, |file| {
            file.sync_data()
        })?;
        file.sync_all()?;
        std::fs::rename(&tmp, path)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(tmp);
    }
    result
}

impl Snapshot for RdbSnapshot {
    type Error = crate::foundation::io::persistence::LoadError;

    fn save(&mut self, dbs: &DbSet) -> Result<Vec<u8>, Self::Error> {
        Ok(crate::foundation::io::persistence::serialize_all(dbs, 0))
    }

    fn load(&mut self, data: &[u8], dbs: &mut DbSet) -> Result<(usize, usize), Self::Error> {
        crate::foundation::io::persistence::load_rdb_bytes_checked(data, dbs, true)
    }

    fn save_to_file(&mut self, path: &str, dbs: &DbSet) -> Result<(), Self::Error> {
        let bytes = self.save(dbs)?;
        std::fs::write(path, bytes).map_err(crate::foundation::io::persistence::LoadError::Io)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::io::persistence::Snapshot;
    use crate::foundation::io::store::kv_engine::DbSet;
    use crate::foundation::io::store::Robj;

    #[test]
    fn rdb_snapshot_saves_loads_and_writes_files() {
        let mut source = DbSet::new(2);
        source
            .get_mut(1)
            .unwrap()
            .set(b"k".to_vec(), Robj::string("v"));
        let mut snapshot = RdbSnapshot;

        let bytes = snapshot.save(&source).unwrap();
        let mut restored = DbSet::new(2);
        let (_loaded, _consumed) = snapshot.load(&bytes, &mut restored).unwrap();

        assert_eq!(
            restored
                .get_mut(1)
                .unwrap()
                .get(b"k")
                .map(|obj| obj.as_bytes())
                .unwrap(),
            b"v"
        );

        let path =
            std::env::temp_dir().join(format!("valkey-rdb-snapshot-{}.rdb", std::process::id()));
        snapshot
            .save_to_file(path.to_str().unwrap(), &source)
            .unwrap();
        assert!(std::fs::metadata(&path).unwrap().len() > 0);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn incremental_sync_runs_at_each_completed_autosync_interval() {
        let mut output = Vec::new();
        let mut syncs = 0usize;
        let payload = vec![0u8; RDB_AUTOSYNC_BYTES * 2 + 1];

        write_with_incremental_sync(&mut output, &payload, true, |_| {
            syncs += 1;
            Ok(())
        })
        .expect("incremental writer succeeds");

        assert_eq!(output, payload);
        assert_eq!(syncs, 2);
    }

    #[test]
    fn disabled_incremental_sync_never_calls_sync_callback() {
        let mut output = Vec::new();
        let mut syncs = 0usize;
        let payload = vec![0u8; RDB_AUTOSYNC_BYTES + 1];

        write_with_incremental_sync(&mut output, &payload, false, |_| {
            syncs += 1;
            Ok(())
        })
        .expect("non-incremental writer succeeds");

        assert_eq!(output, payload);
        assert_eq!(syncs, 0);
    }
}
