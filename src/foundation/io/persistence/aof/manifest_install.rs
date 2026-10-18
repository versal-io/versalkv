use std::io::Write;
use std::path::{Path, PathBuf};

pub struct PreparedSyncedRdbAofBase {
    app_dir: PathBuf,
    manifest_path: PathBuf,
    temp_manifest: PathBuf,
    base_path: PathBuf,
    incr_path: PathBuf,
    old_entries: Vec<super::manifest::AofManifestEntry>,
    old_manifest: Option<Vec<u8>>,
    manifest_committed: bool,
    finalized: bool,
}

impl PreparedSyncedRdbAofBase {
    pub fn incremental_path(&self) -> &Path {
        &self.incr_path
    }

    pub fn commit_manifest(&mut self) -> std::io::Result<()> {
        std::fs::rename(&self.temp_manifest, &self.manifest_path)?;
        self.manifest_committed = true;
        Ok(())
    }

    pub fn finalize(mut self) {
        self.finalized = true;
        for entry in &self.old_entries {
            let old_path = self.app_dir.join(&entry.filename);
            if old_path != self.base_path && old_path != self.incr_path {
                let _ = std::fs::remove_file(old_path);
            }
        }
    }

    fn rollback_manifest(&self) {
        match &self.old_manifest {
            Some(content) => {
                let rollback = self.manifest_path.with_extension("manifest.rollback.tmp");
                if std::fs::write(&rollback, content).is_ok() {
                    let _ = std::fs::rename(rollback, &self.manifest_path);
                }
            }
            None => {
                let _ = std::fs::remove_file(&self.manifest_path);
            }
        }
    }
}

impl Drop for PreparedSyncedRdbAofBase {
    fn drop(&mut self) {
        if self.finalized {
            return;
        }
        if self.manifest_committed {
            self.rollback_manifest();
        }
        let _ = std::fs::remove_file(&self.temp_manifest);
        let _ = std::fs::remove_file(&self.base_path);
        let _ = std::fs::remove_file(&self.incr_path);
    }
}

pub fn prepare_synced_rdb_as_aof_base(
    dir: &str,
    filename: &str,
    appenddirname: &str,
    rdb: &[u8],
) -> std::io::Result<PreparedSyncedRdbAofBase> {
    let app_dir = Path::new(dir).join(appenddirname);
    std::fs::create_dir_all(&app_dir)?;
    let manifest_path = app_dir.join(format!("{}.manifest", filename));
    let old_manifest = match std::fs::read(&manifest_path) {
        Ok(content) => Some(content),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    };
    let old_entries = super::manifest::read_manifest_entries(&manifest_path).unwrap_or_default();
    let (base_sequence, incr_sequence) =
        super::manifest::next_rewrite_sequences(&manifest_path, false)?;
    let base_name = format!("{}.{}.base.rdb", filename, base_sequence);
    let incr_name = format!("{}.{}.incr.aof", filename, incr_sequence);
    let base_path = app_dir.join(&base_name);
    let incr_path = app_dir.join(&incr_name);
    let temp_manifest = app_dir.join(format!("{}.manifest.sync.tmp", filename));
    let mut prepared = PreparedSyncedRdbAofBase {
        app_dir,
        manifest_path,
        temp_manifest,
        base_path,
        incr_path,
        old_entries,
        old_manifest,
        manifest_committed: false,
        finalized: false,
    };
    write_prepared_files(&mut prepared, base_sequence, incr_sequence, rdb)?;
    Ok(prepared)
}

fn write_prepared_files(
    prepared: &mut PreparedSyncedRdbAofBase,
    base_sequence: u64,
    incr_sequence: u64,
    rdb: &[u8],
) -> std::io::Result<()> {
    let _ = std::fs::remove_file(&prepared.base_path);
    let _ = std::fs::remove_file(&prepared.incr_path);
    let _ = std::fs::remove_file(&prepared.temp_manifest);
    let mut base = std::fs::File::create(&prepared.base_path)?;
    base.write_all(rdb)?;
    base.sync_all()?;
    std::fs::File::create(&prepared.incr_path)?.sync_all()?;
    let base_name = prepared.base_path.file_name().unwrap().to_string_lossy();
    let incr_name = prepared.incr_path.file_name().unwrap().to_string_lossy();
    let manifest = format!(
        "file {} seq {} type b\nfile {} seq {} type i\n",
        super::manifest::format_manifest_token(&base_name),
        base_sequence,
        super::manifest::format_manifest_token(&incr_name),
        incr_sequence
    );
    let mut file = std::fs::File::create(&prepared.temp_manifest)?;
    file.write_all(manifest.as_bytes())?;
    file.sync_all()?;
    std::fs::File::open(&prepared.app_dir)?.sync_all()?;
    Ok(())
}

pub fn install_synced_rdb_as_aof_base(
    dir: &str,
    filename: &str,
    appenddirname: &str,
    rdb: &[u8],
) -> std::io::Result<(PathBuf, PathBuf)> {
    let mut prepared = prepare_synced_rdb_as_aof_base(dir, filename, appenddirname, rdb)?;
    let paths = (prepared.base_path.clone(), prepared.incr_path.clone());
    prepared.commit_manifest()?;
    prepared.finalize();
    Ok(paths)
}
