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
    is_single_file: bool,
    single_backup: Option<PathBuf>,
}

impl PreparedSyncedRdbAofBase {
    pub fn incremental_path(&self) -> &Path {
        &self.incr_path
    }

    pub fn installed_incremental_path(&self) -> &Path {
        if self.is_single_file {
            &self.manifest_path
        } else {
            &self.incr_path
        }
    }

    pub fn commit_manifest(&mut self) -> std::io::Result<()> {
        std::fs::rename(&self.temp_manifest, &self.manifest_path)?;
        self.manifest_committed = true;
        super::manifest::sync_directory(&self.app_dir)
    }

    pub fn finalize(mut self) {
        self.finalized = true;
        if let Some(backup) = &self.single_backup {
            let _ = std::fs::remove_file(backup);
        }
        for entry in &self.old_entries {
            let old_path = self.app_dir.join(&entry.filename);
            if old_path != self.base_path && old_path != self.incr_path {
                let _ = std::fs::remove_file(old_path);
            }
        }
    }

    pub fn rollback(mut self) -> std::io::Result<()> {
        if self.manifest_committed {
            if let Err(error) = self.rollback_manifest() {
                // Keep both possible baselines; the owner must invalidate its durability claim.
                self.finalized = true;
                return Err(error);
            }
            self.manifest_committed = false;
        }
        Ok(())
    }

    fn rollback_manifest(&self) -> std::io::Result<()> {
        if self.is_single_file {
            match &self.single_backup {
                Some(backup) => std::fs::rename(backup, &self.manifest_path)?,
                None => std::fs::remove_file(&self.manifest_path)?,
            }
            return super::manifest::sync_directory(&self.app_dir);
        }
        match &self.old_manifest {
            Some(content) => {
                super::manifest::write_manifest_atomically(&self.manifest_path, content)
            }
            None => {
                std::fs::remove_file(&self.manifest_path)?;
                super::manifest::sync_directory(&self.app_dir)
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
            if let Err(error) = self.rollback_manifest() {
                // A failed rollback may leave either manifest active. Keep both baselines.
                log::error!("Failed to roll back AOF manifest; retaining prepared files: {error}");
                return;
            }
        }
        let _ = std::fs::remove_file(&self.temp_manifest);
        let _ = std::fs::remove_file(&self.base_path);
        let _ = std::fs::remove_file(&self.incr_path);
        if let Some(backup) = &self.single_backup {
            let _ = std::fs::remove_file(backup);
        }
    }
}

pub fn prepare_synced_rdb_as_aof_base(
    dir: &str,
    filename: &str,
    appenddirname: &str,
    rdb: &[u8],
) -> std::io::Result<PreparedSyncedRdbAofBase> {
    let app_dir = Path::new(dir).join(appenddirname);
    super::manifest::create_aof_directory(&app_dir)?;
    if appenddirname.is_empty() {
        return prepare_single_file_base(app_dir, filename, rdb);
    }
    let manifest_path = app_dir.join(format!("{}.manifest", filename));
    let old_manifest = match std::fs::read(&manifest_path) {
        Ok(content) => Some(content),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    };
    let old_entries = old_manifest
        .as_deref()
        .map(super::manifest::parse_manifest_bytes)
        .transpose()?
        .unwrap_or_default();
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
        is_single_file: false,
        single_backup: None,
    };
    write_prepared_files(&mut prepared, base_sequence, incr_sequence, rdb)?;
    Ok(prepared)
}

fn prepare_single_file_base(
    app_dir: PathBuf,
    filename: &str,
    rdb: &[u8],
) -> std::io::Result<PreparedSyncedRdbAofBase> {
    let target = app_dir.join(filename);
    let staged = app_dir.join(format!(".{filename}.sync.tmp"));
    let mut prepared = PreparedSyncedRdbAofBase {
        app_dir,
        manifest_path: target,
        temp_manifest: staged.clone(),
        base_path: staged.clone(),
        incr_path: staged,
        old_entries: Vec::new(),
        old_manifest: None,
        manifest_committed: false,
        finalized: false,
        is_single_file: true,
        single_backup: None,
    };
    let mut file = std::fs::File::create(&prepared.temp_manifest)?;
    file.write_all(rdb)?;
    file.sync_all()?;
    if prepared.manifest_path.try_exists()? {
        std::fs::File::open(&prepared.manifest_path)?.sync_all()?;
        prepared.single_backup = Some(create_single_file_backup(
            &prepared.manifest_path,
            &prepared.app_dir,
            filename,
        )?);
    }
    super::manifest::sync_directory(&prepared.app_dir)?;
    Ok(prepared)
}

fn create_single_file_backup(
    target: &Path,
    app_dir: &Path,
    filename: &str,
) -> std::io::Result<PathBuf> {
    static NEXT_BACKUP: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    loop {
        let sequence = NEXT_BACKUP.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let backup = app_dir.join(format!(
            ".{filename}.sync.previous.{}.{}",
            std::process::id(),
            sequence,
        ));
        // A crash or PID reuse may leave this name occupied. Never overwrite or
        // remove an unknown backup, which may be the only surviving old baseline.
        match std::fs::hard_link(target, &backup) {
            Ok(()) => return Ok(backup),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
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
    super::manifest::sync_directory(&prepared.app_dir)?;
    Ok(())
}

pub fn install_synced_rdb_as_aof_base(
    dir: &str,
    filename: &str,
    appenddirname: &str,
    rdb: &[u8],
) -> std::io::Result<(PathBuf, PathBuf)> {
    let mut prepared = prepare_synced_rdb_as_aof_base(dir, filename, appenddirname, rdb)?;
    if let Err(install_error) = prepared.commit_manifest() {
        return match prepared.rollback() {
            Ok(()) => Err(install_error),
            Err(rollback_error) => Err(std::io::Error::new(
                rollback_error.kind(),
                format!(
                    "AOF installation failed: {install_error}; rollback failed: {rollback_error}"
                ),
            )),
        };
    }
    let paths = if prepared.is_single_file {
        (
            prepared.manifest_path.clone(),
            prepared.manifest_path.clone(),
        )
    } else {
        (prepared.base_path.clone(), prepared.incr_path.clone())
    };
    prepared.finalize();
    Ok(paths)
}
