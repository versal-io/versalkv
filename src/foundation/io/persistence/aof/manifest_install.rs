use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, PartialEq, Eq)]
enum ManifestInstallState {
    Prepared,
    Published,
    Durable,
    Finalized,
}

pub struct PreparedSyncedRdbAofBase {
    app_dir: PathBuf,
    manifest_path: PathBuf,
    temp_manifest: PathBuf,
    base_path: PathBuf,
    incr_path: PathBuf,
    old_entries: Vec<super::manifest::AofManifestEntry>,
    old_manifest: Option<Vec<u8>>,
    state: ManifestInstallState,
}

impl PreparedSyncedRdbAofBase {
    pub fn incremental_path(&self) -> &Path {
        &self.incr_path
    }

    pub fn commit_manifest(&mut self) -> std::io::Result<()> {
        self.commit_manifest_with_sync(sync_directory)
    }

    fn commit_manifest_with_sync(
        &mut self,
        sync: impl FnOnce(&Path) -> io::Result<()>,
    ) -> io::Result<()> {
        fs::rename(&self.temp_manifest, &self.manifest_path)?;
        // Record publication before the fallible durability barrier so Drop
        // restores the previous manifest if the directory sync fails.
        self.state = ManifestInstallState::Published;
        sync(&self.app_dir)?;
        self.state = ManifestInstallState::Durable;
        Ok(())
    }

    pub fn finalize(mut self) {
        if self.state != ManifestInstallState::Durable {
            return;
        }
        self.state = ManifestInstallState::Finalized;
        for entry in &self.old_entries {
            let old_path = self.app_dir.join(&entry.filename);
            if old_path != self.base_path && old_path != self.incr_path {
                let _ = std::fs::remove_file(old_path);
            }
        }
    }

    fn rollback_manifest(&self) -> io::Result<()> {
        match &self.old_manifest {
            Some(content) => {
                let rollback = self.manifest_path.with_extension("manifest.rollback.tmp");
                let mut file = File::create(&rollback)?;
                file.write_all(content)?;
                file.sync_all()?;
                fs::rename(rollback, &self.manifest_path)?;
            }
            None => {
                fs::remove_file(&self.manifest_path)?;
            }
        }
        sync_directory(&self.app_dir)
    }
}

impl Drop for PreparedSyncedRdbAofBase {
    fn drop(&mut self) {
        if self.state == ManifestInstallState::Finalized {
            return;
        }
        if self.state != ManifestInstallState::Prepared {
            if let Err(error) = self.rollback_manifest() {
                // Either manifest may survive a crash. Preserve both sets of
                // data files until recovery can establish which one is active.
                log::warn!("Failed to roll back AOF manifest; retaining data files: {error}");
                return;
            }
        }
        let _ = std::fs::remove_file(&self.temp_manifest);
        let _ = std::fs::remove_file(&self.base_path);
        let _ = std::fs::remove_file(&self.incr_path);
    }
}

fn sync_directory(directory: &Path) -> io::Result<()> {
    File::open(directory)?.sync_all()
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
        state: ManifestInstallState::Prepared,
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io;
    use std::path::PathBuf;

    use super::PreparedSyncedRdbAofBase;

    const OLD_MANIFEST: &str = "file appendonly.aof.1.base.rdb seq 1 type b\n\
                               file appendonly.aof.1.incr.aof seq 1 type i\n";

    fn prepare_fixture(label: &str) -> (PathBuf, PreparedSyncedRdbAofBase) {
        let directory = std::env::temp_dir().join(format!(
            "versal-manifest-{label}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let app_dir = directory.join("appendonlydir");
        fs::create_dir_all(&app_dir).expect("create AOF directory");
        fs::write(app_dir.join("appendonly.aof.1.base.rdb"), b"old base").expect("write old base");
        fs::write(app_dir.join("appendonly.aof.1.incr.aof"), b"old increment")
            .expect("write old increment");
        fs::write(app_dir.join("appendonly.aof.manifest"), OLD_MANIFEST)
            .expect("write old manifest");
        let prepared = super::prepare_synced_rdb_as_aof_base(
            directory.to_str().expect("UTF-8 fixture path"),
            "appendonly.aof",
            "appendonlydir",
            b"new base",
        )
        .expect("prepare new AOF files");
        (directory, prepared)
    }

    #[test]
    fn directory_sync_follows_publication_and_precedes_old_file_removal() {
        let (directory, mut prepared) = prepare_fixture("sync-order");
        prepared
            .commit_manifest_with_sync(|app_dir| {
                let manifest = fs::read_to_string(app_dir.join("appendonly.aof.manifest"))?;
                assert!(manifest.contains("appendonly.aof.2.base.rdb"));
                assert!(app_dir.join("appendonly.aof.1.base.rdb").exists());
                assert!(app_dir.join("appendonly.aof.1.incr.aof").exists());
                super::sync_directory(app_dir)
            })
            .expect("commit durable manifest");
        prepared.finalize();
        assert!(!directory
            .join("appendonlydir/appendonly.aof.1.base.rdb")
            .exists());
        fs::remove_dir_all(directory).expect("remove fixture");
    }

    #[test]
    fn failed_directory_sync_restores_the_previous_recovery_point() {
        let (directory, mut prepared) = prepare_fixture("sync-error");
        let error = prepared
            .commit_manifest_with_sync(|_| Err(io::Error::other("injected directory sync failure")))
            .expect_err("a failed durability barrier must fail the commit");
        assert_eq!(error.kind(), io::ErrorKind::Other);
        // Even an accidental finalize after a failed commit must keep old data.
        prepared.finalize();
        let app_dir = directory.join("appendonlydir");
        assert_eq!(
            fs::read_to_string(app_dir.join("appendonly.aof.manifest")).expect("read manifest"),
            OLD_MANIFEST
        );
        assert!(app_dir.join("appendonly.aof.1.base.rdb").exists());
        assert!(app_dir.join("appendonly.aof.1.incr.aof").exists());
        assert!(!app_dir.join("appendonly.aof.2.base.rdb").exists());
        assert!(!app_dir.join("appendonly.aof.2.incr.aof").exists());
        fs::remove_dir_all(directory).expect("remove fixture");
    }

    #[test]
    fn failed_rollback_keeps_files_referenced_by_the_published_manifest() {
        let (directory, mut prepared) = prepare_fixture("rollback-error");
        prepared.commit_manifest().expect("publish new manifest");
        let rollback = prepared
            .manifest_path
            .with_extension("manifest.rollback.tmp");
        fs::create_dir(rollback).expect("prevent rollback file creation");
        drop(prepared);
        let app_dir = directory.join("appendonlydir");
        assert!(fs::read_to_string(app_dir.join("appendonly.aof.manifest"))
            .expect("read manifest")
            .contains("appendonly.aof.2.base.rdb"));
        for filename in [
            "appendonly.aof.1.base.rdb",
            "appendonly.aof.1.incr.aof",
            "appendonly.aof.2.base.rdb",
            "appendonly.aof.2.incr.aof",
        ] {
            assert!(
                app_dir.join(filename).exists(),
                "lost recovery file {filename}"
            );
        }
        fs::remove_dir_all(directory).expect("remove fixture");
    }
}
