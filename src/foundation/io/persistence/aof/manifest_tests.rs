use super::manifest::*;
use super::manifest_install::{install_synced_rdb_as_aof_base, prepare_synced_rdb_as_aof_base};
use std::path::PathBuf;

fn temp_dir(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("versal-aof-manifest-{name}-{}", std::process::id()))
}

#[test]
fn rejects_invalid_manifests() {
    let cases = [
        (
            "missing-file",
            "file missing.aof seq 1 type i\n",
            "missing.aof",
        ),
        (
            "non-monotonic",
            "file second.aof seq 2 type i\nfile first.aof seq 1 type i\n",
            "Found a non-monotonic sequence number",
        ),
        (
            "blank-line",
            "file first.aof seq 1 type i\n\nfile third.aof seq 3 type i\n",
            "Invalid AOF manifest file format",
        ),
        (
            "duplicate-base",
            "file first.aof seq 1 type b\nfile second.aof seq 2 type b\n",
            "Found duplicate base file information",
        ),
        (
            "unknown-type",
            "file first.aof seq 1 type x\n",
            "Unknown AOF file type",
        ),
        (
            "missing-key",
            "filx first.aof seq 1 type i\n",
            "Invalid AOF manifest file format",
        ),
        (
            "odd-field",
            "file first.aof seq 1 type i extra\n",
            "Invalid AOF manifest file format",
        ),
        (
            "path-traversal",
            "file ../../outside.aof seq 1 type h\n",
            "Invalid AOF manifest file format",
        ),
        ("empty", "", "Found an empty AOF manifest"),
    ];
    for (name, manifest, expected) in cases {
        let dir = temp_dir(name);
        let app_dir = dir.join("appendonlydir");
        std::fs::create_dir_all(&app_dir).unwrap();
        std::fs::write(app_dir.join("appendonly.aof.manifest"), manifest).unwrap();
        for filename in ["first.aof", "second.aof", "third.aof"] {
            std::fs::write(app_dir.join(filename), b"").unwrap();
        }
        let error = resolve_aof_read_paths_checked(
            dir.to_str().unwrap(),
            "appendonly.aof",
            "appendonlydir",
        )
        .unwrap_err();
        assert!(error.to_string().contains(expected), "{name}: {error}");
        let _ = std::fs::remove_dir_all(dir);
    }
}

#[test]
fn upgrades_legacy_aof_and_supports_quoted_filenames() {
    let dir = temp_dir("legacy");
    std::fs::create_dir_all(&dir).unwrap();
    let filename = " appendonly\n.aof ";
    std::fs::write(dir.join(filename), b"*1\r\n$4\r\nPING\r\n").unwrap();

    let paths =
        resolve_aof_read_paths_checked(dir.to_str().unwrap(), filename, "appendonlydir").unwrap();

    assert_eq!(paths.len(), 2);
    assert_eq!(std::fs::read(&paths[0]).unwrap(), b"*1\r\n$4\r\nPING\r\n");
    let manifest = std::fs::read_to_string(
        dir.join("appendonlydir")
            .join(format!("{filename}.manifest")),
    )
    .unwrap();
    assert!(manifest.contains("\\n"));
    assert!(!dir.join(filename).exists());
    assert_eq!(
        next_rewrite_sequences(
            &dir.join("appendonlydir")
                .join(format!("{filename}.manifest")),
            false,
        )
        .unwrap(),
        (2, 2)
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn rotating_increment_appends_the_next_sequence() {
    let dir = temp_dir("rotate");
    let app_dir = dir.join("appendonlydir");
    std::fs::create_dir_all(&app_dir).unwrap();
    std::fs::write(app_dir.join("appendonly.aof.1.base.rdb"), b"base").unwrap();
    std::fs::write(app_dir.join("appendonly.aof.1.incr.aof"), b"first").unwrap();
    std::fs::write(
        app_dir.join("appendonly.aof.manifest"),
        "file appendonly.aof.1.base.rdb seq 1 type b\n\
         file appendonly.aof.1.incr.aof seq 1 type i\n",
    )
    .unwrap();

    let path =
        rotate_aof_increment(dir.to_str().unwrap(), "appendonly.aof", "appendonlydir").unwrap();

    assert_eq!(path, app_dir.join("appendonly.aof.2.incr.aof"));
    assert!(path.exists());
    assert!(
        std::fs::read_to_string(app_dir.join("appendonly.aof.manifest"))
            .unwrap()
            .ends_with("file appendonly.aof.2.incr.aof seq 2 type i\n")
    );
    assert_eq!(
        next_rewrite_sequences(&app_dir.join("appendonly.aof.manifest"), true).unwrap(),
        (2, 2)
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn resolving_base_only_manifest_is_read_only_until_increment_is_ensured() {
    let dir = temp_dir("base-only");
    let app_dir = dir.join("appendonlydir");
    std::fs::create_dir_all(&app_dir).unwrap();
    std::fs::write(app_dir.join("appendonly.aof.4.base.rdb"), b"base").unwrap();
    let manifest_path = app_dir.join("appendonly.aof.manifest");
    let original = "file appendonly.aof.4.base.rdb seq 4 type b\n";
    std::fs::write(&manifest_path, original).unwrap();

    let paths =
        resolve_aof_read_paths_checked(dir.to_str().unwrap(), "appendonly.aof", "appendonlydir")
            .unwrap();
    assert_eq!(paths, vec![app_dir.join("appendonly.aof.4.base.rdb")]);
    assert_eq!(std::fs::read_to_string(&manifest_path).unwrap(), original);

    ensure_aof_increment(dir.to_str().unwrap(), "appendonly.aof", "appendonlydir").unwrap();
    assert!(app_dir.join("appendonly.aof.1.incr.aof").exists());
    assert!(std::fs::read_to_string(manifest_path)
        .unwrap()
        .ends_with("file appendonly.aof.1.incr.aof seq 1 type i\n"));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn checked_read_paths_include_base_and_increment_with_independent_sequences() {
    let dir = temp_dir("checked-base-incr-same-seq");
    let app_dir = dir.join("appendonlydir");
    std::fs::create_dir_all(&app_dir).unwrap();
    std::fs::write(
        app_dir.join("appendonly.aof.manifest"),
        "file appendonly.aof.1.base.rdb seq 1 type b\n\
         file appendonly.aof.1.incr.aof seq 1 type i\n",
    )
    .unwrap();
    std::fs::write(app_dir.join("appendonly.aof.1.base.rdb"), b"base").unwrap();
    std::fs::write(app_dir.join("appendonly.aof.1.incr.aof"), b"incr").unwrap();

    let paths =
        resolve_aof_read_paths_checked(dir.to_str().unwrap(), "appendonly.aof", "appendonlydir")
            .unwrap();

    assert_eq!(
        paths,
        vec![
            app_dir.join("appendonly.aof.1.base.rdb"),
            app_dir.join("appendonly.aof.1.incr.aof"),
        ]
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn synced_rdb_replaces_active_aof_files() {
    let dir = temp_dir("synced-rdb");
    let app_dir = dir.join("appendonlydir");
    std::fs::create_dir_all(&app_dir).unwrap();
    std::fs::write(app_dir.join("appendonly.aof.1.base.rdb"), b"old").unwrap();
    std::fs::write(app_dir.join("appendonly.aof.1.incr.aof"), b"tail").unwrap();
    std::fs::write(
        app_dir.join("appendonly.aof.manifest"),
        "file appendonly.aof.1.base.rdb seq 1 type b\n\
         file appendonly.aof.1.incr.aof seq 1 type i\n",
    )
    .unwrap();

    let (base, incr) = install_synced_rdb_as_aof_base(
        dir.to_str().unwrap(),
        "appendonly.aof",
        "appendonlydir",
        b"VALKEY-snapshot",
    )
    .unwrap();

    assert_eq!(std::fs::read(base).unwrap(), b"VALKEY-snapshot");
    assert_eq!(std::fs::read(incr).unwrap(), b"");
    assert!(!app_dir.join("appendonly.aof.1.base.rdb").exists());
    assert!(!app_dir.join("appendonly.aof.1.incr.aof").exists());
    assert_eq!(
        std::fs::read_to_string(app_dir.join("appendonly.aof.manifest")).unwrap(),
        "file appendonly.aof.2.base.rdb seq 2 type b\n\
         file appendonly.aof.2.incr.aof seq 2 type i\n"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn prepared_synced_rdb_manifest_rolls_back_until_finalized() {
    let dir = temp_dir("synced-rdb-rollback");
    let app_dir = dir.join("appendonlydir");
    std::fs::create_dir_all(&app_dir).unwrap();
    let old_manifest = "file appendonly.aof.1.base.rdb seq 1 type b\n\
                        file appendonly.aof.1.incr.aof seq 1 type i\n";
    std::fs::write(app_dir.join("appendonly.aof.1.base.rdb"), b"old").unwrap();
    std::fs::write(app_dir.join("appendonly.aof.1.incr.aof"), b"tail").unwrap();
    std::fs::write(app_dir.join("appendonly.aof.manifest"), old_manifest).unwrap();

    let mut prepared = prepare_synced_rdb_as_aof_base(
        dir.to_str().unwrap(),
        "appendonly.aof",
        "appendonlydir",
        b"new-rdb",
    )
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(app_dir.join("appendonly.aof.manifest")).unwrap(),
        old_manifest
    );
    prepared.commit_manifest().unwrap();
    assert_ne!(
        std::fs::read_to_string(app_dir.join("appendonly.aof.manifest")).unwrap(),
        old_manifest
    );

    drop(prepared);

    assert_eq!(
        std::fs::read_to_string(app_dir.join("appendonly.aof.manifest")).unwrap(),
        old_manifest
    );
    assert!(app_dir.join("appendonly.aof.1.base.rdb").exists());
    assert!(app_dir.join("appendonly.aof.1.incr.aof").exists());
    assert!(!app_dir.join("appendonly.aof.2.base.rdb").exists());
    assert!(!app_dir.join("appendonly.aof.2.incr.aof").exists());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn rotation_replaces_manifest_without_mutating_open_old_manifest() {
    use std::io::Read;
    let dir = temp_dir("atomic-rotation");
    let app_dir = dir.join("appendonlydir");
    std::fs::create_dir_all(&app_dir).unwrap();
    let manifest_path = app_dir.join("appendonly.aof.manifest");
    let original = "file appendonly.aof.1.incr.aof seq 1 type i\n";
    std::fs::write(&manifest_path, original).unwrap();
    std::fs::write(app_dir.join("appendonly.aof.1.incr.aof"), b"tail").unwrap();
    let mut old_manifest = std::fs::File::open(&manifest_path).unwrap();

    rotate_aof_increment(dir.to_str().unwrap(), "appendonly.aof", "appendonlydir").unwrap();

    let mut old_content = String::new();
    old_manifest.read_to_string(&mut old_content).unwrap();
    assert_eq!(old_content, original);
    assert!(std::fs::read_to_string(&manifest_path)
        .unwrap()
        .contains("seq 2 type i"));
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn prepared_manifest_directory_sync_failure_rolls_back_before_cleanup() {
    let dir = temp_dir("commit-directory-sync-failure");
    let app_dir = dir.join("appendonlydir");
    std::fs::create_dir_all(&app_dir).unwrap();
    let manifest_path = app_dir.join("appendonly.aof.manifest");
    let original = "file appendonly.aof.1.incr.aof seq 1 type i\n";
    std::fs::write(&manifest_path, original).unwrap();
    std::fs::write(app_dir.join("appendonly.aof.1.incr.aof"), b"tail").unwrap();
    let mut prepared = prepare_synced_rdb_as_aof_base(
        dir.to_str().unwrap(),
        "appendonly.aof",
        "appendonlydir",
        b"new-base",
    )
    .unwrap();
    super::manifest::FAIL_NEXT_DIRECTORY_SYNC.with(|fail| fail.set(true));

    let result = prepared.commit_manifest();
    super::manifest::FAIL_NEXT_DIRECTORY_SYNC.with(|fail| fail.set(false));
    assert!(
        result.is_err(),
        "manifest rename must include directory synchronization"
    );
    drop(prepared);

    assert_eq!(std::fs::read_to_string(manifest_path).unwrap(), original);
    assert_eq!(
        std::fs::read(app_dir.join("appendonly.aof.1.incr.aof")).unwrap(),
        b"tail"
    );
    assert!(!app_dir.join("appendonly.aof.2.incr.aof").exists());
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn single_file_synced_base_replaces_configured_aof_and_rolls_back() {
    let dir = temp_dir("single-file-synced-base");
    std::fs::create_dir_all(&dir).unwrap();
    let target = dir.join("appendonly.aof");
    std::fs::write(&target, b"old-baseline").unwrap();
    let mut prepared = prepare_synced_rdb_as_aof_base(
        dir.to_str().unwrap(),
        "appendonly.aof",
        "",
        b"new-baseline",
    )
    .unwrap();
    prepared.commit_manifest().unwrap();
    assert_eq!(std::fs::read(&target).unwrap(), b"new-baseline");
    drop(prepared);
    assert_eq!(std::fs::read(&target).unwrap(), b"old-baseline");

    let (base, incr) = install_synced_rdb_as_aof_base(
        dir.to_str().unwrap(),
        "appendonly.aof",
        "",
        b"committed-baseline",
    )
    .unwrap();
    assert_eq!(base, target);
    assert_eq!(incr, target);
    assert_eq!(std::fs::read(&target).unwrap(), b"committed-baseline");
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn single_file_legacy_backup_does_not_block_new_install_or_get_deleted() {
    let dir = temp_dir("single-file-legacy-backup");
    std::fs::create_dir_all(&dir).unwrap();
    let target = dir.join("appendonly.aof");
    let legacy = dir.join(".appendonly.aof.sync.previous");
    std::fs::write(&target, b"legacy-baseline").unwrap();
    std::fs::hard_link(&target, &legacy).unwrap();
    let current = dir.join("current.tmp");
    std::fs::write(&current, b"current-baseline").unwrap();
    std::fs::rename(current, &target).unwrap();
    install_synced_rdb_as_aof_base(dir.to_str().unwrap(), "appendonly.aof", "", b"new-baseline")
        .unwrap();
    assert_eq!(std::fs::read(&target).unwrap(), b"new-baseline");
    assert_eq!(std::fs::read(&legacy).unwrap(), b"legacy-baseline");
    assert_eq!(
        std::fs::read_dir(&dir).unwrap().count(),
        2,
        "finalize must clean only its own backup"
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn single_file_legacy_backup_rollback_restores_this_install_current_target() {
    for fail_directory_sync in [false, true] {
        let dir = temp_dir("single-file-legacy-rollback");
        std::fs::create_dir_all(&dir).unwrap();
        let target = dir.join("appendonly.aof");
        let legacy = dir.join(".appendonly.aof.sync.previous");
        std::fs::write(&target, b"legacy-baseline").unwrap();
        std::fs::hard_link(&target, &legacy).unwrap();
        let current = dir.join("current.tmp");
        std::fs::write(&current, b"current-baseline").unwrap();
        std::fs::rename(current, &target).unwrap();
        let mut prepared = prepare_synced_rdb_as_aof_base(
            dir.to_str().unwrap(),
            "appendonly.aof",
            "",
            b"new-baseline",
        )
        .unwrap();
        super::manifest::FAIL_NEXT_DIRECTORY_SYNC.with(|fail| fail.set(fail_directory_sync));
        let result = prepared.commit_manifest();
        assert_eq!(result.is_err(), fail_directory_sync);
        assert_eq!(std::fs::read(&target).unwrap(), b"new-baseline");
        prepared.rollback().unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"current-baseline");
        assert_eq!(std::fs::read(&legacy).unwrap(), b"legacy-baseline");
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 2);
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[test]
fn single_file_committed_install_finalize_removes_its_backup() {
    let dir = temp_dir("single-file-finalize-backup");
    std::fs::create_dir_all(&dir).unwrap();
    let target = dir.join("appendonly.aof");
    std::fs::write(&target, b"current-baseline").unwrap();
    let mut prepared = prepare_synced_rdb_as_aof_base(
        dir.to_str().unwrap(),
        "appendonly.aof",
        "",
        b"new-baseline",
    )
    .unwrap();
    let backup = std::fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with(".appendonly.aof.sync.previous")
        })
        .unwrap();
    assert_eq!(std::fs::read(&backup).unwrap(), b"current-baseline");
    prepared.commit_manifest().unwrap();
    assert!(backup.exists());
    prepared.finalize();
    assert!(!backup.exists());
    assert_eq!(std::fs::read(&target).unwrap(), b"new-baseline");
    assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1);
    std::fs::remove_dir_all(dir).unwrap();
}
