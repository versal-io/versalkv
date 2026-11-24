use super::*;
use crate::foundation::io::persistence::rdb::compression::{RdbCompression, RdbSnapshotOptions};
use crate::foundation::io::persistence::snapshot::background::BackgroundRdbSave;
use crate::foundation::io::store::structure::list::ListValue;
use crate::foundation::io::store::Robj;
use bytes::Bytes;

#[test]
fn work_items_are_materialized_lazily() {
    let first_entries = (0..1_024)
        .map(|index| FrozenEntry {
            key: index.to_string().into_bytes(),
            value: SnapshotValue::Owned(Robj::string_raw(Bytes::from_static(b"value"))),
            expire_ms: None,
            hash_field_expires: None,
            completion: None,
            _early_capture_reservation: None,
        })
        .collect();
    let second_entries = vec![FrozenEntry {
        key: b"tail".to_vec(),
        value: SnapshotValue::Owned(Robj::string_raw(Bytes::from_static(b"value"))),
        expire_ms: Some(42),
        hash_field_expires: None,
        completion: None,
        _early_capture_reservation: None,
    }];
    let frozen = FrozenRdb {
        header: vec![1, 2, 3],
        databases: vec![
            FrozenDb {
                id: 7,
                dataset_id: None,
                entries: first_entries,
                total_entries: 1_024,
                expires: 0,
            },
            FrozenDb {
                id: 9,
                dataset_id: None,
                entries: second_entries,
                total_entries: 1,
                expires: 1,
            },
        ],
        total_keys: 1_025,
    };
    let first_entry_storage = frozen.databases[0].entries.as_ptr();
    let second_entry_storage = frozen.databases[1].entries.as_ptr();

    let mut items = frozen.into_work_items();
    assert_eq!(items.next_entry_storage_ptr(), Some(first_entry_storage));
    assert!(matches!(items.next(), Some(FrozenWorkItem::Header(_))));
    assert!(matches!(
        items.next(),
        Some(FrozenWorkItem::Database {
            id: 7,
            entries: 1_024,
            expires: 0
        })
    ));
    assert_eq!(items.next_entry_storage_ptr(), Some(first_entry_storage));
    for index in 0..1_024 {
        let Some(FrozenWorkItem::Entry(entry)) = items.next() else {
            panic!("expected an entry");
        };
        assert_eq!(entry.key, index.to_string().into_bytes());
    }
    assert!(matches!(
        items.next(),
        Some(FrozenWorkItem::Database {
            id: 9,
            entries: 1,
            expires: 1
        })
    ));
    assert_eq!(items.next_entry_storage_ptr(), Some(second_entry_storage));
    let Some(FrozenWorkItem::Entry(entry)) = items.next() else {
        panic!("expected the final entry");
    };
    assert_eq!(entry.key, b"tail");
    assert!(items.next().is_none());
}

#[test]
fn incremental_header_includes_slot_import_dataset_metadata() {
    let mut serving = DbSet::new(1);
    serving.get_mut(0).unwrap().set(
        b"serving".to_vec(),
        Robj::string_raw(Bytes::from_static(b"value")),
    );

    let mut importing = DbSet::new(1);
    importing.get_mut(0).unwrap().load_entry(
        b"imported".to_vec(),
        Robj::string_raw(Bytes::from_static(b"value")),
        Some(9_999_999_999_999),
    );

    let frozen = FrozenRdb::capture_header(
        &serving,
        &[(b"import".to_vec(), &importing)],
        &[],
        None,
        RdbSerializeOptions::default(),
    );

    assert_eq!(frozen.total_keys(), 2);
    assert_eq!(frozen.databases.len(), 2);
    assert_eq!(frozen.databases[0].dataset_id, None);
    assert_eq!(frozen.databases[0].total_entries, 1);
    assert_eq!(
        frozen.databases[1].dataset_id.as_deref(),
        Some(b"import".as_slice())
    );
    assert_eq!(frozen.databases[1].total_entries, 1);
    assert_eq!(frozen.databases[1].expires, 1);
}

#[test]
fn incremental_feed_limits_dictionary_scanning_per_drive() {
    let mut dbs = DbSet::new(1);
    for index in 0..4_096 {
        dbs.get_mut(0).unwrap().set(
            format!("key-{index}").into_bytes(),
            Robj::string_raw(Bytes::from_static(b"value")),
        );
    }
    let frozen = FrozenRdb::capture_header(&dbs, &[], &[], None, RdbSerializeOptions::default());
    let dir = std::env::temp_dir().join(format!(
        "versal-forkless-scan-budget-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (mut task, feeder) = BackgroundRdbSave::start_incremental(
        frozen.total_keys(),
        dir.clone(),
        "dump.rdb".to_string(),
        1,
        0,
        false,
        RdbSnapshotOptions {
            compression: RdbCompression::No,
            checksum: true,
        },
    )
    .unwrap();
    let mut feed = frozen.into_incremental_feed(feeder, 0, true);
    let mut additional_datasets = Vec::new();

    assert!(!feed.drive(&mut dbs, &mut additional_datasets, 1));
    assert!(
        !feed.databases[0].is_done,
        "one drive must not scan the complete dictionary"
    );

    task.control().cancel();
    let _ = task.finish();
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn ordinary_scan_does_not_retain_scheduled_key_copies() {
    let mut dbs = DbSet::new(1);
    dbs.get_mut(0).unwrap().set(
        b"ordinary".to_vec(),
        Robj::string_raw(Bytes::from_static(b"value")),
    );
    let frozen = FrozenRdb::capture_header(&dbs, &[], &[], None, RdbSerializeOptions::default());
    let dir = std::env::temp_dir().join(format!(
        "versal-forkless-ordinary-scheduled-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (mut task, feeder) = BackgroundRdbSave::start_incremental(
        frozen.total_keys(),
        dir.clone(),
        "dump.rdb".to_string(),
        1,
        0,
        false,
        RdbSnapshotOptions {
            compression: RdbCompression::No,
            checksum: true,
        },
    )
    .unwrap();
    let mut feed = frozen.into_incremental_feed(feeder, 0, true);

    assert!(!feed.drive(&mut dbs, &mut [], 1));
    assert!(feed.scheduled_keys.is_empty());

    drop(feed);
    task.control().cancel();
    let _ = task.finish();
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn priority_keys_can_target_any_database() {
    let mut dbs = DbSet::new(2);
    dbs.get_mut(0).unwrap().set(
        b"current".to_vec(),
        Robj::string_raw(Bytes::from_static(b"value")),
    );
    dbs.get_mut(1).unwrap().set(
        b"later".to_vec(),
        Robj::string_raw(Bytes::from_static(b"value")),
    );
    let frozen = FrozenRdb::capture_header(&dbs, &[], &[], None, RdbSerializeOptions::default());
    let dir = std::env::temp_dir().join(format!(
        "versal-forkless-priority-db-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (mut task, feeder) = BackgroundRdbSave::start_incremental(
        frozen.total_keys(),
        dir.clone(),
        "dump.rdb".to_string(),
        1,
        0,
        false,
        RdbSnapshotOptions {
            compression: RdbCompression::No,
            checksum: true,
        },
    )
    .unwrap();
    let mut feed = frozen.into_incremental_feed(feeder, 0, true);

    assert!(feed.prioritize_keys(0, [b"current".to_vec()]));
    assert!(feed.prioritize_keys(1, [b"later".to_vec()]));
    assert!(!feed.prioritize_keys(0, std::iter::empty()));

    drop(feed);
    task.control().cancel();
    let _ = task.finish();
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn priority_queue_resolves_current_database_behind_future_database_request() {
    let mut dbs = DbSet::new(2);
    dbs.get_mut(0).unwrap().set(
        b"current-filler".to_vec(),
        Robj::string_raw(Bytes::from_static(b"value")),
    );
    dbs.get_mut(1).unwrap().set(
        b"future-filler".to_vec(),
        Robj::string_raw(Bytes::from_static(b"value")),
    );
    let frozen = FrozenRdb::capture_header(&dbs, &[], &[], None, RdbSerializeOptions::default());
    let dir = std::env::temp_dir().join(format!(
        "versal-forkless-priority-order-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (mut task, feeder) = BackgroundRdbSave::start_incremental(
        frozen.total_keys(),
        dir.clone(),
        "dump.rdb".to_string(),
        1,
        0,
        false,
        RdbSnapshotOptions {
            compression: RdbCompression::No,
            checksum: true,
        },
    )
    .unwrap();
    let mut feed = frozen.into_incremental_feed(feeder, 0, true);

    assert!(!feed.drive(&mut dbs, &mut [], 1));
    let future = ForklessItemId {
        dataset_id: None,
        db_id: 1,
        key: b"future-missing".to_vec(),
    };
    let current = ForklessItemId {
        dataset_id: None,
        db_id: 0,
        key: b"current-missing".to_vec(),
    };
    assert!(feed.prioritize_keys(1, [future.key.clone()]));
    assert!(feed.prioritize_keys(0, [current.key.clone()]));

    assert!(!feed.drive(&mut dbs, &mut [], 1));
    assert!(
        feed.take_resolved_keys().contains(&current),
        "a future-database request must not block a current-database priority key"
    );

    drop(feed);
    task.control().cancel();
    let _ = task.finish();
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn priority_keys_resolve_entries_absent_from_the_snapshot() {
    let mut dbs = DbSet::new(1);
    dbs.get_mut(0).unwrap().set(
        b"existing".to_vec(),
        Robj::string_raw(Bytes::from_static(b"value")),
    );
    let frozen = FrozenRdb::capture_header(&dbs, &[], &[], None, RdbSerializeOptions::default());
    let dir = std::env::temp_dir().join(format!(
        "versal-forkless-priority-missing-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (mut task, feeder) = BackgroundRdbSave::start_incremental(
        frozen.total_keys(),
        dir.clone(),
        "dump.rdb".to_string(),
        1,
        0,
        false,
        RdbSnapshotOptions {
            compression: RdbCompression::No,
            checksum: true,
        },
    )
    .unwrap();
    let mut feed = frozen.into_incremental_feed(feeder, 0, true);
    let missing = ForklessItemId {
        dataset_id: None,
        db_id: 0,
        key: b"missing".to_vec(),
    };

    assert!(feed.prioritize_keys(0, [missing.key.clone()]));
    assert!(!feed.drive(&mut dbs, &mut [], 1));
    assert_eq!(feed.take_resolved_keys(), vec![missing.clone()]);
    assert!(feed.scheduled_keys.contains(&missing));

    drop(feed);
    task.control().cancel();
    let _ = task.finish();
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn priority_shared_key_waits_for_background_serialization() {
    let mut dbs = DbSet::new(1);
    dbs.get_mut(0).unwrap().set(
        b"owned".to_vec(),
        Robj::ListQuicklist(ListValue::from_vecdeque(
            (0..8_192).map(|_| Bytes::from_static(b"value")).collect(),
        )),
    );
    let frozen = FrozenRdb::capture_header(&dbs, &[], &[], None, RdbSerializeOptions::default());
    let dir = std::env::temp_dir().join(format!(
        "versal-forkless-priority-owned-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (mut task, feeder) = BackgroundRdbSave::start_incremental(
        frozen.total_keys(),
        dir.clone(),
        "dump.rdb".to_string(),
        1,
        10_000_000,
        false,
        RdbSnapshotOptions {
            compression: RdbCompression::No,
            checksum: true,
        },
    )
    .unwrap();
    let mut feed = frozen.into_incremental_feed(feeder, 0, true);
    let owned = ForklessItemId {
        dataset_id: None,
        db_id: 0,
        key: b"owned".to_vec(),
    };

    assert!(feed.prioritize_keys(0, [owned.key.clone()]));
    assert!(!feed.drive(&mut dbs, &mut [], 1));
    assert!(feed.take_resolved_keys().is_empty());
    assert_eq!(feed.take_queued_keys(), vec![owned]);

    drop(feed);
    task.control().cancel();
    let _ = task.finish();
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn priority_owned_key_releases_writer_before_background_serialization() {
    let mut dbs = DbSet::new(1);
    dbs.get_mut(0).unwrap().set(
        b"owned".to_vec(),
        Robj::string_raw(Bytes::from_static(b"value")),
    );
    let frozen = FrozenRdb::capture_header(&dbs, &[], &[], None, RdbSerializeOptions::default());
    let dir = std::env::temp_dir().join(format!(
        "versal-forkless-priority-owned-release-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let (mut task, feeder) = BackgroundRdbSave::start_incremental(
        frozen.total_keys(),
        dir.clone(),
        "dump.rdb".to_string(),
        1,
        10_000_000,
        false,
        RdbSnapshotOptions {
            compression: RdbCompression::No,
            checksum: true,
        },
    )
    .unwrap();
    let mut feed = frozen.into_incremental_feed(feeder, 0, true);
    let owned = ForklessItemId {
        dataset_id: None,
        db_id: 0,
        key: b"owned".to_vec(),
    };

    assert!(feed.prioritize_keys(0, [owned.key.clone()]));
    assert!(!feed.drive(&mut dbs, &mut [], 1));
    let resolved = feed.take_resolved_keys();
    let queued = feed.take_queued_keys();
    let processed = task.control().progress().processed;
    drop(feed);
    task.control().cancel();
    let _ = task.finish();
    let _ = std::fs::remove_dir_all(dir);

    assert_eq!(resolved, vec![owned]);
    assert!(queued.is_empty());
    assert_eq!(processed, 0);
}

#[test]
fn missing_priority_work_consumes_the_priority_budget() {
    let mut dbs = DbSet::new(1);
    dbs.get_mut(0).unwrap().set(
        b"ordinary".to_vec(),
        Robj::string_raw(Bytes::from_static(b"value")),
    );
    let frozen = FrozenRdb::capture_header(&dbs, &[], &[], None, RdbSerializeOptions::default());
    let dir = std::env::temp_dir().join(format!(
        "versal-forkless-priority-budget-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (mut task, feeder) = BackgroundRdbSave::start_incremental(
        frozen.total_keys(),
        dir.clone(),
        "dump.rdb".to_string(),
        1,
        0,
        false,
        RdbSnapshotOptions {
            compression: RdbCompression::No,
            checksum: true,
        },
    )
    .unwrap();
    let mut feed = frozen.into_incremental_feed(feeder, 0, true);
    let missing_keys = (0..3).map(|index| format!("missing-{index}").into_bytes());

    assert!(feed.prioritize_keys(0, missing_keys));
    assert!(!feed.drive(&mut dbs, &mut [], 1));

    assert_eq!(feed.take_resolved_keys().len(), 1);
    assert_eq!(
        feed.priority_keys.get(&(None, 0)).map_or(0, VecDeque::len),
        2
    );

    drop(feed);
    task.control().cancel();
    let _ = task.finish();
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn full_priority_budget_still_advances_the_ordinary_scan() {
    let mut dbs = DbSet::new(1);
    let keys: Vec<Vec<u8>> = (0..4)
        .map(|index| format!("key-{index}").into_bytes())
        .collect();
    for key in &keys {
        dbs.get_mut(0)
            .unwrap()
            .set(key.clone(), Robj::string_raw(Bytes::from_static(b"value")));
    }
    let frozen = FrozenRdb::capture_header(&dbs, &[], &[], None, RdbSerializeOptions::default());
    let dir = std::env::temp_dir().join(format!(
        "versal-forkless-priority-fairness-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (mut task, feeder) = BackgroundRdbSave::start_incremental(
        frozen.total_keys(),
        dir.clone(),
        "dump.rdb".to_string(),
        1,
        0,
        false,
        RdbSnapshotOptions {
            compression: RdbCompression::No,
            checksum: true,
        },
    )
    .unwrap();
    let mut feed = frozen.into_incremental_feed(feeder, 0, true);

    assert!(feed.prioritize_keys(0, keys));
    assert!(!feed.drive(&mut dbs, &mut [], 1));
    assert!(!feed.drive(&mut dbs, &mut [], 1));

    let database = &feed.databases[0];
    assert!(
        database.cursor != 0
            || !database.keys.is_empty()
            || database.key_index != 0
            || database.is_done,
        "priority capture must preserve ordinary scan progress"
    );

    drop(feed);
    task.control().cancel();
    let _ = task.finish();
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn ordinary_scan_does_not_recapture_a_queued_priority_key() {
    let mut dbs = DbSet::new(1);
    for index in 0..4 {
        dbs.get_mut(0).unwrap().set(
            format!("key-{index}").into_bytes(),
            Robj::string_raw(Bytes::from_static(b"value")),
        );
    }
    let frozen = FrozenRdb::capture_header(&dbs, &[], &[], None, RdbSerializeOptions::default());
    let dir = std::env::temp_dir().join(format!(
        "versal-forkless-priority-stale-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (mut task, feeder) = BackgroundRdbSave::start_incremental(
        frozen.total_keys(),
        dir.clone(),
        "dump.rdb".to_string(),
        1,
        0,
        false,
        RdbSnapshotOptions {
            compression: RdbCompression::No,
            checksum: true,
        },
    )
    .unwrap();
    let mut feed = frozen.into_incremental_feed(feeder, 0, true);

    assert!(!feed.drive(&mut dbs, &mut [], 1));
    let database = &feed.databases[0];
    let ordinary_key = database.keys[database.key_index].clone();
    let first_priority = database.keys[database.key_index + 1].clone();
    assert!(feed.prioritize_keys(0, [first_priority.clone(), ordinary_key.clone()]));

    assert!(!feed.drive(&mut dbs, &mut [], 1));
    assert!(!feed.drive(&mut dbs, &mut [], 1));
    assert!(!feed.drive(&mut dbs, &mut [], 1));

    let captured = feed
        .take_captured_keys()
        .into_iter()
        .filter(|item| item.key == ordinary_key)
        .count();
    assert_eq!(captured, 1);

    drop(feed);
    task.control().cancel();
    let _ = task.finish();
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn background_handoff_promotes_large_values_without_copying_them() {
    let mut dbs = DbSet::new(1);
    dbs.get_mut(0).unwrap().set(
        b"large".to_vec(),
        Robj::ListQuicklist(ListValue::from_vecdeque(
            (0..8_192).map(|_| Bytes::from_static(b"value")).collect(),
        )),
    );
    let live_ptr = dbs.get(0).unwrap().get_allow_expired(b"large").unwrap();
    let Robj::ListQuicklist(live) = live_ptr else {
        panic!("expected a list")
    };
    let live_storage = live.as_slices().0.as_ptr();

    let frozen = FrozenRdb::capture_primary_with_background_handoff(
        &mut dbs,
        &[],
        None,
        RdbSerializeOptions::default(),
        0,
        true,
    );
    let entry = &frozen.databases[0].entries[0];
    assert!(entry.value.is_shared());
    let SnapshotValue::Shared(value) = &entry.value else {
        unreachable!("large value must use the shared handoff variant");
    };
    let Robj::ListQuicklist(saved) = value.as_ref() else {
        panic!("expected a list")
    };
    assert_eq!(live_storage, saved.as_slices().0.as_ptr());
}
