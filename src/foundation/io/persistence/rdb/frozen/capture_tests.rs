use super::*;
use crate::foundation::io::store::structure::list::ListValue;
use crate::foundation::io::store::Robj;
use bytes::Bytes;

fn early_feed(dbs: &DbSet) -> (ForklessFeed, std::sync::mpsc::Receiver<FrozenWorkItem>) {
    let frozen = FrozenRdb::capture_header(dbs, &[], &[], None, RdbSerializeOptions::default());
    let (feeder, receiver) = RdbSaveFeeder::test_channel(2);
    (frozen.into_incremental_feed(feeder, 0, true), receiver)
}

fn drain_feed(
    feed: &mut ForklessFeed,
    dbs: &mut DbSet,
    receiver: &std::sync::mpsc::Receiver<FrozenWorkItem>,
) -> Vec<(u32, FrozenEntry)> {
    let mut current_db = None;
    let mut entries = Vec::new();
    for _ in 0..10_000 {
        let is_finished = feed.drive(dbs, &mut [], 8);
        while let Ok(item) = receiver.try_recv() {
            match item {
                FrozenWorkItem::Header(_) => {}
                FrozenWorkItem::Database { id, .. } => {
                    assert!(current_db.is_none_or(|current| current < id));
                    current_db = Some(id);
                }
                FrozenWorkItem::Entry(mut entry) => {
                    // Emulate the serializer dropping its reservation while
                    // retaining values solely for assertions.
                    drop(entry._early_capture_reservation.take());
                    entries.push((current_db.expect("database header precedes entry"), entry));
                }
            }
        }
        if is_finished {
            return entries;
        }
    }
    panic!("bounded feed did not finish");
}

#[test]
fn future_priority_capture_bypasses_backpressure_without_reordering_databases() {
    let mut dbs = DbSet::new(3);
    for db_id in 0..3 {
        dbs.get_mut(db_id).unwrap().set(
            b"target".to_vec(),
            Robj::string_raw(Bytes::from_static(b"before")),
        );
    }
    let (mut feed, receiver) = early_feed(&dbs);
    assert!(!feed.drive(&mut dbs, &mut [], 1));
    assert!(feed.is_backpressured());
    assert!(feed.prioritize_keys(2, [b"target".to_vec()]));
    assert!(feed.prioritize_keys(1, [b"target".to_vec()]));

    assert!(!feed.drive(&mut dbs, &mut [], 8));
    let resolved = feed.take_resolved_keys();
    for db_id in [1, 2] {
        assert!(resolved.iter().any(|item| item.db_id == db_id));
        dbs.get_mut(db_id).unwrap().set(
            b"target".to_vec(),
            Robj::string_raw(Bytes::from_static(b"after")),
        );
    }
    let entries = drain_feed(&mut feed, &mut dbs, &receiver);
    assert_eq!(entries.len(), 3);
    for (db_id, entry) in entries {
        assert_eq!(
            entry.value.as_ref().as_bytes(),
            b"before",
            "database {db_id}"
        );
    }
}

#[test]
fn future_large_capture_has_independent_collection_storage() {
    let mut dbs = DbSet::new(2);
    dbs.get_mut(0).unwrap().set(
        b"filler".to_vec(),
        Robj::string_raw(Bytes::from_static(b"value")),
    );
    dbs.get_mut(1).unwrap().set(
        b"list".to_vec(),
        Robj::ListQuicklist(ListValue::from_vecdeque(
            (0..8_192).map(|_| Bytes::from_static(b"value")).collect(),
        )),
    );
    let (mut feed, receiver) = early_feed(&dbs);
    assert!(!feed.drive(&mut dbs, &mut [], 1));
    assert!(feed.prioritize_keys(1, [b"list".to_vec()]));
    assert!(!feed.drive(&mut dbs, &mut [], 1));
    assert_eq!(feed.take_resolved_keys().len(), 1);
    assert!(feed.take_queued_keys().iter().all(|item| item.db_id == 0));
    let Robj::ListQuicklist(live) = dbs.get_mut(1).unwrap().get_mut(b"list").unwrap() else {
        panic!("expected a list");
    };
    let live_storage = live.as_slices().0.as_ptr();
    live.push_back(Bytes::from_static(b"new"));

    let entries = drain_feed(&mut feed, &mut dbs, &receiver);
    let saved = &entries.iter().find(|(db_id, _)| *db_id == 1).unwrap().1;
    assert!(!saved.value.is_shared());
    let Robj::ListQuicklist(list) = saved.value.as_ref() else {
        panic!("expected a list");
    };
    assert_eq!(list.len(), 8_192);
    assert_ne!(list.as_slices().0.as_ptr(), live_storage);
}

#[test]
fn future_missing_capture_skips_later_creation_and_empty_database() {
    let mut dbs = DbSet::new(3);
    for db_id in 0..2 {
        dbs.get_mut(db_id).unwrap().set(
            b"existing".to_vec(),
            Robj::string_raw(Bytes::from_static(b"value")),
        );
    }
    let (mut feed, receiver) = early_feed(&dbs);
    assert!(!feed.drive(&mut dbs, &mut [], 1));
    for db_id in [1, 2] {
        assert!(feed.prioritize_keys(db_id, [b"missing".to_vec()]));
    }
    assert!(!feed.drive(&mut dbs, &mut [], 8));
    assert_eq!(feed.take_resolved_keys().len(), 2);
    for db_id in [1, 2] {
        dbs.get_mut(db_id).unwrap().set(
            b"missing".to_vec(),
            Robj::string_raw(Bytes::from_static(b"created")),
        );
    }
    let entries = drain_feed(&mut feed, &mut dbs, &receiver);
    assert_eq!(entries.len(), 2);
    assert!(entries.iter().all(|(_, entry)| entry.key == b"existing"));
}

#[test]
fn future_capture_quota_and_capacity_include_missing_targets() {
    let mut dbs = DbSet::new(2);
    for db_id in 0..2 {
        dbs.get_mut(db_id).unwrap().set(
            b"filler".to_vec(),
            Robj::string_raw(Bytes::from_static(b"value")),
        );
    }
    let (mut feed, _receiver) = early_feed(&dbs);
    assert!(!feed.drive(&mut dbs, &mut [], 1));
    assert!(feed.prioritize_keys(
        1,
        (0..EARLY_CAPTURE_CAPACITY + 16).map(|index| format!("missing-{index}").into_bytes()),
    ));
    assert!(!feed.drive(&mut dbs, &mut [], 1));
    assert_eq!(feed.take_resolved_keys().len(), 1);
    assert!(!feed.drive(&mut dbs, &mut [], EARLY_CAPTURE_CAPACITY));
    assert_eq!(feed.take_resolved_keys().len(), EARLY_CAPTURE_CAPACITY - 1);
    assert!(!feed.drive(&mut dbs, &mut [], EARLY_CAPTURE_CAPACITY));
    assert!(feed.take_resolved_keys().is_empty());
    assert_eq!(feed.priority_requested.len(), 16);
    let budget = Arc::clone(&feed.early_capture_budget);
    assert_eq!(budget.count.load(Ordering::Relaxed), EARLY_CAPTURE_CAPACITY);
    assert!(budget.bytes.load(Ordering::Relaxed) <= EARLY_CAPTURE_BYTES);
    drop(feed);
    assert_eq!(budget.count.load(Ordering::Relaxed), 0);
    assert_eq!(budget.bytes.load(Ordering::Relaxed), 0);
}

#[test]
fn backpressured_capture_does_not_recapture_an_ordinary_priority_handoff() {
    let mut dbs = DbSet::new(1);
    for index in 0..4 {
        dbs.get_mut(0).unwrap().set(
            format!("key-{index}").into_bytes(),
            Robj::string_raw(Bytes::from_static(b"value")),
        );
    }
    let frozen = FrozenRdb::capture_header(&dbs, &[], &[], None, RdbSerializeOptions::default());
    let (feeder, receiver) = RdbSaveFeeder::test_channel(3);
    let mut feed = frozen.into_incremental_feed(feeder, 0, true);
    assert!(!feed.drive(&mut dbs, &mut [], 1));
    let database = &feed.databases[0];
    let key = database.keys[database.key_index].clone();
    assert!(feed.prioritize_keys(0, [key.clone()]));
    // A previous priority turn grants the ordinary scan its next turn.
    feed.ordinary_turn_pending = true;
    assert!(!feed.drive(&mut dbs, &mut [], 1));
    assert!(feed.is_backpressured());
    assert!(!feed.priority_requested.iter().any(|item| item.key == key));
    assert!(!feed.drive(&mut dbs, &mut [], 1));
    assert!(feed.take_resolved_keys().is_empty());
    assert_eq!(feed.early_capture_budget.count.load(Ordering::Relaxed), 0);
    let entries = drain_feed(&mut feed, &mut dbs, &receiver);
    assert_eq!(
        entries.iter().filter(|(_, entry)| entry.key == key).count(),
        1
    );
}

#[test]
fn oversized_future_target_does_not_prevent_small_target_capture() {
    let mut dbs = DbSet::new(2);
    dbs.get_mut(0).unwrap().set(
        b"filler".to_vec(),
        Robj::string_raw(Bytes::from_static(b"value")),
    );
    dbs.get_mut(1).unwrap().set(
        b"large".to_vec(),
        Robj::string_raw(Bytes::from(vec![b'x'; EARLY_CAPTURE_BYTES])),
    );
    dbs.get_mut(1).unwrap().set(
        b"small".to_vec(),
        Robj::string_raw(Bytes::from_static(b"value")),
    );
    let (mut feed, _receiver) = early_feed(&dbs);
    assert!(!feed.drive(&mut dbs, &mut [], 1));
    assert!(feed.prioritize_keys(1, [b"large".to_vec(), b"small".to_vec()]));
    for _ in 0..2 {
        assert!(!feed.drive(&mut dbs, &mut [], 8));
    }
    assert_eq!(feed.take_resolved_keys()[0].key, b"small");
    assert_eq!(feed.priority_requested.len(), 1);
    assert_eq!(feed.early_capture_budget.count.load(Ordering::Relaxed), 1);
    assert!(feed.early_capture_budget.bytes.load(Ordering::Relaxed) < EARLY_CAPTURE_BYTES);
}

#[test]
fn future_capture_reservation_survives_feed_drop_until_entry_release() {
    let mut dbs = DbSet::new(2);
    for db_id in 0..2 {
        dbs.get_mut(db_id).unwrap().set(
            b"target".to_vec(),
            Robj::string_raw(Bytes::from_static(b"value")),
        );
    }
    let (mut feed, receiver) = early_feed(&dbs);
    assert!(feed.prioritize_keys(1, [b"target".to_vec()]));
    let budget = Arc::clone(&feed.early_capture_budget);
    let mut saved = None;
    for _ in 0..100 {
        feed.drive(&mut dbs, &mut [], 8);
        while let Ok(item) = receiver.try_recv() {
            if let FrozenWorkItem::Entry(entry) = item {
                if entry._early_capture_reservation.is_some() {
                    saved = Some(entry);
                }
            }
        }
        if feed.is_finished() {
            break;
        }
    }
    assert!(feed.is_finished());
    assert!(saved.is_some());
    assert_eq!(budget.count.load(Ordering::Relaxed), 1);
    drop(feed);
    assert_eq!(budget.count.load(Ordering::Relaxed), 1);
    drop(saved);
    assert_eq!(budget.count.load(Ordering::Relaxed), 0);
    assert_eq!(budget.bytes.load(Ordering::Relaxed), 0);
}

#[test]
fn future_capture_preserves_key_and_hash_field_expiration_metadata() {
    use crate::foundation::io::store::structure::hash::HashHt;

    for should_preserve_expired in [false, true] {
        let mut dbs = DbSet::new(2);
        dbs.get_mut(0).unwrap().set(
            b"filler".to_vec(),
            Robj::string_raw(Bytes::from_static(b"value")),
        );
        let mut hash = Robj::HashHashtable(HashHt::new());
        hash.hash_set(b"field", b"before");
        dbs.get_mut(1).unwrap().load_entry_with_hash_field_expires(
            b"hash".to_vec(),
            hash,
            Some(2_000),
            HashMap::from([(b"field".to_vec(), 1_500)]),
        );
        dbs.get_mut(1).unwrap().load_entry(
            b"expired".to_vec(),
            Robj::string_raw(Bytes::from_static(b"old-expired")),
            Some(500),
        );
        let (mut feed, receiver) = early_feed(&dbs);
        feed.capture_ms = 1_000;
        feed.should_preserve_expired = should_preserve_expired;
        assert!(!feed.drive(&mut dbs, &mut [], 1));
        assert!(feed.prioritize_keys(1, [b"hash".to_vec(), b"expired".to_vec()]));
        assert!(!feed.drive(&mut dbs, &mut [], 8));
        assert_eq!(feed.take_resolved_keys().len(), 2);
        let mut hash = Robj::HashHashtable(HashHt::new());
        hash.hash_set(b"field", b"after");
        dbs.get_mut(1).unwrap().load_entry_with_hash_field_expires(
            b"hash".to_vec(),
            hash,
            Some(3_000),
            HashMap::from([(b"field".to_vec(), 2_500)]),
        );
        let entries = drain_feed(&mut feed, &mut dbs, &receiver);
        let hash = &entries
            .iter()
            .find(|(_, entry)| entry.key == b"hash")
            .unwrap()
            .1;
        assert_eq!(hash.expire_ms, Some(2_000));
        assert_eq!(
            hash.hash_field_expires
                .as_ref()
                .unwrap()
                .get(b"field".as_slice()),
            Some(&1_500)
        );
        assert_eq!(hash.value.as_ref().hash_get(b"field").unwrap(), b"before");
        assert_eq!(
            entries.iter().any(|(_, entry)| entry.key == b"expired"),
            should_preserve_expired
        );
    }
}

#[test]
fn future_capture_budget_rejects_unaccounted_stream_and_large_expiration_maps() {
    use crate::foundation::io::store::structure::hash::HashHt;

    assert!(
        early_value_bytes(&Robj::StreamListpack(Box::default()), EARLY_CAPTURE_BYTES).is_none()
    );
    let mut dbs = DbSet::new(1);
    let mut hash = Robj::HashHashtable(HashHt::new());
    for index in 0..128 {
        hash.hash_set(format!("field-{index}").as_bytes(), b"value");
    }
    dbs.get_mut(0).unwrap().load_entry_with_hash_field_expires(
        b"target".to_vec(),
        hash,
        None,
        (0..128)
            .map(|index| (format!("field-{index}").into_bytes(), 10_000))
            .collect(),
    );
    let completion = ForklessItemId {
        dataset_id: None,
        db_id: 0,
        key: b"target".to_vec(),
    };
    assert!(capture_early_forkless_entry(&dbs, &[], &completion, Some(0), 0, true, 512).is_none());
}

#[test]
fn future_capture_tracks_swaps_without_remapping_additional_datasets() {
    let mut primary = DbSet::new(3);
    for (db_id, value) in [(0, b"zero".as_slice()), (1, b"one"), (2, b"two")] {
        primary.get_mut(db_id).unwrap().set(
            b"target".to_vec(),
            Robj::string_raw(Bytes::copy_from_slice(value)),
        );
    }
    let mut importing = DbSet::new(3);
    importing.get_mut(1).unwrap().set(
        b"target".to_vec(),
        Robj::string_raw(Bytes::from_static(b"imported")),
    );
    let frozen = FrozenRdb::capture_header(
        &primary,
        &[(b"import".to_vec(), &importing)],
        &[],
        None,
        RdbSerializeOptions::default(),
    );
    let (feeder, receiver) = RdbSaveFeeder::test_channel(2);
    let mut feed = frozen.into_incremental_feed(feeder, 0, true);
    let mut additional = [(b"import".to_vec(), &mut importing)];
    assert!(!feed.drive(&mut primary, &mut additional, 1));
    primary.swap(1, 2);
    feed.swap_databases(1, 2);
    assert!(feed.prioritize_keys(1, [b"target".to_vec()]));
    assert!(!feed.drive(&mut primary, &mut additional, 8));
    assert_eq!(feed.take_resolved_keys()[0].db_id, 1);
    primary.swap(0, 2);
    feed.swap_databases(0, 2);
    assert!(feed.prioritize_keys(2, [b"target".to_vec()]));
    assert_eq!(feed.databases[3].current_id, 1);
    let mut headers = Vec::new();
    let mut entries = Vec::new();
    for _ in 0..100 {
        feed.drive(&mut primary, &mut additional, 8);
        while let Ok(item) = receiver.try_recv() {
            match item {
                FrozenWorkItem::Header(_) => {}
                FrozenWorkItem::Database { id, .. } => headers.push(id),
                FrozenWorkItem::Entry(mut entry) => {
                    drop(entry._early_capture_reservation.take());
                    entries.push(entry.value.as_ref().as_bytes().to_vec());
                }
            }
        }
        if feed.is_finished() {
            break;
        }
    }
    assert!(feed.is_finished());
    assert_eq!(headers, [0, 1, 2, 1]);
    assert_eq!(
        entries,
        [
            b"zero".to_vec(),
            b"one".to_vec(),
            b"two".to_vec(),
            b"imported".to_vec()
        ]
    );
}
