use std::collections::HashMap;

use crate::foundation::io::store::kv_engine;
use crate::foundation::io::store::kv_engine::{Db, DbSet};
use crate::foundation::io::store::structure::hash::HashHt;
use crate::foundation::io::store::Robj;

fn populate_hash(db: &mut Db, key: &[u8], count: usize, deadline_ms: u64) {
    let mut hash = Robj::HashHashtable(HashHt::new());
    for index in 0..count {
        hash.hash_set(format!("field-{index}").as_bytes(), b"value");
    }
    db.set(key.to_vec(), hash);
    for index in 0..count {
        db.set_hash_field_expire(key, format!("field-{index}").as_bytes(), deadline_ms);
    }
}

#[test]
fn field_batches_resume_without_cleaning_an_entire_large_hash() {
    let mut db = Db::new();
    populate_hash(
        &mut db,
        b"large",
        4096,
        kv_engine::now_ms().saturating_sub(1),
    );

    assert_eq!(db.expire_hash_fields_batch(20, |_| true).0, 20);
    assert_eq!(db.hash_field_expiry_index.len(), 4076);
    assert_eq!(db.pending_hexpired.len(), 20);
    assert_eq!(
        db.get_hash_object(b"large")
            .expect("hash type")
            .expect("remaining fields")
            .hash_len(),
        4076
    );

    let mut total = 20;
    while db.keys_with_volatile_items_count() != 0 {
        let removed = db.expire_hash_fields_batch(20, |_| true).0;
        assert!(
            (1..=20).contains(&removed),
            "every batch must make bounded progress"
        );
        total += removed;
    }
    assert_eq!(total, 4096);
    assert_eq!(db.lazy_expired_hash_fields, 4096);
    assert_eq!(db.pending_hexpired.len(), 4096);
    assert!(db.hash_field_expiry_index.is_empty());
    assert!(db.get_allow_expired(b"large").is_none());
    assert_eq!(db.expire_hash_fields_batch(20, |_| true).0, 0);
}

#[test]
fn ttl_updates_and_persistence_remove_stale_deadlines() {
    let mut db = Db::new();
    let now_ms = kv_engine::now_ms();
    populate_hash(&mut db, b"hash", 2, now_ms.saturating_sub(1));
    for offset_ms in 1..=1000 {
        db.set_hash_field_expire(b"hash", b"field-0", now_ms + 60_000 + offset_ms);
        assert_eq!(
            db.hash_field_expiry_index.len(),
            2,
            "TTL updates must not accumulate stale index entries"
        );
    }
    assert!(db.remove_hash_field_expire(b"hash", b"field-1"));
    assert_eq!(db.expire_hash_fields_batch(20, |_| true).0, 0);
    assert_eq!(db.hash_field_expiry_index.len(), 1);
    assert_eq!(
        db.get_hash_object(b"hash")
            .expect("hash type")
            .expect("live hash")
            .hash_len(),
        2
    );
    assert!(db.remove_hash_field_expire(b"hash", b"field-0"));
    assert!(db.hash_field_expiry_index.is_empty());
    assert_eq!(db.keys_with_volatile_items_count(), 0);
}

#[test]
fn deleting_overwriting_and_flushing_keys_clear_the_deadline_index() {
    let mut db = Db::new();
    let deadline_ms = kv_engine::now_ms() + 60_000;
    populate_hash(&mut db, b"deleted", 3, deadline_ms);
    populate_hash(&mut db, b"overwritten", 3, deadline_ms);
    populate_hash(&mut db, b"flushed", 3, deadline_ms);
    assert!(db.del(b"deleted"));
    assert_eq!(db.hash_field_expiry_index.len(), 6);
    db.set(b"overwritten".to_vec(), Robj::string("replacement"));
    assert_eq!(db.hash_field_expiry_index.len(), 3);
    db.flush();
    assert!(db.hash_field_expiry_index.is_empty());
}

#[test]
fn transferred_and_loaded_field_ttls_are_indexed() {
    let mut db = Db::new();
    let deadline_ms = kv_engine::now_ms().saturating_sub(1);
    populate_hash(&mut db, b"source", 3, deadline_ms);
    let fields = db.take_hash_field_expires(b"source");
    assert!(db.hash_field_expiry_index.is_empty());
    assert_eq!(db.expire_hash_fields_batch(20, |_| true).0, 0);
    db.set_hash_field_expires_map(b"source".to_vec(), fields);
    assert_eq!(db.expire_hash_fields_batch(20, |_| true).0, 3);

    let mut hash = Robj::HashHashtable(HashHt::new());
    hash.hash_set(b"restored", b"value");
    db.load_entry_with_hash_field_expires(
        b"loaded".to_vec(),
        hash,
        None,
        HashMap::from([(b"restored".to_vec(), deadline_ms)]),
    );
    assert_eq!(db.expire_hash_fields_batch(20, |_| true).0, 1);
    assert!(db.get_allow_expired(b"loaded").is_none());
}

#[test]
fn paused_expiration_and_lazy_cleanup_preserve_index_consistency() {
    let mut db = Db::new();
    populate_hash(&mut db, b"hash", 2, kv_engine::now_ms().saturating_sub(1));
    db.set_pause_skip_expire(true);
    assert_eq!(db.expire_hash_fields_batch(20, |_| true).0, 0);
    assert_eq!(db.hash_field_expiry_index.len(), 2);
    db.set_pause_skip_expire(false);
    assert!(db.check_hash_field_expired(b"hash", b"field-0"));
    assert_eq!(db.hash_field_expiry_index.len(), 1);
    assert_eq!(db.cleanup_expired_hash_fields(b"hash"), 1);
    assert!(db.hash_field_expiry_index.is_empty());
    assert_eq!(db.expire_hash_fields_batch(20, |_| true).0, 0);
}

#[test]
fn expiration_notifications_deduplicate_until_drained() {
    let mut dbs = DbSet::new(1);
    let deadline_ms = kv_engine::now_ms().saturating_sub(1);
    let db = dbs.get_mut(0).expect("database zero");
    for _ in 0..2 {
        populate_hash(db, b"hash", 1, deadline_ms);
        assert_eq!(db.expire_hash_fields_batch(20, |_| true).0, 1);
    }
    assert_eq!(dbs.drain_pending_hexpired().len(), 1);
    let db = dbs.get_mut(0).expect("database zero");
    populate_hash(db, b"hash", 1, deadline_ms);
    assert_eq!(db.expire_hash_fields_batch(20, |_| true).0, 1);
    assert_eq!(dbs.drain_pending_hexpired().len(), 1);
}

#[test]
fn protected_deadlines_count_toward_the_budget_without_starving_other_keys() {
    let mut db = Db::new();
    let deadline_ms = kv_engine::now_ms().saturating_sub(1);
    populate_hash(&mut db, b"a-protected", 55, deadline_ms);
    populate_hash(&mut db, b"z-ready", 1, deadline_ms);
    let should_expire_key = |key: &[u8]| key == b"z-ready";

    assert_eq!(db.expire_hash_fields_batch(20, should_expire_key), (0, 20));
    assert_eq!(db.expire_hash_fields_batch(20, should_expire_key), (0, 20));
    assert_eq!(db.expire_hash_fields_batch(20, should_expire_key), (1, 16));
    assert_eq!(db.hash_field_expiry_index.len(), 55);
    assert_eq!(db.pending_hexpired.len(), 1);
    assert!(db.get_allow_expired(b"z-ready").is_none());
    assert_eq!(
        db.get_hash_object(b"a-protected")
            .expect("hash type")
            .expect("protected fields")
            .hash_len(),
        55
    );

    let mut removed = 0;
    while db.keys_with_volatile_items_count() != 0 {
        removed += db.expire_hash_fields_batch(20, |_| true).0;
    }
    assert_eq!(removed, 55);
    assert!(db.hash_field_expiry_index.is_empty());
    db.flush();
    assert!(db.hash_field_expiry_cursor.is_none());
}

#[test]
fn deadline_records_share_long_key_storage_across_updates_and_transfers() {
    let mut db = Db::new();
    let key = vec![b'k'; 64 * 1024];
    let deadline_ms = kv_engine::now_ms() + 60_000;
    populate_hash(&mut db, &key, 64, deadline_ms);
    db.set_hash_field_expire(&key, b"field-0", deadline_ms + 1);

    for is_transferred in [false, true] {
        if is_transferred {
            let fields = db.take_hash_field_expires(&key);
            db.set_hash_field_expires_map(key.clone(), fields);
        }
        let (stored_key, _) = db
            .hash_field_expires
            .get_key_value(key.as_slice())
            .expect("field TTL metadata");
        assert_eq!(db.hash_field_expiry_index.len(), 64);
        for expiry in &db.hash_field_expiry_index {
            assert_eq!(
                expiry.key.as_ptr(),
                stored_key.as_ptr(),
                "deadline records must not duplicate a large key for every field"
            );
        }
    }
}
