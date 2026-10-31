use super::*;
use crate::foundation::io::store::structure::list::ListValue;
use crate::foundation::io::store::structure::set::{IntSet, SetEncoding, ValkeySet};
use crate::foundation::memory::value::ZSetOps;
use bytes::Bytes;
use std::collections::VecDeque;
use std::ptr;

fn past_ms() -> u64 {
    now_ms().saturating_sub(1)
}

fn future_ms() -> u64 {
    now_ms() + 60_000
}

fn first_byte_slot(key: &[u8]) -> usize {
    key.first().copied().unwrap_or_default() as usize & 0b11
}

#[test]
fn lazy_flush_does_not_clone_value_owned_by_background_snapshot() {
    let mut db = Db::new();
    db.set(
        b"large".to_vec(),
        Robj::ListQuicklist(ListValue::from_vecdeque(VecDeque::from(vec![
            Bytes::from(
                vec![b'x'; 128]
            );
            1_024
        ]))),
    );
    let snapshot = db
        .snapshot_value_for_background(b"large", 64 * 1024)
        .unwrap();
    assert!(snapshot.is_shared());

    let deferred = db.drain_values_for_lazyfree();

    assert!(
        deferred.is_empty(),
        "the existing background owner must release the shared value off-thread"
    );
    assert!(matches!(snapshot.as_ref(), Robj::ListQuicklist(_)));
}

#[test]
fn dbset_resize_budget_is_one_shared_allowance() {
    let mut dbs = DbSet::new(2);
    for db_idx in 0..2 {
        for index in 0..7u8 {
            dbs.get_mut(db_idx)
                .unwrap()
                .set(vec![index], Robj::string("value"));
        }
    }
    let expansion_bytes = 2 * std::mem::size_of::<usize>();
    dbs.set_resize_budget(Some(expansion_bytes));

    dbs.get_mut(0).unwrap().set(vec![7], Robj::string("value"));
    dbs.get_mut(1).unwrap().set(vec![7], Robj::string("value"));
    assert_eq!(dbs.data_bucket_count(0), 2);
    assert_eq!(dbs.data_bucket_count(1), 1);

    dbs.get_mut(0).unwrap().set(vec![8], Robj::string("value"));
    dbs.get_mut(1).unwrap().set(vec![8], Robj::string("value"));

    assert_eq!(dbs.data_bucket_count(0), 2);
    assert_eq!(dbs.data_bucket_count(1), 1);
}

#[test]
fn background_resize_can_pause_only_rehash_progress() {
    let mut dbs = DbSet::new(1);
    use crate::foundation::io::store::structure::dict::ResizePolicy;
    dbs.get_mut(0)
        .unwrap()
        .set(vec![0, 0], Robj::string("value"));
    dbs.get_mut(0)
        .unwrap()
        .set_data_resize_policy(ResizePolicy::Forbid);
    for index in 0..64u8 {
        dbs.get_mut(0)
            .unwrap()
            .set(vec![index, 0], Robj::string("value"));
    }
    dbs.get_mut(0)
        .unwrap()
        .set_data_resize_policy(ResizePolicy::Allow);
    dbs.set_resize_budget(Some(usize::MAX));
    assert!(
        dbs.data_bucket_count(0) == 1,
        "fixture needs a one-bucket table before cron expansion"
    );

    // The first cron pass starts expansion, but active rehashing is off
    // so it must leave the new cursor untouched.
    dbs.try_resize_all(false);
    assert!(
        dbs.rehashing_count(0) > 0,
        "fixture must enter incremental rehash"
    );

    let before = dbs.rehashing_bucket_count(0);
    dbs.try_resize_all(false);
    assert_eq!(
        dbs.rehashing_bucket_count(0),
        before,
        "disabling activerehashing must not advance an existing cursor"
    );

    dbs.try_resize_all(true);
    assert!(
        dbs.rehashing_bucket_count(0) < before || dbs.rehashing_count(0) == 0,
        "re-enabling activerehashing must resume the pending cursor"
    );
}

#[test]
fn injected_slot_router_places_data_and_expires_in_matching_slots() {
    let mut db = Db::new_with_slot_router(2, first_byte_slot);

    db.set(vec![1, b'a'], Robj::string("v1"));
    db.set(vec![2, b'a'], Robj::string("v2"));
    db.set_expire(&[1, b'a'], 10);
    db.set_expire(&[2, b'a'], 20);

    assert_eq!(db.get_expire(&[1, b'a']), Some(10));
    assert_eq!(db.get_expire(&[2, b'a']), Some(20));
}

#[test]
fn injected_slot_router_is_used_by_read_and_delete_paths() {
    let mut db = Db::new_with_slot_router(2, first_byte_slot);
    db.set(vec![1, b'a'], Robj::string("v1"));
    db.set(vec![2, b'a'], Robj::string("v2"));

    assert!(db.get(&[1, b'a']).is_some());
    assert!(db.get(&[2, b'a']).is_some());
    assert!(db.del(&[1, b'a']));

    assert!(db.get(&[1, b'a']).is_none());
    assert!(db.get(&[2, b'a']).is_some());
}

#[test]
fn hash_string_ref_marker_tracks_overwrite_and_key_lifecycle() {
    let mut db = Db::new();

    assert!(!db.set_hash_string_ref(b"h", b"field", b"value"));
    db.get_or_create_hash(b"h")
        .unwrap()
        .insert(Bytes::from_static(b"field"), Bytes::from_static(b"copy"));
    assert!(db.set_hash_string_ref(b"h", b"field", b"value"));
    assert!(db.hash_has_string_ref(b"h", b"field"));
    assert_eq!(
        db.get_hash(b"h").unwrap().unwrap().get(b"field".as_slice()),
        Some(&Bytes::from_static(b"value"))
    );

    db.clear_hash_string_ref(b"h", b"field");
    db.get_or_create_hash(b"h")
        .unwrap()
        .insert(Bytes::from_static(b"field"), Bytes::from_static(b"copy"));
    assert!(!db.hash_has_string_ref(b"h", b"field"));

    assert!(db.set_hash_string_ref(b"h", b"field", b"value"));
    assert!(db.del(b"h"));
    assert!(!db.hash_has_string_ref(b"h", b"field"));
}

#[test]
fn typed_accessors_distinguish_missing_wrong_type_and_match() {
    let mut db = Db::new();
    db.set(b"s".to_vec(), Robj::string("value"));
    db.set(
        b"l".to_vec(),
        Robj::ListQuicklist(ListValue::from_vecdeque(VecDeque::from([
            Bytes::from_static(b"x"),
        ]))),
    );
    db.set(
        b"h".to_vec(),
        Robj::HashHashtable(crate::foundation::io::store::structure::hash::HashHt::new()),
    );
    db.set(
        b"z".to_vec(),
        Robj::ZSetSkiplist(crate::foundation::io::store::structure::zset::ZSetSl::new()),
    );
    db.set(b"set".to_vec(), Robj::SetHashtable(ValkeySet::new()));
    db.set(b"stream".to_vec(), Robj::StreamListpack(Box::default()));

    assert_eq!(db.get_string(b"s").unwrap().unwrap(), b"value");
    assert!(db.get_string(b"missing").unwrap().is_none());
    assert!(db.get_string(b"l").is_err());
    assert!(db.get_list(b"l").unwrap().is_some());
    assert!(db.get_hash(b"h").unwrap().is_some());
    assert!(db.get_zset(b"z").unwrap().is_some());
    assert!(db.get_set(b"set").unwrap().is_some());
    assert!(db.get_stream(b"stream").unwrap().is_some());
}

#[test]
fn loading_hash_field_expiry_preserves_an_expired_key_deadline() {
    let mut db = Db::new();
    let mut field_expiries = std::collections::HashMap::new();
    field_expiries.insert(b"field".to_vec(), 1);

    db.load_entry_with_hash_field_expires(
        b"hash".to_vec(),
        Robj::HashHashtable(crate::foundation::io::store::structure::hash::HashHt::new()),
        Some(1),
        field_expiries,
    );

    assert_eq!(db.get_expire(b"hash"), Some(1));
    assert_eq!(db.get_hash_field_expire(b"hash", b"field"), Some(1));
}

#[test]
fn take_clears_replica_local_expiry_metadata_like_delete() {
    let mut db = Db::new();
    db.set(b"key".to_vec(), Robj::string("value"));
    db.mark_replica_local_expire(b"key");
    db.set_replica_local_expire_only(true);
    assert!(db.should_expire_locally(b"key"));

    assert!(db.take(b"key").is_some());

    assert!(!db.should_expire_locally(b"key"));
}

#[test]
fn set_accessors_accept_intset_values_loaded_from_rdb() {
    let mut db = Db::new();
    for key in [b"read".as_slice(), b"write", b"create"] {
        db.set(key.to_vec(), Robj::SetIntset(IntSet::from_slice(&[-7, 42])));
    }

    let read = db.get_set(b"read").unwrap().unwrap();
    assert_eq!(read.encoding, SetEncoding::Intset);
    assert!(read.data.contains_key(b"-7".as_slice()));
    assert!(read.data.contains_key(b"42".as_slice()));

    let write = db.get_set_mut(b"write").unwrap().unwrap();
    assert!(write.insert_with_encoding(Bytes::from_static(b"99"), 512, 128, 64));
    assert_eq!(write.encoding, SetEncoding::Intset);

    let create = db.get_or_create_set(b"create").unwrap();
    assert!(create.insert_with_encoding(Bytes::from_static(b"100"), 512, 128, 64));
    assert_eq!(create.encoding, SetEncoding::Intset);

    for key in [b"write".as_slice(), b"create"] {
        let set = db.get_set(key).unwrap().unwrap();
        assert!(set.data.contains_key(b"-7".as_slice()));
        assert!(set.data.contains_key(b"42".as_slice()));
    }
}

#[test]
fn get_or_create_creates_empty_container_and_rejects_wrong_type() {
    let mut db = Db::new();

    db.get_or_create_list(b"list")
        .unwrap()
        .push_back(Bytes::from_static(b"v"));
    db.get_or_create_hash(b"hash")
        .unwrap()
        .insert(Bytes::from_static(b"f"), Bytes::from_static(b"v"));
    db.get_or_create_zset(b"zset")
        .unwrap()
        .add(b"m", 1.0)
        .unwrap();
    db.get_or_create_set(b"set").unwrap().insert_with_encoding(
        Bytes::from_static(b"1"),
        512,
        128,
        64,
    );
    db.get_or_create_stream(b"stream").unwrap();

    assert_eq!(db.get_list(b"list").unwrap().unwrap().len(), 1);
    assert_eq!(db.get_hash(b"hash").unwrap().unwrap().len(), 1);
    assert_eq!(db.get_zset(b"zset").unwrap().unwrap().len(), 1);
    assert_eq!(db.get_set(b"set").unwrap().unwrap().data.len(), 1);
    assert!(db.get_stream(b"stream").unwrap().is_some());
    assert!(db.get_or_create_hash(b"list").is_err());
}

#[test]
fn configured_hash_writes_keep_listpack_until_the_entry_limit_is_crossed() {
    let mut db = Db::new();

    assert!(db
        .hash_set_with_limits(b"hash", b"first", b"value", 1, 64)
        .unwrap());
    assert!(matches!(db.get(b"hash"), Some(Robj::HashListpack(_))));

    assert!(db
        .hash_set_with_limits(b"hash", b"second", b"value", 1, 64)
        .unwrap());
    assert!(matches!(db.get(b"hash"), Some(Robj::HashHashtable(_))));

    let mut zero_limit = Db::new();
    assert!(zero_limit
        .hash_set_with_limits(b"hash", b"field", b"value", 0, 64)
        .unwrap());
    assert!(matches!(
        zero_limit.get(b"hash"),
        Some(Robj::HashHashtable(_))
    ));
}

#[test]
fn configured_zset_writes_keep_listpack_until_the_entry_limit_is_crossed() {
    let mut db = Db::new();

    assert!(db
        .zset_add_with_limits(b"zset", b"first", 1.0, 1, 64)
        .unwrap());
    assert!(matches!(db.get(b"zset"), Some(Robj::ZSetListpack(_))));

    assert!(db
        .zset_add_with_limits(b"zset", b"second", 2.0, 1, 64)
        .unwrap());
    assert!(matches!(db.get(b"zset"), Some(Robj::ZSetSkiplist(_))));
}

#[test]
fn zset_object_accessor_reads_a_listpack_without_upgrading_it() {
    let mut db = Db::new();
    db.zset_add_with_limits(b"zset", b"member", 1.0, 4, 64)
        .unwrap();

    assert_eq!(
        db.get_zset_object(b"zset")
            .unwrap()
            .unwrap()
            .zset_score(b"member"),
        Some(1.0)
    );
    assert!(matches!(db.get(b"zset"), Some(Robj::ZSetListpack(_))));
}

#[test]
fn hash_object_accessor_reads_a_listpack_without_upgrading_it() {
    let mut db = Db::new();
    db.hash_set_with_limits(b"hash", b"field", b"value", 4, 64)
        .unwrap();

    assert_eq!(
        db.get_hash_object(b"hash")
            .unwrap()
            .unwrap()
            .hash_get(b"field"),
        Some(b"value".to_vec())
    );
    assert!(matches!(db.get(b"hash"), Some(Robj::HashListpack(_))));
}

#[test]
fn adding_hash_field_expiry_upgrades_listpack_to_hashtable() {
    let mut db = Db::new();
    db.hash_set_with_limits(b"hash", b"field", b"value", 4, 64)
        .unwrap();
    assert!(matches!(db.get(b"hash"), Some(Robj::HashListpack(_))));

    let expire = future_ms();
    db.set_hash_field_expire(b"hash", b"field", expire);

    assert!(matches!(db.get(b"hash"), Some(Robj::HashHashtable(_))));
    assert_eq!(db.get_hash_field_expire(b"hash", b"field"), Some(expire));
}

#[test]
fn get_or_create_set_replaces_an_expired_set_and_clears_its_ttl() {
    let mut db = Db::new();
    assert!(db.get_or_create_set(b"set").unwrap().insert_with_encoding(
        Bytes::from_static(b"member"),
        512,
        128,
        64
    ));
    db.set_expire(b"set", past_ms());

    assert!(db.get_or_create_set(b"set").unwrap().insert_with_encoding(
        Bytes::from_static(b"member"),
        512,
        128,
        64
    ));

    assert_eq!(db.get_expire(b"set"), None);
    assert_eq!(db.get_set(b"set").unwrap().unwrap().data.len(), 1);
}

#[test]
fn lazy_expiration_deletes_key_and_records_propagation_once() {
    let mut db = Db::new();
    db.set(b"k".to_vec(), Robj::string("v"));
    db.set_expire(b"k", past_ms());

    assert!(db.get(b"k").is_none());
    assert_eq!(db.dbsize(), 0);
    assert_eq!(db.expired_deletions, vec![b"k".to_vec()]);
    assert!(db.get(b"k").is_none());
    assert_eq!(db.expired_deletions, vec![b"k".to_vec()]);
}

#[test]
fn no_lazy_expire_and_pause_skip_preserve_expired_key_for_internal_reads() {
    let mut db = Db::new();
    db.set(b"k".to_vec(), Robj::string("v"));
    db.set_expire(b"k", past_ms());

    db.set_no_lazy_expire(true);
    assert!(db.get(b"k").is_none());
    assert_eq!(db.get_allow_expired(b"k").unwrap().as_bytes(), b"v");
    assert!(db.expired_deletions.is_empty());

    db.set_no_lazy_expire(false);
    db.set_pause_skip_expire(true);
    assert!(!db.exists(b"k"));
    assert_eq!(db.get_allow_expired(b"k").unwrap().as_bytes(), b"v");
    assert!(db.expired_deletions.is_empty());
}

#[test]
fn set_clears_ttl_and_metadata_while_set_keep_ttl_preserves_ttl() {
    let mut db = Db::new();
    db.set(b"k".to_vec(), Robj::string("old"));
    db.set_expire(b"k", future_ms());
    db.set_lru_idle(b"k", 7);
    db.set(b"k".to_vec(), Robj::string("new"));

    assert_eq!(db.get_expire(b"k"), None);
    assert_eq!(db.get_lru_idle(b"k"), 0);

    let expire = future_ms();
    db.set_expire(b"k", expire);
    db.set_keep_ttl(b"k".to_vec(), Robj::string("kept"));
    assert_eq!(db.get_expire(b"k"), Some(expire));
}

#[test]
fn replica_local_expire_only_removes_marked_keys() {
    let mut db = Db::new();
    db.set(b"local".to_vec(), Robj::string("local"));
    db.set_expire(b"local", past_ms());
    db.mark_replica_local_expire(b"local");
    db.set(b"primary".to_vec(), Robj::string("primary"));
    db.set_expire(b"primary", past_ms());
    db.set_replica_local_expire_only(true);

    crate::foundation::io::store::kv_engine::expire::active_expire_cycle(&mut db, false);

    assert!(db.get_allow_expired(b"local").is_none());
    assert!(db.get_allow_expired(b"primary").is_some());
}

#[test]
fn persist_ttl_and_expiretime_report_valkey_states() {
    let mut db = Db::new();
    assert_eq!(db.ttl_ms(b"missing"), -2);
    assert_eq!(db.expiretime_ms(b"missing"), -2);

    db.set(b"k".to_vec(), Robj::string("v"));
    assert_eq!(db.ttl_ms(b"k"), -1);
    assert_eq!(db.expiretime_ms(b"k"), -1);

    let expire = future_ms();
    db.set_expire(b"k", expire);
    assert!(db.ttl_ms(b"k") > 0);
    assert_eq!(db.expiretime_ms(b"k"), expire as i64);
    assert!(db.persist(b"k"));
    assert!(!db.persist(b"k"));
    assert_eq!(db.ttl_ms(b"k"), -1);
}

#[test]
fn hash_field_expiration_removes_only_expired_field() {
    let mut db = Db::new();
    let hash = db.get_or_create_hash(b"h").unwrap();
    hash.insert(Bytes::from_static(b"expired"), Bytes::from_static(b"v1"));
    hash.insert(Bytes::from_static(b"live"), Bytes::from_static(b"v2"));
    db.set_hash_field_expire(b"h", b"expired", past_ms());
    db.set_hash_field_expire(b"h", b"live", future_ms());

    assert_eq!(db.hash_field_ttl_ms(b"h", b"missing"), -1);
    assert_eq!(db.hash_field_ttl_ms(b"h", b"expired"), -2);
    assert!(db
        .get_hash(b"h")
        .unwrap()
        .unwrap()
        .contains_key(b"expired".as_slice()));
    assert!(db.check_hash_field_expired(b"h", b"expired"));
    assert!(!db
        .get_hash(b"h")
        .unwrap()
        .unwrap()
        .contains_key(b"expired".as_slice()));
    assert!(db.hash_field_ttl_ms(b"h", b"live") > 0);
    assert!(db.remove_hash_field_expire(b"h", b"live"));
    assert!(!db.remove_hash_field_expire(b"h", b"live"));
    assert_eq!(db.hash_field_expiretime_ms(b"h", b"live"), -1);
}

#[test]
fn take_clone_and_iteration_preserve_expire_metadata() {
    let mut db = Db::new();
    let expire = future_ms();
    db.load_entry(b"k".to_vec(), Robj::string("v"), Some(expire));
    db.load_entry(b"expired".to_vec(), Robj::string("x"), Some(past_ms()));

    let cloned = db.clone_entry(b"k").unwrap();
    assert_eq!(cloned.0.as_bytes(), b"v");
    assert_eq!(cloned.1, Some(expire));
    assert!(db.clone_entry(b"expired").is_none());

    let mut seen = Vec::new();
    db.for_each_entry(|key, val, ttl| {
        seen.push((key.to_vec(), val.as_bytes().to_vec(), ttl));
    });
    assert_eq!(seen, vec![(b"k".to_vec(), b"v".to_vec(), Some(expire))]);

    let taken = db.take(b"k").unwrap();
    assert_eq!(taken.0.as_bytes(), b"v");
    assert_eq!(taken.1, Some(expire));
    assert!(taken.2.is_empty());
    assert_eq!(db.dbsize(), 1);
    assert!(db.take(b"expired").is_none());
    assert_eq!(db.expired_deletions, vec![b"expired".to_vec()]);
}

#[test]
fn relocate_entry_rebuilds_raw_string_without_losing_key_metadata() {
    let mut db = Db::new();
    let expire = future_ms();
    let value = vec![b'x'; 128];
    db.load_entry(
        b"key".to_vec(),
        Robj::string_raw(bytes::Bytes::from(value)),
        Some(expire),
    );
    db.set_lru_idle(b"key", 7);
    let before = db.get_no_expire(b"key").unwrap().as_bytes().as_ptr();

    assert!(db.relocate_entry(b"key"));

    let after = db.get_no_expire(b"key").unwrap().as_bytes().as_ptr();
    assert_ne!(before, after);
    assert_eq!(db.get_expire(b"key"), Some(expire));
    assert!(db.get_lru_idle(b"key") >= 7);
}

#[test]
fn scan_variants_and_random_key_respect_expire_modes() {
    let mut db = Db::new();
    db.set(b"a".to_vec(), Robj::string("1"));
    db.set(b"b".to_vec(), Robj::string("2"));
    db.set_expire(b"b", past_ms());

    let (_cursor, keys) = db.scan(0, 10);
    assert_eq!(keys, vec![b"a".to_vec()]);

    db.set(b"b".to_vec(), Robj::string("2"));
    db.set_expire(b"b", past_ms());
    let (_cursor, keys) = db.scan_include_expired(0, 10);
    assert_eq!(keys.len(), 2);
    assert_eq!(db.all_keys_sorted(), vec![b"a".to_vec(), b"b".to_vec()]);

    db.set_no_lazy_expire(true);
    assert!(db.random_key().is_some());
}

#[test]
fn random_key_collects_all_expired_keys_for_propagation() {
    let mut db = Db::new();
    db.set(b"a".to_vec(), Robj::string("1"));
    db.set(b"b".to_vec(), Robj::string("2"));
    db.set_expire(b"a", past_ms());
    db.set_expire(b"b", past_ms());

    assert!(db.random_key().is_none());

    let mut expired = db.expired_deletions.clone();
    expired.sort();
    assert_eq!(expired, vec![b"a".to_vec(), b"b".to_vec()]);
}

#[test]
fn random_key_reads_nonzero_shard_bucket() {
    let mut db = Db::new_with_slot_router(2, first_byte_slot);
    db.set(vec![1, b'a'], Robj::string("value"));

    assert_eq!(db.random_key(), Some(&[1, b'a'][..]));
}

#[test]
fn module_values_and_key_metadata_follow_key_lifecycle() {
    let mut db = Db::new();
    db.set(b"k".to_vec(), Robj::string("v"));
    db.set_lru_idle(b"k", 11);
    assert_eq!(db.get_lru_idle(b"k"), 11);
    assert_eq!(db.get_lfu_freq(b"k"), 0);
    db.set_lfu_freq(b"k", 9);
    assert_eq!(db.get_lfu_freq(b"k"), 9);
    assert_eq!(db.get_lru_idle(b"k"), 0);

    db.set_module_value(b"k".to_vec(), 7, ptr::null_mut());
    assert!(db.is_module_key(b"k"));
    assert_eq!(db.get_module_value(b"k").unwrap().0, 7);
    assert!(db.remove_module_value(b"k"));
    assert!(!db.remove_module_value(b"k"));

    db.set_module_value(b"k".to_vec(), 8, ptr::null_mut());
    assert!(db.del(b"k"));
    assert!(!db.is_module_key(b"k"));
    assert_eq!(db.get_lfu_freq(b"k"), 0);
}

#[test]
fn dbset_swaps_flushes_and_drains_expired_deletions() {
    let mut dbs = DbSet::new(2);
    assert_eq!(dbs.num_dbs(), 2);
    dbs.get_mut(0)
        .unwrap()
        .set(b"a".to_vec(), Robj::string("0"));
    dbs.get_mut(1)
        .unwrap()
        .set(b"b".to_vec(), Robj::string("1"));

    dbs.swap(0, 1);
    assert!(dbs.get_mut(0).unwrap().get(b"b").is_some());
    assert_eq!(dbs.db_size(1), 1);
    dbs.swap(0, 99);
    assert!(dbs.get(99).is_none());

    dbs.get_mut(0).unwrap().set_expire(b"b", past_ms());
    assert!(!dbs.get_mut(0).unwrap().exists(b"b"));
    assert_eq!(dbs.drain_expired_deletions(), vec![(0, b"b".to_vec())]);
    assert!(dbs.drain_expired_deletions().is_empty());

    dbs.flush_all();
    assert_eq!(dbs.db_size(0), 0);
    assert_eq!(dbs.db_size(1), 0);
    dbs.clear_expired_deletions();
}

#[test]
fn slot_operations_cover_all_databases_and_preserve_metadata() {
    let mut dbs = DbSet::new_with_slot_router(2, 2, first_byte_slot);
    let expire = future_ms();
    dbs.get_mut(0)
        .unwrap()
        .load_entry(vec![1, b'a'], Robj::string("zero"), Some(expire));
    dbs.get_mut(1)
        .unwrap()
        .load_entry(vec![1, b'b'], Robj::string("one"), None);
    dbs.get_mut(1)
        .unwrap()
        .load_entry(vec![2, b'c'], Robj::string("other"), None);

    assert_eq!(dbs.count_keys_in_slot(1), 2);
    assert_eq!(
        dbs.get_keys_in_slot(1, 10),
        vec![vec![1, b'a'], vec![1, b'b']]
    );

    let taken = dbs.take_slot(1);
    assert_eq!(taken.len(), 2);
    assert_eq!(taken[0].db_id, 0);
    assert_eq!(taken[0].expire, Some(expire));
    assert_eq!(taken[1].db_id, 1);
    assert_eq!(dbs.count_keys_in_slot(1), 0);
    assert_eq!(dbs.count_keys_in_slot(2), 1);

    dbs.restore_slot_entries(taken);
    assert_eq!(dbs.count_keys_in_slot(1), 2);
    assert_eq!(dbs.get(0).unwrap().get_expire(&[1, b'a']), Some(expire));
}

#[test]
fn take_slot_preserves_hash_field_expirations() {
    let mut dbs = DbSet::new_with_slot_router(2, 2, first_byte_slot);
    let expire = future_ms();
    let db = dbs.get_mut(1).unwrap();
    db.get_or_create_hash(&[1, b'h'])
        .unwrap()
        .insert(Bytes::from_static(b"field"), Bytes::from_static(b"value"));
    db.set_hash_field_expire(&[1, b'h'], b"field", expire);

    let taken = dbs.take_slot(1);
    assert_eq!(taken.len(), 1);
    assert_eq!(
        taken[0].hash_field_expires.get(b"field".as_slice()),
        Some(&expire)
    );

    dbs.restore_slot_entries(taken);
    assert_eq!(
        dbs.get(1)
            .unwrap()
            .get_hash_field_expire(&[1, b'h'], b"field"),
        Some(expire)
    );
    assert_eq!(dbs.delete_slot(1), 1);
    assert_eq!(dbs.count_keys_in_slot(1), 0);
}

#[test]
fn take_slot_removes_expired_entries_physically() {
    let mut dbs = DbSet::new_with_slot_router(1, 2, first_byte_slot);
    let db = dbs.get_mut(0).unwrap();
    db.load_entry(vec![1, b'a'], Robj::string("expired"), Some(past_ms()));

    let taken = dbs.take_slot(1);

    assert!(taken.is_empty());
    assert_eq!(dbs.db_size(0), 0);
}

#[test]
fn flush_discards_pending_expired_deletions() {
    let mut db = Db::new();
    db.set(b"expired".to_vec(), Robj::string("v"));
    db.set_expire(b"expired", past_ms());
    assert!(!db.exists(b"expired"));
    assert_eq!(db.expired_deletions, vec![b"expired".to_vec()]);

    db.flush();

    assert!(db.expired_deletions.is_empty());
}

#[test]
fn snapshot_handoff_shares_only_large_values_at_the_boundary() {
    let mut db = Db::new();
    db.set(
        b"large".to_vec(),
        Robj::ListQuicklist(ListValue::from_vecdeque(
            (0..8_192).map(|_| Bytes::from_static(b"value")).collect(),
        )),
    );
    db.set(b"small".to_vec(), Robj::string("value"));

    let large = db.snapshot_value_for_background(b"large", 1_024).unwrap();
    let small = db.snapshot_value_for_background(b"small", 1_024).unwrap();

    assert!(
        large.is_shared(),
        "large values must be shared only at handoff"
    );
    assert!(!small.is_shared(), "small values must remain owned copies");
    let live = db.get_allow_expired(b"large").unwrap();
    assert_eq!(
        large.collection_storage_ptr(),
        collection_storage_ptr(live),
        "handoff must not copy the large collection"
    );
}

fn collection_storage_ptr(value: &Robj) -> *const () {
    match value {
        Robj::ListQuicklist(list) => list.as_slices().0.as_ptr().cast(),
        _ => std::ptr::null(),
    }
}
