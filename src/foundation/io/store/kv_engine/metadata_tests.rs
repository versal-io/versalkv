use super::{Db, EvictionPolicy, Robj};
use std::ptr;

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
    db.initialize_lfu_freq(b"k", 5);
    assert_eq!(db.get_lfu_freq(b"k"), 9);

    db.set(b"fresh".to_vec(), Robj::string("v"));
    db.initialize_lfu_freq(b"fresh", 5);
    assert_eq!(db.get_lfu_freq(b"fresh"), 5);

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
fn ordinary_set_initializes_lru_metadata_without_a_second_key_index() {
    assert!(std::mem::size_of::<super::KeyMeta>() <= 8);
    let mut db = Db::new();

    db.set(b"plain".to_vec(), Robj::string("value"));

    assert_ne!(db.get_lru_raw(b"plain"), 0);
    assert_eq!(db.get_lru_idle(b"plain"), 0);
    assert_eq!(db.get_lfu_freq(b"plain"), 0);

    db.set_lru_idle(b"plain", 7);

    assert_eq!(db.get_lru_idle(b"plain"), 7);
}

#[test]
fn lfu_policy_initializes_new_keys_at_frequency_five() {
    let mut db = Db::new();
    db.set_lfu_mode(true);

    db.set(b"lfu".to_vec(), Robj::string("value"));

    assert_eq!(db.get_lfu_freq(b"lfu"), 5);
    assert_eq!(db.get_lru_idle(b"lfu"), 0);

    assert!(db.touch_access_time(b"lfu"));
    assert!(db.get_lfu_freq(b"lfu") >= 5);
    assert_eq!(db.get_lru_idle(b"lfu"), 0);
}

#[test]
fn eviction_scores_order_lru_lfu_and_ttl_candidates() {
    let mut db = Db::new();
    db.set(b"older".to_vec(), Robj::string("v"));
    db.set(b"newer".to_vec(), Robj::string("v"));
    db.set_lru_idle(b"older", 100);
    db.set_lru_idle(b"newer", 1);
    assert!(
        db.eviction_score(b"older", EvictionPolicy::AllkeysLru)
            > db.eviction_score(b"newer", EvictionPolicy::AllkeysLru)
    );

    db.set_lfu_freq(b"older", 2);
    db.set_lfu_freq(b"newer", 20);
    assert!(
        db.eviction_score(b"older", EvictionPolicy::AllkeysLfu)
            > db.eviction_score(b"newer", EvictionPolicy::AllkeysLfu)
    );

    let now = super::now_ms();
    db.set_expire(b"older", now + 100);
    db.set_expire(b"newer", now + 10_000);
    assert!(
        db.eviction_score(b"older", EvictionPolicy::VolatileTtl)
            > db.eviction_score(b"newer", EvictionPolicy::VolatileTtl)
    );
}

#[test]
fn stores_short_database_keys_inline() {
    let mut db = Db::new();
    for index in 0..1_000 {
        db.set(
            format!("key:{index}").into_bytes(),
            Robj::string(vec![b'a'; index % 32]),
        );
    }

    assert_eq!(db.dbsize(), 1_000);
    assert!(db.data.keys().all(|key| matches!(
        key,
        crate::foundation::io::store::structure::string::StoredKey::Inline(_)
    )));
}
