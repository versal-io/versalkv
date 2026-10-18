use super::*;
use bytes::Bytes;

fn key(i: usize) -> Bytes {
    Bytes::from(format!("{i}"))
}

#[test]
fn insert_get_remove_roundtrip() {
    let mut ht: HashTable<i64> = HashTable::new();
    assert!(ht.is_empty());
    assert_eq!(ht.insert(key(1), 10), None);
    assert_eq!(ht.insert(key(2), 20), None);
    assert_eq!(ht.insert(key(1), 11), Some(10));
    assert_eq!(ht.len(), 2);
    assert_eq!(ht.get(b"1"), Some(&11));
    assert_eq!(ht.get(b"2"), Some(&20));
    assert_eq!(ht.get(b"3"), None);
    assert_eq!(ht.remove(b"1"), Some(11));
    assert_eq!(ht.remove(b"1"), None);
    assert_eq!(ht.len(), 1);
}

#[test]
fn iter_returns_all_entries() {
    let mut ht: HashTable<usize> = HashTable::new();
    for i in 0..1000 {
        ht.insert(key(i), i);
    }
    for i in 0..1000 {
        assert_eq!(ht.get(key(i).as_ref()), Some(&i));
    }
    let mut seen: Vec<usize> = ht.values().copied().collect();
    seen.sort_unstable();
    assert_eq!(seen, (0..1000).collect::<Vec<_>>());
    assert_eq!(ht.len(), 1000);
}

#[test]
fn grows_to_expected_bucket_count() {
    let mut ht: HashTable<usize> = HashTable::new();
    for i in 0..20000 {
        ht.insert(key(i), i);
    }
    for i in 0..20000 {
        let _ = ht.get(key(i).as_ref());
    }
    let s = ht.stats(0, true);
    assert_eq!(ht.len(), 20000);
    assert_eq!(s.toplevel_buckets, 4096, "20000 entries -> 4096 buckets");
}

#[test]
fn shrink_starts_rehash_to_minimal_ht1() {
    set_can_abort_shrink(false);
    let mut ht: HashTable<usize> = HashTable::new();
    for i in 0..20000 {
        ht.insert(key(i), i);
    }
    for i in 0..20000 {
        let _ = ht.get(key(i).as_ref());
    }
    assert_eq!(ht.stats(0, true).toplevel_buckets, 4096);

    ht.pause_auto_shrink();
    for i in 1..20000 {
        ht.remove(key(i).as_ref());
    }
    ht.resume_auto_shrink();
    assert_eq!(ht.len(), 1);
    assert!(ht.is_rehashing(), "shrink should start rehashing");

    let ht0 = ht.stats(0, true);
    let ht1 = ht.stats(1, true);
    assert_eq!(ht0.used, 1, "ht0 has 1 entry");
    assert_eq!(ht.rehash_index(), 0, "rehash just started at index 0");
    assert_eq!(ht1.size, ENTRIES_PER_BUCKET, "ht1 minimal table size = 7");
    assert_eq!(ht1.used, 0, "ht1 has 0 entries");

    for i in 0..4096 {
        ht.insert(key(100000 + i), i);
        if !ht.is_rehashing() {
            break;
        }
    }
    assert!(!ht.is_rehashing(), "rehash should finish");
    set_can_abort_shrink(true);
}

#[test]
fn zset_test1_full_sequence_completes_rehash() {
    set_can_abort_shrink(false);
    let mut ht: HashTable<usize> = HashTable::new();
    for i in 0..20000 {
        ht.insert(key(i), i);
    }
    assert_eq!(ht.stats(0, false).toplevel_buckets, 4096);

    ht.pause_auto_shrink();
    for i in 1..20000 {
        ht.remove(key(i).as_ref());
    }
    ht.resume_auto_shrink();
    assert!(ht.is_rehashing());
    assert_eq!(ht.rehash_index(), 0);
    assert_eq!(ht.stats(1, false).size, ENTRIES_PER_BUCKET);

    for i in 0..2000 {
        ht.insert(key(i), i);
    }
    set_can_abort_shrink(true);
    assert!(
        !ht.is_rehashing(),
        "final rehash must complete within 2000 writes"
    );
}
