use super::*;
use proptest::prelude::*;
use std::collections::BTreeMap;

type Reference = BTreeMap<Vec<u8>, BTreeMap<Vec<u8>, Vec<u8>>>;

#[test]
fn path_hash_db_mutation_tracks_deltas_and_preserves_metadata() {
    use crate::foundation::io::store::kv_engine::{now_ms, Db};
    use crate::foundation::io::store::Robj;
    use crate::foundation::memory::metrics::memory_stats::{self, MemoryCategory};

    let _guard = memory_stats::lock_global_for_test();
    let tracker = memory_stats::get_tracker();
    for is_lfu in [false, true] {
        let mut db = Db::new();
        let pairs = [(Bytes::from_static(b"f"), Bytes::from_static(b"v"))];
        let mut hash = PathHash::new();
        hash.set_pairs(b"", &pairs, 0, 0);
        db.set(b"h".to_vec(), Robj::PathHash(Box::new(hash)));
        let expiry_ms = now_ms() + 1_000_000;
        db.set_expire(b"h", expiry_ms);
        if is_lfu {
            db.set_lfu_freq(b"h", 201);
        } else {
            db.set_lru_idle(b"h", 17);
        }
        let lru = db.get_lru_raw(b"h");
        let lfu = db.get_lfu_freq(b"h");
        let address = std::ptr::from_ref(db.get_path_hash(b"h").unwrap().unwrap());
        for operation in 0..5 {
            let old_size = db.get_path_hash(b"h").unwrap().unwrap().memory_usage();
            let old_dataset = tracker.get(MemoryCategory::Dataset);
            let result = db
                .mutate_path_hash(b"h", false, false, |hash| match operation {
                    0 => {
                        for index in 0..600 {
                            hash.set_pairs(format!("p{index}").as_bytes(), &pairs, 0, 0);
                        }
                        600
                    }
                    1 => usize::from(hash.delete_path(b"missing")),
                    2 => hash.delete_fields(b"p0", &[Bytes::from_static(b"f")]),
                    3 => hash.delete_prefix(b"p"),
                    _ => usize::from(hash.delete_path(b"")),
                })
                .unwrap()
                .unwrap();
            assert_eq!(result, [600, 0, 1, 599, 1][operation]);
            let hash = db.get_path_hash(b"h").unwrap().unwrap();
            assert_eq!(std::ptr::from_ref(hash), address);
            let new_size = hash.memory_usage();
            let new_dataset = tracker.get(MemoryCategory::Dataset);
            if new_size >= old_size {
                assert_eq!(new_dataset, old_dataset + new_size - old_size);
            } else {
                assert_eq!(new_dataset, old_dataset - (old_size - new_size));
            }
            assert_eq!(db.get_expire(b"h"), Some(expiry_ms));
            assert_eq!(db.get_lru_raw(b"h"), lru);
            assert_eq!(db.get_lfu_freq(b"h"), lfu);
        }
        let (copy, expiry, _) = db.clone_entry(b"h").unwrap();
        assert_eq!(expiry, Some(expiry_ms));
        db.load_entry(b"copy".to_vec(), copy, expiry);
        db.mutate_path_hash(b"h", false, false, |hash| {
            hash.set_pairs(b"new", &pairs, 0, 0)
        })
        .unwrap();
        assert!(db.get_path_hash(b"copy").unwrap().unwrap().is_empty());
        assert_eq!(
            db.mutate_path_hash(b"missing", false, false, |_| 1)
                .unwrap(),
            None
        );
        db.set(b"string".to_vec(), Robj::string("value"));
        assert!(db.mutate_path_hash(b"string", false, false, |_| 1).is_err());
    }
}

#[test]
fn path_hash_snapshot_clone_keeps_index_and_accounting_independent() {
    use crate::foundation::io::store::kv_engine::Db;
    use crate::foundation::io::store::Robj;

    let mut db = Db::new();
    let pairs = [(Bytes::from_static(b"f"), Bytes::from_static(b"v"))];
    db.mutate_path_hash(b"h", true, false, |hash| {
        for path in [b"".as_slice(), b"bb", b"bbbb"] {
            hash.set_pairs(path, &pairs, 0, 0);
        }
    })
    .unwrap();
    let snapshot = db.snapshot_value_for_background(b"h", 1).unwrap();
    assert!(snapshot.is_shared());
    db.mutate_path_hash(b"h", false, false, |hash| hash.delete_prefix(b"b"))
        .unwrap();
    let Robj::PathHash(original) = snapshot.as_ref() else {
        panic!("expected path hash snapshot");
    };
    assert_eq!(original.prefix_matches(b"bbbbx", None).count(), 3);
    assert_eq!(
        db.get_path_hash(b"h")
            .unwrap()
            .unwrap()
            .prefix_matches(b"bbbbx", None)
            .count(),
        1
    );
    original.view.paths.assert_accounting();
    original.index.assert_structure(3);
}

fn assert_consistent(hash: &PathHash, reference: &Reference, query: &[u8]) {
    hash.view.paths.assert_accounting();
    hash.index.assert_structure(reference.len());
    let mut actual = BTreeMap::new();
    for (path, payload) in hash.iter() {
        payload.assert_accounting();
        actual.insert(
            path.to_vec(),
            payload
                .entries()
                .into_iter()
                .map(|(f, v)| (f.to_vec(), v.to_vec()))
                .collect(),
        );
    }
    assert_eq!(&actual, reference);
    for limit in [None, Some(0), Some(query.len() / 2), Some(usize::MAX)] {
        let expected: Vec<_> = reference
            .keys()
            .rev()
            .filter(|path| query.starts_with(path) && path.len() <= limit.unwrap_or(usize::MAX))
            .cloned()
            .collect();
        let matches: Vec<_> = hash
            .prefix_matches(query, limit)
            .map(|(path, payload)| {
                assert_eq!(payload, hash.get(path).unwrap());
                path.to_vec()
            })
            .collect();
        assert_eq!(matches, expected);
    }
}

#[test]
fn path_hash_chunks_pruning_checked_insert_and_clone() {
    let mut hash = PathHash::new();
    let empty_size = hash.memory_usage();
    let pairs = [(Bytes::from_static(b"f"), Bytes::from_static(b"v"))];
    let mut reference = Reference::new();
    for index in 0..600 {
        let path = format!("prefix/{index:04}").into_bytes();
        hash.set_pairs(&path, &pairs, 2, 64);
        reference.insert(path, BTreeMap::from([(b"f".to_vec(), b"v".to_vec())]));
    }
    hash.set_pairs(b"", &pairs, 2, 64);
    reference.insert(Vec::new(), BTreeMap::from([(b"f".to_vec(), b"v".to_vec())]));
    assert_consistent(&hash, &reference, b"prefix/0599");
    let cloned = hash.clone();
    assert_eq!(cloned.memory_usage(), hash.memory_usage());
    assert_consistent(&cloned, &reference, b"prefix/0599");
    let size = hash.memory_usage();
    assert!(!hash.insert_checked(Bytes::new(), PathHashPayload::default()));
    assert!(!hash.insert_checked(Bytes::new(), hash.get(b"").unwrap().clone()));
    assert!(!hash.delete_path(b"missing"));
    assert_eq!(
        hash.delete_fields(b"missing", &[Bytes::from_static(b"f")]),
        0
    );
    assert_eq!(hash.delete_prefix(b"absent"), 0);
    assert_eq!(hash.memory_usage(), size);
    assert_eq!(hash.delete_prefix(b"prefix/"), 600);
    reference.retain(|path, _| !path.starts_with(b"prefix/"));
    assert_consistent(&hash, &reference, b"prefix/0599");
    assert_eq!(
        hash.delete_fields(b"", &[Bytes::from_static(b"f"), Bytes::from_static(b"f")]),
        1
    );
    assert_eq!(hash.memory_usage(), empty_size);
    assert!(cloned.contains_path(b"prefix/0599"));
    hash.set_pairs(b"new", &pairs, 0, 0);
    assert_eq!(hash.delete_prefix(b""), 1);
    assert_eq!(hash.memory_usage(), empty_size);
}

#[test]
fn path_hash_compresses_long_edges_and_drops_deep_chains_iteratively() {
    let pairs = [(Bytes::from_static(b"f"), Bytes::from_static(b"v"))];
    let mut hash = PathHash::new();
    let long_path = vec![b'b'; 1_000_000];
    hash.set_pairs(&long_path, &pairs, 512, 64);
    hash.index.assert_structure(1);
    assert_eq!(
        hash.prefix_matches(&long_path, None)
            .next()
            .unwrap()
            .0
            .len(),
        long_path.len()
    );
    assert!(hash
        .prefix_matches(&long_path[..999_999], None)
        .next()
        .is_none());
    hash.delete_path(&long_path);
    for length in 0..4_096 {
        hash.set_pairs(&long_path[..length], &pairs, 512, 64);
    }
    hash.index.assert_structure(4_096);
    let mut cloned = hash.clone();
    for length in (0..4_096).step_by(2) {
        cloned.delete_path(&long_path[..length]);
    }
    cloned.index.assert_structure(2_048);
    assert_eq!(cloned.prefix_matches(&long_path, None).count(), 2_048);
    drop(hash);
    drop(cloned);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn path_hash_index_and_accounting_match_reference(
        operations in prop::collection::vec(
            (0u8..7, prop::collection::vec(0u8..4, 0..12),
             prop::collection::vec(0u8..4, 0..5),
             prop::collection::vec(any::<u8>(), 0..80),
             prop::collection::vec(0u8..4, 0..16)),
            1..100
        )
    ) {
        let mut hash = PathHash::new();
        let mut reference = Reference::new();
        for (operation, path, field, value, query) in operations {
            match operation {
                0 | 1 => {
                    hash.set_pairs(&path, &[(Bytes::from(field.clone()), Bytes::from(value.clone()))], 2, 16);
                    reference.entry(path.clone()).or_default().insert(field, value);
                }
                2 => {
                    assert_eq!(hash.delete_path(&path), reference.remove(&path).is_some());
                }
                3 => {
                    let expected = reference.get_mut(&path)
                        .map_or(0, |payload| usize::from(payload.remove(&field).is_some()));
                    if reference.get(&path).is_some_and(BTreeMap::is_empty) {
                        reference.remove(&path);
                    }
                    assert_eq!(hash.delete_fields(&path, &[Bytes::from(field.clone()), Bytes::from(field)]), expected);
                }
                4 => {
                    let old_len = reference.len();
                    reference.retain(|stored, _| !stored.starts_with(&path));
                    assert_eq!(hash.delete_prefix(&path), old_len - reference.len());
                }
                5 => {
                    let mut payload = PathHashPayload::default();
                    payload.set_pairs(&[(Bytes::from(field.clone()), Bytes::from(value.clone()))], 2, 16);
                    let is_new = !reference.contains_key(&path);
                    assert_eq!(hash.insert_checked(Bytes::from(path.clone()), payload), is_new);
                    if is_new {
                        reference.insert(path.clone(), BTreeMap::from([(field, value)]));
                    }
                }
                _ => {
                    let mut cloned = hash.clone();
                    assert_eq!(cloned.memory_usage(), hash.memory_usage());
                    assert_consistent(&cloned, &reference, &query);
                    cloned.delete_prefix(&path);
                    assert_consistent(&hash, &reference, &query);
                }
            }
            assert_consistent(&hash, &reference, &query);
            assert_consistent(&hash, &reference, &path);
        }
    }
}

#[test]
fn path_hash_write_growth() {
    for count in [2_000, 4_000, 8_000] {
        let mut hash = PathHash::new();
        let pairs = [(Bytes::from_static(b"f"), Bytes::from_static(b"v"))];
        let start = std::time::Instant::now();
        for index in 0..count {
            std::hint::black_box(hash.memory_usage());
            hash.set_pairs(format!("p{index:08}").as_bytes(), &pairs, 512, 64);
            std::hint::black_box(hash.memory_usage());
        }
        eprintln!("path writes: {count}, elapsed: {:?}", start.elapsed());
    }
}
