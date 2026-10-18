use proptest::prelude::*;
use versal::foundation::io::store::structure::dict::Dict;

proptest! {
    #[test]
    fn dict_len_matches_distinct_insertions(
        keys in proptest::collection::hash_set("[a-z]{1,8}", 0..100)
    ) {
        let mut dict = Dict::new();
        for (index, key) in keys.iter().enumerate() {
            dict.insert(key.clone(), index);
        }

        prop_assert_eq!(dict.len(), keys.len());
    }

    #[test]
    fn dict_removing_all_inserted_keys_leaves_it_empty(
        keys in proptest::collection::hash_set("[a-z]{1,8}", 1..100)
    ) {
        let mut dict = Dict::new();
        for (index, key) in keys.iter().enumerate() {
            dict.insert(key.clone(), index);
        }
        for key in &keys {
            prop_assert!(dict.remove(key).is_some());
        }

        prop_assert!(dict.is_empty());
        prop_assert_eq!(dict.len(), 0);
    }

    #[test]
    fn dict_iteration_visits_every_inserted_key_once(
        keys in proptest::collection::hash_set("[a-z]{1,8}", 0..100)
    ) {
        let mut dict = Dict::new();
        for (index, key) in keys.iter().enumerate() {
            dict.insert(key.clone(), index);
        }

        prop_assert_eq!(dict.iter().count(), dict.len());
        for key in &keys {
            prop_assert!(dict.contains_key(key));
        }
    }

    #[test]
    fn dict_insert_remove_during_rehash_matches_hashmap(
        ops in proptest::collection::vec(
            prop_oneof![
                3 => (Just(0u8), "[a-z]{1,8}", 0usize..10000usize),
                2 => (Just(1u8), "[a-z]{1,8}", Just(0usize)),
                4 => (Just(2u8), "[a-z]{1,8}", Just(0usize)),
            ],
            0..500,
        ),
    ) {
        let mut dict: Dict<String, usize> = Dict::new();
        let mut reference: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();

        for (kind, key, value) in ops {
            match kind {
                0 => {
                    let old_dict = dict.insert(key.clone(), value);
                    let old_ref = reference.insert(key, value);
                    prop_assert_eq!(old_dict, old_ref);
                }
                1 => {
                    prop_assert_eq!(dict.remove(&key), reference.remove(&key));
                }
                2 => {
                    prop_assert_eq!(dict.get(&key), reference.get(&key));
                }
                _ => unreachable!(),
            }
            prop_assert_eq!(dict.len(), reference.len());
        }

        for (k, v) in &reference {
            prop_assert_eq!(dict.get(k), Some(v));
        }
        prop_assert_eq!(dict.iter().count(), reference.len());
    }

    #[test]
    fn dict_scan_covers_all_keys_during_rehash(
        keys in proptest::collection::hash_set("[a-z]{1,8}", 1..200),
    ) {
        let mut dict: Dict<String, usize> = Dict::new();
        for (idx, k) in keys.iter().enumerate() {
            dict.insert(k.clone(), idx);
        }
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut cursor = 0u64;
        loop {
            let mut batch: Vec<String> = Vec::new();
            let next = dict.scan(cursor, |k, _v| batch.push(k.clone()));
            seen.extend(batch);
            cursor = next;
            if cursor == 0 { break; }
        }
        for k in &keys {
            prop_assert!(seen.contains(k), "key {} missed by scan during rehash", k);
        }
    }

    #[test]
    fn dict_drain_then_mixed_ops_matches_hashmap(
        ops_before in proptest::collection::vec(
            prop_oneof![
                3 => (Just(0u8), "[a-z]{1,8}", 0usize..10000usize),
                1 => (Just(1u8), "[a-z]{1,8}", Just(0usize)),
            ],
            1..300,
        ),
        ops_after in proptest::collection::vec(
            prop_oneof![
                3 => (Just(0u8), "[a-z]{1,8}", 0usize..10000usize),
                2 => (Just(1u8), "[a-z]{1,8}", Just(0usize)),
                2 => (Just(2u8), "[a-z]{1,8}", Just(0usize)),
            ],
            0..500,
        ),
    ) {
        let mut dict: Dict<String, usize> = Dict::new();
        let mut reference: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();

        for (kind, key, value) in ops_before {
            match kind {
                0 => { dict.insert(key.clone(), value); reference.insert(key, value); }
                1 => { dict.remove(&key); reference.remove(&key); }
                _ => unreachable!(),
            }
        }

        let partial: Vec<_> = dict.drain().take(7).collect();
        let _ = partial;
        let _fully_drained: Vec<_> = dict.drain().collect();
        reference.clear();
        prop_assert!(dict.is_empty(), "dict not empty after full drain: len={}", dict.len());
        prop_assert!(!dict.is_rehashing(), "dict still rehashing after full drain");
        let bc = dict.bucket_count();
        prop_assert!(bc == 0 || bc.is_power_of_two(),
            "post-drain bucket_count={} neither 0 nor power of 2", bc);

        for (kind, key, value) in ops_after {
            match kind {
                0 => {
                    let old_dict = dict.insert(key.clone(), value);
                    let old_ref = reference.insert(key, value);
                    prop_assert_eq!(old_dict, old_ref);
                }
                1 => {
                    prop_assert_eq!(dict.remove(&key), reference.remove(&key));
                }
                2 => {
                    prop_assert_eq!(dict.get(&key), reference.get(&key));
                }
                _ => unreachable!(),
            }
            prop_assert_eq!(dict.len(), reference.len());
        }
        for (k, v) in &reference {
            prop_assert_eq!(dict.get(k), Some(v));
        }
    }

    #[test]
    fn dict_clear_then_refill_matches_hashmap(
        seed in proptest::collection::hash_set("[a-z]{1,8}", 1..200),
        refill in proptest::collection::vec(
            (Just(0u8), "[a-z]{1,8}", 0usize..10000usize),
            0..300,
        ),
    ) {
        let mut dict: Dict<String, usize> = Dict::new();
        let mut reference: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        for (idx, k) in seed.iter().enumerate() {
            dict.insert(k.clone(), idx);
            reference.insert(k.clone(), idx);
        }
        dict.clear();
        reference.clear();
        prop_assert!(dict.is_empty());
        for (_kind, key, value) in refill {
            let old_dict = dict.insert(key.clone(), value);
            let old_ref = reference.insert(key, value);
            prop_assert_eq!(old_dict, old_ref);
        }
        prop_assert_eq!(dict.len(), reference.len());
        for (k, v) in &reference {
            prop_assert_eq!(dict.get(k), Some(v));
        }
        prop_assert_eq!(dict.iter().count(), reference.len());
    }

    #[test]
    fn dict_resize_and_size_jitter_leaves_consistent_bucket_count(
        keys in proptest::collection::hash_set("[a-z]{1,10}", 100..500),
        extra in proptest::collection::hash_set("[a-z]{1,10}", 50..200),
    ) {
        let mut dict: Dict<String, usize> = Dict::new();
        let mut reference: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        for (idx, k) in keys.iter().enumerate() {
            dict.insert(k.clone(), idx);
            reference.insert(k.clone(), idx);
        }
        dict.try_resize();

        let n_delete = keys.len() / 2;
        let ks: Vec<String> = keys.iter().take(n_delete).cloned().collect();
        for k in &ks {
            dict.remove(k);
            reference.remove(k);
        }
        dict.try_resize();

        for (idx, k) in extra.iter().enumerate() {
            dict.insert(k.clone(), idx + keys.len());
            reference.insert(k.clone(), idx + keys.len());
        }
        dict.try_resize();

        prop_assert_eq!(dict.len(), reference.len());
        let bc = dict.bucket_count();
        prop_assert!(bc.is_power_of_two() || bc == 0,
            "bucket_count={} not a power of two", bc);
        // C hashtable buckets hold seven inline entries on 64-bit targets.
        // The top-level bucket count must therefore provide at least that
        // much physical capacity for every live entry.
        prop_assert!(bc == 0 || bc * 7 >= dict.len(),
            "bucket_count={} too small for len={}", bc, dict.len());
        let mut seen = 0usize;
        for (k, v) in dict.iter() {
            prop_assert_eq!(reference.get(k), Some(v));
            seen += 1;
        }
        prop_assert_eq!(seen, reference.len());
    }
}
