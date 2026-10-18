use proptest::prelude::*;
use versal::foundation::io::store::structure::set::IntSet;

proptest! {
    #[test]
    fn intset_len_matches_distinct_values(
        values in proptest::collection::vec(any::<i64>(), 0..200)
    ) {
        let mut s = IntSet::new();
        let mut expected = std::collections::BTreeSet::new();
        for v in &values {
            let added = s.add(*v, 1000).unwrap_or(true);
            let was_new = expected.insert(*v);
            prop_assert_eq!(added, was_new);
        }
        prop_assert_eq!(s.len(), expected.len());
        prop_assert_eq!(s.is_empty(), expected.is_empty());
    }

    #[test]
    fn intset_contains_all_inserted_values(
        values in proptest::collection::hash_set(any::<i64>(), 0..200)
    ) {
        let mut s = IntSet::new();
        for v in &values {
            s.add(*v, 1000).unwrap();
        }
        for v in &values {
            prop_assert!(s.contains(*v));
        }
        // Non-members should not be present
        for v in 0..100i64 {
            if !values.contains(&v) {
                prop_assert!(!s.contains(v));
            }
        }
    }

    #[test]
    fn intset_elements_always_sorted(
        values in proptest::collection::vec(any::<i64>(), 0..200)
    ) {
        let mut s = IntSet::new();
        for v in &values {
            s.add(*v, 1000).unwrap_or(true);
        }
        let collected: Vec<i64> = s.iter().collect();
        let mut sorted = collected.clone();
        sorted.sort();
        prop_assert_eq!(&collected, &sorted);
        // Also no duplicates
        let deduped: std::collections::BTreeSet<_> = collected.iter().collect();
        prop_assert_eq!(deduped.len(), collected.len());
    }

    #[test]
    fn intset_remove_removes_only_target(
        values in proptest::collection::hash_set(any::<i64>(), 1..200)
    ) {
        let mut s = IntSet::new();
        let mut expected: std::collections::BTreeSet<_> = std::collections::BTreeSet::new();
        for v in &values {
            s.add(*v, 1000).unwrap();
            expected.insert(*v);
        }
        // Remove first value
        let first = *expected.iter().next().unwrap();
        prop_assert!(s.remove(first));
        expected.remove(&first);
        prop_assert_eq!(s.len(), expected.len());
        prop_assert!(!s.contains(first));
        for v in &expected {
            prop_assert!(s.contains(*v));
        }
    }

    #[test]
    fn intset_get_by_index_matches_iter(
        values in proptest::collection::hash_set(any::<i64>(), 0..200)
    ) {
        let mut s = IntSet::new();
        for v in &values {
            s.add(*v, 1000).unwrap();
        }
        let collected: Vec<i64> = s.iter().collect();
        for (i, v) in collected.iter().enumerate() {
            prop_assert_eq!(s.get(i), Some(*v));
        }
        prop_assert_eq!(s.get(values.len()), None);
    }

    #[test]
    fn intset_upgrades_when_exceeding_max_entries(
        values in proptest::collection::vec(any::<i64>(), 10..100)
    ) {
        let mut s = IntSet::new();
        let max = 5;
        let mut added_count = 0;
        let mut upgraded = false;
        for v in &values {
            match s.add(*v, max) {
                Ok(true) => added_count += 1,
                Ok(false) => {}
                Err(_) => {
                    upgraded = true;
                    break;
                }
            }
        }
        if upgraded {
            prop_assert!(added_count >= max);
        }
    }
}
