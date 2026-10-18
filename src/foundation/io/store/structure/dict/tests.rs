use super::core::{Entry, ResizeBudget};
use super::*;

#[test]
fn test_new_dict_is_empty() {
    let d: Dict<String, i32> = Dict::new();
    assert!(d.is_empty());
    assert_eq!(d.len(), 0);
}

#[test]
fn test_default_equals_new() {
    let d: Dict<String, i32> = Dict::default();
    assert!(d.is_empty());
}

#[test]
fn test_insert_and_get() {
    let mut d = Dict::new();
    assert_eq!(d.insert("key1", 100), None);
    assert_eq!(d.len(), 1);
    assert!(!d.is_empty());
    assert_eq!(d.get("key1"), Some(&100));
}

#[test]
fn test_single_entry_bucket_stays_in_its_single_entry_representation() {
    let mut d = Dict::new();
    d.insert("key1", 100);

    assert_eq!(d.bucket_entry_count_for_key("key1"), 1);
}

#[test]
fn test_entry_layout_does_not_store_a_separate_hash_cache() {
    assert_eq!(std::mem::size_of::<Entry<&str, i32>>(), 24);
}

#[test]
fn test_insert_overwrite_returns_old_value() {
    let mut d = Dict::new();
    assert_eq!(d.insert("a", 1), None);
    assert_eq!(d.insert("a", 2), Some(1));
    assert_eq!(d.get("a"), Some(&2));
    assert_eq!(d.len(), 1);
}

#[test]
fn test_get_missing_key_returns_none() {
    let d: Dict<&str, i32> = Dict::new();
    assert_eq!(d.get("nope"), None);
}

#[test]
fn test_get_mut() {
    let mut d = Dict::new();
    d.insert("a", 1);
    if let Some(v) = d.get_mut("a") {
        *v = 42;
    }
    assert_eq!(d.get("a"), Some(&42));
}

#[test]
fn test_get_mut_missing() {
    let mut d: Dict<&str, i32> = Dict::new();
    assert_eq!(d.get_mut("nope"), None);
}

#[test]
fn test_get_key_value() {
    let mut d = Dict::new();
    d.insert("a", 1);
    let (k, v) = d.get_key_value("a").unwrap();
    assert_eq!(*k, "a");
    assert_eq!(*v, 1);
}

#[test]
fn test_contains_key() {
    let mut d = Dict::new();
    d.insert("a", 1);
    assert!(d.contains_key("a"));
    assert!(!d.contains_key("b"));
}

#[test]
fn test_remove_existing() {
    let mut d = Dict::new();
    d.insert("a", 1);
    assert_eq!(d.remove("a"), Some(1));
    assert_eq!(d.len(), 0);
    assert!(d.is_empty());
}

#[test]
fn test_keys() {
    let mut d = Dict::new();
    d.insert("a", 1);
    d.insert("b", 2);
    d.insert("c", 3);
    let mut keys: Vec<&&str> = d.keys().collect();
    keys.sort();
    assert_eq!(keys, vec![&"a", &"b", &"c"]);
}

#[test]
fn test_iter() {
    let mut d = Dict::new();
    d.insert("a", 1);
    d.insert("b", 2);
    let mut items: Vec<(&&str, &i32)> = d.iter().collect();
    items.sort_by_key(|(k, _)| **k);
    assert_eq!(items, vec![(&"a", &1), (&"b", &2)]);
}

#[test]
fn test_clear() {
    let mut d = Dict::new();
    d.insert("a", 1);
    d.insert("b", 2);
    d.clear();
    assert!(d.is_empty());
    assert_eq!(d.len(), 0);
    assert_eq!(d.get("a"), None);
}

#[test]
fn test_drain() {
    let mut d = Dict::new();
    d.insert("a", 1);
    d.insert("b", 2);
    let mut items: Vec<(&str, i32)> = d.drain().collect();
    items.sort_by_key(|(k, _)| *k);
    assert_eq!(items, vec![("a", 1), ("b", 2)]);
    assert!(d.is_empty());
}

#[test]
fn test_expand_with_many_entries() {
    let mut d = Dict::new();
    for i in 0..200 {
        d.insert(format!("key{}", i), i);
    }
    assert_eq!(d.len(), 200);
    for i in 0..200 {
        assert_eq!(d.get(&format!("key{}", i)), Some(&i));
    }
}

#[test]
fn test_shrink_after_removals() {
    let mut d = Dict::new();
    for i in 0..200 {
        d.insert(format!("key{}", i), i);
    }
    assert_eq!(d.len(), 200);
    for i in 0..180 {
        d.remove(&format!("key{}", i));
    }
    assert_eq!(d.len(), 20);
    for i in 180..200 {
        assert_eq!(d.get(&format!("key{}", i)), Some(&i));
    }
}

#[test]
fn test_scan_empty_dict() {
    let d: Dict<&str, i32> = Dict::new();
    let cursor = d.scan(0, |_, _| {});
    assert_eq!(cursor, 0);
}

#[test]
fn test_scan_visits_all_keys() {
    let mut d = Dict::new();
    for i in 0..40 {
        d.insert(format!("k{}", i), i);
    }
    let mut visited = std::collections::HashSet::new();
    let mut cursor = 0;
    loop {
        cursor = d.scan(cursor, |k, _| {
            visited.insert(k.clone());
        });
        if cursor == 0 {
            break;
        }
    }
    assert_eq!(visited.len(), 40);
    for i in 0..40 {
        assert!(visited.contains(&format!("k{}", i)));
    }
}

#[test]
fn test_insert_during_rehash_is_progressive() {
    let mut d: Dict<String, i32> = Dict::new();
    for i in 0..9i32 {
        d.insert(format!("k{i}"), i);
    }
    assert_eq!(d.len(), 9);
    for i in 9i32..200 {
        d.insert(format!("k{i}"), i);
    }
    assert_eq!(d.len(), 200);
    for i in 0..200i32 {
        assert_eq!(d.get(&format!("k{i}")), Some(&i));
    }
    for i in 0..150i32 {
        d.remove(&format!("k{i}"));
    }
    assert_eq!(d.len(), 50);
    for i in 150i32..200 {
        assert_eq!(d.get(&format!("k{i}")), Some(&i));
    }
}

#[test]
fn test_bulk_insert_does_not_accumulate_in_rehash_target_bucket() {
    let mut d: Dict<String, i32> = Dict::new();

    for i in 0..10_000i32 {
        d.insert(i.to_string(), i);
    }

    assert_eq!(d.len(), 10_000);
    assert!(
        d.max_bucket_entry_count() < 64,
        "max bucket chain grew to {} entries",
        d.max_bucket_entry_count()
    );
    for i in [0, 511, 512, 1024, 4096, 9999] {
        assert_eq!(d.get(&i.to_string()), Some(&i));
    }
}

#[test]
fn test_rehash_midway_reads_both_tables() {
    let mut d: Dict<String, i64> = Dict::new();
    for i in 0..9i64 {
        d.insert(format!("k{i}"), i);
    }
    if d.is_rehashing() {
        let _ = d.rehash(2);
    }
    for i in 0..9i64 {
        assert_eq!(
            d.get(&format!("k{i}")),
            Some(&i),
            "key k{i} lost during rehash"
        );
    }
}

#[test]
fn test_borrow_key_lookup() {
    let mut d: Dict<String, i32> = Dict::new();
    d.insert("hello".to_string(), 42);
    assert_eq!(d.get("hello"), Some(&42));
    assert_eq!(d.get_mut("hello"), Some(&mut 42));
    assert!(d.contains_key("hello"));
    assert_eq!(d.remove("hello"), Some(42));
}

#[test]
fn soft_expansion_respects_resize_budget() {
    let mut dict = Dict::new();
    for index in 0..7 {
        dict.insert(index, index);
    }
    assert_eq!(dict.bucket_count(), 1);
    dict.set_resize_budget(Some(0));

    dict.insert(7, 7);

    assert_eq!(dict.bucket_count(), 1);
    assert!(!dict.is_rehashing());
}

#[test]
fn shared_resize_budget_is_consumed_once_across_dicts() {
    let expansion_bytes = 2 * std::mem::size_of::<usize>();
    let budget = ResizeBudget::new(Some(expansion_bytes));
    let mut first = Dict::with_resize_budget(budget.clone());
    let mut second = Dict::with_resize_budget(budget);
    for index in 0..7 {
        first.insert(index, index);
        second.insert(index, index);
    }
    assert_eq!(first.bucket_count(), 1);
    assert_eq!(second.bucket_count(), 1);

    first.insert(7, 7);
    second.insert(7, 7);

    assert!(first.is_rehashing() || first.bucket_count() > 1);
    assert_eq!(second.bucket_count(), 1);
    assert!(!second.is_rehashing());
}

#[test]
fn hard_fill_limit_forces_expansion_without_budget() {
    let mut dict = Dict::new();
    dict.set_resize_budget(Some(0));

    for index in 0..35 {
        dict.insert(index, index);
    }
    assert_eq!(dict.bucket_count(), 1);
    dict.insert(35, 35);

    assert!(dict.bucket_count() > 1 || dict.is_rehashing());
}
