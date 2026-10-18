//! Deterministic Miri coverage for the dictionary's allocation lifecycle.

use versal::foundation::io::store::structure::dict::Dict;

#[test]
fn dict_insert_replace_iterate_and_remove() {
    let mut dict = Dict::new();

    assert_eq!(dict.insert("alpha", 1), None);
    assert_eq!(dict.insert("beta", 2), None);
    assert_eq!(dict.insert("gamma", 3), None);
    assert_eq!(dict.insert("beta", 20), Some(2));
    assert_eq!(dict.len(), 3);

    let mut entries: Vec<_> = dict.iter().map(|(key, value)| (*key, *value)).collect();
    entries.sort_unstable();
    assert_eq!(entries, vec![("alpha", 1), ("beta", 20), ("gamma", 3)]);

    assert_eq!(dict.remove("alpha"), Some(1));
    assert_eq!(dict.remove("missing"), None);
    assert_eq!(dict.get("beta"), Some(&20));
    assert_eq!(dict.len(), 2);
}
