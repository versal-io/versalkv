use super::*;
use crate::foundation::io::store::structure::hashtable::HashTable;
use crate::foundation::memory::value::ZSetOps;
use bytes::Bytes;

fn member(len: usize) -> Bytes {
    Bytes::from(vec![b'x'; len])
}

#[test]
fn infer_zset_encoding_honors_entry_and_member_size_thresholds() {
    let mut data = HashTable::new();
    data.insert(member(4), 1.0);
    data.insert(member(8), 2.0);

    assert_eq!(infer_zset_encoding(&data, 0, 64), "skiplist");
    assert_eq!(infer_zset_encoding(&data, 2, 8), "listpack");
    assert_eq!(infer_zset_encoding(&data, 1, 8), "skiplist");

    let mut large_member = HashTable::new();
    large_member.insert(member(9), 1.0);
    assert_eq!(infer_zset_encoding(&large_member, 2, 8), "skiplist");
}

#[test]
fn check_zset_encoding_upgrade_reports_threshold_crossings() {
    let mut data = HashTable::new();
    data.insert(member(1), 1.0);
    data.insert(member(2), 2.0);

    assert_eq!(
        check_zset_encoding_upgrade(&data, b"m", 0, 8),
        Some("skiplist")
    );
    assert_eq!(
        check_zset_encoding_upgrade(&data, b"m", 1, 8),
        Some("skiplist")
    );
    assert_eq!(
        check_zset_encoding_upgrade(&data, &[b'x'; 9], 2, 8),
        Some("skiplist")
    );
    assert_eq!(
        check_zset_encoding_upgrade(&data, b"m", 2, 8),
        Some("skiplist")
    );
    assert_eq!(
        check_zset_encoding_upgrade(&data, data.keys().next().unwrap().as_ref(), 2, 8),
        None
    );
}

#[test]
fn zsetsl_dual_add_remove_sorted() {
    let mut z = ZSetSl::new();
    assert!(z.add(b"c", 3.0).unwrap());
    assert!(z.add(b"a", 1.0).unwrap());
    assert!(z.add(b"b", 2.0).unwrap());
    // Duplicate add same score
    assert!(!z.add(b"a", 1.0).unwrap());
    // Update score
    assert!(!z.add(b"a", 0.5).unwrap());
    assert_eq!(z.score(b"a"), Some(0.5));
    assert_eq!(z.len(), 3);
    let se = z.sorted_entries();
    assert_eq!(se[0].0.as_ref(), b"a");
    assert_eq!(se[0].1, 0.5);
    assert_eq!(se[1].0.as_ref(), b"b");
    assert_eq!(se[1].1, 2.0);
    assert_eq!(se[2].0.as_ref(), b"c");
    assert_eq!(se[2].1, 3.0);
    assert_eq!(z.rank(b"b"), Some(1));
    assert!(z.remove(b"b").unwrap());
    assert_eq!(z.len(), 2);
    assert_eq!(z.rank(b"c"), Some(1));
}
