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

    assert_eq!(infer_zset_encoding(&data, 0, 64), "btree");
    assert_eq!(infer_zset_encoding(&data, 2, 8), "listpack");
    assert_eq!(infer_zset_encoding(&data, 1, 8), "btree");

    let mut large_member = HashTable::new();
    large_member.insert(member(9), 1.0);
    assert_eq!(infer_zset_encoding(&large_member, 2, 8), "btree");
}

#[test]
fn check_zset_encoding_upgrade_reports_threshold_crossings() {
    let mut data = HashTable::new();
    data.insert(member(1), 1.0);
    data.insert(member(2), 2.0);

    assert_eq!(
        check_zset_encoding_upgrade(&data, b"m", 0, 8),
        Some("btree")
    );
    assert_eq!(
        check_zset_encoding_upgrade(&data, b"m", 1, 8),
        Some("btree")
    );
    assert_eq!(
        check_zset_encoding_upgrade(&data, &[b'x'; 9], 2, 8),
        Some("btree")
    );
    assert_eq!(
        check_zset_encoding_upgrade(&data, b"m", 2, 8),
        Some("btree")
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

#[test]
fn zset_member_payload_stays_shared_after_score_updates() {
    let member = vec![b'x'; 4096];
    let mut zset = ZSetSl::new();
    zset.add(&member, 1.0).unwrap();
    let original = zset.dict().keys().next().unwrap().as_ptr();
    for score in [2.0, -0.0, -1.0, f64::INFINITY] {
        zset.add(&member, score).unwrap();
        let key = zset.dict().keys().next().unwrap();
        let (indexed, _) = zset.ordered_index().iter().next().unwrap();
        assert_eq!(key.as_ptr(), original);
        assert_eq!(indexed.as_ptr(), key.as_ptr());
        assert_eq!(zset.rank(&member), Some(0));
    }
}

#[test]
fn listpack_unknown_item_count_is_decoded_from_entries() {
    let items = vec![b"a".to_vec(), b"1".to_vec(), b"b".to_vec(), b"2".to_vec()];
    let mut raw =
        crate::foundation::io::persistence::codec::decoder_compact::listpack_encode(&items);
    raw[4..6].copy_from_slice(&u16::MAX.to_le_bytes());
    let mut listpack =
        ZSetLp::from_raw_bytes(raw).expect("unknown count is a valid listpack header");
    assert_eq!(listpack.len(), 2);
    listpack.add(b"c", 3.0).unwrap();
    assert_eq!(listpack.len(), 3);
    listpack.remove(b"a").unwrap();
    assert_eq!(listpack.len(), 2);
    assert_eq!(listpack.score(b"b"), Some(2.0));
}

#[test]
fn listpack_count_crossing_the_header_limit_uses_unknown_sentinel() {
    let mut items = Vec::with_capacity(65_534);
    for index in 0..32_767 {
        items.push(format!("{index:06}").into_bytes());
        items.push(b"1".to_vec());
    }
    let raw = crate::foundation::io::persistence::codec::decoder_compact::listpack_encode(&items);
    let mut listpack = ZSetLp::from_raw_bytes(raw).unwrap();
    assert_eq!(
        listpack.add_with_threshold(b"zzzzzz", 1.0, 32_768, 64),
        Ok(true)
    );
    assert_eq!(listpack.len(), 32_768);
    assert_eq!(&listpack.raw_bytes()[4..6], &u16::MAX.to_le_bytes());
    listpack.remove(b"000000").unwrap();
    assert_eq!(listpack.len(), 32_767);
    let encoded = crate::foundation::io::persistence::codec::decoder_compact::listpack_encode(
        &items
            .into_iter()
            .chain([b"zzzzzz".to_vec(), b"1".to_vec()])
            .collect::<Vec<_>>(),
    );
    assert_eq!(&encoded[4..6], &u16::MAX.to_le_bytes());
    assert!(ZSetLp::from_raw_bytes(encoded).is_some());
}
