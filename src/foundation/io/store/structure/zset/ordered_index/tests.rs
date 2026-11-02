use super::OrderedIndex;
use bytes::Bytes;

fn member(id: usize) -> Bytes {
    Bytes::copy_from_slice(&id.to_be_bytes())
}

fn fixture(count: usize) -> OrderedIndex {
    let mut index = OrderedIndex::new();
    for id in (0..count).rev() {
        index.insert(id as f64, member(id));
    }
    index
}

#[test]
fn orders_equal_scores_by_binary_members_and_uses_zero_based_ranks() {
    let mut index = OrderedIndex::new();
    for bytes in [b"\xff".as_slice(), b"\0", b"a", b"", b"\x80"] {
        index.insert(1.0, Bytes::copy_from_slice(bytes));
    }
    index.insert(-2.0, Bytes::from_static(b"first"));
    let expected = [
        (Bytes::from_static(b"first"), -2.0),
        (Bytes::new(), 1.0),
        (Bytes::from_static(b"\0"), 1.0),
        (Bytes::from_static(b"a"), 1.0),
        (Bytes::from_static(b"\x80"), 1.0),
        (Bytes::from_static(b"\xff"), 1.0),
    ];
    assert_eq!(index.sorted_vec(), expected);
    assert_eq!(index.len(), expected.len());
    assert!(!index.is_empty());
    for (rank, (bytes, score)) in expected.iter().enumerate() {
        assert_eq!(index.get_rank(*score, bytes), Some(rank));
        assert_eq!(
            index.get_element_by_rank(rank),
            Some((bytes.clone(), *score))
        );
    }
    assert_eq!(index.get_rank(1.0, b"missing"), None);
    assert_eq!(index.get_element_by_rank(usize::MAX), None);
}

#[test]
fn signed_zero_is_normalized_nan_is_rejected_and_infinities_are_supported() {
    let mut index = OrderedIndex::new();
    index.insert(-0.0, Bytes::from_static(b"zero"));
    index.insert(0.0, Bytes::from_static(b"zero"));
    index.insert(f64::NAN, Bytes::from_static(b"nan"));
    index.insert(f64::NEG_INFINITY, Bytes::from_static(b"low"));
    index.insert(f64::INFINITY, Bytes::from_static(b"high"));
    assert_eq!(index.len(), 3);
    assert_eq!(index.get_rank(-0.0, b"zero"), Some(1));
    assert_eq!(
        index.get_element_by_rank(1).unwrap().1.to_bits(),
        0.0f64.to_bits()
    );
    assert_eq!(
        index.count_in_score_range((f64::NEG_INFINITY, true), (f64::INFINITY, true)),
        3
    );
    assert_eq!(
        index.count_in_score_range((f64::NEG_INFINITY, false), (f64::INFINITY, false)),
        1
    );
    assert_eq!(index.count_in_score_range((f64::NAN, true), (1.0, true)), 0);
    assert_eq!(index.get_rank(f64::NAN, b"zero"), None);
    assert!(!index.delete(f64::NAN, b"zero"));
    assert!(index.delete(0.0, b"zero"));
    assert!(!index.delete(-0.0, b"zero"));
}

#[test]
fn score_ranges_handle_exclusive_bounds_offsets_limits_and_reverse_order() {
    let index = fixture(80);
    for rev in [false, true] {
        let mut expected: Vec<_> = (21..60).map(|id| (member(id), id as f64)).collect();
        if rev {
            expected.reverse();
        }
        assert_eq!(
            index.range_by_score((20.0, false), (60.0, false), 0, -1, rev),
            expected
        );
        assert_eq!(
            index.range_by_score((20.0, false), (60.0, false), 7, 19, rev),
            expected[7..26]
        );
        assert!(index
            .range_by_score((20.0, false), (60.0, false), -7, 1, rev)
            .is_empty());
        for off in [39, 80, i64::MAX] {
            assert!(index
                .range_by_score((20.0, false), (60.0, false), off, -1, rev)
                .is_empty());
        }
        assert!(index
            .range_by_score((20.0, true), (60.0, true), 0, 0, rev)
            .is_empty());
        assert!(index
            .range_by_score((20.0, true), (f64::NAN, true), 0, -1, rev)
            .is_empty());
    }
    assert_eq!(index.count_in_score_range((20.0, false), (60.0, false)), 39);
    assert_eq!(index.count_in_score_range((20.0, true), (20.0, true)), 1);
    assert_eq!(index.count_in_score_range((20.0, false), (20.0, true)), 0);
    assert_eq!(index.count_in_score_range((60.0, true), (20.0, true)), 0);
}

#[test]
fn rank_ranges_normalize_negative_and_extreme_indices() {
    let index = fixture(80);
    let sorted: Vec<_> = (0..80).map(|id| (member(id), id as f64)).collect();
    for rev in [false, true] {
        let mut expected = sorted.clone();
        if rev {
            expected.reverse();
        }
        assert_eq!(index.range_by_rank(0, -1, rev), expected);
        assert_eq!(index.range_by_rank(-5, -1, rev), expected[75..]);
        assert_eq!(index.range_by_rank(17, 42, rev), expected[17..43]);
        assert_eq!(index.range_by_rank(i64::MIN, i64::MAX, rev), expected);
        for (start, end) in [(4, 3), (80, 90), (0, -81), (i64::MAX, i64::MAX)] {
            assert!(index.range_by_rank(start, end, rev).is_empty());
        }
    }
}

#[test]
fn lex_ranges_use_infinity_sentinels_and_treat_empty_bytes_as_literal_bounds() {
    let mut index = OrderedIndex::new();
    for (bytes, score) in [
        (b"".as_slice(), 0.0),
        (b"+", 0.0),
        (b"-", 0.0),
        (b"\x80", 0.0),
        (b"\xff", 0.0),
        (b"b", 0.0),
        (b"b\0", 0.0),
    ] {
        index.insert(score, Bytes::copy_from_slice(bytes));
    }
    let sorted = index.sorted_vec();
    assert_eq!(index.range_by_lex(b"-", false, b"+", false, false), sorted);
    let expected: Vec<_> = sorted
        .iter()
        .filter(|(m, _)| m.as_ref() > b"b".as_slice())
        .cloned()
        .collect();
    assert_eq!(
        index.range_by_lex(b"b", false, b"+", false, false),
        expected
    );
    assert_eq!(
        index.count_in_lex_range(b"b", false, b"+", false),
        expected.len()
    );
    let mut reversed = expected;
    reversed.reverse();
    assert_eq!(index.range_by_lex(b"b", false, b"+", false, true), reversed);
    assert_eq!(index.count_in_lex_range(b"\x80", true, b"\xff", false), 1);
    assert_eq!(index.count_in_lex_range(b"+", true, b"+", true), 0);
    assert_eq!(index.count_in_lex_range(b"-", true, b"-", true), 0);
    assert!(index.range_by_lex(b"+", true, b"+", true, false).is_empty());
    assert!(index.range_by_lex(b"-", true, b"-", true, true).is_empty());
    assert_eq!(
        index.range_by_lex(b"", true, b"", true, false),
        [(Bytes::new(), 0.0)]
    );
    assert_eq!(index.count_in_lex_range(b"", true, b"", true), 1);
    assert_eq!(index.count_in_lex_range(b"", false, b"", true), 0);
    assert_eq!(index.count_in_lex_range(b"-", false, b"", true), 1);
    assert_eq!(
        index.count_in_lex_range(b"", false, b"+", false),
        sorted.len() - 1
    );
    assert_eq!(index.count_in_lex_range(b"\xff", true, b"\x80", true), 0);
}

#[test]
fn delete_and_pop_remove_only_exact_keys_and_preserve_order() {
    let mut index = fixture(2_048);
    assert!(!index.delete(-1.0, &member(10)));
    assert!(!index.delete(10.0, b"missing"));
    assert!(index.delete(10.0, &member(10)));
    assert!(!index.delete(10.0, &member(10)));
    assert!(index.pop_min(0).is_empty());
    assert_eq!(
        index.pop_min(10),
        (0..10)
            .map(|id| (member(id), id as f64))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        index.pop_max(10),
        (2038..2048)
            .rev()
            .map(|id| (member(id), id as f64))
            .collect::<Vec<_>>()
    );
    let expected = index.sorted_vec();
    let clone = index.clone();
    assert_eq!(index, clone);
    assert_eq!(index.pop_min(usize::MAX), expected);
    assert!(index.is_empty());
    assert!(index.pop_max(1).is_empty());
    assert_eq!(clone.len(), 2027);
}

#[test]
fn memory_counts_retained_tree_buffers_without_shared_member_payloads() {
    let mut short = OrderedIndex::new();
    let mut long = OrderedIndex::new();
    assert_eq!(short.memory_usage(), 0);
    for id in 0..2048 {
        short.insert(id as f64, member(id));
        long.insert(id as f64, Bytes::from(vec![id as u8; 8192]));
        assert_eq!(short.memory_usage(), long.memory_usage());
    }
    assert!(short.memory_usage() > 2048 * std::mem::size_of::<(f64, Bytes)>());
    short.pop_min(usize::MAX);
    assert_eq!(short.memory_usage(), 0);
}
