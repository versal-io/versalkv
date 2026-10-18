use super::skiplist::SkipList;
use bytes::Bytes;

#[test]
fn insert_len_rank() {
    let mut z = SkipList::new();
    z.insert(1.0, Bytes::from_static(b"a"));
    z.insert(2.0, Bytes::from_static(b"b"));
    z.insert(3.0, Bytes::from_static(b"c"));
    assert_eq!(z.len(), 3);
    assert_eq!(z.get_rank(1.0, b"a"), Some(0));
    assert_eq!(z.get_rank(2.0, b"b"), Some(1));
    assert_eq!(z.get_rank(3.0, b"c"), Some(2));
}

#[test]
fn same_score_lex() {
    let mut z = SkipList::new();
    for m in ["ccc", "aaa", "bbb"] {
        z.insert(1.5, Bytes::from_static(m.as_bytes()));
    }
    assert_eq!(z.get_rank(1.5, b"aaa"), Some(0));
    assert_eq!(z.get_rank(1.5, b"bbb"), Some(1));
    assert_eq!(z.get_rank(1.5, b"ccc"), Some(2));
    assert_eq!(z.get_element_by_rank(0).unwrap().0.as_ref(), b"aaa");
}

#[test]
fn delete_and_ranks_adjust() {
    let mut z = SkipList::new();
    for i in 0..5 {
        z.insert(i as f64, Bytes::from(format!("m{}", i)));
    }
    assert!(z.delete(2.0, b"m2"));
    assert_eq!(z.len(), 4);
    assert_eq!(z.get_rank(3.0, b"m3"), Some(2));
}

#[test]
fn insert_delete_2k() {
    let mut z = SkipList::new();
    let n = 2000usize;
    for i in 0..n {
        z.insert(i as f64, Bytes::from(format!("k{}", i)));
    }
    assert_eq!(z.len(), n);
    for i in 0..n {
        assert_eq!(z.get_rank(i as f64, format!("k{}", i).as_bytes()), Some(i));
    }
    for i in (0..n).step_by(2) {
        z.delete(i as f64, format!("k{}", i).as_bytes());
    }
    assert_eq!(z.len(), n / 2);
    for (rank, i) in (1..n).step_by(2).enumerate() {
        assert_eq!(
            z.get_rank(i as f64, format!("k{}", i).as_bytes()),
            Some(rank)
        );
    }
    let v: Vec<_> = z.iter().map(|(m, s)| (m.clone(), *s)).collect();
    for w in v.windows(2) {
        assert!(w[0].1 < w[1].1);
    }
}

#[test]
fn score_ranges() {
    let mut z = SkipList::new();
    for i in 1..=10 {
        z.insert(i as f64, Bytes::from(format!("m{}", i)));
    }
    assert_eq!(z.count_in_score_range((3.0, true), (7.0, true)), 5);
    let r = z.range_by_score((3.0, true), (7.0, true), 0, -1, false);
    assert_eq!(r.len(), 5);
    assert_eq!(r[0].1, 3.0);
    assert_eq!(r[4].1, 7.0);
    assert_eq!(z.count_in_score_range((3.0, false), (7.0, false)), 3);
    let r = z.range_by_score((0.0, true), (100.0, true), 2, 3, false);
    assert_eq!(r.len(), 3);
    assert_eq!(r[0].1, 3.0);
    let r = z.range_by_score((3.0, true), (7.0, true), 0, -1, true);
    assert_eq!(r[0].1, 7.0);
    assert_eq!(r[4].1, 3.0);
}

#[test]
fn rank_ranges() {
    let mut z = SkipList::new();
    for i in 0..10 {
        z.insert(i as f64, Bytes::from(format!("m{}", i)));
    }
    let r = z.range_by_rank(-3, -1, false);
    assert_eq!(r.len(), 3);
    assert_eq!(r[0].1, 7.0);
    let r = z.range_by_rank(5, 3, false);
    assert_eq!(r.len(), 0);
    let r = z.range_by_rank(0, 9, true);
    assert_eq!(r[0].1, 9.0);
}

#[test]
fn delete_range_and_pops() {
    let mut z = SkipList::new();
    for i in 0..100 {
        z.insert(i as f64, Bytes::from(format!("m{}", i)));
    }
    assert_eq!(z.delete_range_by_score((20.0, true), (29.0, true)), 10);
    assert_eq!(z.len(), 90);
    let m = z.pop_min(5);
    assert_eq!(m.len(), 5);
    assert_eq!(m[0].1, 0.0);
    assert_eq!(z.len(), 85);
    let m = z.pop_max(3);
    assert_eq!(m.len(), 3);
    assert_eq!(m[0].1, 99.0);
    assert_eq!(z.len(), 82);
    assert_eq!(z.delete_range_by_rank(0, 4), 5);
    assert_eq!(z.len(), 77);
}

#[test]
fn empty_boundary() {
    let mut z = SkipList::new();
    assert_eq!(z.pop_min(1).len(), 0);
    assert_eq!(z.count_in_score_range((0.0, true), (1.0, true)), 0);
    assert!(!z.delete(0.0, b"nope"));
    assert_eq!(z.get_rank(0.0, b"nope"), None);
    assert_eq!(z.get_element_by_rank(0), None);
}

#[test]
fn clone_equiv() {
    let mut z = SkipList::new();
    for i in 0..50 {
        z.insert((i as f64) * 0.5, Bytes::from(format!("m{}", i)));
    }
    let z2 = z.clone();
    assert_eq!(z.len(), z2.len());
    for i in 0..50 {
        let m = format!("m{}", i);
        assert_eq!(
            z.get_rank((i as f64) * 0.5, m.as_bytes()),
            z2.get_rank((i as f64) * 0.5, m.as_bytes())
        );
    }
}

#[test]
fn lex_ranges() {
    let mut z = SkipList::new();
    for m in ["apple", "banana", "cherry", "date", "elder"] {
        z.insert(1.0, Bytes::from_static(m.as_bytes()));
    }
    // Lex range: min=b (incl), max=d (incl). Note: b"banana" > b"b" and b"date" > b"d"
    // so only banana and cherry fall within [b, d] as bounded by short prefix bounds.
    let r = z.range_by_lex(b"b", true, b"d", true, false);
    assert_eq!(r.len(), 2);
    assert_eq!(r[0].0.as_ref(), b"banana");
    assert_eq!(r[1].0.as_ref(), b"cherry");
    // Excluding bounds: strictly between "b" (excl) and "d" (excl): banana, cherry = 2.
    let r = z.range_by_lex(b"b", false, b"d", false, false);
    assert_eq!(r.len(), 2);
    assert_eq!(r[0].0.as_ref(), b"banana");
    assert_eq!(r[1].0.as_ref(), b"cherry");
    // Exact-member bounds.
    assert_eq!(z.count_in_lex_range(b"banana", true, b"cherry", true), 2);
    assert_eq!(z.delete_range_by_lex(b"banana", true, b"cherry", true), 2);
    assert_eq!(z.len(), 3);
}

#[test]
fn lex_inf() {
    let mut z = SkipList::new();
    z.insert(1.0, Bytes::from_static(b"a"));
    z.insert(1.0, Bytes::from_static(b"b"));
    z.insert(1.0, Bytes::from_static(b"c"));
    let r = z.range_by_lex(b"-", true, b"+", true, false);
    assert_eq!(r.len(), 3);
    assert_eq!(z.count_in_lex_range(b"-", true, b"+", true), 3);
}

// Task-required tests: 1000 random scores and 10000 interleaved add/remove.
#[test]
fn random_1000_matches_sorted_vec() {
    use rand::{rngs::StdRng, Rng, SeedableRng};
    let mut rng = StdRng::seed_from_u64(42);
    let mut z = SkipList::new();
    let mut sorted: std::collections::BTreeMap<(i64, Vec<u8>), (f64, Bytes)> =
        std::collections::BTreeMap::new();
    for _ in 0..1000 {
        let s: f64 = rng.gen_range(-1000.0..1000.0);
        let len = rng.gen_range(2usize..8);
        let member: Vec<u8> = (0..len).map(|_| rng.gen_range(b'a'..=b'z')).collect();
        let key = ((s * 1e12) as i64, member.clone());
        if sorted.contains_key(&key) {
            continue;
        }
        sorted.insert(key, (s, Bytes::copy_from_slice(&member)));
        z.insert(s, Bytes::copy_from_slice(&member));
    }
    let iterated: Vec<(Bytes, f64)> = z.iter().map(|(m, s)| (m.clone(), *s)).collect();
    let expected: Vec<(Bytes, f64)> = sorted.values().map(|(s, m)| (m.clone(), *s)).collect();
    assert_eq!(iterated.len(), expected.len());
    assert_eq!(z.len(), expected.len());
    for (a, b) in iterated.iter().zip(expected.iter()) {
        assert_eq!(a, b);
    }
}

#[test]
fn interleaved_10000_iter_matches_sorted() {
    use rand::{rngs::StdRng, Rng, SeedableRng};
    let mut rng = StdRng::seed_from_u64(7);
    let mut z = SkipList::new();
    let mut canon: std::collections::BTreeMap<(u64, Vec<u8>), f64> =
        std::collections::BTreeMap::new();
    for _ in 0..10000 {
        let op: u8 = rng.gen_range(0..3);
        let s_raw: f64 = rng.gen_range(0.0..1000.0);
        let len = rng.gen_range(1usize..6);
        let member: Vec<u8> = (0..len).map(|_| rng.gen_range(b'a'..=b'z')).collect();
        let score_key: u64 = f64::to_bits(s_raw);
        match op {
            0 | 1 => {
                canon.insert((score_key, member.clone()), s_raw);
                z.insert(s_raw, Bytes::copy_from_slice(&member));
            }
            2 => {
                if canon.remove(&(score_key, member.clone())).is_some() {
                    assert!(z.delete(s_raw, &member));
                }
            }
            _ => unreachable!(),
        }
    }
    assert_eq!(z.len(), canon.len());
    let iterated: Vec<(u64, Vec<u8>, f64)> = z
        .iter()
        .map(|(m, s)| {
            let k: u64 = f64::to_bits(*s);
            (k, m.to_vec(), *s)
        })
        .collect();
    let expected: Vec<(u64, Vec<u8>, f64)> = canon
        .iter()
        .map(|((k, m), s)| (*k, m.clone(), *s))
        .collect();
    assert_eq!(iterated.len(), expected.len());
    for (a, b) in iterated.iter().zip(expected.iter()) {
        assert_eq!(a, b);
    }
}
