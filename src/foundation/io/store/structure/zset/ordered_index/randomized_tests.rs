use super::{structural_tests::assert_valid, OrderedIndex};
use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::BTreeMap;

fn member(id: usize) -> Bytes {
    Bytes::copy_from_slice(&id.to_be_bytes())
}

#[derive(Clone, Debug, PartialEq)]
struct OracleKey(f64, Bytes);

impl Eq for OracleKey {}

impl Ord for OracleKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .total_cmp(&other.0)
            .then_with(|| self.1.cmp(&other.1))
    }
}

impl PartialOrd for OracleKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn assert_matches_oracle(index: &OrderedIndex, oracle: &BTreeMap<OracleKey, ()>) {
    let expected: Vec<_> = oracle.keys().map(|key| (key.1.clone(), key.0)).collect();
    assert_eq!(index.len(), expected.len());
    assert_eq!(index.sorted_vec(), expected);
    assert_eq!(index.range_by_rank(0, -1, false), expected);
    let mut rev = expected.clone();
    rev.reverse();
    assert_eq!(index.range_by_rank(0, -1, true), rev);
    for rank in [0, expected.len() / 2, expected.len().saturating_sub(1)] {
        let entry = expected.get(rank).cloned();
        assert_eq!(index.get_element_by_rank(rank), entry);
        if let Some((bytes, score)) = entry {
            assert_eq!(index.get_rank(score, &bytes), Some(rank));
        }
    }
}

#[test]
fn randomized_10000_operations_match_btree_map_oracle() {
    for seed in [7, 42, 0xabcdef] {
        let mut rng = seed;
        let mut index = OrderedIndex::new();
        let mut oracle = BTreeMap::new();
        for id in 0..2048 {
            let key = OracleKey(id as f64 - 1024.0, member(id));
            index.insert(key.0, key.1.clone());
            oracle.insert(key, ());
        }
        for step in 0..10_000 {
            let bits = random(&mut rng);
            let id = ((bits >> 8) % 3072) as usize;
            let score = match bits % 97 {
                0 => f64::NEG_INFINITY,
                1 => f64::INFINITY,
                _ => id as f64 - 1024.0,
            };
            let key = OracleKey(score, member(id));
            match bits % 8 {
                0..=3 => {
                    index.insert(score, key.1.clone());
                    oracle.insert(key, ());
                }
                4..=5 => {
                    assert_eq!(index.delete(score, &key.1), oracle.remove(&key).is_some());
                }
                6..=7 => {
                    let is_reverse = bits % 8 == 7;
                    let count = ((bits >> 32) % 4) as usize;
                    let mut expected: Vec<_> = oracle.keys().cloned().collect();
                    if is_reverse {
                        expected.reverse();
                    }
                    expected.truncate(count);
                    let popped = if is_reverse {
                        index.pop_max(count)
                    } else {
                        index.pop_min(count)
                    };
                    assert_eq!(
                        popped,
                        expected
                            .iter()
                            .map(|key| (key.1.clone(), key.0))
                            .collect::<Vec<_>>()
                    );
                    for key in expected {
                        oracle.remove(&key);
                    }
                }
                _ => unreachable!(),
            }
            assert_valid(&index);
            let min = ((bits >> 16) % 3500) as f64 - 1200.0;
            let max = min + ((bits >> 48) % 500) as f64;
            let min_inc = bits & 32 != 0;
            let max_inc = bits & 64 != 0;
            let mut expected: Vec<_> = oracle
                .keys()
                .filter(|key| {
                    (key.0 > min || (min_inc && key.0 == min))
                        && (key.0 < max || (max_inc && key.0 == max))
                })
                .map(|key| (key.1.clone(), key.0))
                .collect();
            assert_eq!(
                index.count_in_score_range((min, min_inc), (max, max_inc)),
                expected.len()
            );
            let is_reverse = bits & 128 != 0;
            if is_reverse {
                expected.reverse();
            }
            let off = ((bits >> 24) % 600) as usize;
            let limit = ((bits >> 40) % 40) as usize;
            let expected: Vec<_> = expected.into_iter().skip(off).take(limit).collect();
            assert_eq!(
                index.range_by_score(
                    (min, min_inc),
                    (max, max_inc),
                    off as i64,
                    limit as i64,
                    is_reverse
                ),
                expected
            );
            if step % 31 == 0 {
                assert_matches_oracle(&index, &oracle);
            }
        }
        assert_matches_oracle(&index, &oracle);
        for key in oracle.keys() {
            assert!(index.delete(key.0, &key.1));
            assert_valid(&index);
        }
        assert!(index.is_empty());
        assert_eq!(index.memory_usage(), 0);
    }
}

#[test]
fn arbitrary_non_nan_float_bits_match_numeric_ordering_oracle() {
    let mut index = OrderedIndex::new();
    let mut oracle = BTreeMap::new();
    let mut rng = 1234567;
    for id in 0..4096 {
        let raw = f64::from_bits(random(&mut rng));
        if raw.is_nan() {
            continue;
        }
        let score = if raw == 0.0 { 0.0 } else { raw };
        let key = OracleKey(score, member(id));
        index.insert(raw, key.1.clone());
        oracle.insert(key, ());
    }
    assert_valid(&index);
    assert_matches_oracle(&index, &oracle);
    for (rank, key) in oracle.keys().enumerate() {
        assert_eq!(index.get_rank(key.0, &key.1), Some(rank));
        assert_eq!(
            index.get_element_by_rank(rank),
            Some((key.1.clone(), key.0))
        );
        assert_eq!(
            index.count_in_score_range((f64::NEG_INFINITY, true), (key.0, true)),
            rank + 1
        );
    }
}
