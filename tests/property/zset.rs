use bytes::Bytes;
use proptest::prelude::*;
use versal::foundation::io::store::structure::zset::ZSetSl;
use versal::foundation::memory::value::ZSetOps;

fn member_strategy() -> impl Strategy<Value = String> {
    "[a-z]{1,6}"
}

fn score_strategy() -> impl Strategy<Value = f64> {
    (-1000.0f64..1000.0f64).prop_filter("reject NaN/Inf", |f| f.is_finite())
}

fn op_strategy() -> impl Strategy<Value = (u8, String, f64)> {
    prop_oneof![
        3 => (Just(0u8), member_strategy(), score_strategy()),
        1 => (Just(1u8), member_strategy(), Just(0.0f64)),
    ]
}

proptest! {
    #[test]
    fn zsetsl_sorted_entries_match_reference(
        ops in proptest::collection::vec(op_strategy(), 0..200),
    ) {
        let mut zset = ZSetSl::new();
        let mut reference: std::collections::HashMap<String, f64> =
            std::collections::HashMap::new();

        for (kind, member, score) in &ops {
            match kind {
                0 => {
                    let _ = ZSetOps::add(&mut zset, member.as_bytes(), *score);
                    reference.insert(member.clone(), *score);
                }
                1 => {
                    let _ = ZSetOps::remove(&mut zset, member.as_bytes());
                    reference.remove(member);
                }
                _ => unreachable!(),
            }
        }

        prop_assert_eq!(zset.len(), reference.len());

        let mut ref_sorted: Vec<(String, f64)> = reference
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        ref_sorted.sort_by(|a, b| {
            a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });

        let zset_sorted: Vec<(String, f64)> = zset
            .sorted_entries()
            .into_iter()
            .map(|(b, s)| (String::from_utf8_lossy(&b).to_string(), s))
            .collect();

        prop_assert_eq!(zset_sorted.len(), ref_sorted.len());
        for (z, r) in zset_sorted.iter().zip(ref_sorted.iter()) {
            prop_assert_eq!(&z.0, &r.0);
            prop_assert!((z.1 - r.1).abs() < 1e-9f64);
        }
    }

    #[test]
    fn zsetsl_rank_and_count_match_scan(
        ops in proptest::collection::vec(op_strategy(), 0..200),
    ) {
        let mut zset = ZSetSl::new();
        for (kind, member, score) in &ops {
            match kind {
                0 => { let _ = ZSetOps::add(&mut zset, member.as_bytes(), *score); }
                1 => { let _ = ZSetOps::remove(&mut zset, member.as_bytes()); }
                _ => unreachable!(),
            }
        }

        let sorted = zset.sorted_entries();

        for (idx, (member, _score)) in sorted.iter().enumerate() {
            prop_assert_eq!(
                zset.rank(member),
                Some(idx),
                "rank mismatch for {:?}",
                member
            );
        }

        let boundaries: [f64; 5] = [-2000.0, -100.0, 0.0, 100.0, 2000.0];
        for &low_raw in &boundaries {
            for &high_raw in &boundaries {
                if low_raw > high_raw { continue; }
                for low_inc in [true, false] {
                    for high_inc in [true, false] {
                        let min = (low_raw, low_inc);
                        let max = (high_raw, high_inc);
                        let from_scan = sorted.iter().filter(|(_m, s)| {
                            let lo_ok = if low_inc { *s >= low_raw } else { *s > low_raw };
                            let hi_ok = if high_inc { *s <= high_raw } else { *s < high_raw };
                            lo_ok && hi_ok
                        }).count();
                        prop_assert_eq!(
                            zset.count_by_score(min, max),
                            from_scan,
                            "count mismatch for range {:?}",
                            (min, max)
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn zsetsl_range_by_rank_matches_slice(
        ops in proptest::collection::vec(op_strategy(), 0..200),
    ) {
        let mut zset = ZSetSl::new();
        for (kind, member, score) in &ops {
            match kind {
                0 => { let _ = ZSetOps::add(&mut zset, member.as_bytes(), *score); }
                1 => { let _ = ZSetOps::remove(&mut zset, member.as_bytes()); }
                _ => unreachable!(),
            }
        }

        let sorted = zset.sorted_entries();
        let n = sorted.len() as i64;
        let cases: Vec<(i64, i64, bool)> = vec![
            (0, -1, false),
            (0, 0, false),
            (0, 9999, false),
            (-1, -1, false),
            (-3, -1, false),
            (2, 5, false),
            (0, -1, true),
        ];
        for (s, e, rev) in cases {
            let via_api = zset.range_by_rank(s, e, rev);
            let start = if s < 0 { n + s } else { s };
            let end = if e < 0 { n + e } else { e };
            let start = start.max(0);
            let end = end.min(if n > 0 { n - 1 } else { 0 });
            let expected: Vec<(Bytes, f64)> = if start > end || n == 0 {
                Vec::new()
            } else {
                let rng = (start as usize)..=(end as usize);
                let mut v: Vec<(Bytes, f64)> = sorted[rng].to_vec();
                if rev { v.reverse(); }
                v
            };
            prop_assert_eq!(
                via_api.len(),
                expected.len(),
                "len mismatch s={} e={} rev={} n={}", s, e, rev, n
            );
            for (a, b) in via_api.iter().zip(expected.iter()) {
                prop_assert_eq!(&a.0, &b.0);
                prop_assert!((a.1 - b.1).abs() < 1e-9f64);
            }
        }
    }

    #[test]
    fn zsetsl_pop_min_max_match_ordered_slice(
        ops in proptest::collection::vec(op_strategy(), 5..150),
    ) {
        let mut zset = ZSetSl::new();
        for (kind, member, score) in &ops {
            match kind {
                0 => { let _ = ZSetOps::add(&mut zset, member.as_bytes(), *score); }
                1 => { let _ = ZSetOps::remove(&mut zset, member.as_bytes()); }
                _ => unreachable!(),
            }
        }
        let n = zset.len();
        if n == 0 { return Ok(()); }

        let take = (n / 3).max(1).min(n);
        let mut zm = zset.clone();
        let popped_min = zm.pop_min(take);
        prop_assert_eq!(popped_min.len(), take.min(n));
        let sorted_before = zset.sorted_entries();
        for (i, p) in popped_min.iter().enumerate() {
            prop_assert_eq!(&p.0, &sorted_before[i].0);
        }
        prop_assert_eq!(zm.len(), n - take.min(n));

        let mut zm2 = zset.clone();
        let popped_max = zm2.pop_max(take);
        prop_assert_eq!(popped_max.len(), take.min(n));
        for (i, p) in popped_max.iter().enumerate() {
            let idx = sorted_before.len() - 1 - i;
            prop_assert_eq!(&p.0, &sorted_before[idx].0);
        }
        prop_assert_eq!(zm2.len(), n - take.min(n));
    }

    #[test]
    fn zsetsl_score_update_moves_rank_correctly(
        members in proptest::collection::vec(member_strategy(), 10..50),
        final_scores in proptest::collection::vec(score_strategy(), 10..50),
    ) {
        let mut zset = ZSetSl::new();
        let mut reference: std::collections::HashMap<String, f64> =
            std::collections::HashMap::new();

        for (i, m) in members.iter().enumerate() {
            let init = i as f64;
            let _ = ZSetOps::add(&mut zset, m.as_bytes(), init);
            reference.insert(m.clone(), init);
        }

        for (i, m) in members.iter().enumerate() {
            let new_score = final_scores[i % final_scores.len()];
            let _ = ZSetOps::add(&mut zset, m.as_bytes(), new_score);
            reference.insert(m.clone(), new_score);
        }

        let mut ref_sorted: Vec<(String, f64)> = reference
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        ref_sorted.sort_by(|a, b| {
            a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });

        let actual_sorted: Vec<(Bytes, f64)> = zset.sorted_entries();
        prop_assert_eq!(actual_sorted.len(), ref_sorted.len());
        for (idx, (m, s)) in actual_sorted.iter().enumerate() {
            let ms = String::from_utf8_lossy(m).to_string();
            prop_assert_eq!(ms, ref_sorted[idx].0.clone());
            prop_assert_eq!(zset.rank(m), Some(idx));
            prop_assert!((*s - ref_sorted[idx].1).abs() < 1e-9f64);
        }
    }

    #[test]
    fn zsetsl_shuffled_ops_all_queries_consistent(
        ops in proptest::collection::vec(op_strategy(), 100..600),
    ) {
        let mut zset = ZSetSl::new();
        let mut reference: std::collections::HashMap<String, f64> =
            std::collections::HashMap::new();

        for (kind, member, score) in &ops {
            match kind {
                0 => {
                    let _ = ZSetOps::add(&mut zset, member.as_bytes(), *score);
                    reference.insert(member.clone(), *score);
                }
                1 => {
                    let _ = ZSetOps::remove(&mut zset, member.as_bytes());
                    reference.remove(member);
                }
                _ => unreachable!(),
            }
        }

        let sorted = zset.sorted_entries();
        let n = sorted.len() as i64;

        for (idx, (member, _)) in sorted.iter().enumerate() {
            prop_assert_eq!(
                zset.rank(member),
                Some(idx),
                "rank mismatch for {:?}",
                member
            );
        }

        let boundaries: [f64; 7] = [-5000.0, -1000.0, -500.0, 0.0, 500.0, 1000.0, 5000.0];
        for (i, &low_raw) in boundaries.iter().enumerate() {
            for (j, &high_raw) in boundaries.iter().enumerate() {
                if i > j { continue; }
                for low_inc in [true, false] {
                    for high_inc in [true, false] {
                        let min = (low_raw, low_inc);
                        let max = (high_raw, high_inc);
                        let via_range = zset.range_by_score(min, max, 0, -1, false);
                        let via_count = zset.count_by_score(min, max);
                        prop_assert_eq!(
                            via_range.len(),
                            via_count,
                            "range_by_score vs count_by_score mismatch for {:?}",
                            (min, max)
                        );
                        let from_scan: usize = sorted.iter().filter(|(_m, s)| {
                            let lo = if low_inc { *s >= low_raw } else { *s > low_raw };
                            let hi = if high_inc { *s <= high_raw } else { *s < high_raw };
                            lo && hi
                        }).count();
                        prop_assert_eq!(
                            via_count, from_scan,
                            "count_by_score mismatch vs scan for {:?}",
                            (min, max)
                        );
                        for (a, b) in via_range.iter().zip(
                            sorted.iter().filter(|(_m, s)| {
                                let lo = if low_inc { *s >= low_raw } else { *s > low_raw };
                                let hi = if high_inc { *s <= high_raw } else { *s < high_raw };
                                lo && hi
                            })
                        ) {
                            prop_assert_eq!(&a.0, &b.0);
                            prop_assert!((a.1 - b.1).abs() < 1e-9);
                        }
                    }
                }
            }
        }

        if n > 0 {
            let cases: Vec<(i64, i64, bool)> = vec![
                (0, -1, false),
                (0, 0, false),
                (0, n / 2, false),
                (n / 3, 2 * n / 3, false),
                (-1, -1, false),
                (-3, -1, false),
                (0, -1, true),
                (0, 0, true),
                (-1, -1, true),
                (-3, -1, true),
                (0, n / 2, true),
            ];
            for (s, e, rev) in cases {
                let via_api = zset.range_by_rank(s, e, rev);
                let mut s_i = if s < 0 { n + s } else { s };
                let mut e_i = if e < 0 { n + e } else { e };
                if s_i < 0 { s_i = 0; }
                if e_i >= n { e_i = n - 1; }
                let expected: Vec<(Bytes, f64)> = if s_i > e_i {
                    Vec::new()
                } else if rev {
                    let s_fwd = (n - 1 - e_i) as usize;
                    let e_fwd = (n - 1 - s_i) as usize;
                    sorted[s_fwd..=e_fwd].iter().rev().cloned().collect()
                } else {
                    sorted[(s_i as usize)..=(e_i as usize)].to_vec()
                };
                prop_assert_eq!(via_api.len(), expected.len(),
                    "len mismatch s={} e={} rev={} n={}", s, e, rev, n);
                for (idx, (a, b)) in via_api.iter().zip(expected.iter()).enumerate() {
                    prop_assert_eq!(&a.0, &b.0,
                        "rank range idx={} s={} e={} rev={} n={}: got {:?} want {:?}",
                        idx, s, e, rev, n,
                        String::from_utf8_lossy(&a.0), String::from_utf8_lossy(&b.0));
                    prop_assert!((a.1 - b.1).abs() < 1e-9);
                }
            }
        }
    }

    #[test]
    fn zsetsl_remove_range_variants_match_hashmap_subtract(
        ops in proptest::collection::vec(op_strategy(), 50..400),
        score_min in (-2000.0f64..2000.0f64),
        score_max in (-2000.0f64..2000.0f64),
        rank_low in 0usize..150usize,
        rank_high in 0usize..250usize,
    ) {
        // --- setup ---
        let mut zset = ZSetSl::new();
        let mut reference: std::collections::HashMap<String, f64> =
            std::collections::HashMap::new();
        for (kind, member, score) in &ops {
            match kind {
                0 => {
                    let _ = ZSetOps::add(&mut zset, member.as_bytes(), *score);
                    reference.insert(member.clone(), *score);
                }
                1 => {
                    let _ = ZSetOps::remove(&mut zset, member.as_bytes());
                    reference.remove(member);
                }
                _ => unreachable!(),
            }
        }

        // --- remove_range_by_score ---
        let (a, b) = if score_min <= score_max {
            (score_min, score_max)
        } else {
            (score_max, score_min)
        };
        for low_inc in [true, false] {
            for high_inc in [true, false] {
                let mut zm = zset.clone();
                let mut rm = reference.clone();
                let removed = zm.remove_range_by_score((a, low_inc), (b, high_inc));
                rm.retain(|_k, v| {
                    let lo = if low_inc { *v >= a } else { *v > a };
                    let hi = if high_inc { *v <= b } else { *v < b };
                    !(lo && hi)
                });
                prop_assert_eq!(removed, reference.len() - rm.len());
                prop_assert_eq!(zm.len(), rm.len());
                for (m, s) in zm.sorted_entries() {
                    let ms = String::from_utf8_lossy(&m).to_string();
                    prop_assert_eq!(rm.get(&ms), Some(&s));
                }
            }
        }

        // --- remove_range_by_rank ---
        let n = zset.len();
        let rl = rank_low.min(n.saturating_sub(1));
        let rh = rank_high.max(rl).min(n.saturating_sub(1));
        {
            let mut zm = zset.clone();
            let removed = zm.remove_range_by_rank(rl, rh);
            let expected: Vec<(Bytes, f64)> = {
                let mut sorted = zset.sorted_entries();
                let tail = if rh + 1 < sorted.len() { sorted.split_off(rh + 1) } else { Vec::new() };
                let keep_head = if rl < sorted.len() { sorted.drain(..rl).collect() } else { sorted };
                let mut v: Vec<(Bytes, f64)> = keep_head;
                v.extend(tail);
                v
            };
            let should_remove = if n == 0 { 0 } else { (rh - rl + 1).min(n) };
            prop_assert_eq!(removed, should_remove, "remove_range_by_rank wrong count rl={} rh={} n={}", rl, rh, n);
            prop_assert_eq!(zm.len(), expected.len());
            for (a, b) in zm.sorted_entries().iter().zip(expected.iter()) {
                prop_assert_eq!(&a.0, &b.0);
                prop_assert!((a.1 - b.1).abs() < 1e-9);
            }
        }

        {
            let zm = zset.clone();
            prop_assert_eq!(zm.len(), zset.len());
            let left = zset.sorted_entries();
            let right = zm.sorted_entries();
            prop_assert_eq!(left.len(), right.len());
            for (a, b) in left.iter().zip(right.iter()) {
                prop_assert_eq!(&a.0, &b.0);
                prop_assert!((a.1 - b.1).abs() < 1e-9);
            }
        }
    }
}
