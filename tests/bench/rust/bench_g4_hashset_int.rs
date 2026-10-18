#[path = "common.rs"]
mod common;

use common::{measure_destructive, measure_readonly, snapshot_mem, write_csv, CaseRow};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashSet;
use std::hint::black_box;

const N: usize = 100_000;
const SEED: u64 = 0xDEADBEEF;
const GROUP: &str = "G4";

type IntSet = HashSet<i64>;

fn build_seq() -> IntSet {
    (0..N as i64).collect()
}

fn build_rand(seed: u64) -> IntSet {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut s = IntSet::with_capacity(N);
    while s.len() < N {
        s.insert(rng.gen_range(i64::MIN..i64::MAX));
    }
    s
}

fn sanity() {
    let mut s = IntSet::new();
    for i in 0..64i64 {
        s.insert(i);
    }
    assert_eq!(s.len(), 64);
    assert!(s.contains(&42));
    assert!(!s.contains(&99));
    assert!(s.remove(&42));
    assert_eq!(s.len(), 63);
}

fn main() {
    sanity();
    let mut rows: Vec<CaseRow> = Vec::new();

    {
        let (_p, live_bytes, rss) = snapshot_mem(build_seq);
        let mut i = 0i64;
        let samples = measure_destructive(
            |_| {
                let v = i;
                i += 1;
                (IntSet::new(), v)
            },
            |s, v| {
                s.insert(*v);
            },
        );
        rows.push(CaseRow::from_samples(
            GROUP, "add_seq", samples, live_bytes, rss,
        ));
    }

    {
        let (_p, live_bytes, rss) = snapshot_mem(|| build_rand(SEED));
        let mut rng = StdRng::seed_from_u64(SEED ^ 0xE);
        let samples = measure_destructive(
            |_| {
                let v: i64 = rng.gen_range(i64::MIN..i64::MAX);
                (IntSet::new(), v)
            },
            |s, v| {
                s.insert(*v);
            },
        );
        rows.push(CaseRow::from_samples(
            GROUP, "add_rand", samples, live_bytes, rss,
        ));
    }

    {
        let (s, live_bytes, rss) = snapshot_mem(|| build_rand(SEED));
        let hits: Vec<i64> = s.iter().copied().collect();
        let mut rng = StdRng::seed_from_u64(SEED ^ 0xF);
        let samples = measure_readonly(
            move || (s, hits),
            |(s, hits), _i| {
                let v = hits[rng.gen_range(0..hits.len())];
                black_box(s.contains(&v));
            },
        );
        rows.push(CaseRow::from_samples(
            GROUP,
            "contains_hit",
            samples,
            live_bytes,
            rss,
        ));
    }

    {
        let (s, live_bytes, rss) = snapshot_mem(build_seq);
        let mut rng = StdRng::seed_from_u64(SEED ^ 0x10);
        let samples = measure_readonly(
            move || s,
            |s, _i| {
                let v: i64 = -rng.gen_range(1..1_000_000_000_i64);
                black_box(s.contains(&v));
            },
        );
        rows.push(CaseRow::from_samples(
            GROUP,
            "contains_miss",
            samples,
            live_bytes,
            rss,
        ));
    }

    {
        let (_p, live_bytes, rss) = snapshot_mem(build_seq);
        let mut rng = StdRng::seed_from_u64(SEED ^ 0x11);
        let samples = measure_destructive(
            |_| {
                let s = build_seq();
                let victim: i64 = rng.gen_range(0..N as i64);
                (s, victim)
            },
            |s, victim| {
                black_box(s.remove(victim));
            },
        );
        rows.push(CaseRow::from_samples(
            GROUP, "remove", samples, live_bytes, rss,
        ));
    }

    write_csv(GROUP, &rows);
}
