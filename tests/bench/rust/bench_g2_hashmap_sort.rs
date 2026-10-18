//! G2: Rust ZSetSl (skiplist + dict) vs Valkey C zskiplist
//! N = 100_000. Core signals: zrangebyscore / zrank on a skiplist that
//! provides O(log N) seek + O(K) scan.

#[path = "common.rs"]
mod common;

use common::{measure_destructive, measure_readonly, snapshot_mem, write_csv, CaseRow};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use versal::foundation::io::store::structure::zset::ZSetSl;
use versal::foundation::memory::value::ZSetOps;

const N: usize = 100_000;
const SEED: u64 = 0xDEADBEEF;
const GROUP: &str = "G2";

fn gen_members(n: usize) -> Vec<Vec<u8>> {
    (0..n).map(|i| format!("m:{i}").into_bytes()).collect()
}

fn build_seq(members: &[Vec<u8>]) -> ZSetSl {
    let mut z = ZSetSl::new();
    for (i, k) in members.iter().enumerate() {
        let _ = z.add(k, i as f64);
    }
    z
}

fn build_rand(members: &[Vec<u8>], seed: u64) -> ZSetSl {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut z = ZSetSl::new();
    for k in members {
        let _ = z.add(k, rng.gen_range(-1.0e9..1.0e9));
    }
    z
}

fn sanity() {
    let members = gen_members(64);
    let z = build_seq(&members);
    assert_eq!(z.len(), 64);
    let top = z.range_by_score(
        (f64::NEG_INFINITY, false),
        (f64::INFINITY, false),
        0,
        5,
        false,
    );
    assert_eq!(top.len(), 5);
    assert_eq!(top[0].0.as_ref(), b"m:0");
}

fn main() {
    sanity();
    let members = gen_members(N);
    let mut rows: Vec<CaseRow> = Vec::new();

    // zadd_seq: empty zset -> insert one element
    {
        let (_p, live_bytes, rss) = snapshot_mem(|| build_seq(&members));
        let mut i = 0usize;
        let samples = measure_destructive(
            |_| {
                let k = format!("m:{i}").into_bytes();
                let s = i as f64;
                i += 1;
                ((ZSetSl::new(), k), s)
            },
            |st, s| {
                let (z, k) = st;
                let _ = z.add(k, *s);
            },
        );
        rows.push(CaseRow::from_samples(
            GROUP, "zadd_seq", samples, live_bytes, rss,
        ));
    }

    // zadd_rand: empty zset -> insert one with random score
    {
        let (_p, live_bytes, rss) = snapshot_mem(|| build_rand(&members, SEED));
        let mut rng = StdRng::seed_from_u64(SEED ^ 0xA);
        let mut i = 0usize;
        let samples = measure_destructive(
            |_| {
                let k = format!("m:{i}").into_bytes();
                let s: f64 = rng.gen_range(-1.0e9..1.0e9);
                i += 1;
                ((ZSetSl::new(), k), s)
            },
            |st, s| {
                let (z, k) = st;
                let _ = z.add(k, *s);
            },
        );
        rows.push(CaseRow::from_samples(
            GROUP,
            "zadd_rand",
            samples,
            live_bytes,
            rss,
        ));
    }

    // zrangebyscore_slice: fill N (random scores) -> fetch first 1000
    {
        let members_ro = members.clone();
        let (z, live_bytes, rss) = snapshot_mem(move || build_rand(&members_ro, SEED));
        let samples = measure_readonly(
            move || z.clone(),
            |z, _i| {
                let out = z.range_by_score(
                    (f64::NEG_INFINITY, false),
                    (f64::INFINITY, false),
                    0,
                    1000,
                    false,
                );
                black_box(out);
            },
        );
        rows.push(CaseRow::from_samples(
            GROUP,
            "zrangebyscore_slice",
            samples,
            live_bytes,
            rss,
        ));
    }

    // zrank: fill N (random scores) -> rank a random member
    {
        let members_rank = members.clone();
        let (z, live_bytes, rss) = snapshot_mem({
            let m = members_rank.clone();
            move || build_rand(&m, SEED)
        });
        let mut rng = StdRng::seed_from_u64(SEED ^ 0xB);
        let samples = measure_readonly(
            move || (z.clone(), members_rank.clone()),
            |(z, ms), _i| {
                let idx = rng.gen_range(0..ms.len());
                black_box(z.rank(&ms[idx]));
            },
        );
        rows.push(CaseRow::from_samples(
            GROUP, "zrank", samples, live_bytes, rss,
        ));
    }

    // zrem: rebuild zset each sample -> remove one random element
    {
        let (_p, live_bytes, rss) = snapshot_mem(|| build_seq(&members));
        let mut rng = StdRng::seed_from_u64(SEED ^ 0xC);
        let members_ref = members.clone();
        let samples = measure_destructive(
            |_| {
                let z = build_seq(&members_ref);
                let victim = members_ref[rng.gen_range(0..N)].clone();
                (z, victim)
            },
            |z, victim| {
                let _ = black_box(z.remove(victim));
            },
        );
        rows.push(CaseRow::from_samples(
            GROUP, "zrem", samples, live_bytes, rss,
        ));
    }

    write_csv(GROUP, &rows);
}
