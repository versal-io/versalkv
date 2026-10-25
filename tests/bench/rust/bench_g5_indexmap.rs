#[path = "common.rs"]
mod common;

use bytes::Bytes;
use common::{measure_destructive, measure_readonly, snapshot_mem, write_csv, CaseRow};
use indexmap::IndexMap;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::hint::black_box;
use std::time::Instant;

const N: usize = 128;
const FIELD_LEN: usize = 16;
const SEED: u64 = 0xDEADBEEF;
const GROUP: &str = "G5";

type Hash = IndexMap<Bytes, Bytes>;

fn mk_bytes(prefix: &str, i: usize) -> Bytes {
    let raw = format!("{prefix}{i:0width$}", width = FIELD_LEN - prefix.len());
    debug_assert_eq!(raw.len(), FIELD_LEN);
    Bytes::from(raw.into_bytes())
}

fn build_full() -> Hash {
    let mut h = Hash::with_capacity(N);
    for i in 0..N {
        h.insert(mk_bytes("f:", i), mk_bytes("v:", i));
    }
    h
}

fn sanity() {
    let h = build_full();
    assert_eq!(h.len(), N);
    let k = mk_bytes("f:", 0);
    assert!(h.contains_key(&k));
    let v = h.get(&k).unwrap();
    assert_eq!(v, &mk_bytes("v:", 0));
}

fn main() {
    sanity();
    let mut rows: Vec<CaseRow> = Vec::new();

    //
    {
        let (_p, live_bytes, rss) = snapshot_mem(build_full);
        let mut samples: Vec<u64> = Vec::with_capacity(N);
        for m in 0..N {
            let mut h = Hash::with_capacity(N);
            for i in 0..m {
                h.insert(mk_bytes("f:", i), mk_bytes("v:", i));
            }
            let k = mk_bytes("f:", m);
            let v = mk_bytes("v:", m);
            let t0 = Instant::now();
            h.insert(k, v);
            let dt = t0.elapsed().as_nanos() as u64;
            samples.push(dt);
        }
        rows.push(CaseRow::from_samples(
            GROUP, "hset", samples, live_bytes, rss,
        ));
    }

    {
        let (h, live_bytes, rss) = snapshot_mem(build_full);
        let mut rng = StdRng::seed_from_u64(SEED ^ 0x20);
        let samples = measure_readonly(
            move || h,
            |h, _i| {
                let idx = rng.random_range(0..N);
                let k = mk_bytes("f:", idx);
                black_box(h.get(&k));
            },
        );
        rows.push(CaseRow::from_samples(
            GROUP, "hget_hit", samples, live_bytes, rss,
        ));
    }

    {
        let (h, live_bytes, rss) = snapshot_mem(build_full);
        let mut rng = StdRng::seed_from_u64(SEED ^ 0x21);
        let samples = measure_readonly(
            move || h,
            |h, _i| {
                let idx = rng.random_range(0..N);
                let raw = format!("miss:{idx:010}");
                let k = Bytes::from(raw.into_bytes());
                black_box(h.get(&k));
            },
        );
        rows.push(CaseRow::from_samples(
            GROUP,
            "hget_miss",
            samples,
            live_bytes,
            rss,
        ));
    }

    //
    {
        let (_p, live_bytes, rss) = snapshot_mem(build_full);
        let mut rng = StdRng::seed_from_u64(SEED ^ 0x22);
        let samples = measure_destructive(
            |_| {
                let h = build_full();
                let victim = mk_bytes("f:", rng.random_range(0..N));
                (h, victim)
            },
            |h, victim| {
                black_box(h.shift_remove(victim));
            },
        );
        rows.push(CaseRow::from_samples(
            GROUP, "hdel", samples, live_bytes, rss,
        ));
    }

    write_csv(GROUP, &rows);
}
