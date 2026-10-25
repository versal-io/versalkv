#[path = "common.rs"]
mod common;

use common::{measure_destructive, measure_readonly, snapshot_mem, write_csv, CaseRow};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::hint::black_box;
use versal::foundation::io::store::structure::dict::Dict;

const N: usize = 1_000_000;
const SEED: u64 = 0xDEADBEEF;
const GROUP: &str = "G1";

fn gen_keys(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("key:{i}")).collect()
}

fn build_full(keys: &[String]) -> Dict<String, u64> {
    let mut d = Dict::new();
    for (i, k) in keys.iter().enumerate() {
        d.insert(k.clone(), i as u64);
    }
    d
}

fn sanity() {
    let mut d = Dict::new();
    for i in 0..100u64 {
        d.insert(format!("k{i}"), i);
    }
    assert_eq!(d.len(), 100);
    assert_eq!(d.get("k0"), Some(&0));
    assert_eq!(d.get("kzzz"), None);
    assert_eq!(d.remove("k42"), Some(42));
    assert_eq!(d.len(), 99);
}

fn main() {
    sanity();
    let keys = gen_keys(N);
    let mut rows: Vec<CaseRow> = Vec::new();

    {
        let (_probe, live_bytes, rss) = snapshot_mem(|| build_full(&keys));
        let mut i = 0usize;
        let samples = measure_destructive(
            |_| {
                let k = format!("k{i}");
                i += 1;
                (Dict::<String, u64>::new(), k)
            },
            |d, k| {
                d.insert(k.clone(), 1);
            },
        );
        rows.push(CaseRow::from_samples(
            GROUP,
            "insert_seq",
            samples,
            live_bytes,
            rss,
        ));
    }

    {
        let mut rng = StdRng::seed_from_u64(SEED);
        let keys_ro = keys.clone();
        let (d, live_bytes, rss) = snapshot_mem(|| build_full(&keys_ro));
        let samples = measure_readonly(
            move || (d, keys_ro),
            |(d, ks), _i| {
                let idx = rng.random_range(0..ks.len());
                black_box(d.get(ks[idx].as_str()));
            },
        );
        rows.push(CaseRow::from_samples(
            GROUP,
            "lookup_hit",
            samples,
            live_bytes,
            rss,
        ));
    }

    {
        let mut rng = StdRng::seed_from_u64(SEED ^ 0x1);
        let keys_local = gen_keys(N);
        let (d, live_bytes, rss) = snapshot_mem(|| build_full(&keys_local));
        let miss_keys: Vec<String> = (0..N).map(|i| format!("miss:{i}")).collect();
        let samples = measure_readonly(
            move || (d, miss_keys),
            |(d, mk), _i| {
                let idx = rng.random_range(0..mk.len());
                black_box(d.get(mk[idx].as_str()));
            },
        );
        rows.push(CaseRow::from_samples(
            GROUP,
            "lookup_miss",
            samples,
            live_bytes,
            rss,
        ));
    }

    {
        let (_probe, live_bytes, rss) = snapshot_mem(|| build_full(&keys));
        let mut rng = StdRng::seed_from_u64(SEED ^ 0x2);
        let keys_local = keys.clone();
        let samples = measure_destructive(
            |_| {
                let d = build_full(&keys_local);
                let victim = keys_local[rng.random_range(0..N)].clone();
                (d, victim)
            },
            |d, victim| {
                black_box(d.remove(victim.as_str()));
            },
        );
        rows.push(CaseRow::from_samples(
            GROUP, "delete", samples, live_bytes, rss,
        ));
    }

    {
        let keys_local = gen_keys(N);
        let (d, live_bytes, rss) = snapshot_mem(|| build_full(&keys_local));
        let samples = measure_readonly(
            move || d,
            |d, _i| {
                let mut acc: u64 = 0;
                for (_k, v) in d.iter() {
                    acc = acc.wrapping_add(*v);
                }
                black_box(acc);
            },
        );
        rows.push(CaseRow::from_samples(
            GROUP, "iterate", samples, live_bytes, rss,
        ));
    }

    {
        const LONG_N: usize = 100_000;
        const LONG_KEY_LEN: usize = 128;
        let long_keys: Vec<String> = (0..LONG_N)
            .map(|i| {
                let mut s = format!("longkey:{i:08}:");
                while s.len() < LONG_KEY_LEN {
                    s.push('x');
                }
                s.truncate(LONG_KEY_LEN);
                s
            })
            .collect();
        let mut rng = StdRng::seed_from_u64(SEED ^ 0x3);
        let long_keys_ro = long_keys.clone();
        let (d, live_bytes, rss) = snapshot_mem(|| {
            let mut d = Dict::<String, u64>::new();
            for (i, k) in long_keys_ro.iter().enumerate() {
                d.insert(k.clone(), i as u64);
            }
            d
        });
        let samples = measure_readonly(
            move || (d, long_keys_ro),
            |(d, ks), _i| {
                let idx = rng.random_range(0..ks.len());
                black_box(d.get(ks[idx].as_str()));
            },
        );
        rows.push(CaseRow::from_samples(
            GROUP,
            "long_key_lookup",
            samples,
            live_bytes,
            rss,
        ));
    }

    {
        const REHASH_TRIGGER: usize = (1 << 19) + 1;
        let mut base_keys = Vec::with_capacity(REHASH_TRIGGER);
        for i in 0..REHASH_TRIGGER {
            base_keys.push(format!("key:{i}"));
        }
        let base_keys = base_keys;
        let (_probe, live_bytes, rss) = snapshot_mem(|| {
            let mut d = Dict::<String, u64>::new();
            for (i, k) in base_keys.iter().enumerate() {
                d.insert(k.clone(), i as u64);
            }
            d
        });
        let mut counter = 0usize;
        let samples = measure_destructive(
            |_| {
                let mut d = Dict::<String, u64>::new();
                for (i, k) in base_keys.iter().enumerate() {
                    d.insert(k.clone(), i as u64);
                }
                let trigger = format!("trigger:{}", counter);
                counter += 1;
                (d, trigger)
            },
            |d, trigger| {
                d.insert(trigger.clone(), 0xDEADu64);
            },
        );
        rows.push(CaseRow::from_samples(
            GROUP,
            "insert_during_rehash",
            samples,
            live_bytes,
            rss,
        ));
    }

    write_csv(GROUP, &rows);
}
