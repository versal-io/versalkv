#[path = "common.rs"]
mod common;

use common::{measure_destructive, measure_readonly, snapshot_mem, write_csv, CaseRow};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::collections::VecDeque;
use std::hint::black_box;

const N: usize = 1_000_000;
const ELEM_LEN: usize = 16;
const SEED: u64 = 0xDEADBEEF;
const GROUP: &str = "G3";

type Elem = Vec<u8>;
type List = VecDeque<Elem>;

fn mk_elem(i: usize) -> Elem {
    let mut v = Vec::with_capacity(ELEM_LEN);
    v.extend_from_slice(format!("{i:016}").as_bytes());
    debug_assert_eq!(v.len(), ELEM_LEN);
    v
}

fn build_full() -> List {
    let mut l = List::with_capacity(N);
    for i in 0..N {
        l.push_back(mk_elem(i));
    }
    l
}

fn sanity() {
    let mut l = List::new();
    for i in 0..32 {
        l.push_back(mk_elem(i));
    }
    assert_eq!(l.len(), 32);
    assert_eq!(l[0], mk_elem(0));
    assert_eq!(l.pop_front().unwrap(), mk_elem(0));
    assert_eq!(l.len(), 31);
}

fn main() {
    sanity();
    let mut rows: Vec<CaseRow> = Vec::new();

    {
        let (_p, live_bytes, rss) = snapshot_mem(build_full);
        let mut i = 0usize;
        let samples = measure_destructive(
            |_| {
                let e = mk_elem(i);
                i += 1;
                (List::new(), e)
            },
            |l, e| {
                l.push_back(e.clone());
            },
        );
        rows.push(CaseRow::from_samples(
            GROUP,
            "push_back",
            samples,
            live_bytes,
            rss,
        ));
    }

    {
        let (_p, live_bytes, rss) = snapshot_mem(build_full);
        let mut i = 0usize;
        let samples = measure_destructive(
            |_| {
                let e = mk_elem(i);
                i += 1;
                (List::new(), e)
            },
            |l, e| {
                l.push_front(e.clone());
            },
        );
        rows.push(CaseRow::from_samples(
            GROUP,
            "push_front",
            samples,
            live_bytes,
            rss,
        ));
    }

    {
        let (_p, live_bytes, rss) = snapshot_mem(build_full);
        let samples = measure_destructive(
            |_| (build_full(), ()),
            |l, _| {
                black_box(l.pop_front());
            },
        );
        rows.push(CaseRow::from_samples(
            GROUP,
            "pop_front",
            samples,
            live_bytes,
            rss,
        ));
    }

    {
        let (l, live_bytes, rss) = snapshot_mem(build_full);
        let mut rng = StdRng::seed_from_u64(SEED ^ 0xD);
        let samples = measure_readonly(
            move || l,
            |l, _i| {
                let idx = rng.random_range(0..l.len());
                black_box(&l[idx]);
            },
        );
        rows.push(CaseRow::from_samples(
            GROUP,
            "index_random",
            samples,
            live_bytes,
            rss,
        ));
    }

    {
        let (l, live_bytes, rss) = snapshot_mem(build_full);
        let samples = measure_readonly(
            move || l,
            |l, _i| {
                let mut acc: u64 = 0;
                for e in l.iter() {
                    acc = acc.wrapping_add(e[0] as u64);
                }
                black_box(acc);
            },
        );
        rows.push(CaseRow::from_samples(
            GROUP,
            "range_slice",
            samples,
            live_bytes,
            rss,
        ));
    }

    write_csv(GROUP, &rows);
}
