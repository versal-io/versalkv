#[path = "common.rs"]
mod common;

use common::{measure_destructive, measure_readonly, snapshot_mem, write_csv, CaseRow};
use std::hint::black_box;
use versal::foundation::io::store::structure::hash::HashHt;
use versal::foundation::memory::value::HashOps;

const GROUP: &str = "G7";
const SIZES: &[usize] = &[128, 512, 2048];

fn field(i: usize) -> Vec<u8> {
    format!("f{:06}", i).into_bytes()
}

fn value(i: usize) -> Vec<u8> {
    format!("v{:06}_val", i).into_bytes()
}

fn build_lp(n: usize) -> HashHt {
    let mut h = HashHt::new();
    for i in 0..n {
        let _ = h.set(&field(i), &value(i));
    }
    h
}

fn build_ht(n: usize) -> HashHt {
    let mut h = HashHt::new();
    for i in 0..n {
        let _ = h.set(&field(i), &value(i));
    }
    h
}

fn sanity() {
    let lp = build_lp(50);
    assert_eq!(lp.len(), 50);
    assert_eq!(lp.get(&field(25)), Some(value(25)));

    let ht = build_ht(50);
    assert_eq!(ht.len(), 50);
    assert_eq!(ht.get(&field(25)), Some(value(25)));
}

fn main() {
    sanity();
    let mut rows: Vec<CaseRow> = Vec::new();

    for &n in SIZES {
        // ===== Memory =====
        let (_, mem, rss) = snapshot_mem(|| build_lp(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("listpack_mem_n{}", n),
            vec![0],
            mem,
            rss,
        ));
        let (_, mem, rss) = snapshot_mem(|| build_ht(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("hashtable_mem_n{}", n),
            vec![0],
            mem,
            rss,
        ));

        // ===== set new =====
        let samples = measure_destructive(
            |_| (build_lp(n - 1), (field(n - 1), value(n - 1))),
            |h, (k, v)| {
                let _ = black_box(h.set(k, v));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_lp(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("listpack_set_new_n{}", n),
            samples,
            mem,
            rss,
        ));

        let samples = measure_destructive(
            |_| (build_ht(n - 1), (field(n - 1), value(n - 1))),
            |h, (k, v)| {
                let _ = black_box(h.set(k, v));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_ht(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("hashtable_set_new_n{}", n),
            samples,
            mem,
            rss,
        ));

        // ===== set update =====
        let samples = measure_destructive(
            |_| (build_lp(n), (field(n / 2), b"updated_val".to_vec())),
            |h, (k, v)| {
                let _ = black_box(h.set(k, v));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_lp(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("listpack_set_update_n{}", n),
            samples,
            mem,
            rss,
        ));

        let samples = measure_destructive(
            |_| (build_ht(n), (field(n / 2), b"updated_val".to_vec())),
            |h, (k, v)| {
                let _ = black_box(h.set(k, v));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_ht(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("hashtable_set_update_n{}", n),
            samples,
            mem,
            rss,
        ));

        // ===== get hit =====
        let target = field(n / 2);
        let samples = measure_readonly(
            || build_lp(n),
            |h, _| {
                black_box(h.get(&target));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_lp(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("listpack_get_hit_n{}", n),
            samples,
            mem,
            rss,
        ));

        let samples = measure_readonly(
            || build_ht(n),
            |h, _| {
                black_box(h.get(&target));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_ht(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("hashtable_get_hit_n{}", n),
            samples,
            mem,
            rss,
        ));

        // ===== get miss =====
        let miss = b"nonexistent_field_xxxx".to_vec();
        let samples = measure_readonly(
            || build_lp(n),
            |h, _| {
                black_box(h.get(&miss));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_lp(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("listpack_get_miss_n{}", n),
            samples,
            mem,
            rss,
        ));

        let samples = measure_readonly(
            || build_ht(n),
            |h, _| {
                black_box(h.get(&miss));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_ht(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("hashtable_get_miss_n{}", n),
            samples,
            mem,
            rss,
        ));

        // ===== del =====
        let target = field(n / 2);
        let samples = measure_destructive(
            |_| (build_lp(n), target.clone()),
            |h, k| {
                let _ = black_box(h.del(k));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_lp(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("listpack_del_n{}", n),
            samples,
            mem,
            rss,
        ));

        let samples = measure_destructive(
            |_| (build_ht(n), target.clone()),
            |h, k| {
                let _ = black_box(h.del(k));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_ht(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("hashtable_del_n{}", n),
            samples,
            mem,
            rss,
        ));
    }

    write_csv(GROUP, &rows);
}
