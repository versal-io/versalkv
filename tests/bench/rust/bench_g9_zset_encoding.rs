#[path = "common.rs"]
mod common;

use common::{measure_destructive, measure_readonly, snapshot_mem, write_csv, CaseRow};
use std::hint::black_box;
use versal::foundation::io::store::structure::zset::ZSetSl;
use versal::foundation::memory::value::ZSetOps;

const GROUP: &str = "G9";
const SIZES: &[usize] = &[128, 512, 2048];

fn member(i: usize) -> Vec<u8> {
    format!("m{:06}", i).into_bytes()
}

fn build_lp(n: usize) -> ZSetSl {
    let mut z = ZSetSl::new();
    for i in 0..n {
        let _ = z.add(&member(i), i as f64);
    }
    z
}

fn build_sl(n: usize) -> ZSetSl {
    let mut z = ZSetSl::new();
    for i in 0..n {
        let _ = z.add(&member(i), i as f64);
    }
    z
}

fn sanity() {
    let lp = build_lp(50);
    assert_eq!(lp.len(), 50);
    assert_eq!(lp.score(&member(25)), Some(25.0));

    let sl = build_sl(50);
    assert_eq!(sl.len(), 50);
    assert_eq!(sl.score(&member(25)), Some(25.0));
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
        let (_, mem, rss) = snapshot_mem(|| build_sl(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("skiplist_mem_n{}", n),
            vec![0],
            mem,
            rss,
        ));

        // ===== add new =====
        let new_mem = member(n);
        let new_score = n as f64;
        let samples = measure_destructive(
            |_| (build_lp(n - 1), (new_mem.clone(), new_score)),
            |z, (m, s)| {
                let _ = black_box(z.add(m, *s));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_lp(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("listpack_add_new_n{}", n),
            samples,
            mem,
            rss,
        ));

        let samples = measure_destructive(
            |_| (build_sl(n - 1), (new_mem.clone(), new_score)),
            |z, (m, s)| {
                let _ = black_box(z.add(m, *s));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_sl(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("skiplist_add_new_n{}", n),
            samples,
            mem,
            rss,
        ));

        // ===== add (update score) =====
        let target = member(n / 2);
        let samples = measure_destructive(
            |_| (build_lp(n), (target.clone(), 9999.0)),
            |z, (m, s)| {
                let _ = black_box(z.add(m, *s));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_lp(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("listpack_add_update_n{}", n),
            samples,
            mem,
            rss,
        ));

        let samples = measure_destructive(
            |_| (build_sl(n), (target.clone(), 9999.0)),
            |z, (m, s)| {
                let _ = black_box(z.add(m, *s));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_sl(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("skiplist_add_update_n{}", n),
            samples,
            mem,
            rss,
        ));

        // ===== score hit =====
        let target = member(n / 2);
        let samples = measure_readonly(
            || build_lp(n),
            |z, _| {
                black_box(z.score(&target));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_lp(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("listpack_score_hit_n{}", n),
            samples,
            mem,
            rss,
        ));

        let samples = measure_readonly(
            || build_sl(n),
            |z, _| {
                black_box(z.score(&target));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_sl(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("skiplist_score_hit_n{}", n),
            samples,
            mem,
            rss,
        ));

        // ===== score miss =====
        let miss = b"nonexistent_member".to_vec();
        let samples = measure_readonly(
            || build_lp(n),
            |z, _| {
                black_box(z.score(&miss));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_lp(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("listpack_score_miss_n{}", n),
            samples,
            mem,
            rss,
        ));

        let samples = measure_readonly(
            || build_sl(n),
            |z, _| {
                black_box(z.score(&miss));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_sl(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("skiplist_score_miss_n{}", n),
            samples,
            mem,
            rss,
        ));

        // ===== remove =====
        let target = member(n / 2);
        let samples = measure_destructive(
            |_| (build_lp(n), target.clone()),
            |z, m| {
                let _ = black_box(z.remove(m));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_lp(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("listpack_remove_n{}", n),
            samples,
            mem,
            rss,
        ));

        let samples = measure_destructive(
            |_| (build_sl(n), target.clone()),
            |z, m| {
                let _ = black_box(z.remove(m));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_sl(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("skiplist_remove_n{}", n),
            samples,
            mem,
            rss,
        ));
    }

    write_csv(GROUP, &rows);
}
