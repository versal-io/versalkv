#[path = "common.rs"]
mod common;

use common::{measure_destructive, measure_readonly, snapshot_mem, write_csv, CaseRow};
use std::hint::black_box;
use versal::foundation::io::store::structure::set::{IntSet, SetEncoding, ValkeySet};
use versal::foundation::memory::value::SetOps;

const GROUP: &str = "G6";
const N_SMALL: usize = 128; // listpack
const N_MED: usize = 512; //  intset
const N_LARGE: usize = 1024; //  hashtable

fn int_member(i: usize) -> Vec<u8> {
    i.to_string().into_bytes()
}

fn build_intset(n: usize) -> IntSet {
    let mut s = IntSet::new();
    for i in 0..n {
        let _ = IntSet::add(&mut s, i as i64, usize::MAX);
    }
    s
}

fn build_setlp(n: usize) -> ValkeySet {
    let mut s = ValkeySet::new();
    s.encoding = SetEncoding::Listpack;
    for i in 0..n {
        let _ = SetOps::add(&mut s, &int_member(i));
    }
    s
}

fn build_hashtable(n: usize) -> ValkeySet {
    use bytes::Bytes;
    use std::collections::HashSet;
    let mut data: HashSet<Bytes> = HashSet::with_capacity(n);
    for i in 0..n {
        data.insert(Bytes::from(int_member(i)));
    }
    ValkeySet::from_hashset(data)
}

fn sanity() {
    let is = build_intset(100);
    assert_eq!(SetOps::len(&is), 100);
    assert!(SetOps::contains(&is, &int_member(42)));

    let lp = build_setlp(100);
    assert_eq!(SetOps::len(&lp), 100);
    assert!(SetOps::contains(&lp, &int_member(42)));

    let ht = build_hashtable(100);
    assert_eq!(SetOps::len(&ht), 100);
    assert!(SetOps::contains(&ht, &int_member(42)));
}

fn main() {
    sanity();
    let mut rows: Vec<CaseRow> = Vec::new();

    for &n in &[N_SMALL, N_MED, N_LARGE] {
        let (_, mem, rss) = snapshot_mem(|| build_intset(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("intset_mem_n{}", n),
            vec![0],
            mem,
            rss,
        ));

        let (_, mem, rss) = snapshot_mem(|| build_setlp(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("setlp_mem_n{}", n),
            vec![0],
            mem,
            rss,
        ));

        let (_, mem, rss) = snapshot_mem(|| build_hashtable(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("hashtable_mem_n{}", n),
            vec![0],
            mem,
            rss,
        ));
    }

    // ===== add (destructive) =====
    for &n in &[N_SMALL, N_MED, N_LARGE] {
        // IntSet add
        let samples = measure_destructive(
            |_| (build_intset(n - 1), (n - 1) as i64),
            |s, v| {
                let _ = black_box(IntSet::add(s, *v, usize::MAX));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_intset(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("intset_add_n{}", n),
            samples,
            mem,
            rss,
        ));

        // SetLp add
        let samples = measure_destructive(
            |_| (build_setlp(n - 1), int_member(n - 1)),
            |s, m| {
                let _ = black_box(SetOps::add(s, m));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_setlp(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("setlp_add_n{}", n),
            samples,
            mem,
            rss,
        ));

        // Hashtable add
        let samples = measure_destructive(
            |_| (build_hashtable(n - 1), int_member(n - 1)),
            |s, m| {
                let _ = black_box(SetOps::add(s, m));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_hashtable(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("hashtable_add_n{}", n),
            samples,
            mem,
            rss,
        ));
    }

    // ===== contains hit (readonly) =====
    for &n in &[N_SMALL, N_MED, N_LARGE] {
        let target = int_member(n / 2);

        let s = build_intset(n);
        let samples = measure_readonly(
            || s.clone(),
            |s, _| {
                black_box(SetOps::contains(s, &target));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_intset(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("intset_contains_hit_n{}", n),
            samples,
            mem,
            rss,
        ));

        let s = build_setlp(n);
        let samples = measure_readonly(
            || s.clone(),
            |s, _| {
                black_box(SetOps::contains(s, &target));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_setlp(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("setlp_contains_hit_n{}", n),
            samples,
            mem,
            rss,
        ));

        let s = build_hashtable(n);
        let samples = measure_readonly(
            || s.clone(),
            |s, _| {
                black_box(SetOps::contains(s, &target));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_hashtable(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("hashtable_contains_hit_n{}", n),
            samples,
            mem,
            rss,
        ));
    }

    // ===== contains miss (readonly) =====
    for &n in &[N_SMALL, N_MED, N_LARGE] {
        let miss = format!("nonexistent_{}", n + 9999).into_bytes();

        let s = build_intset(n);
        let samples = measure_readonly(
            || s.clone(),
            |s, _| {
                black_box(SetOps::contains(s, &miss));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_intset(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("intset_contains_miss_n{}", n),
            samples,
            mem,
            rss,
        ));

        let s = build_setlp(n);
        let samples = measure_readonly(
            || s.clone(),
            |s, _| {
                black_box(SetOps::contains(s, &miss));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_setlp(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("setlp_contains_miss_n{}", n),
            samples,
            mem,
            rss,
        ));

        let s = build_hashtable(n);
        let samples = measure_readonly(
            || s.clone(),
            |s, _| {
                black_box(SetOps::contains(s, &miss));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_hashtable(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("hashtable_contains_miss_n{}", n),
            samples,
            mem,
            rss,
        ));
    }

    // ===== remove (destructive) =====
    for &n in &[N_SMALL, N_MED, N_LARGE] {
        let target = int_member(n / 2);

        let samples = measure_destructive(
            |_| (build_intset(n), (n / 2) as i64),
            |s, v| {
                let _ = black_box(IntSet::remove(s, *v));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_intset(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("intset_remove_n{}", n),
            samples,
            mem,
            rss,
        ));

        let samples = measure_destructive(
            |_| (build_setlp(n), target.clone()),
            |s, m| {
                let _ = black_box(SetOps::remove(s, m));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_setlp(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("setlp_remove_n{}", n),
            samples,
            mem,
            rss,
        ));

        let samples = measure_destructive(
            |_| (build_hashtable(n), target.clone()),
            |s, m| {
                let _ = black_box(SetOps::remove(s, m));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_hashtable(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("hashtable_remove_n{}", n),
            samples,
            mem,
            rss,
        ));
    }

    write_csv(GROUP, &rows);
}
