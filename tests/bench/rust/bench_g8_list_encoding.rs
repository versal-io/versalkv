#[path = "common.rs"]
mod common;

use common::{measure_destructive, measure_readonly, snapshot_mem, write_csv, CaseRow};
use std::hint::black_box;
use versal::foundation::io::store::structure::list::ListValue;
use versal::foundation::memory::value::ListOps;

const GROUP: &str = "G8";
const SIZES: &[usize] = &[128, 512, 2048];

fn elem(i: usize) -> Vec<u8> {
    format!("e{:06}", i).into_bytes()
}

fn build_lp(n: usize) -> ListValue {
    let mut l = ListValue::new();
    l.set_encoding_override("listpack");
    for i in 0..n {
        let _ = l.push_back(&elem(i));
    }
    l
}

fn build_qd(n: usize) -> ListValue {
    let mut l = ListValue::new();
    l.set_encoding_override("quicklist");
    for i in 0..n {
        let _ = l.push_back(&elem(i));
    }
    l
}

fn sanity() {
    let lp = build_lp(50);
    assert_eq!(ListOps::len(&lp), 50);
    assert_eq!(ListOps::get(&lp, 0), Some(elem(0)));

    let qd = build_qd(50);
    assert_eq!(ListOps::len(&qd), 50);
    assert_eq!(ListOps::get(&qd, 0), Some(elem(0)));
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
        let (_, mem, rss) = snapshot_mem(|| build_qd(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("quicklist_mem_n{}", n),
            vec![0],
            mem,
            rss,
        ));

        // ===== push_back =====
        let new_elem = elem(n);
        let samples = measure_destructive(
            |_| (build_lp(n), new_elem.clone()),
            |l, e| {
                let _ = black_box(ListOps::push_back(l, e));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_lp(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("listpack_push_back_n{}", n),
            samples,
            mem,
            rss,
        ));

        let samples = measure_destructive(
            |_| (build_qd(n), new_elem.clone()),
            |l, e| {
                let _ = black_box(ListOps::push_back(l, e));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_qd(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("quicklist_push_back_n{}", n),
            samples,
            mem,
            rss,
        ));

        // ===== push_front =====
        let samples = measure_destructive(
            |_| (build_lp(n), new_elem.clone()),
            |l, e| {
                let _ = black_box(ListOps::push_front(l, e));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_lp(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("listpack_push_front_n{}", n),
            samples,
            mem,
            rss,
        ));

        let samples = measure_destructive(
            |_| (build_qd(n), new_elem.clone()),
            |l, e| {
                let _ = black_box(ListOps::push_front(l, e));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_qd(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("quicklist_push_front_n{}", n),
            samples,
            mem,
            rss,
        ));

        // ===== pop_back =====
        let samples = measure_destructive(
            |_| (build_lp(n), ()),
            |l, _| {
                let _ = black_box(ListOps::pop_back(l));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_lp(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("listpack_pop_back_n{}", n),
            samples,
            mem,
            rss,
        ));

        let samples = measure_destructive(
            |_| (build_qd(n), ()),
            |l, _| {
                let _ = black_box(ListOps::pop_back(l));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_qd(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("quicklist_pop_back_n{}", n),
            samples,
            mem,
            rss,
        ));

        // ===== pop_front =====
        let samples = measure_destructive(
            |_| (build_lp(n), ()),
            |l, _| {
                let _ = black_box(ListOps::pop_front(l));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_lp(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("listpack_pop_front_n{}", n),
            samples,
            mem,
            rss,
        ));

        let samples = measure_destructive(
            |_| (build_qd(n), ()),
            |l, _| {
                let _ = black_box(ListOps::pop_front(l));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_qd(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("quicklist_pop_front_n{}", n),
            samples,
            mem,
            rss,
        ));

        // ===== get head index =====
        let samples = measure_readonly(
            || build_lp(n),
            |l, _| {
                black_box(ListOps::get(l, 0));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_lp(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("listpack_get_head_n{}", n),
            samples,
            mem,
            rss,
        ));

        let samples = measure_readonly(
            || build_qd(n),
            |l, _| {
                black_box(ListOps::get(l, 0));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_qd(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("quicklist_get_head_n{}", n),
            samples,
            mem,
            rss,
        ));

        // ===== get middle index =====
        let mid = (n as isize) / 2;
        let samples = measure_readonly(
            || build_lp(n),
            |l, _| {
                black_box(ListOps::get(l, mid));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_lp(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("listpack_get_mid_n{}", n),
            samples,
            mem,
            rss,
        ));

        let samples = measure_readonly(
            || build_qd(n),
            |l, _| {
                black_box(ListOps::get(l, mid));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_qd(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("quicklist_get_mid_n{}", n),
            samples,
            mem,
            rss,
        ));

        // ===== get tail index =====
        let tail = (n as isize) - 1;
        let samples = measure_readonly(
            || build_lp(n),
            |l, _| {
                black_box(ListOps::get(l, tail));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_lp(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("listpack_get_tail_n{}", n),
            samples,
            mem,
            rss,
        ));

        let samples = measure_readonly(
            || build_qd(n),
            |l, _| {
                black_box(ListOps::get(l, tail));
            },
        );
        let (_, mem, rss) = snapshot_mem(|| build_qd(n));
        rows.push(CaseRow::from_samples(
            GROUP,
            &format!("quicklist_get_tail_n{}", n),
            samples,
            mem,
            rss,
        ));
    }

    write_csv(GROUP, &rows);
}
