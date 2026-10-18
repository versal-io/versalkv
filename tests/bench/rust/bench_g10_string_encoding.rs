#[path = "common.rs"]
mod common;

use bytes::Bytes;
use common::{measure_destructive, measure_readonly, snapshot_mem, write_csv, CaseRow};
use std::hint::black_box;
use versal::foundation::io::store::structure::string::encoding::int_encodable;
use versal::foundation::io::store::structure::string::{StrEmbstr, StrInt, StrRaw};

const GROUP: &str = "G10";

const INT_VAL: &[u8] = b"-1234567890"; // 11 bytes,  int
const EMBSTR_VAL: &[u8] = &[b'x'; 32]; // 32 bytes, embstr
const RAW_VAL: &[u8] = &[b'y'; 128]; // 128 bytes, raw

fn build_str_int(bytes: Bytes) -> StrInt {
    let n = int_encodable(&bytes).expect("int-encodable bytes");
    StrInt::new(n, bytes)
}

fn build_str_embstr(bytes: Bytes) -> StrEmbstr {
    StrEmbstr::new(bytes)
}

fn build_str_raw(bytes: Bytes) -> StrRaw {
    StrRaw(bytes)
}

fn sanity() {
    let si = build_str_int(Bytes::from_static(INT_VAL));
    assert_eq!(si.as_bytes(), INT_VAL);
    assert_eq!(si.len(), INT_VAL.len());
    assert!(!si.is_empty());
    assert_eq!(si.value, -1234567890);

    let se = build_str_embstr(Bytes::from_static(EMBSTR_VAL));
    assert_eq!(se.as_bytes(), EMBSTR_VAL);
    assert_eq!(se.len(), EMBSTR_VAL.len());
    assert!(!se.is_empty());

    let sr = build_str_raw(Bytes::from_static(RAW_VAL));
    assert_eq!(sr.as_bytes(), RAW_VAL);
    assert_eq!(sr.len(), RAW_VAL.len());
    assert!(!sr.is_empty());
}

fn main() {
    sanity();
    let mut rows: Vec<CaseRow> = Vec::new();

    // ===== Memory footprint =====
    let (_, mem, rss) = snapshot_mem(|| build_str_int(Bytes::copy_from_slice(INT_VAL)));
    rows.push(CaseRow::from_samples(GROUP, "int_mem", vec![0], mem, rss));

    let (_, mem, rss) = snapshot_mem(|| build_str_embstr(Bytes::copy_from_slice(EMBSTR_VAL)));
    rows.push(CaseRow::from_samples(
        GROUP,
        "embstr_mem",
        vec![0],
        mem,
        rss,
    ));

    let (_, mem, rss) = snapshot_mem(|| build_str_raw(Bytes::copy_from_slice(RAW_VAL)));
    rows.push(CaseRow::from_samples(GROUP, "raw_mem", vec![0], mem, rss));

    // ===== Construction from bytes =====
    // StrInt
    let samples = measure_destructive(
        |_| (Bytes::copy_from_slice(INT_VAL), ()),
        |b, _| {
            let _ = black_box(build_str_int(b.clone()));
        },
    );
    let (_, mem, rss) = snapshot_mem(|| build_str_int(Bytes::copy_from_slice(INT_VAL)));
    rows.push(CaseRow::from_samples(
        GROUP,
        "int_from_bytes",
        samples,
        mem,
        rss,
    ));

    // StrEmbstr
    let samples = measure_destructive(
        |_| (Bytes::copy_from_slice(EMBSTR_VAL), ()),
        |b, _| {
            let _ = black_box(build_str_embstr(b.clone()));
        },
    );
    let (_, mem, rss) = snapshot_mem(|| build_str_embstr(Bytes::copy_from_slice(EMBSTR_VAL)));
    rows.push(CaseRow::from_samples(
        GROUP,
        "embstr_from_bytes",
        samples,
        mem,
        rss,
    ));

    // StrRaw
    let samples = measure_destructive(
        |_| (Bytes::copy_from_slice(RAW_VAL), ()),
        |b, _| {
            let _ = black_box(build_str_raw(b.clone()));
        },
    );
    let (_, mem, rss) = snapshot_mem(|| build_str_raw(Bytes::copy_from_slice(RAW_VAL)));
    rows.push(CaseRow::from_samples(
        GROUP,
        "raw_from_bytes",
        samples,
        mem,
        rss,
    ));

    // ===== as_bytes() access =====
    let si = build_str_int(Bytes::copy_from_slice(INT_VAL));
    let samples = measure_readonly(
        || si.clone(),
        |s, _| {
            black_box(s.as_bytes());
        },
    );
    let (_, mem, rss) = snapshot_mem(|| build_str_int(Bytes::copy_from_slice(INT_VAL)));
    rows.push(CaseRow::from_samples(
        GROUP,
        "int_as_bytes",
        samples,
        mem,
        rss,
    ));

    let se = build_str_embstr(Bytes::copy_from_slice(EMBSTR_VAL));
    let samples = measure_readonly(
        || se.clone(),
        |s, _| {
            black_box(s.as_bytes());
        },
    );
    let (_, mem, rss) = snapshot_mem(|| build_str_embstr(Bytes::copy_from_slice(EMBSTR_VAL)));
    rows.push(CaseRow::from_samples(
        GROUP,
        "embstr_as_bytes",
        samples,
        mem,
        rss,
    ));

    let sr = build_str_raw(Bytes::copy_from_slice(RAW_VAL));
    let samples = measure_readonly(
        || sr.clone(),
        |s, _| {
            black_box(s.as_bytes());
        },
    );
    let (_, mem, rss) = snapshot_mem(|| build_str_raw(Bytes::copy_from_slice(RAW_VAL)));
    rows.push(CaseRow::from_samples(
        GROUP,
        "raw_as_bytes",
        samples,
        mem,
        rss,
    ));

    // ===== len() =====
    let samples = measure_readonly(
        || si.clone(),
        |s, _| {
            black_box(s.len());
        },
    );
    let (_, mem_int, rss_int) = snapshot_mem(|| build_str_int(Bytes::copy_from_slice(INT_VAL)));
    rows.push(CaseRow::from_samples(
        GROUP, "int_len", samples, mem_int, rss_int,
    ));

    let samples = measure_readonly(
        || se.clone(),
        |s, _| {
            black_box(s.len());
        },
    );
    let (_, mem_emb, rss_emb) =
        snapshot_mem(|| build_str_embstr(Bytes::copy_from_slice(EMBSTR_VAL)));
    rows.push(CaseRow::from_samples(
        GROUP,
        "embstr_len",
        samples,
        mem_emb,
        rss_emb,
    ));

    let samples = measure_readonly(
        || sr.clone(),
        |s, _| {
            black_box(s.len());
        },
    );
    let (_, mem_raw, rss_raw) = snapshot_mem(|| build_str_raw(Bytes::copy_from_slice(RAW_VAL)));
    rows.push(CaseRow::from_samples(
        GROUP, "raw_len", samples, mem_raw, rss_raw,
    ));

    // ===== is_empty() =====
    let samples = measure_readonly(
        || si.clone(),
        |s, _| {
            black_box(s.is_empty());
        },
    );
    rows.push(CaseRow::from_samples(
        GROUP,
        "int_is_empty",
        samples,
        mem_int,
        rss_int,
    ));

    let samples = measure_readonly(
        || se.clone(),
        |s, _| {
            black_box(s.is_empty());
        },
    );
    rows.push(CaseRow::from_samples(
        GROUP,
        "embstr_is_empty",
        samples,
        mem_emb,
        rss_emb,
    ));

    let samples = measure_readonly(
        || sr.clone(),
        |s, _| {
            black_box(s.is_empty());
        },
    );
    rows.push(CaseRow::from_samples(
        GROUP,
        "raw_is_empty",
        samples,
        mem_raw,
        rss_raw,
    ));

    write_csv(GROUP, &rows);
}
