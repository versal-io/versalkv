use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;

pub use versal::util_alloc::total_allocated;

pub fn read_vmrss_kb() -> u64 {
    let s = match fs::read_to_string("/proc/self/status") {
        Ok(s) => s,
        Err(_) => return 0,
    };
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let num: String = rest
                .trim()
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect();
            return num.parse().unwrap_or(0);
        }
    }
    0
}

#[derive(Debug, Clone)]
pub struct CaseRow {
    pub group: String,
    pub case: String,
    pub median_ns: u64,
    pub p95_ns: u64,
    pub ops_per_sec: u64,
    pub live_bytes: usize,
    pub rss_kb: u64,
}

impl CaseRow {
    pub fn from_samples(
        group: &str,
        case: &str,
        mut samples: Vec<u64>,
        live_bytes: usize,
        rss_kb: u64,
    ) -> Self {
        samples.sort_unstable();
        let n = samples.len();
        let median = if n == 0 { 0 } else { samples[n / 2] };
        let p95 = if n == 0 {
            0
        } else {
            samples[(n * 95 / 100).min(n - 1)]
        };
        let ops_per_sec = 1_000_000_000u64.checked_div(median).unwrap_or(0);
        Self {
            group: group.to_string(),
            case: case.to_string(),
            median_ns: median,
            p95_ns: p95,
            ops_per_sec,
            live_bytes,
            rss_kb,
        }
    }
}

pub fn out_dir() -> PathBuf {
    let base =
        std::env::var("BENCH_OUT_DIR").unwrap_or_else(|_| "target/bench-results/rust".into());
    let p = PathBuf::from(base);
    fs::create_dir_all(&p).expect("create BENCH_OUT_DIR");
    p
}

pub fn write_csv(group: &str, rows: &[CaseRow]) {
    let path = out_dir().join(format!("{group}.csv"));
    let mut f = File::create(&path).expect("create csv");
    writeln!(
        f,
        "group,case,median_ns_per_op,p95_ns_per_op,ops_per_sec,live_bytes,rss_kb"
    )
    .unwrap();
    for r in rows {
        writeln!(
            f,
            "{},{},{},{},{},{},{}",
            r.group, r.case, r.median_ns, r.p95_ns, r.ops_per_sec, r.live_bytes, r.rss_kb
        )
        .unwrap();
    }
    eprintln!("[bench] wrote {}", path.display());
}

pub const WARMUP: usize = 3;
pub const SAMPLES: usize = 30;

pub fn measure_destructive<S, E, F>(mut setup: impl FnMut(usize) -> (S, E), mut op: F) -> Vec<u64>
where
    F: FnMut(&mut S, &E),
{
    let mut samples = Vec::with_capacity(SAMPLES);
    for i in 0..(WARMUP + SAMPLES) {
        let (mut s, e) = setup(i);
        let t0 = Instant::now();
        op(&mut s, &e);
        let dt = t0.elapsed().as_nanos() as u64;
        if i >= WARMUP {
            samples.push(dt);
        }
    }
    samples
}

pub fn measure_readonly<S, F>(setup: impl FnOnce() -> S, mut op: F) -> Vec<u64>
where
    F: FnMut(&S, usize),
{
    let s = setup();
    let mut samples = Vec::with_capacity(SAMPLES);
    for i in 0..(WARMUP + SAMPLES) {
        let t0 = Instant::now();
        op(&s, i);
        let dt = t0.elapsed().as_nanos() as u64;
        if i >= WARMUP {
            samples.push(dt);
        }
    }
    samples
}

pub fn snapshot_mem<T>(build: impl FnOnce() -> T) -> (T, usize, u64) {
    let before = total_allocated();
    let t = build();
    let after = total_allocated();
    let rss = read_vmrss_kb();
    (t, after.saturating_sub(before), rss)
}
