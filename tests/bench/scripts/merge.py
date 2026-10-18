#!/usr/bin/env python3
"""merge.py

 Rust  C  G[1-5].csv  docs/benchmarks/ds-microbench.md
 /  /  / 5  /


  target/bench-results/rust/G{1..5}.csv
  target/bench-results/c/G{1..5}.csv

  docs/benchmarks/ds-microbench.md   README
  target/bench-results/comparison.md
"""

from __future__ import annotations
import csv
import platform
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
RUST_DIR = REPO_ROOT / "target" / "bench-results" / "rust"
C_DIR = REPO_ROOT / "target" / "bench-results" / "c"
DOC_PATH = REPO_ROOT / "docs" / "benchmarks" / "ds-microbench.md"
ARCHIVE_PATH = REPO_ROOT / "target" / "bench-results" / "comparison.md"


GROUP_META: dict[str, dict[str, str]] = {
    "G1": {
        "title": "G1  Dict ",
        "rust": "`Dict` rehash insert/delete/find  bucket",
        "c": "`hashtable`Valkey 8+  + SIMD ",
        "workload": "N=1,000,000   `k:{i}`",
    },
    "G2": {
        "title": "G2  ZSet ",
        "rust": "`ZSetSl` HashTable +  zskiplist",
        "c": "`zskiplist` + dict",
        "workload": "N=100,000   score",
    },
    "G3": {
        "title": "G3  List ",
        "rust": "`VecDeque<Vec<u8>>`",
        "c": "`quicklist`listpack ",
        "workload": "N=1,000,000   elem 16 B",
    },
    "G4": {
        "title": "G4  Set (int-only)",
        "rust": "`HashSet<i64>`",
        "c": "`intset`",
        "workload": "N=100,000   i64",
    },
    "G5": {
        "title": "G5  Hash ()",
        "rust": "`IndexMap<String, String>`",
        "c": "`listpack` hash ",
        "workload": "N=128   kv  16 B",
    },
}


@dataclass
class Row:
    group: str
    case: str
    median_ns: int
    p95_ns: int
    ops_per_sec: int
    live_bytes: int
    rss_kb: int


def load(csv_path: Path) -> dict[tuple[str, str], Row]:
    rows: dict[tuple[str, str], Row] = {}
    if not csv_path.exists():
        return rows
    with csv_path.open() as f:
        for r in csv.DictReader(f):
            row = Row(
                group=r["group"],
                case=r["case"],
                median_ns=int(r["median_ns_per_op"]),
                p95_ns=int(r["p95_ns_per_op"]),
                ops_per_sec=int(r["ops_per_sec"]),
                live_bytes=int(r["live_bytes"]),
                rss_kb=int(r["rss_kb"]),
            )
            rows[(row.group, row.case)] = row
    return rows


def fmt_ns(n: int) -> str:
    if n == 0:
        return "-"
    if n < 1_000:
        return f"{n} ns"
    if n < 1_000_000:
        return f"{n/1_000:.2f} µs"
    if n < 1_000_000_000:
        return f"{n/1_000_000:.2f} ms"
    return f"{n/1_000_000_000:.2f} s"


def fmt_bytes(b: int) -> str:
    if b == 0:
        return "-"
    if b < 1024:
        return f"{b} B"
    if b < 1024 * 1024:
        return f"{b/1024:.1f} KB"
    if b < 1024 * 1024 * 1024:
        return f"{b/1024/1024:.1f} MB"
    return f"{b/1024/1024/1024:.2f} GB"


def ratio(rust_ns: int, c_ns: int) -> str:
    """Rust vs C  median >1 = Rust """
    if rust_ns == 0 or c_ns == 0:
        return "-"
    r = rust_ns / c_ns
    if r >= 1:
        return f"{r:.1f}× slower"
    return f"{1/r:.1f}× faster"


def sh(cmd: list[str]) -> str:
    try:
        return subprocess.check_output(cmd, stderr=subprocess.DEVNULL,
                                       encoding="utf-8", timeout=5).strip()
    except Exception:
        return "n/a"


def read_cpu_model() -> str:
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except Exception:
        pass
    return "n/a"


def read_mem_total_kb() -> int:
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    return int(line.split()[1])
    except Exception:
        pass
    return 0


def env_block() -> list[str]:
    mem_kb = read_mem_total_kb()
    mem_gb = f"{mem_kb / 1024 / 1024:.1f} GB" if mem_kb else "n/a"
    rustc = sh(["rustc", "--version"])
    cc = sh(["cc", "--version"]).splitlines()[0] if shutil.which("cc") else "n/a"
    cmake_ver = sh(["cmake", "--version"]).splitlines()[0] if shutil.which("cmake") else "n/a"
    kernel = sh(["uname", "-sr"])
    return [
        "|  |  |",
        "|---|---|",
        f"| CPU | {read_cpu_model()} |",
        f"| Memory | {mem_gb} |",
        f"| Kernel | {kernel} |",
        f"| Arch | {platform.machine()} |",
        f"| rustc | {rustc} |",
        f"| cc | {cc} |",
        f"| cmake | {cmake_ver} |",
    ]


def highlight_lines(rust: dict[tuple[str, str], Row],
                    c: dict[tuple[str, str], Row]) -> list[str]:
    """ 5 /"""
    pairs = []
    for key in rust.keys() & c.keys():
        r_ns = rust[key].median_ns
        c_ns = c[key].median_ns
        if r_ns == 0 or c_ns == 0:
            continue
        pairs.append((key, r_ns / c_ns, r_ns, c_ns))
    pairs.sort(key=lambda p: p[1])  #  Rust
    picks: list[tuple] = []
    if len(pairs) >= 2:
        picks.extend(pairs[:2])  # Rust  2
    if len(pairs) >= 4:
        picks.extend(pairs[-2:])  # Rust  2

    lines: list[str] = []
    for (grp, case), rel, r_ns, c_ns in picks:
        if rel < 1:
            verdict = f"Rust {1/rel:.1f}× faster"
        else:
            verdict = f"Rust {rel:.1f}× slower"
        lines.append(f"- **{grp} `{case}`** — Rust {fmt_ns(r_ns)}, C {fmt_ns(c_ns)} → *{verdict}*")
    return lines


def main() -> int:
    rust: dict[tuple[str, str], Row] = {}
    c: dict[tuple[str, str], Row] = {}
    for g in range(1, 6):
        rust.update(load(RUST_DIR / f"G{g}.csv"))
        c.update(load(C_DIR / f"G{g}.csv"))

    if not rust and not c:
        print("[merge] no CSV inputs found under", RUST_DIR, "or", C_DIR, file=sys.stderr)
        return 1

    groups = sorted({k[0] for k in rust} | {k[0] for k in c})
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z").strip()

    out: list[str] = []
    out.append("#   Rust vs Valkey C")
    out.append("")
    out.append(f"> **{ts}**  ")
    out.append(">  `tests/bench/scripts/merge.py`  `tests/bench/scripts/run-all.sh` ")
    out.append("")
    out.append("## ")
    out.append("")
    out.append("- ****Rust  `std::time::Instant`C  `clock_gettime(CLOCK_MONOTONIC)``elapsedUs`  µs <1µs  op  0")
    out.append("- ****Rust  `LibcAllocator::total_allocated`C  `zmalloc_used_memory` —— ")
    out.append("- **RSS**`/proc/self/status`  `VmRSS` ")
    out.append("- **** case warm-up 3 +  30`median` / `P95`  50% / 95% ")
    out.append("- ** vs **insert / push / pop / delete  `measure_destructive` sample lookup / range / rank  `measure_readonly`")
    out.append("")
    out.append("## ")
    out.append("")
    out.extend(env_block())
    out.append("")
    out.append("## ")
    out.append("")
    hl = highlight_lines(rust, c)
    if hl:
        out.extend(hl)
    else:
        out.append("-  case ")
    out.append("")

    for g in groups:
        meta = GROUP_META.get(g, {"title": g, "rust": "-", "c": "-", "workload": "-"})
        keys = sorted(
            {k for k in rust if k[0] == g} | {k for k in c if k[0] == g},
            key=lambda k: k[1],
        )
        out.append(f"## {meta['title']}")
        out.append("")
        out.append(f"- Rust {meta['rust']}")
        out.append(f"- C {meta['c']}")
        out.append(f"- {meta['workload']}")
        out.append("")
        out.append("| case | Rust median | Rust P95 | Rust ops/s | Rust live | C median | C P95 | C ops/s | C live | Rust vs C |")
        out.append("|---|---|---|---|---|---|---|---|---|---|")
        for k in keys:
            r = rust.get(k)
            cr = c.get(k)
            r_median = fmt_ns(r.median_ns) if r else "-"
            r_p95 = fmt_ns(r.p95_ns) if r else "-"
            r_ops = f"{r.ops_per_sec:,}" if r else "-"
            r_live = fmt_bytes(r.live_bytes) if r else "-"
            c_median = fmt_ns(cr.median_ns) if cr else "-"
            c_p95 = fmt_ns(cr.p95_ns) if cr else "-"
            c_ops = f"{cr.ops_per_sec:,}" if cr else "-"
            c_live = fmt_bytes(cr.live_bytes) if cr else "-"
            rel = ratio(r.median_ns, cr.median_ns) if (r and cr) else "-"
            out.append(
                f"| `{k[1]}` | {r_median} | {r_p95} | {r_ops} | {r_live} | {c_median} | {c_p95} | {c_ops} | {c_live} | {rel} |"
            )
        out.append("")

    out.append("## ")
    out.append("")
    out.append("```sh")
    out.append("# Rust 5  → sync  valkey unit → build C → merge → ")
    out.append("tests/bench/scripts/run-all.sh")
    out.append("")
    out.append("# ")
    out.append("cargo bench --bench bench_g1_dict --profile bench   # Rust ")
    out.append("tests/bench/scripts/sync-to-valkey.sh               #  C  valkey")
    out.append("tests/bench/scripts/build-and-run-c.sh              #  C ")
    out.append("python3 tests/bench/scripts/merge.py                # ")
    out.append("```")
    out.append("")

    text = "\n".join(out) + "\n"
    DOC_PATH.parent.mkdir(parents=True, exist_ok=True)
    DOC_PATH.write_text(text)
    ARCHIVE_PATH.parent.mkdir(parents=True, exist_ok=True)
    ARCHIVE_PATH.write_text(text)
    print(f"[merge] wrote {DOC_PATH}")
    print(f"[merge] archived to {ARCHIVE_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
