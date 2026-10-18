#!/usr/bin/env python3
"""Extract C-side CSVs from existing docs/benchmarks/ds-microbench.md tables.

Used when the C bench toolchain (g++/cmake-valkey-unit-gtests) is unavailable;
reuses previously-published C baseline numbers for the same stable workloads.
"""
from __future__ import annotations
import csv, re, sys
from pathlib import Path

DOC = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("docs/benchmarks/ds-microbench.md")
OUT = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("target/bench-results/c")
OUT.mkdir(parents=True, exist_ok=True)


def scale_ns(t: str) -> int:
    t = t.strip()
    if t == "-":
        return 0
    m = re.match(r"([\d.]+)\s*(ns|µs|ms|s)", t)
    if not m:
        return 0
    v = float(m.group(1))
    u = m.group(2)
    if u == "ns": return int(v)
    if u == "µs": return int(v * 1_000)
    if u == "ms": return int(v * 1_000_000)
    if u == "s":  return int(v * 1_000_000_000)
    return 0


def scale_bytes(b: str) -> int:
    b = b.strip()
    if b == "-":
        return 0
    m = re.match(r"([\d.]+)\s*(B|KB|MB|GB)", b)
    if not m:
        return 0
    v = float(m.group(1))
    u = m.group(2)
    if u == "B": return int(v)
    if u == "KB": return int(v * 1024)
    if u == "MB": return int(v * 1024 * 1024)
    if u == "GB": return int(v * 1024 * 1024 * 1024)
    return 0


rows_by_group: dict[str, list[list]] = {}
cur = None
for line in DOC.read_text().splitlines():
    m = re.match(r"^## (G[1-5]) ", line)
    if m:
        cur = m.group(1)
        continue
    if cur is None:
        continue
    m = re.match(r"^\| `([^`]+)` \|", line)
    if not m:
        continue
    parts = [p.strip() for p in line.strip().strip("|").split("|")]
    if len(parts) < 10:
        continue
    case = parts[0].strip("`")
    _rm, _rp, _ro, _rl, c_med, c_p95, c_ops, c_live, _ratio = parts[1:10]
    c_ops_i = int(c_ops.replace(",", "")) if c_ops not in ("-", "") else 0
    rows_by_group.setdefault(cur, []).append([
        cur, case, scale_ns(c_med), scale_ns(c_p95), c_ops_i, scale_bytes(c_live), 0,
    ])

for g in ("G1", "G2", "G3", "G4", "G5"):
    path = OUT / f"{g}.csv"
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["group", "case", "median_ns_per_op", "p95_ns_per_op",
                    "ops_per_sec", "live_bytes", "rss_kb"])
        for r in rows_by_group.get(g, []):
            w.writerow(r)
    print(f"[c-csv] wrote {path} ({len(rows_by_group.get(g, []))} rows)")
