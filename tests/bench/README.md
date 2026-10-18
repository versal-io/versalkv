# 数据结构级微基准（Rust vs Valkey C）

对比 Rust 侧承担各数据类型职责的当前实现（自研 `Dict` + 4 类标准库/第三方容器）与 Valkey C 侧特化编码（`hashtable / zskiplist+dict / quicklist / intset / listpack`）的单次操作耗时与内存占用。

设计文档：`docs/superpowers/specs/2026-07-20-ds-microbench-vs-valkey-design.md`
实施计划：`docs/superpowers/plans/2026-07-20-ds-microbench-vs-valkey.md`
编码对比计划：`docs/superpowers/plans/2026-07-27-encoding-benchmarks.md`

## 基准分组

| 分组 | Bench 二进制 | 内容 | 对比维度 |
|------|-------------|------|---------|
| G1 | `bench_g1_dict` | 自研 Dict 哈希表 | Rust 自研 vs 标准库 HashMap，insert/get/remove 性能与内存 |
| G2 | `bench_g2_hashmap_sort` | HashMap 排序对比 | BTreeMap 与 HashMap+sorted Vec 的有序访问开销 |
| G3 | `bench_g3_vecdeque` | VecDeque 列表 | 两端 push/pop、随机访问、内存占用 |
| G4 | `bench_g4_hashset_int` | HashSet 整数集合 | 整数成员 add/contains/remove 与内存 |
| G5 | `bench_g5_indexmap` | IndexMap 有序映射 | 插入顺序保留的哈希表，对比 HashMap + Vec |
| G6 | `bench_g6_set_encoding` | Set 编码对比 | IntSet vs SetLp (listpack) vs Hashtable，整数/字符串成员 |
| G7 | `bench_g7_hash_encoding` | Hash 编码对比 | HashLp (listpack) vs HashHt (hashtable)，字段增删查与内存 |
| G8 | `bench_g8_list_encoding` | List 编码对比 | ListLp (listpack) vs ListValue (quicklist/VecDeque)，两端操作与索引 |
| G9 | `bench_g9_zset_encoding` | ZSet 编码对比 | ZSetLp (listpack) vs ZSetSl (skiplist+dict)，增删排名与范围 |
| G10 | `bench_g10_string_encoding` | String 编码对比 | StrInt vs StrEmbstr vs StrRaw，构造、访问、内存占用 |

## 目录

- `rust/` — 10 份 Rust criterion bench 源码，通过 `Cargo.toml [[bench]]` 条目注册
- `c/` — 10 份 C++ gtest bench 源码，同步到 `thirdparty/valkey/src/unit/` 后由 valkey `make valkey-unit-gtests` 编译
- `sync-to-valkey.sh` — 复制 `c/*.cpp` 到 valkey unit test 目录
- `build-and-run-c.sh` — 在 valkey 侧编译并运行，输出到 `target/bench-results/c/`
- `merge.py` — 汇总 Rust + C 两侧 CSV 到 `target/bench-results/comparison.md`
- `run-all.sh` — 一键跑完整个 pipeline

## 一键运行

```
./tests/bench/run-all.sh
```

结果输出到 `target/bench-results/comparison.md`。

## 单独跑一组（G1）

```
cargo bench --bench bench_g1_dict
./tests/bench/sync-to-valkey.sh
./tests/bench/build-and-run-c.sh   # 内部按 gtest_filter=BenchG*.*, 可临时改 BenchG1.*
```

## 采集口径

- 计时：Rust criterion（median + P95 从 estimates.json 读）；C 手写 `clock_gettime(CLOCK_MONOTONIC)` + 30 轮采样。
- 内存：Rust 用 `mimalloc` 上覆盖 `CountingAllocator` 记 `live_bytes`；C 用 `zmalloc_used_memory()` 装填前后差值。
- 每个 case 附一列 `rss_kb`（读 `/proc/self/status VmRSS`）作为 allocator 差异的复核指标，不参与主结论。
