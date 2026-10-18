/*
 *
 *
 */
#pragma once

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <functional>
#include <string>
#include <sys/stat.h>
#include <vector>

extern "C" {
#include "monotonic.h"
#include "zmalloc.h"
}

namespace bench {

struct CaseRow {
    std::string group;
    std::string case_name;
    uint64_t median_ns_per_op;
    uint64_t p95_ns_per_op;
    uint64_t ops_per_sec;
    size_t live_bytes;
    uint64_t rss_kb;
};

constexpr int kWarmup = 3;
constexpr int kSamples = 30;

inline uint64_t read_vmrss_kb() {
    FILE *f = std::fopen("/proc/self/status", "r");
    if (!f) return 0;
    char line[256];
    uint64_t v = 0;
    while (std::fgets(line, sizeof(line), f)) {
        if (std::strncmp(line, "VmRSS:", 6) == 0) {
            for (char *p = line; *p; ++p) {
                if (*p >= '0' && *p <= '9') {
                    v = v * 10 + (uint64_t)(*p - '0');
                }
            }
            break;
        }
    }
    std::fclose(f);
    return v;
}

inline CaseRow from_samples(const std::string &group, const std::string &case_name,
                            std::vector<uint64_t> samples, size_t live_bytes,
                            uint64_t rss_kb) {
    CaseRow r;
    r.group = group;
    r.case_name = case_name;
    std::sort(samples.begin(), samples.end());
    size_t n = samples.size();
    uint64_t median = n ? samples[n / 2] : 0;
    uint64_t p95 = n ? samples[std::min<size_t>(n * 95 / 100, n - 1)] : 0;
    r.median_ns_per_op = median;
    r.p95_ns_per_op = p95;
    r.ops_per_sec = median ? (1000000000ull / median) : 0;
    r.live_bytes = live_bytes;
    r.rss_kb = rss_kb;
    return r;
}

inline std::string out_dir() {
    const char *env = std::getenv("BENCH_OUT_DIR");
    std::string base = env ? env : "/tmp/bench-results/c";
    std::string cur;
    for (size_t i = 0; i <= base.size(); ++i) {
        char c = i < base.size() ? base[i] : '/';
        if (c == '/') {
            if (!cur.empty()) mkdir(cur.c_str(), 0755);
            cur.push_back(c);
        } else {
            cur.push_back(c);
        }
    }
    return base;
}

inline void write_csv(const std::string &group, const std::vector<CaseRow> &rows) {
    std::string path = out_dir() + "/" + group + ".csv";
    FILE *f = std::fopen(path.c_str(), "w");
    if (!f) {
        std::fprintf(stderr, "[bench] failed to open %s\n", path.c_str());
        return;
    }
    std::fprintf(f, "group,case,median_ns_per_op,p95_ns_per_op,ops_per_sec,live_bytes,rss_kb\n");
    for (const auto &r : rows) {
        std::fprintf(f, "%s,%s,%llu,%llu,%llu,%zu,%llu\n",
                     r.group.c_str(), r.case_name.c_str(),
                     (unsigned long long)r.median_ns_per_op,
                     (unsigned long long)r.p95_ns_per_op,
                     (unsigned long long)r.ops_per_sec,
                     r.live_bytes,
                     (unsigned long long)r.rss_kb);
    }
    std::fclose(f);
    std::fprintf(stderr, "[bench] wrote %s\n", path.c_str());
}

inline uint64_t time_ns(const std::function<void()> &op) {
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    op();
    clock_gettime(CLOCK_MONOTONIC, &t1);
    return (uint64_t)(t1.tv_sec - t0.tv_sec) * 1000000000ull
         + (uint64_t)(t1.tv_nsec - t0.tv_nsec);
}

inline std::vector<uint64_t> measure_destructive(const std::function<void(int)> &setup_before,
                                                 const std::function<void()> &op,
                                                 const std::function<void()> &teardown_after) {
    std::vector<uint64_t> samples;
    samples.reserve(kSamples);
    for (int i = 0; i < kWarmup + kSamples; ++i) {
        setup_before(i);
        uint64_t ns = time_ns(op);
        teardown_after();
        if (i >= kWarmup) samples.push_back(ns);
    }
    return samples;
}

inline std::vector<uint64_t> measure_readonly(const std::function<void()> &op) {
    std::vector<uint64_t> samples;
    samples.reserve(kSamples);
    for (int i = 0; i < kWarmup + kSamples; ++i) {
        uint64_t ns = time_ns(op);
        if (i >= kWarmup) samples.push_back(ns);
    }
    return samples;
}

template <typename F>
inline void snapshot_mem(size_t &live_bytes, uint64_t &rss_kb, F &&build) {
    size_t before = zmalloc_used_memory();
    build();
    size_t after = zmalloc_used_memory();
    live_bytes = after > before ? after - before : 0;
    rss_kb = read_vmrss_kb();
}

}  // namespace bench
