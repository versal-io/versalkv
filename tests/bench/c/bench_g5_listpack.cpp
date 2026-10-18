/*
 *
 *
 */

#include "generated_wrappers.hpp"
#include "bench_common.hpp"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>

extern "C" {
#include "fmacros.h"
#include "listpack.h"
#include "monotonic.h"
#include "zmalloc.h"
}

namespace {

constexpr int N = 128;
constexpr int FIELD_LEN = 16;
const char *kGroup = "G5";
constexpr unsigned long kSeed = 0xDEADBEEFul;

void mk_str(char *out, const char *prefix, int i) {
    int plen = std::strlen(prefix);
    std::snprintf(out, FIELD_LEN + 1, "%s%0*d", prefix, FIELD_LEN - plen, i);
}

unsigned char *build_full() {
    unsigned char *lp = lpNew(0);
    char f[FIELD_LEN + 1], v[FIELD_LEN + 1];
    for (int i = 0; i < N; ++i) {
        mk_str(f, "f:", i);
        mk_str(v, "v:", i);
        lp = lpAppend(lp, (unsigned char *)f, FIELD_LEN);
        lp = lpAppend(lp, (unsigned char *)v, FIELD_LEN);
    }
    return lp;
}

}  // namespace

TEST(BenchG5Listpack, All) {
    monotonicInit();
    std::vector<bench::CaseRow> rows;

    {
        unsigned char *probe = build_full();
        size_t live_bytes = zmalloc_used_memory();
        uint64_t rss_kb = bench::read_vmrss_kb();
        lpFree(probe);

        std::vector<uint64_t> samples;
        samples.reserve(N);
        char f[FIELD_LEN + 1], v[FIELD_LEN + 1];
        for (int m = 0; m < N; ++m) {
            unsigned char *lp = lpNew(0);
            for (int i = 0; i < m; ++i) {
                mk_str(f, "f:", i);
                mk_str(v, "v:", i);
                lp = lpAppend(lp, (unsigned char *)f, FIELD_LEN);
                lp = lpAppend(lp, (unsigned char *)v, FIELD_LEN);
            }
            mk_str(f, "f:", m);
            mk_str(v, "v:", m);
            uint64_t ns = bench::time_ns([&]() {
                lp = lpAppend(lp, (unsigned char *)f, FIELD_LEN);
                lp = lpAppend(lp, (unsigned char *)v, FIELD_LEN);
            });
            samples.push_back(ns);
            lpFree(lp);
        }
        rows.push_back(bench::from_samples(kGroup, "hset", std::move(samples),
                                           live_bytes, rss_kb));
    }

    {
        unsigned char *lp = build_full();
        size_t live_bytes = zmalloc_used_memory();
        uint64_t rss_kb = bench::read_vmrss_kb();
        unsigned long rng = kSeed ^ 0x20;
        auto samples = bench::measure_readonly([&]() {
            rng ^= rng >> 12;
            rng ^= rng << 25;
            rng ^= rng >> 27;
            int idx = (int)((rng * 0x2545F4914F6CDD1DULL) & 0x7FFF) % N;
            char f[FIELD_LEN + 1];
            mk_str(f, "f:", idx);
            unsigned char *p = lpFind(lp, lpFirst(lp), (unsigned char *)f, FIELD_LEN, 1);
            asm volatile("" : : "r"(p));
        });
        lpFree(lp);
        rows.push_back(bench::from_samples(kGroup, "hget_hit", std::move(samples),
                                           live_bytes, rss_kb));
    }

    /* -------- hget_miss -------- */
    {
        unsigned char *lp = build_full();
        size_t live_bytes = zmalloc_used_memory();
        uint64_t rss_kb = bench::read_vmrss_kb();
        unsigned long rng = kSeed ^ 0x21;
        auto samples = bench::measure_readonly([&]() {
            rng ^= rng >> 12;
            rng ^= rng << 25;
            rng ^= rng >> 27;
            int idx = (int)((rng * 0x2545F4914F6CDD1DULL) & 0x7FFF) % N;
            char buf[FIELD_LEN + 1];
            std::snprintf(buf, sizeof(buf), "miss:%010d", idx);
            unsigned char *p = lpFind(lp, lpFirst(lp), (unsigned char *)buf, FIELD_LEN, 1);
            asm volatile("" : : "r"(p));
        });
        lpFree(lp);
        rows.push_back(bench::from_samples(kGroup, "hget_miss", std::move(samples),
                                           live_bytes, rss_kb));
    }

    {
        unsigned char *probe = build_full();
        size_t live_bytes = zmalloc_used_memory();
        uint64_t rss_kb = bench::read_vmrss_kb();
        lpFree(probe);

        unsigned char *lp = nullptr;
        unsigned char *field_ptr = nullptr;
        unsigned long rng = kSeed ^ 0x22;
        auto samples = bench::measure_destructive(
            [&](int) {
                lp = build_full();
                rng ^= rng >> 12;
                rng ^= rng << 25;
                rng ^= rng >> 27;
                int idx = (int)((rng * 0x2545F4914F6CDD1DULL) & 0x7FFF) % N;
                char f[FIELD_LEN + 1];
                mk_str(f, "f:", idx);
                field_ptr = lpFind(lp, lpFirst(lp), (unsigned char *)f, FIELD_LEN, 1);
            },
            [&]() {
                unsigned char *next = nullptr;
                if (field_ptr) {
                    lp = lpDelete(lp, field_ptr, &next);   /*  field */
                    if (next) lp = lpDelete(lp, next, nullptr); /*  value */
                }
            },
            [&]() { lpFree(lp); lp = nullptr; field_ptr = nullptr; });
        rows.push_back(bench::from_samples(kGroup, "hdel", std::move(samples),
                                           live_bytes, rss_kb));
    }

    bench::write_csv(kGroup, rows);
}
