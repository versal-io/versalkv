/*
 *
 * N = 100_000case: add_seq / add_rand / contains_hit / contains_miss / remove
 */

#include "generated_wrappers.hpp"
#include "bench_common.hpp"

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <vector>

extern "C" {
#include "fmacros.h"
#include "intset.h"
#include "monotonic.h"
#include "zmalloc.h"
}

namespace {

constexpr int N = 100000;
const char *kGroup = "G4";
constexpr unsigned long kSeed = 0xDEADBEEFul;

inline int64_t next_rand64(unsigned long *state) {
    *state ^= *state >> 12;
    *state ^= *state << 25;
    *state ^= *state >> 27;
    return (int64_t)(*state * 0x2545F4914F6CDD1DULL);
}

intset *build_seq() {
    intset *s = intsetNew();
    uint8_t ok = 0;
    for (int i = 0; i < N; ++i) {
        s = intsetAdd(s, (int64_t)i, &ok);
    }
    return s;
}

intset *build_rand(unsigned long seed) {
    intset *s = intsetNew();
    uint8_t ok = 0;
    unsigned long st = seed;
    uint32_t added = 0;
    while (added < (uint32_t)N) {
        s = intsetAdd(s, next_rand64(&st), &ok);
        if (ok) ++added;
    }
    return s;
}

}  // namespace

TEST(BenchG4Intset, All) {
    monotonicInit();
    std::vector<bench::CaseRow> rows;

    {
        intset *probe = build_seq();
        size_t live_bytes = zmalloc_used_memory();
        uint64_t rss_kb = bench::read_vmrss_kb();
        zfree(probe);

        intset *s = nullptr;
        int i = 0;
        auto samples = bench::measure_destructive(
            [&](int) { s = intsetNew(); i = 0; },
            [&]() {
                uint8_t ok = 0;
                s = intsetAdd(s, (int64_t)i, &ok);
                ++i;
            },
            [&]() { zfree(s); s = nullptr; });
        rows.push_back(bench::from_samples(kGroup, "add_seq", std::move(samples),
                                           live_bytes, rss_kb));
    }

    {
        intset *probe = build_rand(kSeed);
        size_t live_bytes = zmalloc_used_memory();
        uint64_t rss_kb = bench::read_vmrss_kb();
        zfree(probe);

        intset *s = nullptr;
        unsigned long rng = kSeed ^ 0xE;
        auto samples = bench::measure_destructive(
            [&](int) { s = intsetNew(); },
            [&]() {
                uint8_t ok = 0;
                s = intsetAdd(s, next_rand64(&rng), &ok);
            },
            [&]() { zfree(s); s = nullptr; });
        rows.push_back(bench::from_samples(kGroup, "add_rand", std::move(samples),
                                           live_bytes, rss_kb));
    }

    {
        intset *s = build_rand(kSeed);
        size_t live_bytes = zmalloc_used_memory();
        uint64_t rss_kb = bench::read_vmrss_kb();
        unsigned long rng = kSeed ^ 0xF;
        uint32_t len = intsetLen(s);
        auto samples = bench::measure_readonly([&]() {
            rng ^= rng >> 12;
            rng ^= rng << 25;
            rng ^= rng >> 27;
            uint32_t pos = (uint32_t)(rng * 0x2545F4914F6CDD1DULL) % len;
            int64_t v = 0;
            intsetGet(s, pos, &v);
            uint8_t found = intsetFind(s, v);
            asm volatile("" : : "r"(found));
        });
        zfree(s);
        rows.push_back(bench::from_samples(kGroup, "contains_hit", std::move(samples),
                                           live_bytes, rss_kb));
    }

    {
        intset *s = build_seq();
        size_t live_bytes = zmalloc_used_memory();
        uint64_t rss_kb = bench::read_vmrss_kb();
        unsigned long rng = kSeed ^ 0x10;
        auto samples = bench::measure_readonly([&]() {
            int64_t v = -(int64_t)((next_rand64(&rng) & 0x7FFFFFFFFFFFFFFF) % 1000000000ULL + 1);
            uint8_t found = intsetFind(s, v);
            asm volatile("" : : "r"(found));
        });
        zfree(s);
        rows.push_back(bench::from_samples(kGroup, "contains_miss", std::move(samples),
                                           live_bytes, rss_kb));
    }

    {
        intset *probe = build_seq();
        size_t live_bytes = zmalloc_used_memory();
        uint64_t rss_kb = bench::read_vmrss_kb();
        zfree(probe);

        intset *s = nullptr;
        int64_t victim = 0;
        unsigned long rng = kSeed ^ 0x11;
        auto samples = bench::measure_destructive(
            [&](int) {
                s = build_seq();
                victim = (int64_t)((next_rand64(&rng) & 0x7FFFFFFFFFFFFFFF) % (uint64_t)N);
            },
            [&]() {
                int ok = 0;
                s = intsetRemove(s, victim, &ok);
            },
            [&]() { zfree(s); s = nullptr; });
        rows.push_back(bench::from_samples(kGroup, "remove", std::move(samples),
                                           live_bytes, rss_kb));
    }

    bench::write_csv(kGroup, rows);
}
