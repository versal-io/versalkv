/*
 *
 *   push_back / push_front / pop_front / index_random / range_slice
 */

#include "generated_wrappers.hpp"
#include "bench_common.hpp"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>

extern "C" {
#include "fmacros.h"
#include "monotonic.h"
#include "quicklist.h"
#include "zmalloc.h"
}

namespace {

constexpr int N = 1000000;
constexpr int ELEM_LEN = 16;
const char *kGroup = "G3";
constexpr unsigned long kSeed = 0xDEADBEEFul;

void mk_elem(char *buf, int i) {
    std::snprintf(buf, ELEM_LEN + 1, "%016d", i);
}

quicklist *build_full() {
    quicklist *ql = quicklistCreate();
    char buf[ELEM_LEN + 1];
    for (int i = 0; i < N; ++i) {
        mk_elem(buf, i);
        quicklistPushTail(ql, buf, ELEM_LEN);
    }
    return ql;
}

}  // namespace

TEST(BenchG3Quicklist, All) {
    monotonicInit();
    std::vector<bench::CaseRow> rows;

    {
        size_t live_bytes = 0;
        uint64_t rss_kb = 0;
        {
            quicklist *probe = build_full();
            live_bytes = zmalloc_used_memory();
            rss_kb = bench::read_vmrss_kb();
            quicklistRelease(probe);
        }
        quicklist *ql = nullptr;
        int i = 0;
        char buf[ELEM_LEN + 1];
        auto samples = bench::measure_destructive(
            [&](int) {
                ql = quicklistCreate();
                mk_elem(buf, i);
                ++i;
            },
            [&]() { quicklistPushTail(ql, buf, ELEM_LEN); },
            [&]() {
                quicklistRelease(ql);
                ql = nullptr;
            });
        rows.push_back(bench::from_samples(kGroup, "push_back", std::move(samples),
                                           live_bytes, rss_kb));
    }

    /* -------- push_front -------- */
    {
        quicklist *probe = build_full();
        size_t live_bytes = zmalloc_used_memory();
        uint64_t rss_kb = bench::read_vmrss_kb();
        quicklistRelease(probe);

        quicklist *ql = nullptr;
        int i = 0;
        char buf[ELEM_LEN + 1];
        auto samples = bench::measure_destructive(
            [&](int) {
                ql = quicklistCreate();
                mk_elem(buf, i);
                ++i;
            },
            [&]() { quicklistPushHead(ql, buf, ELEM_LEN); },
            [&]() {
                quicklistRelease(ql);
                ql = nullptr;
            });
        rows.push_back(bench::from_samples(kGroup, "push_front", std::move(samples),
                                           live_bytes, rss_kb));
    }

    {
        quicklist *probe = build_full();
        size_t live_bytes = zmalloc_used_memory();
        uint64_t rss_kb = bench::read_vmrss_kb();
        quicklistRelease(probe);

        quicklist *ql = nullptr;
        auto samples = bench::measure_destructive(
            [&](int) { ql = build_full(); },
            [&]() {
                unsigned char *data = nullptr;
                size_t sz = 0;
                long long slong = 0;
                quicklistPop(ql, QUICKLIST_HEAD, &data, &sz, &slong);
                if (data) zfree(data);
            },
            [&]() {
                quicklistRelease(ql);
                ql = nullptr;
            });
        rows.push_back(bench::from_samples(kGroup, "pop_front", std::move(samples),
                                           live_bytes, rss_kb));
    }

    {
        quicklist *ql = build_full();
        size_t live_bytes = zmalloc_used_memory();
        uint64_t rss_kb = bench::read_vmrss_kb();
        unsigned long rng = kSeed ^ 0xD;
        auto samples = bench::measure_readonly([&]() {
            rng ^= rng >> 12;
            rng ^= rng << 25;
            rng ^= rng >> 27;
            long long idx = (long long)(rng * 0x2545F4914F6CDD1DULL) % N;
            if (idx < 0) idx += N;
            quicklistEntry entry;
            quicklistIter *it = quicklistGetIteratorEntryAtIdx(ql, idx, &entry);
            asm volatile("" : : "r"(&entry));
            if (it) quicklistReleaseIterator(it);
        });
        quicklistRelease(ql);
        rows.push_back(bench::from_samples(kGroup, "index_random", std::move(samples),
                                           live_bytes, rss_kb));
    }

    {
        quicklist *ql = build_full();
        size_t live_bytes = zmalloc_used_memory();
        uint64_t rss_kb = bench::read_vmrss_kb();
        auto samples = bench::measure_readonly([&]() {
            quicklistIter *it = quicklistGetIterator(ql, AL_START_HEAD);
            quicklistEntry entry;
            uint64_t acc = 0;
            while (quicklistNext(it, &entry)) {
                if (entry.value) acc += entry.value[0];
                else acc += (uint64_t)entry.longval;
            }
            quicklistReleaseIterator(it);
            asm volatile("" : : "r"(acc));
        });
        quicklistRelease(ql);
        rows.push_back(bench::from_samples(kGroup, "range_slice", std::move(samples),
                                           live_bytes, rss_kb));
    }

    bench::write_csv(kGroup, rows);
}
