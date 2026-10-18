/*
 *
 * N = 100_000case: zadd_seq / zadd_rand / zrangebyscore_slice / zrank / zrem
 *
 */

#include "generated_wrappers.hpp"
#include "bench_common.hpp"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

extern "C" {
#include "fmacros.h"
#include "monotonic.h"
#include "sds.h"
#include "server.h"
#include "zmalloc.h"
}

namespace {

constexpr int N = 100000;
const char *kGroup = "G2";
constexpr unsigned long kSeed = 0xDEADBEEFul;

static inline unsigned long bench_zslGetNodeSpanAtLevel(const zskiplistNode *x, int level) {
    return x->level[level].span;
}
static inline unsigned long bench_zslGetNodeHeight(const zskiplistNode *x) {
    return x->level[0].span;
}
static unsigned long bench_zslGetRank(zskiplist *zsl, const zskiplistNode *node) {
    unsigned long count_after_node = 0;
    while (node) {
        int highest_node_span = bench_zslGetNodeHeight(node) - 1;
        count_after_node += bench_zslGetNodeSpanAtLevel(node, highest_node_span);
        node = node->level[highest_node_span].forward;
    }
    return zslGetLength(zsl) - count_after_node;
}

robj *make_zset() {
    return createZsetObject();
}

double next_rand_score(unsigned long *state) {
    *state ^= *state >> 12;
    *state ^= *state << 25;
    *state ^= *state >> 27;
    unsigned long v = *state * 0x2545F4914F6CDD1DULL;
    return ((double)(v & 0x7FFFFFFFFFFFFFFFULL) / (double)0x7FFFFFFFFFFFFFFFULL) * 2e9 - 1e9;
}

robj *build_seq() {
    robj *z = make_zset();
    for (int i = 0; i < N; ++i) {
        char buf[32];
        int len = std::snprintf(buf, sizeof(buf), "m:%d", i);
        sds ele = sdsnewlen(buf, len);
        int flags = ZADD_IN_NONE;
        int out = 0;
        zsetAdd(z, (double)i, ele, flags, &out, nullptr);
        sdsfree(ele);
    }
    return z;
}

robj *build_rand(unsigned long seed) {
    unsigned long st = seed;
    robj *z = make_zset();
    for (int i = 0; i < N; ++i) {
        char buf[32];
        int len = std::snprintf(buf, sizeof(buf), "m:%d", i);
        sds ele = sdsnewlen(buf, len);
        int flags = ZADD_IN_NONE;
        int out = 0;
        zsetAdd(z, next_rand_score(&st), ele, flags, &out, nullptr);
        sdsfree(ele);
    }
    return z;
}

}  // namespace

TEST(BenchG2Zset, All) {
    monotonicInit();

    std::vector<bench::CaseRow> rows;

    /* -------- zadd_seq -------- */
    {
        size_t live_bytes = 0;
        uint64_t rss_kb = 0;
        {
            robj *probe = build_seq();
            live_bytes = zmalloc_used_memory();
            rss_kb = bench::read_vmrss_kb();
            decrRefCount(probe);
        }
        robj *z = nullptr;
        int i = 0;
        sds pending = nullptr;
        auto samples = bench::measure_destructive(
            [&](int) {
                z = make_zset();
                char buf[32];
                int len = std::snprintf(buf, sizeof(buf), "m:%d", i);
                pending = sdsnewlen(buf, len);
                ++i;
            },
            [&]() {
                int out = 0;
                zsetAdd(z, (double)i, pending, ZADD_IN_NONE, &out, nullptr);
            },
            [&]() {
                sdsfree(pending);
                decrRefCount(z);
                z = nullptr;
                pending = nullptr;
            });
        rows.push_back(bench::from_samples(kGroup, "zadd_seq", std::move(samples),
                                           live_bytes, rss_kb));
    }

    /* -------- zadd_rand -------- */
    {
        size_t live_bytes = 0;
        uint64_t rss_kb = 0;
        {
            robj *probe = build_rand(kSeed);
            live_bytes = zmalloc_used_memory();
            rss_kb = bench::read_vmrss_kb();
            decrRefCount(probe);
        }
        robj *z = nullptr;
        int i = 0;
        unsigned long rng = kSeed ^ 0xA;
        sds pending = nullptr;
        double pending_score = 0;
        auto samples = bench::measure_destructive(
            [&](int) {
                z = make_zset();
                char buf[32];
                int len = std::snprintf(buf, sizeof(buf), "m:%d", i);
                pending = sdsnewlen(buf, len);
                pending_score = next_rand_score(&rng);
                ++i;
            },
            [&]() {
                int out = 0;
                zsetAdd(z, pending_score, pending, ZADD_IN_NONE, &out, nullptr);
            },
            [&]() {
                sdsfree(pending);
                decrRefCount(z);
                z = nullptr;
                pending = nullptr;
            });
        rows.push_back(bench::from_samples(kGroup, "zadd_rand", std::move(samples),
                                           live_bytes, rss_kb));
    }

    {
        robj *z = build_rand(kSeed);
        zset *zs = (zset *)objectGetVal(z);
        size_t live_bytes = zmalloc_used_memory();
        uint64_t rss_kb = bench::read_vmrss_kb();
        auto samples = bench::measure_readonly([&]() {
            zrangespec range;
            range.min = -1e18;
            range.max = 1e18;
            range.minex = 0;
            range.maxex = 0;
            long rank = 0;
            zskiplistNode *first = zslNthInRange(zs->zsl, &range, 0, &rank);
            int taken = 0;
            zskiplistNode *cur = first;
            while (cur && taken < 1000) {
                asm volatile("" : : "r"(cur));
                cur = cur->level[0].forward;
                ++taken;
            }
        });
        decrRefCount(z);
        rows.push_back(bench::from_samples(kGroup, "zrangebyscore_slice",
                                           std::move(samples), live_bytes, rss_kb));
    }

    {
        robj *z = build_rand(kSeed);
        zset *zs = (zset *)objectGetVal(z);
        size_t live_bytes = zmalloc_used_memory();
        uint64_t rss_kb = bench::read_vmrss_kb();
        unsigned long rng = kSeed ^ 0xB;
        auto samples = bench::measure_readonly([&]() {
            int idx = (int)(next_rand_score(&rng) + 1e9) % N;
            if (idx < 0) idx += N;
            char buf[32];
            int len = std::snprintf(buf, sizeof(buf), "m:%d", idx);
            sds ele = sdsnewlen(buf, len);
            double score = 0;
            zsetScore(z, ele, &score);
            zskiplistNode *x = zs->zsl->header.level[zslGetHeight(zs->zsl) - 1].forward;
            unsigned long rank = 0;
            while (x) {
                rank++;
                sds cur = zslGetNodeElement(x);
                if (x->score == score && sdslen(cur) == sdslen(ele) &&
                    std::memcmp(cur, ele, sdslen(ele)) == 0) {
                    break;
                }
                x = x->level[0].forward;
            }
            asm volatile("" : : "r"(rank));
            sdsfree(ele);
        });
        decrRefCount(z);
        rows.push_back(bench::from_samples(kGroup, "zrank", std::move(samples),
                                           live_bytes, rss_kb));
    }

    {
        size_t live_bytes = 0;
        uint64_t rss_kb = 0;
        {
            robj *probe = build_seq();
            live_bytes = zmalloc_used_memory();
            rss_kb = bench::read_vmrss_kb();
            decrRefCount(probe);
        }
        robj *z = nullptr;
        sds victim = nullptr;
        unsigned long rng = kSeed ^ 0xC;
        auto samples = bench::measure_destructive(
            [&](int) {
                z = build_seq();
                int idx = (int)(next_rand_score(&rng) + 1e9) % N;
                if (idx < 0) idx += N;
                char buf[32];
                int len = std::snprintf(buf, sizeof(buf), "m:%d", idx);
                victim = sdsnewlen(buf, len);
            },
            [&]() { zsetDel(z, victim); },
            [&]() {
                sdsfree(victim);
                decrRefCount(z);
                z = nullptr;
                victim = nullptr;
            });
        rows.push_back(bench::from_samples(kGroup, "zrem", std::move(samples),
                                           live_bytes, rss_kb));
    }

    bench::write_csv(kGroup, rows);
    (void)bench_zslGetRank;
}
