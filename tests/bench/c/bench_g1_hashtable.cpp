/*
 *
 *
 * N = 1_000_000case: insert_seq / lookup_hit / lookup_miss / delete / iterate
 */

#include "generated_wrappers.hpp"
#include "bench_common.hpp"

#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

extern "C" {
#include "fmacros.h"
#include "hashtable.h"
#include "monotonic.h"
#include "mt19937-64.h"
#include "zmalloc.h"
}

namespace {

constexpr int N = 1000000;
const char *kGroup = "G1";

struct keyval {
    unsigned int keysize;
    unsigned int valsize;
    char data[1];
};

keyval *create_keyval(const char *key, const char *val) {
    size_t keysize = std::strlen(key) + 1;
    size_t valsize = std::strlen(val) + 1;
    keyval *e = (keyval *)zmalloc(sizeof(keyval) + keysize + valsize);
    e->keysize = keysize;
    e->valsize = valsize;
    std::memcpy(e->data, key, keysize);
    std::memcpy(e->data + keysize, val, valsize);
    return e;
}

const void *g_getkey(const void *entry) {
    return ((const keyval *)entry)->data;
}
uint64_t g_hashfunc(const void *key) {
    return hashtableGenHashFunction((const char *)key, std::strlen((const char *)key));
}
int g_keycmp(const void *k1, const void *k2) {
    return std::strcmp((const char *)k1, (const char *)k2) == 0;
}
void g_freekv(void *e) { zfree(e); }

hashtableType g_ht_type;

void init_type_once() {
    static bool inited = false;
    if (inited) return;
    std::memset(&g_ht_type, 0, sizeof(g_ht_type));
    g_ht_type.entryGetKey = g_getkey;
    g_ht_type.hashFunction = g_hashfunc;
    g_ht_type.keyCompare = g_keycmp;
    g_ht_type.entryDestructor = g_freekv;
    inited = true;
}

hashtable *build_full() {
    hashtable *ht = hashtableCreate(&g_ht_type);
    for (int i = 0; i < N; ++i) {
        char key[32], val[32];
        std::snprintf(key, sizeof(key), "key:%d", i);
        std::snprintf(val, sizeof(val), "%d", i);
        keyval *e = create_keyval(key, val);
        hashtableAdd(ht, e);
    }
    return ht;
}

}  // namespace

TEST(BenchG1Hashtable, All) {
    monotonicInit();
    init_type_once();

    uint8_t seed[16] = {0};
    hashtableSetHashFunctionSeed(seed);

    std::vector<bench::CaseRow> rows;

    {
        size_t live_bytes = 0;
        uint64_t rss_kb = 0;
        bench::snapshot_mem(live_bytes, rss_kb, [&]() {
            hashtable *probe = build_full();
            hashtableRelease(probe);
        });
        hashtable *probe = build_full();
        size_t probe_live = zmalloc_used_memory();
        hashtableRelease(probe);
        live_bytes = probe_live;

        hashtable *ht = nullptr;
        int i = 0;
        keyval *pending = nullptr;
        auto samples = bench::measure_destructive(
            [&](int) {
                ht = hashtableCreate(&g_ht_type);
                char key[32], val[32];
                std::snprintf(key, sizeof(key), "key:%d", i);
                std::snprintf(val, sizeof(val), "%d", i);
                pending = create_keyval(key, val);
                ++i;
            },
            [&]() { hashtableAdd(ht, pending); },
            [&]() { hashtableRelease(ht); ht = nullptr; pending = nullptr; });
        rows.push_back(bench::from_samples(kGroup, "insert_seq", std::move(samples),
                                           live_bytes, rss_kb));
    }

    /* -------- lookup_hit -------- */
    {
        hashtable *ht = build_full();
        size_t live_bytes = zmalloc_used_memory();
        uint64_t rss_kb = bench::read_vmrss_kb();
        auto samples = bench::measure_readonly([&]() {
            int idx = std::rand() % N;
            char key[32];
            std::snprintf(key, sizeof(key), "key:%d", idx);
            void *found = nullptr;
            hashtableFind(ht, key, &found);
            (void)found;
        });
        hashtableRelease(ht);
        rows.push_back(bench::from_samples(kGroup, "lookup_hit", std::move(samples),
                                           live_bytes, rss_kb));
    }

    /* -------- lookup_miss -------- */
    {
        hashtable *ht = build_full();
        size_t live_bytes = zmalloc_used_memory();
        uint64_t rss_kb = bench::read_vmrss_kb();
        auto samples = bench::measure_readonly([&]() {
            int idx = std::rand() % N;
            char key[32];
            std::snprintf(key, sizeof(key), "miss:%d", idx);
            void *found = nullptr;
            hashtableFind(ht, key, &found);
            (void)found;
        });
        hashtableRelease(ht);
        rows.push_back(bench::from_samples(kGroup, "lookup_miss", std::move(samples),
                                           live_bytes, rss_kb));
    }

    /* -------- delete -------- */
    {
        hashtable *ht = nullptr;
        char victim[32] = {0};
        size_t live_bytes = 0;
        uint64_t rss_kb = 0;
        bench::snapshot_mem(live_bytes, rss_kb, [&]() {
            hashtable *probe = build_full();
            hashtableRelease(probe);
        });
        {
            hashtable *probe = build_full();
            live_bytes = zmalloc_used_memory();
            hashtableRelease(probe);
        }

        auto samples = bench::measure_destructive(
            [&](int) {
                ht = build_full();
                int idx = std::rand() % N;
                std::snprintf(victim, sizeof(victim), "key:%d", idx);
            },
            [&]() { hashtableDelete(ht, victim); },
            [&]() { hashtableRelease(ht); ht = nullptr; });
        rows.push_back(bench::from_samples(kGroup, "delete", std::move(samples),
                                           live_bytes, rss_kb));
    }

    {
        hashtable *ht = build_full();
        size_t live_bytes = zmalloc_used_memory();
        uint64_t rss_kb = bench::read_vmrss_kb();
        auto samples = bench::measure_readonly([&]() {
            hashtableIterator it;
            hashtableInitIterator(&it, ht, 0);
            void *elem = nullptr;
            uint64_t acc = 0;
            while (hashtableNext(&it, &elem)) {
                acc += ((keyval *)elem)->keysize;
            }
            hashtableCleanupIterator(&it);
            asm volatile("" : : "r"(acc));
        });
        hashtableRelease(ht);
        rows.push_back(bench::from_samples(kGroup, "iterate", std::move(samples),
                                           live_bytes, rss_kb));
    }

    {
        constexpr int LONG_N = 100000;
        constexpr int LONG_KEY_LEN = 128;
        std::vector<std::string> long_keys;
        long_keys.reserve(LONG_N);
        for (int i = 0; i < LONG_N; ++i) {
            char buf[192];
            int prefix = std::snprintf(buf, sizeof(buf), "longkey:%08d:", i);
            for (int k = prefix; k < LONG_KEY_LEN; ++k) buf[k] = 'x';
            buf[LONG_KEY_LEN] = '\0';
            long_keys.emplace_back(buf, LONG_KEY_LEN);
        }
        hashtable *ht = hashtableCreate(&g_ht_type);
        for (int i = 0; i < LONG_N; ++i) {
            keyval *e = create_keyval(long_keys[i].c_str(), "v");
            hashtableAdd(ht, e);
        }
        size_t live_bytes = zmalloc_used_memory();
        uint64_t rss_kb = bench::read_vmrss_kb();
        auto samples = bench::measure_readonly([&]() {
            int idx = std::rand() % LONG_N;
            void *found = nullptr;
            hashtableFind(ht, long_keys[idx].c_str(), &found);
            (void)found;
        });
        hashtableRelease(ht);
        rows.push_back(bench::from_samples(kGroup, "long_key_lookup",
                                           std::move(samples), live_bytes, rss_kb));
    }

    {
        constexpr int TRIGGER = 524288;
        auto build_trigger = [&]() -> hashtable * {
            hashtable *ht = hashtableCreate(&g_ht_type);
            for (int i = 0; i < TRIGGER; ++i) {
                char key[32], val[32];
                std::snprintf(key, sizeof(key), "key:%d", i);
                std::snprintf(val, sizeof(val), "%d", i);
                keyval *e = create_keyval(key, val);
                hashtableAdd(ht, e);
            }
            return ht;
        };
        size_t live_bytes = 0;
        uint64_t rss_kb = 0;
        {
            hashtable *probe = build_trigger();
            live_bytes = zmalloc_used_memory();
            rss_kb = bench::read_vmrss_kb();
            hashtableRelease(probe);
        }
        hashtable *ht = nullptr;
        int counter = 0;
        keyval *pending = nullptr;
        auto samples = bench::measure_destructive(
            [&](int) {
                ht = build_trigger();
                char key[32], val[32];
                std::snprintf(key, sizeof(key), "trigger:%d", counter++);
                std::snprintf(val, sizeof(val), "%d", counter);
                pending = create_keyval(key, val);
            },
            [&]() { hashtableAdd(ht, pending); },
            [&]() { hashtableRelease(ht); ht = nullptr; pending = nullptr; });
        rows.push_back(bench::from_samples(kGroup, "insert_during_rehash",
                                           std::move(samples), live_bytes, rss_kb));
    }

    bench::write_csv(kGroup, rows);
}
