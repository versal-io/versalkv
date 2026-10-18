/*
 * G1: Valkey hashtable — C version
 *
 * N = 1_000_000, keys are "key:%09d" format
 * Cases: insert_seq, lookup_hit, lookup_miss, delete, iterate
 */
#include "bench_common.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "hashtable.h"
#include "zmalloc.h"

#define N 1000000
#define KEY_LEN 20

typedef struct {
    char key[KEY_LEN];
    long value;
} entry_t;

static char keys[N][KEY_LEN + 1];
static char miss_keys[N][KEY_LEN + 1];
static int *order;

static uint64_t hashFunction(const void *key) {
    /* FNV-1a hash */
    const char *s = (const char *)key;
    uint64_t h = 14695981039346656037ULL;
    while (*s) {
        h ^= (uint64_t)(unsigned char)*s++;
        h *= 1099511628211ULL;
    }
    return h;
}

static const void *entryGetKey(const void *entry) {
    return ((const entry_t *)entry)->key;
}

static int keyCompare(const void *key1, const void *key2) {
    return strcmp((const char *)key1, (const char *)key2);
}

static void freeEntry(void *entry) {
    zfree(entry);
}

static hashtableType htType = {
    .entryGetKey = entryGetKey,
    .hashFunction = hashFunction,
    .keyCompare = keyCompare,
    .entryDestructor = freeEntry,
};

static void gen_keys(void) {
    for (int i = 0; i < N; i++) {
        snprintf(keys[i], KEY_LEN + 1, "key:%09d", i);
        snprintf(miss_keys[i], KEY_LEN + 1, "miss:%09d", i);
    }
    order = malloc(N * sizeof(int));
    for (int i = 0; i < N; i++) order[i] = i;
    for (int i = N - 1; i > 0; i--) {
        int j = rand() % (i + 1);
        int tmp = order[i];
        order[i] = order[j];
        order[j] = tmp;
    }
}

int main(void) {
    const char *out_dir = getenv("BENCH_OUT_DIR");
    if (!out_dir) out_dir = "target/bench-results/c_pure";

    gen_keys();

    bench_row_t rows[5];
    uint64_t samples[BENCH_SAMPLES];
    int n = BENCH_SAMPLES;
    hashtable *ht;
    size_t live_bytes;
    uint64_t rss_kb;

    /* ---------- insert_seq ---------- */
    {
        ht = hashtableCreate(&htType);
        size_t before = zmalloc_used_memory();
        uint64_t t0 = bench_time_ns();
        for (int i = 0; i < N; i++) {
            entry_t *e = zmalloc(sizeof(entry_t));
            strncpy(e->key, keys[i], KEY_LEN);
            e->value = i;
            hashtableAdd(ht, e);
        }
        uint64_t t1 = bench_time_ns();
        size_t after = zmalloc_used_memory();
        live_bytes = after > before ? after - before : 0;
        rss_kb = bench_rss_kb();

        uint64_t per_op = (t1 - t0) / N;
        for (int i = 0; i < n; i++) samples[i] = per_op;
        bench_compute(&rows[0], "G1", "insert_seq", samples, n, live_bytes, rss_kb);
        hashtableRelease(ht);
    }

    /* ---------- lookup_hit ---------- */
    {
        ht = hashtableCreate(&htType);
        for (int i = 0; i < N; i++) {
            entry_t *e = zmalloc(sizeof(entry_t));
            strncpy(e->key, keys[i], KEY_LEN);
            e->value = i;
            hashtableAdd(ht, e);
        }

        uint64_t t0 = bench_time_ns();
        for (int i = 0; i < N; i++) {
            void *found;
            hashtableFind(ht, keys[order[i]], &found);
        }
        uint64_t t1 = bench_time_ns();

        uint64_t per_op = (t1 - t0) / N;
        for (int i = 0; i < n; i++) samples[i] = per_op;
        bench_compute(&rows[1], "G1", "lookup_hit", samples, n, 0, 0);
        hashtableRelease(ht);
    }

    /* ---------- lookup_miss ---------- */
    {
        ht = hashtableCreate(&htType);
        for (int i = 0; i < N; i++) {
            entry_t *e = zmalloc(sizeof(entry_t));
            strncpy(e->key, keys[i], KEY_LEN);
            e->value = i;
            hashtableAdd(ht, e);
        }

        uint64_t t0 = bench_time_ns();
        for (int i = 0; i < N; i++) {
            void *found;
            hashtableFind(ht, miss_keys[order[i]], &found);
        }
        uint64_t t1 = bench_time_ns();

        uint64_t per_op = (t1 - t0) / N;
        for (int i = 0; i < n; i++) samples[i] = per_op;
        bench_compute(&rows[2], "G1", "lookup_miss", samples, n, 0, 0);
        hashtableRelease(ht);
    }

    /* ---------- delete ---------- */
    {
        ht = hashtableCreate(&htType);
        for (int i = 0; i < N; i++) {
            entry_t *e = zmalloc(sizeof(entry_t));
            strncpy(e->key, keys[i], KEY_LEN);
            e->value = i;
            hashtableAdd(ht, e);
        }

        uint64_t t0 = bench_time_ns();
        for (int i = 0; i < N; i++) {
            hashtableDelete(ht, keys[order[i]]);
        }
        uint64_t t1 = bench_time_ns();

        uint64_t per_op = (t1 - t0) / N;
        for (int i = 0; i < n; i++) samples[i] = per_op;
        bench_compute(&rows[3], "G1", "delete", samples, n, 0, 0);
        hashtableRelease(ht);
    }

    /* ---------- iterate ---------- */
    {
        ht = hashtableCreate(&htType);
        for (int i = 0; i < N; i++) {
            entry_t *e = zmalloc(sizeof(entry_t));
            strncpy(e->key, keys[i], KEY_LEN);
            e->value = i;
            hashtableAdd(ht, e);
        }

        uint64_t t0 = bench_time_ns();
        hashtableIterator *iter = hashtableCreateIterator(ht, 0);
        void *entry;
        long count = 0;
        while (hashtableNext(iter, &entry)) {
            count++;
        }
        hashtableReleaseIterator(iter);
        uint64_t t1 = bench_time_ns();

        uint64_t per_op = (t1 - t0) / N;
        for (int i = 0; i < n; i++) samples[i] = per_op;
        bench_compute(&rows[4], "G1", "iterate", samples, n, 0, 0);
        (void)count;
        hashtableRelease(ht);
    }

    bench_write_csv(out_dir, "G1", rows, 5);
    free(order);
    return 0;
}
