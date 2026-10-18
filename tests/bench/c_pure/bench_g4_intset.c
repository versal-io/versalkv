/*
 * G4: Valkey intset — C version
 *
 * N = 65536 (intset stays in int16 encoding with small values,
 *             or use N=128 for consistency with Rust G6)
 * Actually let's use N=128 to match Rust G6 intset comparison.
 * Cases: add_seq, add_rand, contains_hit, contains_miss, remove
 */
#include "bench_common.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "intset.h"
#include "zmalloc.h"

#define N 128

static int64_t values[N];
static int64_t miss_values[N];
static int *order;

static void gen_data(void) {
    for (int i = 0; i < N; i++) {
        values[i] = i + 1;
        miss_values[i] = -(i + 1);
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

    gen_data();

    bench_row_t rows[5];
    uint64_t samples[BENCH_SAMPLES];
    int n = BENCH_SAMPLES;
    size_t live_bytes;
    uint64_t rss_kb;

    /* ---------- add_seq ---------- */
    {
        size_t before = zmalloc_used_memory();
        intset *is = intsetNew();
        uint64_t t0 = bench_time_ns();
        uint8_t success;
        for (int i = 0; i < N; i++) {
            is = intsetAdd(is, values[i], &success);
        }
        uint64_t t1 = bench_time_ns();
        size_t after = zmalloc_used_memory();
        live_bytes = after > before ? after - before : 0;
        rss_kb = bench_rss_kb();

        uint64_t per_op = (t1 - t0) / N;
        for (int i = 0; i < n; i++) samples[i] = per_op;
        bench_compute(&rows[0], "G4", "add_seq", samples, n, live_bytes, rss_kb);
        zfree(is);
    }

    /* ---------- add_rand ---------- */
    {
        intset *is = intsetNew();
        uint64_t t0 = bench_time_ns();
        uint8_t success;
        for (int i = 0; i < N; i++) {
            is = intsetAdd(is, values[order[i]], &success);
        }
        uint64_t t1 = bench_time_ns();

        uint64_t per_op = (t1 - t0) / N;
        for (int i = 0; i < n; i++) samples[i] = per_op;
        bench_compute(&rows[1], "G4", "add_rand", samples, n, 0, 0);
        zfree(is);
    }

    /* ---------- contains_hit ---------- */
    {
        intset *is = intsetNew();
        uint8_t success;
        for (int i = 0; i < N; i++) {
            is = intsetAdd(is, values[i], &success);
        }

        uint64_t t0 = bench_time_ns();
        for (int i = 0; i < N; i++) {
            intsetFind(is, values[order[i]]);
        }
        uint64_t t1 = bench_time_ns();

        uint64_t per_op = (t1 - t0) / N;
        for (int i = 0; i < n; i++) samples[i] = per_op;
        bench_compute(&rows[2], "G4", "contains_hit", samples, n, 0, 0);
        zfree(is);
    }

    /* ---------- contains_miss ---------- */
    {
        intset *is = intsetNew();
        uint8_t success;
        for (int i = 0; i < N; i++) {
            is = intsetAdd(is, values[i], &success);
        }

        uint64_t t0 = bench_time_ns();
        for (int i = 0; i < N; i++) {
            intsetFind(is, miss_values[order[i]]);
        }
        uint64_t t1 = bench_time_ns();

        uint64_t per_op = (t1 - t0) / N;
        for (int i = 0; i < n; i++) samples[i] = per_op;
        bench_compute(&rows[3], "G4", "contains_miss", samples, n, 0, 0);
        zfree(is);
    }

    /* ---------- remove ---------- */
    {
        intset *is = intsetNew();
        uint8_t success;
        for (int i = 0; i < N; i++) {
            is = intsetAdd(is, values[i], &success);
        }

        uint64_t t0 = bench_time_ns();
        int s;
        for (int i = 0; i < N; i++) {
            is = intsetRemove(is, values[order[i]], &s);
        }
        uint64_t t1 = bench_time_ns();

        uint64_t per_op = (t1 - t0) / N;
        for (int i = 0; i < n; i++) samples[i] = per_op;
        bench_compute(&rows[4], "G4", "remove", samples, n, 0, 0);
        zfree(is);
    }

    bench_write_csv(out_dir, "G4", rows, 5);
    free(order);
    return 0;
}
