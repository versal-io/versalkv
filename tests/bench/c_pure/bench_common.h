/*
 * Pure C microbenchmark harness — links against valkey .o files.
 *
 * Output CSV format matches Rust side:
 *   group,case,median_ns_per_op,p95_ns_per_op,ops_per_sec,live_bytes,rss_kb
 */
#ifndef BENCH_COMMON_H
#define BENCH_COMMON_H

#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/resource.h>

#define BENCH_WARMUP 3
#define BENCH_SAMPLES 30

typedef struct {
    const char *group;
    const char *case_name;
    uint64_t median_ns_per_op;
    uint64_t p95_ns_per_op;
    uint64_t ops_per_sec;
    size_t live_bytes;
    uint64_t rss_kb;
} bench_row_t;

static inline uint64_t bench_time_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static inline uint64_t bench_rss_kb(void) {
    FILE *f = fopen("/proc/self/status", "r");
    if (!f) return 0;
    char line[256];
    uint64_t v = 0;
    while (fgets(line, sizeof(line), f)) {
        if (strncmp(line, "VmRSS:", 6) == 0) {
            char *p = line + 6;
            v = 0;
            while (*p) {
                if (*p >= '0' && *p <= '9') v = v * 10 + (*p - '0');
                p++;
            }
            break;
        }
    }
    fclose(f);
    return v;
}

static int cmp_uint64(const void *a, const void *b) {
    uint64_t x = *(const uint64_t *)a;
    uint64_t y = *(const uint64_t *)b;
    return (x > y) - (x < y);
}

static inline void bench_compute(bench_row_t *row, const char *group,
                                  const char *case_name, uint64_t *samples,
                                  int n_samples, size_t live_bytes, uint64_t rss_kb) {
    qsort(samples, n_samples, sizeof(uint64_t), cmp_uint64);
    row->group = group;
    row->case_name = case_name;
    row->median_ns_per_op = n_samples ? samples[n_samples / 2] : 0;
    row->p95_ns_per_op = n_samples ? samples[n_samples * 95 / 100] : 0;
    row->ops_per_sec = row->median_ns_per_op ? (1000000000ULL / row->median_ns_per_op) : 0;
    row->live_bytes = live_bytes;
    row->rss_kb = rss_kb;
}

static inline void bench_write_csv(const char *out_dir, const char *group,
                                    bench_row_t *rows, int n_rows) {
    char path[512];
    snprintf(path, sizeof(path), "%s/%s.csv", out_dir, group);
    FILE *f = fopen(path, "w");
    if (!f) {
        fprintf(stderr, "[bench] failed to open %s\n", path);
        return;
    }
    fprintf(f, "group,case,median_ns_per_op,p95_ns_per_op,ops_per_sec,live_bytes,rss_kb\n");
    for (int i = 0; i < n_rows; i++) {
        fprintf(f, "%s,%s,%llu,%llu,%llu,%zu,%llu\n",
                rows[i].group, rows[i].case_name,
                (unsigned long long)rows[i].median_ns_per_op,
                (unsigned long long)rows[i].p95_ns_per_op,
                (unsigned long long)rows[i].ops_per_sec,
                rows[i].live_bytes,
                (unsigned long long)rows[i].rss_kb);
    }
    fclose(f);
    fprintf(stderr, "[bench] wrote %s\n", path);
}

#endif /* BENCH_COMMON_H */
