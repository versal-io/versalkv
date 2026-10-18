# Makefile for pure C microbenchmarks
# Links against valkey C object files.

VALKEY_DIR := ../../../thirdparty/valkey
VALKEY_SRC := $(VALKEY_DIR)/src
VALKEY_DEPS := $(VALKEY_DIR)/deps

CC := gcc
CFLAGS := -O2 -g -std=gnu11 -Wall -Wextra
CFLAGS += -I$(VALKEY_SRC) -I$(VALKEY_DEPS)/jemalloc/include -I$(VALKEY_DEPS)/hdr_histogram

# Core valkey objects we need
VALKEY_OBJS := \
	$(VALKEY_SRC)/hashtable.o \
	$(VALKEY_SRC)/zmalloc.o \
	$(VALKEY_SRC)/sds.o \
	$(VALKEY_SRC)/intset.o \
	$(VALKEY_SRC)/ziplist.o \
	$(VALKEY_SRC)/listpack.o \
	$(VALKEY_SRC)/quicklist.o \
	$(VALKEY_SRC)/util.o \
	$(VALKEY_SRC)/crc64.o \
	$(VALKEY_SRC)/monotonic.o \
	$(VALKEY_SRC)/mt19937-64.o \
	$(VALKEY_SRC)/siphash.o \
	$(VALKEY_SRC)/serverassert.o \
	$(VALKEY_DEPS)/fpconv/fpconv_dtoa.o \
	$(VALKEY_DEPS)/hdr_histogram/hdr_histogram.o

LDFLAGS := -lm -lpthread -ldl
LDFLAGS += $(VALKEY_DEPS)/jemalloc/lib/libjemalloc.a

OUT_DIR := ../../../target/bench-results/c_pure

.PHONY: all clean run

all: bench_g1 bench_g4 bench_g5

bench_g1: bench_g1_dict.c bench_common.h
	$(CC) $(CFLAGS) -o $@ bench_g1_dict.c $(VALKEY_OBJS) $(LDFLAGS)

bench_g4: bench_g4_intset.c bench_common.h
	$(CC) $(CFLAGS) -o $@ bench_g4_intset.c $(VALKEY_OBJS) $(LDFLAGS)

bench_g5: bench_g5_listpack.c bench_common.h
	$(CC) $(CFLAGS) -o $@ bench_g5_listpack.c $(VALKEY_OBJS) $(LDFLAGS)

run: all
	mkdir -p $(OUT_DIR)
	@echo "=== G1: dict/hashtable ==="
	./bench_g1
	@echo "=== G4: intset ==="
	./bench_g4
	@echo "=== G5: listpack ==="
	./bench_g5

clean:
	rm -f bench_g1 bench_g4 bench_g5
