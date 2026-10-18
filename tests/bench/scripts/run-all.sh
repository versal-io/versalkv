#!/bin/sh
# run-all.sh
#
#
#   tests/bench/scripts/run-all.sh
#

set -eu

REPO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
SCRIPTS="${REPO_ROOT}/tests/bench/scripts"

export RUST_OUT_DIR="${REPO_ROOT}/target/bench-results/rust"
mkdir -p "${RUST_OUT_DIR}"
echo "[run-all] running Rust bench (release)"
for g in 1 2 3 4 5 6 7 8 9 10; do
    case ${g} in
        1) bench=bench_g1_dict ;;
        2) bench=bench_g2_hashmap_sort ;;
        3) bench=bench_g3_vecdeque ;;
        4) bench=bench_g4_hashset_int ;;
        5) bench=bench_g5_indexmap ;;
        6) bench=bench_g6_set_encoding ;;
        7) bench=bench_g7_hash_encoding ;;
        8) bench=bench_g8_list_encoding ;;
        9) bench=bench_g9_zset_encoding ;;
        10) bench=bench_g10_string_encoding ;;
    esac
    echo "[run-all] Rust G${g} → ${bench}"
    BENCH_OUT_DIR="${RUST_OUT_DIR}" \
        cargo bench --manifest-path "${REPO_ROOT}/Cargo.toml" \
        --bench "${bench}" --profile bench >/dev/null
done

echo "[run-all] sync bench sources to valkey submodule"
sh "${SCRIPTS}/sync-to-valkey.sh"

echo "[run-all] build and run C bench"
BENCH_OUT_DIR="${REPO_ROOT}/target/bench-results/c" sh "${SCRIPTS}/build-and-run-c.sh"

echo "[run-all] merge CSV → comparison.md"
python3 "${SCRIPTS}/merge.py"

echo "[run-all] done. See ${REPO_ROOT}/target/bench-results/comparison.md"
