#!/bin/sh
# build-and-run-c.sh
#
#

set -eu

REPO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
VALKEY_DIR="${REPO_ROOT}/thirdparty/valkey"
BENCH_OUT_DIR="${BENCH_OUT_DIR:-${REPO_ROOT}/target/bench-results/c}"
BENCH_BUILD_DIR="${BENCH_BUILD_DIR:-${VALKEY_DIR}/build-bench}"
BENCH_JOBS="${BENCH_JOBS:-$(nproc 2>/dev/null || echo 4)}"

mkdir -p "${BENCH_OUT_DIR}"
mkdir -p "${BENCH_BUILD_DIR}"

echo "[bench-c] cmake configure -> ${BENCH_BUILD_DIR}"
cmake -S "${VALKEY_DIR}" -B "${BENCH_BUILD_DIR}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_UNIT_GTESTS=ON \
    -DBUILD_LUA=no \
    >/tmp/bench-c-configure.log 2>&1 || {
    echo "[bench-c] configure FAILED; log:" >&2
    tail -80 /tmp/bench-c-configure.log >&2
    exit 1
}

echo "[bench-c] cmake build valkey-unit-gtests (j=${BENCH_JOBS})"
cmake --build "${BENCH_BUILD_DIR}" --target valkey-unit-gtests -j "${BENCH_JOBS}" \
    >/tmp/bench-c-build.log 2>&1 || {
    echo "[bench-c] build FAILED; log tail:" >&2
    tail -120 /tmp/bench-c-build.log >&2
    exit 1
}

BINARY="${BENCH_BUILD_DIR}/src/unit/valkey-unit-gtests"
if [ ! -x "${BINARY}" ]; then
    ALT="$(find "${BENCH_BUILD_DIR}" -type f -name valkey-unit-gtests -perm -u+x 2>/dev/null | head -1)"
    if [ -z "${ALT}" ]; then
        echo "[bench-c] cannot find valkey-unit-gtests binary" >&2
        exit 1
    fi
    BINARY="${ALT}"
fi

echo "[bench-c] running BenchG*.All → ${BENCH_OUT_DIR}"
BENCH_OUT_DIR="${BENCH_OUT_DIR}" "${BINARY}" \
    --gtest_filter='BenchG*.All' \
    --gtest_color=no \
    | tail -40

echo "[bench-c] CSV artifacts:"
ls -la "${BENCH_OUT_DIR}"
