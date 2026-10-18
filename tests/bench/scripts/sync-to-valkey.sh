#!/bin/sh
# sync-to-valkey.sh
#

set -eu

REPO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
SRC_DIR="${REPO_ROOT}/tests/bench/c"
DST_DIR="${REPO_ROOT}/thirdparty/valkey/src/unit"

if [ ! -d "${DST_DIR}" ]; then
    echo "[sync] valkey submodule not initialized: ${DST_DIR}" >&2
    exit 1
fi

echo "[sync] copying bench sources -> ${DST_DIR}"
cp -v "${SRC_DIR}/bench_common.hpp" "${DST_DIR}/"
for f in "${SRC_DIR}"/bench_g*.cpp; do
    cp -v "${f}" "${DST_DIR}/"
done

echo "[sync] done"
