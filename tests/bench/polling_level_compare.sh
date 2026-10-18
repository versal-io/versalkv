#!/usr/bin/env bash

set -Eeuo pipefail
IFS=$'\n\t'

readonly PARENT_COMMIT="8f3b5db686e52c0cedd2a23c0ea296bba57b2815"
readonly SERVER_CPUS="2-6"
readonly CLIENT_CPU="7"
readonly AUX_CPU="0"

usage() {
    printf '%s\n' \
        "Usage: tests/bench/polling_level_compare.sh [--mode smoke|formal] [--output-dir PATH]" \
        "" \
        "Runs the four-stage Mio ET, Polling Edge, Polling Level, and Polling Level" \
        "with IO threads comparison. All generated sources, binaries, JSON, CSV, and" \
        "logs stay under target/polling-level-compare." \
        "" \
        "Options:" \
        "  --mode MODE       smoke or formal (default: formal)" \
        "  --output-dir PATH Explicit run directory below target/polling-level-compare/runs" \
        "  --help            Show this help"
}

MODE="formal"
OUTPUT_DIR=""
while (($# > 0)); do
    case "$1" in
        --mode)
            (($# >= 2)) || { printf '%s\n' "--mode requires a value" >&2; exit 64; }
            MODE="$2"
            shift 2
            ;;
        --output-dir)
            (($# >= 2)) || { printf '%s\n' "--output-dir requires a value" >&2; exit 64; }
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            printf 'Unknown argument: %s\n' "$1" >&2
            usage >&2
            exit 64
            ;;
    esac
done

case "$MODE" in
    smoke)
        RUNS=1
        SINGLE_REQUESTS=2000
        PIPELINE_REQUESTS=4000
        POLL_REQUESTS=3000
        QUEUE_REQUESTS=5000
        REPLICATION_REQUESTS=1000
        CLUSTER_ROUNDS=1
        BLOCKED_REPLY_BYTES=$((8 * 1024 * 1024))
        PORT_MIN=18100
        PORT_MAX=18259
        CLUSTER_BUS_PORT_MIN=28100
        CLUSTER_BUS_PORT_MAX=28259
        ;;
    formal)
        RUNS=3
        SINGLE_REQUESTS=100000
        PIPELINE_REQUESTS=200000
        POLL_REQUESTS=100000
        QUEUE_REQUESTS=500000
        REPLICATION_REQUESTS=20000
        CLUSTER_ROUNDS=3
        BLOCKED_REPLY_BYTES=$((32 * 1024 * 1024))
        PORT_MIN=20100
        PORT_MAX=20259
        CLUSTER_BUS_PORT_MIN=30100
        CLUSTER_BUS_PORT_MAX=30259
        ;;
    *)
        printf 'Unsupported mode: %s\n' "$MODE" >&2
        exit 64
        ;;
esac

readonly PORT_MIN PORT_MAX CLUSTER_BUS_PORT_MIN CLUSTER_BUS_PORT_MAX

readonly CLIENTS=50
readonly PAYLOAD_BYTES=1024
readonly KEYSPACE=10000
readonly PIPELINE_DEPTH=16
readonly BLOCKED_INTERVAL_SECONDS=1
readonly BLOCKED_STALL_DEADLINE_SECONDS=5
readonly BLOCKED_STALL_REQUIRED_SAMPLES=5
readonly BLOCKED_STALL_SAMPLE_INTERVAL_SECONDS=0.025
readonly BENCHMARK_TIMEOUT_SECONDS=120

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly SCRIPT_DIR
REPO_ROOT="$(git -C "$SCRIPT_DIR/../.." rev-parse --show-toplevel)"
readonly REPO_ROOT
readonly ARTIFACT_ROOT="$REPO_ROOT/target/polling-level-compare"
readonly RUNS_ROOT="$ARTIFACT_ROOT/runs"
readonly CACHE_ROOT="$ARTIFACT_ROOT/cache"
readonly SUBMODULE_PATH="$REPO_ROOT/thirdparty/valkey"

if [[ -z "$OUTPUT_DIR" ]]; then
    OUTPUT_DIR="$RUNS_ROOT/${MODE}-$(date -u +%Y%m%dT%H%M%SZ)-$$"
fi
OUTPUT_DIR="$(realpath -m "$OUTPUT_DIR")"
case "$OUTPUT_DIR" in
    "$RUNS_ROOT"/*) ;;
    *)
        printf 'Refusing output path outside %s: %s\n' "$RUNS_ROOT" "$OUTPUT_DIR" >&2
        exit 64
        ;;
esac
if [[ -e "$OUTPUT_DIR" ]]; then
    printf 'Refusing to overwrite existing run directory: %s\n' "$OUTPUT_DIR" >&2
    exit 73
fi
mkdir -p "$OUTPUT_DIR" "$CACHE_ROOT"

readonly COMMAND_LOG="$OUTPUT_DIR/commands.log"
readonly THROUGHPUT_CSV="$OUTPUT_DIR/throughput-runs.csv"
readonly MEASUREMENTS_CSV="$OUTPUT_DIR/measurements.csv"
readonly STAGES_CSV="$OUTPUT_DIR/stages.csv"
readonly SUBMODULE_STATUS_BEFORE="$OUTPUT_DIR/thirdparty-status-before.txt"
readonly SUBMODULE_STATUS_AFTER="$OUTPUT_DIR/thirdparty-status-after.txt"

printf '%s\n' 'stage,workload,run,requests,clients,pipeline,payload_bytes,keyspace,rps,p50_us,p99_us,eventloop_avg_us,eventloop_max_us,eventloop_instantaneous_us' > "$THROUGHPUT_CSV"
printf '%s\n' 'stage,metric,run,value,unit,status,detail' > "$MEASUREMENTS_CSV"
printf '%s\n' 'stage,label,backend,io_threads,always_active,binary,source_commit,superproject_digest,gitlink_sha,gitlink_tree,submodule_archive_sha256,submodule_snapshot_sha256,superproject_manifest_sha256,post_edge_manifest_sha256,source_digest' > "$STAGES_CSV"
: > "$COMMAND_LOG"
git -C "$SUBMODULE_PATH" status --porcelain=v1 --untracked-files=all > "$SUBMODULE_STATUS_BEFORE"

declare -A CHILD_PIDS=()

track_pid() {
    CHILD_PIDS["$1"]=1
}

untrack_pid() {
    unset 'CHILD_PIDS[$1]'
}

cleanup() {
    local pid
    trap - EXIT INT TERM
    for pid in "${!CHILD_PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill -TERM "$pid" 2>/dev/null || true
        fi
    done
    for pid in "${!CHILD_PIDS[@]}"; do
        wait "$pid" 2>/dev/null || true
    done
}
trap cleanup EXIT INT TERM

quote_command() {
    printf '%q ' "$@" >> "$COMMAND_LOG"
    printf '\n' >> "$COMMAND_LOG"
}

csv_escape() {
    local value=${1//$'\n'/ }
    value=${value//\"/\"\"}
    printf '"%s"' "$value"
}

append_measurement() {
    local stage=$1 metric=$2 run=$3 value=$4 unit=$5 status=$6 detail=$7
    {
        csv_escape "$stage"; printf ','
        csv_escape "$metric"; printf ','
        csv_escape "$run"; printf ','
        csv_escape "$value"; printf ','
        csv_escape "$unit"; printf ','
        csv_escape "$status"; printf ','
        csv_escape "$detail"; printf '\n'
    } >> "$MEASUREMENTS_CSV"
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || {
        printf 'Required command is unavailable: %s\n' "$1" >&2
        exit 69
    }
}

for required in cargo git jq perl python3 realpath redis-cli rustc sha256sum ss strace taskset timeout; do
    require_command "$required"
done

if [[ ! -d "$SUBMODULE_PATH/src/commands" ]]; then
    printf 'NEEDS_CONTEXT: thirdparty/valkey is unavailable for read-only build metadata reuse.\n' >&2
    exit 78
fi

for cpu in 2 3 4 5 6 7; do
    if [[ ! -d "/sys/devices/system/cpu/cpu$cpu" ]]; then
        printf 'Required CPU is unavailable: %s\n' "$cpu" >&2
        exit 69
    fi
done

port_is_listening() {
    local port=$1
    ss -ltnH "sport = :$port" | grep -q .
}

port_has_tcp_state() {
    local port=$1
    ss -tanH "sport = :$port" | grep -q .
}

assert_port_free() {
    local port=$1
    if ((port < PORT_MIN || port > PORT_MAX)); then
        printf 'Port %s is outside the reserved range %s-%s\n' "$port" "$PORT_MIN" "$PORT_MAX" >&2
        exit 64
    fi
    if port_is_listening "$port"; then
        printf 'Reserved benchmark port is already listening: %s\n' "$port" >&2
        exit 69
    fi
}

assert_cluster_bus_port_free() {
    local port=$1
    if ((port < CLUSTER_BUS_PORT_MIN || port > CLUSTER_BUS_PORT_MAX)); then
        printf 'Cluster Bus port %s is outside the reserved range %s-%s\n' "$port" "$CLUSTER_BUS_PORT_MIN" "$CLUSTER_BUS_PORT_MAX" >&2
        exit 64
    fi
    if port_has_tcp_state "$port"; then
        printf 'Reserved Cluster Bus port is already in use: %s\n' "$port" >&2
        exit 69
    fi
}

assert_repo_relative_path() {
    local path=$1
    case "$path" in
        ""|/*|../*|*/../*|*/..)
            printf 'Unsafe repository-relative path: %s\n' "$path" >&2
            exit 78
            ;;
    esac
}

worktree_snapshot_digest() {
    {
        git -C "$REPO_ROOT" rev-parse HEAD
        sha256sum "$REPO_ROOT/Cargo.lock" | awk '{print $1}'
        git -C "$REPO_ROOT" diff --binary HEAD -- . ':(exclude)thirdparty/valkey'
        while IFS= read -r -d '' path; do
            assert_repo_relative_path "$path"
            [[ -f "$REPO_ROOT/$path" ]] || continue
            printf '%s\0' "$path"
            sha256sum "$REPO_ROOT/$path" | awk '{print $1}'
        done < <(git -C "$REPO_ROOT" ls-files --others --exclude-standard -z -- . ':(exclude)thirdparty/valkey')
    } | sha256sum | awk '{print $1}'
}

superproject_gitlink_sha() {
    local commit=$1
    local mode type sha path
    local IFS=$' \t'
    read -r mode type sha path < <(git -C "$REPO_ROOT" ls-tree "$commit" -- thirdparty/valkey)
    if [[ "$mode" != 160000 || "$type" != commit || "$path" != thirdparty/valkey || -z "$sha" ]]; then
        printf 'Unable to resolve thirdparty/valkey gitlink from %s\n' "$commit" >&2
        exit 78
    fi
    printf '%s\n' "$sha"
}

submodule_tree_sha() {
    local gitlink_sha=$1
    git -C "$SUBMODULE_PATH" rev-parse "${gitlink_sha}^{tree}"
}

submodule_archive_sha256() {
    local gitlink_sha=$1
    git -C "$SUBMODULE_PATH" archive "$gitlink_sha" | sha256sum | awk '{print $1}'
}

directory_manifest_sha256() {
    local root=$1
    (
        cd "$root"
        while IFS= read -r -d '' path; do
            printf 'file\0%s\0%s\0' "${path#./}" "$(stat -c '%a' "$path")"
            sha256sum "$path" | awk '{print $1}'
        done < <(find . -type f -print0 | LC_ALL=C sort -z)
        while IFS= read -r -d '' path; do
            printf 'link\0%s\0%s\0' "${path#./}" "$(readlink "$path")"
        done < <(find . -type l -print0 | LC_ALL=C sort -z)
    ) | sha256sum | awk '{print $1}'
}

superproject_build_input_paths() {
    local root=$1
    (
        cd "$root"
        for path in Cargo.toml Cargo.lock build.rs rust-toolchain.toml; do
            if [[ -f "$path" || -L "$path" ]]; then
                printf '%s\0' "$path"
            fi
        done
        for directory in command_macros src; do
            if [[ -d "$directory" ]]; then
                find "$directory" \( -type f -o -type l \) -print0
            fi
        done
    ) | LC_ALL=C sort -zu
}

superproject_manifest_sha256() {
    local root=$1
    (
        cd "$root"
        while IFS= read -r -d '' path; do
            if [[ -L "$path" ]]; then
                printf 'link\0%s\0%s\0' "$path" "$(readlink "$path")"
            else
                printf 'file\0%s\0%s\0' "$path" "$(stat -c '%a' "$path")"
                sha256sum "$path" | awk '{print $1}'
            fi
        done < <(superproject_build_input_paths "$root")
    ) | sha256sum | awk '{print $1}'
}

superproject_path_list_sha256() {
    local root=$1
    superproject_build_input_paths "$root" | sha256sum | awk '{print $1}'
}

superproject_build_input_count() {
    local root=$1
    superproject_build_input_paths "$root" | tr -cd '\0' | wc -c | tr -d ' '
}

superproject_identity_field() {
    local source_root=$1 key=$2
    awk -F= -v wanted="$key" '$1 == wanted {print substr($0, index($0, "=") + 1); exit}' "$source_root/.superproject-snapshot.env"
}

write_superproject_identity() {
    local source_root=$1 mode=$2 pre_edge_manifest=$3 post_edge_manifest=$4 current_manifest=$5 paths_sha=$6 input_count=$7
    printf '%s\n' \
        "mode=$mode" \
        "pre_edge_manifest_sha256=$pre_edge_manifest" \
        "post_edge_manifest_sha256=$post_edge_manifest" \
        "current_manifest_sha256=$current_manifest" \
        "path_list_sha256=$paths_sha" \
        "build_input_count=$input_count" > "$source_root/.superproject-snapshot.env"
}

assert_key_files_match_live() {
    local source_root=$1
    local relative
    for relative in \
        src/runtime/connection/write.rs \
        src/runtime/io_threads/pool.rs \
        src/runtime/transport/mod.rs
    do
        if ! cmp -s "$REPO_ROOT/$relative" "$source_root/$relative"; then
            printf 'Prepared source differs from live key file: %s\n' "$relative" >&2
            exit 78
        fi
    done
}

assert_live_superproject_snapshot() {
    local source_root=$1 evidence_name=$2
    local actual_manifest actual_paths actual_count diff_file evidence_file diff_status
    actual_manifest=$(superproject_manifest_sha256 "$source_root")
    actual_paths=$(superproject_path_list_sha256 "$source_root")
    actual_count=$(superproject_build_input_count "$source_root")
    diff_file="$OUTPUT_DIR/${evidence_name}-src.diff"
    evidence_file="$OUTPUT_DIR/${evidence_name}-source-verification.txt"
    diff_status=0
    diff -qr --exclude=generated "$REPO_ROOT/src" "$source_root/src" > "$diff_file" || diff_status=$?
    {
        printf 'status=%s\n' "$([[ "$diff_status" -eq 0 ]] && printf identical || printf different)"
        printf 'scope=src plus superproject manifest build inputs\n'
        printf 'exclusions=directories named generated\n'
        printf 'metadata_exclusions=thirdparty Cargo.lock diff metadata\n'
        printf 'live_manifest_sha256=%s\n' "$LIVE_SUPERPROJECT_MANIFEST_SHA256"
        printf 'prepared_manifest_sha256=%s\n' "$actual_manifest"
        printf 'live_path_list_sha256=%s\n' "$LIVE_SUPERPROJECT_PATH_LIST_SHA256"
        printf 'prepared_path_list_sha256=%s\n' "$actual_paths"
        printf 'live_build_input_count=%s\n' "$LIVE_SUPERPROJECT_BUILD_INPUT_COUNT"
        printf 'prepared_build_input_count=%s\n' "$actual_count"
        printf 'critical_files=src/runtime/connection/write.rs src/runtime/io_threads/pool.rs src/runtime/transport/mod.rs\n'
    } > "$evidence_file"
    if [[ "$actual_manifest" != "$LIVE_SUPERPROJECT_MANIFEST_SHA256" \
        || "$actual_paths" != "$LIVE_SUPERPROJECT_PATH_LIST_SHA256" \
        || "$actual_count" != "$LIVE_SUPERPROJECT_BUILD_INPUT_COUNT" \
        || "$diff_status" -ne 0 ]]; then
        printf 'Prepared source manifest differs from live worktree; evidence=%s diff=%s\n' "$evidence_file" "$diff_file" >&2
        exit 78
    fi
    assert_key_files_match_live "$source_root"
}

validate_superproject_snapshot() {
    local source_root=$1 expected_mode=$2
    local marker_mode expected_current actual_current
    marker_mode=$(superproject_identity_field "$source_root" mode)
    expected_current=$(superproject_identity_field "$source_root" current_manifest_sha256)
    actual_current=$(superproject_manifest_sha256 "$source_root")
    if [[ "$marker_mode" != "$expected_mode" || -z "$expected_current" || "$actual_current" != "$expected_current" ]]; then
        printf 'Cached superproject snapshot mismatch: mode=%s expected_mode=%s expected=%s actual=%s\n' "$marker_mode" "$expected_mode" "$expected_current" "$actual_current" >&2
        exit 78
    fi
    if [[ "$expected_mode" == level ]]; then
        [[ "$actual_current" == "$LIVE_SUPERPROJECT_MANIFEST_SHA256" ]]
        [[ "$(superproject_identity_field "$source_root" path_list_sha256)" == "$LIVE_SUPERPROJECT_PATH_LIST_SHA256" ]]
        [[ "$(superproject_identity_field "$source_root" build_input_count)" == "$LIVE_SUPERPROJECT_BUILD_INPUT_COUNT" ]]
        assert_key_files_match_live "$source_root"
    elif [[ "$expected_mode" == edge ]]; then
        [[ "$(superproject_identity_field "$source_root" pre_edge_manifest_sha256)" == "$LIVE_SUPERPROJECT_MANIFEST_SHA256" ]]
        [[ -n "$(superproject_identity_field "$source_root" post_edge_manifest_sha256)" ]]
    fi
}

snapshot_identity_field() {
    local source_root=$1 key=$2
    awk -F= -v wanted="$key" '$1 == wanted {print substr($0, index($0, "=") + 1); exit}' "$source_root/.submodule-snapshot.env"
}

assert_submodule_snapshot_unchanged() {
    local source_root=$1
    local snapshot_root="$source_root/thirdparty/valkey"
    local expected actual
    if [[ ! -d "$snapshot_root" || -L "$snapshot_root" || -e "$snapshot_root/.git" ]]; then
        printf 'Submodule snapshot must be a clean archive directory without Git metadata: %s\n' "$snapshot_root" >&2
        exit 78
    fi
    expected=$(snapshot_identity_field "$source_root" snapshot_sha256)
    actual=$(directory_manifest_sha256 "$snapshot_root")
    if [[ -z "$expected" || "$actual" != "$expected" ]]; then
        printf 'Submodule snapshot hash changed: expected=%s actual=%s root=%s\n' "$expected" "$actual" "$snapshot_root" >&2
        exit 78
    fi
}

materialize_submodule_snapshot() {
    local source_root=$1 gitlink_sha=$2 expected_tree=$3 expected_archive_sha=$4
    local snapshot_root="$source_root/thirdparty/valkey"
    local archive_file="$source_root/.submodule-${gitlink_sha}.tar"
    local actual_tree actual_archive_sha snapshot_sha
    actual_tree=$(submodule_tree_sha "$gitlink_sha")
    [[ "$actual_tree" == "$expected_tree" ]]
    if [[ -d "$snapshot_root" && ! -L "$snapshot_root" ]]; then
        rmdir "$snapshot_root"
    fi
    if [[ -e "$snapshot_root" || -L "$snapshot_root" ]]; then
        printf 'Unexpected submodule path in temporary source: %s\n' "$snapshot_root" >&2
        exit 73
    fi
    mkdir -p "$snapshot_root"
    quote_command git -C "$SUBMODULE_PATH" archive "$gitlink_sha"
    git -C "$SUBMODULE_PATH" archive "$gitlink_sha" > "$archive_file"
    actual_archive_sha=$(sha256sum "$archive_file" | awk '{print $1}')
    [[ "$actual_archive_sha" == "$expected_archive_sha" ]]
    tar -x -C "$snapshot_root" -f "$archive_file"
    rm "$archive_file"
    snapshot_sha=$(directory_manifest_sha256 "$snapshot_root")
    printf '%s\n' \
        "gitlink_sha=$gitlink_sha" \
        "gitlink_tree=$actual_tree" \
        "archive_sha256=$actual_archive_sha" \
        "snapshot_sha256=$snapshot_sha" \
        'snapshot_kind=git-archive-without-git-metadata' > "$source_root/.submodule-snapshot.env"
    assert_submodule_snapshot_unchanged "$source_root"
}

validate_cached_source() {
    local source_root=$1 expected_gitlink=$2 expected_tree=$3 expected_archive=$4
    [[ "$(snapshot_identity_field "$source_root" gitlink_sha)" == "$expected_gitlink" ]]
    [[ "$(snapshot_identity_field "$source_root" gitlink_tree)" == "$expected_tree" ]]
    [[ "$(snapshot_identity_field "$source_root" archive_sha256)" == "$expected_archive" ]]
    assert_submodule_snapshot_unchanged "$source_root"
}

prepare_archive_source() {
    local commit=$1 source_root=$2 gitlink_sha=$3 gitlink_tree=$4 archive_sha=$5
    if [[ -f "$source_root/Cargo.toml" ]]; then
        validate_cached_source "$source_root" "$gitlink_sha" "$gitlink_tree" "$archive_sha"
        validate_superproject_snapshot "$source_root" parent
        return
    fi
    local preparing="${source_root}.prepare.$$"
    if [[ -e "$preparing" ]]; then
        printf 'Unexpected preparation path already exists: %s\n' "$preparing" >&2
        exit 73
    fi
    mkdir -p "$preparing"
    git -C "$REPO_ROOT" archive "$commit" | tar -x -C "$preparing"
    materialize_submodule_snapshot "$preparing" "$gitlink_sha" "$gitlink_tree" "$archive_sha"
    cp "$REPO_ROOT/Cargo.lock" "$preparing/Cargo.lock"
    local parent_manifest parent_paths parent_count
    parent_manifest=$(superproject_manifest_sha256 "$preparing")
    parent_paths=$(superproject_path_list_sha256 "$preparing")
    parent_count=$(superproject_build_input_count "$preparing")
    write_superproject_identity "$preparing" parent "$parent_manifest" "" "$parent_manifest" "$parent_paths" "$parent_count"
    mv "$preparing" "$source_root"
    validate_cached_source "$source_root" "$gitlink_sha" "$gitlink_tree" "$archive_sha"
    validate_superproject_snapshot "$source_root" parent
}

prepare_worktree_source() {
    local source_root=$1 mode=$2 gitlink_sha=$3 gitlink_tree=$4 archive_sha=$5
    if [[ -f "$source_root/.working-tree-ready" ]]; then
        validate_cached_source "$source_root" "$gitlink_sha" "$gitlink_tree" "$archive_sha"
        validate_superproject_snapshot "$source_root" "$mode"
        if [[ "$mode" == level ]]; then
            assert_live_superproject_snapshot "$source_root" current-level-binary-source-cached
        fi
        return
    fi
    local preparing="${source_root}.prepare.$$"
    local patch_file="$OUTPUT_DIR/current-working-tree.patch"
    if [[ -e "$preparing" ]]; then
        printf 'Unexpected preparation path already exists: %s\n' "$preparing" >&2
        exit 73
    fi
    mkdir -p "$preparing"
    git -C "$REPO_ROOT" archive HEAD | tar -x -C "$preparing"
    git -C "$REPO_ROOT" diff --binary HEAD -- . ':(exclude)thirdparty/valkey' > "$patch_file"
    if [[ -s "$patch_file" ]]; then
        (
            GIT_CEILING_DIRECTORIES="$REPO_ROOT" git -C "$preparing" apply "$patch_file"
        )
    fi
    while IFS= read -r -d '' path; do
        assert_repo_relative_path "$path"
        mkdir -p "$preparing/$(dirname "$path")"
        cp -a "$REPO_ROOT/$path" "$preparing/$path"
    done < <(git -C "$REPO_ROOT" ls-files --others --exclude-standard -z -- . ':(exclude)thirdparty/valkey')
    materialize_submodule_snapshot "$preparing" "$gitlink_sha" "$gitlink_tree" "$archive_sha"
    cp "$REPO_ROOT/Cargo.lock" "$preparing/Cargo.lock"
    assert_live_superproject_snapshot "$preparing" "current-${mode}-pre-patch"
    local pre_edge_manifest post_edge_manifest paths_sha input_count
    pre_edge_manifest=$(superproject_manifest_sha256 "$preparing")
    paths_sha=$(superproject_path_list_sha256 "$preparing")
    input_count=$(superproject_build_input_count "$preparing")
    if [[ "$mode" == edge ]]; then
        local api_file="$preparing/src/foundation/io/transport/api.rs"
        local polling_file="$preparing/src/foundation/io/transport/polling.rs"
        perl -0pi -e '
            my $count = s/    #\[cfg\(test\)\]\n    Edge,/    Edge,/;
            END { exit($count == 1 ? 0 : 42); }
        ' "$api_file"
        perl -0pi -e '
            my $constructor = s/Self::with_mode\(TransportPollMode::Level\)/Self::with_mode(TransportPollMode::Edge)/;
            my $match_arm = s/            #\[cfg\(test\)\]\n            TransportPollMode::Edge =>/            TransportPollMode::Edge =>/;
            END { exit($constructor == 1 && $match_arm == 1 ? 0 : 42); }
        ' "$polling_file"
        grep -q 'Self::with_mode(TransportPollMode::Edge)' "$polling_file"
        post_edge_manifest=$(superproject_manifest_sha256 "$preparing")
        [[ "$post_edge_manifest" != "$pre_edge_manifest" ]]
        printf '%s\n' 'temporary production constructor uses TransportPollMode::Edge with three target-only edits' > "$preparing/.polling-edge-ready"
    elif [[ "$mode" != level ]]; then
        printf 'Unsupported working-tree source mode: %s\n' "$mode" >&2
        exit 64
    else
        post_edge_manifest=""
    fi
    write_superproject_identity "$preparing" "$mode" "$pre_edge_manifest" "$post_edge_manifest" "${post_edge_manifest:-$pre_edge_manifest}" "$paths_sha" "$input_count"
    printf '%s\n' "$mode" > "$preparing/.working-tree-ready"
    mv "$preparing" "$source_root"
    validate_cached_source "$source_root" "$gitlink_sha" "$gitlink_tree" "$archive_sha"
    validate_superproject_snapshot "$source_root" "$mode"
    if [[ "$mode" == level ]]; then
        assert_live_superproject_snapshot "$source_root" current-level-pre-build
    fi
}

build_server() {
    local source_root=$1 target_root=$2 allow_lock_update=$3 log_file=$4 mode=$5
    mkdir -p "$target_root"
    local -a command=(cargo build --release --bin versal-server)
    if [[ "$allow_lock_update" == no ]]; then
        command+=(--locked)
    fi
    assert_submodule_snapshot_unchanged "$source_root"
    validate_superproject_snapshot "$source_root" "$mode"
    quote_command env "CARGO_TARGET_DIR=$target_root" "${command[@]}"
    (
        cd "$source_root"
        CARGO_TARGET_DIR="$target_root" "${command[@]}"
    ) > >(tee "$log_file") 2>&1
    assert_submodule_snapshot_unchanged "$source_root"
    if [[ "$mode" == parent ]]; then
        local parent_pre parent_current parent_paths parent_count
        parent_pre=$(superproject_identity_field "$source_root" pre_edge_manifest_sha256)
        parent_current=$(superproject_manifest_sha256 "$source_root")
        parent_paths=$(superproject_path_list_sha256 "$source_root")
        parent_count=$(superproject_build_input_count "$source_root")
        write_superproject_identity "$source_root" parent "$parent_pre" "" "$parent_current" "$parent_paths" "$parent_count"
    fi
    validate_superproject_snapshot "$source_root" "$mode"
    if [[ "$mode" == level ]]; then
        assert_live_superproject_snapshot "$source_root" current-level-post-build
    fi
}

identity_digest() {
    printf '%s\0' "$@" | sha256sum | awk '{print $1}'
}

readonly HEAD_COMMIT="$(git -C "$REPO_ROOT" rev-parse HEAD)"
readonly PARENT_GITLINK_SHA="$(superproject_gitlink_sha "$PARENT_COMMIT")"
readonly CURRENT_GITLINK_SHA="$(superproject_gitlink_sha "$HEAD_COMMIT")"
git -C "$SUBMODULE_PATH" cat-file -e "${PARENT_GITLINK_SHA}^{commit}"
git -C "$SUBMODULE_PATH" cat-file -e "${CURRENT_GITLINK_SHA}^{commit}"
readonly PARENT_GITLINK_TREE="$(submodule_tree_sha "$PARENT_GITLINK_SHA")"
readonly CURRENT_GITLINK_TREE="$(submodule_tree_sha "$CURRENT_GITLINK_SHA")"
readonly PARENT_SUBMODULE_ARCHIVE_SHA256="$(submodule_archive_sha256 "$PARENT_GITLINK_SHA")"
readonly CURRENT_SUBMODULE_ARCHIVE_SHA256="$(submodule_archive_sha256 "$CURRENT_GITLINK_SHA")"
readonly WORKTREE_SNAPSHOT_DIGEST="$(worktree_snapshot_digest)"
readonly LOCK_DIGEST="$(sha256sum "$REPO_ROOT/Cargo.lock" | awk '{print $1}')"
readonly LIVE_SUPERPROJECT_MANIFEST_SHA256="$(superproject_manifest_sha256 "$REPO_ROOT")"
readonly LIVE_SUPERPROJECT_PATH_LIST_SHA256="$(superproject_path_list_sha256 "$REPO_ROOT")"
readonly LIVE_SUPERPROJECT_BUILD_INPUT_COUNT="$(superproject_build_input_count "$REPO_ROOT")"
readonly A_SOURCE_DIGEST="$(identity_digest clean-v3 "$PARENT_COMMIT" "$PARENT_GITLINK_SHA" "$PARENT_GITLINK_TREE" "$PARENT_SUBMODULE_ARCHIVE_SHA256" "$LOCK_DIGEST")"
readonly B_SOURCE_DIGEST="$(identity_digest clean-v3 edge "$HEAD_COMMIT" "$LIVE_SUPERPROJECT_MANIFEST_SHA256" "$LIVE_SUPERPROJECT_PATH_LIST_SHA256" "$CURRENT_GITLINK_SHA" "$CURRENT_GITLINK_TREE" "$CURRENT_SUBMODULE_ARCHIVE_SHA256")"
readonly CURRENT_SOURCE_DIGEST="$(identity_digest clean-v3 level "$HEAD_COMMIT" "$LIVE_SUPERPROJECT_MANIFEST_SHA256" "$LIVE_SUPERPROJECT_PATH_LIST_SHA256" "$CURRENT_GITLINK_SHA" "$CURRENT_GITLINK_TREE" "$CURRENT_SUBMODULE_ARCHIVE_SHA256")"
readonly MIO_CACHE="$CACHE_ROOT/clean-v3-mio-et-${A_SOURCE_DIGEST}"
readonly EDGE_CACHE="$CACHE_ROOT/clean-v3-polling-edge-${B_SOURCE_DIGEST}"
readonly CURRENT_CACHE="$CACHE_ROOT/clean-v3-polling-level-${CURRENT_SOURCE_DIGEST}"

mkdir -p "$MIO_CACHE" "$EDGE_CACHE" "$CURRENT_CACHE"
prepare_archive_source "$PARENT_COMMIT" "$MIO_CACHE/source" "$PARENT_GITLINK_SHA" "$PARENT_GITLINK_TREE" "$PARENT_SUBMODULE_ARCHIVE_SHA256"
if [[ ! -x "$MIO_CACHE/target/release/versal-server" ]]; then
    build_server "$MIO_CACHE/source" "$MIO_CACHE/target" yes "$OUTPUT_DIR/build-mio-et.log" parent
fi

prepare_worktree_source "$EDGE_CACHE/source" edge "$CURRENT_GITLINK_SHA" "$CURRENT_GITLINK_TREE" "$CURRENT_SUBMODULE_ARCHIVE_SHA256"
if [[ ! -x "$EDGE_CACHE/target/release/versal-server" ]]; then
    build_server "$EDGE_CACHE/source" "$EDGE_CACHE/target" no "$OUTPUT_DIR/build-polling-edge.log" edge
fi

prepare_worktree_source "$CURRENT_CACHE/source" level "$CURRENT_GITLINK_SHA" "$CURRENT_GITLINK_TREE" "$CURRENT_SUBMODULE_ARCHIVE_SHA256"
if [[ ! -x "$CURRENT_CACHE/target/release/versal-server" || ! -x "$CURRENT_CACHE/target/release/versal-benchmark" ]]; then
    quote_command env "CARGO_TARGET_DIR=$CURRENT_CACHE/target" cargo build --release --locked --bin versal-server --bin versal-benchmark
    assert_submodule_snapshot_unchanged "$CURRENT_CACHE/source"
    validate_superproject_snapshot "$CURRENT_CACHE/source" level
    assert_live_superproject_snapshot "$CURRENT_CACHE/source" current-level-binary-source-pre-build
    (
        cd "$CURRENT_CACHE/source"
        CARGO_TARGET_DIR="$CURRENT_CACHE/target" cargo build --release --locked --bin versal-server --bin versal-benchmark
    ) > >(tee "$OUTPUT_DIR/build-polling-level.log") 2>&1
    assert_submodule_snapshot_unchanged "$CURRENT_CACHE/source"
    validate_superproject_snapshot "$CURRENT_CACHE/source" level
    assert_live_superproject_snapshot "$CURRENT_CACHE/source" current-level-binary-source-post-build
fi

readonly A_SUBMODULE_SNAPSHOT_SHA256="$(snapshot_identity_field "$MIO_CACHE/source" snapshot_sha256)"
readonly EDGE_SUBMODULE_SNAPSHOT_SHA256="$(snapshot_identity_field "$EDGE_CACHE/source" snapshot_sha256)"
readonly CURRENT_SUBMODULE_SNAPSHOT_SHA256="$(snapshot_identity_field "$CURRENT_CACHE/source" snapshot_sha256)"
[[ "$EDGE_SUBMODULE_SNAPSHOT_SHA256" == "$CURRENT_SUBMODULE_SNAPSHOT_SHA256" ]]
readonly A_SUPERPROJECT_MANIFEST_SHA256="$(superproject_identity_field "$MIO_CACHE/source" current_manifest_sha256)"
readonly EDGE_PRE_PATCH_MANIFEST_SHA256="$(superproject_identity_field "$EDGE_CACHE/source" pre_edge_manifest_sha256)"
readonly EDGE_POST_PATCH_MANIFEST_SHA256="$(superproject_identity_field "$EDGE_CACHE/source" post_edge_manifest_sha256)"
readonly CURRENT_SUPERPROJECT_MANIFEST_SHA256="$(superproject_identity_field "$CURRENT_CACHE/source" current_manifest_sha256)"
[[ "$EDGE_PRE_PATCH_MANIFEST_SHA256" == "$LIVE_SUPERPROJECT_MANIFEST_SHA256" ]]
[[ "$CURRENT_SUPERPROJECT_MANIFEST_SHA256" == "$LIVE_SUPERPROJECT_MANIFEST_SHA256" ]]

readonly CLIENT_BINARY="$CURRENT_CACHE/target/release/versal-benchmark"
declare -A STAGE_BINARY=(
    [A]="$MIO_CACHE/target/release/versal-server"
    [B]="$EDGE_CACHE/target/release/versal-server"
    [C]="$CURRENT_CACHE/target/release/versal-server"
    [D]="$CURRENT_CACHE/target/release/versal-server"
)
declare -A STAGE_LABEL=(
    [A]="Mio ET baseline"
    [B]="Polling Edge"
    [C]="Polling Level io-threads=1"
    [D]="Polling Level io-threads=5 always-active=yes"
)
declare -A STAGE_BACKEND=([A]="Mio ET" [B]="Polling Edge" [C]="Polling Level" [D]="Polling Level")
declare -A STAGE_THREADS=([A]=1 [B]=1 [C]=1 [D]=5)
declare -A STAGE_ALWAYS=([A]=no [B]=no [C]=no [D]=yes)
declare -A STAGE_COMMIT=([A]="$PARENT_COMMIT" [B]="working-tree:$WORKTREE_SNAPSHOT_DIGEST" [C]="working-tree:$WORKTREE_SNAPSHOT_DIGEST" [D]="working-tree:$WORKTREE_SNAPSHOT_DIGEST")
declare -A STAGE_SUPERPROJECT_DIGEST=([A]="$PARENT_COMMIT" [B]="$WORKTREE_SNAPSHOT_DIGEST" [C]="$WORKTREE_SNAPSHOT_DIGEST" [D]="$WORKTREE_SNAPSHOT_DIGEST")
declare -A STAGE_GITLINK_SHA=([A]="$PARENT_GITLINK_SHA" [B]="$CURRENT_GITLINK_SHA" [C]="$CURRENT_GITLINK_SHA" [D]="$CURRENT_GITLINK_SHA")
declare -A STAGE_GITLINK_TREE=([A]="$PARENT_GITLINK_TREE" [B]="$CURRENT_GITLINK_TREE" [C]="$CURRENT_GITLINK_TREE" [D]="$CURRENT_GITLINK_TREE")
declare -A STAGE_SUBMODULE_ARCHIVE_SHA256=([A]="$PARENT_SUBMODULE_ARCHIVE_SHA256" [B]="$CURRENT_SUBMODULE_ARCHIVE_SHA256" [C]="$CURRENT_SUBMODULE_ARCHIVE_SHA256" [D]="$CURRENT_SUBMODULE_ARCHIVE_SHA256")
declare -A STAGE_SUBMODULE_SNAPSHOT_SHA256=([A]="$A_SUBMODULE_SNAPSHOT_SHA256" [B]="$EDGE_SUBMODULE_SNAPSHOT_SHA256" [C]="$CURRENT_SUBMODULE_SNAPSHOT_SHA256" [D]="$CURRENT_SUBMODULE_SNAPSHOT_SHA256")
declare -A STAGE_SUPERPROJECT_MANIFEST_SHA256=([A]="$A_SUPERPROJECT_MANIFEST_SHA256" [B]="$EDGE_PRE_PATCH_MANIFEST_SHA256" [C]="$CURRENT_SUPERPROJECT_MANIFEST_SHA256" [D]="$CURRENT_SUPERPROJECT_MANIFEST_SHA256")
declare -A STAGE_POST_EDGE_MANIFEST_SHA256=([A]="" [B]="$EDGE_POST_PATCH_MANIFEST_SHA256" [C]="" [D]="")
declare -A STAGE_SOURCE_DIGEST=([A]="$A_SOURCE_DIGEST" [B]="$B_SOURCE_DIGEST" [C]="$CURRENT_SOURCE_DIGEST" [D]="$CURRENT_SOURCE_DIGEST")

for stage in A B C D; do
    {
        csv_escape "$stage"; printf ','
        csv_escape "${STAGE_LABEL[$stage]}"; printf ','
        csv_escape "${STAGE_BACKEND[$stage]}"; printf ','
        csv_escape "${STAGE_THREADS[$stage]}"; printf ','
        csv_escape "${STAGE_ALWAYS[$stage]}"; printf ','
        csv_escape "${STAGE_BINARY[$stage]}"; printf ','
        csv_escape "${STAGE_COMMIT[$stage]}"; printf ','
        csv_escape "${STAGE_SUPERPROJECT_DIGEST[$stage]}"; printf ','
        csv_escape "${STAGE_GITLINK_SHA[$stage]}"; printf ','
        csv_escape "${STAGE_GITLINK_TREE[$stage]}"; printf ','
        csv_escape "${STAGE_SUBMODULE_ARCHIVE_SHA256[$stage]}"; printf ','
        csv_escape "${STAGE_SUBMODULE_SNAPSHOT_SHA256[$stage]}"; printf ','
        csv_escape "${STAGE_SUPERPROJECT_MANIFEST_SHA256[$stage]}"; printf ','
        csv_escape "${STAGE_POST_EDGE_MANIFEST_SHA256[$stage]}"; printf ','
        csv_escape "${STAGE_SOURCE_DIGEST[$stage]}"; printf '\n'
    } >> "$STAGES_CSV"
done

write_server_config() {
    local config=$1 port=$2 data_dir=$3 threads=$4 always=$5
    mkdir -p "$data_dir"
    printf '%s\n' \
        "bind 127.0.0.1" \
        "port $port" \
        "protected-mode no" \
        "dir $data_dir" \
        'save ""' \
        "appendonly no" \
        "loglevel notice" \
        "max-new-connections-per-cycle 100" \
        "io-threads $threads" \
        "io-threads-always-active $always" > "$config"
}

write_replica_config() {
    local config=$1 port=$2 data_dir=$3 threads=$4 always=$5 primary_port=$6
    write_server_config "$config" "$port" "$data_dir" "$threads" "$always"
    printf 'replicaof 127.0.0.1 %s\n' "$primary_port" >> "$config"
}

write_cluster_config() {
    local config=$1 port=$2 data_dir=$3 threads=$4 always=$5
    write_server_config "$config" "$port" "$data_dir" "$threads" "$always"
    printf '%s\n' \
        "cluster-enabled yes" \
        "cluster-config-file $data_dir/nodes.conf" \
        "cluster-node-timeout 5000" \
        "cluster-announce-ip 127.0.0.1" \
        "cluster-announce-port $port" \
        "cluster-announce-bus-port $((port + 10000))" >> "$config"
}

wait_for_server() {
    local pid=$1 port=$2 log_file=$3
    local attempt
    for attempt in $(seq 1 300); do
        if ! kill -0 "$pid" 2>/dev/null; then
            printf 'Server exited before readiness on port %s. Log: %s\n' "$port" "$log_file" >&2
            return 1
        fi
        if taskset -c "$AUX_CPU" redis-cli -h 127.0.0.1 -p "$port" ping 2>/dev/null | grep -qx PONG; then
            return 0
        fi
        sleep 0.05
    done
    printf 'Server readiness timed out on port %s. Log: %s\n' "$port" "$log_file" >&2
    return 1
}

STARTED_PID=""
start_server() {
    local binary=$1 config=$2 log_file=$3 port=$4
    assert_port_free "$port"
    quote_command taskset -c "$SERVER_CPUS" "$binary" "$config"
    taskset -c "$SERVER_CPUS" "$binary" "$config" > "$log_file" 2>&1 &
    STARTED_PID=$!
    track_pid "$STARTED_PID"
    if ! wait_for_server "$STARTED_PID" "$port" "$log_file"; then
        return 1
    fi
}

stop_server() {
    local pid=$1 port=$2
    timeout 2 taskset -c "$AUX_CPU" redis-cli -h 127.0.0.1 -p "$port" shutdown nosave >/dev/null 2>&1 || true
    local attempt
    for attempt in $(seq 1 100); do
        if ! kill -0 "$pid" 2>/dev/null; then
            wait "$pid" 2>/dev/null || true
            untrack_pid "$pid"
            return
        fi
        sleep 0.02
    done
    kill -TERM "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    untrack_pid "$pid"
}

fetch_info() {
    local port=$1 output=$2
    taskset -c "$AUX_CPU" redis-cli --raw -h 127.0.0.1 -p "$port" info all > "$output"
}

info_field() {
    local file=$1 key=$2 default_value=${3:-0}
    local value
    value=$(awk -F: -v wanted="$key" '$1 == wanted {sub(/\r$/, "", $2); print $2; exit}' "$file")
    printf '%s\n' "${value:-$default_value}"
}

run_benchmark() {
    local port=$1 requests=$2 clients=$3 pipeline=$4 payload=$5 keyspace=$6 prefix=$7 json_file=$8 log_file=$9
    local -a command=(
        timeout "$BENCHMARK_TIMEOUT_SECONDS" taskset -c "$CLIENT_CPU" "$CLIENT_BINARY"
        -h 127.0.0.1 -p "$port"
        --requests "$requests"
        --clients "$clients"
        --pipeline "$pipeline"
        --key-prefix "$prefix"
        --key-minimum 0
        --key-maximum "$((keyspace - 1))"
        --data-size "$payload"
        --command 'SET __key__ __data__'
        --command-key-pattern S
        --print-percentiles 50,99
        --json-out-file "$json_file"
    )
    quote_command "${command[@]}"
    "${command[@]}" > "$log_file" 2>&1
    jq -e --argjson requests "$requests" '
        .total_requests == $requests and
        .total_errors == 0 and
        ([.commands[] | select(.name == "SET")] | length == 1)
    ' "$json_file" >/dev/null
}

run_fixed_key_set() {
    local port=$1 bytes=$2 json_file=$3 log_file=$4
    local -a command=(
        timeout "$BENCHMARK_TIMEOUT_SECONDS" taskset -c "$CLIENT_CPU" "$CLIENT_BINARY"
        -h 127.0.0.1 -p "$port"
        --requests 1 --clients 1 --pipeline 1
        --data-size "$bytes"
        --command 'SET blocked-writer-key __data__'
        --print-percentiles 50,99
        --json-out-file "$json_file"
    )
    quote_command "${command[@]}"
    "${command[@]}" > "$log_file" 2>&1
    jq -e '.total_requests == 1 and .total_errors == 0' "$json_file" >/dev/null
}

append_throughput_row() {
    local stage=$1 workload=$2 run=$3 requests=$4 pipeline=$5 json_file=$6 before_info=$7 after_info=$8
    local rps p50 p99 cycles_before cycles_after sum_before sum_after cycle_delta sum_delta avg_us max_us instant_us
    rps=$(jq -r '.commands[] | select(.name == "SET") | .rps' "$json_file")
    p50=$(jq -r '.commands[] | select(.name == "SET") | .percentiles[] | select(.percentile == 50) | .latency_us' "$json_file")
    p99=$(jq -r '.commands[] | select(.name == "SET") | .percentiles[] | select(.percentile == 99) | .latency_us' "$json_file")
    cycles_before=$(info_field "$before_info" eventloop_cycles)
    cycles_after=$(info_field "$after_info" eventloop_cycles)
    sum_before=$(info_field "$before_info" eventloop_duration_sum)
    sum_after=$(info_field "$after_info" eventloop_duration_sum)
    cycle_delta=$((cycles_after - cycles_before))
    sum_delta=$((sum_after - sum_before))
    avg_us=$(awk -v total="$sum_delta" -v count="$cycle_delta" 'BEGIN { if (count > 0) printf "%.3f", total / count; else print "0" }')
    max_us=$(info_field "$after_info" eventloop_duration_max)
    instant_us=$(info_field "$after_info" instantaneous_eventloop_duration_usec)
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$stage" "$workload" "$run" "$requests" "$CLIENTS" "$pipeline" "$PAYLOAD_BYTES" "$KEYSPACE" \
        "$rps" "$p50" "$p99" "$avg_us" "$max_us" "$instant_us" >> "$THROUGHPUT_CSV"
}

run_throughput_stage() {
    local stage=$1 stage_dir=$2 port=$3
    local config="$stage_dir/throughput.conf"
    write_server_config "$config" "$port" "$stage_dir/throughput-data" "${STAGE_THREADS[$stage]}" "${STAGE_ALWAYS[$stage]}"
    start_server "${STAGE_BINARY[$stage]}" "$config" "$stage_dir/throughput-server.log" "$port"
    local server_pid=$STARTED_PID
    local workload run requests pipeline prefix json_file log_file before_info after_info
    for workload in single pipeline; do
        if [[ "$workload" == single ]]; then
            requests=$SINGLE_REQUESTS
            pipeline=1
        else
            requests=$PIPELINE_REQUESTS
            pipeline=$PIPELINE_DEPTH
        fi
        for run in $(seq 1 "$RUNS"); do
            taskset -c "$AUX_CPU" redis-cli -h 127.0.0.1 -p "$port" flushall >/dev/null
            prefix="${stage}-${workload}-${run}-"
            json_file="$stage_dir/${workload}-run-${run}.json"
            log_file="$stage_dir/${workload}-run-${run}.log"
            before_info="$stage_dir/${workload}-run-${run}-info-before.txt"
            after_info="$stage_dir/${workload}-run-${run}-info-after.txt"
            fetch_info "$port" "$before_info"
            run_benchmark "$port" "$requests" "$CLIENTS" "$pipeline" "$PAYLOAD_BYTES" "$KEYSPACE" "$prefix" "$json_file" "$log_file"
            fetch_info "$port" "$after_info"
            append_throughput_row "$stage" "$workload" "$run" "$requests" "$pipeline" "$json_file" "$before_info" "$after_info"
        done
    done
    stop_server "$server_pid" "$port"
}

process_ticks() {
    local pid=$1
    awk '{print $14 + $15}' "/proc/$pid/stat"
}

client_list_field() {
    local line=$1 field=$2
    awk -v wanted="$field" '{
        for (idx = 1; idx <= NF; idx++) {
            separator = index($idx, "=")
            if (separator > 0 && substr($idx, 1, separator - 1) == wanted) {
                print substr($idx, separator + 1)
                exit
            }
        }
    }' <<< "$line"
}

STALL_SETTLE_MS=""
STALL_STABLE_SAMPLES=""
STALL_OUTPUT_BYTES=""
STALL_PENDING_REPLY_BYTES=""

server_tcp_bytes_sent() {
    local server_port=$1 client_port=$2
    ss -tinH "sport = :$server_port and dport = :$client_port" 2>/dev/null \
        | awk '{
            for (idx = 1; idx <= NF; idx++) {
                if ($idx ~ /^bytes_sent:[0-9]+$/) {
                    sub(/^bytes_sent:/, "", $idx)
                    print $idx
                    exit
                }
            }
        }'
}

wait_for_blocked_writer_stall() {
    local port=$1 client_port=$2 client_pid=$3 samples_file=$4 last_list_file=$5
    local client_name=blocked-writer-probe
    local started_ns now_ns elapsed_ms client_list target_line output_bytes pending_bytes
    local previous_output="" stable_samples=0 client_present
    started_ns=$(date +%s%N)
    printf '%s\n' 'elapsed_ms,client_present,output_bytes,pending_reply_bytes,stable_samples' > "$samples_file"
    while true; do
        now_ns=$(date +%s%N)
        elapsed_ms=$(( (now_ns - started_ns) / 1000000 ))
        if ((elapsed_ms >= BLOCKED_STALL_DEADLINE_SECONDS * 1000)); then
            break
        fi
        if ! kill -0 "$client_pid" 2>/dev/null; then
            break
        fi
        client_list=$(taskset -c "$AUX_CPU" redis-cli --raw -h 127.0.0.1 -p "$port" client list 2>/dev/null || true)
        printf '%s\n' "$client_list" > "$last_list_file"
        target_line=$(awk -v wanted="name=$client_name" '{
            for (idx = 1; idx <= NF; idx++) {
                if ($idx == wanted) {
                    print
                    exit
                }
            }
        }' <<< "$client_list")
        if [[ -n "$target_line" ]]; then
            client_present=1
            pending_bytes=$(client_list_field "$target_line" omem)
            output_bytes=$(server_tcp_bytes_sent "$port" "$client_port")
            if [[ -z "$pending_bytes" ]]; then
                printf 'Target CLIENT LIST row lacks pending output: %s\n' "$target_line" >&2
                return 1
            fi
            if [[ -n "$output_bytes" ]] && ((pending_bytes > 0)) && [[ "$previous_output" == "$output_bytes" ]]; then
                stable_samples=$((stable_samples + 1))
            else
                stable_samples=0
            fi
            previous_output=${output_bytes:-}
        else
            client_present=0
            output_bytes=""
            pending_bytes=""
            previous_output=""
            stable_samples=0
        fi
        printf '%s,%s,%s,%s,%s\n' "$elapsed_ms" "$client_present" "$output_bytes" "$pending_bytes" "$stable_samples" >> "$samples_file"
        if ((client_present == 1 && pending_bytes > 0 && stable_samples >= BLOCKED_STALL_REQUIRED_SAMPLES)); then
            STALL_SETTLE_MS=$elapsed_ms
            STALL_STABLE_SAMPLES=$stable_samples
            STALL_OUTPUT_BYTES=$output_bytes
            STALL_PENDING_REPLY_BYTES=$pending_bytes
            return 0
        fi
        sleep "$BLOCKED_STALL_SAMPLE_INTERVAL_SECONDS"
    done
    return 1
}

run_blocked_writer_stage() {
    local stage=$1 stage_dir=$2 port=$3
    local config="$stage_dir/blocked-writer.conf"
    write_server_config "$config" "$port" "$stage_dir/blocked-writer-data" "${STAGE_THREADS[$stage]}" "${STAGE_ALWAYS[$stage]}"
    start_server "${STAGE_BINARY[$stage]}" "$config" "$stage_dir/blocked-writer-server.log" "$port"
    local server_pid=$STARTED_PID
    run_fixed_key_set "$port" "$BLOCKED_REPLY_BYTES" "$stage_dir/blocked-writer-setup.json" "$stage_dir/blocked-writer-setup.log"

    local ready_file="$stage_dir/blocked-writer.ready"
    local client_log="$stage_dir/blocked-writer-client.log"
    local client_hold_seconds=$((BLOCKED_STALL_DEADLINE_SECONDS + BLOCKED_INTERVAL_SECONDS + 2))
    quote_command taskset -c "$CLIENT_CPU" python3 -c '<named non-reading RESP client>' "$port" "$ready_file" "$client_hold_seconds"
    taskset -c "$CLIENT_CPU" python3 - "$port" "$ready_file" "$client_hold_seconds" > "$client_log" 2>&1 <<'PY' &
import pathlib
import socket
import sys
import time

port = int(sys.argv[1])
ready = pathlib.Path(sys.argv[2])
hold_seconds = float(sys.argv[3])
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(("127.0.0.1", port))
sock.sendall(b"*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$20\r\nblocked-writer-probe\r\n")
reply = b""
while not reply.endswith(b"\r\n"):
    chunk = sock.recv(16)
    if not chunk:
        raise RuntimeError("server closed while naming non-reading client")
    reply += chunk
if reply != b"+OK\r\n":
    raise RuntimeError(f"unexpected CLIENT SETNAME reply: {reply!r}")
sock.sendall(b"*2\r\n$3\r\nGET\r\n$18\r\nblocked-writer-key\r\n")
ready.write_text(str(sock.getsockname()[1]), encoding="utf-8")
time.sleep(hold_seconds)
sock.close()
PY
    local client_pid=$!
    track_pid "$client_pid"
    local attempt
    for attempt in $(seq 1 100); do
        [[ -s "$ready_file" ]] && break
        kill -0 "$client_pid" 2>/dev/null || break
        sleep 0.01
    done
    if [[ ! -s "$ready_file" ]]; then
        append_measurement "$stage" blocked_writer_cpu_percent 1 "" percent N/A "non-reading client did not become ready"
        kill -TERM "$client_pid" 2>/dev/null || true
        wait "$client_pid" 2>/dev/null || true
        untrack_pid "$client_pid"
        stop_server "$server_pid" "$port"
        return 1
    else
        local stall_samples="$stage_dir/blocked-writer-stall-samples.csv"
        local last_client_list="$stage_dir/blocked-writer-client-list-last.txt"
        local non_reader_port
        non_reader_port=$(<"$ready_file")
        if [[ ! "$non_reader_port" =~ ^[0-9]+$ ]]; then
            printf 'Invalid non-reading client port: %s\n' "$non_reader_port" >&2
            return 1
        fi
        if ! wait_for_blocked_writer_stall "$port" "$non_reader_port" "$client_pid" "$stall_samples" "$last_client_list"; then
            append_measurement "$stage" blocked_writer_settle_ms 1 "" milliseconds N/A "steady backpressure not reached within ${BLOCKED_STALL_DEADLINE_SECONDS}s"
            append_measurement "$stage" blocked_writer_stable_samples 1 "" samples N/A "requires ${BLOCKED_STALL_REQUIRED_SAMPLES} unchanged output samples with pending reply"
            append_measurement "$stage" blocked_writer_cpu_percent 1 "" percent N/A "CPU window was not started"
            kill -TERM "$client_pid" 2>/dev/null || true
            wait "$client_pid" 2>/dev/null || true
            untrack_pid "$client_pid"
            stop_server "$server_pid" "$port"
            return 1
        fi
        local ticks_start ticks_end ns_start ns_end ticks wall_seconds cpu_seconds cpu_percent clk_tck
        local ping_start_ns ping_end_ns ping_latency_ms ping_reply
        append_measurement "$stage" blocked_writer_settle_ms 1 "$STALL_SETTLE_MS" milliseconds ok "deadline=${BLOCKED_STALL_DEADLINE_SECONDS}s interval=${BLOCKED_STALL_SAMPLE_INTERVAL_SECONDS}s"
        append_measurement "$stage" blocked_writer_stable_samples 1 "$STALL_STABLE_SAMPLES" samples ok "output unchanged and pending reply present"
        append_measurement "$stage" blocked_writer_output_bytes 1 "$STALL_OUTPUT_BYTES" bytes ok "server TCP_INFO bytes_sent at stall"
        append_measurement "$stage" blocked_writer_pending_reply_bytes 1 "$STALL_PENDING_REPLY_BYTES" bytes ok "target CLIENT LIST omem at stall"
        clk_tck=$(getconf CLK_TCK)
        ticks_start=$(process_ticks "$server_pid")
        ns_start=$(date +%s%N)
        sleep "$BLOCKED_INTERVAL_SECONDS"
        ticks_end=$(process_ticks "$server_pid")
        ns_end=$(date +%s%N)
        ticks=$((ticks_end - ticks_start))
        wall_seconds=$(awk -v start="$ns_start" -v end="$ns_end" 'BEGIN { printf "%.6f", (end - start) / 1000000000 }')
        cpu_seconds=$(awk -v ticks="$ticks" -v hz="$clk_tck" 'BEGIN { printf "%.6f", ticks / hz }')
        cpu_percent=$(awk -v cpu="$cpu_seconds" -v wall="$wall_seconds" 'BEGIN { if (wall > 0) printf "%.3f", cpu * 100 / wall; else print "0" }')
        append_measurement "$stage" blocked_writer_cpu_ticks 1 "$ticks" ticks ok "CLK_TCK=$clk_tck"
        append_measurement "$stage" blocked_writer_cpu_seconds 1 "$cpu_seconds" seconds ok "wall_seconds=$wall_seconds"
        append_measurement "$stage" blocked_writer_cpu_percent 1 "$cpu_percent" percent ok "steady non-reading interval=$BLOCKED_INTERVAL_SECONDS seconds"
        ping_start_ns=$(date +%s%N)
        ping_reply=$(taskset -c "$AUX_CPU" redis-cli --raw -h 127.0.0.1 -p "$port" ping 2>&1 || true)
        ping_end_ns=$(date +%s%N)
        ping_latency_ms=$(awk -v start="$ping_start_ns" -v end="$ping_end_ns" 'BEGIN { printf "%.3f", (end - start) / 1000000 }')
        if [[ "$ping_reply" != PONG ]]; then
            append_measurement "$stage" blocked_writer_post_sample_ping_latency_ms 1 "" milliseconds N/A "post-sample PING failed: $ping_reply"
            kill -TERM "$client_pid" 2>/dev/null || true
            wait "$client_pid" 2>/dev/null || true
            untrack_pid "$client_pid"
            stop_server "$server_pid" "$port"
            return 1
        fi
        append_measurement "$stage" blocked_writer_post_sample_ping_latency_ms 1 "$ping_latency_ms" milliseconds ok "single PING after CPU window"
    fi
    kill -TERM "$client_pid" 2>/dev/null || true
    wait "$client_pid" 2>/dev/null || true
    untrack_pid "$client_pid"
    stop_server "$server_pid" "$port"
}

run_poll_wait_stage() {
    local stage=$1 stage_dir=$2 port=$3
    local config="$stage_dir/poll-wait.conf"
    write_server_config "$config" "$port" "$stage_dir/poll-wait-data" "${STAGE_THREADS[$stage]}" "${STAGE_ALWAYS[$stage]}"
    start_server "${STAGE_BINARY[$stage]}" "$config" "$stage_dir/poll-wait-server.log" "$port"
    local server_pid=$STARTED_PID
    local trace_file="$stage_dir/poll-wait.strace"
    local attach_log="$stage_dir/poll-wait-attach.log"
    local probe_log="$stage_dir/poll-wait-pwait2-probe.log"
    local trace_expression='epoll_wait,epoll_pwait,epoll_pwait2'
    local trace_note="$trace_expression"
    quote_command strace -e trace=epoll_pwait2 -o /dev/null true
    if ! strace -e trace=epoll_pwait2 -o /dev/null true > "$probe_log" 2>&1; then
        trace_expression='epoll_wait,epoll_pwait'
        trace_note='epoll_pwait2 unsupported by host strace 4.26; traced epoll_wait,epoll_pwait'
    fi
    local -a trace_command=(taskset -c "$AUX_CPU" strace -f -tt -T -e "trace=$trace_expression" -o "$trace_file" -p "$server_pid")
    quote_command "${trace_command[@]}"
    "${trace_command[@]}" > "$attach_log" 2>&1 &
    local strace_pid=$!
    track_pid "$strace_pid"
    sleep 0.25
    if ! kill -0 "$strace_pid" 2>/dev/null; then
        wait "$strace_pid" 2>/dev/null || true
        untrack_pid "$strace_pid"
        append_measurement "$stage" poll_wait_p50_us 1 "" microseconds N/A "$(tr '\n' ' ' < "$attach_log")"
        append_measurement "$stage" poll_wait_p99_us 1 "" microseconds N/A "see poll-wait-attach.log and commands.log"
    else
        run_benchmark "$port" "$POLL_REQUESTS" "$CLIENTS" "$PIPELINE_DEPTH" "$PAYLOAD_BYTES" "$KEYSPACE" "${stage}-poll-" "$stage_dir/poll-wait-load.json" "$stage_dir/poll-wait-load.log"
        kill -INT "$strace_pid" 2>/dev/null || true
        local detach_attempt
        for detach_attempt in $(seq 1 100); do
            kill -0 "$strace_pid" 2>/dev/null || break
            sleep 0.02
        done
        if kill -0 "$strace_pid" 2>/dev/null; then
            kill -TERM "$strace_pid" 2>/dev/null || true
        fi
        wait "$strace_pid" 2>/dev/null || true
        untrack_pid "$strace_pid"
        local durations="$stage_dir/poll-wait-durations-us.txt"
        sed -n 's/.*<\([0-9][0-9.]*\)>$/\1/p' "$trace_file" \
            | awk '{printf "%.3f\n", $1 * 1000000}' \
            | sort -n > "$durations"
        local count p50 p99
        count=$(wc -l < "$durations")
        if ((count == 0)); then
            append_measurement "$stage" poll_wait_p50_us 1 "" microseconds N/A "strace captured no completed epoll wait durations"
            append_measurement "$stage" poll_wait_p99_us 1 "" microseconds N/A "see poll-wait.strace"
        else
            p50=$(awk -v pct=50 -v count="$count" 'NR == int((count * pct + 99) / 100) {print; exit}' "$durations")
            p99=$(awk -v pct=99 -v count="$count" 'NR == int((count * pct + 99) / 100) {print; exit}' "$durations")
            append_measurement "$stage" poll_wait_samples 1 "$count" calls ok "$trace_note"
            append_measurement "$stage" poll_wait_p50_us 1 "$p50" microseconds ok "$trace_note"
            append_measurement "$stage" poll_wait_p99_us 1 "$p99" microseconds ok "$trace_note"
        fi
    fi
    stop_server "$server_pid" "$port"
}

run_queue_depth_stage() {
    local stage=$1 stage_dir=$2 port=$3
    local config="$stage_dir/queue-depth.conf"
    write_server_config "$config" "$port" "$stage_dir/queue-depth-data" "${STAGE_THREADS[$stage]}" "${STAGE_ALWAYS[$stage]}"
    start_server "${STAGE_BINARY[$stage]}" "$config" "$stage_dir/queue-depth-server.log" "$port"
    local server_pid=$STARTED_PID
    local samples="$stage_dir/queue-depth-samples.csv"
    local stop_file="$stage_dir/queue-depth.stop"
    printf '%s\n' 'timestamp_ns,reads_pending,writes_pending,polls_pending,available_queue_depth' > "$samples"
    (
        while [[ ! -e "$stop_file" ]]; do
            local_info=$(taskset -c "$AUX_CPU" redis-cli --raw -h 127.0.0.1 -p "$port" info all 2>/dev/null || true)
            reads=$(awk -F: '$1 == "io_threaded_reads_pending" {sub(/\r$/, "", $2); print $2; exit}' <<< "$local_info")
            writes=$(awk -F: '$1 == "io_threaded_writes_pending" {sub(/\r$/, "", $2); print $2; exit}' <<< "$local_info")
            polls=$(awk -F: '$1 == "io_threaded_polls_pending" {sub(/\r$/, "", $2); print $2; exit}' <<< "$local_info")
            reads=${reads:-0}
            writes=${writes:-0}
            polls=${polls:-0}
            printf '%s,%s,%s,%s,%s\n' "$(date +%s%N)" "$reads" "$writes" "$polls" "$((reads + writes + polls))" >> "$samples"
            sleep 0.02
        done
    ) &
    local sampler_pid=$!
    track_pid "$sampler_pid"
    run_benchmark "$port" "$QUEUE_REQUESTS" "$CLIENTS" "$PIPELINE_DEPTH" "$PAYLOAD_BYTES" "$KEYSPACE" "${stage}-queue-" "$stage_dir/queue-depth-load.json" "$stage_dir/queue-depth-load.log"
    : > "$stop_file"
    wait "$sampler_pid" 2>/dev/null || true
    untrack_pid "$sampler_pid"
    local reads_max writes_max polls_max queue_max
    reads_max=$(awk -F, 'NR > 1 && $2 > max {max=$2} END {print max+0}' "$samples")
    writes_max=$(awk -F, 'NR > 1 && $3 > max {max=$3} END {print max+0}' "$samples")
    polls_max=$(awk -F, 'NR > 1 && $4 > max {max=$4} END {print max+0}' "$samples")
    queue_max=$(awk -F, 'NR > 1 && $5 > max {max=$5} END {print max+0}' "$samples")
    append_measurement "$stage" worker_reads_pending_max 1 "$reads_max" jobs ok "INFO all samples"
    append_measurement "$stage" worker_writes_pending_max 1 "$writes_max" jobs ok "INFO all samples"
    append_measurement "$stage" worker_polls_pending_max 1 "$polls_max" jobs ok "missing field maps to zero"
    append_measurement "$stage" worker_available_queue_depth_max 1 "$queue_max" jobs ok "reads+writes+polls pending"
    if [[ "$stage" != D && "$queue_max" != 0 ]]; then
        append_measurement "$stage" worker_queue_expected_zero 1 "$queue_max" jobs warning "io-threads=1 should not enqueue worker jobs"
    fi
    stop_server "$server_pid" "$port"
}

wait_for_replication_link() {
    local replica_port=$1
    local attempt info
    for attempt in $(seq 1 600); do
        info=$(taskset -c "$AUX_CPU" redis-cli --raw -h 127.0.0.1 -p "$replica_port" info replication 2>/dev/null || true)
        if grep -qx $'master_link_status:up\r' <<< "$info" || grep -qx 'master_link_status:up' <<< "$info"; then
            return 0
        fi
        sleep 0.05
    done
    return 1
}

replication_offset() {
    local port=$1 field=$2
    taskset -c "$AUX_CPU" redis-cli --raw -h 127.0.0.1 -p "$port" info replication \
        | awk -F: -v wanted="$field" '$1 == wanted {sub(/\r$/, "", $2); print $2; exit}'
}

run_replication_stage() {
    local stage=$1 stage_dir=$2 primary_port=$3 replica_port=$4
    local primary_config="$stage_dir/replication-primary.conf"
    local replica_config="$stage_dir/replication-replica.conf"
    write_server_config "$primary_config" "$primary_port" "$stage_dir/replication-primary-data" "${STAGE_THREADS[$stage]}" "${STAGE_ALWAYS[$stage]}"
    write_replica_config "$replica_config" "$replica_port" "$stage_dir/replication-replica-data" "${STAGE_THREADS[$stage]}" "${STAGE_ALWAYS[$stage]}" "$primary_port"
    start_server "${STAGE_BINARY[$stage]}" "$primary_config" "$stage_dir/replication-primary.log" "$primary_port"
    local primary_pid=$STARTED_PID
    start_server "${STAGE_BINARY[$stage]}" "$replica_config" "$stage_dir/replication-replica.log" "$replica_port"
    local replica_pid=$STARTED_PID
    if ! wait_for_replication_link "$replica_port"; then
        append_measurement "$stage" replication_offset_gap 1 "" bytes N/A "replica link did not reach up within 30 seconds"
        stop_server "$replica_pid" "$replica_port"
        stop_server "$primary_pid" "$primary_port"
        return
    fi
    run_benchmark "$primary_port" "$REPLICATION_REQUESTS" "$CLIENTS" "$PIPELINE_DEPTH" "$PAYLOAD_BYTES" "$KEYSPACE" "${stage}-repl-" "$stage_dir/replication-load.json" "$stage_dir/replication-load.log"
    local start_ns end_ns primary_offset replica_offset gap attempt
    start_ns=$(date +%s%N)
    for attempt in $(seq 1 600); do
        primary_offset=$(replication_offset "$primary_port" master_repl_offset)
        replica_offset=$(replication_offset "$replica_port" slave_repl_offset)
        gap=$((primary_offset - replica_offset))
        ((gap == 0)) && break
        sleep 0.01
    done
    end_ns=$(date +%s%N)
    local catchup_ms
    catchup_ms=$(awk -v start="$start_ns" -v end="$end_ns" 'BEGIN { printf "%.3f", (end - start) / 1000000 }')
    append_measurement "$stage" replication_catchup_ms 1 "$catchup_ms" milliseconds ok "short write load completed before timer"
    local sample primary_info lag
    for sample in 1 2 3; do
        primary_offset=$(replication_offset "$primary_port" master_repl_offset)
        replica_offset=$(replication_offset "$replica_port" slave_repl_offset)
        gap=$((primary_offset - replica_offset))
        primary_info=$(taskset -c "$AUX_CPU" redis-cli --raw -h 127.0.0.1 -p "$primary_port" info replication)
        lag=$(awk -F'lag=' '/^slave[0-9]+:/ {split($2, fields, ","); sub(/\r$/, "", fields[1]); print fields[1]; exit}' <<< "$primary_info")
        append_measurement "$stage" replication_primary_offset "$sample" "$primary_offset" bytes ok "stable sample"
        append_measurement "$stage" replication_replica_offset "$sample" "$replica_offset" bytes ok "stable sample"
        append_measurement "$stage" replication_offset_gap "$sample" "$gap" bytes ok "primary minus replica"
        append_measurement "$stage" replication_reported_lag_ms "$sample" "${lag:-0}" milliseconds ok "primary INFO slave lag"
        sleep 0.2
    done
    stop_server "$replica_pid" "$replica_port"
    stop_server "$primary_pid" "$primary_port"
}

run_cluster_stage() {
    local stage=$1 stage_dir=$2 base_port=$3
    local round port_one port_two round_dir config_one config_two pid_one pid_two peer_id start_ns end_ns elapsed_ms attempt
    local candidate=$base_port
    for round in $(seq 1 "$CLUSTER_ROUNDS"); do
        local selected=no
        while ((candidate <= base_port + 28)); do
            port_one=$candidate
            port_two=$((candidate + 1))
            candidate=$((candidate + 2))
            if port_has_tcp_state "$port_one" || port_has_tcp_state "$port_two"; then
                continue
            fi
            if port_has_tcp_state "$((port_one + 10000))" || port_has_tcp_state "$((port_two + 10000))"; then
                continue
            fi
            selected=yes
            break
        done
        if [[ "$selected" != yes ]]; then
            printf 'No free Cluster port pair remains for stage %s round %s in %s-%s\n' "$stage" "$round" "$base_port" "$((base_port + 29))" >&2
            exit 64
        fi
        assert_cluster_bus_port_free "$((port_one + 10000))"
        assert_cluster_bus_port_free "$((port_two + 10000))"
        round_dir="$stage_dir/cluster-round-$round"
        mkdir -p "$round_dir"
        config_one="$round_dir/node-one.conf"
        config_two="$round_dir/node-two.conf"
        write_cluster_config "$config_one" "$port_one" "$round_dir/node-one-data" "${STAGE_THREADS[$stage]}" "${STAGE_ALWAYS[$stage]}"
        write_cluster_config "$config_two" "$port_two" "$round_dir/node-two-data" "${STAGE_THREADS[$stage]}" "${STAGE_ALWAYS[$stage]}"
        start_server "${STAGE_BINARY[$stage]}" "$config_one" "$round_dir/node-one.log" "$port_one"
        pid_one=$STARTED_PID
        start_server "${STAGE_BINARY[$stage]}" "$config_two" "$round_dir/node-two.log" "$port_two"
        pid_two=$STARTED_PID
        peer_id=$(taskset -c "$AUX_CPU" redis-cli --raw -h 127.0.0.1 -p "$port_two" cluster myid | tr -d '\r')
        start_ns=$(date +%s%N)
        taskset -c "$AUX_CPU" redis-cli --raw -h 127.0.0.1 -p "$port_one" cluster meet 127.0.0.1 "$port_two" > "$round_dir/meet.out"
        local found=no
        for attempt in $(seq 1 1000); do
            if taskset -c "$AUX_CPU" redis-cli --raw -h 127.0.0.1 -p "$port_one" cluster nodes 2>/dev/null | grep -Fq "$peer_id"; then
                found=yes
                break
            fi
            sleep 0.01
        done
        end_ns=$(date +%s%N)
        if [[ "$found" == yes ]]; then
            elapsed_ms=$(awk -v start="$start_ns" -v end="$end_ns" 'BEGIN { printf "%.3f", (end - start) / 1000000 }')
            append_measurement "$stage" cluster_bus_meet_latency_ms "$round" "$elapsed_ms" milliseconds ok "peer ID appeared in CLUSTER NODES"
        else
            append_measurement "$stage" cluster_bus_meet_latency_ms "$round" "" milliseconds N/A "peer ID did not appear within 10 seconds"
        fi
        stop_server "$pid_two" "$port_two"
        stop_server "$pid_one" "$port_one"
    done
}

write_environment() {
    local output="$OUTPUT_DIR/environment.txt"
    {
        printf 'mode=%s\n' "$MODE"
        printf 'utc_started=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        printf 'repo=%s\n' "$REPO_ROOT"
        printf 'head=%s\n' "$(git -C "$REPO_ROOT" rev-parse HEAD)"
        printf 'parent_commit=%s\n' "$PARENT_COMMIT"
        printf 'worktree_snapshot_digest=%s\n' "$WORKTREE_SNAPSHOT_DIGEST"
        printf 'cargo_lock_sha256=%s\n' "$LOCK_DIGEST"
        printf 'live_superproject_manifest_sha256=%s\n' "$LIVE_SUPERPROJECT_MANIFEST_SHA256"
        printf 'live_superproject_path_list_sha256=%s\n' "$LIVE_SUPERPROJECT_PATH_LIST_SHA256"
        printf 'live_superproject_build_input_count=%s\n' "$LIVE_SUPERPROJECT_BUILD_INPUT_COUNT"
        printf 'parent_gitlink_sha=%s\n' "$PARENT_GITLINK_SHA"
        printf 'parent_gitlink_tree=%s\n' "$PARENT_GITLINK_TREE"
        printf 'parent_submodule_archive_sha256=%s\n' "$PARENT_SUBMODULE_ARCHIVE_SHA256"
        printf 'parent_submodule_snapshot_sha256=%s\n' "$A_SUBMODULE_SNAPSHOT_SHA256"
        printf 'current_gitlink_sha=%s\n' "$CURRENT_GITLINK_SHA"
        printf 'current_gitlink_tree=%s\n' "$CURRENT_GITLINK_TREE"
        printf 'current_submodule_archive_sha256=%s\n' "$CURRENT_SUBMODULE_ARCHIVE_SHA256"
        printf 'current_submodule_snapshot_sha256=%s\n' "$CURRENT_SUBMODULE_SNAPSHOT_SHA256"
        printf 'gitlinks_match=%s\n' "$([[ "$PARENT_GITLINK_SHA" == "$CURRENT_GITLINK_SHA" ]] && printf yes || printf no)"
        printf 'stage_a_source_digest=%s\n' "$A_SOURCE_DIGEST"
        printf 'stage_b_source_digest=%s\n' "$B_SOURCE_DIGEST"
        printf 'stage_c_source_digest=%s\n' "$CURRENT_SOURCE_DIGEST"
        printf 'stage_d_source_digest=%s\n' "$CURRENT_SOURCE_DIGEST"
        printf 'stage_a_superproject_manifest_sha256=%s\n' "$A_SUPERPROJECT_MANIFEST_SHA256"
        printf 'stage_b_superproject_manifest_sha256=%s\n' "$EDGE_PRE_PATCH_MANIFEST_SHA256"
        printf 'stage_b_post_edge_manifest_sha256=%s\n' "$EDGE_POST_PATCH_MANIFEST_SHA256"
        printf 'stage_c_superproject_manifest_sha256=%s\n' "$CURRENT_SUPERPROJECT_MANIFEST_SHA256"
        printf 'stage_d_superproject_manifest_sha256=%s\n' "$CURRENT_SUPERPROJECT_MANIFEST_SHA256"
        printf 'submodule_snapshot_kind=git-archive-without-git-metadata\n'
        printf 'server_cpus=%s\n' "$SERVER_CPUS"
        printf 'client_cpu=%s\n' "$CLIENT_CPU"
        printf 'aux_cpu=%s\n' "$AUX_CPU"
        printf 'port_range=%s-%s\n' "$PORT_MIN" "$PORT_MAX"
        printf 'cluster_bus_port_range=%s-%s\n' "$CLUSTER_BUS_PORT_MIN" "$CLUSTER_BUS_PORT_MAX"
        printf 'clients=%s\n' "$CLIENTS"
        printf 'payload_bytes=%s\n' "$PAYLOAD_BYTES"
        printf 'keyspace=%s\n' "$KEYSPACE"
        printf 'single_requests=%s\n' "$SINGLE_REQUESTS"
        printf 'pipeline_requests=%s\n' "$PIPELINE_REQUESTS"
        printf 'pipeline_depth=%s\n' "$PIPELINE_DEPTH"
        printf '\n[uname]\n'
        uname -a
        printf '\n[lscpu]\n'
        lscpu
        printf '\n[rustc]\n'
        rustc -Vv
        printf '\n[cargo]\n'
        cargo -V
        printf '\n[git status]\n'
        git -C "$REPO_ROOT" status --short --branch
        printf '\n[strace]\n'
        strace -V 2>&1 | head -n 2
        printf '\n[clock ticks]\n'
        getconf CLK_TCK
    } > "$output"
}

generate_summary() {
    python3 - "$STAGES_CSV" "$THROUGHPUT_CSV" "$MEASUREMENTS_CSV" "$OUTPUT_DIR/results.json" "$OUTPUT_DIR/summary.csv" "$OUTPUT_DIR/summary.md" "$MODE" <<'PY'
import csv
import json
import math
import pathlib
import statistics
import sys

stages_path, throughput_path, measurements_path, json_path, csv_path, markdown_path, mode = sys.argv[1:]

with open(stages_path, newline="", encoding="utf-8") as handle:
    stages = list(csv.DictReader(handle))
with open(throughput_path, newline="", encoding="utf-8") as handle:
    throughput_rows = list(csv.DictReader(handle))
with open(measurements_path, newline="", encoding="utf-8") as handle:
    measurement_rows = list(csv.DictReader(handle))

def numbers(rows, field="value"):
    values = []
    for row in rows:
        value = row.get(field, "")
        if value == "":
            continue
        try:
            values.append(float(value))
        except ValueError:
            continue
    return values

def median(values):
    return statistics.median(values) if values else None

def maximum(values):
    return max(values) if values else None

def metric_rows(stage, metric):
    return [row for row in measurement_rows if row["stage"] == stage and row["metric"] == metric]

def metric_value(stage, metric, reducer=median):
    return reducer(numbers(metric_rows(stage, metric)))

def display(value, digits=3):
    if value is None:
        return "N/A"
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.{digits}f}"

summaries = []
for stage_info in stages:
    stage = stage_info["stage"]
    item = {"stage": stage, **stage_info}
    for workload in ("single", "pipeline"):
        rows = [row for row in throughput_rows if row["stage"] == stage and row["workload"] == workload]
        item[workload] = {
            "runs": len(rows),
            "rps_median": median(numbers(rows, "rps")),
            "p50_us_median": median(numbers(rows, "p50_us")),
            "p99_us_median": median(numbers(rows, "p99_us")),
            "eventloop_avg_us_median": median(numbers(rows, "eventloop_avg_us")),
            "eventloop_max_us": maximum(numbers(rows, "eventloop_max_us")),
            "eventloop_instantaneous_us_median": median(numbers(rows, "eventloop_instantaneous_us")),
        }
    poll_rows = metric_rows(stage, "poll_wait_p50_us") + metric_rows(stage, "poll_wait_p99_us")
    poll_status = "ok" if poll_rows and all(row["status"] == "ok" for row in poll_rows) else "N/A"
    poll_detail = "; ".join(row["detail"] for row in poll_rows if row["status"] != "ok")
    item["diagnostics"] = {
        "blocked_writer_settle_ms": metric_value(stage, "blocked_writer_settle_ms"),
        "blocked_writer_stable_samples": metric_value(stage, "blocked_writer_stable_samples"),
        "blocked_writer_output_bytes": metric_value(stage, "blocked_writer_output_bytes"),
        "blocked_writer_pending_reply_bytes": metric_value(stage, "blocked_writer_pending_reply_bytes"),
        "blocked_writer_cpu_ticks": metric_value(stage, "blocked_writer_cpu_ticks"),
        "blocked_writer_cpu_seconds": metric_value(stage, "blocked_writer_cpu_seconds"),
        "blocked_writer_cpu_percent": metric_value(stage, "blocked_writer_cpu_percent"),
        "blocked_writer_post_sample_ping_latency_ms": metric_value(stage, "blocked_writer_post_sample_ping_latency_ms"),
        "poll_wait_status": poll_status,
        "poll_wait_p50_us": metric_value(stage, "poll_wait_p50_us"),
        "poll_wait_p99_us": metric_value(stage, "poll_wait_p99_us"),
        "poll_wait_samples": metric_value(stage, "poll_wait_samples"),
        "poll_wait_detail": poll_detail,
        "worker_reads_pending_max": metric_value(stage, "worker_reads_pending_max", maximum),
        "worker_writes_pending_max": metric_value(stage, "worker_writes_pending_max", maximum),
        "worker_polls_pending_max": metric_value(stage, "worker_polls_pending_max", maximum),
        "worker_available_queue_depth_max": metric_value(stage, "worker_available_queue_depth_max", maximum),
        "replication_catchup_ms": metric_value(stage, "replication_catchup_ms"),
        "replication_offset_gap_max": metric_value(stage, "replication_offset_gap", maximum),
        "replication_reported_lag_ms_median": metric_value(stage, "replication_reported_lag_ms"),
        "cluster_bus_meet_latency_ms_median": metric_value(stage, "cluster_bus_meet_latency_ms"),
        "cluster_bus_rounds": len(numbers(metric_rows(stage, "cluster_bus_meet_latency_ms"))),
    }
    summaries.append(item)

result = {
    "mode": mode,
    "disclaimer": "Local microbenchmark only; these results are not a CI performance commitment.",
    "source_identities": [
        {
            key: item[key]
            for key in (
                "stage",
                "source_commit",
                "superproject_digest",
                "gitlink_sha",
                "gitlink_tree",
                "submodule_archive_sha256",
                "submodule_snapshot_sha256",
                "superproject_manifest_sha256",
                "post_edge_manifest_sha256",
                "source_digest",
            )
        }
        for item in summaries
    ],
    "stages": summaries,
}
pathlib.Path(json_path).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")

columns = [
    "stage", "label", "single_rps_median", "single_p50_us_median", "single_p99_us_median",
    "pipeline_rps_median", "pipeline_p50_us_median", "pipeline_p99_us_median",
    "eventloop_avg_us_median", "eventloop_max_us", "eventloop_instantaneous_us_median",
    "blocked_writer_settle_ms", "blocked_writer_stable_samples", "blocked_writer_output_bytes",
    "blocked_writer_pending_reply_bytes", "blocked_writer_cpu_percent",
    "blocked_writer_post_sample_ping_latency_ms", "poll_wait_p50_us", "poll_wait_p99_us",
    "worker_queue_depth_max", "replication_offset_gap_max", "replication_lag_ms_median",
    "cluster_bus_latency_ms_median",
]
with open(csv_path, "w", newline="", encoding="utf-8") as handle:
    writer = csv.DictWriter(handle, fieldnames=columns)
    writer.writeheader()
    for item in summaries:
        diag = item["diagnostics"]
        writer.writerow({
            "stage": item["stage"],
            "label": item["label"],
            "single_rps_median": item["single"]["rps_median"],
            "single_p50_us_median": item["single"]["p50_us_median"],
            "single_p99_us_median": item["single"]["p99_us_median"],
            "pipeline_rps_median": item["pipeline"]["rps_median"],
            "pipeline_p50_us_median": item["pipeline"]["p50_us_median"],
            "pipeline_p99_us_median": item["pipeline"]["p99_us_median"],
            "eventloop_avg_us_median": item["pipeline"]["eventloop_avg_us_median"],
            "eventloop_max_us": item["pipeline"]["eventloop_max_us"],
            "eventloop_instantaneous_us_median": item["pipeline"]["eventloop_instantaneous_us_median"],
            "blocked_writer_settle_ms": diag["blocked_writer_settle_ms"],
            "blocked_writer_stable_samples": diag["blocked_writer_stable_samples"],
            "blocked_writer_output_bytes": diag["blocked_writer_output_bytes"],
            "blocked_writer_pending_reply_bytes": diag["blocked_writer_pending_reply_bytes"],
            "blocked_writer_cpu_percent": diag["blocked_writer_cpu_percent"],
            "blocked_writer_post_sample_ping_latency_ms": diag["blocked_writer_post_sample_ping_latency_ms"],
            "poll_wait_p50_us": diag["poll_wait_p50_us"],
            "poll_wait_p99_us": diag["poll_wait_p99_us"],
            "worker_queue_depth_max": diag["worker_available_queue_depth_max"],
            "replication_offset_gap_max": diag["replication_offset_gap_max"],
            "replication_lag_ms_median": diag["replication_reported_lag_ms_median"],
            "cluster_bus_latency_ms_median": diag["cluster_bus_meet_latency_ms_median"],
        })

lines = [
    f"# Polling Level Comparison ({mode})",
    "",
    "Local microbenchmark only; these results are not a CI performance commitment.",
    "",
    "| Stage | Single RPS | Single p50/p99 us | Pipeline RPS | Pipeline p50/p99 us | Event loop avg/max/instant us | Blocked CPU % | Poll wait p50/p99 us | Queue max | Repl gap/lag ms | Cluster meet ms |",
    "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
]
for item in summaries:
    single = item["single"]
    pipeline = item["pipeline"]
    diag = item["diagnostics"]
    lines.append(
        f"| {item['stage']} {item['label']} | {display(single['rps_median'])} | "
        f"{display(single['p50_us_median'])}/{display(single['p99_us_median'])} | "
        f"{display(pipeline['rps_median'])} | {display(pipeline['p50_us_median'])}/{display(pipeline['p99_us_median'])} | "
        f"{display(pipeline['eventloop_avg_us_median'])}/{display(pipeline['eventloop_max_us'])}/{display(pipeline['eventloop_instantaneous_us_median'])} | "
        f"{display(diag['blocked_writer_cpu_percent'])} | {display(diag['poll_wait_p50_us'])}/{display(diag['poll_wait_p99_us'])} | "
        f"{display(diag['worker_available_queue_depth_max'])} | {display(diag['replication_offset_gap_max'])}/{display(diag['replication_reported_lag_ms_median'])} | "
        f"{display(diag['cluster_bus_meet_latency_ms_median'])} |"
    )
limitations = []
for item in summaries:
    diag = item["diagnostics"]
    if diag["poll_wait_status"] != "ok":
        limitations.append(f"- Stage {item['stage']} poll wait latency is N/A: {diag['poll_wait_detail'] or 'no completed wait samples'}")
if not limitations:
    limitations.append("- Poll wait diagnostics completed for all stages.")
lines.extend([
    "",
    "## Notes",
    "",
    f"- Throughput and latency use the median of {len([r for r in throughput_rows if r['stage'] == 'A' and r['workload'] == 'single'])} run(s) per workload and stage.",
    "- Event-loop average is the INFO eventloop duration delta divided by the cycle delta. Max and instantaneous values come from INFO.",
    "- Blocked-writer CPU starts only after five unchanged server TCP_INFO bytes_sent samples with the named client omem still pending; one PING is measured after the CPU window.",
    "- Worker queue depth is the sampled sum of pending reads, writes, and available poll jobs.",
    "- Replication offsets and lag use three stable INFO samples after catch-up.",
    "- Poll wait, queue, blocked-writer, replication, and Cluster Bus measurements are isolated diagnostics, not simultaneous with throughput runs.",
    *limitations,
    "",
])
pathlib.Path(markdown_path).write_text("\n".join(lines), encoding="utf-8")
PY
}

write_environment

stages=(A B C D)
for stage_index in "${!stages[@]}"; do
    stage=${stages[$stage_index]}
    stage_dir="$OUTPUT_DIR/stage-$stage"
    mkdir -p "$stage_dir"
    base_port=$((PORT_MIN + stage_index * 40))
    printf 'Running stage %s: %s\n' "$stage" "${STAGE_LABEL[$stage]}"
    run_throughput_stage "$stage" "$stage_dir" "$base_port"
    run_blocked_writer_stage "$stage" "$stage_dir" "$((base_port + 1))"
    run_poll_wait_stage "$stage" "$stage_dir" "$((base_port + 2))"
    run_queue_depth_stage "$stage" "$stage_dir" "$((base_port + 3))"
    run_replication_stage "$stage" "$stage_dir" "$((base_port + 4))" "$((base_port + 5))"
    run_cluster_stage "$stage" "$stage_dir" "$((base_port + 10))"
done

git -C "$SUBMODULE_PATH" status --porcelain=v1 --untracked-files=all > "$SUBMODULE_STATUS_AFTER"
if ! cmp -s "$SUBMODULE_STATUS_BEFORE" "$SUBMODULE_STATUS_AFTER"; then
    printf 'NEEDS_CONTEXT: thirdparty/valkey status changed during the benchmark.\n' >&2
    diff -u "$SUBMODULE_STATUS_BEFORE" "$SUBMODULE_STATUS_AFTER" >&2 || true
    exit 78
fi

generate_summary
printf 'Artifacts: %s\n' "$OUTPUT_DIR"
printf 'Summary: %s\n' "$OUTPUT_DIR/summary.md"
