#!/usr/bin/env python3
"""Unified repository tooling for VersalKV.

This file intentionally contains both production commands and their hermetic
self-tests.  Keep it compatible with Python 3.7 and the standard library.
"""

import argparse
from collections import Counter, namedtuple
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from typing import Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_USAGE = 2
SCRIPT_PATH = Path(__file__).resolve()
DEFAULT_REPO_ROOT = SCRIPT_PATH.parent.parent
VERSAL_BINARIES = ("server", "cli", "benchmark", "check-aof", "check-rdb", "sentinel")


class ToolError(Exception):
    def __init__(self, message: str, code: int = EXIT_FAILURE) -> None:
        super().__init__(message)
        self.message = message
        self.code = code


class SignalExit(Exception):
    def __init__(self, code: int) -> None:
        super().__init__(code)
        self.code = code


class RepoContext:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None) -> "RepoContext":
        values = os.environ if env is None else env
        root = values.get("VERSAL_REPO_ROOT")
        return cls(Path(root) if root else DEFAULT_REPO_ROOT)


def run_command(
    argv: Sequence[str],
    cwd: Optional[Path] = None,
    env: Optional[Mapping[str, str]] = None,
    capture: bool = False,
    check: bool = False,
) -> subprocess.CompletedProcess:
    kwargs = {
        "cwd": str(cwd) if cwd is not None else None,
        "env": dict(env) if env is not None else None,
        "check": check,
    }
    if capture:
        kwargs.update({"stdout": subprocess.PIPE, "stderr": subprocess.PIPE, "universal_newlines": True})
    return subprocess.run(list(argv), **kwargs)


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def invoke_cli(arguments: Sequence[str], root: Path) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    env["VERSAL_REPO_ROOT"] = str(root)
    return run_command([sys.executable, str(SCRIPT_PATH)] + list(arguments), env=env, capture=True)


def write_fixture(root: Path, relative: str, content: str) -> Path:
    destination = root / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(content, encoding="utf-8")
    return destination


def write_executable(root: Path, relative: str, content: str) -> Path:
    path = write_fixture(root, relative, content)
    path.chmod(0o755)
    return path


def command_root(ctx: RepoContext, environment_name: str) -> Path:
    override = os.environ.get(environment_name)
    return Path(override).resolve() if override else ctx.root


def iter_files(root: Path, suffix: str) -> Iterable[Path]:
    if not root.exists():
        return []
    return sorted((path for path in root.rglob("*" ) if path.is_file() and path.name.endswith(suffix)), key=lambda item: item.as_posix())


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def rust_code_lines(text: str) -> List[str]:
    """Remove Rust comments and literals while retaining source line numbers."""
    output = []  # type: List[str]
    current = []  # type: List[str]
    index = 0
    block_depth = 0
    in_string = False
    raw_hashes = -1
    escaped = False
    while index < len(text):
        char = text[index]
        next_char = text[index + 1] if index + 1 < len(text) else ""
        if char == "\n":
            output.append("".join(current))
            current = []
            index += 1
            escaped = False
            continue
        if block_depth:
            if char == "/" and next_char == "*":
                block_depth += 1
                index += 2
            elif char == "*" and next_char == "/":
                block_depth -= 1
                index += 2
            else:
                index += 1
            continue
        if raw_hashes >= 0:
            if char == '"' and text[index + 1:index + 1 + raw_hashes] == "#" * raw_hashes:
                index += raw_hashes + 1
                raw_hashes = -1
            else:
                index += 1
            continue
        if in_string:
            delimiter = '"'
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == delimiter:
                in_string = False
            index += 1
            continue
        if char == "/" and next_char == "*":
            block_depth = 1
            index += 2
            continue
        if char == "/" and next_char == "/":
            while index < len(text) and text[index] != "\n":
                index += 1
            continue
        raw = re.match(r"(?:b)?r(#+)?\"", text[index:])
        if raw:
            raw_hashes = len(raw.group(1) or "")
            index += len(raw.group(0))
            continue
        if char == '"':
            in_string = True
            index += 1
            continue
        if char == "'":
            char_literal = re.match(r"'(?:\\.|[^\\'])'", text[index:])
            if char_literal:
                index += len(char_literal.group(0))
                continue
        current.append(char)
        index += 1
    output.append("".join(current))
    return output


def command_check_boundaries(_args: argparse.Namespace, ctx: RepoContext) -> int:
    root = command_root(ctx, "ARCH_ROOT_DIR")
    src = root / "src"
    violations = []  # type: List[str]
    layers = (
        ("foundation", "Foundation", re.compile(r"crate::(?:engine|framework|runtime|config|server)::")),
        ("engine", "L2 Engine", re.compile(r"crate::(?:framework|runtime|config|server)::")),
        ("framework", "L3 Framework", re.compile(r"crate::(?:runtime|config|server)::")),
    )
    for directory, label, pattern in layers:
        for source in iter_files(src / directory, ".rs"):
            matches = []
            for number, line in enumerate(rust_code_lines(read_text(source)), 1):
                if pattern.search(line):
                    matches.append("{}:{}".format(number, line))
            if matches:
                relative = source.relative_to(root).as_posix()
                violations.append("❌ {} {}\n    {}".format(label, relative, "\n    ".join(matches)))
    polling_owners = {
        (src / "foundation/io/transport/polling.rs").resolve(),
        (src / "foundation/io/sys/polling.rs").resolve(),
    }
    polling_pattern = re.compile(r"(?:^|[^A-Za-z0-9_:])(?:::)?\s*polling\s*::")
    for source in iter_files(src, ".rs"):
        if source.resolve() in polling_owners:
            continue
        matches = []
        for number, line in enumerate(rust_code_lines(read_text(source)), 1):
            if polling_pattern.search(line):
                matches.append("{}:{}".format(number, line))
        if matches:
            relative = source.relative_to(root).as_posix()
            violations.append("❌ Polling backend {}\n    {}".format(relative, "\n    ".join(matches)))

    print("──  ────────────────────────────\n")
    if not violations:
        print("✅ ")
        return EXIT_OK
    print("\n".join(violations))
    print("\n⚠️   {} ".format(len(violations)))
    return EXIT_FAILURE


def command_check_directory_layout(_args: argparse.Namespace, ctx: RepoContext) -> int:
    root = command_root(ctx, "DIRECTORY_LAYOUT_ROOT")
    src = root / "src"
    failures = []  # type: List[str]
    required_dirs = (
        "foundation/io/persistence", "foundation/io/store/structure", "engine/command",
        "engine/state", "engine/persistence", "engine/extension", "framework/config",
        "framework/capabilities", "framework/generated/commands", "framework/observability",
        "framework/distributed/migration", "runtime/driver", "runtime/embedded",
        "runtime/server/supervisor/shutdown", "runtime/server/worker/event_loop",
        "runtime/server/worker/event_loop/io_workers", "runtime/server/worker/event_loop/timer",
        "runtime/server/worker/event_loop/transport", "runtime/server/worker/connection",
        "runtime/server/worker/command", "runtime/server/worker/command/blocking",
        "runtime/server/worker/command/dispatch", "runtime/server/worker/command/transaction",
        "runtime/server/worker/maintenance", "runtime/server/worker/hosted",
        "runtime/server/worker/cluster/migration", "runtime/server/worker/cluster/bus",
        "runtime/server/worker/cluster/pubsub", "runtime/server/worker/replication", "server",
    )
    required_files = (
        "bin/server.rs", "framework/generated/mod.rs", "framework/generated/commands/mod.rs",
        "framework/generated/commands/catalog.rs", "runtime/server/message.rs",
        "runtime/server/shared.rs", "runtime/server/worker/runtime.rs",
        "runtime/server/worker/state.rs", "runtime/server/worker/cluster/bus.rs",
        "runtime/server/worker/cluster/pubsub.rs", "runtime/server/worker/cluster/propagation.rs",
        "runtime/server/worker/replication.rs",
    )
    retired = (
        "main.rs", "config", "event_loop", "engine/data", "engine/request", "engine/persist",
        "engine/extend", "engine/telemetry", "foundation/io/persist", "foundation/io/store/ds",
        "framework/runtime_io", "framework/telemetry_services", "framework/extras",
        "framework/distributed/migrate", "framework/distributed/slot_migration",
        "framework/generated/commands.rs", "runtime/core", "runtime/run", "runtime/command",
        "runtime/connection", "runtime/tasks", "runtime/extension", "runtime/io_threads",
        "runtime/scheduling", "runtime/telemetry", "runtime/event_loop", "runtime/request",
        "runtime/clients", "runtime/background", "runtime/blocked_clients",
        "runtime/host_tasks", "runtime/shutdown", "runtime/io_workers", "runtime/timers",
        "runtime/transport", "runtime/distributed", "runtime/state.rs",
        "runtime/server/worker/reactor", "runtime/server/worker/flows",
        "runtime/server/worker/sessions", "runtime/server/worker/tasks",
        "runtime/server/worker/distributed",
    )
    for relative in required_dirs:
        if not (src / relative).is_dir():
            failures.append("directory-layout: missing src/{}".format(relative))
    for relative in required_files:
        if not (src / relative).is_file():
            failures.append("directory-layout: missing src/{}".format(relative))
    for relative in retired:
        if os.path.lexists(str(src / relative)):
            failures.append("directory-layout: retired path still exists: src/{}".format(relative))

    cargo = read_text(root / "Cargo.toml") if (root / "Cargo.toml").is_file() else ""
    if not re.search(r'^name = "versal"$', cargo, re.MULTILINE):
        failures.append("directory-layout: Cargo package must be versal")
    for binary in ("server", "cli", "benchmark", "check-aof", "check-rdb", "sentinel"):
        if 'name = "versal-{}"'.format(binary) not in cargo.splitlines():
            failures.append("directory-layout: tool binary must be versal-{}".format(binary))
    if re.search(r'^name = "valkey-(?:server|cli|benchmark|check-aof|check-rdb|sentinel)"$', cargo, re.MULTILINE):
        failures.append("directory-layout: retired valkey-* tool binary remains in Cargo.toml")
    if 'path = "src/bin/server.rs"' not in cargo.splitlines():
        failures.append("directory-layout: main binary path must be src/bin/server.rs")
    readme = root / "README.md"
    if not readme.is_file() or not read_text(readme).startswith("# VersalKV\n"):
        failures.append("directory-layout: README project title must be VersalKV")

    scan_groups = (
        (re.compile(r"valkey-server-rust|rust-valkey-server|rust-main|runtest-rust|placeholderkv-"), (src, root / "tests")),
        (re.compile(r"valkey-server-rust|runtime_io|telemetry_services|engine/(?:request|data|persist|extend|telemetry)/|foundation/io/persist/|foundation/io/store/ds/|framework/extras/|framework/distributed/(?:migrate|slot_migration)/|(?:^|[^A-Za-z0-9_])L0(?:[^A-Za-z0-9_]|$)"), (root / "docs/architecture.md", root / "docs/design-doc.md", root / "docs/specs")),
        (re.compile(r"crate::event_loop|framework/runtime_io|crate::framework::runtime_io|telemetry_services/|engine/(?:request|data|persist|extend|telemetry)/|foundation/io/persist/|foundation/io/store/ds/|framework/extras/|framework/distributed/(?:migrate|slot_migration)/|(?:^|[^A-Za-z0-9_])L0(?:[^A-Za-z0-9_]|$)"), (src,)),
    )
    for pattern, candidates in scan_groups:
        for candidate in candidates:
            files = [candidate] if candidate.is_file() else list(iter_files(candidate, ""))
            for source in files:
                for number, line in enumerate(read_text(source).splitlines(), 1):
                    if pattern.search(line):
                        failures.append("{}:{}:{}".format(source, number, line))
    for failure in failures:
        print(failure, file=sys.stderr)
    return EXIT_FAILURE if failures else EXIT_OK


def _compiled_rust_sources(root: Path) -> Tuple[set, bool]:
    dep_dir = root / "target" / "debug" / "deps"
    compiled = set()
    dep_files = list(dep_dir.glob("*.d")) if dep_dir.is_dir() else []
    for dep_file in dep_files:
        for token in dep_file.read_text(encoding="utf-8", errors="ignore").replace("\\\n", " ").split():
            token = token.replace("\\ ", " ")
            if not token.endswith(".rs"):
                continue
            path = Path(token)
            if not path.is_absolute():
                path = root / path
            try:
                compiled.add(path.resolve().relative_to(root.resolve()).as_posix())
            except ValueError:
                continue
    return compiled, bool(dep_files)


def command_check_module_layout(_args: argparse.Namespace, ctx: RepoContext) -> int:
    root = command_root(ctx, "ARCH_ROOT_DIR")
    src = root / "src"
    failures = []  # type: List[str]

    compiled, has_dep_info = _compiled_rust_sources(root)
    tracked = [path for path in run_command(["git", "ls-files", "src"], cwd=root, capture=True).stdout.splitlines() if path.endswith(".rs")]
    for relative in tracked:
        if not (root / relative).is_file():
            continue
        if "/tests/" in relative or relative.endswith("_tests.rs") or "/generated/" in relative:
            continue
        if not has_dep_info or relative not in compiled:
            failures.append("module-layout: uncompiled source: {}".format(relative))

    public_types = {}  # type: Dict[str, List[str]]
    for source in iter_files(src, ".rs"):
        relative = source.relative_to(root).as_posix()
        if relative.startswith("src/bin/"):
            continue
        content = read_text(source)
        if relative.startswith("src/framework/context/"):
            for number, line in enumerate(content.splitlines(), 1):
                if re.search(r"^\s*pub\s+fn\s+\w+_mut\s*\(", line):
                    failures.append(
                        "module-layout: public mutable context accessor at {}:{}: {}".format(
                            relative, number, line.strip()
                        )
                    )
        for match in re.finditer(r"^\s*pub\s+(?:struct|enum|trait)\s+([A-Za-z_][A-Za-z0-9_]*)", content, re.MULTILINE):
            public_types.setdefault(match.group(1), []).append(relative)
        for match in re.finditer(r"#\[path\s*=\s*\"([^\"]+)\"\]", content):
            target = (source.parent / match.group(1)).resolve()
            try:
                target.relative_to(source.parent.resolve())
            except ValueError:
                failures.append("module-layout: #[path] escapes parent directory: {} -> {}".format(relative, match.group(1)))
        if re.search(r"(?:^|/)[^/]*(?:_s[0-9]+|_v[0-9]+)(?:\.rs)?$", relative):
            failures.append("module-layout: stage/version file name: {}".format(relative))
        for number, line in enumerate(content.splitlines(), 1):
            if re.search(r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:fn|struct|enum|trait|mod|const|static)\s+[A-Za-z_][A-Za-z0-9_]*(?:_s[0-9]+|_v[0-9]+)\b", line):
                failures.append("module-layout: stage/version symbol at {}:{}: {}".format(relative, number, line.strip()))

    for name, locations in sorted(public_types.items()):
        if len(locations) > 1:
            failures.append("module-layout: same public type name with multiple definitions: {} ({})".format(name, ", ".join(sorted(locations))))

    if failures:
        for failure in failures:
            print(failure, file=sys.stderr)
        return EXIT_FAILURE
    print("✅ module layout checks passed.")
    return EXIT_OK


def command_check_file_sizes(_args: argparse.Namespace, ctx: RepoContext) -> int:
    root = command_root(ctx, "ARCH_ROOT_DIR")
    debt = root / "scripts/architecture-size-debt.txt"
    if debt.is_file() and any(line.strip() and not line.lstrip().startswith("#") for line in read_text(debt).splitlines()):
        print("──  ────────────────────────────\n")
        print("❌ scripts/architecture-size-debt.txt must not contain active debt entries.\n")
        print("⚠️  ")
        return EXIT_FAILURE
    oversized = []
    for source in iter_files(root / "src", ".rs"):
        if (root / "src/framework/generated") in source.parents:
            continue
        lines = read_text(source).count("\n")
        if lines > 4000:
            oversized.append((source.relative_to(root).as_posix(), lines))
    print("──  ────────────────────────────\n")
    for relative, lines in oversized:
        print("❌ {}: {}  4000 ".format(relative, lines))
    if oversized:
        print("\n⚠️  ")
        return EXIT_FAILURE
    print("✅  Rust ")
    return EXIT_OK


def command_check_host_dispatcher(_args: argparse.Namespace, ctx: RepoContext) -> int:
    root = command_root(ctx, "HOST_DISPATCH_ROOT")
    src = root / "src"
    failures = []  # type: List[str]
    forbidden = (
        (r"CURRENT_SCRIPT_CTX", "Lua invocation TLS"),
        (r"BlockedEventBridge", "raw blocked-event bridge"),
        (r"erase_command_conn_lifetime|erase_script_runtime_lifetime", "erased script host lifetime"),
        (r"Option\s*<\s*\*mut\s+ScriptRunCtx", "raw running-script pointer"),
        (r"command_registry:\s*\*const\s+CommandRegistry", "raw Lua command-registry pointer"),
        (r"AofReplayPlayer.*\*mut\s+(?:CommandRegistry|ServerContext)", "raw AOF replay host pointer"),
        (r"loading_rx|loading_started_at|loading_large_rdb|wait_for_initial_loading|start_loading", "startup RDB worker state"),
    )
    rust_sources = list(iter_files(src, ".rs"))
    for expression, description in forbidden:
        matches = []
        pattern = re.compile(expression)
        for source in rust_sources:
            for number, line in enumerate(read_text(source).splitlines(), 1):
                if pattern.search(line):
                    matches.append("{}:{}:{}".format(source, number, line))
        if matches:
            failures.append("host-dispatcher violation: {}\n{}".format(description, "\n".join(matches)))
    required = (
        (r"type HostDispatcher", "src/engine/host.rs", "scoped HostDispatcher protocol"),
        (r"struct RdbLoadTask", "src/framework/host_tasks/rdb.rs", "owned RDB host task"),
        (r"struct AofReplayTask", "src/framework/host_tasks/aof.rs", "resumable AOF host task"),
    )
    for expression, relative, description in required:
        source = root / relative
        if not source.is_file() or not re.search(expression, read_text(source)):
            failures.append("host-dispatcher violation: missing {} in {}".format(description, relative))
    for failure in failures:
        print(failure, file=sys.stderr)
    return EXIT_FAILURE if failures else EXIT_OK


RUNTIME_EVENT_MODEL_RETIRED_IDENTIFIERS = (
    "drain_ready_producers",
    "producer_relay_pending",
    "drive_diskless_relay",
    "drive_disk_sync_relay",
    "drive_disk_sync_batch",
    "drive_slot_snapshot",
    "needs_fast_poll",
    "DiskSyncRelay",
    "poll_data_lazyfree_completions",
    "poll_replication_lazyfree_completions",
    "reap_lingering_diskless_child",
    "reap_lingering_disk_sync_child",
)

RUNTIME_EVENT_MODEL_RETIRED_OWNER_PROBES = (
    "sentinel_links.is_link_token",
    "cluster_bus.is_bus_token",
    "listeners.all.contains",
)

RUNTIME_DRIVER_FORBIDDEN_PATTERNS = (
    (re.compile(r"crate::runtime::(?:event_loop|server|transport)"), "server runtime import"),
    (re.compile(r"\b(?:EventLoopRuntime|RuntimeTransport|JsValue)\b"), "host-specific type"),
    (re.compile(r"\b(?:wasm_bindgen|web_sys|polling)::"), "host-specific dependency"),
    (re.compile(r"cfg(?:_attr)?\s*\([^\n]*feature\s*=\s*\"wasm\""), "WASM feature gate"),
)


def _runtime_driver_violations(root: Path) -> List[str]:
    failures = []  # type: List[str]
    driver = root / "src/runtime/driver"
    for source in iter_files(driver, ".rs"):
        relative = source.relative_to(root).as_posix()
        text = read_text(source)
        for pattern, description in RUNTIME_DRIVER_FORBIDDEN_PATTERNS:
            for match in pattern.finditer(text):
                line = text.count("\n", 0, match.start()) + 1
                failures.append(
                    "runtime-driver: {} at {}:{}".format(description, relative, line)
                )
    worker = root / "src/bin/wasm/worker.mjs"
    if worker.is_file():
        text = read_text(worker)
        for retired in ("execute_at", "next_deadline_ms", ".tick("):
            if retired in text:
                failures.append(
                    "runtime-driver: retired browser scheduling API {} at {}".format(
                        retired, worker.relative_to(root).as_posix()
                    )
                )
    return failures


def _runtime_event_model_violations(root: Path) -> List[str]:
    src = root / "src/runtime"
    failures = []  # type: List[str]
    identifier_pattern = re.compile(
        r"\b(?:{})\b".format("|".join(re.escape(item) for item in RUNTIME_EVENT_MODEL_RETIRED_IDENTIFIERS))
    )
    for source in iter_files(src, ".rs"):
        relative = source.relative_to(root).as_posix()
        for number, line in enumerate(rust_code_lines(read_text(source)), 1):
            match = identifier_pattern.search(line)
            if match:
                failures.append(
                    "runtime-event-model: retired identifier {} at {}:{}".format(
                        match.group(0), relative, number
                    )
                )
    dispatch = root / "src/runtime/server/worker/reactor/dispatch.rs"
    if dispatch.is_file():
        for number, line in enumerate(rust_code_lines(read_text(dispatch)), 1):
            for probe in RUNTIME_EVENT_MODEL_RETIRED_OWNER_PROBES:
                if probe in line:
                    failures.append(
                        "runtime-event-model: retired owner probe {} at {}:{}".format(
                            probe, dispatch.relative_to(root).as_posix(), number
                        )
                    )
    failures.extend(_runtime_driver_violations(root))
    return failures


def command_check_runtime_event_model(_args: argparse.Namespace, ctx: RepoContext) -> int:
    root = command_root(ctx, "RUNTIME_EVENT_MODEL_ROOT")
    failures = _runtime_event_model_violations(root)
    for failure in failures:
        print(failure, file=sys.stderr)
    return EXIT_FAILURE if failures else EXIT_OK


def _runtime_ownership_violations(root: Path) -> List[str]:
    failures = []  # type: List[str]
    required = (
        ("src/runtime/server/mod.rs", r"struct\s+ServerRuntime\b", "server facade"),
        (
            "src/runtime/server/supervisor/mod.rs",
            r"struct\s+ServerSupervisor\b",
            "server supervisor owner",
        ),
        (
            "src/runtime/server/worker/runtime.rs",
            r"RuntimeKernel\s*<\s*ServerWorkerMachine\s*>",
            "worker-owned runtime kernel",
        ),
        (
            "src/runtime/server/worker/state.rs",
            r"struct\s+ServerWorkerState\b",
            "worker-local state",
        ),
    )
    for relative, expression, description in required:
        source = root / relative
        if not source.is_file() or not re.search(expression, read_text(source)):
            failures.append(
                "runtime-ownership: missing {} in {}".format(description, relative)
            )

    root_mod = root / "src/runtime/mod.rs"
    if root_mod.is_file():
        modules = set(
            re.findall(
                r"^\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+([a-zA-Z0-9_]+)\s*;",
                read_text(root_mod),
                re.MULTILINE,
            )
        )
        unexpected = sorted(modules - {"driver", "embedded", "server"})
        if unexpected:
            failures.append(
                "runtime-ownership: runtime root exposes non-host modules: {}".format(
                    ", ".join(unexpected)
                )
            )

    shared_sources = (
        root / "src/runtime/server/shared.rs",
        root / "src/runtime/server/supervisor/mod.rs",
    )
    shared_state_pattern = re.compile(
        r"(?:Arc\s*<\s*)?(?:Mutex|RwLock)\s*<[^>]*ServerWorkerState"
    )
    for source in shared_sources:
        if not source.is_file():
            continue
        for match in shared_state_pattern.finditer(read_text(source)):
            line = read_text(source).count("\n", 0, match.start()) + 1
            failures.append(
                "runtime-ownership: shared worker state lock at {}:{}".format(
                    source.relative_to(root).as_posix(), line
                )
            )
    return failures


def command_check_runtime_ownership(_args: argparse.Namespace, ctx: RepoContext) -> int:
    root = command_root(ctx, "RUNTIME_OWNERSHIP_ROOT")
    failures = _runtime_ownership_violations(root)
    for failure in failures:
        print(failure, file=sys.stderr)
    return EXIT_FAILURE if failures else EXIT_OK


REPLICATION_OFFSET_RETIRED_METHOD_DECLARATIONS = (
    "append_frame",
    "backlog_protection_offset",
    "can_continue_from",
    "current_offset",
    "dual_main_backlog_pin_offset",
    "first_byte_offset",
    "min_protected_backlog_offset",
    "min_protected_offset",
    "next_send_offset",
    "next_stream_offset",
    "read_from_cursor",
    "second_replid_offset",
    "sent_offset",
    "set_min_protected_offset",
    "try_adopt_primary_from_rdb",
    "try_rdb_snapshot_boundary",
    "try_restore_primary_from_rdb",
)

REPLICATION_OFFSET_RETIRED_TYPE_DECLARATIONS = (
    "OffsetRange",
)

REPLICATION_OFFSET_RETIRED_FIELD_DECLARATIONS = (
    "backlog_protection_offset",
    "current_offset",
    "dual_main_backlog_pin_offset",
    "dual_endoff_snapshot_offset",
    "first_byte_offset",
    "min_protected_backlog_offset",
    "min_protected_offset",
    "next_send_offset",
    "next_stream_offset",
    "sent_offset",
)

REPLICATION_OFFSET_OWNER_DIRS = (
    "src/engine/distributed/replication",
    "src/framework/distributed/replication",
    "src/runtime/server/worker/distributed/replication",
)

REPLICATION_OFFSET_OWNER_FILES = (
    "src/runtime/server/worker/distributed/replication.rs",
)


def _replication_offset_owner_sources(root: Path) -> List[Path]:
    sources = set()  # type: set
    for relative in REPLICATION_OFFSET_OWNER_DIRS:
        sources.update(iter_files(root / relative, ".rs"))
    for relative in REPLICATION_OFFSET_OWNER_FILES:
        source = root / relative
        if source.is_file():
            sources.add(source)
    return sorted(sources, key=lambda source: source.as_posix())


def _replication_offset_domain_violations(root: Path) -> List[str]:
    failures = []  # type: List[str]
    method_pattern = re.compile(
        r"\bfn\s+(?P<name>{})\s*\(".format(
            "|".join(
                re.escape(item)
                for item in REPLICATION_OFFSET_RETIRED_METHOD_DECLARATIONS
            )
        )
    )
    type_pattern = re.compile(
        r"\b(?:struct|enum|type)\s+(?P<name>{})\b".format(
            "|".join(
                re.escape(item)
                for item in REPLICATION_OFFSET_RETIRED_TYPE_DECLARATIONS
            )
        )
    )
    field_pattern = re.compile(
        r"(?m)^[ \t]*(?:pub(?:\s*\([^)]*\))?[ \t]+)?"
        r"(?P<name>{})\s*:".format(
            "|".join(
                re.escape(item)
                for item in REPLICATION_OFFSET_RETIRED_FIELD_DECLARATIONS
            )
        )
    )
    struct_pattern = re.compile(
        r"\bstruct\s+[A-Za-z_][A-Za-z0-9_]*(?:\s*<[^{};]*>)?\s*"
        r"\{(?P<body>[^{}]*)\}"
    )
    for source in _replication_offset_owner_sources(root):
        relative = source.relative_to(root).as_posix()
        code = "\n".join(rust_code_lines(read_text(source)))
        declarations = (
            (method_pattern, "method"),
            (type_pattern, "type"),
        )
        for pattern, declaration_kind in declarations:
            for match in pattern.finditer(code):
                number = code.count("\n", 0, match.start("name")) + 1
                failures.append(
                    "replication-offset-domains: retired {} declaration {} at {}:{}".format(
                        declaration_kind, match.group("name"), relative, number
                    )
                )
        for struct_match in struct_pattern.finditer(code):
            body = struct_match.group("body")
            for field_match in field_pattern.finditer(body):
                field_position = struct_match.start("body") + field_match.start("name")
                number = code.count("\n", 0, field_position) + 1
                failures.append(
                    "replication-offset-domains: retired field declaration {} at {}:{}".format(
                        field_match.group("name"), relative, number
                    )
                )
    return failures


def command_check_replication_offset_domains(
    _args: argparse.Namespace, ctx: RepoContext
) -> int:
    failures = _replication_offset_domain_violations(ctx.root)
    for failure in failures:
        print(failure, file=sys.stderr)
    return EXIT_FAILURE if failures else EXIT_OK


def configured_unsafe_roots(root: Path) -> List[str]:
    roots_file = Path(os.environ.get("UNSAFE_ROOTS_FILE", str(root / "scripts/unsafe-roots.txt")))
    if not roots_file.is_file():
        raise ToolError("unsafe-boundaries: roots file not found: {}".format(roots_file), EXIT_FAILURE)
    roots = []
    for raw in read_text(roots_file).splitlines():
        value = raw.split("#", 1)[0].strip()
        if not value:
            continue
        components = value.split("/")
        valid_prefix = components[0] in ("src", "tests", "benches", "command_macros") or value == "build.rs"
        if value.startswith("/") or ".." in components or any(char in value for char in "*?[") or not valid_prefix:
            raise ToolError("unsafe-boundaries: invalid unsafe root: {}".format(value), EXIT_FAILURE)
        roots.append(value)
    if not roots:
        raise ToolError("unsafe-boundaries: no unsafe roots configured", EXIT_FAILURE)
    if len(set(roots)) != len(roots):
        raise ToolError("unsafe-boundaries: duplicate unsafe root", EXIT_FAILURE)
    return roots


UNSAFE_PATTERN = re.compile(r"(?:^|[^A-Za-z0-9_])unsafe\s*(?:\{|fn\b|extern\b|impl\b|trait\b)")


def unsafe_source_files(root: Path) -> List[Path]:
    files = []  # type: List[Path]
    for directory in ("src", "tests", "benches", "command_macros"):
        for source in iter_files(root / directory, ".rs"):
            relative = source.relative_to(root).as_posix()
            if "/target/" in "/{}/".format(relative) or "/thirdparty/" in "/{}/".format(relative) or "/framework/generated/" in "/{}/".format(relative) or "/.git/" in "/{}/".format(relative) or "/.worktrees/" in "/{}/".format(relative):
                continue
            files.append(source)
    if (root / "build.rs").is_file():
        files.append(root / "build.rs")
    return sorted(files, key=lambda item: item.as_posix())


def command_check_unsafe_boundaries(_args: argparse.Namespace, ctx: RepoContext) -> int:
    root = command_root(ctx, "UNSAFE_BOUNDARY_ROOT")
    roots = configured_unsafe_roots(root)
    violations = []  # type: List[str]
    matched_files = 0
    for source in unsafe_source_files(root):
        matches = []
        for number, line in enumerate(read_text(source).splitlines(), 1):
            if UNSAFE_PATTERN.search(line):
                matches.append("{}:{}".format(number, line))
        if not matches:
            continue
        matched_files += 1
        relative = source.relative_to(root).as_posix()
        allowed = any(relative == allowed_root or relative.startswith(allowed_root + "/") for allowed_root in roots)
        if not allowed:
            violations.extend("{}:{}".format(relative, match) for match in matches)
    if violations:
        print("unsafe-boundaries: unsafe syntax outside configured roots:", file=sys.stderr)
        for violation in violations:
            print(violation, file=sys.stderr)
        return EXIT_FAILURE
    print("unsafe-boundaries: allowed roots={} matched files={}".format(len(roots), matched_files))
    return EXIT_OK


def command_check_unsafe_safety(_args: argparse.Namespace, ctx: RepoContext) -> int:
    root = command_root(ctx, "UNSAFE_SAFETY_ROOT_DIR")
    failures = []  # type: List[str]
    for source in unsafe_source_files(root):
        lines = read_text(source).splitlines()
        for index, line in enumerate(lines):
            if not UNSAFE_PATTERN.search(line):
                continue
            start = max(0, index - 8)
            if not any("// SAFETY:" in candidate for candidate in lines[start:index + 1]):
                failures.append("    {}:{}:{}".format(source, index + 1, line))
    for failure in failures:
        print(failure)
    if failures:
        print("\n⚠️  Missing SAFETY comments found. See above.")
        return EXIT_FAILURE
    print("✅ All unsafe blocks have SAFETY comments.")
    return EXIT_OK


def foundation_dependency_files(root: Path, source_name: str, target_name: str) -> List[str]:
    found = []
    source_root = root / "src/foundation" / source_name
    direct = "crate::foundation::{}::".format(target_name)
    grouped = re.compile(
        r"crate::foundation::\{[^;]*" + re.escape(target_name) + r"(?:::|[,}])"
    )
    for source in iter_files(source_root, ".rs"):
        code = "".join(rust_code_lines(read_text(source))).replace(" ", "").replace("\t", "")
        if direct in code or grouped.search(code):
            found.append(source.relative_to(root).as_posix())
    return found


def command_check_foundation_dependencies(args: argparse.Namespace, ctx: RepoContext) -> int:
    root = command_root(ctx, "FOUNDATION_DEP_ROOT")
    foundation = root / "src/foundation"
    if not (foundation / "io").is_dir() or not (foundation / "memory").is_dir():
        raise ToolError("foundation dependency scan requires src/foundation/{io,memory}", EXIT_USAGE)
    unsupported = []
    alias_pattern = re.compile(r"usecrate::foundationas[A-Za-z_]|usecrate::foundation::\{selfas[A-Za-z_]")
    for source in iter_files(foundation, ".rs"):
        code = "".join(rust_code_lines(read_text(source))).replace(" ", "").replace("\t", "")
        if alias_pattern.search(code):
            unsupported.append(source.relative_to(root).as_posix())
    if unsupported:
        print("unsupported foundation root aliases (use canonical crate::foundation::{io,memory} paths):", file=sys.stderr)
        for relative in unsupported:
            print("  {}".format(relative), file=sys.stderr)
        return EXIT_USAGE
    memory_to_io = foundation_dependency_files(root, "memory", "io")
    io_to_memory = foundation_dependency_files(root, "io", "memory")
    print("memory -> io: {} file(s)".format(len(memory_to_io)))
    for relative in memory_to_io:
        print("  {}".format(relative))
    print("io -> memory: {} file(s)".format(len(io_to_memory)))
    for relative in io_to_memory:
        print("  {}".format(relative))
    print("cycle: {}".format("present" if memory_to_io and io_to_memory else "absent"))
    if args.enforce and memory_to_io:
        print("invalid direction: foundation/memory must not depend on foundation/io", file=sys.stderr)
        return EXIT_FAILURE
    return EXIT_OK


def remove_path(path: Path) -> None:
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(str(path))
    elif os.path.lexists(str(path)):
        path.unlink()


def restore_test_binaries(valkey_src: Path, state_dir: Path) -> None:
    backup_dir = state_dir / "original"
    absent_dir = state_dir / "absent"
    lock_path = valkey_src / ".versal-test-binaries.lock"
    owner_path_file = state_dir / "lock-owner-path"
    for binary in VERSAL_BINARIES:
        target = valkey_src / "valkey-{}".format(binary)
        backup = backup_dir / "valkey-{}".format(binary)
        marker = absent_dir / "valkey-{}".format(binary)
        if os.path.lexists(str(backup)):
            remove_path(target)
            shutil.move(str(backup), str(target))
        elif marker.exists():
            remove_path(target)
            marker.unlink()
    if owner_path_file.is_file():
        owner_temp_text = read_text(owner_path_file).splitlines()
        if owner_temp_text:
            remove_path(Path(owner_temp_text[0]))
    if lock_path.is_file() and read_text(lock_path).splitlines()[:1] == [str(state_dir)]:
        lock_path.unlink()
    remove_path(owner_path_file)
    for directory in (backup_dir, absent_dir, state_dir):
        try:
            directory.rmdir()
        except OSError:
            pass


def stage_test_binaries(cargo_bin: Path, valkey_src: Path, state_dir: Path) -> None:
    if not valkey_src.is_dir():
        raise ToolError("Valkey source directory does not exist: {}".format(valkey_src))
    if os.path.lexists(str(state_dir)):
        raise ToolError("Versal test binary state directory already exists: {}".format(state_dir))
    sources = []  # type: List[Tuple[str, Path]]
    for binary in VERSAL_BINARIES:
        source = cargo_bin / "versal-{}".format(binary)
        if not source.is_file() or not os.access(str(source), os.X_OK):
            raise ToolError("Versal test binary is missing or not executable: {}".format(source))
        sources.append((binary, source))

    backup_dir = state_dir / "original"
    absent_dir = state_dir / "absent"
    lock_path = valkey_src / ".versal-test-binaries.lock"
    owner_path_file = state_dir / "lock-owner-path"
    owner_temp = valkey_src / ".versal-test-binaries.owner.{}".format(os.getpid())
    backup_dir.mkdir(parents=True)
    absent_dir.mkdir()
    owner_path_file.write_text(str(owner_temp) + "\n", encoding="utf-8")
    owner_temp.write_text(str(state_dir) + "\n", encoding="utf-8")
    try:
        os.link(str(owner_temp), str(lock_path))
    except OSError:
        remove_path(owner_temp)
        remove_path(owner_path_file)
        try:
            backup_dir.rmdir()
            absent_dir.rmdir()
            state_dir.rmdir()
        except OSError:
            pass
        raise ToolError("Another runner is already using {}".format(valkey_src))
    remove_path(owner_temp)
    try:
        for binary, source in sources:
            target = valkey_src / "valkey-{}".format(binary)
            backup = backup_dir / target.name
            if os.path.lexists(str(target)):
                shutil.move(str(target), str(backup))
            else:
                (absent_dir / target.name).touch()
            shutil.copy2(str(source), str(target))
    except Exception:
        restore_test_binaries(valkey_src, state_dir)
        raise


def command_tcl_binaries(args: argparse.Namespace, _ctx: RepoContext) -> int:
    if args.binary_action == "stage":
        stage_test_binaries(Path(args.cargo_bin_dir), Path(args.valkey_src_dir), Path(args.state_dir))
    else:
        restore_test_binaries(Path(args.valkey_src_dir), Path(args.state_dir))
    return EXIT_OK


def command_tcl_module_fixtures(args: argparse.Namespace, _ctx: RepoContext) -> int:
    units = args.units
    module_units = [unit for unit in units if unit.startswith("unit/moduleapi/")]
    if not module_units:
        return EXIT_OK
    tcl_dir = Path(args.valkey_source_dir)
    module_dir = tcl_dir / "tests/modules"
    make_program = os.environ.get("MAKE", "make")
    built = run_command([make_program, "-C", str(module_dir)])
    if built.returncode != 0:
        return built.returncode
    missing = []  # type: List[str]
    module_pattern = re.compile(r"tests/modules/([^\s\]\"']+\.so)")
    for unit in module_units:
        test_file = tcl_dir / "tests/{}.tcl".format(unit)
        if not test_file.is_file():
            print("Module API Tcl unit missing: tests/{}.tcl".format(unit), file=sys.stderr)
            missing.append("tests/{}.tcl".format(unit))
            continue
        for module in module_pattern.findall(read_text(test_file)):
            if not (module_dir / module).is_file():
                display = "tests/modules/{}".format(module)
                print("Module API fixture missing after build: {}".format(display), file=sys.stderr)
                missing.append(display)
    if missing:
        return EXIT_FAILURE
    print("==> Module API Tcl fixtures ready")
    return EXIT_OK


def run_logged(argv: Sequence[str], log_file: Path, cwd: Path, env: Optional[Mapping[str, str]] = None) -> int:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("wb") as output:
        return subprocess.run(list(argv), cwd=str(cwd), env=dict(env) if env is not None else None, stdout=output, stderr=subprocess.STDOUT).returncode


def command_ci_performance(args: argparse.Namespace, _ctx: RepoContext) -> int:
    baseline = args.baseline_ms
    current = args.current_ms
    if baseline <= 0 or current < 0:
        raise ToolError("baseline must be > 0 and current must be >= 0", EXIT_USAGE)
    limit = baseline * 1.10
    if current > limit:
        print(
            "startup regression {:.3f}ms exceeds 10% gate from baseline {:.3f}ms (limit {:.3f}ms)".format(current, baseline, limit),
            file=sys.stderr,
        )
        return EXIT_FAILURE
    print("startup performance gate ok: current {:.3f}ms <= {:.3f}ms".format(current, limit))
    return EXIT_OK


def running_versal_server_pids() -> List[int]:
    result = run_command(["ps", "-eo", "pid=,comm="], capture=True)
    pids = []
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) == 2 and fields[1] == "versal-server":
            pids.append(int(fields[0]))
    return pids


def cleanup_versal_servers() -> None:
    for pid in running_versal_server_pids():
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


@contextlib.contextmanager
def staged_test_binaries(cargo_bin: Path, valkey_src: Path) -> Iterable[None]:
    with tempfile.TemporaryDirectory(prefix="versal-binary-stage-") as directory:
        state = Path(directory) / "binaries"
        stage_test_binaries(cargo_bin, valkey_src, state)
        try:
            yield
        finally:
            restore_test_binaries(valkey_src, state)


def command_ci_dual_channel(args: argparse.Namespace, ctx: RepoContext) -> int:
    runs = args.runs if args.runs is not None else int(os.environ.get("DUAL_CHANNEL_GATE_RUNS", "5"))
    base_port = args.base_port if args.base_port is not None else int(os.environ.get("DUAL_CHANNEL_GATE_BASE_PORT", "28000"))
    if runs < 1:
        raise ToolError("DUAL_CHANNEL_GATE_RUNS must be at least 1", EXIT_USAGE)
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    output_dir = ctx.root / "target/dual-channel-five-run-gate" / stamp
    output_dir.mkdir(parents=True)
    versions = []
    for command in (["git", "rev-parse", "HEAD"], ["rustc", "--version"], ["cargo", "--version"], ["uname", "-a"]):
        result = run_command(command, cwd=ctx.root, capture=True)
        if result.returncode != 0:
            return result.returncode
        versions.append(result.stdout.strip())
    (output_dir / "environment.txt").write_text(
        "timestamp_utc={}\ngit_head={}\nrustc={}\ncargo={}\nuname={}\n".format(stamp, *versions),
        encoding="utf-8",
    )
    cleanup_versal_servers()
    build_log = output_dir / "build.log"
    if run_logged(["cargo", "build", "--bins"], build_log, ctx.root) != 0:
        return EXIT_FAILURE
    valkey_root = ctx.root / "thirdparty/valkey"
    try:
        with staged_test_binaries(ctx.root / "target/debug", valkey_root / "src"):
            for number in range(1, runs + 1):
                run_dir = output_dir / "run-{}".format(number)
                run_dir.mkdir()
                port = base_port + number * 100
                cleanup_versal_servers()
                print("==> Quality run {}/{}, baseport {}".format(number, runs, port))
                if run_logged(["cargo", "test", "--lib"], run_dir / "unit.log", ctx.root) != 0:
                    return EXIT_FAILURE
                if run_logged(["cargo", "test", "--test", "integration"], run_dir / "integration.log", ctx.root) != 0:
                    return EXIT_FAILURE
                env = dict(os.environ)
                env.pop("VALKEY_BIN_DIR", None)
                env.update({"VALKEY_TEST_TCP_SNDBUF_BYTES": "65536", "VALKEY_TEST_TCP_RCVBUF_BYTES": "87380"})
                command = [
                    "tclsh", "tests/test_helper.tcl", "--single", "integration/dual-channel-replication",
                    "--skip-leaks", "--timeout", "210", "--baseport", str(port), "--portcount", "8000",
                    "--stop", "--dump-logs", "--dont-clean",
                ]
                if run_logged(command, run_dir / "system.log", valkey_root, env) != 0:
                    return EXIT_FAILURE
                (run_dir / "result.txt").write_text("unit=passed\nintegration=passed\nsystem=passed\n", encoding="utf-8")
    finally:
        cleanup_versal_servers()
    print("Five-run dual-channel quality gate passed. Evidence: {}".format(output_dir))
    return EXIT_OK


def clear_directory_children(directory: Path) -> None:
    if not directory.is_dir():
        return
    for child in directory.iterdir():
        remove_path(child)


def command_tcl_repro_cluster(args: argparse.Namespace, ctx: RepoContext) -> int:
    if shutil.which("tclsh") is None:
        print("repro-cluster: tclsh not found in PATH", file=sys.stderr)
        return EXIT_FAILURE
    build = "release" if args.release else "debug"
    output = Path(args.out).resolve() if args.out else ctx.root / ".dbg/repro"
    valkey = ctx.root / "thirdparty/valkey"
    print("repro-cluster: unit={} iters={} build={} out={}".format(args.probe, args.iters, build, output))
    output.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    env.pop("VALKEY_BIN_DIR", None)
    with staged_test_binaries(ctx.root / "target" / build, valkey / "src"):
        for number in range(1, args.iters + 1):
            clear_directory_children(valkey / "tests/tmp")
            log = output / "run_{}.log".format(number)
            command = ["tclsh", "tests/test_helper.tcl", "--single", args.probe, "--skip-leaks", "--dont-clean", "--timeout", "120"]
            run_logged(command, log, valkey, env)
            content = read_text(log)
            if "[err]" in content or "[exception]" in content:
                print("REPRODUCED on iteration {}".format(number))
                saved = output / "fail_{}".format(number)
                saved.mkdir(parents=True, exist_ok=True)
                shutil.copy2(str(log), str(saved / "harness.log"))
                for server_dir in sorted((valkey / "tests/tmp").glob("*/")):
                    if not server_dir.is_dir():
                        continue
                    for name in ("stdout", "stderr", "nodes.conf"):
                        source = server_dir / name
                        if source.is_file():
                            destination = "{}_nodes.conf".format(server_dir.name) if name == "nodes.conf" else "{}_{}.log".format(server_dir.name, name)
                            shutil.copy2(str(source), str(saved / destination))
                print("Saved logs to {}".format(saved))
                return EXIT_OK
            print("iter {}: pass".format(number))
    print("NO REPRO in {} iterations".format(args.iters))
    return EXIT_OK


def generated_metadata_error(message: str) -> ToolError:
    return ToolError("generated-command-metadata: {}".format(message), EXIT_FAILURE)


def generated_snapshot(directory: Path) -> List[Tuple[str, str]]:
    result = []
    for source in sorted(directory.glob("*.rs")):
        digest = hashlib.sha256(source.read_bytes()).hexdigest()
        result.append((source.name, digest))
    return result


CConfigEntry = namedtuple("CConfigEntry", "name alias")
CapabilityEntry = namedtuple("CapabilityEntry", "name alias status")


CONFIG_MACRO_RE = re.compile(
    r'create[A-Za-z]+Config\(\s*"([^"]+)"\s*,\s*(?:NULL|"([^"]+)")',
    re.MULTILINE,
)
REGISTRY_NAME_RE = re.compile(r'\bname:\s*"([^"]+)"')
REGISTRY_ALIAS_RE = re.compile(r'\balias:\s*Some\("([^"]+)"\)')
MACRO_NAME_RE = re.compile(
    r'\b[A-Za-z_][A-Za-z0-9_]*_spec!\s*\(\s*"([^"]+)"', re.MULTILINE
)


# A setting in this table is intentionally not inferred from a ConfigSpec.
# These decisions are reviewed capability boundaries or runtime-verified
# implementations; new C entries must be added here or stay surface_registered.
EXPLICIT_STATUS = {
    # C defrag.c:1290-1310 drives an incremental relocation cycle from these
    # thresholds and budgets. Rust uses the same active-vs-allocated pressure
    # gate, C-compatible defaults/ranges, cursor budget, and live INFO stats.
    "active-defrag-cycle-max": "implemented",
    "active-defrag-cycle-min": "implemented",
    "active-defrag-cycle-us": "implemented",
    "active-defrag-ignore-bytes": "implemented",
    "active-defrag-max-scan-fields": "implemented",
    "active-defrag-threshold-lower": "implemented",
    "active-defrag-threshold-upper": "implemented",
    # C expire.c:195-215 derives active-expire cycle budgets from this 1-10
    # setting. Rust server-cron passes the live value into the KV expiry loop,
    # whose effort-specific batches and budget match that same monotonic policy.
    "active-expire-effort": "implemented",
    "activedefrag": "implemented",
    # C serverCron keeps resize decisions active but pauses migration of an
    # already-created rehash target when this switch is off. Rust threads the
    # live bool from ConfigRegistry through the DB cron boundary and has a
    # focused cursor pause/resume regression.
    "activerehashing": "implemented",
    # C's automatic AOF rewrite controls are consumed by the rewrite policy
    # before each background rewrite decision; Rust's persistence policy and
    # BGREWRITEAOF owner use the same live values.
    "auto-aof-rewrite-min-size": "implemented",
    "auto-aof-rewrite-percentage": "implemented",
    # AOF lifecycle settings below have live owners: appendonly opens/closes
    # the journal transactionally, appendfsync switches the appender policy,
    # truncated-load gates recovery, and the preamble flag selects rewrite and
    # replica-install formats. Focused integration coverage exercises each.
    "appendonly": "implemented",
    "appendfsync": "implemented",
    "appendfilename": "startup_only",
    "appenddirname": "startup_only",
    # Both are C immutable startup resources. Rust applies the hash seed before
    # database/hash construction and opens Unix listeners only during startup;
    # runtime SET rejects all of these names. Unix-socket integration verifies
    # RESP service and configured permissions on the actual filesystem node.
    "hash-seed": "startup_only",
    "unixsocket": "startup_only",
    "unixsocketgroup": "startup_only",
    "unixsocketperm": "startup_only",
    "aof-load-truncated": "implemented",
    # C enables history cleanup immediately when this live test-only switch
    # turns off. Rust preserves history while disabled, then removes history
    # files and entries as soon as auto GC is re-enabled.
    "aof-disable-auto-gc": "implemented",
    "aof-use-rdb-preamble": "implemented",
    # Runtime AOF feeding writes timestamp annotations when the live bool is
    # enabled; rewrite options use the same setting. Focused AOF coverage
    # observes a CONFIG SET transition annotating the incremental file.
    "aof-timestamp-enabled": "implemented",
    # C cluster.c publishes this runtime-updatable endpoint metadata through
    # CLUSTER replies and HELLO. Rust applies it transactionally to the local
    # endpoint snapshot, persists it, and has focused config-set coverage.
    "availability-zone": "implemented",
    # C ACL logging retains only the configured recent entries. Rust updates
    # the live ACL log cap in CONFIG SET and truncates each new denial at that
    # owner; black-box coverage exercises caps of two and zero.
    "acllog-max-len": "implemented",
    # C applies this default when it creates every ACL selector. Rust keeps the
    # L2 constructor context-free and passes the live policy only from ACL
    # SETUSER; root reset/new-user and nested selectors all inherit it.
    "acl-pubsub-default": "implemented",
    # C treats aclfile as immutable. Rust loads it during startup assembly and
    # rejects CONFIG SET so the active ACL registry can never diverge from the
    # displayed path; ACL startup integration covers the loaded user policy.
    "aclfile": "startup_only",
    # C server.c:7000-7019. The startup banner is normally terminal-only and
    # emits the compact mode line otherwise; this immutable option forces the
    # banner through the configured logger even when stdout is not a TTY.
    "always-show-logo": "implemented",
    # C aof.c:2521-2526. The forked rewrite writer carries this policy and
    # syncs each completed 32 MiB output interval before final atomic install.
    "aof-rewrite-incremental-fsync": "implemented",
    # C aof.c:2635 pins the rewrite child. Rust prevalidates the startup list
    # before fork and applies it in the rewrite child before writing its temp
    # base file; the focused Linux regression observes the child's kernel mask.
    "aof-rewrite-cpulist": "implemented",
    # C rdb.c, cluster_migrateslots.c, and replication full-sync child paths
    # apply this to every RDB-producing child. Rust prevalidates in the parent
    # and applies the same mask in BGSAVE, diskless, disk-sync, direct, and
    # slot-migration children before serialization begins.
    "bgsave-cpulist": "implemented",
    # C changeListener releases the old listener before binding a replacement.
    # Rust does the same for overlapping wildcard/concrete endpoints, while
    # restoring the former listeners if the replacement fails. Integration
    # covers CONFIG SET 0.0.0.0 -> 127.0.0.1 on one port; unit coverage freezes
    # rollback after an occupied-port failure.
    "bind": "implemented",
    # Replica upstream reconnects bind the configured source IP before PSYNC.
    # Focused black-box coverage uses 127.0.0.2 and proves the primary's
    # CLIENT LIST sees that exact source address on the replication link.
    "bind-source-addr": "implemented",
    # C bio.c:254 applies this inside every BIO worker. Rust has two
    # equivalent detached-value workers (ordinary and replication lazyfree);
    # L5 sends each a prevalidated affinity mask during startup and each worker
    # applies it to itself before processing later jobs.
    "bio-cpulist": "implemented",
    # C's busy-reply-threshold and lua-time-limit are one setting. Rust maps
    # both spellings to the live hosted Lua/Function timeout before every run;
    # integration coverage lowers it to 10ms and observes the BUSY pipeline.
    "busy-reply-threshold": "implemented",
    "client-default-resp": "conditional_build",
    # C classifies normal, replica, and Pub/Sub output buffers independently.
    # Rust's multi-argument special setter preserves those classes and L5
    # enforces their hard/soft policies for ordinary clients, replication,
    # Pub/Sub, and slot exports; the upstream INFO regression observes the
    # disconnect counter after a configured hard-limit breach.
    "client-output-buffer-limit": "implemented",
    # C networking.c disconnects ordinary clients whose incomplete request
    # buffer exceeds this size_t byte limit, while replication links bypass the
    # ordinary-client path. Rust stores the validated canonical byte count in
    # L3, applies it at both normal and HUP-drain receive boundaries, and
    # exports the matching INFO STATS disconnect counter.
    "client-query-buffer-limit": "implemented",
    # HLL encoding policy is read directly by PFADD/PFMERGE before each
    # serialization, so the threshold changes sparse-to-dense promotion rather
    # than merely changing CONFIG GET output.
    "hll-sparse-max-bytes": "implemented",
    # C defaults this setting to yes. Rust's protocol-error and DEBUG danger
    # diagnostics consume the live value before rendering the bounded query
    # snapshot; integration proves default redaction and live opt-out.
    "hide-user-data-from-log": "implemented",
    # C config.c:updateHZ accepts the broad integer surface then clamps it to
    # 1..500. Rust applies the same clamp to the live config, renders it in
    # INFO SERVER, and reschedules each subsequent cron deadline from that
    # live value rather than retaining the startup interval.
    "hz": "implemented",
    # C config.c:3377 and cluster.c:1236-1244. Rust owns the setting in the
    # Cluster pre-dispatch gate with startup, CONFIG REWRITE, integration, and
    # focused Cluster-down regression coverage.
    "cluster-allow-pubsubshard-when-down": "implemented",
    # Cluster metadata is applied by apply_cluster_config_snapshot: endpoint
    # changes update the local node, persist through nodes.conf, and trigger a
    # topology broadcast. Policy values below are consumed by the runtime cron,
    # command routing, migration coordinator, or reply renderer.
    "cluster-allow-reads-when-down": "implemented",
    "cluster-allow-replica-migration": "implemented",
    "cluster-announce-bus-port": "implemented",
    "cluster-announce-client-ipv4": "implemented",
    "cluster-announce-client-ipv6": "implemented",
    "cluster-announce-client-port": "implemented",
    "cluster-announce-client-tls-port": "implemented",
    "cluster-announce-hostname": "implemented",
    "cluster-announce-human-nodename": "implemented",
    "cluster-announce-ip": "implemented",
    "cluster-announce-port": "implemented",
    "cluster-announce-tls-port": "implemented",
    "cluster-config-file": "startup_only",
    "cluster-config-save-behavior": "implemented",
    "cluster-databases": "startup_only",
    "cluster-enabled": "startup_only",
    "cluster-link-sendbuf-limit": "implemented",
    "cluster-manual-failover-timeout": "implemented",
    "cluster-message-gossip-perc": "implemented",
    "cluster-migration-barrier": "implemented",
    "cluster-node-timeout": "implemented",
    "cluster-ping-interval": "implemented",
    "cluster-port": "startup_only",
    "cluster-preferred-endpoint-type": "implemented",
    "cluster-replica-no-failover": "implemented",
    "cluster-replica-priority": "implemented",
    "cluster-replica-validity-factor": "implemented",
    "cluster-require-full-coverage": "implemented",
    "cluster-slot-migration-log-max-len": "implemented",
    # C cluster_migrateslots.c:1456-1468 waits to pause export writes until
    # the migration reply backlog is at or below this signed byte limit. Rust
    # maintains the same logical remaining-replication projection at every
    # output boundary and consults the live limit before its pause transition.
    "slot-migration-max-failover-repl-bytes": "implemented",
    "cluster-slot-stats-enabled": "implemented",
    # C cluster_legacy.c:2504-2557. CLUSTER FORGET creates the local
    # re-addition expiry from this seconds-valued setting; gossip TTL remains
    # a remote protocol input.
    "cluster-blacklist-ttl": "implemented",
    # C commandlog.c reads this live slow-category threshold and bounded list.
    # Rust reconfigures both the legacy SlowLog and CommandLog owner together.
    "commandlog-execution-slower-than": "implemented",
    "commandlog-large-request-max-len": "implemented",
    "commandlog-large-reply-max-len": "implemented",
    "commandlog-request-larger-than": "implemented",
    "commandlog-reply-larger-than": "implemented",
    "commandlog-slow-execution-max-len": "implemented",
    "crash-log-enabled": "implemented",
    # C server.c:6945-6963 and 7737-7745. A safe foundation facade forks,
    # creates a new session, redirects standard descriptors, and makes only
    # the parent return; the child retains normal listener/pidfile startup.
    "daemonize": "implemented",
    "debug-context": "implemented",
    # Server assembly creates the database set from this immutable startup
    # value; focused black-box coverage verifies the selected range and CONFIG
    # SET rejection after startup.
    "databases": "startup_only",
    # RdbSaveOptions captures these live values for every SAVE/BGSAVE operation
    # and foundation's atomic writer joins them only at the save boundary.
    # Focused CONFIG SET coverage observes the next snapshot at the new path.
    "dir": "implemented",
    "dbfilename": "implemented",
    # The replica snapshots this live setting when it opens a primary link;
    # REPLCONF capability advertisement, primary eligibility, RDB-channel
    # association, terminal ACK handoff, and classic fallback all share that
    # protocol state. Unit and two-process integration coverage observe the
    # advertised capability, DUALCHANNELSYNC, paired INFO rows, and fallback.
    "dual-channel-replication-enabled": "implemented",
    "disable-thp": "unsupported_platform",
    "enable-debug-assert": "conditional_build",
    # DEBUG command dispatch uses the immutable no/yes/local protected-action
    # policy; focused tests exercise every branch independently of the test
    # harness's default debug override.
    "enable-debug-command": "implemented",
    # CONFIG SET checks every protected spec through this immutable no/yes/local
    # policy. Focused config-set coverage exercises all three actions without
    # relying on the integration harness's explicit protected override.
    "enable-protected-configs": "implemented",
    # Live compatibility mode switches identity fields rendered by HELLO and
    # LOLWUT. Black-box coverage observes both transitions through CONFIG SET.
    "extended-redis-compatibility": "implemented",
    # MODULE is a C protected command. Rust applies the startup-only
    # no/yes/local policy to every MODULE subcommand; focused coverage freezes
    # the local-peer distinction rather than only LOAD's former no check.
    "enable-module-command": "implemented",
    # C server.c:7717-7731 accepts ARM64-COW-BUG to override only the
    # MADV_FREE/fork corruption safety gate. Rust runs the equivalent probe
    # before runtime worker creation and applies the same case-insensitive
    # token policy without suppressing unrelated warnings.
    "ignore-warnings": "implemented",
    # C config.c:3393 and 3457. Rust L5 owns an IO-only worker pool: workers
    # process duplicated ordinary-client socket handles while command/state
    # ownership stays on the main loop. CONFIG SET stages and applies resize
    # before the command reply; the always-active flag controls worker parking.
    "io-threads": "implemented",
    "io-threads-always-active": "implemented",
    "jemalloc-bg-thread": "conditional_build",
    # C db.c:788-902. DEL follows the user-del policy, UNLINK stays async,
    # and FLUSHDB/FLUSHALL choose the user-flush default unless explicitly
    # overridden with SYNC or ASYNC. L5 owns detached-object destruction.
    "lazyfree-lazy-user-del": "implemented",
    "lazyfree-lazy-user-flush": "implemented",
    # Eviction, lazy expiry, and server-initiated replacement each choose
    # DEL/UNLINK through their own C-compatible owner; they are independent
    # from the user DEL/FLUSH switches above.
    "lazyfree-lazy-eviction": "implemented",
    "lazyfree-lazy-expire": "implemented",
    "lazyfree-lazy-server-del": "implemented",
    # DB eviction applies these controls live: the sampling budget chooses
    # candidates, LFU settings feed metadata decay/frequency, and tenacity
    # bounds the eviction time slice.
    "lfu-decay-time": "implemented",
    "lfu-log-factor": "implemented",
    "locale-collate": "unsupported_platform",
    # C server.c:214-229. The logger renders legacy, ISO8601-with-offset, or
    # epoch-milliseconds timestamps from a runtime-switchable atomic policy.
    "log-timestamp-format": "implemented",
    # CONFIG SET changes both the live config string and log's process-wide
    # filter. Focused setter coverage observes the LevelFilter transition.
    "loglevel": "implemented",
    # C's mutable format policy supports legacy, logfmt, and JSON output. The
    # Rust foundation formatter reads an atomic live policy for every record,
    # so CONFIG SET changes a running server instead of only its config field.
    # Black-box coverage observes both the C-shaped logfmt fields and a later
    # JSON record from the same process; unit coverage freezes logfmt quoting.
    "log-format": "implemented",
    "logfile": "startup_only",
    # The protected Lua compatibility switch rebuilds the live Lua runtime,
    # rather than only changing registry state. Black-box coverage observes
    # getfenv becoming available, then unavailable again, plus startup config;
    # the legacy alias resolves through the same spec.
    "lua-enable-insecure-api": "implemented",
    # Import mode is consumed by active/lazy expiry, eviction pre-dispatch,
    # scans and import-source client dispatch. CLIENT IMPORT-SOURCE accepts
    # C-style case-insensitive ON/OFF and exposes expired values only to that
    # connection; focused coverage verifies enable, preservation, and cleanup.
    "import-mode": "implemented",
    # C networking.c accepts at most this budget from a ready listener in one
    # event-loop callback. Rust's listener lifecycle reads the live config on
    # every callback; its focused unit test verifies the budget is not cached.
    "max-new-connections-per-cycle": "implemented",
    # C networking.c rejects a newly accepted client when the live count is at
    # the configured limit and increments rejected_connections. Rust does the
    # same before inserting any business indexes; black-box coverage observes
    # both the RESP rejection and INFO STATS counter.
    "maxclients": "implemented",
    "maxclients-reserved": "implemented",
    "priority-subnets": "implemented",
    "priority-preemptive-poll-interval-us": "implemented",
    "maxmemory": "implemented",
    # C's client-memory limit applies to normal/PubSub buffers independently of
    # key eviction. Rust parses byte/percentage values, refreshes client
    # accounting after CONFIG SET, and evicts before an oversized reply reaches
    # the socket; black-box coverage observes evicted_clients.
    "maxmemory-clients": "implemented",
    "maxmemory-eviction-tenacity": "implemented",
    "maxmemory-policy": "implemented",
    "maxmemory-samples": "implemented",
    # The runtime pre-dispatch, EXEC, and script gates count only online
    # replicas whose ACK is within the live max-lag. Black-box CONFIG SET
    # coverage verifies writes reject and recover with the threshold changes.
    "min-replicas-max-lag": "implemented",
    "min-replicas-to-write": "implemented",
    # C aof.c:1359-1362 suppresses both always and everysec fsync whenever a
    # persistence child is doing I/O. Rust derives an effective AOF mode from
    # the live config and child state, temporarily stops the everysec worker
    # without its shutdown-only final sync, and restores the configured mode
    # after the child ends. Focused runtime coverage observes the transition.
    "no-appendfsync-on-rewrite": "implemented",
    # C server.c:setOOMScoreAdj snapshots the prior Linux process score,
    # applies the primary/replica/child class value, and restores it when
    # disabled. Rust has the same /proc owner, CONFIG SET rollback, role
    # transition, and BGSAVE-child paths; focused integration reads the child
    # server process to prove absolute, relative, and restore semantics.
    "oom-score-adj": "implemented",
    "oom-score-adj-values": "implemented",
    "max-new-tls-connections-per-cycle": "unsupported_build",
    # Linux uses IPPROTO_MPTCP listeners; unsupported kernels fail the explicit
    # capability request instead of binding TCP as a silent fallback.
    "mptcp": "implemented",
    # C server.c:6930-6943 and 4997-5000. The server writes the configured
    # PID only after listeners bind, retains cleanup ownership only after a
    # successful write, and removes it before the normal shutdown exit.
    "pidfile": "implemented",
    # CONFIG SET port defers an L5 listener transaction; the old listener is
    # retired only after the new bind succeeds. Black-box coverage reconnects
    # and PINGs through the new live TCP port.
    "port": "implemented",
    # The live parser limit applies to authenticated clients as well as the
    # stricter unauthenticated 16 KiB cap. Rust consumes it at every command
    # parse boundary and enforces C's 1 MiB configuration floor.
    "proto-max-bulk-len": "implemented",
    # C networking.c gates only non-loopback peers while the default ACL user
    # is nopass. Rust reads that actual ACL-user state at accept time rather
    # than using the legacy requirepass approximation; focused lifecycle tests
    # cover both password and loopback exemptions.
    "protected-mode": "implemented",
    # Replica upstream and migration handshakes consume these live credentials.
    # ReplicaLink coverage verifies primaryuser selects two-argument AUTH and
    # primaryauth preserves binary passwords before PING/REPLCONF proceeds.
    "primaryauth": "implemented",
    "primaryuser": "implemented",
    # C validates and immediately reapplies the modifiable process-title
    # template. Rust uses the same validation/refresh boundary; Linux
    # integration observes CONFIG SET rewriting the live /proc cmdline.
    "proc-title-template": "implemented",
    # requirepass is the default ACL user's password contract, not merely a
    # legacy byte field. Startup and CONFIG SET synchronise nopass/password
    # hashes, and new/RESET connections derive their authentication state from
    # that same ACL user. Black-box coverage verifies NOAUTH and default AUTH.
    "requirepass": "implemented",
    # CONFIG SET replaces the live save-param set without forking immediately;
    # server cron consumes the new dirty/time policy through should_auto_bgsave.
    "save": "implemented",
    # Shutdown orchestration consumes this live timeout only while a replica is
    # behind: it pauses writes and establishes the exact catchup deadline; zero
    # bypasses the wait and proceeds immediately.
    "shutdown-timeout": "implemented",
    # C serverCron maps SIGTERM/SIGINT through their separate modifiable flag
    # sets before entering prepareForShutdown. Rust consumes the live setting
    # from the safe signal boundary, stages it on ShutdownState, and drives the
    # shared shutdown pipeline. Black-box coverage proves each CONFIG SET
    # `nosave` policy suppresses the corresponding signal-triggered RDB save.
    "shutdown-on-sigint": "implemented",
    "shutdown-on-sigterm": "implemented",
    # C debug.c applyWatchdogPeriod clamps a nonzero watchdog to at least two
    # current cron ticks and immediately replaces the one-shot SIGALRM timer.
    # Rust normalizes against the live hz at the registry boundary, then L4
    # applies the timer update. Focused black-box coverage observes the clamp,
    # the SIGALRM diagnostic during DEBUG SLEEP, and continued availability.
    "watchdog-period": "implemented",
    # C debug.c chooses _exit(1) for Valgrind-style runs and abort() otherwise.
    # Rust's live config setter and startup assembly update the shared panic
    # hook policy; DEBUG PANIC coverage observes both SIGABRT by default and
    # exit status 1 after CONFIG SET use-exit-on-panic yes.
    "use-exit-on-panic": "implemented",
    # C memory_prefetch.c batches parsed command keys. Rust previews only the
    # existing authenticated pipeline, warms Db::get_no_expire lookups under
    # the same 0-128 key budget, and exposes/resetstats-compatible counters.
    "prefetch-batch-max-size": "implemented",
    # C replication.c:2636-2671. Replica full-sync removes its received RDB
    # only when no RDB/AOF persistence remains enabled.
    "rdb-del-sync-files": "implemented",
    # C loading yields at byte boundaries during RDB/AOF and replica loading.
    # Rust threads the bounded setting into startup hosted loads and both
    # replica receive paths, whose focused shutdown/progress test covers it.
    "loading-process-events-interval-bytes": "implemented",
    # C applies this signed debug delay after every loaded RDB key and AOF
    # command. Rust passes the live signed value through startup/replica/RDB
    # preamble decoding and AOF replay, with the same sampled negative delay.
    "key-load-delay": "implemented",
    # C latency.c samples only at or above this live millisecond threshold.
    # Rust applies the same nonnegative integer setting to LatencyState during
    # startup assembly and CONFIG SET; command and cron paths use that gate.
    "latency-monitor-threshold": "implemented",
    # C tracks per-command duration distributions when latency-tracking is
    # enabled and INFO reads configured percentiles from them. Rust records
    # bounded per-command microsecond histograms on normal, blocked, and
    # replication completion paths; the default is yes and live disable skips
    # subsequent samples.
    "latency-tracking": "implemented",
    "latency-tracking-info-percentiles": "implemented",
    # Every mutating list command reads this live node-size policy and updates
    # list encoding state. Integration coverage crosses the configured limit,
    # persists/reloads, and observes listpack/quicklist transitions.
    "list-max-listpack-size": "implemented",
    # C rdb.c:1573-1580. RdbSaveOptions carries this setting to the atomic
    # snapshot writer, which syncs completed 32 MiB autosync intervals.
    "rdb-save-incremental-fsync": "implemented",
    # C rdb.c version acceptance is selected by this live strict/relaxed
    # policy. Rust passes it into startup, AOF-preamble, replica, and RESTORE
    # RDB decode owners; focused startup coverage loads a checksum-valid
    # future-version header only with the relaxed setting.
    "rdb-version-check": "implemented",
    # C rdb.c:1487 and 1521-1525. The persistent snapshot owner threads the
    # immutable startup setting into RDB serialization: disabled checksums
    # retain the eight-byte trailer as zero, while replication keeps its
    # separately negotiated checksum path. Focused BGSAVE coverage observes
    # the resulting on-disk trailer.
    "rdbchecksum": "implemented",
    # C rdb.c:500-524 applies this live policy to every RDB raw string. Rust
    # threads the config through persistent snapshots, AOF RDB preambles,
    # DUMP/MIGRATE, and every primary full-sync producer (in-process and
    # forked/streaming). The encoder preserves C's integer/LZF threshold and
    # fallback, and focused tests cover BGSAVE, DUMP, AOF preambles, and
    # streaming replication output.
    "rdbcompression": "implemented",
    # C debugDelay accepts signed microseconds: negatives sample a one-us
    # sleep once per absolute-value calls. Rust preserves that signed CONFIG
    # surface and feeds every RDB/AOF rewrite serialization path through the
    # equivalent interruptible delay helper.
    "rdb-key-save-delay": "implemented",
    # Replica upstream connects use their own IPPROTO_MPTCP transport path.
    "repl-mptcp": "implemented",
    # C server.c:4585-4605. A replica AOF flush error is fatal by default;
    # this option records and continues instead. Primary durability policy is
    # unaffected.
    "replica-ignore-disk-write-errors": "implemented",
    # C replication.c:2476 and rdb.c:3191. Direct replica full-sync removes
    # old values before decoding and hands their destruction to L5 lazyfree.
    "replica-lazy-flush": "implemented",
    # Replication settings below are consumed by L5 handshake/full-sync,
    # reconnect, eviction, and command gates. Focused replication unit and
    # integration coverage exercises diskless batching, timeout/backlog, and
    # replica read/write behavior rather than CONFIG surface alone.
    "repl-backlog-size": "implemented",
    "repl-backlog-ttl": "implemented",
    # Native replication negotiates LZ4 and compresses full-sync and continuous
    # streams. ZSTD remains an explicitly rejected optional build capability.
    "repl-compression": "implemented",
    "repl-disable-tcp-nodelay": "implemented",
    "repl-diskless-load": "implemented",
    "repl-diskless-sync": "implemented",
    "repl-diskless-sync-delay": "implemented",
    "repl-diskless-sync-max-replicas": "implemented",
    "repl-ping-replica-period": "implemented",
    "repl-timeout": "implemented",
    "replica-announce-ip": "implemented",
    "replica-announce-port": "implemented",
    "replica-announced": "implemented",
    "replica-ignore-maxmemory": "implemented",
    "replica-priority": "implemented",
    "replica-read-only": "implemented",
    "replica-serve-stale-data": "implemented",
    # C networking.c:939-951 panics only when a read-only replica receives a
    # propagated error under either panic mode, or when AOF replay uses the
    # stronger `panic` mode. Rust centralizes that policy in ReplicationConfig,
    # applies it to both replica dispatch and hosted AOF replay, and verifies
    # the writable/AOF distinctions plus transaction continuation.
    "propagation-error-behavior": "implemented",
    "replicaof": "startup_only",
    "req-res-logfile": "conditional_build",
    # C server.c:7838. The parsed list is applied to the main event-loop
    # thread after startup assembly; Linux errors are surfaced as warnings.
    "server-cpulist": "implemented",
    # C config.c:3351 and server.c:7762. The immutable startup gate controls
    # whether the validated proc-title template may overwrite argv after the
    # listening address and server mode are known.
    "set-proc-title": "implemented",
    "socket-mark-id": "unsupported_platform",
    # C t_stream.c:526-565 and config.c:3550. Rust models the listpack-node
    # boundary during approximate XADD/XTRIM trimming using field-byte size.
    "stream-node-max-bytes": "implemented",
    # Rust models C listpack macro nodes logically rather than storing C's
    # physical listpack nodes. XADD/XTRIM consume this live entry cap when
    # choosing approximate-trim boundaries; black-box coverage sets it to one
    # and observes MAXLEN ~ 2 retain exactly two of three appended entries.
    "stream-node-max-entries": "implemented",
    # SADD and set-store paths consume these three live thresholds to select
    # and migrate concrete set encodings. Black-box threshold crossings cover
    # intset/listpack entry and value boundaries.
    "set-max-intset-entries": "implemented",
    "set-max-listpack-entries": "implemented",
    "set-max-listpack-value": "implemented",
    # C's failed-BGSAVE write gate is evaluated before each write. Rust's
    # pre-dispatch owner reads the live bool, and black-box coverage verifies
    # the default MISCONF plus runtime disabling after the same failure.
    "stop-writes-on-bgsave-error": "implemented",
    # C server.c:7435-7502. Startup supports explicit/auto systemd notify and
    # the Upstart SIGSTOP handshake; a successful supervisor disables daemon
    # forking and is exposed through INFO's process_supervised state.
    "supervised": "implemented",
    # C's immutable listen queue length is consumed only while opening a
    # listener. Rust applies it before startup binds the first TCP listener;
    # L1 transport coverage verifies the configured listen backlog.
    "tcp-backlog": "startup_only",
    # C socket.c:332 applies this only to each newly accepted TCP client.
    # Rust L5 performs the same post-accept operation using the live setting,
    # and L1 transport maps zero to disabled or applies SO_KEEPALIVE plus TCP idle.
    "tcp-keepalive": "implemented",
    # CONFIG SET updates the live tracking-table bound. Post-command tracking
    # eviction removes keys above that bound and emits each removed key to all
    # owners; focused state and setter tests freeze both halves.
    "tracking-table-max-keys": "implemented",
    # C clientsCron disconnects idle ordinary clients while preserving Pub/Sub,
    # blocked, and replication clients. Rust's background client cron owns the
    # same live timeout policy and has a focused lifecycle regression.
    "timeout": "implemented",
    "syslog-enabled": "implemented",
    "syslog-facility": "implemented",
    "syslog-ident": "implemented",
    # Server-side TLS is backed by the L1 nonblocking OpenSSL transport. These
    # settings rebuild the context transactionally and rebind tls-port only
    # after its certificate/key pair validates; client-auth is verified at the
    # actual handshake boundary.
    "tls-auth-clients": "implemented",
    "tls-auth-clients-user": "implemented",
    # The TLS matrix verifies separate algorithm slots, per-identity chain and
    # password loading, atomic server/client publication, background reload,
    # certificate validity, and INFO attribution against the loaded batch.
    "tls-alt-cert-file": "implemented",
    "tls-alt-key-file": "implemented",
    "tls-alt-key-file-pass": "implemented",
    "tls-auto-reload-interval": "implemented",
    "tls-ca-cert-dir": "implemented",
    "tls-ca-cert-file": "implemented",
    "tls-cert-file": "implemented",
    "tls-ciphers": "implemented",
    "tls-ciphersuites": "implemented",
    "tls-client-cert-file": "implemented",
    "tls-client-key-file": "implemented",
    "tls-client-key-file-pass": "implemented",
    "tls-cluster": "implemented",
    "tls-dh-params-file": "unsupported_build",
    "tls-key-file": "implemented",
    "tls-key-file-pass": "implemented",
    "tls-port": "implemented",
    "tls-prefer-server-ciphers": "implemented",
    "tls-protocols": "implemented",
    "tls-replication": "implemented",
    "tls-session-cache-size": "implemented",
    "tls-session-cache-timeout": "unsupported_build",
    "tls-session-caching": "implemented",
}


def read_c_config_entries(path):
    with open(str(path), "r") as source:
        matches = CONFIG_MACRO_RE.findall(source.read())
    entries = {CConfigEntry(name, alias or None) for name, alias in matches}
    return sorted(entries, key=lambda entry: entry.name)


def read_registry_names(items_dir):
    names = set()
    for directory, _, filenames in os.walk(str(items_dir)):
        for filename in filenames:
            if not filename.endswith(".rs"):
                continue
            with open(os.path.join(directory, filename), "r") as source:
                text = source.read()
            names.update(REGISTRY_NAME_RE.findall(text))
            names.update(MACRO_NAME_RE.findall(text))
    return names


def read_registry_aliases(items_dir):
    aliases = set()
    for directory, _, filenames in os.walk(str(items_dir)):
        for filename in filenames:
            if not filename.endswith(".rs"):
                continue
            with open(os.path.join(directory, filename), "r") as source:
                aliases.update(REGISTRY_ALIAS_RE.findall(source.read()))
    return aliases


def classify(name, registry_names):
    if name.startswith("rdma-"):
        return "rdma_excluded"
    if name in EXPLICIT_STATUS:
        return EXPLICIT_STATUS[name]
    if name in registry_names:
        return "surface_registered"
    return "unclassified"


def build_config_parity_report(entries, registry_names):
    return [
        CapabilityEntry(entry.name, entry.alias, classify(entry.name, registry_names))
        for entry in entries
    ]


def command_check_generated_metadata(_args: argparse.Namespace, ctx: RepoContext) -> int:
    root = ctx.root
    json_dir = root / "thirdparty/valkey/src/commands"
    generated_root = root / "src/framework/generated"
    generated = generated_root / "commands"
    if os.path.lexists(str(generated_root / "commands.rs")):
        raise generated_metadata_error("legacy commands.rs still exists")
    for required in (generated / "mod.rs", generated / "catalog.rs"):
        if not required.is_file():
            raise generated_metadata_error("{} is missing".format(required.relative_to(generated_root)))
    stems = sorted(path.stem.replace("-", "_") for path in json_dir.glob("*.json"))
    duplicates = sorted(name for name in set(stems) if stems.count(name) > 1)
    if duplicates:
        raise generated_metadata_error("normalized module collision: {}".format(" ".join(duplicates)))
    actual = sorted(path.stem for path in generated.glob("*.rs") if path.name not in ("mod.rs", "catalog.rs"))
    if stems != actual:
        raise generated_metadata_error("generated command files do not match command JSON files")
    for json_path in sorted(json_dir.glob("*.json")):
        module = json_path.stem.replace("-", "_")
        source = generated / "{}.rs".format(module)
        first = read_text(source).splitlines()[0] if source.is_file() and read_text(source).splitlines() else ""
        expected = "// Auto-generated by build.rs from thirdparty/valkey/src/commands/{}.json. Do not edit.".format(json_path.stem)
        if first != expected:
            raise generated_metadata_error("incorrect source header for {}.rs".format(module))
    expected_fragments = (
        (generated / "get.rs", 'acl_categories: &["FAST", "READ", "STRING"]', "GET ACL categories were not generated"),
        (generated / "get.rs", "flags: (CMD_READONLY | CMD_FAST) as CmdFlags", "GET command flags were not preserved"),
        (generated / "get.rs", "first_key: 1", "GET first-key metadata was not preserved"),
        (generated / "get.rs", "last_key: 1", "GET last-key metadata was not preserved"),
        (generated / "get.rs", "key_step: 1", "GET key-step metadata was not preserved"),
        (generated / "get.rs", 'key_flags: &["RO", "access"]', "GET key-spec flags were not preserved"),
        (generated / "config_get.rs", 'container: Some("config")', "CONFIG GET container metadata was not preserved"),
        (generated / "catalog.rs", "pub const ALL_COMMAND_META: &[&CommandMeta]", "ALL_COMMAND_META catalog is missing"),
        (generated / "catalog.rs", "pub fn acl_command_meta", "ACL metadata lookup is missing"),
    )
    for source, fragment, message in expected_fragments:
        if fragment not in read_text(source):
            raise generated_metadata_error(message)

    with tempfile.TemporaryDirectory(prefix="versal-generated-") as directory:
        fixture = Path(directory)
        project = fixture / "project"
        (project / "src/framework/commands").mkdir(parents=True)
        for module in ("command_runtime", "introspection", "key_extractors", "pattern"):
            write_fixture(
                project,
                "src/framework/commands/{}.rs".format(module),
                "// Static module stub for the metadata-only fixture.\n",
            )
        command_dir = project / "thirdparty/valkey/src/commands"
        command_dir.mkdir(parents=True)
        shutil.copy2(str(root / "build.rs"), str(project / "build.rs"))
        shutil.copy2(str(root / "src/framework/flag_names.rs"), str(project / "src/framework/flag_names.rs"))
        shutil.copy2(str(json_dir / "get.json"), str(command_dir / "get.json"))
        shutil.copy2(str(json_dir / "config-get.json"), str(command_dir / "config-get.json"))
        write_fixture(
            project,
            "Cargo.toml",
            "[package]\nname = \"generated-command-metadata-fixture\"\nversion = \"0.0.0\"\nedition = \"2021\"\nbuild = \"build.rs\"\n\n[lib]\npath = \"src/lib.rs\"\n\n[[bin]]\nname = \"versal-server\"\npath = \"src/bin.rs\"\n\n[build-dependencies]\ncc = \"1\"\nserde_json = \"1\"\n",
        )
        write_fixture(project, "src/lib.rs", "// generated command metadata fixture\n")
        write_fixture(project, "src/bin.rs", "fn main() {}\n")
        target = fixture / "target"

        def cargo_check() -> subprocess.CompletedProcess:
            return run_command(["cargo", "check", "--quiet", "--manifest-path", str(project / "Cargo.toml"), "--target-dir", str(target)], capture=True)

        first = cargo_check()
        if first.returncode != 0:
            sys.stderr.write(first.stderr)
            raise generated_metadata_error("valid fixture did not compile")
        generated_fixture = project / "src/framework/generated/commands"
        first_snapshot = generated_snapshot(generated_fixture)
        run_command(["cargo", "clean", "--quiet", "--manifest-path", str(project / "Cargo.toml"), "--target-dir", str(target)])
        second = cargo_check()
        if second.returncode != 0 or first_snapshot != generated_snapshot(generated_fixture):
            raise generated_metadata_error("two consecutive generations produced different output")

        invalid_cases = (
            ('{"GET":{"arity":2,"acl_categories":"READ"}}\n', "get.json: acl_categories must be an array of strings"),
            ('{"GET":{"arity":2,"acl_categories":["READ",7]}}\n', "get.json: acl_categories must be an array of strings"),
            ('{"GET":{},"SET":{}}\n', "get.json: expected exactly one top-level command object"),
            ('{"GET":\n', "cannot parse {}".format(command_dir / "get.json")),
        )
        get_json = command_dir / "get.json"
        for content, expected in invalid_cases:
            get_json.write_text(content, encoding="utf-8")
            run_command(["cargo", "clean", "--quiet", "--manifest-path", str(project / "Cargo.toml"), "--target-dir", str(target)])
            result = cargo_check()
            if result.returncode == 0 or expected not in result.stderr:
                raise generated_metadata_error("fixture failure did not report: {}".format(expected))
        for source in command_dir.glob("*.json"):
            source.unlink()
        (command_dir / "foo-bar.json").write_text('{"FOO":{"arity":1,"group":"generic"}}\n', encoding="utf-8")
        (command_dir / "foo_bar.json").write_text('{"BAR":{"arity":1,"group":"generic"}}\n', encoding="utf-8")
        run_command(["cargo", "clean", "--quiet", "--manifest-path", str(project / "Cargo.toml"), "--target-dir", str(target)])
        collision = cargo_check()
        if collision.returncode == 0 or "normalized command module collision for foo_bar:" not in collision.stderr:
            raise generated_metadata_error("fixture failure did not report normalized command collision")
    print("generated command metadata fixtures passed ({} commands)".format(len(stems)))
    return EXIT_OK


def command_check_config_parity(args: argparse.Namespace, ctx: RepoContext) -> int:
    entries = read_c_config_entries(ctx.root / "thirdparty/valkey/src/config.c")
    items_dir = ctx.root / "src/engine/extension/config/items"
    registry_names = read_registry_names(items_dir)
    registry_aliases = read_registry_aliases(items_dir)
    report = build_config_parity_report(entries, registry_names)
    unclassified = [entry.name for entry in report if entry.status == "unclassified"]
    alias_gaps = [
        entry.name + " -> " + entry.alias
        for entry in report
        if entry.alias
        and entry.status in ("implemented", "surface_registered")
        and entry.alias not in registry_aliases
    ]

    if args.report:
        for entry in report:
            alias = " (alias: %s)" % entry.alias if entry.alias else ""
            print("%s: %s%s" % (entry.name, entry.status, alias))
        print(
            "status totals: %s"
            % dict(sorted(Counter(entry.status for entry in report).items()))
        )

    if unclassified:
        print("unclassified C configs: %s" % ", ".join(unclassified), file=sys.stderr)
    if alias_gaps:
        print("registry alias gaps: %s" % ", ".join(alias_gaps), file=sys.stderr)
    return EXIT_FAILURE if unclassified or alias_gaps else EXIT_OK


def selftest_cli(_ctx: RepoContext) -> None:
    help_result = run_command([sys.executable, str(SCRIPT_PATH), "--help"], capture=True)
    _assert(help_result.returncode == EXIT_OK, "--help must exit 0")
    _assert("selftest" in help_result.stdout, "--help must list selftest")

    bad_result = run_command([sys.executable, str(SCRIPT_PATH), "not-a-command"], capture=True)
    _assert(bad_result.returncode == EXIT_USAGE, "unknown command must exit 2")

    _assert(RepoContext.from_env({}).root == DEFAULT_REPO_ROOT, "default root mismatch")
    with tempfile.TemporaryDirectory(prefix="versal-cli-selftest-") as directory:
        root = Path(directory)
        _assert(
            RepoContext.from_env({"VERSAL_REPO_ROOT": str(root)}).root == root.resolve(),
            "VERSAL_REPO_ROOT override mismatch",
        )


def selftest_boundaries(_ctx: RepoContext) -> None:
    with tempfile.TemporaryDirectory(prefix="versal-boundaries-") as directory:
        root = Path(directory)
        for name in ("foundation", "engine", "framework", "runtime"):
            write_fixture(root, "src/{}/lib.rs".format(name), "pub fn clean() {}\n")
        clean = invoke_cli(["check", "boundaries"], root)
        _assert(clean.returncode == EXIT_OK, clean.stderr)
        write_fixture(root, "src/foundation/lib.rs", "use crate::engine::command::CommandArgs;\n")
        bad = invoke_cli(["check", "boundaries"], root)
        _assert(bad.returncode == EXIT_FAILURE, "foundation upward import must fail")


def selftest_directory_layout(ctx: RepoContext) -> None:
    clean = invoke_cli(["check", "directory-layout"], ctx.root)
    _assert(clean.returncode == EXIT_OK, clean.stderr)
    with tempfile.TemporaryDirectory(prefix="versal-layout-") as directory:
        bad = invoke_cli(["check", "directory-layout"], Path(directory))
        _assert(bad.returncode == EXIT_FAILURE, "missing layout must fail")


def selftest_file_sizes(ctx: RepoContext) -> None:
    clean = invoke_cli(["check", "file-sizes"], ctx.root)
    _assert(clean.returncode == EXIT_OK, clean.stderr)
    with tempfile.TemporaryDirectory(prefix="versal-size-") as directory:
        root = Path(directory)
        write_fixture(root, "scripts/architecture-size-debt.txt", "# none\n")
        write_fixture(root, "src/huge.rs", "fn line() {}\n" * 4001)
        bad = invoke_cli(["check", "file-sizes"], root)
        _assert(bad.returncode == EXIT_FAILURE, "oversized Rust file must fail")


def selftest_host_dispatcher(ctx: RepoContext) -> None:
    clean = invoke_cli(["check", "host-dispatcher"], ctx.root)
    _assert(clean.returncode == EXIT_OK, clean.stderr)
    with tempfile.TemporaryDirectory(prefix="versal-host-") as directory:
        root = Path(directory)
        write_fixture(root, "src/engine/host.rs", "pub type HostDispatcher = fn();\n")
        write_fixture(root, "src/framework/host_tasks/rdb.rs", "pub struct RdbLoadTask;\n")
        write_fixture(root, "src/framework/host_tasks/aof.rs", "pub struct AofReplayTask;\n")
        write_fixture(root, "src/forbidden.rs", "static CURRENT_SCRIPT_CTX: usize = 0;\n")
        bad = invoke_cli(["check", "host-dispatcher"], root)
        _assert(bad.returncode == EXIT_FAILURE, "forbidden host global must fail")


def selftest_unsafe_boundaries(_ctx: RepoContext) -> None:
    with tempfile.TemporaryDirectory(prefix="versal-unsafe-root-") as directory:
        root = Path(directory)
        write_fixture(root, "scripts/unsafe-roots.txt", "src/allowed\n")
        write_fixture(root, "src/allowed/good.rs", "pub fn good() { unsafe {} }\n")
        write_fixture(root, "src/forbidden/bad.rs", "pub fn bad() { unsafe {} }\n")
        bad = invoke_cli(["check", "unsafe-boundaries"], root)
        _assert(bad.returncode == EXIT_FAILURE, "unsafe outside allowlist must fail")
        (root / "src/forbidden/bad.rs").unlink()
        clean = invoke_cli(["check", "unsafe-boundaries"], root)
        _assert(clean.returncode == EXIT_OK, clean.stderr)


def selftest_unsafe_safety(_ctx: RepoContext) -> None:
    with tempfile.TemporaryDirectory(prefix="versal-unsafe-safety-") as directory:
        root = Path(directory)
        write_fixture(
            root,
            "src/good.rs",
            "pub fn good() {\n    // SAFETY: fixture operation is inert.\n    unsafe {}\n}\n",
        )
        clean = invoke_cli(["check", "unsafe-safety"], root)
        _assert(clean.returncode == EXIT_OK, clean.stderr)
        write_fixture(root, "src/bad.rs", "pub fn bad() { unsafe {} }\n")
        bad = invoke_cli(["check", "unsafe-safety"], root)
        _assert(bad.returncode == EXIT_FAILURE, "missing SAFETY comment must fail")


def selftest_foundation_dependencies(_ctx: RepoContext) -> None:
    with tempfile.TemporaryDirectory(prefix="versal-foundation-") as directory:
        root = Path(directory)
        write_fixture(root, "src/foundation/memory/good.rs", "pub fn clean() {}\n")
        write_fixture(
            root,
            "src/foundation/io/good.rs",
            "use crate::foundation::memory::value::Type;\n",
        )
        clean = invoke_cli(["check", "foundation-dependencies", "--enforce"], root)
        _assert(clean.returncode == EXIT_OK, clean.stderr)
        write_fixture(
            root,
            "src/foundation/memory/bad.rs",
            "use crate::foundation::io::store::Robj;\n",
        )
        bad = invoke_cli(["check", "foundation-dependencies", "--enforce"], root)
        _assert(bad.returncode == EXIT_FAILURE, "memory to io dependency must fail")


def selftest_stage_binaries(_ctx: RepoContext) -> None:
    with tempfile.TemporaryDirectory(prefix="versal-stage-") as directory:
        root = Path(directory)
        cargo = root / "target/debug"
        valkey = root / "valkey/src"
        cargo.mkdir(parents=True)
        valkey.mkdir(parents=True)
        for binary in VERSAL_BINARIES:
            source = write_fixture(root, "target/debug/versal-{}".format(binary), "versal-{}\n".format(binary))
            source.chmod(0o755)
        write_fixture(root, "valkey/src/valkey-server", "original-server\n")
        original_cli = write_fixture(root, "original-cli", "original-cli\n")
        (valkey / "valkey-cli").symlink_to(original_cli)
        state = root / "state"
        staged = invoke_cli(["tcl", "binaries", "stage", str(cargo), str(valkey), str(state)], root)
        _assert(staged.returncode == EXIT_OK, staged.stderr)
        for binary in VERSAL_BINARIES:
            target = valkey / "valkey-{}".format(binary)
            _assert(not target.is_symlink(), "staged binary must be a regular file")
            _assert(read_text(target) == "versal-{}\n".format(binary), "staged content mismatch")
        concurrent = invoke_cli(["tcl", "binaries", "stage", str(cargo), str(valkey), str(root / "state2")], root)
        _assert(concurrent.returncode == EXIT_FAILURE, "concurrent staging must fail")
        restored = invoke_cli(["tcl", "binaries", "restore", str(valkey), str(state)], root)
        _assert(restored.returncode == EXIT_OK, restored.stderr)
        _assert(read_text(valkey / "valkey-server") == "original-server\n", "regular file not restored")
        _assert((valkey / "valkey-cli").is_symlink(), "symlink not restored")
        for binary in ("benchmark", "check-aof", "check-rdb", "sentinel"):
            _assert(not os.path.lexists(str(valkey / "valkey-{}".format(binary))), "absent binary left behind")


def selftest_tcl_module_fixtures(_ctx: RepoContext) -> None:
    with tempfile.TemporaryDirectory(prefix="versal-tcl-module-") as directory:
        root = Path(directory)
        valkey = root / "valkey"
        modules = valkey / "tests/modules"
        modules.mkdir(parents=True)
        write_fixture(
            root,
            "valkey/tests/unit/moduleapi/redismodule_onload.tcl",
            "set testmodule [file normalize tests/modules/hellovalkey.so]\n",
        )
        write_fixture(root, "valkey/tests/unit/quit.tcl", "test {quit} { return OK } {OK}\n")
        fake_make = write_fixture(
            root,
            "fake-make.py",
            "#!/usr/bin/env python3\nimport os, pathlib, sys\n"
            "pathlib.Path(os.environ['MAKE_LOG']).write_text(' '.join(sys.argv[1:]) + '\\n')\n"
            "target = os.environ.get('TCL_FIXTURE_DIR')\n"
            "if target: pathlib.Path(target, 'tests/modules/hellovalkey.so').touch()\n",
        )
        fake_make.chmod(0o755)
        env = dict(os.environ)
        env.update({"VERSAL_REPO_ROOT": str(root), "MAKE": str(fake_make), "MAKE_LOG": str(root / "make.log")})
        missing = run_command(
            [sys.executable, str(SCRIPT_PATH), "tcl", "ensure-module-fixtures", str(valkey), "unit/moduleapi/redismodule_onload"],
            env=env,
            capture=True,
        )
        _assert(missing.returncode == EXIT_FAILURE, "missing module fixture must fail")
        env["TCL_FIXTURE_DIR"] = str(valkey)
        built = run_command(
            [sys.executable, str(SCRIPT_PATH), "tcl", "ensure-module-fixtures", str(valkey), "unit/moduleapi/redismodule_onload"],
            env=env,
            capture=True,
        )
        _assert(built.returncode == EXIT_OK, built.stderr)
        _assert("-C {}".format(modules) in read_text(root / "make.log"), "module make directory mismatch")


def selftest_ci_commands(ctx: RepoContext) -> None:
    accepted = invoke_cli(["ci", "performance", "--baseline-ms", "100", "--current-ms", "110"], ctx.root)
    _assert(accepted.returncode == EXIT_OK, accepted.stderr)
    rejected = invoke_cli(["ci", "performance", "--baseline-ms", "100", "--current-ms", "111"], ctx.root)
    _assert(rejected.returncode == EXIT_FAILURE, "performance regression must fail")
    invalid = invoke_cli(["ci", "dual-channel", "--runs", "0"], ctx.root)
    _assert(invalid.returncode == EXIT_USAGE, "zero dual-channel runs must exit 2")


def selftest_repro_cluster(ctx: RepoContext) -> None:
    help_result = invoke_cli(["tcl", "repro-cluster", "--help"], ctx.root)
    _assert(help_result.returncode == EXIT_OK, help_result.stderr)
    _assert("--iters" in help_result.stdout, "repro help must document iterations")


def selftest_generated_metadata(ctx: RepoContext) -> None:
    result = invoke_cli(["check", "generated-command-metadata"], ctx.root)
    _assert(result.returncode == EXIT_OK, result.stderr)
    _assert("generated command metadata fixtures passed" in result.stdout, "missing metadata summary")


def selftest_module_layout(_ctx: RepoContext) -> None:
    with tempfile.TemporaryDirectory(prefix="versal-module-layout-") as directory:
        root = Path(directory)
        write_fixture(root, "Cargo.toml", "[package]\nname = \"versal\"\n")
        write_fixture(root, "src/lib.rs", "mod child;\n")
        write_fixture(root, "src/child.rs", "pub struct Duplicate { pub value: usize }\n")
        write_fixture(root, "src/other.rs", "pub struct Duplicate { pub text: String }\n")
        write_fixture(root, "src/legacy.rs", "pub fn legacy() {}\n")
        write_fixture(root, "src/framework/context.rs", "pub mod exposed;\n")
        write_fixture(root, "src/framework/context/exposed.rs", "pub fn state_mut() {}\n")
        run_command(["git", "init", "-q"], cwd=root, check=True)
        run_command(["git", "add", "src"], cwd=root, check=True)
        result = invoke_cli(["check", "module-layout"], root)
        _assert(result.returncode == EXIT_FAILURE, "module-layout must reject invalid fixtures")
        _assert("uncompiled" in result.stderr, "module-layout must report uncompiled sources")
        _assert("same public type name" in result.stderr, "module-layout must report duplicate types")
        _assert("public mutable context accessor" in result.stderr, "module-layout must reject public context mutators")


def populate_fake_binaries(root: Path, profile: str = "debug") -> Path:
    binary_dir = root / "target" / profile
    binary_dir.mkdir(parents=True, exist_ok=True)
    for binary in VERSAL_BINARIES:
        write_executable(root, "target/{}/versal-{}".format(profile, binary), "versal-{}-{}\n".format(profile, binary))
    return binary_dir


def selftest_runtest(ctx: RepoContext) -> None:
    with tempfile.TemporaryDirectory(prefix="versal-runtest-") as directory:
        root = Path(directory)
        shutil.copy2(str(ctx.root / "runtest"), str(root / "runtest"))
        (root / "runtest").chmod(0o755)
        populate_fake_binaries(root)
        valkey_src = root / "thirdparty/valkey/src"
        (root / "thirdparty/valkey/tests/unit").mkdir(parents=True)
        valkey_src.mkdir(parents=True)
        write_fixture(root, "thirdparty/valkey/tests/test_helper.tcl", "# fixture\n")
        write_fixture(root, "tests/unit/local-fixture.tcl", "# local fixture\n")
        cargo_log = root / "cargo.jsonl"
        tcl_log = root / "tcl.json"
        write_executable(
            root,
            "bin/cargo",
            "#!/usr/bin/env python3\nimport json, os, sys\n"
            "with open(os.environ['FIXTURE_CARGO_CALLS'], 'a') as out: out.write(json.dumps(sys.argv[1:]) + '\\n')\n",
        )
        write_executable(
            root,
            "bin/tclsh",
            "#!/usr/bin/env python3\nimport json, os, pathlib, sys\n"
            "pathlib.Path(os.environ['FIXTURE_TCL_CALL']).write_text(json.dumps({'cwd': os.getcwd(), 'args': sys.argv[1:], 'valkey_bin_dir': os.environ.get('VALKEY_BIN_DIR'), 'sndbuf': os.environ.get('VALKEY_TEST_TCP_SNDBUF_BYTES'), 'rcvbuf': os.environ.get('VALKEY_TEST_TCP_RCVBUF_BYTES')}))\n"
            "sys.exit(int(os.environ.get('FIXTURE_TCL_EXIT', '0')))\n",
        )
        env = dict(os.environ)
        env.update({
            "PATH": str(root / "bin") + os.pathsep + env.get("PATH", ""),
            "FIXTURE_CARGO_CALLS": str(cargo_log),
            "FIXTURE_TCL_CALL": str(tcl_log),
        })
        result = run_command([str(root / "runtest")], cwd=root, env=env, capture=True)
        _assert(result.returncode == EXIT_OK, result.stderr)
        cargo_calls = [json.loads(line) for line in read_text(cargo_log).splitlines()]
        _assert(cargo_calls == [["build", "--bins"]], "unexpected Cargo calls")
        call = json.loads(read_text(tcl_log))
        _assert(os.path.realpath(call["cwd"]) == os.path.realpath(str(root / "thirdparty/valkey")), "wrong Tcl cwd")
        _assert(call["valkey_bin_dir"] is None, "VALKEY_BIN_DIR must be unset")
        _assert(
            call["sndbuf"] == "65536" and call["rcvbuf"] == "87380",
            "runtest must inject the scoped test-only TCP buffer settings",
        )
        _assert(call["args"] == ["tests/test_helper.tcl", "--skipunit", "unit/obuf-limits", "--skipunit", "unit/oom-score-adj", "--skip-leaks", "--baseport", "5111", "--portcount", "8000", "--timeout", "360", "--stop", "--dump-logs", "--dont-clean"], "unexpected Tcl invocation")
        env["VERSAL_TCL_DENY_SLOW"] = "1"
        result = run_command([str(root / "runtest")], cwd=root, env=env, capture=True)
        _assert(result.returncode == EXIT_OK, result.stderr)
        call = json.loads(read_text(tcl_log))
        _assert(call["args"] == ["tests/test_helper.tcl", "--tags", "-slow", "--skipunit", "unit/obuf-limits", "--skipunit", "unit/oom-score-adj", "--skip-leaks", "--baseport", "5111", "--portcount", "8000", "--timeout", "360", "--stop", "--dump-logs", "--dont-clean"], "unexpected Tcl invocation with slow tag denied")
        env.pop("VERSAL_TCL_DENY_SLOW", None)
        for binary in VERSAL_BINARIES:
            _assert(read_text(valkey_src / "valkey-{}".format(binary)) == "versal-debug-{}\n".format(binary), "binary not staged")
        env["FIXTURE_TCL_EXIT"] = "23"
        failed = run_command([str(root / "runtest")], cwd=root, env=env, capture=True)
        _assert(failed.returncode == 23, "Tcl exit code was not preserved")
        cargo_log.unlink()
        rejected = run_command([str(root / "runtest"), "--probe", "unit/local-fixture"], cwd=root, env=env, capture=True)
        _assert(rejected.returncode == EXIT_USAGE, "runtest arguments must be rejected")
        _assert(not cargo_log.exists(), "argument rejection ran Cargo")


def selftest_runtest_sentinel(ctx: RepoContext) -> None:
    with tempfile.TemporaryDirectory(prefix="versal-sentinel-") as directory:
        root = Path(directory)
        shutil.copy2(str(ctx.root / "runtest-sentinel"), str(root / "runtest-sentinel"))
        (root / "runtest-sentinel").chmod(0o755)
        runner = read_text(root / "runtest-sentinel")
        _assert('temporary="$destination.tmp.$$"' in runner, "Sentinel binary staging must use a process-unique temporary path")
        _assert('mv -f "$temporary" "$destination"' in runner, "Sentinel binary staging must replace the destination inode atomically")
        populate_fake_binaries(root)
        valkey = root / "thirdparty/valkey"
        (valkey / "src").mkdir(parents=True)
        (valkey / "tests/sentinel/tmp/preexisting").mkdir(parents=True)
        write_fixture(root, "thirdparty/valkey/tests/sentinel/run.tcl", "# sentinel fixture\n")
        write_fixture(root, "thirdparty/valkey/tests/sentinel/tmp/preexisting/log.txt", "preexisting\n")
        cargo_log = root / "cargo.json"
        tcl_log = root / "tcl.json"
        write_executable(
            root,
            "bin/cargo",
            "#!/usr/bin/env python3\nimport json, os, pathlib, sys\npathlib.Path(os.environ['FIXTURE_CARGO_CALL']).write_text(json.dumps({'cwd': os.getcwd(), 'args': sys.argv[1:]}))\n",
        )
        write_executable(
            root,
            "bin/tclsh",
            "#!/usr/bin/env python3\nimport json, os, pathlib, sys\n"
            "pathlib.Path(os.environ['FIXTURE_TCL_CALL']).write_text(json.dumps({'cwd': os.getcwd(), 'args': sys.argv[1:], 'valkey_bin_dir': os.environ.get('VALKEY_BIN_DIR')}))\n"
            "pathlib.Path('tests/sentinel/tmp/sentinel_0').mkdir(parents=True, exist_ok=True)\n"
            "pathlib.Path('tests/sentinel/tmp/sentinel_0/log.txt').write_text('fixture-log\\n')\n"
            "sys.exit(int(os.environ.get('FIXTURE_TCL_EXIT', '0')))\n",
        )
        env = dict(os.environ)
        env.update({"PATH": str(root / "bin") + os.pathsep + env.get("PATH", ""), "FIXTURE_CARGO_CALL": str(cargo_log), "FIXTURE_TCL_CALL": str(tcl_log)})
        result = run_command([str(root / "runtest-sentinel")], cwd=root, env=env, capture=True)
        _assert(result.returncode == EXIT_OK, result.stderr)
        call = json.loads(read_text(tcl_log))
        _assert(os.path.realpath(call["cwd"]) == os.path.realpath(str(valkey)), "wrong Sentinel cwd")
        _assert(call["args"] == ["tests/sentinel/run.tcl", "--skip-leaks", "--dont-clean"], "wrong Sentinel args")
        _assert(call["valkey_bin_dir"] is None, "Sentinel VALKEY_BIN_DIR must be unset")
        _assert((valkey / "tests/sentinel/tmp/preexisting/log.txt").is_file(), "preexisting Sentinel log removed")
        env["FIXTURE_TCL_EXIT"] = "23"
        failed = run_command([str(root / "runtest-sentinel")], cwd=root, env=env, capture=True)
        _assert(failed.returncode == 23, "Sentinel Tcl exit code not preserved")
        rejected = run_command([str(root / "runtest-sentinel"), "--probe"], cwd=root, env=env, capture=True)
        _assert(rejected.returncode == EXIT_USAGE, "Sentinel arguments must be rejected")


def selftest_runtest_moduleapi(ctx: RepoContext) -> None:
    with tempfile.TemporaryDirectory(prefix="versal-moduleapi-") as directory:
        root = Path(directory)
        shutil.copy2(str(ctx.root / "runtest-moduleapi"), str(root / "runtest-moduleapi"))
        (root / "runtest-moduleapi").chmod(0o755)
        (root / "scripts").mkdir()
        shutil.copy2(str(SCRIPT_PATH), str(root / "scripts/versal-util.py"))
        populate_fake_binaries(root, "debug")
        populate_fake_binaries(root, "release")
        valkey = root / "thirdparty/valkey"
        valkey_src = valkey / "src"
        valkey_src.mkdir(parents=True)
        originals = {}  # type: Dict[str, str]
        for binary in VERSAL_BINARIES:
            content = "original-{}\n".format(binary)
            originals[binary] = content
            write_executable(root, "thirdparty/valkey/src/valkey-{}".format(binary), content)
        cargo_log = root / "cargo.jsonl"
        runner_log = root / "runner.json"
        write_executable(
            root,
            "bin/cargo",
            "#!/usr/bin/env python3\nimport json, os, sys\n"
            "with open(os.environ['FIXTURE_CARGO_CALLS'], 'a') as out: out.write(json.dumps(sys.argv[1:]) + '\\n')\n",
        )
        write_executable(
            root,
            "thirdparty/valkey/runtest-moduleapi",
            "#!/usr/bin/env python3\nimport json, os, pathlib, sys\n"
            "profile = os.environ['FIXTURE_PROFILE']\n"
            "fixture = pathlib.Path(os.environ['FIXTURE_ROOT'])\n"
            "for name in ('server','cli','benchmark','check-aof','check-rdb','sentinel'):\n"
            "    actual = pathlib.Path('src/valkey-' + name).read_text()\n"
            "    expected = pathlib.Path(fixture, 'target', profile, 'versal-' + name).read_text()\n"
            "    assert actual == expected\n"
            "pathlib.Path(os.environ['FIXTURE_RUNNER_CALL']).write_text(json.dumps({'cwd': os.getcwd(), 'args': sys.argv[1:], 'valkey_bin_dir': os.environ.get('VALKEY_BIN_DIR')}))\n"
            "sys.exit(int(os.environ.get('FIXTURE_RUNNER_EXIT', '0')))\n",
        )
        env = dict(os.environ)
        env.update({
            "PATH": str(root / "bin") + os.pathsep + env.get("PATH", ""),
            "FIXTURE_CARGO_CALLS": str(cargo_log),
            "FIXTURE_ROOT": str(root),
            "FIXTURE_PROFILE": "debug",
            "FIXTURE_RUNNER_CALL": str(runner_log),
        })
        result = run_command([str(root / "runtest-moduleapi")], cwd=root, env=env, capture=True)
        _assert(result.returncode == EXIT_OK, result.stderr)
        call = json.loads(read_text(runner_log))
        _assert(os.path.realpath(call["cwd"]) == os.path.realpath(str(valkey)), "wrong Module API cwd")
        _assert("--skipunit" in call["args"] and "unit/moduleapi/rdbloadsave" in call["args"], "Module API skips changed")
        for binary, content in originals.items():
            _assert(read_text(valkey_src / "valkey-{}".format(binary)) == content, "binary was not restored")
        env.update({"FIXTURE_PROFILE": "release", "FIXTURE_RUNNER_EXIT": "23"})
        failed = run_command([str(root / "runtest-moduleapi"), "--release", "--single", "unit/moduleapi/basics"], cwd=root, env=env, capture=True)
        _assert(failed.returncode == 23, "Module API failure code not preserved")
        cargo_calls = [json.loads(line) for line in read_text(cargo_log).splitlines()]
        _assert(["build", "--release", "--bins"] in cargo_calls, "release Cargo build missing")
        for binary, content in originals.items():
            _assert(read_text(valkey_src / "valkey-{}".format(binary)) == content, "failure did not restore binary")


def selftest_architecture(ctx: RepoContext) -> None:
    result = invoke_cli(["check", "architecture"], ctx.root)
    _assert(result.returncode == EXIT_OK, result.stderr)


def selftest_runtime_event_model(_ctx: RepoContext) -> None:
    with tempfile.TemporaryDirectory(prefix="versal-runtime-event-model-") as directory:
        root = Path(directory)
        write_fixture(root, "src/runtime/server/worker/reactor/mod.rs", "fn old() { producer_relay_pending(); }\n")
        write_fixture(root, "src/runtime/server/worker/reactor/dispatch.rs", "fn old() { sentinel_links.is_link_token(token); }\n")
        failures = _runtime_event_model_violations(root)
        _assert(any("producer_relay_pending" in failure for failure in failures), "retired identifier must be rejected")
        _assert(any("sentinel_links.is_link_token" in failure for failure in failures), "retired owner probe must be rejected")
        write_fixture(root, "src/runtime/server/worker/reactor/mod.rs", "fn current() { disk_sync_batch_target_writable(); }\n")
        write_fixture(root, "src/runtime/server/worker/reactor/dispatch.rs", "fn current() { dispatch_source(route); }\n")
        _assert(not _runtime_event_model_violations(root), "current event model fixture must be accepted")
        write_fixture(
            root,
            "src/runtime/driver/bad.rs",
            "use crate::runtime::server::worker::reactor::transport::RuntimeTransport;\n",
        )
        failures = _runtime_event_model_violations(root)
        _assert(any("runtime-driver" in failure for failure in failures), "driver host dependency must be rejected")
        write_fixture(
            root,
            "src/runtime/driver/bad.rs",
            "use crate::engine::command::CommandArgs;\n",
        )
        _assert(not _runtime_event_model_violations(root), "target-neutral driver fixture must be accepted")


def selftest_runtime_ownership(_ctx: RepoContext) -> None:
    with tempfile.TemporaryDirectory(prefix="versal-runtime-ownership-") as directory:
        root = Path(directory)
        write_fixture(root, "src/runtime/mod.rs", "pub mod driver;\nmod embedded;\nmod server;\n")
        write_fixture(root, "src/runtime/server/mod.rs", "struct ServerRuntime;\n")
        write_fixture(
            root,
            "src/runtime/server/supervisor/mod.rs",
            "struct ServerSupervisor;\n",
        )
        write_fixture(root, "src/runtime/server/shared.rs", "struct ServerShared;\n")
        write_fixture(
            root,
            "src/runtime/server/worker/runtime.rs",
            "struct ServerWorkerMachine;\nstruct Owner(RuntimeKernel<ServerWorkerMachine>);\n",
        )
        write_fixture(
            root,
            "src/runtime/server/worker/state.rs",
            "struct ServerWorkerState;\n",
        )
        _assert(
            not _runtime_ownership_violations(root),
            "worker-owned server layout fixture must be accepted",
        )
        write_fixture(
            root,
            "src/runtime/server/supervisor/mod.rs",
            "struct ServerSupervisor(Arc<Mutex<ServerWorkerState>>);\n",
        )
        failures = _runtime_ownership_violations(root)
        _assert(
            any("shared worker state lock" in failure for failure in failures),
            "shared worker state lock must be rejected",
        )
        write_fixture(
            root,
            "src/runtime/mod.rs",
            "pub mod driver;\nmod embedded;\nmod server;\nmod transport;\n",
        )
        failures = _runtime_ownership_violations(root)
        _assert(
            any("non-host modules" in failure for failure in failures),
            "flat runtime implementation modules must be rejected",
        )


def selftest_replication_offset_domains(_ctx: RepoContext) -> None:
    with tempfile.TemporaryDirectory(prefix="versal-replication-offsets-") as directory:
        root = Path(directory)
        backlog_methods = (
            "append_frame",
            "can_continue_from",
            "current_offset",
            "first_byte_offset",
            "min_protected_offset",
            "next_stream_offset",
            "read_from_cursor",
            "second_replid_offset",
            "set_min_protected_offset",
            "try_adopt_primary_from_rdb",
            "try_rdb_snapshot_boundary",
            "try_restore_primary_from_rdb",
        )
        primary_link_methods = (
            "backlog_protection_offset",
            "dual_main_backlog_pin_offset",
            "next_send_offset",
            "sent_offset",
        )
        runtime_methods = ("min_protected_backlog_offset",)

        def method_declarations(owner: str, names: Sequence[str]) -> str:
            return "\n".join(
                "impl {0} {{\n    pub fn\n    {1}\n    (\n        &self,\n    ) {{}}\n}}".format(
                    owner, name
                )
                for name in names
            )

        write_fixture(
            root,
            "src/engine/distributed/replication/backlog.rs",
            "pub struct\nOffsetRange { start: i64, end: i64 }\n"
            + method_declarations("ReplicationBacklog", backlog_methods)
            + "\n",
        )
        write_fixture(
            root,
            "src/framework/distributed/replication/primary_link.rs",
            "pub struct PrimaryLink {\n"
            "    next_send_offset:\n        i64,\n"
            "    sent_offset: i64,\n"
            "    dual_endoff_snapshot_offset: i64,\n"
            "}\n"
            + method_declarations("PrimaryLink", primary_link_methods)
            + "\n",
        )
        write_fixture(
            root,
            "src/framework/distributed/replication/runtime/view.rs",
            method_declarations("ReplicationRuntime", runtime_methods) + "\n",
        )
        rejected = invoke_cli(["check", "replication-offset-domains"], root)
        _assert(
            rejected.returncode == EXIT_FAILURE,
            "retired replication owner declarations must fail",
        )
        retired_declarations = (
            backlog_methods
            + primary_link_methods
            + runtime_methods
            + (
                "OffsetRange",
                "next_send_offset",
                "sent_offset",
                "dual_endoff_snapshot_offset",
            )
        )
        for identifier in retired_declarations:
            _assert(
                identifier in rejected.stderr,
                "missing retired declaration failure: {}".format(identifier),
            )

        write_fixture(
            root,
            "src/engine/distributed/replication/backlog.rs",
            "fn typed(\n"
            "    current_offset: i64,\n"
            "    clock: &Clock,\n"
            "    alias: &Alias,\n"
            "    batch: &mut Batch,\n"
            ") {\n"
            "    let _ = current_offset;\n"
            "    clock.current_offset();\n"
            "    alias.next_send_offset();\n"
            "    alias.append_frame(frame);\n"
            "    batch.append_frame(frame);\n"
            "    let _: external::OffsetRange;\n"
            "}\n",
        )
        write_fixture(
            root,
            "src/framework/distributed/replication/primary_link.rs",
            "fn typed(alias: &Alias) { alias.sent_offset(); }\n",
        )
        write_fixture(
            root,
            "src/framework/distributed/replication/runtime/view.rs",
            "fn typed(clock: &Clock) { clock.current_offset(); }\n",
        )
        write_fixture(
            root,
            "src/runtime/server/worker/distributed/propagation.rs",
            "struct Batch;\nimpl Batch { fn append_frame(&mut self, frame: Frame) {} }\n",
        )
        write_fixture(
            root,
            "src/unrelated.rs",
            "pub struct OffsetRange;\nimpl Clock { pub fn current_offset(&self) -> i64 { 0 } }\n",
        )
        accepted = invoke_cli(["check", "replication-offset-domains"], root)
        _assert(accepted.returncode == EXIT_OK, accepted.stderr)


def selftest_ci_support(ctx: RepoContext) -> None:
    result = invoke_cli(["check", "ci-support"], ctx.root)
    _assert(result.returncode == EXIT_OK, result.stderr)


def selftest_config_parity(_ctx: RepoContext) -> None:
    entries = [
        CConfigEntry("rdma-port", None),
        CConfigEntry("cluster-allow-pubsubshard-when-down", None),
        CConfigEntry("invented-config", None),
    ]
    report = build_config_parity_report(
        entries,
        {"cluster-allow-pubsubshard-when-down"},
    )
    _assert(
        [entry.status for entry in report]
        == ["rdma_excluded", "implemented", "unclassified"],
        "config parity classification changed",
    )


def selftest_script_entry_policy(_ctx: RepoContext) -> None:
    failures = script_entry_policy_failures(
        {
            "versal-util.py": "#!/usr/bin/env python3\n",
            "helper.py": "VALUE = 1\n",
        },
        {
            "Makefile": "python3 scripts/helper.py\n",
        },
    )
    _assert(
        failures
        == [
            "scripts/helper.py must be removed; versal-util.py is the sole Python file",
            "Makefile invokes non-entry script scripts/helper.py",
        ],
        "single-entry policy did not reject direct helper execution",
    )


def selftest_sentinel_support(ctx: RepoContext) -> None:
    result = invoke_cli(["check", "sentinel-support"], ctx.root)
    _assert(result.returncode == EXIT_OK, result.stderr)


SELFTESTS = {
    "architecture": selftest_architecture,
    "cli": selftest_cli,
    "boundaries": selftest_boundaries,
    "ci-commands": selftest_ci_commands,
    "ci-support": selftest_ci_support,
    "config-parity": selftest_config_parity,
    "directory-layout": selftest_directory_layout,
    "file-sizes": selftest_file_sizes,
    "host-dispatcher": selftest_host_dispatcher,
    "repro-cluster": selftest_repro_cluster,
    "replication-offset-domains": selftest_replication_offset_domains,
    "runtest": selftest_runtest,
    "runtime-event-model": selftest_runtime_event_model,
    "runtime-ownership": selftest_runtime_ownership,
    "runtest-moduleapi": selftest_runtest_moduleapi,
    "runtest-sentinel": selftest_runtest_sentinel,
    "script-entry-policy": selftest_script_entry_policy,
    "stage-binaries": selftest_stage_binaries,
    "tcl-module-fixtures": selftest_tcl_module_fixtures,
    "sentinel-support": selftest_sentinel_support,
    "unsafe-boundaries": selftest_unsafe_boundaries,
    "unsafe-safety": selftest_unsafe_safety,
    "foundation-dependencies": selftest_foundation_dependencies,
    "generated-command-metadata": selftest_generated_metadata,
    "module-layout": selftest_module_layout,
}  # type: Dict[str, Callable[[RepoContext], None]]


def command_selftest(args: argparse.Namespace, ctx: RepoContext) -> int:
    names = sorted(SELFTESTS) if args.name == "all" else [args.name]
    for name in names:
        test = SELFTESTS.get(name)
        if test is None:
            raise ToolError("unknown selftest: {}".format(name), EXIT_USAGE)
        test(ctx)
        print("{} selftest passed".format(name))
    print("selftests passed: {}".format(len(names)))
    return EXIT_OK


def run_selftest_names(ctx: RepoContext, names: Sequence[str]) -> int:
    for name in names:
        SELFTESTS[name](ctx)
        print("{} selftest passed".format(name))
    return EXIT_OK


def command_check_architecture(_args: argparse.Namespace, ctx: RepoContext) -> int:
    checks = (
        (command_check_boundaries, argparse.Namespace()),
        (command_check_file_sizes, argparse.Namespace()),
        (command_check_host_dispatcher, argparse.Namespace()),
        (command_check_runtime_event_model, argparse.Namespace()),
        (command_check_runtime_ownership, argparse.Namespace()),
        (command_check_replication_offset_domains, argparse.Namespace()),
        (command_check_unsafe_boundaries, argparse.Namespace()),
        (command_check_directory_layout, argparse.Namespace()),
        (command_check_module_layout, argparse.Namespace()),
        (command_check_unsafe_safety, argparse.Namespace()),
        (command_check_foundation_dependencies, argparse.Namespace(enforce=True)),
    )
    for function, arguments in checks:
        result = function(arguments, ctx)
        if result != EXIT_OK:
            return result
    return run_selftest_names(
        ctx,
        ("boundaries", "file-sizes", "host-dispatcher", "runtime-event-model", "runtime-ownership", "replication-offset-domains", "unsafe-boundaries", "directory-layout", "module-layout", "foundation-dependencies"),
    )


def script_entry_policy_failures(
    modules: Mapping[str, str], wiring: Mapping[str, str]
) -> List[str]:
    failures = []  # type: List[str]
    for name in sorted(modules):
        if name == "versal-util.py":
            continue
        failures.append(
            "scripts/{} must be removed; versal-util.py is the sole Python file".format(
                name
            )
        )
    for owner, content in sorted(wiring.items()):
        for line in content.splitlines():
            if "-m unittest" in line:
                continue
            for match in re.finditer(r"scripts/([A-Za-z0-9_-]+\.py)", line):
                if match.group(1) != "versal-util.py":
                    failures.append(
                        "{} invokes non-entry script {}".format(owner, match.group(0))
                    )
    return failures


def command_check_ci_support(_args: argparse.Namespace, ctx: RepoContext) -> int:
    failures = []  # type: List[str]
    scripts_root = ctx.root / "scripts"
    shell_scripts = sorted(path.relative_to(ctx.root).as_posix() for path in scripts_root.rglob("*.sh"))
    if shell_scripts:
        failures.append("legacy shell scripts remain: {}".format(", ".join(shell_scripts)))
    makefile = read_text(ctx.root / "Makefile")
    pipeline = read_text(ctx.root / ".github/workflows/ci.yml")
    docs_pipeline = read_text(ctx.root / ".github/workflows/docs.yml") if (ctx.root / ".github/workflows/docs.yml").exists() else ""
    runtest = read_text(ctx.root / "runtest")
    sentinel = read_text(ctx.root / "runtest-sentinel")
    moduleapi = read_text(ctx.root / "runtest-moduleapi")
    modules = {
        path.name: read_text(path) for path in sorted(scripts_root.glob("*.py"))
    }
    wiring = {
        ".github/workflows/ci.yml": pipeline,
        ".github/workflows/docs.yml": docs_pipeline,
        "Makefile": makefile,
        "runtest": runtest,
        "runtest-moduleapi": moduleapi,
        "runtest-sentinel": sentinel,
    }
    failures.extend(script_entry_policy_failures(modules, wiring))
    if "scripts/versal-util.py check ci-support" not in makefile:
        failures.append("Makefile does not route CI support through versal-util.py")
    install_node = pipeline.find("apt-get install -y --no-install-recommends nodejs")
    wasm_check = pipeline.find("make wasm-check")
    if install_node < 0:
        failures.append("CI wasm check does not install Node.js with apt")
    elif wasm_check < 0 or install_node > wasm_check:
        failures.append("CI must install Node.js before running wasm-check")
    rust_unit = pipeline.find("run: cargo test --lib")
    install_build_essential = pipeline.find("build-essential")
    install_clang = pipeline.find("clang")
    install_lld = pipeline.find("lld")
    if install_build_essential < 0:
        failures.append("Rust Check must install build-essential for runtime module tests")
    elif rust_unit < 0 or install_build_essential > rust_unit:
        failures.append("CI must install build-essential before Rust unit tests")
    if install_clang < 0:
        failures.append("Rust Check must install clang for runtime module tests")
    elif rust_unit < 0 or install_clang > rust_unit:
        failures.append("CI must install clang before Rust unit tests")
    if install_lld < 0:
        failures.append("Rust Check must install lld for runtime module tests")
    elif rust_unit < 0 or install_lld > rust_unit:
        failures.append("CI must install lld before Rust unit tests")
    if "CC: clang" not in pipeline:
        failures.append("Rust Check must force module fixture compilation through clang")
    module_fixture_sources = (
        ctx.root / "src/engine/extension/module/loader.rs",
        ctx.root / "src/framework/capabilities/module.rs",
        ctx.root / "src/framework/capabilities/module_loader_tests.rs",
        ctx.root / "tests/integration/module_ffi.rs",
    )
    for source in module_fixture_sources:
        source_text = read_text(source)
        if 'unwrap_or_else(|| "cc".into())' in source_text:
            failures.append(
                "{} module fixture compiler fallback must be clang".format(
                    source.relative_to(ctx.root).as_posix()
                )
            )
        if "-fuse-ld=lld" not in source_text:
            failures.append(
                "{} module fixture compiler must select lld on Linux".format(
                    source.relative_to(ctx.root).as_posix()
                )
            )
    if docs_pipeline and '"scripts/versal-util.py"' not in docs_pipeline:
        failures.append("documentation pipeline does not watch versal-util.py")
    if "python3 \"$ROOT_DIR/scripts/versal-util.py\" tcl binaries" not in moduleapi:
        failures.append("Module API runner does not use Python binary staging")
    for runner_name, content in (("runtest", runtest), ("runtest-sentinel", sentinel), ("runtest-moduleapi", moduleapi)):
        if re.search(r"export\s+VALKEY_BIN_DIR|VALKEY_BIN_DIR=.*(?:target|test_bin|BIN_DIR)", content):
            failures.append("{} must not export a Cargo binary directory".format(runner_name))
    if "--stop --dump-logs --dont-clean" not in runtest:
        failures.append("runtest must retain diagnostics by default")
    if "--skipunit unit/obuf-limits" not in runtest:
        failures.append("runtest must skip unit/obuf-limits until Bulk reply copy avoidance is implemented")
    if "--skipunit unit/oom-score-adj" not in runtest:
        failures.append("runtest must skip unit/oom-score-adj")
    if "VERSAL_TCL_DENY_SLOW" not in pipeline or "--tags -slow" not in runtest:
        failures.append("GitHub Tcl runner must deny upstream slow tests")
    for unit in ("maxmemory-basic", "maxmemory", "io-threads", "memefficiency"):
        if "--skipunit unit/{}".format(unit) in runtest:
            failures.append("runtest must execute enabled unit/{}".format(unit))
    if not (ctx.root / "runtest-sentinel").exists() or "./runtest-sentinel" not in makefile or "./runtest-sentinel" not in pipeline:
        failures.append("Sentinel runner wiring is incomplete")
    if "./runtest-moduleapi" not in pipeline or not re.search(r"^\s*\./runtest\s*$", pipeline, re.MULTILINE):
        failures.append("Tcl runner jobs are missing from CI")
    if "run: cargo test --lib" not in pipeline:
        failures.append("Rust Check must run cargo test --lib")
    if "actions: read" not in pipeline:
        failures.append("CI artifact download retries require actions: read permission")
    if "actions/download-artifact" in pipeline:
        failures.append("CI Tcl artifact downloads must use retryable gh run download")
    if "gh run download \"$GITHUB_RUN_ID\"" not in pipeline:
        failures.append("CI Tcl artifact download retry loop is missing")
    if "versal-debug-binaries.tar.gz" not in pipeline:
        failures.append("CI Tcl binaries must be packaged as a single tar archive")
    if docs_pipeline and "git submodule update --init --recursive" not in docs_pipeline and "submodules: recursive" not in docs_pipeline:
        failures.append("documentation CI must initialize submodules")
    if "$(MAKE) property" not in makefile or "test --test integration" not in makefile:
        failures.append("Makefile CI coverage is incomplete")
    if failures:
        for failure in failures:
            print("ci-support: {}".format(failure), file=sys.stderr)
        return EXIT_FAILURE
    return run_selftest_names(
        ctx,
        ("ci-commands", "tcl-module-fixtures", "runtest", "runtest-moduleapi", "stage-binaries"),
    )


def command_check_sentinel_support(_args: argparse.Namespace, ctx: RepoContext) -> int:
    return run_selftest_names(ctx, ("runtest-sentinel",))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="VersalKV repository tooling")
    subparsers = parser.add_subparsers(dest="group")
    subparsers.required = True

    selftest = subparsers.add_parser("selftest", help="run embedded tooling fixtures")
    selftest.add_argument("name", help="fixture name or 'all'")
    selftest.set_defaults(handler=command_selftest)

    check = subparsers.add_parser("check", help="run repository policy checks")
    check_commands = check.add_subparsers(dest="check_name")
    check_commands.required = True
    check_handlers = {
        "architecture": command_check_architecture,
        "boundaries": command_check_boundaries,
        "ci-support": command_check_ci_support,
        "directory-layout": command_check_directory_layout,
        "file-sizes": command_check_file_sizes,
        "generated-command-metadata": command_check_generated_metadata,
        "module-layout": command_check_module_layout,
        "host-dispatcher": command_check_host_dispatcher,
        "runtime-event-model": command_check_runtime_event_model,
        "runtime-ownership": command_check_runtime_ownership,
        "replication-offset-domains": command_check_replication_offset_domains,
        "sentinel-support": command_check_sentinel_support,
        "unsafe-boundaries": command_check_unsafe_boundaries,
        "unsafe-safety": command_check_unsafe_safety,
    }
    config_parity = check_commands.add_parser("config-parity")
    config_parity.add_argument("--report", action="store_true")
    config_parity.set_defaults(handler=command_check_config_parity)
    for name, handler in sorted(check_handlers.items()):
        child = check_commands.add_parser(name)
        child.set_defaults(handler=handler)
    foundation = check_commands.add_parser("foundation-dependencies")
    foundation.add_argument("--enforce", action="store_true")
    foundation.set_defaults(handler=command_check_foundation_dependencies)

    tcl = subparsers.add_parser("tcl", help="manage Tcl test tooling")
    tcl_commands = tcl.add_subparsers(dest="tcl_command")
    tcl_commands.required = True
    binaries = tcl_commands.add_parser("binaries")
    binary_commands = binaries.add_subparsers(dest="binary_action")
    binary_commands.required = True
    stage = binary_commands.add_parser("stage")
    stage.add_argument("cargo_bin_dir")
    stage.add_argument("valkey_src_dir")
    stage.add_argument("state_dir")
    stage.set_defaults(handler=command_tcl_binaries)
    restore = binary_commands.add_parser("restore")
    restore.add_argument("valkey_src_dir")
    restore.add_argument("state_dir")
    restore.set_defaults(handler=command_tcl_binaries)
    fixtures = tcl_commands.add_parser("ensure-module-fixtures")
    fixtures.add_argument("valkey_source_dir")
    fixtures.add_argument("units", nargs="*")
    fixtures.set_defaults(handler=command_tcl_module_fixtures)
    repro = tcl_commands.add_parser("repro-cluster")
    repro.add_argument("--probe", default="unit/cluster/base")
    repro.add_argument("--iters", type=int, default=40)
    repro.add_argument("--release", action="store_true")
    repro.add_argument("--out")
    repro.set_defaults(handler=command_tcl_repro_cluster)

    ci = subparsers.add_parser("ci", help="run CI support commands")
    ci_commands = ci.add_subparsers(dest="ci_command")
    ci_commands.required = True
    performance = ci_commands.add_parser("performance")
    performance.add_argument("--baseline-ms", type=float, required=True)
    performance.add_argument("--current-ms", type=float, required=True)
    performance.set_defaults(handler=command_ci_performance)
    dual = ci_commands.add_parser("dual-channel")
    dual.add_argument("--runs", type=int)
    dual.add_argument("--base-port", type=int)
    dual.set_defaults(handler=command_ci_dual_channel)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    previous_handlers = {}  # type: Dict[int, object]
    def _signal_handler(signum: int, _frame: object) -> None:
        raise SignalExit(128 + signum)
    for signum in (signal.SIGHUP, signal.SIGTERM):
        previous_handlers[signum] = signal.signal(signum, _signal_handler)
    try:
        args = parser.parse_args(argv)
        return int(args.handler(args, RepoContext.from_env()))
    except ToolError as error:
        print(error.message, file=sys.stderr)
        return error.code
    except KeyboardInterrupt:
        return 130
    except SignalExit as error:
        return error.code
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    sys.exit(main())
