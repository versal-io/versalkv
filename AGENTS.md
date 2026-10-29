# AGENTS.md

This file documents the development conventions for the `versal` crate -- a
pure-Rust Valkey-compatible server. The original C implementation lives in the
`thirdparty/valkey` submodule and is used as the reference behaviour.

## English-only source policy

- All owned source code must be written in English, including comments,
  doc-comments, string literals, log/error messages, test descriptions, and
  developer annotations.
- Only English text is allowed in owned source files. The `thirdparty/valkey`
  submodule is exempt from this source-code policy.

## Project overview

- **Goal**: Reimplement the Valkey server core in Rust while staying compatible
  with the RESP protocol. The main event thread owns all server state; on Linux
  the `polling` 3 crate provides real level-triggered readiness via epoll. A
  bounded current/next-turn continuation model guarantees a single connection
  cannot monopolize a logical turn. Pure socket I/O and poll ownership can be
  briefly handed off to an existing I/O worker.
- **Runtime dependencies**: `polling` 3 (Linux epoll level-triggered event
  loop), `bytes` (zero-copy buffers), `slab` (FD/connection slots), `clap`
  (CLI), `log`/`env_logger` (logging), `libc` (syscalls).
- **Toolchain**: locked to `stable` via `rust-toolchain.toml`.
- **Code quality**: `make clippy` must be warning-free (`too_many_arguments`
  and `type_complexity` are the only allowed structural lints). No new global
  `allow` attributes.

## Layered architecture

Dependencies flow strictly bottom-up; reverse dependencies and cross-layer
coupling are forbidden. There are five vertical layers, low to high:

```
L1  foundation/io/       bytes, storage, protocol, persistence, syscall
L2  foundation/memory/   pure in-memory value contracts, metrics, utils
L3  engine/              command, state, persistence, distributed, extension
L4  framework/           commands, config, context, registry, capabilities,
                         observability
L5  runtime + server + bin  event loop, startup wiring, process entry
```

Within `foundation`, `io` may depend on `memory` contracts/utilities, but
`memory` must never depend on `io`.

| Layer | Key responsibilities |
|-------|----------------------|
| `foundation/io/` | `protocol/` (RESP `Value`, incremental `Parser`, serializer), `store/` (`Robj`, `Dict`, `Db`, `DbSet`), `persistence/` (RDB/AOF codec, snapshot, journal), `sys/` (time, log, signal, rand, process) |
| `foundation/memory/` | `value/` (type/encoding markers, collection traits), `metrics/` (`ServerStats`, memory tracker), `util_*.rs` (alloc/assert/version helpers) |
| `engine/` | `command/` (args, descriptor, context), `state/` (`DbContext`, blocking registry), `persistence/` (RDB/AOF runtime state), `distributed/` (cluster, replication, pubsub, sentinel, shutdown), `extension/` (acl, config, lua, script, function, module) |
| `framework/` | `commands/` (command handlers by domain), `context/` (`ServerContext` facade), `registry.rs`/`spec.rs` (command specs), `config/`, `capabilities/`, `observability/`, `generated/`, `host_tasks/` |
| `runtime/` + `server/` | `runtime/driver/` (host-neutral kernel/continuation), `runtime/server/` (native supervisor/worker), `server/` (startup assembly) |

## Command handler signature

Ordinary command handlers must follow this signature:

```rust
fn(&mut dyn CommandConn, &CommandArgs, &mut ServerContext) -> Value
```

Handlers return `Value` only and never touch sockets directly; I/O,
authentication, rate limiting, and stats are handled by the runtime. Commands
that need to safely re-enter the event loop (Lua, RDB, AOF) use the
`CommandExecutor::Hosted` prepare/run/finalize three-phase model.

## Configuration

Driven by `ConfigRegistry` with 60+ config items covering slowlog, commandlog,
save, maxmemory, hz, cluster, server, replication, persistence, encoding, and
security. New items are registered under `engine/extension/config/` following
the `ConfigSpec` format. Side-effectful items use `Special` semantics: `GET`
is handled by the registry, `SET` currently goes through legacy logic for
safety.

## Building

- Regular build: `cargo build --bins`
- Release: `cargo build --release` (`lto=fat`, `codegen-units=1`)
- Run: `cargo run -- --port 6379 --loglevel notice`
- Unified entry: `make <target>` (`build`, `release`, `check`, `fmt`,
  `clippy`, `unit`, `integration`, `property`, etc.)

## Testing

- Never modify `runtest`, `runtest-sentinel`, `runtest-moduleapi`, other
  `runtest*` scripts, or their supporting test-runner scripts. Fix regression
  failures in owned Rust implementation code instead of changing the harness.

### 1. Integration regression (mainline): `./runtest`

Reuses the upstream Valkey Tcl suite. Takes no arguments; runs debug build,
Rust library unit tests, and the full Tcl regression. Compatibility binaries
(`valkey-server`, `valkey-cli`, etc.) are copied into `thirdparty/valkey/src/`
so the harness discovers them. `unit/obuf-limits` and `unit/oom-score-adj` are
permanently skipped. Default concurrency must be preserved.

- Upstream tests under `thirdparty/valkey` are immutable compatibility
  specifications. Never modify, patch, relax, replace, filter, or add retries
  around them to make a failure pass.
- Do not add test skips or reduce test concurrency. Fix failures in owned
  Rust implementation code without weakening upstream coverage.

### 2. Sentinel regression: `./runtest-sentinel`

Runs the native Sentinel Tcl suite (`tests/sentinel/run.tcl`) against the real
`thirdparty/valkey` tree. Do not run `runtest`, `runtest-sentinel`, or
`runtest-moduleapi` concurrently -- they overwrite the same compatibility
entries.

### 3. Fast local self-tests (auxiliary)

- **Unit tests**: inline `#[cfg(test)]` in `src/**`; run with `make unit`.
- **Cargo integration tests**: `tests/integration.rs`, split by domain under
  `tests/integration/`; run with `make integration`.
- **Property tests**: `make property` runs `proptest`-based tests in
  `tests/property/`.
- **Local Tcl tests**: `tests/unit/*.tcl` are temporarily copied by `runtest`.

## Code style

- Git commit messages must be written entirely in English, including both the
  subject and body.
- Follow official Rust style; run `make fmt` and ensure `make clippy` is
  warning-free before committing.
- Keep changes minimal and focused; avoid mixing unrelated refactors.
- Generated code under `framework/generated/` is managed by `build.rs` from the
  Valkey submodule and must not be hand-edited.

### Error handling

- Return `CommandError` uniformly; never `panic!` in handlers.
- `unwrap()` is allowed only when the value was verified as `Some`/`Ok`
  immediately above, in test code, or in provably-impossible init scenarios
  (with an explanatory comment).

### `unsafe` usage

- Every `unsafe` block must carry a `// SAFETY: ...` comment.
- `unsafe` blocks must be minimal.
- `scripts/unsafe-roots.txt` is the sole allowlist for unsafe source roots:
  `engine/extension/module`, `foundation/memory`, `foundation/io/sys`,
  `foundation/io/store/structure`, and `framework/capabilities`.
- Run `make check-unsafe-boundaries` and `make check-unsafe-safety` for any
  unsafe change; run `make ffi-boundaries` for module/FFI changes.

### Naming

| Category | Rule | Example |
|----------|------|---------|
| Command spec constant | `<CMD>_SPEC`, uppercase | `BLPOP_SPEC` |
| Handler function | same as command, lowercase | `blpop` |
| Internal helper | verb + noun, snake_case | `pop_from_left` |
| Struct | UpperCamelCase | `BlockedState` |
| Boolean variable | `is_`/`has_`/`should_` prefix | `is_empty` |
| Time/duration | `_ms`/`_sec` suffix | `timeout_ms` |

## Pre-commit checklist

- `cargo build --bins` passes, `make fmt` run, `make clippy` warning-free.
- `make unit` and `make integration` pass.
- `./runtest` (mainline, default concurrency) remains green; no `--clients 1`.
- Update `COMMAND` introspection (`framework/commands/server/command/`) if
  command behaviour changed.
- For module/FFI changes, run `make ffi-boundaries`.
- For architecture/file-split changes, run `make check-architecture`.
