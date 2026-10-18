# VersalKV

A pure-Rust reimplementation of the [Valkey](https://valkey.io) server, wire
compatible with the RESP protocol. VersalKV targets the same on-disk formats
(RDB/AOF) and runtime semantics as Valkey while providing a memory-safe,
multi-threaded runtime built on `polling` 3 (epoll on Linux).

## Features

- RESP protocol compatibility (parser, serializer, inline command support).
- Core data structures: strings, hashes, lists, sets, sorted sets, streams,
  bitmaps, geo, and hyperloglog operations.
- Persistence: RDB snapshots and AOF append-only file with rewrite.
- Replication: primary/replica with PSYNC2 partial resync.
- Cluster mode with 16384 hash slots and cluster bus.
- Sentinel for high-availability failover.
- Pub/Sub messaging.
- Lua scripting (`EVAL`/`EVALSHA`) via `mlua` with a sandboxed runtime.
- Module API (Valkey Module FFI).
- ACL for user and command-level access control.
- Multi-threaded I/O with a bounded-turn event loop.
- Optional `wasm32` build exposing an in-browser CLI.

## Quick start

### Prerequisites

- Rust stable (pinned via `rust-toolchain.toml`).
- A C compiler and `make` (for the Valkey Tcl test harness and C shims).
- `tclsh` for the upstream Tcl regression suite.

### Build

```sh
cargo build --bins              # debug build of all binaries
cargo build --release           # release (LTO, codegen-units=1)
```

### Run

```sh
cargo run -- --port 6379 --loglevel notice
```

Or use the built binary directly:

```sh
./target/debug/versal-server --port 6379
```

## Binaries

| Binary | Purpose |
|--------|---------|
| `versal-server` | The main Valkey-compatible server. |
| `versal-cli` | Interactive command-line client. |
| `versal-benchmark` | Workload benchmarking tool. |
| `versal-check-aof` | AOF file integrity checker and repair tool. |
| `versal-check-rdb` | RDB file integrity checker. |
| `versal-sentinel` | Sentinel process for failover orchestration. |

## Testing

VersalKV uses three complementary test lines:

1. **Integration regression** (mainline) — reuses the upstream Valkey Tcl
   suite against the Rust binaries:

   ```sh
   ./runtest
   ./runtest-sentinel       # Sentinel-specific suite
   ```

2. **Cargo unit & integration tests** (fast local loop):

   ```sh
   make unit                # inline #[cfg(test)] under src/
   make integration         # black-box tests in tests/integration/
   make property            # proptest-based property tests
   ```

3. **Additional quality gates**:

   ```sh
   make clippy              # warning-free (only too_many_arguments,
                            #   type_complexity allowed)
   make fmt                 # rustfmt check
   make check-architecture  # layer boundaries + file-size governance
   make check-unsafe-boundaries
   make check-unsafe-safety
   make ffi-boundaries      # module FFI invariants
   make miri                # Miri on Dict lifecycle tests
   make fuzz-smoke          # short fuzz runs on parser & RDB loader
   ```

## Architecture

The codebase is organized in five strictly bottom-up layers. Higher layers may
depend only on lower layers; reverse dependencies are forbidden.

```
L5  runtime + server + bin     event loop, startup assembly, process entry
L4  framework                  command handlers, context, registry, config,
                                capabilities, observability
L3  engine                     command contract, state, persistence,
                                distributed (cluster/replication/pubsub/
                                sentinel), extension (acl/lua/script/module)
L2  foundation/memory          pure value contracts, metrics, utils
L1  foundation/io              protocol, store (Robj/Dict/Db), persistence
                                codec, syscall wrappers
```

Within `foundation`, `io` may use `memory` contracts but not vice versa.

## Project layout

```
src/
├── bin/                 # process entry points (server, cli, benchmark, ...)
├── foundation/          # L1 (io) + L2 (memory)
│   ├── io/              # protocol, store, persistence, sys, transport
│   └── memory/          # value traits, metrics, alloc/assert utils
├── engine/              # L3: command, state, persistence, distributed, extension
├── framework/           # L4: commands, context, config, registry, capabilities
├── runtime/             # L5: driver (host-neutral), embedded, server (native)
└── server/              # L5: startup assembly and argv parsing
tests/
├── integration/         # black-box integration tests by domain
├── property/            # proptest property tests
├── unit/                # local Tcl tests copied into the upstream harness
├── bench/               # C/C++/Rust microbenchmarks
└── formal/              # P-language model checking for replication
thirdparty/valkey/       # upstream Valkey submodule (reference + Tcl harness)
```

## License

VersalKV is licensed under the same terms as upstream Valkey. See the
`LICENSE` file and the `thirdparty/valkey` submodule for details.
