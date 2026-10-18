SHELL := /bin/sh

CARGO ?= cargo
NODE ?= node
PYTHON ?= python3
WASM_PACK ?= wasm-pack
PROPTEST_CASES ?= 1000
FUZZ_TIME ?= 30

.PHONY: help build check ci generated-fmt check-generated-command-metadata check-config-parity fmt clippy ffi-boundaries check-unsafe-safety check-unsafe-boundaries check-host-dispatcher check-boundaries check-file-sizes check-ci-support check-sentinel-tcl-support check-architecture unit integration wasm-check wasm-web wasm-dist tcl sentinel-tcl property fault bench daily-performance weekly-soak nightly \
	fuzz-build fuzz-smoke mutation coverage coverage-core miri doc install-test-tools clean \
	formal-verify formal-tests formal

help:
	@printf '%s\n' \
		'build              Build all Rust binaries' \
		'check              Build, format-check, clippy, unit, and property tests' \
		'ci                 Run the local CI gate set' \
		'check-architecture Enforce layer boundaries and file-size governance' \
		'check-config-parity Require every C config.c entry to have a Rust capability decision' \
		'check-generated-command-metadata Validate generated command source parity' \
		'check-ci-support   Run CI helper script fixtures' \
		'check-unsafe-safety Enforce SAFETY comments for unsafe Rust code' \
		'check-unsafe-boundaries Enforce allowlisted unsafe source roots' \
		'unit               Run library unit tests' \
		'integration        Run Rust black-box integration tests' \
		'wasm-check         Compile the browser-facing wasm interface' \
		'wasm-web           Build the browser CLI wasm-bindgen package' \
		'wasm-dist          Assemble the copy-pasteable browser bundle into dist/versal-wasm' \
		'tcl                Run the Rust Tcl manifest in Docker' \
		'sentinel-tcl       Run the native Sentinel Tcl suite' \
		'property           Run property tests (PROPTEST_CASES=$(PROPTEST_CASES))' \
		'fault              Run deterministic RDB IO fault-injection tests' \
		'bench              Run Criterion microbenchmarks' \
		'daily-performance  Check startup performance baseline regression gate' \
		'weekly-soak        Run weekly long-stability entry point' \
		'nightly            Run nightly property gate' \
		'fuzz-build         Compile all fuzz targets' \
		'fuzz-smoke         Run each fuzz target for FUZZ_TIME seconds' \
		'mutation           Run cargo-mutants on configured core modules' \
		'coverage           Generate an HTML coverage report' \
		'coverage-core      Require 60% line coverage on src/ core runtime modules' \
		'miri               Run strict-provenance Miri against Dict lifecycle tests' \
		'doc                Run documentation tests' \
		'formal-verify      Run P language model checker on replication spec' \
		'formal-tests       Run P language generated integration tests' \
		'formal             Run all P language formal verification (verify + tests)' \
		'install-test-tools Install cargo-fuzz, cargo-mutants, and cargo-llvm-cov'

build:
	$(CARGO) build --bins

check: generated-fmt clippy ffi-boundaries check-unsafe-safety check-architecture check-config-parity unit property

ci: fmt wasm-check clippy unit integration check-architecture check-config-parity check-ci-support check-generated-command-metadata ffi-boundaries check-unsafe-safety property

generated-fmt: build check-generated-command-metadata fmt

check-generated-command-metadata:
	$(PYTHON) scripts/versal-util.py check generated-command-metadata

check-config-parity:
	$(PYTHON) scripts/versal-util.py check config-parity

fmt:
	$(CARGO) fmt --check

clippy:
	$(CARGO) clippy --all-targets -- -D clippy::all -A clippy::too_many_arguments -A clippy::type_complexity

ffi-boundaries:
	! rg -n "GLOBAL_MODULE_API|Box::leak\\(.*ModuleRegistry|&'static mut ModuleRegistry" src/engine/extension/module

check-unsafe-safety:
	$(PYTHON) scripts/versal-util.py selftest unsafe-safety
	$(PYTHON) scripts/versal-util.py check unsafe-safety

check-unsafe-boundaries:
	$(PYTHON) scripts/versal-util.py selftest unsafe-boundaries
	$(PYTHON) scripts/versal-util.py check unsafe-boundaries

check-boundaries:
	$(PYTHON) scripts/versal-util.py check boundaries

check-file-sizes:
	$(PYTHON) scripts/versal-util.py check file-sizes

check-ci-support:
	$(PYTHON) scripts/versal-util.py check ci-support

check-sentinel-tcl-support:
	$(PYTHON) scripts/versal-util.py check sentinel-support

check-host-dispatcher:
	$(PYTHON) scripts/versal-util.py check host-dispatcher
	$(PYTHON) scripts/versal-util.py selftest host-dispatcher

check-architecture: wasm-check
	$(PYTHON) scripts/versal-util.py check architecture

unit:
	$(CARGO) test --lib

integration:
	$(CARGO) test --test integration

wasm-check:
	$(NODE) --test src/bin/wasm/worker.test.mjs
	$(CARGO) test --no-default-features --features wasm --lib
	$(CARGO) build --no-default-features --features wasm --target wasm32-unknown-unknown

# The published wasm must not leak local paths (panic locations embed
# file!() strings). Remap the repo, toolchain, cargo registry and home to
# neutral prefixes; the local registry mirror hash is stripped as well.
# rustc applies the last matching --remap-path-prefix, so the broad home
# mapping comes first and more specific prefixes override it.
WASM_RUSTFLAGS ?= \
	--remap-path-prefix $(HOME)=/home \
	--remap-path-prefix $(HOME)/.rustup=/rustup \
	--remap-path-prefix $(HOME)/.cargo=/cargo \
	--remap-path-prefix $(CURDIR)=/versal

wasm-web:
	RUSTFLAGS="$(WASM_RUSTFLAGS)" \
		$(WASM_PACK) build . --target web --release --out-dir src/bin/wasm/pkg --no-default-features --features wasm
	@printf '%s\n' 'Serve with: python3 -m http.server 8000 --directory src/bin/wasm'

# Self-contained browser runtime bundle; copy the directory verbatim into the
# pages repository (e.g. public/versal-wasm/). Relative ESM imports must stay intact.
WASM_DIST_DIR ?= dist/versal-wasm
wasm-dist: wasm-web
	rm -rf $(WASM_DIST_DIR)
	mkdir -p $(WASM_DIST_DIR)
	cp -R src/bin/wasm/pkg $(WASM_DIST_DIR)/pkg
	cp src/bin/wasm/client.mjs \
		src/bin/wasm/worker.mjs \
		src/bin/wasm/worker-entry.mjs \
		src/bin/wasm/gateway.mjs \
		$(WASM_DIST_DIR)/
	@printf '%s\n' 'Bundle ready at $(WASM_DIST_DIR); copy it into the pages repo unchanged.'

tcl:
	./runtest

sentinel-tcl:
	./runtest-sentinel

property:
	PROPTEST_CASES=$(PROPTEST_CASES) $(CARGO) test --test property

fault:
	$(CARGO) test --lib rdb_write_

bench:
	$(CARGO) bench --bench dict_micro
	$(CARGO) bench --bench protocol_micro

daily-performance:
	$(PYTHON) scripts/versal-util.py ci performance --baseline-ms "$${BASELINE_MS:?}" --current-ms "$${CURRENT_MS:?}"

weekly-soak:
	$(PYTHON) scripts/versal-util.py ci weekly-soak

nightly:
	$(MAKE) property

fuzz-build:
	$(CARGO) build --manifest-path fuzz/Cargo.toml --bins

fuzz-smoke:
	cd fuzz && $(CARGO) fuzz run resp_parser -- -max_total_time=$(FUZZ_TIME)
	cd fuzz && $(CARGO) fuzz run rdb_load -- -max_total_time=$(FUZZ_TIME)

mutation:
	$(CARGO) mutants --config mutants.toml --timeout 30 --build-timeout 60

coverage:
	$(CARGO) llvm-cov --lib --bin versal-server --test integration --html --output-dir target/coverage-report

coverage-core:
	$(CARGO) llvm-cov --lib --bin versal-server --test integration --summary-only --fail-under-lines 70 --ignore-filename-regex '(/src/bin/|/src/framework/commands/|/src/engine/(cluster|module|sentinel)/)'

miri:
	MIRIFLAGS=-Zmiri-strict-provenance $(CARGO) +nightly miri test --test miri_dict

doc:
	$(CARGO) test --doc

install-test-tools:
	$(CARGO) install cargo-fuzz
	$(CARGO) install cargo-mutants
	$(CARGO) install cargo-llvm-cov

clean:
	$(CARGO) clean
	$(MAKE) -C fuzz clean


P_CHECKER ?= p

.PHONY: formal-verify
formal-verify:
	@if command -v $(P_CHECKER) >/dev/null 2>&1; then \
		$(MAKE) -C specs/replication check CHECKER=$(P_CHECKER); \
	else \
		echo "warning: P checker ($(P_CHECKER)) not found — skipping formal verification"; \
	fi

.PHONY: formal-tests
formal-tests:
	cargo test --test formal -- --nocapture

.PHONY: formal
formal: formal-verify formal-tests

