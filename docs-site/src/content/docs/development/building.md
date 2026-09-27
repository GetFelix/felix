---
title: "Building & Testing"
---

How to build, test, and develop Felix — the same commands CI runs.

## Build System

Felix uses **Cargo** (Rust's build tool) and **Task** (optional task runner) for builds.

### Prerequisites

**Required**:

- **Rust 1.97.1+**: Install via [rustup](https://rustup.rs/)
- **Cargo**: Included with Rust

**Optional**:

- **Task**: Convenience task runner ([install](https://taskfile.dev/))
- **cargo-llvm-cov**: Code coverage ([install](#code-coverage))
- **cargo-deny**: Dependency auditing ([install](#dependency-auditing))

### Check Versions

```bash
# Rust and Cargo
rustc --version
cargo --version

# Should show 1.97.1 or later
```

## Building

### Workspace Build

Felix uses a Cargo workspace with multiple crates:

```bash
# Build entire workspace (debug)
cargo build --workspace

# Build release (optimized)
cargo build --workspace --release

# Or with Task
task build
```

**Build profiles**:

- **Debug**: Fast compilation, slow runtime, debug symbols
- **Release**: Slow compilation, fast runtime, optimized

:::tip[Always Use Release for Testing]
Debug builds are 10-100x slower. Use `--release` for performance testing.
:::
### Building Specific Crates

```bash
# Build only the broker
cargo build -p felix-broker-service --release

# Build only the wire protocol crate
cargo build -p felix-wire

# Build client SDK
cargo build -p felix-client
```

### Building with Features

```bash
# Build with telemetry enabled
cargo build --workspace --release --features telemetry

# Build without default features
cargo build --workspace --no-default-features

# List available features
cargo metadata --format-version 1 | jq '.packages[0].features'
```

### Build Output

Build artifacts go to `target/`:

```
target/
  debug/          # Debug builds
    broker        # Broker binary
    libfelix_*.so # Library artifacts
  release/        # Release builds
    broker        # Optimized binary
```

### Clean Builds

```bash
# Remove build artifacts
cargo clean

# Or with Task
task clean

# Remove everything including downloaded crates
task clean-all
# Or: rm -rf target
```

## Running

### Broker Service

```bash
# Run broker (debug)
cargo run -p felix-broker-service

# Run broker (release)
cargo run --release -p felix-broker-service

# Run with environment variables
FELIX_QUIC_BIND=0.0.0.0:5001 cargo run --release -p felix-broker-service
```

### Demo Applications

```bash
# Demos are self-contained (in-process broker)

# Pub/sub demo
cargo run --release -p felix-broker-service --features demo --bin pubsub-demo-simple

# Cache demo
cargo run --release -p felix-broker-service --features demo --bin cache-demo

# Latency benchmark
cargo run --release -p felix-broker-service --features demo --bin latency-demo

# Notifications demo
cargo run --release -p felix-broker-service --bin pubsub-demo-notifications

# Orders/payments pipeline demo
cargo run --release -p felix-broker-service --bin pubsub-demo-orders

# Live RBAC policy change demo (control plane + broker + token exchange)
cargo run --manifest-path demos/rbac-live/Cargo.toml

# Cross-tenant isolation demo (control plane + broker + token exchange)
cargo run --manifest-path demos/cross_tenant_isolation/Cargo.toml

# Or with Task
task demo:pubsub
task demo:cache
task demo:latency
task demo:notifications
task demo:orders
task demo:rbac-live
task demo:cross-tenant-isolation
```

### Custom Demo Arguments

```bash
# Latency demo with custom settings
cargo run --release -p felix-broker-service --features demo --bin latency-demo -- \
    --binary \
    --fanout 10 \
    --batch 64 \
    --payload 4096 \
    --total 10000 \
    --warmup 500
```

## Testing

### Running Tests

```bash
# Run all tests
cargo test --workspace

# Run tests for specific crate
cargo test -p felix-broker

# Run specific test
cargo test test_name

# Run with output
cargo test -- --nocapture

# Run with Task
task test
```

### Test Organization

Tests are organized in multiple ways:

**Unit tests** (inline):
```rust
// In src/my_module.rs
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_my_function() {
        assert_eq!(my_function(1), 2);
    }
}
```

**Integration tests** (separate files):
```
crates/server/felix-broker/
  tests/
    integration_test.rs
```

**Doc tests** (in documentation):
```rust
/// Example usage:
/// ```
/// use felix_broker::Broker;
/// let broker = Broker::new();
/// ```
```

### Test Patterns

**Async tests**:

```rust
#[tokio::test]
async fn test_async_operation() {
    let result = my_async_fn().await;
    assert!(result.is_ok());
}
```

**Test fixtures**:

```rust
fn setup_test_broker() -> Broker {
    BrokerBuilder::new()
        .with_config(test_config())
        .build()
        .unwrap()
}

#[test]
fn test_with_fixture() {
    let broker = setup_test_broker();
    // Test logic
}
```

**Test isolation**:

```rust
// Use serial_test for tests that can't run in parallel
use serial_test::serial;

#[test]
#[serial]
fn test_that_uses_global_state() {
    // Test logic
}
```

### Test Filters

```bash
# Run tests matching pattern
cargo test broker

# Run tests in specific module
cargo test broker::tests::

# Exclude slow tests
cargo test --exclude-tag slow
```

## Code Coverage

### Installing cargo-llvm-cov

```bash
cargo install cargo-llvm-cov
```

### Generating Coverage

```bash
# Generate coverage report
cargo llvm-cov --all-features --workspace

# Generate HTML report
cargo llvm-cov --all-features --workspace --html
open target/llvm-cov/html/index.html

# Or with Task
task coverage
```

**Configuration** (in `.cargo/config.toml`):

The coverage task skips demo binaries:
```bash
cargo llvm-cov --ignore-filename-regex 'demos/broker/.*demo.*' \
  --skip-functions --all-features --workspace
```

### Coverage Targets

- **Target**: >80% code coverage
- **Critical paths**: >95% coverage
- **Tested in CI**: Coverage tracked in PRs

## Linting and Formatting

### Formatting

Felix uses `rustfmt` for consistent code formatting:

```bash
# Format all code
cargo fmt --all

# Check formatting (CI mode)
cargo fmt -- --check

# Or with Task
task fmt
```

**Configuration** (`.rustfmt.toml`):

```toml
edition = "2021"
max_width = 100
tab_spaces = 4
```

### Linting with Clippy

```bash
# Run clippy
cargo clippy --workspace --all-targets --all-features -- -D warnings

# Fix automatically where possible
cargo clippy --fix

# Or with Task
task clippy
```

**Clippy checks**:

- Common mistakes
- Performance issues
- Style violations
- Idiomatic Rust patterns

### Combined Lint Check

```bash
# Format and lint
cargo fmt --all && cargo clippy --workspace --all-targets --all-features -- -D warnings

# Or with Task
task lint
```

## Dependency Auditing

### Installing cargo-deny

```bash
cargo install cargo-deny --version 0.19.0 --locked
```

### Running Audit

```bash
# Check dependencies
cargo-deny check

# Or with Task
task deny
```

**What it checks**:

- Security vulnerabilities
- License compliance
- Banned dependencies
- Duplicate dependencies

**Configuration** (`deny.toml`):

```toml
[advisories]
vulnerability = "deny"
unmaintained = "warn"

[licenses]
unlicensed = "deny"
allow = ["Apache-2.0", "MIT"]

[bans]
multiple-versions = "warn"
```

## Continuous Integration

### GitHub Actions Workflows

Felix uses GitHub Actions for CI:

**.github/workflows/ci.yml**:
- Build on Linux, macOS, Windows
- Run tests
- Check formatting
- Run clippy
- Verify documentation builds

**.github/workflows/coverage.yml**:
- Generate code coverage
- Upload to coverage service
- Update coverage badge

### Running CI Locally

Replicate CI checks locally:

```bash
# Format check
cargo fmt -- --check

# Clippy
cargo clippy --workspace --all-targets --all-features -- -D warnings

# Tests
cargo test --workspace

# Build
cargo build --workspace --release

# Or run all checks
task lint && task test && task build
```

### CI Requirements for PRs

CI runs the same commands as `task lint` and `task test`: formatting, clippy
with warnings denied, the full test suite, the docs build, and dependency
audit. A PR that passes those locally passes CI.

## Performance Testing

### Latency Benchmarks

**Basic run**:

```bash
cargo run --release -p felix-broker-service --features demo --bin latency-demo
```

**Custom configuration**:

```bash
cargo run --release -p felix-broker-service --features demo --bin latency-demo -- \
    --binary \
    --fanout 10 \
    --batch 64 \
    --payload 4096 \
    --total 10000 \
    --warmup 500
```

**Batch latency matrix**:

```bash
# Run full benchmark matrix
task perf:latency-matrix

# Or manually
python3 scripts/perf/run_latency_matrix.py
python3 scripts/perf/normalize_and_aggregate.py
python3 scripts/perf/make_charts.py
python3 scripts/perf/render_markdown_snippets.py
```

### Cache Benchmarks

```bash
# Run cache benchmarks
cargo run --release -p felix-broker-service --features demo --bin cache-demo

# Or with Task
task demo:cache
```

**Configurable parameters**:

```bash
export FELIX_CACHE_CONN_POOL=8
export FELIX_CACHE_STREAMS_PER_CONN=4
export FELIX_CACHE_BENCH_CONCURRENCY=32
export FELIX_CACHE_BENCH_KEYS=1024

cargo run --release -p felix-broker-service --features demo --bin cache-demo
```

## Profiling

### CPU Profiling

**Linux (perf)**:

```bash
# Record profile
sudo perf record -g --call-graph dwarf cargo run --release -p felix-broker-service

# View report
sudo perf report

# Generate flamegraph
cargo install flamegraph
cargo flamegraph -p felix-broker-service
```

**macOS (Instruments)**:

```bash
# Build with debug symbols
cargo build --profile release-with-debug -p felix-broker-service

# Profile with Instruments
instruments -t "Time Profiler" ./target/release-with-debug/broker
```

### Memory Profiling

**Valgrind** (Linux):

```bash
# Build debug
cargo build -p felix-broker-service

# Run with valgrind
valgrind --leak-check=full --show-leak-kinds=all ./target/debug/broker
```

**Heaptrack** (Linux):

```bash
# Install heaptrack
sudo apt install heaptrack heaptrack-gui

# Profile
heaptrack cargo run --release -p felix-broker-service

# Analyze
heaptrack_gui heaptrack.broker.*.gz
```

## Documentation

### API Documentation

**Build and view**:

```bash
# Build docs
cargo doc --workspace --no-deps

# Open in browser
cargo doc --open --no-deps
```

**Document private items**:

```bash
cargo doc --workspace --document-private-items
```

### User Documentation

**Install the documentation dependencies**:

```bash
cd docs-site
npm install
```

**Build and serve**:

```bash
# Serve locally (live reload)
npm run dev

# Build static site
npm run build

# Output to dist/ directory
```

**Open documentation**:

```
Use the local URL printed by Astro.
```

## Task Reference

Felix includes a `Taskfile.yml` for common tasks:

### Available Tasks

```bash
# Build
task build          # Build workspace
task clean          # Clean artifacts
task clean-all      # Remove target/ directory

# Code quality
task fmt            # Format code
task lint           # Format check + clippy
task clippy         # Run clippy

# Testing
task test           # Run tests
task coverage       # Generate coverage report

# Security
task deny           # Audit dependencies

# Demos
task demo:pubsub    # Run pubsub demo
task demo:cache     # Run cache demo
task demo:latency   # Run latency demo
task demo:notifications  # Run notifications demo
task demo:orders         # Run orders pipeline demo
task demo:rbac-live      # Run live RBAC mutation demo
task demo:cross-tenant-isolation  # Run cross-tenant isolation demo

# Benchmarking
task perf:latency-matrix  # Run full latency benchmark matrix

# Wire protocol
task conformance    # Run wire protocol conformance tests
```

### Using Task

```bash
# List all tasks
task --list

# Run task
task build

# Chain tasks
task lint && task test
```

## Troubleshooting Builds

### Rust Version Issues

```bash
# Check version
rustc --version

# Update Rust
rustup update

# Override for project
rustup override set 1.97.1
```

### Dependency Issues

```bash
# Update dependencies
cargo update

# Clear registry cache
rm -rf ~/.cargo/registry

# Rebuild from scratch
cargo clean
cargo build --workspace
```

### Linker Errors

**Linux**:
```bash
sudo apt-get install build-essential pkg-config libssl-dev
```

**macOS**:
```bash
xcode-select --install
```

### Out of Disk Space

```bash
# Clean build artifacts
cargo clean

# Remove old builds
rm -rf target

# Check disk usage
du -sh target
```

### Slow Builds

```bash
# Limit parallel jobs
cargo build -j 2

# Use faster linker (Linux)
sudo apt install lld
export RUSTFLAGS="-C link-arg=-fuse-ld=lld"

# Or use mold
export RUSTFLAGS="-C link-arg=-fuse-ld=mold"
```

## Before committing

`task lint && task test` is the whole pre-commit ritual — it is exactly what
CI runs. For performance work, measure release builds only, warm up first,
average several runs, change one variable at a time, and write down the
environment the numbers came from.

## Next Steps

- **Contributing guide**: [Contributing](/felix/development/contributing/)
- **Project structure**: [Project Structure](/felix/development/project-structure/)
- **Architecture**: [System Design](/felix/architecture/system-design/)
