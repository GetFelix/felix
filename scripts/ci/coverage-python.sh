#!/usr/bin/env bash
# Run the Python binding's Rust unit tests under coverage instrumentation, so
# their profiles land in the llvm-cov target dir the workspace tests wrote to
# and `cargo llvm-cov report` merges them. The binding is outside the workspace
# (see its Cargo.toml), so `cargo llvm-cov --workspace` never builds it. Needs
# a prior `cargo llvm-cov --no-report` run (task coverage does that) and a
# Python with its shared library on the loader path (actions/setup-python).
set -euo pipefail

cd "$(dirname "$0")/../.."

# `report` reads profiles and objects only under <target>/llvm-cov-target.
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/target}/llvm-cov-target"

# Captured first: a failure inside `eval "$(...)"` would not stop the script
# and the tests would run uninstrumented.
coverage_env="$(cargo llvm-cov show-env --sh --no-cfg-coverage)"
eval "$coverage_env"

cd crates/sdk/felix-python
cargo test --locked
