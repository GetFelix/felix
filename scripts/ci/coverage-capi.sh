#!/usr/bin/env bash
# Run the C ABI conformance harness under coverage instrumentation, so its
# profiles land in the same llvm-cov target dir the workspace tests wrote to
# and `cargo llvm-cov report` merges them. Needs a prior `cargo llvm-cov
# --no-report` run (task coverage does that).
set -euo pipefail

cd "$(dirname "$0")/../.."

# `cargo llvm-cov` builds under <target>/llvm-cov-target and `report` reads
# profiles and objects only there, but show-env defaults to <target>. Without
# this the harness builds and profiles would land where the report never looks.
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/target}/llvm-cov-target"

# Exports RUSTFLAGS and LLVM_PROFILE_FILE. run.sh reads
# CARGO_TARGET_DIR, and the C programs and the fixture's broker processes
# inherit LLVM_PROFILE_FILE, so each writes its own profile. Captured first:
# a failure inside `source <(...)` would not stop the script, and the harness
# would then run uninstrumented and quietly lower the coverage.
coverage_env="$(cargo llvm-cov show-env --sh --no-cfg-coverage)"
eval "$coverage_env"

# The fixture starts these; run.sh builds felix-capi itself.
cargo build --locked -p felix-broker-service --bin felix-broker
cargo build --locked -p felix-cluster --bin felix-cluster

crates/sdk/felix-capi/tests/c/run.sh fixture

