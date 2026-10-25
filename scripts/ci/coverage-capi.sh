#!/usr/bin/env bash
# Run the C ABI conformance harness under coverage instrumentation, so its
# profiles land in the same llvm-cov target dir the workspace tests wrote to
# and `cargo llvm-cov report` merges them. Needs a prior `cargo llvm-cov
# --no-report` run (task coverage does that).
set -euo pipefail

cd "$(dirname "$0")/../.."

# Exports RUSTFLAGS, CARGO_TARGET_DIR and LLVM_PROFILE_FILE. run.sh reads
# CARGO_TARGET_DIR, and the C programs and the fixture's broker processes
# inherit LLVM_PROFILE_FILE, so each writes its own profile.
source <(cargo llvm-cov show-env --sh --no-cfg-coverage)

# The fixture starts these; run.sh builds felix-capi itself.
cargo build --locked -p felix-broker-service --bin felix-broker
cargo build --locked -p felix-cluster --bin felix-cluster

marker="$(mktemp)"
crates/sdk/felix-capi/tests/c/run.sh fixture

# Diagnostics: did the harness processes write profiles that name felix-capi code?
bindir="$(rustc --print target-libdir)/../bin"
profraws=$(find "$CARGO_TARGET_DIR" -maxdepth 1 -name '*.profraw' -newer "$marker")
echo "harness wrote $(echo "$profraws" | grep -c .) profiles"
for f in $profraws; do
  echo "$(wc -c <"$f") bytes $f"
done | sort -n | tail -n 20
# shellcheck disable=SC2086
"$bindir/llvm-profdata" merge -failure-mode=warn -o "$marker.profdata" $profraws
echo "functions in harness profiles: $("$bindir/llvm-profdata" show --all-functions "$marker.profdata" | grep -c 'Hash:')"
echo "groups functions with counts: $("$bindir/llvm-profdata" show --all-functions --counts "$marker.profdata" | grep -A3 'groups' | grep -c 'Counters')"
"$bindir/llvm-profdata" show --all-functions "$marker.profdata" | grep -i 'groups' | head -n 5
