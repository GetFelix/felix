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

# The workspace run's own selection, so this rebuilds nothing and the harness
# loads the libfelix (and the fixture's binaries) that run measured. See
# FELIX_CAPI_PREBUILT in run.sh for why a `-p` build would not do. A test
# build leaves the cdylib only under deps/, so cargo reports where it is.
shared="$(cargo test --locked --no-run --all-features --workspace --message-format json \
  | python3 -I -c '
import json, sys
found = []
for line in sys.stdin:
    msg = json.loads(line)
    if (msg.get("reason") == "compiler-artifact" and msg["target"]["name"] == "felix"
            and "cdylib" in msg["target"]["kind"] and not msg["profile"]["test"]):
        found += [f for f in msg["filenames"] if f.endswith((".so", ".dylib"))]
print(found[0] if found else "")
')"
if [ -z "$shared" ]; then
  echo "cargo reported no felix-capi cdylib" >&2
  exit 1
fi

FELIX_CAPI_PREBUILT="$shared" crates/sdk/felix-capi/tests/c/run.sh fixture

