#!/usr/bin/env bash
# felix-broker is the library people embed, and it must build without
# openraft. The Raft log adapter lives in felix-storage behind the `raft`
# feature, which only the control plane turns on.
#
# A workspace build proves nothing here: cargo merges features across
# members, and the simulator and the cluster harness depend on both the
# broker and the control plane. So each check is per package.
# docs/metadata-raft-design.md, "Keeping openraft out of the broker library".
set -euo pipefail
cd "$(dirname "$0")/.."

fail=0
depends_on_openraft() {
  cargo tree --locked -p "$1" -e normal --prefix none | grep -q '^openraft '
}

# The check itself: a package known to depend on openraft must show it.
if ! depends_on_openraft felix-controlplane-service; then
  echo "error: the dependency check found no openraft under felix-controlplane-service, so it cannot be trusted" >&2
  fail=1
fi

# felix-broker-service is the broker binary. Per-shard Raft will make it
# depend on openraft on purpose; remove it from this list in that change.
for package in felix-broker felix-broker-service; do
  if depends_on_openraft "$package"; then
    echo "error: $package depends on openraft:" >&2
    cargo tree --locked -p "$package" -e normal -i openraft >&2 || true
    fail=1
  fi
done

# openraft's types stay inside the adapter and its tests, the tests of the
# store and the copy root built on it, felix-replication's per-shard runtime, and the control
# plane's Raft module.
allowed='^(crates/server/felix-storage/src/raft_log(\.rs|/)|crates/server/felix-replication/src/raft(\.rs|/)|crates/server/felix-storage/tests/raft_log[^/]*\.rs$|crates/server/felix-storage/src/metadata_store/tests/|crates/server/felix-storage/src/raft_copy/tests/|services/felix-controlplane-service/src/raft(\.rs|/))'
outside=$(git grep -lE '\bopenraft::' -- 'crates/**/*.rs' 'services/**/*.rs' 'demos/**/*.rs' 'tests/**/*.rs' | grep -vE "$allowed" || true)
if [ -n "$outside" ]; then
  echo "error: openraft is named outside felix-storage's raft_log, felix-replication's src/raft and the control plane's src/raft:" >&2
  echo "$outside" >&2
  fail=1
fi

# And the library builds on its own, with only the features it asks for.
cargo check --locked -p felix-broker

exit "$fail"
