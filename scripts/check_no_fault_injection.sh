#!/usr/bin/env bash
# Fail if a broker binary carries the test-only peer-partition injection.
#
# A release broker must not drop its peers because an environment variable
# says so. The partition code and its variable name are compiled only into
# debug and `fault-injection` builds, so finding either string in a binary
# means it was built with them.
#
# Usage: scripts/check_no_fault_injection.sh <felix-broker binary>...
set -euo pipefail

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <felix-broker binary>..." >&2
  exit 2
fi

# The variable's name, and the refusal the partition check returns
# (felix-replication's peer/pool.rs).
markers=(
  "FELIX_PEER_PARTITION_FILE"
  "partitioned from this broker (fault injection)"
)

status=0
for binary in "$@"; do
  if [ ! -s "$binary" ]; then
    echo "::error::$binary is missing or empty" >&2
    status=1
    continue
  fi
  for marker in "${markers[@]}"; do
    if LC_ALL=C grep -qaF -- "$marker" "$binary"; then
      echo "::error::$binary carries fault injection: found \"$marker\"" >&2
      status=1
    fi
  done
done
[ "$status" -eq 0 ] && echo "no fault injection in: $*"
exit "$status"
