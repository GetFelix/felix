#!/usr/bin/env bash
# NATS atomic batch in always mode at a few client counts, to pick NATS's best
# before the dur-a64 pair. An atomic client keeps one batch outstanding, so
# client count is the only concurrency knob.
#   SESSION=v060-a ATOMIC_PUBS="16 64 128" ./atomic-tune.sh
set -uo pipefail
# shellcheck source=scripts/perf/azure/nats/nats-lib.sh
source "$(cd "$(dirname "$0")" && pwd)/nats-lib.sh"
: "${ATOMIC_PUBS:=16 64 128}"
: "${ATOMIC_STREAMS:=48}"
EXPECT_MTU="$(expected_mtu)" || exit 1
record_session nats-atomic-tune
apply_mtu "${EXPECT_MTU}" || exit 1
for c in ${ATOMIC_PUBS}; do
  nats_cell "nats-atomic-tune-p4096-a64-c${c}-s${ATOMIC_STREAMS}-t1" always js-atomic 4096 "${c}" 1 64 "${ATOMIC_STREAMS}"
done
finish
