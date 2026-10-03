#!/usr/bin/env bash
# NATS fast batch at more streams than Felix has keys. Fast-batch tuning showed
# NATS at ~480k msg/s (256 B, default sync) with its server ~60% idle and no gain
# from wider windows or more clients, so the remaining knob is stream count.
set -uo pipefail
# shellcheck source=scripts/perf/azure/nats/nats-lib.sh
source "$(cd "$(dirname "$0")" && pwd)/nats-lib.sh"
: "${STREAM_TUNE:=96 192}"
EXPECT_MTU="$(expected_mtu)" || exit 1
record_session nats-stream-tune
apply_mtu "${EXPECT_MTU}" || exit 1
for m in default memory; do
  for s in ${STREAM_TUNE}; do
    nats_cell "nats-stream-tune-${m}-p256-f64-w16-c16-s${s}-t1" "${m}" js-fast 256 16 16 64 "${s}"
  done
done
finish
