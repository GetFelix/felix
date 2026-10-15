#!/usr/bin/env bash
# NATS fast batch (flow 64) at a few window x publisher settings, per mode and
# payload, to find NATS's best before the batched pairs are quoted.
#   SESSION=v060-a TUNE_MODES="default memory" TUNE_PAYLOADS=256 ./fast-tune.sh
set -uo pipefail
# shellcheck source=scripts/perf/azure/nats/nats-lib.sh
source "$(cd "$(dirname "$0")" && pwd)/nats-lib.sh"
: "${TUNE_MODES:=default memory}"
: "${TUNE_PAYLOADS:=256}"
: "${TUNE_GRID:=16:16 256:16 64:64}"   # window:publishers-per-generator
: "${TUNE_STREAMS:=48}"
: "${TUNE_ASYNC_WINDOWS:=1024 4000}"
EXPECT_MTU="$(expected_mtu)" || exit 1
record_session nats-fast-tune
apply_mtu "${EXPECT_MTU}" || exit 1
for m in ${TUNE_MODES}; do
  for p in ${TUNE_PAYLOADS}; do
    for g in ${TUNE_GRID}; do
      w="${g%%:*}" c="${g##*:}"
      nats_cell "nats-fast-tune-${m}-p${p}-f64-w${w}-c${c}-s${TUNE_STREAMS}-t1" "${m}" js-fast "${p}" "${c}" "${w}" 64 "${TUNE_STREAMS}"
    done
    for w in ${TUNE_ASYNC_WINDOWS}; do
      nats_cell "nats-async-tune-${m}-p${p}-w${w}-c16-s${TUNE_STREAMS}-t1" "${m}" js-async "${p}" 16 "${w}" 1 "${TUNE_STREAMS}"
    done
  done
done
finish
