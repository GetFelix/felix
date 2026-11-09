#!/usr/bin/env bash
# A/B two builds on one replicated session: the same three brokers, the same
# generator, rounds that alternate which build goes first. Built to answer one
# question per run, such as whether a latency regression shows up when every
# broker has its own disk (#2148).
#
# The session must be seeded with REPLICATION_FACTOR=3 and carry both refs in
# BROKER_REFS (see session.gcp-rf3-ab.env.example). Each round runs, for each
# build and each payload, a steady cell and a burst cell on a durable Quorum
# stream with fsync on every commit.
#
#   SESSION=<name> ./run-ab.sh <label-A> <label-B>
#
# The labels are the names BROKER_LABELS lists (the ref names with characters
# outside A-Za-z0-9._- turned into '-'). Settings:
#   ROUNDS=3                  rounds; odd rounds run A then B, even rounds B then A
#   PAYLOADS="65536 1024"     payload sizes in bytes
#   AB_STREAM=perf-durable-quorum
#   AB_GENERATORS=1           generators driving each cell (2 on the 22 vCPU session)
#   STEADY_SECS=60 BURST_SECS=30
#   STEADY_PUBLISHERS=4       publishers, one record in flight each, acked
#   BURST_PUBLISHERS=12       publishers, batches of 64, eight batches in flight
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=cells.sh
source "${here}/cells.sh"

label_a="${1:?usage: run-ab.sh <label-A> <label-B>}"
label_b="${2:?usage: run-ab.sh <label-A> <label-B>}"
: "${ROUNDS:=3}"
: "${AB_GENERATORS:=1}"       # generators per cell; publishers are per generator
: "${PAYLOADS:=65536 1024}"
: "${AB_STREAM:=perf-durable-quorum}"
: "${STEADY_SECS:=60}"
: "${BURST_SECS:=30}"
: "${STEADY_PUBLISHERS:=4}"
: "${BURST_PUBLISHERS:=12}"
: "${REPLICATION_FACTOR:?the session inventory has no REPLICATION_FACTOR}"
if [ "${REPLICATION_FACTOR}" != 3 ]; then
  echo "!! this session is RF ${REPLICATION_FACTOR}; a Quorum stream means nothing below 3" >&2
  exit 1
fi
for l in "${label_a}" "${label_b}"; do
  case " ${BROKER_LABELS} " in
    *" ${l} "*) ;;
    *) echo "!! ${l} is not installed on this session (BROKER_LABELS: ${BROKER_LABELS})" >&2; exit 1 ;;
  esac
done

# One configuration per build; every other knob is the calibrated base.
# on_commit: a Quorum ack on a durable stream is only a durable ack then.
fsync="FELIX_DURABLE_FSYNC_MODE=on_commit"

run_build() {
  local round="$1" label="$2" p
  stage "${label}" "${fsync}"
  for p in ${PAYLOADS}; do
    # shellcheck disable=SC2046 # ingest_flags prints words
    CELL_SECS="${STEADY_SECS}" durable_cell "${label}-steady-p${p}-r${round}" "${AB_GENERATORS}" \
      $(PAYLOAD="${p}" BATCH=1 IN_FLIGHT=1 CELL_SECS="${STEADY_SECS}" ingest_flags "${AB_STREAM}" "${SHARDS}" "${STEADY_PUBLISHERS}")
    # shellcheck disable=SC2046
    CELL_SECS="${BURST_SECS}" durable_cell "${label}-burst-p${p}-r${round}" "${AB_GENERATORS}" \
      $(PAYLOAD="${p}" BATCH=64 IN_FLIGHT=8 CELL_SECS="${BURST_SECS}" ingest_flags "${AB_STREAM}" "${SHARDS}" "${BURST_PUBLISHERS}")
  done
}

record_session run-ab
fio_baseline seq256k-j1 256k 1 seq256k-j4 256k 4 seq256k-j8 256k 8 seq4k-j1 4k 1
for r in $(seq 1 "${ROUNDS}"); do
  if [ $((r % 2)) = 1 ]; then first="${label_a}"; second="${label_b}"; else first="${label_b}"; second="${label_a}"; fi
  log "round ${r}: ${first} then ${second}"
  run_build "${r}" "${first}"
  run_build "${r}" "${second}"
done
finish
