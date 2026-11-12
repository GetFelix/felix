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
# Nothing is wiped under a live control plane: a whole-cluster store wipe leaves
# the control plane's replica assignments pointing at empty stores, and every
# publish then times out on quorum. Each cell instead deletes and recreates the
# stream and waits until no shard is under-replicated, as campaign-rf3.sh does.
# Switching builds is the one place the state must go: it reseeds (brokers and
# control plane together, the cluster starts over), so a build never opens a
# store, generation or assignment the other build wrote.
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
# shellcheck source=rf3-streams.sh
source "${here}/rf3-streams.sh"

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
: "${REPL_WAIT_SECS:=240}"
NBROKERS="${#BROKER_VMS[@]}"
WIPE_DURABLE=0    # see the header; RF 1 sessions keep cells.sh's default
for l in "${label_a}" "${label_b}"; do
  case " ${BROKER_LABELS} " in
    *" ${l} "*) ;;
    *) echo "!! ${l} is not installed on this session (BROKER_LABELS: ${BROKER_LABELS})" >&2; exit 1 ;;
  esac
done

# One configuration per build; every other knob is the calibrated base.
# on_commit: a Quorum ack on a durable stream is only a durable ack then.
fsync="FELIX_DURABLE_FSYNC_MODE=on_commit"

# replicated_ready: brokers ready and no shard under-replicated.
replicated_ready() {
  local deadline st
  wait_ready || { note "WARN brokers not ready"; return 1; }
  deadline=$(( $(date +%s) + REPL_WAIT_SECS ))
  while :; do
    st="$(repl_status)"
    case "${st}" in
      under=0\ shards=[1-9]*) AB_REPL="${st}"; return 0 ;;
    esac
    [ "$(date +%s)" -ge "${deadline}" ] && break
    sleep 10
  done
  note "WARN not fully replicated after ${REPL_WAIT_SECS} s: ${st:-no answer}"
  return 1
}

# fresh_stream: the cell's stream recreated, so it starts empty and the disk is
# freed, then fully replicated before anything is measured. Uneven leaders are
# logged, not fixed.
fresh_stream() {
  reset_streams "${AB_STREAM}" || return 1
  sleep 12
  # Stuck here means the cluster itself is wrong, not this cell: every later
  # cell would burn REPL_WAIT_SECS and skip, so stop the run.
  replicated_ready || { echo "!! ${AB_STREAM} did not replicate on ${AB_SEEDED}; stopping the run" >&2; exit 1; }
  leaders_even "${AB_REPL}" "${AB_STREAM}" || note "WARN ${AB_STREAM} leaders uneven (${AB_REPL})"
  echo "${AB_REPL}" > "${OUT}/system/last-replication.txt"
}
DURABLE_PREP=fresh_stream

# switch_build <label>: a full reseed when the build changes (stop the brokers,
# wipe them, stop and restart the control plane empty), with the control plane
# and the brokers both on <label>'s build, then the new build on the clean cluster.
AB_SEEDED=""
switch_build() {
  local label="$1"
  [ "${AB_SEEDED}" != "${label}" ] || return 0
  if [ -n "${AB_SEEDED}" ]; then
    log "switching ${AB_SEEDED} -> ${label}: reseed"
    par_on "${OUT}/system" stop "systemctl stop felix-broker" "${BROKER_VMS[@]}" >/dev/null 2>&1 || true
    # The inventory's ACTIVE_REF is side A; AB_ACTIVE_REF is the build under test.
    AB_ACTIVE_REF="${label}" SESSION="${SESSION}" "${here}/reseed.sh" > "${OUT}/system/reseed-${label}.txt" 2>&1 \
      || { echo "!! reseed failed; see ${OUT}/system/reseed-${label}.txt" >&2; exit 1; }
    # The first seed pass runs on an empty control plane; a 409 before the
    # streams pass means the old tenant survived the reset.
    if sed '/creating streams now that/q' "${OUT}/system/reseed-${label}.txt" | grep -q 'HTTP 409'; then
      echo "!! control plane still holds the old tenant after the reseed; see ${OUT}/system/reseed-${label}.txt" >&2
      exit 1
    fi
    distribute_token || exit 1
    CURRENT_REF=""; CURRENT_OVERRIDES=""
  fi
  finalize_incarnations || exit 1
  AB_SEEDED="${label}"
}

run_build() {
  local round="$1" label="$2" p
  switch_build "${label}"
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

# Both builds run on these same VMs; print and record exactly what each side is.
side() { local l; for l in $BROKER_SPECS; do [ "${l%@*}" = "$1" ] && echo "${l#*@}"; done; }
{
  echo "A ${label_a} $(side "${label_a}")"
  echo "B ${label_b} $(side "${label_b}")"
  echo "brokers ${BROKER_IPS}"
} | tee "${OUT}/ab-builds.txt"
[ "$(side "${label_a}")" != "$(side "${label_b}")" ] || { echo "!! A and B are the same commit" >&2; exit 1; }
record_session run-ab
fio_baseline seq256k-j1 256k 1 seq256k-j4 256k 4 seq256k-j8 256k 8 seq4k-j1 4k 1
for r in $(seq 1 "${ROUNDS}"); do
  if [ $((r % 2)) = 1 ]; then first="${label_a}"; second="${label_b}"; else first="${label_b}"; second="${label_a}"; fi
  log "round ${r}: ${first} then ${second}"
  run_build "${r}" "${first}"
  run_build "${r}" "${second}"
done
finish
