#!/usr/bin/env bash
# Felix and NATS cells alternated on the same VMs, so drive wear, time of day
# and neighbour noise cannot separate the two systems.
#
#   SESSION=v060-a ./interleave.sh
#
# For each trial and each pair: one Felix cell at its best profile and the
# NATS cell it pairs with. Felix goes first on odd trials and NATS on even
# ones, and AB_TRIALS is even, so neither system always runs first. Before a Felix
# cell NATS is stopped and its store removed, Felix's store is wiped and /data
# is trimmed; before a NATS cell nats_cell stops Felix, wipes and trims. Run
# after install.sh, and after the sweep has picked NATS's best shape.
#
# Pairs (AB_PAIRS):
#   dur-b1    Felix perf-durable, batch 1, 64 in flight   <-> always, fast flow 1
#   dur-b64   Felix perf-durable, batch 64, 64 in flight  <-> always, fast flow 64
#   inmem-b1  Felix perf, batch 1, 64 in flight           <-> memory, fast flow 1
#   ff        Felix perf, batch 64, fire-and-forget       <-> core publish
#   inmem-b64 Felix perf, batch 64, 64 in flight          <-> memory, fast flow 64
#   per-b64   Felix perf-durable periodic, batch 64       <-> default sync, fast flow 64
#   dur-a64   Felix perf-durable, batch 64, 64 in flight  <-> always, atomic batch 64
#
# Knobs: AB_PAIRS, AB_PAYLOADS (4096 256), AB_KEYS (48, Felix keys),
# AB_STREAMS (NATS streams, default AB_KEYS), AB_TRIALS (4, even), FELIX_BEST_REF
# (main), FELIX_BEST_ENV and FELIX_BEST_CLIENT_ENV (session A's shapes
# profile), and the nats-lib.sh shape knobs for NATS (FAST_WINDOW, ...).
set -uo pipefail
# shellcheck source=scripts/perf/azure/nats/nats-lib.sh
source "$(cd "$(dirname "$0")" && pwd)/nats-lib.sh"

: "${AB_PAIRS:=dur-b1 dur-b64 inmem-b1 ff}"
: "${AB_PAYLOADS:=4096 256}"
: "${AB_KEYS:=48}"
: "${AB_STREAMS:=${AB_KEYS}}"
: "${AB_TRIALS:=4}"
# Fast batch keeps window x flow messages unacked per client. At flow 64 the
# flow-1 window (64) means 4096 4 KiB messages per client, which in always
# mode outlasts nats bench's ack timeout.
: "${AB_FLOW64_WINDOW:=16}"
# NATS publishers per generator. An atomic-batch client has one batch in
# flight, so dur-a64 may need more clients than Felix's PUBS_PER_GEN.
: "${AB_NATS_PUBS:=${PUBS_PER_GEN}}"
: "${FELIX_BEST_REF:=main}"
: "${FELIX_BEST_ENV:=FELIX_QUIC_LISTENERS=4 FELIX_PUB_INGRESS_WAIT=1 FELIX_DURABLE_FSYNC_MODE=on_commit}"
: "${FELIX_BEST_CLIENT_ENV:=FELIX_PUB_CONN_POOL=4}"

[ $((AB_TRIALS % 2)) = 0 ] || { echo "!! AB_TRIALS must be even so each system goes first equally often" >&2; exit 2; }
EXPECT_MTU="$(expected_mtu)" || exit 1

# felix_ab <name> <stream> <payload> <batch> <in-flight> [fsync-mode]
felix_ab() {
  local name="$1" stream="$2" p="$3" b="$4" f="$5" fsync="${6:-}" env="${FELIX_BEST_ENV}"
  [ -n "${fsync}" ] && env="$(printf '%s' "${env}" | sed -E "s/FELIX_DURABLE_FSYNC_MODE=[a-z_]+/FELIX_DURABLE_FSYNC_MODE=${fsync}/")"
  if [ "${RESUME}" = 1 ] && [ -e "${OUT}/cells/$(tagged "${name}")/done" ]; then log "skip ${name} (done)"; return 0; fi
  nats_agent_on "${NATS_VM}" "nats-agent stop
felix-agent wipe
nats-agent trim" > "${OUT}/system/nats/ab-felix-prep.txt" 2>&1 || {
    echo "!! could not hand the broker to Felix; see ${OUT}/system/nats/ab-felix-prep.txt" >&2
    FAILED_CELLS="${FAILED_CELLS} ${name}"; return 0; }
  # Felix was stopped by the wipe; forget the applied configuration so the
  # cell's configure starts it again.
  CURRENT_REF=""; CURRENT_OVERRIDES=""
  # shellcheck disable=SC2086 # FELIX_BEST_ENV is a list of KEY=VALUE words
  stage "${FELIX_BEST_REF}" ${env}
  # shellcheck disable=SC2046 # ingest_flags prints words
  CELL_LOADGEN_ENV="${FELIX_BEST_CLIENT_ENV}" cell "${name}" "${NGEN}" \
    $(PAYLOAD="${p}" BATCH="${b}" IN_FLIGHT="${f}" ingest_flags "${stream}" "${AB_KEYS}" "${PUBS_PER_GEN}")
}

# pair <pair> <payload> <trial> <felix-first:0|1>
pair() {
  local pr="$1" p="$2" t="$3" first="$4" fstream fb ff fsync="" nmode nkind nwin nflow
  case "${pr}" in
    dur-b1) fstream=perf-durable; fb=1; ff=64; nmode=always; nkind=js-fast; nwin="${FAST_WINDOW}"; nflow=1 ;;
    dur-b64) fstream=perf-durable; fb=64; ff=64; nmode=always; nkind=js-fast; nwin="${AB_FLOW64_WINDOW}"; nflow=64 ;;
    inmem-b1) fstream=perf; fb=1; ff=64; nmode=memory; nkind=js-fast; nwin="${FAST_WINDOW}"; nflow=1 ;;
    ff) fstream=perf; fb=64; ff=""; nmode=default; nkind=core; nwin=0; nflow=1 ;;
    inmem-b64) fstream=perf; fb=64; ff=64; nmode=memory; nkind=js-fast; nwin="${AB_FLOW64_WINDOW}"; nflow=64 ;;
    dur-a64) fstream=perf-durable; fb=64; ff=64; nmode=always; nkind=js-atomic; nwin=1; nflow=64 ;;
    per-b64) fstream=perf-durable; fb=64; ff=64; fsync=periodic; nmode=default; nkind=js-fast; nwin="${AB_FLOW64_WINDOW}"; nflow=64 ;;
    *) echo "!! unknown pair ${pr}" >&2; return 0 ;;
  esac
  local fname="ab-felix-${pr}-p${p}-k${AB_KEYS}-t${t}" nname="ab-nats-${pr}-p${p}-s${AB_STREAMS}-t${t}"
  if [ "${first}" = 1 ]; then
    felix_ab "${fname}" "${fstream}" "${p}" "${fb}" "${ff}" "${fsync}"
    nats_cell "${nname}" "${nmode}" "${nkind}" "${p}" "${AB_NATS_PUBS}" "${nwin}" "${nflow}" "${AB_STREAMS}"
  else
    nats_cell "${nname}" "${nmode}" "${nkind}" "${p}" "${AB_NATS_PUBS}" "${nwin}" "${nflow}" "${AB_STREAMS}"
    felix_ab "${fname}" "${fstream}" "${p}" "${fb}" "${ff}" "${fsync}"
  fi
}

record_session nats-interleave
distribute_token || exit 1
apply_mtu "${EXPECT_MTU}" || exit 1
mkdir -p "${OUT}/system/nats"
for t in $(seq 1 "${AB_TRIALS}"); do
  for pr in ${AB_PAIRS}; do
    for p in ${AB_PAYLOADS}; do pair "${pr}" "${p}" "${t}" $(( t % 2 )); done
  done
done

# Leave the VM as install.sh found it after the last cell: NATS up, Felix off.
nats_agent_on "${NATS_VM}" "systemctl stop felix-broker || true
nats-agent reset default" > "${OUT}/system/nats/ab-final.txt" 2>&1 || true
finish
