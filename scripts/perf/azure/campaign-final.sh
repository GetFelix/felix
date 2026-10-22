#!/usr/bin/env bash
# Final v0.6.0 perf campaign. One L8as_v4 broker on local NVMe RAID0, four
# D4as_v5 generators, MTU 3900, TLS, a pinned main build at product defaults.
#
#   SESSION=<name> BUILD_SHA=<40-char main sha> ./campaign-final.sh --dry-run
#   SESSION=<name> BUILD_SHA=<sha> nohup ./campaign-final.sh &    # re-running resumes
#
# --dry-run prints every cell, the estimated runtime and the cost, and touches
# no VM. A live run starts and repairs the (deallocated) session, runs the
# preflight, and deallocates the VMs when it ends, however it ends.
#
# Order: setup, stage 0 (gate), main-matrix headline cells, latency, NATS, the
# rest of the matrix, sweeps.
#   s0   BUILD_SHA broker against the preview.9 broker (P9_SHA), same client and
#        loadgen (BUILD_SHA) for both, on the headline cells, 2 trials
#        interleaved. A cell more than GATE_PCT (3) percent slower than preview.9
#        (median of trials) logs "FAIL stage0", deallocates and exits.
#   s0b  mem 4 KiB b64 fire-and-forget with the original loadgen (OLD_LOADGEN, no
#        --shards, built side by side as felix-loadgen.old) against the current
#        one; not a gate. One profiled cell on the
#        frame-pointer build.
#   m1   the headline cells to HEAD_TRIALS (3): mem 4 KiB b64 ff, mem 256 B b1
#        acked, durable 4 KiB b64 acked.
#   lat  one-in-flight pubsub latency (on_commit, periodic, in-memory; 256 B and
#        4 KiB; 3 trials) and a latency-vs-load curve (in flight 1/4/16 per
#        publisher; 64 is the matrix cell) for mem and on_commit, 256 B and 4 KiB,
#        acked b1, 2 trials.
#   net  NATS: install if missing, stream-count tune once, then interleaved
#        Felix-vs-NATS pairings (nats/interleave.sh) and latency pairings
#        (nats/nats-latency.sh). NATS at its tuned best, Felix at product defaults.
#   m2   the rest of the matrix: mem and on_commit x 256/4096/65536 B x batch
#        1/64 x acked (64 in flight)/fire-and-forget, spread keys; plus plain
#        keys for mem 256 B b1/b64 acked. 2 trials.
#   sw   listener sweep (FELIX_QUIC_LISTENERS 1/2/4/8) and the receive-window
#        check (default vs 32/64 MiB) on mem 4 KiB b64 fire-and-forget, 2 trials.
#
# Overrides (everything else is the product default):
#   broker  FELIX_QUIC_LISTENERS=4 (what an unset value derives on 8 cores),
#           FELIX_DURABLE_FSYNC_MODE=on_commit (periodic for the periodic latency
#           cells), and from lib.sh: FELIX_ACK_ON_COMMIT=1 (an acked durable
#           publish means durable), FELIX_STORAGE_IO_URING=1,
#           FELIX_IO_RUNTIME_THREADS=0, UDP buffers 24 MiB,
#           FELIX_MTU_UPPER_BOUND=3872 (NIC MTU is 3900).
#   client  UDP buffers 24 MiB and FELIX_MTU_UPPER_BOUND=3872. No connection
#           pool, ack-eliciting threshold or shard-stream override.
#   sweeps  the listener arms set FELIX_QUIC_LISTENERS; 8 listeners also set
#           FELIX_PUB_CONN_POOL=8 (one connection per port). The window arms set
#           FELIX_BROKER_PUB_{CONN,STREAM}_RECV_WINDOW.
#   Set CAMP_INGRESS_WAIT=1 to add FELIX_PUB_INGRESS_WAIT=1 to every broker.
#
# The plan is trimmed to fit MAX_HOURS (18), in this order: 64 KiB b1 cells,
# plain-key b64, listeners 1 and 8, window 64 MiB and, if that is still over,
# the 4-in-flight arm of the latency curve, then the second sweep trial. The level is chosen on the
# full plan, saved in the results dir and reused on resume (TRIM=n overrides).
set -uo pipefail

self="$(cd "$(dirname "$0")" && pwd)"
if [ -f "${self}/lib.sh" ]; then here="${self}"; else here="$(cd "${self}/../.." && pwd)"; fi
logs_dir="${here}/sessions/logs"
mkdir -p "${logs_dir}"
PLOG="${logs_dir}/final-campaign.log"     # progress: one line per step and finished cell
RUNOUT="${logs_dir}/final-campaign.out"   # full harness output
LOCK="${logs_dir}/final-campaign.pid"

dry=0
TRIM_ARG=""
for a in "$@"; do
  case "${a}" in
    --dry-run) dry=1 ;;
    --trim=*) TRIM_ARG="${a#--trim=}" ;;
    *) echo "usage: $0 [--dry-run] [--trim=0..6]" >&2; exit 2 ;;
  esac
done

: "${SESSION:=<session>}"
: "${BUILD_SHA:=<BUILD_SHA>}"
: "${P9_SHA:=71960125bd9a9dee61115a7553f21342a1d136d1}"      # v0.6.0-preview.9
: "${OLD_LOADGEN:=13b6003e548cf5fd7f9c62813549e36577b5a261}" # the original loadgen (#900)
: "${PER:=16}"            # publishers per generator
: "${KEYS:=48}"
: "${NGEN:=4}"
: "${CELL_SECS:=90}"
: "${INFLIGHT:=64}"
: "${GATE_TRIALS:=2}"
: "${GATE_PCT:=3}"
: "${HEAD_TRIALS:=3}"
: "${MAIN_TRIALS:=2}"
: "${LAT_TRIALS:=3}"
: "${CURVE_TRIALS:=2}"
: "${SW_TRIALS:=2}"
: "${LAT_WARMUP:=2000}"
: "${LAT_TOTAL:=20000}"
: "${STREAM_TUNE:=48 96 192}"
: "${AB_PAIRS:=dur-b1 dur-a64 inmem-b1 ff per-b64}"
: "${AB_PAYLOADS:=4096 256}"
: "${AB_TRIALS:=2}"
: "${LAT_PAIRS:=oncommit periodic inmem core}"
: "${LAT_PAYLOADS:=256 4096}"
: "${NATS_LAT_TRIALS:=2}"
: "${FIO_MIN:=1100}"
: "${CAMP_INGRESS_WAIT:=0}"
# Wall-clock minutes per cell, in tenths (run-command overhead dominates).
: "${MIN_ING_X10:=55}"
: "${MIN_LAT_X10:=40}"
: "${SETUP_MIN:=90}"     # start, repair, preflight, and builds that are missing
: "${NATS_INSTALL_MIN:=25}"
: "${MAX_HOURS:=18}"
: "${TRIM_FORCE:=}"
: "${PRICE_PER_HOUR:=1.55}"

BASE_B="FELIX_QUIC_LISTENERS=4 FELIX_DURABLE_FSYNC_MODE=on_commit"
[ "${CAMP_INGRESS_WAIT}" = 1 ] && BASE_B="${BASE_B} FELIX_PUB_INGRESS_WAIT=1"
CLIENT_B="FELIX_UDP_RECV_BUFFER=25165824 FELIX_UDP_SEND_BUFFER=25165824 FELIX_MTU_UPPER_BOUND=3872"
OUT="${OUT:-${here}/sessions/${SESSION}-final-results}"

# ------------------------------------------------------------------ the plan
# Entry: name|step|kind|ref|benv|cenv|loadgen|stream|payload|batch|inflight|shards|gens|check-listeners
#   kind ingest|lat|block; loadgen new|old; inflight ff or a count; shards 48, 1 or none.
PSTEP=(); PSPEC=()
add() {
  PSTEP+=("$2")
  PSPEC+=("$1|$2|$3|$4|$5|$6|$7|$8|$9|${10}|${11}|${12}|${13}|${14:-0}")
}
stream_of() { [ "$1" = dur ] && echo perf-durable || echo perf; }
flname() { [ "$1" = ff ] && echo ff || echo ack; }

# ing <name> <step> <ref> <benv> <cenv> <loadgen> <mem|dur> <payload> <batch> <ff|inflight> <spread|plain|none> [check]
ing() {
  local shards=48
  [ "${11}" = plain ] && shards=1
  [ "${11}" = none ] && shards=none
  add "$1" "$2" ingest "$3" "$4" "$5" "$6" "$(stream_of "$7")" "$8" "$9" "${10}" "${shards}" "${NGEN}" "${12:-0}"
}
shape_label() { printf '%s-p%s-b%s-%s-%s' "$1" "$2" "$3" "$(flname "$4")" "$5"; }

# The headline cells; m1 runs them to HEAD_TRIALS and m2 skips them.
is_headline() {   # m p b fl k
  case "$1 $2 $3 $4 $5" in
    "mem 4096 64 ff spread"|"mem 256 1 ack spread"|"dur 4096 64 ack spread") return 0 ;;
  esac
  return 1
}
# The gate shapes: m p b fl k.
GATE_SHAPES=("mem 256 1 ack spread" "mem 256 64 ack spread" "mem 256 1 ack plain" "mem 256 64 ack plain"
             "mem 4096 64 ff spread" "dur 4096 64 ack spread")

TRIM=0
trim_has() { [ "${TRIM}" -ge "$1" ]; }

build_plan() {
  local t m p b f k s pr n w order arm fl benv fs st
  PSTEP=(); PSPEC=()
  # s0: BUILD_SHA broker vs preview.9 broker, same client; arms alternate by shape and trial.
  for t in $(seq 1 "${GATE_TRIALS}"); do
    for s in "${GATE_SHAPES[@]}"; do
      set -- ${s}
      order="new p9"; [ $((t % 2)) = 0 ] && order="p9 new"
      for arm in ${order}; do
        local ref="${BUILD_SHA}"; [ "${arm}" = p9 ] && ref="${P9_SHA}"
        ing "s0-${arm}-$(shape_label "$1" "$2" "$3" "$4" "$5")-t${t}" s0 "${ref}" "${BASE_B}" "" new "$1" "$2" "$3" "$(fl_of "$4")" "$5"
      done
    done
  done
  # s0b: site headline (original loadgen vs current), then the profiled cell.
  for t in $(seq 1 "${GATE_TRIALS}"); do
    order="old cur"; [ $((t % 2)) = 0 ] && order="cur old"
    for arm in ${order}; do
      case "${arm}" in
        old) ing "s0b-old-mem-p4096-b64-ff-t${t}" s0b "${BUILD_SHA}" "${BASE_B}" "" old mem 4096 64 ff none ;;
        cur) ing "s0b-cur-mem-p4096-b64-ff-spread-t${t}" s0b "${BUILD_SHA}" "${BASE_B}" "" new mem 4096 64 ff spread ;;
      esac
    done
  done
  ing "s0-profile-mem-p4096-b64-ff-spread" s0b "${BUILD_SHA}-fp" "${BASE_B}" "" new mem 4096 64 ff spread
  # m1: headline cells to HEAD_TRIALS.
  for t in $(seq 1 "${HEAD_TRIALS}"); do
    for s in "mem 4096 64 ff spread" "mem 256 1 ack spread" "dur 4096 64 ack spread"; do
      set -- ${s}
      ing "m-$(shape_label "$1" "$2" "$3" "$4" "$5")-t${t}" m1 "${BUILD_SHA}" "${BASE_B}" "" new "$1" "$2" "$3" "$(fl_of "$4")" "$5"
    done
  done
  # lat: one in flight, then the load curve.
  for t in $(seq 1 "${LAT_TRIALS}"); do
    for pr in oncommit periodic inmem; do for p in 256 4096; do
      fs=on_commit; st=perf-durable
      [ "${pr}" = periodic ] && fs=periodic
      [ "${pr}" = inmem ] && { fs=periodic; st=perf; }
      add "l1-${pr}-p${p}-t${t}" lat lat "${BUILD_SHA}" "FELIX_QUIC_LISTENERS=4 FELIX_DURABLE_FSYNC_MODE=${fs}" "" new "${st}" "${p}" 1 ff none 1 0
    done; done
  done
  for t in $(seq 1 "${CURVE_TRIALS}"); do
    for m in mem dur; do for p in 256 4096; do for f in 1 4 16; do
      trim_has 5 && [ "${f}" = 4 ] && continue
      ing "lc-${m}-p${p}-f${f}-t${t}" lat "${BUILD_SHA}" "${BASE_B}" "" new "${m}" "${p}" 1 "${f}" spread
    done; done; done
  done
  # net: blocks run by their own scripts; one plan line each.
  add nats-install net block "" "" "" "" "" "" "" "" "" "" 0
  add nats-tune net block "" "" "" "" "" "" "" "" "" "" 0
  add nats-ab net block "" "" "" "" "" "" "" "" "" "" 0
  add nats-lat net block "" "" "" "" "" "" "" "" "" "" 0
  # m2: everything but the headline cells.
  for t in $(seq 1 "${MAIN_TRIALS}"); do
    for m in mem dur; do for p in 256 4096 65536; do for b in 1 64; do for f in ff "${INFLIGHT}"; do
      trim_has 1 && [ "${p}" = 65536 ] && [ "${b}" = 1 ] && continue
      is_headline "${m}" "${p}" "${b}" "$(flname "${f}")" spread && continue
      ing "m-$(shape_label "${m}" "${p}" "${b}" "${f}" spread)-t${t}" m2 "${BUILD_SHA}" "${BASE_B}" "" new "${m}" "${p}" "${b}" "${f}" spread
    done; done; done; done
    for b in 1 64; do
      [ "${b}" = 64 ] && trim_has 2 && continue
      ing "m-$(shape_label mem 256 "${b}" "${INFLIGHT}" plain)-t${t}" m2 "${BUILD_SHA}" "${BASE_B}" "" new mem 256 "${b}" "${INFLIGHT}" plain
    done
  done
  # sw: listeners, then receive windows (mem 4 KiB b64 fire-and-forget).
  for t in $(seq 1 "${SW_TRIALS}"); do
    trim_has 6 && [ "${t}" -gt 1 ] && continue
    order="1 2 4 8"; [ $((t % 2)) = 0 ] && order="8 4 2 1"
    for n in ${order}; do
      trim_has 3 && { [ "${n}" = 1 ] || [ "${n}" = 8 ]; } && continue
      local cenv=""; [ "${n}" -gt 4 ] && cenv="FELIX_PUB_CONN_POOL=${n}"
      benv="FELIX_QUIC_LISTENERS=${n} FELIX_DURABLE_FSYNC_MODE=on_commit"
      [ "${CAMP_INGRESS_WAIT}" = 1 ] && benv="${benv} FELIX_PUB_INGRESS_WAIT=1"
      ing "sw-l${n}-mem-p4096-b64-ff-t${t}" sw "${BUILD_SHA}" "${benv}" "${cenv}" new mem 4096 64 ff spread "${n}"
    done
    order="default 32 64"; [ $((t % 2)) = 0 ] && order="64 32 default"
    for w in ${order}; do
      trim_has 4 && [ "${w}" = 64 ] && continue
      benv="${BASE_B}"
      [ "${w}" != default ] && benv="${benv} FELIX_BROKER_PUB_CONN_RECV_WINDOW=$((w * 1048576)) FELIX_BROKER_PUB_STREAM_RECV_WINDOW=$((w * 1048576))"
      ing "sw-w${w}-mem-p4096-b64-ff-t${t}" sw "${BUILD_SHA}" "${benv}" "" new mem 4096 64 ff spread
    done
  done
}
fl_of() { [ "$1" = ff ] && echo ff || echo "${INFLIGHT}"; }

# ------------------------------------------------------------- bookkeeping
is_done() { [ -e "${OUT}/cells/${1}/done" ] || [ -e "${OUT}/final/${1}.done" ]; }
spec_name() { printf '%s' "${1%%|*}"; }
count_done() { local n=0 d; for d in "${OUT}/cells/"$1/; do [ -e "${d}done" ] && n=$((n + 1)); done; echo "${n}"; }
nwords() { set -- $1; echo "$#"; }

# Cells each block runs, for the estimate: tune = streams x 2 modes; ab = pairs x payloads x trials x 2
# systems; lat = trials x payloads x (2 per Felix pair + 1 for core).
block_cells() {
  local n
  case "$1" in
    nats-tune) echo $(( $(nwords "${STREAM_TUNE}") * 2 )) ;;
    nats-ab) echo $(( $(nwords "${AB_PAIRS}") * $(nwords "${AB_PAYLOADS}") * AB_TRIALS * 2 )) ;;
    nats-lat)
      n=$(( $(nwords "${LAT_PAIRS}") * 2 ))
      case " ${LAT_PAIRS} " in *" core "*) n=$((n - 1)) ;; esac
      echo $(( n * $(nwords "${LAT_PAYLOADS}") * NATS_LAT_TRIALS )) ;;
    *) echo 0 ;;
  esac
}
block_total_x10() {
  case "$1" in
    nats-install) echo $((NATS_INSTALL_MIN * 10)) ;;
    nats-tune|nats-ab) echo $(( $(block_cells "$1") * MIN_ING_X10 )) ;;
    nats-lat) echo $(( $(block_cells "$1") * MIN_LAT_X10 )) ;;
  esac
}
block_pending_x10() {   # the block's estimate less the cells already done
  local tot done_n=0 per="${MIN_ING_X10}" r
  tot="$(block_total_x10 "$1")"
  [ "${IGNORE_DONE:-0}" = 1 ] && { echo "${tot}"; return; }
  is_done "$1" && { echo 0; return; }
  case "$1" in
    nats-tune) done_n="$(count_done 'nats-stream-tune-*')" ;;
    nats-ab) done_n="$(count_done 'ab-*')" ;;
    nats-lat) done_n=$(( $(count_done 'felix-lat-*') + $(count_done 'nats-lat-*') )); per="${MIN_LAT_X10}" ;;
  esac
  r=$(( tot - done_n * per ))
  [ "${r}" -lt 0 ] && r=0
  echo "${r}"
}
spec_x10() {
  local name kind
  IFS='|' read -r name _ kind _ <<<"$1"
  if [ "${kind}" = block ]; then block_pending_x10 "${name}"
  elif is_done "${name}" && [ "${IGNORE_DONE:-0}" != 1 ]; then echo 0
  elif [ "${kind}" = lat ]; then echo "${MIN_LAT_X10}"
  else echo "${MIN_ING_X10}"; fi
}
compute_rem() {   # REM[i] = pending tenths of minutes of entries i..end
  local i acc=0
  M=${#PSPEC[@]}
  REM=()
  for ((i = M; i >= 0; i--)); do
    [ "${i}" -lt "${M}" ] && acc=$((acc + $(spec_x10 "${PSPEC[i]}")))
    REM[i]=${acc}
  done
}
hours_of() { awk -v x="$1" 'BEGIN { printf "%.1f", x / 600 }'; }
total_x10() { echo $(( REM[0] + SETUP_MIN * 10 )); }

# Pick the lowest trim level whose full plan fits MAX_HOURS.
choose_trim() {
  local saved="${OUT}/final/trim" lvl
  if [ -n "${TRIM_ARG:-${TRIM_FORCE}}" ]; then TRIM="${TRIM_ARG:-${TRIM_FORCE}}"; build_plan; compute_rem; return; fi
  if [ -s "${saved}" ] && [ "${dry}" = 0 ]; then TRIM="$(cat "${saved}")"; build_plan; compute_rem; return; fi
  for lvl in 0 1 2 3 4 5 6; do
    TRIM="${lvl}"; build_plan
    IGNORE_DONE=1 compute_rem
    [ "$(total_x10)" -le $((MAX_HOURS * 600)) ] && break
  done
  [ "${dry}" = 0 ] && { mkdir -p "${OUT}/final"; echo "${TRIM}" > "${saved}"; }
  IGNORE_DONE=0 compute_rem
}
trim_text() {
  case "$1" in
    0) echo "nothing" ;;
    1) echo "64 KiB b1 cells" ;;
    2) echo "64 KiB b1 cells, plain-key b64" ;;
    3) echo "64 KiB b1 cells, plain-key b64, listeners 1 and 8" ;;
    4) echo "64 KiB b1 cells, plain-key b64, listeners 1 and 8, window 64 MiB" ;;
    5) echo "64 KiB b1 cells, plain-key b64, listeners 1 and 8, window 64 MiB, latency curve at 4 in flight" ;;
    6) echo "64 KiB b1 cells, plain-key b64, listeners 1 and 8, window 64 MiB, latency curve at 4 in flight, second trial of the sweeps" ;;
  esac
}

# ----------------------------------------------------------------- dry run
if [ "${dry}" = 1 ]; then
  choose_trim
  echo "session ${SESSION}  results ${OUT}"
  echo "BUILD_SHA ${BUILD_SHA}"
  echo "P9_SHA    ${P9_SHA}"
  echo "OLD_LOADGEN ${OLD_LOADGEN}"
  echo "broker env: ${BASE_B} (+ lib.sh base: ACK_ON_COMMIT=1 STORAGE_IO_URING=1 IO_RUNTIME_THREADS=0 UDP buffers 24 MiB, MTU_UPPER_BOUND=3872)"
  echo "client env: ${CLIENT_B}"
  echo "cell run: ${CELL_SECS} s, ${PER} publishers/generator x ${NGEN} generators, ${KEYS} keys, shared --start-at, in flight ${INFLIGHT} when acked"
  echo "trim level ${TRIM}: trimmed: $(trim_text "${TRIM}")"
  echo
  for ((i = 0; i < M; i++)); do
    IFS='|' read -r name step kind ref benv cenv lg stream p b f sh gens chk <<<"${PSPEC[i]}"
    st=""; is_done "${name}" && st=" [done]"
    if [ "${kind}" = block ]; then
      printf '[%3d/%d] %-44s %-4s block: %s cells, est %s min%s\n' "$((i + 1))" "${M}" "${name}" "${step}" "$(block_cells "${name}")" "$(( $(block_total_x10 "${name}") / 10 ))" "${st}"
      continue
    fi
    printf '[%3d/%d] %-44s %-4s %-6s %s lg=%s %-12s p=%-5s b=%-2s %-4s keys=%-5s est=%sm%s\n' "$((i + 1))" "${M}" "${name}" "${step}" "${kind}" "${ref:0:8}" "${lg}" "${stream}" "${p}" "${b}" "${f}" "${sh}" "$(awk -v x="$(spec_x10 "${PSPEC[i]}")" 'BEGIN { printf "%.1f", x / 10 }')" "${st}"
  done
  echo
  echo "NATS block detail (run by nats/*.sh, resumable):"
  echo "  nats-tune: STREAM_TUNE='${STREAM_TUNE}' x default,memory modes (nats/stream-tune.sh); best stream count becomes AB_STREAMS"
  echo "  nats-ab: AB_PAIRS='${AB_PAIRS}' AB_PAYLOADS='${AB_PAYLOADS}' AB_TRIALS=${AB_TRIALS} (interleave.sh), Felix side at ${BUILD_SHA:0:8}, ${BASE_B}"
  echo "  nats-lat: LAT_PAIRS='${LAT_PAIRS}' LAT_PAYLOADS='${LAT_PAYLOADS}' TRIALS=${NATS_LAT_TRIALS} (nats-latency.sh)"
  echo "  curve: in flight 64 reuses the matrix cells m-{mem,dur}-p{256,4096}-b1-ack-spread"
  echo
  for s in s0 s0b m1 lat net m2 sw; do
    n=0; x=0
    for ((i = 0; i < M; i++)); do
      [ "${PSTEP[i]}" = "${s}" ] || continue
      n=$((n + 1)); x=$((x + $(spec_x10 "${PSPEC[i]}")))
    done
    [ "${n}" = 0 ] || printf 'step %-4s %3d plan lines  ~%s h\n' "${s}" "${n}" "$(hours_of "${x}")"
  done
  cells=0
  for ((i = 0; i < M; i++)); do
    IFS='|' read -r name _ kind _ <<<"${PSPEC[i]}"
    if [ "${kind}" = block ]; then cells=$((cells + $(block_cells "${name}"))); else cells=$((cells + 1)); fi
  done
  tot="$(total_x10)"
  printf 'setup %d min (start, repair, preflight, missing builds)\n' "${SETUP_MIN}"
  printf 'cells %d (%d plan lines), total about %s h, estimated cost $%s at $%s/h (budget %s h)\n' "${cells}" "${M}" "$(hours_of "${tot}")" \
    "$(awk -v x="${tot}" -v p="${PRICE_PER_HOUR}" 'BEGIN { printf "%.0f", x / 600 * p }')" "${PRICE_PER_HOUR}" "${MAX_HOURS}"
  exit 0
fi

# ---------------------------------------------------------------- live run
if [ -s "${LOCK}" ] && kill -0 "$(cat "${LOCK}")" 2>/dev/null; then
  echo "!! final-campaign already running (pid $(cat "${LOCK}"))" >&2; exit 1
fi
[[ "${BUILD_SHA}" =~ ^[0-9a-f]{40}$ ]] || { echo "!! BUILD_SHA must be a 40-char commit sha" >&2; exit 2; }
[ -f "${here}/sessions/${SESSION}.env" ] || { echo "!! no sessions/${SESSION}.env" >&2; exit 2; }
echo $$ > "${LOCK}"
VMS_STARTED=0
dealloc() {
  [ "${VMS_STARTED}" = 1 ] && [ "${NO_DEALLOC:-0}" != 1 ] || return 0
  # shellcheck disable=SC2046
  if az vm deallocate --ids $(az vm list -g "${GROUP}" --query '[].id' -o tsv) -o none; then
    plog "${GROUP} deallocated"
  else
    plog "FAIL deallocate: ${GROUP} may still be running"
  fi
}
on_exit() { dealloc; rm -f "${LOCK}"; }
trap on_exit EXIT
trap 'plog "interrupted"; exit 143' INT TERM
exec >> "${RUNOUT}" 2>&1

cd "${here}" || exit 1
session="${SESSION}"
set -a; source session.env; SESSION="${session}"; source "sessions/${SESSION}.env"; set +a
SESSION="${session}"
export SESSION OUT BUILD_SHA
mkdir -p "${OUT}/system" "${OUT}/final" "${here}/sessions/${SESSION}-results/system"

eta() {   # eta <tenths of minutes from now> -> HH:MM UTC
  local at=$(( $(date +%s) + $1 * 6 ))
  date -u -r "${at}" +%H:%M 2>/dev/null || date -u -d "@${at}" +%H:%M
}
plog() { printf '%s UTC  %s\n' "$(date -u +%H:%M)" "$*" >> "${PLOG}"; printf '%s PROGRESS %s\n' "$(date -u +%H:%M:%S)" "$*"; }
die() { plog "FAIL $*"; plog DONE; exit 1; }

# shellcheck source=lib.sh
source "${here}/lib.sh"
BROKER_VM="$(broker_vm 0)"
read -r -a GENS <<<"${LOADGENS}"

choose_trim
plog "[0/${M}]  START final campaign on ${BUILD_SHA:0:8} vs ${P9_SHA:0:8} (${SESSION}), trim ${TRIM} ($(trim_text "${TRIM}"))  ETA $(eta "$(total_x10)")"

# ------------------------------------------------------------------- setup
do_mtu() {
  local mtu vm
  mtu='dev=$(ip route show default | awk "{print \$5; exit}"); ip link set dev $dev mtu 3900; for vf in /sys/class/net/$dev/lower_*; do [ -e "$vf" ] && ip link set dev "$(basename $vf | sed s/^lower_//)" mtu 3900; done; systemctl disable --now unattended-upgrades >/dev/null 2>&1; systemctl mask unattended-upgrades >/dev/null 2>&1; echo "$(hostname) mtu=$(cat /sys/class/net/$dev/mtu) uu=$(systemctl is-enabled unattended-upgrades 2>&1)"'
  for vm in $(az vm list -g "${GROUP}" --query '[].name' -o tsv); do
    (run_on_str "${vm}" "${mtu}" 2>&1 | grep 'mtu=') &
  done
  wait
}

# do_preflight: one line per check, PREFLIGHT OK or PREFLIGHT FAIL.
do_preflight() {
  local bip="${BROKER_IPS%%,*}" fail=0 vm out r mbs f
  nic='dev=$(ip route show default | awk "{print \$5; exit}"); echo "mtu=$(cat /sys/class/net/$dev/mtu) uu=$(systemctl is-enabled unattended-upgrades 2>&1)"'
  for vm in $(az vm list -g "${GROUP}" --query '[].name' -o tsv); do
    out="$(run_on_str "${vm}" "${nic}; case ${vm} in *loadgen*) ping -M do -s 3872 -c 3 -W 2 ${bip} >/dev/null 2>&1 && echo path=ok || echo path=bad;; esac" 2>&1 | grep 'mtu=\|path=' | tr '\n' ' ')"
    r=BAD
    case "${out}" in *mtu=3900*uu=masked*) r=OK ;; esac
    case "${vm}" in *loadgen*) case "${out}" in *path=ok*) ;; *) r=BAD ;; esac ;; esac
    printf '%-28s %s %s\n' "${vm}" "${r}" "${out}"; [ "${r}" = OK ] || fail=1
  done
  out="$(run_on_str "${BROKER_VM}" 'findmnt -n -o SOURCE /data; systemctl is-active felix-broker; grep -c NODE_REFRESH_TOKEN_FILE /etc/felix/broker.env; test -s /etc/felix/node.refresh_token && echo refresh=yes' 2>&1 | grep -v '^\[std\|Enable succ\|__RUNOK' | tr '\n' ' ')"
  case "${out}" in
    */dev/md0*active*1*refresh=yes*) printf '%-28s OK %s\n' broker-storage-auth "${out}" ;;
    *) printf '%-28s BAD %s\n' broker-storage-auth "${out}"; fail=1 ;;
  esac
  f="${OUT}/system/${BROKER_VM}.fio.txt"
  mbs="$(grep -h 'seq256k-j1.mb_s=' "${f}" 2>/dev/null | cut -d= -f2 | cut -d. -f1)"
  if [ -n "${mbs}" ] && [ "${mbs}" -ge "${FIO_MIN}" ]; then printf '%-28s OK %s MB/s\n' fio "${mbs}"; else printf '%-28s BAD %s\n' fio "${mbs:-none}"; fail=1; fi
  [ "${fail}" = 0 ] && echo "PREFLIGHT OK" || echo "PREFLIGHT FAIL"
}

setup_session() {
  local down f
  down="$(az vm list -g "${GROUP}" -d --query "[?powerState!='VM running'].name" -o tsv 2>/dev/null)"
  if [ -z "${down}" ] && [ -e "${OUT}/final/setup.ok" ]; then
    VMS_STARTED=1
    plog "setup: session already running and prepared; skipping"
    return 0
  fi
  rm -f "${OUT}/final/setup.ok"
  VMS_STARTED=1   # from here a failure must not leave VMs running
  plog "setup: starting ${GROUP}"
  # shellcheck disable=SC2046
  az vm start --ids $(az vm list -g "${GROUP}" --query '[].id' -o tsv) -o none || die "start ${GROUP}"
  run_on_str "${BROKER_VM}" 'systemctl stop felix-broker; /usr/local/sbin/felix-mount-data.sh >/tmp/mount.log 2>&1; echo __RUNOK__' >/dev/null 2>&1
  do_mtu > "${OUT}/final/mtu.txt" 2>&1
  SESSION="${SESSION}" "${here}/reseed.sh" > "${OUT}/final/reseed.txt" 2>&1 || die "reseed (see ${OUT}/final/reseed.txt)"
  f="${OUT}/system/${BROKER_VM}.fio.txt"
  [ -f "${f}" ] && mv "${f}" "${f%.txt}.pre-final-$(date +%H%M).txt"
  SESSION="${SESSION}" "${here}/fio-baseline.sh" > "${OUT}/final/fio.txt" 2>&1
  do_preflight > "${OUT}/final/preflight.txt" 2>&1
  if ! grep -q 'PREFLIGHT OK' "${OUT}/final/preflight.txt"; then
    plog "FAIL preflight (see ${OUT}/final/preflight.txt)"; plog DONE; exit 1
  fi
  plog "setup: preflight OK"
  touch "${OUT}/final/setup.ok"
}

have_label() {   # installed on the broker, not just listed in the inventory
  agent_on "${BROKER_VM}" "test -e /opt/felix/$1/REF" >/dev/null 2>&1
}
ensure_builds() {
  local lbl tag sha got
  for lbl in "${BUILD_SHA}" "${P9_SHA}" "${BUILD_SHA}-fp"; do
    have_label "${lbl}" && continue
    plog "build: ${lbl:0:8}$([ "${lbl}" != "${lbl%-fp}" ] && echo -fp) not installed; deploy-ref.sh (15-25 min)"
    SESSION="${SESSION}" "${here}/deploy-ref.sh" "${lbl%-fp}" $([ "${lbl}" != "${lbl%-fp}" ] && echo --fp) > "${OUT}/final/build-${lbl:0:12}.txt" 2>&1 \
      || die "build ${lbl} (see ${OUT}/final/build-${lbl:0:12}.txt)"
  done
  # Side-by-side loadgens: old and new stay on every generator, a cell swaps one in with cp.
  for tag in old new; do
    sha="${OLD_LOADGEN}"; [ "${tag}" = new ] && sha="${BUILD_SHA}"
    got="$(agent_on "${GENS[$((${#GENS[@]} - 1))]}" "cat /etc/felix/loadgen.${tag}.ref 2>/dev/null || true" 2>/dev/null | grep -E '^[0-9a-f]{40}$' | head -1)"
    [ "${got}" = "${sha}" ] && continue
    plog "build: loadgen ${tag} ${sha:0:8}"
    SESSION="${SESSION}" "${here}/deploy-loadgen.sh" "${sha}" > "${OUT}/final/build-loadgen-${tag}.txt" 2>&1 \
      || die "loadgen ${tag} build (see ${OUT}/final/build-loadgen-${tag}.txt)"
    par_on "${OUT}/system" "lgstash-${tag}" "cp /usr/local/bin/felix-loadgen /usr/local/bin/felix-loadgen.${tag}
echo ${sha} > /etc/felix/loadgen.${tag}.ref" "${GENS[@]}" || die "stash loadgen ${tag}"
  done
}

CURRENT_LG=""
use_lg() {   # old|new: swap the generators' felix-loadgen
  [ "${CURRENT_LG}" = "$1" ] && return 0
  par_on "${OUT}/system" "lguse" "cp /usr/local/bin/felix-loadgen.$1 /usr/local/bin/felix-loadgen" "${GENS[@]}" || return 1
  local sha="${BUILD_SHA}"; [ "$1" = old ] && sha="${OLD_LOADGEN}"
  echo "${sha}@${sha}" > "${here}/sessions/${SESSION}.loadgen-ref"
  CURRENT_LG="$1"
}

collect_facts() {
  local cmds
  {
    echo "session=${SESSION} region=${LOCATION} group=${GROUP}"
    echo "BUILD_SHA=${BUILD_SHA} P9_SHA=${P9_SHA} OLD_LOADGEN=${OLD_LOADGEN}"
    echo "trim=${TRIM} ($(trim_text "${TRIM}"))"
    echo "driver BASE_B=${BASE_B}"
    echo "driver CLIENT_B=${CLIENT_B}"
    echo "CELL_SECS=${CELL_SECS} PER=${PER} NGEN=${NGEN} KEYS=${KEYS} INFLIGHT=${INFLIGHT}"
    echo "lib.sh base_overrides:"; base_overrides | sed 's/^/  /'
    echo "sweep and NATS settings: STREAM_TUNE=${STREAM_TUNE} AB_PAIRS=${AB_PAIRS} AB_PAYLOADS=${AB_PAYLOADS} AB_TRIALS=${AB_TRIALS} LAT_PAIRS=${LAT_PAIRS} LAT_PAYLOADS=${LAT_PAYLOADS} NATS_LAT_TRIALS=${NATS_LAT_TRIALS}"
  } > "${OUT}/final/config.txt"
  az vm list -g "${GROUP}" -d --query "[].{name:name,size:hardwareProfile.vmSize,location:location,zones:zones,power:powerState,ip:privateIps}" -o table > "${OUT}/final/vm-sizes.txt" 2>&1 || true
  cmds='echo "== kernel"; uname -a; cat /proc/cmdline; head -3 /etc/os-release
echo "== cpu"; lscpu
echo "== memory"; free -m; cat /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null
echo "== nic"; dev=$(ip route show default | awk "{print \$5; exit}"); ip -d link show "$dev"; ethtool -i "$dev" 2>/dev/null; ethtool -l "$dev" 2>/dev/null; ethtool -g "$dev" 2>/dev/null
echo "== sysctl"; sysctl -a 2>/dev/null | grep -E "^(net\.core|net\.ipv4\.udp|net\.ipv4\.tcp_[rw]mem|vm\.(swappiness|dirty_[a-z_]+|max_map_count)|kernel\.(io_uring_disabled|sched_[a-z_]+))"
echo "== limits"; ulimit -n
echo "== tools"; fio --version 2>/dev/null; perf --version 2>/dev/null
echo "== felix env (secrets removed)"; for f in /etc/felix/*.env; do [ -e "$f" ] && { echo "-- $f"; grep -vE "TOKEN|SECRET|PASSWORD|BOOTSTRAP" "$f"; }; done
echo "== units"; systemctl cat felix-broker 2>/dev/null | grep -vE "TOKEN|SECRET|PASSWORD|BOOTSTRAP"
echo "== data"; df -h /data 2>/dev/null; findmnt -no SOURCE,FSTYPE,OPTIONS /data 2>/dev/null; cat /proc/mdstat 2>/dev/null'
  par_on "${OUT}/system" facts "${cmds}" "${BROKER_VM}" "${GENS[@]}" || true
}

# --------------------------------------------------------- cell machinery
source "${here}/cells.sh"
export NATS_MTU=3900
export SESSION CELL_SECS RESUME=1 WIPE_DURABLE=1 PROFILE=0 PROFILE_CELLS='^s0-profile' PROFILE_SECS=40
export EXTRA_LOADGEN_ENV="${CLIENT_B}"
export EXTRA_BROKER_ENV="FELIX_MTU_UPPER_BOUND=3872"   # applied last by merge_env
NATS_ACTIVE=1   # unknown at start: stop NATS and wipe before the first Felix cell
LOGGED="${OUT}/final/logged.txt"; touch "${LOGGED}"
mark_logged() { echo "$1" >> "${LOGGED}"; }

ackpcts() {
  grep -h '^LOADGEN_JSON' "$1"/*.run.txt 2>/dev/null | python3 -I -c '
import json, sys, statistics as st
a, d = [], []
for l in sys.stdin:
    try:
        j = json.loads(l.split(" ", 1)[1])
    except Exception:
        continue
    x = j.get("ack_latency_us") or j.get("batch_ack_latency_us")
    if x:
        a.append(x)
    y = j.get("delivery_latency_us")
    if y:
        d.append(y)
def f(rs):
    return "/".join(str(int(st.median([r.get(k, 0) for r in rs]))) for k in ("p50", "p99", "p999")) if rs else "-"
print("ack p50/p99/p999 us %s" % f(a) + (", delivery %s" % f(d) if d else ""))'
}
# cell_nums <dir>: append ingress drops cores
cell_nums() {
  python3 -I "${here}/summarize.py" --cell "$1" 2>/dev/null \
    | sed -n 's/^steady append \([0-9.]*\) MB\/s, ingress \([0-9.]*\) MB\/s, drops \([0-9.]*\)\/s, cores \([0-9.]*\) .*/\1 \2 \3 \4/p'
}
headline() {
  local dir="$1" a i d c
  case "$(basename "${dir}")" in
    l1-*|*lat-*) ackpcts "${dir}"; return ;;
  esac
  read -r a i d c <<<"$(cell_nums "${dir}")"
  if [ -z "${a}" ]; then echo "no steady window; $(ackpcts "${dir}")"; return; fi
  echo "append ${a} MB/s, ingress ${i} MB/s, drops ${d}/s, cores ${c}; $(ackpcts "${dir}")"
}
metric() {   # metric <cell> <mem|dur>: the number the gate compares
  local a i d c
  read -r a i d c <<<"$(cell_nums "${OUT}/cells/$1")"
  [ "$2" = dur ] && echo "${a}" || echo "${i}"
}

# stop_nats_for_felix: what interleave.sh and nats-latency.sh do before a Felix cell.
stop_nats_for_felix() {
  agent_on "${BROKER_VM}" "systemctl stop nats 2>/dev/null || true
felix-agent wipe
fstrim -v /data >/dev/null 2>&1 || true" > "${OUT}/system/final-prep.txt" 2>&1 || return 1
  CURRENT_REF=""; CURRENT_OVERRIDES=""
  NATS_ACTIVE=0
}

run_entry() {   # 0 ran, 2 already done, 1 could not start
  local name step kind ref benv cenv lg stream p b f sh gens chk flags=() shv
  IFS='|' read -r name step kind ref benv cenv lg stream p b f sh gens chk <<<"$1"
  is_done "${name}" && { log "skip ${name} (done)"; return 2; }
  if [ "${NATS_ACTIVE}" = 1 ]; then
    stop_nats_for_felix || { echo "!! ${name}: prep failed; see ${OUT}/system/final-prep.txt" >&2; return 1; }
  fi
  use_lg "${lg}" || { echo "!! ${name}: could not install the ${lg} loadgen" >&2; return 1; }
  # shellcheck disable=SC2086 # benv is a list of KEY=VALUE words
  stage "${ref}" ${benv}
  if [ "${kind}" = lat ]; then
    flags=(--scenario pubsub --stream "${stream}" --payload-bytes "${p}" --fanout 1 --batch 1 --warmup "${LAT_WARMUP}" --total "${LAT_TOTAL}")
  else
    shv="${sh}"; [ "${sh}" = none ] && shv=""
    [ "${f}" = ff ] && f=""
    # shellcheck disable=SC2207 # ingest_flags prints words
    flags=($(SHARDS="${shv}" PAYLOAD="${p}" BATCH="${b}" IN_FLIGHT="${f}" ingest_flags "${stream}" "${KEYS}" "${PER}"))
  fi
  if [ "${stream}" = perf-durable ]; then
    CELL_LOADGEN_ENV="${cenv}" durable_cell "${name}" "${gens}" "${flags[@]}"
  else
    CELL_LOADGEN_ENV="${cenv}" cell "${name}" "${gens}" "${flags[@]}"
  fi
  [ "${chk}" = 0 ] || check_listeners "${chk}" || plog "WARN ${name} listeners != ${chk}"
  return 0
}

# ------------------------------------------------------------- stage 0 gate
gate_check() {
  local s i t m p b fl k lbl nv ov v ref line bad=0 incomplete=0 spec name
  # One retry for gate cells that did not finish; a flaky cell should not end the campaign.
  for ((i = 0; i < M; i++)); do
    [ "${PSTEP[i]}" = s0 ] || continue
    name="$(spec_name "${PSPEC[i]}")"
    case "${name}" in s0-profile*) continue ;; esac
    is_done "${name}" || { plog "gate: retrying ${name}"; run_entry "${PSPEC[i]}"; }
  done
  plog "stage0 gate: ${BUILD_SHA:0:8} vs ${P9_SHA:0:8}, fail below -${GATE_PCT}% (median of ${GATE_TRIALS} trials)"
  for s in "${GATE_SHAPES[@]}"; do
    set -- ${s}
    m="$1"; p="$2"; b="$3"; fl="$4"; k="$5"
    lbl="$(shape_label "${m}" "${p}" "${b}" "${fl}" "${k}")"
    nv=""; ov=""
    for t in $(seq 1 "${GATE_TRIALS}"); do
      if is_done "s0-new-${lbl}-t${t}"; then v="$(metric "s0-new-${lbl}-t${t}" "${m}")"; [ -n "${v}" ] && nv="${nv:+${nv},}${v}"; fi
      if is_done "s0-p9-${lbl}-t${t}"; then v="$(metric "s0-p9-${lbl}-t${t}" "${m}")"; [ -n "${v}" ] && ov="${ov:+${ov},}${v}"; fi
    done
    line="$(python3 -I -c '
import sys, statistics as st
lab, new, old, pct = sys.argv[1], [float(x) for x in sys.argv[2].split(",") if x], [float(x) for x in sys.argv[3].split(",") if x], float(sys.argv[4])
if not new or not old:
    print("%s INCOMPLETE (new %d, p9 %d trials)" % (lab, len(new), len(old)))
    sys.exit(2)
n, o = st.median(new), st.median(old)
d = (n / o - 1) * 100 if o else 0.0
print("%s: new %.1f p9 %.1f MB/s, %+.1f%%%s" % (lab, n, o, d, "  SLOWER" if d < -pct else ""))
sys.exit(1 if d < -pct else 0)' "${lbl}" "${nv}" "${ov}" "${GATE_PCT}")"
    case $? in 0) ;; 2) incomplete=1 ;; *) bad=1 ;; esac
    plog "  gate ${line}"
  done
  if [ "${bad}" = 1 ] || [ "${incomplete}" = 1 ]; then
    plog "FAIL stage0: $([ "${bad}" = 1 ] && echo "BUILD_SHA more than ${GATE_PCT}% slower than preview.9 on a cell")$([ "${incomplete}" = 1 ] && echo " (incomplete cells)"); deallocating"
    plog DONE
    exit 1
  fi
  plog "stage0 gate PASS"
}

# ------------------------------------------------------------------- NATS
ensure_nats() {
  local need=0 needlat=0 vm
  agent_on "${BROKER_VM}" "test -x /usr/local/bin/nats-server && test -s /etc/nats/tls/ca.pem" >/dev/null 2>&1 || need=1
  for vm in "${GENS[@]}"; do
    agent_on "${vm}" "test -x /usr/local/bin/nats && test -s /etc/nats/tls/ca.pem" >/dev/null 2>&1 || need=1
  done
  agent_on "${GENS[0]}" "test -x /usr/local/bin/nats-latency" >/dev/null 2>&1 || needlat=1
  if [ "${need}" = 1 ]; then
    plog "NATS not fully installed; nats/install.sh"
    SESSION="${SESSION}" "${here}/nats/install.sh" || return 1
  fi
  if [ "${need}" = 1 ] || [ "${needlat}" = 1 ]; then
    plog "building nats-latency; nats/install-latency.sh ${BUILD_SHA:0:8}"
    SESSION="${SESSION}" "${here}/nats/install-latency.sh" "${BUILD_SHA}" || return 1
  fi
  NATS_ACTIVE=1
}

best_streams() {   # the stream count with the best default-mode append in the tune cells
  local s best="" bv=0 d v
  for s in ${STREAM_TUNE}; do
    d="${OUT}/cells/nats-stream-tune-default-p256-f64-w16-c16-s${s}-t1"
    [ -e "${d}/done" ] || continue
    v="$(cell_nums "${d}" | awk '{ print $1 }')"
    [ -n "${v}" ] || continue
    if awk -v a="${v}" -v b="${bv}" 'BEGIN { exit !(a > b) }'; then best="${s}"; bv="${v}"; fi
  done
  echo "${best}"
}

watch_cells() {   # watch_cells <block> <rem-after-tenths>: log cells a block finishes
  local d name rest
  while :; do
    sleep 150
    for d in "${OUT}/cells/"*/; do
      name="$(basename "${d}")"
      [ -e "${d}done" ] || continue
      case "${name}" in ab-*|nats-stream-tune-*|felix-lat-*|nats-lat-*) ;; *) continue ;; esac
      grep -qxF "${name}" "${LOGGED}" && continue
      rest=$(( $(block_pending_x10 "$1") + $2 ))
      plog "  ${name}  $(headline "${d}")  ETA $(eta "${rest}")"
      mark_logged "${name}"
    done
  done
}

run_block() {   # run_block <name> <rem-after-tenths>
  local name="$1" wpid="" rc=0 bs
  case "${name}" in nats-install) ;; *) watch_cells "${name}" "$2" & wpid=$! ;; esac
  case "${name}" in
    nats-install) ensure_nats || rc=1 ;;
    nats-tune)
      ensure_nats || rc=1
      if [ "${rc}" = 0 ]; then
        STREAM_TUNE="${STREAM_TUNE}" "${here}/nats/stream-tune.sh" || rc=1
        bs="$(best_streams)"
        if [ -n "${bs}" ]; then echo "${bs}" > "${OUT}/final/ab-streams"; plog "NATS stream tune: best ${bs} streams"
        else plog "WARN NATS stream tune gave no usable cell; interleave keeps ${KEYS} streams"; fi
      fi ;;
    nats-ab)
      ensure_nats || rc=1
      if [ "${rc}" = 0 ]; then
        bs="$(cat "${OUT}/final/ab-streams" 2>/dev/null)"
        AB_STREAMS="${bs:-${KEYS}}" AB_PAIRS="${AB_PAIRS}" AB_PAYLOADS="${AB_PAYLOADS}" AB_TRIALS="${AB_TRIALS}" AB_KEYS="${KEYS}" \
          FELIX_BEST_REF="${BUILD_SHA}" FELIX_BEST_ENV="${BASE_B}" FELIX_BEST_CLIENT_ENV="FELIX_MTU_UPPER_BOUND=3872" \
          "${here}/nats/interleave.sh" || rc=1
      fi ;;
    nats-lat)
      ensure_nats || rc=1
      if [ "${rc}" = 0 ]; then
        STEPS="cells" TRIALS="${NATS_LAT_TRIALS}" LAT_PAIRS="${LAT_PAIRS}" LAT_PAYLOADS="${LAT_PAYLOADS}" \
          FELIX_LAT_REF="${BUILD_SHA}" FELIX_LAT_ENV="FELIX_QUIC_LISTENERS=4$([ "${CAMP_INGRESS_WAIT}" = 1 ] && echo ' FELIX_PUB_INGRESS_WAIT=1')" \
          FELIX_LAT_CLIENT_ENV="FELIX_MTU_UPPER_BOUND=3872" \
          "${here}/nats/nats-latency.sh" || rc=1
      fi ;;
  esac
  [ -z "${wpid}" ] || { kill "${wpid}" 2>/dev/null; wait "${wpid}" 2>/dev/null; }
  # Sub-scripts leave NATS up and Felix off; the next Felix cell starts from scratch.
  CURRENT_REF=""; CURRENT_OVERRIDES=""; NATS_ACTIVE=1
  [ "${rc}" = 0 ] && touch "${OUT}/final/${name}.done"
  return "${rc}"
}

# ----------------------------------------------------------------- the run
setup_session
refresh_client_token || distribute_token || die "client token"
ensure_builds
[ "${CURRENT_LG}" = new ] || use_lg new || die "install the new loadgen"
if ! configure "${BUILD_SHA}" ${BASE_B}; then die "configure ${BUILD_SHA:0:8} (is it installed and the label the SHA?)"; fi
agent_on "${BROKER_VM}" "systemctl stop nats 2>/dev/null || true" >/dev/null 2>&1 || true
record_session final-campaign
collect_facts
plog "setup done; builds and facts recorded in ${OUT}"

cur=""; consec=0; skipstep=0; failed=""
close_step() { [ -z "${cur}" ] || [ -z "${failed}" ] || plog "FAIL step ${cur}: incomplete${failed}"; }
for ((i = 0; i < M; i++)); do
  name="$(spec_name "${PSPEC[i]}")"; step="${PSTEP[i]}"
  if [ "${step}" != "${cur}" ]; then
    close_step
    [ "${cur}" = s0 ] && gate_check
    cur="${step}"; consec=0; skipstep=0; failed=""
    n=0; for ((j = 0; j < M; j++)); do [ "${PSTEP[j]}" = "${step}" ] && n=$((n + 1)); done
    plog "[$((i + 1))/${M}]  STEP ${step} start (${n} plan lines)  ETA $(eta "${REM[i]}")"
  fi
  [ "${skipstep}" = 1 ] && continue
  IFS='|' read -r _ _ kind _ <<<"${PSPEC[i]}"
  if [ "${kind}" = block ]; then
    if is_done "${name}"; then continue; fi
    plog "[$((i + 1))/${M}]  block ${name} start  ETA $(eta "${REM[i]}")"
    if run_block "${name}" "${REM[i + 1]}"; then
      consec=0
      plog "[$((i + 1))/${M}]  block ${name} done  ETA $(eta "${REM[i + 1]}")"
    else
      failed="${failed} ${name}"
      plog "[$((i + 1))/${M}]  FAIL block ${name}  ETA $(eta "${REM[i + 1]}")"
    fi
    continue
  fi
  run_entry "${PSPEC[i]}"; rc=$?
  [ "${rc}" = 2 ] && continue
  if is_done "${name}"; then
    consec=0
    plog "[$((i + 1))/${M}]  ${name}  $(headline "${OUT}/cells/${name}")  ETA $(eta "${REM[i + 1]}")"
    mark_logged "${name}"
  else
    consec=$((consec + 1)); failed="${failed} ${name}"
    plog "[$((i + 1))/${M}]  FAIL ${name}  incomplete (see ${OUT}/cells/${name})  ETA $(eta "${REM[i + 1]}")"
    if [ "${consec}" -ge 3 ]; then
      plog "FAIL ${step}: 3 cells in a row failed; skipping the rest of the step"
      skipstep=1
    fi
  fi
done
close_step
[ "${cur}" = s0 ] && gate_check

# One more try at plan lines that did not finish.
retried=0
for ((i = 0; i < M && retried < 12; i++)); do
  name="$(spec_name "${PSPEC[i]}")"
  IFS='|' read -r _ _ kind _ <<<"${PSPEC[i]}"
  [ "${kind}" = block ] && continue
  is_done "${name}" && continue
  retried=$((retried + 1))
  plog "retry ${name}"
  run_entry "${PSPEC[i]}"
  is_done "${name}" && plog "  ${name}  $(headline "${OUT}/cells/${name}")"
done

# NATS leaves the broker running NATS; hand it back to Felix at the build so the VM ends as it began.
if [ "${NATS_ACTIVE}" = 1 ]; then stop_nats_for_felix || true; fi
python3 "${here}/summarize.py" "${OUT}" || true
plog "DONE"
