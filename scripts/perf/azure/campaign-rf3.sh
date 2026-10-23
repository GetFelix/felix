#!/usr/bin/env bash
# RF=3 cluster campaign for v0.6.0: three L4as_v4 brokers on local NVMe, one per
# zone, four D4as_v5 generators, a D2as_v5 control plane, MTU 3900, one pinned
# main build on every member.
#
#   SESSION=<name> BUILD_SHA=<40-char main sha> ./campaign-rf3.sh --print-provision
#   SESSION=<name> BUILD_SHA=<sha> ./campaign-rf3.sh --dry-run
#   SESSION=<name> BUILD_SHA=<sha> nohup ./campaign-rf3.sh &      # re-running resumes
#
# --dry-run prints every cell, the estimated runtime and the cost, and touches
# no VM. A live run starts and repairs the (deallocated) session, runs the
# preflight and the disk gate, and deallocates the VMs when it ends, however it
# ends.
#
# Steps, in order:
#   m     Quorum ingest matrix: durable (perf-durable-quorum) and in-memory
#         (perf-quorum) replicated streams x 256 B / 4 KiB x batch 1 / 64, acked
#         with 64 in flight, keys spread over every shard. The two streams
#         alternate within a trial, so a drifting host hits both.
#   lat   one in flight, batch 1: Quorum ack and publish-to-delivery
#         p50/p99/p999, in-memory and durable, 256 B and 4 KiB, product delivery
#         batching.
#   latp  the same with delivery batching off (FELIX_EVENT_BATCH_MAX_DELAY_US=0,
#         FELIX_EVENT_BATCH_MAX_EVENTS=1), the latency profile.
#   fwd   forwarding cost, 256 B one in flight: publish straight to the shard's
#         owner (cluster client) against publish through one entry broker that
#         relays to the owner (--via-entry).
#   cache Quorum cache put and get latency and throughput at 1, 8 and 32 workers.
#   lf    finalize generation_start, majority_ack and lease_free_reads (one way),
#         then the headline Quorum, cache and latency cells again, named rf3-lf-*.
#         Skipped, with a log line, when the build has none of the three features.
#   fail  kill -9 one broker mid-run under acked durable load, hold it down,
#         restart it. Time to recovery, throughput dip and acked-record loss.
#         Last, because it leaves the shard leaders unbalanced and runs on the
#         finalized (lease-free) fleet when lf ran.
#
# Conditions on every cell (broker): FELIX_ACK_ON_COMMIT=1,
# FELIX_PUB_INGRESS_WAIT=1, FELIX_DURABLE_FSYNC_MODE=on_commit, FELIX_QUIC_LISTENERS=2
# (what an unset value derives on 4 cores), and from lib.sh FELIX_STORAGE_IO_URING=1,
# FELIX_IO_RUNTIME_THREADS=0, 24 MiB UDP buffers, FELIX_MTU_UPPER_BOUND=3872.
# Client: the same UDP buffers and MTU bound. See README "RF=3 campaign".
#
# Durable cells run on freshly recreated streams (a whole-cluster wipe under live
# replicas is its own experiment, and RF=3 on 960 GB would fill the disks in a
# dozen cells otherwise). Every cell waits until GET /v1/placement/replication
# reports no under-replicated shard, and the stream's leaders are spread evenly.
#
# The plan is trimmed to fit MAX_HOURS (8): latency-profile cells, second lease-free
# trial, second forwarding trial, second trial of the non-headline matrix shapes,
# lease-free latency cells, in that order. The level is saved and reused on resume.
set -uo pipefail

self="$(cd "$(dirname "$0")" && pwd)"
if [ -f "${self}/lib.sh" ]; then here="${self}"; else here="$(cd "${self}/../.." && pwd)"; fi
logs_dir="${here}/sessions/logs"
mkdir -p "${logs_dir}"
PLOG="${logs_dir}/rf3-campaign.log"     # progress: one line per step and finished cell
RUNOUT="${logs_dir}/rf3-campaign.out"   # full harness output
LOCK="${logs_dir}/rf3-campaign.pid"

dry=0
printprov=0
FORCE="${FORCE:-0}"
TRIM_ARG=""
for a in "$@"; do
  case "${a}" in
    --dry-run) dry=1 ;;
    --print-provision) printprov=1 ;;
    --force) FORCE=1 ;;
    --trim=*) TRIM_ARG="${a#--trim=}" ;;
    *) echo "usage: $0 [--dry-run] [--print-provision] [--force] [--trim=0..5]" >&2; exit 2 ;;
  esac
done

# A sleeping laptop stops the run; re-exec under caffeinate on macOS.
if [ "${dry}" = 0 ] && [ "${printprov}" = 0 ] && [ -z "${CAMP_CAFFEINATED:-}" ] && command -v caffeinate >/dev/null 2>&1; then
  CAMP_CAFFEINATED=1 exec caffeinate -dimsu "$0" "$@"
fi

: "${SESSION:=<session>}"
: "${BUILD_SHA:=<BUILD_SHA>}"
: "${LOCATION:=centralus}"
: "${SHARDS:=12}"            # a multiple of 3, so leaders spread evenly over three brokers
: "${PER:=16}"               # publishers per generator
: "${KEYS:=48}"
: "${NGEN:=4}"
: "${CELL_SECS:=90}"
: "${INFLIGHT:=64}"
: "${MAT_TRIALS:=2}"
: "${LAT_TRIALS:=2}"
: "${FWD_TRIALS:=2}"
: "${CACHE_TRIALS:=2}"
: "${LF_TRIALS:=2}"
: "${FAIL_TRIALS:=2}"
: "${LAT_WARMUP:=2000}"
: "${LAT_TOTAL:=20000}"
: "${FAIL_SECS:=150}"        # failover cell length
: "${FAIL_KILL_AFTER:=40}"   # seconds after publishing starts
: "${FAIL_DOWN_SECS:=45}"    # how long the killed broker stays down
: "${FAIL_VICTIM:=1}"        # broker index; not 0, which the loadgen dials first
: "${LF_WAIT_SECS:=45}"      # brokers pick up finalized features on their next control-plane contact
: "${REPL_WAIT_SECS:=240}"
: "${DISK_MAX_PCT:=60}"
# fio gate: 70% of Microsoft's published sequential write rate for the L4as_v4
# local NVMe (2 disks, 750 MBps, learn.microsoft.com Lasv4 series, "Temp Disk
# Sequential Write Throughput"). The harness's job is 256k, one job, fdatasync
# after each write; MBps there is 10^6 bytes/s, as fio prints.
: "${FIO_SPEC_MBS:=750}"
: "${FIO_GATE_PCT:=70}"
FIO_MIN=$(( FIO_SPEC_MBS * FIO_GATE_PCT / 100 ))
# Wall-clock minutes per step, in tenths (run-command overhead dominates).
: "${MIN_ING_X10:=55}"
: "${MIN_LAT_X10:=40}"
: "${MIN_CACHE_X10:=30}"
: "${MIN_FAIL_X10:=80}"
: "${MIN_FIN_X10:=50}"
: "${RESTART_X10:=25}"       # all three brokers: stop, activate, start, ready, settle, replicated
: "${RESET_X10:=20}"         # recreate a durable stream and wait for full replication
: "${SETUP_MIN:=75}"         # start, repair, reseed, fio, preflight
: "${MAX_HOURS:=8}"
# Hourly prices, centralus, Azure retail list (prices.azure.com, 2026-10-22):
# L4as_v4 0.388, D4as_v5 0.194, D2as_v5 0.0972. The operator's rates are lower
# (0.33 / 0.172 / 0.086); set PRICE_* to use them.
: "${PRICE_BROKER:=0.388}"
: "${PRICE_GEN:=0.194}"
: "${PRICE_CP:=0.0972}"
NBROKERS=3

DUR_PROFILE="FELIX_ACK_ON_COMMIT=1 FELIX_PUB_INGRESS_WAIT=1"
BASE_B="FELIX_QUIC_LISTENERS=2 FELIX_DURABLE_FSYNC_MODE=on_commit ${DUR_PROFILE}"
LATP_B="FELIX_EVENT_BATCH_MAX_DELAY_US=0 FELIX_EVENT_BATCH_MAX_EVENTS=1"
CLIENT_B="FELIX_UDP_RECV_BUFFER=25165824 FELIX_UDP_SEND_BUFFER=25165824 FELIX_MTU_UPPER_BOUND=3872"
OUT="${OUT:-${here}/sessions/${SESSION}-rf3-results}"

if [ "${printprov}" = 1 ]; then
  cat <<PROV
# Provision (az calls; nothing here runs them for you). IDP_* come from session.env.
# IDP_JWKS_URL must be exported, or session.sh's seed step fails; the campaign's
# setup reseeds with its own derivation either way.
set -a; source ${here}/session.env; set +a
export IDP_SCOPE="\${IDP_SCOPE:-\${IDP_AUDIENCE:-}}"
export IDP_JWKS_URL="\${IDP_JWKS_URL:-https://login.microsoftonline.com/\${IDP_TENANT_ID}/discovery/v2.0/keys}"
SESSION=${SESSION} LOCATION=${LOCATION} TIER=t2 \\
  BROKER_COUNT=3 BROKER_VM_SIZE=Standard_L4as_v4 USE_LOCAL_NVME=true \\
  LOADGEN_COUNT=4 LOADGEN_VM_SIZE=Standard_D4as_v5 CONTROLPLANE_VM_SIZE=Standard_D2as_v5 \\
  REPLICATION_FACTOR=3 SHARDS=${SHARDS} BROKER_LISTENERS=2 \\
  BROKER_REFS=${BUILD_SHA} LOADGEN_REF=${BUILD_SHA} TEARDOWN_AFTER_H=12 \\
  ${here}/session.sh
# BROKER_REFS builds the SHA on generator 0 and installs it on the three brokers and
# the control plane; LOADGEN_REF builds the instrument from it on every generator.
# The campaign checks every member reports ${BUILD_SHA:0:12} before it measures.
# Quota: Lasv4 12 of 20 vCPUs, Dasv5 18 of 20, regional 30 of 40.
PROV
  exit 0
fi

# ------------------------------------------------------------------ the plan
# Entry: name|step|kind|stream|payload|batch|inflight|benv|extra
#   kind ingest|lat|fwd|cache|fin|fail   extra: ingest/lat -, fwd owner|entry, cache workers
PSTEP=(); PSPEC=()
add() {
  PSTEP+=("$2")
  PSPEC+=("$1|$2|$3|$4|$5|$6|$7|$8|$9")
}
stream_of() { [ "$1" = dur ] && echo perf-durable-quorum || echo perf-quorum; }

TRIM=0
OVER=0
trim_has() { [ "${TRIM}" -ge "$1" ]; }

build_plan() {
  local t m p b order pre tr c w
  PSTEP=(); PSPEC=()
  # m: the Quorum matrix.
  for t in $(seq 1 "${MAT_TRIALS}"); do
    order="mem dur"; [ $((t % 2)) = 0 ] && order="dur mem"
    for p in 256 4096; do for b in 1 64; do
      # The headline shapes keep every trial; the others lose trial 2 at trim 4.
      if trim_has 4 && [ "${t}" -gt 1 ] && ! { [ "${p}" = 256 ] && [ "${b}" = 1 ]; } && ! { [ "${p}" = 4096 ] && [ "${b}" = 64 ]; }; then continue; fi
      for m in ${order}; do
        add "rf3-q-${m}-p${p}-b${b}-ack-t${t}" m ingest "$(stream_of "${m}")" "${p}" "${b}" "${INFLIGHT}" "${BASE_B}" -
      done
    done; done
  done
  # lat: one in flight.
  for t in $(seq 1 "${LAT_TRIALS}"); do for p in 256 4096; do for m in mem dur; do
    add "rf3-lat-${m}-p${p}-t${t}" lat lat "$(stream_of "${m}")" "${p}" 1 1 "${BASE_B}" -
  done; done; done
  if ! trim_has 1; then
    for t in $(seq 1 "${LAT_TRIALS}"); do for p in 256 4096; do for m in mem dur; do
      add "rf3-latp-${m}-p${p}-t${t}" latp lat "$(stream_of "${m}")" "${p}" 1 1 "${BASE_B} ${LATP_B}" -
    done; done; done
  fi
  # fwd: owner-direct against through one entry broker.
  for t in $(seq 1 "${FWD_TRIALS}"); do
    trim_has 3 && [ "${t}" -gt 1 ] && continue
    order="owner entry"; [ $((t % 2)) = 0 ] && order="entry owner"
    for m in mem dur; do for w in ${order}; do
      add "rf3-fwd-${w}-${m}-p256-t${t}" fwd fwd "$(stream_of "${m}")" 256 1 1 "${BASE_B}" "${w}"
    done; done
  done
  # cache: Quorum put and get.
  for t in $(seq 1 "${CACHE_TRIALS}"); do for c in 1 8 32; do
    add "rf3-qcache-c${c}-t${t}" cache cache perf-quorum 256 1 0 "${BASE_B}" "${c}"
  done; done
  # lf: after the one-way finalize.
  add "finalize-lease-free" lf fin - - - - "${BASE_B}" -
  pre=rf3-lf
  for t in $(seq 1 "${LF_TRIALS}"); do
    trim_has 2 && [ "${t}" -gt 1 ] && continue
    order="mem dur"; [ $((t % 2)) = 0 ] && order="dur mem"
    for tr in "256 1" "4096 64"; do
      # shellcheck disable=SC2086
      set -- ${tr}
      for m in ${order}; do
        add "${pre}-q-${m}-p$1-b$2-ack-t${t}" lf ingest "$(stream_of "${m}")" "$1" "$2" "${INFLIGHT}" "${BASE_B}" -
      done
    done
    for c in 1 8; do add "${pre}-qcache-c${c}-t${t}" lf cache perf-quorum 256 1 0 "${BASE_B}" "${c}"; done
    trim_has 5 && continue
    for p in 256 4096; do for m in mem dur; do
      add "${pre}-lat-${m}-p${p}-t${t}" lf lat "$(stream_of "${m}")" "${p}" 1 1 "${BASE_B}" -
    done; done
  done
  # fail: last.
  for t in $(seq 1 "${FAIL_TRIALS}"); do
    add "rf3-fail-t${t}" fail fail perf-failover 4096 64 "${INFLIGHT}" "${BASE_B}" -
  done
}

# ------------------------------------------------------------- bookkeeping
is_done() { [ -e "${OUT}/cells/${1}/done" ] || [ -e "${OUT}/rf3/${1}.done" ]; }
spec_name() { printf '%s' "${1%%|*}"; }
is_durable_stream() { case "$1" in *durable*|perf-failover) return 0 ;; esac; return 1; }

spec_x10() {
  local name kind
  IFS='|' read -r name _ kind _ <<<"$1"
  if is_done "${name}" && [ "${IGNORE_DONE:-0}" != 1 ]; then echo 0; return; fi
  case "${kind}" in
    lat|fwd) echo "${MIN_LAT_X10}" ;;
    cache) echo "${MIN_CACHE_X10}" ;;
    fail) echo "${MIN_FAIL_X10}" ;;
    fin) echo "${MIN_FIN_X10}" ;;
    *) echo "${MIN_ING_X10}" ;;
  esac
}
compute_rem() {   # COST[i] = tenths of minutes for entry i; REM[i] = pending tenths of entries i..end
  local i acc=0 c name kind stream benv prevkey=""
  M=${#PSPEC[@]}
  REM=(); COST=()
  for ((i = 0; i < M; i++)); do
    c="$(spec_x10 "${PSPEC[i]}")"
    IFS='|' read -r name _ kind stream _ _ _ benv _ <<<"${PSPEC[i]}"
    if [ "${c}" -gt 0 ] && [ "${kind}" != fin ]; then
      [ "${benv}" != "${prevkey}" ] && c=$((c + RESTART_X10))
      is_durable_stream "${stream}" && c=$((c + RESET_X10))
      prevkey="${benv}"
    fi
    COST[i]="${c}"
  done
  for ((i = M; i >= 0; i--)); do
    [ "${i}" -lt "${M}" ] && acc=$((acc + COST[i]))
    REM[i]=${acc}
  done
}
hours_of() { awk -v x="$1" 'BEGIN { printf "%.1f", x / 600 }'; }
total_x10() { echo $(( REM[0] + SETUP_MIN * 10 + 120 )); }   # plus two token refreshes and slack

choose_trim() {
  local saved="${OUT}/rf3/trim" lvl
  if [ -n "${TRIM_ARG}" ]; then TRIM="${TRIM_ARG}"; build_plan; compute_rem; return; fi
  if [ -s "${saved}" ] && [ "${dry}" = 0 ]; then TRIM="$(cat "${saved}")"; build_plan; compute_rem; return; fi
  for lvl in 0 1 2 3 4 5; do
    TRIM="${lvl}"; build_plan
    IGNORE_DONE=1 compute_rem
    [ "$(total_x10)" -le $((MAX_HOURS * 600)) ] && break
  done
  OVER=0
  [ "$(total_x10)" -le $((MAX_HOURS * 600)) ] || OVER=1
  [ "${dry}" = 0 ] && { mkdir -p "${OUT}/rf3"; echo "${TRIM}" > "${saved}"; }
  IGNORE_DONE=0 compute_rem
}
trim_text() {
  case "$1" in
    0) echo "nothing" ;;
    1) echo "latency-profile cells" ;;
    2) echo "latency-profile cells, second lease-free trial" ;;
    3) echo "latency-profile cells, second lease-free trial, second forwarding trial" ;;
    4) echo "latency-profile cells, second lease-free trial, second forwarding trial, second trial of the non-headline matrix shapes" ;;
    5) echo "latency-profile cells, second lease-free trial, second forwarding trial, second trial of the non-headline matrix shapes, lease-free latency cells" ;;
  esac
}

# kv_nums: summarize.py --kv on stdin -> "append ingress drops cores", empty without a steady window.
kv_nums() {
  awk -F= '{ v[$1] = $2 } END { if (v["ss_append_mb_s"] != "") printf "%.1f %.1f %.0f %.2f\n", v["ss_append_mb_s"], v["ss_ingress_mb_s"], v["ss_drops_s"], v["ss_cores"] }'
}
self_test() {
  local got
  got="$(printf 'ss_append_mb_s=12.34\nss_ingress_mb_s=99.96\nss_drops_s=3.2\nss_cores=7.5\n' | kv_nums)"
  [ "${got}" = "12.3 100.0 3 7.50" ] || { echo "!! self-test: kv_nums gave '${got}'" >&2; return 1; }
  [ -z "$(printf '' | kv_nums)" ] || { echo "!! self-test: kv_nums on empty input" >&2; return 1; }
  grep -q -- '"--kv"' "${here}/summarize.py" || { echo "!! self-test: summarize.py has no --kv" >&2; return 1; }
  echo "self-test: kv parsing ok"
}

# ----------------------------------------------------------------- dry run
if [ "${dry}" = 1 ]; then
  self_test || exit 4
  choose_trim
  echo "session ${SESSION} (${LOCATION})  results ${OUT}"
  echo "BUILD_SHA ${BUILD_SHA}  (brokers, control plane and loadgen all built from it)"
  echo "topology: ${NBROKERS} x Standard_L4as_v4 (local NVMe RAID0, zones 1/2/3), ${NGEN} x Standard_D4as_v5, 1 x Standard_D2as_v5; RF=3, ${SHARDS} shards"
  echo "broker env: ${BASE_B} (+ lib.sh base: STORAGE_IO_URING=1 IO_RUNTIME_THREADS=0 UDP buffers 24 MiB, MTU_UPPER_BOUND=3872)"
  echo "client env: ${CLIENT_B}"
  echo "cell run: ${CELL_SECS} s, ${PER} publishers/generator x ${NGEN} generators, ${KEYS} keys over ${SHARDS} shards, in flight ${INFLIGHT}; failover cell ${FAIL_SECS} s, kill at +${FAIL_KILL_AFTER} s, down ${FAIL_DOWN_SECS} s"
  echo "fio gate: each broker's seq256k-j1 >= ${FIO_MIN} MB/s (${FIO_GATE_PCT}% of ${FIO_SPEC_MBS} MBps published L4as_v4 sequential write)"
  echo "trim level ${TRIM}: trimmed: $(trim_text "${TRIM}")"
  echo
  for ((i = 0; i < M; i++)); do
    IFS='|' read -r name step kind stream p b f benv extra <<<"${PSPEC[i]}"
    st=""; is_done "${name}" && st=" [done]"
    printf '[%3d/%d] %-34s %-4s %-6s %-22s p=%-5s b=%-2s f=%-3s est=%sm%s\n' "$((i + 1))" "${M}" "${name}" "${step}" "${kind}" "${stream}" "${p}" "${b}" "${f}" "$(awk -v x="${COST[i]}" 'BEGIN { printf "%.1f", x / 10 }')" "${st}"
  done
  echo
  for s in m lat latp fwd cache lf fail; do
    n=0; x=0
    for ((i = 0; i < M; i++)); do
      [ "${PSTEP[i]}" = "${s}" ] || continue
      n=$((n + 1)); x=$((x + COST[i]))
    done
    [ "${n}" = 0 ] || printf 'step %-5s %3d plan lines  ~%s h\n' "${s}" "${n}" "$(hours_of "${x}")"
  done
  tot="$(total_x10)"
  perh="$(awk -v b="${PRICE_BROKER}" -v g="${PRICE_GEN}" -v c="${PRICE_CP}" -v nb="${NBROKERS}" -v ng="${NGEN}" 'BEGIN { printf "%.3f", nb * b + ng * g + c }')"
  printf 'setup %d min (start, repair, reseed, fio gate, preflight); restarts %d min, stream recreations %d min each\n' "${SETUP_MIN}" "$((RESTART_X10 / 10))" "$((RESET_X10 / 10))"
  [ "${OVER}" = 1 ] && echo "!! WARNING: even the deepest trim exceeds ${MAX_HOURS} h; a live run refuses without --force"
  printf 'cells %d, total about %s h, estimated cost $%s at $%s/h (%d x $%s + %d x $%s + $%s; budget %s h)\n' "${M}" "$(hours_of "${tot}")" \
    "$(awk -v x="${tot}" -v p="${perh}" 'BEGIN { printf "%.0f", x / 600 * p }')" "${perh}" "${NBROKERS}" "${PRICE_BROKER}" "${NGEN}" "${PRICE_GEN}" "${PRICE_CP}" "${MAX_HOURS}"
  exit 0
fi

# ---------------------------------------------------------------- live run
if [ -s "${LOCK}" ] && kill -0 "$(cat "${LOCK}")" 2>/dev/null; then
  echo "!! rf3-campaign already running (pid $(cat "${LOCK}"))" >&2; exit 1
fi
[[ "${BUILD_SHA}" =~ ^[0-9a-f]{40}$ ]] || { echo "!! BUILD_SHA must be a 40-char commit sha" >&2; exit 2; }
[ -f "${here}/sessions/${SESSION}.env" ] || { echo "!! no sessions/${SESSION}.env" >&2; exit 2; }
echo $$ > "${LOCK}"
VMS_STARTED=0
dealloc() {
  [ "${VMS_STARTED}" = 1 ] && [ "${NO_DEALLOC:-0}" != 1 ] || return 0
  local try left
  for try in 1 2 3; do
    # shellcheck disable=SC2046
    az vm deallocate --ids $(az vm list -g "${GROUP}" --query '[].id' -o tsv) -o none
    left="$(az vm list -g "${GROUP}" -d --query "[?powerState!='VM deallocated'].name" -o tsv 2>/dev/null)"
    if [ -z "${left}" ]; then plog "${GROUP} deallocated (verified)"; return 0; fi
    plog "WARN deallocate attempt ${try}: still running: $(echo "${left}" | tr '\n' ' ')"
    sleep $((try * 30))
  done
  plog "FAIL deallocate: ${GROUP} STILL RUNNING ($(echo "${left}" | tr '\n' ' ')); stop it by hand"
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
mkdir -p "${OUT}/system" "${OUT}/rf3" "${here}/sessions/${SESSION}-results/system"

eta() {   # eta <tenths of minutes from now> -> HH:MM UTC
  local at=$(( $(date +%s) + $1 * 6 ))
  date -u -r "${at}" +%H:%M 2>/dev/null || date -u -d "@${at}" +%H:%M
}
plog() { printf '%s UTC  %s\n' "$(date -u +%H:%M)" "$*" >> "${PLOG}"; printf '%s PROGRESS %s\n' "$(date -u +%H:%M:%S)" "$*"; }
die() { plog "FAIL $*"; plog DONE; exit 1; }

# shellcheck source=lib.sh
source "${here}/lib.sh"
read -r -a GENS <<<"${LOADGENS}"

choose_trim
if [ "${OVER}" = 1 ] && [ "${FORCE}" != 1 ]; then
  echo "!! the deepest trim still exceeds ${MAX_HOURS} h ($(hours_of "$(total_x10)") h); refusing without --force" >&2
  plog "FAIL plan exceeds ${MAX_HOURS} h ($(hours_of "$(total_x10)") h); not started"; rm -f "${LOCK}"; exit 3
fi
plog "[0/${M}]  START rf3 campaign on ${BUILD_SHA:0:8} (${SESSION}), trim ${TRIM} ($(trim_text "${TRIM}"))  ETA $(eta "$(total_x10)")"

# --------------------------------------------------------- cell machinery
source "${here}/cells.sh"
[ "${#BROKER_VMS[@]}" = "${NBROKERS}" ] || die "inventory has ${#BROKER_VMS[@]} brokers, want ${NBROKERS}"
[ "${#GENS[@]}" -ge "${NGEN}" ] || die "inventory has ${#GENS[@]} generators, want ${NGEN}"
[ "${REPLICATION_FACTOR:-1}" = 3 ] || die "inventory REPLICATION_FACTOR=${REPLICATION_FACTOR:-1}, want 3"
# Without a restart policy of its own, WIPE_DURABLE=1 would wipe under live replicas.
export SESSION CELL_SECS RESUME=1 WIPE_DURABLE=0 PROFILE=0 SETTLE_SECS=15
export EXTRA_LOADGEN_ENV="${CLIENT_B}"
export EXTRA_BROKER_ENV="FELIX_MTU_UPPER_BOUND=3872"   # applied last by merge_env

# ------------------------------------------------------------------- setup
do_mtu() {
  local mtu vm
  mtu='dev=$(ip route show default | awk "{print \$5; exit}"); ip link set dev $dev mtu 3900; for vf in /sys/class/net/$dev/lower_*; do [ -e "$vf" ] && ip link set dev "$(basename $vf | sed s/^lower_//)" mtu 3900; done; systemctl disable --now unattended-upgrades >/dev/null 2>&1; systemctl mask unattended-upgrades >/dev/null 2>&1; echo "$(hostname) mtu=$(cat /sys/class/net/$dev/mtu) uu=$(systemctl is-enabled unattended-upgrades 2>&1)"'
  for vm in $(az vm list -g "${GROUP}" --query '[].name' -o tsv); do
    (run_on_str "${vm}" "${mtu}" 2>&1 | grep 'mtu=') &
  done
  wait
}

# do_preflight: one line per check, PREFLIGHT OK or PREFLIGHT FAIL. The fio gate
# is part of it: a broker below FIO_MIN stops the campaign before any cell runs.
do_preflight() {
  local fail=0 vm out r mbs f nic sizes want
  nic='dev=$(ip route show default | awk "{print \$5; exit}"); echo "mtu=$(cat /sys/class/net/$dev/mtu) uu=$(systemctl is-enabled unattended-upgrades 2>&1)"'
  sizes="$(az vm list -g "${GROUP}" --query '[].[name,hardwareProfile.vmSize]' -o tsv 2>/dev/null)"
  for vm in $(az vm list -g "${GROUP}" --query '[].name' -o tsv); do
    out=""
    # DF path check at the largest payload the MTU bound allows (3872 + 28 = 3900):
    # generators to every broker, brokers to every other broker (replication).
    case "${vm}" in
      *loadgen*) out="$(run_on_str "${vm}" "${nic}; for ip in ${BROKER_IPS//,/ }; do ping -M do -s 3872 -c 3 -W 2 \$ip >/dev/null 2>&1 && echo path=ok || echo path=bad; done" 2>&1 | grep 'mtu=\|path=' | tr '\n' ' ')" ;;
      *broker*) out="$(run_on_str "${vm}" "${nic}; for ip in ${BROKER_IPS//,/ }; do case \"\$(hostname -I)\" in *\$ip*) continue;; esac; ping -M do -s 3872 -c 3 -W 2 \$ip >/dev/null 2>&1 && echo path=ok || echo path=bad; done" 2>&1 | grep 'mtu=\|path=' | tr '\n' ' ')" ;;
      *) out="$(run_on_str "${vm}" "${nic}" 2>&1 | grep 'mtu=' | tr '\n' ' ')" ;;
    esac
    r=BAD
    case "${out}" in *mtu=3900*uu=masked*) r=OK ;; esac
    case "${out}" in *path=bad*) r=BAD ;; esac
    case "${vm}" in *loadgen*|*broker*) case "${out}" in *path=ok*) ;; *) r=BAD ;; esac ;; esac
    printf '%-28s %s %s\n' "${vm}" "${r}" "${out}"; [ "${r}" = OK ] || fail=1
  done
  # Sizes: three L4as_v4 brokers, D4as_v5 generators, D2as_v5 control plane.
  for vm in $(az vm list -g "${GROUP}" --query '[].name' -o tsv); do
    case "${vm}" in
      *broker*) want=Standard_L4as_v4 ;;
      *loadgen*) want=Standard_D4as_v5 ;;
      *) want=Standard_D2as_v5 ;;
    esac
    case "$(printf '%s\n' "${sizes}" | awk -v v="${vm}" '$1 == v { print $2 }')" in
      "${want}") ;;
      *) printf '%-28s BAD size is not %s\n' "${vm}" "${want}"; fail=1 ;;
    esac
  done
  # One build on every member.
  for vm in "${BROKER_VMS[@]}" "$(cp_vm)"; do
    out="$(agent_on "${vm}" "felix-agent snapshot --env" 2>&1 | grep '^bin\..*\.ref=' | tr '\n' ' ')"
    case "${out}" in *"${BUILD_SHA}"*) printf '%-28s OK build %s\n' "${vm}" "${BUILD_SHA:0:12}" ;; *) printf '%-28s BAD build %s\n' "${vm}" "${out}"; fail=1 ;; esac
  done
  for vm in "${GENS[@]}"; do
    out="$(run_on_str "${vm}" 'cat /etc/felix/loadgen.ref; echo __RUNOK__' 2>&1 | grep -v '__RUNOK__\|^\[std' | tr '\n' ' ')"
    case "${out}" in *"${BUILD_SHA}"*) printf '%-28s OK loadgen %s\n' "${vm}" "${BUILD_SHA:0:12}" ;; *) printf '%-28s BAD loadgen %s\n' "${vm}" "${out}"; fail=1 ;; esac
  done
  for vm in "${BROKER_VMS[@]}"; do
    out="$(run_on_str "${vm}" 'findmnt -n -o SOURCE /data; systemctl is-active felix-broker; grep -c NODE_REFRESH_TOKEN_FILE /etc/felix/broker.env; test -s /etc/felix/node.refresh_token && echo refresh=yes' 2>&1 | grep -v '^\[std\|Enable succ\|__RUNOK' | tr '\n' ' ')"
    case "${out}" in
      */dev/md0*active*1*refresh=yes*) printf '%-28s OK storage-auth %s\n' "${vm}" "${out}" ;;
      *) printf '%-28s BAD storage-auth %s\n' "${vm}" "${out}"; fail=1 ;;
    esac
    f="${OUT}/system/${vm}.fio.txt"
    mbs="$(grep -h 'seq256k-j1.mb_s=' "${f}" 2>/dev/null | cut -d= -f2 | cut -d. -f1)"
    if [ -n "${mbs}" ] && [ "${mbs}" -ge "${FIO_MIN}" ]; then printf '%-28s OK fio %s MB/s (gate %s)\n' "${vm}" "${mbs}" "${FIO_MIN}"
    else printf '%-28s BAD fio %s MB/s (gate %s)\n' "${vm}" "${mbs:-none}" "${FIO_MIN}"; fail=1; fi
  done
  [ "${fail}" = 0 ] && echo "PREFLIGHT OK" || echo "PREFLIGHT FAIL"
}

# ---------------------------------------------------- control plane, on gen 0
# cp_remote <body>: run POSIX commands on generator 0 with $u (control plane URL)
# and $t (a fresh admin token) set.
cp_remote() {
  agent_on "${GENS[0]}" "u='http://${CONTROLPLANE_IP}:8080'
felix-agent token-refresh \"\$u\" /home/felix/felix-session/admin.token /home/felix/felix-session/admin.refresh 1800 >/dev/null 2>&1 || true
t=\$(cat /home/felix/felix-session/admin.token)
$1"
}

read -r -d '' REPL_PY <<'PY' || true
import collections, json, sys
d = json.load(sys.stdin)
items = d.get("items", [])
by = collections.defaultdict(collections.Counter)
for it in items:
    name = it.get("stream") or "?"
    if str(it.get("kind", "")).lower() == "cache":
        name = "cache/" + name
    by[name][(it.get("leader") or "none")[-1:]] += 1
parts = ";".join("%s:%s" % (s, ",".join("%s=%d" % (n, c) for n, c in sorted(by[s].items()))) for s in sorted(by))
print("__REPL_BEGIN__under=%d shards=%d %s__REPL_END__" % (d.get("under_replicated", 0), len(items), parts))
PY

# repl_status: "under=<n> shards=<n> <stream>:<broker>=<leaders>,..;..", empty on failure.
repl_status() {
  cp_remote "cat > /tmp/repl.py <<'PYEOF'
${REPL_PY}
PYEOF
curl -fsS -H \"Authorization: Bearer \$t\" \"\$u/v1/placement/replication\" | python3 /tmp/repl.py
echo __RUNOK__" 2>/dev/null | extract_between REPL
}

# leaders_even <status> <stream>: every broker leads, and no two differ by more than one.
leaders_even() {
  printf '%s\n' "$1" | tr ' ;' '\n\n' | grep "^$2:" | head -1 | cut -d: -f2 | tr ',' '\n' \
    | awk -F= -v n="${NBROKERS}" '{ c[NR] = $2 } END { if (NR != n) exit 1; lo = hi = c[1]; for (i = 2; i <= NR; i++) { if (c[i] < lo) lo = c[i]; if (c[i] > hi) hi = c[i] } exit (hi - lo > 1) }'
}

# ensure_ready: brokers ready and every shard fully replicated (#1155: a cluster
# started together settles before it places; recreated streams and restarts need
# the copies caught up before a cell is allowed to measure).
ensure_ready() {
  local deadline st
  wait_ready || { plog "WARN brokers not ready"; return 1; }
  deadline=$(( $(date +%s) + REPL_WAIT_SECS ))
  while :; do
    st="$(repl_status)"
    case "${st}" in
      under=0\ shards=[1-9]*) echo "${st}" > "${OUT}/rf3/last-replication.txt"; return 0 ;;
    esac
    [ "$(date +%s)" -ge "${deadline}" ] && break
    sleep 10
  done
  plog "WARN not fully replicated after ${REPL_WAIT_SECS} s: ${st:-no answer}"
  return 1
}

# reset_streams <stream>...: delete and recreate on the control plane, the way
# seed-remote.sh creates them. Recreating frees the disk and re-places the leaders.
reset_streams() {
  local s list="$*"
  cp_remote "STREAMS='${list}'; SHARDS=${SHARDS}; RF=3
$(cat <<'BODY'
for s in $STREAMS; do
  case "$s" in *durable*|perf-failover) d=true ;; *) d=false ;; esac
  curl -s -X DELETE -H "Authorization: Bearer $t" "$u/v1/tenants/perf/namespaces/default/streams/$s" >/dev/null 2>&1 || true
  sleep 3
  code=$(curl -s -o /tmp/reset.resp -w '%{http_code}' -X POST "$u/v1/tenants/perf/namespaces/default/streams" \
    -H "Authorization: Bearer $t" -H 'Content-Type: application/json' \
    -d "{\"stream\":\"$s\",\"kind\":\"Stream\",\"shards\":$SHARDS,\"replication_factor\":$RF,\"retention\":{\"max_age_seconds\":null,\"max_size_bytes\":null},\"consistency\":\"Quorum\",\"delivery\":\"AtLeastOnce\",\"durable\":$d}")
  case "$code" in 2*) echo "created.$s=$code" ;; *) echo "!! create $s -> $code"; cat /tmp/reset.resp; exit 1 ;; esac
done
BODY
)
echo __RUNOK__" > "${OUT}/system/reset-streams.txt" 2>&1 || { plog "WARN stream recreate failed: $(tail -3 "${OUT}/system/reset-streams.txt" | tr '\n' ' ')"; return 1; }
}

# ensure_fresh <stream>: a recreated stream, fully replicated, with evenly spread leaders.
ensure_fresh() {
  local s="$1" attempt st
  for attempt in 1 2 3; do
    reset_streams "${s}" || continue
    sleep 12
    ensure_ready || continue
    st="$(cat "${OUT}/rf3/last-replication.txt" 2>/dev/null)"
    if leaders_even "${st}" "${s}"; then return 0; fi
    plog "WARN ${s} leaders uneven (${st}); recreating (attempt ${attempt})"
  done
  plog "WARN ${s}: gave up getting even leaders; measuring on $(cat "${OUT}/rf3/last-replication.txt" 2>/dev/null)"
  [ -s "${OUT}/rf3/last-replication.txt" ]
}

# disk_guard: the highest /data use across the brokers. Over DISK_MAX_PCT the
# recreate did not free the disk: wipe everything and seed again.
disk_pct() {
  par_on "${OUT}/system" disk "df --output=pcent /data | tail -1" "${BROKER_VMS[@]}" >/dev/null 2>&1
  cat "${OUT}"/system/*.disk.txt 2>/dev/null | tr -dc '0-9\n' | grep -E '^[0-9]+$' | sort -n | tail -1
}
heavy_reset() {
  plog "WARN full reset (wipe every broker, reseed)"
  par_on "${OUT}/system" stop "systemctl stop felix-broker" "${BROKER_VMS[@]}" >/dev/null 2>&1
  SESSION="${SESSION}" "${here}/reseed.sh" > "${OUT}/rf3/reseed-heavy.txt" 2>&1 || die "heavy reset: reseed (see ${OUT}/rf3/reseed-heavy.txt)"
  distribute_token || die "heavy reset: client token"
  CURRENT_REF=""; CURRENT_OVERRIDES=""
}

setup_session() {
  local down f vm
  down="$(az vm list -g "${GROUP}" -d --query "[?powerState!='VM running'].name" -o tsv 2>/dev/null)"
  if [ -z "${down}" ] && [ -e "${OUT}/rf3/setup.ok" ]; then
    VMS_STARTED=1
    plog "setup: session already running and prepared; skipping"
    return 0
  fi
  rm -f "${OUT}/rf3/setup.ok"
  VMS_STARTED=1   # from here a failure must not leave VMs running
  plog "setup: starting ${GROUP}"
  # shellcheck disable=SC2046
  az vm start --ids $(az vm list -g "${GROUP}" --query '[].id' -o tsv) -o none || die "start ${GROUP}"
  par_on "${OUT}/system" prep 'systemctl stop felix-broker; /usr/local/sbin/felix-mount-data.sh >/tmp/mount.log 2>&1' "${BROKER_VMS[@]}" >/dev/null 2>&1
  do_mtu > "${OUT}/rf3/mtu.txt" 2>&1
  # Streams exist after the reseed; the settle window (#1155) covers brokers that
  # start within seconds of each other, and ensure_ready below proves it.
  SESSION="${SESSION}" "${here}/reseed.sh" > "${OUT}/rf3/reseed.txt" 2>&1 || die "reseed (see ${OUT}/rf3/reseed.txt)"
  distribute_token || die "client token"
  for vm in "${BROKER_VMS[@]}"; do
    f="${OUT}/system/${vm}.fio.txt"
    [ -f "${f}" ] && mv "${f}" "${f%.txt}.pre-rf3-$(date +%H%M).txt"
  done
  RESUME=0 SESSION="${SESSION}" "${here}/fio-baseline.sh" > "${OUT}/rf3/fio.txt" 2>&1
  do_preflight > "${OUT}/rf3/preflight.txt" 2>&1
  if ! grep -q 'PREFLIGHT OK' "${OUT}/rf3/preflight.txt"; then
    plog "FAIL preflight (see ${OUT}/rf3/preflight.txt): $(grep BAD "${OUT}/rf3/preflight.txt" | head -3 | tr '\n' ';')"; plog DONE; exit 1
  fi
  plog "setup: preflight OK (fio >= ${FIO_MIN} MB/s on all ${NBROKERS} brokers)"
  touch "${OUT}/rf3/setup.ok"
}

collect_facts() {
  local cmds
  {
    echo "session=${SESSION} region=${LOCATION} group=${GROUP}"
    echo "BUILD_SHA=${BUILD_SHA}"
    echo "trim=${TRIM} ($(trim_text "${TRIM}"))"
    echo "driver BASE_B=${BASE_B}"
    echo "driver LATP_B=${LATP_B}"
    echo "driver CLIENT_B=${CLIENT_B}"
    echo "CELL_SECS=${CELL_SECS} PER=${PER} NGEN=${NGEN} KEYS=${KEYS} SHARDS=${SHARDS} INFLIGHT=${INFLIGHT} RF=3"
    echo "FAIL_SECS=${FAIL_SECS} FAIL_KILL_AFTER=${FAIL_KILL_AFTER} FAIL_DOWN_SECS=${FAIL_DOWN_SECS} FAIL_VICTIM=${FAIL_VICTIM}"
    echo "FIO gate ${FIO_MIN} MB/s = ${FIO_GATE_PCT}% of ${FIO_SPEC_MBS} MBps (Lasv4 docs, L4as_v4 temp disk sequential write)"
    echo "lib.sh base_overrides:"; base_overrides | sed 's/^/  /'
    echo "Exact conditions, every Felix cell, against the product default:"
    echo "  FELIX_ACK_ON_COMMIT=1          default: durable streams already ack after commit (#1421); set so in-memory acks match the main campaign"
    echo "  FELIX_PUB_INGRESS_WAIT=1       default off  (DIFFERS)"
    echo "  FELIX_STORAGE_IO_URING=1       default off  (DIFFERS, from lib.sh)"
    echo "  FELIX_QUIC_LISTENERS=2         the derived default on 4 cores"
    echo "  FELIX_DURABLE_FSYNC_MODE=on_commit  default periodic  (DIFFERS)"
    echo "  FELIX_IO_RUNTIME_THREADS=0, FELIX_UDP_RECV/SEND_BUFFER=25165824, FELIX_MTU_UPPER_BOUND=3872 (harness)"
    echo "  latp cells also: ${LATP_B}"
    echo "  Quorum streams and cache are RF=3 over three brokers in zones 1/2/3; generators and control plane in zone 1"
    echo "  ingest publishers dial brokers round-robin through a plain client: a publish entering a non-leader broker is forwarded to the shard owner"
  } > "${OUT}/rf3/config.txt"
  az vm list -g "${GROUP}" -d --query "[].{name:name,size:hardwareProfile.vmSize,location:location,zones:zones,power:powerState,ip:privateIps}" -o table > "${OUT}/rf3/vm-sizes.txt" 2>&1 || true
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
  par_on "${OUT}/system" facts "${cmds}" "${BROKER_VMS[@]}" "${GENS[@]}" || true
}

# ------------------------------------------------------------- reporting
LOGGED="${OUT}/rf3/logged.txt"; touch "${LOGGED}"
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
cachenums() {
  grep -h '^LOADGEN_JSON' "$1"/*.run.txt 2>/dev/null | python3 -I -c '
import json, sys
for l in sys.stdin:
    try:
        j = json.loads(l.split(" ", 1)[1])
    except Exception:
        continue
    out = []
    for op in ("put", "get"):
        o = j.get(op) or {}
        lat = o.get("latency_us") or {}
        out.append("%s %.0f op/s p50/p99/p999 %s/%s/%s us" % (op, o.get("throughput_op_s", 0), lat.get("p50"), lat.get("p99"), lat.get("p999")))
    print("; ".join(out))'
}
# cell_nums <dir>: append ingress drops cores
cell_nums() { python3 -I "${here}/summarize.py" --kv "$1" 2>/dev/null | kv_nums; }

# snapval <file> <key>: an integer counter from a snapshot, 0 when absent.
snapval() {
  local v
  v="$(sed -n "s/^$2=//p" "$1" 2>/dev/null | tail -1 | cut -d. -f1)"
  case "${v}" in ''|*[!0-9]*) echo 0 ;; *) echo "${v}" ;; esac
}

# balance <dir>: each broker's process cores, append rate and forwarded share in
# the cell, written to balance.txt. The spread line is max/min of cores.
balance() {
  local dir="$1" vm a b cores app out="" fw req
  for vm in "${BROKER_VMS[@]}"; do
    b="${dir}/${vm}.before.txt"; a="${dir}/${vm}.after.txt"
    cores="$(sed -n 's/^s\.proc_cores=//p' "${a}" 2>/dev/null | tail -1)"
    app="$(sed -n 's/^s\.append_mb_s=//p' "${a}" 2>/dev/null | tail -1)"
    fw=$(( $(snapval "${a}" m.forwarded) - $(snapval "${b}" m.forwarded) ))
    req=$(( $(snapval "${a}" m.publish_requests) - $(snapval "${b}" m.publish_requests) ))
    out="${out}${vm##*-}:cores=${cores:-n/a},append=${app:-n/a}MB/s,fwd=${fw}/${req} "
  done
  echo "${out}" | tee "${dir}/balance.txt" >/dev/null
  echo "${out}" | awk '{ for (i = 1; i <= NF; i++) { split($i, x, ","); split(x[1], y, "="); v = y[2]; if (v ~ /^[0-9.]+$/) { n++; if (n == 1 || v < lo) lo = v; if (n == 1 || v > hi) hi = v } } if (n > 1 && lo > 0) printf "cpu spread %.2fx (", hi / lo; else printf "cpu spread n/a ("; print $0 ")" }'
}

headline() {
  local dir="$1" a i d c base
  base="$(basename "${dir}")"
  case "${base}" in
    *-fail-*) cat "${dir}/failover.txt" 2>/dev/null | tr '\n' ' '; return ;;
    *qcache*) echo "$(cachenums "${dir}"); $(balance "${dir}")"; return ;;
    *-lat-*|*-latp-*|*-fwd-*) echo "$(ackpcts "${dir}"); $(balance "${dir}")"; return ;;
  esac
  read -r a i d c <<<"$(cell_nums "${dir}")"
  if [ -z "${a}" ]; then echo "no steady window; $(ackpcts "${dir}")"; return; fi
  echo "append ${a} MB/s (cluster, 3 copies), ingress ${i} MB/s, drops ${d}/s, cores ${c}; $(ackpcts "${dir}"); $(balance "${dir}")"
}

# ------------------------------------------------------------ the cells
# prep_cell <stream> <durable:0|1>: apply the staged configuration (restarting
# when it changed), then make sure the data and the replicas are in a state worth
# measuring: a recreated stream for durable cells, fully replicated shards always.
prep_cell() {
  local s="$1" durable="$2" pct
  configure "${CONFIG_LABEL}" ${CONFIG_ARGS[@]+"${CONFIG_ARGS[@]}"} || return 1
  if [ "${durable}" = 1 ]; then
    ensure_fresh "${s}" || return 1
    pct="$(disk_pct)"
    if [ -n "${pct}" ] && [ "${pct}" -ge "${DISK_MAX_PCT}" ]; then
      plog "WARN /data at ${pct}% after the recreate"
      heavy_reset
      configure "${CONFIG_LABEL}" ${CONFIG_ARGS[@]+"${CONFIG_ARGS[@]}"} || return 1
      ensure_fresh "${s}" || return 1
    fi
  else
    ensure_ready || return 1
  fi
}

run_entry() {   # 0 ran, 2 already done, 1 could not start
  local name step kind stream p b f benv extra durable=0 flags=()
  IFS='|' read -r name step kind stream p b f benv extra <<<"$1"
  is_done "${name}" && { log "skip ${name} (done)"; return 2; }
  case "${kind}" in
    fin) do_finalize; return $? ;;
    fail) run_failover "${name}" "${benv}"; return $? ;;
  esac
  is_durable_stream "${stream}" && durable=1
  # shellcheck disable=SC2086 # benv is a list of KEY=VALUE words
  stage "${BUILD_SHA}" ${benv}
  prep_cell "${stream}" "${durable}" || { echo "!! ${name}: prep failed" >&2; return 1; }
  case "${kind}" in
    ingest)
      # shellcheck disable=SC2207 # ingest_flags prints words
      flags=($(SHARDS="${SHARDS}" PAYLOAD="${p}" BATCH="${b}" IN_FLIGHT="${f}" ingest_flags "${stream}" "${KEYS}" "${PER}"))
      cell "${name}" "${NGEN}" "${flags[@]}" ;;
    lat)
      cell "${name}" 1 --scenario pubsub --stream "${stream}" --payload-bytes "${p}" --fanout 1 --batch 1 --warmup "${LAT_WARMUP}" --total "${LAT_TOTAL}" ;;
    fwd)
      if [ "${extra}" = entry ]; then
        cell "${name}" 1 --scenario pubsub --stream "${stream}" --payload-bytes "${p}" --fanout 1 --batch 1 --via-entry --warmup "${LAT_WARMUP}" --total "${LAT_TOTAL}"
      else
        cell "${name}" 1 --scenario pubsub --stream "${stream}" --payload-bytes "${p}" --fanout 1 --batch 1 --warmup "${LAT_WARMUP}" --total "${LAT_TOTAL}"
      fi ;;
    cache)
      case "${extra}" in
        1) f=20000 ;;
        8) f=100000 ;;
        *) f=200000 ;;
      esac
      cell "${name}" 1 --scenario cache --cache perf-quorum --payload-bytes 256 --concurrency "${extra}" --total "${f}" ;;
  esac
  return 0
}

# do_finalize: the one-way lease-free switch. Skipped when the build lacks the features.
LF_SKIP=0
[ -e "${OUT}/rf3/lf-skip" ] && LF_SKIP=1
do_finalize() {
  local f have=0
  cp_remote 'curl -fsS -H "Authorization: Bearer $t" "$u/v1/fleet/features"; echo
echo __RUNOK__' > "${OUT}/rf3/fleet-features-before.txt" 2>&1
  for f in generation_start majority_ack lease_free_reads; do
    grep -q "${f}" "${OUT}/rf3/fleet-features-before.txt" && have=$((have + 1))
  done
  if [ "${have}" -lt 3 ]; then
    LF_SKIP=1; touch "${OUT}/rf3/lf-skip"
    plog "lease-free: build lists ${have}/3 of generation_start majority_ack lease_free_reads; skipping the lf step"
    touch "${OUT}/rf3/finalize-lease-free.done"
    return 0
  fi
  finalize_features generation_start majority_ack lease_free_reads || { plog "FAIL finalize"; return 1; }
  sleep "${LF_WAIT_SECS}"
  CURRENT_REF=""; CURRENT_OVERRIDES=""   # restart on the next cell so the brokers start finalized
  cp "${OUT}/system/fleet-features.txt" "${OUT}/rf3/fleet-features-after.txt" 2>/dev/null || true
  touch "${OUT}/rf3/finalize-lease-free.done"
  plog "lease-free features finalized"
}

# ----------------------------------------------------------------- failover
read -r -d '' FAIL_PY <<'PY' || true
import glob, json, re, statistics as st, sys
cdir, kill_t, restart_t = sys.argv[1], float(sys.argv[2]), float(sys.argv[3])
bins = {}
for path in glob.glob(cdir + "/felixperf-broker-*.series.tsv"):
    cols, prev = None, None
    for line in open(path, errors="replace"):
        if line.startswith("#"):
            continue
        parts = line.rstrip("\n").split("\t")
        if cols is None:
            cols = parts
            continue
        if len(parts) != len(cols) or "NA" in parts:
            prev = None
            continue
        row = dict(zip(cols, parts))
        cur = (float(row["t"]), float(row["append_bytes"]))
        if prev and cur[0] > prev[0] and cur[1] >= prev[1]:
            sec = int(cur[0])
            bins[sec] = bins.get(sec, 0.0) + (cur[1] - prev[1]) / (cur[0] - prev[0])
        prev = cur
if not bins:
    print("no series"); sys.exit(0)
k, r = int(kill_t), int(restart_t)
pre = [v for s, v in bins.items() if k - 25 <= s < k - 2]
if len(pre) < 5:
    print("too few pre-kill samples (%d)" % len(pre)); sys.exit(0)
base = st.median(pre)
def pct(v): return 100.0 * v / base if base else 0.0
def first(after, thr, run=3):
    for s in range(after, max(bins) - run + 1):
        if all(pct(bins.get(s + i, 0.0)) >= thr for i in range(run)):
            return s - after
    return None
down = [bins.get(s, 0.0) for s in range(k, r)]
rec1 = first(k, 50)
rec2 = first(r, 90)
low = min((pct(bins.get(s, 0.0)) for s in range(k, r)), default=0)
zero = sum(1 for s in range(k, r) if pct(bins.get(s, 0.0)) < 10)
print("baseline_append_mb_s=%.0f" % (base / 1e6))
print("stall_secs_below_10pct=%d" % zero)
print("secs_to_50pct_after_kill=%s" % rec1)
print("min_pct_during_outage=%.0f mean_pct_during_outage=%.0f (two of three copies append, so about 67 is the best possible)" % (low, pct(st.mean(down)) if down else 0))
print("secs_to_90pct_after_restart=%s" % rec2)
PY

read -r -d '' LOSS_PY <<'PY' || true
import glob, json, sys
cdir = sys.argv[1]
acked = cut = retries = published = 0
for p in glob.glob(cdir + "/*.run.txt"):
    for l in open(p, errors="replace"):
        if l.startswith("LOADGEN_JSON "):
            try:
                j = json.loads(l.split(" ", 1)[1])
            except Exception:
                continue
            if j.get("scenario") == "ingest":
                acked += int(j.get("acked_records", 0)); cut += int(j.get("cut_off_records", 0))
                retries += int(j.get("publish_retries", 0)); published += int(j.get("published", 0))
recv = gaps = losses = None
for l in open(cdir + "/subscriber.txt", errors="replace") if glob.glob(cdir + "/subscriber.txt") else []:
    if l.startswith("LOADGEN_JSON "):
        try:
            j = json.loads(l.split(" ", 1)[1])
        except Exception:
            continue
        recv, gaps, losses = int(j.get("received", 0)), int(j.get("gaps", 0)), int(j.get("shard_losses", 0))
print("acked_records=%d published=%d cut_off=%d publish_retries=%d" % (acked, published, cut, retries))
if recv is None:
    print("subscriber_received=none loss_check=NOT RUN (no subscriber result)")
else:
    ok = gaps == 0 and losses == 0 and recv >= acked
    print("subscriber_received=%d gaps=%d shard_losses=%d" % (recv, gaps, losses))
    print("acked_record_loss=%s" % ("0 (every acked record reached the live subscriber, no offset gap)" if ok else "SUSPECTED: received %d for %d acked, gaps %d, shard losses %d" % (recv, acked, gaps, losses)))
PY

# run_failover <name> <benv>: acked durable load on perf-failover from three
# generators, a subscriber on the fourth, kill -9 of broker FAIL_VICTIM at
# start_at + FAIL_KILL_AFTER, restart FAIL_DOWN_SECS later.
run_failover() {
  local name="$1" benv="$2" dir sa kt vm sub cpid kpid spid k0 r0 victim secs dropin lgenv kv
  dir="${OUT}/cells/${name}"
  victim="${BROKER_VMS[${FAIL_VICTIM}]}"
  sub="${GENS[$((NGEN - 1))]}"
  dropin=/etc/systemd/system/felix-broker.service.d/norestart.conf
  # shellcheck disable=SC2086
  stage "${BUILD_SHA}" ${benv}
  prep_cell perf-failover 1 || { echo "!! ${name}: prep failed" >&2; return 1; }
  rm -rf "${dir}"
  # CELL_SECS is read when ingest_flags runs, so it is set before the flags are built.
  (
    CELL_SECS="${FAIL_SECS}"
    # shellcheck disable=SC2207 # ingest_flags prints words
    ffl=($(SHARDS="${SHARDS}" PAYLOAD=4096 BATCH=64 IN_FLIGHT="${INFLIGHT}" ingest_flags perf-failover "${KEYS}" "${PER}"))
    cell "${name}" $((NGEN - 1)) "${ffl[@]}"
  ) &
  cpid=$!
  sa=""
  for _ in $(seq 1 180); do
    sa="$(sed -n 's/^start_at=\([0-9][0-9]*\)$/\1/p' "${dir}/meta.env" 2>/dev/null | tail -1)"
    [ -n "${sa}" ] && break
    sleep 2
  done
  if [ -z "${sa}" ]; then kill "${cpid}" 2>/dev/null; wait "${cpid}" 2>/dev/null; plog "FAIL ${name}: the cell never got a start time"; return 1; fi
  lgenv=""; for kv in ${CLIENT_B}; do lgenv="${lgenv}export ${kv}
"; done
  # The subscriber starts with the publishers and outlasts them by 25 s. It counts
  # deliveries and offset gaps; it is the loss check's other side.
  ( run_on_str "${sub}" "set -eu
${lgenv}ulimit -n 1048576 || true
felix-loadgen --brokers '${BROKER_ADDRS}' --tenant perf --token-file '${TOKEN_FILE}' --environment 'azure-${SESSION}-${name}-sub' --scenario subscribe --stream perf-failover --fanout 1 --concurrency 1 --duration-secs $((FAIL_SECS + 25)) --start-at ${sa} 2>/dev/null | grep '^LOADGEN_JSON' | tail -1
echo __RUNOK__" > "${dir}/subscriber.txt" 2>&1 ) &
  spid=$!
  kt=$(( sa + FAIL_KILL_AFTER ))
  ( run_on_str "${victim}" "set -u
mkdir -p /etc/systemd/system/felix-broker.service.d
printf '[Service]\nRestart=no\n' > ${dropin}
systemctl daemon-reload
while [ \$(date +%s) -lt ${kt} ]; do sleep 0.2; done
echo kill_t=\$(date +%s.%N)
pkill -9 -x felix-broker
sleep ${FAIL_DOWN_SECS}
rm -f ${dropin}
systemctl daemon-reload
systemctl reset-failed felix-broker 2>/dev/null || true
echo restart_t=\$(date +%s.%N)
systemctl start felix-broker
echo __RUNOK__" > "${dir}/kill.txt" 2>&1 ) &
  kpid=$!
  wait "${cpid}"; wait "${spid}"; wait "${kpid}"
  # Whatever happened, the victim must restart on its own again.
  run_on_str "${victim}" "rm -f ${dropin}; systemctl daemon-reload; systemctl reset-failed felix-broker 2>/dev/null || true; systemctl start felix-broker; echo __RUNOK__" >/dev/null 2>&1
  k0="$(sed -n 's/^kill_t=//p' "${dir}/kill.txt" | tail -1)"; r0="$(sed -n 's/^restart_t=//p' "${dir}/kill.txt" | tail -1)"
  secs=$SECONDS
  if ensure_ready; then
    echo "copies_caught_up_secs_after_restart_call=$(( SECONDS - secs )) (waited from the end of the cell)" > "${dir}/catchup.txt"
  else
    echo "copies_caught_up=NO within ${REPL_WAIT_SECS} s" > "${dir}/catchup.txt"
  fi
  {
    echo "victim=${victim} kill_t=${k0:-none} restart_t=${r0:-none} planned_kill_t=${kt}"
    if [ -n "${k0}" ] && [ -n "${r0}" ]; then python3 -I -c "${FAIL_PY}" "${dir}" "${k0}" "${r0}"; else echo "no kill timestamps (see kill.txt)"; fi
    python3 -I -c "${LOSS_PY}" "${dir}"
    cat "${dir}/catchup.txt"
  } > "${dir}/failover.txt" 2>&1
  # A cell is done when the instrument finished; the loss line decides what it means.
  return 0
}

# ----------------------------------------------------------------- the run
setup_session
refresh_client_token || distribute_token || die "client token"
if ! ensure_ready; then die "cluster not ready and fully replicated after setup (see ${OUT}/rf3/last-replication.txt)"; fi
record_session rf3-campaign
collect_facts
plog "setup done; facts recorded in ${OUT}"

cur=""; consec=0; skipstep=0; failed=""
close_step() { [ -z "${cur}" ] || [ -z "${failed}" ] || plog "FAIL step ${cur}: incomplete${failed}"; }
for ((i = 0; i < M; i++)); do
  name="$(spec_name "${PSPEC[i]}")"; step="${PSTEP[i]}"
  if [ "${step}" != "${cur}" ]; then
    close_step
    cur="${step}"; consec=0; skipstep=0; failed=""
    n=0; for ((j = 0; j < M; j++)); do [ "${PSTEP[j]}" = "${step}" ] && n=$((n + 1)); done
    plog "[$((i + 1))/${M}]  STEP ${step} start (${n} plan lines)  ETA $(eta "${REM[i]}")"
  fi
  [ "${skipstep}" = 1 ] && continue
  [ "${step}" = lf ] && [ "${LF_SKIP}" = 1 ] && continue
  IFS='|' read -r _ _ kind _ <<<"${PSPEC[i]}"
  run_entry "${PSPEC[i]}"; rc=$?
  [ "${rc}" = 2 ] && continue
  if [ "${kind}" = fin ]; then
    if is_done "${name}"; then plog "[$((i + 1))/${M}]  ${name} done"; else failed="${failed} ${name}"; skipstep=1; fi
    continue
  fi
  if is_done "${name}"; then
    consec=0
    plog "[$((i + 1))/${M}]  ${name}  $(headline "${OUT}/cells/${name}")  ETA $(eta "${REM[i + 1]}")"
    mark_logged "${name}"
  else
    consec=$((consec + 1)); failed="${failed} ${name}"
    plog "[$((i + 1))/${M}]  FAIL ${name}  incomplete (see ${OUT}/cells/${name})  ETA $(eta "${REM[i + 1]}")"
    [ "${kind}" = fail ] && [ -s "${OUT}/cells/${name}/failover.txt" ] && plog "  ${name} partial: $(headline "${OUT}/cells/${name}")"
    if [ "${consec}" -ge 3 ]; then
      plog "FAIL ${step}: 3 cells in a row failed; skipping the rest of the step"
      skipstep=1
    fi
  fi
done
close_step

# One more try at plan lines that did not finish (not the one-way finalize or the failover).
retried=0
for ((i = 0; i < M && retried < 10; i++)); do
  name="$(spec_name "${PSPEC[i]}")"
  IFS='|' read -r _ step kind _ <<<"${PSPEC[i]}"
  case "${kind}" in fin|fail) continue ;; esac
  [ "${step}" = lf ] && [ "${LF_SKIP}" = 1 ] && continue
  is_done "${name}" && continue
  retried=$((retried + 1))
  plog "retry ${name}"
  run_entry "${PSPEC[i]}"
  is_done "${name}" && plog "  ${name}  $(headline "${OUT}/cells/${name}")"
done

python3 "${here}/summarize.py" "${OUT}" || true
plog "DONE"
