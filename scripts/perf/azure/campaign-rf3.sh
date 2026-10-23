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

self="$(cd "$(dirname "$0")" && pwd -P)"
if [ -f "${self}/lib.sh" ]; then here="${self}"; else here="$(cd "${self}/../.." && pwd -P)"; fi
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
    *) echo "usage: $0 [--dry-run] [--print-provision] [--force] [--trim=0..7]" >&2; exit 2 ;;
  esac
done

# A sleeping laptop stops the run; re-exec under caffeinate on macOS.
if [ "${dry}" = 0 ] && [ "${printprov}" = 0 ] && [ -z "${CAMP_CAFFEINATED:-}" ] && command -v caffeinate >/dev/null 2>&1; then
  CAMP_CAFFEINATED=1 exec caffeinate -dimsu "$0" "$@"
fi

: "${SESSION:=<session>}"
: "${BUILD_SHA:=<BUILD_SHA>}"
: "${LOCATION:=centralus}"
# The loadgen builds from BUILD_SHA unless overridden; it needs owner-aware batch
# publishing and reconnect on broker loss (#1424).
: "${LOADGEN_SHA:=${BUILD_SHA}}"
# JSON field names the loadgen reports (#1424), in one place; each entry lists the
# names tried, first match wins. A mapping that finds nothing makes the loss check
# INCOMPLETE. There is no delivered side: what the brokers appended comes from
# felixctl inspect shard (inspect_tails).
export MAP_ACKED_SHARD="per_shard_acked"      # ingest: acked records per shard
export MAP_RECONNECTS="reconnects"
export MAP_FAILED_SENDS="failed_sends"
export MAP_ACKED_SENDS="acked_sends"            # exact forwarding, from felix_client::publishes_forwarded()
export MAP_FORWARDED_SENDS="forwarded_sends"    # null for fire-and-forget

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
: "${BAL_WAIT_SECS:=600}"      # overall limit for moving leaders into balance
: "${BAL_BATCH:=3}"            # moves started per round (move limits)
: "${BAL_POLL_SECS:=15}"
: "${RUNCMD_TIMEOUT:=900}"   # one az vm run-command; bounds every wait that polls through one
: "${AZ_TIMEOUT:=600}"
: "${SLACK_MIN:=45}"         # the honest estimate must leave this much under MAX_HOURS
: "${DISK_MAX_PCT:=60}"
# fio gate, a measured baseline: the harness job (256 KiB writes, one job, an
# fdatasync after each) runs once per broker. A broker must reach FIO_REL_PCT of
# the best broker and FIO_FLOOR MB/s. The floor is absolute because three equally
# bad hosts would pass the relative test. It is not Microsoft's 750 MBps for the
# L4as_v4 (direct I/O, deep queue): a queue-depth-1 fdatasync job cannot reach
# that. Basis for 400: the same job on L8as_v4 (4 disks) read 1197-1416 MB/s on a
# good host and 386-562 on the degraded eastus2 hosts (#1406); the L4as_v4 has two
# disks (Lasv4 docs), so a good host is expected near 600-700 and a degraded one
# under 300.
: "${FIO_REL_PCT:=85}"
: "${FIO_FLOOR:=400}"
# Wall-clock minutes per step, in tenths (run-command overhead dominates).
: "${MIN_ING_X10:=55}"
: "${MIN_LAT_X10:=40}"
: "${MIN_CACHE_X10:=30}"
: "${MIN_FAIL_X10:=80}"
: "${MIN_FIN_X10:=50}"
: "${RESTART_X10:=25}"       # all three brokers: stop, activate, start, ready, settle, replicated
: "${RESET_X10:=30}"         # recreate a durable stream, wait for replication; allows for the uneven-leader retries
: "${HEAVY_MIN:=25}"          # one full wipe and reseed (disk guard), reserved in the total
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
# The generated certificate is exported so felixctl can verify the brokers (inspect).
CERT_FILE=/etc/felix/broker-cert.pem
BASE_B="FELIX_QUIC_LISTENERS=2 FELIX_DURABLE_FSYNC_MODE=on_commit ${DUR_PROFILE} FELIX_TLS_CERT_EXPORT=${CERT_FILE}"
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
  BROKER_REFS=${BUILD_SHA} LOADGEN_REF=${LOADGEN_SHA} TEARDOWN_AFTER_H=12 \\
  ${here}/session.sh
# BROKER_REFS builds the SHA on generator 0 and installs it on the three brokers and
# the control plane; LOADGEN_REF builds the instrument on every generator (set
# LOADGEN_SHA to a newer commit for owner-direct ingest and reconnects).
# The campaign checks the brokers and control plane report ${BUILD_SHA:0:12} and the
# generators ${LOADGEN_SHA:0:12} before it measures.
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
        add "rf3-q-${m}-p${p}-b${b}-ack-t${t}" m ingest "$(stream_of "${m}")" "${p}" "${b}" "${INFLIGHT}" "${BASE_B}" owner
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
  # fwd ingest: the old round-robin publishers, a publish entering a non-leader is forwarded.
  for t in $(seq 1 "${FWD_TRIALS}"); do
    trim_has 3 && [ "${t}" -gt 1 ] && continue
    order="mem dur"; [ $((t % 2)) = 0 ] && order="dur mem"
    for p in 256 4096; do for m in ${order}; do
      trim_has 6 && [ "${m}" = dur ] && continue
      add "rf3-fwdq-${m}-p${p}-b64-ack-t${t}" fwd ingest "$(stream_of "${m}")" "${p}" 64 "${INFLIGHT}" "${BASE_B}" fwd
    done; done
  done
  # cache: Quorum put and get.
  for t in $(seq 1 "${CACHE_TRIALS}"); do for c in 1 8 32; do
    trim_has 7 && [ "${t}" -gt 1 ] && continue
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
        add "${pre}-q-${m}-p$1-b$2-ack-t${t}" lf ingest "$(stream_of "${m}")" "$1" "$2" "${INFLIGHT}" "${BASE_B}" owner
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
    add "rf3-fail-t${t}" fail fail perf-failover 4096 64 "${INFLIGHT}" "${BASE_B}" owner
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
    if [ "${c}" -gt 0 ] && [ "${kind}" = fin ]; then
      c=$((c + LF_WAIT_SECS * 10 / 60 + RESTART_X10)); prevkey=""   # the wait, then the restart on the finalized fleet
    elif [ "${c}" -gt 0 ]; then
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
# The cells, setup, one heavy reset held in reserve, token refreshes and the failover
# teardown; the budget the trim must meet leaves SLACK_MIN under MAX_HOURS.
total_x10() { echo $(( REM[0] + SETUP_MIN * 10 + HEAVY_MIN * 10 + 120 )); }
budget_x10() { echo $(( MAX_HOURS * 600 - SLACK_MIN * 10 )); }

choose_trim() {
  local saved="${OUT}/rf3/trim" lvl
  if [ -n "${TRIM_ARG}" ]; then TRIM="${TRIM_ARG}"; build_plan; compute_rem; return; fi
  if [ -s "${saved}" ] && [ "${dry}" = 0 ]; then TRIM="$(cat "${saved}")"; build_plan; compute_rem; return; fi
  for lvl in 0 1 2 3 4 5 6 7; do
    TRIM="${lvl}"; build_plan
    IGNORE_DONE=1 compute_rem
    [ "$(total_x10)" -le "$(budget_x10)" ] && break
  done
  OVER=0
  [ "$(total_x10)" -le "$(budget_x10)" ] || OVER=1
  [ "${dry}" = 0 ] && { mkdir -p "${OUT}/rf3"; echo "${TRIM}" > "${saved}"; }
  IGNORE_DONE=0 compute_rem
}
trim_text() {
  case "$1" in
    0) echo "nothing" ;;
    1) echo "latency-profile cells" ;;
    2) echo "$(trim_text 1), second lease-free trial" ;;
    3) echo "$(trim_text 2), second forwarding trial" ;;
    4) echo "$(trim_text 3), second trial of the non-headline matrix shapes" ;;
    5) echo "$(trim_text 4), lease-free latency cells" ;;
    6) echo "$(trim_text 5), the durable forwarding-ingest arm" ;;
    7) echo "$(trim_text 6), second cache trial" ;;
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
  echo "BUILD_SHA ${BUILD_SHA}  (brokers and control plane); LOADGEN_SHA ${LOADGEN_SHA}"
  echo "topology: ${NBROKERS} x Standard_L4as_v4 (local NVMe RAID0, zones 1/2/3), ${NGEN} x Standard_D4as_v5, 1 x Standard_D2as_v5; RF=3, ${SHARDS} shards"
  echo "broker env: ${BASE_B} (+ lib.sh base: STORAGE_IO_URING=1 IO_RUNTIME_THREADS=0 UDP buffers 24 MiB, MTU_UPPER_BOUND=3872)"
  echo "client env: ${CLIENT_B}"
  echo "cell run: ${CELL_SECS} s, ${PER} publishers/generator x ${NGEN} generators, ${KEYS} keys over ${SHARDS} shards, in flight ${INFLIGHT}; failover cell ${FAIL_SECS} s, kill at +${FAIL_KILL_AFTER} s, down ${FAIL_DOWN_SECS} s"
  echo "fio gate: each broker's seq256k-j1 >= ${FIO_REL_PCT}% of the best broker and >= ${FIO_FLOOR} MB/s (measured baseline; see the header of FIO_FLOOR)"
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
  printf 'cells %d, total about %s h incl. one reserved full reset (slack %s min under %s h), estimated cost $%s at $%s/h (%d x $%s + %d x $%s + $%s; budget %s h)\n' "${M}" "$(hours_of "${tot}")" "$(( (MAX_HOURS * 600 - tot) / 10 ))" "${MAX_HOURS}" \
    "$(awk -v x="${tot}" -v p="${perh}" 'BEGIN { printf "%.0f", x / 600 * p }')" "${perh}" "${NBROKERS}" "${PRICE_BROKER}" "${NGEN}" "${PRICE_GEN}" "${PRICE_CP}" "${MAX_HOURS}"
  exit 0
fi

# ---------------------------------------------------------------- live run
if [ -s "${LOCK}" ] && kill -0 "$(cat "${LOCK}")" 2>/dev/null; then
  echo "!! rf3-campaign already running (pid $(cat "${LOCK}"))" >&2; exit 1
fi
[[ "${BUILD_SHA}" =~ ^[0-9a-f]{40}$ ]] || { echo "!! BUILD_SHA must be a 40-char commit sha" >&2; exit 2; }
[[ "${LOADGEN_SHA}" =~ ^[0-9a-f]{40}$ ]] || { echo "!! LOADGEN_SHA must be a 40-char commit sha" >&2; exit 2; }
[ -f "${here}/sessions/${SESSION}.env" ] || { echo "!! no sessions/${SESSION}.env" >&2; exit 2; }
echo $$ > "${LOCK}"
VMS_STARTED=0
# with_timeout <secs> <cmd>...: the command, killed after secs. timeout(1) where there
# is one; otherwise a background job and a killer.
with_timeout() {
  local secs="$1" p w rc
  shift
  if command -v timeout >/dev/null 2>&1; then timeout "${secs}" "$@"; return $?; fi
  if command -v gtimeout >/dev/null 2>&1; then gtimeout "${secs}" "$@"; return $?; fi
  "$@" & p=$!
  ( sleep "${secs}"; kill "${p}" 2>/dev/null; sleep 5; kill -9 "${p}" 2>/dev/null ) >/dev/null 2>&1 & w=$!
  wait "${p}"; rc=$?
  kill "${w}" 2>/dev/null; wait "${w}" 2>/dev/null
  return "${rc}"
}
# Every az call is bounded, and run-command (what run_on and agent_on use, in the
# scripts this one starts too) gets RUNCMD_TIMEOUT, so a hung call cannot outlast
# the deadline of the wait that polls through it.
AZ_BIN="$(command -v az || true)"
az() {
  case "$1 $2" in
    "vm run-command") with_timeout "${RUNCMD_TIMEOUT}" "${AZ_BIN}" "$@" ;;
    *) with_timeout "${AZ_TIMEOUT}" "${AZ_BIN}" "$@" ;;
  esac
}
export -f az with_timeout
export AZ_BIN RUNCMD_TIMEOUT AZ_TIMEOUT

# dealloc: by the explicit VM list from the inventory, never from a listing that
# can fail. Done only when a listing succeeds and shows every one of them
# deallocated; a failed or short listing is unverified and retried.
dealloc() {
  [ "${VMS_STARTED}" = 1 ] && [ "${NO_DEALLOC:-0}" != 1 ] || return 0
  local try left listing names vm
  names="$(for vm in ${BROKER_VMS[@]+"${BROKER_VMS[@]}"} "felixperf-controlplane" ${GENS[@]+"${GENS[@]}"}; do echo "${vm}"; done | sort -u)"
  for try in 1 2 3 4; do
    # shellcheck disable=SC2086
    az vm deallocate --ids $(for n in ${names}; do printf '%s ' "$(az vm show -g "${GROUP}" -n "${n}" --query id -o tsv)"; done) -o none || plog "WARN deallocate call failed (attempt ${try})"
    if listing="$(az vm list -g "${GROUP}" -d --query '[].[name,powerState]' -o tsv 2>/dev/null)" && [ -n "${listing}" ]; then
      left=""
      for vm in ${names}; do
        printf '%s\n' "${listing}" | grep -q "^${vm}[[:space:]]VM deallocated$" || left="${left} ${vm}"
      done
      if [ -z "${left}" ]; then plog "${GROUP} deallocated (verified: $(echo "${names}" | wc -l | tr -d ' ') VMs)"; return 0; fi
      plog "WARN deallocate attempt ${try}: not deallocated:${left}"
    else
      plog "WARN deallocate attempt ${try}: could not list the VMs; not verified"
    fi
    sleep $((try * 30))
  done
  plog "FAIL deallocate: ${GROUP} NOT VERIFIED DEALLOCATED (${names//$'\n'/ }); check and stop it by hand NOW"
  echo "!!!! ${GROUP} may still be running and billing; deallocate it by hand" >&2
  return 1
}
on_exit() { [ "${PLACEMENT_PAUSED:-0}" = 0 ] || [ "${VMS_STARTED:-0}" = 0 ] || placement_resume; dealloc; rm -f "${LOCK}"; }
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
  local fail=0 vm out r mbs nic sizes want best
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
    case "${out}" in *"${LOADGEN_SHA}"*) printf '%-28s OK loadgen %s\n' "${vm}" "${LOADGEN_SHA:0:12}" ;; *) printf '%-28s BAD loadgen %s\n' "${vm}" "${out}"; fail=1 ;; esac
  done
  for vm in "${BROKER_VMS[@]}"; do
    out="$(run_on_str "${vm}" 'findmnt -n -o SOURCE /data; systemctl is-active felix-broker; grep -c NODE_REFRESH_TOKEN_FILE /etc/felix/broker.env; test -s /etc/felix/node.refresh_token && echo refresh=yes' 2>&1 | grep -v '^\[std\|Enable succ\|__RUNOK' | tr '\n' ' ')"
    case "${out}" in
      */dev/md0*active*1*refresh=yes*) printf '%-28s OK storage-auth %s\n' "${vm}" "${out}" ;;
      *) printf '%-28s BAD storage-auth %s\n' "${vm}" "${out}"; fail=1 ;;
    esac
  done
  # The fio gate: measured per broker, then judged against the best of the three.
  best=0
  for vm in "${BROKER_VMS[@]}"; do
    mbs="$(grep -h 'seq256k-j1.mb_s=' "${OUT}/system/${vm}.fio.txt" 2>/dev/null | cut -d= -f2 | cut -d. -f1)"
    [ -n "${mbs}" ] && [ "${mbs}" -gt "${best}" ] && best="${mbs}"
  done
  for vm in "${BROKER_VMS[@]}"; do
    mbs="$(grep -h 'seq256k-j1.mb_s=' "${OUT}/system/${vm}.fio.txt" 2>/dev/null | cut -d= -f2 | cut -d. -f1)"
    if [ -n "${mbs}" ] && [ "${mbs}" -ge "${FIO_FLOOR}" ] && [ $(( mbs * 100 )) -ge $(( best * FIO_REL_PCT )) ]; then
      printf '%-28s OK fio %s MB/s (best %s; gate %s%% of best and floor %s)\n' "${vm}" "${mbs}" "${best}" "${FIO_REL_PCT}" "${FIO_FLOOR}"
    else
      printf '%-28s BAD fio %s MB/s (best %s; gate %s%% of best and floor %s)\n' "${vm}" "${mbs:-none}" "${best}" "${FIO_REL_PCT}" "${FIO_FLOOR}"; fail=1
    fi
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

# Placement balances leadership cluster-wide, not per stream, so leftover streams
# can leave the measured one 1/1/10. The operator move API fixes that where
# recreating cannot. Plan: counts per broker for one stream, then moves from the
# fullest broker to the emptiest until they differ by at most one.
IFS= read -r -d '' BAL_PY <<'PY' || true
import json, sys
stream, batch, inflight_path = sys.argv[1], int(sys.argv[2]), sys.argv[3]
want_cache = stream.startswith("cache/")
name = stream[6:] if want_cache else stream
items = json.load(sys.stdin).get("items", [])
nodes = sorted(set(it["leader"] for it in items if it.get("leader")))
mine = {n: [] for n in nodes}
for it in items:
    is_cache = str(it.get("kind", "")).lower() == "cache"
    if it.get("stream") == name and is_cache == want_cache and it.get("leader"):
        mine[it["leader"]].append(it)
try:
    inflight = len([m for m in json.load(open(inflight_path)).get("items", [])
                    if m.get("stream") == name and (str(m.get("kind", "")).lower() == "cache") == want_cache])
except Exception:
    inflight = 0
counts = dict((n, len(v)) for n, v in mine.items())
moves = []
if inflight == 0:
    while len(moves) < batch and nodes:
        hi = max(nodes, key=lambda n: counts[n])
        lo = min(nodes, key=lambda n: counts[n])
        if counts[hi] - counts[lo] <= 1:
            break
        it = mine[hi].pop()
        counts[hi] -= 1
        counts[lo] += 1
        mine[lo].append(it)
        moves.append({"tenant_id": it["tenant_id"], "namespace": it["namespace"], "stream": it["stream"],
                      "shard": it["shard"], "kind": it.get("kind", "stream"), "destination": lo, "dry_run": False})
now = [len([1 for it in items if it.get("leader") == n and it.get("stream") == name
            and (str(it.get("kind", "")).lower() == "cache") == want_cache]) for n in nodes]
even = 1 if nodes and max(now) - min(now) <= 1 else 0
print("counts=%s even=%d planned=%d inflight=%d" % ("/".join(str(c) for c in now), even, len(moves), inflight))
for m in moves:
    print(json.dumps(m))
PY

# One balancing round, run on generator 0 (cp_remote supplies $u and $t).
IFS= read -r -d '' BAL_SH <<'SH' || true
curl -fsS -H "Authorization: Bearer $t" "$u/v1/placement/replication" > /tmp/bal.repl || { echo "__BAL_BEGIN__error=replication__BAL_END__"; exit 0; }
curl -fsS -H "Authorization: Bearer $t" "$u/v1/shard-moves" > /tmp/bal.inflight || echo '{"items":[]}' > /tmp/bal.inflight
python3 /tmp/bal.py "$STREAM" "$BATCH" /tmp/bal.inflight < /tmp/bal.repl > /tmp/bal.plan || { echo "__BAL_BEGIN__error=plan__BAL_END__"; exit 0; }
head -n 1 /tmp/bal.plan > /tmp/bal.head
tail -n +2 /tmp/bal.plan > /tmp/bal.moves
started=0; refused=0
while IFS= read -r b; do
  code=$(curl -s -o /tmp/bal.resp -w '%{http_code}' -X POST "$u/v1/shard-moves" -H "Authorization: Bearer $t" -H 'Content-Type: application/json' -d "$b")
  case "$code" in 2*) started=$((started + 1)) ;; *) refused=$((refused + 1)) ;; esac
done < /tmp/bal.moves
echo "__BAL_BEGIN__$(cat /tmp/bal.head) started=${started} refused=${refused}__BAL_END__"
SH

# balance_leaders <stream>: operator moves until the stream's leaders differ by
# at most one per broker, a few at a time (move limits), within BAL_WAIT_SECS.
# Pauses placement afterwards so it does not move them back while measuring.
balance_leaders() {
  local s="$1" deadline out first="" last="" total=0 n
  # A pause stops a started move from progressing, so lift it while balancing.
  [ "${PLACEMENT_PAUSED}" = 0 ] || placement_resume
  deadline=$(( $(date +%s) + BAL_WAIT_SECS ))
  while :; do
    out="$(cp_remote "STREAM='${s}'; BATCH=${BAL_BATCH}
cat > /tmp/bal.py <<'PYEOF'
${BAL_PY}
PYEOF
${BAL_SH}
echo __RUNOK__" 2>/dev/null | extract_between BAL)"
    case "${out}" in
      counts=*)
        last="$(printf '%s\n' "${out}" | sed 's/^counts=\([^ ]*\) .*/\1/')"
        [ -n "${first}" ] || first="${last}"
        n="$(printf '%s\n' "${out}" | sed -n 's/.* started=\([0-9]*\).*/\1/p')"
        total=$(( total + ${n:-0} ))
        case "${out}" in
          # Even counts are not enough: a move still in flight holds a fence,
          # and pausing placement then freezes it with the shard's writes stuck.
          *" even=1 "*" inflight=0"*)
            plog "balanced ${s} leaders ${first} -> ${last} via ${total} moves"
            placement_pause
            return 0 ;;
        esac ;;
      *) plog "WARN balance ${s}: ${out:-no answer}" ;;
    esac
    if [ "$(date +%s)" -ge "${deadline}" ]; then
      plog "WARN ${s}: leaders still ${last:-unknown} after ${BAL_WAIT_SECS} s of moves (${total} started); not measuring"
      return 1
    fi
    sleep "${BAL_POLL_SECS}"
  done
}

# placement_pause / placement_resume: stop or allow placement's own moves.
# An operator move starts under a pause but does not progress until resumed; a
# failed leader is still replaced under one.
PLACEMENT_PAUSED=0
placement_pause() {
  cp_remote 'curl -fsS -X POST -H "Authorization: Bearer $t" "$u/v1/placement/pause" >/dev/null && echo __RUNOK__' >/dev/null 2>&1 \
    && PLACEMENT_PAUSED=1 || plog "WARN could not pause placement; leaders may move during the cell"
}
placement_resume() {
  cp_remote 'curl -fsS -X POST -H "Authorization: Bearer $t" "$u/v1/placement/resume" >/dev/null && echo __RUNOK__' >/dev/null 2>&1 \
    && PLACEMENT_PAUSED=0 || plog "WARN could not resume placement"
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
  local s list="$*" body
  # Not $(cat <<'BODY'): bash 3.2 mis-parses a quoted heredoc inside $() and
  # expands the remote script's variables here.
  IFS= read -r -d '' body <<'BODY' || true
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
  cp_remote "STREAMS='${list}'; SHARDS=${SHARDS}; RF=3
${body}
echo __RUNOK__" > "${OUT}/system/reset-streams.txt" 2>&1 || { plog "WARN stream recreate failed: $(tail -3 "${OUT}/system/reset-streams.txt" | tr '\n' ' ')"; return 1; }
}

# ensure_fresh <stream>: a recreated stream (frees the disk), fully replicated,
# with evenly spread leaders (moved into place, not re-rolled by recreating).
ensure_fresh() {
  local s="$1" attempt st
  for attempt in 1 2 3; do
    reset_streams "${s}" || continue
    sleep 12
    ensure_ready || continue
    st="$(cat "${OUT}/rf3/last-replication.txt" 2>/dev/null)"
    leaders_even "${st}" "${s}" || { plog "WARN ${s} leaders uneven (${st}); balancing"; balance_leaders "${s}" && ensure_ready || return 1; }
    return 0
  done
  return 1
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
  plog "setup: preflight OK (fio within ${FIO_REL_PCT}% of the best broker and >= ${FIO_FLOOR} MB/s on all ${NBROKERS} brokers)"
  touch "${OUT}/rf3/setup.ok"
}

collect_facts() {
  local cmds
  {
    echo "session=${SESSION} region=${LOCATION} group=${GROUP}"
    echo "BUILD_SHA=${BUILD_SHA} LOADGEN_SHA=${LOADGEN_SHA}"
    echo "trim=${TRIM} ($(trim_text "${TRIM}"))"
    echo "driver BASE_B=${BASE_B}"
    echo "driver LATP_B=${LATP_B}"
    echo "driver CLIENT_B=${CLIENT_B}"
    echo "CELL_SECS=${CELL_SECS} PER=${PER} NGEN=${NGEN} KEYS=${KEYS} SHARDS=${SHARDS} INFLIGHT=${INFLIGHT} RF=3"
    echo "FAIL_SECS=${FAIL_SECS} FAIL_KILL_AFTER=${FAIL_KILL_AFTER} FAIL_DOWN_SECS=${FAIL_DOWN_SECS} FAIL_VICTIM=${FAIL_VICTIM}"
    echo "FIO gate: >= ${FIO_REL_PCT}% of the best broker and >= ${FIO_FLOOR} MB/s on seq256k-j1"
    echo "LOADGEN_SHA=${LOADGEN_SHA}"
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

# fwd_line <dir>: how much of the cell's traffic was forwarded. Acked cells: the
# loadgen's exact forwarded_sends over acked_sends. Always: the brokers'
# felix_broker_forwards_total delta summed over the three. The loadgen share (percent)
# goes to forwarded_share.txt for the WARN check.
fwd_line() {
  local dir="$1" vm bf=0 share
  for vm in "${BROKER_VMS[@]}"; do
    bf=$(( bf + $(snapval "${dir}/${vm}.after.txt" m.forwards_total) - $(snapval "${dir}/${vm}.before.txt" m.forwards_total) ))
  done
  share="$(grep -h '^LOADGEN_JSON' "${dir}"/*.run.txt 2>/dev/null | python3 -I -c '
import json, os, sys
A = os.environ["MAP_ACKED_SENDS"].split(); F = os.environ["MAP_FORWARDED_SENDS"].split()
a = f = 0; seen = False
for l in sys.stdin:
    try:
        j = json.loads(l.split(" ", 1)[1])
    except Exception:
        continue
    x = next((j[k] for k in A if j.get(k) is not None), None)
    y = next((j[k] for k in F if j.get(k) is not None), None)
    if x is not None and y is not None:
        a += int(x); f += int(y); seen = True
print("%.2f" % (100.0 * f / a) if seen and a else "")')"
  echo "${share}" > "${dir}/forwarded_share.txt"
  [ -n "${share}" ] && share="${share}% of acked sends" || share="n/a (fire-and-forget)"
  echo "forwarded ${share}, brokers' forwards_total delta ${bf}"
}
# warn_forwarded <name> <dir>: an owner-direct cell should forward almost nothing.
warn_forwarded() {
  local share
  case "$1" in *fwdq*) return 0 ;; esac
  share="$(cat "$2/forwarded_share.txt" 2>/dev/null)"
  [ -n "${share}" ] && awk -v s="${share}" 'BEGIN { exit !(s > 5) }' && plog "WARN $1: forwarded_share ${share}% on an owner-direct cell"
  return 0
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
  echo "append ${a} MB/s (cluster, 3 copies), ingress ${i} MB/s, drops ${d}/s, cores ${c}; $(ackpcts "${dir}"); $(fwd_line "${dir}"); $(balance "${dir}")"
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
    # In-memory streams keep their data, but every broker must lead some shards
    # or the cell measures one broker.
    leaders_even "$(cat "${OUT}/rf3/last-replication.txt" 2>/dev/null)" "${s}" || { balance_leaders "${s}" && ensure_ready || return 1; }
  fi
}

# ingest_mode_flags <owner|fwd> <batch>: owner-direct publishes go to the shard's
# leader through the cluster client (--cluster, any batch size); fwd leaves the old round-robin entry, which forwards to the owner.
ingest_mode_flags() {
  [ "$1" = owner ] || return 0
  echo "--cluster"
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
      flags+=($(ingest_mode_flags "${extra}" "${b}"))
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
import glob, json, os, sys
cdir, before_f, after_f = sys.argv[1], sys.argv[2], sys.argv[3]
ACK_KEYS = os.environ["MAP_ACKED_SHARD"].split()
REC, FAILED = os.environ["MAP_RECONNECTS"].split(), os.environ["MAP_FAILED_SENDS"].split()
def first_int(j, names):
    for n in names:
        if n in j:
            return int(j[n] or 0)
    return 0
def shardmap(j, names):
    for n in names:
        v = j.get(n)
        if isinstance(v, dict):
            return {str(k): int(x) for k, x in v.items()}
        if isinstance(v, list):
            return {str(i): int(x) for i, x in enumerate(v)}
    return None
def inspected(path):
    out = {}
    try:
        for l in open(path, errors="replace"):
            if l.startswith("insp."):
                k, v = l.strip().split("=", 1)
                c = v.split("/")[0]
                out[k[5:]] = int(c) if c.isdigit() else None
    except OSError:
        pass
    return out
acked, gens = 0, 0
ack_shard, have_ack_shard = {}, True
extra = {"reconnects": 0, "failed_sends": 0, "cut_off_records": 0, "publish_retries": 0}
for p in glob.glob(cdir + "/*.run.txt"):
    for l in open(p, errors="replace"):
        if l.startswith("LOADGEN_JSON "):
            try:
                j = json.loads(l.split(" ", 1)[1])
            except Exception:
                continue
            if j.get("scenario") != "ingest":
                continue
            gens += 1
            acked += int(j.get("acked_records", 0))
            extra["reconnects"] += first_int(j, REC)
            extra["failed_sends"] += first_int(j, FAILED)
            extra["cut_off_records"] += int(j.get("cut_off_records", 0) or 0)
            extra["publish_retries"] += int(j.get("publish_retries", 0) or 0)
            m = shardmap(j, ACK_KEYS)
            if m is None:
                have_ack_shard = False
            else:
                for k, x in m.items():
                    ack_shard[k] = ack_shard.get(k, 0) + x
before, after = inspected(before_f), inspected(after_f)
print("generators_reporting=%d acked_records=%d %s" % (gens, acked, " ".join("%s=%d" % kv for kv in sorted(extra.items()))))
why = []
if gens == 0:
    why.append("no generator result")
if acked <= 0:
    why.append("acked_records is %d" % acked)
if not have_ack_shard or not ack_shard:
    why.append("generators report no per-shard acked counts")
if not before or any(v is None for v in before.values()):
    why.append("no complete inspect snapshot before the cell")
if not after or any(v is None for v in after.values()):
    why.append("no complete inspect snapshot after recovery")
if sum(ack_shard.values()) != acked and have_ack_shard and ack_shard:
    why.append("per-shard acked sums to %d, not %d" % (sum(ack_shard.values()), acked))
if why:
    print("acked_record_loss=INCOMPLETE (%s); not a clean result" % "; ".join(why))
else:
    short, total = {}, 0
    for k, x in ack_shard.items():
        grew = after.get(k, 0) - before.get(k, 0)
        total += grew
        if grew < x:
            short[k] = "acked %d, log grew %d" % (x, grew)
    print("appended_records_during_cell=%d (log commit marks after minus before, all shards)" % total)
    if short:
        print("acked_record_loss=SUSPECTED: %s" % short)
    else:
        print("acked_record_loss=0 (on all %d shards the log grew by at least the acked count; %d acked)" % (len(ack_shard), acked))
PY

read -r -d '' INSPECT_PY <<'PY' || true
import json, sys
seen = {}
for l in sys.stdin:
    l = l.strip()
    if not l.startswith("{"):
        continue
    try:
        r = json.loads(l)
    except Exception:
        continue
    ld = r.get("leader") or {}
    seen[r.get("shard")] = (ld.get("committed"), ld.get("tail"), ld.get("node_id", "none"))
n = int(sys.argv[1])
for s in range(n):
    c, t, node = seen.get(s, (None, None, "none"))
    print("insp.%d=%s/%s/%s" % (s, "x" if c is None else c, "x" if t is None else t, node))
PY

# build_felixctl: felixctl from the loadgen checkout on generator 0 (the release layout
# does not ship it), when it is missing or from another commit.
ensure_felixctl() {
  agent_on "${GENS[0]}" "grep -q ${LOADGEN_SHA} /etc/felix/felixctl.ref 2>/dev/null && test -x /usr/local/bin/felixctl && echo have" 2>/dev/null | grep -q '^have' && return 0
  plog "building felixctl at ${LOADGEN_SHA:0:8} on ${GENS[0]}"
  run_on_str "${GENS[0]}" "set -e
H=/home/felix; D=\$H/felix
as_felix() { sudo -u felix env HOME=\$H PATH=\$H/.cargo/bin:/usr/bin:/bin \"\$@\"; }
as_felix git -C \$D fetch -q --depth 1 origin ${LOADGEN_SHA}
as_felix git -C \$D checkout -q --detach FETCH_HEAD
cd \$D && as_felix env CARGO_TARGET_DIR=\$H/target \$H/.cargo/bin/cargo build --release --locked -q -p felixctl
install -m 0755 \$H/target/release/felixctl /usr/local/bin/felixctl
echo ${LOADGEN_SHA} > /etc/felix/felixctl.ref
echo __RUNOK__" > "${OUT}/rf3/felixctl-build.txt" 2>&1 || { plog "WARN felixctl build failed (see ${OUT}/rf3/felixctl-build.txt); the failover loss check will be INCOMPLETE"; return 1; }
}

# inspect_tails <outfile>: every shard of perf-failover as the leaders report it,
# "insp.<shard>=<commit mark>/<tail>/<leader>", x for a missing value. The brokers'
# generated certificates change at every start, so the trust bundle is rebuilt first.
# Needs node.view:cluster:*, which the admin token has. Retries until every shard shows
# a commit mark equal to its tail (the load has stopped), up to 8 times.
inspect_tails() {
  local out="$1" vm b64 all="" n try bundle=/home/felix/felix-session/brokers-ca.pem res
  : > "${out}"
  for vm in "${BROKER_VMS[@]}"; do
    b64="$(run_on_str "${vm}" "printf '__CA_BEGIN__%s__CA_END__\n' \"\$(base64 -w0 < ${CERT_FILE})\"
echo __RUNOK__" 2>/dev/null | extract_between CA)"
    [ -n "${b64}" ] || { echo "no certificate from ${vm}" >> "${out}"; return 1; }
    all="${all}${b64}
"
  done
  for try in 1 2 3 4 5 6 7 8; do
    res="$(cp_remote "rm -f ${bundle}
for b in $(printf '%s ' ${all}); do printf '%s' \"\$b\" | base64 -d >> ${bundle}; done
export FELIX_BROKERS='${BROKER_ADDRS}' FELIX_AUTH_TENANT=perf FELIX_NAMESPACE=default FELIX_AUTH_TOKEN_FILE=/home/felix/felix-session/admin.token FELIX_CA_FILE=${bundle} FELIX_SERVER_NAME=localhost
cat > /tmp/insp.py <<'PYEOF'
${INSPECT_PY}
PYEOF
felixctl inspect shard perf-failover --json 2>/tmp/insp.err | python3 /tmp/insp.py ${SHARDS} | sed 's/^/__R__/'
tail -2 /tmp/insp.err
echo __RUNOK__" 2>&1)"
    printf '%s\n' "${res}" | sed -n 's/^.*__R__\(insp\..*\)$/\1/p' > "${out}"
    n="$(grep -c '=[0-9][0-9]*/[0-9]*/' "${out}")"
    if [ "${n}" = "${SHARDS}" ] && ! awk -F'[=/]' '$2 != $3 { bad = 1 } END { exit bad }' "${out}" 2>/dev/null; then return 0; fi
    [ "${try}" = 1 ] && printf '%s\n' "${res}" | grep -E '^(Error|error|felixctl|!!)' | head -3 >> "${out}.err"
    sleep 10
  done
  [ "${n}" = "${SHARDS}" ]
}

# run_failover <name> <benv>: acked durable load on perf-failover from every
# generator, kill -9 of broker FAIL_VICTIM at start_at + FAIL_KILL_AFTER, restart
# FAIL_DOWN_SECS later. The loss check compares each shard's acked count against how far
# the shard's log grew, read from the leaders by felixctl inspect before the cell and
# after recovery.
run_failover() {
  local name="$1" benv="$2" dir sa kt cpid kpid k0 r0 victim secs dropin
  dir="${OUT}/cells/${name}"
  victim="${BROKER_VMS[${FAIL_VICTIM}]}"
  dropin=/etc/systemd/system/felix-broker.service.d/norestart.conf
  # shellcheck disable=SC2086
  stage "${BUILD_SHA}" ${benv}
  prep_cell perf-failover 1 || { echo "!! ${name}: prep failed" >&2; return 1; }
  placement_resume   # the kill needs normal placement
  ensure_felixctl >/dev/null 2>&1
  inspect_tails "${OUT}/rf3/${name}.inspect-before.txt" || plog "WARN ${name}: no complete inspect snapshot before the cell"
  rm -rf "${dir}"
  # CELL_SECS is read when ingest_flags runs, so it is set before the flags are built.
  (
    CELL_SECS="${FAIL_SECS}"
    # shellcheck disable=SC2207 # ingest_flags prints words
    ffl=($(SHARDS="${SHARDS}" PAYLOAD=4096 BATCH=64 IN_FLIGHT="${INFLIGHT}" ingest_flags perf-failover "${KEYS}" "${PER}") --cluster)
    cell "${name}" "${NGEN}" "${ffl[@]}"
  ) &
  cpid=$!
  sa=""
  for _ in $(seq 1 180); do
    sa="$(sed -n 's/^start_at=\([0-9][0-9]*\)$/\1/p' "${dir}/meta.env" 2>/dev/null | tail -1)"
    [ -n "${sa}" ] && break
    sleep 2
  done
  if [ -z "${sa}" ]; then kill "${cpid}" 2>/dev/null; wait "${cpid}" 2>/dev/null; plog "FAIL ${name}: the cell never got a start time"; return 1; fi
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
  wait "${cpid}"; wait "${kpid}"
  # Whatever happened, the victim must restart on its own again.
  run_on_str "${victim}" "rm -f ${dropin}; systemctl daemon-reload; systemctl reset-failed felix-broker 2>/dev/null || true; systemctl start felix-broker; echo __RUNOK__" >/dev/null 2>&1
  k0="$(sed -n 's/^kill_t=//p' "${dir}/kill.txt" | tail -1)"; r0="$(sed -n 's/^restart_t=//p' "${dir}/kill.txt" | tail -1)"
  secs=$SECONDS
  if ensure_ready; then
    echo "copies_caught_up_secs_after_the_cell=$(( SECONDS - secs ))" > "${dir}/catchup.txt"
  else
    echo "copies_caught_up=NO within ${REPL_WAIT_SECS} s" > "${dir}/catchup.txt"
  fi
  inspect_tails "${OUT}/rf3/${name}.inspect-after.txt" || plog "WARN ${name}: no complete inspect snapshot after recovery"
  cp "${OUT}/rf3/${name}.inspect-before.txt" "${OUT}/rf3/${name}.inspect-after.txt" "${dir}/" 2>/dev/null
  {
    echo "victim=${victim} kill_t=${k0:-none} restart_t=${r0:-none} planned_kill_t=${kt}"
    if [ -n "${k0}" ] && [ -n "${r0}" ]; then python3 -I -c "${FAIL_PY}" "${dir}" "${k0}" "${r0}"; else echo "no kill timestamps (see kill.txt)"; fi
    python3 -I -c "${LOSS_PY}" "${dir}" "${dir}/${name}.inspect-before.txt" "${dir}/${name}.inspect-after.txt"
    cat "${dir}/catchup.txt"
  } > "${dir}/failover.txt" 2>&1
  # A cell is done when the instrument finished; the loss line decides what it means.
  grep -q 'acked_record_loss=0 ' "${dir}/failover.txt" || plog "WARN ${name}: loss check not clean: $(grep acked_record_loss "${dir}/failover.txt" | head -1)"
  return 0
}

# ----------------------------------------------------------------- the run
setup_session
# A failover cell that was interrupted may have left the no-restart drop-in behind.
par_on "${OUT}/system" dropin "rm -f /etc/systemd/system/felix-broker.service.d/norestart.conf
systemctl daemon-reload
systemctl reset-failed felix-broker 2>/dev/null || true
systemctl start felix-broker" "${BROKER_VMS[@]}" || plog "WARN could not clear the failover drop-in on every broker"
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
    [ "${kind}" = ingest ] && warn_forwarded "${name}" "${OUT}/cells/${name}"
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
