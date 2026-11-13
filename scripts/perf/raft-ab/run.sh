#!/usr/bin/env bash
# Hybrid vs Raft shard engine, RF 3, one broker on each GCP build box
# (felix-build-1..3, us-central1-a, internal network). Same binary on both sides;
# FELIX_SHARD_ENGINE picks the engine.
#
# Each cell, on each side, in each round gets a fresh cluster: control plane
# (in memory, dev tokens) on box 1, a broker per box with its own data dir under
# /mnt/build/raft-ab/run/, one RF 3 stream, one measurement, then everything is
# stopped and wiped. Odd rounds run hybrid first, even rounds raft first, and the
# two sides of a cell run back to back.
#
# The generator runs on box ((round - 1) % 3) + 1, the same box for both sides of a
# round. At RF 3 every box holds a copy of every shard and the leaders spread over
# all three, so no box is free of broker work; rotating spreads the generator's CPU
# and its local-leader shortcut evenly over the boxes and both sides. Use a ROUNDS
# that is a multiple of 3.
#
# While it runs it holds all four build slots on every box, so lanes' remote.sh
# jobs queue instead of competing, and refuses to start unless every box has a
# 1 min load <= 2 and no java (TLC) process.
#
#   scripts/perf/raft-ab/build.sh      # once: build on box 3, install on all boxes
#   scripts/perf/raft-ab/run.sh        # the full A/B
#   scripts/perf/raft-ab/run.sh --smoke   # one round, one cell, short
#
# Settings (environment):
#   ROUNDS=6            rounds; odd rounds run hybrid first, even rounds raft first
#   CELLS="oncommit leader quorum fanout"
#   SECS=60             ingest duration per cell
#   PAYLOAD=1024 BATCH=16 PUBLISHERS=8 IN_FLIGHT=4 SHARDS=6 KEYS=$((SHARDS*8))
#   FANOUT=4 FANOUT_TOTAL=200000
#   ONLY_SIDE=hybrid|raft  run one engine only (a harness check, not an A/B)
#   SLOT_WAIT_SECS=3600  how long to wait for the lanes' build jobs to free the slots
#   QUIET_WAIT_SECS=300  after the slots are held, how long to wait for load < 2 and no java
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
RSSH=/Volumes/External/worktrees/.build/remote-ssh
user=hello_getfelix_dev
export CLOUDSDK_PYTHON=/Volumes/External/worktrees/.build/gcloud-venv/bin/python
R=/mnt/build/raft-ab

SMOKE=0
[ "${1:-}" = --smoke ] && SMOKE=1
if [ "$SMOKE" = 1 ]; then
  : "${ROUNDS:=1}" "${CELLS:=oncommit}" "${SECS:=20}"
fi
: "${ROUNDS:=6}" "${CELLS:=oncommit leader quorum fanout}" "${SECS:=60}"
: "${PAYLOAD:=1024}" "${BATCH:=16}" "${PUBLISHERS:=8}" "${IN_FLIGHT:=4}" "${SHARDS:=6}"
: "${KEYS:=$((SHARDS * 8))}" "${FANOUT:=4}" "${FANOUT_TOTAL:=200000}"
: "${SLOT_WAIT_SECS:=3600}" "${QUIET_WAIT_SECS:=300}" "${SETTLE_SECS:=10}" "${REPL_WAIT_SECS:=180}"
MAX_HOLD_SECS=$(( 3600 + ROUNDS * 8 * 600 ))

RUNID=$(date -u +%Y%m%dT%H%M%SZ)$([ "$SMOKE" = 1 ] && echo -smoke)
OUT=${OUT:-/Volumes/External/worktrees/.scratch/perf-raft-ab/runs/$RUNID}
mkdir -p "$OUT/cells"
exec > >(tee -a "$OUT/run.log") 2>&1 < /dev/null

BOXES=(1 2 3)
CP_BOX=1
log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*"; }
raw() { local b=$1; shift; "$RSSH" "$user@felix-build-$b" "$@"; }
on() { local b=$1; shift; raw "$b" "bash $R/node.sh $*"; }
IP=()
for b in "${BOXES[@]}"; do
  IP[$b]=$(gcloud compute instances describe "felix-build-$b" --zone=us-central1-a \
    --format='value(status,networkInterfaces[0].networkIP)' | awk '$1=="RUNNING"{print $2}')
  [ -n "${IP[$b]}" ] || { echo "!! felix-build-$b is not running; start it with remote.sh first" >&2; exit 1; }
done
BROKERS="${IP[1]}:15000,${IP[2]}:15000,${IP[3]}:15000"

# --- the binary, the same on every box -------------------------------------------
for b in "${BOXES[@]}"; do
  raw "$b" "mkdir -p $R/hold $R/run && cat > $R/node.sh" < "$here/node.sh"
done
sums=""
for b in "${BOXES[@]}"; do
  s=$(raw "$b" "cd $R/bin 2>/dev/null && sha256sum -c --quiet SHA256SUMS >&2 && cat REV SHA256SUMS") \
    || { echo "!! felix-build-$b has no intact $R/bin; run build.sh" >&2; exit 1; }
  [ -z "$sums" ] || [ "$s" = "$sums" ] || { echo "!! felix-build-$b has different binaries; run build.sh" >&2; exit 1; }
  sums=$s
done
{
  echo "runid $RUNID"
  echo "rev $(echo "$sums" | head -1)"
  echo "$sums" | tail -n +2
  echo "boxes ${IP[1]} ${IP[2]} ${IP[3]} (control plane on felix-build-$CP_BOX)"
  echo "ROUNDS=$ROUNDS CELLS=\"$CELLS\" SECS=$SECS PAYLOAD=$PAYLOAD BATCH=$BATCH PUBLISHERS=$PUBLISHERS IN_FLIGHT=$IN_FLIGHT SHARDS=$SHARDS KEYS=$KEYS FANOUT=$FANOUT FANOUT_TOTAL=$FANOUT_TOTAL"
} | tee "$OUT/run.env"

# --- reservation -----------------------------------------------------------------
CUR=""
cleanup() {
  set +e
  if [ -n "$CUR" ]; then
    for b in "${BOXES[@]}"; do on "$b" stop "$CUR"; on "$b" wipe "$CUR"; done
  fi
  for b in "${BOXES[@]}"; do on "$b" release "$RUNID" >/dev/null 2>&1; done
  log "released the build slots on felix-build-1..3"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

for b in "${BOXES[@]}"; do
  raw "$b" "setsid nohup bash $R/node.sh hold $RUNID $SLOT_WAIT_SECS $MAX_HOLD_SECS > $R/hold/$RUNID.log 2>&1 < /dev/null &"
done
log "waiting for all 4 build slots on each box (lanes' remote.sh jobs queue behind this run)"
deadline=$(( $(date +%s) + SLOT_WAIT_SECS + 60 ))
while :; do
  states=""; for b in "${BOXES[@]}"; do states="$states $(on "$b" hold-state "$RUNID")"; done
  case "$states" in *timeout*|*released*) echo "!! could not hold the slots:$states" >&2; exit 1 ;; esac
  [ "$states" = " held held held" ] && break
  [ "$(date +%s)" -lt "$deadline" ] || { echo "!! slots not held in time:$states" >&2; exit 1; }
  sleep 10
done
log "slots held on all boxes"

deadline=$(( $(date +%s) + QUIET_WAIT_SECS ))
while :; do
  quiet=1; report=""
  for b in "${BOXES[@]}"; do
    q=$(on "$b" quiet); report="$report box$b:$q"
    echo "$q" | awk -F'[= ]' '{ exit !($2 <= 2 && $4 == 0) }' || quiet=0
  done
  [ "$quiet" = 1 ] && break
  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo "!! boxes not quiet (need load <= 2 and no java):$report" >&2; exit 3
  fi
  log "waiting for quiet:$report"; sleep 20
done
log "quiet:$report"
echo "quiet at start:$report" >> "$OUT/run.env"

# --- one cell --------------------------------------------------------------------
cell_shape() { # sets CONS FSYNC SCEN
  case "$1" in
    oncommit) CONS=Quorum FSYNC=on_commit SCEN=ingest ;;
    leader) CONS=Leader FSYNC=periodic SCEN=ingest ;;
    quorum) CONS=Quorum FSYNC=periodic SCEN=ingest ;;
    fanout) CONS=Quorum FSYNC=on_commit SCEN=pubsub ;;
    *) echo "!! unknown cell $1" >&2; exit 2 ;;
  esac
}

# run_cell <round> <cell> <side> <loadgen box>: a fresh cluster, one measurement, wiped.
run_cell() {
  local r=$1 cell=$2 side=$3 lg=$4 name dir loc b i st e args limit
  name="r$r-$cell-$side"; dir="$R/run/$RUNID/$name"; loc="$OUT/cells/$name"; mkdir -p "$loc"
  cell_shape "$cell"
  CUR=$dir
  log "== $name: $SCEN, $CONS, fsync $FSYNC, loadgen on felix-build-$lg"
  for b in "${BOXES[@]}"; do on "$b" touch "$RUNID"; done
  status=ok
  {
    on "$CP_BOX" cp "$dir" && on "$CP_BOX" seed "$dir" 3
  } > "$loc/setup.txt" 2>&1 || status=setup-failed
  if [ "$status" = ok ]; then
    for b in "${BOXES[@]}"; do
      raw "$CP_BOX" "cat $dir/tokens/node-$b.jwt" | on "$b" put "$dir/node.jwt"
      raw "$CP_BOX" "cat $dir/tokens/node-$b.refresh" | on "$b" put "$dir/node.refresh"
    done
    [ "$lg" = "$CP_BOX" ] || raw "$CP_BOX" "cat $dir/tokens/client.jwt" | on "$lg" put "$dir/tokens/client.jwt"
    local pids=()
    for b in "${BOXES[@]}"; do
      on "$b" broker "$dir" "$b" "${IP[$CP_BOX]}" "$side" "$FSYNC" > "$loc/start-box$b.txt" 2>&1 &
      pids+=($!)
    done
    for i in "${pids[@]}"; do wait "$i" || status=broker-start-failed; done
  fi
  if [ "$status" = ok ]; then
    on "$CP_BOX" stream "$dir" bench "$SHARDS" "$CONS" >> "$loc/setup.txt" 2>&1 || status=stream-failed
  fi
  if [ "$status" = ok ]; then
    status=not-replicated
    for ((i = 0; i < REPL_WAIT_SECS; i += 3)); do
      st=$(on "$CP_BOX" repl "$dir" 2>/dev/null || true)
      case "$st" in "under=0 shards=$SHARDS "*) status=ok; break ;; esac
      sleep 3
    done
    echo "$st" > "$loc/replication.txt"
  fi
  # The engine check: the broker's own environment, and the Raft marker that only
  # a Raft member writes into its shard directory. Every broker holds every shard at RF 3.
  if [ "$status" = ok ]; then
    for b in "${BOXES[@]}"; do
      e=$(on "$b" engine "$dir"); echo "box$b before $e" >> "$loc/engine.txt"
      case "$side:$e" in
        hybrid:"env=hybrid raft_dirs=0 alive=1") ;;
        raft:"env=raft raft_dirs="*" alive=1")
          n=${e#*raft_dirs=}; n=${n%% *}; [ "$n" -ge "$SHARDS" ] || status=engine-mismatch ;;
        *) status=engine-mismatch ;;
      esac
    done
  fi
  if [ "$status" = ok ]; then
    sleep "$SETTLE_SECS"
    if [ "$SCEN" = ingest ]; then
      args="--scenario ingest --stream bench --payload-bytes $PAYLOAD --batch $BATCH --concurrency $PUBLISHERS --in-flight $IN_FLIGHT --duration-secs $SECS --keys $KEYS --shards $SHARDS --cluster"
      limit=$((SECS + 240))
    else
      args="--scenario pubsub --stream bench --payload-bytes $PAYLOAD --batch $BATCH --fanout $FANOUT --warmup 2000 --total $FANOUT_TOTAL"
      limit=900
    fi
    on "$lg" loadgen "$dir" "$limit" --brokers "$BROKERS" --environment "gcp-build-$side" $args > "$loc/loadgen.txt" 2>&1 \
      || status=loadgen-failed
    grep -m1 '^LOADGEN_JSON ' "$loc/loadgen.txt" | sed 's/^LOADGEN_JSON //' > "$loc/loadgen.json" || true
    [ -s "$loc/loadgen.json" ] || status=loadgen-failed
    for b in "${BOXES[@]}"; do
      e=$(on "$b" engine "$dir"); echo "box$b after $e" >> "$loc/engine.txt"
      case "$e" in *"env=$side "*"alive=1") ;; *) status=engine-mismatch-after ;; esac
    done
  fi
  for b in "${BOXES[@]}"; do on "$b" logs "$dir" > "$loc/logs-box$b.txt" 2>&1 || true; done
  for b in "${BOXES[@]}"; do on "$b" stop "$dir" || true; done
  for b in "${BOXES[@]}"; do on "$b" wipe "$dir" || true; done
  CUR=""
  printf '%s\t%s\t%s\t%s\t%s\n' "$r" "$cell" "$side" "$lg" "$status" >> "$OUT/cells.tsv"
  log "   $name: $status$([ -s "$loc/loadgen.json" ] && python3 -c '
import json,sys; d=json.load(open(sys.argv[1]))
if d["scenario"]=="ingest":
    a=d.get("batch_ack_latency_us") or {}; print(" %.0f msg/s, ack p50 %s us p99 %s us, failed %s" % (d["throughput_msg_s"], a.get("p50"), a.get("p99"), d.get("failed_sends")))
else:
    a=d.get("ack_latency_us") or {}; print(" publish %.0f msg/s, delivered %.0f msg/s, ack p99 %s us" % (d["publish_throughput_msg_s"], d["delivered_throughput_msg_s"], a.get("p99")))' "$loc/loadgen.json")"
}

# --- rounds ----------------------------------------------------------------------
printf 'round\tcell\tside\tloadgen_box\tstatus\n' > "$OUT/cells.tsv"
for r in $(seq 1 "$ROUNDS"); do
  if [ $((r % 2)) = 1 ]; then sides="hybrid raft"; else sides="raft hybrid"; fi
  [ -z "${ONLY_SIDE:-}" ] || sides=$ONLY_SIDE
  # The generator rotates over the boxes by round; both sides of a round use the same one.
  lg=${BOXES[$(( (r - 1) % ${#BOXES[@]} ))]}
  log "round $r: $sides, loadgen on felix-build-$lg"
  for cell in $CELLS; do
    for side in $sides; do run_cell "$r" "$cell" "$side" "$lg"; done
  done
done

python3 "$here/summarize.py" "$OUT" > "$OUT/results.md"
cat "$OUT/results.md"
log "results in $OUT/results.md"
