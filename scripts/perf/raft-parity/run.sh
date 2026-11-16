#!/usr/bin/env bash
# Raft vs hybrid parity matrix on the GCP build boxes (felix-build-1..3,
# us-central1-a, internal network). One binary on both sides; FELIX_SHARD_ENGINE
# picks the engine. See docs/perf-raft-parity.md for how to read the result.
#
# Every cell, on each side, in each round gets a fresh cluster: a control plane
# (in memory, dev tokens) on box 1, brokers with their own data dirs under
# /mnt/build/raft-parity/run/, one stream, one measurement, then everything is
# stopped and wiped. Odd rounds run hybrid first, even rounds Raft first, and
# the two sides of a cell run back to back.
#
# The generator rotates over the boxes by round (use ROUNDS as a multiple of 6 so hybrid and Raft each go first equally often);
# both sides of a round use the same one. A single node cell runs its broker on
# box 2 and its generator on box 3. While it runs it holds all four build slots
# on every box, and refuses to start unless every box is quiet.
#
#   scripts/perf/raft-parity/build.sh       # once per revision
#   scripts/perf/raft-parity/run.sh         # the whole matrix
#   scripts/perf/raft-parity/run.sh --smoke # one round, one cheap cell, one box
#
# Settings (environment):
#   ROUNDS=3 (6 balances order)
#   CELLS="..."   cell ids, default all (see summarize.py); names are
#                 <single|rf3>-<oncommit|quorum|leader>-<lat|tput>, rf3-failover,
#                 rf3-kafka, rf3-mqtt, rf3-fanout, and the read cells rf3-kafka-tail,
#                 rf3-resume, rf3-follower-tail (single-resume is a one-node check of
#                 the resume loop; it is not in the default list)
#   BOXES="1 2 3" the first holds the control plane. A smoke run uses one.
#   SECS=60 LAT_SECS=30 FAIL_SECS=40 PAYLOAD=1024 BATCH=16 PUBLISHERS=8 IN_FLIGHT=4
#   LAT_PUBLISHERS=1 SHARDS=6 KEYS=$((SHARDS*8)) FANOUT=4 FANOUT_TOTAL=200000
#   KAFKA_RECORDS=1000000 MQTT_CONNS=8 MQTT_WINDOW=64
#   TAIL_SECS=30 TAIL_PUBLISHERS=4 TAIL_IN_FLIGHT=4 KAFKA_TAIL_RATE=20000   read cells: the
#   window, and a background publisher held to KAFKA_TAIL_RATE records/s (the same rate for the
#   Kafka producer and the Felix publisher) for exactly that window. A cell fails if either
#   engine publishes under 95% of it, if the publisher fails a send, or if it does not finish.
#   RESUME_SPAN=100 RESUME_WORKERS=4 TAIL_SUBS=4 RESUME_HISTORY=20000
#                 records per resume, resume loops, follower subscribers, records prefilled
#                 before a resume cell measures (offsets are drawn from them)
#   ONLY_SIDE=hybrid|raft   one engine only (a harness check, not a comparison)
#   SLOT_WAIT_SECS=3600 QUIET_WAIT_SECS=300
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
RSSH=/Volumes/External/worktrees/.build/remote-ssh
user=hello_getfelix_dev
export CLOUDSDK_PYTHON=/Volumes/External/worktrees/.build/gcloud-venv/bin/python
R=/mnt/build/raft-parity

SMOKE=0
[ "${1:-}" = --smoke ] && SMOKE=1
if [ "$SMOKE" = 1 ]; then
  : "${ROUNDS:=1}" "${CELLS:=single-oncommit-lat single-oncommit-tput}" "${SECS:=15}" "${LAT_SECS:=15}"
  : "${BOXES:=${SMOKE_BOX:-3}}"
fi
: "${BOXES:=1 2 3}" "${ROUNDS:=3}"
: "${SECS:=60}" "${LAT_SECS:=30}" "${FAIL_SECS:=40}"
: "${PAYLOAD:=1024}" "${BATCH:=16}" "${PUBLISHERS:=8}" "${IN_FLIGHT:=4}" "${LAT_PUBLISHERS:=1}" "${SHARDS:=6}"
: "${KEYS:=$((SHARDS * 8))}" "${FANOUT:=4}" "${FANOUT_TOTAL:=200000}"
: "${KAFKA_RECORDS:=1000000}" "${MQTT_CONNS:=8}" "${MQTT_WINDOW:=64}"
: "${TAIL_SECS:=30}" "${TAIL_PUBLISHERS:=4}" "${TAIL_IN_FLIGHT:=4}" "${KAFKA_TAIL_RATE:=20000}" "${RESUME_HISTORY:=20000}" "${RESUME_SPAN:=100}" "${RESUME_WORKERS:=4}" "${TAIL_SUBS:=4}"
: "${SLOT_WAIT_SECS:=3600}" "${QUIET_WAIT_SECS:=300}" "${SETTLE_SECS:=10}" "${REPL_WAIT_SECS:=180}"
if [ -z "${CELLS:-}" ]; then
  CELLS=""
  for topo in single rf3; do for mode in oncommit quorum leader; do
    CELLS="$CELLS $topo-$mode-lat $topo-$mode-tput"
  done; done
  CELLS="$CELLS rf3-failover rf3-kafka rf3-mqtt rf3-fanout rf3-kafka-tail rf3-resume rf3-follower-tail"
fi
MAX_HOLD_SECS=$(( 3600 + ROUNDS * 40 * 600 ))

RUNID=$(date -u +%Y%m%dT%H%M%SZ)$([ "$SMOKE" = 1 ] && echo -smoke || true)
OUT=${OUT:-/Volumes/External/worktrees/.scratch/raft-parity/runs/$RUNID}
mkdir -p "$OUT/cells"
exec > >(tee -a "$OUT/run.log") 2>&1 < /dev/null

read -ra BOXES <<< "$BOXES"
CP_BOX=${BOXES[0]}
log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*"; }
raw() { local b=$1; shift; "$RSSH" "$user@felix-build-$b" "$@"; }
on() { local b=$1; shift; raw "$b" "bash $R/node.sh $*"; }
IP=(); MT=()
for b in "${BOXES[@]}"; do
  read -r st ip mt < <(gcloud compute instances describe "felix-build-$b" --zone=us-central1-a \
    --format='value(status,networkInterfaces[0].networkIP,machineType.basename())')
  [ "$st" = RUNNING ] || { echo "!! felix-build-$b is not running; start it with remote.sh first" >&2; exit 1; }
  IP[$b]=$ip; MT[$b]=$mt
  # A box that just (re)started can answer ssh before its disk is mounted writable.
  ok=0
  for _ in $(seq 1 60); do
    raw "$b" "test -w /mnt/build" 2>/dev/null && { ok=1; break; }
    sleep 5
  done
  [ "$ok" = 1 ] || { echo "!! felix-build-$b: /mnt/build is not writable after 5 min" >&2; exit 1; }
done

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
case " $CELLS " in *-kafka*) for b in "${BOXES[@]}"; do on "$b" kafka-install; done ;; esac
{
  echo "runid $RUNID"
  echo "rev $(echo "$sums" | head -1)"
  echo "$sums" | tail -n +2 | sed 's/^/sha256 /'
  for b in "${BOXES[@]}"; do
    echo "box$b ${IP[$b]} ${MT[$b]} $(on "$b" sysinfo)"
  done
  echo "control plane on felix-build-$CP_BOX"
  echo "config ROUNDS=$ROUNDS CELLS=\"$(echo $CELLS)\" SECS=$SECS LAT_SECS=$LAT_SECS FAIL_SECS=$FAIL_SECS PAYLOAD=$PAYLOAD BATCH=$BATCH PUBLISHERS=$PUBLISHERS IN_FLIGHT=$IN_FLIGHT LAT_PUBLISHERS=$LAT_PUBLISHERS SHARDS=$SHARDS KEYS=$KEYS FANOUT=$FANOUT FANOUT_TOTAL=$FANOUT_TOTAL KAFKA_RECORDS=$KAFKA_RECORDS MQTT_CONNS=$MQTT_CONNS MQTT_WINDOW=$MQTT_WINDOW TAIL_SECS=$TAIL_SECS TAIL_PUBLISHERS=$TAIL_PUBLISHERS TAIL_IN_FLIGHT=$TAIL_IN_FLIGHT KAFKA_TAIL_RATE=$KAFKA_TAIL_RATE RESUME_HISTORY=$RESUME_HISTORY RESUME_SPAN=$RESUME_SPAN RESUME_WORKERS=$RESUME_WORKERS TAIL_SUBS=$TAIL_SUBS"
} | tee "$OUT/run.env"

# --- reservation -----------------------------------------------------------------
CUR=""
LGPID=""
cleanup() {
  set +e
  # Our own background ssh (by PID); `stop` below kills the remote loadgen, Kafka tool
  # and brokers through the pid files node.sh wrote, never by name.
  [ -z "$LGPID" ] || kill "$LGPID" 2>/dev/null
  if [ -n "$CUR" ]; then
    for b in "${BOXES[@]}"; do on "$b" stop "$CUR"; on "$b" wipe "$CUR"; done
  fi
  for b in "${BOXES[@]}"; do on "$b" release "$RUNID" >/dev/null 2>&1; done
  log "released the build slots"
}
trap cleanup EXIT
trap 'exit 130' INT TERM
trap 'exit 143' HUP

for b in "${BOXES[@]}"; do
  raw "$b" "setsid nohup bash $R/node.sh hold $RUNID $SLOT_WAIT_SECS $MAX_HOLD_SECS > $R/hold/$RUNID.log 2>&1 < /dev/null &"
done
log "waiting for all 4 build slots on each box (lanes' remote.sh jobs queue behind this run)"
deadline=$(( $(date +%s) + SLOT_WAIT_SECS + 60 ))
while :; do
  states=""; held=""
  for b in "${BOXES[@]}"; do states="$states $(on "$b" hold-state "$RUNID")"; held="$held held"; done
  case "$states" in *timeout*|*released*) echo "!! could not hold the slots:$states" >&2; exit 1 ;; esac
  [ "$states" = "$held" ] && break
  [ "$(date +%s)" -lt "$deadline" ] || { echo "!! slots not held in time:$states" >&2; exit 1; }
  sleep 10
done
log "slots held"

# wait_quiet <start|cell>: no TLC or builds on any box. At the start the 1 min load must
# also be under 1. Between cells the previous cluster's load is still decaying, so the
# gate there is the runnable count right now (the probe itself is one or two of them).
wait_quiet() {
  local mode=$1 deadline=$(( $(date +%s) + QUIET_WAIT_SECS )) quiet b q
  while :; do
    quiet=1; report=""
    for b in "${BOXES[@]}"; do
      q=$(on "$b" quiet); report="$report box$b:$q"
      echo "$q" | awk -F'[= ]' -v m="$mode" '{ ok = ($4 == 0) && (m == "start" ? $2 < 1 : $6 <= 3); exit !ok }' || quiet=0
    done
    [ "$quiet" = 1 ] && return 0
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "!! boxes not quiet ($mode):$report" >&2; return 3
    fi
    log "waiting for quiet ($mode):$report"; sleep 20
  done
}
wait_quiet start || exit 3
log "quiet:$report"
echo "quiet_at_start $report" >> "$OUT/run.env"

# --- one cell --------------------------------------------------------------------
# cell_shape <id>: sets TOPO CONS FSYNC KIND RF NSHARDS MOUNT STREAM_NAME
cell_shape() {
  MOUNT="" STREAM_NAME=bench NSHARDS=$SHARDS
  case "$1" in
    # The read cells run on one shard so an offset or a follower means one thing.
    single-resume|rf3-resume) TOPO=${1%%-*} CONS=Quorum FSYNC=on_commit KIND=resume NSHARDS=1 ;;
    rf3-follower-tail) TOPO=rf3 CONS=Quorum FSYNC=on_commit KIND=follower-tail NSHARDS=1 ;;
    rf3-kafka-tail) TOPO=rf3 CONS=Quorum FSYNC=on_commit KIND=kafka-tail STREAM_NAME=kbench ;;
    single-*|rf3-*-lat|rf3-*-tput) TOPO=${1%%-*}; local rest=${1#*-}; local mode=${rest%%-*} prof=${rest#*-}
      case "$mode" in
        oncommit) CONS=Quorum FSYNC=on_commit ;;
        quorum) CONS=Quorum FSYNC=periodic ;;
        leader) CONS=Leader FSYNC=periodic ;;
        *) echo "!! unknown cell $1" >&2; exit 2 ;;
      esac
      case "$prof" in lat) KIND=ingest-lat ;; tput) KIND=ingest-tput ;; *) echo "!! unknown cell $1" >&2; exit 2 ;; esac ;;
    rf3-failover) TOPO=rf3 CONS=Quorum FSYNC=on_commit KIND=failover NSHARDS=1 ;;
    rf3-kafka) TOPO=rf3 CONS=Quorum FSYNC=on_commit KIND=kafka STREAM_NAME=kbench ;;
    rf3-mqtt) TOPO=rf3 CONS=Quorum FSYNC=on_commit KIND=mqtt STREAM_NAME=mbench MOUNT="bench/" ;;
    rf3-fanout) TOPO=rf3 CONS=Quorum FSYNC=on_commit KIND=pubsub ;;
    *) echo "!! unknown cell $1" >&2; exit 2 ;;
  esac
  if [ "$TOPO" = single ]; then RF=1; else RF=3; fi
}

pick() { # <n> <fallback>: the n-th box of BOXES, or the fallback box
  local n=$1; echo "${BOXES[$n]:-${BOXES[0]}}"
}

# run_cell <round> <cell> <side>
run_cell() {
  local r=$1 cell=$2 side=$3 name dir loc b i st e args limit lg lead killed="" bb brokers=()
  name="r$r-$cell-$side"; dir="$R/run/$RUNID/$name"; loc="$OUT/cells/$name"; mkdir -p "$loc"
  cell_shape "$cell"
  CUR=$dir
  if [ "$TOPO" = single ]; then
    brokers=("$(pick 1)"); lg=$(pick 2)
  else
    brokers=("${BOXES[@]}"); lg=${BOXES[$(( (r - 1) % ${#BOXES[@]} ))]}
  fi
  log "== $name: $KIND, $CONS, fsync $FSYNC, rf $RF, brokers on ${brokers[*]}, loadgen on felix-build-$lg"
  for b in "${BOXES[@]}"; do on "$b" touch "$RUNID"; done
  wait_quiet cell || exit 3
  echo "$report" > "$loc/quiet.txt"
  status=ok
  { on "$CP_BOX" cp "$dir" && on "$CP_BOX" seed "$dir" 3; } > "$loc/setup.txt" 2>&1 || status=setup-failed
  if [ "$status" = ok ]; then
    for b in "${brokers[@]}"; do
      raw "$CP_BOX" "cat $dir/tokens/node-$b.jwt" | on "$b" put "$dir/node.jwt"
      raw "$CP_BOX" "cat $dir/tokens/node-$b.refresh" | on "$b" put "$dir/node.refresh"
    done
    [ "$lg" = "$CP_BOX" ] || raw "$CP_BOX" "cat $dir/tokens/client.jwt" | on "$lg" put "$dir/tokens/client.jwt"
    local pids=()
    for b in "${brokers[@]}"; do
      on "$b" broker "$dir" "$b" "${IP[$CP_BOX]}" "$side" "$FSYNC" > "$loc/start-box$b.txt" 2>&1 &
      pids+=($!)
    done
    for i in "${pids[@]}"; do wait "$i" || status=broker-start-failed; done
  fi
  if [ "$status" = ok ]; then
    on "$CP_BOX" stream "$dir" "$STREAM_NAME" "$NSHARDS" "$CONS" "$RF" "$MOUNT" >> "$loc/setup.txt" 2>&1 || status=stream-failed
  fi
  if [ "$status" = ok ]; then
    status=not-replicated
    for ((i = 0; i < REPL_WAIT_SECS; i += 3)); do
      st=$(on "$CP_BOX" repl "$dir" 2>/dev/null || true)
      case "$st" in "under=0 shards=$NSHARDS "*) status=ok; break ;; esac
      sleep 3
    done
    echo "$st" > "$loc/replication.txt"
  fi
  # The engine check: the broker's own environment, and the Raft marker that only
  # a Raft member writes into its shard directory.
  if [ "$status" = ok ]; then
    for b in "${brokers[@]}"; do
      e=$(on "$b" engine "$dir"); echo "box$b before $e" >> "$loc/engine.txt"
      case "$side:$e" in
        hybrid:"env=hybrid raft_dirs=0 alive=1") ;;
        raft:"env=raft raft_dirs="*" alive=1")
          n=${e#*raft_dirs=}; n=${n%% *}
          [ "$n" -ge "$NSHARDS" ] || status=engine-mismatch ;;
        *) status=engine-mismatch ;;
      esac
    done
  fi
  if [ "$status" = ok ]; then
    sleep "$SETTLE_SECS"
    local lgbrokers="" kafka_boot="" mqtt_addrs=""
    for b in "${brokers[@]}"; do
      lgbrokers="$lgbrokers,${IP[$b]}:15000"; kafka_boot="$kafka_boot,${IP[$b]}:19092"; mqtt_addrs="$mqtt_addrs,${IP[$b]}:11883"
    done
    lgbrokers=${lgbrokers#,}; kafka_boot=${kafka_boot#,}; mqtt_addrs=${mqtt_addrs#,}
    local common="--brokers $lgbrokers --environment gcp-build-$side --stream $STREAM_NAME"
    case "$KIND" in
      ingest-tput)
        args="--scenario ingest --payload-bytes $PAYLOAD --batch $BATCH --concurrency $PUBLISHERS --in-flight $IN_FLIGHT --duration-secs $SECS --keys $KEYS --shards $NSHARDS --cluster"
        limit=$((SECS + 240)) ;;
      ingest-lat)
        args="--scenario ingest --payload-bytes $PAYLOAD --batch 1 --concurrency $LAT_PUBLISHERS --in-flight 1 --duration-secs $LAT_SECS --keys $KEYS --shards $NSHARDS --cluster"
        limit=$((LAT_SECS + 240)) ;;
      pubsub)
        args="--scenario pubsub --payload-bytes $PAYLOAD --batch $BATCH --fanout $FANOUT --warmup 2000 --total $FANOUT_TOTAL"
        limit=900 ;;
      mqtt)
        args="--scenario mqtt-publish --payload-bytes $PAYLOAD --concurrency $MQTT_CONNS --in-flight $MQTT_WINDOW --duration-secs $SECS --mqtt-addrs $mqtt_addrs --mqtt-topic-prefix bench/"
        limit=$((SECS + 240)) ;;
      resume|follower-tail)
        # Placement: the loadgen sits on the box that is neither the shard leader nor the
        # target follower, on both engines. A fixed-rate publisher runs beside the reader for
        # exactly the reader's window.
        lead="" tgt="" fb=$lgbrokers
        if [ "$TOPO" = single ]; then
          lead=${brokers[0]} tgt=$lead
        else
          lead=$(on "$CP_BOX" repl "$dir" | sed -n 's/.*leaders=broker-\([0-9]*\):.*/\1/p')
          [ -n "$lead" ] || status=no-leader
          [ "${#brokers[@]}" -ge 3 ] || status=needs-3-boxes
          if [ "$status" = ok ]; then
            for b in "${brokers[@]}"; do [ "$b" != "$lead" ] && { tgt=$b; break; }; done
            for b in "${brokers[@]}"; do [ "$b" != "$lead" ] && [ "$b" != "$tgt" ] && { lg=$b; break; }; done
            raw "$CP_BOX" "cat $dir/tokens/client.jwt" | on "$lg" put "$dir/tokens/client.jwt"
          fi
        fi
        stamp=""
        if [ "$KIND" = follower-tail ]; then
          fb="${IP[$tgt]}:15000"
          args="--scenario subscribe --direct --fanout $TAIL_SUBS --concurrency $TAIL_SUBS --duration-secs $TAIL_SECS --stamp-send-time"
          stamp=--stamp-send-time
        else
          args="--scenario resume --concurrency $RESUME_WORKERS --resume-span $RESUME_SPAN --resume-history $RESUME_HISTORY --duration-secs $TAIL_SECS"
        fi
        fg_common="--brokers $fb --environment gcp-build-$side --stream $STREAM_NAME"
        bg_args="--scenario ingest --payload-bytes $PAYLOAD --batch $BATCH --concurrency $TAIL_PUBLISHERS --in-flight $TAIL_IN_FLIGHT --rate $KAFKA_TAIL_RATE --duration-secs $TAIL_SECS --keys 8 --shards 1 --cluster $stamp"
        limit=$((TAIL_SECS + 240)) ;;
      kafka-tail)
        limit=$((TAIL_SECS + 600)) ;;
      failover)
        # The leader's box is the one to kill; the generator runs on another so it survives.
        lead=$(on "$CP_BOX" repl "$dir" | sed -n 's/.*leaders=broker-\([0-9]*\):.*/\1/p')
        [ -n "$lead" ] || status=no-leader
        for b in "${BOXES[@]}"; do [ "$b" != "$lead" ] && lg=$b && break; done
        raw "$CP_BOX" "cat $dir/tokens/client.jwt" | on "$lg" put "$dir/tokens/client.jwt"
        args="--scenario failover --payload-bytes 256 --duration-secs $FAIL_SECS"
        limit=$((FAIL_SECS + 240)) ;;
    esac
  fi
  if [ "$status" = ok ]; then
    if [ "$KIND" = kafka ]; then
      on "$lg" kafka-install > "$loc/kafka-install.txt" 2>&1 || true
      on "$lg" kafka "$dir" 1800 "$kafka_boot" "default.$STREAM_NAME" "$KAFKA_RECORDS" "$PAYLOAD" > "$loc/loadgen.txt" 2>&1 \
        || status=loadgen-failed
    elif [ "$KIND" = kafka-tail ]; then
      on "$lg" kafka-install > "$loc/kafka-install.txt" 2>&1 || true
      on "$lg" kafka-tail "$dir" "$limit" "$kafka_boot" "default.$STREAM_NAME" "$((KAFKA_TAIL_RATE * TAIL_SECS))" "$PAYLOAD" "$KAFKA_TAIL_RATE" > "$loc/loadgen.txt" 2>&1 \
        || status=loadgen-failed
    elif [ "$KIND" = resume ] || [ "$KIND" = follower-tail ]; then
      if [ "$KIND" = resume ]; then
        # The history the resumes draw from is written first and is the same size on both
        # engines, so the offsets do not depend on how fast the publisher ran.
        on "$lg" loadgen "$dir" 600 $common --scenario ingest --payload-bytes $PAYLOAD --batch $BATCH \
          --concurrency 1 --in-flight 1 --total $RESUME_HISTORY --keys 8 --shards 1 --cluster \
          > "$loc/prefill.txt" 2>&1 || status=prefill-failed
      fi
      if [ "$status" = ok ]; then
        start=$(raw "$lg" "date +%s.%N"); start=$(python3 -c "print(float('$start') + 15)")
        on "$lg" loadgen-bg "$dir" ingest $((TAIL_SECS + 240)) $common $bg_args --start-at "$start" > "$loc/ingest-start.txt" 2>&1 || status=loadgen-failed
        on "$lg" loadgen "$dir" "$limit" $fg_common $args --start-at "$start" > "$loc/loadgen.txt" 2>&1 || status=loadgen-failed
        # The publisher ends with the window. Wait for it, so its rate is read and not
        # guessed at and nothing carries into the next cell.
        on "$lg" wait-json "$dir" ingest 90 > "$loc/ingest-wait.txt" 2>&1 || status=publisher-failed
        raw "$lg" "cat $dir/ingest.log" > "$loc/ingest.txt" 2>&1 || status=publisher-failed
      fi
    elif [ "$KIND" = failover ]; then
      start=$(raw "$lg" "date +%s.%N"); start=$(python3 -c "print(float('$start') + 15)")
      on "$lg" loadgen "$dir" "$limit" $common $args --start-at "$start" --kill-file "$dir/kill.at" > "$loc/loadgen.txt" 2>&1 &
      LGPID=$!
      sleep 30   # 15 s to the start, 15 s into the run
      # The kill instant comes from the killed box right before the signal; the loadgen
      # measures from it, so a stall earlier in the run is not counted.
      kt=$(on "$lead" kill-broker "$dir") || status=kill-failed
      killed=$lead
      echo "$kt" | on "$lg" put "$dir/kill.at"
      wait "$LGPID" || status=loadgen-failed
      LGPID=""
    else
      on "$lg" loadgen "$dir" "$limit" $common $args > "$loc/loadgen.txt" 2>&1 || status=loadgen-failed
    fi
    grep -m1 '^LOADGEN_JSON ' "$loc/loadgen.txt" | sed 's/^LOADGEN_JSON //' > "$loc/loadgen.json" || true
    [ -s "$loc/loadgen.json" ] || { [ "$status" != ok ] || status=loadgen-failed; }
    # The background publisher is part of the measurement: it must have finished, sent
    # nothing that failed, and held 95% of the target rate. Otherwise the two engines were
    # not read under the same load and the cell is invalid, not a pass.
    if [ "$status" = ok ] && { [ "$KIND" = resume ] || [ "$KIND" = follower-tail ]; }; then
      python3 - "$loc/loadgen.json" "$loc/ingest.txt" "$KAFKA_TAIL_RATE" "$lead" "$tgt" "$lg" <<'PY' || status=publisher-invalid
import json, sys
path, ingest, target, lead, tgt, lg = sys.argv[1:7]
d = json.load(open(path))
lines = [l for l in open(ingest) if l.startswith("LOADGEN_JSON ")]
if not lines:
    sys.exit("publisher left no LOADGEN_JSON")
p = json.loads(lines[0].split(" ", 1)[1])
rate, target = p.get("throughput_msg_s"), float(target)
if p.get("failed_sends"):
    sys.exit("publisher failed %s sends" % p["failed_sends"])
if rate is None or rate < 0.95 * target:
    sys.exit("publisher held %s of %s msg/s" % (rate, target))
d.update({"publish_throughput_msg_s": rate, "publish_target_msg_s": target,
          "publish_failed_sends": p.get("failed_sends", 0),
          "placement": {"leader_box": lead, "target_box": tgt, "loadgen_box": lg}})
json.dump(d, open(path, "w"))
PY
    fi
    if [ "$status" = ok ] && [ "$KIND" = kafka-tail ]; then
      python3 - "$loc/loadgen.json" <<'PY' || status=kafka-tail-invalid
import json, sys
d = json.load(open(sys.argv[1]))
if d["consumed_records"] < d["produced_records"]:
    sys.exit("consumed %s of %s records" % (d["consumed_records"], d["produced_records"]))
if d["publish_throughput_msg_s"] < 0.95 * d["publish_target_msg_s"]:
    sys.exit("producer held %s of %s msg/s" % (d["publish_throughput_msg_s"], d["publish_target_msg_s"]))
PY
    fi
    if [ "$KIND" = failover ] && [ -s "$loc/loadgen.json" ]; then
      python3 -c '
import json, sys
d = json.load(open(sys.argv[1])); dur = float(sys.argv[2])
sys.exit(0 if 3 < d["kill_at_s"] < dur - 5 else 1)' "$loc/loadgen.json" "$FAIL_SECS" || status=kill-missed
    fi
    for b in "${brokers[@]}"; do
      [ "$b" = "$killed" ] && continue
      e=$(on "$b" engine "$dir"); echo "box$b after $e" >> "$loc/engine.txt"
      case "$e" in *"env=$side "*"alive=1") ;; *) status=engine-mismatch-after ;; esac
    done
  fi
  for b in "${BOXES[@]}"; do on "$b" logs "$dir" > "$loc/logs-box$b.txt" 2>&1 || true; done
  for b in "${BOXES[@]}"; do on "$b" stop "$dir" || true; done
  for b in "${BOXES[@]}"; do on "$b" wipe "$dir" || true; done
  CUR=""
  # A cell that did not measure leaves no JSON for the summary to read.
  [ "$status" = ok ] || rm -f "$loc/loadgen.json"
  printf '%s\t%s\t%s\t%s\t%s\n' "$r" "$cell" "$side" "$lg" "$status" >> "$OUT/cells.tsv"
  log "   $name: $status"
}

# --- rounds ----------------------------------------------------------------------
printf 'round\tcell\tside\tloadgen_box\tstatus\n' > "$OUT/cells.tsv"
for r in $(seq 1 "$ROUNDS"); do
  if [ $((r % 2)) = 1 ]; then sides="hybrid raft"; else sides="raft hybrid"; fi
  [ -z "${ONLY_SIDE:-}" ] || sides=$ONLY_SIDE
  log "round $r: $sides"
  for cell in $CELLS; do
    for side in $sides; do run_cell "$r" "$cell" "$side"; done
  done
done

rc=0
python3 "$here/summarize.py" "$OUT" ${STRICT:+--strict} > /dev/null || rc=$?
cat "$OUT/results.md"
log "results in $OUT/results.md and results.json"
exit $rc
