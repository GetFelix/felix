#!/usr/bin/env bash
# The on-box half of the Raft-vs-hybrid parity matrix. run.sh copies it to
# /mnt/build/raft-parity/node.sh on each box and calls one subcommand per ssh.
# Everything it creates or deletes is under /mnt/build/raft-parity, and it only
# ever signals PIDs it wrote to a pid file itself.
set -euo pipefail

R=/mnt/build/raft-parity
# PARITY_BIN names another install (build.sh BIN_NAME), so a profiling build sits beside the matrix one.
BIN=$R/${PARITY_BIN:-bin}
# The release baseline's broker and control plane (build.sh BASELINE_REF); its loadgen is a
# copy of the one in $BIN, which every side uses.
BASE_BIN=$R/bin-baseline
TENANT=perf
NS=default
# Fixed ports, away from anything the lanes' tests bind (they pick ephemeral ports).
CP_PORT=18443 CP_METRICS=18080 CP_BOOT=18095
QUIC_PORT=15000 INTERNAL_PORT=17000 METRICS_PORT=19100 KAFKA_PORT=19092 MQTT_PORT=11883
KAFKA_VER=3.9.1
BOOT_TOKEN="raft-parity-bootstrap-only"

die() { echo "!! $*" >&2; exit 1; }
# Refuse any path outside the run tree, so a bad argument can never wipe more.
in_run() { case "$1" in "$R"/run/?*) ;; *) die "refusing path outside $R/run: $1" ;; esac; }
# A pid file can outlive a reboot; never signal a reused pid that is plainly not ours.
safe_pid() { case "$(cat "/proc/$1/comm" 2>/dev/null)" in sshd|systemd|init|"") return 1 ;; esac; }
ip_self() { hostname -I | awk '{print $1}'; }

# start <dir> <name> <cmd...>: detached, output to <dir>/<name>.log, pid to <dir>/<name>.pid.
start() {
  local dir=$1 name=$2; shift 2
  setsid nohup "$@" > "$dir/$name.log" 2>&1 < /dev/null &
  echo $! > "$dir/$name.pid"
}

wait_http() { # <url> <secs>
  local i
  for ((i = 0; i < $2; i++)); do curl -fsS -o /dev/null "$1" 2>/dev/null && return 0; sleep 1; done
  return 1
}

# post <url> <body> <header> -> sets code, body in $dir/resp.json
post() {
  code=$(curl -s -o "$RESP" -w '%{http_code}' -X POST "$1" -H 'Content-Type: application/json' -H "$3" -d "$2")
}
field() { python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$RESP" "$1"; }

cmd=${1:?subcommand}; shift
case "$cmd" in

  # hold <runid> <wait-secs> <max-secs>: take every build slot on this box and
  # keep them until $R/hold/<runid> is removed, goes stale (run.sh touches it at
  # every cell; 20 min without a touch means run.sh died), or <max-secs> pass.
  # Run detached; progress is in $R/hold/<runid>.state.
  hold)
    id=$1 wait=$2 max=$3
    mkdir -p "$R/hold"; f=$R/hold/$id; st=$f.state
    echo waiting > "$st"; touch "$f"
    exec 21>/mnt/build/slot-1 22>/mnt/build/slot-2 23>/mnt/build/slot-3 24>/mnt/build/slot-4
    for fd in 21 22 23 24; do
      flock -w "$wait" "$fd" || { echo timeout > "$st"; rm -f "$f"; exit 1; }
    done
    echo held > "$st"; touch "$f"
    end=$(( $(date +%s) + max ))
    while [ -f "$f" ] && [ "$(date +%s)" -lt "$end" ] \
        && [ $(( $(date +%s) - $(stat -c %Y "$f") )) -lt 1200 ]; do
      sleep 5
    done
    echo released > "$st"; rm -f "$f"
    ;;
  hold-state) cat "$R/hold/$1.state" 2>/dev/null || echo none ;;
  touch) [ -f "$R/hold/$1" ] && touch "$R/hold/$1" || die "hold $1 is gone" ;;
  release) rm -f "$R/hold/$1" ;;

  # quiet: one line, load=<1 min load> java=<java processes>.
  # quiet: 1 min load, foreign heavy processes (TLC, builds), and the runnable count right now.
  quiet) echo "load=$(cut -d' ' -f1 /proc/loadavg) busy=$(pgrep -c -x 'java|cargo|rustc|cc1|cc1plus|ld' || true) run=$(awk '/^procs_running/{print $2}' /proc/stat)" ;;

  # cp <dir> [side]: the control plane, in memory, bootstrap with dev tokens on loopback.
  # The baseline side runs the baseline's own control plane, so its broker meets its own release.
  cp)
    dir=$1 side=${2:-}; in_run "$dir"; mkdir -p "$dir"; ip=$(ip_self)
    # The control plane records the engine and brokers refuse to join one that names another.
    cpbin=$BIN/felix-controlplane; engine_env=(FELIX_SHARD_ENGINE="${side:-hybrid}")
    if [ "$side" = baseline ]; then cpbin=$BASE_BIN/felix-controlplane; engine_env=(); fi
    start "$dir" cp env \
      "${engine_env[@]}" \
      FELIX_CONTROLPLANE_STORAGE_BACKEND=memory \
      FELIX_CONTROLPLANE_BIND="$ip:$CP_PORT" FELIX_CONTROLPLANE_METRICS_BIND="$ip:$CP_METRICS" \
      FELIX_REGION_ID=gcp FELIX_BOOTSTRAP_ENABLED=true FELIX_BOOTSTRAP_BIND_ADDR="127.0.0.1:$CP_BOOT" \
      FELIX_BOOTSTRAP_TOKEN=$BOOT_TOKEN FELIX_BOOTSTRAP_DEV_TOKENS=true \
      FELIX_EXCHANGE_TOKEN_TTL_SECONDS=86400 RUST_LOG=info \
      "$cpbin"
    wait_http "http://$ip:$CP_METRICS/ready" 60 || { tail -20 "$dir/cp.log" >&2; die "control plane not ready"; }
    ;;

  # seed <dir> <brokers>: tenant, namespace and tokens (node-<i>.jwt, admin.jwt, client.jwt) in <dir>/tokens.
  seed)
    dir=$1 n=$2; in_run "$dir"; RESP=$dir/resp.json; tok=$dir/tokens; mkdir -p "$tok"
    boot="http://127.0.0.1:$CP_BOOT/internal/bootstrap/tenants/$TENANT"
    post "$boot/initialize" '{"display_name":"perf","idp_issuers":[],
      "initial_admin_principals":["p:admin"],
      "policies":[
        {"subject":"role:broker","object":"cluster:*","action":"node.manage"},
        {"subject":"role:broker","object":"cluster:*","action":"node.view"},
        {"subject":"role:admin","object":"cluster:*","action":"node.manage"},
        {"subject":"role:admin","object":"cluster:*","action":"node.view"},
        {"subject":"role:admin","object":"namespace:perf/*","action":"ns.manage"},
        {"subject":"role:client","object":"stream:perf/default/*","action":"stream.publish"},
        {"subject":"role:client","object":"stream:perf/default/*","action":"stream.subscribe"}],
      "groupings":[{"user":"p:admin","role":"role:admin"},{"user":"p:client","role":"role:client"},
        {"user":"p:broker-1","role":"role:broker"},{"user":"p:broker-2","role":"role:broker"},
        {"user":"p:broker-3","role":"role:broker"}]}' "X-Felix-Bootstrap-Token: $BOOT_TOKEN"
    [ "$code" = 200 ] || { cat "$RESP" >&2; die "initialize -> $code"; }
    mint() { # <out> <json>
      post "$boot/dev-token" "$2" "X-Felix-Bootstrap-Token: $BOOT_TOKEN"
      [ "$code" = 200 ] || { cat "$RESP" >&2; die "dev-token $1 -> $code"; }
      field felix_token > "$tok/$1.jwt"; field refresh_token > "$tok/$1.refresh"
    }
    for i in $(seq 1 "$n"); do mint "node-$i" "{\"principal\":\"p:broker-$i\",\"audience\":\"felix-controlplane\"}"; done
    mint admin '{"principal":"p:admin","audience":"felix-controlplane"}'
    mint client '{"principal":"p:client","audience":"felix-broker","requested":["stream.publish","stream.subscribe"]}'
    post "http://$(ip_self):$CP_PORT/v1/tenants/$TENANT/namespaces" '{"namespace":"default","display_name":"default"}' \
      "Authorization: Bearer $(cat "$tok/admin.jwt")"
    case "$code" in 2*) ;; *) cat "$RESP" >&2; die "namespace -> $code" ;; esac
    ;;

  # put <path>: write stdin to <path> (tokens handed between boxes).
  put) in_run "$1"; mkdir -p "$(dirname "$1")"; umask 077; cat > "$1" ;;

  # broker <dir> <index> <cp-ip> <engine> <fsync>: one broker, waits for /ready.
  # Engine "baseline" is the release baseline: its own binary, hybrid by construction, and
  # no FELIX_SHARD_ENGINE (v0.7.2 reads no such variable and has no other engine).
  broker)
    dir=$1 i=$2 cpip=$3 engine=$4 fsync=$5; in_run "$dir"; ip=$(ip_self)
    mkdir -p "$dir/data"
    brokerbin=$BIN/felix-broker; engine_env=(FELIX_SHARD_ENGINE="$engine")
    if [ "$engine" = baseline ]; then brokerbin=$BASE_BIN/felix-broker; engine_env=(); fi
    start "$dir" broker env \
      "${engine_env[@]}" FELIX_DURABLE_FSYNC_MODE="$fsync" \
      FELIX_NODE_ID="broker-$i" FELIX_REGION_ID=gcp \
      FELIX_CONTROLPLANE_URL="http://$cpip:$CP_PORT" \
      FELIX_QUIC_BIND="$ip:$QUIC_PORT" FELIX_CLIENT_ADVERTISE_ADDR="$ip:$QUIC_PORT" \
      FELIX_INTERNAL_BIND="$ip:$INTERNAL_PORT" FELIX_NODE_ADVERTISE_ADDR="$ip:$INTERNAL_PORT" \
      FELIX_BROKER_METRICS_BIND="$ip:$METRICS_PORT" FELIX_DURABLE_STORAGE_DIR="$dir/data" \
      FELIX_NODE_TOKEN_FILE="$dir/node.jwt" FELIX_NODE_REFRESH_TOKEN_FILE="$dir/node.refresh" \
      FELIX_KAFKA_LISTEN="$ip:$KAFKA_PORT" FELIX_KAFKA_TLS=false FELIX_KAFKA_ANONYMOUS_TENANT=$TENANT \
      FELIX_MQTT_LISTEN="$ip:$MQTT_PORT" FELIX_MQTT_TLS=false FELIX_MQTT_ANONYMOUS_TENANT=$TENANT \
      FELIX_INTERNAL_ALLOW_UNAUTHENTICATED=true FELIX_MTU_UPPER_BOUND=4096 RUST_LOG=info \
      "$brokerbin"
    wait_http "http://$ip:$METRICS_PORT/ready" 120 || { tail -30 "$dir/broker.log" >&2; die "broker-$i not ready"; }
    ;;

  # stream <dir> <name> <shards> <Leader|Quorum> <rf> [mount]: create a durable stream.
  stream)
    dir=$1; in_run "$dir"; RESP=$dir/resp.json
    mount=""; [ -z "${6:-}" ] || mount=",\"mount\":\"$6\""
    post "http://$(ip_self):$CP_PORT/v1/tenants/$TENANT/namespaces/$NS/streams" \
      "{\"stream\":\"$2\",\"kind\":\"Stream\",\"shards\":$3,\"replication_factor\":$5,\"retention\":{\"max_age_seconds\":null,\"max_size_bytes\":null},\"consistency\":\"$4\",\"delivery\":\"AtLeastOnce\",\"durable\":true$mount}" \
      "Authorization: Bearer $(cat "$dir/tokens/admin.jwt")"
    case "$code" in 2*) ;; *) cat "$RESP" >&2; die "create stream -> $code" ;; esac
    ;;

  # repl <dir>: "under=<n> shards=<n> leaders=<node>:<n>,..." from the control plane.
  repl)
    dir=$1; in_run "$dir"
    curl -fsS -H "Authorization: Bearer $(cat "$dir/tokens/admin.jwt")" \
      "http://$(ip_self):$CP_PORT/v1/placement/replication" | python3 -c '
import collections, json, sys
d = json.load(sys.stdin); items = d.get("items", [])
c = collections.Counter((it.get("leader") or "none") for it in items)
print("under=%d shards=%d leaders=%s" % (d.get("under_replicated", 0), len(items),
      ",".join("%s:%d" % kv for kv in sorted(c.items()))))'
    ;;

  # engine <dir>: what this broker actually runs. env= is the process environment,
  # raft_dirs= the shard directories holding the Raft engine marker (felix-storage
  # engine_marker.rs; only Raft writes one), segments= shard directories with data.
  engine)
    dir=$1; in_run "$dir"; pid=$(cat "$dir/broker.pid")
    env=$(tr '\0' '\n' < "/proc/$pid/environ" | sed -n 's/^FELIX_SHARD_ENGINE=//p')
    raft=$(find "$dir/data" -type f -name engine -exec grep -lx raft {} + 2>/dev/null | wc -l)
    echo "env=$env raft_dirs=$raft alive=$(kill -0 "$pid" 2>/dev/null && echo 1 || echo 0)"
    ;;

  # cpu <dir>: "ticks=<utime+stime> hz=<ticks a second>" of this box's broker, read from
  # /proc/<pid>/stat. Two reads bracket a window; the difference is the broker's CPU time.
  # The command name is skipped by cutting at the last ")" since it can hold spaces.
  cpu)
    dir=$1; in_run "$dir"; pid=$(cat "$dir/broker.pid")
    python3 - "$pid" "$(getconf CLK_TCK)" <<'PY'
import sys
stat = open("/proc/%s/stat" % sys.argv[1]).read()
f = stat[stat.rindex(")") + 2:].split()
# After the command name the fields start at 3 (state): utime and stime are 14 and 15.
print("ticks=%d hz=%s" % (int(f[11]) + int(f[12]), sys.argv[2]))
PY
    ;;

  # perf-start <dir>: sample this box's broker with `perf record -g` (run.sh PROFILE=1) until
  # perf-stop. Needs kernel.perf_event_paranoid <= 1 and a broker built with frame pointers
  # (build.sh FRAME_POINTERS=1), or the stacks stop at the first frame.
  perf-start)
    # perf-start <dir> [record 1|0] [extra events] [unix-secs]: from that wall-clock time when given.
    dir=$1 record=${2:-1} extra=${3:-} at=${4:-0}; in_run "$dir"
    pid=$(cat "$dir/broker.pid")
    delay='now=$(date +%s); [ "$0" -le "$now" ] || sleep $(( $0 - now )); exec "$@"'
    # System wide, filtered to the broker when folded: following a pid misses threads a pool
    # starts after the recording does.
    [ "$record" = 0 ] || start "$dir" perf bash -c "$delay" "$at" perf record -a -F 499 -g -o "$dir/perf.data"
    # The broker's own syscalls and switches. sched_switch with prev_state 0 is a preempted
    # (involuntary) switch; context-switches minus it are voluntary.
    ev='syscalls:sys_enter_futex,syscalls:sys_enter_sendmsg,syscalls:sys_enter_sendmmsg,syscalls:sys_enter_recvmsg'
    ev="$ev,syscalls:sys_enter_recvmmsg,syscalls:sys_enter_sendto,syscalls:sys_enter_recvfrom,syscalls:sys_enter_fsync"
    ev="$ev,syscalls:sys_enter_fdatasync,syscalls:sys_enter_sync_file_range,syscalls:sys_enter_epoll_wait"
    ev="$ev,syscalls:sys_enter_io_uring_enter,syscalls:sys_enter_pwrite64,syscalls:sys_enter_pwritev"
    ev="$ev,syscalls:sys_enter_write,syscalls:sys_enter_writev,context-switches,cpu-migrations"
    start "$dir" perfstat bash -c "$delay" "$at" perf stat -p "$pid" -x, -o "$dir/syscalls.csv" -e "$ev" \
      -e sched:sched_switch --filter 'prev_state == 0' ${extra:+-e "$extra"}
    ;;

  # perf-stop <dir>: end the recording and print its stacks folded (profile.py fold).
  perf-stop)
    dir=$1; in_run "$dir"
    for f in perf perfstat; do
      [ -f "$dir/$f.pid" ] || continue
      pid=$(cat "$dir/$f.pid")
      safe_pid "$pid" && kill -INT "$pid" 2>/dev/null || true
      for _ in $(seq 1 60); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
      rm -f "$dir/$f.pid"
    done
    [ -f "$dir/perf.data" ] || exit 0
    perf script -i "$dir/perf.data" --pid "$(cat "$dir/broker.pid")" 2>/dev/null | python3 "$R/profile.py" fold
    rm -f "$dir/perf.data"
    ;;

  # perf-syscalls <dir>: the syscall counts perf-start recorded (perf stat CSV).
  perf-syscalls) in_run "$1"; cat "$1/syscalls.csv" ;;

  # cpu-at <dir> <unix-secs>: wait for that wall-clock time, then the same as cpu. Lets a
  # window that starts at --start-at be opened at its start and not at the connect.
  cpu-at)
    dir=$1 at=$2; in_run "$dir"
    python3 -c 'import sys, time; time.sleep(max(0.0, float(sys.argv[1]) - time.time()))' "$at"
    exec bash "$R/node.sh" cpu "$dir"
    ;;

  # loadgen <dir> <timeout-secs> <args...>: felix-loadgen with the client token; prints its output.
  loadgen)
    dir=$1 limit=$2; shift 2; in_run "$dir"
    # exec keeps the pid file pointing at the process itself, so `stop` reaches it.
    bash -c 'echo $$ > "$0/loadgen.pid"; exec timeout -k 10 "$1" "$2" --tenant "$3" --token-file "$0/tokens/client.jwt" "${@:4}"' \
      "$dir" "$limit" "$BIN/felix-loadgen" "$TENANT" "$@" 2>&1 | tee "$dir/loadgen.log"
    ;;

  # loadgen-bg <dir> <name> <timeout-secs> <args...>: the same loadgen, detached, for
  # a publisher that runs beside a measuring one. Output in <dir>/<name>.log.
  loadgen-bg)
    dir=$1 name=$2 limit=$3; shift 3; in_run "$dir"
    start "$dir" "$name" timeout -k 10 "$limit" "$BIN/felix-loadgen" --tenant "$TENANT" \
      --token-file "$dir/tokens/client.jwt" "$@"
    ;;

  # wait-json <dir> <name> <timeout-secs>: wait for a loadgen-bg run to print its
  # LOADGEN_JSON and exit, so no publisher outlives its cell. Fails on timeout.
  wait-json)
    dir=$1 name=$2 limit=$3; in_run "$dir"
    for _ in $(seq 1 "$limit"); do
      if ! kill -0 "$(cat "$dir/$name.pid")" 2>/dev/null; then
        grep -q '^LOADGEN_JSON ' "$dir/$name.log" && exit 0
        die "$name exited without LOADGEN_JSON"
      fi
      sleep 1
    done
    die "$name still running after $limit s"
    ;;

  # stop <dir>: TERM then KILL the processes this script started under <dir>.
  stop)
    dir=$1; in_run "$dir"
    for p in "$dir"/*.pid; do
      [ -f "$p" ] || continue; pid=$(cat "$p")
      safe_pid "$pid" || { rm -f "$p"; continue; }
      kill "$pid" 2>/dev/null || true
    done
    for _ in $(seq 1 20); do
      alive=0
      for p in "$dir"/*.pid; do [ -f "$p" ] && kill -0 "$(cat "$p")" 2>/dev/null && alive=1; done
      [ $alive = 0 ] && break; sleep 0.5
    done
    for p in "$dir"/*.pid; do [ -f "$p" ] && safe_pid "$(cat "$p")" && kill -9 "$(cat "$p")" 2>/dev/null || true; done
    ;;

  # logs <dir>: the lines worth keeping from a cell (errors, warnings, the tails).
  logs)
    dir=$1; in_run "$dir"
    for l in "$dir"/*.log; do
      [ -f "$l" ] || continue
      echo "== $(basename "$l") ($(wc -l < "$l") lines; ERROR/WARN lines and tail)"
      grep -E ' (ERROR|WARN) ' "$l" | tail -40 || true
      echo "-- tail"; tail -15 "$l"
    done
    ;;

  # kill-broker <dir>: SIGKILL the broker this script started, as a crashed leader.
  kill-broker)
    dir=$1; in_run "$dir"
    date +%s.%N   # the kill instant, on this box's clock, just before the signal
    kill -9 "$(cat "$dir/broker.pid")"
    ;;

  # sysinfo: the machine the numbers came from.
  sysinfo)
    echo "host=$(hostname) cpus=$(nproc) mem_kb=$(awk '/MemTotal/{print $2}' /proc/meminfo)" \
      "kernel=$(uname -r) cpu=\"$(awk -F': ' '/model name/{print $2; exit}' /proc/cpuinfo)\"" \
      "disk=\"$(df -h --output=source,size /mnt/build | tail -1 | xargs)\""
    ;;

  # kafka-install: Kafka's own perf tools, once per box. The tarball is cached outside $R
  # (archive.apache.org serves ~200 KB/s); run.sh relays it to boxes that lack it.
  kafka-install)
    command -v java >/dev/null || die "java is not installed"
    [ -x "$R/kafka/bin/kafka-producer-perf-test.sh" ] && exit 0
    tgz=/mnt/build/kafka-cache/kafka_2.13-$KAFKA_VER.tgz
    if [ ! -s "$tgz" ]; then
      mkdir -p "$(dirname "$tgz")"
      curl --proto "=https" --tlsv1.2 -fsSL -o "$tgz.part" "https://archive.apache.org/dist/kafka/$KAFKA_VER/kafka_2.13-$KAFKA_VER.tgz"
      mv "$tgz.part" "$tgz"
    fi
    mkdir -p "$R/kafka-dl"; cd "$R/kafka-dl"
    tar xzf "$tgz"; rm -rf "$R/kafka"; mv "kafka_2.13-$KAFKA_VER" "$R/kafka"; rm -rf "$R/kafka-dl"
    ;;

  # kafka <dir> <timeout> <bootstrap> <topic> <records> <bytes>: produce with
  # kafka-producer-perf-test and print one LOADGEN_JSON line shaped like ingest.
  # Both engines get the same producer settings: acks=all, 64 KiB batches, no compression.
  kafka)
    dir=$1 limit=$2 boot=$3 topic=$4 records=$5 bytes=$6; in_run "$dir"
    bash -c 'echo $$ > "$0/kafka.pid"; exec timeout -k 10 "$@"' "$dir" "$limit" \
      "$R/kafka/bin/kafka-producer-perf-test.sh" --topic "$topic" \
      --num-records "$records" --record-size "$bytes" --throughput -1 --producer-props \
      bootstrap.servers="$boot" acks=all batch.size=65536 linger.ms=5 compression.type=none \
      max.in.flight.requests.per.connection=5 2>&1 | tee "$dir/kafka.log" | tail -1 | python3 -c '
import json, re, sys
line = sys.stdin.read()
m = re.search(r"(\d+) records sent, ([\d.]+) records/sec \(([\d.]+) MB/sec\), ([\d.]+) ms avg latency, ([\d.]+) ms max latency, (\d+) ms 50th, (\d+) ms 95th, (\d+) ms 99th, (\d+) ms 99.9th", line)
if not m:
    sys.exit("could not parse: " + line)
n, rate, mb, avg, mx, p50, p95, p99, p999 = m.groups()
print("LOADGEN_JSON " + json.dumps({"scenario": "kafka", "acked_records": int(n),
  "throughput_msg_s": float(rate), "throughput_mb_s": float(mb), "failed_sends": 0,
  "batch_ack_latency_us": {"p50": int(p50) * 1000, "p99": int(p99) * 1000,
                           "p999": int(p999) * 1000, "max": float(mx) * 1000}}))'
    ;;

  # kafka-tail <dir> <timeout> <bootstrap> <topic> <records> <bytes> <rate>: a consumer
  # (kafka-console-consumer, no group) tails the topic while kafka-producer-perf-test publishes
  # <rate> records/s into it. Same client settings on both engines. Prints one LOADGEN_JSON
  # line with what the consumer fetched per second and what the producer achieved.
  kafka-tail)
    dir=$1 limit=$2 boot=$3 topic=$4 records=$5 bytes=$6 rate=$7; in_run "$dir"
    start "$dir" kafka-tail-producer bash -c 'sleep 10; exec timeout -k 10 "$@"' _ "$limit" \
      "$R/kafka/bin/kafka-producer-perf-test.sh" --topic "$topic" \
      --num-records "$records" --record-size "$bytes" --throughput "$rate" --producer-props \
      bootstrap.servers="$boot" acks=all batch.size=65536 linger.ms=5 compression.type=none \
      max.in.flight.requests.per.connection=5
    # The consumer assigns partition 0 and uses no group: Felix's Kafka layer refuses
    # consumer groups, and kafka-consumer-perf-test always joins one. It prints each
    # record, so the count and the clock are ours. Same settings on both engines.
    t0=$(date +%s.%N)
    timeout -k 10 "$limit" "$R/kafka/bin/kafka-console-consumer.sh" \
      --bootstrap-server "$boot" --topic "$topic" --partition 0 --offset earliest \
      --max-messages "$records" --timeout-ms 120000 \
      --consumer-property enable.auto.commit=false > "$dir/kafka-tail-consumer.out" \
      2> "$dir/kafka-tail-consumer.log" || true
    t1=$(date +%s.%N)
    consumed=$(wc -l < "$dir/kafka-tail-consumer.out" | tr -d ' ')
    [ "$consumed" -gt 0 ] || die "kafka-console-consumer read nothing: $(tail -3 "$dir/kafka-tail-consumer.log")"
    cons="$consumed $t0 $t1"
    # The producer prints its summary last; give it a moment to finish after the consumer.
    for _ in $(seq 1 60); do
      grep -q 'records sent' "$dir/kafka-tail-producer.log" 2>/dev/null \
        && grep 'records sent' "$dir/kafka-tail-producer.log" | tail -1 | grep -q 'ms 99.9th' && break
      sleep 1
    done
    # The tool's own fetch rate runs from the consumer's start, so it carries the idle
    # seconds before the producer begins. The gated rate divides what was consumed by the
    # producer's own run time instead, and completeness (consumed == produced) is gated apart.
    python3 -c '
import json, re, sys
cons, prod, target, size = sys.argv[1].split(), open(sys.argv[2]).read(), float(sys.argv[3]), int(sys.argv[4])
consumed, elapsed = int(cons[0]), float(cons[2]) - float(cons[1])
m = re.findall(r"(\d+) records sent, ([\d.]+) records/sec", prod)
if not m:
    sys.exit("producer did not finish")
sent, rate = int(m[-1][0]), float(m[-1][1])
window = sent / rate
print("LOADGEN_JSON " + json.dumps({"scenario": "kafka-tail", "consumed_records": consumed,
  "consumed_throughput_msg_s": consumed / window, "fetch_throughput_msg_s": consumed / elapsed,
  "consumed_throughput_mb_s": consumed * size / elapsed / 1e6, "produced_records": sent, "window_s": window,
  "publish_throughput_msg_s": rate, "publish_target_msg_s": target, "failed_sends": 0}))' \
      "$cons" "$dir/kafka-tail-producer.log" "$rate" "$bytes"
    ;;

  # clean-runs: before a run. Stop what earlier runs left (only the PIDs in their pid files),
  # delete their run directories and stale holds, then fail if a perf process is still alive.
  clean-runs)
    for d in "$R"/run/*/*/; do
      [ -d "$d" ] || continue
      bash "$R/node.sh" stop "${d%/}" || true
    done
    for d in "$R"/run/*; do [ -e "$d" ] && { in_run "$d"; rm -rf "$d"; }; done
    rm -f "$R"/hold/*
    left=$(pgrep -x -l 'felix-broker|felix-controlpl|felix-loadgen|java' || true)
    [ -z "$left" ] || die "perf processes still running: $(echo "$left" | tr '\n' ' ')"
    ;;

  wipe) in_run "$1"; rm -rf "$1" ;;

  *) die "unknown subcommand $cmd" ;;
esac
