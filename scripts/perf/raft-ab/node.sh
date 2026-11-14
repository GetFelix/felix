#!/usr/bin/env bash
# The on-box half of the hybrid vs Raft A/B. run.sh copies it to
# /mnt/build/raft-ab/node.sh on each build box and calls one subcommand per ssh.
# Everything it creates or deletes is under /mnt/build/raft-ab, and it only
# ever signals PIDs it wrote to a pid file itself.
set -euo pipefail

R=/mnt/build/raft-ab
BIN=$R/bin
TENANT=perf
NS=default
# Fixed ports, away from anything the lanes' tests bind (they pick ephemeral ports).
CP_PORT=18443 CP_METRICS=18080 CP_BOOT=18095
QUIC_PORT=15000 INTERNAL_PORT=17000 METRICS_PORT=19100
BOOT_TOKEN="raft-ab-bootstrap-only"

die() { echo "!! $*" >&2; exit 1; }
# Refuse any path outside the run tree, so a bad argument can never wipe more.
in_run() { case "$1" in "$R"/run/?*) ;; *) die "refusing path outside $R/run: $1" ;; esac; }
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

  # cp <dir>: the control plane, in memory, bootstrap with dev tokens on loopback.
  cp)
    dir=$1; in_run "$dir"; mkdir -p "$dir"; ip=$(ip_self)
    start "$dir" cp env \
      FELIX_CONTROLPLANE_STORAGE_BACKEND=memory \
      FELIX_CONTROLPLANE_BIND="$ip:$CP_PORT" FELIX_CONTROLPLANE_METRICS_BIND="$ip:$CP_METRICS" \
      FELIX_REGION_ID=gcp FELIX_BOOTSTRAP_ENABLED=true FELIX_BOOTSTRAP_BIND_ADDR="127.0.0.1:$CP_BOOT" \
      FELIX_BOOTSTRAP_TOKEN=$BOOT_TOKEN FELIX_BOOTSTRAP_DEV_TOKENS=true \
      FELIX_EXCHANGE_TOKEN_TTL_SECONDS=86400 RUST_LOG=info \
      "$BIN/felix-controlplane"
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
  broker)
    dir=$1 i=$2 cpip=$3 engine=$4 fsync=$5; in_run "$dir"; ip=$(ip_self)
    mkdir -p "$dir/data"
    start "$dir" broker env \
      FELIX_SHARD_ENGINE="$engine" FELIX_DURABLE_FSYNC_MODE="$fsync" \
      FELIX_NODE_ID="broker-$i" FELIX_REGION_ID=gcp \
      FELIX_CONTROLPLANE_URL="http://$cpip:$CP_PORT" \
      FELIX_QUIC_BIND="$ip:$QUIC_PORT" FELIX_CLIENT_ADVERTISE_ADDR="$ip:$QUIC_PORT" \
      FELIX_INTERNAL_BIND="$ip:$INTERNAL_PORT" FELIX_NODE_ADVERTISE_ADDR="$ip:$INTERNAL_PORT" \
      FELIX_BROKER_METRICS_BIND="$ip:$METRICS_PORT" FELIX_DURABLE_STORAGE_DIR="$dir/data" \
      FELIX_NODE_TOKEN_FILE="$dir/node.jwt" FELIX_NODE_REFRESH_TOKEN_FILE="$dir/node.refresh" \
      FELIX_INTERNAL_ALLOW_UNAUTHENTICATED=true FELIX_MTU_UPPER_BOUND=4096 RUST_LOG=info \
      "$BIN/felix-broker"
    wait_http "http://$ip:$METRICS_PORT/ready" 120 || { tail -30 "$dir/broker.log" >&2; die "broker-$i not ready"; }
    ;;

  # stream <dir> <name> <shards> <Leader|Quorum>: create an RF 3 durable stream.
  stream)
    dir=$1; in_run "$dir"; RESP=$dir/resp.json
    post "http://$(ip_self):$CP_PORT/v1/tenants/$TENANT/namespaces/$NS/streams" \
      "{\"stream\":\"$2\",\"kind\":\"Stream\",\"shards\":$3,\"replication_factor\":3,\"retention\":{\"max_age_seconds\":null,\"max_size_bytes\":null},\"consistency\":\"$4\",\"delivery\":\"AtLeastOnce\",\"durable\":true}" \
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

  # loadgen <dir> <timeout-secs> <args...>: felix-loadgen with the client token; prints its output.
  loadgen)
    dir=$1 limit=$2; shift 2; in_run "$dir"
    timeout -k 10 "$limit" "$BIN/felix-loadgen" --tenant "$TENANT" --token-file "$dir/tokens/client.jwt" "$@" 2>&1 \
      | tee "$dir/loadgen.log"
    ;;

  # stop <dir>: TERM then KILL the processes this script started under <dir>.
  stop)
    dir=$1; in_run "$dir"
    for p in "$dir"/*.pid; do
      [ -f "$p" ] || continue; pid=$(cat "$p")
      kill "$pid" 2>/dev/null || true
    done
    for _ in $(seq 1 20); do
      alive=0
      for p in "$dir"/*.pid; do [ -f "$p" ] && kill -0 "$(cat "$p")" 2>/dev/null && alive=1; done
      [ $alive = 0 ] && break; sleep 0.5
    done
    for p in "$dir"/*.pid; do [ -f "$p" ] && kill -9 "$(cat "$p")" 2>/dev/null || true; done
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

  wipe) in_run "$1"; rm -rf "$1" ;;

  *) die "unknown subcommand $cmd" ;;
esac
