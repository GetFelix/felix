#!/usr/bin/env bash
# One-box Raft latency probe: control plane, three brokers and the generator on
# this box, loopback. Not an A/B (three logs share one disk); it splits the
# publish path into stages. Run from the lane's src dir on a build box:
#   onebox.sh <name> <hybrid|raft> <Leader|Quorum> <on_commit|periodic> [secs] [KEY=VALUE broker env...]
# Everything lives under /mnt/build/<lane>/run/<name> and only its own pids are signalled.
set -euo pipefail
name=$1 engine=$2 cons=$3 fsync=$4 secs=${5:-20}; shift $(( $# < 5 ? $# : 5 ))
extra=("$@")
lane=$(basename "$(dirname "$PWD")")
[ "$(basename "$PWD")" = src ] || { echo "!! run from /mnt/build/<lane>/src" >&2; exit 1; }
BIN=${BIN:-/mnt/build/$lane/target/release}
dir=/mnt/build/$lane/run/$name
rm -rf "$dir"; mkdir -p "$dir/tokens"
: "${PAYLOAD:=1024}" "${BATCH:=16}" "${PUBLISHERS:=8}" "${IN_FLIGHT:=4}" "${SHARDS:=6}"
KEYS=$((SHARDS * 8)); TENANT=perf; NS=default; BOOT_TOKEN=latency-floor-bootstrap
ip=127.0.0.1; CP_PORT=28443; CP_METRICS=28080; CP_BOOT=28095
RESP=$dir/resp.json
start() { local n=$1; shift; setsid nohup "$@" > "$dir/$n.log" 2>&1 < /dev/null & echo $! > "$dir/$n.pid"; }
stop() {
  for p in "$dir"/*.pid; do [ -f "$p" ] && kill "$(cat "$p")" 2>/dev/null || true; done
  sleep 2
  for p in "$dir"/*.pid; do [ -f "$p" ] && kill -9 "$(cat "$p")" 2>/dev/null || true; done
}
trap stop EXIT
wait_http() { local i; for ((i = 0; i < $2; i++)); do curl -fsS -o /dev/null "$1" 2>/dev/null && return 0; sleep 1; done; return 1; }
post() { code=$(curl -s -o "$RESP" -w '%{http_code}' -X POST "$1" -H 'Content-Type: application/json' -H "$3" -d "$2"); }
field() { python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$RESP" "$1"; }

start cp env FELIX_CONTROLPLANE_STORAGE_BACKEND=memory \
  FELIX_CONTROLPLANE_BIND="$ip:$CP_PORT" FELIX_CONTROLPLANE_METRICS_BIND="$ip:$CP_METRICS" \
  FELIX_REGION_ID=gcp FELIX_BOOTSTRAP_ENABLED=true FELIX_BOOTSTRAP_BIND_ADDR="$ip:$CP_BOOT" \
  FELIX_BOOTSTRAP_TOKEN=$BOOT_TOKEN FELIX_BOOTSTRAP_DEV_TOKENS=true \
  FELIX_EXCHANGE_TOKEN_TTL_SECONDS=86400 RUST_LOG=info "$BIN/felix-controlplane"
wait_http "http://$ip:$CP_METRICS/ready" 60 || { tail -20 "$dir/cp.log"; exit 1; }
boot="http://$ip:$CP_BOOT/internal/bootstrap/tenants/$TENANT"
post "$boot/initialize" '{"display_name":"perf","idp_issuers":[],"initial_admin_principals":["p:admin"],
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
[ "$code" = 200 ] || { cat "$RESP"; exit 1; }
mint() {
  post "$boot/dev-token" "$2" "X-Felix-Bootstrap-Token: $BOOT_TOKEN"
  [ "$code" = 200 ] || { cat "$RESP"; exit 1; }
  field felix_token > "$dir/tokens/$1.jwt"; field refresh_token > "$dir/tokens/$1.refresh"
}
for i in 1 2 3; do mint "node-$i" "{\"principal\":\"p:broker-$i\",\"audience\":\"felix-controlplane\"}"; done
mint admin '{"principal":"p:admin","audience":"felix-controlplane"}'
mint client '{"principal":"p:client","audience":"felix-broker","requested":["stream.publish","stream.subscribe"]}'
admin="Authorization: Bearer $(cat "$dir/tokens/admin.jwt")"
post "http://$ip:$CP_PORT/v1/tenants/$TENANT/namespaces" '{"namespace":"default","display_name":"default"}' "$admin"
case "$code" in 2*) ;; *) cat "$RESP"; exit 1 ;; esac

brokers=""
for i in 1 2 3; do
  mkdir -p "$dir/b$i/data"
  start "broker$i" env FELIX_STAGE_PROBE=1 "${extra[@]}" \
    FELIX_SHARD_ENGINE="$engine" FELIX_DURABLE_FSYNC_MODE="$fsync" \
    FELIX_NODE_ID="broker-$i" FELIX_REGION_ID=gcp FELIX_CONTROLPLANE_URL="http://$ip:$CP_PORT" \
    FELIX_QUIC_BIND="$ip:25${i}00" FELIX_CLIENT_ADVERTISE_ADDR="$ip:25${i}00" \
    FELIX_INTERNAL_BIND="$ip:27${i}00" FELIX_NODE_ADVERTISE_ADDR="$ip:27${i}00" \
    FELIX_BROKER_METRICS_BIND="$ip:29${i}00" FELIX_DURABLE_STORAGE_DIR="$dir/b$i/data" \
    FELIX_NODE_TOKEN_FILE="$dir/tokens/node-$i.jwt" FELIX_NODE_REFRESH_TOKEN_FILE="$dir/tokens/node-$i.refresh" \
    FELIX_INTERNAL_ALLOW_UNAUTHENTICATED=true FELIX_MTU_UPPER_BOUND=4096 RUST_LOG=info "$BIN/felix-broker"
  brokers="$brokers${brokers:+,}$ip:25${i}00"
done
for i in 1 2 3; do wait_http "http://$ip:29${i}00/ready" 120 || { tail -30 "$dir/broker$i.log"; exit 1; }; done
post "http://$ip:$CP_PORT/v1/tenants/$TENANT/namespaces/$NS/streams" \
  "{\"stream\":\"bench\",\"kind\":\"Stream\",\"shards\":$SHARDS,\"replication_factor\":3,\"retention\":{\"max_age_seconds\":null,\"max_size_bytes\":null},\"consistency\":\"$cons\",\"delivery\":\"AtLeastOnce\",\"durable\":true}" "$admin"
case "$code" in 2*) ;; *) cat "$RESP"; exit 1 ;; esac
st=""
for ((i = 0; i < 180; i += 3)); do
  st=$(curl -fsS -H "$admin" "http://$ip:$CP_PORT/v1/placement/replication" | python3 -c '
import collections, json, sys
d = json.load(sys.stdin); items = d.get("items", [])
c = collections.Counter((it.get("leader") or "none") for it in items)
print("under=%d shards=%d leaders=%s" % (d.get("under_replicated", 0), len(items), ",".join("%s:%d" % kv for kv in sorted(c.items()))))' || true)
  case "$st" in "under=0 shards=$SHARDS "*) break ;; esac; sleep 3
done
echo "replication: $st"
sleep 8
cpu_ticks() { local t=0 i u s; for i in 1 2 3; do read -r u s < <(awk '{print $14, $15}' "/proc/$(cat "$dir/broker$i.pid")/stat"); t=$((t + u + s)); done; echo $t; }
cpu0=$(cpu_ticks)
if [ "${STRACE:-0}" = 1 ]; then
  for i in 1 2 3; do
    sudo -n strace -f -c -e trace=fdatasync,fsync,sync_file_range -o "$dir/strace$i.txt" -p "$(cat "$dir/broker$i.pid")" > /dev/null 2>&1 &
  done
  sleep 1
fi
timeout -k 10 $((secs + 240)) "$BIN/felix-loadgen" --tenant "$TENANT" --token-file "$dir/tokens/client.jwt" \
  --brokers "$brokers" --environment "onebox-$engine" --scenario ingest --stream bench --payload-bytes "$PAYLOAD" \
  --batch "$BATCH" --concurrency "$PUBLISHERS" --in-flight "$IN_FLIGHT" --duration-secs "$secs" --keys "$KEYS" \
  --shards "$SHARDS" --cluster 2>&1 | grep -v '^LOADGEN_JSON' | tee "$dir/loadgen.log"
cpu1=$(cpu_ticks)
python3 - "$dir/loadgen.log" "$cpu0" "$cpu1" "$(getconf CLK_TCK)" <<'PY'
import re, sys
log = open(sys.argv[1]).read(); m = re.search(r"acked = (\d+)", log)
cpu = (int(sys.argv[3]) - int(sys.argv[2])) / int(sys.argv[4])
if m: print("broker cpu (3 brokers): %.1f s, %.2f us/msg" % (cpu, cpu * 1e6 / int(m.group(1))))
PY
if [ "${STRACE:-0}" = 1 ]; then sudo -n pkill -INT -P $$ strace 2>/dev/null || true; sudo -n kill -INT $(jobs -p) 2>/dev/null || true; sleep 2; fi
stop
for i in 1 2 3; do
  # Whole-run totals: appends and flush rounds the probe saw, against the fsync syscalls strace counted.
  echo "== broker-$i totals: $(awk '/LOG_APPEND_WRITE: n=/{split($2,a,"="); w+=a[2]} /FSYNC: n=/{split($2,a,"="); f+=a[2]} /FLUSH_ROUND_APPENDS_N: n=/{split($2,a,"="); r+=a[2]} END{printf "appends=%d flush_rounds=%d commit_waits=%d", w, r, f}' "$dir/broker$i.log")"
  [ -f "$dir/strace$i.txt" ] && grep -E 'fdatasync|fsync|sync_file_range|total' "$dir/strace$i.txt" || true
done
# The middle report of each broker: steady state, not ramp-up or drain.
for i in 1 2 3; do
  echo "== broker-$i"
  awk '/^STAGE_PROBE/{n++} END{print n+0}' "$dir/broker$i.log" > "$dir/n"
  awk -v want=$(( ($(cat "$dir/n") + 1) / 2 )) '/^STAGE_PROBE/{n++; next} n==want && /^  /{print}' "$dir/broker$i.log"
  grep -E ' (ERROR|WARN) ' "$dir/broker$i.log" | tail -3 || true
done
mkdir -p /mnt/build/$lane/src/probe-out; cp "$dir"/*.log /mnt/build/$lane/src/probe-out/ 2>/dev/null || true
rm -rf "$dir/b1" "$dir/b2" "$dir/b3"
