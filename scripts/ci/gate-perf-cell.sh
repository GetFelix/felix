#!/usr/bin/env bash
# One perf trial of the no-regression gate: start a cluster with one stream (one
# broker for RF 1, three otherwise), drive it with felix-loadgen's ingest scenario, append one JSON line.
#
#   gate-perf-cell.sh <bin dir> <side> <cell> <rf> <payload bytes> <batch> <secs> <round> <results.jsonl>
#
# <bin dir> holds felix-cluster, felix-loadgen and the side's felix-broker, which
# the cluster finds next to its own executable. The gate gives each side its own
# harness and load generator (see gate-perf.yml for why).
#
# Optional environment:
#   ENGINE=raft            sets FELIX_SHARD_ENGINE for the brokers, then checks the shard
#                          directories carry the Raft engine marker. A cell that ran without
#                          it fails instead of reporting. A build before the hybrid's removal
#                          also takes ENGINE=hybrid.
#   FELIX_DURABLE_FSYNC_MODE and FELIX_DURABLE_FSYNC_INTERVAL_MS are passed through to the
#   brokers as they are.
#   UP_LOG_DIR=<dir>       keeps the cluster's up.log there, gzipped, win or lose.
#   Without ENGINE the side must carry no raft marker, so a baseline that ran on Raft fails.
#   FANOUT_TOTAL=N         runs felix-loadgen's pubsub scenario instead of ingest: one
#                          publisher sends N records unacked in <batch>-record batches to
#                          FANOUT subscribers (default 4) and <secs> is ignored. msg_s is
#                          the delivered rate and p50/p99 are publish-to-delivery, so a
#                          backlog the engine drains slowly shows up in p99.
set -euo pipefail

bin=${1:?bin dir}; side=${2:?side}; cell=${3:?cell}; rf=${4:?replication factor}
payload=${5:?payload bytes}; batch=${6:?batch}; secs=${7:?seconds}; round=${8:?round}
results=${9:?results file}

work=$(mktemp -d)
export TMPDIR=$work
# Brokers would otherwise reserve full segments up front.
export FELIX_DURABLE_PREALLOCATE=false
cluster_pid=
engine=${ENGINE:-}
case "$engine" in ''|raft|hybrid) ;; *) echo "ENGINE must be raft or hybrid" >&2; exit 2 ;; esac
[ -z "$engine" ] || export FELIX_SHARD_ENGINE=$engine
cleanup() {
  if [ -n "${UP_LOG_DIR:-}" ] && [ -f "$work/up.log" ]; then
    mkdir -p "$UP_LOG_DIR" && gzip -c "$work/up.log" > "$UP_LOG_DIR/$side-$cell-r$round.up.log.gz" || true
  fi
  if [ -n "$cluster_pid" ]; then
    kill "$cluster_pid" 2>/dev/null || true
    wait "$cluster_pid" 2>/dev/null || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT

# RF 1 is the single-node cell: one broker, no peers.
nodes=3
[ "$rf" = 1 ] && nodes=1
"$bin/felix-cluster" up --nodes "$nodes" --stream perf --replication-factor "$rf" >"$work/up.log" 2>&1 &
cluster_pid=$!
for _ in $(seq 1 240); do
  grep -q '^cluster up\.' "$work/up.log" && break
  kill -0 "$cluster_pid" 2>/dev/null || { cat "$work/up.log"; echo "cluster exited" >&2; exit 1; }
  sleep 0.5
done
grep -q '^cluster up\.' "$work/up.log" || { cat "$work/up.log"; echo "cluster did not come up" >&2; exit 1; }

# What the brokers resolved, once per side and cell, so a mislabelled side shows in the log.
if [ "$round" = 1 ]; then
  echo "broker settings: $side $cell (FELIX_DURABLE_FSYNC_MODE=${FELIX_DURABLE_FSYNC_MODE:-unset} FELIX_DURABLE_FSYNC_INTERVAL_MS=${FELIX_DURABLE_FSYNC_INTERVAL_MS:-unset} FELIX_SHARD_ENGINE=${FELIX_SHARD_ENGINE:-unset})"
  sed 's/\x1b\[[0-9;]*m//g' "$work/up.log" | grep -iE "fsync every|engine|ack level|ack_level|AckLevel|quorum" | sort -u | head -8 | sed "s/^/  [$side $cell] /" || true
fi

session=$work/felix-cluster.json
brokers=$(jq -r '[.nodes[].client_addr] | join(",")' "$session")
# Let placement and the first leases settle before measuring.
sleep 5
if [ -n "${FANOUT_TOTAL:-}" ]; then
  load=(--scenario pubsub --fanout "${FANOUT:-4}" --warmup 2000 --total "$FANOUT_TOTAL")
else
  load=(--scenario ingest --in-flight 8 --concurrency 4 --duration-secs "$secs")
fi
echo "loadgen: $side $cell round $round uses the felix-loadgen built from $(cat "$bin/loadgen.version" 2>/dev/null || echo unknown)"
"$bin/felix-loadgen" --brokers "$brokers" \
  --tenant "$(jq -r .tenant_id "$session")" --namespace "$(jq -r .namespace "$session")" \
  --token "$(jq -r .client_token "$session")" \
  --stream perf --payload-bytes "$payload" --batch "$batch" "${load[@]}" \
  --environment "gate-$side-$cell" >"$work/loadgen.out" 2>"$work/loadgen.err" \
  || { tail -20 "$work/loadgen.err"; echo "loadgen failed" >&2; exit 1; }

markers=$(find "$work" -type f -name engine -exec grep -lx raft {} + 2>/dev/null | wc -l | tr -d ' ')
echo "engine check: $side $cell round $round asked for ${engine:-the release default}, $markers shard directories carry the raft marker"
case "${engine:-default}:$markers" in
  raft:0) echo "asked for raft but no shard directory carries the raft marker" >&2; exit 1 ;;
  raft:*|hybrid:0) ;;
  default:0) ;;
  *) echo "asked for ${engine:-the release default} but $markers shard directories carry the raft marker" >&2; exit 1 ;;
esac

line=$(grep '^LOADGEN_JSON ' "$work/loadgen.out" | head -1 | cut -d' ' -f2-)
[ -n "$line" ] || { echo "no LOADGEN_JSON line" >&2; exit 1; }
if [ -n "${FANOUT_TOTAL:-}" ]; then
  jq -c --arg side "$side" --arg cell "$cell" --argjson round "$round" --arg engine "$engine" \
    '{side: $side, cell: $cell, round: $round, engine: $engine,
      msg_s: .delivered_throughput_msg_s, publish_msg_s: .publish_throughput_msg_s,
      p50_us: .delivery_latency_us.p50, p99_us: .delivery_latency_us.p99,
      received_per_sub: .healthy_received_per_sub, expected_per_sub: .expected_per_sub}' <<<"$line" >>"$results"
else
  jq -c --arg side "$side" --arg cell "$cell" --argjson round "$round" --arg engine "$engine" \
    '{side: $side, cell: $cell, round: $round, engine: $engine,
      msg_s: (.acked_throughput_msg_s // .throughput_msg_s),
      p50_us: .batch_ack_latency_us.p50, p99_us: .batch_ack_latency_us.p99, failed_sends: .failed_sends}' <<<"$line" >>"$results"
fi
