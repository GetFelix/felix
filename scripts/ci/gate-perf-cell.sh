#!/usr/bin/env bash
# One perf trial of the no-regression gate: start a three-node cluster with one
# stream, drive it with felix-loadgen's ingest scenario, append one JSON line.
#
#   gate-perf-cell.sh <bin dir> <side> <cell> <rf> <payload bytes> <batch> <secs> <round> <results.jsonl>
#
# <bin dir> holds felix-cluster, felix-loadgen and the side's felix-broker, which
# the cluster finds next to its own executable. Both sides run the same
# harness and load generator, so only the broker differs.
set -euo pipefail

bin=${1:?bin dir}; side=${2:?side}; cell=${3:?cell}; rf=${4:?replication factor}
payload=${5:?payload bytes}; batch=${6:?batch}; secs=${7:?seconds}; round=${8:?round}
results=${9:?results file}

work=$(mktemp -d)
export TMPDIR=$work
# Brokers would otherwise reserve full segments up front.
export FELIX_DURABLE_PREALLOCATE=false
cluster_pid=
cleanup() {
  if [ -n "$cluster_pid" ]; then
    kill "$cluster_pid" 2>/dev/null || true
    wait "$cluster_pid" 2>/dev/null || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT

"$bin/felix-cluster" up --nodes 3 --stream perf --replication-factor "$rf" >"$work/up.log" 2>&1 &
cluster_pid=$!
for _ in $(seq 1 240); do
  grep -q '^cluster up\.' "$work/up.log" && break
  kill -0 "$cluster_pid" 2>/dev/null || { cat "$work/up.log"; echo "cluster exited" >&2; exit 1; }
  sleep 0.5
done
grep -q '^cluster up\.' "$work/up.log" || { cat "$work/up.log"; echo "cluster did not come up" >&2; exit 1; }

session=$work/felix-cluster.json
brokers=$(jq -r '[.nodes[].client_addr] | join(",")' "$session")
# Let placement and the first leases settle before measuring.
sleep 5
"$bin/felix-loadgen" --brokers "$brokers" \
  --tenant "$(jq -r .tenant_id "$session")" --namespace "$(jq -r .namespace "$session")" \
  --token "$(jq -r .client_token "$session")" \
  --scenario ingest --stream perf --payload-bytes "$payload" --batch "$batch" \
  --in-flight 8 --concurrency 4 --duration-secs "$secs" \
  --environment "gate-$side-$cell" >"$work/loadgen.out" 2>"$work/loadgen.err" \
  || { tail -20 "$work/loadgen.err"; echo "loadgen failed" >&2; exit 1; }

line=$(grep '^LOADGEN_JSON ' "$work/loadgen.out" | head -1 | cut -d' ' -f2-)
[ -n "$line" ] || { echo "no LOADGEN_JSON line" >&2; exit 1; }
jq -c --arg side "$side" --arg cell "$cell" --argjson round "$round" \
  '{side: $side, cell: $cell, round: $round,
    msg_s: (.acked_throughput_msg_s // .throughput_msg_s),
    p99_us: .batch_ack_latency_us.p99, failed_sends: .failed_sends}' <<<"$line" >>"$results"
