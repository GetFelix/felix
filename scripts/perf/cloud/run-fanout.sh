#!/usr/bin/env bash
# The fanout-scaling curve: delivery latency and delivered throughput as the
# subscriber count climbs 1 -> 1000 on one stream. This is the axis Felix's
# encode-once fanout (`Arc<Bytes>` shared across every subscriber, one bounded
# queue per subscriber) is built to win, and the one the ingest matrix never
# exercised. Same harness as run.sh: felix-loadgen on the loadgen VM, dispatched
# over run-command, one LOADGEN_JSON line per case.
set -euo pipefail
: "${SESSION:?SESSION=<name>}"
here="$(cd "$(dirname "$0")" && pwd)"
source "${here}/lib.sh"
source "${here}/sessions/${SESSION}.env"
export GROUP="${GROUP}"

out="${here}/sessions/${SESSION}-results"
mkdir -p "${out}"
IFS=',' read -ra brokers <<<"${BROKER_IPS}"
broker_addrs=$(printf '%s:5000,' "${brokers[@]}"); broker_addrs="${broker_addrs%,}"
TOKEN_FILE="/home/felix/felix-session/token"

run_case() {
  local label="$1"; shift
  echo ">> ${label}"
  if run_on_str "$(loadgen_vm)" "set -eu
export FELIX_MTU_UPPER_BOUND=4096
ulimit -n 1048576 || ulimit -n 65536 || true
felix-loadgen --brokers '${broker_addrs}' --tenant perf --token-file '${TOKEN_FILE}' --environment 'azure-${TIER}' $* > /tmp/felix-case.out 2>/tmp/felix-case.err || { echo '!! case failed'; tail -30 /tmp/felix-case.err; exit 1; }
cat /tmp/felix-case.out
echo __RUNOK__" | tee "${out}/${label}.out"; then :; else
    echo "!! case ${label} did not complete; recorded and continuing" | tee -a "${out}/${label}.out"
  fi
}

# Latency profile (batch 1, per-message ack): delivery p50/p99 and delivered
# throughput vs fanout. Publish volume shrinks as fanout grows so the delivered
# count (publishes x fanout) stays in a ~1-4 M band and each run is ~10-20 s.
run_case "fanout-lat-f1"    --scenario pubsub --stream perf --payload-bytes 256 --fanout 1    --batch 1 --warmup 2000 --total 40000
run_case "fanout-lat-f10"   --scenario pubsub --stream perf --payload-bytes 256 --fanout 10   --batch 1 --warmup 2000 --total 40000
run_case "fanout-lat-f50"   --scenario pubsub --stream perf --payload-bytes 256 --fanout 50   --batch 1 --warmup 2000 --total 40000
run_case "fanout-lat-f100"  --scenario pubsub --stream perf --payload-bytes 256 --fanout 100  --batch 1 --warmup 1000 --total 20000
run_case "fanout-lat-f250"  --scenario pubsub --stream perf --payload-bytes 256 --fanout 250  --batch 1 --warmup 1000 --total 12000
run_case "fanout-lat-f500"  --scenario pubsub --stream perf --payload-bytes 256 --fanout 500  --batch 1 --warmup 500  --total 8000
run_case "fanout-lat-f1000" --scenario pubsub --stream perf --payload-bytes 256 --fanout 1000 --batch 1 --warmup 500  --total 4000

# Throughput profile (batch 64, binary): the delivered-throughput ceiling as
# fanout scales — encode-once should keep delivered msg/s climbing with fanout
# until the delivery path saturates.
run_case "fanout-tp-f1"   --scenario pubsub --stream perf --payload-bytes 256 --fanout 1   --batch 64 --binary --warmup 2000 --total 400000
run_case "fanout-tp-f10"  --scenario pubsub --stream perf --payload-bytes 256 --fanout 10  --batch 64 --binary --warmup 2000 --total 200000
run_case "fanout-tp-f50"  --scenario pubsub --stream perf --payload-bytes 256 --fanout 50  --batch 64 --binary --warmup 1000 --total 60000
run_case "fanout-tp-f100" --scenario pubsub --stream perf --payload-bytes 256 --fanout 100 --batch 64 --binary --warmup 1000 --total 30000
run_case "fanout-tp-f500" --scenario pubsub --stream perf --payload-bytes 256 --fanout 500 --batch 64 --binary --warmup 500  --total 8000

grep -h '^LOADGEN_JSON ' "${out}"/fanout-*.out | sed 's/^LOADGEN_JSON //' > "${out}/fanout.jsonl"
echo ">> $(wc -l < "${out}/fanout.jsonl") fanout rows in ${out}/fanout.jsonl"
