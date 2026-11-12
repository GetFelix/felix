#!/usr/bin/env bash
# The durability-cost matrix: what each level of durability actually costs on
# real storage, measured on one topology so the three are comparable.
#
# `run.sh` publishes to `perf` (durable: false) at the broker default fsync, so
# nothing in it touches the disk. That was deliberate -- it gives a clean 1:1
# against the loopback baseline -- but it leaves the durable path unmeasured on
# Azure, which is what #375 asks for.
#
# Three rows, same hardware, same session:
#
#   in-memory          `perf`, no durable log               (run.sh's number)
#   page-cache durable `perf-durable`, Periodic (250ms)     append + page cache
#   disk-synced        `perf-durable`, OnCommit             append + device flush
#
# The third is the one with no premium-storage figure anywhere: the sole durable
# OnCommit numbers in sessions/ are from local NVMe, a different device class.
# On Premium SSD expect this to be *disk-bound* -- the managed-disk cap is the
# wall, not the commit path -- and that is the result, not a regression.
#
# Also retakes the cache write path at both fsync modes. The committed
# cache-oncommit rows predate #390, which made the cache group-commit like the
# stream path, so they describe code that no longer exists.
set -euo pipefail

: "${SESSION:?SESSION=<name>}"
here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "${here}/lib.sh"
# shellcheck disable=SC1090
source "${here}/sessions/${SESSION}.env"
export GROUP="${GROUP}"

out="${here}/sessions/${SESSION}-results"
mkdir -p "${out}"
failed_cases=""
IFS=',' read -ra brokers <<<"${BROKER_IPS}"
broker_addrs=$(printf '%s:5000,' "${brokers[@]}"); broker_addrs="${broker_addrs%,}"
TOKEN_FILE="/home/felix/felix-session/token"

# Same contract as run.sh: a single failing case is a data point, not a reason
# to lose the hours of paid cluster already spent.
run_case() {
  local label="$1"; shift
  echo ">> ${label}"
  if run_on_str "$(loadgen_vm)" "set -eu
export FELIX_MTU_UPPER_BOUND=4096
ulimit -n 1048576 || true
felix-loadgen --brokers '${broker_addrs}' --tenant perf --token-file '${TOKEN_FILE}' --environment 'azure-durable' $* > /tmp/c.out 2>/tmp/c.err || { echo '!! case failed'; tail -20 /tmp/c.err; exit 1; }
cat /tmp/c.out
echo __RUNOK__" | tee "${out}/${label}.out" | grep -E 'LOADGEN_JSON' ; then
    :
  else
    echo "!! ${label} did not produce a result line"
    failed_cases="${failed_cases} ${label}"
  fi
}

# Set the durable fsync mode on every broker in /etc/felix/overrides.env (a
# drop-in's Environment= loses to the regenerated broker.env), then restart
# and wait for re-register.
set_fsync() {
  mode="$1"
  echo ">> setting FELIX_DURABLE_FSYNC_MODE=${mode} on brokers"
  for i in "${!brokers[@]}"; do
    agent_on "$(broker_vm "${i}")" "rm -f /etc/systemd/system/felix-broker.service.d/10-fsync.conf
systemctl daemon-reload
felix-agent env-set FELIX_DURABLE_FSYNC_MODE=${mode} >/dev/null
felix-agent restart" | grep '^state='
  done
  # The brokers have to re-register and retake their shards before the next
  # case, or the first publish races the assignment feed.
  sleep 8
}

# One pass of the publish shapes against whichever stream is named. Latency
# cases are batch 1 with per-message acks (the request-latency configuration);
# throughput cases are batch 64, sized past the QUIC send window. Fanout is
# fixed at 1 -- this matrix is about the write path, and fanout is already
# swept by run.sh.
pass() {
  stream="$1"; tag="$2"
  for payload in 0 256 4096; do
    run_case "${tag}-lat-p${payload}" \
      --scenario pubsub --stream "${stream}" --payload-bytes "${payload}" \
      --fanout 1 --batch 1 --warmup 2000 --total 20000
  done
  for payload in 256 4096; do
    run_case "${tag}-tp-p${payload}" \
      --scenario pubsub --stream "${stream}" --payload-bytes "${payload}" \
      --fanout 1 --batch 64 --binary --warmup 2000 --total 500000
  done
}

echo "===== ROW 1/3: in-memory (stream perf, fsync irrelevant) ====="
pass perf inmem

echo "===== ROW 2/3: durable + Periodic (stream perf-durable) ====="
set_fsync periodic
pass perf-durable dur-periodic
run_case "cache-periodic-c8" --scenario cache --cache perf --payload-bytes 256 --concurrency 8 --total 20000

echo "===== ROW 3/3: durable + OnCommit (stream perf-durable) ====="
set_fsync on_commit
pass perf-durable dur-oncommit
# The cache retake (#390 made this path group-commit); c1 and c8 because the
# whole point of group commit is what happens when there is more than one
# waiter, and c1 is the per-append device-flush constant.
run_case "cache-oncommit-c1" --scenario cache --cache perf --payload-bytes 256 --concurrency 1 --total 5000
run_case "cache-oncommit-c8" --scenario cache --cache perf --payload-bytes 256 --concurrency 8 --total 20000

echo ">> restoring the broker default fsync mode"
set_fsync periodic

grep -h '^LOADGEN_JSON ' "${out}"/*.out | sed 's/^LOADGEN_JSON //' > "${out}/results.jsonl"

if [ -n "${failed_cases}" ]; then
  echo "!! cases with no result:${failed_cases}"
  exit 1
fi
echo ">> durable matrix complete: ${out}/results.jsonl"
