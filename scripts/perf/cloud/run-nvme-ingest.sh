#!/usr/bin/env bash
# The NVMe upper-bound run: does durable ingest match in-memory once the disk is
# not the wall? On Premium SSD (~170 MB/s) durable was a page-cache burst; these
# brokers put the log on RAID0 local NVMe (~750 MB/s write each, ~2.25 GB/s
# aggregate). Ramp the ingest scenario against the in-memory stream and the
# durable OnCommit stream, and sample broker CPU at the ceiling (the metric that
# survives a disk-bound comparison: MB/s per vCPU).
set -euo pipefail
: "${SESSION:?SESSION=<name>}"
here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "${here}/lib.sh"
# shellcheck disable=SC1090
source "${here}/sessions/${SESSION}.env"
export GROUP="${GROUP}"

out="${here}/sessions/${SESSION}-results"; mkdir -p "${out}"
IFS=',' read -ra brokers <<<"${BROKER_IPS}"
broker_addrs=$(printf '%s:5000,' "${brokers[@]}"); broker_addrs="${broker_addrs%,}"
TOKEN_FILE="/home/felix/felix-session/token"

run_case() {
  local label="$1"; shift
  echo ">> ${label}"
  run_on_str "$(loadgen_vm)" "set -eu
export FELIX_MTU_UPPER_BOUND=4096
ulimit -n 1048576 || true
felix-loadgen --brokers '${broker_addrs}' --tenant perf --token-file '${TOKEN_FILE}' --environment 'azure-nvme' $* > /tmp/c.out 2>/tmp/c.err || { echo '!! case failed'; tail -20 /tmp/c.err; exit 1; }
cat /tmp/c.out
echo __RUNOK__" | tee "${out}/${label}.out" | grep -E 'ingest:|LOADGEN_JSON' || true
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
  sleep 8
}

# 4 KiB ingest ramp; total scales with concurrency so each publisher does ~200k.
ramp() {
  stream="$1"; tag="$2"
  for c in 1 3 6 12 24; do
    total=$(( c * 200000 ))
    run_case "ingest-${tag}-4k-c${c}" --scenario ingest --stream "${stream}" \
      --payload-bytes 4096 --batch 64 --concurrency "${c}" --total "${total}"
  done
}

echo "===== IN-MEMORY (stream perf) ====="
ramp perf inmem

echo "===== DURABLE OnCommit (stream perf-durable) ====="
set_fsync on_commit
ramp perf-durable dur-oncommit

# 256 B message-rate point at peak concurrency, both.
run_case "ingest-inmem-256-c24" --scenario ingest --stream perf --payload-bytes 256 --batch 64 --concurrency 24 --total 12000000
run_case "ingest-dur-256-c24"   --scenario ingest --stream perf-durable --payload-bytes 256 --batch 64 --concurrency 24 --total 12000000

grep -h '^LOADGEN_JSON ' "${out}"/ingest-*.out | sed 's/^LOADGEN_JSON //' > "${out}/nvme-ingest.jsonl"
echo ">> $(wc -l < "${out}/nvme-ingest.jsonl") ingest rows -> ${out}/nvme-ingest.jsonl"
