#!/usr/bin/env bash
# Stand up a 3-node Redpanda cluster on the broker VMs, data on the Premium data
# disk, with Redpanda's own production tuning (`rpk redpanda tune all` — the
# blessed tuning, fair to use since it is what Redpanda ships for). rf and
# partitioning are chosen at topic-create time (bench script), not here.
#
# Two phases: configure every node first, THEN start them together. A node with
# empty_seed_starts_cluster=false blocks until it can form a quorum with the
# other seeds, so starting one alone (and waiting on it) times out — every node
# must be configured and then brought up near-simultaneously. Starts are
# --no-block so run-command returns; the cluster is verified afterward.
set -euo pipefail
: "${SESSION:?SESSION=<name>}"
here="$(cd "$(dirname "$0")" && pwd)"
source "${here}/compare-lib.sh"
source "${here}/sessions/${SESSION}.env"
export GROUP

IFS=',' read -ra BIPS <<<"${BROKER_IPS}"
SEED="${BIPS[0]}"
IPS_CSV="${BROKER_IPS}"

# --- phase 1: install + configure every node (do NOT start) ------------------
for i in "${!BIPS[@]}"; do
  self="${BIPS[$i]}"
  echo ">> configuring Redpanda on broker-${i} (${self})"
  run_on_str "$(broker_vm "${i}")" "$(cat <<REMOTE
#!/bin/sh
set -e
export DEBIAN_FRONTEND=noninteractive
if ! command -v rpk >/dev/null 2>&1; then
  curl -1sLf 'https://dl.redpanda.com/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.deb.sh' | bash
  apt-get install -y redpanda
fi
systemctl stop redpanda 2>/dev/null || true
rm -rf /data/redpanda
mkdir -p /data/redpanda
chown -R redpanda:redpanda /data/redpanda
rpk redpanda config bootstrap --self ${self} --ips ${IPS_CSV}
rpk redpanda config set redpanda.data_directory /data/redpanda
rpk redpanda config set redpanda.empty_seed_starts_cluster false
rpk redpanda mode production || true
rpk redpanda tune all || true
echo "configured broker-${i}"
echo __RUNOK__
REMOTE
)" | tail -2
done

# --- phase 2: start every node (non-blocking) --------------------------------
for i in "${!BIPS[@]}"; do
  echo ">> starting Redpanda on broker-${i}"
  run_on_str "$(broker_vm "${i}")" "$(cat <<'REMOTE'
#!/bin/sh
systemctl reset-failed redpanda 2>/dev/null || true
systemctl --no-block start redpanda
echo "start issued"
echo __RUNOK__
REMOTE
)" | tail -1
done

echo ">> waiting for the cluster to form"
for attempt in $(seq 1 24); do
  sleep 5
  info="$(run_on_str "$(broker_vm 0)" "$(cat <<REMOTE
#!/bin/sh
rpk cluster info --brokers ${SEED}:9092 2>&1 || true
echo __RUNOK__
REMOTE
)" 2>/dev/null || true)"
  brokers_up="$(printf '%s' "${info}" | grep -cE '^[0-9]+\s' || true)"
  if printf '%s' "${info}" | grep -q 'BROKER'; then
    echo "${info}" | sed -n '/stdout/,/__RUNOK__/p' | head -12
    echo ">> cluster reachable after $((attempt*5))s"
    break
  fi
  echo "   ...forming (attempt ${attempt})"
done

# Fairness: match Felix's in-memory acked headline. Redpanda at rf=1 defaults to
# fsync-before-ack, which on a 3.6 ms Premium SSD caps a single partition at
# ~230 appends/s — the pathological number the first pass hit, and NOT a fair
# stand-in for Felix acking into memory. write_caching acks from memory and
# fsyncs in the background; the durable comparison flips it back off to match
# Felix on_commit.
echo ">> setting write_caching_default=true (in-memory acked match)"
run_on_str "$(broker_vm 0)" "$(cat <<REMOTE
#!/bin/sh
rpk cluster config set write_caching_default true 2>&1 | tail -2 || true
rpk cluster config get write_caching_default 2>&1 | tail -1 || true
echo __RUNOK__
REMOTE
)" | sed -n '/stdout/,/__RUNOK__/p' | head -4

echo ">> Redpanda up. Bench with: SESSION=${SESSION} SYSTEM=redpanda TLS=1 bash ${here}/bench-kafka-api.sh"
