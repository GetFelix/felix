#!/usr/bin/env bash
# What the durability guarantee costs: RF=3 `Quorum` beside RF=3 `Leader`, on a
# tier where replication pays real inter-zone distance (#425).
#
# Every published throughput and latency figure before this is RF=1 `Leader`,
# which is not the configuration the docs recommend. RF=1 is also the one
# setting where `Quorum` means nothing -- a quorum of one is the leader -- so
# this script refuses to run unless the session was seeded replicated.
#
# Run it on a **t2** session (brokers in zones 1/2/3) seeded with
# REPLICATION_FACTOR=3. On t1 every broker sits in one proximity placement
# group and the inter-zone RTT the guarantee actually costs is absent, so the
# Quorum/Leader delta would be measuring the placement group rather than
# replication.
#
# Was blocked on #411 until replication stopped being a 2s timer sweep that
# completed at the slowest follower. With #457, #471, #478 and #494 in, a
# Quorum measurement now measures replication rather than a tick.
set -euo pipefail

: "${SESSION:?SESSION=<name>}"
here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "${here}/lib.sh"
# shellcheck disable=SC1090
source "${here}/sessions/${SESSION}.env"
export GROUP="${GROUP}"

: "${TIER:?TIER missing from the session env}"
if [ "${TIER}" != "t2" ]; then
  echo "!! this session is ${TIER}. Quorum costs inter-zone distance, which t1's" >&2
  echo "   proximity placement group removes. Provision a t2 session, or set"    >&2
  echo "   ALLOW_ANY_TIER=1 to measure the placement group on purpose."          >&2
  [ "${ALLOW_ANY_TIER:-0}" = "1" ] || exit 1
fi

out="${here}/sessions/${SESSION}-results"
mkdir -p "${out}"
failed_cases=""
IFS=',' read -ra brokers <<<"${BROKER_IPS}"
broker_addrs=$(printf '%s:5000,' "${brokers[@]}"); broker_addrs="${broker_addrs%,}"
TOKEN_FILE="/home/felix/felix-session/token"

run_case() {
  local label="$1"; shift
  echo ">> ${label}"
  if run_on_str "$(loadgen_vm)" "set -eu
export FELIX_MTU_UPPER_BOUND=4096
ulimit -n 1048576 || true
felix-loadgen --brokers '${broker_addrs}' --tenant perf --token-file '${TOKEN_FILE}' --environment '${CLOUD:-azure}-quorum' $* > /tmp/c.out 2>/tmp/c.err || { echo '!! case failed'; tail -20 /tmp/c.err; exit 1; }
cat /tmp/c.out
echo __RUNOK__" | tee "${out}/${label}.out" | grep -E 'LOADGEN_JSON' ; then
    :
  else
    echo "!! ${label} did not produce a result line"
    failed_cases="${failed_cases} ${label}"
  fi
}

# The comparison is only honest if both sides are the same shape, so every
# stream gets the identical sweep and the only variable is what the broker
# waits for before it answers.
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
  # A concurrency point as well: `Quorum` holds a publish until a majority
  # confirms, so its cost is a queueing cost and a single in-flight publisher
  # cannot show it.
  # Keyed: unkeyed ingest lands every record on shard 0, one leader.
  run_case "${tag}-ingest-4k-c12" \
    --scenario ingest --stream "${stream}" --payload-bytes 4096 \
    --batch 64 --concurrency 12 --total 2400000 --keys "${SHARDS:-12}"
}

set_fsync() {
  echo ">> FELIX_DURABLE_FSYNC_MODE=$1 on brokers"
  for i in "${!brokers[@]}"; do
    agent_on "$(broker_vm "${i}")" "felix-agent env-set FELIX_DURABLE_FSYNC_MODE=$1 >/dev/null
felix-agent restart" | grep '^state='
  done
  sleep 8
}

echo "===== RF=3 Leader (stream perf) ====="
pass perf rf3-leader

echo "===== RF=3 Quorum (stream perf-quorum) ====="
pass perf-quorum rf3-quorum

# The durable pair, because a Quorum acknowledgement on a durable stream is the
# configuration the docs actually recommend, and it is the one nothing has ever
# measured. Left last: it is the slowest pass, so a session that runs out of
# budget still comes home with the in-memory comparison above.
# Both fsync modes: at the default Periodic, "a majority holds it durably"
# means a majority of page caches.
echo "===== RF=3 Quorum + durable, Periodic (stream perf-durable-quorum) ====="
set_fsync periodic
pass perf-durable-quorum rf3-quorum-durable-periodic
echo "===== RF=3 Quorum + durable, OnCommit (stream perf-durable-quorum) ====="
set_fsync on_commit
pass perf-durable-quorum rf3-quorum-durable-oncommit
set_fsync periodic

grep -h '^LOADGEN_JSON ' "${out}"/*.out | sed 's/^LOADGEN_JSON //' > "${out}/results.jsonl"

if [ -n "${failed_cases}" ]; then
  echo "!! cases with no result:${failed_cases}"
  exit 1
fi
echo ">> quorum matrix complete: ${out}/results.jsonl"
