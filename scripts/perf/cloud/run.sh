#!/usr/bin/env bash
# Drive the matrix on the load generator and bring the data home — every case
# dispatched with `az vm run-command` (never SSH; see lib.sh). felix-loadgen
# runs on the loadgen; the operator only shuttles arguments in and JSON out.
#
# The machines stay otherwise idle for the duration — the same hygiene as a
# local run, enforced by construction. Results land in
# sessions/<name>-results/ as one JSONL of LOADGEN_JSON lines plus a
# session.json of environment metadata, which is what makes a number
# publishable: every row says which tier, SKUs, release, and RTT baseline
# produced it.
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

# felix-loadgen runs as root under run-command, so ~ would be root's home; the
# token the seed wrote lives under the felix user. Use the absolute path.
TOKEN_FILE="/home/felix/felix-session/token"

run_case() {
  local label="$1"; shift
  echo ">> ${label}"
  # run-command's returned message is size-capped and keeps only the tail, so
  # the instrument's stderr progress is sent to a file on the VM and never into
  # the message — leaving the compact stdout (the Results block ending in the
  # LOADGEN_JSON line) to come back whole. On failure the stderr tail is
  # surfaced so the .out shows why.
  # FELIX_MTU_UPPER_BOUND matches the brokers: keep the client's MTU discovery
  # in the same Linux-safe, fast-converging band (see broker.yaml). It is above
  # Azure's <=1500 path, so it never caps the achievable MTU.
  # A single case failing must NOT abort the matrix — a run of many cases is
  # hours of paid cluster, and a lone transient (or a scenario that genuinely
  # cannot complete) is a data point, not a reason to lose every case already
  # collected. The failure is recorded in the case's .out and the loop goes on;
  # a missing LOADGEN_JSON line simply contributes nothing to results.jsonl.
  if run_on_str "$(loadgen_vm)" "set -eu
export FELIX_MTU_UPPER_BOUND=4096
# High-fanout watch/subscribe cases open one UDP socket per watcher; give the
# instrument headroom above the default 1024 fd limit (watch fanout 500 hit
# 'Too many open files' otherwise). The broker unit sets its own LimitNOFILE.
ulimit -n 65536 || true
felix-loadgen --brokers '${broker_addrs}' --tenant perf --token-file '${TOKEN_FILE}' --environment '${CLOUD:-azure}-${TIER}' $* > /tmp/felix-case.out 2>/tmp/felix-case.err || { echo '!! case failed'; tail -30 /tmp/felix-case.err; exit 1; }
cat /tmp/felix-case.out
echo __RUNOK__" | tee "${out}/${label}.out"; then
    :
  else
    echo "!! case ${label} did not complete; recorded and continuing" | tee -a "${out}/${label}.out"
    failed_cases="${failed_cases} ${label}"
  fi
}

# RTT baselines first: the number every latency below is read against. Measured
# on the loadgen, broker by broker.
echo ">> network baselines"
rtt_out="$(run_on_str "$(loadgen_vm)" "set -eu
for ip in ${brokers[*]}; do
  rtt=\$(ping -qc 10 \"\$ip\" | awk -F/ 'END{print \$5}')
  printf '__RTT_BEGIN__%s=%s__RTT_END__\n' \"\$ip\" \"\${rtt:-null}\"
done
echo __RUNOK__")"
{
  echo "{"
  echo "  \"session\": \"${SESSION}\", \"tier\": \"${TIER}\","
  echo "  \"release\": \"${RELEASE_TAG}\","
  echo "  \"rtt_ms\": {"
  first=true
  while IFS='=' read -r ip rtt; do
    [ -n "${ip}" ] || continue
    $first || echo ","; first=false
    printf '    "%s": %s' "${ip}" "${rtt:-null}"
  done < <(printf '%s\n' "${rtt_out}" | sed -n 's/.*__RTT_BEGIN__\(.*\)__RTT_END__.*/\1/p')
  echo ""
  echo "  }"
  echo "}"
} > "${out}/session.json"
cat "${out}/session.json"

# --- The matrix. Latency cases are batch 1 with per-message acks (the
# request-latency configuration); throughput cases are batch 64, sized so the
# volume comfortably exceeds the QUIC send window.
for payload in 0 256 4096; do
  for fanout in 1 10 50; do
    run_case "pubsub-lat-p${payload}-f${fanout}" \
      --scenario pubsub --stream perf --payload-bytes "${payload}" \
      --fanout "${fanout}" --batch 1 --warmup 2000 --total 20000
  done
  run_case "pubsub-tp-p${payload}" \
    --scenario pubsub --stream perf --payload-bytes "${payload}" \
    --fanout 10 --batch 64 --binary --warmup 2000 --total 500000
done

run_case "cache-p256" --scenario cache --cache perf --payload-bytes 256 --concurrency 8 --total 20000
run_case "counter" --scenario counter --cache perf --concurrency 8 --total 20000

for watchers in 1 50 500; do
  run_case "watch-f${watchers}" \
    --scenario watch --cache perf --fanout "${watchers}" \
    --payload-bytes 256 --warmup 200 --total 2000
done

# Queues. The drain is the measurement, so the enqueue is not timed: the
# scenario publishes and acks the whole run first, then polls it through one
# consumer group. Redeliveries are reported beside the throughput rather than
# folded into it -- a drain that retried its way to the finish is a different
# number from one that did not.
run_case "queue-p256" --scenario queue --stream perf --payload-bytes 256 --warmup 2000 --total 20000

# Retained join: time-to-complete-state against roster size. Three points
# because one cannot show a curve, and the curve is the question -- an
# application joining a room wants to know how long before it holds the whole
# roster, and whether that grows with the roster or with something worse.
for roster in 100 1000 10000; do
  run_case "retained-r${roster}" \
    --scenario retained --cache perf --payload-bytes 256 --total "${roster}"
done

# One artifact the analysis reads directly.
grep -h '^LOADGEN_JSON ' "${out}"/*.out | sed 's/^LOADGEN_JSON //' > "${out}/results.jsonl"
echo ">> $(wc -l < "${out}/results.jsonl") result rows in ${out}/results.jsonl"
[ -n "${failed_cases}" ] && echo ">> cases that did not complete:${failed_cases}"
echo ">> now: SESSION=${SESSION} ${here}/teardown.sh"
