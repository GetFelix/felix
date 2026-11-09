#!/usr/bin/env bash
# Fanout for a Kafka-wire system (Redpanda or Kafka): how aggregate delivery
# scales as the number of independent consumer groups on ONE topic grows. This is
# the axis to set against Felix's fanout curve — but the mechanism is the
# opposite: Felix encodes a publish once and shares it across subscribers, while
# every Kafka consumer group re-reads the log for itself. So this measures how far
# N groups re-reading the same topic scales before the brokers' read path (CPU,
# page cache, TLS) is the wall.
#
# Produce a fixed backlog once, then run N consumer groups in parallel (split
# across both client VMs), each reading the whole backlog from the start, and sum
# their throughput. TLS matches the ingest run.
set -euo pipefail
: "${SESSION:?SESSION=<name>}"
: "${SYSTEM:?SYSTEM=redpanda|kafka}"
: "${TLS:=1}"
: "${BACKLOG:=2000000}"   # records produced once; each group re-reads all of them
: "${RECORD_SIZE:=256}"
here="$(cd "$(dirname "$0")" && pwd)"
source "${here}/compare-lib.sh"
source "${here}/sessions/${SESSION}.env"
export GROUP

IFS=',' read -ra BIPS <<<"${BROKER_IPS}"
IFS=',' read -ra CIPS <<<"${CLIENT_IPS}"
BOOT="${BIPS[0]}:9092,${BIPS[1]}:9092,${BIPS[2]}:9092"
if [ "${TLS}" = "1" ]; then SUF="-tls"; else SUF=""; fi
RESULT="${here}/sessions/${SESSION}-${SYSTEM}-fanout${SUF}.jsonl"
: > "${RESULT}"
CFG=/tmp/client.properties   # written by bench-kafka-api.sh setup; TLS-aware

# Produce the backlog into a single-partition topic so every group reads the same
# one broker's log (the closest analogue to Felix's single-stream fanout).
seed_backlog() {
  echo ">> producing ${BACKLOG} x ${RECORD_SIZE}B backlog into topic fanout"
  run_on_str "$(client_vm 0)" "$(cat <<REMOTE
#!/bin/sh
K=/opt/kafka/bin; C=${CFG}
\$K/kafka-topics.sh --command-config \$C --bootstrap-server ${BOOT} --delete --topic fanout 2>/dev/null || true
sleep 3
\$K/kafka-topics.sh --command-config \$C --bootstrap-server ${BOOT} --create --topic fanout --partitions 1 --replication-factor 1 2>&1 | tail -1
\$K/kafka-producer-perf-test.sh --topic fanout --num-records ${BACKLOG} --record-size ${RECORD_SIZE} --throughput -1 \
  --producer.config \$C --producer-props acks=1 compression.type=none batch.size=1048576 linger.ms=100 2>&1 | tail -1
echo __RUNOK__
REMOTE
)" | sed -n '/stdout/,/__RUNOK__/p' | tail -3
}

# fanout_run <groups> : launch <groups> consumer-perf-test processes split over
# the two client VMs, each its own group reading the whole backlog; sum MB/s.
fanout_run() {
  g="$1"
  # split groups across the two clients
  g0=$(( (g + 1) / 2 )); g1=$(( g - g0 ))
  echo ">> fanout: ${g} consumer groups (${g0} on client-0, ${g1} on client-1)"
  read_client() {
    ci="$1"; n="$2"; base="$3"
    [ "$n" -gt 0 ] || { echo "__AGG_BEGIN__0 0__AGG_END__"; return; }
    run_on_str "$(client_vm "${ci}")" "$(cat <<REMOTE
#!/bin/sh
K=/opt/kafka/bin/kafka-consumer-perf-test.sh; C=${CFG}
D=\$(mktemp -d); i=0
while [ \$i -lt ${n} ]; do
  gid="fo-${base}-\$i-\$\$"
  \$K --topic fanout --messages ${BACKLOG} --bootstrap-server ${BOOT} --consumer.config \$C \
    --group \$gid --hide-header --timeout 120000 > \$D/c\$i.txt 2>&1 &
  i=\$((i+1))
done
wait
# kafka-consumer-perf-test CSV: start,end,data.consumed.MB,MB.sec,data.consumed.nMsg,nMsg.sec,...
awk -F, '/[0-9]/ && NF>=6 { mb+=\$4; n++ } END { printf "__AGG_BEGIN__%d %.1f__AGG_END__\n", n, mb }' \$D/c*.txt
rm -rf \$D
echo __RUNOK__
REMOTE
)"
  }
  o0="$(read_client 0 "$g0" a)"; o1="$(read_client 1 "$g1" b)"
  a0="$(printf '%s' "$o0" | extract_between AGG)"; a1="$(printf '%s' "$o1" | extract_between AGG)"
  set -- ${a0}; n0="${1:-0}"; m0="${2:-0}"
  set -- ${a1}; n1="${1:-0}"; m1="${2:-0}"
  groups_ok=$(( n0 + n1 ))
  total_mb=$(awk "BEGIN{printf \"%.1f\", ${m0} + ${m1}}")
  printf '{"system":"%s","tls":%s,"scenario":"fanout","record_size":%s,"backlog":%s,"consumer_groups":%s,"groups_measured":%s,"aggregate_mb_per_sec":%s}\n' \
    "${SYSTEM}" "${TLS}" "${RECORD_SIZE}" "${BACKLOG}" "${g}" "${groups_ok}" "${total_mb}" | tee -a "${RESULT}"
}

seed_backlog
for g in 1 5 10 25 50 100; do fanout_run "${g}"; done
echo ">> done. Results: ${RESULT}"
cat "${RESULT}"
