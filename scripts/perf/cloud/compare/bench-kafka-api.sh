#!/usr/bin/env bash
# Benchmark a Kafka-wire system (Redpanda or Kafka) with the workload matched to
# the Felix ingest run: 4 KiB and 256 B records, rf=1, 24 partitions across 3
# brokers, acks=1 (leader-ack — the analog of Felix's in-memory acked path at
# rf=1). The instrument is Kafka's own kafka-producer-perf-test, the way
# felix-loadgen is Felix's — each system measured by its blessed tool, same
# workload.
#
# TLS=1 runs the kafka API over TLS (security.protocol=SSL, CA at /opt/certs/ca.crt).
# This is the FAIR configuration: Felix's QUIC/TLS crypto is always on and is the
# measured broker-CPU cost, so the comparable Kafka/Redpanda number must also pay
# TLS. TLS=0 is kept as the plaintext baseline that quantifies what the crypto
# path costs.
#
# Matched producer knobs (documented so the result is reproducible, not just
# quotable): compression off (Felix does not compress); throughput profile uses
# batch.size=1 MiB linger.ms=100 (matches Felix's delivery-batch throughput
# profile); latency profile uses linger.ms=0 max.in.flight=1 (Felix low-latency
# profile). Results -> sessions/<name>-<system>[-tls].jsonl.
set -euo pipefail
: "${SESSION:?SESSION=<name>}"
: "${SYSTEM:?SYSTEM=redpanda|kafka}"
: "${KAFKA_VER:=3.9.1}"
: "${TLS:=0}"
here="$(cd "$(dirname "$0")" && pwd)"
source "${here}/compare-lib.sh"
source "${here}/sessions/${SESSION}.env"
export GROUP

IFS=',' read -ra BIPS <<<"${BROKER_IPS}"
IFS=',' read -ra CIPS <<<"${CLIENT_IPS}"
BOOT="${BIPS[0]}:9092,${BIPS[1]}:9092,${BIPS[2]}:9092"
if [ "${TLS}" = "1" ]; then SUF="-tls"; else SUF=""; fi
RESULT="${here}/sessions/${SESSION}-${SYSTEM}${SUF}.jsonl"
: > "${RESULT}"

# The client properties file every kafka CLI call reads: bootstrap + (if TLS) the
# SSL block. Written to /tmp/client.properties on each client VM.
client_props() {
  if [ "${TLS}" = "1" ]; then
    printf 'bootstrap.servers=%s\nsecurity.protocol=SSL\nssl.truststore.type=PEM\nssl.truststore.location=/opt/certs/ca.crt\nssl.endpoint.identification.algorithm=https\n' "${BOOT}"
  else
    printf 'bootstrap.servers=%s\n' "${BOOT}"
  fi
}
PROPS_B64="$(client_props | base64 | tr -d '\n')"

setup_clients() {
  for i in "${!CIPS[@]}"; do
    echo ">> client-${i}: kafka CLI + props (TLS=${TLS})"
    run_on_str "$(client_vm "${i}")" "$(cat <<REMOTE
#!/bin/sh
set -e
if [ ! -x /opt/kafka/bin/kafka-producer-perf-test.sh ]; then
  cd /opt
  curl -1sLf -o kafka.tgz https://archive.apache.org/dist/kafka/${KAFKA_VER}/kafka_2.13-${KAFKA_VER}.tgz
  tar xzf kafka.tgz && rm -f kafka.tgz
  ln -sfn /opt/kafka_2.13-${KAFKA_VER} /opt/kafka
fi
echo '${PROPS_B64}' | base64 -d > /tmp/client.properties
echo __RUNOK__
REMOTE
)" | tail -1
  done
}

recreate_topic() {
  parts="$1"
  run_on_str "$(client_vm 0)" "$(cat <<REMOTE
#!/bin/sh
K=/opt/kafka/bin; C=/tmp/client.properties
\$K/kafka-topics.sh --command-config \$C --bootstrap-server ${BOOT} --delete --topic bench 2>/dev/null || true
sleep 3
\$K/kafka-topics.sh --command-config \$C --bootstrap-server ${BOOT} --create --topic bench \
  --partitions ${parts} --replication-factor 1 2>&1 | tail -1
echo __RUNOK__
REMOTE
)" | tail -1
}

# throughput_run <record_size> <producers> <records_per_producer> <acks> <client_idx> <label>
throughput_run() {
  rs="$1"; p="$2"; npp="$3"; acks="$4"; ci="$5"; label="$6"
  echo ">> throughput: rs=${rs} producers=${p} acks=${acks} client=${ci} (${label})"
  out="$(run_on_str "$(client_vm "${ci}")" "$(cat <<REMOTE
#!/bin/sh
K=/opt/kafka/bin/kafka-producer-perf-test.sh; C=/tmp/client.properties
D=\$(mktemp -d)
i=0
while [ \$i -lt ${p} ]; do
  \$K --topic bench --num-records ${npp} --record-size ${rs} --throughput -1 \
    --producer.config \$C \
    --producer-props acks=${acks} compression.type=none \
    batch.size=1048576 linger.ms=100 buffer.memory=268435456 \
    max.in.flight.requests.per.connection=5 > \$D/p\$i.txt 2>&1 &
  i=\$((i+1))
done
wait
awk '/records sent/ {
  for (j=1;j<=NF;j++){ if (\$j ~ /records\/sec/){ rps+=\$(j-1) }; if (\$j ~ /MB\/sec/){ m=\$(j-1); sub(/\(/,"",m); mb+=m } }
  n++
} END { printf "__AGG_BEGIN__%d %.1f %.1f__AGG_END__\n", n, rps, mb }' \$D/p*.txt
grep -h 'records sent' \$D/p*.txt | head -1
rm -rf \$D
echo __RUNOK__
REMOTE
)")"
  agg="$(printf '%s' "${out}" | extract_between AGG)"
  set -- ${agg}
  nprod="${1:-0}"; rps="${2:-0}"; mbs="${3:-0}"
  printf '{"system":"%s","tls":%s,"scenario":"ingest","record_size":%s,"producers":%s,"acks":%s,"client":%s,"label":"%s","records_per_sec":%s,"mb_per_sec":%s}\n' \
    "${SYSTEM}" "${TLS}" "${rs}" "${nprod}" "${acks}" "${ci}" "${label}" "${rps}" "${mbs}" | tee -a "${RESULT}"
}

latency_run() {
  rs="$1"; tgt="$2"; acks="$3"
  echo ">> latency: rs=${rs} target=${tgt}/s acks=${acks}"
  out="$(run_on_str "$(client_vm 0)" "$(cat <<REMOTE
#!/bin/sh
K=/opt/kafka/bin/kafka-producer-perf-test.sh; C=/tmp/client.properties
\$K --topic bench --num-records \$(( ${tgt} * 20 )) --record-size ${rs} --throughput ${tgt} \
  --producer.config \$C \
  --producer-props acks=${acks} compression.type=none linger.ms=0 batch.size=16384 \
  max.in.flight.requests.per.connection=1 2>&1 | tail -2 | head -1
echo __RUNOK__
REMOTE
)")"
  line="$(printf '%s' "${out}" | grep 'records sent' | tail -1)"
  echo "   ${line}"
  printf '{"system":"%s","tls":%s,"scenario":"latency","record_size":%s,"target_rps":%s,"acks":%s,"raw":"%s"}\n' \
    "${SYSTEM}" "${TLS}" "${rs}" "${tgt}" "${acks}" "$(printf '%s' "${line}" | sed 's/"/\\"/g')" >> "${RESULT}"
}

setup_clients
recreate_topic 24

# Single-client throughput ramp: 4 KiB then 256 B, acks=1.
for p in 1 2 4 8 16; do throughput_run 4096 "${p}" 800000 1 0 "single-client"; recreate_topic 24; done
for p in 4 8 16 32; do throughput_run 256  "${p}" 8000000 1 0 "single-client"; recreate_topic 24; done

# Latency: produce->ack at modest bounded rates.
latency_run 256 50000 1
latency_run 1024 50000 1
latency_run 4096 50000 1

echo ">> done. Results: ${RESULT}"
cat "${RESULT}"
