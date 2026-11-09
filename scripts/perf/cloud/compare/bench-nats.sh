#!/usr/bin/env bash
# Benchmark NATS JetStream with the workload matched to the Felix ingest run:
# 4 KiB and 256 B records, rf=1 (replicas=1), data spread across the 3 nodes the
# way Felix shards across 3 brokers — here as 3 R=1 streams, one leader per node.
# The instrument is NATS's own `nats bench`, JetStream mode (publish waits for the
# stream ack — the analog of Felix's acked, durable publish).
#
# Throughput: parallel `nats bench` publishers across the 3 streams, aggregated.
# Results land in sessions/<name>-nats.jsonl.
set -euo pipefail
: "${SESSION:?SESSION=<name>}"
here="$(cd "$(dirname "$0")" && pwd)"
source "${here}/compare-lib.sh"
source "${here}/sessions/${SESSION}.env"
export GROUP

IFS=',' read -ra BIPS <<<"${BROKER_IPS}"
IFS=',' read -ra CIPS <<<"${CLIENT_IPS}"
SRV="${BIPS[0]}:4222,${BIPS[1]}:4222,${BIPS[2]}:4222"
RESULT="${here}/sessions/${SESSION}-nats.jsonl"
: > "${RESULT}"

# Create 3 R=1 file streams; NATS balances the single replica of each across the
# 3 nodes, so all three do work (verify with `nats stream report`).
setup_streams() {
  run_on_str "$(client_vm 0)" "$(cat <<REMOTE
#!/bin/sh
for n in 0 1 2; do
  nats --server ${SRV} stream rm bench\$n -f 2>/dev/null || true
  nats --server ${SRV} stream add bench\$n \
    --subjects "bench.\$n" --storage file --replicas 1 \
    --retention limits --max-bytes 30GB --max-age 0 --max-msgs=-1 \
    --discard old --dupe-window 0s --no-allow-rollup --deny-delete --deny-purge=false \
    --defaults 2>&1 | tail -1 || true
done
nats --server ${SRV} stream report 2>&1 | head -20
echo __RUNOK__
REMOTE
)" | tail -12
}

# throughput_run <record_size> <pub_per_stream> <msgs_per_pub> <label>
throughput_run() {
  rs="$1"; pp="$2"; mpp="$3"; label="$4"
  echo ">> nats throughput: rs=${rs} pub/stream=${pp} (${label})"
  out="$(run_on_str "$(client_vm 0)" "$(cat <<REMOTE
#!/bin/sh
D=\$(mktemp -d)
for n in 0 1 2; do
  nats --server ${SRV} bench "bench.\$n" --js --stream bench\$n --pub ${pp} \
    --size ${rs} --msgs ${mpp} --no-progress > \$D/s\$n.txt 2>&1 &
done
wait
# nats bench prints a "Pub stats:" line with an aggregate msgs/sec figure and a
# MB/sec figure per run; sum across the 3 streams.
awk '/Pub stats:/ {
  for (j=1;j<=NF;j++){
    if (\$j ~ /msgs\/sec/){ g=\$(j-1); gsub(/[,~]/,"",g); rps+=g }
  }
  # size line often carries MB/sec in the same block; capture from the msgs/sec unit
}
/[0-9.]+ (KB|MB|GB)\/sec/ {
  for (j=1;j<=NF;j++){
    if (\$j ~ /MB\/sec/){ v=\$(j-1); gsub(/[,~]/,"",v); mb+=v }
    if (\$j ~ /GB\/sec/){ v=\$(j-1); gsub(/[,~]/,"",v); mb+=v*1024 }
    if (\$j ~ /KB\/sec/){ v=\$(j-1); gsub(/[,~]/,"",v); mb+=v/1024 }
  }
} END { printf "__AGG_BEGIN__%.0f %.1f__AGG_END__\n", rps, mb }' \$D/s*.txt
echo "----RAW----"
grep -H 'Pub stats' \$D/s*.txt
rm -rf \$D
echo __RUNOK__
REMOTE
)")"
  echo "${out}" | sed -n '/----RAW----/,/__RUNOK__/p' | head -6
  agg="$(printf '%s' "${out}" | extract_between AGG)"
  set -- ${agg}
  rps="${1:-0}"; mbs="${2:-0}"
  printf '{"system":"nats","scenario":"ingest","record_size":%s,"pub_per_stream":%s,"streams":3,"label":"%s","records_per_sec":%s,"mb_per_sec":%s}\n' \
    "${rs}" "${pp}" "${label}" "${rps}" "${mbs}" | tee -a "${RESULT}"
}

setup_streams
for pp in 1 2 4 8; do throughput_run 4096 "${pp}" 2000000 "single-client"; done
for pp in 2 4 8; do throughput_run 256 "${pp}" 8000000 "single-client"; done

echo ">> done. Results: ${RESULT}"
cat "${RESULT}"
