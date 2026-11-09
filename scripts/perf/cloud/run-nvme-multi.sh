#!/usr/bin/env bash
# Multi-loadgen aggregate ingest: a single D4 loadgen is crypto-bound at ~1.15
# GB/s, so drive several loadgens at once to push the brokers to their CPU
# ceiling and read MB/s per vCPU (the number that survives a disk-bound
# comparison). Distributes the client token to the extra loadgens, runs the
# ingest scenario on all of them in parallel against the durable stream, samples
# every broker's CPU breakdown at the peak, and sums throughput. Captures full
# per-loadgen output so a failed loadgen is visible, not silently dropped.
set -uo pipefail
: "${SESSION:?SESSION=<name>}"
: "${STREAM:=perf-durable}"
: "${CONC:=16}"
# Routing keys to spread batches over. 0 is unkeyed, which lands every record
# on shard 0 however many shards the stream has -- so a multi-shard stream is
# only actually exercised when this is set.
: "${KEYS:=0}"
# QUIC connections each loadgen opens for publishing. The client default is 4,
# so `--concurrency 16` is 16 publishers multiplexed over 4 connections -- and a
# connection is one driver task doing that connection's packet processing and
# AEAD on the broker. Sharding cannot add drivers, so this is the knob that
# moves a receive-path ceiling when shard count does not.
: "${PUB_CONNS:=}"
# How the client picks which publish stream a batch goes down. The default is
# `hash_stream`, which sends every publish for one stream down ONE worker -- one
# QUIC stream on one connection -- however many connections the pool opened.
# `rr` spreads them across the pool. See #552.
: "${PUB_SHARDING:=}"
# Sourcing the inventory below would clobber a LOADGENS given on the command
# line, and sweeping the generator count is the whole point of this script --
# one, two, then three against the same broker. Remember the caller's value and
# put it back afterwards, so an explicit LOADGENS always wins over the
# inventory's full list.
_loadgens_override="${LOADGENS:-}"
here="$(cd "$(dirname "$0")" && pwd)"
source "${here}/lib.sh"
source "${here}/sessions/${SESSION}.env"
LOADGENS="${_loadgens_override:-${LOADGENS:-felixperf-loadgen}}"
echo ">> generators: ${LOADGENS}"
export GROUP="${GROUP}"

IFS=',' read -ra brokers <<<"${BROKER_IPS}"

# Broker CPU, sampled properly.
#
# The previous version ran `top` mid-run through `az vm run-command`, which has
# seconds of dispatch latency -- often landing after a ~26s case had finished,
# and reporting an idle machine. It once recorded the *only* broker doing any
# writing (14.3M appends) as 100% idle, which is worse than no number at all.
#
# Instead: arm a sampler on every broker before the load starts, difference
# /proc/stat at 1 Hz, and afterwards average only the samples that were
# actually busy. That keeps the idle head and tail out of the mean, and it is
# the method that produced the clean 50-51% reading behind #535. `top` also
# cannot be trusted for softirq, which is ~10% of a loaded broker (QUIC/UDP and
# AEAD) and the difference between "CPU bound" and "waiting".
arm_cpu_sampling() {
  for i in "${!brokers[@]}"; do
    run_on_str "$(broker_vm "${i}")" "rm -f /tmp/cpu-samples.txt
nohup sh -c 'i=0; while [ \$i -lt 600 ]; do awk \"/^cpu /{print \\\$2,\\\$3,\\\$4,\\\$5,\\\$6,\\\$7,\\\$8}\" /proc/stat >> /tmp/cpu-samples.txt; i=\$((i+1)); sleep 1; done' >/dev/null 2>&1 &
echo __RUNOK__" >/dev/null 2>&1 || true
  done
}

# The same sampler on the generators. Without it a run where the generators are
# saturated and the broker is at 55% reads as a broker result, which is how six
# configurations in a row produced the same number (#552).
arm_loadgen_cpu_sampling() {
  for lg in ${LOADGENS}; do
    run_on_str "$lg" "rm -f /tmp/cpu-samples.txt
nohup sh -c 'i=0; while [ \$i -lt 600 ]; do awk \"/^cpu /{print \\\$2,\\\$3,\\\$4,\\\$5,\\\$6,\\\$7,\\\$8}\" /proc/stat >> /tmp/cpu-samples.txt; i=\$((i+1)); sleep 1; done' >/dev/null 2>&1 &
echo __RUNOK__" >/dev/null 2>&1 || true
  done
}

report_loadgen_cpu() {
  for lg in ${LOADGENS}; do
    line=$(run_on_str "$lg" "awk 'NR>1{du=\$1-pu;dn=\$2-pn;ds=\$3-ps;di=\$4-pi;dw=\$5-pw;dq=\$6-pq;dsq=\$7-psq;tot=du+dn+ds+di+dw+dq+dsq; if(tot>0){b=100*(tot-di)/tot; if(b>15){n++;B+=b;U+=100*du/tot;S+=100*ds/tot;I+=100*dsq/tot}}} {pu=\$1;pn=\$2;ps=\$3;pi=\$4;pw=\$5;pq=\$6;psq=\$7} END{if(n>0) printf \"busy=%.0f%% us=%.0f sy=%.0f si=%.0f (%d busy samples)\\n\", B/n,U/n,S/n,I/n,n; else print \"no busy samples\"}' /tmp/cpu-samples.txt
echo __RUNOK__" 2>/dev/null | grep -E "busy=|no busy samples" | head -1 || true)
    echo "      ${lg}: ${line:-n/a}"
  done
}

report_cpu() {
  for i in "${!brokers[@]}"; do
    line=$(run_on_str "$(broker_vm "${i}")" "awk 'NR>1{du=\$1-pu;dn=\$2-pn;ds=\$3-ps;di=\$4-pi;dw=\$5-pw;dq=\$6-pq;dsq=\$7-psq;tot=du+dn+ds+di+dw+dq+dsq; if(tot>0){b=100*(tot-di)/tot; if(b>15){n++;B+=b;U+=100*du/tot;S+=100*ds/tot;I+=100*dsq/tot;W+=100*dw/tot}}} {pu=\$1;pn=\$2;ps=\$3;pi=\$4;pw=\$5;pq=\$6;psq=\$7} END{if(n>0) printf \"busy=%.0f%% us=%.0f sy=%.0f si=%.0f wa=%.0f (%d busy samples)\\n\", B/n,U/n,S/n,I/n,W/n,n; else print \"no busy samples\"}' /tmp/cpu-samples.txt
echo __RUNOK__" 2>/dev/null | grep -E "busy=|no busy samples" | head -1 || true)
    echo "      broker-${i}: ${line:-n/a}"
  done
}

broker_addrs="${BROKER_ADDRS:-$(printf "%s:5000," "${brokers[@]}")}"; broker_addrs="${broker_addrs%,}"
TOKEN_FILE="/home/felix/felix-session/token"

echo ">> distributing the client token"
TOK_B64="$(run_on_str felixperf-loadgen "base64 -w0 < ${TOKEN_FILE}; echo" 2>/dev/null | grep -E '^[A-Za-z0-9+/=]{200,}$' | head -1 || true)"
[ -n "${TOK_B64}" ] || { echo "!! could not read the client token from loadgen-0" >&2; exit 1; }
for lg in ${LOADGENS}; do
  [ "$lg" = "felixperf-loadgen" ] && continue
  run_on_str "$lg" "mkdir -p /home/felix/felix-session
printf '%s' '${TOK_B64}' | base64 -d > ${TOKEN_FILE}
chown -R felix:felix /home/felix/felix-session
test -s ${TOKEN_FILE} && echo 'token written' || echo 'TOKEN EMPTY'
echo __RUNOK__" 2>/dev/null | grep -E 'token written|TOKEN EMPTY' | head -1 | sed "s/^/   $lg: /"
done

run_agg() {
  conc="$1"; total=$(( conc * 400000 ))
  ncount=$(echo ${LOADGENS} | wc -w)
  echo ">> aggregate ingest: ${conc} pubs/loadgen x ${ncount} loadgens = $(( conc * ncount )) publishers, stream=${STREAM}, keys=${KEYS}, conns/loadgen=${PUB_CONNS:-default(4)}, sharding=${PUB_SHARDING:-default(hash_stream)}"
  rm -f /tmp/agg-felixperf-loadgen*.txt
  # Armed before the load, not dispatched into it: run-command takes seconds to
  # reach a VM, which is a large fraction of a case and the reason the old
  # mid-run sample so often caught an idle machine.
  arm_cpu_sampling
  arm_loadgen_cpu_sampling
  pids=""
  for lg in ${LOADGENS}; do
    ( run_on_str "$lg" "export FELIX_MTU_UPPER_BOUND=4096; ${PUB_CONNS:+export FELIX_PUB_CONN_POOL=${PUB_CONNS};} ${PUB_SHARDING:+export FELIX_PUB_SHARDING=${PUB_SHARDING};} ulimit -n 1048576 || true
felix-loadgen --brokers '${broker_addrs}' --tenant perf --token-file '${TOKEN_FILE}' --environment 'azure-nvme-multi' --scenario ingest --stream '${STREAM}' --payload-bytes 4096 --batch 64 --concurrency ${conc} --total ${total} --keys ${KEYS}
echo __RUNOK__" > "/tmp/agg-${lg}.txt" 2>&1 ) &
    pids="$pids $!"
  done
  wait $pids 2>/dev/null || true
  echo "   -- broker CPU at load --"
  report_cpu
  echo "   -- generator CPU at load --"
  report_loadgen_cpu
  echo "   -- per-loadgen results --"
  total_mb=0; total_ms=0; n=0
  for lg in ${LOADGENS}; do
    line=$(grep '^ingest:' "/tmp/agg-${lg}.txt" 2>/dev/null | head -1 || true)
    if [ -n "$line" ]; then
      echo "      ${lg}: ${line}"
      mb=$(echo "$line" | grep -oE '[0-9.]+ MB/s' | grep -oE '[0-9.]+' | head -1)
      ms=$(echo "$line" | grep -oE '[0-9]+ msg/s' | grep -oE '[0-9]+' | head -1)
      total_mb=$(awk "BEGIN{printf \"%.1f\", ${total_mb} + ${mb:-0}}")
      total_ms=$(awk "BEGIN{printf \"%.0f\", ${total_ms} + ${ms:-0}}")
      n=$((n+1))
    else
      echo "      ${lg}: NO RESULT -- tail:"; tail -4 "/tmp/agg-${lg}.txt" 2>/dev/null | sed 's/^/         /'
    fi
  done
  echo "   AGGREGATE: ${total_ms} msg/s, ${total_mb} MB/s across ${n}/${ncount} loadgens"
  rm -f /tmp/agg-felixperf-loadgen*.txt
}

run_agg "${CONC}"
