#!/usr/bin/env bash
# Put NATS on a live Felix session's VMs, after the Felix runs.
#
#   SESSION=v060-a ./install.sh
#
# Broker VM: stops felix-broker (installed, not removed; uninstall.sh starts it
# again), installs nats-server and the nats CLI, makes a throwaway CA and a
# server certificate for its private IP, raises the TCP buffer limits and
# starts JetStream on /data/nats in the default durability mode. Generators:
# the nats CLI, the CA and the same TCP limits. Ends with a TLS round trip from
# every generator.
set -euo pipefail
# shellcheck source=scripts/perf/azure/nats/nats-lib.sh
source "$(cd "$(dirname "$0")" && pwd)/nats-lib.sh"

log "installing nats-server ${NATS_VERSION} on ${NATS_VM} (${NATS_IP})"
out="$(nats_agent_on "${NATS_VM}" "nats-agent install-server '${NATS_VERSION}' '${NATS_SHA256}'
nats-agent install-cli '${NATS_CLI_VERSION}' '${NATS_CLI_SHA256}'
nats-agent tls-init '${NATS_IP}'
nats-agent tune
: > /etc/nats/extra.conf
: > /etc/nats/nats.env
nats-agent reset default
nats-agent ca-get")"
printf '%s\n' "${out}" | grep -E '^(nats_|tls=|tuned=|reset=)' | sed 's/^/   /'
ca="$(printf '%s\n' "${out}" | extract_between CA)"
[ -n "${ca}" ] || { echo "!! no CA from ${NATS_VM}" >&2; exit 1; }

log "installing the nats CLI ${NATS_CLI_VERSION} on ${LOADGEN_VMS[*]}"
nats_par_on "${OUT}/system/nats" install "nats-agent install-cli '${NATS_CLI_VERSION}' '${NATS_CLI_SHA256}'
nats-agent ca-put '${ca}' '${NATS_IP}'
nats-agent tune
nats -s tls://${NATS_IP}:4222 --tlsca /etc/nats/tls/ca.pem rtt" "${LOADGEN_VMS[@]}" || {
  echo "!! a generator failed; see ${OUT}/system/nats/" >&2; exit 1; }
for lg in "${LOADGEN_VMS[@]}"; do
  printf '   %s: %s\n' "${lg}" "$(grep -E 'rtt|RTT|[0-9]+(\.[0-9]+)?(µs|us|ms)' "${OUT}/system/nats/${lg}.install.txt" | tail -1)"
done
log "NATS is up. Next: SESSION=${SESSION} ${nats_dir}/nats-cells.sh"
