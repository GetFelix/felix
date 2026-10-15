#!/usr/bin/env bash
# Take NATS back off the session and give the broker VM back to Felix.
#
#   SESSION=v060-a ./uninstall.sh
#
# Broker: stops and disables nats, deletes /data/nats, restores the TCP
# sysctls and starts felix-broker, then waits for its /ready. Generators: the
# TCP sysctls are restored; the nats CLI binary is left in /opt/nats.
set -euo pipefail
# shellcheck source=scripts/perf/azure/nats/nats-lib.sh
source "$(cd "$(dirname "$0")" && pwd)/nats-lib.sh"

log "removing NATS from ${NATS_VM}"
nats_agent_on "${NATS_VM}" "nats-agent uninstall" | grep -E '^(untuned|felix_broker)=' | sed 's/^/   /'
nats_par_on "${OUT}/system/nats" uninstall "nats-agent untune" "${LOADGEN_VMS[@]}" || true
if wait_ready; then
  log "felix-broker is ready again"
else
  echo "!! felix-broker did not report ready; check it with broker-env.sh show" >&2
  exit 1
fi
