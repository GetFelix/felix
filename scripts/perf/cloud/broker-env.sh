#!/usr/bin/env bash
# Change broker knobs or the active build on a live session, then restart and
# wait for every broker to report /ready.
#
#   SESSION=<name> ./broker-env.sh show
#   SESSION=<name> ./broker-env.sh set FELIX_QUIC_LISTENERS=4 FELIX_STORAGE_IO_URING=0
#   SESSION=<name> ./broker-env.sh unset FELIX_BROKER_PUB_FLUSH_CONCURRENCY
#   SESSION=<name> ./broker-env.sh reset            # back to the calibrated base
#   SESSION=<name> ./broker-env.sh activate <label> # switch build (see BROKER_LABELS)
#
# Knobs live in /etc/felix/overrides.env, a second EnvironmentFile that wins
# over the regenerated broker.env. BROKERS="0 2" limits a change to those.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=cells.sh
source "${here}/cells.sh"

action="${1:?usage: broker-env.sh show|set|unset|reset|activate [...]}"
shift
targets=()
if [ -n "${BROKERS:-}" ]; then
  for i in ${BROKERS}; do targets+=("$(broker_vm "${i}")"); done
else
  targets=("${BROKER_VMS[@]}")
fi
dir="${OUT}/system/broker-env"

apply() {
  par_on "${dir}" last "$1" "${targets[@]}" || { cat "${dir}"/*.last.txt >&2; exit 1; }
  for vm in "${targets[@]}"; do
    echo "== ${vm}"
    grep -E '^(overrides|active)\.|^state=' "${dir}/${vm}.last.txt" | sed 's/^/   /' || true
  done
  wait_ready && log "all brokers ready"
}

case "${action}" in
  show)
    par_on "${dir}" show "cat /etc/felix/overrides.env | sed 's/^/overrides./'
felix-agent snapshot --env" "${targets[@]}"
    for vm in "${targets[@]}"; do
      echo "== ${vm}"
      grep -E '^(overrides\.|bin\.broker\.ref|listen\.ports|env\.FELIX_)' "${dir}/${vm}.show.txt" | sed 's/^/   /'
    done
    ;;
  set) apply "felix-agent env-set $*
felix-agent restart" ;;
  unset) apply "felix-agent env-unset $*
felix-agent restart" ;;
  reset) apply "felix-agent env-replace <<'OVR'
$(base_overrides)
OVR
felix-agent restart" ;;
  activate)
    label="${1:?activate <label>}"
    apply "felix-agent activate broker '${label}'
felix-agent restart"
    ;;
  *) echo "!! unknown action ${action}" >&2; exit 2 ;;
esac
