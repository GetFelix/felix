#!/usr/bin/env bash
# Shared by the NATS comparison scripts. Sourced, never run.
#
# Runs on a live Felix session's VMs (session A: felixperf-broker-0 and the
# four generators), reusing cells.sh for the session inventory, the result
# layout, the sampler series fetch and the summary, so NATS cells land beside
# the Felix cells and summarize.py reads both.

nats_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
here="$(cd "${nats_dir}/.." && pwd)"
# shellcheck source=scripts/perf/azure/cells.sh
source "${here}/cells.sh"

# Pinned releases, checked against the SHA256SUMS published with each.
# https://github.com/nats-io/nats-server/releases/tag/v2.15.0
# https://github.com/nats-io/natscli/releases/tag/v0.5.0
: "${NATS_VERSION:=2.15.0}"
: "${NATS_SHA256:=5d2c51caca950333aba84911df7d377f826f3a59ec36061c6539105084f65c92}"
: "${NATS_CLI_VERSION:=0.5.0}"
: "${NATS_CLI_SHA256:=d9fc93e9e9ab0310deff7e719a8a0da6f9d66f26d473f039aaee70155507aecc}"

[ "${#BROKER_VMS[@]}" -ge 1 ] || { echo "!! the session has no broker" >&2; return 1; }
[ "${#BROKER_VMS[@]}" -eq 1 ] || echo "!! ${#BROKER_VMS[@]} brokers; NATS runs as one server on ${BROKER_VMS[0]}" >&2
# shellcheck disable=SC2034 # read by the scripts that source this
NATS_VM="${BROKER_VMS[0]}"
# shellcheck disable=SC2034
NATS_IP="${BROKER_IP_LIST[0]}"

# nats_agent_on <vm> <commands>: agent_on with nats-agent installed too.
nats_agent_on() {
  run_on_str "$1" "set -e
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
cat > /usr/local/sbin/felix-agent <<'FELIX_AGENT_EOF'
$(cat "${here}/remote/felix-agent.sh")
FELIX_AGENT_EOF
cat > /usr/local/sbin/nats-agent <<'NATS_AGENT_EOF'
$(cat "${nats_dir}/remote/nats-agent.sh")
NATS_AGENT_EOF
chmod 0755 /usr/local/sbin/felix-agent /usr/local/sbin/nats-agent
$2
echo __RUNOK__"
}

# nats_par_on <dir> <suffix> <commands> <vm>...: par_on for nats_agent_on.
nats_par_on() {
  local dir="$1" suffix="$2" cmds="$3" vm p rc=0
  shift 3
  local pids=()
  mkdir -p "${dir}"
  for vm in "$@"; do
    ( nats_agent_on "${vm}" "${cmds}" > "${dir}/${vm}.${suffix}.txt" 2>&1 ) &
    pids+=("$!")
  done
  for p in ${pids[@]+"${pids[@]}"}; do wait "${p}" || rc=1; done
  return "${rc}"
}
