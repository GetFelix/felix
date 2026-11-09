#!/usr/bin/env bash
# Shared plumbing for the session scripts. The one idea here: the operator
# never SSHes into the session. Every operator->VM interaction goes through
# `az vm run-command invoke`, which rides the Azure control plane over HTTPS.
#
# Why not SSH? Two reasons the first live run found the hard way:
#   1. Some operator networks deep-packet-inspect and RST outbound :22 to
#      arbitrary cloud IPs (GitHub's is whitelisted; a fresh Azure VM is not),
#      so `ssh felix@<vm>` resets at key exchange with nothing wrong on the VM.
#   2. Ubuntu 24.04's socket-activated sshd needs /run/sshd, which is not
#      always present on first boot — a second, unrelated way in to fail.
# run-command sidesteps both, needs no inbound :22 at all, and reaches the
# VNet-private control plane by running ON a VM that is already inside the VNet.
#
# Scripts shipped to run-command execute under **dash as root**, so everything
# in cmd_* heredocs must be POSIX sh: no `set -o pipefail`, no arrays, no
# `[[ ]]`. Each remote script ends by printing __RUNOK__; run_on treats its
# absence as failure, because run-command reports the *delivery* as succeeded
# even when the script itself exits non-zero.

# The provider layer. CLOUD=azure|gcp (default azure) picks provider/<cloud>.sh,
# which defines every provider_* function the harness calls. On GCP the same
# POSIX scripts reach the VMs over ssh instead of run-command (see
# provider/gcp.sh); nothing above this layer cares which.
: "${CLOUD:=azure}"
export CLOUD
_lib_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
case "${CLOUD}" in
  azure|gcp) ;;
  *) echo "!! CLOUD=${CLOUD}; expected azure or gcp" >&2; return 1 2>/dev/null || exit 1 ;;
esac
# shellcheck source=/dev/null
source "${_lib_dir}/provider/${CLOUD}.sh"

# Scratch for generated files (rendered user-data, script copies that can carry
# tokens). Never the operator's system temp: FELIX_SCRATCH, default
# <repo>/.scratch/perf-cloud (gitignored).
: "${FELIX_SCRATCH:=${_lib_dir}/../../../.scratch/perf-cloud}"
mkdir -p "${FELIX_SCRATCH}" && chmod 700 "${FELIX_SCRATCH}"
FELIX_SCRATCH="$(cd "${FELIX_SCRATCH}" && pwd)"
export FELIX_SCRATCH

# cli <command...>: run a cloud CLI command, or with DRY_RUN=1 print it to
# stderr and do nothing. Every command that creates, changes or deletes cloud
# resources goes through here, so a dry run shows the whole plan.
cli() {
  if [ "${DRY_RUN:-0}" = 1 ]; then
    local a out="+"
    [ "${1:-}" = _gc ] && { shift; set -- gcloud ${GCP_PROJECT:+--project "${GCP_PROJECT}"} "$@"; }
    for a in "$@"; do
      if [[ "${a}" =~ ^[A-Za-z0-9_@%+=:,./-]+$ ]]; then out="${out} ${a}"; else out="${out} '${a}'"; fi
    done
    printf '%s\n' "${out}" >&2
    return 0
  fi
  "$@"
}

# run_on <vm> <local-script-file>
# Execute the script on the VM as root, echo its combined output, and fail
# unless the script reached its __RUNOK__ sentinel.
run_on() {
  _vm="$1"; _script="$2"
  _msg="$(provider_run_on "${_vm}" "${_script}")" || return 1
  printf '%s\n' "${_msg}"
  printf '%s' "${_msg}" | grep -q '__RUNOK__' || {
    echo "!! remote script on ${_vm} did not complete (no __RUNOK__)" >&2
    return 1
  }
}

# run_on_str <vm> <script-text>
# Same, for a script passed as a string rather than a file. Writes it to a
# private temp file (the script text can carry tokens), runs it, and removes it.
run_on_str() {
  _vm="$1"; _text="$2"
  _tmp="$(mktemp "${FELIX_SCRATCH}/felix-run.XXXXXX")"
  chmod 600 "${_tmp}"
  printf '%s' "${_text}" > "${_tmp}"
  run_on "${_vm}" "${_tmp}"
  _rc=$?
  rm -f "${_tmp}"
  return ${_rc}
}

# Extract the payload a remote script framed between __<TAG>_BEGIN__ and
# __<TAG>_END__ markers in its output. Used to bring a value (a token, a count)
# back to the operator from a run-command message without the [stdout] framing.
extract_between() {
  _tag="$1"
  sed -n "s/.*__${_tag}_BEGIN__\(.*\)__${_tag}_END__.*/\1/p" | head -n1
}

# Deterministic VM names — main.bicep's `prefix` is 'felixperf'.
broker_vm() { echo "felixperf-broker-$1"; }
cp_vm() { echo "felixperf-controlplane"; }
loadgen_vm() { echo "felixperf-loadgen"; }

# ---------------------------------------------------------------- the agent
# felix-agent (remote/felix-agent.sh) does the VM-side work of the session
# drivers. Every call re-installs the operator's copy first, so a fix to the
# agent reaches a live session without a reprovision.
_agent_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/remote"

# agent_on <vm> <commands>: run POSIX commands on the VM with felix-agent
# installed; fails unless every command succeeded.
agent_on() {
  run_on_str "$1" "set -e
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
cat > /usr/local/sbin/felix-agent <<'FELIX_AGENT_EOF'
$(cat "${_agent_dir}/felix-agent.sh")
FELIX_AGENT_EOF
chmod 0755 /usr/local/sbin/felix-agent
$2
echo __RUNOK__"
}

# par_on <dir> <suffix> <commands> <vm>...: agent_on each VM concurrently,
# each VM's output in <dir>/<vm>.<suffix>.txt. Fails if any VM failed.
par_on() {
  local dir="$1" suffix="$2" cmds="$3" vm p rc=0
  shift 3
  local pids=()
  mkdir -p "${dir}"
  for vm in "$@"; do
    ( agent_on "${vm}" "${cmds}" > "${dir}/${vm}.${suffix}.txt" 2>&1 ) &
    pids+=("$!")
  done
  for p in ${pids[@]+"${pids[@]}"}; do wait "${p}" || rc=1; done
  return "${rc}"
}

# resolve_ref <ref>: print `name@sha` for a branch, tag or SHA. Brokers are
# built by full SHA so a moving branch cannot change under a session, and so a
# short SHA (which `git fetch` cannot take) still works. Short SHAs resolve
# against the local clone; branches and tags against GitHub.
resolve_ref() {
  local ref="$1" sha="" name
  name="$(printf '%s' "${ref}" | tr -c 'A-Za-z0-9._-' '-')"
  if [[ "${ref}" =~ ^[0-9a-f]{40}$ ]]; then
    sha="${ref}"
  else
    # A peeled tag (^{}) names the commit; the bare tag line names the tag
    # object, which is not what a build should record.
    sha="$(git ls-remote https://github.com/GetFelix/felix "${ref}" "${ref}^{}" 2>/dev/null | awk -v r="${ref}" '
      $2 == "refs/tags/" r "^{}" { p = $1 } $2 == "refs/tags/" r { t = $1 } $2 == "refs/heads/" r { h = $1 }
      END { print (p != "" ? p : (h != "" ? h : t)) }')"
    if [ -z "${sha}" ] && [[ "${ref}" =~ ^[0-9a-f]{7,39}$ ]]; then
      sha="$(git -C "${_agent_dir}" rev-parse --verify --quiet "${ref}^{commit}" || true)"
    fi
  fi
  [ -n "${sha}" ] || { echo "!! cannot resolve ${ref} (not a branch/tag on GitHub, nor a commit in the local clone)" >&2; return 1; }
  printf '%s@%s\n' "${name}" "${sha}"
}

# fetch_file <vm> <remote-path> <local-path>: bring a small file home. How is
# the provider's call (Azure: a blob; GCP: scp). Meant for folded stacks and
# result files, not bulk data.
fetch_file() { provider_fetch_file "$@"; }

# fetch_file_chunked: the portable fallback, through run-command's ~4 KB output
# cap, 3000 bytes a call.
fetch_file_chunked() {
  local vm="$1" src="$2" dst="$3" size off=0 part
  size="$(agent_on "${vm}" "printf '__SIZE_BEGIN__%s__SIZE_END__\n' \"\$(wc -c < '${src}')\"" | extract_between SIZE)"
  [ -n "${size}" ] || { echo "!! ${src} not readable on ${vm}" >&2; return 1; }
  : > "${dst}"
  while [ "${off}" -lt "${size}" ]; do
    part="$(agent_on "${vm}" "felix-agent chunk '${src}' ${off} 3000" | extract_between CHUNK)"
    [ -n "${part}" ] || { echo "!! chunk at ${off} of ${src} came back empty" >&2; return 1; }
    printf '%s' "${part}" >> "${dst}"
    off=$((off + 3000))
  done
}

# The calibrated broker knobs every session starts from (plan section 4).
# Anything a cell changes goes on top of these, in /etc/felix/overrides.env.
#   - ACK_ON_COMMIT: without it a Leader publish is acked on enqueue, and a
#     "durable latency" cell reports enqueue latency.
#   - IO_URING: on by default for the campaign; the flush-dispatch arms set it
#     explicitly either way.
#   - IO_RUNTIME_THREADS=0 is the Linux default, written down so it is recorded.
#   - UDP buffers: 24 MiB, just under the rmem_max cloud-init sets; the 8 MiB
#     default dropped datagrams (RcvbufErrors) under a saturating ingest.
base_overrides() {
  cat <<ENV
FELIX_QUIC_LISTENERS=${BROKER_LISTENERS:-1}
FELIX_IO_RUNTIME_THREADS=0
FELIX_DURABLE_FSYNC_MODE=periodic
FELIX_ACK_ON_COMMIT=1
FELIX_STORAGE_IO_URING=1
FELIX_UDP_RECV_BUFFER=25165824
FELIX_UDP_SEND_BUFFER=25165824
ENV
}

# merge_env <base-lines> <KEY=VALUE>...: later assignments win.
merge_env() {
  local base="$1" kv k
  shift
  for kv in "$@"; do
    [ -n "${kv}" ] || continue
    k="${kv%%=*}"
    base="$(printf '%s\n' "${base}" | grep -v "^${k}=" || true)"
    base="$(printf '%s\n%s' "${base}" "${kv}")"
  done
  printf '%s\n' "${base}" | sed '/^$/d'
}
