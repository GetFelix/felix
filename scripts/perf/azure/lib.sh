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

# run_on <vm> <local-script-file>
# Execute the script on the VM as root, echo its combined output, and fail
# unless the script reached its __RUNOK__ sentinel.
run_on() {
  _vm="$1"; _script="$2"
  _msg="$(az vm run-command invoke \
    --resource-group "${GROUP}" --name "${_vm}" \
    --command-id RunShellScript --scripts @"${_script}" \
    --query 'value[0].message' -o tsv 2>&1)" || {
    printf '%s\n' "${_msg}" >&2
    return 1
  }
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
  _tmp="$(mktemp "${TMPDIR:-/tmp}/felix-run.XXXXXX")"
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
    sha="$(git ls-remote https://github.com/gabloe/felix "${ref}" "${ref}^{}" 2>/dev/null | awk -v r="${ref}" '
      $2 == "refs/tags/" r "^{}" { p = $1 } $2 == "refs/tags/" r { t = $1 } $2 == "refs/heads/" r { h = $1 }
      END { print (p != "" ? p : (h != "" ? h : t)) }')"
    if [ -z "${sha}" ] && [[ "${ref}" =~ ^[0-9a-f]{7,39}$ ]]; then
      sha="$(git -C "${_agent_dir}" rev-parse --verify --quiet "${ref}^{commit}" || true)"
    fi
  fi
  [ -n "${sha}" ] || { echo "!! cannot resolve ${ref} (not a branch/tag on GitHub, nor a commit in the local clone)" >&2; return 1; }
  printf '%s@%s\n' "${name}" "${sha}"
}

# fetch_file <vm> <remote-path> <local-path>: pull a small file home through
# run-command's ~4 KB output cap, 3000 bytes a call. Meant for folded stacks
# (gzip+base64), not bulk data.
# blob_sas: a read/write SAS URL for this session's artifact container, made on
# first use and cached for 5 h (the SAS lives 6). The container is in the
# session's resource group, so teardown deletes it with everything else.
blob_sas() {
  local cache="${here}/sessions/${SESSION}.blob" acct key exp sas age
  if [ -s "${cache}" ]; then
    age=$(( $(date +%s) - $(stat -f %m "${cache}" 2>/dev/null || stat -c %Y "${cache}") ))
    [ "${age}" -lt 18000 ] && { cat "${cache}"; return 0; }
  fi
  acct="felixperf$(printf '%s' "${GROUP}" | shasum | cut -c1-12)"
  az storage account show -g "${GROUP}" -n "${acct}" -o none 2>/dev/null \
    || az storage account create -g "${GROUP}" -n "${acct}" --sku Standard_LRS \
         --min-tls-version TLS1_2 --allow-blob-public-access false -o none || return 1
  key="$(az storage account keys list -g "${GROUP}" -n "${acct}" --query '[0].value' -o tsv)" || return 1
  az storage container create --account-name "${acct}" --account-key "${key}" -n artifacts -o none || return 1
  exp="$(date -u -v+6H +%Y-%m-%dT%H:%MZ 2>/dev/null || date -u -d '+6 hours' +%Y-%m-%dT%H:%MZ)"
  sas="$(az storage container generate-sas --account-name "${acct}" --account-key "${key}" \
    -n artifacts --permissions rcw --expiry "${exp}" -o tsv)" || return 1
  ( umask 077; printf 'https://%s.blob.core.windows.net/artifacts?%s\n' "${acct}" "${sas}" > "${cache}" )
  cat "${cache}"
}

fetch_file() {
  local vm="$1" src="$2" dst="$3" size off=0 part sas url
  # One PUT to blob storage and one GET, instead of a run-command per 3 KB.
  if [ "${FETCH_VIA_BLOB:-1}" = 1 ] && sas="$(blob_sas 2>/dev/null)" && [ -n "${sas}" ]; then
    url="${sas%%\?*}/${SESSION}/${vm}/$(date +%s)-$(basename "${src}")?${sas#*\?}"
    if run_on_str "${vm}" "curl -fsS -X PUT -H 'x-ms-blob-type: BlockBlob' --data-binary @'${src}' '${url}'
echo __RUNOK__" >/dev/null 2>&1 && curl -fsS -o "${dst}" "${url}"; then
      return 0
    fi
    echo "!! blob fetch of ${src} from ${vm} failed; falling back to chunks" >&2
  fi
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
