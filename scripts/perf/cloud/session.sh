#!/usr/bin/env bash
# Provision one perf session: network and VMs (Azure: resource group and Bicep
# deployment; GCP: network, firewall and instances) -> wait for cloud-init ->
# seed -> print the inventory run.sh consumes. CLOUD=azure|gcp picks the
# provider; DRY_RUN=1 prints every cloud command and the plan, creates nothing.
#
# What the brokers run, in order of precedence:
#   BROKER_REFS="main 8f1736eb"  generator 0 builds each ref from source and
#                                serves it in the VNet; seed.sh installs all of
#                                them and activates ACTIVE_REF (default: the
#                                first). FP_REFS builds frame-pointer variants.
#   RELEASE_URL=<tarball>        any release-layout tarball.
#   RELEASE_TAG=<tag>            the GitHub release.
#
# Azure: sessions are independent resource groups, so several can run at once in
# different regions (LOCATION), each against its own regional vCPU quota.
# Everything a session creates lives in one resource group, and teardown.sh
# deletes the group. The operator drives the VMs with `az vm run-command` over
# HTTPS, never SSH (see lib.sh for why).
#
# GCP: everything carries the label felix-perf=<session> and teardown.sh
# deletes by label. Every VM also has a hard run-time cap (MAX_RUN_HOURS), so
# a forgotten cluster deletes itself. The operator reaches the VMs over ssh.
set -euo pipefail

: "${SESSION:?SESSION=<name> (becomes the resource group felix-perf-<name>)}"
# Refuse to run on a half-loaded or unfilled session.env: a <placeholder> left
# in, or a file that stopped sourcing at a syntax error, would otherwise fall
# through to the defaults below.
for _v in SESSION GCP_PROJECT MAIN_SHA BROKER_REFS LOADGEN_REF AZ_SUBSCRIPTION IDP_TENANT_ID IDP_CLIENT_ID IDP_SCOPE IDP_CLIENT_SECRET; do
  case "${!_v:-}" in
    *"<"*">"*) echo "!! ${_v} still holds a placeholder (${!_v}); fill it in session.env" >&2; exit 1 ;;
  esac
done
if [ "${CLOUD:-azure}" = gcp ]; then
  [ "${FELIX_ENV_LOADED:-}" = 1 ] || { echo "!! session.env was not fully loaded (FELIX_ENV_LOADED unset): fix its syntax errors, then set -a; source it" >&2; exit 1; }
  if [ -z "${GCP_PROJECT:-}" ] && [ -z "$(gcloud config get-value project 2>/dev/null)" ]; then
    echo "!! GCP_PROJECT is unset and gcloud has no default project" >&2; exit 1
  fi
fi
: "${LOCATION:=$([ "${CLOUD:-azure}" = gcp ] && echo "${GCP_ZONE:-us-central1-a}" || echo eastus2)}"
: "${TIER:=t1}"
: "${RELEASE_TAG:=v0.7.0}"
# The instrument builds from a ref that HAS felix-loadgen. The crate merged to
# main in #370, so main is the default again; override for a branch under review.
: "${LOADGEN_REF:=main}"
: "${BROKER_REFS:=}"
: "${FP_REFS:=}"
# Generator 0 builds every broker ref (LTO, one codegen unit) plus the
# instrument, which on a D4 is well past the old 30 minutes.
: "${PROVISION_TIMEOUT_MIN:=120}"
: "${SSH_KEY_FILE:=$HOME/.ssh/id_ed25519.pub}"
# Only the L-series has local NVMe. With the flag on any other size the template
# attaches no data disk, and /data lands on the 30 GB OS disk.
if [ "${CLOUD:-azure}" = azure ]; then
  case "${BROKER_VM_SIZE:-Standard_D4as_v5}" in
    Standard_L*) ;;
    *)
      if [ "${USE_LOCAL_NVME:-false}" = true ]; then
        echo "!! USE_LOCAL_NVME=true, but ${BROKER_VM_SIZE:-Standard_D4as_v5} has no local NVMe; set USE_LOCAL_NVME=false" >&2
        exit 1
      fi
      ;;
  esac
fi

here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "${here}/lib.sh"
: "${BROKER_VM_SIZE:=$(provider_default_size broker)}"
: "${LOADGEN_VM_SIZE:=$(provider_default_size loadgen)}"
: "${CONTROLPLANE_VM_SIZE:=$(provider_default_size controlplane)}"
group="felix-perf-${SESSION}"
export GROUP="${group}"
loadgen_spec="$(resolve_ref "${LOADGEN_REF}")"
broker_specs=""
for r in ${BROKER_REFS}; do broker_specs="${broker_specs:+${broker_specs} }$(resolve_ref "${r}")"; done
fp_specs=""
for r in ${FP_REFS}; do fp_specs="${fp_specs:+${fp_specs} }$(resolve_ref "${r}")"; done
if [ -n "${broker_specs}" ]; then
  release_url=""
  release_desc="source: ${broker_specs}${fp_specs:+ (+fp: ${fp_specs})}"
else
  release_url="${RELEASE_URL:-https://github.com/GetFelix/felix/releases/download/${RELEASE_TAG}/felix-${RELEASE_TAG}-linux-x86_64.tar.gz}"
  release_desc="${release_url}"
fi
bootstrap_token="$(openssl rand -hex 24)"

# The IdP flow. Two ways in: paste IDP_TOKEN + the issuer/JWKS/audience
# yourself, or (the easy path) give the three app-registration secrets and let
# the suite derive the rest and mint the token. The seed and the token-exchange
# scenario both run against whatever this resolves to — the real flow, not demo
# auth.
if [ -z "${IDP_TOKEN:-}" ] && [ -n "${IDP_TENANT_ID:-}" ]; then
  : "${IDP_CLIENT_ID:?set IDP_CLIENT_ID with IDP_TENANT_ID}"
  : "${IDP_CLIENT_SECRET:?set IDP_CLIENT_SECRET with IDP_TENANT_ID}"
  export IDP_SCOPE="${IDP_SCOPE:-${IDP_AUDIENCE:-}}"
  : "${IDP_SCOPE:?set IDP_SCOPE (the app Application ID URI, or the bare client id)}"
  # JWKS is version-agnostic (validates both v1 and v2 tokens). The issuer and
  # the audience are NOT: an app-only credential issues a v1 token
  # (iss=sts.windows.net/<tenant>/, aud=api://<client-id>) even from the v2
  # endpoint, where a v2 token carries the bare client-id GUID. seed-remote.sh
  # derives both from the token rather than assuming them here.
  export IDP_JWKS_URL="${IDP_JWKS_URL:-https://login.microsoftonline.com/${IDP_TENANT_ID}/discovery/v2.0/keys}"
  if [ "${DRY_RUN:-0}" = 1 ]; then IDP_TOKEN=dry-run; else IDP_TOKEN="$("${here}/idp-token.sh")"; fi
  export IDP_TOKEN
  echo ">> minted an IdP token for scope ${IDP_SCOPE}"
fi

echo ">> session ${SESSION}: tier ${TIER}, ${LOCATION}, brokers from ${release_desc}"
echo "   loadgen from ${loadgen_spec}"
provider_check
provider_preflight
provider_create_session
if [ "${DRY_RUN:-0}" = 1 ]; then
  echo ">> dry run: nothing was created" >&2
  exit 0
fi

# The inventory is the contract between the session's scripts.
mkdir -p "${here}/sessions"
# Build labels the brokers get: every broker ref, and <name>-fp for each
# frame-pointer ref. deploy-ref.sh appends to it mid-session.
broker_labels=""
for spec in ${broker_specs}; do broker_labels="${broker_labels:+${broker_labels} }${spec%@*}"; done
for spec in ${fp_specs}; do broker_labels="${broker_labels:+${broker_labels} }${spec%@*}-fp"; done
active_ref="${ACTIVE_REF:-${broker_labels%% *}}"
artifact_base=""
[ -n "${broker_specs}" ] && artifact_base="http://${loadgen_private_ip}:8088"
cat > "${here}/sessions/${SESSION}.env" <<INV
SESSION=${SESSION}
CLOUD=${CLOUD}
GROUP=${group}
TIER=${TIER}
LOCATION=${LOCATION}
RELEASE_TAG=${RELEASE_TAG}
RELEASE_URL="${release_url}"
LOADGEN_SPEC="${loadgen_spec}"
BROKER_SPECS="${broker_specs}"
FP_SPECS="${fp_specs}"
BROKER_LABELS="${broker_labels}"
ACTIVE_REF="${active_ref}"
ARTIFACT_BASE="${artifact_base}"
BROKER_VM_SIZE="${BROKER_VM_SIZE}"
LOADGEN_VM_SIZE="${LOADGEN_VM_SIZE}"
LOADGEN0_VM_SIZE="${LOADGEN0_VM_SIZE:-}"
USE_LOCAL_NVME="${USE_LOCAL_NVME:-false}"
BROKER_DATA_DISK_GIB="${BROKER_DATA_DISK_GIB:-128}"
BROKER_LISTENERS="${BROKER_LISTENERS:-1}"
SHARDS="${SHARDS:-12}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-1}"
LOADGEN_IP=${loadgen_ip}
LOADGEN_PRIVATE_IP=${loadgen_private_ip}
CONTROLPLANE_IP=${cp_ip}
BROKER_IPS=${brokers}
LOADGENS="${loadgens}"
BOOTSTRAP_TOKEN=${bootstrap_token}
INV
if [ "${CLOUD}" = gcp ]; then
  cat >> "${here}/sessions/${SESSION}.env" <<GINV
GCP_ZONE=${GCP_ZONE}
GCP_ZONE_ATTEMPTS="${GCP_ZONE_ATTEMPTS}"
PROVISIONING=${PROVISIONING}
MAX_RUN_HOURS=${MAX_RUN_HOURS}
PLACEMENT=${PLACEMENT}
BROKER_LOCAL_SSDS=${BROKER_LOCAL_SSDS}
GINV
fi
echo ">> inventory: ${here}/sessions/${SESSION}.env"

# Wait for cloud-init via run-command (not SSH): the loadgen writes the
# felix-provisioned marker only after building the instrument, which is the
# slow step. The brokers and control plane finish sooner, but we gate on all
# three so seed.sh never races a half-provisioned VM.
wait_provisioned() {
  vm="$1"
  for _ in $(seq 1 $((PROVISION_TIMEOUT_MIN * 4))); do
    state="$(run_on_str "${vm}" 'if test -f /var/lib/cloud/instance/felix-provision-failed; then echo __STATE_BEGIN__failed__STATE_END__; tail -25 /var/log/felix-provision.log; elif test -f /var/lib/cloud/instance/felix-provisioned; then echo __STATE_BEGIN__done__STATE_END__; fi
echo __RUNOK__' 2>/dev/null || true)"
    case "$(printf '%s' "${state}" | extract_between STATE)" in
      done) return 0 ;;
      failed) printf '%s\n' "${state}" >&2; echo "!! ${vm} failed provisioning" >&2; return 1 ;;
    esac
    sleep 15
  done
  echo "!! ${vm} never finished cloud-init (${PROVISION_TIMEOUT_MIN} min)" >&2
  return 1
}
echo ">> waiting for cloud-init (loadgen builds the instrument${broker_specs:+ and the broker refs}; up to ${PROVISION_TIMEOUT_MIN} min)"
wait_provisioned "$(cp_vm)"
IFS=',' read -ra broker_ip_list <<<"${brokers}"
for i in "${!broker_ip_list[@]}"; do wait_provisioned "$(broker_vm "${i}")"; done
for lg in ${loadgens}; do wait_provisioned "${lg}"; done
if [ -n "${broker_specs}" ]; then
  echo ">> builds on $(loadgen_vm):"
  run_on_str "$(loadgen_vm)" 'cat /srv/felix/BUILD_STATUS; echo __RUNOK__' | grep -E ' (ok|failed) ' || true
fi

mkdir -p "${here}/sessions/${SESSION}-results/system"
echo ">> seeding (bootstrap + exchange on the loadgen; token-drop + start per broker)"
CONTROLPLANE_IP="${cp_ip}" \
BROKER_COUNT="${#broker_ip_list[@]}" \
BOOTSTRAP_TOKEN="${bootstrap_token}" \
IDP_JWKS_URL="${IDP_JWKS_URL:-}" \
IDP_AUDIENCE="${IDP_AUDIENCE:-}" \
IDP_TOKEN="${IDP_TOKEN:-}" \
GROUP="${group}" \
ARTIFACT_BASE="${artifact_base}" \
BROKER_LABELS="${broker_labels}" \
ACTIVE_REF="${active_ref}" \
BROKER_LISTENERS="${BROKER_LISTENERS:-1}" \
ASSIGNMENTS_OUT="${here}/sessions/${SESSION}-results/system/assignments.txt" \
  bash "${here}/seed.sh"

echo ">> auto-teardown backstop at +${TEARDOWN_AFTER_H:-8}h"
provider_set_backstop "${TEARDOWN_AFTER_H:-8}"

echo ">> ready. Next: a driver (session-a.sh / session-b.sh / session-c.sh) or run.sh, e.g."
echo "   SESSION=${SESSION} ${here}/session-a.sh"
echo "   and when finished: SESSION=${SESSION} ${here}/teardown.sh"
