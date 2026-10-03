#!/usr/bin/env bash
# Provision one perf session: resource group -> Bicep deployment -> wait for
# cloud-init -> seed -> print the inventory run.sh consumes.
#
# What the brokers run, in order of precedence:
#   BROKER_REFS="main 8f1736eb"  generator 0 builds each ref from source and
#                                serves it in the VNet; seed.sh installs all of
#                                them and activates ACTIVE_REF (default: the
#                                first). FP_REFS builds frame-pointer variants.
#   RELEASE_URL=<tarball>        any release-layout tarball.
#   RELEASE_TAG=<tag>            the GitHub release.
#
# Sessions are independent resource groups, so several can run at once in
# different regions (LOCATION), each against its own regional vCPU quota.
#
# Everything a session creates lives in one resource group, and teardown.sh
# deletes the group. A scheduled auto-teardown backstops a wedged session so
# a forgotten cluster cannot idle through the budget.
#
# The operator drives the VMs with `az vm run-command` over HTTPS, never SSH
# (see lib.sh for why). The only inbound port the session needs is nothing:
# the control plane is VNet-private and reached by running on a VM inside it.
set -euo pipefail

: "${SESSION:?SESSION=<name> (becomes the resource group felix-perf-<name>)}"
: "${LOCATION:=eastus2}"
: "${TIER:=t1}"
: "${RELEASE_TAG:=v0.5.0}"
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
case "${BROKER_VM_SIZE:-Standard_D4as_v5}" in
  Standard_L*) ;;
  *)
    if [ "${USE_LOCAL_NVME:-false}" = true ]; then
      echo "!! USE_LOCAL_NVME=true, but ${BROKER_VM_SIZE:-Standard_D4as_v5} has no local NVMe; set USE_LOCAL_NVME=false" >&2
      exit 1
    fi
    ;;
esac

here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "${here}/lib.sh"
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
  release_url="${RELEASE_URL:-https://github.com/gabloe/felix/releases/download/${RELEASE_TAG}/felix-${RELEASE_TAG}-linux-x86_64.tar.gz}"
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
  IDP_TOKEN="$("${here}/idp-token.sh")"
  export IDP_TOKEN
  echo ">> minted an IdP token for scope ${IDP_SCOPE}"
fi

echo ">> session ${SESSION}: tier ${TIER}, ${LOCATION}, brokers from ${release_desc}"
echo "   loadgen from ${loadgen_spec}"
# Land in the right subscription when the login has more than one.
[ -n "${AZ_SUBSCRIPTION:-}" ] && az account set --subscription "${AZ_SUBSCRIPTION}"
az group create --name "${group}" --location "${LOCATION}" --output none

# Compile to plain ARM JSON and deploy THAT, so the deployment never touches
# az's bundled bicep — which ships the wrong architecture on Apple Silicon
# (an ELF binary in a macOS install). Prefer a real `bicep` on PATH; fall back
# to az's, which works once check_version is off and a good binary is in place.
compiled="${here}/main.json"
if command -v bicep >/dev/null 2>&1; then
  bicep build "${here}/main.bicep" --outfile "${compiled}"
else
  az bicep build --file "${here}/main.bicep" --outfile "${compiled}"
fi

# allowedSshCidr no longer gates a working path in (we use run-command, not
# SSH), but the NSG rule and the parameter remain so a human CAN open a shell
# for debugging from their own address. 0.0.0.0/0 would be the wrong default;
# resolve the operator's address and scope the rule to it.
my_ip="$(curl -fsS https://api.ipify.org)"

deployment=$(az deployment group create \
  --resource-group "${group}" \
  --template-file "${compiled}" \
  --parameters \
    tier="${TIER}" \
    releaseUrl="${release_url}" \
    loadgenRef="${loadgen_spec}" \
    brokerRefs="${broker_specs}" \
    fpRefs="${fp_specs}" \
    brokerCount="${BROKER_COUNT:-3}" \
    loadgenCount="${LOADGEN_COUNT:-1}" \
    brokerVmSize="${BROKER_VM_SIZE:-Standard_D4as_v5}" \
    loadgenVmSize="${LOADGEN_VM_SIZE:-Standard_D4as_v5}" \
    loadgen0VmSize="${LOADGEN0_VM_SIZE:-}" \
    controlPlaneVmSize="${CONTROLPLANE_VM_SIZE:-Standard_D2as_v5}" \
    brokerDataDiskGib="${BROKER_DATA_DISK_GIB:-128}" \
    useLocalNvme="${USE_LOCAL_NVME:-false}" \
    brokerListeners="${BROKER_LISTENERS:-1}" \
    sshPublicKey="$(cat "${SSH_KEY_FILE}")" \
    allowedSshCidr="${my_ip}/32" \
    bootstrapToken="${bootstrap_token}" \
  --query properties.outputs --output json)

loadgen_ip=$(jq -r .loadgenPublicIp.value <<<"${deployment}")
loadgen_private_ip=$(jq -r .loadgenPrivateIp.value <<<"${deployment}")
cp_ip=$(jq -r .controlPlaneIp.value <<<"${deployment}")
brokers=$(jq -r '.brokerIps.value | join(",")' <<<"${deployment}")
loadgens=$(jq -r '.loadgenNames.value | join(" ")' <<<"${deployment}")

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
BROKER_VM_SIZE="${BROKER_VM_SIZE:-Standard_D4as_v5}"
LOADGEN_VM_SIZE="${LOADGEN_VM_SIZE:-Standard_D4as_v5}"
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
az group update --name "${group}" \
  --set "tags.autoTeardownAfter=$(date -u -v+"${TEARDOWN_AFTER_H:-8}"H '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || date -u -d "+${TEARDOWN_AFTER_H:-8} hours" '+%Y-%m-%dT%H:%M:%SZ')" \
  --output none

echo ">> ready. Next: a driver (session-a.sh / session-b.sh / session-c.sh) or run.sh, e.g."
echo "   SESSION=${SESSION} ${here}/session-a.sh"
echo "   and when finished: SESSION=${SESSION} ${here}/teardown.sh"
