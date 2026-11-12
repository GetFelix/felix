#!/usr/bin/env bash
# Provision the comparison VMs: 3 brokers + 2 clients on the same Azure hardware
# the Felix t1 run used, in one resource group. The systems under test (Redpanda,
# Kafka, NATS) are NOT installed here — install-*.sh lays each down over
# run-command so one deployment serves all three in turn. teardown deletes the
# group.
set -euo pipefail

: "${SESSION:?SESSION=<name> (becomes resource group felix-cmp-<name>)}"
: "${LOCATION:=eastus2}"
: "${SSH_KEY_FILE:=$HOME/.ssh/id_ed25519.pub}"

here="$(cd "$(dirname "$0")" && pwd)"
source "${here}/compare-lib.sh"
group="felix-cmp-${SESSION}"
export GROUP="${group}"

echo ">> comparison session ${SESSION}: ${LOCATION}"
[ -n "${AZ_SUBSCRIPTION:-}" ] && az account set --subscription "${AZ_SUBSCRIPTION}"
az group create --name "${group}" --location "${LOCATION}" --output none

az bicep build --file "${here}/compare.bicep" --outfile "${here}/compare.json"

my_ip="$(curl -fsS https://api.ipify.org)"

deployment=$(az deployment group create \
  --resource-group "${group}" \
  --template-file "${here}/compare.json" \
  --parameters \
    sshPublicKey="$(cat "${SSH_KEY_FILE}")" \
    allowedSshCidr="${my_ip}/32" \
  --query properties.outputs --output json)

brokers=$(jq -r '.brokerIps.value | join(",")' <<<"${deployment}")
clients=$(jq -r '.clientIps.value | join(",")' <<<"${deployment}")

mkdir -p "${here}/sessions"
cat > "${here}/sessions/${SESSION}.env" <<INV
SESSION=${SESSION}
GROUP=${group}
LOCATION=${LOCATION}
BROKER_IPS=${brokers}
CLIENT_IPS=${clients}
INV
echo ">> inventory: ${here}/sessions/${SESSION}.env"
echo "   brokers: ${brokers}"
echo "   clients: ${clients}"

wait_ready() {
  vm="$1"
  for _ in $(seq 1 80); do
    if run_on_str "${vm}" 'test -f /var/lib/cloud/instance/base-ready && echo __RUNOK__' \
        >/dev/null 2>&1; then
      return 0
    fi
    sleep 15
  done
  echo "!! ${vm} never finished cloud-init" >&2
  return 1
}
echo ">> waiting for cloud-init on every VM"
IFS=',' read -ra broker_ip_list <<<"${brokers}"
IFS=',' read -ra client_ip_list <<<"${clients}"
for i in "${!broker_ip_list[@]}"; do wait_ready "$(broker_vm "${i}")"; echo "   broker-${i} ready"; done
for i in "${!client_ip_list[@]}"; do wait_ready "$(client_vm "${i}")"; echo "   client-${i} ready"; done

echo ">> auto-teardown backstop at +8h"
az group update --name "${group}" \
  --set "tags.autoTeardownAfter=$(date -u -v+8H '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || date -u -d '+8 hours' '+%Y-%m-%dT%H:%M:%SZ')" \
  --output none

echo ">> VMs ready. Next: install a system (install-redpanda.sh), then bench-*.sh"
