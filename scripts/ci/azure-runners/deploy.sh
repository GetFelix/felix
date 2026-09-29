#!/usr/bin/env bash
# Self-hosted GitHub Actions runners for gabloe/felix. See README.md.
#
#   ./deploy.sh up          create the group, network and VMs, wait for cloud-init
#   ./deploy.sh register    register (or re-register) every VM with the repo
#   ./deploy.sh status      runner state as GitHub sees it
#   ./deploy.sh down        deregister and delete the resource group
#
# Every VM step goes through `az vm run-command`: the NSG admits nothing
# inbound, so there is no SSH path to use.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
GROUP="${GROUP:-felix-ci-runners}"
LOCATION="${LOCATION:-eastus}"
SIZE="${SIZE:-Standard_D8as_v5}"
COUNT="${COUNT:-2}"
REPO="${REPO:-gabloe/felix}"
LABELS="${LABELS:-felix-azure}"
IMAGE="Canonical:ubuntu-24_04-lts:server:latest"
KEY="${KEY:-$HOME/.ssh/felix-ci-runners}"
PREFIX="felix-ci-runner"

vm_names() { for i in $(seq 1 "${COUNT}"); do echo "${PREFIX}-${i}"; done; }

# run_on <vm> <script>: run as root and fail unless the script reached the end.
# run-command reports success for a delivered script whatever its exit code,
# hence the sentinel. The script goes through a private file, not argv, since
# the registration one carries a token.
run_on() {
  local out tmp
  tmp="$(mktemp)"; chmod 600 "${tmp}"; printf '%s\n' "$2" > "${tmp}"
  out="$(az vm run-command invoke -g "${GROUP}" -n "$1" --command-id RunShellScript \
    --scripts @"${tmp}" --query 'value[0].message' -o tsv)" || { rm -f "${tmp}"; return 1; }
  rm -f "${tmp}"
  printf '%s\n' "${out}" | sed '/__RUNOK__/d'
  grep -q __RUNOK__ <<<"${out}" || { echo "!! script on $1 did not finish" >&2; return 1; }
}

up() {
  [ -f "${KEY}" ] || ssh-keygen -q -t ed25519 -N "" -C felix-ci-runners -f "${KEY}"
  az group create -n "${GROUP}" -l "${LOCATION}" --tags purpose=github-runners -o none
  az network nsg create -g "${GROUP}" -n "${PREFIX}-nsg" --tags purpose=github-runners -o none
  # Explicit, ahead of Azure's default AllowVnetInBound and load-balancer rules.
  az network nsg rule create -g "${GROUP}" --nsg-name "${PREFIX}-nsg" -n deny-all-inbound \
    --priority 100 --direction Inbound --access Deny --protocol '*' \
    --source-address-prefixes '*' --source-port-ranges '*' \
    --destination-address-prefixes '*' --destination-port-ranges '*' -o none
  az network vnet create -g "${GROUP}" -n "${PREFIX}-vnet" --address-prefixes 10.60.0.0/24 \
    --subnet-name runners --subnet-prefixes 10.60.0.0/26 --tags purpose=github-runners -o none
  az network vnet subnet update -g "${GROUP}" --vnet-name "${PREFIX}-vnet" -n runners \
    --network-security-group "${PREFIX}-nsg" -o none

  for vm in $(vm_names); do
    # A Standard public IP is only the outbound path: new subnets have no
    # default outbound access, and the NSG drops everything inbound.
    az vm create -g "${GROUP}" -n "${vm}" --image "${IMAGE}" --size "${SIZE}" \
      --os-disk-size-gb 256 --storage-sku Premium_LRS \
      --admin-username felixadmin --ssh-key-values "${KEY}.pub" --authentication-type ssh \
      --vnet-name "${PREFIX}-vnet" --subnet runners --nsg "" \
      --public-ip-sku Standard --public-ip-address "${vm}-ip" \
      --custom-data "${here}/cloud-init.yaml" \
      --tags purpose=github-runners --no-wait -o none
  done
  for vm in $(vm_names); do
    az vm wait -g "${GROUP}" -n "${vm}" --created
  done
  for vm in $(vm_names); do
    echo "== waiting for cloud-init on ${vm}"
    run_on "${vm}" 'cloud-init status --wait >/dev/null 2>&1
tail -n 3 /var/log/felix-runner-provision.log
grep -q "felix-runner-provision: done" /var/log/felix-runner-provision.log && echo __RUNOK__'
  done
  # package_upgrade usually brings a new kernel; boot into it before any job.
  for vm in $(vm_names); do az vm restart -g "${GROUP}" -n "${vm}" -o none; done
}

# Registration tokens last an hour and are fetched per VM here, so none is ever
# stored. --replace makes this the re-register path too.
register() {
  for vm in $(vm_names); do
    local token
    token="$(gh api -X POST "repos/${REPO}/actions/runners/registration-token" --jq .token)"
    echo "== registering ${vm}"
    run_on "${vm}" "set -e
cd /home/runner/actions-runner
if [ -f .service ]; then ./svc.sh stop || true; ./svc.sh uninstall || true; fi
sudo -u runner -H bash -lc 'cd ~/actions-runner && ./config.sh remove --token ${token} >/dev/null 2>&1 || true
./config.sh --unattended --replace --url https://github.com/${REPO} --token ${token} --name ${vm} --labels ${LABELS} --work _work
. ~/.cargo/env && echo \"\$PATH\" > .path'
./svc.sh install runner
./svc.sh start
echo __RUNOK__"
  done
}

status() {
  gh api "repos/${REPO}/actions/runners" \
    --jq '.runners[] | "\(.name)\t\(.status)\tbusy=\(.busy)\t\([.labels[].name] | join(","))"'
}

down() {
  for vm in $(vm_names); do
    local id
    id="$(gh api "repos/${REPO}/actions/runners" --jq ".runners[] | select(.name==\"${vm}\") | .id")"
    [ -n "${id}" ] && gh api -X DELETE "repos/${REPO}/actions/runners/${id}"
  done
  az group delete -n "${GROUP}" --yes --no-wait
}

case "${1:-}" in
  up) up ;;
  register) register ;;
  status) status ;;
  down) down ;;
  *) sed -n '2,8p' "$0"; exit 2 ;;
esac
