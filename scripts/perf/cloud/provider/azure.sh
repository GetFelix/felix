#!/usr/bin/env bash
# shellcheck disable=SC2154,SC2034 # shares the calling script's variables (session.sh, lib.sh)
# Azure provider for the cloud perf harness. Sourced by lib.sh when CLOUD=azure.
# Interface: see provider/README in ../README.md ("Provider interface").
#
# Every operator->VM interaction goes through `az vm run-command invoke`
# (HTTPS to the Azure control plane, no inbound :22). The reasons are in the
# header of lib.sh.

provider_name() { echo azure; }

# provider_check: fail early when the CLI is missing or not logged in.
provider_check() {
  command -v az >/dev/null || { echo "!! az is not installed" >&2; return 1; }
  [ -z "${AZ_SUBSCRIPTION:-}" ] || cli az account set --subscription "${AZ_SUBSCRIPTION}"
}

provider_default_size() {
  case "$1" in
    controlplane) echo Standard_D2as_v5 ;;
    *) echo Standard_D4as_v5 ;;
  esac
}

# provider_preflight: refuse to start when the quota is short. Azure reports a
# short quota itself at deployment time, so there is nothing to check first.
provider_preflight() { :; }

# provider_run_on <vm> <local-script-file>: run the script on the VM as root
# (dash) and print its combined output. The caller checks for __RUNOK__.
provider_run_on() {
  local _vm="$1" _script="$2" _msg
  _msg="$(az vm run-command invoke \
    --resource-group "${GROUP}" --name "${_vm}" \
    --command-id RunShellScript --scripts @"${_script}" \
    --query 'value[0].message' -o tsv 2>&1)" || {
    printf '%s\n' "${_msg}" >&2
    return 1
  }
  printf '%s\n' "${_msg}"
}

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

# provider_fetch_file <vm> <remote-path> <local-path>
provider_fetch_file() {
  local vm="$1" src="$2" dst="$3" sas url
  # One PUT to blob storage and one GET, instead of a run-command per 3 KB.
  if [ "${FETCH_VIA_BLOB:-1}" = 1 ] && sas="$(blob_sas 2>/dev/null)" && [ -n "${sas}" ]; then
    url="${sas%%\?*}/${SESSION}/${vm}/$(date +%s)-$(basename "${src}")?${sas#*\?}"
    if run_on_str "${vm}" "curl -fsS -X PUT -H 'x-ms-blob-type: BlockBlob' --data-binary @'${src}' '${url}'
echo __RUNOK__" >/dev/null 2>&1 && curl -fsS -o "${dst}" "${url}"; then
      return 0
    fi
    echo "!! blob fetch of ${src} from ${vm} failed; falling back to chunks" >&2
  fi
  fetch_file_chunked "${vm}" "${src}" "${dst}"
}

# provider_vm_names: one VM name per line.
provider_vm_names() { az vm list -g "${GROUP}" --query '[].name' -o tsv; }

# provider_vm_sizes: "name size" per line.
provider_vm_sizes() { az vm list -g "${GROUP}" --query '[].[name,hardwareProfile.vmSize]' -o tsv; }

# provider_create_session: create the network and every VM, then set
# loadgen_ip, loadgen_private_ip, cp_ip, brokers (comma list of private IPs)
# and loadgens (space list of generator names). Reads the session.sh variables.
provider_create_session() {
  local compiled my_ip deployment
  cli az group create --name "${group}" --location "${LOCATION}" --output none

  # Compile to plain ARM JSON and deploy THAT, so the deployment never touches
  # az's bundled bicep, which ships the wrong architecture on Apple Silicon
  # (an ELF binary in a macOS install). Prefer a real `bicep` on PATH; fall back
  # to az's, which works once check_version is off and a good binary is in place.
  compiled="${here}/main.json"
  if command -v bicep >/dev/null 2>&1; then
    cli bicep build "${here}/main.bicep" --outfile "${compiled}"
  else
    cli az bicep build --file "${here}/main.bicep" --outfile "${compiled}"
  fi

  # allowedSshCidr no longer gates a working path in (we use run-command, not
  # SSH), but the NSG rule and the parameter remain so a human CAN open a shell
  # for debugging from their own address. 0.0.0.0/0 would be the wrong default;
  # resolve the operator's address and scope the rule to it.
  my_ip="$(curl -fsS https://api.ipify.org)"

  deployment=$(cli az deployment group create \
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
}

# provider_set_backstop <hours>: tag the group so a forgotten session is easy to find.
provider_set_backstop() {
  cli az group update --name "${group}" \
    --set "tags.autoTeardownAfter=$(date -u -v+"$1"H '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || date -u -d "+$1 hours" '+%Y-%m-%dT%H:%M:%SZ')" \
    --output none
}

# provider_teardown: delete everything the session created.
provider_teardown() {
  echo ">> deleting ${group}"
  cli az group delete --name "${group}" --yes --no-wait
  echo ">> deletion running in Azure; verify later with: az group exists --name ${group}"
}
