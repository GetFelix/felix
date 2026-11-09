#!/usr/bin/env bash
# shellcheck disable=SC2154,SC2034 # shares the calling script's variables (session.sh, lib.sh)
# Google Cloud (Compute Engine) provider for the cloud perf harness. Sourced by
# lib.sh when CLOUD=gcp. The interface is the provider_* set Azure also
# defines (see "Provider interface" in ../README.md).
#
# What differs from Azure:
#   - No VM has an external address (an org policy often forbids them). The
#     operator reaches every VM over ssh through IAP (key from SSH_KEY_FILE,
#     port 22 open to Google's IAP range only), and the VMs reach the internet
#     through a Cloud NAT. The scripts shipped to the VM are the same POSIX sh
#     ones, run as root with sudo.
#   - Everything carries the label felix-perf=<session>. Teardown deletes by
#     that label, and every VM has a run-time cap (MAX_RUN_HOURS) after which
#     GCP deletes it, so a forgotten session cannot keep billing.
#   - Fixed private addresses, so the brokers can be told the control plane's
#     address before it exists: control plane .10, generators .20+, brokers .30+.
#
# Settings (session.env): GCP_PROJECT, GCP_ZONE, BROKER_VM_SIZE,
# LOADGEN_VM_SIZE, CONTROLPLANE_VM_SIZE, BROKER_LOCAL_SSDS, PROVISIONING,
# MAX_RUN_HOURS, PLACEMENT, GCP_VCPU_LIMIT, GCP_NETWORK_MTU.

: "${SSH_KEY_FILE:=$HOME/.ssh/id_ed25519.pub}"
: "${GCP_ZONE:=${LOCATION:-us-central1-a}}"
: "${PROVISIONING:=STANDARD}"      # STANDARD | SPOT
: "${MAX_RUN_HOURS:=8}"            # hard cost cap, enforced by GCP
: "${PLACEMENT:=compact}"          # compact | none (brokers only)
: "${BROKER_LOCAL_SSDS:=1}"        # local SSDs per broker; ignored for *-lssd types
: "${GCP_NETWORK_MTU:=1460}"
: "${USE_LOCAL_NVME:=true}"
GCP_REGION="${GCP_ZONE%-*}"
GCP_SUBNET_PREFIX="10.10.0"
export SSH_KEY_FILE GCP_PROJECT GCP_ZONE GCP_REGION PROVISIONING MAX_RUN_HOURS PLACEMENT BROKER_LOCAL_SSDS USE_LOCAL_NVME

provider_name() { echo gcp; }

provider_default_size() {
  case "$1" in
    broker) echo n2-standard-2 ;;
    loadgen) echo n2-standard-4 ;;
    controlplane) echo e2-small ;;
  esac
}

# gcloud, pinned to the session's project when one is set.
_gc() {
  if [ -n "${GCP_PROJECT:-}" ]; then
    gcloud --project "${GCP_PROJECT}" "$@"
  else
    gcloud "$@"
  fi
}

provider_check() {
  [ "${DRY_RUN:-0}" = 1 ] && return 0
  command -v gcloud >/dev/null || { echo "!! gcloud is not installed (see the README, GCP setup)" >&2; return 1; }
  command -v jq >/dev/null || { echo "!! jq is not installed" >&2; return 1; }
  if [ -z "${GCP_PROJECT:-}" ]; then
    GCP_PROJECT="$(gcloud config get-value project 2>/dev/null)"
    [ -n "${GCP_PROJECT}" ] || { echo "!! no project: set GCP_PROJECT or run gcloud config set project <id>" >&2; return 1; }
    export GCP_PROJECT
  fi
  [ -f "${SSH_KEY_FILE:?}" ] || { echo "!! ${SSH_KEY_FILE} not found (SSH_KEY_FILE)" >&2; return 1; }
}

# ------------------------------------------------------------ sizes and quota

# _gcp_vcpus <machine-type>: vCPUs as GCP counts them for quota.
_gcp_vcpus() {
  case "$1" in
    e2-micro|e2-small|e2-medium) echo 2 ;;
    f1-micro|g1-small) echo 1 ;;
    *) local t="${1%-lssd}"; echo "${t##*-}" ;;
  esac
}

# _gcp_family_metric <machine-type>: the per-family regional quota metric.
_gcp_family_metric() {
  local fam="${1%%-*}"
  printf '%s_CPUS\n' "$(printf '%s' "${fam}" | tr '[:lower:]' '[:upper:]')"
}

# _gcp_lssd_count <machine-type>: local SSDs attached. The -lssd types come
# with a fixed count (an estimate for the quota check; GCP has the final say).
_gcp_lssd_count() {
  case "$1" in
    c4-standard-*-lssd|c4d-standard-*-lssd|c3-standard-*-lssd)
      local n="${1%-lssd}"; n="${n##*-}"
      # c4: 4 and 8 vCPU have one disk, 16 has two (read from the machine type
      # API); larger sizes are estimates.
      case "${n}" in 4|8) echo 1 ;; 16) echo 2 ;; 24) echo 4 ;; 32) echo 5 ;; 48) echo 8 ;; 96) echo 16 ;; *) echo 4 ;; esac ;;
    *) echo "${BROKER_LOCAL_SSDS}" ;;
  esac
}

_gcp_boot_disk_type() {
  case "${1%%-*}" in
    c4|c4d|n4|c4a|n4d) echo hyperdisk-balanced ;;   # these families have no persistent disk
    e2) echo pd-balanced ;;
    *) echo pd-ssd ;;
  esac
}

# _gcp_plan: one "role name machine-type private-ip" line per VM.
_gcp_plan() {
  local i
  echo "controlplane felixperf-controlplane ${CONTROLPLANE_VM_SIZE} ${GCP_SUBNET_PREFIX}.10"
  for ((i = 0; i < ${BROKER_COUNT:-3}; i++)); do
    echo "broker $(broker_vm "${i}") ${BROKER_VM_SIZE} ${GCP_SUBNET_PREFIX}.$((30 + i))"
  done
  for ((i = 0; i < ${LOADGEN_COUNT:-1}; i++)); do
    if [ "${i}" = 0 ]; then
      echo "loadgen $(loadgen_vm) ${LOADGEN0_VM_SIZE:-${LOADGEN_VM_SIZE}} ${GCP_SUBNET_PREFIX}.20"
    else
      echo "loadgen felixperf-loadgen-$((i + 1)) ${LOADGEN_VM_SIZE} ${GCP_SUBNET_PREFIX}.$((20 + i))"
    fi
  done
}

# provider_preflight: print what the session needs against the regional quota
# and refuse to start when anything is short. GCP_VCPU_LIMIT additionally caps
# the vCPU total (a new project often has 12) even when the quota read fails.
provider_preflight() {
  if [ "${USE_LOCAL_NVME}" != true ]; then
    echo "!! GCP brokers keep their data on local NVMe SSD only; USE_LOCAL_NVME must be true" >&2
    return 1
  fi
  local role name type ip total=0 v ssd_gb=0 line metric need
  local -a fam_metrics=() fam_need=()
  echo ">> plan (${PROVISIONING}, zone ${GCP_ZONE}, run-time cap ${MAX_RUN_HOURS}h):"
  while read -r role name type ip; do
    v="$(_gcp_vcpus "${type}")"
    total=$((total + v))
    if [ "${role}" = broker ] && [ "${USE_LOCAL_NVME}" = true ]; then
      ssd_gb=$((ssd_gb + 375 * $(_gcp_lssd_count "${type}")))
    fi
    metric="$(_gcp_family_metric "${type}")"
    fam_metrics+=("${metric}"); fam_need+=("${v}")
    printf '   %-13s %-28s %-22s %2s vCPU  %s\n' "${role}" "${name}" "${type}" "${v}" "${ip}"
  done < <(_gcp_plan)
  printf '   total: %s vCPU, %s GB local SSD\n' "${total}" "${ssd_gb}"
  if [ -n "${GCP_VCPU_LIMIT:-}" ] && [ "${total}" -gt "${GCP_VCPU_LIMIT}" ]; then
    echo "!! ${total} vCPU is over GCP_VCPU_LIMIT=${GCP_VCPU_LIMIT}" >&2
    return 1
  fi
  if command -v gcloud >/dev/null && ! _gc services list --enabled --format 'value(config.name)' | grep -qx iap.googleapis.com; then
    echo "!! the IAP API is not enabled; enable it (once per project) with:" >&2
    echo "   gcloud services enable iap.googleapis.com${GCP_PROJECT:+ --project ${GCP_PROJECT}}" >&2
    echo "   and grant yourself roles/iap.tunnelResourceAccessor if you are not an owner" >&2
    return 1
  fi
  if [ "${DRY_RUN:-0}" = 1 ]; then
    echo "   (dry run: also checks iap.googleapis.com is enabled; live quota not read; needs CPUS ${total}, family totals below)"
    _gcp_family_totals "${total}" "${ssd_gb}" "${fam_metrics[@]}" -- "${fam_need[@]}"
    return 0
  fi
  local quotas
  quotas="$(_gc compute regions describe "${GCP_REGION}" --format=json)" \
    || { echo "!! could not read the quotas of ${GCP_REGION}" >&2; return 1; }
  local short=0 metric_name limit usage
  while read -r metric_name need; do
    limit="$(jq -r --arg m "${metric_name}" '.quotas[] | select(.metric == $m) | .limit' <<<"${quotas}")"
    usage="$(jq -r --arg m "${metric_name}" '.quotas[] | select(.metric == $m) | .usage' <<<"${quotas}")"
    if [ -z "${limit}" ]; then
      printf '   %-24s need %5s  (no such quota in %s)\n' "${metric_name}" "${need}" "${GCP_REGION}"
      continue
    fi
    line="$(printf '   %-24s need %5s  limit %7s  used %7s' "${metric_name}" "${need}" "${limit%.*}" "${usage%.*}")"
    if [ "$(awk -v n="${need}" -v l="${limit}" -v u="${usage}" 'BEGIN { print (n + u <= l) ? 1 : 0 }')" = 1 ]; then
      echo "${line}  ok"
    else
      echo "${line}  SHORT"; short=1
    fi
  done < <(_gcp_quota_needs "${total}" "${ssd_gb}" "${fam_metrics[@]}" -- "${fam_need[@]}")
  if [ "${short}" = 1 ]; then
    echo "!! quota is short; request an increase (console: IAM & Admin, Quotas) or shrink the plan" >&2
    return 1
  fi
}

# _gcp_quota_needs <total-vcpu> <ssd-gb> <metric>... -- <vcpu>...: "metric need"
# lines. Spot VMs draw on the spot quota (PREEMPTIBLE_CPUS) instead of the family one.
_gcp_quota_needs() {
  local total="$1" ssd="$2"; shift 2
  local -a m=() n=()
  while [ "$1" != -- ]; do m+=("$1"); shift; done
  shift
  n=("$@")
  echo "CPUS ${total}"
  [ "${ssd}" -eq 0 ] || echo "LOCAL_SSD_TOTAL_GB ${ssd}"
  local i
  if [ "${PROVISIONING}" = SPOT ]; then
    echo "PREEMPTIBLE_CPUS ${total}"
  else
    for i in "${!m[@]}"; do echo "${m[i]} ${n[i]}"; done | awk '{ s[$1] += $2 } END { for (k in s) print k, s[k] }' | sort
  fi
}

_gcp_family_totals() {
  local total="$1" ssd="$2"; shift 2
  _gcp_quota_needs "${total}" "${ssd}" "$@" | sed 's/^/   /'
}

# ------------------------------------------------------------------- reaching VMs

# ssh runs through an IAP tunnel, with the harness's own key (gcloud's
# `compute ssh` would push keys into project metadata instead).
_gcp_ssh_opts() {
  printf '%s\n' -i "${SSH_KEY_FILE%.pub}" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
    -o LogLevel=ERROR -o ConnectTimeout=30 -o ServerAliveInterval=15 -o IdentitiesOnly=yes \
    -o "ProxyCommand=gcloud ${GCP_PROJECT:+--project ${GCP_PROJECT} }compute start-iap-tunnel %h 22 --listen-on-stdin --zone ${GCP_ZONE}"
}

# provider_run_on <vm> <local-script-file>: ship the script, run it as root, print
# its output. Only a failed connection is an error here; run_on judges the script
# by its __RUNOK__ sentinel, as it does for run-command.
provider_run_on() {
  local vm="$1" script="$2" out rc
  local -a opts=()
  while IFS= read -r o; do opts+=("${o}"); done < <(_gcp_ssh_opts)
  out="$(ssh "${opts[@]}" "felix@${vm}" 'f=$(mktemp) && cat > "$f" && sudo sh "$f" 2>&1; r=$?; rm -f "$f"; exit $r' < "${script}" 2>&1)"
  rc=$?
  if [ "${rc}" = 255 ]; then
    printf '%s\n' "${out}" >&2
    return 1
  fi
  printf '%s\n' "${out}"
}

# provider_fetch_file <vm> <remote-path> <local-path>
provider_fetch_file() {
  local vm="$1" src="$2" dst="$3"
  local -a opts=()
  while IFS= read -r o; do opts+=("${o}"); done < <(_gcp_ssh_opts)
  ssh "${opts[@]}" "felix@${vm}" "sudo cat '${src}'" > "${dst}"
}

provider_vm_names() {
  _gc compute instances list --filter "labels.felix-perf=${SESSION}" --format 'value(name)'
}

provider_vm_sizes() {
  _gc compute instances list --filter "labels.felix-perf=${SESSION}" --format 'value(name,machineType.basename())' | tr '\t' ' '
}

# ------------------------------------------------------------------ create and delete

_gcp_network() { echo "felix-perf-${SESSION}"; }
_gcp_policy() { echo "felix-perf-${SESSION}-compact"; }

# _gcp_render <template> <out> <value>...: fill {0}, {1}, ... in a cloud-init
# template, and the operator's public key.
_gcp_render() {
  local tpl="$1" out="$2" t i=0
  shift 2
  t="$(cat "${tpl}")"
  for v in "$@"; do t="${t//\{${i}\}/${v}}"; i=$((i + 1)); done
  t="${t//__SSH_PUBLIC_KEY__/$(cat "${SSH_KEY_FILE}" 2>/dev/null || echo '<ssh-public-key>')}"
  ( umask 077; printf '%s\n' "${t}" > "${out}" )
}

# _gcp_create_vms <role> <cloud-init-file> <boot-gb> <name:type:ip>...
_gcp_create_vms() {
  local role="$1" userdata="$2" boot_gb="$3" spec name type ip n
  shift 3
  for spec in "$@"; do
    name="${spec%%:*}"; type="${spec#*:}"; ip="${type#*:}"; type="${type%%:*}"
    local -a args=(compute instances create "${name}"
      --zone "${GCP_ZONE}" --machine-type "${type}"
      --image-family ubuntu-2404-lts-amd64 --image-project ubuntu-os-cloud
      --boot-disk-size "${boot_gb}GB" --boot-disk-type "$(_gcp_boot_disk_type "${type}")"
      --network "$(_gcp_network)" --subnet "$(_gcp_network)" --private-network-ip "${ip}"
      --no-address --tags felix-perf --labels "felix-perf=${SESSION},role=${role}"
      --metadata-from-file "user-data=${userdata}"
      --maintenance-policy TERMINATE
      --provisioning-model "${PROVISIONING}"
      --max-run-duration "${MAX_RUN_HOURS}h" --instance-termination-action DELETE)
    if [ "${role}" = broker ] && [ "${USE_LOCAL_NVME}" = true ]; then
      case "${type}" in
        *-lssd) ;;   # local SSDs come with the type
        *) for ((n = 0; n < BROKER_LOCAL_SSDS; n++)); do args+=(--local-ssd interface=NVME); done ;;
      esac
    fi
    if [ "${role}" = broker ] && [ "${PLACEMENT}" = compact ]; then
      args+=(--resource-policies "$(_gcp_policy)")
    fi
    cli _gc "${args[@]}"
  done
}

provider_create_session() {
  local net tmp i role name type ip s
  local -a brokers_spec=() lg_spec=() cp_spec=()
  net="$(_gcp_network)"
  tmp="${FELIX_SCRATCH}/gcp-${SESSION}"
  mkdir -p "${tmp}"; chmod 700 "${tmp}"

  while read -r role name type ip; do
    s="${name}:${type}:${ip}"
    case "${role}" in
      controlplane) cp_spec+=("${s}") ;;
      broker) brokers_spec+=("${s}") ;;
      loadgen) lg_spec+=("${s}") ;;
    esac
  done < <(_gcp_plan)
  cp_ip="${GCP_SUBNET_PREFIX}.10"
  loadgen_private_ip="${GCP_SUBNET_PREFIX}.20"
  loadgen_ip="" brokers="" loadgens=""
  for i in "${!brokers_spec[@]}"; do
    brokers="${brokers:+${brokers},}${GCP_SUBNET_PREFIX}.$((30 + i))"
  done
  for s in "${lg_spec[@]}"; do loadgens="${loadgens:+${loadgens} }${s%%:*}"; done

  local nvme=0; [ "${USE_LOCAL_NVME}" = true ] && nvme=1
  _gcp_render "${_lib_dir}/provider/gcp/cloudinit/controlplane.yaml" "${tmp}/controlplane.yaml" "${release_url}" "${bootstrap_token}"
  _gcp_render "${_lib_dir}/provider/gcp/cloudinit/broker.yaml" "${tmp}/broker.yaml" "${release_url}" "${cp_ip}" "${nvme}" "${BROKER_LISTENERS:-1}"
  _gcp_render "${_lib_dir}/provider/gcp/cloudinit/loadgen.yaml" "${tmp}/loadgen.yaml" "${loadgen_spec}" "${broker_specs}" "${fp_specs}" \
    "$(base64 < "${_lib_dir}/cloudinit/provision-loadgen.sh" | tr -d '\n')"

  cli _gc compute networks create "${net}" --subnet-mode custom --mtu "${GCP_NETWORK_MTU}"
  cli _gc compute networks subnets create "${net}" --network "${net}" --region "${GCP_REGION}" --range "${GCP_SUBNET_PREFIX}.0/24"
  cli _gc compute firewall-rules create "${net}-internal" --network "${net}" --direction INGRESS \
    --allow tcp,udp,icmp --source-ranges "${GCP_SUBNET_PREFIX}.0/24" --target-tags felix-perf
  # 35.235.240.0/20 is Google's IAP range: the only way in is an IAP tunnel.
  cli _gc compute firewall-rules create "${net}-iap-ssh" --network "${net}" --direction INGRESS \
    --allow tcp:22 --source-ranges 35.235.240.0/20 --target-tags felix-perf
  cli _gc compute routers create "${net}" --network "${net}" --region "${GCP_REGION}"
  cli _gc compute routers nats create "${net}" --router "${net}" --region "${GCP_REGION}" \
    --auto-allocate-nat-external-ips --nat-all-subnet-ip-ranges
  if [ "${PLACEMENT}" = compact ]; then
    cli _gc compute resource-policies create group-placement "$(_gcp_policy)" \
      --region "${GCP_REGION}" --collocation COLLOCATED --vm-count "${#brokers_spec[@]}"
  fi

  # The control plane first: the brokers are told its address.
  _gcp_create_vms controlplane "${tmp}/controlplane.yaml" 30 "${cp_spec[@]}"
  _gcp_create_vms broker "${tmp}/broker.yaml" 50 "${brokers_spec[@]}"
  _gcp_create_vms loadgen "${tmp}/loadgen.yaml" 128 "${lg_spec[@]}"
  if [ "${DRY_RUN:-0}" != 1 ]; then
    loadgen_ip="${loadgen_private_ip}"
    rm -rf "${tmp}"
  fi
}

# GCP enforces the cap itself (--max-run-duration); nothing to schedule.
provider_set_backstop() { echo "   (GCP deletes every VM ${MAX_RUN_HOURS}h after it starts: --max-run-duration)"; }

# provider_teardown: delete in dependency order (instances, NAT, router, firewall
# rules, placement policy, subnet, network), then list anything still named or
# labelled for the session. A failed delete does not stop the rest.
provider_teardown() {
  local net names zone left=0
  net="$(_gcp_network)"
  echo ">> deleting everything labelled or named felix-perf-${SESSION}"
  if [ "${DRY_RUN:-0}" = 1 ]; then
    cli _gc compute instances delete "<instances labelled felix-perf=${SESSION}>" --zone "${GCP_ZONE}" --quiet
  else
    while read -r zone names; do
      [ -n "${names}" ] || continue
      # shellcheck disable=SC2086
      cli _gc compute instances delete ${names} --zone "${zone}" --quiet || left=1
    done < <(_gc compute instances list --filter "labels.felix-perf=${SESSION}" --format 'value(name,zone.basename())' \
      | awk '{ n[$2] = n[$2] " " $1 } END { for (z in n) print z n[z] }')
  fi
  cli _gc compute routers nats delete "${net}" --router "${net}" --region "${GCP_REGION}" --quiet || true
  cli _gc compute routers delete "${net}" --region "${GCP_REGION}" --quiet || true
  cli _gc compute firewall-rules delete "${net}-internal" "${net}-iap-ssh" "${net}-ssh" --quiet || true
  cli _gc compute resource-policies delete "$(_gcp_policy)" --region "${GCP_REGION}" --quiet || true
  cli _gc compute networks subnets delete "${net}" --region "${GCP_REGION}" --quiet || true
  cli _gc compute networks delete "${net}" --quiet || true
  [ "${DRY_RUN:-0}" = 1 ] && return 0
  echo ">> still present (should be empty):"
  {
    _gc compute instances list --filter "labels.felix-perf=${SESSION}" --format 'value(name)' | sed 's/^/instance /'
    _gc compute networks list --filter "name~^felix-perf-${SESSION}" --format 'value(name)' | sed 's/^/network /'
    _gc compute networks subnets list --filter "name~^felix-perf-${SESSION}" --format 'value(name)' | sed 's/^/subnet /'
    _gc compute routers list --filter "name~^felix-perf-${SESSION}" --format 'value(name)' | sed 's/^/router /'
    _gc compute firewall-rules list --filter "name~^felix-perf-${SESSION}" --format 'value(name)' | sed 's/^/firewall /'
    _gc compute resource-policies list --filter "name~^felix-perf-${SESSION}" --format 'value(name)' | sed 's/^/policy /'
  } | tee "${FELIX_SCRATCH}/teardown-left-${SESSION}.txt"
  if [ -s "${FELIX_SCRATCH}/teardown-left-${SESSION}.txt" ] || [ "${left}" = 1 ]; then
    echo "!! teardown left resources behind (list above); re-run teardown.sh" >&2
    return 1
  fi
  echo ">> nothing left"
}
