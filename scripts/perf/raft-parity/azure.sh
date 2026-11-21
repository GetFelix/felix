#!/usr/bin/env bash
# The Raft vs hybrid parity matrix on three Azure VMs. One entry point, one command per step:
#
#   azure.sh provision       three VMs in one resource group, one volume at /mnt/build on each
#                            (local NVMe on L series, else one Premium SSD v2 data disk)
#   azure.sh build           build the current revision on VM 3 and install it on all three
#   azure.sh smoke           one round, three cheap cells, both engines (~15 min)
#   azure.sh run             the matrix; ROUNDS defaults to 2 (a multiple of 6 also balances
#                            the generator box and the side order)
#   azure.sh fetch-results   collect the run's results and each VM's logs into one archive
#   azure.sh teardown        delete the resource group, which holds the VMs, data disks and IPs
#   azure.sh all             provision, build, smoke, run, fetch-results, then teardown even on failure
#   azure.sh status | ssh <1-3> [cmd]   what exists; a shell on a VM
#
# Nothing here runs `az vm run-command`: the orchestration is run.sh over ssh, the same
# scripts as on GCP (PARITY_PROVIDER=azure selects hosts.sh's Azure box list).
#
# Settings (environment):
#   NAME=parity           names the resource group, felix-perf-parity-<NAME>, which is also what
#                         scripts/perf/cloud/teardown.sh takes as SESSION=parity-<NAME>
#   LOCATION=centralus    VM_SIZE=Standard_D16ls_v6   ZONE=1   MAX_HOURS=12 (see below)
#                         3 x 16 vCPUs; all three VMs and their disks sit in one zone
#   DATA_DISK=auto        auto: local NVMe for Standard_L* sizes, else premiumv2; or local|premiumv2
#   DISK_GIB=256 DISK_IOPS=12000 DISK_MBPS=500   the Premium SSD v2 disk, the same on all three
#   PLACEMENT=none|ppg    proximity placement group (default none: one zone is close enough)
#   SSH_PORT=22           443 also listens there, for networks that reset outbound :22
#   SSH_KEY_FILE=~/.ssh/id_ed25519.pub   OPERATOR_CIDR=<your ip>/32 (default: looked up)
#   AZ_SUBSCRIPTION=...   ROUNDS=2 CELLS SIDES ...  anything run.sh reads passes through
#   BASELINE_REF=v0.7.2   with build: build the release baseline into bin-baseline instead
#   KEEP=1                `all` leaves the VMs up
#   DRY_RUN=1             provision/teardown print their az commands and change nothing
#
# MAX_HOURS is a backstop and not a stop: each VM is set to deallocate at that clock time
# (az vm auto-shutdown) and the group carries an autoTeardownAfter tag. Compute billing
# stops on deallocation; the disks and IPs remain until teardown.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
top="$(git -C "$here" rev-parse --show-toplevel)"
cloud="$top/scripts/perf/cloud"

: "${NAME:=parity}" "${LOCATION:=centralus}" "${VM_SIZE:=Standard_D16ls_v6}" "${MAX_HOURS:=12}"
: "${ZONE:=1}" "${DATA_DISK:=auto}" "${DISK_GIB:=256}" "${DISK_IOPS:=12000}" "${DISK_MBPS:=500}"
: "${PLACEMENT:=none}" "${SSH_PORT:=22}" "${SSH_KEY_FILE:=$HOME/.ssh/id_ed25519.pub}"
[[ "$NAME" =~ ^[a-z0-9][a-z0-9-]{0,30}$ ]] || { echo "!! NAME must be lowercase letters, digits and dashes" >&2; exit 2; }
[[ "$SSH_PORT" =~ ^[0-9]{1,5}$ ]] || { echo "!! SSH_PORT must be a number" >&2; exit 2; }
[[ "$MAX_HOURS" =~ ^[0-9]{1,3}$ ]] || { echo "!! MAX_HOURS must be a number" >&2; exit 2; }
[[ "$DISK_GIB" =~ ^[0-9]{1,5}$ && "$DISK_IOPS" =~ ^[0-9]{1,6}$ && "$DISK_MBPS" =~ ^[0-9]{1,5}$ ]] \
  || { echo "!! DISK_GIB, DISK_IOPS and DISK_MBPS must be numbers" >&2; exit 2; }
[[ "$ZONE" =~ ^[1-3]?$ ]] || { echo "!! ZONE must be 1, 2, 3 or empty" >&2; exit 2; }
case "$DATA_DISK" in
  auto) case "$VM_SIZE" in Standard_L[0-9]*) DATA_DISK=local ;; *) DATA_DISK=premiumv2 ;; esac ;;
  local|premiumv2) ;;
  *) echo "!! DATA_DISK must be auto, local or premiumv2" >&2; exit 2 ;;
esac
# Premium SSD v2 exists only in a zone, and the disk must be in its VM's zone.
[ "$DATA_DISK" != premiumv2 ] || [ -n "$ZONE" ] || { echo "!! DATA_DISK=premiumv2 needs a ZONE" >&2; exit 2; }

GROUP="felix-perf-parity-$NAME"
TAG="parity-$NAME"
HOME_DIR="${PARITY_HOME:-$top/.scratch/raft-parity-azure}"
STATE="$HOME_DIR/$NAME"
export PARITY_PROVIDER=azure PARITY_STATE="$STATE"
R=/mnt/build/raft-parity
AZ_USER=felix

sub=${1:-}; [ $# -eq 0 ] || shift
case "$sub" in
  provision|build|smoke|run|fetch-results|teardown|all|status|ssh) ;;
  *) sed -n '2,32p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac

mkdir -p "$STATE/logs"; chmod 700 "$STATE"
LOG="$STATE/logs/$sub-$(date -u +%Y%m%dT%H%M%SZ).log"
# status and ssh are interactive; everything else logs to a file as it prints.
case "$sub" in status|ssh) ;; *) exec > >(tee -a "$LOG") 2>&1 ;; esac

log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*"; }
die() { echo "!! $*" >&2; exit 1; }
need() { command -v "$1" >/dev/null || die "$1 is not installed"; }

# lib.sh brings `cli` (prints instead of running under DRY_RUN=1) and the azure provider's
# login check and group teardown. It sources provider/azure.sh for CLOUD=azure.
load_cloud() {
  CLOUD=azure FELIX_SCRATCH="$STATE/scratch"
  export CLOUD FELIX_SCRATCH
  # shellcheck source=/dev/null
  . "$cloud/lib.sh"
  group=$GROUP
  provider_check
}

hosts_loaded() { [ -f "$STATE/hosts.env" ]; }
load_hosts() {
  hosts_loaded || die "no VMs recorded for NAME=$NAME; run azure.sh provision"
  # shellcheck source=hosts.sh
  . "$here/hosts.sh"
}
ssh_box() { host_raw "$@"; }

# vcpus <size>: the digits after the series letters, Standard_L8as_v4 -> 8
vcpus() { echo "$1" | sed -E 's/^Standard_[A-Za-z]+([0-9]+).*/\1/'; }

# ---------------------------------------------------------------- provision
preflight() {
  need az; need jq; need ssh; need curl
  [ -f "$SSH_KEY_FILE" ] || die "no ssh public key at $SSH_KEY_FILE (set SSH_KEY_FILE)"
  [ -f "${SSH_KEY_FILE%.pub}" ] || die "no private key at ${SSH_KEY_FILE%.pub}"
  local n fam used limit need_cores tused tlimit
  n=$(vcpus "$VM_SIZE"); need_cores=$((n * 3))
  fam=$(az vm list-skus -l "$LOCATION" --size "$VM_SIZE" --query '[0].family' -o tsv 2>/dev/null || true)
  [ -n "$fam" ] || die "$VM_SIZE is not offered in $LOCATION for this subscription"
  local usage
  usage=$(az vm list-usage -l "$LOCATION" -o json)
  read -r used limit < <(jq -r --arg f "$fam" '.[] | select(.name.value == $f) | "\(.currentValue) \(.limit)"' <<<"$usage")
  read -r tused tlimit < <(jq -r '.[] | select(.name.value == "cores") | "\(.currentValue) \(.limit)"' <<<"$usage")
  log "quota in $LOCATION: $fam $used/$limit used, total regional cores $tused/$tlimit; need $need_cores"
  if [ $((limit - used)) -lt "$need_cores" ] || [ $((tlimit - tused)) -lt "$need_cores" ]; then
    cat >&2 <<EOF
!! not enough vCPU quota for 3 x $VM_SIZE ($need_cores vCPUs).
   Either raise the "$fam" and total regional cores quota in $LOCATION to at least $need_cores
   (Portal: Quotas > Compute; usually approved within minutes), or use a smaller size that fits:
     VM_SIZE=Standard_D8ls_v6 $0 provision    # 24 vCPUs; slower per box, same disk setup
EOF
    exit 1
  fi
}

render_cloud_init() {
  local out="$STATE/cloud-init.yaml" stanza=""
  [ "$SSH_PORT" = 22 ] || stanza="Port $SSH_PORT"
  sed -e "s|__SSH_PORT_STANZA__|$stanza|" -e "s|__DATA_DISK__|$DATA_DISK|" -e "s|__DISK_GIB__|$DISK_GIB|" \
    "$here/azure-cloud-init.yaml" > "$out"
  echo "$out"
}

create_vm() { # <i> <ci-file> <vnet> <subnet> <nsg>
  local i=$1 ci=$2 vm="felixperf-parity-$1"
  if az vm show -g "$GROUP" -n "$vm" -o none 2>/dev/null; then log "$vm already exists"; return 0; fi
  local ppg=() zone=() disk=()
  [ "$PLACEMENT" != ppg ] || ppg=(--ppg felixperf-parity-ppg)
  [ -z "$ZONE" ] || zone=(--zone "$ZONE")
  if [ "$DATA_DISK" = premiumv2 ]; then
    # Created first and named at VM creation; both live in the group, so teardown takes them.
    # Premium SSD v2 has no host caching and the v6 sizes attach remote disks over NVMe.
    cli az disk create -g "$GROUP" -n "$vm-data" -l "$LOCATION" --zone "$ZONE" --sku PremiumV2_LRS \
      --size-gb "$DISK_GIB" --disk-iops-read-write "$DISK_IOPS" --disk-mbps-read-write "$DISK_MBPS" \
      --logical-sector-size 4096 --tags "felix-perf=$TAG" -o none
    disk=(--attach-data-disks "$vm-data" --disk-controller-type NVMe)
  fi
  # ${ppg[@]+...}: an empty array is an unset variable to bash 3.2 under set -u.
  cli az vm create -g "$GROUP" -n "$vm" -l "$LOCATION" --size "$VM_SIZE" \
    --image Canonical:ubuntu-24_04-lts:server:latest \
    --admin-username "$AZ_USER" --ssh-key-values "$(cat "$SSH_KEY_FILE")" --authentication-type ssh \
    --custom-data "$ci" --os-disk-size-gb 128 --storage-sku Premium_LRS \
    --vnet-name felixperf-parity-vnet --subnet nodes --nsg felixperf-parity-nsg --nsg-rule NONE \
    --public-ip-sku Standard --public-ip-address "$vm-ip" --accelerated-networking true \
    --tags "felix-perf=$TAG" ${ppg[@]+"${ppg[@]}"} ${zone[@]+"${zone[@]}"} ${disk[@]+"${disk[@]}"} -o none
}

cmd_provision() {
  preflight
  load_cloud
  local ci cidr stop stop_hm p i pids=()
  ci=$(render_cloud_init)
  cidr=${OPERATOR_CIDR:-$(curl -fsS https://api.ipify.org)/32}
  stop=$(date -u -v+"$MAX_HOURS"H '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || date -u -d "+$MAX_HOURS hours" '+%Y-%m-%dT%H:%M:%SZ')
  # az group create would overwrite the tags of a group that is not ours, and teardown trusts them.
  if [ "${DRY_RUN:-0}" != 1 ] && [ "$(az group exists -n "$GROUP")" = true ]; then
    [ "$(az group show -n "$GROUP" --query 'tags."felix-perf"' -o tsv)" = "$TAG" ] \
      || die "$GROUP exists and was not made by this script; pick another NAME"
    [ "${FRESH_ONLY:-0}" != 1 ] || die "$GROUP already exists (a KEEP=1 leftover?); run teardown first"
  fi
  local dd="$DATA_DISK"
  [ "$DATA_DISK" != premiumv2 ] || dd="premiumv2 ($DISK_GIB GiB, $DISK_IOPS IOPS, $DISK_MBPS MB/s)"
  log "provisioning 3 x $VM_SIZE in $LOCATION${ZONE:+ zone $ZONE} as $GROUP, data on $dd, ssh from $cidr"
  log "if this fails part way, run: $0 teardown"
  cli az group create -n "$GROUP" -l "$LOCATION" --tags "felix-perf=$TAG" "autoTeardownAfter=$stop" -o none
  [ "$PLACEMENT" != ppg ] || cli az ppg create -g "$GROUP" -n felixperf-parity-ppg -l "$LOCATION" --type Standard -o none
  cli az network nsg create -g "$GROUP" -n felixperf-parity-nsg -l "$LOCATION" -o none
  local ports="22"; [ "$SSH_PORT" = 22 ] || ports="22 $SSH_PORT"
  local prio=100
  for p in $ports; do
    cli az network nsg rule create -g "$GROUP" --nsg-name felixperf-parity-nsg -n "ssh-$p" --priority "$prio" \
      --access Allow --protocol Tcp --direction Inbound --source-address-prefixes "$cidr" \
      --destination-port-ranges "$p" -o none
    prio=$((prio + 10))
  done
  cli az network vnet create -g "$GROUP" -n felixperf-parity-vnet -l "$LOCATION" \
    --address-prefixes 10.50.0.0/16 --subnet-name nodes --subnet-prefixes 10.50.1.0/24 -o none
  for i in 1 2 3; do
    create_vm "$i" "$ci" & pids+=($!)
  done
  for p in "${pids[@]}"; do wait "$p" || die "a VM failed to create (capacity? try PLACEMENT=none or another LOCATION); see above"; done
  # Backstop: deallocate each VM at the same clock time the tag names.
  stop_hm=$(date -u -v+"$MAX_HOURS"H +%H%M 2>/dev/null || date -u -d "+$MAX_HOURS hours" +%H%M)
  for i in 1 2 3; do
    cli az vm auto-shutdown -g "$GROUP" -n "felixperf-parity-$i" --time "$stop_hm" -o none \
      || log "warning: could not set auto-shutdown on box $i; teardown is on you"
  done
  [ "${DRY_RUN:-0}" != 1 ] || { log "dry run: nothing created"; return 0; }
  write_hosts
  wait_ready
  log "ready. Next: $0 build"
}

write_hosts() {
  local json i
  json=$(az vm list-ip-addresses -g "$GROUP" -o json)
  {
    echo "# written by azure.sh provision; $(date -u +%FT%TZ)"
    echo "AZ_GROUP=$GROUP"
    echo "AZ_USER=$AZ_USER"
    echo "AZ_KEY=${SSH_KEY_FILE%.pub}"
    echo "AZ_SSH_PORT=$SSH_PORT"
    for i in 1 2 3; do
      jq -r --arg n "felixperf-parity-$i" --argjson i "$i" --arg size "$VM_SIZE" '
        .[] | select(.virtualMachine.name == $n) | .virtualMachine.network as $net |
        "AZ_PUB_\($i)=\($net.publicIpAddresses[0].ipAddress)\nAZ_IP_\($i)=\($net.privateIpAddresses[0])\nAZ_SIZE_\($i)=\($size)"' <<<"$json"
    done
  } > "$STATE/hosts.env"
  chmod 600 "$STATE/hosts.env"
  [ "$(grep -c '^AZ_PUB_' "$STATE/hosts.env")" = 3 ] || die "could not read the VMs' addresses"
}

wait_ready() {
  load_hosts
  local b deadline=$(( $(date +%s) + 1200 ))
  for b in 1 2 3; do
    log "waiting for box $b (cloud-init: packages, data volume, rustup)"
    until ssh_box "$b" "test -e /var/lib/parity-ready || test -e /var/lib/parity-failed" 2>/dev/null; do
      [ "$(date +%s)" -lt "$deadline" ] || die "box $b not ready after 20 min; check: $0 ssh $b 'cloud-init status --long'"
      sleep 15
    done
    ssh_box "$b" "test ! -e /var/lib/parity-failed" || die "box $b: $(ssh_box "$b" 'cat /var/lib/parity-failed')"
  done
  for b in 1 2 3; do
    ssh_box "$b" "bash -s" <<'REMOTE' | sed "s/^/box $b: /"
set -e
mountpoint -q /mnt/build && test -w /mnt/build || { echo "/mnt/build is not a writable mount" >&2; exit 1; }
echo "$(nproc) cpus, $(awk '/MemTotal/{printf "%d GiB", $2/1048576}' /proc/meminfo), volume $(cat /var/lib/parity-disk), $(df -h --output=size /mnt/build | tail -1 | xargs)"
# What an fsync costs here, kept with the run. A managed disk shows about a millisecond.
mkdir -p /mnt/build/raft-parity
fio --name=fsync --filename=/mnt/build/fio.tmp --size=256M --bs=4k --rw=write --fsync=1 \
  --runtime=10 --time_based --output-format=json > /mnt/build/raft-parity/fio-fsync.json
rm -f /mnt/build/fio.tmp
jq -r '.jobs[0].write | "4 KiB write + fsync: \(.iops | floor) iops, mean \((.lat_ns.mean / 1000) | floor) us"' /mnt/build/raft-parity/fio-fsync.json
REMOTE
  done
}

# ---------------------------------------------------------------- build
cmd_build() {
  load_hosts
  log "building rev $(git -C "$top" rev-parse --short HEAD)${BASELINE_REF:+ (baseline $BASELINE_REF)} on box 3; first build takes 20-40 min, later ones reuse the target dir"
  "$here/build.sh"
}

# ---------------------------------------------------------------- run
# Refuse to measure a revision other than the one in the tree, and anything but a quiet box.
check_installed() {
  load_hosts
  local b rev want
  want=$(git -C "$top" rev-parse HEAD)
  for b in 1 2 3; do
    ssh_box "$b" "test -w /mnt/build" || die "box $b is not reachable or /mnt/build is not writable"
    rev=$(ssh_box "$b" "head -1 $R/bin/REV 2>/dev/null" || true)
    [ -n "$rev" ] || die "box $b has no build; run: $0 build"
    [ "$rev" = "$want" ] || [ "${ALLOW_STALE:-0}" = 1 ] \
      || die "box $b holds $rev but this tree is at $want; run: $0 build (or ALLOW_STALE=1)"
  done
}

cmd_smoke() {
  check_installed
  log "smoke: one round, single-node and RF 3 cells, hybrid then raft, 15 s each"
  CELLS="${CELLS:-single-oncommit-lat rf3-oncommit-lat rf3-oncommit-tput}" ROUNDS=${ROUNDS:-1} \
    SECS=${SECS:-15} LAT_SECS=${LAT_SECS:-15} BOXES="1 2 3" OUT="${OUT:-$STATE/runs/$(date -u +%Y%m%dT%H%M%SZ)-smoke}" \
    "$here/run.sh"
}

cmd_run() {
  check_installed
  export ROUNDS=${ROUNDS:-2}
  if [ $((ROUNDS % 6)) -ne 0 ]; then
    log "ROUNDS=$ROUNDS is not a multiple of 6: generator placement will not be balanced (summarize.py says so in the report)"
  fi
  [ -z "${BASELINE_REF:-}" ] || SIDES="${SIDES:-hybrid raft baseline}"
  log "running the matrix: ROUNDS=$ROUNDS SIDES=${SIDES:-hybrid raft}; keep this machine awake and on the network"
  local pre=()
  command -v caffeinate >/dev/null && pre=(caffeinate -i)
  SIDES="${SIDES:-hybrid raft}" BOXES="1 2 3" ${pre[@]+"${pre[@]}"} "$here/run.sh"
}

# ---------------------------------------------------------------- fetch-results
cmd_fetch_results() {
  load_hosts
  local run=${RUN:-} dir b arch
  if [ -z "$run" ]; then run=$(find "$STATE/runs" -mindepth 1 -maxdepth 1 -type d ! -name '*-smoke' 2>/dev/null | sort | tail -1 | xargs -n1 basename 2>/dev/null || true); fi
  [ -n "$run" ] || die "no run under $STATE/runs"
  dir="$STATE/runs/$run"
  [ -f "$dir/cells.tsv" ] || die "$dir has no cells.tsv (an unfinished run?)"
  python3 "$here/summarize.py" "$dir" > /dev/null || true
  for b in 1 2 3; do
    mkdir -p "$dir/boxes/box$b"
    ssh_box "$b" "cd $R && tar -c hold bin/REV bin/SHA256SUMS fio-fsync.json 2>/dev/null" 2>/dev/null \
      | tar -x -C "$dir/boxes/box$b" 2>/dev/null || log "box $b: no box-side logs to collect"
  done
  arch="$STATE/parity-$run.tar.gz"
  tar -czf "$arch" -C "$STATE/runs" "$run"
  log "run $run: $(grep -m1 '^Verdict' "$dir/results.md" 2>/dev/null || echo 'no verdict')"
  log "report:  $dir/results.md"
  log "archive: $arch"
}

# ---------------------------------------------------------------- teardown
cmd_teardown() {
  load_cloud
  need jq
  if [ "${DRY_RUN:-0}" != 1 ]; then
    local ex
    ex=$(az group exists -n "$GROUP") || die "az group exists failed (login expired?); fix that and run teardown again"
    if [ "$ex" = false ]; then
      log "$GROUP does not exist; nothing to delete"
      [ ! -f "$STATE/hosts.env" ] || mv "$STATE/hosts.env" "$STATE/hosts.env.deleted"
      return 0
    fi
    [ "$ex" = true ] || die "unexpected answer from az group exists: '$ex'"
    # Only ever delete a group this script made.
    [ "$(az group show -n "$GROUP" --query "tags.\"felix-perf\"" -o tsv)" = "$TAG" ] \
      || die "$GROUP is not tagged felix-perf=$TAG; refusing to delete it"
  fi
  close_masters
  provider_teardown
  [ "${DRY_RUN:-0}" = 1 ] && return 0
  local i
  for i in $(seq 1 90); do
    [ "$(az group exists -n "$GROUP" 2>/dev/null)" = false ] && break
    sleep 10
  done
  if [ "$(az group exists -n "$GROUP" 2>/dev/null)" = false ]; then
    log "$GROUP is deleted"
    [ ! -f "$STATE/hosts.env" ] || mv "$STATE/hosts.env" "$STATE/hosts.env.deleted"
  else
    die "$GROUP still exists after 15 min; check: az group show -n $GROUP, and run teardown again"
  fi
}

close_masters() {
  [ -f "$STATE/hosts.env" ] || return 0
  local b
  # shellcheck source=hosts.sh
  . "$here/hosts.sh" 2>/dev/null || return 0
  for b in 1 2 3; do
    ssh -O exit -o ControlPath="/tmp/fpcm-%C" -i "$AZ_KEY" -p "$AZ_SSH_PORT" "$AZ_USER@$(_az_var PUB "$b")" 2>/dev/null || true
  done
}

# ---------------------------------------------------------------- all, status, ssh
cmd_all() {
  # The group is torn down whatever happens after this line, unless KEEP=1.
  if [ "${KEEP:-0}" != 1 ]; then
    trap 'exit 130' INT; trap 'exit 143' TERM; trap 'exit 129' HUP
    trap 'rc=$?; trap - EXIT; exec >>"'"$LOG"'" 2>&1; log "tearing down (exit $rc)"; "$0" teardown || echo "!! TEARDOWN FAILED: run $0 teardown, then az group show -n '"$GROUP"'" >&2; exit $rc' EXIT
  fi
  FRESH_ONLY=1 cmd_provision
  cmd_build
  cmd_smoke
  cmd_run
  cmd_fetch_results
}

cmd_status() {
  need az
  [ -z "${AZ_SUBSCRIPTION:-}" ] || az account set --subscription "$AZ_SUBSCRIPTION"
  if [ "$(az group exists -n "$GROUP")" = true ]; then
    az vm list -g "$GROUP" -d --query '[].{name:name,size:hardwareProfile.vmSize,state:powerState,ip:publicIps}' -o table
    echo "group $GROUP exists; delete it with: $0 teardown"
  else
    echo "group $GROUP does not exist"
  fi
  [ ! -d "$STATE/runs" ] || { echo "runs:"; ls -1 "$STATE/runs"; }
}

cmd_ssh() {
  local b=${1:?box 1-3}; shift
  load_hosts
  ssh_box "$b" "$@"
}

case "$sub" in
  provision) cmd_provision ;;
  build) cmd_build ;;
  smoke) cmd_smoke ;;
  run) cmd_run ;;
  fetch-results) cmd_fetch_results ;;
  teardown) cmd_teardown ;;
  all) cmd_all ;;
  status) cmd_status ;;
  ssh) cmd_ssh "$@" ;;
esac
