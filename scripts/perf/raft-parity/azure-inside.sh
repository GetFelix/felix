#!/usr/bin/env bash
# The parity matrix on the azure.sh VMs when this machine cannot ssh to them (a network that
# blocks outbound ssh on every port). Everything goes through `az vm run-command`: VM 1 becomes
# the operator, holds a clone of the repo, and runs azure.sh build/smoke/run there over the
# vnet's private addresses, exactly as azure.sh would from a laptop.
#
#   azure-inside.sh start              start the three VMs (after azure.sh provision, or deallocated)
#   azure-inside.sh keys               an ssh key on VM 1 for user felix, authorized on all three;
#                                      hosts.env and known_hosts with the private IPs
#   azure-inside.sh checkout <sha>     clone the repo on VM 1 and check out <sha> (a full SHA)
#   azure-inside.sh launch <job> <step>...   run azure.sh <step> in order on VM 1 as a systemd
#                                      unit, logging to /mnt/build/parity-<job>.log; it outlives
#                                      the run-command that started it
#   azure-inside.sh state <job> [n]    the unit's state and the last n (default 20) log lines
#   azure-inside.sh fetch <job> <dir>  results.md, results.json and cells.tsv of the job's latest
#                                      run, in base64 chunks, unpacked into <dir>
#   azure-inside.sh deallocate         stop compute billing, keep the disks
#   azure-inside.sh teardown           azure.sh teardown (deletes the resource group)
#
# A typical run: start, keys, checkout, `launch smoke build smoke`, poll state, read the smoke
# result with fetch, `launch run run`, poll, fetch, teardown. Each run-command takes 30-90 s and
# returns only the last ~4 KB of output, so the steps print little.
#
# Settings: NAME=parity (the azure.sh group felix-perf-parity-<NAME>), REPO, SRC=/mnt/build/src/felix
# on VM 1, ROUNDS CELLS SIDES BASELINE_REF SECS LAT_SECS IDLE_GROUPS (and the other IDLE_* settings) pass through to launch.
# The start and deallocate times go to the cost log, .scratch/raft-parity-azure/<NAME>/cost.log.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
top="$(git -C "$here" rev-parse --show-toplevel)"
: "${NAME:=parity}" "${REPO:=https://github.com/GetFelix/felix}" "${SRC:=/mnt/build/src/felix}"
[[ "$NAME" =~ ^[a-z0-9][a-z0-9-]{0,30}$ ]] || { echo "!! NAME must be lowercase letters, digits and dashes" >&2; exit 2; }
GROUP="felix-perf-parity-$NAME"
VM=felixperf-parity
LOCAL_STATE="${PARITY_HOME:-$top/.scratch/raft-parity-azure}/$NAME"
# The state dir azure.sh uses on VM 1: outside the clone, so a new checkout keeps it.
RHOME=/mnt/build/parity-home
RSTATE=$RHOME/$NAME
KEY=/home/felix/.ssh/id_parity
mkdir -p "$LOCAL_STATE"

log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*"; }
die() { echo "!! $*" >&2; exit 1; }
cost() { echo "$(date -u +%FT%TZ) $*" >> "$LOCAL_STATE/cost.log"; log "$*"; }

# rc <box> <script>: run as root on the VM; prints its stdout, its stderr to ours. The
# script's exit status is not returned by run-command, so scripts end with an OK marker.
rc() {
  local out
  out=$(az vm run-command invoke -g "$GROUP" -n "$VM-$1" --command-id RunShellScript \
    --scripts "$2" --query 'value[0].message' -o tsv) || return 1
  awk '/^\[stdout\]$/{o=1;next} /^\[stderr\]$/{o=2;next} o==1' <<<"$out"
  awk '/^\[stderr\]$/{o=1;next} o' <<<"$out" | sed '/^$/d' >&2
}
# rc_ok <box> <script>: rc, failing unless the script printed RC_OK last.
rc_ok() {
  local out
  out=$(rc "$1" "$2; echo RC_OK") || return 1
  grep -v '^RC_OK$' <<<"$out" || true
  [ "$(tail -1 <<<"$out")" = RC_OK ] || die "box $1: the script failed"
}
ips() {
  az vm list-ip-addresses -g "$GROUP" -o json \
    | jq -r --arg p "$VM-" '.[] | .virtualMachine | "\(.name | ltrimstr($p)) \(.network.privateIpAddresses[0])"' | sort
}
job_ok() { [[ "$1" =~ ^[a-z0-9-]{1,30}$ ]] || die "job names are lowercase letters, digits and dashes"; }

cmd_start() {
  local i s
  for i in 1 2 3; do az vm start -g "$GROUP" -n "$VM-$i" --no-wait; done
  cost "start requested: 3 VMs in $GROUP"
  for _ in $(seq 1 60); do
    s=$(az vm list -g "$GROUP" -d --query '[].powerState' -o tsv | sort -u)
    [ "$s" = "VM running" ] && { cost "all three running"; ips; return 0; }
    sleep 10
  done
  die "the VMs are not all running after 10 min: $s"
}

cmd_keys() {
  local pub i ip1 ip2 ip3 n b
  while read -r n b; do eval "ip$n=$b"; done < <(ips)
  [ -n "${ip1:-}" ] && [ -n "${ip2:-}" ] && [ -n "${ip3:-}" ] || die "could not read the private IPs"
  pub=$(rc_ok 1 "set -e; H=/home/felix; mkdir -p \$H/.ssh
[ -f $KEY ] || ssh-keygen -q -t ed25519 -N '' -C parity-operator -f $KEY
chown -R felix:felix \$H/.ssh; cat $KEY.pub" | tail -1)
  [[ "$pub" =~ ^ssh-ed25519\ [A-Za-z0-9+/=]+(\ [A-Za-z0-9._@-]+)?$ ]] || die "unexpected public key from VM 1: $pub"
  # VM 1 reaches itself the same way: run.sh puts the control plane and a broker on box 1.
  for i in 1 2 3; do
    rc_ok "$i" "set -e; H=/home/felix; mkdir -p \$H/.ssh; touch \$H/.ssh/authorized_keys
grep -qF '$pub' \$H/.ssh/authorized_keys || echo '$pub' >> \$H/.ssh/authorized_keys
chown -R felix:felix \$H/.ssh; chmod 700 \$H/.ssh; chmod 600 \$H/.ssh/authorized_keys
test -e /var/lib/parity-ready; mountpoint -q /mnt/build" &
  done
  wait
  local size; size=$(az vm show -g "$GROUP" -n "$VM-1" --query hardwareProfile.vmSize -o tsv)
  # hosts.sh reaches box n at AZ_PUB_n; here that is the private address.
  rc_ok 1 "set -e; S=$RSTATE; mkdir -p \$S/logs
{ echo '# written by azure-inside.sh keys: private IPs, reached from VM 1 over the vnet'
  echo AZ_GROUP=$GROUP; echo AZ_USER=felix; echo AZ_KEY=$KEY; echo AZ_SSH_PORT=22
  for p in 1:$ip1 2:$ip2 3:$ip3; do n=\${p%%:*}; ip=\${p#*:}
    echo AZ_PUB_\$n=\$ip; echo AZ_IP_\$n=\$ip; echo AZ_SIZE_\$n=$size; done; } > \$S/hosts.env
ssh-keyscan -t ed25519 $ip1 $ip2 $ip3 > \$S/known_hosts 2>/dev/null
chown -R felix:felix $RHOME /mnt/build; chmod 700 \$S; chmod 600 \$S/hosts.env
for ip in $ip1 $ip2 $ip3; do
  sudo -u felix ssh -i $KEY -o BatchMode=yes -o UserKnownHostsFile=\$S/known_hosts felix@\$ip 'echo \$(hostname) ok'
done"
}

cmd_checkout() {
  local sha=${1:-}
  [[ "$sha" =~ ^[0-9a-f]{40}$ ]] || die "checkout takes a full commit SHA"
  rc_ok 1 "set -e; mkdir -p $(dirname "$SRC"); chown felix:felix $(dirname "$SRC")
sudo -u felix bash -c 'set -e; [ -d $SRC/.git ] || git clone -q $REPO $SRC; cd $SRC
git fetch -q origin; git checkout -q --detach $sha; git rev-parse HEAD'"
}

cmd_launch() {
  local job=${1:-}; shift || true; job_ok "$job"
  [ $# -gt 0 ] || die "launch <job> <azure.sh step>..."
  local s envs="PARITY_HOME=$RHOME NAME=$NAME" v
  for s in "$@"; do case "$s" in build|smoke|run|fetch-results) ;; *) die "unknown step $s" ;; esac; done
  for v in ROUNDS CELLS SIDES BASELINE_REF SECS LAT_SECS ALLOW_STALE IDLE_GROUPS IDLE_SHARDS IDLE_BATCH IDLE_BATCH_WAIT_SECS IDLE_SETTLE_SECS; do
    [ -z "${!v:-}" ] || envs="$envs $v=$(printf '%q' "${!v}")"
  done
  rc_ok 1 "set -e; systemctl is-active -q parity-$job && { echo 'parity-$job is already running'; exit 1; }
systemctl reset-failed parity-$job 2>/dev/null || true
systemd-run -q --unit=parity-$job --uid=felix --gid=felix -p WorkingDirectory=$SRC \
  -p StandardOutput=truncate:/mnt/build/parity-$job.log -p StandardError=inherit \
  bash -c 'for s in $*; do $envs scripts/perf/raft-parity/azure.sh \$s || { echo PARITY_DONE fail \$s; exit 1; }; done; echo PARITY_DONE ok'
sleep 1; echo started parity-$job: $*, logging to /mnt/build/parity-$job.log"
}

cmd_state() {
  local job=${1:-} n=${2:-20}; job_ok "$job"
  [[ "$n" =~ ^[0-9]{1,3}$ ]] || die "n is a number"
  rc 1 "echo unit=\$(systemctl is-active parity-$job); tail -n $n /mnt/build/parity-$job.log | cut -c1-200"
}

cmd_fetch() {
  local job=${1:-} dir=${2:-}; job_ok "$job"
  [ -n "$dir" ] || die "fetch <job> <local dir>"
  local glob='*[0-9]Z'; [ "$job" != smoke ] || glob='*-smoke'
  local info run len chunk=3000 off=0 b64=""
  info=$(rc_ok 1 "set -e; run=\$(ls -1d $RSTATE/runs/$glob 2>/dev/null | sort | tail -1); [ -n \"\$run\" ]
cd \$run; tar -cJf /mnt/build/fetch-$job.tar.xz \$(ls results.md results.json cells.tsv 2>/dev/null)
echo \$(basename \$run) \$(base64 -w0 /mnt/build/fetch-$job.tar.xz | wc -c) \$(sha256sum < /mnt/build/fetch-$job.tar.xz | cut -c1-64)")
  read -r run len sum <<<"$(tail -1 <<<"$info")"
  log "run $run: $len bytes of base64 in $(( (len + chunk - 1) / chunk )) chunks"
  while [ "$off" -lt "$len" ]; do
    b64+=$(rc 1 "base64 -w0 /mnt/build/fetch-$job.tar.xz | cut -c$((off + 1))-$((off + chunk))" | tr -d '\n')
    off=$((off + chunk))
  done
  mkdir -p "$dir/$run"
  printf '%s' "$b64" | base64 -d > "$dir/$run.tar.xz"
  [ "$(shasum -a 256 < "$dir/$run.tar.xz" | cut -c1-64)" = "$sum" ] || die "checksum mismatch on $dir/$run.tar.xz"
  tar -xJf "$dir/$run.tar.xz" -C "$dir/$run"
  [ ! -f "$dir/$run/results.json" ] || python3 -c 'import json,sys; json.load(open(sys.argv[1]))' "$dir/$run/results.json"
  log "unpacked into $dir/$run"
}

cmd_deallocate() {
  local i pids=()
  for i in 1 2 3; do az vm deallocate -g "$GROUP" -n "$VM-$i" -o none & pids+=($!); done
  for i in "${pids[@]}"; do wait "$i"; done
  cost "all three deallocated"
}

cmd_teardown() {
  cost "teardown requested"
  "$here/azure.sh" teardown
  cost "teardown finished"
}

sub=${1:-}; [ $# -eq 0 ] || shift
case "$sub" in
  start) cmd_start ;;
  keys) cmd_keys ;;
  checkout) cmd_checkout "$@" ;;
  launch) cmd_launch "$@" ;;
  state) cmd_state "$@" ;;
  fetch) cmd_fetch "$@" ;;
  deallocate) cmd_deallocate ;;
  teardown) cmd_teardown ;;
  *) sed -n '2,25p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
