# shellcheck shell=bash
# The machines the parity matrix runs on, behind four functions. Sourced by run.sh and
# build.sh; PARITY_PROVIDER picks the set (default gcp).
#
#   host_name <box>        a label for logs
#   host_raw <box> <cmd>   run a command on the box (stdin passes through)
#   host_info <box>        "<RUNNING|other> <private ip> <machine type>"
#   host_build <top> <cmd> run a build command (cargo ...) for the tree <top> on box 3,
#                          leaving its target dir in /mnt/build/<basename of top>/
#
# gcp:   the felix-build-1..3 boxes, reached through the lanes' remote-ssh wrapper.
# azure: three VMs made by azure.sh provision, listed in $PARITY_STATE/hosts.env.
#        /mnt/build there is the local NVMe, so every path in node.sh and run.sh is the same.

: "${PARITY_PROVIDER:=gcp}"
case "$PARITY_PROVIDER" in
  gcp)
    RSSH=/Volumes/External/worktrees/.build/remote-ssh
    REMOTE=/Volumes/External/worktrees/.build/remote.sh
    GCP_USER=hello_getfelix_dev
    export CLOUDSDK_PYTHON=/Volumes/External/worktrees/.build/gcloud-venv/bin/python
    host_name() { echo "felix-build-$1"; }
    host_raw() { local b=$1; shift; "$RSSH" "$GCP_USER@felix-build-$b" "$@"; }
    host_info() {
      gcloud compute instances describe "felix-build-$1" --zone=us-central1-a \
        --format='value(status,networkInterfaces[0].networkIP,machineType.basename())'
    }
    host_build() { local top=$1; shift; (cd "$top" && FELIX_BOX=3 "$REMOTE" "$@"); }
    PARITY_ENV=gcp-build
    ;;
  azure)
    : "${PARITY_STATE:?PARITY_STATE=<state dir written by azure.sh provision>}"
    [ -f "$PARITY_STATE/hosts.env" ] || { echo "!! $PARITY_STATE/hosts.env missing; run azure.sh provision" >&2; return 1 2>/dev/null || exit 1; }
    # shellcheck source=/dev/null
    . "$PARITY_STATE/hosts.env"   # AZ_USER AZ_KEY AZ_SSH_PORT and AZ_PUB_<n> AZ_IP_<n> AZ_SIZE_<n>
    _az_var() { local v="AZ_$1_$2"; printf '%s' "${!v:-}"; }
    host_name() { echo "parity-$1"; }
    host_raw() {
      local b=$1; shift
      ssh -i "$AZ_KEY" -p "$AZ_SSH_PORT" -o IdentitiesOnly=yes -o BatchMode=yes \
        -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile="$PARITY_STATE/known_hosts" \
        -o ControlMaster=auto -o ControlPersist=10m -o ControlPath="/tmp/fpcm-%C" \
        -o ServerAliveInterval=30 -o ServerAliveCountMax=10 -o LogLevel=ERROR \
        "$AZ_USER@$(_az_var PUB "$b")" "$@"
    }
    host_info() { echo "RUNNING $(_az_var IP "$1") $(_az_var SIZE "$1")"; }
    host_build() {
      local top=$1; shift
      local lane dest
      lane=$(basename "$top")
      [[ "$lane" =~ ^[A-Za-z0-9._-]+$ ]] || { echo "!! unsafe tree name $lane" >&2; return 1; }
      dest=/mnt/build/$lane
      # The tree at HEAD, so what is built is exactly the revision build.sh records.
      [ -z "$(git -C "$top" status --porcelain --untracked-files=no)" ] \
        || { echo "!! $top has uncommitted changes; commit them so the build matches the recorded revision" >&2; return 1; }
      host_raw 3 "set -e; mkdir -p $dest; find $dest -mindepth 1 -maxdepth 1 ! -name target -exec rm -rf {} +"
      git -C "$top" archive HEAD | host_raw 3 "tar -x -C $dest"
      host_raw 3 "set -e; . \$HOME/.cargo/env; cd $dest; $(printf '%q ' "$@")"
    }
    PARITY_ENV=azure-parity
    ;;
  *) echo "!! PARITY_PROVIDER=$PARITY_PROVIDER; expected gcp or azure" >&2; return 1 2>/dev/null || exit 1 ;;
esac
