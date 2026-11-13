#!/usr/bin/env bash
# Build felix-broker (with the raft feature), felix-controlplane and felix-loadgen
# in release on felix-build-3, then install the same three binaries in
# /mnt/build/raft-ab/bin on all three build boxes. One binary serves both sides
# of the A/B: FELIX_SHARD_ENGINE picks the engine at start.
#
#   scripts/perf/raft-ab/build.sh            # from this worktree
#   SKIP_BUILD=1 scripts/perf/raft-ab/build.sh   # only redistribute
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
top="$(git -C "$here" rev-parse --show-toplevel)"
lane="$(basename "$top")"
RSSH=/Volumes/External/worktrees/.build/remote-ssh
REMOTE=/Volumes/External/worktrees/.build/remote.sh
user=hello_getfelix_dev
export CLOUDSDK_PYTHON=/Volumes/External/worktrees/.build/gcloud-venv/bin/python
on() { local b=$1; shift; "$RSSH" "$user@felix-build-$b" "$@"; }

rev="$(git -C "$top" rev-parse HEAD)"
if [ "${SKIP_BUILD:-0}" != 1 ]; then
  (cd "$top" && FELIX_BOX=3 "$REMOTE" cargo build --release --features felix-broker-service/raft \
    -p felix-broker-service --bin felix-broker \
    -p felix-controlplane-service --bin felix-controlplane \
    -p felix-loadgen --bin felix-loadgen)
fi

on 3 "set -e; mkdir -p /mnt/build/raft-ab/bin; cd /mnt/build/raft-ab/bin
cp /mnt/build/$lane/target/release/felix-broker /mnt/build/$lane/target/release/felix-controlplane /mnt/build/$lane/target/release/felix-loadgen .
echo $rev > REV; sha256sum felix-broker felix-controlplane felix-loadgen > SHA256SUMS; cat SHA256SUMS"

# Box to box over the internal network: the IAP tunnel from the laptop is ~150 KB/s.
ip3=$(gcloud compute instances describe felix-build-3 --zone=us-central1-a --format='value(networkInterfaces[0].networkIP)')
pid=$(on 3 "cd /mnt/build/raft-ab/bin && { setsid nohup python3 -m http.server 18999 --bind $ip3 >/dev/null 2>&1 < /dev/null & echo \$!; }")
trap 'on 3 "kill $pid 2>/dev/null || true"' EXIT
sleep 2
for b in 1 2; do
  on "$b" "set -e; mkdir -p /mnt/build/raft-ab/bin; cd /mnt/build/raft-ab/bin
for f in felix-broker felix-controlplane felix-loadgen REV SHA256SUMS; do curl -fsS -o \$f.new http://$ip3:18999/\$f && mv \$f.new \$f; done
chmod +x felix-broker felix-controlplane felix-loadgen; sha256sum -c SHA256SUMS"
done
echo "installed rev $rev on felix-build-1..3:/mnt/build/raft-ab/bin"
