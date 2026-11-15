#!/usr/bin/env bash
# Build felix-broker (with the raft feature), felix-controlplane and felix-loadgen
# in release on felix-build-3, then install the same three binaries in
# /mnt/build/raft-parity/bin on every box in BOXES (default 1 2 3). One binary serves both sides
# of the A/B: FELIX_SHARD_ENGINE picks the engine at start.
#
#   scripts/perf/raft-parity/build.sh            # from this worktree
#   SKIP_BUILD=1 scripts/perf/raft-parity/build.sh   # only redistribute
#   BOXES=3 scripts/perf/raft-parity/build.sh        # a smoke run's one box (the build box)
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
top="$(git -C "$here" rev-parse --show-toplevel)"
lane="$(basename "$top")"
RSSH=/Volumes/External/worktrees/.build/remote-ssh
REMOTE=/Volumes/External/worktrees/.build/remote.sh
user=hello_getfelix_dev
BOXES=${BOXES:-"1 2 3"}
export CLOUDSDK_PYTHON=/Volumes/External/worktrees/.build/gcloud-venv/bin/python
on() { local b=$1; shift; "$RSSH" "$user@felix-build-$b" "$@"; }

rev="$(git -C "$top" rev-parse HEAD)"
if [ "${SKIP_BUILD:-0}" != 1 ]; then
  # The broker gets its own invocation: next to the other packages,
  # `--features felix-broker-service/raft` left the Raft engine out of the binary.
  (cd "$top" && FELIX_BOX=3 "$REMOTE" cargo build --release \
    -p felix-controlplane-service --bin felix-controlplane -p felix-loadgen --bin felix-loadgen)
  (cd "$top" && FELIX_BOX=3 "$REMOTE" cargo build --release -p felix-broker-service --features raft --bin felix-broker)
fi
on 3 "strings /mnt/build/$lane/target/release/felix-broker | grep -q openraft" \
  || { echo "!! felix-broker was built without the Raft engine" >&2; exit 1; }

on 3 "set -e; mkdir -p /mnt/build/raft-parity/bin; cd /mnt/build/raft-parity/bin
cp /mnt/build/$lane/target/release/felix-broker /mnt/build/$lane/target/release/felix-controlplane /mnt/build/$lane/target/release/felix-loadgen .
echo $rev > REV; sha256sum felix-broker felix-controlplane felix-loadgen > SHA256SUMS; cat SHA256SUMS"

# Box to box over the internal network: the IAP tunnel from the laptop is ~150 KB/s.
others=""; for b in $BOXES; do [ "$b" = 3 ] || others="$others $b"; done
if [ -n "$others" ]; then
ip3=$(gcloud compute instances describe felix-build-3 --zone=us-central1-a --format='value(networkInterfaces[0].networkIP)')
pid=$(on 3 "cd /mnt/build/raft-parity/bin && { setsid nohup python3 -m http.server 18999 --bind $ip3 >/dev/null 2>&1 < /dev/null & echo \$!; }")
trap 'on 3 "kill $pid 2>/dev/null || true"' EXIT
sleep 2
for b in $others; do
  on "$b" "set -e; mkdir -p /mnt/build/raft-parity/bin; cd /mnt/build/raft-parity/bin
for f in felix-broker felix-controlplane felix-loadgen REV SHA256SUMS; do curl -fsS -o \$f.new http://$ip3:18999/\$f && mv \$f.new \$f; done
chmod +x felix-broker felix-controlplane felix-loadgen; sha256sum -c SHA256SUMS"
done
fi
echo "installed rev $rev on felix-build-$BOXES:/mnt/build/raft-parity/bin"
