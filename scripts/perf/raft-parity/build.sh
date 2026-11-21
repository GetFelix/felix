#!/usr/bin/env bash
# Build felix-broker (with the raft feature), felix-controlplane and felix-loadgen
# in release on box 3, then install the same three binaries in
# /mnt/build/raft-parity/bin on every box in BOXES (default 1 2 3). One binary serves both sides
# of the A/B: FELIX_SHARD_ENGINE picks the engine at start.
#
#   scripts/perf/raft-parity/build.sh            # from this worktree
#   SKIP_BUILD=1 scripts/perf/raft-parity/build.sh   # only redistribute
#   BOXES=3 scripts/perf/raft-parity/build.sh        # a smoke run's one box (the build box)
#
# BASELINE_REF=v0.7.2 builds the release baseline instead: felix-broker and felix-controlplane from
# that tag (a separate worktree, no raft feature) into /mnt/build/raft-parity/bin-baseline, leaving
# bin alone. The loadgen there is a copy of the current one in bin, so run the normal build first:
# v0.7.2's loadgen has no mqtt-publish, mqtt-subscribe, resume or failover kill-file scenarios, and
# one client for every side is the fairer comparison.
#   BASELINE_REF=v0.7.2 scripts/perf/raft-parity/build.sh
#
# FRAME_POINTERS=1 builds with -C force-frame-pointers=yes, for run.sh PROFILE=1: `perf record -g`
# walks frame pointers, and a release build has none. Run it from its own clone (the box build
# dir is named after the tree) so it does not replace the matrix build's target dir.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
top="$(git -C "$here" rev-parse --show-toplevel)"
lane="$(basename "$top")"
# shellcheck source=hosts.sh
. "$here/hosts.sh"
BOXES=${BOXES:-"1 2 3"}
on() { host_raw "$@"; }

BIN=/mnt/build/raft-parity/bin
if [ -n "${BASELINE_REF:-}" ]; then
  BIN=/mnt/build/raft-parity/bin-baseline
  # The tag gets its own worktree, and so its own box build dir (remote.sh names it after the lane).
  wt="$(dirname "$top")/parity-baseline-$BASELINE_REF"
  [ -d "$wt" ] || git -C "$top" worktree add --detach "$wt" "refs/tags/$BASELINE_REF"
  top=$wt; lane=$(basename "$top")
  [ "$(git -C "$top" describe --tags --exact-match HEAD 2>/dev/null)" = "$BASELINE_REF" ] \
    || { echo "!! $wt is not at $BASELINE_REF" >&2; exit 1; }
  on 3 "cd /mnt/build/raft-parity/bin && sha256sum -c --quiet SHA256SUMS" \
    || { echo "!! build the current binaries first (the baseline reuses their loadgen)" >&2; exit 1; }
fi
rev="$(git -C "$top" rev-parse HEAD)"
if [ -n "${BASELINE_REF:-}" ]; then
  if [ "${SKIP_BUILD:-0}" != 1 ]; then
    host_build "$top" cargo build --release \
      -p felix-controlplane-service --bin felix-controlplane -p felix-broker-service --bin felix-broker
  fi
elif [ "${SKIP_BUILD:-0}" != 1 ]; then
  cargo=(cargo)
  [ "${FRAME_POINTERS:-0}" != 1 ] || cargo=(env RUSTFLAGS="-C force-frame-pointers=yes" cargo)
  # The broker gets its own invocation: next to the other packages,
  # `--features felix-broker-service/raft` left the Raft engine out of the binary.
  host_build "$top" "${cargo[@]}" build --release \
    -p felix-controlplane-service --bin felix-controlplane -p felix-loadgen --bin felix-loadgen
  host_build "$top" "${cargo[@]}" build --release -p felix-broker-service --features raft --bin felix-broker
fi
if [ -z "${BASELINE_REF:-}" ]; then
  on 3 "strings /mnt/build/$lane/target/release/felix-broker | grep -q openraft" \
    || { echo "!! felix-broker was built without the Raft engine" >&2; exit 1; }
  loadgen_src=/mnt/build/$lane/target/release/felix-loadgen
else
  # The baseline has no Raft engine; make sure the tag build really lacks it, so the two
  # sides cannot be the same binary.
  on 3 "! strings /mnt/build/$lane/target/release/felix-broker | grep -q openraft" \
    || { echo "!! the baseline broker contains the Raft engine" >&2; exit 1; }
  loadgen_src=/mnt/build/raft-parity/bin/felix-loadgen
fi

on 3 "set -e; mkdir -p $BIN; cd $BIN
cp /mnt/build/$lane/target/release/felix-broker /mnt/build/$lane/target/release/felix-controlplane .
cp $loadgen_src ./felix-loadgen
echo ${BASELINE_REF:+$BASELINE_REF }$rev > REV; sha256sum felix-broker felix-controlplane felix-loadgen > SHA256SUMS; cat SHA256SUMS"

# Box to box over the internal network: the IAP tunnel from the laptop is ~150 KB/s.
others=""; for b in $BOXES; do [ "$b" = 3 ] || others="$others $b"; done
if [ -n "$others" ]; then
read -r _ ip3 _ < <(host_info 3)
pid=$(on 3 "cd $BIN && { setsid nohup python3 -m http.server 18999 --bind $ip3 >/dev/null 2>&1 < /dev/null & echo \$!; }")
trap 'on 3 "kill $pid 2>/dev/null || true"' EXIT
sleep 2
for b in $others; do
  on "$b" "set -e; mkdir -p $BIN; cd $BIN
for f in felix-broker felix-controlplane felix-loadgen REV SHA256SUMS; do curl -fsS -o \$f.new http://$ip3:18999/\$f && mv \$f.new \$f; done
chmod +x felix-broker felix-controlplane felix-loadgen; sha256sum -c SHA256SUMS"
done
fi
echo "installed rev $rev on boxes $BOXES: $BIN"
