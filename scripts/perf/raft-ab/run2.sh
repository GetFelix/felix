set -u
cat /proc/loadavg; pgrep -c rustc
cargo build --release -p felix-broker-service --features raft --bin felix-broker 2>&1 | tail -3
K='ingest|batch ack|broker cpu|PUBLISH_TOTAL|PROPOSE_TO_WRITTEN|WRITTEN_TO_APPLIED|COMMIT_TO_APPLIED|RPC_ROUND|FSYNC:|!!|Error'
ob() { echo "=== $*"; bash scripts/perf/raft-ab/onebox.sh "$@" 2>&1 | grep -E "$K"; }
ob a-leader-nolease raft Leader periodic 20 FELIX_SHARD_RAFT_LEASE_MS=0
ob a-leader-lease raft Leader periodic 20
ob b-quorum-commit raft Quorum periodic 20
ob b-quorum-written raft Quorum periodic 20 FELIX_PROTO_QUORUM_WRITTEN=1
ob c-oncommit raft Quorum on_commit 20
ob h-oncommit hybrid Quorum on_commit 20
export IN_FLIGHT=16
ob s-raft-oncommit-if16 raft Quorum on_commit 20
ob s-hybrid-oncommit-if16 hybrid Quorum on_commit 20
export IN_FLIGHT=64
ob s-raft-oncommit-if64 raft Quorum on_commit 20
ob s-hybrid-oncommit-if64 hybrid Quorum on_commit 20
cat /proc/loadavg
