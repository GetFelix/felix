#!/usr/bin/env bash
# ingest_flags prints loadgen flags as words, split on purpose.
# shellcheck disable=SC2046
# Session C: three D4as_v5 brokers in zones 1/2/3, RF=3, two D4as_v5
# generators and the control plane pinned to zone 1. Runs unattended;
# re-running resumes.
#
#   #425   RF=3 Leader (acked on commit, and labelled enqueue-ack) against
#          RF=3 Quorum, and the durable Quorum stream at Periodic and OnCommit.
#          Compare against session B's b425-rf1-* rows: same SKU, same build.
#
#   lease vs lease-free: every step after `lease-free` runs with the
#          generation_start, majority_ack and lease_free_reads fleet features
#          finalized (one-way), its cells tagged -lf. Quorum writes then ack on
#          a majority at the leader's generation, and Quorum cache reads
#          confirm leadership with a majority round instead of the lease.
#
# Storage is not wiped between cells here (WIPE_DURABLE=0): with three
# replicas a whole-cluster wipe under live assignments is its own experiment.
#
# Provision with (see README):
#   SESSION=v060-c LOCATION=centralus TIER=t2 REPLICATION_FACTOR=3 BROKER_COUNT=3 \
#   LOADGEN_COUNT=2 SHARDS=12 BROKER_REFS=main ./session.sh
set -uo pipefail

: "${WIPE_DURABLE:=0}"
# Replicas re-establish after a restart; give them longer than RF=1 needs.
: "${SETTLE_SECS:=15}"
here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=cells.sh
source "${here}/cells.sh"

: "${NEW_REF:=main}"
: "${STEPS:=leader quorum durable shapes cache explore lease-free quorum durable shapes cache}"

if [ "${TIER}" != t2 ] || [ "${REPLICATION_FACTOR:-1}" -lt 3 ]; then
  echo "!! session ${SESSION} is ${TIER}, RF=${REPLICATION_FACTOR:-1}. Quorum costs inter-zone distance;" >&2
  echo "   provision TIER=t2 REPLICATION_FACTOR=3, or set ALLOW_ANY_TIER=1 on purpose." >&2
  [ "${ALLOW_ANY_TIER:-0}" = 1 ] || exit 1
fi

record_session session-c
distribute_token || exit 1
INGEST_GENS="${#LOADGEN_VMS[@]}"
[ -s "${OUT}/system/assignments.txt" ] && log "leaders $(cat "${OUT}/system/assignments.txt")"

for step in ${STEPS}; do
  case "${step}" in
  leader)
    stage "${NEW_REF}" FELIX_ACK_ON_COMMIT=1; write_path_pass c425-rf3-leader-commitack perf 0
    # Without ACK_ON_COMMIT a Leader publish acks on enqueue: the number most
    # earlier docs quote, kept only as a labelled row.
    stage "${NEW_REF}" FELIX_ACK_ON_COMMIT=0; write_path_pass c425-rf3-leader-enqueueack perf 0
    ;;

  quorum)
    stage "${NEW_REF}"; write_path_pass c425-rf3-quorum perf-quorum 0
    ;;

  durable)
    stage "${NEW_REF}" FELIX_DURABLE_FSYNC_MODE=periodic
    write_path_pass c425-rf3-dquorum-periodic perf-durable-quorum 1
    stage "${NEW_REF}" FELIX_DURABLE_FSYNC_MODE=on_commit
    write_path_pass c425-rf3-dquorum-oncommit perf-durable-quorum 1
    ;;

  shapes)
    stage "${NEW_REF}" FELIX_DURABLE_FSYNC_MODE=on_commit
    shape_pass c-shape-quorum perf-quorum 0 "${INGEST_GENS}" $(( 24 / INGEST_GENS ))
    shape_pass c-shape-dquorum perf-durable-quorum 1 "${INGEST_GENS}" $(( 24 / INGEST_GENS ))
    ;;
  cache)
    # A Quorum get alone, then under concurrent reads.
    stage "${NEW_REF}"
    trials c425-rf3-qcache-c1 1 --scenario cache --cache perf-quorum --payload-bytes 256 --concurrency 1 --total 20000
    trials c425-rf3-qcache-c8 1 --scenario cache --cache perf-quorum --payload-bytes 256 --concurrency 8 --total 100000
    ;;
  lease-free)
    finalize_features generation_start majority_ack lease_free_reads || exit 1
    # Brokers turn finalized features on as they next hear from the control
    # plane; the history campaign allows the same.
    sleep 30
    export RUN_TAG=lf
    ;;
  explore)
    # Replication rides one connection per peer by default; one cell at 4.
    stage "${NEW_REF}" FELIX_INTERNAL_CONNS_PER_PEER=4
    cell c425-rf3-quorum-peerconns4-ingest-c24-t1 "${INGEST_GENS}" \
        $(ingest_flags perf-quorum "${SHARDS}" $(( 24 / INGEST_GENS )))
    ;;

  *) echo "!! unknown step ${step}" >&2 ;;
  esac
done

finish
