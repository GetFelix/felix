#!/usr/bin/env bash
# ingest_flags prints loadgen flags as words, split on purpose.
# shellcheck disable=SC2046
# Session B: three D4as_v5 brokers on Premium SSD (P10 at the default 128 GiB),
# two D4as_v5 generators. Runs unattended; re-running resumes.
#
#   #375   in-memory / durable Periodic / durable OnCommit rows side by side,
#          plus the cache put retake (periodic c8, on_commit c1 and c8).
#   #547   the flush-dispatch arms A0/A1/A2 on slow storage: a fix must not
#          regress a ~4 ms flush.
#   shapes payload x batch x fire-and-forget/acked, in-memory and OnCommit.
#   #425   the RF=1 rows session C is compared against (perf, perf-quorum).
#
# Every row runs with FELIX_ACK_ON_COMMIT=1 (the base knobs), so a durable
# latency is a durable latency.
#
# Provision with (see README):
#   SESSION=v060-b LOCATION=westus3 BROKER_COUNT=3 LOADGEN_COUNT=2 SHARDS=12 \
#   BROKER_REFS="main 8f1736eb" ./session.sh
set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=cells.sh
source "${here}/cells.sh"

: "${NEW_REF:=main}"
: "${OLD_REF:=8f1736eb}"
: "${PUBS_PER_GEN:=16}"
: "${STEPS:=fio rows shapes arms rf1}"

record_session session-b
refresh_client_token || exit 1
NGEN="${#LOADGEN_VMS[@]}"
INGEST_GENS="${NGEN}"

for step in ${STEPS}; do
  case "${step}" in
  fio)
    fio_baseline seq256k-j1 256k 1 seq256k-j8 256k 8 seq4k-j1 4k 1 seq4k-j8 4k 8
    ;;

  rows)
    stage "${NEW_REF}" FELIX_DURABLE_FSYNC_MODE=periodic
    write_path_pass b375-inmem perf 0
    write_path_pass b375-periodic perf-durable 1
    durable_trials b375-periodic-cache-c8 1 --scenario cache --cache perf --payload-bytes 256 --concurrency 8 --total 20000

    stage "${NEW_REF}" FELIX_DURABLE_FSYNC_MODE=on_commit
    write_path_pass b375-oncommit perf-durable 1
    # c1 is the per-put device-flush constant; c8 is what group commit buys.
    durable_trials b375-oncommit-cache-c1 1 --scenario cache --cache perf --payload-bytes 256 --concurrency 1 --total 5000
    durable_trials b375-oncommit-cache-c8 1 --scenario cache --cache perf --payload-bytes 256 --concurrency 8 --total 20000
    ;;

  shapes)
    stage "${NEW_REF}" FELIX_DURABLE_FSYNC_MODE=on_commit FELIX_PUB_INGRESS_WAIT=1
    shape_pass b-shape-inmem perf 0 "${NGEN}" "${PUBS_PER_GEN}"
    shape_pass b-shape-dur perf-durable 1 "${NGEN}" "${PUBS_PER_GEN}"
    ;;
  arms)
    for t in $(seq 1 "${TRIALS}"); do
      for arm in A0 A1 A2; do
        case "${arm}" in
          A0) ref="${OLD_REF}"; uring=0 ;;
          A1) ref="${NEW_REF}"; uring=0 ;;
          A2) ref="${NEW_REF}"; uring=1 ;;
        esac
        stage "${ref}" FELIX_STORAGE_IO_URING="${uring}" FELIX_DURABLE_FSYNC_MODE=on_commit \
          FELIX_PUB_INGRESS_WAIT=1
        durable_cell "b547-${arm}-k${SHARDS}-t${t}" "${NGEN}" $(ingest_flags perf-durable "${SHARDS}" "${PUBS_PER_GEN}")
        durable_cell "b547-${arm}-lat-t${t}" 1 --scenario pubsub --stream perf-durable --payload-bytes 256 \
          --fanout 1 --batch 1 --warmup 2000 --total 20000
      done
    done
    ;;

  rf1)
    # The same shapes session C runs at RF=3, same SKU and build.
    stage "${NEW_REF}" FELIX_DURABLE_FSYNC_MODE=periodic
    write_path_pass b425-rf1-leader perf 0
    write_path_pass b425-rf1-quorum perf-quorum 0
    ;;

  *) echo "!! unknown step ${step}" >&2 ;;
  esac
done

finish
