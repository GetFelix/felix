#!/usr/bin/env bash
# Run the metadata Raft benchmark (services/felix-controlplane-service/tests/metadata_bench.rs).
#   metadata_bench.sh OUT.json [quick|full]
# From a checkout; builds in release. Results go to OUT.json as well as stdout.
set -euo pipefail
out=${1:?usage: metadata_bench.sh OUT.json [quick|full]}
scale=${2:-quick}
case "$scale" in quick | full) ;; *) echo "scale must be quick or full" >&2; exit 2 ;; esac
FELIX_METADATA_BENCH_OUT=$(realpath -m "$out") FELIX_METADATA_BENCH_SCALE=$scale \
  cargo test -p felix-controlplane-service --release --test metadata_bench -- --ignored --nocapture
