#!/usr/bin/env bash
# Refresh scripts/perf/ipm-baseline.toml: dispatch the Instructions per message
# workflow five times on one commit, wait, download the ten result files and
# rewrite both architectures' sections from their medians.
#
#   scripts/perf/ipm_refresh.sh [REF]     REF is a pushed branch (default: the current one)
#
# Run it from a branch whose head is pushed, and commit the rewritten baseline
# afterwards. See "Instructions per message" in CONTRIBUTING.md.
set -euo pipefail
repo=GetFelix/felix
ref=${1:-$(git rev-parse --abbrev-ref HEAD)}
sha=$(git rev-parse "origin/$ref")
dir=$(mktemp -d)
ids=()
for _ in 1 2 3 4 5; do
  gh workflow run ipm-gate.yml -R "$repo" --ref "$ref"
  sleep 8
  ids+=("$(gh run list -R "$repo" -w ipm-gate.yml -b "$ref" -e workflow_dispatch -L 1 --json databaseId -q '.[0].databaseId')")
done
echo "dispatched ${ids[*]} on $sha"
for id in "${ids[@]}"; do
  # The compare step may fail on an old baseline; only the artifacts matter.
  gh run watch "$id" -R "$repo" --interval 60 >/dev/null || true
  gh run download "$id" -R "$repo" -D "$dir/$id"
done
for arch in x86_64 aarch64; do
  python3 "$(dirname "$0")/ipm_gate.py" update "$dir"/*/ipm-result-"$arch"/ipm-result.json
done
