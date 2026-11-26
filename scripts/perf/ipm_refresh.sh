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
git fetch -q origin "$ref"
sha=$(git rev-parse "origin/$ref")
me=$(gh api user -q .login)
dir=$(mktemp -d)
ids=()

# The id of the run this dispatch started: one of ours, on this branch, at this
# commit, created after the dispatch, and not one already taken. Fails rather
# than guess.
find_run() {
  local since=$1 found
  for _ in $(seq 1 30); do
    found=$(gh run list -R "$repo" -w ipm-gate.yml -b "$ref" -e workflow_dispatch -u "$me" -L 30 \
      --json databaseId,headSha,createdAt |
      python3 -I -c '
import json, sys
since, sha, taken = sys.argv[1], sys.argv[2], set(sys.argv[3:])
runs = [r for r in json.load(sys.stdin)
        if r["headSha"] == sha and r["createdAt"] >= since and str(r["databaseId"]) not in taken]
print(min((r["databaseId"] for r in runs), default=""))' "$since" "$sha" "${ids[@]+"${ids[@]}"}")
    if [ -n "$found" ]; then
      echo "$found"
      return 0
    fi
    sleep 4
  done
  return 1
}

for _ in 1 2 3 4 5; do
  since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  gh workflow run ipm-gate.yml -R "$repo" --ref "$ref" >/dev/null
  id=$(find_run "$since") || { echo "could not identify the run dispatched at $since on $ref ($sha)" >&2; exit 1; }
  ids+=("$id")
done
echo "dispatched ${ids[*]} on $sha"
for id in "${ids[@]}"; do
  # The compare step may fail on an old baseline; only the artifacts matter.
  gh run watch "$id" -R "$repo" --interval 60 >/dev/null || true
  gh run download "$id" -R "$repo" -D "$dir/$id"
done
joined=$(IFS=,; echo "${ids[*]}")
for arch in x86_64 aarch64; do
  # --commit refuses any result that was not measured at $sha.
  python3 "$(dirname "$0")/ipm_gate.py" update --commit "$sha" --run-ids "$joined" \
    "$dir"/*/ipm-result-"$arch"/ipm-result.json
done
