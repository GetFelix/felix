#!/usr/bin/env bash
# Print the newest stable release tag: highest vX.Y.Z among published,
# non-prerelease releases. Previews (vX.Y.Z-preview.N) and `nightly` never match.
# Needs GH_TOKEN. Fails if there is none.
set -euo pipefail

repo=${GITHUB_REPOSITORY:-GetFelix/felix}
tag=$(gh release list -R "$repo" --exclude-drafts --limit 200 \
  --json tagName,isPrerelease --jq '.[] | select(.isPrerelease | not) | .tagName' \
  | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | sort -V | tail -1)
[ -n "$tag" ] || { echo "no stable release found in $repo" >&2; exit 1; }
echo "$tag"
