#!/usr/bin/env bash
# Claim the npm package names for a release, by hand, from a maintainer's own
# machine.
#
# # Why this is not the release workflow's job
# npm has no way for CI to create a package that does not exist yet. npm's docs
# require the package to exist before a trusted publisher can be configured
# (npm/cli#8544 tracks lifting that), and `npm stage publish` stages a new
# version of an existing package -- it answers 404 for a name it has never
# seen:
#
#   POST /-/stage/package/felix-client-darwin-arm64
#   404 Package "felix-client-darwin-arm64" not found
#
# What is left is a direct publish, which needs either a token that bypasses
# 2FA -- restricted, and losing the ability to publish around January 2027 --
# or a person who can answer the 2FA prompt.
#
# A person, then. Once per package name, ever: after this, the packages exist,
# trusted publishers can be configured on them, and CI publishes every later
# version with no credential at all.
#
# The binaries come from the GitHub release rather than a local build, so what
# is published is what CI built and what the conformance suite ran against.
set -euo pipefail

# Every npm call here is pinned to the public registry. A machine with a
# corporate mirror in ~/.npmrc -- which is a normal thing to have -- would
# otherwise publish Felix to an internal feed, or fail confusingly against one.
# Never drop this flag, and never rely on the ambient registry setting.
REGISTRY="https://registry.npmjs.org/"

tag="${1:-}"
if [ -z "$tag" ]; then
  echo "usage: $0 <tag>   e.g. $0 v0.5.0" >&2
  exit 2
fi

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pkg="$root/crates/sdk/felix-typescript"
version="${tag#v}"

echo "== checking this machine can publish at all"

configured="$(npm config get registry)"
if [ "$configured" != "$REGISTRY" ]; then
  echo "  note: npm is configured for ${configured}"
  echo "        every command here overrides it with ${REGISTRY}"
fi

# Before anything is downloaded or packed: a network that cannot reach the
# registry is the common case on a corporate VPN, and finding out at the upload
# wastes the run and leaves binaries lying around.
if ! curl -sf -o /dev/null --max-time 15 "${REGISTRY}npm" 2>/dev/null; then
  cat >&2 <<'BLOCKED'
error: this network cannot reach https://registry.npmjs.org/.

  A corporate proxy or VPN is in the way. Publishing has to happen from a
  network that can reach the public registry -- a home connection or a phone
  hotspot is enough. Nothing has been changed or published.
BLOCKED
  exit 1
fi

if ! npm whoami --registry="$REGISTRY" >/dev/null 2>&1; then
  cat >&2 <<BLOCKED
error: not logged in to ${REGISTRY}.

  Run:  npm login --registry=${REGISTRY}

  Log in against the public registry explicitly. A corporate mirror in
  ~/.npmrc means a plain \`npm login\` authenticates to the wrong place.
BLOCKED
  exit 1
fi
echo "  publishing to ${REGISTRY} as $(npm whoami --registry="$REGISTRY")"
echo

manifest_version="$(node -p "require('$pkg/package.json').version")"
if [ "$manifest_version" != "$version" ]; then
  echo "error: $tag is $version but package.json is $manifest_version." >&2
  echo "Check out the tag before running this." >&2
  exit 1
fi

echo "== fetching the binaries $tag published"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# `gh` where it exists, plain HTTPS where it does not -- Azure Cloud Shell and
# most minimal containers have curl and no gh, and the release is public, so
# there is nothing to authenticate to.
if command -v gh >/dev/null 2>&1; then
  gh release download "$tag" --repo GetFelix/felix --pattern '*.node' --dir "$work"
else
  echo "  no gh; downloading over https"
  for dir in "$pkg"/npm/*/; do
    node_file="felix.$(basename "$dir").node"
    url="https://github.com/GetFelix/felix/releases/download/${tag}/${node_file}"
    if ! curl -fsSL --max-time 300 -o "$work/$node_file" "$url"; then
      echo "error: could not download $node_file from $tag" >&2
      exit 1
    fi
  done
fi

echo
echo "== placing each binary in its platform package"
for dir in "$pkg"/npm/*/; do
  triple="$(basename "$dir")"
  node_file="felix.${triple}.node"
  if [ ! -f "$work/$node_file" ]; then
    echo "error: $tag has no $node_file, so $triple cannot be published." >&2
    exit 1
  fi
  cp "$work/$node_file" "$dir$node_file"
  printf '  %-22s %s\n' "$triple" "$(du -h "$dir$node_file" | cut -f1)"
done

# npm refuses a prerelease without --tag, so that `latest` stays on the last
# stable version. Tag it with its prerelease id (0.6.0-preview -> preview).
version="$(node -p "require('$pkg/package.json').version")"
tag_args=()
if [[ "$version" == *-* ]]; then
  pre="${version#*-}"
  tag_args=(--tag "${pre%%.*}")
fi

echo
echo "== publishing, platform packages first"
echo "npm will ask for your one-time password, once per package. Pass"
echo "--otp=<code> through if you would rather not be prompted six times."
echo "This is the last time either way: trusted publishing takes over as soon"
echo "as these names exist."
echo
for dir in "$pkg"/npm/*/; do
  echo "-- $(basename "$dir")"
  ( cd "$dir" && npm publish --registry="$REGISTRY" ${tag_args[@]+"${tag_args[@]}"} )
done
echo "-- felix-client"
( cd "$pkg" && npm publish --registry="$REGISTRY" ${tag_args[@]+"${tag_args[@]}"} )

echo
echo "== confirming the registry has them"
missing=0
for name in $(node -p "Object.keys(require('$pkg/package.json').optionalDependencies).join(' ')") felix-client; do
  if npm view "${name}@${version}" version --registry="$REGISTRY" >/dev/null 2>&1; then
    echo "  ok      ${name}@${version}"
  else
    echo "  MISSING ${name}@${version}"
    missing=1
  fi
done

if [ "$missing" -ne 0 ]; then
  echo
  echo "Some packages did not publish. Rerun; npm refuses a duplicate version," >&2
  echo "so the ones that landed will report EPUBLISHCONFLICT and the rest go." >&2
  exit 1
fi

cat <<DONE

All six are on npm. Two things to do now, once:

  1. Configure a trusted publisher on each package -- repository GetFelix/felix,
     workflow release.yml, environment npm.
  2. Delete the NPM_TOKEN secret. Nothing reads it once npm_stage is off.

Every later release then publishes from CI with no credential at all.
DONE
