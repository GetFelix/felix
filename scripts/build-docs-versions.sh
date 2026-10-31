#!/usr/bin/env bash
# Build the versioned docs site: the newest stable release at the root, main
# under /next/. Shared by pages.yml (deploy) and ci.yml (PR check) so the check
# builds exactly what ships.
#
#   scripts/build-docs-versions.sh <stable-tag> <out-dir>
#
# The tag's docs-site is built as that tag shipped it. Any failure aborts; main
# is never deployed at the root in place of the tag.
set -euo pipefail

tag=${1:?usage: build-docs-versions.sh <stable-tag> <out-dir>}
out=${2:?usage: build-docs-versions.sh <stable-tag> <out-dir>}
repo=$(git rev-parse --show-toplevel)
work=$(mktemp -d)
trap 'git -C "$repo" worktree remove --force "$work/stable" >/dev/null 2>&1; rm -rf "$work"' EXIT

git -C "$repo" rev-parse --verify --quiet "refs/tags/$tag^{commit}" >/dev/null \
  || { echo "::error::tag $tag is not in this checkout (fetch tags)"; exit 1; }
git -C "$repo" worktree add --detach "$work/stable" "refs/tags/$tag"

echo "::group::Build $tag at /"
npm ci --prefix "$work/stable/docs-site"
env -u DOCS_BASE npm run build --prefix "$work/stable/docs-site"
echo "::endgroup::"

echo "::group::Build main at /next/"
npm ci --prefix "$repo/docs-site"
DOCS_BASE=/next/ npm run build --prefix "$repo/docs-site"
echo "::endgroup::"

rm -rf "$out"
mkdir -p "$out"
cp -R "$work/stable/docs-site/dist/." "$out/"
mkdir "$out/next"
cp -R "$repo/docs-site/dist/." "$out/next/"

# /next/ stays out of search: no sitemap for it, and no pointer to one.
rm -f "$out"/next/sitemap*.xml "$out/next/CNAME"
find "$out/next" -name '*.html' -exec perl -pi -e 's#<link rel="sitemap"[^>]*>##g' {} +
