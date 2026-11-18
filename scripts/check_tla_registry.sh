#!/usr/bin/env bash
# Every docs/formal/*.cfg is checked somewhere or says why not: registered in
# scripts/check_tla.sh (expectations or walks), named by a workflow (the
# nightly tla-walk.yml jobs), or listed in scripts/tla_unregistered.txt (#2142).
# Kept out of check_tla.sh so editing the list does not change which
# configurations a pull request model-checks.
set -euo pipefail

cd "$(dirname "$0")/.."

registered="$(sed -nE '/^(expectations|walks)=\($/,/^\)$/p' scripts/check_tla.sh \
  | sed -n 's/^  "\([A-Za-z0-9_]*\) .*/\1/p')"
unregistered="$(grep -v '^#' scripts/tla_unregistered.txt | awk 'NF { print $1 }')"

status=0
while read -r name; do
  [ -n "$name" ] || continue
  if [ ! -f "docs/formal/$name.cfg" ]; then
    echo "scripts/tla_unregistered.txt names $name, which has no docs/formal/$name.cfg" >&2
    status=1
  elif grep -qxF "$name" <<<"$registered"; then
    echo "$name is registered in scripts/check_tla.sh and listed in scripts/tla_unregistered.txt" >&2
    status=1
  fi
done <<<"$unregistered"

for cfg in docs/formal/*.cfg; do
  name="$(basename "$cfg" .cfg)"
  grep -qxF "$name" <<<"$registered" && continue
  grep -qxF "$name" <<<"$unregistered" && continue
  grep -qwF "$name" .github/workflows/tla-walk.yml .github/workflows/ci.yml && continue
  echo "$cfg is not registered in scripts/check_tla.sh, run by a workflow, or listed in scripts/tla_unregistered.txt" >&2
  status=1
done
exit "$status"
