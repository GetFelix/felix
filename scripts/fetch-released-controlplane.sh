#!/usr/bin/env bash
# Download the control plane binary of a release that kept its store in
# raft.redb and print its path. A test in
# services/felix-controlplane-service/tests/raft_runtime.rs checks that the
# current build refuses a data directory that release wrote. Linux x86_64
# only: that is what the release ships.
set -euo pipefail

tag=v0.7.1
# sha256 of the v0.7.1 linux-x86_64 archive; bump it with the tag.
sha256=56bf7de15e6f4463bd815ea0e6ee6b7fff1ce9a53f5c8ccaad94ecfd266f2c3f
dest=${1:?usage: fetch-released-controlplane.sh <dir>}
name="felix-$tag-linux-x86_64"

mkdir -p "$dest"
curl --proto '=https' --tlsv1.2 --fail --silent --show-error --location --retry 3 \
  --output "$dest/$name.tar.gz" \
  "https://github.com/GetFelix/felix/releases/download/$tag/$name.tar.gz"
echo "$sha256  $dest/$name.tar.gz" | sha256sum --check --quiet -
tar -xzf "$dest/$name.tar.gz" -C "$dest"
echo "$dest/$name/felix-controlplane"
