#!/usr/bin/env bash
# Download the control plane binary of the last release that ran openraft 0.9
# and print its path. The mixed-version test in
# services/felix-controlplane-service/tests/raft_runtime.rs runs it next to
# the current build. Linux x86_64 only: that is what the release ships.
set -euo pipefail

tag=v0.7.1
dest=${1:?usage: fetch-released-controlplane.sh <dir>}
name="felix-$tag-linux-x86_64"

mkdir -p "$dest"
curl --fail --silent --show-error --location --retry 3 \
  --output "$dest/$name.tar.gz" \
  "https://github.com/GetFelix/felix/releases/download/$tag/$name.tar.gz"
tar -xzf "$dest/$name.tar.gz" -C "$dest"
echo "$dest/$name/felix-controlplane"
