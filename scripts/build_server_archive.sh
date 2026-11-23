#!/usr/bin/env bash
# Builds the Linux x86_64 server release archive so the same commit gives the
# same bytes wherever it is built. Run from the root of a checkout.
#
#   scripts/build_server_archive.sh <tag> <out-dir>
#
# Writes <out-dir>/felix-<tag>-linux-x86_64.tar.gz. The release workflow runs
# this twice on separate runners and refuses to publish unless the sha256 of
# the two archives match. See "Reproduce a release" in docs-site
# (security/reproducible-builds).
set -euo pipefail

tag=${1:?usage: build_server_archive.sh <tag> <out-dir>}
out=${2:?usage: build_server_archive.sh <tag> <out-dir>}
root=$(pwd)
mkdir -p "$out"
out=$(cd "$out" && pwd)

# The commit time is the only clock the build sees.
SOURCE_DATE_EPOCH=$(git log -1 --format=%ct)
export SOURCE_DATE_EPOCH
export LC_ALL=C TZ=UTC

# Absolute paths end up in panic messages, debug info and C __FILE__ strings.
# Rewrite the checkout, the cargo home and the toolchain to fixed names.
cargo_home=${CARGO_HOME:-$HOME/.cargo}
sysroot=$(rustc --print sysroot)
if [ -n "${CARGO_ENCODED_RUSTFLAGS:-}" ]; then
  echo "CARGO_ENCODED_RUSTFLAGS is set and takes precedence over the flags this script needs; unset it" >&2
  exit 1
fi
export RUSTFLAGS="--remap-path-prefix=${root}=/build/felix --remap-path-prefix=${cargo_home}=/build/cargo --remap-path-prefix=${sysroot}=/build/rustc"
export CFLAGS="-ffile-prefix-map=${root}=/build/felix -ffile-prefix-map=${cargo_home}=/build/cargo"
export CXXFLAGS="$CFLAGS"
# A cache from another path would reuse artifacts built under it.
export CARGO_TARGET_DIR="${root}/target"
unset CARGO_INCREMENTAL

echo "build environment"
rustc -vV
cc --version | head -1
ld --version | head -1
gzip --version | head -1
tar --version | head -1

cargo build --release --locked \
  -p felix-broker-service --bin felix-broker \
  -p felix-controlplane-service --bin felix-controlplane

name="felix-${tag}-linux-x86_64"
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
mkdir "$stage/$name"
install -m 0755 target/release/felix-broker target/release/felix-controlplane "$stage/$name/"
install -m 0644 README.md LICENSE "$stage/$name/"
chmod 0755 "$stage/$name"

# Sorted names, fixed owner and mtime, GNU format (no pax timestamps), and no
# name or time in the gzip header.
tar -C "$stage" --sort=name --format=gnu \
  --mtime="@${SOURCE_DATE_EPOCH}" --owner=0 --group=0 --numeric-owner \
  -cf - "$name" | gzip -n -9 > "$out/$name.tar.gz"

sha256sum "$out/$name.tar.gz"
