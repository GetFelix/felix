---
title: "Reproduce a Release"
description: "Rebuild the Linux x86_64 server archive from a tag and check that its sha256 matches the one the release published."
---

The Linux x86_64 server archive (`felix-<tag>-linux-x86_64.tar.gz`) is built to
be byte-for-byte reproducible. The same commit built again gives the same
file, so you can rebuild a release yourself and compare, instead of trusting
that the published archive came from the published source.

## How the release checks itself

The release workflow builds the archive twice, on two separate runners, from
two different checkout paths, with no shared build cache. It compares the
sha256 of the two archives and stops the release if they differ. The published
`SHA256SUMS` is the signed record of the sha256; see
[Verify a release download](/getting-started/installation/#verify-a-release-download).

What makes the output the same:

- The Rust toolchain is pinned by `rust-toolchain.toml`, and dependencies by `Cargo.lock` (`--locked`).
- Checkout, cargo home and toolchain paths are rewritten to fixed names, in
  Rust (`--remap-path-prefix`) and in C code (`-ffile-prefix-map`).
- The archive has sorted entries, root ownership, a fixed mode, and an mtime of
  the commit time. gzip runs with `-n`, so its header carries no name or time.
- The shipped crates have no build script or `env!` that records a date or host. The check is the byte comparison above.

## Reproduce it

You need the same build environment the release used: Ubuntu 24.04 with
`rustup`, a C toolchain (`build-essential`, `cmake`) and GNU tar. The release
log prints the exact `rustc`, `cc`, `ld`, `gzip` and `tar` versions it used.
A different linker or C compiler version can change the bytes.

```bash
tag=v0.8.0   # the release you are checking
git clone --branch "$tag" https://github.com/GetFelix/felix.git
cd felix
scripts/build_server_archive.sh "$tag" out
sha256sum out/felix-"$tag"-linux-x86_64.tar.gz
```

Download `SHA256SUMS` from the release into `out/` and check:

```bash
cd out
grep linux-x86_64 SHA256SUMS | sha256sum -c
```

It should print `OK`. If they are not, check the tool versions
in the release's build log against `rustc -vV`, `cc --version`, `ld --version`
and `gzip --version` on your machine before suspecting the archive.

The script sets `RUSTFLAGS` itself, so a `RUSTFLAGS` of your own is replaced, and it refuses to run with `CARGO_ENCODED_RUSTFLAGS` set. Keep `CARGO_HOME` and the rustup toolchain outside the checkout, or the path rewriting changes and the bytes differ.

Releases before the first one carrying this change are not reproducible.

Only the x86_64 server archive is checked this way. The aarch64 archive and
the container images are not yet.
