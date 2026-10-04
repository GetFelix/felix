#!/bin/sh
# Loadgen provisioning and the session's build service, run as root. main.bicep
# ships this file base64-encoded (so, unlike the cloud-init templates that go
# through Bicep's format(), it may use braces), and deploy-ref.sh re-ships the
# current copy whenever it builds mid-session.
#
#   provision-loadgen.sh provision <loadgen-spec> <broker-specs> <fp-specs>
#   provision-loadgen.sh build <spec> <fp:0|1>
#
# A spec is `name@sha`; session.sh and deploy-ref.sh resolve every ref to a
# full SHA, so a short SHA or a moving branch cannot change under a session.
# `provision` builds felix-loadgen on every generator; on generator 0 it also
# builds each broker spec. `build` builds one more spec on generator 0. Builds
# are packed as release-layout tarballs under /srv/felix/<name>/ and served to
# the VNet on :8088; brokers and the control plane pull from there.
set -eu

MODE="${1:?usage: provision-loadgen.sh provision|build ...}"
shift
REPO=https://github.com/GetFelix/felix
HOME_DIR=/home/felix
CARGO="$HOME_DIR/.cargo/bin/cargo"
RUSTUP="$HOME_DIR/.cargo/bin/rustup"
# Plain builds share one target dir so the second ref reuses compiled deps.
# The frame-pointer variant changes RUSTFLAGS, which invalidates everything,
# so it gets its own dir instead of thrashing the shared one.
TARGET="$HOME_DIR/target"
TARGET_FP="$HOME_DIR/target-fp"
OUT=/srv/felix
STATUS="$OUT/BUILD_STATUS"

as_felix() {
  sudo -u felix env HOME="$HOME_DIR" PATH="$HOME_DIR/.cargo/bin:/usr/bin:/bin" "$@"
}

# checkout <dir> <sha>: fetch exactly one commit. `clone --branch` cannot take
# a SHA, and its old `|| clone` fallback silently built the default branch.
checkout() {
  dir="$1"; sha="$2"
  rm -rf "$dir"
  as_felix git init -q "$dir"
  as_felix git -C "$dir" remote add origin "$REPO"
  as_felix git -C "$dir" fetch -q --depth 1 origin "$sha"
  as_felix git -C "$dir" checkout -q --detach FETCH_HEAD
  got=$(as_felix git -C "$dir" rev-parse HEAD)
  [ "$got" = "$sha" ] || { echo "!! $dir is at $got, wanted $sha" >&2; return 1; }
  # rust-toolchain.toml pins the compiler; install it explicitly rather than
  # relying on rustup's implicit install, which some rustup versions disable.
  (cd "$dir" && as_felix "$RUSTUP" toolchain install >/dev/null 2>&1) || true
}

# build_ref <name> <sha> <fp:0|1>
build_ref() {
  name="$1"; sha="$2"; fp="$3"
  label="$name"; target="$TARGET"; flags=""; debug=false
  if [ "$fp" = 1 ]; then
    # line-tables-only keeps perf's symbolisation useful without the size of
    # full debug info.
    label="$name-fp"; target="$TARGET_FP"
    flags="-C force-frame-pointers=yes"; debug=line-tables-only
  fi
  src="$HOME_DIR/builds/$label"
  log="/var/log/felix-build-$label.log"
  mkdir -p "$OUT/.status"
  echo running > "$OUT/.status/$label"
  echo ">> building $label ($sha); log $log"
  # A failed ref is recorded and the others still build; seeding fails loudly
  # on the missing tarball if the session actually needs it.
  if checkout "$src" "$sha" >"$log" 2>&1 \
    && (cd "$src" && as_felix env CARGO_TARGET_DIR="$target" RUSTFLAGS="$flags" \
      CARGO_PROFILE_RELEASE_DEBUG="$debug" CARGO_PROFILE_RELEASE_STRIP=none \
      "$CARGO" build --release --locked \
        -p felix-broker-service --bin felix-broker \
        -p felix-controlplane-service --bin felix-controlplane) >>"$log" 2>&1; then
    root="felix-$label-linux-x86_64"
    stage="$OUT/$label/$root"
    rm -rf "${OUT:?}/$label"; mkdir -p "$stage"
    cp "$target/release/felix-broker" "$target/release/felix-controlplane" "$stage/"
    printf '%s %s%s\n' "$label" "$sha" "$([ "$fp" = 1 ] && echo ' frame-pointers')" > "$stage/REF"
    tar -C "$OUT/$label" -czf "$OUT/$label/$root.tar.gz" "$root"
    rm -rf "$stage"
    (cd "$OUT/$label" && sha256sum "$root.tar.gz" > SHA256SUMS)
    echo "$label ok $sha" >> "$STATUS"
    echo ok > "$OUT/.status/$label"
  else
    echo "$label failed $sha" >> "$STATUS"
    echo failed > "$OUT/.status/$label"
    tail -20 "$log" >&2
  fi
}

serve() {
  [ -e /etc/systemd/system/felix-artifacts.service ] || {
    cat > /etc/systemd/system/felix-artifacts.service <<'UNIT'
[Unit]
Description=Serve Felix build artifacts to the session VNet
After=network-online.target

[Service]
ExecStart=/usr/bin/python3 -m http.server 8088 --directory /srv/felix
Restart=always

[Install]
WantedBy=multi-user.target
UNIT
    systemctl daemon-reload
  }
  systemctl enable --now felix-artifacts
}

on_exit() {
  rc=$?
  [ "$rc" -eq 0 ] || touch /var/lib/cloud/instance/felix-provision-failed
}

case "$MODE" in
  provision)
    # session.sh stops waiting as soon as this appears, instead of sitting out
    # the whole provisioning timeout on a build that already failed.
    trap on_exit EXIT
    lg_spec="$1"; broker_specs="${2:-}"; fp_specs="${3:-}"
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sudo -u felix sh -s -- -y --profile minimal
    lg_name="${lg_spec%@*}"; lg_sha="${lg_spec#*@}"
    echo ">> felix-loadgen from $lg_name ($lg_sha)"
    checkout "$HOME_DIR/felix" "$lg_sha"
    (cd "$HOME_DIR/felix" && as_felix env CARGO_TARGET_DIR="$TARGET" "$CARGO" build --release --locked -p felix-loadgen)
    install -m 0755 "$TARGET/release/felix-loadgen" /usr/local/bin/felix-loadgen
    mkdir -p /etc/felix
    printf '%s %s\n' "$lg_name" "$lg_sha" > /etc/felix/loadgen.ref
    if [ "$(hostname)" = felixperf-loadgen ] && [ -n "$broker_specs" ]; then
      mkdir -p "$OUT"; : > "$STATUS"
      for spec in $broker_specs; do build_ref "${spec%@*}" "${spec#*@}" 0; done
      for spec in $fp_specs; do build_ref "${spec%@*}" "${spec#*@}" 1; done
      serve
    fi
    touch /var/lib/cloud/instance/felix-provisioned
    ;;
  build)
    spec="$1"; fp="${2:-0}"
    mkdir -p "$OUT"; touch "$STATUS"
    build_ref "${spec%@*}" "${spec#*@}" "$fp"
    serve
    ;;
  *) echo "!! unknown mode $MODE" >&2; exit 2 ;;
esac
