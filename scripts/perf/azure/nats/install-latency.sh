#!/usr/bin/env bash
# Build nats-latency (nats/latency/) on generator 0 and install it at
# /usr/local/bin/nats-latency. Run after install.sh, which put the CA there.
#
#   SESSION=v060-a ./install-latency.sh [ref]     (default: this branch)
#
# The same path deploy-loadgen.sh builds felix-loadgen by: fetch the ref into
# the generator's felix clone and build with its rustup toolchain, release
# profile, --locked. Only generator 0 runs latency cells, as for Felix.
set -euo pipefail
# shellcheck source=scripts/perf/azure/nats/nats-lib.sh
source "$(cd "$(dirname "$0")" && pwd)/nats-lib.sh"

ref="${1:-${NATS_LAT_REF:-$(git -C "${nats_dir}" rev-parse --abbrev-ref HEAD)}}"
sha="$(git ls-remote https://github.com/GetFelix/felix "refs/heads/${ref}" "${ref}" | awk 'NR==1{print $1}')"
[ -n "${sha}" ] || sha="${ref}"
lg="${LOADGEN_VMS[0]}"
log "building nats-latency ${ref} (${sha}) on ${lg}"

out="$(run_on_str "${lg}" "set -e
H=/home/felix; D=\$H/felix; S=\$H/nats-latency-src
as_felix() { sudo -u felix env HOME=\$H PATH=\$H/.cargo/bin:/usr/bin:/bin \"\$@\"; }
as_felix git -C \$D fetch -q --depth 1 origin ${sha}
rm -rf \$S; as_felix mkdir -p \$S
as_felix git -C \$D archive FETCH_HEAD scripts/perf/azure/nats/latency | as_felix tar -x -C \$S
cd \$S/scripts/perf/azure/nats/latency
as_felix \$H/.cargo/bin/rustup toolchain install >/dev/null 2>&1 || true
as_felix env CARGO_TARGET_DIR=\$H/target-nats-latency \$H/.cargo/bin/cargo build --release --locked -q
install -m 0755 \$H/target-nats-latency/release/nats-latency /usr/local/bin/nats-latency
mkdir -p /etc/nats
printf '%s %s\n' '${ref}' \"\$(as_felix git -C \$D rev-parse FETCH_HEAD)\" > /etc/nats/latency.ref
test -s /etc/nats/tls/ca.pem || { echo '!! no CA at /etc/nats/tls/ca.pem; run install.sh first'; exit 1; }
echo __REF_BEGIN__\$(cat /etc/nats/latency.ref)__REF_END__
echo __RUNOK__")" || { printf '%s\n' "${out}" | tail -20 >&2; exit 1; }
got="$(printf '%s\n' "${out}" | extract_between REF)"
[ -n "${got}" ] || { echo "!! no build ref from ${lg}" >&2; exit 1; }
echo "${got}" > "${here}/sessions/${SESSION}.nats-latency-ref"
log "nats-latency ${got} installed on ${lg}"
