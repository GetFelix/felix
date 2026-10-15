#!/usr/bin/env bash
# Rebuild felix-loadgen from any git ref on every generator of a live session
# and install it in place — no reprovision. For client-side changes: the
# brokers keep whatever deploy-ref.sh gave them.
#
#   SESSION=<name> ./deploy-loadgen.sh <branch|tag|sha>
#
# Refuses while a felix-loadgen is running anywhere (FORCE=1 overrides).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
. "${here}/lib.sh"
: "${SESSION:?set SESSION}"
# shellcheck source=/dev/null
. "${here}/sessions/${SESSION}.env"
ref="${1:?usage: deploy-loadgen.sh <ref>}"
sha="$(git ls-remote https://github.com/GetFelix/felix "${ref}" | awk 'NR==1{print $1}')"
[ -n "${sha}" ] || sha="${ref}"
read -r -a vms <<< "${LOADGENS}"

if [ "${FORCE:-0}" != 1 ]; then
  for vm in "${vms[@]}"; do
    if run_on_str "${vm}" 'pgrep -x felix-loadgen >/dev/null && echo __BUSY_BEGIN__1__BUSY_END__; echo __RUNOK__' \
      | extract_between BUSY | grep -q 1; then
      echo "!! felix-loadgen is running on ${vm}; wait for the run or set FORCE=1" >&2
      exit 1
    fi
  done
fi

# Checkout, build and install in one remote script; the target dir is kept, so
# only the workspace crates rebuild.
script="set -e
H=/home/felix; D=\$H/felix
as_felix() { sudo -u felix env HOME=\$H PATH=\$H/.cargo/bin:/usr/bin:/bin \"\$@\"; }
as_felix git -C \$D fetch -q --depth 1 origin ${sha}
as_felix git -C \$D checkout -q --detach FETCH_HEAD
cd \$D && as_felix env CARGO_TARGET_DIR=\$H/target \$H/.cargo/bin/cargo build --release --locked -q -p felix-loadgen
install -m 0755 \$H/target/release/felix-loadgen /usr/local/bin/felix-loadgen
printf '%s %s\n' '${ref}' \"\$(as_felix git -C \$D rev-parse HEAD)\" > /etc/felix/loadgen.ref
echo __REF_BEGIN__\$(cat /etc/felix/loadgen.ref)__REF_END__
echo __RUNOK__"

pids=()
for vm in "${vms[@]}"; do
  ( run_on_str "${vm}" "${script}" | extract_between REF | sed "s/^/   ${vm}: /" ) &
  pids+=("$!")
done
rc=0
for p in "${pids[@]}"; do wait "${p}" || rc=1; done
if [ "${rc}" = 0 ]; then
  echo "${ref}@${sha}" > "${here}/sessions/${SESSION}.loadgen-ref"
  echo ">> felix-loadgen ${ref} (${sha}) installed on ${#vms[@]} generators"
fi
exit "${rc}"
