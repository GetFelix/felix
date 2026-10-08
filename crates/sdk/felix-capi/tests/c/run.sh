#!/usr/bin/env bash
# Build the C test against the header and both library forms, then run it.
#
#   tests/c/run.sh offline   no broker needed
#   tests/c/run.sh fixture   against `felix-cluster client-fixture`, which
#                            must already be built (as CI's Python job does)
set -euo pipefail

mode="${1:-offline}"
crate="$(cd "$(dirname "$0")/../.." && pwd)"
repo="$(cd "$crate/../../.." && pwd)"
target="${CARGO_TARGET_DIR:-$repo/target}/debug"
work="$(mktemp -d)"
fixture_pid=""
cleanup() {
  if [ -n "$fixture_pid" ]; then kill "$fixture_pid" 2>/dev/null || true; wait "$fixture_pid" 2>/dev/null || true; fi
  rm -rf "$work"
}
trap cleanup EXIT

cd "$repo"
cargo build --locked -q -p felix-capi
# What a static link needs from the system differs per platform; rustc says.
native="$(cargo rustc --locked -p felix-capi --lib --crate-type staticlib -- --print native-static-libs 2>&1 \
  | sed -n 's/.*native-static-libs: //p' | tail -n 1)"
if [ -z "$native" ]; then
  echo "rustc did not report the static library's native dependencies" >&2
  exit 1
fi

cflags=(-std=c11 -Wall -Wextra -Werror -I "$crate/include")
case "$(uname -s)" in
  Darwin) shared="$target/libfelix.dylib" ;;
  *) shared="$target/libfelix.so" ;;
esac
cc "${cflags[@]}" "$crate/tests/c/pubsub.c" "$shared" -Wl,-rpath,"$target" -o "$work/pubsub-shared"
# shellcheck disable=SC2086 # the native library list is meant to split
cc "${cflags[@]}" "$crate/tests/c/pubsub.c" "$target/libfelix.a" $native -o "$work/pubsub-static"

if [ "$mode" = offline ]; then
  "$work/pubsub-shared" offline
  "$work/pubsub-static" offline
  exit 0
fi

RUST_LOG=warn "$target/felix-cluster" client-fixture \
  --out "$work/fixture.json" --ca-file "$work/ca.pem" >"$work/fixture.log" 2>&1 &
fixture_pid=$!
for _ in $(seq 1 600); do
  if [ -s "$work/fixture.json" ] && [ -s "$work/ca.pem" ] &&
    python3 -c 'import json,sys; json.load(open(sys.argv[1]))' "$work/fixture.json" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$fixture_pid" 2>/dev/null; then
    echo "the fixture exited during startup:" >&2
    cat "$work/fixture.log" >&2
    exit 1
  fi
  sleep 0.2
done
if [ ! -s "$work/fixture.json" ]; then
  echo "the fixture was not ready in time" >&2
  cat "$work/fixture.log" >&2
  exit 1
fi

args=()
while IFS= read -r line; do args+=("$line"); done < <(python3 - "$work/fixture.json" <<'PY'
import json, sys
f = json.load(open(sys.argv[1]))
for value in (",".join(f["addrs"]), f["tenant_id"], f["namespace"], f["token"],
              f["ca_file"], f["durable_stream"], f["missing_stream"],
              f["unauthorized_token"]):
    print(value)
PY
)

"$work/pubsub-shared" offline
"$work/pubsub-static" offline
"$work/pubsub-shared" fixture "${args[@]}"
"$work/pubsub-static" fixture "${args[@]}"
