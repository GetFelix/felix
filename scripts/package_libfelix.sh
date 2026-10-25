#!/usr/bin/env bash
# Packages the felix-capi libraries already built in target/release into a
# release archive, then proves the archive links and loads both ways.
#
#   scripts/package_libfelix.sh <tag> <rid> <outdir>
#
# <rid> is the NuGet runtime id (linux-x64, osx-arm64, win-x64, ...). Writes
# libfelix-<tag>-<rid>.tar.gz (.zip on Windows) and its .sha256 to <outdir>.
# Windows needs cl on PATH (a Developer Command Prompt environment).
set -euo pipefail

tag="$1"
rid="$2"
out="$3"
repo="$(cd "$(dirname "$0")/.." && pwd)"
crate="$repo/crates/sdk/felix-capi"
release="${CARGO_TARGET_DIR:-$repo/target}/release"
root="libfelix-${tag}-${rid}"

case "$rid" in
  win-*)   shared=felix.dll; static=felix.lib; import=felix.dll.lib ;;
  osx-*)   shared=libfelix.dylib; static=libfelix.a; import="" ;;
  *)       shared=libfelix.so; static=libfelix.a; import="" ;;
esac

cd "$repo"
# What a static link needs from the system differs per platform; rustc says.
# --color never, or CARGO_TERM_COLOR=always leaves an escape code in the list.
# Run before copying: it may relink the static library.
native="$(cargo rustc --color never --locked --release -p felix-capi --lib --crate-type staticlib -- --print native-static-libs 2>&1 \
  | sed -n 's/.*native-static-libs: //p' | tail -n 1)"
if [ -z "$native" ]; then
  echo "rustc did not report the static library's native dependencies" >&2
  exit 1
fi
case "$rid" in
  win-*)
    # MSVC takes library files, not -l flags.
    converted=""
    for lib in $native; do
      case "$lib" in -l*) lib="${lib#-l}.lib" ;; esac
      converted="$converted $lib"
    done
    native="${converted# }" ;;
esac

stage="$(mktemp -d)"
trap 'rm -rf "$stage"' EXIT
mkdir -p "$stage/$root/lib" "$stage/$root/include"
cp "$release/$shared" "$release/$static" "$stage/$root/lib/"
if [ -n "$import" ] && [ -f "$release/$import" ]; then
  cp "$release/$import" "$stage/$root/lib/"
fi
cp "$crate/include/felix.h" "$stage/$root/include/"
cp "$crate/LICENSE" "$stage/$root/"

case "$rid" in
  win-*)
    dyn_cmd="cl -I include app.c lib\\felix.dll.lib"
    static_cmd="cl -I include app.c lib\\felix.lib $native"
    run_note="Put lib\\felix.dll next to the program or on PATH when it runs." ;;
  *)
    dyn_cmd="cc -I include app.c -L lib -lfelix -Wl,-rpath,\"\$PWD/lib\""
    static_cmd="cc -I include app.c lib/$static $native"
    run_note="Set the rpath to wherever the shared library is installed." ;;
esac
cat >"$stage/$root/LINKING.txt" <<TXT
libfelix ${tag} for ${rid}

include/felix.h        the C header
lib/$shared
lib/$static
TXT
if [ -n "$import" ] && [ -f "$release/$import" ]; then echo "lib/$import" >>"$stage/$root/LINKING.txt"; fi
cat >>"$stage/$root/LINKING.txt" <<TXT

Shared (from this directory):
  $dyn_cmd
$run_note

Static (from this directory), with the system libraries this platform needs:
  $static_cmd
TXT

# Smoke check from the staged tree, as a user would build it.
smoke="$stage/smoke"
mkdir -p "$smoke"
cp "$crate/tests/c/archive_smoke.c" "$stage/$root/app.c"
(
  cd "$stage/$root"
  case "$rid" in
    win-*)
      cl -nologo -W3 -WX -I include app.c lib/felix.dll.lib -Fe"../smoke/shared.exe" -Fo"../smoke/shared.obj"
      cl -nologo -W3 -WX -I include app.c lib/felix.lib $native -Fe"../smoke/static.exe" -Fo"../smoke/static.obj"
      PATH="$PWD/lib:$PATH" "../smoke/shared.exe"
      "../smoke/static.exe" ;;
    *)
      cc -std=c11 -pthread -Wall -Wextra -Werror -I include app.c -L lib -lfelix -Wl,-rpath,"$PWD/lib" -o ../smoke/shared
      # shellcheck disable=SC2086 # the native library list is meant to split
      cc -std=c11 -pthread -Wall -Wextra -Werror -I include app.c lib/"$static" $native -o ../smoke/static
      ../smoke/shared
      ../smoke/static ;;
  esac
)
rm "$stage/$root/app.c"

mkdir -p "$out"
out="$(cd "$out" && pwd)"
case "$rid" in
  win-*) archive="$root.zip";    ( cd "$stage" && 7z a -tzip -bso0 "$out/$archive" "$root" ) ;;
  *)     archive="$root.tar.gz"; tar -C "$stage" -czf "$out/$archive" "$root" ;;
esac
( cd "$out" && { sha256sum "$archive" 2>/dev/null || shasum -a 256 "$archive"; } >"$archive.sha256" )
echo "wrote $out/$archive"
