#!/usr/bin/env bash
# Tests for `scripts/check_tla.sh --affected`, the list of configurations a
# pull request model-checks. Each case commits a change in a scratch repo
# holding this script, ci.yml and stub specs, and compares the list. No TLC.
set -euo pipefail

src="$(cd "$(dirname "$0")/.." && pwd)"
work="$(mktemp -d "${TMPDIR:-/tmp}/test_check_tla.XXXXXX")"
trap 'rm -rf "$work"' EXIT

git_() { git -C "$work" -c user.name=test -c user.email=test@example.invalid "$@"; }

mkdir -p "$work/scripts" "$work/docs/formal" "$work/.github/workflows"
cp "$src/scripts/check_tla.sh" "$work/scripts/"
cp "$src/.github/workflows/ci.yml" "$work/.github/workflows/"
# The real modules' dependencies, without their bodies.
echo 'EXTENDS Naturals, Sequences, FiniteSets, TLC' >"$work/docs/formal/FelixShard.tla"
echo 'EXTENDS FelixShard' >"$work/docs/formal/FelixShardReads.tla"
printf 'EXTENDS Naturals,\n  FelixShardReads\n' >"$work/docs/formal/FelixShardSessions.tla"
echo 'Fig == INSTANCE FelixShard' >"$work/docs/formal/FelixShardFigure8.tla"
echo 'EXTENDS Naturals' >"$work/docs/formal/FelixRaftSwitch.tla"
echo 'CONSTANTS N = 3' >"$work/docs/formal/FelixRaftSwitchLag.cfg"
echo '# A formal model' >"$work/docs/formal/README.md"
git_ init -q -b main
git_ add -A
git_ commit -qm base
git_ tag base

failed=0
# Runs one case: commit what the caller changed, list it, compare, reset.
check() {
  local name="$1" expected="$2" got
  shift 2
  git_ add -A
  git_ commit -qm "$name"
  got="$("$work/scripts/check_tla.sh" --affected base "$@" 2>/dev/null)"
  # `# total: ?` skips the total, which follows pr_minutes.
  if grep -qx '# total: ? minutes' <<<"$expected"; then
    expected="$(grep -vx '# total: ? minutes' <<<"$expected")"
    got="$(grep -v '^# total: ' <<<"$got")"
  fi
  if [ "$got" != "$expected" ]; then
    echo "FAIL $name"
    diff <(echo "$expected") <(echo "$got") | head -20 || true
    failed=1
  else
    echo "ok   $name"
  fi
  git_ reset -q --hard base
}

# The registered configurations matching a pattern, as a pull request lists them.
registered() {
  sed -n '/^expectations=(/,/^)/p' "$src/scripts/check_tla.sh" | grep -oE "^  \"($1) " | tr -d ' "' \
    | awk 'NR == FNR { w[$1] = $2; next } w[$0] == "" || w[$0] <= 10' \
      <(sed -n '/^weights=(/,/^)/p' "$src/scripts/check_tla.sh" | tr -d '"') -
}
footer() { printf '# total: %s minutes\n# shards: %s' "$1" "$2"; }

echo 'CONSTANTS N = 4' >"$work/docs/formal/FelixRaftSwitchLag.cfg"
check "cfg only" "FelixRaftSwitchLag
$(footer 3 1)"

# A rename must list the old path too, or the registered name it leaves
# behind is never checked.
git_ mv docs/formal/FelixRaftSwitchLag.cfg docs/formal/FelixRaftSwitchLagRenamed.cfg
check "renamed cfg" "FelixRaftSwitchLag
$(footer 3 1)"

echo 'CONSTANTS N = 2' >"$work/docs/formal/FelixShardMadeUp.cfg"
check "unregistered cfg" "$(footer 0 0)"

echo 'EXTENDS FelixShard \* edited' >"$work/docs/formal/FelixShardReads.tla"
reads="$(registered 'FelixShardReads[A-Za-z]*|FelixShardSessions[A-Za-z]*')"
check "module and its dependents" "$reads
$(footer ? 1)"

echo '\* edited' >>"$work/docs/formal/FelixShard.tla"
git_ add -A
git_ commit -qm shard
bad=""
got="$("$work/scripts/check_tla.sh" --affected base 2>/dev/null)"
for want in FelixShard FelixShardLease FelixShardReadsNoRound FelixShardSessionsSubscriber FelixShardFigure8; do
  if ! grep -qx "$want" <<<"$got"; then echo "FAIL base module: no $want"; bad=1; fi
done
if grep -q '^FelixRaft' <<<"$got"; then echo "FAIL base module: lists FelixRaft"; bad=1; fi
walks="$("$work/scripts/check_tla.sh" --affected base --simulate 2>/dev/null)"
if ! grep -qx FelixShardFencedCrashUnprobed <<<"$walks"; then echo "FAIL base module walks"; bad=1; fi
if [ -n "$bad" ]; then failed=1; else echo "ok   base module"; fi
git_ reset -q --hard base

sed -i.bak 's/^  "FelixShardLease pass"$/  "FelixShardLease violates AtMostOneServing"/' "$work/scripts/check_tla.sh"
rm "$work/scripts/check_tla.sh.bak"
check "registration line" "FelixShardLease
$(footer ? 1)"

sed -i.bak 's/^  "FelixRaftSwitchLag 3"$/  "FelixRaftSwitchLag 4"/' "$work/scripts/check_tla.sh"
rm "$work/scripts/check_tla.sh.bak"
check "pr_minutes line" "FelixRaftSwitchLag
$(footer 4 1)"

# A new module's `module_of` line is that module's configurations, not all.
sed -i.bak 's/^    FelixRaft\*) echo FelixRaft ;;$/    FelixRaftWritten*) echo FelixRaftWritten ;;\
&/' "$work/scripts/check_tla.sh"
rm "$work/scripts/check_tla.sh.bak"
check "module_of line added" "$(footer 0 0)"

# A changed mapping lists what the old pattern matched: those configurations
# now check a different module.
sed -i.bak 's/^    FelixRaftSleep\*) echo FelixRaftSleep ;;$/    FelixRaftSleepy*) echo FelixRaftSleep ;;/' \
  "$work/scripts/check_tla.sh"
rm "$work/scripts/check_tla.sh.bak"
check "module_of mapping changed" "$(registered 'FelixRaftSleep[A-Za-z]*')
$(footer ? 1)"

# The default branch is not a mapping of one pattern: it is all of them.
sed -i.bak 's/^    \*) echo FelixShard ;;$/    *) echo FelixRaft ;;/' "$work/scripts/check_tla.sh"
rm "$work/scripts/check_tla.sh.bak"
git_ add -A
git_ commit -qm default
if [ "$("$work/scripts/check_tla.sh" --affected base 2>/dev/null | tail -1)" != "# shards: 30" ]; then
  echo "FAIL module_of default branch"
  failed=1
else
  echo "ok   module_of default branch"
fi
git_ reset -q --hard base

sed -i.bak 's/^# Each configuration and what it must do\./# What each configuration must do./' "$work/scripts/check_tla.sh"
rm "$work/scripts/check_tla.sh.bak"
check "comment in the runner" "$(footer 0 0)"

sed -i.bak 's/^SPEC_DIR="docs\/formal"$/SPEC_DIR=docs\/formal/' "$work/scripts/check_tla.sh"
rm "$work/scripts/check_tla.sh.bak"
git_ add -A
git_ commit -qm runner
got="$("$work/scripts/check_tla.sh" --affected base 2>/dev/null)"
if [ "$(tail -1 <<<"$got")" != "# shards: 30" ] || ! grep -qx FelixRaftSwitchLag <<<"$got"; then
  echo "FAIL runner logic: $(tail -2 <<<"$got" | tr '\n' ' ')"
  failed=1
else
  echo "ok   runner logic"
fi
git_ reset -q --hard base

sed -i.bak 's/^    name: TLA+ walk (unprobed crash)$/    name: TLA+ walk (unprobed crashes)/' "$work/.github/workflows/ci.yml"
rm "$work/.github/workflows/ci.yml.bak"
git_ add -A
git_ commit -qm workflow
if [ "$("$work/scripts/check_tla.sh" --affected base 2>/dev/null | tail -1)" != "# shards: 30" ]; then
  echo "FAIL formal job in ci.yml"
  failed=1
else
  echo "ok   formal job in ci.yml"
fi
git_ reset -q --hard base

awk '/^  fuzz:$/ { f = 1 } f && /timeout-minutes:/ && !done { sub(/[0-9]+/, "99"); done = 1 } { print }' \
  "$work/.github/workflows/ci.yml" >"$work/ci.yml.new"
mv "$work/ci.yml.new" "$work/.github/workflows/ci.yml"
check "other job in ci.yml" "$(footer 0 0)"

echo 'More prose.' >>"$work/docs/formal/README.md"
check "README only" "$(footer 0 0)"

if [ "$failed" -ne 0 ]; then
  echo "check_tla.sh --affected tests failed"
  exit 1
fi
echo "check_tla.sh --affected tests passed"
