#!/usr/bin/env bash
# Model-check the specs in docs/formal under each configuration beside them,
# and hold each to the outcome the configuration declares.
#
# A configuration that is expected to pass must pass. One that is expected to
# find a violation must find exactly that invariant violated: those exist to
# show a check is load-bearing (drop it and TLC finds the trace) or to pin a
# finding the design has not yet acted on, and a "violation" that quietly
# turned into a pass would be a model that stopped saying anything.
#
# Needs Java 11+ on PATH, or Docker. The TLA+ tools are fetched once, pinned
# by release and checksum, into target/tla/.
set -euo pipefail

cd "$(dirname "$0")/.."

TLA_VERSION="v1.7.4"
TLA_SHA256="936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"
TLA_URL="https://github.com/tlaplus/tlaplus/releases/download/${TLA_VERSION}/tla2tools.jar"
JAR="target/tla/tla2tools-${TLA_VERSION}.jar"
SPEC_DIR="docs/formal"

fetch_tools() {
  if [ -f "$JAR" ]; then return; fi
  mkdir -p "$(dirname "$JAR")"
  echo "fetching TLA+ tools ${TLA_VERSION}"
  curl -sSL -o "$JAR.tmp" "$TLA_URL"
  actual="$(shasum -a 256 "$JAR.tmp" | awk '{print $1}')"
  if [ "$actual" != "$TLA_SHA256" ]; then
    echo "tla2tools.jar checksum mismatch: $actual" >&2
    rm -f "$JAR.tmp"
    exit 1
  fi
  mv "$JAR.tmp" "$JAR"
}

# Run TLC on one configuration, printing its output; the exit code is TLC's.
#
# Checkpoints off and the scratch directory outside the tree: TLC otherwise
# writes gigabytes of state fingerprints into a `states/` directory beside the
# spec, and a run that is killed leaves them there.
tlc() {
  local cfg="$1"
  local scratch
  scratch="$(mktemp -d)"
  # A configuration checks the module whose name it starts with.
  local module="FelixShard"
  case "$cfg" in
    FelixPlacementPacing*) module="FelixPlacementPacing" ;;
  esac
  local flags=(-deadlock -workers auto -checkpoint 0 -config "$cfg.cfg" "$module.tla")
  if command -v java >/dev/null 2>&1 && java -version >/dev/null 2>&1; then
    (cd "$SPEC_DIR" && java -XX:+UseParallelGC -jar "../../$JAR" \
      -metadir "$scratch" "${flags[@]}")
  elif command -v docker >/dev/null 2>&1; then
    docker run --rm \
      -v "$PWD/$SPEC_DIR:/spec" -v "$PWD/$JAR:/tla2tools.jar" -v "$scratch:/scratch" \
      -w /spec eclipse-temurin:21-jre java -XX:+UseParallelGC -jar /tla2tools.jar \
      -metadir /scratch "${flags[@]}"
  else
    echo "check_tla.sh needs java or docker" >&2
    exit 1
  fi
}

# Each configuration and what it must do. `pass`, or `violates <Invariant>`.
expectations=(
  "FelixShardLease pass"
  "FelixShardLogOrder pass"
  "FelixShardThinMargin violates AtMostOneServing"
  "FelixShardRealMarginsLease pass"
  "FelixShardRealMargins pass"
  "FelixShardAckWithoutLease pass"
  "FelixShardNoCommitCheck violates NoStaleCommit"
  "FelixShardNoReportOrder violates AckedSurvive"
  "FelixShardReportAtTail violates QuorumReportNamesASuccessor"
  "FelixShardReportUnpaired violates AckedSurvive"
  "FelixShard pass"
  "FelixShardHandoff pass"
  "FelixShardHandoffNoWait violates AtMostOneServing"
  "FelixShardStalePlannerCas pass"
  "FelixShardStalePlanner violates AtMostOneServing"
  "FelixShardStalePromotionCas pass"
  "FelixShardStalePromotion violates AtMostOneServing"
  "FelixShardHandoffLeaderAck pass"
  "FelixShardHandoffNoClaimFence violates AckedSurvive"
  "FelixShardHandoffAdmitAck pass"
  "FelixShardHandoffAdmitAckClaimFence violates AckedSurvive"
  "FelixShardStagedMove pass"
  "FelixShardStagedMoveSingle pass"
  "FelixShardStagedMoveVotes violates StagedCopyNeverDelaysAck"
  "FelixShardCancel pass"
  "FelixShardCancelStalePlannerCas pass"
  "FelixShardCancelStalePlanner violates AtMostOneServing"
  "FelixShardCancelResend pass"
  "FelixShardCancelResendMemory violates NoDuplicate"
  "FelixPlacementPacing pass"
  "FelixPlacementPacingUncountedReplacement violates CopiesWithinLimit"
  "FelixPlacementPacingTwoPlanners pass"
  "FelixPlacementPacingUnfenced violates CopiesWithinLimit"
  "FelixShardIdempotentFailover pass"
  "FelixShardIdempotentFailoverMemory violates NoDuplicate"
  "FelixShardIdempotentHandoff pass"
  "FelixShardIdempotentHandoffMemory violates NoDuplicate"
)

fetch_tools
failed=0
for entry in "${expectations[@]}"; do
  cfg="${entry%% *}"
  expect="${entry#* }"
  echo "== $cfg (expected: $expect)"
  output="$(tlc "$cfg" 2>&1)" && status=0 || status=$?
  summary="$(echo "$output" | grep -E "states generated|Error:|is violated|Finished in" | tail -4)"
  echo "$summary"
  case "$expect" in
    pass)
      if [ "$status" -ne 0 ] || echo "$output" | grep -q "is violated"; then
        echo "   FAIL: expected no violation"
        echo "$output" | tail -80
        failed=1
      fi
      ;;
    "violates "*)
      invariant="${expect#violates }"
      if ! echo "$output" | grep -q "Invariant $invariant is violated"; then
        echo "   FAIL: expected TLC to violate $invariant"
        echo "$output" | tail -40
        failed=1
      fi
      ;;
  esac
done

if [ "$failed" -ne 0 ]; then
  echo "TLA+ checks failed"
  exit 1
fi
echo "TLA+ checks passed"
