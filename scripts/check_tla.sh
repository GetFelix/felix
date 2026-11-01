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
# `scripts/check_tla.sh FelixShardFigure8 FelixShardLease` checks only those;
# a named configuration not registered below must pass. The CI workflow's
# manual dispatch takes such a list, for the runs too long for every PR.
# `TLC_WORKERS=4` caps TLC's worker threads; the default is one per core.
# `TLA_SHARD=1/3` checks the second of three shards, balanced by the time each
# configuration takes (`weights` below), so CI can split the set across
# parallel jobs. `TLA_QUICK=1` checks only the shorter ones (see below).
# `TLA_COVERAGE=1` prints how often each action fired, to show a configuration
# reaches the behaviour it was written for. Operator names in place of `1`
# (`TLA_COVERAGE="TrimSet Promote"`) also print the count for each expression
# in those FelixShard.tla definitions, which shows a branch was taken.
#
# `scripts/check_tla.sh --simulate` runs the long random walks instead (`walks`
# below): TLC's simulation mode, each configuration for `TLA_SIM_MINUTES`
# (default 25) of traces up to `TLA_DEPTH` steps (default 300), from
# `TLA_SEED` or a random seed it prints. A walk is held to its declared outcome
# the same way, and TLC's full output is kept under target/tla/walk/ so a
# trace survives the run. Sampling, not proof: a pass means nothing was found.
#
# Needs Java 11+ on PATH, or Docker or Podman (CONTAINER_ENGINE picks one;
# see scripts/container_engine.sh). The TLA+ tools are fetched once, pinned
# by release and checksum, into target/tla/.
set -euo pipefail

cd "$(dirname "$0")/.."

mode=check
if [ "${1:-}" = "--simulate" ]; then
  mode=simulate
  shift
fi

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
# spec, and a run that is killed leaves them there. TLC_SCRATCH_DIR puts it on
# a bigger disk: the largest configurations outgrow a hosted runner's root disk.
tlc() {
  local cfg="$1"
  shift
  local scratch
  scratch="$(mktemp -d "${TLC_SCRATCH_DIR:-${TMPDIR:-/tmp}}/tlc.XXXXXX")"
  # A configuration checks the module whose name it starts with.
  local module="FelixShard"
  case "$cfg" in
    FelixPlacementPacing*) module="FelixPlacementPacing" ;;
    FelixShardFigure8*) module="FelixShardFigure8" ;;
    FelixShardReads*) module="FelixShardReads" ;;
    FelixShardSessions*) module="FelixShardSessions" ;;
    FelixAtomicCommit*) module="FelixAtomicCommit" ;;
    FelixHeadTrim*) module="FelixHeadTrim" ;;
    FelixGroups*) module="FelixGroups" ;;
  esac
  local flags=(-deadlock -workers "${TLC_WORKERS:-auto}" -checkpoint 0 "$@"
    -config "$cfg.cfg" "$module.tla")
  # A simulation runs until stopped, so a walk is stopped by the clock: exit
  # 124 from `timeout` is a walk that spent its budget.
  local limit=()
  if [ "$mode" = simulate ]; then limit=(timeout "${TLA_SIM_MINUTES:-25}m"); fi
  local status=0
  if command -v java >/dev/null 2>&1 && java -version >/dev/null 2>&1; then
    (cd "$SPEC_DIR" && ${limit[@]+"${limit[@]}"} java -XX:+UseParallelGC -jar "../../$JAR" \
      -metadir "$scratch" "${flags[@]}") || status=$?
  elif engine="$(scripts/container_engine.sh)" && [ -n "$engine" ]; then
    "$engine" run --rm \
      -v "$PWD/$SPEC_DIR:/spec" -v "$PWD/$JAR:/tla2tools.jar" -v "$scratch:/scratch" \
      -w /spec docker.io/library/eclipse-temurin:21-jre ${limit[@]+"${limit[@]}"} java -XX:+UseParallelGC -jar /tla2tools.jar \
      -metadir /scratch "${flags[@]}" || status=$?
  else
    echo "check_tla.sh needs java, docker or podman" >&2
    exit 1
  fi
  # The larger configurations leave gigabytes in it, one run after another.
  rm -rf "$scratch"
  return "$status"
}

# Each configuration and what it must do. `pass`, or `violates <Invariant>`.
expectations=(
  "FelixShardLease pass"
  "FelixShardLogOrder pass"
  "FelixShardThinMargin violates AtMostOneServing"
  "FelixShardRealMarginsLease pass"
  "FelixShardRealMargins pass"
  "FelixShardAckWithoutLease pass"
  "FelixShardFencedAck pass"
  "FelixShardFollowerLabels violates AckedOnMajority"
  "FelixShardFigure8 pass"
  "FelixShardFigure8NoStartRecord violates AckedOnMajority"
  "FelixShardFigure8CutOver pass"
  "FelixShardFigure8CutOverNoStartRecord violates AckedOnMajority"
  "FelixShardFigure8FollowerAcks pass"
  "FelixShardFigure8FollowerAcksNoStartRecord violates AckedOnMajority"
  "FelixShardUnfencedAck violates AckedHeldByLeader"
  "FelixShardFencedAckAnyKept pass"
  "FelixShardFencedAckAnyReplaced violates AckedHeldByLeader"
  "FelixShardFencedAckSeat pass"
  "FelixShardFencedAckGrow pass"
  "FelixShardFencedAckSeatPair pass"
  "FelixShardFencedAckUndoPair pass"
  "FelixShardFencedAckUndoPairUnfenced violates AckedOnMajority"
  "FelixShardFencedAckTrim pass"
  "FelixShardFencedAckSeatEarly violates AckedHeldByLeader"
  "FelixShardReadsRound pass"
  "FelixShardReadsNoRound violates NoStaleRead"
  "FelixShardReadsLease violates NoStaleRead"
  "FelixShardSessionsSubscriber pass"
  "FelixShardSessionsPastMark violates NoLostDelivery"
  "FelixShardSessionsGroupRound pass"
  "FelixShardSessionsGroupNoRound violates NoStaleGroupCommit"
  "FelixShardSessionsGroupLease violates NoStaleGroupCommit"
  "FelixShardFencedPromotion pass"
  "FelixShardNoCommitCheck violates NoStaleCommit"
  "FelixShardNoReportOrder violates AckedSurvive"
  "FelixShardReportAtTail violates QuorumReportNamesASuccessor"
  "FelixShardReportUnpaired violates AckedSurvive"
  "FelixShardReportFromAnswers violates AckedSurvive"
  "FelixShardReportFloor pass"
  "FelixShardWipe pass"
  "FelixShardWipeFull pass"
  "FelixShardWipeNoIds violates AckedSurvive"
  "FelixShardWipeStripped pass"
  "FelixShardWipeStrippedOnArrival violates AckedSurvive"
  "FelixShardWipeStrippedNoStamp violates AckedSurvive"
  "FelixShardCrashKeepsId violates AckedSurvive"
  "FelixShardProcessCrashKeepsId pass"
  "FelixShardMachineCrashKeepsId violates AckedSurvive"
  "FelixShardCleanStopUnsynced violates AckedSurvive"
  "FelixShardLeaderProcessCrash pass"
  "FelixShardLeaderCrashKeepsGen violates AckedSurvive"
  "FelixShardLeaderCrashGivesUp pass"
  "FelixShardLeaderCrashReclaim pass"
  "FelixShardLeaderCrashReclaimBlind violates AckedSurvive"
  "FelixShardFencedWipe pass"
  "FelixShardFencedWipeCountsWiped violates AckedHeldByLeader"
  "FelixShardFencedCrash pass"
  "FelixShardFencedCrashCountsRotated violates AckedHeldByLeader"
  "FelixShardFencedCrashCountsUnnamed violates AckedHeldByLeader"
  "FelixShardFencedCrashNamesByRegistration violates AckedHeldByLeader"
  "FelixShardFencedCrashInheritedCounts violates NoInheritedCount"
  "FelixShardFencedLeaderCrashTwiceNamesByInheritance pass"
  "FelixShardFencedLeaderCrashTwiceInheritedCounts violates NoInheritedCount"
  "FelixShardFencedLeaderCrash pass"
  "FelixShardFencedLeaderCrashKeepsGen violates AckedHeldByLeader"
  "FelixShardFencedLeaderCrashCountsRotated violates AckedHeldByLeader"
  "FelixShardShortCopySeatAnySet violates AckedHeldByLeader"
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
  "FelixAtomicCommit pass"
  "FelixAtomicCommitSplitRecords violates NoPartialCommit"
  "FelixAtomicCommitPartialApply violates NoPartialCommit"
  "FelixHeadTrim pass"
  "FelixHeadTrimCrash pass"
  "FelixHeadTrimTwoPromotions pass"
  "FelixHeadTrimCrashTwoPromotions pass"
  "FelixHeadTrimLocal violates NoReadBelowAckedTrim"
  "FelixHeadTrimFloorFromBase violates NoReadBelowAckedTrim"
  "FelixHeadTrimApplyOnStore violates NothingLostAboveFloor"
  "FelixHeadTrimDeleteAboveCommit violates NothingLostAboveFloor"
  "FelixHeadTrimUnconditional violates StaleTrimRefused"
  "FelixHeadTrimHiddenTrims violates StaleTrimRefused"
  "FelixHeadTrimNoOpUnchecked violates StaleTrimRefused"
  "FelixHeadTrimRetention violates StaleTrimRefused"
  "FelixShardFencedAckTwoPromotionsStart pass"
  "FelixShardFencedAckStagedMoveDestination violates AckedHeldByLeader"
  "FelixShardFencedAckMoveShort pass"
  "FelixShardFencedAckStagedMoveShort pass"
  "FelixShardFencedAckMoveCancelShort pass"
  "FelixShardFencedAckMoveDestination violates AckedHeldByLeader"
  "FelixShardFencedCache pass"
  "FelixShardFencedCacheUnfenced violates AckedHeldByLeader"
  "FelixShardFencedCacheNoCounterCatchUp violates CountersHeldByLeader"
  "FelixShardElect pass"
  "FelixShardElectNoBallot violates OneLeaderPerGeneration"
  "FelixShardElectStaleSet violates AckedHeldByLeader"
  "FelixShardElectAdopt pass"
  "FelixShardElectAdoptPromote pass"
  "FelixShardElectSwapNoJoint violates AckedHeldByLeader"
  "FelixShardElectSwapKeep1 pass"
  "FelixShardElectSwapKeep2 pass"
  "FelixShardElectSwapDrop0 pass"
  "FelixShardElectSwapDrop1 pass"
  "FelixShardElectSwapDrop2 pass"
  "FelixShardElectSwapDrop0StoodOneDest pass"
  "FelixShardElectSwapDrop0StoodOneOther pass"
  "FelixShardElectSwapDrop0StoodSeveralOther pass"
  "FelixShardElectSwapDrop1StoodOneDest pass"
  "FelixShardElectSwapDrop1StoodOneOther pass"
  "FelixShardElectSwapDrop1StoodSeveralOther pass"
  "FelixShardElectPreVote pass"
  "FelixShardElectNoBallotAcks violates AckedHeldByLeader"
  "FelixShardElectStepDown pass"
  "FelixShardElectHandoffLeaders pass"
  "FelixShardElectHandoffUnfenced violates OneLeaderPerGeneration"
  "FelixShardSuspicion pass"
  "FelixShardSuspicionLease violates AtMostOneServing"
  "FelixShardSuspicionWipeRoutes pass"
  "FelixShardSuspicionWipeUnbacked violates AckedHeldByLeader"
  "FelixShardElectWipe violates OneLeaderPerGeneration"
  "FelixShardElectWipeWaits violates AckedHeldByLeader"
  "FelixShardElectWipeWaitsAll violates OneLeaderPerGeneration"
  "FelixShardElectWipeWaitsSeeded violates OneLeaderPerGeneration"
  "FelixShardElectWipeWaitsFloor pass"
  "FelixShardElectWipeWaitsFloorDeep violates OneLeaderPerGeneration"
  "FelixShardElectWipeOracle pass"
  "FelixShardElectWipeOracleBatches violates AckedHeldByLeader"
  "FelixShardElectWipeOracleLearnsSet pass"
  "FelixShardElectWipeNonVoter pass"
  "FelixShardElectWipeNonVoterCountsWiped violates OneLeaderPerGeneration"
  "FelixShardElectWipeNonVoterElects violates NoLeaderBesideNonVoter"
  "FelixShardElectWipeNonVoterReadmits violates NeverReadmitted"
  "FelixShardElectWipeNonVoterNoCatchUp violates AckedHeldByLeader"
  "FelixShardElectWipeNonVoterFailsOver violates NoFailoverBesideNonVoter"
  "FelixShardElectWipeNonVoterPromotes violates OneLeaderPerGeneration"
  "FelixShardElectWipeNonVoterCreates pass"
  "FelixShardElectWipeNonVoterCreateCountsEmpty violates AckedHeldByLeader"
  "FelixShardElectWipeNonVoterCreatesOpens violates NoCreationBesideNonVoter"
  "FelixGroups pass"
  "FelixGroupsNoCheck violates NoSkip"
  "FelixGroupsLengthOnly violates NoSkip"
  "FelixGroupsDropGroup violates NoSkip"
  "FelixGroupsSplitSources pass"
  "FelixGroupsSplitSourcesLengthOnly violates NoSkip"
  "FelixGroupsShipUnsynced violates NoSkip"
)

# The long random walks, for `--simulate`: one pass configuration per family
# of set changes, and its negative twin, which must reach its violation
# within the budget to show the walks get that far at all. The inherited-name
# configurations with promotions on suspicion are here too: they outgrow a
# runner's disk exhaustively, so sampling them is what is left, as are the
# crashed leader's refusal beside a seat (with and without the fence ids'
# comparison) and beside a staged copy, voting or not. So is the unprobed
# crash, which no longer finishes in a runner's six hours exhaustively (#1584);
# ci.yml walks it per push, with its twin, which also stays an exhaustive check.
walks=(
  "FelixShardWalkSpares pass"
  "FelixShardWalkSparesSeatEarly violates AckedHeldByLeader"
  "FelixShardWalkMoves pass"
  "FelixShardWalkMovesDestination violates AckedHeldByLeader"
  "FelixShardWalkHandoff pass"
  "FelixShardWalkHandoffNoWait violates AtMostOneServing"
  "FelixShardWalkElect pass"
  "FelixShardWalkElectStaleSet violates AckedHeldByLeader"
  "FelixShardWalkElectSwap pass"
  "FelixShardWalkElectSwapNoJoint violates AckedHeldByLeader"
  "FelixShardFencedCrashNamesByInheritance pass"
  "FelixShardFencedCrashInheritedCounts violates NoInheritedCount"
  "FelixShardFencedWipeNamesByInheritance pass"
  "FelixShardFencedWipeInheritedCounts violates NoInheritedCount"
  "FelixShardFencedLeaderCrashNamesByInheritance pass"
  "FelixShardFencedLeaderCrashInheritedCounts violates NoInheritedCount"
  "FelixShardShortCopySeat pass"
  "FelixShardShortCopySeatAnyKeys pass"
  "FelixShardShortCopySeatAnySet violates AckedHeldByLeader"
  "FelixShardShortCopyStaged pass"
  "FelixShardShortCopyStagedVotes pass"
  "FelixShardShortCopyStagedVotesSetOnly violates AckedHeldByLeader"
  "FelixShardFencedCrashUnprobed pass"
  "FelixShardFencedCrashCountsUnnamed violates AckedHeldByLeader"
)
registered=("${expectations[@]}")
if [ "$mode" = simulate ]; then registered=("${walks[@]}"); fi

shard_index=0
shard_count=1
if [ -n "${TLA_SHARD:-}" ]; then
  shard_index="${TLA_SHARD%/*}"
  shard_count="${TLA_SHARD#*/}"
fi

# Configurations named on the command line, when any are, and no others. A
# named one that is not registered above is run by hand and must pass.
selected=("${registered[@]}")
if [ "$#" -gt 0 ]; then
  selected=()
  for name in "$@"; do
    entry="$name pass"
    for known in "${registered[@]}"; do
      if [ "${known%% *}" = "$name" ]; then entry="$known"; fi
    done
    selected+=("$entry")
  done
fi

# FelixShardElectSwap is not registered: it does not finish in one run, so
# it is checked in pieces split at the cut-over. The eleven that take under 45
# minutes are registered above; FelixShardElectSwapKeep0, the four
# FelixShardElectSwap{Keep0,Keep1,Keep2,Drop2}Stood and
# FelixShardElectSwapDrop{0,1}StoodSeveralDest run in the nightly exhaustive
# job (tla-walk.yml).
# So are FelixShardElectWipeNonVoter{Twice,Failover,FailoverWipesLeader,
# WipesLeader}: each runs well over an hour on a four-core runner. Their
# violating twins and witnesses stay registered.

# Minutes each of the longer configurations takes on a four-core CI runner,
# from the `Finished in` lines of a CI run (a new one is a guess until it has
# run); anything not listed is taken as one. Shards are filled longest first, each
# configuration going to the least loaded, so one long run does not land on
# top of others and push a job past its hour.
weights=(
  "FelixShardFencedAckMoveShort 31"
  "FelixShardWipeStripped 10"
  "FelixShardFencedAckMoveCancelShort 45"
  "FelixShardFencedAckStagedMoveShort 26"
  "FelixShardFencedAckTwoPromotionsStart 24"
  "FelixShardFencedCache 22"
  "FelixShardReadsRound 21"
  "FelixShardSessionsGroupRound 16"
  "FelixGroupsSplitSources 17"
  "FelixShardFencedAck 12"
  "FelixGroups 12"
  "FelixHeadTrim 17"
  "FelixHeadTrimCrash 13"
  "FelixHeadTrimTwoPromotions 2"
  "FelixHeadTrimCrashTwoPromotions 3"
  "FelixShardFigure8CutOver 9"
  "FelixShard 7"
  "FelixShardWipeFull 5"
  "FelixShardLeaderCrashGivesUp 3"
  "FelixShardProcessCrashKeepsId 20"
  "FelixShardLeaderProcessCrash 25"
  "FelixShardLeaderCrashReclaim 3"
  "FelixShardFencedWipe 25"
  "FelixShardFencedCrash 26"
  "FelixShardFencedCrashInheritedCounts 3"
  "FelixShardFencedLeaderCrashTwiceNamesByInheritance 21"
  "FelixShardFencedPromotion 7"
  "FelixShardCancelResend 6"
  "FelixShardFigure8FollowerAcks 6"
  "FelixShardCancel 6"
  "FelixShardReportFloor 5"
  "FelixShardAckWithoutLease 4"
  "FelixShardFigure8 4"
  "FelixShardRealMargins 4"
  "FelixShardRealMarginsLease 4"
  "FelixShardFencedAckSeat 15"
  "FelixShardFencedAckSeatPair 12"
  "FelixShardFencedAckUndoPair 26"
  "FelixShardFencedAckTrim 9"
  "FelixShardFollowerLabels 3"
  "FelixShardHandoff 3"
  "FelixShardIdempotentHandoff 3"
  "FelixShardStalePlannerCas 3"
  "FelixShardLogOrder 3"
  "FelixShardStagedMove 3"
  "FelixShardElectHandoffLeaders 6"
  "FelixShardElectAdopt 11"
  "FelixShardElectPreVote 5"
  "FelixShardElectAdoptPromote 3"
  "FelixShardSessionsSubscriber 2"
  "FelixShardSuspicion 2"
  "FelixShardSuspicionWipeRoutes 3"
  "FelixShardSuspicionWipeUnbacked 2"
  "FelixShardElectWipeOracle 5"
  "FelixShardElectWipeNonVoter 20"
  "FelixShardElectWipeNonVoterCreates 25"
  "FelixShardElectSwapKeep1 27"
  "FelixShardElectSwapKeep2 6"
  "FelixShardElectSwapDrop0 43"
  "FelixShardElectSwapDrop1 17"
  "FelixShardElectSwapDrop2 5"
  "FelixShardElectSwapDrop0StoodOneDest 39"
  "FelixShardElectSwapDrop0StoodOneOther 13"
  "FelixShardElectSwapDrop0StoodSeveralOther 1"
  "FelixShardElectSwapDrop1StoodOneDest 12"
  "FelixShardElectSwapDrop1StoodOneOther 3"
)

weight_of() {
  local w
  for w in "${weights[@]}"; do
    if [ "${w%% *}" = "$1" ]; then echo "${w#* }"; return; fi
  done
  echo 1
}

# `TLA_QUICK=1` leaves out the registered configurations weighing more than
# ten minutes, except those whose own .cfg changed against `TLA_CHANGED_BASE`
# (a revision; CI passes the PR's base), which run if they weigh 30 minutes or
# less. Heavier ones would not fit a pull request's hour beside the rest, and
# run on main or by dispatch (ci.yml's `tla_configs`). A changed .tla does
# not pull in its heavy configurations: FelixShard.tla is behind most of them.
# Pull requests check that tier; the rest run when the spec changes on main.
# Configurations named on the command line always run.
if [ -n "${TLA_QUICK:-}" ] && [ "$#" -eq 0 ]; then
  changed_cfgs=" "
  if [ -n "${TLA_CHANGED_BASE:-}" ]; then
    while read -r f; do
      f="${f##*/}"
      changed_cfgs="$changed_cfgs${f%.cfg} "
    done < <(git diff --name-only "$TLA_CHANGED_BASE" HEAD -- "$SPEC_DIR/*.cfg")
  fi
  quick=()
  for entry in "${selected[@]}"; do
    weight="$(weight_of "${entry%% *}")"
    if [ "$weight" -le 10 ]; then
      quick+=("$entry")
    elif [[ "$changed_cfgs" == *" ${entry%% *} "* ]]; then
      if [ "$weight" -le 30 ]; then
        quick+=("$entry")
      else
        echo "skipping changed ${entry%% *} (about $weight min): it runs on main or by dispatch"
      fi
    fi
  done
  selected=(${quick[@]+"${quick[@]}"})
fi

# Print the shard for each selected configuration, in order.
assign_shards() {
  local entry position=0
  for entry in "${selected[@]}"; do
    echo "$(weight_of "${entry%% *}") $position"
    position=$((position + 1))
  done | sort -k1,1nr -k2,2n | awk -v n="$shard_count" '
    { best = 0
      for (i = 1; i < n; i++) if (load[i] < load[best]) best = i
      load[best] += $1; shard[$2] = best }
    END { for (p = 0; p < NR; p++) print shard[p] }'
}
shards=()
while read -r s; do shards+=("$s"); done < <(assign_shards)

WALK_DIR="target/tla/walk"
LOG_DIR="target/tla/log"
if [ "$mode" = simulate ]; then LOG_DIR="$WALK_DIR"; fi
if [ "$mode" = simulate ] && command -v java >/dev/null 2>&1 && ! command -v timeout >/dev/null 2>&1; then
  echo "check_tla.sh --simulate needs timeout (GNU coreutils)" >&2
  exit 1
fi
fetch_tools
failed=0
position=-1
for entry in "${selected[@]}"; do
  cfg="${entry%% *}"
  expect="${entry#* }"
  position=$((position + 1))
  if [ "${shards[$position]}" -ne "$shard_index" ]; then continue; fi
  extra=()
  if [ "$mode" = simulate ]; then
    seed="${TLA_SEED:-$(od -An -N4 -tu4 /dev/urandom | tr -d ' ')}"
    extra=(-simulate -depth "${TLA_DEPTH:-300}" -seed "$seed")
    echo "== $cfg (expected: $expect; seed $seed, depth ${TLA_DEPTH:-300}, ${TLA_SIM_MINUTES:-25} min)"
  else
    echo "== $cfg (expected: $expect)"
  fi
  if [ -n "${TLA_COVERAGE:-}" ]; then extra+=(-coverage 60); fi
  # TLC's whole output goes to a file, and its progress lines (state counts
  # about once a minute) to the log as they come, so a run stopped at the job
  # limit still shows how far it got.
  log="$LOG_DIR/$cfg.out"
  mkdir -p "$LOG_DIR"
  if tlc "$cfg" ${extra[@]+"${extra[@]}"} 2>&1 | tee "$log" \
    | awk '/^Progress|^Finished computing initial states|^Running Random Simulation/ { print "   " $0; fflush() }'; then
    status=0
  else
    status="${PIPESTATUS[0]}"
    # TLC passed but the log did not get its output.
    if [ "$status" -eq 0 ]; then status=1; fi
  fi
  output="$(cat "$log")"
  summary="$(echo "$output" | grep -E "Running Random Simulation|states generated|depth of the complete|Error:|is violated|Finished in" | tail -5 || true)"
  echo "$summary"
  # How often each action fired, from the report TLC prints at the end.
  if [ -n "${TLA_COVERAGE:-}" ]; then
    echo "$output" | grep -E '^<[A-Za-z0-9_]+ line' | tail -n 80 || true
    for op in $TLA_COVERAGE; do
      [ "$op" = 1 ] && continue
      # The definition runs from its first line to the next blank one.
      range="$(awk -v op="$op" '$0 ~ "^" op "(\\(| ==)" { s = NR } s && !e && NR > s && /^$/ { e = NR } END { print s, e }' "$SPEC_DIR/FelixShard.tla")"
      echo "-- $op (FelixShard.tla lines $range)"
      echo "$output" | awk -v r="$range" 'BEGIN { split(r, a, " ") }
        match($0, /line [0-9]+, col [0-9]+ to line [0-9]+, col [0-9]+ of module FelixShard:/) {
          split(substr($0, RSTART + 5), n, ","); if (n[1] + 0 >= a[1] && n[1] + 0 < a[2]) print }' | head -n 120 || true
    done
  fi
  # A walk that ran out its budget without a violation is a clean run.
  clean=0
  if [ "$status" -eq 0 ]; then clean=1; fi
  if [ "$mode" = simulate ]; then
    if [ "$status" -eq 124 ] && grep -q "Running Random Simulation" <<<"$output" \
      && ! grep -q "^Error:" <<<"$output"; then
      clean=1
    fi
  fi
  case "$expect" in
    pass)
      if [ "$clean" -ne 1 ] || grep -q "is violated" <<<"$output"; then
        echo "   FAIL: expected no violation"
        echo "$output" | tail -80
        failed=1
      fi
      ;;
    "violates "*)
      invariant="${expect#violates }"
      if ! grep -q "Invariant $invariant is violated" <<<"$output"; then
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
