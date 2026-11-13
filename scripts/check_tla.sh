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
# `scripts/check_tla.sh --plan-heavy` prints which changed configurations a
# pull request checks in jobs of their own (see `--plan-heavy` below).
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
elif [ "${1:-}" = "--plan-heavy" ]; then
  mode=plan-heavy
  shift
elif [ "${1:-}" = "--affected" ]; then
  mode=affected
  affected_base="${2:?--affected needs a base revision}"
  shift 2
  affected_walks=""
  if [ "${1:-}" = "--simulate" ]; then
    affected_walks=1
    shift
  fi
  TLA_CHANGED_BASE="${TLA_CHANGED_BASE:-$affected_base}"
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
  # The release CDN has served a wrong body with a 200 now and then, so a
  # mismatch is retried. Only a file that matches the pinned hash is kept.
  local attempt actual delay
  for attempt in 1 2 3 4; do
    if curl -sSL --fail --retry 3 -o "$JAR.tmp" "$TLA_URL"; then
      actual="$(shasum -a 256 "$JAR.tmp" | awk '{print $1}')"
      if [ "$actual" = "$TLA_SHA256" ]; then
        mv "$JAR.tmp" "$JAR"
        return
      fi
      echo "tla2tools.jar checksum mismatch (attempt $attempt/4): $actual," \
        "$(wc -c <"$JAR.tmp" | tr -d ' ') bytes, starts: $(head -c 200 "$JAR.tmp" | LC_ALL=C tr -cd '[:print:]')" >&2
    else
      echo "tla2tools.jar download failed (attempt $attempt/4)" >&2
    fi
    rm -f "$JAR.tmp"
    if [ "$attempt" -lt 4 ]; then
      delay=$((attempt == 1 ? 5 : attempt == 2 ? 15 : 30))
      sleep "$delay"
    fi
  done
  echo "tla2tools.jar could not be fetched with the pinned checksum" >&2
  exit 1
}

# The module a configuration checks: the one whose name it starts with.
module_of() {
  case "$1" in
    FelixPlacementPacing*) echo FelixPlacementPacing ;;
    FelixShardWalkElectStaleSetReplay) echo FelixShardWalkElectStaleSetReplay ;;
    FelixShardWalkSparesSeatEarlyReplay) echo FelixShardWalkSparesSeatEarlyReplay ;;
    FelixShardFigure8*) echo FelixShardFigure8 ;;
    FelixShardReads*) echo FelixShardReads ;;
    FelixShardSessions*) echo FelixShardSessions ;;
    FelixAtomicCommit*) echo FelixAtomicCommit ;;
    FelixHeadTrim*) echo FelixHeadTrim ;;
    FelixGroups*) echo FelixGroups ;;
    FelixRaftMembership*) echo FelixRaftMembership ;;
    FelixRaftIdentity*) echo FelixRaftIdentity ;;
    FelixRaftPlacement*) echo FelixRaftPlacement ;;
    FelixRaftReform*) echo FelixRaftReform ;;
    FelixRaftLease*) echo FelixRaftLease ;;
    FelixRaftSleep*) echo FelixRaftSleep ;;
    FelixRaftSwitch*) echo FelixRaftSwitch ;;
    FelixRaft*) echo FelixRaft ;;
    FelixShardUnified*) echo FelixShardUnified ;;
    FelixShardMoveGiveUp*) echo FelixShardMoveGiveUp ;;
    *) echo FelixShard ;;
  esac
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
  local scratch module
  scratch="$(mktemp -d "${TLC_SCRATCH_DIR:-${TMPDIR:-/tmp}}/tlc.XXXXXX")"
  module="$(module_of "$cfg")"
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
  "FelixShardWalkElectStaleSetReplay violates AckedHeldByLeader"
  "FelixShardWalkSparesSeatEarlyReplay violates AckedHeldByLeader"
  "FelixShardLease pass"
  "FelixShardLeaseAnyHeartbeat violates AtMostOneServing"
  "FelixShardLeaseRoutesHeartbeat pass"
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
  "FelixShardFigure8AckWait pass"
  "FelixShardFigure8AckWaitByOffset violates AckedHeldByLeader"
  "FelixShardUnfencedAck violates AckedHeldByLeader"
  "FelixShardFencedAckAnyKept pass"
  "FelixShardFencedAckAnyReplaced violates AckedHeldByLeader"
  "FelixShardReplacedPendingLeft violates AckedHeldByLeader"
  "FelixShardReplacedSetUnknownAny violates AckedHeldByLeader"
  "FelixShardReplacedSetUnknownRenames violates NeverRenameUnknownSet"
  "FelixShardReplacedSetUnknownFirstRenames violates NeverRenameFirstUnknownSet"
  "FelixShardReplacedSetUnknownMarkAny violates AckedHeldByLeader"
  "FelixShardReplacedSetUnknownMarkRenames violates NeverRenameUnknownSet"
  "FelixShardReplacedPendingLeftKept violates AckedHeldByLeader"
  "FelixShardReplacedPendingLeftKeptLogBatchDrops violates AckedHeldByLeader"
  "FelixShardReplacedPendingLeftTwice violates AckedHeldByLeader"
  "FelixShardReplacedPendingLeftTwiceAll violates AckedHeldByLeader"
  "FelixShardReplacedPendingLeftTwiceAllLog violates AckedHeldByLeader"
  "FelixShardReplacedPendingLeftTwiceAllLogBatchDrops violates AckedHeldByLeader"
  "FelixShardReplacedPendingLeftTwiceAllLogReportNoFloor violates AckedHeldByLeader"
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
  "FelixShardFencedAckMovePromoted pass"
  "FelixShardFencedAckMovePromotedReach violates NeverPromotesDestination"
  "FelixShardFencedCache pass"
  "FelixShardFencedCacheUnfenced violates AckedHeldByLeader"
  "FelixShardFencedCacheNoCounterCatchUp violates CountersHeldByLeader"
  "FelixShardElect pass"
  "FelixShardElectNoBallot violates OneLeaderPerGeneration"
  "FelixShardElectStaleSet violates AckedHeldByLeader"
  "FelixShardElectAdopt pass"
  "FelixShardElectRenameDue violates NeverRenameDue"
  "FelixShardElectRenameFencingDue violates NeverRenameWhileFencing"
  "FelixShardElectAdoptPromote pass"
  "FelixShardElectReissue pass"
  "FelixShardElectReissueReplace pass"
  "FelixShardElectReissueReplaceBatchSet violates AdoptedMatchesOpened"
  "FelixShardElectAdoptAboveKeptSet violates AdoptedMatchesOpened"
  "FelixShardElectAdoptFailoverApart0Gen2 pass"
  "FelixShardElectAdoptFailoverApart1Gen2 pass"
  "FelixShardElectAdoptFailoverApart0StoodGen2 pass"
  "FelixShardElectAdoptFailoverApart1StoodGen2 pass"
  "FelixShardElectAdoptFailoverSame0StoodGen3 pass"
  "FelixShardElectAdoptFailoverApart0StoodGen3 pass"
  "FelixShardElectAdoptFailoverSame1StoodGen3 pass"
  "FelixShardElectAdoptFailoverApart1StoodGen3 pass"
  "FelixShardElectAdoptFailoverSame0StoodGen4 pass"
  "FelixShardElectAdoptFailoverApart0StoodGen4 pass"
  "FelixShardElectAdoptFailoverSame1StoodGen4 pass"
  "FelixShardElectAdoptFailoverApart1StoodGen4 pass"
  "FelixShardSwapCounters pass"
  "FelixShardSwapCountersFlipUnchecked violates CountersOnSetMajority"
  "FelixShardSwapCountersNewSetOnly violates CountersOnSetMajority"
  "FelixShardElectSwapNoJoint violates AckedHeldByLeader"
  "FelixShardElectSwapKeep1 pass"
  "FelixShardElectSwapKeep2 pass"
  "FelixShardElectSwapDrop0 pass"
  "FelixShardElectSwapDrop1 pass"
  "FelixShardElectSwapDrop2 pass"
  "FelixShardElectSwapDrop2DropsChange pass"
  "FelixShardElectSwapKeep2DropsChange pass"
  "FelixShardElectSwapDrop0StoodOneDest pass"
  "FelixShardElectSwapDrop0StoodOneOther pass"
  "FelixShardElectSwapDrop0StoodSeveralOther pass"
  "FelixShardElectSwapDrop1StoodOneDest pass"
  "FelixShardElectSwapDrop1StoodOneOther pass"
  "FelixShardElectSwapDrop1StoodSeveralOther pass"
  "FelixShardElectRenameKeep0StoodSeveralOther pass"
  "FelixShardElectRenameKeep1StoodSeveralOther pass"
  "FelixShardElectRenameDrop0StoodSeveralOther pass"
  "FelixShardElectRenameDrop1StoodSeveralOther pass"
  "FelixShardElectRenameKeep0 pass"
  "FelixShardElectRenameKeep1 pass"
  "FelixShardElectRenameDrop0 pass"
  "FelixShardElectRenameDrop1 pass"
  "FelixShardElectRenameKeep0StoodOneOther pass"
  "FelixShardElectRenameKeep1StoodOneOther pass"
  "FelixShardElectRenameDrop1StoodOneOther pass"
  "FelixShardElectRenameDrop1StoodSeveralDest pass"
  "FelixShardElectSwapFlipKeep1 pass"
  "FelixShardElectSwapFlipKeep1StoodOneOther pass"
  "FelixShardElectSwapFlipKeep0StoodSeveralOther pass"
  "FelixShardElectSwapFlipKeep1StoodSeveralOther pass"
  "FelixShardElectSwapFlipDrop1 pass"
  "FelixShardElectSwapFlipDrop0StoodOneOther pass"
  "FelixShardElectSwapFlipDrop0StoodSeveralOther pass"
  "FelixShardElectSwapFlipDrop1StoodOneOther pass"
  "FelixShardElectSwapFlipDrop1StoodSeveralOther pass"
  "FelixShardElectSwapFlipSkipsBallot violates OneLeaderPerGeneration"
  "FelixShardElectSwapFlipJoinLagsFirstStart violates AckedOnMajority"
  "FelixShardElectLive pass"
  "FelixShardElectLiveLoosePreVote violates NoStandAgainstHeardLeader"
  "FelixShardElectLiveNoBackoff violates liveness"
  "FelixShardElectLiveReissue pass"
  "FelixShardElectLiveNoReissue violates liveness"
  "FelixShardElectLiveUnclean pass"
  "FelixShardElectLiveUncleanOwnBallot violates liveness"
  "FelixShardElectLiveUncleanLed pass"
  "FelixShardElectUnclean pass"
  "FelixShardElectUncleanNoStartRecord pass"
  "FelixShardElectUncleanVouchOnShip violates AckedHeldByLeader"
  "FelixShardElectUncleanForgetsLed violates AckedHeldByLeader"
  "FelixShardCpLiveMain violates liveness"
  "FelixShardCpLiveNoOverlayRefused violates liveness"
  "FelixShardCpLiveNoHaltReported violates liveness"
  "FelixShardCpLiveNoAboveReported violates liveness"
  "FelixShardCpLiveReleased pass"
  "FelixShardCpLiveReleasedNoAboveReported violates liveness"
  "FelixShardCpLiveRefusalsCopyExcluded violates liveness"
  "FelixShardCpLiveRefusalsPinsForever violates liveness"
  "FelixShardElectRefusals pass"
  "FelixShardElectRefusalsPinsCleared violates AckedHeldByLeader"
  "FelixShardElectRenameMinorityDrop violates OneLeaderPerGeneration"
  "FelixShardCpLiveSkippedGenerationUnknownPrior violates liveness"
  "FelixShardCpDownNoSelfSuspicion violates liveness"
  "FelixShardCpDownBrokerSuspicion violates liveness"
  "FelixShardCpDownDegraded violates liveness"
  "FelixShardElectRouteLag pass"
  "FelixShardElectRouteLagNoFix violates liveness"
  "FelixShardElectRouteLagReportOnly pass"
  "FelixShardElectRouteLagFence pass"
  "FelixShardElectRouteLagFenceNoFix violates liveness"
  "FelixShardElectRouteLagReportStore pass"
  "FelixShardElectRouteLagReportStoreNoRepeat violates liveness"
  "FelixShardElectRouteLagFenceReportStore pass"
  "FelixShardElectRenameKeep1StoodOneOtherReportStore pass"
  "FelixShardElectRenameDrop1StoodOneOtherReportStore pass"
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
  "FelixShardElectWipeNonVoterNoCatchUp violates OneLeaderPerGeneration"
  "FelixShardElectWipeNonVoterFailsOver violates NoFailoverBesideNonVoter"
  "FelixShardElectWipeNonVoterPromotes violates OneLeaderPerGeneration"
  "FelixShardElectWipeNonVoterDropsLearners violates PlacementKnowsLearners"
  "FelixShardElectWipeNonVoterCreates pass"
  "FelixShardElectWipeNonVoterCreatesLearnerAcks violates AckedHeldByLeader"
  "FelixShardElectWipeNonVoterCreateCountsEmpty violates OneLeaderPerGeneration"
  "FelixShardElectWipeNonVoterCreatesOpens violates NoCreationBesideNonVoter"
  "FelixShardElectWipeNonVoterCreatesReadmits violates NoFailoverAfterReadmit"
  "FelixShardElectWipeNonVoterEvidenceOnly violates OneLeaderPerGeneration"
  "FelixShardElectWipeNonVoterSetlessUnmarked violates AckedHeldByLeader"
  "FelixShardElectWipeNonVoterCountersCountLearner violates CountersHeldByLeader"
  "FelixShardElectWipeNonVoterCountersUnchecked violates CountersHeldByLeader"
  "FelixShardElectWipeNonVoterNoCatchUpFailover violates AckedHeldByLeader"
  "FelixShardElectWipeNonVoterEvidenceOnlyFailover violates AckedHeldByLeader"
  "FelixShardElectWipeNonVoterGroupsKeepDropsMark violates CountersHeldByLeader"
  "FelixShardElectWipeNonVoterIdlessCreation pass"
  "FelixShardElectWipeNonVoterIdlessCreationExempt violates AckedHeldByLeader"
  "FelixShardElectWipeNonVoterReclaim pass"
  "FelixShardElectWipeNonVoterReclaimLive pass"
  "FelixShardElectWipeNonVoterReclaimLivePassOnly violates liveness"
  "FelixShardElectUncleanContinuing pass"
  "FelixShardElectUncleanContinuingBatchBallot violates OneLeaderPerGeneration"
  "FelixShardElectUncleanContinuingAnyCopy violates OneLeaderPerGeneration"
  "FelixShardElectUncleanContinuingCountsForgotten violates OneLeaderPerGeneration"
  "FelixShardElectUncleanContinuingRetry violates RetriedContinuingNeverOpens"
  "FelixShardElectUncleanContinuingRetryBelowOnly pass"
  "FelixShardElectUncleanContinuingRetrySafe pass"
  "FelixShardElectUncleanContinuingDoubted pass"
  "FelixGroups pass"
  "FelixGroupsNoCheck violates NoSkip"
  "FelixGroupsLengthOnly violates NoSkip"
  "FelixGroupsDropGroup violates NoSkip"
  "FelixGroupsSplitSources pass"
  "FelixGroupsSplitSourcesLengthOnly violates NoSkip"
  "FelixGroupsShipUnsynced violates NoSkip"
  "FelixShardUnified pass"
  "FelixShardUnifiedSets pass"
  "FelixShardUnifiedReadmit pass"
  "FelixShardUnifiedRestore pass"
  "FelixShardUnifiedMixed pass"
  "FelixShardUnifiedDropEvidenced pass"
  "FelixShardUnifiedVouch pass"
  "FelixShardUnifiedCountNonKept violates OneLeaderPerGeneration"
  "FelixShardUnifiedCoverFromDoubted violates AckedHeldByLeader"
  "FelixShardUnifiedDropUnopenedLeaving violates AckedHeldByLeader"
  "FelixShardUnifiedSkipCoverage violates AckedHeldByLeader"
  "FelixShardUnifiedCountsWiped violates OneLeaderPerGeneration"
  "FelixShardUnifiedAnyCopy violates OneLeaderPerGeneration"
  "FelixShardUnifiedBatchBallot violates OneLeaderPerGeneration"
  "FelixShardUnifiedCountsForgotten violates OneLeaderPerGeneration"
  "FelixShardUnifiedNamesByRegistration violates OneLeaderPerGeneration"
  "FelixShardUnifiedTrustsRegistered violates AckedHeldByLeader"
  "FelixShardUnifiedStandInNoCatchUp violates AckedHeldByLeader"
  "FelixShardUnifiedVouchBelowStart violates AckedHeldByLeader"
  "FelixShardUnifiedVouchMark pass"
  "FelixShardUnifiedContinuingDoubted violates NoContinuingOnForeignBallot"
  "FelixShardUnifiedContinuingDoubtedLegacy pass"
  "FelixShardUnifiedCpLiveElectedFailover pass"
  "FelixShardUnifiedCpLiveElectedFailoverLegacy violates liveness"
  "FelixShardUnifiedCpLiveReadmitUnopened pass"
  "FelixShardUnifiedCpLiveReadmitUnopenedLegacy violates liveness"
  "FelixShardUnifiedCpLiveMoveDoubted pass"
  "FelixShardUnifiedIdleVouch pass"
  "FelixShardUnifiedIdleVouchLegacy violates liveness"
  "FelixShardUnifiedIdleVouchWrite pass"
  "FelixShardUnifiedVouchEmptyLegacy violates liveness"
  "FelixShardUnifiedLearnerBlocking pass"
  "FelixShardUnifiedLearnerBlockingTwo pass"
  "FelixShardUnifiedLearnerVariantDropped violates AckedHeldByLeader"
  "FelixShardMoveGiveUp pass"
  "FelixShardMoveGiveUpStall violates liveness"
  "FelixShardMoveGiveUpLeads violates AckedHeldByLeader"
  "FelixShardMoveGiveUpKeep pass"
  "FelixShardMoveGiveUpKeepLive pass"
  "FelixShardMoveGiveUpDrop violates AckedHeldByLeader"
  "FelixShardMoveGiveUpUnstage violates AckedHeldByLeader"
  "FelixShardMoveGiveUpTwoDrop violates AckedHeldByLeader"
  "FelixShardMoveGiveUpTwoKeep violates AckedHeldByLeader"
  "FelixShardMoveGiveUpFourDrop pass"
  "FelixShardMoveGiveUpFailoverDrop pass"
  "FelixShardMoveGiveUpFailoverStall violates liveness"
  "FelixShardMoveGiveUpFailoverLive pass"
  "FelixShardUnifiedTwoRotated pass"
  "FelixShardUnifiedTwoRotatedForgets violates OneLeaderPerGeneration"
  "FelixShardUnifiedCpLiveMoveRotated pass"
  "FelixShardUnifiedCpLiveMoveRotatedLegacy violates liveness"
  "FelixRaft pass"
  "FelixRaftCrashes pass"
  "FelixRaftTerms pass"
  "FelixRaftTermsFull pass"
  "FelixRaftFourTerms pass"
  "FelixRaftSnapshot pass"
  "FelixRaftSnapshotReplace pass"
  "FelixRaftSnapshotCrashes pass"
  "FelixRaftOffsets pass"
  "FelixRaftOffsetsFailover pass"
  "FelixRaftOffsetsFailoverCrash pass"
  "FelixRaftOffsetsFull pass"
  "FelixRaftAckBeforeFlush violates AckedSurvive"
  "FelixRaftVoteNotDurable violates OneLeaderPerTerm"
  "FelixRaftLeaderResumes violates LogMatching"
  "FelixRaftTornTailKept violates LogMatching"
  "FelixRaftTruncateNotDurable violates StateMachineSafety"
  "FelixRaftRecoveredTailUnsynced violates AckedSurvive"
  "FelixRaftSaveCommittedUnflushed violates AckedDelivered"
  "FelixRaftPurgeBeforeSnapshot violates AckedSurvive"
  "FelixRaftSnapshotTwoRenames violates StateMachineSafety"
  "FelixRaftCommitPriorTerm violates LeaderCompleteness"
  "FelixRaftDeliverUncommitted violates OffsetNeverReused"
  "FelixRaftDeliverUncommittedDense violates OffsetsDense"
  "FelixRaftSkipsNotCounted violates OffsetsDense"
  "FelixRaftReachFailover violates NeverFailover"
  "FelixRaftReachTruncate violates NeverTruncates"
  "FelixRaftReachLostTail violates NeverLosesTail"
  "FelixRaftReachSkips violates NeverSkipsBetweenRecords"
  "FelixRaftReachInstall violates NeverInstallsSnapshot"
  "FelixRaftReachInstallCrash violates NeverCrashesMidInstall"
  "FelixRaftReachSnapshotReplace violates NeverReplacesSnapshot"
  "FelixRaftReachDeliverFailover violates NeverDeliversAcrossFailover"
  "FelixRaftMembership pass"
  "FelixRaftMembershipFailover pass"
  "FelixRaftMembershipSkipJoint violates OneLeaderPerTerm"
  "FelixRaftMembershipChangeBeforeCommit violates StateMachineSafety"
  "FelixRaftMembershipLearnerCounts violates AckedSurvive"
  "FelixRaftMembershipClaimRaces violates NeverStops"
  "FelixRaftMembershipReopenOnReturn violates NeverStops"
  "FelixRaftMembershipReachMove violates NeverMoves"
  "FelixRaftMembershipReachStepDown violates NeverRemovedLeaderStepsDown"
  "FelixRaftMembershipReachFailoverInJoint violates NeverFailoverInJoint"
  "FelixRaftIdentity pass"
  "FelixRaftIdentityRelaxedCrash pass"
  "FelixRaftIdentityRootRestore pass"
  "FelixRaftIdentityDirRestore pass"
  "FelixRaftIdentityMarkExact pass"
  "FelixRaftIdentityWipeThenRestore pass"
  "FelixRaftIdentityRootRestoreSameRun pass"
  "FelixRaftIdentityRootRestoreNoWitness pass"
  "FelixRaftIdentityRootRestoreBatched pass"
  "FelixRaftIdentityWipeThenRootRestore pass"
  "FelixRaftIdentityMarkBatched violates AckedHeldByLeader"
  "FelixRaftIdentityRootRestoreNoSequence violates OneLeaderPerTerm"
  "FelixRaftIdentityRootRestoreUnheard violates OneLeaderPerTerm"
  "FelixRaftIdentityRootRestoreYoung violates AckedHeldByLeader"
  "FelixRaftIdentityWipeKeepsId violates LeaderCompleteness"
  "FelixRaftIdentityNoRestoreCheck violates LeaderCompleteness"
  "FelixRaftIdentityNoStorageIdCheck violates LeaderCompleteness"
  "FelixRaftIdentityMarkForgetsCopy violates LeaderCompleteness"
  "FelixRaftIdentityNoVoteMark violates OneLeaderPerTerm"
  "FelixRaftIdentityVoteBeforeMark violates OneLeaderPerTerm"
  "FelixRaftIdentityRejoins violates NeverRejoins"
  "FelixRaftIdentityRejoinLeads violates NeverRotatedLeads"
  "FelixRaftIdentityRestoreCaught violates NeverRotates"
  "FelixRaftIdentityRootRestoreCaught violates NeverRotates"
  "FelixRaftIdentityRootRestoreSameRunCaught violates NeverRotates"
  "FelixRaftIdentityRootRestoreNoWitnessCaught violates NeverRotates"
  "FelixRaftPlacement pass"
  "FelixRaftPlacementFailover pass"
  "FelixRaftPlacementReseat pass"
  "FelixRaftPlacementShrink pass"
  "FelixRaftPlacementMoveWipe pass"
  "FelixRaftPlacementStale pass"
  "FelixRaftPlacementStaleFull pass"
  "FelixRaftPlacementSameId pass"
  "FelixRaftPlacementReseatWrites pass"
  "FelixRaftPlacementSameIdWrites pass"
  "FelixRaftPlacementShrinkRetain pass"
  "FelixRaftPlacementReturnWipe pass"
  "FelixRaftPlacementStaleWipe violates AckedOnDisk"
  "FelixRaftPlacementSameIdCounted violates AckedSurvive"
  "FelixRaftPlacementSameIdFive violates OneLeaderPerTerm"
  "FelixRaftPlacementSkipJoint violates OneLeaderPerTerm"
  "FelixRaftPlacementNoWithhold violates LeaderCompleteness"
  "FelixRaftPlacementAnswerAnyId violates LeaderCompleteness"
  "FelixRaftPlacementDeleteOnTarget violates AckedOnDisk"
  "FelixRaftPlacementTwoWipes violates AckedOnDisk"
  "FelixRaftPlacementNoWithholdOffsets violates OffsetNeverReused"
  "FelixRaftPlacementPlaceDoomed violates AckedOnDisk"
  "FelixRaftPlacementReachMove violates NeverMoves"
  "FelixRaftPlacementReachShrink violates NeverShrinks"
  "FelixRaftPlacementReachDelete violates NeverDeletes"
  "FelixRaftPlacementReachReseat violates NeverReseats"
  "FelixRaftPlacementReachWipedLeads violates NeverWipedLeads"
  "FelixRaftPlacementReachVotesAgain violates NeverVotesAgain"
  "FelixRaftPlacementReachAck violates NeverAcks"
  "FelixRaftPlacementForm pass"
  "FelixRaftPlacementFormWaiver pass"
  "FelixRaftPlacementFormSilent pass"
  "FelixRaftPlacementFormWaiverMany pass"
  "FelixRaftPlacementFormWaiverAnswers pass"
  "FelixRaftPlacementFormTwoWipes pass"
  "FelixRaftPlacementFormAfterWipes pass"
  "FelixRaftPlacementFormMajority violates AckedHeldSurvive"
  "FelixRaftPlacementFormAlias violates AckedHeldSurvive"
  "FelixRaftPlacementFormEarlyWipe pass"
  "FelixRaftPlacementFormEarlyWipeFirstBoot pass"
  "FelixRaftPlacementFormEarlyWipeTwo pass"
  "FelixRaftPlacementFormEarlyWipeUnrecorded violates AckedHeldSurvive"
  "FelixRaftPlacementFormEarlyWipeTwoUnrecorded violates OneLeaderPerTerm"
  "FelixRaftPlacementFormWaiverExcuses violates AckedHeldSurvive"
  "FelixRaftPlacementFormWaiverAnyNumber violates NobodyForms"
  "FelixRaftPlacementReachForm violates NeverAcks"
  "FelixRaftPlacementReachFormWaiver violates NeverAcks"
  "FelixRaftPlacementReachEarlyWipeVotesAgain violates NeverEarlyWipedVotesAgain"
  "FelixRaftPlacementFormLogWipe pass"
  "FelixRaftPlacementFormLogWipeFirstBoot pass"
  "FelixRaftPlacementFormLogWipeUnreached violates AckedHeldSurvive"
  "FelixRaftPlacementReachLogWipeVotesAgain violates NeverLogWipedVotesAgain"
  "FelixRaftSwitch pass"
  "FelixRaftSwitchLag pass"
  "FelixRaftSwitchLagWipe pass"
  "FelixRaftSwitchWiped pass"
  "FelixRaftSwitchGenZero pass"
  "FelixRaftSwitchStarts pass"
  "FelixRaftSwitchDiffers pass"
  "FelixRaftSwitchReseal pass"
  "FelixRaftSwitchDigestCoversGeneration pass"
  "FelixRaftSwitchAllVoters violates LeaderCompleteness"
  "FelixRaftSwitchTermsBelowBase violates LeaderCompleteness"
  "FelixRaftSwitchTermsBelowBaseCuts violates NeverCutsHeldRecord"
  "FelixRaftSwitchNoDigest violates LeaderCompleteness"
  "FelixRaftSwitchDigestFromOwnStart violates LeaderCompleteness"
  "FelixRaftSwitchBlindWrite violates LeaderCompleteness"
  "FelixRaftSwitchBlindWriteTwoBases violates OneBase"
  "FelixRaftSwitchStaleIdVotes violates LeaderCompleteness"
  "FelixRaftSwitchReachCommit violates NeverCommitsAboveBase"
  "FelixRaftSwitchReachPromote violates NeverPromotes"
  "FelixRaftSwitchReachPromotedLeads violates NeverPromotedLeads"
  "FelixRaftSwitchReachInstall violates NeverInstalls"
  "FelixRaftSwitchReachGenZero violates NeverElects"
  "FelixRaftSwitchReachWipedSwitch violates NeverSwitches"
  "FelixRaftSwitchReachReseal violates NeverSwitchesAfterReseal"
  "FelixRaftReform pass"
  "FelixRaftReformTwoRotated pass"
  "FelixRaftReformTwoRotatedOffsets pass"
  "FelixRaftReformTwoRotatedOffsetsTerms pass"
  "FelixRaftReformEpochNotChecked pass"
  "FelixRaftReformRejoin pass"
  "FelixRaftReformStaleMembershipIntact pass"
  "FelixRaftReformLearnerWaited pass"
  "FelixRaftReformOneReplica pass"
  "FelixRaftReformOneReplicaLearner pass"
  "FelixRaftReformDead pass"
  "FelixRaftReformStaleMembership pass"
  "FelixRaftReformStaleMembershipTwoRotated pass"
  "FelixRaftReformRejoinFull pass"
  "FelixRaftReformTwoRotatedFull pass"
  "FelixRaftReformTwoRestarts pass"
  "FelixRaftReformBeforeAll violates AckedSurvive"
  "FelixRaftReformReportNotPromise violates AckedSurvive"
  "FelixRaftReformPlanNotRechecked violates AckedSurvive"
  "FelixRaftReformRetireNotDurable violates AckedSurvive"
  "FelixRaftReformBaseNotHighest violates AckedSurvive"
  "FelixRaftReformRotatedNotWaited violates AckedSurvive"
  "FelixRaftReformStaleMembershipTwoMovesForgotten violates AckedSurvive"
  "FelixRaftReformStopNotRechecked violates ElectedGroupLeftAlone"
  "FelixRaftReformNoCas violates StateMachineSafety"
  "FelixRaftReformOneReplicaSoloPlacedTwo violates AckedSurvive"
  "FelixRaftReformStaleMembershipTwoRotatedUnlisted violates AckedSurvive"
  "FelixRaftReformStaleMembershipFullUnlisted violates AckedSurvive"
  "FelixRaftReformLearnerHolds violates AckedSurvive"
  "FelixRaftReformWrongDeclaration violates AckedSurvive"
  "FelixRaftReformTwoRotatedOffsetsTrailing violates OffsetNeverReused"
  "FelixRaftReformTwoRotatedOffsetsNoFloor violates OffsetNeverReused"
  "FelixRaftReformTwoRotatedOffsetsTermsIndexOnly violates OffsetNeverReused"
  "FelixRaftReformTwoRotatedOffsetsTermsCuts violates NeverCuts"
  "FelixRaftReformTwoRotatedOffsetsTermsUnvouched violates LossNeedsLostDisk"
  "FelixRaftReformTwoRotatedOffsetsCutToCommit violates AckedSurvive"
  "FelixRaftReformCommits violates NeverCommitsAfterReform"
  "FelixRaftReformOneReplicaCommits violates NeverCommitsAfterReform"
  "FelixRaftReformOneReplicaSelfStarts violates NeverSelfStarts"
  "FelixRaftReformTwoRotatedOneVoter violates NeverOneVoterCommits"
  "FelixRaftReformTwoRotatedOffsetsPads violates NeverPads"
  "FelixRaftReformRejoinHappens violates NeverRetiredRejoins"
  "FelixRaftReformStaleMembershipHappens violates NeverReformsAfterChange"
  "FelixRaftReformLearnerHoldsHappens violates NeverLearnerHolds"
  "FelixRaftReformDeadReforms violates NeverReformsWithoutDead"
  "FelixRaftReformDeadLoses violates NothingLost"
  "FelixRaftSwitchGenZeroFull pass"
  "FelixRaftMembershipTerms pass"
  "FelixRaftLease pass"
  "FelixRaftLeaseAck pass"
  "FelixRaftLeaseDeliver pass"
  "FelixRaftLeaseRestart pass"
  "FelixRaftLeaseRestartRenews pass"
  "FelixRaftLeaseStore pass"
  "FelixRaftLeaseStoreStall pass"
  "FelixRaftLeaseStoreWide pass"
  "FelixRaftLeaseRestartForgets violates LeaseExclusive"
  "FelixRaftLeaseLong violates LeaseExclusive"
  "FelixRaftLeaseDrift violates LeaseExclusive"
  "FelixRaftLeaseFromAck violates LeaseExclusive"
  "FelixRaftLeaseVoteInWindow violates LeaseExclusive"
  "FelixRaftLeaseStoreNotDurable violates LeaseExclusive"
  "FelixRaftLeaseStoreVote violates LeaseExclusive"
  "FelixRaftLeaseStoreKept violates LeaseExclusive"
  "FelixRaftLeaseStoreResupport violates LeaseExclusive"
  "FelixRaftLeaseStoreWithdrawEarly violates LeaseExclusive"
  "FelixRaftLeaseStoreLong violates LeaseExclusive"
  "FelixRaftLeaseAckLong violates NoAckBehindNewLeader"
  "FelixRaftLeaseDeliverUncommitted violates DeliveredNeverReplaced"
  "FelixRaftLeaseReachLocalAck violates NeverLocalAck"
  "FelixRaftLeaseReachRenewed violates NeverLapsedThenRenewed"
  "FelixRaftLeaseReachFailover violates NeverFailoverAfterLease"
  "FelixRaftLeaseStoreStalledAsks violates StalledLeaseLapses"
  "FelixRaftLeaseStoreReachLocalAck violates NeverLocalAck"
  "FelixRaftLeaseStoreReachFailover violates NeverFailoverAfterLease"
  "FelixRaftSleep pass"
  "FelixRaftSleepWrite pass"
  "FelixRaftSleepLeaderDies pass"
  "FelixRaftSleepLivenessMisses pass"
  "FelixRaftSleepFalseAlarm pass"
  "FelixRaftSleepRestart pass"
  "FelixRaftSleepAnyState pass"
  "FelixRaftSleepEnds pass"
  "FelixRaftSleepNoWake violates QuiescentLed"
  "FelixRaftSleepSleepUnsupported violates QuiescentLed"
  "FelixRaftSleepCampaignOnWake violates NoNeedlessDeposal"
  "FelixRaftSleepRestartNoHold violates NoNeedlessDeposal"
  "FelixRaftSleepLeaderSleepsFirst violates NoNeedlessDeposal"
  "FelixRaftSleepHeartbeatKeepsProposal violates NoNeedlessDeposal"
  "FelixRaftSleepFalseAlarmFollower violates NoNeedlessDeposal"
  "FelixRaftSleepAskLost violates NoNeedlessDeposal"
  "FelixRaftSleepHoldTooShort violates NoNeedlessDeposal"
  "FelixRaftSleepAcceptUnchecked violates AsleepIsLevel"
  "FelixRaftSleepFalseAlarmFollowerBehind violates AsleepIsLevel"
  "FelixRaftSleepReachWakeOnWrite violates NeverWakesOnWrite"
  "FelixRaftSleepReachAuditElection violates NeverElectsAfterDyingAsleep"
)

# The long random walks, for `--simulate`: one pass configuration per family
# of set changes, and its negative twin, which must reach its violation
# within the budget to show the walks get that far at all. The election and
# spares families' twins are fixed traces instead
# (FelixShardWalkElectStaleSetReplay, FelixShardWalkSparesSeatEarlyReplay,
# checked with the others above): the walks found them only by chance. The inherited-name
# configurations with promotions on suspicion are here too: they outgrow a
# runner's disk exhaustively, so sampling them is what is left, as are the
# crashed leader's refusal beside a seat (with and without the fence ids'
# comparison) and beside a staged copy, voting or not. So is the unprobed
# crash, which no longer finishes in a runner's six hours exhaustively (#1584);
# ci.yml walks it per push, with its twin, which also stays an exhaustive check.
# FelixShardElectUncleanContinuingDoubtedFull (the registered Doubted with one
# client write) is walked too: exhaustively it passed 178M distinct states
# unfinished in six hours, so the write case is sampled nightly while Doubted
# (MaxWrites 0) stays exhaustive.
walks=(
  "FelixShardWalkSpares pass"
  "FelixShardWalkMoves pass"
  "FelixShardWalkMovesDestination violates AckedHeldByLeader"
  "FelixShardWalkHandoff pass"
  "FelixShardWalkHandoffNoWait violates AtMostOneServing"
  "FelixShardWalkElect pass"
  "FelixShardWalkElectSwap pass"
  "FelixShardWalkElectSwapNoJoint violates AckedHeldByLeader"
  "FelixShardWalkElectRename pass"
  "FelixShardWalkElectRenameUnfenced violates AckedHeldByLeader"
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
  "FelixShardElectUncleanContinuingDoubtedFull pass"
)
registered=("${expectations[@]}")
if [ "$mode" = simulate ] || [ -n "${affected_walks:-}" ]; then registered=("${walks[@]}"); fi

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

# FelixShardCpDown, FelixShardCpDownPreVoteUnbounded and
# FelixShardCpDownStandBehindDeadOnly are not registered: none finishes in
# five hours in CI (docs/formal/README.md has how far each got). Nor are
# FelixShardCpLive and FelixShardCpLiveNoStandBehindBallot, which run past a
# pull request's hour, and FelixShardElectAdoptAboveKeptSetReissued, which
# had not finished after seven minutes and 3.9M distinct states on a laptop.
# FelixShardCpLiveRefusals does not finish on a laptop either, and its twin
# FelixShardCpLiveRefusalsUnknownNoBallot has found no violation yet.
# FelixShardCpLiveSkippedGeneration, FelixShardCpLive cut down to the #2024
# scenario, runs past it too and is checked nightly (tla-walk.yml); its
# violating twin stays registered.
# FelixShardElectSwap is not registered: it does not finish in one run, so
# it is checked in pieces split at the cut-over. The eleven that take under 45
# minutes are registered above; FelixShardElectSwapKeep0, the four
# FelixShardElectSwap{Keep0,Keep1,Keep2,Drop2}Stood and
# FelixShardElectSwapDrop{0,1}StoodSeveralDest run in the nightly exhaustive
# job (tla-walk.yml).
# FelixShardElectSwapFlip is checked the same way, in twenty pieces. The nine
# under 45 minutes are registered above; FelixShardElectSwapFlip{Keep0,Drop0},
# the four FelixShardElectSwapFlip{Keep,Drop}{0,1}StoodOneDest (each the same
# states as its unstood twin), FelixShardElectSwapFlipKeep0StoodOneOther and
# the four FelixShardElectSwapFlip{Keep,Drop}{0,1}StoodSeveralDest run nightly.
# So is FelixShardElectSwapFlipJoinLags.
# So are FelixShardElectWipeNonVoter{Twice,Failover,FailoverWipesLeader,
# WipesLeader,Setless,Groups}: each runs well over an hour on a four-core runner. Their
# violating twins and witnesses stay registered.
# So is FelixShardCpLiveReleasedFull, the registered FelixShardCpLiveReleased
# with every first leader, any number of stops and adoption up to 2. It ran
# past six hours twice, so nothing checks it yet, not even the nightly job.
# So is FelixShardCpLiveNoOwnBallotRefused, the single-fix twin of FelixShardCpLive
# without OwnBallotRefused. It needs adoption up to 4 and a restart; cut to one
# election and one stop it still reached 37M distinct states and 6 h on a CI
# runner without finishing, and at adoption 3 it passes. FelixShardCpLiveMain
# (every fix off) still shows the same violation.
# So are FelixShardUnifiedReadmitFull and FelixShardUnifiedDropEvidencedFull,
# the registered Readmit and DropEvidenced with one client write.
# So is FelixShardElectRename, which is checked in pieces
# (FelixShardElectRename{Keep,Drop}{0,1}{,Stood...}): the twelve that fit a
# pull request's shards are registered above.
# FelixShardElectRename{Keep0,Drop0,Keep1}StoodSeveralDest,
# FelixShardElectRenameDrop0StoodOneOther and the four StoodOneDest pieces run
# nightly (tla-walk.yml). The cut-over overwrites everything the destination's
# stand wrote, so the StoodOneDest pieces should reach the same states as the
# pieces where nobody stood; that is argued, not measured. Its
# witnesses and the two rename walks stay registered.
# FelixShardElectAdoptFailover is checked in pieces split at the first
# failover (FelixShardElectAdoptFailover{Same,Apart}{0,1}{,Stood}Gen{2,3,4}).
# The twelve registered above are empty: no first failover reaches them, and
# each checks it stays so. FelixShardElectAdoptFailover{Same0,Apart0,Apart1,Same1}Gen4
# and Same1Gen2 run nightly. Same{0,1}Gen{2,3} without Same1Gen2, Apart{0,1}Gen3
# did not finish in six hours, so nothing checks them yet. Same{0,1}StoodGen2
# are split again (Stood{One,Several}{Dest,Other}) and not measured yet.
# FelixRaftReformStaleMembershipFull, FelixRaftReformStaleMembershipTwoMoves
# and FelixRaftLeaseAckWide are not registered: each is registered once a
# dispatch run gives its declared outcome.
# FelixRaftPlacementFull takes 160 minutes on a CI runner and runs nightly
# (tla-walk.yml). FelixRaftPlacementMoveWipeFull did not finish in three
# hours, so nothing checks it yet.

# Minutes each of the longer configurations takes on a four-core CI runner,
# from the `Finished in` lines of a CI run (a new one is a guess until it has
# run); anything not listed is taken as one. Shards are filled longest first, each
# configuration going to the least loaded, so one long run does not land on
# top of others and push a job past its hour.
# A configuration missing here weighs one, so it is never left to
# `--plan-heavy`: give a new heavy one its weight.
weights=(
  "FelixShardMoveGiveUpKeep 22"
  "FelixShardMoveGiveUpKeepLive 22"
  "FelixShardMoveGiveUpFailoverLive 13"
  "FelixShardMoveGiveUpFailoverDrop 3"
  "FelixShardMoveGiveUpFourDrop 20"
  "FelixShardElectUnclean 39"
  "FelixShardElectUncleanNoStartRecord 7"
  "FelixShardElectUncleanContinuingDoubted 50"
  "FelixShardElectUncleanVouchOnShip 2"
  "FelixShardElectLiveUncleanLed 2"
  "FelixShardFencedAckMoveShort 60"
  "FelixShardFencedAckMovePromoted 235"
  "FelixShardWipeStripped 29"
  "FelixShardFencedAckMoveCancelShort 95"
  "FelixShardFencedAckStagedMoveShort 65"
  "FelixShardFencedAckTwoPromotionsStart 60"
  "FelixShardFencedCache 51"
  "FelixShardReadsRound 35"
  "FelixShardSessionsGroupRound 30"
  "FelixGroupsSplitSources 17"
  "FelixShardFencedAck 28"
  "FelixGroups 24"
  "FelixHeadTrim 17"
  "FelixHeadTrimCrash 12"
  "FelixHeadTrimTwoPromotions 2"
  "FelixHeadTrimCrashTwoPromotions 3"
  "FelixShardFigure8CutOver 19"
  "FelixShard 12"
  "FelixShardWipeFull 7"
  "FelixShardLeaderCrashGivesUp 3"
  "FelixShardProcessCrashKeepsId 15"
  "FelixShardLeaderProcessCrash 63"
  "FelixShardLeaderCrashReclaim 3"
  "FelixShardFencedWipe 33"
  "FelixShardFencedCrash 30"
  "FelixShardFencedCrashInheritedCounts 3"
  "FelixShardFencedLeaderCrashTwiceNamesByInheritance 21"
  "FelixShardFencedPromotion 12"
  "FelixShardCancelResend 11"
  "FelixShardFigure8FollowerAcks 10"
  "FelixShardFigure8AckWait 10"
  "FelixShardCancel 10"
  "FelixShardReportFloor 10"
  "FelixShardAckWithoutLease 6"
  "FelixShardFigure8 7"
  "FelixShardUnifiedRestore 68"
  "FelixShardUnifiedDropEvidenced 167"
  "FelixShardUnifiedReadmit 83"
  "FelixShardUnified 32"
  "FelixShardUnifiedMixed 20"
  "FelixShardUnifiedCpLiveReadmitUnopened 10"
  "FelixShardUnifiedLearnerBlockingTwo 96"
  "FelixShardUnifiedLearnerBlocking 8"
  "FelixShardUnifiedLearnerVariantDropped 5"
  "FelixShardUnifiedIdleVouchWrite 16"
  "FelixShardUnifiedCpLiveMoveRotated 1"
  "FelixShardUnifiedCpLiveMoveRotatedLegacy 1"
  "FelixShardUnifiedTwoRotated 1"
  "FelixShardUnifiedTwoRotatedForgets 1"
  "FelixShardUnifiedSets 7"
  "FelixShardUnifiedCpLiveMoveDoubted 3"
  "FelixShardUnifiedVouchMark 3"
  "FelixShardUnifiedContinuingDoubtedLegacy 2"
  "FelixShardRealMargins 6"
  "FelixShardRealMarginsLease 4"
  "FelixShardFencedAckSeat 15"
  "FelixShardFencedAckSeatPair 18"
  "FelixShardFencedAckUndoPair 16"
  "FelixShardFencedAckTrim 10"
  "FelixShardFollowerLabels 6"
  "FelixShardHandoff 5"
  "FelixShardHandoffAdmitAck 3"
  "FelixShardIdempotentHandoff 5"
  "FelixShardStalePlannerCas 5"
  "FelixShardLogOrder 3"
  "FelixShardStagedMove 3"
  "FelixShardElectHandoffLeaders 10"
  "FelixShardElectAdopt 40"
  "FelixShardElectPreVote 1"
  "FelixShardElectAdoptPromote 10"
  "FelixShardReplacedPendingLeft 3"
  "FelixShardReplacedPendingLeftKept 5"
  "FelixShardReplacedPendingLeftKeptLogBatchDrops 5"
  "FelixShardReplacedPendingLeftTwice 30"
  "FelixShardReplacedPendingLeftTwiceAllLog 25"
  "FelixShardReplacedPendingLeftTwiceAll 21"
  "FelixShardReplacedPendingLeftTwiceAllLogBatchDrops 19"
  "FelixShardReplacedPendingLeftTwiceAllLogReportNoFloor 42"
  "FelixShardElectReissue 1"
  "FelixShardElectReissueReplaceBatchSet 2"
  "FelixShardElectLiveReissue 10"
  "FelixShardElectRouteLag 12"
  "FelixShardElectRouteLagReportOnly 18"
  "FelixShardElectRouteLagNoFix 2"
  "FelixShardElectRouteLagFence 27"
  "FelixShardElectRouteLagFenceNoFix 1"
  "FelixShardElectRouteLagReportStore 68"
  "FelixShardElectRouteLagFenceReportStore 69"
  "FelixShardElectRenameKeep1StoodOneOtherReportStore 4"
  "FelixShardElectRenameDrop1StoodOneOtherReportStore 4"
  "FelixShardElectAdoptFailoverApart0Gen2 5"
  "FelixShardElectAdoptFailoverApart1Gen2 5"
  "FelixShardElectAdoptFailoverApart0StoodGen2 5"
  "FelixShardElectAdoptFailoverApart1StoodGen2 5"
  "FelixShardElectAdoptFailoverSame0StoodGen3 4"
  "FelixShardElectAdoptFailoverApart0StoodGen3 4"
  "FelixShardElectAdoptFailoverSame1StoodGen3 3"
  "FelixShardElectAdoptFailoverApart1StoodGen3 5"
  "FelixShardElectAdoptFailoverSame0StoodGen4 5"
  "FelixShardElectAdoptFailoverApart0StoodGen4 4"
  "FelixShardElectAdoptFailoverSame1StoodGen4 3"
  "FelixShardElectAdoptFailoverApart1StoodGen4 5"
  "FelixShardSwapCounters 40"
  "FelixShardSessionsSubscriber 4"
  "FelixShardSuspicion 2"
  "FelixShardSuspicionWipeRoutes 3"
  "FelixShardSuspicionWipeUnbacked 2"
  "FelixShardElectWipeOracle 12"
  "FelixShardElectWipeNonVoter 45"
  "FelixShardElectWipeNonVoterCreates 6"
  "FelixShardElectWipeNonVoterCreatesReadmits 2"
  "FelixShardElectWipeNonVoterWipesLeader 40"
  "FelixShardElectWipeNonVoterTwice 60"
  "FelixShardElectWipeNonVoterFailover 90"
  "FelixShardElectWipeNonVoterFailoverWipesLeader 100"
  "FelixShardElectWipeNonVoterSetless 120"
  "FelixShardElectWipeNonVoterGroups 120"
  "FelixShardElectWipeNonVoterGroupsKeepDropsMark 35"
  "FelixShardElectWipeNonVoterIdlessCreation 12"
  "FelixShardElectWipeNonVoterCountersUnchecked 8"
  "FelixShardElectWipeNonVoterReclaim 21"
  "FelixShardElectWipeNonVoterReclaimLive 5"
  "FelixShardElectSwapKeep1 2"
  "FelixShardElectSwapKeep2 1"
  "FelixShardElectSwapDrop0 4"
  "FelixShardElectSwapDrop1 1"
  "FelixShardElectSwapDrop2 5"
  "FelixShardElectSwapDrop2DropsChange 6"
  "FelixShardElectSwapKeep2DropsChange 9"
  "FelixShardElectSwapDrop0StoodOneDest 2"
  "FelixShardElectSwapDrop0StoodOneOther 1"
  "FelixShardElectSwapDrop0StoodSeveralOther 1"
  "FelixShardElectSwapDrop1StoodOneDest 1"
  "FelixShardElectSwapDrop1StoodOneOther 5"
  "FelixShardElectSwapFlipKeep1 2"
  "FelixShardElectSwapFlipDrop1 1"
  "FelixShardElectSwapFlipKeep1StoodOneOther 2"
  "FelixShardElectSwapFlipDrop1StoodOneOther 1"
  "FelixShardElectSwapFlipDrop0StoodOneOther 1"
  "FelixShardElectRenameKeep0 12"
  "FelixShardElectRenameKeep1 8"
  "FelixShardElectRenameDrop0 6"
  "FelixShardElectRenameDrop1 6"
  "FelixShardElectRenameKeep0StoodOneOther 4"
  "FelixShardElectRenameKeep1StoodOneOther 2"
  "FelixShardElectRenameDrop1StoodOneOther 3"
  "FelixShardElectRenameDrop1StoodSeveralDest 11"
  "FelixShardElectStepDown 3"
  "FelixShardHandoffLeaderAck 3"
  "FelixShardCpLiveNoOverlayRefused 6"
  "FelixShardCpLiveNoHaltReported 5"
  "FelixShardCpLiveNoAboveReported 6"
  "FelixShardCpLiveReleased 34"
  "FelixShardCpLiveRefusalsCopyExcluded 2"
  "FelixShardCpLiveRefusalsPinsForever 2"
  "FelixShardElectRefusals 6"
  "FelixShardCpLiveSkippedGenerationUnknownPrior 32"
  "FelixShardCpDownNoSelfSuspicion 15"
  "FelixRaft 3"
  "FelixRaftCrashes 2"
  "FelixRaftSnapshot 6"
  "FelixRaftSnapshotReplace 6"
  "FelixRaftOffsetsFailover 7"
  "FelixRaftFourTerms 45"
  "FelixRaftTermsFull 59"
  "FelixRaftOffsetsFull 20"
  "FelixRaftSnapshotCrashes 21"
  "FelixRaftOffsetsFailoverCrash 41"
  "FelixRaftCommitPriorTerm 1"
  "FelixRaftSaveCommittedUnflushed 2"
  "FelixRaftMembership 4"
  "FelixRaftMembershipFailover 8"
  "FelixRaftIdentityDirRestore 2"
  "FelixRaftIdentityWipeThenRestore 2"
  "FelixRaftIdentityRootRestoreSameRun 2"
  "FelixRaftIdentityRootRestoreNoWitness 9"
  "FelixRaftIdentityRootRestoreBatched 18"
  "FelixRaftIdentityWipeThenRootRestore 7"
  "FelixRaftPlacementFailover 10"
  "FelixRaftPlacementReseat 2"
  "FelixRaftPlacementReseatWrites 7"
  "FelixRaftPlacementMoveWipe 3"
  "FelixRaftPlacementStaleWipe 5"
  "FelixRaftPlacementReturnWipe 2"
  "FelixRaftPlacementStaleFull 49"
  "FelixRaftPlacementSameId 2"
  "FelixRaftPlacementSameIdFive 20"
  "FelixRaftPlacementFormAfterWipes 2"
  "FelixRaftPlacementFormEarlyWipeFirstBoot 2"
  "FelixRaftSwitchGenZero 6"
  "FelixRaftSwitchStarts 11"
  "FelixRaftSwitchReseal 4"
  "FelixRaftReform 3"
  "FelixRaftReformTwoRotated 2"
  "FelixRaftReformOneReplica 2"
  "FelixRaftReformDead 2"
  "FelixRaftReformStaleMembershipTwoRotated 7"
  "FelixRaftReformStaleMembershipTwoMovesForgotten 20"
  "FelixRaftReformRejoinFull 13"
  "FelixRaftReformTwoRotatedFull 15"
  "FelixRaftReformTwoRotatedOffsetsTerms 12"
  "FelixRaftSwitchGenZeroFull 37"
  "FelixRaftMembershipTerms 109"
  "FelixRaftLeaseAck 2"
  "FelixRaftLeaseDeliver 6"
  "FelixRaftLeaseRestart 6"
  "FelixRaftLeaseRestartRenews 5"
  "FelixRaftLeaseStore 2"
  "FelixRaftLeaseStoreStall 2"
  "FelixRaftLeaseStoreWide 36"
  "FelixRaftSleep 2"
  "FelixRaftSleepEnds 2"
)

# Minutes a configuration took on a pull request's runner, from the `Finished
# in` lines of a CI run, where that differs from `weights` (main's runners)
# by two or more. Only shard balancing reads this; which configurations a
# pull request runs still goes by `weights`.
pr_minutes=(
  "FelixHeadTrimCrashTwoPromotions 1"
  "FelixRaft 2"
  "FelixRaftCrashes 4"
  "FelixRaftIdentityDirRestore 3"
  "FelixRaftIdentityRootRestoreNoWitness 7"
  "FelixRaftIdentityRootRestoreSameRun 2"
  "FelixRaftIdentityWipeThenRootRestore 4"
  "FelixRaftLeaseAck 4"
  "FelixRaftLeaseDeliver 8"
  "FelixRaftLeaseRestart 12"
  "FelixRaftLeaseRestartRenews 8"
  "FelixRaftLeaseStore 3"
  "FelixRaftLeaseStoreKept 15"
  "FelixRaftMembership 6"
  "FelixRaftMembershipFailover 20"
  "FelixRaftOffsetsFailover 17"
  "FelixRaftPlacementFailover 8"
  "FelixRaftPlacementFormAfterWipes 3"
  "FelixRaftPlacementFormLogWipeFirstBoot 4"
  "FelixRaftPlacementMoveWipe 4"
  "FelixRaftPlacementReseatWrites 8"
  "FelixRaftPlacementReturnWipe 3"
  "FelixRaftPlacementSameId 5"
  "FelixRaftPlacementStaleWipe 3"
  "FelixRaftReform 3"
  "FelixRaftReformOneReplica 2"
  "FelixRaftReformRejoinFull 13"
  "FelixRaftReformStaleMembershipTwoMovesForgotten 25"
  "FelixRaftReformStaleMembershipTwoRotated 5"
  "FelixRaftReformTwoRotatedFull 15"
  "FelixRaftReformTwoRotatedOffsetsTerms 15"
  "FelixRaftSnapshot 7"
  "FelixRaftSnapshotCrashes 26"
  "FelixRaftSnapshotReplace 8"
  "FelixRaftSwitchGenZero 11"
  "FelixRaftSwitchLag 3"
  "FelixRaftSwitchReseal 11"
  "FelixRaftTerms 2"
  "FelixShard 9"
  "FelixShardAckWithoutLease 5"
  "FelixShardCancel 10"
  "FelixShardCpLiveNoAboveReported 4"
  "FelixShardCpLiveNoHaltReported 6"
  "FelixShardCpLiveNoOverlayRefused 5"
  "FelixShardCpLiveRefusalsPinsForever 2"
  "FelixShardElectAdoptFailoverApart0Gen2 4"
  "FelixShardElectAdoptFailoverApart0StoodGen2 3"
  "FelixShardElectAdoptFailoverApart0StoodGen3 3"
  "FelixShardElectAdoptFailoverApart0StoodGen4 4"
  "FelixShardElectAdoptFailoverApart1Gen2 4"
  "FelixShardElectAdoptFailoverApart1StoodGen2 5"
  "FelixShardElectAdoptFailoverApart1StoodGen3 3"
  "FelixShardElectAdoptFailoverApart1StoodGen4 5"
  "FelixShardElectAdoptFailoverSame0StoodGen3 3"
  "FelixShardElectAdoptFailoverSame0StoodGen4 3"
  "FelixShardElectAdoptFailoverSame1StoodGen3 4"
  "FelixShardElectAdoptFailoverSame1StoodGen4 4"
  "FelixShardElectAdoptPromote 11"
  "FelixShardElectHandoffLeaders 10"
  "FelixShardElectLiveReissue 10"
  "FelixShardElectLiveUncleanLed 4"
  "FelixShardElectRefusals 5"
  "FelixShardElectRenameDrop0 9"
  "FelixShardElectRenameDrop1 11"
  "FelixShardElectRenameDrop1StoodOneOther 1"
  "FelixShardElectRenameKeep0StoodOneOther 3"
  "FelixShardElectRenameKeep1 7"
  "FelixShardElectRenameKeep1StoodOneOtherReportStore 3"
  "FelixShardElectStepDown 2"
  "FelixShardElectSwapDrop0 4"
  "FelixShardElectSwapDrop0StoodOneDest 4"
  "FelixShardElectSwapDrop1StoodOneOther 1"
  "FelixShardElectSwapDrop2 1"
  "FelixShardElectSwapDrop2DropsChange 4"
  "FelixShardElectSwapKeep2DropsChange 10"
  "FelixShardElectUncleanNoStartRecord 12"
  "FelixShardElectUncleanVouchOnShip 3"
  "FelixShardElectWipeNonVoterCountersUnchecked 10"
  "FelixShardElectWipeNonVoterCreates 6"
  "FelixShardElectWipeNonVoterReclaimLive 5"
  "FelixShardFencedAckTrim 11"
  "FelixShardFencedCrashInheritedCounts 1"
  "FelixShardFigure8 4"
  "FelixShardFigure8AckWait 7"
  "FelixShardFigure8FollowerAcks 12"
  "FelixShardFollowerLabels 7"
  "FelixShardHandoff 6"
  "FelixShardHandoffAdmitAck 3"
  "FelixShardHandoffLeaderAck 3"
  "FelixShardIdempotentHandoff 4"
  "FelixShardLeaderCrashGivesUp 1"
  "FelixShardLeaderCrashReclaim 1"
  "FelixShardLease 2"
  "FelixShardLeaseRoutesHeartbeat 2"
  "FelixShardLogOrder 4"
  "FelixShardMoveGiveUpFailoverDrop 2"
  "FelixShardRealMargins 4"
  "FelixShardRealMarginsLease 6"
  "FelixShardReplacedPendingLeftKept 3"
  "FelixShardReplacedPendingLeftKeptLogBatchDrops 5"
  "FelixShardReportFloor 13"
  "FelixShardSessionsSubscriber 3"
  "FelixShardStagedMove 5"
  "FelixShardStalePlannerCas 4"
  "FelixShardSuspicion 2"
  "FelixShardSuspicionWipeRoutes 1"
  "FelixShardUnifiedContinuingDoubtedLegacy 3"
  "FelixShardUnifiedCpLiveMoveDoubted 2"
  "FelixShardUnifiedCpLiveReadmitUnopened 12"
  "FelixShardUnifiedLearnerBlocking 6"
  "FelixShardUnifiedLearnerVariantDropped 2"
  "FelixShardUnifiedSets 7"
  "FelixShardWipeFull 5"
)

weight_of() {
  local w
  for w in "${weights[@]}"; do
    if [ "${w%% *}" = "$1" ]; then echo "${w#* }"; return; fi
  done
  echo 1
}

# The configurations whose own .cfg changed against `TLA_CHANGED_BASE` (a
# revision; CI passes the PR's base), space-separated.
changed_cfgs=" "
if [ -n "${TLA_CHANGED_BASE:-}" ]; then
  # Captured first so a base that does not resolve fails the run: an empty
  # list would quietly leave a changed configuration unchecked.
  changed_files="$(git diff --no-renames --name-only "$TLA_CHANGED_BASE" HEAD -- "$SPEC_DIR/*.cfg")"
  while read -r f; do
    [ -n "$f" ] || continue
    f="${f##*/}"
    changed_cfgs="$changed_cfgs${f%.cfg} "
  done <<<"$changed_files"
  # FelixRaft.tla is behind few configurations, so a change to it counts as a
  # change to each: FelixRaftFourTerms is the only passing check of the
  # commit rule for earlier terms and is over ten minutes.
  # The track 4 modules beside it are each behind their own few, the same way.
  raft_parts="Membership Identity Placement Reform Lease Sleep Switch"
  if ! git diff --quiet "$TLA_CHANGED_BASE" HEAD -- "$SPEC_DIR/FelixRaft.tla"; then
    for f in "$SPEC_DIR"/FelixRaft*.cfg; do
      f="${f##*/}"
      for part in $raft_parts; do
        case "$f" in FelixRaft"$part"*) continue 2 ;; esac
      done
      changed_cfgs="$changed_cfgs${f%.cfg} "
    done
  fi
  for part in $raft_parts; do
    if ! git diff --quiet "$TLA_CHANGED_BASE" HEAD -- "$SPEC_DIR/FelixRaft$part.tla"; then
      for f in "$SPEC_DIR"/FelixRaft"$part"*.cfg; do
        [ -e "$f" ] || continue
        f="${f##*/}"
        changed_cfgs="$changed_cfgs${f%.cfg} "
      done
    fi
  done
fi

# `--affected <base>` lists what a pull request checks: the configurations
# whose result the diff from <base> (base...HEAD) can change.
#
# - A changed .cfg is that configuration.
# - A changed .tla is every configuration of that module and of each module
#   that EXTENDS or INSTANCEs it, transitively.
# - In this script, a changed line of a registration table (`expectations`,
#   `walks`, `weights`, `pr_minutes`) is the configuration it names. A changed
#   `Pattern) echo Module ;;` line of `module_of`, old or new, is the
#   configurations the pattern matches plus those of the module and its
#   dependents, as for a .tla. Any other changed line, unless a comment or
#   blank, is all of them.
# - In ci.yml, a changed line before `jobs:` or in the `changes`, `formal*`
#   or `ci-ok` jobs is all of them, unless a comment or blank.
# - Anything else, docs/formal/README.md included, is nothing.
#
# Only registered configurations are listed, in registration order, then
# filtered to a pull request's tier (`TLA_QUICK` below). Two comment lines
# follow: the total of their `pr_minutes` and the shards to split it into,
# one per 30 minutes, at most 30 and none when nothing is listed.
# `--affected <base> --simulate` lists the affected `walks` instead.

# The modules one .tla extends or instantiates, space-separated.
module_deps() {
  awk '{ sub(/\\\*.*/, "") }
    cont || /^[[:space:]]*EXTENDS[[:space:]]/ {
      line = $0; sub(/^[[:space:]]*EXTENDS/, "", line); gsub(/,/, " ", line)
      printf "%s ", line; cont = ($0 ~ /,[[:space:]]*$/) }
    { while (match($0, /INSTANCE[[:space:]]+[A-Za-z0-9_]+/)) {
        dep = substr($0, RSTART, RLENGTH); sub(/^INSTANCE[[:space:]]+/, "", dep)
        printf "%s ", dep; $0 = substr($0, RSTART + RLENGTH) } }' "$1"
}

# Changed lines of one file between two revisions, comments and blank lines
# left out, as `old <line> <text>` or `new <line> <text>`.
changed_lines() {
  git diff -U0 --no-color "$1" "$2" -- "$3" | awk '
    /^@@/ {
      seen = 1
      match($0, /-[0-9]+/); o = substr($0, RSTART + 1, RLENGTH - 1)
      match($0, /\+[0-9]+/); n = substr($0, RSTART + 1, RLENGTH - 1)
      next }
    !seen || /^\\/ { next }
    { text = substr($0, 2) }
    /^-/ { side = "old"; line = o++ }
    /^\+/ { side = "new"; line = n++ }
    text ~ /^[[:space:]]*(#|$)/ { next }
    { print side, line, text }'
}

# Lines of this script inside a registration table, as `first-last` ranges.
table_ranges() {
  awk '/^(expectations|walks|weights|pr_minutes)=\($/ { s = NR + 1 }
    s && /^\)$/ { printf "%d-%d ", s, NR - 1; s = 0 }'
}

# Lines of ci.yml that decide how the models are checked: the triggers, and
# the jobs that pick, run and gate the checks.
workflow_ranges() {
  awk 'function close_job() { if (keep) printf "%d-%d ", s, NR - 1 }
    /^jobs:/ { printf "1-%d ", NR; injobs = 1; next }
    injobs && /^  [A-Za-z0-9_-]+:[[:space:]]*$/ {
      close_job(); name = $1; sub(/:$/, "", name); s = NR
      keep = (name == "changes" || name == "ci-ok" || name ~ /^formal/) }
    END { if (injobs) { NR++; close_job() } else printf "1-%d ", NR }'
}

# Lines of this script inside the body of `module_of`, as a `first-last` range.
module_of_range() {
  awk '/^module_of\(\) \{$/ { s = NR + 1 }
    s && /^\}$/ { printf "%d-%d ", s, NR - 1; s = 0 }'
}

# Prints the `old`/`new` changed lines, each marked `in` when it falls in the
# given old/new ranges, else `map` when in the optional second pair, else `out`.
in_ranges() {
  awk -v old="$1" -v new="$2" -v old2="${3:-}" -v new2="${4:-}" '
    function within(l, r,   k, i, parts, ab) {
      k = split(r, parts, " ")
      for (i = 1; i <= k; i++) { split(parts[i], ab, "-"); if (l >= ab[1] && l <= ab[2]) return 1 }
      return 0 }
    { l = $2 + 0; o = ($1 == "old")
      print (within(l, o ? old : new) ? "in" : within(l, o ? old2 : new2) ? "map" : "out"), $0 }'
}

# The configuration names a diff affects, one per line, or `*` for all.
affected_names() {
  local base files f name modules=" " grew deps dep lines kind pat mod
  local patterns=()
  # Errors return, so the caller fails: an empty list would quietly leave a
  # changed configuration unchecked.
  base="$(git merge-base "$1" HEAD)" || return 1
  files="$(git diff --no-renames --name-only "$base" HEAD)" || return 1
  while read -r f; do
    case "$f" in
      "$SPEC_DIR"/*.cfg)
        name="${f##*/}"
        echo "${name%.cfg}"
        ;;
      "$SPEC_DIR"/*.tla)
        name="${f##*/}"
        modules="$modules${name%.tla} "
        ;;
      scripts/check_tla.sh)
        lines="$(changed_lines "$base" HEAD "$f" \
          | in_ranges "$(git show "$base:$f" | table_ranges)" "$(table_ranges <"$f")" \
            "$(git show "$base:$f" | module_of_range)" "$(module_of_range <"$f")" \
          | awk '$1 == "out" { print "*"; next }
              match($0, /^in (old|new) [0-9]+ [[:space:]]*"[A-Za-z0-9_]+/) {
                n = substr($0, RSTART, RLENGTH); sub(/.*"/, "", n); print n; next }
              $1 == "map" && NF == 7 && $4 ~ /^[A-Za-z0-9_]+\*?\)$/ && $5 == "echo" \
                && $6 ~ /^[A-Za-z0-9_]+$/ && $7 == ";;" {
                sub(/\)$/, "", $4); print "map", $4, $6; next }
              { print "*" }')" || return 1
        # A `module_of` mapping, old or new, is the configurations its pattern
        # matches and every configuration of its module and dependents.
        while read -r kind pat mod; do
          if [ "$kind" = map ]; then
            patterns+=("$pat")
            [[ "$modules" == *" $mod "* ]] || modules="$modules$mod "
          elif [ -n "$kind" ]; then
            echo "$kind"
          fi
        done <<<"$lines"
        ;;
      .github/workflows/ci.yml)
        changed_lines "$base" HEAD "$f" \
          | in_ranges "$(git show "$base:$f" | workflow_ranges)" "$(workflow_ranges <"$f")" \
          | awk '$1 == "in" { hit = 1 } END { if (hit) print "*" }' || return 1
        ;;
    esac
  done <<<"$files"
  [ "$modules" != " " ] || [ "${#patterns[@]}" -gt 0 ] || return 0
  # Modules built on a changed one, until no more join.
  grew=1
  while [ -n "$grew" ]; do
    grew=""
    for f in "$SPEC_DIR"/*.tla; do
      [ -e "$f" ] || continue
      name="${f##*/}"
      name="${name%.tla}"
      [[ "$modules" == *" $name "* ]] && continue
      deps="$(module_deps "$f")"
      for dep in $deps; do
        if [[ "$modules" == *" $dep "* ]]; then
          modules="$modules$name "
          grew=1
          break
        fi
      done
    done
  done
  for f in "${registered[@]}"; do
    name="${f%% *}"
    if [[ "$modules" == *" $(module_of "$name") "* ]]; then echo "$name"; continue; fi
    for pat in ${patterns[@]+"${patterns[@]}"}; do
      # shellcheck disable=SC2053 # $pat is a case glob from module_of.
      if [[ "$name" == $pat ]]; then echo "$name"; break; fi
    done
  done
}

if [ "$mode" = affected ]; then
  names=" $(affected_names "$affected_base" | tr '\n' ' ') "
  kept=()
  for entry in "${selected[@]}"; do
    if [[ "$names" == *" * "* ]] || [[ "$names" == *" ${entry%% *} "* ]]; then kept+=("$entry"); fi
  done
  selected=(${kept[@]+"${kept[@]}"})
  # The walks have no tiers; the exhaustive checks get a pull request's.
  if [ -z "$affected_walks" ]; then TLA_QUICK=1; fi
fi

# `--plan-heavy` prints the changed registered configurations weighing more
# than 30 minutes as a JSON matrix, each with a job timeout of twice its
# weight plus headroom. A pull request checks each of them in a job of its
# own (ci.yml's `formal-heavy`), since they would not fit an hour beside the
# rest.
if [ "$mode" = plan-heavy ]; then
  sep=""
  printf '['
  for entry in "${registered[@]}"; do
    name="${entry%% *}"
    weight="$(weight_of "$name")"
    if [ "$weight" -gt 30 ] && [[ "$changed_cfgs" == *" $name "* ]]; then
      timeout=$((weight * 2 + 15))
      if [ "$timeout" -gt 360 ]; then timeout=360; fi
      printf '%s{"cfg":"%s","timeout":%d}' "$sep" "$name" "$timeout"
      sep=","
    fi
  done
  echo ']'
  exit 0
fi

# `TLA_QUICK=1` leaves out the registered configurations weighing more than
# ten minutes, except changed ones of 30 minutes or less. A changed one over
# 30 minutes is left to a job of its own (`--plan-heavy`). A changed .tla
# does not pull in its heavy configurations: FelixShard.tla is behind most of
# them. FelixRaft.tla and the track 4 modules beside it are the exceptions:
# a change to one counts as a change to each of its configurations (see
# `changed_cfgs`). Pull requests check that tier; the rest run when the spec
# changes on main. Configurations named on the command line always run. FelixShard,
# the base configuration every other one varies, runs on every pull request
# whatever its weight.
if [ -n "${TLA_QUICK:-}" ] && [ "$#" -eq 0 ] && [ -z "${affected_walks:-}" ]; then
  quick=()
  for entry in ${selected[@]+"${selected[@]}"}; do
    weight="$(weight_of "${entry%% *}")"
    if [ "$weight" -le 10 ] || [ "${entry%% *}" = FelixShard ]; then
      quick+=("$entry")
    elif [[ "$changed_cfgs" == *" ${entry%% *} "* ]]; then
      if [ "$weight" -le 30 ]; then
        quick+=("$entry")
      else
        echo "leaving changed ${entry%% *} (about $weight min) to a job of its own" >&2
      fi
    fi
  done
  selected=(${quick[@]+"${quick[@]}"})
fi

# Minutes to balance shards by: a pull request's measured time when listed.
plan_weight_of() {
  local w
  for w in "${pr_minutes[@]}"; do
    if [ "${w%% *}" = "$1" ]; then echo "${w#* }"; return; fi
  done
  weight_of "$1"
}

if [ "$mode" = affected ]; then
  total=0
  for entry in ${selected[@]+"${selected[@]}"}; do
    echo "${entry%% *}"
    total=$((total + $(plan_weight_of "${entry%% *}")))
  done
  count=$(((total + 29) / 30))
  if [ "$count" -gt 30 ]; then count=30; fi
  echo "# total: $total minutes"
  echo "# shards: $count"
  exit 0
fi

# Print the shard for each selected configuration, in order.
assign_shards() {
  local entry position=0
  for entry in "${selected[@]}"; do
    echo "$(plan_weight_of "${entry%% *}") $position"
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
      # `violates liveness` expects a temporal property to fail; an action
      # property is named as an invariant is.
      pattern="(Invariant|Action property) $invariant is violated"
      if [ "$invariant" = liveness ]; then pattern="Temporal properties were violated"; fi
      if ! grep -qE "$pattern" <<<"$output"; then
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
