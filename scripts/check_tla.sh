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
    FelixShardWalkElectStaleSetReplay) module="FelixShardWalkElectStaleSetReplay" ;;
    FelixShardWalkSparesSeatEarlyReplay) module="FelixShardWalkSparesSeatEarlyReplay" ;;
    FelixShardFigure8*) module="FelixShardFigure8" ;;
    FelixShardReads*) module="FelixShardReads" ;;
    FelixShardSessions*) module="FelixShardSessions" ;;
    FelixAtomicCommit*) module="FelixAtomicCommit" ;;
    FelixHeadTrim*) module="FelixHeadTrim" ;;
    FelixGroups*) module="FelixGroups" ;;
    FelixShardUnified*) module="FelixShardUnified" ;;
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
  "FelixShardCpLiveNoOwnBallotRefused violates liveness"
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

# Minutes each of the longer configurations takes on a four-core CI runner,
# from the `Finished in` lines of a CI run (a new one is a guess until it has
# run); anything not listed is taken as one. Shards are filled longest first, each
# configuration going to the least loaded, so one long run does not land on
# top of others and push a job past its hour.
# A configuration missing here weighs one, so it is never left to
# `--plan-heavy`: give a new heavy one its weight.
weights=(
  "FelixShardElectUnclean 22"
  "FelixShardElectUncleanNoStartRecord 12"
  "FelixShardElectUncleanContinuingDoubted 40"
  "FelixShardElectUncleanVouchOnShip 2"
  "FelixShardElectLiveUncleanLed 2"
  "FelixShardFencedAckMoveShort 31"
  "FelixShardWipeStripped 29"
  "FelixShardFencedAckMoveCancelShort 95"
  "FelixShardFencedAckStagedMoveShort 65"
  "FelixShardFencedAckTwoPromotionsStart 60"
  "FelixShardFencedCache 51"
  "FelixShardReadsRound 21"
  "FelixShardSessionsGroupRound 16"
  "FelixGroupsSplitSources 17"
  "FelixShardFencedAck 20"
  "FelixGroups 12"
  "FelixHeadTrim 17"
  "FelixHeadTrimCrash 6"
  "FelixHeadTrimTwoPromotions 2"
  "FelixHeadTrimCrashTwoPromotions 3"
  "FelixShardFigure8CutOver 14"
  "FelixShard 12"
  "FelixShardWipeFull 7"
  "FelixShardLeaderCrashGivesUp 3"
  "FelixShardProcessCrashKeepsId 9"
  "FelixShardLeaderProcessCrash 63"
  "FelixShardLeaderCrashReclaim 3"
  "FelixShardFencedWipe 25"
  "FelixShardFencedCrash 26"
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
  "FelixShardUnifiedRestore 27"
  "FelixShardUnifiedDropEvidenced 14"
  "FelixShardUnifiedReadmit 25"
  "FelixShardUnified 16"
  "FelixShardUnifiedMixed 11"
  "FelixShardUnifiedCpLiveReadmitUnopened 10"
  "FelixShardUnifiedLearnerBlockingTwo 70"
  "FelixShardUnifiedLearnerBlocking 8"
  "FelixShardUnifiedLearnerVariantDropped 5"
  "FelixShardUnifiedIdleVouchWrite 16"
  "FelixShardUnifiedSets 7"
  "FelixShardUnifiedCpLiveMoveDoubted 3"
  "FelixShardUnifiedVouchMark 3"
  "FelixShardUnifiedContinuingDoubtedLegacy 10"
  "FelixShardRealMargins 6"
  "FelixShardRealMarginsLease 4"
  "FelixShardFencedAckSeat 15"
  "FelixShardFencedAckSeatPair 12"
  "FelixShardFencedAckUndoPair 11"
  "FelixShardFencedAckTrim 10"
  "FelixShardFollowerLabels 6"
  "FelixShardHandoff 5"
  "FelixShardHandoffAdmitAck 3"
  "FelixShardIdempotentHandoff 5"
  "FelixShardStalePlannerCas 5"
  "FelixShardLogOrder 3"
  "FelixShardStagedMove 3"
  "FelixShardElectHandoffLeaders 10"
  "FelixShardElectAdopt 11"
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
  "FelixShardElectRouteLagFence 20"
  "FelixShardElectRouteLagFenceNoFix 1"
  "FelixShardElectRouteLagReportStore 25"
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
  "FelixShardElectWipeNonVoterGroupsKeepDropsMark 19"
  "FelixShardElectWipeNonVoterIdlessCreation 12"
  "FelixShardElectWipeNonVoterCountersUnchecked 8"
  "FelixShardElectWipeNonVoterReclaim 15"
  "FelixShardElectWipeNonVoterReclaimLive 3"
  "FelixShardElectSwapKeep1 27"
  "FelixShardElectSwapKeep2 8"
  "FelixShardElectSwapDrop0 43"
  "FelixShardElectSwapDrop1 17"
  "FelixShardElectSwapDrop2 5"
  "FelixShardElectSwapDrop2DropsChange 6"
  "FelixShardElectSwapKeep2DropsChange 9"
  "FelixShardElectSwapDrop0StoodOneDest 39"
  "FelixShardElectSwapDrop0StoodOneOther 13"
  "FelixShardElectSwapDrop0StoodSeveralOther 1"
  "FelixShardElectSwapDrop1StoodOneDest 12"
  "FelixShardElectSwapDrop1StoodOneOther 5"
  "FelixShardElectSwapFlipKeep1 15"
  "FelixShardElectSwapFlipDrop1 20"
  "FelixShardElectSwapFlipKeep1StoodOneOther 11"
  "FelixShardElectSwapFlipDrop1StoodOneOther 8"
  "FelixShardElectSwapFlipDrop0StoodOneOther 41"
  "FelixShardElectRenameKeep0 12"
  "FelixShardElectRenameKeep1 8"
  "FelixShardElectRenameDrop0 6"
  "FelixShardElectRenameDrop1 14"
  "FelixShardElectRenameKeep0StoodOneOther 11"
  "FelixShardElectRenameKeep1StoodOneOther 14"
  "FelixShardElectRenameDrop1StoodOneOther 3"
  "FelixShardElectRenameDrop1StoodSeveralDest 11"
  "FelixShardElectStepDown 3"
  "FelixShardHandoffLeaderAck 3"
  "FelixShardCpLiveNoOwnBallotRefused 175"
  "FelixShardCpLiveNoOverlayRefused 6"
  "FelixShardCpLiveNoHaltReported 5"
  "FelixShardCpLiveNoAboveReported 6"
  "FelixShardCpLiveReleased 15"
  "FelixShardCpLiveRefusalsCopyExcluded 2"
  "FelixShardCpLiveRefusalsPinsForever 2"
  "FelixShardElectRefusals 6"
  "FelixShardCpLiveSkippedGenerationUnknownPrior 20"
  "FelixShardCpDownNoSelfSuspicion 15"
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
  changed_files="$(git diff --name-only "$TLA_CHANGED_BASE" HEAD -- "$SPEC_DIR/*.cfg")"
  while read -r f; do
    [ -n "$f" ] || continue
    f="${f##*/}"
    changed_cfgs="$changed_cfgs${f%.cfg} "
  done <<<"$changed_files"
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
# them. Pull requests check that tier; the rest run when the spec changes on
# main. Configurations named on the command line always run. FelixShard,
# the base configuration every other one varies, runs on every pull request
# whatever its weight.
if [ -n "${TLA_QUICK:-}" ] && [ "$#" -eq 0 ]; then
  quick=()
  for entry in "${selected[@]}"; do
    weight="$(weight_of "${entry%% *}")"
    if [ "$weight" -le 10 ] || [ "${entry%% *}" = FelixShard ]; then
      quick+=("$entry")
    elif [[ "$changed_cfgs" == *" ${entry%% *} "* ]]; then
      if [ "$weight" -le 30 ]; then
        quick+=("$entry")
      else
        echo "leaving changed ${entry%% *} (about $weight min) to a job of its own"
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
