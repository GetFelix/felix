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
# `scripts/check_tla.sh FelixRaft FelixRaftLeaseAck` checks only those;
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
    if curl --proto "=https" --tlsv1.2 -sSL --fail --retry 3 -o "$JAR.tmp" "$TLA_URL"; then
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
    FelixShardReads*) echo FelixShardReads ;;
    FelixShardSessions*) echo FelixShardSessions ;;
    FelixAtomicCommit*) echo FelixAtomicCommit ;;
    FelixCrossShardCommit*) echo FelixCrossShardCommit ;;
    FelixHeadTrim*) echo FelixHeadTrim ;;
    FelixGroups*) echo FelixGroups ;;
    FelixRaftMembership*) echo FelixRaftMembership ;;
    FelixRaftIdentity*) echo FelixRaftIdentity ;;
    FelixRaftFirstForm*) echo FelixRaftFirstForm ;;
    FelixRaftFormedSet*) echo FelixRaftFormedSet ;;
    FelixRaftPlacement*) echo FelixRaftPlacement ;;
    FelixRaftReform*) echo FelixRaftReform ;;
    FelixRaftRestoreRepeated*) echo FelixRaftRestoreRepeated ;;
    FelixRaftRestore*) echo FelixRaftRestore ;;
    FelixRaftLease*) echo FelixRaftLease ;;
    FelixRaftSleep*) echo FelixRaftSleep ;;
    FelixRaftCache*) echo FelixRaftCache ;;
    FelixRaftSwitch*) echo FelixRaftSwitch ;;
    FelixRaftWritten*) echo FelixRaftWritten ;;
    FelixRaftBatch*) echo FelixRaftBatch ;;
    FelixRaftGroups*) echo FelixRaftGroups ;;
    FelixRaftCond*) echo FelixRaftCond ;;
    FelixRaftIdempotent*) echo FelixRaftIdempotent ;;
    FelixRaftReads*) echo FelixRaftReads ;;
    FelixRaftSessions*) echo FelixRaftSessions ;;
    FelixRaftStreamPurge*) echo FelixRaftStreamPurge ;;
    FelixRaftElect*) echo FelixRaftElect ;;
    FelixRaft*) echo FelixRaft ;;
    FelixShardMoveGiveUp*) echo FelixShardMoveGiveUp ;;
    *) echo "no module for configuration $1" >&2; exit 1 ;;
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
  "FelixShardReadsRound pass"
  "FelixShardReadsNoRound violates NoStaleRead"
  "FelixShardReadsLease violates NoStaleRead"
  "FelixShardSessionsSubscriber pass"
  "FelixShardSessionsPastMark violates NoLostDelivery"
  "FelixShardSessionsGroupRound pass"
  "FelixShardSessionsGroupNoRound violates NoStaleGroupCommit"
  "FelixShardSessionsGroupLease violates NoStaleGroupCommit"
  "FelixPlacementPacing pass"
  "FelixPlacementPacingUncountedReplacement violates CopiesWithinLimit"
  "FelixPlacementPacingTwoPlanners pass"
  "FelixPlacementPacingUnfenced violates CopiesWithinLimit"
  "FelixAtomicCommit pass"
  "FelixAtomicCommitSplitRecords violates NoPartialCommit"
  "FelixAtomicCommitPartialApply violates NoPartialCommit"
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
  "FelixHeadTrimFollowerNoReadIndex violates NoReadBelowAckedTrim"
  "FelixHeadTrimLeaderNoReadIndex violates NoReadBelowAckedTrim"
  "FelixHeadTrimTailNoReadIndex violates NoReadBelowAckedTrim"
  "FelixGroups pass"
  "FelixGroupsNoCheck violates NoSkip"
  "FelixGroupsLengthOnly violates NoSkip"
  "FelixGroupsDropGroup violates NoSkip"
  "FelixGroupsSplitSources pass"
  "FelixGroupsSplitSourcesLengthOnly violates NoSkip"
  "FelixGroupsShipUnsynced violates NoSkip"
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
  "FelixRaftElectLive pass"
  "FelixRaftElectLiveFaults pass"
  "FelixRaftElectLiveSlack pass"
  "FelixRaftElectLiveFaultsSlack pass"
  "FelixRaftElectLiveSplits violates liveness"
  "FelixRaftElectLiveNoOpen violates liveness"
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
  "FelixRaftWritten pass"
  "FelixRaftWrittenCrash pass"
  "FelixRaftWrittenLearnerCounts violates AckedHeld"
  "FelixRaftWrittenOtherTermCounts violates AckedHeld"
  "FelixRaftWrittenJointOneHalf violates AckedHeld"
  "FelixRaftWrittenCommits violates CommittedHeld"
  "FelixRaftWrittenPowerCut pass"
  "FelixRaftWrittenPowerCutKeeps violates AckedNeverLost"
  "FelixRaftWrittenReachEarly violates NeverAcksEarly"
  "FelixRaftWrittenReachEarlyInJoint violates NeverAcksEarlyInJoint"
  "FelixRaftWrittenReachCommitsAfterFailover violates NeverCommitsAfterFailover"
  "FelixRaftWrittenCommitsPowerCut pass"
  "FelixRaftWrittenCommitsPowerCutKeeps violates AckedNeverLost"
  "FelixRaftWrittenCommitsReform pass"
  "FelixRaftWrittenCommitsReformTrailing violates OffsetNeverReused"
  "FelixRaftWrittenCommitsReformLoses violates NeverLosesCommitted"
  "FelixRaftWrittenReformOnCommit pass"
  "FelixRaftWrittenCommitsReformTermsFloorHolder violates OffsetNeverReused"
  "FelixRaftWrittenCommitsReformTermsHappens violates NeverCutsAcrossTerms"
  "FelixRaftWrittenCommitsReformTermsVoteOnly violates LossNeedsLostDisk"
  "FelixRaftWrittenCommitsReformJointDeadOneSetDirected violates BaseAgrees"
  "FelixRaftWrittenReformVouchDead pass"
  "FelixRaftWrittenReformGate pass"
  "FelixRaftWrittenReformGateOpen violates BaseAgrees"
  "FelixRaftWrittenReformVouchDeadFive violates NeverLosesCommitted"
  "FelixRaftWrittenReformReplacedByBroker violates BaseAgrees"
  "FelixRaftWrittenReformReplacedHappens violates NoDeposedAppend"
  "FelixRaftBatch pass"
  "FelixRaftBatchKeepsCut pass"
  "FelixRaftBatchEachRecord violates BatchExactlyOnce"
  "FelixRaftBatchRewrite violates BatchExactlyOnce"
  "FelixRaftBatchRewriteOrder violates BatchInOrder"
  "FelixRaftBatchForgetsCut violates BatchExactlyOnce"
  "FelixRaftBatchForgetsCutPlace violates ProducerPlaceAgrees"
  "FelixRaftBatchInstallWithoutTable violates BatchExactlyOnce"
  "FelixRaftBatchTableAtApply violates BatchExactlyOnce"
  "FelixRaftBatchHeldBeforeCommit violates BatchExactlyOnce"
  "FelixRaftBatchReachFinish violates NeverFinishesACut"
  "FelixRaftBatchReachFinishOverTail violates NeverFinishesOverTail"
  "FelixRaftBatchReachRefusal violates NeverRefuses"
  "FelixRaftBatchReachInstall violates NeverInstalls"
  "FelixRaftBatchKeepsCutReachSnapshotFinish violates NeverFinishesFromSnapshot"
  "FelixRaftBatchReachHeld violates NeverAnswersHeld"
  "FelixRaftBatchResetCrash pass"
  "FelixRaftBatchResetCrashDropsTable violates BatchExactlyOnce"
  "FelixRaftGroups pass"
  "FelixRaftGroupsNoCreationIndex violates NoSkip"
  "FelixRaftGroupsNoDeleteWatermark violates NoSkip"
  "FelixRaftGroupsNoRedrivenSet violates NoSkip"
  "FelixRaftGroupsIfNewTableOnly violates IfNewKeepsServed"
  "FelixRaftGroupsTrackerAcrossTerms violates RedrivenOwed"
  "FelixRaftGroupsHydrateBeforeApply violates RedrivenOwed"
  "FelixRaftGroupsUserOpsBeforeOwnApply violates RedrivenOwed"
  "FelixRaftGroupsStaleByHydrateIndex violates RedrivenOwed"
  "FelixRaftGroupsDeadAfterAdvance violates NoSkip"
  "FelixRaftGroupsNoRefreshOnDrop violates TrackerHeard"
  "FelixRaftGroupsTwoChanges pass"
  "FelixRaftStreamPurgeSmall pass"
  "FelixRaftStreamPurgeLearnerSmall pass"
  "FelixRaftStreamPurgeLearnerReformSmall pass"
  "FelixRaftStreamPurgeNoCapSmall pass"
  "FelixRaftStreamPurgeFourSmall pass"
  "FelixRaftStreamPurgeFourReformSmall pass"
  "FelixRaftStreamPurgeTwoReformsSmall pass"
  "FelixRaftStreamPurgeNoCapSmallRefuse pass"
  "FelixRaftStreamPurgePastFloor violates NoAckedLoss"
  "FelixRaftStreamPurgeInstallAboveFloor violates NoAckedLoss"
  "FelixRaftStreamPurgeSnapAtApplied violates CatchUpPossible"
  "FelixRaftStreamPurgePastMatch violates CatchUpPossible"
  "FelixRaftStreamPurgeTablesAtApplied violates TablesAgree"
  "FelixRaftStreamPurgeFloorAtApplied violates TablesAgree"
  "FelixRaftStreamPurgeNoHeaders violates HeadersAgree"
  "FelixRaftStreamPurgeDropFirst violates AppliedCoversPurge"
  "FelixRaftStreamPurgeReformEntries violates OffsetNeverReused"
  "FelixRaftStreamPurgeReformNoFloor violates OffsetNeverReused"
  "FelixRaftStreamPurgeReformNoFloorTwice violates OffsetNeverReused"
  "FelixRaftStreamPurgeReformKeepsOwnFloor violates NoReadBelowFloor"
  "FelixRaftStreamPurgeReformFloorNotStored violates NoReadBelowFloor"
  "FelixRaftStreamPurgeReachInstall violates NeverServesAfterInstall"
  "FelixRaftStreamPurgeReachPurge violates NeverPurgesARecord"
  "FelixRaftStreamPurgeReachTrimPurge violates NeverPurgesATrim"
  "FelixRaftStreamPurgeReachWriteAfterReform violates NeverWritesAfterReform"
  "FelixRaftStreamPurgeReachPurgeAfterReform violates NeverPurgesAfterReform"
  "FelixRaftStreamPurgeReachTrimPurgeFour violates NeverPurgesATrim"
  "FelixRaftStreamPurgeReachWriteAfterReformFour violates NeverWritesAfterReform"
  "FelixRaftStreamPurgeReachPurgeAfterReformFour violates NeverPurgesAfterReform"
  "FelixRaftStreamPurgeReachWriteAfterReformTwo violates NeverWritesAfterReform"
  "FelixRaftStreamPurgeReachPurgeAfterReformTwo violates NeverPurgesAfterReform"
  "FelixRaftStreamPurgeReachWriteAndTrimBetweenReforms violates NeverWriteAndTrimBetweenReforms"
  "FelixRaftStreamPurgeReachRaiseBetweenReforms violates NeverRaiseBetweenReforms"
  "FelixRaftStreamPurgeReachTrimPurgeTwoReforms violates NeverPurgesATrimAfterTwoReforms"
  "FelixRaftStreamPurgeReachInstallThenReform violates NeverInstallsThenReforms"
  "FelixRaftStreamPurgeInstallLowersFloor violates OffsetNeverReused"
  "FelixRaftStreamPurgeInstallRefusesLowerFloor violates CatchUpPossible"
  "FelixRaftStreamPurgeInstallKeepsFloorSmall pass"
  "FelixRaftStreamPurgeReachLowerFloorInstall violates NeverInstallsLowerFloor"
  "FelixRaftStreamPurgeInstallLowersFloorSmall violates OffsetNeverReused"
  "FelixRaftStreamPurgeReachRewrittenInstall violates NeverInstallsRewrittenSnap"
  "FelixRaftCond pass"
  "FelixRaftCondTwoTerms pass"
  "FelixRaftCondLeaderDecides violates CondKeptAtExpected"
  "FelixRaftCondLeaderDecidesNoMembers pass"
  "FelixRaftCondReachAnswers violates NeverOkAndMismatch"
  "FelixRaftCondBatch pass"
  "FelixRaftCondBatchNoHide violates WholeBatchesOnly"
  "FelixRaftCondBatchBoundOnly violates WholeBatchesOnly"
  "FelixRaftCondBatchAnyLast violates WholeBatchesOnly"
  "FelixRaftCondBatchReachCut violates NeverCutCommitted"
  "FelixRaftCondBatchReachRead violates NeverReadsBatch"
  "FelixRaftCondBatchReachEarlyCut violates NeverEarlyCut"
  "FelixRaftReads pass"
  "FelixRaftReadsWritten pass"
  "FelixRaftReadsNoRound violates NoStaleRead"
  "FelixRaftReadsNoBlankBarrier violates NoStaleRead"
  "FelixRaftReadsSkipsAnswered violates NoStaleRead"
  "FelixRaftSessionsSubscriber pass"
  "FelixRaftSessionsPastCommit violates NoLostDelivery"
  "FelixRaftSessionsGroup pass"
  "FelixRaftSessionsGroupAckOnWrite violates NoStaleGroupCommit"
  "FelixRaftSessionsGroupIgnoresTerm violates NoStaleGroupCommit"
  "FelixRaftIdempotent pass"
  "FelixRaftIdempotentTwoTerms pass"
  "FelixRaftIdempotentMemory violates NoDuplicate"
  "FelixRaftIdempotentRewrite violates NoDuplicate"
  "FelixRaftIdempotentReachFinish violates NeverFinishesACut"
  "FelixRaftIdempotentReachHeld violates NeverHeldByLaterLeader"
  "FelixRaftIdentity pass"
  "FelixRaftIdentityBatched pass"
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
  "FelixRaftPlacementStaleWipe pass"
  "FelixRaftPlacementStaleWipeNoVersion violates AckedOnDisk"
  "FelixRaftPlacementStaleWipeRewind pass"
  "FelixRaftPlacementStaleWipeRewindSameInc violates AckedOnDisk"
  "FelixRaftPlacementDirectTermsNoBarrier violates StateMachineSafety"
  "FelixRaftPlacementTransferMove pass"
  "FelixRaftPlacementTransferMoveSelfRemove violates NeverLeadsRemoved"
  "FelixRaftPlacementReachTransfer violates NeverTransfers"
  "FelixRaftPlacementPlaced pass"
  "FelixRaftPlacementReachPlaced violates NeverTransfers"
  "FelixRaftPlacementSingleMove pass"
  "FelixRaftPlacementSingleMoveSelfRemove violates NeverLeadsRemoved"
  "FelixRaftPlacementStampDoomable pass"
  "FelixRaftPlacementStampDoomableNoStamp violates DroppedIsDoomable"
  "FelixRaftPlacementRewindSpareCommitted violates AckedOnDisk"
  "FelixRaftPlacementStaleWipeTwoTerms pass"
  "FelixRaftPlacementSameIdCounted violates AckedSurvive"
  "FelixRaftPlacementSameIdFive violates OneLeaderPerTerm"
  "FelixRaftPlacementTwoWipesFloor pass"
  "FelixRaftPlacementTwoWipesFloorVotersOnly violates AckedHeldSurvive"
  "FelixRaftPlacementPlacedTargetNoWrite pass"
  "FelixRaftPlacementReachPlacedTargetNoWrite violates NeverTransfersOffTarget"
  "FelixRaftPlacementTwoWipesFloorBlock pass"
  "FelixRaftPlacementFormWaiverFloor pass"
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
  "FelixRaftSwitchInFlight pass"
  "FelixRaftSwitchInFlightFailover pass"
  "FelixRaftSwitchPowerLoss pass"
  "FelixRaftSwitchCopySource pass"
  "FelixRaftSwitchDigestTag pass"
  "FelixRaftSwitchDigestSide pass"
  "FelixRaftSwitchVoteFloor pass"
  "FelixRaftSwitchSealHolds pass"
  "FelixRaftSwitchStartOrder pass"
  "FelixRaftSwitchInFlightClaimFirst violates LeaderAcksInBase"
  "FelixRaftSwitchInFlightFailoverLeaseAtEntry violates NoAckAfterHandOver"
  "FelixRaftSwitchPowerLossSameGen violates HybridAcksInBase"
  "FelixRaftSwitchCopySourceAny violates StateMachineSafety"
  "FelixRaftSwitchDigestTagSkipped violates LeaderCompleteness"
  "FelixRaftSwitchDigestSideSkipped violates SideComplete"
  "FelixRaftSwitchVoteFloorOff violates LeaderCompleteness"
  "FelixRaftSwitchSealHoldsReopen violates HybridAcksInBase"
  "FelixRaftSwitchSealHoldsBallot violates HybridAcksInBase"
  "FelixRaftSwitchStartOrderMarkFirst violates NeverReadsBelowBase"
  "FelixRaftSwitchReachInFlightAck violates NeverSwitchesAfterLeaderAck"
  "FelixRaftSwitchReachFailover violates NeverSwitchesAfterFailover"
  "FelixRaftSwitchReachPowerLoss violates NeverSwitchesAfterFailover"
  "FelixRaftSwitchFailoverWipe pass"
  "FelixRaftSwitchPowerLossLag pass"
  "FelixRaftSwitchPowerLossLagAll pass"
  "FelixRaftSwitchPowerLossFollowers pass"
  "FelixRaftSwitchEnds pass"
  "FelixRaftSwitchFailoverWipeVotes violates HybridAcksInBase"
  "FelixRaftSwitchPowerLossPeriodic violates HybridAcksInBase"
  "FelixRaftSwitchPowerLossAckBeforeSync violates BaseAgree"
  "FelixRaftSwitchEndsLeaderWiped violates liveness"
  "FelixRaftSwitchReachStartCrash violates NeverRestartsBeforeMark"
  "FelixRaftFirstForm pass"
  "FelixRaftFirstFormDisjoint pass"
  "FelixRaftFirstFormSingle pass"
  "FelixRaftFirstFormNoHold violates OneGroup"
  "FelixRaftFirstFormNoEpoch violates OneGroup"
  "FelixRaftFormedSet pass"
  "FelixRaftFormedSetSingle pass"
  "FelixRaftFormedSetNoHold violates OneGroup"
  "FelixRaftFormedSetByBroker violates OneGroup"
  "FelixRaftFormedSetEven pass"
  "FelixRaftFormedSetKeepOne violates OneGroup"
  "FelixRaftReform pass"
  "FelixRaftReformTwoRotated pass"
  "FelixRaftReformTwoRotatedOffsets pass"
  "FelixRaftReformTwoRotatedOffsetsTerms pass"
  "FelixRaftReformTwoReformsTrailingFloor pass"
  "FelixRaftReformTwoReformsTrailingFloorHappens violates NeverPadsByReserve"
  "FelixRaftReformTwoReformsTrailingFloorLeaderReserve violates OffsetNeverReused"
  "FelixRaftReformTwoReformsTrailingFloorAllDead violates OffsetNeverReused"
  "FelixRaftReformTwoReformsEpochMark violates NeverKeepsAcrossOldRestore"
  "FelixRaftReformKeptOldKept pass"
  "FelixRaftReformKeptOldKeptCap violates NeverCaps"
  "FelixRaftReformKeptTermsT violates NeverVouchesByT"
  "FelixRaftReformReplacedDiskByBroker violates OffsetNeverReused"
  "FelixRaftReformReplacedDiskLost violates OffsetNeverReused"
  "FelixRaftReformEpochNotChecked pass"
  "FelixRaftReformRejoin pass"
  "FelixRaftReformStaleMembershipIntact pass"
  "FelixRaftReformLearnerWaited pass"
  "FelixRaftReformOneReplica pass"
  "FelixRaftReformAlone pass"
  "FelixRaftReformAloneCluster pass"
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
  "FelixRaftReformAloneCutToCommit violates KeptLossBound"
  "FelixRaftReformAloneTwoCuts pass"
  "FelixRaftReformAloneTwoCutsHappens violates NeverReformsAloneTwice"
  "FelixRaftReformAloneCutBeforeStart violates NeverRecoversFromCutBeforeStart"
  "FelixRaftReformAloneClusterUnchecked violates LossNeedsLostDisk"
  "FelixRaftReformTwoRotatedOneVoter violates NeverOneVoterCommits"
  "FelixRaftReformTwoRotatedOffsetsPads violates NeverPads"
  "FelixRaftReformRejoinHappens violates NeverRetiredRejoins"
  "FelixRaftReformStaleMembershipHappens violates NeverReformsAfterChange"
  "FelixRaftReformLearnerHoldsHappens violates NeverLearnerHolds"
  "FelixRaftReformDeadReforms violates NeverReformsWithoutDead"
  "FelixRaftReformDeadLoses violates NothingLost"
  "FelixRaftReformAbandon pass"
  "FelixRaftReformAbandonHappens violates NeverAbandonCommits"
  "FelixRaftReformAbandonOutside violates AbandonedNotLed"
  "FelixRaftReformAbandonAtFloor violates OffsetNeverReused"
  "FelixRaftReformAbandonFromZero violates OffsetNeverReused"
  "FelixRaftReformAbandonOpen violates AbandonedNotLed"
  "FelixRaftRestore pass"
  "FelixRaftRestoreGap pass"
  "FelixRaftRestoreNoReserve violates BackedUpNeverReused"
  "FelixRaftRestoreNoRaise violates RestoresAtPoint"
  "FelixRaftRestoreLeaderMoved violates BackedUpNeverReused"
  "FelixRaftRestoreAfterBackup violates OffsetNeverReused"
  "FelixRaftRestoreGapShort violates OffsetNeverReused"
  "FelixRaftRestoreRepeated pass"
  "FelixRaftRestoreRepeatedNoTime violates OffsetNeverReused"
  "FelixRaftRestoreRepeatedEndGap violates OffsetNeverReused"
  "FelixRaftRestoreRepeatedMinApart violates OffsetNeverReused"
  "FelixRaftSwitchGenZeroFull pass"
  "FelixRaftMembershipTerms pass"
  "FelixRaftLease pass"
  "FelixRaftLeaseAck pass"
  "FelixRaftLeaseDeliver pass"
  "FelixRaftLeaseRestart pass"
  "FelixRaftLeaseRestartRenews pass"
  "FelixRaftLeaseRestoreLed pass"
  "FelixRaftLeaseRestoreNamed pass"
  "FelixRaftLeaseRestoreSingle pass"
  "FelixRaftLeaseStepDown pass"
  "FelixRaftLeaseStepDownSync pass"
  "FelixRaftLeaseTransfer pass"
  "FelixRaftLeaseTransferThreeTerms pass"
  "FelixRaftLeaseTransferWrite pass"
  "FelixRaftLeaseTransferStepDown pass"
  "FelixRaftLeaseRestoreTransferStepDown pass"
  "FelixRaftLeaseRestoreTransferThreeTerms pass"
  "FelixRaftLeaseRestoreTransferStepDownWrite pass"
  "FelixRaftLeaseRestoreStepDown pass"
  "FelixRaftLeaseSuspend pass"
  "FelixRaftLeaseStepDownWrite pass"
  "FelixRaftLeaseSuspendRoleLate pass"
  "FelixRaftLeaseStepDownDrift pass"
  "FelixRaftLeaseTransferDrift pass"
  "FelixRaftLeaseSuspendDrift pass"
  "FelixRaftLeaseStore pass"
  "FelixRaftLeaseStoreStall pass"
  "FelixRaftLeaseStoreWide pass"
  "FelixRaftLeaseRestartForgets violates LeaseExclusive"
  "FelixRaftLeaseRestoreAnyVote violates LeaseExclusive"
  "FelixRaftLeaseReachRestoreLed violates NeverHoldSkipped"
  "FelixRaftLeaseRestoreNamedAny violates LeaseExclusive"
  "FelixRaftLeaseReachHeldGrant violates NeverHeldGrant"
  "FelixRaftLeaseReachRestoreTransferThreeTerms violates NeverThirdTermOnHeldGrant"
  "FelixRaftLeaseReachRestoreTransferThreeTermsWrite violates NeverThirdTermLocalAck"
  "FelixRaftLeaseStepDownNoVoid violates LeaseExclusive"
  "FelixRaftLeaseTransferKeepsLease violates LeaseExclusive"
  "FelixRaftLeaseTransferVoidLate violates LeaseExclusive"
  "FelixRaftLeaseTransferVoidEquality violates LeaseExclusive"
  "FelixRaftLeaseSuspendLateCheck violates LeaseExclusive"
  "FelixRaftLeaseReachLeaseAfterVoid violates NeverLeaseAfterVoid"
  "FelixRaftLeaseSuspendUnchecked violates LeaseExclusive"
  "FelixRaftLeaseTransferStepDownNoVoid violates LeaseExclusive"
  "FelixRaftLeaseReachStepDownLocalAck violates NeverLocalAck"
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
  "FelixRaftLeaseReachSoleLocalAck violates NeverLocalAck"
  "FelixRaftLeaseReachRenewed violates NeverLapsedThenRenewed"
  "FelixRaftLeaseReachFailover violates NeverFailoverAfterLease"
  "FelixRaftLeaseStoreStalledAsks violates StalledLeaseLapses"
  "FelixRaftLeaseStoreReachLocalAck violates NeverLocalAck"
  "FelixRaftLeaseStoreReachFailover violates NeverFailoverAfterLease"
  "FelixRaftLeaseSleep pass"
  "FelixRaftLeaseSleepHoldEndClearsWindow violates LeaseExclusive"
  "FelixRaftLeaseSleepAnswerRenews violates LeaseExclusive"
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
  "FelixRaftSleepWake pass"
  "FelixRaftSleepWakeWrite pass"
  "FelixRaftSleepAuditLost pass"
  "FelixRaftSleepFalseAlarmFollowerAsked pass"
  "FelixRaftSleepFalseAlarmFollowerAskedLost pass"
  "FelixRaftSleepFalseAlarmFollowerBehindAsked pass"
  "FelixRaftSleepTtl pass"
  "FelixRaftSleepMidHandshake pass"
  "FelixRaftSleepWriteBeforeRound violates NoWriteRefused"
  "FelixRaftSleepAuditCampaigns pass"
  "FelixRaftSleepFalseAlarmFollowerAskedLostDroppedPreVoteGrants violates NoNeedlessDeposal"
  "FelixRaftSleepFalseAlarmFollowerAskedLostNoVoteHold violates NoNeedlessDeposal"
  "FelixRaftSleepFalseAlarmFollowerAskedLostNoPreVote violates NoNeedlessDeposal"
  "FelixRaftSleepFalseAlarmFollowerAskedLostNoLeftOutAsked pass"
  "FelixRaftSleepFalseAlarmFollowerBehindAskedNoRefusalWakes pass"
  "FelixRaftSleepNoTtlWake violates QuiescentTtl"
  "FelixRaftSleepReachMidHandshake violates NeverElectedMidHandshake"
  "FelixRaftSleepTtlWrite pass"
  "FelixRaftSleepTtlWriteUnconditional violates ExpiryKeepsNewer"
  "FelixRaftSleepTtlWriteReachRace violates NeverExpirySkipsNewer"
  "FelixRaftCache pass"
  "FelixRaftCacheClock pass"
  "FelixRaftCacheDedup pass"
  "FelixRaftCacheExpiry pass"
  "FelixRaftCacheClockSnapshot pass"
  "FelixRaftCacheNoRequestId violates SumExact"
  "FelixRaftCacheSnapshotDropsDedup violates SumExact"
  "FelixRaftCacheSnapshotIdAhead violates MembersAgree"
  "FelixRaftCacheReadNoApplyWait violates ReadsFresh"
  "FelixRaftCachePurgePastSnapshot violates LiveReadable"
  "FelixRaftCacheClockApplyLocalClock violates MembersAgree"
  "FelixRaftCacheClockReadsLocalClock violates OneClock"
  "FelixRaftCacheClockReadsLogTime violates ReadsNotStale"
  "FelixRaftCacheClockStampNotMonotone violates OneClock"
  "FelixRaftCacheClockFloorFromCommitted violates StampsMonotone"
  "FelixRaftCacheClockFloorCountsBlank violates StampsMonotone"
  "FelixRaftCacheDedupPutNotDeduped violates AnswersKept"
  "FelixRaftCacheDedupPutAbsentNotDeduped violates AppliedOnce"
  "FelixRaftCacheDedupDeleteNotDeduped violates AppliedOnce"
  "FelixRaftCacheExpiryUnconditional violates ExpiryRemovesItsVersion"
  "FelixRaftCacheClockSnapshotDropsTime violates StampsMonotone"
  "FelixRaftCacheReachInstallRead violates NeverReadAfterInstall"
  "FelixRaftCacheReachRetry violates NeverRetryApplied"
  "FelixRaftCacheDedupReachRetryAnswer violates NeverRetryAnswered"
  "FelixRaftCacheClockReachExpiry violates NeverExpiredThenTaken"
  "FelixRaftCacheExpiryReachRefresh violates NeverExpirySkipsNewer"
  "FelixRaftCacheExpiryReachExpireRead violates NeverReadAfterExpiry"
  "FelixRaftCacheClockSnapshotReachInstallStamp violates NeverStampFromSnapshot"
  "FelixRaftCacheDelete pass"
  "FelixRaftCacheDeleteSnapshot pass"
  "FelixRaftCacheCond pass"
  "FelixRaftCachePutAbsentSnapshot pass"
  "FelixRaftCacheCondSnapshot pass"
  "FelixRaftCacheDeleteSeesExpired violates OneClock"
  "FelixRaftCacheDeleteSnapshotSeesExpired violates AnswersKept"
  "FelixRaftCachePutAbsentSnapshotSeesExpired violates MembersAgree"
  "FelixRaftCacheCondSeesExpired violates OneClock"
  "FelixRaftCacheCondSnapshotSeesExpired violates AnswersKept"
  "FelixRaftCacheDeleteReachExpired violates NeverDeleteOfExpired"
  "FelixRaftCacheDeleteSnapshotReachDropped violates NeverDroppedAndKept"
  "FelixRaftCacheCondReachApplied violates NeverCondApplied"
  "FelixRaftCacheCondReachStale violates NeverCondStale"
  "FelixRaftCacheTrim pass"
  "FelixRaftCacheTrimRetireDropsFloor violates AppliedOnce"
  "FelixRaftCacheTrimSnapshotDropsFloor violates MembersAgree"
  "FelixRaftCacheTrimRetireLocal violates MembersAgree"
  "FelixRaftCacheTrimFloorPastUnanswered violates ForgetsOnlyDone"
  "FelixRaftCacheTrimNoFloorClamp violates ForgetsOnlyDone"
  "FelixRaftCacheTrimReachForgotten violates NeverForgotten"
  "FelixRaftCacheTrimReachRetryAboveFloor violates NeverRetryAboveFloor"
  "FelixRaftCacheTrimReachRetryAfterRetire violates NeverRetryAfterRetire"
  "FelixRaftCacheTrimReachInstalledFloor violates NeverInstalledFloor"
  "FelixRaftCacheFull pass"
  "FelixRaftCacheFullSnapshot pass"
  "FelixRaftCacheFullNotRecorded violates FullFinal"
  "FelixRaftCacheFullSnapshotRawSize violates MembersAgree"
  "FelixRaftCacheFullReachFull violates NeverFull"
)

# The long random walks, for `--simulate`: configurations too big to check
# exhaustively, sampled. A pass means nothing was found; a twin must still
# reach its violation within the budget.
walks=(
  "FelixCrossShardCommit pass"
  "FelixCrossShardCommitLive pass"
  "FelixCrossShardCommitNoFirstWins violates Atomicity"
  "FelixCrossShardCommitReleaseOnTimeout violates Atomicity"
  "FelixCrossShardCommitTrimUndone violates Atomicity"
  "FelixCrossShardCommitNoFence violates NoDoubleApply"
  "FelixCrossShardCommitWriteCheckEarly violates PreconditionsHeld"
  "FelixCrossShardCommitApplyWithoutLock violates NoForgedApply"
  "FelixCrossShardCommitNoTokenCheck violates NoForgedApply"
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

# FelixRaftReformStaleMembershipFull, FelixRaftReformStaleMembershipTwoMoves
# and FelixRaftLeaseAckWide are not registered: each is registered once a
# dispatch run gives its declared outcome.
# FelixRaftWrittenFailover passes in 46 minutes with 8 workers on a 32-core
# machine and runs nightly (tla-walk.yml). FelixRaftWrittenJoint passes in
# 2 h 10 min there, too long for a hosted runner's six hours, so only that
# box run checks it. FelixRaftWrittenTerms and FelixRaftWrittenJointFailover
# did not finish in two hours there (docs/formal/README.md has how far each
# got).
# FelixRaftWrittenFailoverPowerCut passes in 15 minutes with 8 workers on a
# 32-core machine and is not registered: a box run checks it.
# FelixRaftWrittenCommitsFailoverPowerCut passes in 17 minutes with 30
# workers on a 32-core machine and is not registered: a box run checks it.
# FelixRaftWrittenReformReplaced (past 10M distinct states in 4 minutes
# on a box, queue still growing), FelixRaftWrittenReformReplacedFull and
# FelixRaftReformReplacedDisk are not registered: they run past a pull
# request's time; run them as a TLA workflow dispatch.
# FelixRaftWrittenCommitsReformJoint{Dead,DeadOneSet,CutDead} are not
# registered: under their scopes none finished in six hours on a CI runner
# (run 45097934400; 335M, 242M and 169M distinct states). JointCutDead runs
# nightly. JointDead and JointDeadOneSet run there as well, reported only
# (tla-walk.yml, #2729); FelixRaftWrittenCommitsReformJointDeadOneSetDirected
# reaches JointDeadOneSet's violation in seconds and is registered.
# FelixRaftReformKeptTerms passes in 56 minutes and
# FelixRaftReformTwoReformsOldRestore in 4 h 16 min on a CI runner, past a
# pull request's hour, so they are not registered and run nightly
# (tla-walk.yml). FelixRaftWrittenCommitsReformTerms did not finish in a
# runner's six hours and is not registered. It passed on a 32-core build box
# (1,752,892,374 distinct states, depth 45, 6 h 53 min).
# FelixRaftLeaseRestoreTransferThreeTermsWrite is not registered: it passes
# (191M distinct states) in 90 minutes on 16 cores, about ten hours on a CI
# runner by its siblings' times, past the nightly job's six. Run it on a
# build box.
# FelixRaftPlacementFull (160 minutes on a CI runner) and
# FelixRaftPlacementRewindSpare (176 minutes) run nightly (tla-walk.yml).
# FelixRaftPlacementMoveWipeFull did not finish in three hours and
# FelixRaftPlacementTransferMoveFull, TransferMove with a write, not in six,
# so nothing checks them yet (issue 2497).
# FelixRaftPlacement{DirectTerms,MoveWipeTwoMoves,TransferMoveWipe,
# StaleWipeRewindNoAck} did not finish in six hours on a CI runner, and
# FelixRaftPlacement{StaleReturnWipe,StaleReturnWipeAnyVersion} ran past
# 45 minutes on a build box at their bounds; each is registered once a
# dispatch run gives its declared outcome.
# FelixRaftPlacementPlacedTarget, Placed with one target change, is not
# registered until a dispatch run passes it.
# FelixHeadTrim is not registered: its dispatch (run 43870273108) passed 29 of
# 30 shards and shard 0 hit the six hour limit at depth 34 with no violation
# (1.75B states, 244M distinct).

# Minutes each of the longer configurations takes on a four-core CI runner,
# from the `Finished in` lines of a CI run (a new one is a guess until it has
# run); anything not listed is taken as one. Shards are filled longest first, each
# configuration going to the least loaded, so one long run does not land on
# top of others and push a job past its hour.
# A configuration missing here weighs one, so it is never left to
# `--plan-heavy`: give a new heavy one its weight.
weights=(
  "FelixRaftReformKeptOldKept 12"
  "FelixRaftReformKeptOldKeptCap 6"
  "FelixShardMoveGiveUpKeep 22"
  "FelixShardMoveGiveUpKeepLive 22"
  "FelixShardMoveGiveUpFailoverLive 13"
  "FelixShardMoveGiveUpFailoverDrop 3"
  "FelixShardMoveGiveUpFourDrop 20"
  "FelixShardReadsRound 35"
  "FelixShardSessionsGroupRound 30"
  "FelixGroupsSplitSources 17"
  "FelixGroups 24"
  "FelixHeadTrimCrash 240"
  "FelixHeadTrimTwoPromotions 2"
  "FelixHeadTrimCrashTwoPromotions 3"
  "FelixShardSessionsSubscriber 4"
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
  "FelixRaftPlacementFailover 36"
  "FelixRaftPlacementReseat 2"
  "FelixRaftPlacementReseatWrites 7"
  "FelixRaftPlacementMoveWipe 22"
  "FelixRaftPlacementStaleWipe 27"
  "FelixRaftPlacementStaleWipeNoVersion 60"
  "FelixRaftPlacementTransferMove 51"
  "FelixRaftPlacementRewindSpareCommitted 57"
  "FelixRaftPlacementDirectTermsNoBarrier 46"
  "FelixRaftPlacementPlacedTargetNoWrite 108"
  "FelixRaftPlacementStaleWipeTwoTerms 150"
  "FelixRaftPlacementStaleWipeRewind 275"
  "FelixRaftPlacementStaleWipeRewindSameInc 191"
  "FelixRaftPlacementReturnWipe 2"
  "FelixRaftPlacementStaleFull 39"
  "FelixRaftPlacementSameId 2"
  "FelixRaftPlacementSameIdFive 14"
  "FelixRaftPlacementTwoWipesFloor 2"
  "FelixRaftPlacementTwoWipesFloorVotersOnly 2"
  "FelixRaftPlacementTwoWipesFloorBlock 2"
  "FelixRaftPlacementFormAfterWipes 2"
  "FelixRaftPlacementFormEarlyWipeFirstBoot 2"
  "FelixRaftSwitchGenZero 6"
  "FelixRaftSwitchStarts 11"
  "FelixRaftSwitchReseal 4"
  "FelixRaftSwitchFailoverWipe 60"
  "FelixRaftSwitchPowerLossLagAll 8"
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
  "FelixRaftWritten 40"
  "FelixRaftWrittenCrash 5"
  "FelixRaftWrittenCommitsPowerCut 24"
  "FelixRaftWrittenCommitsReform 2"
  "FelixRaftWrittenCommitsReformTermsFloorHolder 70"
  "FelixRaftWrittenReformGate 20"
  "FelixRaftWrittenReformVouchDeadFive 45"
  "FelixRaftReformTwoReformsTrailingFloor 9"
  "FelixRaftRestore 1"
  "FelixRaftRestoreNoReserve 1"
  "FelixRaftRestoreNoRaise 1"
  "FelixRaftRestoreLeaderMoved 1"
  "FelixRaftRestoreAfterBackup 1"
  "FelixRaftRestoreGap 1"
  "FelixRaftRestoreGapShort 1"
  "FelixRaftRestoreRepeated 1"
  "FelixRaftRestoreRepeatedNoTime 1"
  "FelixRaftRestoreRepeatedEndGap 1"
  "FelixRaftRestoreRepeatedMinApart 1"
  "FelixRaftBatch 18"
  "FelixRaftBatchKeepsCut 18"
  "FelixRaftStreamPurgeSmall 12"
  "FelixRaftStreamPurgeNoCapSmall 12"
  "FelixRaftStreamPurgeLearnerSmall 6"
  "FelixRaftStreamPurgeLearnerReformSmall 30"
  "FelixRaftStreamPurgeFourSmall 3"
  "FelixRaftStreamPurgeFourReformSmall 20"
  "FelixRaftStreamPurgeTwoReformsSmall 5"
  "FelixRaftStreamPurgeInstallLowersFloor 3"
  "FelixRaftStreamPurgeInstallRefusesLowerFloor 2"
  "FelixRaftStreamPurgeInstallKeepsFloorSmall 2"
  "FelixRaftStreamPurgeReachRewrittenInstall 15"
  "FelixRaftStreamPurgeNoCapSmallRefuse 12"
  "FelixRaftCond 300"
  "FelixRaftCondTwoTerms 40"
  "FelixRaftCondLeaderDecidesNoMembers 200"
  "FelixRaftCondBatch 20"
  "FelixRaftIdempotent 20"
  "FelixRaftReads 8"
  "FelixRaftReadsWritten 1"
  "FelixRaftSessionsSubscriber 25"
  "FelixRaftSessionsGroup 2"
  "FelixRaftLeaseAck 2"
  "FelixRaftLeaseDeliver 6"
  "FelixRaftLeaseRestart 6"
  "FelixRaftLeaseRestartRenews 5"
  "FelixRaftLeaseRestoreLed 12"
  "FelixRaftLeaseRestoreNamed 38"
  "FelixRaftLeaseRestoreStepDown 6"
  "FelixRaftLeaseRestoreTransferStepDown 11"
  "FelixRaftLeaseRestoreTransferThreeTerms 7"
  "FelixRaftLeaseRestoreTransferStepDownWrite 40"
  "FelixRaftLeaseReachRestoreTransferThreeTermsWrite 9"
  "FelixRaftLeaseStore 2"
  "FelixRaftLeaseStoreStall 2"
  "FelixRaftLeaseStoreWide 36"
  "FelixRaftLeaseStepDown 8"
  "FelixRaftLeaseStepDownDrift 5"
  "FelixRaftLeaseStepDownNoVoid 2"
  "FelixRaftLeaseStepDownWrite 8"
  "FelixRaftLeaseSuspendLateCheck 2"
  "FelixRaftLeaseSuspendDrift 22"
  "FelixRaftLeaseSuspendRoleLate 28"
  "FelixRaftLeaseSuspendUnchecked 2"
  "FelixRaftLeaseTransfer 10"
  "FelixRaftLeaseTransferDrift 35"
  "FelixRaftLeaseTransferKeepsLease 2"
  "FelixRaftLeaseTransferStepDownNoVoid 2"
  "FelixRaftLeaseTransferThreeTerms 12"
  "FelixRaftLeaseTransferVoidEquality 4"
  "FelixRaftLeaseTransferVoidLate 2"
  "FelixRaftLeaseTransferStepDown 38"
  "FelixRaftLeaseTransferWrite 40"
  "FelixRaftLeaseSleep 93"
  "FelixRaftCache 4"
  "FelixRaftCacheClock 9"
  "FelixRaftCacheClockSnapshot 18"
  "FelixRaftCacheExpiry 2"
  "FelixRaftCacheDelete 9"
  "FelixRaftCacheCond 3"
  "FelixRaftCacheDeleteSnapshot 50"
  "FelixRaftCachePutAbsentSnapshot 45"
  "FelixRaftCacheCondSnapshot 33"
  "FelixRaftCacheTrim 105"
  "FelixRaftSleep 2"
  "FelixRaftSleepEnds 2"
  "FelixRaftSleepMidHandshake 87"
  "FelixRaftSleepWake 9"
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
  "FelixRaftPlacementFormAfterWipes 3"
  "FelixRaftPlacementFormLogWipeFirstBoot 4"
  "FelixRaftPlacementReseatWrites 8"
  "FelixRaftPlacementReturnWipe 3"
  "FelixRaftPlacementSameId 5"
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
  "FelixShardMoveGiveUpFailoverDrop 2"
  "FelixShardSessionsSubscriber 3"
  "FelixRaftCache 6"
  "FelixRaftCacheClock 13"
  "FelixRaftCacheClockSnapshot 26"
  "FelixRaftCacheDelete 13"
  "FelixRaftCacheDeleteSnapshot 72"
  "FelixRaftCachePutAbsentSnapshot 65"
  "FelixRaftCacheCondSnapshot 47"
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
  raft_parts="Membership Identity Placement Reform Lease Sleep Switch Cache Cond"
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
# does not pull in its heavy configurations, except FelixRaft.tla and the track 4 modules beside it are the exceptions:
# a change to one counts as a change to each of its configurations (see
# `changed_cfgs`). Pull requests check that tier; the rest run when the spec
# changes on main. Configurations named on the command line always run.
if [ -n "${TLA_QUICK:-}" ] && [ "$#" -eq 0 ] && [ -z "${affected_walks:-}" ]; then
  quick=()
  for entry in ${selected[@]+"${selected[@]}"}; do
    weight="$(weight_of "${entry%% *}")"
    if [ "$weight" -le 10 ]; then
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
