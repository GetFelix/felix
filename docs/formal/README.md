# A formal model of one shard

> **Retired.** The hybrid engine's models are deleted: `FelixShard.tla`'s
> configurations, `FelixShardUnified`, `FelixShardFigure8` and the long walks
> over them. The v0.7.0 tag holds them. `FelixShard.tla` itself stays only as
> the base that `FelixShardReads.tla` and `FelixShardSessions.tla` extend until
> their Raft twins (`FelixRaftReads`, `FelixRaftSessions`) replace them.
> `FelixShardMoveGiveUp` stays until its liveness check has a Raft twin. Where
> the text below names a `FelixShard*` configuration that is not in this
> directory, it was deleted with the hybrid and the text is history. See
> [hybrid-retirement.md](../hybrid-retirement.md).

`FelixShard.tla` is a TLA+ model of the protocol in
[`docs/replication-design.md`](../replication-design.md): the lease that lets a
broker serve a shard, the replication that puts its records on a majority, and
the promotion that names the next leader when the lease lapses. `task tla:check`
runs TLC over every configuration, locally and in CI: about nine and a half
minutes on sixteen cores, and about half an hour on a four-core CI runner.

Prose about a safety interval is an argument; a model checker either finds the
interleaving that breaks it or runs out of interleavings to try. This one found
something the prose had not: an acknowledged `Quorum` record lost when the
leader died before its last position report. The broker was fixed (#527), and
the configuration that removes the fix is kept as the proof that it is needed;
see [The ordering that is load-bearing](#the-ordering-that-is-load-bearing).

## What is modelled

One shard, three brokers, one control plane, discrete time.

- **Clocks.** `now` is real time. Each broker's clock is monotonic and within
  `Drift` of real time, which is the design's drift-rate assumption in the only
  shape a finite model needs. On each tick a broker's clock moves by zero, one
  or two: standing still is slow, two is fast.
- **Leases.** The leader heartbeats; the control plane accepts a heartbeat only
  while the lease it granted has not lapsed, and extends it by `L`. The broker
  extends its own belief from the instant it *sent* the heartbeat, on its own
  clock, and stops serving `Eps` before that belief expires. Heartbeats can be
  lost. The control plane grants the next generation no earlier than `Margin`
  after the expiry it recorded. Deregistering a node has no action of its
  own: from then on the control plane renews nothing for it, which is every
  later heartbeat lost, and placement still waits out the timeout and margin
  from the node's last heartbeat before promoting (`left_within_lease`).
  The control plane renews only a broker it names leader at the generation
  the broker believes it leads. The code's lease is per broker, so it holds
  a broker marked down until its routes show what changed while it was away;
  `FelixShardLeaseAnyHeartbeat.cfg` checks the code's old rule in place of
  this one. `FelixShardLeaseRoutesHeartbeat.cfg` checks the new one, assuming
  what its head check gives: the routes it waits for show the move.
- **Writes.** Admission checks the broker is serving. The write then waits,
  and claims its place in the log; with `FenceAtClaim` the claim checks the
  handoff fence again. `AckOnAdmit` acknowledges a `Leader` write when it is
  admitted rather than when it commits, and with `FenceFromAdmit` a write
  holds the fence from admission, as the broker's routing now has every local
  write do. Commit checks the lease again, or not, which is the
  `CheckAtCommit` knob. Anything may happen between admission and the claim,
  and between the claim and the commit: those gaps are a queue and a paused
  process.
- **Replication.** The leader ships the next record a follower is missing. A
  follower whose log disagrees with the leader's keeps what a newer generation
  than its own last accepted one says, above its high-water mark; anything else
  halts it. A follower refuses a leader older than one it has heard from.
  The model reads "heard from" off the follower's log (`LastGen`); the code
  keeps the highest generation it accepted in a file beside the log, which is
  never below that and survives a restart, so it only refuses more. Under
  `AckByFollowers` the model keeps that generation too (`promised`). The
  follower's high-water mark is learned from the leader, as the code's
  follower learns the commit offset each batch carries.
- **Acknowledgement.** Under `Quorum`, once a majority including the leader
  holds the record; under `Leader`, on the leader's own commit. With
  `AckByFollowers`, once a majority holds it and still holds the leader's
  generation as the highest it accepted (`promised`), with no report and no
  lease in the condition. Only a record of the leader's own generation is
  counted, taking the ones below it along, as in Raft.
- **Promotion fence**, under `FenceOnPromote`. A promoted leader persists its
  generation, then asks the others to: each that has not accepted a newer one
  persists it, answers with its log, and from then on refuses the older
  leader. The new leader takes the log of any answer ahead of its own by (last
  generation, length), and opens for writes, and ships, only once a majority,
  itself included, has answered. Modelled on promotion only: the handoff and
  cancel configurations do not turn it on. The broker has it without
  `AckByFollowers`, acknowledging on the report and the lease as before;
  `FelixShardFencedPromotion.cfg` is that configuration.
- **Reports.** The leader tells the control plane which followers hold every
  record it does. The report travels on its own: it may arrive after the
  acknowledgements it describes, or never. With `ReportBeforeAck` a `Quorum`
  ack counts only a report the control plane stored for the acknowledging
  broker's own generation: the broker learns that from the per-shard answer
  (`accepted`), not from the request's status, so a deposed leader cannot
  release an ack on a report that belongs to its successor.
- **Promotion.** After the lapse and the margin, the control plane names a new
  leader. `Promotion = "leader-report"` is the design as written: a follower
  the last report named as caught up. `Promotion = "log-order"` is the live
  replica with the greatest (last generation, length): Raft's election
  restriction, which needs no report.
- **Planned handoff**, under `Handoff`. The control plane may move the shard
  while its leader is alive: it fences the leader, which stops serving when
  it sees the fence but keeps its lease and keeps shipping, and names the
  successor only once the leader has reported that its log stopped growing.
  A write claimed before the fence still commits. `WaitForDrained` is that
  wait. The report counts claimed writes only, as the broker's write fence
  does. Reports carry the generation they were made at and one from a
  superseded generation is dropped on arrival, as the store does.
  The consumer-group dead-letter and cursor logs that ride a stream shard
  are written through the same fence as its records, and here a write to
  either is a write to the one log. That says nothing about how the two
  relate to each other, and they are not one log: they flush, ship and are
  replaced separately. `FelixGroups.tla` models them as two (see
  [Group state: two logs per replica](#group-state-two-logs-per-replica)).
  A cache's counter log is modelled on its own, under `Counters`.
- **Re-sends**, under `Resends`. A client may send a write it has no answer
  for again, as an idempotent producer does after a lost acknowledgement or a
  leader change. The serving broker appends it unless it already knows the
  write, and `SequencesInLog` says where it looks: its log, as the broker does
  (sequences are stored with the records and derived from them), or only what
  it wrote itself under the generation it leads, which is sequences kept in a
  leader's memory. Acknowledgement is per write, so these configurations run
  one write: with two, a deposed leader's stale first copy reads as a second
  acknowledged record at its offset until it is truncated.
- **Planners.** The control plane decides from a read of the store, not from
  its live state. A decision (promote, fence, cut over) either reads and writes
  in one step, or comes from a read one of `Planners` took earlier (`cpView`:
  the assignment, the last report, whether the lease had lapsed) and still
  holds, one write per read. Every assignment write bumps `ver`, the store's
  generation. With `CasWrites` a write lands only if `ver` is still what its
  read saw, which is `put_shard_assignment_if`. `Planners = {}` is a single
  instance whose reads are never stale; the `StalePlanner` and
  `StalePromotion` configurations hold one read across the other instance's
  writes, as two control-plane instances over one database do.
- **Cancel.** With `Cancel`, an operator's cancel of a fenced move is one more
  planner decision: the leader that was fenced serves again at a new
  generation, keeping the writes it has queued and claimed, since they are
  inside its fence and land in its own log. `CancelCas` makes that write
  conditional like the others.
- **Elections and ballots.** With `Elections`, a replica stands for leader
  with no control-plane step: `Elect` takes the generation after the one it
  promised, persists its own promise, fences the set it last knew and opens
  like a promoted leader, up to `Elections` generations past the control
  plane's. Two replicas can then pick one generation, which the control plane
  never does. With `Ballots` each promise names the leader it was made to
  (`ballot`), and a replica answers a fence, a batch or a read round at the
  generation it promised only from that leader. The first leader starts with
  its set's promise, as an elected one would. The broker's election driver
  (`felix-replication`, `election/candidate.rs`) is what elects.
- **The set check and adoption.** The design for electing without the
  control plane (`docs/replication-design.md`, "Electing without the control
  plane"), built behind `replica_election`. With `SetCheck` each replica keeps the set it last
  learned and the generation of the assignment that wrote it (`setGen`). A
  batch and a fence carry the sender's pair; a replica that knows a newer one
  refuses, and otherwise learns it. A refused candidate learns the newer set
  and stops standing (`RefuseStaleSet`); it may stand again at a later
  generation, so each generation is fenced on one set. A leader or candidate
  that takes a fence or a batch at a newer generation stops leading. Only a
  member of its own set stands. A set change (`Reseat`, `GrowSet`, `Seat`,
  `TrimSet`, and `Swap`, a cut-over that changes several members at once, as
  `replicas_after_cut_over` can) fences before it opens, on the new set, and
  with `JointFence` on a majority of the old set too. A copy outside the set
  joins it only through such a change. With `Adopt` the control plane takes a
  leader that opened above the generation it read as its own, a conditional
  write like any other, when the leader's set is the one it records
  (`AdoptMatch`), and otherwise only moves its generation past it. `Adopt`
  also bounds the generations replicas stand for. `PreVote` lets a replica
  stand only once a majority of its set no longer hears a leader, and has a
  replica that still hears one refuse a candidate.
- **Every change fenced.** With `FenceEveryChange`, a cut-over, a cancel's
  hand-back and a failover that names a move's destination fence and catch
  up before they serve, as a promotion does, instead of opening at once.
  The broker does this for every leadership it takes once every replica
  offers the fence. `FenceEveryChange = FALSE` is the broker before it did,
  which is safe while only the control plane picks generations.

Not modelled: readers. The code bounds every reader of a `Quorum` shard by
the quorum mark and refuses reads on a lapsed lease, so what a reader sees is
a prefix of what `AckQuorum` allows; nothing a reader does feeds back into
the protocol. Counter adds on a `Quorum` cache now wait for a counter mark,
matching the model's treatment of the logs riding a shard as one log.

Also not modelled: the storage layer (a commit is a commit), network partitions as
such (they are lost heartbeats, lost reports, and delays), retention, and the
bootstrap of a follower below the leader's base. Leaving them out relies on
retention never deleting at or above a `Quorum` shard's commit offset, so a
follower bootstrapped or rebuilt at the leader's base is missing only records
a majority already held (see "Retention and the quorum mark" in
`docs/replication-design.md`).

Not modelled either: the generation the control plane writes at the flip. In
the code, taking a pending set is an ordinary assignment write, a new
generation naming the new leader, which the leader then opens on the new set
alone; `CpFlip` keeps `gen`. The control plane's tests cover the write
(`pending_set.rs`), but TLC does not check the bump, so the model does not
show that a leader opening that generation is fenced like any other. To be
modelled before the election driver.

Nor is a replication factor changed during a move. One raised while a restore
is seat-fenced leaves the set short, and `seat_after_fence` then seats the
newcomer as a top-up instead of undoing it; that case is a unit test only.

## What is checked

| Invariant | Says |
| --- | --- |
| `AtMostOneServing` | No two brokers serve the shard at once. |
| `AckedSurvive` | Whoever is serving holds every acknowledged record. |
| `AckedHeldByLeader` | The leader at the newest generation holds every acknowledged record once it may serve. `AckedSurvive` without the lease, for configurations where two brokers can serve at once. Where replicas elect or placement promotes on suspicion it is checked per generation instead: every leader that opened holds every record acknowledged at its generation or below. |
| `AckedAgree` | Two brokers never hold different acknowledged records at one offset. |
| `AckedOnMajority` | Every acknowledged `Quorum` record is on a majority. A set of four, which a promoted replacement leads beside the three it joined, needs two, since every fence there takes three. |
| `NoTruncationBelowHwm` | A follower never discards a record below its high-water mark. |
| `NoStaleCommit` | No broker commits at a generation the control plane has superseded. |
| `StagedCopyNeverDelaysAck` | A `Quorum` write the stream's own replicas would acknowledge is never held back by a destination's copy. A latency property, checked only where a destination is staged. |
| `NoDuplicate` | No log holds one write twice. Checked where writes are re-sent. |
| `OneLeaderPerGeneration` | No two brokers open for writes at one generation. Checked where replicas elect themselves and where placement promotes on suspicion. |
| `AdoptedMatchesOpened` | The control plane's leader, once open at the control plane's generation and while it still holds it, leads the set the control plane records, an adopted leader included: the pending set while a cut-over's change is pending. Checked where the control plane adopts. |
| `SetChangesJoint` | Once a set change opened, a majority of the new set and of the set before it know the new set. A history invariant over consecutive sets, checked under `SetCheck`. |
| `CountersOnSetMajority` | Under `Counters`, every acknowledged counter update is on a majority of the set the control plane records, as `AckedOnMajority` for the cache log: the old set while a change is pending, the new set after the flip. |
| `QuorumReportNamesASuccessor` | Under `Quorum`, a report from a leader still serving names a follower that may take over, whenever a majority is still replicating. A liveness property in invariant form, checked in `FelixShard.cfg`. |
| `ElectionSettles` | In `FelixRaftElect.tla`: once a majority of the voters stays up and connected, a leader among them is elected that none of them has outvoted, and it stays. A temporal property, checked under `LiveSpec` in `FelixRaftElectLive.cfg`, `FelixRaftElectLiveFaults.cfg` and their `Slack` variants. The hybrid's version, with the control plane down, was checked in `FelixShardElectLive.cfg`, deleted with the hybrid. See [Raft election liveness](#raft-election-liveness). |
| `EventuallyServes` (Raft) | In `FelixRaftElect.tla`: the same leader's blank entry is committed, so the shard is open, and every member of that majority follows it. Checked with `ElectionSettles`. |
| `NoStandAgainstHeardLeader` | Nobody starts fencing against an open leader that a majority of its set still hears. An action property, checked with the liveness configurations. |
| `OwnBallotCaughtUp` | The control plane's leader does not stay open at its generation while its own copy holds its own ballot above it. While it does, it never counts its own copy, so with one follower down nothing it writes is acknowledged (#1933). A temporal property, checked under `RenameSpec` in the `FelixShardElectRouteLag*.cfg` configurations. |
| `OwnBallotFenceCaughtUp` | Nor does it stay fencing at its generation while its own copy holds its own ballot above it. The code's open takes the generation's ballot on its own copy, which refuses it, so it never opens there (#1940). Checked under `RenameSpec` in the `FelixShardElectRouteLagFence*.cfg` configurations. |
| `CollisionSuperseded` | With the control plane up, it does not stay at a generation whose leader can never open, because another broker was elected there and opened, while that broker still leads. A temporal property, checked under `ReissueSpec` in `FelixShardElectLiveReissue.cfg`. |
| `CpLeaderServes` | With the control plane up and every stopped broker back, the leader it names ends up open at its generation, serving with a mark over its whole log and a majority of its set taking its batches, and stays (`<>[](LeaderServes /\ ServesWith(leader))`). Checked under `CpLiveSpec` in `FelixShardCpLive.cfg` and its twins. |
| `CpDownServesInBound` | With the control plane stopped for good, some live leader ends up open and serving on a majority that holds its generation for it, and stays (`CpDownServes`), or elections stop only at the model's bound with a broker that would still stand (`Exhausted`). Checked under `CpDownSpec` in `FelixShardCpDown*.cfg`. |
| `CpDownFullyBacked` | The same, with every live member of the leader's set behind it. Not owed: `FelixShardCpDownDegraded.cfg` is its witness. |

## The configurations, and what each must do

`scripts/check_tla.sh` holds each configuration to a declared outcome. A
configuration that must find a violation exists to show a check is
load-bearing, or to pin a finding the design has not acted on yet; a violation
that quietly became a pass would be a model that stopped saying anything.

A configuration too long for any CI job is listed in
`scripts/tla_unregistered.txt` with the reason, and `task docs:evidence` runs
`scripts/check_tla_registry.sh`, which fails on a `.cfg` that is neither
registered, run by a workflow, nor listed there.

| Configuration | Knobs | Must |
| --- | --- | --- |
| `FelixShardElectAdoptFailover{Same,Apart}{0,1}{,Stood}Gen{2,3,4}.cfg`, `FelixShardElectAdoptFailoverSame{0,1}Stood{One,Several}{Dest,Other}Gen2.cfg` | `FelixShardElectAdoptFailover.cfg` split at its first failover (`SplitAtFailover`, `CutOverPiece`): the promoted broker's own set is the control plane's or not, 0 or 1 writes were admitted before it, a replica had stood past the control plane's generation or not, and the failover takes generation 2, 3, or 4 and up (`PieceGens`). Each piece also checks everything before its failover, so together they reach every state the whole does | pass, the same invariants. Twelve pieces hold no failover: they hold just the 1.48M distinct states before one (depth 36, 3 to 5 min on a four-core CI runner) and check that they stay empty (`NoCutOverInPiece`). `Apart0Gen4` 13.5M distinct, 46 min; `Apart1Gen4` 15.5M, 54 min; `Same0Gen4` 36.3M, 2 h 15 min; these run nightly. `Same1Gen4` 56.8M, 3 h 40 min, and `Same1Gen2` 149.3M, depth 51, 5 h 20 min (close to the six-hour job limit), also run nightly. Five did not finish in six hours, with no violation by then: `Same0Gen2` (130M distinct), `Same0Gen3` (224M), `Same1Gen3` (111M), `Apart0Gen3` (108M), `Apart1Gen3` (156M). They need another split and are not checked yet. `Same0StoodGen2` ran out of runner disk and `Same1StoodGen2` did not finish, so each is split again into `Stood{One,Several}{Dest,Other}` (`PieceStoodCounts`, `PieceDestStood`: one or several replicas stood, the promoted broker among them or not). Those eight are not measured yet |
| `FelixShardElectSwap{Keep,Drop}{0,1,2}{,Stood}.cfg` | `FelixShardElectSwap.cfg` split by how its cut-over went: the new set keeps or drops the old leader, 0, 1 or 2 writes were admitted before it, and a replica had stood past the control plane's generation or not (`SplitAtCutOver`, `CutOverPiece`). `Drop0Stood` did not finish in six hours and `Drop1Stood` took 5 h 15 min, so each is split again by whether one or several replicas had promised past that generation and whether the destination was one of them (`FelixShardElectSwapDrop{0,1}Stood{One,Several}{Dest,Other}.cfg`). Each piece also checks everything before its cut-over, so together they reach every state the whole does. `QuietView` merges states that differ only in what no step reads again | pass, the same invariants. On a four-core CI runner: `Keep2` 2.46M distinct, 5 min; `Drop2` 1.45M, 5 min; `Drop1` 7.17M, 17 min; `Keep1` 9.23M, 27 min; `Drop0` 17.9M, 43 min; `Keep0` 17.7M, 53 min; `Keep2Stood` 47.9M, 1 h 58 min; `Drop2Stood` 35.5M, 2 h 9 min; `Keep1Stood` 97.0M, 2 h 49 min; `Keep0Stood` 97.1M, 2 h 50 min; `Drop0StoodSeveralOther` 5,189, 3 s; `Drop0StoodOneOther` 4.24M, 13 min; `Drop0StoodOneDest` 17.9M, 39 min; `Drop0StoodSeveralDest` 84.0M, 2 h 37 min; `Drop1StoodSeveralOther` 5,189, 2 s; `Drop1StoodOneOther` 1.91M, 3 min; `Drop1StoodOneDest` 7.17M, 12 min. Those over 45 minutes run nightly. The two `StoodSeveralOther` pieces hold no cut-over: the old leader must still lead when it cuts over, and it stops leading once it answers a fence, so besides the destination only the third member can have stood. They hold just the states before a cut-over and check that they stay empty (`NoCutOverInPiece`) |
| `FelixShardElectRename{Keep,Drop}{0,1}{,Stood{One,Several}{Dest,Other}}.cfg` | `FelixShardElectRename.cfg` split at its cut-over as `FelixShardElectSwap.cfg` is (`SplitAtCutOver`, `CutOverPiece`). Each piece also checks everything before its cut-over, so together they reach every state the whole does. The `StoodOneDest` pieces run nightly: the cut-over overwrites everything the destination's stand wrote, so they should reach the same states as the pieces where nobody stood (argued, not measured) | pass, the same invariants. `{Keep,Drop}{0,1}StoodSeveralOther` 37,646 distinct, 2 s on a laptop. On a four-core CI runner the others take 2 to 36 min (1.1M to 25.1M distinct, depth 37 to 40), except `Keep0StoodSeveralDest` (38 min to 1 h 16 min), `Keep1StoodSeveralDest` (51 min) and `Drop0StoodSeveralDest` (1 h 21 min), which run nightly with `Drop0StoodOneOther` and the `StoodOneDest` pieces. The four `StoodSeveralOther` pieces hold no cut-over, as in `FelixShardElectSwap.cfg`, and check that they stay empty (`NoCutOverInPiece`) |
| `FelixShardElectRename{Keep,Drop}1StoodOneOtherReportStore.cfg` | `FelixShardElectRename{Keep,Drop}1StoodOneOther.cfg` with the rename read from the report the control plane stored (`ReportStore`, below), which can outlast the state that caused the refusal | pass, the same invariants (`Keep` 965K distinct states, `Drop` 846K, depth 38, under a minute on a laptop) |
| `FelixShardElectSwapFlip{Keep,Drop}{0,1}{,Stood{One,Several}{Dest,Other}}.cfg` | `FelixShardElectSwapFlip.cfg` split at its cut-over as `FelixShardElectSwap.cfg` is: the new set keeps or drops the old leader, 0 or 1 writes were admitted before it, and no replica, one or several had promised past the control plane's generation, the destination among them or not. Each piece also checks everything before its cut-over, so together they reach every state the whole does | pass, the same invariants. On a four-core CI runner: `Keep1` 10.2M distinct, 14 min; `Drop1` 6.70M, 20 min; `Keep1StoodOneOther` 5.81M, 11 min; `Drop1StoodOneOther` 3.53M, 7 min; `Drop0StoodOneOther` 17.7M, 41 min; `Keep0StoodOneOther` 20.2M, 54 min; `Keep0` 35.4M, 1 h 9 min; `Drop0` 32.4M, 1 h 19 min; `Drop1StoodSeveralDest` 43.5M, 2 h 10 min; `Keep0StoodSeveralDest` 97.4M, 2 h 18 min; `Drop0StoodSeveralDest` and `Keep1StoodSeveralDest` over 2 h; the four `StoodSeveralOther` 694, under a second. Each `StoodOneDest` reaches the same states as its unstood twin, as the cut-over overwrites everything the destination's stand wrote. Those over 45 minutes and the four `StoodOneDest` run nightly. The four `StoodSeveralOther` pieces hold no cut-over, as in `FelixShardElectSwap.cfg`, and check that they stay empty (`NoCutOverInPiece`) |
| `FelixShardFencedAckMove.cfg`, `FelixShardFencedAckStagedMove.cfg`, `FelixShardFencedAckMoveCancel.cfg` | the three above with `L = 2` and time to 2 | pass, the same invariants: 66.0M distinct states, depth 44, 58 min, and 54.7M, depth 44, 31 min, on a CI runner; the cancel had not finished at 54M after 25 min. Nightly |
| `FelixShardReadsRound.cfg` | `FelixShardReads.tla`: `FelixShardFencedAck.cfg`'s writes with one read, confirmed by a round of fences at the leader's generation after it takes its value (`ReadConfirm = "round"`); `L = 2`, time to 2, one write | pass `NoStaleRead`, `AckedHeldByLeader`, `AckedOnMajority` (22.1M distinct states, depth 27, 6 min on four workers) |
| `FelixShardReadsNoRound.cfg` | the same with the round skipped | violate `NoStaleRead` |
| `FelixShardReadsLease.cfg` | the same with the lease in place of the round | violate `NoStaleRead` |
| `FelixShardSessionsSubscriber.cfg` | `FelixShardSessions.tla`: `FelixShardReadsRound.cfg`'s writes and clocks with a subscriber that reads up to the committed mark of any broker that believes it leads, and resumes at its next offset when it moves; no lease on its path | pass `NoLostDelivery` (1.46M distinct states, depth 25, 58 s) |
| `FelixShardSessionsPastMark.cfg` | the same with the subscriber reading to the end of the broker's log | violate `NoLostDelivery` (18.0K distinct states, depth 10) |
| `FelixShardSessionsGroupRound.cfg` | the same writes with one group commit, written to the coordinator's log and confirmed by a round of fences at its generation before it is acknowledged | pass `NoStaleGroupCommit` (13.9M distinct states, depth 27, 14 min) |
| `FelixShardSessionsGroupNoRound.cfg` | the same with the round skipped | violate `NoStaleGroupCommit` (5.7K distinct states, depth 8) |
| `FelixShardSessionsGroupLease.cfg` | the same with the lease in place of the round | violate `NoStaleGroupCommit` (6.4K distinct states, depth 8) |
| `FelixPlacementPacing.cfg` | `FelixPlacementPacing.tla`: moves and follower replacements across four shards, two copies at once, one per node, one planner | pass `CopiesWithinLimit` and `FencedOnlyInAMove` |
| `FelixPlacementPacingUncountedReplacement.cfg` | the same with a follower replacement invisible to the count, as it used to be written | violate `CopiesWithinLimit` |
| `FelixPlacementPacingTwoPlanners.cfg` | two planners over three shards and one slot, the lease changing hands at any step, one also reading without it as an operator does; every start fenced by the placement token | pass `CopiesWithinLimit` and `FencedOnlyInAMove` |
| `FelixPlacementPacingUnfenced.cfg` | the same with starts conditional only on their shard's generation | violate `CopiesWithinLimit` |
| `FelixAtomicCommit.cfg` | `FelixAtomicCommit.tla`: two atomic commits, each one record, applied whole by three views on every broker, across two promotions | pass `NoPartialCommit` and `CommitSurvives` (21.8K distinct states) |
| `FelixAtomicCommitSplitRecords.cfg` | the same with each part written as its own record, the mark stopping only at a commit's end | violate `NoPartialCommit` |
| `FelixAtomicCommitPartialApply.cfg` | the same with the views applying a record one part at a time | violate `NoPartialCommit` |
| `FelixHeadTrim.cfg` | `FelixHeadTrim.tla`: a conditional head trim as a replicated record, two lease holders, two writes and three trims or no-ops across one promotion, the deposed leader still taking writes and trims, followers rebuilt at the leader's base, a follower read after an acknowledged trim served once the follower reaches the leader's read index | pass `NoReadBelowAckedTrim`, `NothingLostAboveFloor`, `FloorRecordKept`, `NeverMoreThanAsked` and `StaleTrimRefused`. Not registered, run by dispatch: run 43870273108 in 30 shards passed 29, and shard 0 hit the six hour limit at depth 34 (1.75B states, 244M distinct) with no violation |
| `FelixHeadTrimCrash.cfg` | the same with one broker crash, its commit offset read back behind, and two trims or no-ops | pass the same |
| `FelixHeadTrimTwoPromotions.cfg` | the same across two promotions, with one trim or no-op | pass the same |
| `FelixHeadTrimCrashTwoPromotions.cfg` | one write, one trim or no-op, a crash and two promotions | pass the same |
| `FelixHeadTrimLocal.cfg` | the floor kept as the leader's own state, not a record | violate `NoReadBelowAckedTrim` |
| `FelixHeadTrimFloorFromBase.cfg` | a broker's readable start taken from where its segments start | violate `NoReadBelowAckedTrim` |
| `FelixHeadTrimApplyOnStore.cfg` | the floor applied when the record is stored, before it commits | violate `NothingLostAboveFloor` |
| `FelixHeadTrimDeleteAboveCommit.cfg` | deletion following the floor of every stored trim, committed or not | violate `NothingLostAboveFloor` |
| `FelixHeadTrimUnconditional.cfg` | the trim appended at whatever the tail is | violate `StaleTrimRefused` |
| `FelixHeadTrimHiddenTrims.cfg` | `CursorTooOld` without the floor-setting trim's tag, as first designed | violate `StaleTrimRefused` |
| `FelixHeadTrimNoOpUnchecked.cfg` | a no-op trim answered with the tail without checking it | violate `StaleTrimRefused` |
| `FelixHeadTrimRetention.cfg` | retention deleting past the floor, up to the commit offset | violate `StaleTrimRefused` |
| `FelixHeadTrimFollowerNoReadIndex.cfg` | a follower serving a read that could start below a floor without first applying the leader's read index | violate `NoReadBelowAckedTrim` |
| `FelixHeadTrimLeaderNoReadIndex.cfg` | a leader serving such a read without its own read-index round, so a deposed leader that never heard its successor serves below the successor's trim | violate `NoReadBelowAckedTrim` |
| `FelixHeadTrimTailNoReadIndex.cfg` | a follower behind a trim starting a tail read at its own committed tail without the leader's read index | violate `NoReadBelowAckedTrim` |
| `FelixGroups.cfg` | `FelixGroups.tla`: two groups' dead-letter and cursor logs on three replicas, flushed separately, across a crash and two promotions; each cursor checked against the dead letters it needs before a shard is served | pass `NoSkip` (66.3M distinct states, 11 min on a CI runner) |
| `FelixGroupsNoCheck.cfg` | the same with cursors served unchecked | violate `NoSkip` |
| `FelixGroupsLengthOnly.cfg` | the same with the check comparing the dead-letter log's length only | violate `NoSkip` |
| `FelixGroupsDropGroup.cfg` | the same with a group that has nothing standing dropped, and recreated at the tail | violate `NoSkip` |
| `FelixGroupsSplitSources.cfg` | the same with a promotion taking dead letters and cursors from different replicas, cursors replacing its own | pass `NoSkip` (60.5M distinct states, 17 min on a CI runner) |
| `FelixGroupsSplitSourcesLengthOnly.cfg` | the same with the check comparing lengths only | violate `NoSkip` |
| `FelixGroupsShipUnsynced.cfg` | `FelixGroups.cfg` with dead letters shipped before they are synced | violate `NoSkip` |
| `FelixShardMoveGiveUp.cfg` | `FelixShardMoveGiveUp.tla`: a fenced move whose leader crashes uncleanly before it reports drained; the give-up at the move's generation ends the move, and the leader leads again only if its probe found no follower ahead | pass `OneLeaderPerGeneration`, `AckedHeldByLeader`, `AtMostOneServing` and `EventuallyServes` (97k distinct states, seconds) |
| `FelixShardMoveGiveUpStall.cfg` | the same with the move waiting for a drained report or the fence timeout, whose clock restarts with the control plane | violate `EventuallyServes` |
| `FelixShardMoveGiveUpLeads.cfg` | the same with a broker that leads its given-up copy without the probe | violate `AckedHeldByLeader` |
| `FelixShardMoveGiveUpKeep.cfg` | replication factor 2 with the destination staged as a third copy, two writes; every move given up keeps the destination, and a trim fence drops a copy later | pass `OneLeaderPerGeneration`, `AckedHeldByLeader`, `AtMostOneServing` (29.5M distinct states, 7 min locally) |
| `FelixShardMoveGiveUpKeepLive.cfg` | the same with one write and the crash during the move, for liveness | pass the invariants, `EventuallyServes` and `EventuallyAtFactor` |
| `FelixShardMoveGiveUpDrop.cfg` | the same with the fence timeout's retake dropping the destination | violate `AckedHeldByLeader` |
| `FelixShardMoveGiveUpUnstage.cfg` | the same with a staged move given up before its fence dropping the destination | violate `AckedHeldByLeader` |
| `FelixShardMoveGiveUpTwoDrop.cfg`, `FelixShardMoveGiveUpTwoKeep.cfg` | replication factor 1 with the destination staged as a second copy, dropped or kept | both violate `AckedHeldByLeader`: one copy is lost to an unclean crash either way, so a set of two drops the destination |
| `FelixShardMoveGiveUpFourDrop.cfg` | replication factor 3 with the destination staged as a fourth copy, every exit dropping it | pass the invariants: a follower that stays holds every acknowledged record (20.6M distinct states with symmetry, 10 min) |
| `FelixShardMoveGiveUpFailoverDrop.cfg` | a set of three whose leader is taken for dead, the failover dropping a follower | pass the invariants: the drop loses no record, it stalls |
| `FelixShardMoveGiveUpFailoverStall.cfg` | the same for liveness | violate `EventuallyServes`: the promoted follower's fence needs the dead leader's given-up copy |
| `FelixShardMoveGiveUpFailoverLive.cfg` | the failover keeping the destination in a set of three | pass the invariants and `EventuallyServes` (2.9M distinct states, 4 min) |
| `FelixRaft.cfg` | `FelixRaft.tla`: one Raft group on the segment log with the `raft_log` adapter's rules, three brokers, two terms, one write, one crash, the subscriber reading | pass every check in [One engine: Raft on the segment log](#one-engine-raft-on-the-segment-log) (1.2M distinct states) |
| `FelixRaftCrashes.cfg` | the same with two crashes and no subscriber | pass the same (1.6M distinct states) |
| `FelixRaftTerms.cfg` | three terms, no write, no crash, no subscriber | pass the same (1.1M distinct states) |
| `FelixRaftFourTerms.cfg` | four terms, no write, no crash, no subscriber | pass the same (40.9M distinct states) |
| `FelixRaftTermsFull.cfg` | three terms with one write | pass the same (26.0M distinct states, 59 min) |
| `FelixRaftSnapshot.cfg` | one snapshot, a purge up to it and a follower brought up by snapshot in three writes, one crash | pass the same (5.5M distinct states) |
| `FelixRaftSnapshotReplace.cfg` | two snapshots, no crash: a broker replaces a snapshot it stored | pass the same (5.8M distinct states) |
| `FelixRaftSnapshotCrashes.cfg` | `FelixRaftSnapshot.cfg` with two crashes | pass the same (9.4M distinct states, 21 min) |
| `FelixRaftOffsets.cfg` | one term, two writes and a membership entry, the subscriber reading, no crash | pass the same (0.4M distinct states) |
| `FelixRaftOffsetsFailover.cfg` | two terms, two writes, the subscriber reading across a change of leader, no crash | pass the same (5.7M distinct states) |
| `FelixRaftOffsetsFailoverCrash.cfg` | the same with one crash | pass the same (16.1M distinct states, 41 min) |
| `FelixRaftOffsetsFull.cfg` | two terms, one write and a membership entry, the subscriber reading | pass the same (8.9M distinct states, 20 min) |
| `FelixRaftAckBeforeFlush.cfg` | the flush callback fires before the fsync | violate `AckedSurvive` |
| `FelixRaftRecoveredTailUnsynced.cfg` | entries found at the open are reported flushed without an fsync | violate `AckedSurvive` |
| `FelixRaftVoteNotDurable.cfg` | `save_vote` returns before the vote is on disk | violate `OneLeaderPerTerm` |
| `FelixRaftLeaderResumes.cfg` | a restarted leader leads again in its stored term | violate `LogMatching` |
| `FelixRaftTornTailKept.cfg` | recovery reads a torn record as an entry | violate `LogMatching` |
| `FelixRaftTruncateNotDurable.cfg` | `truncate_after` returns before the cut is on disk; the next flush makes it durable | violate `StateMachineSafety` |
| `FelixRaftSaveCommittedUnflushed.cfg` | the committed or applied index read back is past the recovered log, the subscriber reading | violate `AckedDelivered` |
| `FelixRaftPurgeBeforeSnapshot.cfg` | purge runs before the snapshot covering it is stored | violate `AckedSurvive` |
| `FelixRaftSnapshotTwoRenames.cfg` | the snapshot's data and id are replaced by two renames | violate `StateMachineSafety` |
| `FelixRaftCommitPriorTerm.cfg` | a leader commits an earlier term's entry by counting copies, four terms | violate `LeaderCompleteness` |
| `FelixRaftDeliverUncommitted.cfg` | a subscriber is given entries above the commit index | violate `OffsetNeverReused` |
| `FelixRaftDeliverUncommittedDense.cfg` | the same with two writes: the next leader's skipped count covers an offset the subscriber already holds a record at | violate `OffsetsDense` |
| `FelixRaftSkipsNotCounted.cfg` | a batch does not report the offsets blank and membership entries hold | violate `OffsetsDense` |
| `FelixRaftReachFailover.cfg` | witness: an acknowledged record held by a later leader after a crash | violate `NeverFailover` |
| `FelixRaftReachTruncate.cfg` | witness: a follower cuts off a conflicting tail | violate `NeverTruncates` |
| `FelixRaftReachLostTail.cfg` | witness: a crash loses an unflushed tail | violate `NeverLosesTail` |
| `FelixRaftReachSkips.cfg` | witness: a blank entry between two delivered records | violate `NeverSkipsBetweenRecords` |
| `FelixRaftReachInstall.cfg` | witness: a follower brought up by snapshot | violate `NeverInstallsSnapshot` |
| `FelixRaftReachInstallCrash.cfg` | witness: a crash between an install's second and third write | violate `NeverCrashesMidInstall` |
| `FelixRaftReachSnapshotReplace.cfg` | witness: a broker replaces a snapshot it stored | violate `NeverReplacesSnapshot` |
| `FelixRaftReachDeliverFailover.cfg` | witness: two deliveries in a row from leaders of different terms | violate `NeverDeliversAcrossFailover` |
| `FelixRaftElectLive.cfg` | `FelixRaftElect.tla`: three voters, one crash or cut, one timeout while a round is open, five terms, under weak fairness (`LiveSpec`, no symmetry) | pass `ElectionSettles` and `EventuallyServes` (33k distinct states, seconds) |
| `FelixRaftElectLiveFaults.cfg` | the same with two faults and eight terms. At six it fails on the bound: a pre-vote that passed before a new leader's heartbeat arrived stands anyway, deposes that leader, and the last term goes to a candidate with the shorter log | pass the same and `TermsLeft` (587k distinct states, about a minute) |
| `FelixRaftElectLiveSlack.cfg` | `FelixRaftElectLive.cfg` with six terms | pass the same and `TermsLeft`, no member reaches the sixth term (the same 33k states) |
| `FelixRaftElectLiveFaultsSlack.cfg` | `FelixRaftElectLiveFaults.cfg` with nine terms | pass the same and `TermsLeft` (the same 587k states) |
| `FelixRaftElectLiveSplits.cfg` | no faults, three terms, timeouts not staggered (`Staggered = FALSE`) | violate `ElectionSettles` (7k distinct states): all three pass their pre-vote before any stands, stand in one term, and do it again until no term is left |
| `FelixRaftElectLiveNoOpen.cfg` | no faults, two terms, the commit of the blank entry not owed (`NoOpenSpec`) | violate `EventuallyServes` (535 distinct states): a leader is elected and never opens |
| `FelixRaftMembership.cfg` | `FelixRaftMembership.tla`: three voters and a newcomer, two terms, one write, two membership calls (a whole move), every rule on | pass `OneLeaderPerTerm`, `LogMatching`, `LeaderCompleteness`, `StateMachineSafety`, `AckedSurvive`, `NeverStops` (4.5M distinct states) |
| `FelixRaftMembershipFailover.cfg` | the newcomer already a learner, two terms, no write, three calls: a joint change and a new leader's repeat of it | pass the same (7.1M distinct states) |
| `FelixRaftMembershipTerms.cfg` | the newcomer already a learner, three terms, one write, one joint change | pass the same (91.4M distinct states, 109 min in CI) |
| `FelixRaftMembershipSkipJoint.cfg` | a move swaps one voter for the learner in one entry | violate `OneLeaderPerTerm` |
| `FelixRaftMembershipChangeBeforeCommit.cfg` | the leader leaves the joint configuration before the joint entry is committed | violate `StateMachineSafety` |
| `FelixRaftMembershipLearnerCounts.cfg` | a learner counts toward a majority | violate `AckedSurvive` |
| `FelixRaftMembershipClaimRaces.cfg` | the membership call is made with the fence open and a claim outstanding | violate `NeverStops` |
| `FelixRaftMembershipReopenOnReturn.cfg` | the fence reopens with the call's entry still queued | violate `NeverStops` |
| `FelixRaftMembershipReachMove.cfg` | witness: a whole move commits and an old voter is out | violate `NeverMoves` |
| `FelixRaftMembershipReachStepDown.cfg` | witness: a leader moved out of the voter set steps down | violate `NeverRemovedLeaderStepsDown` |
| `FelixRaftMembershipReachFailoverInJoint.cfg` | witness: a joint entry commits under one leader and the next term's leader leaves the joint configuration | violate `NeverFailoverInJoint` |
| `FelixRaftWritten.cfg` | `FelixRaftWritten.tla`: three voters, one term, two writes, one process crash, one power cut, every rule on | pass `OneLeaderPerTerm`, `LogMatching`, `LeaderCompleteness`, `StateMachineSafety`, `CommittedHeld`, `AckedHeld` (4.3M distinct states) |
| `FelixRaftWrittenCrash.cfg` | two process crashes and no power cut | pass the same and `AckedNeverLost` (0.6M distinct states) |
| `FelixRaftWrittenFailover.cfg` | two terms, one write, one power cut | pass the same as `FelixRaftWritten.cfg` (56.0M distinct states, 46 minutes with 8 workers on a 32-core machine); not registered for pull requests, runs nightly (`tla-walk.yml`) |
| `FelixRaftWrittenTerms.cfg` | two terms, two writes and no crash: a deposed leader's tail at or past the new leader's record | pass the same and `AckedNeverLost`; not registered: no violation in 206M distinct states and 2 hours 13 minutes with 8 workers on a 32-core machine, unfinished |
| `FelixRaftWrittenJoint.cfg` | three voters and a learner, one swap through a joint configuration, one write | pass the same and `AckedNeverLost` (111.2M distinct states, 2 hours 10 minutes with 8 workers on a 32-core machine); not registered: too long for a hosted runner, so only that run checks it. `FelixRaftWrittenJointOneHalf.cfg` and `FelixRaftWrittenLearnerCounts.cfg` violate within seconds on the same model |
| `FelixRaftWrittenJointFailover.cfg` | the same across a change of leader | pass the same and `AckedNeverLost`; not registered: no violation in 217M distinct states and 2 hours 13 minutes with 8 workers on a 32-core machine, unfinished |
| `FelixRaftWrittenLearnerCounts.cfg` | a learner's written report counts | violate `AckedHeld` |
| `FelixRaftWrittenOtherTermCounts.cfg` | a report counts whatever term its log id carries | violate `AckedHeld` |
| `FelixRaftWrittenJointOneHalf.cfg` | a majority of either half of a joint configuration is enough | violate `AckedHeld` |
| `FelixRaftWrittenCommits.cfg` | the commit index counts written reports | violate `CommittedHeld` |
| `FelixRaftWrittenPowerCut.cfg` | every rule on and one power cut that rotates the cut broker's copy (`CutRotates`), as a broker whose copy vouched for an unsynced record does | pass the same and `AckedNeverLost` (16.8k distinct states) |
| `FelixRaftWrittenFailoverPowerCut.cfg` | `CutRotates` at failover bounds: two terms, one write, one process crash, two power cuts | pass the same and `AckedNeverLost` (32.3M distinct states, depth 35, finished in 15 minutes with 8 workers on a 32-core machine); not registered for pull requests |
| `FelixRaftWrittenPowerCutKeeps.cfg` | the same with the copy kept: the loss the early acknowledgement would leave open | violate `AckedNeverLost` |
| `FelixRaftWrittenReachEarly.cfg` | witness: a record acknowledged on the written count before it commits | violate `NeverAcksEarly` |
| `FelixRaftWrittenReachEarlyInJoint.cfg` | witness: the same under a joint configuration | violate `NeverAcksEarlyInJoint` |
| `FelixRaftWrittenReachCommitsAfterFailover.cfg` | witness: a record acknowledged early is committed by the next term's leader | violate `NeverCommitsAfterFailover` |
| `FelixRaftWrittenCommitsPowerCut.cfg` | the proposal to commit on a written majority on relaxed brokers: `WrittenCommits`, `CutRotates` and `Reserve`, one term, two writes, one process crash, one power cut | pass `OneLeaderPerTerm`, `LogMatching`, `LeaderCompleteness`, `StateMachineSafety`, `CommittedHeld`, `AckedHeld`, `AckedNeverLost` (11.4M distinct states, 24 minutes on a CI runner) |
| `FelixRaftWrittenCommitsFailoverPowerCut.cfg` | the same at failover bounds: two terms, one write, one process crash, two power cuts, with `Reserve` off (without a re-form a reservation only holds back counts and reports, so this checks more behaviours) | pass the same (71.2M distinct states, 17 minutes with 30 workers on a 32-core machine; not registered, a box run checks it) |
| `FelixRaftWrittenCommitsPowerCutKeeps.cfg` | the proposal with the cut copy kept | violate `AckedNeverLost` |
| `FelixRaftWrittenCommitsReform.cfg` | the proposal, two power cuts and the operator's re-form, padded up to the highest reservation | pass `OneLeaderPerTerm`, `LogMatching`, `StateMachineSafety`, `CommittedHeld`, `AckedNeverLost`, `OffsetNeverReused` (781k distinct states) |
| `FelixRaftWrittenCommitsReformTrailing.cfg` | the same without the reservation: the re-form pads up to the trailing floor only | violate `OffsetNeverReused` |
| `FelixRaftWrittenCommitsReformLoses.cfg` | witness: under the proposal a power cut of a majority loses a committed record | violate `NeverLosesCommitted` |
| `FelixRaftWrittenReformOnCommit.cfg` | the design as built (commit on fsynced copies), two power cuts and a re-form with the trailing floor alone | pass `OneLeaderPerTerm`, `LogMatching`, `StateMachineSafety`, `OffsetNeverReused`, `NeverLosesCommitted` (58k distinct states) |
| `FelixRaftWrittenCommitsReformTerms.cfg` | the proposal across two terms with the re-form's vouch rule (saved commit, the floor's entry, the whole log only when its last entry has the highest term of any vote read), two power cuts and one broker declared dead | `OffsetNeverReused`, `LossNeedsLostDiskAtTop` (no record a read copy holds is lost while the group is unmarked and no vote is above every entry) and the safety checks above: expected to pass. Before the dead-broker voter-set rule it passed (1,752,892,374 distinct states, 16,415,841,106 generated, depth 45, 6 hours 53 minutes on a 32-core build box); a run under that rule is still owed. Not registered, past a runner's six hours |
| `FelixRaftWrittenCommitsReformTermsFloorHolder.cfg` | the same with the shortcut the rule replaces: the copy with the highest last log id is the base when some copy holds the floor's entry | violate `OffsetNeverReused` (73.0M distinct states, depth 26, 9 minutes 16 seconds on a 32-core build box) |
| `FelixRaftWrittenCommitsReformTermsHappens.cfg` | witness, with no broker dead: a re-form after a term-2 election cuts the longest copy read | violate `NeverCutsAcrossTerms` (21.8k distinct states, 1 second) |
| `FelixRaftWrittenCommitsReformTermsVoteOnly.cfg` | witness: a vote in a term that wrote nothing cuts every copy, so a committed entry a read copy holds is lost (reported as "cut by a vote", needs `--accept-loss`) | violate `LossNeedsLostDisk` (387k distinct states, 4 seconds) |
| `FelixRaftWrittenReformVouchDead.cfg` | the vouch rule with the commit on fsynced copies (`WrittenCommits` off), `Reserve`, one term and one broker declared dead | pass (#2661): a leads term 1, a and b fsync and reserve its first entry, a commits it, a is declared dead and c loses power. The dead broker is a minority of the voters, so b still vouches for its whole log and the base keeps the entry |
| `FelixRaftWrittenReformVouchDeadFive.cfg` | the vouch rule with five voters, two terms and two brokers declared dead | violate `NeverLosesCommitted` (#3023): the dead are a minority, so T stands, but a term-2 candidacy that wins nothing raises T above every entry, so no copy vouches past its saved commit and the committed blank is cut. This is the cut by a vote that `FelixRaftWrittenCommitsReformTermsVoteOnly.cfg` shows; the planner reports it as `cut_by_vote` and a start needs `--accept-loss`. Under `ReformScope` with `MaxWrites = 0`: 3,967,898 distinct states, 44 min 24 s on a CI runner (run 45176967426); with a record, 3 h 56 min (run 45097934400) |
| `FelixRaftWrittenCommitsReformJointDead.cfg` | a joint change from {a, b, c} to {a, b, d}, two brokers declared dead | `BaseAgrees` (no base entry differs from the committed one): expected to pass (#2661). Not registered: under `JointReformScope`, unfinished after six hours on a CI runner (335M distinct states, depth 21, run 45097934400) |
| `FelixRaftWrittenCommitsReformJointDeadOneSet.cfg` | the same, with the gate counting the first voter set only | expected to violate `BaseAgrees` (#2661); a trace is reachable with these constants (c leads term 1 past the joint exit, writes the record to b and loses power; a and d commit in term 2 and die). Not registered: under `JointReformScope`, no violation in 242M distinct states, depth 21, before a CI runner's six hours ran out (run 45097934400); walked nightly (`tla-walk.yml`) |
| `FelixRaftWrittenCommitsReformJointCutDead.cfg` | five voters move to {a, b, c} on c's unsynced write; c is cut, a and b die | `BaseAgrees`, `OffsetNeverReused`: expected to pass (#2661). Not registered: under `ReformScope`, unfinished after six hours on a CI runner (169M distinct states, depth 23, run 45097934400); runs nightly (`tla-walk.yml`) |
| `FelixRaftWrittenReformGate.cfg` | three voters, two declared dead, two terms, one crash | pass (#2729): c leads term 1, holds a tail and crashes, so the tail is still term 1 at the re-form; a and b elect in term 2, commit and die. The dead are a majority, so the group is marked and c vouches only for its saved commit. Under `ReformScope`: 11,888,341 distinct states, depth 38, 19 min 30 s on a CI runner (run 45176967426) |
| `FelixRaftWrittenReformGateOpen.cfg` | the same with the gate forced open | violate `BaseAgrees` (#2729): the crashed term-1 leader vouches for its term-1 tail over the term-2 commit the dead made. 39 s on a CI runner (run 45176967426), depth 20 |
| `FelixRaftWrittenReformReplaced.cfg` | voters a1, b1, c1 with b2 a learner; b1's disk is replaced with nothing kept, c1 is declared dead, and a1, deposed, appends its term-1 tail to b2 | `BaseAgrees`: pass (#2671): voters are keyed by copy id, so b1's vote is unknown, {b1, c1} is a majority and the group is marked. Not registered, run by dispatch |
| `FelixRaftWrittenReformReplacedHappens.cfg` | witness: the same scope, a re-form after b2 saved the deposed term 1 while term 2 committed | violates `NoDeposedAppend` (184k distinct states, 3 s) |
| `FelixRaftWrittenReformReplacedFull.cfg` | `FelixRaftWrittenReformReplaced.cfg` without `ReplacedScope` and with reservations | `BaseAgrees`: pass. Not registered, run by dispatch |
| `FelixRaftWrittenReformReplacedByBroker.cfg` | the same with voters keyed by broker: b2's vote stands in for b1's | violates `BaseAgrees` (#2671): a1 vouches for x at the index where c1 committed f (226k distinct states, depth 19, 7 s) |
| `FelixRaftBatch.cfg` | `FelixRaftBatch.tla`: two members, two producers with two batches each of two or three records, two changes of leader, two purges, snapshot installs; a purge past a cut batch keeps its sequence and not its count | pass `BatchExactlyOnce`, `BatchInOrder`, `ProducerPlaceAgrees` (8,525,374 distinct states, 42.9M generated, depth 19) |
| `FelixRaftBatchKeepsCut.cfg` | the same with the count kept at a purge, so a member past the held pieces finishes the batch | pass the same (8,525,374 distinct states, 43.2M generated, depth 20: the same distinct count, re-run to confirm) |
| `FelixRaftBatchEachRecord.cfg` | each record is its own message with its own term and lease check (#2472; the code now takes the term-bound shape, and this stays as the counterexample twin); no change of leader, no purge | violate `BatchExactlyOnce`: a batch is acknowledged with a record missing |
| `FelixRaftBatchRewrite.cfg` | the re-send of a cut batch is written whole (#2516) | violate `BatchExactlyOnce`: the held prefix is in the log twice |
| `FelixRaftBatchRewriteOrder.cfg` | the same, checked for order | violate `BatchInOrder` |
| `FelixRaftBatchForgetsCut.cfg` | a purge past a cut batch drops it from the table snapshot | violate `BatchExactlyOnce`: the member takes the re-send as new |
| `FelixRaftBatchForgetsCutPlace.cfg` | the same, checked one step earlier | violate `ProducerPlaceAgrees`: two members place one re-send differently |
| `FelixRaftBatchInstallWithoutTable.cfg` | a snapshot install gives the base and not the table snapshot (#2562) | violate `BatchExactlyOnce`: a member installed past a cut batch takes the re-send as new |
| `FelixRaftBatchResetCrash.cfg` | the install taken in the code's two durable steps (save, then drop) with one crash between them, finished at open from the pending-rewind marker (#2574, #2579); one producer, two batches of two or three records, two changes of leader, two purges | pass `BatchExactlyOnce`, `BatchInOrder`, `ProducerPlaceAgrees` (counts from the first Actions run) |
| `FelixRaftBatchResetCrashDropsTable.cfg` | the save does not keep the carried table | violate `BatchExactlyOnce`: a reset finished after the crash leaves the member with the base and no table |
| `FelixRaftBatchTableAtApply.cfg` | the leader's table reads committed entries only; no change of leader | violate `BatchExactlyOnce`: a send that arrives while the first is in flight is written too |
| `FelixRaftBatchHeldBeforeCommit.cfg` | a send is answered as held while its pieces are uncommitted | violate `BatchExactlyOnce`: a batch is acknowledged with nothing committed |
| `FelixRaftBatchReachFinish.cfg` | witness: a cut batch is finished by its re-send and acknowledged | violate `NeverFinishesACut` |
| `FelixRaftBatchReachFinishOverTail.cfg` | witness: a new leader finishes a cut batch while the held pieces it took over are uncommitted. The code never does this, since it serves a term only once every earlier entry is applied; the model allows more | violate `NeverFinishesOverTail` |
| `FelixRaftBatchReachRefusal.cfg` | witness: a member that purged past a cut batch refuses its re-send | violate `NeverRefuses` |
| `FelixRaftBatchReachInstall.cfg` | witness: a member installs another member's snapshot | violate `NeverInstalls` |
| `FelixRaftBatchReachHeld.cfg` | witness, every rule on: a send is answered as held from the committed log | violate `NeverAnswersHeld` |
| `FelixRaftBatchKeepsCutReachSnapshotFinish.cfg` | witness: with the count kept, a leader that retains none of the held pieces finishes the batch from its table snapshot | violate `NeverFinishesFromSnapshot` |
| `FelixRaftGroups.cfg` | `FelixRaftGroups.tla`: two members, one group over two records, one change of leader, four entries; every rule on | pass `NoSkip`, `RedrivenOwed`, `IfNewKeepsServed`, `TrackerHeard` (7,972,425 distinct states, 17.6M generated, depth 38, 31 seconds with 8 workers on a 32-core machine) |
| `FelixRaftGroupsTwoChanges.cfg` | the same with two changes of leader, so a member leads, loses the term and leads again | pass the same (27,964,287 distinct states, 71.0M generated, depth 41, 108 seconds with 8 workers on a 32-core machine) |
| `FelixRaftGroupsNoCreationIndex.cfg` | apply ignores the creation token | violate `NoSkip`: an advance decided before a seek lands after it and moves the cursor over a record the new incarnation never read |
| `FelixRaftGroupsNoDeleteWatermark.cfg` | an entry naming an absent group is never dropped | violate `NoSkip`: an advance decided before a delete lands after it and recreates the group past a record |
| `FelixRaftGroupsStaleByHydrateIndex.cfg` | the earlier rule: drop any entry whose tracker hydrated before the shard's last delete | violate `RedrivenOwed`: another group's delete silences this group's tracker, so its dead letter of a redriven record is dropped and nothing owes the record |
| `FelixRaftGroupsNoRefreshOnDrop.cfg` | the leader keeps a tracker whose entries apply drops | violate `TrackerHeard`: a group with nothing committed hydrates, another group's delete commits, and the tracker's dead letter lands after it and is dropped while the tracker serves on |
| `FelixRaftGroupsNoRedrivenSet.cfg` | a redrive deletes the dead letter and owes the record in memory only | violate `NoSkip`: a record below the cursor is in no set |
| `FelixRaftGroupsDeadAfterAdvance.cfg` | a dead letter is proposed in any order with the advance over it | violate `NoSkip`: the advance commits first and the record is in no set |
| `FelixRaftGroupsIfNewTableOnly.cfg` | if_new checks the table only | violate `IfNewKeepsServed`: a new group with a record handed out and nothing committed is moved |
| `FelixRaftGroupsTrackerAcrossTerms.cfg` | a member keeps its tracker across terms; two changes of leader | violate `RedrivenOwed`: a member leading again serves from a tracker that misses a redrive committed in between |
| `FelixRaftGroupsHydrateBeforeApply.cfg` | the leader decides from a prefix of the committed log it has applied | violate `RedrivenOwed`: the tracker misses a committed redrive |
| `FelixRaftGroupsUserOpsBeforeOwnApply.cfg` | hydrate, redrive and seek do not wait for the leader's own entries in flight | violate `RedrivenOwed`: a seek's tracker misses a redrive the leader proposed just before it |
| `FelixRaftStreamPurge.cfg` | `FelixRaftStreamPurge.tla`: three voters, four entries, two trims, one change of leader, one restart, one re-form; every rule on | pass `NoReadBelowFloor`, `NoAckedLoss`, `PurgeBelowFloor`, `AppliedCoversPurge`, `TablesAgree`, `HeadersAgree`, `OffsetNeverReused`, `CatchUpPossible`. Not registered, run by dispatch: over 80M distinct states and unfinished after 45 minutes on a build box |
| `FelixRaftStreamPurgeSmall.cfg` | the same with three entries | pass the same (13,827,652 distinct states, 69.2M generated, 3.6 minutes with 10 workers on a shared 32-core box) |
| `FelixRaftStreamPurgeLearner.cfg` | three voters and a learner, four entries, one change of leader, one restart, no re-form | pass the same. Not registered, run by dispatch: over 60M distinct states and unfinished after 45 minutes |
| `FelixRaftStreamPurgeLearnerSmall.cfg` | the same with three entries | pass the same (7,979,726 distinct states, 55.8M generated, 100 seconds) |
| `FelixRaftStreamPurgeLearnerReform.cfg` | three voters and a learner, three entries, two trims, one change of leader, one restart, one re-form | pass the same. Not registered, run by dispatch: over 68M distinct states and unfinished after 45 minutes |
| `FelixRaftStreamPurgeLearnerReformSmall.cfg` | the same with one trim and no change of leader | pass the same (45,082,276 distinct states, 261.8M generated, 5.6 minutes) |
| `FelixRaftStreamPurgeNoCap.cfg` | the leader purges past a member's match | pass the same: the cap is not a safety rule. Not registered, run by dispatch: over 84M distinct states and unfinished after 45 minutes |
| `FelixRaftStreamPurgeNoCapSmall.cfg` | the same with three entries | pass the same (16,369,637 distinct states, 81.9M generated, 3.5 minutes) |
| `FelixRaftStreamPurgeFive.cfg` | three voters, five entries, one restart, no change of leader, no re-form, no cap | pass the same. Not registered, run by dispatch: over 67M distinct states and unfinished after 45 minutes |
| `FelixRaftStreamPurgeFourSmall.cfg` | the same with four entries | pass the same (4,526,442 distinct states, 26.8M generated, 47 seconds) |
| `FelixRaftStreamPurgeFiveReform.cfg` | three voters, five entries, one re-form, no restart, no cap | pass the same. Not registered, run by dispatch: over 84M distinct states and unfinished after 45 minutes |
| `FelixRaftStreamPurgeFourReformSmall.cfg` | the same with four entries | pass the same (43,521,359 distinct states, 200.7M generated, 4.2 minutes) |
| `FelixRaftStreamPurgeTwoReforms.cfg` | three voters and a learner, three entries, one restart, two re-forms: the floor a re-form carries is read by the next | pass the same. Not registered, run by dispatch: out of heap on a build box |
| `FelixRaftStreamPurgeTwoReformsSmall.cfg` | the same with two entries | pass the same (9,235,335 distinct states, 45.3M generated, 2.6 minutes on a loaded box) |
| `FelixRaftStreamPurgeTwoReformsWrite.cfg` | three voters, no learner, four entries, two trims, no restart, two re-forms: a write and a trim fit between the re-forms | pass the same. Not registered, run by dispatch: over 88M distinct states and unfinished after 13 minutes on a build box |
| `FelixRaftStreamPurgeLearnerReformInstall.cfg` | `LearnerReformSmall` without the leader's cap and without a restart, so the learner installs before the re-form | pass the same (10,979,767 distinct states, depth 32, 14 minutes on a hosted runner). Not registered, run by dispatch |
| `FelixRaftStreamPurgePastFloor.cfg` | a purge bounded by the applied entry, not by floor - 1 | violate `NoAckedLoss`: a record at or above the floor is purged |
| `FelixRaftStreamPurgeInstallAboveFloor.cfg` | the snapshot at the applied entry, and a member installs any snapshot ahead of it | violate `NoAckedLoss`: the member skips records it was never sent |
| `FelixRaftStreamPurgeSnapAtApplied.cfg` | the snapshot at the applied entry, install rule kept, leader cap kept | violate `CatchUpPossible`: a follower purges past a slow member, leads, and the member refuses every snapshot |
| `FelixRaftStreamPurgePastMatch.cfg` | the same without the cap and with no change of leader | violate `CatchUpPossible` |
| `FelixRaftStreamPurgeTablesAtApplied.cfg` | the snapshot at the purge point carries the current tables | violate `TablesAgree`: entries above the purge point are applied twice |
| `FelixRaftStreamPurgeReformEntries.cfg` | a re-form reads a copy's last entry only | violate `OffsetNeverReused`: a copy that just installed a snapshot reads as empty |
| `FelixRaftStreamPurgeReformNoFloorTwice.cfg` | two re-forms, the second reads a floor only from a copy that installed or whose log reaches it, not from one the first re-form carried it into | violate `OffsetNeverReused`: the second re-form gives out offsets below the carried floor again. One re-form passes at these bounds (`FelixRaftStreamPurgeReformNoFloorOnce.cfg`) |
| `FelixRaftStreamPurgeReformNoFloor.cfg` | a re-form reads the last entry and the purged id, not the floor | violate `OffsetNeverReused`: offsets below an installed floor are given out again |
| `FelixRaftStreamPurgeReformKeepsOwnFloor.cfg` | a re-form leaves each copy its own floor; a learner installs | violate `NoReadBelowFloor`: the new leader serves below the learner's floor |
| `FelixRaftStreamPurgeReformFloorNotStored.cfg` | the re-form raises the floor in memory, not in the snapshot; learner, restart | violate `NoReadBelowFloor`: after a restart the copy serves below the floor |
| `FelixRaftStreamPurgeReachInstall.cfg` | witness: a member that installed a snapshot leads and serves | violate `NeverServesAfterInstall` |
| `FelixRaftStreamPurgeReachPurge.cfg` | witness: a member purges past a record | violate `NeverPurgesARecord` |
| `FelixRaftStreamPurgeReachTrimPurge.cfg` | witness at the bounds of `FelixRaftStreamPurgeFive.cfg`: a purge drops a trim entry | violate `NeverPurgesATrim` |
| `FelixRaftStreamPurgeReachWriteAfterReform.cfg` | witness at the bounds of `FelixRaftStreamPurgeFiveReform.cfg`: a write follows a re-form | violate `NeverWritesAfterReform` |
| `FelixRaftStreamPurgeReachPurgeAfterReform.cfg` | witness at the same bounds: a purge follows a re-form | violate `NeverPurgesAfterReform` |
| `FelixRaftStreamPurgeReformNoFloorOnce.cfg` | the bounds of `ReformNoFloorTwice` with one re-form: the control | pass the same. Not registered, run by dispatch: about 43 minutes in CI, too long for a PR shard |
| `FelixRaftStreamPurgeReachTrimPurgeFour.cfg` | witness at the bounds of `FourSmall.cfg`: a purge drops a trim entry | violate `NeverPurgesATrim` |
| `FelixRaftStreamPurgeReachWriteAfterReformFour.cfg`, `...ReachPurgeAfterReformFour.cfg` | witnesses at the bounds of `FourReformSmall.cfg`: a write, and a purge, follow a re-form | violate `NeverWritesAfterReform`, `NeverPurgesAfterReform` |
| `FelixRaftStreamPurgeReachWriteAfterReformTwo.cfg`, `...ReachPurgeAfterReformTwo.cfg` | witnesses at the bounds of `TwoReformsWrite.cfg`: a write, and a purge, follow a re-form | violate `NeverWritesAfterReform`, `NeverPurgesAfterReform` |
| `FelixRaftStreamPurgeReachWriteAndTrimBetweenReforms.cfg` | witness at the same bounds: a write and a trim land between the two re-forms | violate `NeverWriteAndTrimBetweenReforms` |
| `FelixRaftStreamPurgeReachRaiseBetweenReforms.cfg` | witness at the same bounds: a member applies a floor above the carried one before the second re-form | violate `NeverRaiseBetweenReforms` |
| `FelixRaftStreamPurgeReachTrimPurgeTwoReforms.cfg` | witness at the same bounds: a trim entry is purged in a history with two re-forms | violate `NeverPurgesATrimAfterTwoReforms` |
| `FelixRaftStreamPurgeReachInstallThenReform.cfg` | witness at the bounds of `LearnerReformInstall.cfg`: a learner installs a snapshot, then the group re-forms | violate `NeverInstallsThenReforms` |
| `FelixRaftStreamPurgeInstallLowersFloor.cfg` | `NoCap`'s bounds, the install storing the leader's snapshot as it is, steered by `FloorDropPath` to trims naming 3 then 4 | violate `OffsetNeverReused`: an install lowers a member's floor from 4 to 3, a restart keeps 3, and a re-form gives out offset 4 again (9,502,017 distinct states, 2.5 minutes with 4 workers) |
| `FelixRaftStreamPurgeInstallKeepsFloor.cfg` | the same history, the install keeping the higher floor | pass every invariant (30,733,729 distinct states, 16 minutes with 4 workers on a shared box). Not registered, run by dispatch |
| `FelixRaftStreamPurgeInstallKeepsFloorSmall.cfg` | `InstallKeepsFloor`'s bounds, steered by `LowerFloorPath`: two records, then trims naming 3 and 4 at offsets 3 and 4, and no restart or re-form before a lower-floor install | pass every invariant (112,075 distinct states, 1.75M generated, 42 seconds with 4 workers) |
| `FelixRaftStreamPurgeReachLowerFloorInstall.cfg` | witness at the same bounds: a member installs a snapshot carrying a floor below its own | violate `NeverInstallsLowerFloor` (9 seconds with 4 workers) |
| `FelixRaftStreamPurgeInstallLowersFloorSmall.cfg` | `InstallKeepsFloorSmall` with the install storing the leader's snapshot as it is | violate `OffsetNeverReused` (14 seconds with 4 workers) |
| `FelixRaftStreamPurgeInstallKeepsFloorLong.cfg` | `InstallKeepsFloorSmall` with a learner, five entries and two changes of leader, steered by `LowerFloorPath` | pass every invariant. Not registered, run by dispatch |
| `FelixRaftStreamPurgeReachRewrittenInstall.cfg` | witness at the same bounds: the installer leads and a member behind it installs the snapshot a lower-floor install rewrote | violate `NeverInstallsRewrittenSnap` (5 minutes with 8 workers, 1.29M distinct states) |
| `FelixRaftStreamPurgeInstallRefusesLowerFloor.cfg` | the same history, the member refusing a snapshot carrying a lower floor | violate `CatchUpPossible`: the new leader's snapshot is refused, so the member is stuck (1.7 minutes with 4 workers) |
| `FelixRaftStreamPurgeInstallRefusesLowerFloorSafe.cfg` | the same without `CatchUpPossible` | pass the safety invariants (30,733,729 distinct states, 18 minutes with 4 workers). Not registered, run by dispatch |
| `FelixRaftStreamPurgeNoCapSmallRefuse.cfg` | `NoCapSmall` with the refusal, without `CatchUpPossible` | pass the safety invariants (16,369,637 distinct states, 81.9M generated, 6 minutes with 4 workers) |
| `FelixRaftStreamPurgeNoCapRefuse.cfg`, `...TwoReformsRefuse.cfg` | `NoCap` and `TwoReforms` with the refusal, without `CatchUpPossible` | pass the safety invariants. Not registered, run by dispatch |
| `FelixRaftCond.cfg` | `FelixRaftCond.tla`: three members, three terms, logs up to five entries, one plain publish, one membership entry, three conditional publishes (two naming offset 2, one offset 3); each member decides `kept` from the entry and its index | pass `CondKeptAtExpected`, `OneKeptPerOffset`, `MembersAgree`, `AnswersHold` (169,456,277 distinct states, depth 34, 24 minutes with 12 workers on a 32-core machine) |
| `FelixRaftCondTwoTerms.cfg` | the same with two terms | pass the same (18,646,633 distinct states, depth 30, 3 minutes with 12 workers on a 32-core machine) |
| `FelixRaftCondLeaderDecides.cfg` | the leader decides `kept` from its guess of the next index when the publish arrives, and followers copy it | violate `CondKeptAtExpected`: a membership entry lands between the guess and the append, and the record is kept one index past its offset |
| `FelixRaftCondLeaderDecidesNoMembers.cfg` | the same with no membership entry | pass the same as `FelixRaftCond.cfg` (101,575,424 distinct states, depth 36, 23 minutes with 8 workers on a 32-core machine): the membership entry is what breaks the guess |
| `FelixRaftCondReachAnswers.cfg` | witness, one term: one conditional publish is answered "ok" and another "mismatch" | violate `NeverOkAndMismatch` |
| `FelixRaftCondBatch.cfg` | conditional batches: two terms, one membership entry, a batch of three naming offsets 2..4, one record naming 2, a batch of two naming 3..4; readers skip a batch as soon as a committed entry inside its range is not its next piece, and the read bound stops before the batch the commit ends inside (`resolve` and `bound` in `crates/server/felix-storage/src/log/conditional.rs`) | pass `CondKeptAtExpected`, `OneKeptPerOffset`, `MembersAgree`, `AnswersHold`, `WholeBatchesOnly`, `ReadsStable` (1,190,750 distinct states, depth 28, 1 minute on a hosted runner) |
| `FelixRaftCondBatchThreeTerms.cfg` | the same with three terms | pass the same (10,979,767 distinct states, depth 32, 14 minutes on a hosted runner). Not registered, run by dispatch |
| `FelixRaftCondBatchNoHide.cfg` | readers see every committed kept piece | violate `WholeBatchesOnly`: a reader sees the first piece of a batch whose last piece is not committed |
| `FelixRaftCondBatchBoundOnly.cfg` | the read bound stops before the batch the commit ends inside, but nothing skips a cut one | violate `WholeBatchesOnly`: a change of leader commits a cut batch and its blank, the bound passes it, and readers see the cut |
| `FelixRaftCondBatchAnyLast.cfg` | the skip checks only that the entry at a cut piece's last index is kept, not that it is a last piece of the same length; a batch of three naming 2..4 and one record naming 4 | violate `WholeBatchesOnly`: the batch is cut after its first piece, the record is kept at 4, and readers see the cut piece |
| `FelixRaftCondBatchReachCut.cfg` | witness: a change of leader commits a strict prefix of a batch | violate `NeverCutCommitted` |
| `FelixRaftCondBatchReachRead.cfg` | witness: a reader sees a record of a batch of more than one | violate `NeverReadsBatch` |
| `FelixRaftCondBatchReachEarlyCut.cfg` | witness: a batch is cut, and the read bound passes it, while its last index is still uncommitted | violate `NeverEarlyCut` |
| `FelixRaftIdempotent.cfg` | `FelixRaftIdempotent.tla`: three members, three terms, logs up to five entries, one producer with two batches of one or two records, the second started once the first is written, either re-sent to any leader at any time; the leader places a send by the producer table read from its whole log. New two-record batches are modelled ahead of the engine, which refuses them today | pass `NoDuplicate`, `AckedWhole`, `InOrder`, `MembersAgree` |
| `FelixRaftIdempotentTwoTerms.cfg` | the same with two terms: one change of leader | pass the same |
| `FelixRaftIdempotentMemory.cfg` | the table kept in the leader's memory, started empty when it wins a term | violate `NoDuplicate`: a batch committed under one leader and not yet answered is re-sent to the next, which writes it again |
| `FelixRaftIdempotentRewrite.cfg` | the re-send of a cut batch written whole, over the pieces the next leader holds | violate `NoDuplicate`: the held piece is committed twice |
| `FelixRaftIdempotentReachFinish.cfg` | witness: a batch of two records cut by a change of leader is finished by the re-send and acknowledged | violate `NeverFinishesACut` |
| `FelixRaftIdempotentReachHeld.cfg` | witness: a re-send to the next leader is answered as held from the earlier leader's entries | violate `NeverHeldByLaterLeader` |
| `FelixRaftReads.cfg` | `FelixRaftReads.tla`: `FelixRaft.tla` with one `Quorum` read, three brokers, two terms, one write, one crash; the read takes the open term, a read-index round fixes its bound at max(the term's blank entry, the commit index), the broker applies the bound, then takes its value | pass `NoStaleRead`, `LeaderCompleteness`, `StateMachineSafety` (5.7M distinct states, 8 min) |
| `FelixRaftReadsWritten.cfg` | the same with writes answered on a written majority before their commit, no crash, one term (two terms passed 83M distinct states without finishing); the read bound also covers the newest answered write | pass `NoStaleRead` (0.1M distinct states, 9 s) |
| `FelixRaftReadsNoRound.cfg` | the round skipped | violate `NoStaleRead`: a deposed leader answers without its successor's write |
| `FelixRaftReadsNoBlankBarrier.cfg` | a read served before the term opened, its bound the commit index alone | violate `NoStaleRead`: a new leader reads before it learned what its predecessor committed |
| `FelixRaftReadsSkipsAnswered.cfg` | written-majority answers, the bound left at the read index; one term | violate `NoStaleRead`: an answered write above the commit index is missed |
| `FelixRaftSessionsSubscriber.cfg` | `FelixRaftSessions.tla`: a subscriber reading up to what any broker that believes it leads applied, resuming by offset; checked against the open leader no broker has outvoted; two terms, two writes, one crash | pass `NoLostDelivery`, `OffsetNeverReused` (16.1M distinct states, 24 min) |
| `FelixRaftSessionsPastCommit.cfg` | the subscriber reads past the commit index | violate `NoLostDelivery` |
| `FelixRaftSessionsGroup.cfg` | one group commit, proposed in the term its coordinator has open and answered once applied as the entry that term wrote; two terms, one crash | pass `NoStaleGroupCommit` (1.7M distinct states, 2 min) |
| `FelixRaftSessionsGroupAckOnWrite.cfg` | the commit answered once in the coordinator's own log | violate `NoStaleGroupCommit` |
| `FelixRaftSessionsGroupIgnoresTerm.cfg` | the commit answered once applied past its index, whatever entry holds it | violate `NoStaleGroupCommit`: the next leader's entry took the offset |
| `FelixRaftIdentity.cfg` | `FelixRaftIdentity.tla`: three brokers, two terms, one write, one membership change, one fault that empties the shard directory or loses the root | pass `OneLeaderPerTerm`, `LeaderCompleteness`, `StateMachineSafety`, `AckedSurvive`, `AckedHeldByLeader` (0.2M distinct states) |
| `FelixRaftIdentityBatched.cfg` | the same faults plus the relaxed-fsync crash, with the batched log mark the broker ships; no restore, so the mark never moves | pass the same (0.2M distinct states) |
| `FelixRaftIdentityRelaxedCrash.cfg` | the fault is a relaxed-fsync crash: the storage id rotates and the directory rotates its copy when opened | pass the same (0.1M distinct states) |
| `FelixRaftIdentityRootRestore.cfg` | the whole root goes back to a backup from an earlier run; the incarnation check catches it | pass the same (0.8M distinct states) |
| `FelixRaftIdentityRootRestoreSameRun.cfg` | the same, and the backup may be from the run the restore interrupts. The machine stayed up, so the run witness holds the root sequence | pass the same (1.6M distinct states) |
| `FelixRaftIdentityRootRestoreNoWitness.cfg` | the same after a machine restart, so no witness is left: the root goes back after the control plane heard its last sequence | pass the same (8.7M distinct states) |
| `FelixRaftIdentityRootRestoreBatched.cfg` | the same-run restore with a witness and the batched log mark, a mark interval after the last append | pass the same (11.8M distinct states) |
| `FelixRaftIdentityWipeThenRootRestore.cfg` | two faults on one broker: the directory is emptied, then the root goes back to a backup taken in the same run, before the wipe or after it. The new copy stays a learner, so this shows the votes since the backup are caught, not the new copy's own mark write | pass the same (7.4M distinct states) |
| `FelixRaftIdentityRootRestoreNoSequence.cfg` | the gate does not compare the root sequence: a backup of the current run reads as a crash and its copy votes twice in a term | violate `OneLeaderPerTerm` (issue #2303) |
| `FelixRaftIdentityRootRestoreUnheard.cfg` | no witness, and the root goes back before the control plane heard its last sequence | violate `OneLeaderPerTerm`: the residual the design names (issue #2303, Questions, 12) |
| `FelixRaftIdentityRootRestoreYoung.cfg` | a witness, the batched log mark, and the root goes back inside the mark interval | violate `AckedHeldByLeader`: the residual the design names (Questions, 11) |
| `FelixRaftIdentityDirRestore.cfg` | one shard directory goes back, older log or vote or both, a mark interval after the last append, batched mark | pass the same (2.2M distinct states) |
| `FelixRaftIdentityMarkExact.cfg` | one shard directory goes back at any time, exact mark (still selectable, not the default) | pass the same (0.6M distinct states) |
| `FelixRaftIdentityMarkBatched.cfg` | the same bounds with the batched mark: the copy votes for a candidate that lacks an acknowledged record | violate `AckedHeldByLeader`: the residual the design accepts (Questions, 11) |
| `FelixRaftIdentityWipeThenRestore.cfg` | two faults on one broker: the directory is emptied, then an older directory with the old copy id is put back | pass the same (3.0M distinct states) |
| `FelixRaftIdentityWipeKeepsId.cfg` | a member made in an empty directory takes the old id | violate `LeaderCompleteness` |
| `FelixRaftIdentityNoRestoreCheck.cfg` | a restored root keeps its storage id, so the copy keeps its id with an older log and vote | violate `LeaderCompleteness` |
| `FelixRaftIdentityNoStorageIdCheck.cfg` | a copy cut short by a relaxed-fsync crash keeps its id | violate `LeaderCompleteness` |
| `FelixRaftIdentityNoVoteMark.cfg` | a directory restored to an older vote with the same log votes twice in a term | violate `OneLeaderPerTerm` |
| `FelixRaftIdentityVoteBeforeMark.cfg` | the vote is answered before its mark is synced, and the directory goes back in between | violate `OneLeaderPerTerm` |
| `FelixRaftIdentityMarkForgetsCopy.cfg` | the marks are reset at a rotation and the old directory, put back, passes them | violate `LeaderCompleteness` (issue #2303) |
| `FelixRaftIdentityRejoins.cfg` | witness: a rotated copy is swapped in by a committed membership change | violate `NeverRejoins` |
| `FelixRaftIdentityRejoinLeads.cfg` | witness: a rotated copy, a voter again, wins an election | violate `NeverRotatedLeads` |
| `FelixRaftIdentityRestoreCaught.cfg` | witness: a restored directory is caught and rotated | violate `NeverRotates` |
| `FelixRaftIdentityRootRestoreCaught.cfg` | witness: a restored root is caught and rotated | violate `NeverRotates` |
| `FelixRaftIdentityRootRestoreSameRunCaught.cfg` | witness: with no restart, a root restored in its own run is caught by the root sequence and rotated | violate `NeverRotates` |
| `FelixRaftIdentityRootRestoreNoWitnessCaught.cfg` | witness: the same with no run witness, so the control plane's copy of the sequence caught it | violate `NeverRotates` |
| `FelixRaftPlacement.cfg` | `FelixRaftPlacement.tla`: three voters and a spare broker, one term, one write, one move by the control plane (a learner entry and a `change_membership` call), no lost disk, every rule on | pass `OneLeaderPerTerm`, `LogMatching`, `LeaderCompleteness`, `StateMachineSafety`, `AckedSurvive`, `AckedOnDisk`, `OffsetNeverReused`, `DeliveredAgree`, `CommitNeverLowers` (534K distinct states). With one term `LeaderCompleteness` has no earlier term to check, and with one write `OffsetNeverReused` has one record |
| `FelixRaftPlacementFailover.cfg` | the same with two terms and no write: a new leader finishes the move or acts on the target again | pass the election, log and commit checks (7.8M distinct states). With no write the Acked, Offset and Delivered checks have nothing to hold |
| `FelixRaftPlacementFull.cfg` | the same with two terms and the write | pass the same (39.3M distinct states, 160 minutes on a CI runner); not registered, checked nightly |
| `FelixRaftPlacementReseat.cfg` | three brokers, two terms: one directory is wiped, the copy takes a new copy id and the leader swaps it in | pass the same (347K distinct states) |
| `FelixRaftPlacementShrink.cfg` | three brokers, two terms: a seat is removed, three voters become two, and the removed copy is deleted under the rule | pass the same (314K distinct states) |
| `FelixRaftPlacementMoveWipe.cfg` | the move in one term with one lost directory and the moved copy deleted under the rule | pass the same (5.7M distinct states) |
| `FelixRaftPlacementMoveWipeFull.cfg` | the same with two terms | not registered and not checked yet: unfinished after three hours on a CI runner, at 98M distinct states with 18.5M queued |
| `FelixRaftPlacementStale.cfg` | three brokers, one term, two target changes, and a leader that may act on the replaced target | pass the same (154K distinct states) |
| `FelixRaftPlacementStaleFull.cfg` | the same with two terms | pass the same (6.4M distinct states, 39 minutes on a CI runner) |
| `FelixRaftPlacementSameId.cfg` | the metadata group's rejoin: three members, three terms, one write, one wipe; the member keeps its id, withholds its vote, answers no leader until every other member has given its term and then none below the highest, and votes again once caught up | pass the same |
| `FelixRaftPlacementSameIdCounted.cfg` | `SameId` without the floor: the wiped member answers a leader of any term | violate `AckedSurvive`: an acknowledged write is lost (#2365) |
| `FelixRaftPlacementSameIdFive.cfg` | five members, two terms, no write, with a rule the code does not use: a withheld member is not counted and there is no floor. A candidate holds the wiped member's vote and is one short, the member catches up with the older leader, votes again, and votes a second time in that term | violate `OneLeaderPerTerm`: not counting the member is not enough for five (1.65M distinct states, 7 min on a laptop) |
| `FelixRaftPlacementSameIdFiveFloor.cfg` | `SameIdFive` with the floor the code has | pass: the candidate gives its term, so the member never follows the older leader. Not registered: unfinished in 60 min on a CI runner (#2825) |
| `FelixRaftPlacementSameIdFiveFloorTwoWipes.cfg` | `SameIdFiveFloor` with a write and two lost volumes; each wiped member is told the other is withheld | expected to pass `OneLeaderPerTerm` and `AckedSurvive`: the rest of the quorum holds every term the pair promised. Not registered: unfinished after 2 h on a CI runner (14.4M distinct states, depth 20, dispatch 44442468320) |
| `FelixRaftPlacementFormFiveFloor.cfg` | five members on a first start with the floor, a write and a lost volume; late members are still withheld and empty | pass `OneLeaderPerTerm` and `AckedSurvive`: a withheld member's answer proves nothing, and only the members that vote prove the floor. Not registered: unfinished after 3 h 30 min on a CI runner (dispatch 44581623665) |
| `FelixRaftPlacementTwoWipesFloor.cfg` | three members with the floor and two lost volumes, past what three survive, with an operator waiver; the floor takes every term given, a withheld member's included | pass `AckedHeldSurvive`; `OneLeaderPerTerm` and `AckedSurvive` are not listed, since under the waiver the third alone may not hold the term the pair promised |
| `FelixRaftPlacementTwoWipesFloorVotersOnly.cfg` | the same with the floor taken from voting members only, dropping the term a withheld member follows | violate `AckedHeldSurvive`: the waived member serves a deposed leader, which acks a record the next leader drops (#2824) |
| `FelixRaftPlacementTwoWipesFloorBlock.cfg` | the same group with no waiver: each wiped member blocks past the tolerance | pass `OneLeaderPerTerm`, `AckedSurvive`, `AckedHeldSurvive` |
| `FelixRaftPlacementFormWaiverFloor.cfg` | `FormWaiver` with the floor: the waived slot never starts | pass: its silence is not an answer, so a wiped member never learns a floor |
| `FelixRaftPlacementSkipJoint.cfg` | `change_membership` writes the target in one entry | violate `OneLeaderPerTerm` |
| `FelixRaftPlacementNoWithhold.cfg` | a wiped member that keeps its id votes at once | violate `LeaderCompleteness` |
| `FelixRaftPlacementAnswerAnyId.cfg` | a wiped copy answers a request that names the copy id it replaced | violate `LeaderCompleteness` |
| `FelixRaftPlacementDeleteOnTarget.cfg` | a copy is deleted as soon as the target drops it, with one lost disk | violate `AckedOnDisk` |
| `FelixRaftPlacementTwoWipes.cfg` | two lost directories in a group of three: the bound, not a rule | violate `AckedOnDisk` |
| `FelixRaftPlacementReachMove.cfg` | witness: a seat moves to the spare broker | violate `NeverMoves` |
| `FelixRaftPlacementReachShrink.cfg` | witness: the voter set gets smaller | violate `NeverShrinks` |
| `FelixRaftPlacementReachDelete.cfg` | witness: a removed copy's directory is deleted | violate `NeverDeletes` |
| `FelixRaftPlacementReachReseat.cfg` | witness: a rotated copy is swapped in for its old id | violate `NeverReseats` |
| `FelixRaftPlacementReachWipedLeads.cfg` | witness: a wiped broker leads again under its new copy id | violate `NeverWipedLeads` |
| `FelixRaftPlacementReachVotesAgain.cfg` | witness: a member that kept its id through a wipe votes again | violate `NeverVotesAgain` |
| `FelixRaftPlacementReseatWrites.cfg` | `FelixRaftPlacementReseat.cfg` with two writes, so `OffsetNeverReused` has two records to compare | pass the same (2.2M distinct states) |
| `FelixRaftPlacementSameIdWrites.cfg` | `FelixRaftPlacementSameId.cfg` with two terms and two writes | pass the same (364K distinct states) |
| `FelixRaftPlacementNoWithholdOffsets.cfg` | the same with a wiped member that votes at once: a leader elected on its vote gives a delivered record's offset to another record | violate `OffsetNeverReused` |
| `FelixRaftPlacementShrinkRetain.cfg` | `FelixRaftPlacementShrink.cfg` with `retain = true`: the removed voter stays as a learner | pass the same (277K distinct states) |
| `FelixRaftPlacementMoveWipeFailover.cfg` | `FelixRaftPlacementMoveWipe.cfg` with two terms and no write: a leader change during a move with one lost disk | not registered and not checked yet: 14M distinct states and unfinished after ten minutes on a laptop; run through the dispatch |
| `FelixRaftPlacementReturnWipe.cfg` | three brokers, one term, one lost disk: a seat is removed and one is moved back, the leader on the current target only; the id told to delete is not placed again | pass the same (2.2M distinct states) |
| `FelixRaftPlacementPlaceDoomed.cfg` | the same where the control plane may place an id it told to delete | violate `AckedOnDisk` |
| `FelixRaftPlacementStaleWipe.cfg` | `FelixRaftPlacementStale.cfg` with one lost disk, every rule on: a leader may act on a replaced target and the control plane tells a removed copy's broker to delete it. The version rule keeps a stale target from giving a deleted copy its vote back (#2385) | pass the same (4.5M distinct states, 27 minutes on a CI runner) |
| `FelixRaftPlacementStaleWipeNoVersion.cfg` | the same where a leader acts on a target older than the membership in its log | violate `AckedOnDisk`: the copy gets its vote back, a write is acknowledged with it, the other disk is lost and the deletion takes the last copy (9.2M distinct states, 60 minutes on a CI runner) |
| `FelixRaftPlacementStampDoomable.cfg` | three voters, one term: a seat is removed, placed back and removed again. The stamp writes the last target's version into the log, so once the group is quiet the removed copy can be told to delete | pass `DroppedIsDoomable` and the safety checks (64K distinct states) |
| `FelixRaftPlacementStampDoomableNoStamp.cfg` | the same without the stamp: the log keeps the first removal's version, below the last target that named the copy | violate `DroppedIsDoomable`: the copy can never be deleted |
| `FelixRaftPlacementSingleMove.cfg` | a group of one moves: voter `a`, spare `b`, two terms, one write. `a` grows to `{a, b}`, transfers to `b`, and `b` shrinks to `{b}` | pass the safety checks and `NeverLeadsRemoved` (1.3K distinct states) |
| `FelixRaftPlacementSingleMoveSelfRemove.cfg` | the same where `a` changes `{a}` to `{b}` itself | violate `NeverLeadsRemoved`: `a` leads a settled membership it is not a voter of |
| `FelixRaftPlacementTransferMove.cfg` | three voters and a spare, two terms, no write: a seat moves to the spare, and when it is the leader's seat the leader grows, then transfers leadership to a voter of the target | pass the election, log and commit checks and `NeverLeadsRemoved` (8.0M distinct states, 51 minutes on a CI runner) |
| `FelixRaftPlacementTransferMoveSelfRemove.cfg` | the same where the leader writes a target without itself | violate `NeverLeadsRemoved` |
| `FelixRaftPlacementReachTransfer.cfg` | witness: in `FelixRaftPlacementTransferMove.cfg` a transfer hands leadership over | violate `NeverTransfers` |
| `FelixRaftPlacementPlaced.cfg` | `FelixRaftPlacement.cfg` with leaders that follow placement (`PlacedLeaders`) and no target change: a settled leader with an entry of its term committed may hand its leadership to any caught-up voter | pass every safety invariant |
| `FelixRaftPlacementReachPlaced.cfg` | witness: with no target change a placement hand-over still lands | violate `NeverTransfers` |
| `FelixRaftPlacementPlacedTarget.cfg` | `Placed.cfg` with one target change, so a placed transfer may race a membership change | not registered: a CI dispatch (run 43773912279) ran past six hours |
| `FelixRaftPlacementPlacedTargetNoWrite.cfg` | `PlacedTarget.cfg` without the client write: the target change still takes both calls (`AddLearner`, then `Change`) and a placed transfer may race it. With no write the ack invariants hold trivially; the membership invariants carry the check | expected to pass every safety invariant; registered once its dispatch finishes |
| `FelixRaftPlacementReachPlacedTargetNoWrite.cfg` | witness at the bounds of `PlacedTargetNoWrite.cfg`: a transfer lands while the target's new member is a learner and the voters are not yet the target's, with the leader in both, so only the placed rule can have sent it (`NeverTransfersOffTarget`) | violate `NeverTransfersOffTarget`, so the bounded twin reaches the race |
| `FelixRaftPlacementTransferMoveFull.cfg` | `FelixRaftPlacementTransferMove.cfg` with one write | not registered and not checked yet: unfinished after six hours on a CI runner (issue #2497) |
| `FelixRaftPlacementRewindSpare.cfg` | `FelixRaftPlacementMoveWipe.cfg` with one restore of the control plane's metadata from a backup while a leader's appended joint entry names the spare broker's copy. The doom waits for every member's report, and a report names the copies in appended entries too | pass the safety checks (30.5M distinct states, 176 minutes on a CI runner); not registered, checked nightly |
| `FelixRaftPlacementRewindSpareCommitted.cfg` | the same where the doom reads the committed history instead of the reports, and a learner entry takes the target's version | violate `AckedOnDisk`: the spare's copy is told to delete while an appended joint entry still gives it a vote (57 minutes on a CI runner) |
| `FelixRaftPlacementStaleWipeTwoTerms.cfg` | `FelixRaftPlacementStaleWipe.cfg` with two terms and no write: a new leader can find a stale target's entry | pass the election, log and commit checks (17.3M distinct states, 40 minutes on a build box). With no write the Acked, Offset and Delivered checks have nothing to hold |
| `FelixRaftPlacementMoveWipeTwoMoves.cfg` | `FelixRaftPlacementMoveWipe.cfg` with a second target change, which can land while the first move's joint entry is uncommitted | expected to pass the same; not registered yet: past 30M distinct states and 45 minutes on a build box, and unfinished after six hours on a CI runner |
| `FelixRaftPlacementTransferMoveWipe.cfg` | `FelixRaftPlacementTransferMove.cfg` with one lost directory, no write | expected to pass the election, log and commit checks and `NeverLeadsRemoved`; not registered yet: past 34M distinct states and 45 minutes on a build box, and unfinished after six hours on a CI runner. The same with a write is issue #2497 |
| `FelixRaftPlacementDirectTerms.cfg` | four voters, three terms, two target changes, at least three voters, and openraft 0.10's direct change: one membership entry when every old majority meets every new one. A leader waits for its blank entry to commit before a membership call | expected to pass the election, log and commit checks; not registered yet: past 32M distinct states and 45 minutes on a build box, and unfinished after six hours on a CI runner |
| `FelixRaftPlacementDirectTermsNoBarrier.cfg` | the same where a leader proposes a membership before its blank entry commits | violate `StateMachineSafety`: an older leader's direct entry overwrites a committed one (20.2M distinct states, 22 minutes on a build box) |
| `FelixRaftPlacementStaleWipeRewind.cfg` | `FelixRaftPlacementStaleWipe.cfg` with one restore of the control plane's metadata from a backup and two target changes. The restore raises the incarnation, every broker acknowledges it before any target or doom, and a delete is void below the highest incarnation the broker has seen | pass the safety checks (46.9M distinct states, 4 h 35 min on a CI runner). The implementation does not have these rules yet (issue #2529) |
| `FelixRaftPlacementStaleWipeRewindSameInc.cfg` | the same where the restore keeps the incarnation | violate `AckedOnDisk` at depth 36: a delete told before the restore takes a copy a later target placed again (25.2M distinct states, 3 h 11 min on a CI runner). With one target change it passes, so the bound is two. Issue #2529 tracks the rules the code still lacks |
| `FelixRaftPlacementStaleWipeRewindNoAck.cfg` | the same where targets and dooms go out before every broker has acknowledged the new incarnation | expected to violate `AckedOnDisk`; not registered yet: unfinished after six hours on a CI runner |
| `FelixRaftPlacementStaleReturnWipe.cfg` | three brokers, one term, one lost disk: a seat is removed, placed back and removed again, and a leader may act on a replaced target. The doom rule is the report rule above | expected to pass the safety checks; not registered yet: unfinished in a CI dispatch so far |
| `FelixRaftPlacementStaleReturnWipeAnyVersion.cfg` | the same where a doom ignores the last target version that named the copy | expected to violate `AckedOnDisk` (not registered yet): a leader on the target that placed the copy back gives it its vote again, and the deletion takes a copy that holds an acknowledged write |
| `FelixRaftPlacementReachAck.cfg` | witness: three brokers whose membership is given at the start elect a leader and acknowledge a write | violate `NeverAcks` |
| `FelixRaftPlacementForm.cfg` | a first start of the metadata group: three empty members withhold their votes and each forms only when every other member answers empty and has recorded it; two terms, one write, one volume lost after the first commit | pass every check but `OffsetNeverReused`, which one write cannot fail (379K distinct states) |
| `FelixRaftPlacementFormWaiver.cfg` | the same with one slot waived (`form-without`) and its machine never started: two members form and run as two of three | pass the same (345 distinct states) |
| `FelixRaftPlacementFormSilent.cfg` | a member never starts and no waiver names it | pass `NobodyForms`. One state, since the rule's guard is what is checked |
| `FelixRaftPlacementFormWaiverMany.cfg` | the waiver names two of three slots and neither machine starts | pass `NobodyForms`: the empty answers are not a majority. One state |
| `FelixRaftPlacementFormWaiverAnyNumber.cfg` | the same where a waiver may name a majority | violate `NobodyForms` |
| `FelixRaftPlacementFormWaiverAnswers.cfg` | a group that exists, two terms, one write, one lost volume, while a waiver names a slot whose member is up and answers | pass the same checks as `Form` (195K distinct states) |
| `FelixRaftPlacementFormWaiverExcuses.cfg` | the same where a waived member's answer is skipped like its silence | violate `AckedHeldSurvive` |
| `FelixRaftPlacementFormAfterWipes.cfg` | a group that exists, three terms, one write, one volume lost after the first commit: the wiped member takes the first-start step again and cannot form over a member that holds the log | pass the same checks (1.9M distinct states) |
| `FelixRaftPlacementFormTwoWipes.cfg` | the same with two terms and two lost volumes: two wiped members do not form over a third that holds the log | pass `AckedHeldSurvive` only, since two lost volumes of three is past what the group survives (1.3M distinct states) |
| `FelixRaftPlacementFormMajority.cfg` | the same where a majority of empty answers is enough and silence is skipped, the rule before #2371 | violate `AckedHeldSurvive` |
| `FelixRaftPlacementFormAlias.cfg` | `FormAfterWipes` where an answer is counted for the address it came from, so two addresses that reach one member count it twice and a silent member reads as empty | violate `AckedHeldSurvive` |
| `FelixRaftPlacementFormEarlyWipe.cfg` | a group that exists, two terms, one write, one volume lost at any time, also before the first commit (#2374): the other members hold a record that the wiped member set out to form, so it does not form again | pass the same checks as `Form` (105K distinct states) |
| `FelixRaftPlacementFormEarlyWipeUnrecorded.cfg` | the same without the record (`FormRecord = FALSE`): every answer is empty, the member forms again and votes in a term it forgot | violate `AckedHeldSurvive` |
| `FelixRaftPlacementFormEarlyWipeFirstBoot.cfg` | `FormEarlyWipe` from a first start, so the volume can be lost between a member's mark, the others' records and its first vote | pass the same checks (991K distinct states) |
| `FelixRaftPlacementFormEarlyWipeTwo.cfg` | `FormEarlyWipe` with two lost volumes: the third member names both, and neither forms again | pass `OneLeaderPerTerm`, `LogMatching`, `AckedHeldSurvive`, `DeliveredAgree` (568K distinct states) |
| `FelixRaftPlacementFormEarlyWipeTwoUnrecorded.cfg` | the same without the record | violate `OneLeaderPerTerm` |
| `FelixRaftPlacementReachForm.cfg` | witness: three empty members form under the rule, elect a leader and acknowledge a write | violate `NeverAcks` |
| `FelixRaftPlacementReachFormWaiver.cfg` | witness: two members form with the third waived and never started | violate `NeverAcks` |
| `FelixRaftPlacementReachEarlyWipeVotesAgain.cfg` | witness: a member that lost its volume before the first commit catches up and votes again, and a write is acknowledged | violate `NeverEarlyWipedVotesAgain` |
| `FelixRaftReform.cfg` | `FelixRaftReform.tla`: three intact voters, the command plans, stops them, reads them again and writes epoch 1; two writes, one restart, one late append, no disk lost | pass `OneLeaderPerTerm`, `AckedSurvive`, `StateMachineSafety`, `NothingLost`, `OffsetNeverReused`, `ElectedGroupLeftAlone` (6.0M distinct states) |
| `FelixRaftReformCommits.cfg` | the same as a witness: a re-form completes and the new epoch commits | violate `NeverCommitsAfterReform` |
| `FelixRaftReformTwoRotated.cfg` | two of three disks lost or restored, each copy left with any prefix of its log, one write; rotated copies are listed and read | pass `OneLeaderPerTerm`, `AckedSurvive`, `StateMachineSafety` (2.4M distinct states) |
| `FelixRaftReformTwoRotatedFull.cfg` | the same with two writes | pass the same (40.6M distinct states) |
| `FelixRaftReformTwoRotatedOneVoter.cfg` | a witness: the group re-forms with one voter, which commits | violate `NeverOneVoterCommits` |
| `FelixRaftReformTwoRotatedOffsets.cfg` | the offset floor: two disks lost with acknowledged records on them, two writes, one term, the floor exact. When no copy holds the floor's entry, a copy whose saved commit index lags keeps its whole log, since its last log id has the floor's term | pass `AckedSurvive`, `OffsetNeverReused`, `LossNeedsLostDisk` (0.27M distinct states) |
| `FelixRaftReformTwoRotatedOffsetsCutToCommit.cfg` | the same where every log is cut to its saved commit index when no copy holds the floor's entry: a follower holds two acknowledged records with a lagging commit index and the base loses one, though no disk lost it | violate `AckedSurvive` (#2402) |
| `FelixRaftReformTwoRotatedOffsetsPads.cfg` | a witness: a base ends below the floor and is padded with a gap | violate `NeverPads` |
| `FelixRaftReformTwoRotatedOffsetsTrailing.cfg` | the same with the floor of the design, the last offset a leader reported | violate `OffsetNeverReused`: the accepted residual, pinned (issue #2305, Questions 13) |
| `FelixRaftReformTwoRotatedOffsetsNoFloor.cfg` | the same with no floor | violate `OffsetNeverReused` |
| `FelixRaftReformTwoRotatedOffsetsTerms.cfg` | the exact floor with two terms: the copy left ends in an uncommitted entry of the older term, at an offset the floor's term acknowledged with another record. The floor is a log id, no copy holds its entry, and the base is what the copy can vouch for | pass `OneLeaderPerTerm`, `AckedSurvive`, `StateMachineSafety`, `OffsetNeverReused` (17.0M distinct states) |
| `FelixRaftReformTwoRotatedOffsetsTermsIndexOnly.cfg` | the same with the floor an offset with no term, and the base never cut | violate `OffsetNeverReused` (#2402) |
| `FelixRaftReformTwoRotatedOffsetsTermsCuts.cfg` | a witness: no copy holds the floor's entry and the base is cut | violate `NeverCuts` |
| `FelixRaftReformTwoRotatedOffsetsTermsUnvouched.cfg` | a witness: the copy left holds a committed record of the older term above its saved commit index, cannot tell it from an uncommitted one, and the record is lost | violate `LossNeedsLostDisk` |
| `FelixRaftReformRejoin.cfg` | an apply stops one copy and does not finish, the group elects, and the copy rejoins as a learner under a new id; two disks lost, a second plan, one term per epoch | pass `OneLeaderPerTerm`, `AckedSurvive`, `StateMachineSafety`, `ElectedGroupLeftAlone` (0.25M distinct states) |
| `FelixRaftReformRejoinFull.cfg` | the same with two terms per epoch | pass `OneLeaderPerTerm`, `AckedSurvive`, `StateMachineSafety` (14.4M distinct states) |
| `FelixRaftReformRejoinHappens.cfg` | a witness: a stopped copy rotates and takes an append as a learner | violate `NeverRetiredRejoins` |
| `FelixRaftReformStaleMembershipIntact.cfg` | four brokers, {a, b, c} to {a, b, d} through a joint configuration, d placed before the leader proposes, no disk lost | pass `OneLeaderPerTerm`, `AckedSurvive`, `StateMachineSafety`, `NothingLost`, `OffsetNeverReused` (0.08M distinct states) |
| `FelixRaftReformStaleMembership.cfg` | the same with one disk lost or restored to any prefix | pass `OneLeaderPerTerm`, `AckedSurvive`, `StateMachineSafety` (1.4M distinct states) |
| `FelixRaftReformStaleMembershipHappens.cfg` | a witness: the change commits and the group then re-forms | violate `NeverReformsAfterChange` |
| `FelixRaftReformStaleMembershipTwoRotated.cfg` | the same with two disks lost: no copy of the old voters names d, and the command lists d because it is placed; once the change completes, c is still listed because it was placed in the epoch | pass `OneLeaderPerTerm`, `AckedSurvive`, `StateMachineSafety` (12.4M distinct states) |
| `FelixRaftReformStaleMembershipTwoMoves.cfg` | the same with a move and the move back: {a, b, c} to {a, b, d} and back to {a, b, c}; d is unplaced and can hold every entry while the copies of a, b and c are empty; not registered, run by dispatch | expected to pass the same. Not shown yet: no violation in the first 6.7M distinct states on the build box |
| `FelixRaftReformStaleMembershipTwoMovesForgotten.cfg` | the same with the list cut to the brokers placed now: d drops off when the move back commits, the empty copies name {a, b, c}, and the group re-forms from an empty base while d's disk holds the acknowledged entries | violate `AckedSurvive` |
| `FelixRaftReformStaleMembershipTwoRotatedUnlisted.cfg` | the same with d left off the list | violate `AckedSurvive` (issue #2311) |
| `FelixRaftReformStaleMembershipFull.cfg` | `FelixRaftReformStaleMembershipTwoRotated.cfg` with a client write; not registered, run by dispatch | expected to pass the same. Not shown yet: no violation in the first 59M distinct states of a run on the build box, which stopped under it |
| `FelixRaftReformStaleMembershipFullUnlisted.cfg` | the same with d left off the list | violate `AckedSurvive` (issue #2311). Its passing side is the row above, which is not shown yet |
| `FelixRaftReformLearnerWaited.cfg` | three voters and a placed learner d that no membership names; d holds a record that a and b lose with their disks | pass `OneLeaderPerTerm`, `AckedSurvive`, `StateMachineSafety` (1.3M distinct states) |
| `FelixRaftReformLearnerHolds.cfg` | the same with d left off the list | violate `AckedSurvive` (issue #2311) |
| `FelixRaftReformLearnerHoldsHappens.cfg` | a witness: the learner holds a record | violate `NeverLearnerHolds` |
| `FelixRaftReformOneReplica.cfg` | one voter whose disk is restored: with records it starts by itself, empty it is re-formed by the command; twice, with a restart and a late append | pass `OneLeaderPerTerm`, `AckedSurvive`, `StateMachineSafety` (7.6M distinct states) |
| `FelixRaftReformOneReplicaCommits.cfg` | a witness: the re-formed single voter commits | violate `NeverCommitsAfterReform` |
| `FelixRaftReformOneReplicaSelfStarts.cfg` | a witness: the restored copy starts by itself, with no plan, and commits | violate `NeverSelfStarts` |
| `FelixRaftReformOneReplicaLearner.cfg` | one voter and a placed learner d: the restored copy does not start by itself, and the command reads both | pass `OneLeaderPerTerm`, `AckedSurvive`, `StateMachineSafety` (0.02M distinct states) |
| `FelixRaftReformOneReplicaSoloPlacedTwo.cfg` | the same where the restored copy starts by itself although d is placed | violate `AckedSurvive` |
| `FelixRaftReformDead.cfg` | one broker declared dead and gone, one more disk may be lost; the command re-forms from the two it reaches | pass `OneLeaderPerTerm`, `AckedSurvive`, `StateMachineSafety` (1.8M distinct states) |
| `FelixRaftReformDeadReforms.cfg` | a witness: the group re-forms without the dead broker and commits | violate `NeverReformsWithoutDead` |
| `FelixRaftReformDeadLoses.cfg` | a witness: a record only the dead broker held is lost | violate `NothingLost` |
| `FelixRaftReformAbandon.cfg` | `Abandon`: one replica, its broker a declared dead, the log abandoned in one store step: the next epoch starts on b with an empty base, its first index above the floor, every earlier first index and the restore time line (an oracle here). Before that a's copy may rotate and re-form or start by itself; a can come back with its copy afterwards and does not open | pass `OneLeaderPerTerm`, `AbandonedNotLed`, `AckedSurvive`, `StateMachineSafety`, `OffsetNeverReused` |
| `FelixRaftReformAbandonHappens.cfg` | a witness: the abandoned epoch elects b and commits | violate `NeverAbandonCommits` |
| `FelixRaftReformAbandonOutside.cfg` | the dead check in a step before the write; a comes back with an acknowledged record in between | violate `AckedSurvive` |
| `FelixRaftReformAbandonAtFloor.cfg` | the first index just above the floor, with no time line; the floor trails the commits | violate `OffsetNeverReused` |
| `FelixRaftReformAbandonFromZero.cfg` | the blank copy forms the group from index 0 | violate `OffsetNeverReused` |
| `FelixRaftReformAbandonOpen.cfg` | a copy behind the abandoned epoch opens: a comes back and leads its old group | violate `AbandonedNotLed` |
| `FelixRaftReformWrongDeclaration.cfg` | two brokers declared dead are alive, and elect and acknowledge in the old epoch after the third re-formed | violate `AckedSurvive`: what the command cannot protect against, pinned |
| `FelixRaftReformBeforeAll.cfg` | the command goes on while a placed broker has not answered and is not declared dead | violate `AckedSurvive` |
| `FelixRaftReformEpochNotChecked.cfg` | a member takes a late append from the old epoch. It cannot cut the base out of the log: a member starting the new epoch saves the end of the base as its commit index before it takes any append (#2419), and the log refuses a cut below that | pass `AckedSurvive` (3.0M distinct states). It rests on that save alone, so the epoch check on appends is a second guard |
| `FelixRaftReformReportNotPromise.cfg` | the copies are read without being stopped, and the old epoch acknowledges a record the base lacks | violate `AckedSurvive` (issue #2304) |
| `FelixRaftReformPlanNotRechecked.cfg` | apply stops the copies and uses what the plan read instead of reading them again | violate `AckedSurvive` (issue #2304) |
| `FelixRaftReformRetireNotDurable.cfg` | two stopped voters restart, elect and acknowledge in the old epoch | violate `AckedSurvive` |
| `FelixRaftReformTwoRestarts.cfg` | the same bounds with the marker durable: one write, two restarts, two stopped voters restart and stay stopped | pass `OneLeaderPerTerm`, `AckedSurvive`, `StateMachineSafety`, `NothingLost`, `OffsetNeverReused`, `ElectedGroupLeftAlone` (0.13M distinct states) |
| `FelixRaftReformStopNotRechecked.cfg` | the group elects after the plan and apply stops its leader | violate `ElectedGroupLeftAlone` |
| `FelixRaftReformBaseNotHighest.cfg` | the base is a shorter log | violate `AckedSurvive` |
| `FelixRaftReformRotatedNotWaited.cfg` | a rotated copy is not read, and its disk holds the record | violate `AckedSurvive` (issue #2304) |
| `FelixRaftReformNoCas.cfg` | one replica; epoch 1 is written twice from epoch 0 with two bases | violate `StateMachineSafety` |
| `FelixRaftReformTwoReformsTrailingFloor.cfg` | `Kept`, two re-forms, one write per epoch (`TwoReformsShape`): an epoch-1 record committed by a leader and one follower, cut from both by power cuts, the floor trailing and the leader dead | pass `OneLeaderPerTerm`, `AckedSurvive`, `StateMachineSafety`, `OffsetNeverReused`, `KeptLossBound` (6.53M distinct states, depth 40, 8 min 40 s on 2 cores of a build box) |
| `FelixRaftReformTwoReformsTrailingFloorHappens.cfg` | witness: after both power cuts, the second re-form pads past its base and the floor by a power-cut copy's reservation alone, above its log and at an epoch-1 committed record | violate `NeverPadsByReserve` (9.5k distinct states, depth 18, 2 seconds) |
| `FelixRaftReformTwoReformsTrailingFloorLeaderReserve.cfg` | the same with only the leader reserving | violate `OffsetNeverReused` (396k distinct states, 36 seconds) |
| `FelixRaftReformTwoReformsTrailingFloorAllDead.cfg` | the same with both copies that counted the record dead: the documented residual of a trailing floor | violate `OffsetNeverReused` (408k distinct states, 31 seconds) |
| `FelixRaftReformTwoReformsEpochMark.cfg` | witness: an epoch-0 restore and a dead epoch-0 voter leave the second re-form unmarked, and it keeps an epoch-1 record | violate `NeverKeepsAcrossOldRestore` (84k distinct states, 6 seconds) |
| `FelixRaftReformTwoReformsOldRestore.cfg` | the same bounds as a pass, with a second write so epoch 2 writes too | pass the same as `...TrailingFloor.cfg` (380.6M distinct states, depth 43, 4 h 16 min on a CI runner); not registered for pull requests, runs nightly (`tla-walk.yml`) |
| `FelixRaftReformAlone.cfg` | `Kept` with `Alone = "whole"`: a standalone broker's group of one, one power cut that rotates the copy as a restore with a saved commit that can lag what was synced, and one self re-form | pass `OneLeaderPerTerm`, `AckedSurvive`, `StateMachineSafety`, `OffsetNeverReused`, `KeptLossBound` |
| `FelixRaftReformAloneCutToCommit.cfg` | the same with the usual vouch rule, which cuts the base at the lagging saved commit | violate `KeptLossBound` |
| `FelixRaftReformAloneTwoCuts.cfg` | the same as `FelixRaftReformAlone.cfg` with two power cuts, two re-forms and three plans: a cut can fall between the plan, the stop, the epoch write and the start, or after a finished re-form | pass the same as `FelixRaftReformAlone.cfg` (#3048) |
| `FelixRaftReformAloneTwoCutsHappens.cfg` | witness: a second power cut after a finished self re-form, and a second re-form that keeps an epoch-1 record and commits in epoch 2 | violate `NeverReformsAloneTwice` |
| `FelixRaftReformAloneCutBeforeStart.cfg` | witness: a power cut after the self re-form wrote epoch 1 and before its copy started, and the group re-forms again and commits | violate `NeverRecoversFromCutBeforeStart` |
| `FelixRaftReformKeptOldKept.cfg` | `Kept`, epoch 0 writes (`OldKeptShape`) over two terms: its leader is declared dead with two committed records, returns in epoch 1 with its epoch-0 copy, and the second re-form reads that copy | pass the same as `...TrailingFloor.cfg` (12.07M distinct states, depth 32, 12 min on a CI runner) |
| `FelixRaftReformKeptOldKeptCap.cfg` | witness: the epoch-1 cap lowers what the returned epoch-0 copy vouches for. It needs the second term: one dead voter of three does not mark epoch 0, so with one term the first re-form keeps everything the dead leader committed | violate `NeverCaps` (5.27M distinct states at the violation, depth 19, 6 min on a CI runner) |
| `FelixRaftReformKeptTerms.cfg` | `Kept`, one re-form over two terms of epoch 0 and one power cut, the group unmarked | pass the same as `...TrailingFloor.cfg` (82.3M distinct states, depth 34, 56 min on a CI runner); not registered for pull requests, runs nightly (`tla-walk.yml`) |
| `FelixRaftReformKeptTermsT.cfg` | witness: T keeps one copy whole and cuts another, longer one | violate `NeverVouchesByT` (335k distinct states, depth 12, 21 s) |
| `FelixRaftReformReplacedDisk.cfg` | `Kept`, one rotation that can replace the disk with nothing kept but the reservation (#2671), one broker declared dead, two terms | pass the same as `...TrailingFloor.cfg`. Not registered, run by dispatch |
| `FelixRaftReformReplacedDiskByBroker.cfg` | the same with a vote counted as read when any copy of its broker was read (`VoteByBroker`): A holds x at term 1, C commits f at the same index in term 2 with b, b's disk is replaced, C is declared dead | violate `OffsetNeverReused` (10.5M distinct states, depth 15, 5 min on 4 workers) |
| `FelixRaftReformReplacedDiskLost.cfg` | the same with the disk lost outright, reservation included (`ReplacedReserve <- LostReserve`), and one term: A commits x with b, A's disk is lost, b is declared dead | violate `OffsetNeverReused`: the documented residual of a trailing floor |
| `FelixRaftRestore.cfg` | `FelixRaftRestore.tla`: a restore from a backup point with no offset gap. Epoch 0 acknowledges up to four offsets, ahead of its commit and of the commit the copy saves, the point is read from the leader's acknowledged mark, its directory is copied while it still leads, the control plane is exported at any time, the cut saves the point as committed, and epoch 1 starts above the point, the restored floor and the copied reservation (step 2), with no offset gap | pass `BackedUpNeverReused`, `BaseKept`, `ReserveCovers`, `RestoresAtPoint` (25,737 distinct states, under a second) |
| `FelixRaftRestoreNoReserve.cfg` | the same with the first index leaving out the copied reservation | violate `BackedUpNeverReused` |
| `FelixRaftRestoreNoRaise.cfg` | the same where the cut only lowers the copy's saved commit | violate `RestoresAtPoint`: a copy that holds the point but saved its commit below it is refused at the point |
| `FelixRaftRestoreLeaderMoved.cfg` | the same where the copied copy stops leading after the point and falls behind its reservation before the copy | violate `BackedUpNeverReused`: an offset acknowledged before the copy is given out again. Closed by the gap |
| `FelixRaftRestoreAfterBackup.cfg` | `FelixRaftRestore.cfg` checked against every offset epoch 0 acknowledged | violate `OffsetNeverReused`: an offset acknowledged after both the copy and the export, the documented residual |
| `FelixRaftRestoreGap.cfg` | the restore offset gap: epoch 1 starts at least `Gap` = 2 past the point, at most two offsets are acknowledged after it, and the copied copy may stop leading | pass `BackedUpNeverReused`, `OffsetNeverReused`, `BaseKept`, `ReserveCovers`, `RestoresAtPoint` (72,328 distinct states, under a second) |
| `FelixRaftRestoreGapShort.cfg` | the same with a gap of 1 | violate `OffsetNeverReused` |
| `FelixRaftRestoreRepeated.cfg` | `FelixRaftRestoreRepeated.tla`: two backups and three restores, a backup may be taken from a restored epoch, each restore planned on a clock off true time by up to a tick; at most one offset per tick, the line rises two per tick, restores five ticks apart | pass `OffsetNeverReused` (5,650,196 distinct states, a few seconds) |
| `FelixRaftRestoreRepeatedNoTime.cfg` | pins the old rule, flipping both `Line` (off) and `EndGap` (on): the first index at the backup's end plus the gap, no time line. `...EndGap.cfg` is the one-change twin | violate `OffsetNeverReused`: a second restore from one backup starts where the first did |
| `FelixRaftRestoreRepeatedMinApart.cfg` | `...Repeated.cfg` with restores three ticks apart under a skew of one tick, below the four-times-skew boundary | violate `OffsetNeverReused`: a restore planned on a slow clock starts below an offset the earlier one gave out |
| `FelixRaftRestoreRepeatedEndGap.cfg` | the same with the gap added past the backup's end as well as past the line | violate `OffsetNeverReused`: a backup of a restored epoch restores above the line, and an older backup restored after it starts below offsets it gave out |
| `FelixRaftSwitch.cfg` | `FelixRaftSwitch.tla`: three copies that all hold the log up to `N`, one wipe at any step, two campaigns, one Raft write, every rule on | pass `LeaderCompleteness`, `StateMachineSafety`, `BaseAgree`, `OneBase`, `OneLeaderPerTerm` (2.3M distinct states) |
| `FelixRaftSwitchLag.cfg` | b may lag: a and c acknowledge, b starts empty as a learner and is promoted; no wipe | pass the same (1.4M distinct states) |
| `FelixRaftSwitchLagWipe.cfg` | the same with one wipe and no Raft write | pass the same (1.1M distinct states) |
| `FelixRaftSwitchWiped.cfg` | b lags and c is wiped before the control plane's write | pass the same and `NeverElects`: unavailable, not wrong (98 distinct states) |
| `FelixRaftSwitchGenZero.cfg` | any copy may carry generation 0 on a prefix of its records | pass the same (10.6M distinct states, 4 min) |
| `FelixRaftSwitchGenZeroFull.cfg` | the same with b lagging and promoted | pass the same (19.2M distinct states, 37 min in CI) |
| `FelixRaftSwitchStarts.cfg` | any copy may start at offset 2; the seal names `S`; b may lag and be promoted | pass the same (8.1M distinct states, 11 min in CI) |
| `FelixRaftSwitchDiffers.cfg` | a follower may hold a different record below `N`; it fails its digest and starts as a learner | pass the same (1.4M distinct states) |
| `FelixRaftSwitchReseal.cfg` | the hybrid opens again and is sealed a second time with the first report in flight; one wipe | pass the same (7.3M distinct states, 13 min in CI) |
| `FelixRaftSwitchAllVoters.cfg` | the whole set recorded as voters, b lagging and c wiped | violate `LeaderCompleteness`: b and the new c elect b without a record a and the old c acknowledged |
| `FelixRaftSwitchTermsBelowBase.cfg` | the hybrid log kept as the Raft log; b differs below `N` under the same generation | violate `LeaderCompleteness`: log matching takes b as level, it is promoted and leads |
| `FelixRaftSwitchTermsBelowBaseCuts.cfg` | a witness for the same rule in the generation 0 case | violate `NeverCutsHeldRecord`: a generation 0 learner cuts records it holds and takes the same content back |
| `FelixRaftSwitchNoDigest.cfg` | a follower acknowledges without the digest | violate `LeaderCompleteness`: a copy that differs below `N` votes and leads |
| `FelixRaftSwitchDigestFromOwnStart.cfg` | the digest runs from each copy's own first record | violate `LeaderCompleteness`: a copy that starts above `S` leads without the record at `S` |
| `FelixRaftSwitchDigestCoversGeneration.cfg` | the digest covers the generation field, b and c at generation 0 | pass the same and `NeverSwitches`: no majority ever acknowledges. Availability, not safety |
| `FelixRaftSwitchBlindWrite.cfg` | the control plane's write without the compare | violate `LeaderCompleteness`: the first seal's voters cut a write the reopened hybrid acknowledged |
| `FelixRaftSwitchBlindWriteTwoBases.cfg` | the same | violate `OneBase` |
| `FelixRaftSwitchStaleIdVotes.cfg` | a wiped copy still votes under its old id; b and c wiped after acknowledging | violate `LeaderCompleteness` |
| `FelixRaftSwitchReachCommit.cfg` | a witness for `FelixRaftSwitch.cfg` | violate `NeverCommitsAboveBase`: the switch completes and Raft acknowledges a record above `N` |
| `FelixRaftSwitchReachPromote.cfg` | a witness for `FelixRaftSwitchLag.cfg` | violate `NeverPromotes`: a learner's promotion commits |
| `FelixRaftSwitchReachPromotedLeads.cfg` | a witness for `FelixRaftSwitchLag.cfg` | violate `NeverPromotedLeads`: a promoted learner is elected |
| `FelixRaftSwitchReachInstall.cfg` | a witness for `FelixRaftSwitchLag.cfg` | violate `NeverInstalls`: a learner takes the records below the base from the leader |
| `FelixRaftSwitchReachGenZero.cfg` | `FelixRaftSwitchDigestCoversGeneration.cfg`'s copies with the rule on | violate `NeverElects`: the switch completes and a leader is elected |
| `FelixRaftSwitchReachWipedSwitch.cfg` | a witness for `FelixRaftSwitchWiped.cfg` | violate `NeverSwitches`: the control plane records the switch with c wiped |
| `FelixRaftSwitchReachReseal.cfg` | a witness for `FelixRaftSwitchReseal.cfg` | violate `NeverSwitchesAfterReseal`: the switch completes from the second seal |
| `FelixRaftSwitchInFlight.cfg` | the seal leader has a write in flight at the seal; followers catch up; every rule on | pass the above and `HybridAcksInBase`, `LeaderAcksInBase`, `NoAckAfterHandOver` (0.38M distinct states) |
| `FelixRaftSwitchInFlightClaimFirst.cfg` | the record at `N` before the write is answered (`ClaimBeforeQuiesce`) | violate `LeaderAcksInBase`: the write is acknowledged above `N` |
| `FelixRaftSwitchInFlightFailover.cfg` | `InFlight` with a failover after the lease lapses; the write may be a `Quorum` one | pass the same (3.3M distinct states, 6 min on 10 workers) |
| `FelixRaftSwitchInFlightFailoverLeaseAtEntry.cfg` | a `Leader` write acknowledged whatever the lease (`LeaseAtEntryOnly`) | violate `NoAckAfterHandOver` |
| `FelixRaftSwitchFailoverWipe.cfg` | `InFlightFailover` with c lagging and one wipe of any copy | pass the same (14.6M distinct states, 16 min on 10 workers) |
| `FelixRaftSwitchFailoverWipeVotes.cfg` | a wiped copy answers the failover's fence (`WipedVotesInFailover`) | violate `HybridAcksInBase`: the fence's furthest copy lacks a record a majority acknowledged |
| `FelixRaftSwitchPowerLoss.cfg` | the seal leader has an unsynced tail and loses it; a failover follows | pass `HybridAcksInBase` and the above (1.5M distinct states) |
| `FelixRaftSwitchPowerLossLag.cfg` | the same with c lagging | pass the same (1.5M distinct states) |
| `FelixRaftSwitchPowerLossLagAll.cfg` | the same where any copy, not only the seal leader, may lose its tail | pass the same (1.89M distinct states) |
| `FelixRaftSwitchPowerLossPeriodic.cfg` | the same on a `Periodic` log (`SealPeriodic`) | violate `HybridAcksInBase`: the seal leader loses a record only it and b held on disk, and the failover seals without it |
| `FelixRaftSwitchPowerLossSameGen.cfg` | the seal leader appends `N` again after losing its tail (`ResealSameGen`) | violate `HybridAcksInBase` |
| `FelixRaftSwitchPowerLossFollowers.cfg` | every copy may lose its tail once, before it starts: a follower after acknowledging, the leader after reporting, a broker between its vote floor and its mark | pass the above (0.19M distinct states) |
| `FelixRaftSwitchPowerLossAckBeforeSync.cfg` | `N` acknowledged and reported before it is on disk (`AckBeforeSync`) | violate `BaseAgree`: a recorded voter loses `N` and starts with a shorter log |
| `FelixRaftSwitchReachStartCrash.cfg` | a witness for `FelixRaftSwitchPowerLossFollowers.cfg` | violate `NeverRestartsBeforeMark`: a broker crashes between its vote floor and its mark |
| `FelixRaftSwitchCopySource.cfg` | a learner copies the records below the base from the leader, with purges past `N` | pass the above (3.3M distinct states, 8 min on 10 workers) |
| `FelixRaftSwitchCopySourceAny.cfg` | a learner copies from any member, uncommitted tail included (`CopyFromAny`) | violate `StateMachineSafety` |
| `FelixRaftSwitchDigestTag.cfg` | a follower differs only in a producer tag | pass the above (0.45M distinct states) |
| `FelixRaftSwitchDigestTagSkipped.cfg` | the digest leaves the tag out (`DigestSkipsTag`) | violate `LeaderCompleteness` |
| `FelixRaftSwitchDigestSide.cfg` | a follower's dead-letter and group-cursor logs lack the leader's state | pass the above and `SideComplete` (0.45M distinct states) |
| `FelixRaftSwitchDigestSideSkipped.cfg` | the digest leaves the side logs out (`DigestSkipsSide`) | violate `SideComplete` |
| `FelixRaftSwitchVoteFloor.cfg` | members start with the vote floor; two campaigns, one Raft write | pass the above (0.45M distinct states) |
| `FelixRaftSwitchVoteFloorOff.cfg` | no floor (`NoVoteFloor`) | violate `LeaderCompleteness` |
| `FelixRaftSwitchSealHolds.cfg` | a sealed shard does not open without the control plane | pass the above (0.45M distinct states) |
| `FelixRaftSwitchSealHoldsReopen.cfg` | the sealed leader reopens on a timeout (`ReopenOnTimeout`) | violate `HybridAcksInBase` |
| `FelixRaftSwitchSealHoldsBallot.cfg` | sealed copies elect a hybrid leader (`BallotIgnoresSeal`) | violate `HybridAcksInBase` |
| `FelixRaftSwitchStartOrder.cfg` | a voter purges before it writes its mark | pass the above and `NeverReadsBelowBase` (0.45M distinct states) |
| `FelixRaftSwitchStartOrderMarkFirst.cfg` | the mark before the purge (`MarkFirst`) | violate `NeverReadsBelowBase` |
| `FelixRaftSwitchReachInFlightAck.cfg` | a witness for `FelixRaftSwitchInFlight.cfg` | violate `NeverSwitchesAfterLeaderAck` |
| `FelixRaftSwitchReachFailover.cfg` | a witness for `FelixRaftSwitchInFlightFailover.cfg` | violate `NeverSwitchesAfterFailover` |
| `FelixRaftSwitchReachPowerLoss.cfg` | a witness for `FelixRaftSwitchPowerLoss.cfg` | violate `NeverSwitchesAfterFailover` |
| `FelixRaftSwitchEnds.cfg` | liveness under `FairSpec`: a write in flight, c lagging, one follower wiped, one follower's power cut | pass `SwitchEnds` (21K distinct states) |
| `FelixRaftSwitchEndsLeaderWiped.cfg` | the same with the seal leader wipeable | violate liveness: a seal leader wiped before it reports needs a failover, which the model leaves unfair |
| `FelixRaftLease.cfg` | `FelixRaftLease.tla`: the first version's lease, three brokers, two terms, clocks gaining 1 or 2 units a tick, lease 2 against an election timeout of 4, no write, no restart, no bound on time | pass `LeaseExclusive`, `OneLeaderPerTerm` (0.57M distinct states). With no restart it does not yet gate the lease pull request (issue #2307) |
| `FelixRaftLeaseAck.cfg` | one write, lease 1 against an election timeout of 2 | pass every Part 1 check in [Raft leader leases](#raft-leader-leases) (1.7M distinct states) |
| `FelixRaftLeaseAckWide.cfg` | one write at lease 2 against 4; not registered, run by dispatch | pass the same |
| `FelixRaftLeaseDeliver.cfg` | two writes and the subscriber reading across a change of leader, no drift, lease and election timeout of one tick | pass the same (9.8M distinct states, 3.5 min) |
| `FelixRaftLeaseRestart.cfg` | `FelixRaftLease.cfg` with one restart; a restarted broker refuses votes for one election timeout | pass `LeaseExclusive`, `OneLeaderPerTerm` (4.1M distinct states) on the node layer's vote hold (issue #2307, `Group::open` and `Group::dispatch` in `raft/group.rs`) |
| `FelixRaftLeaseRestartRenews.cfg` | the same with the hold off and openraft 0.10.0's rule on: at a start the window is renewed when the stored vote is a leader's whose message the member took | pass the same (3.5M distinct states) |
| `FelixRaftLeaseRestartForgets.cfg` | neither rule: a restarted follower may vote at once | violate `LeaseExclusive` (issue #2307) |
| `FelixRaftLeaseRestoreLed.cfg` | `FelixRaftLeaseRestart.cfg` with no drift, lease and election timeout of 1 tick, three terms and one write, and the hold skipped for campaigning by a restarted broker whose stored vote is its own committed one (`led`): it took no later leader's message, so it promised nobody, and its lease died with the process. As a voter it is still held | pass `LeaseExclusive`, `OneLeaderPerTerm`, `NoAckBehindNewLeader`, `AckedSurvive`, `StateMachineSafety` (6.2M distinct states) |
| `FelixRaftLeaseReachRestoreLed.cfg` | a witness for `FelixRaftLeaseRestoreLed.cfg` | violate `NeverHoldSkipped`: a restarted broker leads inside its hold |
| `FelixRaftLeaseRestoreAnyVote.cfg` | the same with the hold skipped for any stored vote | violate `LeaseExclusive`: a follower that answered the leader restarts, campaigns at once, and a voter outside the leader's window elects it |
| `FelixRaftLeaseRestoreSingle.cfg` | `FelixRaftLeaseRestoreLed.cfg` with one voter, whose lease needs no answer (`Servers` is fixed, so nobody else can lead) | pass the same |
| `FelixRaftLeaseRestoreNamed.cfg` | `FelixRaftLeaseRestoreLed.cfg` with two restarts, so two of the three brokers (a quorum) can be inside their hold at once, and a broker inside its hold granting the candidate its stored vote names: the leader of its current term | pass the same (15.4M distinct states) |
| `FelixRaftLeaseReachHeldGrant.cfg` | a witness for `FelixRaftLeaseRestoreNamed.cfg` | violate `NeverHeldGrant`: a broker inside its hold grants a vote |
| `FelixRaftLeaseRestoreNamedAny.cfg` | the same with the stored vote exempting any candidate, not only the leader it names (`HoldExemptsAny`) | violate `LeaseExclusive` |
| `FelixRaftLeaseRestoreTransferStepDown.cfg` | `FelixRaftLeaseTransferStepDown.cfg` with both restart fast paths, one restart, two terms, no write and `ElectionTimeoutMin` = 1 (the parent has 0, so a stepped-down leader campaigning in the same tick is not checked): a leader that authorized a transfer restarts with its own vote stored and its transfer wiped, and campaigns at once | pass `LeaseExclusive`, `OneLeaderPerTerm` (4.8M distinct states) |
| `FelixRaftLeaseRestoreTransferThreeTerms.cfg` | `FelixRaftLeaseRestoreTransferStepDown.cfg` with three terms, under the view `RestartView` | pass `LeaseExclusive`, `OneLeaderPerTerm` (2.7M distinct states under the view) |
| `FelixRaftLeaseRestoreTransferStepDownWrite.cfg` | `FelixRaftLeaseRestoreTransferStepDown.cfg` with one write, under `RestartView` | pass the same and `NoAckBehindNewLeader`, `AckedSurvive`, `StateMachineSafety` (15.3M distinct states under the view) |
| `FelixRaftLeaseRestoreTransferThreeTermsWrite.cfg` | `FelixRaftLeaseRestoreTransferThreeTerms.cfg` with one write; not registered, run on a build box | pass the same as the two-term one (191M distinct states under the view, 90 minutes on 16 cores) |
| `FelixRaftLeaseReachRestoreTransferThreeTerms.cfg` | a witness for it, with the second term won on the transfer (`SecondTermByTransfer`) | violate `NeverThirdTermOnHeldGrant`: the transfer's target leads a third term with a voter in that term inside its hold, a held voter having granted a vote, while the leader that started the transfer still leads the first |
| `FelixRaftLeaseReachRestoreTransferThreeTermsWrite.cfg` | the same with one write | violate `NeverThirdTermLocalAck`: that third-term leader acknowledges the write under its lease |
| `FelixRaftLeaseRestoreStepDown.cfg` | the same with three terms and no transfer (`ElectionTimeoutMin` = 1 as well): a leader that voided a term restarts with its void wiped and campaigns in a third term | pass `LeaseExclusive`, `OneLeaderPerTerm` (2.6M distinct states) |
| `FelixRaftLeaseStepDown.cfg` | `FelixRaftLease.cfg` with no drift, lease 2 against an election timeout of 2, the role read late, one step-down on a higher term (the leader adopts and grants the candidate's vote), openraft's grant to the leader a voter follows, `ElectionTimeoutMin` = 1 below the lease, the void on a higher term, three terms | pass `LeaseExclusive`, `OneLeaderPerTerm` (3.6M distinct states) |
| `FelixRaftLeaseReachLeaseAfterVoid.cfg` | `FelixRaftLeaseStepDown.cfg` with two terms | violate the witness `NeverLeaseAfterVoid`: a broker that voided a term passes the check in a later one |
| `FelixRaftLeaseStepDownNoVoid.cfg` | `FelixRaftLeaseStepDown.cfg` with no void | violate `LeaseExclusive` (issue #2249) |
| `FelixRaftLeaseStepDownSync.cfg` | the same with no void and the role read when the check runs | pass `LeaseExclusive`, `OneLeaderPerTerm` (0.39M distinct states) |
| `FelixRaftLeaseStepDownWrite.cfg` | lease 1 against an election timeout of 1, `ElectionTimeoutMin` = 0 (the stepped-down leader campaigns in the same tick), two terms, two step-downs and one write | pass the same, `NoAckBehindNewLeader`, `AckedSurvive`, `StateMachineSafety` (2.0M distinct states) |
| `FelixRaftLeaseReachStepDownLocalAck.cfg` | a witness for `FelixRaftLeaseStepDownWrite.cfg` | violate `NeverLocalAck`: a write is acknowledged on local durability |
| `FelixRaftLeaseStepDownDrift.cfg` | `FelixRaftLeaseStepDownWrite.cfg` with no write and clocks gaining 1 or 2 a tick, lease 1 against an election timeout of 2 | pass `LeaseExclusive`, `OneLeaderPerTerm` (0.46M distinct states) |
| `FelixRaftLeaseTransfer.cfg` | lease 2 against an election timeout of 2, no drift, `ElectionTimeoutMin` = 1, the role read late and one transfer: heartbeats stop, followers that take it clear their window, the target campaigns at once, the transfer may expire; Felix voids the lease before the send | pass `LeaseExclusive`, `OneLeaderPerTerm` (3.0M distinct states) |
| `FelixRaftLeaseTransferKeepsLease.cfg` | the same with no void | violate `LeaseExclusive` |
| `FelixRaftLeaseTransferVoidLate.cfg` | the same with the void a separate later step | violate `LeaseExclusive` |
| `FelixRaftLeaseTransferThreeTerms.cfg` | the transfer at lease 1 against an election timeout of 1, three terms: a broker that voided term 1 is elected in term 2 and votes in term 3 | pass `LeaseExclusive`, `OneLeaderPerTerm` (6.1M distinct states) |
| `FelixRaftLeaseTransferVoidEquality.cfg` | the same with the void one term, overwritten by the next and compared by equality | violate `LeaseExclusive` |
| `FelixRaftLeaseTransferStepDown.cfg` | `FelixRaftLeaseTransferThreeTerms.cfg` with `ElectionTimeoutMin` = 0 and one step-down as well | pass `LeaseExclusive`, `OneLeaderPerTerm` (16.8M distinct states, 38 min on a CI runner) |
| `FelixRaftLeaseTransferStepDownNoVoid.cfg` | the same with no void on a higher term | violate `LeaseExclusive` |
| `FelixRaftLeaseTransferWrite.cfg` | the transfer at lease 1 against an election timeout of 1, two terms and one write | pass the same, `NoAckBehindNewLeader`, `AckedSurvive`, `StateMachineSafety` (19.9M distinct states, 40 min on a CI runner) |
| `FelixRaftLeaseTransferDrift.cfg` | `FelixRaftLeaseTransferWrite.cfg` with no write and clocks gaining 1 or 2 a tick, lease 1 against an election timeout of 2 | pass the same (4.4M distinct states) |
| `FelixRaftLeaseSuspend.cfg` | lease 2 against an election timeout of 2, no drift, one suspend: the clock stops, the broker takes no step, and on waking it refuses for one lease | pass `LeaseExclusive`, `OneLeaderPerTerm` (0.38M distinct states) |
| `FelixRaftLeaseSuspendUnchecked.cfg` | the same with no wake check (also a VM pause) | violate `LeaseExclusive` |
| `FelixRaftLeaseSuspendLateCheck.cfg` | the same with the jump seen by its own step after the wake, as a background watch would | violate `LeaseExclusive` |
| `FelixRaftLeaseSuspendRoleLate.cfg` | the suspend at lease 1 against an election timeout of 1, the role read late and one write | pass the same, `NoAckBehindNewLeader`, `AckedSurvive`, `StateMachineSafety` (8.4M distinct states) |
| `FelixRaftLeaseSuspendDrift.cfg` | `FelixRaftLeaseSuspendRoleLate.cfg` with no write and clocks gaining 1 or 2 a tick, lease 1 against an election timeout of 2 | pass the same (2.8M distinct states) |
| `FelixRaftLeaseLong.cfg` | a lease of 3 against an election timeout of 4, one more than the inequality allows | violate `LeaseExclusive` |
| `FelixRaftLeaseDrift.cfg` | clocks gain up to 4 units a tick while the inequality was given 2 | violate `LeaseExclusive` |
| `FelixRaftLeaseFromAck.cfg` | the lease runs from when the answer came back, not from the hand-off | violate `LeaseExclusive` |
| `FelixRaftLeaseVoteInWindow.cfg` | a follower grants a vote inside its refusal window | violate `LeaseExclusive` |
| `FelixRaftLeaseAckLong.cfg` | `FelixRaftLeaseAck.cfg` with a lease of 2 against an election timeout of 2 | violate `NoAckBehindNewLeader` |
| `FelixRaftLeaseDeliverUncommitted.cfg` | the subscriber reads the leader's whole log | violate `DeliveredNeverReplaced` |
| `FelixRaftLeaseReachLocalAck.cfg` | witness: a write acknowledged on local durability under the lease | violate `NeverLocalAck` |
| `FelixRaftLeaseReachSoleLocalAck.cfg` | witness for `FelixRaftLeaseRestoreSingle.cfg`: the only voter acknowledges a write on its lease | violate `NeverLocalAck` |
| `FelixRaftLeaseReachRenewed.cfg` | witness: a lapsed lease makes a write wait for commit and that commit renews it | violate `NeverLapsedThenRenewed` |
| `FelixRaftLeaseReachFailover.cfg` | witness: a failover after the old lease ran out, the new leader passing the check | violate `NeverFailoverAfterLease` |
| `FelixRaftLeaseStore.cfg` | later design: store-liveness leases with the group asleep, three liveness heartbeats, one restart, epochs to 3, lease 1 against support of 2 | pass `LeaseExclusive`, `NoAckBehindNewLeader`, `OneLeaderPerTerm` (1.6M distinct states) on rules the design page does not state (issue #2307) |
| `FelixRaftLeaseStoreWide.cfg` | four liveness heartbeats | pass the same on the same rules (16.3M distinct states, 36 min in CI) |
| `FelixRaftLeaseStoreStall.cfg` | one broker's disk stalls, no restart | pass the same and `StalledLeaseLapses` on the same rules (1.2M distinct states) |
| `FelixRaftLeaseStoreNotDurable.cfg` | support is lost at a restart | violate `LeaseExclusive` |
| `FelixRaftLeaseStoreVote.cfg` | a vote is granted while support for the leader's store stands | violate `LeaseExclusive` |
| `FelixRaftLeaseStoreKept.cfg` | a restarted leader leads on and its lease check does not compare epochs | violate `LeaseExclusive` |
| `FelixRaftLeaseStoreResupport.cfg` | a withdrawal does not raise the epoch | violate `LeaseExclusive` |
| `FelixRaftLeaseStoreWithdrawEarly.cfg` | support is withdrawn before its expiry | violate `LeaseExclusive` |
| `FelixRaftLeaseStoreLong.cfg` | a lease of 2 against support of 2 | violate `LeaseExclusive` |
| `FelixRaftLeaseStoreStalledAsks.cfg` | a broker with a stalled disk keeps sending liveness heartbeats | violate `StalledLeaseLapses` |
| `FelixRaftLeaseStoreReachLocalAck.cfg` | witness: the first write after sleep acknowledged locally with no group message | violate `NeverLocalAck` |
| `FelixRaftLeaseStoreReachFailover.cfg` | witness: a failover after support was withdrawn, the new leader passing the check | violate `NeverFailoverAfterLease` |
| `FelixRaftLeaseSleep.cfg` | `FelixRaftLease.cfg` with sleep (issue #2239): up to two wakes not caused by the leader start a vote hold that may end early; ending it ends only the hold, and a sleep answer renews nothing | pass `LeaseExclusive` |
| `FelixRaftLeaseSleepHoldEndClearsWindow.cfg` | the hold's early end also clears openraft's refusal window | violate `LeaseExclusive` |
| `FelixRaftLeaseSleepAnswerRenews.cfg` | a sleep answer renews the lease, though the follower's window did not restart | violate `LeaseExclusive` |
| `FelixRaftSleep.cfg` | The handshake with every rule on: a write, an audit, a lost request or answer, a restart, a false alarm about the leader | pass on two rules the design page states and nobody has built (issue #2310) |
| `FelixRaftSleepWrite.cfg` | Writes arriving during the handshake are committed and no member sleeps behind | pass on two rules the design page states and nobody has built (issue #2310) |
| `FelixRaftSleepLeaderDies.cfg` | The leader's broker dies; liveness alone (audit off) elects a leader | pass on two rules the design page states and nobody has built (issue #2310) |
| `FelixRaftSleepLivenessMisses.cfg` | Liveness misses the death; the audit alone elects a leader | pass on two rules the design page states and nobody has built (issue #2310) |
| `FelixRaftSleepFalseAlarm.cfg` | A false alarm about a healthy leader deposes nobody, given that the ask arrives and the hold outlasts the answer | pass on two rules the design page states and nobody has built (issue #2310). It does not test that the hold is long enough: the model has no clock and a hold ends only when no live leader could still answer |
| `FelixRaftSleepRestart.cfg` | A restarted member holds until it hears the leader, given that the hold outlasts the leader noticing the new epoch | pass on two rules the design page states and nobody has built (issue #2310). It does not test that the hold is long enough: the model has no clock and a hold ends only when no live leader could still answer |
| `FelixRaftSleepAnyState.cfg` | Members fall asleep in any state with no handshake; safety only | pass |
| `FelixRaftSleepEnds.cfg` | Every behaviour ends (temporal, no symmetry) | pass on two rules the design page states and nobody has built (issue #2310) |
| `FelixRaftSleepNoWake.cfg` | Neither wake path: the group never elects | violate `QuiescentLed` |
| `FelixRaftSleepCampaignOnWake.cfg` | A woken follower campaigns without asking | violate `NoNeedlessDeposal` |
| `FelixRaftSleepRestartNoHold.cfg` | A restarted member ticks at once | violate `NoNeedlessDeposal` |
| `FelixRaftSleepLeaderSleepsFirst.cfg` | The leader sleeps before the followers answer | violate `NoNeedlessDeposal` |
| `FelixRaftSleepAcceptUnchecked.cfg` | A sleep request accepted without comparing the log position | violate `AsleepIsLevel` |
| `FelixRaftSleepHeartbeatKeepsProposal.cfg` | A heartbeat during the handshake leaves the proposal open | violate `NoNeedlessDeposal` |
| `FelixRaftSleepHoldTooShort.cfg` | A hold shorter than the ask's round trip | violate `NoNeedlessDeposal` |
| `FelixRaftSleepSleepUnsupported.cfg` | A follower sleeps for a leader it no longer supports | violate `QuiescentLed` |
| `FelixRaftSleepFalseAlarmFollower.cfg` | Design as written: a false alarm about a follower lets the leader sleep without it | violate `NoNeedlessDeposal` (issue #2310) |
| `FelixRaftSleepFalseAlarmFollowerBehind.cfg` | Design as written: the same alarm leaves the whole group asleep with a member behind | violate `AsleepIsLevel` (issue #2310) |
| `FelixRaftSleepAskLost.cfg` | Design as written: the ask after a false alarm is lost, the hold runs out and the follower deposes a healthy leader | violate `NoNeedlessDeposal` (issue #2320) |
| `FelixRaftSleepReachWakeOnWrite.cfg` | Witness: a group sleeps, wakes on a write and sleeps again | violate `NeverWakesOnWrite` |
| `FelixRaftSleepReachAuditElection.cfg` | Witness: an election through the audit alone | violate `NeverElectsAfterDyingAsleep` |
| `FelixRaftSleepWake.cfg` | The second design on issue #2239, every rule on: pre-vote, the vote hold after a wake, the request to a left-out member, the waking refusal, the audit that asks first, a lease that lapses in sleep. One write, one sleep, one lost audit heartbeat, one false alarm, one restart | pass |
| `FelixRaftSleepWakeWrite.cfg` | A write reaches a sleeping leader whose lease lapsed; it waits for a heartbeat round | pass `NoWriteRefused`, `QuiescentLevel` |
| `FelixRaftSleepAuditLost.cfg` | A lost audit heartbeat fires a follower's audit under a healthy leader; it asks first | pass `NoNeedlessDeposal` |
| `FelixRaftSleepFalseAlarmFollowerAsked.cfg` | `FelixRaftSleepFalseAlarmFollower.cfg` with the second design's rules | pass `NoNeedlessDeposal` |
| `FelixRaftSleepFalseAlarmFollowerAskedLost.cfg` | the same with one request or answer lost, so the left-out member stays awake under a sleeping leader | pass `NoNeedlessDeposal`: its pre-vote wakes the others, who hold |
| `FelixRaftSleepFalseAlarmFollowerBehindAsked.cfg` | `FelixRaftSleepFalseAlarmFollowerBehind.cfg` with the second design's rules | pass `AsleepIsLevelCounted`, `QuiescentLevel` |
| `FelixRaftSleepTtl.cfg` | A cache group's expiry timer wakes a sleeping leader, which proposes the delete; no audit | pass `QuiescentTtl` |
| `FelixRaftSleepMidHandshake.cfg` | The leader's broker restarts or dies during the handshake, one request or answer lost | pass |
| `FelixRaftSleepWriteBeforeRound.cfg` | `FelixRaftSleepWakeWrite.cfg` with the write proposed at once after the wake; openraft refuses it (LeaseExpired) | violate `NoWriteRefused` |
| `FelixRaftSleepAuditCampaigns.cfg` | `FelixRaftSleepAuditLost.cfg` with a follower that ticks at once after its audit | pass: the vote hold drops its pre-vote. The ask saves a round; the hold keeps the leader |
| `FelixRaftSleepFalseAlarmFollowerAskedLostDroppedPreVoteGrants.cfg` | a pre-vote the hold drops counts as granted, as openraft's default `pre_vote` answers | violate `NoNeedlessDeposal` |
| `FelixRaftSleepFalseAlarmFollowerAskedLostNoVoteHold.cfg` | no vote hold: a sleeper woken by a pre-vote grants it | violate `NoNeedlessDeposal` |
| `FelixRaftSleepFalseAlarmFollowerAskedLostNoPreVote.cfg` | no pre-vote: the member raises its term | violate `NoNeedlessDeposal` |
| `FelixRaftSleepFalseAlarmFollowerAskedLostNoLeftOutAsked.cfg` | no request to the left-out member | pass: the vote hold keeps the leader |
| `FelixRaftSleepFalseAlarmFollowerBehindAskedNoRefusalWakes.cfg` | a refusal does not wake the leader | pass: the left-out member is caught up later |
| `FelixRaftSleepNoTtlWake.cfg` | `FelixRaftSleepTtl.cfg` with a timer that does not wake the leader | violate `QuiescentTtl` |
| `FelixRaftSleepReachMidHandshake.cfg` | Witness for `FelixRaftSleepMidHandshake.cfg` | violate `NeverElectedMidHandshake`: a leader is elected with the old one's request still out |
| `FelixRaftSleepTtlWrite.cfg` | `FelixRaftSleepTtl.cfg` with two writes: one can reach the leader between the expiry timer and its delete, which names the version the timer saw | pass `ExpiryKeepsNewer` |
| `FelixRaftSleepTtlWriteUnconditional.cfg` | the same with a delete that removes whatever the key holds | violate `ExpiryKeepsNewer` |
| `FelixRaftSleepTtlWriteReachRace.cfg` | Witness for `FelixRaftSleepTtlWrite.cfg` | violate `NeverExpirySkipsNewer`: the delete commits after a write its timer did not see |
| `FelixRaftCache.cfg` | `FelixRaftCache.tla`: the cache shard's state machine (issue #2238), two members, an add and a put-if-absent, three proposals with retries, one leader change, one crash, two snapshots, one read, no clock | pass |
| `FelixRaftCacheClock.cfg` | expiry, with clocks 0..2 that differ by member, an expiring put, a put-if-absent and one expiry delete; no snapshot | pass |
| `FelixRaftCacheDedup.cfg` | retries of a put, a put-if-absent and a delete; three proposals, one crash, one snapshot | pass `AppliedOnce`, `AnswersKept` |
| `FelixRaftCacheExpiry.cfg` | the expiry delete against a put that refreshes the key; clocks 0..1 | pass `ExpiryRemovesItsVersion` |
| `FelixRaftCacheClockSnapshot.cfg` | expiry with a snapshot, an install and a crash; clocks 0..2 | pass `StampsMonotone` |
| `FelixRaftCacheNoRequestId.cfg` | adds are applied again on a retry | violate `SumExact`: a retry counts twice |
| `FelixRaftCacheDedupPutNotDeduped.cfg` | puts are applied again on a retry | violate `AnswersKept`: the retry overwrites a later write and answers a new version |
| `FelixRaftCacheDedupPutAbsentNotDeduped.cfg` | put-if-absents are applied again on a retry | violate `AppliedOnce`: the retry brings back a deleted key |
| `FelixRaftCacheDedupDeleteNotDeduped.cfg` | deletes are applied again on a retry | violate `AppliedOnce`: the retry removes a later write |
| `FelixRaftCacheSnapshotDropsDedup.cfg` | the snapshot leaves out the applied request ids | violate `SumExact` |
| `FelixRaftCacheSnapshotIdAhead.cfg` | the snapshot's id is read after its state | violate `MembersAgree` |
| `FelixRaftCachePurgePastSnapshot.cfg` | purge goes up to what the member applied | violate `LiveReadable` |
| `FelixRaftCacheReadNoApplyWait.cfg` | a read is answered before the leader applied its commit index | violate `ReadsFresh` |
| `FelixRaftCacheClockApplyLocalClock.cfg` | apply checks expiry on the member's clock | violate `MembersAgree` |
| `FelixRaftCacheClockReadsLocalClock.cfg` | a read checks expiry on the leader's clock and answers without the delete | violate `OneClock` |
| `FelixRaftCacheClockReadsLogTime.cfg` | a read checks expiry at the log's time only | violate `ReadsNotStale`: an idle group reads an expired key as present |
| `FelixRaftCacheClockStampNotMonotone.cfg` | a leader stamps its own clock even when behind | violate `OneClock` |
| `FelixRaftCacheClockFloorFromCommitted.cfg` | the stamp floor ignores the uncommitted tail a new leader inherited | violate `StampsMonotone` |
| `FelixRaftCacheClockFloorCountsBlank.cfg` | the stamp floor reads a blank entry's time | violate `StampsMonotone` |
| `FelixRaftCacheExpiryUnconditional.cfg` | the expiry delete removes whatever the key holds | violate `ExpiryRemovesItsVersion`: a put that committed before the delete is lost |
| `FelixRaftCacheClockSnapshotDropsTime.cfg` | the snapshot leaves out the log's time | violate `StampsMonotone`: a leader that installed it stamps from its own clock |
| `FelixRaftCacheReachInstallRead.cfg` | witness: a member installs the leader's snapshot, then leads and serves a read | violate `NeverReadAfterInstall` |
| `FelixRaftCacheReachRetry.cfg` | witness: a retried add is committed twice and applied | violate `NeverRetryApplied` |
| `FelixRaftCacheDedupReachRetryAnswer.cfg` | witness: a request is committed twice and then answered | violate `NeverRetryAnswered` |
| `FelixRaftCacheClockReachExpiry.cfg` | witness: a read finds the key expired, then a put-if-absent takes it | violate `NeverExpiredThenTaken` |
| `FelixRaftCacheExpiryReachRefresh.cfg` | witness: an expiry delete commits after a newer write and leaves it | violate `NeverExpirySkipsNewer` |
| `FelixRaftCacheExpiryReachExpireRead.cfg` | witness: a read waits for the expiry delete and answers absent | violate `NeverReadAfterExpiry` |
| `FelixRaftCacheClockSnapshotReachInstallStamp.cfg` | witness: a leader that installed a snapshot stamps from the snapshot's time, ahead of its clock | violate `NeverStampFromSnapshot` |
| `FelixRaftCacheDelete.cfg` | a delete against expiry: an expiring put and a delete, three proposals (a retry moves the log's time), one expiry delete, clocks 0..2, one read | pass `OneClock` (6.3M distinct states) |
| `FelixRaftCacheDeleteSnapshot.cfg` | a snapshot that holds live records only (`SnapshotDropsExpired`): an expiring put and a delete, three proposals, one expiry delete, clocks 0..1, one crash, one snapshot, one read | pass `AnswersKept`, `MembersAgree` (31.2M distinct states) |
| `FelixRaftCachePutAbsentSnapshot.cfg` | the same with a put-if-absent in place of the delete | pass (28.4M distinct states) |
| `FelixRaftCacheCond.cfg` | a put and a delete that name a version, against an expiring put; three proposals, one expiry delete, clocks 0..1, one read | pass `CondExact`, `OneClock` (1.3M distinct states) |
| `FelixRaftCacheCondSnapshot.cfg` | `FelixRaftCacheDeleteSnapshot.cfg` with a conditional put in place of the delete: a refusal answers the same version on a member that restored the snapshot and on one that replayed the log | pass `AnswersKept`, `ProjectionIsFold` (19.5M distinct states) |
| `FelixRaftCacheDeleteSeesExpired.cfg` | a delete treats an expired record as present | violate `OneClock`: a read answers absent, then the delete removes a version |
| `FelixRaftCacheDeleteSnapshotSeesExpired.cfg` | the same with a snapshot that dropped the record | violate `AnswersKept`: a member that replayed the log removes a version, one that restored the snapshot answers absent |
| `FelixRaftCachePutAbsentSnapshotSeesExpired.cfg` | a put-if-absent treats an expired record as present, with a snapshot that dropped it | violate `MembersAgree` |
| `FelixRaftCacheCondSeesExpired.cfg` | a conditional write matches the version of an expired record | violate `OneClock` |
| `FelixRaftCacheCondSnapshotSeesExpired.cfg` | the same with a snapshot that dropped the record | violate `AnswersKept`: a member that replayed the log applies the write, one that restored the snapshot refuses it |
| `FelixRaftCacheDeleteReachExpired.cfg` | witness: a delete finds an expired record that no expiry delete removed and answers absent | violate `NeverDeleteOfExpired` |
| `FelixRaftCacheDeleteSnapshotReachDropped.cfg` | witness: a member restores a snapshot that left out an expired record while another still holds it | violate `NeverDroppedAndKept` |
| `FelixRaftCacheCondReachApplied.cfg` | witness: a conditional write applies | violate `NeverCondApplied` |
| `FelixRaftCacheCondReachStale.cfg` | witness: a conditional write is refused because a newer write replaced the version it named | violate `NeverCondStale` |
| `FelixRaftCacheTrim.cfg` | the applied-id table trimmed by entries: an add and a put from one producer, each entry carrying the producer's floor, one retire, four proposals (retries and late copies included), one leader change, one crash, two snapshots | pass `ForgetsOnlyDone`, `RetriesKeepAnswers`, `AppliedOnce`, `MembersAgree` (63.5M distinct states, depth 36, 39 min on a 32-core build box) |
| `FelixRaftCacheTrimRetireDropsFloor.cfg` | retire forgets the producer, floor included | violate `AppliedOnce`: a late copy of the add commits after the retire and is applied again |
| `FelixRaftCacheTrimSnapshotDropsFloor.cfg` | the snapshot keeps the answers and leaves out the floor | violate `MembersAgree`: in six steps a member restores a snapshot holding floor 0 while the others hold floor 1. The check compares state, so this is the difference itself; the trace does not go on to a retry that the two members answer differently |
| `FelixRaftCacheTrimRetireLocal.cfg` | the leader forgets a retired producer itself, with no entry | violate `MembersAgree` |
| `FelixRaftCacheTrimFloorPastUnanswered.cfg` | the producer sends its own sequence as the floor, past a request it has no answer for | violate `ForgetsOnlyDone`: that request commits later and is answered "forgotten", never applied |
| `FelixRaftCacheTrimNoFloorClamp.cfg` | the producer sends its sequence plus one as the floor and apply does not clamp it to the entry's sequence | violate `ForgetsOnlyDone`: the first copy drops its own answer, and a retry that commits before the answer reaches the producer is answered "forgotten" |
| `FelixRaftCacheFull.cfg` | the state bound (issue #2460): a bound of one, an add, a delete and a put, four proposals with retries, one leader change, one crash, one snapshot. The second of the add and the put answers "full", and a retry after the delete gets the same answer | pass `FullFinal`, `SumExact`, `AnswersKept` |
| `FelixRaftCacheFullSnapshot.cfg` | the bound with a snapshot that holds live records only: an expiring put and an add, clocks 0..1, one crash, one snapshot. A member that restored the snapshot and one that replayed the log judge the add the same | pass `MembersAgree`, `ProjectionIsFold` |
| `FelixRaftCacheFullNotRecorded.cfg` | a "full" answer is not kept | violate `FullFinal`: a retry after the delete applies the add its first commit refused |
| `FelixRaftCacheFullSnapshotRawSize.cfg` | the bound counts an expired record no expiry delete removed yet | violate `MembersAgree`: the member that replayed the log answers "full", the one that restored the snapshot applies the add |
| `FelixRaftCacheFullReachFull.cfg` | witness: a write is answered "full" | violate `NeverFull` |
| `FelixRaftCacheTrimReachForgotten.cfg` | witness: a late copy below the floor answers "forgotten" | violate `NeverForgotten` |
| `FelixRaftCacheTrimReachRetryAboveFloor.cfg` | witness: a retry commits after the floor passed the other request and keeps its answer | violate `NeverRetryAboveFloor` |
| `FelixRaftCacheTrimReachRetryAfterRetire.cfg` | witness: a late copy commits after the retire entry | violate `NeverRetryAfterRetire` |
| `FelixRaftCacheTrimReachInstalledFloor.cfg` | witness: a member installs a snapshot whose floor passed a request | violate `NeverInstalledFloor` |

Drift is checked where it matters and nowhere else. The lease configurations
carry drifting clocks and no writes, so every interleaving of three drifting
clocks is affordable; most replication configurations carry writes and
synchronised clocks. `FelixShardRealMargins.cfg`, `FelixShardAckWithoutLease.cfg`
and the fenced pair carry both, bought with one write and time to 5, or two
writes and one promotion.

### One shard is one log, across a delete and recreate

The model follows one shard's log, and its invariants lean on two things about
it: a generation never repeats, and everything a broker keeps per shard and
generation (ballots, the accepted generation, quorum marks, follower positions,
reports) describes that log. A stream deleted and created again under its name
reuses the shard keys, so stream incarnations keep both true rather than adding
anything to the model:

- each incarnation has its own directories, so its ballot, replica state and
  epochs start empty, and a follower refuses replication for an incarnation it
  does not hold (`a_follower_refuses_an_incarnation_it_does_not_hold`,
  `a_recreate_between_the_check_and_the_open_is_refused`);
- an incarnated stream's first generation is the change seq that assigns it,
  above every generation an earlier incarnation reached on one control-plane
  timeline (`a_recreated_stream_never_repeats_a_generation`). A restore
  rewinds the seq, so the generation alone does not separate the two runs:
  the leader keys follower positions and quorum marks by incarnation as well,
  so a mark or position kept for the deleted stream never counts for the new
  one even at an equal generation
  (`a_recreate_at_the_same_generation_does_not_inherit_the_mark`). A broker
  compares the incarnation before the generation, so a restore that rewinds
  the seq still opens the new stream as a new run rather than dropping it as
  stale (`a_new_incarnation_opens_at_a_lower_generation`);
- a stream from before incarnations, deleted before the finalize and created
  again at incarnation 0, reopens the old logs and restarts its shards at
  generation 0, as every release before incarnations did. The model does not
  cover a generation going back on one log; that case is unchanged here and
  ends once the name is deleted after the finalize.

Each incarnated stream is then one run of the model from an empty log, with
none of the last run's per-shard state. Nothing in the spec names the
incarnation.

Cache shards (the cache log and the counter log riding it, which
`FelixShardFencedCache.cfg` covers) get the same treatment once
`cache_incarnations` is finalized: their own directories per incarnation, a
first generation from the change seq, replication state keyed by incarnation,
and a follower that refuses a cache incarnation it does not hold
(`a_cache_follower_refuses_an_incarnation_it_does_not_hold`,
`a_cache_recreate_between_the_check_and_the_open_is_refused`). Each
incarnated cache is one run of the model too.

### What ties this to the code, and what does not

A spec and an implementation are two artifacts in two languages. Nothing in the
toolchain makes one follow the other, and the gap is not hypothetical: the
broker gained the report-before-mark ordering in #268, this model went on
describing the design without it, and `check_tla.sh` pinned the resulting
`AckedSurvive` violation as *expected*, asserting for three weeks that Felix
loses acknowledged records, for a design it no longer had. An issue was then
filed against the model's finding, proposing work the code did not need.

So every configuration carries an `Evidence:` block naming the tests that
establish what it assumes of the implementation, and
`scripts/check_spec_evidence.py` (run by `task docs:evidence`) fails when a
cited test no longer exists or a configuration cites nothing. Rename the test
for a behaviour and the spec is put in front of you.

A configuration that deliberately models something the code does *not* do says
`Evidence: none` and why; the counterexample configurations instead cite the
test proving the check they remove is really there.

Citations do not catch the change that actually drifted: #268 changed the
protocol without renaming a cited test. So a pull request that touches the
code this model describes (`services/felix-broker-service/src/{cluster/lease,cluster/membership,serving,shards/lifecycle,shards/routing}`,
`crates/server/felix-replication/src`, the replica's persisted promise in
`crates/server/felix-storage/src/disk_log/{ballot,replica_state,epochs}`
(`DiskLog::accept_generation` and the open's choice between the ballot and
the replica state live in `ballot.rs` so they are covered), and
`services/felix-controlplane-service/src/{api/nodes/reports,cluster/membership,cluster/placement}`,
tests and metrics aside) must also touch `docs/formal/`, or carry a line

```
Spec-Unaffected: <why>
```

in a commit message or the PR description. `scripts/check_spec_pairing.py`
enforces it in CI (`task tla:pairing BASE=origin/main` locally). It is blunt on
purpose: most edits to those files are not protocol changes, and the marker is
how you say so. What it buys is that nobody changes the protocol without being
asked whether the model still describes it.

**What this does not do.** A cited test can keep its name while its assertions
change, and the spec can model a behaviour wrongly while every citation
resolves. This makes drift harder to introduce silently; it does not detect it.
Checking that the implementation *conforms* to the spec needs trace validation
(emitting protocol events and checking recorded runs are behaviours of the
spec), which is a different and much larger mechanism.

**Trace validation is not planned.** It needs the broker and control plane to
emit protocol events behind a test-only feature, a mapping from those events
onto the spec's variables, and TLC in trace mode in CI. That is a project rather than a
check. And what it buys is bounded: it shows the runs the tests happened to
make are behaviours the spec permits, and says nothing about paths no test
exercises. The drift that actually occurred (#268) is what the two checks
above catch. Worth revisiting if the protocol grows another mechanism of the
size of the planned handoff, or if drift gets past both checks once.

### Two planners, one read

`FelixShardStalePlanner.cfg` finds this. One instance reads the shard while
its leader is live and its report lists both followers caught up, and holds
that read. The other fences towards one follower, the leader reports drained,
and it cuts over. The held read's fence lands next: it names the old leader
again, at a new generation, with the other follower as successor. That leader
reports drained, and the cut-over to the second follower lands while the first
still holds a live lease. `FelixShardStalePromotion.cfg` is the failover
version: two promotions from one report of two caught-up followers. With
`CasWrites`, each late write finds a newer generation and writes nothing.

### Taking back a fenced move

An operator's cancel of a fenced move names the old leader again at a new
generation. That is safe for the same reason a cut-over back to the leader
is: nobody else has led since the fence, so the leader's log is the whole
shard, and the writes still inside its fence were admitted against that log
and land in it. `FelixShardCancel.cfg` checks it with writes acknowledged on
admission, the broker's default; dropping the queued writes at the retake, as
a promotion does for a node that never led, makes TLC find an acknowledged
write missing at once.

What makes it unsafe is timing, and the conditional write is what rules it
out. `FelixShardCancelStalePlanner.cfg` decides the cancel from a read taken
while the move was fenced, lets the other instance cut over, then lands the
cancel: the old leader serves beside the new one. With `CancelCas`, the late
cancel writes nothing, and the API decides it again from a fresh read, which
finds nothing to cancel.

Time stops short of any lease lapse in these configurations. A leader whose
lease lapses drops what it acknowledged on admission whether or not a move
is cancelled; that is the acknowledge-on-admission trade-off, not the
cancel's.

A retake keeps the leader's log, and with it the producer sequences its
records carry, so a write re-sent after a cancel is answered from there.
`FelixShardCancelResend.cfg` adds re-sends to `FelixShardCancel.cfg` and
reaches exactly the same states: every re-send is answered, none appends.
It runs two writes, unlike the other re-send configurations, because no
lease lapses and so no deposed leader keeps a stale copy. In
`FelixShardCancelResendMemory.cfg` the retaken leader, now at a new
generation, knows none of what it wrote before and stores the re-sent write
twice.

### A fence before the destination is level

Placement fences a move once the destination is within
`FELIX_SHARD_MOVE_FENCE_MAX_LAG_RECORDS` of the leader's tail, because under
steady writes it may never be exactly level. The model's `Fence` asks nothing
of the destination's position at all, so every bound the code may use is
covered: the cut-over still waits for a drained report naming the destination
level, and that is what keeps an acknowledged write on whoever leads next.

### Pacing across shards

`FelixPlacementPacing.tla` is a second, much smaller model: many shards, no
records, only the copies placement starts and finishes. It checks the move
limits hold when every copy is one the store names -- a move's `successor`, a
replacement's `joining` -- and that a fence stands only on a move in flight.
A fenced leader that never reports drained serves again after
`FELIX_SHARD_FENCE_TIMEOUT_MS` (`FenceTimeOut`), which ends the move and its
fence together; whether that can race a cut-over is the shard model's
`Retake` under `CancelCas`.
`FelixPlacementPacingUncountedReplacement.cfg` writes a replacement the way it
used to be, with nothing in the assignment saying a copy is running, and TLC
finds a move starting beside it under a limit of one.

It also models several instances planning at once. Each planner starts
copies from its own read: the lease holder, an instance that took a read
while it held the lease and has since lost it (the lease may change hands at
any step, which is expiry under a pause), and an operator's request, which
reads without the lease. A start is conditional on its shard being as read,
the generation check, and with `Fenced` on the placement token being
unchanged since the read, counting the planner's own writes. Every write
and every change of holder advances the token. `FelixPlacementPacingTwoPlanners.cfg`
holds the limit with the token; `FelixPlacementPacingUnfenced.cfg` drops it,
and TLC finds two planners each starting a copy on a different shard from a
read with one slot free, which is what two Postgres-backed instances could
do before the token.

### The interval that is load-bearing

With no margin on either side, TLC finds a leader whose clock runs slow still
serving when the control plane, whose lease copy has lapsed, names the next
one. `Margin` and `Eps` together have to outlast what `Drift` can do to the
two clocks, which is the design's safety interval, and removing it is two
leaders in one step. `FelixShardRealMargins.cfg` is the code's own split: a
quarter of the lease on each side, which holds against a drift of a quarter.
The control plane's quarter is measured on its own monotonic clock as well as
the store's, which is what makes its expiry real time here rather than a wall
clock that can step.

### The fence that takes the clock out

Under `Quorum` today, an acknowledged record survives because the report
orders the ack and promotion reads the report, and the lease keeps a deposed
leader from writing. `FelixShardAckWithoutLease.cfg` shows the lease check at
release is not what does it. `FelixShardFencedAck.cfg` goes further, and is the broker once the fleet
finalizes `majority_ack`: the ack counts followers at the leader's generation,
no report and no lease, and the promoted leader fences a majority before it
serves. `AckByFollowers` takes the lease off the whole write path (an `ASSUME`
refuses a configuration that checks it at the commit or the ack, and
`Serving` does not read it), so a deposed leader admits and commits until it
steps down. It passes with no margin on either side of the lease, so two
brokers do serve at once. The old leader keeps writing; it just cannot find a
majority for it.

The leader counts what each follower answered at its generation
(`confirmed`), as the code counts `ReplicateOk`: a follower that took a newer
leader's fence after answering still counts, because its fence answer carries
the record. The leader counts itself only while it has taken no newer fence;
counting itself after that is how a deposed leader would acknowledge on itself
and a follower the fence missed.

`FelixShardUnfencedAck.cfg` drops the fence and TLC finds the record lost in
eleven steps: the old leader still believes it leads, the control plane
promotes a follower, and the old leader commits, ships to the other follower, which has
never heard of the new generation, and acknowledges on that majority.

The catch-up is load-bearing too. Checked by hand with the fence answering but
the new leader not taking the log ahead of its own, TLC finds a record a
majority acknowledged before the fence missing from the new leader.

A cache shard has two logs, the cache log and the counter log, and one promise
per replica, kept on the cache log. `Counters` adds the counter log (`clog`)
beside `log`: a counter update ships, is counted at the leader's generation and
acknowledged like a put, and a promoted leader fences both logs on a majority
before it serves. Promotion reads only the cache log, so nothing picks a leader
that holds the counters; the fence's catch-up is what brings them.
`FelixShardFencedCache.cfg` passes, `FelixShardFencedCacheUnfenced.cfg` loses a
put without the fence, and `FelixShardFencedCacheNoCounterCatchUp.cfg`, which
fences the counter log but does not take it, loses a counter update the old
leader acknowledged on itself and the follower the new leader fenced.

A set change on a cache shard has to carry the counter log across as well
(#1830). `FelixShardSwapCounters.cfg` adds the counter log to a cut-over that
changes several members at once. Three rules keep every acknowledged counter
update on a majority of the set the control plane records:

- The cut-over waits until the destination holds the old leader's counter log.
- While the change is pending, the counter mark needs a majority of both
  sets, as the cache log's does (`CountersJoint`).
- The control plane takes the new set only once the counter log's start
  record is held too, not just the cache log's (`FlipChecksCounters`).

Without the third, an update held by the old leader and the destination is on
one replica of the new set after the flip
(`FelixShardSwapCountersFlipUnchecked.cfg`). Without the second, an update the
leader acknowledges on the new set alone is on one replica of the old set,
which is still the control plane's (`FelixShardSwapCountersNewSetOnly.cfg`).
The fence still takes the counter log on the new set alone, as `promotion.rs`
does. These configurations have no elections, so a candidate standing while a
change is pending is not checked against it.

### Ballots: one leader per generation

The control plane issues each generation once, so a replica has only ever
needed to remember the generation it accepted, not whom from. A replica that
elects itself breaks that: two followers that lose the leader together both
pick the next generation, and before ballots a replica answered a fence at a
generation it had already accepted as a leader confirming it still leads.
`FelixShardElectNoBallot.cfg` lets it, and TLC finds two candidates each
taking the third replica's answer and both opening. `FelixShardElect.cfg` keeps
a ballot per promise and passes: a replica answers at its accepted generation
only the leader it promised, and a candidate persists its own ballot before it
fences, so two majorities at one generation would share a replica that
answered both.

`AnswerFence` keeps the strict `<` it always had when no replica elects: with
one leader per generation the equal case is the same leader asking again.
`Ballots = FALSE` and `Elections = 0` in the bounded configurations that
predate ballots leave the model, and its state space, exactly as they were. So
does `FenceEveryChange = FALSE` there. The long walks (below) turn ballots and
the fence on every change on, as the broker ships them.

Ballots are not enough on their own. `FelixShardElectStaleSet.cfg` replaces a
follower and lets the one that left stand on the set it last knew. The old
set's majority never promised the generations the new set used, so no ballot
is in the way, and TLC finds it opening without a record the new set
acknowledged.

### The set check: electing on the current set

`SetCheck` closes that. A fence and a batch carry the generation of the
assignment that wrote the sender's set (`setGen`) and the set itself. A
replica that knows a newer one refuses and says which; otherwise it learns
the sender's. A refused candidate learns the newer set and stops standing.
It may stand again at a later generation, fenced on that set from the
start, if it is still a member. A set change fences before it opens,
carrying the new set to a majority of it, and with `JointFence` to a
majority of the old set too. `FelixShardElectAdopt.cfg` passes with both: the
replica that left is refused by a follower that took the change's fence,
learns it is out, and stops.

The joint half is the design's default, because the product does not change
sets one replica at a time. A replacement joins beside a follower and then
seats, which alone would keep every majority of one set overlapping one of
the next, but a move's cut-over does not: `replicas_after_cut_over` keeps
some of the staying replicas and tops the set up from brokers outside it, so
{X, b, c} can become {Y, X, Z}. `Swap` models that cut-over, the destination
leading on a set that differs from the old one by more than one member.
`FelixShardElectSwap.cfg` is the same check with the joint fence. It is
too big for one run, so it is checked in pieces, one for each way the
cut-over can go (`FelixShardElectSwap{Keep,Drop}{0,1,2}{,Stood}.cfg`, with
`Drop0Stood` and `Drop1Stood` each split in four),
and
`FelixShardElectSwapNoJoint.cfg`, fencing the new set alone, finds a
replica left on the old set standing at the new leader's generation, opening
on a majority of the old set that the new leader never fenced, and
acknowledging a record the new leader does not hold. The cut-over waits for
the drain, as a move's does, so the old leader has stopped and the copies
joining hold the destination's log, which holds the old leader's; without that wait the old leader would go on
acknowledging on the old set after the control plane moved on. `SetChangesJoint` states it
over consecutive sets: once a change opened, a majority of the new set and
of the old one know the new set. Until then the change is pending, and it
travels with the set: a replica that learns the new set from a fence also
learns the old one, and fences both if it stands, as Raft's joint
configuration does. A walk found the gap when only the
leader that started the change remembered it: the destination was deposed
mid-fence, a replica that had learned the new set from it stood and opened on
a majority of the new set alone, and a replica of the old set opened beside
it at the same generation (OneLeaderPerGeneration).

The control plane flips only after the joint fence. `Swap` records the new set
as pending (`cpNext`, written at `cpNextGen`) and the old set stays the
control plane's (`CpSet`), so `AckedOnMajority` keeps measuring records
against it. While its change is pending a leader acknowledges on a majority
of both sets (`HeldAtGen`) and ships to both. `CpFlip` takes the new set once
a leader on it has its generation's start record acknowledged, which then
sits on a majority of both, and `FinishChange` lets a leader drop the old set
once the control plane holds its set.

A destination can already lead the cut-over's generation: it was elected
there, and the control plane, which never heard of the election, names it
there again. Its log then holds two start records at that generation, the
elected stint's and the named stint's. Records the elected stint
acknowledged sit between them, on a majority of the old set only. A copy
joining in the cut-over may hold any prefix of the destination's log
(`JoinLags`), since nothing checks how far a top-up copy has got. The flip
must wait for the named stint's start (`StintStarts`), which reaches the new
set only behind those records.
`FelixShardElectSwapFlipJoinLagsFirstStart.cfg` takes the generation's first
start instead and loses an acknowledged write (#1787); the code's
`own_start` counts from the current stint's start, as `StintStarts` does. There the flip waits for that first start to be
held on a majority of both sets, read from the logs as `joint_start` counts
it, and a joining copy that holds the start but not the write is enough.

The control plane may flip on a report that has since gone stale, so the
model lets it flip once the leader opened jointly. Under `StintStarts` that
open must be at the control plane's generation: a destination that stood
again, past the generation it was named at, never opened the named stint
and so never reported it.

A failover (`Promote`) abandons the pending change; it writes the set at a
generation above the pending one. While a change is pending, `CpAdopt` adopts
only a leader on the pending set that is in the old set, and not a joining
copy, and takes just the generation of one on the old set, which the new set's
higher set generation deposes; the walk found an adopted old-set leader fenced
that way. The one exception is the control plane's own leader on the old set:
it opened past the change, and only it could have taken it, so the change is
dropped. With no change pending, a leader elected on a set other than the
recorded one is not adopted on its generation alone. A majority of the elected
set may hold records no majority of the recorded one does, and a later
failover would promote from the recorded set. If the leader is in the recorded
set, the control plane writes the next generation under it with the elected
set as the set being left and the recorded one pending, as a cut-over does
(see the reissue below). If it is not, the elected set becomes the recorded
set at its generation. `FelixShardElectAdoptAboveKeptSet.cfg` found the
generation-only form leaving a pending change below the adopted generation
that no leader could complete. It still finds one case the code gets wrong
(#1929): when the elected leader is the recorded
leader but was elected on another set, adoption takes the generation alone.
That case is reachable since placement may name a leader that then refuses
the generation (#1916). `ReissueOwnOnOtherSet` models the fix (#1930): the
control plane's own leader elected on another set is reissued like any other
member, and `FelixShardElectAdoptAboveKeptSetReissued.cfg` finds no violation with it.
The election walks turn it on, since the swap walk reaches the same case.
No placement change other than a failover
starts while one is pending. Only the control plane ends a pending change, so
while it is down a lost majority of the old set stops writes, and a
destination lost before the rest of the new set learns it leaves the change
pending until a failover clears it; nothing here checks liveness.
`AdoptedMatchesOpened` compares the control plane's leader, while it still
holds the control plane's generation, with the pending set while there is one,
since that is the set the control plane named it for. The swap walk found the
window this closes: a replica removed by a cut-over that never heard of it
stood, fenced the stopped old leader, and acknowledged on a majority of the
old set, which the control plane had already dropped
(`AckedOnMajority`).

Three rules come from TLC. The first version let a refused candidate fence on
at the same generation with the set it had just learned, and let a leader or
candidate go on leading after it had answered a newer leader's fence.
`FelixShardElectAdopt.cfg` found b opening at generation 2 on the original
set {a, b, c}, while c, also standing at 2, answered b's later fence at 4,
learned the set {b, c, d} there, and opened at 2 as well with d's answer
(`OneLeaderPerGeneration`, 14 states). Ballots keep one leader per
generation only between majorities of one set, so a refused candidate stops,
standing again only at a new generation. The second run found a, the
control plane's first leader at generation 1, answering b's fence for a seat
at 4, learning the set {b, c, d} there and leading on: it shipped its
generation-1 record to d and counted c and d as a majority of the set it had
just learned, acknowledging a record the new set held on d alone
(`AckedOnMajority`, 18 states). A leader or candidate that takes a fence, or
a batch, at a newer generation has promised its own away, so it now stops
leading there and then. Third, `AckedHeldByLeader` once judged only the
newest generation's leader. With elections that misleads both ways: an
elected leader that steps down on its own lease leaves a deposed one as the
newest (`FelixShardElectStepDown.cfg`), and a candidate's promise to itself
that reached nobody would hide the leader below it. It is now checked per
generation, which `FelixShardElectNoBallotAcks.cfg` exercises with two
generations of elections.

`Adopt` is the control plane's half. A leader that opened above the
generation a planner read reports it; on the set the control plane records,
the control plane makes it its leader at that generation and carries on from
there, and on another set it only moves its own generation past it. Either
way it never issues a generation at or below one it heard was elected. The
write is conditional on the read, like every placement write, and
a walk had a planner acting on a held read.
`FelixShardElectAdopt.cfg` passes with replacements under adopted leaders, and
`FelixShardElectAdoptPromote.cfg` with promotion on suspicion beside adoption.
At `Adopt = 2` no adoption can mismatch: the only generation a replica can
elect itself to past the control plane's comes before any set change.
`FelixShardElectAdoptNoMatch.cfg` lets generation 3 in and drops only the
comparison (`AdoptMatch`). It is not registered in `scripts/check_tla.sh`
and has not been run yet, so whether the comparison is needed for safety or
only for the control plane's bookkeeping is still open.

A generation recorded and not adopted leaves the control plane naming its
old leader there, and that leader cannot serve it: it promised the generation
to the elected replica, or a majority of its set did. `Renames` adds the
control plane's answer (`CpRename`): once the leader reports that, the
control plane names it again one generation up, on its own set with any
pending change kept, and the leader fences there like any new leader,
jointly while a change is pending. `FelixShardElectRename.cfg` checks it
after a cut-over, where a replica that never heard of the change is elected
on the set being left; `FelixShardElectRenameDue.cfg` shows the step is
reached. Opened without the fence (`RenameUnfenced`), the renamed leader
loses what the elected one acknowledged on the old set
(a walk found it). The leader that already took the
generation and is still fencing is named again the same way when its fence
is refused: when too many of its set promised the generation away, or, for a
joint fence, too many of the set being left
(`FelixShardElectRenameFencingDue.cfg`).

`PreVote` models pre-vote and stickiness: a replica stands only once a
majority of its set, itself included, no longer hears a leader, and a replica
that still hears its leader refuses a candidate's fence. Safety does not rest
on it; every other election configuration runs without it.
`FelixShardElectPreVote.cfg` runs with it, time moving so leases lapse.

The configurations above check safety only: nothing bad happens, whether or
not anyone is ever elected. `FelixShardElectLive.cfg` checks that an election
finishes. With the control plane down and any one broker stopped for good,
even the leader, the replicas elect a leader that stays (`ElectionSettles`).

That needs three things the safety checks do without:

- Fairness: a step an election needs is taken once it stays possible.
- A pre-vote that a member answers yes only when it leads nothing open,
  hears no leader and holds no log ahead of the candidate's, as the
  broker's `pre_vote` does (#1815).
- Backoff: a candidate whose round cannot reach a majority gives it up. Real
  backoff is randomised and separates candidates within a few rounds. The
  model cannot say how few, so it allows at most `Splits` rounds, over the
  whole run, where two candidates stand at once.

A candidate's own detector may be wrong (`DetectorMisfires`): it may suspect
a leader it still hears. If the open leader answered the pre-vote yes
(`FelixShardElectLiveLoosePreVote.cfg`), that candidate would get the leader's
yes and depose a leader the rest of the set hears.

The model bounds generations, so `ElectionSettles` alone cannot see churn: a
run that used every generation would stop electing and settle. Without
backoff (`FelixShardElectLiveNoBackoff.cfg`) candidates split until no
generation is left and nobody leads, which is how the model shows splitting
forever. `NoStandAgainstHeardLeader` rules churn out directly, since it holds
at every step. Not covered: liveness with writes in flight, or with a wiped
voter.

With the control plane up, placement can name a leader at the generation a
candidate is elected at. Ballots let only one of the two open there. When the
elected one opens, the control plane's leader never will. Short of another
promotion or election, the generation stays there. On the elected leader's
report the control plane writes the next generation with it as leader
(CpReissue, `reissue_past_collision`). When the set it was elected on is not
the control plane's, the write is a set change from the elected set to the
control plane's, shaped as a cut-over: the elected set becomes the recorded
one, a candidate refuses to stand while the change is pending, a failover
promotes from the elected set, and the leader fences both sets jointly. The
change completes only once it opened on both. The leader ships to the
pending set alone, so when the control plane's set and the leader do not
hold a majority of the elected set the change could never complete; the
elected set is then named alone, with nothing pending (`ReissueAt`, shared
with `CpAdopt`). No configuration reaches that case: with sets of three out of
four brokers, any two sets share two members. The model also leaves out the
code's window of 16 generations past the control plane's, beyond which
adoption takes the generation alone.
The report may be old, so the leader takes the generation only if it has not
promised it to another; otherwise it never opens there. `FelixShardElectReissue.cfg` checks that this
keeps one leader per generation and every acknowledged record.
`FelixShardElectLiveReissue.cfg` checks that the control plane then moves on
(`CollisionSuperseded`); without the reissue it does not
(`FelixShardElectLiveNoReissue.cfg`). The property is stated while the
elected leader still leads: once it is deposed, a later election moves the
generation on, and the model bounds elections. It is close to the reissue's
own enabling condition, since the liveness configurations have no set
changes or joining copies to hold it back; what it adds is that adoption and
the reissue are taken at all.

`FelixShardCpLive.cfg` asks the same of the control plane's own leader
while the control plane runs (#1917): the leader it names ends up serving
and stays (`CpLeaderServes`). The run allows what the safety checks allow
and the earlier liveness checks left out. A candidate may give its standing
up and keep the ballot it persisted (`Withdraws`, the stand timeout). A
stopped broker may come back with what it persisted (`Restarts`). Placement
may name a leader below a ballot and renames it on the refusal (`Renames`).
`FelixShardCpLiveMain.cfg` is main's code, and TLC finds a shard that never
serves: a leader whose own ballot is above its assignment never reports the
refusal (`keep_own_set` in `promotion.rs`, #1919). That fix and three from
the list of dead ends in #1914 each have a twin with that one fix off
(`FelixShardCpLiveNo<Fix>.cfg`), and TLC finds a shard that never serves in
each but `StandBehindBallot`'s, which has not finished. `DeposeOnAssigned` has no twin: with the control plane up, a leader
that stops serving because one follower is ahead is exactly the open leader
`HaltReported` renames, so that fix matters only with the control plane
down, where `FelixShardCpDown.cfg` has it on. The fifth
from that list, `CatchAllRename`, is modelled but no configuration turns it
on yet.

Those fixes are not enough on their own. With all of them on, TLC finds the
open leader answering a candidate's higher fence: its own log takes the
candidate's generation, so it stops serving, and the candidate then
withdraws. The leader reports nothing, since it opened at its generation
and its refusals are only checked when an open fails, and nobody suspects
it while it is alive (#1951). `AboveReported` has it report the generation
refused, and `FelixShardCpLiveNoAboveReported.cfg` is that fix's twin.
Both reports carry the ballot the generation was lost to, and placement
renames the leader one generation above it (`RenamePastBallot`) rather than
one above its own, which would take a report per generation in between.

#2046 is a shard that stayed stuck with the control plane up. A candidate
stood one generation up and withdrew. Placement then renamed the leader one
up, onto the candidate's withdrawn ballot. The leader's fence there had one
member promised to that candidate and one answering from a copy its
failover's pins (`fence_storage_ids`) did not name, so it neither opened nor
reported a refusal. `FelixShardCpLiveRefusals.cfg` models the rules adopted
for it. Copies may change and be admitted again (`CopyPins`). Pins survive a
rename until the leader's first fenced open and drop for a copy once it is
admitted. A fence with one promise away and the rest silent is refused even
when another copy answered. A refusal for a lost set (`UnknownCarriesBallot`)
or for a member that took a newer generation (`AheadRefused`) carries the
ballot, and placement renames above it. Candidates stand above the control
plane's generation as well as their own promise (`StandsAboveCp`), and the
backoff is a bound on standings (`MaxStandings`). Each rule has a twin
with it off. With the exclusion kept, TLC finds the #2046 stall. With the
pins kept for good, a re-admitted copy never counts again. Without the
carried ballot no stall has turned up: the leader loses its set only after
it opened, by which time its pins are gone. `FelixShardElectRefusals.cfg`
checks the same rules for safety. Its twin, `FelixShardElectRefusalsPinsCleared.cfg`,
drops the pins at a rename, as the code did. TLC then finds a record lost
whether or not the exclusion is kept: the refusal comes from a promise away
and a silent member before any other copy answers.
`FelixShardElectRenameMinorityDrop.cfg` refuses a joint fence on one
promise away and has the rename drop the set being left, and TLC finds two
leaders at one generation.

`FelixShardCpDown.cfg` checks what happens when the control plane stops for
good partway through (`CpStops`). Elections alone must then get a leader
serving on a majority (`CpDownServes`). Three things decide it:

- Suspicion. The broker stands only against a leader whose broker it
  suspects (`BrokerSuspicion`, `suspicion.rs`). That never fires against a
  live broker whose shard is stuck, and
  `FelixShardCpDownBrokerSuspicion.cfg` finds such a shard with no leader
  for good. Per-shard suspicion (`ShardSuspicion`) also stands when the
  shard's leader is not serving. That is not enough on its own: a broker
  never stands for a shard whose route names itself (`follows` in
  `candidate.rs`). `FelixShardCpDownNoSelfSuspicion.cfg` finds the leader
  elected one generation above its assignment and open there, then stopped
  and restarted with the control plane down. Its open at the assignment is
  refused for its own newer ballot, it does not stand, and nobody else can
  win a pre-vote because its log is ahead. `SelfSuspicion` has that broker
  stand too.
- The pre-vote. A broker still fencing refuses it, but only while its fence
  can still complete (`PreVoteBounded`). With no bound on that refusal
  (`FelixShardCpDownPreVoteUnbounded.cfg`), TLC has found no stuck shard
  yet, though the run did not finish.
- A promise to a candidate. A replica does not stand above a candidate it
  promised while that candidate still stands. Once the candidate withdraws,
  stops, or comes back without its standing, the promise no longer holds
  the replica back. Freeing it only when the candidate is down
  (`FelixShardCpDownStandBehindDeadOnly.cfg`) left a shard with no leader
  before `SelfSuspicion`: the candidate restarts, drops its candidacy, and
  every replica that answered it waits on it. With `SelfSuspicion` that
  run has not found one.

The model bounds generations, so a run can stop electing only because the
bound is used up. `CpDownServesInBound` accepts that ending only when some
live broker would still stand but for the bound (`Exhausted`); a real dead
end, where nobody would stand, still fails. Candidates that keep splitting
the vote until the bound also count as in bound: `Exhausted` asks only that
a broker would start a round, not that it could win one. The code relies on
randomised timing to end such splits, which the model does not have.

One case leaves the shard served, but not fully. Placement names a broker at
the generation another was just elected and opened at, then the control
plane stops. The elected leader serves on a majority, and the named one
keeps fencing at that generation for good, so the shard runs one copy short
(`FullyBacked`, `FelixShardCpDownDegraded.cfg`). Neither per-shard suspicion
nor the freed promise reaches it: the named broker never stands, with its
own round in flight, and if it did, the others hear the leader and refuse
its pre-vote. The control plane's last placement also still names the
stuck broker, so only clients that follow the elected leader reach it.
Closing it would take the leader fencing again above that copy.

A promotion takes its ballot before its routes land: the broker's
lifecycle persists the new generation's ballot, and the routes that name it
leader follow a reconcile later. A candidate round that read the old route
in that gap still sees the old leader, and before #1936 it could stand at
the generation above its own broker's new leadership, then withdraw. The
leader then opens at the control plane's generation below its own ballot and
never counts its own copy there (#1933). `RouteLags` lets that round stand
(StandOnStaleRoute), as one step that promises itself the next generation
and fences nobody. #1936 adds two rules. The round does not stand while its
copy's ballot is past its route and names its own broker (`NamedHereWaits`,
`named_here` in election/candidate.rs, held for 30 s on the assumption that
routes land within that), and a leader left below its own ballot reports its
generation refused, so the control plane names it again one up
(`OwnBallotReported`, OwnBallotAbove in RenameDue; `ballot_above` in
driver/shard.rs, reported from driver/shards.rs once it lasts 3 s). `FelixShardElectRouteLagNoFix.cfg` finds #1933 without
them; `FelixShardElectRouteLag.cfg` passes with both, and
`FelixShardElectRouteLagReportOnly.cfg` shows the report alone is enough.
The stale round fences nobody unless `StaleFenceAnswered` lets one member
answer it, its reply arriving after the round gave up (#1940). That member
then refuses the leader's own fence. By default the model's open ignores the
leader's own promise; the code's open takes the generation's ballot on its
own copy first (`keep_own_set` in promotion.rs), which a newer promise
refuses, and `OpenChecksOwnBallot` models that. With either, the leader can
stay fencing below its own ballot, and neither of #1936's rules reports it:
`FelixShardElectRouteLagFenceNoFix.cfg` finds that. With the open check alone
no member has to answer: the leader stands on the stale route while
fencing, a follower answers its fence, and the open never happens. The fix
(`OwnBallotFenceRefused`, OwnBallotFenceAbove in RenameDue; the own-copy
check at the start of `fence_shard_as`) has a fencing leader whose own copy
promised a newer generation report its generation refused at once, and
`FelixShardElectRouteLagFence.cfg` passes with it. In the code a member's
newer promise is reported too (#1914), which the model leaves out. The model allows the stale
round after a rename too, where the code's old
route names the broker itself and nothing stands; that only adds behaviours.

A refusal reaches the control plane in a replica report, and the control
plane keeps one report per shard, the last. At the same generation any later
report replaces it (`ReplicaReport::supersedes` in
services/felix-controlplane-service/src/model/shard.rs), and placement renames
only on the refusal the stored report carries (`refused` in
cluster/placement/replica_positions.rs). An open leader keeps sending pass
reports after its refusal, so before #1971 the next one hid the refusal and
the shard was never renamed (#1969). Under `ReportStore` the model keeps that
stored refusal (`cpRefused`): the leader sends a refusal while it has one
(SendRefusal, which liveness takes once it stays possible, as
`report_refusal` in driver/shards.rs re-sends it), its pass reports land over
it (PassReport), and RenameDue reads the store instead of the leader's state.
`RefusalRepeats` is #1971's rule: the reporter repeats the refusal on every
later report at its generation (`Refusals` in
crates/server/felix-replication/src/reporter.rs).
`FelixShardElectRouteLagReportStoreNoRepeat.cfg` finds #1969 without it, and
`FelixShardElectRouteLagReportStore.cfg` and
`FelixShardElectRouteLagFenceReportStore.cfg` pass with it. The store keeps
a refusal after the state behind it changed, and the
`FelixShardElectRename*ReportStore.cfg` pieces check the safety invariants
with renames on that. The model never expires a report; the code drops one
past its TTL, which only takes renames away.

These check the design in `docs/replication-design.md`, "Electing without
the control plane", which the broker now follows once `replica_election` is
finalized.

The ballot lives in the replica's `ballot` file, written and fsynced before
the generation it names is answered (`DiskLog::accept_generation`); see
`docs/replication-design.md`, "Ballots".

`FelixShardFencedAck.cfg` allows one promotion. `FelixShardFencedAckTwoPromotions.cfg`
allows two (`L = 2`, without drift). Neither tells a
leader that counts only records of its own generation from one that counts
any it holds; that takes a third leadership (below). The two-promotion
configuration takes 28 minutes on a CI runner, so it runs nightly instead of
on every push;
`FelixShardFencedAckTwoPromotionsStart.cfg`, with the start record on and one
write, fits CI. The manual dispatch of the CI workflow takes a list of
configurations for runs like these:

```bash
gh workflow run ci.yml --ref <branch> -f tla_timeout_minutes=120 \
  -f tla_configs="FelixShardFencedAckTwoPromotions FelixShardFencedAckMove"
java -jar target/tla/tla2tools-v1.7.4.jar -deadlock -workers auto \
  -config docs/formal/FelixShardFencedAckTwoPromotions.cfg docs/formal/FelixShard.tla
```

The order a fence answer gives is only as good as the labels behind it. A
record's `g` is the generation that wrote it and `lg` the one the broker
holding it believes wrote it; `Ahead` and `LastGen` read `lg`. With
`LabelOnReceipt` a follower labels what it is shipped with the sender's
generation, as it still does with a batch from a sender that predates labels, and
`FelixShardFollowerLabels.cfg` finds an acknowledged record lost: a follower's
copy of an inherited record looks newer than a log that holds more.

Raft's Figure 8 needs a third leadership after the two that disagree. The
current-term commit rule that closes it is checked in `FelixRaft.tla`.

### A move under follower acks

The broker used to open a planned move's cut-over, and a cancel's hand-back,
without the promotion fence: the destination takes over only once the drained
leader's whole log is on it, and a cancel returns the shard to the leader that
had it. The move configurations model that (`FenceEveryChange = FALSE`), and
persist the new leader's generation before its
start record, as `open` does. Placement fences a move only on a report from the
generation it read, and a leader still in its own promotion fence sends none,
so a move never catches one mid-fence.

What TLC found is the failover in between. The destination's broker cannot
tell a failover that names it from the cut-over it expects, so it opens that
without the fence too. A report can name the destination caught up and the
leader then acknowledge a record on itself and another follower, since under
`majority_ack` nothing waits for the report. The leader dies, placement
promotes the destination from that report, and the destination serves
without the record (`FelixShardFencedAckMoveDestination.cfg`,
`FelixShardFencedAckStagedMoveDestination.cfg`, twelve steps). Placement now
leaves a move's destination out of a `Quorum` failover, which ends the move
(`PromoteDestination = FALSE`), and the move configurations pass.

### Every change of leader fenced

Without elections the unfenced cut-over is safe, because the control plane
issues each generation once and nobody else leads in between. With them it
is not. The control plane picks a cut-over's generation from its own view,
which does not include an election held while it was away. In
`FelixShardElectHandoffUnfenced.cfg` a replica elects itself to the
generation after the control plane's while a move is fenced, fences the old
leader and opens; the control plane then cuts over to the destination at that
same generation, and the destination, which never promised it to the
candidate, opens there too (`OneLeaderPerGeneration`, ten steps by hand).

`FelixShardElectHandoff.cfg` fences the cut-over and the hand-back like a
promotion (`FenceEveryChange`). The new leader persists its own ballot, which
it cannot do at a generation it already promised a candidate, fences a
majority and takes the answer furthest ahead. A majority that took the
candidate's ballot refuses it, so it stays closed until a higher generation;
one that did not has a replica holding whatever the candidate acknowledged.
With one write it takes an hour on a CI runner, so it runs nightly, like
`FelixShardFencedAckTwoPromotions.cfg`; `FelixShardElectHandoffLeaders.cfg`
checks one leader per generation without the write on every pull request.
The broker fences every leadership it takes this way, including a
generation of a shard it serves that skips one. The generation right after
the one it serves opens unfenced, as `Regenerate` does in the model without
`SetCheck`, where only a follower replacement reaches it. With elections that
is a second leader at the generation: a candidate can take it on the followers
the open never asked. Under `SetCheck` it fences like any other change
(`FelixShardElectAdopt.cfg`), jointly, which is what self-election will have to ship.
`FelixShardElectHandoff.cfg` now runs with `SetCheck` on as well. With no
spares the set never changes there, but a leader that answers a newer fence
now steps down, which adds states.

### Failover on the followers' word

The control plane promotes once a dead leader's lease has lapsed and the
margin has passed. Once the fleet acknowledges by its followers and confirms
reads by a round, nothing a deposed leader does depends on the lease, so
placement may promote as soon as a majority of the set says it cannot reach
the leader (`Suspicions`). A detector can be wrong, so the model lets
placement promote on any read up to that generation, with the old leader
alive, serving and renewing. `FelixShardSuspicion.cfg` keeps every
acknowledged record that way, and never opens two leaders at one generation,
with one such promotion; the moves and spares walks (below) promote on
suspicion up to the tenth generation of every walk. `FelixShardSuspicionLease.cfg` does the same
where the lease still decides who serves, and TLC finds two brokers serving,
which is why placement acts on a suspicion only once `generation_start`,
`majority_ack` and `lease_free_reads` are all finalized: short of any of them
the brokers keep the lease, which is this configuration. The model puts every
leader on one path. A real fleet can have a leader still on the lease after
the finalize, one that has not heard of it yet, so placement also needs the
leader's own report at the current generation to say it serves by round
(`by_round`).

### A follower that loses its promises

A promise is a file in the shard's directory, so a follower whose data
directory is wiped has none, and would answer a leader its old copy had
promised to refuse (#1267). `Forget` takes the promise and keeps the log:
in the broker the empty copy would take the deposed leader's log back from
that leader's first batch, so keeping it only helps that leader. In
`FelixShardSuspicionWipeUnbacked.cfg` the follower takes the successor's
fence, is wiped, and then takes the old leader's write, which the old leader
acknowledges on the two of them while the successor serves without it.

The broker cannot recover the promise, but it can bound it. Without
elections the control plane issues every generation, before anyone fences
for it, so the routing view a wiped broker seeds on restart names the newest
generation it ever promised, or a later one, and the one leader named there.
With `PromiseFromRoutes` the follower answers only what that view backs,
which is what the broker does for any copy with no promise on disk
(`refuse_unbacked` in `crates/server/felix-replication/src/replica.rs`),
and `FelixShardSuspicionWipeRoutes.cfg` passes. The model seeds the view at
the wipe, the oldest view a restart can read, so the code refuses at least
as much as the model does.

Two things this does not cover. With elections a candidate's generation is
not in the control plane until it is adopted, so a view cannot back a
promise made to one, and the configuration assumes `Elections = 0`; electing
without the control plane needs its own guard before it ships (#1331, next
section). And it
takes the routing view to be no older than the control plane was at the
wipe. A view from a lagging Raft member can be older, so the broker also
checks a leader-confirmed read of the assignment, taken after the restart
and so no older than the model's view (#1332). Only while the control
plane serves that read: one that answers 404 or 501 (an older build, or a
Raft leader below the soft-state level) leaves the view to answer alone, and
a lagging member can then still break the assumption.

### A wiped voter in an election

With elections, a follower that loses its directory loses its ballot and its
set too (`Forget` clears `promised`, `ballot`, `setGen`, `mine` and
`oldSet`), and the control plane cannot back the lost promise: a candidate's
generation is in no view until it is adopted. `FelixShardElectWipe.cfg`
lets the wiped replica answer as if it had never promised, and TLC finds a
second leader at the generation it had already promised.

#1331 asks for a guard: a wiped replica with no promise refuses candidates'
fences, and does not stand, until the control plane has adopted a
generation past its wipe, read linearizably (`gen` in the model, not a
planner's `cpView`). `WipedWaitsForAdopt` models it, with the bound taken at
the wipe, the lowest a registration can read. The knobs beside it model what
the waiting replica may still do, and what it does once the wait ends. Each
configuration is FelixShardElect.cfg's shard with at most one write, the set
check, and adoption up to generation 2, or 3 where named:

- The guard as #1331 words it fails first on the control plane's own
  leader. An election the control plane has not heard of deposed it, and the
  wiped replica takes its batch and acknowledges a write the elected leader
  does not hold (`FelixShardElectWipeWaits.cfg`). The waiting replica must
  refuse every fence and batch.
- The bound fails next. Refusing everything while it waits, with the bound
  either the control plane's generation at registration
  (`FelixShardElectWipeWaitsAll.cfg`) or that plus a ballot seeded from it,
  as `PromiseFromRoutes` does (`FelixShardElectWipeWaitsSeeded.cfg`), TLC
  finds two leaders at one generation: the replica promised generation 2
  before its wipe, the control plane then adopts the leader at 2, which ends
  the wait, and the replica answers a second candidate at 2.
- A floor once the wait ends (`WipedAboveAdopted`: nothing below the control
  plane's generation, read the same way, only its leader at it, candidates
  above it) passes while elections reach one generation past adoption
  (`FelixShardElectWipeWaitsFloor.cfg`) and fails at two
  (`FelixShardElectWipeWaitsFloorDeep.cfg`): the replica promised 3 before
  its wipe, the control plane adopts 2, and the replica stands at 3 itself.
  No generation the control plane holds bounds what a replica promised to
  candidates it never heard of.
- With a bound no broker can know, the newest generation the replica had
  promised (`WipeBoundOracle`), refusing everything while waiting and the
  floor after pass at generation 3 (`FelixShardElectWipeOracle.cfg`). That
  isolates the questions about what a waiting replica may still do. Taking
  batches from an unadopted leader is not safe: acknowledging is a vote for
  the records, and a leader deposed by an election the replica took part in
  before its wipe gets a write acknowledged that the newer leader lacks
  (`FelixShardElectWipeOracleBatches.cfg`). Saying yes to pre-votes, which a
  replica that hears no leader does, and learning a candidate's set without
  answering it are safe (`FelixShardElectWipeOracleLearnsSet.cfg`).

In short: the guard as #1331 words it is unsafe, no bound the control plane
holds is safe (question 1), taking batches while waiting is unsafe (question
2), and pre-votes and learning a set are safe (question 4).

#### The adopted rule: a wiped replica rejoins as a non-voter

Adopted on 2026-10-31 (#1331): a copy with a new storage id is a non-voting
member of its sets (`WipedNonVoter`). Its wipe empties its log, mark, ballot
and set file (`Forget`). It answers no fence and takes no batch, and no
majority counts it, until the control plane re-admits it (`Readmit`): the set
is written again at the next generation, and the leader named there fences it
jointly, counting the old set's majority without the wiped replica. The
re-admission completes only once the replica holds that leader's log through
the start record at the re-admission's generation (`Readmitted`), as a Raft
learner is caught up before it is promoted. Until then it may learn a set,
say yes to a pre-vote and copy the confirmed leader's log (`CatchUp`),
taking that leader's ballot as any batch does; none of it counts.
Control-plane failover never promotes a non-voter.

Each configuration is `FelixShardElectWipe.cfg`'s shard with one write,
elections, and adoption up to generation 3, where every bound the control
plane holds failed:

- `FelixShardElectWipeNonVoter.cfg` (one follower wiped once, no time),
  `FelixShardElectWipeNonVoterFailover.cfg` (time, so control-plane failover
  is reachable; adoption only up to generation 2) and
  `FelixShardElectWipeNonVoterFailoverWipesLeader.cfg` (the same, where a
  leader or candidate can be wiped as well, `WipeLeads`) pass. So do `FelixShardElectWipeNonVoterWipesLeader.cfg`
  (`WipeLeads` without time: the control plane gets a new leader only by
  adopting an elected one) and `FelixShardElectWipeNonVoterTwice.cfg` (two
  wipes, `Forgets`: the same replica again while its re-admission is
  pending, or a second one).
- Without the catch-up the rule finds two leaders at one generation
  (`FelixShardElectWipeNonVoterNoCatchUp.cfg`). c promises 3 to candidate b,
  which opens; c is wiped and forgets it; the control plane re-admits c at 2
  through a, which then stands at 3, and adopts b at 3. Re-admitted once b
  is open, without a batch from it, c holds no ballot for 3, answers a, and a
  opens at 3 too. The catch-up brings the leader's records and its ballot;
  either one missing is unsafe.
- Counting the wiped replica in the re-admission's old majority finds two
  leaders at one generation (`FelixShardElectWipeNonVoterCountsWiped.cfg`).
- Failover that may promote a non-voter finds two leaders at one
  generation (`FelixShardElectWipeNonVoterPromotes.cfg`). c promised 2 to
  candidate b, is wiped and forgets it, and the control plane, which has not
  heard of 2, promotes c at 2 beside b. The lost promise is as much the
  reason as the lost log.
- A new shard (`Creates`, `FelixShardElectWipeNonVoterCreates.cfg`) starts
  with its first leader fencing at generation 1 and no follower holding a
  promise or a set. A follower that was not wiped answers with an empty log
  and counts, and that passes: it holds every acknowledged record, which is
  none. Treating a missing set file as a new shard instead
  (`FelixShardElectWipeNonVoterCreateCountsEmpty.cfg`, `CreateCountsEmpty`)
  finds two leaders at one generation: b opens at 2 on {b, c}, c is wiped
  and loses its promise with its set file, and the creating leader a stands at
  2 and opens on {a, c}. Only evidence of the wipe tells a new copy from a wiped one; the broker
  uses the storage id the control plane recorded for the copy. A witness
  shows a new shard opening beside a copy wiped before it answered
  (`FelixShardElectWipeNonVoterCreatesOpens.cfg`).
- The non-voter is the copy that keeps the learner mark, as the broker
  does, not a ghost of every wipe. Every control-plane write names each
  member's registered storage id, so a set written before the wipe names the
  old one (`NamesOther`), and learning it marks the copy. That alone is not
  enough (`FelixShardElectWipeNonVoterEvidenceOnly.cfg`): a promises 2 to
  candidate b and is wiped before it hears any set, the control plane fails
  over to c at 2 naming a's new id, and a answers c's fence with no promise
  and an empty log, so b and c both open at 2. So a broker the control plane
  last registered under another storage id is a learner for every shard it
  held (`ReplacedIdLearner`). A broker it never registered held nothing and
  votes.
  Any later set naming the new id re-admits it once it holds that set's
  generation-start record from the confirmed leader.
- The learner mark has to stop fences that carry no set as well. A leader
  sends one to a replica whose capabilities it could not learn, and it is
  checked against no set, so the copy's own mark is all that can refuse it.
  A copy wiped before it heard any set keeps no mark in a set file, only the
  replaced-storage marker under its data root (`SetlessSeesMarker`). Without
  it the copy answers with an empty log and a failover loses a write
  (`FelixShardElectWipeNonVoterSetlessUnmarked.cfg`).
- A cache's counter log keeps no set, so its fence is judged on the cache
  log's mark (`CounterFenceSeesLearner`); judged on its own, a non-voter's
  empty counter log counts and an acknowledged counter update is lost
  (`FelixShardElectWipeNonVoterCountersCountLearner.cfg`). The leader ships
  the two logs apart (`CatchUp`, `CatchUpCounters`), so re-admission waits
  for both to hold the start record (`ReadmitChecksCounters`;
  `FelixShardElectWipeNonVoterCountersUnchecked.cfg` re-admits on the cache
  log alone).
- Without elections the model's leader counts a non-voter's answers to its
  catch-up batches (`LearnerAcksCount`); the code's leader leaves them out
  (#1762). A non-voter also takes them from a leader the control plane named
  at that leader's generation after it moved on (`CatchUpFromDeposed`), as
  the code's confirmed read, reused for 10 s without elections, lets it. That
  cache is in memory, so its reads came after the wipe: the deposed leader's
  generation is at least the one at the wipe. Both are on in every
  configuration without elections. With elections, counting a learner loses
  a write (`FelixShardElectWipeNonVoterCreatesLearnerAcks.cfg`), which is why
  the code's leader does not.
- A stream's group logs carry no start record, so re-admission does not
  wait for them. The copy keeps a wiped mark (`marked`) past re-admission
  until its group log holds a record of its set's generation
  (`GroupsCaughtUp`): until then it refuses group fences, and as a leader its
  own group log does not count toward the group majority. The mark is kept
  in the set file, so whatever writes a newer set must carry it: with the
  promoted leader's own set write dropping it (`KeepSetDropsMark`), a group
  take retried after a failure counts the leader's empty group log
  (`FelixShardElectWipeNonVoterGroupsKeepDropsMark.cfg`).
- A shard created before sets named member ids keeps a creation set that
  names none (`IdlessCreation`), which is no evidence of a wipe. A copy
  wiped while the shard is still at generation 1 is marked by the
  replaced-storage marker alone; exempting generation 1 from it
  (`GenOneExempt`) lets that copy vote with an empty log
  (`FelixShardElectWipeNonVoterIdlessCreationExempt.cfg`).
- Without elections (`Elections = 0`), the two older faults still lose a
  write: re-admitted before it caught up
  (`FelixShardElectWipeNonVoterNoCatchUpFailover.cfg`), or with only the
  evidence a set carries (`FelixShardElectWipeNonVoterEvidenceOnlyFailover.cfg`).
  Those are the faults the code could have before elections ship; the
  elections twins above find them as two leaders at one generation.
- Witnesses show the checked paths are reached: a leader at a generation
  taken after the wipe opens without the non-voter
  (`FelixShardElectWipeNonVoterElects.cfg`), the re-admission completes
  (`FelixShardElectWipeNonVoterReadmits.cfg`), and the control plane fails
  over beside a non-voter (`FelixShardElectWipeNonVoterFailsOver.cfg`). At the creation bound the re-admission, a
  write after it and a later leader are all reached
  (`FelixShardElectWipeNonVoterCreatesReadmits.cfg`).

Two invariants count a non-voter differently. A real wipe loses records,
so a write acknowledged on {a, b} is on a alone once b is wiped, and
`AckedOnMajority` as written fails for any wipe. Under `WipedNonVoter` it
counts a non-voter as holding every record: the wipe is the one loss the
record already survived, and only voters' copies can be counted by a fence or
an acknowledgement, so what it checks is that no majority the voters can form
lacks the record. `SetChangesJoint` likewise counts a non-voter as knowing an
installed set, since it can help no old-set replica gather a majority; a
second wipe would otherwise break it by forgetting the set, not by any
majority forming. `AckedHeldByLeader` and `OneLeaderPerGeneration`, the
properties a lost write or a split brain break, are unchanged.

What is not checked:

- Liveness. The claim would be that with fairness and a majority not wiped
  a leader is eventually serving and the replica eventually re-admitted.
  The model bounds generations (`Adopt`, `Elections`) and time, and candidates
  may stand at any moment, so a run can spend every generation on duelling
  candidates and end with no leader: a liveness check fails on the bound,
  not on the rule. TLC's symmetry reduction is also unsound for liveness, and
  without it these state spaces grow about sixfold. The witnesses above show
  only that the paths exist.
- Spares and set changes. There is no fourth broker, so a re-admission
  rewrites the same set and a wipe beside a cut-over, swap or replacement is
  not checked; there the replica forgets a newer set as well. A
  re-admission onto a different set is not modelled.
- Size. Three brokers, so the overlap argument is only checked in its
  smallest shape. Failover only with adoption up to generation 2 and one
  lapse of the lease. One write, so acknowledgements after a re-admission at the
  same generation are not exercised. Two wipes at most, and a wiped leader
  only in the configurations that name `WipeLeads`.
- Planners. The control plane re-admits from a live read, so a re-admission
  decided on a stale view is not checked. Placement holds a wiped copy as a
  learner (`cpLearner`) from its registration under the new id until the
  leader's report shows it holding the start record of the control plane's
  generation (`CpAdmit`). Failover and swaps never name such a copy, and
  `PlacementKnowsLearners` checks that every non-voter is one. `CpAdmit`
  takes the copy's own re-admission as done when the report can show it,
  which stands for the code's order: a copy re-admits itself before it
  answers the batch carrying the record. The wipe (`Forget`) sets
  `cpLearner` in the same step, while the code learns of a wipe only when the
  broker registers under its new id. Until then only the broker's own guard
  keeps the copy from leading or voting; the model does not check placement
  in that window, since splitting the step would add an action to the
  largest configurations.
- Cursors. A leader's count of a replica's answers is not reset at the
  wipe, so an answer from before it can still count toward a later
  acknowledgement. That admits more than a leader that resets on the new
  storage id, so it hides nothing.
- Crash. It never fires under `WipedNonVoter` (`Wipes = 0` in those
  configurations), which relies on acknowledged records being fsynced
  (`OnCommit`): a crash then loses nothing acknowledged.
- Failure budget. At most one distinct broker is a non-voter at a time
  (`NonVoterCredit`). A second wipe of the same broker is one failure; two
  distinct brokers wiped, including `WipeLeads` with `Forgets = 2`, exceed
  what three replicas tolerate and are not checked.
- Control-plane re-admission (#1737). The control plane re-admits only from
  a report naming the copy under its current registered storage id, so a
  report sent before a second wipe cannot vouch for an empty copy. The model
  has no report ids here and cannot show this; the code's tests do.
- Three brokers. The set that re-admits a copy is fenced by both other
  copies, so its generation is at least any the copy promised before its
  wipe, including a candidate's above the control plane's. With five, a
  fence needs three of them, and that no longer follows; it is not checked.
- The creation configurations reach generation 3 only after the re-admission
  (`ThreeOnceReadmitted`), and wipe once (`Forgets = 1`). So they do not
  check a re-admission, a write, a failover and then a second wipe. A wipe
  beside an election at 3 is checked in `FelixShardElectWipeNonVoter.cfg`,
  and a second wipe after a re-admission in
  `FelixShardElectWipeNonVoterTwice.cfg`, both of which start past creation.
- Cache shards. The counter log is checked only without elections
  (`FelixShardElectWipeNonVoterSetless.cfg`), since `Elections > 0` leaves
  counters out. A stream's group logs are checked the same way, with the
  counter log standing for them (`GroupLogs`): one log, not the dead-letter
  and cursor pair, and the leader waits for a group majority where the code,
  short of answers once every replica has replied, takes the furthest one.
- Only the control plane's leader sends a fence without a set, as in the
  code, where a candidate's fence always carries its set. Such fences are
  checked only in the configurations without elections.

### The round that makes a read linearizable

`FelixShardReads.tla` adds reads to `FelixShard`. A read begins on a broker
serving the shard, takes its value (everything in its log) and remembers which
writes were acknowledged by then; it is answered later, and `NoStaleRead`
says every one of those writes is in its value. With follower acks and the
promotion fence, the deposed leader keeps believing it leads under drift and
no margin, so it keeps beginning reads. `ReadConfirm = "round"` has it answer
only once a majority has taken its fence at the read's generation after the
value was taken (`ConfirmRead`; the broker votes for itself the same way, on
`promised`), and `FelixShardReadsRound.cfg` passes: the successor fenced a
majority before it acknowledged anything, and that majority shares a replica
with the round's. `FelixShardReadsNoRound.cfg` answers on belief alone and
`FelixShardReadsLease.cfg` on the lease; TLC finds the deposed leader
answering without its successor's write in both, 14 steps in. The model has
one log per broker, so the counter log a cache replica also checks is not in
it, and the read's wait for the mark is left out: that keeps a read from
returning a record a failover can take back, a different property.

### Readers and group commits without the lease

`FelixShardSessions.tla` extends `FelixShardReads.tla` with a subscriber and
a group commit. The subscriber reads in offset order from any broker that
believes it leads, up to that broker's mark, and resumes at its next offset
when it moves; `NoLostDelivery` says everything it was handed is at the same
offset in the current leader's log. A deposed leader keeps delivering until it
learns, and that is safe because its mark covers only records a majority held
at its generation, which every later leader's fence takes up.
`FelixShardSessionsPastMark.cfg` reads past the mark and TLC finds a record
handed out that the successor replaced. A group commit is written to the
coordinator's own log and confirmed by the same round a read uses before it
is acknowledged; `NoStaleGroupCommit` says none is acknowledged at a
generation older than one that had opened before it began. Without the round,
or with the lease in its place under drift and no margin, TLC finds the
deposed coordinator acknowledging after its successor opened.

### The check that is load-bearing

With `CheckAtCommit = FALSE`, TLC finds a broker that admits a write while its
lease is valid, is paused while the lease lapses and the next generation is
granted, and then commits. That is the "process suspension" case the design
names, and the second check is what closes it.

### The wait that is load-bearing

`FelixShardHandoffNoWait.cfg` names the successor as soon as the fence is
written. TLC finds two leaders in seven steps: the control plane fences the
leader and cuts over, and the old leader has simply not seen the fence yet:
it holds a valid lease, believes it leads, and is serving. Nothing about the
lease closes this, because the lease has not lapsed; the leader is alive and
was meant to keep it.

What closes it is the leader saying it stopped. `WaitForDrained = TRUE` holds
the cut-over until a report at the fenced generation says the leader has
stopped serving and its log is not growing, and the same configuration then
explores 2.39M distinct states without a violation. The generation on the report matters
as much as the flag: an earlier leader's drained report is about a leadership
that has ended, and believing it lets the next move skip its wait.

The broker's half is `ShardLifecycle::observe`, which closes the shard's write
fence the moment a draining assignment arrives and never serves it again at
that generation, and the replication driver, which withholds the drained
report until the fence is closed with no write inside it and the successor
holds the shard's auxiliary logs as well as its main one, and names as caught
up only followers that hold both (`drain_ready` in
`crates/server/felix-replication/src/driver/shard.rs`; with no
successor, every follower level on the main log must hold them). That is what
lets the model treat those logs as part of the one log: a drained report over
the main log alone would let the cut-over drop a record the model says
survived.

### The fence at the claim that is load-bearing

`FelixShardHandoffNoClaimFence.cfg` checks the fence at admission only. TLC
finds an acknowledged record lost in eleven steps:

1. The leader admits a write. It waits to be claimed (in the broker, in a
   publish queue).
2. The control plane fences the leader, which sees it and stops serving.
3. The leader reports `drained`. The report counts claimed writes, and this
   one is not claimed yet, so the report is true as far as it goes.
4. The write is claimed and committed. Under `Leader` consistency the commit
   is the acknowledgement.
5. The control plane cuts over on the drained report, to a successor the
   report named caught up. It does not hold the record. `AckedSurvive` fails.

Nothing about waiting longer closes this: however still the leader's tail
holds, a write can wait in the queue for longer. What closes it is the claim
checking the fence: `FenceAtClaim = TRUE`, which is
`FelixShardHandoffLeaderAck.cfg`, passes. The broker's check is
`ShardFence::enter` in `services/felix-broker-service/src/shards/lifecycle/fence.rs`,
entered by every write right before it claims its place in the log and held
until the write is durable, and the drained report waits for the fence to be
closed with nothing inside it. Under `Quorum` the report-before-mark ordering
keeps such a record from being acknowledged, which is why the counterexample
needs `Leader`; the record would still land on the old leader after it said
it had stopped.

### The fence from admission, for a write acknowledged there

The claim check is only safe for a write nobody has been told about yet. The
broker acknowledges a publish when it is queued unless `ack_on_commit` is on,
and refusing that publish at its claim loses a record the client holds an ack
for. `FelixShardHandoffAdmitAckClaimFence.cfg` models it: the write is
acknowledged on admission, the leader sees the fence while it is queued,
refuses the claim and reports `drained`, and the successor takes over without
it. TLC finds `AckedSurvive` violated.

`FelixShardHandoffAdmitAck.cfg` has the write hold the fence from admission,
as `enqueue_publish` does for a publish nobody waits on: the claim is not
refused, and the drained report waits until the write is claimed and
committed. It passes.

### The copy that is not counted

`StageMove` starts the run with a move's destination already added to the
replica set and holding nothing: `staged`. The leader ships to it like any
follower, but the quorum is a majority of the rest, the replica set the stream
asked for (`ReplicaSet`); `AckedOnMajority` holds records to that set. Fence
and cut-over go to the staged node, and a promotion or cut-over that makes it
leader makes it an ordinary member.

With it left out, every safety invariant holds (`FelixShardStagedMove`,
`FelixShardStagedMoveSingle`). The reason is promotion: under
`leader-report` a failover picks only a replica the last report named caught
up, and a report holds a destination only once its log equals the leader's, so
a destination behind an acknowledged record cannot be picked. Under
`log-order` promotion this would not hold with one replica, since the
destination is then the longest live log; the implementation promotes from the
report.

Counting it (`LearnerVotes`) is not unsafe, only slow, so the companion
configuration checks a latency property instead: `StagedCopyNeverDelaysAck`
says that whenever the replica set would acknowledge a record, the leader can.
`FelixShardStagedMoveVotes` finds the leader holding a record that it, the
stream's only replica, has written and reported, and unable to acknowledge it
until the destination has copied it, which on a real shard is the whole
copy.

### A crashed leader's own refusal

A leader that crashed under a relaxed fsync mode and gave its copy up still
holds its promise, so it could answer its successor's fence, and counted
there its short log could stand in for the follower that holds what it lost.
The broker refuses that fence until a newer leader has shipped to it
(`refuse_short_copy`), except where a set of two acknowledged what it may
have lost (`acknowledged_by_two`, #1484). `RefuseShortCopy` models both:
`ShortCopy` is the given-up copy no newer leader has had promise since, and
`AckedByTwo` the exception: the copy's last record is from the generation
it led, the fencer leads the next one, its set is the same two, and the failover's fence ids name exactly those two (`FenceIds`,
the old set and the new one as #1633 names them, from the last report the
control plane took, which a set change does not clear). Configurations that
leave it off are unchanged: every variable it adds stays at its initial value.

Identities are left out of these configurations (`StorageIds = FALSE`), so
every fence answer counts as from the copy the ids name. In the code a
rotated copy named by its old identity is never counted at all, and the
exception changes the outcome only where the ids name the copy the crashed
leader answers from, as after its own given-up report.

`FelixShardShortCopySeat.cfg` is the RF 2 seat with the crash before the
seat's generation is reported, the shape of #1486. `FelixShardShortCopyStaged.cfg`
is a failover beside a staged destination, #1635: there the code's fence ids
name the staged copy and the exception refuses, so the configuration drops
the ids' comparison to check that naming only the set that acknowledged
would be safe while the staged copy never votes. Both reach an open on the exception: a temporary invariant
saying no leader opens counting a short copy's answer fails in each. Neither
finishes exhaustively in the time CI has (past 30M distinct states, start
records on, without an end in sight), so they are nightly walks, sampled.
`AckedOnMajority` is not checked: a lost copy is a lost copy, and a record
two acknowledged is on one of them until the new leader ships it.

Dropping the whole set check (`ShortCopyChecksSet`) loses a record:
`FelixShardShortCopySeatAnySet.cfg`. Dropping only the ids' comparison
(`ShortCopyChecksKeys`) does not in the model:
`FelixShardShortCopySeatAnyKeys.cfg` passes (sampled nightly, like the Seat
shape it drops the comparison from). No failover there shrinks the
voting set, so a fencer with a set of two at the generation after the one the
crashed leader led has the two it led; the comparison only refuses more,
around copies outside the set (a staged destination, a dropped copy a report
still names). It stays in the code against placement that would shrink a set
at a failover.

A staged destination can still count. The broker leaves it out of the quorum
only if the leading process saw it added (`staged_learner`), so a leader
restarted cleanly at that generation counts it toward a `Quorum` write
(`LearnerVotes`). `FelixShardShortCopyStagedVotes.cfg` is that shape at RF 2:
the set is `{a, b}` with c staged, a acknowledges a write on a and c, crashes
without it, and the failover sets `{b, a}` and drops c. The code's fence ids
still name c, so a refuses b's fence and the shard stays closed until
`abandon`. It passes, and that refusal is what keeps the write. Its twin
`FelixShardShortCopyStagedVotesSetOnly.cfg` takes the fix #1635 proposed, ids
without the staged copy (`FenceIdsSetOnly`), and violates
`AckedHeldByLeader`: a answers, and b opens without the write. Both are
nightly walks. The fix waits until every leader treats a staged copy the same
way.

### The ordering that is load-bearing

This is a counterexample with the fix taken out, not a bug in the broker. With
`ReportBeforeAck = FALSE`, TLC finds this in a second:

1. The leader reports its two followers level with it.
2. It admits and commits a write, ships it to one follower, and acknowledges it
   under `Quorum`: leader plus one follower is a majority of three.
3. It dies before the next report leaves. The report the control plane holds is
   recent, and predates the acknowledgement.
4. The lease lapses, the margin passes, and the control plane promotes the
   *other* follower, which the report named as level. It holds no copy of the
   acknowledged record. `AckedSurvive` fails.

Report expiry does not close this. The design's expiry is about reports older
than the time it takes to notice a leader is gone; this report is fresh, it is
just older than the last acknowledgement.

**What closes it is ordering, not freshness**, and the broker does it: the
leader reports who holds the record, waits for that report to land, and only
then moves the quorum mark that releases the acknowledgement. That is
`publish_mark` in `crates/server/felix-replication/src/driver/shard.rs`, which moves the
mark only `if reported`, and `await_quorum`, which blocks the publish on the
mark. With `ReportBeforeAck = TRUE` (`FelixShard.cfg`, the implemented design),
TLC explores 5.4M distinct states and finds no violation.

So the pair is the point. The ordering is not merely present in the code; the
model shows the guarantee fails without it.

> A caution on reading a pass. `FelixShard.cfg` passing is only meaningful if
> acknowledgements actually happen under the added precondition. A
> precondition nothing can satisfy would make `AckedSurvive` vacuously true.
> Checked by hand with a temporary `acked = {}` invariant, which TLC violates
> in 2,307 states: acknowledgements are released, and the pass is about them.

Since #1924 the broker never names a follower short of the log its leader
inherited (`acknowledged` in crates/server/felix-replication/src/driver/shard.rs),
which the model calls `ReportFloor`. The floor is not a pure guard, so a pass
without it does not carry over. It shrinks the report's `holders`, and
`QuorumReportNamesASuccessor` needs them non-empty. It raises the report's
`len`, and under `ReportBeforeAck` that is what an acknowledgement may reach.
So every configuration whose report reads that way (`Quorum`, a `ReportBound`
other than `"tail"`, and `ReportBeforeAck` or `QuorumReportNamesASuccessor`)
sets `ReportFloor = TRUE`, the witnesses included, and each still does what
the table says. `FelixShardReportFromAnswers.cfg` keeps it off: it is the
witness that the floor is needed.

Promotion by log order finds no trace, in the same bounds. A replica holding an
acknowledged `Quorum` record is in every majority that could acknowledge one
after it, so the replica with the greatest (last generation, length) among the
live ones holds every acknowledged record; the generation comes first because
a stale proposal from an older leader can be longer than the log that
superseded it. That rule needs each replica to say where it is (a position on
its own heartbeat) rather than the leader to say where its followers were.

### One record per commit

`FelixAtomicCommit.tla` is a small model of its own, for
[atomic commits](../atomic-commit.md): an event, a state update and an
enqueue written together on one shard. Leadership is plain Raft (the vote,
the committed mark counting only the leader's own generation) because the
question is not whether the log survives a failover, which `FelixShard.tla`
answers, but whether a mixed batch survives it whole. Each broker keeps three
views of its log and applies committed records to them; `NoPartialCommit`
says no view set anywhere shows part of a commit, and `CommitSurvives` says a
commit any view shows is still in the leader's log.

The broker writes a commit as one record, and the model passes. Written as a
record per part, it fails even with the leader's mark stopping only at a
commit's end: replication ships by bytes, a follower can hold the event
without the state, and once promoted it keeps that half and its mark covers
it with its next commit. Applied a part at a time, it fails on one broker
with no failover at all. Those two are why the record is one record and why
the state view is updated under the ring's lock.

### A trim is a record

`FelixHeadTrim.tla` is a small model of its own for the conditional head
trim designed in issue 1662 and described in
[Conditional head trim](../replication-design.md#conditional-head-trim).
Nothing in it is built yet; every configuration says so in its evidence
line. Leadership is the same plain Raft as `FelixAtomicCommit.tla`, with a
generation-start record, so the question is only what a trim adds.

A trim is a record carrying the floor it asks for and its holder's tag,
appended only at the tail its holder expects. Each broker derives a head
entry from each trim record it holds, applies the floor when its commit
offset passes the entry, and deletes anywhere up to the lower of that floor and its
commit offset, which stands in for segment boundaries that differ between
replicas. A read from below where the serving leader can serve, the floor or
a base past it, gets `CursorTooOld` with the tag of the trim that set the
floor; readers skip trim records. The serving leader answers reads, and so
does a follower for a read that could start below a floor (a resume from an
offset or the earliest record, or a history page): it first takes a read
index from a leader that a majority confirms, and serves once its own commit
offset reaches it. Two lease holders write and trim, the
second taking over from a first that is not told, and the second makes a
fenced write before it trims. A holder learns the tail by reading from its
own cursor and stops on a newer epoch, in a write or in the refusal's tag. A
trim at or below the floor is a no-op that checks the tail and answers with
it. A deposed leader keeps taking writes and trims until a newer generation
reaches it, and a commit and its answer are separate steps. The passing
run is split in four, since together they do not finish in an hour (the
first, `FelixHeadTrim.cfg`, runs by dispatch only):
`FelixHeadTrim.cfg` has three trims or no-ops and one promotion,
`FelixHeadTrimCrash.cfg` adds a broker crash that reads its commit offset
back behind, `FelixHeadTrimTwoPromotions.cfg` has a second promotion, and
`FelixHeadTrimCrashTwoPromotions.cfg` has both with one write and one trim.

A crash keeps the broker's log. A record it had not flushed was counted by
no quorum, as replicas acknowledge and the leader counts itself only after a
flush, so losing it leaves the same state as never having received it, which
the model reaches without the crash. The head entries are derived from the
records, so they go with them.

`NoReadBelowAckedTrim` says the serving leader serves nothing below an
acknowledged trim's floor, and a follower serves nothing below the floor of
a trim acknowledged before its read began, `NothingLostAboveFloor` that it holds every
acknowledged write at or above it, `FloorRecordKept` that its floor is the
one its trim records set, deleted ones included, so deletion never removes
the record a scan needs, `NeverMoreThanAsked` that no broker's
floor is above what acknowledged trims asked for, and `StaleTrimRefused`
that no committed write or trim from one holder follows a committed write
from a newer one.

Each twin drops one rule and TLC finds the trace it prevents. Kept as the
leader's own state, the floor is lost at the first promotion. Taken from
where segments start, it lags the floor asked for. Applied when stored, it
lets a leader delete acknowledged writes below a trim that a failover then
cuts. Deletion that follows stored trims removes acknowledged writes for a
trim nobody acknowledged. A trim that ignores the tail lands after the new
holder's write. A no-op answered without the tail check hands the stale
holder a tail past the new holder's write. Retention that deletes past the
floor can delete the new holder's fenced write with no trim at all, which is
why felixd's streams run with retention off. A follower that serves from its
own state without the read index serves below a trim the leader committed
with the other follower and acknowledged.

`FelixHeadTrimHiddenTrims.cfg` is a finding. The design first had readers
skip trim records with nothing in `CursorTooOld` but where the log starts.
TLC found a holder that took the lease and lost it before writing: the new
holder made its fenced write and trimmed past it, and the old one, refused
as too old, resumed at the floor, saw no newer epoch, wrote at the tail and
trimmed after it. The design now returns the floor-setting trim's headers,
where the holder's epoch is, in `CursorTooOld`, and the model passes.

### Group state: two logs per replica

`FelixGroups.tla` models a stream shard's consumer-group state as what it
is: a dead-letter log and a cursor log on every replica, each flushed on its
own, each shipped and replaced on its own, with crashes that keep whatever
each had on disk. A group passes a record by finishing it or by giving up on
it, which writes a dead letter first. `NoSkip` says that on the serving
leader every record a group's cursor has passed was finished or is that
group's dead letter.

Nothing about the order of writes or shipping keeps that on its own: a
follower's dead letters can be lost in a crash its cursors survive, or
replaced by a newer leader's before the cursors that relied on them are.
What keeps it is that each cursor record names the dead letters it needs
(the log's length at the commit and the generation at that length), the
leader syncs its dead letters before a cursor that needs them and before it
ships them, so one generation never puts two different entries at one
offset where anyone could see both, and every broker checks its cursors
against the dead letters it holds before it serves, putting a group back to
its latest standing cursor or its origin, never removing it.
`FelixGroupsNoCheck.cfg` serves cursors unchecked, `FelixGroupsLengthOnly.cfg`
checks the length alone (a lost dead letter's offset refilled by another
group's), `FelixGroupsDropGroup.cfg` drops a group with nothing standing,
and `FelixGroupsShipUnsynced.cfg` ships dead letters before syncing them (a
crash loses one a follower holds, and the leader writes another at the same
offset and generation); TLC finds a skip in each. The last one was found by
TLC in the model as first written, and is why the leader syncs before it
ships.

The promotion fences both logs in one round, takes the dead letters of the
answer furthest ahead and merges that replica's cursors into its own, or
serves its own pair when the round falls short. `FelixGroupsSplitSources.cfg`
takes the dead letters from one replica and the cursors from another, and
passes: with the check in place the pair need not come from one place, and
taking it from one only keeps more of the groups' progress. With the check
comparing lengths only, `FelixGroupsSplitSourcesLengthOnly.cfg`, it fails.

The fence itself, the lease and the records' log are `FelixShard.tla`'s; this
model takes from it only that a promotion fences a majority and a replica
refuses a leader older than one it accepted.

### One engine: Raft on the segment log

`FelixRaft.tla` is the first part of the model for the 0.8 engine: one Raft
group whose log is Felix's segment log, as
[The replication engine interface](../replication-engine.md) decides it. The
`raft_log` adapter that puts openraft's log on `DiskLog` is in
`crates/server/felix-storage/src/raft_log.rs` (#2217, #2222; purge and the
snapshot file are #2255). The configurations' evidence lines do not cite its
tests yet (#2297 tracks that). The model states what the adapter has to
guarantee, and reviews of the adapter are checked against it.

Three brokers run elections and replicate a log of three kinds of entry: a
leader's blank entry, a membership entry and a client record. The offset is
the Raft index and the record's generation is its term. Votes are openraft's
with one leader per term (`leader_id_std`): a term, a broker and a committed
flag, where a committed vote is above every
uncommitted vote of its term. A restarted broker is a follower even when its
stored vote says it led.

Storage is modelled at the level of the adapter's contract. `append` returns
with the entries readable and not yet on disk, and a flush later moves the
durable mark and fires the callback. A crash keeps the log up to some point at or
past the durable mark, and a record cut short there is dropped at the open.
`truncate_after`, the vote file and the snapshot file each take effect in one
step: the model treats each as on disk when the call returns, and the files
are replaced by rename. A purge is one step too. The model does not ask
whether it survives a crash, since a purge that is lost only leaves more log.
The state machine is rebuilt at a restart from the snapshot and the log up to
the committed index read back.

A follower takes a leader's snapshot the way openraft does, as three writes:
it cuts its log back to the commit index if the log holds another entry at
the snapshot's index, replaces the snapshot and the state machine, and purges
the log up to the snapshot's id. A crash can fall between any two. After the
second write the log is still the old one: it ends below what the broker
applied and can hold entries the snapshot replaced. The open
restarts the log at the snapshot, which is the purge past the log's end that
openraft issues at startup.

One subscriber reads from brokers that believe they lead, up to the commit
index. Each delivered record reports the blank and membership entries
directly below it, and the serving broker counts them from its own log. It
does not know what an earlier leader delivered.

Each check has a configuration that drops one rule and must violate it, and
the model has witnesses that the behaviour the checks are about is reached.

| Check | What it says | Dropped rule that breaks it |
| --- | --- | --- |
| `OneLeaderPerTerm` | No term elects two brokers. | `VoteNotDurable`: `save_vote` returns before the vote is on disk. |
| `LogMatching` | Two logs that agree on an entry's term agree on everything up to it. | `LeaderResumes`: a restarted leader leads again in its stored term. `TornTailKept`: recovery reads a torn record as an entry. |
| `TermsMonotone` | Terms never go down along a log. The open-time scan refuses a log that breaks this. | None. It holds whenever the others do. |
| `LeaderCompleteness` | A leader holds every entry committed in an earlier term. | `CommitPriorTerm`: a leader commits an earlier term's entry by counting copies. |
| `StateMachineSafety`, `AppliedAgree` | Every broker applies the one committed sequence, so no two differ on an applied entry. | `SnapshotTwoRenames`: the snapshot's data and id are replaced separately. `TruncateNotDurable`: `truncate_after` returns before the cut is on disk. |
| `AppliedInLog` | No cut reaches below what a broker applied, and the log holds everything applied except between a snapshot's install and its purge. `DiskLog`'s own commit offset is not modelled; kept at or below the applied index, its `BelowCommit` refusal never meets a correct cut. | None. It holds whenever the others do. |
| `AckedSurvive` | An acknowledged record is on the disks of a majority. | `AckBeforeFlush`: the callback fires before the fsync. `RecoveredTailUnsynced`: entries found at the open are reported flushed without an fsync. `PurgeBeforeSnapshot`: purge runs before the snapshot covering it is stored. |
| `PurgedCoveredBySnapshot` | No log is purged past its stored snapshot. | None of its own. `PurgeBeforeSnapshot` and `SaveCommittedUnflushed` break it first, and their configurations are held to the harm that follows. |
| `OffsetsDense` | Delivered offsets, less the skipped ones each serving broker reports from its own log, have no gap. | `SkipsNotCounted`: a batch does not report them. `DeliverUncommitted`: a subscriber is given entries above the commit index. |
| `OffsetNeverReused`, `DeliveredInOrder` | A delivered offset holds that record in the committed sequence, and the k-th delivery is the k-th committed record. | `DeliverUncommitted`: a subscriber is given entries above the commit index. |
| `VoteNeverLowers` | The stored vote only rises. | None. |
| `AckedDelivered` | When nothing more can happen under a leader no broker has outvoted, the subscriber holds every acknowledged record. Checked where there is a subscriber and no snapshot (`FelixRaft.cfg`, `FelixRaftOffsets.cfg`, `FelixRaftOffsetsFailover.cfg`). | `SaveCommittedUnflushed`: the committed or applied index read back is past the recovered log. |
| `QuiescentServed` | When nothing more can happen and a leader no broker has outvoted stands, it has committed its whole log, acknowledged every record in it, brought every broker level and fed the subscriber. | None. This is the liveness check. |

`OffsetsDense` is the client's own drop check. The subscriber resumes after
the last offset it received, and whichever broker serves it next counts the
entries directly below the next record that hold no event, from its own log.
The check holds because that broker's committed log is the one the earlier
deliveries came from, also across a change of leader
(`FelixRaftOffsetsFailover.cfg`). Without `skipped_before` it fires at every
blank or membership entry. `DeliveredInOrder` is the stronger statement.
Numbering only the records gives a count with no gaps, and the subscriber
receives exactly that sequence.

Three dropped rules are held to the harm they cause, not to the check that
restates them. `TruncateNotDurable` lets a follower learn the commit of the
entry that replaced a cut tail before the cut is on disk. A crash brings the
old tail back under the stored commit index and the restart applies an entry
that was never committed. `SaveCommittedUnflushed` leaves a broker whose log
starts past entries it lost. The model marks them lost in its state machine,
so `StateMachineSafety` would fail by construction and is not the check used.
The harm shown is that the broker is elected and a subscriber resuming below
the start of its log is never given an acknowledged record
(`AckedDelivered`). `PurgeBeforeSnapshot` leaves an acknowledged record on
one disk.

The passing run is split by what it varies, each with three brokers:

- `FelixRaft.cfg`: two terms, one write, one crash and the subscriber.
- `FelixRaftCrashes.cfg`: two crashes.
- `FelixRaftTerms.cfg`: three terms, no write and no crash.
- `FelixRaftFourTerms.cfg`: four terms, no write and no crash. Three terms
  pass with or without the rule against committing an earlier term's entry by
  counting copies. Four are the fewest where the rule matters:
  `FelixRaftCommitPriorTerm.cfg` has the same bounds without it and fails.
- `FelixRaftSnapshot.cfg`: a snapshot, a purge and a follower brought up by
  snapshot in three writes, with one crash.
- `FelixRaftSnapshotReplace.cfg`: two snapshots and no crash, so a broker
  replaces a snapshot it already stored.
- `FelixRaftOffsets.cfg`: two writes and a membership entry under one leader,
  with the subscriber.
- `FelixRaftOffsetsFailover.cfg`: two writes over two terms with the
  subscriber, so two deliveries can come from different leaders.

Five take more than ten minutes on a CI runner, so a pull request checks them
only when it changes `FelixRaft.tla` or the configuration, and main checks
them on every spec change: `FelixRaftFourTerms.cfg`, `FelixRaftTermsFull.cfg`
(three terms with a write), `FelixRaftOffsetsFull.cfg` (one write and a
membership entry across a change of leader),
`FelixRaftOffsetsFailoverCrash.cfg` (`FelixRaftOffsetsFailover.cfg` with one
crash) and `FelixRaftSnapshotCrashes.cfg` (`FelixRaftSnapshot.cfg` with two
crashes).

The eight `FelixRaftReach*.cfg` witnesses each claim something never happens
and must be violated: a record acknowledged under one leader held by a later
one after a crash, a follower cutting off a conflicting tail, a crash losing
an unflushed tail, a blank entry between two delivered records, two
deliveries in a row from leaders of different terms, a follower brought up by
snapshot, a crash between an install's second and third write, and a broker
replacing a snapshot it stored.

#### What the adapter must guarantee

The model passes only with these rules, and each maps to a call the adapter
implements.

| Model step | openraft call | Rule |
| --- | --- | --- |
| `LeaderAppend`, `Replicate`, `Flush` | `append` and its `IOFlushed` callback | Entries are readable when `append` returns. The callback fires only after the fsync that covers them. |
| `Replicate` with a cut, `InstallCut` | `truncate_after` | The cut is on disk before the call returns. Durable by the next flush is not enough: a follower applies and saves a commit index past the cut before it flushes, and a crash then brings the old tail back under that index. |
| `Restart` | the open, then `get_log_state` | A torn record is cut off, never read as an entry. Everything reported at the open is on disk, because openraft takes it as flushed. `DiskLog`'s open already syncs the active segment before it returns. |
| `Restart` | `read_committed`, `applied_state` | Neither index read back is past the recovered log. openraft commits an entry before the local copy is flushed, so the stored index has to be held back or clamped at the open. With `read_committed` past the log, openraft fails to re-apply and the broker does not start. With `applied_state` past it, openraft restarts the log at that index and trusts the state machine to hold what is missing; a shard's state machine is the log, so those records are gone from the replica. That second case is the one the model's knob takes. `DiskLog` loads its stored commit offset as it is, so the adapter has to clamp, and has to lower `DiskLog`'s own offset with it or a later correct cut is refused with `BelowCommit`. |
| `SaveVote` | `save_vote`, `read_vote` | The vote, with its committed flag, is on disk before the call returns, and the stored vote never goes down. |
| `BuildSnapshot`, `InstallState` | `build_snapshot`, `install_snapshot` | The snapshot's data and id change together. A crash leaves the old pair or the new pair. |
| `InstallCut`, `InstallState`, `InstallPurge`, `Restart` | `truncate_after`, `install_snapshot`, `purge`, then the open | A snapshot install is three writes, and the broker opens after a crash between any two. After the second, the log ends below the snapshot, and openraft purges up to the applied index at startup. |
| `Purge`, `InstallPurge`, `Restart` | `purge` | Only up to the stored snapshot. A purge to an id past the log's end works: it leaves an empty log that starts at that id. |
| `Deliver` | the broker's read bound | Subscribers read up to the commit index. A batch reports the offsets directly below its first record that hold no event, counted from the serving broker's log. |

A restarted leader must not lead again in its stored term. openraft 0.10
handles that itself: it lowers the committed flag in memory at startup. It
then campaigns for a new term when `enable_leader_restore` is on (the
default) and the broker is a voter, and stays a follower otherwise.

#### What it does not cover yet

Issue #2198 lists the rest, and none of it is in this module (a membership
entry here only holds an index). Membership changes, copy identity,
re-forming a group, the switch from the hybrid protocol, the leader lease and
sleep each have a module of their own, in the sections below. They leave storage abstract, because
this module covers the adapter's crash rules. A message is one step that reads
both brokers, so the model has no lost, repeated or reordered messages beyond
a leader acting on an old answer. A broker in the middle of a snapshot
install takes no other step until the three writes land or it crashes. The
`SnapshotTwoRenames` rule is dropped for a snapshot the broker builds, not
for one it installs. The order of writes inside one adapter call is left to
the simulator's power-loss crash points.

### Raft election liveness

`FelixRaft.tla` checks liveness only on states where nothing more can happen
(`QuiescentServed`), and only for a leader that stands there: a state with no
leader passes it, so it never asks whether one is elected.
`FelixRaftSleep.tla`'s `QuiescentLed` does ask, on the same kind of state, but
that model is deferred with sleep. `FelixRaftElect.tla` asks as temporal
properties under weak fairness, for elections as openraft 0.10 runs them with
Felix's timers: pre-vote, the leader lease, a restarted leader standing again
at once (`enable_leader_restore`), and a randomised election timeout.

- `ElectionSettles`: if from some point on a majority of the voters is up and
  connected, the group ends up with a leader among them that none of them has
  outvoted, and keeps it.
- `EventuallyServes`: under the same condition that leader's blank entry is
  committed, so the shard is open (`opened` in `raft/lead.rs`), and every
  member of the majority follows it and holds its log.

Faults are crashes and cuts, a cut being one member that reaches no other.
There are at most `MaxFaults` of them. A crashed member may come back and a cut
may heal, but nothing makes either happen. "Stays so" is the suffix after the
last fault: who is up and connected never changes again. Weak fairness covers
the election timeout, the pre-vote, each vote answer, the step to leader, each
append and the commit of the blank entry. Each needs its member up, and each
message needs both ends up and connected, so a crashed or cut member is owed
nothing. Faults, restarts and heals are never owed.

Time is abstract. An election timeout fires only while no leader the member
would follow can reach it, since that leader's heartbeat comes first. The
randomised timeout is what keeps rounds apart, and the model cannot say how
long that takes. So with `Staggered` at most `Splits` timeouts in a behaviour
fire while another round is open (a pre-vote that passed and has not stood, or
a candidate that can still win). `FelixRaftElectLiveSplits.cfg` drops the rule:
all three members pass their pre-vote before any of them stands, they split the
term three ways, and they do it again until the terms run out. Pre-vote does
not prevent that. It only stops a member nobody would elect from raising its
term.

`Splits = 1` excludes a behaviour in which two rounds collide. openraft draws
each timeout from 1 to 2 seconds on top of a fixed per-member offset, and a
vote round takes one round trip, so a collision is rare and two in one election
are taken as none. Repeated collisions are unlikely but possible, and the model
does not show that an election ends under them.

Terms are bounded too, and a behaviour that uses them all with no leader
fails the check, so the passing configurations allow more terms than their
faults and the one split can spend. The count: one term for the first
election, one for the split, and at most three for each fault (the election it
forces, the term a crashed leader restores itself at, and one pre-vote that
passed before a new leader's heartbeat reached the member and stands anyway,
deposing that leader). One fault makes five, two make eight. Two faults and six
terms fail on exactly that stale stand.

The count is an upper bound, and it is not taken on trust.
`FelixRaftElectLiveSlack.cfg` and `FelixRaftElectLiveFaultsSlack.cfg` give one
term more (six and nine) and check `TermsLeft`, that no member ever reaches the
last term. It holds, so the bound never stops a step in those runs, and their
verdict is the verdict with terms unbounded. Each finds exactly the states of
the configuration it extends (32,616 for one fault, 587,152 for two), and
`FelixRaftElectLiveFaults.cfg` checks `TermsLeft` itself: with two faults no
member reaches the eighth term, so eight has room to spare.

What is proven, then: with three voters, at most two crashes or cuts, and at
most one colliding round, once a majority stays up and connected a leader is
elected among them, stays, opens, and is followed by all of them, for any
number of terms. What is not shown: more than two faults, more than three
voters, a partition other than one member cut off, lost or reordered messages,
membership changes, repeated colliding rounds, and the lease's own timing,
which `FelixRaftLease.tla` covers.

### Raft membership change and the fence around it

`FelixRaftMembership.tla` models one Raft group changing its voter set, as
[per-shard Raft](../per-shard-raft.md) decides it ("Membership" and "Entries
openraft writes itself"). It has four brokers: three voters and a newcomer.

What is modelled:

- Membership is carried by membership entries. A broker uses the latest one in
  its own log, committed or not. A configuration is an old set, a new set and
  learners. While old and new differ it is joint, and an election or a commit
  needs a majority of both.
- A move is three entries: the newcomer added as a learner, the joint
  configuration once the learner is level, and the entry that leaves it.
  openraft writes the last one itself when the joint entry is committed. A new
  leader that finds a joint configuration repeats the call.
- A change starts only after the previous membership entry is committed. A
  leader outside the new set steps down once that set is committed. The design
  transfers leadership first; the model has only the step-down.
- A publish claims the next offset on the leader and later hands the record to
  Raft, which gives it the next index. Before a membership call the engine
  holds the fence and waits until no claim is left. The fence reopens when
  every entry the call proposed is committed, or the call was refused before
  appending. If Raft assigns an index a claimed record is not at, the group
  stops.

What is checked: `OneLeaderPerTerm`, `LogMatching`, `LeaderCompleteness`,
`StateMachineSafety`, `AckedSurvive` and `NeverStops`. `AckedSurvive` here says
that no broker that could still win an election lacks an acknowledged record:
it is a voter of the configuration it holds, and the brokers whose logs are not
ahead of its own make a majority of each voter set. "On a majority of each
voter set" is not an invariant across a change. A record committed under the
leader's latest configuration need not be on a majority of an older one, and
the log comparison in the vote is what keeps it.

Each knob drops one rule:

- `SkipJoint`: a move swaps one voter for the learner in a single entry.
- `ChangeBeforeCommit`: a change is proposed while the previous membership
  entry is uncommitted.
- `LearnerCounts`: a learner's vote and its copy count toward a majority.
- `ClaimRacesMembership`: the membership call is made with the fence open.
- `ReopenOnReturn`: the fence reopens when the call returned or timed out,
  with an entry still queued or uncommitted.

A removed leader that never steps down has no twin. It commits with the new
set's majority and does not count itself, so it costs liveness, not safety.

Abstracted: storage (an entry in a log is durable; `FelixRaft.tla` covers the
adapter's fsync rules), crashes, snapshots, the subscriber and the lease. A
message is one atomic step between two brokers. A deposed leader's claims are
dropped, which stands for the adapter's term gate.

### Acknowledging a `Quorum` write on a written majority

`FelixRaftWritten.tla` models the early acknowledgement of a `Quorum` write on
a `Periodic` stream, as [per-shard Raft](../per-shard-raft.md) decides it
("Two signals from one append"). A follower tells the leader how far its log is
written before its fsync, and the leader acknowledges a record once a majority
of the voters has written it, or at its commit, whichever is first.

What is modelled:

- A broker's log is what it has written. `synced` is how far that is on disk.
  A process crash keeps the whole log, because the kernel has it. A power cut
  keeps a prefix no shorter than `synced`.
- A written report is the id (term and index) of an entry the sender holds:
  its last one for a strict follower, or, for a relaxed follower, whose
  answer is its report, the last one of the append it answered. The leader
  keeps the latest one from each member and can act on it after the
  member's log has changed. Nothing ties a report to the sender's vote, and
  nothing clears reports at an election: the term on the log id is the whole
  defence.
- The leader counts a member for a record of its own term when the report's
  log id carries the leader's term and is at or past the record. It counts
  itself. Only voters of the latest configuration in its log count, a majority
  of each half while it is joint, and never a learner.
- The count releases the acknowledgement and nothing else. The commit index
  counts fsynced copies, as in `FelixRaft.tla`.
- A voter compares the candidate's log with its own, unsynced entries
  included, as openraft does. Comparing fsynced positions instead would let
  a candidate without an acknowledged record win a follower that has
  written it, and cut it there, with no crash.
- One membership change: three voters and a learner, and a swap of one voter
  for the learner through a joint configuration.

What the acknowledgement promises:

- `AckedHeld`: no broker that could still win an election lacks an
  acknowledged record, unless a power cut took that record from a copy before
  its fsync and the record was never committed. "Could still win" is the same
  test as `AckedSurvive` in `FelixRaftMembership.tla`.
- `AckedNeverLost`: the same with no exception. It is checked wherever no
  broker loses power.
- `CommittedHeld`: no broker that could still win an election lacks a
  committed entry, power cuts included.

What has run to the end: one term with two writes, a process crash and a
power cut (`FelixRaftWritten.cfg`), and the same with two crashes and no power
cut, where `AckedNeverLost` passes too (`FelixRaftWrittenCrash.cfg`), a
change of leader with a power cut (`FelixRaftWrittenFailover.cfg`), and the
membership change (`FelixRaftWrittenJoint.cfg`). Two terms with two writes
(`FelixRaftWrittenTerms.cfg`) and the membership change across a change of
leader (`FelixRaftWrittenJointFailover.cfg`) found no violation but did not
finish; the table above has how far each got. Every twin below, the joint and second-term ones included, fails
within seconds.

A power cut is the one way the acknowledgement could lose a record: a copy
that held the record loses power before its fsync, and a majority then elects
without a copy that still holds it. At three voters the leader and one follower
hold the record, one of the two loses power, and it and the third voter elect
while the last copy is not heard. The copy that loses power can be the
leader's own. `FelixRaftWrittenPowerCutKeeps.cfg` pins that as an expected
violation of `AckedNeverLost`, with every rule on, one power cut, and
`CutRotates` off.

The broker closes it as the hybrid does (issue #2423). A copy syncs a marker
in its data root before it first tells a leader of an unsynced write, or
counts its own as leader. A root with the marker, or on a relaxed fsync mode,
records every run as relaxed, so after a power cut it starts under a new
storage id and each copy under a new copy id. A copy on `OnCommit` that never
vouched keeps its id: no leader counted it for a record it had not synced. So
after a site-wide power loss a cluster with every broker on `OnCommit` keeps
every voter, and a cluster that acknowledged on written reports rotates
every copy that vouched.

The model does not track which copies vouched. `CutRotates` rotates every cut
copy, which is the code's behaviour for a copy that vouched; the model's cut
only removes an unsynced tail, while the code also rotates a vouched copy
whose tail was all synced, which removes a voter and adds no behaviour. A
copy that never vouched is the `CutRotates = FALSE` cut of a copy that no
report or leader's count named, which the model does not separate from the
unsafe one. With `CutRotates` the cut broker
leaves the voter set as that identity: its log is empty, its vote cleared, and
it never votes, campaigns, answers or counts again. The leader keeps whatever
it already heard from the old copy. `FelixRaftWrittenPowerCut.cfg` is the same
model with `CutRotates` on, and passes `AckedNeverLost` with the other
invariants.

That is safety only. The model checks no liveness, and the price shows up
there: the rotated seat stays empty, because adding the new copy needs a
membership change that per-shard Raft does not make yet. A group that loses a
seat this way keeps going on the other two, and one more unreachable voter
closes the shard until the copy holding the record returns. The hybrid's
closed shard after the same crash is the same cost.

A failed flush counts as a power cut for that copy. A Raft log whose flush
failed is cut back to its last sync when it restarts, so it comes back without
the record just as if it had lost power, and the model's power cut covers it.
The code is stricter: such a copy restarts under a new copy id and does not
vote with the cut log (`docs/per-shard-raft.md`, "A poisoned log").

Each knob drops one rule, and each twin fails with no power cut at all:

- `LearnerCounts`: a learner's report counts. The leader and the learner hold
  the record, and the other two voters elect without it.
- `OtherTermCounts`: a report counts by its index, whatever term its log id
  carries. A deposed leader's unreplicated tail reaches past the new leader's
  record, the new leader counts it, and the deposed leader can be elected
  again without the record.
- `JointOneHalf`: a majority of either half of a joint configuration is
  enough. A record after an uncommitted joint entry is acknowledged on a
  majority of the new half, and the old voters, which never saw the joint
  entry, elect without it.
- `WrittenCommits`: the commit index counts written reports too. An entry
  commits on copies that are not on disk, and one power cut takes it.

`WrittenCommits` is also a proposal, written up in
[Per-shard Raft](../per-shard-raft.md) under "Proposal: commit on a written
majority on relaxed brokers" and not built: on a broker where a power cut
rotates every copy, openraft hears an append is flushed once it is written,
so the commit, apply and delivery move on a written majority. With
`CutRotates` the twin's loss goes away: the copy that lost the entry no longer
votes (`FelixRaftWrittenCommitsPowerCut.cfg` passes, and
`FelixRaftWrittenCommitsPowerCutKeeps.cfg` without the rotation does not).

Two more knobs model what the proposal needs after a power cut of a majority:

- `MaxReforms`: with 1, once no broker leads or can be elected, the operator
  re-forms the group (`Reform`). The base is the longest log any copy holds
  or a rotated copy kept, and the new epoch starts above it, padded up to the
  floor: the last commit index a leader reported to the control plane
  (`ReportFloor`). `FelixRaftReform.tla` has the command's steps and why the
  floor carries a term. Every re-form configuration here has one term, where
  every log is a prefix of the leader's and the longest is the base. With two
  terms and the commit on written copies, the longest kept log can hold an
  uncommitted entry at an offset another term committed. `Vouch` checks that
  case ([#2592](https://github.com/GetFelix/felix/issues/2592)).
- `Vouch`: the re-form rule of the design for
  [#2250](https://github.com/GetFelix/felix/issues/2250). Let T be the highest
  term of any vote the re-form read and of the floor. A rotated copy keeps the
  term it held when cut (`keptVote`) and its saved commit (`keptCommit`). A
  copy vouches for its saved commit, for the floor's entry if it holds it, and
  for its whole log only if its last entry is of term T. The base is the
  longest vouched prefix. `DeclareDead` (up to `MaxDead`) takes a broker out
  of the re-form's reads; its vote is unknown, so the group is marked and no
  copy vouches for its whole log.
- `FloorHolder`: the shortcut `Vouch` replaces. When some copy holds the
  floor's entry, the base is the copy with the highest last log id.
- `Reserve`: a copy counts or reports an entry only below an index it has
  reserved in a synced file, which a rotation keeps, and the re-form pads up
  to the highest reservation too.

`OffsetNeverReused` says that after the re-form every committed offset holds
the same record in the base or lies in the padded gap. Subscribers get only
committed records, so no offset one was given comes back with another record.
With the commit on written copies, two cut tails can take committed records
(`FelixRaftWrittenCommitsReformLoses.cfg` finds it). Without `Reserve`, a
record committed after the last floor report is above the floor, and its
offset is given out again (`FelixRaftWrittenCommitsReformTrailing.cfg`). With
it, `OffsetNeverReused` holds (`FelixRaftWrittenCommitsReform.cfg`). With the
commit on fsynced copies and the longest log as the base, a re-form loses
nothing and reuses nothing with the trailing floor alone
(`FelixRaftWrittenReformOnCommit.cfg`), and the vouch rule below does the
same with a broker dead.

Across two terms the longest log is no longer safe. In
`FelixRaftWrittenCommitsReformTermsFloorHolder.cfg`, a leads term 1 and writes
an entry past the floor; b and c commit a term-2 entry at that index on a
written majority, and a power cut takes it from both. The shortcut makes a's
log the base and serves its term-1 entry where the term-2 one was committed.
b's and c's kept votes say term 2, so `Vouch` cuts a to the floor, and
`FelixRaftWrittenCommitsReformTerms.cfg` passes with a broker
declared dead (1,752,892,374 distinct states, depth 45, 6 hours 53 minutes on a
32-core build box).
`FelixRaftWrittenCommitsReformTermsHappens.cfg` shows the rule does cut the
longest copy there. The price is a lone vote: a term that wrote nothing still
raises T and cuts every copy to its saved commit, so a committed entry a read
copy holds is lost (`FelixRaftWrittenCommitsReformTermsVoteOnly.cfg`). The
command reports such a group as "cut by a vote" and needs `--accept-loss`.

A broker declared dead is not read, so its vote is unknown. While the dead
are a minority of each voter set, every election took a vote from a broker
the re-form read, so T is still the highest elected term and the rule keeps
a whole log as before. a commits entry 1, fsynced and reserved on a and b,
and is declared dead before b learns the commit; c loses power. b's last
entry has term T, so the base keeps entry 1
(`FelixRaftWrittenReformVouchDead.cfg`). A minority of dead does not stop the
cut by a vote: with five voters and two dead, a term-2 candidacy that wins
nothing still cuts a committed entry (`FelixRaftWrittenReformVouchDeadFive.cfg`,
#3023), and the command asks for `--accept-loss`. Once the dead hold a majority of a
voter set, a term T' above every vote read could have elected a leader
among them, so no copy vouches past its saved commit and the floor. The voter
sets are the ones the re-form can know: the seed voters, both halves of every
membership entry in a copy read, committed or not, and each target the control
plane ordered, which it records before asking the leader. A target counts even
when no copy read holds its entry (`FelixRaftWrittenCommitsReformJointCutDead.cfg`),
and counting only the first voter set vouches for a stale tail
(`FelixRaftWrittenCommitsReformJointDeadOneSet.cfg`). With the gate forced
open, a copy holding a term-1 tail vouches for it over a term-2 commit the dead
made (`FelixRaftWrittenReformGateOpen.cfg`; `FelixRaftWrittenReformGate.cfg`
passes). The leader holding that tail has to crash: with no crash it stops
leading only by taking term 2, and its tail is never vouched for. The joint
outcomes are what those configurations are built to show, not checked
results: none has finished (see their rows above).

Voter sets hold copy ids, not brokers. A vote is unknown unless the re-form
read it from the copy that cast it: the copy in place, or the copy a power cut
rotated, whose kept vote is read. A replaced disk keeps nothing, so its old
copy's vote is unknown even when the broker's new copy has voted since. The
new copy's vote can be a deposed leader's term, saved when that leader appended
to it as a learner, and says nothing about the term the old copy voted in.
Counting it for the broker leaves the group unmarked and a1 vouches for a
term-1 tail over the term-2 commit that b1's vote made
(`FelixRaftWrittenReformReplacedByBroker.cfg`; `FelixRaftWrittenReformReplaced.cfg`
passes). Both run under `ReplacedScope`, an action constraint that turns off
written reports and the floor report and lets only a leader crash and only
after a commit lose the replaced disk, so the vote gate alone decides and
TLC reaches the deposed leader's append in seconds. A restore whose kept copy
is reclaimed later is the same state as a replaced disk: a new copy and no
kept copy of the old id.

With `Reserve`, every copy also reserves before its fsynced copy counts toward
the commit, the leader's own included. A fsynced copy is not enough on its
own: the vouch rule keeps a whole log only while the group is unmarked and
its last term is the highest vote read, so a lone vote in a later term, or
dead brokers holding a majority of a voter set, cut a fsynced copy back to its
saved commit. Without a reservation at that entry the re-form starts below it
and reuses a committed offset (`OffsetNeverReused`). The code reserves before every count, in every
fsync mode (`crates/server/felix-storage/src/raft_log/writer.rs`).

Abstracted: votes are durable and followers do not learn the commit index
(`FelixRaft.tla` has the adapter's rules and what followers apply), snapshots,
the subscriber and the lease. A message is one atomic step between two brokers.

### Copy identity under Raft

`FelixRaftIdentity.tla` models the member ids of one Raft group as
[Per-shard Raft](../per-shard-raft.md) decides them under "Group and member
identity". It is the model's side of #1331. A member id is a broker and a
copy id. The copy id lives in the shard directory with the storage id it was
minted under. The data root holds the storage id and, per shard, a mark of
the highest vote and last log id, with the copy id the marks belong to. The
vote is always synced. The root also holds a sequence number that rises with
every synced mark write. The run witness holds a copy of it while the machine
stays up, and the control plane holds the one it last heard in a heartbeat.

A member that opens its directory rotates its copy (new id, never voted, an
empty Raft log, a learner) when the directory is empty, was minted under
another storage id, holds another copy id than the marks name, or has a vote
or last log id below its mark. A rotated copy becomes a voter again only
through a membership change: a joint entry once it has the leader's log,
then the final entry, each committed under the quorum in force.

The faults all hit one broker: an emptied shard directory, a lost data root,
a crash under relaxed fsync that shortens the log, a whole root restored
from a backup, and one shard directory restored under a running root. The
log mark is exact (written with every append) or batched (a separate step,
so it trails).

A restored root gets a new storage id, which rotates every copy in it, when
its incarnation is behind the control plane's or its sequence is below the
witness's copy or the control plane's. A backup taken in the run the restore
interrupts carries the current incarnation, so only the sequence catches it
(`RootRestoreSameRun`). `WitnessLost` is a machine restart with the restore.
Two windows stay open, as the design says: a root that goes back before the
control plane heard its last sequence when no witness is left
(`RestoreUnheard`), and one that goes back inside a mark interval of the
batched log mark (`RestoreYoung`).

Checked: `OneLeaderPerTerm`, `LeaderCompleteness`, `StateMachineSafety` (no
two leaders commit different entries at one index), `AckedSurvive` (no set
of copies that could elect a leader lacks an acknowledged record, which
without faults is "on a majority of disks") and `AckedHeldByLeader` (no
leader is elected without a record acknowledged in an earlier term).

Each knob drops one rule: `WipeKeepsId`, `NoRestoreCheck` (no gate in front
of Raft), `NoStorageIdCheck` (a directory is not compared with the root's
storage id), `NoVoteMark`, `VoteBeforeMark` (the vote is answered before its
mark is synced), `MarkForgetsCopy` (the marks do not name a copy id) and
`NoRootSequence` (the gate does not compare the root sequence).
`ExactMark = FALSE` is the batched mark the broker ships, and with `RestoreYoung` it
is the residual Questions, 11 accepts.

Abstracted: the log is durable when appended (`FelixRaft.tla` has the fsync
rules), messages are atomic steps, a learner needs no membership entry,
nothing is purged, a rotated copy starts from an empty log, and the leader
that stops on a follower going backwards is left out. A follower cuts and
appends in one step, so its last log id never goes down; an implementation
that syncs the mark between the two must not read a cut as a restore. The
witness is written in the same step as the mark, and the sequence is counted
only for the broker that has a backup, from the backup on: an earlier write
is in the backup. After a restore the witness is taken to hold the restored
sequence, where a real one keeps the higher value. No configuration depends
on that: a second restore of the same root is behind on its incarnation.

### Placement, lost disks and the voter set

`FelixRaftPlacement.tla` models one Raft group whose voter set follows the
control plane's placement while disks are lost. `FelixRaftMembership.tla` has
one move with the write fence around it, and `FelixRaftIdentity.tla` has the
checks that decide when a copy lost what it promised. This module joins the
two and adds the control plane as the proposer, changes of more than one
seat, a smaller set, a removed copy's directory, and the offsets subscribers
were given. It follows [Per-shard Raft](../per-shard-raft.md) ("Membership",
"Group and member identity") for shard groups and
[the metadata Raft design](../metadata-raft-design.md) ("Rejoining after a
lost volume") for the metadata group, so one module covers both.

What is modelled:

- The control plane holds a target, the member ids the group should have. It
  moves a seat to another broker (a move, a drain or a replaced node), removes
  a seat with nowhere to put it, and swaps a seat's id when the broker reports
  a new copy.
- The leader makes Raft follow the target it read, which may be one the
  control plane has replaced (`StaleTargets`). It adds each missing member as
  a learner and calls `change_membership` with the whole target. openraft
  writes a joint entry and, once that is committed, the entry that leaves it.
  One membership entry at a time. A new voter need not be level.
- A wipe empties a broker's directory: log, term and vote. With
  `Rotate = TRUE` (shard groups) the copy takes a new copy id, nothing answers
  for the old one, and the leader swaps the new id in. With `Rotate = FALSE`
  (the metadata group) the member keeps its id and withholds its vote until
  it has applied up to an index a confirmed leader gave it.
- A copy the target dropped is deleted once a committed membership that is
  not joint has no vote for its id. The deletion is not counted as a fault.
  The control plane never places an id it told to delete; the broker comes
  back under its next copy id.
- A broker's subscribers are given a record when the broker learns it is
  committed. The offset is the entry's index.

An unclean restart is not a fault here: under fsync on commit it loses nothing
a peer counted. `FelixRaft.tla` has the adapter's fsync rules and
`FelixRaftIdentity.tla` the restores.

What is checked: `OneLeaderPerTerm`, `LogMatching`, `LeaderCompleteness`,
`StateMachineSafety`, `AckedSurvive` (no member that could still win an
election lacks an acknowledged record), `AckedOnDisk` (some directory holds
every acknowledged record), `OffsetNeverReused` (no offset delivered with two
records, no record at two offsets), `DeliveredAgree` (what subscribers
were given is what is committed at those offsets) and `CommitNeverLowers`
(no step lowers a broker's commit index, except one that empties its
directory).

A configuration's bounds decide which of these can fail in it. With no
client write (`Failover`, `MoveWipeFailover`, `SameIdFive`) the Acked,
Offset and Delivered checks have nothing to hold. With one term
`LeaderCompleteness` has no earlier term. `OffsetNeverReused` needs two
records, which only the two-write configurations have;
`FelixRaftPlacementNoWithholdOffsets.cfg` shows it can fail.

Each knob drops one rule:

- `SkipJoint`: `change_membership` writes the target in one entry.
- `AnswerAnyId`: a copy answers a request that names an older copy id of its
  broker.
- `DeleteOnTarget`: a copy is deleted as soon as the target drops it.
- `PlaceDoomed`: the control plane may place a member id it told to delete.
- `NoWithhold`: a wiped member that keeps its id votes at once.
- `WithheldCounts`: a leader counts the appends and heartbeats of a member
  whose vote is withheld.
- `FormMajority`: an empty member forms on a majority of empty answers and
  skips a member that does not answer.
- `FormAlias`: an answer is counted for the address it came from, not for
  the member that gave it, so a member that does not answer can be counted
  as empty.
- `WaiverExcusesAnswer`: a waived member's answer is skipped like its silence.
- `WaiverAnyNumber`: a waiver may name a majority of the slots.

- `Floor` (FALSE drops it): a member whose vote is withheld answers no
  leader until every other member has answered, with its term or with word
  that it is withheld too, and then only a leader at or above the highest
  term given. Silence blocks.

A wiped member forgot the terms it promised. Counted with no floor
(`FelixRaftPlacementSameIdCounted.cfg`), the leader of an older term
commits with it after a newer leader was elected on its vote, and an
acknowledged write is lost (#2365). Not counting a withheld member at all
is enough for three members and not for five:
`FelixRaftPlacementSameIdFive.cfg` elects two leaders in one term. A
candidate holds the wiped member's vote and is one vote short; the member
catches up with the older leader, votes again, and grants a second vote in
the term it forgot.

The floor closes both. Any term the member promised is held by the majority
it promised it with, bar members that lost their volumes too, so the
highest term the members that vote give is at least that term while one of
that majority keeps its volume. The member serves no leader below it, so
it never acks an older leader and never takes a vote back in a term it
forgot (`FelixRaftPlacementSameId.cfg`,
`FelixRaftPlacementSameIdFiveFloor.cfg`,
`FelixRaftPlacementSameIdFiveFloorTwoWipes.cfg`).
The cost is that a wiped member needs every other member up before it
counts for anything.

Deleting a removed copy needs two rules (#2385). A leader acts on no target
older than the membership in its log, and the control plane dooms a copy only
on the brokers' membership reports, which name the copies in appended entries
as well as committed ones. With both, a leader on a replaced target cannot
give a doomed copy its vote back (`FelixRaftPlacementStaleWipe.cfg` passes).
Without the version rule it can, and the deletion becomes a second lost disk
(`FelixRaftPlacementStaleWipeNoVersion.cfg`). A doom read from the committed
history misses a joint entry that is only appended
(`FelixRaftPlacementRewindSpareCommitted.cfg`). A restore of the control
plane's metadata raises the incarnation, and a delete is void below the
highest incarnation its broker has seen
(`FelixRaftPlacementStaleWipeRewind.cfg` and its two twins).

`FelixRaftPlacementTwoWipes.cfg` pins the bound instead of a rule: a group of
three survives one lost disk at a time.

A lost disk across a leader change is checked for a seat swap with a write
(`FelixRaftPlacementReseat.cfg`, two terms). For a move to another broker it
is checked in one term only: the two-term configurations
(`FelixRaftPlacementMoveWipeFailover.cfg`, `FelixRaftPlacementMoveWipeFull.cfg`)
have no result yet.

Abstracted: storage, snapshots, the write fence and the lease. A message is
one atomic step between two brokers. A leader counts the answers it has,
whatever became of the copies that gave them. None of the shard side is built
yet: no code calls `add_learner` or `change_membership` for a shard group, and
no code mints a copy id.

The metadata group's first start is a step of the model (`Forming`). A member
with an empty directory withholds its vote and asks every other member for
its standing. It votes only when every one of them answers and none holds a
log entry. Silence is read as empty only from a slot the operator waived, and
only while the members that answer empty, this one included, are a majority.
A member that lost its volume takes the same step. `FirstBoot` starts every
member this way, so no leader exists until members have formed.

- `FelixRaftPlacementForm.cfg` forms three empty members and then loses a
  volume. `FelixRaftPlacementReachForm.cfg` shows they do form and
  acknowledge a write.
- `FelixRaftPlacementFormWaiver.cfg` forms two members with the third waived
  and never started. A volume lost after that leaves no quorum, and the
  wiped member never votes again. Without the waiver they never form
  (`FelixRaftPlacementFormSilent.cfg`).
- A waiver names a minority. With two of three slots waived and absent the
  one member left does not form (`FelixRaftPlacementFormWaiverMany.cfg`);
  `FelixRaftPlacementFormWaiverAnyNumber.cfg` drops the limit and it does.
- A waiver excuses silence only. In `FelixRaftPlacementFormWaiverAnswers.cfg`
  the waived member is up, answers and may hold the log, and a wiped member
  does not form past its answer. `FelixRaftPlacementFormWaiverExcuses.cfg`
  skips the answer (`WaiverExcusesAnswer`) and loses an acknowledged write.
  A waived member that is up always answers in the model; `Absent` is the
  silent case.
- `FelixRaftPlacementFormAfterWipes.cfg` is a group that exists: a wiped
  member cannot form over a member that holds the log, whether or not that
  member answers. `FelixRaftPlacementFormMajority.cfg` drops the rule
  (`FormMajority`, a majority of empty answers is enough and silence is
  skipped, as before #2371) and loses an acknowledged write: the wiped member
  forms with a member that never received the write while its holder is
  silent. `FelixRaftPlacementFormAlias.cfg` keeps the rule and counts
  answers by address (`FormAlias`): two addresses in the peer list reach one
  member, the silent holder is counted as empty on that member's answer, and
  the same write is lost. The metadata group has each answer carry the id of
  the member that gave it and counts it for that member only.
- `FelixRaftPlacementFormTwoWipes.cfg` loses two volumes. Two wiped members
  do not form over a third that holds the log. When all three are empty
  they do form, which is right for the log and wrong for a vote one of them
  forgot, the gap below, so this one checks `AckedHeldSurvive` only.

`AckedHeldSurvive` is `AckedSurvive` for the records some directory still
holds. Losing every copy of a record is quorum loss, which no first-start
rule can see, so a violation of this one is the rule's own. The twins and
`FelixRaftPlacementFormTwoWipes.cfg` list it. Where `AckedSurvive` is
listed it implies it.

What the bounds leave unchecked. In `FelixRaftPlacementFormAfterWipes.cfg`
and `FelixRaftPlacementFormWaiverAnswers.cfg` the volume is lost after the
first commit, so a member that holds the log is always left and `Form` is
never taken: they check that the guard holds, and their twins show what it
stops. `FelixRaftPlacementFormSilent.cfg` and
`FelixRaftPlacementFormWaiverMany.cfg` are one state each for the same
reason. `OffsetNeverReused` is not listed on any of these: they have one
write.

The answers alone leave a gap (#2374). An answer shows the log and not the
vote. A volume lost after a vote and before the group's first log entry
meets only empty answers, so the member forms again and votes for a
candidate of an older term than the one it forgot. Both lead, and the older
leader acknowledges a write the newer one lacks
(`FelixRaftPlacementFormEarlyWipeUnrecorded.cfg`).

The forming record closes it (`FormRecord`). Forming is three steps, each on
disk before the next:

1. `Intend`: the answers allow forming and none names this member. It
   writes its own mark.
2. `Announce`: another member records that this one set out to form,
   whatever its own state.
3. `Form`: the member votes once every member that answers holds the
   record.

A wipe takes the mark and the records the directory held. A member that is
named and holds no mark lost a volume that may have voted, so it never
forms: it votes again only by catching up with a confirmed leader. A group
that starts formed starts with every mark and record written.

- `FelixRaftPlacementFormEarlyWipe.cfg` is a group that exists and one
  volume lost at any time. The wiped member is named, so `Intend` never
  fires and it comes back only through `VoteAgain`.
  `FelixRaftPlacementReachEarlyWipeVotesAgain.cfg` shows it does come back
  after a volume lost before the first commit, and
  `FelixRaftPlacementFormEarlyWipeUnrecorded.cfg` is the same configuration
  with `FormRecord = FALSE`.
- `FelixRaftPlacementFormEarlyWipeFirstBoot.cfg` starts from three empty
  members, so the volume can be lost between a member's mark, the others'
  records and its first vote. Here all three steps fire. A member that
  keeps its mark goes on although the others name it, and a member that
  starts late forms beside members that already vote.
- `FelixRaftPlacementFormEarlyWipeTwo.cfg` loses two volumes. The third
  member still names both. `FelixRaftPlacementFormEarlyWipeTwoUnrecorded.cfg`
  is its twin: without the record one of them votes twice in a term.

The first-start configurations above (`FirstBoot`) run with the record. The
ones for a group that exists (`FelixRaftPlacementFormAfterWipes.cfg`,
`FelixRaftPlacementFormTwoWipes.cfg`, the waiver pair and their twins) set
`FormRecord = FALSE` and `EarlyWipes = FALSE`: they check the answers rule by
itself, for a volume lost after the group has committed an entry. With the
record on, one lost volume could not tell the two rules apart, since the
other members name the wiped one whatever they answer.

What the record leaves unmodelled: a `forming` request is one atomic step,
so a record is written or not asked for. `Form` does not read the answers
again, which allows more than the metadata group does. A member's check
that the forming member names its own group is not modelled, and neither is
a restart, since every step here is on disk.

The configurations without `Forming` are given their membership at the start
and are unchanged by it; `FelixRaftPlacementReachAck.cfg` shows they elect a
leader and acknowledge a write. A waiver is a constant here, so its being
used up after one forming is not modelled. The metadata group rejoining
under a new id after a lost volume, the fix #2365 settled on, is not
modelled either. The shard rule it copies is here (`Rotate = TRUE`,
`FelixRaftPlacementReseat.cfg`, `AnswerAnyId`).

### Re-forming a group

`FelixRaftReform.tla` models the operator's command in
[per-shard-raft.md](../per-shard-raft.md#re-forming-a-group) that gives a
Raft group a new epoch when it can no longer elect. Nothing in the model
starts a re-form by itself, except the restored copy of a shard with one
replica. Nothing is built yet.

A group id carries an epoch that starts at 0. A member id is a broker and a
copy id. A copy whose disk was lost or restored rotates: it has a new id
that never voted and keeps what is left of its log as plain records. A
power cut does not rotate a copy, since Raft logs are fsync on commit.
Membership changes inside an epoch go through a joint configuration, and a
member uses the newest membership in its log, committed or not.

The control plane keeps three things the command needs:

- `placed`: the brokers placed for the group. `Place` records a learner
  before the leader sends it anything, and a move's target before the
  leader proposes the change. `Unplace` removes a broker when the leader
  reports the membership that drops it as committed.
- `everPlaced`: every broker placed in the current epoch. `Unplace` does
  not remove one; a re-form starts the record again with the brokers the
  new epoch places.
- `dead`: the brokers an operator declared dead (`DeclareDead`).
- `cpFloor`: the log id of the highest commit a leader reported
  (`ReportFloor`): the index, and the epoch and term of the entry there.
- `commit`: each copy's commit index, saved on its disk. It can lag the
  leader's and never passes it, and a copy takes no append that would cut
  its log below it (`KeepsCommit`).

The command is three actions:

- `Plan` is the dry run. It lists every broker placed in the epoch
  (`everPlaced`) and reads every copy that is not declared dead: copy id,
  last log id, newest membership. It refuses while a listed copy leads,
  and while a copy names a voter that is neither listed nor declared dead.
  It changes nothing.
- `Stop` sets a durable marker. A stopped member does not vote, acknowledge
  or append in that epoch again, also after a restart. `Stop` asks for a
  leader again first, and does nothing while a listed copy leads.
- `Reform` needs every listed copy that is not dead to be stopped, reads
  each again, and goes on only if what they hold is what the plan showed.
  It takes the highest last log id as the base, pads it with a gap up to
  the floor, makes the copies that hold the base the voters, and writes the
  next epoch by compare-and-swap. When no copy read holds the floor's
  entry, the base is instead the longest log a copy can vouch for (#2402,
  below).

`StartEpoch` starts a member at the new epoch from the base. `SelfStart` is
the one re-form without the command: a rotated copy that kept records, in a
group where no other broker was placed in the epoch, writes the next epoch
with itself as the single voter.

No answer of a copy is kept between runs. A plan holds what the operator
saw, and `Reform` uses only what it reads after the stop.

The first voters of an epoch are in the control plane: it writes the first
member set of a new group, the voters of a switch, and the voters of every
re-form. The model keeps them in `boot`, per broker, and a lost disk does
not clear it. `BootIsRecorded` checks that `boot` never holds anything
else.

The list is every broker placed in the epoch, not only the ones placed now,
because a removed voter's disk can hold the newest records. In
`FelixRaftReformStaleMembershipTwoMoves.cfg` the group moves from {a, b, c}
to {a, b, d} and back, both changes committed by a and b, and a and b then
lose their disks. The copies of {a, b, c} are empty and name {a, b, c}, so
nothing on them points at d. Only the record of every placed broker does.
With the list cut to the brokers placed now
(`FelixRaftReformStaleMembershipTwoMovesForgotten.cfg`) the group re-forms
from an empty base while d's disk holds the acknowledged entries. With
that record, the check that a copy names no voter off the list never
refuses; it stays as a guard against a wrong record.

What is checked:

- `AckedSurvive`: an acknowledged record is in the base of every later
  epoch and in the log of every later leader. The losses allowed are the
  ones the design documents. At the re-form no disk held the record any
  more, not counting the disks of brokers declared dead. Or no copy held
  the floor's entry, and no disk that held the record could vouch for it
  (#2402, below). A record gets there through lost disks or through a
  declaration. The model records those in `lost`; it does not check how
  each disk lost the record. A disk that is not on the command's list
  still counts as holding its records.
- `LossNeedsLostDisk`: the first of those losses is the only one. An
  action property, checked where every record has one term.
- `StateMachineSafety`: no index is committed with two different entries,
  in one epoch or across epochs, except over a documented loss.
- `OneLeaderPerTerm`, per epoch.
- `NothingLost`, in the configs where no disk is lost and no broker is
  declared dead.
- `OffsetNeverReused`: no offset is given out with two records. A gap is
  not a record.
- `ElectedGroupLeftAlone`: no copy is stopped while a broker leads its
  epoch. A group that elected after the plan is skipped by `apply`. It is
  an action property, checked in `FelixRaftReform.cfg`,
  `FelixRaftReformTwoRestarts.cfg` and `FelixRaftReformRejoin.cfg`, and
  `FelixRaftReformStopNotRechecked.cfg` violates it. It says nothing about
  a copy stopped just before the election: that one stays stopped (#2403).
- `BootIsRecorded` and `PlacedIsRecorded`, in every passing config. The
  second says every broker a membership of the current epoch names is in
  `everPlaced`.
- `AbandonedNotLed`: no live broker leads an epoch behind an abandoned
  one. With `OneLeaderPerTerm` the shard has one leader.

Each knob drops one rule: `ReformBeforeAll` (go on while a placed broker
has not answered and is not declared dead), `EpochNotChecked` (a member
takes an append from another epoch), `ReportNotPromise` (read the copies
without stopping them), `PlanNotRechecked` (apply uses the list and the
answers of the plan and does not read the copies again), `RotatedNotWaited`
(a rotated copy is not read), `RetireNotDurable` (a restart forgets the
stop marker), `BaseNotHighest`, `NoCas`, `SoloPlacedTwo` (a restored copy
starts by itself although a second broker is placed), `StopNotRechecked`
(apply stops a copy although a listed copy leads), `PlacedForgotten`
(the list is the brokers placed now, so a removed broker drops off),
`FloorIndexOnly` (the floor is an offset with no term, and a base is never
cut) and `CutToCommit` (a cut base ends at the saved commit index on every
copy).
`RetireNotDurable` needs two restarts to fail, and
`FelixRaftReformTwoRestarts.cfg` is the same bounds with the rule on.

`Abandon` models giving the log up when every copy is gone (#3096). `"on"`
is the design: in one store step, refused unless every broker placed in
the epoch or named by the assignment is declared dead, the next epoch is
written with an empty base on one live broker, its first index above the
floor, every earlier epoch's first index and the restore time line. The
time line is an oracle here, above every offset given out so far;
`FelixRaftRestoreRepeated.tla` checks the clock argument behind it. So this
model cannot see a planning clock that runs behind the one that started an
earlier epoch on the line; the code refuses the abandon then, and a test
covers it
(`offsets_after_an_abandon_are_above_every_offset_the_lost_group_gave_out`). Only
the blank copy the epoch names as its voter starts it, at the first index,
and a copy behind an abandoned epoch never opens. A broker declared dead
can come back with its copy (`Comeback`). Each other value drops one rule:
`"outside"` (the dead check in a step before the write), `"floor"` (the
first index just above the floor), `"zero"` (the blank copy forms from
index 0) and `"open"` (a copy behind the abandoned epoch opens).

Three constants are not knobs. `WaitPlaced` is the command's list: on, the
placed brokers; off, the voters in the control plane's own record of the
membership, which leaves out a move's target and a learner. `Floor` is
`"trailing"` for the design (the last offset a leader reported), `"exact"`
for a floor that hears of every commit at once, and `"none"`. `DeadRuns`
says whether a broker declared dead is gone or still running.

How the three issues the first version of this module found are covered:

- **#2304**, a re-form resting on a stored report. No report is stored.
  `FelixRaftReformRejoin.cfg` stops a copy in an apply that does not
  finish, lets the group elect without it and brings the copy back as a
  learner under a new id; the plan no longer matches and a second plan
  re-forms the group. `FelixRaftReformReportNotPromise.cfg` (read without
  stopping), `FelixRaftReformPlanNotRechecked.cfg` (use the plan's answers)
  and `FelixRaftReformRotatedNotWaited.cfg` (skip a rotated copy) each lose
  a record a disk holds.
- **#2305**, offsets reused after a majority of disks is lost.
  `FelixRaftReformTwoRotatedOffsets.cfg` passes `OffsetNeverReused` with an
  exact floor and one term. With no floor it fails. With the trailing floor
  of the design it fails too: that is the residual of Questions, 13, pinned
  as `FelixRaftReformTwoRotatedOffsetsTrailing.cfg`.
- **#2311**, a broker no copy names.
  `FelixRaftReformStaleMembershipTwoRotated.cfg` has {a, b, c} changing to
  {a, b, d} and the disks of a and b lost
  (`FelixRaftReformStaleMembershipFull.cfg` adds a client write and has not
  finished a run yet), and
  `FelixRaftReformLearnerWaited.cfg` has a learner d holding a record a and
  b lose. The two that have run pass because d is placed and so listed. The same
  configurations with d left off the list (`...Unlisted`,
  `FelixRaftReformLearnerHolds.cfg`) lose what d's disk holds.

A declaration can be wrong. In `FelixRaftReformWrongDeclaration.cfg` two
brokers declared dead are alive, elect and acknowledge a write in the old
epoch after the third has re-formed without them. The design names this as
what the command cannot protect against, and the violation is pinned.

Abstracted: storage is a log and a rotation that cuts it to any prefix
(`FelixRaft.tla` has the fsync rules). An election is one step over a
quorum. An append is one step between two brokers, plus a bounded set of
in-flight appends that can arrive late and more than once. A leader can
step down at any step and does not propose its own removal. It sends to the
brokers its newest membership names and to placed brokers that no
membership of the epoch has named, which are learners. The command reads
whole logs where the design reads last log ids. The read after the stop and
the write of the base are one step, so a disk lost between the two is not
modelled. A removed copy keeps its directory; its deletion is in
`FelixRaftPlacement.tla`. An unreachable broker is one that takes no step,
and the command's scope is one group.

Not modelled: moving a shard with one replica, since the model's leader
never removes itself, so the second placed broker of the self-start rule is
always a learner here. And a group whose placed brokers are all declared
dead: the command needs one copy to read.

**#2402**, an uncommitted entry inside the base. An offset-only floor does
not cover it: in `FelixRaftReformTwoRotatedOffsetsTermsIndexOnly.cfg` the
new epoch serves a stale entry at an offset that was acknowledged with
another record. The floor is a log id, and every copy saves its commit
index. When no copy read holds the floor's entry, the base is the longest
log a copy can vouch for (`Vouches`): the whole log of a copy whose last
log id has the floor's epoch and term and is not above its index, and the
log up to the saved commit index of any other copy.
`FelixRaftReformTwoRotatedOffsetsTerms.cfg` passes with that rule.

`AckedSurvive` allows a loss in two cases only: no disk holds the record,
not counting brokers declared dead, or no copy held the floor's entry and
no disk that holds the record can vouch for it. `LossNeedsLostDisk` is the
first case alone. It holds with one term
(`FelixRaftReformTwoRotatedOffsets.cfg`), where a copy can vouch for all
it holds, and the second case needs two terms
(`FelixRaftReformTwoRotatedOffsetsTermsUnvouched.cfg`). A saved commit
index that only lags is not a loss: the twin that cuts every log to it
(`CutToCommit`, `FelixRaftReformTwoRotatedOffsetsCutToCommit.cfg`) violates
`AckedSurvive` with no disk lost beyond the tails.

**Kept copies and two re-forms (#2250).** With `Kept`, a rotation renames
the whole copy into a kept copy that holds its log, saved commit, vote, why
it rotated and its reservation. `PowerCutRotate` loses only the unsynced tail
and keeps the vote; `Rotate` is a restore that can bring back any prefix and
an older vote, or a disk replaced with nothing kept (#2671), which is also
where a restore ends once its kept copy is reclaimed; `Return` brings a broker declared dead back under a new copy
id with its kept copies. The re-form reads every current and kept copy of the
brokers it reads. A copy vouches for its saved commit, for the floor's index
if it holds the floor's entry, and for its whole log when its last key equals
T, the highest (epoch, term) of any vote, entry or floor read. An epoch-e copy
is capped at the lowest base end of the epochs after e. The group is marked,
and the whole-log clause dropped, when a broker placed in the epoch being
closed is dead, has no vote of that epoch read, or has a restore kept copy of
that epoch. The new epoch starts above the base, the floor and every
reservation read, and with `Reserve = "every"` each copy reserves before its
write counts.

`FelixRaftReformTwoReformsTrailingFloor.cfg` passes: an epoch-1 record
committed by a leader and one follower and then cut from both, with the
floor trailing and the leader dead, keeps its offset through the follower's
kept reservation. With only the leader reserving
(`...TrailingFloorLeaderReserve.cfg`), or with both copies dead
(`...TrailingFloorAllDead.cfg`, the documented residual), the offset is given
out again. `FelixRaftReformTwoReformsEpochMark.cfg` shows that an epoch-0
restore and a dead epoch-0 voter leave the second re-form unmarked, and
`FelixRaftReformTwoReformsOldRestore.cfg` checks that case reuses nothing.

Two epochs blow up the state space, so these six configurations run under
the action constraint `TwoReformsShape`. Epoch 0 is closed empty, each later
epoch takes one write, and the plan that closes epoch 1 waits until epoch 1
committed a record. Each constraint only removes interleavings the scenario
does not need, and each witness still fires under it. With epoch 0 empty,
the cap and T decide nothing there, so two more pairs check them.

`FelixRaftReformKeptOldKept.cfg` writes in epoch 0 (`OldKeptShape`): its
leader is declared dead holding two committed records, the first re-form
keeps what the others vouch for, and the dead broker returns in epoch 1
(`Return`) with its epoch-0 copy. The second re-form reads that copy although
epoch 1 never placed it, and the epoch-1 cap lowers what it vouches for
(`...OldKeptCap.cfg`). `FelixRaftReformKeptTerms.cfg` has one re-form over
two terms of epoch 0 and a power cut; there T keeps one copy whole and cuts
another to its safe part (`...KeptTermsT.cfg`).

With `Kept`, `AckedSurvive` allows any record the vouch rule left out.
`KeptLossBound` bounds that: when the group is unmarked and no vote read is
above every entry read, no acknowledged record a read copy holds below its
epoch's cap is lost. Not modelled with `Kept`: `Reserve = "none"`, and the
"cut by a vote" flag the command reports
(`FelixRaftWrittenCommitsReformTermsVoteOnly.cfg` shows the case).

**Restoring from a backup point (#2250).** `FelixRaftRestore.tla` is a
small model of its own for the offsets of a whole-cluster restore. The
backup reads the point from the leader, copies that leader's directory and
exports the control plane, and the restore starts epoch 1 at
max(point + 1 + `Gap`, restored floor + 1, copied reservation + 1). Every
copy raises its reservation before a write passes it. With no gap (`Gap` = 0),
`FelixRaftRestore.cfg` shows that nothing acknowledged by the time of the
copy is given out again (`BackedUpNeverReused`), and only through the
reservation (`...NoReserve.cfg`). The point is the leader's acknowledged
mark, which can be past its commit and past the commit the copy saved, so
the cut saves the point as committed; a cut that only lowers the saved
commit leaves the plan refusing a copy that holds the point
(`...NoRaise.cfg`, `RestoresAtPoint`). Two cases reuse an offset with no gap: one
acknowledged after both the copy and the export (`...AfterBackup.cfg`, the
documented residual), and one acknowledged before the copy when the copied
copy stopped leading after the point and fell behind its own reservation
(`...LeaderMoved.cfg`). The restore offset gap closes both while no more
than `Gap` offsets were acknowledged after the point (`...Gap.cfg`), and a
gap one short reuses an offset (`...GapShort.cfg`). The code takes the gap
from `felixctl raft reform plan --offset-gap` (2^40 by default) and starts
the epoch at max(point's offset + gap, ...) for a single restore, where the
point's offset is the model's point + 1. The code's first index is at least
that: it takes the larger of the point's end and the time line
`2^14 * plan time + gap`, and refuses a plan whose line is below the end.
The cut's check that the entry below the point is of the point's term is not
modelled: no copy in the model holds another leader's entries.

`FelixRaftRestoreRepeated.tla` checks restores that repeat. Every restored
epoch starts on one line, `TimeRate * clock + Gap`, read from the planning
node's own clock. The line rises faster than the shard gives out offsets
(`Rate` = `TimeRate` / 2), so an epoch started on it stays below it, and a
later restore from any backup starts past everything earlier epochs gave
out, provided the restores are far enough apart for the clock skew: with a
skew of a tick, five ticks pass (`...Repeated.cfg`) and three do not
(`...RepeatedMinApart.cfg`).
Without the line a second restore from one backup reuses offsets
(`...RepeatedNoTime.cfg`); adding the gap past the backup's end as well
lifts a restored epoch above the line, where an older backup restored after
it lands below its offsets (`...RepeatedEndGap.cfg`).

One finding of this module is open. The design does not say how a copy
comes back after an apply stopped it, did not finish, and the group
elected without it (#2403). The model has it rotate and rejoin as a
learner (`RetiredRejoins`), which is an assumption, not a rule of the
page.

### The switch from the hybrid to Raft

`FelixRaftSwitch.tla` models moving one shard from the hybrid protocol to
Raft, as [Per-shard Raft](../per-shard-raft.md) decides it ("Moving a shard
from the hybrid to Raft"). These configs gate the pull request that builds
the switch.

The model starts at the seal. The hybrid leader has fenced at generation `G`
and appended the generation-start record at offset `N`, which names a start
offset `S` and a digest of the records from `S` up to `N`. What each copy
holds at that moment is an arbitrary choice, bounded only by what the
hybrid's fence guarantees: the leader holds every acknowledged record below
`N`, and a record counts as acknowledged only when a majority holds it. A
copy can lag, start later than the leader, carry generation 0 where another
copy carries the real generation, or hold a different record below `N`. A
copy can be wiped at any step, which gives it a new copy id.

A follower acknowledges `N` with its copy id if its log starts at or below
`S` and its digest matches. The leader reports the base and the copy ids of
a majority, once. The control plane writes engine, base and voters in one
compare-and-swap on generation and set version. A broker whose copy id is
recorded cuts above `N` and starts with its log purged up to `(G, N)`;
every other copy starts empty as a learner, takes the records below the
base from the Raft leader and is promoted by a membership entry. Raft then
elects at a term above `G`, commits a blank entry and takes writes. The
hybrid can also open again after a seal, acknowledge a write and be sealed
at a later generation while the first seal's report is still in flight.

Checked:

- `LeaderCompleteness`: a Raft leader holds, by content, every record the
  hybrid acknowledged from `S` up and every record Raft acknowledged before
  its term. This is "no acknowledged record is lost".
- `StateMachineSafety`: above the base, every member applies the one
  committed record at each offset.
- `BaseAgree`: members that vote hold the same content from `S` to `N`,
  whatever generation each copy wrote on a record.
- `OneBase`: no two brokers start from different bases.
- `OneLeaderPerTerm`.

Each knob drops one rule:

- `AllVoters`: the control plane records the whole set as voters.
- `TermsBelowBase`: no copy purges or starts empty, so the hybrid log is
  the Raft log and the generation field is the term.
- `NoDigest`: a follower acknowledges `N` on reaching it.
- `DigestCoversGeneration`: the digest includes the generation field.
- `DigestFromOwnStart`: a follower's digest runs from its own first record,
  so a copy that starts above `S` acknowledges.
- `CpBlindWrite`: the control plane writes any report it receives.
- `StaleIdVotes`: a broker is matched to the recorded voters by name, so a
  wiped copy still votes.

Two results are about availability, not safety. With a and c's old copy id
recorded and c wiped, a alone cannot elect: the safety checks pass and no
leader is ever elected (`FelixRaftSwitchWiped.cfg`). With the digest
covering the generation field and generation 0 on the followers, no
majority ever acknowledges and the switch never happens
(`FelixRaftSwitchDigestCoversGeneration.cfg`).

The implementation design on issue #2242 adds the parts the seal has
around it, each off unless a configuration turns it on:

- A write already inside the seal leader's fence (`InFlight`). It takes an
  offset. As a `Leader` write it is acknowledged on the leader's disk while
  the leader's lease holds; as a `Quorum` write, once a majority holds it,
  and then it is checked like any record the hybrid acknowledged, across a
  failover too. The record at `N` waits until it is answered or failed.
- Followers catching up to the seal leader during the seal (`Catchup`),
  only from the copy id that sealed. The seal leader serves any copy that
  does not hold the sealed promise, so a wiped follower can catch up under
  its new id and acknowledge `N`.
- The control plane giving up a seal once the leader's lease has lapsed and
  sealing again with the copy furthest ahead in a majority (`Failover`). A
  wiped copy does not answer that fence: as
  [the replication design](../replication-design.md) has it for elections,
  it does not vote until it is re-admitted, which the model never does.
- Copies whose log has a tail not yet on disk (`MayUnsynced`). Any of them
  can lose it to a power cut, once per behaviour, at any point before it
  starts its member: a follower after it acknowledged `N`, the seal leader
  after it reported, a broker between its vote floor and its member mark.
  A seal leader that loses power before it reports gives up the seal.
  Every copy's log is fsync on commit, which the switch requires of every
  copy and not only the seal leader, so a record the hybrid acknowledged
  is on disk on a majority. A follower writes `N` to
  disk before it acknowledges and the leader before it reports.
- A Raft member moving its purged id past `N` (`MaxPurges`).
- A follower's record differing only in its producer tag (`MayDifferTag`),
  and its dead-letter and group-cursor logs lacking the leader's state
  (`MayDifferSide`).

A broker starts its member in two steps: the log, purged id and vote
floor, then the member mark. A crash between them is reachable, and the
broker starts again from the record.

New checks: `HybridAcksInBase` (once the switch is recorded, every record
the hybrid acknowledged from `S` up is in the base, by content, below `N`;
on a log that is fsync on commit, which is why the switch refuses a
`Periodic` one),
`LeaderAcksInBase` (the same for `Leader` acknowledgements while the writer
is the seal leader), `NoAckAfterHandOver` (no `Leader` acknowledgement once
the shard is handed to another copy or to Raft), `SideComplete` (a Raft
leader holds the side logs) and `NeverReadsBelowBase` (a recorded voter's
purged id is at `N`). One knob each drops a rule: `ClaimBeforeQuiesce`,
`LeaseAtEntryOnly`, `ResealSameGen`, `CopyFromAny`, `DigestSkipsTag`,
`DigestSkipsSide`, `NoVoteFloor`, `ReopenOnTimeout`, `BallotIgnoresSeal`,
`MarkFirst`, `WipedVotesInFailover` (a wiped copy answers the failover's
fence), `SealPeriodic` (the switch seals a `Periodic` log) and
`AckBeforeSync` (a follower acknowledges `N`, and the leader reports,
before `N` is on disk).

Liveness: `SwitchEnds` says a switch that started ends with the control
plane's record and every copy's member started. Its configuration sets
`MaxCampaigns = 0`, so it says nothing about electing a leader or a learner
catching up afterwards. `FairSpec` gives weak
fairness to the switch's own steps and none to faults, failovers, reseals
or Raft, so it holds only where no fault needs the control plane to step
in (`FelixRaftSwitchEnds.cfg`). These states stay stuck in the model, and
the design accepts them:

- The seal leader wiped, or out of power, before it reports. Its seal
  never reports, and only a control-plane failover, which waits for its
  lease, ends it (`FelixRaftSwitchEndsLeaderWiped.cfg`).
- No majority passes the digest. The design reopens the shard and
  rebuilds the copies that differ; the model's reseal does not rebuild.
- A recorded voter wiped after it acknowledged. The switch ends, but the
  voters left may be too few to elect, and the group needs a re-form
  (`FelixRaftSwitchWiped.cfg`).

The model's term is the record's generation field. In the code a Raft term
is one less (`raft_log`: generation = term + 1), so the base's term `G`
here is Raft term `G - 1`, and the vote `Start` stores at `G` is the vote
at Raft term `G - 1` that the design calls the floor.

Abstracted: storage is durable at once except the tails under
`MayUnsynced` (`FelixRaft.tla` covers the adapter's fsync rules once a
member has started), a message is one atomic step, and the hybrid before
the seal is the initial choice. Reseal and failover are one step each, and
one write is in flight at the seal. The digest and `S` travel beside the
seal; in the code they are in the record at `N`, which a follower appends
as the leader sent it. That record names its generation in its payload,
so two seals' records never compare equal. The control plane's copy of
the records below `N` is history for the checks; it records only the base
and the voters. The promotion adds one
voter in one step. Records below `S` are outside the check, as the design
says. Cache shards, whose digest is over live state, are not modelled yet.

### Raft leader leases

`FelixRaftLease.tla` models the lease that lets a `Leader` write be
acknowledged on local durability, as
[Per-shard Raft](../per-shard-raft.md) ("Acknowledging a write") and
[The replication engine interface](../replication-engine.md) (Decisions, 5)
decide it. Its configurations gate the pull request that turns on
`admit_local_ack`. The ones with a restart pass on the node layer's vote
hold (issue #2307, under "A restarted follower" below), which is built in
`raft/group.rs`.
`FelixRaftLeaseRestartRenews` passes on openraft's own start-time renewal,
but only if `save_vote` is durable when it returns, which waits on the Raft
engine. The
module has two parts. The first is the lease the first
version ships. The second, the `FelixRaftLeaseStore*` configurations, is the
later design in "Leases that survive sleep" and is not in the first version.

**Time.** Real time moves in ticks, and in each tick every broker's clock
gains between `RateLo` and `RateHi` units, chosen again at every tick. A
broker only reads its own clock, and only to ask how long ago something
happened, so the model keeps ages instead of clock readings. An age stops
growing once no rule can tell it from an older one, which makes the state
space finite without a bound on how long a run lasts. The inequality the
config must satisfy is `Lease * DriftHi <= ElectionTimeout * DriftLo`, the
integer form of `lease * (1 + p) / (1 - p) <= election_timeout_max`.

**The first version's lease.** A leader's append or heartbeat is in flight,
so its hand-off time, the time the follower takes it and the time the answer
returns all differ. The lease runs from the hand-off time of the newest
message a majority has answered (openraft's `last_quorum_acked`). A follower
refuses to vote, and does not campaign, for the election timeout from the
moment it took a leader's message. A write is acknowledged on local
durability while the lease check passes and otherwise waits for commit, which
is what a `Quorum` write does. A failed check changes no role. The subscriber
is given records up to the commit index.

Checked:

- `LeaseExclusive`: no two brokers pass the lease check at one real time.
- `NoAckBehindNewLeader`: no write is acknowledged on local durability once a
  leader of a higher term has been elected. This is the edge of the `Leader`
  loss window: a record acknowledged this way can be lost only to a failover
  that starts after the acknowledgement.
- `AckedSurvive`, `StateMachineSafety`, `OneLeaderPerTerm`, and
  `DeliveredNeverReplaced`: what the subscriber was given is what every
  broker has committed at that offset.

**A restarted follower.** The refusal window lives in memory, so a follower
that answers the leader, restarts and is then asked for a vote must not
grant it at once. `FelixRaftLeaseRestartForgets.cfg` has no rule for that
and shows the old leader still passing the lease check when the new one is
elected. The design's hold on a restarted member stops it campaigning, and
the model keeps that hold; it does not stop it granting a vote to a broker
that never heard the leader. Two rules close it, and the model checks each
alone:

- The node layer's vote hold (issue #2307): a restarted broker refuses votes
  for one election timeout from its start, whatever vote it stored (the
  code holds no member without a stored vote: it has taken no message).
  `FelixRaftLeaseRestart.cfg` passes on it. The design page has this rule,
  and `Group::open` and `Group::dispatch` (`raft/group.rs`) build it, with
  the two exemptions the `FelixRaftLeaseRestore*` configurations check.
- openraft 0.10.0's renewal (`RestartRenewsFollowed`): `Engine::startup`
  renews the window for one election timeout when the stored vote is
  committed and another broker's, which is the vote of a follower that took
  its leader's message. A vote given to a candidate gets no window, and a
  restarted leader's own vote is demoted with none.
  `FelixRaftLeaseRestartRenews.cfg` passes on it with the hold off. The
  members a lease counts on have all taken the leader's message, so the
  votes that get no window are not ones the lease rests on.

The design does not rest the lease on the engine's rule staying as it is,
so the lease rests on the vote hold. Both configurations have one restart
and two terms.

**Skipping the hold for a restored leader (pull request 2445).** openraft
restores a member as leader at a start when its stored vote is its own and
committed. Such a member took no later leader's message, so it promised
nobody, and its own lease died with the process; the other voters still keep
their window and their hold. The model keeps that on disk as `led`, and
`RestoreOwnLead` lets such a member, or the only voter, campaign without the
hold. It lifts only the campaign side: `MayGrant` still has the member grant
no candidate but the one its stored vote names, itself, until the hold ends. It campaigns in a new term rather than resuming the old one, which
gives it no more than a resumed leader has: no lease until a quorum answers
again. `FelixRaftLeaseRestoreLed.cfg` passes with three terms and one write,
and `FelixRaftLeaseReachRestoreLed.cfg` shows the skip is taken.
`FelixRaftLeaseRestoreAnyVote.cfg`, which also skips the hold for a vote
given to another broker, fails: a follower that answered the leader just
before it stopped campaigns at once and a voter outside the leader's window
elects it.

When a quorum restarts together, the holds also drop the old leader's vote
request. A broker inside its hold may grant the candidate its stored vote
names (`HoldExemptsNamed`), since it promised that candidate already.
The model grants inside the hold only to the leader of the broker's current
term. openraft stores one vote, and a vote grant, adopting a higher vote and
an accepted append all replace it, so the stored vote can name a candidate
that never led: a leads term 1, adopts c's term 2 vote and restarts, and its
disk holds term 2, c, uncommitted. The rule Felix implements needs only the
vote record: grant c inside the hold if and only if the stored vote is
committed and names c, and an uncommitted vote exempts nobody. A committed
vote names the leader of its term, so this grants a strict subset of what
the model allows. The leader's own fast path (`RestoreOwnLead`) is the same
test with the broker itself: committed and naming itself.
`FelixRaftLeaseRestoreNamed.cfg` passes with the rule and two restarts (two
of the three brokers), and `FelixRaftLeaseReachHeldGrant.cfg` shows a held
broker granting. `FelixRaftLeaseRestoreNamedAny.cfg`, where the stored vote
exempts any candidate, fails. `FelixRaftLeaseRestoreTransferStepDown.cfg`
runs both fast paths with a transfer and a step-down: a leader that
authorized a transfer restarts with its own vote and campaigns at once.
`FelixRaftLeaseRestoreStepDown.cfg` does the same after a void, which needs
a third term. Both leave out the write, and the first has two terms and the
second no transfer, so each finishes in CI; with all of them the run does
not finish in 40 minutes. Both also set `ElectionTimeoutMin` to 1 where
`FelixRaftLeaseTransferStepDown.cfg` has 0, so a stepped-down leader that
campaigns in the same tick is not checked with a restart.

The write and the third term come back under a view (issue #2471).
`RestartView` has TLC explore once the states that differ only in what no
step and no check can tell apart: an answer no newer than the one its leader
already has, a `TransferLeader` to a broker already past its term, Felix's
view of the role while the check refuses on it, and `votedFor`, which these
configurations do not read. The comment on `RestartView` argues each one.
`FelixRaftLeaseRestoreTransferStepDownWrite.cfg` adds one write to the
two-term configuration, and `FelixRaftLeaseRestoreTransferThreeTerms.cfg`
the third term to the one with no write.
`FelixRaftLeaseReachRestoreTransferThreeTerms.cfg` shows the case the third
term is for: a transfer's target wins the second term, then leads a third
with a voter in that term inside its hold, after a held voter granted a
vote, while the leader that started the transfer still leads the first.
With both, in
`FelixRaftLeaseRestoreTransferThreeTermsWrite.cfg`, the run passes but takes
90 minutes on 16 cores. By its siblings' times that is about ten hours on a
CI runner, past the nightly job's six, so it is not registered and nothing
in CI checks it.
`FelixRaftLeaseReachRestoreTransferThreeTermsWrite.cfg`, which stops at the
first state where that third-term leader has acknowledged the write under
its lease, is registered.

**Late metrics, step-down, transfer and suspend (issue #2249).** Felix reads
openraft's role and `last_quorum_acked` from metrics, which arrive late
(`RoleSync` off: Felix's check runs on the term and age it last saw). Three
paths let a new leader be elected inside the old leader's window, and the
model has each:

- A leader that takes an append's answer naming a candidate's higher term
  adopts that vote (openraft's `HigherVote`). The vote is not committed, so
  it grants the candidate at once, and the candidate wins with it while
  Felix's late view of the old leader still passes, however short the lease.
  Later the stepped-down leader campaigns and the followers it led grant it
  inside their window (`from_leased_leader`). The node layer voids every
  term below the one a message names, before openraft sees it.
  `FelixRaftLeaseStepDown.cfg` passes with the void and `ElectionTimeoutMin`
  below the lease, so the void does the work, not a bound on the lease.
  `FelixRaftLeaseStepDownWrite.cfg` adds a write with `ElectionTimeoutMin`
  = 0.
  `FelixRaftLeaseStepDownNoVoid.cfg` fails without it.
  `FelixRaftLeaseStepDownSync.cfg` passes without it when the role is read
  when the check runs, the fix the design does not take.
- A transfer stops heartbeats, clears the window of every follower that takes
  the `TransferLeader` message, and lets the target's voters grant inside it.
  A transfer that does not finish makes the leader step down and campaign.
  Felix voids its lease for the term before the message leaves.
  openraft's target campaigns the moment it takes the message, so a target
  that first follows a leader of a later term no longer campaigns for the
  old transfer.
  `FelixRaftLeaseTransfer.cfg` passes; with no void
  (`FelixRaftLeaseTransferKeepsLease.cfg`) or the void as a later step
  (`FelixRaftLeaseTransferVoidLate.cfg`) it fails.
- A suspended broker's monotonic clock stops while real time runs. Felix
  compares it with boottime inside the check and refuses local acks for one
  more lease. `FelixRaftLeaseSuspend.cfg` passes, and
  `FelixRaftLeaseSuspendRoleLate.cfg` passes with the role read late. With
  no wake check (`FelixRaftLeaseSuspendUnchecked.cfg`) it fails. That twin
  is also a VM pause, which stops both clocks, and a platform with no clock
  that counts suspended time: the design states both as assumptions. With
  the jump seen by a background watch after the wake
  (`FelixRaftLeaseSuspendLateCheck.cfg`) it fails too.

The void only grows. A broker that voided term 1 for a transfer can be
elected again in term 2 and then vote in term 3; a void kept as one term and
compared by equality lets the vote's void of term 2 lift the transfer's void
of term 1, and Felix's late view of term 1 passes beside the term 3 leader
(`FelixRaftLeaseTransferVoidEquality.cfg`). `FelixRaftLeaseTransferThreeTerms.cfg`
passes with the void kept as the highest term voided.

Each path has a configuration at lease 2, where the lease spans ticks. The
ones that add a write, a third term, both paths at once or drift use a lease
of 1 so they finish. The void refuses whole terms, so what it does does not
depend on how long the lease is.

Only messages openraft accepts raise the void: append and snapshot requests,
answers naming a higher vote, and votes it grants, where the void is taken
on the outgoing answer before it leaves. A vote request openraft refuses
changes nothing in openraft, so no new leader can use it, and the model has
no void for it. Voiding on it would let one partitioned candidate take local
acks from a stable leader for the rest of its term. The cost of a void that
does happen is the rest of that term:
`FelixRaftLeaseReachLeaseAfterVoid.cfg` shows the same broker passing the
check again once it is elected in a later term.

For the implementation:

- The void is one number per group, in memory, that only grows.
  `admit_local_ack` (`raft/engine.rs`) refuses every term at or below it. A
  restart starts with no lease, so it need not be kept.
- The network adapter raises it before openraft sees the message, on the
  messages above. `Group::transfer` raises it to its own term before the
  send.
- A granted pre-vote changes no vote and must not void. Felix refuses
  snapshots before openraft sees them (`raft/group.rs`), so openraft accepts
  none today; voiding on one anyway is safe.
- The wake check compares the monotonic clock with boottime inside
  `admit_local_ack`, not in a background task.
- The config refuses a lease unless
  `lease * (1 + p) / (1 - p) <= election_timeout_max` (`raft/runtime.rs`).

**Store-liveness leases (later design).** The group sends nothing of its own.
Each pair of brokers exchanges liveness heartbeats. A broker gives a peer's
store support for an epoch until an expiry on its own clock, keeps that on
disk, and withdraws only after the expiry. Withdrawing raises the epoch it
will accept, so withdrawn support never returns at the same epoch. A store's
epoch rises at every start, and when a peer reports it withdrew. A voter that
elects a leader whose store it supports records that epoch on disk and
refuses to vote for anyone else until it has withdrawn. The leader's lease
holds while a majority has recorded its current epoch and has answered a
liveness heartbeat handed off less than a lease ago. A broker whose disk has
stalled sends no liveness heartbeats and grants none. The same
`LeaseExclusive` is checked, and `StalledLeaseLapses`: a broker whose disk
has been stalled for a lease does not pass the check. A stalled broker that
kept asking would lose no record, since it cannot write, but it would keep
the lease and block the failover.

One rule here is not in the design page: the expiry is read on a clock that survives a restart. Two are in the page and not built. A
vote for another broker waits for the withdrawal, not just the expiry, and a
withdrawal raises the epoch. Without the second
(`FelixRaftLeaseStoreResupport.cfg`) a follower withdraws, votes, then
supports the old leader at the same epoch again and its lease comes back. The expiry is kept on
disk, so the implementation has to read it on a clock that keeps running
across a restart, or wait out a full support period after one.

**Abstracted.** Storage (log, term and vote survive a restart whole;
`FelixRaft.tla` has the fsync rules). Elections are one atomic step over a
majority, since vote messages in flight could only make an election later. An
append carries the leader's whole log. In the later design a liveness
heartbeat is taken the moment it is handed off, the earliest its support can
start, and only its answer is in flight. The heartbeat batch window and the
tick interval are not separate steps: the window sits between the hand-off,
where the stamp is taken, and the send, so it is part of a message delay that
has no upper bound in the model, and a timeout noticed at the next tick is a
vote or campaign taken later than allowed. Neither can break a check. The
lease check must read the clock when it runs; a check that used a value
cached at the last tick would add the tick to the lease. No blank entries,
membership changes, snapshots or pre-vote. A campaign that does not win is
not a step; `StepDown` stands for the leader learning of its higher term.

### Raft sleep and wake

Sleep and wake are deferred to v0.9.0, with the store liveness they rest on
(decided in issue #2167). Nothing in this section gates 0.8. The configurations stay
registered so the design keeps its checks until it is built. The design page
keeps sleep under "Deferred: sleep and wake", and its table "Rules the model found" now
states the two rules of issue #2310, with three
more the model does not have yet.

`FelixRaftSleep.tla` models one Raft group whose members can be asleep, as
[Per-shard Raft](../per-shard-raft.md) describes ("Store liveness", "Sleep and wake"). A member that is asleep gets no ticks: it starts no
election, and a sleeping leader sends no heartbeat. Votes, appends and commits still work
on any message, and a Raft message wakes the member it reaches.

The model has the sleep handshake (the request names vote, last log id and commit index,
followers sleep first, the leader last), every wake path (a write, a Raft message, a
liveness change, the audit timer), the ask a follower sends before it campaigns, the hold
on a restarted member, and store liveness with false alarms and missed failures.

Time is abstract. A timeout is a step that is enabled only when the event that would have
beaten it cannot happen. For example, a hold ends only while no live leader exists that
would have answered. That is an assumption about timing, not a result, and it decides what
`NoNeedlessDeposal` can show (below).

Liveness is checked on states where nothing more can happen (`QuiescentLed`,
`QuiescentLevel`), as `FelixRaft.tla` does. Every budget is bounded, so every behaviour
ends, and `FelixRaftSleepEnds` checks that as a temporal property. This keeps the main
checks invariants, which run with the symmetry set.

What is checked:

- The safety checks of `FelixRaft.tla`, with members falling asleep at any moment with no
  handshake (`MaxDozes`). Safety does not rest on the handshake.
- `QuiescentLed`: with a majority alive the group ends with a live leader.
- `QuiescentLevel`: under that leader every write is committed and every live member has it.
- `AsleepIsLevel`: if every live member is asleep, they share last log id and commit index.
- `NoNeedlessDeposal`: unless a leader's broker died or restarted, no election is won and
  the first leader never stops leading. A leader that steps down for a campaign that never
  wins counts as deposed. A violation costs availability, not safety.

What `NoNeedlessDeposal` does not test. Under the timing assumption a held or awake
follower cannot campaign while a live leader it accepts can still answer. So
`FelixRaftSleepFalseAlarm.cfg` and `FelixRaftSleepRestart.cfg` show that the rules leave
no other path to a deposal. They do not show that the hold is long enough, or that one
election timeout covers the ask and the heartbeat that answers it. Two faults on the wake
path are modelled and each deposes a healthy leader: a lost ask
(`FelixRaftSleepAskLost.cfg`, the design as written, issue #2320) and a hold shorter than
the ask's round trip (`FelixRaftSleepHoldTooShort.cfg`). A lost wake heartbeat is not
modelled: with no clock it cannot be told from a late one, and the woken leader's next
heartbeat follows it.

Knobs, each dropping one rule: `NoLivenessWake`, `NoAudit`, `AcceptUnchecked`,
`CampaignOnWake`, `LeaderSleepsFirst`, `RestartNoHold`, `HoldTooShort`. Two more drop rules
that are in the design page but not built, and the model needs: `HeartbeatKeepsProposal` (a
heartbeat during the handshake does not close the proposal) and `SleepUnsupported` (a
follower accepts a sleep request from a leader whose broker it does not support). Issue
#2310 has both. Every passing configuration with a handshake runs with both rules on, and
its row and header say so.

A third gap has no rule yet. The design leaves out of the handshake a member whose broker
store liveness says is down. When that alarm is false the member is healthy and awake: it
hears nothing from the sleeping leader and deposes it
(`FelixRaftSleepFalseAlarmFollower.cfg`), or it accepts an older sleep request and the
whole group sleeps with it behind (`FelixRaftSleepFalseAlarmFollowerBehind.cfg`). Both are
the design as written and are pinned as expected violations (issue #2310). The passing
configurations only raise false alarms about a leader's broker.

Two twins show less than the others. `FelixRaftSleepAcceptUnchecked.cfg` violates
`AsleepIsLevel`, which restates the rule it drops: the model has no lasting harm for it
that does not also need the third gap. `FelixRaftSleepLeaderSleepsFirst.cfg` reaches a
needless deposal, not a member left behind for good. All the harms in this module are
availability: the safety checks pass with members asleep in any state.

The second design on issue #2239 closes the third gap with five rules, each a knob:
`PreVote`, `VoteHold` (a vote or pre-vote that wakes a member is dropped and the member
holds), `LeftOutAsked` (the request also goes to a left-out member), `RefusalWakes` (a
refused request wakes the leader) and `AuditAsks` (a follower woken by its audit asks
first). `LeaseLapses` makes the first write after a wake wait for a heartbeat round, and
`MaxTtlWakes` lets a cache group's expiry timer wake its leader, which proposes the
cache model's conditional delete of the version the timer saw (`ExpiryKeepsNewer`). The `FelixRaftSleepWake`
family runs with all of them on. Dropping pre-vote or the hold, or counting a dropped
pre-vote as granted, deposes a healthy leader. Dropping `LeftOutAsked`, `RefusalWakes` or
`AuditAsks` does not: the hold alone keeps the leader, and those three only save a
pre-vote round or catch a member up sooner. Their configurations are registered as
passing to record that.

Not modelled: storage (an append is durable), snapshots, membership changes, read rounds,
ranked starts and the per-tick election cap. Messages are atomic steps except the sleep
request. Only a leader's broker dies, and a disk stall counts as a death.

### Raft cache state machine

`FelixRaftCache.tla` models a cache shard's Raft group as the second design on issue #2238
has it: one log of cache writes, deletes and counter adds that every member applies to a
projection, with each member snapshotting that projection on its own. That snapshot is the
only compaction. The leader proposes no copy or checkpoint entries, so the first design's
`CopyUnchecked` and `CheckpointUnchecked` twins and its `CompactionInvisible` check are
retired: the entries they guarded do not exist. Raft itself is abstracted to one committed
sequence, which `FelixRaft.tla` already checks, with a new leader that keeps a prefix of
the old leader's uncommitted tail and appends a blank entry.

The rules and their checks:

- Apply is deterministic and never reads a clock (`MembersAgree`, `ProjectionIsFold`).
- The projection holds the log's time, the stamp of the last stamped entry applied, and the
  snapshot carries it. The leader stamps no earlier than the last stamped entry in its log,
  the inherited tail included and blank entries skipped, or its snapshot's time when its
  log holds none (`StampsMonotone`). A leader that installed a snapshot therefore stamps
  from the snapshot, not from its clock.
- Every write carries a request id, and the projection keeps each applied id with its
  answer. A retry changes nothing (`AppliedOnce`, `SumExact`) and gets the first answer
  (`AnswersKept`).
- Expiry is a delete the leader proposes for the version it saw expire. It removes only
  that version (`ExpiryRemovesItsVersion`), so a write is never removed by the expiry of an
  older version.
- A read waits until the leader applied its commit index (`ReadsFresh`) and evaluates
  expiry at the leader's read time, the later of its clock and the log's time. When that
  finds a key expired that the log's time does not, the leader commits the conditional
  delete first and answers after applying it. So an idle group never reads an expired key
  as present (`ReadsNotStale`), and an "absent" is always in the log, where a later leader
  with a slower clock cannot contradict it (`OneClock`). The two simpler rules both fail:
  answering from the leader's clock alone breaks `OneClock`, and answering at the log's
  time alone breaks `ReadsNotStale`.
- An expired record is absent to every write, judged at the entry's stamp: a put-if-absent
  takes the key, a delete answers "absent", a write that names a version finds none. So
  after a read answered "absent", no write finds the key present until something writes it
  (`OneClock`). It also lets a snapshot hold live records only (`SnapshotDropsExpired`): a
  member that restored one and a member that replayed the log hold different records and
  give the same answers (`AnswersKept`, and `MembersAgree` over what a request can
  observe). A delete that removes an expired record breaks both: `OneClock` on its own,
  `AnswersKept` once a snapshot dropped the record.
- A put or a delete may name a version. It applies only to that version, unexpired
  (`CondExact`).
- Purge goes up to the member's snapshot and no further (`LiveReadable`).
- The table of applied ids is trimmed by entries, never by a member on its own. Each write
  carries its producer's floor, the lowest sequence the producer has no answer for, and
  applying it drops the answers below that floor. A retry below the floor answers
  "forgotten" and is not applied, which only happens to a request the producer already has
  an answer for (`ForgetsOnlyDone`). A retry at or above it gets its first answer
  (`RetriesKeepAnswers`). The snapshot holds the floor with the kept answers
  (`MembersAgree`). A retire entry drops a finished producer's answers and keeps its floor,
  raised past its last sequence: a copy of a request held in a queue or on the network can
  still commit after the retire, and with the floor gone it would be applied again
  (`AppliedOnce`). So a retired producer costs one floor in the table, not zero.

Each twin drops one rule and breaks one check; the module header lists them.

The bounds are small on purpose. Each configuration has one read (`MaxReads`) and at most
one expiry delete (`MaxExpiries`), and two or three proposals. One read is enough for
every read check, which compares a single read with the log before or after it; no check
relates two reads. One expiry delete covers the delete racing a write and a read waiting
for it. Two expiries of two versions of the key are not explored.

Not modelled: more than one producer (the floor is per producer, so producers do not
interact), a producer id used again after its retire, membership entries (they are
stampless like the blank entry), more than one key, and the log side of install and
purge (`FelixRaft.tla`'s snapshot configurations).

Two trim cases are not modelled either:

- A producer that restarts under the same id at a lower sequence, with no retire. A new
  write below the old floor answers "forgotten" and is never applied. `ForgetsOnlyDone`
  cannot see this, because the model has one producer whose sequences are its request
  ids. The producer has to retire, or take a new id, before it starts its sequence again.
- A retry that sends a floor recomputed since the first copy. The model keeps the first
  floor. This is safe by inspection: a retry that finds its answer in the table is a
  dedup hit, and a dedup hit never raises the floor.

### Idempotent batches on a Raft shard

`FelixRaftBatch.tla` models an idempotent batch of several records on a Raft
stream shard (#2524), which a Raft shard takes. The configurations cite no
tests; the code's tests are in `crates/server/felix-replication/src/raft/tests/idempotent.rs`.

An idempotent batch carries a producer id and a sequence number, and the shard
must store each of its records once however often the producer sends it. On
Raft every record is its own log entry, so a batch can be cut: some of its
entries commit and the rest do not. The model asks what a re-send must do
then.

What is modelled:

- Each entry of a batch carries a piece mark with the batch's full identity:
  producer, sequence, the record's position in the batch, and the batch's
  length.
- A batch is one message bound to the leader's term. The term and the lease
  are checked once, so the leader appends every piece or none, at consecutive
  indexes.
- The commit is not atomic. A change of leader hands the new leader a prefix
  of the old leader's uncommitted entries and drops the rest, and the new
  leader's blank entry follows. A batch with only its first pieces kept is a
  cut batch.
- A batch is acknowledged when every piece its message carried has committed
  under the leader that took it. Otherwise the producer sends it again, at
  any time, also while its first send is still in flight. A producer starts
  its next batch only after the one before is acknowledged.
- The producer table is a function of a member's table snapshot and the log
  it retains. The leader's includes its uncommitted entries: its own appends
  and the ones it took over at a change of leader. Per producer it holds the
  highest sequence seen, how many of its pieces are held, and its length.
  Pieces only land in order, so a piece at position j means positions 0 to j
  are held. That is why the count can be read from any suffix of the log.
- The leader places a send by its table: answered as held when every piece
  is and the committed log has them all, finished from the first missing
  position when the batch is cut, written whole when no piece is held, and
  refused when the table cannot say. Placing and appending are one step:
  the leader takes one producer's sends in turn and holds the turn until
  the pieces are appended, so two sends of one batch are never both placed
  as new.
- A purge writes the table as of the new base into the table snapshot. A
  snapshot install gives a member another member's base and table snapshot.
  So a member can lead with none of a cut batch's held pieces in its log.

What it promises:

- `BatchExactlyOnce`: no (producer, sequence, position) is in the committed
  log twice, and an acknowledged batch has every position in it.
- `BatchInOrder`: a producer's records are in the log in sequence and
  position order, and a batch starts only after the one before it is whole.
- `ProducerPlaceAgrees`: every member places a batch the way the whole log
  does, or refuses it. Two members never place one re-send differently,
  whatever each has purged or installed.

`AtPurge` is what a purge does with a batch that is cut at the new base, and
two choices are safe. `"refuse"` keeps the sequence and drops the count: the
member refuses the re-send, and a member that still holds the pieces finishes
it (`FelixRaftBatch.cfg`). `"keep"` keeps the count too, so any member can
finish it (`FelixRaftBatchKeepsCut.cfg`). The design uses `"refuse"`, with
the expired-sequence error. The model shows both are safe and does not weigh
them: it has no record payloads, so it cannot say whether a member without
the held records can tell that a re-send repeats the same batch. Nor does it
follow a producer past that error. A refused producer keeps re-sending the
same sequence and never starts the next one, so what a producer may do after
giving a sequence up is not checked.

Each knob drops one rule, and each twin fails:

- `TermBoundBatch = FALSE`: each record is its own message with its own term
  and lease check (#2472; the code now takes the term-bound shape, and FALSE stays as the counterexample twin). A lease that lapses and comes back inside the batch
  refuses some records and takes others. A batch of three lands its first and
  last records, the table counts it whole by its last position, and the
  re-send is acknowledged with the middle record missing. No change of leader
  is needed. The count is read from the highest position, which is only
  right when pieces land in order, and one message per batch is what keeps
  them in order.
- `ResendFinishes = FALSE`: the re-send of a cut batch is written whole
  (#2516). The held prefix is in the log twice, and out of order.
- `AtPurge = "forget"`: a purge past a cut batch drops it from the table
  snapshot. That member, or one that installs its snapshot, takes the re-send
  as a new batch and writes the held records again.
- `InstallKeepsTable = FALSE`: a snapshot install gives the member its
  sender's base and not its table snapshot (#2562). The member knows only
  what it holds above the base, so once it leads it takes the re-send of a
  batch cut below the base as new and writes the held records again.
- `TableAtAppend = FALSE`: the leader's table reads committed entries only. A
  send that arrives while the first is in flight is placed as new too. So is
  a re-send that reaches a new leader before the pieces it took over commit.
  Either way the records are in the log twice.
- `HeldAfterCommit = FALSE`: a send is answered as held as soon as the
  leader's log has every piece. The batch is acknowledged before it commits,
  and a change of leader can still drop it.

Abstracted: elections, replication and followers' uncommitted tails
(`FelixRaft.tla` has them; here a change of leader is one step), the lease
itself, record payloads and producers that expire. A restart with a torn
tail is not modelled either. It leaves a prefix with no blank entry after it,
and the table here needs no marker to see a cut, so the model says nothing
about how the code recognises one. Every member holds the committed log up
to its tail. Readers see the records of a cut batch
before it is finished.

A new leader in the model places sends as soon as it leads. The code serves a
term only once every earlier entry is applied (`raft/deliver.rs`), so it never
places a send over a tail it took over. The model allows more than the code,
so its passes still hold for the code. `FelixRaftBatchReachFinishOverTail.cfg`
and the second case of `FelixRaftBatchTableAtApply.cfg` are behaviour only the
model has.

What the code does to match the model. The model's producer table is the
code's, on a Raft log:

1. Every entry of a batch names the batch: producer, sequence, position and
   length. The first record is `RecordMark::Opens` and the rest are
   `RecordMark::Piece` (`log.rs`).
2. A leader's blank entry, or any other record, does not abandon a cut batch:
   `ProducerState` keeps it as a cut batch (`disk_log/producers.rs`).
3. The table keeps an unfinished batch per producer, not one per shard. The
   cut batches are capped at `MAX_CUTS`; past the cap the oldest becomes a
   forgotten entry, which the model, with two producers, never reaches.
4. A purge keeps a cut batch or refuses its re-send, never drops it (the
   `"refuse"` choice of `AtPurge`). A cut batch whose records are all purged
   is kept as a forgotten entry in `producers.forgotten`, so its re-send,
   sequence 0 included, is refused with `sequence_expired`.

### Reads and sessions on a Raft shard

`FelixRaftReads.tla` and `FelixRaftSessions.tla` extend `FelixRaft.tla` with
the client-facing properties `FelixShardReads.tla` and `FelixShardSessions.tla`
check on the hybrid (#2266): a `Quorum` read is never stale (`NoStaleRead`), a
group commit is never answered by a coordinator a newer one replaced
(`NoStaleGroupCommit`), and nothing delivered is taken back
(`NoLostDelivery`). The fence round and the lease are replaced by openraft's
read index. Each spec's header maps its steps to the code, file and line.

What is abstracted:

- The read is one read in flight, and its value is the set of records the
  broker applied. The answered bound is read when the read begins, before the
  round, where the code reads it after: a smaller bound, so the check is at
  least as strict.
- Both state reads always run the round. The lease read, which skips it, is
  reached only before a read that could start below a head trim, and
  `FelixRaftLease.tla` checks it.
- Written-majority answers are checked without crashes and in one term; with
  two terms the state space passed 83M distinct states without finishing. What a crash does to
  an unsynced majority is `FelixRaftWritten.tla`'s question.
- A group commit is a record entry. What it does to group state is
  `FelixRaftGroups.tla`'s question; this model asks only when it is answered.

### Consumer groups on a Raft shard

`FelixRaftGroups.tla` models a consumer group on a Raft stream shard (#2446).
A Raft shard refuses group operations today. The model is the design the code
is being built to, so its configurations cite no tests.

On the hybrid a group's state lives in two side logs beside the shard log, and
replication ships them. On Raft the group writes are entries in the stream's
own Raft log instead, and every member folds the committed ones, in order, into
the same group table: the cursor, the dead letters and the redriven records.
The leader serves the group from an in-memory tracker of claims and
acknowledgements, hydrated from that table. The model asks what keeps a
tracker's writes from undoing a seek or a delete, and what keeps a tracker in
step with the table.

What is modelled:

- One group over records 1..N. Entries are `advance(to)`, `dead(o)`,
  `redrive(o)`, `finish(o)` (a redriven record was acknowledged), `seek(to)`,
  `delete`, and the delete of another group in the same shard. The other
  groups appear only through their deletes, because the delete watermark is
  shard-wide.
- An acknowledgement updates the tracker first and proposes after, in any
  order with other proposals. So an advance decided before a seek can land
  after it. A dead letter is appended before the run over it closes, as the
  code writes it before settling.
- A live group has a creation token: the term and index of the seek that
  created or last reset it, or, for a group its first acknowledgement or dead
  letter created, the term its tracker was made in and the index it hydrated
  at. The term keeps a seek dropped at a change of leader apart from a later
  seek at the same index. Every tracker-driven entry carries the creation
  token of the group its tracker serves.
- The shard keeps the index of its latest delete of any group, and nothing
  per deleted name. Apply drops an entry whose creation token differs from the
  live group's, or that names an absent group with a token index below the
  latest delete. Within that the cursor only moves forward. Deleting one group
  leaves the other groups' trackers serving.
- That watermark also drops the entries of a group with nothing committed yet
  whose tracker hydrated before another group's delete. So when the leader
  applies a dropped entry that carries its own tracker's token, it drops the
  tracker and the next poll hydrates a fresh one past the delete. Whatever the
  dropped entries settled, a dead letter included, is delivered again. Only
  whether the old tracker handed anything out is kept, for if_new.
- A seek with if_new moves only a group that does not exist, decided by the
  leader from the table and its tracker.
- The leader decides from its applied table only, never from its own
  entries in flight. So a hydrate, a redrive and a seek first wait until the
  leader has applied its own last proposed entry. After a change of leader
  that entry is at or past everything committed before the term, so the wait
  also stands in for the read index.
- A tracker belongs to a term and is dropped at a change of leader. A change
  of leader commits a prefix of the old leader's uncommitted entries, drops
  the rest, and drops every proposal not yet made.

What is checked:

- `NoSkip`: every record between where the group started (or was last seeked
  to) and its cursor is a dead letter, is redriven, or was acknowledged to a
  tracker of this incarnation of the group.
- `RedrivenOwed`: the leader's tracker owes, holds or is finishing every
  redriven record in the table, so a redrive is delivered again.
- `IfNewKeepsServed`: an if_new seek is never placed while the group has
  handed out records in this term.
- `TrackerHeard`: no committed entry carrying the leader's current tracker's
  token was dropped. A tracker never serves on after the table threw its
  writes away, and with `NoSkip` a dropped dead letter is delivered again
  rather than lost.

Each broken configuration drops one rule and finds the trace: no creation
token at apply, no delete watermark, the earlier rule that dropped every entry
from a tracker hydrated before any delete, keeping a tracker whose entries were
dropped, if_new checking the table only,
trackers kept across terms, deciding from part of the committed log, deciding
before the leader's own entries are applied, a dead letter proposed in any
order with the advance over it, and a redrive that keeps the record in memory
only.

Not modelled: the group marks' place in the stream's offsets. The rule that
skipping a mark proposes nothing is what keeps two groups, or an idle group,
from feeding each other entries forever. That is a property of how the log
grows, not of any one state, so a code test holds it instead: an idle group
beside an active one, and the Raft log stops growing. How many reads a re-skip
over many marks costs is a liveness bound and is tested in code too. Also left
out: visibility timeouts (a lapse is a redelivery), attempt counts, discard,
snapshots and purge (the table travels in the stream snapshot once Raft logs
are purged), and the hybrid's lease and round, which
`FelixShardSessions.tla`'s `NoStaleGroupCommit` covers and Raft's term check
replaces.

### Purging a Raft stream shard

`FelixRaftStreamPurge.tla` models how a Raft stream shard frees disk below a
head trim (#2602). Today a head trim on a Raft shard hides records and
deletes nothing: stream shards never purge their Raft log, and every member
refuses a stream snapshot. The model is the design the code is being built
to, behind the `raft_stream_purge` fleet feature, so its configurations cite
no tests.

A purge drops a member's log up to an index. A member whose next entry the
leader has purged can only be sent the leader's snapshot, and a stream
snapshot carries no records. So the questions are where a purge may stop, what
the snapshot must carry, and when a member may install it.

What is modelled:

- An offset is a Raft index. A head trim is an entry `trim(f)`: once a member
  applies it, no record below offset `f` is readable there, and a read below
  it gets the trim's headers. The leader proposes a floor no higher than the
  offset the trim lands at (felix-broker `trim/proposed.rs` allows a floor equal to the
  tail), so the trim entry is never below its own floor.
- A purge to `P` takes two steps. The member writes and fsyncs a stream
  snapshot at `P`, then drops its log up to `P`. A restart can fall between
  them.
- The snapshot holds `P`, the tables (producers, consumer groups, cut batches)
  as of `P`, and the carried floor: the floor and trim headers the member has
  applied, which are above `P`. The tables are built from the previous
  snapshot and the entries the member still holds up to `P`. They are a fold
  of three numbers: records applied, trims that raised the floor, and the
  floor as of the index. Like the real tables, the fold is wrong when an entry
  is applied twice or skipped, and it reads the floor.
- A purge is bounded by the applied entry and by floor - 1. On the leader it
  is also capped at the lowest match of any member, learners included.
- The leader sends a member whose next entry it purged its snapshot. The
  member installs it only if every entry it skips is below the carried floor
  (index <= floor - 1). Otherwise it refuses, as today. It stores the higher
  of its own floor and the carried one, with that floor's headers, so an
  install never lowers the floor it keeps across a restart.
- A restart rolls a member's state back to its snapshot, and it applies its
  log again.
- A re-form starts a new epoch from the copies the operator's command reads
  (`FelixRaftReform.tla` has the command). Copies not read are declared dead.
  The leader is a read voter with the longest log. The voter set does not
  change, so a re-form whose longest read log is a learner's is not taken. The base is the highest
  offset a read copy vouches for, padded to the control plane's offset floor,
  which trails the commit. A copy vouches for its last entry, its purged id,
  and its floor: the trim that named floor `f` sits at offset `f` or above and
  was committed. Every read copy takes the highest floor read, with its
  headers, since the trim that set it may be in no read copy's log, and the
  re-form writes that floor into each copy's snapshot so a restart keeps it.
- A change of leader goes to a voter holding the committed log.

What is checked:

- `NoReadBelowFloor`: a leader serving reads (it has applied every committed
  entry) serves no record below the highest floor any live member ever had
  (a history variable, so a restart that loses a floor cannot hide it).
- `NoAckedLoss`: every member holds every committed record at or above the
  committed floor, up to the end of its log. No purge or install drops one.
- `PurgeBelowFloor`: a member that purged anything purged below its floor.
- `AppliedCoversPurge`: a member's state covers every entry it dropped,
  across restarts.
- `TablesAgree`: every member's tables equal the fold of the log up to its
  applied entry, across purges, installs and restarts.
- `HeadersAgree`: a member with a floor answers reads below it with the
  headers of a trim that named that floor. Any trim naming it passes, and two
  members' headers are not compared.
- `OffsetNeverReused`: a re-form never gives out again a committed offset that
  a copy it read held, or installed a snapshot or a floor for. It is checked
  against a history variable, not against what the re-form reads. A re-form
  that carries a floor into a copy counts as that copy holding it, so a second
  re-form must read it.
- `CatchUpPossible`: a member behind the leader's purge can install the
  leader's snapshot. This is not safety. A member that fails it is stuck, not
  wrong, until a later purge gives it a snapshot it accepts.

The broken configurations, and what each shows:

- A purge bounded by the applied entry only, not by floor - 1, drops a record
  at or above the floor (`FelixRaftStreamPurgePastFloor.cfg`).
- A snapshot taken at the applied entry, as a plain Raft snapshot is, sits
  above the trim and so above floor - 1. A member that installs it anyway
  skips records it was never sent (`FelixRaftStreamPurgeInstallAboveFloor.cfg`).
  With the install rule kept, every member behind the purge refuses it and
  stays stuck. The leader's cap does not save it: a follower purges past a
  slow member and then leads (`FelixRaftStreamPurgeSnapAtApplied.cfg`), and
  without the cap the leader strands the member itself
  (`FelixRaftStreamPurgePastMatch.cfg`). So the snapshot has to be written at
  the purge point, not at the applied entry.
- A snapshot at the purge point that carries the member's current tables
  makes an installing or restarting member apply the entries above the purge
  point twice (`FelixRaftStreamPurgeTablesAtApplied.cfg`).
- A snapshot with a single floor, the carried one, mixes two points: its
  tables are as of `P` but its floor is as of the applied entry. A member that
  restores it applies the trim above `P` again, finds the floor already there,
  and its tables miss that the trim raised it
  (`FelixRaftStreamPurgeFloorAtApplied.cfg`). The tables need their own floor
  as of `P`, and the carried floor is a separate field.
- A snapshot that carries the floor without the trim's headers leaves a member
  that restores it refusing reads below the floor with no headers, where the
  leader returns them (`FelixRaftStreamPurgeNoHeaders.cfg`).
- A purge that drops the log before the snapshot is durable, with a restart in
  between, leaves a member whose snapshot is below entries it no longer holds
  (`FelixRaftStreamPurgeDropFirst.cfg`).
- A re-form that reads a copy's last entry only sees nothing on a copy whose
  log is empty above its purge, which is a copy that just installed a
  snapshot, and gives out offsets that copy purged
  (`FelixRaftStreamPurgeReformEntries.cfg`). One that reads the purged id but
  not the floor gives out again offsets below a floor the copy installed
  (`FelixRaftStreamPurgeReformNoFloor.cfg`).
- A re-form that leaves each copy its own floor lets the new leader serve
  records below a floor another read copy installed
  (`FelixRaftStreamPurgeReformKeepsOwnFloor.cfg`). The copy that installed is
  a learner there: with three voters, the quorum that committed the trim
  always leaves a read copy that holds it.
- A re-form that raises the floor in memory but not in the snapshot loses it
  at the next restart, and the copy serves below it
  (`FelixRaftStreamPurgeReformFloorNotStored.cfg`).
- A second re-form that does not read the floor the first carried into a copy
  gives out offsets below it again, since that copy's log holds no entry for
  the floor and it installed nothing
  (`FelixRaftStreamPurgeReformNoFloorTwice.cfg`).
- An install that stores the leader's snapshot as it is lowers a member's
  floor. A member installs one leader's snapshot carrying floor 4, then the
  next leader's carrying floor 3 (taken before that leader applied the trim to
  4). A restart leaves it at floor 3, and a re-form that reads only it gives
  out offset 4 again (`FelixRaftStreamPurgeInstallLowersFloor.cfg`, steered
  to that history by `FloorDropPath`). A plain Raft install replaces the whole
  state machine, so the real code would lower the floor at once, without the
  restart.

Two rules close it. Storing the higher floor with its headers passes
(`FelixRaftStreamPurgeInstallKeepsFloor.cfg` on the same steered history).
Every pass configuration uses that rule, but the registered ones mostly
cannot reach the install where it matters: a member whose floor is above the
snapshot's carried floor, which is above the snapshot's index, which is above
the member's log end. That needs at least four entries, two trims and a change
of leader, and the registered `Small` bounds have three entries or no change
of leader. The registered `FelixRaftStreamPurgeInstallKeepsFloorSmall.cfg`
covers it: it is steered harder (`LowerFloorPath`: two records, then trims
naming 3 and 4 at offsets 3 and 4, and no restart or re-form until a member
has installed a snapshot carrying a floor below its own), its witness
(`...ReachLowerFloorInstall.cfg`) reaches that install, and its "replace"
twin (`...InstallLowersFloorSmall.cfg`) violates `OffsetNeverReused`, so CI
tells the two rules apart. `...InstallKeepsFloorLong.cfg` (a learner, five
entries, two changes of leader, dispatch only) goes past the install: the
installer leads, takes writes, purges, and another member installs the
snapshot the install rewrote (`...ReachRewrittenInstall.cfg`, registered). Refusing a snapshot that
carries a lower floor is also safe
(`FelixRaftStreamPurgeInstallRefusesLowerFloorSafe.cfg`,
`...NoCapSmallRefuse.cfg`), but the member then cannot catch up: the next
leader purged past it with a snapshot it refuses, and nothing sends it a
better one until that leader purges again
(`FelixRaftStreamPurgeInstallRefusesLowerFloor.cfg` violates
`CatchUpPossible`). So the install keeps the higher floor.

Bounds. The pass configurations at the bounds above are too big for CI's
hour: each passed 60 to 85 million distinct states without finishing in 45
minutes. They are not registered and run by the CI dispatch
(`tla_configs`, 360 minutes). The `Small` variants lower only the counts
(entries, a trim, a change of leader or a restart, never a rule or an
invariant) and are registered. A pass at the small bounds says nothing about
a longer log, so the dispatch result is what shows the big bounds.

The leader's cap at the slowest match is not a safety rule. With the snapshot
at the purge point, a member sent it always accepts it
(`FelixRaftStreamPurgeNoCap.cfg` passes, and so do the five-entry
configurations, which run without the cap). The cap only saves sending a
snapshot to a member that is merely behind.

Inside one epoch the floor never needs the snapshot to survive. The trim that
set a member's floor is above floor - 1, so no purge or install drops it, and a
leader applies it again before it serves. The carried floor is there for the
install check and for a re-form, whose base can be below that trim.

Witnesses show the bounds reach what they are meant to: a member that
installed serves as leader, a purge drops a record, a purge drops a trim entry
(at the bounds of `FelixRaftStreamPurgeFive.cfg`), and a write and a purge
follow a re-form (at the bounds of `FelixRaftStreamPurgeFiveReform.cfg`). The registered small
bounds have their own witnesses (`...ReachTrimPurgeFour.cfg`,
`...ReachWriteAfterReformFour.cfg`, `...ReachPurgeAfterReformFour.cfg`), so a
pass there is a pass over a space that reaches them. `TwoReformsSmall` (two
entries) cannot write after a re-form: a floor-carrying re-form takes the base
to the last index. `TwoReformsWrite` (four entries, no learner, no restart)
is the registered two-re-form bound that does, with witnesses for a write and a
trim between the re-forms, a floor raised above the carried one, a trim entry
purged, and a write and a purge after a re-form. `LearnerReformInstall` drops
the leader's cap so the learner installs before the re-form
(`...ReachInstallThenReform.cfg`); `LearnerReformSmall` keeps the cap, so
there the learner cannot install before the re-form. What the registered
bounds do not cover (a learner and a restart together with two re-forms and a
write between them) is left to the dispatch of `FelixRaftStreamPurgeTwoReforms.cfg`.

Abstracted: terms and uncommitted divergence (`FelixRaft.tla` has them; here
there is one global log and every member holds a prefix of it), votes, record
payloads, and lost disks (a copy a re-form does not read stands in for one).
The producer table's own rules at a purge and an install are
`FelixRaftBatch.tla`'s. Here only the index the tables are taken at is
checked.

What the code has to change, behind `raft_stream_purge`:

1. The stream snapshot (felix-replication `raft/machine.rs`, `stream_data`) is written at the
   purge point, with the producer, group and cut-batch tables as of that
   index, including the floor as of that index. Separately it carries the
   member's current floor and the headers of the trim that set it, which
   reads below the floor return (felix-broker `durable.rs` `too_old`). Today it carries
   only the forgotten cut batches, at the applied entry.
2. A member accepts a stream snapshot whose index is at most the floor that
   snapshot carries minus one (the snapshot's own floor, not the member's or
   the max of the two), and refuses any other (felix-replication `raft/group.rs`, `Refused::NoSnapshots`
   refuses every one today). The model compares the index with the member's
   log end. With terms, the member compares it with its matching index and
   drops a divergent tail above it, as Raft does.
3. Applying an entry folds against the tables' own floor, never the carried
   floor.
4. The purge driver purges to at most floor - 1. It fsyncs the snapshot
   before it drops any log, and on restart a durable snapshot above the log's
   first index finishes the drop. The leader also caps at its slowest match,
   to spare a snapshot.
5. Felix drives every purge, not openraft: openraft builds a snapshot's meta
   at the applied entry (`Builder::meta` in `machine.rs` takes the last log id
   and the membership together). The driver takes the term and the membership
   from the entry at `P` before it purges, asserts that the purge point equals
   the snapshot's index and is at most floor - 1, and openraft never purges or
   builds a snapshot at the applied entry. Stream shards run with
   `SnapshotPolicy::Never` today, which keeps it that way. The model has no
   membership.
6. A re-form's first offset is above max(floor, purged id, last entry) of
   every copy it reads, and the new epoch starts at the highest floor read and
   its headers, written into each copy's base snapshot (`machine.rs`
   `base_snapshot`).
7. An install never lowers a member's floor (`InstallFloor = "max"`):
   - the serving floor in memory is the max of the member's and the
     snapshot's carried floor, with that floor's headers, before the
     installed state is swapped in;
   - a durable floor record that only rises is written before or with the
     swap, and a restart takes the max of it and the snapshot's floor;
   - that max'd floor and its headers are what a restart loads, what a
     re-form reads, and what this member sends when it later ships its own
     snapshot;
   - the max applies to the carried floor only; the tables, including their
     floor as of the index, come from the snapshot unchanged;
   - a trim at or below the serving floor neither lowers it nor swaps its
     headers;
   - the accept check in item 2 uses the snapshot's own floor.

The group table travels in the snapshot only once Raft stream snapshots carry
it (the consumer-group work, `FelixRaftGroups.tla`). Until then a member that
installs a snapshot would rebuild an incomplete group table, so
`raft_stream_purge` waits for that change.

### Conditional publishes on a Raft shard

`FelixRaftCond.tla` models a conditional publish on a Raft stream shard
(#2446). A conditional publish names an offset, `expected`, and lands there
or not at all. The offset is the Raft index.

What is modelled:

- Raft in the abstract: per-member logs of entries with terms, elections by
  vote, followers that cut a conflicting tail before they append, and a
  commit at a majority in the leader's own term.
- The leader appends one entry for a conditional publish wherever its log
  ends. Every member decides from the entry and its index alone: at
  `expected` it holds the record, anywhere else a blank that readers skip.
  The stored entry carries that decision as `kept`, worked out when the
  member writes it. A cut removes the entry and its decision together.
- Plain publishes, the blank a leader writes when its term starts, and a
  membership entry the leader may append at any time. Publishes go through
  the leader's queue in order. The blank and the membership entry do not.
- The leader answers a client once its entry is committed: "ok at i" when it
  is kept, "mismatch" otherwise.

What it promises, over each member's committed prefix:

- `CondKeptAtExpected`: every kept conditional record is at its expected
  offset.
- `OneKeptPerOffset`: of the publishes naming one offset, at most one is kept.
- `MembersAgree`: two members that committed an index hold the same entry
  there, `kept` included.
- `AnswersHold`: a client answered "ok at i" is kept at i, its expected
  offset, on every member that committed i. One answered "mismatch" is kept
  nowhere.

`LeaderDecides = TRUE` drops the rule: the leader decides `kept` when the
publish arrives, from its guess of the index (its log, plus the queue ahead,
plus one), and followers copy it. A membership entry appended between the
guess and the append moves the publish one index on, where it is kept
(`FelixRaftCondLeaderDecides.cfg`). With no membership entry the guess is
right (`FelixRaftCondLeaderDecidesNoMembers.cfg`), which shows the
membership entry is what breaks it. `FelixRaftCondReachAnswers.cfg` shows the
passes are not vacuous: one publish is answered "ok" and another "mismatch".
The leader's queue is modelled only for the guess. When members decide at
write time, a publish is appended as it arrives, which changes no outcome.

Abstracted: disks, crashes and snapshots (`FelixRaft.tla` has them). A
message is one step that reads both members, and a leader counts a follower
toward a commit by reading its log. A client sends once; a retry is a new
publish. Any leader holding the entry committed may answer, which allows more
than the code.

#### Batches of several records

A conditional batch of N records (#2751) cannot be one entry: the offset is
the Raft index, so N records need N indexes. The model appends a batch as N
entries in one step, at consecutive indexes, the way the leader writes them
in one call bound to its term. Each entry carries its position in the batch
and the batch's length. A member keeps the piece at position p only at index
`expected + p`, so a batch is kept whole or blanked whole.

The commit is not atomic. A change of leader can commit a strict prefix of a
batch and put the new leader's blank where the next piece was. That prefix is
a cut batch: it stays in the log, kept, and must never reach a reader.

A reader on a member decides from that member's log alone:

- The read bound is the commit index, except when the entry at the commit
  is a kept piece that is not its batch's last. Then the bound stops before
  that batch's first piece: the batch may still commit whole, or be cut.
- Below the bound, a kept piece at index i, position p, length n, is a record
  only if every entry from i + 1 to `i + n - 1 - p` is committed and is a
  kept piece of length n at the next position. The first committed entry in
  that range that is anything else cuts the batch at once, before its last
  index commits, so a quiet stream does not hold the bound. Pieces take
  consecutive indexes and a cut only takes a tail, so a run that reaches a
  last piece is one batch. The check reads the marks, not a batch identity,
  because the stored entries have none.

This is the rule of `resolve` and `bound` in
`crates/server/felix-storage/src/log/conditional.rs`.

What it adds:

- `WholeBatchesOnly`: every record a reader can see belongs to a batch whose
  every piece the member holds committed and kept, and the reader sees every
  piece.
- `ReadsStable` (a property): a member's read bound never falls, and what a
  reader sees below it never changes.
- `AnswersHold` now covers every piece: a batch answered "ok" is kept at
  `expected .. expected + n - 1` on every member that committed those
  indexes. A cut batch is never answered; its proposer lost its term.

`Hide` selects the read rule. `"whole"` is the rule above
(`FelixRaftCondBatch.cfg`). `"none"` lets a reader see every committed kept
piece and violates `WholeBatchesOnly` (`FelixRaftCondBatchNoHide.cfg`).
`"bound"` keeps the bound but drops the skip, and violates it too
(`FelixRaftCondBatchBoundOnly.cfg`): once the cut and the blank after it
commit, the bound passes the cut. `"kept"` checks only that the entry at the
last index is a kept piece and violates it as well
(`FelixRaftCondBatchAnyLast.cfg`): a record of another publish can be kept
there, which is why the check compares position and length. Three witnesses
show the pass is not vacuous: a cut batch does commit
(`FelixRaftCondBatchReachCut.cfg`), a whole batch is read
(`FelixRaftCondBatchReachRead.cfg`), and a batch is cut, with the bound past
it, before its last index commits (`FelixRaftCondBatchReachEarlyCut.cfg`).

The single-record configurations set every batch length to one, which is the
model as it was. Not modelled: a member that predates the rule and would
deliver a cut prefix. The design keeps such a member out with a fleet gate.
`LeaderDecides` is checked with one-record publishes only.

## How CI checks the models

TLC reads only the spec, so the model-check jobs in `.github/workflows/ci.yml`
run only when `docs/formal/`, `scripts/check_tla.sh` or that workflow changes.

A pull request checks only the configurations its diff can affect. The
`formal-plan` job lists them with `scripts/check_tla.sh --affected <base>`:

| The pull request changes | It checks |
| --- | --- |
| a `.cfg` | that configuration |
| a `.tla` | every configuration of that module and of each module that `EXTENDS` or `INSTANCE`s it, transitively |
| a registration line in `check_tla.sh` (`expectations`, `walks`, `weights`, `pr_minutes`) | the configuration that line names |
| a `Pattern) echo Module ;;` line of `module_of` in `check_tla.sh`, added, changed or removed | the configurations its old and new patterns match, plus those of the module and its dependents, as for a `.tla` |
| any other code line in `check_tla.sh`, the `module_of` default branch included | all of them |
| `ci.yml` before `jobs:`, or in the `changes`, `formal*` or `ci-ok` jobs | all of them |
| comments, blank lines, the README, anything else | nothing |

Only registered configurations are listed. From those, a pull request checks
the ones that take ten minutes or less, plus `FelixShard.cfg` whatever its
time and any whose own `.cfg` it changes up to 30 minutes. A `FelixRaft*.tla`
change counts as a change to each of its configurations. The list is split
into one shard per 30 minutes of measured time (`pr_minutes`), at most thirty,
and no shard runs when the list is empty; `CI ok` treats the skipped job as a
pass. A changed configuration over 30 minutes runs in a job of its own
(`formal-heavy`), planned with `scripts/check_tla.sh --plan-heavy`. The
walk of the unprobed crash (`formal-walk`) runs when those configurations
are affected.

So a pull request that edits only `FelixRaftSwitch.tla` runs the
`FelixRaftSwitch*` configurations in a few shards, not thirty, and one that
edits `FelixShard.tla` still runs nearly everything, since most modules
extend it. Run `scripts/check_tla.sh --affected origin/main` to see the list
before you push.

A push to main checks every registered configuration in thirty shards, filled
by each configuration's measured time, and walks the unprobed crash. Main's
shards share a concurrency group per shard: one run proceeds and only the
newest later push waits, so a burst of merges does not queue hours of runner
time. A slow configuration that a change to the `.tla` alone breaks therefore
shows up on main, not on the pull request; run it on the branch through the
workflow's dispatch (`tla_configs`) when the change is risky. A dispatch also
uses thirty shards.

Whether a code change still matches the spec is a separate, quick check on
every pull request that touches code (`scripts/check_spec_pairing.py`).

Some configurations are too big for that hour, and run nightly instead, each in
its own job with a six-hour limit (the `exhaustive` job in
`.github/workflows/tla-walk.yml`): `FelixRaftPlacementFull.cfg`,
`FelixRaftPlacementRewindSpare.cfg`, `FelixRaftWrittenFailover.cfg`,
`FelixRaftReformTwoReformsOldRestore.cfg`, `FelixRaftReformKeptTerms.cfg` and
`FelixRaftWrittenCommitsReformJointCutDead.cfg`. Each must pass. A failure
shows as a failed run of that nightly workflow. Either set can also be run on
a branch through the dispatch of either workflow.

## Long random walks

A few configurations are too big to check exhaustively, so TLC samples them in
its simulation mode: up to `TLA_DEPTH` steps per trace, for `TLA_SIM_MINUTES`
each. They run nightly, not per push (`.github/workflows/tla-walk.yml`).
A walk shows less than a check. A pass means the walk found nothing in its
budget, not that nothing is there. Each walked negative twin must find its
violation within the same budget, which shows the walk reaches deep enough to
lose a record at all. A twin checks only the invariant it expects.

| Configuration | Knobs | Must |
| --- | --- | --- |
| `FelixCrossShardCommit.cfg` | `FelixCrossShardCommit.tla`, the [cross-shard commit](../cross-shard-commit.md) design: a home shard and two participants as Raft groups with a leader change each, two txns, messages lost, duplicated and delayed, lock asks at any time, dedup ids forgotten under the fence, home retention, a single-shard write, and a forger on the internal port | pass `Atomicity`, `NoDoubleApply`, `PreconditionsHeld`, `NoForgedApply` |
| `FelixCrossShardCommitLive.cfg` | the same without the forger, under fairness | pass `EventuallyRetired` |
| `FelixCrossShardCommitNoFirstWins.cfg` | every decision record acts on its own | violate `Atomicity` |
| `FelixCrossShardCommitReleaseOnTimeout.cfg` | a lock released on its timer | violate `Atomicity` |
| `FelixCrossShardCommitTrimUndone.cfg` | retention trims a decision before done | violate `Atomicity` |
| `FelixCrossShardCommitNoFence.cfg` | no term or low-water fence, ids forgotten at will | violate `NoDoubleApply` |
| `FelixCrossShardCommitWriteCheckEarly.cfg` | a single-shard write checks for locks when proposed, not at its place in the log | violate `PreconditionsHeld` |
| `FelixCrossShardCommitApplyWithoutLock.cfg` | an apply written with no lock | violate `NoForgedApply` |
| `FelixCrossShardCommitNoTokenCheck.cfg` | a prepare granted on the peer credential alone | violate `NoForgedApply` |

```bash
TLA_SIM_MINUTES=10 scripts/check_tla.sh --simulate FelixCrossShardCommit
```

Each walk gets `TLA_SIM_MINUTES` (default 25) of traces up to `TLA_DEPTH`
steps (default 300). The script prints the seed it used; `TLA_SEED` sets it.
TLC's whole output for each walk, with any trace, is written to
`target/tla/walk/`, and the nightly job uploads it when a walk fails. With
more than one worker a seed need not replay the same trace, so the uploaded
trace is the record.

## Running it

```bash
task tla:check          # java, docker or podman; fetches the TLA+ tools once, pinned
task tla:walk           # the long random walks, TLA_SIM_MINUTES each
```

To explore a configuration by hand, with the trace when a check fails:

```bash
java -jar target/tla/tla2tools-v1.7.4.jar -deadlock -workers auto \
  -config docs/formal/FelixShard.cfg docs/formal/FelixShard.tla
```

The bounds (`MaxTime`, `MaxWrites`, and the `SYMMETRY` over brokers) keep the
whole suite inside the CI job's hour. Widening them widens what is checked; the
invariants do not change. The script runs TLC with checkpoints off and its
scratch directory outside the tree. Run by hand without `-metadir`, TLC writes
a `states/` directory beside the spec that reaches gigabytes; `.gitignore`
covers it.
