# A formal model of one shard

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
| `ElectionSettles` | With the control plane down, elections end with a live leader that stays open and that no live broker has promised past. A temporal property (`<>[]LeaderSettled`), checked under `LiveSpec` in `FelixShardElectLive.cfg`. |
| `NoStandAgainstHeardLeader` | Nobody starts fencing against an open leader that a majority of its set still hears. An action property, checked with the liveness configurations. |
| `OwnBallotCaughtUp` | The control plane's leader does not stay open at its generation while its own copy holds its own ballot above it. While it does, it never counts its own copy, so with one follower down nothing it writes is acknowledged (#1933). A temporal property, checked under `RenameSpec` in the `FelixShardElectRouteLag*.cfg` configurations. |
| `OwnBallotFenceCaughtUp` | Nor does it stay fencing at its generation while its own copy holds its own ballot above it. The code's open takes the generation's ballot on its own copy, which refuses it, so it never opens there (#1940). Checked under `RenameSpec` in the `FelixShardElectRouteLagFence*.cfg` configurations. |
| `CollisionSuperseded` | With the control plane up, it does not stay at a generation whose leader can never open, because another broker was elected there and opened, while that broker still leads. A temporal property, checked under `ReissueSpec` in `FelixShardElectLiveReissue.cfg`. |
| `CpLeaderServes` | With the control plane up and every stopped broker back, the leader it names ends up open at its generation, serving with a mark over its whole log and a majority of its set taking its batches, and stays (`<>[](LeaderServes /\ ServesWith(leader))`). Checked under `CpLiveSpec` in `FelixShardCpLive.cfg` and its twins. |
| `CpDownServesInBound` | With the control plane stopped for good, some live leader ends up open and serving on a majority that holds its generation for it, and stays (`CpDownServes`), or elections stop only at the model's bound with a broker that would still stand (`Exhausted`). Checked under `CpDownSpec` in `FelixShardCpDown*.cfg`. |
| `CpDownFullyBacked` | The same, with every live member of the leader's set behind it. Not owed: `FelixShardCpDownDegraded.cfg` is its witness. |
| `EventuallyServes` | In `FelixShardUnified.tla`: once faults stop, some leader serves for good, unless a majority of the nodes lost their trusted logs, which waits for the operator by design. Checked under `LiveSpec` in the `FelixShardUnifiedCpLive*.cfg` configurations. |
| `NoContinuingOnForeignBallot` | In `FelixShardUnified.tla`: no continuing fence opens counting a crashed copy whose ballot names another leader. A reachability probe: under the rule this happens and is safe, so `FelixShardUnifiedContinuingDoubted.cfg` must find it. |

## The configurations, and what each must do

`scripts/check_tla.sh` holds each configuration to a declared outcome. A
configuration that must find a violation exists to show a check is
load-bearing, or to pin a finding the design has not acted on yet; a violation
that quietly became a pass would be a model that stopped saying anything.

| Configuration | Knobs | Must |
| --- | --- | --- |
| `FelixShardLease.cfg` | drifting clocks, no writes: heartbeats, lapses, promotions | pass `AtMostOneServing` and `NoStaleCommit` (0.78M distinct states) |
| `FelixShardLeaseAnyHeartbeat.cfg` | `FelixShardLease.cfg` with a heartbeat renewing a broker the control plane no longer names leader, as the code did before it waited on its routes | violate `AtMostOneServing`: a leader promoted away heartbeats again and renews at its old generation beside its successor |
| `FelixShardLeaseRoutesHeartbeat.cfg` | `FelixShardLease.cfg` with the code's rule: that broker's renewal waits for routes at the assignment feed's head as of the accept, which name its successor | pass `AtMostOneServing` and `NoStaleCommit` (0.78M distinct states) |
| `FelixShardLogOrder.cfg` | both lease checks, `Quorum`, two writes, promotion by log order | pass every invariant (1.52M distinct states) |
| `FelixShardThinMargin.cfg` | drifting clocks with `Margin = 0` and `Eps = 0` | violate `AtMostOneServing` |
| `FelixShardRealMarginsLease.cfg` | the margins the code runs: the broker gives up a quarter of the lease (`Eps = 1` of `L = 4`), and the control plane marks it down a quarter past its expiry (`Margin = 1`), against clocks that drift by a quarter; no writes | pass `AtMostOneServing` and `NoStaleCommit` (2.10M distinct states); with `Margin = 0` it finds two brokers serving |
| `FelixShardRealMargins.cfg` | the same margins and drift with one `Quorum` write carried across a promotion, acknowledged on the report's answer and a valid lease, as the code does | pass every invariant (2.38M distinct states) |
| `FelixShardAckWithoutLease.cfg` | the same with the lease taken out of the acknowledgement: the report alone releases it | pass every invariant (2.38M distinct states, the same ones: the report is only sent on a valid lease) |
| `FelixShardFencedAck.cfg` | the broker once `majority_ack` is finalized: acknowledged by follower acks at the leader's generation, no lease anywhere on the write's path and no report, the promotion fence, and the start record; no margin on either side of the lease, drifting clocks, two writes | pass `AckedHeldByLeader`, `AckedAgree`, `AckedOnMajority`, `NoTruncationBelowHwm` (13.0M distinct states, depth 31, 100 s on sixteen cores) |
| `FelixShardFencedAckTwoPromotions.cfg` | the same with two promotions (`L = 2`) and no drift | pass `AckedHeldByLeader`, `AckedAgree`, `AckedOnMajority`, `NoTruncationBelowHwm` (36.7M distinct states, depth 36, 28.5 min on a four-core CI runner; nightly, see below) |
| `FelixShardFencedAckTwoPromotionsStart.cfg` | the same with the start record on and one write | pass, the same invariants (28.8M distinct states, depth 36, 18.5 min on a CI runner) |
| `FelixShardFollowerLabels.cfg` | the same with followers labelling a shipped record with the sender's generation rather than the one that wrote it (`LabelOnReceipt`) | violate `AckedOnMajority` |
| `FelixShardUnfencedAck.cfg` | the same without the fence | violate `AckedHeldByLeader` |
| `FelixShardFencedCache.cfg` | `FelixShardFencedAck.cfg` for a cache shard (`Counters`): a counter log beside the cache log, shipped, counted and fenced the same way under one promise per replica, the fence taking the counter log furthest ahead too; no drift | pass `AckedHeldByLeader`, `AckedAgree`, `AckedOnMajority`, `NoTruncationBelowHwm`, `CountersHeldByLeader`, `CountersOnMajority` (not yet measured) |
| `FelixShardFencedCacheUnfenced.cfg` | the same without the fence | violate `AckedHeldByLeader` |
| `FelixShardFencedCacheNoCounterCatchUp.cfg` | the same with a fence that takes no counter log | violate `CountersHeldByLeader` |
| `FelixShardElect.cfg` | `FelixShardFencedAck.cfg` with replicas electing themselves (`Elections = 2`) and ballots, two writes, time standing still so the control plane never promotes | pass `OneLeaderPerGeneration`, `AckedHeldByLeader`, `AckedAgree`, `AckedOnMajority`, `NoTruncationBelowHwm` (not yet measured) |
| `FelixShardElectNoBallot.cfg` | the same without ballots, one generation of elections and no writes | violate `OneLeaderPerGeneration` |
| `FelixShardElectStaleSet.cfg` | `FelixShardElect.cfg` with a follower replacement, as in `FelixShardFencedAckSeat.cfg`, and one generation of elections past the control plane's | violate `AckedHeldByLeader`: the replica that left stands on the set it started in, and opens without a record the new set acknowledged |
| `FelixShardElectAdopt.cfg` | `FelixShardElectStaleSet.cfg` with the set check, the joint fence, and the control plane adopting elected leaders up to generation 2 (`Adopt = 2`, which also bounds the generations replicas stand for) and replacing followers under them | pass `OneLeaderPerGeneration`, `AckedHeldByLeader`, `AckedAgree`, `AckedOnMajority`, `NoTruncationBelowHwm`, `AdoptedMatchesOpened`, `SetChangesJoint` (not yet measured) |
| `FelixShardElectAdoptAboveKeptSet.cfg` | `FelixShardElectReissueReplace.cfg` with no writes, adoption up to generation 4, refusals that teach no set and candidates standing no lower than the generation of the set they keep (`StandsAboveKeptSet`) | violate `AdoptedMatchesOpened` (#1929, about 15 s): placement names a leader at a generation it then refuses, a candidate elected there on its stale set is the control plane's own leader, and adoption takes the generation alone (`adopting_elected_set` returns before comparing the sets) |
| `FelixShardElectAdoptAboveKeptSetReissued.cfg` | the same with the control plane's own leader elected on another set reissued on it (`ReissueOwnOnOtherSet`, #1930) | pass; not registered: no violation in 3.9M distinct states and seven minutes on a laptop, where the twin above violates in 15 s |
| `FelixShardElectAdoptTwoWrites.cfg` | the same with two writes | pass, the same invariants (nightly) |
| `FelixShardElectAdoptFailover.cfg` | `FelixShardElectAdopt.cfg` taken to a failover after a recorded-only adoption: placement promotes on suspicion up to generation 3 (`Suspicions = 3`), so the control plane's set and the set in the promoted broker's log can differ, and the fence takes a majority of both (`FenceRecordedSet` beside `FenceOwnSet`) | too big for one run (192.5M distinct states at depth 20, no violation, after six hours); checked in the pieces below |
| `FelixShardElectAdoptFailover{Same,Apart}{0,1}{,Stood}Gen{2,3,4}.cfg`, `FelixShardElectAdoptFailoverSame{0,1}Stood{One,Several}{Dest,Other}Gen2.cfg` | `FelixShardElectAdoptFailover.cfg` split at its first failover (`SplitAtFailover`, `CutOverPiece`): the promoted broker's own set is the control plane's or not, 0 or 1 writes were admitted before it, a replica had stood past the control plane's generation or not, and the failover takes generation 2, 3, or 4 and up (`PieceGens`). Each piece also checks everything before its failover, so together they reach every state the whole does | pass, the same invariants. Twelve pieces hold no failover: they hold just the 1.48M distinct states before one (depth 36, 3 to 5 min on a four-core CI runner) and check that they stay empty (`NoCutOverInPiece`). `Apart0Gen4` 13.5M distinct, 46 min; `Apart1Gen4` 15.5M, 54 min; `Same0Gen4` 36.3M, 2 h 15 min; these run nightly. `Same1Gen4` 56.8M, 3 h 40 min, and `Same1Gen2` 149.3M, depth 51, 5 h 20 min (close to the six-hour job limit), also run nightly. Five did not finish in six hours, with no violation by then: `Same0Gen2` (130M distinct), `Same0Gen3` (224M), `Same1Gen3` (111M), `Apart0Gen3` (108M), `Apart1Gen3` (156M). They need another split and are not checked yet. `Same0StoodGen2` ran out of runner disk and `Same1StoodGen2` did not finish, so each is split again into `Stood{One,Several}{Dest,Other}` (`PieceStoodCounts`, `PieceDestStood`: one or several replicas stood, the promoted broker among them or not). Those eight are not measured yet |
| `FelixShardElectSwap.cfg` | `FelixShardElectStaleSet.cfg` with a cut-over that changes the set by more than one member at once (`Swaps = 1`), fenced jointly, the set check, one generation of elections, two writes | pass, the same invariants, checked in the fifteen pieces below: one run did not finish in six hours |
| `FelixShardElectSwap{Keep,Drop}{0,1,2}{,Stood}.cfg` | `FelixShardElectSwap.cfg` split by how its cut-over went: the new set keeps or drops the old leader, 0, 1 or 2 writes were admitted before it, and a replica had stood past the control plane's generation or not (`SplitAtCutOver`, `CutOverPiece`). `Drop0Stood` did not finish in six hours and `Drop1Stood` took 5 h 15 min, so each is split again by whether one or several replicas had promised past that generation and whether the destination was one of them (`FelixShardElectSwapDrop{0,1}Stood{One,Several}{Dest,Other}.cfg`). Each piece also checks everything before its cut-over, so together they reach every state the whole does. `QuietView` merges states that differ only in what no step reads again | pass, the same invariants. On a four-core CI runner: `Keep2` 2.46M distinct, 5 min; `Drop2` 1.45M, 5 min; `Drop1` 7.17M, 17 min; `Keep1` 9.23M, 27 min; `Drop0` 17.9M, 43 min; `Keep0` 17.7M, 53 min; `Keep2Stood` 47.9M, 1 h 58 min; `Drop2Stood` 35.5M, 2 h 9 min; `Keep1Stood` 97.0M, 2 h 49 min; `Keep0Stood` 97.1M, 2 h 50 min; `Drop0StoodSeveralOther` 5,189, 3 s; `Drop0StoodOneOther` 4.24M, 13 min; `Drop0StoodOneDest` 17.9M, 39 min; `Drop0StoodSeveralDest` 84.0M, 2 h 37 min; `Drop1StoodSeveralOther` 5,189, 2 s; `Drop1StoodOneOther` 1.91M, 3 min; `Drop1StoodOneDest` 7.17M, 12 min. Those over 45 minutes run nightly. The two `StoodSeveralOther` pieces hold no cut-over: the old leader must still lead when it cuts over, and it stops leading once it answers a fence, so besides the destination only the third member can have stood. They hold just the states before a cut-over and check that they stay empty (`NoCutOverInPiece`) |
| `FelixShardElectRename.cfg` | `FelixShardElectSwap.cfg` with one write, generation 3 in reach (`Adopt = 3`), and the control plane naming its leader again, fenced, one generation up, once that leader reports it cannot serve a generation a recorded election moved the control plane to, or its fence at the control plane's generation is refused on either set (`Renames`, `CpRename`); `QuietView` | too big for one run (no violation in its first 5.8M distinct states); checked in the pieces below |
| `FelixShardElectRenameDue.cfg` | its witness | violate `NeverRenameDue` with `StandsOnPendingSet` on (835 distinct states, depth 9, 1 s; the broker refuses to stand while a change to its kept set is pending, so with the refusal modelled this is unreachable, and the witness shows what it protects): after a cut-over, a replica on the set being left is elected at generation 3, the control plane records only that generation, and a majority of the pending set promised it away, so the destination's fence cannot reach one |
| `FelixShardElectRenameFencingDue.cfg` | its witness for the leader that took the generation | violate `NeverRenameWhileFencing` with `StandsOnPendingSet` on (298 distinct states, depth 8, under a second): the leader fences jointly while a set change is pending, and a replica on the set being left stood at that generation and won another's promise, so a majority of the old set is out of reach |
| `FelixShardElectRename{Keep,Drop}{0,1}{,Stood{One,Several}{Dest,Other}}.cfg` | `FelixShardElectRename.cfg` split at its cut-over as `FelixShardElectSwap.cfg` is (`SplitAtCutOver`, `CutOverPiece`). Each piece also checks everything before its cut-over, so together they reach every state the whole does. The `StoodOneDest` pieces run nightly: the cut-over overwrites everything the destination's stand wrote, so they should reach the same states as the pieces where nobody stood (argued, not measured) | pass, the same invariants. `{Keep,Drop}{0,1}StoodSeveralOther` 37,646 distinct, 2 s on a laptop. On a four-core CI runner the others take 2 to 36 min (1.1M to 25.1M distinct, depth 37 to 40), except `Keep0StoodSeveralDest` (38 min to 1 h 16 min), `Keep1StoodSeveralDest` (51 min) and `Drop0StoodSeveralDest` (1 h 21 min), which run nightly with `Drop0StoodOneOther` and the `StoodOneDest` pieces. The four `StoodSeveralOther` pieces hold no cut-over, as in `FelixShardElectSwap.cfg`, and check that they stay empty (`NoCutOverInPiece`) |
| `FelixShardElectRename{Keep,Drop}1StoodOneOtherReportStore.cfg` | `FelixShardElectRename{Keep,Drop}1StoodOneOther.cfg` with the rename read from the report the control plane stored (`ReportStore`, below), which can outlast the state that caused the refusal | pass, the same invariants (`Keep` 965K distinct states, `Drop` 846K, depth 38, under a minute on a laptop) |
| `FelixShardElectSwapNoJoint.cfg` | the same without the joint configuration: the cut-over fences, ships and acknowledges on the new set alone | violate `AckedHeldByLeader`: a replica left on the old set stands at the new leader's generation, opens on a majority of the old set the new leader never fenced, and acknowledges a record the new leader does not hold |
| `FelixShardElectSwapFlip.cfg` | `FelixShardElectSwap.cfg` with one write and the flip as the code writes it (`FlipNewGeneration`): the control plane takes the pending set at the next generation, naming the cut-over's leader again, which fences the new set alone. A candidate may already hold that generation, and the flip may follow a stale report: if the leader promised that generation to a candidate since, the leader refuses the generation | pass, the same invariants, checked in the twenty pieces below: one run did not finish in six hours |
| `FelixShardElectSwapFlip{Keep,Drop}{0,1}{,Stood{One,Several}{Dest,Other}}.cfg` | `FelixShardElectSwapFlip.cfg` split at its cut-over as `FelixShardElectSwap.cfg` is: the new set keeps or drops the old leader, 0 or 1 writes were admitted before it, and no replica, one or several had promised past the control plane's generation, the destination among them or not. Each piece also checks everything before its cut-over, so together they reach every state the whole does | pass, the same invariants. On a four-core CI runner: `Keep1` 10.2M distinct, 14 min; `Drop1` 6.70M, 20 min; `Keep1StoodOneOther` 5.81M, 11 min; `Drop1StoodOneOther` 3.53M, 7 min; `Drop0StoodOneOther` 17.7M, 41 min; `Keep0StoodOneOther` 20.2M, 54 min; `Keep0` 35.4M, 1 h 9 min; `Drop0` 32.4M, 1 h 19 min; `Drop1StoodSeveralDest` 43.5M, 2 h 10 min; `Keep0StoodSeveralDest` 97.4M, 2 h 18 min; `Drop0StoodSeveralDest` and `Keep1StoodSeveralDest` over 2 h; the four `StoodSeveralOther` 694, under a second. Each `StoodOneDest` reaches the same states as its unstood twin, as the cut-over overwrites everything the destination's stand wrote. Those over 45 minutes and the four `StoodOneDest` run nightly. The four `StoodSeveralOther` pieces hold no cut-over, as in `FelixShardElectSwap.cfg`, and check that they stay empty (`NoCutOverInPiece`) |
| `FelixShardElectSwapFlipSkipsBallot.cfg` | the same with replicas answering the flip's fence by the generation alone, not the ballot (`NamedFenceSkipsBallot`) | violate `OneLeaderPerGeneration`: a candidate fences both sets without the leader and opens at the generation after the cut-over's; the flip names the leader there, and it opens too |
| `FelixShardElectSwapFlipJoinLags.cfg` | `FelixShardElectSwapFlip.cfg` where a copy joining in the cut-over holds any prefix of the destination's log (`JoinLags`), and the flip waits for the start record of the destination's named stint (`StintStarts`). Checked in the piece where the twin below finds its trace: the cut-over keeps the old leader, after the write, once several replicas had promised past the control plane's generation, the destination among them | pass, the same invariants (not yet measured) |
| `FelixShardElectSwapFlipJoinLagsFirstStart.cfg` | the same with the flip taking the generation's first start record instead of the named stint's (#1787) | violate `AckedOnMajority`: a destination elected past the old leader acknowledges a write on the old set, the cut-over names it again at that generation, the joining copy holds that stint's start but not the write, and the flip follows the start once a majority of both sets holds it, leaving the write on one copy of the new set |
| `FelixShardElectPreVote.cfg` | `FelixShardElect.cfg` with pre-vote and stickiness (`PreVote`), time to 2 so leases lapse and elections follow, two writes, the set check | pass, the same invariants (not yet measured) |
| `FelixShardElectLive.cfg` | three brokers with the control plane down (`CpDown`), no writes, no symmetry, up to four generations of elections, one broker (the leader or not) allowed to stop for good (`FailStops = 1`), a candidate whose own detector may be wrong (`DetectorMisfires`), a pre-vote the open leader refuses (`PreVoteAsked`), and backoff with at most two split rounds (`Backoff`, `Splits = 2`), under fairness (`LiveSpec`) | pass `ElectionSettles` and `NoStandAgainstHeardLeader` (3,000 distinct states, 1 s locally) |
| `FelixShardElectLiveLoosePreVote.cfg` | the same with a looser pre-vote: any live member that hears no leader says yes, the open leader included | violate `NoStandAgainstHeardLeader`: a follower whose detector misfires gets the leader's yes and its own and deposes a leader the rest of the set hears (#1815) |
| `FelixShardElectLiveNoBackoff.cfg` | the same without backoff | violate `ElectionSettles`: candidates split, stand again at once and split again until every generation the model allows is used, and nobody leads |
| `FelixShardElectLiveReissue.cfg` | `FelixShardElectLive.cfg` with the control plane up: promotion on the followers' word up to generation 1 (`Suspicions = 1`), adoption up to generation 3, two generations of elections, no broker stopping, and the control plane moving past a generation claimed twice (`Reissues`, CpReissue), with adoption and the reissue taken once they stay possible (`ReissueSpec`) | pass `CollisionSuperseded` (503K distinct states, 4 min locally) |
| `FelixShardElectLiveNoReissue.cfg` | the same without the reissue | violate `CollisionSuperseded`: placement names one broker at the generation another is elected at and opens, and the control plane keeps naming the one that cannot open |
| `FelixShardElectLiveUnclean.cfg` | `FelixShardElectLive.cfg` with storage identities and one broker that restarts uncleanly while following, under a new identity it votes with again, its process remembering the unclean start (`UncleanCrashes`): it answers no fence, stands for no copy and opens none that it may have led or gave up, and a ballot it wrote standing since is not that evidence (#2025) | pass `ElectionSettles` (29,000 distinct states, 3 s locally) |
| `FelixShardElectLiveUncleanOwnBallot.cfg` | the same with the rule before #2025: its own candidacy's ballot counts as having led (`OwnBallotLedBefore`) | violate `ElectionSettles`: the restarted broker stands and wins, its open is refused and the copy given up, and it then refuses the third broker's fence, so with the leader's broker stopped nobody is elected |
| `FelixShardElectLiveUncleanLed.cfg` | `FelixShardElectLiveUnclean.cfg` where the restart may hit a broker whose ballot names itself: the leader, a candidate still fencing, or one that stood (`UncleanLeads`), losing any suffix of its log; no broker stops for good, and detectors do not misfire | pass `ElectionSettles` (504,000 distinct states, 71 s locally; with misfires it passed 5.6 million states in 12 minutes unfinished) |
| `FelixShardElectUnclean.cfg` | safety of `FelixShardElect.cfg` with that restart, two writes, and a batch vouching for the copy only once it holds a record of its leader's generation (`VouchOnGeneration`, as the code does since #2029) | pass (11.1 million distinct states, 10 minutes locally) |
| `FelixShardElectUncleanVouchOnShip.cfg` | the same with the rule before #2029: any batch vouches | violate `AckedHeldByLeader`: the crashed leader gets back only its first lost record, stands, and opens with the empty third copy's vote without the second |
| `FelixShardElectUncleanForgetsLed.cfg` | the same with the restarted process not reading its own ballot as the copy it led (`UncleanForgetsLed`) | violate `AckedHeldByLeader` |
| `FelixShardCpLive.cfg` | three brokers with the control plane up, no writes, no symmetry, up to four generations, one broker allowed to stop and come back (`Restarts`), standings that may be withdrawn and keep their ballot (`Withdraws`), renames on a refusal (`Renames`), four fixes from the list of dead ends in #1914 on (`OverlayRefused`, `HaltReported`, `DeposeOnAssigned`, `StandBehindBallot`), a fence's step on the leader's own log reporting its refusal (`OwnBallotRefused`, #1919), and a leader that lost its generation to a newer one in its own log reporting it (`AboveReported`, #1951) with placement renaming it above the ballot it lost to (`RenamePastBallot`), under `CpLiveSpec` | pass (#1989, #2024); not registered: it runs for hours. A 5 h CI run timed out at 40M distinct states with no violation. The #2024 part is checked in `FelixShardCpLiveSkippedGeneration.cfg` |
| `FelixShardCpLiveSkippedGeneration.cfg` | `FelixShardCpLive.cfg` cut to the #2024 scenario: adoption stops at generation 3 (`Adopt`), where it goes to 4, and at most two brokers stop (`Stops`). A leader adopted at generation 3 whose log keeps its set from generation 1 restarts, and its fence learns the set before from the snapshot's `previous`. At generation 2 the case is out of reach | pass: the leader the control plane names ends up serving (`CpLeaderServes`); not registered: it runs past a pull request's hour, so the nightly exhaustive job checks it |
| `FelixShardCpLiveSkippedGenerationUnknownPrior.cfg` | the same with the snapshot allowed to lack the set before (`PriorMayBeUnknown`), as before #2024 | violate `CpLeaderServes`: the restarted leader whose log keeps its set two generations back never fences or reports (about 9 min on a laptop) |
| `FelixShardCpLiveMain.cfg` | the same with those fixes off, as main's code is | violate `CpLeaderServes`: a leader fencing at its assignment while its own ballot is above it never reports the refusal (`keep_own_set`, #1919) |
| `FelixShardCpLiveNoOwnBallotRefused.cfg` | `FelixShardCpLive.cfg` without `OwnBallotRefused` | violate `CpLeaderServes`: a leader fencing at its assignment with its own withdrawn standing above it never reports. **Not model-checked in CI**: it ran 2 h 53 min at 42.4M distinct states, then outgrew the six-hour limit on a CI runner; `FelixShardCpLiveMain.cfg` still shows the violation |
| `FelixShardCpLiveNoOverlayRefused.cfg` | without `OverlayRefused` | violate `CpLeaderServes`: the control plane's leader steps down when it answers a higher fence, every member ends promised to a candidate that withdrew, and nothing reports a refusal (1.59M distinct states, 2 min locally) |
| `FelixShardCpLiveNoHaltReported.cfg` | without `HaltReported` | violate `CpLeaderServes`: an open leader whose followers took a withdrawn candidate's higher fence halts on their refusals, and the halt is never reported (1.54M distinct states, 2 min locally) |
| `FelixShardCpLiveNoStandBehindBallot.cfg` | without `StandBehindBallot` | violate `CpLeaderServes`; not registered: no violation yet in 3.9M distinct states and 26 minutes in a pull request's CI |
| `FelixShardCpLiveNoAboveReported.cfg` | without `AboveReported`, as the code is | violate `CpLeaderServes` (#1951): the open leader answers a candidate's higher fence and stops serving, the candidate withdraws, and the leader reports nothing (895K distinct states, 3 min locally) |
| `FelixShardCpLiveReleased.cfg` | `FelixShardCpLive.cfg` cut down to the released leader: two stands, adoption up to 3, one stop, the first leader a fixed broker (`FirstLeaderFixed`), `SetCheck` on so the leader that answers a candidate's fence releases its generation (`bgen` 0) | pass `CpLeaderServes`: the released leader reports the generation it promised to a candidate that withdrew, and the rename past that ballot serves (`note_released` in driver/shards.rs). 5.7M distinct states, 15 min on a laptop |
| `FelixShardCpLiveReleasedNoAboveReported.cfg` | `FelixShardCpLiveReleased.cfg` with `AboveReported` off | violates `CpLeaderServes` in about a minute: the first leader comes back, stands, the named leader answers its fence and the candidate withdraws, and nothing reports. Shows the cut-down bound reaches the release |
| `FelixShardCpLiveReleasedFull.cfg` | the larger bound: every first leader, any number of stops, adoption up to 2 | expected to pass; runs past six hours on a CI runner, so the nightly job runs it |
| `FelixShardCpLiveRefusals.cfg` | the rules adopted for #2046 under `CpLiveSpec`: no restarts, generation 3, the leader starting on a failover's pins (`FailoverStart`), two copies changing one after the other and admitted again by a joint fence (`CopyPins`), a set lost after an open (`SetLossAfterOpen`), candidates standing above the control plane's generation from a route up to one generation old with at most two standings (`StandsAboveCp`, `StaleStandRoute`, `MaxStandings`); pins kept on a rename until the leader's first fenced open and dropped on admission, a promise away beside another copy refused, and lost-set and ahead refusals carrying their ballot | pass `CpLeaderServes`; not registered: no violation in 5.28M distinct states (15 min locally), unfinished |
| `FelixShardCpLiveRefusalsCopyExcluded.cfg` | the same keeping the other-copies exclusion (`OtherCopiesExcluded`) | violate `CpLeaderServes`: the #2046 stall, a renamed leader still on its failover's pins with one member promised to a withdrawn candidate and one answering from another copy (498K distinct states, 1 min locally) |
| `FelixShardCpLiveRefusalsPinsForever.cfg` | the same keeping the pins for good | violate `CpLeaderServes`: a copy changed while the leader fenced is re-admitted at a generation another member promised away, and its pin keeps it from counting (504K distinct states, 1 min locally) |
| `FelixShardCpLiveRefusalsUnknownNoBallot.cfg` | the same with a lost set's refusal carrying no ballot | not registered: no violation in 3.56M distinct states (10 min locally). The set is lost only after an open, which drops the pins, so the third member counts |
| `FelixShardElectRefusals.cfg` | the same rules for safety: one write, elections to generation 2 with no pre-vote, one failover on the followers' word (`Suspicions = 1`, `Promotion = "any"`), one copy changing and losing what it had not synced | pass `OneLeaderPerGeneration`, `AckedHeldByLeader`, `AckedAgree`, `NoTruncationBelowHwm`, `AdoptedMatchesOpened`, `SetChangesJoint` (2.37M distinct states, depth 29, 2 min on four workers) |
| `FelixShardElectRefusalsPinsCleared.cfg` | the same with a rename dropping the pins, as the code did | violate `AckedHeldByLeader`: b stands and withdraws, its copy changes and loses the write, the failover to c is refused on b's promise and a's silence, and c opens at 3 on b's empty copy. Keeping the other-copies exclusion fails the same way (143K distinct states, depth 13) |
| `FelixShardElectRenameMinorityDrop.cfg` | `FelixShardElectRename.cfg` with a joint fence refused on one promise away (`JointMinorityRefused`) and a rename naming the pending set alone (`RenameDropsChange`) | violate `OneLeaderPerGeneration`: c's joint fence is refused on a's promise to b and renamed on the new set alone, and the old set elects a at the same generation (41K distinct states, depth 12). `RenameDropsChange` alone fails the same way; `JointMinorityRefused` alone found nothing in 3.3M distinct states (4 min, unfinished) |
| `FelixShardCpDown.cfg` | `FelixShardCpLive.cfg` with the control plane stopping for good (`CpStops`), suspicion per broker and per shard (a broker named the leader included, `SelfSuspicion`), a pre-vote refusal bounded by the fence it waits on (`PreVoteBounded`), a promise freed once its candidate no longer stands and held at most a bound (`StandBehindBounded`), and a candidate withdrawing only once its fence cannot complete (`WithdrawsStuck`), under `CpDownSpec` | not registered: no violation of `CpDownServesInBound` in 56.8M distinct states and five hours in CI, with the queue still growing |
| `FelixShardCpDownNoSelfSuspicion.cfg` | the same without `SelfSuspicion`: a broker named the leader never stands for its shard, as in the code | violate `CpDownServesInBound`: a leader elected above its assignment restarts with the control plane down, its open is refused for its own ballot, and only it could win a pre-vote (2.5M distinct states, 12 min in CI) |
| `FelixShardCpDownBrokerSuspicion.cfg` | the same with suspicion per broker only, as the code has it | violate `CpDownServesInBound`: a restarted leader fences at a generation its followers promised past, and nobody suspects a live broker |
| `FelixShardCpDownPreVoteUnbounded.cfg` | the same with the pre-vote refused for as long as the asked broker fences | not registered: no violation in 49.6M distinct states and five hours in CI, with the queue still growing |
| `FelixShardCpDownStandBehindDeadOnly.cfg` | the same with a promise freed only once its candidate is down | not registered: no violation in 88.6M distinct states and five hours in CI. Before `SelfSuspicion` it violated `CpDownServesInBound` (the candidate restarts without its standing, and the replica that answered it never stands); with it, that broker stands for its own shard, and no counterexample has turned up |
| `FelixShardCpDownDegraded.cfg` | `FelixShardCpDown.cfg` held to `CpDownFullyBacked` | violate it: placement names one broker at the generation another was elected and opened at, then stops; the named one fences there for good (357K distinct states locally at three generations) |
| `FelixShardElectRouteLag.cfg` | three brokers with the control plane up, no writes, no symmetry, promotion on the followers' word up to generation 1, adoption up to generation 3, one generation of elections, one broker allowed to stop for good, renames on a refusal (`Renames`), and a promoted broker whose routes lag the ballot its promotion took (`RouteLags`), with both of #1936's rules, under fairness that includes the rename (`RenameSpec`) | pass `OwnBallotCaughtUp` (2.27M distinct states, 12 min in CI); with `NamedHereWaits` the stale round never stands, so this shows nothing else reaches the state |
| `FelixShardElectRouteLagNoFix.cfg` | the same without either rule (`NamedHereWaits`, `OwnBallotReported` off) | violate `OwnBallotCaughtUp` (about a minute with one worker; the trace varies with timing): the old leader stops, the control plane names another broker at 2, that broker's round on the old route stands at 3 and withdraws, and it opens at 2 below its own ballot, for good (#1933) |
| `FelixShardElectRouteLagReportOnly.cfg` | the same with the round still standing but the leader reporting its generation refused (`OwnBallotReported`) | pass `OwnBallotCaughtUp` (3.22M distinct states, 18 min in CI): the control plane names it again one generation up |
| `FelixShardElectRouteLagFenceNoFix.cfg` | `FelixShardElectRouteLagReportOnly.cfg` with a member allowed to answer the stale round's fence (`StaleFenceAnswered`) and the open checking the leader's own ballot (`OpenChecksOwnBallot`), with no report from a fencing leader | violate `OwnBallotFenceCaughtUp` (seconds): the old leader stops, the control plane names another broker at 2, its round on the old route stands at 3 while it fences, a follower answers its fence at 2, and the open never happens (#1940). The trace needs no answer to the stale round; with `OpenChecksOwnBallot` off and `StaleFenceAnswered` on the model also violates, through a member that answered it (the code reports that member's refusal, #1914) |
| `FelixShardElectRouteLagFence.cfg` | the same with a fencing leader below its own copy's promise reporting its generation refused (`OwnBallotFenceRefused`) | pass `OwnBallotCaughtUp` and `OwnBallotFenceCaughtUp` (2.36M distinct states, 19 min in CI): the control plane names it again one generation up |
| `FelixShardElectRouteLagReportStore.cfg` | `FelixShardElectRouteLagReportOnly.cfg` with the refusal carried in a report the control plane stores, the last one winning, and the open leader's pass reports landing over it, each repeating the refusal (`ReportStore`, `RefusalRepeats`) | pass `OwnBallotCaughtUp` (10.1M distinct states, depth 31, 16 min on a laptop) |
| `FelixShardElectRouteLagReportStoreNoRepeat.cfg` | the same with pass reports that do not repeat the refusal (`RefusalRepeats` off), as before #1971 | violate `OwnBallotCaughtUp` (1.0M distinct states, 1 min on a laptop): each refusal is replaced by the next pass report before the control plane renames, so the leader stays open below its own ballot (#1969) |
| `FelixShardElectRouteLagFenceReportStore.cfg` | `FelixShardElectRouteLagFence.cfg` with `ReportStore` | pass `OwnBallotCaughtUp` and `OwnBallotFenceCaughtUp` (8.51M distinct states, depth 31, 27 min on a laptop, the one ReportStore config over twenty minutes): a fencing leader runs no passes, so nothing replaces its refusal |
| `FelixShardElectNoBallotAcks.cfg` | `FelixShardElect.cfg` without ballots, two generations of elections and two writes, the set check on so the first leader can follow a candidate | violate `AckedHeldByLeader`, checked per generation |
| `FelixShardElectAdoptPromote.cfg` | `FelixShardElect.cfg` with one write, the set check, adoption up to generation 2 and promotion on suspicion up to generation 2 | pass, the same invariants (186K distinct states without the write; not yet measured with it) |
| `FelixShardElectReissue.cfg` | `FelixShardElectAdoptPromote.cfg` with promotion on suspicion up to generation 1 and the control plane moving past a generation it named one leader at while another was elected and opened there: the next generation, under the elected leader, on the control plane's set (`Reissues`, CpReissue) | pass, the same invariants (130,796 distinct states, 6 s locally) |
| `FelixShardElectReissueReplace.cfg` | `FelixShardElectReissue.cfg` with four brokers, a spare that promotion puts in place of the old leader (`ReplaceOnPromote`), no write, and adoption up to generation 2: the elected leader's set differs from the control plane's when they collide, so the reissue is a pending change from the elected set to the control plane's, and the reissued leader fences both (`ReissuePending`) | pass, the same invariants (66,576 distinct states, 4 s locally) |
| `FelixShardElectReissueReplaceBatchSet.cfg` | `FelixShardElectReissueReplace.cfg` with one write, adoption up to generation 4, candidates standing no lower than the kept set's generation (`StandsAboveKeptSet`), refusals that teach no set, and a follower keeping its own old set as the one being left when it learns a set from a batch (`LearnsLeavingSet` off) | violate `AdoptedMatchesOpened` first (#1929, as `FelixShardElectAdoptAboveKeptSet.cfg`); with that fixed, `AckedHeldByLeader`: a candidate fences no majority of the elected set and opens without the acknowledged write, so a broker takes the set being left only from the leader's fence or the control plane |
| `FelixShardSwapCounters.cfg` | `FelixShardElectSwapFlip.cfg` on a cache shard (`Counters`), one write, no elections: the cut-over waits for the destination's counter log, the counter mark needs both majorities while the change is pending (`CountersJoint`), and the flip waits for the counter log's start record (`FlipChecksCounters`) | pass the same invariants and `CountersOnSetMajority` (not yet measured; over 4M distinct states) |
| `FelixShardSwapCountersFlipUnchecked.cfg` | the same with the flip on the cache log's start alone | violate `CountersOnSetMajority`: an update acknowledged on {a, b}, the cut-over to b on {b, c, d}, and after the flip only b holds it (#1830) |
| `FelixShardSwapCountersNewSetOnly.cfg` | the same with the counter mark on the new set alone while the change is pending | violate `CountersOnSetMajority`: the update is acknowledged on a majority of the new set, and the old set the control plane still records holds it on one replica (#1830) |
| `FelixShardElectStepDown.cfg` | `FelixShardElect.cfg` with time to 2, `L = 2` and `Margin = 2` (so no promotion), one write and one generation of elections: an elected leader acknowledges a write and then steps down when its own lease lapses, leaving the control plane's leader at an older generation it has promised away | pass the same invariants (1.49M distinct states, under a minute). Found by `FelixShardWalkElect.cfg`: before `Newest` counted promises, `AckedHeldByLeader` took that deposed leader for the newest one |
| `FelixShardElectHandoff.cfg` | `FelixShardElect.cfg` with a planned move and a cancel, every change fenced (`FenceEveryChange`), the set check on, one generation of elections, one write, reports flowing so a move can start | pass `OneLeaderPerGeneration`, `AckedHeldByLeader`, `AckedAgree`, `AckedOnMajority`, `NoTruncationBelowHwm` (111.2M distinct states, 2 h 23 min on a four-core CI runner; nightly) |
| `FelixShardElectHandoffLeaders.cfg` | the same without the write, for every pull request | pass `OneLeaderPerGeneration` and the rest, which hold trivially without a write (5.05M distinct states, 5.9 min on a CI runner, with the set check's step-down) |
| `FelixShardElectHandoffUnfenced.cfg` | `FelixShardElectHandoffLeaders.cfg` with the cut-over opened without the fence, no writes and no cancel | violate `OneLeaderPerGeneration`: a candidate opens at the generation after the control plane's, and the control plane cuts over to the destination at that same generation |
| `FelixShardSuspicion.cfg` | `FelixShardFencedAck.cfg` with placement promoting at the first generation before the lease lapses (`Suspicions = 1`), on any read, as it does on the followers' word; promotion from the leader's report, ballots and every change fenced as the broker ships, time standing still, so no lease lapses in a run | pass `OneLeaderPerGeneration`, `AckedHeldByLeader`, `AckedAgree`, `AckedOnMajority`, `NoTruncationBelowHwm` (1.14M distinct states, 75 s on a CI runner, measured before ballots and the fence on every change were turned on, which add no states here). Not `AtMostOneServing`: the old leader serves until it hears of its successor, as under `FelixShardFencedAck.cfg` |
| `FelixShardSuspicionLease.cfg` | `FelixShardFencedPromotion.cfg` with the same early promotion: acknowledgements on the report and the lease, time to 4 | violate `AtMostOneServing`: the old leader still serves on its lease after the new one opens |
| `FelixShardSuspicionWipeUnbacked.cfg` | `FelixShardSuspicion.cfg` with one write and a follower that loses its data directory once, and with it the generation and ballot it promised (`LosePromises`, #1267); its log stays | violate `AckedHeldByLeader`: the follower took the successor's fence, loses it, and takes the deposed leader's write, which that leader acknowledges on the two of them; the successor opened without it |
| `FelixShardSuspicionWipeRoutes.cfg` | the same with the fix (`PromiseFromRoutes`): a follower with no promise on disk answers only what its routing view backs, which names the newest generation the control plane had issued or a later one | pass, the same invariants as `FelixShardSuspicion.cfg` (state count and time to be measured in CI) |
| `FelixShardElectWipe.cfg` | `FelixShardElect.cfg` with no write, the set check, adoption up to generation 2, and a follower that loses its ballot and its set once (`LosePromises`, #1331); no guard | violate `OneLeaderPerGeneration`: the wiped replica answers a second candidate at the generation it had promised |
| `FelixShardElectWipeWaits.cfg` | the same with one write and the guard as #1331 words it (`WipedWaitsForAdopt`): no candidate's fence until the control plane's generation is past its own at the wipe; no adoption, so the wait never ends | violate `AckedHeldByLeader`: the control plane's leader, deposed by an election it has not heard of, gets its write acknowledged on the wiped replica |
| `FelixShardElectWipeWaitsAll.cfg` | `FelixShardElectWipe.cfg` with the guard refusing every fence and batch while it waits | violate `OneLeaderPerGeneration`: adopting the election the replica took part in ends the wait |
| `FelixShardElectWipeWaitsSeeded.cfg` | the same with the ballot seeded from that bound (`PromiseFromRoutes`) | violate `OneLeaderPerGeneration`, the same way |
| `FelixShardElectWipeWaitsFloor.cfg` | `FelixShardElectWipeWaitsAll.cfg` with one write and a floor once the wait ends: nothing below the control plane's generation, read linearizably, and only its leader at it (`WipedAboveAdopted`) | pass `OneLeaderPerGeneration`, `AckedHeldByLeader`, `AckedAgree`, `AckedOnMajority`, `NoTruncationBelowHwm` (33.6K distinct states, 1 s) |
| `FelixShardElectWipeWaitsFloorDeep.cfg` | the same with elections up to generation 3, no write | violate `OneLeaderPerGeneration` |
| `FelixShardElectWipeOracle.cfg` | `FelixShardElectWipeWaitsFloor.cfg` at generation 3, waiting for a bound no broker can know, the newest generation the replica had promised (`WipeBoundOracle`) | pass, the same invariants (5.45M distinct states, depth 30, 1 min 45 s on eight workers) |
| `FelixShardElectWipeOracleBatches.cfg` | the same taking batches from an unadopted leader while it waits (`WipedTakesBatches`) | violate `AckedHeldByLeader` |
| `FelixShardElectWipeOracleLearnsSet.cfg` | `FelixShardElectWipeOracle.cfg` with pre-vote and stickiness, and the waiting replica learning a candidate's set without answering it (`WipedLearnsSet`) | pass, the same invariants (83K distinct states, 2 s) |
| `FelixShardElectWipeNonVoter.cfg` | `FelixShardElectWipe.cfg` with one write, generation 3, and the adopted rule (`WipedNonVoter`): the wipe empties the log, the replica is a non-voter until a re-admission whose joint fence counts the old set's majority without it, and votes again only once it holds the leader's log through the start record there; one follower wiped once, no time | pass `OneLeaderPerGeneration`, `AckedHeldByLeader`, `AckedAgree`, `AckedOnMajority`, `NoTruncationBelowHwm`, `AdoptedMatchesOpened`, `SetChangesJoint` (20.4M distinct states, depth 33, 15 min on four workers) |
| `FelixShardElectWipeNonVoterNoCatchUp.cfg` | the same, re-admitted as soon as the confirmed leader is open at its set's generation or a later one (`ReadmitBeforeCatchUp`) | violate `OneLeaderPerGeneration`: the re-admitted replica took no ballot from that leader, and promises its generation, which it had promised before the wipe, to a second one |
| `FelixShardElectWipeNonVoterCountsWiped.cfg` | the same, no write, with the re-admission counting the wiped replica (`ReadmitCountsWiped`) | violate `OneLeaderPerGeneration` |
| `FelixShardElectWipeNonVoterWipesLeader.cfg` | `FelixShardElectWipeNonVoter.cfg` where a leader or candidate can be wiped (`WipeLeads`) | pass, the same invariants (19.3M distinct states, depth 35, 8 min on seven workers; runs nightly, not per PR, see below) |
| `FelixShardElectWipeNonVoterTwice.cfg` | `FelixShardElectWipeNonVoter.cfg` with two wipes (`Forgets`), of the same replica again or of a second once the first is re-admitted; `OneNonVoter` keeps at most one non-voter at a time, since two distinct wiped brokers exceed what three replicas tolerate | pass, the same invariants (29.8M distinct states, depth 38, 12 min on eight workers; runs nightly, not per PR, see below) |
| `FelixShardElectWipeNonVoterFailover.cfg` | `FelixShardElectWipeNonVoter.cfg` with a one-tick lease and time to 1, so control-plane failover is reachable, and adoption up to generation 2 (at 3 it passed 136M distinct states unfinished); failover never names a non-voter | pass, the same invariants (46.9M distinct states, depth 37, 10 min on twelve workers; runs nightly, not per PR, see below) |
| `FelixShardElectWipeNonVoterFailoverWipesLeader.cfg` | the same where a leader or candidate can be wiped (`WipeLeads`) | pass, the same invariants (53.2M distinct states, depth 37, 11 min on twelve workers; runs nightly, not per PR, see below) |
| `FelixShardElectWipeNonVoterPromotes.cfg` | `FelixShardElectWipeNonVoterFailover.cfg` with failover free to name a non-voter (`PromoteNonVoter`) | violate `OneLeaderPerGeneration`: the non-voter forgot its promise to a candidate and is promoted at that generation |
| `FelixShardElectWipeNonVoterDropsLearners.cfg` | `FelixShardElectWipeNonVoterFailover.cfg` with placement writes that may name no learners (`WriteDropsLearners`), as a write does whose read of the assignment it replaces failed, or one from a control plane that predates learners on Postgres | violate `PlacementKnowsLearners`: a follower is wiped and held a learner, then a placement write drops it while it is still a non-voter (127 distinct states, depth 5) |
| `FelixShardElectWipeNonVoterCreates.cfg` | `FelixShardElectWipeNonVoter.cfg` starting as the shard is created (`Creates`): the first leader fences at generation 1, no follower holds a promise or a set, and a follower not wiped votes with an empty log; adoption up to generation 3, with no broker promising 3 before the re-admission (`ThreeOnceReadmitted`; unbounded at 3 it passed 137M distinct states unfinished in two hours) | pass, the same invariants and `PlacementKnowsLearners` (2.14M distinct states, depth 35, 2 min on four workers) |
| `FelixShardElectWipeNonVoterCreatesLearnerAcks.cfg` | the same, the leader counting a non-voter's catch-up answers (`LearnerAcksCount`, on by default only without elections; the code's leader leaves a learner out) | violate `AckedHeldByLeader`: c promises candidate b generation 2, is wiped, catches up from a at 1, and a acknowledges on {a, c}; b opens on {b, c} without the write. The code's leader leaves a learner out of its marks, so this is the twin that counts it |
| `FelixShardElectWipeNonVoterCreateCountsEmpty.cfg` | the same, where a copy with no set file votes, wiped or not (`CreateCountsEmpty`) | violate `OneLeaderPerGeneration`: the wiped copy forgot its promise of generation 2 and gives it to a second leader |
| `FelixShardElectWipeNonVoterCreatesOpens.cfg` | `FelixShardElectWipeNonVoterCreates.cfg` with no write, checking a witness | violate `NoCreationBesideNonVoter`: the new shard opens while a copy wiped before it answered is a non-voter |
| `FelixShardElectWipeNonVoterEvidenceOnly.cfg` | `FelixShardElectWipeNonVoterFailover.cfg` with only the evidence the code sees in a set (`ReplacedIdLearner` off): a copy is a learner once a set names its old storage id | violate `OneLeaderPerGeneration`: a follower promises 2 to a candidate and is wiped before it hears a set naming its old id; it answers the fence of a failover at 2 that names its new one, with no promise and an empty log |
| `FelixShardElectWipeNonVoterSetless.cfg` | the rule as the code runs it before elections: only the control plane names leaders (`Elections = 0`), with a one-tick lease and time to 1 so it fails over and re-admits, a cache shard whose counter log the wipe takes too (`Counters`), and fences without a set from the control plane's leader (`SetlessFences`) | pass `OneLeaderPerGeneration`, `AckedHeldByLeader`, `AckedAgree`, `AckedOnMajority`, `NoTruncationBelowHwm`, `AdoptedMatchesOpened`, `SetChangesJoint`, `CountersHeldByLeader`, `CountersOnMajority` (over 41M distinct states unfinished after 15 min on eight workers; runs nightly, not per PR, see below) |
| `FelixShardElectWipeNonVoterSetlessUnmarked.cfg` | the same, a fence without a set refused only on a mark kept in a set file (`SetlessSeesMarker` off) | violate `AckedHeldByLeader`: the write is acknowledged by {b, c}, c is wiped, the failover's fence reaches c without a set, and c answers with an empty log |
| `FelixShardElectWipeNonVoterCountersCountLearner.cfg` | the same, the counter fence answered by a non-voter (`CounterFenceSeesLearner` off) | violate `CountersHeldByLeader`: the failover's counter fence counts the wiped copy's empty counter log |
| `FelixShardElectWipeNonVoterCountersUnchecked.cfg` | the same, re-admitted once the cache log holds the start record, the counter log not checked (`ReadmitChecksCounters` off) | violate `CountersHeldByLeader` (`CountersOnMajority` left out, as it breaks at the re-admission itself) |
| `FelixShardElectWipeNonVoterGroups.cfg` | `FelixShardElectWipeNonVoterSetless.cfg` without fences that carry no set, the counter log standing for a stream's group logs (`GroupLogs`): re-admission does not wait for them, and the copy stays marked wiped until its group log holds a record of its set's generation, refusing group fences and, as a leader, not counting its own | pass, the same invariants (no violation through 33.6M distinct states, depth 24, 17 min on six workers, unfinished; runs nightly, not per PR, see below) |
| `FelixShardElectWipeNonVoterGroupsKeepDropsMark.cfg` | the same, the promoted leader's set write dropping its own mark (`KeepSetDropsMark`), as `keep_own_set` did | violate `CountersHeldByLeader`: a group record acknowledged on {a, c}; c wiped and re-admitted with its group log behind, promoted, drops its mark keeping its set, and opens on its own group log and b's, which lags |
| `FelixShardElectWipeNonVoterIdlessCreation.cfg` | the same rule on a stream shard without counters, its creation set naming no storage ids (`IdlessCreation`), as a shard created before sets named member ids keeps | pass, the same invariants (4.68M distinct states, depth 35, 2 min on six workers) |
| `FelixShardElectWipeNonVoterIdlessCreationExempt.cfg` | the same, a copy that holds no set taking any generation-1 set as a voter (`GenOneExempt`), as `replaced_copy_of` did | violate `AckedHeldByLeader`: c is wiped at generation 1, learns the id-less creation set as a voter, and a failover's lagging leader opens on {b, c} |
| `FelixShardElectUncleanContinuing.cfg` | the wiped-replica rule at generation 2 with two wipes and `WipeLeads`, a continuing open (the leader that served the generation before, same process, not draining) taking no catch-up and counting an answer from the copy it expects or one attesting a kept ballot that names it there, trusting a doubted copy with that ballot (`ContinuingTrustsBallot`) that never lost its promises (`ContinuingKeptBallot`), and a copy that lost them answering anyway (`ContinuingForgottenAnswers`), as a restored directory does; the copy promises and stays doubted, and the one that lost its promises is not counted (#2047). No configuration shows a doubted copy served by the rule: every non-voter here was wiped, and a short copy stops being one once it promises a newer leader | pass |
| `FelixShardElectUncleanContinuingBatchBallot.cfg` | the same without `ContinuingKeptBallot`: a wiped copy's ballot can come from a batch it took after the wipe | violate `OneLeaderPerGeneration`: b opens at 2 and is wiped; it takes a's batch at 1; a's re-admission of c at 2 counts b |
| `FelixShardElectUncleanContinuingAnyCopy.cfg` | the same trusting any doubted copy, ballot or not (`ContinuingTrustsAnyCopy`) | violate `OneLeaderPerGeneration`: c promised b at 2 and was wiped; a's re-admission of b at 2 counts c |
| `FelixShardElectUncleanContinuingCountsForgotten.cfg` | the same counting the answer of a copy that lost its promises as one from the copy it expects (`ContinuingCountsForgotten`), as the code did before the kept-ballot attestation | violate `OneLeaderPerGeneration`: b stands at 2, c promises it, b opens at 2; b is wiped; a, still the control plane's leader at 1, re-admits b at 2; c is wiped, losing its promise to b, answers a's fence, and a counts it as the copy it expects and opens at 2 beside b |
| `FelixShardElectUncleanContinuingRetry.cfg` | `FelixShardElectUncleanContinuing.cfg` with the continuing fence retried once after its first answers came in, as a lost answer or a Pending round restarts it (`ContinuingRetries`), and every follower answering from a new copy so only attested answers count (`ContinuingCopiesNew`); the retry finds the ballot at the fence's own generation, which the attestation takes | violate `RetriedContinuingNeverOpens`, a canary: c answers a's fence at 2, a retries, c answers again from a ballot at 2 and a opens |
| `FelixShardElectUncleanContinuingRetryBelowOnly.cfg` | the same attesting only a ballot at the generation before (`ContinuingAttestsBelowOnly`) | pass: the retry's answers are never counted, so a retried fence never opens |
| `FelixShardElectUncleanContinuingRetrySafe.cfg` | the retried continuing fence without its canary, every safety invariant on | pass: no two leaders at one generation, and the leader keeps every acknowledged record |
| `FelixShardElectUncleanContinuingDoubted.cfg` | `FelixShardElectUncleanContinuingRetrySafe.cfg` with a broker that restarts uncleanly, adoption to generation 3, one wipe and no client write, so a doubted copy answers a continuing open's fence (`ContinuingTrusts`), every safety invariant on | pass: no two leaders at one generation (19.6M distinct states, 47 min in CI); runs exhaustively in every `formal` job. With no write the record invariants hold only trivially; `FelixShardElectUncleanContinuingDoubtedFull.cfg` covers them |
| `FelixShardElectUncleanContinuingDoubtedFull.cfg` | the same with one client write | pass: the leader keeps every acknowledged record. **Not exhaustive**: it did not finish in six hours (178M distinct states and still growing), so the nightly random walk (`tla-walk.yml`, family `unclean-continuing`) samples it; a pass means nothing was found |
| `FelixShardElectWipeNonVoterNoCatchUpFailover.cfg` | `FelixShardElectWipeNonVoterNoCatchUp.cfg`'s fault with `Elections = 0`, no counters and no fence without a set | violate `AckedHeldByLeader`: re-admitted with an empty log, the copy answers the next failover's fence |
| `FelixShardElectWipeNonVoterEvidenceOnlyFailover.cfg` | `FelixShardElectWipeNonVoterEvidenceOnly.cfg`'s fault with `Elections = 0`, no counters and no fence without a set | violate `AckedHeldByLeader`: wiped before it heard any set, the copy answers a failover's fence that names its new id |
| `FelixShardElectWipeNonVoterCreatesReadmits.cfg` | `FelixShardElectWipeNonVoterCreates.cfg` at the same bound, checking a witness | violate `NoFailoverAfterReadmit`: the wiped copy is re-admitted, the write is acknowledged at the re-admission's generation, and a leader opens above it |
| `FelixShardElectWipeNonVoterElects.cfg` | `FelixShardElectWipeNonVoter.cfg`, checking a witness | violate `NoLeaderBesideNonVoter`: a leader at a generation taken after the wipe opens without the non-voter |
| `FelixShardElectWipeNonVoterReadmits.cfg` | the same | violate `NeverReadmitted`: the re-admission completes after the catch-up |
| `FelixShardElectWipeNonVoterFailsOver.cfg` | `FelixShardElectWipeNonVoterFailover.cfg`, checking a witness | violate `NoFailoverBesideNonVoter`: the control plane fails over while a wiped replica is a non-voter |
| `FelixShardFencedAckAnyKept.cfg` | `FelixShardFencedAck.cfg` with a spare fourth broker outside the replica set (`Spares`), promotion of any replica however far behind (`Promotion = "any"`), and the promotion keeping the replica set, the old leader in it; no drift | pass `AckedHeldByLeader`, `AckedAgree`, `AckedOnMajority`, `NoTruncationBelowHwm` (0.48M distinct states, depth 31, 27 s on four workers; with `Drift = 1`, 39.0M distinct states in 35 min on four workers, by hand) |
| `FelixShardFencedAckAnyReplaced.cfg` | the same with the promotion swapping the old leader for the spare (`ReplaceOnPromote`), as failover's `choose_replicas` would | violate `AckedHeldByLeader`: the new leader and the spare are a majority of the new set and open without the record the old leader and the third replica acknowledged |
| `FelixShardReplacedPendingLeft.cfg` | `FelixShardFencedAckAnyReplaced.cfg` with the joint fence (`SetCheck`, `JointFence`) and promotion on suspicion up to generation 2 (`Suspicions = 2`) | violate `AckedHeldByLeader` (#1878): b is promoted at 2 on {b, c, d}, and its joint fence, which also covers {a, b, c}, never opens; c is promoted at 3 on {a, c, d} and fences that set and {b, c, d} but not {a, b, c}, so c and d open without the write a and b acknowledged at 1. A failover drops the set an unopened failover was still leaving, in the model's `Promote` and in `set_before` alike |
| `FelixShardReplacedPendingLeftKeptLog.cfg` | `FelixShardReplacedPendingLeftKept.cfg` with each member keeping the sets it leaves as the broker's set file does (`LogDropsSets`, #1881): a fence or a batch that raises the set carries the sets it was sent (a batch, the control plane's set before its generation, as `check_batch_set` takes it) plus, unless the log holds a record of its own set's generation, that set and the ones it kept. Earlier sets are kept only under `JointFence`; the file's cap of 16 earlier sets (`KeptSetsCap`) cannot be reached with four brokers | pass `AckedHeldByLeader`, `LeftRecorded`, `AckedAgree`, `NoTruncationBelowHwm` (not finished: no violation in 6 h, 129M distinct states at depth 31) |
| `FelixShardReplacedPendingLeftTwiceAllLog.cfg` | `FelixShardReplacedPendingLeftTwiceAll.cfg` with the same rule (`LogDropsSets`) | violate `AckedHeldByLeader` (15 states deep, 10.9M distinct states): b opens at 2 on {b, c, d} and write 1 is acknowledged on b and d; d is promoted at 3 on {a, c, d} and never opens; a, never asked by d, is promoted at 4 on {a, b, c} and fences that set and {a, c, d} but not {b, c, d}. Not registered until the Report variant says whether placement can promote a |
| `FelixShardReplacedPendingLeftKept.cfg` | `FelixShardReplacedPendingLeft.cfg` keeping the set an unopened failover leaves (`KeepsUnopened`, #1880) and fencing the recorded set (`FenceRecordedSet`), time standing still | violate `AckedHeldByLeader` (17 deep, 2 min): a member taking a batch at the generation it knows drops the sets it keeps, which the broker never does (`check_set`); the Log variant is the faithful check |
| `FelixShardReplacedPendingLeftTwice.cfg` | `FelixShardReplacedPendingLeftKept.cfg` up to generation 3 with one kept set (`OneLeavingSet`) | violate `AckedHeldByLeader` (#1881, 20 min) |
| `FelixShardReplacedPendingLeftTwiceAll.cfg` | `FelixShardReplacedPendingLeftTwice.cfg` keeping every set (#1881's fix) | violate `AckedHeldByLeader` (16 deep, 11 min), with the coarse batch rule of the Kept row |
| `FelixShardReplacedPendingLeftTwiceAllLogReport.cfg` | `FelixShardReplacedPendingLeftTwiceAllLog.cfg` with leader-report placement and no follower named short of the log the leader inherited (`ReportFloor`, as the broker does since #1924; without it an acknowledged write is lost at depth 22, #1922) | did not finish in 6 h: 167 million distinct states, 75 million still queued, no violation (run 42054530207) |
| `FelixShardReplacedPendingLeftTwiceAllLogReportNoFloor.cfg` | `FelixShardReplacedPendingLeftTwiceAllLogReport.cfg` without `ReportFloor`, as `driver/shard.rs` was before #1924 | violates `AckedHeldByLeader` (22 deep, 42 min, #1922) |
| `FelixShardReplacedPendingLeftKeptLogBatchDrops.cfg` | `FelixShardReplacedPendingLeftKeptLog.cfg` with a batch that raises the set keeping none (`BatchDropsSets`), the rule before review | violate `AckedHeldByLeader` (17 deep, 5 min) |
| `FelixShardReplacedPendingLeftTwiceAllLogBatchDrops.cfg` | `FelixShardReplacedPendingLeftTwiceAllLog.cfg` with `BatchDropsSets` | violate `AckedHeldByLeader` (16 deep, 19 min) |
| `FelixShardReplacedSetUnknown.cfg` | `FelixShardReplacedPendingLeft.cfg` with leader-report placement, and a leader that may lose the set its generation leaves (`SetLossUpTo`, #1872): it learned the shard from a snapshot, or restarted after it opened. It is named again one up (`CpRename`) only once it opened that generation, and a failover keeps an unopened set (`KeepsUnopened`, #1880) and fences the control plane's set from the generation before beside its own (`FenceRecordedSet`, as `set_before` does). Without `KeepsUnopened`, b opens at 2 on c's answer alone, its report names c caught up at 0 as the broker's does, and c, promoted at 3, never fences {a, b, c} (#1878) | pass `AckedHeldByLeader`, `AckedAgree`, `NoTruncationBelowHwm` (not finished: no violation in 6 h, 142M distinct states at depth 21) |
| `FelixShardReplacedSetUnknownAny.cfg` | the same renaming whenever the set is unknown (`UnknownRenamesAny`), the rule the fix replaced | violate `AckedHeldByLeader` (13 states deep): c is promoted at 2 on {b, c, d} and loses the set it leaves; it is named again at 3, where the fence leaves {b, c, d} alone, and opens with d without the write a and b acknowledged at 1 |
| `FelixShardReplacedSetUnknownRenames.cfg` | a witness: `FelixShardReplacedSetUnknown.cfg` checking that no leader past generation 1 is due a rename after losing its set in a generation it opened (`NeverRenameUnknownSet`) | violate `NeverRenameUnknownSet` at depth 8, so the passing configuration reaches the rename the fix allows |
| `FelixShardReplacedSetUnknownFirstRenames.cfg` | a witness: the same checking that no leader is due a rename after losing its set at the first generation, which it opened (`NeverRenameFirstUnknownSet`). The broker renames there on its own ballot with no kept set (`NoSetBefore::Unknown(0)`) | violate `NeverRenameFirstUnknownSet` at depth 4, so the passing configuration reaches that rename |
| `FelixShardReplacedSetUnknownMark.cfg` | `FelixShardReplacedSetUnknown.cfg` with the first-assignment mark (`FirstAssignMark`): a leader that loses its set at the first generation reads the set before as empty, which the control plane marks only when its change log holds the shard's `assigned` change at that generation | pass, the same invariants (not finished: no violation in 6 h, 139M distinct states at depth 20) |
| `FelixShardReplacedSetUnknownMarkAny.cfg` | the same trusting the mark at any generation (`MarkAnyGeneration`) | violate `AckedHeldByLeader` (12 states deep): c is promoted at 2 on {b, c, d}, learns the shard from a snapshot while fencing, reads the set before as empty and opens with d without the write a and b acknowledged at 1 |
| `FelixShardReplacedSetUnknownMarkRenames.cfg` | a witness for `FelixShardReplacedSetUnknownMark.cfg`, as `FelixShardReplacedSetUnknownRenames.cfg` is without the mark | violate `NeverRenameUnknownSet` at depth 9: the rename stays reachable with the mark |
| `FelixShardFencedAckSeat.cfg` | `FelixShardFencedAckAnyKept.cfg` with one follower replacement (`MaxMoves = 1`): a spare joins beside a leaving follower at one generation, counting toward the quorum, and the leaving one goes at the next, once the newcomer holds what a majority of the set held when it joined (`SeatHoldsCopy`); one write, `L = 2`, time to 3, no start records | pass `AckedHeldByLeader`, `AckedAgree`, `AckedOnMajority`, `NoTruncationBelowHwm` (6.93M distinct states, depth 35, 15 min on a four-core CI runner; a failover that names the joining copy leaves four in the set, which `TrimFence` and `TrimSet` then trim) |
| `FelixShardFencedAckSeatLonger.cfg` | the same with start records, `L = 4` and time to 4 | pass, the same invariants (34.9M distinct states, depth 37, 36 min on a four-core CI runner; nightly) |
| `FelixShardFencedAckSeatEarly.cfg` | the same with the seat not waiting, as placement did when a report named the newcomer caught up before anything was counted at the joining generation | violate `AckedHeldByLeader`: a record acknowledged on the leader and the leaving follower, the newcomer seated without it, and the next leader fencing the newcomer and the lagging follower |
| `FelixShardFencedAckGrow.cfg` | `FelixShardFencedAckSeat.cfg` with a set a failover left short of the replication factor: two brokers hold the shard and the third is a spare, and `Grow` lets placement add the spare with nobody leaving (`GrowSet`), then seat it, as a restore does. A copy joining the even set counts toward every majority that acknowledged, so a failover keeps it (`Counted`), and a follower is replaced only beside an odd set; one move, one write, `L = 2`, time to 3 | pass `AckedHeldByLeader`, `AckedAgree`, `AckedOnMajority`, `NoTruncationBelowHwm` (604K distinct states, depth 25, 21 s on a CI runner) |
| `FelixShardFencedAckSeatPair.cfg` | `FelixShardFencedAckGrow.cfg` without `Grow`: a replication factor of two, so replacing b seats the spare c into `{a, c}`, and up to the seat a record can be acknowledged on a and b alone. The seat fences a first (`SeatFence`) and seats c only once a's drained report has c holding its whole log (`seat_or_fence`, #1616). A failover may keep b (`SeatDropped`, as `left_unconfirmed` does while the leader has not reported at the seat's generation), and a set left above the factor is fenced (`TrimFence`), then drops a follower once the leader reports drained and every other follower holds its log (`TrimSet`); one move, one write, `L = 2`, time to 3 | pass `AckedHeldByLeader`, `AckedAgree`, `AckedOnMajority`, `NoTruncationBelowHwm` (9.3M distinct states, depth 37, 12 min on a four-core CI runner, so out of the quick tier). Without the fence TLC finds a record written before c joins reaching b after, acknowledged on a and b, and c seated without it, so `{a, c}` holds it on a alone (`AckedOnMajority`) |
| `FelixShardFencedAckUndoPair.cfg` | `FelixShardFencedAckSeatPair.cfg` where placement may also give the replacement up (`Undoes`). c joins `{a, b}` and two of the three acknowledge, so a record can be on a and c alone after any report. The undo fences a first (`UndoFence`) and drops c only once a's drained report has b holding its whole log (`Undo`, `undo_or_fence` and `seat_after_fence`, #1628); the fenced leader may seat c instead. The model is RF 2 only, where the leader and b are the whole set left, so the code's rule for larger sets (the leader and the followers at its tail a majority of the set left, a lost follower not needed) is not checked here; unit tests in `placement/tests/restore.rs` cover it | pass `AckedHeldByLeader`, `AckedAgree`, `AckedOnMajority`, `NoTruncationBelowHwm` (11.1M distinct states, depth 37, 10.5 min locally on 4 workers, so out of the quick tier). Coverage shows the fenced `Undo` branch taken |
| `FelixShardFencedAckUndoPairUnfenced.cfg` | `FelixShardFencedAckUndoPair.cfg` with the undo made while the leader serves (`UndoUnfenced`), as placement made it before #1628 | violates `AckedOnMajority`: a record acknowledged on a and c alone, c dropped, `{a, b}` holding it on a only (depth 9) |
| `FelixShardFencedAckTrim.cfg` | `FelixShardFencedAckSeatPair.cfg` without the seat: `{a, b, c}` hold the shard and the replication factor is two (`LoweredFactor`), nothing is replaced (`MaxMoves = 0`), so the seat's window cannot open. The set is fenced and trimmed (`TrimFence`, `TrimSet`), judged from the leader's reports as `trim_fence` and `trim` judge, and a failover may keep the dropped copy (`SeatDropped`) and leave the set above the factor again | pass `AckedHeldByLeader`, `AckedAgree`, `AckedOnMajority`, `NoTruncationBelowHwm` (4.0M distinct states, depth 38, 7 to 11 min on a CI runner) |
| `FelixShardFencedAckMoveShort.cfg` | `FelixShardFencedAck.cfg`'s acknowledgement with a planned move toward a follower: fence, drained report, cut-over without the fence (`FenceEveryChange = FALSE`); a failover during the move never names the destination (`PromoteDestination = FALSE`); report-read promotion, start records, one write, `L = 1`, time to 1 | pass `AckedHeldByLeader`, `AckedAgree`, `AckedOnMajority`, `NoTruncationBelowHwm` (32.7M distinct states, depth 43, 22 min on a CI runner) |
| `FelixShardFencedAckStagedMoveShort.cfg` | the same from a staged move: two replicas and a destination copying outside the quorum | pass, the same invariants (25.2M distinct states, depth 43, 20 min) |
| `FelixShardFencedAckMoveCancelShort.cfg` | the move with the operator's cancel on: the fenced leader takes the shard back without the fence | pass, the same invariants (42.2M distinct states, depth 43, 35 min) |
| `FelixShardFencedAckMove.cfg`, `FelixShardFencedAckStagedMove.cfg`, `FelixShardFencedAckMoveCancel.cfg` | the three above with `L = 2` and time to 2 | pass, the same invariants: 66.0M distinct states, depth 44, 58 min, and 54.7M, depth 44, 31 min, on a CI runner; the cancel had not finished at 54M after 25 min. Nightly |
| `FelixShardFencedAckMoveDestination.cfg` | the move with a failover free to name the destination, as placement did | violate `AckedHeldByLeader`: the destination opens as if cut over, without the fence and without a record the old leader acknowledged after its last report |
| `FelixShardFencedAckStagedMoveDestination.cfg` | the same from a staged move | violate `AckedHeldByLeader` |
| `FelixShardReadsRound.cfg` | `FelixShardReads.tla`: `FelixShardFencedAck.cfg`'s writes with one read, confirmed by a round of fences at the leader's generation after it takes its value (`ReadConfirm = "round"`); `L = 2`, time to 2, one write | pass `NoStaleRead`, `AckedHeldByLeader`, `AckedOnMajority` (22.1M distinct states, depth 27, 6 min on four workers) |
| `FelixShardReadsNoRound.cfg` | the same with the round skipped | violate `NoStaleRead` |
| `FelixShardReadsLease.cfg` | the same with the lease in place of the round | violate `NoStaleRead` |
| `FelixShardSessionsSubscriber.cfg` | `FelixShardSessions.tla`: `FelixShardReadsRound.cfg`'s writes and clocks with a subscriber that reads up to the committed mark of any broker that believes it leads, and resumes at its next offset when it moves; no lease on its path | pass `NoLostDelivery` (1.46M distinct states, depth 25, 58 s) |
| `FelixShardSessionsPastMark.cfg` | the same with the subscriber reading to the end of the broker's log | violate `NoLostDelivery` (18.0K distinct states, depth 10) |
| `FelixShardSessionsGroupRound.cfg` | the same writes with one group commit, written to the coordinator's log and confirmed by a round of fences at its generation before it is acknowledged | pass `NoStaleGroupCommit` (13.9M distinct states, depth 27, 14 min) |
| `FelixShardSessionsGroupNoRound.cfg` | the same with the round skipped | violate `NoStaleGroupCommit` (5.7K distinct states, depth 8) |
| `FelixShardSessionsGroupLease.cfg` | the same with the lease in place of the round | violate `NoStaleGroupCommit` (6.4K distinct states, depth 8) |
| `FelixShardFigure8FollowerAcks.cfg` | `FelixShardFencedAck.cfg`'s acknowledgement from the seeded Figure 8 history, promotion still reading the report | pass every invariant it checks (7.67M distinct states, depth 36, 58 s on sixteen cores) |
| `FelixShardFigure8FollowerAcksNoStartRecord.cfg` | the same without the start record: follower acks count a record the leader inherited | violate `AckedOnMajority` |
| `FelixShardFigure8AckWait.cfg` | `FelixShardFigure8FollowerAcks.cfg` with the clients of x and y still waiting on the brokers that wrote them (`AckWaits`). A wait follows its broker into any later generation it leads, and is answered only if the log still holds a record of the write's generation at the write's offset (`still_holds`, quorum.rs) | pass every invariant it checks (6.68M distinct states, depth 36, 3 min on six workers) |
| `FelixShardFigure8AckWaitByOffset.cfg` | the same with a wait answered by its offset alone (`AckByOffset`), as before #2048 | violate `AckedHeldByLeader` (depth 9, under a second): a is promoted at 3, its fence takes b's log, whose y sits at x's offset, and a's mark at 3 acknowledges x |
| `FelixShardFigure8.cfg` | the broker as built (`FelixShardFencedPromotion.cfg`) with the generation start record (`StartRecord`), started from a history two leaderships in (`FelixShardFigure8.tla`) | pass every invariant it checks (5.99M distinct states, depth 39, 35 s on sixteen cores) |
| `FelixShardFigure8NoStartRecord.cfg` | the same without the start record: the mark counts records the leader inherited | violate `AckedOnMajority`: Raft's Figure 8, see below |
| `FelixShardFigure8CutOver.cfg` | the start record, from a history one leadership further (`SeededCutOverInit`): the next leader comes from a move's cut-over or a cancelled move's hand-back | pass every invariant it checks (12.9M distinct states, depth 39, 90 s) |
| `FelixShardFigure8CutOverNoStartRecord.cfg` | the same without the start record, and without the cancel, so the trace goes through the cut-over | violate `AckedOnMajority`: Figure 8 through a move, see below |
| `FelixShardFencedPromotion.cfg` | the broker as built: `FelixShardRealMargins.cfg` with the promotion fence and its catch-up, acknowledgements still on the report and the lease | pass every invariant and `AckedHeldByLeader` (8.15M distinct states, depth 30, 55 s on sixteen cores) |
| `FelixShardNoCommitCheck.cfg` | commit-time lease check removed | violate `NoStaleCommit` |
| `FelixShardNoReportOrder.cfg` | the design *before* #268: a `Quorum` ack released before the report describing it lands | violate `AckedSurvive` |
| `FelixShard.cfg` | the design as implemented: report-before-mark, followers reported against the offset a majority holds, promotion from the leader's report, the leader acting on the answer its report got | pass every invariant (5.38M distinct states) |
| `FelixShardReportAtTail.cfg` | the same with followers reported only when level with the leader's tail | violate `QuorumReportNamesASuccessor` |
| `FelixShardReportUnpaired.cfg` | followers measured at the majority's offset, but the report claiming the whole log | violate `AckedSurvive` |
| `FelixShardReportFromAnswers.cfg` | followers counted by what they answered, as the broker counts them, with two promotions in a row and no floor under the report | violate `AckedSurvive` |
| `FelixShardReportFloor.cfg` | the same, with no follower named short of the log the leader inherited | pass |
| `FelixShardWipe.cfg` | `FelixShardReportFloor.cfg` with a follower that loses its disk, the leader learning each follower's storage identity and counting a follower toward the mark only on an answer from that identity this pass, with the clock stopped inside the first lease | pass `AckedOnLiveMajority` and the invariants below (265k distinct states, depth 30, 22 s on a four-core CI runner). No promotion happens here |
| `FelixShardWipeFull.cfg` | the same with the clock to 3, the least at which placement promotes after a wipe, and placement promoting a follower only under the identity it has now | pass `AckedSurvive`, `AckedOnLiveMajority` and the rest (3.6M distinct states, depth 37, 4 min 30 s on a four-core CI runner) |
| `FelixShardWipeNoIds.cfg` | the same without storage identities: a wiped follower stays counted on an idle shard | violate `AckedSurvive` (found in seconds) |
| `FelixShardWipeStripped.cfg` | `FelixShardWipe.cfg` with the clock to 3 and the wiped follower registering again in a step of its own and without an identity, the leader learning each follower's incarnation in the handshake beside its identity and reporting that: a leader still on the old connection reports the old process's incarnation, which placement does not count | pass (state count and time to be measured in CI) |
| `FelixShardWipeStrippedOnArrival.cfg` | the same with the control plane stamping the incarnations as the report arrives, as it still does for a follower that predates the handshake's incarnation: the leader, still on its old connection, reports the follower under its old identity afterwards and the stamp counts it | violate `AckedSurvive` |
| `FelixShardWipeStrippedNoStamp.cfg` | `FelixShardWipeStripped.cfg` without the incarnation check: placement promotes the empty follower on the report from before the wipe | violate `AckedSurvive` |
| `FelixShardCrashKeepsId.cfg` | `FelixShardWipeFull.cfg` with the follower crashing under a relaxed fsync mode instead, keeping its data directory and its identity and losing the record it had answered for: placement promotes it on the report from before the crash. The passing case at one write is `FelixShardProcessCrashKeepsId.cfg`. At two writes, the least that loses a partial suffix, the run passed the 60-minute CI job unfinished, so on this report-gated path that case is **not model-checked in CI**. On the fenced path start records make it affordable: `FelixShardFencedCrash.cfg` | violate `AckedSurvive` |
| `FelixShardProcessCrashKeepsId.cfg` | `FelixShardCrashKeepsId.cfg` with the rule the broker follows (#1367): a crash is the process's, which keeps the whole log, or the machine's, which may lose the unsynced suffix (`ProcessCrashes`), and only a machine crash gives the follower a new identity (`RotateOnCrash`, `KeepIdOnSameBoot`). A follower back from a process crash may stop cleanly and lose power while down (`CleanStops`), after the keep path synced the filesystem (`SyncOnKeep`). One write, so a machine crash empties the log or loses nothing | pass `AckedSurvive`, `AckedOnLiveMajority` and the rest (state count and time to be measured in CI) |
| `FelixShardMachineCrashKeepsId.cfg` | the same with the boot check taking a machine crash for a process crash (`BootCheckBlind`): the follower loses the record and keeps its identity | violate `AckedSurvive` |
| `FelixShardCleanStopUnsynced.cfg` | `FelixShardProcessCrashKeepsId.cfg` without the filesystem sync on the keep path (`SyncOnKeep` off): the follower back from a process crash stops cleanly, which vouches for pages only the kernel held, the power goes while it is down, and the next start keeps the identity without the record | violate `AckedSurvive` |
| `FelixShardLeaderCrashKeepsGen.cfg` | `FelixShardCrashKeepsId.cfg` with the leader crashing instead (`CrashLeader`), under a new identity: it acknowledged a record on itself and a follower, loses it, and comes back still leading at its generation on its lease, with the clock stopped inside the first lease | violate `AckedSurvive` |
| `FelixShardLeaderCrashGivesUp.cfg` | the same with the fix (`GiveUpOnCrash`, #1295): the restarted leader leads nothing from its copy, its report names its own identity, and placement promotes a caught-up follower once the leader's registered identity differs, without waiting for the lease. One write, so the crash loses the whole log or nothing; the fence is not modelled here | pass (state count and time to be measured in CI) |
| `FelixShardLeaderProcessCrash.cfg` | `FelixShardLeaderCrashGivesUp.cfg` with process crashes (`ProcessCrashes`, `KeepIdOnSameBoot`, #1367): a leader whose process crashed keeps its log and its identity, does not give its copy up, and comes back leading at its generation; one whose machine crashed gives it up as before. The clock runs to 3, so lease expiry and a promotion after it returns are explored | pass (state count and time to be measured in CI) |
| `FelixShardLeaderCrashReclaim.cfg` | `FelixShardLeaderCrashGivesUp.cfg` with the probe and the reclaim (`Reclaims`): the followers are measured as they stand, placement promotes one that is furthest and not behind the leader that gave its copy up (`ByProbe`), and names that leader again at the next generation only when every follower is behind it. The model also allows the reclaim beside a promotion on the report from before the crash, which admits more than the code does | pass (state count and time to be measured in CI) |
| `FelixShardLeaderCrashReclaimBlind.cfg` | the same reclaiming without measuring the followers (`ReclaimBlind`, which also turns `ByProbe` off): the leader leads again without a record it acknowledged on itself and a follower | violate `AckedSurvive` |
| `FelixShardFencedWipe.cfg` | the wipe on the fenced, follower-acked path the broker ships: `FelixShardSuspicion.cfg` with one write, one wipe, reports counted from answers with the floor, and storage identities; the promoted leader counts a fence answer only from the identity the report its promotion was decided on named (`FenceCounts`, #1335). Every crash in these configurations is the machine's, which may lose a suffix and rotates; a process crash keeps both and only adds states | pass `OneLeaderPerGeneration`, `AckedHeldByLeader`, `AckedAgree`, `NoTruncationBelowHwm` (state count and time to be measured in CI) |
| `FelixShardFencedWipeCountsWiped.cfg` | the same counting every fence answer (`FenceChecksIds <- No`): the successor's fence counts the empty copy | violate `AckedHeldByLeader` |
| `FelixShardFencedCrash.cfg` | the follower crash with a new identity (`RotateOnCrash`) on the same path, the passing case the report path cannot afford: the promoted leader's start record is a second record, so one write lets a crash keep part of a log | pass, the invariants of `FelixShardFencedWipe.cfg` (state count and time to be measured in CI) |
| `FelixShardFencedCrashUnprobed.cfg` | `FelixShardFencedCrash.cfg` with the first leader not yet knowing either follower's identity (`UnprobedStart`), so a report can name none for one; placement names such a member with an empty identity, which nothing matches. A walk per push, not a check: see below | pass, sampled (exhaustively, it did not finish in six hours on a hosted four-core runner, #1584) |
| `FelixShardFencedCrashCountsUnnamed.cfg` | `FelixShardFencedCrash.cfg` also counting a replica the report named no identity for (`UnnamedCounts`): the leader reported before it asked one follower who it is, then shipped it a record and acknowledged it on the two of them; the follower crashes, and the successor's fence counts it. The code names such a member with an empty identity, which nothing matches | violate `AckedHeldByLeader` |
| `FelixShardFencedCrashNamesByRegistration.cfg` | `FelixShardFencedCrash.cfg` with placement naming a replica the report left unnamed by its registered identity while it is at the report's incarnation (`NameByRegistration`), a rule considered and not taken: for a follower the leader had not asked yet that incarnation is stamped as the report arrives, which can follow a crash the report predates | violate `AckedHeldByLeader` |
| `FelixShardFencedCrashNamesByInheritance.cfg` | `FelixShardFencedCrashUnprobed.cfg` with two promotions on suspicion, every promoted leader blind until it asks, process and machine crashes (`ProcessCrashes`, `KeepIdOnSameBoot`), and a report carrying the identity its leader's own promotion named each replica by, which placement names a replica the report names none for by (`NameByInheritance`, #1574). A nightly walk, not a check: see below | pass, sampled (exhaustively, 27.9M distinct states at depth 22 with 14.3M still queued when TLC's queue filled the runner's disk, 31 min in) |
| `FelixShardFencedWipeNamesByInheritance.cfg` | `FelixShardFencedWipe.cfg` with the same two promotions and inherited names. A nightly walk | pass, sampled (exhaustively, even with state constraints, 30.7M distinct states at depth 23 with 13.9M queued when the disk filled) |
| `FelixShardFencedLeaderCrashNamesByInheritance.cfg` | `FelixShardFencedLeaderCrash.cfg` with inherited names and a second promotion on suspicion. A nightly walk | pass, sampled (did not finish in 5 h 50 min) |
| `FelixShardFencedWipeInheritedCounts.cfg` | the witness for the wipe walk, the same bounds, so a clean walk means it got that far. A nightly walk | violate `NoInheritedCount` |
| `FelixShardFencedLeaderCrashInheritedCounts.cfg` | the witness for the leader crash walk, the same bounds. A nightly walk | violate `NoInheritedCount` |
| `FelixShardFencedCrashInheritedCounts.cfg` | A witness for the inherited-name configurations: a leader promoted at the second failover opens with a fence answer counted from a replica it named by inheritance | violate `NoInheritedCount` |
| `FelixShardFencedLeaderCrashTwiceNamesByInheritance.cfg` | `FelixShardFencedLeaderCrash.cfg` with inherited names and two leader crashes (`Wipes = 2`, no suspicion), so the second replacement can be decided on a report that names a follower only by inheritance. Every promotion replaces a leader that gave its copy up, so no deposed leader serves beside the new one; that is what keeps it small enough to finish | pass `AckedHeldByLeader`, `AckedAgree`, `NoTruncationBelowHwm` (11.2M distinct states, depth 43, 20.5 min on a CI runner) |
| `FelixShardFencedLeaderCrashTwiceInheritedCounts.cfg` | its witness, the same bounds: the leader promoted after the second crash opens with a fence answer counted from a replica it named by inheritance | violate `NoInheritedCount` (99K distinct states, depth 19, 10 s) |
| `FelixShardFencedCrashCountsRotated.cfg` | the same counting the rotated copy, as before #1335: under `Periodic` with three replicas, a record acknowledged on the leader and the follower that lost it is gone once the leader is replaced | violate `AckedHeldByLeader` |
| `FelixShardFencedLeaderCrash.cfg` | `FelixShardLeaderCrashGivesUp.cfg` on the fenced, follower-acked path, no suspicion: the crashed leader's copy still holds its promise and answers the successor's fence, and only the identity check keeps it out (the model has no `refuse_short_copy`) | pass `AckedHeldByLeader`, `AckedAgree`, `NoTruncationBelowHwm` (state count and time to be measured in CI) |
| `FelixShardFencedLeaderCrashCountsRotated.cfg` | `FelixShardFencedLeaderCrash.cfg` counting every fence answer (`FenceChecksIds <- No`): the crashed leader's short copy counts in its successor's fence | violate `AckedHeldByLeader` |
| `FelixShardShortCopySeatAnySet.cfg` | `FelixShardShortCopySeat.cfg` (a nightly walk: the crashed leader's refusal, `RefuseShortCopy`, after an RF 2 seat) with the set-of-two exception's set check dropped (`ShortCopyChecksSet <- No`): the crashed leader of `{a, b, c}` answers its successor's fence at the next generation | violate `AckedHeldByLeader` (255K distinct states, depth 14, under a minute) |
| `FelixShardFencedLeaderCrashKeepsGen.cfg` | the same without the give-up (`GiveUpOnCrash`): the crashed leader keeps leading at its generation without the record | violate `AckedHeldByLeader` |
| `FelixShardHandoff.cfg` | a planned move off a live leader: fence, drained report, cut over; writes hold the fence from admission | pass every invariant (2.39M distinct states) |
| `FelixShardHandoffNoWait.cfg` | the same move cutting over without waiting for the drained report | violate `AtMostOneServing` |
| `FelixShardStalePlannerCas.cfg` | two instances moving the shard, one acting on a held read; writes conditional on the generation read | pass every invariant (3.30M distinct states) |
| `FelixShardStalePlanner.cfg` | the same, writing unconditionally | violate `AtMostOneServing` |
| `FelixShardStalePromotionCas.cfg` | two instances failing the shard over, one acting on a held read; writes conditional | pass every invariant (29K distinct states) |
| `FelixShardStalePromotion.cfg` | the same, writing unconditionally | violate `AtMostOneServing` |
| `FelixShardHandoffLeaderAck.cfg` | a planned move under `Leader` acknowledgement, the claim checking the fence | pass every invariant (1.32M distinct states) |
| `FelixShardHandoffNoClaimFence.cfg` | the same move with the fence checked at admission only | violate `AckedSurvive` |
| `FelixShardHandoffAdmitAck.cfg` | the same move with the write acknowledged on admission and holding the fence from there | pass every invariant (1.25M distinct states) |
| `FelixShardHandoffAdmitAckClaimFence.cfg` | acknowledged on admission, fenced at the claim | violate `AckedSurvive` |
| `FelixShardStagedMove.cfg` | two replicas and a staged destination left out of the quorum while it copies, then the move | pass every invariant (1.99M distinct states) |
| `FelixShardStagedMoveSingle.cfg` | the same with one replica: the leader alone is the quorum | pass every invariant (0.75M distinct states) |
| `FelixShardStagedMoveVotes.cfg` | one replica, with the destination counted toward the quorum | violate `StagedCopyNeverDelaysAck` |
| `FelixShardCancel.cfg` | writes acknowledged on admission, a fenced move cancelled and the shard taken back, a second move after | pass every invariant (6.28M distinct states) |
| `FelixShardCancelStalePlannerCas.cfg` | a cancel decided from a held read while the move cuts over; every write conditional | pass every invariant (447K distinct states) |
| `FelixShardCancelStalePlanner.cfg` | the same with the cancel written unconditionally | violate `AtMostOneServing` |
| `FelixPlacementPacing.cfg` | `FelixPlacementPacing.tla`: moves and follower replacements across four shards, two copies at once, one per node, one planner | pass `CopiesWithinLimit` and `FencedOnlyInAMove` |
| `FelixPlacementPacingUncountedReplacement.cfg` | the same with a follower replacement invisible to the count, as it used to be written | violate `CopiesWithinLimit` |
| `FelixPlacementPacingTwoPlanners.cfg` | two planners over three shards and one slot, the lease changing hands at any step, one also reading without it as an operator does; every start fenced by the placement token | pass `CopiesWithinLimit` and `FencedOnlyInAMove` |
| `FelixPlacementPacingUnfenced.cfg` | the same with starts conditional only on their shard's generation | violate `CopiesWithinLimit` |
| `FelixShardIdempotentFailover.cfg` | a write re-sent across a failover, checked against the promoted broker's log | pass every invariant and `NoDuplicate` (0.20M distinct states) |
| `FelixShardIdempotentFailoverMemory.cfg` | the same with the sequences in the leader's memory | violate `NoDuplicate` |
| `FelixShardIdempotentHandoff.cfg` | a write re-sent across a planned move, checked against the new leader's log | pass every invariant and `NoDuplicate` (2.39M distinct states) |
| `FelixShardIdempotentHandoffMemory.cfg` | the same with the sequences in the leader's memory | violate `NoDuplicate` |
| `FelixShardCancelResend.cfg` | `FelixShardCancel.cfg` with writes re-sent, checked against the retaken leader's log | pass every invariant and `NoDuplicate` (6.28M distinct states) |
| `FelixShardCancelResendMemory.cfg` | the same with the sequences in the leader's memory | violate `NoDuplicate` |
| `FelixAtomicCommit.cfg` | `FelixAtomicCommit.tla`: two atomic commits, each one record, applied whole by three views on every broker, across two promotions | pass `NoPartialCommit` and `CommitSurvives` (21.8K distinct states) |
| `FelixAtomicCommitSplitRecords.cfg` | the same with each part written as its own record, the mark stopping only at a commit's end | violate `NoPartialCommit` |
| `FelixAtomicCommitPartialApply.cfg` | the same with the views applying a record one part at a time | violate `NoPartialCommit` |
| `FelixHeadTrim.cfg` | `FelixHeadTrim.tla`: a conditional head trim as a replicated record, two lease holders, two writes and three trims or no-ops across one promotion, the deposed leader still taking writes and trims, followers rebuilt at the leader's base | pass `NoReadBelowAckedTrim`, `NothingLostAboveFloor`, `FloorRecordKept`, `NeverMoreThanAsked` and `StaleTrimRefused` |
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
| `FelixGroups.cfg` | `FelixGroups.tla`: two groups' dead-letter and cursor logs on three replicas, flushed separately, across a crash and two promotions; each cursor checked against the dead letters it needs before a shard is served | pass `NoSkip` (66.3M distinct states, 11 min on a CI runner) |
| `FelixGroupsNoCheck.cfg` | the same with cursors served unchecked | violate `NoSkip` |
| `FelixGroupsLengthOnly.cfg` | the same with the check comparing the dead-letter log's length only | violate `NoSkip` |
| `FelixGroupsDropGroup.cfg` | the same with a group that has nothing standing dropped, and recreated at the tail | violate `NoSkip` |
| `FelixGroupsSplitSources.cfg` | the same with a promotion taking dead letters and cursors from different replicas, cursors replacing its own | pass `NoSkip` (60.5M distinct states, 17 min on a CI runner) |
| `FelixGroupsSplitSourcesLengthOnly.cfg` | the same with the check comparing lengths only | violate `NoSkip` |
| `FelixGroupsShipUnsynced.cfg` | `FelixGroups.cfg` with dead letters shipped before they are synced | violate `NoSkip` |
| `FelixShardUnified.cfg` | `FelixShardUnified.tla`: the one fence rule (promises from kept ballots, coverage from trusted logs), one write, an unclean crash, a wipe, two starts | pass `OneLeaderPerGeneration` and `AckedHeldByLeader` (20.5M distinct states, 6 min with four workers) |
| `FelixShardUnifiedSets.cfg` | set changes to any set holding the leader, with an unclean crash: joint fences, shrinks to one and two voters, readmissions | pass (3.7M distinct states, 1 min with four workers) |
| `FelixShardUnifiedMixed.cfg` | one node predates the attestation and is counted under today's rules | pass (13.7M distinct states, 4 min with four workers) |
| `FelixShardUnifiedRestore.cfg` | a restored directory and a fence round that starts over | pass (35.8M distinct states, 9 min with four workers) |
| `FelixShardUnifiedReadmit.cfg` | a wiped copy readmitted by a set change, three starts, no writes | pass (25.1M distinct states, 8 min with four workers) |
| `FelixShardUnifiedReadmitFull.cfg` | the same with one write | pass, nightly |
| `FelixShardUnifiedDropEvidenced.cfg` | a set between the anchor and the fence left out when its own leader's trusted answer shows it never opened, no writes | pass (50.6M distinct states, 14 min with four workers) |
| `FelixShardUnifiedDropEvidencedFull.cfg` | the same with one write | pass, nightly |
| `FelixShardUnifiedVouch.cfg` | two writes, two unclean crashes: a copy is trusted again only when a batch leaves it level through max(start record, quorum mark) | pass |
| `FelixShardUnifiedVouchMark.cfg` | the vouch at the code's quorum mark, held to the start record too | pass |
| `FelixShardUnifiedVouchBelowStart.cfg` | the same vouch without the start record: the mark can sit below it right after an open | violate `AckedHeldByLeader` |
| `FelixShardUnifiedStandInNoCatchUp.cfg` | a copy trusted once a batch reaches the start record, short of the mark | violate `AckedHeldByLeader` |
| `FelixShardUnifiedCoverFromDoubted.cfg` | coverage counts a copy an unclean crash may have shortened | violate `AckedHeldByLeader` |
| `FelixShardUnifiedTrustsRegistered.cfg` | a wiped copy's empty log counts as the copy its set named (#1335) | violate `AckedHeldByLeader` |
| `FelixShardUnifiedSkipCoverage.cfg` | a leader that is not continuing opens on its own log | violate `AckedHeldByLeader` |
| `FelixShardUnifiedDropUnopenedLeaving.cfg` | a set being left is dropped on silence | violate `AckedHeldByLeader` |
| `FelixShardUnifiedCountNonKept.cfg` | a promise counts from a copy that lost its ballots | violate `OneLeaderPerGeneration` |
| `FelixShardUnifiedCountsWiped.cfg` | a fence counts a wiped seat's fresh promise in its own new set (#1331) | violate `OneLeaderPerGeneration` |
| `FelixShardUnifiedAnyCopy.cfg` | a continuing fence counts every answer, a lost copy's included | violate `OneLeaderPerGeneration` |
| `FelixShardUnifiedBatchBallot.cfg` | a lost copy's ballot taken from a batch since the wipe counts as kept | violate `OneLeaderPerGeneration` |
| `FelixShardUnifiedCountsForgotten.cfg` | a restore goes unnoticed and the copy claims kept ballots | violate `OneLeaderPerGeneration` |
| `FelixShardUnifiedNamesByRegistration.cfg` | a wiped copy counted as the copy it replaced, by registered storage id (#1335) | violate `OneLeaderPerGeneration` |
| `FelixShardUnifiedContinuingDoubted.cfg` | a continuing fence counts a crashed copy whose ballot names another leader | violate `NoContinuingOnForeignBallot`: the shape is reachable under the rule, and it is safe |
| `FelixShardUnifiedContinuingDoubtedLegacy.cfg` | the same under today's rules | pass `NoContinuingOnForeignBallot`: today's window never counts that copy |
| `FelixShardUnifiedCpLiveElectedFailover.cfg` | an election past the old leader, then a voter crashes uncleanly | pass `EventuallyServes` |
| `FelixShardUnifiedCpLiveElectedFailoverLegacy.cfg` | the same under today's rules | violate `EventuallyServes` |
| `FelixShardUnifiedCpLiveReadmitUnopened.cfg` | a readmission whose leader crashes uncleanly before it opens | pass `EventuallyServes` |
| `FelixShardUnifiedCpLiveReadmitUnopenedLegacy.cfg` | the same under today's rules | violate `EventuallyServes` |
| `FelixShardUnifiedCpLiveMoveDoubted.cfg` | a move to two voters, one back from an unclean crash | pass `EventuallyServes` |
| `FelixShardUnifiedLearnerBlocking.cfg` | four seats, a set change staging one learner, an unclean crash; a fence needs a majority of the voters and half of the set with its learner | pass (6.4M distinct states, 4 min with four workers) |
| `FelixShardUnifiedLearnerBlockingTwo.cfg` | five seats and two learners, where a majority of the voters is short of half the whole set | pass (41.3M distinct states, 41 min with four workers) |
| `FelixShardUnifiedLearnerVariantDropped.cfg` | the same with only the voters fenced: an acknowledgement on one voter and both learners slips past | violate `AckedHeldByLeader` |
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
| `FelixRaftIdentity.cfg` | `FelixRaftIdentity.tla`: three brokers, two terms, one write, one membership change, one fault that empties the shard directory or loses the root | pass `OneLeaderPerTerm`, `LeaderCompleteness`, `StateMachineSafety`, `AckedSurvive`, `AckedHeldByLeader` (0.2M distinct states) |
| `FelixRaftIdentityRelaxedCrash.cfg` | the fault is a relaxed-fsync crash: the storage id rotates and the directory rotates its copy when opened | pass the same (0.1M distinct states) |
| `FelixRaftIdentityRootRestore.cfg` | the whole root goes back to a backup from an earlier run; the incarnation check catches it | pass the same (0.7M distinct states) |
| `FelixRaftIdentityRootRestoreSameRun.cfg` | the same, and the backup may be from the run the restore interrupts | violate `OneLeaderPerTerm`: a design hole, pinned (issue #2303). Neither the incarnation check nor the run witness tells this backup from a crash, and the marks go back with the root (issue #2303) |
| `FelixRaftIdentityDirRestore.cfg` | one shard directory goes back, older log or vote or both, a mark interval after the last append, batched mark | pass the same (2.2M distinct states) |
| `FelixRaftIdentityMarkExact.cfg` | one shard directory goes back at any time, exact mark | pass the same (0.6M distinct states) |
| `FelixRaftIdentityMarkBatched.cfg` | the same bounds with the batched mark: the copy votes for a candidate that lacks an acknowledged record | violate `AckedHeldByLeader` |
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
| `FelixRaftPlacement.cfg` | `FelixRaftPlacement.tla`: three voters and a spare broker, one term, one write, one move by the control plane (a learner entry and a `change_membership` call), no lost disk, every rule on | pass `OneLeaderPerTerm`, `LogMatching`, `LeaderCompleteness`, `StateMachineSafety`, `AckedSurvive`, `AckedOnDisk`, `OffsetNeverReused`, `DeliveredAgree`, `CommitNeverLowers` (137K distinct states). With one term `LeaderCompleteness` has no earlier term to check, and with one write `OffsetNeverReused` has one record |
| `FelixRaftPlacementFailover.cfg` | the same with two terms and no write: a new leader finishes the move or acts on the target again | pass the election, log and commit checks (2.7M distinct states). With no write the Acked, Offset and Delivered checks have nothing to hold |
| `FelixRaftPlacementFull.cfg` | the same with two terms and the write | pass the same (39.3M distinct states, 160 minutes on a CI runner); not registered, checked nightly |
| `FelixRaftPlacementReseat.cfg` | three brokers, two terms: one directory is wiped, the copy takes a new copy id and the leader swaps it in | pass the same (565K distinct states) |
| `FelixRaftPlacementShrink.cfg` | three brokers, two terms: a seat is removed, three voters become two, and the removed copy is deleted under the rule | pass the same (207K distinct states) |
| `FelixRaftPlacementMoveWipe.cfg` | the move in one term with one lost directory and the moved copy deleted under the rule | pass the same (1.1M distinct states) |
| `FelixRaftPlacementMoveWipeFull.cfg` | the same with two terms | not registered and not checked yet: unfinished after three hours on a CI runner, at 98M distinct states with 18.5M queued |
| `FelixRaftPlacementStale.cfg` | three brokers, one term, two target changes, and a leader that may act on the replaced target | pass the same (257K distinct states) |
| `FelixRaftPlacementStaleFull.cfg` | the same with two terms | pass the same (12.6M distinct states, 49 minutes on a CI runner) |
| `FelixRaftPlacementSameId.cfg` | the metadata group's rejoin with the rule of #2365: three members, three terms, one write, one wipe; the member keeps its id, withholds its vote and is counted by no leader until it votes again | pass the same (2.1M distinct states) |
| `FelixRaftPlacementSameIdCounted.cfg` | the metadata group as the code is today: a leader counts a withheld member's appends | violate `AckedSurvive`: an acknowledged write is lost. Pinned finding, #2365 |
| `FelixRaftPlacementSameIdFive.cfg` | five members, two terms, no write, with the rule that a withheld member is not counted: a candidate holds the wiped member's vote and is one short, the member catches up with the older leader, votes again, and votes a second time in that term | violate `OneLeaderPerTerm`: the rule is not enough for five members. Pinned finding, #2365 (1.65M distinct states, 7 min on a laptop) |
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
| `FelixRaftPlacementReseatWrites.cfg` | `FelixRaftPlacementReseat.cfg` with two writes, so `OffsetNeverReused` has two records to compare | pass the same (3.5M distinct states) |
| `FelixRaftPlacementSameIdWrites.cfg` | `FelixRaftPlacementSameId.cfg` with two terms and two writes | pass the same (364K distinct states) |
| `FelixRaftPlacementNoWithholdOffsets.cfg` | the same with a wiped member that votes at once: a leader elected on its vote gives a delivered record's offset to another record | violate `OffsetNeverReused` |
| `FelixRaftPlacementShrinkRetain.cfg` | `FelixRaftPlacementShrink.cfg` with `retain = true`: the removed voter stays as a learner | pass the same (242K distinct states) |
| `FelixRaftPlacementMoveWipeFailover.cfg` | `FelixRaftPlacementMoveWipe.cfg` with two terms and no write: a leader change during a move with one lost disk | not registered and not checked yet: 14M distinct states and unfinished after ten minutes on a laptop; run through the dispatch |
| `FelixRaftPlacementReturnWipe.cfg` | three brokers, one term, one lost disk: a seat is removed and one is moved back, the leader on the current target only; the id told to delete is not placed again | pass the same (843K distinct states) |
| `FelixRaftPlacementPlaceDoomed.cfg` | the same where the control plane may place an id it told to delete | violate `AckedOnDisk` |
| `FelixRaftPlacementStaleWipe.cfg` | `FelixRaftPlacementStale.cfg` with one lost disk, every rule on: a leader on the replaced target gives a copy its vote back after the control plane told its broker to delete it | violate `AckedOnDisk`: an acknowledged write is on no disk. Pinned finding, #2385 |
| `FelixRaftPlacementReachAck.cfg` | witness: three brokers whose membership is given at the start elect a leader and acknowledge a write | violate `NeverAcks` |
| `FelixRaftPlacementForm.cfg` | a first start of the metadata group: three empty members withhold their votes and each forms only when every other member answers empty; two terms, one write, one volume lost after the first commit | pass every check but `OffsetNeverReused`, which one write cannot fail (98K distinct states) |
| `FelixRaftPlacementFormWaiver.cfg` | the same with one slot waived (`form-without`) and its machine never started: two members form and run as two of three | pass the same (334 distinct states) |
| `FelixRaftPlacementFormSilent.cfg` | a member never starts and no waiver names it | pass `NobodyForms`. One state, since the rule's guard is what is checked |
| `FelixRaftPlacementFormWaiverMany.cfg` | the waiver names two of three slots and neither machine starts | pass `NobodyForms`: the empty answers are not a majority. One state |
| `FelixRaftPlacementFormWaiverAnyNumber.cfg` | the same where a waiver may name a majority | violate `NobodyForms` |
| `FelixRaftPlacementFormWaiverAnswers.cfg` | a group that exists, two terms, one write, one lost volume, while a waiver names a slot whose member is up and answers | pass the same checks as `Form` (195K distinct states) |
| `FelixRaftPlacementFormWaiverExcuses.cfg` | the same where a waived member's answer is skipped like its silence | violate `AckedHeldSurvive` |
| `FelixRaftPlacementFormAfterWipes.cfg` | a group that exists, three terms, one write, one volume lost after the first commit: the wiped member takes the first-start step again and cannot form over a member that holds the log | pass the same checks (1.9M distinct states) |
| `FelixRaftPlacementFormTwoWipes.cfg` | the same with two terms and two lost volumes: two wiped members do not form over a third that holds the log | pass `AckedHeldSurvive` only, since two lost volumes of three is past what the group survives (1.3M distinct states) |
| `FelixRaftPlacementFormMajority.cfg` | the same where a majority of empty answers is enough and silence is skipped, the rule before #2371 | violate `AckedHeldSurvive` |
| `FelixRaftPlacementFormEarlyWipe.cfg` | `FelixRaftPlacementFormAfterWipes.cfg` with two terms and a volume lost before the first commit: every answer is empty, the member forms again and votes in a term it forgot | violate `AckedHeldSurvive`. Pinned finding, #2374 |
| `FelixRaftPlacementReachForm.cfg` | witness: three empty members form under the rule, elect a leader and acknowledge a write | violate `NeverAcks` |
| `FelixRaftPlacementReachFormWaiver.cfg` | witness: two members form with the third waived and never started | violate `NeverAcks` |
| `FelixRaftReform.cfg` | `FelixRaftReform.tla`: three intact voters retire and re-form, two writes, one restart, one late append, no rotation | pass `OneLeaderPerTerm`, `AckedSurvive`, `StateMachineSafety`, `NothingLost`, `OffsetNeverReused`, `SlowGroupLeftAlone` (8.3M distinct states, 2.5 min) |
| `FelixRaftReformCommits.cfg` | the same as a witness: a re-form completes and the new epoch commits | violate `NeverCommitsAfterReform` |
| `FelixRaftReformSlow.cfg` | a witness for step 2: two voters report, the third never does, nothing is retired and the group elects by itself | violate `NeverSlowGroupElects` |
| `FelixRaftReformTwoRotated.cfg` | relaxed fsync, two of three copies rotate with any prefix of their logs, one write | pass `OneLeaderPerTerm`, `AckedSurvive`, `StateMachineSafety`, `SlowGroupLeftAlone` on two rules the design does not have yet (issue #2304) (9.2M distinct states) |
| `FelixRaftReformTwoRotatedFull.cfg` | the same with two writes | pass the same, on two rules the design does not have yet (issue #2304) (107M distinct states, 112 min in CI) |
| `FelixRaftReformTwoRotatedOneVoter.cfg` | a witness: the group re-forms with one voter, which commits | violate `NeverOneVoterCommits` |
| `FelixRaftReformTwoRotatedOffsets.cfg` | the same without the exception for a documented loss | violate `OffsetNeverReused`: the relaxed fsync loss, pinned (issue #2305) |
| `FelixRaftReformRejoin.cfg` | a retired copy rotates and rejoins as a learner after announcing its new id; two more rotations, one term per epoch | pass the same as `FelixRaftReformTwoRotated.cfg`, on two rules the design does not have yet (issue #2304) (0.73M distinct states) |
| `FelixRaftReformRejoinFull.cfg` | the same with two terms per epoch | pass the same, on two rules the design does not have yet (issue #2304) (33.9M distinct states, 34 min in CI) |
| `FelixRaftReformRejoinHappens.cfg` | a witness: a retired copy rotates and takes an append as a learner | violate `NeverRetiredRejoins` |
| `FelixRaftReformStaleMembershipIntact.cfg` | four brokers, {a, b, c} to {a, b, d} through a joint configuration, the control plane's record possibly stale, no rotation | pass `OneLeaderPerTerm`, `AckedSurvive`, `StateMachineSafety`, `NothingLost`, `OffsetNeverReused` (0.7M distinct states) |
| `FelixRaftReformStaleMembership.cfg` | the same with one directory lost or restored to any prefix, which covers a relaxed fsync crash | pass `OneLeaderPerTerm`, `AckedSurvive`, `StateMachineSafety` on two rules the design does not have yet (issue #2304) (23.2M distinct states, 40 min in CI) |
| `FelixRaftReformStaleMembershipTwoRotated.cfg` | the design as written: the same with two directories lost or restored to before the change, so both copies rotate with empty logs, no report names d and d's disk holds the entry | violate `AckedSurvive` (issue #2311) |
| `FelixRaftReformStaleMembershipFull.cfg` | the same with a client write; not registered | violate `AckedSurvive` in a dispatch run, as the row above |
| `FelixRaftReformStaleMembershipRelaxed.cfg` | two copies rotate on relaxed fsync, which keeps the synced membership entries, so their reports name d; no client write; not registered, run by dispatch | expected to pass `OneLeaderPerTerm`, `AckedSurvive`, `StateMachineSafety` on two rules the design does not have yet (issue #2304). Not shown yet: no violation in the first 20M distinct states of a ten minute local run |
| `FelixRaftReformStaleMembershipPlaced.cfg` | `FelixRaftReformStaleMembershipTwoRotated.cfg` with the rule issue #2311 proposes: the control plane waits for every broker placed in the epoch; not registered, run by dispatch | expected to pass the same on that rule and the two of issue #2304, none of them in the design yet. Not shown yet |
| `FelixRaftReformLearnerHolds.cfg` | the design as written: three voters and a learner d that no membership names; d holds a record that a and b lose on relaxed fsync, and d is not waited for | violate `AckedSurvive` (issue #2311) |
| `FelixRaftReformLearnerWaited.cfg` | the same with the rule issue #2311 proposes | pass `OneLeaderPerTerm`, `AckedSurvive`, `StateMachineSafety` on that rule and the two of issue #2304, none of them in the design yet (6.2M distinct states) |
| `FelixRaftReformStaleMembershipHappens.cfg` | a witness: the change commits and the group then re-forms | violate `NeverReformsAfterChange` |
| `FelixRaftReformOneReplica.cfg` | one voter whose copy rotates, re-formed from its own report, twice, with a restart and a late append | pass `OneLeaderPerTerm`, `AckedSurvive`, `StateMachineSafety`, `SlowGroupLeftAlone` on two rules the design does not have yet (issue #2304) (7.5M distinct states) |
| `FelixRaftReformOneReplicaCommits.cfg` | a witness: the re-formed single voter commits | violate `NeverCommitsAfterReform` |
| `FelixRaftReformBeforeAll.cfg` | the control plane re-forms before every voter has reported | violate `AckedSurvive` |
| `FelixRaftReformEpochNotChecked.cfg` | a late append from the old epoch is taken by a new-epoch voter and cuts the base out of its log | violate `AckedSurvive` |
| `FelixRaftReformReportNotPromise.cfg` | three intact voters report without retiring, and the old epoch acknowledges a record the base lacks | violate `AckedSurvive` |
| `FelixRaftReformOwnMembershipOnly.cfg` | the control plane waits only for its own stale {a, b, c} while d holds the committed entry | violate `AckedSurvive` |
| `FelixRaftReformRetireNotDurable.cfg` | two retired voters restart, elect and acknowledge in the old epoch | violate `AckedSurvive` |
| `FelixRaftReformBaseNotHighest.cfg` | the base is a shorter report | violate `AckedSurvive` |
| `FelixRaftReformNoCas.cfg` | one voter and one learner; epoch 1 is written twice from epoch 0 with two bases | violate `StateMachineSafety` |
| `FelixRaftReformRotatedNotWaited.cfg` | the design as written: a voter known to have rotated is not waited for, and its disk holds the record | violate `AckedSurvive` (issue #2304) |
| `FelixRaftReformJoinKeepsReport.cfg` | the design as written: a retired copy rejoins as a learner and its old report stands | violate `AckedSurvive` (issue #2304) |
| `FelixRaftSwitch.cfg` | `FelixRaftSwitch.tla`: three copies that all hold the log up to `N`, one wipe at any step, two campaigns, one Raft write, every rule on | pass `LeaderCompleteness`, `StateMachineSafety`, `BaseAgree`, `OneBase`, `OneLeaderPerTerm` (2.2M distinct states) |
| `FelixRaftSwitchLag.cfg` | b may lag: a and c acknowledge, b starts empty as a learner and is promoted; no wipe | pass the same (1.4M distinct states) |
| `FelixRaftSwitchLagWipe.cfg` | the same with one wipe and no Raft write | pass the same (1.0M distinct states) |
| `FelixRaftSwitchWiped.cfg` | b lags and c is wiped before the control plane's write | pass the same and `NeverElects`: unavailable, not wrong (56 distinct states) |
| `FelixRaftSwitchGenZero.cfg` | any copy may carry generation 0 on a prefix of its records | pass the same (10.6M distinct states, 4 min) |
| `FelixRaftSwitchGenZeroFull.cfg` | the same with b lagging and promoted | pass the same (19.2M distinct states, 37 min in CI) |
| `FelixRaftSwitchStarts.cfg` | any copy may start at offset 2; the seal names `S`; b may lag and be promoted | pass the same (8.1M distinct states, 11 min in CI) |
| `FelixRaftSwitchDiffers.cfg` | a follower may hold a different record below `N`; it fails its digest and starts as a learner | pass the same (1.4M distinct states) |
| `FelixRaftSwitchReseal.cfg` | the hybrid opens again and is sealed a second time with the first report in flight; one wipe | pass the same (7.1M distinct states, 2.5 min) |
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
| `FelixRaftLease.cfg` | `FelixRaftLease.tla`: the first version's lease, three brokers, two terms, clocks gaining 1 or 2 units a tick, lease 2 against an election timeout of 4, no write, no restart, no bound on time | pass `LeaseExclusive`, `OneLeaderPerTerm` (0.57M distinct states). With no restart it does not yet gate the lease pull request (issue #2307) |
| `FelixRaftLeaseAck.cfg` | one write, lease 1 against an election timeout of 2 | pass every Part 1 check in [Raft leader leases](#raft-leader-leases) (1.7M distinct states) |
| `FelixRaftLeaseAckWide.cfg` | one write at lease 2 against 4; not registered, run by dispatch | pass the same |
| `FelixRaftLeaseDeliver.cfg` | two writes and the subscriber reading across a change of leader, no drift, lease and election timeout of one tick | pass the same (9.8M distinct states, 3.5 min) |
| `FelixRaftLeaseRestart.cfg` | `FelixRaftLease.cfg` with one restart; a restarted broker refuses votes for one election timeout | pass `LeaseExclusive`, `OneLeaderPerTerm` (4.1M distinct states) on a rule that is not built and openraft 0.10.0 lacks (issue #2307), so it does not yet gate the lease pull request |
| `FelixRaftLeaseRestartForgets.cfg` | the design as written and the engine as it is: a restarted follower may vote at once, as openraft 0.10.0 does | violate `LeaseExclusive` (issue #2307) |
| `FelixRaftLeaseLong.cfg` | a lease of 3 against an election timeout of 4, one more than the inequality allows | violate `LeaseExclusive` |
| `FelixRaftLeaseDrift.cfg` | clocks gain up to 4 units a tick while the inequality was given 2 | violate `LeaseExclusive` |
| `FelixRaftLeaseFromAck.cfg` | the lease runs from when the answer came back, not from the hand-off | violate `LeaseExclusive` |
| `FelixRaftLeaseVoteInWindow.cfg` | a follower grants a vote inside its refusal window | violate `LeaseExclusive` |
| `FelixRaftLeaseAckLong.cfg` | `FelixRaftLeaseAck.cfg` with a lease of 2 against an election timeout of 2 | violate `NoAckBehindNewLeader` |
| `FelixRaftLeaseDeliverUncommitted.cfg` | the subscriber reads the leader's whole log | violate `DeliveredNeverReplaced` |
| `FelixRaftLeaseReachLocalAck.cfg` | witness: a write acknowledged on local durability under the lease | violate `NeverLocalAck` |
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
| `FelixRaftSleep.cfg` | The handshake with every rule on: a write, an audit, a lost request or answer, a restart, a false alarm about the leader | pass on two rules the design does not state yet (issue #2310) |
| `FelixRaftSleepWrite.cfg` | Writes arriving during the handshake are committed and no member sleeps behind | pass on two rules the design does not state yet (issue #2310) |
| `FelixRaftSleepLeaderDies.cfg` | The leader's broker dies; liveness alone (audit off) elects a leader | pass on two rules the design does not state yet (issue #2310) |
| `FelixRaftSleepLivenessMisses.cfg` | Liveness misses the death; the audit alone elects a leader | pass on two rules the design does not state yet (issue #2310) |
| `FelixRaftSleepFalseAlarm.cfg` | A false alarm about a healthy leader deposes nobody, given that the ask arrives and the hold outlasts the answer | pass on two rules the design does not state yet (issue #2310). It does not test that the hold is long enough: the model has no clock and a hold ends only when no live leader could still answer |
| `FelixRaftSleepRestart.cfg` | A restarted member holds until it hears the leader, given that the hold outlasts the leader noticing the new epoch | pass on two rules the design does not state yet (issue #2310). It does not test that the hold is long enough: the model has no clock and a hold ends only when no live leader could still answer |
| `FelixRaftSleepAnyState.cfg` | Members fall asleep in any state with no handshake; safety only | pass |
| `FelixRaftSleepEnds.cfg` | Every behaviour ends (temporal, no symmetry) | pass on two rules the design does not state yet (issue #2310) |
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
configuration does. `FelixShardWalkElectSwap.cfg` found the gap when only the
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
`FelixShardWalkElect.cfg` has a planner acting on a held read.
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
(`FelixShardWalkElectRenameUnfenced.cfg`). The leader that already took the
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

Two promotions do not reach Raft's Figure 8, which needs a third leadership
after the two that disagree, and three from the start did not finish.
`FelixShardFigure8.tla` starts from the history instead: a wrote x at 1, b
wrote y at 2, neither shipped. Without the start record
(`FelixShardFigure8NoStartRecord.cfg`), every label correct, x is lost. c is
promoted at 3, takes x in its fence and acknowledges it on a majority,
because the report and the quorum mark count a record the leader inherited
like its own; a is promoted at 4 and its fence takes b's log, which is ahead
by generation. Counting only records of the leader's own generation closes
it, as `HeldAtGen` does: with that check deleted, the same seeded history
under `FelixShardFencedAckTwoPromotions.cfg`'s knobs fails in twelve steps.

The broker closes it the way Raft does, with a no-op at the start of each
term. `StartRecord` models it: a leader, once it opens for writes (after its
fence under `FenceOnPromote`, at the promotion, cut-over or cancel
otherwise), appends a generation-start record at its own generation before
any client write. It ships like any record but is no client's write, so it
is never acknowledged and costs no write (`StartId`). The mark, and the
length the report measures holders at, stop only at a record of the
leader's own generation, so an inherited record is acknowledged once the
start record behind it reaches a majority. c's start record then has to be
on a or b before x counts, which gives a's log the newer last generation,
and `FelixShardFigure8.cfg` passes. With follower acks the same holds:
`FelixShardFigure8FollowerAcks.cfg` passes, and without the start record
(`FelixShardFigure8FollowerAcksNoStartRecord.cfg`) c acknowledges x once a and
c answer for it, and loses it the same way. `HeldAtGen` applies the
own-generation rule only under `StartRecord`, as the broker applies it only
once `generation_start` is finalized, which is why `majority_ack` needs both.
The other configurations without follower acks set `StartRecord = FALSE` and
explore the states they did before.

A move reaches the same loss one leadership later, which is why the broker
writes the record on every leadership change and not only on a promotion.
`SeededCutOverInit` starts where c, promoted at 3 before the fleet finalized
`generation_start`, holds x unacknowledged and is stopped for a move to a.
Without the record (`FelixShardFigure8CutOverNoStartRecord.cfg`), a cuts over
at 4, acknowledges x on a and c, and c, promoted at 5 from a's report, takes
b's y in its fence, and AckedOnMajority fails. With it
(`FelixShardFigure8CutOver.cfg`, the cancel on too, so c's hand-back is
covered as well) the new leader's start record has to reach a majority before
x counts, and every later fence prefers that log to b's. 12.9M distinct
states, 90 s.

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
floor; readers skip trim records. Only the serving leader answers reads. Two lease holders write and trim, the
second taking over from a first that is not told, and the second makes a
fenced write before it trims. A holder learns the tail by reading from its
own cursor and stops on a newer epoch, in a write or in the refusal's tag. A
trim at or below the floor is a no-op that checks the tail and answers with
it. A deposed leader keeps taking writes and trims until a newer generation
reaches it, and a commit and its answer are separate steps. The passing
run is split in four, since together they do not finish in an hour:
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
acknowledged trim's floor, `NothingLostAboveFloor` that it holds every
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
why felixd's streams run with retention off.

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

### One fence rule: exclusion from kept ballots, coverage from trusted logs

`FelixShardUnified.tla` is a separate, smaller model of the fence rule every change of leader uses: failover,
election, rename, skip, set change, readmission and a continuing leader. It splits what a fence must prove in two.

- **Exclusion**: a majority of every set the fence leaves, and of its own, promised with ballots the copy kept.
  An unclean crash keeps ballots. A wipe or a restore loses them until a set introduced after the loss readmits
  the copy.
- **Coverage**: a continuing leader has it already. Any other leader needs trusted answers (a clean start, or
  vouched by a batch that left the copy level through max(the leader's start record, the quorum mark it carries))
  that meet every majority of each such set, and takes the log furthest ahead.

`docs/replication-design.md`, "One fence rule for every change of leader", states the rule in full.

`UnifiedFence = No` gives today's rules in outline. `CpLiveElectedFailoverLegacy` and `CpLiveReadmitUnopenedLegacy`
show two stalls the simulator sweep found: an election past the old leader followed by an unclean crash, and a
readmission whose leader crashed before it opened. `ContinuingDoubtedLegacy` is not a stall. It passes, showing that
today's window never lets a continuing fence count a doubted copy whose ballot names another leader, which
`ContinuingDoubted` shows the rule does, safely. Each shortcut configuration breaks one part of the rule and must
fail: `CountNonKept`, `CountsWiped` (#1331), `AnyCopy`, `BatchBallot`, `CountsForgotten` and `NamesByRegistration`
let a lost promise make a second leader. `CoverFromDoubted`, `TrustsRegistered` (#1335), `StandInNoCatchUp`,
`VouchBelowStart`, `SkipCoverage` and `DropUnopenedLeaving` let a leader open without an acknowledged record.

`CountsForgotten` and `TrustsRegistered` are why a broker started without the control plane attests nothing. Until
the restore check against the control plane has run, a restored directory looks like one that kept its ballots
and its log. Counted as kept, its forgotten promise makes a second leader (`CountsForgotten`). Counted as trusted,
its short log loses an acknowledged write (`TrustsRegistered`). Once the check has passed, the broker attests as a
start that asked the control plane first would.

A batch vouches for a copy only when it leaves it level through both the leader's start record and the mark it
carries. The mark is the code's quorum offset, which right after an open can sit below the start record:
`VouchBelowStart` drops the start record and loses an acknowledged write, and `VouchMark` passes at the same bounds.
`FelixShardUnifiedReadmit` and `FelixShardUnifiedDropEvidenced` run without client writes, so coverage is checked
on start records. Their `Full` twins have one write and run nightly.

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

### Copy identity under Raft

`FelixRaftIdentity.tla` models the member ids of one Raft group as
[Per-shard Raft](../per-shard-raft.md) decides them under "Group and member
identity". It is the model's side of #1331. A member id is a broker and a
copy id. The copy id lives in the shard directory with the storage id it was
minted under. The data root holds the storage id and, per shard, a mark of
the highest vote and last log id, with the copy id the marks belong to. The
vote is always synced.

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

Checked: `OneLeaderPerTerm`, `LeaderCompleteness`, `StateMachineSafety` (no
two leaders commit different entries at one index), `AckedSurvive` (no set
of copies that could elect a leader lacks an acknowledged record, which
without faults is "on a majority of disks") and `AckedHeldByLeader` (no
leader is elected without a record acknowledged in an earlier term).

Each knob drops one rule: `WipeKeepsId`, `NoRestoreCheck` (no gate in front
of Raft), `NoStorageIdCheck` (a directory is not compared with the root's
storage id), `NoVoteMark`, `VoteBeforeMark` (the vote is answered before its
mark is synced) and `MarkForgetsCopy` (the marks do not name a copy id).
`ExactMark = FALSE` with `RestoreYoung` is the fallback of Questions, 11.

Abstracted: the log is durable when appended (`FelixRaft.tla` has the fsync
rules), messages are atomic steps, a learner needs no membership entry,
nothing is purged, a rotated copy starts from an empty log, and the leader
that stops on a follower going backwards is left out. A follower cuts and
appends in one step, so its last log id never goes down; an implementation
that syncs the mark between the two must not read a cut as a restore.

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
- `WaiverExcusesAnswer`: a waived member's answer is skipped like its silence.
- `WaiverAnyNumber`: a waiver may name a majority of the slots.

`WithheldCounts = TRUE` is what the metadata group does today, and it is a
finding: `FelixRaftPlacementSameIdCounted.cfg` loses an acknowledged write
(#2365). The wiped member forgot the term it voted in, so the leader of
an older term commits with it after a newer leader was elected on its vote.
The config is pinned as an expected violation until the rule is built.

Not counting a withheld member is needed, and it is enough for three members
(`FelixRaftPlacementSameId.cfg` passes). It is not enough for five:
`FelixRaftPlacementSameIdFive.cfg` elects two leaders in one term. A
candidate holds the wiped member's vote and is one vote short. The member
catches up with the older leader, votes again, and grants a second vote in
the term it forgot. With three members the candidate already has its
majority when the member votes, so the older leader cannot be confirmed. A
sure fix gives the wiped member a new id through a membership change, as
shard groups do, or has it hear from every other member before it votes.

The deletion rule has a gap of its own (#2385). The control plane looks at
the committed membership once and the broker deletes later. A leader acting
on a replaced target can give the id its vote back in between, so the
deletion becomes a second lost disk: `FelixRaftPlacementStaleWipe.cfg`
loses an acknowledged write with every rule on, and is pinned. A leader held
to the current target does not (`FelixRaftPlacementReturnWipe.cfg`), and a
replaced target with no lost disk does not (`FelixRaftPlacementStale.cfg`).

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
  silent.
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

The rule has a gap (#2374). An answer shows the log and not the vote. A
volume lost after a vote and before the group's first log entry meets only
empty answers, so the member forms again and votes for a candidate of an
older term than the one it forgot. Both lead, and the older leader
acknowledges a write the newer one lacks.
`FelixRaftPlacementFormEarlyWipe.cfg` is pinned on it. The passing
configurations above set `EarlyWipes = FALSE`, which keeps the lost volume
until the group has committed an entry.

The configurations without `Forming` are given their membership at the start
and are unchanged by it; `FelixRaftPlacementReachAck.cfg` shows they elect a
leader and acknowledge a write. A waiver is a constant here, so its being
used up after one forming is not modelled. The metadata group rejoining
under a new id after a lost volume, the fix #2365 settled on, is not
modelled either. The shard rule it copies is here (`Rotate = TRUE`,
`FelixRaftPlacementReseat.cfg`, `AnswerAnyId`).

### Re-forming a group

`FelixRaftReform.tla` models the path in
[per-shard-raft.md](../per-shard-raft.md#re-forming-a-group) that gives a
Raft group a new epoch when it can no longer elect.

In 0.8 a re-form starts only from an operator's command, one bulk command
with a dry run, and nothing starts one automatically (decided in issue
#2167). The model's first two steps are the automatic trigger, so they
describe a design that is not shipping. So do `SlowGroupLeftAlone` and
`FelixRaftReformSlow.cfg`, which are about when step 2 fires. Steps 3 to 5
are what the command still has to do. It stands where `AskRetire` does, and
it must list every placed broker, a move's target included, which is
`WaitPlaced` below (issue #2311).

The design page now has a rule for each hole this model found, in its table
"Rules the model found" (issues #2304, #2305 and #2311). They are rules for
the operator's command, and they are not the two rules of issue #2304 that
the passing configurations here run with: the command stores no report, so
nothing is left to drop. The model has not been moved to them yet. Where a
row, a header or the text below says the design lacks a rule, or calls
something the design as written, it means the automatic design this module
models. Nothing is built either way.

A group id carries an
epoch that starts at 0. A member id is a broker and a copy id. A copy that
rotates has a new id that never voted and keeps what is left of its log as
plain records. Membership changes inside an epoch go through a joint
configuration, and a member uses the newest membership in its log,
committed or not.

The five steps are five actions. `FirstReport` promises nothing.
`AskRetire` fires only when every voter's broker reported the group
leaderless or a majority of some voter set has rotated. `Retire` sets a
durable marker and then reports the log and the membership. `Reform` waits
for every voter of every membership any report names, takes the highest
reported last log id as the base and the copies whose reports hold it as
the voters, and writes the next epoch by compare-and-swap. `StartEpoch`
starts a member at the new epoch from the base. A group with one replica
is the same model with one voter.

What is checked:

- `AckedSurvive`: an acknowledged record is in the base of every later
  epoch and in the log of every later leader. One loss is allowed, the one
  the design documents: at the re-form no disk held the record, a voter's
  or not. The model records those in `lost` and excuses nothing else.
- `StateMachineSafety`: no index is committed with two different entries,
  in one epoch or across epochs, except over a documented loss.
- `OneLeaderPerTerm`, per epoch.
- `NothingLost` and `OffsetNeverReused`, in the configs where no copy
  rotates. That is fsync on commit, and a re-form then loses nothing and
  reuses no offset.
- `SlowGroupLeftAlone`: with no copy rotated and a voter that has not
  reported, nothing is retired.

Each knob drops one rule: `ReformBeforeAll` (re-form from the reports at
hand), `EpochNotChecked` (a member takes an append from another epoch),
`ReportNotPromise` (report without retiring), `OwnMembershipOnly` (wait
only for the control plane's own record of the membership),
`RetireNotDurable` (a restart forgets the marker), `BaseNotHighest`,
`NoCas`, `RotatedNotWaited` (a voter known to have rotated counts as done
before it has reported its records) and `JoinKeepsReport` (a copy that
rotates after it reported joins as a learner while the control plane still
holds its old report).

The last two are rules the design page does not state. Step 4 says a voter
is done when it "has retired and reported, or is known to have rotated".
Read as two separate ways to be done, a rotated copy's records are never
collected, and `FelixRaftReformRotatedNotWaited.cfg` loses a record its
disk holds. The page also lets a retired copy rotate and rejoin as a
learner. Its old report stays with the control plane, the learner takes
newer records, and `FelixRaftReformJoinKeepsReport.cfg` loses one of them.
The passing configs run with both knobs off, which is two rules the design
does not have yet: a rotated voter still sends a retire report, and a new
copy tells the control plane its id before it joins, so that the control
plane drops the report of the copy it replaces. Every configuration that
passes with a rotation or a rejoin passes on those rules, and its row and
header say so. They show the rules are enough, not that the design as
written is safe (issue #2304).

Two constants are not knobs. `DirLost` says what a rotation leaves. Off, it
is a relaxed fsync run that lost power: the tail may be gone, but membership
entries are synced in every fsync mode, so the log keeps everything up to
its newest membership entry. On, it is a directory that was lost or restored
from an older copy, and any prefix is left. `WaitPlaced` is the rule issue
#2311 proposes: the control plane records the target of a membership change
before the leader proposes it, and step 4 waits for a retire report from
every broker placed in the epoch, voter or not. Every broker of the model
counts as placed. Off is the design as written.

The stale-membership case of the design page, {a, b, c} changing to
{a, b, d}, splits on those two constants. With a and b rotating on relaxed
fsync and no client write, their reports still name d;
`FelixRaftReformStaleMembershipRelaxed.cfg` checks that and is run by
dispatch. With the two directories lost, no report names d and
`FelixRaftReformStaleMembershipTwoRotated.cfg` loses an entry d's disk
holds. The same gap needs no change and no lost directory:
`FelixRaftReformLearnerHolds.cfg` has three voters and a learner d, and
loses a record that a and b lose on relaxed fsync and only d still holds,
because no membership names d. A voter that a change removed is in the same
position. Both are the design as written and are pinned as expected
violations (issue #2311). `FelixRaftReformLearnerWaited.cfg` passes with
`WaitPlaced` on.

After a relaxed fsync loss the offsets of the lost records are above the
base and the new epoch gives them out again.
`FelixRaftReformTwoRotatedOffsets.cfg` pins that.

Abstracted: storage is a log and a rotation that cuts it to a prefix
(`FelixRaft.tla` has the fsync rules). A membership entry is synced when
it is appended. An election is one step over a
quorum. An append is one step between two brokers, plus a bounded set of
in-flight appends that can arrive late and more than once. A report
carries the whole log where the design sends its last log id. A leader
does not propose its own removal. A leader replicates to any broker of the
model: one that no membership names is a learner.

Not modelled: the operator's command itself. Step 2 fires only under the
automatic conditions, so a command given to a group that can still elect is
not covered. Neither is its refusal while a placed broker is unreachable
and not declared dead, nor a re-form that goes on without a dead broker.

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

Abstracted: storage is durable at once (`FelixRaft.tla` covers the
adapter's fsync rules), a message is one atomic step, and the hybrid before
the seal is the initial choice. The hybrid's rebuild and catch-up during
the seal are left out, so a copy that lags or was wiped stays so until Raft
brings it up. The promotion adds one voter in one step. Records below `S`
are outside the check, as the design says. Cache shards, whose digest is
over live state, are not modelled.

### Raft leader leases

`FelixRaftLease.tla` models the lease that lets a `Leader` write be
acknowledged on local durability, as
[Per-shard Raft](../per-shard-raft.md) ("Acknowledging a write") and
[The replication engine interface](../replication-engine.md) (Decisions, 5)
decide it. Its configurations gate the pull request that turns on
`admit_local_ack`, but their passing does not open the gate yet: the passing
ones either have no restart or pass on a rule that is not built and that
openraft 0.10.0 lacks (issue #2307, under "A restarted follower" below). The
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

**A restarted follower.** The refusal window lives in memory. openraft 0.10.0
builds the stored vote with a zero lease at startup, so a follower that
answers the leader, restarts and is then asked for a vote grants it at once.
`FelixRaftLeaseRestartForgets.cfg` shows the old leader still passing the
lease check when the new one is elected. The design's hold on a restarted
member stops it campaigning, and the model keeps that hold; it does not stop
it granting a vote to a broker that never heard the leader. `FelixRaftLeaseRestart.cfg` adds the
rule that closes it: a restarted broker refuses votes for one election
timeout from its start. The design page now has this rule, as the vote hold
(issue #2307). openraft 0.10.0 does not, and it is not built. Until it is
built, `FelixRaftLeaseRestart.cfg`
passing says only that the rule would be enough, and
`FelixRaftLeaseRestartForgets.cfg` is what the engine does.

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

Two rules here are not in the design page's one-paragraph description. A
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
membership changes, snapshots, pre-vote or leadership transfer.

### Raft sleep and wake

Sleep and wake are deferred to v0.9.0, with the store liveness they rest on
(decided in issue #2167). Nothing in this section gates 0.8. The configurations stay
registered so the design keeps its checks until it is built. The design page
keeps sleep under "Deferred: sleep and wake", and its table "Rules the model found" now
states the two rules of issue #2310 that this section says the design lacks, with three
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
the design page does not state yet and the model needs: `HeartbeatKeepsProposal` (a
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

Not modelled: storage (an append is durable), snapshots, membership changes, read rounds,
leases, pre-vote, ranked starts and the per-tick election cap. Messages are atomic steps
except the sleep request. Only a leader's broker dies, and a disk stall counts as a death.

## Which configurations run where

TLC reads only the spec, so the `formal` job in `.github/workflows/ci.yml`
runs only when `docs/formal/`, `scripts/check_tla.sh` or that workflow
changes. A pull request checks the registered configurations that take ten
minutes or less (`TLA_QUICK=1`), plus `FelixShard.cfg`, the base
configuration, whatever its time, and any whose own `.cfg` it changes. A
changed configuration of 30 minutes or less runs in the twenty shards with the
quick ones. A heavier one runs in a job of its own (`formal-heavy`): the
`formal-heavy-plan` job lists them with `scripts/check_tla.sh --plan-heavy`. A push
to main checks every registered configuration. The shards are twenty jobs, on a pull request and on main,
filled by each configuration's measured time. Main's shards share a
concurrency group per shard: one run proceeds and only the newest later push
waits, so a burst of merges does not queue hours of runner time. A slow configuration that a
change to the `.tla` alone breaks therefore shows up on main, not on the pull
request; run it on the branch through the workflow's dispatch (`tla_configs`)
when the change is risky.

Whether a code change still matches the spec is a separate, quick check on
every pull request that touches code (`scripts/check_spec_pairing.py`).

Some configurations are too big for that hour, and run nightly instead, each in
its own job with a six-hour limit (the `exhaustive` job in
`.github/workflows/tla-walk.yml`): `FelixShardElectSwapKeep0.cfg`, the four
`FelixShardElectSwap{Keep0,Keep1,Keep2,Drop2}Stood.cfg` and
`FelixShardElectSwapDrop{0,1}StoodSeveralDest.cfg`
pieces of `FelixShardElectSwap.cfg`, eleven pieces of
`FelixShardElectSwapFlip.cfg` (`Keep0`, `Drop0`, `Keep0StoodOneOther`, the
four `StoodOneDest` and the four `StoodSeveralDest`),
`FelixShardElectHandoff.cfg`,
`FelixShardElectAdoptTwoWrites.cfg`,
`FelixShardFencedAckTwoPromotions.cfg`, `FelixShardFencedAckSeatLonger.cfg`,
`FelixShardFencedAckMove.cfg`, `FelixShardFencedAckStagedMove.cfg`,
`FelixShardFencedAckMoveCancel.cfg`, `FelixShardUnifiedReadmitFull.cfg`,
`FelixShardUnifiedDropEvidencedFull.cfg` and `FelixRaftPlacementFull.cfg`. Each must pass. A smaller sibling of each
runs per push. A failure shows the same way a failed walk does, as a failed
run of that nightly workflow. Either set can also be run on a branch through
the dispatch of either workflow.

## Long random walks

The configurations above check every behaviour up to their bounds, and the
bounds are small: a move or two, a promotion or two, a few ticks. Raising any
of them costs about an order of magnitude in states, so exhaustive search
cannot reach a behaviour that grows the set, replaces a follower, fails over
and moves the shard again. The `FelixShardWalk*.cfg` configurations lift the
bounds (time to 40, six writes, six moves) and TLC samples behaviours up to
300 steps long instead, in its simulation mode. The inherited-name families
keep their exhaustive bounds and sample them, since those are too big to
check exhaustively. They run nightly, not per push
(`.github/workflows/tla-walk.yml`). The exception is
`FelixShardFencedCrashUnprobed.cfg` and its twin
`FelixShardFencedCrashCountsUnnamed.cfg`, walked per push at the same budget
by the `formal-walk` job in `.github/workflows/ci.yml` whenever the `formal`
job runs: it was an exhaustive check per push until it stopped finishing
(#1584). The twin is also still checked exhaustively.

A walk shows less than a check. It is sampling: a pass means the walks found
nothing in their budget, not that nothing is there, and uniform random choice
under-samples rare interleavings. What it adds is depth: invariants checked on
behaviours far longer than any exhaustive configuration reaches. Each walked
negative twin must find its violation within the same budget (the election and
spares twins are replayed as fixed traces instead, see below), which shows the
walks are long and varied enough to lose a record at all. A twin checks only
the invariant it expects, so the first violation it finds is that one.

The spec admits spares only without handoff and cancel, so there is one walk
per family:

| Configuration | Knobs | Must |
| --- | --- | --- |
| `FelixShardWalkSpares.cfg` | `FelixShardFencedAckGrow.cfg` and `FelixShardFencedAckSeat.cfg` together: two replicas and two spares, the set grown to three, then followers replaced and seated; promotion of any replica, on suspicion up to generation 10, start records, `L = 2`, drifting clocks | pass `OneLeaderPerGeneration`, `AckedHeldByLeader`, `AckedAgree`, `AckedOnMajority`, `NoTruncationBelowHwm` |
| `FelixShardWalkSparesSeatEarly.cfg` | the same with the seat not waiting (`SeatHoldsCopy = FALSE`) | violate `AckedHeldByLeader`; not walked: the walks found it only by chance, so `FelixShardWalkSparesSeatEarlyReplay.cfg` replays one counterexample step by step (`FelixShardWalkSparesSeatEarlyReplay.tla`) and must still violate it, in seconds, with the exhaustive checks |
| `FelixShardWalkMoves.cfg` | follower acks with planned moves: the first from a staged destination, any fenced move open to the operator's cancel, failovers that never name the destination, promotion on suspicion up to generation 10; start records, `L = 2`, drifting clocks | pass `OneLeaderPerGeneration`, `AckedHeldByLeader`, `AckedAgree`, `AckedOnMajority`, `NoTruncationBelowHwm` |
| `FelixShardWalkMovesDestination.cfg` | the same with a failover free to name the destination | violate `AckedHeldByLeader` |
| `FelixShardWalkHandoff.cfg` | the broker as built under the lease (`FelixShardFencedPromotion.cfg` with start records) with `FelixShardHandoff.cfg`'s move; `FelixShardRealMargins.cfg`'s margins and drift, writes acknowledged on commit | pass every invariant and `AckedHeldByLeader` |
| `FelixShardWalkHandoffNoWait.cfg` | the same cutting over without waiting for the drained report | violate `AtMostOneServing` |
| `FelixShardWalkElect.cfg` | `FelixShardWalkSpares.cfg` with replicas electing themselves up to two generations past the control plane's, the set check with the joint fence, the control plane adopting elected leaders up to generation 10, and a planner acting on a held read, the control plane's own leader elected on another set reissued on it (`ReissueOwnOnOtherSet`) | pass `OneLeaderPerGeneration`, `AckedHeldByLeader`, `AckedAgree`, `AckedOnMajority`, `NoTruncationBelowHwm`, `AdoptedMatchesOpened` |
| `FelixShardWalkElectStaleSet.cfg` | the same without the set check | violate `AckedHeldByLeader`; not walked: the walks found it only by chance, so `FelixShardWalkElectStaleSetReplay.cfg` replays one counterexample step by step (`FelixShardWalkElectStaleSetReplay.tla`) and must still violate it, in seconds, with the exhaustive checks |
| `FelixShardWalkElectSwap.cfg` | `FelixShardElectSwap.cfg` lifted: up to six cut-overs that change the set by more than one member, promotion on suspicion up to generation 10, elections two generations past the control plane's, adoption to generation 10 and a planner acting on a held read, six writes, time to 40, drifting clocks, the control plane's own leader elected on another set reissued on it (`ReissueOwnOnOtherSet`) | pass the same invariants and `SetChangesJoint` |
| `FelixShardWalkElectSwapNoJoint.cfg` | the same without the joint configuration, and without promotion on suspicion or adoption so the walks reach the trace within their budget | violate `AckedHeldByLeader` |
| `FelixShardWalkElectRename.cfg` | `FelixShardWalkElectSwap.cfg` with the control plane naming its leader again after an election it recorded and did not adopt (`Renames`), the control plane's own leader elected on another set reissued on it (`ReissueOwnOnOtherSet`) | pass the same invariants |
| `FelixShardWalkElectRenameUnfenced.cfg` | the same with the renamed leader opening without its fence (`RenameUnfenced`) | violate `AckedHeldByLeader` |
| `FelixShardFencedCrashNamesByInheritance.cfg` | inherited names under two promotions on suspicion with process and machine crashes (the table above) | pass `OneLeaderPerGeneration`, `AckedHeldByLeader`, `AckedAgree`, `NoTruncationBelowHwm` |
| `FelixShardFencedCrashInheritedCounts.cfg` | its witness | violate `NoInheritedCount` |
| `FelixShardFencedWipeNamesByInheritance.cfg` | the same with a wipe | pass the same invariants |
| `FelixShardFencedWipeInheritedCounts.cfg` | its witness | violate `NoInheritedCount` |
| `FelixShardFencedLeaderCrashNamesByInheritance.cfg` | a leader crash and a second promotion on suspicion | pass `AckedHeldByLeader`, `AckedAgree`, `NoTruncationBelowHwm` |
| `FelixShardFencedLeaderCrashInheritedCounts.cfg` | its witness | violate `NoInheritedCount` |
| `FelixShardShortCopySeat.cfg` | a crashed leader's refusal and its set-of-two exception after an RF 2 seat (see [A crashed leader's own refusal](#a-crashed-leaders-own-refusal)) | pass `AckedHeldByLeader`, `AckedAgree`, `NoTruncationBelowHwm` |
| `FelixShardShortCopySeatAnySet.cfg` | its twin, the exception without its set check | violate `AckedHeldByLeader` |
| `FelixShardShortCopyStaged.cfg` | the refusal beside a staged destination, the ids' comparison dropped | pass the same invariants |
| `FelixShardShortCopyStagedVotes.cfg` | the refusal beside a staged destination that votes, the code's fence ids | pass the same invariants |
| `FelixShardShortCopyStagedVotesSetOnly.cfg` | its twin, fence ids without the staged copy (#1635's proposed fix) | violate `AckedHeldByLeader` |
| `FelixShardFencedCrashUnprobed.cfg` | a follower crash with a new identity, the first leader not yet knowing either follower's identity (the table above). Per push | pass `OneLeaderPerGeneration`, `AckedHeldByLeader`, `AckedAgree`, `NoTruncationBelowHwm` |
| `FelixShardFencedCrashCountsUnnamed.cfg` | its twin, counting a replica the report named no identity for. Per push | violate `AckedHeldByLeader` |

Every walk except the short-copy configurations (which set `Ballots` and
`FenceEveryChange` to `FALSE`) runs with ballots and every change of leader
fenced (`Ballots`, `FenceEveryChange`), as the broker ships, and all but the
election walks without elections, which the broker does not hold yet. The
exception is `FelixShardWalkMovesDestination.cfg`, whose violation is the
destination opening without the fence. The follower-acked walks, moves and spares and
their twins, also promote on suspicion (`Suspicions = 10`), as placement does
once the fleet acknowledges by its followers; past the tenth generation only a
lapsed lease promotes, so the rest of a walk still reaches moves a promotion
would keep ending. The handoff walk runs on the lease, where placement never
acts on a suspicion.

The inherited-name walks are the exhaustive configurations unchanged, sampled
because they do not finish. State constraints that kept the second promotion
to the inherited case, the fault between the two promotions, the write on one
side of the first promotion, or a deposed leader idle, still left over 30M
distinct states and a queue of 13M when the runner's disk filled: a leader
deposed on suspicion keeps serving beside each new one.
`FelixShardFencedLeaderCrashTwiceNamesByInheritance.cfg` is the one that
finishes, per push.

The other walks carry no `SYMMETRY`: simulation stores no states, so it buys nothing.
`Tick` draws each clock from its drift window rather than from `0..MaxTime`,
which gives the same clocks and keeps a tick cheap at time to 40.

`Next` lists every action as its own disjunct. TLC's simulation mode picks an
enabled action and then one of its successors, but it splits `Next` into
actions only at disjunctions and constant `\E`. With everything in one
conjunction, a walk picked uniformly among successor states, and with drift
`Tick` has up to 81 of them. The walks ticked time to its bound within a few
dozen steps and spent the rest with time stopped, where no lease lapses after
a replacement is seated, so the spares twin found nothing.

`GrowSet` only grows a set short of the factor. Without that guard, a walk
with two spares grew the set to four, and with no spare left it never
replaced a follower again.

```bash
TLA_SIM_MINUTES=10 scripts/check_tla.sh --simulate FelixShardWalkSpares
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
