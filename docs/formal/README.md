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
| `CollisionSuperseded` | With the control plane up, it does not stay at a generation whose leader can never open, because another broker was elected there and opened, while that broker still leads. A temporal property, checked under `ReissueSpec` in `FelixShardElectLiveReissue.cfg`. |
| `CpLeaderServes` | With the control plane up and every stopped broker back, the leader it names ends up open at its generation, serving with a mark over its whole log and a majority of its set taking its batches, and stays (`<>[](LeaderServes /\ ServesWith(leader))`). Checked under `CpLiveSpec` in `FelixShardCpLive.cfg` and its twins. |
| `CpDownServesInBound` | With the control plane stopped for good, some live leader ends up open and serving on a majority that holds its generation for it, and stays (`CpDownServes`), or elections stop only at the model's bound with a broker that would still stand (`Exhausted`). Checked under `CpDownSpec` in `FelixShardCpDown*.cfg`. |
| `CpDownFullyBacked` | The same, with every live member of the leader's set behind it. Not owed: `FelixShardCpDownDegraded.cfg` is its witness. |

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
| `FelixShardElectRenameDue.cfg` | its witness | violate `NeverRenameDue` (35K distinct states, depth 11, 1 s): after a cut-over, a replica on the set being left is elected at generation 3, the control plane records only that generation, and a majority of the pending set promised it away, so the destination's fence cannot reach one |
| `FelixShardElectRenameFencingDue.cfg` | its witness for the leader that took the generation | violate `NeverRenameWhileFencing` (170 distinct states, depth 6, 1 s): the leader fences jointly while a set change is pending, and a replica on the set being left stood at that generation and won another's promise, so a majority of the old set is out of reach |
| `FelixShardElectRename{Keep,Drop}{0,1}{,Stood{One,Several}{Dest,Other}}.cfg` | `FelixShardElectRename.cfg` split at its cut-over as `FelixShardElectSwap.cfg` is (`SplitAtCutOver`, `CutOverPiece`). Each piece also checks everything before its cut-over, so together they reach every state the whole does. The `StoodOneDest` pieces run nightly: the cut-over overwrites everything the destination's stand wrote, so they should reach the same states as the pieces where nobody stood (argued, not measured) | pass, the same invariants. `{Keep,Drop}{0,1}StoodSeveralOther` 37,646 distinct, 2 s on a laptop. On a four-core CI runner the others take 2 to 36 min (1.1M to 25.1M distinct, depth 37 to 40), except `Keep0StoodSeveralDest` (38 min to 1 h 16 min), `Keep1StoodSeveralDest` (51 min) and `Drop0StoodSeveralDest` (1 h 21 min), which run nightly with `Drop0StoodOneOther` and the `StoodOneDest` pieces. The four `StoodSeveralOther` pieces hold no cut-over, as in `FelixShardElectSwap.cfg`, and check that they stay empty (`NoCutOverInPiece`) |
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
| `FelixShardCpLive.cfg` | three brokers with the control plane up, no writes, no symmetry, up to four generations, one broker allowed to stop and come back (`Restarts`), standings that may be withdrawn and keep their ballot (`Withdraws`), renames on a refusal (`Renames`), four fixes from the list of dead ends in #1914 on (`OverlayRefused`, `HaltReported`, `DeposeOnAssigned`, `StandBehindBallot`), a fence's step on the leader's own log reporting its refusal (`OwnBallotRefused`, #1919), and a leader that lost its generation to a newer one in its own log reporting it (`AboveReported`, #1951) with placement renaming it above the ballot it lost to (`RenamePastBallot`), under `CpLiveSpec` | violate `CpLeaderServes` (#1989); not registered: it runs for hours. A CI run violated it after 3 h 32 min (39.6M distinct states): the control plane adopts an elected leader whose log keeps its set two generations back, the leader restarts, and its fence can neither take the assignment's set nor report (`Skipped` in `set_before`, promotion/joint.rs) |
| `FelixShardCpLiveMain.cfg` | the same with those fixes off, as main's code is | violate `CpLeaderServes`: a leader fencing at its assignment while its own ballot is above it never reports the refusal (`keep_own_set`, #1919) |
| `FelixShardCpLiveNoOwnBallotRefused.cfg` | `FelixShardCpLive.cfg` without `OwnBallotRefused` | violate `CpLeaderServes`: a leader fencing at its assignment with its own withdrawn standing above it never reports (321K distinct states locally at three generations) |
| `FelixShardCpLiveNoOverlayRefused.cfg` | without `OverlayRefused` | violate `CpLeaderServes`: the control plane's leader steps down when it answers a higher fence, every member ends promised to a candidate that withdrew, and nothing reports a refusal (349K distinct states locally) |
| `FelixShardCpLiveNoHaltReported.cfg` | without `HaltReported` | violate `CpLeaderServes`: an open leader whose followers took a withdrawn candidate's higher fence halts on their refusals, and the halt is never reported (937K distinct states locally) |
| `FelixShardCpLiveNoStandBehindBallot.cfg` | without `StandBehindBallot` | violate `CpLeaderServes`; not registered: no violation yet in 3.9M distinct states and 26 minutes in a pull request's CI |
| `FelixShardCpLiveNoAboveReported.cfg` | without `AboveReported`, as the code is | violate `CpLeaderServes` (#1951): the open leader answers a candidate's higher fence and stops serving, the candidate withdraws, and the leader reports nothing (895K distinct states, 3 min locally) |
| `FelixShardCpDown.cfg` | `FelixShardCpLive.cfg` with the control plane stopping for good (`CpStops`), suspicion per broker and per shard (a broker named the leader included, `SelfSuspicion`), a pre-vote refusal bounded by the fence it waits on (`PreVoteBounded`), a promise freed once its candidate no longer stands and held at most a bound (`StandBehindBounded`), and a candidate withdrawing only once its fence cannot complete (`WithdrawsStuck`), under `CpDownSpec` | not registered: no violation of `CpDownServesInBound` in 56.8M distinct states and five hours in CI, with the queue still growing |
| `FelixShardCpDownNoSelfSuspicion.cfg` | the same without `SelfSuspicion`: a broker named the leader never stands for its shard, as in the code | violate `CpDownServesInBound`: a leader elected above its assignment restarts with the control plane down, its open is refused for its own ballot, and only it could win a pre-vote (2.5M distinct states, 12 min in CI) |
| `FelixShardCpDownBrokerSuspicion.cfg` | the same with suspicion per broker only, as the code has it | violate `CpDownServesInBound`: a restarted leader fences at a generation its followers promised past, and nobody suspects a live broker |
| `FelixShardCpDownPreVoteUnbounded.cfg` | the same with the pre-vote refused for as long as the asked broker fences | not registered: no violation in 49.6M distinct states and five hours in CI, with the queue still growing |
| `FelixShardCpDownStandBehindDeadOnly.cfg` | the same with a promise freed only once its candidate is down | not registered: no violation in 88.6M distinct states and five hours in CI. Before `SelfSuspicion` it violated `CpDownServesInBound` (the candidate restarts without its standing, and the replica that answered it never stands); with it, that broker stands for its own shard, and no counterexample has turned up |
| `FelixShardCpDownDegraded.cfg` | `FelixShardCpDown.cfg` held to `CpDownFullyBacked` | violate it: placement names one broker at the generation another was elected and opened at, then stops; the named one fences there for good (357K distinct states locally at three generations) |
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
| `FelixShardElectWipeNonVoterNoCatchUpFailover.cfg` | `FelixShardElectWipeNonVoterNoCatchUp.cfg`'s fault with `Elections = 0`, no counters and no fence without a set | violate `AckedHeldByLeader`: re-admitted with an empty log, the copy answers the next failover's fence |
| `FelixShardElectWipeNonVoterEvidenceOnlyFailover.cfg` | `FelixShardElectWipeNonVoterEvidenceOnly.cfg`'s fault with `Elections = 0`, no counters and no fence without a set | violate `AckedHeldByLeader`: wiped before it heard any set, the copy answers a failover's fence that names its new id |
| `FelixShardElectWipeNonVoterCreatesReadmits.cfg` | `FelixShardElectWipeNonVoterCreates.cfg` at the same bound, checking a witness | violate `NoFailoverAfterReadmit`: the wiped copy is re-admitted, the write is acknowledged at the re-admission's generation, and a leader opens above it |
| `FelixShardElectWipeNonVoterElects.cfg` | `FelixShardElectWipeNonVoter.cfg`, checking a witness | violate `NoLeaderBesideNonVoter`: a leader at a generation taken after the wipe opens without the non-voter |
| `FelixShardElectWipeNonVoterReadmits.cfg` | the same | violate `NeverReadmitted`: the re-admission completes after the catch-up |
| `FelixShardElectWipeNonVoterFailsOver.cfg` | `FelixShardElectWipeNonVoterFailover.cfg`, checking a witness | violate `NoFailoverBesideNonVoter`: the control plane fails over while a wiped replica is a non-voter |
| `FelixShardFencedAckAnyKept.cfg` | `FelixShardFencedAck.cfg` with a spare fourth broker outside the replica set (`Spares`), promotion of any replica however far behind (`Promotion = "any"`), and the promotion keeping the replica set, the old leader in it; no drift | pass `AckedHeldByLeader`, `AckedAgree`, `AckedOnMajority`, `NoTruncationBelowHwm` (0.48M distinct states, depth 31, 27 s on four workers; with `Drift = 1`, 39.0M distinct states in 35 min on four workers, by hand) |
| `FelixShardFencedAckAnyReplaced.cfg` | the same with the promotion swapping the old leader for the spare (`ReplaceOnPromote`), as failover's `choose_replicas` would | violate `AckedHeldByLeader`: the new leader and the spare are a majority of the new set and open without the record the old leader and the third replica acknowledged |
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

## Which configurations run where

TLC reads only the spec, so the `formal` job in `.github/workflows/ci.yml`
runs only when `docs/formal/`, `scripts/check_tla.sh` or that workflow
changes. A pull request checks the registered configurations that take ten
minutes or less (`TLA_QUICK=1`), plus `FelixShard.cfg`, the base
configuration, whatever its time, and any whose own `.cfg` it changes. A
changed configuration of 30 minutes or less runs in the seven shards with the
quick ones. A heavier one runs in a job of its own (`formal-heavy`): the
`formal-heavy-plan` job lists them with `scripts/check_tla.sh --plan-heavy`. A push
to main checks every registered configuration. The shards are seven jobs,
filled by each configuration's measured time. A slow configuration that a
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
`FelixShardFencedAckMove.cfg`, `FelixShardFencedAckStagedMove.cfg` and
`FelixShardFencedAckMoveCancel.cfg`. Each must pass. A smaller sibling of each
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
