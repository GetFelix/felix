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
  The logs that ride a shard (consumer-group cursors, dead letters,
  counters) are not modelled separately. They are written through the same
  fence, the drained report waits for them to be on the successor, and a
  follower without them is left out of the report's candidates, so a write to
  any of them is modelled as a write to the one log.
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
  its set's promise, as an elected one would. Nothing in the broker elects
  yet; the ballot is in the code now so that a later change can.
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

## What is checked

| Invariant | Says |
| --- | --- |
| `AtMostOneServing` | No two brokers serve the shard at once. |
| `AckedSurvive` | Whoever is serving holds every acknowledged record. |
| `AckedHeldByLeader` | The leader at the newest generation holds every acknowledged record once it may serve. `AckedSurvive` without the lease, for configurations where two brokers can serve at once. The newest generation is the control plane's unless a replica elected itself past it. |
| `AckedAgree` | Two brokers never hold different acknowledged records at one offset. |
| `AckedOnMajority` | Every acknowledged `Quorum` record is on a majority. A set of four, which a promoted replacement leads beside the three it joined, needs two, since every fence there takes three. |
| `NoTruncationBelowHwm` | A follower never discards a record below its high-water mark. |
| `NoStaleCommit` | No broker commits at a generation the control plane has superseded. |
| `StagedCopyNeverDelaysAck` | A `Quorum` write the stream's own replicas would acknowledge is never held back by a destination's copy. A latency property, checked only where a destination is staged. |
| `NoDuplicate` | No log holds one write twice. Checked where writes are re-sent. |
| `OneLeaderPerGeneration` | No two brokers open for writes at one generation. Checked where replicas elect themselves. |
| `QuorumReportNamesASuccessor` | Under `Quorum`, a report from a leader still serving names a follower that may take over, whenever a majority is still replicating. A liveness property in invariant form, checked in `FelixShard.cfg`. |

## The configurations, and what each must do

`scripts/check_tla.sh` holds each configuration to a declared outcome. A
configuration that must find a violation exists to show a check is
load-bearing, or to pin a finding the design has not acted on yet; a violation
that quietly became a pass would be a model that stopped saying anything.

| Configuration | Knobs | Must |
| --- | --- | --- |
| `FelixShardLease.cfg` | drifting clocks, no writes: heartbeats, lapses, promotions | pass `AtMostOneServing` and `NoStaleCommit` (0.78M distinct states) |
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
| `FelixShardElectHandoff.cfg` | `FelixShardElect.cfg` with a planned move and a cancel, every change fenced (`FenceEveryChange`), one generation of elections, one write, reports flowing so a move can start | pass `OneLeaderPerGeneration`, `AckedHeldByLeader`, `AckedAgree`, `AckedOnMajority`, `NoTruncationBelowHwm` (48.7M distinct states, 61 min on a four-core CI runner; nightly) |
| `FelixShardElectHandoffLeaders.cfg` | the same without the write, for every pull request | pass `OneLeaderPerGeneration` and the rest, which hold trivially without a write |
| `FelixShardElectHandoffUnfenced.cfg` | `FelixShardElectHandoffLeaders.cfg` with the cut-over opened without the fence, no writes and no cancel | violate `OneLeaderPerGeneration`: a candidate opens at the generation after the control plane's, and the control plane cuts over to the destination at that same generation |
| `FelixShardSuspicion.cfg` | `FelixShardFencedAck.cfg` with placement promoting at the first generation before the lease lapses (`Suspicions = 1`), on any read, as it does on the followers' word; promotion from the leader's report, time standing still, so no lease lapses in a run | pass `AckedHeldByLeader`, `AckedAgree`, `AckedOnMajority`, `NoTruncationBelowHwm` (not yet measured) |
| `FelixShardSuspicionLease.cfg` | `FelixShardFencedPromotion.cfg` with the same early promotion: acknowledgements on the report and the lease, time to 4 | violate `AtMostOneServing`: the old leader still serves on its lease after the new one opens |
| `FelixShardFencedAckAnyKept.cfg` | `FelixShardFencedAck.cfg` with a spare fourth broker outside the replica set (`Spares`), promotion of any replica however far behind (`Promotion = "any"`), and the promotion keeping the replica set, the old leader in it; no drift | pass `AckedHeldByLeader`, `AckedAgree`, `AckedOnMajority`, `NoTruncationBelowHwm` (0.48M distinct states, depth 31, 27 s on four workers; with `Drift = 1`, 39.0M distinct states in 35 min on four workers, by hand) |
| `FelixShardFencedAckAnyReplaced.cfg` | the same with the promotion swapping the old leader for the spare (`ReplaceOnPromote`), as failover's `choose_replicas` would | violate `AckedHeldByLeader`: the new leader and the spare are a majority of the new set and open without the record the old leader and the third replica acknowledged |
| `FelixShardFencedAckSeat.cfg` | `FelixShardFencedAckAnyKept.cfg` with one follower replacement (`MaxMoves = 1`): a spare joins beside a leaving follower at one generation, counting toward the quorum, and the leaving one goes at the next, once the newcomer holds what a majority of the set held when it joined (`SeatHoldsCopy`); one write, `L = 2`, time to 3, no start records | pass `AckedHeldByLeader`, `AckedAgree`, `AckedOnMajority`, `NoTruncationBelowHwm` (2.20M distinct states, depth 27, 90 s on a four-core CI runner) |
| `FelixShardFencedAckSeatLonger.cfg` | the same with start records, `L = 4` and time to 4 | pass, the same invariants (34.9M distinct states, depth 37, 36 min on a four-core CI runner; nightly) |
| `FelixShardFencedAckSeatEarly.cfg` | the same with the seat not waiting, as placement did when a report named the newcomer caught up before anything was counted at the joining generation | violate `AckedHeldByLeader`: a record acknowledged on the leader and the leaving follower, the newcomer seated without it, and the next leader fencing the newcomer and the lagging follower |
| `FelixShardFencedAckGrow.cfg` | `FelixShardFencedAckSeat.cfg` with a set a failover left short of the replication factor: two brokers hold the shard and the third is a spare, and `Grow` lets placement add the spare with nobody leaving (`GrowSet`), then seat it, as a restore does. A copy joining the even set counts toward every majority that acknowledged, so a failover keeps it (`Counted`), and a follower is replaced only beside an odd set; one move, one write, `L = 2`, time to 3 | pass `AckedHeldByLeader`, `AckedAgree`, `AckedOnMajority`, `NoTruncationBelowHwm` (604K distinct states, depth 25, 21 s on a CI runner) |
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
| `FelixPlacementPacing.cfg` | `FelixPlacementPacing.tla`: moves and follower replacements across four shards, two copies at once, one per node, one planner | pass `CopiesWithinLimit` and `FencedNeverTimesOut` (313 distinct states) |
| `FelixPlacementPacingUncountedReplacement.cfg` | the same with a follower replacement invisible to the count, as it used to be written | violate `CopiesWithinLimit` |
| `FelixPlacementPacingTwoPlanners.cfg` | two planners over three shards and one slot, the lease changing hands at any step, one also reading without it as an operator does; every start fenced by the placement token | pass `CopiesWithinLimit` and `FencedNeverTimesOut` (56K distinct states) |
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

Drift is checked where it matters and nowhere else. The lease configurations
carry drifting clocks and no writes, so every interleaving of three drifting
clocks is affordable; most replication configurations carry writes and
synchronised clocks. `FelixShardRealMargins.cfg`, `FelixShardAckWithoutLease.cfg`
and the fenced pair carry both, bought with one write and time to 5, or two
writes and one promotion.

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
code this model describes (`services/felix-broker-service/src/{cluster/lease,cluster/membership,replication,serving,shards/lifecycle}`
and `services/felix-controlplane-service/src/{api/nodes/reports,cluster/membership,cluster/placement}`,
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
replacement's `joining` -- and that only a move before its fence times out.
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
`Ballots = FALSE` and `Elections = 0` in every other configuration leave the
model, and its state space, exactly as they were. So does
`FenceEveryChange = FALSE` everywhere but the two `FelixShardElectHandoff`
configurations.

Ballots are not enough on their own. `FelixShardElectStaleSet.cfg` replaces a
follower and lets the one that left stand on the set it last knew. The old
set's majority never promised the generations the new set used, so no ballot
is in the way, and TLC finds it opening without a record the new set
acknowledged. A fence that carries the generation of the set it counts, and a
replica that refuses one built on a set older than one it knows, is what
self-election still needs. Nothing in the broker elects yet, so this pins the
finding rather than a defect.

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
the one it serves opens unfenced, as `Regenerate` does in the model, where
only a follower replacement reaches it; that is not yet checked with
elections: `FelixShardElectStaleSet.cfg` fails first on the stale set.

### Failover on the followers' word

The control plane promotes once a dead leader's lease has lapsed and the
margin has passed. Once the fleet acknowledges by its followers and confirms
reads by a round, nothing a deposed leader does depends on the lease, so
placement may promote as soon as a majority of the set says it cannot reach
the leader (`Suspicions`). A detector can be wrong, so the model lets
placement promote on any read up to that generation, with the old leader
alive, serving and renewing. `FelixShardSuspicion.cfg` keeps every
acknowledged record that way. `FelixShardSuspicionLease.cfg` does the same
where the lease still decides who serves, and TLC finds two brokers serving,
which is why placement acts on a suspicion only once both `majority_ack` and
`lease_free_reads` are finalized.

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

## Which configurations run where

Every configuration registered in `scripts/check_tla.sh` runs on every push
and pull request that touches the code or the spec: the `formal` job in
`.github/workflows/ci.yml`, split across seven jobs by each configuration's
measured time so that each stays well under its hour.

Six configurations are too big for that hour, and run nightly instead, each in
its own job with a two-and-a-half-hour limit (the `exhaustive` job in
`.github/workflows/tla-walk.yml`): `FelixShardElectHandoff.cfg`,
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
300 steps long instead, in its simulation mode. They run nightly, not per push
(`.github/workflows/tla-walk.yml`).

A walk shows less than a check. It is sampling: a pass means the walks found
nothing in their budget, not that nothing is there, and uniform random choice
under-samples rare interleavings. What it adds is depth: invariants checked on
behaviours far longer than any exhaustive configuration reaches. Each family's
negative twin must find its violation within the same budget, which shows the
walks are long and varied enough to lose a record at all. A twin checks only
the invariant it expects, so the first violation it finds is that one.

The spec admits spares only without handoff and cancel, so there is one walk
per family:

| Configuration | Knobs | Must |
| --- | --- | --- |
| `FelixShardWalkSpares.cfg` | `FelixShardFencedAckGrow.cfg` and `FelixShardFencedAckSeat.cfg` together: two replicas and two spares, the set grown to three, then followers replaced and seated; promotion of any replica, start records, `L = 2`, drifting clocks | pass `AckedHeldByLeader`, `AckedAgree`, `AckedOnMajority`, `NoTruncationBelowHwm` |
| `FelixShardWalkSparesSeatEarly.cfg` | the same with the seat not waiting (`SeatHoldsCopy = FALSE`) | violate `AckedHeldByLeader` |
| `FelixShardWalkMoves.cfg` | follower acks with planned moves: the first from a staged destination, any fenced move open to the operator's cancel, failovers that never name the destination; start records, `L = 2`, drifting clocks | pass `AckedHeldByLeader`, `AckedAgree`, `AckedOnMajority`, `NoTruncationBelowHwm` |
| `FelixShardWalkMovesDestination.cfg` | the same with a failover free to name the destination | violate `AckedHeldByLeader` |
| `FelixShardWalkHandoff.cfg` | the broker as built under the lease (`FelixShardFencedPromotion.cfg` with start records) with `FelixShardHandoff.cfg`'s move; `FelixShardRealMargins.cfg`'s margins and drift, writes acknowledged on commit | pass every invariant and `AckedHeldByLeader` |
| `FelixShardWalkHandoffNoWait.cfg` | the same cutting over without waiting for the drained report | violate `AtMostOneServing` |

The walks carry no `SYMMETRY`: simulation stores no states, so it buys nothing.
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
