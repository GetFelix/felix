# Rebalancing: from partial to done

The status table listed online rebalancing as partial. This was the plan for
finishing it: what was wrong with moves beyond the known gaps, and the order the
work landed in. Each phase was one pull request and useful on its own. All of
it has landed; the status row now reads done, with the edges listed under
[What is left](#what-is-left).

## How a move works

The control plane moves a shard in steps (`cluster/placement/moves.rs`):

1. **Stage.** The destination is added to the shard's replicas and the leader
   starts shipping it the log.
2. **Fence.** Once the destination is within the lag bound (phase 4), the
   assignment goes `draining`. The old leader stops serving the shard but
   keeps shipping.
3. **Drained.** The old leader reports that the shard's log has stopped growing.
4. **Cut over.** The control plane names the destination leader at a new
   generation.

Moves start when a broker is drained (`POST /v1/nodes/{id}/drain`) or when a
broker leads more than its share of shards.

Every assignment write bumps the shard's generation, including the fence. So the
old leader sees a fence as a new, draining generation of a shard it already
holds, not as a release of the current one. Anything that has to happen when a
broker stops serving a shard must handle both paths.

## What was wrong beyond the known gaps

The status row named four gaps: publishes refused during the switch, subscribers
ended rather than migrated, one move at a time as the only pacing, and no limit
on the copy's bandwidth. Reading the code turned up correctness problems that
come first.

- **An acknowledged write could be lost.** Admission checks that the broker
  serves the shard, but the write then waits in a queue and was committed
  without checking again. A write queued across the fence could commit on the
  old leader after it reported drained, and the new owner never received it.
  Cache writes, counter adds, consumer-group writes and forwarded writes had
  the same gap. "Drained" was a guess: the log tail holding still for two
  passes.
- **Group positions, dead letters and counters were not waited for.** They are
  copied to the new owner after the drained report, and nothing waits for the
  copy. A dead-letter entry or counter add that had not arrived was lost.
- **Two control-plane instances could undo each other's steps.** Every instance
  runs placement, and assignment writes did not check the generation they were
  planned from. A stale fence could hand a shard back to its old leader after
  the new one had acknowledged writes.
- **Readers on the old leader went silent.** Nothing ended a moved shard's
  subscriptions or cache watches on a broker that stayed up, so a client had no
  reason to look for the new owner.
- **Follower replacements were not counted against the move limit.**
- **A move may never reach the fence under steady writes**, because the
  destination has to be exactly level with the tail.
- **The staged destination counts toward the quorum** while it is still
  copying, so a `Quorum` publish can wait for the whole copy.

The switch-over also takes seconds rather than milliseconds, because three
broker loops poll the control plane every 2 s and placement runs every 5 s.

## Phases

| Phase | What it does | Status |
| --- | --- | --- |
| 0 | Correctness: the write fence, waiting for group state and counters, conditional assignment writes, ending readers on a moved shard | done |
| 1 | Fast switch-over: wake the loops instead of polling, long-poll the assignment feed, run placement when a report arrives, warm the destination, publish routes and servable shards together | done |
| 2 | No refused publishes: hold a publish to a moving shard briefly and forward it, a typed "shard moving" refusal the client retries, the destination not counted toward quorum while it copies | done |
| 3 | Subscriptions follow the shard: a final frame telling the client where to resume, and the client resuming there with no gap or duplicate | done |
| 4 | Pacing: count every copy in flight, a per-node limit, drains before rebalancing, start the fence within a lag threshold, a move timeout, a bandwidth limit on copies | done |
| 5 | Operator controls: list, start, cancel and pause moves over the API and a CLI | done |
| 6 | Idempotent producers keep their sequences across a planned move | done |
| 7 | Docs and the status row | done |
| 8 | A broker hands its shards off before it stops (`FELIX_SHUTDOWN_HANDOFF_TIMEOUT_MS`) | done |

Load-aware placement (moving shards by load rather than by count) is separate
work with its own status row.

### Phase 0: correctness

- **Conditional assignment writes** (merged, #660). A placement write lands only
  if the assignment is still at the generation it was planned from, in memory,
  Postgres and Raft. `FelixShardStalePlanner` and `FelixShardStalePromotion`
  show the race without the check.
- **Readers end when their shard moves** (merged, #664). A broker ends a shard's subscriptions
  and cache watches whenever it stops serving it, after what was already queued
  for them. Phase 3 builds on this: the last frame says where to resume, and
  the client follows.
- **The write fence** (merged, #664). Every write enters a per-shard fence right before it
  claims its place in the log and stays counted until it is durable. The fence
  closes when the broker stops serving the shard, and a write reaching it after
  that is refused with the refusal its path already had. The drained report
  waits for the fence to be closed and empty, so it is exact.
  `FelixShardHandoffNoClaimFence` loses an acknowledged write without it.
- **Group state and counters** (merged, #665). The drained report also waits until the
  group-cursor, dead-letter and counter logs are level on the destination.

### Phase 1: fast switch-over

- The shard watch wakes the routing feed, and the feed wakes the replication
  driver, instead of each waiting for its next tick.
- The assignment change feed long-polls (`wait_ms`), so a broker hears about a
  fence or cut-over as soon as it is written. Old control planes ignore the
  parameter.
- A drained report, or one showing the destination caught up, wakes placement.
- A broker named as a shard's destination prepares to serve it while it is
  still copying, so taking over is quick.
- Routes and the set of servable shards are published together, so there is no
  moment where one is updated and the other is not.
- Metrics for how long a move takes and how long its switch-over lasts.

The target is a switch-over well under a second on a local cluster.

Progress:

- **Control plane** (merged, #662, #665). The change feed long-polls, a
  caught-up or drained report runs placement at once, and the assignment
  carries `successor`.
- **Broker.** The watch long-polls the feed (20 s) and falls back to the sync
  interval against a control plane that answers at once. The watch wakes the
  routing feed; the feed wakes replication after acting on a change, so the
  drained report goes out on the pass right after the fence. A broker named
  as `successor` opens the shard's log and in-memory state while it is still
  copying. The feed publishes routes and the servable set in one swap. The
  destination records `felix_broker_shard_move_seconds` and
  `felix_broker_shard_switchover_seconds`.
  `a_move_switches_over_in_well_under_a_second` measures fence to first
  accepted publish with every broker on the 2 s default interval: about
  90 ms on a local debug build, against 8 s before.

### Phase 2: no refused publishes

- **Hold and forward.** A publish that reaches any broker while its shard is
  between the fence and the cut-over waits, before it is accepted, until the
  broker's routes show the new owner, then goes there. A forwarded publish
  that reaches the old leader, or a new leader whose routes are behind the
  requester's, waits there the same way. A publish now takes its place in the
  write fence when it is routed rather than when it is claimed, so one routed
  just before the fence lands and the move waits for it, instead of being
  refused at its claim. Bounded by `FELIX_SHARD_MOVE_HOLD_MS` (2 s) and
  `FELIX_SHARD_MOVE_HOLD_MAX` (1024 waiting). Cache, counter and group
  operations were held the same way afterwards; see the last phase below.
- **A typed refusal.** Past either bound the publish is refused with
  `shard_unavailable`, reason `moving`, retry class `retry` and a
  `retry_after_ms` hint. There is no separate feature bit: `moving` is a new
  reason under an existing code, and a client that does not know it still
  reads the retry class. A client without error codes gets the refusal it
  always did.
- **The destination does not count toward the quorum while it copies.** A
  leader that saw the destination added leaves it out of the quorum mark, and
  ships the copy in 50 ms slices so a pass never waits for the whole copy.
  `FelixShardStagedMove` and `FelixShardStagedMoveSingle` pass;
  `FelixShardStagedMoveVotes`, which counts it, finds a `Quorum` write held on
  the stream's own majority and waiting for the copy.
- A `Quorum` stream placed with one replica is acknowledged by its leader.
  Nothing ships for such a shard, so the quorum wait used to read the missing
  mark as a lost leadership and refuse every publish.

`continuous_publishing_through_a_move_is_never_refused` runs two publishers
through a move and sees no refusal and no record lost or stored twice.

### Phase 3: subscriptions follow the shard

- **The frame.** A broker that stops serving a shard ends its subscriptions
  and cache watches with `shard_moved {subscription_id, resume_from?,
  node_id?, addr?, generation}`, after everything it committed has fanned out.
  Only a client that offered `FEATURE_SHARD_MOVED` (`0x1000`) gets it; any
  other sees the stream end byte for byte as before.
- **An exact `resume_from`.** For a durable stream it is the replay ring's next
  sequence, read under the log lock that each publish captures its fanout list
  under. So every record below it was offered to the subscriber (delivered, or
  dropped by its queue) and none at or above it was. It is a stream position,
  not the subscriber's: a queue that dropped records does not move it. An
  in-memory stream sends none, since its sequence means nothing elsewhere.
- **The client resumes at `max(last delivered + 1, resume_from)`.** A record
  fanned out just before the move can still arrive, putting `last + 1` past
  `resume_from`; records the queue dropped stay dropped. Nothing is repeated
  and nothing is skipped that would otherwise have been delivered.
  `ClusterClient::subscribe` returns a `ClusterSubscription` that does this on
  its own; an in-memory stream resumes at the new owner's tail.
- **Where to go.** `node_id` and `addr` are the successor during a move, else
  the new leader. The frame goes out at the fence, before the cut-over, so the
  client tries the named broker first, then the entry broker (which redirects),
  retrying with backoff until the new owner takes the subscription or the
  reconnect deadline (30 s without one) passes.
- **Cache watches.** `resume_from` is the shard log's tail once the writes in
  flight have landed, and absent if they had not; the watcher then resumes
  after the last change it saw. `ClusterClient::watch_cache` returns a
  `ClusterCacheWatch` that hands out `CacheWatchItem::ShardMoved` as a notice
  and reopens on the new owner at `max(offset after the last change,
  resume_from)`; the cache log moves with the shard and keeps its offsets, so
  nothing is repeated or skipped. A sharded watch follows each shard the same
  way. A `Client` watch still ends with the frame.
- **Sharded subscriptions** follow each shard and report
  `ShardEvent::ShardMoved`. The Python and TypeScript bindings wrap
  `ClusterClient`, so their subscriptions follow too, and they surface the move
  as `ShardMoved` / `CacheWatchShardMoved` (Python) and `shardMoved` (Node).
- **Two ways the last frames were lost** in the broker's subscriber writer,
  both fixed. Deliveries queued in the same batch as the subscriber's
  unregister were dropped; the writer now writes them first. And the feeder
  forgot the subscriber's connection before its last deliveries were routed,
  so one still in the lane queue found no connection; the lane now forgets it
  after handling the unregister. The final frame rides on the unregister,
  which is sent with backpressure, so a full lane cannot drop it.

Evidence: `routing::subscriptions_follow`
(`a_subscription_follows_its_shard_to_the_new_owner`) reads a durable stream
from its start while a publisher keeps writing and the shard moves, and checks
every offset arrives once, in order, with every acknowledged record.
`routing::watches_follow` does the same for a key watch and a sharded prefix
watch against cache writes, without the caller reopening either. The
conformance runner checks the frame on the wire, offered and not.

### Phase 4: pacing

- **Every copy counts.** A follower replacement on a draining node used to
  swap the new follower in with nothing in the assignment saying a copy was
  running, so it escaped the move limit. It now names the follower being
  copied in (`joining`), keeps the one it replaces until the new one is
  within the lag bound, and holds a slot until then.
  `FelixPlacementPacingUncountedReplacement` shows the limit broken without
  the count.
- **A per-node limit**, `FELIX_SHARD_MOVES_MAX_PER_NODE`, on copies into or
  out of one broker, counting both ends.
- **Drains go first.** Slots go to drains, then to rebalancing, then to
  shards whose last move timed out. Within a drain, the draining broker's
  leaderships go before its follower copies are replaced.
- **The fence starts within a lag bound.** The replica report carries the
  leader's tail, and a move fences once its destination is within
  `FELIX_SHARD_MOVE_FENCE_MAX_LAG_RECORDS`. The write fence and the drained
  report already wait for the remainder, so nothing is lost by not waiting for
  exactly level. A follower the leader cannot reach is left out of the
  report's offsets so it is never fenced on a stale position.
  `a_move_completes_while_a_publisher_keeps_writing` moves a shard under four
  writers.
- **A move timeout**, `FELIX_SHARD_MOVE_TIMEOUT_MS`. A staged move or a
  replacement past it is dropped in one write and its slot goes to the next
  move. A fenced move is finished rather than timed out: going back means a
  new generation and clients following the shard twice, and going on waits
  for at most the lag bound's worth of copy. An operator can still take one
  back (phase 5).
- **A bandwidth limit**, `FELIX_SHARD_MOVE_BYTES_PER_SEC`, a token bucket per
  leader over its copies to move destinations, applied only to a destination
  the quorum does not need and never to the remainder after the fence.
- `max_shards` is compared against roles, not leaders, when choosing a
  destination.
- **The limits hold across instances.** Each write was conditional on its
  own shard's generation only, so two Postgres-backed instances, or a pass
  and an operator's request on another instance, could each read one free
  slot and start moves on two different shards. Now one instance runs the
  timed passes, the holder of a lease in the store (three reconcile
  intervals, renewed every pass, released on shutdown; under Raft, the
  leader), and every placement write is also fenced by a placement token
  kept beside the lease: read before anything a pass or request decides
  from, advanced by every placement write and every change of holder, and
  checked under the same lock or log entry as the write. A pass woken by a
  report still runs wherever the report arrived, which keeps the switch-over
  fast and is safe for the same reason. `FelixPlacementPacingTwoPlanners`
  passes with two planners and an operator; `FelixPlacementPacingUnfenced`
  breaks the limit without the token.
  `two_instances_cannot_both_take_the_last_move_slot` runs against memory,
  Postgres (two pools over one database) and Raft;
  `an_ex_holders_pass_is_fenced_after_a_takeover` and
  `an_expired_lease_is_taken_over_and_fences_the_old_holder` cover an
  instance that paused past its lease.

### Phase 5: operator controls

- **Seeing moves.** `GET /v1/shard-moves` lists each move and follower
  replacement in progress: its step (staged, fenced, replacing), why it
  started (`move_reason` on the assignment: drain, balance, operator,
  replace), when, and the destination's lag from the leader's latest report.
  `GET /v1/placement/plan` runs a pass over a fresh read and says what it
  would write, without writing it.
- **Starting a move.** `POST /v1/shard-moves` names a shard and a
  destination. It is decided like a placement step and refused where
  placement would not make it, including at the move limits, then runs like
  any other move.
- **Cancelling.** Before the fence the destination is dropped, as a timeout
  does. After it the fenced leader serves again at a new generation
  (`retake`): nobody has led since, so its log holds every accepted write,
  and writes still inside its write fence land in that log. Held publishes go
  to it once its routes show it serving, and subscriptions ended with
  `shard_moved` look for the destination, are redirected, and resume at
  their offset. Nothing changed on the broker for this: reopening a shard at
  a new generation after a draining one was already the path a cut-over back
  to the leader takes. After the cut-over a cancel is a 409.
- **Conditional, and decided again.** Every operator write lands only at the
  generation and placement token it was decided from; on a conflict the
  request is decided again from a new read, so a cancel racing a cut-over finds nothing to cancel.
  `FelixShardCancel` passes with writes acknowledged on admission;
  `FelixShardCancelStalePlanner`, with the cancel written unconditionally,
  serves the shard on two brokers.
  A retake keeps the leader's log and so the producer sequences in it:
  `FelixShardCancelResend` re-sends writes across the cancel and none is
  stored twice.
- **Pausing.** A switch in the store (`placement_settings`, a Raft command,
  memory) that every instance's placement reads each pass. Paused, placement
  starts no move or replacement of its own, drains included. Moves in flight
  finish, since a fenced leader has already stopped serving; an operator can
  cancel one, and can still start moves, which is how shards are moved by
  hand without placement moving others meanwhile. New shards and failovers
  are not moves and still happen.
- **A command line.** `felix-controlplane admin moves | plan | move | cancel
  | pause | resume`, a client of the API with plain tables or `--json`.

`routing::operator_moves` runs these against real brokers: an operator's
move completes; a staged move cancelled leaves the shard where it was; a
fenced move cancelled with a publisher and a following subscriber running
hands the shard back with every acknowledged record delivered once and in
order; and a paused placement leaves a draining broker's shard alone until
resumed.

### Phase 6: idempotent producers keep their sequences

A move's destination, like a promoted replica, used to know no producers: the
sequences were in the old leader's memory, so the first batch a producer sent
after the move was refused as `unknown_producer`, and a re-send of the batch in
flight could not be told from a new one.

The sequences are now in the log. Each record of a producer's batch is stored
with a mark naming the producer and sequence (storage format v3), the marks
are shipped with the records, and every broker derives each producer's place
from its own log: on each append, and on open from a snapshot saved at each
rollover plus the active segment recovery scans anyway. So the destination
answers the re-send from the records it was copied, before it takes its first
write, and a failover and a restart get the same answer by the same means.
There is no separate channel, and nothing for the drained report to wait on:
the marks are in the records the move already waits for. This also closes
#608, the same gap on a failover.

A batch the old leader stopped partway through is finished by the re-send
rather than written again. A producer is remembered while any of its batches
is in the log, so retention decides when one is forgotten, on every replica
alike.

Evidence: `clients::idempotent` in `felix-cluster`:
`a_producer_keeps_its_sequence_across_a_planned_move` and
`a_producer_keeps_its_sequence_when_its_leader_dies` re-send the last batch to
the new leader after a drain and after a kill, see it answered without a
second copy, and carry on, and
`a_producer_publishing_through_its_leaders_death_loses_and_repeats_nothing`
kills the leader while a producer is mid-stream and finds every record once,
in order. All three failed before, with `unknown_producer`. The
TLA+ configurations `FelixShardIdempotentHandoff` and
`FelixShardIdempotentFailover` pass `NoDuplicate`; their `Memory` companions,
with the sequences in the leader's memory, violate it.

Cost: opening a 244 MiB shard in 17 segments with 1000 producers takes the
same time with the snapshot as an unmarked shard (about 20 ms on a local
release build; `producer_state_open`), and about 100 ms when the snapshot is
missing and the sealed segments are read instead. Durable publish throughput,
plain and idempotent, is unchanged within run-to-run noise
(`idempotent_throughput`).

### Phase 7: docs and the status row

The status row moved to done, citing the tests above. Pages written by
separate phases were read against each other and against the code: the
control-plane record, the scaling page, the semantics pages and the README
still described publishes refused and readers ended rather than followed, and
now do not. The scaling page gained a sequence diagram of a move.

### Phase 8: handoff on shutdown

A clustered broker told to stop now drains itself before it closes its
listener (`node/handoff.rs`). Readiness goes off first, so no new client is
sent to it; it asks the control plane for the drain with its own credential,
keeps accepting and serving while placement moves its shards, and carries on
with the old shutdown once it leads nothing or
`FELIX_SHUTDOWN_HANDOFF_TIMEOUT_MS` (30 s) has passed. It skips the handoff
when it leads nothing, when no other broker is eligible, and when the control
plane does not answer within 5 s, and a second signal ends the wait, so the
handoff can delay a shutdown but never hang one. The drain ends with the
process: registering again makes the broker live, as it always did.

It waits for leaderships only. A broker being restarted is coming back to its
follower copies, and replacing them would copy every shard once per restart.
For the same reason a drain's leaderships now get move slots before its
follower replacements; with one slot, a replacement's copy used to be able to
hold it while the leaders waited (`a_draining_leader_is_moved_before_its_follower_is_replaced`).

`routing::shutdown_handoff` sends SIGTERM to the leader of a replicated shard
under a publisher and a subscriber: placement writes move steps and no
placement, no publish is refused, the subscriber sees every acknowledged
record once and in order, and the broker exits cleanly. Before the change the
same test saw a failover and refused publishes. It also restarts a broker that
handed off and sees it lead again, and stops a lone broker without waiting.

Two more tests cover the ends of that bound. With sixteen `Quorum` shards
(three copies, per-message acks) on four brokers and a 1.5 s timeout, the
stopping broker hands some shards off and still leads others when it gives
up; those fail over once it exits, and every acknowledged record is read back
from the new leaders exactly once, with no gap in any shard's offsets
(`a_handoff_that_times_out_loses_no_acknowledged_record`). A lone broker
stopped under per-message acks with `FELIX_ACK_ON_COMMIT` off (acked once
queued) has every acknowledged record on restart
(`a_lone_broker_that_stops_keeps_what_it_acknowledged`). The claim is for
`Quorum`: a `Leader` stream acknowledges before a follower has the record, so
a shard that fails over can lose what its leader acknowledged last.

The first of those found two bugs. A broker that took a shard over, by
failover or by a move, replayed only the first replicated batch and then the
live edge: its replay ring was filled when that batch opened the stream and
never touched again, though the records were on its disk. The ring is now
emptied as replicated records move the tail
(`records_replicated_after_the_first_batch_replay_from_the_start`). And the
client required publish acks in request order, while the broker answers each
as it completes; a `Quorum` publish answered after the one behind it failed
every publish on that stream (`acks_answered_out_of_order_reach_their_own_requests`).

It also failed now and then with a shard that never failed over after the
broker exited. The broker closed its outbound peer connections, which
replication ships over, while its peer listener could still append forwarded
publishes, and it kept reporting afterwards. A record that reached only the
leader then left a last report naming no caught-up follower, and the control
plane, which promotes only such a follower, left the shard unplaced for good.
The broker now stops its peer listener first, waits (for at most half of the
remaining drain deadline) until each shard it leads has a follower level with
it, stops replicating, and only then closes the pool
(`a_stopping_leader_ships_its_tail_before_it_stops_replicating`, which cuts the
followers off for the last write and heals them during the drain).

### Holding cache, counter and group operations

Every cache and counter operation now goes through the same hold as a
publish (`resolve_cache_route` calls `dispatch_write`), and takes its place in
the write fence there, so one routed just before the fence lands and the move
waits for it. The owner of a forwarded one waits the same way
(`ForwardingHandler::apply_cache_op` settles before its ownership check, and
again after a fence refusal), then applies it or answers `NotLeader`, which
the requester follows. The hold cannot count an add twice: it waits before
anything is applied, and a forwarded add whose answer is lost is reported as
indeterminate rather than re-sent, as before.

Group operations are not forwarded; a poll's claims and the acks for them
belong on the broker that leads the shard. So a group operation is held at
the broker it reached and, once the move cuts over, answered with `NotLeader`
naming the new owner, which `ClusterClient::group_sharded` follows. The
new owner leads only after the drained report, and that waits for the group
cursors and dead letters to be on it, so a held ack applies on top of the
copied state and is never applied on the old leader as well: the fence
refused it there. A poll that is waiting for work when its shard leaves
answers empty rather than with an error. `handoff::writes_of_every_kind_through_a_move_are_never_refused`
runs cache puts and deletes, counter adds, publishes and a group's polls and
acks through a move: nothing is refused, every acknowledged cache write reads
back, the counter equals the acknowledged adds, and the group finishes every
record once. Without the change it sees hundreds of refused cache writes and
counter adds and a few refused group operations.

The test also found writes refused before any fence. A drain made the
draining broker ineligible, and brokers read "eligible" as "may forward to",
so every write through another broker to a shard still waiting its turn to
move was refused as `owner_unavailable`. The node listing now says
`routable` separately (live or draining, heartbeat inside the window), and
brokers forward on that.

The model needed nothing new: a held operation has not been admitted, which
`FelixShard.tla` already allows for any write, and the logs that ride the
shard are modelled as writes to the one log.

### A group's in-flight state across a move away and back

What a group has handed out and not yet finished lives in the leader's memory,
beside the durable cursor. It belongs to one term of leading the shard. A
broker that led it, moved it away and got it back used to reopen with the
tracker from its first term, so after A -> B -> A it handed out again records
the group had acknowledged on B (#685).

The lifecycle now marks an open as a new term when the broker was not serving
the shard just before: released, fenced, or failed. Before that open, the
shard's trackers are dropped (`GroupReader::reset_shard`), and the next group
operation rebuilds them from the cursor log that came back with the shard.
Reopening a shard the broker is still serving keeps them. Every assignment
write bumps the generation, so that covers staging a replica, and the fence
itself, which reaches the old leader as a new draining generation rather than
a release. Clearing on every open would redeliver what is in flight at each of
those.

Not serving is the signal rather than a gap in generations, because the
lifecycle sees a coalesced set of assignments, and several writes to one shard
between two reads are normal. A cancelled move that retakes the shard after
its fence resets too; nobody else led in between, so that costs a redelivery
of what was in flight and nothing more.

`handoff::a_shard_moved_away_and_back_hands_out_nothing_already_acked` acks on
A, finishes everything on B, and moves the shard back: without the change A
hands out offset 11, which B had finished.

### Following the group redirect

Progress: done. `ClusterClient` now has the single-shard group calls
(`group_poll`, `group_poll_wait`, `group_ack`, `group_nack`,
`group_dead_letters`, `group_discard`, `group_redrive`), which follow
`NotLeader` the way `group_sharded` did and remember each shard's leader;
`group_sharded` uses the same code. The Python and TypeScript clients' group
calls go through them, so a consumer on either keeps working when its shard
moves. `consumer_groups::a_cluster_client_follows_a_group_redirect_to_the_leader`
polls and acks through a broker that does not lead the shard; with the calls
sent to the connected broker, as the bindings did, the poll fails with
`NotLeader`.

A redirect to a draining broker now carries its address. The catalog keeps a
second list of client addresses for every routable broker, used for
redirects, moved-reader handoffs and the publish-ack owner hint; `topology`
still offers only eligible brokers, so new clients are not pointed at a
broker on its way out. `node_catalog::tests::a_redirect_to_a_draining_broker_carries_its_client_address`
fails without it with no address in the redirect.

## What is left

- **A plain `Client` does not follow a group redirect.** It is one broker's
  connections, so it returns `NotLeaderError` and leaves connecting to the
  owner to `ClusterClient`. This is by design and documented, not a gap to
  close.
- **A dead lease holder pauses the timed passes** until its lease expires,
  three reconcile intervals (15 s by default). Moves in flight carry on and
  woken passes still run, but a failover waiting on the timer waits that
  long. A holder that shuts down releases the lease and costs nothing.
- **A shutdown handoff is bounded.** What a stopping broker still leads when
  `FELIX_SHUTDOWN_HANDOFF_TIMEOUT_MS` runs out fails over. For a `Quorum`
  stream that loses no acknowledged record; for a `Leader` one it can, as
  any failover can. A durable unreplicated shard has nothing to fail over
  to and is unavailable until the broker restarts. Moves are paced like any
  other, so a broker leading many unreplicated shards needs a longer timeout
  (and grace period).
- **Load-aware placement** is separate work with its own status row.

## Checking the work

Each correctness fix comes with a test that fails without it: a unit test for
the mechanism, and where the behaviour only shows across processes, a
`felix-cluster` test against real brokers. Changes to the replication, lease or
placement code come with a TLA+ configuration that passes, and where the change
closes a hole, a companion configuration that shows the hole
(`docs/formal/README.md`).
