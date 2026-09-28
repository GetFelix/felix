# Shard replication and leader failover

**Decision: leadership is a time-bounded lease issued by the control plane, and
replication is log shipping from the leader to its followers. Per-shard Raft is
rejected.**

Recorded for M5.1 (#110). The rejected alternative and what would overturn the
decision are both below.

## What has to be true

Constraints first, because the comparison is only meaningful against them.

**Safety.** At most one broker may commit writes for a shard at a given epoch,
under any combination of partition, pause, and clock error the failure model
admits. A record acknowledged under `Quorum` must survive any failure the
configured majority tolerates.

**Availability.** A shard survives its leader failing. It also survives the
control plane being briefly unreachable — a broker must not stop serving because
a metadata service restarted.

**Write latency.** A publish already crosses ingress, offset assignment,
durability, and fanout in a fixed order. Replication adds one network round trip
for `Quorum` and none for `Leader`. It must not add a *consensus* round trip to
the common path.

**Operational.** A broker holds many shards. Whatever runs per shard runs
hundreds or thousands of times per broker.

**Dependency.** The storage layer is fixed. Its invariants are load-bearing and
documented, and a replication design that violates them is not a replication
design, it is a storage rewrite.

## The constraint that decides it

`docs/storage-format.md` and `docs/durable-storage.md` state it plainly:

> **Records are never rewritten**, which is what lets recovery keep trusting
> "valid bytes end at EOF".

That single property is why torn-tail repair is sound, why a missing index can
be rebuilt, and why preallocation reserves blocks without changing `st_size`.

**The invariant is narrower than "never rewritten", and it has to be.**

An earlier version of this document argued that Felix avoids truncation
entirely, because a leader only ships records it has committed. That is false
under `Quorum`, where shipping is what *produces* the commit: a follower stores
a record before any majority holds it, so a leader that dies mid-flight leaves
that record on some followers and not others. The next leader may legitimately
reuse the offset. Every leader-change scheme has to reconcile that, and Felix is
not an exception — `Divergence::Conflict` exists precisely because it happens.

The invariant that actually holds, and that recovery depends on, is:

> **No record at or below the high-water mark is ever rewritten.**

Below the mark a record is on a majority and can never be un-committed. Above it
a record is a proposal, and discarding a proposal the cluster did not adopt is
not the same act as rewriting history. Torn-tail repair and interior-corruption
detection rest on the *committed* prefix being immutable, which this gives them.

So the case against per-shard Raft is not about truncation, because truncation
is required either way. It is:

1. **Election and clock trade-offs.** Raft elects by term and majority vote,
   which needs no clock assumption but adds a round of voting to every failover
   and a second failure detector beside the one the control plane already runs.
   Leases reuse the heartbeat the broker already sends, and pay for it with the
   safety interval described below.
2. **Two logs, or one.** Raft over the stream log means either the segment log
   *is* the Raft log — with Raft's index and term bookkeeping in the record
   format — or every record is written twice and the two can disagree after a
   crash. The second is a durability path the storage design deliberately does
   not have.

Neither is a tuning problem. Both are a different storage layer.

The storage layer was in fact built anticipating the other answer:
`docs/durable-storage.md` says "Replication (M5) is what `seal`'s checksum and
`read_range`'s bounded paging exist to serve." Sealed-segment checksums and
bounded range reads are the primitives of *shipping*, not of consensus.

## Why the control plane being Raft-backed does not settle this

M7 makes control-plane metadata highly available, and Raft is the right tool
there. It does not follow that payload replication should use Raft, and the
issue asks for this to be said explicitly.

The two have opposite cost profiles:

| | Control-plane metadata | Stream payload |
| --- | --- | --- |
| Volume | Assignments, tenants, streams — kilobytes, changing rarely | The entire data plane |
| Groups | One, for the cluster | One per shard: hundreds or thousands per broker |
| What consistency buys | Linearizable reads of small state everyone must agree on | Durability of records already ordered by a single writer |
| Cost of a round trip | Paid once per metadata change | Paid on every publish |

A stream's records are already totally ordered by their leader. Consensus is not
being asked to *establish* an order — the log has one. It is being asked to
replicate an existing order durably. That is a weaker requirement than Raft
solves, and paying Raft's price for it is paying for agreement that has already
happened.

## The selected design

### Leases

The control plane grants a **lease** on `(shard, generation)` to one broker for a
bounded duration `L`. The lease is the authority to serve; the assignment alone
is not.

A lease is bound to the generation. A new generation is a new lease, never a
renewal of the old one — which is what makes generation the epoch that fences
writes.

The leader renews through the heartbeat it already sends. A renewal that does not
arrive before expiry means the lease lapses; nothing revokes it explicitly,
because a revocation cannot be delivered to a partitioned broker, which is
precisely the case that matters.

### The exact condition under which a broker may accept a write

A broker may accept a write for shard `S` if and only if **all** of:

1. It holds a lease for `S` at generation `G`.
2. Its own monotonic clock reads earlier than `expiry(G) − ε`, where `ε` covers
   clock drift and the time between this check and the record reaching disk.
3. Its local shard state for `S` is open at exactly generation `G` — the check
   `IngressRouter::dispatch` already performs.
4. For a `Quorum` stream, a majority of the replica set for `G` **including the
   leader** has durably stored the record. For a `Leader` stream, the leader's
   own configured durable commit point has been reached.

Conditions 1–3 are checked **twice**: once when the request is admitted, and
again immediately before the record is committed to the log. The second check is
not redundant. Everything between them can take arbitrarily long — a full ingress
queue, a slow fsync, a VM pause — and a lease that was valid on admission may
have expired by the time the bytes reach the disk. Fencing only at the routing
boundary leaves exactly the window this design exists to close.

**As built, the commit check is one gate.** Every write path already enters the
shard's write fence right before it claims its place in the log: a direct or
idempotent publish, a forwarded publish on the owner, a cache put or delete, a
counter add, a consumer-group ack, nack or dead-letter change, and a Kafka
produce. The fence checks the generation (condition 3) and reads the lease
against the clock (conditions 1 and 2), so no path can skip either. A write
that took its place at admission and waited in a queue has the lease read
again when it claims its offsets. While the lease has lapsed the fence refuses
everything, retryably, and serves again once a heartbeat renews it.

For a `Quorum` write, condition 4 is followed by the lease once more, when the
mark releases the acknowledgement: a broker whose lease lapsed while it waited
answers "unknown" rather than acknowledging, which is `AckChecksLease` in
`FelixShard.tla`. The model says that check is not what keeps an acknowledged
record: `FelixShardAckWithoutLease.cfg` drops it, lets the leader act on its
report's answer as the code does, and with clocks drifting under the real
margins TLC finds leaders acknowledging on a lapsed lease, and after being
replaced, and no acknowledged record lost. What does keep it is the report:
a leader acknowledges only on a report stored for its own generation, and
promotion reads that generation's report.

The lease still decides whether a deposed leader can write at all. The model
has the design that takes the clock out of `Quorum` safety, which the broker
does not implement yet: a promoted leader persists its generation on a
majority and takes any tail ahead of its own before it serves, and a write is
acknowledged once a majority holds it at the current generation. The fence is
built, and a promoted leader uses it whenever every replica offers it; the
acknowledgement still waits on the report and the lease. See
[Fencing a promotion](#fencing-a-promotion).
`FelixShardFencedAck.cfg` keeps every acknowledged record with no margin on
either side of the lease; `FelixShardUnfencedAck.cfg`, the same without the
fence, loses one. See [`docs/formal/README.md`](formal/README.md).

The commit check refuses a publish even when it was acknowledged on enqueue
(`ack_on_commit` off), since writing it would be the split brain. So admission
reads the clock for such a publish and, with little lease left, waits for the
write instead of acknowledging the enqueue. A pause after the ack can still
strand one; it is counted in `felix_broker_acked_publishes_dropped_total`. See
`docs/semantics.md`, "Consistency".

### Failover

1. The lease lapses (no renewal within `L`).
2. The control plane selects an eligible replica: one whose durable high-water
   mark is within the configured catch-up bound of the last known committed
   offset.
3. It publishes assignment generation `G+1` naming that replica as leader, and
   grants it a lease.

The control plane must not publish `G+1` before it is certain no broker can still
believe it holds `G`. With lease duration `L` granted at control-plane time `t`,
`G+1` may be granted no earlier than `t + L + margin`, where `margin` covers
clock drift between the two parties and the network delay of the grant itself.
The leader stops at `t + L − ε` by its own clock. The gap between those two
instants is the safety interval, and it is why leases are safe without
synchronized clocks.

The broker cannot observe `t`, so it anchors at the instant it **sent** the
heartbeat — always at or before `t`, so the round trip comes out of its own
lease rather than out of the margin. Anchoring at the instant the *response* was
handled runs the other way: a buffered read or a VM pause in between would push
the lease past `t + L`, which is the safety interval being spent by the same
kind of stall the twice-checked conditions above exist to survive.

The safety interval is the whole mechanism, so it is worth seeing:

<p align="center">
  <img src="assets/leader-lease.svg" alt="A lease on one time axis: broker A stops accepting at expiry minus epsilon by its own clock, the control plane grants the next generation no earlier than expiry plus a margin, and the gap between them is a safety interval in which no broker is leader" width="900">
</p>

```mermaid
sequenceDiagram
    participant A as Broker A (leader at G)
    participant CP as Control plane
    participant B as Broker B (replica)

    CP->>A: lease(shard, G) until t+L
    Note over A: serving

    A--xCP: renewal lost (A fails, or is partitioned)

    Note over A: at t+L-ε by A's own clock,<br/>A stops serving
    Note over CP: waits until t+L+margin<br/>before granting again

    CP->>B: assignment G+1, lease until t'+L
    Note over B: serving

    Note over A,B: the gap between A stopping and B starting<br/>is the safety interval: no epoch has two leaders
```

Nothing in that sequence requires A and CP to agree on the time. It requires only
that neither clock runs more than `ρ` faster or slower than real time, so the two
instants cannot cross.

As built, `L` is the control plane's `expiry_timeout_ms`, which every heartbeat
response carries. The broker gives up `ε = L/4`. The control plane marks a
silent broker down, which is what lets placement hand its shards on, only after
`L + margin`, where the margin is `FELIX_NODE_REGRANT_MARGIN_MS` and defaults to
`L/4` (it may not be set lower). Silence is measured twice and both have to
agree: against the store's clock, which stamps heartbeats, and by the sweeping
instance's own monotonic clock, which starts over whenever it sees the stamp
change and whenever the instance starts watching (a restart, a Raft election, or
the store coming back). The store's clock is a wall clock, and after an election
or a database failover a different machine's, so a step forward alone could
otherwise expire a broker still inside its lease. `FelixShardRealMarginsLease.cfg`
checks these margins, a quarter each side against clocks that drift by a
quarter, and passes; with the control plane's margin at zero it finds two
brokers serving. `FelixShardRealMargins.cfg` adds a `Quorum` write carried
across a promotion under the same margins and drift, and every acknowledged
record survives.

### Fencing a promotion

The lease keeps a deposed leader out only while clocks drift within the bound
the margins assume. The fence is the part of the modelled design that does not
need that: a promoted leader asks each replica to take its generation before it
opens for writes, and a replica that has taken it refuses every older leader.
Any majority that could acknowledge a record overlaps the majority that took
the fence, so the deposed leader cannot find one. `FelixShardFencedAck.cfg`
checks the design with no margin on either side of the lease.

The replica's side is `AnswerFence` in the model. It persists the generation,
fsynced, in the same file that keeps the highest generation it accepted a
leader at, before it answers. From then on it refuses the older leader's
records, bootstraps, rebuilds, and gap answers, restarts included, whatever its
routing view says. The shard's cursor, dead-letter and counter logs check the
shard's own log as well as their own. It answers with how far its log reaches,
its commit offset, and the generation its last record was written at, which is
how the model orders logs (`Ahead`).

It is negotiated, not assumed. A broker offers the `FENCE` capability in the
peer handshake, and a peer that did not offer it is never sent `Fence`
(`docs/internal-protocol.md`, "Capabilities"). `FELIX_INTERNAL_FENCE=false`
withdraws the offer.

**The leader's side** is `OpenForWrites`. A broker promoted to lead a stream
shard opens its log, persists its own generation, and then waits in a
`fencing` phase: the shard is routed to it but not servable, so writes are
refused retryably and nothing ships. Replication sends `Fence` to every
replica at once and opens the shard on the first majority, the leader
counted. Before it opens, the leader takes the log of the answer furthest
ahead by (generation of the last record, length), when that is ahead of its
own: it reads it with `ReplicateFetch` from where the two may disagree (the
later of its commit offset and the start of its own last generation), drops
its own records past the first disagreement, and appends the rest. Where this
leadership begins is recorded only then, so the records it took keep their
own generation. Answers that arrive after the majority are not waited for,
as in the model.

What makes the deposed leader harmless is the majority, not the clock. The
follower it can still reach may never have heard of the promotion from the
control plane, and its routing view would take the batch; the fence it took
refuses it (`a_partitioned_leader_is_refused_by_the_majority_its_successor_fenced`,
with the old leader cut off past its lease and its clock slowed a
hundredfold, and the same frozen).

When the fence applies:

- Only on a promotion. A move's destination takes over from a leader that
  drained into it, and a cancelled move hands the shard back to the leader
  that had it; neither is fenced, and the model does not fence them either.
- Only for stream shards. A cache shard's log opens lazily and is compacted
  underneath, and keeps the lease.
- Only when every replica in the new set offers both `FENCE` and
  `TAIL_FETCH`, as its latest handshake with this broker in either direction
  says, and this broker offers them too. Otherwise the shard opens at once
  on the lease, exactly as before (`a_mixed_fleet_fails_over_on_the_lease`),
  and a peer that did not offer them is never sent either message.
  `felix_broker_promotions_opened_total{path}` says which path each promotion
  took.

The cost is availability. A promoted leader that cannot reach a majority of
its replicas does not serve, where on the lease alone it would have opened;
it retries every 200 ms. It could not have acknowledged a `Quorum` write
without that majority anyway, but a `Leader` write it would have.

**What this does not change yet.** Acknowledgements still come from the report
and the lease (`AckByFollowers` is off), so a deposed leader's `Quorum` write
is kept unacknowledged by the report gate as before; the fence adds the
followers' refusal on top. A `Leader` stream acknowledges on the leader's own
commit, which no follower sees, so its deposed leader is still kept out by the
lease alone. Acknowledging on a majority at the leader's generation, and
serving reads without a lease, are later changes.
`FelixShardFencedPromotion.cfg` checks this configuration, the fence with the
report and the lease, under the real margins.

### The clock assumption, stated precisely

Safety requires a bound on clock **drift rate**, not synchronized clocks. For any
two nodes, over a real interval `T`, each monotonic clock advances by between
`T(1−ρ)` and `T(1+ρ)` for a known `ρ`.

This is a real assumption and it can be violated. The realistic violation is not
NTP error — it is **process suspension**: a VM migration, a stop-the-world pause,
a throttled container. A broker suspended past its expiry wakes believing it
still holds a lease.

Host or VM suspend is the worst case, because it can stop the clock itself:
`CLOCK_MONOTONIC` does not advance while the system is suspended, so a lease
timed on it would wake with its whole remainder intact. On Linux the lease
therefore reads `CLOCK_BOOTTIME`, which counts suspended time
(`cluster/lease/clock.rs`); other platforms fall back to `std::time::Instant`.
Timers that only schedule work stay on tokio's clock; every validity check reads
the lease clock.

That is exactly why condition 2 is re-checked at the durable-append boundary
rather than only at admission. A suspended broker's next check is after it wakes,
and it sees an expired lease before its bytes reach disk. A suspension *between*
that check and the write completing is bounded by `ε`, which is the parameter to
tune, and the residual risk to state honestly rather than claim away.

### Replication

The leader ships records to followers over the internal transport that already
exists (#105). Under `Quorum` it ships them *before* they are committed — that
is what makes the majority — so a follower can hold a record no majority ever
acknowledged, from a leader that then died.

A follower therefore truncates, but only **above** the high-water mark. Below it
a record is on a majority and is never discarded: it was ordered by a leader
holding an unexpired lease, and the safety interval means no other leader existed
at that generation. The follower learns the mark from the leader: under `Quorum`
every batch carries the leader's quorum mark (`ReplicateRecords.commit_offset`),
and the follower keeps the lower of it and the end of what the batch left level
with the leader. The leader keeps its own mark the same way, so a deposed leader
is held to it when it follows.

A follower also refuses a leader older than one it has already accepted. The
routing view answers that while it is current, but it is rebuilt after a restart
and may lag; so each shard log keeps the highest generation it accepted a leader
at, written and fsynced before the first batch at a new generation is stored or
acknowledged, and a leader claiming a shard records its own generation the same
way. The commit offset is kept in the same small file (`replica`, beside the
segments). Under `FsyncMode::OnCommit` it is on disk before the batch that
carried it is acknowledged. Under `Periodic` and `None` it is written at most once
a second, and after a crash it may read back behind, which weakens the guard:
truncation may cut into records committed since, until the next batch carries
the offset again. Above it, a record is a proposal that the cluster may not
have adopted, and a new leader reusing the offset is ordinary rather than
alarming.

Reconciling that needs the two sides to agree on where their histories diverge,
which is what the generation of each record establishes — see
[Divergence and truncation](#divergence-and-truncation) (#406).

Catch-up for a new or lagging follower is a bounded `read_range` from the leader,
with sealed-segment checksums to verify wholesale rather than record by record —
the primitives the storage layer already exposes for this purpose.

**This works only while the leader still holds what the follower is missing.**
Once retention has trimmed past a follower's position, shipping cannot reach it:
the records are not on the leader to send, and starting the follower at the
surviving base offset would leave its log with a hole nothing downstream could
detect. Such a follower is offered a log that *begins* at the leader's oldest surviving
offset (`ReplicateBootstrap`). A replica holding nothing takes it and replication
resumes; one holding records of its own refuses, because a log placed over them
would have a hole nothing downstream could detect, and it is halted. The leader
then rebuilds it under the policy below, or leaves it to an operator when the
policy says so.

### Idempotent producers across a leader change

An idempotent producer's sequence has to be wherever the shard's leader is, or
a re-send after a failover or a move lands a second time, or is refused as
`unknown_producer` with the producer left not knowing whether its batch
landed. It is not state beside the log: each record of a producer's batch is
stored with a mark naming the producer and the sequence (see
`docs/storage-format.md`, "Producer marks"), and the marks are shipped with
the records (`ReplicateMarkedRecords` in `docs/internal-protocol.md`). A
broker's producer state is derived from its own log, on every append and on
every open, so it is the same on any replica that holds the same records:

- **A promoted replica or a move's destination** knows every batch it was
  shipped. A re-send of one is answered with where it landed; the next
  sequence is appended. Nothing is sent at promotion, because there is nothing
  to send.
- **A restarted broker** rebuilds the state as it opens the shard, before the
  shard takes a write, from a snapshot saved at each rollover plus the active
  segment that recovery scans anyway.
- **A batch cut short** — its leader died while writing or shipping it, so the
  replica holds only its first records — is finished by the re-send: the
  missing records are appended, and the batch is not written twice. If
  anything else was appended after it first, it can never be finished, and the
  re-send is appended whole.
- **A replica that disagrees with the leader** about a record's mark has
  diverged, and is repaired or halted like one that disagrees about its bytes.
- **Truncation** takes the batches it removes out of the state with them.

What is remembered is a function of the log. A producer is known while any of
its batches is in the log; once retention removes the last of them it is
forgotten on every replica, and its next batch is refused as
`unknown_producer`. So is one evicted as the least recently written of more
than 4096 on a shard, and one whose batches all precede the base a follower
was bootstrapped or rebuilt at. `unknown_producer` after a leader change now
means the history is genuinely gone, not that the leader changed. The client
ends the producer on that stream and reports it rather than starting again
under a new id, because whether the batch in flight landed is exactly what
nobody can say any more.

An in-memory stream has no log and keeps sequences in its leader's memory, so
they last as long as the leader.

`FelixShardIdempotentFailover.cfg` and `FelixShardIdempotentHandoff.cfg` check
that a re-send never stores a write twice across a failover or a move;
`FelixShardIdempotentFailoverMemory.cfg` and
`FelixShardIdempotentHandoffMemory.cfg`, with the sequences in the leader's
memory, find the promoted broker storing it again.

### The `Leader` loss window, precisely

For `ConsistencyLevel::Leader`, a record is acknowledged once it is durable on
the leader alone. Records acknowledged but not yet shipped are lost at **any**
failover, not only when the leader's storage is lost: the promoted replica
writes new records at those offsets, and when the old leader returns as a
follower its unshipped suffix belongs to a previous generation and disagrees
with the new leader, so it is truncated (see
[Divergence and truncation](#divergence-and-truncation)). Losing the disk is
one way to reach that; a lease lapse is another.

The window is bounded by the leader's **replication lag**: the byte range between
its durable high-water mark and the lowest follower's acknowledged mark. It must
be exported as a metric, because a bound nobody can observe is not a bound. An
operator choosing `Leader` is choosing this window, and must be able to see how
large it currently is.

`Quorum` has no such window: a majority including the leader holds every
acknowledged record, so any failure within the configured majority preserves it.

### Replica reports and the committed mark

A `Quorum` acknowledgement is only as good as the report failover will read, so
the leader moves its quorum mark only once the control plane has **stored** a
report naming the replicas that hold the records. The report endpoint answers
each shard separately, in request order: `accepted`, `stale`, `not_leader`,
`unassigned` or `future_generation`. The status is 200 when every shard was
accepted and 409 when any was not. The leader moves a shard's mark only when
that shard's entry says `accepted` and names the shard and generation it sent.

Stored reports only move forward. A report replaces the held one at a later
generation, or at the same generation with a leader tail at least as far along
(`ReplicaReport::supersedes`, applied the same way by the memory, Postgres and
Raft stores). A slow request can otherwise land after a newer one and put back
a view in which a follower that has since fallen behind still looks caught up.
A report from a generation older than the assignment's is refused as stale.

A restarted leader whose unsynced tail was lost can report a lower tail than
the one held for the same generation; those reports are refused until its tail
passes the old one, and its `Quorum` publishes wait meanwhile. That is a
liveness cost, not a safety one: the held report describes records the
followers do have.

Across versions: a broker that predates per-shard answers treats 409 as the
whole batch failing and holds every mark in it for a pass, which is safe. A
new broker talking to a control plane that predates them gets 204 and treats
the batch as stored, which is the old behaviour and the old exposure, until
the control plane is upgraded.

**Readers stop at the committed mark.** On a `Quorum` shard with replicas, the
quorum mark at the leader's generation is the committed high-water mark:
consumer-group polls hand out nothing at or past it, Kafka `Fetch` returns
nothing past it and reports it as the high watermark and last stable offset,
and `ListOffsets` latest is the mark. Before the first mark of a generation it
is zero. `Leader` streams, shards placed without replicas and single-node
brokers are unbounded, as their commit point is local durability.

QUIC subscriptions are gated too. A durable batch past the mark joins a
per-stream hold under its commit turn, so the hold is in offset order; when the
mark moves, the driver releases what it covers, appending to the replay ring
and fanning out together, with one shared envelope as an unheld publish does.
The ring therefore holds only committed records. A restart keeps that: a
`Quorum` ring is refilled from disk only when the commit offset reaches the
tail, and otherwise starts empty, since a ring stopping at the commit offset
would leave a hole before the tail. `Latest` and `cursor_tail`
are the mark, and resumed history is read with `Broker::read_committed`, which
stops at the mark and waits for it. Registration still happens before any
history is read. A broker that just took a shard has no mark for its
generation yet (`Settling`): a `Latest` subscribe is refused as retryable until
the first mark. Held batches are dropped when the shard is released (never
delivered as committed); subscribers are ended with where to resume and pick
up on the next leader. While the lease is lapsed the bound is `Refused`: new
subscribes, watches and cache reads are refused (`felix_broker_lease_refusals_total{boundary="read"}`),
and the readers of every led shard are ended as for a move.

### Divergence and truncation

Two brokers can hold different records at the same offset. It takes a leader
dying mid-flight under `Quorum`: the record reached some followers, no majority
acknowledged it, and the next leader reuses the offset for its own. Nothing is
wrong with either broker.

Finding it is the easy half and is done: a batch's overlap with what a follower
already holds is compared byte for byte, and a mismatch is
`Divergence::Conflict`. A follower answers with the end of the batch it was sent
rather than its own tail, so the leader cannot resume past records neither side
has compared (#406).

Repairing it needs the two to agree on where their histories part, and offsets
alone do not say — both logs have an offset 100, and being told "they differ at
100" does not say how far back the agreement goes. The generation does say,
because a generation belongs to exactly one leader: the highest generation both
brokers hold is the last one they cannot disagree within, and its end offset on
the leader is the furthest point the follower can keep.

So a follower records where each generation began in its log, as a small map
beside the segments. It then repairs itself, with no exchange at all: a
conflict is droppable when the batch that found it comes from a **newer**
generation than the one this follower last accepted, and the divergence sits at
or after where that older generation began. Both conditions matter — a leader
disagreeing with *itself* is an inconsistency rather than a predecessor's
leftovers, and repairing that would let a leader rewrite its own history.

Dropping a suffix also resets the stream's in-memory tail: the replay ring,
its next offset and the commit order. Storing the new leader's records only
moves that tail forward, so when they end below where the dropped ones did, the
ring kept the dropped records, and a reader of this broker once promoted was
handed them next to the records that replaced them. A rebuild resets it the
same way.

Anything else halts, as before. That is the same shape Kafka arrived at without
Raft (KIP-101, KIP-279), reached without adding a message: the generation is
already on every batch, and the follower's own history supplies the rest.

Not exchanging it is deliberate, and the reason has since narrowed. It used to
be that a new kind could not be sent to a peer that might not understand it: an
unknown kind ended the stream, and those streams are long-lived lanes carrying
every in-flight request, so a probe cost far more than it learned. A peer now
steps over a kind it does not know and refuses that one frame, so the cost is no
longer prohibitive — but it is still a round trip, and repairing from what a
follower already knows needs none, along with no negotiation and no
rolling-upgrade order. An older peer predating that change still drops the
stream, so a probe would also have to wait out a deployment.

What it gives up is the case where the follower's history is absent or does not
reach back far enough. Those halt, which is exactly today's behaviour.

The same map answers a second question, on the leader's side: **where a fresh
cursor starts.**

A cursor is a belief about a follower's position under one leadership, so a
generation change discards it. What replaced it was offset zero, which meant
every follower of every shard the failed broker led byte-compared the whole log
before anything new could move — the leader reading its own log off disk and
pushing records the follower already had. On a log of any size that turns a
failover into an outage, and it happened for every shard at once.

The leader records where *its own* generation begins, which until then only
followers did — leaving a broker's history with a hole over exactly the stretch
it led. It is recorded when the shard is taken, while it is still `Opening`:
that is the one moment the tail *is* the generation's start, because the phase
exists precisely to hold writes back until recovery finishes.

Below that offset, this broker's records were taken from earlier leaders while
it was a follower, and so were the follower's — two prefixes of the same log
agree. At or above it is where they can differ: what this leadership wrote, and
what a predecessor left on the follower alone. So comparison starts one record
below the boundary, so the first batch overlaps something the follower already
holds and the boundary is checked rather than assumed — the same check Raft
makes at `prevLogIndex`. A follower further behind than that still says so with
a `LogGap`, and the leader rewinds in that one exchange.

The follower holds the other end of that check, because the leader's starting
point is only a guess about the follower: a batch can still begin at the
follower's tail with records it wrote under an older generation sitting just
below. Accepting it would leave those records uncompared, and one of them may
be a dead leader's unacknowledged write at an offset the new leader filled
differently: two brokers disagreeing at a committed offset. So a batch from a newer generation that begins past them is
answered with a `LogGap` naming the first uncompared record — the later of
where the older generation began here and the commit offset. The leader rewinds
and the overlap is compared like any other, so a conflict is found and repaired
as above. When that takes several batches, the follower remembers in memory how
far it has got, and it records the new generation's start only once it is level
with the leader, so a conflict found partway is still the older generation's
suffix and repairable. A restart forgets the progress and costs a re-compare,
not correctness.

Without a history entry for the generation it falls back to zero, which is slow
rather than wrong. A shard's consumer-group cursors, dead letters and counters
still start there: those logs are written only when group state changes, so the
comparison is over almost nothing, and the leader does not open them at takeover
to record against.

Three things this deliberately does not do:

- **It does not go in the record format.** A generation per record would mean a
  segment format bump, and `SegmentHeader::decode` rejects an unknown version
  outright rather than guess — by design, since a moved field produces a
  plausible mis-parse. The map is derived state that can be rebuilt or absent.
- **It does not truncate below the high-water mark.** Everything there is on a
  majority. A truncation point computed below it is a bug, not a repair, and
  refuses rather than proceeds: the follower keeps the records, answers with
  the conflict, and counts `felix_broker_replicated_total{outcome="below_commit"}`.
  The storage layer refuses the cut on its own as well
  (`StorageError::BelowCommit`), so no caller can get round it.
- **It does not make a halted follower repair itself.** Truncating a divergent
  suffix is a decision with a policy attached — how many followers may rebuild
  at once, and at what bandwidth — so the repair is the leader's, under that
  policy, and described in "Rebuilding a halted follower" below.

### Who may be promoted

A leader reports, on every replication pass, which of its followers hold every
record it may have acknowledged. The leader is the only party that can say: it
knows both its own tail and how far each follower has acknowledged, where a
follower knows only where it is.

The bound is **zero** — a follower is caught up when it is missing nothing a
client was promised. What that covers depends on the stream. Under `Leader` a
write is acknowledged before it ships, so it is the leader's whole log. Under
`Quorum` it is the log up to the offset a majority holds, which is the most the
mark sent with the report can release; the mark already out is a floor. The
tail would be wrong there: a publish landing between shipping and the report
leaves every follower one record short of it, the report names nobody, and a
leader that dies right then can never be replaced, because nothing else will
report on the shard again. During a move the leader reports against its tail
under either level, since the cut-over hands over an exact copy.

A bound above zero is a bound on how much a promotion may silently lose, and
there is no honest value for it that is not a policy decision; zero needs no
such decision, and a follower reaches it constantly on a healthy shard.

Reports expire, after the node expiry timeout plus one heartbeat. A report says
a follower *was* caught up; the leader kept writing afterwards, and promoting on
a stale report loses whatever was written since. The window is derived from the
liveness settings rather than configured separately, because it has to outlive
exactly one thing: the time it takes to notice the leader is gone.

A halted follower is never reported, however close its last position was. It has
stopped rather than fallen behind.

The last report is the one promotion reads, so a leader that stops on purpose
has to make it a good one. A stopping broker stops taking forwarded writes,
ships until each shard it leads has a follower level with it, and only then
stops replicating and closes its peer connections
(`node/shutdown.rs`, `Replication::caught_up`). The other way round, a record
written after the connections closed, or not yet shipped when they did, left a
last report naming no follower, and the shard never failed over.

**The report cannot be allowed to trail the acknowledgement.** On its own this
rule is not enough, and a model check shows why: if the report travels after the
acknowledgements it describes, a leader that reports two followers level, then
acknowledges a `Quorum` write held by only one of them, then dies, leaves the
control plane a fresh report naming the other — and promoting it loses the
acknowledged record. Report expiry does not close it; the report is recent, it
is just older than the acknowledgement.

What closes it is ordering. The leader reports who holds the record and waits
for that report to land *before* moving the quorum mark, and the mark is what
releases the acknowledgement — so the control plane cannot be behind a client.
A report that does not land leaves the mark where it was, and the publish waits
rather than being acknowledged on a report nobody received.

Both halves are checked. `docs/formal/FelixShard.tla` explores 5.38M distinct
states of the implemented design without violating `AckedSurvive`, and
`FelixShardNoReportOrder.cfg` — the same design with the ordering removed —
loses an acknowledged record in a second (`task tla:check`). Promotion then
prefers the replica furthest ahead among those reported, with score only
breaking ties. See [`docs/formal/README.md`](formal/README.md).

**If no replica qualifies, the shard is left unplaced.** The alternative is what
the code used to do: fall back to ordinary scoring and hand the shard to
whichever node scores highest, which may never have seen it. That broker then
serves an empty log at a newer generation while the records sit on replicas that
were not chosen — a failover that *is* the data loss, and one nothing downstream
reports as one. Unavailable is visible and recoverable; silently empty is
neither.

The same holds for a durable stream that never asked for replication. Its
leader holds the only copy, so the shard stays assigned to it, unplaced, until
it returns (`Unplaceable::OwnerUnavailable`). Placing it elsewhere would be the
same silent empty log. An operator who would rather lose the records than wait
abandons them explicitly (`POST /v1/placement/abandon/...`, see
[control-plane.md](control-plane.md#operator-controls)); that is the only path
by which a durable shard is placed on a broker that does not hold its log. An
in-memory stream has no log to lose and is placed again at once.

Positions are held in the control plane's memory. They change constantly, are
advisory, and expire in about a second, so persisting them would cost a write
per report for data that is worthless by the time it could be read back. The
consequence is that a second control-plane instance starts knowing nothing and
cannot promote until leaders have reported to it — which matters for M7's
multi-instance work and not before.

## Failure model

| Situation | Behaviour |
| --- | --- |
| Leader fails | Lease lapses; a caught-up replica is promoted at `G+1` after the safety interval. Unavailable for at most `L + margin + promotion`. |
| Leader partitioned from the control plane | Keeps serving until its lease expires, then stops. The lease runs from the last accepted heartbeat, so with the defaults that is 5 to 11 s into the partition; a partition shorter than that costs nothing, a longer one costs availability, not safety. Serving resumes on the first heartbeat accepted afterwards. Silent past the expiry window, the broker is marked down and registers again once it can reach the control plane. |
| Leader partitioned from followers | `Quorum` writes fail — correctly, the majority is unreachable. `Leader` writes succeed and accumulate loss-window exposure, which the lag metric shows. |
| Control plane unavailable | No new leases are granted. Existing leases run to expiry (5 to 11 s with the defaults), then shards go unavailable. Deliberate: granting without a functioning authority is how split-brain happens. When it comes back, brokers renew within about 3 s. The expiry sweep waits one expiry window after a restart, a Raft leader change, or regaining its store, so the outage does not mark the fleet down. |
| Broker suspended past expiry | Refused at the durable-append check on waking. |
| Stale broker after reassignment | Its lease has expired, so it refuses. This is what closes #239 by construction rather than by racing a watch. |

## What is implemented so far

`#111` builds the leadership half:

- **Leases**, renewed by the heartbeat, with the duration taken from the control
  plane's expiry window. Checked at admission (cheap, cached) and again at commit
  (authoritative, reads the clock).
- **Replica sets**, chosen by the same score as leadership so the whole set is a
  deterministic function of the shard and the cluster. `replication_factor`
  defaults to 1, so a stream that never asked for replication is unchanged.
- **Promotion**, gated on a caught-up follower.

The gate matters more than the promotion. A replica that holds no log can be
promoted perfectly well and will then serve an empty shard — the failover *is*
the data loss. So promotion requires a follower within the catch-up bound.

Leaders now report which followers hold everything they do, so the gate has real
input and promotion fires: a lost leader is replaced by a replica that holds the
log, in around a second on a local three-node cluster.

Failover works: a lost leader is replaced by a replica that holds the log, and a
quorum-acknowledged record is readable from the replacement.

Getting there needed five separate fixes, and the common thread is worth
recording. A `Quorum` acknowledgement is a promise about *which brokers hold a
record*, and every one of these was a way for the cluster's own account of that
to drift from the truth:

- the commit order was not rebased when records arrived by replication, so the
  first write a promoted broker accepted never completed
- a stream raised to `Quorum` kept acknowledging on the leader alone until the
  broker restarted, because the live stream state was never updated
- the leader's tail was read before shipping and used after, so a follower level
  with the *old* tail was reported caught up for a record it did not have
- the acknowledgement was released before the control plane was told who held
  the record, so a leader could die having promised a client something the
  cluster could not act on
- promotion chose by placement score rather than by how much a replica held

Both halves of this are implemented (#112). The follower's side is the exchange,
the append rule, and the fence at the storing end. The leader's side keeps one
cursor per follower, ships bounded batches from its own log, and moves the
cursor only on the follower's answer — so a follower that has fallen behind or
been rebuilt is caught up by its own `LogGap`, with no separate negotiation and
nothing kept on disk.

Replication to a follower stops on `LogConflict` or `FencedEpoch`. Neither
converges by retrying: the first means the two logs disagree about bytes both
sides hold, the second that this broker is no longer the leader.

`Stream.consistency` is now wired into the acknowledgement path (#113). A
`Leader` publish is acknowledged once the leader's own durability policy is
satisfied, exactly as before. A `Quorum` publish is held until a majority of the
replica set *of the generation it was written at* holds its records durably.

The majority always counts the leader, so `replication_factor: 1` — the default
— makes `Quorum` behave exactly like `Leader` rather than never acknowledging.
A halted follower counts for nothing: it has stopped rather than fallen behind,
and letting its last position count would make an acknowledgement mean less than
it says.

The mark that releases such a publish advances **at the majority, not at the
last follower**. A pass ships to every follower at once and moves the mark as
soon as enough of them have answered to make one — with three replicas, the
moment the first follower has the records. Waiting for all of them put one dead
or slow replica's whole timeout in front of every acknowledgement on the shard,
every pass, which is the failure `Quorum` exists to tolerate rather than be
stalled by (#411). The rest of the set is still shipped to; what changed is when
the acknowledgement is released, not who gets the records.

**Nor does the next mark wait for a slow peer.** Releasing the first mark at
the majority was not enough while a pass still ended with its slowest follower
and every shard's pass ended together: the next record's mark waited for the
next pass, so one paused peer held every `Quorum` publish on the broker for a
dial timeout, even a move's destination that counts toward no quorum. So each
shard passes on its own, starting again as soon as its last pass ends. Once its
mark and report are out, a pass keeps waiting on the followers still answering
only until the shard's next pass is wanted, by an append or anything else that
wakes the driver; then it hands their exchanges to the driver. A shard being
handed over waits for everyone, since its fence already holds the writes and the
drained report needs the destination's answer. Until such an exchange ends,
its follower is not shipped to again and counts at the position it had when
the exchange began, a floor like any follower that has not answered yet. Its auxiliary logs wait too, so they do not
dial the same slow peer again. When the exchange ends, the cursor goes back and
the shard passes again if the follower moved; one that failed waits for the
next wake, so a peer that fails fast is not redialled in a loop. The report
still lands before the mark moves, and a shard promoted here still ships
nothing and moves no mark until a majority has taken its fence, so nothing here
changes what an acknowledgement rests on.

The replica report goes to the control plane **before** the mark is published,
and is awaited. Releasing the publish first leaves a window in which a leader
has told a client its record is on a majority and has told the control plane
nothing about which replica holds it — and a leader that dies in that window is
replaced by whichever replica scores highest, which may be the one that does not
have it. A report that did not land leaves the mark where it was, for the same
reason: the argument rests on the control plane knowing who holds the record, so
releasing on a failed report reaches the same window by another route.

The report is written to the control plane's **store**, not kept by the
instance that received it. That is the other half of the same argument: with
several instances over one database, the instance a report reaches and the
instance that later promotes need not be the same process, and a report held
only in memory was a position no other promoter could use — an
acknowledgement resting on it could not be made good at failover. See
[control-plane.md](control-plane.md#replica-reports).

That costs a control-plane round trip on the path of a quorum publish, which is
the price of the acknowledgement meaning what it says. One report per shard per
pass in the healthy case: the majority report already describes every follower,
because they finish together. A follower that answers late enough to move after
that report sends a second one, so a replica that is level does not look behind
— and so out of promotion — until the next pass.

**Reports are not one round trip each.** A flush takes every report queued at
that moment and sends them as one request, which the endpoint has always
accepted; reports arriving while that request is in flight go together in the
next one. So a pass shipping sixteen shards concurrently costs round trips
proportional to how long the control plane takes to answer, not to how many
shards this broker leads.

Group commit rather than a window, and for the reason `disk_log/sync.rs` makes
the same choice: a timer would add its own wait to a pass with a single shard to
report, which is the deployment least able to spare it on a `Quorum` publish.
Batches grow under load, which is when they are worth having, and an idle broker
waits for nothing. `felix_broker_replica_reports_per_request` says how well it
is working — one, on a broker leading hundreds of shards, means it is not.

A wait that runs out is reported as a failure, and the distinction matters: the
records *are* durable on the leader and may yet reach a majority. The broker is
not saying the write failed, it is saying it cannot vouch for it at the level the
stream asked for. `FELIX_PUBLISH_QUORUM_TIMEOUT_MS` sets the budget. Leadership
moving mid-wait ends it the same way, immediately, rather than running the clock
out on an answer that can no longer come.

A control plane that sends a consistency level this broker does not recognise is
refused rather than defaulted. Falling back to `Leader` would serve a stream the
operator asked to be quorum-replicated at the weaker guarantee, silently.

The `Leader` loss window is exported as `felix_broker_replication_lag_records`:
how far the slowest follower is behind, across every shard this broker leads. A
halted follower is excluded from it — it has stopped rather than fallen behind,
and `felix_broker_replication_halted` is where that shows.

That gauge is a bare **count**, and has to stay one: a label per shard is a
label per stream per tenant, which is unbounded by design in a multi-tenant
broker. So it answers "is replication healthy here" and nothing more, and the
only way to learn *which* replica had stopped was to grep for the warning
logged at the halt.

A halt does not resolve on its own — the follower is out of every quorum until
someone acts — so the broker also serves a listing beside the metrics, at
`GET /replication/halted` on `FELIX_BROKER_METRICS_BIND`. It names the shard,
the node, the generation, how far the follower had got, why it stopped, and
what to do about it, because the reason alone does not say whether the
follower's data is wrong or merely incomplete. It is a listing rather than a
metric, which is what lets it carry an identity: it is read on demand and its
size is the number of halted replicas, normally zero. A healthy broker answers
`[]` rather than 404 — "nothing is halted" and "this broker does not answer
that question" are different things to a dashboard.

Read-only, deliberately. That listener has no authentication of its own, so it
carries what is worth knowing and nothing worth doing. The rebuild is the
leader's, below, and an entry here clears once it has begun.

The rule and its refusals are in `docs/internal-protocol.md`.

### Rebuilding a halted follower

A halted follower is out of every quorum, and stays out until its copy of the
shard is discarded and rebuilt from the leader's. The leader does that itself,
because it is the only party that can: it knows the follower is halted, holds
the copy the majority agrees on, and already has the shipping path to send it.

The rebuild is one message. `ReplicateRebuild` names the shard, which of its
logs, and the leader's oldest surviving offset; a follower of that shard at
that generation discards the log — records, index, and generation history —
and answers that its new copy begins at the offset it was given. From there it
is an ordinary follower that far behind: shipping resumes at the base, and the
follower is counted as caught up when it reaches the tail, like any other. The
same fence applies as to storing records: a superseded leader cannot make a
follower discard anything, which is the most damage a stale leader could do and
the one thing the check most has to stop.

A follower also refuses a rebuild that would discard a record it holds below its
commit offset and at or above the leader's base. Those records were acknowledged
on a majority; a leader asking to replace them may not hold them, and this copy
may be the last. The halt stands for an operator, and the refusal counts as
`below_commit`. Records below the leader's base are gone from the leader
already, so a follower that is merely too far behind (`needs_bootstrap`) is
rebuilt as before.

It happens under a policy, because a rebuild is a full transfer of the shard,
and every halted follower at once — across every shard a failed broker led — is
how a recovery becomes an outage:

| Variable | Default | Meaning |
| --- | --- | --- |
| `FELIX_REPLICATION_REBUILD_MAX_CONCURRENT` | `1` | Rebuilds in flight at once, across every shard this broker leads. `0` rebuilds nothing: every halt is an operator's, as before. |
| `FELIX_REPLICATION_REBUILD_BYTES_PER_SEC` | `0` | Bytes per second a rebuilding follower is shipped at, per follower. `0` is unlimited. |

The cap is counted per leader rather than per cluster: a broker leading a shard
can only see its own followers. A slot is held from the follower's acceptance
until the leader finds it level, and given back if the follower refuses or the
shard changes generation under it. The rate paces only followers being rebuilt;
a follower merely behind is shipped at full speed, as before.

Only a `diverged` or `needs_bootstrap` halt is rebuilt. A `fenced` halt says
this broker is no longer the leader, and nothing it ships is authoritative. A
follower that predates the message answers with an error, and stays halted
until it is upgraded or an operator acts.

`felix_broker_replication_rebuilds_total{outcome}` counts rebuilds `started`,
`completed`, and `refused`; `felix_broker_replication_rebuilding` is how many
this broker has in flight. The halted listing drops an entry when its rebuild
begins, since the follower is shipping again.

### Planned handoff

Everything above is about a leader that is *gone*. A leader that is alive and
must give a shard up — its node is draining, or it holds more than its share —
needs a different fence. The lease cannot be it: the lease is per node, the
node keeps heartbeating, and a revocation that has to reach the old leader is
the thing the lease design exists to avoid depending on.

The fence is the assignment itself. The control plane writes the shard
`draining` at a new generation, and the broker's rule for a draining
assignment is that it never serves it: a shard already active at that
generation is released in place, one that arrives draining is opened only so
the log is recovered, and either way it lands `closed` and stays there however
often the assignment is re-delivered. The next assignment, the one that names
the successor, is not written until the old leader has said it stopped.

Saying so rides the replica report. The leader keeps leading for replication
while draining — the followers are caught up from it, and the successor is
one of them — and reports `drained` once no write can land any more. That
needs more than closing admission. Admission checks ownership, and a write it
lets in can then wait in a publish queue for as long as the queue is deep;
a tail that has not moved for a while says nothing about a write still
queued. So every write — a publish, a forwarded publish, a cache put or
delete, a counter add, a consumer group's poll, ack, nack or dead-letter
change — enters a per-shard write fence at the moment it claims its place in
the log, and stays counted until it is durable and fanned out. The shard
lifecycle closes the fence as soon as it sees the move, whether the move
arrives as a draining copy of the served generation or as a new, draining
generation, and before the new servable set is published. A write that
reaches its claim after that is refused, the same way a publish to a shard
this broker does not serve is refused. A publish acknowledged when it is
queued rather than when it is written — the default, `ack_on_commit` off —
cannot be refused later, because the client already holds the ack. So a
publish enters the fence earlier still, when it is routed, and holds its place
until it is written: the move waits for it rather than losing it, whichever
ack it asked for. The leader reports `drained` when the
fence is closed with nothing inside it, and reads the tail it reports only
after seeing that, so the tail is final. The fence lives in
`shards/lifecycle/fence.rs`.

The control plane cuts over on that report and no earlier one: a report from
before the fence, at the previous generation, describes a leader that was
still writing.

The report covers the logs that ride the shard as well as the shard's own. A
stream shard carries its consumer groups' cursors and dead letters, a cache
shard its counters; they are written through the same fence and shipped to
the same replicas. Normally they go after the report and never hold it up,
since no publish waits on group state. The drained pass is the exception: it
ships them first, and reports `drained` only once the move's successor holds
the shard's log and each of them. A dead letter or counter add the new leader
lacks is lost at the cut-over — a dead-lettered record past the group's
cursor is skipped, and an acknowledged add is gone from the sum. Any other
follower still behind on one of them is left out of the report's `caught_up`,
so it is not promoted, but it does not hold the move. A move that has lost
its successor cuts over to whichever follower is level, so there every
follower level on the shard's log must hold them too.

A successor that cannot take them holds the move at `Draining`: the control
plane shows it waiting on the leader,
`felix_broker_replication_drain_withheld_total{log}` counts each pass held,
and the leader logs the shard and the follower. The report carries no
positions for these logs; the leader gates on them, so the control plane has
nothing more to check. The broker learns the successor from the assignment's
`successor` field.

The successor does not have to be exactly level for the fence. A busy
shard's successor almost never is at the instant a report is made, so the
control plane fences once it is within `FELIX_SHARD_MOVE_FENCE_MAX_LAG_RECORDS`
of the leader's tail, which the report carries as `leader_offset`. That is
safe for any bound, because the drained report above waits for the successor
to hold everything to the final tail; the bound only limits how long the
switch-over waits on the copy. A follower whose last batch did not reach it
is left out of the report's offsets, so an unreachable successor is never
fenced on the position it had before it went quiet. While a fenced shard has not
reported drained, the leader runs that shard's next pass 10 ms later rather
than on the next wake, so the remainder, and a destination that has not yet seen the
new generation, cost milliseconds rather than a sync interval.

The copy to the successor can be held to `FELIX_SHARD_MOVE_BYTES_PER_SEC`,
one token bucket per leader across every shard it leads. Only a successor the
quorum does not need is paced: one still copying, which the leader leaves out
of the quorum, or one the rest of the replica set can make a majority
without. So a `Quorum` publish never waits on the limit. The remainder after
the fence is not paced at all, since the shard is not being served. A pass
waits at most 50 ms on the limit for a successor before leaving the rest to
the next pass. `felix_broker_replication_move_throttled_bytes_total` counts
what was shipped under it.

So the ordering is the same shape as report-before-mark. The successor is
staged as a replica and caught up *before* the fence; the leader stops
*before* it reports; the control plane names the successor *after* the
report; and every step is an assignment the next pass reads back, so a
control-plane restart resumes the move where it was. Between the fence and
the successor opening, nobody serves the shard. That window is the cost of the
fence, the same way the safety interval is the cost of the lease.

A publish that lands in the window is held rather than refused. Routing sees
the shard's leader fenced — this broker, or the one it would forward to — and
waits, before admitting the publish, for the routes to change; then it
dispatches again, to the new owner. A publish routed a moment before the fence
closed finds the fence shut when it tries to enter and waits the same way. A
forwarded publish that reaches the old leader, or reaches the new one before
its own routes have caught up with the requester's, waits on that broker until
they settle, and is then applied or redirected. Nothing held has been
acknowledged, so a hold that runs out (`FELIX_SHARD_MOVE_HOLD_MS`, 2 s) or
finds too many already waiting (`FELIX_SHARD_MOVE_HOLD_MAX`) is a plain refusal,
`shard_unavailable` with reason `moving`, and the client retries. Holding lives
in `shards/routing/hold.rs`. Cache and counter operations are held and
forwarded the same way. Group operations are held at the broker they reached
and then redirected to the new owner with `NotLeader`: they are not forwarded,
and the new owner has the group's cursors and dead letters before it leads,
because the drained report waits for them.

Readers leave at the fence too. The old leader ends each subscription and
cache watch on the shard once the writes inside its fence have fanned out, and
its last frame, `shard_moved`, names the successor and the offset to resume
from, so a client resumes there with nothing repeated or skipped
(`docs/protocol.md`, "Shard moves"). An operator can cancel a fenced move: the
old leader serves again at a new generation, and because nobody has led since
the fence its log holds every write it accepted
([control-plane.md](control-plane.md#operator-controls)).

While it copies, the destination is not counted toward the quorum. A leader
that saw the destination added to the replica set — it was not a follower of
the previous generation — computes the quorum mark over the set without it,
the set the stream asked for. Counting it made every `Quorum` publish on a
one-replica stream wait for the whole copy, and made one on a larger stream
wait for it whenever a replica was down. Leaving it out loses nothing: a
promotion picks only a replica the last report names caught up, and the
cut-over waits for the destination to be level. A destination that was
already a replica keeps counting, and a leader with no earlier pass to compare
against counts it too — slower, never weaker. The copy is also shipped in
slices of 50 ms, with the shard's next pass run as each slice ends, so the
copy is not slowed by waiting for a wake.
`FelixShardStagedMoveVotes` in `docs/formal/` shows the wait without this, and
`FelixShardStagedMove` and `FelixShardStagedMoveSingle` that safety holds with
it.

Staging the destination starts a generation under the same leader, and a mark
belongs to one generation, so the new one has none until its first pass
reports. A `Quorum` publish that finds no mark for the generation its broker
leads waits for one instead of reading the gap as lost leadership, and one
already waiting when the generation changes carries over to the new mark,
which counts a majority of the new replica set holding the log up to it. Only
the broker ceasing to lead the shard ends the wait early.

The window is kept short by waking each step rather than polling for it. The
broker long-polls the assignment feed, so the fence and the cut-over reach it
as they are written. Its assignment watch wakes the routing feed, and the feed
wakes replication once it has acted on the change, so the drained report goes
out on the pass right after the fence. The control plane runs placement when
that report lands. The destination sees itself named as `successor` from the
stage onward and opens the shard's log and in-memory state while still
copying, so taking over only records the new generation. The feed then
publishes the routes and the set of shards it serves in one swap, so no
publish sees one updated and the other not. On a local cluster with every
broker on the default 2 s interval the switch-over is tens of milliseconds;
the destination records it as `felix_broker_shard_switchover_seconds`.

A destination that dies before it leads is passed over: another caught-up
replica, or the old leader itself, takes the shard at a new generation. If it
dies after the fence but before it is level, the drained report would wait
for it forever, so the control plane first drops it at a new generation that
is still fenced; the leader then reports drained against the followers it
has, and reports even when that is none. A
leader that dies mid-move is a failover, and the successor is a candidate
there like any other replica. Neither path can name a broker holding less
than the report said, because the report is the only input either reads.

> `a_move_switches_over_in_well_under_a_second` — with every broker on the
> default sync interval and placement on a slow timer, the destination accepts
> a publish well under a second after the fence, and nothing acknowledged
> before the move is lost.
>
> `a_drained_broker_hands_its_shard_over_with_every_record` — an unreplicated
> durable shard moves off a draining broker and every record acknowledged
> before the drain is readable from the new owner.
>
> `continuous_publishing_through_a_move_is_never_refused` — two publishers,
> one through the old owner and one through the destination, run through a
> whole move: none is refused, and every acknowledged record is on the new
> owner exactly once.
>
> `a_subscription_follows_its_shard_to_the_new_owner` — a subscriber reading
> from the start while a publisher writes through the move receives every
> offset once, in order, including every acknowledged record.
>
> `cancelling_a_fenced_move_loses_no_acknowledged_write` — a fenced move is
> cancelled with a publisher and a following subscriber running; the old
> leader takes the shard back and every acknowledged record is delivered once.
>
> `a_quorum_publish_during_a_copy_is_not_held_by_it` — a one-replica `Quorum`
> stream keeps acknowledging while its destination is stalled mid-copy.
>
> `a_move_completes_while_a_publisher_keeps_writing` — four writers keep the
> log growing between every two reports; the move still fences, cuts over,
> and every acknowledged write is on the new owner.
>
> `a_move_that_cannot_copy_is_abandoned_after_its_timeout` — the leader
> cannot reach a live destination; the staging is undone in one write at the
> move timeout, the leader serves throughout, and the move finishes once the
> destination is reachable.
>
> `records_acknowledged_during_a_move_survive_it` — publishes arriving through
> the staging, fence and cut-over are either acknowledged and on the new owner,
> or refused.
>
> `a_destination_that_dies_mid_transfer_does_not_take_the_shard` — the
> staged successor is killed before the cut-over; the shard lands on a broker
> that holds the log.
>
> `a_draining_shard_reports_drained_once_its_fence_is_quiet` — the broker
> side of the fence: no drained report while a write is inside it, however
> still the tail looks, and the report that follows includes that write.
>
> `a_draining_shard_withholds_drained_until_its_dead_letters_are_shipped`,
> `a_draining_cache_withholds_drained_until_its_counters_are_shipped` — no
> drained report while the successor has the shard's log but not its dead
> letters or counters; the report follows once they are shipped.
> `a_lagging_replica_other_than_the_successor_is_left_out_not_waited_for` —
> another replica missing a dead letter does not hold the report, and is not
> in its `caught_up`.
>
> `a_moved_shard_keeps_its_group_state_and_counters` — group acks and counter
> adds keep arriving while a shard and a cache move off a live broker; on the
> new owner no acknowledged record is handed out again, the dead-letter list
> is unchanged, and the counter holds every acknowledged add.
>
> `a_durable_publish_claimed_after_the_fence_is_refused` — a publish admitted
> before the fence and claimed after it is refused and never written; the
> same holds for cache, counter, consumer-group and forwarded writes
> (`cache_writes_after_the_fence_are_refused`,
> `a_counter_add_after_the_fence_is_refused`,
> `an_ack_after_the_fence_is_refused`,
> `a_forwarded_publish_after_the_fence_is_refused`).

Both halves are model-checked. `docs/formal/FelixShardHandoff.cfg` explores
the move as implemented without a violation;
`FelixShardHandoffNoWait.cfg` — the same move cutting over as soon as the
fence is written — finds two brokers serving the shard at once in seven
steps, because the old leader has not seen the fence yet. The lease does not
close that: it has not lapsed, and the leader is alive and meant to keep it.
Only the leader's own word that it stopped does. And
`FelixShardHandoffNoClaimFence.cfg` — the fence checked at admission only —
finds a write admitted before the fence, claimed after the drained report,
and acknowledged by the old leader after the successor took over, which the
successor does not hold.

The steps, their triggers and the policy that bounds them are in
[control-plane.md](control-plane.md#moving-a-shard).

## What this does to the other M5 issues

- **#111 (fenced leadership)** — this is now specific: the epoch is the
  assignment generation, the fence is the lease, and it is enforced at both the
  routing and durable-append boundaries. **#239 is subsumed**: a stale ex-owner is
  a broker without a valid lease, and the same check refuses it.
- **#112 (replicate records)** — append-only shipping, not a consensus log. The
  catch-up path is `read_range` plus sealed-segment checksums. **Done**, both
  halves. Catch-up currently re-reads from the offset the follower names rather
  than verifying whole sealed segments by checksum; that is an optimisation for
  #114, not a change to the rule.
- **#113 (Leader and Quorum)** — the majority is over the replica set *of the
  current generation*, and an acknowledgement from a replica at an older
  generation does not count toward it.
- **#114 (bootstrap followers)** — bounded range reads from the leader; no
  snapshot-install protocol is needed, because the log is the snapshot.
- **#115 (failure injection)** — needs lease expiry, clock skew, and suspension
  as injectable faults, not just process kills. The harness can stop and move a
  broker today (#108); it cannot yet pause one or skew its clock.
- **#116 (semantics)** — must document the `Leader` loss window in terms of the
  lag metric, and state that `Quorum` is majority-including-leader.

## What would overturn this

Stated because a decision without one is an opinion.

**Raft's real advantage is that it needs no clock assumption for safety.** Leases
trade that for a simpler data path. If Felix ever needs to run where drift rate
cannot be bounded — or where process suspension is common enough that `ε` cannot
be chosen — that trade stops being worth it.

The other trigger is the storage layer changing. The argument above rests on
"no committed record is ever rewritten." If that invariant is ever relaxed for another
reason, Raft's cost drops sharply and this should be revisited rather than
inherited.

What is *not* a reason to revisit: throughput. Leases were not chosen because
they are faster in the common case. `Quorum` pays one round trip either way, and
the difference between designs is at failover and in the storage layer, not in
steady-state publish latency.
