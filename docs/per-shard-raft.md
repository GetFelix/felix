# Per-shard Raft

This page is the design for track 4 of the one-engine plan (issue #2167):
every shard runs one Raft group, through openraft, behind the engine
interface. Nothing here is built yet. The decisions it rests on are in
[`replication-engine.md`](replication-engine.md), which this page calls "the
engine page". Read its "Decisions" first. The protocol being replaced is in
[`replication-design.md`](replication-design.md) and is called "the hybrid"
here.

The page covers the engine interface as Raft needs it, how thousands of
groups share one timer and one heartbeat message per peer, how idle groups
sleep, how the write fence, leases and read bound map to Raft, how a shard
moves from the hybrid to Raft, and how all of it is tested and gated. It ends
with the pull request split and the questions that are still open.

## Summary

A broker holds one Raft member per shard copy. The design point is 2,000
members per broker and the gate is 5,000 (engine page, "How many groups a
broker carries"). Three things make that cheap:

1. One timer per broker ticks every awake group. Groups do not run their own
   timers.
2. An idle group sleeps. It is not ticked, sends no heartbeats and runs no
   election timer. It wakes on a write, on any Raft message, or when the
   brokers' own liveness exchange says a member's broker is gone or came
   back.
3. Heartbeats of awake groups travel as one message per peer broker.

Safety comes from Raft alone. Sleep, the shared timer, coalescing and the
liveness exchange only decide when a group does work. If any of them is
wrong, a group is slow to elect or elects when it did not need to. None of
them can make two leaders commit at one offset.

## What changes for the rest of the broker

| Today (hybrid) | Under per-shard Raft |
| --- | --- |
| The control plane assigns a leader and a generation | Raft elects the leader. The term is the generation. The control plane only places replica sets |
| A promoted leader runs a fence, then opens | A leader opens when the blank entry of its term commits |
| A `Leader` shard serves on the control-plane lease | A Raft leader lease, checked when a write is acknowledged |
| `Quorum` reads confirm by lease or read round | A read-index round |
| Cache shards have a cache log and a counter log | One log, one group |
| A wiped copy is vouched back in | A wiped copy is a new member and joins as a learner |
| Suspicion is reported through the control plane | Brokers exchange liveness directly, tied to their disks |

## The engine interface

The trait in `crates/server/felix-replication/src/engine.rs` was shaped by
routing the hybrid's callers through it. Raft needs it smaller. The target:

```rust
/// Where a write landed: the offset one past its last record, and the
/// generation (Raft term) it was written at.
pub struct Written {
    pub end: u64,
    pub generation: u64,
}

pub trait Engine: Send + Sync {
    /// Records up to `written.end` are durable on this leader's log, in the
    /// broker's commit order. Raft adopts them as entries.
    fn appended(&self, shard: &ShardKey, written: Written);

    /// Wait until `written` is committed. An error means this broker cannot
    /// say so, not that the write is lost.
    fn await_commit(&self, shard: &ShardKey, written: Written, timeout: Duration)
        -> impl Future<Output = Result<(), CommitError>> + Send;

    /// May a write at `generation` be acknowledged on local durability
    /// alone? The leader lease check for `Leader` streams.
    fn admit_local_ack(&self, shard: &ShardKey, generation: u64) -> Result<(), CommitError>;

    /// One past the highest committed offset, at every consistency level.
    fn read_bound(&self, shard: &ShardKey) -> Result<u64, Unreadable>;

    /// Confirm this broker still leads `shard`, for a read of state.
    fn confirm_read(&self, shard: &ShardKey)
        -> impl Future<Output = Result<(), CommitError>> + Send;

    /// The control plane placed `shard` on `set`.
    fn placement_changed(&self, shard: &ShardKey, set: &ReplicaSet);

    /// A peer's replication message.
    fn handle(&self, from: Option<&str>, caller: PeerCapabilities, request: InternalMessage)
        -> impl Future<Output = InternalMessage> + Send;
}

pub(crate) trait EngineHost: Send + Sync {
    /// This broker leads `shard` at `generation` and may take writes.
    fn opened(&self, shard: &ShardKey, generation: u64);
    /// This broker no longer leads `shard` at `generation`.
    fn deposed(&self, shard: &ShardKey, generation: u64);
    /// Whether `shard` is closed with no write still inside.
    fn quiesced(&self, shard: &ShardKey) -> bool;
}
```

What goes away, and why it is safe to drop:

- `cache_write_at`, `await_cache_quorum` and `await_counter_quorum` become
  `await_commit`. A cache shard has one log and one commit index (see "Cache
  and counters").
- `committed_bound` merges into `read_bound`. Today it differs by a lease
  check. Under Raft a deposed leader's commit index is still a true lower
  bound on what is committed, so delivering up to it is always safe and needs
  no lease.
- `read_skips_lease` and `await_readable` go with the control-plane lease.
  Every read is bounded by `read_bound`.
- `EngineHost::generation` and `replicated` go. The engine tells the host the
  generation through `opened`, and every shard is a group.
- The `HybridHost` callbacks stay on the hybrid until it is deleted.

The interface does not assume one group per stream. A `ShardKey` maps to a
group through a registry inside the engine, so several small streams could
later share a group without changing a caller.

### Group and member identity

A group id is the `ShardKey`. A member id is the pair of broker id and
storage id. The storage id changes when a broker loses its disk or restarts
after a failed fsync (`run_state::forget_boot`). So a copy that may have
forgotten a vote or an acknowledged record is a new member to Raft. It has
never voted, it joins as a learner, and the old member is removed by a
membership change. This is the rule issue #1331 asks for, and it replaces
vouching and copy trust.

`Config::allow_log_reversion` stays off for shard groups. It lets a voter
come back with a shorter log under the same id, and a voter that lost an
acknowledged entry can then help elect a leader without it. With the storage
id in the member id the case does not arise, and a log that goes backwards
under one id is a bug to stop on. (The engine page says the adapter sets it
for every group. See Questions, 2.)

A new group is created by every member of the control plane's first set
calling `initialize` with that same set, which openraft allows. A member
initializes only when its own member id is in the set and its log is empty.

### Adopting the leader's appends

The engine page decides that the broker keeps appending on the leader and
openraft adopts the records (Decisions, 2). In openraft terms: after the
broker's `commit`, `appended` hands openraft one entry per record through
`client_write_many`, in commit order. openraft assigns each the next index
and calls the adapter's `append`. The adapter finds the record already
durable at that index with that term and reports it flushed.

Two rules make this safe.

**The vote is the fence.** The adapter keeps a term gate under `DiskLog`'s
append lock. `save_vote` for a term above `T` closes the gate for `T` before
it returns. A broker append at generation `T` after that fails. So once
openraft has voted in a later term, no record of the old term can reach the
log, whatever the service's write fence believes at that moment. The
service-level fence (`ShardFence`) stays as the fast path and for routing. It
is not what safety rests on.

**No tail openraft has not seen.** When the gate closes, the adapter cuts any
records above the last index openraft accepted, before `save_vote` returns.
Those records were never committed. A `Leader` publisher may have been
acknowledged for them, which is inside the loss window `Leader` already
documents. After a crash the same tail is read back as uncommitted entries of
the old term, and the next leader keeps or cuts it, as the engine page says.

If openraft assigns an index the record is not at, the group stops. That
means the commit order and openraft disagree, which is a bug.

The first implementation pull request proves adoption on the pinned openraft
with a test, before anything builds on it. If it cannot be made to hold, the
fallback is `client_write` with the adapter appending, and the gate's latency
cells price it.

## One timer for every group

Every group is built with `enable_tick: false`. Its internal ticker task
stays parked and costs no CPU (measured: idle CPU 0 at 5,000 groups, against
86% of a core per 1,000 members with the default tick). One task per broker,
the node ticker, calls `Raft::trigger().tick()` on every awake group each
interval. openraft 0.10.1 adds that call (openraft#2305). It runs the same
check the internal ticker runs: a follower past its election timeout starts
an election, a leader whose heartbeat is due sends one.

```mermaid
flowchart LR
  T[Node ticker, one per broker] -->|tick, concurrently| A1[Awake group]
  T -->|tick, concurrently| A2[Awake group]
  T -. never ticked .-> S[Asleep group]
  A1 -->|heartbeat| C[Per-peer heartbeat batch]
  A2 -->|heartbeat| C
  L[Store liveness] -->|wake| S
  C -->|one message per interval| P[Peer broker]
  L -->|rides in the same message| C
```

Rules for the ticker:

- **Tick concurrently.** `tick()` waits while a group's API channel is full,
  so a loop that awaits each group in turn lets one busy group delay the
  rest. The ticker polls all of an interval's ticks together in its own task.
  It spawns nothing per tick.
- **Drop, never queue.** A tick still waiting when the next interval starts
  is dropped. openraft reads the clock when it handles a tick, so a late tick
  sends one heartbeat and a dropped tick loses nothing. The ticker counts
  dropped ticks per group as a metric.
- **Only awake groups.** A sleeping group is not ticked at all. This is what
  makes idle CPU zero, since even a slow tick costs a wakeup per member.
- **The interval adds to timeouts.** A timeout is noticed at the next tick,
  so the tick interval is added to election and heartbeat latency. The lease
  margin (engine page, Decisions, 5) counts it.

Cost when every group is awake: a tick costs about 9 microseconds of CPU
(from the measurement above). 2,000 awake members at 20 ticks a second is
about a third of a core. With most groups asleep it is close to nothing.

Timer values are in Questions, 4. The hybrid suspects a leader after 5
seconds, so Raft does not need 50 ms heartbeats to match its failover time.

## Heartbeat coalescing

Without coalescing, each awake leader sends each follower one empty
AppendEntries per heartbeat interval. A broker with 2,000 awake members at RF
3 leads about 667 groups, so it sends about 1,300 heartbeats per interval and
answers about as many.

### The upstream shape

openraft sends a heartbeat through the group's network handle, and
`openraft-multi` routes it through `GroupRouter::append_entries(target,
group, request)` with no entries. The pull request Felix sends to
`openraft-multi` (openraft#2304, item 2) has three parts:

1. A wrapper router that implements `GroupRouter` over any other
   `GroupRouter`. An AppendEntries with no entries is parked in a buffer for
   its target. Anything else passes straight through.
2. A new `GroupRouter::heartbeats(target, batch)` method that carries many
   `(group, request)` pairs and returns one result per pair. Its default
   implementation calls `append_entries` for each, so existing routers keep
   working. A router that implements it sends one message.
3. A receive helper that hands each request in a batch to its group's
   `Raft::append_entries` concurrently and collects the answers.

The buffer flushes when the oldest heartbeat in it has waited a short window
(a few milliseconds), or when the batch reaches a size cap. Since the node
ticker ticks all groups together, their heartbeats arrive in the same
window, and one flush carries them all. One heartbeat is about 60 bytes
(vote, previous log id, commit index), so 2,000 fit in one message.

The delay is safe for leases. openraft takes the lease's start from when it
handed the heartbeat to the network, which is before the wait, and a follower
starts refusing votes when it receives it, which is after. Both err on the
safe side. The window is added to the margin like the tick interval.

### The interim

Felix does not coalesce privately (engine page, "Limits found in openraft are
fixed upstream", and #2215). Until the upstream change ships, sleep carries
the load: an idle group sends no heartbeats, and a busy group's appends
already serve as its heartbeats. What is left is groups that are awake and
lightly written, which heartbeat one by one over the shared peer connection.
The design-point run measures that traffic on real transport, and the
numbers go on openraft#2304. If it breaks a gate cell before the release, the
wrapper is carried in the tree as an exact copy of the upstream pull request
and deleted when the release that has it is pinned. See Questions, 3.

## Store liveness

Sleep needs a way to notice that a sleeping leader's broker is gone without
per-group heartbeats. That is store liveness: each pair of brokers that
share a group exchanges one small heartbeat per interval. It rides in the
same per-peer message as the group heartbeats. It follows the rules on the
engine page ("What other multi-Raft systems learned", 1):

- **Peer to peer.** Brokers exchange it directly. The control plane is not
  involved, so a control-plane outage does not stop failover.
- **Tied to the disk.** A broker writes and fsyncs a small probe file on each
  volume that holds shard logs, once per interval. It sends liveness
  heartbeats only while the last probe finished recently. A broker whose
  disk has stalled goes silent, even if its process and network are fine.
- **Epochs.** Each broker has a store epoch that rises at every start. A peer
  that sees a new epoch knows the broker restarted.

A broker that has not heard a peer for the suspect time withdraws support
for it and tells the sleep logic, which wakes the groups it shares with that
peer.

In this design store liveness decides only when to wake. A wrong answer is
never unsafe: a false alarm wakes groups that then find a healthy leader
through pre-vote, and a missed failure is caught by the audit described
below. Using it to keep leases alive while a group sleeps is a later step
with its own safety argument (see "Leases that survive sleep").

`suspicion.rs` is the hybrid's version of this signal. It pings peers but
reports through the control-plane heartbeat and writes nothing to disk. It
is deleted with the hybrid.

## Sleep and wake

A member is awake or asleep. The node ticker holds the state. openraft does
not know about it: an asleep member is simply one that gets no ticks.

```mermaid
stateDiagram-v2
  [*] --> Awake: start
  Awake --> Asleep: sleep handshake done
  Asleep --> Awake: write or read round
  Asleep --> Awake: any Raft message for the group
  Asleep --> Awake: placement change
  Asleep --> Awake: liveness change of a member's broker
  Asleep --> Awake: audit timer
```

### Going to sleep

The leader proposes sleep when all of these have held for the idle time:

- It is the leader and its blank entry is committed.
- Nothing has been proposed, and no read round is waiting.
- Every member matches the leader's last log id, and has acknowledged a
  heartbeat that carried the current commit index. A member whose broker
  store liveness says is down is left out, as long as a majority matches.
- Membership is committed and not joint. No snapshot is being sent and no
  leadership transfer is running.

It then sends each member a sleep request naming its vote, last log id and
commit index. A follower accepts only if its own vote, last log id and commit
index are the same. It then marks the group asleep and answers. The leader
sleeps only after every live member has answered. Followers sleep first and
the leader last, so a lost answer leaves the leader awake, and its next
heartbeat wakes the followers again.

A follower that is behind cannot be put to sleep, because the request names
the exact log position.

### Waking

Any of these wakes a member. Waking always happens before the event is
handed to openraft.

- **A write, or a read that needs a round, on the leader.** The leader wakes
  and replicates. The append wakes each follower when it arrives.
- **Any Raft message for the group.** AppendEntries, a vote or pre-vote, a
  snapshot, a transfer.
- **A placement change** for the shard.
- **Liveness.** When support for a broker is withdrawn, or its epoch
  changes, every group shared with it wakes. A leader woken this way sends a
  heartbeat at once, so a restarted follower hears it before its election
  timeout.
- **The audit timer.** Each sleeping leader wakes on a long, jittered timer
  (about a minute), sends one heartbeat round and sleeps again through the
  handshake. A sleeping follower that has heard nothing for two audit periods
  wakes and ticks, so ordinary Raft election takes over. At 5,000 members
  this is about 80 wakeups a second per broker. The audit bounds how long a
  bug in sleep or in liveness can leave a group without a leader.

### When a broker dies with sleeping leaders

Every follower of every group it led wakes together. Two things keep that
from becoming an election storm:

- **Ranked starts.** Each waking follower ranks itself among the group's live
  voters by a hash of the group id and member id. The first calls
  `trigger().elect(true)` (with pre-vote) at once. The others get their first
  tick one election timeout later per rank. By then the first has usually
  won and its append resets their timers. This only shapes timing: if the
  ranks are wrong, the usual randomized timeouts decide.
- **A cap per tick.** The ticker starts at most a fixed number of elections
  per interval and queues the rest.

### What sleep costs a write

A sleeping leader has not heard from a majority, so its lease has lapsed.
The first write after sleep is replicated and acknowledged by a majority
before a `Leader` publisher is acknowledged: one round trip, once. `Quorum`
writes wait for that round anyway.

### Leases that survive sleep

CockroachDB's leader leases keep a sleeping group's lease valid for as long
as a majority of the group's stores support the leader's store. The first
write after sleep then pays nothing, and a dead broker's groups elect
without a storm. Felix wants this, but it moves store liveness into the
safety argument: a follower's promise not to vote has to be tied to the
support it gave, and the support has to survive a restart. That needs a
change inside openraft's vote handling (openraft#1651) and a checked model.
It is not in the first version. See Questions, 5.

## Write fence, leases and read bound under Raft

### When a leader opens

The engine calls `EngineHost::opened(shard, term)` when the blank entry of
its term is committed. At that point the leader holds every committed
record, its commit index is current, and its lease clock has started. The
blank entry is the generation-start record and goes through the broker's
commit order like any other append. This costs one round at the start of a
term, as the hybrid's fence does today.

### When it closes

The engine calls `deposed` and the write fence closes on any of:

- openraft reports a vote for a higher term, or a role other than leader.
- `admit_local_ack` fails the lease check.
- A leadership transfer starts.
- The group stops on a storage error.

openraft reports role changes through its metrics channel, which is
asynchronous. The term gate in the adapter covers the gap: after the vote
is saved, an append at the old term fails under the log's own lock.

### Acknowledging a write

A `Quorum` write is acknowledged when `await_commit` returns: the commit
index has passed it and the entry there has the term it was written at. If
the leader is deposed first, the waiter gets leadership lost and the client
gets an unknown outcome, as today. Nothing on this path asks the control
plane anything, which is the property issue #1792 asks for.

A `Leader` write is acknowledged on local durability if `admit_local_ack`
passes: `now < last_quorum_acked + lease`. openraft 0.10 publishes
`last_quorum_acked` as an instant; the older `millis_since_quorum_ack` is
deprecated. The value only moves forward, so a stale read errs toward
refusing. The lease must satisfy the inequality on the engine page
(Decisions, 5), with the tick interval and the heartbeat batch window added
to the left side.

### Reads

`read_bound` is the commit index plus one, for every stream and every
consistency level. Subscriber fanout, cursor replay, Kafka fetch and cache
watch all take it. The engine publishes each commit index advance into the
table the commit hold already waits on.

`confirm_read` is a read-index round (`Raft::get_read_linearizer` with the
read-index policy). On a sleeping group it wakes the group for one round.
Raft lease reads (`get_local_read_linearizer`) would save that round. They
stay off until they are measured and the model covers the lease (engine
page, "openraft version").

A broker that restarts answers reads again once its member has caught up
and a leader is known. It does not need the control plane to confirm an
assignment first, which is what kept shards unreadable in issue #1914.

### Membership

The control plane still chooses the replica set and paces moves. The group's
leader makes Raft's membership match it:

1. Add each new member as a learner and wait until it has caught up.
2. Call `change_membership` with the new set. openraft goes through joint
   consensus on its own and commits the membership as the typed record.
3. If the leader is not in the new set, transfer leadership first.

Only the leader acts, one change at a time, and the steps can be repeated
safely. A copy with a new storage id is handled by the same loop: the set
names the broker, the member id differs, so the old member is swapped for a
learner with the new id.

The control plane no longer assigns leaders. Brokers report who leads each
shard, and the routing table uses that as a hint. A broker that gets a write
for a shard it does not lead answers with the leader it knows. To balance
leaders the control plane asks the current leader for a transfer.

### Shards with one replica

An RF 1 shard runs a group with one voter. It elects itself, commits on its
own disk and sleeps like any other group, and moving it to another broker is
an ordinary membership change. This removes the last use of the
control-plane lease. See Questions, 1.

## Cache and counters

A cache shard's group has one log holding puts, deletes, counter deltas and
counter checkpoints (engine page, Decisions, 4). For track 4 that means:

- **One wait.** A cache or counter write waits on `await_commit` at its
  position in the one log.
- **The state machine is the projection.** The cache index and the counter
  sums are rebuilt from a snapshot plus the log, as the cache index is
  today.
- **Compaction is proposed by the leader.** Copies of live records and the
  counter checkpoint are entries. Followers apply them and do not compact on
  their own. Dropping old segments is Raft's purge, done only behind a
  durable snapshot.
- **The snapshot** is the live cache records and one counter checkpoint, as
  of one log id.

## Moving a shard from the hybrid to Raft

The switch is per shard, one way, and looks to Raft like a snapshot install.
It needs the `record_generations` fleet feature finalized (records carry
their term) and a new fleet feature, `shard_raft`, that says every broker
can run the Raft engine.

```mermaid
sequenceDiagram
  participant CP as Control plane
  participant L as Hybrid leader
  participant F as Followers
  CP->>L: seal shard at a new generation G
  L->>F: fence at G (no writes admitted)
  F-->>L: promises and tails
  L->>F: generation-start record of G at offset N
  F-->>L: acknowledged by a majority
  L->>CP: sealed at (G, N)
  CP->>CP: commit engine = raft, base (G, N), members
  CP-->>L: assignment
  CP-->>F: assignment
  Note over L,F: each copy holding (G, N) starts its member with base (G, N)
  Note over L,F: Raft elects at a term above G and opens
```

1. **Seal.** The control plane promotes the shard to a new generation `G`
   with a seal flag. The hybrid leader runs its ordinary fence and catch-up
   and appends the generation-start record at offset `N`, but does not open
   for writes. For a cache shard it first appends one counter checkpoint
   holding the counter log's sums, so the cache log alone carries the state.
2. **Record the base.** When a majority holds the record at `N`, the leader
   reports it. The control plane commits, in one metadata write, that the
   shard's engine is Raft, its base is `(G, N)`, and its members are the
   current set with their storage ids. This write is the switch. Before it,
   the shard is a hybrid shard that happens to be closed, and can be opened
   again. After it, no broker runs the hybrid for this shard.
3. **Start.** A copy that holds the record `(G, N)` cuts anything above it
   and starts its Raft member with the log purged up to `(G, N)`, the base
   as its snapshot id, and the recorded membership. Everything up to `N`
   stays on disk as readable records and is never compared by term, so
   records whose writer is unknown (generation 0) are never Raft entries.
4. **Catch up the rest.** A copy that does not hold `(G, N)`, or was halted,
   starts empty and receives a snapshot install from the Raft leader, which
   is the record range below the base copied the way a rebuild copies it
   today.
5. **Elect.** Members campaign at a term above `G`. The winner commits its
   blank entry and opens.

Why it is safe: the hybrid's fence guarantees that every acknowledged record
is at or below `N` on the leader and that no lower generation can commit
again. Every copy that starts from the base holds the same bytes up to `N`,
because it holds the record `(G, N)` and the hybrid's log matching covers
everything below it. The base is recorded once, in the control plane's Raft,
so no two brokers can start from different bases.

The shard is closed from the seal until the Raft leader opens: one hybrid
promotion plus one Raft election. Shards switch a few at a time, paced like
moves. There is no switch back. See Questions, 6.

## Testing

### Simulator: Raft clusters

The simulator builds Raft-engine clusters through the same service code. All
four modes (`off`, `clocks`, `placement`, `power-loss`) run against them,
with the history checkers unchanged. Cluster checkers named after
`FelixShard.tla` invariants are replaced by the same checks named after
`FelixRaft.tla`: one leader per term, acknowledged records held by the
leader, election settles.

Acceptance cases carried over from open issues:

- **#1331.** A broker restarted with a new storage id never votes under its
  old member id. It rejoins as a learner. A test wipes a voter that held an
  acknowledged record and checks the record survives every later election.
- **#1914.** After a killed broker returns, every shard is served and
  readable. No read is refused once the faults heal.
- **#1792.** With the control plane paused for the whole run, `Quorum`
  acknowledgements continue. The acknowledgement path makes no control-plane
  request.
- **#1009 and #1197.** With the control plane down, a leader crash fails
  over, writes continue and consumer groups keep working. Passing this in
  the Raft cluster closes both.
- **#2032.** Seed 4 is rerun under Raft.

### Simulator: the sleep nemesis

A fifth mode, `sleep`. It sets the idle time to two seconds and the audit
period to twenty, and adds a schedule that stops client writes to a random
subset of shards long enough for them to sleep. Then it draws faults aimed
at sleeping groups:

| Fault | What it must not break |
| --- | --- |
| Crash, kill or power-cut the broker that leads the most sleeping groups | Every group elects; no storm stalls the run |
| Freeze a broker (process alive, silent) | Liveness withdraws support; groups move |
| Stall a broker's disk while its network works | The broker goes silent because its probe stalls; groups move |
| Drop or delay sleep requests and answers | A half-asleep group wakes fully or sleeps fully |
| Partition a sleeping leader, then write to it | The write is refused or times out; a new leader takes it |
| A write arriving during the handshake | It is committed; no member sleeps behind |
| Restart a sleeping follower | It hears the leader before it campaigns, or loses a pre-vote |
| A false liveness alarm for a healthy broker | Pre-vote leaves the leader in place |
| Liveness that misses a dead broker | The audit elects a leader within two periods |
| A placement change on a sleeping shard | The group wakes and applies it |
| Clock steps and skew (with `clocks`) | The lease margin holds |

New cluster checkers:

- `SleepersWake` (healed and end): every shard that slept is served, and a
  write to it is acknowledged within the settle time.
- `AsleepIsLevel` (every second): if every live member of a group is
  asleep, they have the same last log id and commit index.
- `IdleGroupsSleep` (end, a warning): in the quiet tail of a run, a group
  with no writes got no ticks. This catches a group that fails to sleep,
  which is a cost bug and not a safety bug.

The history checkers stay the judge of safety.

### History checker and scale

Every nemesis in `history.yml` runs against a Raft cluster, including the
duplicate-offset nemesis of the engine page. `scale-nightly.yml` adds a run
that kills a broker leading sleeping groups at the design point and reports
the time until every shard is served.

## The TLA+ model

`FelixRaft.tla` (pull request #2220) models one group with a fixed voter
set and no leases. Track 4 needs these added before its pull requests that
touch them are labelled reviewed.

**Joint consensus.** Membership is carried by the membership entry. A member
uses the latest membership in its log, committed or not. Add learners, a
joint configuration that needs a majority of both the old and the new set
for election and commit, and the rule that a new change starts only after
the previous one is committed. Add a removed leader stepping down. Checks:
the existing ones across a change, plus no two leaders in one term under any
pair of overlapping configurations. Twin configs that must fail: a change
that skips the joint step with two members changed, a second change before
the first commits, and a learner counted toward a quorum.

**Member identity.** A wipe gives a broker a new member id. A twin where a
wiped broker keeps its id and votes must violate `LeaderCompleteness`. This
is the model's side of #1331.

**Leader leases.** Add a clock per broker with bounded drift, the leader's
lease from the send time of its last majority-acknowledged message, and the
follower's vote refusal from its receive time. Add `Leader`
acknowledgements on local durability under the lease. Check that no two
brokers pass the lease check for one shard at the same real time, and that a
`Leader` record delivered to a subscriber is never replaced. Twins that must
fail: a lease longer than the margin allows, a lease measured from the
acknowledgement instead of the send, and a follower that votes inside its
refusal window.

**Sleep.** Add asleep members that take no timeout steps, the handshake, and
the wake rules. Sleep changes only liveness, so the check is a liveness one:
with a leader's broker gone and a majority alive, a leader is eventually
elected, through liveness or through the audit. A twin with neither wake
path must fail it. Safety checks must pass with members sleeping in any
state, including behind, to show safety does not rest on the handshake.

**The switch.** Start a group from a base `(G, N)` with records below it
that differ in term between members (the generation 0 case). The twin that
compares terms below the base must fail, as the engine page already asks.

**Later, with store-liveness leases:** support with epochs and expiry, a
lease that holds while a majority supports the leader's store, and a vote
refused while support stands. Not needed for the first version.

The hybrid's `FelixShard*` specs stay until the gate passes.

## Performance plan

Two comparisons, both needed.

**Against the hybrid, same commit.** While both engines are in the tree, the
build that has the Raft engine picks one at start. `perf-env-ab.yml` runs
both sides on one runner, interleaved, six rounds. This is the gate on the
engine page: throughput within 10% and p99 within 20% in every cell,
failover no worse.

**Against the latest v0.7 release.** The release A/B builds the v0.7 tag and
the candidate and runs them on the same runners. Steady and single-node
cells must match the release. This catches a regression that both engines
share.

Cells, each at 1 KiB and 64 KiB where payload size applies:

| Cell | What it prices |
| --- | --- |
| Single node, RF 1 | Adoption and the single-voter group on the hot path |
| RF 3 steady, `Quorum` | The Raft core's cost per record, pipelined appends |
| RF 3 steady, `Leader` | The lease check; delivered latency (commit before delivery) |
| RF 3 promotion burst | Election plus blank entry against fence plus generation-start |
| Cache, mixed puts and counter adds | One log: fsyncs per write |
| `Quorum` read p99 | Read-index rounds |
| Failover time, control plane up and down | Timer values and ranked starts |
| First write after sleep | The one extra round, p50 and p99 |
| Broker loss with sleeping leaders, design point | Time until every shard is served |

Rules for the runs: a clean machine and clean artifacts, nothing compiled
during a run, throughput and delivered throughput reported separately, and
batched cells read as throughput only. Functional runs are in Actions. The
real-network cells run on cloud VMs with separate disks per broker.

A miss is fixed in the engine or upstream. It is not written up as a limit.

## The groups-per-broker gate

Criterion 6 of the gate, made concrete. Three real brokers on real
transport, RF 3, so each broker holds every shard: 5,000 shards is 5,000
members per broker, about a third of them leaders.

| Measure | At 2,000 members (design point) | At 5,000 members (gate) |
| --- | --- | --- |
| Idle CPU per broker, 60 s after settling | under 2% of one core | under 5% of one core |
| Resident memory added by Raft, against the hybrid on the same shards | reported | under 1 GB |
| Groups asleep after the idle time | all | all |
| Ticks to sleeping groups | 0 | 0 |
| First write after sleep | within the A/B cell | reported |
| One broker killed: time until every shard is served | no worse than the hybrid | reported, no stall |
| Broker start to all members caught up | reported | reported |

10,000 members is a stress run. Its numbers go upstream and nothing passes
or fails on it.

Memory is the tight one. From the 0.10.0 measurement (180 KB per leader
member, 145 KB per follower), 5,000 members at one third leaders is about
780 MB. Sleep does not reduce it, because a parked task keeps its memory.
The headroom comes from the upstream ask for fewer tasks per member
(openraft#2304, item 3). If the gate run is over 1 GB, that ask is the fix,
and `max_shards` holds brokers under it meanwhile.

`max_shards` defaults to the gate size once Raft is the engine, and
placement answers `at_capacity` past it.

## Pull request split

Each pull request is safe to merge on its own. Until the gate passes, the
Raft engine is compiled only with a cargo feature that no release enables,
and the hybrid is what ships.

| # | Pull request | Depends on | Why it is safe alone |
| --- | --- | --- | --- |
| 1 (#2227) | Engine trait takes its final shape (`appended`, `Written`, `admit_local_ack`, `opened`, `placement_changed`), implemented by the hybrid | | No behaviour change. Existing tests and seeds pass unchanged |
| 2 (#2229) | Store liveness: disk probe, epochs, per-peer exchange. Runs and reports metrics, drives nothing | | Nothing reads it yet |
| 3 (#2231) | Group runtime: type config, member id with storage id, group registry, peer messages for Raft behind a negotiated capability, node ticker on openraft 0.10.1 | track 2 adapter | Behind the feature. No shard uses it. A peer without the capability is never sent a Raft message |
| 4 (#2233) | Stream shards on Raft: adoption with the term gate, `await_commit`, `read_bound`, read index, the leader lease, `opened` and `deposed`. Simulator and history runs on fresh Raft clusters | 1, 3 | Behind the feature, fresh clusters only. Starts with the adoption test |
| 5 (#2235) | Membership: learner, joint change, transfer, wiped-copy replacement, leader reports and routing hints, single-voter RF 1 groups | 4 | Behind the feature. Needs the model's joint consensus and identity configs first |
| 6 (#2238) | Cache and counters in one group: state machine, proposed compaction, snapshot | 4 | Behind the feature |
| 7 (#2239) | Sleep and wake, the audit, ranked starts, and the `sleep` simulator mode with its checkers | 2, 4 | Behind the feature. Sleep can be turned off, and safety does not depend on it. Needs the model's sleep configs first |
| 8 (#2241) | Heartbeat coalescing: the `openraft-multi` pull request, then adopting the release that has it | 3 | Upstream change is additive. Felix adopts it by a version pin |
| 9 (#2242) | The switch from the hybrid: seal, base, `shard_raft` fleet feature | 4, 5, 6 | Gated by a fleet feature nobody finalizes before the gate passes. Needs the model's switch config first |
| 10 (#2243) | Gate runs: A/B cells, groups-per-broker run, write-up in `docs/` | 4 to 8 | Workflows and docs only |

TLA+ additions (issue #2244, part of #2198) land in the model's own pull
requests, ahead of the pull requests that name them.

## Questions

Each has a recommendation, judged on safety and performance only.

1. **Do RF 1 shards run a single-voter Raft group, or stay outside Raft on
   the control-plane lease?** Recommended: a single-voter group. It removes
   the control-plane lease and its bug class entirely, and a move is a
   membership change instead of a second mechanism. The cost is about 180 KB
   per shard and whatever adoption adds per batch, which the single-node
   cell must show is within the gate.

2. **Is `allow_log_reversion` on for shard groups?** The engine page says
   the adapter sets it for every group. Recommended: off for shard groups,
   with the storage id in the member id. A voter that returns with a shorter
   log under its old id can help elect a leader that lacks an acknowledged
   record. A new id per storage id makes the copy a learner and closes that.
   The control plane's group is track 3's call.

3. **What is the heartbeat-coalescing interim?** Recommended: none in the
   tree. Sleep removes idle heartbeats, which is the gate. Measure the awake
   traffic at the design point, post it upstream, send the pull request. Only
   if an A/B cell fails on heartbeat cost does Felix carry the wrapper, as an
   exact copy of the upstream pull request. The wrapper is safe either way,
   so this is about keeping one copy of the code.

4. **Which timer values?** Recommended to start: tick 50 ms, heartbeat
   200 ms, election timeout 1 to 2 s, liveness heartbeat 1 s with support
   withdrawn after 3 s. That is a quarter of the default heartbeat traffic
   and tick cost, and still fails over faster than the hybrid's 5 s
   suspicion. The failover cells decide the final values. A shorter election
   timeout shortens the lease, so the two are tuned together.

5. **Does 0.8 ship leases that survive sleep (store-liveness leases)?**
   Recommended: no. Ship sleep with a lapsed lease first. It adds no safety
   argument and costs one round on the first write after idle. Leases backed
   by store liveness need a change inside openraft's vote handling and a
   checked model, and a mistake there is two leaders acknowledging. Take it
   after the model covers it and the upstream issue (openraft#1651) has a
   shape.

6. **Is the switch from the hybrid live and per shard, or done with the
   cluster stopped?** Recommended: live, per shard, as designed above. Both
   are equally safe if the base is recorded once. The live switch is the one
   the simulator and the model can exercise under faults, and it keeps the
   closed window to one shard at a time. A stopped-cluster switch is
   simpler to build but is tested less, and closes every shard at once.

7. **Does a leader open at the blank entry's commit, or as soon as its vote
   is committed?** Recommended: at the blank entry's commit. Opening earlier
   saves one round per term but lets a leader take writes before it knows
   its commit index or has a lease, and every one of those writes would wait
   for the blank entry anyway.

8. **Is sleep on by default when Raft first lands?** Recommended: yes, from
   the first gate run. The 5,000-member gate cannot pass without it, and a
   mode that is off by default is not the one the sweeps test. The audit
   timer bounds the damage of a sleep bug to availability for about two
   minutes.

9. **Do Raft lease reads replace read-index rounds for `Quorum` reads in
   0.8?** Recommended: not at first. Read index is safe without any clock
   assumption. Turn lease reads on only if the `Quorum` read p99 cell misses
   the gate, and only after the model's lease configs pass.
