# Leader-held group claims on Raft shards: design

Status: design, not built. Tracks
[#3360](https://github.com/GetFelix/felix/issues/3360). Lands in the same
window as [#3359](https://github.com/GetFelix/felix/issues/3359) (a Raft entry
is a record batch), before format v9 freezes. The choices that need a decision
are listed at the end.

## What is decided

`Advance`, `DeadLetter`, `Seek` and `Delete` stay durable group entries. A
claim is held by the shard's leader with a visibility lease and is not written
to the log. A failover redelivers records that were claimed and not yet
acknowledged, which at-least-once delivery already permits. A per-group
setting keeps durable claims for workloads that need them, such as KBL's work
queue.

## What the code does today

Checked against `main` at 7c5f53d.

A group on a Raft stream shard is served by `RaftGroups`
(`crates/server/felix-broker/src/queue/raft.rs`). The leader keeps a
`GroupTracker` in memory and every member folds the committed group entries
into the same `GroupTable` (`queue/table.rs`).

A poll does three things in order. It confirms the term with a read index and
waits for this member to apply up to it (`OnRaft::fresh`,
`services/felix-broker-service/src/shards/raft/groups.rs:49`, which runs
`ReadPolicy::ReadIndex`, `felix-replication/src/raft/group.rs:3525`). On a
new incarnation's first hand-out it writes the creation `Advance`
(`queue/raft.rs:330`). Then it writes one `GroupOp::Claim` entry naming every record
it hands out, with the holder, the attempt counts and the visibility, and
answers only once that entry has applied (`queue/raft.rs:351`). An extension writes
a one-record `Claim` the same way (`queue/raft.rs:437`).

The table keeps every standing claim until the cursor passes it, a dead letter
replaces it, or a seek voids it (`queue/table.rs:33`, `:196`). Acks ahead of the
cursor and nacks are not written, so their records stay in the table as
claims. A new leader rebuilds every table claim as held for its full
visibility from the moment it hydrates (`queue/raft.rs:876`), with its attempt
count, on connection 0 so a named member's `reclaim` takes it back
(`queue/tracker.rs:501`). The snapshot carries the claims (table version 2,
`queue/table.rs:229`).

Nacks, delayed nacks and acks above a gap are leader memory already
(`queue/raft.rs:402`, `queue/tracker.rs:47`).

### The issue's claims

- "A poll answers only once a claim entry for what it hands out has applied,
  and an extension likewise." Correct.
- "A read pays commit latency: a quorum round, plus an fsync per copy on
  `on_commit`." Correct for the claim, and incomplete. A poll also pays a
  read-index round before it decides anything, and that round stays after
  this change. What goes is the commit: the append, the replication, the
  fsync on each copy and the apply wait. A poll goes from a read index plus a
  commit to a read index alone. Removing the read-index round too needs a
  lease read, which is a separate question (see "Not in scope").
- "Claim volume grows with consumers × polls." Correct in kind. It is one
  entry per poll that hands out at least one record (the entry lists all of
  them) plus one per extension. Empty polls write nothing. A long-running
  consumer that extends every few seconds, as KBL's agents do every 30 s,
  writes an entry per extension.
- "It lands under the data's retention and in every member's replay and
  snapshot." Correct for retention and replay: group entries are segment
  records with `RecordMark::Group`. The snapshot carries only the claims still
  standing, pruned at every advance, so its size is bounded by what is in
  flight, not by poll volume. Stream members refuse every snapshot install
  today (`docs/per-shard-raft.md`, "Group runtime"), so that path is not live.
- "Each claim entry uses up a stream offset." Correct: the poll skips group
  records as not data (`queue/raft.rs:305`), and `skipped_before` counts them.
  #3359 removes this for every non-data entry, claims or not.

### Doc claims the code does not back

- `docs/per-shard-raft.md` ("Consumer groups") says
  `docs/formal/FelixRaftGroups.tla` "leaves out discard ... and `if_new`". It
  also leaves out claims, visibility and attempt counts entirely
  (`FelixRaftGroups.tla`, "Abstracted"). The durable-claim rule, that a new
  leader holds every claim a consumer was told about, is checked by no
  model. Only unit and cluster tests cover it
  (`queue/raft/tests.rs`, `a_claim_survives_a_change_of_leader`;
  `felix-cluster/tests/raft_shards.rs`,
  `a_group_claim_survives_its_raft_leader_dying`).
- `docs/storage-format.md` ("Group records") lists the operations as "a
  cursor advance, a dead letter, a redrive or its finish, a discard, a seek or
  a delete", without the claim that its own layout block shows as op 7.
- Neither the history checker nor `felix-sim` exercises consumer groups. The
  history checker's delivery rules cover subscribers only
  (`felix-cluster/src/history/checker/delivery.rs`).

## What a failover costs a leader-held group

The issue names one cost: claimed, unacknowledged records are delivered
again. Reading the code, a leader change loses four more things the table
holds today, and each needs a rule or a line in the docs.

1. **Concurrent delivery.** The old holder may still be working when the new
   leader hands the record to someone else. Two consumers process it at once,
   for up to the old claim's remaining visibility. At-least-once allows it;
   consumers must be idempotent.
2. **Attempt counts.** The tracker's counts die with it. Without a rule, a
   record that has failed four times starts again at attempt 1, `attempts`
   lies to the consumer, and the dead-letter bound holds only per term.
3. **Named-member reclaim.** A member that reconnects with `reclaim` gets
   nothing back on the new leader: its records are owed to the whole group.
4. **Extensions.** An extension of a claim made under the old leader is
   refused as `group_claim_lapsed` on the new one. The consumer stops or
   polls again.

Two things get better. A nacked record is owed again at once on the new
leader, where today the table holds it as claimed for a full visibility. An
ack above a gap is redelivered at once rather than after a visibility.

## Design

### The setting

Each group has one setting, `claims`, with two values:

- `leader` (the default): claims are leader memory with a visibility lease.
- `durable`: every claim and extension is a group entry, applied before the
  answer, exactly as today.

**Default `leader`.** The decision text makes the durable mode the exception.
`leader` is what the hybrid engine did, so it is the behaviour 0.7 users had.
It takes a commit off every poll, and most queue workloads (webhook relays,
fan-out jobs, short tasks) finish within a visibility and are idempotent. The
workloads that need `durable` are the ones where two workers on one item is
expensive, which in practice means long claims kept alive by extension.

**KBL sets `durable`.** KBL's consumer group is "what keeps two agents off
the same item" (`docs-site/.../built-on-felix/agents.md`), and its agents hold
claims for minutes, extending every 30 s. A leader-held claim would let a
failover put a second agent on an item the first is still working, and cost
both their tokens. The durable acks KBL needs on `work.events` are a stream
ack level and are unrelated; this setting is about the group. KBL configures
its group once at setup; that change is filed against KBL.

The setting is per group, not per stream, because two groups on one stream
can want different things (a durable work queue and a best-effort audit
reader). It is stored in the shard's group table so every member agrees on
it, and it survives a seek (a seek resets a position, not a policy) but not a
delete.

There is no broker-wide environment default. The leader decides how a claim
is held, so an env default would let two leaders of one shard run a group
differently depending on which broker won the election.

### Group entry changes

Two new operations in the `GroupEntry` payload (`queue/entry.rs`):

```text
op 8 configure
  u8   claims: 0 leader, 1 durable
op 9 attempts                                     (creation token, as ops 0 to 4)
  u32  records, then per record ascending: u64 offset, u32 attempts
```

`Configure` carries no creation token and always applies, as a seek does. On
an absent group it creates the group at cursor 0 with a token that is its own
log id (`CreatedBy::Configure`), which is where a hydrate of an absent group
starts anyway. On a live group it sets the setting and leaves the token alone,
so writes in flight from the current tracker still apply.

`Attempts` is a slim `Claim`: no holder, no visibility. It records attempt
counts for redeliveries only (see "Poll"). The fold keeps them in a new
per-group `attempts: BTreeMap<u64, u32>`, pruned like claims at every
advance, dead letter and seek.

The table moves to version 3: per group, `claims` (u8) and the `attempts`
map after the claims. Version 2 decodes with `leader` and no attempts. No
released build writes group entries (they are 0.8 only, and 0.8 starts fresh
with no upgrade from 0.7), so version 1 and 2 decoding can be dropped in the
same change if the reviewers prefer.

`Claim` (op 7) stays, and is written only for `durable` groups and by the
hand-over below.

### Poll

For a `leader` group:

1. Read index and apply wait, as today. This is what stops a deposed leader
   that does not know it from serving a group.
2. The tracker hands out, owed records first, as today.
3. A record at its attempt bound is dead-lettered first: `DeadLetter`, then
   the `Advance` over it, both durable, before the poll answers. Unchanged.
4. On a new incarnation's first hand-out, the creation `Advance`. Unchanged.
5. If any record goes out at attempt 2 or more, one `Attempts` entry for
   those records, applied before the answer. First deliveries write nothing.
6. Answer.

A healthy group writes nothing per poll. A group that is retrying pays a
commit only on the polls that carry retries, so the cost tracks failures, not
traffic.

For a `durable` group the poll is today's.

### Extend

`leader`: the tracker moves the lease, nothing is written. The answer still
waits for the read index, so an extension answered by a deposed leader is
answered only while it led. `durable`: today's one-record `Claim`.

### Ack, nack, dead letter, redrive, discard, seek, delete

Unchanged in both modes. An ack writes the `Advance` it closes (and
`FinishRedrive` for a redriven record) and answers once applied, so an
acknowledged advance is never lost. Nacks stay leader memory. A dead letter
is written before the advance over it. Seek and delete are durable and void
every claim, leader-held or durable.

A seek on a `leader` group also drops the `attempts` map, as it drops claims.

### Configure

`Configure` runs under the group's slot lock, so no poll interleaves.

Switching to `durable` first writes the tracker's standing claims as `Claim`
entries (one per holder, each at its remaining visibility), then the
`Configure`. A failover after the answer finds every claim in the table.
Switching to `leader` writes the `Configure` alone. Claims already in the
table stay until the cursor passes them, and a later leader still honours
them. That only delays a redelivery.

### Leader change, failover and moves

A new leader hydrates the tracker from the table, as today:

- the cursor, dead letters above it settled, redriven records owed;
- `durable` groups: every table claim restored for its full visibility from
  now, on connection 0, as today;
- `leader` groups: no claims. Each record in `attempts` gets that count, so
  its next delivery reports one more. Everything at or above the cursor that
  is not settled is owed in offset order;
- `inherit_below(log tail)`, as today, so a late ack or nack of a record the
  old leader handed out is taken rather than refused. An ack settles it even
  if it has since gone to another consumer; the second ack is a duplicate,
  which is already accepted.

A record first delivered under the old leader and not yet retried has no
`Attempts` entry, so the new leader delivers it as attempt 1 again. The
undercount is at most one per leader change the record lives through. The
dead-letter bound becomes `FELIX_GROUP_MAX_ATTEMPTS` plus the number of leader
changes during the record's life, which in practice is the configured bound.

**Planned moves keep their claims.** A leadership transfer (a rebalancing
move, a drain, a rolling restart) is not a crash: the old leader knows its
claims. Before it transfers, it fences its groups (holds every slot lock so no
poll is answered), writes each group's standing claims as `Claim` entries at
their remaining visibility, waits for them to apply, then transfers. The new
leader restores them as it restores durable claims. If the transfer fails,
the leader unfences and goes on, and the extra claims in the table only delay
redelivery. This makes leader-held claims cost nothing on the moves operators
make on purpose; only crashes and partitions redeliver. It is the last PR in
the sequence and can slip without changing the guarantees.

### No double delivery inside one leader term

Within one term a record is handed to a second consumer only after its claim
lapsed, was nacked, or a seek or delete voided it. Today that holds partly
because a re-hydrated tracker in the same term restores claims from the
table. In `leader` mode the tracker's claims exist nowhere else, so **a
tracker that holds a standing leader-held claim must not be dropped while its
term lasts**. The code drops a tracker in four places:

- `write_held` on a refused proposal (`queue/raft.rs:744`). A refusal can mean the
  term is over (fine) or that the proposal was refused before it was taken
  while this member still leads. In the second case the tracker must stay;
  the failed operation unclaims only what it handed out. `GroupConsensus::propose`
  has to say which case it was.
- `write_held` when the fold dropped one of its entries (`queue/raft.rs:753`). That
  happens only after a seek or delete of the group, which voids its claims
  anyway, or before the incarnation's creation committed, when no claim has
  been answered. Safe, and the model checks it.
- `locked` when the term changed or the table no longer names the tracker.
  Safe: a new term, or the same seek or delete case.
- `ShardGroups::reset` on a rebuild from the log or a snapshot install. A
  leader must step down before either; today a stream member refuses installs.
  This needs an assertion rather than an assumption.

`evict_idle` already keeps any tracker with a standing claim.

Across terms there is no such promise in `leader` mode. In `durable` mode a
record whose claim was answered is not handed to another consumer until the
claim lapses, in any term, because the next leader restores it.

### Dead letters across failover

`DeadLetter` and the `Advance` over it are written before any poll answers,
so a dead letter is never lost and never redelivered. A dead letter whose
advance did not commit before the leader changed is settled on hydrate
(`queue/raft.rs:866`), unchanged. With `Attempts` entries a record that has been
retried keeps its count, so the bound is reached on schedule across
failovers. The undercount above affects only a record's first delivery.

## Client-visible semantics

For a `leader` group on a Raft shard, after a leader change:

- Records claimed and not acknowledged are delivered again, possibly while
  their old holder still works on them.
- A record's `attempts` can be one lower than the deliveries it has had.
- `reclaim` returns nothing; extensions of old claims answer
  `group_claim_lapsed`.
- Acknowledged advances, dead letters, redrives and seeks are kept.
- A planned leadership transfer keeps claims (once the hand-over lands).

A `durable` group behaves as every Raft group does today.

Within one leader's term both modes are the same: one holder per record while
its claim stands.

Poll latency for a `leader` group drops by one commit: one quorum append, an
fsync per copy under `on_commit`, and the apply wait.

## Wire and API

A new request, `group_configure { stream, shard?, group, claims }`, answered
with the setting now in force. `shard` omitted applies it to every shard of
the stream, as `group_delete` does. It needs the same permission as
`group_seek`. Brokers that serve it advertise a new feature bit,
`FEATURE_GROUP_CONFIGURE`; a client refuses the call to a broker without it.
`group_describe` answers `claims` too, as an optional field that defaults to
`leader`, so an old client reads the answer it always did.

`felixctl group configure --claims durable|leader`, and the call in the Rust
client. The Python, TypeScript, C# and C clients follow in their own PRs.

The hybrid engine ignores the setting: its claims are leader memory already.
It is answered there but has no effect, and `group_describe` says `leader`.

New group ops are refused by a member that does not know them, which stops
that member. No release has shipped Raft groups, so the new ops ride on the
existing `raft_consumer_groups` fleet feature rather than a new one. If a
main-to-main rolling upgrade has to keep working, they need their own fleet
feature and the leader writes neither op until it is finalized.

## Spec, simulation and history checks

**`FelixRaftGroups.tla`** gains claims. Today it abstracts them away and
assumes a claim is always acked or dead-lettered, which is why the durable
claim rule is unchecked. Added state: the tracker's standing claims, a
history of hand-outs as (record, term, claim id), a `Lapse` step that ends a
claim nondeterministically, `claim(o)` and `attempts(o)` entries, and a
`ClaimsDurable` constant for the group's setting.

New properties:

- `OneHolderPerTerm`: no record is handed out twice in one term while the
  first hand-out's claim stands.
- `DurableExclusive` (with `ClaimsDurable`): no record is handed out while an
  answered claim on it stands, in any term.
- `AttemptsKept`: a retried record's recorded attempts never go down across
  terms.
- `NoSkip` stays as it is and must still hold: an answered ack's advance is
  never lost.

New knobs, each expected to fail: `KeepTrackerInTerm = FALSE` (a refused
proposal drops the tracker; `OneHolderPerTerm` fails),
`ClaimBeforeAnswer = FALSE` with `ClaimsDurable` (`DurableExclusive` fails),
and `HandOverBeforeTransfer = FALSE` with polls not fenced during the
hand-over (`DurableExclusive` fails for the restored claims, if the model
treats hand-over claims as durable). These run in Actions, not locally.

**`FelixRaftSessions.tla`** names "a poll's claim" as a group commit. In
`leader` mode a poll is a read, covered by `FelixRaftReads`' read. The model
comment and `FelixRaftSessionsGroup.cfg` change so a poll is a read and only
acks, dead letters and durable claims are commits.

**`felix-sim`** runs no consumer groups. Add a group workload (poll, ack,
nack, extend) beside the publish workload, with the nemesis killing and
partitioning leaders, in both modes.

**The history checker** gets group rules, with a workload in
`felix-cluster/src/history/workload.rs`:

- G1, no loss: after a drain every acknowledged publish was delivered to the
  group, dead-lettered, or skipped by retention.
- G2, no regress: a record is not delivered again after its ack and every
  lower record's ack were answered, unless a seek or redrive came between.
- G3, one holder per term: two deliveries of one offset with overlapping
  claim windows come from different leaders' terms. The history records the
  answering broker and the nemesis records leader changes, which is enough to
  place each delivery in a term.
- G4 (`durable` groups only): no two deliveries of one offset have
  overlapping claim windows, allowing for the leader's clock slack.

## Interaction with #3359

#3359 makes a Raft entry a record batch with its own base offset and count,
so group entries stop using up stream offsets. Three consequences here:

- The poll's skip of group records (`queue/raft.rs:305`) and the `skipped_before`
  they cause go away with #3359, for every group entry, not just claims.
- The new ops change the `GroupEntry` payload, not the segment format. If
  #3359 moves group entries out of segment records, the payload moves with
  them unchanged. Whichever lands second rebases on the other's layout block
  in `docs/storage-format.md`.
- With claims gone from the common case, the group entries left are one per
  ack that advances, so #3359's per-entry cost argument applies to acks.
  Batching advances is a possible follow-up, not part of this design.

## PR sequence

1. **Spec.** `FelixRaftGroups.tla` with claims, lapse, the setting, the new
   properties and failing knobs; `FelixRaftSessions.tla` comment and config.
   TLC in Actions. No code.
2. **Format.** `Configure` and `Attempts` ops, table version 3,
   `CreatedBy::Configure`, round-trip and fold tests. Every group still writes
   claims, so no behaviour changes. Docs: `storage-format.md` "Group records",
   `per-shard-raft.md` "Consumer groups".
3. **Leader-held claims.** The poll and extend rules above, `Attempts` for
   retries, hydrate restoring attempts, the tracker kept on a refused proposal
   while the term lasts, and the `reset` assertion. Unit tests in
   `queue/raft/tests.rs`: a leader-held claim is redelivered after a change of
   leader; attempts survive a change of leader; a refused proposal mid-term
   does not hand a standing claim out twice (revert-checked). The existing
   durable-claim unit and cluster tests run on a group configured `durable`.
   A new cluster test for leader-held redelivery. Docs: `protocol.md`,
   `features/queues.md`, `architecture/semantics.md`, changelog fragment.
4. **Wire and clients.** `group_configure`, the feature bit, `claims` on
   `group_describe`, felixctl, the Rust client. Docs: `protocol.md` request
   and feature table, `reference/felixctl.md`,
   `clients/rust/groups-and-commits.md`. Then a KBL issue to configure its
   group `durable`, and the `built-on-felix/agents.md` line that says so.
5. **History checker and sim.** The group workload and G1 to G4. Can run in
   parallel with 4.
6. **Hand-over on planned transfer.** Fence, write claims, transfer. Cluster
   test: a leadership transfer keeps a leader-held claim.

PRs 3 and 4 can merge into one if the setting has to be reachable the day
the default changes; otherwise KBL runs on `leader` claims between them.
That is the strongest argument for landing 4 first, with the setting
accepted but having no effect until 3.

## Risks

- **The in-term drop.** The one way this design can break a guarantee it
  keeps is a tracker dropped mid-term while it holds claims. The four drop
  sites above are the whole list on main today; any new one needs the same
  review. `OneHolderPerTerm` and G3 are the checks.
- **Concurrent processing after a crash.** Users who read "a record is held
  by one consumer at a time" as an absolute will be surprised. The queues
  page has to say plainly that it holds within one leader, and point at
  `durable`.
- **Attempts undercount.** Small and bounded, but a consumer that treats
  `attempts == 1` as "never seen" can be wrong after a failover. Documented.
- **Switching to `durable` under load.** The flush of standing claims is one
  entry per holder and is bounded by `FELIX_GROUP_MAX_IN_FLIGHT`.
- **Hand-over fence.** A transfer that hangs holds polls for its duration.
  Bound the fence by the election timeout and unfence on expiry.
- **Mixed builds.** New op codes stop a member that predates them. Fine for
  a fresh 0.8 fleet, a problem for main-to-main rolling upgrades.

## Not in scope

- **Lease reads for polls.** After this change a poll still pays a read-index
  round. openraft's `ReadPolicy::LeaseRead` would remove it at the cost of a
  clock assumption on the leader. Separate issue; the history checker rules
  above would catch a lease that is too long.
- **Batching advances.** One entry per advancing ack is unchanged.
- **The hybrid engine.** Its claims are leader memory already; it is being
  retired.

## Decisions needed

1. Default `leader`, with `durable` per group. Proposed above.
2. Write `Attempts` for retries (a commit on polls that carry retries), or
   accept a per-term dead-letter bound and write nothing. Proposed: write
   them.
3. Ride on `raft_consumer_groups` or add a fleet feature for the new ops.
   Proposed: ride on it, since 0.8 starts fresh.
4. Land the wire setting (PR 4) before the default changes (PR 3), so KBL
   never runs on leader-held claims. Proposed: yes.
5. Include the planned-transfer hand-over in 0.8, or file it for 0.9.
   Proposed: 0.8, last in the sequence; rolling restarts are when operators
   would otherwise see redeliveries.
