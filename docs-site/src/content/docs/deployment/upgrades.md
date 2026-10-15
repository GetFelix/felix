---
title: Upgrades and compatibility
description: What happens when two versions of Felix meet on the client protocol, between brokers, against the control plane and on a broker's own disk, and the upgrade order that follows.
---

Two versions of Felix can meet in five places. This page says what happens on
a mismatch in each.

## The five compatibility surfaces

| Surface | Between | On mismatch |
|---|---|---|
| Client protocol | client ↔ broker | Negotiated; old and new interoperate |
| Internal protocol | broker ↔ broker | Version bump is a **hard cutover**; new *kinds* are additive |
| Control-plane REST | broker ↔ control plane | Additive JSON; old and new interoperate |
| Fleet features | broker ↔ every other broker | Off until an operator finalizes it, which needs every serving broker to have it; then an older broker is refused |
| Storage format | broker ↔ its own disk | Unknown version **refuses to start**; no rollback |

### Client protocol: negotiated, so order does not matter

`felix-wire`'s frame flags are capabilities, not a version. A client offers
`Auth.client_flags`, the broker answers `AuthOk.server_flags`, and each side
uses the intersection. A peer that predates negotiation sends or receives a
plain `Ok`, which is read as `ORIGINAL_V1_FLAGS`. Those are the three bits that
existed before negotiation, and the set is frozen: adding to it would make new
clients assume support that old brokers do not have.

An unknown flag bit is *rejected*, not masked off, because a flag selects the
payload layout and ignoring one means confidently misparsing the body.

**So clients and brokers upgrade independently, in either order.** A new client
against an old broker loses the features the old broker did not advertise, and
nothing else.

`VERSION` (1) exists for a change negotiation cannot express, such as a
header change. Bumping it would break every client at once, and nothing has
needed to.

### Internal protocol: a version bump is a cutover

`INTERNAL_VERSION` (1) is checked on every frame. A peer speaking a
different one is refused. There is no negotiation: no capability
exchange in `Hello`, and no way to speak an older dialect on request.

**A bump therefore requires every broker to restart together.** There is no
rolling upgrade across it, and no partial cluster. That is deliberate. The
design says the version exists for "the change that cannot cover: the header, or
an existing body layout". It is still a maintenance window, and it should be
planned as one.

Adding a message kind is the additive path, and behaves differently in each
direction:

- **New broker → old broker.** The old one does not know the kind. A current
  broker steps over the frame and answers `UnsupportedKind`, keeping the
  stream. A much older one drops the stream instead, and
  because those streams multiplex every in-flight request to that peer, it takes
  the requests with it. They are retried, so the cost is latency, not loss.
- **Old broker → new broker.** Nothing happens. An old broker never sends a kind
  it does not have.

**So during a rolling upgrade, upgrade brokers one at a time and let each settle
before the next.** A new broker will send new kinds to peers that may refuse
them, and refusal is the designed outcome. What is being waited out is the
window in which that costs a dropped stream rather than a typed answer.

### Control-plane REST: additive, and the broker tolerates absence

The broker seeds from the control plane and watches its change feed. The shapes
are serde types shared through `felix-common::membership`, and optional fields
default to the pre-existing behaviour, so a field a broker does not know is
ignored and one the control plane does not send takes its default.

**Upgrade the control plane first.** A new broker may report fields an old
control plane drops (the reports still land, minus the new information), while
an old broker against a new control plane simply does not use what it cannot
see. Neither direction fails, so the order is a preference rather than a
requirement.

**Catalog feeds on Postgres during a control-plane roll.** A control plane
hands out each tenant, namespace, stream and cache change's `seq` in commit
order only when every instance writing to the database takes the feed's lock.
While older and newer control-plane instances share one database, a write
through an older instance can still commit out of order, and a broker can skip
that change until it restarts. The guarantee holds once
every control-plane instance runs a release that has it, so finish the roll
before relying on it, and keep catalog writes to a minimum while it is under
way.

### Fleet features: on only when an operator finalizes them

A change to how brokers treat each other, such as how a key maps to a shard,
cannot be negotiated per connection: two brokers can parse every frame and
still route the same key differently. Those changes ship as fleet
features. Each broker reports the ones it implements when it registers, and
the control plane tracks which ones every live or draining broker
*supports*. Support alone turns nothing on. A feature is *enabled* only when
an operator finalizes it, and brokers act on the enabled set alone.

**So a rolling upgrade is safe to stop or reverse at any point before the
finalize.** Old and new brokers mix freely, and any broker can be rolled back
on its own. Nothing changes behaviour until you finalize.

**Finalizing is one-way.** The control plane refuses to finalize while any
serving broker lacks the feature. After it, a broker without the feature is
refused at registration with 409 naming the feature, and exits, rather than
being let in to switch the fleet's routing back. That holds for a broker that
was down during the finalize too.

#### Runbook: rolling out a fleet feature

1. **Roll every broker** to the new build, one at a time as in
   [Upgrade order](#upgrade-order). Nothing turns on yet.
2. **Verify.** Run the new build for as long as you would want the option to
   roll back. Check that the feature shows as supported and that no broker is
   missing it:

   ```bash
   felix-controlplane admin features
   felix-controlplane admin features finalize jump_hash_routing --dry-run
   ```

   The dry run names any live or draining broker that lacks the feature. A
   broker that is down is not counted, and will be refused if it comes back
   on the old build, so replace or retire it first.
3. **Finalize.** This is the point of no return for the feature:

   ```bash
   felix-controlplane admin features finalize jump_hash_routing
   ```

   Every broker turns it on within a heartbeat. Watch
   `felix_broker_fleet_feature_enabled{feature="jump_hash_routing"}` go to 1
   on each broker, or `GET /v1/fleet/features`.

After step 3, rolling back means a build that still has the feature. There
is no way to disable a finalized feature.

What finalizing `jump_hash_routing` changes: streams created from then on map
routing keys to shards with jump consistent hashing, which keeps most keys in
place should a stream's shard count ever grow (it cannot change today). Streams that already exist keep the modulo mapping they were created
with, and nothing about them moves.

#### `generation_start`

Once finalized, a leader writes a generation-start record whenever it starts
leading a stream shard (a promotion, either end of a move, a cancelled move
handing the shard back), and its quorum mark counts only past that record. A
cache leader writes one on the shard's cache log and one on its counter log,
and each mark counts only past its own log's record.
That closes a way a promotion could lose an acknowledged record (Raft's
Figure 8; see the replication design notes). Until it is finalized, brokers
count as before and keep that exposure. In-memory streams never write the
record: their publishes take no log offsets and are not replicated, so there
is nothing inherited for it to cover, and their shards open on promotion as
before.

- **Finalize it only after every broker runs a version that supports it.**
  The control plane refuses the finalize otherwise, and the dry run above
  names the brokers still missing it.
- **It is one-way twice over.** Beyond the gate refusing an older broker, the
  first record rolls each log onto a v4 segment, which an older build cannot
  open (see [Storage format](#storage-format-the-one-that-does-not-roll-back)).
  Take a backup before finalizing.
- **Shards already serving are not interrupted.** A shard keeps the
  generation it had when you finalized, and its leader writes no record at
  that generation, even when it restarts: the records it holds at that
  generation are its own. Its first record comes with its next leadership
  change.
- **Subscribers see a gap.** The record takes an offset that no reader
  delivers. A client that negotiated `FLAG_EVENT_BATCH_SKIPPED` (`0x0800`, see
  [capability negotiation](/architecture/wire-protocol/#capability-negotiation))
  is told about it as `skipped_before` on the next batch. An older client that
  treats every offset jump as a drop reports a drop of one at each leadership
  change.

#### `majority_ack`

Once finalized, together with `generation_start`, a `Quorum` stream
acknowledges a write as soon as a majority of its replicas has answered that
it holds it at the leader's generation. Neither the control plane's replica
report nor the leader's lease is on the write's path any more, so a leader
that loses the control plane keeps acknowledging what its followers hold, and
a promoted leader always fences a majority before it serves. `Leader`
streams and reads keep the lease, and so do caches until `fenced_caches` is
finalized.

- **Finalize `generation_start` first.** `majority_ack` has no effect
  without it, so the control plane refuses to finalize it until
  `generation_start` is enabled.
- **Upgrade every control-plane instance first.** Don't finalize
  `majority_ack` while any control plane runs a build that rebuilds a
  failed-over `Quorum` shard's replica set (see the changelog entry "A
  `Quorum` failover keeps its replica set"). Such a control plane swaps the
  dead leader for a spare broker, and the promoted leader's fence can then
  open without a write the old set acknowledged: when the cluster has more
  brokers than the replication factor, or when a majority of the set is down.
- **Every broker must fence on promotion.** A broker running with
  `FELIX_INTERNAL_FENCE=false` does not report `majority_ack`, so the dry run
  names it and the finalize is refused until it runs with the fence. After
  the finalize, such a broker is refused at registration.
- **Nothing changes on the wire or on disk.** Rolling a broker back to a build
  that has the feature is safe. Rolling it back to one without it is refused,
  as for any finalized feature.
- **What clients see.** A publish to a leader that has been replaced but has
  not heard yet waits and times out as "unknown". It is not refused at once
  for a lapsed lease. The client retries it against the new leader,
  and an idempotent producer keeps it from landing twice.

#### `lease_free_reads`

Once finalized, together with `majority_ack` and `generation_start`, a get or
counter get on a replicated `Quorum` cache no longer trusts the lease. After
the read takes its value, the broker sends the promotion fence at its own
generation to the shard's replicas and answers once a majority, itself
included, has taken it.

It also takes readers and consumer groups on replicated `Quorum` shards off the
lease. Subscriptions, replay, Kafka fetches and cache watches keep going
through a lapsed lease, because they only see the committed mark. They end when
a replica refuses the broker for a newer leader, or when no majority has
confirmed it for a lease duration, and the client finds the shard again as it
does after a move. A group poll, ack, nack or dead-letter change is confirmed
by the same round before it is acknowledged.

- **Finalize `generation_start` and `majority_ack` first.**
  `lease_free_reads` has no effect until all three are finalized, so the
  control plane refuses it until both are enabled. Placement fails a
  `Quorum` stream over on its followers' word only once all three are, and
  only against a leader whose latest replica report says it serves by round.
  A broker learns of the finalize on its next heartbeat and reports it after,
  so for a heartbeat or so after the finalize leaders still fail over on the
  lease. A Raft control plane keeps the report's flag only once every member
  is at metadata level 11.
- **Finalize with every control-plane member upgraded.** The order is checked
  by the member that serves the `finalize` call. During a rolling
  control-plane upgrade an older member does not check it, so finish the
  control-plane upgrade first, or finalize in order yourself.
- **Every broker must fence on promotion and read by round.** As for
  `majority_ack`, a broker running with `FELIX_INTERNAL_FENCE=false` does not
  report the feature, and neither does one running with
  `FELIX_QUORUM_READS=lease`. The dry run names it and the finalize is
  refused until it runs without either.
- **Nothing changes on the wire or on disk.** The round is the existing
  `Fence`, sent at a generation the replica already accepted, which writes
  nothing.
- **What clients see.** A broker cut off from the control plane keeps its
  subscribers, watches and group consumers. A group operation on a broker
  whose replicas no longer answer it fails with `leadership_lost` instead of
  `shard_unavailable`, and each group write waits for one round.
- **What clients see.** A read costs a round trip to the nearest majority
  (concurrent reads of a shard share rounds). A leader that loses the control
  plane keeps serving reads its replicas confirm. One cut off from its
  replicas refuses them as `leadership_lost`, which is retryable, as soon as
  the round fails instead of at its lease's expiry.
- **Keeping the lease.** `FELIX_QUORUM_READS=lease` keeps a broker's reads on
  the lease, the faster path that is only as safe as the clocks and the lease
  margins. It keeps the whole fleet there: the broker does not report
  `lease_free_reads`, so the feature cannot be finalized while it serves, and
  once the feature is finalized a broker started with it is refused at
  registration. The refusal names `FELIX_QUORUM_READS`: unset it (reads then
  confirm by a round, the default) and restart the broker. Before upgrading a
  fleet that already finalized `lease_free_reads`, remove
  `FELIX_QUORUM_READS=lease` from every broker, or each upgraded broker is
  refused.

#### `publisher_principal`

Once finalized, a durable stream stores the principal that published each
record, and subscribers and consumer groups that ask are told it (see
[Who published an event](../../api/broker-api/#who-published-an-event)).

- **It is the one-way step.** A record with its publisher is storage format
  v6, which an older broker refuses to open, and the feature is what lets a
  broker write one. Take a backup first.
- **Replication carries it.** A follower that predates the feature refuses a
  batch with publishers, which is why it waits for the whole fleet.
- **Cost.** Each record grows by its publisher's length plus one byte: about
  65 bytes for a 64-character principal id. A record from a connection with no
  principal grows by nothing.
- **A single broker** has no fleet; `FELIX_RECORD_PUBLISHERS=true` turns it on
  there, with the same one-way step.
- **What clients see.** Nothing unless they ask. A client that asked gets the
  publisher on records written after the finalize, and none on older ones.

#### `group_cursor_dependencies`

Once finalized, each consumer-group cursor records the dead letters it passed
records on, and a broker puts a cursor whose dead letters it no longer has
back where one stands before it serves the shard. That closes a crash that
keeps the cursor log's tail and loses the dead-letter log's, which otherwise
skips a record.

- **It is the one-way step for consumer groups.** A cursor written this way
  is 33 bytes, and an older broker refuses to read it: its groups on that
  shard fail until it is upgraded again. Take a backup first.
- **A single broker** writes them from the start.
- **What clients see.** Nothing, unless a crash or failover left a cursor
  without its dead letter: the group then redelivers from where it last
  stood instead of skipping.

#### `fenced_caches`

`majority_ack` for caches. Once finalized, together with `majority_ack` and
`generation_start`, a replicated `Quorum` cache acknowledges a put, delete or
counter add as soon as a majority of its replicas has answered that it holds
it at the leader's generation, without the report or the lease. A promoted
cache shard never opens on the lease: it fences a majority on its cache log
and its counter log, and takes the furthest ahead of each, before it serves.

Before the finalize, a broker of this build already fences a promoted cache
shard whenever every replica offers the `CACHE_FENCE` capability, and opens
it on the lease otherwise, but cache writes are still acknowledged on the
report and the lease. A broker of an older build can be promoted to a cache
shard without fencing it, which is why the lease stays until every broker
fences caches.

- **Finalize `generation_start` and `majority_ack` first.** `fenced_caches`
  has no effect until all three are finalized, so the control plane refuses
  it until both are enabled.
- **Upgrade every control-plane instance first.** An older control plane
  rebuilds a failed-over `Quorum` cache's replica set instead of keeping it,
  so the promoted leader's fence could open without a write the old set
  acknowledged, as for `majority_ack` and streams.
- **Every broker must fence on promotion.** A broker running with
  `FELIX_INTERNAL_FENCE=false` does not report `fenced_caches`, so the dry run
  names it and the finalize is refused until it runs with the fence.
- **Nothing changes on the wire or on disk.** A cache shard's logs already
  get their generation-start records once `generation_start` is finalized.
- **What clients see.** A cache write to a leader that has been replaced but
  has not heard yet times out as "unknown" rather than being refused for a
  lapsed lease, as a `Quorum` publish does under `majority_ack`. A promoted
  cache shard waits for a majority of its replicas rather than opening alone.

#### `stream_incarnations`

Once finalized, every stream the control plane creates gets an incarnation,
higher for every creation of its name, and brokers keep each incarnation's
logs, group positions and dead letters in directories of their own. A stream
deleted and created again under the same name then starts empty on every
broker, including one that was down for the delete: the control plane keeps a
tombstone per deleted name and serves it in the catalog snapshot.

Before the finalize nothing changes: a deleted stream's data stays on the
brokers, and a stream created again under the name reopens it, as before. Its
shards restart at the generations the old one used, where a new, empty log
would be judged by what the brokers kept for the old one. That data goes once
the name is deleted again after the finalize.

- **Upgrade every control-plane instance first.** Under the Raft backend the
  finalize is refused with 409 until every member is at metadata version 10.
  With Postgres, the first upgraded instance runs migration 0028 for the whole
  database. An instance from before this release that is still running after
  the finalize cannot create streams (the database refuses a stream without an
  incarnation), its deletes still record tombstones, and the database stamps
  its shard assignments with the incarnation. Its catalog answers leave
  incarnations out, which brokers refuse rather than misread, so upgrade every
  instance before finalizing.
- **Under Raft, deletes keep shard state until every member is at metadata
  version 10.** Before that, deleting a namespace or tenant keeps its streams'
  and caches' shard assignments and replica reports, and deleting a stream or
  cache keeps its replica reports, as the older release does, so every member
  holds the same state. From then on a delete takes both.
- **Every broker must report it.** A broker that predates it would keep a new
  stream's records in the directory a deleted stream of that name used.
- **Nothing moves on disk.** Existing streams keep incarnation 0 and their
  directory names. Downgrading a broker below the release that added it loses
  sight of incarnated streams' data, which sits in directories it does not
  look for.
- **Brokers check it.** Once a broker has seen a stream with an incarnation it
  records that on disk, and from then on, and from the start after a restart,
  opens a stream's logs only at the incarnation the catalog registered; a
  follower refuses records for an incarnation it does not hold until its
  catalog catches up, which it polls for at once. A broker never moves a name
  to a lower incarnation or to one the control plane deleted, and refuses a
  catalog record without the incarnation for a name it has seen incarnated;
  `felix_broker_stream_incarnation_refused_total` counts both.
- **The finalize is one-way.** As with every fleet feature, it cannot be
  undone, and a broker that does not report it is refused from then on.
- **What clients see.** Nothing. Stream answers carry `incarnation` once it
  is not 0.

A control plane older than fleet features sends none, so brokers keep them
all off. Under the Raft backend a broker's features are kept, and a feature
can be finalized, only once every control-plane member is at metadata
version 2; a broker registered before that reports them again on its next
restart. `jump_hash_routing` needs metadata version 3, because a member
before it would store a jump-hash stream as modulo: until every member is
there the finalize, and any request for a `jump_hash` stream, is refused
with 409. Upgrade the control plane first.

Under the Raft backend a member is added to the group only if it runs a
build at or above the metadata version the group already uses: the one a
finalized fleet feature needed (10 for `stream_incarnations`, 3 for
`jump_hash_routing`, 2 for the others), or the newest one any committed
entry needed, such as 10 once a delete has taken shard state. The leader
asks the new member before the membership changes and refuses a lower
build, or one that does not report a version, with an error naming both
versions. Added anyway, it would refuse entries the other members apply,
and its metadata would part ways with theirs. While neither applies, an older
build may join, as it would part-way through a rolling upgrade.

A member already in the group that comes back on an older build, for
example after its volume was lost, is held rather than fed: the leader
sends it no entries and no snapshot, logs a warning once, and sets
`felix_meta_raft_member_below_floor{member}` to 1. The leader holds a
member only on evidence: the member reported an older version, or it lost
the log it had acknowledged and has not reported a version since. A member
that answers with a conflict before it has accepted anything from the
current leader, as an empty one does, is held only until a probe of that
member alone answers, or gives up after 200 ms and a second try of up to a
second: a version at the group's or no answer releases it. After a leader change, a member that is behind can be
held that briefly for each conflict while the leader finds where their logs
match. A member that is slow to answer or cannot be asked is otherwise not
held. A held member that is behind gets no heartbeats either, so it may
call elections; the others refuse them while they hear from the leader, but
once it is released it can cost one extra election. The group keeps committing with the other members, but a held
voter is one fewer failure the group can survive. Upgrade that member; replication to it resumes within about a
second of it starting on a build at the group's version.

### Storage format: the one that does not roll back

The format version is in every segment header, and one newer than the build
knows is a `Corruption`, not a warning. A broker will not open a log written
by a newer build.

That makes a storage format bump irreversible without restoring from backup: a
broker that has written one segment at the new version cannot be rolled back to
the old build, because the old build refuses to read its own data directory.

Two things soften it, and neither is a rollback path:

- **Indexes are derived.** A `.index` file whose version does not match is
  rebuilt from the segment it describes, so the index format can change freely.
- **The generation history is derived too.** An `epochs` file that is absent,
  short, or fails its checksum reads as empty rather than failing, which costs
  automatic divergence repair and never a record.

Shard state files beside the segments are not versioned with them. The
`ballot` file a replica keeps from this release on, naming the leader it
accepted its generation from, is new: an older build ignores it and opens the
shard as before, so rolling back is safe. Rolling forward again, the first
leader to reach a replica at its accepted generation is recorded as the one it
accepted.

**So before an upgrade that changes `FORMAT_VERSION`: take a backup, and treat
the rollout as one-way.**

The current build reads format 6 but writes 3, which the previous release
reads, so upgrading to it is still reversible. It moves a log to a newer
version only to hold a record that needs it, and writes each such record only
once something enables it: a v4 segment for a generation-start record, once
`generation_start` is finalized (see [`generation_start`](#generation_start)
above); v5 for an atomic commit, once `atomic_commit` is; and v6 for a record
with its publisher, once `publisher_principal` is, or on a single broker with
`FELIX_RECORD_PUBLISHERS=true` (see
[`publisher_principal`](#publisher_principal)). Each of those is the one-way
step. Consumer-group cursors are not in the segment format but have a layout
of their own: a cluster moves to the newer one once
[`group_cursor_dependencies`](#group_cursor_dependencies) is finalized, and a
single broker from the start, after which an older build cannot read that
shard's groups.

## Upgrade order

For a release that changes none of the versions above (the ordinary case):

1. **Control plane**, instance by instance. Readiness takes an instance out of
   rotation while it restarts, and brokers serve from the catalog they already
   hold while it is away. Under the Raft backend, restarting the *leader* pauses
   metadata writes for one election (about 1.2s).
2. **Brokers**, one at a time, waiting for each to report ready and for its
   shards to be back in their replica sets before the next. `GET
   /replication/halted` on the metrics port should be empty before continuing.
   A replica that halted during the previous restart is one that will not be
   there for the next.
3. **Clients**, whenever. The protocol negotiates.

If the release changes `INTERNAL_VERSION`, step 2 is not rolling: stop every
broker, upgrade, start every broker.

If it changes `FORMAT_VERSION`, back up first and do not plan to roll back.

If it adds a fleet feature, the feature stays off after step 2 until you
finalize it. See the runbook above.

## Rollback

| Changed | Rollback |
|---|---|
| Nothing versioned | Reverse the order above |
| Client protocol capability | Safe; clients lose the feature |
| Internal protocol *kind* | Safe once every broker is back on the old build; see the note on credentialed forwards below |
| Fleet feature, not finalized | Safe; broker by broker |
| Fleet feature, finalized | **Not possible** to a build without it; a broker on such a build is refused. Roll back to a build that still has the feature |
| Control-plane metadata version (Raft) | Safe until something at the new level is written, such as a jump-hash stream. After that, **not** below it: a member from metadata version 3 on refuses entries and snapshots with fields it does not know, and stops; an older one silently drops them. Roll the member forward |
| `INTERNAL_VERSION` | Cutover again, in both directions |
| `FORMAT_VERSION` | **Not possible.** Restore from backup |

## What this does not cover

Topology, install steps and replacing a persistent volume are on
[Kubernetes Deployment](/deployment/kubernetes/). Adding, draining and
removing a broker have their own page:
[Adding, draining and removing brokers](/deployment/scaling/).

The two observable checks available during any upgrade:

```bash
# Which replicas replication has stopped for. Empty before you continue.
curl -s http://broker:8080/replication/halted | jq

# What a broker would actually run with, without starting it.
felix-broker --print-config
```

See [Observability](/features/observability/) for what to watch while a
rollout is in progress.

### A note on credentialed forwards

Forwarded publishes and cache operations carry the client's credential on
kinds of their own, and an upgraded owner refuses the credential-less legacy
kinds. That refusal is the fix, not a side effect. During a rolling broker
upgrade the two builds meet in both directions, and they behave differently:

- **Upgraded broker forwarding to an old owner.** The old owner answers
  `UnsupportedKind`. The forwarder sends the legacy kind once instead, and the
  publish goes through. That owner checks nothing either way, so nothing is
  lost that the upgrade had gained.
- **Old broker forwarding to an upgraded owner.** Refused `Unauthorized`, and
  the client's publish fails, until that broker is upgraded too.

So the window is bounded by how long un-upgraded brokers keep forwarding to
upgraded ones: upgrade brokers quickly and in one pass, and expect publishes
that cross the boundary in that direction to fail with `Unauthorized` while it
is open. Rolling back closes it the same way in reverse.
