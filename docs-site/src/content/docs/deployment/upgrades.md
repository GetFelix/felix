---
title: Upgrades and compatibility
description: What happens when two versions of Felix meet on the client protocol, between brokers, against the control plane and on a broker's own disk, and the upgrade order that follows.
---

Two versions of Felix can meet in five places. This page says what happens on
a mismatch in each. What changed in each release is in the
[release notes](/release-notes/), and the line in development has an
[upgrade page](/release-notes/upgrading-to-0-7/).

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

**Storage identities during a roll.** A broker now registers the identity of
its data directory, and its replica reports name the identity each follower
was counted under. Placement never promotes a follower whose two identities
differ, and keeps a dead leader's last report believed past the usual window
for one whose identities match. The control plane also stamps each report
with the leader that made it and the incarnation of every node it names, and
counts a follower without an identity only from a fresh report taken since it
last registered. Until every instance runs this release, a registration or report
written through an older instance carries neither identity nor stamp: that
follower is judged by the report's age alone, as on the previous release,
until a current instance takes the broker's next registration or the leader's
next report. A Raft control plane stores identities once every member is at
metadata level 12, and the stamps at level 13; until then a follower reads as
unverified and a report as unstamped. A broker on the previous release
registers without an identity, so its followers count only through the stamp.
So until both the control plane and the brokers are rolled, a follower that
lost its disk can still be promoted: before the control plane, on what was
reported before it restarted, and before the brokers, on what a leader that
cannot see its identity reports after. Even with the control plane rolled, a
follower whose registration lost its identity (through an older instance, or
from an older broker) can be counted on what its leader reports from the old
connection after the restart, until the leader redials it: at most QUIC's
idle timeout after a crash ([#1281](https://github.com/GetFelix/felix/issues/1281)).
Avoid wiping or replacing a broker's disk until the roll is done. A leader that
dies while placement is slow can leave its shard unplaceable, as before.

**Clean stops and crashes.** A broker now records in
`<FELIX_DURABLE_STORAGE_DIR>/run-state` whether it stopped cleanly. Under
`FELIX_DURABLE_FSYNC_MODE` `periodic` (the default) or `none`, one that
crashed, or whose SIGTERM drain ran past `FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS`,
starts under a new storage id, and its leaders copy each shard it follows
again. The first start on this release finds no record and keeps its id
unless startup would repair a torn tail, so a roll of brokers stopped with
SIGTERM changes no id. A leader that crashes that way gives up the shards it
led instead of reopening them with the shorter log, and the control plane
promotes a follower for each, or names it again when every follower holds
less than it kept ([#1295](https://github.com/GetFelix/felix/issues/1295)). Give the drain time
to finish during a roll; a broker killed partway through costs one copy per
shard it follows, and a failover per shard it leads. A downgrade and
upgrade again keeps whatever the newer release last recorded, so a crash on
the older release in between is not detected.

**A first start of the metadata Raft group.** Members now record each other
before any of them votes
([forming the group](https://github.com/GetFelix/felix/blob/main/docs/metadata-raft-design.md#forming-the-group)),
over a new peer route, `POST /internal/raft/v2/forming`. A group that already
runs never uses it, so upgrading one within a release line needs nothing
(from 0.7 to 0.8 the group starts over: see
[Control plane metadata in 0.8](#control-plane-metadata-in-08)). A group that has not
formed yet needs every control-plane member on a release with the route: a
member on an older release answers 404, and the others wait and log
`it runs a build without the forming route`. Start a new cluster with every
member on the same release.

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

**A new cluster starts with them finalized.** A cluster that never ran an
older release has nothing to roll back to, so its first broker registration
finalizes every feature its brokers report, in order, except
`publisher_principal`, which stays an opt-in because it grows every record by
its publisher's id, `record_generations`, the one-way move to a new storage
format (see [`record_generations`](#record_generations)), and `joint_fence` and `replica_election`, which an operator
finalizes (see [Failover without the control
plane](/deployment/failover-without-control-plane/#turning-it-on)). The control plane treats a cluster as new when no stream
or cache was ever created and no broker has registered twice, and, under the
Raft backend, only once every member runs this release. It finalizes
through the same checks as the command below, so a feature some broker does
not report stays off, along with the features that need it. If a
control-plane member is still starting when the brokers register, the
control plane retries the finalize in the background until every member
answers, for up to ten minutes. If it gives up, it logs a warning, and the
features are finalized as on an upgraded cluster, with the runbook below. Everything below applies to a cluster upgraded from an older
release, and to `publisher_principal` and `record_generations`.

#### A new cluster: the first broker's configuration wins

Two broker settings drop features from what a broker reports:
`FELIX_QUORUM_READS=lease` drops `lease_free_reads`, and
`FELIX_INTERNAL_FENCE=false` drops `majority_ack`, `lease_free_reads` and
`fenced_caches`. The first broker to register decides what a new cluster
finalizes, and finalizing is one-way, so give every broker the same values
before any of them starts.

A broker started later with a setting the first one did not have is refused
at registration with 409, logs the reason and exits:

```text
node broker-2 does not support fleet feature(s) lease_free_reads that the
fleet has enabled; run a build that has them, since an enabled feature cannot
be disabled; a broker started with FELIX_QUORUM_READS=lease does not support
lease_free_reads: unset FELIX_QUORUM_READS (the default reads by a round) and
restart the broker
```

With `FELIX_INTERNAL_FENCE=false` the message lists every finalized feature the
broker lacks (`majority_ack`, `lease_free_reads` and `fenced_caches`) and
carries both hints: unset `FELIX_QUORUM_READS` and unset
`FELIX_INTERNAL_FENCE`. If only the fence is off, the reads hint still shows,
because `lease_free_reads` is among the missing features. To recover, remove the setting from
that broker and restart it; it registers normally. If you meant the whole
cluster to run with the setting, the features cannot be turned off again:
create the cluster afresh (a new control-plane store and empty broker data
directories) with the setting on every broker from the start.

#### Runbook: rolling out a fleet feature

1. **Roll every broker** to the new build, one at a time as in
   [Upgrade order](#upgrade-order). Nothing turns on yet.
2. **Verify.** Run the new build for as long as you would want the option to
   roll back. Check that the feature shows as supported and that no broker is
   missing it:

   ```bash
   felixctl features ls
   felixctl features finalize jump_hash_routing --dry-run
   ```

   The dry run names any live or draining broker that lacks the feature. A
   broker that is down is not counted, and will be refused if it comes back
   on the old build, so replace or retire it first.
3. **Finalize.** This is the point of no return for the feature:

   ```bash
   felixctl features finalize jump_hash_routing --yes
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
- **Change a stream's consistency with every control-plane member
  upgraded.** A pass that read a stream as `Quorum` must not replace its
  leader on its followers' word after the stream became `Leader`, so the
  change fences placement. It takes every member at metadata level 19; until
  then a change of consistency fences nothing, as before.
- **A second failover in a row needs every control-plane member upgraded.**
  A leader promoted by a failover reports the identity that failover named
  each follower by until it reaches the follower, so the next failover can
  count that follower's fence answer. The report keeps it only once every member is at
  metadata level 20; until then such a follower's answer does not count, as
  before, and a shard that needs it waits.
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
- **Before the finalize reaches a broker.** A broker that stands for election
  (`FELIX_INTERNAL_ELECTION=true`) confirms each `Quorum` read with both the
  lease and the round until it hears the feature was finalized. Cut off from
  the control plane when you finalize, it never hears, and its replicas may
  elect a successor without waiting out its lease. Other brokers stay on the
  lease until they hear: no election can pass them while they hold it. A
  replica running with `FELIX_INTERNAL_FENCE=false` never answers the round,
  so if most of a shard's replicas run that way, reads on an election broker
  fail with `leadership_lost` until the finalize. Turn election on across the
  whole fleet rather than mixing it with the fence off.
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

#### `atomic_commit`

Once finalized, a cluster member accepts atomic commits: an event and its state
updates written as one record. Until then a commit over QUIC is refused with
`invalid_request` ("atomic commits are not enabled for this fleet"). The
record needs storage format v5 and a replication mark an older build refuses.

- **Replication carries it.** A replica that predates the record would refuse
  the batch and stop replicating, so the cluster waits for the whole fleet.
- **A single broker** accepts commits from the start: it has no replicas.
- **A new cluster** starts with it finalized.
- **What clients see.** Nothing until a client commits. See
  [Atomic commits](/clients/rust/groups-and-commits/#atomic-commits).

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

Before the finalize a deleted stream's data stays on the brokers, and the
control plane refuses with 409 to create a stream under a name deleted since
this release, naming the finalize to run first. A stream created again under
an older deleted name still reopens the old data. That data goes once the name
is deleted again after the finalize.

- **Upgrade every control-plane instance first.** Under the Raft backend the
  finalize is refused with 409 until every member is at metadata version 10,
  when proposed through an upgraded member. A member below version 10, such
  as one still on `v0.6.0-preview.7`, does not know the feature and accepts
  it. That is safe from version 14 on: until every member is at 14, streams
  are created at incarnation 0 and no delete of one records a tombstone, on
  every member. A cluster that ran an unreleased build between versions 10
  and 13 may need members rebuilt from a snapshot; see
  `docs/metadata-raft-design.md`.
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

#### `cache_incarnations`

The same as `stream_incarnations`, for caches. Once finalized, every cache the
control plane creates gets an incarnation, and brokers keep each
incarnation's entries and counters in directories of their own, so a cache
deleted and created again starts empty on every broker, including one that
was down for the delete.

- **Upgrade every control-plane instance first.** Under the Raft backend an
  upgraded member refuses the finalize with 409 until every member is at
  metadata version 14. A member still on the older release does not know
  the feature and accepts it. That is safe, because the members still apply
  every entry alike. Until every member is at version 14, caches are created
  at incarnation 0 and no cache delete records a tombstone; a cache create
  through an upgraded member is refused with 409 meanwhile. Finalize through
  an upgraded member anyway.
- **Every broker must report it.** A broker that predates it would open a new
  cache in the directories a deleted cache of that name used.
- **Nothing moves on disk.** Existing caches keep incarnation 0, their
  directory names and their data. Before the finalize a deleted cache's data
  stays and a cache created again under the name reopens it, as before.
- **Brokers check it.** Once a broker has seen a cache with an incarnation it
  opens a cache's logs only at the registered incarnation, refuses stale
  registrations (`felix_broker_cache_incarnation_refused_total`), and a
  follower refuses cache and counter records for an incarnation it does not
  hold.
- **The finalize is one-way.**
- **What clients see.** Nothing. Cache answers carry `incarnation` once it is
  not 0.

#### `head_trim`

Once finalized, a durable stream accepts head trims: a trim record that hides
the stream's history below an offset. Until then a broker in a cluster refuses
every trim and writes nothing. A `trim_before` over QUIC is refused with
`feature_not_enabled` and `detail.reason` `head_trim`. The Kafka listener
has no request that trims.

- **It is the one-way step.** A trim record is storage format v7, which an
  older broker refuses to open. Take a backup first.
- **Replication carries it.** A trim ships with a mark of its own, and a
  follower that predates the feature would refuse the batch and stop
  replicating there. A leader also sends a batch holding a trim only to a
  follower that offered `HEAD_TRIM` in its handshake, and holds the others at
  the trim, counted as `trim_unsupported` in
  `felix_broker_replication_shipped_total`.
- **A single broker** allows trims from the start: it has no followers.
  A data directory that trimmed as a single broker joins a cluster only once
  that cluster has finalized `head_trim`. A log that has trimmed holds
  records an older follower or leader cannot read, and a promoted older
  leader's fetch of it fails to decode, which stalls that shard's failover.
  So the first trim writes a `head-trimmed` marker in the data directory,
  before the trim record, and a broker holding it that registers with a
  fleet that has not finalized `head_trim` deregisters and stops with an
  error saying so. Finalize `head_trim` first, then start it.
- **Tokens.** The control plane mints the `stream.trim` grant into broker
  tokens only once `head_trim` is finalized, since a broker that predates
  the action refuses a whole token naming it. `stream.manage` lets a holder
  trim before that.
- **What clients see.** Nothing until something trims. Reads below a trim's
  floor are then refused as too old, with the trim's headers for a client
  that offered `FEATURE_HEAD_TRIM`.

#### `record_headers`

Once finalized, a durable stream in a cluster accepts record keys and headers
from clients that send them, stores them with the record, and replication and
forwarding carry them. Until then a broker in a cluster does not offer
`EXT_RECORD_HEADERS`, so a client never sends any. A frame that carries them
anyway is refused as invalid (`unsupported frame flags`) and nothing is stored.

- **It is the one-way step.** A record with headers is storage format v8,
  which an older broker refuses to open. A log moves onto v8 only at the first
  record that has headers, so a cluster whose clients never send any stays
  readable by older builds. Take a backup first.
- **Replication carries it.** A batch holding a block goes only to a follower
  that offered `RECORD_HEADERS` in its handshake. A follower that did not is
  held at the first such record rather than sent it bare, counted as
  `headers_unsupported` in `felix_broker_replication_shipped_total`. The
  finalize needs every broker to report the feature, so no follower is held
  once it is done.
- **MQTT forwards** need the owner to be on a build that offers
  `SEQUENCED_FORWARDS` too. During a rolling upgrade, an MQTT message that
  reaches an upgraded broker for a shard an older broker owns is refused
  `not_owner`, which is what every broker did before forwarding existed. It
  is forwarded once the owner is upgraded.
- **Kafka keys and headers** reach this format only with
  `FELIX_KAFKA_RECORD_HEADERS=true`, which is off by default. Enabling the
  Kafka listener alone never moves a log to v8, though nearly every Kafka
  producer sends keys. Setting it is the one-way step for those producers.
- **A single broker** accepts headers from the start, unless
  `FELIX_RECORD_HEADERS=false`. A data directory that stored a record with
  headers joins a cluster only once that cluster has finalized
  `record_headers`: an older follower would be held at the record, and an
  older leader promoted over it could not read it. So the first such record
  writes a `record-headers` marker in the data directory, before the record,
  and a broker holding it that registers with a fleet that has not finalized
  `record_headers` deregisters and stops with an error saying so. Finalize
  `record_headers` first, then start it.
- **A new cluster** starts with it finalized.
- **What clients see.** Nothing changes for a client that never sends headers.
  One that does learns at connect time whether the cluster accepts them.

#### `record_generations`

Groundwork for running each shard on Raft, which needs the term of every
entry in the log itself. Once it is finalized, each durable stream log starts
its next segment in storage format v9, with an 8-byte generation in every
record header: the generation of the leader that wrote the record, carried to
every follower with it. Clients see nothing: reads, offsets and payloads are
unchanged. Finalize `generation_start` first; the control plane refuses this
one until it is.

```sh
felixctl features finalize record_generations --dry-run
felixctl features finalize record_generations --yes
```

- **It is the one-way step.** A broker older than this release refuses to
  open a v9 segment, so after the finalize no broker can be rolled back to
  one. Take a backup first. Nothing changes on disk until you finalize: the
  feature turns nothing on by being supported.
- **Only new segments move.** The segment a log is writing when the broker
  hears of the finalize is never rewritten. The log moves at its next roll,
  so a quiet stream can stay on the older format for a while.
- **Committed votes.** The same gate lets a shard save a committed vote,
  which the previous release also refuses. Nothing commits a vote yet.
- **A data directory that has a v9 segment** joins a cluster only once that
  cluster has finalized `record_generations`. The broker writes a
  `record-generations` marker in the data directory before the first v9
  segment, and a broker holding it that registers with a fleet that has not
  finalized the feature deregisters and stops with an error saying so.
- **A new cluster** does not finalize it; it stays the operator's step until
  replicated clusters are measured with it on. A single broker has no fleet
  and stays on the older format.
- **Records written before the finalize** keep no generation. A follower on
  v9 stores them as written by an unknown generation (0) rather than guess.

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

The current build reads format 9 but writes 3, which the previous release
reads, so upgrading to it is still reversible. It moves a log to a newer
version only to hold a record that needs it, and writes each such record only
once something enables it: a v4 segment for a generation-start record, once
`generation_start` is finalized (see [`generation_start`](#generation_start)
above); v5 for an atomic commit, once `atomic_commit` is; and v6 for a record
with its publisher, once `publisher_principal` is, or on a single broker with
`FELIX_RECORD_PUBLISHERS=true` (see
[`publisher_principal`](#publisher_principal)); and v7 for a head trim, once
`head_trim` is, or from the start on a single broker (see
[`head_trim`](#head_trim)); and v8 for a record with a key or headers, once
`record_headers` is, or from the start on a single broker (see
[`record_headers`](#record_headers)); and v9 for every new segment of a
stream log, once `record_generations` is (see
[`record_generations`](#record_generations)), and never on a single
broker. Every MQTT message carries headers, so an
MQTT publish to a mounted stream is what moves that stream's logs to v8;
mounting the stream is the explicit step that opts in, and a broker that
refuses headers refuses the MQTT publish too. Each of those is the one-way
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

## Control plane metadata in 0.8

0.8 does not upgrade a 0.7 control plane's metadata. There is no in-place
upgrade and no conversion tool. To move to 0.8, stop the cluster, start the
control plane and every broker on 0.8 with empty data directories, and recreate
tenants, namespaces, streams, caches and tokens (steps below).

**Why.** 0.8 keeps the control plane's Raft state (log, vote, snapshot) in
`store/` under `FELIX_RAFT_DATA_DIR`, on the same storage engine the brokers
use. 0.7 and earlier kept it in one file, `raft.redb`, in that directory.
0.8 cannot read that file, and no tool converts it.

**What happens if you start 0.8 on a 0.7 data directory.** The member
refuses to start and names `raft.redb`. It writes nothing, so starting it on
0.7 again carries on where it stopped. A 0.8 member also never talks to a
0.7 one: their peer routes differ, so a mixed group is two partitions, not
one group.

**What starts over.** Everything the control plane holds: tenants,
namespaces, streams, caches, consumer checkpoints, signing keys (so every
issued token stops working) and shard assignments. The brokers' data does
not carry over either. The new group knows nothing of the old streams and
caches, so a broker that kept its data directory never serves that data and
never deletes it: it stays on disk until the directory is wiped. Wipe the
brokers too. A 0.8 broker does not yet refuse a 0.7 data directory
([#2438](https://github.com/GetFelix/felix/issues/2438)).

**How to move a cluster to 0.8.**

1. Stop every client, broker and control-plane member.
2. Keep the old data directories if you may want to go back.
3. Start every control-plane member on 0.8 with an empty
   `FELIX_RAFT_DATA_DIR` and `FELIX_RAFT_INITIAL_CLUSTER_STATE=new`, as for a
   new cluster.
4. Start every broker on 0.8 with an empty data directory (nothing checks
   this yet, [#2438](https://github.com/GetFelix/felix/issues/2438)).
5. Recreate tenants, namespaces, streams and caches, and issue new tokens.

**Going back.** Stop everything and start 0.7 on the directories kept in
step 2. Do not start 0.7 on a directory 0.8 wrote: 0.7 does not recognise
`store/`, creates an empty `raft.redb` beside it and starts as a new member
([#2424](https://github.com/GetFelix/felix/issues/2424)).

## Rollback

| Changed | Rollback |
|---|---|
| Nothing versioned | Reverse the order above |
| Client protocol capability | Safe; clients lose the feature |
| Internal protocol *kind* | Safe once every broker is back on the old build; see the note on credentialed forwards below |
| Fleet feature, not finalized | Safe; broker by broker |
| Fleet feature, finalized | **Not possible** to a build without it; a broker on such a build is refused. Roll back to a build that still has the feature |
| Control-plane metadata version (Raft) | Safe until something at the new level is written, such as a jump-hash stream. After that, **not** below it: a member from metadata version 3 on refuses entries and snapshots with fields it does not know, and stops; an older one silently drops them. Roll the member forward |
| Control plane 0.7 to 0.8 (Raft) | Only to the 0.7 data directories kept before the move; see [Control plane metadata in 0.8](#control-plane-metadata-in-08) |
| `INTERNAL_VERSION` | Cutover again, in both directions |
| `FORMAT_VERSION` | **Not possible.** Restore from backup |

A broker rolled back from 0.7 can refuse a shard's replica set file. In the
short window before a 0.7 broker reads whether `joint_fence` is finalized, it
keeps older replica sets in that file, and an older build fails to open a log
whose file has them. Start the broker on 0.7 again and roll it back once the
shard's next replica set is written; a broker that knows `joint_fence` is off
drops the older sets at that write.

Rolling a broker back below 0.7 and forward again can leave it doubting every
copy it holds. 0.7 keeps `copy-trust` in the data directory, stamped with the
directory's storage id. If the older build gave the directory a new id while
it ran (after a machine crash, for example), the next 0.7 start finds a stamp
that does not match and treats every copy as untrusted, possibly short, and
led here before a crash. Each copy clears once its shard's leader ships to it.
Until then an older broker's failover fence to that copy is refused, so a
shard whose leader is down at that moment fails over only on its other
copies. Roll forward while the shards' leaders are up, and let each broker
catch up on its shards before you move to the next one.

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
