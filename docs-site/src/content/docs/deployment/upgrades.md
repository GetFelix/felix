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
handing the shard back), and its quorum mark counts only past that record.
That closes a way a promotion could lose an acknowledged record (Raft's
Figure 8; see the replication design notes). Until it is finalized, brokers
count as before and keep that exposure.

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
  [capability negotiation](/felix/architecture/wire-protocol/#capability-negotiation))
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
streams, caches and reads keep the lease.

- **Finalize `generation_start` first, or in the same change window.**
  `majority_ack` has no effect until both are finalized.
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
included, has taken it. Stream readers and cache watches keep the lease.

- **Finalize `generation_start` and `majority_ack` first, or in the same
  change window.** `lease_free_reads` has no effect until all three are
  finalized.
- **Every broker must fence on promotion.** As for `majority_ack`, a broker
  running with `FELIX_INTERNAL_FENCE=false` does not report the feature, so
  the dry run names it and the finalize is refused until it runs with the
  fence.
- **Nothing changes on the wire or on disk.** The round is the existing
  `Fence`, sent at a generation the replica already accepted, which writes
  nothing.
- **What clients see.** A read costs a round trip to the nearest majority
  (concurrent reads of a shard share rounds). A leader that loses the control
  plane keeps serving reads its replicas confirm. One cut off from its
  replicas refuses them as `leadership_lost`, which is retryable, as soon as
  the round fails instead of at its lease's expiry.
- **Keeping the lease.** `FELIX_QUORUM_READS=lease` keeps one broker's reads on
  the lease after the finalize, the faster path that is only as safe as the
  clocks and the lease margins.

A control plane older than fleet features sends none, so brokers keep them
all off. Under the Raft backend a broker's features are kept, and a feature
can be finalized, only once every control-plane member is at metadata
version 2; a broker registered before that reports them again on its next
restart. Upgrade the control plane first.

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

**So before an upgrade that changes `FORMAT_VERSION`: take a backup, and treat
the rollout as one-way.**

The current build reads format 4 but writes 3, which the previous release
reads, so upgrading to it is still reversible. It writes a v4 segment only
for a generation-start record, and only once `generation_start` is finalized
(see [`generation_start`](#generation_start) above), so that finalize is the one-way step.

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
| `INTERNAL_VERSION` | Cutover again, in both directions |
| `FORMAT_VERSION` | **Not possible.** Restore from backup |

## What this does not cover

Topology, install steps and replacing a persistent volume are on
[Kubernetes Deployment](/felix/deployment/kubernetes/). Adding, draining and
removing a broker have their own page:
[Adding, draining and removing brokers](/felix/deployment/scaling/).

The two observable checks available during any upgrade:

```bash
# Which replicas replication has stopped for. Empty before you continue.
curl -s http://broker:8080/replication/halted | jq

# What a broker would actually run with, without starting it.
felix-broker --print-config
```

See [Observability](/felix/features/observability/) for what to watch while a
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
