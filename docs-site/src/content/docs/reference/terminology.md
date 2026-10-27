---
title: "Terminology"
description: "What the words used across the Felix docs, code and tools mean."
tableOfContents:
  maxHeadingLevel: 2
---

The rest of the docs use these words without stopping to define them. Each
entry says what the term means in Felix and links to the page that covers it in
depth. Terms are grouped by area. The last section lists words that mean
different things in different places, such as *fence*, *lease* and
*incarnation*.

## Resources and scoping

### Tenant

The top-level scope, usually one customer or organization. Every request names
a tenant, and a token is only good for the tenant it was issued for. Nothing is
shared between tenants. See [Security](/features/security/).

### Namespace

A scope inside a tenant, typically an environment or a team. `acme/production/orders`
and `acme/staging/orders` are unrelated streams that happen to share a name.

### Stream

A named, ordered, append-only sequence of records inside a namespace. A stream
is split into [shards](#shard), and order holds within a shard. A stream is
[ephemeral](#ephemeral-stream) unless it is created with `durable: true`. See
[Pub/Sub Streaming](/features/pubsub/).

### Ephemeral stream

A stream kept only in memory, which is the default. Delivery is at-most-once,
records are lost when the broker restarts, and a subscriber can only replay the
little the [replay ring](#replay-ring) still holds. The docs also call it an
in-memory or non-durable stream.

### Durable stream

A stream created with `durable: true` on a broker that has
`FELIX_DURABLE_STORAGE_DIR` set. Its records go to a segmented log on disk, are
replicated to followers when the replication factor is above 1, can be replayed
by offset, and are delivered at-least-once. See [Durable Storage](/architecture/durable-storage/).

### Cache

A named key/value space with per-key versions, optional TTLs and watches. It is
a log read through an index from each key to the offset of its latest write, so
a cache shard replicates and fails over the same way a stream shard does. See
[Distributed Cache](/features/cache/).

### Counter

A cache-scoped number stored as a log of signed deltas and read as their sum.
`counter_add` returns the sum including your delta. Adds are at-least-once, so
a retried add can count twice. See [Rate Limits and Counters](/use-cases/rate-limits-and-counters/).

### Queue

A stream read through a [consumer group](#consumer-group). A queue is a way of
reading a durable stream, so any durable stream can be used as one. A stream's
`kind: Queue` field does not change how it behaves. See
[Queues and Consumer Groups](/features/queues/).

### Projection

One way of reading a shard's log. A stream reads it by offset, a cache by key,
and a consumer group through a shared cursor. All three read the same log, and
nothing is copied into a second store. The docs sum this up as "one core log,
many semantics". See [Projections](/architecture/projections/).

### Record

One entry in a shard's log. It carries its payload, offset and checksum, and
optionally an append time, the idempotent producer's id and sequence, and the
publishing principal. See [Durable Segment Format](/architecture/storage-format/).

### Batch

The unit a client publishes and the broker fans out. Subscriber queue capacity
counts batches, not records.

### Offset

A record's position in its shard's log. Offsets are per shard, start at 0 and
never name a different record for the life of the shard, including after
compaction. Every event delivered from a durable stream carries its offset, so a
jump in offsets is a dropped record. Offsets are not always consecutive: see
[skipped_before](#skipped_before).

### Tail

The offset the next append will get, which is one past the last record. The peer
protocol calls the same number `log_end`.

### Atomic commit

`commit(entity_key, ops)` writes one event and a set of keyed state updates as a
single record on one shard, so no reader sees one without the other. The
commit's offset becomes the version of every key it wrote, and the keys are read
back with `state_get`. See [Atomic Commits](/features/atomic-commits/).

### Region

A property of a stream that keeps its leader and replicas on brokers in that
region, or in regions it has a bridge to. It is fixed when the stream is created.
See [Data Residency](/use-cases/data-residency/).

### Region bridge

A one-way allowance, set with `FELIX_REGION_BRIDGES` as `source>dest`, that lets
a region's streams place copies in another region. A two-way bridge is two
pairs.

## The cluster

### Broker

The data-plane process. It serves clients over QUIC, talks to other brokers over
a second QUIC endpoint, leads some shards, holds copies of others, and forwards
requests for shards it does not lead. See [Components](/architecture/components/).

### Node

The control plane's word for a registered broker, as in `node_id`, `felixctl node`
and the node catalog. A node and a broker are the same thing.

### Control plane

The service that holds cluster metadata (tenants, namespaces, streams, caches,
nodes and shard assignments) and decides where shards live. Brokers read it at
startup and follow its changes. Clients never call it to publish or read, but a
replicated write can still wait on it: by default, the leader acknowledges only
after its [replica report](#replica-report) has reached the control plane and
it has re-checked its [lease](#leader-lease). With the `majority_ack`
[fleet feature](#fleet-feature) finalized, `Quorum` streams skip both waits;
`Leader` streams and caches keep them. See [System Design](/architecture/system-design/).

### Data plane

The brokers, serving clients and each other. Publishes and deliveries never go
through the control plane.

### Metadata Raft

A Raft group built into the control-plane instances, so the control plane can
store its metadata without an external database. Memory and Postgres are the
other storage backends. Records in streams are not replicated through Raft. See
[Metadata Raft](/architecture/metadata-raft/).

### Fleet

The set of brokers that are live or draining. A [fleet feature](#fleet-feature)
can only be turned on once every broker in the fleet supports it.

### Zone

A failure domain inside a region, such as an availability zone or a rack, set
on a broker with `FELIX_NODE_ZONE`. Placement spreads a shard's copies across
zones where it can.

### Heartbeat

A broker's periodic report to the control plane. It proves the broker is alive
and renews its [leases](#leader-lease). A broker that goes quiet for
`FELIX_NODE_EXPIRY_TIMEOUT_MS` is marked down.

### Shard

One log with one leader, plus its replicas. Streams and caches are split into
shards so their load can spread across brokers. Ordering, offsets and leadership
are all per shard. The Kafka listener calls a shard a partition.

### Routing key

The key a publisher gives so that records with the same key always land on the
same shard, which keeps them in order. A publish without a key goes to shard 0.
How a key maps to a shard is fixed per stream: `modulo` by default, or
`jump_hash` on streams created after the `jump_hash_routing` fleet feature is
finalized.

### Explicit shard

A publish, commit or state read that names its shard directly instead of
hashing a key. A shard number out of range is refused, never wrapped.

### Shard width

How many shards a stream has. A keyed publish can say which width it routed by,
and the broker refuses it with `stale_width` if the stream has since changed.

### Placement

The control plane's job of choosing each shard's leader and replicas. It uses
rendezvous hashing over the live brokers, spreads copies across zones, keeps
them inside the stream's region, and re-plans on a timer. The control-plane
instance holding the placement lease runs the timed passes, and every placement
write is conditional on the shard's generation, so instances that plan at the
same time cannot both apply a move. See
[Adding and Removing Brokers](/deployment/scaling/).

### Assignment

The control-plane record that names a shard's leader, replicas and
[generation](#generation). Brokers load a snapshot of all assignments and then
follow a feed of changes.

### Forward

What a broker does with a request for a shard it does not lead: it passes the
request to the leader over the peer link and relays the answer. A client can
also be told to go to the leader itself, with `not_leader`. Kafka requests are
never forwarded.

### ClusterClient

The client that knows several brokers, follows redirects, shard moves and
failovers, and reconnects under a `ReconnectPolicy`. Every language binding is
built on it. See [Choosing a Client](/clients/overview/).

### Seed

A broker address a client starts from. A seed given as a host name is resolved
again when none of the addresses the client knows answer.

## Replication and failover

### Leader

The one broker that accepts writes for a shard and ships them to its followers.
The client API and wire protocol call it the *owner* (`shard_owners`,
`ShardOwner`); the two words mean the same role.

### Follower

A broker that holds a copy of a shard and receives the leader's records. The
docs also say *replica*.

### Replica set

A shard's leader and followers together. A `Quorum` majority is counted over the
whole set, the leader included.

### Replication factor

How many brokers hold each shard, the leader included. The default is 1, which
means no followers. A `Quorum` stream needs at least 3 for a majority to survive
losing a broker.

### Log shipping

How records reach followers. The leader sends records at their offsets, and a
follower checks that each batch starts at its own tail. A gap or a mismatch is
answered explicitly, never papered over. Felix uses this instead of running Raft
per shard.

### Consistency level

What acknowledging a write means for a stream or cache. Under `Leader`, the
default, the leader acknowledges once it has written the record. Under
`Quorum`, it acknowledges once a majority of the replica set holds it. See
[Semantics](/architecture/semantics/) and the
[Leader vs Quorum](/demos/cluster-consistency/) demo.

### Quorum mark

The highest offset a majority of a shard's replica set holds. A `Quorum` publish
is acknowledged when the mark passes it, readers of a `Quorum` shard see nothing
above it, and retention never deletes past it. `felixctl inspect` shows it as
`committed`. The docs also call it the commit offset, committed mark or
high-water mark.

### Generation

A per-shard number the control plane raises every time the shard's leadership
changes. Replication and fence messages carry the generation they were sent
under, so anything from an older leader can be refused. The peer protocol and
storage code also call it an epoch.

### Generation-start record

A record a new leader writes before it serves. It takes an offset but is never
delivered, which is why subscribers can see a gap of one offset after a
failover. See [skipped_before](#skipped_before).

### Leader lease

A broker's time-limited right to act as a shard's leader at one generation,
renewed by its heartbeats to the control plane. When the lease lapses, the
broker stops serving the shard and refuses with `shard_unavailable` (reason
`fenced`) until it hears from the control plane again.

### Promotion

Making a follower the new leader at a higher generation, after the leader is
lost or during a planned move. Only a follower that can be shown to hold every
acknowledged record may be promoted. If none can, the shard stays unavailable
instead of opening empty.

### Failover

A promotion caused by losing the leader, as opposed to a planned move.

### Promotion fence

The step a newly promoted leader takes before serving: it sends its generation
to the replicas, and a majority must accept it. A replica that has accepted a
generation refuses anything from an older leader, so the deposed leader can no
longer get writes acknowledged.

### Ballot

The (generation, leader) pair a replica has promised to. It answers replication
and fence messages only from that leader, and refuses older ones as
`FencedEpoch`.

### Linearizable read

A get or counter get on a `Quorum` cache that returns only after a majority
holds the value and the leader has confirmed it is still the leader. By default
the confirmation is the leader's lease, which trusts the clocks. With the
`lease_free_reads` fleet feature, it is a round of fences answered by a
majority, which needs no clocks.

### Replica report

The leader's report to the control plane of which followers hold what. Failover
reads it to decide which follower can safely take over.

### Halted replica

A follower the leader has stopped shipping to, because its log diverged or it
needs a fresh copy. It counts toward no quorum and placement avoids it until it
is rebuilt.

### Divergence

Two copies of a shard holding different records at the same offset, which can
follow a failed `Quorum` write. Followers detect it by comparing bytes, and the
diverged copy is truncated back to the last agreed point or rebuilt.

### Rebalancing

Moving shards so brokers carry a fair share: onto new brokers, off overloaded
ones, and off brokers that are draining. Every move is a staged
[shard move](#shard-move).

### Shard move

A planned handoff of a live shard in three steps. The destination joins as a
follower and catches up, the old leader is fenced once the destination is close
enough, and the destination is promoted at a new generation. See
[Moving Shards by Hand](/deployment/moving-shards/).

### Drain

Marking a broker as leaving. It keeps serving while placement moves everything
it leads elsewhere. A broker sent SIGTERM drains itself. See
[Graceful Shutdown](/deployment/graceful-shutdown/).

### Restore

Replacing a follower whose broker has been gone longer than
`FELIX_SHARD_RESTORE_AFTER_MS` with a new copy on a live broker, so the shard
returns to its replication factor without an operator.

### Abandon

An operator command that discards a shard's log and places it fresh when no copy
can take over. It loses data and exists for the case where the alternative is a
shard that never comes back.

### Storage id

16 random bytes stored in the `storage-id` file of a broker's data directory,
naming that directory's contents. It changes when the directory is wiped,
restored from an older snapshot, or recovered after a crash that may have lost
acknowledged writes. Peers use it to tell a restarted broker from one whose disk
can no longer be trusted. See [Durable Storage](/architecture/durable-storage/).

### Node incarnation

A number the control plane raises each time a broker registers, so a restart can
be told apart from a broker that never went away.

### Stream incarnation

Which creation of a stream name this is. A stream that is deleted and recreated
under the same name starts empty at a higher incarnation, and brokers keep the
two logs apart. Caches have the same thing. Both depend on fleet features
(`stream_incarnations`, `cache_incarnations`).

## Storage

### Segment

One file of a shard's durable log, named by the offset of its first record.
There is one active segment taking appends; the rest are sealed, synced and
never rewritten. Retention deletes whole sealed segments. See
[Durable Segment Format](/architecture/storage-format/).

### Sparse index

The `.index` file beside a segment, with an entry every few kilobytes that tells
a read where to start scanning. It is never trusted: a missing or stale index is
rebuilt from the segment it describes.

### Torn tail

A partial record at the end of the newest segment, left by a crash in the middle
of a write. It was never acknowledged, so recovery cuts it off. Corruption
anywhere else in a log stops the broker from starting, because it might hide
acknowledged records.

### Fsync mode

How often a durable log forces data to the disk: `none`, `periodic` (the
default) or `on_commit`. The modes differ in how much acknowledged data a
machine crash can lose. Under `periodic`, a `Leader` acknowledgement means the
record is in the operating system's page cache. See
[Durable Storage](/architecture/durable-storage/).

### Group commit

Letting one fsync cover many concurrent appends. It is the main reason
`on_commit` can keep up with load.

### Ack on commit

Whether a broker acknowledges a publish after the write (the default for
durable streams) or as soon as it has accepted it (`FELIX_ACK_ON_COMMIT=0`). It
is separate from [AckMode](#ackmode), which decides whether the client wants an
acknowledgement at all.

### Retention

Deleting a durable stream's oldest sealed segments once the log exceeds a size
or age bound. It is off unless configured. A stream's own `retention` applies,
and a bound it leaves unset falls back to the broker's
`FELIX_DURABLE_RETENTION_BYTES` and `FELIX_DURABLE_RETENTION_SECONDS`. A read
below the oldest surviving offset is refused as too old.

### Compaction

A background pass over cache and counter logs that copies the live values
forward and drops superseded and expired records. Offsets are never renumbered.

### Replay ring

The in-memory buffer of a shard's most recent records that lets a subscriber
resume without reading disk. Older positions are read from the durable log, and
an ephemeral stream has nothing older.

### Tiered storage

Copying sealed segments to a second location before retention deletes them
locally. Today the only target is a directory (`FELIX_DURABLE_OFFLOAD_DIR`), and
nothing reads the copies back yet.

### Storage format version

The version of the on-disk record format. A broker refuses to start on a version
it does not know, and there is no downgrade. Format changes ship behind fleet
features so older brokers never meet records they cannot read. See
[Upgrades & Compatibility](/deployment/upgrades/).

## Publishing

### Publisher

A client handle that publishes to a stream over a long-lived QUIC stream.

### AckMode

What a publisher asks the broker to acknowledge: nothing (`None`, fire and
forget), each message (`PerMessage`) or each batch (`PerBatch`). An
unacknowledged publish can be shed when the broker is full.

### Pipelined publishes

Keeping several acknowledged publishes in flight on one stream instead of
waiting for each answer. Acknowledgements still come back in order.

### Idempotent producer

A publisher with a broker-issued producer id that numbers its batches per shard.
If an acknowledgement is lost and the batch is sent again, the leader recognizes
the sequence and answers without writing it twice. The sequences are stored in
the log and replicate, so this holds across a failover. It does not make delivery
to subscribers exactly-once. See [Semantics](/architecture/semantics/).

### Conditional publish

`publish_if` appends only when the shard's tail equals the offset the caller
expects, and otherwise returns the current tail without writing. It lets exactly
one of several writers win, which the docs call a single-writer fence.
`commit_if` does the same for atomic commits.

### Ingress

The inbound side of a broker: accepting publishes and deciding whether there is
room for them. Ingress is bounded by byte budgets and queue depth
(`pub_inflight_bytes`, `pub_ingress_wait`). "Authorized at ingress" means the
broker the client connected to checked the request, even if it then forwarded
it.

### Egress

The outbound side of a broker: delivering events to subscribers. Egress is
bounded per connection (`subscriber_egress_max_bytes`) and measured by
`felix_sub_egress_bytes`.

### Backpressure

The limits that keep a broker from taking on more than it can deliver: QUIC flow
control, the publish queue, subscriber queues and the ingress budget. A full
publish queue answers `overloaded`, which a client can retry.

### Tenant quota

A per-tenant cap on publish bytes and messages per second, enforced separately
on each broker and off unless configured. An acknowledged publish over the cap
is refused as `overloaded` with reason `tenant_quota`, and a Kafka produce is
throttled instead. Quotas limit publishing only. See
[Semantics](/architecture/semantics/).

## Reading

### Subscription

A client reading one stream shard as records arrive. Each subscription has its
own bounded queue on the broker and its own QUIC stream. See
[Pub/Sub Streaming](/features/pubsub/).

### Fanout

Delivering one publish to many subscribers. The broker encodes each batch once
and shares it between them. In benchmarks, *fanout* is the number of subscribers
per publish.

### Start position

Where a subscription or consumer group begins: `latest` (the default),
`earliest`, or an exact offset. `earliest` means the oldest record retention has
kept, which is not always offset 0.

### Subscriber queue

The bounded queue each subscription has on the broker, limited by entries and
bytes.

### Subscriber queue policy

What happens when a subscriber queue is full. `DropNew`, the default, drops the
new batch for that subscriber only. `Block` makes publishers wait. `DropOld` is
accepted but currently behaves as `DropNew`. See
[Pub/Sub Streaming](/features/pubsub/).

### Slow consumer

A subscriber that cannot keep up. By default it loses records on its own while
publishers and other subscribers carry on. On a durable stream the subscription
ends at its first drop with `subscription_lagged` and the offset to resume from.
See the [Slow-consumer Isolation](/demos/slow-consumer-isolation/) demo.

### Writer lane

A connection's outbound queue and the task that writes it to the network. Lanes
let one connection carry many subscriptions without one blocking the others.

### Replay

Reading a durable stream again from an offset that retention still holds.

### Range read

`stream_read`: one page of committed records from a start offset, without
registering a subscriber or waiting for new records.

### skipped_before

A count delivered with an event: how many offsets just below it hold no event,
such as a [generation-start record](#generation-start-record). Without it, a gap
like that would look like a drop.

### At-most-once and at-least-once

The two delivery guarantees Felix makes. A plain subscription to an ephemeral
stream is at-most-once: a record may be missed but is never repeated. Replay
from a durable stream and consumer groups are at-least-once: a record may arrive
twice but is not lost. Felix does not offer exactly-once delivery. See
[Semantics](/architecture/semantics/).

### Consumer group

A named position in a durable stream shared by several consumers, plus the
bookkeeping for records handed out and not yet settled. It is how a stream is
read as a [queue](#queue). See [Queues and Consumer Groups](/features/queues/).

### Claim

A record a poll has handed to one consumer. No other consumer gets it until it
is acknowledged, handed back, or the claim expires.

### Visibility timeout

How long a claim lasts before the record is owed to the group again. The default
is `FELIX_GROUP_VISIBILITY_TIMEOUT_MS`, and a poll can ask for a different one
up to a cap. A consumer still working can extend its claim.

### Ack and nack

Settling a claim (ack) or handing it back to be delivered again (nack),
optionally after a delay. Acknowledging a claim the broker no longer knows, for
example after a failover, is refused as `stale_claim` and the record is
delivered again.

### Attempts

How many times a record has been delivered to its group. After
`FELIX_GROUP_MAX_ATTEMPTS`, the record becomes a [dead letter](#dead-letter).

### Dead letter

A record a group has given up on. The record stays in the stream; the group only
remembers its offset. Dead letters can be listed, redriven (made deliverable
again) or discarded.

### Group cursor

The offset below which a consumer group has settled everything. It moves only
over a run of settled records, so one unsettled record holds it back.

### Cache version

The offset of the write that set a key. It only grows and is never reused, which
makes it the value `cache_put_if` compares against.

### Conditional cache write

`cache_put_if` and `cache_delete_if`, which apply only if the key is absent or
still at a given version. A failed condition is an answer, not an error: it
returns the current version. This is the basis for leases and locks; see
[Leases, Locks and Idempotency](/use-cases/leases-and-locks/).

### Fencing token

The version handed out when a lease is claimed with a conditional cache write.
A downstream system that only accepts increasing tokens can reject a holder whose
lease has passed to someone else.

### Cache watch

A stream of every change to a key or key prefix, in the order the shard wrote
them, resumable from an offset. A retained watch first sends each key's current
value, then changes. Watch offsets have gaps because other keys share the shard;
only a `lagged` event means a change was missed.

### TTL

An expiry time on a cache entry. It is stored as an absolute time, so it
survives a restart, and the leader writes a delete when the entry expires.

## Protocol and transport

### QUIC

The UDP-based transport Felix uses between clients and brokers and between
brokers. It is always encrypted with TLS 1.3 and multiplexes many independent
streams over one connection, so a stalled stream does not hold up the others.
See [QUIC Transport](/features/quic-transport/).

### Control stream

The long-lived two-way QUIC stream a client opens first. It authenticates once
and then carries requests and their answers.

### Event stream

A one-way QUIC stream the broker opens for each subscription to deliver its
events.

### Frame

The unit on the wire: a 12-byte header (magic number, version, flags, length)
followed by a payload. See [Wire Protocol](/architecture/wire-protocol/).

### Frame flags

Bits in the frame header that select the payload's layout. A peer rejects a
frame with a bit it does not know, because guessing at the layout would misread
the body.

### Feature bits

Bits a broker advertises when a client authenticates, one per request type it
supports. A client only sends requests the broker advertised.

### Capability negotiation

How Felix adds wire features without version bumps. The client offers the flags
it understands (`client_flags`), the broker answers with the ones it will use
(`server_flags`), and a peer that predates negotiation is treated as speaking the
original three flags (`ORIGINAL_V1_FLAGS`). See
[Wire Protocol](/architecture/wire-protocol/).

### Internal protocol

The protocol brokers use with each other for forwarding, replication and fences.
It has its own version and its own frame magic.

### Retry class

The part of every error that tells a client what to do next: `retry`,
`retry_after`, `redirect`, `outcome_unknown` or `fatal`. `outcome_unknown` means
the write may or may not have happened, which an idempotent producer can
resolve by sending it again.

## Operations

### Fleet feature

Cluster-wide behavior that stays off until it is [finalized](#finalize).
Brokers report which fleet features they support when they register. On an
upgraded cluster an operator finalizes each feature, which is refused unless
every serving broker supports it. A new cluster finalizes them for itself. Examples are `majority_ack`, `lease_free_reads` and
`stream_incarnations`. See [Upgrades & Compatibility](/deployment/upgrades/).

### Finalize

The one-way step that turns a fleet feature on. Afterwards, a broker that does
not support the feature is refused when it registers. A new cluster finalizes
every feature its brokers report at the first registration, except the opt-in
`publisher_principal`.

### Readiness and liveness

The two health questions a broker answers. Liveness asks whether the process is
running. Readiness asks whether it should get traffic, and it turns false while
the broker drains, before the listener closes. See
[Graceful Shutdown](/deployment/graceful-shutdown/).

### Under-replicated

A shard with fewer copies on serving brokers than its replication factor, counted
by `felix_shards_under_replicated`. See [Observability](/features/observability/).

### Backup point

A consistent cut across shards, recording each leader's committed offset at one
moment. See [Backup and Restore](/deployment/backup-and-restore/).

### Bootstrap mode

A one-time setup mode for a new cluster that seeds the first tenant, token
issuers and permissions before any admin token exists. See
[Security](/features/security/).

### felixctl

The command-line tool for publishing, subscribing, caches, groups, access
control and cluster administration. A *context* is a saved connection profile.
See [felixctl](/getting-started/felixctl/).

### felix-gateway

A WebSocket relay for browsers, which cannot speak QUIC. A browser joins one
*scope*, a set of streams, caches and counters, with a token narrowed to it. See
[Browsers](/clients/browsers/).

### Kafka listener

An optional second front door on a broker that speaks the Kafka protocol over
durable streams. Topics are named `<namespace>.<stream>` and partitions are
shards. See [Kafka Compatibility](/features/kafka/).

## Security

### Principal

An authenticated identity, taken from a token's subject. Principals are what
permissions are granted to, and a record can carry the principal that published
it.

### Felix token

A signed token that names a tenant and a set of permissions. Brokers verify it
themselves, so checking a request never needs a call to the control plane. A
client usually gets one by exchanging a token from its identity provider at the
control plane. See [Security](/features/security/).

### Narrowing

A token request can only ask for a subset of the permissions its holder has,
never more.

### Cluster scope

`cluster:*`, the resource that operator permissions such as `node.manage` apply
to. It contains no tenant data, and tenant permissions never reach it.

### Revocation

Withdrawing a principal's access. Brokers learn of it from a feed, end the
principal's open streams, and refuse tokens issued before it. A broker that
cannot reach the feed keeps accepting a revoked token until the token expires.
See [Security](/features/security/).

## Testing and performance

### Conformance suite

A shared catalog of scenarios and test vectors that every client and the broker
must pass, so the clients in different languages behave the same. See
[Choosing a Client](/clients/overview/).

### Client fixture

A test cluster built for conformance runs, with a control endpoint that can cut
links between the client and brokers and mint short-lived tokens.

### Test vectors

Files that pin the exact bytes of wire frames, so every implementation encodes
and decodes them the same way.

### History checker

A test that runs clients against a real three-broker cluster while injecting
faults, records every operation, and checks the history against Felix's
guarantees. See [How Felix Is Tested](/architecture/testing/).

### Nemesis

The schedule of faults a history test injects: crashes, partitions, power loss
and control-plane failures.

### p50, p99, p999

Latency percentiles: the time within which 50%, 99% and 99.9% of messages are
delivered. Felix reports them per payload size and fanout. See
[Benchmarks](/features/benchmarks/).

### Throughput and delivered throughput

*Throughput* is what publishers sent. *Delivered throughput* counts every
delivery to every subscriber, so it is throughput times the fanout. A batched
run measures throughput and says nothing about the latency of a single request.

## Words with more than one meaning

**Fence** names four things. A [promotion fence](#promotion-fence) is a new
leader's generation accepted by a majority. The fence step of a
[shard move](#shard-move) stops the old leader. A single-writer fence is a
[conditional publish](#conditional-publish). A [fencing token](#fencing-token)
is the version from a lease claim.

**Lease** names three things. A [leader lease](#leader-lease) is a broker's
right to lead a shard. The placement lease is held by the control-plane
instance that runs the timed placement passes. A lease in
[Leases, Locks and Idempotency](/use-cases/leases-and-locks/) is an application
pattern built on conditional cache writes.

**Incarnation** names two things. A [node incarnation](#node-incarnation) counts
a broker's registrations. A [stream incarnation](#stream-incarnation) counts how
many times a name has been created. Neither is a [generation](#generation) or a
[storage id](#storage-id).

**Leader and owner** are the same role. The control plane, replication and
metrics say leader, and the client API and wire protocol say owner. In
[Metadata Raft](/architecture/metadata-raft/), the leader is the Raft leader of
the control plane, which is a different role.

**Generation and epoch** are the same number. The docs say generation, and parts
of the peer protocol and storage code say epoch. The Kafka producer epoch is
unrelated.

**Commit** appears in several unrelated terms: the
[quorum mark](#quorum-mark) (commit offset), an [atomic commit](#atomic-commit),
[ack on commit](#ack-on-commit), the `on_commit` [fsync mode](#fsync-mode),
[group commit](#group-commit), and a consumer group's committed position.

**Ack** can mean a publish acknowledgement ([AckMode](#ackmode)), a `Quorum`
acknowledgement, settling a consumer group claim ([ack and nack](#ack-and-nack)),
or Kafka's `acks` setting.

**At-most-once and at-least-once** describe two things. On a stream they say how
a reader receives records. In the C client, `FELIX_DELIVERY_AT_MOST_ONCE` and
`FELIX_DELIVERY_AT_LEAST_ONCE` say whether a publish is sent again after a lost
connection.

**Cursor** can mean a subscription's position, a [group cursor](#group-cursor),
or replay from the [replay ring](#replay-ring).

**Lag** can mean how far a follower or a move destination is behind the leader,
a subscription ended by a drop (`subscription_lagged`), a cache watch that
missed a change, or how far a consumer group is behind the tail.

**Snapshot** can mean the control plane's assignment snapshot, a Raft snapshot,
or a cache's saved key index.

**Shard** almost always means a data shard. The thread-per-core mode in
[Concurrency Internals](/development/internals-concurrency/) also uses the word
for a CPU core's runtime.

**Partition** is a Kafka partition, which is a shard, or a network partition.

**Queue** is a stream read through a consumer group, and also any bounded buffer
in a broker, such as the publish queue or a subscriber queue.

**Durable** can describe a stream that is stored on disk, or an acknowledgement.
How durable an acknowledged write is depends on the [fsync mode](#fsync-mode).
