---
title: "Coming from NATS"
description: "How NATS subjects, queue groups, JetStream, KV and accounts map onto Felix streams, consumer groups, the cache and tenants, and the NATS features Felix does not have."
---

A team runs NATS for service messaging. Core NATS carries live updates and
fire-and-forget events, JetStream holds the events that must not be lost, and a
KV bucket holds configuration. The question is what each of those becomes on
Felix, and which parts of the NATS model have no counterpart.

NATS is a good system, small and fast, and a lot of what it does is outside
Felix's scope. Read the last two sections before planning a move. For some
workloads the right answer is to keep NATS.

## How it maps

| NATS | Felix |
|---|---|
| Account | Tenant. Namespaces split a tenant further |
| Subject | A stream, addressed as `tenant/namespace/stream`, plus an optional routing key |
| Core pub/sub | An ephemeral stream (`--durable false`), in memory |
| Queue group on core NATS | A consumer group on a stream. It adds acknowledgements, so it is closer to JetStream than to core |
| JetStream stream | A durable stream, written to a CRC-checked log and replayable by offset |
| JetStream consumer | A subscription that resumes from an offset, or a consumer group |
| `AckWait`, nak, term, in-progress | Visibility timeout, `group_nack` with an optional delay, giving up on a record, extending a claim |
| `MaxDeliver` | `FELIX_GROUP_MAX_ATTEMPTS`, with a dead-letter list when it is reached |
| KV bucket | A cache, with versions, conditional writes, TTLs and watches |
| R3 stream | `replication_factor` 3 with `Quorum` consistency |
| WebSocket listener | [felix-gateway](/clients/browsers/) |

## Subjects become streams

A NATS subject is a dotted string, and a subscriber can use `*` and `>` to match
many of them. Felix has no subject tree and no wildcard subscriptions. A stream
has a name, and a subscription names exactly one stream.

What to do instead depends on why you used the wildcard:

- **Hierarchy for organisation**, such as `orders.retail.placed` and
  `orders.wholesale.placed`. Use a namespace for the coarse part (`orders`) and
  one stream per kind of event (`retail-placed`). A subscriber that wants both
  opens two subscriptions.
- **Fanning a family of subjects into one consumer**, such as `orders.>`. Put
  the events in one stream and carry the kind in the payload. The consumer reads
  one stream and filters in code.
- **Splitting by key**, such as `orders.<customer-id>`. Publish with a routing
  key on a stream with several shards. A key always lands on the same shard, so
  records for one customer stay in order. Ordering holds per key, not across the
  stream. See [Pub/Sub Streaming](/features/pubsub/#ordering).
- **Per-entity channels at very large scale**, such as a subject per device or
  per user. A stream per entity is a heavier object than a subject. Keep these
  as routing keys on a shared stream, or keep NATS for that traffic.

Streams are created ahead of time, through the control plane or `felixctl`.
Nothing is created by publishing to a new name.

```bash
felixctl -n orders stream create placed --shards 4 --replication 3 --consistency quorum
felixctl -n orders stream create ticks --durable false --delivery at-most-once
```

## Core pub/sub becomes an ephemeral stream

An ephemeral stream lives in memory and is lost on restart, the same bargain as
core NATS. Each subscriber gets its own bounded queue with an overflow policy, so
a slow subscriber drops records and never holds up the publisher or the other
subscribers. NATS handles a slow subscriber differently: the server disconnects
it as a slow consumer. A Felix subscriber stays connected and misses records,
and on a durable stream the offsets on delivered events show exactly what was
missed. See [Isolation and backpressure](/features/pubsub/#isolation-and-backpressure).

## Queue groups and JetStream consumers become consumer groups

A core NATS queue group picks a random member for each message. If the member
dies before it finishes, the message is gone. A Felix consumer group is closer
to a JetStream pull consumer shared by several workers:

- A consumer polls, gets a record on a visibility timeout, and acknowledges it.
- `nack` hands it back at once or after a delay. A claim that lapses is redelivered.
- Each delivery carries `attempts`. After `FELIX_GROUP_MAX_ATTEMPTS` (5 by
  default) the record is put on a dead-letter list and the group moves on. The
  record stays in the log. An operator can redrive it or discard it.
- The group's position and dead-letter list replicate with the shard, so a
  leader failover resumes where the group was.

NATS's `MaxDeliver` defaults to unlimited, and when it is reached the server
stops delivering and raises an advisory. Felix keeps the dead-letter list for
you to inspect and redrive. A NATS consumer can also be told to give up on a
message early with a term, and Felix has the matching `dead-letters add`.

Two differences matter for the design:

- **A group is bound to one shard.** There is no coordinator that spreads shards
  across a group's workers. On a stream with four shards you run at least one
  worker per shard. A NATS consumer, by contrast, hands any message to any
  worker.
- **Order is not preserved across workers**, in either system. Redeliveries
  reorder. If a record's handler is not idempotent, make it so. Neither system
  offers exactly-once delivery to a consumer.

See [Queues and Consumer Groups](/features/queues/) and the
[work queue scenario](/use-cases/work-queues/).

## JetStream streams become durable streams

A JetStream stream captures messages from the subjects it is bound to, with
limits-based, interest-based or work-queue retention. A Felix durable stream is
a log you publish to by name. Subscribers read from the start, from the tail or
from a stored offset, and each event carries its offset. A consumer's position
in JetStream lives on the server; in Felix a plain subscription keeps no
server-side position, so the client stores the offset it last saw. A consumer
group is the option when you want the server to hold the position.

Retention is by size or age, and off until you set it. Replication is per
stream: `replication_factor` sets the number of copies, and the consistency
mode sets what an acknowledgement means.

- `Leader` acknowledges once the leader has the record. It is fast, and a
  record the leader acknowledged can be lost if the leader fails before its
  followers have it.
- `Quorum` acknowledges once a majority hold the record. This is the closest to
  an R3 JetStream stream, which commits by quorum.

Felix does not use per-shard Raft for the data path. It uses a leader per shard
with log shipping and, for `Quorum`, majority acknowledgement. The
[Leader vs Quorum demo](/demos/cluster-consistency/) shows the difference under
a partition, and [Semantics](/architecture/semantics/) states the guarantees.

## KV becomes the cache

A Felix cache holds keys under a tenant, namespace and cache name.

- A put can carry a TTL. A log-backed cache survives a restart.
- `cache_get_versioned` returns a version, and `cache_put_if` and
  `cache_delete_if` write only if the key is absent or still at that version.
  This is the same idea as KV's revision-based create and update, and it is what
  leases and locks are built from. See [Leases, locks and idempotency](/use-cases/leases-and-locks/).
- A watch on a key or a key prefix delivers each change with a resumable
  offset, and a retained watch delivers the current values first and then the
  changes. If a watcher falls behind it is told and can resume without a gap.
- Counters are a separate primitive with a durable increment.

Differences from a KV bucket: there is no per-key history, so you cannot read
earlier revisions of a key. A conditional write checks one key, and there are no
multi-key operations. A prefix watch reads one shard, and the cluster client
merges the shards for you. Keys in a cache are not subjects, so you cannot watch
`config.>`. See [Distributed Cache](/features/cache/).

## Accounts become tenants

A NATS account is an isolated subject space with its own users. A Felix tenant
is the top of a hierarchy, `tenant / namespace / stream or cache`, and the scope
is part of every operation. Tokens are tenant-scoped, and RBAC grants name
objects such as `stream:acme/orders/placed` or `cache:acme/prod/sessions/user-*`,
with actions like `stream.subscribe` or `group.consume`. Grants can be narrowed to
one stream, one consumer group or a key prefix.

Cross-account import and export has no direct counterpart. Sharing between
tenants is not a built-in feature. Tenants authenticate through the control
plane, which can exchange an identity provider's OIDC token for a Felix token.
Per-tenant publish rate limits exist, set in each broker's environment. See
[Security](/features/security/) and [Multi-tenant SaaS](/use-cases/multi-tenant-saas/).

## Browsers

NATS can serve the NATS protocol over WebSocket. Felix clients speak QUIC, which
browsers cannot, so browsers go through [felix-gateway](/clients/browsers/): a
WebSocket relay that signs the user in, holds a token narrowed to one scope, and
forwards JSON messages. It is a separate process you run, and it is pre-1.0.

## What Felix does not have

- **Request-reply.** There is no built-in request pattern, no reply subject, and
  no scatter-gather. You can build one from two streams and a correlation id in
  the payload, but the latency and the setup cost are not those of a NATS
  request. Keep NATS for RPC-style traffic.
- **Message headers.** A Felix record is a payload and, on durable streams, an
  offset and append time. Metadata goes in the payload.
- **Subject wildcards and subject mapping.** Described above. There is no
  server-side rewriting or weighted routing of subjects.
- **Object store.** Felix has no large-object store. Keep blobs elsewhere and
  publish a reference.
- **MQTT.** Felix has no MQTT listener. It has a Kafka listener for producers
  and self-assigning consumers; see [Coming from Kafka](/use-cases/kafka-migration/).
- **Leaf nodes and superclusters.** Felix brokers form one cluster. Pinning a
  stream's replicas to a region is possible, and gives data residency, but
  there is no leaf topology for edge sites and no gateway between independent
  clusters. See [Data residency](/use-cases/data-residency/).
- **Mirrors and sources.** There is no stream mirroring between clusters.
- **Follower reads.** Every read goes to the shard's leader.
- **A single-binary deployment.** A Felix deployment has brokers and
  a control plane, backed by Postgres or its Raft backend. NATS is simpler to
  run, and is lighter on a small machine.

## Compared with NATS

NATS has a mature server, a very wide set of client libraries, a flexible
subject model, request-reply, and a topology that reaches edge devices. It is
the better fit for service-to-service messaging, RPC and edge connectivity.
JetStream is a capable log, and per-key TTL and watches make KV a good fit for
configuration.

Felix is aimed at a different spread of work: very high fanout to many live
subscribers with isolation between them, and a durable log, work queues, caches
and counters that share one log and one set of offsets, so you do not run a
second system for state. Felix is pre-1.0, while NATS has years in production. Latency and throughput figures, with their conditions, are on
[Benchmarks](/features/benchmarks/). Do not compare them with a NATS number from
another setup.

## A migration path

Move the workloads that gain from the log first, and leave RPC on NATS.

1. Pick a stream of live updates with many subscribers. Create an ephemeral
   stream, publish to it from the same code path that publishes to the NATS
   subject, and move subscribers over one at a time.
2. Move a JetStream stream that holds events you replay. Create a durable
   stream with `Quorum`, dual-publish, and start the consumers from the offset
   that matches where they were. Switch consumers, then stop publishing to NATS.
3. Move work queues to consumer groups, one worker per shard, and check the
   handlers tolerate a repeat.
4. Move configuration from KV into a cache and replace the watchers with cache
   watches. Use conditional writes where you used revisions.
5. Leave request-reply, headers-dependent flows and edge or MQTT connectivity on
   NATS.

## Try it

The [Quickstart](/getting-started/quickstart/) starts a broker and a control
plane, and [felixctl](/getting-started/felixctl/) creates streams, publishes,
subscribes and works a consumer group.

## Related

- [Publish/Subscribe](/features/pubsub/)
- [Queues and Consumer Groups](/features/queues/)
- [Distributed Cache](/features/cache/)
- [Coming from Kafka](/use-cases/kafka-migration/)
