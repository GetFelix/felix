---
title: "Coming from Kafka"
description: "Point Kafka producers and self-assigning consumers at Felix by changing a bootstrap address, then move fanout, queues and caches onto Felix clients in steps."
---

A team has a few dozen services producing to Kafka and a handful of consumers
reading from it. Some consumers feed dashboards or caches and need every service
instance to see every event. Some feed warehouses through Kafka Connect. The
question is which of those can point at a Felix broker by changing a bootstrap
address, and which have to change.

Felix runs a Kafka listener. Kafka producers write to durable Felix streams and
consumers that assign their own partitions read them. Stock clients work
unchanged (kcat is the client tested here), and the same records are also reachable from Felix subscriptions, consumer
groups and caches.

## Try it

Start the stack from the
[Quickstart](/getting-started/quickstart/#try-it-in-one-command), but first turn
the Kafka listener on. Put this in `compose.override.yaml` next to `compose.yaml`;
Compose reads it automatically:

```yaml
services:
  felix-broker:
    environment:
      FELIX_KAFKA_LISTEN: 0.0.0.0:9092
      FELIX_KAFKA_ADVERTISE_ADDR: 127.0.0.1:9092
      FELIX_KAFKA_TLS: "false"
      FELIX_KAFKA_ANONYMOUS_TENANT: dev
    ports:
      - "127.0.0.1:9092:9092"
```

`FELIX_KAFKA_TLS=false` and `FELIX_KAFKA_ANONYMOUS_TENANT` are development
switches: connections need no authentication and read and write every stream of
the `dev` tenant (see [Kafka compatibility](/features/kafka/#anonymous-access-for-development)
for what that implies). Then create a durable stream and use kcat from the host:

```bash
docker compose up -d
docker compose run --rm felixctl stream create created --shards 4
echo '{"id":42}' | kcat -b 127.0.0.1:9092 -P -t default.created -p 0
kcat -b 127.0.0.1:9092 -C -t default.created -o beginning -e -q -f '%p:%o:%s\n'
```

The stream lives in the `default` namespace, so the topic is `default.created`.
The stream is durable by default. In production use SASL_SSL with a Felix token as
the password and the tenant as the username.
[Kafka compatibility](/features/kafka/) covers authentication, client settings
and what else is supported.

## Why Felix fits

Moving a producer behind the Kafka listener changes a bootstrap address. The
records land in a durable Felix stream, so the same data is also reachable with
Felix's own features, and the Kafka producers keep working unchanged.

A Felix subscriber gets its own bounded queue, so a slow instance reaches its
own bound and does not hold the others back. Each event carries its offset, so
a drop shows as a jump, and one subscription per instance gives every instance
every event.

On a log-backed cache, a retained watch delivers each key's current value and
then every change after it, resuming by offset with no gap. That replaces the
consumer that rebuilt a table from a compacted topic.

Consumer groups, caches and counters run on the same log and offsets as the
stream your Kafka producers write, with acknowledgement, redelivery and dead
letters on the group. The queue and the cache do not need a second system
that must agree with the log.

Felix clients reach the broker over QUIC with tenant-scoped tokens that can be
narrowed by action and resource, and revoking a principal ends its open streams
and Kafka connections within about one control-plane sync interval. Both
kinds of client use the same tokens, so one place sets who can read what.

## How it maps

With `FELIX_KAFKA_LISTEN` set, a durable stream appears as a topic named
`<namespace>.<stream>`. The tenant is not in the name; it comes from the SASL
username. Partition N is shard N, and offsets are the log offsets a Felix client
sees on `Event.offset`. In-memory streams are not visible.

Produce accepts gzip, snappy, lz4 and zstd, and `acks` of 0, 1 and all. `acks=all`
waits for the stream's own consistency: a majority on a `Quorum` stream, the
leader alone on a `Leader` stream. Use a `Quorum` stream when a write must
survive losing the leader. Idempotent producers work, including across a
failover, because their sequences are stored in the log and replicated.

Three things are not carried over on the way in. The record key picks the
partition and is then dropped, headers are dropped, and the producer's timestamp
is replaced by the broker's append time. A consumer that reads meaning from the
key needs it in the value instead. Topics cannot be created from Kafka tools;
create the stream through the control plane or felixctl.

## Example

The listener speaks the Kafka wire protocol, so stock producers and consumers
connect to it in the usual way. A consumer must assign its own partitions and
store its own offsets, and it must not set `group.id`. [Kafka
compatibility](/features/kafka/#feeding-an-existing-kafka-consuming-program) has
client settings and caveats for kcat, librdkafka and Java.

Resuming a Felix client from a stored offset looks like this. The offset is the
same one a Kafka consumer sees:

```rust
use felix_client::StartPosition;

let mut events = client
    .subscribe_from("dev", "default", "created", Some(StartPosition::Offset(saved + 1)))
    .await?;
while let Some(event) = events.next_event().await? {
    handle(&event.payload);
    saved = event.offset.unwrap_or(saved);
}
```

## A migration path

Start with producers. Point one producer at Felix, check its records with
`kcat -C` or a Felix subscriber, then move the next. Producers can move one at
a time, because Felix's own clients and Kafka producers write to the same stream
and readers cannot tell them apart except by the missing keys.

Then separate ingest from fanout. Keep the Kafka clients where they write,
and move the consumers whose job is to deliver events to many instances onto
Felix subscriptions, with one subscription per instance, an offset checkpoint
for resume, and a bounded per-subscriber queue so one slow instance does not hold
the others back. Watches on a cache cover the case where consumers were building
a table from a compacted topic to hold current values. Consumers that need work
split between workers move to a Felix consumer group through a Felix client,
which gives acknowledgement, redelivery and dead letters.

Stages that need Connect, Streams or ksqlDB stay on Kafka until you replace
them. Mirroring a Kafka topic into Felix with MirrorMaker is not possible, since
it uses groups, so a copy is a producer you write.

## Compared with Kafka

Felix is built for fanout to many live subscribers with isolation between
them, and for work queues, caches and counters next to a durable log: they share
one log and one set of offsets, are reached over QUIC, and Kafka clients can
still read the log. Consumer groups with rebalancing, transactions, compaction
and the connector and stream-processing ecosystem are Kafka's, so a stage that
needs any of those stays on Kafka. Latency and
throughput figures, with their conditions, are on
[Benchmarks](/features/benchmarks/). Do not compare them with a Kafka number
from another setup.

## Limits you can plan around

**No consumer groups or transactions.** A consumer that sets `group.id` and calls
`subscribe()` is refused, and so is a producer that sets `transactional.id`. That
means Kafka Connect, Kafka Streams, ksqlDB, Debezium and MirrorMaker cannot point at
Felix. Keep those stages on Kafka, or replace them: a Felix consumer group through
a Felix client covers work split between workers. Mirroring a topic into Felix is a
producer you write.

**Keys, headers and producer timestamps are not kept.** The record key picks the
partition and is then dropped, headers are dropped, and the timestamp is the
broker's append time. Put anything a consumer needs into the value.

**Streams, not topics.** Only durable streams are visible. Create the stream
through the control plane or `felixctl`, because topics cannot be created from
Kafka tools. There are no admin APIs, no log compaction or Kafka retention
settings, and a tombstone is stored as an empty payload and deletes nothing.

**SASL/PLAIN only.** Use SASL_SSL with a Felix token as the password.

**Produces on one connection are answered one at a time.** Spread load across
partitions. See the Limits section of [Kafka compatibility](/features/kafka/#limits)
and the protocol reference
[`docs/kafka-compatibility.md`](https://github.com/GetFelix/felix/blob/main/docs/kafka-compatibility.md)
for the full list.

## Related

- [Kafka compatibility](/features/kafka/)
- [Publish/Subscribe](/features/pubsub/)
- [Queues](/features/queues/)
- [Audit logs and replay](/use-cases/event-log-and-replay/)
