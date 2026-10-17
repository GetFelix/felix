---
title: "Coming from Kafka"
description: "Which Kafka producers and consumers can point at Felix today, which cannot, and how to move a system over in steps."
---

A team has a few dozen services producing to Kafka and a handful of consumers
reading from it. Some consumers feed dashboards or caches and need every service
instance to see every event. Some feed warehouses through Kafka Connect. The
question is which of those can point at a Felix broker by changing a bootstrap
address, and which have to change.

## What works and what does not

Felix serves Kafka producers, and Kafka consumers that
assign their own partitions and keep their own offsets. It does not serve
consumer groups or transactions. A consumer that sets `group.id` and calls
`subscribe()` is refused, and so is a producer that sets `transactional.id`.

That rules out anything built on groups today: Kafka Connect, Kafka Streams,
ksqlDB, Debezium and MirrorMaker. If your pipeline depends on one of them for a
stage, that stage stays on Kafka. The protocol reference is
[`docs/kafka-compatibility.md`](https://github.com/GetFelix/felix/blob/main/docs/kafka-compatibility.md)
and the user guide is [Kafka compatibility](/features/kafka/).

## How it maps

With `FELIX_KAFKA_LISTEN` set, a durable stream appears as a topic named
`<namespace>.<stream>`. The tenant is not in the name; it comes from the SASL
username. Partition N is shard N, and offsets are the log offsets a Felix client
sees on `Event.offset`, so a stored offset means the same thing on both sides.
In-memory streams are not visible.

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

## Example

Start a development broker with the Kafka listener and no authentication (see
[Kafka compatibility](/features/kafka/#anonymous-access-for-development) for
what that implies), create a durable stream, and use kcat:

```bash
export FELIX_DURABLE_STORAGE_DIR=/var/lib/felix
export FELIX_KAFKA_LISTEN=127.0.0.1:9092
export FELIX_KAFKA_TLS=false
export FELIX_KAFKA_ANONYMOUS_TENANT=t1
felix-broker

felixctl -n orders stream create created --shards 4
echo '{"id":42}' | kcat -b 127.0.0.1:9092 -P -t orders.created -p 0
kcat -b 127.0.0.1:9092 -C -t orders.created -o beginning -e -q -f '%p:%o:%s\n'
```

`-n orders` puts the stream in the `orders` namespace, so the topic is
`orders.created`. The stream is durable by default. In production use
SASL_SSL with a Felix token as the password and the tenant as the username.

A consumer that assigns its own partitions and stores its own offsets is the
only shape Felix serves. kcat 1.7.1 is the tested client. This confluent-kafka
configuration is an untested sketch, and `confluent_kafka.Consumer` normally
wants a `group.id`, which librdkafka answers by asking for a group coordinator
that Felix refuses. [Kafka compatibility](/features/kafka/#feeding-an-existing-kafka-consuming-program)
has the caveats:

```python
from confluent_kafka import Consumer, TopicPartition

consumer = Consumer({
    "bootstrap.servers": "broker-1.internal:9092",
    "security.protocol": "SASL_SSL",
    "ssl.ca.location": "felix-ca.pem",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": "t1",
    "sasl.password": felix_token,
    "enable.auto.commit": False,
})
start = load_offsets()
consumer.assign([TopicPartition("orders.created", p, o) for p, o in start.items()])
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

```rust
use felix_client::StartPosition;

let mut events = client
    .subscribe_from("t1", "orders", "created", Some(StartPosition::Offset(saved + 1)))
    .await?;
while let Some(event) = events.next_event().await? {
    handle(&event.payload);
    saved = event.offset.unwrap_or(saved);
}
```

Stages that need Connect, Streams or ksqlDB stay on Kafka until you replace
them. Mirroring a Kafka topic into Felix with MirrorMaker is not possible, since
it uses groups, so a copy is a producer you write.

## Compared with Kafka

Kafka is the more mature system. It has consumer groups with rebalancing,
transactions, compaction, a deep connector and stream-processing ecosystem, and
years of operation at large scale. Pick Kafka, or keep a stage on it, when you
need any of those. Felix is the better fit when the workload is fanout to many
live subscribers with isolation between them, or work queues, caches and
counters next to a durable log: they share one log and one set of offsets, are
reached over QUIC, and Kafka clients can still read the log. Latency and
throughput figures, with their conditions, are on
[Benchmarks](/features/benchmarks/). Do not compare them with a Kafka number
from another setup.

## Try it

The one-command stack does not turn on the Kafka listener. The example above
starts a development broker with it and gives the `kcat` produce and consume
commands, and [Kafka compatibility](/features/kafka/) covers authentication
and what else is supported.

## Where it stops

No consumer groups, no transactions, no admin APIs, no log compaction or Kafka
retention settings, and only SASL/PLAIN. Keys, headers and producer timestamps
are not kept. A tombstone is stored as an empty payload and deletes nothing.
Only durable streams are visible. A connection's produces are answered one at a
time, so spread load across partitions. See the Limits section of
[Kafka compatibility](/features/kafka/#limits) for the full list.

## Related

- [Kafka compatibility](/features/kafka/)
- [Publish/Subscribe](/features/pubsub/)
- [Queues](/features/queues/)
- [Audit logs and replay](/use-cases/event-log-and-replay/)
