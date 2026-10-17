---
title: "Audit Logs and Replay"
description: "Use a durable stream as an append-only audit and event log you can replay from an offset or a point in time."
---

A payments team needs to answer what happened to a customer's account at 14:02
last Tuesday, who changed it, and in what order. A search team needs to rebuild
a projection after a bad deploy. Both need a record of events that is written
once, kept for a defined time, and readable from any point. A durable Felix
stream is that log.

## How this is usually built

Kafka is the usual choice for an event log. It is mature, has configurable
retention and log compaction, tiered storage, and a very large ecosystem of
connectors and processing frameworks. If you already run it and use its tooling,
that is a good reason to stay.

JetStream gives NATS a persistent stream with replay by sequence or time and
per-consumer acknowledgement. A database audit table, written by triggers or by
the application in the same transaction as the change, is the other common
pattern, and its strength is that the audit row commits with the change. Its
weakness is that it shares the database's load, retention and access controls
with everything else.

## Why Felix fits

A durable stream keeps the audit log and the live feed in one place. The same
records you replay from an offset are the ones subscribers received live, with
the same offsets, so a consumer that follows the stream and one that replays it
use one position and cannot disagree about what happened.

Gaps are visible. Every event on a durable stream carries its log offset, and a
jump between consecutive offsets, less any `skipped_before`, tells a reader its
queue dropped events. A reader never has to guess whether it missed a record.

Replay by time is built in. Records carry the broker's append time, and
`offset_for_time` returns the first offset at or after a time on a shard, so
"what happened at 14:02" is a lookup followed by a range read, without an
index you maintain.

Who wrote a record can be stored with it, and an idempotent producer makes a
retried write appear once. Together they answer "who changed it" and keep a
retry from doubling an entry. Both need the options described below, and the
idempotent producer is Rust only.

Integrity is checked in the storage layer. Each record has a CRC, a torn tail
is repaired, and interior corruption stops the broker from starting instead of
dropping acknowledged records. With `Quorum` and `on_commit`, an acknowledged
entry is on a majority of replicas and flushed.

## How it works on Felix

A stream created with `durable: true` writes every publish to a segmented log
on the broker before the publish is fanned out. Each record
carries a CRC, so corruption is detected on read and at recovery. A torn tail
after a crash is repaired, and corruption in the interior makes the broker
refuse to start rather than drop acknowledged records. Records are never
rewritten.

When a publish is acknowledged depends on the stream and on `ack_on_commit`. A
`Quorum` stream, or any stream when the broker (`FELIX_ACK_ON_COMMIT`) or the
client (`ack_on_commit`) sets it, acknowledges after the write reaches disk
under the fsync policy. A `Leader` stream without `ack_on_commit` acknowledges
when the publish is queued.

How hard each write is pushed to disk is the fsync policy. Under `on_commit` a
write is flushed before it counts as committed, so a publish acknowledged after
commit survives a machine crash. `periodic` is the default and bounds loss to
one interval. `none` survives a process crash but not a machine crash.
Replication is chosen per stream: a `Leader` stream's committed write is on the
leader, a `Quorum` stream's on a majority of the replicas. For an audit log,
choose `Quorum` and `on_commit`, or set `ack_on_commit` on a `Leader` stream. See
[Durable storage](/architecture/durable-storage/) for what each combination
guarantees.

Every event on a durable stream carries its log offset. A consumer stores the
offset it last handled and resumes at offset plus one. A jump between
consecutive offsets, less any `skipped_before` the event reports, means the
subscriber's queue dropped events, which is how a reader notices a gap instead of
silently missing records.

Records carry the time the broker
appended them, so you can look up the first offset at or after a time and replay
from there. A durable stream can also store the principal that published each record,
the subject of the token the broker accepted the write from, so replay says who
wrote what. Producers can be idempotent, so a retried publish does not
appear in the log twice.

## Example

A quorum-replicated stream with a week of retention:

```bash
felixctl stream create audit --shards 3 --replication 3 --consistency quorum --retention-secs 604800
```

Write with an idempotent producer, so a retry after a lost acknowledgement does
not duplicate the entry:

```rust
let cluster = Arc::new(cluster);
let producer = cluster.idempotent_producer().await?;
producer
    .publish_keyed("acme", "prod", "audit", Bytes::from("account-17"), entry)
    .await?;
```

Debugging 14:02: find the offset for that time on each shard, then read a
bounded range and print who wrote each record. Set `timestamps` and `publishers`
in the `ClientConfig` first so events carry them.

```rust
let shard = 0;
let from = client
    .offset_for_time("acme", "prod", "audit", shard, since_micros)
    .await?
    .unwrap_or(0);
let end = client
    .offset_for_time("acme", "prod", "audit", shard, until_micros)
    .await?;

let mut at = from;
loop {
    let page = client
        .read("acme", "prod", "audit", shard, at, end, 500)
        .await?;
    if page.records.is_empty() {
        break;
    }
    for record in &page.records {
        println!("{} {:?}", record.offset, record.payload);
    }
    at = page.next_offset;
}
```

`read` is a bounded range read: it registers nothing with the stream's fanout,
never waits at the tail, and returns the offset to continue from. To replay and
then keep following live events, subscribe at the offset instead:

```rust
use felix_client::StartPosition;

let mut replay = client
    .subscribe_from("acme", "prod", "audit", Some(StartPosition::Offset(from)))
    .await?;
while let Some(event) = replay.next_event().await? {
    // event.offset, event.timestamp_micros and event.publisher
}
```

From a shell, `felixctl sub audit --from 1500 --format offsets` resumes at an
offset, and `--from earliest --count 10` shows the oldest retained records.
Rebuilding a projection is the same loop with `apply(record)` in place of
`println!`, then a `subscribe_from` at the final `next_offset`.

## Compared with Kafka, JetStream and an audit table

Kafka has broader retention and compaction options, offload that can be read
back, exactly-once semantics between topics, and the ecosystem. Felix puts the
log, the live fanout, the cache and the queues on the same offsets, and has a
Kafka listener so kcat or a partition-assigning client can still read the log
(see [Coming from Kafka](/use-cases/kafka-migration/)). Pick Kafka when you need
history that outlives one machine, or its connectors and stream processing.

JetStream also replays by sequence or time. Felix adds offsets that mean the
same thing on the cache, queue and Kafka views of a stream, and a log whose
records cannot be rewritten. In Felix a plain subscriber keeps its own offset,
and server-side tracking is a consumer group, a separate read path (see
[Queues](/features/queues/)). Pick JetStream if you want every consumer's
position tracked on the server.

An audit table commits with the business change, which a separate log matches
only when you write both through [one commit](/use-cases/event-and-state/). What
Felix gives you is a log on its own storage, with record checksums, that no
UPDATE statement can reach and that does not share the database's load, retention
or access controls. Pick the table when the audit row must commit atomically with
data that has to stay in your database.

## Try it

Start the stack from the
[Quickstart](/getting-started/quickstart/#try-it-in-one-command) (it covers Podman
too) and run these from the directory holding `compose.yaml`.

`orders` is durable and may already hold records, such as the quickstart's
`hello`. Publish three, and note the offset the first one prints:

```bash
docker compose run --rm felixctl pub orders 'account opened'
docker compose run --rm felixctl pub orders 'limit raised'
docker compose run --rm felixctl pub orders 'account closed'
```

The first prints `published 1 to orders at offset N`. Read from there, and
again from the next offset:

```bash
docker compose run --rm felixctl sub orders --from N --count 3 --format offsets
docker compose run --rm felixctl sub orders --from N+1 --count 2 --format offsets
```

Write the offsets as numbers, for example `--from 7` and `--from 8`. Each output
line starts with the shard and the log offset. `--from earliest` replays
everything the stream holds, including older records. Nothing is removed when
a record is read, so a consumer that rebuilds a projection can resume from the
last offset it saw.

## Where it stops

Only durable streams are replayable. An in-memory stream has no log, no times
and no offsets. Retention is off by default and a read below what retention kept
fails; set a bound on the stream. Tiered storage is partial: sealed segments can
be offloaded to a directory, verified and recorded before retention may delete
them, but nothing reads the copies back yet, so replay reaches only what is
local. There is no cloud backend.

Record times are the clock of the broker that led when the record was written,
so a clock that steps back makes an offset-for-time lookup approximate. The
publisher principal is the broker vouching for who sent the write, not for the
payload, and it is stored only when `FELIX_RECORD_PUBLISHERS` (or the
`publisher_principal` fleet feature in a cluster) is on. That is a one-way
change to the storage format for the data directory. The Rust client exposes
publisher identity, `offset_for_time` and range reads; the Python and TypeScript
bindings document none of them yet, so from those languages use the Kafka
listener or felixctl for replay. Idempotent producers are Rust only. Streams are not
compacted by key, so a stream grows until retention trims it.

## Related

- [Durable storage](/architecture/durable-storage/)
- [Publish/Subscribe](/features/pubsub/)
- [Rust client: replaying from a time](/clients/rust/#when-an-event-was-written-and-replaying-from-a-time)
- [Kafka compatibility](/features/kafka/)
