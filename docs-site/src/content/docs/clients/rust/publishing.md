---
title: "Rust: Publishing"
description: "Publish single records and batches, route by key, and send safely with idempotent producers."
---

Part of the [Rust client guide](/clients/rust/). Setup and configuration are on that page.

## Publishing

### Single Message Publish

```rust
use felix_client::AckMode;

// Fire-and-forget: returns once queued. Call `client.finish()` before exiting.
client
    .publish("acme", "prod", "events", b"message".to_vec(), AckMode::None)
    .await?;

// Returns once the broker acknowledges it.
let offset = client
    .publish("acme", "prod", "events", b"important".to_vec(), AckMode::PerMessage)
    .await?;
println!("written at {offset:?}");
```

An acknowledged publish returns `Option<u64>`: the log offset of the first
record in the batch. The rest of a batch follow it with no gaps, so record `i`
is at `offset + i`. It is `None` when the broker acknowledged the batch as soon
as it was queued instead of once it was written (an in-memory stream, or a
broker run with `FELIX_ACK_ON_COMMIT=0`), when the stream has no log, and against a broker older
than the offset flag. An unacknowledged publish always returns `None`. On a
`Quorum` stream the offset is where the batch was committed, and a re-sent
`IdempotentProducer` batch reports where the first copy landed. Only the
broker that owns the shard acknowledges before writing; a publish forwarded
through another broker is answered after the write and has its offset, so one
stream can return both.

A durable stream's ack carries its offset by default. To get the offset of every
acknowledged publish, in-memory streams included, without turning
`FELIX_ACK_ON_COMMIT` on for the whole broker, set `ack_on_commit: true` in the
`ClientConfig`. That client's connections ask for commit acks
(`FEATURE_ACK_ON_COMMIT`), and every other client on the broker keeps the
broker's default. `Connection::supports_ack_on_commit()` says whether the broker
honours it; one that predates it answers as before.

### The routing key decides the shard

**Without a key every record lands on shard 0**, so a multi-shard stream
behaves like a single-shard one. If you created a stream with several shards to
get throughput and are not passing a key, you are not getting it.

```rust
use bytes::Bytes;

client
    .publish_keyed(
        "acme",
        "prod",
        "orders",
        payload,
        Bytes::from(customer_id),
        AckMode::PerMessage,
    )
    .await?;
```

Records sharing a key share a shard and stay ordered with respect to each
other. Records with different keys do not, once a stream has more than one
shard. A consumer needing total order wants a single-shard stream.

Which shard a key lands on follows the stream's own mapping, modulo or jump
hash, fixed when the stream was created. `Client` asks for it once per
stream along with the shard count (`Connection::stream_routing`), so it computes
the same shard the broker does.

### Naming the shard

A producer that owns its partitioning, one shard per partition, can name the
shard instead of searching for keys that hash where it wants them:

```rust
// District 2 of the city writes to shard 2, whatever its name hashes to.
client
    .publish_batch_on_shard("acme", "city", "ticks", 2, vec![tick], AckMode::PerBatch)
    .await?;
```

`publish_on_shard` and `publish_batch_on_shard` (on `Publisher` and
`Client`) use the shard as given. `Client` sends the batch to
that shard's owner, the way it does for a key's shard, and the batch goes on
the same writer as keyed publishes to that shard, so the two stay in order.
A shard the stream does not have is refused with `invalid_request` and nothing
is written; it is never wrapped onto another shard. The same goes for
`publish_if_on_shard` and the commits below.

It needs a broker that advertised `FEATURE_EXPLICIT_SHARD`, and fails without
sending anything otherwise: an older broker would ignore the shard and write
the batch to shard 0. The binary publish frames have no room for a shard, so
these batches are sent as JSON. Caches take no shard: a cache value is found
by hashing its key on every read.

### At-least-once may duplicate

By default a publish whose outcome was ambiguous (the broker may or may not
have written it before the connection went) is **reported, not re-sent**,
because nothing downstream can tell two copies apart.

```rust
client
    .publish_at_least_once("acme", "prod", "orders", payload, AckMode::PerMessage)
    .await?;
```

With this opt-in the record is certain to land and **may land twice**.
It does not carry a routing key, because the re-send path has nowhere to put
one. A keyed record that must be re-sent goes through
`publish_records_at_least_once` as a batch of one, which is what the Python and
TypeScript bindings do when `at_least_once` comes with a key.

For at-least-once *without* the duplication, see
[`IdempotentProducer`](#idempotent-producers).

### Batch Publishing

Publish multiple messages efficiently:

```rust
let messages = vec![
    b"message 1".to_vec(),
    b"message 2".to_vec(),
    b"message 3".to_vec(),
];

client
    .publish_batch("acme", "prod", "events", messages, AckMode::PerBatch)
    .await?;
```

### Batches through a Client

`Client` batches go straight to each shard's leader. A plain `Connection`
sends every batch to the broker it is connected to, and on a cluster that broker
forwards the batches for shards it does not lead.

```rust
// One key: the batch is one frame to that shard's leader. The result is the
// offset of the batch's first record. It is None for an in-memory stream or
// AckMode::None, as for a single publish.
let first_offset = client
    .publish_batch_keyed("acme", "prod", "orders", key, payloads, AckMode::PerBatch)
    .await?;
if let Some(offset) = first_offset {
    tracing::debug!(offset, "batch appended");
}

// Records with their own keys: split by shard, one ordered frame per shard,
// sent to the leaders concurrently.
let report = client
    .publish_records("acme", "prod", "orders", records, AckMode::PerBatch)
    .await;
for piece in report.failed() {
    if let Err(err) = &piece.outcome {
        tracing::warn!(shard = piece.shard, records = ?piece.records, "batch failed: {err:#}");
    }
}
```

- **Order.** A shard's records go in the order you gave them, as one batch the
  broker appends contiguously. Different shards have no order between them.
- **Retries.** As for a single publish: a batch the broker may have written is
  not sent again, so nothing arrives twice. A batch a cached leader refused
  without applying (it moved or is draining) is sent once through the entry
  broker, which forwards it and names the new leader.
  `publish_records_at_least_once` also re-sends ambiguous batches after the
  reconnect backoff and **may duplicate**.
- **Failure is per shard.** `BatchReport` lists each shard's records by their
  position in your input and whether that shard was accepted. A failed shard
  does not stop the others, and no record goes unreported. With `AckMode::None`
  a shard counts as accepted once queued.
- **Unkeyed.** `publish_batch` puts the whole batch on shard 0, like
  `publish`.
- Not idempotent. Use [`IdempotentProducer`](#idempotent-producers) when a
  re-send must not duplicate.

The first publish to a stream asks the cluster who leads each shard (the pieces
of a batch racing it wait for the answer, up to a second), and the answer is
refreshed in the background every few seconds or soon after a leader turns out
wrong, so even the first batch, and a publish that asks for no ack
(`AckMode::None`), goes to the leader. A cluster that does not answer in time
costs one wait and the batch is forwarded as before. Leaders are connected on
first use, so an unreachable one only affects its own shards. A write that
lands while leadership is moving may still be forwarded by the old leader. With
`AckMode::None`, two calls can reach the broker out of order if the route for a
shard switches between them; await acks where order across calls matters. After
`publish_records_at_least_once` re-splits records under a new width, the report
can list a shard twice.

Each shard's batch uses that shard's own publish stream while the client has
room for one (`FELIX_PUB_SHARD_STREAMS`), so a slow shard does not hold up the
others.

### Keys and headers

A record can carry a key and headers: small metadata such as a content type or
a trace id that travels with the payload and comes back on `event.headers`.
Build a `RecordHeaders` and publish with `publish_with_headers`, or
`publish_batch_with_headers` for a batch with one `RecordHeaders` per payload
(an empty one leaves that record plain).

```rust
use felix_client::{RecordHeaders, RecordHeadersError};

let headers = RecordHeaders {
    key: Some(Bytes::from_static(b"order-7")),
    headers: vec![
        ("content-type".to_string(), Bytes::from_static(b"application/json")),
        ("trace-id".to_string(), Bytes::from_static(b"4bf92f35")),
    ],
};
let payload = br#"{"total": 42}"#.to_vec();
match client
    .publish_with_headers("acme", "prod", "orders", headers, payload.clone(), AckMode::PerBatch)
    .await
{
    Ok(_) => {}
    // The broker predates record headers. Nothing was sent, so the payload
    // can go without them if the consumers can do without.
    Err(err) if matches!(
        err.downcast_ref::<RecordHeadersError>(),
        Some(RecordHeadersError::Unsupported)
    ) => {
        client
            .publish("acme", "prod", "orders", payload, AckMode::PerBatch)
            .await?;
    }
    Err(err) => return Err(err),
}

let mut orders = client.subscribe("acme", "prod", "orders").await?;
while let Some(event) = orders.next_event().await? {
    let Some(headers) = &event.headers else {
        continue; // published without a key or headers
    };
    for (name, value) in &headers.headers {
        tracing::info!(name, value = ?value, "header");
    }
}
```

- **The key does not route.** It belongs to the record. A record goes to the
  shard `publish` would send it to, whatever its key.
- **Names.** A header name is UTF-8 and not empty, and a name may repeat.
  Names starting with `felix.` are reserved for the broker and refused with
  `RecordHeadersError::ReservedName` before anything is sent.
- **Older brokers.** A publish with headers against a broker that does not
  accept them (an older one, a single broker run with
  `FELIX_RECORD_HEADERS=false`, or a broker in a cluster whose fleet has not
  finalized `record_headers`) fails with
  `RecordHeadersError::Unsupported` and sends nothing, so headers are never
  dropped quietly. A publish with no headers works
  everywhere. On the read side `event.headers` is `None` there.
- **Opting out.** `record_headers` in the `ClientConfig` is on by default. Set
  it to `false` and the client neither offers nor accepts headers.
- Idempotent producers and routing keys do not carry headers.
- **Where headers do not reach.** A clustered broker refuses them until its
  fleet finalizes `record_headers`. Consumer groups do not deliver them. The
  Kafka listener drops Kafka keys and headers unless `FELIX_KAFKA_RECORD_HEADERS`
  is on. Only the Rust client sends and
  reads them.

### Publisher API

`Publisher` belongs to the single-connection `Connection`, which applications
should not need; `Client` publishes through the same workers per broker.
For a tool that holds a `Connection`:

```rust
use felix_client::AckMode;

// Uses the pools and sharding from the client's `ClientConfig`.
let publisher = client.publisher().await?;

for i in 0..10_000 {
    let payload = format!("event {i}");
    publisher
        .publish("acme", "prod", "events", payload.into_bytes(), AckMode::None)
        .await?;
}

// `AckMode::None` returns once queued: wait for the writes before going on.
publisher.finish().await?;
```

### Publisher Sharding

`ClientConfig::publish_sharding` decides which worker stream a publish goes on:

```rust
use felix_client::PublishSharding;

// Spread publishes evenly over the workers, giving up per-stream order.
config.publish_sharding = PublishSharding::RoundRobin;
```

`HashStream` is the default. It sends every publish to one stream through the
same writer, so each stream's publishes reach the broker in order.
`RoundRobin` spreads load evenly across writers, but publishes to one stream
can arrive out of order.

A shard stalled on a quorum does not hold up the other shards on its publish
stream. A broker that grants the client a publish window also agrees to answer
plain acked publishes as they complete, and the client keeps writing to the
stream while an answer is outstanding, so publishes to healthy shards are
answered while the stalled one waits. Each shard keeps its own order, because
the broker commits a shard's publishes in the order the stream carried them.
Only an idempotent producer's batches are answered in the order they were
sent; see [idempotent producers](#idempotent-producers).

Under `HashStream` the client also gives the first shards it sees a stream of
their own. A stalled shard still holds its slots in the stream's publish
window, so on a stream of its own it uses up only its own window. Each shard stream
is placed on the least-loaded connection, so a busy multi-shard stream
spreads over the client's connections and the broker's listeners instead of
riding one. The stream opens on the shard's first publish, which pays one
extra round trip to authenticate it, and stays open. A client keeps at most
`publish_shard_streams` of them per broker (4 by default,
`FELIX_PUB_SHARD_STREAMS`); shards past that share the pool.

The cap is per broker and first come, first served across every stream the
client publishes to: the first four shards (tenant, namespace, stream, shard)
it publishes to each get a dedicated stream, and every later shard uses the
shared pool permanently. A stalled shard on the pool takes slots from the
pool stream's window while it waits; its answers no longer hold back the other
pooled shards' answers.

Four fully isolates streams of up to four shards. Isolation matters when a
shard can stall on its own, as with quorum waits or a dead follower on a
replicated stream. On a wide stream any cap protects only the first N shards
seen, so a high cap buys arbitrary partial protection at a throughput cost: on
an 8 vCPU broker with a 48-shard stream and 256 B acked publishes, 16 streams
measured about 97 MB/s, 4 about 125 MB/s and 0 (everything on the pool) about
162 MB/s. For wide replicated streams that need isolation, raise
`FELIX_PUB_SHARD_STREAMS` to the total number of shards the client publishes
to on a broker.

Each broker connection asks for a stream's width on its first keyed publish
to it (one more round trip) and picks a key's stream from that answer alone,
whoever publishes: a plain publisher, a `Client` or an idempotent
producer. So two of them never put one key on two writers. If the width
cannot be learned, every keyed publish to that stream goes on shard 0's
stream, as one writer.

A stream deleted and created again at another width makes the kept width
stale, and two keys that now share a shard could go out on two writers. A
broker advertising `FEATURE_PUBLISH_WIDTH` is told the width each keyed batch
was routed by and refuses one routed by a width the stream no longer has, so
nothing sent that way is written. An acknowledged publish fails with
`stale_width` (`Retry`), and the client drops the width so a retry asks for
the new one. A fire-and-forget publish is dropped; the client asks for a
width again every 5 seconds while it publishes to the stream, so it catches
up without a refusal to read. A refusal with `not_found` also drops the
width. Against an older broker the width is kept until `not_found`, as
before.

A `Client` keeps its own widths, to pick the broker for a key, and an
idempotent producer numbers each shard's batches by one. Both are dropped and
asked again the same way, and a batch is routed and declared by the width that
picked its broker or numbered it, so the broker that takes it checks that
width. Under a new width a producer starts the stream's sequences again under
a fresh producer id for that stream, so they never meet sequences a leader
still holds. While a batch on the stream is in doubt it does not: the batch
may have landed, and only a re-send under the width and id it was numbered by
is answered as a duplicate. Until it settles, a re-send under a width that
has gone is refused with an error that says so. If the width will not come
back, `abandon_in_doubt(tenant, namespace, stream)` gives those batches up
and numbers the stream afresh under a new id; an abandoned batch may land
twice. `IdempotentProducer::producer_id`
keeps returning the first id; `producer_id_for(tenant, namespace, stream)`
returns the one a stream's batches go out under.

The ordering guarantee is per shard: every publish one client makes to one
shard goes through one writer and one QUIC stream, so the shard holds them in
the order they were issued. Unkeyed publishes are all shard 0, so a stream
published without keys, like a single-shard stream, keeps total order and
stays on one connection per client.

### Errors you can act on

Calls return `anyhow::Result`, and the cases worth branching on are carried as
typed errors inside it. Recover them with `downcast_ref`. Matching on the
message would break the first time one is reworded.

[Errors Across Clients](/clients/errors/) maps every broker code and retry class to the type each client raises.

```rust
use felix_client::{AckMode, BrokerError, RetryClass};

let result = client
    .publish("acme", "prod", "events", payload, AckMode::PerMessage)
    .await;
if let Err(err) = result {
    match err.downcast_ref::<BrokerError>() {
        // Nothing was applied, so sending the record again later is safe.
        Some(refusal) if matches!(refusal.retry, RetryClass::Retry | RetryClass::RetryAfter) => {
            tracing::warn!(code = ?refusal.code, "publish refused; will retry later");
        }
        // It may have landed (`OutcomeUnknown`), will never succeed
        // (`Fatal`), or no broker answered at all: report it.
        _ => return Err(err),
    }
}
```

| Type | Recover with | What it means |
| --- | --- | --- |
| `SubscribeCursorError` | `downcast_ref` | the start offset is gone, or ahead of the tail |
| `NotLeaderError` | `downcast_ref` | the broker does not own the shard; this is routing, not failure |
| `PublishRefused` | `downcast_ref` | an idempotent publish the broker would not append, with the reason |
| `BrokerError` | `downcast_ref` | any other refusal from a broker that sends error codes: the `code`, and a `retry` class saying whether the request may have been applied |

`BrokerError.retry` is the field to branch on. `OutcomeUnknown` (a quorum
timeout, say) means the publish may have landed, so resending a plain publish
can write it twice; `Retry`, `RetryAfter` and `Redirect` mean nothing was
applied. A broker that predates error codes returns the same failures as plain
errors with the same text, so treat a missing `BrokerError` as "no code", not as
success. The codes and their classes are listed under
[Error codes](https://github.com/GetFelix/felix/blob/main/docs/protocol.md#error-codes).

`Client` already acts on the class before an error reaches you:

- `Fatal` is returned at once.
- `OutcomeUnknown` is returned by `publish` and never re-sent.
  `publish_at_least_once` and an idempotent producer send it again: the first
  accepts duplicates by name, the second's sequence prevents them.
- `Retry` and `Redirect` (`shard_unavailable`, `draining`, `not_leader`) from a
  cached shard owner drop that owner and go straight to the entry broker, even
  from `publish`, since nothing was applied. From the entry broker the retrying
  paths back off. `stale_width` backs off too: it does not say the owner is
  wrong. `stale_claim` is never retried; the client marks it `fatal`.
- `RetryAfter` backs off for at least `retry_after_ms`. `not_found` is retried
  for 5 s from the first one, long enough for a newly promoted broker to hear
  about the stream from the control plane and no longer.
- `publish` replaces its connection only for `draining` or when no broker
  answered; any other coded answer came from a live broker.

A broker without error codes gets the old handling: only a credential failure
is final, and everything else is retried. The full table is under "Retries" in
[the multi-node client guide](https://github.com/GetFelix/felix/blob/main/docs/multi-node-client.md#retries).

`SubscribeCursorError` says where a refused reader can resume:

```rust
use felix_client::{StartPosition, SubscribeCursorError};

if let Some(cursor) = err.downcast_ref::<SubscribeCursorError>() {
    // `available` is the nearest offset that would have worked: the oldest
    // retained for TooOld, the current tail for InFuture. Resuming from it is
    // the smallest gap you can take rather than restarting at `earliest`.
    tracing::warn!(requested = cursor.requested, nearest = cursor.available, "cursor out of range");
    start = StartPosition::Offset(cursor.available);
}
```

A `TooOld` below a head trim's floor also carries `trim_headers`, the bytes the
trim was written with (see [Trimming a shard's head](/clients/rust/groups-and-commits/#trimming-a-shards-head)).
It is `None` when retention set the floor.

## Idempotent producers

At-least-once *without* the duplication. The producer numbers its batches, the
shard's leader remembers the last few, and a batch carrying a sequence it
already holds is answered from memory rather than appended, so a re-send after
a lost acknowledgement lands once.

The [Python](/clients/python/#idempotent-producers) and
[TypeScript](/clients/typescript/#idempotent-producers) bindings wrap it too, as
`producer()`.

The producer keeps a handle on its client, so `idempotent_producer` is called
on an `Arc<Client>` (or `Arc<Connection>`), and the producer it returns can
be stored or moved into a task without borrowing anything.

```rust
let client = Arc::new(client);
let producer = client.idempotent_producer().await?;

producer.publish("acme", "prod", "orders", payload).await?;

// Before exiting: let what the producer was handed finish.
producer.close_within(Duration::from_secs(10)).await?;
```

`publish_keyed` and `publish_batch_keyed` take a routing key, which picks the
shard as it does for a plain keyed publish. The leader numbers batches per
shard, so the producer keeps one sequence per shard: keys on the same shard
share it, and an unkeyed batch is shard 0's. The producer works out the shard
itself from the stream's width, asked once of a broker advertising
`FEATURE_STREAM_SHARDS`, and fails without sending if it cannot learn it.
Under a `Client` each shard's batches go on that shard's own publish
stream, as its other publishes do; a plain `Connection`'s producer stays on the
hashed pool with the rest of that client's publishes.

```rust
use bytes::Bytes;

producer
    .publish_keyed("acme", "prod", "orders", Bytes::from("customer-42"), payload)
    .await?;
```

One call at a time waits a round trip per batch. `publish_batches` sends several
batches in one call and keeps up to a window of them unanswered at once, under
consecutive sequences:

```rust
let batches: Vec<Vec<Vec<u8>>> = orders.iter().map(|order| vec![order.encode()]).collect();
producer.publish_batches("acme", "prod", "orders", batches).await?;
```

The window is whatever the broker granted the stream
(`FELIX_BROKER_PUBLISH_WINDOW`, 256 by default), capped at 64 because the leader
remembers 64 sequences per producer. The broker answers a stream's idempotent
batches in the order it sent them, even when it answers other publishes as they
complete, so when one fails the producer knows it is the
earliest failure; it and every batch behind it are in doubt, and a
`Client` producer re-sends them, in order, under the same sequences until
they land. If the call still fails, make **the same call again**: the batches
that were acknowledged are skipped and only the ones in doubt go out. Against a
broker that grants no window the call sends one batch at a time, with the same
result.

The sequence is the mechanism, so the failures are about the sequence and are
worth branching on:

```rust
use anyhow::bail;
use felix_client::{PublishRefusalReason, PublishRefused};

if let Err(err) = producer.publish("acme", "prod", "orders", payload).await {
    match err.downcast_ref::<PublishRefused>().map(|refused| &refused.reason) {
        // Something was skipped and is lost to this broker. Do not carry on
        // past it; the gap will not close by retrying.
        Some(PublishRefusalReason::SequenceGap { expected }) => bail!("gap at {expected}"),
        // A new leader knows no producers. Start again under a fresh id.
        Some(PublishRefusalReason::UnknownProducer) => {
            producer = client.idempotent_producer().await?;
        }
        // Older than the window the broker keeps, so whether it was appended
        // cannot be told any more.
        Some(PublishRefusalReason::SequenceExpired) => bail!("outside the dedup window"),
        // A different batch under a sequence the broker already holds: this
        // producer reused a number it had spent, and the batch was not written.
        Some(PublishRefusalReason::SequenceReused) => bail!("sequence reused"),
        // Any other refusal, or no answer at all: the batch is in doubt, so
        // the next call on this shard must be the same batch (see below).
        _ => return Err(err),
    }
}
```

On a durable stream the producer's sequences are stored in the log and
replicated, so they survive a failover, a planned move and a restart. On an
in-memory stream they are the leader's alone: a new leader answers
`UnknownProducer`, and the producer starts again under a new id rather than
being told a batch landed that nobody can vouch for.

:::note[Dropping a publish does not cancel it]
The producer sends from a task of its own, in the order calls were made, and a
call only hands its batches over and waits for the answer. Dropping the call's
future, in a `tokio::time::timeout`, a `select!` or a handler whose client hung
up, leaves the batch going out under its sequence and the producer usable.
Because the batch may still land after you stopped waiting, **do not publish it
again**. If its answer is in doubt and nobody is waiting for it, the producer
re-sends it itself before the next batch on that shard; if that re-send fails
too, the next call fails without sending and tries again on the call after.

Dropping the producer lets what it was handed finish. `close().await` waits for
that, which is the call to make before a process exits. `close_within(timeout)`
gives up after `timeout` and stops the producer where it is.
:::

A publish that returns an error other than a refusal is in doubt for the same
reason, but the producer still has the batch. The next call on that shard must
be the same batch, with the same key: it goes out under the same sequence and
lands once. A call with a different batch, or another key on the same shard,
fails without sending anything, so either re-send until it succeeds or take a
fresh id.
