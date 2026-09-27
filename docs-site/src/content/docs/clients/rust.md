---
title: "Rust Client SDK"
---

`felix-client` is the Rust SDK: publish, subscribe, cache, consumer groups,
and the cluster client, over pooled QUIC connections. This page is the
working reference — setup, configuration, and the patterns that matter in
practice.

It is also what every other language binds to rather than reimplementing —
see [Clients in Other Languages](/felix/clients/overview/) for Python and for how
new languages are gated on a conformance suite.

## Installation

```bash
cargo add felix-client
```

Or in `Cargo.toml`:

```toml
[dependencies]
felix-client = "0.5"
```

Optional features:

```toml
[dependencies]
felix-client = { version = "0.5", features = ["telemetry"] }
```

**Features**:

- `telemetry`: per-operation timing and frame counters (adds overhead)

## Quick Start

### Basic Publish/Subscribe

```rust
use felix_client::{Client, ClientConfig};
use std::net::SocketAddr;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Connect to broker
    let quinn = quinn::ClientConfig::with_platform_verifier();
    let config = ClientConfig::optimized_defaults(quinn);
    let addr: SocketAddr = "127.0.0.1:5000".parse()?;
    let client = Client::connect(addr, "localhost", config).await?;
    let publisher = client.publisher().await?;

    // Publish a message
    use felix_wire::AckMode;
    publisher
        .publish(
            "acme",           // tenant_id
            "prod",           // namespace
            "events",         // stream
            b"Hello Felix".to_vec(), // payload
            AckMode::None,
        )
        .await?;

    // Subscribe to stream
    let mut subscription = client.subscribe(
        "acme",
        "prod",
        "events"
    ).await?;

    // Receive events
    while let Some(event) = subscription.next_event().await? {
        println!("Received: {:?}", event.payload);
    }

    Ok(())
}
```

### Basic Cache Operations

```rust
use bytes::Bytes;

// Store value with 60-second TTL
client.cache_put(
    "acme",
    "prod",
    "sessions",
    "user-123",
    Bytes::from_static(b"session-data"),
    Some(60_000)  // TTL in milliseconds
).await?;

// Retrieve value
match client.cache_get("acme", "prod", "sessions", "user-123").await? {
    Some(value) => println!("Found: {:?}", value),
    None => println!("Not found or expired"),
}
```

## Client Configuration

### ClientConfig

```rust
use felix_client::{ClientConfig, PublishSharding};
use std::net::SocketAddr;

let quinn = quinn::ClientConfig::with_platform_verifier();
let config = ClientConfig {
    // Connection pools
    event_conn_pool: 8,              // Connections for pub/sub
    cache_conn_pool: 8,              // Connections for cache
    publish_conn_pool: 4,            // Connections for publishing
    
    // Streams per connection
    publish_streams_per_conn: 2,     // Publish streams per conn
    cache_streams_per_conn: 4,       // Cache streams per conn
    
    // Publish sharding
    publish_sharding: PublishSharding::HashStream,

    ..ClientConfig::optimized_defaults(quinn)
};

let addr: SocketAddr = "127.0.0.1:5000".parse()?;
let client = Client::connect(addr, "localhost", config).await?;
```

### TLS and ALPN

`ClientConfig` takes a ready-made `quinn::ClientConfig`.
`felix_client::quic_client_config` builds one: it verifies the broker against
the roots you pass, or the platform trust store for `None`, and offers the
`felix/1` ALPN when its second argument is `true`.

```rust
let quinn = felix_client::quic_client_config(None, true)?;
let config = ClientConfig::optimized_defaults(quinn);
```

A broker with `FELIX_TLS_REQUIRE_ALPN=true` serves only clients that offer
`felix/1`. A broker older than ALPN support refuses them, so offer it only once
every broker you connect to is current. If you build the rustls config yourself
(for a client certificate, say), set `alpn_protocols` to
`vec![felix_wire::CLIENT_ALPN.to_vec()]` to offer it.

### Configuration Tuning

**Low-latency configuration**:

```rust
let quinn = quinn::ClientConfig::with_platform_verifier();
let config = ClientConfig {
    event_conn_pool: 4,
    cache_conn_pool: 4,
    publish_streams_per_conn: 1,
    cache_streams_per_conn: 2,
    publish_conn_pool: 2,
    ..ClientConfig::optimized_defaults(quinn)
};
```

**High-throughput configuration**:

```rust
let quinn = quinn::ClientConfig::with_platform_verifier();
let config = ClientConfig {
    event_conn_pool: 16,
    cache_conn_pool: 16,
    publish_streams_per_conn: 4,
    cache_streams_per_conn: 8,
    publish_conn_pool: 8,
    publish_sharding: PublishSharding::HashStream,
    ..ClientConfig::optimized_defaults(quinn)
};
```

## Publishing

### Single Message Publish

```rust
// Fire-and-forget (no ack)
use felix_wire::AckMode;
let publisher = client.publisher().await?;
publisher
    .publish("acme", "prod", "events", b"message".to_vec(), AckMode::None)
    .await?;

// With acknowledgement
publisher
    .publish("acme", "prod", "events", b"important".to_vec(), AckMode::PerMessage)
    .await?;
```

### The routing key decides the shard

**Without a key every record lands on shard 0**, so a multi-shard stream
behaves like a single-shard one. If you created a stream with several shards to
get throughput and are not passing a key, you are not getting it.

```rust
cluster
    .publish_keyed(
        "acme", "prod", "orders",
        payload,
        bytes::Bytes::from(customer_id),
        AckMode::PerMessage,
    )
    .await?;
```

Records sharing a key share a shard and stay ordered with respect to each
other. Records with different keys do not, once a stream has more than one
shard. A consumer needing total order wants a single-shard stream.

### At-least-once duplicates, and says so

By default a publish whose outcome was ambiguous — the broker may or may not
have written it before the connection went — is **reported, not re-sent**,
because nothing downstream can tell two copies apart.

```rust
cluster
    .publish_at_least_once("acme", "prod", "orders", payload, AckMode::PerMessage)
    .await?;
```

That is the opt-in: the record is then certain to land and **may land twice**.
It does not carry a routing key — the re-send path has nowhere to put one — so
it is `publish_keyed` or `publish_at_least_once`, not both.

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

publisher.publish_batch(
    "acme",
    "prod",
    "events",
    messages,
    AckMode::PerBatch,
).await?;
```

### Publisher API

For high-throughput publishing, use the `Publisher` API:

```rust
use felix_client::Publisher;
use felix_wire::AckMode;

// Create publisher (uses ClientConfig settings)
let publisher = client.publisher().await?;

// Publish messages
for i in 0..10000 {
    let payload = format!("Event {}", i);
    publisher
        .publish("acme", "prod", "events", payload.into_bytes(), AckMode::None)
        .await?;
}
```

### Publisher Sharding

Control load distribution across worker streams:

```rust
use felix_client::PublishSharding;

// Round-robin: distribute evenly across workers
PublishSharding::RoundRobin

// Hash-based: consistent hashing by stream name
PublishSharding::HashStream
```

**When to use each**:

- **RoundRobin**: Default, good for single stream, evenly distributes load
- **HashStream**: Publishing to multiple streams, keeps stream-specific ordering

### Errors you can act on

Calls return `anyhow::Result`, and the cases worth branching on are carried as
typed errors inside it. Recover them with `downcast_ref` — matching on the
message would break the first time one is reworded.

```rust
use felix_client::{PublishRefused, PublishRefusalReason, SubscribeCursorError};

match cluster.publish("acme", "prod", "events", payload, AckMode::PerMessage).await {
    Ok(()) => {}
    Err(err) => {
        if let Some(refused) = err.downcast_ref::<PublishRefused>() {
            match refused.reason {
                // Routing, not a failure: the client already followed it.
                PublishRefusalReason::NotLeader { .. } => {}
                // The producer's sequence cannot be mended by retrying.
                _ => return Err(err),
            }
        }
        // Everything else: the cluster client has already tried the other
        // brokers, so arriving here means none of them answered.
        return Err(err);
    }
}
```

| Type | Recover with | What it means |
| --- | --- | --- |
| `SubscribeCursorError` | `downcast_ref` | the start offset is gone, or ahead of the tail |
| `NotLeaderError` | `downcast_ref` | the broker does not own the shard — routing, not failure |
| `PublishRefused` | `downcast_ref` | an idempotent publish the broker would not append, with the reason |
| `BrokerError` | `downcast_ref` | any other refusal from a broker that sends error codes: the `code`, and a `retry` class saying whether the request may have been applied |

`BrokerError.retry` is the field to branch on. `OutcomeUnknown` — a quorum
timeout, say — means the publish may have landed, so resending a plain publish
can write it twice; `Retry`, `RetryAfter` and `Redirect` mean nothing was
applied. A broker that predates error codes returns the same failures as plain
errors with the same text, so treat a missing `BrokerError` as "no code", not as
success. The codes and their classes are listed under
[Error codes](https://github.com/gabloe/felix/blob/main/docs/protocol.md#error-codes).

`ClusterClient` already acts on the class before an error reaches you:

- `Fatal` is returned at once.
- `OutcomeUnknown` is returned by `publish` and never re-sent.
  `publish_at_least_once` and an idempotent producer send it again: the first
  accepts duplicates by name, the second's sequence prevents them.
- `Retry` and `Redirect` (`shard_unavailable`, `draining`, `not_leader`) from a
  cached shard owner drop that owner and go straight to the entry broker, even
  from `publish`, since nothing was applied. From the entry broker the retrying
  paths back off.
- `RetryAfter` backs off for at least `retry_after_ms`. `not_found` is retried
  for 5 s from the first one, long enough for a newly promoted broker to hear
  about the stream from the control plane and no longer.
- `publish` replaces its connection only for `draining` or when no broker
  answered; any other coded answer came from a live broker.

A broker without error codes gets the old handling: only a credential failure
is final, and everything else is retried. The full table is under "Retries" in
[the multi-node client guide](https://github.com/gabloe/felix/blob/main/docs/multi-node-client.md#retries).

`SubscribeCursorError` carries more than the other clients get:

```rust
if let Some(cursor) = err.downcast_ref::<SubscribeCursorError>() {
    // `available` is the nearest offset that would have worked — the oldest
    // retained for TooOld, the current tail for InFuture. Resuming from it is
    // the smallest gap you can take rather than restarting at `earliest`.
    eprintln!("asked for {}, nearest is {}", cursor.requested, cursor.available);
    start = StartPosition::Offset(cursor.available);
}
```

## Idempotent producers

At-least-once *without* the duplication. The producer numbers its batches, the
shard's leader remembers the last few, and a batch carrying a sequence it
already holds is answered from memory rather than appended — so a re-send after
a lost acknowledgement lands once.

Rust only: neither binding wraps this yet.

```rust
let producer = cluster.idempotent_producer().await?;

producer
    .publish("acme", "prod", "orders", payload)
    .await?;
```

The sequence is the mechanism, so the failures are about the sequence and are
worth branching on:

```rust
use felix_client::{PublishRefused, PublishRefusalReason};

if let Err(err) = producer.publish("acme", "prod", "orders", payload).await
    && let Some(refused) = err.downcast_ref::<PublishRefused>()
{
    match &refused.reason {
        // Something was skipped and is lost to this broker. Do not carry on
        // past it; the gap will not close by retrying.
        PublishRefusalReason::SequenceGap { expected } => bail!("gap at {expected}"),
        // A new leader knows no producers. Take a fresh id and start again.
        PublishRefusalReason::UnknownProducer => reinitialise().await?,
        // Older than the window the broker keeps, so whether it was appended
        // cannot be told any more.
        PublishRefusalReason::SequenceExpired => bail!("outside the dedup window"),
        // A different batch under a sequence the broker already holds: this
        // producer reused a number it had spent, and the batch was not written.
        PublishRefusalReason::SequenceReused => bail!("sequence reused"),
        // Routing, not failure: the client follows it itself.
        PublishRefusalReason::NotLeader { .. } => {}
        _ => return Err(err),
    }
}
```

The producer's state is the **leader's and in memory**. It survives everything
but the leader itself: a new leader answers `UnknownProducer`, and the producer
starts again under a new id rather than being told a batch landed that nobody
can vouch for.

:::caution[Do not race this against a timeout]
`publish_batch` is not cancel-safe, and the consequence is specific rather than
vague. Dropping the future mid-send leaves the sequence in doubt: the batch may
have been appended under it, and the cursor still points at it. A broker that
predates `sequence_reused` answers a remembered sequence *without appending*,
so reusing it there would discard a different batch and report success; a
current broker refuses it, but the producer cannot tell which it has.

So a cancelled publish **stops the producer** — the next call refuses and says
why, and you take a fresh id. A producer is cheap to re-initialise; silently
dropped records are not cheap at all.
:::

A publish that returns an error other than a refusal is in doubt for the same
reason, but the producer still has the batch. The next call on that stream must
be the same batch: it goes out under the same sequence and lands once. A call
with a different batch fails without sending anything, so either re-send until
it succeeds or take a fresh id.

## Subscribing

### Creating Subscriptions

```rust
let mut subscription = client.subscribe("acme", "prod", "events").await?;

// Process events
while let Some(event) = subscription.next_event().await? {
    process_event(event).await?;
}
```

`None` means the broker ended the stream. A lost connection is an error,
`SubscriptionLost`, so the loop above stops with `?` rather than exiting as if
the stream had finished. A `ClusterClient` subscription resubscribes by itself
from the offset after the last one it delivered; on an in-memory stream there
is no offset to resume from, so it returns the error.

### Event Structure

```rust
use bytes::Bytes;
use std::sync::Arc;

pub struct Event {
    pub tenant_id: Arc<str>,
    pub namespace: Arc<str>,
    pub stream: Arc<str>,
    pub payload: Bytes,
    /// The log offset on a durable stream. `None` on an in-memory one, and
    /// against a broker that did not negotiate offsets.
    pub offset: Option<u64>,
}
```

### Offsets are how you notice a drop

Subscriber queues shed under the default policy rather than blocking the
publisher, so a subscriber can silently miss records. Offsets are contiguous,
so **a jump between consecutive events is exactly a drop**:

```rust
let mut expected: Option<u64> = None;
while let Some(event) = subscription.next_event().await? {
    if let (Some(want), Some(got)) = (expected, event.offset)
        && got != want
    {
        tracing::warn!(dropped = got - want, "subscriber queue overflowed");
    }
    expected = event.offset.map(|offset| offset + 1);
    handle(&event.payload);
}
```

Worth writing even if you never resume from offsets. It is the only signal the
queue overflowed.

### A consumer that survives a restart

Checkpoint what you handled and resume at the next one. `start` is the first
record you have **not** seen, so a resuming consumer passes `offset + 1`.

```rust
use felix_client::{SubscribeCursorError, StartPosition};

let mut start = match checkpoint.load()? {
    Some(offset) => StartPosition::Offset(offset + 1),
    None => StartPosition::Earliest,
};

loop {
    let mut subscription = cluster
        .subscribe_from("acme", "prod", "events", Some(start))
        .await?;

    loop {
        match subscription.next_event().await {
            Ok(Some(event)) => {
                handle(&event.payload).await?;
                if let Some(offset) = event.offset {
                    checkpoint.save(offset)?;
                    start = StartPosition::Offset(offset + 1);
                }
            }
            Ok(None) => break,                     // ended, not moved; resubscribe
            Err(err) => {
                if let Some(cursor) = err.downcast_ref::<SubscribeCursorError>() {
                    // Retention discarded it. `available` is the nearest offset
                    // that would have worked, so this takes the smallest gap
                    // rather than restarting at the beginning -- and says so,
                    // because a silent restart at the tail loses records with
                    // nothing reported.
                    tracing::error!(
                        requested = cursor.requested,
                        resuming_at = cursor.available,
                        "checkpoint is past retention",
                    );
                    start = StartPosition::Offset(cursor.available);
                    break;
                }
                return Err(err);
            }
        }
    }
}
```

`next_event` is **cancel-safe**: it awaits an `mpsc` receive, so racing it in a
`tokio::select!` consumes nothing when another branch wins. You can put a
timeout around it without losing a record.

### Where a subscription joined

On a durable stream, a subscribe with a start position tells you where it
joined:

```rust
let sub = client
    .subscribe_from("acme", "prod", "orders", Some(StartPosition::Offset(1_000)))
    .await?;
let first = sub.start_offset(); // Some(1000)
let live = sub.live_offset();   // the tail when you subscribed
```

Events below `live_offset()` are catch-up; events from it on are new, and none
are skipped between the two. From `Latest` the two offsets are equal. Both are
`None` for a plain tail subscribe, an in-memory stream, or an older broker.

### When a shard moves

A rebalance or a drain can move a stream's shard to another broker. The old
owner delivers everything it committed, then ends the subscription with a
`shard_moved` frame saying where the shard went and where to resume.

`ClusterClient::subscribe` and `subscribe_from` return a `ClusterSubscription`
that follows the shard on its own: `next_event` resubscribes on the new owner
and carries on. On a durable stream it resumes at
`max(last delivered offset + 1, resume_from)`, so nothing is repeated or
skipped; an in-memory stream resumes at the new owner's tail. `moves()` counts
how often it followed, and `client()` is the connection it is on now.

A `Client` subscription ends instead, and says why:

```rust
while let Some(event) = subscription.next_event().await? {
    handle(&event);
}
if let Some(moved) = subscription.shard_moved() {
    // Resubscribe at the larger of `moved.resume_from` and your last offset
    // + 1, on `moved.addr` if it is set, or via any broker, which redirects.
}
```

See [Multi-node client](https://github.com/gabloe/felix/blob/main/docs/multi-node-client.md#when-a-shard-moves).

### Multiple Subscriptions

Handle multiple streams concurrently:

```rust
use tokio::select;

let mut sub1 = client.subscribe("acme", "prod", "orders").await?;
let mut sub2 = client.subscribe("acme", "prod", "inventory").await?;
let mut sub3 = client.subscribe("acme", "staging", "logs").await?;

loop {
    select! {
        event = sub1.next_event() => {
            if let Some(event) = event? {
                handle_order(event).await?;
            } else {
                break;
            }
        }
        event = sub2.next_event() => {
            if let Some(event) = event? {
                handle_inventory(event).await?;
            } else {
                break;
            }
        }
        event = sub3.next_event() => {
            if let Some(event) = event? {
                handle_log(event).await?;
            } else {
                break;
            }
        }
    }
}
```

### Async Event Processing

Avoid blocking the subscription loop:

```rust
// Bad: blocks subscription loop
while let Some(event) = subscription.next_event().await? {
    expensive_processing(event).await?;  // Blocks next event
}

// Good: spawn task for processing
while let Some(event) = subscription.next_event().await? {
    tokio::spawn(async move {
        expensive_processing(event).await.ok();
    });
}

// Better: use bounded channel for backpressure
let (tx, mut rx) = mpsc::channel(100);

tokio::spawn(async move {
    while let Some(event) = rx.recv().await {
        expensive_processing(event).await.ok();
    }
});

while let Some(event) = subscription.next_event().await? {
    tx.send(event).await.ok();
}
```

### Subscription Lifecycle

```rust
// Subscribe
let mut sub = client.subscribe("acme", "prod", "events").await?;

// Process events
for _ in 0..100 {
    if let Some(event) = sub.next_event().await? {
        process(event);
    }
}

// Drop subscription to close
drop(sub);
```

## Cache Operations

### Put and Get

```rust
// Put with TTL
client.cache_put(
    "acme",               // tenant
    "prod",               // namespace
    "sessions",           // cache
    "user-abc",           // key
    session_data,         // value (Bytes)
    Some(3600_000)        // 1 hour TTL
).await?;

// Get
match client.cache_get("acme", "prod", "sessions", "user-abc").await? {
    Some(data) => {
        let session: Session = deserialize(&data)?;
        // Use session
    }
    None => {
        // Session expired or doesn't exist
        return Err("Invalid session");
    }
}
```

### Without TTL

```rust
// Store permanently (until evicted or restart)
client
    .cache_put("acme", "prod", "config", "app-settings", config_data, None)
    .await?;
```

### Delete

```rust
// Answers with the value that was removed, or `None` if the key was not there —
// so a caller can tell a delete that did something from one that did not.
match client.cache_delete("acme", "prod", "sessions", "user-abc").await? {
    Some(removed) => audit_log("session revoked", removed),
    None => { /* already gone, or never there */ }
}
```

Needs a broker advertising `FEATURE_CACHE_DELETE`; the client returns an error
rather than probing, because an unrecognised message type ends the broker's
control loop.

### Watch

Subscribe to changes for one key or key prefix. Each change carries its
cache-log offset, so a watch can be resumed exactly where it left off:

```rust
use felix_client::{CacheWatchFilter, CacheWatchItem};

let mut watch = client
    .watch_cache(
        "acme",
        "prod",
        "sessions",
        CacheWatchFilter::Prefix("user:".into()),
        None,          // from now; Some(offset) resumes gaplessly
    )
    .await?;

let mut checkpoint = watch.resume_offset();
while let Some(item) = watch.recv().await {
    match item {
        CacheWatchItem::Change(change) => {
            match &change.value {
                Some(value) => apply_update(&change.key, value),
                None => remove(&change.key),   // a delete
            }
            checkpoint = change.offset + 1;
        }
        CacheWatchItem::Lagged { resume_from } => {
            // The watch fell behind and was ended; re-watch from
            // `resume_from` to replay everything missed.
            checkpoint = resume_from;
            break;
        }
        CacheWatchItem::ShardMoved(moved) => {
            // The shard moved to another broker, which ended this `Client`
            // watch. Re-watch there; without a `resume_from`, `checkpoint` is
            // right. A `ClusterClient` watch follows on its own instead.
            checkpoint = moved.resume_from.unwrap_or(checkpoint);
            break;
        }
    }
}
```

A resume whose history compaction has collapsed begins with each matching
key's current value instead, and `watch.resnapshot()` says so. Needs a broker
advertising `FEATURE_CACHE_WATCH` — only brokers whose cache is log-backed do.
`ClusterClient::watch_cache` returns a `ClusterCacheWatch`, which follows a
moved shard itself: `ShardMoved` arrives as a notice and the changes carry on
from the new owner, none repeated or skipped. A prefix watch reads one shard; on a multi-shard cache use
`ClusterClient::watch_cache_sharded` (see [Clusters](#clusters)).
See [Cache Features](/felix/features/cache/#7-keyed-watch) for the full
contract.

### Retained Watch

Start from current state instead of from now: each matching key's current
value first, then live changes — the join primitive for presence and state
sync:

```rust
let mut watch = client
    .watch_cache_retained(
        "acme",
        "prod",
        "presence",
        CacheWatchFilter::Prefix("room:7:".into()),
    )
    .await?;

// The state phase is exactly this many changes; 0 means empty, definitively.
let state_size = watch.retained_count().expect("retained watches report a count");
```

Needs `FEATURE_CACHE_WATCH_RETAINED`, a separate bit so an older watch-capable
broker is never asked for state it would silently not deliver. Mutually
exclusive with `from_offset` — a resume already replays what a retained start
shortcuts.

### Counters

```rust
// Apply a delta and learn the sum including it, in one round trip.
let after = client.counter_add("acme", "prod", "limits", "user:42:reqs", 1).await?;

// Read; None means never written — distinct from a sum of zero.
let sum = client.counter_get("acme", "prod", "metrics", "page:home").await?;
```

Scoped and routed like cache keys, stored beside the cache; durable and
replicated with the shard. At-least-once: a retry after a lost ack counts
twice. Needs a broker advertising `FEATURE_COUNTERS` (durable brokers only).

### Concurrent Cache Operations

Pipeline multiple cache operations:

```rust
use futures::future::join_all;

// Issue multiple requests concurrently
let futures = (0..10).map(|i| {
    let key = format!("key-{}", i);
    client.cache_get("acme", "prod", "data", &key)
});

let results = join_all(futures).await;

for result in results {
    if let Ok(Some(value)) = result {
        process(value);
    }
}
```

### Cache Namespacing

Cache keys are scoped to prevent collisions:

```rust
// These are independent entries
client
    .cache_put("acme", "prod", "sessions", "user-123", data1, ttl)
    .await?;
client
    .cache_put("acme", "prod", "profiles", "user-123", data2, ttl)
    .await?;
client
    .cache_put("acme", "prod", "temp", "user-123", data3, ttl)
    .await?;
```

## Consumer Groups

The other way to read a stream. `subscribe` pushes every record to every
subscriber; a **consumer group** hands each record to one consumer and takes it
back if nobody says it was handled.

Records are **pulled**, because only the consumer knows when it has capacity:

```rust
loop {
    // Waits up to five seconds for work rather than spinning on empty polls.
    let records = client
        .group_poll_wait("acme", "prod", "jobs", 0, "fulfilment", 32, Duration::from_secs(5))
        .await?;

    for record in records {
        // `attempts` is 1 on a first delivery and higher on a redelivery, so a
        // consumer can treat a retry differently. Absent means the broker did
        // not report it, which is not the same as a first attempt.
        match handle(&record.payload, record.attempts) {
            Ok(()) => client.group_ack("acme", "prod", "jobs", 0, "fulfilment", record.offset).await?,
            // Hand it back for immediate redelivery rather than waiting out the
            // visibility timeout.
            Err(_) => client.group_nack("acme", "prod", "jobs", 0, "fulfilment", record.offset).await?,
        }
    }
}
```

An empty batch means nothing was available. **It is an answer, not an error.**

### Dead letters

Past `FELIX_GROUP_MAX_ATTEMPTS` a record is dead-lettered, so one poison record
cannot stall the queue behind it. These need `FEATURE_GROUP_DEAD_LETTERS`, a
separate bit from `FEATURE_CONSUMER_GROUP`:

```rust
let offsets = client.group_dead_letters("acme", "prod", "jobs", 0, "fulfilment").await?;
for offset in offsets {
    if worth_retrying(offset) {
        client.group_redrive("acme", "prod", "jobs", 0, "fulfilment", offset).await?;
    } else {
        client.group_discard("acme", "prod", "jobs", 0, "fulfilment", offset).await?;
    }
}
```

A dead letter is a **pointer, not a copy**: the record is still in the stream's
log at that offset, readable by an ordinary replay.

### What a group needs

- **Durable storage on the broker.** A group's position lives in a log, so a
  broker without `FELIX_DURABLE_STORAGE_DIR` serves no groups and does not
  advertise `FEATURE_CONSUMER_GROUP`.
- **The shard's leader.** A poll is refused rather than forwarded, because
  relaying would put the claim and the acknowledgement on different brokers.
  Another broker answers with `NotLeaderError`, and so does the old leader once
  a shard move cuts over. `Client` hands that back; the same calls on
  `ClusterClient` (`group_poll`, `group_ack`, ...) follow it to the leader.
- **Idempotent handling.** This is at-least-once: a crash after handling and
  before acknowledging is indistinguishable from a crash before handling, so the
  record comes back.

## Clusters

`Client` talks to one broker. `ClusterClient` follows the cluster — it takes
several addresses, learns the rest, reconnects when the broker it is using goes
away, and follows a redirect to whichever broker owns a shard.

A broker that is killed or cut off sends nothing to say so; the client notices
when the QUIC connection has been silent for the idle timeout (6 s by default,
`FELIX_MAX_IDLE_TIMEOUT_MS`). A publish waiting on that connection fails then
and is retried on another broker, so a leader failover costs the client about
that long. Keep-alives every 2 s (`FELIX_KEEPALIVE_MS`) keep a quiet but healthy
connection open. The 30 s ack timeout is only a backstop for a broker that is
alive and never answers.

```rust
let client = Arc::new(ClusterClient::connect(&seeds, "localhost", config).await?);

// Every shard of a multi-shard stream, merged into one channel.
let mut subscription = client
    .subscribe_sharded("acme", "prod", "orders", Some(StartPosition::Earliest))
    .await?;

while let Some(item) = subscription.next().await {
    match item {
        ShardEvent::Record { shard, event } => handle(shard, event),
        ShardEvent::ShardLost { shard, error } => warn!(shard, %error, "shard down"),
        ShardEvent::ShardRecovered { shard } => info!(shard, "shard back"),
        // Followed to its new owner; records carry on from there.
        ShardEvent::ShardMoved { shard, .. } => info!(shard, "shard moved"),
        _ => {}                            // `ShardEvent` is non-exhaustive
    }
}
```

This needs a broker advertising `FEATURE_STREAM_SHARDS`, because the shard count
comes from asking one, and `FEATURE_REDIRECT` to follow each shard to its owner.

**Ordering is per shard and nothing more** — merging cannot restore an order
that never existed. Resumption is a vector: `positions()` returns one offset per
shard, and `resubscribe_sharded` takes it back. See
[Multi-node client](https://github.com/gabloe/felix/blob/main/docs/multi-node-client.md)
for the full contract.

Prefix watches on a multi-shard cache work the same way. `watch_cache_sharded`
opens one watch per shard and merges them. The retained version sends
`ShardedCacheWatchItem::StateComplete` once every shard's current values have
arrived. A shard that moves is followed to its new owner and reported as
`ShardedCacheWatchItem::ShardMoved`; its changes carry on from where the old
owner left off. Needs `FEATURE_CACHE_SHARDS`.

## Connection Management

### Automatic Reconnection

Clients should implement reconnection logic:

```rust
use std::net::SocketAddr;

async fn connect_with_retry(
    addr: SocketAddr,
    server_name: &str,
    config: ClientConfig,
    max_retries: u32,
) -> Result<Client> {
    for attempt in 0..max_retries {
        match Client::connect(addr, server_name, config.clone()).await {
            Ok(client) => return Ok(client),
            Err(e) if attempt < max_retries - 1 => {
                let delay = Duration::from_millis(100 * 2u64.pow(attempt));
                eprintln!("Connection failed, retrying in {:?}: {}", delay, e);
                tokio::time::sleep(delay).await;
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!()
}
```

### Health Monitoring

Check connection health:

```rust
async fn monitor_connection(client: &Client) -> Result<()> {
    loop {
        match client.health_check().await {
            Ok(()) => {
                // Connection healthy
            }
            Err(e) => {
                eprintln!("Health check failed: {:?}", e);
                // Implement reconnection
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
```

## Telemetry

### Enabling Telemetry

Compile with telemetry feature:

```toml
[dependencies]
felix-client = { version = "0.5", features = ["telemetry"] }
```

### Collecting Metrics

```rust
use felix_client::{frame_counters_snapshot, reset_frame_counters};

// Get current frame counters
let counters = frame_counters_snapshot();
println!("Frames out: {}", counters.frames_out_ok);
println!("Frames in: {}", counters.frames_in_ok);
println!("Publish batches acked: {}", counters.pub_batches_out_ok);
println!("Events received: {}", counters.sub_items_in_ok);

// Reset counters
reset_frame_counters();
```

### Timing Measurements

```rust
use felix_client::timings;

// Sample one operation in every 100, then drain what was recorded. Each
// field of the returned tuple is one stage's samples in nanoseconds; the
// order is documented on `timings::ClientTimingSamples`.
timings::enable_collection(100);
// ... run the workload ...
if let Some(samples) = timings::take_samples() {
    let e2e_latency_ns = &samples.17;
    println!("end-to-end samples: {}", e2e_latency_ns.len());
}
```

:::caution[Telemetry Overhead]
Telemetry adds measurable overhead (5-15% in high-throughput workloads). Use only for debugging and profiling, not in production hot paths unless necessary.
:::
## Patterns

### Connection Pooling

```rust
// Good: reuse client across application
use felix_wire::AckMode;
use std::net::SocketAddr;

lazy_static! {
    static ref FELIX_CLIENT: Client = {
        let quinn = quinn::ClientConfig::with_platform_verifier();
        let config = ClientConfig::optimized_defaults(quinn);
        let addr: SocketAddr = "127.0.0.1:5000".parse().unwrap();
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(Client::connect(addr, "localhost", config))
            .unwrap()
    };
}

// Use shared client
let publisher = FELIX_CLIENT.publisher().await?;
publisher
    .publish("acme", "prod", "events", data.to_vec(), AckMode::None)
    .await?;
```

### Error Recovery

```rust
async fn publish_with_retry(
    client: &Client,
    tenant: &str,
    namespace: &str,
    stream: &str,
    data: &[u8],
    max_retries: u32
) -> Result<()> {
    use felix_wire::AckMode;
    let publisher = client.publisher().await?;
    for attempt in 0..max_retries {
        match publisher
            .publish(tenant, namespace, stream, data.to_vec(), AckMode::PerMessage)
            .await
        {
            Ok(()) => return Ok(()),
            Err(e) if is_retriable(&e) && attempt < max_retries - 1 => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!()
}

// Client calls return `anyhow::Error`. What counts as transient is the
// application's call; inspect the error chain to decide.
fn is_retriable(_error: &anyhow::Error) -> bool {
    true
}
```

### Resource Cleanup

```rust
// Subscriptions are cleaned up on drop
{
    let mut sub = client.subscribe("acme", "prod", "events").await?;
    // Process events...
}  // Automatic close on drop
```

### Batching for Throughput

```rust
use felix_wire::AckMode;
use tokio::time::{interval, Duration};

async fn batching_publisher(client: &Client) -> Result<()> {
    let publisher = client.publisher().await?;
    let mut batch = Vec::new();
    let mut ticker = interval(Duration::from_millis(10));
    
    loop {
        select! {
            _ = ticker.tick() => {
                if !batch.is_empty() {
                    publisher
                        .publish_batch("acme", "prod", "events", batch.clone(), AckMode::PerBatch)
                        .await?;
                    batch.clear();
                }
            }
            msg = receive_message() => {
                batch.push(msg);
                if batch.len() >= 64 {
                    publisher
                        .publish_batch("acme", "prod", "events", batch.clone(), AckMode::PerBatch)
                        .await?;
                    batch.clear();
                }
            }
        }
    }
}
```

## Testing

### Integration Tests

```rust
#[tokio::test]
async fn test_cache_ttl() {
    use std::net::SocketAddr;

    let quinn = quinn::ClientConfig::with_platform_verifier();
    let config = ClientConfig::optimized_defaults(quinn);
    let addr: SocketAddr = "127.0.0.1:5000".parse().unwrap();
    let client = Client::connect(addr, "localhost", config).await.unwrap();
    
    // Store with 100ms TTL
    use bytes::Bytes;
    client
        .cache_put("test", "default", "cache", "key", Bytes::from_static(b"value"), Some(100))
        .await
        .unwrap();
    
    // Immediately readable
    assert_eq!(
        client
            .cache_get("test", "default", "cache", "key")
            .await
            .unwrap(),
        Some(b"value".to_vec())
    );
    
    // Wait for expiration
    tokio::time::sleep(Duration::from_millis(150)).await;
    
    // Should be expired
    assert_eq!(
        client
            .cache_get("test", "default", "cache", "key")
            .await
            .unwrap(),
        None
    );
}
```

## Performance in one paragraph

Reuse one client (its pools are the expensive part), batch publishes when
latency permits, pipeline cache requests, and keep the subscription loop
non-blocking — spawn slow work instead of stalling the reader. Everything
else is a knob to turn off a measurement; see
[Benchmarks](/felix/features/benchmarks/).

## API Reference Summary

| Operation | Method | Use Case |
|-----------|--------|----------|
| Single publish | `Publisher::publish()` | Low-rate events |
| Batch publish | `Publisher::publish_batch()` | High-throughput |
| Subscribe | `subscribe()` | Event consumption |
| Cache put | `cache_put()` | Store with TTL |
| Cache get | `cache_get()` | Retrieve value |
| Publisher | `Client::publisher()` | Streaming publish |

For complete API documentation, see the [rustdoc](https://docs.rs/felix-client).
