---
title: "Rust: Clusters and Operations"
description: "Connecting to a cluster, connection management, telemetry, patterns, and testing."
---

Part of the [Rust client guide](/clients/rust/). Setup and configuration are on that page.

## Clusters

`Client` follows the cluster: it takes one or more addresses, learns the
rest, reconnects when the broker it is using goes away, and follows a redirect
to whichever broker owns a shard. `Connection` talks to one broker; it is the
single-connection building block, and applications should use `Client`
(see [Which client to use](/clients/rust/#which-client-to-use)).

It holds one connection per broker, shared by every role that broker plays
(entry, shard owner, redirect target, producer leader), with every stream
multiplexed on it. A second connection opens only when the first is saturated
(`cluster_streams_per_conn` streams, or the broker's QUIC stream credit), up to
`cluster_conn_pool`. A connection that dies fails only its own streams and is
replaced when a stream next needs the room; `connections_per_node()` reports
the count per broker.

A broker that is killed or cut off sends nothing to say so; the client notices
when the QUIC connection has been silent for the idle timeout (6 s by default,
`FELIX_MAX_IDLE_TIMEOUT_MS`). A publish waiting on that connection fails then
and is retried on another broker, so a leader failover costs the client about
that long. Keep-alives every 2 s (`FELIX_KEEPALIVE_MS`) keep a quiet but healthy
connection open. The 30 s ack timeout is only a backstop for a broker that is
alive and never answers.

```rust
use felix_client::{ShardEvent, StartPosition};
use tracing::{info, warn};

let client = Arc::new(Client::connect(&seeds, "localhost", config).await?);

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
        // The stream is gone; this shard is not reopened.
        ShardEvent::StreamDeleted { shard } => info!(shard, "stream deleted"),
        _ => {}                            // `ShardEvent` is non-exhaustive
    }
}
```

This needs a broker advertising `FEATURE_STREAM_SHARDS`, because the shard count
comes from asking one, and `FEATURE_REDIRECT` to follow each shard to its owner.
A shard whose owner is still opening it (`not_ready`, as a promoted leader
answers while it fences its replicas) is asked again with the `ReconnectPolicy`
backoff; the call fails only if some shard is still refused after the last attempt.

**Ordering is per shard only.** Merging cannot restore an order that never
existed. Resumption is a vector: `positions()` returns one offset per
shard, and `resubscribe_sharded` takes it back. See
[Multi-node client](https://github.com/GetFelix/felix/blob/main/docs/multi-node-client.md)
for the full contract.

`Connection::shard_owners(tenant, namespace, name, ShardKind::Stream)` (or
`ShardKind::Cache`) says which broker owns each shard: one `ShardOwner` per
shard with its node id, client address and generation, from the answering
broker's routing snapshot. It needs `FEATURE_SHARD_OWNERS`.

Cache and counter calls (`cache_put`, `cache_get`, `cache_delete`,
`counter_add`, `counter_get`) are on `Client` too. Each goes to the owner
of the key's shard, asked once per cache with `shard_owners`, and otherwise
through the broker in use, which forwards. A write that may have been applied
is not sent twice, as for `publish`; a failed read is asked again under the
`ReconnectPolicy`, and a write only after a refusal that applied nothing.

An `AckMode::None` publish returns once it is queued. Call
`Client::finish()` before exiting: it waits until everything queued on
every broker the client holds has been written, then closes the publish
streams, so publishing through that client afterwards fails.
`Publisher::finish()` does the same for one `Connection`.

Prefix watches on a multi-shard cache work the same way. `watch_cache_sharded`
opens one watch per shard and merges them. The retained version sends
`ShardedCacheWatchItem::StateComplete` once every shard's current values have
arrived. A shard that moves is followed to its new owner and reported as
`ShardedCacheWatchItem::ShardMoved`; its changes carry on from where the old
owner left off. A lost connection is followed the same way, without a notice,
and a move or loss inside the state phase restarts that shard's state phase,
so `StateComplete` waits for its new state. A shard that cannot be followed
ends with `ShardClosed`, whose `error` says why. Needs `FEATURE_CACHE_SHARDS`. The [Python](/clients/python/#a-prefix-across-every-shard)
and [TypeScript](/clients/typescript/#a-prefix-across-every-shard) bindings wrap it, with the same items.

## Connection Management

`Client` handles reconnection: it retries on another broker with the backoff in
`ReconnectPolicy`, which `connect_with_policy` lets you tune. `Connection` does
not reconnect: when its connection drops, calls fail and the application
decides what to do.

```rust
use std::time::Duration;

use felix_client::{Client, ReconnectPolicy};

let policy = ReconnectPolicy {
    attempts: 10,
    max_backoff: Duration::from_secs(5),
    ..ReconnectPolicy::default()
};
let client = Client::connect_with_policy(&seeds, "localhost", config, policy).await?;
```

When every address the client holds has stopped answering, it resolves names
again and dials what is new: seeds given as `host:port` names, and the names
brokers advertised. That is what gets it back in after a rollout gives every
broker a new IP. `connect_with_policy` takes `SocketAddr`s, which have no name
to resolve; `connect_seeds` takes `Seed`s, which can be either:

```rust
use felix_client::{Client, ReconnectPolicy, Seed};

let seeds: Vec<Seed> = vec!["felix.prod.svc.cluster.local:5000".parse()?];
let client =
    Client::connect_seeds(&seeds, "localhost", config, ReconnectPolicy::default()).await?;
```

Names are resolved at most once a second for a client and its identities,
all at once, each bounded to 2 s, and without holding the lock operations
read the client through. A name has at most one system lookup running, so one
that hangs does not stop the others resolving. See `docs/multi-node-client.md`.

### Reconnecting

Publishes, opening and following subscriptions, commits, `publish_if`, state reads, cache calls and consumer-group calls retry under these options. A read, and a group describe or dead-letter listing, retries on any error that is not terminal. A write, including a group poll, which claims records, retries only on a broker's retryable refusal, which says nothing was applied, so a write that may have applied is never sent twice. The deadline covers re-routes and, for a call over every shard of a stream, all its shards together. Four options tune the retrying, and all default to the Rust client's own policy, so a client that sets none of them behaves as it always did.

- **Attempts** is how many times a call tries to reach a broker before it gives up. Each attempt tries every broker the client knows. The default is 5, and the minimum is 1.
- **Deadline** is a ceiling on one call across every attempt and every wait between them. It is off by default, and it is checked between attempts, before each backoff sleep, so a call can last the deadline plus one attempt's own timeout. Attempt counts alone do not bound time: five attempts against a broker that takes its full publish timeout to answer is minutes.
- **Backoff** is the ceiling of the first wait between attempts. It doubles each attempt up to the max backoff, and each wait is a random fraction of its ceiling, so every client of a cluster does not retry in step. The default is 200 ms.
- **Max backoff** is the longest any one wait can be. The default is 2 s. Every wait is capped at it, even when the backoff is larger.

`Client::connect_with_policy` takes them as the `attempts`, `deadline`, `backoff` and `max_backoff` fields of a `ReconnectPolicy`.

```rust
let policy = ReconnectPolicy {
    attempts: 10,
    deadline: Some(Duration::from_secs(30)),
    ..ReconnectPolicy::default()
};
let client = Client::connect_with_policy(&seeds, "localhost", config, policy).await?;

// Stops retrying once 30 s have passed, checked between attempts, and reports the last error.
client
    .publish_at_least_once("t1", "default", "orders", b"hello".to_vec(), AckMode::PerMessage)
    .await?;
```

A deadline shorter than one attempt's own timeout means no retry happens: the first attempt spends the whole budget and the call returns after trying once. Set it to cover at least one attempt plus the backoff you can afford, and leave it off if you would rather wait out a failover. A client from `with_identity` inherits the policy. [Building Resilient Clients](/clients/resilient-clients/) shows reconnection, token refresh, retries, resuming and shutdown side by side in every language.

## Telemetry

### Enabling Telemetry

Compile with the telemetry feature:

```bash
cargo add felix-client --features telemetry
```

### Collecting Metrics

```rust
use felix_client::{frame_counters_snapshot, reset_frame_counters};

// Get current frame counters
let counters = frame_counters_snapshot();
println!("Frames out: {}", counters.frames_out_ok);
println!("Frames in: {}", counters.frames_in_ok);
println!("Publish batches sent: {}", counters.pub_batches_out_ok);
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

Connect once and share the client: it is cheap to clone an `Arc` of it into
every task, and its pools are the expensive part. Connect inside the
application's own runtime: the client's tasks live on the runtime that created
it, so a client built on a throwaway runtime dies with that runtime.

```rust
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use felix_client::{AckMode, ClientConfig, Client, quic_client_config};
use tokio::task::JoinSet;

#[tokio::main]
async fn main() -> Result<()> {
    let config = ClientConfig::from_env_or_yaml(quic_client_config(None, false)?, None)?;
    let seeds: Vec<SocketAddr> = vec!["127.0.0.1:5000".parse()?];
    let client = Arc::new(Client::connect(&seeds, "localhost", config).await?);

    let mut workers = JoinSet::new();
    for worker in 0..4 {
        let client = Arc::clone(&client);
        workers.spawn(async move {
            let payload = format!("hello from worker {worker}").into_bytes();
            client
                .publish("acme", "prod", "events", payload, AckMode::None)
                .await
        });
    }
    while let Some(published) = workers.join_next().await {
        published??;
    }

    // Before exiting: wait for everything queued with `AckMode::None`.
    client.finish().await?;
    Ok(())
}
```

### Error Recovery

`Client` already retries what it safely can (see [Errors you can act
on](/clients/rust/publishing/#errors-you-can-act-on)). A caller that wants more attempts on top resends
only what the broker says was not applied:

```rust
use std::time::Duration;

use anyhow::Result;
use felix_client::{AckMode, BrokerError, Client, RetryClass};

/// Publish to `stream`, resending only when the broker applied nothing, with
/// a doubling pause between at most `attempts` tries.
async fn publish_with_retry(
    client: &Client,
    stream: &str,
    payload: &[u8],
    attempts: u32,
) -> Result<Option<u64>> {
    let mut pause = Duration::from_millis(100);
    let mut attempt = 1;
    loop {
        let result = client
            .publish("acme", "prod", stream, payload.to_vec(), AckMode::PerMessage)
            .await;
        match result {
            Err(err) if attempt < attempts && safe_to_resend(&err) => {
                tokio::time::sleep(pause).await;
                pause = (pause * 2).min(Duration::from_secs(5));
                attempt += 1;
            }
            result => return result,
        }
    }
}

// `OutcomeUnknown` means the publish may have been applied, so a plain resend
// could write it twice. Use an `IdempotentProducer` when that must not happen.
fn safe_to_resend(error: &anyhow::Error) -> bool {
    matches!(
        error.downcast_ref::<BrokerError>().map(|refusal| refusal.retry),
        Some(RetryClass::Retry | RetryClass::RetryAfter)
    )
}
```

### Resource Cleanup

Dropping a subscription, a watch or a client closes it. The one thing a drop
cannot do is wait, so a client that published with `AckMode::None` calls
`finish` before it goes:

```rust
// Closes the subscription on the broker.
drop(subscription);

// Waits until everything queued with `AckMode::None` is written, then closes
// the publish streams.
client.finish().await?;
```

### Batching for Throughput

```rust
use anyhow::Result;
use felix_client::{AckMode, Client};
use tokio::sync::mpsc;
use tokio::time::{Duration, interval};

const MAX_BATCH: usize = 64;

/// Publish what arrives on `messages` in batches of up to `MAX_BATCH`,
/// sending a partial batch every 10 ms, until every sender is gone.
async fn batching_publisher(
    client: &Client,
    mut messages: mpsc::Receiver<Vec<u8>>,
) -> Result<()> {
    let mut batch = Vec::with_capacity(MAX_BATCH);
    let mut ticker = interval(Duration::from_millis(10));
    let mut open = true;
    while open {
        tokio::select! {
            _ = ticker.tick() => {}
            // `recv` is cancel-safe: a tick that wins loses no message.
            message = messages.recv() => match message {
                Some(message) => {
                    batch.push(message);
                    if batch.len() < MAX_BATCH {
                        continue;
                    }
                }
                None => open = false,
            },
        }
        if !batch.is_empty() {
            let full = std::mem::replace(&mut batch, Vec::with_capacity(MAX_BATCH));
            client
                .publish_batch("acme", "prod", "events", full, AckMode::PerBatch)
                .await?;
        }
    }
    Ok(())
}
```

## Testing

### Integration Tests

A test against a real broker is marked `#[ignore]`, so `cargo test` skips it
and `cargo test -- --ignored` runs it once a broker is up:

```rust
use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use felix_client::{ClientConfig, Client, quic_client_config};

#[tokio::test]
#[ignore = "needs a broker on 127.0.0.1:5000"]
async fn cache_entry_expires() -> Result<()> {
    let config = ClientConfig::from_env_or_yaml(quic_client_config(None, false)?, None)?;
    let seeds: Vec<SocketAddr> = vec!["127.0.0.1:5000".parse()?];
    let client = Client::connect(&seeds, "localhost", config).await?;

    client
        .cache_put("test", "default", "cache", "key", Bytes::from_static(b"value"), Some(100))
        .await?;
    let stored = client.cache_get("test", "default", "cache", "key").await?;
    assert_eq!(stored, Some(Bytes::from_static(b"value")));

    tokio::time::sleep(Duration::from_millis(150)).await;
    let expired = client.cache_get("test", "default", "cache", "key").await?;
    assert_eq!(expired, None);
    Ok(())
}
```

## Performance

Reuse one client (its pools are the expensive part), batch publishes when
latency permits, pipeline cache requests, and keep the subscription loop
non-blocking by spawning slow work instead of stalling the reader. Tune
anything else from a measurement; see
[Benchmarks](/features/benchmarks/).
