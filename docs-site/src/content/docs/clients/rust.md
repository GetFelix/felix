---
title: "Rust Client SDK"
---

`felix-client` is the Rust SDK: publish, subscribe, cache, consumer groups,
and the cluster client, over multiplexed QUIC connections. This page covers
setup, configuration, and common patterns.

The Python and TypeScript clients and the C ABI bind to it, and the C# SDK binds the C ABI. See [Choosing a Client](/clients/overview/) for those bindings
and for how a new language is gated on a conformance suite.

## Installation

```bash
cargo add felix-client
```

With the optional telemetry feature:

```bash
cargo add felix-client --features telemetry
```

**Features**:

- `telemetry`: per-operation timing and frame counters (adds overhead)

## Quick Start

### Basic Publish/Subscribe

```rust
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use felix_client::{AckMode, ClientConfig, ClusterClient, quic_client_config};

#[tokio::main]
async fn main() -> Result<()> {
    // FELIX_AUTH_TENANT and FELIX_AUTH_TOKEN supply the tenant and the token,
    // so neither is written into the program.
    let config = ClientConfig::from_env_or_yaml(quic_client_config(None, false)?, None)?;

    // One broker is enough. The rest of a cluster is discovered from it.
    let seeds: Vec<SocketAddr> = vec!["127.0.0.1:5000".parse()?];
    let client = Arc::new(ClusterClient::connect(&seeds, "localhost", config).await?);

    // Subscribe before publishing: a subscription starts at the stream's tail.
    let mut subscription = client.subscribe("acme", "prod", "events").await?;

    // tenant, namespace, stream, payload, and when the broker answers.
    client
        .publish("acme", "prod", "events", b"Hello Felix".to_vec(), AckMode::PerMessage)
        .await?;

    let event = tokio::time::timeout(Duration::from_secs(5), subscription.next_event())
        .await
        .context("no event within 5 seconds")??
        .context("the broker ended the subscription")?;
    println!("received {:?}", event.payload);
    Ok(())
}
```

`subscribe` takes `&Arc<ClusterClient>`, because a subscription keeps a handle
on the client to follow its shard. The examples below that subscribe assume an
`Arc<ClusterClient>`.

### Basic Cache Operations

```rust
use bytes::Bytes;

// Store a value that expires after 60 seconds (the TTL is in milliseconds).
let value = Bytes::from_static(b"session-data");
client
    .cache_put("acme", "prod", "sessions", "user-123", value, Some(60_000))
    .await?;

match client.cache_get("acme", "prod", "sessions", "user-123").await? {
    Some(value) => println!("found {value:?}"),
    None => println!("not found or expired"),
}
```

### Which client to use

Use `ClusterClient`, whether you run one broker or a cluster. It sends each
record straight to the broker that leads its shard, reconnects when a broker
goes away, and follows shards that move. A single broker needs no cluster
mode for it, and the client asks nothing of the control plane: discovery finds
nothing to add, and that broker owns every shard.

`Client` is the single-connection building block underneath it, for tools and
special cases. It talks to one broker. In a cluster, a publish that reaches a
broker that does not lead the shard is forwarded to the one that does, and
that hop is expensive. In the v0.6.0 three-node, three-replica run, sending
through `Client` instead of straight to the owner cut in-memory throughput
(256 B records, batches of 64) 2.4×, from 2,034 to 864 MB/s of payload with
34% more broker CPU, and durable quorum-acked throughput 4.4×, from 475 to 107
MB/s. See [v0.6.0 performance](/features/v060-performance/).

### Getting and refreshing a token

A token lasts 15 minutes by default, and the broker checks it each time a
stream authenticates, including the streams a reconnect opens. [Getting a
token](/clients/overview/#getting-a-token) covers where one comes from. A
process that outlives one gives `ClientConfig` a `token_provider`, and the
client asks it for a token when it needs one. It asks lazily: when no token is
held, when the held one has passed two thirds of its life, or when a broker has
just refused it (one refetch per refused stream, retried only if the provider
returns a different token, then the operation fails with the auth error). If a
fetch fails while the held token is still valid, the client keeps using that
one, logs a warning and asks again about five seconds later.

`auth_token` is a fixed token, which suits a short job. The
`FELIX_AUTH_TENANT` and `FELIX_AUTH_TOKEN` variables fill `auth_tenant_id` and
`auth_token` when you build the config with `ClientConfig::from_env_or_yaml`.

The provider is an `Arc<dyn TokenProvider>`, usually a `RefreshingToken`
built from an async closure `Fn() -> impl Future<Output = anyhow::Result<String>>`:
`RefreshingToken::with_initial(token, fetch)` starts from a token you already
hold, and `RefreshingToken::new(fetch)` starts with none, so the first connect
calls `fetch`. Setting `token_provider` overrides `auth_token`. Two thirds of a
token's life is read from its `exp` claim; a token without one is kept until a
broker refuses it.

**When it is called.** When a stream is about to authenticate and the client
holds no token, the held one is due for replacement, or a broker just refused
it. An idle client makes no calls, and there is no timer. The client never runs
two calls at once: streams that open together wait on one and share its token.
That is also what makes it safe to rotate a refresh token inside the provider.
The first fetch at connect is cut off at 30 seconds and not counted in
`connect_timeout`; a refetch after a refusal happens in the handshake and does
count.

**Threading.** The closure is called on the task that needs the token, and the
future it returns runs as a task spawned on that task's Tokio runtime, which
for the client's own streams is the runtime it was connected on. So the closure
must be `Send + Sync + 'static`, its future `Send + 'static`, and neither may
block: use an async HTTP client. A `RefreshingToken` asked for a token outside
a Tokio runtime panics.

**Failure.** The provider fails when it returns an error, returns an empty or
blank token, or does not answer within 30 seconds. While the held token is still valid the client keeps it,
logs a warning and asks again about five seconds later. Otherwise the operation
that needed the token fails with `felix_client::AuthError` carrying the
provider's error message and its causes. A provider failure is final for that
operation: it is not retried, because each call can rotate a refresh token.
The next operation asks again. The 30 second timeout fails the operations
waiting on the call, but the call itself keeps running: the client never starts
a second one while it is outstanding, the next refresh joins it, and a late
answer is used. A broker's refusal of the credential is an `AuthError` too, so
recover either with `err.downcast_ref::<AuthError>()`, not by matching the
message.

**Cancellation.** `RefreshingToken::new_cancellable` and
`with_initial_cancellable` pass the closure a
`tokio_util::sync::CancellationToken`. It fires at the 30 second timeout and
when the `RefreshingToken` is dropped, which happens once the config and every
client built from it are gone. A provider that ignores it still
works: it is not called again while pending, and its late answer is used.
Cancelling ends nothing by itself; the future decides what to return. A
provider that rotates a refresh token is usually better off ignoring it: a
refresh the control plane answered but the provider abandoned loses the
replacement refresh token.

**Async variant.** There is no separate one: the provider is async by
construction, so it awaits its HTTP calls directly.

The client has no HTTP code of its own, so the exchange and refresh calls are
yours. This example uses `reqwest` (with its `json` feature) and `serde`:

```rust
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use felix_client::{ClientConfig, RefreshingToken};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::Mutex;

const CONTROLPLANE: &str = "https://controlplane:8443/v1/tenants/acme";

// No `Debug`, so the tokens cannot end up in a log line by accident.
#[derive(Deserialize)]
struct Minted {
    felix_token: String,
    refresh_token: String,
}

async fn exchange(http: &reqwest::Client, oidc_token: &str) -> Result<Minted> {
    let minted = http
        .post(format!("{CONTROLPLANE}/token/exchange"))
        .bearer_auth(oidc_token)
        .json(&json!({
            "requested": ["stream.publish", "stream.subscribe"],
            "resources": ["stream:acme/prod/*"],
        }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(minted)
}

async fn refresh(http: &reqwest::Client, refresh_token: &str) -> Result<Minted> {
    let minted = http
        .post(format!("{CONTROLPLANE}/token/refresh"))
        .json(&json!({ "refresh_token": refresh_token }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(minted)
}

async fn configure(config: &mut ClientConfig, oidc_token: &str) -> Result<()> {
    // Well inside the client's 30 second limit on one fetch.
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let first = exchange(&http, oidc_token).await?;
    // The client runs one fetch at a time, so this lock is never contended;
    // it only gives the `Fn` closure somewhere to keep the newest refresh token.
    let refresh_token = Arc::new(Mutex::new(first.refresh_token));

    let tokens = RefreshingToken::with_initial(first.felix_token, move || {
        let (http, refresh_token) = (http.clone(), Arc::clone(&refresh_token));
        async move {
            let mut refresh_token = refresh_token.lock().await;
            let minted = refresh(&http, &refresh_token).await?;
            *refresh_token = minted.refresh_token;
            Ok(minted.felix_token)
        }
    });
    config.auth_tenant_id = Some("acme".into());
    config.token_provider = Some(Arc::new(tokens));
    Ok(())
}
```

The refresh token works once: every refresh returns a replacement, so the
provider stores it before it returns. A 403 means the refresh token expired or
was revoked: run the exchange again (fresh OIDC login) and connect a new
client; the client keeps the last token until it expires.

A gateway gives each user's handle a provider of its own; see [Acting for many
users](#acting-for-many-users).

## Client Configuration

### ClientConfig

```rust
use std::net::SocketAddr;

use felix_client::{ClientConfig, ClusterClient, PublishSharding, quic_client_config};

let quinn = quic_client_config(None, false)?;
let config = ClientConfig {
    // Connection pools
    event_conn_pool: 8,              // Connections for pub/sub
    cache_conn_pool: 8,              // Connections for cache
    publish_conn_pool: 4,            // Connections for publishing
    // A ClusterClient shares one connection per broker instead, and
    // grows it to at most this many when its streams are saturated.
    cluster_conn_pool: 8,
    cluster_streams_per_conn: 1024,
    
    // Streams per connection
    publish_streams_per_conn: 2,     // Publish streams per conn
    cache_streams_per_conn: 4,       // Cache streams per conn
    
    // Publish sharding
    publish_sharding: PublishSharding::HashStream,
    // ClusterClient only: streams of their own for up to this many shards
    // per broker (0 turns them off)
    publish_shard_streams: 4,

    // Everything else, the tenant and token included, from FELIX_* variables.
    ..ClientConfig::from_env_or_yaml(quinn, None)?
};

let seeds: Vec<SocketAddr> = vec!["127.0.0.1:5000".parse()?];
let client = ClusterClient::connect(&seeds, "localhost", config).await?;
```

### Acting for many users

A gateway that serves many users, each with their own Felix token, can hold one
`Client` and give each user a handle that shares its connections:

```rust
use std::sync::Arc;

use felix_client::{Client, RefreshingToken, TokenProvider};

let gateway = Client::connect(addr, "localhost", config).await?;

// Every stream alice's handle opens authenticates with alice's token, and the
// broker checks each of her requests against her grants, not the gateway's.
// `refresh_alice` is a token fetch like the one above, for her refresh token.
let alice_tokens: Arc<dyn TokenProvider> =
    Arc::new(RefreshingToken::with_initial(alice_token, refresh_alice));
let alice = gateway.with_identity("acme", alice_tokens).await?;
let feed = alice.subscribe("acme", "app", "alice-feed").await?;

// A fixed token, for a handle that will not outlive it.
let bob = gateway.with_identity_token("acme", bob_token).await?;
```

Refresh each chain from one place at a time and keep the newest refresh token.
Two concurrent refreshes of one token both succeed within the control plane's
reuse grace window (`FELIX_REFRESH_REUSE_GRACE_SECONDS`), but the older result
is a replay once the window ends.

A handle opens one publish stream and one cache stream and no connections, so
the gateway's connection count stays the same however many users it serves.
One user's token expiring stops only that user's new streams, and revoking the
user ends only that user's open ones.
Dropping a handle closes its streams. The gateway's own token only opens the
connections, so it needs no permissions: a gateway holding `token.delegate`
can use a connection-only token from the control plane
([docs/auth.md](https://github.com/GetFelix/felix/blob/main/docs/auth.md#the-gateways-own-connections)).

A refused publish or cache request ends the stream it was sent on, as on any
client, so a handle that has had one refused cannot publish or reach the cache
afterwards; build a new one. The broker gives each user on a connection their
own subscription cap and publish byte budget, so one user at a limit leaves the
others room. The whole connection also has a ceiling that its users share, four
users' worth by default (`FELIX_MAX_SUBSCRIPTIONS_PER_CONN_TOTAL` and
`FELIX_BROKER_PUBLISH_CONN_TOTAL_INFLIGHT_BYTES`): four users at their limits
use all of it, and a fifth is refused or slowed. If more users than that can be
at full load at once, raise the ceilings or spread the users over more than one
`Client`. Users also share the connections' flow-control windows, so read every
subscription promptly or drop it. A broker that binds tokens to client certificates accepts a user's token on
the gateway's certificate only once the control plane has delegated it to the
gateway (`/token/delegate`).

`ClusterClient::with_identity` and `with_identity_token` do the same for a
cluster. The handle is a `ClusterClient` that follows moved shards, redirects
and failed brokers as the gateway's does, and reaches each broker over the
gateway's connection to it, opening one only where the gateway has none. A
user whose subscription's shard moves to another broker keeps the
subscription, and the gateway still holds one connection per broker:

```rust
let gateway = ClusterClient::connect(&seeds, "broker.internal", config).await?;
// `subscribe` takes the handle as an `Arc`, like any `ClusterClient`.
let alice = Arc::new(gateway.with_identity("acme", alice_tokens).await?);
let feed = alice.subscribe("acme", "app", "alice-feed").await?;
```

Its routing state is its own, learned from its own requests, and when a
broker's connection is rebuilt the handle moves to the new one on its next
request there. See [docs/auth.md](https://github.com/GetFelix/felix/blob/main/docs/auth.md#many-users-over-one-client).

### TLS and ALPN

`ClientConfig` takes a ready-made `quinn::ClientConfig`.
`felix_client::quic_client_config` builds one: it verifies the broker against
the roots you pass, or the platform trust store for `None`, and offers the
`felix/1` ALPN when its second argument is `true`.

```rust
use felix_client::{ClientConfig, quic_client_config};

let quinn = quic_client_config(None, true)?;
let config = ClientConfig::from_env_or_yaml(quinn, None)?;
```

A broker with `FELIX_TLS_REQUIRE_ALPN=true` serves only clients that offer
`felix/1`. A broker older than ALPN support refuses them, so offer it only once
every broker you connect to is current.

To trust a private CA and present a client certificate, read both from PEM
files:

```rust
use std::sync::Arc;

use felix_client::{ClientIdentity, quic_client_config_with_identity, root_store_from_pem_file};

let roots = Arc::new(root_store_from_pem_file("ca.pem")?);
let identity = ClientIdentity::from_pem_files("client.pem", "client.key")?;
let quinn = quic_client_config_with_identity(Some(roots), identity, true)?;
```

`ClientIdentity::new` takes a chain and key you already hold. Each loader
names the file it could not use. For anything else, build the rustls config
yourself and set `alpn_protocols` to `vec![felix_wire::CLIENT_ALPN.to_vec()]`
to offer `felix/1`.

### Configuration Tuning

**Low-latency configuration**:

```rust
let config = ClientConfig {
    event_conn_pool: 4,
    cache_conn_pool: 4,
    publish_streams_per_conn: 1,
    cache_streams_per_conn: 2,
    publish_conn_pool: 2,
    ..ClientConfig::from_env_or_yaml(quic_client_config(None, false)?, None)?
};
```

**High-throughput configuration**:

```rust
let config = ClientConfig {
    event_conn_pool: 16,
    cache_conn_pool: 16,
    publish_streams_per_conn: 4,
    cache_streams_per_conn: 8,
    publish_conn_pool: 8,
    publish_sharding: PublishSharding::HashStream,
    ..ClientConfig::from_env_or_yaml(quic_client_config(None, false)?, None)?
};
```

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
broker's default. `Client::supports_ack_on_commit()` says whether the broker
honours it; one that predates it answers as before.

### The routing key decides the shard

**Without a key every record lands on shard 0**, so a multi-shard stream
behaves like a single-shard one. If you created a stream with several shards to
get throughput and are not passing a key, you are not getting it.

```rust
use bytes::Bytes;

cluster
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
hash, fixed when the stream was created. `ClusterClient` asks for it once per
stream along with the shard count (`Client::stream_routing`), so it computes
the same shard the broker does.

### Naming the shard

A producer that owns its partitioning, one shard per partition, can name the
shard instead of searching for keys that hash where it wants them:

```rust
// District 2 of the city writes to shard 2, whatever its name hashes to.
cluster
    .publish_batch_on_shard("acme", "city", "ticks", 2, vec![tick], AckMode::PerBatch)
    .await?;
```

`publish_on_shard` and `publish_batch_on_shard` (on `Publisher` and
`ClusterClient`) use the shard as given. `ClusterClient` sends the batch to
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
cluster
    .publish_at_least_once("acme", "prod", "orders", payload, AckMode::PerMessage)
    .await?;
```

With this opt-in the record is certain to land and **may land twice**.
It does not carry a routing key, because the re-send path has nowhere to put
one, so you can use `publish_keyed` or `publish_at_least_once` but not both.

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

### Batches through a ClusterClient

`ClusterClient` batches go straight to each shard's leader. A plain `Client`
sends every batch to the broker it is connected to, and on a cluster that broker
forwards the batches for shards it does not lead.

```rust
// One key: the batch is one frame to that shard's leader.
cluster
    .publish_batch_keyed("acme", "prod", "orders", key, payloads, AckMode::PerBatch)
    .await?;

// Records with their own keys: split by shard, one ordered frame per shard,
// sent to the leaders concurrently.
let report = cluster
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

### Publisher API

`Publisher` belongs to the single-connection `Client`, which applications
should not need; `ClusterClient` publishes through the same workers per broker.
For a tool that holds a `Client`:

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

Under `HashStream` the client goes one step further: each shard of a stream
gets a stream of its own. The broker answers a stream's pipelined publishes in
order, so shards sharing a stream would wait on the slowest; on separate
streams a shard stalled on a quorum holds up only itself. Each shard stream
is placed on the least-loaded connection, so a busy multi-shard stream
spreads over the client's connections and the broker's listeners instead of
riding one. The stream opens on the shard's first publish, which pays one
extra round trip to authenticate it, and stays open. A client keeps at most
`publish_shard_streams` of them per broker (4 by default,
`FELIX_PUB_SHARD_STREAMS`); shards past that share the pool.

The cap is per broker and first come, first served across every stream the
client publishes to: the first four shards (tenant, namespace, stream, shard)
it publishes to each get a dedicated stream, and every later shard uses the
shared pool permanently. A stalled dedicated shard holds up only itself; a
stalled pooled shard can hold up every other pooled shard, including other
streams' shards.

Four fully isolates streams of up to four shards. Isolation matters when a
shard can stall on its own, as with quorum waits or a dead follower on a
replicated stream. On a wide stream any cap protects only the first N shards
seen, so a high cap buys arbitrary partial protection at a throughput cost: on
an 8 vCPU broker with a 48-shard stream and 256 B acked publishes, 16 streams
measured about 97 MB/s, 4 about 125 MB/s and 0 (everything on the pool) about
162 MB/s. For wide replicated streams that need isolation, raise
`FELIX_PUB_SHARD_STREAMS` to the total number of shards the client publishes
to on a broker. Adaptive dedicated streams are tracked in #1417.

Each broker connection asks for a stream's width on its first keyed publish
to it (one more round trip) and picks a key's stream from that answer alone,
whoever publishes: a plain publisher, a `ClusterClient` or an idempotent
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

A `ClusterClient` keeps its own widths, to pick the broker for a key, and an
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

```rust
use felix_client::{AckMode, BrokerError, RetryClass};

let result = cluster
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

`ClusterClient` already acts on the class before an error reaches you:

- `Fatal` is returned at once.
- `OutcomeUnknown` is returned by `publish` and never re-sent.
  `publish_at_least_once` and an idempotent producer send it again: the first
  accepts duplicates by name, the second's sequence prevents them.
- `Retry` and `Redirect` (`shard_unavailable`, `draining`, `not_leader`) from a
  cached shard owner drop that owner and go straight to the entry broker, even
  from `publish`, since nothing was applied. From the entry broker the retrying
  paths back off. `stale_claim` and `stale_width` back off too: neither says
  the owner is wrong.
- `RetryAfter` backs off for at least `retry_after_ms`. `not_found` is retried
  for 5 s from the first one, long enough for a newly promoted broker to hear
  about the stream from the control plane and no longer.
- `publish` replaces its connection only for `draining` or when no broker
  answered; any other coded answer came from a live broker.

A broker without error codes gets the old handling: only a credential failure
is final, and everything else is retried. The full table is under "Retries" in
[the multi-node client guide](https://github.com/GetFelix/felix/blob/main/docs/multi-node-client.md#retries).

`SubscribeCursorError` carries more than the other clients get:

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

## Idempotent producers

At-least-once *without* the duplication. The producer numbers its batches, the
shard's leader remembers the last few, and a batch carrying a sequence it
already holds is answered from memory rather than appended, so a re-send after
a lost acknowledgement lands once.

Rust only: neither binding wraps this yet.

The producer keeps a handle on its client, so `idempotent_producer` is called
on an `Arc<ClusterClient>` (or `Arc<Client>`), and the producer it returns can
be stored or moved into a task without borrowing anything.

```rust
let cluster = Arc::new(cluster);
let producer = cluster.idempotent_producer().await?;

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
Under a `ClusterClient` each shard's batches go on that shard's own publish
stream, as its other publishes do; a plain `Client`'s producer stays on the
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
remembers 64 sequences per producer. The broker answers a pipelining stream in
the order it sent the batches, so when one fails the producer knows it is the
earliest failure; it and every batch behind it are in doubt, and a
`ClusterClient` producer re-sends them, in order, under the same sequences until
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
            producer = cluster.idempotent_producer().await?;
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

## Subscribing

### Creating Subscriptions

```rust
let mut subscription = client.subscribe("acme", "prod", "events").await?;

while let Some(event) = subscription.next_event().await? {
    process_event(event).await?;
}
```

`None` means the broker ended the stream. A lost connection is an error,
`SubscriptionLost`, so the loop above stops with `?` rather than exiting as if
the stream had finished. A `ClusterClient` subscription resubscribes by itself
from the offset after the last one it delivered; on an in-memory stream there
is no offset to resume from, so it returns the error. `next_event` is
cancel-safe, so wrapping it in `tokio::time::timeout` is fine: a resubscribe in
progress keeps running in the background and the next call picks it up.

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
    /// Offsets just before `offset` that hold no event. Non-zero only on the
    /// first event after a leader change.
    pub skipped_before: u64,
    /// Who published it, when `ClientConfig::publishers` asked and the broker
    /// recorded one.
    pub publisher: Option<Arc<str>>,
    /// When the broker appended it, in microseconds since the Unix epoch,
    /// when `ClientConfig::timestamps` asked. `None` on an in-memory stream.
    pub timestamp_micros: Option<u64>,
}
```

### Who published an event

Set `publishers: true` in the `ClientConfig` and each event says who published
it: `event.publisher` is the subject of the token the broker accepted the
write from. Group records carry the same value as `publisher`. Off by default,
since it adds the principal to every batch.

```rust
let mut config = ClientConfig::from_env_or_yaml(quic_client_config(None, false)?, None)?;
config.publishers = true;
let client = Arc::new(ClusterClient::connect(&seeds, "localhost", config).await?);
let mut inputs = client.subscribe("arena", "match-7", "inputs").await?;
while let Some(event) = inputs.next_event().await? {
    let Some(player) = event.publisher.as_deref() else {
        continue; // written before the broker recorded publishers
    };
    apply_input(player, &event.payload);
}
```

That is the broker vouching that the write came from that principal; it says
nothing about the payload's contents. An in-memory stream always reports it. A
durable stream reports it only for records written once the broker stores
publishers (the `publisher_principal` fleet feature, or
`FELIX_RECORD_PUBLISHERS=true` on a single broker), and then on replay and to
consumer groups as well.

### Sizing the broker's queue for a subscription

Each subscription has a bounded queue on the broker. Set
`broker_sub_queue_capacity` in the `ClientConfig` to ask for a different size
than the stream's default, counted in published batches. The broker clamps it
to its `FELIX_SUBSCRIBER_QUEUE_CAPACITY_MAX` and the subscription reports what
it got:

```rust
config.broker_sub_queue_capacity = Some(4096);
let client = Client::connect(addr, "localhost", config).await?;
let subscription = client.subscribe("acme", "prod", "webhooks").await?;
// `None` from a broker that predates the option: the stream's default applies.
let granted = subscription.queue_capacity();
tracing::info!(?granted, "subscription queue");
```

It applies to every subscription the client opens, including the ones a
`ClusterClient` reopens after a move or a lag. A larger queue makes drops
rarer; it does not change what happens on a drop, which the stream's policy
decides (see [When a subscription falls behind](#when-a-subscription-falls-behind)).

### When an event was written, and replaying from a time

Set `timestamps: true` in the `ClientConfig` and each event from a durable
stream carries `event.timestamp_micros`: when the broker appended it, in
microseconds since the Unix epoch. Group records carry the same value as
`timestamp_micros`. Off by default, since it adds eight bytes to every event.

To replay from a time, look up the offset and subscribe there.
`offset_for_time` works whether or not `timestamps` is set:

```rust
let start = client
    .offset_for_time("acme", "prod", "webhooks", 0, since_micros)
    .await?;
let position = match start {
    Some(offset) => StartPosition::Offset(offset),
    // Nothing that recent yet.
    None => StartPosition::Latest,
};
let replay = client
    .subscribe_from("acme", "prod", "webhooks", Some(position))
    .await?;
```

The answer is the first record on that shard appended at or after the time,
and the oldest record when the time is older than all of them. Times come from
the leading broker's clock, so the search is exact while that clock only moves
forward, and close otherwise. `client.supports_offset_for_time()` says whether
the broker answers it. `ClusterClient::offset_for_time` asks whichever broker
leads the shard, following redirects.

### Reading a range and stopping

`subscribe_from` keeps going into live delivery. To load a fixed slice, such as
a match's events up to a snapshot offset, read it page by page instead. No
subscription is opened:

```rust
let mut from = start;
while from < end {
    let page = client
        .read("acme", "prod", "matches", 0, from, Some(end), 500)
        .await?;
    if page.records.is_empty() {
        break; // nothing committed past here yet
    }
    for record in &page.records {
        apply(record.offset, &record.payload);
    }
    from = page.next_offset;
}
```

Continue from `next_offset` rather than the last offset plus one, since offsets
that hold no record are passed over. A page holds at most `max_records` (`0`
for the broker's cap) and is capped in size by the broker. A start below what
retention kept, or past the tail, fails with `SubscribeCursorError`.
`ClusterClient::read` does the same against whichever broker leads the shard,
and `client.supports_read()` says whether the broker answers it.

### Offsets are how you notice a drop

Subscriber queues shed under the default policy rather than blocking the
publisher, so a subscriber can silently miss records. **A jump between
consecutive offsets is a drop**, with one exception the broker tells you about:
a new leader writes a generation-start record that takes an offset and is
never delivered, and the event after it says so in `skipped_before`.

```rust
let mut expected: Option<u64> = None;
while let Some(event) = subscription.next_event().await? {
    if let (Some(want), Some(got)) = (expected, event.offset) {
        // The first offset this event accounts for, past any that hold no event.
        let first = got - event.skipped_before;
        if first > want {
            tracing::warn!(dropped = first - want, "subscriber queue overflowed");
        }
    }
    expected = event.offset.map(|offset| offset + 1);
    handle(&event.payload);
}
```

Worth writing even if you never resume from offsets. It is the only signal the
queue overflowed. A broker older than the skip count reports none, so against
one a leader change reads as a drop of one.

### A consumer that survives a restart

Checkpoint what you handled and resume at the next one. `start` is the first
record you have **not** seen, so a resuming consumer passes `offset + 1`.

```rust
use felix_client::{StartPosition, SubscribeCursorError};

let mut start = match checkpoint.load().await? {
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
                    checkpoint.save(offset).await?;
                    start = StartPosition::Offset(offset + 1);
                }
            }
            // Nothing left to read.
            Ok(None) if subscription.stream_deleted() => return Ok(()),
            // Ended for another reason: resubscribe from `start`.
            Ok(None) => break,
            Err(err) => {
                if let Some(cursor) = err.downcast_ref::<SubscribeCursorError>() {
                    // Retention discarded it. `available` is the nearest offset
                    // that would have worked, so this takes the smallest gap
                    // rather than restarting at the beginning, and says so,
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
let subscription = client
    .subscribe_from("acme", "prod", "orders", Some(StartPosition::Offset(1_000)))
    .await?;
let first = subscription.start_offset(); // Some(1000)
let live = subscription.live_offset(); // the tail when you subscribed
```

Events below `live_offset()` are catch-up; events from it on are new, and none
are skipped between the two. A reader that has received an event at
`live_offset() - 1`, or asked to start at `live_offset()`, has caught up: the
offset leaves out any generation-start records at the end of the log, which
never arrive as events. From `Latest` the two offsets are equal and both stop
short of those records, so reading history up to `live_offset() - 1` after a
`Latest` subscribe ends on an event that exists. Both are
`None` for a plain tail subscribe, an in-memory stream, or an older broker.

### When a shard moves

A rebalance or a drain can move a stream's shard to another broker. The old
owner delivers everything it committed, then ends the subscription with a
`shard_moved` frame saying where the shard went and where to resume.

`ClusterClient::subscribe` and `subscribe_from` (shard 0), and
`subscribe_shard` (a shard you name), return a `ClusterSubscription` that
follows the shard on its own: `next_event` resubscribes on the new owner
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
} else if subscription.stream_deleted() {
    // The stream was deleted: there is nothing to resubscribe to.
}
```

A `ClusterSubscription` does not follow a deleted stream: `next_event` returns
`None` and `stream_deleted()` says why.

See [Multi-node client](https://github.com/GetFelix/felix/blob/main/docs/multi-node-client.md#when-a-shard-moves).

### When a subscription falls behind

Under `DropNew` or `DropOld`, a subscriber that cannot keep up loses records,
in the broker's queue for it or in the client's own. On a durable stream the
subscription then ends: `next_event` returns the events queued before the drop
and then a `SubscriptionLagged` error whose `resume_from` is where the
broker's queue first dropped. It comes as soon as the drop happens, not with
the next publish. Resubscribe after the last event you received, not at
`resume_from`: the broker can also drop a frame on its way out to a slow
subscriber without reporting it, and that frame can sit below `resume_from`.

```rust
use felix_client::{StartPosition, SubscriptionLagged};

let mut last = None;
let resume_at = loop {
    match subscription.next_event().await {
        Ok(Some(event)) => {
            last = event.offset;
            handle(&event);
        }
        Ok(None) => return Ok(()),
        Err(err) => match err.downcast_ref::<SubscriptionLagged>() {
            // Subscribe again after the last record; the log still has the rest.
            Some(lagged) => break last.map_or(lagged.resume_from, |last| last + 1),
            None => return Err(err),
        },
    }
};
let subscription = client
    .subscribe_from("acme", "prod", "events", Some(StartPosition::Offset(resume_at)))
    .await?;
```

`ClusterSubscription` does this itself, so a slow reader catches up from the
log instead of losing records. An in-memory stream has no offsets, so it keeps
dropping as before; `felix_client_sub_queue_dropped_total` and
`felix_client_sub_dispatch_dropped_total` count those drops.

### Multiple Subscriptions

Handle multiple streams concurrently:

```rust
let mut orders = client.subscribe("acme", "prod", "orders").await?;
let mut inventory = client.subscribe("acme", "prod", "inventory").await?;
let mut logs = client.subscribe("acme", "staging", "logs").await?;

// `next_event` is cancel-safe, so the branches that lose a round lose nothing.
// Stops when any of the three ends.
loop {
    tokio::select! {
        event = orders.next_event() => match event? {
            Some(event) => handle_order(event).await?,
            None => break,
        },
        event = inventory.next_event() => match event? {
            Some(event) => handle_inventory(event).await?,
            None => break,
        },
        event = logs.next_event() => match event? {
            Some(event) => handle_log(event).await?,
            None => break,
        },
    }
}
```

### Async Event Processing

Slow work inside the read loop holds up the next event, and a reader that
falls behind loses records (see [When a subscription falls
behind](#when-a-subscription-falls-behind)). Hand the work to a task over a
bounded channel instead. A task per event would also keep up, but with no
limit on how many run at once or on the memory they hold.

```rust
use tokio::sync::mpsc;

let (events, mut queued) = mpsc::channel(100);
let worker = tokio::spawn(async move {
    while let Some(event) = queued.recv().await {
        expensive_processing(event).await?;
    }
    anyhow::Ok(())
});

while let Some(event) = subscription.next_event().await? {
    if events.send(event).await.is_err() {
        break; // the worker stopped; its result below says why
    }
}
// Closing the channel lets the worker drain what is queued and finish.
drop(events);
worker.await??;
```

Once the channel is full, `send` waits, and the reader slows to the worker's
pace like any slow reader.

### Subscription Lifecycle

```rust
let mut subscription = client.subscribe("acme", "prod", "events").await?;

// Read up to 100 events, then stop.
for _ in 0..100 {
    let Some(event) = subscription.next_event().await? else {
        break;
    };
    process(event);
}

// Dropping the subscription closes it on the broker.
drop(subscription);
```

## Cache Operations

### Put and Get

```rust
use anyhow::bail;

// tenant, namespace, cache, key, value (`Bytes`), TTL in milliseconds
client
    .cache_put("acme", "prod", "sessions", "user-abc", session_data, Some(60 * 60 * 1000))
    .await?;

let Some(data) = client.cache_get("acme", "prod", "sessions", "user-abc").await? else {
    bail!("session user-abc expired or never existed");
};
let session: Session = serde_json::from_slice(&data)?;
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
// Answers with the value that was removed, or `None` if the key was not there,
// so a caller can tell a delete that did something from one that did not.
match client.cache_delete("acme", "prod", "sessions", "user-abc").await? {
    Some(removed) => audit_log("session revoked", removed),
    None => { /* already gone, or never there */ }
}
```

Needs a broker advertising `FEATURE_CACHE_DELETE`. The client returns an error
rather than probing, because a broker that does not advertise
`FEATURE_UNSUPPORTED` ends its control loop on an unrecognised message type.

### Conditional writes

```rust
use felix_client::CacheCondition;

// Read, change, and write back only if nobody wrote in between.
loop {
    let Some(current) = client
        .cache_get_versioned("acme", "prod", "config", "endpoints")
        .await?
    else {
        break;
    };
    let edited = edit(&current.value);
    let result = client
        .cache_put_if("acme", "prod", "config", "endpoints", edited, None,
            CacheCondition::Version(current.version))
        .await?;
    if result.applied {
        break;
    }
}
```

`CacheCondition::Absent` writes only if the key has no live entry, which is
how to take a lease or claim an idempotency key. `cache_delete_if` removes a
key only at a given version. A refusal is `applied: false` with the key's
current version, not an error. Needs a broker advertising
`FEATURE_CACHE_CONDITIONAL`; `ClusterClient` has the same three calls and never
resends a conditional write after a lost answer. See
[Conditional writes](/features/cache/#9-conditional-writes).

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

When `recv` returns `None` without either, the broker either closed the
stream or ended the watch with an error, and `watch.refusal()` holds that error
as a `BrokerError`. A `revoked` code there means the credential the watch was
opened with was revoked; watch again with a fresh token. A subscription ended
for the same reason returns the `BrokerError` from `next_event`.

A resume whose history compaction has collapsed begins with each matching
key's current value instead, and `watch.resnapshot()` says so. Needs a broker
advertising `FEATURE_CACHE_WATCH`, which only brokers with a log-backed cache send.
`ClusterClient::watch_cache` returns a `ClusterCacheWatch`, which follows a
moved shard itself: `ShardMoved` arrives as a notice and the changes carry on
from the new owner, none repeated or skipped. A prefix watch reads one shard; on a multi-shard cache use
`ClusterClient::watch_cache_sharded` (see [Clusters](#clusters)).
See [Cache Features](/features/cache/#7-keyed-watch) for the full
contract.

### Retained Watch

Start from current state instead of from now: each matching key's current
value first, then live changes. Use it to join presence or state sync:

```rust
let watch = client
    .watch_cache_retained(
        "acme",
        "prod",
        "presence",
        CacheWatchFilter::Prefix("room:7:".into()),
    )
    .await?;

// The state phase is exactly this many changes; 0 means empty, definitively.
let state_size = watch
    .retained_count()
    .context("the broker did not report the retained count")?;
```

Needs `FEATURE_CACHE_WATCH_RETAINED`, a separate bit so an older watch-capable
broker is never asked for state it would silently not deliver. Mutually
exclusive with `from_offset`, because a resume already replays what a retained start
shortcuts.

### Counters

```rust
// Apply a delta and learn the sum including it, in one round trip.
let after = client
    .counter_add("acme", "prod", "limits", "user:42:reqs", 1)
    .await?;

// Read; None means never written, which differs from a sum of zero.
let views = client
    .counter_get("acme", "prod", "metrics", "page:home")
    .await?;
```

Scoped and routed like cache keys, stored beside the cache; durable and
replicated with the shard. At-least-once: a retry after a lost ack counts
twice. Needs a broker advertising `FEATURE_COUNTERS` (durable brokers only).

### Concurrent Cache Operations

Requests issued together share the client's connections and run
concurrently:

```rust
use tokio::task::JoinSet;

let mut lookups = JoinSet::new();
for i in 0..10 {
    let client = Arc::clone(&client);
    lookups.spawn(async move {
        let key = format!("key-{i}");
        client.cache_get("acme", "prod", "data", &key).await
    });
}

// Answers arrive in completion order. The first failure stops the loop, and
// dropping the set cancels the rest.
while let Some(answer) = lookups.join_next().await {
    if let Some(value) = answer?? {
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
        // consumer can treat a retry differently. 0 means the broker did not
        // report it, which is not the same as a first attempt.
        match handle(&record.payload, record.attempts).await {
            Ok(()) => {
                client
                    .group_ack("acme", "prod", "jobs", 0, "fulfilment", record.offset)
                    .await?;
            }
            Err(err) => {
                tracing::warn!(offset = record.offset, "job failed: {err:#}");
                // Hand it back for immediate redelivery rather than waiting
                // out the visibility timeout.
                client
                    .group_nack("acme", "prod", "jobs", 0, "fulfilment", &record)
                    .await?;
            }
        }
    }
}
```

An empty batch means nothing was available. It is not an error.

### Dead letters

Past `FELIX_GROUP_MAX_ATTEMPTS` a record is dead-lettered, so one poison record
cannot stall the queue behind it. These need `FEATURE_GROUP_DEAD_LETTERS`, a
separate bit from `FEATURE_CONSUMER_GROUP`:

```rust
let offsets = client
    .group_dead_letters("acme", "prod", "jobs", 0, "fulfilment")
    .await?;
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

### Creating, moving and deleting a group

A group starts at offset 0 on its first poll unless it was created first. These
need `FEATURE_GROUP_ADMIN` (`Client::supports_group_admin`):

```rust
use felix_client::StartPosition;

// Left alone if the group already exists, so safe on every start.
client
    .group_create("acme", "prod", "jobs", 0, "fulfilment", StartPosition::Latest)
    .await?;

// Replay everything the shard still holds.
client
    .group_seek("acme", "prod", "jobs", 0, "fulfilment", StartPosition::Earliest)
    .await?;

let info = client
    .group_describe("acme", "prod", "jobs", 0, "fulfilment")
    .await?;
println!("committed {:?}, {} behind", info.committed, info.lag());

// `false` if there was no such group.
let existed = client
    .group_delete("acme", "prod", "jobs", 0, "fulfilment")
    .await?;
```

A seek voids the claims standing when it lands, so an ack for a record the
group now owes fails with `stale_claim`. `ClusterClient` has the same calls per
shard, and `group_create_stream`, `group_seek_stream`, `group_describe_stream`
and `group_delete_stream` for every shard of a stream.

A consumer can also manage each claim itself on a broker that advertises
`FEATURE_GROUP_CLAIM_CONTROL` (`Client::supports_group_claim_control`):

```rust
use std::time::Duration;

use felix_client::GroupPollOptions;

// Claims from this poll stand for five minutes instead of the broker's default.
let options = GroupPollOptions {
    visibility: Some(Duration::from_secs(300)),
    ..Default::default()
};
let records = client
    .group_poll_with("acme", "prod", "jobs", 0, "fulfilment", 10, &options)
    .await?;

for record in &records {
    match check(record) {
        Job::Done => {
            client
                .group_ack("acme", "prod", "jobs", 0, "fulfilment", record.offset)
                .await?;
        }
        // Still working: keep the claim for another minute.
        Job::Running => {
            client
                .group_extend("acme", "prod", "jobs", 0, "fulfilment", record, Duration::from_secs(60))
                .await?;
        }
        // Failed for now: back off before the next try.
        Job::Failed => {
            client
                .group_nack_after("acme", "prod", "jobs", 0, "fulfilment", record, Duration::from_secs(30))
                .await?;
        }
        // Will never work: listed as a dead letter at once, and can be redriven.
        Job::Poison => {
            client
                .group_dead_letter("acme", "prod", "jobs", 0, "fulfilment", record.offset)
                .await?;
        }
    }
}
```

An extension or a nack, delayed or not, is refused with `stale_claim` once the
claim has lapsed and the record has been handed out again: each names the
delivery by `record.attempts`. The client refuses a delay or a visibility
to a broker without the bit, which would ignore it. `ClusterClient` has the
same calls, and `ShardedGroup` has `extend`, `nack_after` and `dead_letter`.

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

## Atomic commits

`commit` writes an event and the state it changes as one record on the shard
an entity key routes to; `state_get` reads that state back with the commit's
offset as its version. `Client` answers a shard led elsewhere with
`NotLeaderError`, and `ClusterClient` follows it.

```rust
use felix_client::{CommitError, CommitOp};

let receipt = cluster
    .commit("acme", "orders", b"order-42", vec![
        CommitOp::enqueue("order-events", r#"{"type":"placed"}"#),
        CommitOp::put("order-events", "order-42", r#"{"status":"placed"}"#),
    ])
    .await?;
let state = cluster
    .state_get("acme", "orders", "order-events", b"order-42", "order-42")
    .await?;
// The state's version is the offset of the commit that wrote it.
println!("committed at {}, state version {:?}", receipt.offset, state.version);

// Refused before anything is sent: another stream is another log.
let refused = cluster
    .commit("acme", "orders", b"order-42", vec![
        CommitOp::publish("order-events", "placed"),
        CommitOp::put("inventory", "sku-1", "3"),
    ])
    .await;
if let Err(err) = refused {
    match err.downcast_ref::<CommitError>() {
        // Op 1 names a stream the entity key does not route to.
        Some(CommitError::NotOnOwningShard { index, .. }) => {
            tracing::error!(index, "commit spans two streams");
        }
        _ => return Err(err),
    }
}
```

`CommitError::EventCount` refuses a commit without exactly one event, and
`CommitError::Unsupported` a broker that did not advertise
`FEATURE_ATOMIC_COMMIT`.

`commit_if` and `publish_if` write only at an expected offset, the shard's
next, for a single writer that must not append after it has been replaced:

```rust
use anyhow::bail;
use felix_client::ConditionalWrite;

// The offset this writer expects to write next, starting at an empty shard.
let mut next = 0;
for tick in ticks {
    match cluster
        .publish_if("acme", "games", "match-7", None, vec![tick], next)
        .await?
    {
        ConditionalWrite::Written { offset } => next = offset + 1,
        ConditionalWrite::Refused { tail } => {
            bail!("another writer has the stream; its next offset is {tail}")
        }
    }
}
```

A refusal is an answer, not an error, and writes nothing. Both need
`FEATURE_PUBLISH_CONDITIONAL`, and fail without sending anything against a
broker that did not advertise it. `ClusterClient` follows the leader; neither
is forwarded between brokers. Do not resend one whose answer was lost: read
the shard at the expected offset instead.

`commit_on_shard`, `commit_if_on_shard`, `publish_if_on_shard` and
`state_get_on_shard` take a shard instead of hashing the entity key, for a
writer that keeps one shard per partition:

```rust
let ops = vec![
    CommitOp::publish("ticks", tick),
    CommitOp::put("ticks", "district", state),
];
let written = cluster
    .commit_if_on_shard("acme", "city", district, b"district", ops, next)
    .await?;
let current = cluster
    .state_get_on_shard("acme", "city", "ticks", district, "district")
    .await?;
```

A keyed `state_get(entity_key)` reads the shard the key hashes to, so it does
not find state committed to another named shard; read it with
`state_get_on_shard`. A subscriber that picks its shard by hashing a key and
filters by key misses those records the same way: subscribe to the named shard.

What atomic does and does not cover is in
[`docs/atomic-commit.md`](https://github.com/GetFelix/felix/blob/main/docs/atomic-commit.md).

## Clusters

`ClusterClient` follows the cluster: it takes one or more addresses, learns the
rest, reconnects when the broker it is using goes away, and follows a redirect
to whichever broker owns a shard. `Client` talks to one broker; it is the
single-connection building block, and applications should use `ClusterClient`
(see [Which client to use](#which-client-to-use)).

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

`Client::shard_owners(tenant, namespace, name, ShardKind::Stream)` (or
`ShardKind::Cache`) says which broker owns each shard: one `ShardOwner` per
shard with its node id, client address and generation, from the answering
broker's routing snapshot. It needs `FEATURE_SHARD_OWNERS`.

Cache and counter calls (`cache_put`, `cache_get`, `cache_delete`,
`counter_add`, `counter_get`) are on `ClusterClient` too. Each goes to the owner
of the key's shard, asked once per cache with `shard_owners`, and otherwise
through the broker in use, which forwards. A write that may have been applied
is not sent twice, as for `publish`; a failed read is asked again once.

An `AckMode::None` publish returns once it is queued. Call
`ClusterClient::finish()` before exiting: it waits until everything queued on
every broker the client holds has been written, then closes the publish
streams, so publishing through that client afterwards fails.
`Publisher::finish()` does the same for one `Client`.

Prefix watches on a multi-shard cache work the same way. `watch_cache_sharded`
opens one watch per shard and merges them. The retained version sends
`ShardedCacheWatchItem::StateComplete` once every shard's current values have
arrived. A shard that moves is followed to its new owner and reported as
`ShardedCacheWatchItem::ShardMoved`; its changes carry on from where the old
owner left off. Needs `FEATURE_CACHE_SHARDS`.

## Connection Management

`ClusterClient` handles reconnection: it retries on another broker with the backoff in
`ReconnectPolicy`, which `connect_with_policy` lets you tune. `Client` does
not reconnect: when its connection drops, calls fail and the application
decides what to do.

```rust
use std::time::Duration;

use felix_client::{ClusterClient, ReconnectPolicy};

let policy = ReconnectPolicy {
    attempts: 10,
    max_backoff: Duration::from_secs(5),
    ..ReconnectPolicy::default()
};
let client = ClusterClient::connect_with_policy(&seeds, "localhost", config, policy).await?;
```

When every address the client holds has stopped answering, it resolves names
again and dials what is new: seeds given as `host:port` names, and the names
brokers advertised. That is what gets it back in after a rollout gives every
broker a new IP. `connect_with_policy` takes `SocketAddr`s, which have no name
to resolve; `connect_seeds` takes `Seed`s, which can be either:

```rust
use felix_client::{ClusterClient, ReconnectPolicy, Seed};

let seeds: Vec<Seed> = vec!["felix.prod.svc.cluster.local:5000".parse()?];
let client =
    ClusterClient::connect_seeds(&seeds, "localhost", config, ReconnectPolicy::default()).await?;
```

Names are resolved at most once a second for a client and its identities,
all at once, each bounded to 2 s, and without holding the lock operations
read the client through. A name has at most one system lookup running, so one
that hangs does not stop the others resolving. See `docs/multi-node-client.md`.

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
use felix_client::{AckMode, ClientConfig, ClusterClient, quic_client_config};
use tokio::task::JoinSet;

#[tokio::main]
async fn main() -> Result<()> {
    let config = ClientConfig::from_env_or_yaml(quic_client_config(None, false)?, None)?;
    let seeds: Vec<SocketAddr> = vec!["127.0.0.1:5000".parse()?];
    let client = Arc::new(ClusterClient::connect(&seeds, "localhost", config).await?);

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

`ClusterClient` already retries what it safely can (see [Errors you can act
on](#errors-you-can-act-on)). A caller that wants more attempts on top resends
only what the broker says was not applied:

```rust
use std::time::Duration;

use anyhow::Result;
use felix_client::{AckMode, BrokerError, ClusterClient, RetryClass};

/// Publish to `stream`, resending only when the broker applied nothing, with
/// a doubling pause between at most `attempts` tries.
async fn publish_with_retry(
    client: &ClusterClient,
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
use felix_client::{AckMode, ClusterClient};
use tokio::sync::mpsc;
use tokio::time::{Duration, interval};

const MAX_BATCH: usize = 64;

/// Publish what arrives on `messages` in batches of up to `MAX_BATCH`,
/// sending a partial batch every 10 ms, until every sender is gone.
async fn batching_publisher(
    client: &ClusterClient,
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
use felix_client::{ClientConfig, ClusterClient, quic_client_config};

#[tokio::test]
#[ignore = "needs a broker on 127.0.0.1:5000"]
async fn cache_entry_expires() -> Result<()> {
    let config = ClientConfig::from_env_or_yaml(quic_client_config(None, false)?, None)?;
    let seeds: Vec<SocketAddr> = vec!["127.0.0.1:5000".parse()?];
    let client = ClusterClient::connect(&seeds, "localhost", config).await?;

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

## API Reference Summary

| Operation | Method | Use Case |
|-----------|--------|----------|
| Connect | `ClusterClient::connect()` | Any deployment: one broker or a cluster |
| Single publish | `ClusterClient::publish()` | Low-rate events |
| Batch publish | `ClusterClient::publish_batch()` | High-throughput |
| Idempotent publish | `idempotent_producer()` | Safe resend after `OutcomeUnknown` |
| Subscribe | `subscribe()` | Event consumption |
| Cache put / get / delete | `cache_put()`, `cache_get()`, `cache_delete()` | Key-value with TTL |
| Conditional cache writes | `cache_put_if()`, `cache_delete_if()`, `cache_get_versioned()` | Leases, idempotency keys, compare-and-set |
| Cache watch | `watch_cache()`, `watch_cache_retained()` | Follow changes to a key or prefix |
| Counters | `counter_add()`, `counter_get()` | Durable counters |
| Consumer groups | `group_poll()`, `group_ack()`, `group_nack()` | Work queues |
| Single connection | `Client::connect()`, `Client::publisher()` | Tools and special cases; one broker, no reconnect |
| One shard | `ClusterClient::subscribe_shard()` | Follow a chosen shard through moves |
| Flush | `ClusterClient::finish()`, `Publisher::finish()` | Before exit, after `AckMode::None` |
| Shard owners | `Client::shard_owners()` | Which broker owns each shard |
| Mutual TLS | `quic_client_config_with_identity()` | Present a client certificate |

For complete API documentation, see the [rustdoc](https://docs.rs/felix-client).
