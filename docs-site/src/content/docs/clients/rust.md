---
title: "Rust Client SDK"
---

:::note
The old single-page guide is now split. Jump to [Publishing](/clients/rust/publishing/), [Subscribing](/clients/rust/subscribing/), [Cache Operations](/clients/rust/cache/), [Consumer Groups and Commits](/clients/rust/groups-and-commits/) or [Clusters and Operations](/clients/rust/operations/).
:::

`felix-client` is the Rust SDK: publish, subscribe, cache, consumer groups,
and the cluster client, over multiplexed QUIC connections. This page covers
setup and configuration; the topic pages below cover the rest.

The Python and TypeScript clients and the C ABI bind to it, and the C# SDK binds the C ABI. See [Choosing a Client](/clients/overview/) for those bindings
and for how a new language is gated on a conformance suite.

## Topic pages

- [Publishing](/clients/rust/publishing/): single and batch publishes, keys, acknowledgement modes, headers, and idempotent producers.
- [Subscribing](/clients/rust/subscribing/): live and replayed reads, filters, shards, and resuming after a restart.
- [Cache Operations](/clients/rust/cache/): key-value, conditional writes, leases, watches, and counters.
- [Consumer Groups and Commits](/clients/rust/groups-and-commits/): work queues, atomic commits, and trimming.
- [Clusters and Operations](/clients/rust/operations/): clusters, connections, telemetry, patterns, and testing.
- [Errors](/clients/errors/) and [Auth and TLS](/clients/auth-and-tls/): shared by every client.

## Installation

```bash
cargo add felix-client@0.7.0-preview.2
```

This page describes the 0.7 API. While 0.7 is in preview, a plain `cargo add felix-client` installs the last stable release, which does not have it, so name the preview. Replace `2` with the newest preview on the [releases page](https://github.com/GetFelix/felix/releases).

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
use felix_client::{AckMode, ClientConfig, Client, quic_client_config};

#[tokio::main]
async fn main() -> Result<()> {
    // FELIX_AUTH_TENANT and FELIX_AUTH_TOKEN supply the tenant and the token,
    // so neither is written into the program.
    let config = ClientConfig::from_env_or_yaml(quic_client_config(None, false)?, None)?;

    // One broker is enough. The rest of a cluster is discovered from it.
    let seeds: Vec<SocketAddr> = vec!["127.0.0.1:5000".parse()?];
    let client = Arc::new(Client::connect(&seeds, "localhost", config).await?);

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

`subscribe` takes `&Arc<Client>`, because a subscription keeps a handle
on the client to follow its shard. The examples below that subscribe assume an
`Arc<Client>`.

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

Use `Client`, whether you run one broker or a cluster. It sends each
record straight to the broker that leads its shard, reconnects when a broker
goes away, and follows shards that move. A single broker needs no cluster
mode for it, and the client asks nothing of the control plane: discovery finds
nothing to add, and that broker owns every shard.

`Connection` is the single-connection building block underneath it, for tools and
special cases. It talks to one broker. In a cluster, a publish that reaches a
broker that does not lead the shard is forwarded to the one that does, and
that hop is expensive. In the v0.6.0 three-node, three-replica run, sending
through `Connection` instead of straight to the owner cut in-memory throughput
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

## API at a glance

| To do this | Call | Details |
|------------|------|---------|
| Connect to one broker or a cluster | `Client::connect()` | [Setup](#client-configuration) |
| Publish records | `Client::publish()`, `publish_batch()` | [Publishing](/clients/rust/publishing/) |
| Read a stream | `subscribe()` | [Subscribing](/clients/rust/subscribing/) |
| Store keys with a TTL | `cache_put()`, `cache_get()` | [Cache Operations](/clients/rust/cache/) |
| Share work between consumers | `group_poll()`, `group_ack()` | [Consumer Groups and Commits](/clients/rust/groups-and-commits/) |
| Attach headers to a record | `publish_with_headers()` | [Publishing](/clients/rust/publishing/) |

For complete API documentation, see the [rustdoc](https://docs.rs/felix-client).

## Client Configuration

### ClientConfig

```rust
use std::net::SocketAddr;

use felix_client::{ClientConfig, Client, PublishSharding, quic_client_config};

let quinn = quic_client_config(None, false)?;
let config = ClientConfig {
    // Connection pools
    event_conn_pool: 8,              // Connections for pub/sub
    cache_conn_pool: 8,              // Connections for cache
    publish_conn_pool: 4,            // Connections for publishing
    // A Client shares one connection per broker instead, and
    // grows it to at most this many when its streams are saturated.
    cluster_conn_pool: 8,
    cluster_streams_per_conn: 1024,
    
    // Streams per connection
    publish_streams_per_conn: 2,     // Publish streams per conn
    cache_streams_per_conn: 4,       // Cache streams per conn
    
    // Publish sharding
    publish_sharding: PublishSharding::HashStream,
    // Client only: streams of their own for up to this many shards
    // per broker (0 turns them off)
    publish_shard_streams: 4,

    // Everything else, the tenant and token included, from FELIX_* variables.
    ..ClientConfig::from_env_or_yaml(quinn, None)?
};

let seeds: Vec<SocketAddr> = vec!["127.0.0.1:5000".parse()?];
let client = Client::connect(&seeds, "localhost", config).await?;
```

### Acting for many users

A gateway that serves many users, each with their own Felix token, can hold one
`Client` and give each user a handle that shares its connections:

```rust
use std::sync::Arc;

use felix_client::{Client, RefreshingToken, TokenProvider};

let gateway = Client::connect(&seeds, "broker.internal", config).await?;

// Every stream alice's handle opens authenticates with alice's token, and the
// broker checks each of her requests against her grants, not the gateway's.
// `refresh_alice` is a token fetch like the one above, for her refresh token.
let alice_tokens: Arc<dyn TokenProvider> =
    Arc::new(RefreshingToken::with_initial(alice_token, refresh_alice));
let alice = Arc::new(gateway.with_identity("acme", alice_tokens).await?);
let feed = alice.subscribe("acme", "app", "alice-feed").await?;

// A fixed token, for a handle that will not outlive it.
let bob = gateway.with_identity_token("acme", bob_token).await?;
```

Refresh each chain from one place at a time and keep the newest refresh token.
Two concurrent refreshes of one token both succeed within the control plane's
reuse grace window (`FELIX_REFRESH_REUSE_GRACE_SECONDS`), but the older result
is a replay once the window ends.

A handle opens streams on the gateway's connections, never connections of its
own: one publish stream, and one cache stream on its first cache request. The
gateway's pools bound its connection count however many users it serves.
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
`Connection`. Users also share the connections' flow-control windows, so read every
subscription promptly or drop it. A broker that binds tokens to client certificates accepts a user's token on
the gateway's certificate only once the control plane has delegated it to the
gateway (`/token/delegate`).

Each handle is a `Client` that follows moved shards, redirects and failed
brokers as the gateway's does, and reaches each broker over the gateway's
connection to it, opening one only where the gateway has none. A user whose
subscription's shard moves to another broker keeps the subscription, and the
gateway still holds one connection per broker.

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
