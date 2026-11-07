# felix-client

[Felix](https://github.com/GetFelix/felix) is a broker that serves streams, work queues and a cache from one replicated log, over QUIC. This is the Rust client. It publishes to and reads streams, runs consumer groups, uses the cache and counters, and follows a cluster as brokers fail and shards move. The Python, TypeScript, C and C# clients are bindings over this crate.

## Install

```bash
cargo add felix-client
```

The optional `telemetry` feature adds frame and byte counters, exported as metrics. It is off by default.

## Quick start

Connect, subscribe, publish one record and read it back:

```rust
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use felix_client::{AckMode, Client, ClientConfig, quic_client_config};

#[tokio::main]
async fn main() -> Result<()> {
    let token = std::env::var("FELIX_TOKEN").context("set FELIX_TOKEN")?;

    let mut config = ClientConfig::from_env_or_yaml(quic_client_config(None, false)?, None)?;
    config.auth_tenant_id = Some("t1".into());
    config.auth_token = Some(token);

    // One broker is enough. The rest of a cluster is discovered from it.
    let seeds: Vec<SocketAddr> = vec!["127.0.0.1:5000".parse()?];
    let client = Arc::new(Client::connect(&seeds, "localhost", config).await?);

    // Subscribe before publishing: a subscription starts at the stream's tail.
    let mut events = client.subscribe("t1", "default", "events").await?;
    client
        .publish("t1", "default", "events", b"hello".to_vec(), AckMode::PerMessage)
        .await?;

    let event = tokio::time::timeout(Duration::from_secs(5), events.next_event())
        .await
        .context("no event within 5 seconds")??
        .context("the broker ended the subscription")?;
    println!("{:?}", event.payload);
    Ok(())
}
```

To run it you need a running broker, a token for tenant `t1`, and the stream `t1/default/events`. The [quick start](https://docs.getfelix.dev/getting-started/quickstart/) shows how to start a broker, [Auth and TLS](https://docs.getfelix.dev/clients/auth-and-tls/) covers tokens, and [`felixctl stream create`](https://docs.getfelix.dev/getting-started/felixctl/) makes the stream. Use `Client` for one broker or a cluster. `Connection` is the single-connection layer underneath it, for tools and special cases.

## What it covers

- Publish and subscribe, with a log offset on every event of a durable stream
- Sharded streams: routing keys, named shards, and subscribing to every shard
- Consumer groups: poll, acknowledge, retry later, dead letters
- Cache, counters and cache watches
- Atomic commits, conditional writes and head trims
- Idempotent producers
- Token refresh, cluster discovery, failover and following a shard that moves

Record headers and client certificates (mutual TLS) work in this client only.

## Configuration

| Setting | Where |
| --- | --- |
| Broker addresses | the `seeds` argument of `Client::connect` |
| Tenant | `ClientConfig::auth_tenant_id`, or `FELIX_AUTH_TENANT` |
| Token | `ClientConfig::auth_token`, or `FELIX_AUTH_TOKEN` |
| Token provider | `ClientConfig::token_provider`, usually a `RefreshingToken`. It overrides `auth_token`. |
| CA file | `root_store_from_pem_file`, passed to `quic_client_config` |
| Server name | the `server_name` argument of `Client::connect` |
| Client certificate | `ClientIdentity` with `quic_client_config_with_identity` |
| Reconnect policy | `Client::connect_with_policy` and `ReconnectPolicy` |

TLS is always on, and there is no switch to skip verification. With no CA file the client trusts the operating system's roots. A development broker with a self-signed certificate needs its CA file:

```rust
use std::sync::Arc;

use felix_client::{quic_client_config, root_store_from_pem_file};

let roots = Arc::new(root_store_from_pem_file("ca.pem")?);
let quinn = quic_client_config(Some(roots), false)?;
```

A token lasts 15 minutes by default. A process that runs longer sets `token_provider` so the client can fetch a new one. [Auth and TLS](https://docs.getfelix.dev/clients/auth-and-tls/) lists the same options for every client.

## Errors

A failed call returns an `anyhow::Error`. Check whether the write may have landed first, then read the broker's retry class:

```rust
use felix_client::{AuthError, BrokerError, RetryClass, outcome_unknown};

fn handle(err: &anyhow::Error) {
    if outcome_unknown(err) {
        // The record may have been written. Reconcile before sending it again.
    } else if err.downcast_ref::<AuthError>().is_some() {
        // The token was refused or the token provider failed. Retrying will not help.
    } else if let Some(broker) = err.downcast_ref::<BrokerError>() {
        // Nothing was applied for `Retry`, `RetryAfter` and `Redirect`.
        let retry_later = matches!(
            broker.retry,
            RetryClass::Retry | RetryClass::RetryAfter | RetryClass::Redirect
        );
        eprintln!("{:?}: retry later: {retry_later}", broker.code);
    }
}
```

A `Client` already retries what is safe to retry. What reaches you is what was left. Do not match on the message text. [Errors across clients](https://docs.getfelix.dev/clients/errors/) maps every code to the type each client raises.

## Supported platforms

Rust 1.97 or newer, edition 2024.

## Documentation

- [Rust client guide](https://docs.getfelix.dev/clients/rust/)
- [Choosing a client](https://docs.getfelix.dev/clients/overview/)
- [Delivery semantics](https://docs.getfelix.dev/architecture/semantics/)
- [API reference on docs.rs](https://docs.rs/felix-client)

## License

Apache-2.0.
