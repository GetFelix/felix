---
title: "Quickstart"
---

The shortest path to a running Felix cluster with something happening on it.

A broker on its own is not a working system: it authenticates every connection
against a control plane, so it needs one to talk to and a credential to present.
The local cluster command below starts all of that for you, which is why it is
the first thing here rather than the last.

## Prerequisites

- **Rust** 1.97.1 or later ([rustup](https://rustup.rs/))
- **Git**
- Optional: [Task](https://taskfile.dev/), for the shortcuts CI uses

## Build

```bash
git clone https://github.com/GetFelix/felix.git
cd felix
cargo build --release
```

Use the release profile for anything you intend to measure. A debug build is
several times slower and will mislead you.

## A cluster, in one command

`felix-cluster` starts a control plane, mints the credentials, and brings up as
many brokers as you ask for:

```bash
cargo run --release -p felix-cluster -- up --nodes 3
```

```
starting a 3-node cluster...

cluster up.

control plane   http://127.0.0.1:52704

node       client                 metrics
broker-0   127.0.0.1:53348        127.0.0.1:52706
broker-1   127.0.0.1:65027        127.0.0.1:52707
broker-2   127.0.0.1:50410        127.0.0.1:52708

placeable: broker-0, broker-1, broker-2
shard ownership:
  stream/t1/ns/orders/0 -> broker-2

session   /tmp/felix-cluster.json

holding the cluster. press Ctrl-C to tear it down.
```

The session file sits in your system temp directory. It records the tenant
(`t1`), namespace (`ns`), a client token and every broker's address, and it is
how the other commands find the cluster.

It holds until you interrupt it. In a second window, subscribe:

```bash
cargo run --release -p felix-cluster -- subscribe orders
```

```
subscribing to orders on broker-2 (owner)
waiting for events. Ctrl-C to stop.
```

And in a third, publish:

```bash
cargo run --release -p felix-cluster -- publish orders "hello"
```

```
published "hello" to orders via broker-0 → forwarded to broker-2 → acknowledged
```

The subscriber prints it with its log offset:

```
[broker-2] offset      1  hello
```

That line is the whole model in miniature. The stream's shard is owned by
`broker-2`, you published through `broker-0`, and `broker-0` forwarded the
record to the owner and waited for it to be written before acknowledging. Which
broker you connect to is a routing detail, not a correctness one. The
acknowledgement says when forwarding happened, so a client can see that it is
paying for a relay on every record.

### The rest of the cluster commands

```bash
cargo run --release -p felix-cluster -- smoke        # publish through a non-owner, receive from the owner
cargo run --release -p felix-cluster -- demo         # the cross-broker story, paced for reading
cargo run --release -p felix-cluster -- failover     # kill the leader, keep publishing
cargo run --release -p felix-cluster -- consistency  # what Quorum buys and Leader costs, under a fault
cargo run --release -p felix-cluster -- status       # membership and shard ownership, then exit
```

`up` first for `subscribe` and `publish`; the others start their own cluster.

### With felixctl

[`felixctl`](/getting-started/felixctl/) works against the same cluster
and covers more: keyed and idempotent publishes, subscribing from an offset,
cache reads and watches, shard owners, creating streams and caches, moving
shards, draining brokers and benchmarks.
That page shows how to turn the session file into a `felixctl` context.

## One process, no cluster

If you would rather see the data path than the cluster, the demos embed a
broker in-process and need nothing running:

```bash
cargo run --release -p felix-broker-service --features demo --bin pubsub-demo-simple
```

```
== Felix QUIC Pub/Sub Demo ==
Goal: demonstrate publish/subscribe over QUIC (not cache).
This demo spins up an in-process broker + QUIC server, then runs a client.
Step 1/6: booting in-process broker + QUIC server.
Step 2/6: connecting QUIC client.
Step 3/6: opening a subscription stream.
Subscribe response: Subscribed
Step 4/6: publishing two messages on the same stream.
Step 5/6: receiving events.
Event on demo-topic: hello
Event on demo-topic: world
Shutting down demo.
Demo complete.
```

Others worth running: `cache-demo`, `queue-semantics-demo`,
`durable-restart-demo`, `pubsub-demo-orders`, `latency-demo`. Each is
self-contained and prints what it is proving as it goes.

## Running a broker yourself

```bash
cargo run --release -p felix-broker-service
```

On its own this starts and then stops:

```
INFO felix_broker_service::node: broker started
Error: FELIX_CONTROLPLANE_URL must be set for auth
```

That is deliberate. A broker validates every client token against its tenant's
signing keys, which it fetches from the control plane, and a clustered broker registers itself
there so shards can be placed on it. There is no unauthenticated mode to fall
back to.

So running brokers yourself means running a control plane, pointing each broker
at it, and giving each one a node credential:

- **Locally**, `felix-cluster up` does all three, and the session file it writes
  names every address it chose.
- **On Kubernetes**, the Helm chart at `deploy/helm/felix` wires them together.
  See [Kubernetes](/deployment/kubernetes/).
- **With containers**, the images are published on every release and pull
  without credentials. See [Installation](/getting-started/installation/#docker-alternative)
  and [Docker Compose](/deployment/docker-compose/). The broker image
  needs the same control-plane URL and credential as any other broker.

## Using the Rust client

A client connects with a tenant and a Felix token, and it verifies the
broker's certificate. Where each comes from:

- **The tenant and token.** A client token is a Felix token issued by the
  control plane's token exchange (`POST /v1/tenants/{tenant}/token/exchange`)
  for an identity-provider token; see
  [Security](/features/security/#token-exchange-oidc--felix). For a
  `felix-cluster up` cluster, the session file holds a ready-made one
  (`tenant_id`, `namespace`, `client_token`) along with each broker's client
  address.
- **The certificate.** A broker without `FELIX_TLS_CERT` generates a
  self-signed `localhost` certificate at every start. `FELIX_TLS_CERT_EXPORT`
  writes it to a file the client can trust. A broker with a certificate from a
  public CA needs no roots at all: pass `None` to use the platform trust store.

### Publish and subscribe

```rust
use std::net::SocketAddr;
use std::sync::Arc;

use felix_client::{Client, ClientConfig, quic_client_config};
use felix_client::AckMode;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Trust the broker's certificate.
    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter("broker-cert.pem")? {
        roots.add(cert?)?;
    }
    let quinn = quic_client_config(Some(Arc::new(roots)), true)?;

    let mut config = ClientConfig::optimized_defaults(quinn);
    config.auth_tenant_id = Some("t1".to_string());
    config.auth_token = Some("<felix token>".to_string());

    let addr: SocketAddr = "127.0.0.1:5000".parse()?;
    let client = Client::connect(addr, "localhost", config).await?;

    let mut subscription = client.subscribe("t1", "ns", "orders").await?;
    tokio::spawn(async move {
        while let Ok(Some(event)) = subscription.next_event().await {
            println!("offset {:?}: {:?}", event.offset, event.payload);
        }
    });

    let publisher = client.publisher().await?;
    for i in 0..10 {
        publisher
            .publish("t1", "ns", "orders", format!("order {i}").into_bytes(), AckMode::PerMessage)
            .await?;
    }
    Ok(())
}
```

The stream must already exist in the control plane. `felix-cluster up`
creates `orders` in tenant `t1`, namespace `ns`. A subscription is served only
by the broker that owns the stream's shard, so connect to the owner that `up`
prints. A publish can go through any broker.

### Cache

```rust
use bytes::Bytes;

// `client` built as above; the token needs cache.read and cache.write.
client
    .cache_put("t1", "ns", "users", "user:123", Bytes::from_static(b"alice"), Some(60_000))
    .await?;

if let Some(value) = client.cache_get("t1", "ns", "users", "user:123").await? {
    println!("cached: {value:?}");
}
```

The cache must exist in the control plane too. `felix-cluster up` creates one
named `users`. [Rust client](/clients/rust/) covers the rest of the API.

## Performance Testing

### Latency Benchmark

Run the latency demo with various configurations:

```bash
# Basic run with defaults
cargo run --release -p felix-broker-service --features demo --bin latency-demo

# Custom configuration
cargo run --release -p felix-broker-service --features demo --bin latency-demo -- \
    --binary \
    --fanout 10 \
    --batch 64 \
    --payload 4096 \
    --total 10000 \
    --warmup 500
```

**Parameters:**

- `--binary`: Use binary batch format (higher throughput)
- `--fanout N`: Number of concurrent subscribers
- `--batch N`: Batch size for publishing
- `--payload N`: Payload size in bytes
- `--total N`: Total messages to send
- `--warmup N`: Warmup messages before measurement

### Cache Benchmark

```bash
cargo run --release -p felix-broker-service --features demo --bin cache-demo
```

Measures put, get-hit and get-miss latency across payload sizes. Variables such as `FELIX_CACHE_BENCH_CONCURRENCY` and `FELIX_CACHE_BENCH_PAYLOADS` shape the run.

## Configuration

The broker and the Rust client each read `FELIX_*` environment variables, and
the broker also reads a YAML file.

### Environment Variables

On the broker, event batching trades latency for throughput:

```bash
export FELIX_EVENT_BATCH_MAX_EVENTS=64
export FELIX_EVENT_BATCH_MAX_DELAY_US=250
```

On the client, `ClientConfig::from_env_or_yaml` reads the connection pools:

```bash
export FELIX_EVENT_CONN_POOL=8
export FELIX_CACHE_CONN_POOL=8
export FELIX_CACHE_STREAMS_PER_CONN=4
```

### Config File

Create `/tmp/felix-config.yml`:

```yaml
quic_bind: "0.0.0.0:5000"
metrics_bind: "0.0.0.0:8080"
event_batch_max_events: 64
event_batch_max_delay_us: 250
pub_conn_recv_window: 16777216
```

Point the broker at it with `FELIX_BROKER_CONFIG=/tmp/felix-config.yml`. It
still needs `FELIX_CONTROLPLANE_URL` and a node credential. See [Running a
broker yourself](#running-a-broker-yourself).

See [Configuration Reference](/reference/configuration/) for all options.

## Using Task

If you have [Task](https://taskfile.dev/) installed, you can use convenience commands:

```bash
# Build
task build

# Run tests
task test

# Format code
task fmt

# Run linter
task lint

# Run demos
task demo:pubsub
task demo:cache
task demo:latency
task demo:queues
task demo:notifications
task demo:orders
task demo:slow-consumer
task demo:state-divergence
task demo:rbac-live
task demo:cross-tenant-isolation
```

See `Taskfile.yml` in the repository root for all available tasks.

## Next Steps

Now that you have Felix running:

- **Explore the Architecture:** [System Design](/architecture/system-design/)
- **Work from the terminal:** [felixctl](/getting-started/felixctl/)
- **Learn the APIs:** [Broker API](/api/broker-api/)
- **Tune Performance:** [Performance Guide](/features/performance/)
- **Deploy Properly:** [Deployment Guides](/deployment/local/)
- **Contribute:** [Development Guide](/development/contributing/)

## Troubleshooting

### Port Already in Use

If port 5000 or 8080 is in use:

```bash
export FELIX_QUIC_BIND="0.0.0.0:5001"
export FELIX_BROKER_METRICS_BIND="0.0.0.0:8081"
```

The metrics variable is prefixed because the control plane has one of its own.
Set `FELIX_METRICS_BIND` and the broker warns that nothing reads it rather than
silently keeping the default.

`felix-cluster up` picks free ports for every process, so it has nothing to
collide with.

### Build Errors

Ensure you have Rust 1.97.1 or later:

```bash
rustc --version
# Should show: rustc 1.97.1 or higher
```

Update if needed:

```bash
rustup update
```

### Connection Refused

Make sure the broker is running and listening:

```bash
lsof -i :5000
```

A broker that exits right after logging `broker started` is missing its
control-plane configuration, not failing to bind. Read the line after it.

See [Troubleshooting Guide](/reference/troubleshooting/) for more help.
