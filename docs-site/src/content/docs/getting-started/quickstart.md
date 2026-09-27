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
git clone https://github.com/gabloe/felix.git
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
cluster up.

control plane   http://127.0.0.1:52704

node       client                 metrics
broker-0   127.0.0.1:53348        127.0.0.1:52706
broker-1   127.0.0.1:65027        127.0.0.1:52707
broker-2   127.0.0.1:50410        127.0.0.1:52708

placeable: broker-0, broker-1, broker-2
shard ownership:
  stream/t1/ns/orders/0 -> broker-2

holding the cluster. press Ctrl-C to tear it down.
```

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
broker you connect to is a routing detail, not a correctness one — and since
0.5.0 the acknowledgement says when forwarding happened, so a client can see it
is paying to relay every record.

### The rest of the cluster commands

```bash
cargo run --release -p felix-cluster -- smoke        # publish through a non-owner, receive from the owner
cargo run --release -p felix-cluster -- demo         # the cross-broker story, paced for reading
cargo run --release -p felix-cluster -- failover     # kill the leader, keep publishing
cargo run --release -p felix-cluster -- consistency  # what Quorum buys and Leader costs, under a fault
cargo run --release -p felix-cluster -- status       # membership and shard ownership, then exit
```

`up` first for `subscribe` and `publish`; the others start their own cluster.

## One process, no cluster

If you would rather see the data path than the cluster, the demos embed a
broker in-process and need nothing running:

```bash
cargo run --release -p felix-broker-service --features demo --bin pubsub-demo-simple
```

```
== Felix QUIC Pub/Sub Demo ==
Step 1/6: booting in-process broker + QUIC server.
Step 2/6: connecting QUIC client.
Step 3/6: opening a subscription stream.
Subscribe response: Subscribed
Step 4/6: publishing two messages on the same stream.
Step 5/6: receiving events.
Event on demo-topic: hello
Event on demo-topic: world
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
INFO felix_broker: broker started
Error: FELIX_CONTROLPLANE_URL must be set for auth
```

That is deliberate. A broker validates every client token against its tenant's
signing keys, which it fetches from the control plane, and it registers itself
there so shards can be placed on it. There is no unauthenticated mode to fall
back to.

So running brokers yourself means running a control plane, pointing each broker
at it, and giving each one a node credential:

- **Locally**, `felix-cluster up` does all three, and the session file it writes
  names every address it chose.
- **On Kubernetes**, the Helm chart at `deploy/helm/felix` wires them together —
  see [Kubernetes](/felix/deployment/kubernetes/).
- **With containers**, the images are published on every release and pull
  without credentials — see [Installation](/felix/getting-started/installation/#container-images)
  and [Docker Compose](/felix/deployment/docker-compose/). The broker image
  needs the same control-plane URL and credential as any other broker.

## Try the Cache

Run the cache demonstration:

```bash
cargo run --release -p felix-broker-service --features demo --bin cache-demo
```

This benchmarks cache operations (put, get_hit, get_miss) across various payload sizes and measures latency/throughput.

## Using the Client SDK

Here's a minimal example of using the Felix Rust client:

### Publish and Subscribe

```rust
use felix_client::{Client, ClientConfig};
use felix_wire::AckMode;
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Configure client
    let quinn = quinn::ClientConfig::with_platform_verifier();
    let config = ClientConfig::optimized_defaults(quinn);
    let addr: SocketAddr = "127.0.0.1:5000".parse()?;

    // Connect to broker
    let client = Client::connect(addr, "localhost", config).await?;
    let publisher = client.publisher().await?;

    // Subscribe to a stream
    let mut subscription = client
        .subscribe("my-tenant", "my-namespace", "my-stream")
        .await?;

    // Spawn a task to receive events
    tokio::spawn(async move {
        while let Some(event) = subscription.next_event().await.unwrap() {
            println!("Received: {:?}", event.payload);
        }
    });

    // Publish messages
    for i in 0..10 {
        let payload = format!("Message {}", i);
        publisher
            .publish(
                "my-tenant",
                "my-namespace",
                "my-stream",
                payload.into_bytes(),
                AckMode::None,
            )
            .await?;
    }

    Ok(())
}
```

### Cache Operations

```rust
use bytes::Bytes;
use felix_client::{Client, ClientConfig};
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let quinn = quinn::ClientConfig::with_platform_verifier();
    let config = ClientConfig::optimized_defaults(quinn);
    let addr: SocketAddr = "127.0.0.1:5000".parse()?;
    let client = Client::connect(addr, "localhost", config).await?;

    // Put a value with 60-second TTL
    client
        .cache_put(
            "my-tenant",
            "my-namespace",
            "users",
            "user:123",
            Bytes::from_static(b"alice"),
            Some(60_000),
        )
        .await?;

    // Get the value
    if let Some(value) = client
        .cache_get("my-tenant", "my-namespace", "users", "user:123")
        .await?
    {
        println!("Cached value: {:?}", value);
    }

    Ok(())
}
```

:::note[API Surface]
The exact client API is evolving. Check `crates/sdk/felix-client/src/` for the current implementation. The examples above represent the intended ergonomics.
:::
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

Measures cache operations at various payload sizes with configurable concurrency.

## Configuration

Felix can be configured via environment variables or a YAML config file.

### Environment Variables

Key performance tuning variables:

```bash
# Event delivery (pub/sub)
export FELIX_EVENT_CONN_POOL=8
export FELIX_EVENT_BATCH_MAX_DELAY_US=250

# Cache operations
export FELIX_CACHE_CONN_POOL=8
export FELIX_CACHE_STREAMS_PER_CONN=4

# Publishing
export FELIX_PUBLISH_CHUNK_BYTES=16384
```

### Config File

Create `/tmp/felix-config.yml`:

```yaml
quic_bind: "0.0.0.0:5000"
metrics_bind: "0.0.0.0:8080"
event_batch_max_events: 64
event_batch_max_delay_us: 250
cache_conn_recv_window: 268435456
```

Point the broker at it with `FELIX_BROKER_CONFIG=/tmp/felix-config.yml`. It
still needs `FELIX_CONTROLPLANE_URL` and a node credential — see [Running a
broker yourself](#running-a-broker-yourself).

See [Configuration Reference](/felix/reference/configuration/) for all options.

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
task demo:notifications
task demo:orders
task demo:rbac-live
task demo:cross-tenant-isolation
```

See `Taskfile.yml` in the repository root for all available tasks.

## Next Steps

Now that you have Felix running:

- **Explore the Architecture:** [System Design](/felix/architecture/system-design/)
- **Learn the APIs:** [Broker API](/felix/api/broker-api/)
- **Tune Performance:** [Performance Guide](/felix/features/performance/)
- **Deploy Properly:** [Deployment Guides](/felix/deployment/local/)
- **Contribute:** [Development Guide](/felix/development/contributing/)

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

See [Troubleshooting Guide](/felix/reference/troubleshooting/) for more help.
