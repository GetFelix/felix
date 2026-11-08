---
title: "Quickstart"
---

The shortest path to a running Felix cluster with something happening on it.
This page targets `main` (the 0.7 line), so the compose file and the Rust
example below match the code you build from a clone. For 0.6.2 or earlier, use
the stable docs at <https://docs.getfelix.dev/>.

A broker on its own is not a working system: it authenticates every connection
against a control plane, so it needs one to talk to and a credential to present.
Both ways of starting Felix below set all of that up for you, which is why they
come first here rather than last.

## Try it in one command

With Docker or Podman and nothing else, this runs a control plane, a broker and
development credentials from the 0.7 images:

```bash
curl -fsSLO https://raw.githubusercontent.com/GetFelix/felix/main/compose.yaml
docker compose up -d
docker compose logs felix-dev-ready
```

With Podman, use `PODMAN_COMPOSE_PROVIDER=docker-compose podman compose up -d`;
`podman-compose` does not run this file correctly.

The last command prints the connection settings once the broker is ready. Then,
from the same directory, subscribe in one terminal and publish in a second one:

```bash
docker compose run --rm felixctl sub orders
```

```bash
docker compose run --rm felixctl pub orders 'hello'
```

The broker keeps its data on disk in the stack's volume, so `orders` is a
durable stream and the `config` and `stats` caches can be watched, counted in
and read back. The [use cases](/use-cases/overview/) each end with a short "Try
it" that runs against this stack.

The credentials are for your machine only, and the printed settings show how
to use an installed `felixctl` instead. See
[Docker Compose](/deployment/docker-compose/#try-it-in-one-command) for what
the stack runs and why its credentials are safe locally. The rest of this page
builds Felix from source.

**Stop here if you only want to try Felix.** The [tutorials](/tutorials/overview/)
build on this stack and take you from a first stream to a small app.

## Prerequisites

- **Rust** 1.97 or later; the repo pins 1.99.1 and rustup fetches it ([rustup](https://rustup.rs/))
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

session   ${TMPDIR:-/tmp}/felix-cluster.json

holding the cluster. press Ctrl-C to tear it down.
```

The session file sits in your system temp directory (`$TMPDIR`, or `/tmp`). It records the tenant
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

Make a crate for the example. The client API below is the 0.7 one, so depend on
`felix-client` from your clone of the repository. Run this from the directory
that contains your `felix` clone, not from inside it, so the new crate sits
next to the clone:

```bash
cd ..
cargo new orders-demo && cd orders-demo
cargo add felix-client --path ../felix/crates/sdk/felix-client
cargo add tokio --features macros,rt-multi-thread
cargo add anyhow bytes rustls@0.23
```

```rust
use std::net::SocketAddr;
use std::sync::Arc;

use felix_client::{AckMode, ClientConfig, Client, quic_client_config};
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

    // One broker is enough; the client discovers the rest of a cluster.
    let seeds: Vec<SocketAddr> = vec!["127.0.0.1:5000".parse()?];
    let client = Arc::new(Client::connect(&seeds, "localhost", config).await?);

    let mut subscription = client.subscribe("t1", "ns", "orders").await?;
    tokio::spawn(async move {
        while let Ok(Some(event)) = subscription.next_event().await {
            println!("offset {:?}: {:?}", event.offset, event.payload);
        }
    });

    for i in 0..10 {
        client
            .publish("t1", "ns", "orders", format!("order {i}").into_bytes(), AckMode::PerMessage)
            .await?;
    }
    Ok(())
}
```

Replace `127.0.0.1:5000` with a client address from the session file
(`${TMPDIR:-/tmp}/felix-cluster.json`, written by `felix-cluster up`): each
entry of `nodes` has a `client_addr`. Replace `<felix token>` with its top-level
`client_token`. A `felix-cluster` broker writes its certificate to `broker-cert.pem` in its
data directory, and the `cert_file` of each entry names it. Copy one to
`broker-cert.pem` in the crate, or change the path in the code.

The stream must already exist in the control plane. `felix-cluster up`
creates `orders` in tenant `t1`, namespace `ns`. A subscription is served only
by the broker that owns the stream's shard, and a publish is cheapest sent
straight to it. `Client` finds that broker and follows it when it
changes, so any broker `up` prints works as the seed. Use `Client` for
a single broker too; the plain `Connection` is a single-connection building block
for tools (see [Which client to use](/clients/rust/#which-client-to-use)).

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

## Benchmarks, configuration and troubleshooting

- **Benchmarks.** `latency-demo` and current numbers are on [Benchmarks](/features/benchmarks/); `cache-demo` is covered in the [Performance Guide](/features/performance/).
- **Configuration.** The broker and the Rust client read `FELIX_*` environment variables, and the broker also reads a YAML file. Every option is in the [Configuration Reference](/reference/configuration/).
- **Task shortcuts.** `task --list` shows the build, test, lint and demo shortcuts.
- **Troubleshooting.** Port clashes and a refused connection are in [Troubleshooting](/reference/troubleshooting/).

## Where next

- [Tutorials](/tutorials/overview/): from a first stream to a small app
- [felixctl](/getting-started/felixctl/): create streams and publish from the terminal
- [Demos](/demos/overview/): runnable examples of isolation, queues and failover
- [Pub/Sub Streaming](/features/pubsub/), [Queues and Consumer Groups](/features/queues/) and [Distributed Cache](/features/cache/): the three ways to use the log
- [Broker API](/api/broker-api/): the wire-level API
- [Performance Guide](/features/performance/): tuning
- [System Design](/architecture/system-design/): the architecture
- [Deployment](/deployment/local/): run it properly
- [Contributing](/development/contributing/): the development guide
