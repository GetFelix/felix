# felix-client

The Rust client for [Felix](https://github.com/GetFelix/felix): a QUIC pub/sub
broker with durable streams, consumer groups, and a log-backed cache.

The Python and Node packages carry the same name on PyPI and npm, and both are
bindings over this crate rather than reimplementations — so reconnection,
redirect-following, retry classification and offset bookkeeping behave the same
in all three.

```toml
[dependencies]
felix-client = "0.4"
```

```rust
use felix_client::{AckMode, ClientConfig, ClusterClient};
use std::net::SocketAddr;
use std::sync::Arc;

let quinn = quinn::ClientConfig::with_platform_verifier();
let config = ClientConfig::optimized_defaults(quinn);
// One broker is enough; the rest of a cluster is discovered.
let seeds: Vec<SocketAddr> = vec!["127.0.0.1:5000".parse()?];

let client = Arc::new(ClusterClient::connect(&seeds, "localhost", config).await?);

client
    .publish("acme", "prod", "events", b"hello".to_vec(), AckMode::PerMessage)
    .await?;

let mut events = client.subscribe("acme", "prod", "events").await?;
while let Some(event) = events.next_event().await? {
    println!("{:?}", event.payload);
}
```

Use `ClusterClient` whether you run one broker or a cluster: it sends each
record straight to its shard's leader and handles failover and reconnects.
`Client` is the single-connection building block underneath it, for tools and
special cases; in a cluster its publishes are forwarded to the shard's leader,
which cost 1.3× to 4.4× throughput under load
([v0.6.0 performance](https://docs.getfelix.dev/features/v060-performance/)).

## What it handles for you

- **Cluster membership.** `ClusterClient` takes several broker addresses,
  asks whichever it reaches which others exist, and rebuilds its connection
  from the rest when one fails.
- **Shard redirects.** A publish for a shard the broker does not own is
  forwarded and acknowledged by the owner; the ack says so and names the owner.
- **Shard moves.** A `ClusterClient` subscription follows its shard when a
  rebalance moves it to another broker, resuming exactly where it left off on a
  durable stream.
- **Retry classification.** A failure is typed by whether retrying it could
  plausibly succeed, so a caller is not guessing from a message.
- **Idempotent publishes.** `publish_at_least_once` resends; the idempotent
  producer path removes the duplicate the resend would otherwise create.
- **Atomic commits.** `commit` writes an event and state changes as one record
  on an entity's shard, `state_get` reads the state at the commit's version,
  and `CommitError` refuses a commit that would span two logs. See
  [atomic commits](../../../docs/atomic-commit.md).

## Features

- `telemetry` (off) — frame and byte counters, exported as metrics.

## Documentation

- [Rust client guide](https://docs.getfelix.dev/clients/rust/)
- [Delivery semantics](https://docs.getfelix.dev/architecture/semantics/) —
  what is guaranteed, and what is not

Apache-2.0.
