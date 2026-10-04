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
use felix_client::{AckMode, Client, ClientConfig};
use std::net::SocketAddr;

let quinn = quinn::ClientConfig::with_platform_verifier();
let config = ClientConfig::optimized_defaults(quinn);
let addr: SocketAddr = "127.0.0.1:5000".parse()?;

let client = Client::connect(addr, "localhost", config).await?;
let publisher = client.publisher().await?;

publisher
    .publish("acme", "prod", "events", b"hello".to_vec(), AckMode::None)
    .await?;

let mut events = client.subscribe("acme", "prod", "events").await?;
while let Some(event) = events.next_event().await? {
    println!("{:?}", event.payload);
}
```

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

- [Rust client guide](https://getfelix.github.io/felix/clients/rust/)
- [Delivery semantics](https://getfelix.github.io/felix/architecture/semantics/) —
  what is guaranteed, and what is not

Apache-2.0.
