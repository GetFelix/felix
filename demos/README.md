# Felix Demos

This directory contains runnable demo binaries and example workflows. The demos are intentionally
self-contained and optimized for clarity over production hardening.

## Broker demos

Located in `demos/broker/` and built from the `felix-broker-service` package:

- `pubsub-demo-simple`: End-to-end QUIC publish/subscribe flow.
- `cache-demo`: Cache put/get workflow over QUIC.
- `latency-demo`: Latency and throughput measurement harness.
- `pubsub-demo-notifications`: Multi-tenant real-time alerts demo.
- `pubsub-demo-orders`: Orders/payments pipeline demo.
- `durable-restart-demo`: Publish, crash, restart, recover — the durability guarantee end to end.

### Run

```bash
cargo run --release -p felix-broker-service --features demo --bin pubsub-demo-simple
cargo run --release -p felix-broker-service --features demo --bin cache-demo
cargo run --release -p felix-broker-service --features demo --bin latency-demo
cargo run --release -p felix-broker-service --bin pubsub-demo-notifications
cargo run --release -p felix-broker-service --bin pubsub-demo-orders
cargo run --release -p felix-broker-service --bin durable-restart-demo
```

### Notes

- Demos rely on demo auth helpers and are not intended for production.
- The broker demos spin up an in-process broker instance for convenience.

## Slow-consumer isolation (start here)

The flagship demo. One telemetry stream, three dashboard consumers, one of which
stops draining mid-run. Runs the identical workload under both subscriber queue
policies and compares them, so the trade-off Felix makes is visible rather than
asserted.

Location: `demos/slow-consumer/`

### Run

```bash
cargo run --release --manifest-path demos/slow-consumer/Cargo.toml
```

Or with Task:

```bash
task demo:slow-consumer
```

Renders a live terminal UI, falling back to plain text automatically when stdout
is not a terminal.

## Local state divergence

The counterpart to the slow-consumer demo. Consumers hold a local copy of a config
keyspace built from a change stream; one stalls, recovers, and is still permanently
wrong once everything settles. Demonstrates a gap rather than a feature.

Location: `demos/state-divergence/`

### Run

```bash
cargo run --release --manifest-path demos/state-divergence/Cargo.toml
```

```bash
task demo:state-divergence
```

## Live RBAC Policy Change (Control Plane Mutation)

Location: `demos/rbac-live/`

Demonstrates that RBAC mutations applied through the control plane immediately
change what the broker allows for the same principal (after reissuing a Felix
access token, since permissions are embedded in tokens).

This demo uses an in-memory control-plane store (no Postgres required).

### Run

```bash
cargo run --manifest-path demos/rbac-live/Cargo.toml
```

Or via Task:

```bash
task demo:rbac-live
```

### Expected output (excerpt)

```
STEP 9 publish denied: PASS
STEP 12 RBAC policies added: PASS
STEP 15 publish allowed: PASS
STEP 17 cache allowed: PASS
```

## Cross-Tenant Isolation (Control Plane + Broker)

Location: `demos/cross_tenant_isolation/`

Demonstrates that tokens minted for one tenant cannot access another tenant's
resources, even with the same upstream identity. Uses a Postgres-backed control
plane, a real broker, and a fake ES256 IdP.

### Run

```bash
cargo run --manifest-path demos/cross_tenant_isolation/Cargo.toml
```

Or via Task:

```bash
task demo:cross-tenant-isolation
```

### Expected output (excerpt)

```
STEP 13 t1 publish allowed: PASS
STEP 16 t1 token on t2 publish denied: PASS
STEP 19 t2 token publish denied: PASS
```
