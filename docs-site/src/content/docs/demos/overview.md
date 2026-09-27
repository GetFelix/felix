---
title: "Demos Overview"
---

Felix includes a set of runnable demos that showcase core capabilities such as
pub/sub, cache operations, latency benchmarking, and multi-tenant workflows.
Each demo is a **self-contained binary**: it starts an in-process broker and
QUIC server on a random local port, runs the scenario, and exits.

## Quick notes

- You do **not** need a separately running broker for these demos.
- Demo auth helpers are enabled for convenience (not production-safe).
- The RBAC live demo starts a control plane, broker, and fake IdP on local ports.
- All commands are run from the repository root.
- If you use Task, run `task demo:slow-consumer`, `task demo:state-divergence`, `task demo:queues`, `task cluster:consistency`, `task demo:pubsub`, `task demo:cache`, `task demo:latency`, `task demo:notifications`, `task demo:orders`, `task demo:rbac-live`, or `task demo:cross-tenant-isolation`.

## Demo catalog

### Local State Divergence (`demo-state-divergence`)

- The counterpart to the isolation demo: what an at-most-once *configuration*
  costs a consumer that holds a local copy of state. Both demos run an ephemeral
  stream that drops on overflow; a durable stream replays by offset and a queue
  redelivers, so this is a cost you opt into, not Felix's delivery model.
- A stalled consumer recovers, everything settles, and it is still permanently
  wrong about most of the keyspace — with no signal that it is.
- Demonstrates what dropping costs, which is the case for choosing a durable
  stream or a queue when a consumer keeps derived state.
- See [Local State Divergence](/felix/demos/state-divergence/).

```bash
cargo run --release --manifest-path demos/state-divergence/Cargo.toml
```

### Queue Semantics (`queue-semantics-demo`)

- The other way to read the log: a consumer group hands each record to one
  consumer and takes it back if nobody says it was handled.
- Work distribution, redelivery after a worker dies mid-job, an attempt bound,
  and a dead letter — with the two jobs queued behind the poison one running
  anyway, which is the point of the bound.
- Honest about the cost: it counts the redeliveries, because at-least-once is a
  promise about loss and not about duplicates.
- Deterministic — it drives the visibility timeout rather than sleeping — so
  `task demo:check` runs it as a behavioural test.
- See [Queue Semantics](/felix/demos/queue-semantics/).

```bash
cargo run --release -p felix-broker-service --bin queue-semantics-demo
```

### Leader vs Quorum (`felix-cluster consistency`)

- The same fault — a leader cut off from its replicas — put to two streams that
  differ only in `consistency`.
- Quorum refuses the write while the shard stays available. Leader takes it, and
  the shard goes unavailable when the leader dies, because promoting a replica
  would drop a record that was acknowledged.
- Neither is data loss. The demo's point is that `Leader` trades availability
  for latency, and moves when you find out.
- See [Leader vs Quorum](/felix/demos/cluster-consistency/).

```bash
task cluster:consistency
```

### Slow-consumer Isolation (`demo-slow-consumer`)

- The flagship demo: one consumer stalls, the healthy ones carry on.
- Runs the identical workload under both subscriber queue policies and compares
  them, so the trade-off is measured rather than claimed.
- Live terminal UI, with automatic plain-text fallback when stdout is not a TTY.
- See [Slow-consumer Isolation](/felix/demos/slow-consumer-isolation/).

```bash
cargo run --release --manifest-path demos/slow-consumer/Cargo.toml
```

### Live RBAC Policy Change (`demo-rbac-live`)

- Demonstrates live RBAC mutations via the control plane API and immediate
  authorization changes in the broker without restarts.
- Uses a fake ES256 OIDC IdP and the real Felix token exchange flow.
- Exercises publish/subscribe/cache operations before and after RBAC updates.
- Uses an in-memory control-plane store (no Postgres required).

```bash
cargo run --manifest-path demos/rbac-live/Cargo.toml
```

Expected output includes step-by-step PASS/FAIL markers such as:

```
STEP 9 publish denied: PASS
STEP 12 RBAC policies added: PASS
STEP 15 publish allowed: PASS
```

### Cross-Tenant Isolation (`demo-cross-tenant-isolation`)

- Proves tenant boundaries are enforced end-to-end by the broker.
- Uses a Postgres-backed control plane, a fake ES256 IdP, and real token exchange.
- Demonstrates that a `t1` token cannot access `t2` resources.

```bash
cargo run --manifest-path demos/cross_tenant_isolation/Cargo.toml
```

Expected output includes step-by-step PASS/FAIL markers such as:

```
STEP 13 t1 publish allowed: PASS
STEP 16 t1 token on t2 publish denied: PASS
STEP 19 t2 token publish denied: PASS
```

### Pub/Sub Demo (`pubsub-demo-simple`)
**What it does**:
- Demonstrates a basic QUIC publish/subscribe round-trip
- Shows subscription, publishing, and event delivery

**Run**:
```bash
task demo:pubsub
# or
cargo run --release -p felix-broker-service --features demo --bin pubsub-demo-simple
```

**What to expect**:
- Step-by-step logs ending with two events (`hello`, `world`) and "Demo complete"

---

### Cache Demo (`cache-demo`)
**What it does**:
- Benchmarks cache `put`, `get_hit`, and `get_miss` over QUIC
- Reports latency percentiles and throughput
- Performs a TTL sanity check

**Run**:
```bash
task demo:cache
# or
cargo run --release -p felix-broker-service --features demo --bin cache-demo
```

**Useful env vars**:
```bash
FELIX_CACHE_BENCH_WARMUP=200
FELIX_CACHE_BENCH_SAMPLES=2000
FELIX_CACHE_BENCH_PAYLOADS=0,64,256,1024,4096
FELIX_CACHE_BENCH_CONCURRENCY=1
FELIX_CACHE_BENCH_KEYS=1024
FELIX_CACHE_BENCH_OPS=put,get_hit,get_miss
```

**What to expect**:
- A config summary line
- Per-payload stats including p50/p99/p999 and throughput

---

### Latency Demo (`latency-demo`)
**What it does**:
- Measures pub/sub latency and throughput
- Supports fanout, batch size, and payload tuning

**Run**:
```bash
# Basic run
task demo:latency
# or
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

**What to expect**:
- One or more result lines with p50/p99/p999 latencies
- Throughput metrics (overall and per-subscriber)

---

### Notifications Demo (`pubsub-demo-notifications`)
**What it does**:
- Simulates multi-tenant real-time alerts
- Demonstrates tenant isolation and fanout
- Writes "last N" alerts to cache
- Supports subscriber drop/restart

**Run**:
```bash
task demo:notifications
# or
cargo run --release -p felix-broker-service --bin pubsub-demo-notifications
```

**Optional flags**:
- `--alerts=10` (default: 10)
- `--last-n=5` (default: 5)
- `--drop-subscriber`

**What to expect**:
- Cross-tenant access blocked
- Subscriber fanout logs per tenant
- Cache snapshot output for `last_alerts`

---

### Orders/Payments Pipeline Demo (`pubsub-demo-orders`)
**What it does**:
- Implements a three-stage pipeline: `orders` -> `payments` -> `shipments`
- Uses idempotent workers and cache-backed state
- Supports worker restart mid-run

**Run**:
```bash
task demo:orders
# or
cargo run --release -p felix-broker-service --bin pubsub-demo-orders
```

**Optional flags**:
- `--orders=12` (default: 12)
- `--duplicate-every=5` (default: 5)
- `--kill-worker=payments`

**What to expect**:
- Step-by-step pipeline logs
- Cache snapshot output for each order
- Final summary matching expected processed count
