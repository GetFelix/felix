# Felix Demos

This document summarizes the runnable demos under `demos/`. Each demo is a
self-contained binary that starts an in-process broker and QUIC server, then
executes an example workflow. Demos prioritize clarity over production
hardening and are intended for learning, diagnostics, and performance tuning.

## How to run

From the repository root:

```bash
cargo run --release -p felix-broker-service --bin <demo-binary>
```

Or, if you have [Task](https://taskfile.dev/) installed:

```bash
task <demo-task>
```

**Notes**:
- These demos do **not** require a separately running broker.
- Demo auth helpers are enabled; do not reuse them for production.
- Each demo binds to a random localhost port for the in-process broker.

---

## Demo catalog

### Local State Divergence (`demo-state-divergence`)

**What it shows**: what at-most-once delivery costs a consumer maintaining a local
copy of state. A consumer stalls, recovers, everything quiesces — and it is still
permanently wrong about most of the keyspace, with no signal that it is.

This demonstrates a gap rather than a feature. It is the executable form of the
tension described in `docs-site/src/content/docs/getting-started/what-felix-is-for.md`, and it
becomes the acceptance test for gap-free snapshot-plus-stream subscribe when that
lands.

**Run**:

```bash
cargo run --release --manifest-path demos/state-divergence/Cargo.toml
```

```bash
task demo:state-divergence
```

**Optional flags**: `--rate`, `--keys`, `--consumers`, `--payload`,
`--queue-capacity`, `--duration`, `--mode {lossy|lossless|both}`, `--no-tui`.

**What to expect**: under production defaults the stalled consumer ends with most
of its keyspace holding stale values. Configuring every checkpoint to block removes
the divergence — at the cost of the slowest consumer throttling the publisher.

---

### Queue Semantics (`queue-semantics-demo`)

The other way to read the log. A consumer group hands each record to one
consumer, waits to be told it was handled, and takes it back if nobody does.

**Run**

```bash
task demo:queues
# or: cargo run --release -p felix-broker-service --bin queue-semantics-demo
```

**What it shows**

1. Two workers poll one group; no offset is handed to both.
2. A worker dies holding work. Polling returns nothing while the claims are
   live; once the visibility timeout lapses another worker gets them, at
   `attempts = 2`. Nothing is lost — and the demo counts the duplicates,
   because at-least-once is a promise about loss and not about duplicates.
3. A job that always fails is retried to `max_attempts`, dead-lettered, and the
   two jobs queued behind it run anyway. That last part is the point: before
   there was an attempt bound, one poison record stalled a queue for ever.
4. A ledger asserting `published = completed + dead-lettered`.

**Notes**

- No sleeps. `GroupReader::poll` takes the current time as an argument, so the
  demo drives the visibility timeout itself and its output is identical every
  run. That determinism is why `task demo:check` runs it as a behavioural test:
  every guarantee it narrates is an assertion.
- Durable storage is required — a group's cursor is a projection over the
  stream's log, so a non-durable stream serves no groups.
- One shard, one broker. Group state — cursor and dead-letter list alike — is
  replicated with its shard in a cluster; showing that takes the cluster tests
  rather than this single-broker demo.

### Leader vs Quorum (`felix-cluster consistency`)

The same fault put to both consistency levels, on a real three-node cluster.

**Run**

```bash
task cluster:consistency
# slower, to read as it runs: task cluster:consistency -- --pace 3
```

**What it shows**

Two streams identical but for `consistency`, both replicated three ways. The
fault is a leader cut off from its replicas — followers frozen with `SIGSTOP`,
so the leader is healthy and alone. Each stream's own followers are frozen in
turn, so the run does not depend on the two streams sharing a leader.

- **Quorum** refuses the write; the shard stays available. The refused record
  may still be present, because it landed on the leader before the answer came
  back — a refusal means "cannot be vouched for", not "did not happen".
- **Leader** takes the write. The leader is then killed while the replicas are
  still frozen, and **no replica is promoted**: opening the shard would drop a
  record that was acknowledged. The shard is unavailable until the old leader
  returns with its disk.

Neither is data loss. `Leader` trades availability for latency and moves when
you find out — publish time under Quorum, failover time under Leader.

**Notes**

- Asserts both outcomes, and fails if a shard is ever served *without* a record
  its leader acknowledged. That would be silent loss rather than unavailability.
- Recovery is described, not demonstrated: the harness cannot restart a killed
  broker yet.
- The counterpart is `task cluster:failover`, which shows a quorum-acknowledged
  record surviving the loss of the broker that acknowledged it.

### Slow-consumer Isolation (`demo-slow-consumer`)

**What it shows**: that one slow consumer does not degrade the healthy ones, and
what the alternative costs. Runs the same workload twice — once under `drop_new`
(the production default) and once under `block` — and prints them side by side.

This is the demo to run first: it demonstrates the property in Felix's own one-line
description of itself rather than a feature.

**Run**:

```bash
cargo run --release --manifest-path demos/slow-consumer/Cargo.toml
```

```bash
task demo:slow-consumer
```

**Optional flags**: `--rate`, `--subscribers`, `--payload`, `--queue-capacity`,
`--duration`, `--policy {drop_new|block|both}`, `--no-tui`.

**What to expect**: under `drop_new` the publisher holds its target rate and the
healthy consumers lose nothing while the stalled one sheds. Under `block` nothing
is lost anywhere and the publisher is throttled, slowing every consumer to the
speed of the slowest. Neither is correct in general; which you want is a product
decision.

Numbers are single-node loopback at fanout 3. Lost events are gone because this
demo runs an ephemeral stream that drops on overflow; that configuration is
at-most-once by choice, which is what makes the drops visible. A durable stream
replays by offset and a queue redelivers.

---

### Live RBAC Policy Change (`demo-rbac-live`)

Demonstrates a full end-to-end authorization flow using the real control plane,
broker, and token exchange endpoints. It starts a fake ES256 OIDC IdP, bootstraps
tenant configuration, performs publish/subscribe/cache operations that are
initially denied, then applies RBAC updates via the control plane API and shows
the same operations succeed without restarting services.

This demo uses an in-memory control-plane store (no Postgres required).

Run:

```bash
cargo run --manifest-path demos/rbac-live/Cargo.toml
```

Or via Task:

```bash
task demo:rbac-live
```

Expected output includes step-by-step PASS/FAIL markers such as:

```
STEP 9 publish denied: PASS
STEP 12 RBAC policies added: PASS
STEP 15 publish allowed: PASS
STEP 17 cache allowed: PASS
```

### Cross-Tenant Isolation (`demo-cross-tenant-isolation`)

Demonstrates that Felix enforces tenant boundaries end-to-end. The demo boots a
Postgres-backed control plane, a real broker, a fake ES256 OIDC IdP, and then
shows that a token minted for `t1` cannot access `t2` resources. It also shows
that a `t2` token without RBAC grants is denied in `t2`.

Run:

```bash
cargo run --manifest-path demos/cross_tenant_isolation/Cargo.toml
```

Or via Task:

```bash
task demo:cross-tenant-isolation
```

Expected output includes step-by-step PASS/FAIL markers such as:

```
STEP 13 t1 publish allowed: PASS
STEP 16 t1 token on t2 publish denied: PASS
STEP 19 t2 token publish denied: PASS
```

### Pub/Sub Demo (`pubsub-demo-simple`)
**What it shows**:
- QUIC publish/subscribe round-trip
- Basic fanout and per-message acknowledgements

**Run**:
```bash
cargo run --release -p felix-broker-service --features demo --bin pubsub-demo-simple
```

**What to expect**:
- Step-by-step logs (boot broker, connect client, open subscription)
- Two events printed (`hello`, `world`)
- Clean shutdown with "Demo complete"

---

### Cache Demo (`cache-demo`)
**What it shows**:
- Cache `put`, `get_hit`, and `get_miss` operations over QUIC
- Latency/throughput statistics across payload sizes
- TTL sanity check

**Run**:
```bash
cargo run --release -p felix-broker-service --features demo --bin cache-demo
```

**Common tuning env vars**:
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
- Per-payload stats (p50/p99/p999 and throughput)
- A TTL check that confirms expiry behavior

---

### Latency Demo (`latency-demo`)
**What it shows**:
- Pub/sub latency and throughput under configurable fanout, batch sizes, and payloads
- Optional matrix or full sweep execution

**Run**:
```bash
# Single run
cargo run --release -p felix-broker-service --features demo --bin latency-demo

# Customized run
cargo run --release -p felix-broker-service --features demo --bin latency-demo -- \
    --binary \
    --fanout 10 \
    --batch 64 \
    --payload 4096 \
    --total 10000 \
    --warmup 500

# Fairness A/B knobs for throughput-mode cliffs
cargo run --release -p felix-broker-service --bin latency-demo --all-features -- \
    --warmup 200 --total 5000 --payload 256 --fanout 1 --batch 64 \
    --pub-conns 4 --pub-streams-per-conn 2 --pub-stream-count 1 \
    --pub-yield-every-batches 1

cargo run --release -p felix-broker-service --bin latency-demo --all-features -- \
    --warmup 200 --total 5000 --payload 256 --fanout 1 --batch 64 \
    --pub-conns 4 --pub-streams-per-conn 2 --pub-stream-count 1 \
    --sub-dedicated-thread
```

**What to expect**:
- One or more result lines with p50/p99/p999 latencies
- Throughput metrics (overall and per-subscriber)
- Scheduler fairness probes:
  - `--pub-yield-every-batches N` reduces publisher burst monopolization
  - dedicated subscriber drain is on by default (set `FELIX_SUB_DEDICATED_THREAD=0` or `--sub-shared-thread` to opt out)
  - `--sub-dedicated-thread` forces isolation explicitly

---

### Notifications Demo (`pubsub-demo-notifications`)
**What it shows**:
- Multi-tenant fanout and isolation (t1 vs t2)
- Cache snapshots of the last N alerts
- Failure injection via subscriber drop/restart

**Run**:
```bash
task demo:notifications
# or
cargo run --release -p felix-broker-service --bin pubsub-demo-notifications
```

**Optional flags**:
- `--alerts=10` (default: 10)
- `--last-n=5` (default: 5)
- `--drop-subscriber` (restart one subscriber mid-run)

**What to expect**:
- Cross-tenant access denial
- Alert fanout logs per tenant
- Cache snapshot output for `last_alerts`

---

### Orders/Payments Pipeline Demo (`pubsub-demo-orders`)
**What it shows**:
- Multi-stage pipeline: `orders` -> `payments` -> `shipments`
- Idempotent workers (dedupe by event ID)
- Cache-backed last-known state
- Failure injection via worker restart

**Run**:
```bash
task demo:orders
# or
cargo run --release -p felix-broker-service --bin pubsub-demo-orders
```

**Optional flags**:
- `--orders=12` (default: 12)
- `--duplicate-every=5` (default: 5)
- `--kill-worker=payments` (restart payments mid-run)

**What to expect**:
- Step-by-step pipeline logs
- Cache snapshots for each order in `order_state`
- Final summary matching expected processed count
