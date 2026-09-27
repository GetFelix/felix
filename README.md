<p align="center">
  <img src="https://raw.githubusercontent.com/gabloe/felix/main/docs/assets/logo.PNG" alt="Felix logo" width="360" />
</p>
<p align="center">
  <a href="https://github.com/gabloe/felix/actions/workflows/ci.yml">
    <img src="https://github.com/gabloe/felix/actions/workflows/ci.yml/badge.svg" alt="CI status" />
  </a>
  <a href="https://github.com/gabloe/felix/actions/workflows/coverage.yml">
    <img src="https://raw.githubusercontent.com/gabloe/felix/badges/coverage.svg" alt="Coverage" />
  </a>
  <a href="https://github.com/gabloe/felix/blob/main/LICENSING.md">
    <img src="https://img.shields.io/badge/license-AGPL--3.0%20%2B%20Apache--2.0-blue.svg" alt="License: AGPL-3.0 + Apache-2.0 (split, see LICENSING.md)" />
  </a>
  <a href="https://www.rust-lang.org/">
    <img src="https://img.shields.io/badge/rust-1.97.1-blue" alt="Rust 1.97.1" />
  </a>
  <a href="https://gabloe.github.io/felix">
    <img src="https://img.shields.io/badge/Documentation-8A2BE2" alt="Documentation" />
  </a>
</p>

---

Felix is in **early active development**. This README is intentionally brief while the design and
implementation are still moving quickly.

## System Overview

Felix is a low-latency, QUIC-based replicated log designed for high fanout, high
throughput, and predictable tail latency when properly tuned.

Streams, caches, and queues are three semantics over that one log rather than
three subsystems: a stream is the log read forward, a cache is a key → latest-value
projection of it, and a queue is a durable cursor over it. A framed protocol
(felix-wire) over QUIC streams carries all three, with explicit control over
multiplexing, batching, and flow control.

Shards are placed across brokers by the control plane, replicated by leader leases
and log shipping, and survive losing a leader; a publish can be made to wait for a
quorum of the replica set before it is acknowledged.

Two processes run a cluster: brokers, which hold the data and serve clients over
QUIC, and a control plane, which holds the metadata and decides which broker
leads each shard. [ARCHITECTURE.md](ARCHITECTURE.md) maps the code: what each
crate is, where things live, and the invariants that hold across them.

Pub/sub data flow (happy path)
- Client opens a bidirectional control stream to publish/subscribe and receive acks.
- Broker validates scope, enqueues publish jobs, and fans out to subscribers.
- Each subscription has a dedicated unidirectional event stream for delivery.
- Events are sent as single frames or binary batches with count/time-bounded batching.

Cache data flow (current architecture)
- Client maintains a cache connection pool with long-lived stream workers.
- Cache requests carry a `request_id` and are multiplexed over these streams.
- Broker processes request frames in a read loop and replies on the same stream.
- This avoids per-request stream setup costs and improves tail latency under concurrency.

Performance

Felix is tuned end-to-end: QUIC transport (path MTU discovery, congestion
window, socket buffers), a shared-frame fanout path that encodes a publish
batch once regardless of subscriber count, dense stream handles on the
publish hot path, byte-budgeted admission control at both client and broker
ingest, and an opt-in thread-per-core mode (`core_shards`) for stream
ownership. Measured, lossless, with TLS 1.3 always on: sub-millisecond
p999 latency at low fanout, millions of deliveries/sec for small payloads,
and multi-hundred-MB/s sustained for KB-sized payloads at fanout 10. See
[Benchmarks](https://gabloe.github.io/felix/features/benchmarks/) for
current numbers and methodology, and
[Environment Variables](https://gabloe.github.io/felix/reference/environment-variables/) /
[Configuration](https://gabloe.github.io/felix/reference/configuration/) for
the full set of tuning knobs (transport, queue depths/policies, batching,
admission control, core sharding).

- Instrumentation: build with `--features telemetry` to enable per-stage
  timings and frame counters. Default builds compile telemetry out
  (`cfg(feature = "telemetry")`, no runtime branches when disabled) to avoid
  instrumentation overhead on hot paths — validate overhead on your own
  workload before enabling it in production.

Use cases
- Real-time streaming with high fanout and tunable latency/throughput trade-offs.
- Event pipelines with batch publishing and batch delivery for efficient fanout.
- Low-latency caching over QUIC with predictable tail latency under load.

![One append-only log per shard, read three ways: as a stream by offset, as a cache through a key index, and as a queue through a cursor shared by a consumer group.](docs/assets/one-log.svg)

Streams, caches and queues are three readings of the same bytes, not three
subsystems. They share one durability path, one recovery path, one placement
rule and one replication path — which is the point of building it this way.

For how a cluster fits together, see
[`docs/architecture.md`](docs/architecture.md); for what each reading stores and
the test behind every claim, [`docs/projections.md`](docs/projections.md).

## Current Focus

- Control-plane availability and resiliency: readiness that reflects real
  dependencies, drains that a load balancer can act on, and surviving a rolling
  restart
- Hardening the multi-node story — chaos testing and cluster-scale latency
  budgets
- Fanout, backpressure, and isolation as core product behavior
- Protocol and conformance

## Docs

Full documentation site: **https://gabloe.github.io/felix** — architecture,
wire protocol, configuration/environment-variable reference, benchmarks,
and (for contributors) function-by-function internals walkthroughs of the
publish path, subscribe/fanout path, and backpressure/concurrency model.

In the repository:
- `ARCHITECTURE.md` — a map of the code, for anyone about to change it
- `CONTRIBUTING.md` — how to contribute, and how the code is organized
- `docs/architecture.md` — system architecture
- `docs/protocol.md` — wire protocol specification
- `docs/control-plane.md` — control plane. Opens with the original Raft sketch, marked as such, then points at the design that was actually built
- `docs/semantics.md` — delivery semantics and guarantees
- `docs/design.md` — product and protocol design notes
- `docs/auth.md` — authentication and authorization
- `docs/kafka-compatibility.md` — what the Kafka listener speaks, and why it stops where it does
- `docs/broker-config.md`, `docs/client-config.md` — config field reference with example profiles
- `docs/demos.md` — demo binaries and what each one shows
- `docs/todos.md` — the original MVP checklist, kept as a historical record

The project is intentionally building depth before breadth: defining a
stable wire envelope and internal data model, and measuring
latency/backpressure behavior early to keep p99/p999 predictable.

---

## What works today

- Multi-broker clusters, with every shard of every stream and cache placed on
  one owner by rendezvous hashing
- Durable log-structured storage: segments, sparse indexes rebuilt rather than
  trusted, torn-tail repair, and a refusal to start on interior corruption
- Replication with leader leases, `Leader` or `Quorum` acknowledgement, and
  failover to a replica that actually holds the log
- A log-backed cache, routed to one owner per key and replicated
- Consumer groups: poll, acknowledge, redeliver, bound the redelivery,
  dead-letter and redrive
- A control plane over REST, tenant-scoped tokens with RBAC, and capability
  negotiation on the wire. Its metadata store is Postgres or an embedded Raft
  group (`FELIX_CONTROLPLANE_STORAGE_BACKEND=raft`), so availability need not
  rest on an external database
- Online rebalancing: a shard with a live leader is moved by staging the
  destination as a replica, fencing the leader and cutting over once the copy
  is level. Draining a broker and adding one both work this way, and an
  operator can start, cancel and pause moves. The switch-over takes tens of
  milliseconds; publishes arriving during it are held and forwarded rather
  than refused, and subscriptions follow the shard to its new owner
- Kafka wire compatibility: with `FELIX_KAFKA_LISTEN` set, Kafka producers
  (idempotent ones included, with the guarantee holding across a failover) and
  consumers that assign their own partitions work against durable streams.
  Consumer groups and transactions are refused with an error that says why, so
  Connect, Streams and ksqlDB do not work. See
  [Kafka compatibility](https://gabloe.github.io/felix/features/kafka/)
- Mutually authenticated broker-to-broker QUIC, with each certificate's name
  checked against the node id in both directions
  (`FELIX_INTERNAL_TLS_CERT` / `_KEY` / `_CA`). A cluster member refuses to
  start without them unless `FELIX_INTERNAL_ALLOW_UNAUTHENTICATED=true`
- Operator-supplied client-facing certificates (`FELIX_TLS_CERT` / `_KEY`,
  optional client certificates with `FELIX_TLS_CLIENT_CA`), re-read on
  rotation, and optional TLS on the control-plane API
  (`FELIX_CONTROLPLANE_TLS_CERT` / `_KEY`)

## What does not exist yet

- Per-stream retention: a policy is recorded on the stream and nothing reads it.
  Retention itself works, but it is configured per broker
  (`FELIX_DURABLE_RETENTION_BYTES` / `_SECONDS`) and is off unless set, so by
  default a log grows until the disk does
- Tiered storage, cross-region bridges, encryption at rest, and audit logging
- Clients beyond Rust, Python and TypeScript. All three wrap the same
  implementation and publish under the same name — `felix-client` on
  crates.io, PyPI and npm — and the conformance catalogue is what the next
  language is gated on

The [status table](https://gabloe.github.io/felix/getting-started/what-felix-is-for/)
is kept current per capability and is the page to trust when another disagrees.

---

## Getting Started

Build the workspace:

```bash
cargo build --workspace
```

Run the broker service:

```bash
cargo run -p felix-broker-service
```

Run the wire protocol conformance runner:

```bash
cargo run -p felix-conformance
```

With no arguments the conformance runner drives a real broker over QUIC and
checks publish, subscribe and cache behaviour against the protocol. With
`verify <results.json>` it checks a client's results against the scenario
catalogue every Felix client is held to, which is how the Python and TypeScript
clients prove they behave like the Rust one.

Felix runs as a cluster of brokers over a control plane, and as a single broker for development. Neither has been run in production by anyone.

---

## Design Discipline

Felix intentionally prioritizes:
- Fanout + backpressure + isolation over unified feature bundles
- Clear invariants over feature count
- Explicit boundaries over implicit behavior
- Measured performance over assumptions

If a feature cannot be enforced in code, it is considered incomplete.

---

## Roadmap (Condensed)

Done: QUIC transport with backpressure, the durable log, the control plane and
placement, intra-region clustering with replication and failover, the log-backed
cache, consumer groups, tenant-scoped RBAC, control-plane high availability
over either Postgres or an embedded Raft group, broker-to-broker mTLS, a Helm
chart, and Python and TypeScript clients over the Rust one.

Next, roughly in order:

- Per-stream retention, so a stream's declared policy is the one enforced
- Moving a shard without pausing its publishes or ending its subscriptions
- Tiered storage and cold-tier reads
- Explicit cross-region bridges
- Encryption at rest, audit logging, and the compliance surface around them

Detailed plans live in `docs/`, and the per-capability status table on the docs
site is the authority.

---

## License

Felix uses a split license: the wire protocol (`felix-wire`), transport layer
(`felix-transport`), client SDK (`felix-client` and its Python and TypeScript
bindings), and the shapes the services share (`felix-common`) are Apache-2.0. The broker,
the control-plane server components and the test tooling (including the
conformance kit, which links the broker) are AGPL-3.0:
open source, but running a modified Felix as a network service means
publishing your changes. See [LICENSING.md](LICENSING.md) for the full
breakdown and rationale.

Copyright (c) 2026 Felix Authors
