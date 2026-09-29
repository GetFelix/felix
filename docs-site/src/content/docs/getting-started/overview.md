---
title: "Overview"
---

Felix is a distributed data backend that serves three things most systems run
three products for: event streams, work queues, and a key-value cache. All
three are readings of the same replicated, append-only log, reached over one
QUIC connection.

This page is the short version of how it fits together. The
[status table](/felix/getting-started/what-felix-is-for/) is the page to trust
for what is and is not built.

## The core idea: one log, three readings

Internally there is a single primitive: an append-only log per shard. Each
external API is a different way of reading it:

- **Streams** read by offset. Every subscription keeps its own cursor, so a
  slow reader never holds anyone else back.
- **Queues** read through a cursor a consumer group shares. A record goes to
  one consumer, is redelivered if unacknowledged, and dead-lettered when it
  keeps failing.
- **Cache** reads through a key index over the same log: latest value per key,
  with TTL.

Because it is one log, there is one durability path, one recovery path, one
placement rule and one replication path. There is no second
system, so the cache and the stream cannot drift apart.

## What Felix optimizes for

Felix puts predictable latency under load ahead of peak batch throughput.
Several choices follow from that.

Felix clients use QUIC, so streams multiplex over one connection without
head-of-line blocking, and every connection is TLS 1.3. Queues are bounded and
their overflow policy is explicit, so one slow path degrades locally instead of
cascading. A stalled subscriber loses *its own* events (under the default
`drop_new` policy) rather than stalling the publisher or other subscribers; the
[slow-consumer demo](/felix/demos/slow-consumer-isolation/) runs both policies
side by side. Batching bounds, flow-control windows, pool sizes and fsync policy
are all ordinary configuration.

Measured numbers are kept on the [Benchmarks](/felix/features/benchmarks/) page
so they only need updating in one place.

## The pieces

These are the main pieces. Everything below the dotted line is transport-independent: the broker core has no idea QUIC
exists, which is what makes it testable in-process.

```mermaid
flowchart TB
    subgraph app["Your application"]
        direction LR
        A1(["publisher"])
        A2(["subscriber"])
        A3(["cache client"])
    end

    SDK["felix-client<br/><small>publisher / subscription / cache APIs,<br/>connection + stream pools</small>"]
    WIRE["felix-wire<br/><small>frame header, JSON control messages,<br/>binary data-plane frames</small>"]
    TRANS["felix-transport<br/><small>QUIC endpoints, streams, flow control,<br/>dedicated I/O runtimes</small>"]
    BRK["felix-broker<br/><small>stream registry, log, subscriber<br/>registry, fanout</small>"]
    SVC["services/felix-broker-service<br/><small>network service: handlers, auth,<br/>metrics, control-plane sync</small>"]
    CP["services/felix-controlplane-service<br/><small>tenants, namespaces, streams,<br/>tokens, RBAC</small>"]

    A1 e1@--> SDK
    A2 e2@--> SDK
    A3 e3@--> SDK
    SDK e4@--> WIRE
    WIRE e5@--> TRANS
    TRANS e6@-->|"QUIC + TLS 1.3"| SVC
    SVC e7@--> BRK
    SVC e8@-.->|"seeds metadata at startup"| CP

    e1@{ animate: true }
    e2@{ animate: true }
    e3@{ animate: true }
    e4@{ animate: true }
    e5@{ animate: true }
    e6@{ animate: true }
    e7@{ animate: true }

    classDef step fill:#e8f0fe,stroke:#4a6fa5,color:#1a2b40
    classDef core fill:#fdf0e3,stroke:#b07d3a,color:#3d2a12
    classDef endpoint fill:#e9f5ec,stroke:#4a8a5e,color:#16301f
    class SDK,WIRE,TRANS,SVC step
    class BRK,CP core
    class A1,A2,A3 endpoint
```

- **`felix-wire`** is the protocol: a fixed frame header, JSON control
  messages, binary frames on the data plane, and capability negotiation so
  old and new peers interoperate. Full details in
  [Wire Protocol](/felix/architecture/wire-protocol/).
- **`felix-transport`** wraps QUIC: endpoints, streams, flow-control windows,
  and dedicated I/O runtimes so the transport driver is never starved by
  application tasks.
- **`felix-broker`** is the data plane with no networking in it: the log,
  subscriber registry, fanout, cache index, consumer groups.
- **`felix-storage`** writes the durable log to disk as append-only segments.
  See [Durable Storage](/felix/architecture/durable-storage/).
- **`felix-replication`** ships each shard's log from its leader to its
  followers and counts the majority a `Quorum` write waits for.
- **`felix-client`** is the Rust SDK: publisher, subscription, and cache APIs
  over pooled connections, with reconnection and redirect-following in the
  cluster client. Python (`crates/sdk/felix-python`) and TypeScript
  (`crates/sdk/felix-typescript`) ship too, both as bindings over this client
  rather than reimplementations; Go and C# are not started.
- **The control plane** (`services/felix-controlplane-service`) holds metadata (tenants,
  namespaces, streams, caches, nodes) behind a REST API, and assigns every
  shard to a broker. Brokers watch its assignment feed. It is not on the data
  path: a publish never waits on it.

## Delivery guarantees, plainly

Configured per stream:

- **Ephemeral streams** are at-most-once. The broker acknowledges receipt,
  fans out from memory, and a subscriber that falls behind misses records.
  This is the low-latency mode, and the right one when stale data is worthless
  anyway.
- **Durable streams** persist every record and replay from any retained
  offset. By default a `Leader` stream acknowledges a publish when it is
  queued, before the write, so a crash can still lose an acknowledged record.
  Set `ack_on_commit` to acknowledge only after the write. With
  `consistency: quorum`, the acknowledgement additionally waits until a
  majority of the replica set holds the record, so a failover cannot lose an
  acknowledged write.
- **Consumer groups** redeliver anything unacknowledged, count attempts, and
  park repeat failures as dead letters you can list, discard, or redrive.
- **Idempotent producers** make a re-sent publish land once, but delivery is
  not exactly-once: a consumer can still see a record twice on redelivery, so
  deduplicate there if duplicates matter.

A lost leader is replaced by a replica that provably holds the log. That takes about a
second on a local three-node cluster. This is tested against process kill,
graceful stop, a leader frozen past its lease, and a partitioned broker that
keeps heartbeating.

## Security

Today: TLS 1.3 on every connection, OIDC token exchange at the control plane,
tenant-scoped tokens, RBAC enforced at the broker, and mutually authenticated
broker-to-broker QUIC once `FELIX_INTERNAL_TLS_CERT`, `_KEY` and `_CA` are set.
Not yet: end-to-end payload encryption, encryption at rest, audit logging.
Details in [Security](/felix/features/security/).

## Running it

A single broker is fine for development and for workloads that fit on one
machine. Durable storage and the cache both work on one broker; replication
needs a second one to replicate to.

A cluster is brokers plus a control plane:

![Clients connect to any broker over QUIC. Brokers are peers that forward requests for shards they do not own and replicate the ones they lead. A control plane places shards by rendezvous hashing, and brokers watch its assignment feed. Inside a shard, one append-only log is read as a stream by offset and as a cache through a key index.](/felix/diagrams/architecture.svg)

The control plane keeps its metadata in one of three backends:

| Backend | What holds the metadata | When to pick it |
| --- | --- | --- |
| `memory` | This process, nothing else. The default. | Development, and the harness |
| `postgres` | One external database the instances share | A platform that already runs an HA Postgres |
| `raft` | The control-plane instances themselves, replicated between them | No external database to run: edge sites, appliances, or anywhere Postgres is a burden |

Raft has shipped: the instances form a quorum and survive losing one without
losing an acknowledged write. Postgres remains fully supported; the trade
between the two, and the migration path, are in
[Metadata Raft](/felix/architecture/metadata-raft/) and
[Control-plane HA](/felix/deployment/control-plane-ha/).

Felix runs anywhere a process runs. For orchestrators it ships the pieces
they expect: readiness and liveness endpoints that answer different questions,
and a bounded graceful drain on SIGTERM. See
[Graceful shutdown](/felix/deployment/graceful-shutdown/).

## Is Felix right for your workload?

A good fit: real-time streaming with high fanout, low-latency caching,
work distribution with retries and dead letters, and services that currently
run a broker *and* a cache *and* a queue and would rather run one system.
Services that already talk Kafka can keep their producers and their
partition-assigning consumers: Felix serves the Kafka protocol for those (see
[Kafka compatibility](/felix/features/kafka/)).

A bad fit: petabyte-scale batch pipelines, complex stream processing
(joins, windowing; use Flink or Kafka Streams), or anything that needs a
mature connector ecosystem today. Kafka Connect and Kafka Streams need
consumer groups, which Felix's Kafka listener does not offer. Felix is young
and its ecosystem is small.

The full version of this list, kept current per capability, is
[What Felix Is For](/felix/getting-started/what-felix-is-for/).

## Where to next

- [Quickstart](/felix/getting-started/quickstart/): run a broker and the demos
- [Installation](/felix/getting-started/installation/): build from source
- [System design](/felix/architecture/system-design/): the architecture in depth
- [Broker API](/felix/api/broker-api/): the wire-level API
