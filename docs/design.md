

# Felix – Comprehensive Design Document

This document captures the *intended technical design* of Felix as it exists today.
It is deliberately opinionated, explicit about trade‑offs, and written to guide
implementation decisions rather than to serve as marketing material.

Felix is being built incrementally. This document describes the **target shape**
of the system, while acknowledging that early milestones will be simpler.

---

## 1. Problem Statement

Modern systems increasingly need a data backend that can simultaneously provide:

- Low‑latency real‑time data distribution
- Message queue semantics for work coordination
- Fast, coherent caching
- Durable event logs for replay and recovery
- Strong data sovereignty and residency guarantees
- Clear, enforceable security boundaries

Today, these needs are typically met by stitching together multiple systems
(Kafka, Redis, RabbitMQ, Service Bus, etc.). This creates:
- Operational complexity
- Inconsistent semantics
- Fragile data‑sovereignty guarantees
- Hard‑to‑audit cross‑system behavior

Felix exists to collapse these concerns into a **single, coherent system**
without sacrificing performance or security.

---

## 2. Design Philosophy

### 2.1 Sovereignty First (target design, not yet implemented)
Felix's target design treats **data sovereignty as a core architectural
constraint**, not an operational afterthought. This is a design goal for a
later milestone, not current behavior. What is enforced today is placement and
forwarding: a stream created with a `region` has every copy in that region or
one `FELIX_REGION_BRIDGES` bridges it to, and a broker forwards only to leaders
in regions it has a bridge to (`docs/control-plane.md`, "Regions"). There is no
bridge agent, per-region encryption or audit trail. Treat everything below as
intent to be validated, not a guarantee to build on.

- One Felix cluster is intended to represent exactly one sovereign region.
- No implicit cross‑region replication should exist.
- Any cross‑region data movement should be explicit, auditable, and cryptographically isolated.

The goal is that if data is not allowed to leave a region, Felix enforces
that in code. This is not enforced today, and no sovereignty or
compliance claim should be made about Felix until it is.

### 2.2 Unified Core, Multiple Semantics
Felix is built around **one internal abstraction**:
an append‑only log with cursors.

Different user‑visible semantics are projections over this core:

- **Pub/Sub:** independent cursors per subscriber
- **Queues:** shared consumer‑group cursors with acknowledgements
- **Cache:** key → latest value with TTL, backed by the log for invalidation

This avoids the semantic drift that occurs when multiple systems are composed.

### 2.3 Low‑Latency Bias
Felix optimizes for **predictable tail latency** rather than absolute throughput.

Key consequences:
- QUIC as the primary transport
- Optional non‑persistent (ephemeral) streams
- Bounded memory everywhere
- Sharded, localized contention rather than global locks

Durability and batching are opt‑in where they make sense.

### 2.4 Kubernetes as the Substrate
Felix assumes Kubernetes for:
- Process lifecycle
- Networking and service discovery
- Failure detection
- Identity primitives

Felix does not reimplement cluster membership or scheduling logic that Kubernetes
already provides.

---

## 3. High‑Level Architecture

Felix consists of three major planes:

1. **Data Plane (Broker)**
2. **Storage Plane**
3. **Control Plane**

The data plane handles all hot‑path traffic. The control plane is never on the
critical path for publish or consume operations.

---

## 4. Data Plane (Broker)

### 4.1 Responsibilities
The broker is responsible for:
- Accepting client connections
- Routing publish and subscribe requests
- Enforcing delivery semantics
- Managing cursors and acknowledgements
- Applying backpressure

Each broker instance may host:
- Zero or more shard leaders
- Zero or more shard followers

Only shard leaders accept writes.

### 4.2 Shards
A **shard** is the unit of concurrency, replication, and ownership.

Properties:
- Each stream is divided into N shards.
- A shard has exactly one leader at a time.
- Shards are independently replicated and failed over.

Shards are intentionally coarse‑grained to reduce coordination overhead.

### 4.3 Internal Concurrency Model
Felix avoids a single global broker lock.

Instead:
- Each shard owns its own mutable state.
- Connection handlers resolve the shard early.
- Contention is localized to the shard level.

Shared components (routing tables, metadata caches) are read‑mostly and
updated via watch mechanisms.

---

## 5. Transport and Wire Protocol

### 5.1 Transport
Felix uses **QUIC exclusively** for the data plane.

Reasons:
- Built‑in encryption
- Multiplexed streams
- Flow control and backpressure
- No head‑of‑line blocking

### 5.2 Wire Envelope
All messages use a versioned envelope containing:
- Protocol version
- Message type
- Stream and shard identifiers
- Optional compression
- Optional encryption metadata
- Payload

The protocol is designed to be forward‑compatible.

---

## 6. Storage Model

### 6.1 Ephemeral Storage
Ephemeral storage is fully in‑memory and optimized for latency.

Use cases:
- Real‑time fanout
- Caching
- Transient signals

Characteristics:
- No disk writes
- TTL‑based eviction
- At‑most‑once or best‑effort delivery

### 6.2 Durable Storage
Durable storage is implemented as:
- Segmented append‑only log files, each record checksummed
- Sparse indexes for offset lookup
- Crash recovery that repairs a torn tail and refuses to start on corruption
  anywhere else

This is a log‑structured store, **not** write‑ahead logging: the log is the
primary data structure rather than a journal protecting one, so recovery
validates and truncates instead of replaying into a separate store. See
[Durable Storage](durable-storage.md).

Durability is configurable per stream:
- `on_commit`: fsync before the acknowledgement, amortised across concurrent
  publishers by group commit
- `periodic { interval }`: batched fsync on a timer, bounding loss by the
  interval
- `none`: no explicit fsync (survives a process crash, not a power loss)

### 6.3 Retention
Retention policies are enforced per stream:
- Time‑based
- Size‑based
- Compaction (future)

---

## 7. Metadata and Control Plane

### 7.1 Metadata Scope
The control plane stores:
- Stream definitions
- Shard counts
- Placement and leadership
- Retention and quota policies
- Region and bridge configuration

Metadata is **strongly consistent**.

### 7.2 Interaction with Brokers
Brokers:
- Cache metadata locally
- Watch for changes
- Never block the data path on metadata calls

---

## 8. Routing and Placement

Routing is:
- Region‑aware
- Shard‑aware
- Leader‑aware

Clients are routed directly to shard leaders whenever possible.

Region boundaries are enforced here; a broker will not route traffic outside
its configured region unless explicitly bridged.

---

## 9. Consistency and Delivery Semantics

Felix supports tunable guarantees per stream:

- **At‑most‑once:** ephemeral, low‑latency
- **At‑least‑once:** default
- **Exactly‑once:** planned (idempotent producers + transactions)

Replication acknowledgement policies:
- Leader‑only
- Quorum
- All replicas

---

## 10. Security Model

### 10.1 Transport Security
- TLS for client connections
- mTLS for broker‑to‑broker communication

### 10.2 Authorization
- Tenant‑scoped namespaces
- Stream‑level RBAC
- Authorization enforced on publish and subscribe

### 10.3 Encryption
- Envelope encryption per region and tenant
- Optional end‑to‑end encryption (broker routes ciphertext)
- Explicit key rotation and key identifiers

---

## 11. Multi‑Region and Bridges

Felix does not support implicit federation.

Cross‑region replication requires:
- Explicit bridge configuration
- Explicit stream allowlists
- Independent encryption contexts
- Auditable replication paths

Bridges are treated as first‑class, inspectable components.

---

## 12. Failure Model

Felix is designed to tolerate:
- Pod crashes
- Node failures
- Network partitions within a region

Failure handling principles:
- Fast failure detection via Kubernetes
- Shard‑level failover
- Clear trade‑offs between availability and consistency

---

## 13. Non‑Goals

Felix intentionally does not attempt to:
- Replace Kafka’s entire ecosystem
- Optimize for batch analytics workloads
- Support every possible client protocol
- Hide all complexity from operators

Explicitness is preferred over magic.

---

## 14. Implementation Strategy

Felix is built incrementally:

1. Single‑node broker
2. Ephemeral streams and cache
3. Durable storage
4. Metadata and placement
5. Intra‑region clustering
6. Region bridges
7. Security hardening
8. Compliance features

Each step is expected to deliver a usable, testable system.

---

## 15. Guiding Principle

If a property cannot be **enforced in code**, it is not considered part of the design.

This principle applies to sovereignty, security, consistency, and isolation.