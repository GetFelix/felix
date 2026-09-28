# Architecture

This is a map of the code: what the parts are, where each one lives, and the rules that hold
across them. It is for someone about to change Felix. For what Felix is and the design behind
it, read [`docs/architecture.md`](docs/architecture.md); for the wire format,
[`docs/protocol.md`](docs/protocol.md).

Names below are crates, modules and types rather than links, so they stay findable with a
search when files move.

## Bird's-eye view

Felix is a replicated log. A stream is the log read forward, a cache is a key-to-latest-value
projection of it, and a queue is a durable cursor over it. All three share one durability path,
one recovery path, one placement rule and one replication path.

Two processes run a cluster:

- **Brokers** hold the data. Each broker owns some shards, writes their logs to disk, fans
  records out to subscribers, and ships committed records to the replicas of the shards it
  leads. Clients talk to brokers over QUIC.
- **The control plane** holds the metadata: tenants, streams, caches, the node catalog, and
  which broker leads each shard. It serves that over REST and decides placement. It carries no
  payload data and never reaches into a broker; brokers read it and act.

A client connects to any broker. If the shard it wants lives elsewhere, the broker either
forwards the request to the owner or tells the client where to go.

## Code map

The workspace is grouped by role. [`crates/README.md`](crates/README.md) has a table of every
crate; this section says what is inside the important ones.

```
crates/protocol/   how peers talk            felix-wire, felix-transport
crates/server/     what the services use     felix-broker, felix-storage, felix-router,
                                             felix-replication, felix-authz, felix-common
crates/sdk/        what applications link    felix-client, felix-python, felix-typescript
crates/testing/    exercising Felix          felix-cluster, felix-conformance, felix-loadgen
services/          the two deployables       felix-broker-service, felix-controlplane-service
```

Dependencies point one way. `felix-wire` and `felix-transport` depend on nothing else here. The
client depends only on those two, which is what keeps it Apache-2.0 and small; it is also all
that is published to crates.io. Nothing on the server side uses the client at run time;
brokers forward to each other over their own protocol (`felix-wire`'s `internal`). The broker
service links the client only for its `soak` tool and tests.

### `felix-wire`: the frame codec

Pure encoding and decoding, no I/O. `client/` is the protocol a client and a broker speak:
the frame header and its flags, the negotiated feature bits, the `Message` enum (JSON control
frames), and `binary/`, the batch formats the hot path uses. `internal/` is a separate protocol
brokers speak to each other, with its own magic number so a misdirected connection fails
loudly. `routing.rs` maps a routing key to a shard. Byte-for-byte fixtures live in `tests/vectors/`.

### `felix-transport`: QUIC

`QuicServer`, `QuicClient` and `QuicConnection` over quinn, plus `TransportConfig` and the
socket and runtime tuning behind it. It knows nothing about Felix messages. `io_runtime.rs`
explains the one sizing rule worth knowing: a server endpoint gets a runtime to itself.

### `felix-broker`: the broker's semantics

Everything a broker means, with no sockets and no control-plane client. Start at `Broker` in
`broker.rs`; its methods are split by concern under `broker/` (`publish`, `subscribe`,
`registry`, `shards`, `shard_logs`). `stream/` is a stream shard's in-memory side: replay ring,
subscriber registry, per-subscriber queues, delivery. `cache/` adds watch fanout to the cache
store, and `queue/` is consumer groups: claims, acknowledgements, redelivery and dead letters.
`replication.rs` is the leader-side seam the broker service drives.

### `felix-storage`: where records are kept

A log-structured segment store. `segment/` is the byte format and one segment file's reader,
writer, scan and index; `disk_log/` is a shard's durable log built from segments, with
recovery, retention, rollover and group commit (`sync.rs`); `io.rs` is the platform layer.
`cache/` and `counter_log.rs` are stores projected from their own logs. `commit_order.rs` is
`CommitSequencer`, which orders publishes rather than storing them.

### `felix-router`, `felix-authz`, `felix-common`

`felix-router` answers which broker serves a shard, from assignments the control plane
publishes, and whether traffic may cross regions. `felix-authz` is tokens and permission
matching. `felix-common` is what the broker and control plane must agree on exactly: the
membership JSON shapes, the registry of every `FELIX_*` variable (`env_registry.rs`), and the
start-up and drain helpers both services use.

### `felix-replication`: brokers talking to brokers

The broker-to-broker transport in `peer/` (connection pool, listener, mTLS) and the log
replication that travels over it: `driver/` ships committed records to each shard's
followers, `replica.rs` stores them on a follower, `quorum.rs` is what a `Quorum` write waits
on, and `reporter.rs` tells the control plane which replicas hold the log. What it needs from
the broker service comes in through three small traits: `quorum::ShardServing`,
`driver::WriteFence` and `reporter::Credential`.

### `felix-broker-service`: a broker node

The process around `felix_broker::Broker`. `node.rs` is the entry point: it starts every
listener and background task and owns the shutdown order. The rest groups into four jobs:

- `serving/` serves clients. `quic/` accepts connections and decodes frames; its
  `streams/control` module is the per-connection control loop that holds auth state and
  dispatches every message, and `handlers/` does the per-message work for publish, subscribe
  and cache watches. `forward/` sends a request to the broker that owns the shard, and `auth/`
  checks each action against the caller's token.
- `cluster/` is belonging to a cluster: membership and heartbeats, the node's credential and its
  refresh, the lease that grants the right to serve, and `catalog_sync`, which keeps the local
  metadata in step with the control plane.
- `shards/` is owning shards: `watch` follows assignments, `lifecycle` is what this broker has
  actually done about them, and `routing` answers whether a shard is local, remote or
  unavailable.
- `node/cluster.rs` wires in `felix-replication`, which ships records to followers and
  applies them on a follower, over its own broker-to-broker transport.

`config/` reads the environment, and `observability/` serves metrics and health.

### `felix-controlplane-service`: the metadata service

`server.rs` is the entry point, and `migrate.rs` the second one (moving metadata between
backends). `model/` is the vocabulary; the rules about an assignment or a node's lifecycle are
stated on those types. `api/` is the REST layer and `auth/` token exchange and JWKS. `store/`
is the `ControlPlaneStore` trait with three backends, in memory, Postgres and embedded Raft
(`raft/` is the consensus seam), held to one shared contract test. `cluster/` makes the
timer-driven decisions: `membership` expires silent nodes, and `placement` decides which broker
leads each shard.

### `felix-client` and the bindings

`Client` in `client.rs` connects to one broker; its API is split by area under `client/`.
`publish/`, `subscribe/` and `cache/` are the machinery behind those calls, `connection/` the
handshake and event routing, and `cluster/` is `ClusterClient`, which follows shard owners and
reconnects. `felix-python` and `felix-typescript` wrap this client rather than reimplement it.

### `felix-cluster` and friends

`felix-cluster` starts a real cluster on one machine, an in-process control plane and several
broker processes, for integration tests and the `felix-cluster` CLI. It runs the prebuilt
`felix-broker` binary, so build that first. `felix-conformance` is the scenario catalogue and
verifier every client is held to. `felix-loadgen` drives a remote cluster for the
real-network performance suite.

## Invariants

These hold across crates. Breaking one is almost always a bug, even when the new order looks
more natural.

- **A publish goes decode, claim offsets, make durable, fan out, in that order.** The broker
  takes offsets from storage before waiting on durability, so a batch claims its place in the
  stream the moment its offsets are consumed; `CommitSequencer` then holds later publishes behind
  earlier ones whether those succeed, fail or are cancelled. Fanout happens only after
  durability. Reading history before registering a subscriber loses any publish landing in
  between, which is why registration comes first.
- **Records on disk are never rewritten.** Recovery therefore trusts that valid bytes end at
  end of file, and preallocation reserves blocks without changing the file's size.
- **A torn tail is repaired; interior corruption stops the broker.** Refusing to start beats
  silently losing acknowledged records.
- **Indexes are derived, never trusted.** A missing, short or stale index is rebuilt from its
  segment.
- **A slow subscriber never blocks a publisher.** Each subscriber has a bounded queue with an
  explicit overflow policy, dropping by default. Durable streams deliver log offsets with each
  event so a consumer can see a drop as a gap.
- **Features are negotiated, not versioned.** A new capability is a feature bit the client
  offers and the broker accepts. An unknown frame flag is rejected, never masked, and
  `ORIGINAL_V1_FLAGS` is frozen. A new optional JSON field defaults to the old behaviour, so an
  old peer and a new one exchange identical bytes.
- **A durable broker serves nothing before it is seeded.** With a control plane configured, it
  neither reports ready nor accepts clients until the catalog is applied and every durable log is
  recovered.
- **A broker serves a shard only with a lease and at the generation the control plane names.**
  The lease is checked on admission and against the clock again before a record commits.
- **Placement is a pure function of a metadata snapshot**, so two control-plane instances
  agree without coordinating.

## Cross-cutting concerns

**Licensing.** Protocol, client, bindings and `felix-common` are Apache-2.0; the rest,
including `felix-conformance`, is AGPL-3.0. [`LICENSING.md`](LICENSING.md) is the authoritative
table and `task publish:check` enforces it, by label and by dependency graph.

**Configuration.** Services read `FELIX_*` environment variables, optionally overlaid by YAML.
Every variable the workspace reads is listed in `felix-common`'s `env_registry.rs`, and CI fails
when code and the reference docs disagree.

**Observability.** Metrics are Prometheus, served by each service. Per-stage latency timings
are compiled in only with the `telemetry` feature, so default builds pay nothing for them.

**Testing.** Unit tests sit next to their module in `<module>/tests.rs`. Each crate's `tests/`
holds integration tests; `felix-cluster`'s run against real broker processes. Wire and storage
formats are fuzzed (`fuzz/` in those crates), and the replication and handoff protocol is
model-checked in TLA+ under `docs/formal/`; CI flags a change to the modelled code that does not
touch the spec. Docs that cite tests are checked too: `task docs:evidence` fails when a cited
test no longer exists.

**Code organization.** The rules for crate layout, module shape and the order of items inside a
file are in [`CONTRIBUTING.md`](CONTRIBUTING.md).
