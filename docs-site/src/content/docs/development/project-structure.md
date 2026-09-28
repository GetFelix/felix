---
title: "Project Structure"
---

Understanding Felix's repository layout, crate organization, and architectural conventions.

## Repository Overview

```
felix/
├── crates/              # Rust crates (libraries)
├── services/            # Runnable services (binaries)
├── docs/                # Design and architecture docs
├── docs-site/           # User documentation (Astro Starlight)
├── scripts/             # Build and automation scripts
├── docker/              # Docker configuration
├── .github/             # GitHub Actions workflows
├── githooks/            # Git hooks for development
├── Cargo.toml           # Workspace manifest
├── Cargo.lock           # Dependency lock file
├── Taskfile.yml         # Task runner configuration
├── deny.toml            # Dependency audit rules
└── rust-toolchain.toml  # Rust version specification
```

## Crates Directory

The `crates/` directory contains all library crates following a modular architecture:

```
crates/
├── felix-broker/        # Broker core (pub/sub, cache, fanout)
├── felix-wire/          # Wire protocol and framing
├── felix-transport/     # QUIC transport abstraction
├── felix-storage/       # Storage layer (ephemeral + durable)
├── felix-client/        # Client SDK
├── felix-common/        # Shared types and utilities
├── felix-router/        # Region-aware routing
├── felix-replication/   # Broker-to-broker transport and log replication
├── felix-authz/         # Authentication and authorization
└── felix-conformance/   # Wire protocol conformance tests
```

### Core Crates

#### felix-broker

**Purpose**: Broker core logic for pub/sub, cache and consumer groups.

**Responsibilities**:
- Stream registry and subscription management
- Event fanout and batching
- Cache storage with TTL
- Consumer groups: claims, redelivery, dead letters
- Backpressure and flow control
- Connection lifecycle management

**Key modules**:
- `broker.rs`: The `Broker` struct, its construction and accessors. What it does is split under `broker/`:
  - `registry.rs`: Tenant / namespace / stream / cache registries
  - `shards.rs`: Resolving a stream shard to its state, and `StreamHandle`
  - `publish.rs`: The publish path — claim offsets, wait for durability, append, fan out
  - `subscribe.rs`: Live subscriptions, resuming from a position, and paging history off disk
  - `shard_logs.rs`: `LogKind` and the hooks replication calls
  - `metadata.rs` / `keys.rs`: Stream and cache metadata; map keys plus their borrowed lookup twins
- `stream/`: One shard in memory — `state.rs` (subscriber registry, publish snapshot, replay ring), `delivery.rs` (shared delivery batches, queue-depth accounting, `SubQueuePolicy`), `subscription.rs` (receive handles and the unregister guard), `producers.rs` (idempotent producer sequences)
- `cache/watch.rs`: Cache-watch fanout over a cache shard's write order
- `queue/`: Consumer groups — `reader.rs` joins the stream's log, the cursor and the tracker into poll / ack / nack; `cursors.rs` is a group's durable cursor; `tracker.rs` holds in-flight claims, the visibility timeout and attempt counts; `dead_letters.rs` records offsets a group gave up on, as pointers into the stream's log rather than copies
- `durable.rs`: The `DurableStorage` / `StreamLog` seam between the broker and a shard's log
- `replication.rs`: Follower-side acceptance of records a leader shipped
- `error.rs` / `telemetry.rs` / `timings.rs`: `BrokerError`, cfg-gated metrics shims, sampled publish timings

`CommitSequencer`, which holds a publish behind the ones that took their offsets
before it, lives in `felix-storage`.

Apart from the `replication` and `timings` modules, everything public is
re-exported from `lib.rs`, so downstream code addresses these types as
`felix_broker::<Name>`.

**Dependencies**:
- `felix-wire`: Protocol framing
- `felix-storage`: Data persistence

#### felix-wire

**Purpose**: Wire protocol definition and frame encoding/decoding.

**Responsibilities**:
- Frame type definitions
- Binary batch encoding/decoding
- Capability negotiation (frame flags and feature bits)
- Frame validation
- The broker-to-broker protocol

**Key modules**:
- `client/frame.rs`: Protocol constants, `FrameHeader`, and `Frame`
- `client/flags.rs` / `client/features.rs`: Frame-flag bits and feature bits
- `client/message.rs`: The `Message` enum and its JSON codec; the types its fields carry are in `client/message/fields.rs`
- `client/text.rs`: Hand-rolled zero-copy JSON writer for the publish-batch hot path
- `client/binary.rs`: Binary batch codec, one submodule per frame (publish, acked publish, publish ack, event batch)
- `internal.rs`: The protocol brokers speak to each other, split by message family under `internal/`
- `routing.rs`: Which shard a routing key belongs to
- `error.rs`: Wire `Error` type

**Key types**:
- `Frame` / `FrameHeader`: Top-level frame and its 12-byte header
- `Message`: The v1 message enum carried in JSON control frames
- `binary::PublishBatch` / `binary::EventBatch`: Binary batch frame formats

Frame, flag, feature, message, and error items are re-exported at the crate root;
`text`, `binary`, `internal`, and `routing` are addressed through their module paths
(`felix_wire::binary::…`).

**Protocol layers**:
1. **Envelope**: Version, type, length
2. **Binary frames**: Zero-copy fast paths

#### felix-transport

**Purpose**: QUIC endpoints and connections, and the transport tuning shared by
the client and the broker.

**Responsibilities**:
- QUIC client/server endpoint setup
- Connection and stream lifetime
- Flow-control, MTU, and UDP socket configuration
- Dedicated I/O runtimes for quinn's driver tasks

TLS is configured by the caller: endpoints are built from a quinn server or client
config. Connection pooling lives in `felix-client`, not here.

**Key modules**:
- `server.rs` / `client.rs`: `QuicServer` and `QuicClient`
- `connection.rs`: `QuicConnection`, `ConnectionId`, `ConnectionInfo`
- `config.rs`: `TransportConfig`, its defaults and environment overrides;
  `config/quinn_settings.rs` turns it into quinn settings and `config/loopback.rs`
  holds the loopback MTU pin
- `socket.rs`: UDP socket setup and the buffer sizes the OS actually granted
- `io_runtime.rs`: the runtime pool quinn's driver tasks run on

**Key types**:
- `QuicClient`: Client-side endpoint
- `QuicServer`: Server-side listener
- `QuicConnection`: An established connection; opens and accepts streams
- `TransportConfig`: Transport configuration

**Based on**: `quinn` (QUIC implementation)

#### felix-storage

**Purpose**: Storage layer abstraction for ephemeral and durable data.

**Responsibilities**:
- Ephemeral in-memory storage
- Durable WAL and log segments
- TTL management
- Retention policies
- Compaction for the cache and counter logs; stream logs are not compacted

**Storage types**:
- `EphemeralStore`: In-memory with TTL
- `DurableStore`: persistent log-structured segment store
- `CacheStore`: Key-value with expiration

#### felix-client

**Purpose**: Rust client SDK for Felix.

**Responsibilities**:
- Publish API
- Subscribe API
- Cache operations (put/get)
- Connection management
- Stream pooling
- Error handling

**Key types**:
- `Client`: Main client interface
- `Publisher`: Publishing handle
- `Subscription`: Subscription handle
- `ClientConfig`: Client configuration

**Key modules**:
- `client.rs`, `client/`: `Client`, with its API split by area
- `connection.rs`, `connection/`: where pooled connections go, stream authentication, event stream routing
- `publish.rs`, `publish/`: `Publisher`, its writer tasks, admission, acks, and the idempotent producer
- `subscribe.rs`, `subscribe/`: `Subscription` and the pipeline that feeds it
- `cache.rs`, `cache/`: cache workers and cache watches
- `cluster.rs`, `cluster/`: `ClusterClient` and the sharded subscription, group and watch views
- `config.rs`, `config/`: `ClientConfig`, its defaults, and the env and YAML overrides

**Example usage**:
```rust
use felix_client::{Client, ClientConfig};
use felix_wire::AckMode;
use std::net::SocketAddr;

let quinn = quinn::ClientConfig::with_platform_verifier();
let config = ClientConfig::optimized_defaults(quinn);
let addr: SocketAddr = "127.0.0.1:5000".parse()?;
let client = Client::connect(addr, "localhost", config).await?;
let publisher = client.publisher().await?;
publisher
    .publish("tenant", "namespace", "stream", b"data".to_vec(), AckMode::None)
    .await?;
```

### Supporting Crates

#### felix-common

**Purpose**: What two crates that do not depend on each other must agree on exactly.

**Contents**:
- `membership.rs`: The broker-to-control-plane membership shapes
- `env_registry.rs`: Every `FELIX_*` variable the workspace reads
- `lifecycle.rs`: Termination signals, readiness gating and bounded drain (feature `lifecycle`)
- `ids.rs` / `error.rs`: `RegionId` and its parse error

**Principle**: Minimal dependencies, stable API.

#### felix-router

**Purpose**: Which node serves a shard, and whether traffic may reach it.

**Contents**:
- `shard.rs` with `shard/table.rs` and `shard/router.rs`: The routing table the control plane's assignments are built into, and `ShardRouter`, which resolves a shard against it
- `region.rs`: `RegionRouter`, the directional region bridge allowlist, and `parse_bridges` for `FELIX_REGION_BRIDGES`

#### felix-replication

**Purpose**: Brokers talking to brokers.

**Contents**:
- `peer.rs` / `peer/`: The broker-internal transport — connection pool, listener, mTLS
- `driver.rs`, `ship.rs`, `quorum.rs`, `rebuild.rs`, `reporter.rs`: Shipping to followers, the `Quorum` wait, and replica reports to the control plane
- `replica.rs`: Applying shipped records as a follower

The broker service plugs in through three traits: `quorum::ShardServing` (its ingress router), `driver::WriteFence` (its shard fence) and `reporter::Credential` (its node credential).

#### felix-authz

**Purpose**: Authentication and authorization.

**Responsibilities**:
- Token-based auth (OIDC exchange + Felix JWTs)
- RBAC policies and permission matching
- Tenant isolation enforcement
- Broker-to-broker mTLS, when certificates are configured

#### felix-conformance

**Purpose**: Wire protocol conformance test suite.

**Responsibilities**:
- Test vector validation
- Cross-implementation testing
- Protocol regression tests

**Usage**:
```bash
cargo run -p felix-conformance
```

## Services Directory

The `services/` directory contains runnable binaries:

```
services/
├── felix-broker-service/        # Broker service
│   ├── src/
│   │   ├── main.rs              # Thin entrypoint: --print-config, then node::run_with_shutdown
│   │   ├── node.rs, node/       # Startup order, readiness gating, the shutdown drain
│   │   ├── serving/             # Client-facing QUIC, auth, forwarding to shard owners
│   │   ├── cluster/             # Membership, lease, credential, catalog sync
│   │   ├── shards/              # Shard ownership: watch, lifecycle, ingress routing
│   │   ├── config.rs, config/   # Configuration loading and validation
│   │   ├── observability.rs, observability/  # Tracing, metrics, sampled timings
│   │   └── bin/soak/            # Resource-leak and lifecycle soak harness
│   ├── Cargo.toml
│   └── README.md                # Performance profiles
└── felix-controlplane-service/  # Control plane service

demos/
├── broker/              # Demo binaries for the broker crate
│   ├── simple_pubsub_demo.rs
│   ├── cache_demo.rs
│   ├── latency_demo.rs
│   ├── notifications_demo.rs
│   └── orders_demo.rs
├── rbac-live/            # End-to-end RBAC mutation demo crate
│   └── src/main.rs
└── cross_tenant_isolation/  # End-to-end tenant isolation demo crate
    └── src/main.rs
```

### Broker Service

**Location**: `services/felix-broker-service/`

**Entrypoint**: `src/main.rs`, which hands off to `node::run_with_shutdown`

**Modules**:
- `node/`: Runs the process as a broker node. `run_with_shutdown` calls the startup steps in order — `storage.rs` (durable storage, cache, consumer groups, counters), `listeners.rs` (QUIC listeners and accept loops), `sync.rs` (catalog sync and the readiness flip), `membership.rs`, `cluster.rs` (shard state, peer transport, shard and replication tasks) — then `shutdown.rs` waits for the signal and drains
- `serving/`: Serving clients — `quic/` (connections, streams, publish and subscribe handlers), `auth.rs`, `forward.rs` (forwarding to a shard's owner), `core_shards.rs`, `group_ops.rs`, `cache_routing.rs`
- `cluster/`: Belonging to a cluster — `membership.rs`, `lease.rs`, `credential.rs`, `catalog_sync.rs`, `node_catalog.rs`, `client_endpoints.rs`
- `shards/`: Owning shards — `watch.rs` (assignments from the control plane), `lifecycle.rs`, `routing.rs` (ingress dispatch)
- `config.rs` / `config/`: `BrokerConfig` from env and YAML, durable storage config, validation
- `observability.rs` / `observability/`: Tracing, the metrics server, sampled timings

**Demo binaries** (see `demos/broker/`):

- **`simple_pubsub_demo.rs`**: Pub/sub demonstration
- **`cache_demo.rs`**: Cache benchmark
- **`latency_demo.rs`**: Latency measurement tool
- **`notifications_demo.rs`**: Multi-tenant notifications workflow demo
- **`orders_demo.rs`**: Orders/payments pipeline demo
- **`rbac-live/`**: Live RBAC policy change demo (control plane + broker + token exchange)
- **`cross_tenant_isolation/`**: Cross-tenant isolation demo (Postgres + control plane + broker)

## Documentation

### Design Docs (`docs/`)

Architecture and design documentation:

```
docs/
├── architecture.md      # System architecture overview
├── demos.md             # Demo catalog and run instructions
├── protocol.md          # Wire protocol specification
├── control-plane.md     # Control plane design
├── semantics.md         # Delivery semantics
├── design.md            # Product design notes
├── broker-config.md     # Broker configuration
├── client-config.md     # Client configuration
├── todos.md             # The original MVP checklist (historical)
└── assets/              # Diagrams and images
    └── logo.PNG
```

**Purpose**: Technical design for contributors.

### User Docs (`docs-site/`)

User-facing documentation (Astro Starlight):

```
docs-site/
├── src/
│   ├── content/
│   │   └── docs/
│   │       ├── index.md
│   │       ├── getting-started/
│   │       ├── architecture/
│   │       ├── api/
│   │       ├── features/
│   │       ├── deployment/
│   │       ├── reference/
│   │       └── development/
│   └── content.config.ts
├── astro.config.mjs     # Site and navigation configuration
└── package.json
```

**Purpose**: End-user guides and API references.

**Build**:
```bash
cd docs-site
npm install
npm run dev
```

## Scripts Directory

Automation and utility scripts:

```
scripts/
└── perf/                # Performance benchmarking
    ├── run_latency_matrix.py
    ├── normalize_and_aggregate.py
    ├── make_charts.py
    ├── render_markdown_snippets.py
    └── presets.yml      # Benchmark configurations
```

## Docker Directory

Docker build configuration:

```
docker/
├── broker.Dockerfile           # Multi-stage broker build
├── controlplane.Dockerfile     # Control plane build
├── prometheus/
│   ├── prometheus.yml          # Prometheus config
│   └── prometheus.Dockerfile
└── otel-collector/
    ├── config.yml              # OTEL config
    └── otel-collector.Dockerfile
```

## GitHub Workflows

CI/CD configuration:

```
.github/
├── workflows/
│   ├── ci.yml           # Main CI pipeline
│   └── coverage.yml     # Code coverage
└── dependabot.yml       # Dependency updates
```

## Configuration Files

### Cargo.toml (Workspace)

**Purpose**: Define workspace and shared dependencies.

```toml
[workspace]
members = [
    "crates/*",
    "services/*",
]
resolver = "2"

[workspace.dependencies]
tokio = { version = "1.35", features = ["full"] }
anyhow = "1.0"
# ... shared dependencies
```

### rust-toolchain.toml

**Purpose**: Pin Rust version for consistency.

```toml
[toolchain]
channel = "1.97.1"
components = ["rustfmt", "clippy"]
```

### deny.toml

**Purpose**: Configure cargo-deny for dependency auditing.

**Checks**:
- Security vulnerabilities
- License compliance
- Banned crates
- Duplicate dependencies

### Taskfile.yml

**Purpose**: Define common development tasks.

**Tasks**: build, test, lint, fmt, coverage, demos, etc.

## Naming Conventions

### Crate Names

- **Library crates**: `felix-<component>` (e.g., `felix-broker`)
- **Binary crates**: Service name (e.g., `broker`)
- **All lowercase**, hyphen-separated

### Module Structure

```
crate_root/
├── lib.rs              # Public API (library)
├── main.rs             # Entrypoint (binary)
├── module_name.rs      # Single-file module
└── module_name/        # Multi-file module
    ├── mod.rs          # Module root
    ├── submodule.rs
    └── tests.rs        # Module tests
```

### File Naming

- **Snake_case**: `my_module.rs`
- **Tests**: `mod_tests.rs` or `tests/`
- **Binaries**: `bin/my_app.rs`

### Type Naming

- **PascalCase**: Structs, enums, traits (`BrokerConfig`, `FrameType`)
- **snake_case**: Functions, methods, variables (`publish_event`, `config_value`)
- **SCREAMING_SNAKE_CASE**: Constants (`DEFAULT_PORT`, `MAX_BATCH_SIZE`)

## Dependency Management

### Dependency Categories

**Core dependencies**:
- `tokio`: Async runtime
- `quinn`: QUIC implementation
- `serde`: Serialization
- `anyhow`/`thiserror`: Error handling

**Development dependencies**:
- `serial_test`: Test isolation
- `tempfile`: Temporary files in tests
- `criterion`: Benchmarking

### Dependency Rules

1. **Minimize dependencies**: Only add when necessary
2. **Pin versions**: Use exact versions in workspace
3. **Audit regularly**: Run `cargo-deny check`
4. **No unmaintained crates**: Check maintenance status
5. **License compliance**: Only Apache-2.0 / MIT

### Adding Dependencies

```bash
# Add to workspace
cargo add --workspace <crate>

# Add to specific crate
cargo add -p felix-broker <crate>

# Add dev dependency
cargo add --dev <crate>
```

## Testing Structure

### Test Organization

**Unit tests**: Inline in source files
```rust
#[cfg(test)]
mod tests {
    use super::*;
    // Tests here
}
```

**Integration tests**: `tests/` directory
```
crate/
  tests/
    integration_test.rs
    common/
      mod.rs           # Shared test utilities
```

**Conformance tests**: Separate crate (`felix-conformance`)

### Test Naming

- **Functions**: `test_<what_it_does>`
- **Modules**: `tests` or `<module>_tests`
- **Files**: `integration_test.rs`, `e2e_test.rs`

## Build Artifacts

### Target Directory

```
target/
├── debug/              # Debug builds
│   ├── broker          # Binary
│   ├── deps/           # Dependencies
│   └── build/          # Build scripts
├── release/            # Release builds
└── doc/                # Generated docs
```

### Cargo Cache

```
~/.cargo/
├── registry/           # Downloaded crate sources
├── git/                # Git dependencies
└── bin/                # Installed binaries
```

## Development Environment

### Recommended Setup

**Editor**: VS Code with rust-analyzer

**Extensions**:
- rust-analyzer: IntelliSense
- CodeLLDB: Debugging
- Better TOML: TOML syntax

**.vscode/settings.json**:
```json
{
    "rust-analyzer.cargo.features": "all",
    "rust-analyzer.checkOnSave.command": "clippy"
}
```

### Git Hooks

```bash
# Point git at the repo's hooks
bash scripts/setup-githooks.sh
```

- **pre-commit**: `cargo fmt --check`, so a commit stays instant.
- **pre-push**: `task lint`, which covers formatting, workspace clippy with `-D warnings`, and the per-crate feature checks.

Tests are not run by either hook; CI runs them.

## Code Organization Principles

### Modularity

- **Small, focused crates**: Each crate has a single purpose
- **Clear boundaries**: Minimal cross-crate dependencies
- **Public API**: `pub` means "someone outside this crate uses this."
  Everything else is `pub(crate)`, and the `unreachable_pub` lint (enforced
  workspace-wide, promoted to an error by CI's `-D warnings`) catches drift.

### Module style

One style throughout: a module `foo` is `foo.rs` with its submodules in
`foo/` — never `foo/mod.rs`. Clippy's `mod_module_files` lint enforces it
workspace-wide. The single exception is `tests/common/mod.rs`, which is the
standard Cargo pattern for helpers shared between integration-test binaries.

### Dependency versions

Shared dependencies are declared once in the root `[workspace.dependencies]`
and inherited with `dep = { workspace = true }`; a member adds features on
top when it needs them. A version bump is one edit, and two crates cannot
drift onto different versions of the same dependency.

### Layering

```
┌─────────────────────────┐
│   Services (binaries)   │
├─────────────────────────┤
│    Application Layer    │
│  (broker, client, etc)  │
├─────────────────────────┤
│     Protocol Layer      │
│   (wire, transport)     │
├─────────────────────────┤
│    Foundation Layer     │
│  (common, storage)      │
└─────────────────────────┘
```

### Dependency Direction

Dependencies flow **downward**:
- Services depend on applications
- Applications depend on protocol
- Protocol depends on foundation
- Foundation has minimal dependencies

**Never**: Lower layers depend on upper layers.

## Future Structure Changes

New capabilities may add crates, language SDKs, and deployment packaging
(Helm charts, operators). The core structure — `crates/` for libraries,
`services/` for binaries, demos outside the workspace — will remain stable.
Placeholder crates are not kept around: a crate exists when something uses
it.

## Next Steps

- **Contributing**: [Contributing Guide](/felix/development/contributing/)
- **Building**: [Building & Testing](/felix/development/building/)
- **Architecture**: [System Design](/felix/architecture/system-design/)
