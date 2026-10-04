---
title: "Project Structure"
description: "Where things live in the Felix repository, and the rules that shape the layout."
---

The workspace members are listed in the root
[Cargo.toml](https://github.com/GetFelix/felix/blob/main/Cargo.toml), which is
the authority when this page and the tree disagree. Each group directory under
`crates/` and `services/` has a README saying what it holds.

## Top level

```
felix/
├── crates/
│   ├── protocol/     felix-wire, felix-transport
│   ├── server/       felix-broker, felix-storage, felix-kafka, felix-replication,
│   │                 felix-router, felix-authz, felix-common
│   ├── sdk/          felix-client, felix-python*, felix-typescript*
│   └── testing/      felix-cluster, felix-conformance, felix-loadgen
├── services/
│   ├── felix-broker-service/        builds the felix-broker binary
│   └── felix-controlplane-service/  builds the felix-controlplane binary
├── demos/
│   ├── broker/                      demo binaries of felix-broker-service
│   └── slow-consumer/*, state-divergence/*, rbac-live/*, cross_tenant_isolation/*
├── docs/             design docs, specs, formal/ (TLA+), security/
├── docs-site/        this site (Astro Starlight)
├── docker/           broker, controlplane, prometheus, otel-collector Dockerfiles
├── deploy/helm/felix/
├── scripts/          CI checks, perf pipeline, cluster demo helpers
├── vendor/           vendored crates, recorded in VENDORED.toml
├── githooks/         pre-commit and pre-push
└── .github/workflows/
```

Entries marked `*` declare their own `[workspace]` and are not members:
the two language bindings, which build with maturin and napi-rs, and four demo
crates. `task python:check`, `task ts:check` and `task demo:check` build them.
The fuzz crates under `crates/*/*/fuzz/` are outside the workspace too, because
`cargo-fuzz` needs nightly.

## Crates

Dependencies point one way. `sdk` and `server` depend on `protocol`, `testing`
depends on whatever it exercises, and `protocol` depends on nothing else in the
repository. Only `felix-wire`, `felix-transport` and `felix-client` are
published to crates.io.

**Protocol**

- `felix-wire`: the frame codec. The frame header and flags, the `Message`
  enum, the binary batch formats and the broker-to-broker messages. No I/O.
  [docs/protocol.md](https://github.com/GetFelix/felix/blob/main/docs/protocol.md)
  is its specification.
- `felix-transport`: the QUIC layer. Endpoints, connection and stream lifetime,
  and transport tuning shared by client and broker.

**Server**

- `felix-broker`: the broker's semantics. Streams, caches and consumer groups
  over one log, the publish path (`broker/publish.rs`) and fanout
  (`stream/delivery.rs`). Start at `Broker`.
- `felix-storage`: a log-structured segment store and the caches projected
  from it. See below.
- `felix-kafka`: a read-only Kafka listener that answers Kafka consumers from
  the broker's shard logs.
- `felix-replication`: broker-to-broker transport (`peer/`, including its
  mTLS in `peer/tls.rs`) and log replication (`driver/`, `replica.rs`,
  `quorum.rs`, `promotion.rs`, `reporter.rs`). It does not depend on the broker
  service, which plugs in through small traits.
- `felix-router`: which node serves a shard, from the control plane's
  assignments.
- `felix-authz`: tokens, JWKS and permission matching.
- `felix-common`: what the broker and control plane must agree on exactly:
  `membership`, `fleet`, `clock` (including the test-only clock fault seam),
  `tls`, `lifecycle`, `ids`, `error`, and `env_registry`, the list of every
  `FELIX_*` variable.

**SDK**

- `felix-client`: the Rust client. Start at `Client`.
- `felix-python`, `felix-typescript`: pyo3 and napi bindings over
  `felix-client`.

**Testing**

- `felix-cluster`: a real multi-node cluster on one machine, with fault
  injection and the history checker. A library for tests and a CLI
  (`task cluster:up`). See
  [How Felix Is Tested](/architecture/testing/).
- `felix-conformance`: the client conformance catalogue and verifier.
- `felix-loadgen`: the load generator for the real-network performance suite.

### felix-storage

A segment store, not a WAL. The crate docs in `src/lib.rs` are the map:

- `log.rs`: the `AppendOnlyLog` trait and its types.
- `disk_log.rs` and `disk_log/`: the durable log. `append.rs` (the append path,
  which runs on the log's append thread, and background rollover), `flush.rs`, `sync/` (fsync policy and group
  commit), `segments/` (with `rollover` and `truncation`), `recovery/`
  (startup validation and torn-tail repair), `retention.rs`, and the per-shard
  state files (`durable_mark`, `replica_state`, `epochs`, `producers`).
- `segment.rs` and `segment/`: one segment file. `format` (the byte format),
  `scan` (torn tail or corruption), `reader`, `writer`, `cursor`, and the
  sparse `index`.
- `io.rs`: positioned reads, preallocation and flushes (`F_FULLFSYNC` on
  macOS), and `log_thread`, the per-log threads appends and flushes run on.
- `cache.rs` (`EphemeralCache` and `LogCache`) and `counter_log.rs`: stores
  projected from their own logs, compacted in the background by
  `compaction.rs`.
- `commit_order.rs`: the `CommitSequencer` that orders publishes per log.
- `fault.rs`: the test-only fsync fault seam.

[docs/durable-storage.md](https://github.com/GetFelix/felix/blob/main/docs/durable-storage.md)
and
[docs/storage-format.md](https://github.com/GetFelix/felix/blob/main/docs/storage-format.md)
describe the design.

## Services

`services/felix-broker-service` is the broker process. `serving/` is the client
side (`quic/` decodes frames and holds the publish and subscribe handlers, plus
`auth`, `forward`, `kafka` and `limits`). `cluster/` talks to the control plane
(`lease`, `membership`, `credential`, `catalog_sync`). `node.rs` and `node/`
wire it together and gate readiness on the control-plane seed. `shards/` covers shard lifecycle and
routing, `restore.rs` is the `felix-broker restore-point` subcommand, and
`observability/` serves metrics. It also builds the `demos/broker/*` binaries and the `soak` harness.

`services/felix-controlplane-service` is the REST control plane: `api/`,
`store/` (memory, Postgres or Raft), `raft/`, `cluster/` (membership and
placement), `auth/` and `admin/` (the `felix-controlplane admin` CLI).

## Demos

`demos/broker/` holds the demos that run an in-process broker, built as
binaries of `felix-broker-service`: `simple_pubsub_demo`, `cache_demo`,
`latency_demo`, `notifications_demo`, `orders_demo`, `durable_restart_demo` and
`queue_semantics_demo`.

`demos/slow-consumer`, `demos/state-divergence`, `demos/rbac-live` and
`demos/cross_tenant_isolation` are standalone crates outside the workspace.
Run `task demo:check` after changing a public API they might use.

## Docs, deployment and CI

- `docs/`: design docs and specifications, including `protocol.md`,
  `durable-storage.md`, `storage-format.md`, `replication-design.md`,
  `cluster-harness.md`, `history-checker.md`, `kafka-compatibility.md`, the
  TLA+ models in `formal/` and the security reviews in `security/`.
- `docs-site/src/content/docs/`: this site. The home page is `index.mdx`.
- `docker/*.Dockerfile`: the broker and control-plane images, plus Prometheus
  and an OpenTelemetry collector for local stacks.
- `deploy/helm/felix/`: the Helm chart, checked by `task chart:check`.
- `.github/workflows/`: `ci`, `coverage`, `history`, `fuzz-nightly`, `soak`,
  `pages`, `release`, `cla`, `perf-pr`, `perf-publish` and
  `perf-comprehensive`. [Building & Testing](/development/building/#what-ci-runs)
  says what each runs.

Licences differ by path.
[LICENSING.md](https://github.com/GetFelix/felix/blob/main/LICENSING.md) has the
table, and [deny.toml](https://github.com/GetFelix/felix/blob/main/deny.toml)
lists the dependency licences allowed.

## Layout rules

These come from
[CONTRIBUTING.md](https://github.com/GetFelix/felix/blob/main/CONTRIBUTING.md);
[Contributing](/development/contributing/) summarises them.

- A module `foo` is `foo.rs` with its children in `foo/`. There is no `mod.rs`,
  except `tests/common/mod.rs` for helpers shared between integration-test
  binaries.
- `lib.rs` holds the crate docs, module declarations and re-exports, not code.
- Unit tests live in `<module>/tests.rs`, declared last in the module as
  `#[cfg(test)] mod tests;`. Integration tests live in the crate's `tests/`.
- `pub` means another crate uses it. Everything else is `pub(crate)`.
- Crate names start with `felix-`, and the directory is named after the
  package.
- Shared dependency versions live in `[workspace.dependencies]`.
