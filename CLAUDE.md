# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

Rust 1.97, edition 2024. The `Taskfile.yml` shortcuts are the source of truth — CI runs the same ones.

```bash
task lint          # cargo fmt --check + clippy --workspace --all-targets --all-features -D warnings
task test          # cargo test --workspace (with Postgres when Docker is available)
task demo:check    # build/clippy/test the demo crates that are OUTSIDE the workspace
task coverage      # llvm-cov; spins up a Postgres container unless CI or FELIX_TEST_DATABASE_URL is set
task conformance   # wire-protocol conformance runner
```

Single test / narrower runs:

```bash
cargo test -p felix-storage --lib disk_log::                    # one module
cargo test -p felix-broker --test durable_streams <name>        # one integration test
cargo test -p felix-broker-service quic_subscribe                             # QUIC integration tests
```

Docs and perf:

```bash
cd docs-site && npm run build          # runs check:mermaid first, then astro build
node scripts/check-mermaid.mjs         # validate every mermaid diagram in docs/ and docs-site/
task perf:latency-matrix               # run matrix -> aggregate -> charts -> markdown snippets
```

`scripts/perf/` needs `matplotlib` (see `scripts/perf/requirements.txt`); the aggregate step
degrades gracefully without `pandas`.

### Two traps worth knowing

- **`demos/slow-consumer`, `demos/state-divergence`, `demos/rbac-live`, and
  `demos/cross_tenant_isolation` are separate crates, not workspace members.** `task lint`
  and `task test` cannot see them, so a workspace-scoped "this is unused, delete it" signal
  is unreliable — deleting a public item that only a demo uses passes lint and breaks the
  build. Run `task demo:check` after changing public APIs.
- **The cluster harness runs a *prebuilt* `target/<profile>/felix-broker`.** `cargo test -p
  felix-cluster` does not rebuild it, so a broker-side change is not in the binary those
  tests spawn until `cargo build -p felix-broker-service --bin felix-broker` runs. This silently
  invalidates "revert the fix and watch the test fail": the test keeps passing because it
  is still running the old broker. `task test` is fine — `cargo test --workspace` builds
  the binary first.

## Architecture

### The publish path is where most invariants live

A publish crosses four components in a fixed order, and the order is the design:

1. **`services/felix-broker-service/src/serving/quic/`** decodes the frame. `handlers/publish.rs` and
   `handlers/subscribe.rs` own the per-message work; `streams/control.rs` is the control-stream
   loop that owns auth state and dispatches every `Message` variant.
2. **`crates/server/felix-broker/src/broker/publish.rs`** takes offsets from storage *before*
   waiting on durability (`begin_append` → `commit`), so the batch claims its place in the
   stream's order the instant its offsets are consumed. `felix-storage`'s `CommitSequencer` then makes
   later publishes wait behind earlier ones regardless of whether those succeed, fail, or are
   cancelled.
3. **`crates/server/felix-storage/src/disk_log/`** persists it. See below.
4. **Fanout** happens after durability, via `stream/delivery.rs`. One `DeliveryEnvelope` is shared by
   every subscriber and caches its encoded frame, so a publish is encoded once regardless of
   fanout — with a second cached encoding when some subscribers negotiated event offsets and
   others did not.

Reordering these is almost always a bug. Several past defects were "the natural order" —
e.g. reading history before registering a subscriber loses any publish landing in between.

### Storage: a log-structured segment store, not a WAL

`crates/server/felix-storage/src/`:

- `io.rs` — platform I/O: `pread`, preallocation, `F_FULLFSYNC` on macOS, and the
  optional io_uring flusher.
- `segment/` — the byte format (`format.rs`), the startup scan that decides torn tail vs.
  corruption (`scan.rs`), range reads (`reader.rs`), the writer, and the sparse index.
- `disk_log.rs` — the async `AppendOnlyLog` seam; under `disk_log/`: `append.rs` (the append
  path and background rollover), `flush.rs`, `segments.rs` (with `rollover.rs` and
  `truncation.rs`), `recovery.rs` (startup validation and torn-tail repair), `sync.rs` (fsync
  policy and group commit), `retention.rs`.
- `cache/` and `counter_log.rs` — the cache and counters, each projected from its own logs.

Load-bearing properties, all documented in `docs/durable-storage.md` and
`docs/storage-format.md`:

- **Records are never rewritten**, so recovery can trust "valid bytes end at EOF". Preallocation
  reserves blocks *without* changing `st_size` for exactly this reason.
- **A torn tail is repaired; interior corruption is fatal.** Refusing to start beats silently
  losing acknowledged records.
- **Indexes are derived, never trusted** — a missing, short, or stale index is rebuilt from the
  segment it describes. This is why it is safe to skip fsyncing a freshly created index.
- **Group commit** is the single biggest throughput lever under `FsyncMode::OnCommit`; one
  blocking flush serves many waiters.

### Subscribers are isolated by construction

Per-subscriber bounded queues with an explicit overflow policy (`SubQueuePolicy`, default
`DropNew`). A publisher never blocks on a slow subscriber. `handlers/subscribe/feeder.rs`
and `event_writer.rs` do per-subscriber batching and writing behind a lane manager.

Because dropping is the default, **a subscriber can silently miss records** — which is why
delivered events carry log offsets for durable streams: a jump in offsets is exactly a drop.

### Wire protocol: capability negotiation, not versioning

`crates/protocol/felix-wire/`. Frame flags (`client/flags.rs`) select the *payload layout*, so an
unknown flag bit is rejected rather than masked off — masking one means confidently misparsing
the body.

New features are added as negotiated flag bits, not version bumps: a client offers
`Auth.client_flags`, the broker answers `AuthOk.server_flags`. A peer that predates negotiation
sends/receives a plain `Ok`, and the only safe reading of that silence is `ORIGINAL_V1_FLAGS`.
`ORIGINAL_V1_FLAGS` is frozen — never add to it.

Optional JSON fields must default to the pre-existing behaviour so an old peer and a new peer
exchange byte-identical frames.

### Replication is its own crate

`crates/server/felix-replication/` owns the broker-to-broker transport (`peer/`) and log
replication (`driver/`, `replica.rs`, `quorum.rs`, `reporter.rs`). It does not depend on the
broker service: the service hands in its ingress router, shard fence and node credential
through `quorum::ShardServing`, `driver::WriteFence` and `reporter::Credential`, and
`services/felix-broker-service/src/node/cluster.rs` wires it up.

### Control plane and startup ordering

`services/felix-controlplane-service/` serves metadata over REST; the broker seeds from it at startup.
`services/felix-broker-service/src/node.rs` gates readiness and the accept loop on that seeding, so the
broker does not accept traffic for streams it does not yet know about.

## Conventions

- **Code organization rules are in CONTRIBUTING.md** ("How the code is organized"): grouped
  crate layout, `lib.rs` as a table of contents, domain-named modules, item order inside a
  file, and unit tests in `<module>/tests.rs`. Follow them for new code.
- **Comment the *why*, briefly.** A non-obvious constraint — an ordering requirement, a
  failure mode, a limit that exists for a reason — is worth a sentence or two next to the
  code that depends on it. Aim for that, not for an essay.
  - Don't restate what the code says. If the comment tracks the code line by line, delete it.
  - Don't narrate history ("previously this did X"). The reason the current code is shaped
    this way is what matters; git holds the rest.
  - When an explanation genuinely needs paragraphs, it belongs in `docs/` with a one-line
    pointer from the code.
  - `///` on public items is documentation and is held to a different standard: say what the
    thing does and what it guarantees, and keep it accurate.
  - Comments should not be verbose. Keep it succinct.
  - Comments should be written in a human-voice, not in an AI-slop voice.
- **Prefer `pub(crate)`.** Reach for `pub` only when something outside the crate uses it.
  Most of the workspace's public surface is only used internally, which makes the real API
  hard to see and every item look load-bearing. `unreachable_pub` and clippy's
  `mod_module_files` are enforced through `[workspace.lints]` (every member sets
  `[lints] workspace = true`), so `task lint` catches unexported `pub` and any new
  `mod.rs` — the module style is `foo.rs` + `foo/`, with `tests/common/mod.rs` as the
  one sanctioned exception. Shared dependency versions live in
  `[workspace.dependencies]`; add features per member rather than re-pinning versions.
- **Docs are treated as part of the change.** `docs/protocol.md`, `docs/durable-storage.md`,
  `docs/storage-format.md`, `docs/storage-performance.md`, and the status tables in
  `docs-site/src/content/docs/getting-started/what-felix-is-for.md` make specific claims about
  what is implemented. That page states outright that stale status markers are worse than none.
  If you ship a capability, move its row; if you find a claim the code cannot back, fix the claim.
- **A regression test that passes without the fix proves nothing.** For concurrency and
  durability fixes, revert the fix, watch the new test fail, then restore it.
- Benchmarks: `throughput` is what publishers sent, `delivered_throughput` counts subscriber
  deliveries — they differ by the fanout factor. Batched runs (`batch > 1`) measure a
  throughput profile, not request latency.
