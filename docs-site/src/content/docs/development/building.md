---
title: "Building & Testing"
description: "The toolchain, the task commands CI runs, and the two build traps worth knowing."
---

The shortcuts in `Taskfile.yml` are the source of truth. CI runs the same
tasks, so a green local run of the ones below predicts a green CI run.

## Toolchain

- **Rust 1.97.1**, pinned in `rust-toolchain.toml` with `rustfmt` and `clippy`.
  The workspace is edition 2024 with `rust-version = "1.97"`.
- **[Task](https://taskfile.dev)** for the shortcuts.
- **Docker**, optional. `task test` uses it to start Postgres for the control
  plane's Postgres tests and skips them without it.
- For the extras only: nightly Rust and `cargo-fuzz` for
  [fuzzing](/felix/development/fuzzing/), Java or Docker for `task tla:check`,
  Node for the docs site, and `helm` with PyYAML for `task chart:check`.

## Building

```bash
cargo build --workspace                                   # debug
task build                                                # cargo build --workspace --release
cargo build -p felix-broker-service --bin felix-broker    # just the broker
cargo build -p felix-controlplane-service                 # the control plane (felix-controlplane)
cargo build -p felix-broker-service --features telemetry  # with OpenTelemetry export
```

`.cargo/config.toml` points every crate at one `target/` directory, including
the standalone demo crates, so they reuse the workspace's dependency builds.

The profiles, all in `Cargo.toml` except `profiling`:

| Profile | Settings | Use |
| --- | --- | --- |
| `dev` | `debug = "line-tables-only"`, dependencies built without debug info | Everyday builds. Backtraces keep file and line; step-debugging locals needs `debug = true` |
| `release` | `lto = "fat"`, `codegen-units = 1`, panics unwind | Benchmarks and images |
| `profiling` | `release` plus `debug = true` (in `.cargo/config.toml`) | `cargo build --profile profiling -p felix-broker-service --bin felix-broker` for perf or Instruments |

## Testing

```bash
task test                                                  # what CI runs
cargo test -p felix-storage --lib disk_log::               # one module
cargo test -p felix-broker --test durable_streams <name>   # one integration test
cargo test -p felix-broker-service --test quic_subscribe   # one QUIC integration test
```

`task test` does more than `cargo test --workspace`:

1. It builds `felix-loadgen` first. A test in `felix-cluster` shells out to that
   binary, and `cargo test` would not build it.
2. Outside CI it runs `task pg:up`, a `postgres:16-alpine` container named
   `felix-pg-tests` on port 55432.
3. With `FELIX_TEST_DATABASE_URL` set, or Docker available, it runs the
   workspace tests with `--features felix-controlplane-service/pg-tests`, so the
   Postgres store tests run too. Otherwise it runs plain `cargo test --workspace`.
4. Outside CI it runs `task pg:down`.

To point the Postgres tests at a database of your own, set
`FELIX_TEST_DATABASE_URL`. CI does this with a Postgres service container. If an
interrupted run leaves test containers behind, `task pg:sweep` removes them.

Cluster and distributed tests are covered in
[How Felix Is Tested](/felix/architecture/testing/).

## Two traps

**Four demo crates are outside the workspace.** `demos/slow-consumer`,
`demos/state-divergence`, `demos/rbac-live` and `demos/cross_tenant_isolation`
each declare their own `[workspace]`. `task lint` and `task test` cannot see
them, so deleting a public item that only a demo uses passes both and breaks
the demo. Run `task demo:check` after changing a public API. It formats, lints,
builds and tests those crates, and runs the queue-semantics demo, which asserts
what it narrates. `task lock:refresh` re-locks them after a version bump. The
demos in `demos/broker/` are binaries of `felix-broker-service` and are in the
workspace.

**The cluster harness runs a prebuilt `felix-broker`.** `felix-cluster` spawns
`target/<profile>/felix-broker` and does not rebuild it, so `cargo test -p
felix-cluster` after a broker change tests the old binary. That silently breaks
"revert the fix and watch the test fail". Rebuild first:

```bash
cargo build -p felix-broker-service --bin felix-broker
cargo test -p felix-cluster
```

`task test` is safe, because `cargo test --workspace` builds the broker binary.

## Lint and format

```bash
task fmt    # cargo fmt --all
task lint   # fmt check, clippy --workspace --all-targets --all-features -D warnings,
            # then cargo check -p felix-common on its default features
```

The last step catches a misplaced `#[cfg]` that workspace feature unification
would hide. The workspace lints (`unreachable_pub`, clippy's
`mod_module_files`) are set in `[workspace.lints]` and fail under `-D warnings`.

`bash scripts/setup-githooks.sh` points git at `githooks/`. The pre-commit hook
runs `cargo fmt -- --check`, and the pre-push hook runs `task lint`. Neither runs
tests.

## Coverage

```bash
task coverage
```

This needs `cargo-llvm-cov`. It builds `felix-loadgen` into
`target/llvm-cov-target`, starts Postgres the same way `task test` does, and
writes `lcov.info` for the whole workspace with `--all-features`. It ignores
`demos/` and `src/bin/`. `task coverage:demos` covers only the demos.
`.github/workflows/coverage.yml` runs `task coverage` on pushes to main and on
pull requests.

## What CI runs

`ci.yml` runs on Linux only. `test` runs on the self-hosted runners described
below; the rest run on GitHub-hosted `ubuntu-latest`. Its jobs:

| Job | Runs |
| --- | --- |
| `test` | `task lint`, `task test`, `task deny`, `task publish:check`, `task ci:toolchains`, `task ci:timeouts`, `task docs:evidence`, `scripts/check_release_version.py`, `task demo:check` (then fails if a demo lockfile changed), `task deny:rsa:default` |
| `images` | Builds `docker/broker.Dockerfile` and `docker/controlplane.Dockerfile`, then checks each image starts, serves `/ready` and does not run as root |
| `chart` | `scripts/check_chart.py`, the same as `task chart:check` |
| `python`, `typescript` | Builds each binding and runs its client conformance suite, then verifies the results with `felix-conformance verify` |
| `formal` | `scripts/check_spec_pairing.py` against the PR base, then `scripts/check_tla.sh` (`task tla:check`) |
| `fuzz` | `task fuzz` with `FUZZ_SECONDS=30` |

`task deny` runs `cargo-deny check` against
[deny.toml](https://github.com/gabloe/felix/blob/main/deny.toml), then checks
the upstream versions of the crates under `vendor/` for advisories.

Other workflows:

- `coverage.yml`: `task coverage`.
- `pages.yml`: builds and deploys this docs site.
- `history.yml`: the nightly history-checker campaign.
- `fuzz-nightly.yml`: the long fuzz campaign.
- `soak.yml`: a weekly soak and resource-leak run.
- `perf-pr.yml`, `perf-publish.yml`, `perf-comprehensive.yml`: benchmarks. The
  PR run is advisory and never fails a check.
- `release.yml`: builds and publishes a tagged release.
- `cla.yml`: the CLA Assistant bot.

## Self-hosted runners

Two persistent Azure VMs (8 vCPU each, label `felix-azure`) take the jobs that
are too slow for a GitHub-hosted runner:

- `ci.yml`'s `test` job
- `coverage.yml`
- `history.yml`, the nightly history campaign
- `fuzz-nightly.yml`

Everything else, including the four TLA+ shards, stays on GitHub-hosted
runners. The shards run in parallel there, which two machines could not match.

There are two runners and each takes one job at a time, so when both are busy
jobs wait in the queue. The nightly fuzz matrix and the history campaign start
at the same time and keep both runners busy for a few hours.

**Fork pull requests never run on them.** The repository is public, and a
persistent machine that ran a stranger's code would hand it to the next job.
Each of those jobs picks its runner like this:

```yaml
runs-on: ${{ (github.event_name != 'pull_request' || github.event.pull_request.head.repo.full_name == github.repository) && fromJSON('["self-hosted","felix-azure"]') || 'ubuntu-latest' }}
```

Pushes, schedules, manual runs and pull requests from branches in this
repository go to `felix-azure`. A pull request from a fork gets
`ubuntu-latest`. On top of that, the repository requires a maintainer to
approve workflow runs for every outside contributor's pull request.

On the self-hosted path the jobs skip `swatinem/rust-cache` and keep a warm
target directory per job under `~/felix-cache` instead, through
`.github/actions/warm-target`. The coverage job clears old profiles before it
runs, so a warm directory does not skew the numbers.

To rebuild, re-register or tear down the machines, use
`scripts/ci/azure-runners/deploy.sh` (`up`, `register`, `status`, `down`).
Its README covers the network rules, what is installed and how registration
tokens are handled. A runner that shows offline after a reboot or a runner
update usually needs only `./deploy.sh register`.

## Docs site

```bash
cd docs-site && npm ci && npm run build
```

The build runs the mermaid, diagram and evidence checks before `astro build`.
`npm run dev` serves the site with live reload.

## Cleaning up

- `task clean` runs `cargo clean` and removes the latency demo's raw output.
- `task clean-all` deletes `target/` and any per-demo `target/` directories.
- `task clean:stale` uses `cargo-sweep` to prune artifacts from old toolchains
  and anything untouched for a week, keeping the current build incremental.
