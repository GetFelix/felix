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
- **Docker or Podman**, optional. `task test` uses one to start Postgres for
  the control plane's Postgres tests and skips them without either.
  `CONTAINER_ENGINE` picks one; see [Docker or Podman](/getting-started/containers/).
- For the extras only: nightly Rust and `cargo-fuzz` for
  [fuzzing](/development/fuzzing/), Java, Docker or Podman for `task tla:check`,
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
3. With `FELIX_TEST_DATABASE_URL` set, or Docker or Podman available, it runs the
   workspace tests with `--features felix-controlplane-service/pg-tests`, so the
   Postgres store tests run too. Otherwise it runs plain `cargo test --workspace`.
4. Outside CI it runs `task pg:down`.

To point the Postgres tests at a database of your own, set
`FELIX_TEST_DATABASE_URL`. CI does this with a Postgres service container. If an
interrupted run leaves test containers behind, `task pg:sweep` removes them.

Cluster and distributed tests are covered in
[How Felix Is Tested](/architecture/testing/).

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
[deny.toml](https://github.com/GetFelix/felix/blob/main/deny.toml), then checks
the upstream versions of the crates under `vendor/` for advisories.

Other workflows:

- `coverage.yml`: `task coverage`.
- `pages.yml`: builds and deploys this docs site.
- `history.yml`: the nightly history-checker campaign.
- `fuzz-nightly.yml`: the long fuzz campaign.
- `tla-walk.yml`: TLC's simulation mode over long random walks of replica-set
  changes, one job per family, nightly (`task tla:walk`).
- `power-loss-nightly.yml`: the storage power-loss suite across 110 seeds per
  scenario from a random base, where pull requests run eight plus the pinned ones.
- `soak.yml`: a weekly soak and resource-leak run.
- `perf-pr.yml`, `perf-publish.yml`, `perf-comprehensive.yml`: benchmarks. The
  PR run is advisory and never fails a check.
- `release.yml`: builds and publishes a tagged release. See
  [Releases](#releases).
- `nightly.yml`: builds main through `release.yml` every night and publishes
  it to GitHub only. See [Nightly builds](#nightly-builds).
- `cla.yml`: the CLA Assistant bot.

## Releases

Pushing a `v*` tag runs `release.yml`. A tag with a `-` suffix
(`v0.6.0-preview`) is a GitHub prerelease, and its images are not tagged
`latest`. The tag has to match every version field in the tree, and every image tag the
docs pin (`scripts/check_release_version.py`), and its release notes are its
`CHANGELOG.md` section. The workflow:

- creates the GitHub release with `felix-<tag>-linux-x86_64.tar.gz` (broker
  and control plane) and `SHA256SUMS`;
- attaches `felixctl-<tag>-<target>.tar.gz` (`.zip` on Windows) and a
  `.sha256` for `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`,
  `aarch64-apple-darwin`, `x86_64-apple-darwin` and `x86_64-pc-windows-msvc`.
  Each holds the binary, its README and LICENSE, `completions/` and `man/`;
- writes `Formula/felixctl.rb` in `<owner>/homebrew-tap` from those archives'
  `.sha256` files and pushes it with the `HOMEBREW_TAP_DEPLOY_KEY` secret, a deploy key with write access to the tap
  (`PUBLISH_HOMEBREW`). A dry run keeps the formula as the
  `homebrew-formula-<tag>` artifact, and a nightly skips it;
- builds `ghcr.io/<owner>/felix-broker`, `felix-controlplane` and `felixctl`
  for linux/amd64 and linux/arm64, each on a runner of that architecture, and
  when `PUBLISH_IMAGES` is `true` pushes them as one multi-arch tag and signs it;
- after the Python and Node conformance suites pass, attaches the wheels and
  Node addons and publishes them to PyPI (`PUBLISH_PYPI`) and npm
  (`PUBLISH_NPM`);
- after both conformance suites, packages `felix-transport`, `felix-wire`,
  `felix-client`, `felix-loadgen`, `felix-storage` and `felixctl` together, then publishes them
  to crates.io in that order (`PUBLISH_CRATES`), skipping any version already
  there.

### crates.io credentials

The crates.io job uses the `CARGO_REGISTRY_TOKEN` secret when it is set, and
trusted publishing otherwise. crates.io only allows a trusted publisher on a
crate that already exists, so a release that adds a crate name publishes it
with the token, which needs the `publish-new` scope for that name. After that
release you can, optionally:

1. On crates.io, add a trusted publisher to each new crate: repository
   `GetFelix/felix`, workflow `release.yml`, environment `crates-io`.
2. Once every crate has one, run `gh secret delete CARGO_REGISTRY_TOKEN` and
   revoke the token. Later releases then publish with no stored credential.

npm has the same limit with no token fallback, so a new npm package name is
published once by hand with `scripts/npm_first_publish.sh`.

### Rehearsing a release

`dry_run` builds everything (binaries, archives, images, wheels, addons and
crate packages) and publishes nothing. `ref` builds a branch or commit as if
it were `tag`, so the rehearsal can run before the tag exists:

```bash
gh workflow run release.yml --ref main \
  -f tag=v0.6.0-preview.2 -f ref=main -f dry_run=true
```

`ref` without `dry_run` fails the run. The felixctl archives are kept as a
workflow artifact named `felixctl-<tag>-archives`.

### Nightly builds

`nightly.yml` runs at 04:41 UTC. It builds the newest commit on `main` whose
`ci.yml` push run passed, and does nothing if the `nightly` tag already points
at that commit. It calls `release.yml` with `nightly` set to the date, which
builds everything a release builds and changes what gets published:

- images go to GHCR as `nightly` (moves every night) and `nightly-YYYYMMDD`,
  multi-arch and signed like a release's. Version tags and `latest` are not
  touched;
- nothing goes to crates.io, PyPI or npm;
- after `scripts/ci/nightly_smoke.sh` has run a publish and subscribe through
  the pulled images, the `nightly` tag moves to the commit and the `nightly`
  GitHub pre-release is recreated on it with the broker, control plane and
  felixctl archives, the wheels, the Node addons and one `SHA256SUMS`. Its
  notes link the commit and the CI run and carry the `[Unreleased]` section of
  `CHANGELOG.md`. It is never marked latest.

A last step deletes `nightly-YYYYMMDD` image tags older than 14 days. It can
only delete versions of a package that grants this repository admin access,
and it does not fail the run when it cannot.

To build one now, or rehearse without publishing:

```bash
gh workflow run nightly.yml --ref main -f force=true
gh workflow run nightly.yml --ref main -f dry_run=true
```

A dispatch from any branch other than `main` is always a dry run. A pull
request that changes `nightly.yml` or `release.yml` runs it as a dry run too,
with the smoke test against images built in the runner.

The binaries, wheels and addons carry the version of the last release, so
`felix-broker --version` does not tell a nightly apart. The GitHub release
title names the commit.

## Self-hosted runners

Four persistent Azure VMs (8 vCPU each, label `felix-azure`) take the jobs that
are too slow for a GitHub-hosted runner, but only for pushes to `main`,
schedules and manual runs:

- `ci.yml`'s `test` job
- `coverage.yml`
- `history.yml`, the nightly history campaign
- `power-loss-nightly.yml`

Everything else stays on GitHub-hosted runners. The four TLA+ shards run in
parallel there, which two machines could not match, and each fuzz target uses
one core, so the nightly fuzz matrix gains little from eight.

Each runner takes one job at a time, so when all four are busy jobs wait in
the queue.

**Pull requests never run on them.** Every pull request, from a branch or a
fork, runs these jobs on `ubuntu-latest`. That keeps PR CI from queueing behind
`main` and the nightlies, and it keeps fork code off persistent machines: the
repository is public, and a machine that ran a stranger's code would hand it to
the next job. Each of those jobs picks its runner like this:

```yaml
runs-on: ${{ github.event_name != 'pull_request' && vars.FELIX_SELF_HOSTED_RUNNERS != 'false' && fromJSON('["self-hosted","felix-azure"]') || 'ubuntu-latest' }}
```

When the Azure runners are down, set the repository variable
`FELIX_SELF_HOSTED_RUNNERS` to `false` and every job runs on `ubuntu-latest`.
Delete the variable to go back.

On top of that, the repository requires a maintainer to approve workflow runs
for every outside contributor's pull request.

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
