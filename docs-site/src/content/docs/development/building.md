---
title: "Building & Testing"
description: "The toolchain, the task commands CI runs, and the two build traps worth knowing."
---

The shortcuts in `Taskfile.yml` are the source of truth. CI runs the same
tasks, so a green local run of the ones below predicts a green CI run.

## Toolchain

- **Rust 1.99.1**, pinned in `rust-toolchain.toml` with `rustfmt` and `clippy`.
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

`ci.yml` runs on Linux only. `test` and `cluster` run on the self-hosted runners
described below for pushes to main; the rest run GitHub-hosted (see [Larger runners](#larger-runners)).
Every push and pull request starts `ci.yml` and `coverage.yml`. Their first
job, `changes`, checks whether anything besides docs and Markdown changed
(the TLA+ specs under `docs/formal` count as code), and the other jobs skip
when nothing did. Each workflow ends with a job that always runs, `CI ok` and
`Coverage ok`. They fail if any job failed or was cancelled, and they are the
checks `main` requires, so `gh pr merge --auto --squash` waits for them. A
docs-only change gets both within a minute or two.

`coverage` itself runs only on pushes to main, where it publishes the badge;
on a pull request it is skipped and `Coverage ok` passes.

Its jobs, in [`ci.yml`](https://github.com/GetFelix/felix/blob/main/.github/workflows/ci.yml),
which is the authority for what each one runs:

- `test`: the workspace tests except `felix-cluster`, then the stress tests.
- `cluster`: the `felix-cluster` suites, split into `routing`, `failures`, `history` and the rest.
- `checks`: lint, `cargo-deny`, publish and toolchain checks, `task docs:evidence`, the changelog and release-version checks, and `task demo:check`.
- `images`: builds the broker and control-plane images and checks each starts, serves `/ready` and does not run as root.
- `compose`: brings up `compose.yaml` with those images and does one `felixctl` round trip.
- `chart`: `task chart:check`.
- `python`, `typescript`: build each binding and run its client conformance suite.
- `spec-pairing`: on pull requests that change code, checks that a change to a modelled area names its spec.
- `formal`: the TLA+ checks, when `docs/formal/` or the check script changes. A pull request runs the short configurations and any it changes; main runs all of them.
- `fuzz`: `task fuzz` for 30 seconds per target.

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
- `perf-env-ab.yml`: manual. Runs the fast subsets against one build twice,
  once per environment setting, to price a configuration default.
- `release.yml`: builds and publishes a tagged release. See
  [Releases](#releases).
- `nightly.yml`: builds main through `release.yml` every night and publishes
  it to GitHub only. See [Nightly builds](#nightly-builds).
- `cla.yml`: the CLA Assistant bot.

## Releases

Pushing a `v*` tag runs `release.yml`. A tag with a `-` suffix
(`v0.6.0-preview`) is a GitHub prerelease, and its images are not tagged
`latest`. The tag has to match every version field in the tree, and every image tag the
docs and `compose.yaml` pin (`scripts/check_release_version.py`), and its release notes are its
`CHANGELOG.md` section. The release step that bumps the `compose.yaml` default makes `scripts/check_version_pins.py` (in `task docs:evidence`) list every docs pin to bump. The workflow:

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
- builds `libfelix` natively on Linux, macOS and Windows, x64 and arm64, packs
  them into one `Felix.Client` NuGet package (failing if any of the six is
  missing) and pushes it to nuget.org when `PUBLISH_NUGET` is `true`, skipping
  a version already there;
- attaches `libfelix-<tag>-<rid>.tar.gz` (`.zip` on Windows) and a `.sha256`
  for `linux-x64`, `linux-arm64`, `osx-x64`, `osx-arm64`, `win-x64` and
  `win-arm64`, through `release-capi-archives.yml`. Each holds `lib/` (shared
  and static library, plus the import library on Windows), `include/felix.h`,
  `LICENSE` and `LINKING.txt` with the platform's static-link system libraries
  and the link commands. Before an archive is kept, `scripts/package_libfelix.sh`
  compiles a C program against its header and links and runs it both ways, so
  a dry run exercises every platform and attaches nothing;
- after both conformance suites, packages `felix-transport`, `felix-wire`,
  `felix-client`, `felix-loadgen` and `felixctl` together, then publishes them
  to crates.io in that order (`PUBLISH_CRATES`), skipping any version already
  there;
- once those jobs finish, sends `release-published` to
  `<owner>/getfelix.github.io` so the landing page shows the new version. This
  needs the `SITE_DISPATCH_TOKEN` secret: a fine-grained token on that one
  repository with Contents read and write. Without it the job only leaves a
  notice, and the site picks the release up on its own schedule;
- once those jobs finish, sends a `felix-release` dispatch to each repo in
  `.github/companions.txt` so it can open a bump PR (see below).

### Before you tag

Companion repos run their integration tests against Felix main each night (see
[Nightly runs in the companions](#nightly-runs-in-the-companions)). Before a
tag, the latest run in each repo that has the workflow should have succeeded,
and none should have an open `felix-nightly` issue:

```bash
for repo in $(sed -e 's/#.*//' .github/companions.txt | awk 'NF {print $1}'); do
  run=$(gh run list -R "GetFelix/$repo" --workflow felix-nightly.yml --branch main \
    --limit 1 --json status,conclusion,createdAt,url \
    --jq '.[0] // empty | "\(if .conclusion == "" then "not finished (" + .status + ")" else .conclusion end) \(.createdAt) \(.url)"' \
    2>/dev/null) || { echo "$repo: no nightly yet"; continue; }
  echo "$repo: ${run:-no run on main yet}"
  gh issue list -R "GetFelix/$repo" --label felix-nightly --state open
done
```

Only `success` is green. `failure`, `cancelled`, `startup_failure`, "not
finished" and "no run on main yet" are not. A repo that prints "no nightly yet"
has not added the workflow, so the check does not cover it.

A failed run means a change on main broke that companion. Fix Felix, or send
the companion a fix, before tagging.

A run only counts if it tested the commit you are tagging. It tests the commit
behind the `nightly` tag, and that tag moves last in the nightly build, so
after a failed build it can lag main by a day or more. Open the run's "Find the
Felix commit" step, or the issue, to read the commit it used and compare it with
the one you are tagging. If they differ, build a nightly from the newest green
`main` commit and run the companions again:

```bash
gh workflow run nightly.yml -R GetFelix/felix --ref main -f force=true
gh run list -R GetFelix/felix --workflow nightly.yml --limit 1   # wait until it completes
gh workflow run felix-nightly.yml -R GetFelix/<repo>
```

Wait for the nightly build to finish before you start a companion's run. The
`nightly` tag only moves at the end, and a companion run started earlier tests
the old commit.

### Release notes and the upgrade page

The docs site's [release notes](/release-notes/) are generated from
`CHANGELOG.md` at build time by `scripts/gen-release-notes.mjs`, so assembling
the changelog fragments is all a release needs for them. The hand-written
[Upgrading to 0.7](/release-notes/upgrading-to-0-7/) page is not generated.
Review it before tagging: it should match the assembled changelog, drop the
"not released" notice, and say whether the release adds a fleet feature or
changes `INTERNAL_VERSION` or `FORMAT_VERSION`. When the next line starts,
add its page and link it from `deployment/upgrades.md`.

### Backfilling the C archives

A release that predates the archives gets them without rerunning anything
else. The workflow builds the tag's own source, smoke-links the six archives
and uploads them to the existing release; it refuses a name that is not a tag
or has no published release. It defaults to a dry run:

```bash
gh workflow run release-capi-archives.yml -f tag=v0.7.0-preview.2            # build and smoke only
gh workflow run release-capi-archives.yml -f tag=v0.7.0-preview.2 -f dry_run=false
```

Uploading again replaces the files already there.

### Companion repos

The `companion-dispatch` job runs under the same conditions as the site job.
It sends a `repository_dispatch` with event type `felix-release` to every repo
listed in `.github/companions.txt`. A repo that fails is reported and the rest
still get theirs; the job then fails, and nothing else in the release depends
on it. A prerelease tag (one with a `-` suffix, such as `v0.6.0-preview.9`)
only goes to repos whose line ends in `prerelease`.

The payload a companion workflow can rely on:

```json
{
  "version": "0.6.0",
  "tag": "v0.6.0",
  "prerelease": false,
  "release_url": "https://github.com/GetFelix/felix/releases/tag/v0.6.0"
}
```

`version` has no leading `v`. A companion listens with
`on: repository_dispatch: types: [felix-release]` and reads
`github.event.client_payload`.

The job needs a GitHub App, so that the dispatch and the bump PRs are not tied
to a person and the PRs trigger CI. Without the two secrets the job only leaves
a notice. An org owner sets it up once:

1. In the GetFelix org settings, under Developer settings, create a GitHub App.
   Turn the webhook off.
2. Give it repository permissions Contents (read and write), Pull requests
   (read and write), Workflows (read and write) and Metadata (read).
3. Install it on felix and the companion repos. Felix's nightly build uses it
   to move the `nightly` tag (see [Nightly builds](#nightly-builds)).
4. Generate a private key for the App.
5. Add the App ID as the org secret `FELIX_RELEASE_BOT_APP_ID` and the key as
   `FELIX_RELEASE_BOT_PRIVATE_KEY`, shared with felix and the companion repos.
   The companions use the same App to open their PRs.

#### The receiving side

Each companion handles the event with the same reusable workflow,
`.github/workflows/felix-bump.yml` in
[GetFelix/.github](https://github.com/GetFelix/.github). The companion keeps
three files:

- `.felix-version`, a `key=value` manifest: `version` (the Felix version the
  repo is on), `cargo_dirs` and `cargo_crates` (where to regenerate
  `Cargo.lock`, and which Felix crates must end up locked at the new version),
  `npm_dirs` (where to regenerate `package-lock.json`) and `prerelease` (`true`
  to also take prereleases).
- `.github/felix-bump.sh <old> <new>`, which edits every place the repo pins the
  Felix version: Cargo and npm dependencies, image tags in compose files and
  workflows, install lines in the README and docs, and the Felix entry in the
  CHANGELOG's unreleased section. It fails if a pin it expects is missing, so a
  moved pin breaks the bump instead of being skipped.
- `.github/workflows/felix-release.yml`, which runs on the `felix-release`
  dispatch and on `workflow_dispatch`, and calls the reusable workflow with
  `secrets: inherit`.

The reusable workflow mints a release bot token scoped to the calling repo and
reads `.felix-version`. It stops with a notice if the repo is already on that
version, if the version is older, or if it is a prerelease the repo has not
opted into. Otherwise it runs the bump script, regenerates the lockfiles
(retrying while crates.io and npm catch up with the release), and pushes
`felix/bump-<version>` as the bot. It opens or updates the PR
`chore: use Felix <version>` with that version's CHANGELOG section, enables
squash auto-merge, and closes any older bump PR still open. The repo's required
checks decide whether the bump merges.

Running the companion's workflow by hand with `test_pr: true` opens a draft PR
titled `TEST: felix bump bot` that only touches `.felix-version`. It checks
that the bot can open PRs there and that CI starts on them. Close it without
merging.

To add a companion:

1. Add `.felix-version`, `.github/felix-bump.sh` and
   `.github/workflows/felix-release.yml`, copied from an existing companion such
   as felix-gateway. Write the bump script's pins from the repo's last manual
   bump.
2. Turn on "Allow auto-merge" in the repo's settings.
3. Install the release bot on the repo, and share the two
   `FELIX_RELEASE_BOT_*` org secrets with it.
4. Append the repo's name to `.github/companions.txt` here.
5. Add the nightly run (see [below](#nightly-runs-in-the-companions)). Copy
   felix-gateway's `.github/workflows/felix-nightly.yml` and
   `.github/felix-nightly.sh` as the template.

#### Nightly runs in the companions

A bump PR shows that a companion works on a release. The nightly run shows that
it still works on main, so a change that breaks a companion is found the day it
lands. Each companion keeps:

- `.github/workflows/felix-nightly.yml`, which runs on a schedule after the
  Felix nightly build (04:41 UTC) and on `workflow_dispatch`, and calls the
  reusable workflow of the same name in GetFelix/.github. The calling job must
  grant both `contents: read` and `issues: write`. Setting only `issues: write`
  leaves `contents` at none and the checkout fails. The optional
  `timeout_minutes` input limits the script and defaults to 60.
- `.github/felix-nightly.sh`, which starts Felix from the `nightly` images
  (`$FELIX_IMAGE_TAG`) and runs the integration tests the repo's own CI runs
  against Felix. It exits non-zero when they fail. It must not run `npm ci` or
  `cargo --locked`: the workflow rewrites the manifests and lockfiles first, so
  they no longer match the ones in git.
- `.felix-version`, the same manifest the bump uses.

The reusable workflow:

1. Finds the Felix commit behind the `nightly` tag. The images and the tag come
   from one build, but the tag moves last, so after a failed nightly build the
   two can be a day apart. The run warns when the broker image's revision label
   names another commit, and still uses the tag's.
2. For each directory in `cargo_dirs`, rewrites every dependency on a crate in
   `cargo_crates` into a git dependency on that commit, keeping features and
   `default-features`, and runs `cargo update` on those crates.
3. For each directory in `npm_dirs`, builds the TypeScript client from
   `crates/sdk/felix-typescript` at that commit, packs the JavaScript and the
   built addon into one tarball (the platform packages are not published for a
   nightly), points every `package.json` that depends on `felix-client` at the
   tarball, and runs `npm install`.
4. Runs `.github/felix-nightly.sh` with `FELIX_NIGHTLY_SHA` and
   `FELIX_IMAGE_TAG=nightly` set.
5. On failure, opens an issue titled "Nightly against Felix main fails" with the
   label `felix-nightly` in the companion, or comments on the open one. The
   issue names the run, the Felix commit and the step that failed. A green run
   closes the issue, with a comment, if one is open. Only runs on the
   repo's default branch open or close issues. A run from another branch
   just passes or fails.

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

### NuGet credentials

The NuGet job holds no API key. It trades the workflow's OIDC identity for a
short-lived key through nuget.org trusted publishing. To turn it on:

1. On nuget.org, under the `GetFelix` account, add a trusted publishing policy:
   repository `GetFelix/felix`, workflow `release.yml`, environment `dotnet`.
   While the package is new the policy is pending, and the first push creates
   `Felix.Client` under that account.
2. Create the `dotnet` environment on the repository.
3. Set the repository variable `PUBLISH_NUGET` to `true`, and `NUGET_USER` to
   the nuget.org account name if it is not `GetFelix`.

A dry run packs the nupkg and checks its contents without pushing.

npm has the same limit with no token fallback, so a new npm package name is
published once by hand with `scripts/npm_first_publish.sh`.

### Rehearsing a release

`dry_run` builds everything (binaries, archives, images, wheels, addons and
crate packages) and publishes nothing. `ref` builds a branch or commit as if
it were `tag`, so the rehearsal can run before the tag exists:

```bash
gh workflow run release.yml --ref main \
  -f tag=v0.7.0-preview.2 -f ref=main -f dry_run=true
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

The tag and release move with a token from the release bot App (see
[Companion repos](#companion-repos)), not the workflow's own token. GitHub
refuses to point a ref at a commit that changes `.github/workflows/` unless the
token has the Workflows permission, and the workflow's own token never has it.
The nightly cannot publish without the App: the first job checks that the
`FELIX_RELEASE_BOT_*` secrets are set and the App is installed on the repo, so
either one missing fails the run before the build starts.

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

- `ci.yml`'s `test` and `cluster` jobs, and `formal` when the spec changes
- `coverage.yml`
- `history.yml`, the nightly history campaign
- `power-loss-nightly.yml`

Everything else stays on GitHub-hosted runners. Each fuzz target uses one
core, so the nightly fuzz matrix gains little from eight.

Each runner takes one job at a time, so when all four are busy jobs wait in
the queue.

**Pull requests never run on them.** Every pull request, from a branch or a
fork, runs these jobs GitHub-hosted. That keeps PR CI from queueing behind
`main` and the nightlies, and it keeps fork code off persistent machines: the
repository is public, and a machine that ran a stranger's code would hand it to
the next job. Each of those jobs picks its runner like this:

```yaml
runs-on: ${{ github.event_name != 'pull_request' && vars.FELIX_SELF_HOSTED_RUNNERS != 'false' && fromJSON('["self-hosted","felix-azure"]') || vars.FELIX_LARGE_RUNNER || 'ubuntu-latest' }}
```

When the Azure runners are down, set the repository variable
`FELIX_SELF_HOSTED_RUNNERS` to `false` and those jobs run GitHub-hosted.
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

## Larger runners

The repository variable `FELIX_LARGE_RUNNER` names a GitHub-hosted larger
runner (`felix-runner-x64`: 8 cores, 32 GB, up to 40 at once). When it is set,
`test` and `coverage` (wherever they run GitHub-hosted), the seven TLA+
shards and `perf-pr.yml` use it instead of the 4-core `ubuntu-latest`. TLC
runs a worker per core, so the shards gain as much as `test` does.
`perf-pr.yml` benchmarks the merge-base and the PR head in one job,
alternating their trials, so its comparison holds on any runner.
`perf-publish.yml` stays on `ubuntu-latest`: it adds to a stored history, and a change of machine would show up there as a
regression or a speedup.

Larger runners are billed per minute even on a public repository, up to the
organization's Actions budget; past it, their jobs are refused. They are fresh
machines for every job, so pull requests can use them. Delete the variable and
every job is back on `ubuntu-latest`.

## Docs site

```bash
cd docs-site && npm ci && npm run build
```

The build runs the mermaid, diagram and evidence checks before `astro build`.
`npm run dev` serves the site with live reload. Use these scripts rather than
running `astro` directly: they first generate the release notes pages from
`CHANGELOG.md`, which the sidebar needs.

### Versions

docs.getfelix.dev serves the newest stable release at the root, built from that
release's tag as it shipped. `main` is published under `/next/`, marked
`noindex` and left out of the sitemap, with a banner pointing back to the stable
docs. Previews are opt-in: the root never recommends one.

A plain `npm run build` builds `main` at `/`. `DOCS_BASE=/next/` builds it for
the sub-path, and a small integration (`docs-site/plugins/prefix-base-links.mjs`)
prefixes the base onto root-relative links in the built HTML, so keep writing
them as `/getting-started/...`. To build both trees as the site does:

```bash
scripts/build-docs-versions.sh "$(scripts/latest-stable-tag.sh)" site
node scripts/check-docs-links.mjs site
```

`pages.yml` runs the first script on a push to `main`, after the performance
data publishes, and on demand. A stable release ends with a job in `release.yml`
that dispatches `pages.yml` on `main`, which moves the root to the new tag. It
is dispatched rather than triggered by the release event because that event runs
on the tag, and the `github-pages` environment only deploys from `main`. A failed build of the tag
fails the deploy; `main` never fills in at the root. CI runs both scripts on a
docs change, and a link that leaves its own tree fails the check.

## Cleaning up

- `task clean` runs `cargo clean` and removes the latency demo's raw output.
- `task clean-all` deletes `target/` and any per-demo `target/` directories.
- `task clean:stale` uses `cargo-sweep` to prune artifacts from old toolchains
  and anything untouched for a week, keeping the current build incremental.
