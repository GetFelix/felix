# Contributing to Felix

Thanks for your interest in contributing. Before your first pull request is
merged, please read this. It's short.

## License Split

Felix uses a split license. The wire protocol, client SDK, transport layer,
and shared types are Apache-2.0. The broker, the test tooling (conformance
suite included) and the control-plane server components are the GNU Affero General Public License v3.0. See
[LICENSING.md](LICENSING.md) for the full breakdown of which path is under
which license. Know which part of the tree your PR touches before you start.

## Contributor License Agreement

Every contribution needs two things, whichever license path it lands in.
Together they let the project change its licensing later without tracking
down past contributors one by one.

1. **DCO sign-off.** Certify you wrote (or have the right to submit) the
   code, by adding `-s` to your commit:

   ```bash
   git commit -s -m "your message"
   ```

   This adds a `Signed-off-by: Your Name <you@example.com>` trailer. It's the
   same mechanism used by the Linux kernel and Docker.

2. **CLA grant.** On your first pull request, the CLA Assistant bot will
   comment asking you to reply with a fixed phrase to sign. The full text is
   in [CLA.md](CLA.md); in short, you confirm the contribution is your
   original work (or you have the right to submit it) and grant the project
   a broad, non-exclusive license to use and relicense it. You keep your
   copyright. You sign once, not per PR.

## AI-Assisted Contributions

AI tools (Claude, Copilot, etc.) are fine to use, and this project uses them.

You're responsible for what you submit. Review AI-generated or AI-assisted
code as if you wrote it yourself. The CLA/DCO sign-off is still your
assertion that you have the right to submit it.

Mention substantial AI assistance in the PR description (the tool, and
roughly how much of the change it wrote) so reviewers know. A one-line note
like "drafted with Claude Code, reviewed and tested by me" is enough.

## Getting Started

- `cargo build --workspace` builds everything.
- `task test` runs the full test suite. It needs no database or container. See
  [Docker or Podman](docs-site/src/content/docs/getting-started/containers.md)
  for the tasks that do use one.
- `task lint` runs `cargo fmt --check` and `cargo clippy -D warnings`. Both
  must pass in CI.
- See [ARCHITECTURE.md](ARCHITECTURE.md) for how the pieces fit together
  and [docs/](docs/) for design docs.

## How the code is organized

These rules are what reviewers will hold a change to. Most of them exist so
that someone new can find their way from the directory tree alone.

### Crates

- Crates are grouped by role under `crates/` (`protocol`, `server`, `sdk`,
  `testing`), and the deployables live in `services/`. The directory is always
  named after the package. [crates/README.md](crates/README.md) says what each
  group is for.
- Put a new crate in the group whose users it shares. Crate names start with
  `felix-`; published names are permanent, so choose carefully.
- Shared dependency versions go in `[workspace.dependencies]`. Members add
  features, they don't re-pin versions.
- `crates/` holds what cargo builds, including the bindings compiled from Rust
  (Python on PyO3, TypeScript on napi-rs, the C ABI). `sdks/<language>/` holds
  SDKs built by their own toolchain on top of `felix-capi` (C# now, Go next).
  They live in this repo so they move in step with the C ABI, and may move to
  their own repos once that interface settles.

### Modules

- One module style: `foo.rs` with its children in `foo/`. No `mod.rs`
  (clippy enforces this), no `#[path]`, no `include!` of Rust source.
- `lib.rs` is a table of contents: the crate docs, the module declarations
  and the re-exports. Types and functions live in modules.
- Group modules by what they are about (`stream/`, `queue/`, `publish/`),
  not by kind of code. Avoid grab-bag names like `utils`, `helpers`,
  `common`, `misc` or `types`. A module named after its parent
  (`client/client.rs`) is a sign the parent is the wrong shape.
- Default to `pub(crate)`. Use `pub` only for what another crate uses;
  `unreachable_pub` enforces this.
- Split a file when it holds two ideas with separate invariants. Length alone
  is no reason to split, though a file past about 800 lines of non-test code
  usually holds more than one idea.

### Inside a file

Write a file so it reads top-down: the thing a reader came for first, the
details below it.

1. The `//!` module doc: what this module is for, and anything a reader must
   know before changing it.
2. `mod` declarations, then `pub use` re-exports.
3. `use` imports in three blocks separated by a blank line: `std`, external
   crates, then `crate::`/`super::`.
4. Constants.
5. The main type of the module, then its inherent `impl`, then its trait
   impls. Keep every impl for a type next to the type.
6. Supporting types, in the order they are first used.
7. Free functions, public before private.
8. `#[cfg(test)] mod tests;` last.

Inside an `impl`: constructors, then accessors, then operations in the order
a caller uses them, then private helpers.

### Tests

- Unit tests go in `<module>/tests.rs`, declared as `#[cfg(test)] mod tests;`
  at the bottom of the module. When that file grows past several hundred
  lines, make it a hub for shared helpers with themed files under
  `<module>/tests/`.
- Integration tests go in the crate's `tests/`. Related files that share
  setup can be one binary: `tests/<area>/main.rs` with a module per file.
  Keep a test in a binary of its own when it changes process-wide state such
  as environment variables.
- A decoder that reads bytes from outside the process (the network, disk, a
  peer) gets a fuzz target in its crate's `fuzz/`. `task fuzz` runs them all
  briefly; a nightly workflow runs them for longer. See
  `docs-site/src/content/docs/development/fuzzing.md`.

## Writing docs

Documentation ships with the change it describes. Write plain, direct prose
and say what the thing does and what it guarantees. Don't use em-dashes.
Prefer sentences to bullet lists with bold labels. Skip "not X, but Y"
constructions used for effect, signposting such as "the key insight", and
filler that restates what was just said.

## Pull Requests

- Keep PRs focused; a bug fix doesn't need an unrelated refactor along for
  the ride.
- Add tests for new behavior.
- Record user-visible changes as a fragment in `changelog.d/`, not in
  `CHANGELOG.md` (see `changelog.d/README.md`). Fragments are merged at release
  time, so PRs don't conflict on the changelog.
- `task lint` and `task test` should pass locally before you open a PR. CI
  runs both plus `cargo-deny`.
- PR CI runs only the jobs your change can affect (below).
- The `Instructions per message` and `Performance Regression Check` workflows
  run on a PR only when it touches hot-path code (`crates/server`,
  `crates/protocol`, `crates/sdk/felix-client`, the cluster and loadgen test
  crates, `services/felix-broker-service`, `scripts/perf`, `Cargo.toml`,
  `Cargo.lock`, `rust-toolchain.toml`, `.cargo/config.toml`) or its own workflow file, to spare shared
  runners. Neither feeds `CI ok`. A new push cancels a PR's older run of ci, coverage,
  nightly, perf-pr, ipm-gate and tla-walk (tla-walk cancels only PR runs, not
  nightly or dispatch); runs on main are never cancelled.
- The `Instructions per message` workflow counts the CPU instructions the
  broker retires per acknowledged message (a durable Raft broker with periodic
  fsync, 256 B, acked, 48 keys over 48 shards, batch 1 and 64) and fails when a
  cell is more than 3% over `scripts/perf/ipm-baseline.toml`. The broker runs
  under callgrind, since hosted runners expose no hardware counters, so only
  user-space instructions are counted, less the idle broker's per-second cost.
  - Each cell runs five reps. Every rep starts a fresh broker with a fresh
    data directory, gives it a fixed unmeasured warm-up, then counts. The cell
    reports its median rep, with the spread across reps. A broker that runs
    for a long time under callgrind gets slower, so reusing one would make
    later reps cost more.
  - Under callgrind the broker runs hundreds of times slower than native while
    its timers fire in real time, so its per-second work (timers, heartbeats,
    polling) is a large share of what it executes. Taking the idle rate off
    does not remove all of it, because a broker under load does more per
    second than an idle one. So every cell publishes at a fixed pace for a
    fixed time, and its figure includes the same per-second work in every rep
    and every run:
    - `b1`: 20 msg/s on aarch64 and 100 msg/s on x86_64, for 300 s.
    - `b64`: 4,000 msg/s for 20 s on both.

    Unpaced, a rep on a fast runner finishes sooner and picks up less
    per-second work: one x86_64 `b64` run read 2% under the others because its
    reps took 4.8 s where theirs took 9. More reps do not help with that, since
    every rep in a run shares the runner.
  - What each cell can see, in instructions per message. `b1` publishes one
    message at a time, so it measures the cost of a lone publish plus the
    broker's per-second work at that pace: about 508k on aarch64 and 284k on
    x86_64, against about 190k and 184k flat out. It turns red at +3%: about
    +15k per message on aarch64 and +8.5k on x86_64, which is roughly 8% and
    5% of the flat-out cost. A smaller change to the per-message path cannot
    move it past the tolerance. `b1` is the cell for a cost added to a lone
    publish (a timer, a wake, a syscall, an extra task hop). `b64` publishes
    batches, costs about 10.8k (aarch64) and 11.3k (x86_64) per message, and
    turns red at about +325 to +340 per message, which is about 3%. It is the
    cell for a cost added per message inside a batch. A cost added per batch
    or per publish call shows in `b1` first.
  - What it cannot see: kernel time, time spent waiting on the disk or the
    network, and native per-second cost (callgrind's figures say nothing about
    a broker at full speed). A change to per-second work moves both cells as
    if it were per-message cost.
  - A rep that could not hold its pace is a measurement failure, not a
    regression: it issued more than 3% off the rate, or took more than 3%
    (plus 1 s for the last acks) over its time. The run fails, says which rep
    and why, and still uploads the result file with every rep in it. Rerun it.
    The result also records the runner's CPU model.
  - If the gate is red, read which cell and why. A pace or measurement
    failure is a bad run: rerun it. A cell over the tolerance in a run that
    held its pace is a real cost: find what the PR added per message (look at
    the `b64` cell first, it is the more sensitive one) and remove it, or
    explain it and refresh the baseline as below. Do not rerun a red gate until
    it goes green.
  - A run whose baseline was taken at another pace fails with exit code 3
    and the message "baseline predates the fixed pace: refresh needed". It is
    not a regression and says so in the job summary and the annotation; the job
    is red until the baseline is refreshed.
  - To refresh both architectures in one go from a pushed branch, run
    `scripts/perf/ipm_refresh.sh <branch>`. It dispatches five runs, waits,
    and rewrites both sections of `scripts/perf/ipm-baseline.toml`. The test
    cluster opens Raft members in written-commit mode, as a deployment does.
  - If the extra cost is deliberate, dispatch the workflow five times on the
    PR's head commit and run `python3 scripts/perf/ipm_gate.py update` with
    the five `ipm-result.json` artifacts (one per architecture per run). It
    writes the median of the five runs. Commit the baseline with a sentence on
    what the instructions buy. A toolchain bump updates it the same way. A
    single run is never a baseline: across five runs of one commit `b1` moved
    by 0.6% (aarch64) and 0.4% (x86_64), and `b64` by 0.9% and 2.1%.

### How pull request CI picks its jobs

On a pull request, the `changes` job in `.github/workflows/ci.yml` runs
`scripts/ci/affected.py` over the changed files and turns the heavy jobs on or
off. Pushes to `main` and manual runs still run everything, so a gap in the
mapping shows up as a red `main`, not a silent miss.

- A changed crate affects itself and every workspace crate that depends on
  it, from `cargo metadata`. A change only under a crate's tests (`tests/`,
  `benches/`, `examples/`, a `tests.rs` or `tests/` module) affects that crate
  alone.
- Each job runs when a crate it builds is affected. The cluster, simulator,
  image and compose jobs follow the broker, control plane, loadgen and
  `felixctl` binaries and the `felix-cluster` harness. The chart job runs on
  any non-test Rust change, since its observability check reads metric names
  from the code. Fuzzing follows the fuzzed decoder crates.
  The Python and Node conformance jobs follow their binding, `felix-client`,
  and the broker service, conformance kit and harness with everything they
  depend on. Chart, Docker, compose and script files turn on the jobs that
  read them, and docs pages turn on the crate tests that read them.
- The `test` job runs the affected crates' doctests. `Coverage` runs the whole
  workspace whenever any crate is affected, since its gate compares totals.
- `Cargo.toml`, `Cargo.lock`, the toolchain, `.cargo/`, `Taskfile.yml`, any
  `build.rs`, `vendor/`, the CI workflow, its actions, the selector itself, and
  any path the script does not know run everything.

To force a full run, add the `ci:full` label and re-run the workflow (or
push). Check what a change selects locally with
`git diff --name-only origin/main... | python3 scripts/ci/affected.py`.
