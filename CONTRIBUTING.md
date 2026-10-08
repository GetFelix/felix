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
- `task test` runs the full test suite (spins up Postgres locally if Docker
  or Podman is available; `CONTAINER_ENGINE=podman` forces Podman). See
  [Docker or Podman](docs-site/src/content/docs/getting-started/containers.md).
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
- `task lint` and `task test` should pass locally before you open a PR. CI
  runs both plus `cargo-deny`.
