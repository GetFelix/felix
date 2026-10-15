---
title: "Contributing to Felix"
description: "Licensing, sign-off, and the rules a pull request to Felix is reviewed against."
---

This page summarises
[CONTRIBUTING.md](https://github.com/GetFelix/felix/blob/main/CONTRIBUTING.md),
which is the canonical text. Read it before your first pull request.

## Licensing

Felix is split-licensed. The wire protocol, transport, client SDKs and
`felix-common` are Apache-2.0. The broker, the server libraries, the control
plane and the test tooling are AGPL-3.0-only.
[LICENSING.md](https://github.com/GetFelix/felix/blob/main/LICENSING.md) has the
table of which path is under which licence. `task publish:check` fails CI if a
crate's manifest drifts from that table, or if an Apache-2.0 crate depends on
an AGPL one. Know which side your change lands on before you start.

## Sign-off and CLA

Every contribution needs both:

1. **A DCO sign-off on each commit.** Commit with `-s`, which adds a
   `Signed-off-by:` trailer:

   ```bash
   git commit -s -m "your message"
   ```

2. **A CLA grant, once.** On your first pull request the CLA Assistant bot
   (`.github/workflows/cla.yml`) asks you to reply with a fixed phrase. The
   text is in [CLA.md](https://github.com/GetFelix/felix/blob/main/CLA.md). You
   keep your copyright.

## AI-assisted changes

AI tools are fine to use. You are responsible for what you submit, so review
generated code as if you wrote it. Disclose substantial AI assistance in the
PR description. A line such as "drafted with Claude Code, reviewed and tested
by me" is enough.

## How the code is organised

Reviewers hold changes to these rules. The full list is in CONTRIBUTING.md
under "How the code is organized".
[Project Structure](/development/project-structure/) shows the layout
they produce.

- Crates are grouped by role under `crates/` (`protocol`, `server`, `sdk`,
  `testing`). Deployables live in `services/`. Directories are named after the
  package.
- One module style: `foo.rs` with its children in `foo/`. No `mod.rs` (clippy's
  `mod_module_files` enforces this), no `#[path]`.
- `lib.rs` is a table of contents: crate docs, module declarations and
  re-exports.
- Name modules by what they are about, not by kind of code. No `utils`,
  `helpers` or `types`.
- Default to `pub(crate)`. The `unreachable_pub` lint catches a `pub` that
  nothing outside the crate uses.
- Unit tests go in `<module>/tests.rs`, declared as `#[cfg(test)] mod tests;`
  at the bottom of the module. Integration tests go in the crate's `tests/`.
- A decoder that reads bytes from outside the process gets a fuzz target. See
  [Fuzzing](/development/fuzzing/).
- Shared dependency versions go in `[workspace.dependencies]`. Members add
  features and never re-pin a version.

## Comments

Comment the reason, briefly. An ordering requirement, a failure mode or a limit
that exists for a reason deserves a sentence next to the code that depends on
it. Don't restate what the code says, and don't narrate history. If an
explanation needs paragraphs, put it in `docs/` and point to it from the code.

```rust
// Offsets are claimed before waiting on durability, so the batch keeps its
// place in the stream's order even while an earlier publish is still flushing.
let pending = log.begin_append(&payloads).await?;
```

`///` on a public item is documentation: say what the item does and what it
guarantees, and keep it accurate.

## Tests

Add tests for new behaviour. For a concurrency or durability fix, revert the
fix, watch the new test fail, then restore it. A regression test that passes
without the fix proves nothing.

Changes to cluster behaviour need a cluster test, and changes to the modelled
replication protocol need the TLA+ model to follow. See
[How Felix Is Tested](/architecture/testing/).

## Docs ship with the change

`docs/` and this site make specific claims about what is implemented. If you
ship a capability, update the pages that describe it in the same PR, including
the status table in
[What Felix Is For](/getting-started/what-felix-is-for/). If you find a
claim the code cannot back, fix the claim. `task docs:evidence` checks that
cited tests and `FELIX_*` variable names still exist.

## Before you open a PR

```bash
task lint            # fmt check, workspace clippy -D warnings, per-crate feature checks
task test            # the workspace tests, with Postgres when Docker or Podman is available
task docs:evidence   # doc citations and env-var names
task demo:check      # if you changed a public API the standalone demos use
```

CI runs more than these; [Building & Testing](/development/building/)
lists every job. Keep PRs focused. A bug fix does not need an unrelated
refactor riding along.

## Releases

A release is cut by pushing a `v*` tag, and `.github/workflows/release.yml`
builds and publishes it. The workspace, the Python wheel, the npm package and
the Helm chart each carry their own version, so run
`task release:check -- v<version>` first to confirm they all match the tag.
Before tagging, try the latest [nightly build](/development/building/#nightly-builds),
which is main as of last night, built the way a release is.
