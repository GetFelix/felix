# Changelog fragments

Add one file here per change instead of editing `CHANGELOG.md`, so pull requests
stop conflicting on the same lines.

## Naming

`changelog.d/<name>.<section>.md`

- `<name>` is free. Use the issue or PR number and a slug: `1220-durable-mark`.
- `<section>` is one of `security`, `added`, `changed`, `deprecated`, `removed`,
  `fixed`, `performance`, `documentation`.

## Body

Markdown bullets, written exactly as they should read under the heading. A change
that belongs in two sections gets two files. No headings: the file name sets the
section, and `--check` refuses a `#`, `##` or `###` line.

`changelog.d/1220-durable-mark.fixed.md`:

```markdown
- A durable publish no longer acknowledges before its mark is written (#1220).
  The ack now waits for the commit, so a crash cannot lose an acknowledged record.
```

## Releasing

`python3 scripts/changelog_assemble.py <version> [--date YYYY-MM-DD]` (or
`task changelog:assemble -- <version>`) merges the fragments into the
`## [Unreleased]` section of `CHANGELOG.md` in a fixed section order, retitles it
as the release, leaves an empty `## [Unreleased]`, and deletes the fragments.
Entries already written in `CHANGELOG.md` are kept. The release workflow refuses a
tag while fragments are still here.

CI runs `task changelog:check` to validate names, sections and non-empty bodies.
