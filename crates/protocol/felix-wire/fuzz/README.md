# Protocol fuzzing

The storage format is written by Felix; these bytes are written by whoever
connects. That is the whole reason this directory exists separately from
`crates/server/felix-storage/fuzz`, and why the properties here are about refusing
input rather than recovering from it.

```bash
cargo install cargo-fuzz
task fuzz                    # every target, 60s each
FUZZ_SECONDS=600 task fuzz   # an afternoon's worth
```

One target at a time, if that is what you want:

```bash
cd crates/protocol/felix-wire/fuzz
cargo +nightly fuzz run frame corpus/frame seeds/frame -- -max_total_time=300
```

A crash leaves its input in `artifacts/`; re-run the target with that file as
an argument to reproduce it. CI uploads that directory when a target dies,
because otherwise the bytes go with the runner.

Every target also runs for much longer each night, from a corpus kept between
runs; see `docs-site/src/content/docs/development/fuzzing.md`.

## `seeds` and `corpus`

`seeds/` is the committed set: one well-formed input per layout, so a run
starts from something structured rather than spending its budget guessing four
magic bytes. It is small and hand-generated, and it is read-only.

`corpus/` is libFuzzer's own working directory — it grows by thousands of files
as coverage is found, and it is git-ignored. Passing it first on the command
line is what keeps the growth out of `seeds/`.

The deterministic subset runs in the normal test suite as
`crates/protocol/felix-wire/tests/wire_fuzz.rs`, so a regression in the obvious cases
fails a plain `cargo test` rather than waiting for a fuzz budget.
