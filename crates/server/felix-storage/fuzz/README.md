# Storage fuzzing

libFuzzer targets for the durable segment format and the other files a log
directory holds. They exist because segment bytes are untrusted input: from a disk that may have rotted or been cut mid-write,
and from a replication peer over the network. A panic
in this decoder is a remote crash.

## Targets

| Target | Input | Asserts |
| --- | --- | --- |
| `segment_record` | one record's bytes | decoding never panics; a successful decode reports exactly the bytes it consumed and re-encodes identically |
| `segment_recovery` | a whole segment file | recovery returns a contiguous, self-consistent prefix or a typed error — never a log with a hole |
| `sparse_index` | an index file | a malformed index loads as `None`; entries stay strictly ascending; every seek lands at or after the segment header |
| `cache_record` | one cache log record | decodes or is refused as corruption; a put re-encodes identically, a delete round-trips |
| `counter_record` | one counter log record, or a forwarded sum | decodes or is refused; a record re-encodes identically |
| `sidecar_state` | a durable mark, replica state, generation history or producer snapshot | none panics; each decoder also sees the input with its CRC fixed up, and whatever decodes round-trips |

`counter_record` and `sidecar_state` reach decoders the crate does not export,
through its `fuzzing` feature (`src/fuzzing.rs`).

## Running

```sh
cargo install cargo-fuzz
cd crates/server/felix-storage/fuzz

# A few minutes each is enough to catch regressions.
cargo +nightly fuzz run segment_record   -- -max_total_time=300
cargo +nightly fuzz run segment_recovery -- -max_total_time=300
cargo +nightly fuzz run sparse_index     -- -max_total_time=300

# Reproduce a crash the fuzzer found.
cargo +nightly fuzz run segment_record artifacts/segment_record/crash-<hash>
```

`task fuzz` runs every target briefly, and CI does the same on each PR. The
nightly workflow (`.github/workflows/fuzz-nightly.yml`) runs each for much
longer from a corpus it keeps between runs, and uploads any crash as the
`fuzz-crash-<target>` artifact. See
`docs-site/src/content/docs/development/fuzzing.md`.

The crate is deliberately outside the workspace: `cargo-fuzz` requires nightly
and links libFuzzer, and neither belongs in `cargo build --workspace`.

## Corpus

There are no committed seeds here; `corpus/` is libFuzzer's working directory
and is git-ignored. The deterministic counterpart is
`cargo test -p felix-storage --test format_fuzz`: the same properties, driven by
a seeded generator so they run on stable and reproduce exactly. An input worth
keeping belongs there as a test.

## What is *not* fuzzed here

Concurrency. Interleavings of append, flush and rollover are covered by
`tests/crash_recovery.rs`, which kills a real process mid-write, because
libFuzzer's single-threaded model cannot express them.
