---
title: "Fuzzing"
---

Every decoder that reads bytes Felix did not just produce in memory has a
[libFuzzer](https://llvm.org/docs/LibFuzzer.html) target. The targets run twice:
briefly on every pull request, as a regression gate, and for much longer every
night, as a campaign that builds on what earlier nights found.

## The targets

Three fuzz crates, each outside the workspace because `cargo-fuzz` needs
nightly and links libFuzzer:

| Crate | Target | Decoder | Where its bytes come from |
| --- | --- | --- | --- |
| `crates/protocol/felix-wire/fuzz` | `frame` | client frame header and frame, strict and flag-lenient | any client |
| | `client_message` | JSON control messages | any client |
| | `binary_payload` | binary publish, ack and event-batch bodies | any client |
| | `internal_message` | broker-to-broker frames, envelope and correlation id | a peer broker |
| `crates/server/felix-kafka/fuzz` | `kafka_request` | Kafka request header and every body the listener answers, plus SASL/PLAIN | any Kafka client, before auth |
| | `kafka_records` | produce record batches and their codecs | any Kafka client |
| `crates/server/felix-storage/fuzz` | `segment_record` | one segment record | disk, and replication peers |
| | `segment_recovery` | a whole segment file at startup | disk |
| | `sparse_index` | a segment's sparse index | disk |
| | `cache_record` | a cache log record | disk, and replication peers |
| | `counter_record` | a counter log record and a forwarded counter sum | disk, replication, peer brokers |
| | `sidecar_state` | the durable mark, replica state, generation history and producer snapshot | disk |

Each target asserts more than "does not panic": decoded values re-encode to
what they came from, lengths agree with the bytes present, and nothing
decodes past a bound. The doc comment at the top of each target lists its
properties. The per-shard state files are checksummed, so `sidecar_state` also
feeds each decoder a copy with the CRC fixed up. Otherwise mutation would
almost never get past the checksum.

## Per pull request

The `fuzz` job in `ci.yml` runs `task fuzz` with `FUZZ_SECONDS=30`: every target
for 30 seconds, starting from the committed seeds. That catches a decoder
change that breaks something the seeds or a minute of mutation reaches. It is
not meant to find anything deep.

## Nightly

`.github/workflows/fuzz-nightly.yml` runs at 03:17 UTC and can be started by
hand from the Actions tab (**Nightly fuzz → Run workflow**), with a per-target
budget and an option to minimize the corpus.

- **One job per target**, run in parallel, each for 25 minutes by default.
- **The corpus carries over.** Each target's corpus is kept in the Actions
  cache under `fuzz-corpus-<target>-<run id>`. A run restores the newest entry
  for its target, fuzzes on top of it with the committed seeds as a second
  input, and saves the result under a new key. The two newest entries per
  target are kept and older ones are deleted.
- **The corpus is minimized weekly.** On Sundays, or when the run is started
  with *minimize* set, `cargo fuzz cmin` drops every input that adds no
  coverage before the corpus is saved.
- **A crash fails the run.** The job for that target goes red, the input that
  crashed it is uploaded as the `fuzz-crash-<target>` artifact, and the job
  summary shows the commands to reproduce it. A hang longer than 30 seconds on
  one input counts as a crash.

## Reproducing a crash

Download the `fuzz-crash-<target>` artifact from the failed run (the run page
lists it under **Artifacts**; `gh run download <run id> -n fuzz-crash-<target>`
does the same). It holds a `crash-<hash>`, `timeout-<hash>` or `oom-<hash>`
file. Then, in the target's crate:

```bash
cargo install cargo-fuzz
cd crates/server/felix-kafka/fuzz          # the crate the target lives in
cargo +nightly fuzz run kafka_request path/to/crash-<hash>
cargo +nightly fuzz tmin kafka_request path/to/crash-<hash>   # optional: shrink it
```

A local run writes its crashes to `<crate>/fuzz/artifacts/<target>/`, and those
reproduce the same way.

Fix the decoder, then add the crashing input as a regression test in the
decoder's own crate, so the fix is held by `cargo test` on stable and not
only by the next fuzz run. If the input is worth keeping as a starting point
for mutation, add it to the target's `seeds/` directory too.

## Running locally

```bash
cargo install cargo-fuzz
task fuzz                    # every target, 60 s each
FUZZ_SECONDS=600 task fuzz   # longer

cd crates/protocol/felix-wire/fuzz
cargo +nightly fuzz run frame corpus/frame seeds/frame -- -max_total_time=300
```

`seeds/` is committed and read-only. `corpus/` is libFuzzer's working directory
and is git-ignored. Passing `corpus/<target>` first is what keeps new inputs out
of `seeds/`.

## Adding a target

1. Add `fuzz_targets/<name>.rs` and a `[[bin]]` entry to the crate's
   `Cargo.toml`. A decoder the crate does not export is reached through its
   `fuzzing` feature (`felix-kafka` and `felix-storage` have one).
2. Add the target to `task fuzz` in `Taskfile.yml` and to the matrix in
   `fuzz-nightly.yml`.
3. Add a row to the table above, and to the crate's `fuzz/README.md`.
