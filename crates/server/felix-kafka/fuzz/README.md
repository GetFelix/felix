# Kafka listener fuzzing

`kafka_request` fuzzes a whole request frame after its length prefix: the
header, every body the listener answers, the refusals for the group and
transaction APIs, and the SASL/PLAIN message. It must decode, answer or refuse,
never panic. `seeds/kafka_request/` is its starting point.

`kafka_records` fuzzes a partition's record batches, the bytes any Kafka
client sends in a produce, so mutation reaches the batch format and the codecs.
It must decode or refuse, never panic, and no batch may decompress past
`MAX_BATCH_BYTES`.

```bash
cargo install cargo-fuzz
task fuzz                    # every target in the repo, 60s each
cd crates/server/felix-kafka/fuzz
cargo +nightly fuzz run kafka_records corpus/kafka_records seeds/kafka_records -- -max_total_time=300
cargo +nightly fuzz run kafka_request corpus/kafka_request seeds/kafka_request -- -max_total_time=300
```

Both run for much longer every night; see
`docs-site/src/content/docs/development/fuzzing.md` for where a crash shows up
and how to reproduce it.

`seeds/` is committed and written by `gen_seeds.py`; `corpus/` is libFuzzer's
working directory and is git-ignored. See `crates/protocol/felix-wire/fuzz/README.md`
for why they are kept apart. `seeds/kafka_request/` holds valid request frames,
which the deterministic tests also use.

The deterministic subset — every seed decodes, and every truncation of one is
refused without a panic — runs in `cargo test -p felix-kafka` as
`src/api/tests/parse.rs`.
