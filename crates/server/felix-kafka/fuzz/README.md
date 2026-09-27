# Kafka listener fuzzing

`kafka_records` fuzzes a partition's record batches, the bytes any Kafka
client sends in a produce, so mutation reaches the batch format and the codecs.
It must decode or refuse, never panic, and no batch may decompress past
`MAX_BATCH_BYTES`.

```bash
cargo install cargo-fuzz
task fuzz                    # every target in the repo, 60s each
cd crates/server/felix-kafka/fuzz
cargo +nightly fuzz run kafka_records corpus/kafka_records seeds/kafka_records -- -max_total_time=300
```

`seeds/` is committed and written by `gen_seeds.py`; `corpus/` is libFuzzer's
working directory and is git-ignored. See `crates/protocol/felix-wire/fuzz/README.md`
for why they are kept apart. `seeds/kafka_request/` holds valid request frames
used by the deterministic tests.

The deterministic subset — every seed decodes, and every truncation of one is
refused without a panic — runs in `cargo test -p felix-kafka` as
`src/api/tests/parse.rs`.
