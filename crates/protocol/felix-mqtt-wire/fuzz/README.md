# MQTT packet fuzzing

`mqtt_packet` decodes arbitrary bytes as the stream a device sends. Its first
byte picks the state the rest is decoded in: before `CONNECT` (only `CONNECT` is
accepted), or after it at 3.1.1 or 5.0. It must decode, wait for more bytes, or
refuse, never panic, and a packet it decodes must encode and decode back to the
same packet, in the same bytes the second time.

```bash
cargo install cargo-fuzz
task fuzz                    # every target in the repo, 60s each
cd crates/protocol/felix-mqtt-wire/fuzz
mkdir -p corpus/mqtt_packet
cargo +nightly fuzz run mqtt_packet corpus/mqtt_packet seeds/mqtt_packet -- -max_total_time=300
```

It also runs for much longer every night; see
`docs-site/src/content/docs/development/fuzzing.md` for where a crash shows up
and how to reproduce it.

`seeds/mqtt_packet/` is committed and written by `gen_seeds.py` from the golden
vectors in `../tests/vectors/`. Each seed is a selector byte and then packets.
`corpus/` is libFuzzer's working directory and is git-ignored. The seeds are
also decoded by `../tests/generated.rs`, so they cannot go stale unnoticed.
