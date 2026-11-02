# MQTT over WebSockets fuzzing

`ws_frames` takes arbitrary bytes in one of two shapes, picked by the first
byte: an HTTP upgrade request, or the stream of frames a client sends after it.

- The request must be accepted or refused, never panic.
- The frames must decode to the same payload, control frames and verdict whether
  they arrive whole or are cut into reads of any size (the second byte sets
  the size), and waiting for more input must take nothing from the buffer.

```bash
cargo install cargo-fuzz
task fuzz                    # every target in the repo, 60s each
cd crates/server/felix-mqtt/fuzz
mkdir -p corpus/ws_frames
cargo +nightly fuzz run ws_frames corpus/ws_frames seeds/ws_frames -- -max_total_time=300
```

It also runs for much longer every night; see
`docs-site/src/content/docs/development/fuzzing.md` for where a crash shows up
and how to reproduce it.

`seeds/ws_frames/` is committed: a valid upgrade, two refused ones, and frames
that are whole, fragmented, interleaved with a ping, closing, and text.
`corpus/` is libFuzzer's working directory and is git-ignored.
