- `cargo bench --workspace --no-run` builds again: the broker's unit tests that
  slow the fsync now get `felix-storage`'s `fault-injection` feature in a
  release build too.
