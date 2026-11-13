# Raft message fuzzing

`raft_request` fuzzes the body of a `Raft` frame, the bytes a listener reads
from a peer broker: every message kind, entries, memberships and the sender's
deadline. It must decode or refuse, never panic, and a count the bytes cannot
back must not size an allocation. What decoded must encode the same way twice.

`raft_answer` does the same for the body of a `RaftAnswer` frame, which a
sender reads back.

```bash
cargo install cargo-fuzz
task fuzz                    # every target in the repo, 60s each
cd crates/server/felix-replication/fuzz
cargo +nightly fuzz run raft_request corpus/raft_request -- -max_total_time=300
cargo +nightly fuzz run raft_answer corpus/raft_answer -- -max_total_time=300
```

Both run for much longer every night; see
`docs-site/src/content/docs/development/fuzzing.md` for where a crash shows up
and how to reproduce it. `corpus/` is libFuzzer's working directory and is
git-ignored.

The deterministic subset (every message round-trips, and every single-bit
corruption is refused without a panic) runs in
`cargo test -p felix-replication --features raft` as `src/raft/wire/tests.rs`.
