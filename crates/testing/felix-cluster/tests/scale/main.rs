//! One cluster carrying many shards at once, and what a broker still owes a
//! small shard while it is busy with hundreds of others.
//!
//! Every test here starts real broker processes; see `docs/cluster-harness.md`.
//! Run with `cargo test -p felix-cluster --test scale`. The nightly size is
//! ignored by default; `.github/workflows/scale-nightly.yml` runs it with
//! `-- --ignored`.

mod lease_starvation;
