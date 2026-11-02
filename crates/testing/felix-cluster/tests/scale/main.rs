//! One cluster carrying many shards at once, and what a broker still owes a
//! small shard while it is busy with hundreds of others.
//!
//! Every test here starts real broker processes; see `docs/cluster-harness.md`.
//! Both sizes are `#[ignore]` for now; `docs/cluster-harness.md` ("Shard
//! starvation") gives the commands, and `.github/workflows/scale-nightly.yml`
//! runs them by name.

mod lease_starvation;
