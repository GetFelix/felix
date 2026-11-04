//! One cluster carrying many shards at once, and what a broker still owes a
//! small shard while it is busy with hundreds of others.
//!
//! Every test here starts real broker processes; see `docs/cluster-harness.md`.
//! The nightly size is `#[ignore]`; `docs/cluster-harness.md` ("Shard
//! starvation") gives the commands, and `.github/workflows/scale-nightly.yml`
//! runs both sizes by name.

mod failover_heal;
mod lease_starvation;
