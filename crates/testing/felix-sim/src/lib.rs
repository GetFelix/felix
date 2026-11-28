//! A deterministic simulation of groups of the per-shard Raft engine.
//!
//! Members run in one process on tokio's virtual time and reach each other
//! through simulated links instead of QUIC, so faults that are slow or rare
//! on a real cluster (a partition, a lost answer, a crash mid-request) can be
//! injected from a seed and replayed. See `docs/simulation.md`.
//!
//! - `raft` (feature `raft`): the groups, their faults and their checks.
//!   `tests/raft_seeds.rs` shows it used.
//! - [`run`]: comparing two runs of one seed.

#[cfg(feature = "raft")]
mod clock;
#[cfg(feature = "raft")]
mod payload;
#[cfg(feature = "raft")]
mod power;
#[cfg(feature = "raft")]
pub mod raft;
pub mod run;
