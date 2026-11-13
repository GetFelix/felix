//! A deterministic simulation of a Felix cluster.
//!
//! Brokers run in one process on tokio's virtual time and reach each other
//! through [`net::SimNet`] instead of QUIC, so faults that are slow or rare on
//! a real cluster (a partition, a lost answer, a crash mid-request) can be
//! injected from a seed and replayed. See `docs/simulation.md`.
//!
//! The pieces, bottom up:
//!
//! - [`net`]: the in-memory peer network.
//! - [`clock`]: each node's clocks over the shared virtual time.
//! - [`controlplane`]: the real control plane in process, with faults.
//! - [`link`]: one broker's path to the control plane, which a fault can cut.
//! - [`cluster`]: three to five real brokers, the control plane and the
//!   network on one clock.
//! - [`nemesis`]: the faults, drawn from the seed.
//! - [`client`]: logical clients, and the history they record.
//! - [`check`]: checks on the live cluster, and how to add one.
//! - `raft` (feature `raft`): groups of the per-shard Raft engine on the same
//!   clock, with no broker service around them.
//! - [`run`]: one seed start to finish. [`run::run_seed`] is the entry point;
//!   `tests/seeds.rs` shows it used.

pub mod check;
pub mod client;
pub mod clock;
pub mod cluster;
pub mod controlplane;
pub mod link;
pub mod nemesis;
pub mod net;
#[cfg(feature = "raft")]
pub mod raft;
pub mod run;
