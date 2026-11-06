//! A deterministic simulation of a Felix cluster.
//!
//! Brokers run in one process on tokio's virtual time and reach each other
//! through [`net::SimNet`] instead of QUIC, so faults that are slow or rare on
//! a real cluster (a partition, a lost answer, a crash mid-request) can be
//! injected from a seed and replayed. See `docs/simulation.md`.
//!
//! - [`net`]: the in-memory peer network.

pub mod net;
