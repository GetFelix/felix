//! A local multi-node Felix cluster, for integration and failure tests.
//!
//! Starts one control plane and N broker processes, waits for each of them to
//! actually be usable, and tears everything down when the [`Cluster`] is
//! dropped.
//!
//! # What is real
//!
//! **Brokers are real processes.** Each has its own client-facing QUIC port,
//! internal peer port, metrics port, node identity, credential, and data
//! directory, and they reach each other over the internal transport exactly as
//! they would in a deployment. Stopping one is a real process exit.
//!
//! **The control plane is in process.** See [`controlplane`] for why, and for
//! what that does and does not exercise.
//!
//! # Waiting
//!
//! Nothing here sleeps for a fixed duration and hopes. Every wait polls a real
//! signal — a readiness endpoint, a node's placement eligibility in the catalog,
//! an assignment naming a leader — and fails with what it was still waiting for
//! rather than proceeding into a confusing failure later.
//!
//! # Layout
//!
//! [`Cluster`] is what a test holds. Its operations live in one file per
//! concern under `cluster/`: start-up, streams, groups, caches, ownership,
//! placement, metrics, and faults. [`Fault`] is the vocabulary a test injects
//! faults in, and `proxy` the network the link faults act on.
//! [`BrokerNode`] is one broker process and
//! [`ClusterConfig`] is what a test asks for. [`scenarios`] holds the
//! assertions that must hold on any deployment, and [`session`] is how the
//! `felix-cluster` CLI finds a cluster started in another terminal.
//! [`history`] runs randomized fault campaigns and checks what the clients
//! saw against what a replicated log promises.

pub mod client;
mod cluster;
mod config;
pub mod controlplane;
mod fault;
pub mod history;
mod node;
pub mod pki;
pub mod ports;
mod proxy;
pub mod scenarios;
pub mod session;
pub mod wait;

pub use cluster::{Assignment, Cluster};
pub use config::{CacheSpec, ClusterConfig, StreamSpec};
pub use controlplane::ControlPlane;
pub use fault::{ClockFault, Endpoint, Fault, FsyncFault};
pub use node::BrokerNode;
