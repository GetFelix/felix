//! The broker process: everything around [`felix_broker::Broker`] that makes
//! it a node in a cluster.
//!
//! **The entry point is [`node::run_with_shutdown`]**: it starts the listeners
//! and the background tasks and owns the shutdown order. `main.rs` is argument
//! handling around it. The rest of the crate is the subsystems it wires
//! together, exposed as a library so the integration tests in `tests/` can
//! drive them without a process.
//!
//! `felix-broker` holds the semantics — streams, caches, queues, the publish
//! path. Nothing in that crate knows about sockets, the control plane, or
//! other brokers. Everything here is that missing half, and the modules group
//! into four jobs.
//!
//! # 1. Serving clients
//!
//! [`serving::quic`] accepts connections, decodes frames and runs the
//! per-message work, and [`serving::auth`] checks the token on each action.
//! A publish for a shard another broker owns goes there through
//! [`serving::forward`]. `serving::cache_routing` decides which broker answers
//! for a cache key, and `serving::group_ops` and `serving::core_shards` are
//! the queue and shard operations behind the handlers.
//!
//! # 2. Belonging to a cluster
//!
//! [`cluster::membership`] registers this node and heartbeats;
//! [`cluster::credential`] holds the token it presents and refreshes it before
//! expiry. [`cluster::catalog_sync`] syncs the metadata catalog,
//! `cluster::node_catalog` turns node ids into addresses,
//! `cluster::client_endpoints` is what this broker tells a client about where
//! to connect, and [`cluster::lease`] is the authority to serve at all —
//! renewed by the same heartbeat, checked cheaply on admission and against the
//! clock again before any record is committed.
//!
//! # 3. Owning shards
//!
//! [`shards::watch`] follows what the control plane assigned, by snapshot and
//! then by change feed. [`shards::lifecycle`] is what this broker has actually
//! *done* about that, which is deliberately separate state: a shard serves
//! only once its log is open, and only at the generation the control plane
//! currently names. [`shards::routing`] answers the question every publish
//! asks first: is this shard mine, should it be forwarded, or can nobody serve
//! it right now?
//!
//! # 4. Replicating
//!
//! The `felix-replication` crate ships committed records to the followers of
//! every shard this broker leads, and applies them on a follower, over its
//! broker-to-broker transport in `felix_replication::peer`. It is also what
//! tells the control plane which replicas hold the log, which is what a
//! failover and a planned move both read. `node` wires it in: the ingress
//! router, the shard fence and the node credential are what it reads through
//! its traits.
//!
//! [`config`] parses the environment and decides where logs live, and
//! [`observability`] serves metrics and health and holds the opt-in per-stage
//! latency instrumentation in [`observability::timings`].
//!
//! Each module owns its own tests at `<module>/tests.rs` and its own metrics
//! at `<module>/metrics.rs`.

pub mod serving;

pub mod cluster;

pub mod shards;

pub mod config;
pub mod node;
pub mod observability;

// HTTP helpers shared by the unit tests of several modules.
#[cfg(test)]
mod test_support;
