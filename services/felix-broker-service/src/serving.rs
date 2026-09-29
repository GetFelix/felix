//! Serving clients.
//!
//! [`quic`] accepts connections, decodes frames and runs the per-message work.
//! [`auth`] checks the token on each action. `cache_routing` decides which
//! broker answers for a cache key, `group_ops` is the consumer-group work
//! behind the queue handlers, and `core_shards` pins stream work to cores.
//! [`kafka`] is the read-only Kafka listener around the `felix-kafka` crate.
//! [`limits`] holds the per-address connection caps and per-tenant publish
//! quotas the listeners enforce.
//! [`forward`] sends a publish or cache operation to the broker that owns its
//! shard, and answers the ones other brokers send here.

pub mod auth;
pub(crate) mod cache_routing;
pub(crate) mod commit_ops;
pub(crate) mod core_shards;
pub mod forward;
pub(crate) mod group_ops;
pub mod kafka;
pub mod limits;
pub mod quic;
pub(crate) mod tls;
