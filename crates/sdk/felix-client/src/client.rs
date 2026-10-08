//! [`Client`]: one broker, reached over multiplexed QUIC connections.
//!
//! Publish, cache and event traffic each run on their own streams. A client
//! built with [`Client::connect`] gives each kind its own pool of connections,
//! so one workload cannot head-of-line block another and each can be tuned
//! separately. One a [`crate::ClusterClient`] builds puts all of them on one
//! connection and adds more only when that one is saturated. The child
//! modules split `Client`'s API by area; the struct and its fields live here.

mod cache;
mod cache_watch;
mod commit;
mod connect;
mod discovery;
mod groups;

pub use groups::{GroupInfo, GroupMember, GroupPollOptions, GroupPosition};
mod publish;
mod subscribe;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize};

use felix_transport::QuicConnection;

use crate::cache::CacheWorker;
use crate::config::ClientRuntimeConfig;
use crate::connection::NodeConnections;
use crate::publish::{PublishAdmission, PublishSharding, PublishWorker, ShardStreams};

/// A client of one broker, over multiplexed QUIC connections.
///
/// Built with [`Client::connect`]. For a client that survives losing that
/// broker, use [`crate::ClusterClient`].
pub struct Client {
    /// The address this client was built for.
    dialled: SocketAddr,
    // Where each kind of stream is opened. Three separate sets for a client
    // built with `connect`, so each can have its own transport tuning; the
    // same set three times for a shared one.
    publish_node: Arc<NodeConnections>,
    cache_node: Arc<NodeConnections>,
    event_node: Arc<NodeConnections>,
    // The connections the publish and cache workers' streams are on. Those
    // streams are opened once, so a client that has lost one of these has
    // lost workers it will not get back.
    worker_connections: Vec<QuicConnection>,

    // Publish worker pool: multiple streams, each with one writer.
    publish_workers: Arc<Vec<PublishWorker>>,
    publish_sharding: PublishSharding,
    publish_admission: Arc<PublishAdmission>,
    // Shared by every publisher from this client, so a stream keeps one
    // writer however many publishers publish to it.
    publish_stream_hasher: ahash::RandomState,
    // Opened on demand, one per shard published to, beside the pool.
    publish_shard_streams: Arc<ShardStreams>,

    // Per-stream cache workers: each owns exactly one bi-directional QUIC stream and
    // serializes cache round-trips (encode -> write -> read -> decode).
    cache_workers: Vec<CacheWorker>,

    cache_request_counter: AtomicU64,
    cache_worker_rr: AtomicUsize,

    // In-flight work per connection slot, for the gauges. A cache count is
    // raised once a request is queued and lowered by the worker that answers
    // it; the two race, so the value is approximate.
    cache_conn_counts: Arc<Vec<AtomicUsize>>,
    event_conn_counts: Arc<Vec<AtomicUsize>>,
    auth_tenant_id: String,
    runtime_config: ClientRuntimeConfig,
    /// Optional requests this broker said it implements.
    server_features: u32,
}

#[cfg(test)]
mod tests;
