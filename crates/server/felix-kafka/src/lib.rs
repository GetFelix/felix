//! Kafka wire compatibility for a Felix broker.
//!
//! A Kafka consumer that assigns its own partitions — `kcat`, a librdkafka or
//! Java consumer calling `assign` — can read Felix streams through this crate,
//! and a Kafka producer, idempotent or not, can write to them. Topics are
//! `<namespace>.<stream>`, a partition is a shard, and offsets are Felix's own.
//! Consumer groups and transactions are refused with an error the client
//! prints. `docs/kafka-compatibility.md` has the whole mapping and the reasons
//! behind it.
//!
//! **Start at [`KafkaService`]**: the broker service accepts a connection,
//! terminates TLS, and hands the stream to
//! [`KafkaService::serve_connection`]. What the listener needs from the
//! cluster — credential checks, shard leaders, other brokers' addresses — it
//! asks through [`Cluster`], which the service implements.

mod api;
mod cluster;
mod errors;
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub mod fuzzing;
mod metrics;
mod records;
mod service;
mod topic;

pub use cluster::{
    Cluster, Endpoint, Placement, Principal, ShardRef, WriteError, WritePermit, kafka_node_id,
};
pub use service::{KafkaService, Settings};
