//! The Rust client: how an application publishes, subscribes, and reads a
//! cache.
//!
//! **Start at [`Client`].** It connects to one broker over QUIC and is the
//! root of everything else: [`Client::publisher`] for a [`Publisher`],
//! [`Client::subscribe`] for a [`Subscription`], and the cache and counter
//! calls directly on it. Beyond that:
//!
//! - [`ClusterClient`] takes several broker addresses and rebuilds its
//!   connection from the rest when one fails, so an application outlives the
//!   broker it happened to reach.
//! - [`ShardedSubscription`] follows every shard of a stream at once and
//!   reports per-shard offsets, since one number cannot describe where a
//!   sharded consumer got to.
//! - [`IdempotentProducer`] re-sends across a reconnection without
//!   duplicating.
//!
//! # The rule that shapes this crate
//!
//! This is a latency-oriented client, not a general-purpose "do anything
//! concurrently" QUIC client. Quinn's `SendStream` is effectively a
//! single-writer resource: concurrent writes from several tasks are
//! serialized by a mutex inside Quinn, which is a performance cliff under
//! load. So every stream here has one writer loop, fed through a bounded
//! queue, and parallelism comes from *more* connections or *more* streams —
//! never from writing to one stream from two tasks. The docs of the modules
//! that own each kind of stream say how they hold to that.
//!
//! The frame codec and message types are `felix-wire`; the QUIC layer is
//! `felix-transport`. This crate is the application-facing shape over both.
//!
//! # Where things live
//!
//! - `client`: [`Client`] itself, one file per area of its API.
//! - `connection`: where pooled connections go, stream authentication, and
//!   the router that hands server-opened event streams to subscriptions.
//! - `publish`: [`Publisher`], its writer tasks, admission, acks, and
//!   [`IdempotentProducer`].
//! - `subscribe`: [`Subscription`] and the pipeline that feeds it.
//! - `cache`: the cache workers and [`CacheWatch`].
//! - `cluster`: [`ClusterClient`] and the sharded views built on it.
//! - `config`: [`ClientConfig`], its defaults, and the env and YAML overrides.
//! - `tls`: [`quic_client_config`], the QUIC TLS setup, whether it offers
//!   the `felix/1` ALPN, and a client certificate ([`ClientIdentity`]).
//! - `telemetry` and [`timings`]: counters and sampled timings, mostly
//!   compiled out unless the `telemetry` feature is on.

// First, and `#[macro_use]`: the `t_*` metric macros are textually scoped, and
// importing them by path would leave the import unused in whichever build
// compiles their call sites out.
#[macro_use]
mod telemetry;

mod auth;
mod cache;
mod client;
mod cluster;
mod commit;
mod config;
mod connection;
mod error;
mod frame_io;
mod publish;
mod subscribe;
#[cfg(test)]
mod test_support;
mod tls;

pub mod timings;

pub use auth::{RefreshingToken, TokenFuture, TokenProvider};
pub use cache::{CacheChange, CacheWatch, CacheWatchFilter, CacheWatchItem};
pub use client::Client;
pub use cluster::{
    ClusterCacheWatch, ClusterClient, ClusterSubscription, ReconnectPolicy, ShardEvent,
    ShardOffsets, ShardedCacheWatch, ShardedCacheWatchItem, ShardedGroup, ShardedGroupRecord,
    ShardedSubscription,
};
pub use commit::{CommitError, CommitOp, CommitReceipt, StateValue};
pub use config::{ClientConfig, ClientSubQueuePolicy};
pub use error::{
    BrokerError, NotLeaderError, PublishRefused, SubscribeCursorError, SubscriptionLost,
};
pub use publish::{IdempotentProducer, PublishSharding, Publisher};
pub use subscribe::{Event, ShardMoved, Subscription};
pub use telemetry::{
    FrameCountersSnapshot, frame_counters_snapshot, publishes_forwarded, reset_frame_counters,
};
pub use tls::{
    ClientIdentity, quic_client_config, quic_client_config_with_identity, root_store_from_pem_file,
};

pub use felix_wire::routing::ShardRouting;
pub use felix_wire::{
    AckMode, BrokerEndpoint, CursorErrorReason, ErrorCode, ErrorDetail, PublishRefusalReason,
    RetryClass, StartPosition,
};
