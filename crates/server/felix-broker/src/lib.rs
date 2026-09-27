//! The broker core: streams, caches and queues over one log.
//!
//! **Start at [`Broker`].** It owns every stream and cache this process
//! serves, and the publish path runs through it: [`Broker::claim_publish`]
//! takes offsets and a place in the commit order, [`Broker::complete_publish`]
//! makes the record durable and fans it out. A [`StreamHandle`] is the cached
//! lookup a hot publish path holds instead of resolving the stream again.
//!
//! This crate is the *logic*, not the process. It has no sockets and no
//! control-plane client: `services/felix-broker-service` wires those around it. That split
//! is why the semantics can be tested without a network.
//!
//! # One log, three semantics
//!
//! A stream is the log read forward, a cache is a key-to-latest-value
//! projection of it, and a queue is a durable cursor over it. They are not
//! three subsystems:
//!
//! - [`Subscription`] and [`Cursor`] read the log forward.
//! - [`CacheWatchHub`] delivers each applied write in the shard's write order.
//! - [`GroupReader`], [`ConsumerGroups`] and [`DeadLetters`] are the queue: a
//!   claim, an acknowledgement, a redelivery after a visibility timeout, and an
//!   offset given up on.
//! - [`replication`] stores what a leader shipped, on a follower.
//! - [`DurableStorage`] is the disk-backed side, for streams created
//!   `durable: true`.
//!
//! Apart from [`replication`] and [`timings`], everything public is
//! re-exported at the crate root, so downstream code writes
//! `felix_broker::<Name>`.
//!
//! # Invariants worth knowing before changing anything here
//!
//! Offsets are taken *before* the durability wait, so a batch claims its place
//! in the stream's order the instant its offsets are consumed; `CommitSequencer`
//! in `felix-storage` then makes later publishes wait behind earlier ones
//! whether those succeed, fail, or are cancelled. Fanout happens *after*
//! durability. Reordering those steps is almost always a bug — see
//! `docs/architecture.md`.

mod broker;
mod cache;
mod durable;
mod error;
mod handoff;
mod queue;
mod stream;
mod telemetry;

pub mod replication;
pub mod timings;

pub use broker::{
    Broker, CacheMetadata, ClaimedPublish, ConsistencyLevel, IdempotentOutcome, LogKind,
    PublishOutcome, RECORD_SEQUENCE_WRAP, StreamHandle, StreamMetadata,
};
pub use error::{BrokerError, NotReadable, Result};
pub use handoff::{ShardHandoff, ShardMoved};

// Streams.
pub use broker::{Cursor, HistoryRange, JoinOffsets, ResumedSubscription};
pub use felix_wire::StartPosition;
pub use stream::{
    DeliveryEnvelope, ReadBound, ReadBounds, SubQueuePolicy, Subscription, SubscriptionGuard,
    SubscriptionReceiver,
};

// Caches.
pub use cache::{CacheChangeEvent, CacheWatchFilter, CacheWatchHub, CacheWatchSubscription};

// Queues.
pub use queue::{Claimed, ConsumerGroups, DeadLetters, GroupKey, GroupReader};

// Durability.
pub use durable::{DurableStorage, StreamLog};
