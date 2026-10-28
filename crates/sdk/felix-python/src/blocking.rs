//! The synchronous surface: [`Client`] and the handles its reads return.
//!
//! Every call releases the GIL and blocks on the shared Tokio runtime, so a
//! thread pool or `asyncio.to_thread` gives concurrency. Iterating a handle
//! is the same as calling its read method in a loop.

mod cache_watch;
mod client;
mod producer;
mod sharded_subscription;
mod subscription;

pub(crate) use cache_watch::CacheWatchHandle;
pub(crate) use client::Client;
pub(crate) use producer::ProducerHandle;
pub(crate) use sharded_subscription::ShardedSubscriptionHandle;
pub(crate) use subscription::SubscriptionHandle;
