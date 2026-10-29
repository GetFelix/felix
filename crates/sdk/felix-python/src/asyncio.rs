//! The `async def` surface.
//!
//! Python realtime backends are asyncio backends, so this is the API most
//! callers should reach for: every method returns an awaitable driven by the
//! caller's own event loop, and a subscription is an `async for`.
//!
//! It wraps exactly the same [`ClusterClient`] the synchronous surface does,
//! so reconnection, redirect-following and retry classification behave
//! identically whichever one an application picks. The only difference is who
//! waits: the sync surface blocks a thread with the GIL released, this one
//! yields to the event loop.
//!
//! The awaited work runs on the shared Tokio runtime rather than the Python
//! loop — `pyo3_async_runtimes` moves the result back — which is what lets a
//! subscription's background reader keep draining while the coroutine that
//! owns it is parked.
//!
//! Scope arguments outnumber clippy's default here for the same reason they do
//! on the synchronous surface; see `lib.rs`.
#![allow(clippy::too_many_arguments)]

mod cache_watch;
mod client;
mod producer;
mod sharded_cache_watch;
mod sharded_group;
mod sharded_subscription;
mod subscription;

pub(crate) use cache_watch::AsyncCacheWatch;
pub(crate) use client::AsyncClient;
pub(crate) use producer::AsyncProducer;
pub(crate) use sharded_cache_watch::AsyncShardedCacheWatch;
pub(crate) use sharded_group::AsyncShardedGroup;
pub(crate) use sharded_subscription::AsyncShardedSubscription;
pub(crate) use subscription::AsyncSubscription;
