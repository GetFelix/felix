//! Python bindings for the Felix client.
//!
//! This is a **wrapper over `felix-client`, not a second implementation of the
//! protocol.** Reconnection, redirect-following, retry classification, offset
//! bookkeeping and the frame codec all live in the Rust client and are shared
//! by every language that binds to it. A Python-native client would be a
//! second place for those to be subtly wrong, in exactly the areas — failover
//! and delivery accounting — where subtly wrong is most expensive.
//!
//! There are two surfaces over the same client. `blocking` releases the GIL
//! and blocks on a shared Tokio runtime; `asyncio` returns awaitables driven by
//! the caller's event loop. Both hand back the same Python types (`types`), so
//! an application can switch between them without changing what it checks
//! for.
//!
//! A cache entry is named by tenant, namespace, cache, and key before its
//! value and options ever appear, so several of these methods carry more
//! arguments than clippy's default. Bundling them into a struct would move
//! the argument list rather than shorten it, and every Python caller has the
//! parts separately anyway — the same reasoning `StorageApi` records.

#![allow(clippy::too_many_arguments)]

mod args;
mod asyncio;
mod batch;
mod blocking;
mod closable;
mod commit;
mod conditional;
mod errors;
mod group_admin;
mod producer;
mod runtime;
mod sharded_cache_watch;
mod sharded_group;
mod stream_page;
mod tls;
mod token;
mod types;
mod watch_progress;

use pyo3::prelude::*;

#[pymodule]
fn _felix(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;

    module.add_class::<blocking::Client>()?;
    module.add_class::<blocking::SubscriptionHandle>()?;
    module.add_class::<blocking::CacheWatchHandle>()?;
    module.add_class::<blocking::ShardedSubscriptionHandle>()?;
    module.add_class::<blocking::ShardedCacheWatchHandle>()?;
    module.add_class::<blocking::ShardedGroupHandle>()?;
    module.add_class::<blocking::ProducerHandle>()?;

    module.add_class::<asyncio::AsyncClient>()?;
    module.add_class::<asyncio::AsyncSubscription>()?;
    module.add_class::<asyncio::AsyncCacheWatch>()?;
    module.add_class::<asyncio::AsyncShardedSubscription>()?;
    module.add_class::<asyncio::AsyncShardedCacheWatch>()?;
    module.add_class::<asyncio::AsyncShardedGroup>()?;
    module.add_class::<asyncio::AsyncProducer>()?;

    module.add_class::<types::Event>()?;
    module.add_class::<types::GroupRecord>()?;
    module.add_class::<types::CacheChange>()?;
    module.add_class::<types::CacheWatchLagged>()?;
    module.add_class::<types::CacheWatchShardMoved>()?;
    module.add_class::<types::CacheWatchFilter>()?;
    module.add_class::<types::ShardRecord>()?;
    module.add_class::<types::ShardLost>()?;
    module.add_class::<types::ShardRecovered>()?;
    module.add_class::<types::ShardMoved>()?;
    module.add_class::<types::StreamDeleted>()?;
    module.add_class::<commit::CommitOp>()?;
    module.add_class::<commit::CommitReceipt>()?;
    module.add_class::<commit::StateValue>()?;
    module.add_class::<conditional::CacheCondition>()?;
    module.add_class::<conditional::CacheConditionResult>()?;
    module.add_class::<conditional::VersionedValue>()?;
    module.add_class::<conditional::ConditionalWrite>()?;
    module.add_class::<conditional::ConditionalTrim>()?;
    module.add_class::<group_admin::GroupPosition>()?;
    module.add_class::<group_admin::GroupInfo>()?;
    module.add_class::<sharded_cache_watch::ShardedCacheChange>()?;
    module.add_class::<sharded_cache_watch::CacheStateComplete>()?;
    module.add_class::<sharded_cache_watch::CacheShardLagged>()?;
    module.add_class::<sharded_cache_watch::CacheShardClosed>()?;
    module.add_class::<sharded_group::ShardedGroupRecord>()?;
    module.add_class::<stream_page::StreamRecord>()?;
    module.add_class::<stream_page::StreamPage>()?;
    module.add_class::<batch::ShardPublish>()?;
    module.add_class::<batch::BatchReport>()?;

    errors::register(module)?;
    Ok(())
}
