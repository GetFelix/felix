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
mod blocking;
mod commit;
mod errors;
mod runtime;
mod tls;
mod types;

use pyo3::prelude::*;

#[pymodule]
fn _felix(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;

    module.add_class::<blocking::Client>()?;
    module.add_class::<blocking::SubscriptionHandle>()?;
    module.add_class::<blocking::CacheWatchHandle>()?;
    module.add_class::<blocking::ShardedSubscriptionHandle>()?;

    module.add_class::<asyncio::AsyncClient>()?;
    module.add_class::<asyncio::AsyncSubscription>()?;
    module.add_class::<asyncio::AsyncCacheWatch>()?;
    module.add_class::<asyncio::AsyncShardedSubscription>()?;

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
    module.add_class::<commit::CommitOp>()?;
    module.add_class::<commit::CommitReceipt>()?;
    module.add_class::<commit::StateValue>()?;

    errors::register(module)?;
    Ok(())
}
