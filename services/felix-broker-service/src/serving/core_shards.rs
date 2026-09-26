//! Core-sharded executor for stream-owned work (thread-per-core).
//!
//! Runs broker-core stream work (publish workers, per-subscription lane feeders)
//! on a fixed set of dedicated single-threaded runtimes, one per configured
//! shard, pinned to CPU cores on Linux. A stream's handle id deterministically
//! selects its owning shard, so all mutation of that stream's state — publish
//! append, fanout enqueue, and the subscriber-side dequeue — happens on one
//! core. This removes cross-core cache traffic and cross-thread wakeups from
//! the per-message path, at the cost of serializing each stream's pipeline on
//! its owning core (which is the point: shared-nothing ownership).
//!
//! QUIC I/O (endpoint driver, connection writers) stays on the main runtime:
//! quinn performs packetization and TLS in its driver task regardless of where
//! callers live, and its streams are executor-agnostic.
//!
//! # Configuration
//! Disabled by default (`core_shards = 0`); enable with `FELIX_CORE_SHARDS=N`
//! or `core_shards: N` in the broker config file. The process-wide instance is
//! initialized from the first config that requests it (matching the existing
//! global writer-lane-manager pattern).
use std::sync::{Arc, OnceLock};

use tokio::sync::oneshot;

/// A fixed set of single-threaded runtimes, one per shard.
#[derive(Debug)]
pub(crate) struct CoreShards {
    handles: Vec<tokio::runtime::Handle>,
    // Dropping the senders releases each shard's `block_on`, ending its thread.
    _shutdown: Vec<oneshot::Sender<()>>,
}

impl CoreShards {
    /// Build `count` shard threads. Each thread hosts a current-thread tokio
    /// runtime and is pinned to a core on Linux (best-effort no-op elsewhere).
    pub(crate) fn new(count: usize) -> Arc<Self> {
        let count = count.max(1);
        let mut handles = Vec::with_capacity(count);
        let mut shutdown = Vec::with_capacity(count);
        for shard_id in 0..count {
            let (handle_tx, handle_rx) = std::sync::mpsc::sync_channel(1);
            let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
            std::thread::Builder::new()
                .name(format!("felix-shard-{shard_id}"))
                .spawn(move || {
                    pin_to_core(shard_id);
                    let runtime = match tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(runtime) => runtime,
                        Err(err) => {
                            tracing::error!(shard_id, error = %err, "core shard runtime build failed");
                            return;
                        }
                    };
                    if handle_tx.send(runtime.handle().clone()).is_err() {
                        return;
                    }
                    // Keep the runtime alive (and its spawned tasks running)
                    // until the shard set is dropped.
                    let _ = runtime.block_on(shutdown_rx);
                })
                .expect("spawn core shard thread");
            let handle = handle_rx
                .recv()
                .expect("core shard runtime handle unavailable");
            handles.push(handle);
            shutdown.push(shutdown_tx);
        }
        tracing::info!(shards = count, "core shard executors started");
        Arc::new(Self {
            handles,
            _shutdown: shutdown,
        })
    }

    /// Number of shards.
    pub(crate) fn len(&self) -> usize {
        self.handles.len()
    }

    /// Deterministic owner shard for a stream handle id. Must stay consistent
    /// with publish-worker sharding so a stream's publish worker and its
    /// subscriptions' lane feeders land on the same core.
    pub(crate) fn shard_for(&self, handle_id: u64) -> usize {
        (handle_id as usize) % self.handles.len()
    }

    /// Runtime handle of the shard owning `handle_id`.
    pub(crate) fn handle_for(&self, handle_id: u64) -> &tokio::runtime::Handle {
        &self.handles[self.shard_for(handle_id)]
    }
}

/// Process-wide shard set, initialized from the first config that enables it.
/// Returns `None` while `core_shards == 0` (feature disabled).
pub(crate) fn global_shards(config: &crate::config::BrokerConfig) -> Option<Arc<CoreShards>> {
    static SHARDS: OnceLock<Option<Arc<CoreShards>>> = OnceLock::new();
    SHARDS
        .get_or_init(|| {
            if config.core_shards == 0 {
                None
            } else {
                Some(CoreShards::new(config.core_shards))
            }
        })
        .clone()
}

#[cfg(target_os = "linux")]
fn pin_to_core(shard_id: usize) {
    let cores = std::thread::available_parallelism()
        .map(std::num::NonZero::get)
        .unwrap_or(1);
    let core = shard_id % cores;
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(core, &mut set);
        if libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) != 0 {
            tracing::warn!(shard_id, core, "core pinning failed; continuing unpinned");
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn pin_to_core(_shard_id: usize) {
    // No portable hard-pinning API on macOS/Windows; shards still get
    // dedicated threads, which preserves the single-writer ownership model.
}

#[cfg(test)]
mod tests;
