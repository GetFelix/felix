//! The lock on a run's data directory, and what has to stop before it is let
//! go.
//!
//! A start that fails after storage opened has tasks running that write
//! under the root (the Raft members, the restore gate, cache expiry). Letting
//! the lock go when the start returns would let a second broker begin a run
//! under them. So every way out of a run goes through [`RunGuard::close`]:
//! stop every task, close storage, record the stop, and only then release.

use std::path::PathBuf;
use std::sync::Arc;

use felix_broker::Broker;
use felix_storage::StorageId;
use felix_storage::disk_log::RootLock;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::shards::routing::EngineShards;

/// Holds the data directory's lock for one run. See the module docs.
///
/// Dropped without [`close`](Self::close), as when a caller drops the start's
/// future, it cancels the run's tokens and closes on a task of its own. If
/// that cannot run to the end (no runtime, or the runtime goes away first)
/// the lock is kept until the process exits rather than let go with storage
/// still open.
pub(super) struct RunGuard {
    /// `None` once closed or handed to the task that closes it.
    teardown: Option<Teardown>,
}

/// What closing a run stops, in the order it stops them.
struct Teardown {
    lock: Option<RootLock>,
    stops: Vec<CancellationToken>,
    /// Tasks that can write under the root, joined before storage closes.
    writers: TaskTracker,
    engine_shards: Option<Arc<dyn EngineShards>>,
    broker: Option<Arc<Broker>>,
    /// Where to record the stop, once storage closed.
    run: Option<(PathBuf, StorageId)>,
    /// The drain already closed storage and recorded the stop.
    drained: bool,
}

impl RunGuard {
    /// Hold `lock`, or nothing for a broker without durable storage.
    pub(super) fn new(lock: Option<RootLock>) -> Self {
        Self {
            teardown: Some(Teardown {
                lock,
                stops: Vec::new(),
                writers: TaskTracker::new(),
                engine_shards: None,
                broker: None,
                run: None,
                drained: false,
            }),
        }
    }

    fn parts(&mut self) -> &mut Teardown {
        self.teardown
            .as_mut()
            .expect("a run guard is open until closed")
    }

    /// The lock storage opens under.
    pub(super) fn lock(&self) -> Option<&RootLock> {
        self.teardown.as_ref().and_then(|parts| parts.lock.as_ref())
    }

    /// Cancelled when the run closes.
    pub(super) fn stop_with(&mut self, token: &CancellationToken) {
        self.parts().stops.push(token.clone());
    }

    /// Where tasks that can write under the root are spawned, so the run
    /// waits for them before storage closes.
    pub(super) fn writers(&self) -> &TaskTracker {
        &self
            .teardown
            .as_ref()
            .expect("a run guard is open until closed")
            .writers
    }

    /// Wait for `task` before storage closes.
    pub(super) fn track<T: Send + 'static>(&self, task: JoinHandle<T>) {
        self.writers().spawn(async move {
            let _ = task.await;
        });
    }

    /// Storage is open: closing the run shuts it down and records the stop
    /// under `run`.
    pub(super) fn opened(&mut self, broker: &Arc<Broker>, run: Option<(PathBuf, StorageId)>) {
        let parts = self.parts();
        parts.broker = Some(Arc::clone(broker));
        parts.run = run;
    }

    /// The members of shards the broker's own storage does not open, stopped
    /// before storage closes.
    pub(super) fn engine_shards(&mut self, shards: Option<Arc<dyn EngineShards>>) {
        self.parts().engine_shards = shards;
    }

    /// The drain stopped everything and closed storage.
    pub(super) fn drained(&mut self) {
        self.parts().drained = true;
    }

    /// Stop the run and let the lock go, last.
    pub(super) async fn close(mut self) {
        if let Some(teardown) = self.teardown.take() {
            teardown.run().await;
        }
    }
}

impl Drop for RunGuard {
    fn drop(&mut self) {
        let Some(teardown) = self.teardown.take() else {
            return;
        };
        for stop in &teardown.stops {
            stop.cancel();
        }
        // Without a runtime the teardown drops here, keeping the lock.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(teardown.run());
        }
    }
}

impl Teardown {
    async fn run(mut self) {
        for stop in &self.stops {
            stop.cancel();
        }
        self.writers.close();
        self.writers.wait().await;
        if !self.drained {
            let engine_closed = match &self.engine_shards {
                Some(shards) => shards.shutdown().await,
                None => true,
            };
            let flushed = match &self.broker {
                Some(broker) => match broker.shutdown_storage().await {
                    Ok(()) => true,
                    Err(err) => {
                        tracing::error!(error = %err, "failed to flush durable storage after a failed start");
                        false
                    }
                },
                None => true,
            };
            // Nothing was acknowledged, so a start that closed everything
            // stopped as cleanly as a drain does.
            if let Some((root, id)) = &self.run {
                if engine_closed && flushed && !felix_storage::disk_log::any_log_poisoned() {
                    if let Err(err) = super::shutdown::record_clean_stop(root, *id) {
                        tracing::error!(error = %err, "could not record a clean stop; the next start takes a new storage id");
                    }
                } else if let Err(err) = super::storage::forget_boot(root) {
                    tracing::error!(error = %err, "could not record that the stop may have lost records");
                }
            }
        }
        drop(self.lock.take());
    }
}

impl Drop for Teardown {
    /// A teardown that did not finish keeps the lock until the process
    /// exits: storage may still be open.
    fn drop(&mut self) {
        if let Some(lock) = self.lock.take() {
            std::mem::forget(lock);
        }
    }
}

#[cfg(test)]
mod tests;
