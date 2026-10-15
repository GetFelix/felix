//! Durability policy: when written bytes are pushed to stable storage, and who
//! waits for it.
//!
//! The three `FsyncMode` variants differ only in *when* a flush is triggered:
//!
//! | mode                 | flush trigger                | acknowledged when          |
//! |----------------------|------------------------------|----------------------------|
//! | `None`               | segment seal, shutdown       | bytes reach the page cache |
//! | `Periodic{interval}` | background timer             | bytes reach the page cache |
//! | `OnCommit`           | the append itself            | bytes reach the device     |
//!
//! ## Group commit
//!
//! `OnCommit` is the expensive one, and the reason it is affordable at all is
//! group commit. An `fsync` flushes *the whole file*, not one caller's bytes — so
//! when N appends are in flight, one flush can satisfy all N. Every appender
//! contends for the same flush lock; the winner flushes, and the losers wake to
//! find their target already durable and return without flushing again. Under
//! concurrency the device sees one flush per round trip rather than one per
//! append, which is the difference between a few hundred and a few tens of
//! thousands of durable appends per second on the same hardware. The losers
//! also watch the durable bound while they wait, so one flush wakes all of
//! them together instead of handing the lock down the queue one by one.
//!
//! This is the same mechanism behind PostgreSQL's `commit_delay` group commit and
//! the WAL group-commit path in MySQL and RocksDB. `felix_storage_sync_batch_appends`
//! reports the fan-in actually achieved.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{Mutex, MutexGuard, Notify, watch};

use crate::Result;
use crate::log::{FsyncMode, Offset};
use crate::{StorageError, metrics_names};

/// A flush that reached a bounded retry limit without advancing. Only reachable
/// if a flush implementation reports less progress than it made, which would
/// otherwise spin.
const MAX_FLUSH_ATTEMPTS: usize = 8;

/// Tracks how much of the log is durable and coordinates who flushes.
#[derive(Debug)]
pub(super) struct Durability {
    mode: FsyncMode,
    /// Exclusive upper bound: every offset below this is on stable storage.
    /// A watch channel so waiters are woken by the flusher rather than polling.
    durable_tx: watch::Sender<Offset>,
    /// Held by whichever task is currently flushing. Everyone else queues here
    /// and finds the work already done when they get in.
    flush_lock: Mutex<()>,
    /// Flushes actually issued. One relaxed increment against a syscall that
    /// costs microseconds at best, so it is always on rather than behind a
    /// feature -- and it is the only machine-independent way to see group
    /// commit working. Wall-clock speedup cannot tell "the flushes coalesced"
    /// from "this machine could not put enough appends in flight to coalesce",
    /// which is how a CI runner with two cores reads as a regression.
    flushes: AtomicU64,
}

impl Durability {
    /// `durable_upto` is the exclusive offset bound recovered from disk — every
    /// record already on disk at open time is by definition durable.
    pub(super) fn new(mode: FsyncMode, durable_upto: Offset) -> Self {
        Self {
            mode,
            durable_tx: watch::channel(durable_upto).0,
            flush_lock: Mutex::new(()),
            flushes: AtomicU64::new(0),
        }
    }

    /// Exclusive bound on durable offsets: everything below it survives a crash.
    pub(super) fn durable_upto(&self) -> Offset {
        *self.durable_tx.borrow()
    }

    /// How many flushes have actually been issued.
    ///
    /// The direct measure of group commit: N appends that coalesce produce far
    /// fewer than N flushes, whatever the machine's timing looks like.
    pub(super) fn flushes(&self) -> u64 {
        self.flushes.load(Ordering::Relaxed)
    }

    /// Whether an append must wait for a flush before it may be acknowledged.
    pub(super) fn acknowledges_before_sync(&self) -> bool {
        !matches!(self.mode, FsyncMode::OnCommit)
    }

    /// Block until every offset below `target` is durable.
    ///
    /// `flush` performs one device flush and returns the exclusive offset bound
    /// it made durable. It is called by at most one task at a time; concurrent
    /// callers wait on that single flush.
    pub(super) async fn ensure_durable<F, Fut>(&self, target: Offset, flush: F) -> Result<()>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<Offset>>,
    {
        let mut receiver = self.durable_tx.subscribe();
        if *receiver.borrow_and_update() >= target {
            return Ok(());
        }

        self.flush_until(target, flush, &mut receiver).await
    }

    /// Run an unconditional flush under the same lock used by group commit.
    pub(super) async fn force_flush<F, Fut>(&self, flush: F) -> Result<Offset>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Offset>>,
    {
        let _guard = self.flush_lock.lock().await;
        let durable_upto = flush().await?;
        self.note_durable(durable_upto);
        Ok(durable_upto)
    }

    /// Publish flush progress. Monotonic: a late report cannot walk it back.
    pub(super) fn note_durable(&self, durable_upto: Offset) {
        self.durable_tx.send_if_modified(|current| {
            if durable_upto > *current {
                *current = durable_upto;
                true
            } else {
                false
            }
        });
    }

    /// Prevent flush progress from racing an operation that rewrites the log.
    pub(super) async fn lock_flushes(&self) -> MutexGuard<'_, ()> {
        self.flush_lock.lock().await
    }

    /// Lower the durable bound after truncation.
    ///
    /// The caller must hold [`Self::lock_flushes`] and the segment write lock,
    /// so no flush can publish stale progress and no append can observe the
    /// truncated tail before this reset.
    pub(super) fn reset_after_truncate(&self, durable_upto: Offset) {
        self.durable_tx.send_replace(durable_upto);
    }

    async fn flush_until<F, Fut>(
        &self,
        target: Offset,
        flush: F,
        receiver: &mut watch::Receiver<Offset>,
    ) -> Result<()>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<Offset>>,
    {
        for _ in 0..MAX_FLUSH_ATTEMPTS {
            // Contend for the flusher role, and stop contending once someone
            // else's flush covers this target. Waiting only on the lock would
            // wake the covered waiters one at a time, each through the lock in
            // turn; the watch wakes them all at once.
            let guard = tokio::select! {
                biased;
                covered = async {
                    receiver.wait_for(|durable| *durable >= target).await.is_ok()
                } => {
                    if covered {
                        return Ok(());
                    }
                    self.flush_lock.lock().await
                }
                guard = self.flush_lock.lock() => guard,
            };
            if *receiver.borrow_and_update() >= target {
                // Someone else's flush already covered us — the group-commit
                // fast path, and by far the common case under load.
                drop(guard);
                return Ok(());
            }

            self.flushes.fetch_add(1, Ordering::Relaxed);
            let outcome = flush().await;
            // Published before the lock goes, so the next holder does not
            // flush again for what this one already made durable.
            if let Ok(durable_upto) = &outcome {
                self.note_durable(*durable_upto);
            }
            drop(guard);

            match outcome {
                Ok(durable_upto) => {
                    if durable_upto >= target {
                        return Ok(());
                    }
                }
                Err(err) => {
                    metrics::counter!(metrics_names::SYNC_FAILURES_TOTAL).increment(1);
                    return Err(err);
                }
            }
        }

        Err(StorageError::SyncFailed(format!(
            "flush did not reach offset {target} after {MAX_FLUSH_ATTEMPTS} attempts"
        )))
    }
}

/// Background task that flushes on a timer for `FsyncMode::Periodic`.
///
/// Owns its own shutdown signal so a dropped log does not leave a task running
/// against a closed file, and so graceful shutdown can flush one last time
/// before the process exits.
#[derive(Debug)]
pub(super) struct PeriodicSyncer {
    shutdown: Arc<Notify>,
    handle: tokio::task::JoinHandle<()>,
}

impl PeriodicSyncer {
    /// Run `flush` every `interval` until shutdown.
    ///
    /// Fails rather than panics when there is no Tokio runtime: `Periodic` mode
    /// needs a timer, and a log opened outside a runtime would otherwise die at
    /// an unrelated call site with a message about reactors.
    ///
    /// Flush failures are logged rather than killing the task. A failed fsync
    /// poisons the log (see `LogInner::poison`), so the ticks that follow
    /// skip flushing and appends report the error instead.
    pub(super) fn spawn<F, Fut>(interval: Duration, flush: F) -> Result<Self>
    where
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = Result<Offset>> + Send,
    {
        if tokio::runtime::Handle::try_current().is_err() {
            return Err(StorageError::InvalidConfig(
                "FsyncMode::Periodic needs a Tokio runtime; open the log from async code or choose FsyncMode::None or OnCommit",
            ));
        }
        let shutdown = Arc::new(Notify::new());
        let signal = Arc::clone(&shutdown);
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // Skipped ticks must not queue up into a burst of flushes after a
            // stall; one late flush is as good as five.
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // The first tick fires immediately and has nothing to flush.
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Err(err) = flush().await {
                            metrics::counter!(metrics_names::SYNC_FAILURES_TOTAL).increment(1);
                            tracing::error!(error = %err, "periodic log flush failed");
                        }
                    }
                    _ = signal.notified() => {
                        // Final flush so a graceful stop loses nothing that a
                        // clean shutdown could have kept.
                        if let Err(err) = flush().await {
                            metrics::counter!(metrics_names::SYNC_FAILURES_TOTAL).increment(1);
                            tracing::error!(error = %err, "final log flush failed");
                        }
                        return;
                    }
                }
            }
        });
        Ok(Self { shutdown, handle })
    }

    /// Stop the task, waiting for its final flush to complete.
    pub(super) async fn shutdown(self) {
        self.shutdown.notify_waiters();
        // The task may not be parked on `notified()` yet; `notify_waiters` does
        // not latch, so nudge it until it observes the signal.
        loop {
            if self.handle.is_finished() {
                break;
            }
            self.shutdown.notify_waiters();
            tokio::task::yield_now().await;
        }
        let _ = self.handle.await;
    }
}

#[cfg(test)]
mod tests;
