//! Retention: deleting the oldest sealed segments once a bound is exceeded.
//!
//! This runs on its own timer rather than from an append. Retention is bulk file
//! deletion — it unlinks whole segments and their indexes — and putting that on
//! the publish path would trade a bounded disk for an unbounded p999. The
//! rollover work is the cautionary tale: storage work sharing a lock with
//! appends is what makes appends wait on flushes.
//!
//! See `docs/durable-storage.md` for what a trimmed log means to a reader.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::Notify;

use super::segments::{RetentionOutcome, remove_segment_files};
use super::{LogInner, now_micros};
use crate::{Result, StorageError, metrics_names};

/// Background task that enforces retention on a timer.
///
/// Holds only a weak reference to the log, so a dropped log stops the work
/// instead of being kept alive by its own housekeeping.
#[derive(Debug)]
pub(super) struct RetentionTask {
    shutdown: Arc<Notify>,
    handle: tokio::task::JoinHandle<()>,
}

impl RetentionTask {
    /// Run `sweep` every `interval` until shutdown.
    ///
    /// A failed sweep is logged and retried on the next tick rather than
    /// killing the task: a transient I/O error must not silently turn retention
    /// off for the rest of the process's life, because the symptom of that is a
    /// full disk hours later.
    pub(super) fn spawn<F, Fut>(interval: Duration, sweep: F) -> Result<Self>
    where
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = Result<RetentionOutcome>> + Send,
    {
        if tokio::runtime::Handle::try_current().is_err() {
            return Err(StorageError::InvalidConfig(
                "retention needs a Tokio runtime; open the log from async code or leave retention_bytes and retention_age unset",
            ));
        }
        let shutdown = Arc::new(Notify::new());
        let signal = Arc::clone(&shutdown);
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // A stall must not queue up a burst of sweeps; one late sweep
            // reclaims exactly what five would have.
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // Consume the immediate first tick. A sweep racing `open` would
            // mean a freshly opened log could delete records before its caller
            // has read anything, which makes "what can I still read" depend on
            // scheduling. Callers wanting an immediate pass have
            // `enforce_retention_now`.
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        match sweep().await {
                            Ok(outcome) if outcome.segments_deleted > 0 => {
                                tracing::info!(
                                    segments = outcome.segments_deleted,
                                    bytes = outcome.bytes_reclaimed,
                                    base_offset = outcome.base_offset,
                                    "retention deleted segments"
                                );
                            }
                            Ok(_) => {}
                            Err(err) => {
                                metrics::counter!(metrics_names::RETENTION_FAILURES_TOTAL)
                                    .increment(1);
                                tracing::error!(error = %err, "retention sweep failed");
                            }
                        }
                    }
                    // No final sweep on shutdown: retention is not a durability
                    // obligation, and deleting data on the way out is the last
                    // thing a stopping process should do.
                    _ = signal.notified() => return,
                }
            }
        });
        Ok(Self { shutdown, handle })
    }

    /// Stop the task and wait for it to observe the signal.
    pub(super) async fn shutdown(self) {
        self.shutdown.notify_waiters();
        // `notify_waiters` does not latch, so a task not yet parked on
        // `notified()` would miss it; nudge until it lands.
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

impl LogInner {
    /// One retention pass, on a blocking thread.
    ///
    /// Deleting files is a syscall per file and can block on a busy device, so
    /// it never runs on a reactor worker — the same rule the rollover path
    /// follows for its flushes.
    ///
    /// The segment lock is held only to take the chosen segments out of the
    /// list. Choosing them may read a cold record per segment for its age, and
    /// deleting them is a syscall per file; appends wait on neither.
    ///
    /// With offload on, the copies are made first, and a failed copy does not
    /// stop the deletions after it: retention deletes only recorded segments,
    /// and the error is returned once it is done.
    pub(super) async fn sweep_retention(self: Arc<Self>) -> Result<RetentionOutcome> {
        let offloaded = self.offload_pass().await;
        match &offloaded {
            Ok(outcome) if outcome.segments > 0 => tracing::info!(
                shard = %self.label,
                segments = outcome.segments,
                bytes = outcome.bytes,
                "offloaded segments"
            ),
            Ok(_) => {}
            Err(err) => tracing::warn!(shard = %self.label, error = %err, "offload pass failed"),
        }
        // A test stop stands for a crash, after which nothing else runs.
        #[cfg(test)]
        if let (Some(offloader), Err(_)) = (&self.offloader, &offloaded)
            && offloader.faults.stopped()
        {
            return offloaded.map(|_| RetentionOutcome::default());
        }
        let inner = Arc::clone(&self);
        let retained = tokio::task::spawn_blocking(move || -> Result<RetentionOutcome> {
            let mut outcome = RetentionOutcome::default();
            let bounds = *inner.retention_bounds.lock();
            if !bounds.is_set() {
                outcome.base_offset = inner.segments.read().base_offset();
                return Ok(outcome);
            }
            // Held until the unlinks are done: see `LogInner::manifest`.
            let manifest = inner.manifest.lock();
            let plan = inner.segments.read().retention_plan();
            let mut chosen =
                plan.choose(bounds, inner.retention_floor(), now_micros(), &inner.label)?;
            if inner.offloader.is_some() {
                inner.keep_offloaded_prefix(&manifest, &mut chosen);
            }
            let removed = {
                let mut segments = inner.segments.write();
                let removed = segments.remove_head(&chosen)?;
                if !removed.is_empty() {
                    inner.producers.lock().prune(segments.base_offset());
                }
                outcome.base_offset = segments.base_offset();
                removed
            };
            // Out of the list first, so nothing new can reach these files; a
            // crash before the unlinks leaves them as a longer log, which the
            // next pass trims again. Oldest first, each unlink synced before
            // the next: otherwise a power loss can keep a newer unlink and undo
            // an older one, and recovery refuses the gap that leaves.
            for descriptor in &removed {
                remove_segment_files(&inner.dir, descriptor.id)?;
                crate::io::sync_dir(&inner.dir).map_err(StorageError::Io)?;
                outcome.segments_deleted += 1;
                outcome.bytes_reclaimed += descriptor.size_bytes;
                #[cfg(test)]
                if let Some(offloader) = &inner.offloader {
                    offloader
                        .faults
                        .stop(super::offload::test_hooks::Stop::Unlinked)?;
                }
            }
            drop(manifest);
            if !removed.is_empty() {
                metrics::counter!(metrics_names::RETENTION_SEGMENTS_DELETED_TOTAL)
                    .increment(outcome.segments_deleted as u64);
                metrics::counter!(metrics_names::RETENTION_BYTES_RECLAIMED_TOTAL)
                    .increment(outcome.bytes_reclaimed);
            }
            metrics::gauge!(metrics_names::RETENTION_BASE_OFFSET).set(outcome.base_offset as f64);
            Ok(outcome)
        })
        .await
        .map_err(|err| StorageError::Io(std::io::Error::other(err)))??;
        offloaded.map(|_| retained)
    }

    /// The offset no retention pass or head trim may reach, if one is held:
    /// the commit offset, on a log under `Quorum` replication. Records above it
    /// may still be waiting for a majority, and a record deleted there is one a
    /// rebuilt follower could later be counted as holding.
    ///
    /// The commit offset only rises, so a floor read before the plan is a
    /// safe bound for the whole pass.
    pub(super) fn retention_floor(&self) -> Option<crate::log::Offset> {
        self.hold_at_commit
            .load(Ordering::Acquire)
            .then(|| self.commit_offset.load(Ordering::Acquire))
    }

    /// Cut `chosen` back to the segments, from the head, whose copy the
    /// manifest records. The rest wait for a copy.
    fn keep_offloaded_prefix(
        &self,
        manifest: &super::offload::Manifest,
        chosen: &mut Vec<crate::log::SegmentId>,
    ) {
        let descriptors = self.segments.read().descriptors();
        let recorded = chosen
            .iter()
            .take_while(|id| {
                descriptors
                    .iter()
                    .find(|descriptor| descriptor.id == **id)
                    .is_some_and(|descriptor| manifest.records(descriptor))
            })
            .count();
        chosen.truncate(recorded);
    }

    /// Delete the sealed head segments that hold only offsets below `before`,
    /// and below the retention floor when one is held.
    ///
    /// For compaction, which has copied everything live out of them first.
    /// Out of the list under the lock, then unlinked without it, so a crash in
    /// between leaves a longer log. Each unlink is made durable before the
    /// next: a power loss that undid an older one but kept a newer one would
    /// leave a gap in the chain, which recovery rightly refuses.
    pub(super) async fn trim_head_before(
        self: Arc<Self>,
        before: crate::log::Offset,
    ) -> Result<Vec<crate::log::SegmentDescriptor>> {
        let inner = Arc::clone(&self);
        tokio::task::spawn_blocking(move || {
            let manifest = inner.manifest.lock();
            let before = inner
                .retention_floor()
                .map_or(before, |floor| floor.min(before));
            let removed = {
                let mut segments = inner.segments.write();
                let active = segments.active().id();
                let offload = inner.offloader.is_some();
                let chosen: Vec<_> = segments
                    .descriptors()
                    .into_iter()
                    .take_while(|descriptor| {
                        descriptor.id != active
                            && descriptor.last_offset < before
                            && (!offload || manifest.records(descriptor))
                    })
                    .map(|descriptor| descriptor.id)
                    .collect();
                let removed = segments.remove_head(&chosen)?;
                if !removed.is_empty() {
                    inner.producers.lock().prune(segments.base_offset());
                }
                removed
            };
            for descriptor in &removed {
                remove_segment_files(&inner.dir, descriptor.id)?;
                crate::io::sync_dir(&inner.dir).map_err(StorageError::Io)?;
            }
            Ok(removed)
        })
        .await
        .map_err(|err| StorageError::Io(std::io::Error::other(err)))?
    }
}
