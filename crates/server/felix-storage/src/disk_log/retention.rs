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
    /// and the error is returned once it is done. A segment past the bounds
    /// without a recorded copy is kept, however long offload keeps failing,
    /// and reported as held.
    pub(super) async fn sweep_retention(self: Arc<Self>) -> Result<RetentionOutcome> {
        let offloaded = self.offload_pass().await;
        // A failure is logged, on a backoff, by the pass itself.
        if let Ok(outcome) = &offloaded
            && outcome.segments > 0
        {
            tracing::info!(
                shard = %self.label,
                segments = outcome.segments,
                bytes = outcome.bytes,
                "offloaded segments"
            );
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
            let bounds = *inner.retention_bounds.lock();
            let mut outcome = if bounds.is_set() {
                inner.retain(bounds)?
            } else {
                if let Some(offloader) = &inner.offloader {
                    offloader.note_held(&inner.label, 0, 0);
                }
                RetentionOutcome::default()
            };
            let below_floor = inner.delete_below_floor()?;
            outcome.segments_deleted += below_floor.len();
            outcome.bytes_reclaimed += below_floor
                .iter()
                .map(|descriptor| descriptor.size_bytes)
                .sum::<u64>();
            outcome.base_offset = inner.segments.read().base_offset();
            if outcome.segments_deleted > 0 {
                metrics::gauge!(metrics_names::RETENTION_BASE_OFFSET)
                    .set(outcome.base_offset as f64);
            }
            Ok(outcome)
        })
        .await
        .map_err(|err| StorageError::Io(std::io::Error::other(err)))??;
        offloaded.map(|_| retained)
    }

    /// Delete what the retention bounds say to, on a blocking thread.
    fn retain(&self, bounds: crate::log::Retention) -> Result<RetentionOutcome> {
        let mut outcome = RetentionOutcome::default();
        // Held until the unlinks are done: see `LogInner::manifest`.
        let manifest = self.manifest.lock();
        let plan = self.segments.read().retention_plan();
        let mut chosen = plan.choose(bounds, self.retention_floor(), now_micros(), &self.label)?;
        if let Some(offloader) = &self.offloader {
            let (segments, bytes) = self.keep_offloaded_prefix(&manifest, &mut chosen);
            offloader.note_held(&self.label, segments, bytes);
        }
        let removed = {
            let mut segments = self.segments.write();
            let removed = segments.remove_head(&chosen)?;
            if !removed.is_empty() {
                self.note_head_removed(segments.base_offset());
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
            remove_segment_files(&self.dir, descriptor.id)?;
            crate::io::sync_dir(&self.dir).map_err(StorageError::Io)?;
            outcome.segments_deleted += 1;
            outcome.bytes_reclaimed += descriptor.size_bytes;
            #[cfg(test)]
            if let Some(offloader) = &self.offloader {
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
    }

    /// Delete the sealed head segments wholly below the head floor, on a
    /// blocking thread.
    ///
    /// Never past the trim record that set the floor, so the floor can
    /// always be read back from the log. On a log held at its commit offset,
    /// never past the commit offset on disk either, so a restart never reads
    /// back a commit offset below the base. The floor itself only counts
    /// trims below the apply bound, so a trim a crash or a truncation could
    /// still take away deletes nothing.
    pub(super) fn delete_below_floor(&self) -> Result<Vec<crate::log::SegmentDescriptor>> {
        if self.head_floor() == 0 {
            return Ok(Vec::new());
        }
        // Before the floor is read: a truncation, which can lower it, takes
        // this lock first.
        let manifest = self.manifest.lock();
        let bound = {
            let mut head = self.head.lock();
            self.apply_head(&mut head);
            head.delete_bound()
        };
        let Some(mut before) = bound else {
            return Ok(Vec::new());
        };
        if self.hold_at_commit.load(Ordering::Acquire) {
            before = before.min(self.commit_persisted.load(Ordering::Acquire));
        }
        self.delete_head_before(&manifest, before)
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
    /// manifest records. The rest wait for a copy; returns how many segments
    /// and bytes that keeps.
    fn keep_offloaded_prefix(
        &self,
        manifest: &super::offload::Manifest,
        chosen: &mut Vec<crate::log::SegmentId>,
    ) -> (usize, u64) {
        let descriptors = self.segments.read().descriptors();
        let descriptor =
            |id: &crate::log::SegmentId| descriptors.iter().find(|descriptor| descriptor.id == *id);
        let recorded = chosen
            .iter()
            .take_while(|id| descriptor(id).is_some_and(|descriptor| manifest.records(descriptor)))
            .count();
        let held = &chosen[recorded..];
        let bytes = held
            .iter()
            .filter_map(descriptor)
            .map(|descriptor| descriptor.size_bytes)
            .sum();
        let segments = held.len();
        chosen.truncate(recorded);
        (segments, bytes)
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
            inner.delete_head_before(&manifest, before)
        })
        .await
        .map_err(|err| StorageError::Io(std::io::Error::other(err)))?
    }

    /// [`Self::trim_head_before`], on the calling thread, holding the
    /// manifest lock.
    fn delete_head_before(
        &self,
        manifest: &super::offload::Manifest,
        before: crate::log::Offset,
    ) -> Result<Vec<crate::log::SegmentDescriptor>> {
        let before = self
            .retention_floor()
            .map_or(before, |floor| floor.min(before));
        let removed = {
            let mut segments = self.segments.write();
            let active = segments.active().id();
            let offload = self.offloader.is_some();
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
                self.note_head_removed(segments.base_offset());
            }
            removed
        };
        for descriptor in &removed {
            #[cfg(all(test, target_os = "linux"))]
            super::segments::stop_point(&self.dir)?;
            remove_segment_files(&self.dir, descriptor.id)?;
            crate::io::sync_dir(&self.dir).map_err(StorageError::Io)?;
        }
        Ok(removed)
    }

    /// Forget the producer batches and trim records below `base`, once the
    /// segments holding them are out of the list. Called holding the
    /// `segments` write lock.
    fn note_head_removed(&self, base: crate::log::Offset) {
        self.producers.lock().prune(base);
        let mut head = self.head.lock();
        head.prune_below(base);
        self.publish_head(&head);
    }
}
