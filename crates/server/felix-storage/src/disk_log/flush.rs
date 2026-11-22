//! Making the active segment durable.
//!
//! One flush covers every record written before it, which is what lets group
//! commit in `sync` hand a single flush to many waiting appends.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use super::LogInner;
use crate::fs::File;
use crate::io::sync_data;
use crate::log::{Offset, SegmentId};
use crate::{Result, StorageError, metrics_names};

/// What a flush covers, read under the segment lock behind the appends
/// already queued.
#[derive(Clone)]
struct Capture {
    handle: Arc<File>,
    /// A rolled-over segment whose seal has not been synced yet.
    retired: Option<Arc<File>>,
    segment_id: SegmentId,
    synced_bytes: u64,
    /// What the flush makes durable once it has synced both files.
    durable_upto: Offset,
    written: u64,
    rewinds: u64,
}

/// A flush holding the flush lock, ready to go to the flush thread with no
/// task waiting on it. See [`LogInner::begin_detached_flush`].
#[cfg(feature = "raft")]
pub(crate) struct DetachedFlush {
    inner: Arc<LogInner>,
    guard: tokio::sync::OwnedMutexGuard<()>,
    capture: Capture,
}

/// What a detached flush answers. Fails the flush if the job is dropped
/// before it runs, as when the blocking pool shuts down, so nobody waits for
/// it for ever.
#[cfg(feature = "raft")]
struct Answer(Option<Box<dyn FnOnce(Result<()>) + Send>>);

impl LogInner {
    /// Flush the active segment and report the exclusive offset bound now
    /// durable.
    ///
    /// The file handle and the offset it covers are captured under the lock,
    /// then the lock is released before the flush: an `fsync` must never be held
    /// across the lock that appends need.
    ///
    /// The capture runs behind the appends already queued on the append
    /// thread, so one flush covers every append in flight. Taken straight
    /// away it would miss the ones still queued, and each of those would wait
    /// for a flush of its own. With none queued it is taken here.
    pub(super) async fn flush(self: Arc<Self>) -> Result<Offset> {
        // After a failed fsync the kernel may have dropped the dirty pages and
        // cleared the error, so the next fsync "succeeds" having written
        // nothing. Only refusing to flush again keeps `durable_upto` below the
        // lost bytes.
        self.check_healthy()?;
        let capturer = Arc::clone(&self);
        let capture = self
            .appender
            .run_behind(move || Ok(capturer.capture()))
            .await
            .map_err(StorageError::Io)??;

        let started = Instant::now();

        // With `FELIX_STORAGE_IO_URING=1` on Linux the fsync goes into a ring
        // and the kernel completes it. `None` means the ring is not
        // available -- an old kernel, or a container that forbids the syscall --
        // and the flush thread below takes over, because durability must not
        // depend on an optimisation being present (#548).
        #[cfg(target_os = "linux")]
        let via_uring: Option<std::io::Result<()>> = if crate::io::uring_fsync::enabled() {
            // The retired segment first: `durable_upto` covers records in both,
            // and may not be reported until every one of them is on disk.
            let mut result = Some(Ok(()));
            if let Some(retired) = capture.retired.as_ref() {
                result = crate::io::uring_fsync::fsync(Arc::clone(retired)).await;
            }
            if matches!(result, Some(Ok(()))) {
                result = crate::io::uring_fsync::fsync(Arc::clone(&capture.handle)).await;
            }
            result
        } else {
            None
        };
        #[cfg(not(target_os = "linux"))]
        let via_uring: Option<std::io::Result<()>> = None;

        let injected = self.injected_failure(capture.durable_upto);

        // The mark is written only once the sync has returned, so it never runs
        // ahead of the device. Flushes are serialised, so marks land in order.
        let outcome = match via_uring {
            Some(result) => result.and(injected).map(|()| {
                self.note_synced_since(capture.segment_id, capture.synced_bytes, capture.rewinds)
            }),
            None => {
                let syncer = Arc::clone(&self);
                let synced = capture.clone();
                self.flusher
                    .run(move || syncer.sync_captured(&synced, injected))
                    .await
            }
        };
        self.finish_flush(&capture, outcome, started)
    }

    /// Start a flush that covers `target` without a task waiting for it: the
    /// flush thread answers, so the caller's own wake-up is the only one.
    /// `None` when that cannot start at once and the caller waits as usual:
    /// another flush holds the lock, appends are still queued on the append
    /// thread, `target` is already durable, the log is unhealthy, or the
    /// fsync goes through io_uring.
    #[cfg(feature = "raft")]
    pub(super) fn begin_detached_flush(self: &Arc<Self>, target: Offset) -> Option<DetachedFlush> {
        #[cfg(target_os = "linux")]
        if crate::io::uring_fsync::enabled() {
            return None;
        }
        if !self.appender.is_idle() || self.durability.durable_upto() >= target {
            return None;
        }
        let guard = self.durability.try_lock_flushes_owned()?;
        if self.durability.durable_upto() >= target || self.check_healthy().is_err() {
            return None;
        }
        // Inline, as `run_behind` would with nothing queued.
        let capture = self.capture().ok()?;
        (capture.durable_upto >= target).then(|| DetachedFlush {
            inner: Arc::clone(self),
            guard,
            capture,
        })
    }

    /// The active segment's handle and what a flush of it covers. Fails if
    /// the writer's own sync failed.
    fn capture(&self) -> Result<Capture> {
        let (handle, segment_id, synced_bytes, durable_upto, written) = {
            let segments = self.segments.read();
            let active = segments.active();
            // The writer's own sync (a seal, a truncation) failed. Flushing
            // through a cloned handle would not see that.
            if active.is_poisoned() {
                self.poison_after_writer_failure(&segments);
                drop(segments);
                return Err(self.check_healthy().expect_err("just poisoned"));
            }
            (
                active.sync_handle(),
                active.id(),
                active.size_bytes(),
                segments.tail_offset(),
                self.records_written.load(Ordering::Relaxed),
            )
        };
        Ok(Capture {
            handle,
            // Taken after the active handle, so a rollover that lands in
            // between is seen here rather than missed: `commit_roll` sets it
            // before releasing the lock this read just took.
            retired: self.pending_seal.lock().clone(),
            segment_id,
            synced_bytes,
            durable_upto,
            written,
            // Read under the flush lock, which a truncation takes too.
            rewinds: self.mark.rewinds(),
        })
    }

    /// An fsync failure a test asked for, for a flush up to `durable_upto`.
    fn injected_failure(&self, durable_upto: Offset) -> std::io::Result<()> {
        #[cfg(any(test, feature = "fault-injection"))]
        let injected = self.fail_next_flush.swap(false, Ordering::AcqRel)
            || self
                .fail_flush_past
                .try_update(Ordering::AcqRel, Ordering::Acquire, |past| {
                    (durable_upto > past).then_some(u64::MAX)
                })
                .is_ok();
        #[cfg(not(any(test, feature = "fault-injection")))]
        let injected = {
            let _ = durable_upto;
            false
        };
        if injected {
            Err(std::io::Error::other("injected fsync failure"))
        } else {
            Ok(())
        }
    }

    /// Sync what `capture` covers, on the flush thread, then write the mark.
    fn sync_captured(
        &self,
        capture: &Capture,
        injected: std::io::Result<()>,
    ) -> std::io::Result<()> {
        #[cfg(test)]
        if let Some(pause) = self.pause_next_sync.lock().take() {
            pause.wait();
            pause.wait();
        }
        let synced = (|| {
            // The retired segment first, for the same reason.
            if let Some(retired) = &capture.retired {
                sync_data(retired)?;
            }
            sync_data(&capture.handle)?;
            injected
        })();
        match synced {
            // On this thread too: the mark is a write, and a write waits on a
            // busy filesystem journal like a sync does.
            Ok(()) => {
                self.note_synced_since(capture.segment_id, capture.synced_bytes, capture.rewinds)
            }
            // Here rather than in the caller, which may have stopped waiting:
            // the failure must poison anyway.
            Err(ref err) => self.record_flush_failure(
                capture.retired.is_some(),
                &StorageError::SyncFailed(err.to_string()),
            ),
        }
        synced
    }

    /// Account for a flush that has synced, or failed to, and return what it
    /// made durable.
    fn finish_flush(
        &self,
        capture: &Capture,
        outcome: std::io::Result<()>,
        started: Instant,
    ) -> Result<Offset> {
        if let Err(err) = outcome {
            let err = StorageError::SyncFailed(err.to_string());
            self.record_flush_failure(capture.retired.is_some(), &err);
            return Err(err);
        }

        metrics::counter!(metrics_names::SYNC_TOTAL).increment(1);
        #[cfg(test)]
        self.flushes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        metrics::histogram!(metrics_names::SYNC_DURATION_SECONDS)
            .record(started.elapsed().as_secs_f64());
        // Counted from what this flush covers rather than the callers waiting
        // when it starts: appends still queued on the append thread are covered
        // too, and have not started waiting yet. Records rather than appends,
        // because the broker merges publishes that queue up into one append,
        // and those share the flush as much as separate appends do. Flushes are
        // serialised, so the swap sees the previous flush's count.
        let covered = capture.written
            - self
                .records_flushed
                .swap(capture.written, Ordering::Relaxed);
        if covered > 0 && !self.durability.acknowledges_before_sync() {
            metrics::histogram!(metrics_names::SYNC_BATCH_APPENDS).record(covered as f64);
        }

        {
            let mut segments = self.segments.write();
            // A rollover may have swapped the active segment while the flush was
            // in flight. Its records were covered by the `pending_seal` sync
            // above, and the retired writer is owned by the rollover task, so
            // there is nothing to record against it here.
            if segments.active().id() == capture.segment_id {
                segments.active_mut().mark_synced(capture.synced_bytes);
            }
            metrics::gauge!(metrics_names::UNSYNCED_BYTES)
                .set(segments.active().unsynced_bytes() as f64);
        }
        // Rechecked after the flush, not only before it. A rollover can fail
        // while this flush is in flight, and `durable_upto` spans the retired
        // segment as well as the active one -- reporting it would acknowledge
        // records that the failed seal left unflushed.
        self.check_healthy()?;

        Ok(capture.durable_upto)
    }

    /// Whether a flush now would have nothing to do: no unsynced bytes in the
    /// active segment, no retired segment awaiting its seal, and the durable
    /// bound already at the tail.
    ///
    /// The bound check is what keeps the first tick after a rollover flushing:
    /// the seal synced the retired records but only a flush publishes them as
    /// durable. Checked in the same order as `flush` reads its inputs, so a
    /// rollover landing in between is seen as a pending seal.
    pub(super) fn fully_durable(&self) -> bool {
        let (synced, tail) = {
            let segments = self.segments.read();
            (segments.active().is_synced(), segments.tail_offset())
        };
        synced && self.pending_seal.lock().is_none() && self.durability.durable_upto() >= tail
    }

    /// Wait until every offset below `target` is durable, flushing if needed.
    pub(super) async fn ensure_durable(self: &Arc<Self>, target: Offset) -> Result<()> {
        self.durability
            .ensure_durable(target, || Arc::clone(self).flush())
            .await
    }
}

#[cfg(feature = "raft")]
impl DetachedFlush {
    /// Hand the flush to the flush thread, which calls `done` once the
    /// flush has covered the target, or failed. `done` runs before the new
    /// durable bound is published and before the lock goes, so no flush
    /// started after this one answers before it.
    pub(crate) fn run(self, done: Box<dyn FnOnce(Result<()>) + Send>) {
        let Self {
            inner,
            guard,
            capture,
        } = self;
        inner.durability.count_flush();
        let injected = inner.injected_failure(capture.durable_upto);
        let started = Instant::now();
        let mut answer = Answer(Some(done));
        let flusher = Arc::clone(&inner);
        inner.flusher.spawn(move || {
            let outcome = flusher.sync_captured(&capture, injected);
            match flusher.finish_flush(&capture, outcome, started) {
                Ok(durable_upto) => {
                    answer.send(Ok(()));
                    flusher.durability.note_durable(durable_upto);
                }
                Err(err) => {
                    metrics::counter!(metrics_names::SYNC_FAILURES_TOTAL).increment(1);
                    answer.send(Err(err));
                }
            }
            drop(guard);
        });
    }
}

#[cfg(feature = "raft")]
impl Answer {
    fn send(&mut self, result: Result<()>) {
        if let Some(done) = self.0.take() {
            done(result);
        }
    }
}

#[cfg(feature = "raft")]
impl Drop for Answer {
    fn drop(&mut self) {
        self.send(Err(StorageError::Io(std::io::Error::other(
            "the flush was dropped before it ran",
        ))));
    }
}
