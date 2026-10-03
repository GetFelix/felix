//! Making the active segment durable.
//!
//! One flush covers every record written before it, which is what lets group
//! commit in `sync` hand a single flush to many waiting appends.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::LogInner;
use crate::io::sync_data;
use crate::log::Offset;
use crate::{Result, StorageError, metrics_names};

impl LogInner {
    /// Flush the active segment and report the exclusive offset bound now
    /// durable.
    ///
    /// The file handle and the offset it covers are captured under the lock,
    /// then the lock is released before the flush: an `fsync` must never be held
    /// across the lock that appends need.
    ///
    /// The capture runs on the append thread, behind the appends already
    /// queued there, so one flush covers every append in flight. Taken
    /// straight away it would miss the ones still queued, and each of those
    /// would wait for a flush of its own.
    pub(super) async fn flush(self: Arc<Self>) -> Result<Offset> {
        // After a failed fsync the kernel may have dropped the dirty pages and
        // cleared the error, so the next fsync "succeeds" having written
        // nothing. Only refusing to flush again keeps `durable_upto` below the
        // lost bytes.
        self.check_healthy()?;
        let capturer = Arc::clone(&self);
        let (handle, segment_id, synced_bytes, durable_upto, appends) = self
            .appender
            .run(move || {
                let segments = capturer.segments.read();
                let active = segments.active();
                // The writer's own sync (a seal, a truncation) failed. Flushing
                // through a cloned handle would not see that.
                if active.is_poisoned() {
                    capturer.poison_after_writer_failure(&segments);
                    drop(segments);
                    return Ok(Err(capturer.check_healthy().expect_err("just poisoned")));
                }
                Ok(Ok((
                    active.sync_handle(),
                    active.id(),
                    active.size_bytes(),
                    segments.tail_offset(),
                    capturer.appends.load(Ordering::Relaxed),
                )))
            })
            .await
            .map_err(StorageError::Io)??;
        // Taken after the active handle, so a rollover that lands in between is
        // seen here rather than missed: `commit_roll` sets it before releasing
        // the lock this read just took.
        let retired = self.pending_seal.lock().clone();
        let had_pending_seal = retired.is_some();

        let started = std::time::Instant::now();

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
            if let Some(retired) = retired.as_ref() {
                result = crate::io::uring_fsync::fsync(Arc::clone(retired)).await;
            }
            if matches!(result, Some(Ok(()))) {
                result = crate::io::uring_fsync::fsync(Arc::clone(&handle)).await;
            }
            result
        } else {
            None
        };
        #[cfg(not(target_os = "linux"))]
        let via_uring: Option<std::io::Result<()>> = None;

        let outcome = match via_uring {
            Some(result) => result,
            None => {
                self.flusher
                    .run(move || {
                        // The retired segment first, for the same reason.
                        if let Some(retired) = retired {
                            sync_data(&retired)?;
                        }
                        sync_data(&handle)
                    })
                    .await
            }
        };
        #[cfg(test)]
        let outcome = if self
            .fail_next_flush
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            Err(std::io::Error::other("injected fsync failure"))
        } else {
            outcome
        };
        if let Err(err) = outcome {
            let err = StorageError::SyncFailed(err.to_string());
            if had_pending_seal {
                self.record_roll_failure(&err);
            } else {
                self.poison(format!("a flush failed ({err})"));
            }
            return Err(err);
        }

        metrics::counter!(metrics_names::SYNC_TOTAL).increment(1);
        #[cfg(test)]
        self.flushes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        metrics::histogram!(metrics_names::SYNC_DURATION_SECONDS)
            .record(started.elapsed().as_secs_f64());
        // Counted from the appends this flush covers rather than the callers
        // waiting when it starts: those still queued on the append thread are
        // covered too, and have not started waiting yet. Flushes are serialised,
        // so the swap sees the previous flush's count.
        let covered = appends - self.appends_flushed.swap(appends, Ordering::Relaxed);
        if covered > 0 && !self.durability.acknowledges_before_sync() {
            metrics::histogram!(metrics_names::SYNC_BATCH_APPENDS).record(covered as f64);
        }

        {
            let mut segments = self.segments.write();
            // A rollover may have swapped the active segment while the flush was
            // in flight. Its records were covered by the `pending_seal` sync
            // above, and the retired writer is owned by the rollover task, so
            // there is nothing to record against it here.
            if segments.active().id() == segment_id {
                segments.active_mut().mark_synced(synced_bytes);
            }
            metrics::gauge!(metrics_names::UNSYNCED_BYTES)
                .set(segments.active().unsynced_bytes() as f64);
        }
        // Only now that the sync has returned, so the mark never runs ahead of
        // the device. Flushes are serialised, so marks land in order.
        self.note_synced(segment_id, synced_bytes);

        // Rechecked after the flush, not only before it. A rollover can fail
        // while this flush is in flight, and `durable_upto` spans the retired
        // segment as well as the active one -- reporting it would acknowledge
        // records that the failed seal left unflushed.
        self.check_healthy()?;

        Ok(durable_upto)
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
