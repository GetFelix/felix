//! Making the active segment durable.
//!
//! One flush covers every record written before it, which is what lets group
//! commit in `sync` hand a single flush to many waiting appends.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::LogInner;
use crate::fs::File;
use crate::io::{OnFailure, sync_data};
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
    /// The capture runs behind the appends already queued on the append
    /// thread, so one flush covers every append in flight. Taken straight
    /// away it would miss the ones still queued, and each of those would wait
    /// for a flush of its own. With none queued a Raft log takes it here.
    pub(super) async fn flush(self: Arc<Self>) -> Result<Offset> {
        // After a failed fsync the kernel may have dropped the dirty pages and
        // cleared the error, so the next fsync "succeeds" having written
        // nothing. Only refusing to flush again keeps `durable_upto` below the
        // lost bytes.
        self.check_healthy()?;
        let capturer = Arc::clone(&self);
        // Hybrid logs keep the hand-off: taking the capture inline moves the
        // simulator's schedule, and hybrid is frozen until it is removed.
        let inline = self.config.raft_log;
        let (handle, segment_id, synced_bytes, durable_upto, written) = self
            .appender
            .run_behind(inline, move |may_wait| {
                // An append rolling inline holds the write lock across a
                // seal's fsync.
                let segments = if may_wait {
                    capturer.segments.read()
                } else {
                    capturer.segments.try_read()?
                };
                let active = segments.active();
                // The writer's own sync (a seal, a truncation) failed. Flushing
                // through a cloned handle would not see that.
                if active.is_poisoned() {
                    capturer.poison_after_writer_failure(&segments);
                    drop(segments);
                    return Some(Ok(Err(capturer
                        .check_healthy()
                        .expect_err("just poisoned"))));
                }
                Some(Ok(Ok((
                    active.sync_handle(),
                    active.id(),
                    active.size_bytes(),
                    segments.tail_offset(),
                    capturer.records_written.load(Ordering::Relaxed),
                ))))
            })
            .await
            .map_err(StorageError::Io)??;
        // Taken after the active handle, so a rollover that lands in between is
        // seen here rather than missed: `commit_roll` sets it before releasing
        // the lock this read just took.
        let retired = self.pending_seal.lock().clone();
        // Read under the flush lock, which a truncation takes too.
        let rewinds = self.mark.rewinds();
        let had_pending_seal = retired.is_some();

        let started = std::time::Instant::now();

        // With `FELIX_STORAGE_IO_URING=1` on Linux the fsync goes into a ring
        // and the kernel completes it. `None` means the ring is not
        // available -- an old kernel, or a container that forbids the syscall --
        // and the flush thread below takes over, because durability must not
        // depend on an optimisation being present (#548).
        let via_uring: Option<std::io::Result<()>> = if self.ring_enabled() {
            // The retired segment first: `durable_upto` covers records in both,
            // and may not be reported until every one of them is on disk.
            let mut result = Some(Ok(()));
            if let Some(retired) = retired.as_ref() {
                result = self
                    .ring_fsync(Arc::clone(retired), had_pending_seal)
                    .await?;
            }
            if matches!(result, Some(Ok(()))) {
                result = self
                    .ring_fsync(Arc::clone(&handle), had_pending_seal)
                    .await?;
            }
            result
        } else {
            None
        };

        #[cfg(any(test, feature = "fault-injection"))]
        let injected = self
            .fail_next_flush
            .swap(false, std::sync::atomic::Ordering::AcqRel)
            || self
                .fail_flush_past
                .try_update(Ordering::AcqRel, Ordering::Acquire, |past| {
                    (durable_upto > past).then_some(u64::MAX)
                })
                .is_ok();
        #[cfg(not(any(test, feature = "fault-injection")))]
        let injected = false;
        let injected_failure = move || {
            if injected {
                Err(std::io::Error::other("injected fsync failure"))
            } else {
                Ok(())
            }
        };

        // The mark is written only once the sync has returned, so it never runs
        // ahead of the device. Flushes are serialised, so marks land in order.
        let outcome = match via_uring {
            Some(result) => result
                .and_then(|()| injected_failure())
                .map(|()| self.note_synced_since(segment_id, synced_bytes, rewinds)),
            None => {
                let marker = Arc::clone(&self);
                self.flusher
                    .run(move || {
                        let synced = (|| {
                            // The retired segment first, for the same reason.
                            if let Some(retired) = retired {
                                sync_data(&retired)?;
                            }
                            sync_data(&handle)?;
                            injected_failure()
                        })();
                        match synced {
                            // On this thread too: the mark is a write, and a
                            // write waits on a busy filesystem journal like a
                            // sync does.
                            Ok(()) => marker.note_synced_since(segment_id, synced_bytes, rewinds),
                            // Here rather than in the caller, which may have
                            // stopped waiting: the failure must poison anyway.
                            Err(ref err) => marker.record_flush_failure(
                                had_pending_seal,
                                &StorageError::SyncFailed(err.to_string()),
                            ),
                        }
                        synced
                    })
                    .await
            }
        };
        if let Err(err) = outcome {
            let err = StorageError::SyncFailed(err.to_string());
            self.record_flush_failure(had_pending_seal, &err);
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
        let covered = written - self.records_flushed.swap(written, Ordering::Relaxed);
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
        // Rechecked after the flush, not only before it. A rollover can fail
        // while this flush is in flight, and `durable_upto` spans the retired
        // segment as well as the active one -- reporting it would acknowledge
        // records that the failed seal left unflushed.
        self.check_healthy()?;

        Ok(durable_upto)
    }

    /// Whether flushes go through a ring rather than the flush thread.
    fn ring_enabled(&self) -> bool {
        #[cfg(test)]
        if self.flush_thread_only.load(Ordering::Acquire) {
            return false;
        }
        #[cfg(test)]
        if self.fake_ring.lock().is_some() {
            return true;
        }
        platform_ring_enabled()
    }

    /// `fdatasync` one file through the ring, with this log's ring fsyncs
    /// one at a time. `None` means there is no ring.
    async fn ring_fsync(
        self: &Arc<Self>,
        file: Arc<File>,
        had_pending_seal: bool,
    ) -> Result<Option<std::io::Result<()>>> {
        // The ring runs fsyncs concurrently, and Linux reports a writeback
        // error once per open file. With two of this log's fsyncs in the ring
        // an earlier one could fail while a later one succeeds and is reaped
        // first, and the later flush would call durable what the kernel
        // dropped. The flush lock cannot prevent that: it goes with a caller
        // that stops waiting. So the gate goes with the hook, which the ring
        // drops only once it has answered.
        let gate = Arc::clone(&self.ring_gate).lock_owned().await;
        // An fsync answered while this one waited may have failed.
        self.check_healthy()?;
        let record = self.flush_failure_recorder(had_pending_seal);
        let on_failure = OnFailure::new(move |err| {
            record(err);
            drop(gate);
        });
        #[cfg(test)]
        {
            let fake = self.fake_ring.lock().clone();
            if let Some(ring) = fake {
                return Ok(ring.fsync(on_failure).await);
            }
        }
        Ok(platform_ring_fsync(file, on_failure).await)
    }

    /// Poisons the log for a failed sync, from wherever the failure is
    /// seen. Handed to the io_uring thread, which runs it when it reaps the
    /// completion: this flush's caller may have stopped waiting by then.
    pub(super) fn flush_failure_recorder(
        self: &Arc<Self>,
        had_pending_seal: bool,
    ) -> Box<dyn FnOnce(&std::io::Error) + Send> {
        let log = Arc::clone(self);
        Box::new(move |err| {
            log.record_flush_failure(had_pending_seal, &StorageError::SyncFailed(err.to_string()));
        })
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

#[cfg(target_os = "linux")]
fn platform_ring_enabled() -> bool {
    crate::io::uring_fsync::enabled()
}

#[cfg(not(target_os = "linux"))]
fn platform_ring_enabled() -> bool {
    false
}

#[cfg(target_os = "linux")]
async fn platform_ring_fsync(
    file: Arc<File>,
    on_failure: OnFailure,
) -> Option<std::io::Result<()>> {
    crate::io::uring_fsync::fsync(file, on_failure).await
}

#[cfg(not(target_os = "linux"))]
async fn platform_ring_fsync(
    _file: Arc<File>,
    _on_failure: OnFailure,
) -> Option<std::io::Result<()>> {
    None
}

/// Stands in for the io_uring flusher on every platform: each fsync is
/// handed to the test, which answers it when and how it likes.
#[cfg(test)]
#[derive(Clone)]
pub(super) struct FakeRing(pub(super) tokio::sync::mpsc::UnboundedSender<RingFsync>);

#[cfg(test)]
impl FakeRing {
    async fn fsync(&self, mut on_failure: OnFailure) -> Option<std::io::Result<()>> {
        let (reply, wait) = tokio::sync::oneshot::channel();
        on_failure.submitted();
        if let Err(unsent) = self.0.send(RingFsync { on_failure, reply }) {
            unsent.0.on_failure.withdraw();
            return None;
        }
        // The real ring's answer to a dropped request.
        Some(
            wait.await
                .unwrap_or_else(|_| Err(std::io::Error::other("io_uring service thread stopped"))),
        )
    }
}

/// One fsync in the fake ring. Dropped unanswered, as a panicking ring thread
/// would drop it, the hook runs before the reply goes (fields drop in order).
#[cfg(test)]
pub(super) struct RingFsync {
    on_failure: OnFailure,
    reply: tokio::sync::oneshot::Sender<std::io::Result<()>>,
}

#[cfg(test)]
impl RingFsync {
    /// Answer it the way the real ring does: the hook runs on a failure and
    /// is dropped either way before the reply.
    pub(super) fn answer(self, outcome: std::io::Result<()>) {
        self.on_failure.answer(&outcome);
        let _ = self.reply.send(outcome);
    }
}
