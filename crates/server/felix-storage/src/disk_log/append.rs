//! The append path: assign offsets and write a batch, rolling the active
//! segment first when the batch will not fit.
//!
//! A roll can also start early, in the background, while the segment still has
//! room (`LogConfig::rollover_threshold_percent`). A background roll that fails
//! is terminal for the log: see [`RollState`]. The active segment's block
//! reservation grows in the background too, but a failed extension is not
//! terminal.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::segments::{RollOutcome, SegmentSet};
use super::{DiskLog, LogInner, PendingAppend, producers};
use crate::log::{AppendRecord, AppendResult, Offset, RecordMark};
use crate::segment::SegmentWriter;
use crate::segment::reservation::Extension;
use crate::{CommitSequencer, CommitTurn, Result, StorageError, metrics_names};

impl DiskLog {
    /// Roll if needed, then assign offsets and write the batch, if `condition`
    /// holds; `Err` with the log's tail, and nothing written, when it does
    /// not. With `order`, the batch's range is claimed there as soon as its
    /// offsets are assigned.
    ///
    /// The work runs on the log's append thread. A batch whose caller gives
    /// up before the thread starts on it is skipped. Once started it is
    /// written and kept whatever the caller does; a caller gone by then never
    /// hears its offsets, and its claim is released with the reply. So a
    /// caller with work to finish after the append must not be cancelled
    /// while it waits (see `cache::log_cache` and `counter_log`).
    pub(super) async fn write_batch(
        inner: Arc<LogInner>,
        records: Vec<AppendRecord>,
        condition: WriteIf,
        order: Option<&Arc<CommitSequencer>>,
    ) -> Result<std::result::Result<Written, Offset>> {
        if records.is_empty() {
            return Err(StorageError::InvalidRange);
        }
        inner.check_healthy()?;
        // Digested here rather than where they are observed, which is under
        // the `segments` write lock every other append waits on.
        let digests: Vec<u64> = if records.iter().any(|r| r.mark != RecordMark::None) {
            records
                .iter()
                .map(|r| producers::marked_digest(r.mark, &r.payload))
                .collect()
        } else {
            Vec::new()
        };

        let appender = Arc::clone(&inner);
        let order = order.cloned();
        let appended = inner
            .appender
            .run_then(
                // `gone` turns true once this future is dropped: a batch the
                // thread has not started on is then skipped.
                move |gone| {
                    let kept =
                        appender.append_now(&records, &digests, condition, order.as_ref(), gone);
                    let (reply, index) = match kept {
                        Ok(Ok(kept)) => (
                            Ok(Ok((kept.written, kept.prepare_roll, kept.extension))),
                            (!kept.index.is_empty())
                                .then(|| (kept.index, kept.segment, Arc::clone(&appender))),
                        ),
                        Ok(Err(tail)) => (Ok(Err(tail)), None),
                        Err(err) => (Err(err), None),
                    };
                    (Ok((reply, records, appender)), index)
                },
                // After the reply: a lone caller does not wait for the index
                // write.
                |index| {
                    if let Some((index, segment, inner)) = index {
                        let _appends = inner.append_lock.lock();
                        index.write(segment);
                    }
                },
            )
            .await
            .map_err(StorageError::Io)?;
        // Dropped here rather than on the append thread, so neither the batch's
        // memory nor the log's reference count changes hands per append.
        let (appended, records, appender) = appended;
        drop((records, appender));
        let appended = appended?;
        let (written, prepare_roll, extension) = match appended {
            Ok(appended) => appended,
            Err(tail) => return Ok(Err(tail)),
        };
        if let Some(extension) = extension {
            Arc::clone(&inner).extend_reservation(extension);
        }

        // Start the replacement while the current segment still has room,
        // so the flushes it costs never land on an append. `Idle ->
        // Preparing` is a compare-exchange, which is what keeps exactly one
        // rollover in flight and leaves `Failed` terminal.
        if prepare_roll
            && inner
                .roll_state
                .compare_exchange(
                    RollState::Idle as u8,
                    RollState::Preparing as u8,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
        {
            let roller = Arc::clone(&inner);
            metrics::counter!(metrics_names::SEGMENT_ROLL_BACKGROUND_TOTAL).increment(1);
            let handle = tokio::spawn(async move {
                match Arc::clone(&roller).roll_in_background().await {
                    Ok(()) => roller
                        .roll_state
                        .store(RollState::Idle as u8, Ordering::Release),
                    Err(err) => {
                        // Terminal. A rollover fails on a failed fsync or a
                        // failed file creation, and both mean the log can no
                        // longer honour what it has already acknowledged.
                        // Retrying would just fail again, quietly, forever.
                        tracing::error!(
                            shard = %roller.label,
                            error = %err,
                            "background segment rollover failed; the log will reject further appends",
                        );
                        metrics::counter!(metrics_names::SEGMENT_ROLL_FAILED_TOTAL).increment(1);
                        // Usually already recorded, by the seal that failed.
                        // This covers a failure earlier in the rollover --
                        // building the replacement, or installing it.
                        roller.record_roll_failure(&err);
                    }
                }
            });
            // Replaces a handle only when the previous roll has finished,
            // because the compare-exchange above admits one at a time.
            *inner.roll_task.lock() = Some(handle);
        }

        Ok(Ok(written))
    }
}

/// A batch [`DiskLog::write_batch`] wrote, and its claim if it was asked for
/// one.
pub(super) struct Written {
    pub(super) pending: PendingAppend,
    pub(super) turn: Option<CommitTurn<'static>>,
}

impl Written {
    /// The batch and its claim, for a write that was given an order.
    pub(super) fn claimed(self) -> (PendingAppend, CommitTurn<'static>) {
        (self.pending, self.turn.expect("written with an order"))
    }
}

/// What [`LogInner::append_now`] kept: the batch, whether the segment has
/// crossed the early-roll threshold, the reservation step it has earned, and
/// index entries still to write.
struct Kept {
    written: Written,
    prepare_roll: bool,
    extension: Option<Extension>,
    index: crate::segment::index::UnwrittenEntries,
    segment: u64,
}

/// Where a test holds an append.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HoldAt {
    /// Just before the write.
    Write,
    /// After the batch is kept, before the caller hears of it.
    Reply,
}

/// When [`DiskLog::write_batch`] may write.
#[derive(Debug, Clone, Copy)]
pub(super) enum WriteIf {
    Always,
    /// The batch is the rest of this producer batch, which must still be open
    /// at the tail.
    Continuing {
        producer_id: u64,
        sequence: u64,
    },
    /// The batch must start at exactly this offset.
    At(Offset),
}

impl LogInner {
    /// The append itself, on the log's append thread: roll if the batch will
    /// not fit, then encode the batch and give it its place under `segments`,
    /// write it without that lock, and take it in under the lock again.
    /// `Err` with the tail when `condition` does not hold or the caller gave
    /// up before the thread got to it; otherwise the batch, and whether the
    /// segment has crossed the early-roll threshold.
    ///
    /// The tail is read under the same lock the condition was checked under,
    /// so a refusal reports the tail it was refused against.
    fn append_now(
        &self,
        records: &[AppendRecord],
        digests: &[u64],
        condition: WriteIf,
        order: Option<&Arc<CommitSequencer>>,
        gone: &dyn Fn() -> bool,
    ) -> Result<std::result::Result<Kept, Offset>> {
        let _appends = self.append_lock.lock();
        if gone() {
            return Ok(Err(self.segments.read().tail_offset()));
        }
        let mut segments = self.segments.write();
        if segments.would_roll_within(records, self.roll_pending()) {
            self.before_inline_roll();
            self.sync_pending_seal()?;
            if let Err(err) = segments.roll_for(records) {
                self.poison_after_writer_failure(&segments);
                return Err(err);
            }
            // `roll` sealed the retired segment, so the snapshot describes
            // only records already on the device.
            self.note_sealed_before(segments.active().id());
            let snapshot = self.producer_snapshot(&segments);
            drop(segments);
            self.store_producer_snapshot(snapshot);
            segments = self.segments.write();
        }
        let holds = match condition {
            WriteIf::Always => true,
            WriteIf::Continuing {
                producer_id,
                sequence,
            } => self.batch_open_at_tail(&segments, producer_id, sequence),
            WriteIf::At(offset) => segments.tail_offset() == offset,
        };
        if !holds {
            return Ok(Err(segments.tail_offset()));
        }
        segments.check_open()?;
        let staged = segments.active_mut().stage(records)?;
        drop(segments);

        self.hold_append_at(HoldAt::Write);
        let wrote = staged.write();

        // Kept even if the caller has gone by now, as when the write ran in
        // its poll: cutting the batch back would free the preallocated blocks
        // past it, and a flush may already have covered it.
        let mut segments = self.segments.write();
        let segment = segments.active().id();
        let ((first_offset, last_offset), index) = segments.active_mut().finish(staged, wrote)?;
        self.observe_marks(first_offset, records, digests);
        self.records_written
            .fetch_add(records.len() as u64, Ordering::Relaxed);
        let durable_target = segments.tail_offset();
        let prepare_roll = segments.should_prepare_roll();
        let extension = segments.active_mut().reservation_due();
        drop(segments);

        let pending = PendingAppend {
            result: AppendResult {
                first_offset,
                last_offset,
            },
            durable_target,
        };
        let turn = order.map(|order| order.reserve_owned(first_offset, last_offset + 1));
        self.hold_append_at(HoldAt::Reply);
        Ok(Ok(Kept {
            written: Written { pending, turn },
            prepare_roll,
            extension,
            index,
            segment,
        }))
    }

    /// Build the next segment and swap it in, with every fsync off the lock.
    ///
    /// Runs on a blocking thread. The segment lock is taken twice and held only
    /// for pointer work: once to install the replacement, once to record the
    /// retired segment as sealed. The two expensive halves — creating the new
    /// file and flushing the old one — happen with no lock held, so an append
    /// never queues behind a rollover's flushes.
    async fn roll_in_background(self: Arc<Self>) -> Result<()> {
        let inner = Arc::clone(&self);
        tokio::task::spawn_blocking(move || {
            // The plan is copied out under a read lock; the segment is built
            // with no lock at all. Creating it fsyncs its header and its parent
            // directory, so by the time anything can be appended here it is
            // durable -- and holding a lock across those two flushes would stall
            // every append waiting on the write lock, which is the entire cost
            // this design exists to remove.
            let plan = { inner.segments.read().roll_plan() };
            let prepared = plan.build()?;

            let (retired, snapshot, active) = {
                let _appends = inner.append_lock.lock();
                let mut segments = inner.segments.write();
                match segments.commit_roll(prepared)? {
                    RollOutcome::Installed(retired) => {
                        // Published before the lock is released, so no flush can
                        // observe the new active segment without also seeing the
                        // retired one it has to cover.
                        *inner.pending_seal.lock() = Some(retired.sync_handle());
                        // As of the new segment's base: everything it covers
                        // is in segments that are sealed once this roll is.
                        let active = segments.active().id();
                        (retired, inner.producer_snapshot(&segments), active)
                    }
                    // The tail moved past the offset this segment was built
                    // for. Delete it here — leaving it for recovery to clean up
                    // works, but only because recovery knows the rule.
                    RollOutcome::Stale(prepared) => {
                        drop(segments);
                        metrics::counter!(metrics_names::SEGMENT_ROLL_DISCARDED_TOTAL).increment(1);
                        return prepared.discard();
                    }
                }
            };

            inner
                .roll_state
                .store(RollState::Sealing as u8, Ordering::Release);

            // Flushed with no lock held. The segment is already listed as
            // sealed and readable — this only makes it durable.
            let mut retired = retired;
            match inner.seal_retired(&mut retired) {
                Ok(()) => {
                    // Only now: these records are on the device, so a flush no
                    // longer has to cover them.
                    *inner.pending_seal.lock() = None;
                    inner.note_sealed_before(active);
                    // Saved only once they are, so the snapshot never vouches
                    // for a batch a crash could still take away.
                    inner.store_producer_snapshot(snapshot);
                    Ok::<(), StorageError>(())
                }
                Err(err) => {
                    // Order matters. `pending_seal` stays set and the failure is
                    // recorded before this task returns, so an `OnCommit` append
                    // already in flight cannot find a cleared `pending_seal`
                    // alongside a roll state that has not been marked failed
                    // yet, sync only the new segment, and acknowledge records
                    // that are still sitting in an unflushed retired one.
                    inner.record_roll_failure(&err);
                    Err(err)
                }
            }
        })
        .await
        .map_err(|err| StorageError::Io(std::io::Error::other(err)))?
    }

    /// Grow the active segment's block reservation on a blocking thread, so
    /// the append that earned it does not wait on the call. Best effort: a
    /// failure only means later writes allocate their own blocks, which the
    /// write path already handles down to a full disk.
    fn extend_reservation(self: Arc<Self>, extension: Extension) {
        tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            {
                let hold = self.hold_next_extension.lock().take();
                if let Some(release) = hold {
                    let _ = release.recv();
                }
            }
            #[cfg(test)]
            let applied = if self.fail_extensions.load(Ordering::Acquire) {
                Err(std::io::ErrorKind::StorageFull.into())
            } else {
                extension.apply()
            };
            #[cfg(not(test))]
            let applied = extension.apply();
            if let Err(err) = applied {
                tracing::warn!(
                    shard = %self.label,
                    error = %err,
                    reserve_to = extension.to(),
                    "could not extend the active segment's block reservation; appends will allocate as they write",
                );
                metrics::counter!(metrics_names::SEGMENT_RESERVE_FAILED_TOTAL).increment(1);
            }
            #[cfg(test)]
            self.extensions_done.fetch_add(1, Ordering::AcqRel);
        });
    }

    /// Seal the active segment now, off the append path, and return the
    /// active segment's base afterwards: every offset below it is in a sealed
    /// segment. Waits out a rollover already in flight rather than racing it.
    pub(super) async fn roll_now(self: Arc<Self>) -> Result<crate::log::Offset> {
        loop {
            self.check_healthy()?;
            match self.roll_state.compare_exchange(
                RollState::Idle as u8,
                RollState::Preparing as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(1)).await,
            }
        }
        match Arc::clone(&self).roll_in_background().await {
            Ok(()) => {
                self.roll_state
                    .store(RollState::Idle as u8, Ordering::Release);
                Ok(self.segments.read().active().base_offset())
            }
            Err(err) => {
                self.record_roll_failure(&err);
                Err(err)
            }
        }
    }

    /// Whether a background rollover is between its start and its completion.
    fn roll_pending(&self) -> bool {
        matches!(
            RollState::from_u8(self.roll_state.load(Ordering::Acquire)),
            RollState::Preparing | RollState::Sealing
        )
    }

    /// Stretches an inline rollover to the length a real one's flushes take,
    /// which a temp-dir test does not otherwise reproduce, and marks the window
    /// so a test can see what the runtime managed to do during it.
    #[cfg(test)]
    fn before_inline_roll(&self) {
        let millis = self.slow_inline_roll_millis.load(Ordering::Acquire);
        if millis == 0 {
            return;
        }
        self.inline_roll_active.store(true, Ordering::Release);
        std::thread::sleep(std::time::Duration::from_millis(millis));
        self.inline_roll_active.store(false, Ordering::Release);
    }

    #[cfg(not(test))]
    #[inline]
    fn before_inline_roll(&self) {}

    /// Holds the next append at `at` while a test asks, and marks the window.
    /// Bounded, so a test whose runtime the hold stalls fails instead of
    /// hanging.
    #[cfg(test)]
    fn hold_append_at(&self, at: HoldAt) {
        let release = {
            let mut hold = self.hold_next_append.lock();
            match hold.take() {
                Some((point, release)) if point == at => release,
                other => {
                    *hold = other;
                    return;
                }
            }
        };
        self.append_held.store(true, Ordering::Release);
        let _ = release.recv_timeout(std::time::Duration::from_secs(2));
        self.append_held.store(false, Ordering::Release);
    }

    #[cfg(not(test))]
    #[inline]
    fn hold_append_at(&self, _at: HoldAt) {}

    /// Sync the segment a background roll retired but has not sealed yet.
    ///
    /// For an inline roll, which seals the active segment: without this the
    /// newer segment could reach the device whole while the older one's tail
    /// does not, and recovery cannot tell that from lost records.
    pub(super) fn sync_pending_seal(&self) -> Result<()> {
        let Some(retired) = self.pending_seal.lock().clone() else {
            return Ok(());
        };
        crate::io::sync_data(&retired).map_err(|err| {
            let err = StorageError::SyncFailed(err.to_string());
            self.record_roll_failure(&err);
            err
        })
    }

    /// Seal the retired segment, with a hook tests use to force a failure.
    fn seal_retired(&self, retired: &mut SegmentWriter) -> Result<()> {
        #[cfg(test)]
        {
            let hold = self.hold_next_seal.lock().take();
            if let Some(release) = hold {
                let _ = release.recv();
            }
        }
        #[cfg(test)]
        if self.fail_seal.load(Ordering::Acquire) {
            return Err(StorageError::SyncFailed(
                "injected seal failure".to_string(),
            ));
        }
        retired.seal().map(|_| ())
    }

    /// Record a failed rollover as terminal, and say why.
    pub(super) fn record_roll_failure(&self, err: &StorageError) {
        self.poison(format!("a background segment rollover failed ({err})"));
        self.roll_state
            .store(RollState::Failed as u8, Ordering::Release);
    }

    /// Stop the log for good.
    ///
    /// Idempotent and first-writer-wins: the first failure is the interesting
    /// one, and a later flush failing for the same underlying reason should not
    /// overwrite it.
    pub(super) fn poison(&self, reason: String) {
        let mut failure = self.failure.lock();
        if failure.is_none() {
            tracing::error!(shard = %self.label, %reason, "log stopped; it will reject further appends");
            *failure = Some(reason);
        }
    }

    /// Poison the log if a truncation or reset failed partway. Its segment,
    /// mark and epoch files may no longer agree with memory, and a failed
    /// fsync may have dropped pages a later one would then claim.
    pub(super) fn poison_after_rewind(&self, rewound: Result<()>) -> Result<()> {
        if let Err(err) = &rewound {
            self.poison(format!("a truncation or reset failed: {err}"));
        }
        rewound
    }

    /// Poison the log if the active writer has poisoned itself, which it does
    /// when one of its own syncs fails.
    pub(super) fn poison_after_writer_failure(&self, segments: &SegmentSet) {
        if segments.active().is_poisoned() {
            self.poison("a sync of the active segment failed".to_string());
        }
    }

    /// Whether a failure has stopped the log for good. Unlike
    /// [`Self::check_healthy`], a clean close does not count.
    pub(super) fn is_poisoned(&self) -> bool {
        self.failure.lock().is_some()
            || RollState::from_u8(self.roll_state.load(Ordering::Acquire)) == RollState::Failed
    }

    /// The terminal error, if one has been recorded.
    ///
    /// Checked on every path that either accepts new work or reports
    /// durability — not just at the entry to an append. A flush or rollover can
    /// fail while an append is already in flight, and that append must not be
    /// acknowledged on the strength of a flush that did not cover it.
    pub(super) fn check_healthy(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(StorageError::Closed(self.label.clone()));
        }
        if let Some(reason) = self.failure.lock().as_deref() {
            return Err(StorageError::SyncFailed(format!(
                "{}: {reason}; the log is no longer accepting appends",
                self.label
            )));
        }
        // A `Failed` state with no recorded reason means the rollover task
        // itself panicked or was cancelled. Still terminal.
        if RollState::from_u8(self.roll_state.load(Ordering::Acquire)) == RollState::Failed {
            return Err(StorageError::SyncFailed(format!(
                "{}: a background segment rollover failed; the log is no longer accepting appends",
                self.label
            )));
        }
        Ok(())
    }
}

/// Lifecycle of the background rollover.
///
/// Explicit rather than a boolean because the states are not symmetric: the
/// first two are recoverable and self-clearing, the last is terminal.
///
/// ```text
///   Idle ──► Preparing ──► Sealing ──► Idle
///              │              │
///              └──────────────┴──────► Failed  (terminal)
/// ```
///
/// * `Preparing` — building the replacement segment. Nothing is installed yet;
///   a crash here leaves an uninstalled segment that recovery discards.
/// * `Sealing` — the replacement is live and the retired segment is being
///   flushed. Reads already route across it, so a crash here loses nothing that
///   the fsync policy had promised.
/// * `Failed` — the retired segment could not be flushed. The log stops
///   accepting appends: a failed fsync means bytes that were reported durable
///   may not be, and continuing to append over that is worse than stopping.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub(super) enum RollState {
    Idle = 0,
    Preparing = 1,
    Sealing = 2,
    Failed = 3,
}

impl RollState {
    pub(super) fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Preparing,
            2 => Self::Sealing,
            3 => Self::Failed,
            _ => Self::Idle,
        }
    }
}
