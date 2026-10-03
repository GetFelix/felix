//! The append path: assign offsets and write a batch, rolling the active
//! segment first when the batch will not fit.
//!
//! Appends run on the log's append thread, never on a reactor thread: a
//! `write` into the page cache blocks once the kernel throttles a process that
//! dirties pages faster than the device takes them, and a reactor thread
//! blocked there takes every task scheduled on it down too. The thread also
//! drops the `segments` lock for the write itself, so nothing that only reads
//! the segment set waits on the disk either.
//!
//! A roll can also start early, in the background, while the segment still has
//! room (`LogConfig::rollover_threshold_percent`). A background roll that fails
//! is terminal for the log: see [`RollState`].

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::segments::{PreparedSegment, RollOutcome, SegmentSet};
use super::{DiskLog, LogInner, PendingAppend, producers};
use crate::commit_order::{CommitSequencer, CommitTurn};
use crate::log::{AppendRecord, AppendResult, RecordMark};
use crate::segment::SegmentWriter;
use crate::{Result, StorageError, metrics_names};

/// How long the append thread polls for the next append before parking, and
/// an uncontended caller polls for its result before yielding.
///
/// An append's write into the page cache takes a few microseconds, and so does
/// each kernel wake-up a hand-off to another thread costs. Without polling, a
/// lone publisher's unbatched appends measured about three times slower than
/// writing on the caller's thread; with it, within a few percent. See
/// `docs/storage-performance.md`.
pub(super) const APPEND_SPIN: std::time::Duration = std::time::Duration::from_micros(20);

impl DiskLog {
    /// Roll if needed, then assign offsets and write the batch, if `condition`
    /// holds; `None`, and nothing written, when it does not. Checked on the
    /// append thread, which is the only place offsets are assigned, so nothing
    /// can land in between.
    ///
    /// Cancelling the returned future is as safe as it was when the write ran
    /// in the caller's own poll. A batch whose caller has gone before it is
    /// accounted for is not written, or is cut off again if it already was,
    /// so it spends no offsets and leaves nothing behind. One whose caller
    /// goes in the moment after holds its offsets before the cancellation
    /// returns, exactly as a batch cancelled while waiting to be flushed does.
    ///
    /// With `order`, the batch's range is claimed there as soon as its offsets
    /// are, and handed back with it. That covers the moment after: the claim
    /// the caller never received is dropped with the reply and releases the
    /// range, so the writers queued behind it are not left waiting on a turn
    /// nobody holds.
    pub(super) async fn write_batch(
        inner: Arc<LogInner>,
        records: Vec<AppendRecord>,
        condition: WriteIf,
        order: Option<Arc<CommitSequencer>>,
    ) -> Result<Option<Written>> {
        if records.is_empty() {
            return Err(StorageError::InvalidRange);
        }
        inner.check_healthy()?;
        // Digested here, where it costs nobody else anything, rather than on
        // the append thread every other append on this log queues behind.
        let digests: Vec<u64> = if records.iter().any(|r| r.mark != RecordMark::None) {
            records
                .iter()
                .map(|r| producers::marked_digest(r.mark, &r.payload))
                .collect()
        } else {
            Vec::new()
        };

        let writer = Arc::clone(&inner);
        let appended = inner
            .append_thread
            .call_attended(move |waiting| {
                writer.append_on_thread(&records, &digests, condition, order, waiting)
            })
            .await
            .map_err(StorageError::Io)??;
        let Some(appended) = appended else {
            return Ok(None);
        };
        if appended.prepare_roll {
            inner.start_background_roll();
        }
        Ok(Some(appended.written))
    }
}

/// A batch written by [`DiskLog::write_batch`].
pub(super) struct Written {
    pub(super) pending: PendingAppend,
    /// The batch's range in the caller's commit order, when one was given.
    pub(super) turn: Option<CommitTurn<'static>>,
}

/// What the append thread hands back for one batch.
struct Appended {
    written: Written,
    /// The segment has crossed the early-roll threshold.
    prepare_roll: bool,
}

impl LogInner {
    /// The body of [`DiskLog::write_batch`], on the append thread. `waiting`
    /// says whether the caller still wants the batch. `None`, with nothing
    /// written, when `condition` fails or the caller has gone.
    fn append_on_thread(
        &self,
        records: &[AppendRecord],
        digests: &[u64],
        condition: WriteIf,
        order: Option<Arc<CommitSequencer>>,
        waiting: &dyn Fn() -> bool,
    ) -> Result<Option<Appended>> {
        if !waiting() {
            return Ok(None);
        }
        let mut segments = self.segments.write();
        // While a background rollover is building the replacement, the
        // segment is allowed to grow past its configured size rather than
        // rolling here. That headroom is what gives the preparation time to
        // finish; without it the inline path below wins every race.
        let roll_pending = self.roll_pending();
        if segments.would_roll_within(records, roll_pending) {
            // The hard-limit fallback, and the only roll when the background
            // one is off (the default). It flushes under the lock, but on this
            // thread, so appends wait for it here and not on a reactor thread.
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
            return Ok(None);
        }
        let staged = segments.stage(records)?;
        drop(segments);

        // The write, with no lock held. Readers, flushes and the periodic
        // syncer only need `segments` briefly, and none of them waits for the
        // disk here. Nothing else can change the active segment meanwhile:
        // everything that does runs on this thread.
        self.hold_append_at(HoldAt::Write);
        let wrote = staged.write();

        let mut segments = self.segments.write();
        if let Err(err) = wrote {
            return Err(segments.active_mut().abandon(staged, err));
        }
        // Decided under the lock that readers of the tail take, so a caller
        // that gave up before this sees no trace of the batch, and one that
        // gives up after it finds the batch in the tail.
        if !waiting() {
            let abandoned = std::io::Error::other("the append's caller stopped waiting");
            let _ = segments.active_mut().abandon(staged, abandoned);
            self.poison_after_writer_failure(&segments);
            return Ok(None);
        }
        let (first_offset, last_offset, index) = segments.active_mut().finish(staged);
        self.observe_marks(first_offset, records, digests);
        let durable_target = segments.tail_offset();
        let prepare_roll = segments.should_prepare_roll();
        drop(segments);

        // Index entries go to their file only once the batch is kept, and with
        // no lock held, for the same reason as the batch itself.
        if let Err(err) = index.write() {
            self.segments
                .write()
                .active_mut()
                .index_write_failed(first_offset, &err);
        }

        let turn = order.map(|order| order.reserve_owned(first_offset, last_offset + 1));
        self.hold_append_at(HoldAt::Reply);
        Ok(Some(Appended {
            written: Written {
                pending: PendingAppend {
                    result: AppendResult {
                        first_offset,
                        last_offset,
                    },
                    durable_target,
                },
                turn,
            },
            prepare_roll,
        }))
    }

    /// Start building the next segment while the current one still has room,
    /// so the flushes it costs never land on an append. `Idle -> Preparing` is
    /// a compare-exchange, which is what keeps exactly one rollover in flight
    /// and leaves `Failed` terminal.
    fn start_background_roll(self: &Arc<Self>) {
        if self
            .roll_state
            .compare_exchange(
                RollState::Idle as u8,
                RollState::Preparing as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return;
        }
        let roller = Arc::clone(self);
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
        // Replaces a handle only when the previous roll has finished, because
        // the compare-exchange above admits one at a time.
        *self.roll_task.lock() = Some(handle);
    }

    /// Stops an append at `at` while a test holds it. Bounded, so a runtime
    /// the hold does stall comes back and the test can say so instead of
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
    At(crate::log::Offset),
}

impl LogInner {
    /// Build the next segment and swap it in, with every fsync off the lock.
    ///
    /// Three steps. The replacement is built on a blocking thread with no lock
    /// held, installed on the append thread under a lock held only for pointer
    /// work, and the retired segment is flushed on a blocking thread with no
    /// lock held. The two expensive halves -- creating the new file and
    /// flushing the old one -- never hold up an append. The install runs on
    /// the append thread because an append's write runs without the lock, and
    /// swapping the segment out from under a staged batch would land it in a
    /// segment already listed as sealed.
    async fn roll_in_background(self: Arc<Self>) -> Result<()> {
        let builder = Arc::clone(&self);
        // The plan is copied out under a read lock; the segment is built with
        // no lock at all. Creating it fsyncs its header and its parent
        // directory, so by the time anything can be appended here it is
        // durable -- and holding a lock across those two flushes would stall
        // every append waiting on the write lock, which is the entire cost this
        // design exists to remove.
        let prepared = tokio::task::spawn_blocking(move || {
            let plan = { builder.segments.read().roll_plan() };
            plan.build()
        })
        .await
        .map_err(|err| StorageError::Io(std::io::Error::other(err)))??;

        let installer = Arc::clone(&self);
        let installed = self
            .append_thread
            .call(move || installer.install_rolled(prepared))
            .await
            .map_err(StorageError::Io)??;

        let inner = Arc::clone(&self);
        tokio::task::spawn_blocking(move || {
            let (mut retired, snapshot, active) = match installed {
                Installed::Rolled {
                    retired,
                    snapshot,
                    active,
                } => (retired, snapshot, active),
                // The tail moved past the offset this segment was built for.
                // Delete it here — leaving it for recovery to clean up works,
                // but only because recovery knows the rule.
                Installed::Stale(prepared) => {
                    metrics::counter!(metrics_names::SEGMENT_ROLL_DISCARDED_TOTAL).increment(1);
                    return prepared.discard();
                }
            };

            // Flushed with no lock held. The segment is already listed as
            // sealed and readable — this only makes it durable.
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

    /// Swap a prepared segment in, on the append thread.
    fn install_rolled(&self, prepared: PreparedSegment) -> Result<Installed> {
        let mut segments = self.segments.write();
        let retired = match segments.commit_roll(prepared)? {
            RollOutcome::Installed(retired) => retired,
            RollOutcome::Stale(prepared) => return Ok(Installed::Stale(prepared)),
        };
        // Published before the lock is released, so no flush can observe the
        // new active segment without also seeing the retired one it has to
        // cover.
        *self.pending_seal.lock() = Some(retired.sync_handle());
        // As of the new segment's base: everything it covers is in segments
        // that are sealed once this roll is.
        let active = segments.active().id();
        let snapshot = self.producer_snapshot(&segments);
        drop(segments);
        self.roll_state
            .store(RollState::Sealing as u8, Ordering::Release);
        Ok(Installed::Rolled {
            retired: Box::new(retired),
            snapshot,
            active,
        })
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

/// Where a test can hold an append on the append thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HoldAt {
    /// Just before the write, standing in for a write the kernel throttles.
    Write,
    /// After the batch is kept and claimed, before its caller hears of it.
    Reply,
}

/// What installing a prepared segment did.
enum Installed {
    /// It is the active segment now, and `retired` is to be sealed.
    Rolled {
        retired: Box<SegmentWriter>,
        snapshot: (crate::log::Offset, producers::ProducerState),
        active: crate::log::SegmentId,
    },
    /// Another roll got there first; the files are to be deleted.
    Stale(PreparedSegment),
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
