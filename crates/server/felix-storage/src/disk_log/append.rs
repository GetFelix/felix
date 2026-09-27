//! The append path: assign offsets and write a batch, rolling the active
//! segment first when the batch will not fit.
//!
//! A roll can also start early, in the background, while the segment still has
//! room (`LogConfig::rollover_threshold_percent`). A background roll that fails
//! is terminal for the log: see [`RollState`].

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::segments::{RollOutcome, SegmentSet};
use super::{DiskLog, LogInner, PendingAppend, producers};
use crate::log::{AppendRecord, AppendResult, RecordMark};
use crate::segment::SegmentWriter;
use crate::{Result, StorageError, metrics_names};

/// Bound on rollover retries in a single append.
///
/// Each retry means another publisher won the race to fill the segment; a
/// handful is generous, and failing loudly beats spinning under a pathological
/// interleaving.
const MAX_ROLL_ATTEMPTS: usize = 8;

impl DiskLog {
    /// Roll if needed, then assign offsets and write the batch, if `condition`
    /// holds; `None`, and nothing written, when it does not. Checked under the
    /// lock the write takes, so nothing can land in between.
    pub(super) async fn write_batch(
        inner: Arc<LogInner>,
        records: Vec<AppendRecord>,
        condition: WriteIf,
    ) -> Result<Option<PendingAppend>> {
        if records.is_empty() {
            return Err(StorageError::InvalidRange);
        }
        inner.check_healthy()?;
        // Shared with the rollover re-check, which runs on a blocking thread;
        // cloning it there would copy the batch once per retry.
        let records = Arc::new(records);
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

        // The hard-limit fallback. A background roll normally replaces the
        // segment well before this, so reaching here means the log filled
        // faster than a replacement could be built — rare, and still correct.
        for attempt in 0..MAX_ROLL_ATTEMPTS {
            // Taken before anything touches `segments`, the read below
            // included: a rollover holds the synchronous lock, so a publisher
            // that reaches it at all parks a worker either way.
            let mut gate = match inner.roll_gate.try_read() {
                Ok(gate) => gate,
                Err(_) => inner.roll_gate.read().await,
            };

            // While a background rollover is building the replacement, the
            // segment is allowed to grow past its configured size rather than
            // blocking here. That headroom is what gives the preparation time
            // to finish; without it the inline path below wins every race.
            let roll_pending = inner.roll_pending();
            if inner
                .segments
                .read()
                .would_roll_within(&records, roll_pending)
            {
                drop(gate);
                // Exclusive, so no append can be queued on `segments` while
                // the rollover flushes.
                let exclusive = inner.roll_gate.write().await;
                let roller = Arc::clone(&inner);
                let batch = Arc::clone(&records);
                tokio::task::spawn_blocking(move || {
                    let mut segments = roller.segments.write();
                    // Re-checked under the write lock: another publisher may
                    // have rolled already, and rolling twice leaves an empty
                    // segment behind.
                    if segments.would_roll_within(&batch, roller.roll_pending()) {
                        roller.before_inline_roll();
                        roller.sync_pending_seal()?;
                        if let Err(err) = segments.roll() {
                            roller.poison_after_writer_failure(&segments);
                            return Err(err);
                        }
                        // `roll` sealed the retired segment, so the snapshot
                        // describes only records already on the device.
                        roller.note_sealed_before(segments.active().id());
                        let snapshot = roller.producer_snapshot(&segments);
                        drop(segments);
                        roller.store_producer_snapshot(snapshot);
                    }
                    Ok::<(), StorageError>(())
                })
                .await
                .map_err(|err| StorageError::Io(std::io::Error::other(err)))??;
                // Downgraded rather than released: the gate is fair, so
                // releasing it would let every publisher queued behind this
                // rollover refill the segment before the one that paid for it
                // appends, which the retry budget does not cover.
                gate = exclusive.downgrade();
            }

            let mut segments = inner.segments.write();
            let roll_pending = inner.roll_pending();
            if segments.would_roll_within(&records, roll_pending) {
                // Filled again in the gap. Drop the lock and roll off-thread.
                drop(segments);
                drop(gate);
                debug_assert!(attempt + 1 < MAX_ROLL_ATTEMPTS, "rollover retry starved");
                continue;
            }
            let holds = match condition {
                WriteIf::Always => true,
                WriteIf::Continuing {
                    producer_id,
                    sequence,
                } => inner.batch_open_at_tail(&segments, producer_id, sequence),
                WriteIf::At(offset) => segments.tail_offset() == offset,
            };
            if !holds {
                return Ok(None);
            }
            let (first_offset, last_offset) = segments.append_within(&records, roll_pending)?;
            inner.observe_marks(first_offset, &records, &digests);
            let durable_target = segments.tail_offset();
            let prepare_roll = segments.should_prepare_roll();
            drop(segments);
            drop(gate);

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
                            metrics::counter!(metrics_names::SEGMENT_ROLL_FAILED_TOTAL)
                                .increment(1);
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

            return Ok(Some(PendingAppend {
                result: AppendResult {
                    first_offset,
                    last_offset,
                },
                durable_target,
            }));
        }

        Err(StorageError::Unsupported(
            "append could not secure segment capacity; rollover kept losing the race",
        ))
    }
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
