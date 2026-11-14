//! A crash-safe, disk-backed `AppendOnlyLog`.
//!
//! Module map:
//!
//! * `layout`    — `ShardKey` to directory name, safely.
//! * `segments`  — the segment set: rollover, offset routing, truncation.
//! * `sealed`    — sealed segments, and the bounded cache of their open files.
//! * `recovery`  — startup discovery, validation and torn-tail repair.
//! * `sync`      — fsync policy and group commit.
//! * `durable_mark` — how far the active segment is known to be synced.
//! * `retention` — deleting the oldest segments once a bound is exceeded.
//! * `offload`   — copying sealed segments to an object store first.
//! * `epochs`    — where each leadership generation began.
//! * `replica_state` — the highest generation accepted, and the commit offset.
//! * `ballot`    — which leader that generation was accepted from, and
//!   accepting one.
//! * `set`       — the replicas this one counts, the set it is leaving, and
//!   accepting a newer one.
//! * `producers` — each idempotent producer's place, derived from the records.
//! * `append`    — the append path, and the rollover it may have to start.
//! * `flush`     — making the active segment durable.
//! * `provider`  — one log per shard under a common root.
//!
//! This file is the seam between them and the async `AppendOnlyLog` trait.
//!
//! ## Where the blocking happens
//!
//! Two kinds of blocking I/O live behind this API, and they are treated
//! differently on purpose:
//!
//! * A `write` into the page cache is sub-microsecond in the normal case, so the
//!   append path performs it inline. Handing it to `spawn_blocking` would add
//!   more scheduling latency than the syscall itself costs, and this project
//!   cares about p999.
//! * A *rollover* is the exception on that path: sealing a segment and creating
//!   its successor fsync two files and a directory. Appends that trigger one
//!   roll on a blocking thread first, so the flush cost never lands on a
//!   reactor worker.
//! * An `fsync` genuinely blocks, for milliseconds on slow hardware. It runs on
//!   the log's own flush thread so it cannot stall a reactor thread, and does
//!   not queue behind other work on the shared blocking pool. The durable
//!   mark write that follows it runs there too (inline after an io_uring
//!   fsync): on a busy filesystem journal a write waits like a sync. Flushes
//!   are grouped, so one runs for many appends.
//! * `read_range` may touch cold blocks, so it runs entirely on `spawn_blocking`.
//!   It is a replay and catch-up path, not the publish hot path.

pub mod inspect;
pub mod layout;

mod append;
mod ballot;
pub(crate) mod crash_point;
#[cfg(feature = "raft")]
pub(crate) mod dir_lock;
pub(crate) mod durable_mark;
mod epochs;
mod flush;
#[cfg(feature = "fuzzing")]
pub(crate) mod fuzzing;
mod head;
mod offload;
mod producers;
mod provider;
mod recovery;
pub(crate) mod replica_state;
mod retention;
mod sealed;
mod segments;
mod set;
mod sync;

pub use producers::ProducerSequence;
pub use provider::DiskLogProvider;
pub use segments::RetentionOutcome;
pub use set::{Member, Membership, ReplicaSet};

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use parking_lot::{Mutex, RwLock};

use self::append::RollState;
use self::producers::ProducerState;
use self::sealed::SealedFiles;
use self::segments::SegmentSet;
use self::sync::{Durability, PeriodicSyncer};
use crate::io::log_thread::LogThread;
use crate::log::{
    AppendOnlyLog, AppendRecord, AppendResult, BoxFuture, Epoch, FsyncMode, LogConfig, LogRecord,
    Offset, ReadPage, ReadRange, RecordMark, SealedSegment, SegmentDescriptor, WrittenBy,
};
use crate::segment::{ReadBudget, TrimRecords};
use crate::{CommitSequencer, CommitTurn, Result, StorageError, metrics_names};

/// How long an append thread and a lone appender poll before parking. A
/// wake-up on each side costs more than the `write` being handed over.
const APPEND_SPIN: std::time::Duration = std::time::Duration::from_micros(20);

/// How often an advancing commit offset is written behind when the log is not
/// fsynced on commit. Its records are written behind too, so an offset that
/// outlived them would guard nothing.
pub const COMMIT_PERSIST_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// Set the first time any log in this process is poisoned, or a node
/// record's sync fails, and never cleared. A log closed since still counts:
/// whatever it acknowledged may not be on disk.
static ANY_POISONED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Whether a failure has stopped any log in this process for good, or a
/// node record's sync failed. A broker that saw one cannot say it stopped
/// with everything it acknowledged synced.
pub fn any_log_poisoned() -> bool {
    ANY_POISONED.load(Ordering::Acquire)
}

/// Count a failed sync of a node record as a poisoned log: the broker may
/// have lost what it vouched for.
pub(crate) fn note_lost_durability() {
    ANY_POISONED.store(true, Ordering::Release);
}

/// A log a failure stopped, for a readiness probe to report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoisonedLog {
    pub shard: String,
    /// The first failure, which is not always a flush: a truncation that
    /// failed partway poisons too.
    pub reason: String,
}

/// One entry per shard, with its first failure. Like [`ANY_POISONED`], never
/// cleared.
static POISONED: Mutex<Vec<PoisonedLog>> = Mutex::new(Vec::new());

/// The logs a failure has stopped in this process, each with the failure that
/// stopped it first.
pub fn poisoned_logs() -> Vec<PoisonedLog> {
    POISONED.lock().clone()
}

fn note_poisoned(shard: &str, reason: &str) {
    let mut poisoned = POISONED.lock();
    if !poisoned.iter().any(|log| log.shard == shard) {
        poisoned.push(PoisonedLog {
            shard: shard.to_string(),
            reason: reason.to_string(),
        });
    }
}

/// What [`DiskLog::accept_generation`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GenerationCheck {
    /// The generation already accepted, from this leader or with no leader
    /// named.
    Current,
    /// Newer than any accepted before; it is now on disk.
    Raised,
    /// Older than one already accepted. The sender has been replaced.
    Superseded { accepted: u64 },
    /// The generation already accepted, from another leader. Two nodes claim
    /// one generation, and this replica answered the other.
    Promised { leader: String },
}

/// What [`DiskLog::accept_set`] found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetCheck {
    /// The set already kept.
    Current,
    /// Newer than the set kept before; it is now on disk.
    Raised,
    /// Older than the set kept, whose generation is `known`.
    Stale { known: u64 },
}

/// A durable, segmented, append-only log for one shard.
///
/// Cheap to clone: every clone shares the same files and the same in-memory
/// state. Cloning is how the provider hands the same shard to several callers
/// without two writers racing over one directory.
#[derive(Clone, Debug)]
pub struct DiskLog {
    inner: Arc<LogInner>,
}

impl DiskLog {
    /// Open (and recover) the log rooted at `dir`.
    ///
    /// `label` is the human-readable shard name used in errors and logs.
    ///
    /// Must be called from inside a Tokio runtime when the configured policy is
    /// [`FsyncMode::Periodic`], which needs a background timer; the other
    /// policies have no such requirement.
    ///
    /// A Raft log ([`LogConfig::raft_log`]) locks `dir` until it is closed or
    /// its last clone is dropped. Another open of the directory, from this
    /// process or another, fails with [`std::io::ErrorKind::ResourceBusy`]
    /// meanwhile. The lock goes with the process. To reopen in the same
    /// process, close the log first: a dropped clone can live on for a moment
    /// in a background task.
    pub fn open(
        dir: impl Into<PathBuf>,
        label: impl Into<String>,
        config: LogConfig,
    ) -> Result<Self> {
        let files = SealedFiles::new(config.max_open_sealed_segments);
        Self::open_shared(
            dir.into(),
            label.into(),
            config,
            None,
            files,
            OtherCommit::Drop,
        )
    }

    /// Open a shard log, creating it to begin at `base_offset` if it does not
    /// exist yet.
    ///
    /// For a replica receiving history that starts partway through a stream:
    /// the records before `base_offset` are gone from every copy in the
    /// cluster, so a log that begins there is complete rather than truncated,
    /// and a read below it is `Trimmed` exactly as it would be on the leader.
    ///
    /// **An existing log is opened as it stands and `base_offset` is ignored.**
    /// A restart must not reinterpret a shard that is already here, and the
    /// base it was created at is recorded in its own first segment. Only an
    /// empty directory is a shard being placed for the first time.
    pub fn open_at(
        dir: impl Into<PathBuf>,
        label: impl Into<String>,
        config: LogConfig,
        base_offset: Offset,
    ) -> Result<Self> {
        let files = SealedFiles::new(config.max_open_sealed_segments);
        Self::open_shared(
            dir.into(),
            label.into(),
            config,
            Some(base_offset),
            files,
            OtherCommit::Drop,
        )
    }

    /// [`Self::open`] for a Raft log that must not start on a committed id
    /// saved for some other log. When the index it names holds an entry of
    /// another term, `open` takes nothing as committed; this fails, with
    /// the saved id left as it was.
    ///
    /// `keep_alive` is held until the last clone of the log is dropped. The
    /// store passes its lock, so it is not released while a flusher can
    /// still write.
    #[cfg(feature = "raft")]
    pub(crate) fn open_refusing_other_commit(
        dir: impl Into<PathBuf>,
        label: impl Into<String>,
        config: LogConfig,
        keep_alive: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Result<Self> {
        let files = SealedFiles::new(config.max_open_sealed_segments);
        let log = Self::open_shared(
            dir.into(),
            label.into(),
            config,
            None,
            files,
            OtherCommit::Refuse,
        )?;
        *log.inner.keep_alive.lock() = Some(keep_alive);
        Ok(log)
    }

    /// Let go of the `keep_alive` value now, for a log that was shut down
    /// cleanly and is not written again.
    #[cfg(feature = "raft")]
    pub(crate) fn release_keep_alive(&self) {
        self.inner.keep_alive.lock().take();
    }

    pub fn label(&self) -> &str {
        &self.inner.label
    }

    pub fn config(&self) -> &LogConfig {
        &self.inner.config
    }

    /// Where the oldest segment starts: the oldest offset still on disk.
    /// Replication ships from here. A reader starts at
    /// [`Self::readable_base`] instead.
    pub fn base_offset(&self) -> Offset {
        self.inner.segments.read().base_offset()
    }

    /// The oldest offset [`AppendOnlyLog::read_range`] serves: the higher of
    /// [`Self::base_offset`] and the head floor that trim records set. A read
    /// below it is [`StorageError::Trimmed`].
    ///
    /// A trim's floor applies once its record is durable and, on a log held
    /// at its commit offset ([`Self::hold_retention_at_commit`]), below the
    /// commit offset. With no trim waiting for that, this costs two atomic
    /// loads beyond the base offset.
    pub fn readable_base(&self) -> Offset {
        let base = self.inner.segments.read().base_offset();
        base.max(self.inner.head_floor())
    }

    /// The trim record that set the head floor, if one applies.
    pub fn head_trim(&self) -> Option<HeadTrim> {
        self.inner.head_trim()
    }

    /// Exclusive bound on durable offsets: everything below it survives a crash.
    pub fn durable_offset(&self) -> Offset {
        self.inner.durability.durable_upto()
    }

    /// Whether a failed flush, sync, rollover or truncation has stopped the
    /// log. It then refuses appends until reopened, and records past
    /// [`Self::durable_offset`] may be ones whose append was never
    /// acknowledged.
    pub fn is_poisoned(&self) -> bool {
        self.inner.is_poisoned()
    }

    /// How many flushes this log has issued.
    ///
    /// Group commit means one flush serves many waiting appends, so N appends
    /// that coalesce produce far fewer than N flushes. That ratio is the
    /// property, and counting is the only way to see it that does not also
    /// measure the machine: a wall-clock speedup cannot tell "the flushes
    /// coalesced" from "this box could not put enough appends in flight for
    /// them to".
    pub fn flushes(&self) -> u64 {
        self.inner.durability.flushes()
    }

    /// Bytes written but not yet flushed — the data a crash would lose now.
    pub fn unsynced_bytes(&self) -> u64 {
        self.inner.segments.read().active().unsynced_bytes()
    }

    /// Device flushes this log has performed, for tests asserting that group
    /// commit shared them.
    #[cfg(test)]
    pub(crate) fn flushes_performed(&self) -> u64 {
        self.inner
            .flushes
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Sealed segments whose file and index the shared cache holds open.
    #[cfg(test)]
    pub(crate) fn open_sealed_segments(&self) -> usize {
        self.inner.segments.read().open_sealed_segments()
    }

    /// Every segment on disk, oldest first.
    pub fn segments(&self) -> Vec<SegmentDescriptor> {
        self.inner.segments.read().descriptors()
    }

    /// Where `producer_id`'s batch `sequence` stands in this log.
    ///
    /// Reflects every batch written so far, durable or not, so a caller that
    /// serialises a producer's batches sees each one as soon as it is written.
    pub fn producer_sequence(&self, producer_id: u64, sequence: u64) -> ProducerSequence {
        self.inner.producers.lock().classify(producer_id, sequence)
    }

    /// The sequence `producer_id` owes next in this log, or `None` when the
    /// log holds no batch from it.
    pub fn producer_next_sequence(&self, producer_id: u64) -> Option<u64> {
        self.inner.producers.lock().next_sequence(producer_id)
    }

    /// Assign offsets and write `records`, without waiting for durability.
    ///
    /// Split out of [`AppendOnlyLog::append`] so a caller can learn the offsets
    /// the moment they are consumed, rather than only once the batch is
    /// durable. After this returns, the records exist on disk and hold their
    /// offsets whether or not the durability wait that follows succeeds,
    /// fails, or is cancelled. A caller dropped before it returns may still
    /// have its batch written; one that must claim a place in the log's offset
    /// order uses [`Self::append_claimed`] instead.
    ///
    /// The returned [`PendingAppend`] must be passed to [`DiskLog::commit`] for
    /// the configured fsync policy to be honoured. Dropping it does not undo
    /// the write.
    pub async fn append_pending(&self, records: &[AppendRecord]) -> Result<PendingAppend> {
        let inner = Arc::clone(&self.inner);
        Self::write_batch(
            inner,
            records.to_vec(),
            append::WriteIf::Always,
            None,
            WrittenBy::Leader,
        )
        .await
        .map(|written| {
            written
                .expect("an unconditional write is always written")
                .pending
        })
    }

    /// [`DiskLog::append_pending`], with the batch's range claimed in `order`
    /// as soon as its offsets are assigned. A caller dropped before this
    /// returns still releases the range, so the writers behind it are not
    /// stranded, but the batch may still be written: run this to completion
    /// if what follows it must happen.
    pub async fn append_claimed(
        &self,
        records: &[AppendRecord],
        order: &Arc<CommitSequencer>,
    ) -> Result<(PendingAppend, CommitTurn<'static>)> {
        let inner = Arc::clone(&self.inner);
        let written = Self::write_batch(
            inner,
            records.to_vec(),
            append::WriteIf::Always,
            Some(order),
            WrittenBy::Leader,
        )
        .await?
        .expect("an unconditional write is always written");
        Ok(written.claimed())
    }

    /// [`DiskLog::append_claimed`], only if the batch would start at exactly
    /// `first_offset`. `Err` with the log's tail, and nothing written or
    /// claimed, when it would start anywhere else.
    ///
    /// The check is made where offsets are assigned, under the same lock, so
    /// two writers expecting the same offset cannot both pass it. A refused
    /// batch consumes no offset and claims no range, so it holds up no later
    /// append.
    pub async fn append_claimed_at(
        &self,
        first_offset: Offset,
        records: &[AppendRecord],
        order: &Arc<CommitSequencer>,
    ) -> Result<std::result::Result<(PendingAppend, CommitTurn<'static>), Offset>> {
        let inner = Arc::clone(&self.inner);
        let written = Self::write_batch(
            inner,
            records.to_vec(),
            append::WriteIf::At(first_offset),
            Some(order),
            WrittenBy::Leader,
        )
        .await?;
        Ok(written.map(append::Written::claimed))
    }

    /// [`DiskLog::append_pending`], only if the batch would start at exactly
    /// `first_offset`. `None`, and nothing written, when the tail is anywhere
    /// else.
    ///
    /// For a writer that decided what to write from a tail it read earlier,
    /// such as a follower storing a leader's batch at the leader's offsets:
    /// another append landing in between would otherwise put these records
    /// at offsets their sender never assigned.
    ///
    /// `writers` names the generations that wrote the records where they
    /// came from; a v9 segment stamps them with those, never its own.
    pub async fn append_pending_at(
        &self,
        first_offset: Offset,
        records: &[AppendRecord],
        writers: WrittenBy,
    ) -> Result<Option<PendingAppend>> {
        let inner = Arc::clone(&self.inner);
        let written = Self::write_batch(
            inner,
            records.to_vec(),
            append::WriteIf::At(first_offset),
            None,
            writers,
        )
        .await?;
        Ok(written.ok().map(|written| written.pending))
    }

    /// Write the rest of `producer_id`'s batch `sequence`, which the log holds
    /// only part of ([`ProducerSequence::Partial`]), without waiting for
    /// durability, claimed in `order` as [`Self::append_claimed`] does.
    /// `records` must be marked [`RecordMark::Continues`].
    ///
    /// `None`, and nothing written, when the batch is no longer open at the
    /// tail: something else was appended after its first part, so its rest can
    /// never follow it.
    pub async fn continue_claimed(
        &self,
        producer_id: u64,
        sequence: u64,
        records: &[AppendRecord],
        order: &Arc<CommitSequencer>,
    ) -> Result<Option<(PendingAppend, CommitTurn<'static>)>> {
        debug_assert!(records.iter().all(|r| r.mark == RecordMark::Continues));
        let inner = Arc::clone(&self.inner);
        let written = Self::write_batch(
            inner,
            records.to_vec(),
            append::WriteIf::Continuing {
                producer_id,
                sequence,
            },
            Some(order),
            WrittenBy::Leader,
        )
        .await?;
        Ok(written.ok().map(append::Written::claimed))
    }

    /// Wait until every record below `offset` satisfies the configured fsync
    /// policy. For a batch this log already held when asked about it, whose
    /// writer may not have waited.
    pub async fn wait_durable(&self, offset: Offset) -> Result<()> {
        if self.inner.durability.acknowledges_before_sync() {
            return Ok(());
        }
        self.inner.ensure_durable(offset).await?;
        self.inner.check_healthy()
    }

    /// Wait until a [`PendingAppend`] satisfies the configured fsync policy.
    /// A batch holding a trim record is synced under every policy.
    pub async fn commit(&self, pending: &PendingAppend) -> Result<()> {
        if self.inner.durability.acknowledges_before_sync() && !pending.holds_trim {
            return Ok(());
        }
        self.inner.ensure_durable(pending.durable_target).await?;
        // `ensure_durable` returns immediately when the target is already
        // covered, which skips the check inside `flush`. A rollover that failed
        // since then still has to be reported rather than acknowledged.
        self.inner.check_healthy()
    }

    /// The tail once every append already handed to the append thread has
    /// been written or skipped.
    ///
    /// An append whose caller was cancelled still lands if the thread had
    /// started on it, so a plain tail read taken after the cancellation can
    /// name a position below that record. This read queues behind it instead.
    pub async fn settled_tail_offset(&self) -> Result<Offset> {
        let inner = Arc::clone(&self.inner);
        self.inner
            .appender
            .run(move || Ok(inner.segments.read().tail_offset()))
            .await
            .map_err(StorageError::Io)
    }

    /// Force a flush regardless of the configured policy.
    pub async fn sync(&self) -> Result<()> {
        self.inner
            .durability
            .force_flush(|| Arc::clone(&self.inner).flush())
            .await?;
        self.inner.check_healthy()
    }

    /// Run one retention pass now, instead of waiting for the timer.
    ///
    /// Exists so retention is testable deterministically and so an operator can
    /// reclaim space without waiting out `retention_check_interval`. Returns
    /// what the pass reclaimed; a no-op when no bound is configured.
    pub async fn enforce_retention_now(&self) -> Result<segments::RetentionOutcome> {
        Arc::clone(&self.inner).sweep_retention().await
    }

    /// Enforce `retention` from the next pass on, in place of the bounds the
    /// log opened with. Starts the retention timer if it was not running,
    /// which needs a Tokio runtime.
    pub fn set_retention(&self, retention: crate::log::Retention) -> Result<()> {
        self.inner.segments.read().check_open()?;
        self.inner.config.check_retention(retention)?;
        *self.inner.retention_bounds.lock() = retention;
        if retention.is_set() {
            self.inner.start_retention()?;
        }
        Ok(())
    }

    /// The bounds retention enforces now.
    pub fn retention(&self) -> crate::log::Retention {
        *self.inner.retention_bounds.lock()
    }

    /// Seal the active segment and start a new one, returning the new
    /// segment's base. Everything below it is then in sealed segments, which
    /// is what lets compaction trim them whole.
    pub(crate) async fn roll_now(&self) -> Result<Offset> {
        Arc::clone(&self.inner).roll_now().await
    }

    /// Delete the sealed head segments holding only offsets below `before`,
    /// returning what was removed. The active segment is never removed.
    pub(crate) async fn trim_before(&self, before: Offset) -> Result<Vec<SegmentDescriptor>> {
        Arc::clone(&self.inner).trim_head_before(before).await
    }

    /// Discard every record and start again, empty, at `base_offset`.
    ///
    /// The one caller is a follower rebuilding a shard whose copy has
    /// diverged -- see `docs/replication-design.md`. The log stays open and
    /// every handle to it stays valid; a read in flight fails rather than
    /// returning records that no longer exist. The generation history goes
    /// with the records it described.
    ///
    /// Refused with [`StorageError::BelowCommit`] when it would discard a
    /// record this log holds below its commit offset and at or above
    /// `base_offset`: those were acknowledged on a majority, and nothing here
    /// can tell whether the leader asking still has them. Records below
    /// `base_offset` are gone from the leader too, so dropping them is not a
    /// loss this log can prevent.
    pub async fn reset_to(&self, base_offset: Offset) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        let fence = inner.fence_rolls().await?;
        let _flush_guard = inner.durability.lock_flushes().await;
        let operation = Arc::clone(&inner);
        felix_common::blocking::spawn(move || {
            let _fence = fence;
            let mut manifest = operation.manifest.lock();
            let _appends = operation.append_lock.lock();
            let mut segments = operation.segments.write();
            let commit = operation.commit_offset.load(Ordering::Acquire);
            let discarded_from = segments.base_offset().max(base_offset);
            if discarded_from < commit.min(segments.tail_offset()) {
                return Err(StorageError::BelowCommit {
                    offset: discarded_from,
                    commit,
                });
            }
            segments.check_open()?;
            operation.forget_all_offloaded(&mut manifest)?;
            let rewound = (|| {
                segments.reset_to(base_offset)?;
                segments.active_mut().sync()?;
                operation.note_rewound(&segments)?;
                operation.durability.reset_after_truncate(base_offset);
                let mut epochs = operation.epochs.lock();
                *epochs = epochs::EpochMap::default();
                epochs::store(&operation.dir, &epochs)?;
                operation.reset_producers(&segments)
            })();
            operation.poison_after_rewind(rewound)
        })
        .await
        .map_err(|err| StorageError::Io(std::io::Error::other(err)))?
    }

    /// Cut the log back to `offset` to put a copy of it back as it stood at
    /// a backup point. Offline only: run against a copy, with no broker
    /// serving it.
    ///
    /// Unlike truncation this may cut below the commit offset. A restore goes
    /// back in time on purpose, so the commit offset is lowered to `offset`
    /// and made durable first, and then the suffix goes through the same path
    /// truncation takes. Refused with [`StorageError::OutsideLog`] when the
    /// log ends before `offset` (the copy is incomplete) or begins after it
    /// (retention or compaction dropped records the point still had). Running
    /// it again is a no-op.
    pub async fn restore_to(&self, offset: Offset) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        let fence = inner.fence_rolls().await?;
        let _flush_guard = inner.durability.lock_flushes().await;
        let operation = Arc::clone(&inner);
        felix_common::blocking::spawn(move || {
            let _fence = fence;
            let mut manifest = operation.manifest.lock();
            let _appends = operation.append_lock.lock();
            let mut segments = operation.segments.write();
            segments.check_open()?;
            let (base, tail) = (segments.base_offset(), segments.tail_offset());
            if offset > tail || offset < base {
                return Err(StorageError::OutsideLog { offset, base, tail });
            }
            // Lowered on disk before anything is cut, so a restore interrupted
            // between the two can simply be run again.
            {
                let mut persisted = operation.replica_persisted.lock();
                if operation.commit_offset.load(Ordering::Acquire) > offset {
                    // The generation saved named the record below the old
                    // offset. A Raft log names the one below the new offset.
                    let mut committed = operation.committed_generation.lock();
                    #[cfg(feature = "raft")]
                    let below = match offset.checked_sub(1) {
                        Some(last) if operation.config.raft_log && last >= base => segments
                            .read_plan(last)
                            .generation_at(last, &operation.label)?
                            .flatten()
                            .unwrap_or(0),
                        _ => 0,
                    };
                    #[cfg(not(feature = "raft"))]
                    let below = 0;
                    let state = replica_state::ReplicaState {
                        commit_offset: offset,
                        committed_generation: below,
                        ..operation.replica_state_with(*committed)
                    };
                    operation.poison_on_sync_failure(
                        "the replica state",
                        replica_state::store(&operation.dir, &state),
                    )?;
                    *committed = below;
                    operation.commit_offset.store(offset, Ordering::Release);
                    operation.commit_persisted.store(offset, Ordering::Release);
                    *persisted = (state, Some(tokio::time::Instant::now()));
                }
            }
            if offset == tail {
                return Ok(());
            }
            operation.forget_offloaded_from(&mut manifest, offset)?;
            let rewound = operation.cut_suffix(&mut segments, offset);
            operation.poison_after_rewind(rewound)
        })
        .await
        .map_err(|err| StorageError::Io(std::io::Error::other(err)))?
    }

    /// Label the records from `from` on with the generations that wrote them,
    /// as the log they were copied from has them.
    ///
    /// Generations this history held from `from` on go first: they describe
    /// no record here, since those records have just arrived, and one left
    /// in place would label them. Each of `generations` then begins no
    /// earlier than `from`; one already covered by a newer entry is skipped,
    /// as [`AppendOnlyLog::record_generation`] skips it.
    pub fn label_generations(&self, from: Offset, generations: &[Epoch]) -> Result<()> {
        let segments = self.inner.segments.read();
        segments.check_open()?;
        let mut epochs = self.inner.epochs.lock();
        let before = epochs.entries().to_vec();
        epochs.truncate_from(from);
        for epoch in generations {
            epochs.record(epoch.generation, epoch.start_offset.max(from));
        }
        if epochs.entries() != before.as_slice() {
            self.inner.check_not_poisoned()?;
            self.inner.poison_on_sync_failure(
                "the generation history",
                epochs::store(&self.inner.dir, &epochs),
            )?;
        }
        Ok(())
    }

    /// Whether the generation history labels every record in `first..=last`.
    ///
    /// False for any of them that arrived after a cut with no label, such as
    /// records a promotion took from a replica that sent none: the entry
    /// before the cut would otherwise claim them. Since this log was opened.
    pub fn labels_known(&self, first: Offset, last: Offset) -> bool {
        self.inner.epochs.lock().labelled(first, last)
    }

    /// Whether [`DiskLog::label_generations`] with these arguments would
    /// change the stored history, and so write and fsync it.
    pub fn relabels(&self, from: Offset, generations: &[Epoch]) -> bool {
        let epochs = self.inner.epochs.lock();
        let mut labelled = epochs.clone();
        labelled.truncate_from(from);
        for epoch in generations {
            labelled.record(epoch.generation, epoch.start_offset.max(from));
        }
        labelled.entries() != epochs.entries()
    }

    /// Whether `AppendOnlyLog::record_generation` with these arguments would
    /// change the stored history, and so write and fsync it.
    pub fn records_generation(&self, generation: u64, start_offset: Offset) -> bool {
        self.inner
            .epochs
            .lock()
            .clone()
            .record(generation, start_offset)
    }

    /// Note that this broker leads the log at `generation` from now on. A v9
    /// segment stamps the leader's own appends with it, and refuses them once
    /// the log accepts a newer generation: that belongs to another leader,
    /// even after this log records where it begins (a deposed leader's
    /// history takes the new leader's labels). Only rises; kept in memory,
    /// since a broker opens a shard to lead it again after a restart.
    pub fn lead_at(&self, generation: u64) {
        self.inner
            .led_generation
            .fetch_max(generation.saturating_add(1), Ordering::AcqRel);
    }

    /// Whether this log stamps records with the generation that wrote them,
    /// or may soon: its active segment is v9, or its gate has said yes or
    /// would now. A sender ships the writers of a batch only then, so a
    /// cluster that has not finalized `record_generations` sends what it
    /// always did.
    pub fn stamps_generations(&self) -> bool {
        self.inner.config.generations.heard()
            || self.inner.segments.read().active().holds_generations()
    }

    /// Note that the leader wrote `generation`'s start record at `offset`.
    pub fn note_stint_start(&self, generation: u64, offset: Offset) {
        *self.inner.stint.lock() = Some(Epoch {
            generation,
            start_offset: offset,
        });
    }

    /// Where the last start record noted for `generation` sits, if one was
    /// noted since the log was opened.
    ///
    /// A leader that opens again at a generation it already started writes
    /// another start record, so the history's start for the generation can
    /// be an earlier stint's. Kept in memory only: after a restart the next
    /// open that writes one notes it again.
    pub fn stint_start(&self, generation: u64) -> Option<Offset> {
        self.inner
            .stint
            .lock()
            .filter(|stint| stint.generation == generation)
            .map(|stint| stint.start_offset)
    }

    /// One past the last record known committed. Zero if none is known.
    pub fn commit_offset(&self) -> Offset {
        self.inner.commit_offset.load(Ordering::Acquire)
    }

    /// The commit offset the replica state on disk holds: what a restart
    /// reads back. At or below [`Self::commit_offset`], which is saved
    /// behind the caller.
    pub fn saved_commit_offset(&self) -> Offset {
        // An open can lower the offset in memory below the saved one, to the
        // log's end.
        self.inner
            .commit_persisted
            .load(Ordering::Acquire)
            .min(self.commit_offset())
    }

    /// Record that every record below `offset` is committed.
    ///
    /// Takes effect at once for [`AppendOnlyLog::truncate`] and
    /// [`DiskLog::reset_to`], which refuse to cut below it, and for retention.
    /// It is written to disk behind the call, so the replication path never
    /// waits on it: under [`FsyncMode::OnCommit`] a write starts at once and
    /// one write in flight serves every advance made while it runs; otherwise
    /// it is written at most once per [`COMMIT_PERSIST_INTERVAL`]. Always with
    /// a raised generation and at shutdown. A write that fails is retried every
    /// [`COMMIT_PERSIST_INTERVAL`] until it lands, whether or not anything
    /// calls this again.
    ///
    /// After a crash the offset read back may therefore be behind, never
    /// ahead. That weakens only the guard: truncation may cut into records
    /// committed since, until the leader's next batch carries the offset
    /// again. See `docs/replication-design.md`, "Divergence and truncation".
    ///
    /// Fails only on a closed log, or, when not under `OnCommit`, when a write
    /// due now fails.
    ///
    /// Refused on a Raft log, whose commit offset moves only with the term
    /// of the entry it names (the adapter's `save_committed`).
    pub async fn advance_commit_offset(&self, offset: Offset) -> Result<()> {
        if self.inner.config.raft_log {
            return Err(StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "{}: a Raft log's commit offset is saved with its entry's term",
                    self.inner.label
                ),
            )));
        }
        self.raise_commit_offset(offset).await
    }

    async fn raise_commit_offset(&self, offset: Offset) -> Result<()> {
        let previous = self.inner.commit_offset.fetch_max(offset, Ordering::AcqRel);
        // Not `offset <= previous`: a write that failed left memory ahead of
        // disk, and the next call with the same offset has to write again.
        if offset.max(previous) <= self.inner.commit_persisted.load(Ordering::Acquire) {
            return Ok(());
        }
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(StorageError::Closed(self.inner.label.clone()));
        }
        if matches!(self.inner.config.fsync_mode, FsyncMode::OnCommit) {
            write_commit_behind(&self.inner);
            return Ok(());
        }
        let due = {
            let persisted = self.inner.replica_persisted.lock();
            persisted
                .1
                .is_none_or(|at| at.elapsed() >= COMMIT_PERSIST_INTERVAL)
        };
        if due && let Err(err) = self.persist_replica_state().await {
            retry_replica_state(&self.inner);
            return Err(err);
        }
        Ok(())
    }

    /// Raise the commit offset in memory only, so a trim's floor below it
    /// applies at once. A later [`Self::advance_commit_offset`] writes it.
    ///
    /// For a leader about to open reads up to `offset`: a reader let in
    /// before the floor applies is served below an acknowledged trim.
    ///
    /// Does nothing on a Raft log, as [`Self::advance_commit_offset`].
    pub fn note_commit_offset(&self, offset: Offset) {
        if !self.inner.config.raft_log {
            self.inner.commit_offset.fetch_max(offset, Ordering::AcqRel);
        }
    }

    /// Keep retention and compaction's head trims below the commit offset.
    ///
    /// For a log replicated under `Quorum`, where a record above the commit
    /// offset may still be waiting for a majority. Deleting it there would let
    /// a follower rebuilt at the new base be counted as holding it. Off for a
    /// new log, since a log nobody advances the commit offset on would never
    /// trim. [`LogConfig::retention_hold`] sets it at open.
    ///
    /// Takes effect in memory at once, from the next pass. A change is then
    /// written to the replica state, and the log reopens with it, so a
    /// restarted log is held before replication has touched it. If that write
    /// fails the error is returned and the hold stays in effect in memory.
    pub async fn hold_retention_at_commit(&self, hold: bool) -> Result<()> {
        // A closed log's directory may already be another log's.
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(StorageError::Closed(self.inner.label.clone()));
        }
        if self.inner.hold_at_commit.swap(hold, Ordering::AcqRel) == hold {
            return Ok(());
        }
        // A floor applied before the hold is withdrawn, not kept.
        {
            let mut head = self.inner.head.lock();
            self.inner.apply_head(&mut head);
        }
        self.persist_replica_state().await
    }

    /// Whether retention is held at the commit offset. See
    /// [`Self::hold_retention_at_commit`].
    pub fn retention_held_at_commit(&self) -> bool {
        self.inner.hold_at_commit.load(Ordering::Acquire)
    }

    /// Write the replica state if memory is ahead of disk.
    async fn persist_replica_state(&self) -> Result<()> {
        persist_replica_state(Arc::clone(&self.inner)).await
    }

    /// Stop background work, flush everything one last time, and refuse any
    /// further change to the files, from this handle or any clone of it.
    ///
    /// For a shard this broker no longer holds: once this returns, another
    /// `DiskLog` may be opened over the same directory, and a Raft log has
    /// let go of its lock on it. Reads, appends and
    /// flushes through an old handle fail with [`StorageError::Closed`]. The
    /// sealed segments' files are released at once; the active segment's
    /// when the last handle is dropped.
    pub async fn close(&self) -> Result<()> {
        if self.inner.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(());
        }
        // First, so nothing written from here on can race the flush below or
        // a log opened after this returns. Every file change checks it under
        // the lock taken here.
        let inner = Arc::clone(&self.inner);
        felix_common::blocking::spawn(move || {
            let _appends = inner.append_lock.lock();
            inner.segments.write().close();
        })
        .await
        .map_err(|err| StorageError::Io(std::io::Error::other(err)))?;
        let stopped = self.stop_background().await;
        let flushed = match stopped {
            Ok(()) => match self.persist_replica_state().await {
                Ok(()) => self.sync().await,
                Err(err) => Err(err),
            },
            Err(err) => Err(err),
        };
        let marked = self.inner.mark.sync().map_err(StorageError::Io);
        // Last, and under the flush lock, so no flush through a stale handle
        // can move the mark after the one above.
        let _flushes = self.inner.durability.lock_flushes().await;
        // And under the replica-state writer's lock, so a write behind that
        // found the log open lands before this returns.
        let inner = Arc::clone(&self.inner);
        felix_common::blocking::spawn(move || {
            let _persisted = inner.replica_persisted.lock();
            inner
                .closed
                .store(true, std::sync::atomic::Ordering::Release);
            // Only now: nothing can write through this log any more.
            #[cfg(feature = "raft")]
            inner.dir_lock.lock().take();
        })
        .await
        .map_err(|err| StorageError::Io(std::io::Error::other(err)))?;
        flushed.and(marked)
    }

    /// Run `write`, a change to a file in this log's directory, unless the
    /// log is closed. A close cannot start between the check and the write.
    #[cfg(feature = "raft")]
    pub(crate) fn write_while_open<T>(
        &self,
        write: impl FnOnce() -> std::io::Result<T>,
    ) -> Result<T> {
        let segments = self.inner.segments.read();
        segments.check_open()?;
        write().map_err(StorageError::Io)
    }

    /// Whether [`Self::close`] has finished on this log or any clone of it.
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Stop background work and flush everything one last time.
    ///
    /// Call before dropping the process's last handle: without it, `Periodic`
    /// mode can lose up to one interval of writes that a clean stop could have
    /// kept.
    pub async fn shutdown(&self) -> Result<()> {
        self.stop_background().await?;
        self.persist_replica_state().await?;
        self.sync().await?;
        self.inner.mark.sync()?;
        Ok(())
    }

    /// Stop the timers and wait out a rollover in flight.
    async fn stop_background(&self) -> Result<()> {
        // Retention first: it must not start deleting while the rest of
        // shutdown is flushing, and it has nothing to finish on the way out.
        if let Some(offloader) = &self.inner.offloader {
            offloader.halt();
        }
        let retention = self.inner.retention.lock().take();
        if let Some(retention) = retention {
            retention.shutdown().await;
        }
        let syncer = self.inner.syncer.lock().take();
        if let Some(syncer) = syncer {
            syncer.shutdown().await;
        }
        // A rollover in flight owns the retired segment and is the only thing
        // that will ever flush it. Dropping the runtime here would abandon it
        // half-installed, so wait it out first — it is bounded by two fsyncs.
        let roll = self.inner.roll_task.lock().take();
        if let Some(roll) = roll {
            // A panicked rollover has already recorded itself as `Failed`; the
            // `sync` below is what reports the problem.
            let _ = roll.await;
        }
        self.inner.check_healthy()
    }

    /// Open a log whose sealed segments' files are cached in `files`, shared
    /// with the other logs under one root.
    pub(crate) fn open_shared(
        dir: PathBuf,
        label: String,
        config: LogConfig,
        base_offset: Option<Offset>,
        files: Arc<SealedFiles>,
        #[cfg_attr(not(feature = "raft"), allow(unused_variables))] other_commit: OtherCommit,
    ) -> Result<Self> {
        config.validate()?;
        // Before recovery, which would repair a tail that another open log
        // may still be writing.
        #[cfg(feature = "raft")]
        let dir_lock = if config.raft_log {
            crate::io::create_dir_all_durable(&dir)?;
            Some(dir_lock::DirLock::take(&dir)?)
        } else {
            None
        };

        if let Some(base_offset) = base_offset.filter(|base| *base > 0) {
            recovery::place_empty_shard(&dir, &label, &config, base_offset)?;
        }
        // Before recovery, which needs it to tell a segment that was
        // offloaded from one that was lost.
        let manifest = offload::manifest::load(&dir)?;
        // Before the segments are looked at, so recovery sees the cut log.
        if let Some(marker) = durable_mark::load_poison(&dir) {
            recovery::after_poison(&dir, &label, marker)?;
        }
        let recovered = recovery::recover_shard(&dir, &label, &config, &manifest)?;
        // Does not touch the archive: an unreachable one must not stop the
        // log from serving. The first pass opens it.
        let offloader = config
            .offload
            .as_ref()
            .map(|target| offload::Offloader::open(target, &dir))
            .transpose()?;
        if recovered.truncated_bytes > 0 {
            tracing::warn!(
                shard = %label,
                truncated_bytes = recovered.truncated_bytes,
                "recovered log after an unclean shutdown"
            );
        }
        // Recovery synced the active segment to exactly where it resumes, so
        // the mark starts there. That also retires a mark naming a segment or
        // a length recovery removed, which would make the next recovery refuse
        // damage in bytes nobody synced.
        let mark = durable_mark::MarkFile::open(&dir)?;
        mark.record_durably(durable_mark::DurableMark {
            segment: recovered.active.id(),
            synced_bytes: recovered.active.size_bytes(),
        })?;
        // A freshly created mark is lost with its directory entry, and with
        // it the repair rule for the whole first segment.
        crate::io::sync_dir(&dir).map_err(StorageError::Io)?;
        // Read before the directory is handed to the segment set.
        let epochs = epochs::load(&dir);
        let saved = replica_state::load(&dir)?;
        let mut replica = saved;
        // Before retention starts below, so its first sweep sees the hold.
        let hold = match config.retention_hold {
            crate::log::RetentionHold::AsSaved => replica.hold_at_commit,
            crate::log::RetentionHold::Held => true,
            crate::log::RetentionHold::Lifted => false,
        };
        // openraft may save a committed id whose entry is not flushed yet,
        // so a crash can leave the offset past the log. Lowered on disk
        // before anything is appended: what is written at those indexes
        // later was never committed.
        let tail = recovered.active.next_offset();
        let past_the_log = config.raft_log && replica.commit_offset > tail;
        if past_the_log {
            replica.commit_offset = tail;
        }
        replica.hold_at_commit = hold;
        let (kept_set, accepted) =
            set::reconcile(&dir, &label, ballot::reconcile(&dir, &replica)?)?;
        let (replica_set, wiped) = kept_set.map_or((None, None), |(set, wiped)| (Some(set), wiped));
        let set_generation = replica_set.as_ref().map_or(0, |set| set.current.generation);
        let epochs_dir = dir.clone();
        let segments = SegmentSet::new(
            dir,
            label.clone(),
            config.clone(),
            recovered.sealed,
            recovered.active,
            files,
        )?;
        #[cfg(feature = "raft")]
        if config.raft_log {
            let found = committed_generation_here(&segments, &replica, past_the_log, &label)?;
            if found.is_none()
                && other_commit == OtherCommit::Refuse
                && another_entry_is_there(&segments, &replica, past_the_log, &label)?
            {
                // Before the replica state is rewritten below, so the next
                // open finds the same thing.
                return Err(StorageError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "{label}: index {} was saved as committed at generation {}, but the \
                         entry there is another one; this log is not the one that id was saved for",
                        replica.commit_offset.saturating_sub(1),
                        replica.committed_generation
                    ),
                )));
            }
            if found.is_none() {
                tracing::warn!(
                    shard = %label,
                    commit_offset = replica.commit_offset,
                    committed_generation = replica.committed_generation,
                    "the entry below the saved commit offset is not the one that was committed; \
                     nothing in this log is taken as committed"
                );
                replica.commit_offset = segments.base_offset();
            }
            replica.committed_generation = found.unwrap_or(0);
        }
        // One write: a lowered offset beside the generation saved for the
        // higher one would read at the next open as another entry's id.
        if replica != saved {
            replica_state::store(&epochs_dir, &replica)?;
        }
        // Before the log is usable, so no append can be accepted against a
        // producer state that does not yet include what is already on disk.
        let producer_state =
            producers::rebuild(&epochs_dir, &segments, Some(&recovered.active_marks))?;
        let active_has_trims = recovered
            .active_marks
            .iter()
            .any(|(_, mark, _)| mark.is_trim());
        let (head, read_sealed) = head::rebuild(&epochs_dir, &segments, active_has_trims)?;
        if read_sealed {
            // So the next open need not read those segments again.
            let as_of = segments.active().base_offset();
            let below: Vec<_> = head
                .entries()
                .iter()
                .filter(|entry| entry.offset < as_of)
                .cloned()
                .collect();
            if let Err(err) = head::store(&epochs_dir, as_of, &below) {
                tracing::warn!(shard = %label, error = %err, "could not save the head file");
            }
        }
        let holds_trims = !head.entries().is_empty();

        // Everything that survived recovery is on disk, so the durable bound
        // starts at the recovered tail.
        let durable_upto = segments.tail_offset();
        let inner = Arc::new(LogInner {
            label,
            keep_alive: Mutex::new(None),
            #[cfg(feature = "raft")]
            dir_lock: Mutex::new(dir_lock),
            config: config.clone(),
            segments: RwLock::new(segments),
            append_lock: Mutex::new(()),
            records_written: AtomicU64::new(0),
            records_flushed: AtomicU64::new(0),
            durability: Durability::new(config.fsync_mode, durable_upto),
            syncer: Mutex::new(None),
            retention: Mutex::new(None),
            retention_bounds: Mutex::new(config.retention()),
            manifest: Mutex::new(manifest),
            offloader,
            epochs: Mutex::new(epochs),
            stint: Mutex::new(None),
            accepted_generation: AtomicU64::new(accepted.generation),
            led_generation: AtomicU64::new(0),
            ballot: RwLock::new(accepted),
            set_generation: AtomicU64::new(set_generation),
            replica_set: RwLock::new(replica_set),
            wiped: RwLock::new(wiped),
            commit_offset: AtomicU64::new(replica.commit_offset),
            commit_persisted: AtomicU64::new(replica.commit_offset),
            committed_generation: Mutex::new(replica.committed_generation),
            // Set before retention starts below, so a sweep after a restart
            // is held before any replication pass or batch has run.
            hold_at_commit: std::sync::atomic::AtomicBool::new(replica.hold_at_commit),
            replica_retry: std::sync::atomic::AtomicBool::new(false),
            commit_writer: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            replica_writes: AtomicU64::new(0),
            replica_persisted: Mutex::new((replica, None)),
            batch_open: std::sync::atomic::AtomicBool::new(producer_state.is_open()),
            producers: Mutex::new(producer_state),
            head: Mutex::new(head),
            head_pending: std::sync::atomic::AtomicBool::new(true),
            head_floor: AtomicU64::new(0),
            dir: epochs_dir,
            roll_state: AtomicU8::new(RollState::Idle as u8),
            failure: Mutex::new(None),
            closed: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            fail_seal: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            hold_next_seal: Mutex::new(None),
            #[cfg(test)]
            hold_next_extension: Mutex::new(None),
            #[cfg(test)]
            fail_extensions: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            extensions_done: AtomicU64::new(0),
            #[cfg(test)]
            fail_next_flush: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            fail_next_rewind_sync: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            pause_next_read: Mutex::new(None),
            #[cfg(test)]
            slow_inline_roll_millis: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            flushes: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            inline_roll_active: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            hold_next_append: Mutex::new(None),
            #[cfg(test)]
            append_held: std::sync::atomic::AtomicBool::new(false),
            roll_task: Mutex::new(None),
            pending_seal: Mutex::new(None),
            mark,
            appender: LogThread::spinning("felix-append", APPEND_SPIN),
            flusher: LogThread::new("felix-flush"),
        });

        if let FsyncMode::Periodic { interval } = config.fsync_mode {
            let weak = Arc::downgrade(&inner);
            let syncer = PeriodicSyncer::spawn(interval, move || {
                let weak = weak.clone();
                async move {
                    match weak.upgrade() {
                        // The log is gone; report the highest offset so the
                        // task simply stops doing work.
                        None => Ok(Offset::MAX),
                        // Every open log ticks, and most are idle; an fsync
                        // of a clean file is still a syscall and a flush-thread
                        // round trip.
                        Some(inner) if inner.fully_durable() => Ok(inner.durability.durable_upto()),
                        // Already logged once when it was poisoned; appends
                        // and commits are what report it from here on.
                        Some(inner) if inner.check_healthy().is_err() => {
                            Ok(inner.durability.durable_upto())
                        }
                        Some(inner) => inner.durability.force_flush(|| inner.clone().flush()).await,
                    }
                }
            })?;
            *inner.syncer.lock() = Some(syncer);
        }

        // The floor applies at once to what recovery kept, below the bound.
        inner.head_floor();
        let deletes_below_floor = holds_trims
            && !config.retention_check_interval.is_zero()
            && tokio::runtime::Handle::try_current().is_ok();
        if config.retention().is_set() || inner.offloader.is_some() || deletes_below_floor {
            inner.start_retention()?;
        }

        Ok(Self { inner })
    }
}

impl DiskLog {
    /// A count that changes whenever records are removed from either end of
    /// the log or the log is reset. A reader that pages through the log compares
    /// it before and after, since a cut in between can leave it holding
    /// records from both sides of the cut.
    #[cfg(feature = "raft")]
    pub(crate) fn cuts(&self) -> u64 {
        self.inner.segments.read().generation()
    }

    /// The tail as it is now, without a future to poll.
    #[cfg(feature = "raft")]
    pub(crate) fn tail_now(&self) -> Offset {
        self.inner.segments.read().tail_offset()
    }

    /// What [`Self::commit`] answers when it has nothing to wait for, and
    /// `None` when a flush must cover the batch first.
    #[cfg(feature = "raft")]
    pub(crate) fn commit_now(&self, pending: &PendingAppend) -> Option<Result<()>> {
        let durability = &self.inner.durability;
        if durability.acknowledges_before_sync() && !pending.holds_trim {
            return Some(Ok(()));
        }
        (durability.durable_upto() >= pending.durable_target).then(|| self.inner.check_healthy())
    }

    /// The read behind both `read_range` methods, and the tail it ran to.
    /// Without trims a start below the head floor is refused; with them only
    /// one below the oldest segment is, since replication ships from there.
    fn read_records(
        &self,
        range: ReadRange,
        trims: TrimRecords,
    ) -> BoxFuture<'_, Result<(Vec<LogRecord>, Offset)>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            if inner.closed.load(std::sync::atomic::Ordering::Acquire) {
                return Err(StorageError::Closed(inner.label.clone()));
            }
            let started = std::time::Instant::now();
            let (records, tail) = felix_common::blocking::spawn(move || {
                loop {
                    // Planned under the lock, read without it: a cold `pread`
                    // must not hold up the appends queued on `segments`.
                    let (plan, tail) = {
                        let segments = inner.segments.read();
                        let base = segments.base_offset();
                        let oldest = match trims {
                            TrimRecords::Skip => base.max(inner.head_floor()),
                            TrimRecords::Include | TrimRecords::Only => base,
                        };
                        if range.start < oldest {
                            // Not an empty range: these offsets existed and
                            // are gone, or a trim hides them.
                            return Err(inner.trimmed(range.start, base, trims));
                        }
                        (segments.read_plan(range.start), segments.tail_offset())
                    };
                    #[cfg(test)]
                    if let Some(pause) = inner.pause_next_read.lock().take() {
                        pause.wait();
                        pause.wait();
                    }
                    let mut budget =
                        ReadBudget::new(range.max_bytes, inner.config.max_records_per_read)
                            .with_trims(trims);
                    let read = plan.execute(range.start, &mut budget, &inner.label);
                    // A truncation in between may have put other records at
                    // the positions just read. Rare, so read again.
                    if inner.segments.read().generation() == plan.generation {
                        return read.map(|records| (records, tail));
                    }
                }
            })
            .await
            .map_err(|err| StorageError::Io(std::io::Error::other(err)))??;

            metrics::counter!(metrics_names::READ_RECORDS_TOTAL).increment(records.len() as u64);
            metrics::counter!(metrics_names::READ_BYTES_TOTAL).increment(
                records
                    .iter()
                    .map(|r| {
                        let block = r.headers.as_ref().map_or(0, |block| block.len());
                        (r.payload.len() + block) as u64
                    })
                    .sum::<u64>(),
            );
            metrics::histogram!(metrics_names::READ_DURATION_SECONDS)
                .record(started.elapsed().as_secs_f64());
            Ok((records, tail))
        })
    }
}

/// A stalled device, for tests here and in other crates. Not in a plain
/// release build, like [`crate::fault`].
#[cfg(any(debug_assertions, test, feature = "fault-injection"))]
impl DiskLog {
    /// Hold every flush of this log until the guard is dropped. Writes
    /// still land.
    #[doc(hidden)]
    pub async fn hold_flushes(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.inner.durability.lock_flushes().await
    }
}

/// Hooks for tests outside this module.
#[cfg(test)]
impl DiskLog {
    /// Fail the next flush, as a failed fsync would.
    pub(crate) fn fail_next_flush(&self) {
        self.inner.fail_next_flush.store(true, Ordering::Release);
    }

    /// Wait until the commit offset in memory is the one on disk.
    pub(crate) async fn commit_offset_persisted(&self) {
        while self.inner.commit_persisted.load(Ordering::Acquire) < self.commit_offset() {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }

    /// Move the commit offset and leave the generation saved with it, which
    /// nothing outside a test can do to a Raft log.
    pub(crate) fn move_commit_offset_for_tests(&self, offset: Offset) {
        self.inner.commit_offset.store(offset, Ordering::Release);
    }

    /// Stop the next truncation or reset after `unlinks` unlinks.
    pub(crate) fn stop_after_unlinks(&self, unlinks: usize) {
        segments::stop_after_unlinks(&self.inner.dir, unlinks);
    }
}

/// What an open does with a Raft log whose saved committed id names an
/// index that holds some other entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OtherCommit {
    /// Nothing in the log is taken as committed.
    Drop,
    /// The open fails, with the saved id left as it was.
    #[cfg(feature = "raft")]
    Refuse,
}

/// The generation of the record below a Raft log's saved commit offset, or
/// `None` when that record is some other entry than the one that was
/// committed: its generation is not the one saved with the offset.
///
/// When the open lowered the offset to the log's end (`lowered`), the record
/// there is an earlier entry of the committed one's log, so its generation
/// may be lower and never higher. An offset at or below the base names no
/// record this log holds, and what was saved stands.
#[cfg(feature = "raft")]
fn committed_generation_here(
    segments: &SegmentSet,
    replica: &replica_state::ReplicaState,
    lowered: bool,
    label: &str,
) -> Result<Option<u64>> {
    let saved = replica.committed_generation;
    let Some(last) = replica.commit_offset.checked_sub(1) else {
        return Ok(Some(saved));
    };
    if last < segments.base_offset() {
        return Ok(Some(saved));
    }
    let Some(Some(found)) = segments.read_plan(last).generation_at(last, label)? else {
        return Ok(None);
    };
    let same_entry = found == saved || (lowered && found < saved);
    Ok((saved != 0 && same_entry).then_some(found))
}

/// Whether the record below the saved commit offset is there, with a known
/// generation that is not the saved one's. A record that cannot be read
/// that way is not proof of another log's entry.
#[cfg(feature = "raft")]
fn another_entry_is_there(
    segments: &SegmentSet,
    replica: &replica_state::ReplicaState,
    lowered: bool,
    label: &str,
) -> Result<bool> {
    let saved = replica.committed_generation;
    let Some(last) = replica.commit_offset.checked_sub(1) else {
        return Ok(false);
    };
    if saved == 0 || last < segments.base_offset() {
        return Ok(false);
    }
    Ok(match segments.read_plan(last).generation_at(last, label)? {
        Some(Some(found)) if found != crate::log::UNKNOWN_WRITER => {
            found > saved || (!lowered && found != saved)
        }
        _ => false,
    })
}

#[cfg(feature = "raft")]
impl DiskLog {
    /// Record that the entry at `index`, written by `generation`, and every
    /// entry below it are committed. [`Self::advance_commit_offset`] with
    /// the generation kept beside the offset, so an open can tell that
    /// entry from another at the same index.
    pub(crate) async fn save_committed(&self, index: Offset, generation: u64) -> Result<()> {
        let offset = index + 1;
        {
            let mut committed = self.inner.committed_generation.lock();
            if offset >= self.inner.commit_offset.fetch_max(offset, Ordering::AcqRel) {
                *committed = generation;
            }
        }
        self.raise_commit_offset(offset).await
    }

    /// The directory that holds the log's files.
    pub fn dir(&self) -> &std::path::Path {
        &self.inner.dir
    }

    /// The commit offset and the generation saved with it, zero for none.
    pub(crate) fn committed(&self) -> (Offset, u64) {
        let committed = self.inner.committed_generation.lock();
        (self.commit_offset(), *committed)
    }
}

/// What the log holds at one offset, as its record header says.
#[cfg(feature = "raft")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WriterAt {
    /// No record: the offset is below the oldest segment, or at or past the
    /// tail.
    Absent,
    /// A record in a segment older than format v9, which keeps no generation.
    Unstamped,
    /// The generation stamped on the record. [`crate::log::UNKNOWN_WRITER`]
    /// means the writer is not known, never that generation 0 wrote it.
    Generation(u64),
}

#[cfg(feature = "raft")]
impl DiskLog {
    /// Which generation wrote the record at `offset`, read from the record's
    /// own header and checked against the header checksum. The payload is
    /// not read.
    ///
    /// This is the log's answer, not the generation history's
    /// ([`AppendOnlyLog::generations`]), which is a hint that can be missing
    /// or short. A Raft term must come from here.
    pub(crate) async fn writer_at(&self, offset: Offset) -> Result<WriterAt> {
        Ok(self.writers_at(Some(offset)).await?.1)
    }

    /// The offset of the last record and which generation wrote it, or `None`
    /// for a log that holds no record. One answer about one tail: a
    /// truncation in between restarts the read.
    pub(crate) async fn last_writer(&self) -> Result<Option<(Offset, WriterAt)>> {
        let (tail, writer) = self.writers_at(None).await?;
        Ok(tail
            .checked_sub(1)
            .filter(|_| writer != WriterAt::Absent)
            .map(|last| (last, writer)))
    }

    /// The tail, and the writer of the record at `offset`, or of the last
    /// record when `offset` is `None`.
    async fn writers_at(&self, offset: Option<Offset>) -> Result<(Offset, WriterAt)> {
        let inner = Arc::clone(&self.inner);
        if inner.closed.load(Ordering::Acquire) {
            return Err(StorageError::Closed(inner.label.clone()));
        }
        felix_common::blocking::spawn(move || {
            loop {
                // Planned under the lock and read without it, as a range
                // read is.
                let (plan, tail, offset) = {
                    let segments = inner.segments.read();
                    let tail = segments.tail_offset();
                    let Some(offset) = offset.or(tail.checked_sub(1)) else {
                        return Ok((tail, WriterAt::Absent));
                    };
                    if offset < segments.base_offset() || offset >= tail {
                        return Ok((tail, WriterAt::Absent));
                    }
                    (segments.read_plan(offset), tail, offset)
                };
                let found = plan.generation_at(offset, &inner.label);
                // A truncation in between may have put another record there.
                if inner.segments.read().generation() != plan.generation {
                    continue;
                }
                return Ok((
                    tail,
                    match found? {
                        None => WriterAt::Absent,
                        Some(None) => WriterAt::Unstamped,
                        Some(Some(generation)) => WriterAt::Generation(generation),
                    },
                ));
            }
        })
        .await
        .map_err(|err| StorageError::Io(std::io::Error::other(err)))?
    }
}

impl AppendOnlyLog for DiskLog {
    fn append(&self, records: &[AppendRecord]) -> BoxFuture<'_, Result<AppendResult>> {
        // `records` is borrowed for the duration of the future, so the write
        // happens first and only offsets cross the await.
        let records = records.to_vec();
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            let started = std::time::Instant::now();
            let pending = Self::write_batch(
                Arc::clone(&inner),
                records,
                append::WriteIf::Always,
                None,
                WrittenBy::Leader,
            )
            .await?
            .expect("an unconditional write is always written")
            .pending;

            // `OnCommit` is the only policy that makes the caller wait. The
            // others acknowledge once the bytes are in the page cache and rely
            // on the periodic flush (or the operating system) from there. A
            // trim waits whatever the policy, so its floor applies by then.
            if !inner.durability.acknowledges_before_sync() || pending.holds_trim {
                inner.ensure_durable(pending.durable_target).await?;
                // Checked again after the wait: `check_healthy` at the top of
                // `write_batch` only covers rollovers that had already failed
                // when this append started, and this one may have been in flight
                // across the failure.
                inner.check_healthy()?;
            }

            metrics::histogram!(metrics_names::APPEND_DURATION_SECONDS)
                .record(started.elapsed().as_secs_f64());
            Ok(pending.result)
        })
    }

    fn read_range(&self, range: ReadRange) -> BoxFuture<'_, Result<ReadPage>> {
        Box::pin(async move {
            let start = range.start;
            let (records, tail) = self.read_records(range, TrimRecords::Skip).await?;
            // The plan read to `tail` unless the budget stopped it, which
            // only a returned record can do. So an empty page skipped
            // nothing but trims, and the next read starts past them.
            let next = records
                .last()
                .map_or(tail.max(start), |record| record.offset + 1);
            Ok(ReadPage { records, next })
        })
    }

    fn read_range_with_trims(&self, range: ReadRange) -> BoxFuture<'_, Result<Vec<LogRecord>>> {
        Box::pin(async move {
            let (records, _) = self.read_records(range, TrimRecords::Include).await?;
            Ok(records)
        })
    }

    fn tail_offset(&self) -> BoxFuture<'_, Result<Offset>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move { Ok(inner.segments.read().tail_offset()) })
    }

    fn record_generation(&self, generation: u64, start_offset: Offset) -> Result<bool> {
        // Held across the write so a close cannot land in between: the file
        // may belong to another log the moment one does.
        let segments = self.inner.segments.read();
        segments.check_open()?;
        let mut epochs = self.inner.epochs.lock();
        self.inner.check_not_poisoned()?;
        if !epochs.record(generation, start_offset) {
            return Ok(false);
        }
        self.inner.poison_on_sync_failure(
            "the generation history",
            epochs::store(&self.inner.dir, &epochs),
        )?;
        Ok(true)
    }

    fn generations(&self) -> Vec<Epoch> {
        self.inner.epochs.lock().entries().to_vec()
    }

    fn generation_end(&self, generation: u64, tail: Offset) -> Option<Offset> {
        self.inner.epochs.lock().end_of(generation, tail)
    }

    fn truncate(&self, offset: Offset) -> BoxFuture<'_, Result<()>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            let fence = inner.fence_rolls().await?;
            let _flush_guard = inner.durability.lock_flushes().await;
            let operation = Arc::clone(&inner);
            felix_common::blocking::spawn(move || {
                let _fence = fence;
                // Before the append lock: an offload pass takes it before
                // the segment lock, and so must everything else.
                let mut manifest = operation.manifest.lock();
                let _appends = operation.append_lock.lock();
                let mut segments = operation.segments.write();
                let commit = operation.commit_offset.load(Ordering::Acquire);
                if offset < commit.min(segments.tail_offset()) {
                    return Err(StorageError::BelowCommit { offset, commit });
                }
                segments.check_open()?;
                // The copies go first: one that outlived a crash would
                // describe records the log no longer has.
                operation.forget_offloaded_from(&mut manifest, offset)?;
                let rewound = operation.cut_suffix(&mut segments, offset);
                operation.poison_after_rewind(rewound)
            })
            .await
            .map_err(|err| StorageError::Io(std::io::Error::other(err)))?
        })
    }

    fn seal(&self) -> BoxFuture<'_, Result<SealedSegment>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            felix_common::blocking::spawn(move || {
                let _appends = inner.append_lock.lock();
                let mut segments = inner.segments.write();
                inner.sync_pending_seal()?;
                let (descriptor, checksum) = match segments.seal_active() {
                    Ok(sealed) => sealed,
                    Err(err) => {
                        inner.poison_after_writer_failure(&segments);
                        // The index, which the writer does not own.
                        return inner.poison_on_sync_failure("a segment seal", Err(err));
                    }
                };
                // Start a fresh segment so the sealed one is immutable from here
                // on, which is what makes its checksum meaningful.
                if segments.active().record_count() > 0 {
                    if let Err(err) = segments.roll() {
                        inner.poison_after_writer_failure(&segments);
                        // Appends would otherwise go on into a segment whose
                        // directory entry may not survive a crash.
                        return inner.poison_on_sync_failure("a segment rollover", Err(err));
                    }
                    inner.note_sealed_before(segments.active().id());
                }
                Ok(SealedSegment {
                    descriptor,
                    checksum,
                })
            })
            .await
            .map_err(|err| StorageError::Io(std::io::Error::other(err)))?
        })
    }
}

/// A batch that has been written and given offsets, but not yet flushed.
#[derive(Debug, Clone)]
pub struct PendingAppend {
    pub result: AppendResult,
    /// Exclusive offset bound this batch needs durable.
    durable_target: Offset,
    /// The batch holds a trim record, which is synced before it is
    /// acknowledged whatever the fsync policy: its floor applies only once
    /// the record is durable.
    holds_trim: bool,
}

/// The trim record that set a log's head floor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadTrim {
    /// Where the record is.
    pub offset: Offset,
    /// The floor it asked for. Reads below it are refused.
    pub floor: Offset,
    /// The trimming caller's headers, the record's payload.
    pub headers: bytes::Bytes,
}

impl PendingAppend {
    pub fn first_offset(&self) -> Offset {
        self.result.first_offset
    }

    pub fn last_offset(&self) -> Offset {
        self.result.last_offset
    }
}

/// What a rollover saves beside the segments, as of `as_of`.
struct Snapshot {
    as_of: Offset,
    producers: ProducerState,
    /// `None` while the log holds no v7 segment, so has no trims to save.
    head: Option<Vec<head::HeadEntry>>,
}

/// Shared state behind every clone of a [`DiskLog`].
struct LogInner {
    label: String,
    /// Dropped with the log; see [`DiskLog::open_refusing_other_commit`].
    #[cfg_attr(not(feature = "raft"), allow(dead_code))]
    keep_alive: Mutex<Option<Arc<dyn std::any::Any + Send + Sync>>>,
    /// A Raft log's hold on its directory, until the log is closed or the
    /// last clone is dropped.
    #[cfg(feature = "raft")]
    dir_lock: Mutex<Option<dir_lock::DirLock>>,
    config: LogConfig,
    /// Guards the segment set. Held only for pointer work: a range read plans
    /// under the read lock and does its I/O after releasing it, and an append
    /// writes its batch after releasing it.
    segments: RwLock<SegmentSet>,
    /// Held by an append from placing its batch to taking it in, and by
    /// anything else that changes the active segment: a background roll's
    /// install, truncation, reset, restore, seal and close. Taken only off the
    /// reactor.
    append_lock: Mutex<()>,
    /// Records written, and how many of them the last flush covered, for
    /// reporting group-commit fan-in.
    records_written: AtomicU64,
    records_flushed: AtomicU64,
    durability: Durability,
    /// `None` unless the fsync policy is `Periodic`. Taken on shutdown.
    syncer: Mutex<Option<PeriodicSyncer>>,
    retention: Mutex<Option<retention::RetentionTask>>,
    /// The bounds retention enforces, which [`DiskLog::set_retention`] may
    /// change after the log opens.
    retention_bounds: Mutex<crate::log::Retention>,
    /// Which sealed segments have a verified copy in the object store.
    ///
    /// Lock order: this, then `append_lock`, then `segments`. Held by
    /// whatever unlinks or cuts segments, so a copy cannot be recorded for a
    /// segment on its way out, or forgotten for one about to be unlinked.
    manifest: Mutex<offload::Manifest>,
    /// `None` unless `LogConfig::offload` is set.
    offloader: Option<offload::Offloader>,
    /// Where each leadership generation began, for repairing a divergence.
    ///
    /// Its own lock rather than living under `segments`: it is read and written
    /// on the replication path, not the append path, and the append path is
    /// where lock contention costs something.
    epochs: Mutex<epochs::EpochMap>,
    /// The start record this log's leader last wrote; see
    /// [`DiskLog::stint_start`].
    stint: Mutex<Option<Epoch>>,
    /// The highest generation a leader was accepted at, read without a lock
    /// on every replicated batch. Raised only after it is on disk.
    accepted_generation: AtomicU64,
    /// One past the generation this broker opened the log to lead at, zero
    /// for none since it opened. A leader's appends are stamped with it. See
    /// [`DiskLog::lead_at`].
    led_generation: AtomicU64,
    /// The accepted generation with the leader it was accepted from, when one
    /// was named, and whether that vote is committed. Read together on every
    /// replicated batch, so a raise is seen with its own leader. Written only
    /// under `replica_persisted`.
    ballot: RwLock<ballot::Accepted>,
    /// The generation of `replica_set`, zero for none, read without a lock on
    /// every stamped batch. Raised only after the set is on disk.
    set_generation: AtomicU64,
    /// The replica set this log counts. Written only under `replica_persisted`.
    replica_set: RwLock<Option<ReplicaSet>>,
    /// This copy's wiped mark, kept with the set but apart from it. Written
    /// only under `replica_persisted`.
    wiped: RwLock<Option<set::WipedMark>>,
    /// One past the last record known committed. Raised in memory at once and
    /// written through or behind with the log's fsync mode; see
    /// [`DiskLog::advance_commit_offset`].
    commit_offset: AtomicU64,
    /// The generation of the record just below `commit_offset`, as a Raft
    /// log saved it; zero when not known. The two change together under
    /// this lock, so the file never pairs one id's index with another's
    /// term.
    committed_generation: Mutex<u64>,
    /// The commit offset `replica` last held on disk, so a raise whose write
    /// failed or is still due is told apart from one already written.
    commit_persisted: AtomicU64,
    /// Retention and head trims stop at `commit_offset`; see
    /// [`DiskLog::hold_retention_at_commit`].
    hold_at_commit: std::sync::atomic::AtomicBool,
    /// A task is writing the replica state again after a failed write; see
    /// [`DiskLog::advance_commit_offset`].
    replica_retry: std::sync::atomic::AtomicBool,
    /// A task is writing the commit offset behind
    /// [`DiskLog::advance_commit_offset`].
    commit_writer: std::sync::atomic::AtomicBool,
    /// Writes `persist_replica_state` made, for tests that count them.
    #[cfg(test)]
    replica_writes: AtomicU64,
    /// What `replica_state` last wrote, and when. Held only by the writer.
    /// Tokio's clock, so a simulation decides when to persist on virtual time.
    replica_persisted: Mutex<(replica_state::ReplicaState, Option<tokio::time::Instant>)>,
    /// Each idempotent producer's place in the log. Written only under the
    /// `segments` write lock, in offset order, so it always matches the tail.
    producers: Mutex<producers::ProducerState>,
    /// The log's trim records and the floor they set. Taken under `segments`
    /// when both are held.
    head: Mutex<head::Head>,
    /// Whether `head` has an entry the apply bound has not passed yet. While
    /// it is clear a read checks only `head_floor`.
    head_pending: std::sync::atomic::AtomicBool,
    /// `head`'s floor, for reads.
    head_floor: AtomicU64,
    /// Mirrors `ProducerState::is_open`, so an unmarked append can skip the
    /// lock in the common case of no batch waiting for records.
    batch_open: std::sync::atomic::AtomicBool,
    /// Where `epochs` is persisted, kept because the log needs it on truncation
    /// and nothing else hands it a directory.
    dir: PathBuf,
    /// Where the background rollover is in its lifecycle. At most one runs at
    /// a time, and a failure is terminal for the log.
    roll_state: AtomicU8,
    /// The in-flight rollover, so shutdown can wait for it to finish rather
    /// than dropping the runtime out from under a half-installed segment.
    roll_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// A retired segment that has been swapped out but not yet flushed.
    ///
    /// `flush` reports durability for the whole log, but it only ever syncs the
    /// *active* segment. With an inline rollover that was sound, because
    /// sealing happened before the replacement existed. A background rollover
    /// breaks it: between the swap and the seal there are records in the
    /// retired segment that no sync of the active segment covers, and reporting
    /// them durable would be a lie under `FsyncMode::OnCommit`. So `flush`
    /// syncs this handle too for as long as it is set.
    ///
    /// Cleared only after a *successful* seal. A failed one leaves it in place
    /// so every later flush keeps trying to cover those records rather than
    /// quietly reporting them durable.
    pending_seal: Mutex<Option<Arc<crate::fs::File>>>,
    /// Where this log's appends run.
    appender: LogThread,
    /// Where this log's device flushes run.
    flusher: LogThread,
    /// How far the active segment is known to be synced, for recovery after
    /// a power loss. Written after every flush.
    mark: durable_mark::MarkFile,
    /// Why the log stopped accepting work, once a flush or rollover has failed.
    ///
    /// Separate from `roll_state` because the two are read for different
    /// reasons and, critically, are written in a specific order: this is set
    /// *before* anything observable is relaxed, so there is no window in which
    /// a durability wait can see a cleared `pending_seal` and a state that is
    /// not yet `Failed`. `roll_state` remains the scheduler's view; this is the
    /// durability view.
    failure: Mutex<Option<String>>,
    /// Set once `DiskLog::close` has made its last flush. From then on even a
    /// flush is refused, so a stale handle cannot touch the files another log
    /// may now own.
    closed: std::sync::atomic::AtomicBool,
    /// Forces the next seal to fail, so the failure path can be tested.
    #[cfg(test)]
    fail_seal: std::sync::atomic::AtomicBool,
    /// Stops the next background seal before it flushes the retired segment,
    /// until the sender is used or dropped. Taking it is the sign it got there.
    #[cfg(test)]
    hold_next_seal: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    /// Stops the next reservation extension before it reserves anything,
    /// until the sender is used or dropped.
    #[cfg(test)]
    hold_next_extension: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    /// Makes every reservation extension fail as if the disk were full.
    #[cfg(test)]
    fail_extensions: std::sync::atomic::AtomicBool,
    /// Reservation extensions that have finished, whether or not they reserved.
    #[cfg(test)]
    extensions_done: AtomicU64,
    /// Makes the next flush report a failed fsync after the real one ran.
    #[cfg(test)]
    fail_next_flush: std::sync::atomic::AtomicBool,
    /// Makes the next truncation or reset fail to sync the durable mark.
    #[cfg(test)]
    fail_next_rewind_sync: std::sync::atomic::AtomicBool,
    /// Stops the next read between planning and reading: it waits on the
    /// barrier once to say it has planned, and again to go on.
    #[cfg(test)]
    pause_next_read: Mutex<Option<Arc<std::sync::Barrier>>>,
    /// Milliseconds an inline rollover holds the segment lock, for tests.
    #[cfg(test)]
    slow_inline_roll_millis: std::sync::atomic::AtomicU64,
    /// Set while a stretched inline rollover holds the segment lock.
    #[cfg(test)]
    inline_roll_active: std::sync::atomic::AtomicBool,
    /// Holds the next append at a point, until the sender is used or dropped.
    #[cfg(test)]
    hold_next_append: Mutex<Option<(append::HoldAt, std::sync::mpsc::Receiver<()>)>>,
    /// Set while an append is held there.
    #[cfg(test)]
    append_held: std::sync::atomic::AtomicBool,
    /// Device flushes performed, so tests can assert group-commit fan-in —
    /// per instance, where the global `SYNC_TOTAL` counter cannot isolate one
    /// log from the rest of a parallel test run.
    #[cfg(test)]
    flushes: std::sync::atomic::AtomicU64,
}

impl std::fmt::Debug for LogInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogInner")
            .field("label", &self.label)
            .field("fsync_mode", &self.config.fsync_mode)
            .finish_non_exhaustive()
    }
}

/// Micros since the Unix epoch, matching `AppendRecord::timestamp_micros`.
impl LogInner {
    /// Run retention on its timer from now on, unless it already runs.
    fn start_retention(self: &Arc<Self>) -> Result<()> {
        let mut running = self.retention.lock();
        if running.is_some() {
            return Ok(());
        }
        let weak = Arc::downgrade(self);
        let task =
            retention::RetentionTask::spawn(self.config.retention_check_interval, move || {
                let weak = weak.clone();
                async move {
                    match weak.upgrade() {
                        None => Ok(segments::RetentionOutcome::default()),
                        Some(inner) => inner.sweep_retention().await,
                    }
                }
            })?;
        *running = Some(task);
        Ok(())
    }

    /// Account for a batch just written at `first_offset`. Called holding the
    /// `segments` write lock the batch was written under.
    ///
    /// `digests` are the records' [`producers::marked_digest`]s, computed
    /// before the lock was taken, or empty when no record is marked.
    fn observe_marks(&self, first_offset: Offset, records: &[AppendRecord], digests: &[u64]) {
        use std::sync::atomic::Ordering;
        if digests.is_empty() && !self.batch_open.load(Ordering::Acquire) {
            return;
        }
        let mut producers = self.producers.lock();
        let mut trims = Vec::new();
        for (index, (offset, record)) in (first_offset..).zip(records).enumerate() {
            producers.observe(
                offset,
                record.mark,
                digests.get(index).copied().unwrap_or(0),
            );
            if let RecordMark::Trim { floor } = record.mark {
                trims.push(head::HeadEntry {
                    offset,
                    floor,
                    headers: record.payload.clone(),
                });
            }
        }
        self.batch_open
            .store(producers.is_open(), Ordering::Release);
        drop(producers);
        if !trims.is_empty() {
            let mut head = self.head.lock();
            head.observe(trims);
            self.publish_head(&head);
        }
    }

    /// Where a trim's floor starts to apply: below the durable offset, and on
    /// a log held at its commit offset below that too. A floor from a record
    /// a crash or a truncation can still take away would hide records nobody
    /// asked to hide.
    fn head_apply_bound(&self) -> Offset {
        let durable = self.durability.durable_upto();
        if self.hold_at_commit.load(Ordering::Acquire) {
            durable.min(self.commit_offset.load(Ordering::Acquire))
        } else {
            durable
        }
    }

    /// The head floor, applying first any entry the bound has passed.
    fn head_floor(&self) -> Offset {
        if self.head_pending.load(Ordering::Acquire) {
            let mut head = self.head.lock();
            self.apply_head(&mut head);
        }
        self.head_floor.load(Ordering::Acquire)
    }

    fn apply_head(&self, head: &mut head::Head) {
        head.apply(self.head_apply_bound());
        self.publish_head(head);
    }

    /// Copy `head` out for reads. Called holding the `head` lock.
    fn publish_head(&self, head: &head::Head) {
        self.head_floor.store(head.floor(), Ordering::Release);
        self.head_pending
            .store(head.has_pending(), Ordering::Release);
    }

    fn head_trim(&self) -> Option<HeadTrim> {
        let mut head = self.head.lock();
        self.apply_head(&mut head);
        head.setter().map(|entry| HeadTrim {
            offset: entry.offset,
            floor: entry.floor,
            headers: entry.headers.clone(),
        })
    }

    /// The refusal of a read from `requested`, below where the log serves.
    /// Called holding the `segments` read lock, so the floor can only have
    /// risen since the caller checked it.
    #[cold]
    fn trimmed(&self, requested: Offset, base: Offset, trims: TrimRecords) -> StorageError {
        let mut head = self.head.lock();
        self.apply_head(&mut head);
        let oldest = match trims {
            TrimRecords::Skip => base.max(head.floor()),
            TrimRecords::Include | TrimRecords::Only => base,
        };
        StorageError::Trimmed {
            requested,
            oldest,
            trim_headers: head.setter().map(|entry| entry.headers.clone()),
        }
    }

    /// Drop every record at or after `offset` and bring the durable mark,
    /// the generation history and the producer table down with them. Called
    /// holding the flush lock and the `segments` write lock.
    fn cut_suffix(&self, segments: &mut SegmentSet, offset: Offset) -> Result<()> {
        let cuts_records = offset < segments.tail_offset();
        segments.truncate(offset)?;
        segments.active_mut().sync()?;
        self.note_rewound(segments)?;
        self.durability.reset_after_truncate(segments.tail_offset());
        // The history cannot outlive the records it describes, or it would
        // answer with a start offset the log no longer holds.
        let mut epochs = self.epochs.lock();
        if cuts_records {
            epochs.cut_at(offset);
        } else {
            epochs.truncate_from(offset);
        }
        epochs::store(&self.dir, &epochs)?;
        self.reset_producers(segments)
    }

    /// Whether `producer_id`'s batch `sequence` is open with its next record
    /// due at the tail. Called holding the `segments` write lock.
    fn batch_open_at_tail(&self, segments: &SegmentSet, producer_id: u64, sequence: u64) -> bool {
        matches!(
            self.producers.lock().classify(producer_id, sequence),
            ProducerSequence::Partial { first, held, .. }
                if first + u64::from(held) == segments.tail_offset()
        )
    }

    /// The producer state and the head entries as of the tail, for the
    /// snapshots taken at a rollover. Called holding the `segments` write
    /// lock.
    ///
    /// The producer state is taken even when empty, which is almost every
    /// log: without a snapshot an open has to read every v3 sealed segment to
    /// learn there is nothing in them. The head entries are taken once the
    /// log is on v7, for the same reason.
    fn snapshot(&self, segments: &SegmentSet) -> Snapshot {
        let producers = self.producers.lock().clone();
        let head = {
            let head = self.head.lock();
            (!head.entries().is_empty() || segments.newest_sealed_holds_trims())
                .then(|| head.entries().to_vec())
        };
        Snapshot {
            as_of: segments.tail_offset(),
            producers,
            head,
        }
    }

    /// Save a snapshot taken by [`Self::snapshot`]. Failing costs a longer
    /// open later, so it is logged rather than returned.
    fn store_snapshot(&self, snapshot: Snapshot) {
        // Held so a close cannot land between the check and the write.
        let segments = self.segments.read();
        if segments.check_open().is_err() {
            return;
        }
        if let Err(err) = producers::store(&self.dir, &snapshot.producers, snapshot.as_of) {
            tracing::warn!(
                shard = %self.label,
                error = %err,
                "could not save the producer snapshot; the next open reads further back",
            );
        }
        if let Some(entries) = &snapshot.head
            && let Err(err) = head::store(&self.dir, snapshot.as_of, entries)
        {
            tracing::warn!(
                shard = %self.label,
                error = %err,
                "could not save the head file; the next open reads the trim records again",
            );
        }
    }

    /// Rebuild producer state and the head entries after the log was cut
    /// back to its tail. The producer snapshot, the head file, and a cache's
    /// key index snapshot when this log backs a cache, may describe records
    /// that are gone, so all three are removed first, durably: one that came
    /// back after a crash would be read as describing the records that
    /// replaced them.
    fn reset_producers(&self, segments: &SegmentSet) -> Result<()> {
        {
            let mut head = self.head.lock();
            head.truncate_from(segments.tail_offset());
            head.prune_below(segments.base_offset());
            self.publish_head(&head);
        }
        head::discard(&self.dir).map_err(StorageError::Io)?;
        producers::discard(&self.dir).map_err(StorageError::Io)?;
        crate::index_snapshot::remove(&self.dir).map_err(StorageError::Io)?;
        crate::io::sync_dir(&self.dir).map_err(StorageError::Io)?;
        let state = producers::rebuild(&self.dir, segments, None)?;
        self.batch_open
            .store(state.is_open(), std::sync::atomic::Ordering::Release);
        *self.producers.lock() = state;
        Ok(())
    }
}

impl LogInner {
    /// The replica state as memory has it.
    fn replica_state(&self) -> replica_state::ReplicaState {
        let committed = self.committed_generation.lock();
        self.replica_state_with(*committed)
    }

    /// [`Self::replica_state`] for a caller that holds `committed_generation`.
    fn replica_state_with(&self, committed_generation: u64) -> replica_state::ReplicaState {
        replica_state::ReplicaState {
            accepted_generation: self.accepted_generation.load(Ordering::Acquire),
            commit_offset: self.commit_offset.load(Ordering::Acquire),
            hold_at_commit: self.hold_at_commit.load(Ordering::Acquire),
            committed_generation,
        }
    }
}

/// Write the replica state if memory is ahead of disk. A write is refused
/// once the log is closed: its directory may already be another log's.
async fn persist_replica_state(inner: Arc<LogInner>) -> Result<()> {
    felix_common::blocking::spawn(move || {
        let mut persisted = inner.replica_persisted.lock();
        let state = inner.replica_state();
        if state == persisted.0 {
            return Ok(());
        }
        // `close` sets `closed` under this lock and returns only after. A
        // write that sees it unset here therefore lands before `close`
        // returns, while the directory is still this log's.
        if inner.closed.load(Ordering::Acquire) {
            return Err(StorageError::Closed(inner.label.clone()));
        }
        // Not retried once poisoned: the write behind would otherwise try
        // again, and a retry after a failed flush proves nothing.
        inner.check_not_poisoned()?;
        inner.poison_on_sync_failure(
            "the replica state",
            replica_state::store(&inner.dir, &state),
        )?;
        #[cfg(test)]
        inner.replica_writes.fetch_add(1, Ordering::Relaxed);
        inner
            .commit_persisted
            .store(state.commit_offset, Ordering::Release);
        *persisted = (state, Some(tokio::time::Instant::now()));
        Ok(())
    })
    .await
    .map_err(|err| StorageError::Io(std::io::Error::other(err)))?
}

/// Write the commit offset behind the caller. At most one writer runs per
/// log; it loops until disk has caught up with memory, so an advance made
/// while a write is in flight is carried by the next one rather than adding
/// its own.
fn write_commit_behind(inner: &Arc<LogInner>) {
    if inner.commit_writer.swap(true, Ordering::AcqRel) {
        return;
    }
    // Weak, so the writer neither keeps a dropped log alive nor outlives it.
    let weak = Arc::downgrade(inner);
    tokio::spawn(async move {
        loop {
            let Some(inner) = weak.upgrade() else {
                return;
            };
            let written = persist_replica_state(Arc::clone(&inner)).await;
            // A swap, not a store: as a read-modify-write it reads an
            // advance's `swap(true)` that came first, so the load below sees
            // that advance's offset. A store then a load could miss both.
            inner.commit_writer.swap(false, Ordering::AcqRel);
            if written.is_err() {
                if !inner.closed.load(Ordering::Acquire) {
                    retry_replica_state(&inner);
                }
                return;
            }
            // An advance that found the flag set while this write ran left
            // its offset to this writer.
            let behind = inner.commit_offset.load(Ordering::Acquire)
                > inner.commit_persisted.load(Ordering::Acquire);
            if !behind || inner.commit_writer.swap(true, Ordering::AcqRel) {
                return;
            }
        }
    });
}

/// Write the replica state again until it reaches disk. On a follower only
/// the leader's next batch would call [`DiskLog::advance_commit_offset`]
/// again, and an idle shard sends none.
fn retry_replica_state(inner: &Arc<LogInner>) {
    if inner.replica_retry.swap(true, Ordering::AcqRel) {
        return;
    }
    let weak = Arc::downgrade(inner);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(COMMIT_PERSIST_INTERVAL).await;
            let Some(inner) = weak.upgrade() else {
                return;
            };
            // A poisoned log writes nothing more, so there is nothing to
            // retry.
            if inner.closed.load(Ordering::Acquire) || inner.is_poisoned() {
                return;
            }
            if persist_replica_state(Arc::clone(&inner)).await.is_err() {
                continue;
            }
            // A swap for the same reason as the writer's.
            inner.replica_retry.swap(false, Ordering::AcqRel);
            // A write that failed while this one ran found the flag set
            // and left the retry to this task.
            let behind = inner.commit_offset.load(Ordering::Acquire)
                > inner.commit_persisted.load(Ordering::Acquire);
            if !behind || inner.replica_retry.swap(true, Ordering::AcqRel) {
                return;
            }
        }
    });
}

/// Wall-clock microseconds for retention; virtual time under a simulator.
fn now_micros() -> u64 {
    felix_common::clock::system_time()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests;
