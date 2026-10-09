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
//! * `ballot`    — which leader that generation was accepted from.
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
//!   not queue behind other work on the shared blocking pool. Flushes are
//!   grouped, so one runs for many appends.
//! * `read_range` may touch cold blocks, so it runs entirely on `spawn_blocking`.
//!   It is a replay and catch-up path, not the publish hot path.

pub mod inspect;
pub mod layout;

mod append;
mod ballot;
mod durable_mark;
mod epochs;
mod flush;
#[cfg(feature = "fuzzing")]
pub(crate) mod fuzzing;
mod offload;
mod producers;
mod provider;
mod recovery;
pub(crate) mod replica_state;
mod retention;
mod sealed;
mod segments;
mod sync;

pub use producers::ProducerSequence;
pub use provider::DiskLogProvider;
pub use segments::RetentionOutcome;

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
    Offset, ReadRange, RecordMark, SealedSegment, SegmentDescriptor,
};
use crate::segment::ReadBudget;
use crate::{CommitSequencer, CommitTurn, Result, StorageError, metrics_names};

/// How long an append thread and a lone appender poll before parking. A
/// wake-up on each side costs more than the `write` being handed over.
const APPEND_SPIN: std::time::Duration = std::time::Duration::from_micros(20);

/// How often an advancing commit offset is written behind when the log is not
/// fsynced on commit. Its records are written behind too, so an offset that
/// outlived them would guard nothing.
pub const COMMIT_PERSIST_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

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
    pub fn open(
        dir: impl Into<PathBuf>,
        label: impl Into<String>,
        config: LogConfig,
    ) -> Result<Self> {
        let files = SealedFiles::new(config.max_open_sealed_segments);
        Self::open_shared(dir.into(), label.into(), config, None, files)
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
        Self::open_shared(dir.into(), label.into(), config, Some(base_offset), files)
    }

    pub fn label(&self) -> &str {
        &self.inner.label
    }

    pub fn config(&self) -> &LogConfig {
        &self.inner.config
    }

    /// Oldest offset still readable.
    pub fn base_offset(&self) -> Offset {
        self.inner.segments.read().base_offset()
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
        Self::write_batch(inner, records.to_vec(), append::WriteIf::Always, None)
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
    pub async fn append_pending_at(
        &self,
        first_offset: Offset,
        records: &[AppendRecord],
    ) -> Result<Option<PendingAppend>> {
        let inner = Arc::clone(&self.inner);
        let written = Self::write_batch(
            inner,
            records.to_vec(),
            append::WriteIf::At(first_offset),
            None,
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
    pub async fn commit(&self, pending: &PendingAppend) -> Result<()> {
        if self.inner.durability.acknowledges_before_sync() {
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
        let _flush_guard = inner.durability.lock_flushes().await;
        let operation = Arc::clone(&inner);
        tokio::task::spawn_blocking(move || {
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
        let _flush_guard = inner.durability.lock_flushes().await;
        let operation = Arc::clone(&inner);
        tokio::task::spawn_blocking(move || {
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
                    let state = replica_state::ReplicaState {
                        accepted_generation: operation.accepted_generation.load(Ordering::Acquire),
                        commit_offset: offset,
                        hold_at_commit: operation.hold_at_commit.load(Ordering::Acquire),
                    };
                    replica_state::store(&operation.dir, &state)?;
                    operation.commit_offset.store(offset, Ordering::Release);
                    *persisted = (state, Some(std::time::Instant::now()));
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

    /// The highest leadership generation a leader of this shard was accepted
    /// at here, as a follower or as the leader itself. Zero if none.
    pub fn accepted_generation(&self) -> u64 {
        self.inner.accepted_generation.load(Ordering::Acquire)
    }

    /// The leader this log accepted its generation from, if it was named.
    pub fn accepted_leader(&self) -> Option<Arc<str>> {
        self.inner.ballot.read().1.clone()
    }

    /// Accept `leader` at `generation`, persisting it first if it is new.
    ///
    /// Returns once a raised generation is on disk, so a caller that
    /// acknowledges afterwards has made a promise that survives a restart: a
    /// leader older than this one is refused from here on, whatever the
    /// routing view says after the restart.
    ///
    /// With a leader named, the promise is a ballot: at the generation already
    /// accepted only that leader is [`GenerationCheck::Current`], and any
    /// other is [`GenerationCheck::Promised`]. A generation accepted with no
    /// leader named takes the first one that asks, persisted before this
    /// returns. `None` checks the generation alone.
    pub async fn accept_generation(
        &self,
        generation: u64,
        leader: Option<&str>,
    ) -> Result<GenerationCheck> {
        if let Some(found) = ballot::check(&self.inner.ballot.read(), generation, leader) {
            return Ok(found);
        }
        let inner = Arc::clone(&self.inner);
        let leader: Option<Arc<str>> = leader.map(Arc::from);
        tokio::task::spawn_blocking(move || {
            let mut persisted = inner.replica_persisted.lock();
            // Re-checked under the writer's lock: a concurrent request may
            // have raised it, or named its leader, while this waited.
            if let Some(found) = ballot::check(&inner.ballot.read(), generation, leader.as_deref())
            {
                return Ok(found);
            }
            // Before `replica`, so a crash between the two leaves a ballot
            // the open takes the generation from, never a raised generation
            // with no leader.
            if let Some(leader) = &leader {
                ballot::store(
                    &inner.dir,
                    &ballot::Ballot {
                        generation,
                        leader: leader.to_string(),
                    },
                )?;
            }
            let raised = generation > inner.accepted_generation.load(Ordering::Acquire);
            if raised {
                let state = replica_state::ReplicaState {
                    accepted_generation: generation,
                    commit_offset: inner.commit_offset.load(Ordering::Acquire),
                    hold_at_commit: inner.hold_at_commit.load(Ordering::Acquire),
                };
                replica_state::store(&inner.dir, &state)?;
                *persisted = (state, Some(std::time::Instant::now()));
            }
            *inner.ballot.write() = (generation, leader);
            inner
                .accepted_generation
                .store(generation, Ordering::Release);
            Ok(if raised {
                GenerationCheck::Raised
            } else {
                GenerationCheck::Current
            })
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
            epochs::store(&self.inner.dir, &epochs)?;
        }
        Ok(())
    }

    /// One past the last record known committed. Zero if none is known.
    pub fn commit_offset(&self) -> Offset {
        self.inner.commit_offset.load(Ordering::Acquire)
    }

    /// Record that every record below `offset` is committed.
    ///
    /// Takes effect at once for [`AppendOnlyLog::truncate`] and
    /// [`DiskLog::reset_to`], which refuse to cut below it. Under
    /// [`FsyncMode::OnCommit`] it is on disk when this returns, so a restart
    /// reads back every offset acknowledged under it. Otherwise it reaches disk
    /// at most once per [`COMMIT_PERSIST_INTERVAL`], and always with a raised
    /// generation and at shutdown; the offset read back after a crash may then
    /// be behind, which weakens the guard: truncation may cut into records
    /// committed since, until the leader's next batch carries the offset again.
    pub async fn advance_commit_offset(&self, offset: Offset) -> Result<()> {
        let previous = self.inner.commit_offset.fetch_max(offset, Ordering::AcqRel);
        if offset <= previous {
            return Ok(());
        }
        // Concurrent advances coalesce: whoever takes the writer's lock next
        // writes the highest offset raised so far, and the rest find it done.
        let due = matches!(self.inner.config.fsync_mode, FsyncMode::OnCommit) || {
            let persisted = self.inner.replica_persisted.lock();
            persisted
                .1
                .is_none_or(|at| at.elapsed() >= COMMIT_PERSIST_INTERVAL)
        };
        if due {
            self.persist_replica_state().await?;
        }
        Ok(())
    }

    /// Keep retention and compaction's head trims below the commit offset.
    ///
    /// For a log replicated under `Quorum`, where a record above the commit
    /// offset may still be waiting for a majority. Deleting it there would let
    /// a follower rebuilt at the new base be counted as holding it. Off for a
    /// new log, since a log nobody advances the commit offset on would never
    /// trim.
    ///
    /// Takes effect in memory at once, from the next pass. A change is then
    /// written to the replica state, and the log reopens with it, so a
    /// restarted log is held before replication has touched it. If that write
    /// fails the error is returned and the hold stays in effect in memory.
    pub async fn hold_retention_at_commit(&self, hold: bool) -> Result<()> {
        if self.inner.hold_at_commit.swap(hold, Ordering::AcqRel) == hold {
            return Ok(());
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
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let mut persisted = inner.replica_persisted.lock();
            let state = replica_state::ReplicaState {
                accepted_generation: inner.accepted_generation.load(Ordering::Acquire),
                commit_offset: inner.commit_offset.load(Ordering::Acquire),
                hold_at_commit: inner.hold_at_commit.load(Ordering::Acquire),
            };
            if state == persisted.0 {
                return Ok(());
            }
            replica_state::store(&inner.dir, &state)?;
            *persisted = (state, Some(std::time::Instant::now()));
            Ok(())
        })
        .await
        .map_err(|err| StorageError::Io(std::io::Error::other(err)))?
    }

    /// Stop background work, flush everything one last time, and refuse any
    /// further change to the files, from this handle or any clone of it.
    ///
    /// For a shard this broker no longer holds: once this returns, another
    /// `DiskLog` may be opened over the same directory. Reads, appends and
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
        tokio::task::spawn_blocking(move || {
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
        self.inner
            .closed
            .store(true, std::sync::atomic::Ordering::Release);
        flushed.and(marked)
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
    ) -> Result<Self> {
        config.validate()?;

        if let Some(base_offset) = base_offset.filter(|base| *base > 0) {
            recovery::place_empty_shard(&dir, &label, &config, base_offset)?;
        }
        // Before recovery, which needs it to tell a segment that was
        // offloaded from one that was lost.
        let manifest = offload::manifest::load(&dir)?;
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
        let replica = replica_state::load(&dir)?;
        // A ballot ahead of `replica` is a raise that crashed between the two
        // writes; it was never answered, but taking it only refuses more.
        let (accepted_generation, accepted_leader) = match ballot::load(&dir)? {
            Some(ballot) if ballot.generation >= replica.accepted_generation => {
                (ballot.generation, Some(Arc::<str>::from(ballot.leader)))
            }
            _ => (replica.accepted_generation, None),
        };
        let epochs_dir = dir.clone();
        let segments = SegmentSet::new(
            dir,
            label.clone(),
            config.clone(),
            recovered.sealed,
            recovered.active,
            files,
        )?;
        // Before the log is usable, so no append can be accepted against a
        // producer state that does not yet include what is already on disk.
        let producer_state =
            producers::rebuild(&epochs_dir, &segments, Some(&recovered.active_marks))?;

        // Everything that survived recovery is on disk, so the durable bound
        // starts at the recovered tail.
        let durable_upto = segments.tail_offset();
        let inner = Arc::new(LogInner {
            label,
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
            accepted_generation: AtomicU64::new(accepted_generation),
            ballot: RwLock::new((accepted_generation, accepted_leader)),
            commit_offset: AtomicU64::new(replica.commit_offset),
            // Restored before retention starts below, so a sweep after a
            // restart is held before any replication pass or batch has run.
            hold_at_commit: std::sync::atomic::AtomicBool::new(replica.hold_at_commit),
            replica_persisted: Mutex::new((replica, None)),
            batch_open: std::sync::atomic::AtomicBool::new(producer_state.is_open()),
            producers: Mutex::new(producer_state),
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

        if config.retention().is_set() || inner.offloader.is_some() {
            inner.start_retention()?;
        }

        Ok(Self { inner })
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
            let pending =
                Self::write_batch(Arc::clone(&inner), records, append::WriteIf::Always, None)
                    .await?
                    .expect("an unconditional write is always written")
                    .pending;

            // `OnCommit` is the only policy that makes the caller wait. The
            // others acknowledge once the bytes are in the page cache and rely
            // on the periodic flush (or the operating system) from there.
            if !inner.durability.acknowledges_before_sync() {
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

    fn read_range(&self, range: ReadRange) -> BoxFuture<'_, Result<Vec<LogRecord>>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            if inner.closed.load(std::sync::atomic::Ordering::Acquire) {
                return Err(StorageError::Closed(inner.label.clone()));
            }
            let started = std::time::Instant::now();
            let records = tokio::task::spawn_blocking(move || {
                loop {
                    // Planned under the lock, read without it: a cold `pread`
                    // must not hold up the appends queued on `segments`.
                    let plan = {
                        let segments = inner.segments.read();
                        let oldest = segments.base_offset();
                        if range.start < oldest {
                            // Not an empty range: these offsets existed and
                            // are gone.
                            return Err(StorageError::Trimmed {
                                requested: range.start,
                                oldest,
                            });
                        }
                        segments.read_plan(range.start)
                    };
                    #[cfg(test)]
                    if let Some(pause) = inner.pause_next_read.lock().take() {
                        pause.wait();
                        pause.wait();
                    }
                    let mut budget =
                        ReadBudget::new(range.max_bytes, inner.config.max_records_per_read);
                    let read = plan.execute(range.start, &mut budget, &inner.label);
                    // A truncation in between may have put other records at
                    // the positions just read. Rare, so read again.
                    if inner.segments.read().generation() == plan.generation {
                        return read;
                    }
                }
            })
            .await
            .map_err(|err| StorageError::Io(std::io::Error::other(err)))??;

            metrics::counter!(metrics_names::READ_RECORDS_TOTAL).increment(records.len() as u64);
            metrics::counter!(metrics_names::READ_BYTES_TOTAL)
                .increment(records.iter().map(|r| r.payload.len() as u64).sum::<u64>());
            metrics::histogram!(metrics_names::READ_DURATION_SECONDS)
                .record(started.elapsed().as_secs_f64());
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
        if !epochs.record(generation, start_offset) {
            return Ok(false);
        }
        epochs::store(&self.inner.dir, &epochs)?;
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
            let _flush_guard = inner.durability.lock_flushes().await;
            let operation = Arc::clone(&inner);
            tokio::task::spawn_blocking(move || {
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
            tokio::task::spawn_blocking(move || {
                let _appends = inner.append_lock.lock();
                let mut segments = inner.segments.write();
                inner.sync_pending_seal()?;
                let (descriptor, checksum) = match segments.seal_active() {
                    Ok(sealed) => sealed,
                    Err(err) => {
                        inner.poison_after_writer_failure(&segments);
                        return Err(err);
                    }
                };
                // Start a fresh segment so the sealed one is immutable from here
                // on, which is what makes its checksum meaningful.
                if segments.active().record_count() > 0 {
                    segments.roll()?;
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
}

impl PendingAppend {
    pub fn first_offset(&self) -> Offset {
        self.result.first_offset
    }

    pub fn last_offset(&self) -> Offset {
        self.result.last_offset
    }
}

/// Shared state behind every clone of a [`DiskLog`].
struct LogInner {
    label: String,
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
    /// The highest generation a leader was accepted at, read without a lock
    /// on every replicated batch. Raised only after it is on disk.
    accepted_generation: AtomicU64,
    /// The accepted generation with the leader it was accepted from, when one
    /// was named. Read together on every replicated batch, so a raise is seen
    /// with its own leader. Written only under `replica_persisted`.
    ballot: RwLock<(u64, Option<Arc<str>>)>,
    /// One past the last record known committed. Raised in memory at once and
    /// written through or behind with the log's fsync mode; see
    /// [`DiskLog::advance_commit_offset`].
    commit_offset: AtomicU64,
    /// Retention and head trims stop at `commit_offset`; see
    /// [`DiskLog::hold_retention_at_commit`].
    hold_at_commit: std::sync::atomic::AtomicBool,
    /// What `replica_state` last wrote, and when. Held only by the writer.
    replica_persisted: Mutex<(replica_state::ReplicaState, Option<std::time::Instant>)>,
    /// Each idempotent producer's place in the log. Written only under the
    /// `segments` write lock, in offset order, so it always matches the tail.
    producers: Mutex<producers::ProducerState>,
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
    pending_seal: Mutex<Option<Arc<std::fs::File>>>,
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
        for (index, (offset, record)) in (first_offset..).zip(records).enumerate() {
            producers.observe(
                offset,
                record.mark,
                digests.get(index).copied().unwrap_or(0),
            );
        }
        self.batch_open
            .store(producers.is_open(), Ordering::Release);
    }

    /// Drop every record at or after `offset` and bring the durable mark,
    /// the generation history and the producer table down with them. Called
    /// holding the flush lock and the `segments` write lock.
    fn cut_suffix(&self, segments: &mut SegmentSet, offset: Offset) -> Result<()> {
        segments.truncate(offset)?;
        segments.active_mut().sync()?;
        self.note_rewound(segments)?;
        self.durability.reset_after_truncate(segments.tail_offset());
        // The history cannot outlive the records it describes, or it would
        // answer with a start offset the log no longer holds.
        let mut epochs = self.epochs.lock();
        epochs.truncate_from(offset);
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

    /// The producer state as of the tail, for the snapshot taken at a
    /// rollover. Called holding the `segments` write lock.
    ///
    /// Taken even when empty, which is almost every log: without a snapshot
    /// an open has to read every v3 sealed segment to learn there is nothing
    /// in them.
    fn producer_snapshot(&self, segments: &SegmentSet) -> (Offset, ProducerState) {
        (segments.tail_offset(), self.producers.lock().clone())
    }

    /// Save a snapshot taken by [`Self::producer_snapshot`]. Failing costs a
    /// longer open later, so it is logged rather than returned.
    fn store_producer_snapshot(&self, (as_of, state): (Offset, ProducerState)) {
        // Held so a close cannot land between the check and the write.
        let segments = self.segments.read();
        if segments.check_open().is_err() {
            return;
        }
        if let Err(err) = producers::store(&self.dir, &state, as_of) {
            tracing::warn!(
                shard = %self.label,
                error = %err,
                "could not save the producer snapshot; the next open reads further back",
            );
        }
    }

    /// Rebuild producer state after the log was cut back. The producer
    /// snapshot, and a cache's key index snapshot when this log backs a
    /// cache, may describe records that are gone, so both are removed first,
    /// durably: one that came back after a crash would be read as describing
    /// the records that replaced them.
    fn reset_producers(&self, segments: &SegmentSet) -> Result<()> {
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

fn now_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests;
