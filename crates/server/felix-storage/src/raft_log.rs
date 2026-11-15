//! A [`DiskLog`] read as an openraft log: one record is one entry, the
//! record's offset is the entry's index, and the generation in the record's
//! header gives its term.
//!
//! [`RaftLog`] implements openraft's `RaftLogStorage` (append, truncate,
//! purge and the vote) and [`LogReader`] its `RaftLogReader`.
//! [`SnapshotStore`] keeps a group's snapshot for the state machine that
//! runs beside the log. The design is in `docs/replication-engine.md`,
//! "Raft log on the segment store".
//!
//! These rules hold everywhere in this module:
//!
//! - **A record's generation is its entry's term plus one.** openraft writes
//!   its first entry at term 0, and generation 0 in a record header means
//!   "writer unknown" ([`UNKNOWN_WRITER`]). Shifting by one keeps the two
//!   apart, so a record whose writer is unknown is an error and never a
//!   term-0 entry.
//! - **Blank and membership entries are records readers skip.** A blank
//!   entry is a [`RecordMark::GenerationStart`] record and a membership entry
//!   a [`RecordMark::Membership`] record. Each takes an offset, because the
//!   offset is the Raft index, but no client read returns it and a delivered
//!   batch's `skipped_before` counts it, so a consumer still reads any other
//!   jump in offsets as a drop.
//! - **A record that cannot be an entry stops the read.** A record with no
//!   generation, one below the log's first index, or a missing offset is an
//!   error, never a guess.
//! - **The log starts after the purged id.** The id is on disk before a
//!   purge deletes anything, and segments go whole, so records at or below
//!   it can remain. They are never read as entries. Under
//!   [`RaftLog::purge_hides_past`] an id past the stored snapshot is kept
//!   in memory only and read the same way.
//!
//! openraft types stay inside this module and the engine that drives it.

mod codec;
mod flush_queue;
pub(crate) mod held;
mod hide;
mod purged;
mod reader;
pub(crate) mod reserved;
pub(crate) mod snapshot;
pub(crate) mod state_file;
mod tail_cache;
#[cfg(test)]
pub(crate) mod tests;
pub mod transfer;
mod writer;
mod written;

pub use codec::EntryCodec;
/// The leader id in a log id: for this log, the term. Named in the bounds
/// of everything generic over [`LogTypes`].
pub use openraft::type_config::alias::CommittedLeaderIdOf;
pub use reader::LogReader;
pub use snapshot::{
    SnapshotData, SnapshotId, SnapshotStore, StagedSnapshot, StoredReader, StoredSnapshot,
};
pub use tail_cache::TailBudget;
pub use written::{WrittenAt, WrittenToken};

use std::io;
use std::ops::Deref;
use std::sync::Arc;

use openraft::RaftTypeConfig;
use openraft::entry::{Entry, EntryPayload};
use openraft::storage::LogState;
use openraft::type_config::alias::LogIdOf;

use self::flush_queue::FlushQueue;
use self::hide::{Bounds, Covering, PastSnapshot};
use self::tail_cache::Tail;
use crate::DiskLog;
use crate::disk_log::WriterAt;
use crate::log::{FsyncMode, Offset};
#[cfg(doc)]
use crate::log::{LogConfig, RecordMark, UNKNOWN_WRITER};
use crate::raft_copy::{CopyMarks, LogMarkMode};
use crate::waiters::{Queue, Waiters};

/// The openraft type configs this log stores: a log id is a term and an
/// index, and an entry is openraft's own [`Entry`].
///
/// `leader_id_std::LeaderId` gives the first. The adapter has no place to
/// keep the node id that `leader_id_adv` puts in a log id, so a config using
/// it does not satisfy this trait.
pub trait LogTypes:
    RaftTypeConfig<
        Term = u64,
        Entry = Entry<
            CommittedLeaderIdOf<Self>,
            EntryPayload<<Self as RaftTypeConfig>::D, <Self as RaftTypeConfig>::Membership>,
        >,
        Payload = EntryPayload<<Self as RaftTypeConfig>::D, <Self as RaftTypeConfig>::Membership>,
    >
where
    CommittedLeaderIdOf<Self>: From<u64> + Deref<Target = u64>,
{
}

impl<C> LogTypes for C
where
    C: RaftTypeConfig<
            Term = u64,
            Entry = Entry<
                CommittedLeaderIdOf<C>,
                EntryPayload<<C as RaftTypeConfig>::D, <C as RaftTypeConfig>::Membership>,
            >,
            Payload = EntryPayload<<C as RaftTypeConfig>::D, <C as RaftTypeConfig>::Membership>,
        >,
    CommittedLeaderIdOf<C>: From<u64> + Deref<Target = u64>,
{
}

/// A [`DiskLog`] as an openraft log. Cheap to clone; every clone and every
/// [`LogReader`] reads the same log.
pub struct RaftLog<C, K> {
    shared: Arc<Shared<K>>,
    types: std::marker::PhantomData<fn() -> C>,
}

/// One entry of a log: its index and the term that wrote it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogPoint {
    pub term: u64,
    pub index: u64,
}

/// How far a Raft log has got since it was opened, by the last entry of
/// each kind. `None` until this adapter's first append.
///
/// `written` moves as soon as an append has handed its entries to the log,
/// before the fsync, so a power cut can take what it names. `flushed` moves
/// once they are on disk, and is the only one Raft is told about. A
/// truncation moves both back.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LogPositions {
    pub written: Option<LogPoint>,
    pub flushed: Option<LogPoint>,
}

impl LogPositions {
    /// Drop what a truncation to `kept` cut.
    fn cut_to(&mut self, kept: Option<LogPoint>) {
        for point in [&mut self.written, &mut self.flushed] {
            if point.is_some_and(|at| kept.is_none_or(|kept| at.index > kept.index)) {
                *point = kept;
            }
        }
    }
}

/// The positions' watch, and a queue for each position beside it so a
/// write waiting on one is woken only once it gets there.
#[derive(Default)]
struct Positions {
    sent: tokio::sync::watch::Sender<LogPositions>,
    written: Queue<(u64, u64)>,
    flushed: Queue<(u64, u64)>,
}

impl Positions {
    fn send_modify(&self, modify: impl FnOnce(&mut LogPositions)) {
        self.sent.send_modify(modify);
        // Read back rather than taken from `modify`: a flush callback on
        // another thread may have sent since.
        let now = *self.sent.borrow();
        let key = |at: Option<LogPoint>| at.map(|at| (at.term, at.index));
        self.written.advance(key(now.written));
        self.flushed.advance(key(now.flushed));
    }
}

struct Shared<K> {
    log: DiskLog,
    codec: K,
    positions: Positions,
    /// The last purged entry.
    bounds: parking_lot::Mutex<Bounds>,
    /// Set by [`RaftLog::purge_within`] or [`RaftLog::purge_hides_past`]:
    /// the snapshot that bounds a purge, and what a purge past it does.
    covering: std::sync::OnceLock<Covering>,
    /// Appends whose callback has not fired. A truncation waits for it to
    /// empty.
    flushes: FlushQueue,
    /// The newest entries, so replication reads them without the segments.
    tail: Arc<Tail>,
    /// Holds the next paged read after its first page: the read signals the
    /// sender, then waits for the receiver.
    #[cfg(test)]
    pause_after_page: parking_lot::Mutex<Option<tests::PagePause>>,
    /// Reads of the segments, as against the tail cache.
    #[cfg(test)]
    segment_reads: std::sync::atomic::AtomicU64,
    /// The root's marks for this copy, once [`RaftLog::mark_with`] set them.
    marking: std::sync::OnceLock<Marking>,
    /// The log does not wait for its fsync before an append's callback: Raft
    /// counts an entry once it is written. Only on a root that rotates on a
    /// crash ([`run_state::rotates_on_crash`](crate::run_state::rotates_on_crash)).
    written_commit: bool,
    /// The copy's reservation (`raft-reserved`): no callback fires for an
    /// entry above it.
    reserved: tokio::sync::Mutex<Option<u64>>,
}

/// How this log keeps its marks at the data root.
struct Marking {
    marks: CopyMarks,
    /// The log mark is written with every flush, not in batches.
    exact: bool,
}

impl<C, K> Clone for RaftLog<C, K> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            types: std::marker::PhantomData,
        }
    }
}

impl<C, K> RaftLog<C, K>
where
    C: LogTypes,
    CommittedLeaderIdOf<C>: From<u64> + Deref<Target = u64>,
    K: EntryCodec<C>,
{
    /// Use `log` as a Raft log, with `codec` turning records into entries.
    ///
    /// Refused unless the log was opened with [`LogConfig::raft_log`]: any
    /// other log starts on a format with no generations and may delete
    /// entries by retention.
    ///
    /// Under [`FsyncMode::OnCommit`] an append's callback fires after its
    /// fsync. Under `Periodic` or `None` it fires once the entries are
    /// written and the copy's reservation covers them, so Raft counts an
    /// entry a power cut can still take. That is refused unless the log is
    /// under a root whose run rotates its storage id after a machine crash
    /// ([`run_state::rotates_on_crash`](crate::run_state::rotates_on_crash)),
    /// so a copy that loses a counted entry no longer votes. See
    /// `docs/per-shard-raft.md`, "Proposal: commit on a written majority".
    ///
    /// Refused when the copy's `raft-reserved` does not decode, on every
    /// open: nothing can rebuild it.
    pub fn new(log: DiskLog, codec: K) -> io::Result<Self> {
        Self::with_tail_budget(log, codec, TailBudget::process())
    }

    /// As [`Self::new`], with this log's tail cache counted against `budget`
    /// instead of the process's. Every log of one broker shares one.
    pub fn with_tail_budget(log: DiskLog, codec: K, budget: Arc<TailBudget>) -> io::Result<Self> {
        if !log.config().raft_log {
            return Err(invalid(format!(
                "{} was not opened as a Raft log",
                log.label()
            )));
        }
        let written_commit = log.config().fsync_mode != FsyncMode::OnCommit;
        if written_commit && !crate::run_state::rotates_on_crash(log.dir()) {
            return Err(invalid(format!(
                "{} is a Raft log under {:?} on a root whose run does not rotate on a crash; it must fsync on commit",
                log.label(),
                log.config().fsync_mode
            )));
        }
        let purged = purged::load(log.dir())?;
        let mut reserved = reserved::load(log.dir())?;
        if written_commit {
            // Raft counts every entry it finds at start as flushed, so the
            // reservation covers them before the log is handed over. A copy
            // opened on commit before, or one whose last raise failed after
            // its write, can hold entries above it.
            let tail = log.tail_now();
            if tail > 0 && reserved.is_none_or(|reserved| reserved < tail - 1) {
                let next = (tail - 1).saturating_add(reserved::STEP);
                reserved::store(log.dir(), next)?;
                reserved = Some(next);
            }
        }
        Ok(Self {
            shared: Arc::new(Shared {
                log,
                codec,
                positions: Positions::default(),
                bounds: parking_lot::Mutex::new(Bounds {
                    on_disk: purged,
                    hidden: None,
                }),
                covering: std::sync::OnceLock::new(),
                flushes: FlushQueue::default(),
                tail: Tail::new(budget),
                #[cfg(test)]
                pause_after_page: parking_lot::Mutex::new(None),
                #[cfg(test)]
                segment_reads: std::sync::atomic::AtomicU64::new(0),
                marking: std::sync::OnceLock::new(),
                written_commit,
                reserved: tokio::sync::Mutex::new(reserved),
            }),
            types: std::marker::PhantomData,
        })
    }

    /// Keep this copy's marks at the data root from here on: the vote mark
    /// is synced before [`RaftLogStorage::save_vote`] returns, and the log
    /// mark follows the flushed log as `mode` says. Set once, before the
    /// log is handed to Raft.
    ///
    /// [`RaftLogStorage::save_vote`]: openraft::storage::RaftLogStorage::save_vote
    pub fn mark_with(&self, marks: CopyMarks, mode: LogMarkMode) -> io::Result<()> {
        let exact = match mode {
            LogMarkMode::Exact => true,
            LogMarkMode::Batched(_) => false,
        };
        let batch = marks.clone();
        self.shared
            .marking
            .set(Marking { marks, exact })
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("{}: the log already keeps marks", self.shared.log.label()),
                )
            })?;
        if let LogMarkMode::Batched(interval) = mode {
            batch.batch_from(self.positions(), interval);
        }
        Ok(())
    }

    /// Mark the log as it was recovered: sync it, then raise the log mark
    /// to its last entry. Until the first append the batch has no flushed
    /// position to mark, so without this a reopened copy's mark could trail
    /// what it holds for as long as it takes no append. Call it after
    /// [`Self::mark_with`] and before the log is handed to Raft.
    pub async fn mark_recovered(&self) -> io::Result<()> {
        let Some(marking) = self.shared.marking.get() else {
            return Ok(());
        };
        let log = &self.shared.log;
        log.sync().await.map_err(writer::io_error)?;
        let last = last_point(log).await?;
        if last.is_some() {
            marking.marks.raise_log(last).await?;
        }
        Ok(())
    }

    /// The log's written and flushed positions, as they change.
    pub fn positions(&self) -> tokio::sync::watch::Receiver<LogPositions> {
        self.shared.positions.sent.subscribe()
    }

    /// Waiters on [`LogPositions::written`], keyed by `(term, index)`, woken
    /// only once the position reaches their key.
    pub fn written_waiters(&self) -> Waiters<(u64, u64)> {
        self.shared.positions.written.waiters()
    }

    /// Waiters on [`LogPositions::flushed`], as [`Self::written_waiters`].
    pub fn flushed_waiters(&self) -> Waiters<(u64, u64)> {
        self.shared.positions.flushed.waiters()
    }

    /// The first index the log holds or will hold, read live: one past a
    /// purge that is hidden or on disk, else where the oldest segment starts.
    pub fn first_index_reader(&self) -> Arc<dyn Fn() -> Offset + Send + Sync>
    where
        K: Send + Sync + 'static,
    {
        let shared = Arc::clone(&self.shared);
        Arc::new(move || shared.first_index())
    }

    /// The log underneath.
    pub fn disk_log(&self) -> &DiskLog {
        &self.shared.log
    }

    /// A reader of this log. It sees entries appended after it was made.
    pub fn reader(&self) -> LogReader<C, K> {
        LogReader::new(Arc::clone(&self.shared))
    }

    /// Refuse any purge past the snapshot `snapshots` holds.
    ///
    /// For a group whose state machine cannot rebuild what the log has
    /// dropped: one kept in memory, or a shard, whose state machine is the
    /// log. openraft purges only up to a snapshot it built or installed, so
    /// this refuses nothing it asks for; it is there for the case where the
    /// snapshot did not reach disk. Fails if the log is already purged past
    /// the snapshot, or a snapshot was already named.
    pub fn purge_within(&self, snapshots: &SnapshotStore) -> io::Result<()> {
        self.bound_purges(snapshots, PastSnapshot::Refuse)
    }

    fn bound_purges(&self, snapshots: &SnapshotStore, past: PastSnapshot) -> io::Result<()> {
        let log = &self.shared.log;
        let purged = self.shared.bounds.lock().on_disk;
        let covered = snapshots.covers().map(|last| last.index);
        if let Some(purged) = purged
            && covered.is_none_or(|covered| covered < purged.index)
        {
            return Err(invalid(format!(
                "{}: purged through index {}, past its snapshot, which covers {covered:?}",
                log.label(),
                purged.index
            )));
        }
        let covering = Covering {
            snapshots: snapshots.clone(),
            past,
        };
        self.shared.covering.set(covering).map_err(|_| {
            io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("{}: a snapshot already bounds its purges", log.label()),
            )
        })
    }

    /// The purged id and the last entry's id, as openraft's `get_log_state`
    /// wants them. The applied state is not consulted. A log with no entry
    /// above the purged id ends at the purged id.
    pub async fn log_state(&self) -> io::Result<LogState<C>> {
        let log = &self.shared.log;
        let purged = self.shared.purged_id::<C>()?;
        let first = self.shared.first_index();
        let last_log_id = match log.last_writer().await.map_err(io::Error::other)? {
            Some((last, writer)) if last >= first => {
                Some(log_id::<C>(term_at(writer, last)?, last))
            }
            _ => purged.clone(),
        };
        Ok(LogState {
            last_purged_log_id: purged,
            last_log_id,
        })
    }
}

/// Write `dir`'s purged id: the log starts after `index`, whose entry is
/// of generation `generation`. For a directory no log has open.
pub(crate) fn store_purged(
    dir: &std::path::Path,
    index: Offset,
    generation: u64,
) -> io::Result<()> {
    purged::store(dir, &purged::Purged { index, generation })
}

/// The index the log in `dir` restarts at after its purged id, if one is
/// saved.
pub(crate) fn first_after_purged(dir: &std::path::Path) -> io::Result<Option<Offset>> {
    Ok(purged::load(dir)?.map(|purged| purged.index + 1))
}

/// The last entry `log` holds as a Raft log, or the purged id when it holds
/// none above it. What a member opening the directory would report as its
/// last log id.
pub(crate) async fn last_point(log: &DiskLog) -> io::Result<Option<LogPoint>> {
    let purged = purged::load(log.dir())?;
    let first = match purged {
        Some(purged) => log.base_offset().max(purged.index + 1),
        None => log.base_offset(),
    };
    match log.last_writer().await.map_err(io::Error::other)? {
        Some((last, writer)) if last >= first => Ok(Some(LogPoint {
            term: term_at(writer, last)?,
            index: last,
        })),
        _ => purged
            .map(|purged| {
                Ok(LogPoint {
                    term: term_of(Some(purged.generation), purged.index)?,
                    index: purged.index,
                })
            })
            .transpose(),
    }
}

impl<K> Shared<K> {
    /// Wait for every queued flush. On a written-commit log a queued flush
    /// waits for a sync it does not start, and under `None` none comes, so
    /// this syncs first.
    async fn drain_flushes(&self) -> io::Result<()> {
        if self.written_commit {
            self.log.sync().await.map_err(writer::io_error)?;
        }
        self.flushes.drained().await;
        Ok(())
    }

    /// Raise the reservation, synced, to [`reserved::STEP`] past `index`
    /// unless it already covers `index`. Never lowered.
    async fn reserve_through(&self, index: u64) -> io::Result<()> {
        let mut reserved = self.reserved.lock().await;
        if reserved.is_some_and(|reserved| reserved >= index) {
            return Ok(());
        }
        let next = index.saturating_add(reserved::STEP);
        let dir = self.log.dir().to_path_buf();
        tokio::task::spawn_blocking(move || reserved::store(&dir, next))
            .await
            .map_err(io::Error::other)??;
        *reserved = Some(next);
        Ok(())
    }

    /// The index of the first entry the log holds or will hold: one past the
    /// purged id, or where the oldest segment starts when nothing was
    /// purged. Records below it are never read as entries.
    fn first_index(&self) -> Offset {
        let base = self.log.base_offset();
        match self.bounds.lock().purged() {
            Some(purged) => base.max(purged.index + 1),
            None => base,
        }
    }

    fn purged_id<C>(&self) -> io::Result<Option<LogIdOf<C>>>
    where
        C: LogTypes,
        CommittedLeaderIdOf<C>: From<u64> + Deref<Target = u64>,
    {
        let Some(purged) = self.bounds.lock().purged() else {
            return Ok(None);
        };
        Ok(Some(log_id::<C>(
            term_of(Some(purged.generation), purged.index)?,
            purged.index,
        )))
    }
}

fn log_id<C>(term: u64, index: Offset) -> LogIdOf<C>
where
    C: LogTypes,
    CommittedLeaderIdOf<C>: From<u64> + Deref<Target = u64>,
{
    LogIdOf::<C>::new(CommittedLeaderIdOf::<C>::from(term), index)
}

/// The generation that stands for `term`, in a record header and in the
/// ballot: the term plus one, so that no term is stored as
/// [`UNKNOWN_WRITER`]. The inverse of [`term_of_generation`]; anything that
/// moves between Raft terms and generations goes through this pair.
pub fn generation_of(term: u64) -> io::Result<u64> {
    term.checked_add(1)
        .ok_or_else(|| invalid(format!("term {term} has no generation")))
}

/// The term `generation` stands for, or `None` for [`UNKNOWN_WRITER`], which
/// stands for no term. The inverse of [`generation_of`].
pub fn term_of_generation(generation: u64) -> Option<u64> {
    generation.checked_sub(1)
}

/// The term of the record at `offset`, from the generation in its header.
fn term_of(generation: Option<u64>, offset: Offset) -> io::Result<u64> {
    match generation {
        None => Err(invalid(format!(
            "the record at offset {offset} is in a segment that keeps no generation, so it has no term"
        ))),
        Some(generation) => term_of_generation(generation).ok_or_else(|| {
            invalid(format!(
                "the writer of the record at offset {offset} is unknown, so it has no term"
            ))
        }),
    }
}

fn term_at(writer: WriterAt, offset: Offset) -> io::Result<u64> {
    match writer {
        WriterAt::Absent => Err(invalid(format!(
            "the log holds no record at offset {offset}"
        ))),
        WriterAt::Unstamped => term_of(None, offset),
        WriterAt::Generation(generation) => term_of(Some(generation), offset),
    }
}

fn invalid(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
