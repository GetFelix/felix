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
//! Three rules hold everywhere in this module:
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
//!   it can remain. They are never read as entries.
//!
//! openraft types stay inside this module and the engine that drives it.

mod codec;
mod purged;
mod reader;
mod snapshot;
mod state_file;
#[cfg(test)]
mod tests;
mod writer;

pub use codec::EntryCodec;
pub use reader::LogReader;
pub use snapshot::{SnapshotId, SnapshotStore, StoredSnapshot};

use std::io;
use std::ops::Deref;
use std::sync::Arc;

use openraft::RaftTypeConfig;
use openraft::entry::{Entry, EntryPayload};
use openraft::storage::LogState;
use openraft::type_config::alias::{CommittedLeaderIdOf, LogIdOf};

use self::purged::Purged;
use crate::DiskLog;
use crate::disk_log::WriterAt;
use crate::log::Offset;
#[cfg(doc)]
use crate::log::{LogConfig, RecordMark, UNKNOWN_WRITER};

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

struct Shared<K> {
    log: DiskLog,
    codec: K,
    /// The last purged entry, as `raft-purged` holds it.
    purged: parking_lot::Mutex<Option<Purged>>,
    /// Set by [`RaftLog::purge_within`]: a purge stops at this snapshot.
    covering: std::sync::OnceLock<SnapshotStore>,
    /// Read-held by each append until its callback has fired, and
    /// write-held by a truncation.
    flushing: Arc<tokio::sync::RwLock<()>>,
    /// Holds the next paged read after its first page: the read signals the
    /// sender, then waits for the receiver.
    #[cfg(test)]
    pause_after_page: parking_lot::Mutex<Option<tests::PagePause>>,
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
    pub fn new(log: DiskLog, codec: K) -> io::Result<Self> {
        if !log.config().raft_log {
            return Err(invalid(format!(
                "{} was not opened as a Raft log",
                log.label()
            )));
        }
        let purged = purged::load(log.dir())?;
        Ok(Self {
            shared: Arc::new(Shared {
                log,
                codec,
                purged: parking_lot::Mutex::new(purged),
                covering: std::sync::OnceLock::new(),
                flushing: Arc::default(),
                #[cfg(test)]
                pause_after_page: parking_lot::Mutex::new(None),
            }),
            types: std::marker::PhantomData,
        })
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
        let log = &self.shared.log;
        let purged = *self.shared.purged.lock();
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
        self.shared.covering.set(snapshots.clone()).map_err(|_| {
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

impl<K> Shared<K> {
    /// The index of the first entry the log holds or will hold: one past the
    /// purged id, or where the oldest segment starts when nothing was
    /// purged. Records below it are never read as entries.
    fn first_index(&self) -> Offset {
        let base = self.log.base_offset();
        match *self.purged.lock() {
            Some(purged) => base.max(purged.index + 1),
            None => base,
        }
    }

    fn purged_id<C>(&self) -> io::Result<Option<LogIdOf<C>>>
    where
        C: LogTypes,
        CommittedLeaderIdOf<C>: From<u64> + Deref<Target = u64>,
    {
        let Some(purged) = *self.purged.lock() else {
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
