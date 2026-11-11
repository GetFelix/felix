//! openraft's `RaftLogStorage` over a [`DiskLog`]: append, truncate and the
//! vote.
//!
//! openraft serializes these calls, which is what lets each one read the
//! tail and then act on it.

use std::io;
use std::ops::Deref;
use std::sync::Arc;

use openraft::OptionalSend;
use openraft::entry::RaftEntry;
use openraft::storage::{IOFlushed, LogState, RaftLogStorage};
use openraft::type_config::alias::{CommittedLeaderIdOf, LogIdOf, VoteOf};
use openraft::vote::{RaftLeaderId, RaftVote};

use super::codec::record_from_entry;
use super::purged::{self, Purged};
use super::{
    EntryCodec, LogReader, LogTypes, RaftLog, generation_of, invalid, log_id, term_at,
    term_of_generation,
};
#[cfg(doc)]
use crate::DiskLog;
use crate::StorageError;
use crate::disk_log::WriterAt;
use crate::log::{AppendOnlyLog, AppendRecord, Epoch, Offset, WrittenBy};
use crate::segment::{GENERATION_VERSION, SEGMENT_HEADER_LEN, record_header_len};

impl<C, K> RaftLogStorage<C> for RaftLog<C, K>
where
    C: LogTypes,
    CommittedLeaderIdOf<C>: From<u64> + Deref<Target = u64>,
    K: EntryCodec<C>,
{
    type LogReader = LogReader<C, K>;

    async fn get_log_state(&mut self) -> io::Result<LogState<C>> {
        self.log_state().await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.reader()
    }

    /// On disk before this returns. See [`DiskLog::save_vote`] for what is
    /// refused.
    async fn save_vote(&mut self, vote: &VoteOf<C>) -> io::Result<()> {
        let leader_id = vote.leader_id();
        let generation = generation_of(leader_id.term())?;
        let leader = self.shared.codec.encode_node_id(leader_id.node_id());
        self.shared
            .log
            .save_vote(generation, &leader, vote.is_committed())
            .await
            .map_err(io_error)
    }

    /// Kept as the log's commit offset with the id's term beside it, written
    /// behind the call: after a crash the id read back may be older, never
    /// newer.
    async fn save_committed(&mut self, committed: Option<LogIdOf<C>>) -> io::Result<()> {
        match committed {
            None => Ok(()),
            Some(id) => self
                .shared
                .log
                .save_committed(id.index(), generation_of(**id.committed_leader_id())?)
                .await
                .map_err(io_error),
        }
    }

    /// The last committed id saved, and only while the log holds that
    /// entry. An open lowers a saved id whose entry a crash took before it
    /// was flushed, and drops one whose index holds another term's entry.
    /// A mismatch found here, on an open log, is an error.
    ///
    /// Only committed entries are purged, so a saved id at or below the
    /// purged id, or none at all, reads back as the purged id.
    async fn read_committed(&mut self) -> io::Result<Option<LogIdOf<C>>> {
        let log = &self.shared.log;
        let (offset, saved) = log.committed();
        let index = match offset.checked_sub(1) {
            Some(index) if index >= self.shared.first_index() => index,
            _ => return self.shared.purged_id::<C>(),
        };
        let writer = log.writer_at(index).await.map_err(io_error)?;
        let term = term_at(writer, index)?;
        if generation_of(term)? != saved {
            return Err(invalid(format!(
                "{}: index {index} was saved as committed at {}, but the entry there is of term {term}",
                log.label(),
                match term_of_generation(saved) {
                    Some(term) => format!("term {term}"),
                    None => "no term".to_string(),
                }
            )));
        }
        Ok(Some(log_id::<C>(term, index)))
    }

    /// Returns once the entries are written and a reader sees them.
    /// `callback` fires from another task, once the log's fsync policy is
    /// met for everything up to the last of them: under
    /// `FsyncMode::OnCommit`, once a flush has covered them. A flush covers
    /// a prefix of the log, so nothing below them is left out.
    ///
    /// The first entry must be the next index. An empty log takes any first
    /// index and starts there.
    async fn append<I>(&mut self, entries: I, callback: IOFlushed<C>) -> io::Result<()>
    where
        I: IntoIterator<Item = C::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let shared = &self.shared;
        let log = &shared.log;
        let now = now_micros();
        // The log writes a batch into one segment whole, so the entries go
        // down in batches of about a segment each.
        let batch_bytes = log
            .config()
            .segment_size_bytes
            .saturating_sub(SEGMENT_HEADER_LEN);
        let record_header = record_header_len(GENERATION_VERSION);
        let mut batches: Vec<Batch> = Vec::new();
        let mut next = None;
        for entry in entries {
            let index = entry.index();
            if next.is_some_and(|next| next != index) {
                return Err(invalid(format!(
                    "{}: an append skips to index {index}",
                    log.label()
                )));
            }
            next = Some(index + 1);
            let (record, generation) = record_from_entry::<C, K>(&shared.codec, &entry, now)?;
            let bytes = record_header
                + record.payload.len() as u64
                + record
                    .headers
                    .as_ref()
                    .map_or(0, |headers| headers.len() as u64);
            if batches
                .last()
                .is_none_or(|batch| batch.bytes + bytes > batch_bytes)
            {
                batches.push(Batch::starting_at(index));
            }
            let batch = batches.last_mut().expect("pushed above");
            batch.bytes += bytes;
            // One run per term, as the segment stamps them.
            if batch
                .writers
                .last()
                .is_none_or(|run| run.generation != generation)
            {
                batch.writers.push(Epoch {
                    generation,
                    start_offset: index,
                });
            }
            batch.records.push(record);
        }
        let Some(first) = batches.first().map(|batch| batch.first) else {
            callback.io_completed(Ok(()));
            return Ok(());
        };

        // Taken before the entries are written and held until the callback
        // has fired, so a truncation waits for it.
        let flushing = Arc::clone(&shared.flushing).read_owned().await;
        let tail = log.tail_offset().await.map_err(io_error)?;
        let purged = *shared.purged.lock();
        // No entry above the purged id: the log restarts where this append
        // begins. That is right after the purged id when there is one, and
        // anywhere when nothing was ever purged.
        if tail <= shared.first_index() {
            if purged.is_some_and(|purged| purged.index + 1 != first) {
                return Err(invalid(format!(
                    "{}: an append at index {first} does not follow the purged id",
                    log.label()
                )));
            }
            if first > tail {
                log.reset_to(first).await.map_err(io_error)?;
            }
        }
        let mut written = Vec::with_capacity(batches.len());
        for batch in batches {
            let pending = log
                .append_pending_at(batch.first, &batch.records, WrittenBy::Writers(batch.writers))
                .await
                .map_err(io_error)?
                .ok_or_else(|| {
                    invalid(format!(
                        "{}: an append at index {} does not continue the log, which ended at {tail}",
                        log.label(),
                        batch.first
                    ))
                })?;
            written.push(pending);
        }

        let shared = Arc::clone(shared);
        tokio::spawn(async move {
            let mut flushed = Ok(());
            for pending in &written {
                flushed = shared.log.commit(pending).await.map_err(io_error);
                if flushed.is_err() {
                    break;
                }
            }
            callback.io_completed(flushed);
            drop(flushing);
        });
        Ok(())
    }

    /// Cut every entry after `last_log_id`, or every entry with `None`. The
    /// cut is on disk before this returns, newest segment first, so a crash
    /// part way leaves a longer log and never a gap.
    ///
    /// Refused when the log holds another term's entry at that index: the
    /// caller is not describing this log.
    async fn truncate_after(&mut self, last_log_id: Option<LogIdOf<C>>) -> io::Result<()> {
        let log = &self.shared.log;
        // A flush still waiting on entries this cuts would never reach them
        // and would report the append failed. Each one finishes first.
        let _flushed = self.shared.flushing.write().await;
        let first = self.shared.first_index();
        let keep = match last_log_id {
            None => first,
            Some(id) => {
                let index = id.index();
                let asked = **id.committed_leader_id();
                let held = if index >= first {
                    match log.writer_at(index).await.map_err(io_error)? {
                        WriterAt::Absent => None,
                        writer => Some(term_at(writer, index)?),
                    }
                } else {
                    // Below the log: only the purged id itself can be named.
                    match self.shared.purged_id::<C>()? {
                        Some(purged) if purged.index() == index => {
                            Some(**purged.committed_leader_id())
                        }
                        _ => {
                            return Err(invalid(format!(
                                "{}: asked to keep through index {index}, below the log's first index {first}",
                                log.label()
                            )));
                        }
                    }
                };
                if held.is_some_and(|term| term != asked) {
                    return Err(invalid(format!(
                        "{}: asked to keep through index {index} of term {asked}, but the entry there is of term {}",
                        log.label(),
                        held.unwrap_or_default()
                    )));
                }
                index + 1
            }
        };
        // After a purge past the log's end the first index is above the
        // tail, and there is nothing to cut.
        if keep >= log.tail_offset().await.map_err(io_error)? {
            return Ok(());
        }
        log.truncate(keep).await.map_err(io_error)
    }

    /// Drop every entry up to and including `log_id`. The id is on disk
    /// before anything is deleted, and from then on the log starts after
    /// it. Segments wholly at or below it are deleted, oldest first, each
    /// unlink made durable before the next, so a crash part way leaves a
    /// log with no gap.
    ///
    /// An id past the log's end empties the log, which then starts right
    /// after the id. openraft asks for that at startup when the state
    /// machine is ahead of the log.
    ///
    /// Refused when the log holds another term's entry at that index, and,
    /// under [`RaftLog::purge_within`], past the stored snapshot. An id at
    /// or below the purged id does nothing.
    async fn purge(&mut self, log_id: LogIdOf<C>) -> io::Result<()> {
        let shared = &self.shared;
        let log = &shared.log;
        // A purge past the end drops entries a flush may still be waiting
        // on, as a truncation does.
        let _flushed = shared.flushing.write().await;
        let index = log_id.index();
        let term = **log_id.committed_leader_id();
        if shared
            .purged
            .lock()
            .is_some_and(|purged| index <= purged.index)
        {
            return Ok(());
        }
        if let Some(snapshots) = shared.covering.get() {
            let covered = snapshots.covers().map(|last| last.index);
            if covered.is_none_or(|covered| covered < index) {
                return Err(invalid(format!(
                    "{}: asked to purge through index {index}, past the stored snapshot, which covers {covered:?}",
                    log.label()
                )));
            }
        }
        let tail = log.tail_offset().await.map_err(io_error)?;
        if index >= shared.first_index() && index < tail {
            let held = term_at(log.writer_at(index).await.map_err(io_error)?, index)?;
            if held != term {
                return Err(invalid(format!(
                    "{}: asked to purge through index {index} of term {term}, but the entry there is of term {held}",
                    log.label()
                )));
            }
        }

        let purged = Purged {
            index,
            generation: generation_of(term)?,
        };
        let dir = log.dir().to_path_buf();
        felix_common::blocking::spawn(move || purged::store(&dir, &purged))
            .await
            .map_err(io::Error::other)??;
        *shared.purged.lock() = Some(purged);

        let first: Offset = index + 1;
        if first >= tail {
            log.reset_to(first).await.map_err(io_error)
        } else {
            log.trim_before(first).await.map(drop).map_err(io_error)
        }
    }
}

/// Entries of one append that go to the log as one batch.
struct Batch {
    first: Offset,
    records: Vec<AppendRecord>,
    writers: Vec<Epoch>,
    bytes: u64,
}

impl Batch {
    fn starting_at(first: Offset) -> Self {
        Self {
            first,
            records: Vec::new(),
            writers: Vec::new(),
            bytes: 0,
        }
    }
}

/// A storage error as openraft takes it, keeping an I/O error's kind.
fn io_error(err: StorageError) -> io::Error {
    match err {
        StorageError::Io(err) => err,
        other => io::Error::other(other),
    }
}

/// The timestamp of a blank or membership record. Commands carry their own.
fn now_micros() -> u64 {
    felix_common::clock::system_time()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_micros().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}
