//! openraft's `RaftLogStorage` over a [`DiskLog`]: append, truncate and the
//! vote.
//!
//! openraft serializes these calls, which is what lets each one read the
//! tail and then act on it.

use std::io;
use std::ops::Deref;

use openraft::OptionalSend;
use openraft::entry::RaftEntry;
use openraft::storage::{IOFlushed, LogState, RaftLogStorage};
use openraft::type_config::alias::{CommittedLeaderIdOf, LogIdOf, VoteOf};
use openraft::vote::{RaftLeaderId, RaftVote};

use super::codec::record_from_entry;
use super::flush_queue::FlushQueue;
use super::purged::Purged;
use super::{
    EntryCodec, LogReader, LogTypes, RaftLog, generation_of, invalid, log_id, term_at,
    term_of_generation,
};
#[cfg(doc)]
use crate::DiskLog;
use crate::StorageError;
use crate::disk_log::WriterAt;
use crate::log::{
    AppendOnlyLog, AppendRecord, Epoch, LogRecord, Offset, UNKNOWN_WRITER, WrittenBy,
};
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
    /// `callback` fires once the log's fsync policy is
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
        // Only an append larger than a segment fills `earlier`.
        let mut earlier: Vec<Batch> = Vec::new();
        let mut batch = Batch::default();
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
            if batch.records.is_empty() {
                batch.first = index;
            } else if batch.bytes + bytes > batch_bytes {
                earlier.push(std::mem::replace(&mut batch, Batch::starting_at(index)));
            }
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
        if batch.records.is_empty() {
            callback.io_completed(Ok(()));
            return Ok(());
        }
        let first = earlier.first().map_or(batch.first, |batch| batch.first);

        let tail = match shared.hides() {
            Some(snapshots) => {
                shared.admit(snapshots, first).await?;
                log.tail_now()
            }
            None => {
                let tail = log.tail_now();
                let purged = shared.bounds.lock().on_disk;
                // No entry above the purged id: the log restarts where this
                // append begins. That is right after the purged id when
                // there is one, and anywhere when nothing was ever purged.
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
                tail
            }
        };
        // Read before the write: see `TailCache`.
        let cuts = log.cuts();
        let mut last = None;
        for batch in earlier.into_iter().chain([batch]) {
            let pending = log
                .append_pending_at(
                    batch.first,
                    &batch.records,
                    WrittenBy::Writers(batch.writers.clone()),
                )
                .await
                .map_err(io_error)?
                .ok_or_else(|| {
                    invalid(format!(
                        "{}: an append at index {} does not continue the log, which ended at {tail}",
                        log.label(),
                        batch.first
                    ))
                })?;
            shared.tail.lock().extend(cuts, batch.into_log_records());
            last = Some(pending);
        }
        // A flush covers a prefix of the log, so the last batch's wait
        // stands for the append.
        let last = last.expect("at least one batch");

        // Fired here only when no earlier callback is still to come, so
        // callbacks keep the order of their appends.
        let flushed = if shared.flushes.is_empty() {
            log.commit_now(&last)
        } else {
            None
        };
        match flushed {
            Some(flushed) => callback.io_completed(flushed.map_err(io_error)),
            None => FlushQueue::push(
                shared,
                last,
                Box::new(move |flushed| callback.io_completed(flushed)),
            ),
        }
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
        self.shared.flushes.drained().await;
        // Before the cut, so no read finds here an entry the log has lost.
        self.shared.tail.lock().clear();
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
        if keep >= log.tail_now() {
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
    /// under [`RaftLog::purge_within`], past the stored snapshot. Under
    /// [`RaftLog::purge_hides_past`] an id past the stored snapshot is
    /// removed up to the snapshot and hidden from there. An id at or below
    /// the purged id removes nothing new.
    async fn purge(&mut self, log_id: LogIdOf<C>) -> io::Result<()> {
        let shared = &self.shared;
        let log = &shared.log;
        // A purge past the end drops entries a flush may still be waiting
        // on, as a truncation does.
        shared.flushes.drained().await;
        let index = log_id.index();
        let term = **log_id.committed_leader_id();
        let purged = Purged {
            index,
            generation: generation_of(term)?,
        };
        if let Some(snapshots) = shared.hides() {
            return shared.purge_hiding(snapshots, purged).await;
        }
        if shared
            .bounds
            .lock()
            .on_disk
            .is_some_and(|on_disk| index <= on_disk.index)
        {
            return Ok(());
        }
        if let Some(covering) = shared.covering.get() {
            let covered = covering.snapshots.covers().map(|last| last.index);
            if covered.is_none_or(|covered| covered < index) {
                return Err(invalid(format!(
                    "{}: asked to purge through index {index}, past the stored snapshot, which covers {covered:?}",
                    log.label()
                )));
            }
        }
        shared.check_held(purged).await?;
        shared.remove_through(purged).await
    }
}

impl<C, K> RaftLog<C, K>
where
    C: LogTypes,
    CommittedLeaderIdOf<C>: From<u64> + Deref<Target = u64>,
    K: EntryCodec<C>,
{
    /// Whether the log holds nothing a member could have voted from: no
    /// vote, no record and no purged id. A member that never voted leaves
    /// this, and so does a log directory that was emptied.
    pub async fn is_blank(&self) -> io::Result<bool> {
        let log = &self.shared.log;
        let tail = log.tail_offset().await.map_err(io_error)?;
        Ok(matches!(log.accepted_vote(), (0, None, _))
            && self.shared.bounds.lock().on_disk.is_none()
            && tail == log.base_offset())
    }
}

/// Entries of one append that go to the log as one batch.
#[derive(Default)]
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
            ..Self::default()
        }
    }

    /// The records as a read of the log returns them, but for the
    /// checksum, which only the segment holds.
    fn into_log_records(self) -> impl Iterator<Item = LogRecord> {
        let Self { first, writers, .. } = self;
        let mut runs = writers.into_iter().peekable();
        let mut generation = UNKNOWN_WRITER;
        self.records
            .into_iter()
            .zip(first..)
            .map(move |(record, offset)| {
                while let Some(run) = runs.next_if(|run| run.start_offset <= offset) {
                    generation = run.generation;
                }
                LogRecord {
                    offset,
                    timestamp_micros: record.timestamp_micros,
                    checksum: 0,
                    payload: record.payload,
                    mark: record.mark,
                    publisher: record.publisher,
                    headers: record.headers,
                    generation: Some(generation),
                }
            })
    }
}

/// A storage error as openraft takes it, keeping an I/O error's kind.
pub(super) fn io_error(err: StorageError) -> io::Error {
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
