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
use super::{
    EntryCodec, LogReader, LogTypes, RaftLog, first_index, generation_of, invalid, log_id, term_at,
    term_of_generation,
};
#[cfg(doc)]
use crate::DiskLog;
use crate::StorageError;
use crate::disk_log::WriterAt;
use crate::log::{AppendOnlyLog, Epoch, WrittenBy};

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
    async fn read_committed(&mut self) -> io::Result<Option<LogIdOf<C>>> {
        let log = &self.shared.log;
        let (offset, saved) = log.committed();
        let Some(index) = offset.checked_sub(1) else {
            return Ok(None);
        };
        if index < first_index(log) {
            return Ok(None);
        }
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
        let mut records = Vec::new();
        // One run per term, as the segment stamps them.
        let mut writers: Vec<Epoch> = Vec::new();
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
            if writers
                .last()
                .is_none_or(|run| run.generation != generation)
            {
                writers.push(Epoch {
                    generation,
                    start_offset: index,
                });
            }
            records.push(record);
        }
        let Some(first) = next.map(|next| next - records.len() as u64) else {
            callback.io_completed(Ok(()));
            return Ok(());
        };

        // Taken before the entries are written and held until the callback
        // has fired, so a truncation waits for it.
        let flushing = Arc::clone(&shared.flushing).read_owned().await;
        let tail = log.tail_offset().await.map_err(io_error)?;
        if first > tail && tail == first_index(log) {
            log.reset_to(first).await.map_err(io_error)?;
        }
        let pending = log
            .append_pending_at(first, &records, WrittenBy::Writers(writers))
            .await
            .map_err(io_error)?
            .ok_or_else(|| {
                invalid(format!(
                    "{}: an append at index {first} does not continue the log, which ends at {tail}",
                    log.label()
                ))
            })?;

        let shared = Arc::clone(shared);
        tokio::spawn(async move {
            let flushed = shared.log.commit(&pending).await.map_err(io_error);
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
        let keep = match last_log_id {
            None => first_index(log),
            Some(id) => {
                let index = id.index();
                let writer = log.writer_at(index).await.map_err(io_error)?;
                if index >= first_index(log) && writer != WriterAt::Absent {
                    let term = term_at(writer, index)?;
                    if term != **id.committed_leader_id() {
                        return Err(invalid(format!(
                            "{}: asked to keep through index {index} of term {}, but the entry there is of term {term}",
                            log.label(),
                            **id.committed_leader_id()
                        )));
                    }
                }
                index + 1
            }
        };
        log.truncate(keep).await.map_err(io_error)
    }

    /// Not built yet: purge arrives with snapshots.
    async fn purge(&mut self, _log_id: LogIdOf<C>) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "the Raft log does not purge yet",
        ))
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
