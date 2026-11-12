//! openraft's `RaftLogReader` over a [`DiskLog`].

use std::fmt::Debug;
use std::io;
use std::ops::{Bound, Deref, RangeBounds};
use std::sync::Arc;

use openraft::storage::RaftLogReader;
use openraft::type_config::alias::{CommittedLeaderIdOf, VoteOf};
use openraft::vote::{RaftLeaderId, RaftVote};
use openraft::{OptionalSend, RaftTypeConfig};

use super::codec::entry_from_record;
use super::{EntryCodec, LogTypes, Shared, invalid, term_of_generation};
use crate::log::{AppendOnlyLog, Offset, ReadRange};

/// Payload bytes one read of the log asks for. A range larger than this is
/// read in pages.
const PAGE_BYTES: usize = 4 << 20;

/// Reads entries and the vote. Made by [`super::RaftLog::reader`].
pub struct LogReader<C, K> {
    shared: Arc<Shared<K>>,
    types: std::marker::PhantomData<fn() -> C>,
}

impl<C, K> Clone for LogReader<C, K> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            types: std::marker::PhantomData,
        }
    }
}

impl<C, K> LogReader<C, K>
where
    C: LogTypes,
    CommittedLeaderIdOf<C>: From<u64> + Deref<Target = u64>,
    K: EntryCodec<C>,
{
    pub(super) fn new(shared: Arc<Shared<K>>) -> Self {
        Self {
            shared,
            types: std::marker::PhantomData,
        }
    }

    /// Every entry the log holds in `start..end`, or, with `whole` false,
    /// the first page of them. Entries missing at either end of the range
    /// are left out; a gap between two entries is an error.
    async fn entries(&self, start: Offset, end: Offset, whole: bool) -> io::Result<Vec<C::Entry>> {
        let log = &self.shared.log;
        'read: loop {
            // A cut between two pages could join records from before it to
            // ones written after it, so the read starts again.
            let cuts = log.cuts();
            let first = self.shared.first_index();
            let mut next = start.max(first);
            let mut entries = Vec::new();
            while next < end {
                let page = log
                    .read_range_with_trims(ReadRange {
                        start: next,
                        max_bytes: PAGE_BYTES,
                    })
                    .await;
                // Checked before the error: a head removed under the read
                // fails it as `Trimmed`, and openraft takes a range missing
                // its start, so the read starts again above the new head.
                if log.cuts() != cuts {
                    continue 'read;
                }
                let page = page.map_err(io::Error::other)?;
                if page.is_empty() {
                    break;
                }
                for record in page {
                    if record.offset >= end {
                        return Ok(entries);
                    }
                    if record.offset != next {
                        return Err(invalid(format!(
                            "{}: the log holds no record at offset {next}, below one at {}",
                            log.label(),
                            record.offset
                        )));
                    }
                    entries.push(entry_from_record::<C, K>(
                        &self.shared.codec,
                        record,
                        first,
                    )?);
                    next += 1;
                }
                if !whole {
                    break;
                }
                #[cfg(test)]
                let pause = self.shared.pause_after_page.lock().take();
                #[cfg(test)]
                if let Some((reached, resume)) = pause {
                    let _ = reached.send(());
                    let _ = resume.await;
                }
            }
            return Ok(entries);
        }
    }
}

impl<C, K> RaftLogReader<C> for LogReader<C, K>
where
    C: LogTypes,
    CommittedLeaderIdOf<C>: From<u64> + Deref<Target = u64>,
    K: EntryCodec<C>,
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, io::Error> {
        let (start, end) = bounds(&range);
        self.entries(start, end, true).await
    }

    async fn limited_get_log_entries(
        &mut self,
        start: u64,
        end: u64,
    ) -> Result<Vec<C::Entry>, io::Error> {
        self.entries(start, end, false).await
    }

    /// The vote as the ballot file holds it: the accepted generation is the
    /// vote's term plus one, and the leader it names is the node voted for.
    async fn read_vote(&mut self) -> Result<Option<VoteOf<C>>, io::Error> {
        let log = &self.shared.log;
        match log.accepted_vote() {
            (0, None, _) => Ok(None),
            // An empty leader is a generation given up, not a vote.
            (generation, Some(leader), committed) if !leader.is_empty() => {
                let Some(term) = term_of_generation(generation) else {
                    return Err(invalid(format!(
                        "{}: a vote for {leader:?} was accepted at no generation",
                        log.label()
                    )));
                };
                let node_id = self.shared.codec.decode_node_id(&leader)?;
                let leader_id = <C as RaftTypeConfig>::LeaderId::new(term, node_id);
                Ok(Some(VoteOf::<C>::from_leader_id(leader_id, committed)))
            }
            (generation, leader, _) => Err(invalid(format!(
                "{}: generation {generation} was accepted from {leader:?}, which is no vote",
                log.label()
            ))),
        }
    }
}

/// `range` as a first index and one past the last.
fn bounds(range: &impl RangeBounds<u64>) -> (Offset, Offset) {
    let start = match range.start_bound() {
        Bound::Included(start) => *start,
        Bound::Excluded(start) => start.saturating_add(1),
        Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
        Bound::Included(end) => end.saturating_add(1),
        Bound::Excluded(end) => *end,
        Bound::Unbounded => Offset::MAX,
    };
    (start, end)
}
