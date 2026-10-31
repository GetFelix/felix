//! Conditional head trims: hiding a durable stream's history below an offset,
//! only if the stream's tail is the one the caller expects.
//!
//! A trim is a record in the shard's log. It takes its place in the stream's
//! order exactly as a conditional publish does, offsets first and durability
//! after, and reaches the replay ring in its turn, where it drops the entries
//! below its floor instead of being appended. See `docs/durable-storage.md`,
//! "Conditional head trim".

use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;

use super::Broker;
use super::publish::Append;
use super::shards::StreamHandle;
use super::subscribe::{COMMIT_RECHECK, not_readable};
use crate::ConsistencyLevel;
use crate::durable::StreamLog;
use crate::error::{BrokerError, NotReadable, Result};
use crate::stream::ReadBound;

/// Bytes read per page while looking for atomic commits before a trim.
const COMMIT_SCAN_PAGE_BYTES: usize = 1 << 20;

/// How long a trim waits for the committed mark or the replay ring before it
/// answers [`BrokerError::TrimTimedOut`].
pub(crate) const TRIM_WAIT: std::time::Duration = std::time::Duration::from_secs(30);

/// What a head trim did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TrimOutcome {
    /// Where the trim record landed, so the next tail is one past it.
    /// `None` when the floor already stood at or past `before` and nothing
    /// was written.
    pub offset: Option<u64>,
    /// Where reads start once the trim is acknowledged.
    pub oldest: u64,
}

impl Broker {
    /// Refuse reads of the shard below `before`, only if its next offset is
    /// exactly `expected`, and delete the sealed segments below that floor
    /// in the background.
    ///
    /// The trim record lands at `expected`, so `before` may be at most that
    /// ([`BrokerError::TrimPastTail`]). A tail other than `expected` is
    /// [`BrokerError::OffsetMismatch`], with nothing written, checked before
    /// anything else: a trim at or below the current floor writes nothing
    /// and answers with the floor, but a stale one is still refused.
    ///
    /// The record goes through the commit order like a publish: offsets
    /// first, then durability, then the ring in its turn, where entries
    /// below the floor are dropped. It is answered only once the ring has
    /// it, so no reader is served below the floor after the answer. On a
    /// `Quorum` stream that waits for the committed mark to pass the record,
    /// and a no-op answer waits for the mark to reach `expected`.
    ///
    /// Refused on a broker with replication enabled
    /// ([`BrokerError::TrimNotReplicatedYet`]) until replication ships trim
    /// records. The waits for the committed mark and the ring are bounded by
    /// [`BrokerError::TrimTimedOut`].
    ///
    /// A trim whose caller stopped waiting may still have landed, and a
    /// retry with the same `expected` is then refused as an offset mismatch.
    /// `headers` are stored with the record and returned with
    /// [`BrokerError::CursorTooOld`] to readers below the floor it sets.
    /// Refused on a stream with no log, and on one holding atomic commits,
    /// whose state lives in the records a trim removes.
    pub async fn trim_before_at(
        &self,
        handle: &StreamHandle,
        before: u64,
        expected: u64,
        headers: Bytes,
    ) -> Result<TrimOutcome> {
        let Some(log) = &handle.state.durable else {
            return Err(BrokerError::TrimNeedsDurableStream);
        };
        // Replication cannot ship a trim record until the wire carries one,
        // and a follower would stop at it. A process-start fact rather than
        // the shard's live route, which is empty during startup and moves, so
        // a follower added mid-trim cannot slip past it.
        if self.replication_enabled() {
            return Err(BrokerError::TrimNotReplicatedYet);
        }
        if before > expected {
            return Err(BrokerError::TrimPastTail {
                before,
                offset: expected,
            });
        }
        // The floor before the tail: the floor only rises, so if the tail is
        // still `expected` when it is read, the no-op answer held at that
        // moment.
        let floor = log.head_trim().map_or(0, |trim| trim.floor);
        let tail = log.tail_offset().await?;
        if tail != expected {
            return Err(BrokerError::OffsetMismatch { expected, tail });
        }
        if before <= floor {
            // Answered only once what it stands on is committed, as a write
            // would be: past the mark, the floor and the tail can still be
            // lost at a failover.
            await_committed(handle, expected, self.trim_wait).await?;

            return Ok(TrimOutcome {
                offset: None,
                oldest: log.readable_base(),
            });
        }
        refuse_commit_stream(handle, log, expected).await?;
        let claimed = self
            .claim(
                handle,
                std::slice::from_ref(&headers),
                Append::Trim { before, expected },
                None,
            )
            .await?
            .expect("a trim claims or is refused");
        let (trim_applied, applied) = tokio::sync::oneshot::channel();
        let outcome = self.complete_claimed(claimed, Some(trim_applied)).await?;
        let offset = outcome.offsets.map(|(first, _)| first);
        // A held trim is answered before its floor reaches the ring, and a
        // reader resuming from the ring in between would be served below it.
        let stood_at = offset.unwrap_or(expected);
        match tokio::time::timeout(self.trim_wait, applied).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                return Err(BrokerError::PublishSuperseded {
                    first_offset: stood_at,
                });
            }
            Err(_) => return Err(BrokerError::TrimTimedOut { offset: stood_at }),
        }
        // The ring has it, so the mark has passed it. A log held at its commit
        // offset applies the floor only below that offset, which replication
        // may not have moved yet, and on a broker without it never does.
        if let Some(offset) = offset
            && log.readable_base() < before
        {
            log.advance_commit_offset(offset + 1).await?;
        }
        Ok(TrimOutcome {
            offset,
            oldest: log.readable_base().max(before),
        })
    }
}

/// Wait until a `Quorum` stream's committed mark reaches `end`. Other streams
/// do not wait.
async fn await_committed(handle: &StreamHandle, end: u64, wait: std::time::Duration) -> Result<()> {
    let state = &handle.state;
    if state.consistency() != ConsistencyLevel::Quorum {
        return Ok(());
    }
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        let appended = Arc::clone(&state.appended);
        let moved = appended.notified();
        tokio::pin!(moved);
        // Registered before the bound is read, so a mark moving in between
        // still wakes this.
        moved.as_mut().enable();
        match state.read_bound() {
            ReadBound::Refused => {
                let (stream, shard) = state.held.shard().unwrap_or_default();
                return Err(not_readable(stream, shard, NotReadable::Refused));
            }
            bound if bound.covers(end) => return Ok(()),
            _ => {}
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(BrokerError::TrimTimedOut { offset: end });
        }
        let _ = tokio::time::timeout(COMMIT_RECHECK, moved).await;
    }
}

/// Refuse a trim landing at `expected` on a stream that holds an atomic
/// commit. Every record below `expected` is read once, from where an
/// earlier trim's scan stopped; the trim then lands only at `expected`, so a
/// commit appended after this is above it.
async fn refuse_commit_stream(handle: &StreamHandle, log: &StreamLog, expected: u64) -> Result<()> {
    let commits = &handle.state.commits;
    let scan = commits.start();
    let mut next = scan.below.max(log.readable_base());
    while next < expected && !commits.seen.load(Ordering::Acquire) {
        let page = match log.read_log_from(next, COMMIT_SCAN_PAGE_BYTES).await {
            Ok(page) => page,
            // Retention or another trim moved the start past `next`.
            Err(BrokerError::CursorTooOld { oldest, .. }) => {
                next = oldest;
                continue;
            }
            Err(err) => return Err(err),
        };
        if page
            .records
            .iter()
            .any(|record| record.offset < expected && record.mark.is_commit())
        {
            commits.seen.store(true, Ordering::Release);
            break;
        }
        // The tail fell short of `expected`, so the trim would be refused.
        if page.next <= next {
            return Err(BrokerError::OffsetMismatch {
                expected,
                tail: next,
            });
        }
        next = page.next;
    }
    if commits.seen.load(Ordering::Acquire) {
        return Err(BrokerError::TrimOnCommitStream);
    }
    commits.clear_below(scan, expected);
    Ok(())
}

#[cfg(test)]
mod tests;
