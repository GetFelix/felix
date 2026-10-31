//! Conditional head trims: hiding a durable stream's history below an offset,
//! only if the stream's tail is the one the caller expects.
//!
//! A trim is a record in the shard's log. It takes its place in the stream's
//! order exactly as a conditional publish does, offsets first and durability
//! after, and reaches the replay ring in its turn, where it drops the entries
//! below its floor instead of being appended. See `docs/durable-storage.md`,
//! "Conditional head trim".

use std::sync::atomic::Ordering;

use bytes::Bytes;

use super::Broker;
use super::publish::Append;
use super::shards::StreamHandle;
use crate::durable::StreamLog;
use crate::error::{BrokerError, Result};

/// Bytes read per page while looking for atomic commits before a trim.
const COMMIT_SCAN_PAGE_BYTES: usize = 1 << 20;

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
    /// below the floor are dropped. On a `Quorum` stream it reaches the ring,
    /// and its floor applies, once the committed mark passes it; the caller
    /// waits for the mark as for a publish. On a `Leader` stream an
    /// acknowledged trim can be lost at a failover, like any record there.
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
        let outcome = self.complete_publish(claimed).await?;
        Ok(TrimOutcome {
            offset: outcome.offsets.map(|(first, _)| first),
            oldest: log.readable_base().max(before),
        })
    }
}

/// Refuse a trim landing at `expected` on a stream that holds an atomic
/// commit. Every record below `expected` is read once, from where an
/// earlier trim's scan stopped; the trim then lands only at `expected`, so a
/// commit appended after this is above it.
async fn refuse_commit_stream(handle: &StreamHandle, log: &StreamLog, expected: u64) -> Result<()> {
    let commits = &handle.state.commits;
    let mut next = commits
        .clear_below
        .load(Ordering::Acquire)
        .max(log.readable_base());
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
    commits.clear_below.fetch_max(expected, Ordering::AcqRel);
    Ok(())
}

#[cfg(test)]
mod tests;
