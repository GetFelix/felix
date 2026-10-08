//! Atomic commits on the publish path, and reads of the state they write.
//!
//! A commit is a publish of one record: it claims its offset, waits on
//! durability (and on a `Quorum` stream, on the committed mark) and reaches
//! readers exactly as a one-event batch does. Its state updates ride along
//! and are applied where the event enters the ring. See `crate::commit`.

use std::sync::Arc;

use bytes::Bytes;

use super::Broker;
use super::publish::{Append, PublishOutcome};
use super::shards::StreamHandle;
use crate::commit::{CommitRecord, StateOp, StateView};
use crate::error::{BrokerError, Result};

/// Bytes read from the log per page while rebuilding a state view.
const REBUILD_PAGE_BYTES: usize = 1 << 20;

/// Rebuilds attempted before a read gives up and reports the shard busy. A
/// rebuild loses only to a commit landing while it read, so this is only
/// reached under a steady stream of commits.
const MAX_REBUILDS: usize = 8;

/// A state read: a key's value and the commit that wrote it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateRead {
    /// `None` when the key was never written or was deleted.
    pub value: Option<Bytes>,
    /// Offset of the commit that wrote `value`.
    pub version: Option<u64>,
    /// Offset of the last commit the answer reflects: every commit at or
    /// below it, and no later one. Its event is readable on the stream.
    pub as_of: Option<u64>,
}

impl Broker {
    /// Append `event` and `ops` to the shard as one commit record, and make
    /// both visible together.
    ///
    /// The whole commit lands or none of it does, on this shard and on every
    /// replica that holds its offset. The outcome's offsets are the commit's
    /// one offset, which is also the version its state updates carry.
    pub async fn commit_to_handle(
        &self,
        handle: &StreamHandle,
        event: Bytes,
        ops: Vec<StateOp>,
        publisher: Option<&Bytes>,
    ) -> Result<PublishOutcome> {
        self.commit_to_handle_at(handle, event, ops, None, publisher)
            .await
    }

    /// [`Self::commit_to_handle`], only if the commit would land at exactly
    /// `expected` when there is one: nothing has been appended to the shard
    /// since its writer read it. Refused, as
    /// [`Broker::claim_publish_at`] is, with nothing written.
    pub async fn commit_to_handle_at(
        &self,
        handle: &StreamHandle,
        event: Bytes,
        ops: Vec<StateOp>,
        expected: Option<u64>,
        publisher: Option<&Bytes>,
    ) -> Result<PublishOutcome> {
        if handle.log().is_none() {
            return Err(BrokerError::CommitNeedsDurableStream);
        }
        let record = CommitRecord {
            event: event.clone(),
            ops,
        };
        let stored = record.encode();
        let events = [event];
        let mut claimed = self
            .claim(
                handle,
                &events,
                Append::Commit {
                    record: &stored,
                    expected,
                },
                publisher,
            )
            .await?
            .expect("a commit claims or is refused");
        claimed.set_commit(Arc::from(record.ops));
        self.complete_publish(claimed).await
    }

    /// `key` in the shard's state, as of the last commit readers can see.
    ///
    /// Only commits whose events have reached the ring are reflected, so on
    /// a `Quorum` stream a commit is readable here once the committed mark
    /// covers it, and not before.
    pub async fn state_get(&self, handle: &StreamHandle, key: &str) -> Result<StateRead> {
        let Some(log) = handle.log() else {
            return Err(BrokerError::CommitNeedsDurableStream);
        };
        for _ in 0..MAX_REBUILDS {
            let end = match handle.state.read_state(key) {
                Ok((entry, as_of)) => {
                    return Ok(StateRead {
                        value: entry.as_ref().map(|entry| entry.value.clone()),
                        version: entry.map(|entry| entry.version),
                        as_of,
                    });
                }
                Err(end) => end,
            };
            // A view rebuilt from disk must hold only committed records. The ring
            // is bounded by the hold, but a promoted leader's `next_seq` covers
            // everything it inherited, committed or not.
            match handle.read_bound() {
                bound if bound.covers(end) => {}
                crate::ReadBound::Refused => {
                    return Err(BrokerError::StateNotReadable(crate::NotReadable::Refused));
                }
                _ => return Err(BrokerError::StateNotReadable(crate::NotReadable::Settling)),
            }
            let mut view = StateView::default();
            let mut next = log.base_offset();
            while next < end {
                let records = log.read_log_from(next, REBUILD_PAGE_BYTES).await?;
                let Some(last) = records.last().map(|record| record.offset) else {
                    break;
                };
                for record in records.into_iter().filter(|r| r.offset < end) {
                    if record.mark.is_commit() {
                        let commit = CommitRecord::decode(&record.payload)?;
                        view.apply(record.offset, &commit.ops);
                    }
                }
                next = last + 1;
            }
            handle.state.install_state_view(view, end);
        }
        Err(BrokerError::StateViewBusy)
    }
}
