//! Atomic commits on the publish path, and reads of the state they write.
//!
//! A commit is a publish of one record: it claims its offset, waits on
//! durability (and on a `Quorum` stream, on the committed mark) and reaches
//! readers exactly as a one-event batch does. Its state updates ride along
//! and are applied where the event enters the ring. See `crate::commit`.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use super::Broker;
use super::publish::{Append, Attribution, Expected, PublishOutcome};
use super::shards::StreamHandle;
use crate::commit::{CommitRecord, StateOp, StateView};
use crate::error::{BrokerError, Result};
use crate::{ProposalCommitted, ProposalFailed, ProposedPublish};

/// Bytes read from the log per page while rebuilding a state view.
const REBUILD_PAGE_BYTES: usize = 1 << 20;

/// Rebuilds attempted before a read gives up and reports the shard busy. A
/// rebuild loses only to a commit landing while it read, so this is only
/// reached under a steady stream of commits.
const MAX_REBUILDS: usize = 8;

/// How often [`Broker::await_state_view`] looks at the ring again when
/// nothing wakes it.
const VIEW_RECHECK: Duration = Duration::from_millis(50);

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
                None,
            )
            .await?
            .expect("a commit claims or is refused");
        claimed.set_commit(Arc::from(record.ops));
        self.complete_publish(claimed).await
    }

    /// [`Self::commit_to_handle_at`] on a shard an engine writes: `propose`
    /// hands the engine the commit, its record encoded as
    /// [`CommitRecord::encode`] lays it out, which the engine stores with
    /// the commit mark as one entry.
    ///
    /// The event reaches the replay ring and `ops` the state view together,
    /// once the engine has committed the entry, as a publish's batch does.
    /// With `expected`, the engine's log stores a blank in place of the
    /// commit when its entry does not land at that offset, and the answer is
    /// `mismatch` with the offset after it, as
    /// [`Self::propose_publish_at`] answers. See `docs/per-shard-raft.md`,
    /// "Atomic commits".
    #[allow(clippy::too_many_arguments)]
    pub fn propose_commit<F, E>(
        &self,
        handle: &StreamHandle,
        event: Bytes,
        ops: Arc<[StateOp]>,
        attribution: Attribution,
        expected: Option<u64>,
        stalled_after: Duration,
        mismatch: impl FnOnce(u64) -> E + Send + 'static,
        propose: impl FnOnce(u64) -> F,
    ) -> Result<ProposedPublish<E>>
    where
        F: Future<Output = std::result::Result<ProposalCommitted, ProposalFailed<E>>>
            + Send
            + 'static,
        E: Send + 'static,
    {
        if handle.log().is_none() {
            return Err(BrokerError::CommitNeedsDurableStream);
        }
        // Before the proposal, as the hybrid sets it before its append, so a
        // trim checked after this refuses.
        handle
            .state
            .commits
            .seen
            .store(true, std::sync::atomic::Ordering::Release);
        let expected = expected.map(|offset| Expected {
            offset,
            mismatch: Box::new(mismatch) as Box<dyn FnOnce(u64) -> E + Send>,
        });
        self.propose(
            handle,
            vec![event],
            attribution,
            expected,
            None,
            Some(ops),
            stalled_after,
            propose,
        )
    }

    /// Wait, up to `wait`, until the shard's state view holds every record
    /// the read bound covers now. History reads stop at that bound, but the
    /// view moves only when a held batch is released into the ring, so a
    /// state read that skipped this could miss an event a reader already saw.
    ///
    /// Fails with [`BrokerError::StateNotReadable`] when the bound is not
    /// known or the view does not get there in time.
    pub async fn await_state_view(&self, handle: &StreamHandle, wait: Duration) -> Result<()> {
        let state = &handle.state;
        let end = match state.read_bound() {
            crate::ReadBound::Committed(bound) => bound,
            crate::ReadBound::Unbounded => return Ok(()),
            crate::ReadBound::Refused => {
                return Err(BrokerError::StateNotReadable(crate::NotReadable::Refused));
            }
            crate::ReadBound::Settling => {
                return Err(BrokerError::StateNotReadable(crate::NotReadable::Settling));
            }
        };
        // Entries at the bound's tail that hold no record leave the ring
        // below it until the next publish; on an idle shard that is forever.
        let end = state.records_end(end);
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let moved = state.ring_moved.notified();
            tokio::pin!(moved);
            moved.as_mut().enable();
            if state.tail_seq() >= end {
                return Ok(());
            }
            // The recheck covers the few moves of the ring that do not wake.
            let recheck = deadline.min(tokio::time::Instant::now() + VIEW_RECHECK);
            if tokio::time::timeout_at(recheck, moved).await.is_err()
                && tokio::time::Instant::now() >= deadline
            {
                return Err(BrokerError::StateNotReadable(crate::NotReadable::Settling));
            }
        }
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
            let mut next = log.readable_base();
            while next < end {
                let page = log.read_log_from(next, REBUILD_PAGE_BYTES).await?;
                for record in page.records.into_iter().filter(|r| r.offset < end) {
                    if record.mark.is_commit() {
                        let commit = CommitRecord::decode(&record.payload)?;
                        view.apply(record.offset, &commit.ops);
                    }
                }
                if page.next <= next {
                    break;
                }
                next = page.next;
            }
            handle.state.install_state_view(view, end);
        }
        Err(BrokerError::StateViewBusy)
    }
}
