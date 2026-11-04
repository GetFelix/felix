//! Atomic commits on the publish path, and reads of the state they write.
//!
//! A commit is a publish of one record: it claims its offset, waits on
//! durability (and on a `Quorum` stream, on the committed mark) and reaches
//! readers exactly as a one-event batch does. Its state updates ride along
//! and are applied where the event enters the ring. See `crate::commit`.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;

use super::Broker;
use super::publish::{Append, PublishOutcome};
use super::shards::StreamHandle;
use crate::commit::{CommitRecord, StateOp, StateView, VersionCheck};
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
        self.commit_to_handle_checked(handle, event, ops, expected, &[], publisher)
            .await
    }

    /// [`Self::commit_to_handle_at`], and only if each of `checks` holds.
    /// Refused with [`BrokerError::VersionMismatch`] on the first that does
    /// not, with nothing written.
    ///
    /// The checks and the claim run under one lock, and a commit records the
    /// keys it writes before releasing it, so two commits requiring the same
    /// version cannot both pass. The claim is still taken in the order the
    /// lock was won, so the commit sequencer orders commits as before.
    pub async fn commit_to_handle_checked(
        &self,
        handle: &StreamHandle,
        event: Bytes,
        ops: Vec<StateOp>,
        expected: Option<u64>,
        checks: &[VersionCheck],
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
        // Held until the offset is known: a later commit's check must see
        // this one's keys.
        let _gate = handle.state.claim_gate.lock().await;
        for check in checks {
            let read = self.state_get(handle, &check.key).await?;
            let epoch = handle.state.claims_epoch.load(Ordering::Acquire);
            let held =
                handle
                    .state
                    .key_claims
                    .lock()
                    .current(&check.key, read.version, read.as_of, epoch);
            if held != check.required {
                return Err(BrokerError::VersionMismatch {
                    key: check.key.clone(),
                    version: held,
                });
            }
        }
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
        let withdraw = claimed.first_offset().map(|offset| {
            let epoch = handle.state.claims_epoch.load(Ordering::Acquire);
            let undo = handle
                .state
                .key_claims
                .lock()
                .claim(offset, epoch, &record.ops);
            (offset, undo)
        });
        drop(_gate);
        claimed.set_commit(Arc::from(record.ops));
        let outcome = self.complete_publish(claimed).await;
        // A caller that goes away mid-wait leaves the claim: the commit still
        // lands then. Only a failure takes its versions back.
        if let (Some((offset, undo)), Err(_)) = (withdraw, &outcome) {
            handle.state.key_claims.lock().withdraw(offset, undo);
        }
        outcome
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
