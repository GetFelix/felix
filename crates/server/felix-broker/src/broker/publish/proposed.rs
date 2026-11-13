//! A publish whose offsets the shard's engine gives it: a shard on Raft,
//! where openraft makes each record the next log index.
//!
//! The batch is never read back. The engine writes and commits it, and the
//! records the publisher handed in are what joins the commit hold and is
//! fanned out, in one shared envelope, once the engine says they are
//! committed. A record a later leader cut was never committed, so no
//! subscriber has it. See `docs/per-shard-raft.md`, "Delivering committed
//! records".

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use bytes::Bytes;
use felix_storage::CommitTurn;

use super::PublishOutcome;
use super::completion::spawn_release;
use crate::broker::Broker;
use crate::broker::shards::StreamHandle;
use crate::error::{BrokerError, Result};
use crate::stream::HeldBatch;

type Committing<E> =
    Pin<Box<dyn Future<Output = std::result::Result<ProposalCommitted, E>> + Send>>;
type Finishing<E> = Pin<Box<dyn Future<Output = std::result::Result<PublishOutcome, E>> + Send>>;

impl Broker {
    /// Hand `payloads` to the shard's engine and take the batch's place in
    /// the shard's delivery order.
    ///
    /// `propose` is called once, with the time to stamp every record, while
    /// the place is held. It must queue the batch with the engine before it
    /// returns, because the order of these calls is taken as the order of
    /// the batches' offsets. Its future resolves once the engine has
    /// committed the batch under the leader that wrote it, or to the
    /// engine's error, which delivers nothing. It must not resolve to an
    /// error while the batch may still commit (a timeout): the batches
    /// behind it would go out while it never did. Bound the publisher's
    /// wait with [`ProposedPublish::answer_within`] instead.
    ///
    /// The batch is delivered from `payloads` as they are, after every batch
    /// handed over before it. Finish it with [`ProposedPublish::complete`];
    /// one dropped on the way still delivers, on a task of its own, so the
    /// batches behind it are not held up.
    pub fn propose_publish<F, E>(
        &self,
        handle: &StreamHandle,
        payloads: Vec<Bytes>,
        propose: impl FnOnce(u64) -> F,
    ) -> Result<ProposedPublish<E>>
    where
        F: Future<Output = std::result::Result<ProposalCommitted, E>> + Send + 'static,
        E: Send + 'static,
    {
        let state = &handle.state;
        if !state.active.load(std::sync::atomic::Ordering::Acquire) {
            return Err(BrokerError::StreamHandleInactive(handle.id()));
        }
        if payloads.is_empty() {
            return Ok(ProposedPublish {
                finishing: None,
                answer_within: None,
            });
        }
        let timestamp_micros = crate::append_time_now();
        let (committed, turn) = state
            .proposals
            .hand_over(|| Box::pin(propose(timestamp_micros)) as Committing<E>);
        Ok(ProposedPublish {
            finishing: Some(Box::pin(finish(
                handle.clone(),
                payloads,
                timestamp_micros,
                committed,
                turn,
            ))),
            answer_within: None,
        })
    }
}

/// Where an engine committed a proposed batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProposalCommitted {
    /// One past the batch's last offset.
    pub end: u64,
    /// How many offsets just below the batch hold no record (a leader's
    /// blank entry, a membership change). Subscribers are told they were
    /// skipped, so the jump is not read as a drop.
    pub skipped_before: u64,
}

/// A batch handed to the shard's engine, on its way to its subscribers.
///
/// Dropped before [`Self::complete`] finishes, the rest runs on a task of
/// its own.
#[must_use = "a proposed publish reports its outcome only through `complete`"]
pub struct ProposedPublish<E: Send + 'static> {
    finishing: Option<Finishing<E>>,
    answer_within: Option<AnswerWithin<E>>,
}

type AnswerWithin<E> = (Duration, Box<dyn FnOnce() -> E + Send>);

impl<E: Send + 'static> ProposedPublish<E> {
    /// Answer [`Self::complete`] with `timed_out` if the batch is not
    /// committed within `timeout`. The batch keeps its place and is still
    /// delivered if it commits.
    pub fn answer_within(
        mut self,
        timeout: Duration,
        timed_out: impl FnOnce() -> E + Send + 'static,
    ) -> Self {
        self.answer_within = Some((timeout, Box::new(timed_out)));
        self
    }

    /// Wait until the batch is committed and queued for delivery behind
    /// every batch handed over before it. Reports its offsets and no
    /// subscribers: the fanout runs after this returns.
    pub async fn complete(mut self) -> std::result::Result<PublishOutcome, E> {
        let Some(finishing) = self.finishing.as_mut() else {
            return Ok(PublishOutcome {
                subscribers: 0,
                offsets: None,
            });
        };
        let outcome = match self.answer_within.take() {
            None => finishing.await,
            Some((timeout, timed_out)) => {
                match tokio::time::timeout(timeout, finishing).await {
                    Ok(outcome) => outcome,
                    // `Drop` finishes the batch on a task.
                    Err(_) => return Err(timed_out()),
                }
            }
        };
        self.finishing = None;
        outcome
    }
}

impl<E: Send + 'static> Drop for ProposedPublish<E> {
    fn drop(&mut self) {
        let Some(finishing) = self.finishing.take() else {
            return;
        };
        // Outside a runtime the process is going down, and nothing is left
        // to deliver to.
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        runtime.spawn(async move {
            let _ = finishing.await;
        });
    }
}

/// Wait for the commit, then for the turn of the batch handed over before
/// this one, and join the hold. The turn is released however this ends.
///
/// A batch the engine gave up on still takes its turn, as a gap: its
/// records may have committed, so the replay ring restarts above it and a
/// reader resuming below it reads the log, never a ring that skips it.
async fn finish<E>(
    handle: StreamHandle,
    payloads: Vec<Bytes>,
    timestamp_micros: u64,
    committed: Committing<E>,
    turn: CommitTurn<'static>,
) -> std::result::Result<PublishOutcome, E> {
    let committed = committed.await;
    let count = payloads.len() as u64;
    let first_offset = match &committed {
        Ok(at) => at.end.checked_sub(count),
        Err(_) => None,
    };
    if let (Ok(at), None) = (&committed, first_offset) {
        tracing::error!(
            end = at.end,
            count,
            "an engine's committed end is below its batch"
        );
    }
    // Never reset, so a wait cannot be refused.
    let _ = turn.wait().await;
    let state = &handle.state;
    let batch = match (&committed, first_offset) {
        (Ok(at), Some(first_offset)) => HeldBatch {
            payloads,
            publisher: None,
            record_headers: None,
            timestamp_micros: Some(timestamp_micros),
            first_offset,
            commit: None,
            trim: None,
            trim_applied: None,
            skipped_before: at.skipped_before,
            gap: false,
        },
        _ => HeldBatch::gap(),
    };
    let start = state.held.push(batch);
    drop(turn);
    if start {
        spawn_release(&handle);
    }
    committed?;
    Ok(PublishOutcome {
        subscribers: 0,
        offsets: first_offset.map(|first| (first, first + count - 1)),
    })
}

#[cfg(test)]
mod tests;
