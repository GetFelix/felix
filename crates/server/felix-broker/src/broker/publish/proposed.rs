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

use bytes::Bytes;
use felix_storage::CommitTurn;

use super::PublishOutcome;
use super::completion::spawn_release;
use crate::broker::Broker;
use crate::broker::shards::StreamHandle;
use crate::error::{BrokerError, Result};
use crate::stream::HeldBatch;

type Committing<E> = Pin<Box<dyn Future<Output = std::result::Result<u64, E>> + Send>>;
type Finishing<E> = Pin<Box<dyn Future<Output = std::result::Result<PublishOutcome, E>> + Send>>;

impl Broker {
    /// Hand `payloads` to the shard's engine and take the batch's place in
    /// the shard's delivery order.
    ///
    /// `propose` is called once, with the time to stamp every record, while
    /// the place is held. It must queue the batch with the engine before it
    /// returns, because the order of these calls is taken as the order of
    /// the batches' offsets. Its future resolves once the engine has
    /// committed the batch under the leader that wrote it, to the offset
    /// one past its last record, or to the engine's error, which delivers
    /// nothing.
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
        F: Future<Output = std::result::Result<u64, E>> + Send + 'static,
        E: Send + 'static,
    {
        let state = &handle.state;
        if !state.active.load(std::sync::atomic::Ordering::Acquire) {
            return Err(BrokerError::StreamHandleInactive(handle.id()));
        }
        if payloads.is_empty() {
            return Ok(ProposedPublish(None));
        }
        let timestamp_micros = crate::append_time_now();
        let (committed, turn) = state
            .proposals
            .hand_over(|| Box::pin(propose(timestamp_micros)) as Committing<E>);
        Ok(ProposedPublish(Some(Box::pin(finish(
            handle.clone(),
            payloads,
            timestamp_micros,
            committed,
            turn,
        )))))
    }
}

/// A batch handed to the shard's engine, on its way to its subscribers.
///
/// Dropped before [`Self::complete`] finishes, the rest runs on a task of
/// its own.
#[must_use = "a proposed publish reports its outcome only through `complete`"]
pub struct ProposedPublish<E: Send + 'static>(Option<Finishing<E>>);

impl<E: Send + 'static> ProposedPublish<E> {
    /// Wait until the batch is committed and queued for delivery behind
    /// every batch handed over before it. Reports its offsets and no
    /// subscribers: the fanout runs after this returns.
    pub async fn complete(mut self) -> std::result::Result<PublishOutcome, E> {
        let Some(finishing) = self.0.as_mut() else {
            return Ok(PublishOutcome {
                subscribers: 0,
                offsets: None,
            });
        };
        let outcome = finishing.await;
        self.0 = None;
        outcome
    }
}

impl<E: Send + 'static> Drop for ProposedPublish<E> {
    fn drop(&mut self) {
        let Some(finishing) = self.0.take() else {
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
async fn finish<E>(
    handle: StreamHandle,
    payloads: Vec<Bytes>,
    timestamp_micros: u64,
    committed: Committing<E>,
    turn: CommitTurn<'static>,
) -> std::result::Result<PublishOutcome, E> {
    let end = committed.await?;
    let count = payloads.len() as u64;
    let Some(first_offset) = end.checked_sub(count) else {
        tracing::error!(end, count, "an engine's committed end is below its batch");
        return Ok(PublishOutcome {
            subscribers: 0,
            offsets: None,
        });
    };
    // Never reset, so a wait cannot be refused.
    let _ = turn.wait().await;
    let state = &handle.state;
    let start = state.held.push(HeldBatch {
        payloads,
        publisher: None,
        record_headers: None,
        timestamp_micros: Some(timestamp_micros),
        first_offset,
        commit: None,
        trim: None,
        trim_applied: None,
    });
    drop(turn);
    if start {
        spawn_release(&handle);
    }
    Ok(PublishOutcome {
        subscribers: 0,
        offsets: Some((first_offset, end - 1)),
    })
}

#[cfg(test)]
mod tests;
