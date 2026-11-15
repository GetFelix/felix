//! A head trim on a shard whose engine gives it its offset: a shard on Raft.
//!
//! The checks are the hybrid's, and so are the answers. The trim record is
//! one entry, conditional on landing at `expected` exactly as a conditional
//! publish is, and its floor reaches the replay ring in the entry's turn.
//! See `docs/per-shard-raft.md`, "Head trims".

use std::future::Future;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::oneshot;

use super::{TrimOutcome, await_committed, refuse_commit_stream};
use crate::broker::Broker;
use crate::broker::publish::{Expected, HeldTrim};
use crate::broker::shards::StreamHandle;
use crate::error::{BrokerError, Result};
use crate::{ProposalCommitted, ProposalFailed, ProposedPublish};

impl Broker {
    /// [`Self::trim_before_at`] for a shard whose engine writes its log.
    ///
    /// Refused as the hybrid refuses, with nothing proposed: `before` past
    /// `expected` is [`BrokerError::TrimPastTail`], a tail other than
    /// `expected` is [`BrokerError::OffsetMismatch`], and a stream holding
    /// atomic commits is [`BrokerError::TrimOnCommitStream`]. A trim at or
    /// below the current floor writes nothing and answers with the floor.
    ///
    /// Otherwise `propose` is handed one record, the headers, and must
    /// queue it as a trim to `before` that lands only at `expected`, as
    /// [`Self::propose_publish_at`] does. The leader's log decides that at
    /// the entry's final index, so a tail that moved after the check still
    /// refuses it there, with `mismatch` given the offset after the blank
    /// it took. [`ProposedTrim::complete`] answers once the floor is in the
    /// ring.
    #[allow(clippy::too_many_arguments)]
    pub async fn propose_trim_at<F, E>(
        &self,
        handle: &StreamHandle,
        before: u64,
        expected: u64,
        headers: Bytes,
        stalled_after: Duration,
        mismatch: impl FnOnce(u64) -> E + Send + 'static,
        propose: impl FnOnce(u64) -> F,
    ) -> Result<ProposedTrim<E>>
    where
        F: Future<Output = std::result::Result<ProposalCommitted, ProposalFailed<E>>>
            + Send
            + 'static,
        E: Send + 'static,
    {
        let Some(log) = &handle.state.durable else {
            return Err(BrokerError::TrimNeedsDurableStream);
        };
        if before > expected {
            return Err(BrokerError::TrimPastTail {
                before,
                offset: expected,
            });
        }
        // Read in the hybrid's order: the floor only rises, so if the tail
        // is still `expected` after it, the no-op answer held then.
        let floor = log.head_trim().map_or(0, |trim| trim.floor);
        let tail = log.tail_offset().await?;
        if tail != expected {
            return Err(BrokerError::OffsetMismatch { expected, tail });
        }
        if before <= floor {
            return Ok(ProposedTrim {
                handle: handle.clone(),
                trim_wait: self.trim_wait,
                step: Step::NoOp { expected },
            });
        }
        refuse_commit_stream(handle, log, expected).await?;
        self.mark_head_trimmed().await?;
        let (applied, applied_rx) = oneshot::channel();
        let expected_at = Expected {
            offset: expected,
            mismatch: Box::new(mismatch),
        };
        let proposed = self.propose(
            handle,
            vec![headers],
            Some(expected_at),
            Some(HeldTrim { before, applied }),
            stalled_after,
            propose,
        )?;
        Ok(ProposedTrim {
            handle: handle.clone(),
            trim_wait: self.trim_wait,
            step: Step::Proposed {
                before,
                expected,
                proposed,
                applied: applied_rx,
            },
        })
    }
}

/// A trim handed to a shard's engine, on its way to an answer. See
/// [`Broker::propose_trim_at`].
pub struct ProposedTrim<E: Send + 'static> {
    handle: StreamHandle,
    trim_wait: Duration,
    step: Step<E>,
}

enum Step<E: Send + 'static> {
    NoOp {
        expected: u64,
    },
    Proposed {
        before: u64,
        expected: u64,
        proposed: ProposedPublish<E>,
        applied: oneshot::Receiver<()>,
    },
}

/// Why a [`ProposedTrim`] was not answered with an outcome.
#[derive(Debug)]
pub enum TrimFailed<E> {
    /// The engine's answer, including the mismatch of a trim its log
    /// stored as a blank.
    Engine(E),
    /// The broker's: the floor did not reach the ring in time, or the
    /// shard moved before it did. The trim may have landed.
    Broker(BrokerError),
}

impl<E: Send + 'static> ProposedTrim<E> {
    /// Answer `timed_out()` if the engine has not committed the trim
    /// within `timeout`, as [`ProposedPublish::answer_within`] does. The
    /// trim may still land.
    pub fn answer_within(
        self,
        timeout: Duration,
        timed_out: impl FnOnce() -> E + Send + 'static,
    ) -> Self {
        let step = match self.step {
            Step::Proposed {
                before,
                expected,
                proposed,
                applied,
            } => Step::Proposed {
                before,
                expected,
                proposed: proposed.answer_within(timeout, timed_out),
                applied,
            },
            noop => noop,
        };
        Self { step, ..self }
    }

    /// Wait for the trim's commit and for its floor to reach the ring, so
    /// no reader of this broker is served below it after the answer. A
    /// no-op waits for what it stands on to be committed, as on the hybrid.
    pub async fn complete(self) -> std::result::Result<TrimOutcome, TrimFailed<E>> {
        let Some(log) = self.handle.state.durable.as_ref() else {
            return Err(TrimFailed::Broker(BrokerError::TrimNeedsDurableStream));
        };
        let (before, expected, proposed, applied) = match self.step {
            Step::NoOp { expected } => {
                await_committed(&self.handle, expected, self.trim_wait)
                    .await
                    .map_err(TrimFailed::Broker)?;
                return Ok(TrimOutcome {
                    offset: None,
                    oldest: log.readable_base(),
                });
            }
            Step::Proposed {
                before,
                expected,
                proposed,
                applied,
            } => (before, expected, proposed, applied),
        };
        let outcome = proposed.complete().await.map_err(TrimFailed::Engine)?;
        let offset = outcome.offsets.map(|(first, _)| first);
        let stood_at = offset.unwrap_or(expected);
        match tokio::time::timeout(self.trim_wait, applied).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                return Err(TrimFailed::Broker(BrokerError::PublishSuperseded {
                    first_offset: stood_at,
                }));
            }
            Err(_) => {
                return Err(TrimFailed::Broker(BrokerError::TrimTimedOut {
                    offset: stood_at,
                }));
            }
        }
        Ok(TrimOutcome {
            offset,
            oldest: log.readable_base().max(before),
        })
    }
}
