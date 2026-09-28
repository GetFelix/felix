//! Rebuilding a halted follower from the leader's log, under a policy.

use felix_broker::StreamLog;
use felix_wire::internal::{InternalMessage, ReplicateBootstrap, ReplicateRebuild, ShardRef};

use super::follower::{FollowerCursor, Halt};
use super::metrics;
use super::ship::{Progress, read_answer, replica_log, unreachable_outcome};
use crate::peer::PeerRequester;

/// What the leader may do about a halted follower on its own.
///
/// A halt does not resolve itself: the follower is out of every quorum until
/// its copy is discarded and rebuilt. Doing that automatically is a full
/// transfer per shard, and doing it for every halted follower at once, across
/// every shard a failed broker led, is how a recovery becomes an outage. So
/// it happens under a cap and, optionally, a rate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RebuildPolicy {
    /// Rebuilds in flight at once, across every shard this broker leads.
    /// Zero leaves every halt to an operator.
    pub max_concurrent: usize,
    /// Bytes per second a rebuilding follower is shipped at; zero is
    /// unlimited. Applied per rebuilding follower, on this leader.
    pub bytes_per_sec: u64,
}

impl Default for RebuildPolicy {
    fn default() -> Self {
        Self {
            max_concurrent: 1,
            bytes_per_sec: 0,
        }
    }
}

/// The policy plus how much of it is in use, shared by every shard this
/// broker ships in one pass so the cap holds across them.
#[derive(Debug)]
pub struct Rebuilds {
    policy: RebuildPolicy,
    in_flight: std::sync::Mutex<usize>,
}

impl Rebuilds {
    pub fn new(policy: RebuildPolicy) -> Self {
        Self {
            policy,
            in_flight: std::sync::Mutex::new(0),
        }
    }

    /// Never rebuild; every halt waits for an operator.
    pub fn disabled() -> Self {
        Self::new(RebuildPolicy {
            max_concurrent: 0,
            bytes_per_sec: 0,
        })
    }

    pub fn policy(&self) -> RebuildPolicy {
        self.policy
    }

    /// Reconcile the count with the cursors that actually carry a rebuild,
    /// once per pass: a cursor discarded on a generation change takes its
    /// slot with it, and nothing else would give it back.
    pub fn set_in_flight(&self, count: usize) {
        *self.in_flight.lock().expect("rebuild slots") = count;
        metrics::record_rebuilding(count);
    }

    pub(super) fn try_begin(&self) -> bool {
        let mut in_flight = self.in_flight.lock().expect("rebuild slots");
        if *in_flight >= self.policy.max_concurrent {
            return false;
        }
        *in_flight += 1;
        metrics::record_rebuilding(*in_flight);
        true
    }

    pub(super) fn finish(&self) {
        let mut in_flight = self.in_flight.lock().expect("rebuild slots");
        *in_flight = in_flight.saturating_sub(1);
        metrics::record_rebuilding(*in_flight);
    }
}

pub(super) enum Rebuild {
    /// The follower discarded its copy; its new one begins here.
    Accepted { base_offset: u64 },
    /// Not answered. Asked again next pass.
    Unreachable,
    /// Answered no, or with something that was not an answer. Not asked again
    /// at this generation.
    Refused,
}

/// Ask a halted follower to discard its copy and start again at this
/// leader's oldest record.
pub(super) async fn request_rebuild<R: PeerRequester>(
    requester: &R,
    log: &StreamLog,
    shard: &ShardRef,
    log_kind: felix_broker::LogKind,
    cursor: &FollowerCursor,
) -> Rebuild {
    let base_offset = log.base_offset();
    let request = InternalMessage::ReplicateRebuild(ReplicateRebuild {
        // The pool assigns the real id; it owns the connection this lands on.
        correlation_id: 0,
        shard: shard.clone(),
        log: replica_log(log_kind),
        base_offset,
    });
    let answer = match requester
        .request(&cursor.node_id, cursor.addr, request)
        .await
    {
        Ok(answer) => answer,
        Err(err) => {
            tracing::warn!(
                node_id = %cursor.node_id,
                stream = %shard.stream,
                shard = shard.shard,
                error = %err,
                "could not reach the follower to rebuild it",
            );
            return Rebuild::Unreachable;
        }
    };
    match answer {
        InternalMessage::ReplicateOk(ok) => Rebuild::Accepted {
            base_offset: ok.durable_offset,
        },
        InternalMessage::ReplicateError(err) => {
            tracing::warn!(
                node_id = %cursor.node_id,
                stream = %shard.stream,
                shard = shard.shard,
                code = ?err.code,
                detail = %err.detail,
                "the follower refused to rebuild",
            );
            Rebuild::Refused
        }
        // Anything else is a peer that did not understand the request,
        // most likely one that predates rebuilds. The halt stands until it
        // is upgraded or an operator acts.
        other => {
            tracing::warn!(
                node_id = %cursor.node_id,
                stream = %shard.stream,
                shard = shard.shard,
                kind = ?other.kind(),
                "the follower did not understand a rebuild; it stays halted",
            );
            Rebuild::Refused
        }
    }
}

/// Offer a follower a log that begins where this leader's surviving log does.
///
/// Only the follower can accept: one holding records of its own has a gap it
/// cannot fill, and discarding them is an operator's decision rather than a
/// leader's. A refusal halts this follower, which is where it stood before the
/// offer was made.
pub(super) async fn offer_bootstrap<R: PeerRequester>(
    requester: &R,
    shard: &ShardRef,
    log_kind: felix_broker::LogKind,
    cursor: &mut FollowerCursor,
    base_offset: u64,
) -> Progress {
    let offer = ReplicateBootstrap {
        // The pool assigns the real id; it owns the connection this lands on.
        correlation_id: 0,
        shard: shard.clone(),
        base_offset,
    };
    let request = match log_kind {
        felix_broker::LogKind::Cache => InternalMessage::ReplicateCacheBootstrap(offer),
        felix_broker::LogKind::GroupCursors => InternalMessage::ReplicateGroupBootstrap(offer),
        felix_broker::LogKind::GroupDeadLetters => {
            InternalMessage::ReplicateDeadLetterBootstrap(offer)
        }
        felix_broker::LogKind::Counters => InternalMessage::ReplicateCounterBootstrap(offer),
        felix_broker::LogKind::Stream => InternalMessage::ReplicateBootstrap(offer),
    };
    let answer = match requester
        .request(&cursor.node_id, cursor.addr, request)
        .await
    {
        Ok(answer) => answer,
        Err(err) => {
            // Unreachable, not unwilling. The offer stands and goes again.
            metrics::record_shipped(unreachable_outcome(&err));
            return Progress::Retry;
        }
    };

    match read_answer(&answer) {
        Progress::Stored { durable_offset } => {
            cursor.next_offset = durable_offset;
            tracing::info!(
                node_id = %cursor.node_id,
                stream = %shard.stream,
                shard = shard.shard,
                base_offset,
                "the follower placed its log and replication resumed",
            );
            metrics::record_shipped(metrics::OUTCOME_BOOTSTRAPPED);
            Progress::Resume {
                offset: durable_offset,
            }
        }
        // The follower will not take it, and nothing about that resolves by
        // asking again: it holds records of its own, so a person has to decide
        // what becomes of them.
        _ => {
            tracing::error!(
                node_id = %cursor.node_id,
                stream = %shard.stream,
                shard = shard.shard,
                base_offset,
                "replication stopped: the follower refused a bootstrap and cannot be caught up",
            );
            cursor.halted = Some(Halt::NeedsBootstrap);
            metrics::record_shipped(metrics::OUTCOME_NEEDS_BOOTSTRAP);
            Progress::Halted(Halt::NeedsBootstrap)
        }
    }
}
