//! Shipping one batch to one follower, and reading its answer.

use bytes::Bytes;
use felix_broker::StreamLog;
use felix_storage::log::RecordMark;
use felix_wire::internal::{
    ErrorCode, InternalMessage, PeerCapabilities, ProducerMark, ReplicaLog, ReplicateRecords,
    ShardRef, batch_checksum,
};

use super::follower::{FollowerCursor, Halt, RebuildBackoff};
use super::metrics;
use super::rebuild::{Rebuild, Rebuilds, offer_bootstrap, request_rebuild};
use crate::peer::{PeerError, PeerRequester};

/// Ship one batch to one follower and apply its answer to the cursor.
///
/// Returns what the exchange concluded. The cursor is advanced, rewound, or
/// halted here, so a caller only has to decide whether to go round again.
pub async fn ship_once<R: PeerRequester>(
    requester: &R,
    log: &StreamLog,
    shard: &ShardRef,
    log_kind: felix_broker::LogKind,
    cursor: &mut FollowerCursor,
    max_batch_bytes: usize,
    rebuilds: &Rebuilds,
) -> Progress {
    ship_once_with(
        requester,
        log,
        shard,
        log_kind,
        cursor,
        max_batch_bytes,
        rebuilds,
        None,
    )
    .await
}

/// [`ship_once`], telling the follower that everything below `commit_offset`
/// is committed, so it refuses to discard any of it later.
#[allow(clippy::too_many_arguments)]
pub async fn ship_once_with<R: PeerRequester>(
    requester: &R,
    log: &StreamLog,
    shard: &ShardRef,
    log_kind: felix_broker::LogKind,
    cursor: &mut FollowerCursor,
    max_batch_bytes: usize,
    rebuilds: &Rebuilds,
    commit_offset: Option<u64>,
) -> Progress {
    if let Some(halt) = cursor.halted {
        let backing_off = cursor
            .rebuild_refused
            .is_some_and(|backoff| tokio::time::Instant::now() < backoff.retry_at);
        if !halt.rebuildable() || backing_off || !rebuilds.try_begin() {
            return Progress::Halted(halt);
        }
        match request_rebuild(requester, log, shard, log_kind, cursor).await {
            Rebuild::Accepted { base_offset } => {
                cursor.halted = None;
                cursor.rebuild_refused = None;
                cursor.next_offset = base_offset;
                cursor.confirmed = 0;
                cursor.rebuilding = true;
                tracing::warn!(
                    node_id = %cursor.node_id,
                    stream = %shard.stream,
                    shard = shard.shard,
                    generation = shard.generation,
                    reason = %halt,
                    base_offset,
                    "the follower discarded its copy of this shard; rebuilding it from here",
                );
                metrics::record_rebuild(metrics::OUTCOME_REBUILD_STARTED);
            }
            Rebuild::Unreachable => {
                rebuilds.finish();
                return Progress::Halted(halt);
            }
            Rebuild::Refused => {
                cursor.rebuild_refused = Some(RebuildBackoff::after(
                    cursor.rebuild_refused,
                    tokio::time::Instant::now(),
                ));
                rebuilds.finish();
                metrics::record_rebuild(metrics::OUTCOME_REBUILD_REFUSED);
                return Progress::Halted(halt);
            }
        }
    }

    let records = match log.read_log_from(cursor.next_offset, max_batch_bytes).await {
        Ok(records) => records,
        // The follower is asking for records this leader has already trimmed.
        // Shipping cannot bridge that: the records are not here to send, and
        // starting the follower at the surviving base would leave its log with
        // a hole that nothing downstream could detect.
        //
        // Halting is the honest answer. It stops a retry that could never
        // succeed, keeps the follower out of every quorum, and says plainly
        // that the shard needs its history transferred -- which is #114's
        // subject and is not implemented.
        Err(felix_broker::BrokerError::CursorTooOld { oldest, requested }) => {
            // The records between the follower's position and this leader's
            // oldest are gone from both. Shipping cannot bridge that, so the
            // follower is offered the one fact it cannot work out for itself:
            // where the surviving log begins.
            tracing::info!(
                node_id = %cursor.node_id,
                stream = %shard.stream,
                shard = shard.shard,
                requested,
                oldest,
                "the follower is below this leader's oldest record; offering a bootstrap",
            );
            return offer_bootstrap(requester, shard, log_kind, cursor, oldest).await;
        }
        Err(err) => {
            // The leader could not read its own log. Nothing is wrong with the
            // follower, so this must not look like divergence.
            tracing::warn!(
                node_id = %cursor.node_id,
                offset = cursor.next_offset,
                error = %err,
                "could not read the local log to replicate",
            );
            metrics::record_shipped(metrics::OUTCOME_READ_FAILED);
            return Progress::Retry;
        }
    };

    if records.is_empty() {
        if cursor.rebuilding {
            cursor.rebuilding = false;
            rebuilds.finish();
            tracing::info!(
                node_id = %cursor.node_id,
                stream = %shard.stream,
                shard = shard.shard,
                "the rebuilt follower has reached this leader's tail",
            );
            metrics::record_rebuild(metrics::OUTCOME_REBUILD_COMPLETED);
        }
        return Progress::UpToDate;
    }

    let first_offset = records[0].offset;
    // Marks go only when there are any: an unmarked batch is byte for byte
    // what a follower that predates them reads.
    let marks: Vec<ProducerMark> = if records.iter().any(|r| r.mark != RecordMark::None) {
        records
            .iter()
            .map(|record| felix_broker::replication::mark_to_wire(record.mark))
            .collect()
    } else {
        Vec::new()
    };
    let payloads: Vec<Bytes> = records.into_iter().map(|record| record.payload).collect();
    let batch_end = first_offset + payloads.len() as u64;
    let batch_bytes: usize = payloads.iter().map(Bytes::len).sum();
    let batch = ReplicateRecords {
        // The pool assigns the real id; it owns the connection this lands on.
        correlation_id: 0,
        shard: shard.clone(),
        first_offset,
        checksum: batch_checksum(&payloads, &marks),
        payloads,
        marks,
        // A follower that refused the committed kind is sent what it reads.
        commit_offset: commit_offset.filter(|_| !cursor.legacy_frames),
        // Without them the follower labels the records with this leader's
        // generation, including any this leader inherited. Only a stream
        // log's labels are compared by a fence, so the other logs go without.
        generations: (!cursor.legacy_frames
            && log_kind == felix_broker::LogKind::Stream
            && requester
                .recorded_capabilities(&cursor.node_id)
                .is_some_and(|offered| offered.contains(PeerCapabilities::GENERATION_LABELS)))
        .then(|| crate::replica::generations_over(&log.generations(), first_offset, batch_end)),
    };
    // Which log this is belongs in the message kind, not in the shard
    // reference: the bodies are identical, and a follower that guessed wrong
    // would append one of a shard's logs into another.
    let request = match log_kind {
        felix_broker::LogKind::Cache => InternalMessage::ReplicateCacheRecords(batch),
        felix_broker::LogKind::GroupCursors => InternalMessage::ReplicateGroupRecords(batch),
        felix_broker::LogKind::GroupDeadLetters => {
            InternalMessage::ReplicateDeadLetterRecords(batch)
        }
        felix_broker::LogKind::Counters => InternalMessage::ReplicateCounterRecords(batch),
        felix_broker::LogKind::Stream if !batch.marks.is_empty() => {
            InternalMessage::ReplicateMarkedRecords(batch)
        }
        felix_broker::LogKind::Stream => InternalMessage::ReplicateRecords(batch),
    };

    let mut answer = requester
        .request(&cursor.node_id, cursor.addr, request.clone())
        .await;
    if let Ok(InternalMessage::ForwardPublishError(refusal)) = &answer
        && refusal.code == ErrorCode::UnsupportedKind
        && matches!(
            request.kind(),
            felix_wire::internal::Kind::ReplicateCommittedRecords
                | felix_wire::internal::Kind::ReplicateLabelledRecords
        )
    {
        // A follower from before commit offsets. Nothing was stored; it gets
        // the frames it reads, and learns no commit offset from this leader.
        tracing::info!(
            node_id = %cursor.node_id,
            stream = %shard.stream,
            shard = shard.shard,
            "the follower predates commit offsets; shipping without them",
        );
        cursor.legacy_frames = true;
        answer = requester
            .request(&cursor.node_id, cursor.addr, without_commit(request))
            .await;
    }
    let answer = match answer {
        Ok(answer) => answer,
        Err(err) => {
            metrics::record_shipped(unreachable_outcome(&err));
            cursor.stalled = true;
            return Progress::Retry;
        }
    };

    let progress = read_answer(&answer);
    cursor.stalled = progress == Progress::Retry;
    match progress {
        Progress::Stored { durable_offset } => {
            // The follower's own account of where it is, since it did the
            // writing and a partially applied batch would leave the two
            // disagreeing — but never past the end of what was sent. The leader
            // has compared nothing beyond that, so accepting a higher answer
            // means resuming past records neither side has checked.
            cursor.next_offset = durable_offset.min(batch_end);
            cursor.confirmed = cursor.next_offset;
            cursor.shipped_bytes += batch_bytes as u64;
            metrics::record_shipped(metrics::OUTCOME_OK);
            // A rebuild is a full transfer, and the policy may say how fast.
            // Paced after the batch landed, so the follower is never waiting
            // on records that were already read.
            let rate = rebuilds.policy().bytes_per_sec;
            if cursor.rebuilding && rate > 0 {
                let nanos = (batch_bytes as u128 * 1_000_000_000) / rate as u128;
                let nanos = nanos.min(u64::MAX as u128) as u64;
                tokio::time::sleep(std::time::Duration::from_nanos(nanos)).await;
            }
        }
        Progress::Resume { offset } => {
            cursor.next_offset = offset;
            cursor.confirmed = cursor.confirmed.min(offset);
            metrics::record_shipped(metrics::OUTCOME_RESUMED);
        }
        Progress::Halted(halt) => {
            cursor.halted = Some(halt);
            tracing::error!(
                node_id = %cursor.node_id,
                stream = %shard.stream,
                shard = shard.shard,
                generation = shard.generation,
                reason = %halt,
                "replication to a follower stopped",
            );
            metrics::record_shipped(match halt {
                Halt::Diverged => metrics::OUTCOME_DIVERGED,
                Halt::Fenced => metrics::OUTCOME_FENCED,
                Halt::NeedsBootstrap => metrics::OUTCOME_NEEDS_BOOTSTRAP,
            });
        }
        Progress::Retry => metrics::record_shipped(metrics::OUTCOME_REFUSED),
        Progress::UpToDate => {}
    }
    progress
}

/// What one exchange with a follower concluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Progress {
    /// The follower stored the batch and is now at this offset.
    Stored { durable_offset: u64 },
    /// The follower wants a different offset. Not an error: it is how a
    /// follower that has fallen behind, or been rebuilt, is caught up.
    Resume { offset: u64 },
    /// Nothing to ship; the follower is level with this leader.
    UpToDate,
    /// Transient. The same batch may go again.
    Retry,
    /// Replication to this follower is over until the assignment changes.
    Halted(Halt),
}

/// Read the follower's answer.
///
/// Separate from the exchange so the table in this module's documentation is
/// testable without a follower on the other end.
pub fn read_answer(answer: &InternalMessage) -> Progress {
    match answer {
        InternalMessage::ReplicateOk(ok) => Progress::Stored {
            durable_offset: ok.durable_offset,
        },
        InternalMessage::ReplicateError(err) => match err.code {
            ErrorCode::LogGap => Progress::Resume {
                offset: err.expected_offset,
            },
            ErrorCode::LogConflict => Progress::Halted(Halt::Diverged),
            ErrorCode::FencedEpoch => Progress::Halted(Halt::Fenced),
            // Everything else is this moment rather than this pairing: the
            // follower is behind, busy, or could not read the frame.
            _ => Progress::Retry,
        },
        // Anything else is a peer that did not understand the request. Retrying
        // is harmless and the alternative -- treating it as divergence -- would
        // stop replication over a protocol confusion.
        _ => Progress::Retry,
    }
}

/// The same batch as its log's own kind, for a follower that refused the
/// committed one.
fn without_commit(request: InternalMessage) -> InternalMessage {
    let strip = |mut batch: ReplicateRecords| {
        batch.commit_offset = None;
        batch.generations = None;
        batch
    };
    match request {
        InternalMessage::ReplicateRecords(b) => InternalMessage::ReplicateRecords(strip(b)),
        InternalMessage::ReplicateMarkedRecords(b) => {
            InternalMessage::ReplicateMarkedRecords(strip(b))
        }
        InternalMessage::ReplicateCacheRecords(b) => {
            InternalMessage::ReplicateCacheRecords(strip(b))
        }
        InternalMessage::ReplicateGroupRecords(b) => {
            InternalMessage::ReplicateGroupRecords(strip(b))
        }
        InternalMessage::ReplicateDeadLetterRecords(b) => {
            InternalMessage::ReplicateDeadLetterRecords(strip(b))
        }
        InternalMessage::ReplicateCounterRecords(b) => {
            InternalMessage::ReplicateCounterRecords(strip(b))
        }
        other => other,
    }
}

pub(super) fn replica_log(log_kind: felix_broker::LogKind) -> ReplicaLog {
    match log_kind {
        felix_broker::LogKind::Stream => ReplicaLog::Stream,
        felix_broker::LogKind::Cache => ReplicaLog::Cache,
        felix_broker::LogKind::GroupCursors => ReplicaLog::GroupCursors,
        felix_broker::LogKind::GroupDeadLetters => ReplicaLog::GroupDeadLetters,
        felix_broker::LogKind::Counters => ReplicaLog::Counters,
    }
}

pub(super) fn unreachable_outcome(err: &PeerError) -> &'static str {
    match err {
        PeerError::Timeout { .. } => metrics::OUTCOME_TIMEOUT,
        PeerError::Disconnected { .. } => metrics::OUTCOME_DISCONNECTED,
        _ => metrics::OUTCOME_UNREACHABLE,
    }
}
