//! A follower's side of the seal that moves a shard to Raft: the seal's
//! fence, the acknowledgement of the record at `N`, and the writes a
//! promise refuses. The disk steps are
//! `felix_storage::disk_log::switch::seal`'s; see "Moving a shard from the
//! hybrid to Raft" in `docs/per-shard-raft.md`.

use felix_broker::{LogKind, StreamLog};
use felix_storage::StorageId;
use felix_storage::disk_log::switch::seal::{SealPromise, SealRefused};
use felix_storage::log::{FsyncMode, LogRecord, RecordMark};
use felix_wire::internal::{
    ErrorCode, InternalMessage, ReplicaLog, SealAck, SealAckOk, SealFence, SealFenceOk, SealStarts,
    ShardRef,
};

use super::{ReplicaHandler, refuse_other_leader, refused, shard_key};
use crate::peer::metrics;

impl ReplicaHandler {
    /// Take the seal's fence: the fence's checks and ballot, and the
    /// promise in place of a plain accept. Answered with where this copy's
    /// three logs start. A copy without dead-letter and cursor logs, or
    /// asked on another log than the shard's own, promises nothing.
    pub(crate) async fn seal_fence(
        &self,
        sender: Option<&str>,
        request: SealFence,
    ) -> InternalMessage {
        let correlation_id = request.fence.correlation_id;
        if request.fence.log != ReplicaLog::Stream || request.fence.election.is_some() {
            metrics::record_replicated(metrics::OUTCOME_REFUSED);
            return refused(
                correlation_id,
                ErrorCode::Malformed,
                0,
                "a seal fences a stream shard's own log, as a promotion".to_string(),
            );
        }
        let Some((dead_letters, cursors)) = self.side_logs(&request.fence.shard).await else {
            metrics::record_replicated(metrics::OUTCOME_REFUSED);
            return refused(
                correlation_id,
                ErrorCode::Unauthorized,
                0,
                "this broker holds no dead-letter or cursor log for that shard".to_string(),
            );
        };
        // Refused before the ballot: a copy that answered would count
        // toward the claim and then could never acknowledge `N`.
        if let Some(log) = self.stream_log(&request.fence.shard).await
            && [&log, &dead_letters, &cursors]
                .iter()
                .any(|log| log.disk_log().config().fsync_mode != FsyncMode::OnCommit)
        {
            metrics::record_replicated(metrics::OUTCOME_REFUSED);
            return refused(
                correlation_id,
                ErrorCode::Unauthorized,
                0,
                "a copy that does not fsync on commit takes no part in a seal".to_string(),
            );
        }
        let shard = request.fence.shard.clone();
        let sealer = StorageId::from_bytes(request.sealer);
        let _turn = self.fence_turn(sender).await;
        let answer = self
            .fence_in_turn(sender, false, Some(sealer), request.fence)
            .await;
        let InternalMessage::FenceOk(ok) = answer else {
            return answer;
        };
        let Some(log) = self.stream_log(&shard).await else {
            return refused(
                correlation_id,
                ErrorCode::Unauthorized,
                0,
                "this broker has no log for that shard".to_string(),
            );
        };
        InternalMessage::SealFenceOk(SealFenceOk {
            ok,
            starts: SealStarts {
                log: log.disk_log().base_offset(),
                dead_letters: dead_letters.disk_log().base_offset(),
                cursors: cursors.disk_log().base_offset(),
            },
        })
    }

    /// Check this copy against the seal's record and append it; answered
    /// with this broker's copy id once the record is on disk.
    pub(crate) async fn seal_ack(&self, sender: Option<&str>, request: SealAck) -> InternalMessage {
        let correlation_id = request.correlation_id;
        let Some(copy) = self.local_storage else {
            return refused(
                correlation_id,
                ErrorCode::Unavailable,
                0,
                "this broker has no storage identity to acknowledge the seal under".to_string(),
            );
        };
        let key = shard_key(&request.shard, LogKind::Stream);
        if let Some(refusal) = self.check_role(correlation_id, &key, request.shard.generation) {
            return refusal;
        }
        let (Some(log), Some((dead_letters, cursors))) = (
            self.stream_log(&request.shard).await,
            self.side_logs(&request.shard).await,
        ) else {
            return refused(
                correlation_id,
                ErrorCode::Unauthorized,
                0,
                "this broker has no log for that shard".to_string(),
            );
        };
        if let Some(refusal) =
            refuse_other_leader(&log, correlation_id, &key, request.shard.generation, sender)
        {
            return refusal;
        }
        let record = LogRecord {
            offset: request.offset,
            timestamp_micros: request.timestamp_micros,
            checksum: 0,
            payload: request.payload,
            mark: RecordMark::GenerationStart,
            publisher: None,
            headers: None,
            generation: Some(request.shard.generation),
        };
        match log
            .disk_log()
            .ack_seal(
                &record,
                StorageId::from_bytes(request.sealer),
                dead_letters.disk_log(),
                cursors.disk_log(),
            )
            .await
        {
            Ok(Ok(())) => {
                tracing::info!(
                    stream = %key.stream,
                    shard = key.shard,
                    generation = request.shard.generation,
                    offset = request.offset,
                    "acknowledged the seal's record; this copy may vote from its base",
                );
                InternalMessage::SealAckOk(SealAckOk {
                    correlation_id,
                    copy: copy.to_bytes(),
                })
            }
            Ok(Err(why)) => seal_refusal(correlation_id, &why),
            Err(err) => {
                metrics::record_replicated(metrics::OUTCOME_ERROR);
                refused(correlation_id, ErrorCode::StorageFailed, 0, err.to_string())
            }
        }
    }

    /// The refusal a write to `shard` gets while this copy holds a seal
    /// promise: a rebuild, a bootstrap or an election's ballot always, a
    /// batch unless it comes from the sealing copy. `None` when no promise
    /// holds. Read after the write's own ballot: a later generation taken
    /// there has already ended the promise.
    pub(crate) async fn sealed_refusal(
        &self,
        correlation_id: u64,
        shard: &ShardRef,
        from: Option<StorageId>,
        write: SealedWrite,
    ) -> Option<InternalMessage> {
        let promise = self.seal_promise(shard).await?;
        if write == SealedWrite::Batch
            && from.is_some_and(|from: StorageId| promise.admits_batch(from))
        {
            return None;
        }
        metrics::record_replicated(metrics::OUTCOME_REFUSED);
        Some(refused(
            correlation_id,
            ErrorCode::FencedEpoch,
            0,
            format!(
                "this copy is sealed at generation {} for the switch to Raft: {}, until a later \
                 generation reopens the shard",
                promise.generation,
                write.what(),
            ),
        ))
    }

    /// The promise `shard`'s copy holds, if any.
    pub(crate) async fn seal_promise(&self, shard: &ShardRef) -> Option<SealPromise> {
        self.stream_log(shard).await?.disk_log().seal_promise()
    }

    async fn stream_log(&self, shard: &ShardRef) -> Option<StreamLog> {
        self.log_of(shard, LogKind::Stream).await
    }

    async fn side_logs(&self, shard: &ShardRef) -> Option<(StreamLog, StreamLog)> {
        Some((
            self.log_of(shard, LogKind::GroupDeadLetters).await?,
            self.log_of(shard, LogKind::GroupCursors).await?,
        ))
    }

    async fn log_of(&self, shard: &ShardRef, kind: LogKind) -> Option<StreamLog> {
        let key = shard_key(shard, kind);
        self.broker
            .shard_log_off_runtime(
                kind,
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
                None,
                self.expected_incarnation(&key, shard.incarnation),
            )
            .await
    }
}

/// What a sealed copy is asked to store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SealedWrite {
    Batch,
    Rebuild,
    Ballot,
}

impl SealedWrite {
    fn what(self) -> &'static str {
        match self {
            Self::Batch => "it takes batches only from the sealing copy",
            Self::Rebuild => "it takes no rebuild",
            Self::Ballot => "it takes no election ballot",
        }
    }
}

pub(crate) fn seal_refusal(correlation_id: u64, why: &SealRefused) -> InternalMessage {
    metrics::record_replicated(metrics::OUTCOME_REFUSED);
    let (code, expected) = match why {
        SealRefused::NoPromise | SealRefused::PromisedOther { .. } => (ErrorCode::FencedEpoch, 0),
        SealRefused::EndsAt { tail, .. } => (ErrorCode::LogConflict, *tail),
        _ => (ErrorCode::LogConflict, 0),
    };
    refused(
        correlation_id,
        code,
        expected,
        format!("the seal is refused: {why:?}"),
    )
}
