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

use std::sync::Arc;
use std::time::Duration;

use felix_common::random::HashMap;
use tokio::sync::watch;

use super::{ReplicaHandler, refuse_other_leader, refused, shard_key};
use crate::peer::metrics;

/// The longest a seal's acknowledgement holds its request before answering
/// that the check is still running. Under the default 5 s peer request
/// timeout, so the leader hears that rather than nothing.
const ACK_ANSWER_WAIT: Duration = Duration::from_secs(2);

/// Seal acknowledgements being checked, one per shard at most.
///
/// The check reads every record from the seal's start, which on a large
/// log outlasts a request's deadline. It runs off the request that asked:
/// a request that cannot wait it out is answered as retryable, and the
/// leader's next one finds the check running or its result.
pub(super) struct SealAcks {
    checks: Arc<parking_lot::Mutex<HashMap<felix_router::ShardKey, SealCheck>>>,
    pub(super) answer_wait: Duration,
    /// Each check takes a permit before it starts, so a test holds one up.
    #[cfg(test)]
    pub(super) gate: Option<Arc<tokio::sync::Semaphore>>,
    #[cfg(test)]
    pub(super) started: Arc<std::sync::atomic::AtomicUsize>,
}

impl Default for SealAcks {
    fn default() -> Self {
        Self {
            checks: Default::default(),
            answer_wait: ACK_ANSWER_WAIT,
            #[cfg(test)]
            gate: None,
            #[cfg(test)]
            started: Default::default(),
        }
    }
}

/// What a check concluded; a storage error as its text.
type Checked = Result<Result<(), SealRefused>, String>;

struct SealCheck {
    /// The request it checks, without its correlation id.
    of: SealAck,
    done: watch::Receiver<Option<Checked>>,
}

fn same_seal(a: &SealAck, b: &SealAck) -> bool {
    a.shard == b.shard
        && a.sealer == b.sealer
        && a.offset == b.offset
        && a.timestamp_micros == b.timestamp_micros
        && a.payload == b.payload
}

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
    /// with this broker's copy id once the record is on disk, or as
    /// unavailable while the check still runs. See [`SealAcks`].
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
        let generation = request.shard.generation;
        let offset = request.offset;
        let checked = match self.seal_checked(&key, request, &log, dead_letters, cursors) {
            Ok(done) => self.await_check(&key, done).await,
            Err(busy) => Err(busy),
        };
        let checked = match checked {
            Ok(checked) => checked,
            Err(why) => {
                metrics::record_replicated(metrics::OUTCOME_REFUSED);
                return refused(correlation_id, ErrorCode::Unavailable, 0, why.to_string());
            }
        };
        // The check may have finished before this request came: a later
        // generation taken since has ended the promise it answered under.
        let lapsed = || {
            log.disk_log()
                .seal_promise()
                .is_none_or(|promise| promise.generation != generation)
        };
        match checked {
            Ok(Ok(())) if lapsed() => seal_refusal(correlation_id, &SealRefused::NoPromise),
            Ok(Ok(())) => {
                tracing::info!(
                    stream = %key.stream,
                    shard = key.shard,
                    generation,
                    offset,
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
                refused(correlation_id, ErrorCode::StorageFailed, 0, err)
            }
        }
    }

    /// The check of `request`'s record against this copy: the one already
    /// running or finished for the shard, or a new one. Refused while the
    /// shard's check is of another record.
    fn seal_checked(
        &self,
        key: &felix_router::ShardKey,
        request: SealAck,
        log: &StreamLog,
        dead_letters: StreamLog,
        cursors: StreamLog,
    ) -> Result<watch::Receiver<Option<Checked>>, &'static str> {
        let mut checks = self.seal_acks.checks.lock();
        if let Some(check) = checks.get(key) {
            if same_seal(&check.of, &request) {
                return Ok(check.done.clone());
            }
            if check.done.borrow().is_none() {
                return Err("this copy is still checking another seal's record");
            }
        }
        let (finished, done) = watch::channel(None);
        checks.insert(
            key.clone(),
            SealCheck {
                of: request.clone(),
                done: done.clone(),
            },
        );
        #[cfg(test)]
        let gate = {
            self.seal_acks
                .started
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.seal_acks.gate.clone()
        };
        let log = log.clone();
        tokio::spawn(async move {
            #[cfg(test)]
            let _open = match &gate {
                Some(gate) => gate.acquire().await.ok(),
                None => None,
            };
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
            let checked = log
                .disk_log()
                .ack_seal(
                    &record,
                    StorageId::from_bytes(request.sealer),
                    dead_letters.disk_log(),
                    cursors.disk_log(),
                )
                .await
                .map_err(|err| err.to_string());
            // Kept by the map until a request reads it.
            let _ = finished.send(Some(checked));
        });
        Ok(done)
    }

    /// Wait for a check as long as a request may, and take its result out
    /// of the map: a repeat after that checks the copy again.
    async fn await_check(
        &self,
        key: &felix_router::ShardKey,
        mut done: watch::Receiver<Option<Checked>>,
    ) -> Result<Checked, &'static str> {
        let wait = self.seal_acks.answer_wait;
        let checked = match tokio::time::timeout(wait, done.wait_for(Option::is_some)).await {
            Ok(Ok(checked)) => checked.clone(),
            // The task ended without a result: it panicked.
            Ok(Err(_)) => Some(Err("the seal's check did not finish".to_string())),
            Err(_) => None,
        };
        let Some(checked) = checked else {
            return Err("this copy is still checking its records against the seal");
        };
        let mut checks = self.seal_acks.checks.lock();
        if checks
            .get(key)
            .is_some_and(|check| check.done.same_channel(&done))
        {
            checks.remove(key);
        }
        Ok(checked)
    }

    /// The refusal a write to `shard` gets while this copy holds a seal
    /// promise: a rebuild or a bootstrap at the promise's generation, an
    /// election's ballot always, a batch unless it comes from the sealing
    /// copy. `None` when no promise holds.
    ///
    /// A batch is checked after its own ballot, so a later generation taken
    /// there has already ended the promise. A rebuild is checked before
    /// its ballot, and a side log's ballot never ends the shard's promise,
    /// so one from a later generation is let through to its ballot here.
    pub(crate) async fn sealed_refusal(
        &self,
        correlation_id: u64,
        shard: &ShardRef,
        from: Option<StorageId>,
        write: SealedWrite,
    ) -> Option<InternalMessage> {
        let promise = self.seal_promise(shard).await?;
        let code = match (write, from) {
            (SealedWrite::Batch, Some(from)) if promise.admits_batch(from) => return None,
            (SealedWrite::Rebuild, _) if shard.generation > promise.generation => return None,
            // The sealing leader's own batch, sent before it took its
            // promise. Its next one names its copy, so this is no fence:
            // a fenced answer would halt this follower for the generation.
            (SealedWrite::Batch, None) if shard.generation == promise.generation => {
                ErrorCode::Unavailable
            }
            _ => ErrorCode::FencedEpoch,
        };
        metrics::record_replicated(metrics::OUTCOME_REFUSED);
        Some(refused(
            correlation_id,
            code,
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
