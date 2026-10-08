//! The follower's side: storing records the shard's leader shipped.
//!
//! Two checks stand between a batch and this broker's disk, and they answer
//! different questions:
//!
//! 1. **May this broker store these records at all?** Decided here, from the
//!    routing view: is this node in the shard's replica set, at the epoch the
//!    sender named? A sender at an older epoch has been superseded, and its
//!    records must not be stored — it may have written them after losing the
//!    shard. That is the fence, and it is the same one the durable-append check
//!    applies on the leader.
//! 2. **Do these records belong where the batch says?** Decided by
//!    [`felix_broker::replication::apply`], against the log's own tail.
//!
//! The order matters. Position is only meaningful once the sender is
//! established as the current leader: applying first and checking after would
//! let a fenced leader's bytes reach the disk, and records are never rewritten.
use std::collections::HashMap;
use std::sync::Arc;

use felix_broker::Broker;
use felix_broker::replication::{self, Divergence};
use felix_router::{ReplicaRole, ShardRouter};
use felix_storage::disk_log::GenerationCheck;
use felix_wire::internal::{
    ErrorCode, Fence, FenceOk, GenerationStart, InternalMessage, ReplicaLog, ReplicateBootstrap,
    ReplicateError, ReplicateFetch, ReplicateOk, ReplicateRebuild, ReplicateRecords,
};

/// The most one fetch answer carries, whatever the leader asked for.
const MAX_FETCH_BYTES: usize = 4 * 1024 * 1024;

use crate::peer::metrics;

/// Stores replicated records against this broker's local logs.
pub struct ReplicaHandler {
    broker: Arc<Broker>,
    router: Arc<ShardRouter>,
    /// How far each shard's records from before the current leader's
    /// generation have been compared with it, while that takes more than one
    /// batch. See [`unverified_from`].
    verified: parking_lot::Mutex<HashMap<felix_router::ShardKey, Verified>>,
    /// The generation whose leader shipped a record that disagreed with one
    /// this broker holds as committed, per log. A rebuild from that leader
    /// would only find the same disagreement again.
    mismatched: parking_lot::Mutex<HashMap<(felix_router::ShardKey, felix_broker::LogKind), u64>>,
}

/// Progress comparing a follower's older records with a newer leader's.
#[derive(Debug, Clone, Copy)]
struct Verified {
    generation: u64,
    through: u64,
}

impl ReplicaHandler {
    pub fn new(broker: Arc<Broker>, router: Arc<ShardRouter>) -> Self {
        Self {
            broker,
            router,
            verified: parking_lot::Mutex::new(HashMap::new()),
            mismatched: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    /// Begin this shard's log where the leader's surviving log begins.
    ///
    /// The leader has nothing older left, so the records below `base_offset`
    /// are gone from every copy: a log that starts there is complete rather
    /// than truncated. The follower cannot work that out for itself, which is
    /// why the leader has to say it.
    ///
    /// **A follower holding records of its own refuses.** Discarding them is an
    /// operator's decision, not a leader's — and a log placed over them would
    /// have a hole between what it held and what it was given, which nothing
    /// downstream could detect.
    /// Discard this broker's copy of one of the shard's logs and start again
    /// at the leader's base.
    ///
    /// Only at the leader's request, and only for a shard this broker follows
    /// at the named generation: the leader decided the copy is not worth
    /// keeping, and the leader's copy is the one the majority holds. The
    /// records go, the generation history goes with them, and the answer is
    /// where the new copy begins.
    pub async fn rebuild(
        &self,
        sender: Option<&str>,
        request: ReplicateRebuild,
    ) -> InternalMessage {
        let correlation_id = request.correlation_id;
        let log_kind = match request.log {
            ReplicaLog::Stream => felix_broker::LogKind::Stream,
            ReplicaLog::Cache => felix_broker::LogKind::Cache,
            ReplicaLog::GroupCursors => felix_broker::LogKind::GroupCursors,
            ReplicaLog::GroupDeadLetters => felix_broker::LogKind::GroupDeadLetters,
            ReplicaLog::Counters => felix_broker::LogKind::Counters,
        };
        let key = shard_key(&request.shard, log_kind);
        if let Some(refusal) = self.check_role(correlation_id, &key, request.shard.generation) {
            return refusal;
        }
        if let Some(refusal) = self
            .check_shard_fence(
                correlation_id,
                &key,
                log_kind,
                request.shard.generation,
                sender,
            )
            .await
        {
            return refusal;
        }
        let Some(log) = self
            .broker
            .shard_log_at(
                log_kind,
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
                request.base_offset,
            )
            .await
        else {
            metrics::record_replicated(metrics::OUTCOME_REFUSED);
            return refused(
                correlation_id,
                ErrorCode::Unauthorized,
                0,
                "this broker has no log for that shard".to_string(),
            );
        };
        if let Some(refusal) =
            accept_sender(&log, correlation_id, &key, request.shard.generation, sender).await
        {
            return refusal;
        }
        // Records below the leader's base are gone from the leader too, so only
        // what this broker holds from there up is at stake.
        let from = log.base_offset().max(request.base_offset);
        if let Some(commit) = cuts_committed(&log, from).await {
            return self
                .rebuild_above_commit(correlation_id, &key, log_kind, &log, &request, from, commit)
                .await;
        }
        if let Err(err) = log.rebuild_at(request.base_offset).await {
            metrics::record_replicated(metrics::OUTCOME_ERROR);
            return refused(correlation_id, ErrorCode::StorageFailed, 0, err.to_string());
        }
        // The in-memory tail was built from the records that just went.
        self.reset_tail(&key, log_kind, request.base_offset).await;
        tracing::warn!(
            stream = %key.stream,
            shard = key.shard,
            log = ?request.log,
            generation = request.shard.generation,
            base_offset = request.base_offset,
            "discarded this broker's copy of a shard at the leader's request; rebuilding",
        );
        metrics::record_replicated(metrics::OUTCOME_REBUILT);
        InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id,
            durable_offset: request.base_offset,
        })
    }

    /// A rebuild that reaches below this broker's commit offset keeps the
    /// committed records and drops only what lies past them.
    ///
    /// The answer is `from`, so the leader ships again from there and every
    /// kept record is compared byte for byte with the leader's before
    /// anything lands after it. A committed record the leader disagrees with
    /// shows up as a conflict below the commit offset, which halts and is
    /// remembered, so the next rebuild at that generation is refused instead
    /// of repeating the transfer (#863).
    #[allow(clippy::too_many_arguments)]
    async fn rebuild_above_commit(
        &self,
        correlation_id: u64,
        key: &felix_router::ShardKey,
        log_kind: felix_broker::LogKind,
        log: &felix_broker::StreamLog,
        request: &ReplicateRebuild,
        from: u64,
        commit: u64,
    ) -> InternalMessage {
        let generation = request.shard.generation;
        if self.committed_mismatch(key, log_kind) == Some(generation) {
            tracing::error!(
                stream = %key.stream,
                shard = key.shard,
                log = ?request.log,
                generation,
                base_offset = request.base_offset,
                commit_offset = commit,
                "refusing to rebuild: this leader disagreed with a record this broker \
                 holds as committed, so it may not hold every committed record",
            );
            metrics::record_replicated(metrics::OUTCOME_BELOW_COMMIT);
            return refused(
                correlation_id,
                ErrorCode::LogConflict,
                0,
                format!(
                    "this broker holds committed records below {commit} that generation \
                     {generation} disagreed with"
                ),
            );
        }
        let tail = match log.tail_offset().await {
            Ok(tail) => tail,
            Err(err) => {
                metrics::record_replicated(metrics::OUTCOME_ERROR);
                return refused(correlation_id, ErrorCode::StorageFailed, 0, err.to_string());
            }
        };
        let keep = commit.min(tail);
        if keep < tail {
            if let Err(err) = log.truncate(keep).await {
                metrics::record_replicated(metrics::OUTCOME_ERROR);
                return refused(correlation_id, ErrorCode::StorageFailed, 0, err.to_string());
            }
            self.reset_tail(key, log_kind, keep).await;
        }
        tracing::warn!(
            stream = %key.stream,
            shard = key.shard,
            log = ?request.log,
            generation,
            base_offset = request.base_offset,
            commit_offset = commit,
            dropped = tail - keep,
            "kept the committed records at the leader's request to rebuild; \
             they are compared again as the leader re-ships them",
        );
        metrics::record_replicated(metrics::OUTCOME_REBUILT);
        InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id,
            durable_offset: from,
        })
    }

    /// The generation whose leader last disagreed with a committed record of
    /// this log, if any.
    fn committed_mismatch(
        &self,
        key: &felix_router::ShardKey,
        log_kind: felix_broker::LogKind,
    ) -> Option<u64> {
        self.mismatched
            .lock()
            .get(&(key.clone(), log_kind))
            .copied()
    }

    /// What the broker derived from a log's records -- a stream's replay ring
    /// and next offset, a cache's index, a counter shard's sums -- describes
    /// the records a rebuild or truncation just dropped, so it is reset to
    /// where the log ends now.
    async fn reset_tail(
        &self,
        key: &felix_router::ShardKey,
        log_kind: felix_broker::LogKind,
        tail: u64,
    ) {
        if let Err(err) = self
            .broker
            .reset_log(
                log_kind,
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
                tail,
            )
            .await
        {
            tracing::warn!(
                stream = %key.stream,
                shard = key.shard,
                log = ?log_kind,
                error = %err,
                "dropped records from the log but could not reset what was read from them",
            );
        }
    }

    pub async fn bootstrap(
        &self,
        sender: Option<&str>,
        request: ReplicateBootstrap,
        log_kind: felix_broker::LogKind,
    ) -> InternalMessage {
        let correlation_id = request.correlation_id;
        let key = shard_key(&request.shard, log_kind);

        if let Some(refusal) = self.check_role(correlation_id, &key, request.shard.generation) {
            return refusal;
        }
        if let Some(refusal) = self
            .check_shard_fence(
                correlation_id,
                &key,
                log_kind,
                request.shard.generation,
                sender,
            )
            .await
        {
            return refusal;
        }
        // Creates the log at `base_offset` when this broker has never held the
        // shard, and opens what is there otherwise. The base it comes back with
        // is the authority either way.
        let Some(log) = self
            .broker
            .shard_log_at(
                log_kind,
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
                request.base_offset,
            )
            .await
        else {
            metrics::record_replicated(metrics::OUTCOME_REFUSED);
            return refused(
                correlation_id,
                ErrorCode::Unauthorized,
                0,
                "this broker has no log for that shard".to_string(),
            );
        };

        if let Some(refusal) =
            accept_sender(&log, correlation_id, &key, request.shard.generation, sender).await
        {
            return refusal;
        }

        let mut base = log.base_offset();
        let mut tail = match log.tail_offset().await {
            Ok(tail) => tail,
            Err(err) => {
                metrics::record_replicated(metrics::OUTCOME_ERROR);
                return refused(correlation_id, ErrorCode::StorageFailed, 0, err.to_string());
            }
        };

        // An empty log has nothing to keep, so it moves to the leader's base.
        // A new copy of a trimmed shard is usually one: the leader's first
        // batch opened it at 0 before the leader found it needed this offer.
        // Refused, it would wait for a rebuild slot, or for ever with none.
        if tail == base && base < request.base_offset {
            if let Err(err) = log.rebuild_at(request.base_offset).await {
                metrics::record_replicated(metrics::OUTCOME_ERROR);
                return refused(correlation_id, ErrorCode::StorageFailed, 0, err.to_string());
            }
            self.reset_tail(&key, log_kind, request.base_offset).await;
            (base, tail) = (request.base_offset, request.base_offset);
        }

        // What matters is whether the two logs meet, not whether they start in
        // the same place. Retention and cache compaction trim brokers at their
        // own pace, so bases differing is ordinary — and demanding they match
        // refused bootstraps that had no gap in them, which is how a replica
        // set quietly shrinks over successive failovers.
        //
        // They meet when this broker's records span the leader's base: it holds
        // everything from there up to `tail`, and the leader ships on from
        // `tail`. Either side of that is a real hole.
        let covers_base = base <= request.base_offset && tail >= request.base_offset;
        if !covers_base {
            metrics::record_replicated(metrics::OUTCOME_CONFLICT);
            let why = if tail < request.base_offset {
                "its records end before the leader's begin"
            } else {
                "its records begin after the leader's"
            };
            tracing::error!(
                stream = %key.stream,
                shard = key.shard,
                held_from = base,
                held_to = tail,
                offered_from = request.base_offset,
                "refusing to bootstrap: {why}",
            );
            return refused(
                correlation_id,
                ErrorCode::LogConflict,
                tail,
                format!(
                    "this broker holds {base}..{tail} and the leader offers from {}: {why}",
                    request.base_offset
                ),
            );
        }
        tracing::info!(
            stream = %key.stream,
            shard = key.shard,
            base_offset = base,
            "shard log placed for bootstrap",
        );
        metrics::record_replicated(metrics::OUTCOME_BOOTSTRAPPED);
        InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id,
            durable_offset: tail,
        })
    }

    pub async fn apply(
        &self,
        sender: Option<&str>,
        batch: ReplicateRecords,
        log_kind: felix_broker::LogKind,
    ) -> InternalMessage {
        let correlation_id = batch.correlation_id;
        let key = felix_router::ShardKey {
            tenant_id: batch.shard.tenant_id.clone(),
            namespace: batch.shard.namespace.clone(),
            stream: batch.shard.stream.clone(),
            shard: batch.shard.shard,
            // The cursors belong to their stream's shard, so ownership is
            // checked against that shard rather than a placement of their own.
            kind: match log_kind {
                // The counter log belongs to its cache's shard, so ownership
                // is checked against the cache's placement — the cursors make
                // the same argument about their stream.
                felix_broker::LogKind::Cache | felix_broker::LogKind::Counters => {
                    felix_router::ShardKind::Cache
                }
                felix_broker::LogKind::Stream
                | felix_broker::LogKind::GroupCursors
                | felix_broker::LogKind::GroupDeadLetters => felix_router::ShardKind::Stream,
            },
        };

        if let Some(refusal) = self.check_role(correlation_id, &key, batch.shard.generation) {
            return refusal;
        }
        if let Some(refusal) = self
            .check_shard_fence(
                correlation_id,
                &key,
                log_kind,
                batch.shard.generation,
                sender,
            )
            .await
        {
            return refusal;
        }

        // A replica set naming a broker with no log for the shard is a
        // configuration error, not a transient one. Saying so beats accepting
        // and silently keeping nothing.
        let Some(log) = self
            .broker
            .shard_log(
                log_kind,
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
            )
            .await
        else {
            metrics::record_replicated(metrics::OUTCOME_REFUSED);
            return refused(
                correlation_id,
                ErrorCode::Unauthorized,
                0,
                "this broker has no log for that shard".to_string(),
            );
        };

        if let Some(refusal) =
            accept_sender(&log, correlation_id, &key, batch.shard.generation, sender).await
        {
            return refusal;
        }

        // A generation this follower has not seen before starts here. Recorded
        // before the apply, because the apply is what may need it: the divergent
        // suffix it finds belongs to whatever generation was newest until now.
        let previous_generation = log.generations().last().copied();

        // A newer leader's batch that starts past records this follower wrote
        // under an older generation would leave them uncompared, and one of
        // them may be a dead leader's unacknowledged write at an offset the new
        // leader filled differently. Send the leader back to the first of them.
        let verified = self
            .verified
            .lock()
            .get(&key)
            .filter(|verified| verified.generation == batch.shard.generation)
            .map(|verified| verified.through);
        let unverified = match log.tail_offset().await {
            Ok(tail) => unverified_from(
                previous_generation,
                batch.shard.generation,
                log.commit_offset(),
                verified,
                tail,
            ),
            Err(_) => None,
        };
        let mut outcome = match unverified {
            Some(expected) if batch.first_offset > expected => Ok(Err(Divergence::Gap {
                expected,
                first_offset: batch.first_offset,
            })),
            _ => {
                replication::apply(
                    &log,
                    batch.first_offset,
                    batch.checksum,
                    &batch.payloads,
                    &batch.marks,
                    &batch.publishers,
                )
                .await
            }
        };

        // A divergent suffix left by a leader that is gone is droppable: no
        // majority acknowledged it, and dropping it lets this follower rejoin
        // instead of halting until an operator notices (#406).
        //
        // Two conditions decide that, and neither is optional:
        //
        // - The sender's generation is newer than the one this follower last
        //   accepted. A leader disagreeing with *itself* is an inconsistency,
        //   not a predecessor's leftovers, and repairing it would let a leader
        //   rewrite its own history.
        // - The divergence is at or after where that older generation began, so
        //   what is dropped belongs to it.
        //
        // Anything else halts. Without the generation history there is no way
        // to tell a suffix from a divergence reaching further back, and
        // truncating on a bare conflict would discard records nothing has
        // established are safe to lose.
        if let Ok(Err(Divergence::Conflict { offset, .. })) = &outcome {
            let diverged_at = *offset;
            // Below `verified` the records came from this sender already, and
            // a leader disagreeing with its own records is not repairable.
            let repairable = previous_generation.filter(|previous| {
                batch.shard.generation > previous.generation
                    && diverged_at >= previous.start_offset
                    && verified.is_none_or(|through| diverged_at >= through)
            });
            // A suffix that reaches below the commit offset is not a dead
            // leader's leftovers: a majority acknowledged part of it. Nor is it
            // anything a rebuild from this leader could repair, so the next
            // one at this generation is refused.
            let below_commit = cuts_committed(&log, diverged_at).await;
            if let Some(commit) = below_commit {
                self.mismatched
                    .lock()
                    .insert((key.clone(), log_kind), batch.shard.generation);
                tracing::error!(
                    stream = %key.stream,
                    shard = key.shard,
                    diverged_at,
                    commit_offset = commit,
                    sender_generation = batch.shard.generation,
                    "the leader disagrees with a committed record here; refusing \
                     to drop it, and replication stops here",
                );
                metrics::record_replicated(metrics::OUTCOME_BELOW_COMMIT);
            }
            match repairable.filter(|_| below_commit.is_none()) {
                Some(previous) => match log.truncate(diverged_at).await {
                    Ok(()) => {
                        tracing::warn!(
                            stream = %key.stream,
                            shard = key.shard,
                            diverged_at,
                            dropped_generation = previous.generation,
                            "dropped a divergent suffix from a previous \
                             generation and resumed replication",
                        );
                        metrics::record_replicated(metrics::OUTCOME_TRUNCATED);
                        // What was read from the dropped records (a stream's
                        // replay ring, a cache's index) still describes them,
                        // and the apply below only moves it forward, so it
                        // would be left for readers if this broker is promoted.
                        self.reset_tail(&key, log_kind, diverged_at).await;
                        outcome = replication::apply(
                            &log,
                            batch.first_offset,
                            batch.checksum,
                            &batch.payloads,
                            &batch.marks,
                            &batch.publishers,
                        )
                        .await;
                    }
                    Err(err) => tracing::error!(
                        stream = %key.stream,
                        shard = key.shard,
                        error = %err,
                        "could not drop a divergent suffix; replication stops here",
                    ),
                },
                None if below_commit.is_some() => {}
                None => tracing::error!(
                    stream = %key.stream,
                    shard = key.shard,
                    diverged_at,
                    sender_generation = batch.shard.generation,
                    last_accepted = ?previous_generation.map(|epoch| epoch.generation),
                    "this divergence is not a previous generation's suffix; \
                     replication stops here",
                ),
            }
        }

        match outcome {
            // A newer leader was accepted while this batch was being stored.
            // Its records stay, as any divergent suffix does, but this sender
            // must not count them toward its quorum.
            Ok(Ok(_)) if log.accepted_generation() > batch.shard.generation => {
                metrics::record_replicated(metrics::OUTCOME_FENCED);
                refused(
                    correlation_id,
                    ErrorCode::FencedEpoch,
                    0,
                    format!(
                        "this broker accepted generation {} while storing a batch at {}",
                        log.accepted_generation(),
                        batch.shard.generation
                    ),
                )
            }
            Ok(Ok(applied)) => {
                // Only what this batch left level with the leader: past the
                // durable offset nothing here has been compared.
                if let Some(commit) = batch.commit_offset
                    && let Err(err) = log
                        .advance_commit_offset(commit.min(applied.durable_offset))
                        .await
                {
                    tracing::warn!(
                        stream = %key.stream,
                        error = %err,
                        "could not write the commit offset through; it holds in memory",
                    );
                }
                // Past the durable offset nothing has been compared, so what
                // this follower holds there keeps its labels until it has.
                let level = log
                    .tail_offset()
                    .await
                    .is_ok_and(|tail| applied.durable_offset >= tail);
                if level
                    && let Err(err) = label_appended(
                        &log,
                        applied.durable_offset - applied.appended as u64,
                        applied.durable_offset,
                        batch.shard.generation,
                        batch.generations.as_deref(),
                    )
                {
                    tracing::warn!(
                        stream = %key.stream,
                        error = %err,
                        "stored a replicated batch but could not record its generation",
                    );
                }
                // Until the sender's own generation is recorded here, the
                // newest one recorded is older, and `unverified_from` would
                // send the sender back over records it has just compared.
                // Remembered in memory only: after a restart they are compared
                // once more.
                let caught_up = log
                    .generations()
                    .last()
                    .is_some_and(|newest| newest.generation >= batch.shard.generation);
                if caught_up {
                    self.verified.lock().remove(&key);
                } else {
                    self.verified.lock().insert(
                        key.clone(),
                        Verified {
                            generation: batch.shard.generation,
                            through: applied.durable_offset,
                        },
                    );
                }
                // The records went straight to the log, so the stream's own view
                // of its tail has to be told. Without this the first publish
                // this broker accepts once promoted waits on commit turns that
                // were never taken.
                //
                // A cache needs no equivalent: it has no commit sequencer, and
                // its index notices records that arrived underneath it the next
                // time the shard is read.
                if log_kind == felix_broker::LogKind::Stream
                    && let Err(err) = self
                        .broker
                        .adopt_replicated(
                            &key.tenant_id,
                            &key.namespace,
                            &key.stream,
                            key.shard,
                            applied.durable_offset,
                        )
                        .await
                {
                    tracing::warn!(
                        stream = %key.stream,
                        error = %err,
                        "stored a replicated batch but could not advance the stream's tail",
                    );
                }
                metrics::record_replicated(metrics::OUTCOME_OK);
                InternalMessage::ReplicateOk(ReplicateOk {
                    correlation_id,
                    durable_offset: applied.durable_offset,
                })
            }
            Ok(Err(divergence)) => {
                metrics::record_replicated(divergence_outcome(&divergence));
                refused(
                    correlation_id,
                    divergence_code(&divergence),
                    divergence.expected_offset(),
                    divergence.to_string(),
                )
            }
            Err(err) => {
                // This broker's own disk failed. Distinct from every refusal
                // above: nothing is wrong with what the leader sent, so the
                // leader must not treat it as divergence and stop.
                metrics::record_replicated(metrics::OUTCOME_ERROR);
                refused(correlation_id, ErrorCode::StorageFailed, 0, err.to_string())
            }
        }
    }

    /// Take a promoted leader's fence: persist its generation, so every older
    /// leader is refused from here on, and answer where this copy of the
    /// shard's log stands. `AnswerFence` in `docs/formal/FelixShard.tla`.
    ///
    /// The generation is written to the shard's own log, stream or cache.
    /// Its cursor, dead-letter and counter logs check that one as well as
    /// their own, so the fence covers them without a request each. A
    /// promoted cache leader fences the counter log as well, after the cache
    /// log, for where it ends: the answer is the counter log's.
    pub async fn fence(&self, sender: Option<&str>, request: Fence) -> InternalMessage {
        let correlation_id = request.correlation_id;
        let generation = request.shard.generation;
        let log_kind = match request.log {
            ReplicaLog::Stream => felix_broker::LogKind::Stream,
            ReplicaLog::Cache => felix_broker::LogKind::Cache,
            ReplicaLog::Counters => felix_broker::LogKind::Counters,
            other => {
                metrics::record_replicated(metrics::OUTCOME_REFUSED);
                return refused(
                    correlation_id,
                    ErrorCode::Malformed,
                    0,
                    format!("a fence names a shard's own log, not {other:?}"),
                );
            }
        };
        let key = shard_key(&request.shard, log_kind);
        // The fence comes with a promotion, usually before this broker's
        // routing view has the new generation. Behind is the normal case here,
        // not a reason to make the new leader wait for the watch.
        match self.router.replica_role(&key, generation) {
            ReplicaRole::Follower | ReplicaRole::Behind { .. } => {}
            _ => {
                if let Some(refusal) = self.check_role(correlation_id, &key, generation) {
                    return refusal;
                }
            }
        }
        let Some(log) = self
            .broker
            .shard_log(
                log_kind,
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
            )
            .await
        else {
            // Nowhere to keep the promise, so no promise: the leader must not
            // count this broker toward its majority.
            metrics::record_replicated(metrics::OUTCOME_REFUSED);
            return refused(
                correlation_id,
                ErrorCode::Unauthorized,
                0,
                "this broker has no log for that shard".to_string(),
            );
        };
        if let Some(refusal) = self
            .check_shard_fence(correlation_id, &key, log_kind, generation, sender)
            .await
        {
            return refusal;
        }
        // A cache's counter log takes its leader's generation on its own, so
        // a newer leader that has only written counters has reached this
        // replica there and nowhere else. Refused, or an older leader's read
        // round would count this replica for a shard it no longer leads.
        if log_kind == felix_broker::LogKind::Cache {
            let counters = self
                .broker
                .shard_log(
                    felix_broker::LogKind::Counters,
                    &key.tenant_id,
                    &key.namespace,
                    &key.stream,
                    key.shard,
                )
                .await
                .map_or(0, |log| log.accepted_generation());
            if counters > generation {
                metrics::record_replicated(metrics::OUTCOME_FENCED);
                return refused(
                    correlation_id,
                    ErrorCode::FencedEpoch,
                    0,
                    format!(
                        "this broker accepted generation {counters}, the sender is at {generation}"
                    ),
                );
            }
        }
        // A fence at the generation already accepted changes nothing here: it
        // is the leader confirming it still leads, once per read round. From
        // any other node it is refused below, by the ballot.
        let confirming = log.accepted_generation() == generation;
        if let Some(refusal) = accept_sender(&log, correlation_id, &key, generation, sender).await {
            return refusal;
        }
        // Read after the generation is on disk: a batch from the old leader
        // that was being stored as the fence landed is either in this answer
        // or refused as fenced once it finishes.
        let log_end = match log.tail_offset().await {
            Ok(tail) => tail,
            Err(err) => {
                metrics::record_replicated(metrics::OUTCOME_ERROR);
                return refused(correlation_id, ErrorCode::StorageFailed, 0, err.to_string());
            }
        };
        if confirming {
            metrics::record_replicated(metrics::OUTCOME_FENCE_CONFIRMED);
        } else {
            tracing::info!(
                stream = %key.stream,
                shard = key.shard,
                generation,
                log_end,
                "fenced by a promoted leader; older leaders are refused from here on",
            );
            metrics::record_replicated(metrics::OUTCOME_FENCE_TAKEN);
        }
        InternalMessage::FenceOk(FenceOk {
            correlation_id,
            log_end,
            commit_offset: log.commit_offset(),
            last_generation: last_generation(&log.generations(), log_end),
        })
    }

    /// Serve the leader that fenced this replica its copy of the shard's log
    /// from `from_offset`. The catch-up in `AnswerFence`: a new leader takes
    /// the log of a replica ahead of it before it opens for writes.
    ///
    /// Only for a leader at exactly the generation this replica last
    /// accepted, which is the one that fenced it.
    pub async fn fetch(&self, sender: Option<&str>, request: ReplicateFetch) -> InternalMessage {
        let correlation_id = request.correlation_id;
        let generation = request.shard.generation;
        let log_kind = match request.log {
            ReplicaLog::Stream => felix_broker::LogKind::Stream,
            ReplicaLog::Cache => felix_broker::LogKind::Cache,
            ReplicaLog::Counters => felix_broker::LogKind::Counters,
            other => {
                return refused(
                    correlation_id,
                    ErrorCode::Malformed,
                    0,
                    format!("a fetch reads a shard's own log, not {other:?}"),
                );
            }
        };
        let key = shard_key(&request.shard, log_kind);
        match self.router.replica_role(&key, generation) {
            ReplicaRole::Follower | ReplicaRole::Behind { .. } => {}
            _ => {
                if let Some(refusal) = self.check_role(correlation_id, &key, generation) {
                    return refusal;
                }
            }
        }
        let Some(log) = self
            .broker
            .shard_log(
                log_kind,
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
            )
            .await
        else {
            return refused(
                correlation_id,
                ErrorCode::Unauthorized,
                0,
                "this broker has no log for that shard".to_string(),
            );
        };
        if let Some(refusal) = self
            .check_shard_fence(correlation_id, &key, log_kind, generation, sender)
            .await
        {
            return refusal;
        }
        let accepted = log.accepted_generation();
        if let Some(refusal) = refuse_other_leader(&log, correlation_id, &key, generation, sender) {
            return refusal;
        }
        if accepted != generation {
            let code = if accepted > generation {
                ErrorCode::FencedEpoch
            } else {
                // Not fenced by this leader yet; it fences, then reads.
                ErrorCode::StaleRoute
            };
            return refused(
                correlation_id,
                code,
                0,
                format!(
                    "this broker accepted generation {accepted}, the reader is at {generation}"
                ),
            );
        }
        let max_bytes = (request.max_bytes as usize).clamp(1, MAX_FETCH_BYTES);
        let records = match log.read_log_from(request.from_offset, max_bytes).await {
            Ok(records) => records,
            Err(err) => {
                return refused(correlation_id, ErrorCode::StorageFailed, 0, err.to_string());
            }
        };
        let marks: Vec<felix_wire::internal::ProducerMark> = if records
            .iter()
            .any(|record| record.mark != felix_storage::log::RecordMark::None)
        {
            records
                .iter()
                .map(|record| replication::mark_to_wire(record.mark))
                .collect()
        } else {
            Vec::new()
        };
        let first_offset = records
            .first()
            .map_or(request.from_offset, |record| record.offset);
        let publishers = replication::publishers_to_wire(&records);
        let payloads: Vec<bytes::Bytes> =
            records.into_iter().map(|record| record.payload).collect();
        let end = first_offset + payloads.len() as u64;
        let mut batch = ReplicateRecords {
            correlation_id,
            shard: request.shard,
            first_offset,
            checksum: felix_wire::internal::batch_checksum(&payloads, &marks, &publishers),
            payloads,
            marks,
            commit_offset: None,
            generations: request
                .labelled
                .then(|| generations_over(&log.generations(), first_offset, end)),
            publishers,
        };
        crate::ship::carry_marks(&mut batch, log_kind, false);
        match log_kind {
            felix_broker::LogKind::Cache => InternalMessage::ReplicateCacheRecords(batch),
            felix_broker::LogKind::Counters => InternalMessage::ReplicateCounterRecords(batch),
            _ if !batch.marks.is_empty() => InternalMessage::ReplicateMarkedRecords(batch),
            _ => InternalMessage::ReplicateRecords(batch),
        }
    }

    /// Refuse a sender to one of a shard's other logs once the shard's own
    /// log has accepted a newer leader, or another leader at the sender's
    /// generation, which is where a fence is kept.
    async fn check_shard_fence(
        &self,
        correlation_id: u64,
        key: &felix_router::ShardKey,
        log_kind: felix_broker::LogKind,
        generation: u64,
        sender: Option<&str>,
    ) -> Option<InternalMessage> {
        let own = match log_kind {
            felix_broker::LogKind::Stream | felix_broker::LogKind::Cache => return None,
            felix_broker::LogKind::GroupCursors | felix_broker::LogKind::GroupDeadLetters => {
                felix_broker::LogKind::Stream
            }
            felix_broker::LogKind::Counters => felix_broker::LogKind::Cache,
        };
        let log = self
            .broker
            .shard_log(own, &key.tenant_id, &key.namespace, &key.stream, key.shard)
            .await?;
        if let Some(refusal) = refuse_other_leader(&log, correlation_id, key, generation, sender) {
            return Some(refusal);
        }
        let accepted = log.accepted_generation();
        (accepted > generation).then(|| {
            metrics::record_replicated(metrics::OUTCOME_FENCED);
            refused(
                correlation_id,
                ErrorCode::FencedEpoch,
                0,
                format!(
                    "this broker accepted generation {accepted}, the sender is at {generation}"
                ),
            )
        })
    }

    /// May this broker store anything for `key` at `generation`?
    ///
    /// Shared by both entry points on purpose: storing records and placing the
    /// log that holds them are the same authority question, and a fence applied
    /// to one and not the other is a fence with a way round it.
    fn check_role(
        &self,
        correlation_id: u64,
        key: &felix_router::ShardKey,
        generation: u64,
    ) -> Option<InternalMessage> {
        match self.router.replica_role(key, generation) {
            ReplicaRole::Follower => None,
            ReplicaRole::Fenced { have, named } => {
                metrics::record_replicated(metrics::OUTCOME_FENCED);
                Some(refused(
                    correlation_id,
                    ErrorCode::FencedEpoch,
                    0,
                    format!("this broker is at generation {have}, the sender at {named}"),
                ))
            }
            ReplicaRole::Behind { have, named } => {
                // Not a refusal of the leader, only of this moment: the watch
                // has not caught up. Retryable, and the leader will find us
                // ready once it has.
                metrics::record_replicated(metrics::OUTCOME_BEHIND);
                Some(refused(
                    correlation_id,
                    ErrorCode::StaleRoute,
                    0,
                    format!("this broker is at generation {have}, the sender at {named}"),
                ))
            }
            ReplicaRole::NotAReplica => {
                metrics::record_replicated(metrics::OUTCOME_REFUSED);
                Some(refused(
                    correlation_id,
                    ErrorCode::Unauthorized,
                    0,
                    "this broker is not a replica of that shard".to_string(),
                ))
            }
        }
    }
}

/// Refuse a sender older than a leader this log already accepted, or another
/// node at the generation it accepted, and persist a newer one, with its
/// ballot, before anything it sends is stored or acknowledged.
///
/// The routing view in `check_role` is rebuilt after a restart and may lag;
/// this is what still refuses a leader this broker has seen superseded.
/// `sender` is the node id the peer gave in its `Hello`, or `None` when this
/// broker does not keep ballots, and then only the generation is checked.
async fn accept_sender(
    log: &felix_broker::StreamLog,
    correlation_id: u64,
    key: &felix_router::ShardKey,
    generation: u64,
    sender: Option<&str>,
) -> Option<InternalMessage> {
    match log.accept_generation(generation, sender).await {
        Ok(GenerationCheck::Current | GenerationCheck::Raised) => None,
        Ok(GenerationCheck::Promised { leader }) => Some(promised_elsewhere(
            correlation_id,
            key,
            generation,
            &leader,
            sender,
        )),
        Ok(GenerationCheck::Superseded { accepted }) => {
            tracing::warn!(
                stream = %key.stream,
                shard = key.shard,
                accepted,
                sender_generation = generation,
                "refusing a leader older than one this replica already accepted",
            );
            metrics::record_replicated(metrics::OUTCOME_FENCED);
            Some(refused(
                correlation_id,
                ErrorCode::FencedEpoch,
                0,
                format!(
                    "this broker accepted generation {accepted}, the sender is at {generation}"
                ),
            ))
        }
        Err(err) => {
            metrics::record_replicated(metrics::OUTCOME_ERROR);
            Some(refused(
                correlation_id,
                ErrorCode::StorageFailed,
                0,
                err.to_string(),
            ))
        }
    }
}

/// Refuse `sender` at `generation` when this log accepted that generation
/// from another leader. Read only: a fetch and a shard's other logs take no
/// ballot of their own here.
fn refuse_other_leader(
    log: &felix_broker::StreamLog,
    correlation_id: u64,
    key: &felix_router::ShardKey,
    generation: u64,
    sender: Option<&str>,
) -> Option<InternalMessage> {
    let sender_id = sender?;
    if log.accepted_generation() != generation {
        return None;
    }
    let leader = log.accepted_leader()?;
    (*leader != *sender_id)
        .then(|| promised_elsewhere(correlation_id, key, generation, &leader, sender))
}

/// A second node claiming a generation this replica accepted from another.
/// Answered as fenced: whichever of the two reads it must not count this
/// replica, and the control plane never names two, so one of them is stale.
fn promised_elsewhere(
    correlation_id: u64,
    key: &felix_router::ShardKey,
    generation: u64,
    leader: &str,
    sender: Option<&str>,
) -> InternalMessage {
    tracing::warn!(
        stream = %key.stream,
        shard = key.shard,
        generation,
        promised = leader,
        sender = sender.unwrap_or_default(),
        "refusing a second leader at a generation this replica accepted from another",
    );
    metrics::record_replicated(metrics::OUTCOME_FENCED);
    refused(
        correlation_id,
        ErrorCode::FencedEpoch,
        0,
        format!("this broker accepted generation {generation} from {leader}"),
    )
}

/// Where a batch from a leader at `generation` must start for every record
/// this follower wrote under an older generation to be compared, or `None`
/// when there is nothing uncompared.
///
/// Records below the commit offset were compared when they were committed.
/// Past it, those written since the last recorded generation began came from
/// that generation's leader, or were this broker's own writes as leader; a
/// newer leader shipping only past them would never learn they disagree.
fn unverified_from(
    previous: Option<felix_storage::log::Epoch>,
    generation: u64,
    commit: u64,
    verified: Option<u64>,
    tail: u64,
) -> Option<u64> {
    let previous = previous.filter(|previous| generation > previous.generation)?;
    let from = previous.start_offset.max(commit).max(verified.unwrap_or(0));
    (from < tail).then_some(from)
}

/// Label the records a batch appended, `from..durable`.
///
/// A labelled batch says which generation wrote each of them, and that is
/// what they keep. A batch from a sender that predates labels says only who
/// sent it, so its generation is taken to start where the batch appended: an
/// overclaim when the sender inherited the records, which is why labels exist.
pub(crate) fn label_appended(
    log: &felix_broker::StreamLog,
    from: u64,
    durable: u64,
    sender_generation: u64,
    generations: Option<&[GenerationStart]>,
) -> Result<(), felix_broker::BrokerError> {
    match generations {
        Some(generations) => {
            let epochs: Vec<felix_storage::log::Epoch> = generations
                .iter()
                .filter(|start| start.start_offset <= durable)
                .map(|start| felix_storage::log::Epoch {
                    generation: start.generation,
                    start_offset: start.start_offset,
                })
                .collect();
            log.label_generations(from, &epochs)
        }
        None => log.record_generation(sender_generation, from).map(|_| ()),
    }
}

/// The generations of `history` that wrote a record in `first..end`, as a
/// batch of those records carries them: the one `first` belongs to, and each
/// later one starting by `end`.
pub(crate) fn generations_over(
    history: &[felix_storage::log::Epoch],
    first: u64,
    end: u64,
) -> Vec<GenerationStart> {
    let covering = history
        .iter()
        .rposition(|epoch| epoch.start_offset <= first)
        .unwrap_or(0);
    history
        .iter()
        .skip(covering)
        .filter(|epoch| epoch.start_offset <= end)
        .map(|epoch| GenerationStart {
            generation: epoch.generation,
            start_offset: epoch.start_offset,
        })
        .collect()
}

/// The generation the record just below `log_end` was written at, from the
/// log's generation history; zero for an empty log.
pub(crate) fn last_generation(generations: &[felix_storage::log::Epoch], log_end: u64) -> u64 {
    generations
        .iter()
        .rev()
        .find(|epoch| epoch.start_offset < log_end)
        .map_or(0, |epoch| epoch.generation)
}

/// The commit offset, when cutting this log from `from` would discard a
/// committed record it holds. Storage refuses the cut regardless; asking first
/// is what lets the refusal say why.
async fn cuts_committed(log: &felix_broker::StreamLog, from: u64) -> Option<u64> {
    let commit = log.commit_offset();
    // An unreadable tail counts as holding everything below the commit offset.
    let tail = log.tail_offset().await.unwrap_or(u64::MAX);
    (from < commit.min(tail)).then_some(commit)
}

fn divergence_code(divergence: &Divergence) -> ErrorCode {
    match divergence {
        Divergence::Gap { .. } => ErrorCode::LogGap,
        Divergence::Conflict { .. } => ErrorCode::LogConflict,
        // A batch that did not survive the trip is a transport problem, and
        // resending the same records is the repair.
        Divergence::Corrupt { .. } => ErrorCode::Malformed,
    }
}

fn divergence_outcome(divergence: &Divergence) -> &'static str {
    match divergence {
        Divergence::Gap { .. } => metrics::OUTCOME_GAP,
        Divergence::Conflict { .. } => metrics::OUTCOME_CONFLICT,
        Divergence::Corrupt { .. } => metrics::OUTCOME_CORRUPT,
    }
}

/// The placement a log's ownership is checked against. The cursor and
/// dead-letter logs belong to their stream's shard and the counter log to
/// its cache's, rather than having placements of their own.
fn shard_key(
    shard: &felix_wire::internal::ShardRef,
    log_kind: felix_broker::LogKind,
) -> felix_router::ShardKey {
    felix_router::ShardKey {
        tenant_id: shard.tenant_id.clone(),
        namespace: shard.namespace.clone(),
        stream: shard.stream.clone(),
        shard: shard.shard,
        kind: match log_kind {
            felix_broker::LogKind::Cache | felix_broker::LogKind::Counters => {
                felix_router::ShardKind::Cache
            }
            felix_broker::LogKind::Stream
            | felix_broker::LogKind::GroupCursors
            | felix_broker::LogKind::GroupDeadLetters => felix_router::ShardKind::Stream,
        },
    }
}

fn refused(
    correlation_id: u64,
    code: ErrorCode,
    expected_offset: u64,
    detail: String,
) -> InternalMessage {
    InternalMessage::ReplicateError(ReplicateError {
        correlation_id,
        code,
        expected_offset,
        detail,
    })
}

#[cfg(test)]
mod tests;
