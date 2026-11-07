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
mod election;

use felix_common::random::HashMap;
use std::sync::Arc;
use std::time::Duration;

use felix_broker::Broker;
use felix_broker::replication::{self, Divergence};
use felix_router::{ReplicaRole, ShardRouter};
use felix_storage::disk_log::GenerationCheck;
use felix_storage::run_state::copy_trust::Since;
use felix_wire::internal::{
    ErrorCode, Fence, FenceOk, GenerationStart, InternalMessage, PeerCapabilities, RefusalReason,
    ReplicaLog, ReplicateBootstrap, ReplicateError, ReplicateFetch, ReplicateOk, ReplicateRebuild,
    ReplicateRecords,
};
use tokio::time::Instant;

/// The most one fetch answer carries, whatever the leader asked for.
const MAX_FETCH_BYTES: usize = 4 * 1024 * 1024;

/// The longest a fence waits for its leader's turn before it runs anyway.
/// Under the default 5 s peer request timeout, so a fence that waited is
/// still answered before its leader gives up on it. A request timeout set
/// below this lets a queued fence time out before it runs.
const FENCE_TURN_WAIT: std::time::Duration = std::time::Duration::from_secs(2);

/// How many logs' fenced generations are remembered. Past it, one is
/// forgotten for each new one.
const MAX_FENCED_AT: usize = 65_536;

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
    /// this broker holds as committed, per log, with the stream or cache
    /// incarnation it shipped. A rebuild from that leader would only find the same
    /// disagreement again; a recreated stream at the same generation is
    /// another log.
    mismatched:
        parking_lot::Mutex<HashMap<(felix_router::ShardKey, felix_broker::LogKind), (u64, u64)>>,
    /// Set when this process started after a crash that may have cost it
    /// records it acknowledged. See [`crate::unclean`].
    unclean: Option<Arc<crate::unclean::UncleanStart>>,
    /// Confirmed reads of the control plane's assignments, for a copy with
    /// no promise. Without it the routing view answers alone.
    confirmed: Option<Arc<dyn crate::confirmed::ConfirmedRoutes>>,
    /// When this replica last heard each shard's leader, from a batch or
    /// fence that carried a set. See [`Self::with_stickiness`].
    heard: parking_lot::Mutex<HashMap<felix_router::ShardKey, Instant>>,
    /// Confirmed reads, made off the peer lane and reused while fresh.
    confirmed_reads: Arc<crate::confirmed::ConfirmedReads>,
    /// How long after hearing its leader this replica refuses a candidate.
    stickiness: Duration,
    /// Which cross-broker features the fleet has; `None` reads all as off.
    fleet: Option<Arc<crate::quorum::QuorumMarks>>,
    /// This broker's data directory identity, to tell a set written for this
    /// copy from one written for a copy that was wiped. See `election.rs`.
    local_storage: Option<felix_storage::StorageId>,
    /// Whether the control plane last registered this broker under another
    /// identity. See `election.rs`.
    replaced_storage: bool,
    /// Each leader's turn to fence here. See [`Self::fence`].
    fence_turns: parking_lot::Mutex<HashMap<String, Arc<tokio::sync::Semaphore>>>,
    /// The generation each log last took a fence at, which a read round's
    /// fence repeats. Only paces fences; the ballot decides every one.
    fenced_at: parking_lot::Mutex<HashMap<(felix_router::ShardKey, felix_broker::LogKind), u64>>,
    /// Whether a log in this process is poisoned. A test stands in for the
    /// process-wide flag, which never clears.
    poisoned: fn() -> bool,
    /// Run after an incarnation check passes, to change the catalog between
    /// the check and the open.
    #[cfg(test)]
    incarnation_checked: parking_lot::Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
}

/// Progress comparing a follower's older records with a newer leader's.
#[derive(Debug, Clone, Copy)]
struct Verified {
    generation: u64,
    /// The stream or cache incarnation whose log was compared; 0 before
    /// incarnations.
    incarnation: u64,
    through: u64,
}

/// A failover's set of two beside the copy a crashed leader kept: what
/// [`ReplicaHandler::failover_two`] found.
struct FailoverTwo {
    /// The two the route shows, this broker and the fencing leader.
    routed: std::collections::BTreeSet<String>,
    /// The copies the failover's fence ids name.
    named: Vec<String>,
}

impl FailoverTwo {
    /// Whether the fence ids name exactly the two.
    fn acknowledged(&self) -> bool {
        self.named.iter().eq(self.routed.iter())
    }

    /// The copies the fence ids name beyond the two, when they name both:
    /// with this control plane that is a staged or joining successor the
    /// failover left out (#1635).
    fn beyond(&self) -> Vec<String> {
        if !self.routed.iter().all(|node| self.named.contains(node)) {
            return Vec::new();
        }
        self.named
            .iter()
            .filter(|node| !self.routed.contains(*node))
            .cloned()
            .collect()
    }
}

impl ReplicaHandler {
    pub fn new(broker: Arc<Broker>, router: Arc<ShardRouter>) -> Self {
        Self {
            broker,
            router,
            verified: parking_lot::Mutex::new(HashMap::default()),
            mismatched: parking_lot::Mutex::new(HashMap::default()),
            unclean: None,
            confirmed: None,
            heard: parking_lot::Mutex::new(HashMap::default()),
            confirmed_reads: Default::default(),
            stickiness: crate::suspicion::DEFAULT_SUSPECT_AFTER,
            fleet: None,
            local_storage: None,
            replaced_storage: false,
            fence_turns: parking_lot::Mutex::new(HashMap::default()),
            fenced_at: parking_lot::Mutex::new(HashMap::default()),
            poisoned: felix_storage::disk_log::any_log_poisoned,
            #[cfg(test)]
            incarnation_checked: parking_lot::Mutex::new(None),
        }
    }

    /// Answer no promotion fence from a copy this broker led before an
    /// unclean start. See [`Self::fence`].
    pub fn with_unclean_start(mut self, unclean: Arc<crate::unclean::UncleanStart>) -> Self {
        self.unclean = Some(unclean);
        self
    }

    /// Have a copy with no promise on disk check the control plane's
    /// confirmed assignment before it takes a leader. See
    /// [`crate::confirmed`].
    pub fn with_confirmed_routes(
        mut self,
        confirmed: Arc<dyn crate::confirmed::ConfirmedRoutes>,
    ) -> Self {
        self.confirmed = Some(confirmed);
        self
    }

    /// Keep a refused copy's confirmed answer fresh for a leader that
    /// retries it up to `retry` apart: the driver's tick, which is the
    /// control-plane sync interval. Without it the retry is waited for up to
    /// 5 s. Call before [`Self::confirmed_prefetch`].
    pub fn with_leader_retry(mut self, retry: Duration) -> Self {
        self.confirmed_reads = Arc::new(crate::confirmed::ConfirmedReads::new(retry));
        self
    }

    /// What starts a copy's confirmed read when the routing view first names
    /// it, so the leader's first request finds the answer. `None` without
    /// [`Self::with_confirmed_routes`].
    pub fn confirmed_prefetch(&self) -> Option<crate::confirmed::Prefetch> {
        self.confirmed.as_ref().map(|routes| {
            crate::confirmed::Prefetch::new(
                Arc::clone(&self.confirmed_reads),
                Arc::clone(routes),
                self.fleet.clone(),
            )
        })
    }

    /// Read fleet features from `marks`: under `joint_fence` a voter of the
    /// set a joint fence leaves answers it once no route names it.
    pub fn with_fleet(mut self, marks: Arc<crate::quorum::QuorumMarks>) -> Self {
        self.fleet = Some(marks);
        self
    }

    /// Name this broker's storage identity, which a set naming another one
    /// for this broker contradicts: that copy was wiped (#1331).
    pub fn with_local_storage(mut self, storage: felix_storage::StorageId) -> Self {
        self.local_storage = Some(storage);
        self
    }

    /// Say the control plane last registered this broker under another
    /// storage identity, so every shard log it creates without a kept set is
    /// a learner of a set past generation 1 (`ReplacedIdLearner` in
    /// `docs/formal/FelixShard.tla`).
    pub fn with_replaced_storage(mut self, replaced: bool) -> Self {
        self.replaced_storage = replaced;
        self
    }

    /// Refuse a candidate's fence for this long after hearing the shard's
    /// leader. The suspicion window: a replica that would not yet name its
    /// leader as gone does not help replace it either.
    pub fn with_stickiness(mut self, stickiness: Duration) -> Self {
        self.stickiness = stickiness;
        self
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
        let log_kind = log_kind(request.log);
        let key = shard_key(&request.shard, log_kind);
        if let Some(refusal) = self.check_role(correlation_id, &key, request.shard.generation) {
            return refusal;
        }
        if let Some(refusal) =
            self.check_incarnation(correlation_id, &key, request.shard.incarnation)
        {
            return refusal;
        }
        if let Some(refusal) = self
            .check_shard_fence(
                correlation_id,
                &key,
                log_kind,
                request.shard.generation,
                sender,
                request.shard.incarnation,
            )
            .await
        {
            return refusal;
        }
        let Some(log) = self
            .broker
            .shard_log_off_runtime(
                log_kind,
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
                Some(request.base_offset),
                self.expected_incarnation(&key, request.shard.incarnation),
            )
            .await
        else {
            if let Some(refusal) =
                self.check_incarnation(correlation_id, &key, request.shard.incarnation)
            {
                return refusal;
            }
            metrics::record_replicated(metrics::OUTCOME_REFUSED);
            return refused(
                correlation_id,
                ErrorCode::Unauthorized,
                0,
                "this broker has no log for that shard".to_string(),
            );
        };
        if let Some(refusal) = self.refuse_unconfirmed(
            correlation_id,
            &key,
            &log,
            request.shard.generation,
            sender,
            request.shard.incarnation,
        ) {
            return refusal;
        }
        if let Some(refusal) =
            accept_sender(&log, correlation_id, &key, request.shard.generation, sender).await
        {
            return refusal;
        }
        // Records below the leader's base are gone from the leader too, so only
        // what this broker holds from there up is at stake.
        let from = log.physical_base().max(request.base_offset);
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
            learner: false,
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
        let incarnation = request.shard.incarnation.unwrap_or(0);
        if self.committed_mismatch(key, log_kind) == Some((generation, incarnation)) {
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
            learner: false,
        })
    }

    /// The generation and incarnation whose leader last disagreed with a
    /// committed record of this log, if any.
    fn committed_mismatch(
        &self,
        key: &felix_router::ShardKey,
        log_kind: felix_broker::LogKind,
    ) -> Option<(u64, u64)> {
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
        if let Some(refusal) =
            self.check_incarnation(correlation_id, &key, request.shard.incarnation)
        {
            return refusal;
        }
        if let Some(refusal) = self
            .check_shard_fence(
                correlation_id,
                &key,
                log_kind,
                request.shard.generation,
                sender,
                request.shard.incarnation,
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
            .shard_log_off_runtime(
                log_kind,
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
                Some(request.base_offset),
                self.expected_incarnation(&key, request.shard.incarnation),
            )
            .await
        else {
            if let Some(refusal) =
                self.check_incarnation(correlation_id, &key, request.shard.incarnation)
            {
                return refusal;
            }
            metrics::record_replicated(metrics::OUTCOME_REFUSED);
            return refused(
                correlation_id,
                ErrorCode::Unauthorized,
                0,
                "this broker has no log for that shard".to_string(),
            );
        };

        if let Some(refusal) = self.refuse_unconfirmed(
            correlation_id,
            &key,
            &log,
            request.shard.generation,
            sender,
            request.shard.incarnation,
        ) {
            return refusal;
        }
        if let Some(refusal) =
            accept_sender(&log, correlation_id, &key, request.shard.generation, sender).await
        {
            return refusal;
        }

        let mut base = log.physical_base();
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
            learner: false,
        })
    }

    pub async fn apply(
        &self,
        sender: Option<&str>,
        batch: ReplicateRecords,
        log_kind: felix_broker::LogKind,
    ) -> InternalMessage {
        let correlation_id = batch.correlation_id;
        // Only a stream's records carry header blocks; storing another log's
        // batch without them would leave it differing from the leader's.
        if log_kind != felix_broker::LogKind::Stream
            && batch
                .headers
                .as_deref()
                .is_some_and(|headers| headers.iter().any(Option::is_some))
        {
            metrics::record_replicated(metrics::OUTCOME_REFUSED);
            return refused(
                correlation_id,
                ErrorCode::Malformed,
                0,
                format!("a {log_kind:?} log's records carry no headers"),
            );
        }
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
        if let Some(refusal) = self.check_incarnation(correlation_id, &key, batch.shard.incarnation)
        {
            return refusal;
        }
        if let Some(refusal) = self
            .check_shard_fence(
                correlation_id,
                &key,
                log_kind,
                batch.shard.generation,
                sender,
                batch.shard.incarnation,
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
            .shard_log_off_runtime(
                log_kind,
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
                None,
                self.expected_incarnation(&key, batch.shard.incarnation),
            )
            .await
        else {
            if let Some(refusal) =
                self.check_incarnation(correlation_id, &key, batch.shard.incarnation)
            {
                return refusal;
            }
            metrics::record_replicated(metrics::OUTCOME_REFUSED);
            return refused(
                correlation_id,
                ErrorCode::Unauthorized,
                0,
                "this broker has no log for that shard".to_string(),
            );
        };

        if let Some(refusal) = self.refuse_unconfirmed(
            correlation_id,
            &key,
            &log,
            batch.shard.generation,
            sender,
            batch.shard.incarnation,
        ) {
            return refusal;
        }
        // Before the ballot changes: it is the evidence of a copy this
        // broker led before an unclean start.
        if own_kind(log_kind).is_none()
            && let Err(err) = self
                .doubt_before_ballot(&key, &log, batch.shard.generation)
                .await
        {
            tracing::error!(
                stream = %key.stream,
                shard = key.shard,
                error = %err,
                "could not record what this copy may have lost before taking a newer ballot \
                 (that this broker led it before a crash, or since when it is unkept); \
                 refusing the batch rather than take its ballot",
            );
            metrics::record_replicated(metrics::OUTCOME_REFUSED);
            return refused(
                correlation_id,
                ErrorCode::Unavailable,
                0,
                "this broker could not record what its copy may have lost before taking the ballot"
                    .to_string(),
            );
        }
        if let Some(refusal) =
            accept_sender(&log, correlation_id, &key, batch.shard.generation, sender).await
        {
            return refusal;
        }
        if let Some(stamped) = batch.set_generation
            && let Some(refusal) = self
                .check_batch_set(
                    correlation_id,
                    &key,
                    &log,
                    stamped,
                    batch.shard.generation,
                    sender,
                )
                .await
        {
            return refusal;
        }

        // Held like the leader's copy: this one counts toward the mark, and if
        // it led, it would be the copy a client was promised. Before the first
        // apply, since a trim batch applies its floor as soon as it is synced
        // and a floor applied without the hold is never taken back by it.
        if matches!(
            log_kind,
            felix_broker::LogKind::Stream | felix_broker::LogKind::Cache
        ) {
            let hold = crate::quorum::acknowledges_at_quorum(&self.broker, &key).await;
            if let Err(err) = log.hold_retention_at_commit(hold).await {
                tracing::warn!(
                    stream = %key.stream,
                    error = %err,
                    "could not write the retention hold through; it applies in memory",
                );
            }
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
            .filter(|verified| {
                verified.generation == batch.shard.generation
                    && verified.incarnation == batch.shard.incarnation.unwrap_or(0)
            })
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
        // Before any trim record is stored here, as on the leader.
        if let Err(err) = self.broker.mark_head_trimmed_for(&batch.marks).await {
            metrics::record_replicated(metrics::OUTCOME_ERROR);
            return refused(correlation_id, ErrorCode::StorageFailed, 0, err.to_string());
        }
        if let Err(err) = self
            .broker
            .mark_headers_stored_for(batch.headers.as_deref())
            .await
        {
            metrics::record_replicated(metrics::OUTCOME_ERROR);
            return refused(correlation_id, ErrorCode::StorageFailed, 0, err.to_string());
        }
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
                    batch.times.as_deref().unwrap_or_default(),
                    batch.headers.as_deref(),
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
                self.mismatched.lock().insert(
                    (key.clone(), log_kind),
                    (batch.shard.generation, batch.shard.incarnation.unwrap_or(0)),
                );
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
                            batch.times.as_deref().unwrap_or_default(),
                            batch.headers.as_deref(),
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
                if batch.set_generation.is_some() {
                    self.heard_leader(&key);
                }
                // Not a reason to fail the batch: the offset only guards
                // truncation, and the log writes it behind. Only what
                // this batch left level with the leader: past the
                // durable offset nothing here has been compared.
                if let Some(commit) = batch.commit_offset
                    && let Err(err) = log
                        .advance_commit_offset(commit.min(applied.durable_offset))
                        .await
                {
                    tracing::warn!(
                        stream = %key.stream,
                        error = %err,
                        "could not record the commit offset; it holds in memory and is retried",
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
                    .await
                {
                    tracing::warn!(
                        stream = %key.stream,
                        error = %err,
                        "stored a replicated batch but could not record its generation",
                    );
                }
                // Only once the copy is level with this leader up to where
                // its generation starts (#2029). It opened after fencing a
                // majority, so below that offset it holds every acknowledged
                // record, start record written or not. Only the batch's own
                // labels say where that is: an unlabelled batch labels
                // whatever it carried, inherited records too, with the
                // sender's generation.
                let reaches_own = batch.generations.as_deref().is_some_and(|starts| {
                    starts.iter().any(|start| {
                        start.generation == batch.shard.generation
                            && start.start_offset <= applied.durable_offset
                    })
                });
                if own_kind(log_kind).is_none() && level && reaches_own {
                    let shard = crate::driver::watch_key(&key);
                    if let Some(unclean) = &self.unclean {
                        unclean.vouch(&shard);
                    }
                    // The commit offset is the quorum offset at the sender's
                    // generation, so level through it and through the start
                    // the copy holds what the sender acknowledged. Zero is a
                    // mark carried only to pick the frame layout.
                    let trusts = batch
                        .commit_offset
                        .is_some_and(|mark| mark > 0 && mark <= applied.durable_offset);
                    if trusts && let Some(unclean) = &self.unclean {
                        unclean.trust(&shard);
                    }
                    self.record_vouch(&shard, trusts).await;
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
                            incarnation: batch.shard.incarnation.unwrap_or(0),
                            through: applied.durable_offset,
                        },
                    );
                }
                // Before the tail advances: a trim scanning in between must
                // not see the new tail without the commit being noted.
                if log_kind == felix_broker::LogKind::Stream
                    && batch
                        .marks
                        .iter()
                        .any(|mark| matches!(mark, felix_wire::internal::ProducerMark::Commit))
                    && let Err(err) = self
                        .broker
                        .note_replicated_commit(
                            &key.tenant_id,
                            &key.namespace,
                            &key.stream,
                            key.shard,
                        )
                        .await
                {
                    tracing::warn!(
                        stream = %key.stream,
                        error = %err,
                        "stored a replicated commit but could not note it for head trims",
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
                let learner = self.admit_when_caught_up(&key, log_kind, &log).await;
                if !learner && own_kind(log_kind).is_none() {
                    self.record_readmitted(&key, &log, batch.shard.generation)
                        .await;
                }
                metrics::record_replicated(metrics::OUTCOME_OK);
                InternalMessage::ReplicateOk(ReplicateOk {
                    correlation_id,
                    durable_offset: applied.durable_offset,
                    learner,
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
    /// promoted leader fences those logs as well, after the shard's own, for
    /// where each ends: the answer is that log's.
    ///
    /// One leader's promotion fences run here one at a time. Each may open a
    /// log and write a ballot, and a promotion burst running them side by side
    /// starves the writes this broker already serves. A read round's fence
    /// writes nothing and skips the queue, so it does not wait behind a
    /// promotion's (#1796). That is known from what this broker last
    /// answered, so after a restart a log's first read round queues once.
    ///
    /// Answered as to a caller that offered no capabilities; see
    /// [`Self::fence_from`].
    pub async fn fence(&self, sender: Option<&str>, request: Fence) -> InternalMessage {
        self.fence_from(sender, PeerCapabilities::NONE, request)
            .await
    }

    /// [`Self::fence`] from a caller that offered `caller` in its `Hello`.
    ///
    /// To a caller that offered [`PeerCapabilities::UNIFIED_FENCE`], the
    /// shard's own log answers where it would refuse a copy that is a
    /// learner, or the one this broker led before a crash: such a leader
    /// counts the answer by its attestation, which says the copy's promises
    /// or its log count for nothing. Every ballot and set refusal stands.
    /// Group and counter logs carry no attestation and refuse as before.
    pub async fn fence_from(
        &self,
        sender: Option<&str>,
        caller: PeerCapabilities,
        request: Fence,
    ) -> InternalMessage {
        // A candidate's fence on the shard's own log, from a sender that names
        // itself: once taken, this replica routes the shard to it, so its
        // batches pass the role check and clients are sent to it (rule 9).
        let candidate = request
            .election
            .as_ref()
            .filter(|election| election.candidate)
            .filter(|_| matches!(request.log, ReplicaLog::Stream | ReplicaLog::Cache))
            .zip(sender)
            .zip(self.fleet.as_ref().and_then(|marks| marks.elections()))
            .map(|((election, leader), elections)| {
                let key = shard_key(&request.shard, log_kind(request.log));
                let elected = crate::election::Elected {
                    leader: leader.to_string(),
                    generation: request.shard.generation,
                    set: crate::replica_set::membership(&election.set),
                    leaving: election
                        .leaving
                        .as_ref()
                        .map(crate::replica_set::membership),
                    incarnation: request.shard.incarnation.unwrap_or(0),
                };
                (Arc::clone(elections), key, elected)
            });
        let log_kind = log_kind(request.log);
        let key = (shard_key(&request.shard, log_kind), log_kind);
        let generation = request.shard.generation;
        let confirming = self.fenced_at.lock().get(&key) == Some(&generation);
        let _turn = if confirming {
            None
        } else {
            self.fence_turn(sender).await
        };
        // A broker with nothing durable to attest from answers as an older one,
        // under the refusals that caller's counting relies on.
        let unified = caller.contains(PeerCapabilities::UNIFIED_FENCE)
            && self.fleet.as_ref().is_none_or(|fleet| fleet.attests());
        let answer = self.fence_in_turn(sender, unified, request).await;
        if matches!(answer, InternalMessage::FenceOk(_)) {
            let mut fenced_at = self.fenced_at.lock();
            // Forgetting only sends a read round's fence through the queue.
            if fenced_at.len() >= MAX_FENCED_AT && !fenced_at.contains_key(&key) {
                let any = fenced_at.keys().next().cloned();
                if let Some(any) = any {
                    fenced_at.remove(&any);
                }
            }
            let at = fenced_at.entry(key).or_insert(generation);
            *at = (*at).max(generation);
        }
        // A unified answer that keeps no promise did nothing to elect the
        // candidate, so it routes nothing to it: the same as the refusal an
        // older caller gets from this copy.
        if let (Some((elections, key, elected)), InternalMessage::FenceOk(ok)) =
            (candidate, &answer)
            && !(unified
                && ok
                    .attestation
                    .as_ref()
                    .is_some_and(|a| a.kept_from == u64::MAX))
        {
            elections.record(key, elected);
        }
        answer
    }

    /// Wait for `sender`'s turn to fence, or [`FENCE_TURN_WAIT`] at most: a
    /// fence stuck on its storage must not hold up that leader's others for
    /// longer than they would wait anyway.
    async fn fence_turn(&self, sender: Option<&str>) -> Option<tokio::sync::OwnedSemaphorePermit> {
        let turns = Arc::clone(
            self.fence_turns
                .lock()
                .entry(sender.unwrap_or_default().to_string())
                .or_insert_with(|| Arc::new(tokio::sync::Semaphore::new(1))),
        );
        tokio::time::timeout(FENCE_TURN_WAIT, turns.acquire_owned())
            .await
            .ok()?
            .ok()
    }

    async fn fence_in_turn(
        &self,
        sender: Option<&str>,
        unified: bool,
        request: Fence,
    ) -> InternalMessage {
        let correlation_id = request.correlation_id;
        let generation = request.shard.generation;
        let log_kind = log_kind(request.log);
        let key = shard_key(&request.shard, log_kind);
        // Only the shard's own log carries the attestation that makes an
        // answer in place of a refusal safe to count.
        let attested = unified && own_kind(log_kind).is_none();
        // The fence comes with a promotion, usually before this broker's
        // routing view has the new generation. Behind is the normal case here,
        // not a reason to make the new leader wait for the watch.
        match self.router.replica_role(&key, generation) {
            ReplicaRole::Follower | ReplicaRole::Behind { .. } => {}
            // A joint fence also asks the voters of the set being left, which
            // the new route no longer names. The set check below decides.
            ReplicaRole::NotAReplica if self.joint_fence() && self.votes_in_leaving(&request) => {}
            _ => {
                if let Some(refusal) = self.check_role(correlation_id, &key, generation) {
                    return refusal;
                }
            }
        }
        if let Some(refusal) =
            self.check_incarnation(correlation_id, &key, request.shard.incarnation)
        {
            return refusal;
        }
        let Some(log) = self
            .broker
            .shard_log_off_runtime(
                log_kind,
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
                None,
                self.expected_incarnation(&key, request.shard.incarnation),
            )
            .await
        else {
            if let Some(refusal) =
                self.check_incarnation(correlation_id, &key, request.shard.incarnation)
            {
                return refusal;
            }
            // A broker without consumer groups holds no group state and can
            // store none, so it answers as an empty log rather than leaving
            // the leader waiting on a promise it has no log to keep.
            if matches!(
                log_kind,
                felix_broker::LogKind::GroupCursors | felix_broker::LogKind::GroupDeadLetters
            ) && self.broker.group_reader().is_none()
            {
                if let Some(refusal) = self
                    .check_shard_fence(
                        correlation_id,
                        &key,
                        log_kind,
                        generation,
                        sender,
                        request.shard.incarnation,
                    )
                    .await
                {
                    return refusal;
                }
                return InternalMessage::FenceOk(FenceOk {
                    correlation_id,
                    log_end: 0,
                    commit_offset: 0,
                    last_generation: 0,
                    kept_ballot: false,
                    attestation: None,
                });
            }
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
            .check_shard_fence(
                correlation_id,
                &key,
                log_kind,
                generation,
                sender,
                request.shard.incarnation,
            )
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
                .shard_log_off_runtime(
                    felix_broker::LogKind::Counters,
                    &key.tenant_id,
                    &key.namespace,
                    &key.stream,
                    key.shard,
                    None,
                    None,
                )
                .await
                .map_or(0, |log| log.accepted_generation());
            if counters > generation {
                metrics::record_replicated(metrics::OUTCOME_FENCED);
                return refused_for(
                    correlation_id,
                    ErrorCode::FencedEpoch,
                    RefusalReason::Accepted {
                        accepted: counters,
                        sender: generation,
                    },
                );
            }
        }
        let voter = if attested {
            match self.fences_as_non_voter(correlation_id, &key, &log, &request) {
                // Even a learner continuing from its leader keeps no promise
                // here: unlike an older caller's, a unified fence gives a
                // learner a ballot, so a ballot it holds may be one taken
                // while its promises counted for nothing, and a later crash
                // that keeps it does not make it kept (design-unified-fence.md
                // section 1, "kept": never lost, or readmitted).
                Ok(non_voter) => !non_voter,
                Err(refusal) => return *refusal,
            }
        } else {
            if let Some(refusal) = self
                .refuse_non_voter_fence(correlation_id, &key, log_kind, &log, &request, sender)
                .await
            {
                return refusal;
            }
            true
        };
        // A copy led before the crash answers, untrusted, only while this
        // process tracks that: the ballot it takes would erase the given-up
        // mark, the one record of it a clean restart has.
        let tracked = attested.then_some(self.unclean.as_ref()).flatten();
        // Read before the ballot replaces it.
        let given_up = log.gave_up();
        if tracked.is_none()
            && let Some(refusal) = self
                .refuse_short_copy(
                    correlation_id,
                    &key,
                    log_kind,
                    &log,
                    generation,
                    sender,
                    request.shard.incarnation,
                )
                .await
        {
            return refusal;
        }
        if let Some(refusal) = self
            .refuse_unbacked(
                correlation_id,
                &key,
                log_kind,
                &log,
                generation,
                sender,
                request.shard.incarnation,
            )
            .await
        {
            return refusal;
        }
        // A fence at the generation already accepted changes nothing here: it
        // is the leader confirming it still leads, once per read round. From
        // any other node it is refused below, by the ballot.
        if let Some(election) = &request.election
            && let Some(refusal) = self.check_fence_set(
                correlation_id,
                &key,
                &log,
                election,
                (generation, sender),
                attested,
            )
        {
            return refusal;
        }
        let confirming = log.accepted_generation() == generation;
        // Read before the ballot moves to this generation: a continuing
        // leader counts a rotated copy only on this (#2047).
        let kept_ballot = self.continues_from(&key, &log, generation, sender);
        // So a later fence from an older leader still finds it led before,
        // in this process and, through the copy-trust record, after a clean
        // restart: the ballot replaces the one, or the given-up mark, that
        // said so.
        if attested && let Err(err) = self.doubt_before_ballot(&key, &log, generation).await {
            metrics::record_replicated(metrics::OUTCOME_ERROR);
            return refused(correlation_id, ErrorCode::StorageFailed, 0, err.to_string());
        }
        // A ballot taken as no voter would read, after a crash that keeps
        // it, as a promise kept: record first that it is not one.
        if !voter && let Err(err) = self.unkeep(&key, generation).await {
            metrics::record_replicated(metrics::OUTCOME_ERROR);
            return refused(correlation_id, ErrorCode::StorageFailed, 0, err.to_string());
        }
        if let Some(refusal) = accept_sender(&log, correlation_id, &key, generation, sender).await {
            return refusal;
        }
        // After the ballot, as the open expects, and before the answer.
        if let Some(election) = &request.election
            && let Some(refusal) = self
                .keep_fence_set(correlation_id, &key, &log, election, (generation, sender))
                .await
        {
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
        if request.election.as_ref().is_some_and(|e| !e.candidate) {
            self.heard_leader(&key);
        }
        InternalMessage::FenceOk(FenceOk {
            correlation_id,
            log_end,
            commit_offset: log.commit_offset(),
            last_generation: last_generation(&log.generations(), log_end),
            kept_ballot,
            attestation: attested.then(|| self.attestation(&key, voter, given_up)),
        })
    }

    /// What this replica attests about its copy of `key`'s log in a fence
    /// answer. It travels only to a caller that offered
    /// [`PeerCapabilities::UNIFIED_FENCE`].
    ///
    /// Its promises count from set generation 0 when the directory kept every
    /// ballot and the copy votes, and for no set otherwise; a readmission's
    /// lower bound is not tracked yet. The copy is trusted when its promises
    /// count, it was not `given_up` and is not recorded as the copy this
    /// broker led before a crash (marked on disk), no log in this process is
    /// poisoned (a failed flush may have dropped acknowledged pages), and the
    /// process started clean or a leader has since shipped it level through
    /// that leader's mark ([`crate::unclean::UncleanStart::trust`]). The
    /// directory's copy-trust record keeps that last part across restarts:
    /// after any start that rotated the storage id, a later clean start still
    /// waits for the vouch.
    ///
    /// [`PeerCapabilities::UNIFIED_FENCE`]: felix_wire::internal::PeerCapabilities::UNIFIED_FENCE
    fn attestation(
        &self,
        key: &felix_router::ShardKey,
        voter: bool,
        given_up: bool,
    ) -> felix_wire::internal::FenceAttestation {
        attest_copy(
            self.fleet.as_deref(),
            self.unclean.as_deref(),
            (self.poisoned)(),
            key,
            voter,
            given_up,
        )
    }

    fn copy_trust(&self) -> Option<&Arc<felix_storage::run_state::copy_trust::CopyTrust>> {
        self.fleet.as_ref().and_then(|fleet| fleet.copy_trust())
    }

    /// `log`, `key`'s own log, is about to take a newer leader's ballot at
    /// `generation` from a batch. A copy this broker led before an unclean
    /// start is recorded as doubted first ([`UncleanStart::accepting`]), and
    /// so is one given up on disk after a clean restart, whose given-up
    /// ballot the new one replaces. On an error the ballot must not be taken.
    ///
    /// [`UncleanStart::accepting`]: crate::unclean::UncleanStart::accepting
    async fn doubt_before_ballot(
        &self,
        key: &felix_router::ShardKey,
        log: &felix_broker::StreamLog,
        generation: u64,
    ) -> Result<(), felix_storage::StorageError> {
        let shard = crate::driver::watch_key(key);
        self.pin_unkept_floor(&shard, log).await?;
        if let Some(unclean) = &self.unclean {
            return unclean.accepting(&shard, log, generation).await;
        }
        let Some(trust) = self.copy_trust() else {
            return Ok(());
        };
        let name = crate::unclean::copy_key(&shard);
        if !log.gave_up() || generation < log.accepted_generation() || trust.doubted(&name) {
            return Ok(());
        }
        let trust = Arc::clone(trust);
        tokio::task::spawn_blocking(move || trust.doubt(&name))
            .await
            .unwrap_or_else(|err| Err(felix_storage::StorageError::Io(std::io::Error::other(err))))
    }

    /// A copy unkept since a generation the record does not know (it did not
    /// decode, or named another storage id) is unkept since at least the
    /// ballot it holds now. Recorded before a newer ballot replaces that, so
    /// only a set newer than it readmits the copy.
    async fn pin_unkept_floor(
        &self,
        shard: &crate::ShardKey,
        log: &felix_broker::StreamLog,
    ) -> Result<(), felix_storage::StorageError> {
        let Some(trust) = self.copy_trust() else {
            return Ok(());
        };
        let name = crate::unclean::copy_key(shard);
        if trust.unkept(&name) != Some(Since::Unknown) {
            return Ok(());
        }
        let trust = Arc::clone(trust);
        let floor = log.accepted_generation();
        tokio::task::spawn_blocking(move || trust.unkeep(&name, floor))
            .await
            .unwrap_or_else(|err| Err(felix_storage::StorageError::Io(std::io::Error::other(err))))
    }

    /// Whether `key`'s copy took a ballot while its promises counted for
    /// nothing and has not been readmitted since ([`Self::unkeep`]).
    fn unkept(&self, key: &felix_router::ShardKey) -> bool {
        self.copy_trust().is_some_and(|trust| {
            trust
                .unkept(&crate::unclean::copy_key(&crate::driver::watch_key(key)))
                .is_some()
        })
    }

    /// Record, durably, that `key`'s copy is about to take a ballot while its
    /// promises count for nothing, as a learner answering a unified fence
    /// does. Call before the ballot is written. Afterwards the ballot reads
    /// as a promise kept through any crash, so without this record an older
    /// caller's fence would count it ([`Self::continues_from`]). An error
    /// means the ballot must not be taken. `generation` is the ballot's: only
    /// a set newer than it readmits the copy ([`Self::readmitted_since`]).
    pub(crate) async fn unkeep(
        &self,
        key: &felix_router::ShardKey,
        generation: u64,
    ) -> Result<(), felix_storage::StorageError> {
        let Some(trust) = self.copy_trust() else {
            return Ok(());
        };
        let name = crate::unclean::copy_key(&crate::driver::watch_key(key));
        if matches!(trust.unkept(&name), Some(Since::At(since)) if since >= generation) {
            return Ok(());
        }
        let trust = Arc::clone(trust);
        tokio::task::spawn_blocking(move || trust.unkeep(&name, generation))
            .await
            .unwrap_or_else(|err| Err(felix_storage::StorageError::Io(std::io::Error::other(err))))
    }

    /// After a batch from the leader of `sent_at`: an unkept copy that is now
    /// readmitted, a voter in a set newer than its unkept ballot, counts its
    /// promises again. A batch alone, or wipe evidence without a set, is no
    /// readmission (the design's "Wipe" row).
    async fn record_readmitted(
        &self,
        key: &felix_router::ShardKey,
        log: &felix_broker::StreamLog,
        sent_at: u64,
    ) {
        let Some(trust) = self.copy_trust() else {
            return;
        };
        let shard = crate::driver::watch_key(key);
        let name = crate::unclean::copy_key(&shard);
        // An unknown floor is pinned before the batch's ballot; if that did
        // not happen, nothing says which set is newer.
        let Some(Since::At(since)) = trust.unkept(&name) else {
            return;
        };
        if !self.readmitted_since(log, since, sent_at).await {
            return;
        }
        let trust = Arc::clone(trust);
        if let Ok(Err(err)) = tokio::task::spawn_blocking(move || trust.clear_unkept(&name)).await {
            tracing::warn!(
                stream = %key.stream,
                shard = key.shard,
                error = %err,
                "could not record that this copy was readmitted; its promises count for nothing \
                 until the next batch records it",
            );
        }
    }

    /// Record in the directory's copy-trust record that a leader shipped
    /// `shard`'s copy level through its generation's start, which ends any
    /// doubt, and, when `trusts`, through its mark too. Off the runtime: each
    /// change is synced, once per copy after a crash.
    async fn record_vouch(&self, shard: &crate::ShardKey, trusts: bool) {
        let Some(trust) = self.copy_trust() else {
            return;
        };
        let name = crate::unclean::copy_key(shard);
        if !trust.doubted(&name) && (!trusts || trust.trusted(&name)) {
            return;
        }
        let trust = Arc::clone(trust);
        let recorded = tokio::task::spawn_blocking(move || trust.vouch(&name, trusts)).await;
        if let Ok(Err(err)) = recorded {
            tracing::warn!(
                stream = %shard.stream,
                shard = shard.shard,
                error = %err,
                "could not record that a leader vouched for this copy; it stays untrusted \
                 and is vouched for again by the next batch",
            );
        }
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
        let log_kind = log_kind(request.log);
        let key = shard_key(&request.shard, log_kind);
        match self.router.replica_role(&key, generation) {
            ReplicaRole::Follower | ReplicaRole::Behind { .. } => {}
            _ => {
                if let Some(refusal) = self.check_role(correlation_id, &key, generation) {
                    return refusal;
                }
            }
        }
        if let Some(refusal) =
            self.check_incarnation(correlation_id, &key, request.shard.incarnation)
        {
            return refusal;
        }
        let Some(log) = self
            .broker
            .shard_log_off_runtime(
                log_kind,
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
                None,
                self.expected_incarnation(&key, request.shard.incarnation),
            )
            .await
        else {
            if let Some(refusal) =
                self.check_incarnation(correlation_id, &key, request.shard.incarnation)
            {
                return refusal;
            }
            return refused(
                correlation_id,
                ErrorCode::Unauthorized,
                0,
                "this broker has no log for that shard".to_string(),
            );
        };
        if let Some(refusal) = self
            .check_shard_fence(
                correlation_id,
                &key,
                log_kind,
                generation,
                sender,
                request.shard.incarnation,
            )
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
            return refused(correlation_id, code, 0, reader_detail(accepted, generation));
        }
        let max_bytes = (request.max_bytes as usize).clamp(1, MAX_FETCH_BYTES);
        // A promoted leader reads a cursor log whole to take its groups, and
        // compaction may have trimmed its head: what is left is all it needs.
        let from_offset = match log_kind {
            felix_broker::LogKind::GroupCursors => request.from_offset.max(log.physical_base()),
            _ => request.from_offset,
        };
        let records = match log.read_replicated_from(from_offset, max_bytes).await {
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
        let first_offset = records.first().map_or(from_offset, |record| record.offset);
        let publishers = replication::publishers_to_wire(&records);
        let headers = replication::headers_to_wire(&records);
        // A reader that did not ask for the blocks would store these records
        // without them, so it is refused instead.
        if headers.is_some() && !(request.labelled && request.timed && request.headers) {
            return refused(
                correlation_id,
                ErrorCode::UnsupportedKind,
                0,
                "these records carry headers; the fetch must ask for them".to_string(),
            );
        }
        let times = (request.labelled && request.timed).then(|| {
            records
                .iter()
                .map(|record| record.timestamp_micros)
                .collect()
        });
        let payloads: Vec<bytes::Bytes> =
            records.into_iter().map(|record| record.payload).collect();
        let end = first_offset + payloads.len() as u64;
        let mut batch = ReplicateRecords {
            correlation_id,
            shard: request.shard,
            first_offset,
            checksum: felix_wire::internal::batch_checksum(
                &payloads,
                &marks,
                &publishers,
                headers.as_deref().unwrap_or_default(),
            ),
            payloads,
            marks,
            commit_offset: None,
            generations: request
                .labelled
                .then(|| generations_over(&log.generations(), first_offset, end)),
            publishers,
            times,
            headers,
            set_generation: None,
        };
        crate::ship::carry_marks(&mut batch, log_kind, false);
        match log_kind {
            felix_broker::LogKind::Cache => InternalMessage::ReplicateCacheRecords(batch),
            felix_broker::LogKind::Counters => InternalMessage::ReplicateCounterRecords(batch),
            felix_broker::LogKind::GroupCursors => InternalMessage::ReplicateGroupRecords(batch),
            felix_broker::LogKind::GroupDeadLetters => {
                InternalMessage::ReplicateDeadLetterRecords(batch)
            }
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
        sent: Option<u64>,
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
            .shard_log_off_runtime(
                own,
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
                None,
                self.expected_incarnation(key, sent),
            )
            .await?;
        if let Some(refusal) = refuse_other_leader(&log, correlation_id, key, generation, sender) {
            return Some(refusal);
        }
        let accepted = log.accepted_generation();
        (accepted > generation).then(|| {
            metrics::record_replicated(metrics::OUTCOME_FENCED);
            refused_for(
                correlation_id,
                ErrorCode::FencedEpoch,
                RefusalReason::Accepted {
                    accepted,
                    sender: generation,
                },
            )
        })
    }

    /// Refuse a fence when this broker's copy of the shard is the one it led
    /// before a crash that may have cost it records it acknowledged, and no
    /// newer leader has shipped to it since. Counted, it could stand in a
    /// majority for records only its lost tail held. Retryable: the
    /// promotion waits for replicas that can vouch for what they hold.
    ///
    /// Judged on the shard's own log, which a group or counter log follows.
    /// On an unclean start a copy whose ballot names this broker is given up
    /// on disk here, so a later restart still refuses. Not when a set of two
    /// acknowledged what it may have lost (`acknowledged_by_two`).
    #[allow(clippy::too_many_arguments)]
    async fn refuse_short_copy(
        &self,
        correlation_id: u64,
        key: &felix_router::ShardKey,
        log_kind: felix_broker::LogKind,
        log: &felix_broker::StreamLog,
        generation: u64,
        sender: Option<&str>,
        sent: Option<u64>,
    ) -> Option<InternalMessage> {
        let opened;
        let log = match own_kind(log_kind) {
            None => log,
            Some(own) => {
                opened = self
                    .broker
                    .shard_log_off_runtime(
                        own,
                        &key.tenant_id,
                        &key.namespace,
                        &key.stream,
                        key.shard,
                        None,
                        self.expected_incarnation(key, sent),
                    )
                    .await?;
                &opened
            }
        };
        // The leader this copy promised the generation before, moving on to
        // the next: it holds what this copy might have lost (#2047).
        if self.continues_from(key, log, generation, sender) {
            return None;
        }
        let shard = crate::driver::watch_key(key);
        let led_before = self
            .unclean
            .as_ref()
            .is_some_and(|unclean| unclean.led_before(&shard, log, generation));
        // The ballot stops saying so once the copy takes a newer leader's,
        // and a clean restart forgets the rest; this record does neither.
        let doubted = self
            .copy_trust()
            .is_some_and(|trust| trust.doubted(&crate::unclean::copy_key(&shard)));
        let led_before = led_before || doubted;
        if !log.gave_up() && !led_before {
            return None;
        }
        // Refusing the other copy of a set of two would leave no majority at
        // all, and it holds every record the two acknowledged.
        if self.acknowledged_by_two(key, log, generation, sender).await {
            return None;
        }
        let gives_up = led_before
            && self
                .unclean
                .as_ref()
                .is_some_and(|unclean| log.accepted_leader().as_deref() == Some(unclean.node_id()));
        if gives_up {
            // A batch after a clean restart replaces the given-up ballot
            // without asking who led the copy, so record that apart first.
            // This fence is refused either way, and the given-up ballot only
            // adds to the record; a batch that would replace it tries again
            // and is refused if it cannot (`doubt_before_ballot`).
            if !doubted && let Some(trust) = self.copy_trust() {
                let trust = Arc::clone(trust);
                let name = crate::unclean::copy_key(&shard);
                if let Ok(Err(err)) = tokio::task::spawn_blocking(move || trust.doubt(&name)).await
                {
                    tracing::error!(
                        stream = %key.stream,
                        shard = key.shard,
                        error = %err,
                        "could not record that this broker led the copy before its crash",
                    );
                }
            }
            if let Err(err) = log.give_up_generation().await {
                tracing::warn!(
                    stream = %key.stream,
                    shard = key.shard,
                    error = %err,
                    "could not record giving up the generation this broker led before its crash",
                );
            }
        }
        metrics::record_replicated(metrics::OUTCOME_REFUSED);
        let beyond = self
            .failover_two(key, log, generation, sender)
            .await
            .map(|two| two.beyond())
            .unwrap_or_default();
        if !beyond.is_empty() {
            // Every check of the exception passed but the ids, which also
            // name a copy outside the routed pair. Placement leaves an
            // old-set copy out of the report only for a staged or joining
            // successor (#1635).
            let extra = beyond.join(", ");
            tracing::warn!(
                stream = %key.stream,
                shard = key.shard,
                generation,
                extra = %extra,
                "refusing a fence: the failover's fence ids name {extra} beside the set of \
                 two; with this control plane that is a staged or joining successor the \
                 failover left out; no retry ends it, see the replication design's \
                 \"A leader's own crash\" (#1635)",
            );
            metrics::record_fence_refused_ids_beyond_two();
            return Some(refused(
                correlation_id,
                ErrorCode::Unavailable,
                0,
                format!(
                    "this broker led the shard before a crash, and the failover's fence ids \
                     also name {extra} beside the set of two, which with this control plane is \
                     a staged or joining successor the failover left out; see the replication \
                     design's \"A leader's own crash\" (#1635)"
                ),
            ));
        }
        tracing::info!(
            stream = %key.stream,
            shard = key.shard,
            generation,
            "refusing a fence: this copy is the one this broker led before a crash that \
             may have cost it acknowledged records",
        );
        Some(refused(
            correlation_id,
            ErrorCode::Unavailable,
            0,
            "this broker led the shard before a crash that may have cost it acknowledged \
             records, and no newer leader has shipped to it since"
                .to_string(),
        ))
    }

    /// Whether every record `log` may have lost in a crash was acknowledged
    /// by a set of two whose other member is `sender`, so `sender` holds it.
    ///
    /// Read from what acknowledged, not from the routes now: placement can
    /// shrink a set from three to two at a new generation under the same
    /// leader, and a record the three acknowledged may be on neither of the
    /// two (#1336). The copy's last record must be from the generation it
    /// accepted, which puts everything it lost in that generation. The
    /// failover that followed it names that generation's set
    /// (`fence_storage_ids`, from the assignment just before it), and that
    /// set must be the two the routes show, and `sender` must lead it with
    /// the fence at that generation.
    async fn acknowledged_by_two(
        &self,
        key: &felix_router::ShardKey,
        log: &felix_broker::StreamLog,
        generation: u64,
        sender: Option<&str>,
    ) -> bool {
        self.failover_two(key, log, generation, sender)
            .await
            .is_some_and(|two| two.acknowledged())
    }

    /// The set of two of [`Self::acknowledged_by_two`], every check but the
    /// fence ids' passed, and the copies those ids name.
    async fn failover_two(
        &self,
        key: &felix_router::ShardKey,
        log: &felix_broker::StreamLog,
        generation: u64,
        sender: Option<&str>,
    ) -> Option<FailoverTwo> {
        let sender = sender?;
        let accepted = log.accepted_generation();
        let log_end = log.tail_offset().await.ok()?;
        if last_generation(&log.generations(), log_end) != accepted {
            return None;
        }
        let snapshot = self.router.snapshot();
        let route = snapshot.get(key)?;
        let acknowledged = route.fence_storage_ids.as_deref()?;
        // The fence must be the one at the failover's generation, from the
        // leader of the set of two, not a later fence this view predates.
        if accepted.checked_add(1) != Some(route.generation)
            || route.generation != generation
            || route.leader.node_id != sender
        {
            return None;
        }
        let routed: std::collections::BTreeSet<String> = std::iter::once(&route.leader)
            .chain(&route.replicas)
            .map(|node| node.node_id.clone())
            .collect();
        let local = self.router.local_node_id();
        if routed.len() != 2 || !routed.contains(local) || !routed.contains(sender) {
            return None;
        }
        Some(FailoverTwo {
            routed,
            named: acknowledged.keys().cloned().collect(),
        })
    }

    /// Refuse a fence for a copy that holds no promise on disk unless the
    /// routing view backs it.
    ///
    /// A wiped broker has lost every ballot it gave, so on its own it would
    /// answer a leader its old directory had promised to refuse (#1267).
    /// Every copy without a promise is held to this, not only those on a
    /// directory new this start: nothing on disk tells a wiped copy from a
    /// new one once the broker has restarted again. The
    /// control plane issued each generation it promised before the wipe, and
    /// a routing view taken since names the newest of them or a later one.
    /// An older generation is already refused by the routing view; at the
    /// view's own generation only the leader it names is answered, and a
    /// newer one was never promised. With no route for the shard nothing
    /// says which generations those were, so the fence waits for the view.
    ///
    /// Judged on the shard's own log, which a group or counter log follows.
    /// Once the copy has taken a generation the ballot on disk answers.
    #[allow(clippy::too_many_arguments)]
    async fn refuse_unbacked(
        &self,
        correlation_id: u64,
        key: &felix_router::ShardKey,
        log_kind: felix_broker::LogKind,
        log: &felix_broker::StreamLog,
        generation: u64,
        sender: Option<&str>,
        sent: Option<u64>,
    ) -> Option<InternalMessage> {
        let opened;
        let log = match own_kind(log_kind) {
            None => log,
            Some(own) => {
                opened = self
                    .broker
                    .shard_log_off_runtime(
                        own,
                        &key.tenant_id,
                        &key.namespace,
                        &key.stream,
                        key.shard,
                        None,
                        self.expected_incarnation(key, sent),
                    )
                    .await;
                match &opened {
                    Some(log) => log,
                    // No log of its own: no promise either, and the group or
                    // counter log's fence is refused like the shard's would be.
                    None => return Some(unbacked(correlation_id, key, generation)),
                }
            }
        };
        if crate::unclean::held(log) && !self.is_learner(log) {
            return None;
        }
        let viewed = match self.router.snapshot().get(key) {
            None => Some(unbacked(correlation_id, key, generation)),
            Some(route) if route.generation == generation => match sender {
                Some(sender) if sender != route.leader.node_id => Some(promised_elsewhere(
                    correlation_id,
                    key,
                    generation,
                    &route.leader.node_id,
                    Some(sender),
                )),
                _ => None,
            },
            // A view behind the sender is usual for a fence; the confirmed
            // read below decides, refusing a generation newer than the control
            // plane's under `joint_fence`.
            Some(_) => None,
        };
        if viewed.is_some() {
            return viewed;
        }
        self.refuse_unconfirmed(correlation_id, key, log, generation, sender, sent)
    }

    /// Refuse a leader for a copy with no promise on disk unless the control
    /// plane's confirmed assignment backs it.
    ///
    /// The routing view can lag the control plane: it is seeded from
    /// whichever member answered, so after a wipe it may still name a leader
    /// already replaced (#1332). Only ever refuses more than the view does,
    /// and asks nothing once the copy holds a promise. A control plane that
    /// does not serve the read leaves the view to answer alone, as before.
    ///
    /// The read is made off the peer lane, and a request that has no fresh
    /// answer yet is refused retryably (see
    /// [`crate::confirmed::ConfirmedReads`]). A shard created after this
    /// copy's storage id registered skips it ([`Self::created_here`]).
    fn refuse_unconfirmed(
        &self,
        correlation_id: u64,
        key: &felix_router::ShardKey,
        log: &felix_broker::StreamLog,
        generation: u64,
        sender: Option<&str>,
        sent: Option<u64>,
    ) -> Option<InternalMessage> {
        let learner = self.is_learner(log);
        if crate::unclean::held(log) && !learner {
            return None;
        }
        if !learner && self.created_here(key, generation, sender, sent) {
            return None;
        }
        // Under `joint_fence` a candidate's generation is in no assignment,
        // so a copy with no promise, or one not yet re-admitted, cannot take
        // an unconfirmed leader on the strength of an assignment it has not
        // seen (#1331).
        // Unread, the gate may be on: refuse as if it were. Off, the read
        // answers as before.
        let elections = self.joint_gate() != Some(false);
        let Some(confirmed) = self.confirmed.as_ref() else {
            return elections.then(|| unbacked(correlation_id, key, generation));
        };
        // Never awaited here: a fence must answer within the leader's timeout.
        let fresh = crate::confirmed::freshness(elections, sent.unwrap_or(0));
        let decided =
            self.confirmed_reads
                .decide(confirmed, key, sent.unwrap_or(0), fresh, |answer| {
                    confirmed_refusal(correlation_id, key, generation, sender, elections, answer)
                });
        decided.unwrap_or_else(|| Some(reading_confirmed(correlation_id, key, generation)))
    }

    /// Whether the request is the named leader's at the shard's creation, and
    /// the view's creation set names this copy under the storage id it holds
    /// now. That set was written after this id registered, so after any wipe
    /// of this broker: no earlier copy here held the shard, and this one lost
    /// no promise for the confirmed read to stand in for. Its empty ballot is
    /// a true one, as on any new follower. The same test as the creation case
    /// of `is_wiped_learner` in `promotion/joint.rs`. A shard with no
    /// incarnation is left to the read: a delete and re-create at generation
    /// 1 (which the control plane refuses) would keep the storage id and look
    /// the same here.
    fn created_here(
        &self,
        key: &felix_router::ShardKey,
        generation: u64,
        sender: Option<&str>,
        sent: Option<u64>,
    ) -> bool {
        let Some(own) = self.local_storage else {
            return false;
        };
        if generation != 1 {
            return false;
        }
        let snapshot = self.router.snapshot();
        snapshot.get(key).is_some_and(|route| {
            route.generation == 1
                && route.incarnation != 0
                && sent.unwrap_or(0) == route.incarnation
                && sender == Some(route.leader.node_id.as_str())
                && crate::replica_set::names_storage(route, self.router.local_node_id(), own)
        })
    }

    /// Refuse a request for a shard at an incarnation this broker does
    /// not hold, as a stale route: retried, and taken once the catalog here
    /// has caught up. See `crate::incarnation`.
    ///
    /// Checked before the log opens, for the refusal's reason; the open
    /// itself then takes only the named incarnation
    /// ([`Self::expected_incarnation`]), since a delete and create can land in
    /// between.
    fn check_incarnation(
        &self,
        correlation_id: u64,
        key: &felix_router::ShardKey,
        sent: Option<u64>,
    ) -> Option<InternalMessage> {
        let Some(reason) = crate::incarnation::mismatch(&self.broker, key, sent) else {
            #[cfg(test)]
            if let Some(hook) = &*self.incarnation_checked.lock() {
                hook();
            }
            return None;
        };
        metrics::record_replicated(metrics::OUTCOME_BEHIND);
        Some(refused(correlation_id, ErrorCode::StaleRoute, 0, reason))
    }

    /// The incarnation a log for `key` may open at for a request that named
    /// `sent`, once this broker requires registration; `None` opens whatever
    /// is registered, as before incarnations.
    fn expected_incarnation(&self, key: &felix_router::ShardKey, sent: Option<u64>) -> Option<u64> {
        crate::incarnation::registration_required(&self.broker, key.kind).then(|| sent.unwrap_or(0))
    }

    fn joint_fence(&self) -> bool {
        self.joint_gate() == Some(true)
    }

    /// Whether `log`'s ballot names `sender`, another broker, at the
    /// generation right before `generation`: a fence from the leader this
    /// copy last promised, moving to its next generation. Or at `generation`
    /// itself, as a retried fence finds it: that kept ballot promised the
    /// generation to no one else either. Only after an
    /// unclean start that kept every ballot: a wiped or restored copy may
    /// have promised the next generation to a candidate before it lost that
    /// ballot, and a ballot it took since proves nothing. Such a copy refuses
    /// as it would. Lets a doubted copy promise, never vouch: it stays
    /// doubted. The fence answer says it held ([`FenceOk::kept_ballot`]), and
    /// only that lets a continuing leader count the copy (#2047).
    ///
    /// Nor when the copy took its ballot while its promises counted for
    /// nothing ([`Self::unkeep`]): that ballot looks like any other after a
    /// restart.
    fn continues_from(
        &self,
        key: &felix_router::ShardKey,
        log: &felix_broker::StreamLog,
        generation: u64,
        sender: Option<&str>,
    ) -> bool {
        let kept = self
            .unclean
            .as_ref()
            .is_some_and(|unclean| unclean.kept_promises())
            && !self.unkept(key);
        kept && sender.is_some_and(|sender| {
            sender != self.router.local_node_id()
                && (generation.checked_sub(1) == Some(log.accepted_generation())
                    || generation == log.accepted_generation())
                && log.accepted_leader().as_deref() == Some(sender)
        })
    }

    /// Whether the fleet has `joint_fence`, or `None` while it is off here and
    /// this broker has not yet taken a registration's answer. The peer listener is bound
    /// before registration, so the gate can be unread while requests arrive.
    fn joint_gate(&self) -> Option<bool> {
        joint_gate(self.fleet.as_deref())
    }

    /// Whether `request` is a joint fence whose set being left lists this
    /// broker as a voter.
    fn votes_in_leaving(&self, request: &Fence) -> bool {
        let local = self.router.local_node_id();
        request
            .election
            .as_ref()
            .and_then(|election| election.leaving.as_ref())
            .is_some_and(|leaving| {
                leaving
                    .members
                    .iter()
                    .any(|member| member.node_id == local && !member.learner)
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
                Some(refused_for(
                    correlation_id,
                    ErrorCode::FencedEpoch,
                    RefusalReason::View {
                        have,
                        sender: named,
                    },
                ))
            }
            ReplicaRole::Behind { have, named } => {
                // Not a refusal of the leader, only of this moment: the watch
                // has not caught up. Retryable, and the leader will find us
                // ready once it has.
                metrics::record_replicated(metrics::OUTCOME_BEHIND);
                Some(refused_for(
                    correlation_id,
                    ErrorCode::StaleRoute,
                    RefusalReason::View {
                        have,
                        sender: named,
                    },
                ))
            }
            ReplicaRole::NotAReplica => {
                metrics::record_replicated(metrics::OUTCOME_REFUSED);
                Some(refused_for(
                    correlation_id,
                    ErrorCode::Unauthorized,
                    RefusalReason::NotAReplica,
                ))
            }
        }
    }
}

/// See [`ReplicaHandler::joint_gate`].
pub(crate) fn joint_gate(fleet: Option<&crate::quorum::QuorumMarks>) -> Option<bool> {
    match fleet {
        Some(marks) => marks.fleet_decides(felix_common::fleet::JOINT_FENCE),
        None => Some(false),
    }
}

fn own_kind(log_kind: felix_broker::LogKind) -> Option<felix_broker::LogKind> {
    match log_kind {
        felix_broker::LogKind::Stream | felix_broker::LogKind::Cache => None,
        felix_broker::LogKind::GroupCursors | felix_broker::LogKind::GroupDeadLetters => {
            Some(felix_broker::LogKind::Stream)
        }
        felix_broker::LogKind::Counters => Some(felix_broker::LogKind::Cache),
    }
}

/// A fence refused for a copy with no promise the routing view cannot back
/// yet.
/// Retryable: the leader asks again once this broker's watch has the shard.
fn unbacked(correlation_id: u64, key: &felix_router::ShardKey, generation: u64) -> InternalMessage {
    tracing::info!(
        stream = %key.stream,
        shard = key.shard,
        generation,
        "refusing a fence: this copy holds no promise and the routing view does not \
         have the shard yet",
    );
    metrics::record_replicated(metrics::OUTCOME_BEHIND);
    refused(
        correlation_id,
        ErrorCode::StaleRoute,
        0,
        "this copy holds no promise and the routing view does not have the shard yet".to_string(),
    )
}

/// What a confirmed answer says to a leader at `generation` for a copy with
/// no promise: `None` takes it. Only ever refuses more than the routing view.
fn confirmed_refusal(
    correlation_id: u64,
    key: &felix_router::ShardKey,
    generation: u64,
    sender: Option<&str>,
    elections: bool,
    answer: &crate::confirmed::Confirmed,
) -> Option<InternalMessage> {
    use crate::confirmed::Confirmed;
    match answer {
        Confirmed::NotServed if elections => Some(unbacked(correlation_id, key, generation)),
        Confirmed::NotServed => None,
        // Never held (`ConfirmedReads` drops them); a missing answer would
        // back nothing.
        Confirmed::Unavailable | Confirmed::Unassigned => {
            Some(unbacked(correlation_id, key, generation))
        }
        Confirmed::Assigned {
            generation: assigned,
            ..
        } if *assigned > generation => {
            tracing::warn!(
                stream = %key.stream,
                shard = key.shard,
                assigned,
                sender_generation = generation,
                "refusing a leader the control plane has replaced",
            );
            metrics::record_replicated(metrics::OUTCOME_FENCED);
            Some(refused_for(
                correlation_id,
                ErrorCode::FencedEpoch,
                RefusalReason::Assigned {
                    assigned: *assigned,
                    sender: generation,
                },
            ))
        }
        Confirmed::Assigned {
            generation: assigned,
            leader,
        } if *assigned == generation => match sender {
            Some(sender) if sender != leader.as_str() => Some(promised_elsewhere(
                correlation_id,
                key,
                generation,
                leader,
                Some(sender),
            )),
            _ => None,
        },
        // Newer than the control plane. Without elections no copy can
        // have promised it before a wipe; with them one may have, to a
        // candidate the control plane never saw.
        Confirmed::Assigned { .. } if elections => Some(unbacked(correlation_id, key, generation)),
        Confirmed::Assigned { .. } => None,
    }
}

/// Refuse while the control plane's confirmed assignment is being read off
/// the lane. Retryable; the retry is answered from the read.
fn reading_confirmed(
    correlation_id: u64,
    key: &felix_router::ShardKey,
    generation: u64,
) -> InternalMessage {
    tracing::debug!(
        stream = %key.stream,
        shard = key.shard,
        generation,
        "refusing a leader until the control plane's confirmed assignment is read",
    );
    metrics::record_replicated(metrics::OUTCOME_BEHIND);
    refused(
        correlation_id,
        ErrorCode::StaleRoute,
        0,
        "this copy holds no promise and is reading the control plane's assignment".to_string(),
    )
}

fn log_kind(log: ReplicaLog) -> felix_broker::LogKind {
    match log {
        ReplicaLog::Stream => felix_broker::LogKind::Stream,
        ReplicaLog::Cache => felix_broker::LogKind::Cache,
        ReplicaLog::GroupCursors => felix_broker::LogKind::GroupCursors,
        ReplicaLog::GroupDeadLetters => felix_broker::LogKind::GroupDeadLetters,
        ReplicaLog::Counters => felix_broker::LogKind::Counters,
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
        Ok(check) => ballot_refusal(check, correlation_id, key, generation, sender),
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

/// The refusal a ballot's answer to `sender` at `generation` calls for, if
/// any.
fn ballot_refusal(
    check: GenerationCheck,
    correlation_id: u64,
    key: &felix_router::ShardKey,
    generation: u64,
    sender: Option<&str>,
) -> Option<InternalMessage> {
    match check {
        GenerationCheck::Current | GenerationCheck::Raised => None,
        GenerationCheck::Promised { leader } => Some(promised_elsewhere(
            correlation_id,
            key,
            generation,
            &leader,
            sender,
        )),
        GenerationCheck::Superseded { accepted } => {
            tracing::warn!(
                stream = %key.stream,
                shard = key.shard,
                accepted,
                sender_generation = generation,
                "refusing a leader older than one this replica already accepted",
            );
            metrics::record_replicated(metrics::OUTCOME_FENCED);
            Some(refused_for(
                correlation_id,
                ErrorCode::FencedEpoch,
                RefusalReason::Accepted {
                    accepted,
                    sender: generation,
                },
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
    refused_for(
        correlation_id,
        ErrorCode::FencedEpoch,
        RefusalReason::PromisedTo {
            generation,
            leader: leader.to_string(),
        },
    )
}

/// The detail of a refusal at a generation promised to `leader`. Brokers that
/// predate [`RefusalReason`] send only this text, which [`promised_to`] reads
/// back from them.
pub(crate) fn promised_detail(generation: u64, leader: &str) -> String {
    format!("this broker accepted generation {generation} from {leader}")
}

/// The node a `FencedEpoch` refusal says `generation` was promised to, or
/// `None` when the refusal is for any other reason, such as a newer
/// generation.
pub(crate) fn promised_to(err: &ReplicateError, generation: u64) -> Option<&str> {
    match &err.reason {
        Some(RefusalReason::PromisedTo {
            generation: promised,
            leader,
        }) => (*promised == generation && !leader.is_empty()).then_some(leader.as_str()),
        Some(_) => None,
        None => err
            .detail
            .strip_prefix(&format!(
                "this broker accepted generation {generation} from "
            ))
            .filter(|leader| !leader.is_empty()),
    }
}

/// What a replica answers a batch for a shard whose route at that
/// generation does not name it a follower; [`refuses_leader`] reads it back.
pub(crate) const NOT_A_REPLICA: &str = "this broker is not a replica of that shard";

/// What a replica answers a fence while it does not vote; [`not_a_voter`]
/// reads it back.
pub(crate) const NOT_A_VOTER: &str = "this broker does not vote until a set change re-admits it";

/// Whether a refusal says its sender holds no vote, so the position it would
/// have reported counts for nothing.
pub(crate) fn not_a_voter(err: &ReplicateError) -> bool {
    match &err.reason {
        Some(reason) => *reason == RefusalReason::NotVoter,
        None => err.detail == NOT_A_VOTER,
    }
}

/// Whether a follower's refusal of a leader at `generation` says its copy
/// does not follow that leader there: its route names it outside the set,
/// as the leader the control plane named, or its ballot holds the
/// generation for another node.
pub(crate) fn refuses_leader(err: &ReplicateError, generation: u64) -> bool {
    match err.code {
        ErrorCode::Unauthorized => match &err.reason {
            Some(reason) => *reason == RefusalReason::NotAReplica,
            None => err.detail == NOT_A_REPLICA,
        },
        ErrorCode::FencedEpoch => promised_to(err, generation).is_some(),
        _ => false,
    }
}

/// What a replica answers a sender at `generation` once its ballot holds
/// the newer `accepted`; [`superseded_by`] reads it back.
pub(crate) fn superseded_detail(accepted: u64, generation: u64) -> String {
    format!("this broker accepted generation {accepted}, the sender is at {generation}")
}

/// What a replica answers a fetch at `generation` when its ballot holds
/// `accepted`; [`superseded_by`] reads a newer one back.
pub(crate) fn reader_detail(accepted: u64, generation: u64) -> String {
    format!("this broker accepted generation {accepted}, the reader is at {generation}")
}

/// What a replica answers a sender at `generation` when the control plane's
/// confirmed assignment is at the newer `assigned`; [`assigned_newer`] reads
/// it back.
pub(crate) fn assigned_detail(assigned: u64, generation: u64) -> String {
    format!("the control plane assigned generation {assigned}, the sender is at {generation}")
}

/// Whether a `FencedEpoch` refusal of a sender at `generation` says the
/// control plane assigned a newer generation. Unlike a ballot or a view,
/// which an election may leave behind with no leader, that names one.
pub(crate) fn assigned_newer(err: &ReplicateError, generation: u64) -> bool {
    match &err.reason {
        Some(RefusalReason::Assigned { assigned, sender }) => {
            *sender == generation && *assigned > generation
        }
        Some(_) => false,
        None => err
            .detail
            .strip_prefix("the control plane assigned generation ")
            .and_then(|rest| rest.strip_suffix(&format!(", the sender is at {generation}")))
            .and_then(|assigned| assigned.parse::<u64>().ok())
            .is_some_and(|assigned| assigned > generation),
    }
}

/// The newer generation a `FencedEpoch` refusal of a sender at `generation`
/// says the control plane assigned, if it says so.
pub(crate) fn assigned_by(err: &ReplicateError, generation: u64) -> Option<u64> {
    match &err.reason {
        Some(RefusalReason::Assigned { assigned, sender }) => {
            (*sender == generation && *assigned > generation).then_some(*assigned)
        }
        Some(_) => None,
        None => err
            .detail
            .strip_prefix("the control plane assigned generation ")
            .and_then(|rest| rest.strip_suffix(&format!(", the sender is at {generation}")))
            .and_then(|assigned| assigned.parse::<u64>().ok())
            .filter(|assigned| *assigned > generation),
    }
}

/// What a replica answers a sender at `generation` when its routing view is
/// at `have`; [`superseded_by`] reads it back from a fenced refusal.
pub(crate) fn view_detail(have: u64, generation: u64) -> String {
    format!("this broker is at generation {have}, the sender at {generation}")
}

/// The newer generation a `FencedEpoch` refusal says this replica holds,
/// when it refused a sender at `generation` for that: its ballot, or its
/// routing view ([`ReplicaRole::Fenced`]). A view above the control plane's
/// comes from a candidate's fence this replica took, whose ballot it also
/// holds. `None` for any other refusal.
pub(crate) fn superseded_by(err: &ReplicateError, generation: u64) -> Option<u64> {
    let newer = match &err.reason {
        Some(
            RefusalReason::Accepted {
                accepted: newer,
                sender,
            }
            | RefusalReason::View {
                have: newer,
                sender,
            },
        ) => (*sender == generation).then_some(*newer)?,
        Some(_) => return None,
        None => {
            let detail = &err.detail;
            detail
                .strip_prefix("this broker accepted generation ")
                .and_then(|rest| {
                    rest.strip_suffix(&format!(", the sender is at {generation}"))
                        .or_else(|| rest.strip_suffix(&format!(", the reader is at {generation}")))
                })
                .or_else(|| {
                    detail
                        .strip_prefix("this broker is at generation ")?
                        .strip_suffix(&format!(", the sender at {generation}"))
                })?
                .parse::<u64>()
                .ok()?
        }
    };
    (newer > generation).then_some(newer)
}

/// The text a refusal for `reason` carries, which is all a leader that
/// predates [`RefusalReason`] reads.
fn detail_of(reason: &RefusalReason) -> String {
    match reason {
        RefusalReason::PromisedTo { generation, leader } => promised_detail(*generation, leader),
        RefusalReason::Accepted { accepted, sender } => superseded_detail(*accepted, *sender),
        RefusalReason::View { have, sender } => view_detail(*have, *sender),
        RefusalReason::Assigned { assigned, sender } => assigned_detail(*assigned, *sender),
        RefusalReason::NotAReplica => NOT_A_REPLICA.to_string(),
        RefusalReason::NotVoter => NOT_A_VOTER.to_string(),
    }
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
pub(crate) async fn label_appended(
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
            log.label_generations_off_runtime(from, epochs).await
        }
        None => log
            .record_generation_off_runtime(sender_generation, from)
            .await
            .map(|_| ()),
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

/// What a copy of `key`'s log attests in a fence answer, for this broker's
/// copy whether it answers as a replica or fences as a promoted leader. See
/// [`ReplicaHandler::attestation`].
pub(crate) fn attest_copy(
    fleet: Option<&crate::quorum::QuorumMarks>,
    unclean: Option<&crate::unclean::UncleanStart>,
    poisoned: bool,
    key: &felix_router::ShardKey,
    voter: bool,
    given_up: bool,
) -> felix_wire::internal::FenceAttestation {
    let shard = crate::driver::watch_key(key);
    let name = crate::unclean::copy_key(&shard);
    let trust = fleet.and_then(crate::quorum::QuorumMarks::copy_trust);
    let kept = voter
        && fleet.is_some_and(crate::quorum::QuorumMarks::kept_ballots)
        && !trust.is_some_and(|trust| trust.unkept(&name).is_some());
    let trusted = kept
        && !given_up
        && !trust.is_some_and(|trust| trust.doubted(&name))
        && !poisoned
        && unclean.is_none_or(|unclean| unclean.trusted(&shard))
        && trust.is_none_or(|trust| trust.trusted(&name));
    felix_wire::internal::FenceAttestation {
        kept_from: if kept { 0 } else { u64::MAX },
        trusted,
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

/// A refusal for `reason`, with the detail an older leader reads instead.
fn refused_for(correlation_id: u64, code: ErrorCode, reason: RefusalReason) -> InternalMessage {
    InternalMessage::ReplicateError(ReplicateError {
        correlation_id,
        code,
        expected_offset: 0,
        detail: detail_of(&reason),
        reason: Some(reason),
    })
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
        reason: None,
    })
}

#[cfg(test)]
mod tests;
