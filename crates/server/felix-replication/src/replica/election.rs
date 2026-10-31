//! The replica's set checks, for fences and batches that carry a replica set.
//!
//! Rule 4 of "Electing without the control plane" in
//! `docs/replication-design.md`: a replica answers a fence or a batch only at
//! the newest set it knows, never at an older one, and it writes a newer set
//! to disk before it answers at it. `SetCurrent`, `LearnSet`, `AnswerFence`
//! and `RefuseStaleSet` in `docs/formal/FelixShard.tla`.
//!
//! A fence or batch without a set skips all of this, so a broker that never
//! offered `ELECTION` behaves as it did.

use std::time::Instant;

use felix_storage::disk_log::{ReplicaSet, SetCheck};
use felix_wire::internal::{ErrorCode, FenceElection, InternalMessage, ReplicaSetView};

use super::{ReplicaHandler, refused};
use crate::peer::metrics;

impl ReplicaHandler {
    /// Checks a fence's set before its ballot is taken: an older set is
    /// refused naming the newer one, a broker that does not vote in the set
    /// refuses, and so does one that still hears its leader when a candidate
    /// asks. Writes nothing.
    pub(super) fn check_fence_set(
        &self,
        correlation_id: u64,
        key: &felix_router::ShardKey,
        log: &felix_broker::StreamLog,
        election: &FenceElection,
        generation: u64,
    ) -> Option<InternalMessage> {
        // A set is stamped with a route generation no newer than the fence's
        // own. One ahead is a sender running ahead of itself; taking it would
        // make this replica refuse the real leader's batches as StaleSet.
        // Unauthorized, not StaleSet: nobody is deposed, the answer just
        // must not count.
        if election.set.generation > generation {
            metrics::record_replicated(metrics::OUTCOME_REFUSED);
            return Some(refused(
                correlation_id,
                ErrorCode::Unauthorized,
                0,
                format!(
                    "the set of generation {} is newer than the fence's generation {generation}",
                    election.set.generation
                ),
            ));
        }
        if let Some(refusal) = stale_set(correlation_id, key, log, election.set.generation) {
            return Some(refusal);
        }
        let local = self.router.local_node_id();
        let votes_in = |set: &ReplicaSetView| {
            set.members
                .iter()
                .any(|member| member.node_id == local && !member.learner)
        };
        // Learners never vote (#1331), and a broker outside the set has no
        // say in it. Unauthorized rather than FencedEpoch: the sender is not
        // superseded, it just must not count this answer.
        if !votes_in(&election.set) && !election.leaving.as_ref().is_some_and(votes_in) {
            metrics::record_replicated(metrics::OUTCOME_REFUSED);
            return Some(refused(
                correlation_id,
                ErrorCode::Unauthorized,
                0,
                format!(
                    "this broker does not vote in the set of generation {}",
                    election.set.generation
                ),
            ));
        }
        // A candidate replaces a leader only when the leader has gone quiet.
        // Refusing while it still ships keeps a replica that merely lost
        // touch with it from deposing a working leader.
        if election.candidate && self.heard_leader_within(key) {
            metrics::record_replicated(metrics::OUTCOME_REFUSED);
            return Some(refused(
                correlation_id,
                ErrorCode::Unavailable,
                0,
                "this replica still hears its leader".to_string(),
            ));
        }
        None
    }

    /// Persist the fence's set after its ballot and before the answer, so a
    /// restart cannot answer at an older set than this one.
    pub(super) async fn keep_fence_set(
        &self,
        correlation_id: u64,
        key: &felix_router::ShardKey,
        log: &felix_broker::StreamLog,
        election: &FenceElection,
    ) -> Option<InternalMessage> {
        let set = ReplicaSet {
            current: crate::replica_set::membership(&election.set),
            leaving: election
                .leaving
                .as_ref()
                .map(crate::replica_set::membership),
        };
        keep(correlation_id, key, log, set).await
    }

    /// Check a batch stamped with the generation of the sender's set, after
    /// its ballot is taken and before it is stored. A newer set is learned
    /// from the routing view when it has that generation; otherwise the
    /// batch is refused for a fence to carry the set first.
    pub(super) async fn check_batch_set(
        &self,
        correlation_id: u64,
        key: &felix_router::ShardKey,
        log: &felix_broker::StreamLog,
        stamped: u64,
        generation: u64,
    ) -> Option<InternalMessage> {
        if let Some(refusal) = stale_set(correlation_id, key, log, stamped) {
            return Some(refusal);
        }
        // A leader stamps its route generation, which is the batch's own. Any
        // other stamp is refused as StaleRoute (retried) rather than StaleSet,
        // which would halt a leader that is not at fault.
        if stamped != generation {
            metrics::record_replicated(metrics::OUTCOME_REFUSED);
            return Some(refused(
                correlation_id,
                ErrorCode::StaleRoute,
                0,
                format!(
                    "the set of generation {stamped} does not match the batch's generation {generation}"
                ),
            ));
        }
        if stamped == log.set_generation() {
            return None;
        }
        let snapshot = self.router.snapshot();
        let Some(route) = snapshot
            .get(key)
            .filter(|route| route.generation == stamped)
        else {
            metrics::record_replicated(metrics::OUTCOME_REFUSED);
            return Some(refused(
                correlation_id,
                ErrorCode::StaleRoute,
                0,
                format!(
                    "this broker does not know the set of generation {stamped}; a fence carries it"
                ),
            ));
        };
        keep(
            correlation_id,
            key,
            log,
            crate::replica_set::kept(route, None),
        )
        .await
    }

    /// Note that this replica heard its leader for `key`, for the stickiness
    /// check. Called only for fences and batches that carry a set, so a
    /// broker without `ELECTION` takes no lock here.
    pub(super) fn heard_leader(&self, key: &felix_router::ShardKey) {
        self.heard.lock().insert(key.clone(), Instant::now());
    }

    fn heard_leader_within(&self, key: &felix_router::ShardKey) -> bool {
        self.heard
            .lock()
            .get(key)
            .is_some_and(|heard| heard.elapsed() < self.stickiness)
    }
}

/// Refuse a set older than the one this log counts, naming the newer one.
fn stale_set(
    correlation_id: u64,
    key: &felix_router::ShardKey,
    log: &felix_broker::StreamLog,
    sent: u64,
) -> Option<InternalMessage> {
    let known = log.set_generation();
    (sent < known).then(|| refuse_stale(correlation_id, key, sent, known))
}

fn refuse_stale(
    correlation_id: u64,
    key: &felix_router::ShardKey,
    sent: u64,
    known: u64,
) -> InternalMessage {
    tracing::warn!(
        stream = %key.stream,
        shard = key.shard,
        known,
        sent,
        "refusing a sender that counts an older replica set",
    );
    metrics::record_replicated(metrics::OUTCOME_FENCED);
    refused(
        correlation_id,
        ErrorCode::StaleSet,
        known,
        format!(
            "this replica counts the set of generation {known}, newer than the sender's {sent}"
        ),
    )
}

async fn keep(
    correlation_id: u64,
    key: &felix_router::ShardKey,
    log: &felix_broker::StreamLog,
    set: ReplicaSet,
) -> Option<InternalMessage> {
    let sent = set.current.generation;
    match log.accept_set(set).await {
        Ok(SetCheck::Current | SetCheck::Raised) => None,
        // Raised by another request since the check above.
        Ok(SetCheck::Stale { known }) => Some(refuse_stale(correlation_id, key, sent, known)),
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
