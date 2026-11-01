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
use felix_wire::internal::{
    ErrorCode, Fence, FenceElection, InternalMessage, PreVote, PreVoteAnswer, PreVoteRefusal,
    ReplicaSetView,
};

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
        let set = if self.joint_fence() {
            self.mark_local(log, set, self.readmission(election, log))
        } else {
            set
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
        let set = crate::replica_set::kept(route, None);
        let set = if self.joint_fence() {
            self.mark_local(log, set, false)
        } else {
            set
        };
        keep(correlation_id, key, log, set).await
    }

    /// Whether this copy's log lists it as a learner: wiped, or re-admitted
    /// and not yet caught up. Always false without `joint_fence`.
    pub(super) fn is_learner(&self, log: &felix_broker::StreamLog) -> bool {
        self.joint_fence() && log.member_is_learner(self.router.local_node_id()) == Some(true)
    }

    /// Whether this copy may vote: its log keeps a set that lists it as a
    /// voter. No set, or a learner mark, is a copy that cannot say it holds
    /// what a majority acknowledged (#1331).
    fn is_voter(&self, log: &felix_broker::StreamLog) -> bool {
        log.member_is_learner(self.router.local_node_id()) == Some(false)
    }

    /// The set as this copy keeps it. It lists this copy as a learner unless
    /// the log already kept it as a voter and the set does not name another
    /// storage id for it, so a copy that lost its directory (no set, or a set
    /// written for the old identity) is never a voter by what it learns. A
    /// learner stays one until [`Self::admit_when_caught_up`]. A set learned
    /// outside the re-admission drops the set being left, so the pending
    /// change cannot later read as one.
    fn mark_local(
        &self,
        log: &felix_broker::StreamLog,
        mut set: ReplicaSet,
        readmission: bool,
    ) -> ReplicaSet {
        let local = self.router.local_node_id();
        let was_voter = self.is_voter(log);
        for member in set.current.members.iter_mut().filter(|m| m.node == local) {
            let other = matches!(
                (member.storage, self.local_storage),
                (Some(named), Some(own)) if named != own
            );
            member.learner |= other || self.local_storage.is_none() || !was_voter;
            if member.learner && !readmission {
                set.leaving = None;
            }
        }
        set
    }

    /// Whether `election` is the fence of this copy's re-admission: a joint
    /// fence from a leader, naming this copy a voter under the storage id it
    /// holds now, in a set newer than the one it keeps. The set change is
    /// what ends the wait; a report that this copy caught up did so for the
    /// identity the control plane read then, and a set naming another id is
    /// refused here (`Readmit` in `docs/formal/FelixShard.tla`).
    fn readmission(&self, election: &FenceElection, log: &felix_broker::StreamLog) -> bool {
        let local = self.router.local_node_id();
        let Some(own) = self.local_storage else {
            return false;
        };
        election.leaving.is_some()
            && !election.candidate
            && election.set.generation > log.set_generation()
            && election.set.members.iter().any(|member| {
                member.node_id == local
                    && !member.learner
                    && member.storage_id == Some(own.to_bytes())
            })
    }

    /// Under `joint_fence`, refuse a fence from a copy that is not a voter,
    /// after learning the set the fence carries. It promises nothing: its
    /// answer would count toward a majority it may hold none of the
    /// acknowledged records for. Only the fence of its re-admission is taken.
    /// `AnswerFence`'s `NonVoter` guard and `WipedLearnSet`.
    pub(super) async fn refuse_non_voter_fence(
        &self,
        correlation_id: u64,
        key: &felix_router::ShardKey,
        log: &felix_broker::StreamLog,
        request: &Fence,
    ) -> Option<InternalMessage> {
        if !self.joint_fence() || self.is_voter(log) {
            return None;
        }
        let local = self.router.local_node_id();
        match &request.election {
            Some(election) if self.readmission(election, log) => return None,
            Some(election) => {
                // A set from a sender running ahead of itself is refused by
                // the set check; learning it would mask that.
                if election.set.generation <= request.shard.generation {
                    let set = ReplicaSet {
                        current: crate::replica_set::membership(&election.set),
                        leaving: None,
                    };
                    let set = self.mark_local(log, set, false);
                    if let Err(err) = log.accept_set(set).await {
                        tracing::warn!(error = %err, "could not keep the set a fence carried");
                    }
                }
            }
            // A fence without a set cannot say who votes. Only a copy the log
            // already lists as a learner refuses it, so a shard that never
            // kept a set answers as it did.
            None if log.member_is_learner(local) != Some(true) => return None,
            None => {}
        }
        tracing::warn!(
            stream = %key.stream,
            shard = key.shard,
            generation = request.shard.generation,
            "refusing a fence: this copy is not a voter until a set change re-admits it",
        );
        metrics::record_replicated(metrics::OUTCOME_REFUSED);
        Some(refused(
            correlation_id,
            ErrorCode::Unauthorized,
            0,
            "this broker does not vote until a set change re-admits it".to_string(),
        ))
    }

    /// Count this copy as a voter once its re-admission set is kept and its
    /// log holds the generation-start record of that set, which every record
    /// acknowledged before it precedes. `Readmitted` in
    /// `docs/formal/FelixShard.tla`. Run after a batch is stored.
    pub(super) async fn admit_when_caught_up(&self, log: &felix_broker::StreamLog) {
        if !self.is_learner(log) {
            return;
        }
        let Some(own) = self.local_storage else {
            return;
        };
        let Some(set) = log.replica_set() else {
            return;
        };
        let local = self.router.local_node_id();
        // Only a set kept at a re-admission carries the set being left; one
        // learned any other way is a learner's until the next change.
        let named = set
            .current
            .members
            .iter()
            .any(|member| member.node == local && member.storage == Some(own));
        if set.leaving.is_none() || !named {
            return;
        }
        let Ok(tail) = log.tail_offset().await else {
            return;
        };
        let held = log
            .generations()
            .iter()
            .any(|epoch| epoch.generation == set.current.generation && epoch.start_offset < tail);
        if !held {
            return;
        }
        match log.admit_member(local).await {
            Ok(true) => tracing::info!(
                generation = set.current.generation,
                "re-admitted: caught up through the start of the set's generation",
            ),
            Ok(false) => {}
            Err(err) => tracing::warn!(error = %err, "could not record this copy's re-admission"),
        }
    }

    /// Answer a pre-vote. Writes nothing, so it is safe for a copy that does
    /// not vote: that one answers `NotVoting`, which no asker counts.
    pub async fn pre_vote(&self, request: PreVote) -> InternalMessage {
        let correlation_id = request.correlation_id;
        let log_kind = super::log_kind(request.log);
        let key = super::shard_key(&request.shard, log_kind);
        let answer = |refusal: Option<PreVoteRefusal>| {
            InternalMessage::PreVoteAnswer(PreVoteAnswer {
                correlation_id,
                refusal,
            })
        };
        let Some(log) = self
            .broker
            .shard_log_for(
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
            return answer(Some(PreVoteRefusal::UnknownShard));
        };
        if !self.is_voter(&log) {
            return answer(Some(PreVoteRefusal::NotVoting));
        }
        if request.set_generation < log.set_generation() {
            return answer(Some(PreVoteRefusal::StaleSet));
        }
        if log.accepted_generation() >= request.shard.generation {
            return answer(Some(PreVoteRefusal::StaleGeneration));
        }
        if self.heard_leader_within(&key) {
            return answer(Some(PreVoteRefusal::LeaderReachable));
        }
        let Ok(end) = log.tail_offset().await else {
            return answer(Some(PreVoteRefusal::UnknownShard));
        };
        let mine = (super::last_generation(&log.generations(), end), end);
        if mine > (request.last_generation, request.log_end) {
            return answer(Some(PreVoteRefusal::LogBehind));
        }
        answer(None)
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
