//! The replica's set checks, for fences and batches that carry a replica set.
//!
//! Rule 4 of "Electing without the control plane" in
//! `docs/replication-design.md`: a replica answers a fence or a batch only at
//! the newest set it knows, never at an older one, and it writes a newer set
//! to disk before it answers at it. `SetCurrent`, `LearnSet`, `AnswerFence`
//! and `RefuseStaleSet` in `docs/formal/FelixShard.tla`.
//!
//! A fence or batch without a set skips the set checks, so a broker that
//! never offered `ELECTION` behaves as it did. Under `joint_fence` a learner
//! still refuses such a fence.

use std::time::Instant;

use felix_storage::disk_log::{Membership, ReplicaSet, SetCheck};
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
        keep(correlation_id, key, log, self.mark_local(log, set)).await
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
        if let Some(refusal) = self.refuse_undecided(correlation_id, key, log, &set.current) {
            return Some(refusal);
        }
        let was_learner = self.is_learner(log);
        if let Some(refusal) = keep(correlation_id, key, log, self.mark_local(log, set)).await {
            return Some(refusal);
        }
        // Marked by this batch: it was checked as a voter's, so it is refused
        // and comes again to be checked as a learner's.
        (!was_learner && self.is_learner(log)).then(|| undecided(correlation_id, key))
    }

    /// Whether this copy's log lists it as a learner: a set named another
    /// storage id for it, so it lost what that copy promised and held, and it
    /// has not caught up since. Read whatever the gate says, so a gate that
    /// reads off for a moment cannot turn a wiped copy back into a voter.
    pub(super) fn is_learner(&self, log: &felix_broker::StreamLog) -> bool {
        log.member_is_learner(self.router.local_node_id()) == Some(true)
    }

    /// Whether this copy's log marks it wiped: it learned it lost what it
    /// held. Outlives the learner mark until the group logs catch up too
    /// ([`Self::admit_when_caught_up`]).
    fn is_wiped(&self, log: &felix_broker::StreamLog) -> bool {
        log.member_is_wiped(self.router.local_node_id())
    }

    /// Whether `set` names this broker with a storage id other than its own:
    /// the evidence of a wipe (#1331). A set naming none, or no set at all, is
    /// no evidence, so a new shard's copies and copies written before sets
    /// existed vote (`Creates` and `CreateCountsEmpty` in
    /// `docs/formal/FelixShard.tla`).
    fn names_other_copy<'a>(
        &self,
        mut members: impl Iterator<Item = (&'a str, Option<felix_storage::StorageId>)>,
    ) -> bool {
        let local = self.router.local_node_id();
        let Some(own) = self.local_storage else {
            return false;
        };
        members.any(|(node, storage)| node == local && storage.is_some_and(|named| named != own))
    }

    /// Whether `set` names this broker under the storage id it holds now.
    fn names_own_copy<'a>(
        &self,
        mut members: impl Iterator<Item = (&'a str, Option<felix_storage::StorageId>)>,
    ) -> bool {
        let local = self.router.local_node_id();
        let Some(own) = self.local_storage else {
            return false;
        };
        members.any(|(node, storage)| node == local && storage == Some(own))
    }

    /// A copy whose log holds no set yet, on a broker the control plane last
    /// registered under another storage id, may have held this shard under
    /// that id: nothing it learns can say, since a set written after the new
    /// id registered names it. A set at generation 1 naming the new id
    /// (`names_own`) is the shard's creation after the wipe, which no copy
    /// held before; one naming no ids may be a shard older than member ids.
    /// A broker new to the control plane holds no such evidence, so its
    /// copies vote.
    fn replaced_copy_of(
        &self,
        log: &felix_broker::StreamLog,
        generation: u64,
        names_own: bool,
    ) -> bool {
        self.replaced_storage && !(generation == 1 && names_own) && log.replica_set().is_none()
    }

    fn names_other_in(&self, log: &felix_broker::StreamLog, set: &Membership) -> bool {
        let members = || {
            set.members
                .iter()
                .map(|member| (member.node.as_str(), member.storage))
        };
        self.replaced_copy_of(log, set.generation, self.names_own_copy(members()))
            || self.names_other_copy(members())
    }

    fn names_other_in_view(&self, log: &felix_broker::StreamLog, set: &ReplicaSetView) -> bool {
        let members = || {
            set.members.iter().map(|member| {
                (
                    member.node_id.as_str(),
                    member.storage_id.map(felix_storage::StorageId::from_bytes),
                )
            })
        };
        self.replaced_copy_of(log, set.generation, self.names_own_copy(members()))
            || self.names_other_copy(members())
    }

    /// The set as this copy keeps it: a learner stays one, and under
    /// `joint_fence` a set naming another storage id for this copy makes it
    /// a learner marked wiped, apart from a learner placement staged. The
    /// entry in the set being left is marked too, so a set listing the copy
    /// only there still records the mark; the log keeps it from then on
    /// whatever later sets list. Only [`Self::admit_when_caught_up`] ends
    /// either mark.
    fn mark_local(&self, log: &felix_broker::StreamLog, mut set: ReplicaSet) -> ReplicaSet {
        let local = self.router.local_node_id();
        let evidence =
            self.non_voting_gate() == Some(true) && self.names_other_in(log, &set.current);
        let learner = self.is_learner(log) || evidence;
        let wiped = self.is_wiped(log) || evidence;
        let leaving = set
            .leaving
            .iter_mut()
            .flat_map(|leaving| &mut leaving.members);
        for member in set
            .current
            .members
            .iter_mut()
            .chain(leaving)
            .filter(|m| m.node == local)
        {
            member.learner |= learner;
            member.wiped |= wiped;
        }
        set
    }

    /// Refuse, retryably, a set that names another storage id for this copy
    /// while the `joint_fence` gate is unread: whether that makes it a
    /// learner is the gate's answer. Copies with no such evidence answer as
    /// they would, so a broker restarting with its data serves before it
    /// registers.
    fn refuse_undecided(
        &self,
        correlation_id: u64,
        key: &felix_router::ShardKey,
        log: &felix_broker::StreamLog,
        set: &Membership,
    ) -> Option<InternalMessage> {
        (self.non_voting_gate().is_none() && !self.is_learner(log) && self.names_other_in(log, set))
            .then(|| undecided(correlation_id, key))
    }

    /// Refuse a fence from a learner, after learning the set the fence
    /// carries. It promises nothing: its answer would count toward a
    /// majority it may hold none of the acknowledged records for. That
    /// includes the fence of the set change that re-admits it, so no leader
    /// counts it before it catches up, and a fence with no set, which a
    /// leader sends a replica whose capabilities it has not learned.
    /// `AnswerFence`'s `NonVoter` guard and `WipedLearnSet`.
    ///
    /// A group or counter log keeps no set: the shard's own log decides, as
    /// `AnswerCounterFence`'s guard does. A group log's fence is refused
    /// for as long as the copy is marked wiped, past its re-admission: the
    /// group logs carry no generation-start record to admit them by.
    pub(super) async fn refuse_non_voter_fence(
        &self,
        correlation_id: u64,
        key: &felix_router::ShardKey,
        log_kind: felix_broker::LogKind,
        log: &felix_broker::StreamLog,
        request: &Fence,
    ) -> Option<InternalMessage> {
        let opened;
        let own = match super::own_kind(log_kind) {
            None => log,
            Some(kind) => {
                opened = self
                    .broker
                    .shard_log_for(
                        kind,
                        &key.tenant_id,
                        &key.namespace,
                        &key.stream,
                        key.shard,
                        None,
                        self.expected_incarnation(key, request.shard.incarnation),
                    )
                    .await;
                // With no log of its own, `refuse_unbacked` refuses it.
                opened.as_ref().unwrap_or(log)
            }
        };
        let group = matches!(
            log_kind,
            felix_broker::LogKind::GroupCursors | felix_broker::LogKind::GroupDeadLetters
        );
        let learner = self.is_learner(own) || (group && self.is_wiped(own));
        let evidence = match &request.election {
            Some(election) => self.names_other_in_view(own, &election.set),
            None => self.replaced_copy_of(own, request.shard.generation, false),
        };
        match self.non_voting_gate() {
            _ if !learner && !evidence => return None,
            None if !learner => return Some(undecided(correlation_id, key)),
            Some(false) if !learner => return None,
            _ => {}
        }
        // A set from a sender running ahead of itself is refused by the set
        // check; learning it would mask that.
        if let Some(election) = &request.election
            && election.set.generation <= request.shard.generation
        {
            let set = ReplicaSet {
                current: crate::replica_set::membership(&election.set),
                leaving: election
                    .leaving
                    .as_ref()
                    .map(crate::replica_set::membership),
            };
            if let Err(err) = log.accept_set(self.mark_local(log, set)).await {
                tracing::warn!(error = %err, "could not keep the set a fence carried");
            }
        }
        tracing::warn!(
            stream = %key.stream,
            shard = key.shard,
            generation = request.shard.generation,
            "refusing a fence: this copy is not a voter until it catches up with a set naming it",
        );
        metrics::record_replicated(metrics::OUTCOME_REFUSED);
        Some(refused(
            correlation_id,
            ErrorCode::Unauthorized,
            0,
            "this broker does not vote until a set change re-admits it".to_string(),
        ))
    }

    /// Count this copy as a voter once its kept set names it under the
    /// storage id it holds now and its log holds the generation-start record
    /// of that set's generation. `Readmitted` in `docs/formal/FelixShard.tla`.
    ///
    /// Only the control plane can name this id, and only after the wipe, so
    /// the leader at that generation fenced after it, and opened without this
    /// copy, which refuses every fence while a learner. A learner takes
    /// batches only from the leader the control plane confirms at their
    /// generation, so the record is that leader's, and every record a
    /// majority acknowledged precedes it. A cache's counter log must hold its
    /// own record too: its fence is answered once the cache log is admitted
    /// (`ReadmitChecksCounters`). A newer set naming it again moves the
    /// target, so a leader lost before the copy caught up does not strand it.
    ///
    /// The wiped mark ends once each group log also holds a record written at
    /// that generation or later, which follows everything the leader that
    /// wrote it took when it opened (it took no group answer from this copy
    /// while marked). Until then its group fences are refused, and as a
    /// leader its own group logs do not count. Run after a batch is stored on
    /// any of the shard's logs.
    ///
    /// Returns whether the copy is still a learner, which the answer to the
    /// batch says ([`ReplicateOk::learner`]) so the leader leaves it out of
    /// its marks.
    ///
    /// [`ReplicateOk::learner`]: felix_wire::internal::ReplicateOk::learner
    pub(super) async fn admit_when_caught_up(
        &self,
        key: &felix_router::ShardKey,
        log_kind: felix_broker::LogKind,
        log: &felix_broker::StreamLog,
    ) -> bool {
        let opened;
        let own = match super::own_kind(log_kind) {
            None => log,
            Some(kind) => {
                opened = self
                    .broker
                    .shard_log(kind, &key.tenant_id, &key.namespace, &key.stream, key.shard)
                    .await;
                match &opened {
                    Some(own) => own,
                    None => return false,
                }
            }
        };
        self.admit(key, own).await;
        self.is_learner(own)
    }

    async fn admit(&self, key: &felix_router::ShardKey, own: &felix_broker::StreamLog) {
        let learner = self.is_learner(own);
        if !learner && !self.is_wiped(own) {
            return;
        }
        let Some(own_id) = self.local_storage else {
            return;
        };
        let Some(set) = own.replica_set() else {
            return;
        };
        let local = self.router.local_node_id();
        let generation = set.current.generation;
        let named = set
            .current
            .members
            .iter()
            .any(|member| member.node == local && member.storage == Some(own_id));
        if !named {
            return;
        }
        if learner
            && !(holds_start(own, generation).await
                && self.counters_caught_up(key, generation).await)
        {
            return;
        }
        let settled = self.groups_caught_up(key, generation).await;
        if !learner && !settled {
            return;
        }
        match own.admit_member(local, settled).await {
            Ok(true) => tracing::info!(
                generation,
                settled,
                "re-admitted: caught up through the start of the set's generation",
            ),
            Ok(false) => {}
            Err(err) => tracing::warn!(error = %err, "could not record this copy's re-admission"),
        }
    }

    /// Whether a cache shard's counter log holds the start record of
    /// `generation`. A counter log that cannot be opened has not.
    async fn counters_caught_up(&self, key: &felix_router::ShardKey, generation: u64) -> bool {
        if key.kind != felix_router::ShardKind::Cache || self.broker.counters().is_none() {
            return true;
        }
        match self
            .broker
            .shard_log(
                felix_broker::LogKind::Counters,
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
            )
            .await
        {
            Some(counters) => holds_start(&counters, generation).await,
            None => false,
        }
    }

    /// Whether a stream shard's group logs each hold a record written at
    /// `generation` or later. A group log that cannot be opened has not.
    async fn groups_caught_up(&self, key: &felix_router::ShardKey, generation: u64) -> bool {
        if key.kind != felix_router::ShardKind::Stream || self.broker.group_reader().is_none() {
            return true;
        }
        for kind in [
            felix_broker::LogKind::GroupDeadLetters,
            felix_broker::LogKind::GroupCursors,
        ] {
            let Some(group) = self
                .broker
                .shard_log(kind, &key.tenant_id, &key.namespace, &key.stream, key.shard)
                .await
            else {
                return false;
            };
            if !holds_from(&group, generation).await {
                return false;
            }
        }
        true
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
        if self.is_learner(&log) {
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

/// The answer while the `joint_fence` gate is unread and the request needs
/// it: retryable, and nothing is written.
fn undecided(correlation_id: u64, key: &felix_router::ShardKey) -> InternalMessage {
    tracing::debug!(
        stream = %key.stream,
        shard = key.shard,
        "refusing until this broker has registered and knows whether joint_fence is enabled",
    );
    metrics::record_replicated(metrics::OUTCOME_REFUSED);
    refused(
        correlation_id,
        ErrorCode::Unavailable,
        0,
        "this broker has not yet learned the fleet's features".to_string(),
    )
}

/// Whether `log` holds a record written at `generation` or later.
async fn holds_from(log: &felix_broker::StreamLog, generation: u64) -> bool {
    let Ok(tail) = log.tail_offset().await else {
        return false;
    };
    log.generations()
        .iter()
        .any(|epoch| epoch.generation >= generation && epoch.start_offset < tail)
}

/// Whether `log` holds the generation-start record of `generation`.
async fn holds_start(log: &felix_broker::StreamLog, generation: u64) -> bool {
    let Ok(tail) = log.tail_offset().await else {
        return false;
    };
    log.generations()
        .iter()
        .any(|epoch| epoch.generation == generation && epoch.start_offset < tail)
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
