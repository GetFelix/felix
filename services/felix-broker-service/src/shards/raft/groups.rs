//! Consumer groups on this broker's Raft stream shards: the
//! [`GroupConsensus`] the broker's [`RaftGroups`] reader runs over.

use std::sync::Arc;
use std::time::Instant;

use felix_broker::{
    BrokerError, FreshGroups, GroupConsensus, GroupEntry, GroupKey, NotReadable, Proposed,
    RaftGroups,
};
use felix_common::random::HashMap;
use felix_replication::engine::{CommitError, Written};
use felix_replication::raft::{GroupWriteError, RaftEngine};

use super::Lead;
use crate::shards::{ShardKey, ShardKind};

/// The consumer-group reader of this broker's Raft stream shards.
pub(crate) type Groups = RaftGroups<OnRaft>;

/// A shard's groups as its Raft group decides them.
pub(crate) struct OnRaft {
    engine: RaftEngine,
    /// The terms this broker serves, as `Leading` keeps them.
    terms: Arc<parking_lot::Mutex<HashMap<ShardKey, Lead>>>,
}

impl OnRaft {
    pub(super) fn new(
        engine: RaftEngine,
        terms: Arc<parking_lot::Mutex<HashMap<ShardKey, Lead>>>,
    ) -> Self {
        Self { engine, terms }
    }

    /// The term this broker serves `shard` in: led, and its stream at the
    /// committed tail.
    fn serving(&self, shard: &ShardKey) -> Result<u64, BrokerError> {
        self.terms
            .lock()
            .get(shard)
            .filter(|lead| lead.open)
            .map(|lead| lead.term)
            .ok_or(BrokerError::StateNotReadable(NotReadable::Refused))
    }
}

impl GroupConsensus for OnRaft {
    async fn fresh(&self, key: &GroupKey, deadline: Instant) -> felix_broker::Result<FreshGroups> {
        let shard = shard_of(key);
        let term = self.serving(&shard)?;
        // A read-index round, then this member's apply up to it.
        within(deadline, self.engine.confirm_read(&shard, term)).await?;
        let groups = self
            .engine
            .groups(&shard)
            .ok_or(BrokerError::StateNotReadable(NotReadable::Refused))?;
        Ok(FreshGroups { term, groups })
    }

    async fn propose(
        &self,
        key: &GroupKey,
        term: u64,
        entry: &GroupEntry,
    ) -> felix_broker::Result<Proposed> {
        // Refused by openraft unless this member leads `term`; an append
        // that may have happened closes the term before the error returns.
        let written = self
            .engine
            .propose_group(&shard_of(key), term, now_micros(), entry)
            .await
            .map_err(proposal_error)?;
        Ok(Proposed {
            term: written.generation,
            index: written.end - 1,
        })
    }

    async fn applied(
        &self,
        key: &GroupKey,
        proposed: Proposed,
        deadline: Instant,
    ) -> felix_broker::Result<()> {
        let written = Written {
            end: proposed.index + 1,
            generation: proposed.term,
        };
        let timeout = deadline.saturating_duration_since(Instant::now());
        // Returns once applied: the machine folds a group entry before the
        // commit table moves past it.
        self.engine
            .await_commit(&shard_of(key), written, timeout)
            .await
            .map_err(broker_error)
    }
}

/// `work`, given up at `deadline`.
async fn within(
    deadline: Instant,
    work: impl Future<Output = Result<(), CommitError>>,
) -> felix_broker::Result<()> {
    match tokio::time::timeout_at(deadline.into(), work).await {
        Ok(done) => done.map_err(broker_error),
        Err(_) => Err(BrokerError::ProposalsStalled {
            waited: std::time::Duration::ZERO,
        }),
    }
}

fn broker_error(err: CommitError) -> BrokerError {
    match err {
        CommitError::TimedOut { timeout, .. } => BrokerError::ProposalsStalled { waited: timeout },
        _ => BrokerError::StateNotReadable(NotReadable::Refused),
    }
}

/// A proposal's error: retryable only when the entry was provably not
/// appended. Otherwise it may commit, and a retry could apply a redrive,
/// a delete or a claim twice.
fn proposal_error(err: GroupWriteError) -> BrokerError {
    if !err.may_commit {
        return broker_error(err.error);
    }
    BrokerError::WriteUnsettled {
        timed_out: matches!(err.error, CommitError::TimedOut { .. }),
        detail: err.error.to_string(),
    }
}

fn shard_of(key: &GroupKey) -> ShardKey {
    ShardKey {
        tenant_id: key.tenant_id.clone(),
        namespace: key.namespace.clone(),
        stream: key.stream.clone(),
        shard: key.shard,
        kind: ShardKind::Stream,
    }
}

/// The shard of `shard` as a group key, for [`RaftGroups::step_down`].
pub(super) fn any_group_of(shard: &ShardKey) -> GroupKey {
    GroupKey {
        tenant_id: shard.tenant_id.clone(),
        namespace: shard.namespace.clone(),
        stream: shard.stream.clone(),
        shard: shard.shard,
        group: String::new(),
    }
}

fn now_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_micros() as u64)
}

#[cfg(test)]
mod tests;
