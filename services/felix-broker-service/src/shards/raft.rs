//! The stream shards a Raft group runs on this broker.
//!
//! [`RaftShards`] takes every durable stream shard out of the feed's
//! assignments before the hybrid sees them and keeps this broker's Raft
//! members in line with them: a member per shard this broker is a replica
//! of, on the shard's own directory. See `docs/per-shard-raft.md`.

use std::collections::BTreeSet;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use felix_broker::{Broker, StreamLog};
use felix_common::random::HashMap;
use felix_replication::driver::WriteFence;
use felix_replication::quorum::ShardServing;
use felix_replication::raft::{GroupId, GroupRuntime, MemberId, RaftEngine};
use felix_storage::DiskLog;
use felix_storage::log::{FsyncMode, LogConfig};

use super::routing::{Dispatch, EngineShards, Reason};
use super::watch::ShardAssignment;
use super::{ShardKey, ShardKind};

/// How long a leader that could not move its stream to the committed tail
/// waits before it tries again.
const LEAD_RETRY: Duration = Duration::from_millis(50);

/// This broker's Raft members of stream shards.
pub(crate) struct RaftShards {
    broker: Arc<Broker>,
    runtime: GroupRuntime,
    engine: RaftEngine,
    local: String,
    leading: Arc<Leading>,
    /// Held across a whole pass and a whole close, so a stream's delete
    /// never runs between a member's claim of its directory and its open.
    open: tokio::sync::Mutex<HashMap<ShardKey, Member>>,
}

/// One open member: the log is this module's to close, because the storage
/// provider never opened it.
struct Member {
    incarnation: u64,
    log: DiskLog,
    follow: Option<tokio::task::JoinHandle<()>>,
}

impl RaftShards {
    /// `runtime` must have no group open yet: it reports each shard's
    /// leadership here from this call on.
    pub(crate) fn new(broker: Arc<Broker>, runtime: GroupRuntime, local: String) -> Arc<Self> {
        let leading = Arc::new(Leading {
            broker: Arc::clone(&broker),
            terms: Arc::default(),
            #[cfg(test)]
            stall: tokio::sync::watch::Sender::new(false),
        });
        runtime.report_to(Arc::clone(&leading), Arc::clone(&leading));
        let shards = Arc::new(Self {
            engine: RaftEngine::new(runtime.clone()),
            broker,
            runtime,
            local,
            leading,
            open: tokio::sync::Mutex::default(),
        });
        shards
            .broker
            .set_engine_logs(Arc::clone(&shards) as Arc<dyn felix_broker::EngineLogs>);
        shards
    }

    /// The Raft engine, when this broker holds a member of `shard`.
    pub(crate) fn engine_for(&self, shard: &ShardKey) -> Option<&RaftEngine> {
        self.runtime.status(shard).map(|_| &self.engine)
    }

    /// The term this broker leads `shard` in with its stream brought up to
    /// the committed tail, or `None` while it may not take writes.
    pub(crate) fn leading(&self, shard: &ShardKey) -> Option<u64> {
        self.leading.generation(shard)
    }

    async fn open_member(
        &self,
        key: &ShardKey,
        assignment: &ShardAssignment,
    ) -> anyhow::Result<Member> {
        let broker = Arc::clone(&self.broker);
        let (shard, incarnation) = (key.clone(), assignment.incarnation);
        // The marker write fsyncs and the open recovers the log.
        let log = tokio::task::spawn_blocking(move || -> anyhow::Result<DiskLog> {
            let storage = broker
                .durable_storage()
                .context("a Raft shard needs durable storage")?;
            let (dir, label) = storage.claim_for_raft(
                &shard.tenant_id,
                &shard.namespace,
                &shard.stream,
                shard.shard,
                incarnation,
            )?;
            let config = LogConfig {
                raft_log: true,
                fsync_mode: FsyncMode::OnCommit,
                ..storage.config().clone()
            };
            Ok(DiskLog::open(dir, label, config)?)
        })
        .await
        .context("open task")??;
        let mut member = Member {
            incarnation,
            log,
            follow: None,
        };
        match self.start_member(key, assignment, &member.log).await {
            Ok(follow) => {
                member.follow = follow;
                Ok(member)
            }
            Err(err) => {
                self.close_member(key, member).await;
                Err(err)
            }
        }
    }

    async fn start_member(
        &self,
        key: &ShardKey,
        assignment: &ShardAssignment,
        log: &DiskLog,
    ) -> anyhow::Result<Option<tokio::task::JoinHandle<()>>> {
        self.runtime
            .open(GroupId::new(key.clone(), 0), 0, log.clone())
            .await
            .context("open the Raft member")?;
        let members: BTreeSet<MemberId> = assignment
            .replicas
            .iter()
            .chain(std::iter::once(&assignment.leader))
            .map(|broker| MemberId::new(broker.as_str(), 0))
            .collect();
        self.runtime
            .initialize(key, members)
            .await
            .context("form the Raft group")?;
        let handle = self
            .broker
            .open_stream_on_engine_log(
                &key.tenant_id,
                &key.namespace,
                &key.stream,
                key.shard,
                StreamLog::from_log(log.clone()),
            )
            .await
            .context("open the stream on the member's log")?;
        Ok(self.engine.follow_commits(key, handle))
    }

    /// Stop the member, then close its log: the member writes to the log
    /// until it has stopped.
    async fn close_member(&self, key: &ShardKey, member: Member) {
        self.runtime.close(key).await;
        self.leading.forget(key);
        if let Some(follow) = member.follow {
            follow.abort();
        }
        if let Err(err) = member.log.close().await {
            tracing::warn!(shard = ?key, error = %err, "could not close a Raft member's log");
        }
    }

    #[cfg(test)]
    pub(crate) fn stall_leading(&self, stalled: bool) {
        self.leading.stall.send_replace(stalled);
    }

    #[cfg(test)]
    pub(crate) async fn log_of(&self, shard: &ShardKey) -> Option<DiskLog> {
        self.open
            .lock()
            .await
            .get(shard)
            .map(|member| member.log.clone())
    }
}

impl std::fmt::Debug for RaftShards {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftShards")
            .field("local", &self.local)
            .finish_non_exhaustive()
    }
}

impl EngineShards for RaftShards {
    fn take<'a>(
        &'a self,
        assignments: &'a mut HashMap<ShardKey, ShardAssignment>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let mut open = self.open.lock().await;
            let streams: Vec<ShardKey> = assignments
                .keys()
                .filter(|key| key.kind == ShardKind::Stream)
                .cloned()
                .collect();
            let mut held = HashMap::default();
            for key in streams {
                let metadata = self
                    .broker
                    .stream_metadata(&key.tenant_id, &key.namespace, &key.stream)
                    .await;
                match metadata {
                    // A stream in memory has no log for a group to run on.
                    Some(metadata) if !metadata.durable => continue,
                    Some(_) => {}
                    // Not registered here yet, so not known to be durable.
                    // Neither engine opens it this pass: a log the hybrid
                    // opened now would sit beside the member's later.
                    None => {
                        tracing::debug!(shard = ?key, "assigned stream is not registered yet");
                        assignments.remove(&key);
                        continue;
                    }
                }
                let Some(assignment) = assignments.remove(&key) else {
                    continue;
                };
                if assignment.leader == self.local || assignment.replicas.contains(&self.local) {
                    held.insert(key, assignment);
                }
            }
            let gone: Vec<ShardKey> = open
                .iter()
                .filter(|(key, member)| {
                    held.get(*key)
                        .is_none_or(|assignment| assignment.incarnation != member.incarnation)
                })
                .map(|(key, _)| key.clone())
                .collect();
            for key in gone {
                let Some(member) = open.remove(&key) else {
                    continue;
                };
                self.close_member(&key, member).await;
                // Ends the readers; a later open starts from the log again.
                if let Err(err) = self
                    .broker
                    .close_stream_shard(
                        &key.tenant_id,
                        &key.namespace,
                        &key.stream,
                        key.shard,
                        None,
                    )
                    .await
                {
                    tracing::warn!(shard = ?key, error = %err, "could not close a Raft shard's stream");
                }
            }
            for (key, assignment) in held {
                if open.contains_key(&key) {
                    continue;
                }
                match self.open_member(&key, &assignment).await {
                    Ok(member) => {
                        open.insert(key, member);
                    }
                    // Tried again on the next pass.
                    Err(err) => {
                        tracing::warn!(shard = ?key, error = %format!("{err:#}"), "could not open a Raft shard");
                    }
                }
            }
        })
    }

    fn dispatch(
        &self,
        key: &ShardKey,
        nodes: &dyn Fn(&str) -> Option<SocketAddr>,
    ) -> Option<Dispatch> {
        let status = self.runtime.status(key)?;
        // Led is not enough: the term is served once `Leading` opened it.
        if let Some(term) = self.leading(key) {
            return Some(Dispatch::Local { generation: term });
        }
        let leader = status
            .leader
            .filter(|leader| *leader.broker != *self.local)
            .map(|leader| leader.broker);
        Some(match leader {
            // Electing, or elected here and still moving to the tail.
            None => Dispatch::Unavailable(Reason::NotReady),
            Some(leader) => match nodes(&leader) {
                Some(advertise_addr) => Dispatch::Forward {
                    node_id: leader.to_string(),
                    advertise_addr,
                    generation: status.term,
                    incarnation: 0,
                },
                None => Dispatch::Unavailable(Reason::OwnerUnavailable(leader.to_string())),
            },
        })
    }

    fn shutdown(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let mut open = self.open.lock().await;
            for (key, member) in open.drain() {
                self.close_member(&key, member).await;
            }
            self.runtime.shutdown().await;
        })
    }
}

impl felix_broker::EngineLogs for RaftShards {
    fn close_through<'a>(
        &'a self,
        tenant_id: &'a str,
        namespace: &'a str,
        stream: &'a str,
        through: u64,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let mut open = self.open.lock().await;
            let closing: Vec<ShardKey> = open
                .iter()
                .filter(|(key, member)| {
                    key.tenant_id == tenant_id
                        && key.namespace == namespace
                        && key.stream == stream
                        && member.incarnation <= through
                })
                .map(|(key, _)| key.clone())
                .collect();
            for key in closing {
                if let Some(member) = open.remove(&key) {
                    self.close_member(&key, member).await;
                }
            }
        })
    }
}

/// The shards this broker leads, as its Raft members report them.
struct Leading {
    broker: Arc<Broker>,
    terms: Arc<parking_lot::Mutex<HashMap<ShardKey, Lead>>>,
    /// While true, a new term's move to the committed tail does not start.
    #[cfg(test)]
    stall: tokio::sync::watch::Sender<bool>,
}

struct Lead {
    term: u64,
    /// The stream is at the committed tail, so a write of this term may be
    /// handed over.
    open: bool,
}

impl Leading {
    fn forget(&self, key: &ShardKey) {
        self.terms.lock().remove(key);
    }
}

impl ShardServing for Leading {
    fn replicated(&self, _key: &ShardKey) -> bool {
        true
    }

    fn generation(&self, key: &ShardKey) -> Option<u64> {
        self.terms
            .lock()
            .get(key)
            .filter(|lead| lead.open)
            .map(|lead| lead.term)
    }

    /// A Raft leader's hold on a shard is its term, not this broker's lease.
    fn lease_valid(&self) -> bool {
        true
    }

    fn record_ack_refusal(&self) {}
}

impl WriteFence for Leading {
    fn quiesced(&self, key: &ShardKey) -> bool {
        !self.terms.lock().contains_key(key)
    }

    fn opened(&self, key: &ShardKey, term: u64) {
        {
            let mut terms = self.terms.lock();
            if terms.get(key).is_some_and(|lead| lead.term >= term) {
                return;
            }
            terms.insert(key.clone(), Lead { term, open: false });
        }
        let (broker, terms, key) = (
            Arc::clone(&self.broker),
            Arc::clone(&self.terms),
            key.clone(),
        );
        // The term is served only once the stream is at the committed tail:
        // a batch committed below it and delivered after it would reach a
        // resuming reader twice.
        #[cfg(test)]
        let mut stall = self.stall.subscribe();
        tokio::spawn(async move {
            #[cfg(test)]
            let _ = stall.wait_for(|stalled| !stalled).await;
            loop {
                let moved = broker
                    .lead_from_tail(&key.tenant_id, &key.namespace, &key.stream, key.shard)
                    .await;
                {
                    let mut terms = terms.lock();
                    let Some(lead) = terms.get_mut(&key).filter(|lead| lead.term == term) else {
                        return;
                    };
                    if moved.is_ok() {
                        lead.open = true;
                        return;
                    }
                }
                tokio::time::sleep(LEAD_RETRY).await;
            }
        });
    }

    fn deposed(&self, key: &ShardKey, term: u64) {
        let mut terms = self.terms.lock();
        if terms.get(key).is_some_and(|lead| lead.term <= term) {
            terms.remove(key);
        }
    }
}

#[cfg(test)]
mod tests;
