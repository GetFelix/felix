//! The stream shards a Raft group runs on this broker.
//!
//! [`RaftShards`] takes every durable stream shard out of the feed's
//! assignments before the hybrid sees them and keeps this broker's Raft
//! members open: a member per shard this broker is a replica of, on the
//! shard's own directory. Placement decides where a group forms. Once it
//! has, its members are the group's own, and placement changes none of them.
//! See `docs/per-shard-raft.md`, "Forming a group".

use std::collections::BTreeSet;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::time::Duration;

use anyhow::Context;
use felix_broker::{Broker, StreamLog};
use felix_common::random::{HashMap, HashSet};
use felix_replication::driver::WriteFence;
use felix_replication::quorum::ShardServing;
use felix_replication::raft::{GroupId, GroupRuntime, MemberId, RaftEngine};
use felix_storage::DiskLog;
use felix_storage::log::{FsyncMode, LogConfig};

use super::routing::{Dispatch, EngineShards, Reason, Taken};
use super::watch::ShardAssignment;
use super::{ShardKey, ShardKind};

/// How long a leader that could not move its stream to the committed tail
/// waits before it tries again.
const LEAD_RETRY: Duration = Duration::from_millis(50);

/// How long the broker that forms a group waits for each other member to
/// say its log is empty. One that does not is asked again on the next pass.
const ASK_WITHIN: Duration = Duration::from_secs(5);

/// Raft shards open here whose group's voters are not the brokers placement
/// names for the shard.
pub(crate) const OFF_PLACEMENT: &str = "felix_broker_raft_shards_off_placement";

/// This broker's Raft members of stream shards.
pub(crate) struct RaftShards {
    me: Weak<Self>,
    broker: Arc<Broker>,
    runtime: GroupRuntime,
    engine: RaftEngine,
    local: String,
    leading: Arc<Leading>,
    /// Every Raft shard in the feed and the broker its placement names as
    /// leader: where a request goes while no member is open here.
    placed: parking_lot::Mutex<HashMap<ShardKey, String>>,
    /// Shards whose other members are being asked whether a group reached
    /// them.
    forming: parking_lot::Mutex<HashSet<ShardKey>>,
    /// Shards whose forming was put off, so the reason is logged once, not
    /// on every pass.
    put_off: parking_lot::Mutex<HashSet<ShardKey>>,
    /// Shards whose marker could not be read, logged once while it stays so.
    unreadable: parking_lot::Mutex<HashSet<ShardKey>>,
    /// Shards not placed here whose directory was found unmarked, by
    /// incarnation: only this module marks one, so it is read once.
    unclaimed: parking_lot::Mutex<HashMap<ShardKey, u64>>,
    /// Shards already reported as off their placement, so each is logged
    /// once per time it happens.
    off_placement: parking_lot::Mutex<HashSet<ShardKey>>,
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
            ending: Arc::default(),
            #[cfg(test)]
            stall: tokio::sync::watch::Sender::new(false),
            #[cfg(test)]
            stall_ends: tokio::sync::watch::Sender::new(false),
        });
        runtime.report_to(Arc::clone(&leading), Arc::clone(&leading));
        let shards = Arc::new_cyclic(|me| Self {
            me: me.clone(),
            engine: RaftEngine::new(runtime.clone()),
            broker,
            runtime,
            local,
            leading,
            placed: parking_lot::Mutex::default(),
            forming: parking_lot::Mutex::default(),
            put_off: parking_lot::Mutex::default(),
            unreadable: parking_lot::Mutex::default(),
            unclaimed: parking_lot::Mutex::default(),
            off_placement: parking_lot::Mutex::default(),
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
        self.unclaimed.lock().remove(key);
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
        match self.start_member(key, incarnation, &member.log).await {
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

    /// Opens the member and forms nothing: a member on an empty log waits
    /// for [`Self::form`] here or for a leader's first append.
    ///
    /// The group's epoch is the stream's incarnation, so a member of an
    /// earlier incarnation refuses this one's messages. Its pre-vote answer
    /// would otherwise say blank for a directory the new group never touched.
    async fn start_member(
        &self,
        key: &ShardKey,
        incarnation: u64,
        log: &DiskLog,
    ) -> anyhow::Result<Option<tokio::task::JoinHandle<()>>> {
        self.runtime
            .open(GroupId::new(key.clone(), incarnation), 0, log.clone())
            .await
            .context("open the Raft member")?;
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

    /// Whether an earlier run of this broker opened a member of `key`. Such
    /// a member is opened again whatever placement says now: it may be a
    /// voter its group counts on.
    ///
    /// Only an unmarked directory is remembered. A failed read, or a stream
    /// not yet at `incarnation` here, is read again on the next pass: taken
    /// for "never opened" for good, it would leave a voter closed.
    async fn claimed(&self, key: &ShardKey, incarnation: u64) -> bool {
        if self.unclaimed.lock().get(key) == Some(&incarnation) {
            return false;
        }
        let (broker, shard) = (Arc::clone(&self.broker), key.clone());
        let read = tokio::task::spawn_blocking(move || -> anyhow::Result<Option<bool>> {
            let Some(storage) = broker.durable_storage() else {
                return Ok(Some(false));
            };
            Ok(storage.claimed_for_raft(
                &shard.tenant_id,
                &shard.namespace,
                &shard.stream,
                shard.shard,
                incarnation,
            )?)
        })
        .await
        .context("read task")
        .and_then(|read| read);
        if read.is_ok() {
            self.unreadable.lock().remove(key);
        }
        match read {
            Ok(Some(true)) => true,
            Ok(Some(false)) => {
                self.unclaimed.lock().insert(key.clone(), incarnation);
                false
            }
            Ok(None) => false,
            Err(err) if !self.unreadable.lock().insert(key.clone()) => {
                tracing::debug!(shard = ?key, error = %format!("{err:#}"), "could not read whether this broker held a Raft member of the shard");
                false
            }
            Err(err) => {
                tracing::warn!(shard = ?key, error = %format!("{err:#}"), "could not read whether this broker held a Raft member of the shard");
                false
            }
        }
    }

    /// Form `key`'s group from `members`, off the pass: a member that is
    /// down must not slow the feed. Only the broker with the lowest id in
    /// the set gets here, so a set is formed by one broker or by none, and
    /// only once every other member has said no group reached it. The rest
    /// join when the leader's first append arrives.
    fn form(&self, key: ShardKey, members: BTreeSet<MemberId>, incarnation: u64) {
        let Some(me) = self.me.upgrade() else {
            return;
        };
        if !self.forming.lock().insert(key.clone()) {
            return;
        }
        tokio::spawn(async move {
            match me.runtime.peers_blank(&key, &members, ASK_WITHIN).await {
                Ok(()) => {
                    // Under the pass's lock, so the member formed is the one
                    // the others were asked about.
                    me.put_off.lock().remove(&key);
                    let open = me.open.lock().await;
                    if open
                        .get(&key)
                        .is_some_and(|member| member.incarnation == incarnation)
                        && let Err(err) = me.runtime.initialize(&key, members).await
                    {
                        tracing::warn!(shard = ?key, error = %err, "could not form a Raft group");
                    }
                }
                Err(err) if me.put_off.lock().insert(key.clone()) => {
                    tracing::info!(shard = ?key, reason = %err, "not forming the shard's Raft group yet");
                }
                Err(err) => {
                    tracing::debug!(shard = ?key, reason = %err, "not forming the shard's Raft group yet");
                }
            }
            me.forming.lock().remove(&key);
        });
    }

    /// Whether `key`'s group here has voters other than the brokers
    /// placement names. Placement cannot change a formed group yet, so the
    /// group keeps the members it formed with and this says so.
    fn off_placement(&self, key: &ShardKey, placed: &BTreeSet<&str>) -> bool {
        let voters = self.runtime.voters(key).unwrap_or_default();
        let differs = !voters.is_empty()
            && voters
                .iter()
                .map(|voter| &*voter.broker)
                .ne(placed.iter().copied());
        if !differs {
            self.off_placement.lock().remove(key);
        } else if self.off_placement.lock().insert(key.clone()) {
            let voters: Vec<&str> = voters.iter().map(|voter| &*voter.broker).collect();
            tracing::warn!(
                shard = ?key,
                ?voters,
                ?placed,
                "a Raft shard's placement differs from its group's members; the group keeps \
                 its members, because moves, drains and replacement are not supported on \
                 Raft shards yet"
            );
        }
        differs
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

    /// The route for a Raft shard with no member open here. Its placement's
    /// leader holds a member, and answers with the group's leader when that
    /// is another broker. `None` for a shard the feed has not placed on Raft.
    fn dispatch_without_member(
        &self,
        key: &ShardKey,
        nodes: &dyn Fn(&str) -> Option<SocketAddr>,
    ) -> Option<Dispatch> {
        let leader = self.placed.lock().get(key).cloned()?;
        // Placed here: the member is still opening, or its open failed and
        // the next feed pass tries again.
        if leader.is_empty() || leader == self.local {
            return Some(Dispatch::Unavailable(Reason::NotReady));
        }
        Some(match nodes(&leader) {
            Some(advertise_addr) => Dispatch::Forward {
                node_id: leader,
                advertise_addr,
                generation: 0,
                incarnation: 0,
            },
            None => Dispatch::Unavailable(Reason::OwnerUnavailable(leader)),
        })
    }

    #[cfg(test)]
    pub(crate) fn stall_leading(&self, stalled: bool) {
        self.leading.stall.send_replace(stalled);
    }

    #[cfg(test)]
    pub(crate) fn stall_ends(&self, stalled: bool) {
        self.leading.stall_ends.send_replace(stalled);
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

/// The brokers placement names for a shard, lowest id first.
fn brokers_of(assignment: &ShardAssignment) -> BTreeSet<&str> {
    std::iter::once(&assignment.leader)
        .chain(&assignment.replicas)
        .map(String::as_str)
        .filter(|broker| !broker.is_empty())
        .collect()
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
    ) -> Pin<Box<dyn Future<Output = Taken> + Send + 'a>> {
        Box::pin(async move {
            let mut open = self.open.lock().await;
            let mut taken = Taken::new();
            let streams: Vec<ShardKey> = assignments
                .keys()
                .filter(|key| key.kind == ShardKind::Stream)
                .cloned()
                .collect();
            // Every Raft shard in the feed, and whether its placement names
            // as many brokers as the stream asks for.
            let mut fed: HashMap<ShardKey, (ShardAssignment, bool)> = HashMap::default();
            let mut placed = HashMap::default();
            for key in streams {
                let metadata = self
                    .broker
                    .stream_metadata(&key.tenant_id, &key.namespace, &key.stream)
                    .await;
                let wanted = match metadata {
                    // A stream in memory has no log for a group to run on.
                    Some(metadata) if !metadata.durable => continue,
                    Some(metadata) => metadata.replication_factor,
                    // Not registered here yet, so not known to be durable.
                    // Neither engine opens it this pass: a log the hybrid
                    // opened now would sit beside the member's later.
                    None => {
                        tracing::debug!(shard = ?key, "assigned stream is not registered yet");
                        assignments.remove(&key);
                        continue;
                    }
                };
                let Some(assignment) = assignments.remove(&key) else {
                    continue;
                };
                taken.push((key.clone(), assignment.routing));
                placed.insert(key.clone(), assignment.leader.clone());
                let full = wanted
                    .is_some_and(|wanted| brokers_of(&assignment).len() == wanted.max(1) as usize);
                fed.insert(key, (assignment, full));
            }
            *self.placed.lock() = placed;
            self.unclaimed.lock().retain(|key, _| fed.contains_key(key));
            self.put_off.lock().retain(|key| fed.contains_key(key));
            self.unreadable.lock().retain(|key| fed.contains_key(key));
            self.off_placement
                .lock()
                .retain(|key| fed.contains_key(key));
            // A member whose placement dropped this broker stays open: it
            // is still a voter of its group. Only a stream that is gone, or
            // made again, closes one.
            let gone: Vec<ShardKey> = open
                .iter()
                .filter(|(key, member)| {
                    fed.get(*key)
                        .is_none_or(|(assignment, _)| assignment.incarnation != member.incarnation)
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
            let mut off_placement = 0_u32;
            for (key, (assignment, full)) in &fed {
                let brokers = brokers_of(assignment);
                if !open.contains_key(key) {
                    if !brokers.contains(self.local.as_str())
                        && !self.claimed(key, assignment.incarnation).await
                    {
                        continue;
                    }
                    match self.open_member(key, assignment).await {
                        Ok(member) => {
                            open.insert(key.clone(), member);
                        }
                        // Tried again on the next pass.
                        Err(err) => {
                            tracing::warn!(shard = ?key, error = %format!("{err:#}"), "could not open a Raft shard");
                            continue;
                        }
                    }
                }
                if self.off_placement(key, &brokers) {
                    off_placement += 1;
                }
                // A set short of the stream's replica count forms nothing: a
                // group of the brokers placed so far would be formed again,
                // on empty logs, by the ones placed later.
                let blank = self
                    .runtime
                    .status(key)
                    .is_some_and(|status| status.term == 0 && status.last_index.is_none());
                if *full && blank && brokers.first() == Some(&self.local.as_str()) {
                    let members = brokers
                        .iter()
                        .map(|broker| MemberId::new(*broker, 0))
                        .collect();
                    self.form(key.clone(), members, assignment.incarnation);
                }
            }
            metrics::gauge!(OFF_PLACEMENT).set(f64::from(off_placement));
            taken
        })
    }

    fn dispatch(
        &self,
        key: &ShardKey,
        nodes: &dyn Fn(&str) -> Option<SocketAddr>,
    ) -> Option<Dispatch> {
        let Some(status) = self.runtime.status(key) else {
            return self.dispatch_without_member(key, nodes);
        };
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
            self.placed.lock().clear();
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
    /// Deposed terms per shard whose subscribers are still to be ended. No
    /// later term is served while one is left: its subscribers would be
    /// ended with the deposed term's.
    ending: Arc<parking_lot::Mutex<HashMap<ShardKey, usize>>>,
    /// While true, a new term's move to the committed tail does not start.
    #[cfg(test)]
    stall: tokio::sync::watch::Sender<bool>,
    /// While true, a deposed term's subscribers are not ended.
    #[cfg(test)]
    stall_ends: tokio::sync::watch::Sender<bool>,
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
        let (broker, terms, ending, key) = (
            Arc::clone(&self.broker),
            Arc::clone(&self.terms),
            Arc::clone(&self.ending),
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
                    // `Ok(false)` moved nothing: the stream is not open on
                    // the member's log yet, or the group cannot say what is
                    // committed.
                    if moved.is_ok_and(|at_tail| at_tail) && !ending.lock().contains_key(&key) {
                        lead.open = true;
                        return;
                    }
                }
                tokio::time::sleep(LEAD_RETRY).await;
            }
        });
    }

    fn deposed(&self, key: &ShardKey, term: u64) {
        {
            let mut terms = self.terms.lock();
            if !terms.get(key).is_some_and(|lead| lead.term <= term) {
                return;
            }
            terms.remove(key);
            // Counted before a later term can be reported, so that term
            // waits for this one's subscribers to be ended.
            *self.ending.lock().entry(key.clone()).or_default() += 1;
        }
        // Live fanout comes from the leader's publishes, so a subscriber
        // left here would wait on a broker that gets none. Ended after the
        // term is gone: a reader registering from now on sees no local
        // route and is refused.
        let (broker, ending, key) = (
            Arc::clone(&self.broker),
            Arc::clone(&self.ending),
            key.clone(),
        );
        #[cfg(test)]
        let mut stall = self.stall_ends.subscribe();
        tokio::spawn(async move {
            #[cfg(test)]
            let _ = stall.wait_for(|stalled| !stalled).await;
            broker
                .end_subscriptions(&key.tenant_id, &key.namespace, &key.stream, key.shard, None)
                .await;
            let mut ending = ending.lock();
            if let Some(left) = ending.get_mut(&key) {
                *left -= 1;
                if *left == 0 {
                    ending.remove(&key);
                }
            }
        });
    }
}

#[cfg(test)]
pub(crate) mod tests;
