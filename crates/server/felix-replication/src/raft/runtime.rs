//! The groups one broker holds, and their lifecycle.

use std::collections::{BTreeSet, HashMap};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use arc_swap::ArcSwap;
use felix_storage::DiskLog;
use felix_storage::raft_log::{LogPoint, LogPositions};
use openraft::impls::leader_id_std::LeaderId;
use openraft::raft::{VoteRequest, VoteResponse};
use openraft::{Config, SnapshotPolicy, Vote};
use tokio_util::sync::CancellationToken;

use super::group::{Group, Timers};
use super::network::{Addressee, RaftReply, RaftRequest, RaftTransport, Refused, Reply, Request};
use super::ticker;
use super::types::{GroupId, MemberId, ShardTypes};
use crate::ShardKey;
use crate::driver::WriteFence;
use crate::engine::{EngineHost, Host};
use crate::quorum::ShardServing;

/// The timers every group on a broker shares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConfig {
    /// How often the ticker runs each group's timer check. A timeout is
    /// noticed at the next check, so this adds to the two below.
    pub tick: Duration,
    /// How often a leader with nothing to send sends a heartbeat. One goes
    /// out at the first timer check after this has passed, so with the
    /// default tick a busy leader sends one every 250 ms.
    pub heartbeat: Duration,
    /// How often it sends one once the group is idle: no entry sent for
    /// [`Self::idle_after`]. A follower of an idle leader waits three of
    /// these before it may stand, so a dead leader of an idle group is
    /// noticed that much later. Equal to `heartbeat`, groups never slow
    /// down.
    pub idle_heartbeat: Duration,
    pub idle_after: Duration,
    /// A follower that has heard no leader for a time picked between these
    /// two starts an election.
    pub election_min: Duration,
    pub election_max: Duration,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            tick: Duration::from_millis(50),
            heartbeat: Duration::from_millis(200),
            idle_heartbeat: Duration::from_secs(2),
            idle_after: Duration::from_secs(5),
            election_min: Duration::from_secs(1),
            election_max: Duration::from_secs(2),
        }
    }
}

impl RuntimeConfig {
    pub(crate) fn raft(&self) -> Result<Config, InvalidTimers> {
        if self.tick.is_zero() {
            return Err(InvalidTimers("the tick interval is zero".into()));
        }
        if self.idle_heartbeat < self.heartbeat {
            return Err(InvalidTimers(
                "the idle heartbeat is shorter than the heartbeat".into(),
            ));
        }
        let millis = |duration: Duration| duration.as_millis() as u64;
        Config {
            cluster_name: "felix-shard".into(),
            heartbeat_interval: millis(self.heartbeat),
            election_timeout_min: millis(self.election_min),
            election_timeout_max: millis(self.election_max),
            // The node ticker runs the timer check. A tick task per group
            // costs most of a core per thousand idle members.
            enable_tick: false,
            // A shard's log is its data. Entries leave it by retention,
            // never because a snapshot was taken.
            snapshot_policy: SnapshotPolicy::Never,
            // A snapshot that is taken anyway purges nothing. The default
            // keeps only the last 1,000 entries under one.
            max_in_snapshot_log_to_keep: u64::MAX,
            // A voter that comes back with a shorter log stops its leader.
            // A copy that lost records rejoins under a new copy id instead.
            allow_log_reversion: Some(false),
            // openraft keeps six ring buffers of this many slots per member
            // for its `runtime-stats` feature, which is off: about 360 KB a
            // member at the default 1,024.
            log_stage_capacity: Some(1),
            ..Config::default()
        }
        .validate()
        .map_err(|err| InvalidTimers(err.to_string()))
    }
}

#[derive(Debug, thiserror::Error)]
#[error("the Raft timers are not usable: {0}")]
pub struct InvalidTimers(String);

#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    #[error("the runtime has shut down")]
    Closed,
    #[error("the shard already has a group here")]
    AlreadyOpen,
    /// A close of the shard's last member has not finished. Until it has,
    /// that member may still write to the log. Try again after the close
    /// returns.
    #[error("the shard's last group here is still stopping")]
    Closing,
    /// The log cannot be a Raft log, or its Raft state does not read back.
    #[error(transparent)]
    Log(#[from] io::Error),
    #[error("the group did not start: {0}")]
    Raft(String),
}

#[derive(Debug, thiserror::Error)]
pub enum InitializeError {
    #[error("the shard has no group here")]
    NoSuchGroup,
    /// A member forms a group only from a set that names it. A copy wiped
    /// since has a new id, so it cannot start the group again on an empty log.
    #[error("{0} is not in the first member set")]
    NotAMember(MemberId),
    #[error("the group was not formed: {0}")]
    Raft(String),
}

/// Why a member set was not found untouched by any group.
#[derive(Debug, thiserror::Error)]
pub enum NotBlank {
    #[error("the shard has no group here")]
    NoSuchGroup,
    #[error("{0} did not answer: {1}")]
    Unanswered(MemberId, String),
    #[error("{0} already holds a vote or an entry of a group")]
    Formed(MemberId),
}

/// What a member is doing in its group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Leader,
    Candidate,
    Follower,
    Learner,
    Stopped,
}

/// A member's view of its group, as of its last state change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupStatus {
    pub member: MemberId,
    pub role: Role,
    pub term: u64,
    /// The leader this member follows, itself included.
    pub leader: Option<MemberId>,
    pub last_index: Option<u64>,
    /// The last entry this member's log has written since it was opened,
    /// fsynced or not. A power cut can still take it.
    pub written: Option<LogPoint>,
    /// The last entry on disk. Votes, a follower's answer and the commit
    /// index count this one.
    pub flushed: Option<LogPoint>,
    /// The commit index as openraft last published it. It is published
    /// after the entries are handed on to be applied, so for a moment it
    /// can be below the read bound. Never read it as a limit on the bound.
    pub committed: Option<u64>,
    /// Timer checks the ticker sent this member.
    pub ticks: u64,
    /// Timer checks dropped because the group had not taken one by the next
    /// interval. It rises while a group is too busy to look at its timers.
    pub dropped_ticks: u64,
    /// Calls made to openraft for publishes. Each carries every batch that
    /// was queued when it was made, so under load this is well below the
    /// number of batches.
    pub client_writes: u64,
}

enum Slot {
    /// Reserved by an open that is still starting the member.
    Opening,
    Open(Arc<Group>),
    /// Held by a close until the member has stopped, so no second member
    /// starts on the log while the first can still write to it.
    Closing(Arc<Group>),
}

/// A shard's slot while its member starts. Dropped before the member is
/// placed, by a failed or abandoned open, it frees the slot.
struct Reserved<'a> {
    registry: &'a Registry,
    shard: Option<Arc<ShardKey>>,
}

impl Drop for Reserved<'_> {
    fn drop(&mut self) {
        if let Some(shard) = self.shard.take() {
            let mut slots = self.registry.slots.write();
            if matches!(slots.by_shard.get(&shard), Some(Slot::Opening)) {
                slots.by_shard.remove(&shard);
            }
        }
    }
}

pub(crate) struct Registry {
    slots: parking_lot::RwLock<Slots>,
    /// The open groups, as the ticker walks them. Replaced whole on every
    /// open and close, so a tick takes no lock.
    ticked: ArcSwap<Vec<Arc<Group>>>,
    /// Times the ticker ended on a panic and was started again.
    pub(crate) ticker_restarts: AtomicU64,
    /// Makes the ticker's next interval panic.
    #[cfg(test)]
    pub(crate) trip_ticker: std::sync::atomic::AtomicBool,
}

#[derive(Default)]
struct Slots {
    by_shard: HashMap<Arc<ShardKey>, Slot>,
    closed: bool,
}

impl Registry {
    pub(crate) fn ticked(&self) -> Arc<Vec<Arc<Group>>> {
        self.ticked.load_full()
    }

    fn publish(&self, slots: &Slots) {
        let open = slots
            .by_shard
            .values()
            .filter_map(|slot| match slot {
                Slot::Open(group) => Some(Arc::clone(group)),
                Slot::Opening | Slot::Closing(_) => None,
            })
            .collect();
        self.ticked.store(Arc::new(open));
    }

    fn get(&self, shard: &ShardKey) -> Option<Arc<Group>> {
        match self.slots.read().by_shard.get(shard) {
            Some(Slot::Open(group)) => Some(Arc::clone(group)),
            _ => None,
        }
    }
}

/// This broker's member of every Raft group it holds.
///
/// Cheap to clone; every clone is the same runtime.
#[derive(Clone)]
pub struct GroupRuntime {
    inner: Arc<Inner>,
}

struct Inner {
    broker: String,
    tick: Duration,
    timers: Timers,
    raft: Arc<Config>,
    transport: Arc<dyn RaftTransport>,
    registry: Arc<Registry>,
    /// Who is told when a shard opens and closes here.
    host: OnceLock<Arc<dyn EngineHost>>,
    stop: CancellationToken,
    ticker: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl GroupRuntime {
    /// A runtime for `broker` with its ticker running and no groups.
    pub fn start(
        broker: impl Into<String>,
        config: RuntimeConfig,
        transport: Arc<dyn RaftTransport>,
    ) -> Result<Self, InvalidTimers> {
        let runtime = Self::without_ticker(broker, config, transport)?;
        runtime.start_ticker();
        Ok(runtime)
    }

    /// The runtime with no ticker: its groups never notice a timeout.
    pub(crate) fn without_ticker(
        broker: impl Into<String>,
        config: RuntimeConfig,
        transport: Arc<dyn RaftTransport>,
    ) -> Result<Self, InvalidTimers> {
        Ok(Self {
            inner: Arc::new(Inner {
                broker: broker.into(),
                tick: config.tick,
                timers: Timers {
                    started: tokio::time::Instant::now(),
                    tick: config.tick.as_millis() as u64,
                    heartbeat: config.heartbeat.as_millis() as u64,
                    idle_heartbeat: config.idle_heartbeat.as_millis() as u64,
                    idle_after: config.idle_after.as_millis() as u64,
                    election_min: config.election_min.as_millis() as u64,
                    election_max: config.election_max.as_millis() as u64,
                },
                raft: Arc::new(config.raft()?),
                transport,
                registry: Arc::new(Registry {
                    slots: parking_lot::RwLock::default(),
                    ticked: ArcSwap::default(),
                    ticker_restarts: AtomicU64::new(0),
                    #[cfg(test)]
                    trip_ticker: std::sync::atomic::AtomicBool::new(false),
                }),
                host: OnceLock::new(),
                stop: CancellationToken::new(),
                ticker: parking_lot::Mutex::new(None),
            }),
        })
    }

    /// Tell `fence` when a shard opens for writes here and when it closes:
    /// `opened(shard, term)` once the blank entry of a term this broker
    /// leads is committed, `deposed(shard, term)` when it stops leading
    /// that term. See `docs/per-shard-raft.md`, "When a leader opens".
    ///
    /// Call it before the first [`Self::open`]: a group opened earlier
    /// reports to nobody. Only the first call counts.
    pub fn report_to<S, F>(&self, serving: Arc<S>, fence: Arc<F>)
    where
        S: ShardServing + ?Sized + 'static,
        F: WriteFence + ?Sized + 'static,
    {
        self.set_host(Arc::new(Host::new(serving, fence)));
    }

    pub(crate) fn set_host(&self, host: Arc<dyn EngineHost>) {
        let _ = self.inner.host.set(host);
    }

    pub(crate) fn start_ticker(&self) {
        let inner = &self.inner;
        let mut ticker = inner.ticker.lock();
        if ticker.is_none() && !inner.stop.is_cancelled() {
            *ticker = Some(tokio::spawn(ticker::supervise(
                Arc::clone(&inner.registry),
                inner.tick,
                inner.timers,
                inner.stop.clone(),
            )));
        }
    }

    /// Start this broker's member of `group` on `log`, as copy `copy`.
    ///
    /// `log` must be a Raft log that fsyncs on commit. The member resumes
    /// from what the log holds: its vote, its entries and its membership. A
    /// member on an empty log waits to be told its group's first members
    /// ([`Self::initialize`]) or to hear from a leader.
    pub async fn open(&self, group: GroupId, copy: u64, log: DiskLog) -> Result<(), OpenError> {
        let inner = &self.inner;
        let registry = &inner.registry;
        let shard = group.shard.clone();
        {
            let mut slots = registry.slots.write();
            if slots.closed {
                return Err(OpenError::Closed);
            }
            match slots.by_shard.get(&shard) {
                None => {}
                Some(Slot::Closing(_)) => return Err(OpenError::Closing),
                Some(Slot::Opening | Slot::Open(_)) => return Err(OpenError::AlreadyOpen),
            }
            slots.by_shard.insert(shard.clone(), Slot::Opening);
        }
        let mut reserved = Reserved {
            registry,
            shard: Some(shard),
        };
        let member = MemberId::new(inner.broker.clone(), copy);
        let group = Group::open(
            group,
            member,
            log,
            Arc::clone(&inner.raft),
            Arc::clone(&inner.transport),
            inner.timers,
            inner.host.get().cloned(),
        )
        .await?;
        let group = Arc::new(group);
        let placed = {
            let mut slots = registry.slots.write();
            let placed = !slots.closed;
            if placed && let Some(shard) = reserved.shard.take() {
                slots.by_shard.insert(shard, Slot::Open(Arc::clone(&group)));
                registry.publish(&slots);
            }
            placed
        };
        if !placed {
            // The runtime shut down while the member was starting.
            group.shutdown().await;
            return Err(OpenError::Closed);
        }
        Ok(())
    }

    /// Form `shard`'s group from its first member set. Every member in the
    /// set is told the same set. Doing it again, or after the member has
    /// heard from the group, changes nothing.
    pub async fn initialize(
        &self,
        shard: &ShardKey,
        members: BTreeSet<MemberId>,
    ) -> Result<(), InitializeError> {
        let group = self
            .inner
            .registry
            .get(shard)
            .ok_or(InitializeError::NoSuchGroup)?;
        if !members.contains(&group.member) {
            return Err(InitializeError::NotAMember(group.member.clone()));
        }
        group
            .initialize(members)
            .await
            .map_err(InitializeError::Raft)
    }

    /// Ask every other member of `members` whether it has heard from a
    /// group of `shard`. `Ok` only when each one answered within `within`
    /// that it holds no vote and no entry.
    ///
    /// The one member that forms a group asks this first: a set with a
    /// member a group already reached must not be formed a second time,
    /// because the second group would elect on empty logs and overwrite
    /// what the first acknowledged. Silence is not an answer. The question
    /// is a pre-vote, which changes nothing on the member asked.
    pub async fn peers_blank(
        &self,
        shard: &ShardKey,
        members: &BTreeSet<MemberId>,
        within: Duration,
    ) -> Result<(), NotBlank> {
        let group = self
            .inner
            .registry
            .get(shard)
            .ok_or(NotBlank::NoSuchGroup)?;
        let asks = members
            .iter()
            .filter(|member| **member != group.member)
            .map(|member| {
                let request = RaftRequest {
                    group: group.id.clone(),
                    to: member.clone(),
                    beat: 0,
                    report: false,
                    deadline: within,
                    body: Request::PreVote(VoteRequest::new(
                        Vote {
                            leader_id: LeaderId {
                                term: 0,
                                voted_for: group.member.clone(),
                            },
                            committed: false,
                        },
                        None,
                    )),
                };
                let transport = Arc::clone(&self.inner.transport);
                async move {
                    let unanswered = |why: String| NotBlank::Unanswered(member.clone(), why);
                    match tokio::time::timeout(within, transport.send(request)).await {
                        Ok(Ok(RaftReply(Reply::Vote(answer)))) => {
                            if blank(&answer) {
                                Ok(())
                            } else {
                                Err(NotBlank::Formed(member.clone()))
                            }
                        }
                        Ok(Ok(_)) => Err(unanswered("not an answer to the question".into())),
                        Ok(Err(refused)) => Err(unanswered(refused.to_string())),
                        Err(_) => Err(unanswered(format!("nothing within {within:?}"))),
                    }
                }
            });
        futures::future::try_join_all(asks).await.map(|_| ())
    }

    /// The brokers that vote in `shard`'s group, as this member's log has
    /// them. Empty until the member has heard from a group.
    pub fn voters(&self, shard: &ShardKey) -> Option<BTreeSet<MemberId>> {
        self.inner.registry.get(shard).map(|group| group.voters())
    }

    /// Stop this broker's member of `shard`'s group and forget it. The log
    /// is left as it is, for the caller to close or reopen. False if the
    /// shard has no group here.
    ///
    /// The shard cannot be opened again until the member has stopped: an
    /// open before that is refused with [`OpenError::Closing`]. That holds
    /// even if the caller drops this future.
    pub async fn close(&self, shard: &ShardKey) -> bool {
        let registry = Arc::clone(&self.inner.registry);
        let (shard, group) = {
            let mut slots = registry.slots.write();
            // A slot still opening is its open's to place or free.
            let Some((shard, Slot::Open(group))) = slots.by_shard.get_key_value(shard) else {
                return false;
            };
            let (shard, group) = (Arc::clone(shard), Arc::clone(group));
            slots
                .by_shard
                .insert(Arc::clone(&shard), Slot::Closing(Arc::clone(&group)));
            registry.publish(&slots);
            (shard, group)
        };
        // In its own task, so the slot is freed when the member has
        // stopped and not when the caller stops waiting.
        let stopped = tokio::spawn(async move {
            group.shutdown().await;
            let mut slots = registry.slots.write();
            if matches!(slots.by_shard.get(&shard), Some(Slot::Closing(_))) {
                slots.by_shard.remove(&shard);
            }
        });
        let _ = stopped.await;
        true
    }

    /// Stop the ticker and every group. Later opens are refused.
    pub async fn shutdown(&self) {
        let inner = &self.inner;
        inner.stop.cancel();
        let ticker = inner.ticker.lock().take();
        if let Some(ticker) = ticker {
            let _ = ticker.await;
        }
        let groups: Vec<Arc<Group>> = {
            let mut slots = inner.registry.slots.write();
            slots.closed = true;
            let groups = slots
                .by_shard
                .drain()
                .filter_map(|(_, slot)| match slot {
                    // A member being closed is stopped by its close too.
                    // Both wait for the same stop.
                    Slot::Open(group) | Slot::Closing(group) => Some(group),
                    Slot::Opening => None,
                })
                .collect();
            inner.registry.publish(&slots);
            groups
        };
        futures::future::join_all(groups.iter().map(|group| group.shutdown())).await;
    }

    /// Hand a peer's message to the member it names.
    pub async fn handle(&self, request: RaftRequest) -> Result<RaftReply, Refused> {
        let RaftRequest {
            group,
            to,
            beat,
            report,
            body,
            ..
        } = request;
        match self.inner.registry.get(&group.shard) {
            // Another epoch or another copy of this shard is another member.
            Some(member) if member.id == group && member.member == to => {
                member.handle(body, beat, report).await.map(RaftReply)
            }
            _ => Err(Refused::NoSuchMember(Box::new(Addressee { group, to }))),
        }
    }

    /// This broker's member of `shard`'s group, if it has one.
    pub fn status(&self, shard: &ShardKey) -> Option<GroupStatus> {
        self.inner.registry.get(shard).map(|group| group.status())
    }

    /// What `shard`'s log holds, as it changes: written as soon as an
    /// append has handed its entries to the log, flushed once they are
    /// fsynced. A publisher's acknowledgement on a stream that does not
    /// fsync on commit waits on the first; everything Raft counts waits on
    /// the second. See `docs/per-shard-raft.md`, "Two signals from one
    /// append".
    pub fn log_positions(
        &self,
        shard: &ShardKey,
    ) -> Option<tokio::sync::watch::Receiver<LogPositions>> {
        self.inner
            .registry
            .get(shard)
            .map(|group| group.positions())
    }

    /// How many groups are open here.
    pub fn groups(&self) -> usize {
        self.inner.registry.ticked.load().len()
    }

    /// How many times the ticker ended on a panic and was started again.
    /// Anything but zero is a bug worth a look: while the ticker is down no
    /// group on this broker heartbeats or notices a timeout.
    pub fn ticker_restarts(&self) -> u64 {
        self.inner.registry.ticker_restarts.load(Ordering::Relaxed)
    }

    pub(crate) fn group(&self, shard: &ShardKey) -> Option<Arc<Group>> {
        self.inner.registry.get(shard)
    }

    #[cfg(test)]
    pub(crate) fn trip_ticker(&self) {
        self.inner
            .registry
            .trip_ticker
            .store(true, Ordering::Relaxed);
    }
}

/// Whether a pre-vote answer comes from a member no group has reached: it
/// holds no vote and no entry. Either one alone means a group got there.
pub(super) fn blank(answer: &VoteResponse<ShardTypes>) -> bool {
    answer.vote.leader_id.term == 0 && answer.last_log_id.is_none()
}
