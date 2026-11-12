//! The groups one broker holds, and their lifecycle.

use std::collections::{BTreeSet, HashMap};
use std::io;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use felix_storage::DiskLog;
use felix_storage::raft_log::{LogPoint, LogPositions};
use openraft::{Config, SnapshotPolicy};
use tokio_util::sync::CancellationToken;

use super::group::{Group, Timers};
use super::network::{Addressee, RaftReply, RaftRequest, RaftTransport, Refused};
use super::ticker;
use super::types::{GroupId, MemberId};
use crate::ShardKey;

/// The timers every group on a broker shares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConfig {
    /// How often the ticker runs each group's timer check. A timeout is
    /// noticed at the next check, so this adds to the two below.
    pub tick: Duration,
    /// How often a leader with nothing to send sends a heartbeat.
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
            idle_heartbeat: Duration::from_secs(1),
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
    pub committed: Option<u64>,
    /// Timer checks the ticker sent this member.
    pub ticks: u64,
    /// Timer checks dropped because the group had not taken one by the next
    /// interval. It rises while a group is too busy to look at its timers.
    pub dropped_ticks: u64,
}

enum Slot {
    /// Reserved by an open that is still starting the member.
    Opening,
    Open(Arc<Group>),
}

/// A shard's slot while its member starts. Dropped before the member is
/// placed, by a failed or abandoned open, it frees the slot.
struct Reserved<'a> {
    registry: &'a Registry,
    shard: Option<ShardKey>,
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
}

#[derive(Default)]
struct Slots {
    by_shard: HashMap<ShardKey, Slot>,
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
                Slot::Opening => None,
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
                }),
                stop: CancellationToken::new(),
                ticker: parking_lot::Mutex::new(None),
            }),
        })
    }

    pub(crate) fn start_ticker(&self) {
        let inner = &self.inner;
        let mut ticker = inner.ticker.lock();
        if ticker.is_none() && !inner.stop.is_cancelled() {
            *ticker = Some(tokio::spawn(ticker::run(
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
            if slots.by_shard.contains_key(&shard) {
                return Err(OpenError::AlreadyOpen);
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

    /// Stop this broker's member of `shard`'s group and forget it. The log
    /// is left as it is, for the caller to close or reopen. False if the
    /// shard has no group here.
    pub async fn close(&self, shard: &ShardKey) -> bool {
        let registry = &self.inner.registry;
        let group = {
            let mut slots = registry.slots.write();
            // A slot still opening is its open's to place or free.
            if !matches!(slots.by_shard.get(shard), Some(Slot::Open(_))) {
                return false;
            }
            let Some(Slot::Open(group)) = slots.by_shard.remove(shard) else {
                return false;
            };
            registry.publish(&slots);
            group
        };
        group.shutdown().await;
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
                    Slot::Open(group) => Some(group),
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
            body,
        } = request;
        match self.inner.registry.get(&group.shard) {
            // Another epoch or another copy of this shard is another member.
            Some(member) if member.id == group && member.member == to => {
                member.handle(body, beat).await.map(RaftReply)
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

    #[cfg(test)]
    pub(crate) fn group(&self, shard: &ShardKey) -> Option<Arc<Group>> {
        self.inner.registry.get(shard)
    }
}
