//! The groups one broker holds, and their lifecycle.

use std::collections::{BTreeSet, HashMap};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use arc_swap::ArcSwap;
use std::path::Path;

use felix_storage::DiskLog;
use felix_storage::log::LogConfig;
use felix_storage::raft_copy::{CopyMarks, CopyRoot, LogMarkMode, Rotation};
use felix_storage::raft_log::{LogPoint, LogPositions};
use openraft::{Config, SnapshotPolicy};
use tokio_util::sync::CancellationToken;

use super::group::{Group, Timers};
use super::network::{Addressee, RaftReply, RaftRequest, RaftTransport, Refused};
use super::ticker;
use super::types::{GroupId, MemberId};
use crate::driver::WriteFence;
use crate::engine::{EngineHost, Host};
use crate::quorum::ShardServing;
use crate::{ShardKey, ShardKind};

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
    /// How a member opened by [`GroupRuntime::open_copy`] keeps its log
    /// mark at the data root.
    pub log_mark: LogMarkMode,
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
            log_mark: LogMarkMode::Exact,
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

/// A member [`GroupRuntime::open_copy`] started.
pub struct OpenedMember {
    /// This broker and the copy id the directory holds.
    pub member: MemberId,
    /// The shard's log. Close it once the member is closed.
    pub log: DiskLog,
    /// Set when the directory's copy was rotated.
    pub rotation: Option<Rotation>,
}

/// The name a shard's marks go by at the data root. It is on disk, so it
/// must not change: names come from users and may hold any character, so
/// each is length-prefixed, and the kind is a fixed tag.
pub(super) fn mark_name(shard: &ShardKey) -> String {
    let ShardKey {
        tenant_id,
        namespace,
        stream,
        shard,
        kind,
    } = shard;
    let kind = match kind {
        ShardKind::Stream => "stream",
        ShardKind::Cache => "cache",
    };
    format!(
        "{}:{tenant_id}/{}:{namespace}/{}:{stream}/{shard}/{kind}",
        tenant_id.len(),
        namespace.len(),
        stream.len()
    )
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
    log_mark: LogMarkMode,
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
                log_mark: config.log_mark,
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
    ///
    /// Nothing here checks that the directory may still hold copy `copy`,
    /// and no marks are kept, so it is for tests only. A shard opens
    /// through [`Self::open_copy`].
    #[cfg(any(test, feature = "testing"))]
    pub async fn open(&self, group: GroupId, copy: u64, log: DiskLog) -> Result<(), OpenError> {
        let reserved = self.reserve(&group)?;
        let member = MemberId::new(self.inner.broker.clone(), copy);
        self.start_member(reserved, group, member, log, None).await
    }

    /// Start this broker's member of `group` on the shard directory `dir`,
    /// opened as a copy through `root`, a data root that passed the gate.
    ///
    /// The directory keeps its copy id only if it may: an empty directory,
    /// one minted under another storage id, one whose copy the root's marks
    /// do not name, or one whose vote or log is below its marks gets a new
    /// copy id that never voted and an empty log. The member id is this
    /// broker and the copy id in the directory. From here on the vote mark
    /// is synced before a vote is answered and the log mark follows the log
    /// as [`RuntimeConfig::log_mark`] says. See `docs/per-shard-raft.md`,
    /// "Group and member identity".
    pub async fn open_copy(
        &self,
        group: GroupId,
        dir: &Path,
        config: LogConfig,
        root: &CopyRoot,
    ) -> Result<OpenedMember, OpenError> {
        let reserved = self.reserve(&group)?;
        let opened = root
            .open_copy(&mark_name(&group.shard), dir, group.to_string(), config)
            .await?;
        let member = MemberId::new(self.inner.broker.clone(), opened.copy);
        let log = opened.log;
        let started = self
            .start_member(
                reserved,
                group,
                member.clone(),
                log.clone(),
                Some(opened.marks),
            )
            .await;
        if let Err(err) = started {
            let _ = log.close().await;
            return Err(err);
        }
        Ok(OpenedMember {
            member,
            log,
            rotation: opened.rotation,
        })
    }

    /// Hold `group`'s shard while a member starts on it.
    fn reserve(&self, group: &GroupId) -> Result<Reserved<'_>, OpenError> {
        let registry = &self.inner.registry;
        let shard = group.shard.clone();
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
        Ok(Reserved {
            registry,
            shard: Some(shard),
        })
    }

    async fn start_member(
        &self,
        mut reserved: Reserved<'_>,
        group: GroupId,
        member: MemberId,
        log: DiskLog,
        marks: Option<CopyMarks>,
    ) -> Result<(), OpenError> {
        let inner = &self.inner;
        let registry = &inner.registry;
        let group = Group::open(
            group,
            member,
            log,
            marks.map(|marks| (marks, inner.log_mark)),
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
