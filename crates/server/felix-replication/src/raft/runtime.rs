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
use felix_storage::raft_copy::{
    CopyMarks, CopyRoot, KeptReport, LogMarkMode, Rotation, reclaim_kept,
};
use felix_storage::raft_log::{LogPoint, LogPositions};
use openraft::impls::leader_id_std::LeaderId;
use openraft::raft::{VoteRequest, VoteResponse};
use openraft::{Config, SnapshotPolicy, Vote};
use tokio_util::sync::CancellationToken;

use super::group::{self, Group, Timers};
use super::network::{Addressee, RaftReply, RaftRequest, RaftTransport, Refused, Reply, Request};
use super::propose::Clock;
use super::ticker;
use super::types::{GroupId, MemberId, ShardTypes};
use crate::driver::WriteFence;
use crate::engine::{EngineHost, Host};
use crate::quorum::ShardServing;
use crate::{ShardKey, ShardKind};

/// How often a kept-copy reclaim that waits only on a commit save looks
/// again. A log not under `FsyncMode::OnCommit` saves at most this often.
const SAVE_POLL: Duration = felix_storage::disk_log::COMMIT_PERSIST_INTERVAL;

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
    /// A cache shard snapshots its state each time this many entries have
    /// committed since its last snapshot. Zero never snapshots, and the
    /// shard's log then grows without bound.
    pub cache_snapshot_every: u64,
    /// How many of the entries under a cache shard's snapshot stay in its
    /// log, so a member a little behind catches up from the log and only
    /// one further behind is sent the snapshot.
    pub cache_log_keep: u64,
    /// How long after a majority's last answer a leader still acknowledges
    /// a `Leader` write on its own log. A follower promises its leader
    /// `election_max` on its own clock, so the lease must end before that
    /// on any two clocks within the drift bound:
    /// `lease * (1 + p) / (1 - p) <= election_max`.
    pub lease: Duration,
    /// `p` above, in parts per million: how much faster or slower than real
    /// time a broker's clock may run.
    pub clock_drift_ppm: u32,
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
            cache_snapshot_every: 16_384,
            cache_log_keep: 1_024,
            // A quarter under the follower's promise, which holds for
            // clocks up to 10% off.
            lease: Duration::from_millis(1500),
            clock_drift_ppm: 100_000,
            log_mark: LogMarkMode::Exact,
        }
    }
}

/// How long one piece of a snapshot may take to send and be answered. The
/// last piece's answer waits for the install as well.
pub(super) const SNAPSHOT_PIECE_MILLIS: u64 = 10_000;

fn wall_clock_micros() -> u64 {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH);
    now.map_or(0, |now| now.as_micros() as u64)
}

impl RuntimeConfig {
    pub(crate) fn raft(&self, kind: ShardKind) -> Result<Config, InvalidTimers> {
        if self.tick.is_zero() {
            return Err(InvalidTimers("the tick interval is zero".into()));
        }
        if self.idle_heartbeat < self.heartbeat {
            return Err(InvalidTimers(
                "the idle heartbeat is shorter than the heartbeat".into(),
            ));
        }
        if !self.lease_fits() {
            return Err(InvalidTimers(format!(
                "a lease of {:?} can outlast a follower's promise of {:?} on clocks {} ppm off",
                self.lease, self.election_max, self.clock_drift_ppm
            )));
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
            // A stream shard's log is its data. Entries leave it by
            // retention, never because a snapshot was taken. A cache
            // shard's state is in its snapshot, so its log is cut to it.
            // openraft checks the policy after each batch of messages, not
            // on its tick, which is off.
            snapshot_policy: match kind {
                ShardKind::Cache if self.cache_snapshot_every > 0 => {
                    SnapshotPolicy::LogsSinceLast(self.cache_snapshot_every)
                }
                _ => SnapshotPolicy::Never,
            },
            // For a stream shard a snapshot that is taken anyway purges
            // nothing. The default keeps only the last 1,000 entries.
            max_in_snapshot_log_to_keep: match kind {
                ShardKind::Cache if self.cache_snapshot_every > 0 => self.cache_log_keep,
                _ => u64::MAX,
            },
            // The deadline of one piece of a snapshot, which also sizes it.
            install_snapshot_timeout: SNAPSHOT_PIECE_MILLIS,
            // A voter that comes back with a shorter log stops its leader.
            // A copy that lost records rejoins under a new copy id instead.
            allow_log_reversion: Some(false),
            // A member whose stored vote is its own committed one stands
            // for the next term as it starts. It promised nobody, so it
            // skips the vote hold too (`Group::open`).
            enable_leader_restore: Some(true),
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

impl RuntimeConfig {
    /// `lease * (1 + p) / (1 - p) <= election_max`: the leader's lease on
    /// a fast clock ends before a follower's promise on a slow one.
    fn lease_fits(&self) -> bool {
        const MILLION: u128 = 1_000_000;
        let drift = u128::from(self.clock_drift_ppm);
        drift < MILLION
            && self.lease.as_nanos() * (MILLION + drift)
                <= self.election_max.as_nanos() * (MILLION - drift)
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

/// A member [`GroupRuntime::open_copy`] started.
pub struct OpenedMember {
    /// This broker and the copy id the directory holds.
    pub member: MemberId,
    /// The shard's log. Close it once the member is closed.
    pub log: DiskLog,
    /// Set when the directory's copy was rotated.
    pub rotation: Option<Rotation>,
    /// What reclaiming the copies earlier rotations kept did at open. The
    /// ones it held are tried again as the group commits.
    pub kept: KeptReport,
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
    /// What a cache shard's member runs on: it snapshots and purges.
    raft_cache: Arc<Config>,
    transport: Arc<dyn RaftTransport>,
    registry: Arc<Registry>,
    /// Who is told when a shard opens and closes here.
    host: OnceLock<Arc<dyn EngineHost>>,
    /// The clock cache shards opened from here on stamp by.
    clock: parking_lot::RwLock<Clock>,
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
                    lease: config.lease.as_millis() as u64,
                },
                raft: Arc::new(config.raft(ShardKind::Stream)?),
                raft_cache: Arc::new(config.raft(ShardKind::Cache)?),
                transport,
                clock: parking_lot::RwLock::new(Arc::new(wall_clock_micros)),
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

    #[cfg(test)]
    pub(crate) fn set_clock(&self, clock: Clock) {
        *self.inner.clock.write() = clock;
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
    /// and no marks are kept. A shard is meant to open through
    /// [`Self::open_copy`]. The broker opening its groups here is interim:
    /// it runs without the copy and restore check until it opens them
    /// through the gate (#2441).
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
        let kept_config = config.clone();
        let shard = group.shard.clone();
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
        let kept = self.reclaim_kept(&shard, dir, kept_config).await;
        Ok(OpenedMember {
            member,
            log,
            rotation: opened.rotation,
            kept,
        })
    }

    /// Delete the kept copies the running member already covers. For the
    /// ones it is behind, a task waits for its saved commit and flushed tail
    /// to reach them and tries again. It ends when the group or the runtime
    /// stops and never deletes anything after that. Failures are logged: a
    /// kept copy left on disk is never worse than one deleted early.
    async fn reclaim_kept(&self, shard: &ShardKey, dir: &Path, config: LogConfig) -> KeptReport {
        let Some(group) = self.inner.registry.get(shard) else {
            return KeptReport::default();
        };
        let (stop, stopped) = (self.inner.stop.clone(), group.stopped());
        let halted = {
            let (stop, stopped) = (stop.clone(), stopped.clone());
            move || stop.is_cancelled() || stopped.is_cancelled()
        };
        let report = match reclaim_kept(dir, group.current_copy(), config.clone(), &halted).await {
            Ok(report) => report,
            Err(err) => {
                tracing::warn!(dir = %dir.display(), error = %err, "could not reclaim kept shard copies");
                return KeptReport::default();
            }
        };
        let Some(mut wait) = report.wait_for() else {
            return report;
        };
        let dir = dir.to_path_buf();
        let (mut commits, mut positions) = (group.commits(), group.positions());
        let covers = |group: &Group, wait: u64| {
            let at = group.current_copy();
            at.committed().is_some_and(|at| at >= wait)
                && at.flushed_last().is_some_and(|at| at >= wait)
        };
        // The saved commit follows the one in memory with nothing to wake
        // on, so a wait that lacks only the save polls for it.
        let saving = |group: &Group, wait: u64| {
            let at = group.current_copy();
            at.committed_in_memory().is_some_and(|at| at >= wait)
                && !at.committed().is_some_and(|at| at >= wait)
        };
        tokio::spawn(async move {
            loop {
                while !covers(&group, wait) {
                    let poll = saving(&group, wait);
                    tokio::select! {
                        () = stop.cancelled() => return,
                        () = stopped.cancelled() => return,
                        changed = commits.changed() => if changed.is_err() { return },
                        changed = positions.changed() => if changed.is_err() { return },
                        () = tokio::time::sleep(SAVE_POLL), if poll => {}
                    }
                }
                if halted() {
                    return;
                }
                let report = match reclaim_kept(&dir, group.current_copy(), config.clone(), &halted)
                    .await
                {
                    Ok(report) => report,
                    Err(err) => {
                        tracing::warn!(dir = %dir.display(), error = %err, "could not reclaim kept shard copies");
                        return;
                    }
                };
                let Some(next) = report.wait_for() else {
                    return;
                };
                // Covered, yet still held: the pass saw an older state. Wait
                // for the next change instead of spinning.
                if covers(&group, next) {
                    tokio::select! {
                        () = stop.cancelled() => return,
                        () = stopped.cancelled() => return,
                        changed = commits.changed() => if changed.is_err() { return },
                        changed = positions.changed() => if changed.is_err() { return },
                    }
                }
                wait = next;
            }
        });
        report
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
        let host = group::Host {
            engine: inner.host.get().cloned(),
            clock: Arc::clone(&inner.clock.read()),
        };
        let raft = match group.shard.kind {
            ShardKind::Stream => &inner.raft,
            ShardKind::Cache => &inner.raft_cache,
        };
        let group = Group::open(
            group,
            member,
            log,
            marks.map(|marks| (marks, inner.log_mark)),
            Arc::clone(raft),
            Arc::clone(&inner.transport),
            inner.timers,
            host,
        )
        .await?;
        let group = Arc::new(group);
        group.start_expiry();
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
