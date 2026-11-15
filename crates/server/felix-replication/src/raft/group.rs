//! One member of one group: its openraft instance over the shard's log.

use std::collections::BTreeSet;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use felix_storage::DiskLog;
use felix_storage::raft_copy::{CopyMarks, LogMarkMode, MarkLog};
use felix_storage::raft_log::{LogPoint, LogPositions, RaftLog, SnapshotStore, TailBudget};
use felix_storage::waiters::Waiters;
use openraft::error::{ForwardReason, InitializeError, LinearizableReadError, RaftError};
use openraft::raft::{SnapshotResponse, VoteResponse};
use openraft::storage::{RaftLogReader, Snapshot};
use openraft::type_config::alias::{LogIdOf, VoteOf};
use openraft::vote::RaftLeaderId;
use openraft::{Config, Raft, ReadPolicy, ServerState};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use bytes::Bytes;

use super::cache::{Answer, CacheCommand};
use super::codec::ShardCodec;
use super::commit::{self, Commits};
use super::install::{self, Incoming, Piece};
use super::lead::{self, Leadership};
use super::lease::{Lease, View};
use super::machine::{self, Cache, Machine, SnapshotData};
use super::membership::Target;
use super::network::{
    Addressee, Network, RaftRequest, RaftTransport, Refused, Reply, Request, Router, term_of,
};
use super::propose::{
    self, AckAt, Batch, Clock, Proposal, Stamper, Unacknowledged, Unstamped, Waiting,
};
use super::reach::{Reach, Refusals};
use super::read_rounds::ReadRounds;
use super::runtime::{GroupStatus, OpenError, Role};
use super::types::{GroupId, MemberId, ShardTypes};
use super::written::Reports;
use crate::ShardKind;
use crate::engine::{CommitError, EngineHost, Written};

pub(crate) mod driver;
pub(crate) mod membership;

use driver::MembershipProgress;

/// Why a member a group reached before its rotation does not answer that
/// it is blank. Re-forming the group is an operator command.
pub(super) const REACHED_BEFORE: &str =
    "an earlier copy of this shard held records, so no group forms on it until it is re-formed";

pub(crate) struct Group {
    pub(crate) id: GroupId,
    pub(crate) member: MemberId,
    raft: Raft<ShardTypes, Machine>,
    /// The member's log, to drain its queued flushes at the stop.
    log: RaftLog<ShardTypes, ShardCodec>,
    pace: Arc<Pace>,
    lease: Arc<Lease>,
    /// Empty for a member that started with no vote on disk.
    hold: Hold,
    /// Carries this member's written position to its leader.
    transport: Arc<dyn RaftTransport>,
    /// The written positions other members told this one.
    reports: Arc<Reports>,
    /// The furthest entry acknowledged on `reports` before its commit,
    /// which a confirmed read must wait to have applied.
    answered: Arc<parking_lot::Mutex<Option<LogPoint>>>,
    /// The data root's mark log, when the member has one. Its
    /// vouched-unsynced marker is synced before this member's unsynced write
    /// is told to a leader or counted here, so a power cut rotates the copy.
    root: Option<MarkLog>,
    /// The brokers on which this group's member refused this one's calls.
    refusals: Arc<Refusals>,
    /// What the log holds written and flushed, as the adapter reports it.
    positions: tokio::sync::watch::Receiver<LogPositions>,
    /// Waits on `positions.written` and `positions.flushed`, woken only
    /// once the position reaches theirs.
    written_waiters: Waiters<(u64, u64)>,
    flushed_waiters: Waiters<(u64, u64)>,
    /// What this member has applied, as its state machine reports it.
    commits: tokio::sync::watch::Receiver<Commits>,
    /// Waits on the applied index, woken only once it reaches theirs.
    applied: Waiters<u64>,
    /// The member's log, and its first index read live: a purge held only in
    /// memory counts.
    copy: felix_storage::raft_copy::CurrentCopy,
    /// Whether this member leads and the shard is open.
    leadership: Arc<Leadership>,
    /// Set when a group reached an earlier copy of this directory. The
    /// member then never answers that it is blank, so no group forms on it.
    reached_before: AtomicBool,
    /// A cache shard's projection, as this member has applied it.
    cache: Option<Cache>,
    /// The snapshot a cache shard's member is being sent.
    incoming: Arc<Incoming>,
    /// What a cache shard's leader stamps entries and judges expiry by.
    clock: Clock,
    #[cfg(test)]
    pub(crate) gate: Arc<propose::Gate>,
    #[cfg(test)]
    pub(crate) apply_hold: Arc<tokio::sync::Mutex<()>>,
    /// The newest target this member was told, kept in memory only: after
    /// a restart nothing changes until the control plane speaks again.
    target: tokio::sync::watch::Sender<Option<Target>>,
    /// Where the membership stands against the target, as the driver
    /// reports it.
    progress: Arc<tokio::sync::watch::Sender<MembershipProgress>>,
    /// How far behind a learner may be when it is promoted.
    lag_threshold: u64,
    /// Timer checks the group was sent.
    ticks: AtomicU64,
    /// Timer checks the ticker gave up on because the group had not taken
    /// them by the next interval.
    dropped_ticks: AtomicU64,
    /// The term this member was first seen leading, and when, on the
    /// runtime's clock. It ages a term no majority has answered yet.
    leads_since: parking_lot::Mutex<Option<(u64, u64)>>,
    /// Set once a timer check finds the group's core gone.
    stopped: AtomicBool,
    /// The shard's publish queue, in commit order. Its task starts with the
    /// first publish, so a group that takes none has none.
    proposals: OnceLock<mpsc::UnboundedSender<Proposal>>,
    /// Calls made to openraft for publishes. One carries every batch that
    /// was queued when it was made.
    client_writes: Arc<AtomicU64>,
    /// Cancelled when the member is told to stop. Ends the publish task and
    /// every publisher's wait.
    stop: CancellationToken,
    /// The read-index rounds the shard's reads share.
    reads: Arc<ReadRounds<Round>>,
    /// The runtime the group was opened on, where its openraft core runs.
    /// The publish task starts there, not on the first publisher's runtime,
    /// which may end while the group lives on.
    runtime: tokio::runtime::Handle,
}

/// The runtime's clock and timers, as a group needs them to say when its
/// next timer check is due.
#[derive(Clone, Copy)]
pub(crate) struct Timers {
    pub(crate) started: tokio::time::Instant,
    pub(crate) tick: u64,
    pub(crate) heartbeat: u64,
    pub(crate) idle_heartbeat: u64,
    pub(crate) idle_after: u64,
    pub(crate) election_min: u64,
    pub(crate) election_max: u64,
    pub(crate) lease: u64,
}

impl Timers {
    pub(crate) fn now(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    /// A peer that has answered nothing for this long (5 s by default) no
    /// longer holds its leader at the short gap. That is less than the 6-7 s
    /// an idle follower waits, which is the safe side: a follower that still
    /// hears the leader was told the idle gap, so the slow heartbeats reach
    /// it inside its wait. Only one that restarts meanwhile and loses its
    /// first idle heartbeat can stand (docs/per-shard-raft.md, "Idle groups").
    fn silent(&self) -> u64 {
        self.election_min + 2 * self.election_max
    }
}

/// How fast a group's timers run: a leader that has sent no entry for
/// `idle_after` heartbeats at the idle interval, and its followers wait
/// longer for it. Shared by the member and its side of the router, which
/// sees what the leader sends and what comes back.
///
/// The rule that keeps a slow leader from being deposed: a follower waits
/// by the gap its leader last told it, and a leader slows down only once
/// every follower has answered a message that named the idle gap. A lost
/// message leaves the leader fast or the follower patient, never a patient
/// leader with an impatient follower. A follower that restarts forgets the
/// gap it was told, so an append it does not take voids its answer. One
/// exception remains: an append that reaches the follower late and still
/// finishes inside its deadline there, but past the leader's, can store the
/// short gap after a later heartbeat's idle one (#2600). See
/// `docs/per-shard-raft.md`, "Idle groups".
pub(crate) struct Pace {
    timers: Timers,
    /// When the next timer check can matter, in milliseconds on the
    /// runtime's clock. The ticker skips the group until then.
    due: AtomicU64,
    /// When this member last took an append from its leader or granted a
    /// vote. Either restarts its election timer.
    heard: AtomicU64,
    /// As a follower: the longest its leader said it may stay silent.
    promised: AtomicU64,
    /// When this member last took an append, or [`NEVER`].
    led: AtomicU64,
    /// As a leader: when it last sent an entry to anyone.
    wrote: AtomicU64,
    peers: parking_lot::Mutex<Vec<Peer>>,
    /// As a leader: the newest entry any append has carried.
    sent: parking_lot::Mutex<Option<LogIdOf<ShardTypes>>>,
    /// Spreads the members of a group over the idle wait, so they do not
    /// all stand in the same tick.
    jitter: u64,
}

struct Peer {
    id: MemberId,
    /// The `wrote` this peer answered an idle heartbeat under. It counts
    /// only while `wrote` is still that, so any later entry voids it.
    idle_under: Option<u64>,
    answered: u64,
    /// Appends naming the short gap that are on their way to this peer.
    short_out: u32,
}

impl Peer {
    fn settled(&mut self, sent: &Sent, timers: &Timers) {
        if sent.gap <= timers.heartbeat {
            debug_assert!(self.short_out > 0, "settled an append never sent");
            self.short_out = self.short_out.saturating_sub(1);
        }
    }
}

/// An append on its way to a peer, for [`Pace::answered`] or
/// [`Pace::unanswered`]. Exactly one of them must be called for it.
pub(crate) struct Sent {
    pub(crate) gap: u64,
    wrote: u64,
    /// Whether no append naming the short gap was on its way to the peer
    /// when this one left.
    clean: bool,
}

impl Pace {
    pub(super) fn new(timers: Timers, group: &GroupId, member: &MemberId) -> Self {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        (group, member).hash(&mut hasher);
        let now = timers.now();
        Self {
            timers,
            due: AtomicU64::new(0),
            heard: AtomicU64::new(0),
            promised: AtomicU64::new(timers.heartbeat),
            led: AtomicU64::new(NEVER),
            wrote: AtomicU64::new(now),
            peers: parking_lot::Mutex::new(Vec::new()),
            sent: parking_lot::Mutex::new(None),
            jitter: hasher.finish() % (timers.election_max - timers.election_min).max(1),
        }
    }

    /// Whether an append whose last entry is `last` carries an entry no
    /// append carried before. Only that makes the group busy: a retry to a
    /// member that is down, or a catch-up, sends nothing new, and would
    /// otherwise hold the group at the short gap for as long as it lasts.
    pub(crate) fn new_entries(&self, last: &LogIdOf<ShardTypes>) -> bool {
        let mut sent = self.sent.lock();
        if sent.as_ref().is_some_and(|sent| last <= sent) {
            return false;
        }
        *sent = Some(*last);
        true
    }

    /// The leader is sending `to` an append. The gap it names is the idle
    /// one once no entry has gone out for `idle_after`. `entries` is
    /// whether the append carries a new one ([`Self::new_entries`]).
    pub(crate) fn sending(&self, to: &MemberId, entries: bool) -> Sent {
        let timers = &self.timers;
        if entries {
            // The followers hear the short gap in this message.
            self.wrote();
        }
        // Read under the lock, so the sends to a peer name their gaps in the
        // order they count in `short_out`: a later send never names an older
        // `wrote` than one already counted.
        let mut peers = self.peers.lock();
        let now = timers.now();
        let wrote = self.wrote.load(Ordering::Relaxed);
        if !peers.iter().any(|peer| &peer.id == to) {
            peers.push(Peer {
                id: to.clone(),
                idle_under: None,
                answered: now,
                short_out: 0,
            });
        }
        let peer = peers
            .iter_mut()
            .find(|peer| &peer.id == to)
            .expect("just added");
        let gap = match now >= wrote + timers.idle_after {
            true => timers.idle_heartbeat,
            false => timers.heartbeat,
        };
        let clean = peer.short_out == 0;
        if gap <= timers.heartbeat {
            peer.short_out += 1;
        }
        Sent { gap, wrote, clean }
    }

    /// The leader has an entry to send. An idle group is busy again: the
    /// answers it slowed down on are void and its next heartbeat is due
    /// within the short gap. A publish calls this itself, so a group with
    /// nobody to send to, or nobody reachable, speeds up all the same.
    pub(crate) fn wrote(&self) {
        let timers = &self.timers;
        let now = timers.now();
        self.wrote.store(now, Ordering::Relaxed);
        self.due
            .fetch_min(now + timers.heartbeat, Ordering::Relaxed);
    }

    /// `to` took the append: it now waits by the gap the append named.
    ///
    /// An idle answer counts only if no short-gap append was on its way when
    /// it left. The follower stores each gap once it has handled the append,
    /// and an entry append it took first can finish its fsync after the
    /// heartbeat's answer, leaving it at the short gap.
    pub(crate) fn answered(&self, to: &MemberId, sent: &Sent) {
        let timers = &self.timers;
        let now = timers.now();
        if let Some(peer) = self.peers.lock().iter_mut().find(|peer| &peer.id == to) {
            peer.settled(sent, timers);
            peer.answered = now;
            if sent.gap > timers.heartbeat && sent.clean {
                peer.idle_under = Some(sent.wrote);
            }
        }
    }

    /// `to` did not take the append: it was lost, refused or rejected. The
    /// peer may have restarted and forgotten the idle gap, so the leader
    /// goes back to the short one until it answers again.
    pub(crate) fn unanswered(&self, to: &MemberId, sent: &Sent) {
        let timers = &self.timers;
        let now = timers.now();
        let mut peers = self.peers.lock();
        let Some(peer) = peers.iter_mut().find(|peer| &peer.id == to) else {
            return;
        };
        peer.settled(sent, timers);
        peer.idle_under = None;
        // A peer long silent is not waited for (`leader_gap`), so a broker
        // that is down does not keep its groups' leaders fast.
        if now < peer.answered + timers.silent() {
            self.due
                .fetch_min(now + timers.heartbeat, Ordering::Relaxed);
        }
    }

    /// How long a leader waits until its next heartbeat.
    pub(super) fn leader_gap(&self, now: u64) -> u64 {
        let timers = &self.timers;
        let wrote = self.wrote.load(Ordering::Relaxed);
        if timers.idle_heartbeat <= timers.heartbeat || now < wrote + timers.idle_after {
            return timers.heartbeat;
        }
        // A peer long silent no longer holds the leader to the short gap.
        let silent = timers.silent();
        let patient = |peer: &Peer| peer.idle_under == Some(wrote) || now >= peer.answered + silent;
        match self.peers.lock().iter().all(patient) {
            true => timers.idle_heartbeat,
            false => timers.heartbeat,
        }
    }

    /// How long a follower goes unchecked after it hears its leader. At the
    /// short gap that is the shortest election timeout. At the idle gap it
    /// is three of them, so two heartbeats can be lost.
    pub(super) fn follower_wait(&self) -> u64 {
        let timers = &self.timers;
        let promised = self.promised.load(Ordering::Relaxed);
        if promised <= timers.heartbeat {
            return timers.election_min;
        }
        timers
            .election_min
            .max(promised.saturating_mul(3))
            .saturating_add(self.jitter)
    }

    /// This member took an append from its leader, which named `gap`.
    pub(super) fn followed(&self, gap: Option<u64>) {
        let timers = &self.timers;
        let now = timers.now();
        if let Some(gap) = gap {
            // An unknown gap is the short one: checked early is safe. The
            // value is a peer's, so it is held to the longest gap this
            // member would name itself: no message can make it wait longer.
            let gap = match gap {
                0 => timers.heartbeat,
                gap => gap.min(timers.idle_heartbeat),
            };
            self.promised.store(gap, Ordering::Relaxed);
        }
        self.heard.store(now, Ordering::Relaxed);
        self.due
            .store(now.saturating_add(self.follower_wait()), Ordering::Relaxed);
    }

    /// This member took an append that named `gap` and reached it at
    /// `arrived`. If handling it outlasted `deadline` the leader has stopped
    /// waiting and counts it unanswered, and may already have had a later
    /// heartbeat's idle gap answered. Storing a short gap could then leave
    /// the follower at the short gap under a slow leader, so it keeps the one
    /// it has. A late idle gap is still stored: a fast leader with a patient
    /// follower is the safe pairing.
    pub(super) fn took(&self, gap: u64, arrived: u64, deadline: Duration) {
        let late = self.timers.now().saturating_sub(arrived) >= deadline.as_millis() as u64;
        let short = gap <= self.timers.heartbeat;
        self.followed((!(late && short)).then_some(gap));
        self.led.store(self.timers.now(), Ordering::Relaxed);
    }

    /// The leader handed its leadership on: this member counts on it no
    /// longer. If the target never stands, the old leader stands once the
    /// transfer expires, and must not be refused for a leader that is gone.
    pub(super) fn forget_leader(&self) {
        self.led.store(NEVER, Ordering::Relaxed);
    }

    /// Whether this member, as a follower, still counts on the leader it
    /// last took an append from: it would not stand itself yet.
    pub(super) fn follows_live_leader(&self, now: u64) -> bool {
        let led = self.led.load(Ordering::Relaxed);
        led != NEVER && now < led.saturating_add(self.follower_wait())
    }

    /// Whether a leader that a majority answered `acked` ago is still
    /// waited for by its followers. Some may have been told the idle gap
    /// even when the leader is back at the short one.
    pub(super) fn leads_live(&self, acked: Duration) -> bool {
        let timers = &self.timers;
        let wait = timers
            .election_min
            .max(timers.idle_heartbeat.saturating_mul(3));
        (acked.as_millis() as u64) < wait
    }

    /// This member granted a vote. Whoever wins starts at the short gap.
    pub(super) fn voted(&self) {
        self.promised
            .store(self.timers.heartbeat, Ordering::Relaxed);
        self.followed(None);
    }
}

/// The vote hold of a member that started with a vote on disk it may have
/// promised a leader on. See `docs/per-shard-raft.md`, "A restarted member
/// grants no vote at first".
#[derive(Default)]
struct Hold {
    /// On the runtime's clock: until then this member takes no vote or
    /// pre-vote request, but for the one below.
    until: u64,
    /// Whether the member is ticked inside the hold, so it campaigns at
    /// once: it promised nobody, or no other voter can have taken its
    /// promise. It still grants nobody else until `until`.
    campaigns: bool,
    /// The stored vote, if it was committed and nothing can have replaced
    /// it since. It names the leader this member followed, and that one
    /// candidate is let through once: the promise was made to it. A vote
    /// that is not committed may name a candidate that never led, and
    /// lets nobody through.
    follows: parking_lot::Mutex<Option<VoteOf<ShardTypes>>>,
}

impl Hold {
    /// Whether the candidate asking with `vote` is the leader the stored
    /// vote names. Once: granting replaces the stored vote with one that
    /// is not committed.
    fn lets_through(&self, vote: &VoteOf<ShardTypes>) -> bool {
        let mut follows = self.follows.lock();
        let named = follows
            .as_ref()
            .is_some_and(|stored| stored.leader_id().node_id() == vote.leader_id().node_id());
        if named {
            *follows = None;
        }
        named
    }

    /// openraft is about to take a message that can replace the stored
    /// vote: an append or a snapshot under `vote`, or one that has this
    /// member stand.
    fn vote_may_change(&self, vote: Option<&VoteOf<ShardTypes>>) {
        let mut follows = self.follows.lock();
        if follows.as_ref() != vote {
            *follows = None;
        }
    }
}

/// Timer checks dropped on this broker because a group had not taken one by
/// the next interval, across every group: a label per group would be a label
/// per shard.
pub(super) const DROPPED_TICKS_TOTAL: &str = "felix_broker_raft_dropped_ticks_total";

/// Counts a timer check that was dropped before the group took it.
pub(super) struct Dropped<'a>(pub(super) Option<&'a AtomicU64>);

impl Drop for Dropped<'_> {
    fn drop(&mut self) {
        if let Some(dropped) = self.0 {
            dropped.fetch_add(1, Ordering::Relaxed);
            metrics::counter!(DROPPED_TICKS_TOTAL).increment(1);
        }
    }
}

/// What one read-index round answered, shared by the reads that joined it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Round {
    /// A majority answered this member as leader of this term.
    Leads {
        term: u64,
        /// One past the commit index a majority confirmed.
        index: u64,
    },
    /// This member leads but its lease lapsed: ask again after a heartbeat.
    /// The pinned openraft says this to writes only.
    LeaseExpired,
    NotLeader,
    /// No majority answered in time.
    QuorumNotEnough,
    Stopped,
}

/// What a member is told and asks of the broker it runs on.
pub(crate) struct Host {
    /// Who is told when the shard opens and closes here.
    pub(crate) engine: Option<Arc<dyn EngineHost>>,
    pub(crate) clock: Clock,
}

/// [`Pace`]'s mark for a time that has not come yet.
const NEVER: u64 = u64::MAX;

/// Expired values deleted by one pass of the expiry task.
const EXPIRE_AT_ONCE: usize = 256;

/// How long a stop waits for the member's queued flushes.
const DRAIN_LIMIT: Duration = Duration::from_secs(5);

impl Group {
    /// Start `member` on `log`. The log decides where the member resumes: an
    /// empty one is a member that has never voted.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn open(
        id: GroupId,
        member: MemberId,
        log: DiskLog,
        marks: Option<(CopyMarks, LogMarkMode)>,
        config: Arc<Config>,
        transport: Arc<dyn RaftTransport>,
        timers: Timers,
        host: Host,
        tail_budget: Arc<TailBudget>,
        reach: Arc<Reach>,
    ) -> Result<Self, OpenError> {
        let Host {
            engine: host,
            clock,
        } = host;
        let snapshots = SnapshotStore::open(log.dir())?;
        let log =
            RaftLog::<ShardTypes, ShardCodec>::with_tail_budget(log, ShardCodec, tail_budget)?;
        // A member that has voted may have promised a leader not to vote
        // again for one election timeout, and that promise was in memory.
        // It started after the last message it took, so waiting the
        // timeout out from here covers it. A member with no vote has
        // taken no message and is not held.
        //
        // The vote on disk, not openraft's: at a start openraft demotes
        // its own committed vote in memory.
        let stored = RaftLogReader::read_vote(&mut log.reader()).await?;
        // One whose vote is its own committed one took no later leader's
        // message, so it promised nobody and may stand at once. It still
        // grants nobody else inside the hold, as FelixRaftLease.tla's
        // MayGrant has it.
        let led = stored
            .as_ref()
            .is_some_and(|vote| vote.is_committed() && vote.leader_id().node_id() == &member);
        // A stream shard's state is its log, and a cache shard's is its
        // snapshot plus the log above it. Either way nothing can rebuild
        // what a purge past the snapshot would drop.
        log.purge_within(&snapshots)?;
        let copy_id = marks.as_ref().map(|(marks, _)| marks.copy());
        let root = marks.as_ref().map(|(marks, _)| marks.mark_log().clone());
        if let Some((marks, mode)) = marks {
            log.mark_with(marks, mode)?;
            log.mark_recovered().await?;
        }
        let positions = log.positions();
        let (written_waiters, flushed_waiters) = (log.written_waiters(), log.flushed_waiters());
        let copy = felix_storage::raft_copy::CurrentCopy::new(
            log.disk_log().clone(),
            copy_id,
            log.first_index_reader(),
        );
        let (machine, commits, applied, cache) =
            Machine::open(snapshots, id.shard.kind, Some(log.disk_log())).await?;
        #[cfg(test)]
        let apply_hold = Arc::clone(&machine.hold);
        let pace = Arc::new(Pace::new(timers, &id, &member));
        let lease = Arc::new(Lease::new(Duration::from_millis(timers.lease)));
        let reports = Arc::new(Reports::new());
        let refusals = Arc::new(Refusals::new(id.clone()));
        let network = Network {
            router: Router::new(
                Arc::clone(&transport),
                Arc::clone(&pace),
                Arc::clone(&lease),
                Arc::clone(&reports),
                reach,
                Arc::clone(&refusals),
                Arc::clone(&config),
            ),
            group: id.clone(),
        };
        let lag_threshold = config.replication_lag_threshold;
        let raft = Raft::new(member.clone(), config, network, log.clone(), machine)
            .await
            .map_err(|err| OpenError::Raft(err.to_string()))?;
        // A sole voter's promise binds nobody else, so it stands at once
        // too. The newest set must
        // name this member alone, and the committed one no other voter:
        // while a change down to one voter is not committed, the others
        // still vote. The committed set is empty when the log holds only
        // the group's first set.
        let voter = member.clone();
        let sole_voter = raft
            .with_raft_state(move |state| {
                let sets = &state.membership_state;
                sets.effective().voter_ids().eq([voter.clone()])
                    && sets.committed().voter_ids().all(|id| id == voter)
            })
            .await
            .map_err(|err| OpenError::Raft(err.to_string()))?;
        let hold = match stored {
            Some(vote) => Hold {
                until: timers.now() + timers.election_max,
                campaigns: led || sole_voter,
                follows: parking_lot::Mutex::new(Some(vote).filter(|vote| vote.is_committed())),
            },
            None => Hold::default(),
        };
        let stop = CancellationToken::new();
        let leadership = Arc::new(Leadership::new(Arc::clone(&id.shard), host));
        // Ends on `stop`, or when the group's core is gone.
        tokio::spawn(lead::run(
            Arc::clone(&leadership),
            raft.clone(),
            commits.clone(),
            stop.clone(),
        ));
        Ok(Self {
            id,
            member,
            raft,
            log,
            pace,
            lease,
            hold,
            transport,
            reports,
            answered: Arc::default(),
            root,
            refusals,
            positions,
            written_waiters,
            flushed_waiters,
            commits,
            applied,
            copy,
            leadership,
            reached_before: AtomicBool::new(false),
            cache,
            incoming: Arc::default(),
            clock,
            #[cfg(test)]
            gate: Arc::default(),
            #[cfg(test)]
            apply_hold,
            target: tokio::sync::watch::Sender::default(),
            progress: Arc::default(),
            lag_threshold,
            ticks: AtomicU64::new(0),
            dropped_ticks: AtomicU64::new(0),
            leads_since: parking_lot::Mutex::new(None),
            stopped: AtomicBool::new(false),
            proposals: OnceLock::new(),
            client_writes: Arc::default(),
            stop,
            reads: Arc::default(),
            runtime: tokio::runtime::Handle::current(),
        })
    }

    /// Start the task that makes this group's membership match its
    /// target while this member leads. It ends on `stop`, or when the
    /// group's core is gone.
    pub(crate) fn drive(self: &Arc<Self>) {
        let driver = driver::Driver::new(
            Arc::clone(self),
            self.lag_threshold,
            driver::Retry::default(),
            Arc::clone(&self.progress),
        );
        self.runtime.spawn(driver::run(
            driver,
            self.target.subscribe(),
            self.leadership.changes(),
            self.stop.clone(),
        ));
    }

    /// The control plane's target for this group. Only a target newer than
    /// the one held replaces it. Returns whether it did.
    pub(crate) fn retarget(&self, target: Target) -> bool {
        self.target.send_if_modified(|held| {
            let newer = held
                .as_ref()
                .is_none_or(|held| target.version > held.version);
            if newer {
                *held = Some(target);
            }
            newer
        })
    }

    pub(crate) fn membership_progress(&self) -> MembershipProgress {
        self.progress.borrow().clone()
    }

    /// Form the group from its first member set. Every member of the set
    /// calls this with the same set. A member that has already voted or
    /// holds an entry is past this step, and the call does nothing.
    pub(crate) async fn initialize(&self, members: BTreeSet<MemberId>) -> Result<(), String> {
        // Each group gets its own copy of the names. A caller forming many
        // groups from one set would otherwise have every group's clones
        // count on the same three allocations, from every thread.
        let members: BTreeSet<MemberId> = members
            .iter()
            .map(|member| MemberId::new(&*member.broker, member.copy))
            .collect();
        match self.raft.initialize(members).await {
            Ok(()) | Err(RaftError::APIError(InitializeError::NotAllowed(_))) => Ok(()),
            Err(err) => Err(err.to_string()),
        }
    }

    /// One timer check: an election if the election timeout has passed, a
    /// heartbeat if one is due. Waits while the group's queue is full. A
    /// caller that gives up drops the future, which is counted.
    pub(crate) async fn tick(&self) {
        if self.stopped.load(Ordering::Relaxed) {
            return;
        }
        // A member that takes no vote does not ask for one, unless it
        // promised nobody. One handed the leadership meanwhile is ticked
        // too: its first tick as leader comes once `Pace::wrote` or
        // `Pace::unanswered` lowers `due` below the hold's end.
        if self.holds_votes() && !self.hold.campaigns && !self.leads() {
            self.pace.due.store(self.hold.until, Ordering::Relaxed);
            return;
        }
        self.ticks.fetch_add(1, Ordering::Relaxed);
        let mut dropped = Dropped(Some(&self.dropped_ticks));
        let ticked = self.raft.trigger().tick().await;
        dropped.0 = None;
        self.pace.due.store(self.next_due(), Ordering::Relaxed);
        if let Err(err) = ticked
            && !self.stopped.swap(true, Ordering::Relaxed)
        {
            tracing::warn!(group = %self.id, member = %self.member, error = %err, "a Raft group stopped and is no longer ticked");
        }
    }

    /// Whether this member started less than one election timeout ago with
    /// a vote on disk. See `docs/per-shard-raft.md`, "A restarted member
    /// grants no vote at first".
    pub(super) fn holds_votes(&self) -> bool {
        self.pace.timers.now() < self.hold.until
    }

    /// Mark this member as one a group reached an earlier copy of. Called
    /// before the member takes messages.
    pub(super) fn set_reached_before(&self) {
        self.reached_before.store(true, Ordering::Release);
    }

    pub(super) fn reached_before(&self) -> bool {
        self.reached_before.load(Ordering::Acquire)
    }

    /// Whether this group's member on `broker` refused the last call to it.
    pub(crate) fn refused_on(&self, broker: &str) -> bool {
        self.refusals.holds(broker)
    }

    /// If this member leads, retry its members on `broker` now rather than
    /// after openraft's next backoff delay. That broker just answered.
    pub(crate) async fn retry_now(&self, broker: &str) {
        use openraft::async_runtime::watch::WatchReceiver;
        let to: Vec<MemberId> = match &self.raft.metrics().borrow_watched().replication {
            Some(followers) => followers
                .keys()
                .filter(|member| &*member.broker == broker)
                .cloned()
                .collect(),
            None => return,
        };
        if !to.is_empty() {
            let _ = self.raft.trigger().reset_backoff(to).await;
        }
    }

    /// If this member leads, heartbeat now, whatever its gap.
    pub(crate) async fn heartbeat_now(&self) {
        if self.leads() {
            let _ = self.raft.trigger().heartbeat().await;
        }
    }

    /// Whether this member follows a leader it heard within its wait, or
    /// leads with a majority's answer within its followers' wait.
    fn leader_seems_live(&self) -> bool {
        use openraft::Instant as _;
        use openraft::async_runtime::watch::WatchReceiver;
        let now = self.pace.timers.now();
        if self.pace.follows_live_leader(now) {
            return true;
        }
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow_watched();
        let acked = metrics.last_quorum_acked.map(|acked| acked.elapsed());
        metrics.state == ServerState::Leader
            && acked.is_some_and(|acked| self.pace.leads_live(acked))
    }

    /// Whether this member, as leader, is still heard by its group: a
    /// majority answered it within its followers' wait. A term no majority
    /// has answered yet counts from when it was first seen here. openraft
    /// never steps a leader down for a lost majority, so without this a
    /// cut-off leader would serve readers for good. A sole voter needs
    /// nobody.
    fn leads_heard(&self, metrics: &openraft::RaftMetrics<ShardTypes>) -> bool {
        use openraft::Instant as _;
        if metrics.membership_config.membership().voter_ids().count() <= 1 {
            return true;
        }
        let now = self.pace.timers.now();
        let mut since = self.leads_since.lock();
        let term = metrics.current_term;
        let first = match *since {
            Some((led, at)) if led == term => at,
            _ => {
                *since = Some((term, now));
                now
            }
        };
        let age = match metrics.last_quorum_acked {
            Some(acked) => acked.elapsed(),
            None => Duration::from_millis(now.saturating_sub(first)),
        };
        self.pace.leads_live(age)
    }

    /// A pre-vote answer that grants nothing, naming this member's vote and
    /// last log id as openraft's own refusal would.
    fn refusal(&self) -> VoteResponse<ShardTypes> {
        use openraft::async_runtime::watch::WatchReceiver;
        let vote = self.raft.metrics().borrow_watched().vote.clone();
        let last = self.raft.data_metrics().borrow_watched().last_log;
        VoteResponse::new(vote, last, false)
    }

    fn leads(&self) -> bool {
        use openraft::async_runtime::watch::WatchReceiver;
        self.raft.metrics().borrow_watched().state == ServerState::Leader
    }

    /// When a timer check could next find something to do.
    pub(crate) fn due_at(&self) -> u64 {
        self.pace.due.load(Ordering::Relaxed)
    }

    /// When the next timer check can matter, after one was just sent.
    ///
    /// A leader's only timer is its heartbeat, at the short gap or the idle
    /// one. A voter's is its election timeout, which cannot pass until
    /// `election_min` after it last heard its leader, so a follower that
    /// keeps hearing one is never checked. A follower of an idle leader
    /// waits longer ([`Pace::follower_wait`]).
    /// Once that time has passed it is checked every interval, since the
    /// timeout openraft picked for it is not visible from here. A learner
    /// never stands, and is checked only so a change of role is seen.
    fn next_due(&self) -> u64 {
        use openraft::async_runtime::watch::WatchReceiver;
        let pace = &self.pace;
        let Timers {
            tick, election_min, ..
        } = pace.timers;
        let now = pace.timers.now();
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow_watched();
        match metrics.state {
            // One past the interval, so the check lands after the heartbeat
            // is due and not just before it. An entry not yet committed
            // keeps the short gap: it waits on a member that is down, and a
            // heartbeat is what finds that member back and retries it.
            ServerState::Leader => {
                let committed = metrics.local_committed.map(|log_id| log_id.index);
                let gap = match metrics.last_log_index > committed {
                    true => pace.timers.heartbeat,
                    false => pace.leader_gap(now),
                };
                now + gap + 1
            }
            ServerState::Follower | ServerState::Candidate => {
                let heard = pace.heard.load(Ordering::Relaxed);
                heard.saturating_add(pace.follower_wait()).max(now + tick)
            }
            ServerState::Learner | ServerState::Shutdown => now + election_min,
        }
    }

    /// `beat` is the gap a leader's append named. Taking the append or
    /// granting a vote restarts this member's election timer. `report` is
    /// the leader asking to be told once the append's entries are written.
    ///
    /// The timer restarts even if the caller stops waiting: openraft
    /// persists the append or vote either way, and a follower that keeps
    /// answering past the sender's deadline would otherwise stand for
    /// election while it still hears its leader. Past `deadline` it keeps
    /// its gap ([`Pace::took`]).
    pub(crate) async fn handle(
        self: &Arc<Self>,
        body: Request,
        beat: u64,
        report: bool,
        deadline: Duration,
    ) -> Result<Reply, Refused> {
        let group = Arc::clone(self);
        let arrived = self.pace.timers.now();
        // A granted pre-vote promises nothing, so it leaves the pace alone.
        let pre_vote = matches!(body, Request::PreVote(_));
        let handled = tokio::spawn(async move {
            let reply = group.answer(body, report).await?;
            match &reply {
                Reply::Append(Ok(_)) => group.pace.took(beat, arrived, deadline),
                Reply::Vote(vote) if vote.vote_granted && !pre_vote => group.pace.voted(),
                _ => {}
            }
            Ok(reply)
        });
        match handled.await {
            Ok(reply) => reply,
            Err(err) if err.is_panic() => std::panic::resume_unwind(err.into_panic()),
            Err(err) => Err(Refused::Stopped(
                Box::new(Addressee {
                    group: self.id.clone(),
                    to: self.member.clone(),
                }),
                err.to_string(),
            )),
        }
    }

    /// Answer `body`. When it is an append whose leader asked for a report,
    /// the report goes out as soon as the entries are written, while
    /// openraft's answer waits for their fsync. An answer that is ready
    /// first says more, and the report is dropped.
    async fn answer(&self, body: Request, report: bool) -> Result<Reply, Refused> {
        use openraft::entry::RaftEntry;
        use openraft::vote::RaftLeaderId;
        let asked = match &body {
            Request::Append(rpc) if report => rpc.entries.last().map(|last| {
                let last = last.log_id();
                let at = LogPoint {
                    term: last.leader_id.term,
                    index: last.index,
                };
                (rpc.vote.leader_id().node_id().clone(), at)
            }),
            _ => None,
        };
        let Some((leader, last)) = asked else {
            return self.dispatch(body).await;
        };
        let answer = self.dispatch(body);
        let tell = self.tell_written(leader, last);
        tokio::pin!(answer, tell);
        let mut told = false;
        loop {
            tokio::select! {
                biased;
                reply = &mut answer => return reply,
                () = &mut tell, if !told => told = true,
            }
        }
    }

    /// Tell `leader` this member's written position once it holds the
    /// leader's entry `last`: the id of the last entry the log has taken,
    /// before its fsync. A log that never gets there says nothing.
    async fn tell_written(&self, leader: MemberId, last: LogPoint) {
        let holds = |at: &LogPositions| {
            let written = at.written;
            written.filter(|at| at.term == last.term && at.index >= last.index)
        };
        let key = (last.term, last.index);
        let waited = self.written_waiters.wait_for(&self.positions, key, holds);
        let Ok(at) = waited.await else {
            return;
        };
        // Before the report leaves. Without the marker a power cut here would
        // keep the storage id, and the leader would go on counting a record
        // this copy lost.
        if let Some(root) = &self.root
            && let Err(err) = root.vouch_unsynced().await
        {
            tracing::debug!(group = %self.id, error = %err, "no written report: its marker did not sync");
            return;
        }
        let deadline = Duration::from_millis(self.pace.timers.heartbeat);
        let request = RaftRequest {
            group: self.id.clone(),
            to: leader,
            beat: 0,
            report: false,
            deadline,
            body: Request::Written {
                from: self.member.clone(),
                at,
            },
        };
        // A lost report costs nothing but the early acknowledgement: the
        // leader counts silence as not written and waits for the commit.
        let _ = tokio::time::timeout(deadline, self.transport.send(request)).await;
    }

    async fn dispatch(&self, body: Request) -> Result<Reply, Refused> {
        let stopped = |reason: String| {
            let member = Addressee {
                group: self.id.clone(),
                to: self.member.clone(),
            };
            Refused::Stopped(Box::new(member), reason)
        };
        // A term-0 pre-vote is the question a group is formed on.
        if let Request::PreVote(rpc) = &body
            && rpc.vote.leader_id.term == 0
            && self.reached_before()
        {
            return Err(stopped(REACHED_BEFORE.to_string()));
        }
        // openraft refuses a pre-vote only within its own lease of
        // `election_max` (2 s), which lapses just before each idle
        // heartbeat. A member that still counts on a live leader refuses it
        // for as long as it would wait for that leader itself. The refusal
        // is a real answer, so the candidate keeps the broker counted as
        // reachable and still learns a higher term or a longer log.
        if let Request::PreVote(_) = &body
            && self.leader_seems_live()
        {
            return Ok(Reply::Vote(self.refusal()));
        }
        let held = self.holds_votes();
        let asks = match &body {
            Request::Vote(rpc) => !(held && self.hold.lets_through(&rpc.vote)),
            Request::PreVote(_) => true,
            // Anything else openraft takes may replace the stored vote. A
            // cache snapshot is handed over at its last piece, below.
            Request::Append(rpc) => {
                self.hold.vote_may_change(Some(&rpc.vote));
                false
            }
            Request::TransferLeader(_) => {
                self.hold.vote_may_change(None);
                self.pace.forget_leader();
                false
            }
            Request::Snapshot { .. } | Request::Written { .. } => false,
        };
        if held && asks {
            // Dropped, not refused: the candidate asks again at its next
            // timeout.
            return Err(Refused::Unreachable(format!(
                "{} of {} has just started and takes no vote yet",
                self.member, self.id
            )));
        }
        // The lease is given up before openraft sees a message it takes,
        // or on the answer, before that leaves. A pre-vote changes no
        // vote and gives nothing up.
        match body {
            Request::Append(rpc) => {
                self.lease.named(term_of(&rpc.vote));
                self.raft
                    .append_entries(rpc)
                    .await
                    .map(Reply::Append)
                    .map_err(|err| stopped(err.to_string()))
            }
            Request::Vote(rpc) => {
                let answer = self.raft.vote(rpc).await;
                let answer = answer.map_err(|err| stopped(err.to_string()))?;
                // The vote this member holds now: the candidate's if it
                // granted, and still its own if it refused.
                self.lease.named(term_of(&answer.vote));
                Ok(Reply::Vote(answer))
            }
            Request::PreVote(rpc) => self
                .raft
                .pre_vote(rpc)
                .await
                .map(Reply::Vote)
                .map_err(|err| stopped(err.to_string())),
            Request::Snapshot {
                vote,
                meta,
                data_len,
                offset,
                data,
            } if self.id.shard.kind == ShardKind::Cache => {
                use openraft::async_runtime::watch::WatchReceiver;
                // Before the outvoted check: the sender's term ends the
                // ones below it either way, and an outvoted sender's term
                // is never above this member's, so its lease is untouched.
                self.lease.named(term_of(&vote));
                // A sender this member has outvoted is told so before any
                // of its snapshot is kept.
                let mine = self.raft.metrics().borrow_watched().vote.clone();
                if vote < mine {
                    return Ok(Reply::Snapshot(SnapshotResponse::new(mine)));
                }
                let piece = self.incoming.take(&vote, &meta, data_len, offset, &data);
                match piece.map_err(Refused::Unreachable)? {
                    Piece::More => {
                        if offset == 0 && self.incoming.watch() {
                            self.drop_incoming_when_idle();
                        }
                        Ok(Reply::Snapshot(SnapshotResponse::new(vote)))
                    }
                    Piece::Whole(snapshot) => {
                        // openraft takes a snapshot the state machine cannot
                        // restore as a storage failure and stops the member,
                        // so one this build cannot read (a newer format, a
                        // bad sender) is refused here and the member keeps
                        // running behind.
                        // The state parsed here is the one installed, so
                        // the bytes are read once.
                        let bytes = Bytes::from(snapshot);
                        let restored = match machine::restore(
                            bytes.clone(),
                            meta.last_log_id.is_some(),
                        )
                        .await
                        {
                            Ok(restored) => restored,
                            Err(err) => {
                                tracing::warn!(group = %self.id, member = %self.member, upto = ?meta.last_log_id, error = %err, "refused a snapshot this member cannot read");
                                return Err(Refused::Unreachable(format!(
                                    "a snapshot this member cannot read: {err}"
                                )));
                            }
                        };
                        let snapshot = SnapshotData {
                            bytes,
                            restored: Some(restored),
                        };
                        self.hold.vote_may_change(Some(&vote));
                        self.raft
                            .install_full_snapshot(vote, Snapshot { meta, snapshot })
                            .await
                            .map(Reply::Snapshot)
                            .map_err(|err| stopped(err.to_string()))
                    }
                }
            }
            // A stream group's snapshot carries no records, only the cut
            // batches its sender refuses by name, and installing one purges
            // the log up to it: the member would drop records it was never
            // sent. Refused until the records below it travel too.
            Request::Snapshot { vote, meta, .. } => {
                self.lease.named(term_of(&vote));
                let member = Box::new(Addressee {
                    group: self.id.clone(),
                    to: self.member.clone(),
                });
                tracing::error!(group = %self.id, member = %self.member, upto = ?meta.last_log_id, "refused a snapshot: taking it would purge records this member was not sent");
                Err(Refused::NoSnapshots(member))
            }
            Request::TransferLeader(req) => self
                .raft
                .handle_transfer_leader(req)
                .await
                .map(Reply::TransferLeader)
                .map_err(|err| stopped(err.to_string())),
            // Kept whatever this member's role is now: the count checks the
            // term on the log id, not when the report came.
            Request::Written { from, at } => {
                self.reports.heard(from, at);
                Ok(Reply::Written)
            }
        }
    }

    /// Queue `batch` behind every batch queued before it and answer where
    /// it landed, once its records are as far as `batch.ack` asks.
    ///
    /// The batch is queued by the call, not by the first poll of what it
    /// returns. Dropping that future takes nothing back: the batch is still
    /// written if this member leads, and the batches behind it keep their
    /// place. An error is an unknown outcome and must not be retried as if
    /// nothing was written.
    ///
    /// A cache shard takes no batch: its entries are commands stamped by
    /// its publish task, and a raw record would fail to apply on every
    /// member.
    pub(crate) fn propose(
        &self,
        batch: Batch,
    ) -> impl Future<Output = Result<Written, Unacknowledged>> + Send + use<> {
        let split = match self.cache {
            Some(_) => Err(propose::not_a_stream()),
            None => propose::split(batch),
        };
        self.queue(split)
    }

    fn queue(
        &self,
        split: Result<(Proposal, Waiting), CommitError>,
    ) -> impl Future<Output = Result<Written, Unacknowledged>> + Send + use<> {
        let waiting = if self.stop.is_cancelled() {
            Err(propose::stopped())
        } else {
            split.and_then(|(proposal, waiting)| {
                let queued = self.proposer().send(proposal);
                queued.map(|()| waiting).map_err(|_| propose::stopped())
            })
        };
        let positions = self.positions.clone();
        let flushed = self.flushed_waiters.clone();
        let leading = self.leadership.watch(&self.raft);
        let stop = self.stop.clone();
        async move { propose::acknowledged(waiting?, positions, flushed, leading, stop).await }
    }

    /// The member's copy, for deciding whether a kept copy of an earlier
    /// rotation is still needed.
    pub(crate) fn current_copy(&self) -> &felix_storage::raft_copy::CurrentCopy {
        &self.copy
    }

    /// Cancelled when the member is shut down.
    pub(crate) fn stopped(&self) -> CancellationToken {
        self.stop.clone()
    }

    /// The commit table as the state machine publishes it.
    pub(crate) fn commits(&self) -> tokio::sync::watch::Receiver<Commits> {
        self.commits.clone()
    }

    /// One past the last offset this member knows committed. It moves when
    /// the state machine applies an entry, so it never passes the commit
    /// index, on a leader or a follower.
    pub(crate) fn read_bound(&self) -> u64 {
        self.commits.borrow().read_bound()
    }

    /// See [`Commits::records_end`].
    pub(crate) fn records_end(&self, end: u64) -> u64 {
        self.commits.borrow().records_end(end)
    }

    /// Wait until `written` is committed: the commit index has passed it
    /// and the entry there has the term it was written at.
    pub(crate) fn await_commit(
        &self,
        written: Written,
        timeout: Duration,
    ) -> impl Future<Output = Result<(), CommitError>> + Send + use<> {
        commit::committed(
            written,
            self.commits.clone(),
            self.applied.clone(),
            self.leadership.watch(&self.raft),
            self.stop.clone(),
            timeout,
        )
    }

    /// Wait until a `Quorum` write may be acknowledged. With `AckAt::Flushed`
    /// that is its commit. With `AckAt::Written` it is the commit or a
    /// majority of the voters having written it, whichever is first. The
    /// written count acknowledges and does nothing else: the commit index
    /// and the read bound do not move on it.
    pub(crate) fn await_quorum(
        &self,
        written: Written,
        ack: AckAt,
        timeout: Duration,
    ) -> impl Future<Output = Result<(), CommitError>> + Send + use<> {
        let commit = self.await_commit(written, timeout);
        let early = (ack == AckAt::Written)
            .then(|| written.end.checked_sub(1))
            .flatten()
            .map(|index| LogPoint {
                term: written.generation,
                index,
            });
        let reports = Arc::clone(&self.reports);
        let answered = Arc::clone(&self.answered);
        let raft = self.raft.clone();
        let me = self.member.clone();
        let root = self.root.clone();
        async move {
            let Some(at) = early else {
                return commit.await;
            };
            // The written count includes this leader's own unsynced write,
            // so the marker syncs before the count is read. In this arm, so
            // a slow sync holds up neither the commit nor the timeout.
            let majority = async {
                if let Some(root) = &root
                    && root.vouch_unsynced().await.is_err()
                {
                    std::future::pending::<()>().await;
                }
                reports.majority(&raft, &me, at).await;
            };
            tokio::select! {
                biased;
                // First, so a member that no longer leads is refused even
                // with a majority's reports in hand.
                committed = commit => committed,
                () = majority => {
                    // Before the caller is told, so a read that starts
                    // after the answer waits for this entry.
                    let mut answered = answered.lock();
                    if answered.is_none_or(|was| (was.term, was.index) < (at.term, at.index)) {
                        *answered = Some(at);
                    }
                    Ok(())
                }
            }
        }
    }

    /// Confirm by a read-index round that this member still leads `term`,
    /// the term it had open before the reader took its value. The term
    /// open now is not enough: a member deposed and elected again since
    /// the read is open in a later term, and its value can miss what the
    /// leader in between committed.
    ///
    /// A majority that does not answer is a timeout, and so is a lapsed
    /// lease: neither says another leader exists, and neither closes the
    /// shard. Ok only once this member has applied the read index and
    /// still has `term` open, so a value the reader takes after this
    /// returns has every answered write.
    pub(crate) async fn confirm_read(&self, term: u64) -> Result<(), CommitError> {
        let patience = Duration::from_millis(self.pace.timers.election_max);
        let deadline = tokio::time::Instant::now() + patience;
        let bound = self.read_index(term).await?;
        // The read index is the commit index, and a write answered once a
        // majority had written it can be past it. It is of this term and
        // on a majority, so it commits unless this member stops leading.
        let answered = *self.answered.lock();
        let answered = answered
            .filter(|at| at.term == term)
            .map_or(0, |at| at.index + 1);
        let unanswered = CommitError::TimedOut {
            what: "read",
            timeout: patience,
        };
        self.applied_through(bound.max(answered) - 1, deadline, unanswered)
            .await?;
        // The wait can outlast the term: a member deposed during it still
        // applies the index as a follower.
        if self.open_term() != Some(term) {
            return Err(CommitError::LeadershipLost {
                what: "read",
                detail: "the shard closed while the read waited for its apply",
            });
        }
        Ok(())
    }

    /// One past the index a reader must have applied before it reads: the
    /// leader's commit index when a majority confirmed that this member
    /// still leads `term`. `confirm_read` then waits for the apply, bounded
    /// by `election_max` from its start; the cache read waits under the
    /// caller's deadline.
    async fn read_index(&self, term: u64) -> Result<u64, CommitError> {
        let lost = |detail| CommitError::LeadershipLost {
            what: "read",
            detail,
        };
        match self.open_term() {
            Some(open) if open == term => {}
            Some(_) => return Err(lost("the shard was opened again after the read began")),
            None => return Err(lost("this broker does not have the shard open")),
        }
        let timers = self.pace.timers;
        let patience = Duration::from_millis(timers.election_max);
        let unanswered = CommitError::TimedOut {
            what: "read",
            timeout: patience,
        };
        let deadline = tokio::time::Instant::now() + patience;
        loop {
            let raft = self.raft.clone();
            let stop = self.stop.clone();
            let entered =
                self.reads
                    .enter(&self.runtime, patience, Round::QuorumNotEnough, move || {
                        read_round(raft.clone(), stop.clone())
                    });
            // A read that arrives during a round waits for the next one too,
            // so the bound is on the wait, not on one round.
            let round = tokio::select! {
                biased;
                () = self.stop.cancelled() => return Err(propose::stopped()),
                round = tokio::time::timeout_at(deadline, entered.answer()) => match round {
                    Ok(Some(round)) => round,
                    // The task asking rounds ended without an answer. A new
                    // read starts a new task.
                    Ok(None) => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        continue;
                    }
                    Err(_) => return Err(unanswered),
                },
            };
            match round {
                Round::Leads { term: leads, index } if leads == term => return Ok(index),
                Round::Leads { .. } => {
                    return Err(lost(
                        "this broker leads a later term than the read began in",
                    ));
                }
                Round::LeaseExpired => {}
                Round::NotLeader => return Err(lost("this broker does not lead the shard")),
                Round::QuorumNotEnough => return Err(unanswered),
                Round::Stopped => return Err(propose::stopped()),
            }
            // The leader is still the leader. Ask for a heartbeat, wait for
            // a majority to answer it, and ask again.
            if tokio::time::Instant::now() >= deadline {
                return Err(unanswered);
            }
            let _ = self.raft.trigger().heartbeat().await;
            let answered = lead::quorum_answered(&self.raft);
            let again = Duration::from_millis(timers.heartbeat);
            let _ = tokio::time::timeout(again, answered).await;
        }
    }

    /// Wait until this member has applied `index`. A write can be answered
    /// after it commits but before it is applied here, and a value taken
    /// from below the read index would miss it. `timed_out` once `deadline`
    /// passes.
    async fn applied_through(
        &self,
        index: u64,
        deadline: tokio::time::Instant,
        timed_out: CommitError,
    ) -> Result<(), CommitError> {
        let applied = self.applied.wait_for(&self.commits, index, |commits| {
            commits
                .applied()
                .is_some_and(|applied| applied.index >= index)
                .then_some(())
        });
        tokio::select! {
            biased;
            () = self.stop.cancelled() => Err(propose::stopped()),
            waited = tokio::time::timeout_at(deadline, applied) => match waited {
                Ok(Ok(_)) => Ok(()),
                // The state machine is gone with the member.
                Ok(Err(_)) => Err(propose::stopped()),
                Err(_) => Err(timed_out),
            },
        }
    }

    /// A transfer of leadership away from this member is about to start.
    /// Gives up the lease and closes the shard for the rest of the term
    /// this member leads, before it returns that term. `None` if it leads
    /// none.
    pub(crate) async fn transfer_starting(&self) -> Option<u64> {
        self.transfer_starting_in(None).await
    }

    /// [`Self::transfer_starting`], and nothing is given up unless the
    /// term this member leads is `only`, when one is named.
    pub(super) async fn transfer_starting_in(&self, only: Option<u64>) -> Option<u64> {
        // Asked of the core, in the queue a transfer request goes through.
        let leads = self.raft.with_raft_state(move |state| {
            let leads = state.server_state == ServerState::Leader;
            let term = state.vote_ref().leader_id().term();
            (leads && only.is_none_or(|only| only == term)).then_some(term)
        });
        let leads = leads.await.ok().flatten();
        if let Some(term) = leads {
            self.lease.void_through(term);
        }
        self.leadership.transfer_starting(leads)
    }

    /// Hand this member's leadership to `to`. The lease is given up and
    /// the shard closed before openraft is asked. Returns the term given
    /// up, or `None` if this member leads none, and then nothing is asked.
    pub(crate) async fn transfer(&self, to: MemberId) -> Option<u64> {
        let term = self.transfer_starting().await?;
        // A core that has stopped has nothing to hand over.
        let _ = self.raft.trigger().transfer_leader(to).await;
        Some(term)
    }

    /// Whether a write of `term` may be acknowledged on this member's log
    /// alone: it leads `term`, a majority answered a message it handed off
    /// less than one lease ago, it has not given the term up, and it was
    /// not suspended since.
    pub(crate) fn admit_local_ack(&self, term: u64) -> Result<(), CommitError> {
        use openraft::Instant as _;
        use openraft::async_runtime::watch::WatchReceiver;
        let view = || {
            let metrics = self.raft.metrics();
            let metrics = metrics.borrow_watched();
            let leads = metrics.state == ServerState::Leader;
            View {
                leads: leads.then(|| metrics.vote.leader_id().term()),
                acked: metrics.last_quorum_acked.map(|acked| acked.elapsed()),
            }
        };
        self.lease.admit(term, view).map_err(propose::lost)
    }

    /// The term this member has the shard open in.
    pub(crate) fn open_term(&self) -> Option<u64> {
        let lead = self.leadership.lead();
        lead.term.filter(|_| lead.open)
    }

    #[cfg(test)]
    pub(crate) fn watcher_looks(&self) -> u64 {
        self.leadership.looks.load(Ordering::Relaxed)
    }

    fn proposer(&self) -> &mpsc::UnboundedSender<Proposal> {
        let timers = self.pace.timers;
        self.proposals.get_or_init(|| {
            let (queue, queued) = mpsc::unbounded_channel();
            // Ends on `stop`, or when this group and its queue are dropped.
            self.runtime.spawn(propose::run(
                self.raft.clone(),
                Arc::clone(&self.pace),
                queued,
                self.stop.clone(),
                Arc::clone(&self.client_writes),
                propose::Lease {
                    length: Duration::from_millis(timers.election_max),
                    again: Duration::from_millis(timers.heartbeat),
                },
                self.cache.clone().map(|cache| Stamper {
                    #[cfg(test)]
                    gate: Arc::clone(&self.gate),
                    cache,
                    leadership: Arc::clone(&self.leadership),
                    clock: Arc::clone(&self.clock),
                    last: 0,
                }),
            ));
            queue
        })
    }

    fn cache_group(&self) -> Result<&Cache, CommitError> {
        self.cache.as_ref().ok_or(CommitError::LeadershipLost {
            what: "cache request",
            detail: "the shard is not a cache shard",
        })
    }

    /// Stamp `writes`, commit them in order and return once this member
    /// has applied them. Nothing is written unless it has the shard open.
    async fn cache_commit(
        &self,
        writes: Vec<Unstamped>,
        timeout: Duration,
    ) -> Result<(), CommitError> {
        self.cache_group()?;
        let queued = self.queue(propose::split_cache(writes)).await;
        let written = queued.map_err(|unacked| unacked.error)?;
        // The state machine folds an entry in before it publishes it.
        self.await_commit(written, timeout).await
    }

    /// Commit `command` and return what it answered when it was first
    /// applied, so a retry gets the first answer. A put with a `ttl`
    /// (microseconds, zero for none) expires that long after its stamp.
    ///
    /// [`Answer::Forgotten`] means the request is below its producer's
    /// floor and its answer is gone: it was applied by an earlier entry, or
    /// a later entry of the same producer (a higher floor, a retire) dropped
    /// the answer before this read. Either way it was applied once, provided
    /// the caller follows the floor rule: a request's `floor` is the lowest
    /// sequence the producer has no answer for, never higher, and a retire's
    /// is one past the producer's last sequence once every request has its
    /// answer. A floor past an unanswered request turns that request into
    /// Forgotten without applying it.
    pub(crate) async fn cache_write(
        &self,
        command: CacheCommand,
        ttl: u64,
        timeout: Duration,
    ) -> Result<Answer, CommitError> {
        let cache = self.cache_group()?;
        let request = match &command {
            CacheCommand::Put { request, .. }
            | CacheCommand::Delete { request, .. }
            | CacheCommand::Add { request, .. } => Some(*request),
            CacheCommand::Expire { .. } | CacheCommand::Retire { .. } => None,
        };
        self.cache_commit(vec![Unstamped { command, ttl }], timeout)
            .await?;
        let answer = request.and_then(|request| {
            let cache = cache.read();
            cache.answer(request.producer, request.sequence)
        });
        Ok(answer.unwrap_or(Answer::Forgotten))
    }

    /// Returns once this member is known to lead and has applied
    /// everything committed when a majority said so. A read of its state
    /// taken then misses no acknowledged write.
    async fn cache_fresh(
        &self,
        deadline: tokio::time::Instant,
        timeout: Duration,
    ) -> Result<&Cache, CommitError> {
        let cache = self.cache_group()?;
        let lost = CommitError::LeadershipLost {
            what: "read",
            detail: "this broker does not have the shard open",
        };
        let term = self.open_term().ok_or(lost)?;
        let bound = self.read_index(term).await?;
        // Applied through `bound - 1`; a bound of 0 holds at once.
        let index = bound.saturating_sub(1);
        let applied = self.applied.wait_for(&self.commits, index, |commits| {
            (commits.read_bound() >= bound).then_some(())
        });
        tokio::select! {
            biased;
            () = self.stop.cancelled() => Err(propose::stopped()),
            applied = applied => applied.map(|_| cache).map_err(|_| propose::stopped()),
            () = tokio::time::sleep_until(deadline) => Err(CommitError::TimedOut {
                what: "read",
                timeout,
            }),
        }
    }

    /// `key`'s value and version, if it has an unexpired one.
    ///
    /// Expiry is judged at the later of this leader's clock and the log's
    /// time. When only the clock says the value expired, the delete is
    /// committed before the reader is told it is gone, so no later leader
    /// with a slower clock finds the key again.
    pub(crate) async fn cache_read(
        &self,
        key: &str,
        timeout: Duration,
    ) -> Result<Option<(Bytes, u64)>, CommitError> {
        use super::cache::Read;
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let cache = self.cache_fresh(deadline, timeout).await?;
            let read = cache.read().read(key, (self.clock)());
            let version = match read {
                Read::Value { value, version } => return Ok(Some((value, version))),
                Read::Absent => return Ok(None),
                Read::Expire { version } => version,
            };
            let expire = CacheCommand::Expire {
                key: key.to_owned(),
                version,
            };
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            let writes = vec![Unstamped {
                command: expire,
                ttl: 0,
            }];
            self.cache_commit(writes, left).await?;
        }
    }

    /// A counter's sum, if it was ever added to.
    pub(crate) async fn counter_read(
        &self,
        key: &str,
        timeout: Duration,
    ) -> Result<Option<i64>, CommitError> {
        let deadline = tokio::time::Instant::now() + timeout;
        let cache = self.cache_fresh(deadline, timeout).await?;
        Ok(cache.read().sum(key))
    }

    /// Delete what has expired by this leader's clock, and say how long
    /// until the next value does. `None` if nothing is waiting to expire
    /// or this member does not have the shard open.
    async fn expire(&self) -> Option<Duration> {
        let cache = self.cache.as_ref()?;
        self.open_term()?;
        let timers = self.pace.timers;
        let due = cache.read().due((self.clock)(), EXPIRE_AT_ONCE);
        if !due.is_empty() {
            let writes = due
                .into_iter()
                .map(|(key, version)| Unstamped {
                    command: CacheCommand::Expire { key, version },
                    ttl: 0,
                })
                .collect();
            let patience = Duration::from_millis(timers.election_max);
            if self.cache_commit(writes, patience).await.is_err() {
                // Deposed, or no majority in reach. Look again later.
                return Some(Duration::from_millis(timers.heartbeat));
            }
        }
        let cache = cache.read();
        let now = (self.clock)().max(cache.time());
        let next = cache.next_expiry()?;
        Some(Duration::from_micros(next.saturating_sub(now)))
    }

    /// Start a cache shard's expiry task: on the leader, it deletes each
    /// value when it expires. It wakes when leadership changes, when an
    /// entry is applied and when the next value is due, so an idle group
    /// with nothing to expire costs nothing. A stream shard has none.
    pub(crate) fn start_expiry(self: &Arc<Self>) {
        if self.cache.is_none() {
            return;
        }
        let group = Arc::downgrade(self);
        let mut lead = self.leadership.changes();
        let mut commits = self.commits.clone();
        let stop = self.stop.clone();
        self.runtime.spawn(async move {
            loop {
                let wait = match group.upgrade() {
                    Some(group) => group.expire().await,
                    None => return,
                };
                let due = async {
                    match wait {
                        Some(wait) => tokio::time::sleep(wait).await,
                        None => std::future::pending().await,
                    }
                };
                tokio::select! {
                    biased;
                    () = stop.cancelled() => return,
                    changed = lead.changed() => if changed.is_err() { return },
                    changed = commits.changed() => if changed.is_err() { return },
                    () = due => {}
                }
            }
        });
    }

    #[cfg(test)]
    pub(crate) fn incoming(&self) -> &Incoming {
        &self.incoming
    }

    /// A sender that goes away part way must not leave its pieces held.
    /// One task at a time, however often senders start again.
    fn drop_incoming_when_idle(&self) {
        let incoming = Arc::clone(&self.incoming);
        let stop = self.stop.clone();
        self.runtime.spawn(async move {
            loop {
                tokio::select! {
                    () = stop.cancelled() => return,
                    () = tokio::time::sleep(install::IDLE) => {}
                }
                if incoming.settled() {
                    return;
                }
            }
        });
    }

    /// The data root's mark log, when the member has one.
    pub(crate) fn mark_root(&self) -> Option<&MarkLog> {
        self.root.as_ref()
    }

    /// Stop the member's tasks. Entries and the vote it acknowledged are
    /// already on disk; nothing is written here.
    pub(crate) async fn shutdown(&self) {
        self.stop.cancel();
        if let Err(err) = self.raft.shutdown().await {
            tracing::warn!(group = %self.id, member = %self.member, error = %err, "a Raft group did not stop cleanly");
        }
        // Before the caller closes the log, so each queued flush gets its
        // real result. A hung disk does not hold the stop.
        match tokio::time::timeout(DRAIN_LIMIT, self.log.drain_flushes()).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                tracing::warn!(group = %self.id, member = %self.member, error = %err, "a stopped member's queued flushes failed");
            }
            Err(_) => {
                tracing::warn!(group = %self.id, member = %self.member, "a stopped member's queued flushes timed out");
            }
        }
    }

    pub(crate) fn status(&self) -> GroupStatus {
        use openraft::async_runtime::watch::WatchReceiver;
        let metrics = self.raft.metrics().borrow_watched().clone();
        let role = if metrics.running_state.is_err() {
            Role::Stopped
        } else {
            match metrics.state {
                ServerState::Leader => Role::Leader,
                ServerState::Candidate => Role::Candidate,
                ServerState::Follower => Role::Follower,
                ServerState::Learner => Role::Learner,
                ServerState::Shutdown => Role::Stopped,
            }
        };
        // Borrowed once. A second borrow while the first is held waits behind
        // an append that publishes in between, and that append behind the
        // first: the caller and the member's log would both stop for good.
        let positions = *self.positions.borrow();
        // Pre-vote keeps a cut-off member at its term, so openraft names the
        // last leader forever. A follower counts on it only as long as it
        // would refuse a pre-vote for it.
        let live = match metrics.state {
            ServerState::Leader => self.leads_heard(&metrics),
            _ => self.pace.follows_live_leader(self.pace.timers.now()),
        };
        let leader_hint = metrics.current_leader.clone();
        GroupStatus {
            member: self.member.clone(),
            role,
            term: metrics.current_term,
            leader: leader_hint.clone().filter(|_| live),
            leader_hint,
            last_index: metrics.last_log_index,
            written: positions.written,
            flushed: positions.flushed,
            committed: metrics.local_committed.map(|id| id.index),
            ticks: self.ticks.load(Ordering::Relaxed),
            dropped_ticks: self.dropped_ticks.load(Ordering::Relaxed),
            client_writes: self.client_writes.load(Ordering::Relaxed),
            membership: self.membership_progress(),
        }
    }

    /// The committed log id, while this member leads its group.
    pub(crate) fn led_commit(&self) -> Option<LogPoint> {
        use openraft::async_runtime::watch::WatchReceiver;
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow_watched();
        if metrics.running_state.is_err() || metrics.state != ServerState::Leader {
            return None;
        }
        metrics.local_committed.as_ref().map(|id| LogPoint {
            term: **id.committed_leader_id(),
            index: id.index,
        })
    }

    pub(crate) fn voters(&self) -> BTreeSet<MemberId> {
        use openraft::async_runtime::watch::WatchReceiver;
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow_watched();
        metrics.membership_config.membership().voter_ids().collect()
    }

    /// Every member of the latest membership this member holds, voters and
    /// learners.
    pub(crate) fn members(&self) -> BTreeSet<MemberId> {
        use openraft::async_runtime::watch::WatchReceiver;
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow_watched();
        let membership = metrics.membership_config.membership();
        membership
            .voter_ids()
            .chain(membership.learner_ids())
            .collect()
    }

    pub(crate) fn positions(&self) -> tokio::sync::watch::Receiver<LogPositions> {
        self.positions.clone()
    }

    /// When this member last took an append or granted a vote.
    #[cfg(test)]
    pub(crate) fn heard(&self) -> u64 {
        self.pace.heard.load(Ordering::Relaxed)
    }

    /// How long ago this member last took an append, and how long it
    /// waits for its leader after one.
    #[cfg(test)]
    pub(crate) fn led_age(&self) -> (u64, u64) {
        let led = self.pace.led.load(Ordering::Relaxed);
        let age = self.pace.timers.now().saturating_sub(led);
        (age, self.pace.follower_wait())
    }

    /// How long this member, as a leader, would wait for its next heartbeat.
    #[cfg(test)]
    pub(crate) fn leader_gap(&self) -> u64 {
        self.pace.leader_gap(self.pace.timers.now())
    }

    #[cfg(test)]
    pub(crate) fn write_in_term(
        &self,
        command: super::types::Command,
        term: u64,
    ) -> impl Future<Output = bool> + use<> {
        use futures::StreamExt;
        let raft = self.raft.clone();
        async move {
            let sent = propose::write(&raft, vec![command], Some(term)).await;
            let mut results = sent.expect("running");
            matches!(results.next().await, Some(Ok(Ok(_))))
        }
    }

    /// Queue `batch` as [`Self::propose`] does, without its check of the
    /// shard's kind, to reach the publish task's own check.
    #[cfg(test)]
    pub(crate) fn propose_unchecked(
        &self,
        batch: Batch,
    ) -> impl Future<Output = Result<Written, Unacknowledged>> + Send + use<> {
        self.queue(propose::split(batch))
    }

    #[cfg(test)]
    pub(crate) fn cache(&self) -> &Cache {
        self.cache.as_ref().expect("a cache group")
    }

    #[cfg(test)]
    pub(crate) fn lease(&self) -> &Lease {
        &self.lease
    }

    #[cfg(test)]
    pub(crate) fn reports(&self) -> &Reports {
        &self.reports
    }

    #[cfg(test)]
    pub(crate) fn read_rounds_asked(&self) -> u64 {
        self.reads.asked()
    }

    #[cfg(any(test, feature = "testing"))]
    pub(crate) fn raft(&self) -> &Raft<ShardTypes, Machine> {
        &self.raft
    }
}

/// One read-index round, asked of the core after every read that shares it
/// arrived.
async fn read_round(raft: Raft<ShardTypes, Machine>, stop: CancellationToken) -> Round {
    let round = tokio::select! {
        biased;
        () = stop.cancelled() => return Round::Stopped,
        round = raft.get_read_log_id(ReadPolicy::ReadIndex) => round,
    };
    match round {
        Ok((read, _)) => Round::Leads {
            term: **read.committed_leader_id(),
            index: read.index() + 1,
        },
        Err(RaftError::APIError(LinearizableReadError::ForwardToLeader(forward)))
            if forward.reason == ForwardReason::LeaseExpired =>
        {
            Round::LeaseExpired
        }
        Err(RaftError::APIError(LinearizableReadError::ForwardToLeader(_))) => Round::NotLeader,
        Err(RaftError::APIError(LinearizableReadError::QuorumNotEnough(_))) => {
            Round::QuorumNotEnough
        }
        Err(RaftError::Fatal(_)) => Round::Stopped,
    }
}
