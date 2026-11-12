//! One member of one group: its openraft instance over the shard's log.

use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use felix_storage::DiskLog;
use felix_storage::raft_log::{LogPositions, RaftLog, SnapshotStore};
use openraft::error::{InitializeError, RaftError};
use openraft::{Config, Raft, ServerState};

use super::codec::ShardCodec;
use super::machine::Machine;
use super::network::{Addressee, Network, RaftTransport, Refused, Reply, Request, Router};
use super::runtime::{GroupStatus, OpenError, Role};
use super::types::{GroupId, MemberId, ShardTypes};

pub(crate) struct Group {
    pub(crate) id: GroupId,
    pub(crate) member: MemberId,
    raft: Raft<ShardTypes, Machine>,
    pace: Arc<Pace>,
    /// What the log holds written and flushed, as the adapter reports it.
    positions: tokio::sync::watch::Receiver<LogPositions>,
    /// Timer checks the group was sent.
    ticks: AtomicU64,
    /// Timer checks the ticker gave up on because the group had not taken
    /// them by the next interval.
    dropped_ticks: AtomicU64,
    /// Set once a timer check finds the group's core gone.
    stopped: AtomicBool,
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
}

impl Timers {
    pub(crate) fn now(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
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
/// leader with an impatient follower. See `docs/per-shard-raft.md`, "Idle
/// groups".
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
    /// As a leader: when it last sent an entry to anyone.
    wrote: AtomicU64,
    peers: parking_lot::Mutex<Vec<Peer>>,
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
}

/// An append on its way to a peer, for [`Pace::answered`].
pub(crate) struct Sent {
    pub(crate) gap: u64,
    wrote: u64,
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
            wrote: AtomicU64::new(now),
            peers: parking_lot::Mutex::new(Vec::new()),
            jitter: hasher.finish() % (timers.election_max - timers.election_min).max(1),
        }
    }

    /// The leader is sending `to` an append. The gap it names is the idle
    /// one once no entry has gone out for `idle_after`.
    pub(crate) fn sending(&self, to: &MemberId, entries: bool) -> Sent {
        let timers = &self.timers;
        let now = timers.now();
        if entries {
            self.wrote.store(now, Ordering::Relaxed);
            // The followers hear the short gap in this message.
            self.due
                .fetch_min(now + timers.heartbeat, Ordering::Relaxed);
        }
        let wrote = self.wrote.load(Ordering::Relaxed);
        let mut peers = self.peers.lock();
        if !peers.iter().any(|peer| &peer.id == to) {
            peers.push(Peer {
                id: to.clone(),
                idle_under: None,
                answered: now,
            });
        }
        let gap = match now >= wrote + timers.idle_after {
            true => timers.idle_heartbeat,
            false => timers.heartbeat,
        };
        Sent { gap, wrote }
    }

    /// `to` took the append: it now waits by the gap the append named.
    pub(crate) fn answered(&self, to: &MemberId, sent: &Sent) {
        let now = self.timers.now();
        if let Some(peer) = self.peers.lock().iter_mut().find(|peer| &peer.id == to) {
            peer.answered = now;
            if sent.gap > self.timers.heartbeat {
                peer.idle_under = Some(sent.wrote);
            }
        }
    }

    /// How long a leader waits until its next heartbeat.
    pub(super) fn leader_gap(&self, now: u64) -> u64 {
        let timers = &self.timers;
        let wrote = self.wrote.load(Ordering::Relaxed);
        if timers.idle_heartbeat <= timers.heartbeat || now < wrote + timers.idle_after {
            return timers.heartbeat;
        }
        // A peer silent this long has timed out whatever it was told, so it
        // no longer holds the leader to the short gap.
        let silent = timers.election_min + 2 * timers.election_max;
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
        timers.election_min.max(3 * promised) + self.jitter
    }

    /// This member took an append from its leader, which named `gap`.
    pub(super) fn followed(&self, gap: Option<u64>) {
        let timers = &self.timers;
        let now = timers.now();
        if let Some(gap) = gap {
            // An unknown gap is the short one: checked early is safe.
            let gap = if gap == 0 { timers.heartbeat } else { gap };
            self.promised.store(gap, Ordering::Relaxed);
        }
        self.heard.store(now, Ordering::Relaxed);
        self.due
            .store(now + self.follower_wait(), Ordering::Relaxed);
    }

    /// This member granted a vote. Whoever wins starts at the short gap.
    pub(super) fn voted(&self) {
        self.promised
            .store(self.timers.heartbeat, Ordering::Relaxed);
        self.followed(None);
    }
}

/// Counts a timer check that was dropped before the group took it.
struct Dropped<'a>(Option<&'a AtomicU64>);

impl Drop for Dropped<'_> {
    fn drop(&mut self) {
        if let Some(dropped) = self.0 {
            dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl Group {
    /// Start `member` on `log`. The log decides where the member resumes: an
    /// empty one is a member that has never voted.
    pub(crate) async fn open(
        id: GroupId,
        member: MemberId,
        log: DiskLog,
        config: Arc<Config>,
        transport: Arc<dyn RaftTransport>,
        timers: Timers,
    ) -> Result<Self, OpenError> {
        let snapshots = SnapshotStore::open(log.dir())?;
        let log = RaftLog::<ShardTypes, ShardCodec>::new(log, ShardCodec)?;
        // A shard's state is its log, so nothing can rebuild what a purge
        // past the snapshot would drop.
        log.purge_within(&snapshots)?;
        let positions = log.positions();
        let machine = Machine::open(snapshots).await?;
        let pace = Arc::new(Pace::new(timers, &id, &member));
        let network = Network {
            router: Router::new(transport, Arc::clone(&pace)),
            group: id.clone(),
        };
        let raft = Raft::new(member.clone(), config, network, log, machine)
            .await
            .map_err(|err| OpenError::Raft(err.to_string()))?;
        Ok(Self {
            id,
            member,
            raft,
            pace,
            positions,
            ticks: AtomicU64::new(0),
            dropped_ticks: AtomicU64::new(0),
            stopped: AtomicBool::new(false),
        })
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
        match self.raft.metrics().borrow_watched().state {
            // One past the interval, so the check lands after the heartbeat
            // is due and not just before it.
            ServerState::Leader => now + pace.leader_gap(now) + 1,
            ServerState::Follower | ServerState::Candidate => {
                (pace.heard.load(Ordering::Relaxed) + pace.follower_wait()).max(now + tick)
            }
            ServerState::Learner | ServerState::Shutdown => now + election_min,
        }
    }

    /// `beat` is the gap a leader's append named. Taking the append or
    /// granting a vote restarts this member's election timer.
    pub(crate) async fn handle(&self, body: Request, beat: u64) -> Result<Reply, Refused> {
        let reply = self.dispatch(body).await?;
        match &reply {
            Reply::Append(Ok(_)) => self.pace.followed(Some(beat)),
            Reply::Snapshot(_) => self.pace.followed(None),
            Reply::Vote(vote) if vote.vote_granted => self.pace.voted(),
            _ => {}
        }
        Ok(reply)
    }

    async fn dispatch(&self, body: Request) -> Result<Reply, Refused> {
        let stopped = |reason: String| {
            let member = Addressee {
                group: self.id.clone(),
                to: self.member.clone(),
            };
            Refused::Stopped(Box::new(member), reason)
        };
        match body {
            Request::Append(rpc) => self
                .raft
                .append_entries(rpc)
                .await
                .map(Reply::Append)
                .map_err(|err| stopped(err.to_string())),
            Request::Vote(rpc) => self
                .raft
                .vote(rpc)
                .await
                .map(Reply::Vote)
                .map_err(|err| stopped(err.to_string())),
            Request::PreVote(rpc) => self
                .raft
                .pre_vote(rpc)
                .await
                .map(Reply::Vote)
                .map_err(|err| stopped(err.to_string())),
            Request::Snapshot { vote, meta } => {
                let snapshot = openraft::type_config::alias::SnapshotOf::<ShardTypes, Vec<u8>> {
                    meta,
                    snapshot: Vec::new(),
                };
                self.raft
                    .install_full_snapshot(vote, snapshot)
                    .await
                    .map(Reply::Snapshot)
                    .map_err(|err| stopped(err.to_string()))
            }
            Request::TransferLeader(req) => self
                .raft
                .handle_transfer_leader(req)
                .await
                .map(Reply::TransferLeader)
                .map_err(|err| stopped(err.to_string())),
        }
    }

    /// Stop the member's tasks. Entries and the vote it acknowledged are
    /// already on disk; nothing is written here.
    pub(crate) async fn shutdown(&self) {
        if let Err(err) = self.raft.shutdown().await {
            tracing::warn!(group = %self.id, member = %self.member, error = %err, "a Raft group did not stop cleanly");
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
        GroupStatus {
            member: self.member.clone(),
            role,
            term: metrics.current_term,
            leader: metrics.current_leader,
            last_index: metrics.last_log_index,
            written: self.positions.borrow().written,
            flushed: self.positions.borrow().flushed,
            committed: metrics.local_committed.map(|id| id.index),
            ticks: self.ticks.load(Ordering::Relaxed),
            dropped_ticks: self.dropped_ticks.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn positions(&self) -> tokio::sync::watch::Receiver<LogPositions> {
        self.positions.clone()
    }

    #[cfg(test)]
    pub(crate) fn raft(&self) -> &Raft<ShardTypes, Machine> {
        &self.raft
    }
}
