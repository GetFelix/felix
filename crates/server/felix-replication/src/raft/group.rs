//! One member of one group: its openraft instance over the shard's log.

use std::collections::BTreeSet;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use felix_storage::DiskLog;
use felix_storage::raft_log::{LogPositions, RaftLog, SnapshotStore};
use openraft::error::{ForwardReason, InitializeError, LinearizableReadError, RaftError};
use openraft::raft::SnapshotResponse;
use openraft::storage::Snapshot;
use openraft::{Config, Raft, ReadPolicy, ServerState};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use bytes::Bytes;

use super::cache::{Answer, CacheCommand};
use super::codec::ShardCodec;
use super::commit::{self, Commits};
use super::install::{self, Incoming, Piece};
use super::lead::{self, Leadership};
use super::machine::{self, Cache, Machine, SnapshotData};
use super::network::{Addressee, Network, RaftTransport, Refused, Reply, Request, Router};
use super::propose::{self, Batch, Clock, Proposal, Stamper, Unstamped, Waiting};
use super::runtime::{GroupStatus, OpenError, Role};
use super::types::{GroupId, MemberId, ShardTypes};
use crate::ShardKind;
use crate::engine::{CommitError, EngineHost, Written};

pub(crate) struct Group {
    pub(crate) id: GroupId,
    pub(crate) member: MemberId,
    raft: Raft<ShardTypes, Machine>,
    pace: Arc<Pace>,
    /// What the log holds written and flushed, as the adapter reports it.
    positions: tokio::sync::watch::Receiver<LogPositions>,
    /// What this member has applied, as its state machine reports it.
    commits: tokio::sync::watch::Receiver<Commits>,
    /// Whether this member leads and the shard is open.
    leadership: Arc<Leadership>,
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
    /// Timer checks the group was sent.
    ticks: AtomicU64,
    /// Timer checks the ticker gave up on because the group had not taken
    /// them by the next interval.
    dropped_ticks: AtomicU64,
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
/// gap it was told, so an append it does not take voids its answer. See
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
        if entries {
            // The followers hear the short gap in this message.
            self.wrote();
        }
        let now = timers.now();
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
    pub(crate) fn answered(&self, to: &MemberId, sent: &Sent) {
        let now = self.timers.now();
        if let Some(peer) = self.peers.lock().iter_mut().find(|peer| &peer.id == to) {
            peer.answered = now;
            if sent.gap > self.timers.heartbeat {
                peer.idle_under = Some(sent.wrote);
            }
        }
    }

    /// `to` did not take the append: it was lost, refused or rejected. The
    /// peer may have restarted and forgotten the idle gap, so the leader
    /// goes back to the short one until it answers again.
    pub(crate) fn unanswered(&self, to: &MemberId) {
        let timers = &self.timers;
        let now = timers.now();
        let mut peers = self.peers.lock();
        let Some(peer) = peers.iter_mut().find(|peer| &peer.id == to) else {
            return;
        };
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

    /// This member granted a vote. Whoever wins starts at the short gap.
    pub(super) fn voted(&self) {
        self.promised
            .store(self.timers.heartbeat, Ordering::Relaxed);
        self.followed(None);
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

/// What a member is told and asks of the broker it runs on.
pub(crate) struct Host {
    /// Who is told when the shard opens and closes here.
    pub(crate) engine: Option<Arc<dyn EngineHost>>,
    pub(crate) clock: Clock,
}

/// Expired values deleted by one pass of the expiry task.
const EXPIRE_AT_ONCE: usize = 256;

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
        host: Host,
    ) -> Result<Self, OpenError> {
        let Host {
            engine: host,
            clock,
        } = host;
        let snapshots = SnapshotStore::open(log.dir())?;
        let log = RaftLog::<ShardTypes, ShardCodec>::new(log, ShardCodec)?;
        // A stream shard's state is its log, and a cache shard's is its
        // snapshot plus the log above it. Either way nothing can rebuild
        // what a purge past the snapshot would drop.
        log.purge_within(&snapshots)?;
        let positions = log.positions();
        let (machine, commits, cache) = Machine::open(snapshots, id.shard.kind).await?;
        #[cfg(test)]
        let apply_hold = Arc::clone(&machine.hold);
        let pace = Arc::new(Pace::new(timers, &id, &member));
        let network = Network {
            router: Router::new(transport, Arc::clone(&pace)),
            group: id.clone(),
        };
        let raft = Raft::new(member.clone(), config, network, log, machine)
            .await
            .map_err(|err| OpenError::Raft(err.to_string()))?;
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
            pace,
            positions,
            commits,
            leadership,
            cache,
            incoming: Arc::default(),
            clock,
            #[cfg(test)]
            gate: Arc::default(),
            #[cfg(test)]
            apply_hold,
            ticks: AtomicU64::new(0),
            dropped_ticks: AtomicU64::new(0),
            stopped: AtomicBool::new(false),
            proposals: OnceLock::new(),
            client_writes: Arc::default(),
            stop,
            runtime: tokio::runtime::Handle::current(),
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
                let heard = pace.heard.load(Ordering::Relaxed);
                heard.saturating_add(pace.follower_wait()).max(now + tick)
            }
            ServerState::Learner | ServerState::Shutdown => now + election_min,
        }
    }

    /// `beat` is the gap a leader's append named. Taking the append or
    /// granting a vote restarts this member's election timer.
    ///
    /// The timer restarts even if the caller stops waiting: openraft
    /// persists the append or vote either way, and a follower that keeps
    /// answering past the sender's deadline would otherwise stand for
    /// election while it still hears its leader.
    pub(crate) async fn handle(
        self: &Arc<Self>,
        body: Request,
        beat: u64,
    ) -> Result<Reply, Refused> {
        let group = Arc::clone(self);
        let handled = tokio::spawn(async move {
            let reply = group.dispatch(body).await?;
            match &reply {
                Reply::Append(Ok(_)) => group.pace.followed(Some(beat)),
                Reply::Vote(vote) if vote.vote_granted => group.pace.voted(),
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
            Request::Snapshot {
                vote,
                meta,
                data_len,
                offset,
                data,
            } if self.id.shard.kind == ShardKind::Cache => {
                // A sender this member has outvoted is told so before any
                // of its snapshot is kept.
                use openraft::async_runtime::watch::WatchReceiver;
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
                        self.raft
                            .install_full_snapshot(vote, Snapshot { meta, snapshot })
                            .await
                            .map(Reply::Snapshot)
                            .map_err(|err| stopped(err.to_string()))
                    }
                }
            }
            // A stream group's snapshot carries no records, and installing
            // one purges the log up to it: the member would drop records it
            // was never sent. Refused until retention says how a member
            // that far behind gets them.
            Request::Snapshot { meta, .. } => {
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
    ) -> impl Future<Output = Result<Written, CommitError>> + Send + use<> {
        let split = match self.cache {
            Some(_) => Err(propose::not_a_stream()),
            None => propose::split(batch),
        };
        self.queue(split)
    }

    fn queue(
        &self,
        split: Result<(Proposal, Waiting), CommitError>,
    ) -> impl Future<Output = Result<Written, CommitError>> + Send + use<> {
        let waiting = if self.stop.is_cancelled() {
            Err(propose::stopped())
        } else {
            split.and_then(|(proposal, waiting)| {
                let queued = self.proposer().send(proposal);
                queued.map(|()| waiting).map_err(|_| propose::stopped())
            })
        };
        let positions = self.positions.clone();
        let leading = self.leadership.watch(&self.raft);
        let stop = self.stop.clone();
        async move { propose::acknowledged(waiting?, positions, leading, stop).await }
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
            self.leadership.watch(&self.raft),
            self.stop.clone(),
            timeout,
        )
    }

    /// Confirm by a read-index round that this member still leads `term`,
    /// the term it had open before the reader took its value. The term
    /// open now is not enough: a member deposed and elected again since
    /// the read is open in a later term, and its value can miss what the
    /// leader in between committed.
    ///
    /// A majority that does not answer is a timeout, and so is a lapsed
    /// lease: neither says another leader exists, and neither closes the
    /// shard.
    pub(crate) async fn confirm_read(&self, term: u64) -> Result<(), CommitError> {
        self.read_index(term).await.map(drop)
    }

    /// [`Self::confirm_read`], and one past the index a reader must have
    /// applied before it reads: the leader's commit index when a majority
    /// confirmed it.
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
            let round = tokio::select! {
                biased;
                () = self.stop.cancelled() => return Err(propose::stopped()),
                round = self.raft.get_read_log_id(ReadPolicy::ReadIndex) => round,
            };
            let forward = match round {
                Ok((read, _)) if **read.committed_leader_id() == term => {
                    return Ok(read.index() + 1);
                }
                Ok(_) => {
                    return Err(lost(
                        "this broker leads a later term than the read began in",
                    ));
                }
                Err(RaftError::APIError(LinearizableReadError::ForwardToLeader(forward))) => {
                    forward
                }
                Err(RaftError::APIError(LinearizableReadError::QuorumNotEnough(_))) => {
                    return Err(unanswered);
                }
                Err(RaftError::Fatal(_)) => return Err(propose::stopped()),
            };
            if forward.reason != ForwardReason::LeaseExpired {
                return Err(lost("this broker does not lead the shard"));
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

    /// A transfer of leadership away from this member is about to start.
    /// Closes the shard for the rest of the term this member leads, before
    /// it returns that term. `None` if it leads none.
    pub(crate) async fn transfer_starting(&self) -> Option<u64> {
        use openraft::vote::RaftLeaderId;
        // Asked of the core, in the queue a transfer request goes through.
        let leads = self.raft.with_raft_state(|state| {
            let leads = state.server_state == ServerState::Leader;
            leads.then(|| state.vote_ref().leader_id().term())
        });
        self.leadership
            .transfer_starting(leads.await.ok().flatten())
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
        let written = self.queue(propose::split_cache(writes)).await?;
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
        let mut commits = self.commits.clone();
        let applied = commits.wait_for(|commits| commits.read_bound() >= bound);
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

    /// Stop the member's tasks. Entries and the vote it acknowledged are
    /// already on disk; nothing is written here.
    pub(crate) async fn shutdown(&self) {
        self.stop.cancel();
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
            client_writes: self.client_writes.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn positions(&self) -> tokio::sync::watch::Receiver<LogPositions> {
        self.positions.clone()
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
    ) -> impl Future<Output = Result<Written, CommitError>> + Send + use<> {
        self.queue(propose::split(batch))
    }

    #[cfg(test)]
    pub(crate) fn cache(&self) -> &Cache {
        self.cache.as_ref().expect("a cache group")
    }

    #[cfg(test)]
    pub(crate) fn raft(&self) -> &Raft<ShardTypes, Machine> {
        &self.raft
    }
}
