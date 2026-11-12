//! One member of one group: its openraft instance over the shard's log.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use felix_storage::DiskLog;
use felix_storage::raft_log::{RaftLog, SnapshotStore};
use openraft::error::{InitializeError, RaftError};
use openraft::{Config, Raft, ServerState};

use super::codec::ShardCodec;
use super::machine::Machine;
use super::network::{Addressee, Network, Refused, Reply, Request, Router};
use super::runtime::{GroupStatus, OpenError, Role};
use super::types::{GroupId, MemberId, ShardTypes};

pub(crate) struct Group {
    pub(crate) id: GroupId,
    pub(crate) member: MemberId,
    raft: Raft<ShardTypes, Machine>,
    timers: Timers,
    /// When the next timer check can matter, in milliseconds on the
    /// runtime's clock. The ticker skips the group until then.
    due: AtomicU64,
    /// When this member last took an append from its leader or granted a
    /// vote. Either restarts its election timer.
    heard: AtomicU64,
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
    pub(crate) election_min: u64,
}

impl Timers {
    pub(crate) fn now(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
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
        router: Router,
        timers: Timers,
    ) -> Result<Self, OpenError> {
        let snapshots = SnapshotStore::open(log.dir())?;
        let log = RaftLog::<ShardTypes, ShardCodec>::new(log, ShardCodec)?;
        // A shard's state is its log, so nothing can rebuild what a purge
        // past the snapshot would drop.
        log.purge_within(&snapshots)?;
        let machine = Machine::open(snapshots).await?;
        let network = Network {
            router,
            group: id.clone(),
        };
        let raft = Raft::new(member.clone(), config, network, log, machine)
            .await
            .map_err(|err| OpenError::Raft(err.to_string()))?;
        Ok(Self {
            id,
            member,
            raft,
            timers,
            due: AtomicU64::new(0),
            heard: AtomicU64::new(0),
            ticks: AtomicU64::new(0),
            dropped_ticks: AtomicU64::new(0),
            stopped: AtomicBool::new(false),
        })
    }

    /// Form the group from its first member set. Every member of the set
    /// calls this with the same set. A member that has already voted or
    /// holds an entry is past this step, and the call does nothing.
    pub(crate) async fn initialize(&self, members: BTreeSet<MemberId>) -> Result<(), String> {
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
        self.due.store(self.next_due(), Ordering::Relaxed);
        if let Err(err) = ticked
            && !self.stopped.swap(true, Ordering::Relaxed)
        {
            tracing::warn!(group = %self.id, member = %self.member, error = %err, "a Raft group stopped and is no longer ticked");
        }
    }

    /// Whether a timer check now could find something to do.
    pub(crate) fn due(&self, now: u64) -> bool {
        now >= self.due.load(Ordering::Relaxed)
    }

    /// When the next timer check can matter, after one was just sent.
    ///
    /// A leader's only timer is its heartbeat. A voter's is its election
    /// timeout, which cannot pass until `election_min` after it last heard
    /// its leader, so a follower that keeps hearing one is never checked.
    /// Once that time has passed it is checked every interval, since the
    /// timeout openraft picked for it is not visible from here. A learner
    /// never stands, and is checked only so a change of role is seen.
    fn next_due(&self) -> u64 {
        use openraft::async_runtime::watch::WatchReceiver;
        let Timers {
            tick,
            heartbeat,
            election_min,
            ..
        } = self.timers;
        let now = self.timers.now();
        match self.raft.metrics().borrow_watched().state {
            // One past the interval, so the check lands after the heartbeat
            // is due and not just before it.
            ServerState::Leader => now + heartbeat + 1,
            ServerState::Follower | ServerState::Candidate => {
                (self.heard.load(Ordering::Relaxed) + election_min).max(now + tick)
            }
            ServerState::Learner | ServerState::Shutdown => now + election_min,
        }
    }

    /// This member took an append from its leader or granted a vote, which
    /// restarts its election timer.
    fn heard(&self) {
        let now = self.timers.now();
        self.heard.store(now, Ordering::Relaxed);
        self.due
            .store(now + self.timers.election_min, Ordering::Relaxed);
    }

    pub(crate) async fn handle(&self, body: Request) -> Result<Reply, Refused> {
        let reply = self.dispatch(body).await?;
        match &reply {
            Reply::Append(Ok(_)) | Reply::Snapshot(_) => self.heard(),
            Reply::Vote(vote) if vote.vote_granted => self.heard(),
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
            committed: metrics.local_committed.map(|id| id.index),
            ticks: self.ticks.load(Ordering::Relaxed),
            dropped_ticks: self.dropped_ticks.load(Ordering::Relaxed),
        }
    }

    #[cfg(test)]
    pub(crate) fn raft(&self) -> &Raft<ShardTypes, Machine> {
        &self.raft
    }
}
