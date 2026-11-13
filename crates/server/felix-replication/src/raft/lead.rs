//! Whether this member leads its group, and whether the shard is open.
//!
//! One task per member watches openraft's role and vote, and the commit
//! table until the term opens. It tells the host `opened` once an entry of
//! the member's own term is committed, and `deposed` when the member stops
//! leading that term. A refused lease check is not among its inputs. See
//! `docs/per-shard-raft.md`, "When a leader opens" and "When it closes".

use std::sync::Arc;

use openraft::metrics::RaftServerMetrics;
use openraft::type_config::alias::WatchReceiverOf;
use openraft::{Raft, ServerState};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::commit::Commits;
use super::machine::Machine;
use super::propose::{Leading, Lost};
use super::types::ShardTypes;
use crate::ShardKey;
use crate::engine::EngineHost;

type Server = WatchReceiverOf<ShardTypes, RaftServerMetrics<ShardTypes>>;

/// This member's hold on its shard, as the watcher last saw it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Lead {
    /// The term this member leads, unless a transfer started in it.
    pub(crate) term: Option<u64>,
    /// Whether the host was told that term is open.
    pub(crate) open: bool,
    /// The term a transfer started in. It never opens again.
    void: Option<u64>,
    /// The watcher has ended: the group's core stopped, or the member was
    /// told to stop.
    gone: bool,
}

pub(crate) struct Leadership {
    shard: Arc<ShardKey>,
    host: Option<Arc<dyn EngineHost>>,
    /// Held over a change and the host call it makes, so the host hears
    /// them in order from the watcher and from a transfer.
    changing: parking_lot::Mutex<()>,
    lead: watch::Sender<Lead>,
    /// Looks the watcher has taken.
    #[cfg(test)]
    pub(crate) looks: std::sync::atomic::AtomicU64,
}

/// The term this member leads, as openraft says now.
fn leading_term(server: &Server) -> Option<u64> {
    use openraft::async_runtime::watch::WatchReceiver;
    use openraft::vote::RaftLeaderId;
    let server = server.borrow_watched();
    (server.state == ServerState::Leader).then(|| server.vote.leader_id().term())
}

fn stopped(raft: &Raft<ShardTypes, Machine>) -> bool {
    use openraft::async_runtime::watch::WatchReceiver;
    raft.metrics().borrow_watched().running_state.is_err()
}

impl Leadership {
    pub(crate) fn new(shard: Arc<ShardKey>, host: Option<Arc<dyn EngineHost>>) -> Self {
        Self {
            shard,
            host,
            changing: parking_lot::Mutex::new(()),
            lead: watch::Sender::default(),
            #[cfg(test)]
            looks: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub(crate) fn lead(&self) -> Lead {
        *self.lead.borrow()
    }

    /// Whether this member still leads, for a publisher or a commit wait.
    pub(crate) fn watch(&self, raft: &Raft<ShardTypes, Machine>) -> Watched {
        Watched {
            lead: self.lead.subscribe(),
            server: raft.server_metrics(),
        }
    }

    /// openraft says this member leads `leads`, and the last applied entry
    /// has term `committed`. `gone` is the watcher's last look.
    fn observe(&self, leads: Option<u64>, committed: Option<u64>, gone: bool) {
        let _changing = self.changing.lock();
        let was = self.lead();
        let term = leads.filter(|term| was.void != Some(*term));
        let mut open = was.open && was.term == term;
        if was.open
            && !open
            && let (Some(host), Some(term)) = (&self.host, was.term)
        {
            host.deposed(&self.shard, term);
        }
        // An entry of this term is committed, so the term's blank entry is,
        // and this member holds every committed record.
        if let Some(term) = term
            && !open
            && committed == Some(term)
        {
            if let Some(host) = &self.host {
                host.opened(&self.shard, term);
            }
            open = true;
        }
        let now = Lead {
            term,
            open,
            void: was.void,
            gone: was.gone || gone,
        };
        self.lead.send_if_modified(|lead| {
            let changed = *lead != now;
            *lead = now;
            changed
        });
    }

    /// A transfer of leadership away from this member is about to start in
    /// term `leads`, as the group's core says, not its metrics: those can
    /// trail an election, and the term would open mid-transfer. The shard
    /// closes before this returns and stays closed for the rest of that
    /// term, whether or not the transfer goes through. Returns the term
    /// closed.
    pub(crate) fn transfer_starting(&self, leads: Option<u64>) -> Option<u64> {
        let term = leads?;
        {
            let _changing = self.changing.lock();
            self.lead.send_modify(|lead| lead.void = Some(term));
        }
        self.observe(leads, None, false);
        Some(term)
    }
}

/// Keep `leadership` in step with openraft until `stop`, or until the group
/// stops, which closes the shard.
pub(crate) async fn run(
    leadership: Arc<Leadership>,
    raft: Raft<ShardTypes, Machine>,
    mut commits: watch::Receiver<Commits>,
    stop: CancellationToken,
) {
    use openraft::async_runtime::watch::WatchReceiver;
    let mut server = raft.server_metrics();
    loop {
        if stopped(&raft) {
            break;
        }
        #[cfg(test)]
        leadership
            .looks
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let leads = leading_term(&server);
        let committed = commits.borrow_and_update().applied().map(|at| at.term);
        leadership.observe(leads, committed, false);
        // The commit table moves with every write. It is watched only
        // while a term waits to open.
        // A term a transfer started in never opens, so it is not waited on.
        let lead = leadership.lead();
        let opens = lead.term.filter(|_| !lead.open);
        let committed = async {
            match opens {
                Some(term) => commits
                    .wait_for(|commits| commits.applied().is_some_and(|at| at.term == term))
                    .await
                    .is_ok(),
                None => std::future::pending().await,
            }
        };
        let running = tokio::select! {
            biased;
            () = stop.cancelled() => false,
            changed = server.changed() => changed.is_ok(),
            running = committed => running,
        };
        if !running {
            break;
        }
    }
    // One change, so no publisher reads the close without the reason.
    leadership.observe(None, None, true);
}

/// Returns once a majority's last answer to this leader is newer than it
/// is now, or the group is gone.
pub(crate) async fn quorum_answered(raft: &Raft<ShardTypes, Machine>) {
    use openraft::async_runtime::watch::WatchReceiver;
    let mut metrics = raft.metrics();
    let acked = |metrics: &WatchReceiverOf<ShardTypes, openraft::RaftMetrics<ShardTypes>>| {
        metrics.borrow_watched().last_quorum_acked
    };
    let before = acked(&metrics);
    while acked(&metrics) == before && metrics.changed().await.is_ok() {}
}

/// [`Leading`] read from the watcher, checked against openraft.
pub(crate) struct Watched {
    lead: watch::Receiver<Lead>,
    server: Server,
}

impl Leading for Watched {
    async fn lost(&mut self, term: Option<u64>) -> Lost {
        use openraft::async_runtime::watch::WatchReceiver;
        loop {
            let lead = *self.lead.borrow_and_update();
            let held =
                |leads: Option<u64>| leads.is_some() && term.is_none_or(|term| leads == Some(term));
            // The watcher hears of an election a moment after openraft
            // does, so a new leader is not "lost" on the watcher's word
            // alone. A transfer is the watcher's to say.
            let elected = leading_term(&self.server).filter(|term| lead.void != Some(*term));
            if !held(lead.term) && !held(elected) {
                return match self.server.borrow_watched().state {
                    _ if lead.gone => Lost::Stopped,
                    ServerState::Shutdown => Lost::Stopped,
                    ServerState::Candidate => Lost::Electing,
                    _ => Lost::Deposed,
                };
            }
            // Either can move without the other: openraft can win and lose
            // a term between two looks of the watcher.
            let moved = tokio::select! {
                moved = self.lead.changed() => moved.is_ok(),
                moved = self.server.changed() => moved.is_ok(),
            };
            // The watcher or the core is gone.
            if !moved {
                return Lost::Stopped;
            }
        }
    }
}
