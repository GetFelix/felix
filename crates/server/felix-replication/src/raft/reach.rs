//! Which peer brokers stopped answering this runtime's Raft calls, and
//! which members answer again, so that leaders here retry them at once.
//!
//! openraft waits longer and longer, up to a second, before it retries a
//! stream to a member it could not reach, and only a call on that stream
//! that succeeds ends the wait. Heartbeats keep going meanwhile. The router
//! reports every call's outcome here, and the ticker cuts the wait short
//! for members that answer again (#2543). While a broker is down its
//! members' waits grow past that second ([`Waits`]). See
//! `docs/per-shard-raft.md`, "Group runtime".

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use arc_swap::ArcSwap;
use felix_common::random::HashMap;
use openraft::network::Backoff;
use tokio::time::Instant;

use super::types::GroupId;

/// openraft's longest backoff to a broker that is not down. No member is
/// retried early more often than this, so a struggling peer is never called
/// faster than without the wake.
pub(crate) const CEILING: Duration = Duration::from_secs(1);

/// The longest a stream shard member's stream waits between retries while
/// its broker is down. Its first answer ends the wait, and a heartbeat at
/// the short gap finds it: the group's own while it has an entry not yet
/// committed, otherwise the one the ticker sends each short gap to a broker
/// that is down.
pub(crate) const WHILE_DOWN: Duration = Duration::from_secs(10);

/// A broker no call has failed to for this long is forgotten.
const FORGET_AFTER: u64 = 30_000;

const HEALTHY: u64 = 0;
const DOWN: u64 = 1;
/// Any other state is a broker whose calls have failed, with no answer
/// between, since `state - FAILING` milliseconds.
const FAILING: u64 = 2;

/// Every peer broker this runtime's calls failed to, by name.
pub(crate) struct Reach {
    started: Instant,
    /// How long a broker's calls must keep failing before it counts as
    /// down: one slow call is not an outage.
    streak: u64,
    peers: ArcSwap<HashMap<Arc<str>, Arc<Peer>>>,
    /// Serializes adding and forgetting peers.
    edit: parking_lot::Mutex<()>,
    /// Whole-broker walks the ticker started.
    #[cfg(test)]
    pub(crate) walks: std::sync::atomic::AtomicUsize,
    /// Leaves every wait to openraft, as without the wake.
    #[cfg(test)]
    pub(crate) no_wake: AtomicBool,
    /// Replication streams leaders here opened, by broker.
    #[cfg(test)]
    pub(crate) streams: parking_lot::Mutex<HashMap<Arc<str>, u32>>,
}

/// One peer broker. Its fields change only on a failure or on an answer
/// after one, so calls to a healthy broker only read them.
#[derive(Default)]
pub(crate) struct Peer {
    state: AtomicU64,
    /// It answered after it was down, so every leader here owes it a retry.
    back: AtomicBool,
    /// When a call to it last failed or one of its members opened.
    used: AtomicU64,
    /// Groups whose member there refused and has since answered.
    opened: parking_lot::Mutex<Vec<GroupId>>,
    /// The group whose leader's append to it failed last: while it is down,
    /// the ticker has that group heartbeat at the short gap.
    scout: parking_lot::Mutex<Option<GroupId>>,
}

impl Reach {
    /// A broker counts as down once its calls fail for `streak`.
    pub(crate) fn new(started: Instant, streak: Duration) -> Self {
        Self {
            started,
            streak: streak.as_millis() as u64,
            peers: ArcSwap::default(),
            edit: parking_lot::Mutex::new(()),
            #[cfg(test)]
            walks: Default::default(),
            #[cfg(test)]
            no_wake: AtomicBool::new(false),
            #[cfg(test)]
            streams: Default::default(),
        }
    }

    pub(crate) fn now(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    /// A call to `broker`, sent at `sent` (from [`Reach::now`]), was
    /// refused by the transport or went unanswered. Only a call sent after
    /// the failures began extends them: calls in flight together fail
    /// together, and an answer to a later one may already have arrived.
    pub(crate) fn lost(&self, broker: &Arc<str>, sent: u64) {
        let now = self.now();
        self.with(broker, |peer| {
            peer.used.store(now, Ordering::Relaxed);
            let state = peer.state.load(Ordering::Acquire);
            let next = match state {
                HEALTHY => FAILING + now,
                DOWN => return,
                since
                    if sent >= since - FAILING
                        && now.saturating_sub(since - FAILING) >= self.streak =>
                {
                    DOWN
                }
                _ => return,
            };
            let _ = peer
                .state
                .compare_exchange(state, next, Ordering::AcqRel, Ordering::Relaxed);
        });
    }

    /// A leader's append from `group` to `broker` failed, as reported to
    /// [`Self::lost`].
    pub(crate) fn lost_by(&self, broker: &Arc<str>, group: &GroupId) {
        if let Some(peer) = self.peers.load().get(broker) {
            *peer.scout.lock() = Some(group.clone());
        }
    }

    /// `broker` answered a call, whatever the answer said.
    pub(crate) fn answered(&self, broker: &str) {
        let peers = self.peers.load();
        let Some(peer) = peers.get(broker) else {
            return;
        };
        let state = peer.state.load(Ordering::Acquire);
        if state == HEALTHY {
            return;
        }
        if peer
            .state
            .compare_exchange(state, HEALTHY, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
            && state == DOWN
        {
            peer.back.store(true, Ordering::Release);
        }
    }

    /// `group`'s member on `broker` answered after it refused: it opened
    /// late, after its broker was already back.
    pub(crate) fn opened(&self, broker: &Arc<str>, group: &GroupId) {
        let now = self.now();
        self.with(broker, |peer| {
            peer.used.store(now, Ordering::Relaxed);
            peer.opened.lock().push(group.clone());
        });
    }

    /// Whether `broker` counts as down: its calls have kept failing for the
    /// streak, with no answer since.
    pub(crate) fn down(&self, broker: &str) -> bool {
        self.peers
            .load()
            .get(broker)
            .is_some_and(|peer| peer.is_down())
    }

    pub(crate) fn peers(&self) -> Arc<HashMap<Arc<str>, Arc<Peer>>> {
        self.peers.load_full()
    }

    /// Forget brokers nothing has failed to for a while, such as those that
    /// left the cluster.
    pub(crate) fn forget_idle(&self) {
        let now = self.now();
        let idle = |peer: &Peer| {
            !peer.back.load(Ordering::Acquire)
                && now.saturating_sub(peer.used.load(Ordering::Relaxed)) >= FORGET_AFTER
                && peer.opened.lock().is_empty()
        };
        if !self.peers.load().values().any(|peer| idle(peer)) {
            return;
        }
        let _edit = self.edit.lock();
        let mut peers = HashMap::clone(&self.peers.load());
        peers.retain(|_, peer| !idle(peer));
        self.peers.store(Arc::new(peers));
    }

    fn with(&self, broker: &Arc<str>, act: impl FnOnce(&Peer)) {
        if let Some(peer) = self.peers.load().get(broker) {
            return act(peer);
        }
        let _edit = self.edit.lock();
        let mut peers = HashMap::clone(&self.peers.load());
        let peer = Arc::clone(peers.entry(Arc::clone(broker)).or_default());
        self.peers.store(Arc::new(peers));
        act(&peer);
    }
}

impl Peer {
    /// Whether it answered after it was down, clearing that.
    pub(crate) fn take_back(&self) -> bool {
        self.back.swap(false, Ordering::AcqRel)
    }

    pub(crate) fn take_opened(&self) -> Vec<GroupId> {
        std::mem::take(&mut self.opened.lock())
    }

    /// A group whose call to it failed, for the ticker's heartbeat while it
    /// is down.
    pub(crate) fn scout(&self) -> Option<GroupId> {
        self.scout.lock().clone()
    }

    pub(crate) fn is_down(&self) -> bool {
        self.state.load(Ordering::Acquire) == DOWN
    }
}

/// openraft's retry delays for one member's stream. While the member's
/// broker is down they keep doubling past openraft's ceiling, up to
/// [`WHILE_DOWN`], so a dead broker costs each group a retry every few
/// seconds rather than one a second. A broker that answers is not down, and
/// the ticker then cuts the wait short (#2567).
pub(crate) struct Waits {
    usual: Backoff,
    reach: Arc<Reach>,
    broker: Arc<str>,
    last: Duration,
}

impl Waits {
    pub(crate) fn new(usual: Backoff, reach: Arc<Reach>, broker: Arc<str>) -> Self {
        Self {
            usual,
            reach,
            broker,
            last: Duration::ZERO,
        }
    }
}

impl Iterator for Waits {
    type Item = Duration;

    fn next(&mut self) -> Option<Duration> {
        let usual = self.usual.next()?;
        self.last = match self.reach.down(&self.broker) {
            true => self.last.saturating_mul(2).max(usual).min(WHILE_DOWN),
            false => usual,
        };
        Some(self.last)
    }
}

/// The brokers on which one group's member refused its calls: it was not
/// open there, or had stopped. Such a broker being back says nothing about
/// this group, so only the member's own answer counts.
pub(crate) struct Refusals {
    pub(crate) group: GroupId,
    /// Whether any broker in `by` still refuses: calls take no lock while
    /// none does.
    any: AtomicBool,
    by: parking_lot::Mutex<Vec<Refusal>>,
}

struct Refusal {
    broker: Arc<str>,
    refused: bool,
    /// When this member was last retried early, kept for `CEILING`.
    woke: Option<u64>,
}

impl Refusals {
    pub(crate) fn new(group: GroupId) -> Self {
        Self {
            group,
            any: AtomicBool::new(false),
            by: parking_lot::Mutex::default(),
        }
    }

    pub(crate) fn refused(&self, broker: &Arc<str>) {
        let mut by = self.by.lock();
        match by.iter_mut().find(|r| r.broker == *broker) {
            Some(refusal) => refusal.refused = true,
            None => by.push(Refusal {
                broker: Arc::clone(broker),
                refused: true,
                woke: None,
            }),
        }
        self.any.store(true, Ordering::Release);
    }

    /// The member on `broker` answered. Whether it refused before and is
    /// due a retry now.
    pub(crate) fn answered(&self, broker: &str, now: u64) -> bool {
        if !self.any.load(Ordering::Acquire) {
            return false;
        }
        let ceiling = CEILING.as_millis() as u64;
        let mut by = self.by.lock();
        let mut wake = false;
        if let Some(refusal) = by.iter_mut().find(|r| &*r.broker == broker)
            && refusal.refused
        {
            refusal.refused = false;
            if refusal
                .woke
                .is_none_or(|at| now.saturating_sub(at) >= ceiling)
            {
                refusal.woke = Some(now);
                wake = true;
            }
        }
        by.retain(|r| r.refused || r.woke.is_some_and(|at| now.saturating_sub(at) < ceiling));
        self.any
            .store(by.iter().any(|r| r.refused), Ordering::Release);
        wake
    }

    /// Whether the member on `broker` refused last.
    pub(crate) fn holds(&self, broker: &str) -> bool {
        self.any.load(Ordering::Acquire)
            && self
                .by
                .lock()
                .iter()
                .any(|r| r.refused && &*r.broker == broker)
    }
}

#[cfg(test)]
mod tests;
