//! The node ticker: the one timer behind every group on a broker.
//!
//! Each group is built with openraft's heartbeat and election timers off.
//! This task asks a leader for a heartbeat check once per heartbeat, at the
//! short or the idle gap, and turns a follower's election timer on once its
//! wait for the leader has run out. A follower that hears its leader is
//! never asked. See
//! `docs/per-shard-raft.md`, "One timer for every group".
//!
//! Turning that timer on is the only way a member here starts an election,
//! so the ticker also bounds how many groups on the broker are in one at
//! once ([`Elections`]). See "Many groups electing at once" there.

use std::collections::{HashSet, VecDeque};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use felix_common::random::HashMap;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use super::group::{Group, Timers};
use super::reach::{CEILING, Peer};
use super::runtime::{ElectionLoad, Registry};

/// Times the ticker ended on a panic and was started again. Any increase
/// is a bug: no group on the broker was ticked in between.
const TICKER_RESTARTS_TOTAL: &str = "felix_broker_raft_ticker_restarts_total";

/// Members here let stand whose election is not over.
const ELECTIONS_IN_FLIGHT: &str = "felix_broker_raft_elections_in_flight";

/// Members here whose wait for a leader ran out and that wait for a place.
const ELECTIONS_WAITING: &str = "felix_broker_raft_elections_waiting";

/// Run the ticker until `stop`, starting it again if it panics.
///
/// Without a ticker no leader here heartbeats and no follower here starts a
/// new election, and nothing else would show it. A restart puts both back within a
/// heartbeat, where failing the runtime would stop every group for a bug
/// that may be in one. The restarts are logged and counted for whoever
/// watches the broker.
pub(crate) async fn supervise(
    registry: Arc<Registry>,
    every: Duration,
    timers: Timers,
    stop: CancellationToken,
) {
    loop {
        let ticker = run(Arc::clone(&registry), every, timers, stop.clone());
        let Err(panic) = AssertUnwindSafe(ticker).catch_unwind().await else {
            return;
        };
        let reason = panic
            .downcast_ref::<&str>()
            .map(|reason| reason.to_string())
            .or_else(|| panic.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "no message".into());
        let restarts = registry.ticker_restarts.fetch_add(1, Ordering::Relaxed) + 1;
        metrics::counter!(TICKER_RESTARTS_TOTAL).increment(1);
        tracing::error!(%reason, restarts, "the Raft ticker panicked and is being started again");
        // A ticker that panics every time must not spin.
        tokio::select! {
            biased;
            () = stop.cancelled() => return,
            () = tokio::time::sleep(Duration::from_millis(timers.heartbeat)) => {}
        }
    }
}

async fn run(registry: Arc<Registry>, every: Duration, timers: Timers, stop: CancellationToken) {
    let mut interval = tokio::time::interval(every);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut looks = Looks::default();
    let mut back = Back::default();
    let mut elections = Elections::new(&timers, Arc::clone(&registry.vote_saves));
    loop {
        tokio::select! {
            biased;
            () = stop.cancelled() => return,
            _ = interval.tick() => {}
        }
        #[cfg(test)]
        if registry.trip_ticker.swap(false, Ordering::Relaxed) {
            panic!("tripped by a test");
        }
        registry.tail_budget.report();
        back.retry(&registry, Duration::from_millis(timers.heartbeat));
        let due = looks.due(registry.ticked(), &timers);
        if timers.elections > 0 {
            let now = timers.now();
            elections.settle(now);
            let wanting = due.iter().filter(|group| group.wants_to_stand());
            elections.queue(wanting.cloned(), now);
            elections.admit(now);
            elections.report(&registry);
        }
        tokio::select! {
            biased;
            () = stop.cancelled() => return,
            () = tick_all(&due, Instant::now() + every) => {}
        }
    }
}

/// Retries peer members that answer again, ahead of openraft's backoff.
///
/// A broker back from an outage gets one walk of every group here that
/// leads a member on it, except the groups whose member there still
/// refuses: those are not open yet, and each is retried on its own member's
/// first answer instead. A broker is walked at most once per `CEILING` and
/// never twice at once, so a flapping or struggling peer is not called more
/// often than openraft's backoff would call it at its ceiling.
#[derive(Default)]
struct Back {
    walks: HashMap<Arc<str>, Walk>,
}

#[derive(Default)]
struct Walk {
    last: Option<Instant>,
    busy: Arc<AtomicBool>,
    /// When a group last heartbeat to the broker while it was down.
    scouted: Option<Instant>,
    scouting: Arc<AtomicBool>,
}

/// Retry commands sent to the groups' cores at once.
const RESETS_AT_ONCE: usize = 64;

impl Back {
    fn retry(&mut self, registry: &Registry, heartbeat: Duration) {
        let reach = &registry.reach;
        #[cfg(test)]
        if reach.no_wake.load(Ordering::Relaxed) {
            return;
        }
        let peers = reach.peers();
        self.walks.retain(|broker, _| peers.contains_key(broker));
        let now = Instant::now();
        for (broker, peer) in peers.iter() {
            let walk = self.walks.entry(Arc::clone(broker)).or_default();
            scout(registry, peer, walk, now, heartbeat);
            if walk.busy.load(Ordering::Acquire) {
                continue;
            }
            let whole = walk.last.is_none_or(|at| now - at >= CEILING) && peer.take_back();
            let opened = peer.take_opened();
            let groups: Vec<Arc<Group>> = if whole {
                walk.last = Some(now);
                #[cfg(test)]
                reach.walks.fetch_add(1, Ordering::Relaxed);
                registry
                    .ticked()
                    .iter()
                    .filter(|group| !group.refused_on(broker))
                    .cloned()
                    .collect()
            } else {
                opened
                    .iter()
                    .filter_map(|id| registry.get(&id.shard).filter(|group| group.id == *id))
                    .collect()
            };
            if groups.is_empty() {
                continue;
            }
            walk.busy.store(true, Ordering::Release);
            let busy = Arc::clone(&walk.busy);
            let broker = Arc::clone(broker);
            tokio::spawn(async move {
                futures::stream::iter(groups)
                    .for_each_concurrent(RESETS_AT_ONCE, |group| {
                        let broker = Arc::clone(&broker);
                        async move { group.retry_now(&broker).await }
                    })
                    .await;
                busy.store(false, Ordering::Release);
            });
        }
        reach.forget_idle();
    }
}

/// When the ticker next has to read each group's due time.
///
/// Reading it means touching the group, and with thousands of groups that
/// is most of what an interval costs. Anything other than a timer check
/// that moves a group's due time moves it at least a heartbeat ahead of the
/// moment it runs (`Pace::wrote`, `Pace::followed`), so a group read now
/// and found not due cannot become due sooner than that.
#[derive(Default)]
struct Looks {
    groups: Arc<Vec<Arc<Group>>>,
    /// By position in `groups`, on the runtime's clock.
    at: Vec<u64>,
}

impl Looks {
    /// The groups whose timer check is due now.
    fn due(&mut self, groups: Arc<Vec<Arc<Group>>>, timers: &Timers) -> Vec<Arc<Group>> {
        if !Arc::ptr_eq(&self.groups, &groups) {
            // A group was opened or closed, and positions moved.
            self.at.clear();
            self.at.resize(groups.len(), 0);
            self.groups = groups;
        }
        let now = timers.now();
        let mut due = Vec::new();
        for (group, look) in self.groups.iter().zip(&mut self.at) {
            if now < *look {
                continue;
            }
            let at = group.due_at();
            if now >= at {
                // The check sets a new due time: read it next interval.
                due.push(Arc::clone(group));
            } else {
                *look = at.min(now + timers.heartbeat);
            }
        }
        due
    }
}

/// While a broker is down, one group that leads here heartbeats to it each
/// short gap. Idle groups heartbeat it only every idle gap, and the walk
/// that retries them all waits for the broker's first answer, so this keeps
/// that answer within about a heartbeat of the broker coming back. It costs
/// one group's heartbeat per dead broker, whatever the number of groups.
fn scout(registry: &Registry, peer: &Peer, walk: &mut Walk, now: Instant, heartbeat: Duration) {
    if !peer.is_down()
        || walk.scouting.load(Ordering::Acquire)
        || walk.scouted.is_some_and(|at| now - at < heartbeat)
    {
        return;
    }
    walk.scouted = Some(now);
    let Some(group) = peer
        .scout()
        .and_then(|id| registry.get(&id.shard).filter(|group| group.id == id))
    else {
        return;
    };
    walk.scouting.store(true, Ordering::Release);
    let scouting = Arc::clone(&walk.scouting);
    tokio::spawn(async move {
        group.heartbeat_now().await;
        scouting.store(false, Ordering::Release);
    });
}

/// One timer check on each of `groups`, until `deadline`.
///
/// The checks are polled together in this task. A check waits while its
/// group's queue is full, so asking the groups in turn would let one busy
/// group hold up the rest. A check still waiting at the deadline is dropped,
/// never queued behind the next interval's: openraft reads the clock when it
/// runs a check, so the next one does the same work.
async fn tick_all(groups: &[Arc<Group>], deadline: Instant) {
    if groups.is_empty() {
        return;
    }
    let mut waiting: FuturesUnordered<_> = groups.iter().map(|group| group.tick()).collect();
    let all = async { while waiting.next().await.is_some() {} };
    let _ = tokio::time::timeout_at(deadline, all).await;
}

/// What the election bound needs from a member. A trait so its rules can be
/// tested without openraft.
pub(super) trait Campaigner {
    /// Whether it may still stand, having heard no leader and granted no
    /// vote since `since`.
    fn may_stand(&self, since: u64) -> bool;
    /// Turn its election timer on, if it may still stand.
    fn arm(&self, since: u64) -> bool;
    /// Turn its election timer off.
    fn disarm(&self);
    /// Whether the election it was let into is over.
    fn settled(&self) -> bool;
    /// Vote and pre-vote requests sent so far, and how many are unanswered.
    fn asks(&self) -> (u64, u64);
    fn set_waiting(&self, waiting: bool);
    /// Its place among the members that came due together.
    fn order(&self) -> u64;
}

impl Campaigner for Group {
    fn may_stand(&self, since: u64) -> bool {
        Group::may_stand(self, since)
    }
    fn arm(&self, since: u64) -> bool {
        Group::arm(self, since)
    }
    fn disarm(&self) {
        self.hold_elections();
    }
    fn settled(&self) -> bool {
        Group::settled(self)
    }
    fn asks(&self) -> (u64, u64) {
        Group::asks(self)
    }
    fn set_waiting(&self, waiting: bool) {
        Group::set_waiting(self, waiting);
    }
    fn order(&self) -> u64 {
        Group::order(self)
    }
}

/// The bound on elections at once on this broker.
///
/// A member whose wait for its leader ran out gets its election timer
/// turned on only while fewer than `limit` members here have theirs on;
/// the rest wait in the order they came due. openraft still decides when a
/// member stands and whether it wins. A place is given back when the
/// member leads, hears a leader or grants a vote, or lost: every request
/// of its round answered without it leading (seen on two looks), no stand
/// within `election_max`, or no result within `election_min` of standing.
/// One that lost has its timer turned off and waits again at the back.
///
/// `limit` starts at [`super::RuntimeConfig::elections`] and follows this
/// broker's vote saves: a slow one cuts it to what the disk would have
/// saved in time, and each election that ends gives one place back.
pub(super) struct Elections<M: Campaigner = Group> {
    most: usize,
    limit: usize,
    queue: VecDeque<Waiting<M>>,
    held: Vec<Held<M>>,
    /// Queued or held, by address.
    known: HashSet<usize>,
    /// A vote save slower than this lowers `limit`: a quarter of
    /// `election_min`, so a round's saves end well inside the timeout.
    slow: u64,
    /// The longest an armed member keeps its place before it stands.
    stand: u64,
    /// The longest a member that stood keeps its place.
    round: u64,
    saves: Arc<VoteSaves>,
    lowered: Lowered,
    peak: usize,
    reported: ElectionLoad,
}

struct Waiting<M> {
    member: Arc<M>,
    since: u64,
}

struct Held<M> {
    member: Arc<M>,
    since: u64,
    /// Its requests sent before it was let in.
    asks: u64,
    stood: Option<u64>,
    /// Every request of its round was answered when last looked at.
    answered: bool,
}

/// The last time `limit` was lowered.
#[derive(Default)]
struct Lowered {
    from: usize,
    took: u64,
}

/// How long this broker's vote saves take, waits for the disk included.
#[derive(Default)]
pub(crate) struct VoteSaves {
    /// When `limit` was last lowered.
    since: AtomicU64,
    /// The slowest save that began after `since`, under the limit as it is.
    slowest: AtomicU64,
    /// The slowest that began before it, under the limit as it was.
    slowest_before: AtomicU64,
}

impl VoteSaves {
    /// A vote save that ended at `now` took `took`.
    pub(crate) fn saved(&self, now: u64, took: u64) {
        let slowest = match now.saturating_sub(took) >= self.since.load(Ordering::Relaxed) {
            true => &self.slowest,
            false => &self.slowest_before,
        };
        slowest.fetch_max(took, Ordering::Relaxed);
    }
}

fn address<M>(member: &Arc<M>) -> usize {
    Arc::as_ptr(member) as *const () as usize
}

impl<M: Campaigner> Elections<M> {
    fn new(timers: &Timers, saves: Arc<VoteSaves>) -> Self {
        Self {
            most: timers.elections,
            limit: timers.elections,
            queue: VecDeque::new(),
            held: Vec::new(),
            known: HashSet::new(),
            slow: timers.election_min / 4,
            stand: timers.election_max,
            round: timers.election_min,
            saves,
            lowered: Lowered::default(),
            peak: 0,
            reported: ElectionLoad::default(),
        }
    }

    /// Queue the members in `wanting` that are not queued or held yet, in
    /// their order.
    fn queue(&mut self, wanting: impl Iterator<Item = Arc<M>>, now: u64) {
        let mut new: Vec<Arc<M>> = wanting
            .filter(|member| !self.known.contains(&address(member)))
            .collect();
        new.sort_by_key(|member| member.order());
        for member in new {
            self.known.insert(address(&member));
            self.wait(member, now);
        }
    }

    fn wait(&mut self, member: Arc<M>, since: u64) {
        member.set_waiting(true);
        self.queue.push_back(Waiting { member, since });
    }

    /// Let members in from the front of the queue while places are free.
    fn admit(&mut self, now: u64) {
        while self.held.len() < self.limit {
            let Some(Waiting { member, since }) = self.queue.pop_front() else {
                break;
            };
            member.set_waiting(false);
            if !member.arm(since) {
                self.known.remove(&address(&member));
                continue;
            }
            let asks = member.asks().0;
            self.held.push(Held {
                member,
                since: now,
                asks,
                stood: None,
                answered: false,
            });
        }
        self.peak = self.peak.max(self.held.len());
    }

    /// Give back the places of elections that are over, drop waiting
    /// members that heard a leader, and move `limit`.
    fn settle(&mut self, now: u64) {
        let mut ended = 0;
        let mut lost = Vec::new();
        let (stand, round) = (self.stand, self.round);
        let known = &mut self.known;
        self.held.retain_mut(|held| {
            if held.member.settled() {
                ended += 1;
                known.remove(&address(&held.member));
                return false;
            }
            let (asks, out) = held.member.asks();
            if held.stood.is_none() && asks > held.asks {
                held.stood = Some(now);
            }
            let over = match held.stood {
                None => now >= held.since + stand,
                Some(stood) => (held.answered && out == 0) || now >= stood + round,
            };
            held.answered = held.stood.is_some() && out == 0;
            if over {
                lost.push(Arc::clone(&held.member));
            }
            !over
        });
        for member in lost {
            // Its round may still be won: openraft takes the answers
            // either way. It just does not start another before its turn.
            member.disarm();
            self.wait(member, now);
        }
        let known = &mut self.known;
        self.queue.retain(|waiting| {
            let keep = waiting.member.may_stand(waiting.since);
            if !keep {
                waiting.member.set_waiting(false);
                known.remove(&address(&waiting.member));
            }
            keep
        });

        let slow = self.slow;
        // To what the disk would have saved inside `slow`.
        let fits = |from: usize, took: u64| ((from as u64 * slow / took) as usize).max(1);
        let slowest = self.saves.slowest.swap(0, Ordering::Relaxed);
        let before = self.saves.slowest_before.swap(0, Ordering::Relaxed);
        if slowest > slow {
            self.lowered = Lowered {
                from: self.limit,
                took: slowest,
            };
            self.limit = fits(self.limit, slowest);
            self.saves.since.store(now, Ordering::Relaxed);
        } else if before > slow && before > self.lowered.took {
            // Queued under the limit as it was, and slower than the save
            // it was lowered for: that limit was further off than it looked.
            self.lowered.took = before;
            self.limit = self.limit.min(fits(self.lowered.from, before));
        } else {
            self.limit = (self.limit + ended).min(self.most);
        }
    }

    fn load(&self) -> ElectionLoad {
        ElectionLoad {
            in_flight: self.held.len(),
            waiting: self.queue.len(),
            limit: self.limit,
            peak: self.peak,
        }
    }

    fn report(&mut self, registry: &Registry) {
        let load = self.load();
        if load != self.reported {
            metrics::gauge!(ELECTIONS_IN_FLIGHT).set(load.in_flight as f64);
            metrics::gauge!(ELECTIONS_WAITING).set(load.waiting as f64);
            registry.set_election_load(load);
            self.reported = load;
        }
    }
}

#[cfg(test)]
mod tests;
