//! The node ticker: the one timer behind every group on a broker.
//!
//! openraft notices an election timeout or a due heartbeat only when its
//! timer check runs. Each group is built with its own tick off, and this
//! task asks for a check, each interval, from the groups whose timer can
//! have run out. A follower that hears its leader is
//! never asked, and a leader is asked once per heartbeat. See
//! `docs/per-shard-raft.md`, "One timer for every group".
//!
//! A check is also the only thing that has a member stand for election, so
//! the ticker bounds how many groups on the broker are in an election at
//! once ([`Elections`]). See "Many groups electing at once" there.

use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use super::group::{Group, Timers, Turn};
use super::runtime::{ElectionLoad, Registry};

/// Times the ticker ended on a panic and was started again. Any increase
/// is a bug: no group on the broker was ticked in between.
const TICKER_RESTARTS_TOTAL: &str = "felix_broker_raft_ticker_restarts_total";

/// Groups on this broker in an election now: sent timer checks that can
/// have them stand, and not yet following or leading.
const ELECTIONS_IN_FLIGHT: &str = "felix_broker_raft_elections_in_flight";

/// Groups whose timer check could start an election and that wait for one
/// of the elections in flight to end first.
const ELECTIONS_WAITING: &str = "felix_broker_raft_elections_waiting";

/// How many elections the bound starts at, before any has been timed.
const ELECTIONS_AT_FIRST: usize = 8;

/// Run the ticker until `stop`, starting it again if it panics.
///
/// Without a ticker no leader here heartbeats and no follower here stands,
/// and nothing else would show it. A restart puts both back within a
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
    let mut elections = Elections::new(&timers);
    // A ticker started again has no queue, so no group is left in one.
    for group in registry.ticked().iter() {
        group.set_turn(Turn::None);
    }
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
        let due = looks.due(registry.ticked(), &timers);
        let due = elections.admit(due, &timers);
        elections.report(&registry);
        tokio::select! {
            biased;
            () = stop.cancelled() => return,
            () = tick_all(&due, Instant::now() + every) => {}
        }
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

/// The broker's bound on elections in flight.
///
/// A vote is fsynced before it is answered, by the candidate and by each
/// voter, each group on its own files. When every group on a broker loses
/// its leader together and all stand in the same second, a round of those
/// fsyncs can outlast the election timeout, and then the timeouts fire
/// again and add more. So a member whose check could have it stand is sent
/// one only while fewer than `limit` others are. The rest wait in the
/// order they came due. Holding a check back is as safe as dropping one.
///
/// A member counts until it leads, follows a leader or votes for another
/// candidate, and for no longer than `hold`: then it goes to the back of
/// the queue, so a member that cannot win does not keep the others out.
///
/// `limit` follows the disk. Each election that ends raises it by one, up
/// to the configured bound. It is halved when a round took half the
/// shortest election timeout, and when a member is seen to stand a second
/// time: its first round outlasted the timeout, which is how the loop
/// above starts.
struct Elections {
    /// The configured bound. 0 is none: every due group is checked.
    most: usize,
    limit: usize,
    held: Vec<Held>,
    waiting: VecDeque<Arc<Group>>,
    /// A round that took longer than this lowers `limit`.
    slow: u64,
    /// The longest a member is counted.
    hold: u64,
    /// When `limit` was last lowered. Elections started under the old
    /// limit run long too and must not lower it again.
    lowered: u64,
    reported: ElectionLoad,
}

/// A member counted as an election in flight.
struct Held {
    group: Arc<Group>,
    since: u64,
    /// Its term when last looked at. A higher one says it has stood.
    term: u64,
    /// When it was first seen to have stood, and how many times it has.
    stood: Option<u64>,
    rounds: u32,
}

impl Elections {
    fn new(timers: &Timers) -> Self {
        Self {
            most: timers.elections,
            limit: timers.elections.min(ELECTIONS_AT_FIRST),
            held: Vec::new(),
            waiting: VecDeque::new(),
            slow: timers.election_min / 2,
            hold: 2 * timers.election_max,
            lowered: 0,
            reported: ElectionLoad::default(),
        }
    }

    /// The groups of `due` to check now: every one that cannot stand, and
    /// of the ones that can, those counted in flight.
    fn admit(&mut self, due: Vec<Arc<Group>>, timers: &Timers) -> Vec<Arc<Group>> {
        if self.most == 0 {
            return due;
        }
        let now = timers.now();
        self.settle(now);
        let mut checked = Vec::with_capacity(due.len());
        let mut arrived = Vec::new();
        for group in due {
            let stands = group.standing_term().is_some();
            match group.turn() {
                Turn::Admitted => checked.push(group),
                Turn::Waiting if stands => {}
                Turn::None if stands => arrived.push(group),
                // A leader's heartbeat, or a learner: never held back. One
                // that was waiting was handed the lead meanwhile, and its
                // place in the queue is passed over when it comes up.
                Turn::Waiting | Turn::None => {
                    group.set_turn(Turn::None);
                    checked.push(group);
                }
            }
        }
        // By a hash of the member, so two brokers that lost the same
        // leader do not stand for the same groups in the same moment.
        arrived.sort_unstable_by_key(|group| group.rank());
        for group in arrived {
            group.set_turn(Turn::Waiting);
            self.waiting.push_back(group);
        }
        while self.held.len() < self.limit {
            let Some(group) = self.waiting.pop_front() else {
                break;
            };
            if group.turn() != Turn::Waiting {
                continue;
            }
            // It may have heard a leader or been closed while it waited.
            let Some(term) = group
                .standing_term()
                .filter(|_| now >= group.may_stand_at())
            else {
                group.set_turn(Turn::None);
                continue;
            };
            group.set_turn(Turn::Admitted);
            self.held.push(Held {
                group: Arc::clone(&group),
                since: now,
                term,
                stood: None,
                rounds: 0,
            });
            checked.push(group);
        }
        checked
    }

    /// Stop counting the members whose election is over, and send the ones
    /// counted for too long to the back of the queue.
    fn settle(&mut self, now: u64) {
        let (mut ended, mut slow) = (0, false);
        let mut again = Vec::new();
        self.held.retain_mut(|held| {
            let group = &held.group;
            let term = group.standing_term();
            if term.is_none() || now < group.may_stand_at() {
                // It leads, or it took a leader's append or voted.
                ended += 1;
                slow |= held.stood.is_some_and(|stood| now - stood > self.slow);
                group.set_turn(Turn::None);
                return false;
            }
            if let Some(term) = term.filter(|term| *term > held.term) {
                held.term = term;
                held.stood.get_or_insert(now);
                held.rounds += 1;
                slow |= held.rounds == 2;
            }
            if now >= held.since + self.hold {
                group.set_turn(Turn::Waiting);
                again.push(Arc::clone(group));
                return false;
            }
            true
        });
        self.waiting.extend(again);
        if slow {
            if now >= self.lowered + self.hold {
                self.limit = (self.limit / 2).max(1);
                self.lowered = now;
            }
        } else {
            self.limit = (self.limit + ended).min(self.most);
        }
    }

    fn report(&mut self, registry: &Registry) {
        let load = ElectionLoad {
            in_flight: self.held.len(),
            waiting: self.waiting.len(),
            limit: self.limit,
        };
        if load != self.reported {
            metrics::gauge!(ELECTIONS_IN_FLIGHT).set(load.in_flight as f64);
            metrics::gauge!(ELECTIONS_WAITING).set(load.waiting as f64);
            registry.set_election_load(load);
            self.reported = load;
        }
    }
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
