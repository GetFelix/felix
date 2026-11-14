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
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use super::group::{Group, Timers, Turn};
use super::runtime::{ElectionLoad, Registry};

/// Times the ticker ended on a panic and was started again. Any increase
/// is a bug: no group on the broker was ticked in between.
const TICKER_RESTARTS_TOTAL: &str = "felix_broker_raft_ticker_restarts_total";

/// Groups on this broker in an election now: this broker's member stood
/// and does not lead, follow or know it lost yet.
const ELECTIONS_IN_FLIGHT: &str = "felix_broker_raft_elections_in_flight";

/// Groups whose timer check could start an election and that wait for a
/// place, the ones waiting to stand again after a lost election included.
const ELECTIONS_WAITING: &str = "felix_broker_raft_elections_waiting";

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
    let mut elections = Elections::new(&timers, Arc::clone(&registry.vote_saves));
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
/// one only while a place is free. Holding a check back is as safe as
/// dropping one.
///
/// A place is taken by a check until openraft has run it, and then only if
/// the member stood: openraft picks the moment, so most checks find the
/// timeout not yet passed and give the place straight back. A member that
/// stood keeps its place, unchecked, until its election is over: it leads,
/// follows or voted for another, or it lost. It lost when every vote
/// request came back and it does not lead, or after `hold`.
///
/// A member that lost waits apart from the others, in `lost`, until its
/// group has a leader. Those take turns on a few places of their own, so
/// members that cannot win (no quorum in reach) cost the rest one lost
/// round each and nothing after.
///
/// `limit` follows the disk and nothing else: it starts at the configured
/// bound, falls when a vote save here took longer than `slow`, and rises
/// by one for each election that ends.
struct Elections {
    /// The configured bound. 0 is none: every due group is checked.
    most: usize,
    limit: usize,
    /// Waiting for a place, in the order they came due.
    fresh: VecDeque<Queued>,
    /// Waiting to stand again after a lost election.
    lost: VecDeque<Queued>,
    probes: Vec<Probe>,
    held: Vec<Held>,
    /// A vote save that took longer than this lowers `limit`.
    slow: u64,
    /// The longest a member that stood keeps its place. openraft lets it
    /// stand again after this, so its round is over either way.
    hold: u64,
    tick: u64,
    saves: Arc<VoteSaves>,
    lowered: Lowered,
    peak: usize,
    reported: ElectionLoad,
}

/// One retry place for this many of `limit`, and never fewer than one.
const RETRY_SHARE: usize = 8;

/// How long this broker's vote saves take: the slowest since the ticker
/// last looked, waits for the disk included.
#[derive(Default)]
pub(crate) struct VoteSaves {
    /// When `limit` was last lowered for a slow save.
    since: AtomicU64,
    /// Of the saves that began after `since`, under the limit as it is.
    slowest: AtomicU64,
    /// Of the saves that began before it, under the limit as it was.
    slowest_before: AtomicU64,
    /// Of every save, for tests that must know the disk was never slow.
    #[cfg(test)]
    pub(crate) slowest_ever: AtomicU64,
}

impl VoteSaves {
    /// A vote save that ended at `now` took `took`.
    pub(crate) fn saved(&self, now: u64, took: u64) {
        let slowest = match now.saturating_sub(took) >= self.since.load(Ordering::Relaxed) {
            true => &self.slowest,
            false => &self.slowest_before,
        };
        slowest.fetch_max(took, Ordering::Relaxed);
        #[cfg(test)]
        self.slowest_ever.fetch_max(took, Ordering::Relaxed);
    }
}

/// The last time `limit` was lowered.
#[derive(Default)]
struct Lowered {
    /// The limit before.
    from: usize,
    /// The slow save it was lowered for.
    took: u64,
}

/// A member waiting for a place.
struct Queued {
    group: Arc<Group>,
    /// Its term when last looked at. A candidate at a higher one stood.
    term: u64,
    /// openraft cannot let it stand before this.
    after: u64,
}

/// A member sent a check that may have it stand.
struct Probe {
    member: Queued,
    retry: bool,
    /// The vote requests it had sent before the check.
    sent: u64,
    /// The term it is a candidate in once the check has run.
    answer: JoinHandle<Option<u64>>,
}

/// A member that stood and whose election is not over.
struct Held {
    group: Arc<Group>,
    retry: bool,
    since: u64,
    sent: u64,
    /// Its vote requests were all back when last looked at. It is given
    /// one more interval to be seen leading.
    answered: bool,
}

impl Elections {
    fn new(timers: &Timers, saves: Arc<VoteSaves>) -> Self {
        Self {
            most: timers.elections,
            limit: timers.elections,
            fresh: VecDeque::new(),
            lost: VecDeque::new(),
            probes: Vec::new(),
            held: Vec::new(),
            slow: timers.election_min / 4,
            hold: timers.election_min,
            tick: timers.tick,
            saves,
            lowered: Lowered::default(),
            peak: 0,
            reported: ElectionLoad::default(),
        }
    }

    /// The groups of `due` to check now: the ones that cannot stand. The
    /// ones that can are queued, and those with a place are sent a check
    /// from here.
    fn admit(&mut self, due: Vec<Arc<Group>>, timers: &Timers) -> Vec<Arc<Group>> {
        if self.most == 0 {
            return due;
        }
        let now = timers.now();
        self.settle(now);
        let mut checked = Vec::with_capacity(due.len());
        let mut arrived = Vec::new();
        for group in due {
            match group.standing_term() {
                // A leader's heartbeat, or a learner: never held back.
                None => {
                    group.set_lost(false);
                    checked.push(group);
                }
                Some(term) if group.turn() == Turn::None => {
                    match now < group.may_stand_at() {
                        // Inside its vote hold. The check moves its due time.
                        true => checked.push(group),
                        false => arrived.push((group, term)),
                    }
                }
                Some(_) => {}
            }
        }
        // By a hash of the member, so two brokers that lost the same
        // leader do not stand for the same groups in the same moment.
        arrived.sort_unstable_by_key(|(group, _)| group.rank());
        for (group, term) in arrived {
            group.set_turn(Turn::Waiting);
            let queue = match group.lost() {
                true => &mut self.lost,
                false => &mut self.fresh,
            };
            queue.push_back(Queued {
                group,
                term,
                after: 0,
            });
        }
        let retries = self.taken(true);
        let mut places = self.limit.saturating_sub(self.taken(false));
        while places > 0
            && let Some(member) = self.fresh.pop_front()
        {
            self.probe(member, false);
            places -= 1;
        }
        let mut places = (self.limit / RETRY_SHARE).max(1).saturating_sub(retries);
        while places > 0
            && let Some(member) = self.next_lost(now)
        {
            self.probe(member, true);
            places -= 1;
        }
        checked
    }

    /// The places taken, by members that lost before or by the rest.
    fn taken(&self, retry: bool) -> usize {
        let probes = self.probes.iter().filter(|probe| probe.retry == retry);
        let held = self.held.iter().filter(|held| held.retry == retry);
        probes.count() + held.count()
    }

    /// The next member to stand again: one a peer asked for a vote, else
    /// the one that has waited longest.
    fn next_lost(&mut self, now: u64) -> Option<Queued> {
        let ready = |member: &Queued| now >= member.after;
        let next = self
            .lost
            .iter()
            .position(|member| ready(member) && member.group.asked())
            .or_else(|| self.lost.iter().position(ready))?;
        let member = self.lost.remove(next)?;
        member.group.clear_asked();
        Some(member)
    }

    /// Send `member` a check, on a task of its own: it keeps its place
    /// until openraft has run the check, however long the group's queue.
    fn probe(&mut self, member: Queued, retry: bool) {
        let group = Arc::clone(&member.group);
        group.set_turn(Turn::Admitted);
        self.probes.push(Probe {
            sent: group.votes_sent(),
            answer: tokio::spawn(async move { group.probe().await }),
            member,
            retry,
        });
    }

    /// Look at every member with a place or in a queue: which stood, whose
    /// election is over, and which no longer wait.
    fn settle(&mut self, now: u64) {
        for probe in std::mem::take(&mut self.probes) {
            if !probe.answer.is_finished() {
                self.probes.push(probe);
                continue;
            }
            let Probe {
                mut member,
                retry,
                sent,
                answer,
            } = probe;
            let candidate = answer.now_or_never().and_then(Result::ok).flatten();
            if candidate.is_some_and(|term| term > member.term) {
                self.held.push(Held {
                    group: member.group,
                    retry,
                    since: now,
                    sent,
                    answered: false,
                });
                continue;
            }
            // Its timeout has not passed. Others get the place meanwhile.
            member.term = candidate.unwrap_or(member.term);
            member.group.set_turn(Turn::Waiting);
            match retry {
                true => self.lost.push_back(member),
                false => self.fresh.push_back(member),
            }
        }
        self.peak = self.peak.max(self.held.len());

        let mut ended = 0;
        for mut held in std::mem::take(&mut self.held) {
            let group = &held.group;
            let term = group.standing_term();
            let Some(term) = term.filter(|_| now >= group.may_stand_at()) else {
                // It leads, or it took a leader's append or voted.
                group.set_turn(Turn::None);
                ended += 1;
                continue;
            };
            let back = group.votes_are_back(held.sent);
            if (back && held.answered) || now >= held.since + self.hold {
                group.set_lost(true);
                group.set_turn(Turn::Waiting);
                self.lost.push_back(Queued {
                    group: held.group,
                    term,
                    after: (held.since + self.hold).saturating_sub(self.tick),
                });
                continue;
            }
            held.answered = back;
            self.held.push(held);
        }

        // One that heard a leader, voted, leads or was closed waits no more.
        let waits = |member: &Queued| {
            let group = &member.group;
            let waits = group.standing_term().is_some() && now >= group.may_stand_at();
            if !waits {
                group.set_turn(Turn::None);
            }
            waits
        };
        self.fresh.retain(waits);
        self.lost.retain(waits);

        // To what the disk would have saved inside `slow`.
        let fits = |from: usize, took: u64| ((from as u64 * self.slow / took) as usize).max(1);
        let slowest = self.saves.slowest.swap(0, Ordering::Relaxed);
        let before = self.saves.slowest_before.swap(0, Ordering::Relaxed);
        if slowest > self.slow {
            self.lowered = Lowered {
                from: self.limit,
                took: slowest,
            };
            self.limit = fits(self.limit, slowest);
            self.saves.since.store(now, Ordering::Relaxed);
        } else if before > self.lowered.took {
            // Queued under the limit as it was, and slower than the save
            // it was lowered for: that limit was further off than it
            // looked.
            self.lowered.took = before;
            self.limit = self.limit.min(fits(self.lowered.from, before));
        } else {
            self.limit = (self.limit + ended).min(self.most);
        }
    }

    fn report(&mut self, registry: &Registry) {
        let load = ElectionLoad {
            in_flight: self.held.len(),
            waiting: self.fresh.len() + self.lost.len(),
            limit: self.limit,
            peak: self.peak,
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

#[cfg(test)]
mod tests;
