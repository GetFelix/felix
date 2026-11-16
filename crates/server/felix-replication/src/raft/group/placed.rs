//! Hands a group's leadership to the member placement names, on the member
//! that leads it. The membership driver asks this only when it has no
//! membership work to do. See `docs/per-shard-raft.md`, "Leaders follow
//! placement".

use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use crate::raft::membership::View;
use crate::raft::types::MemberId;

pub(crate) const PLACED_TRANSFERS_TOTAL: &str = "felix_broker_raft_placed_transfers_total";

/// How often a leader held back from a transfer looks again. A preferred
/// member must stay caught up between looks, so this also bounds how much
/// of a lag the settle window can miss.
const POLL: Duration = Duration::from_secs(1);

/// How a leader hands its leadership to the broker placement names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placement {
    /// How long the preferred member must stay caught up and answering
    /// before a transfer is asked. A broker that just restarted is still
    /// opening members, and may go away again.
    pub settle: Duration,
    /// How long a leader leads its term before it hands it on, so a group
    /// that just elected does not elect again at once. Waived in a group's
    /// first term, which the broker that formed it wins.
    pub led_min: Duration,
    /// The wait after a transfer that left this member leading. It doubles
    /// on each one up to `backoff_cap`, with 20% jitter either way.
    pub backoff: Duration,
    pub backoff_cap: Duration,
    /// Placement transfers one broker has in flight at once. Each closes
    /// its shard for up to an election timeout.
    pub permits: usize,
    /// Placement transfers one broker starts per second, at most.
    pub starts_per_second: u32,
}

impl Default for Placement {
    fn default() -> Self {
        Self {
            settle: Duration::from_secs(5),
            led_min: Duration::from_secs(10),
            backoff: Duration::from_secs(30),
            backoff_cap: Duration::from_secs(600),
            permits: 2,
            starts_per_second: 5,
        }
    }
}

/// The placement transfers one broker may run, shared by its groups.
/// Waiters are served in order, so a group that just finished cannot take
/// its permit again ahead of the others.
pub(crate) struct Budget {
    permits: Arc<Semaphore>,
    total: usize,
    gap: Duration,
    next: parking_lot::Mutex<Option<Instant>>,
    /// The most permits ever out at once.
    #[cfg(any(test, feature = "testing"))]
    peak: std::sync::atomic::AtomicUsize,
}

impl Budget {
    pub(crate) fn new(placement: &Placement) -> Self {
        let total = placement.permits.max(1);
        Self {
            permits: Arc::new(Semaphore::new(total)),
            total,
            gap: Duration::from_secs(1) / placement.starts_per_second.max(1),
            next: parking_lot::Mutex::new(None),
            #[cfg(any(test, feature = "testing"))]
            peak: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Wait for a permit, then for this broker's next start slot.
    pub(crate) async fn take(&self) -> OwnedSemaphorePermit {
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .expect("the budget's semaphore is never closed");
        let at = {
            let mut next = self.next.lock();
            let now = Instant::now();
            let at = next.map_or(now, |next| next.max(now));
            *next = Some(at + self.gap);
            at
        };
        tokio::time::sleep_until(at).await;
        #[cfg(any(test, feature = "testing"))]
        self.peak.fetch_max(
            self.total - self.permits.available_permits(),
            std::sync::atomic::Ordering::Relaxed,
        );
        permit
    }

    #[cfg(any(test, feature = "testing"))]
    pub(crate) fn peak(&self) -> usize {
        self.peak.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Why a leader keeps a leadership placement would move.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Held {
    /// The membership is joint or not committed, or nothing of this term
    /// is committed yet.
    Unsettled,
    /// The preferred member is behind or not answering.
    Lagging,
    /// Another voter is behind or not answering. A transfer now could leave
    /// the group a bare majority with no leader.
    Degraded,
}

/// The member placement wants to lead, if the leader `view` describes
/// should hand it the leadership now, or why not yet. `None` when there is
/// nothing to hand over: no preference, this member is preferred, or the
/// preferred broker holds no voter of the group. `live` holds the members
/// that answered this leader lately.
pub(crate) fn placed_step(
    view: &View,
    live: &BTreeSet<MemberId>,
    preferred: Option<&str>,
) -> Option<Result<MemberId, Held>> {
    let preferred = preferred?;
    if *view.me.broker == *preferred {
        return None;
    }
    let voters = &view.config.new;
    let to = voters.iter().find(|voter| *voter.broker == *preferred)?;
    if view.config.old != view.config.new
        || !view.config_committed
        || !view.own_term_committed
        || view.transferring
    {
        return Some(Err(Held::Unsettled));
    }
    let caught_up = |member: &MemberId| {
        live.contains(member)
            && view.matched.get(member).is_some_and(|matched| {
                view.last_index.saturating_sub(*matched) <= view.lag_threshold
            })
    };
    if !caught_up(to) {
        return Some(Err(Held::Lagging));
    }
    if !voters
        .iter()
        .filter(|voter| **voter != view.me)
        .all(caught_up)
    {
        return Some(Err(Held::Degraded));
    }
    Some(Ok(to.clone()))
}

/// What the placement rule wants the driver to do.
#[derive(Debug)]
pub(crate) enum Decision {
    /// Nothing to hand over.
    Nothing,
    /// Look again after this long.
    Hold(Duration),
    /// Wait for a permit from the broker's budget.
    Queue,
    /// Hand the leadership to this member, within this long.
    Transfer(MemberId, Duration),
}

/// What a leader keeps between looks to pace its placement transfers. All
/// of it is pacing: a new leader starts from the log.
pub(crate) struct Placing {
    config: Placement,
    /// How long a placement transfer waits for its target to hold the
    /// whole log: the shortest election timeout.
    within: Duration,
    /// When a permit is given back if the term has not ended by then.
    hold_for: Duration,
    pub(crate) budget: Arc<Budget>,
    preferred: Option<String>,
    /// The term this member was first seen leading, and when.
    led: Option<(u64, Instant)>,
    /// The preferred member, and since when it has been caught up.
    caught_up: Option<(MemberId, Instant)>,
    /// The term a placement transfer was asked in.
    asked: Option<u64>,
    /// No transfer is asked before this.
    backoff: Option<Instant>,
    failures: u32,
    /// A permit taken and not used yet.
    pub(crate) ready: Option<OwnedSemaphorePermit>,
    /// The permit of a transfer in flight: its term and when it is given
    /// back anyway.
    in_flight: Option<(u64, Instant, OwnedSemaphorePermit)>,
    /// The last reason counted, so a long hold is counted once.
    last_held: Option<&'static str>,
}

impl Placing {
    pub(crate) fn new(
        config: Placement,
        within: Duration,
        hold_for: Duration,
        budget: Arc<Budget>,
    ) -> Self {
        Self {
            config,
            within,
            hold_for,
            budget,
            preferred: None,
            led: None,
            caught_up: None,
            asked: None,
            backoff: None,
            failures: 0,
            ready: None,
            in_flight: None,
            last_held: None,
        }
    }

    /// Placement now names `broker`. A new name starts the pacing again.
    pub(crate) fn prefer(&mut self, broker: Option<String>) {
        if self.preferred != broker {
            self.preferred = broker;
            self.caught_up = None;
            self.asked = None;
            self.backoff = None;
            self.failures = 0;
            self.last_held = None;
        }
    }

    /// This member does not lead. A transfer it asked went through.
    pub(crate) fn not_leading(&mut self) {
        if self.asked.take().is_some() {
            metrics::counter!(PLACED_TRANSFERS_TOTAL, "outcome" => "landed").increment(1);
            self.backoff = None;
            self.failures = 0;
        }
        self.led = None;
        self.caught_up = None;
        self.ready = None;
        self.in_flight = None;
        self.last_held = None;
    }

    /// What to do as the leader of `term` that `view` describes.
    pub(crate) fn decide(
        &mut self,
        me: &MemberId,
        view: &View,
        live: &BTreeSet<MemberId>,
        term: u64,
    ) -> Decision {
        let now = Instant::now();
        if self
            .in_flight
            .as_ref()
            .is_some_and(|(at, until, _)| *at != term || now >= *until)
        {
            self.in_flight = None;
        }
        let led = match self.led {
            Some((at, since)) if at == term => since,
            _ => {
                self.led = Some((term, now));
                now
            }
        };
        match self.asked {
            Some(asked) if asked == term => return Decision::Hold(POLL),
            // Still leading in a later term: the transfer did not go through.
            Some(_) => {
                self.asked = None;
                self.failed(me, now);
            }
            None => {}
        }
        let step = placed_step(view, live, self.preferred.as_deref());
        let to = match step {
            None => {
                self.release();
                return Decision::Nothing;
            }
            Some(Err(held)) => {
                self.release();
                // The settle window starts again once it is caught up.
                self.caught_up = None;
                self.held(match held {
                    Held::Unsettled => "unsettled",
                    Held::Lagging => "lagging",
                    Held::Degraded => "degraded",
                });
                return Decision::Hold(POLL);
            }
            Some(Ok(to)) => to,
        };
        let since = match &self.caught_up {
            Some((member, since)) if *member == to => *since,
            _ => {
                self.caught_up = Some((to.clone(), now));
                now
            }
        };
        // The broker that formed the group wins its first term: there is no
        // earlier leader to churn with, and its members just answered. Only
        // while that term is young: a later preference waits like any other.
        let first_term = term <= 1 && now.duration_since(led) < self.config.led_min;
        let mut not_before = self.backoff;
        if !first_term {
            not_before = not_before
                .max(Some(since + self.config.settle))
                .max(Some(led + self.config.led_min));
        }
        if let Some(at) = not_before.filter(|at| *at > now) {
            self.release();
            return Decision::Hold((at - now).min(POLL));
        }
        if self.ready.is_none() {
            self.held("budget");
            return Decision::Queue;
        }
        Decision::Transfer(to, self.within)
    }

    /// A transfer was asked in `term`. Its permit is held until the term
    /// ends or the transfer has had its time.
    pub(crate) fn asked(&mut self, term: u64) {
        self.asked = Some(term);
        self.last_held = None;
        if let Some(permit) = self.ready.take() {
            self.in_flight = Some((term, Instant::now() + self.hold_for, permit));
        }
    }

    /// The driver has membership work in `term` instead. A permit taken
    /// and not used goes back, and so does one whose transfer's term ended,
    /// so membership work never holds the broker's budget.
    pub(crate) fn busy(&mut self, term: u64) {
        self.ready = None;
        if self
            .in_flight
            .as_ref()
            .is_some_and(|(at, _, _)| *at != term)
        {
            self.in_flight = None;
        }
    }

    /// The leader of `term` waits on a target told for an earlier term.
    /// Placement work is dropped as in `busy`, and a transfer asked in an
    /// earlier term counts as failed, as `decide` would have counted it.
    pub(crate) fn untold(&mut self, me: &MemberId, term: u64) {
        self.busy(term);
        if self.asked.is_some_and(|asked| asked != term) {
            self.asked = None;
            self.failed(me, Instant::now());
        }
    }

    /// Whether a permit is held, ready or in flight.
    #[cfg(test)]
    pub(crate) fn holds_permit(&self) -> bool {
        self.ready.is_some() || self.in_flight.is_some()
    }

    /// Failed transfers counted since the last landing or new preference.
    #[cfg(test)]
    pub(crate) fn failures(&self) -> u32 {
        self.failures
    }

    /// How long a transfer asked may take before its permit is given back.
    pub(crate) fn hold_for(&self) -> Duration {
        self.hold_for
    }

    fn release(&mut self) {
        self.ready = None;
    }

    fn held(&mut self, why: &'static str) {
        if self.last_held != Some(why) {
            self.last_held = Some(why);
            metrics::counter!(PLACED_TRANSFERS_TOTAL, "outcome" => "held", "why" => why)
                .increment(1);
        }
    }

    fn failed(&mut self, me: &MemberId, now: Instant) {
        metrics::counter!(PLACED_TRANSFERS_TOTAL, "outcome" => "failed").increment(1);
        self.failures = self.failures.saturating_add(1);
        let doubled = self
            .config
            .backoff
            .saturating_mul(1 << (self.failures - 1).min(16));
        let wait = doubled.min(self.config.backoff_cap);
        // Spread from the member and the count, not a random draw, so a
        // simulation run replays the same waits.
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        (me, self.failures).hash(&mut hasher);
        let percent = 80 + (hasher.finish() % 41) as u32;
        let wait = wait * percent / 100;
        self.backoff = Some(now + wait);
    }
}
