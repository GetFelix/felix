//! The leader lease: when a leader may acknowledge a write on its own log
//! alone. See `docs/per-shard-raft.md`, "Acknowledging a write". The rules
//! here are the ones `docs/formal/FelixRaftLease.tla` checks.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use felix_common::clock;
use tokio::time::Instant;

pub(crate) const NOT_LEADING: &str = "this broker does not lead the term the write was written in";
pub(crate) const UNHEARD: &str = "no majority has answered this leader in this term";
pub(crate) const LAPSED: &str = "no majority has answered this leader within its lease";
pub(crate) const VOID: &str = "this broker gave up its lease for the term";
pub(crate) const WOKE: &str = "this broker was suspended less than one lease ago";

/// Boottime may run ahead of the monotonic clock by this much before it is
/// read as a suspend. Two clock reads are never taken at one instant, and a
/// refusal costs a whole lease of commit waits. A smaller jump is added to
/// the lease's age instead, so it is never lost.
const WAKE_SLACK: Duration = Duration::from_millis(10);

/// What openraft says of this member, read in one look at its metrics.
#[derive(Debug, Clone, Copy)]
pub(crate) struct View {
    /// The term it leads, if it leads one.
    pub(crate) leads: Option<u64>,
    /// How long ago the newest message a majority has answered was handed
    /// off.
    pub(crate) acked: Option<Duration>,
}

/// One member's lease state. In memory only: a restarted member starts with
/// no lease, and with nothing void.
pub(crate) struct Lease {
    length: Duration,
    /// Every term up to this one is refused, for the life of the member.
    /// It only grows: a later void must not lift an earlier one.
    void: AtomicU64,
    origin: Instant,
    wake: parking_lot::Mutex<Wake>,
    /// Suspended time a test has added to boottime.
    #[cfg(test)]
    slept: AtomicU64,
}

/// What the lease knows of suspends. The monotonic clock stops while the
/// host or VM is suspended and boottime does not, so openraft's instants
/// look newer after a wake than they are.
#[derive(Debug, Default)]
pub(crate) struct Wake {
    /// How far boottime was ahead of the monotonic clock at the last jump.
    ahead: Duration,
    /// On the monotonic clock: local acknowledgements are refused until
    /// then.
    refuse_until: Duration,
}

impl Wake {
    pub(crate) fn starting(boot: Duration) -> Self {
        Self {
            ahead: boot,
            refuse_until: Duration::ZERO,
        }
    }

    /// One look at both clocks. `None` while a suspend was seen less than
    /// one `lease` ago. Otherwise the suspended time not counted as a
    /// suspend, to add to the lease's age.
    pub(crate) fn look(
        &mut self,
        mono: Duration,
        boot: Duration,
        lease: Duration,
    ) -> Option<Duration> {
        let ahead = boot.saturating_sub(mono);
        let jumped = ahead.saturating_sub(self.ahead);
        if jumped > WAKE_SLACK {
            self.ahead = ahead;
            self.refuse_until = mono.saturating_add(lease);
        }
        (mono >= self.refuse_until).then_some(jumped)
    }
}

impl Lease {
    pub(crate) fn new(length: Duration) -> Self {
        Self {
            length,
            void: AtomicU64::new(0),
            origin: Instant::now(),
            wake: parking_lot::Mutex::new(Wake::starting(clock::boottime())),
            #[cfg(test)]
            slept: AtomicU64::new(0),
        }
    }

    /// Give up the lease for every term up to `term`.
    pub(crate) fn void_through(&self, term: u64) {
        self.void.fetch_max(term, Ordering::SeqCst);
    }

    /// A message openraft takes names `term`: every term below it is over.
    /// Call it before openraft sees the message. openraft's role reaches
    /// the check late, and until it does the old term would still pass.
    pub(crate) fn named(&self, term: u64) {
        if let Some(below) = term.checked_sub(1) {
            self.void_through(below);
        }
    }

    /// Whether a write of `term` may be acknowledged on this log alone,
    /// given what openraft says when `view` is called.
    pub(crate) fn admit(&self, term: u64, view: impl FnOnce() -> View) -> Result<(), &'static str> {
        // The clocks first, and both in this call: a jump seen by a
        // background task would be seen after the checks it should stop.
        let mono = Instant::now().saturating_duration_since(self.origin);
        let boot = self.boottime();
        let slept = self.wake.lock().look(mono, boot, self.length);
        let slept = slept.ok_or(WOKE)?;
        let view = view();
        if view.leads != Some(term) {
            return Err(NOT_LEADING);
        }
        let acked = view.acked.ok_or(UNHEARD)?;
        if acked.saturating_add(slept) >= self.length {
            return Err(LAPSED);
        }
        // Last, so a void taken at any time before this line refuses.
        if term <= self.void.load(Ordering::SeqCst) {
            return Err(VOID);
        }
        Ok(())
    }

    fn boottime(&self) -> Duration {
        let boot = clock::boottime();
        #[cfg(test)]
        let boot = boot + Duration::from_millis(self.slept.load(Ordering::SeqCst));
        boot
    }

    #[cfg(test)]
    pub(crate) fn void(&self) -> u64 {
        self.void.load(Ordering::SeqCst)
    }

    /// The host was suspended for `time`: boottime ran and the monotonic
    /// clock did not.
    #[cfg(test)]
    pub(crate) fn suspended_for(&self, time: Duration) {
        self.slept
            .fetch_add(time.as_millis() as u64, Ordering::SeqCst);
    }
}
