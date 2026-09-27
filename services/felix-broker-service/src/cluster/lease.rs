//! The broker's authority to serve the shards it leads.
//!
//! An assignment says who *should* lead a shard. A lease says who may still act
//! on that, and it expires on its own. The difference matters because a
//! revocation cannot be delivered to a broker that is partitioned — which is
//! exactly the case where it is needed. See `docs/replication-design.md`.
//!
//! # The lease is the heartbeat
//!
//! No separate protocol. A broker renews by heartbeating, which it already
//! does, and the control plane grants by keeping the node live, which it already
//! decides. The lease is the time since the last *accepted* heartbeat, measured
//! on the broker's own monotonic clock — so the two ends never compare wall
//! clocks, and no clock synchronisation is assumed.
//!
//! # Two checks, and why they differ
//!
//! `docs/replication-design.md` requires the lease be checked when a request is
//! admitted and again before its record is committed. They are not the same
//! check:
//!
//! - **Admission** ([`LeaseState::looks_valid`]) reads one atomic and may be
//!   stale by up to the refresh interval. It sheds early and cheaply, on a path
//!   that prides itself on costing two atomic loads.
//! - **Commit** ([`LeaseState::is_valid_now`]) reads the clock. It is the
//!   authoritative one, and it is the reason a process suspended past its expiry
//!   cannot write on waking: the cached flag would still say yes, and the clock
//!   says no. It sits behind an fsync, so its cost is not measurable. The
//!   shard fence makes it, because every write path (publish, forward, cache,
//!   counter, group) enters the fence; a `Quorum` ack makes it once more
//!   before it is released.
pub mod metrics;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// How much of the lease is given up as safety margin.
///
/// The broker stops serving this long before the control plane could possibly
/// regrant, which is the gap that makes two leaders impossible. Expressed as a
/// fraction because it has to scale with the lease: a fixed margin is either
/// most of a short lease or negligible against a long one.
const SAFETY_MARGIN_FRACTION: u32 = 4;

/// How often the cached admission flag is refreshed, as a fraction of the
/// margin. Frequent enough that the cached answer is never far behind, cheap
/// enough to be invisible.
const REFRESH_FRACTION: u32 = 4;

/// Whether this broker may currently serve the shards it leads.
#[derive(Debug)]
pub struct LeaseState {
    /// Millis since `base` of the last accepted heartbeat, **plus one**.
    ///
    /// Zero means never renewed. The offset is what keeps that sentinel from
    /// colliding with a real renewal in the first millisecond of the process,
    /// which is otherwise a broker that heartbeats immediately and reads as
    /// holding no lease.
    renewed_at_millis: AtomicU64,
    /// The admission-path answer. Refreshed on a timer; authoritative only in
    /// the negative direction, since it can lag a renewal but is re-derived
    /// from the clock before any commit.
    looks_valid: AtomicBool,
    /// Monotonic origin. `Instant` has no representation to store, so
    /// everything is kept as a duration from here.
    base: Instant,
    /// How long a lease lasts without renewal, already net of the margin, in
    /// millis. Atomic because the control plane owns this cadence and can change
    /// it, exactly as it owns the heartbeat interval.
    usable_millis: AtomicU64,
}

impl LeaseState {
    /// A lease of `lease` duration, of which a quarter is held back as margin.
    ///
    /// Starts **invalid**: a broker has no authority until its first heartbeat
    /// is accepted. Starting valid would let a broker that never successfully
    /// registered serve for a full lease period.
    pub fn new(lease: Duration) -> Self {
        Self {
            renewed_at_millis: AtomicU64::new(0),
            looks_valid: AtomicBool::new(false),
            base: Instant::now(),
            usable_millis: AtomicU64::new(usable_millis(lease)),
        }
    }

    /// Adopt the control plane's expiry window.
    ///
    /// The lease must never outlive it: a broker still serving after the control
    /// plane has declared it down is the split this exists to prevent. Taken
    /// from the heartbeat response for the same reason the interval is — the
    /// cadence belongs to the control plane, not to broker configuration.
    pub fn adopt(&self, expiry_timeout: Duration) {
        self.usable_millis
            .store(usable_millis(expiry_timeout), Ordering::Release);
    }

    /// How long a renewal is good for, net of the margin.
    pub fn usable(&self) -> Duration {
        Duration::from_millis(self.usable_millis.load(Ordering::Acquire))
    }

    /// Record an accepted heartbeat, anchored at the moment it was **sent**.
    ///
    /// Anchoring at arrival instead would add the round trip — and any pause in
    /// it — to a lease the control plane already started counting, which is the
    /// safety interval being spent.
    pub fn renew_at(&self, sent: Instant) {
        let stamp = sent.saturating_duration_since(self.base).as_millis() as u64 + 1;
        // `fetch_max`, not `store`: two heartbeat responses can race, and an
        // out-of-order one must not move the anchor backwards and shorten the
        // lease.
        self.renewed_at_millis.fetch_max(stamp, Ordering::Release);
        self.looks_valid.store(true, Ordering::Release);
    }

    /// [`LeaseState::renew_at`] anchored at now. Tests only — the heartbeat
    /// path has a send instant, and using it is the point.
    #[cfg(test)]
    pub fn renew(&self) {
        self.renew_at(Instant::now());
    }

    /// Give up the lease immediately.
    ///
    /// For a broker that has been told it is no longer a member: it should stop
    /// serving now rather than run out the remaining margin.
    pub fn surrender(&self) {
        self.looks_valid.store(false, Ordering::Release);
        self.renewed_at_millis.store(0, Ordering::Release);
    }

    /// The cheap admission check. One relaxed load.
    #[inline]
    pub fn looks_valid(&self) -> bool {
        self.looks_valid.load(Ordering::Relaxed)
    }

    /// The authoritative check, against the clock.
    ///
    /// Called before a record is committed. A broker suspended past its expiry
    /// fails here even though the cached flag still says otherwise, which is the
    /// entire reason this exists separately.
    pub fn is_valid_now(&self) -> bool {
        !self.remaining().is_zero()
    }

    /// How long the lease has left, against the clock. Zero once it has lapsed
    /// or was never held.
    pub fn remaining(&self) -> Duration {
        let stamp = self.renewed_at_millis.load(Ordering::Acquire);
        if stamp == 0 {
            return Duration::ZERO;
        }
        let elapsed = self.base.elapsed().as_millis() as u64 + 1;
        self.usable()
            .saturating_sub(Duration::from_millis(elapsed.saturating_sub(stamp)))
    }

    /// Keep the cached flag in step with the clock.
    ///
    /// Only ever moves it to `false`; a renewal is what moves it back. That
    /// asymmetry is deliberate — this task being late must not extend a lease.
    pub fn spawn_refresh(
        self: Arc<Self>,
        shutdown: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let period = (self.usable() / (SAFETY_MARGIN_FRACTION * REFRESH_FRACTION))
            .max(Duration::from_millis(10));
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(period);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    _ = ticker.tick() => {}
                }
                if !self.is_valid_now() && self.looks_valid.swap(false, Ordering::AcqRel) {
                    tracing::warn!(
                        usable_ms = self.usable().as_millis() as u64,
                        "lease expired; this broker is no longer serving the shards it led",
                    );
                    crate::cluster::lease::metrics::record_expiry();
                }
                crate::cluster::lease::metrics::set_held(self.is_valid_now());
            }
        })
    }
}

/// A lease is only usable for part of its life; the rest is the safety margin
/// the control plane's regrant wait is measured against.
fn usable_millis(lease: Duration) -> u64 {
    lease
        .saturating_sub(lease / SAFETY_MARGIN_FRACTION)
        .as_millis() as u64
}

#[cfg(test)]
mod tests;
