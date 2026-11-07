//! Holding a write while its shard moves.
//!
//! A planned move leaves a gap between the fence and the cut-over in which
//! nobody serves the shard. A write that reaches a broker in that gap waits
//! here until the broker's routes show where the shard went, and then goes
//! there, instead of being refused. The wait happens before the write is
//! admitted, so nothing is acknowledged while it is held.
//!
//! Bounded twice: in time, by the window, and in how many writes may wait at
//! once, because each one holds its payload. Past either bound the write is
//! refused as `moving` and the client retries.

pub mod metrics;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::time::Instant;

/// How long a write may wait for a move to cut over, and how many may wait.
#[derive(Debug)]
pub struct MoveHold {
    window: Duration,
    max_held: usize,
    held: AtomicUsize,
}

impl MoveHold {
    /// Hold for up to `window`, with at most `max_held` writes waiting.
    pub fn new(window: Duration, max_held: usize) -> Self {
        Self {
            window,
            max_held,
            held: AtomicUsize::new(0),
        }
    }

    /// Never hold: a write to a moving shard is refused at once.
    pub fn disabled() -> Self {
        Self::new(Duration::ZERO, 0)
    }

    /// How many writes are waiting right now.
    pub fn held(&self) -> usize {
        self.held.load(Ordering::Relaxed)
    }

    /// Start holding one write, or say why it cannot be held.
    pub(crate) fn begin(&self) -> Result<Held<'_>, HoldRefused> {
        if self.window.is_zero() {
            return Err(HoldRefused::Full);
        }
        // Counted first and given back on failure, so two writes racing for
        // the last place cannot both get it.
        if self.held.fetch_add(1, Ordering::AcqRel) >= self.max_held {
            self.held.fetch_sub(1, Ordering::AcqRel);
            metrics::record_refused(HoldRefused::Full);
            return Err(HoldRefused::Full);
        }
        metrics::record_held();
        let started = Instant::now();
        Ok(Held {
            hold: self,
            started,
            deadline: started + self.window,
        })
    }
}

/// One write being held. Dropping it frees its place.
#[derive(Debug)]
pub(crate) struct Held<'a> {
    hold: &'a MoveHold,
    started: Instant,
    deadline: Instant,
}

impl Held<'_> {
    pub(crate) fn deadline(&self) -> Instant {
        self.deadline
    }

    /// The move settled and the write goes on.
    pub(crate) fn settled(self) {
        metrics::record_hold_seconds(self.started.elapsed());
    }

    /// The window ran out with the shard still moving.
    pub(crate) fn timed_out(self) {
        metrics::record_hold_seconds(self.started.elapsed());
        metrics::record_refused(HoldRefused::TimedOut);
    }
}

impl Drop for Held<'_> {
    fn drop(&mut self) {
        self.hold.held.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Why a write to a moving shard was refused rather than held.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HoldRefused {
    /// Holding is off, or as many writes as allowed are already waiting.
    Full,
    /// It was held for the whole window and the move had not cut over.
    TimedOut,
}

impl HoldRefused {
    fn label(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::TimedOut => "timed_out",
        }
    }
}
