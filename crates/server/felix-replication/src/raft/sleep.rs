//! openraft's sleeps, without a timer for a deadline that is nearly here.
//!
//! Tokio's timer wheel fires on whole milliseconds, so a sleep of a few
//! microseconds lasts up to a millisecond. openraft makes such sleeps on
//! the write path: its IO completion forwarder waits until 1 us after it
//! last looked before it forwards a second completion. [`ShardSleep`]
//! yields instead, a bounded number of times, and arms the timer only if
//! the deadline is still ahead after that. It never ends before its
//! deadline, and it always yields at least once.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use openraft::rt::{AsyncRuntime, Instant};
use openraft_rt_tokio::{TokioInstant, TokioRuntime};

#[cfg(test)]
mod tests;

/// Below this, a deadline yields before it arms a timer: one timer tick.
const TICK: Duration = Duration::from_millis(1);

/// How often a close deadline yields before it arms the timer. A task
/// that wakes itself is polled again within a microsecond or so, so one
/// yield is not enough for even a 1 us deadline.
const YIELDS: u8 = 32;

/// A sleep until a deadline. See the module docs.
#[must_use = "a sleep does nothing unless polled"]
pub(crate) enum ShardSleep {
    /// Within a tick: polled again after each yield.
    Soon {
        deadline: TokioInstant,
        yields: u8,
    },
    Timer(Pin<Box<tokio::time::Sleep>>),
}

// Diagnostic counters (#3204), exported once a second.
static CALLS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static SOON: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static YIELDED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static TIMED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn export_counters() {
    static STARTED: std::sync::Once = std::sync::Once::new();
    STARTED.call_once(|| {
        std::thread::spawn(|| {
            use std::sync::atomic::Ordering::Relaxed;
            loop {
                metrics::counter!("felix_diag_shard_sleep_calls_total")
                    .absolute(CALLS.load(Relaxed));
                metrics::counter!("felix_diag_shard_sleep_soon_total").absolute(SOON.load(Relaxed));
                metrics::counter!("felix_diag_shard_sleep_yields_total")
                    .absolute(YIELDED.load(Relaxed));
                metrics::counter!("felix_diag_shard_sleep_timer_total")
                    .absolute(TIMED.load(Relaxed));
                std::thread::sleep(Duration::from_secs(1));
            }
        });
    });
}

impl ShardSleep {
    pub(crate) fn until(deadline: TokioInstant) -> Self {
        export_counters();
        CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if deadline < TokioInstant::now() + TICK {
            SOON.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Self::Soon {
                deadline,
                yields: 0,
            }
        } else {
            Self::timer(deadline)
        }
    }

    /// A duration too long to add to now never ends, as tokio's own sleep.
    pub(crate) fn after(duration: Duration) -> Self {
        match tokio::time::Instant::now().checked_add(duration) {
            Some(_) => Self::until(TokioInstant::now() + duration),
            None => Self::Timer(Box::pin(tokio::time::sleep(duration))),
        }
    }

    fn timer(deadline: TokioInstant) -> Self {
        Self::Timer(Box::pin(TokioRuntime::sleep_until(deadline)))
    }
}

impl Future for ShardSleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = &mut *self;
        if let Self::Soon { deadline, yields } = this {
            // The first yield is unconditional, so a sleep of a nanosecond
            // still yields, as openraft's `yield_now` expects.
            if *yields > 0 && TokioInstant::now() >= *deadline {
                return Poll::Ready(());
            }
            if *yields < YIELDS {
                *yields += 1;
                YIELDED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            // Yielding on would spin, and under a paused test clock
            // nothing would ever move the clock.
            TIMED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            *this = Self::timer(*deadline);
        }
        match this {
            Self::Timer(sleep) => sleep.as_mut().poll(cx),
            Self::Soon { .. } => unreachable!("replaced by a timer above"),
        }
    }
}
