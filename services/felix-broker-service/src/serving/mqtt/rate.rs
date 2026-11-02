//! How fast new connections are taken off the kernel's queue.

use std::time::Duration;

use tokio::time::{Instant, sleep};

/// A token bucket with a tenth of a second of burst, so a steady rate does not
/// depend on the timer being finer than a millisecond.
pub(super) struct RateGate {
    per_sec: f64,
    burst: f64,
    tokens: f64,
    last: Instant,
}

impl RateGate {
    /// `0` never waits.
    pub(super) fn new(per_sec: u32) -> Self {
        let burst = (f64::from(per_sec) / 10.0).max(1.0);
        Self {
            per_sec: f64::from(per_sec),
            burst,
            tokens: burst,
            last: Instant::now(),
        }
    }

    /// Wait for a place in the rate.
    pub(super) async fn admit(&mut self) {
        if self.per_sec == 0.0 {
            return;
        }
        loop {
            let now = Instant::now();
            let earned = now.duration_since(self.last).as_secs_f64() * self.per_sec;
            self.last = now;
            self.tokens = (self.tokens + earned).min(self.burst);
            if self.tokens >= 1.0 {
                self.tokens -= 1.0;
                return;
            }
            sleep(Duration::from_secs_f64((1.0 - self.tokens) / self.per_sec)).await;
        }
    }
}
