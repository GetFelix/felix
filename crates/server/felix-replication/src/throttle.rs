//! A bandwidth limit on copying a shard to a move's destination.
//!
//! One token bucket per broker, shared by every shard it leads, since the
//! limit is there to protect this broker's disk and network from the moves it
//! is a source for. Only the destination is paced, and only when the quorum
//! does not need it: a learner, which the quorum leaves out, or a replica the
//! rest of the set can make a majority without. A `Quorum` publish never
//! waits on the limit.

use std::sync::Mutex;
use std::time::Duration;

use felix_router::Route;
use tokio::time::Instant;

use super::follower::FollowerCursor;
use super::majority_of;
use super::metrics;

/// The most one pass waits on the limit for one destination before leaving
/// the rest to the next pass. A pass ends when its slowest follower does, so
/// this bounds what pacing adds to a pass.
pub(super) const PACE_SLICE: Duration = Duration::from_millis(50);

/// A token bucket in bytes, refilled at `bytes_per_sec` and holding at most
/// one second's worth.
///
/// A batch is sent whole and charged after, so the bucket can go below zero;
/// nothing more is sent until it has refilled past zero again. That keeps the
/// average at the limit without splitting batches.
#[derive(Debug)]
pub struct MoveThrottle {
    bytes_per_sec: u64,
    bucket: Mutex<Bucket>,
}

impl MoveThrottle {
    /// `0` is unlimited.
    pub fn new(bytes_per_sec: u64) -> Self {
        Self {
            bytes_per_sec,
            bucket: Mutex::new(Bucket {
                tokens: bytes_per_sec as i128,
                refilled: Instant::now(),
            }),
        }
    }

    pub fn unlimited() -> Self {
        Self::new(0)
    }

    pub fn is_limited(&self) -> bool {
        self.bytes_per_sec > 0
    }

    /// How long until the next batch may go.
    pub fn wait(&self) -> Duration {
        if !self.is_limited() {
            return Duration::ZERO;
        }
        let mut bucket = self.bucket.lock().expect("move throttle");
        bucket.refill(self.bytes_per_sec);
        if bucket.tokens >= 0 {
            return Duration::ZERO;
        }
        let nanos = (-bucket.tokens) * 1_000_000_000 / self.bytes_per_sec as i128;
        Duration::from_nanos(nanos.min(u64::MAX as i128) as u64)
    }

    /// Charge a batch that was sent.
    pub fn charge(&self, bytes: u64) {
        if !self.is_limited() {
            return;
        }
        metrics::record_move_throttled(bytes);
        let mut bucket = self.bucket.lock().expect("move throttle");
        bucket.refill(self.bytes_per_sec);
        bucket.tokens -= bytes as i128;
    }

    /// Wait for the limit before sending to a paced destination. `false` when
    /// the wait would run past this pass's slice, which started at `started`:
    /// the pass has waited its share, and the copy goes on in the next one.
    pub(super) async fn pace(&self, started: Instant) -> bool {
        let wait = self.wait();
        if wait.is_zero() {
            return true;
        }
        let left = PACE_SLICE.saturating_sub(started.elapsed());
        tokio::time::sleep(wait.min(left)).await;
        wait <= left
    }
}

#[derive(Debug)]
struct Bucket {
    tokens: i128,
    refilled: Instant,
}

impl Bucket {
    fn refill(&mut self, bytes_per_sec: u64) {
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(self.refilled);
        self.refilled = now;
        let earned = elapsed.as_nanos() as i128 * bytes_per_sec as i128 / 1_000_000_000;
        self.tokens = (self.tokens + earned).min(bytes_per_sec as i128);
    }
}

/// The move's destination, when the rest of the replica set can make a
/// majority without it. For a destination that is not a learner; a learner is
/// always paced.
///
/// Counted as the quorum is: the destination is one of the replicas, so the
/// majority is of the whole set, and the others have to supply it alone.
pub(super) fn paced_destination<'a>(
    route: &'a Route,
    followers: &[FollowerCursor],
) -> Option<&'a str> {
    let successor = route.successor.as_deref()?;
    if !followers.iter().any(|f| f.node_id == successor) {
        return None;
    }
    let others = followers
        .iter()
        .filter(|f| f.node_id != successor && f.halted.is_none())
        .count();
    // `majority_of` counts the leader.
    (others + 1 >= majority_of(followers.len())).then_some(successor)
}

#[cfg(test)]
mod tests;
