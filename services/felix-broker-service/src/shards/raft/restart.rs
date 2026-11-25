//! Members whose core stopped on a storage error, and when each may open
//! again. openraft never restarts a stopped core, so the feed pass closes
//! the member and opens it again from disk once the disk takes writes. See
//! `docs/per-shard-raft.md`, "A member that stops on a storage error".

use std::time::Duration;

use felix_common::random::HashMap;
use tokio::time::Instant;

use crate::shards::ShardKey;

/// How long a reopened member must run before its next stop counts as a
/// first one again.
const STEADY: Duration = Duration::from_secs(60);

/// The longest a member that keeps stopping waits between reopens.
const MAX_WAIT: Duration = Duration::from_secs(30);

/// A copy here that a storage error stopped, kept until it rejoins a
/// working group or its shard leaves this broker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoppedCopy {
    /// The term the copy was at when it stopped.
    pub(crate) term: u64,
    pub(crate) error: String,
}

#[derive(Default)]
pub(super) struct Restarts(HashMap<ShardKey, Restart>);

struct Restart {
    /// Stops in a row, each within [`STEADY`] of the reopen before it.
    stops: u32,
    reopened: Option<Instant>,
    not_before: Instant,
}

impl Restarts {
    /// `key`'s member stopped and was closed. The first stop reopens on the
    /// next pass; a member that keeps stopping waits longer each time, since
    /// every open scans its log.
    pub(super) fn stopped(&mut self, key: &ShardKey) {
        let now = Instant::now();
        let restart = self.0.entry(key.clone()).or_insert(Restart {
            stops: 0,
            reopened: None,
            not_before: now,
        });
        if restart
            .reopened
            .is_some_and(|at| now.duration_since(at) >= STEADY)
        {
            restart.stops = 0;
        }
        restart.stops = restart.stops.saturating_add(1);
        restart.reopened = None;
        restart.not_before = now + wait_after(restart.stops);
    }

    /// Whether `key` is closed after a stop, and so has to find the disk
    /// healed before it opens.
    pub(super) fn restarting(&self, key: &ShardKey) -> bool {
        self.0
            .get(key)
            .is_some_and(|restart| restart.reopened.is_none())
    }

    /// Whether `key` may try to open now.
    pub(super) fn due(&self, key: &ShardKey) -> bool {
        self.0
            .get(key)
            .is_none_or(|restart| Instant::now() >= restart.not_before)
    }

    pub(super) fn reopened(&mut self, key: &ShardKey) {
        if let Some(restart) = self.0.get_mut(key) {
            restart.reopened = Some(Instant::now());
        }
    }

    /// Forget the shards `keep` refuses, and members that have run steady.
    pub(super) fn retain(&mut self, keep: impl Fn(&ShardKey) -> bool) {
        let now = Instant::now();
        self.0.retain(|key, restart| {
            keep(key)
                && !restart
                    .reopened
                    .is_some_and(|at| now.duration_since(at) >= STEADY)
        });
    }
}

fn wait_after(stops: u32) -> Duration {
    match stops {
        0 | 1 => Duration::ZERO,
        n => Duration::from_secs(1)
            .saturating_mul(1 << (n - 2).min(5))
            .min(MAX_WAIT),
    }
}

#[cfg(test)]
mod tests;
