//! What replication last knew about each shard this broker leads, for an
//! operator asking why one is not serving or which replica is behind.
//!
//! A listing read on demand, like [`crate::halted`], because a shard cannot be
//! a metric label. The driver replaces a shard's entry after each of its passes
//! and fence attempts and drops the shards it stops leading, so a read takes
//! this board's lock and nothing the driver or the shard lifecycle holds.
use std::collections::HashMap;

use parking_lot::RwLock;

use crate::{FollowerCursor, ShardKey};

/// One shard as the driver left it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardStatus {
    /// The leadership the driver is shipping or fencing at.
    pub generation: u64,
    /// The leader's tail when the last pass read it. Absent before the first
    /// pass, and while a promotion is fencing.
    pub tail: Option<u64>,
    /// Each follower's cursor. Empty while fencing: nothing ships until the
    /// fence is done.
    pub followers: Vec<FollowerStatus>,
    /// The promotion fence, while the shard waits for it.
    pub fence: Option<FenceStatus>,
    /// Fenced for a move and not yet reported drained.
    pub drain_pending: bool,
    /// No follower that could still catch up holds the whole log.
    pub behind: bool,
}

/// One follower's cursor, as the last pass left it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FollowerStatus {
    pub node_id: String,
    /// A move's destination still copying, left out of the quorum.
    pub learner: bool,
    pub next_offset: u64,
    /// How far the follower has answered that its log is this leader's.
    pub confirmed: u64,
    /// An exchange with it from an earlier pass is still unanswered.
    pub busy: bool,
    /// The offset that unanswered exchange was launched at.
    pub busy_at: Option<u64>,
    /// Records behind the tail the pass read.
    pub lag: Option<u64>,
    /// `halted`, `rebuilding`, `copying`, `stalled` or `shipping`.
    pub state: &'static str,
    /// Why shipping stopped, as [`crate::halted::HaltedReplica::reason`].
    pub halted: Option<&'static str>,
}

/// A promotion's fence, as the latest attempt left it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FenceStatus {
    pub took: Vec<String>,
    /// Replicas that have not taken it.
    pub pending: Vec<String>,
    /// Attempts in a row at this generation that left the shard closed.
    pub attempts: u32,
    /// When the next attempt is due.
    pub retry_at: Option<tokio::time::Instant>,
    /// Why the latest attempt did not open the shard.
    pub why: Option<String>,
}

/// Every shard the driver leads, keyed as the quorum marks are.
#[derive(Debug, Default)]
pub struct ShardStatusBoard {
    /// Each entry with when it was written.
    shards: RwLock<HashMap<ShardKey, (tokio::time::Instant, ShardStatus)>>,
    /// What each shard's pass is doing right now, written when a pass starts
    /// and ends and when an exchange ends, not when a pass folds.
    live: RwLock<HashMap<ShardKey, LiveShard>>,
    /// The driver's latest scan, which stops advancing if its loop is blocked.
    driver_scan: std::sync::atomic::AtomicU64,
}

/// A shard's pass and unanswered exchanges as they stand, for reading why a
/// mark is stuck while no pass has ended to say.
#[derive(Debug, Default)]
struct LiveShard {
    /// The scan and start time of the pass under way, if one is.
    pass: Option<(u64, tokio::time::Instant)>,
    /// The scan of the last pass to end.
    last_scan: Option<u64>,
    /// Each exchange launched and not yet answered: the follower, the offset
    /// its cursor was at, and when it went out.
    exchanges: Vec<(String, u64, tokio::time::Instant)>,
    /// Passes started since the quorum mark last moved, and the mark that
    /// was. A mark that is stuck while this grows is being asked for; one
    /// that is stuck while it stays put is not.
    passes_since_mark: u32,
    mark_seen: Option<u64>,
    /// What last woke the shard: `append`, `tick`, `routes`, `busy answer`
    /// or `straggler answer`.
    last_wake: Option<&'static str>,
}

impl LiveShard {
    fn mark_moved_to(&mut self, mark: Option<u64>) {
        if self.mark_seen != mark {
            self.mark_seen = mark;
            self.passes_since_mark = 0;
        }
    }
}

/// [`LiveShard`] with its ages worked out, as the quorum-timeout warning
/// prints it.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct LiveReport {
    pub(crate) driver_scan: u64,
    pub(crate) last_scan: Option<u64>,
    /// The scan of the pass under way and how long it has run, in ms.
    pub(crate) pass: Option<(u64, u128)>,
    /// `node@offset sent Nms ago`, for each unanswered exchange.
    pub(crate) launched: Vec<String>,
    pub(crate) passes_since_mark: u32,
    pub(crate) last_wake: Option<&'static str>,
}

impl ShardStatusBoard {
    pub fn new() -> Self {
        Self::default()
    }

    /// What the driver last recorded for `key`. `None` for a shard this broker
    /// does not lead, or one no pass has reached yet.
    pub fn get(&self, key: &ShardKey) -> Option<ShardStatus> {
        self.shards
            .read()
            .get(key)
            .map(|(_, status)| status.clone())
    }

    /// [`Self::get`], with how long ago the driver wrote it. A pass stuck on a
    /// straggler leaves the entry its previous pass wrote.
    pub(crate) fn get_aged(&self, key: &ShardKey) -> Option<(ShardStatus, std::time::Duration)> {
        let shards = self.shards.read();
        let (at, status) = shards.get(key)?;
        Some((status.clone(), at.elapsed()))
    }

    pub(crate) fn pass_started(
        &self,
        key: &ShardKey,
        scan: u64,
        mark: Option<u64>,
        launched: Vec<(String, u64)>,
    ) {
        let now = tokio::time::Instant::now();
        let mut live = self.live.write();
        let shard = live.entry(key.clone()).or_default();
        shard.pass = Some((scan, now));
        shard.mark_moved_to(mark);
        shard.passes_since_mark = shard.passes_since_mark.saturating_add(1);
        // A follower still busy keeps its original launch.
        for (node, at) in launched {
            if !shard.exchanges.iter().any(|(held, _, _)| *held == node) {
                shard.exchanges.push((node, at, now));
            }
        }
    }

    /// A pass ended; `outstanding` are the exchanges still unanswered, which
    /// are all that remain of it. Those it finished itself are dropped.
    pub(crate) fn pass_ended(
        &self,
        key: &ShardKey,
        scan: u64,
        mark: Option<u64>,
        outstanding: Vec<(String, u64)>,
    ) {
        let mut live = self.live.write();
        // Only a shard a pass started on: one no longer led has no entry to
        // leave behind.
        let Some(shard) = live.get_mut(key) else {
            return;
        };
        shard.mark_moved_to(mark);
        let started = shard
            .pass
            .take()
            .map_or_else(tokio::time::Instant::now, |(_, at)| at);
        shard.last_scan = Some(scan);
        shard
            .exchanges
            .retain(|(node, _, _)| outstanding.iter().any(|(held, _)| held == node));
        for (node, at) in outstanding {
            if !shard.exchanges.iter().any(|(held, _, _)| *held == node) {
                shard.exchanges.push((node, at, started));
            }
        }
    }

    /// A pass ended on a shard this broker no longer leads.
    pub(crate) fn forget(&self, key: &ShardKey) {
        self.live.write().remove(key);
    }

    pub(crate) fn exchange_ended(&self, key: &ShardKey, node: &str) {
        if let Some(shard) = self.live.write().get_mut(key) {
            shard.exchanges.retain(|(held, _, _)| held != node);
        }
    }

    pub(crate) fn scanned(&self, scan: u64) {
        self.driver_scan
            .store(scan, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn driver_scan(&self) -> u64 {
        self.driver_scan.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn live_report(&self, key: &ShardKey) -> Option<LiveReport> {
        let now = tokio::time::Instant::now();
        let ago = |at: tokio::time::Instant| now.saturating_duration_since(at).as_millis();
        let live = self.live.read();
        let shard = live.get(key)?;
        Some(LiveReport {
            passes_since_mark: shard.passes_since_mark,
            last_wake: shard.last_wake,
            driver_scan: self.driver_scan(),
            last_scan: shard.last_scan,
            pass: shard.pass.map(|(scan, at)| (scan, ago(at))),
            launched: shard
                .exchanges
                .iter()
                .map(|(node, offset, at)| format!("{node}@{offset} sent {}ms ago", ago(*at)))
                .collect(),
        })
    }

    /// Record what last woke `key`. Appends repeat, so an unchanged wake
    /// takes no write lock.
    pub(crate) fn note_wake(&self, key: &ShardKey, wake: &'static str) {
        if self
            .live
            .read()
            .get(key)
            .is_some_and(|shard| shard.last_wake == Some(wake))
        {
            return;
        }
        self.live.write().entry(key.clone()).or_default().last_wake = Some(wake);
    }

    pub(crate) fn put(&self, key: ShardKey, status: ShardStatus) {
        self.shards
            .write()
            .insert(key, (tokio::time::Instant::now(), status));
    }

    /// Keep only the shards `led` says this broker still leads.
    pub(crate) fn retain(&self, led: impl Fn(&ShardKey) -> bool) {
        self.shards.write().retain(|key, _| led(key));
        self.live.write().retain(|key, _| led(key));
    }
}

/// A follower's line on the board, from its cursor and the tail the pass read.
pub(crate) fn follower_status(
    cursor: &FollowerCursor,
    tail: Option<u64>,
    learner: bool,
) -> FollowerStatus {
    let state = if cursor.halted.is_some() {
        "halted"
    } else if cursor.rebuilding {
        "rebuilding"
    } else if learner {
        "copying"
    } else if cursor.stalled {
        "stalled"
    } else {
        "shipping"
    };
    FollowerStatus {
        node_id: cursor.node_id.clone(),
        learner,
        next_offset: cursor.next_offset,
        confirmed: cursor.confirmed,
        busy: false,
        busy_at: None,
        lag: tail.map(|tail| tail.saturating_sub(cursor.next_offset)),
        state,
        halted: cursor.halted.map(|halt| crate::halted::describe(halt).0),
    }
}

#[cfg(test)]
mod tests;
