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
    shards: RwLock<HashMap<ShardKey, ShardStatus>>,
}

impl ShardStatusBoard {
    pub fn new() -> Self {
        Self::default()
    }

    /// What the driver last recorded for `key`. `None` for a shard this broker
    /// does not lead, or one no pass has reached yet.
    pub fn get(&self, key: &ShardKey) -> Option<ShardStatus> {
        self.shards.read().get(key).cloned()
    }

    pub(crate) fn put(&self, key: ShardKey, status: ShardStatus) {
        self.shards.write().insert(key, status);
    }

    /// Keep only the shards `led` says this broker still leads.
    pub(crate) fn retain(&self, led: impl Fn(&ShardKey) -> bool) {
        self.shards.write().retain(|key, _| led(key));
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
        lag: tail.map(|tail| tail.saturating_sub(cursor.next_offset)),
        state,
        halted: cursor.halted.map(|halt| crate::halted::describe(halt).0),
    }
}

#[cfg(test)]
mod tests;
