//! Which replicas replication has stopped for, and why.
//!
//! The metric next door is a bare count, deliberately: a label per shard is a
//! label per stream per tenant, which is unbounded in a multi-tenant broker. So
//! `felix_broker_replication_halted` can say *three replicas have stopped* and
//! nothing more, and the only way to learn which was to grep the broker's logs
//! for the warning that accompanied each halt.
//!
//! That is a poor position to be in, because a halt does not resolve on its
//! own. The follower is out of every quorum until an operator acts, and acting
//! means knowing exactly which replica of which shard, on which node, and
//! whether it diverged or simply needs history this leader no longer holds —
//! the answers decide whether a rebuild is the right move at all.
//!
//! This is a *listing*, not a metric, which is why it can carry the identity a
//! metric cannot: it is read on demand and its size is the number of halted
//! replicas, normally zero.
use parking_lot::Mutex;
use serde::Serialize;

use super::Halt;

/// One replica this broker has stopped shipping to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HaltedReplica {
    pub tenant_id: String,
    pub namespace: String,
    pub stream: String,
    pub shard: u32,
    /// `stream` or `cache` — a cache shard's log is a separate one under the
    /// same name, so the name alone does not identify it.
    pub kind: &'static str,
    /// The node whose replica stopped.
    pub node_id: String,
    /// The leadership this broker was shipping at. A halt belongs to a
    /// generation: the cursors are discarded when it changes, and so is this.
    pub generation: u64,
    /// How far the follower had got before it stopped, so an operator can see
    /// what a rebuild would re-transfer.
    pub next_offset: u64,
    pub reason: &'static str,
    /// What to do about it, in a sentence. The reason alone does not say
    /// whether the follower's data is wrong or merely incomplete.
    pub remedy: &'static str,
}

/// Why replication stopped, as a stable string and a remedy.
///
/// Stable because an operator's dashboard or runbook keys off it; the enum's
/// `Display` is prose and may be reworded.
pub(crate) fn describe(halt: Halt) -> (&'static str, &'static str) {
    match halt {
        Halt::Diverged => (
            "diverged",
            "the follower holds different bytes at an offset this leader also \
             holds, and no generation history bounds the disagreement. Its copy \
             of this shard has to be discarded and rebuilt; the leader does that \
             itself when FELIX_REPLICATION_REBUILD_MAX_CONCURRENT allows, and \
             this entry clears once it has.",
        ),
        Halt::Fenced => (
            "fenced",
            "the follower knows a newer generation than this broker is shipping \
             at, so this broker is no longer the leader. Resolves itself once \
             the assignment feed catches up; persistent means this broker is \
             partitioned from the control plane.",
        ),
        Halt::NeedsBootstrap => (
            "needs_bootstrap",
            "the follower wants records retention has already removed from this \
             leader, so shipping cannot reach it, and it holds records of its \
             own so it refused a log placed at the surviving base. Its copy of \
             this shard has to be discarded and rebuilt; the leader does that \
             itself when FELIX_REPLICATION_REBUILD_MAX_CONCURRENT allows, and \
             this entry clears once it has.",
        ),
    }
}

/// The halted replicas as of the last replication pass.
///
/// Replaced wholesale each pass rather than accumulated: a halt that has been
/// resolved must stop being reported, and a pass sees the whole truth.
#[derive(Debug, Default)]
pub struct HaltedReplicas {
    current: Mutex<Vec<HaltedReplica>>,
}

impl HaltedReplicas {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace the listing with what the pass just saw.
    ///
    /// `pub` for the same reason `QuorumMarks::publish` is: the driver writes
    /// it and the process that serves the listing holds the other end.
    pub fn publish(&self, halted: Vec<HaltedReplica>) {
        *self.current.lock() = halted;
    }

    /// What is halted now. Empty on a healthy broker, and on one that leads
    /// nothing.
    pub fn snapshot(&self) -> Vec<HaltedReplica> {
        self.current.lock().clone()
    }
}

#[cfg(test)]
mod tests;
