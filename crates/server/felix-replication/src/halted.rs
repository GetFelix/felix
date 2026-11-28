//! Which replicas replication has stopped for, and why.
//!
//! A halt does not resolve on its own. The follower is out of every quorum
//! until an operator acts, and acting means knowing exactly which replica of
//! which shard, on which node, and why it stopped.
//!
//! This is a *listing*, not a metric, which is why it can carry the identity a
//! metric cannot: it is read on demand and its size is the number of halted
//! replicas, normally zero.
use parking_lot::Mutex;
use serde::Serialize;

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
