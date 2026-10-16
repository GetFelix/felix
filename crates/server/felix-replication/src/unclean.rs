//! A broker that restarted after a crash that may have cost it records it
//! acknowledged: its storage id rotated at startup (`run_state::begin`).
//!
//! A copy of a shard this broker led before the crash may be short of what
//! it acknowledged, so it must not lead that shard again from it, and must
//! not answer a promotion fence for it: either counts the short copy as a
//! vote, and the records only the lost tail held can be overwritten. See
//! "A leader's own crash" in `docs/replication-design.md` (#1295).

use std::collections::HashMap;
use std::sync::Arc;

use crate::ShardKey;

/// What this process knows about the copies it led before the crash.
#[derive(Debug)]
pub struct UncleanStart {
    node_id: Arc<str>,
    /// The generation this process took leadership of each shard at. A ballot
    /// at that generation naming this broker is this process's own.
    led: parking_lot::Mutex<HashMap<ShardKey, u64>>,
}

impl UncleanStart {
    pub fn new(node_id: impl Into<Arc<str>>) -> Self {
        Self {
            node_id: node_id.into(),
            led: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    /// This process leads `key` at `generation`.
    pub fn lead(&self, key: &ShardKey, generation: u64) {
        self.led.lock().insert(key.clone(), generation);
    }

    /// Whether this process took the lead of `key` at `generation`.
    pub fn leads_at(&self, key: &ShardKey, generation: u64) -> bool {
        self.led.lock().get(key) == Some(&generation)
    }

    /// Whether `log` is `key`'s copy as this broker led it before the crash:
    /// its accepted generation was this broker's own, and no newer leader has
    /// been accepted here since. `generation` is the one being asked about.
    ///
    /// Without a ballot naming a leader only an assignment at the accepted
    /// generation itself says so: the control plane names one leader per
    /// generation, so a broker named at a generation it accepted led it.
    pub fn led_before(
        &self,
        key: &ShardKey,
        log: &felix_broker::StreamLog,
        generation: u64,
    ) -> bool {
        let accepted = log.accepted_generation();
        if accepted == 0 || self.leads_at(key, accepted) {
            return false;
        }
        match log.accepted_leader() {
            // Its own ballot, or one it already gave up.
            Some(leader) => *leader == *self.node_id || leader.is_empty(),
            None => generation == accepted,
        }
    }

    /// This broker's node id.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }
}

#[cfg(test)]
mod tests;
