//! A shard's assignment as the control plane confirms it.
//!
//! A copy with no promise on disk answers a leader only as the control plane
//! backs it (see `ReplicaHandler::refuse_unbacked`). The routing view is
//! seeded from whichever control-plane member answered, which can lag, so
//! such a copy also asks for a read the control plane's leader confirmed
//! (#1332).

/// What a confirmed read found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Confirmed {
    /// The control plane names `leader` at `generation`.
    Assigned { generation: u64, leader: String },
    /// The control plane has no assignment for the shard.
    Unassigned,
    /// The control plane does not serve confirmed reads (an older build, or
    /// a Raft leader that predates them). The routing view answers alone, as
    /// it did before.
    NotServed,
    /// No confirmed answer arrived. Nothing is known, so nothing is answered.
    Unavailable,
}

/// Reads a shard's assignment no older than any write the control plane
/// acknowledged before the call.
#[async_trait::async_trait]
pub trait ConfirmedRoutes: Send + Sync {
    async fn confirmed(&self, key: &felix_router::ShardKey) -> Confirmed;
}

/// How long a confirmed leader is reused for batches at its generation. A
/// learner checks every batch it takes, and one read each is a round trip to
/// the control plane per batch. Short, since the control plane can move past
/// the generation and only a fresh read sees that.
const CACHED_FOR: std::time::Duration = std::time::Duration::from_millis(500);

/// The last confirmed leader per shard, at the generation it was read for.
#[derive(Debug, Default)]
pub(crate) struct ConfirmedCache {
    leaders: parking_lot::Mutex<
        std::collections::HashMap<felix_router::ShardKey, (u64, String, std::time::Instant)>,
    >,
}

impl ConfirmedCache {
    /// The leader confirmed at `generation`, if read for it within
    /// [`CACHED_FOR`].
    pub(crate) fn lookup(&self, key: &felix_router::ShardKey, generation: u64) -> Option<String> {
        self.leaders
            .lock()
            .get(key)
            .filter(|(at, _, read)| *at == generation && read.elapsed() < CACHED_FOR)
            .map(|(_, leader, _)| leader.clone())
    }

    pub(crate) fn keep(&self, key: &felix_router::ShardKey, generation: u64, leader: &str) {
        self.leaders.lock().insert(
            key.clone(),
            (generation, leader.to_string(), std::time::Instant::now()),
        );
    }
}
